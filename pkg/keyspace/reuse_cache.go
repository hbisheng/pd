// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package keyspace

import (
	"context"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// StopIDReuse cancels and joins the old leader's loader before its cache can be
// replaced. The loader checks its context again before publishing each read.
func (manager *Manager) StopIDReuse() {
	manager.reuse.lifecycleMu.Lock()
	defer manager.reuse.lifecycleMu.Unlock()
	manager.stopReuseLoader()
}

func (manager *Manager) stopReuseLoader() {
	if manager.reuse.loaderCancel != nil {
		manager.reuse.loaderCancel()
		<-manager.reuse.loaderDone
		manager.reuse.loaderCancel = nil
	}
}

func (manager *Manager) bucketCount() uint32 {
	width := uint32(manager.bitmapBytes() * 8)
	return (constant.MaxValidKeyspaceID + 1 + width - 1) / width
}

func revisionedTxn(txn kv.Txn) (kv.RevisionedTxn, error) {
	result, ok := txn.(kv.RevisionedTxn)
	if !ok {
		return nil, errors.New("free bitmap requires revision-aware transactions")
	}
	return result, nil
}

func (manager *Manager) loadFreeBuckets(ctx context.Context, done chan struct{}) {
	defer close(done)
	interval := manager.config.GetIDReuseLoadInterval()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := manager.loadNextFreeBucketPage(ctx); err != nil && ctx.Err() == nil {
				log.Warn("failed to load free keyspace bitmap page", zap.Error(err))
			}
			timer.Reset(interval)
		}
	}
}

// loadNextFreeBucketPage bounds server work by numeric bucket interval, even
// when it contains no keys. claimMu covers only this read and its publication.
func (manager *Manager) loadNextFreeBucketPage(ctx context.Context) error {
	manager.reuse.claimMu.Lock()
	defer manager.reuse.claimMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	manager.reuse.mu.RLock()
	ready := manager.reuse.revisionReady
	hasMetadata := len(manager.reuse.metadataNeeds) != 0
	manager.reuse.mu.RUnlock()
	if !ready {
		if err := manager.refreshReuseRevisionLocked(ctx); err != nil {
			return err
		}
	}
	manager.reuse.metadataTurn = !manager.reuse.metadataTurn
	if hasMetadata && (manager.reuse.metadataTurn || manager.reuse.loadNext == manager.bucketCount()) {
		return manager.loadReuseMetadataPageLocked(ctx)
	}
	// A conflict can invalidate an interval already traversed by the loader.
	for bucket := range manager.reuse.invalidBuckets {
		return manager.readFreeBucketIntervalLocked(ctx, bucket, bucket+1)
	}
	start := manager.reuse.loadNext
	end := min(start+uint32(manager.config.GetIDReuseLoadPageBuckets()), manager.bucketCount())
	if start == end {
		return nil
	}
	if err := manager.readFreeBucketIntervalLocked(ctx, start, end); err != nil {
		return err
	}
	manager.reuse.loadNext = end
	return nil
}

func (manager *Manager) readFreeBucketIntervalLocked(ctx context.Context, start, end uint32) error {
	manager.reuse.mu.RLock()
	proof, ready := manager.reuse.revision, manager.reuse.revisionReady
	manager.reuse.mu.RUnlock()
	if !ready {
		return errors.New("free bitmap revision is not loaded")
	}
	var values []kv.RevisionedValue
	err := manager.store.RunInTxn(ctx, func(txn kv.Txn) error {
		rt, err := revisionedTxn(txn)
		if err != nil {
			return err
		}
		rt.CompareModRevision(freeIDRevisionPath(), proof.ModRevision)
		values, err = rt.LoadRangeWithRevision(bitmapPath(start), bitmapPath(end), int(end-start))
		return err
	})
	if err != nil {
		if errors.ErrorEqual(err, errs.ErrEtcdTxnConflict) {
			manager.reuse.mu.Lock()
			manager.reuse.revisionReady = false
			manager.reuse.mu.Unlock()
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	loaded := make(map[uint32]kv.RevisionedValue, len(values))
	for _, value := range values {
		bucket, err := strconv.ParseUint(strings.TrimPrefix(value.Key, bitmapPrefix()), 10, 32)
		if err != nil || uint32(bucket) < start || uint32(bucket) >= end || len(value.Value) != manager.bitmapBytes() || value.ModRevision <= 0 {
			return errors.New("invalid free bitmap encoding or revision")
		}
		loaded[uint32(bucket)] = value
	}
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	if !manager.reuse.revisionReady || manager.reuse.revision.ModRevision != proof.ModRevision {
		return errors.New("metadata changed during bitmap loading")
	}
	for bucket := start; bucket < end; bucket++ {
		value, exists := loaded[bucket]
		if !exists {
			value.Key = bitmapPath(bucket)
		}
		manager.publishFreeBucketLocked(bucket, value.Value, value.ModRevision)
	}
	return nil
}

func (manager *Manager) refreshReuseRevisionLocked(ctx context.Context) error {
	var revision kv.RevisionedValue
	err := manager.store.RunInTxn(ctx, func(txn kv.Txn) error {
		var err error
		revision, err = readReuseRevision(txn)
		return err
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	if manager.reuse.revision.ModRevision > revision.ModRevision {
		return errors.New("metadata changed during revision loading")
	}
	manager.reuse.revision, manager.reuse.revisionReady = revision, true
	manager.reuse.metadataPages = make(map[uint32]reuseMetadataPage)
	for start, need := range manager.reuse.metadataNeeds {
		need.next = start
		manager.reuse.metadataNeeds[start] = need
	}
	manager.reuse.buckets = make(map[uint32]kv.RevisionedValue)
	manager.reuse.invalidBuckets = make(map[uint32]struct{})
	manager.reuse.freeBuckets = nil
	manager.reuse.loadNext = 0
	return nil
}

func (manager *Manager) loadReuseMetadataPageLocked(ctx context.Context) error {
	manager.reuse.mu.RLock()
	selected, first := ^uint32(0), ^uint32(0)
	for start := range manager.reuse.metadataNeeds {
		first = min(first, start)
		if start >= manager.reuse.metadataCursor {
			selected = min(selected, start)
		}
	}
	if selected == ^uint32(0) {
		selected = first
	}
	need := manager.reuse.metadataNeeds[selected]
	page := need.next
	proof, ready := manager.reuse.revision, manager.reuse.revisionReady
	manager.reuse.mu.RUnlock()
	if first == ^uint32(0) || !ready {
		return nil
	}
	var values []kv.RevisionedValue
	err := manager.store.RunInTxn(ctx, func(txn kv.Txn) error {
		rt, err := revisionedTxn(txn)
		if err != nil {
			return err
		}
		rt.CompareModRevision(freeIDRevisionPath(), proof.ModRevision)
		values, err = rt.LoadRangeWithRevision(keypath.KeyspaceMetaPath(page*reuseMetadataPageIDs), keypath.KeyspaceMetaPath((page+1)*reuseMetadataPageIDs), int(reuseMetadataPageIDs))
		return err
	})
	if err != nil {
		if errors.ErrorEqual(err, errs.ErrEtcdTxnConflict) {
			manager.reuse.mu.Lock()
			manager.reuse.revisionReady = false
			manager.reuse.mu.Unlock()
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var loaded reuseMetadataPage
	for _, value := range values {
		meta := &keyspacepb.KeyspaceMeta{}
		if err := proto.Unmarshal([]byte(value.Value), meta); err != nil {
			return err
		}
		if meta.GetId()/reuseMetadataPageIDs != page {
			return errors.New("invalid keyspace metadata page")
		}
		mask := uint64(1) << (meta.GetId() % reuseMetadataPageIDs)
		loaded.present |= mask
		if meta.State == keyspacepb.KeyspaceState_TOMBSTONE {
			loaded.tombstones |= mask
		}
		if manager.transactionalReuseConfig(meta.Config) {
			loaded.transactional |= mask
		}
	}
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	if !manager.reuse.revisionReady || manager.reuse.revision.ModRevision != proof.ModRevision {
		return errors.New("metadata changed during page loading")
	}
	manager.reuse.metadataPages[page] = loaded
	// Keep a stable interval key while advancing its frontier. A very large
	// merged range cannot monopolize discovery ahead of unrelated Regions.
	need = manager.reuse.metadataNeeds[selected]
	delete(manager.reuse.metadataNeeds, selected)
	for page++; page < need.end; page++ {
		if _, exists := manager.reuse.metadataPages[page]; !exists {
			need.next = page
			manager.reuse.metadataNeeds[selected] = need
			break
		}
	}
	manager.reuse.metadataCursor = selected + 1
	return nil
}

func (manager *Manager) loadFreeBucketLocked(bucket uint32) (kv.RevisionedValue, error) {
	value, loaded := manager.reuse.buckets[bucket]
	if loaded {
		return value, nil
	}
	ctx := manager.reuse.loaderCtx
	if ctx == nil {
		ctx = manager.ctx
	}
	if err := manager.readFreeBucketIntervalLocked(ctx, bucket, bucket+1); err != nil {
		return kv.RevisionedValue{}, err
	}
	return manager.reuse.buckets[bucket], nil
}

func (manager *Manager) invalidateFreeBucketLocked(bucket uint32) {
	manager.publishFreeBucketLocked(bucket, "", 0)
	delete(manager.reuse.buckets, bucket)
	manager.reuse.invalidBuckets[bucket] = struct{}{}
}

// cachedFreeBucketsLocked preserves ascending order. Unknown lower coverage is
// checked by memoryOrderingProof rather than rediscovered by an etcd scan.
func (manager *Manager) cachedFreeBucketsLocked() []uint32 {
	return append([]uint32(nil), manager.reuse.freeBuckets...)
}

// changeCachedFreeBit adds a revision comparison without fetching the old value.
// The caller publishes the resulting bucket only after the transaction commits.
func (manager *Manager) changeCachedFreeBit(txn kv.Txn, value kv.RevisionedValue, id uint32, free bool) (string, error) {
	rt, err := revisionedTxn(txn)
	if err != nil {
		return "", err
	}
	rt.CompareModRevision(value.Key, value.ModRevision)
	bits, err := changedFreeBit(value.Value, manager.bitmapBytes(), id, free)
	if err != nil {
		return "", err
	}
	if bits == "" {
		return bits, txn.Remove(value.Key)
	}
	return bits, txn.Save(value.Key, bits)
}

func (manager *Manager) publishFreeBucketLocked(bucket uint32, value string, revision int64) {
	index := sort.Search(len(manager.reuse.freeBuckets), func(i int) bool { return manager.reuse.freeBuckets[i] >= bucket })
	present := index < len(manager.reuse.freeBuckets) && manager.reuse.freeBuckets[index] == bucket
	if value == "" && present {
		manager.reuse.freeBuckets = append(manager.reuse.freeBuckets[:index], manager.reuse.freeBuckets[index+1:]...)
	} else if value != "" && !present {
		manager.reuse.freeBuckets = append(manager.reuse.freeBuckets, 0)
		copy(manager.reuse.freeBuckets[index+1:], manager.reuse.freeBuckets[index:])
		manager.reuse.freeBuckets[index] = bucket
	}
	if value == "" {
		revision = 0
	}
	manager.reuse.buckets[bucket] = kv.RevisionedValue{Key: bitmapPath(bucket), Value: value, ModRevision: revision}
	delete(manager.reuse.invalidBuckets, bucket)
}

func (manager *Manager) reconcileReusedClaim(expected *keyspacepb.KeyspaceMeta) bool {
	matched := false
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		exists, id, err := manager.store.LoadKeyspaceID(txn, expected.Name)
		if err != nil || !exists || id != expected.GetId() {
			return err
		}
		meta, err := manager.store.LoadKeyspaceMeta(txn, id)
		matched = err == nil && meta != nil && meta.State == keyspacepb.KeyspaceState_DISABLED &&
			meta.Name == expected.Name && meta.CreatedAt == expected.CreatedAt && maps.Equal(meta.Config, expected.Config)
		return err
	})
	return err == nil && matched
}

func (manager *Manager) reconcileReclaimedID(expected *keyspacepb.KeyspaceMeta) bool {
	removed := false
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId())
		if err != nil || meta != nil {
			return err
		}
		exists, _, err := manager.store.LoadKeyspaceID(txn, expected.Name)
		removed = err == nil && !exists
		return err
	})
	if err != nil || !removed {
		return false
	}
	if err := manager.refreshReuseRevisionLocked(manager.ctx); err != nil {
		return false
	}
	id := expected.GetId()
	value, err := manager.loadFreeBucketLocked(id / uint32(manager.bitmapBytes()*8))
	return err == nil && len(value.Value) == manager.bitmapBytes() && value.Value[(id%uint32(manager.bitmapBytes()*8))/8]&(1<<(id%8)) != 0
}
