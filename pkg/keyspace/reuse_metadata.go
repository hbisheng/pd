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
	"math/bits"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

const reuseMetadataPageIDs = uint32(64)

// A loaded page distinguishes absent IDs from live owners and tombstones using
// three bits per ID. No tenant names or configuration are preloaded.
type reuseMetadataPage struct{ present, tombstones, transactional uint64 }
type reuseMetadataNeed struct{ next, end uint32 }
type reuseProgress struct {
	meta     *keyspacepb.KeyspaceMeta
	revision int64
}

// reuseMetadataTxn observes only staged keyspace metadata mutations. The outer
// transaction still owns all name/group/bitmap operations and their conditions.
type reuseMetadataTxn struct {
	kv.Txn
	changes map[uint32]*keyspacepb.KeyspaceMeta
}

func (t *reuseMetadataTxn) Save(key, value string) error {
	if err := t.Txn.Save(key, value); err != nil {
		return err
	}
	if strings.HasPrefix(key, keypath.KeyspaceMetaPrefix()) {
		meta := &keyspacepb.KeyspaceMeta{}
		if err := proto.Unmarshal([]byte(value), meta); err != nil {
			return err
		}
		t.changes[meta.GetId()] = meta
	}
	return nil
}
func (t *reuseMetadataTxn) Remove(key string) error {
	if err := t.Txn.Remove(key); err != nil {
		return err
	}
	if strings.HasPrefix(key, keypath.KeyspaceMetaPrefix()) {
		id, err := strconv.ParseUint(strings.TrimPrefix(key, keypath.KeyspaceMetaPrefix()), 10, 32)
		if err != nil {
			return err
		}
		t.changes[uint32(id)] = nil
	}
	return nil
}
func (t *reuseMetadataTxn) LoadRangeWithRevision(key, end string, limit int) ([]kv.RevisionedValue, error) {
	return t.Txn.(kv.RevisionedTxn).LoadRangeWithRevision(key, end, limit)
}
func (t *reuseMetadataTxn) CompareModRevision(key string, revision int64) {
	t.Txn.(kv.RevisionedTxn).CompareModRevision(key, revision)
}
func (t *reuseMetadataTxn) CommitRevision() int64 { return t.Txn.(kv.RevisionedTxn).CommitRevision() }

func readReuseRevision(txn kv.Txn) (kv.RevisionedValue, error) {
	rt, err := revisionedTxn(txn)
	if err != nil {
		return kv.RevisionedValue{}, err
	}
	key := freeIDRevisionPath()
	values, err := rt.LoadRangeWithRevision(key, key+"\x00", 1)
	if err != nil {
		return kv.RevisionedValue{}, err
	}
	if len(values) == 0 {
		return kv.RevisionedValue{Key: key}, nil
	}
	return values[0], nil
}

// runMetadataTxn keeps normal, explicit-ID, lifecycle, rollback and group-removal
// writers in the same existing revision fence used by memory ordering proofs.
// It publishes compact state only after commit and never acquires claimMu while
// a caller may hold a keyspace/group lock.
func (manager *Manager) runMetadataTxn(f func(kv.Txn) error) error {
	if !manager.config.IsIDReuseEnabled() {
		return manager.store.RunInTxn(manager.ctx, f)
	}
	for attempt := 0; attempt < 3; attempt++ {
		err := manager.runMetadataTxnWithProof(f, nil)
		if !errors.ErrorEqual(err, errs.ErrEtcdTxnConflict) {
			return err
		}
	}
	return errs.ErrEtcdTxnConflict
}

func (manager *Manager) runMetadataTxnWithProof(f func(kv.Txn) error, proof *kv.RevisionedValue) error {
	var wrapped *reuseMetadataTxn
	var before kv.RevisionedValue
	var after string
	attempted := false
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		wrapped = &reuseMetadataTxn{Txn: txn, changes: make(map[uint32]*keyspacepb.KeyspaceMeta)}
		if err := f(wrapped); err != nil {
			return err
		}
		if len(wrapped.changes) == 0 {
			return nil
		}
		if proof != nil {
			before = *proof
		} else {
			manager.reuse.mu.RLock()
			var ready bool
			before, ready = manager.reuse.revision, manager.reuse.revisionReady
			manager.reuse.mu.RUnlock()
			if !ready {
				var err error
				before, err = readReuseRevision(txn)
				if err != nil {
					return err
				}
			}
		}
		revision := uint64(0)
		if before.Value != "" {
			var err error
			revision, err = strconv.ParseUint(before.Value, 10, 64)
			if err != nil {
				return err
			}
		}
		if revision == ^uint64(0) {
			return errors.New("free ID revision exhausted")
		}
		after = strconv.FormatUint(revision+1, 10)
		wrapped.CompareModRevision(freeIDRevisionPath(), before.ModRevision)
		attempted = true
		return txn.Save(freeIDRevisionPath(), after)
	})
	if err != nil {
		if !attempted {
			return err
		}
		manager.reuse.mu.Lock()
		manager.reuse.revisionReady = false
		manager.reuse.metadataPages = make(map[uint32]reuseMetadataPage)
		manager.reuse.mu.Unlock()
		return err
	}
	if len(wrapped.changes) == 0 {
		return nil
	}
	committed := wrapped.CommitRevision()
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	coherent := manager.reuse.revisionReady && manager.reuse.revision.ModRevision == before.ModRevision
	if coherent {
		manager.reuse.revision = kv.RevisionedValue{Key: freeIDRevisionPath(), Value: after, ModRevision: committed}
	} else {
		manager.reuse.revisionReady = false
		manager.reuse.metadataPages = make(map[uint32]reuseMetadataPage)
	}
	for id, meta := range wrapped.changes {
		if page, loaded := manager.reuse.metadataPages[id/reuseMetadataPageIDs]; loaded {
			mask := uint64(1) << (id % reuseMetadataPageIDs)
			page.present &^= mask
			page.tombstones &^= mask
			page.transactional &^= mask
			if meta != nil {
				page.present |= mask
				if meta.State == keyspacepb.KeyspaceState_TOMBSTONE {
					page.tombstones |= mask
				}
				if manager.transactionalReuseConfig(meta.Config) {
					page.transactional |= mask
				}
			}
			manager.reuse.metadataPages[id/reuseMetadataPageIDs] = page
		}
		if current, waiting := manager.reuse.progress[id]; waiting && committed >= current.revision {
			manager.reuse.progress[id] = reuseProgress{meta: meta, revision: committed}
		}
		if meta != nil {
			manager.cacheReuseMetaLocked(meta)
		} else {
			delete(manager.reuse.pending, id)
			delete(manager.reuse.grants, id)
			manager.keyspaceNameLookup.Delete(id)
			manager.keyspaceStateLookup.Delete(id)
		}
	}
	return nil
}

// metadataCoverageLocked queues only the required numeric interval. Unknown
// pages defer reuse; the paced loader advances independently of future Creates.
// The caller holds reuse.mu.
func (manager *Manager) metadataCoverageLocked(first, end uint32) bool {
	start, last := first/reuseMetadataPageIDs, end/reuseMetadataPageIDs
	for page := start; page <= last; page++ {
		if _, loaded := manager.reuse.metadataPages[page]; !loaded {
			need, exists := manager.reuse.metadataNeeds[start]
			if !exists {
				need.next = page
			}
			need.next, need.end = min(need.next, page), max(need.end, last+1)
			manager.reuse.metadataNeeds[start] = need
			return false
		}
	}
	return true
}
func (manager *Manager) metadataStateLocked(id uint32) (present, tombstone bool) {
	page := manager.reuse.metadataPages[id/reuseMetadataPageIDs]
	mask := uint64(1) << (id % reuseMetadataPageIDs)
	return page.present&mask != 0, page.tombstones&mask != 0
}
func (manager *Manager) freeBitLocked(id uint32) bool {
	value := manager.reuse.buckets[id/uint32(manager.bitmapBytes()*8)].Value
	return value != "" && value[(id%uint32(manager.bitmapBytes()*8))/8]&(1<<(id%8)) != 0
}

// memoryOrderingProof requires complete bitmap and metadata coverage from the
// current Region's first complete ID. A zero bit alone never proves a hole.
func (manager *Manager) memoryOrderingProof(region *core.RegionInfo, id uint32, reclaim bool) (kv.RevisionedValue, bool) {
	first, end := firstRegionKeyspace(region), id
	if reclaim {
		end++
	}
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	if !manager.reuse.revisionReady || !manager.metadataCoverageLocked(first, end) {
		return kv.RevisionedValue{}, false
	}
	width := uint32(manager.bitmapBytes() * 8)
	for bucket := first / width; bucket <= end/width; bucket++ {
		if _, loaded := manager.reuse.buckets[bucket]; !loaded {
			return kv.RevisionedValue{}, false
		}
	}
	if !reclaim {
		for _, bucket := range manager.reuse.freeBuckets {
			if bucket < first/width {
				continue
			}
			if bucket > id/width {
				break
			}
			for offset, value := range []byte(manager.reuse.buckets[bucket].Value) {
				for value != 0 {
					bit := uint32(bits.TrailingZeros8(value))
					candidate := bucket*width + uint32(offset*8) + bit
					if candidate >= first && candidate < id {
						return kv.RevisionedValue{}, false
					}
					value &^= 1 << bit
				}
			}
		}
	}
	for page := first / reuseMetadataPageIDs; page <= id/reuseMetadataPageIDs; page++ {
		pending := manager.reuse.metadataPages[page].tombstones
		for pending != 0 {
			bit := uint32(bits.TrailingZeros64(pending))
			candidate := page*reuseMetadataPageIDs + bit
			pending &^= uint64(1) << bit
			if candidate < first || candidate >= id {
				continue
			}
			present, _ := manager.metadataStateLocked(candidate + 1)
			if present || manager.freeBitLocked(candidate+1) {
				return kv.RevisionedValue{}, false
			}
		}
	}
	if reclaim {
		present, tombstone := manager.metadataStateLocked(id + 1)
		if (!present || !tombstone) && !manager.freeBitLocked(id+1) {
			return kv.RevisionedValue{}, false
		}
	}
	return manager.reuse.revision, true
}
