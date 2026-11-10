// Copyright 2026 TiKV Project Authors.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcsconstant "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

type idReuseConfig interface {
	IsIDReuseEnabled() bool
	IsEncryptedIDReuseEnabled() bool
	GetIDReuseThreshold() uint64
	GetIDReuseBucketBytes() int
	GetIDReuseLoadPageBuckets() int
	GetIDReuseLoadInterval() time.Duration
	GetIDReuseReclaimInterval() time.Duration
}

type idReuse struct {
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	bitmap    *freeIDBitmap
	limiter   *rate.Limiter
	store     *reuseLeaderStore
	watermark uint64
	threshold uint64
	encrypted bool
}

type reuseLeaderStore struct {
	freeIDBitmapStore
	leaderKey, leaderValue string
}

// RunInTxnWithConditions fences writes against the current PD leader key.
func (s *reuseLeaderStore) RunInTxnWithConditions(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	guarded := append([]clientv3.Cmp{clientv3.Compare(clientv3.Value(s.leaderKey), "=", s.leaderValue)}, conditions...)
	return s.freeIDBitmapStore.RunInTxnWithConditions(ctx, guarded, f)
}

func keyspaceFreePrefix() string { return fmt.Sprintf("/pd/%d/keyspaces/free/", keypath.ClusterID()) }

// StartIDReuse starts the opt-in leader-local loader. With reuse disabled it
// returns before any storage access. Call StopIDReuse when leadership ends.
func (manager *Manager) StartIDReuse(backend kv.Base, leaderKey, leaderValue string) error {
	manager.reuseLifecycleMu.Lock()
	defer manager.reuseLifecycleMu.Unlock()
	manager.stopIDReuseLocked()
	cfg, ok := manager.config.(idReuseConfig)
	if !ok || !cfg.IsIDReuseEnabled() {
		return nil
	}
	if cluster, ok := manager.cluster.(interface{ IsServiceIndependent(string) bool }); ok && cluster.IsServiceIndependent(mcsconstant.SchedulingServiceName) {
		return errors.New("keyspace ID reuse requires integrated scheduling")
	}
	store, ok := backend.(freeIDBitmapStore)
	if !ok || leaderKey == "" || leaderValue == "" {
		return errors.New("keyspace ID reuse requires revision-aware storage and a leader identity")
	}
	ctx, cancel := context.WithCancel(manager.ctx)
	guarded := &reuseLeaderStore{freeIDBitmapStore: store, leaderKey: leaderKey, leaderValue: leaderValue}
	bitmap, err := newFreeIDBitmap(guarded, keyspaceFreePrefix(), cfg.GetIDReuseBucketBytes())
	if err != nil {
		cancel()
		return err
	}
	if err = bitmap.initialize(ctx); err != nil {
		cancel()
		return err
	}
	response, err := store.CreateRawTxnWithContext(ctx).Then(kv.RawTxnOp{Key: keypath.KeyspaceAllocIDPath(), OpType: kv.RawTxnOpGet}).Commit()
	if err != nil {
		cancel()
		return err
	}
	var watermark uint64
	if len(response.Responses) == 1 && len(response.Responses[0].KeyValuePairs) == 1 {
		watermark, err = typeutil.BytesToUint64([]byte(response.Responses[0].KeyValuePairs[0].Value))
		if err != nil {
			cancel()
			return err
		}
	}
	r := &idReuse{ctx: ctx, cancel: cancel, done: make(chan struct{}), bitmap: bitmap, store: guarded, watermark: watermark,
		threshold: cfg.GetIDReuseThreshold(), encrypted: cfg.IsEncryptedIDReuseEnabled(), limiter: rate.NewLimiter(rate.Every(cfg.GetIDReuseReclaimInterval()), 1)}
	manager.reuse.Store(r)
	go func() {
		defer close(r.done)
		_ = bitmap.runLoader(ctx, uint32(cfg.GetIDReuseLoadPageBuckets()), cfg.GetIDReuseLoadInterval())
	}()
	return nil
}

// StopIDReuse cancels and joins the loader before a new leader cache is installed.
func (manager *Manager) StopIDReuse() {
	manager.reuseLifecycleMu.Lock()
	defer manager.reuseLifecycleMu.Unlock()
	manager.stopIDReuseLocked()
}
func (manager *Manager) stopIDReuseLocked() {
	if r := manager.reuse.Swap(nil); r != nil {
		r.cancel()
		<-r.done
	}
}

func (manager *Manager) reuseAdmitted(r *idReuse) bool {
	high := r.watermark
	if alloc, ok := manager.idAllocator.(interface{ GetReservedWindowEnd() uint64 }); ok {
		high = max(high, alloc.GetReservedWindowEnd())
	}
	return manager.reuse.Load() == r && r.ctx.Err() == nil && high >= r.threshold
}

// RequestKeyspaceReclaim is called by Cleaner. A deferred candidate returns
// false; a committed reclamation atomically deletes metadata and sets its bit.
func (manager *Manager) RequestKeyspaceReclaim(ctx context.Context, id uint32) (bool, error) {
	r := manager.reuse.Load()
	if r == nil || !manager.reuseAdmitted(r) {
		return false, errors.New("keyspace ID reuse is disabled or below its allocation threshold")
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := r.limiter.Wait(ctx); err != nil {
		return false, err
	}
	return manager.reclaimKeyspace(ctx, r.bitmap, id)
}

func supportsReuseEncryption(config map[string]string, enabled bool) bool {
	if config["encryption"] == "" {
		return true
	}
	var value struct {
		Enabled bool   `json:"enabled"`
		CMEKID  string `json:"cmek_id"`
		Vendor  string `json:"vendor"`
	}
	if json.Unmarshal([]byte(config["encryption"]), &value) != nil {
		return false
	}
	return !value.Enabled || (enabled && value.CMEKID == "" && value.Vendor == "")
}

// createReusedKeyspace is entered only while the explicitly enabled service is
// active. Returning nil, nil leaves the ordinary Create path untouched.
func (manager *Manager) createReusedKeyspace(r *idReuse, request *CreateKeyspaceRequest) (*keyspacepb.KeyspaceMeta, error) {
	if !manager.reuseAdmitted(r) || !manager.transactionalReuseConfig(request.Config) || !supportsReuseEncryption(request.Config, r.encrypted) {
		return nil, nil
	}
	config := maps.Clone(request.Config)
	if config == nil {
		config = make(map[string]string)
	}
	delete(config, MetaServiceGroupAddressesKey)
	delete(config, MetaServiceGroupIDKey)
	delete(config, TSOKeyspaceGroupIDKey)
	groupConfig, err := manager.kgm.GetKeyspaceConfigByKind(endpoint.StringUserKind(config[UserKindKey]))
	if err != nil {
		return nil, err
	}
	config[TSOKeyspaceGroupIDKey] = groupConfig[TSOKeyspaceGroupIDKey]
	config[UserKindKey] = groupConfig[UserKindKey]
	config[RegionBoundType] = txnRegionBound.String()
	if kerneltype.IsNextGen() && config[GCManagementType] == "" {
		config[GCManagementType] = KeyspaceLevelGC
	}
	meta, err := manager.claimFreeKeyspace(r, request.Name, config, request.CreateTime)
	if err != nil || meta == nil {
		return meta, err
	}
	err = manager.initializeReusedKeyspace(r, meta)
	if err == nil {
		err = manager.finishReusedKeyspace(r, meta, keyspacepb.KeyspaceState_ENABLED)
	}
	if err != nil {
		if cancelErr := manager.finishReusedKeyspace(r, meta, keyspacepb.KeyspaceState_ARCHIVED); cancelErr != nil {
			return nil, errors.Annotatef(cancelErr, "cancel reused creation: %v", err)
		}
		return nil, err
	}
	meta.State = keyspacepb.KeyspaceState_ENABLED
	if manager.mgm != nil {
		manager.mgm.AttachEndpoints(meta.Config)
	}
	return meta, nil
}

func firstRegionKeyspace(region *core.RegionInfo) uint32 {
	return uint32(sort.Search(int(constant.ReservedKeyspaceIDStart), func(id int) bool {
		return bytes.Compare(MakeRegionBound(uint32(id)).TxnLeftBound, region.GetStartKey()) >= 0
	}))
}

func (manager *Manager) claimFreeKeyspace(r *idReuse, name string, config map[string]string, createdAt int64) (*keyspacepb.KeyspaceMeta, error) {
	b := r.bitmap
	b.mu.Lock()
	buckets := append([]uint32(nil), b.freeBuckets...)
	b.mu.Unlock()
	seen := make(map[uint64]bool)
	width := uint32(b.bucketBytes * 8)
	for _, bucket := range buckets {
		for start := bucket * width; start < (bucket+1)*width; {
			id, ok := b.firstFree(start, (bucket+1)*width)
			if !ok {
				break
			}
			start = id + 1
			if !reusableKeyspaceID(id) {
				return nil, errors.New("protected ID in free bitmap")
			}
			region := manager.coveringKeyspaceRegion(id)
			if region == nil || seen[region.GetID()] {
				continue
			}
			seen[region.GetID()] = true
			meta, err := manager.claimFreeKeyspaceID(r, id, name, config, createdAt)
			if err != nil || meta != nil {
				return meta, err
			}
		}
	}
	return nil, nil
}

func (manager *Manager) claimFreeKeyspaceID(r *idReuse, id uint32, name string, config map[string]string, createdAt int64) (*keyspacepb.KeyspaceMeta, error) {
	if manager.mgm != nil {
		manager.mgm.RLock()
		defer manager.mgm.RUnlock()
	}
	manager.metaLock.Lock(id)
	defer manager.metaLock.Unlock(id)
	b := r.bitmap
	b.mu.Lock()
	defer b.mu.Unlock()
	region := manager.coveringKeyspaceRegion(id)
	if !manager.reuseAdmitted(r) || region == nil || !reusableKeyspaceID(id+1) || !regionCoversKeyspace(region.GetMeta(), MakeRegionBound(id+1)) {
		return nil, nil
	}
	first, ok := b.firstFreeLocked(firstRegionKeyspace(region), id+1)
	if !ok || first != id {
		return nil, nil
	}
	meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: name, State: keyspacepb.KeyspaceState_DISABLED, CreatedAt: createdAt, StateChangedAt: createdAt, Config: maps.Clone(config)}
	err := b.updateLocked(r.ctx, id, false, func(txn kv.Txn) error {
		exists, _, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if exists {
			return errs.ErrKeyspaceExists
		}
		old, err := manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if old != nil {
			return errs.ErrEtcdTxnConflict.FastGenByArgs()
		}
		if manager.mgm != nil && manager.mgm.hasGroupsLocked() {
			groupID, err := manager.mgm.findMinMetaGroup(txn)
			if err != nil && !errors.ErrorEqual(err, errNoAvailableMetaServiceGroups) {
				return err
			}
			if err == nil {
				meta.Config[MetaServiceGroupIDKey] = groupID
				if err := manager.mgm.updateAssignmentTxn(txn, "", groupID); err != nil {
					return err
				}
			}
		}
		if err := manager.store.SaveKeyspaceID(txn, id, name); err != nil {
			return err
		}
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if errors.ErrorEqual(err, errs.ErrEtcdTxnConflict) {
		return nil, nil
	}
	if err != nil {
		current, _, readErr := manager.store.LoadKeyspaceMetaWithRevision(r.ctx, id)
		if readErr != nil || !sameReusedKeyspace(current, meta) || current.State != keyspacepb.KeyspaceState_DISABLED {
			return nil, err
		}
	}
	manager.cache.Save(id, name, meta.State)
	manager.keyspaceNameLookup.Store(id, name)
	manager.keyspaceStateLookup.Store(id, meta.State)
	return meta, nil
}

func sameReusedKeyspace(current, expected *keyspacepb.KeyspaceMeta) bool {
	return current != nil && current.Name == expected.Name && current.CreatedAt == expected.CreatedAt && maps.Equal(current.Config, expected.Config)
}

func (manager *Manager) initializeReusedKeyspace(r *idReuse, meta *keyspacepb.KeyspaceMeta) error {
	// Install the ordinary split rule only for the same still-disabled owner.
	err := func() error {
		manager.metaLock.Lock(meta.GetId())
		defer manager.metaLock.Unlock(meta.GetId())
		current, _, err := manager.store.LoadKeyspaceMetaWithRevision(r.ctx, meta.GetId())
		if err != nil {
			return err
		}
		if !sameReusedKeyspace(current, meta) || current.State != keyspacepb.KeyspaceState_DISABLED {
			return errors.New("reused creation was changed or cancelled")
		}
		region := manager.coveringKeyspaceRegion(meta.GetId())
		if region == nil || (bytes.Equal(region.GetStartKey(), MakeRegionBound(meta.GetId()).TxnLeftBound) && bytes.Equal(region.GetEndKey(), MakeRegionBound(meta.GetId()).TxnRightBound)) {
			return errors.New("reused keyspace no longer requires split")
		}
		return manager.splitKeyspaceRegion(meta.GetId(), false, txnRegionBound)
	}()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(manager.config.GetCheckRegionSplitInterval())
	defer ticker.Stop()
	timer := time.NewTimer(manager.config.GetWaitRegionSplitTimeout())
	defer timer.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-timer.C:
			return errs.ErrRegionSplitTimeout
		case <-ticker.C:
			if manager.hasKeyspaceRegionBound(meta.GetId(), txnRegionBound) {
				return nil
			}
		}
	}
}

// finishReusedKeyspace verifies the claimed owner in the final transaction.
// Cancellation never puts the ID back into the bitmap while a split is in flight.
func (manager *Manager) finishReusedKeyspace(r *idReuse, expected *keyspacepb.KeyspaceMeta, state keyspacepb.KeyspaceState) error {
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	if state == keyspacepb.KeyspaceState_ENABLED && manager.kgm != nil {
		if err := manager.kgm.UpdateKeyspaceForGroup(endpoint.StringUserKind(expected.Config[UserKindKey]), expected.Config[TSOKeyspaceGroupIDKey], expected.GetId(), opAdd); err != nil {
			return err
		}
	}
	var finished *keyspacepb.KeyspaceMeta
	err := r.store.RunInTxnWithConditions(r.ctx, nil, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId())
		if err != nil {
			return err
		}
		if !sameReusedKeyspace(meta, expected) || meta.State != keyspacepb.KeyspaceState_DISABLED {
			if state == keyspacepb.KeyspaceState_ARCHIVED {
				return nil
			}
			return errors.New("reused creation changed before completion")
		}
		meta.State = state
		meta.StateChangedAt = time.Now().Unix()
		if err := manager.store.SaveKeyspaceMeta(txn, meta); err != nil {
			return err
		}
		finished = meta
		return nil
	})
	if err == nil && finished != nil {
		expected.StateChangedAt = finished.StateChangedAt
		manager.cache.Save(finished.GetId(), finished.Name, state)
		manager.keyspaceStateLookup.Store(finished.GetId(), state)
	}
	return err
}

// rejectFreeID prevents explicit-ID Create from bypassing the bitmap claim.
// It is used only when reuse is enabled; the comparison joins the metadata txn.
func (manager *Manager) rejectFreeID(txn kv.Txn, id uint32) error {
	r := manager.reuse.Load()
	if r == nil {
		return nil
	}
	width := uint32(r.bitmap.bucketBytes * 8)
	value, err := txn.Load(r.bitmap.bucketKey(id / width))
	if err != nil {
		return err
	}
	if value != "" && (len(value) != r.bitmap.bucketBytes || value[(id%width)/8]&(1<<(id%8)) != 0) {
		return errors.New("free keyspace IDs must be claimed through CreateKeyspace")
	}
	return nil
}
