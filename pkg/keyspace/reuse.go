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
	"maps"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

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
func (manager *Manager) createReusedKeyspace(r *idReuse, request *CreateKeyspaceRequest, tracer *createKeyspaceTracer) (*keyspacepb.KeyspaceMeta, error) {
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
	configFinished := time.Now()
	meta, err := manager.claimFreeKeyspace(r, request.Name, config, request.CreateTime)
	if err != nil || meta == nil {
		return meta, err
	}
	tracer.SetKeyspace(meta.GetId(), meta.Name)
	// Claiming the ID also saves its metadata in one transaction.
	createKeyspaceStepDurationGetConfig.Observe(configFinished.Sub(tracer.lastCheckTime).Seconds())
	tracer.lastCheckTime = configFinished
	tracer.OnAllocateIDFinished()
	err = manager.initializeReusedKeyspace(r, meta)
	if err == nil {
		tracer.OnSplitRegionFinished()
		err = manager.finishReusedKeyspace(r, meta, keyspacepb.KeyspaceState_ENABLED)
	}
	if err != nil {
		if cancelErr := manager.finishReusedKeyspace(r, meta, keyspacepb.KeyspaceState_ARCHIVED); cancelErr != nil {
			return nil, errors.Annotatef(cancelErr, "cancel reused creation: %v", err)
		}
		return nil, err
	}
	tracer.OnEnableKeyspaceFinished()
	tracer.OnCreateKeyspaceComplete()
	meta.State = keyspacepb.KeyspaceState_ENABLED
	if manager.mgm != nil {
		manager.mgm.AttachEndpoints(meta.Config)
	}
	return meta, nil
}

// claimFreeKeyspace is best effort before ownership is persisted. Busy claims,
// loader reads and reclamation writes leave ordinary allocation available.
func (manager *Manager) claimFreeKeyspace(r *idReuse, name string, config map[string]string, createdAt int64) (*keyspacepb.KeyspaceMeta, error) {
	if !r.claimMu.TryLock() {
		return nil, nil
	}
	defer r.claimMu.Unlock()
	candidates := r.bitmap.candidates()
	seen := make(map[uint64]bool)
	for {
		id, ok := candidates.tryNext()
		if !ok {
			return nil, nil
		}
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

func (manager *Manager) claimFreeKeyspaceID(r *idReuse, id uint32, name string, config map[string]string, createdAt int64) (*keyspacepb.KeyspaceMeta, error) {
	if manager.mgm != nil {
		manager.mgm.RLock()
		defer manager.mgm.RUnlock()
	}
	manager.metaLock.Lock(id)
	defer manager.metaLock.Unlock(id)
	b := r.bitmap
	if !b.mu.TryLock() {
		return nil, nil
	}
	defer b.mu.Unlock()
	region := manager.coveringKeyspaceRegion(id)
	if !manager.reuseAdmitted(r) || region == nil || !reusableKeyspaceID(id+1) || !regionCoversKeyspace(region.GetMeta(), MakeRegionBound(id+1)) {
		return nil, nil
	}
	// Lookup releases the lock. Recheck both bits before the bucket CAS.
	// The reserved bucket-end ID makes a bucket-start predecessor always non-free.
	free, known := b.isFreeLocked(id)
	previousFree, previousKnown := b.isFreeLocked(id - 1)
	if !known || !free || !previousKnown || previousFree {
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
	// Match config updates: exclude bulk removal before taking the metadata
	// lock, because removal takes the group lock before inspecting metadata.
	if manager.kgm != nil {
		manager.kgm.membershipMutationLock.Lock()
		defer manager.kgm.membershipMutationLock.Unlock()
	}
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	var groupID uint64
	var group *endpoint.KeyspaceGroup
	if state == keyspacepb.KeyspaceState_ENABLED && manager.kgm != nil {
		var err error
		groupID, err = strconv.ParseUint(expected.Config[TSOKeyspaceGroupIDKey], 10, 64)
		if err != nil {
			return err
		}
	}
	if manager.kgm != nil {
		manager.kgm.Lock()
		defer manager.kgm.Unlock()
	}
	var finished *keyspacepb.KeyspaceMeta
	loadFinishedGroup := func(txn kv.Txn) error {
		group = nil
		if manager.kgm == nil || finished == nil || finished.State != keyspacepb.KeyspaceState_ENABLED {
			return nil
		}
		id, err := strconv.ParseUint(finished.Config[TSOKeyspaceGroupIDKey], 10, 32)
		if err != nil {
			return err
		}
		group, err = manager.kgm.store.LoadKeyspaceGroup(txn, uint32(id))
		return err
	}
	err := r.store.RunInTxnWithConditions(r.ctx, nil, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId())
		if err != nil {
			return err
		}
		if !sameReusedKeyspace(meta, expected) || meta.State != keyspacepb.KeyspaceState_DISABLED {
			if state == keyspacepb.KeyspaceState_ARCHIVED {
				// An uncertain Enable may finish before cancellation gets here.
				// Preserve that result and repair its caches instead of archiving it.
				if sameReusedKeyspace(meta, expected) {
					finished = meta
					return loadFinishedGroup(txn)
				}
				return nil
			}
			return errors.New("reused creation changed before completion")
		}
		// Membership and ENABLED commit together under the same leader guard.
		if state == keyspacepb.KeyspaceState_ENABLED && manager.kgm != nil {
			group, err = manager.kgm.updateKeyspaceForGroupTxn(txn, endpoint.StringUserKind(meta.Config[UserKindKey]), groupID, meta.GetId(), opAdd)
			if err != nil {
				return err
			}
		}
		meta.State = state
		meta.StateChangedAt = time.Now().Unix()
		if err := manager.store.SaveKeyspaceMeta(txn, meta); err != nil {
			return err
		}
		finished = meta
		return nil
	})
	if err != nil {
		if finished == nil {
			return err
		}
		// A lost commit reply must not leave ENABLED metadata with DISABLED
		// caches. Read back while holding the same locks; never publish the
		// staged values until storage confirms them. The request may have expired.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), etcdutil.DefaultRequestTimeout)
		defer cancel()
		readErr := manager.store.RunInTxn(ctx, func(txn kv.Txn) error {
			var readErr error
			finished, readErr = manager.store.LoadKeyspaceMeta(txn, expected.GetId())
			if readErr != nil {
				return readErr
			}
			return loadFinishedGroup(txn)
		})
		if readErr != nil {
			manager.evictKeyspacesFromCache([]uint32{expected.GetId()})
			return errors.Annotatef(readErr, "reconcile reused creation after %v", err)
		}
		if sameReusedKeyspace(finished, expected) && finished.State == state {
			err = nil
		}
	}
	if group != nil {
		manager.kgm.putKeyspaceGroupToCacheLocked(group)
	}
	if finished == nil && err != nil {
		manager.evictKeyspacesFromCache([]uint32{expected.GetId()})
	}
	if finished != nil {
		expected.StateChangedAt = finished.StateChangedAt
		manager.cache.Save(finished.GetId(), finished.Name, finished.State)
		manager.keyspaceNameLookup.Store(finished.GetId(), finished.Name)
		manager.keyspaceStateLookup.Store(finished.GetId(), finished.State)
	}
	return err
}
