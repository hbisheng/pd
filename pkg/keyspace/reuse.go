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
	"encoding/binary"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

const (
	// MinimumKeyspaceReuseTombstoneDuration is the mandatory quarantine after
	// Cleaner finishes and records TOMBSTONE.
	MinimumKeyspaceReuseTombstoneDuration = 30 * 24 * time.Hour
	// KeyspaceGenerationKey and KeyspaceRestoreVersionKey are PD-owned config
	// entries that preserve the prepared lineage across a create/delete cycle.
	KeyspaceGenerationKey     = "__pd_keyspace_generation"
	KeyspaceRestoreVersionKey = "__pd_keyspace_restore_version"

	freeIDScanLimit = 32
)

type resetIntent struct {
	Purpose                pdpb.KeyspaceResetPurpose `json:"purpose"`
	KeyspaceID             uint32                    `json:"keyspace_id"`
	Name                   string                    `json:"name"`
	Config                 map[string]string         `json:"config,omitempty"`
	TombstoneChangedAt     int64                     `json:"tombstone_changed_at,omitempty"`
	ExpectedGeneration     uint64                    `json:"expected_generation"`
	Generation             uint64                    `json:"generation"`
	ExpectedRestoreVersion uint64                    `json:"expected_restore_version"`
	RestoreVersion         uint64                    `json:"restore_version"`
	EncryptionMetaFileID   uint64                    `json:"encryption_meta_file_id,omitempty"`
	RegionID               uint64                    `json:"region_id"`
	RegionVersion          uint64                    `json:"region_version"`
	ConfVer                uint64                    `json:"conf_ver"`
	Reused                 bool                      `json:"reused"`
}

func (intent *resetIntent) command() *pdpb.KeyspaceReset {
	return &pdpb.KeyspaceReset{
		Purpose:                intent.Purpose,
		KeyspaceId:             intent.KeyspaceID,
		ExpectedGeneration:     intent.ExpectedGeneration,
		Generation:             intent.Generation,
		ExpectedRestoreVersion: intent.ExpectedRestoreVersion,
		RestoreVersion:         intent.RestoreVersion,
		EncryptionMetaFileId:   intent.EncryptionMetaFileID,
	}
}

func marshalResetIntent(intent *resetIntent) (string, error) {
	value, err := json.Marshal(intent)
	return string(value), err
}

func unmarshalResetIntent(value string) (*resetIntent, error) {
	intent := &resetIntent{}
	if err := json.Unmarshal([]byte(value), intent); err != nil {
		return nil, err
	}
	return intent, nil
}

func encodeFreeID(generation, restoreVersion uint64) string {
	value := make([]byte, 16)
	binary.BigEndian.PutUint64(value[:8], generation)
	binary.BigEndian.PutUint64(value[8:], restoreVersion)
	return string(value)
}

func decodeFreeID(value string) (uint64, uint64, error) {
	if len(value) != 16 {
		return 0, 0, errors.Errorf("invalid free keyspace record length %d", len(value))
	}
	return binary.BigEndian.Uint64([]byte(value[:8])), binary.BigEndian.Uint64([]byte(value[8:])), nil
}

func (manager *Manager) loadResetIntents() error {
	loaded := make(map[uint32]*resetIntent)
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		prefix := keypath.KeyspaceResetPrefix()
		_, values, err := txn.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), 0)
		if err != nil {
			return err
		}
		for _, value := range values {
			intent, err := unmarshalResetIntent(value)
			if err != nil {
				return err
			}
			loaded[intent.KeyspaceID] = intent
		}
		return nil
	})
	if err != nil {
		return err
	}
	manager.resetMu.Lock()
	manager.resets = loaded
	manager.resetMu.Unlock()
	return nil
}

func (manager *Manager) loadResetIntent(txn kv.Txn, id uint32) (*resetIntent, string, error) {
	value, err := txn.Load(keypath.KeyspaceResetPath(id))
	if err != nil || value == "" {
		return nil, value, err
	}
	intent, err := unmarshalResetIntent(value)
	return intent, value, err
}

func (manager *Manager) cacheResetIntent(intent *resetIntent) {
	manager.resetMu.Lock()
	manager.resets[intent.KeyspaceID] = intent
	manager.resetMu.Unlock()
}

func (manager *Manager) removeCachedResetIntent(id uint32) {
	manager.resetMu.Lock()
	delete(manager.resets, id)
	manager.resetMu.Unlock()
}

func (manager *Manager) exactKeyspaceRegion(id uint32) (*core.RegionInfo, error) {
	if manager.cluster == nil {
		return nil, errors.New("raft cluster is unavailable")
	}
	bound := MakeRegionBound(id)
	regions := manager.cluster.GetBasicCluster().ScanRegions(bound.TxnLeftBound, bound.TxnRightBound, 2)
	if len(regions) != 1 {
		return nil, errors.Errorf("keyspace %d must have exactly one transaction Region, got %d", id, len(regions))
	}
	region := regions[0]
	if !bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) || !bytes.Equal(region.GetEndKey(), bound.TxnRightBound) {
		return nil, errors.Errorf("region %d does not exactly cover transaction keyspace %d", region.GetID(), id)
	}
	return region, nil
}

func (manager *Manager) rejectIndependentScheduling() error {
	if cluster, ok := manager.cluster.(interface{ IsServiceIndependent(string) bool }); ok &&
		cluster.IsServiceIndependent(constant.SchedulingServiceName) {
		return errors.New("keyspace ID reuse is unsupported with independent scheduling service")
	}
	return nil
}

// RecordKeyspaceResetStatuses replaces one store's ephemeral applied-status
// snapshot. Reports are deliberately not durable and are recollected after a
// PD restart.
func (manager *Manager) RecordKeyspaceResetStatuses(storeID uint64, statuses []*pdpb.KeyspaceResetStatus) {
	current := make(map[uint64]*pdpb.KeyspaceResetStatus, len(statuses))
	for _, status := range statuses {
		region := manager.cluster.GetBasicCluster().GetRegion(status.GetRegionId())
		if region == nil || region.GetRegionEpoch().GetVersion() != status.GetRegionVersion() ||
			region.GetRegionEpoch().GetConfVer() != status.GetConfVer() {
			continue
		}
		for _, peer := range region.GetPeers() {
			if peer.GetId() == status.GetPeerId() && peer.GetStoreId() == storeID {
				current[peer.GetId()] = status
				break
			}
		}
	}
	manager.resetStatusMu.Lock()
	manager.resetStatuses[storeID] = current
	manager.resetStatusMu.Unlock()
	manager.tryFinalizeResets()
}

func (manager *Manager) appliedState(region *core.RegionInfo, keyspaceID uint32) (uint64, uint64, error) {
	manager.resetStatusMu.RLock()
	defer manager.resetStatusMu.RUnlock()
	var generation, restoreVersion uint64
	for i, peer := range region.GetPeers() {
		status := manager.resetStatuses[peer.GetStoreId()][peer.GetId()]
		if status == nil || status.GetRegionId() != region.GetID() || status.GetKeyspaceId() != keyspaceID ||
			status.GetRegionVersion() != region.GetRegionEpoch().GetVersion() ||
			status.GetConfVer() != region.GetRegionEpoch().GetConfVer() {
			return 0, 0, errors.Errorf("missing current applied status for peer %d", peer.GetId())
		}
		if i == 0 {
			generation = status.GetGeneration()
			restoreVersion = status.GetRestoreVersion()
			continue
		}
		if generation != status.GetGeneration() || restoreVersion != status.GetRestoreVersion() {
			return 0, 0, errors.Errorf("keyspace %d peers disagree on applied lineage", keyspaceID)
		}
	}
	return generation, restoreVersion, nil
}

func (manager *Manager) resetApplied(intent *resetIntent) bool {
	region, err := manager.exactKeyspaceRegion(intent.KeyspaceID)
	appliedRegionVersion := intent.RegionVersion + 1
	if err != nil || region.GetID() != intent.RegionID ||
		region.GetRegionEpoch().GetVersion() != appliedRegionVersion ||
		region.GetRegionEpoch().GetConfVer() != intent.ConfVer {
		return false
	}
	manager.resetStatusMu.RLock()
	defer manager.resetStatusMu.RUnlock()
	for _, peer := range region.GetPeers() {
		status := manager.resetStatuses[peer.GetStoreId()][peer.GetId()]
		if status == nil || status.GetRegionId() != intent.RegionID ||
			status.GetKeyspaceId() != intent.KeyspaceID || status.GetGeneration() != intent.Generation ||
			status.GetRestoreVersion() != intent.RestoreVersion ||
			status.GetRegionVersion() != appliedRegionVersion || status.GetConfVer() != intent.ConfVer {
			return false
		}
		if intent.Purpose == pdpb.KeyspaceResetPurpose_CREATE &&
			(status.GetEncryptionMetaFileId() != intent.EncryptionMetaFileID || !status.GetKeyReady()) {
			return false
		}
	}
	return true
}

// KeyspaceResetForRegion returns an intent only for the immutable Region and
// epoch recorded by PD. A topology change therefore stops replay automatically.
func (manager *Manager) KeyspaceResetForRegion(region *core.RegionInfo) *pdpb.KeyspaceReset {
	if manager.rejectIndependentScheduling() != nil {
		return nil
	}
	manager.resetMu.RLock()
	for _, intent := range manager.resets {
		if !intent.Reused && intent.Purpose == pdpb.KeyspaceResetPurpose_CREATE {
			continue
		}
		if intent.RegionID == region.GetID() && intent.RegionVersion == region.GetRegionEpoch().GetVersion() &&
			intent.ConfVer == region.GetRegionEpoch().GetConfVer() {
			command := intent.command()
			manager.resetMu.RUnlock()
			return command
		}
	}
	manager.resetMu.RUnlock()
	// RemoveKeyspace participates in its caller's larger transaction, so it
	// cannot safely publish to memory before that transaction commits. Refresh
	// lazily on the first matching Region heartbeat.
	if manager.loadResetIntents() == nil {
		manager.resetMu.RLock()
		defer manager.resetMu.RUnlock()
		for _, intent := range manager.resets {
			if intent.RegionID == region.GetID() && intent.RegionVersion == region.GetRegionEpoch().GetVersion() &&
				intent.ConfVer == region.GetRegionEpoch().GetConfVer() {
				return intent.command()
			}
		}
	}
	return nil
}

func cloneConfig(config map[string]string) map[string]string {
	cloned := make(map[string]string, len(config)+2)
	for key, value := range config {
		cloned[key] = value
	}
	return cloned
}

func configWithoutLineage(config map[string]string) map[string]string {
	cloned := cloneConfig(config)
	delete(cloned, KeyspaceGenerationKey)
	delete(cloned, KeyspaceRestoreVersionKey)
	delete(cloned, MetaServiceGroupIDKey)
	delete(cloned, MetaServiceGroupAddressesKey)
	return cloned
}

func freeIDFromPath(path string) (uint32, error) {
	id := strings.TrimPrefix(path, keypath.KeyspaceFreePrefix())
	value, err := strconv.ParseUint(id, 10, 32)
	return uint32(value), err
}

func (manager *Manager) claimFreeKeyspace(name string, config map[string]string, createTime int64) (*keyspacepb.KeyspaceMeta, *resetIntent, error) {
	var created *keyspacepb.KeyspaceMeta
	var intent *resetIntent
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		prefix := keypath.KeyspaceFreePrefix()
		keys, values, err := txn.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), freeIDScanLimit)
		if err != nil || len(keys) == 0 {
			return err
		}
		if err := manager.rejectIndependentScheduling(); err != nil {
			return err
		}
		if !isCMEKCreate(config) {
			return errors.New("reused keyspace IDs require an enabled CMEK configuration")
		}
		id, err := freeIDFromPath(keys[0])
		if err != nil {
			return err
		}
		generation, restoreFloor, err := decodeFreeID(values[0])
		if err != nil {
			return err
		}
		if restoreFloor == ^uint64(0) {
			return errors.New("keyspace restore version is exhausted")
		}
		region, err := manager.exactKeyspaceRegion(id)
		if err != nil {
			return err
		}
		nameExists, _, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if nameExists {
			return errs.ErrKeyspaceExists
		}
		if meta, err := manager.store.LoadKeyspaceMeta(txn, id); err != nil || meta != nil {
			if err != nil {
				return err
			}
			return errors.Errorf("free keyspace %d still has metadata", id)
		}
		newConfig := cloneConfig(config)
		newConfig[KeyspaceGenerationKey] = strconv.FormatUint(generation, 10)
		newConfig[KeyspaceRestoreVersionKey] = strconv.FormatUint(restoreFloor+1, 10)
		created = &keyspacepb.KeyspaceMeta{
			Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: id},
			Name:           name,
			State:          keyspacepb.KeyspaceState_DISABLED,
			CreatedAt:      createTime,
			StateChangedAt: createTime,
			Config:         newConfig,
		}
		intent = &resetIntent{
			Purpose:                pdpb.KeyspaceResetPurpose_CREATE,
			KeyspaceID:             id,
			Name:                   name,
			Config:                 cloneConfig(newConfig),
			ExpectedGeneration:     generation,
			Generation:             generation,
			ExpectedRestoreVersion: restoreFloor,
			RestoreVersion:         restoreFloor + 1,
			RegionID:               region.GetID(),
			RegionVersion:          region.GetRegionEpoch().GetVersion(),
			ConfVer:                region.GetRegionEpoch().GetConfVer(),
			Reused:                 true,
		}
		intentValue, err := marshalResetIntent(intent)
		if err != nil {
			return err
		}
		if err := manager.store.SaveKeyspaceID(txn, id, name); err != nil {
			return err
		}
		if err := manager.store.SaveKeyspaceMeta(txn, created); err != nil {
			return err
		}
		if err := txn.Save(keypath.KeyspaceResetPath(id), intentValue); err != nil {
			return err
		}
		return txn.Remove(keys[0])
	})
	if err != nil || created == nil {
		return created, intent, err
	}
	manager.cacheResetIntent(intent)
	manager.keyspaceNameLookup.Store(created.GetId(), created.GetName())
	manager.keyspaceStateLookup.Store(created.GetId(), created.GetState())
	return created, intent, nil
}

func (manager *Manager) saveFreshPendingKeyspace(keyspace *keyspacepb.KeyspaceMeta) (*resetIntent, error) {
	intent := &resetIntent{
		Purpose:    pdpb.KeyspaceResetPurpose_CREATE,
		KeyspaceID: keyspace.GetId(),
		Name:       keyspace.GetName(),
		Config:     cloneConfig(keyspace.GetConfig()),
	}
	value, err := marshalResetIntent(intent)
	if err != nil {
		return nil, err
	}
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		nameExists, _, err := manager.store.LoadKeyspaceID(txn, keyspace.GetName())
		if err != nil {
			return err
		}
		if nameExists {
			return errs.ErrKeyspaceExists
		}
		meta, err := manager.store.LoadKeyspaceMeta(txn, keyspace.GetId())
		if err != nil {
			return err
		}
		if meta != nil {
			return errs.ErrKeyspaceExists
		}
		if err := manager.store.SaveKeyspaceID(txn, keyspace.GetId(), keyspace.GetName()); err != nil {
			return err
		}
		if err := manager.store.SaveKeyspaceMeta(txn, keyspace); err != nil {
			return err
		}
		return txn.Save(keypath.KeyspaceResetPath(keyspace.GetId()), value)
	})
	if err != nil {
		return nil, err
	}
	manager.cacheResetIntent(intent)
	manager.keyspaceNameLookup.Store(keyspace.GetId(), keyspace.GetName())
	manager.keyspaceStateLookup.Store(keyspace.GetId(), keyspace.GetState())
	return intent, nil
}

func (manager *Manager) updatePendingCreateRegion(intent *resetIntent) error {
	region, err := manager.exactKeyspaceRegion(intent.KeyspaceID)
	if err != nil {
		return err
	}
	updated := *intent
	updated.RegionID = region.GetID()
	updated.RegionVersion = region.GetRegionEpoch().GetVersion()
	updated.ConfVer = region.GetRegionEpoch().GetConfVer()
	value, err := marshalResetIntent(&updated)
	if err != nil {
		return err
	}
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		current, _, err := manager.loadResetIntent(txn, intent.KeyspaceID)
		if err != nil {
			return err
		}
		if current == nil || current.Name != intent.Name || current.Reused {
			return errors.New("pending keyspace create intent changed")
		}
		return txn.Save(keypath.KeyspaceResetPath(intent.KeyspaceID), value)
	})
	if err != nil {
		return err
	}
	*intent = updated
	manager.cacheResetIntent(intent)
	return nil
}

func (manager *Manager) resumePendingCreate(name string, requestedConfig map[string]string) (*keyspacepb.KeyspaceMeta, *resetIntent, error) {
	var meta *keyspacepb.KeyspaceMeta
	var intent *resetIntent
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		exists, id, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil || !exists {
			return err
		}
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		intent, _, err = manager.loadResetIntent(txn, id)
		if err != nil {
			return err
		}
		if meta == nil || intent == nil || intent.Purpose != pdpb.KeyspaceResetPurpose_CREATE ||
			meta.GetState() != keyspacepb.KeyspaceState_DISABLED ||
			!reflect.DeepEqual(configWithoutLineage(meta.GetConfig()), configWithoutLineage(requestedConfig)) {
			return errs.ErrKeyspaceExists
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if meta == nil {
		return nil, nil, nil
	}
	manager.cacheResetIntent(intent)
	return meta, intent, nil
}

// StartKeyspaceReclaim persists the fenced empty-reset intent in the caller's
// transaction. The old identity remains TOMBSTONE until every current peer has
// reported the applied reset.
func (manager *Manager) StartKeyspaceReclaim(txn kv.Txn, id uint32) error {
	if err := manager.rejectIndependentScheduling(); err != nil {
		return err
	}
	if isProtectedKeyspaceID(id) {
		return newModifyProtectedKeyspaceError()
	}
	meta, err := manager.store.LoadKeyspaceMeta(txn, id)
	if err != nil {
		return err
	}
	if meta == nil {
		return errs.ErrKeyspaceNotFound
	}
	if meta.GetState() != keyspacepb.KeyspaceState_TOMBSTONE {
		return errors.Errorf("cannot reclaim keyspace in state %s", meta.GetState().String())
	}
	if manager.now().Unix()-meta.GetStateChangedAt() < int64(MinimumKeyspaceReuseTombstoneDuration/time.Second) {
		return errors.Errorf("keyspace %d has not completed the %s tombstone quarantine", id, MinimumKeyspaceReuseTombstoneDuration)
	}
	if existing, _, err := manager.loadResetIntent(txn, id); err != nil || existing != nil {
		return err
	}
	// Re-establish the transaction-keyspace boundary removed by Cleaner and
	// retain it while the ID is free, so the prepared empty Region cannot merge
	// into an adjacent keyspace before Create claims it.
	if labeler := manager.cluster.GetRegionLabeler(); labeler != nil {
		if err := labeler.SetLabelRule(buildLabelRule(id, txnRegionBound)); err != nil {
			return err
		}
	}
	region, err := manager.exactKeyspaceRegion(id)
	if err != nil {
		return err
	}
	generation, restoreVersion, err := manager.appliedState(region, id)
	if err != nil {
		return err
	}
	metaGeneration, metaRestoreVersion, err := parseLineage(meta.GetConfig())
	if err != nil {
		return err
	}
	if (meta.GetConfig()[KeyspaceGenerationKey] != "" && metaGeneration != generation) ||
		(meta.GetConfig()[KeyspaceRestoreVersionKey] != "" && metaRestoreVersion != restoreVersion) {
		return errors.New("keyspace metadata lineage does not match applied peer state")
	}
	if generation == ^uint64(0) || restoreVersion == ^uint64(0) {
		return errors.New("keyspace reset lineage is exhausted")
	}
	intent := &resetIntent{
		Purpose:                pdpb.KeyspaceResetPurpose_RECLAIM,
		KeyspaceID:             id,
		Name:                   meta.GetName(),
		TombstoneChangedAt:     meta.GetStateChangedAt(),
		ExpectedGeneration:     generation,
		Generation:             generation + 1,
		ExpectedRestoreVersion: restoreVersion,
		RestoreVersion:         restoreVersion + 1,
		RegionID:               region.GetID(),
		RegionVersion:          region.GetRegionEpoch().GetVersion(),
		ConfVer:                region.GetRegionEpoch().GetConfVer(),
	}
	value, err := marshalResetIntent(intent)
	if err != nil {
		return err
	}
	return txn.Save(keypath.KeyspaceResetPath(id), value)
}

// SelectKeyspaceEncryptionMetaFile performs the single-winner CAS for a CSE
// candidate file. Losing candidates remain unused and callers receive the
// already selected file ID.
func (manager *Manager) SelectKeyspaceEncryptionMetaFile(id uint32, generation, restoreVersion, candidate uint64) (uint64, error) {
	if candidate == 0 {
		return 0, errors.New("encryption metadata candidate file ID must be non-zero")
	}
	var selected uint64
	var updated *resetIntent
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		intent, _, err := manager.loadResetIntent(txn, id)
		if err != nil {
			return err
		}
		if intent == nil || intent.Purpose != pdpb.KeyspaceResetPurpose_CREATE || !intent.Reused ||
			intent.Generation != generation || intent.RestoreVersion != restoreVersion {
			return errors.New("no matching pending reused-keyspace create")
		}
		if intent.EncryptionMetaFileID != 0 {
			selected = intent.EncryptionMetaFileID
			return nil
		}
		intent.EncryptionMetaFileID = candidate
		value, err := marshalResetIntent(intent)
		if err != nil {
			return err
		}
		if err := txn.Save(keypath.KeyspaceResetPath(id), value); err != nil {
			return err
		}
		selected = candidate
		updated = intent
		return nil
	})
	if err == nil && updated != nil {
		manager.cacheResetIntent(updated)
	}
	return selected, err
}

func (manager *Manager) tryFinalizeResets() {
	manager.resetMu.RLock()
	intents := make([]*resetIntent, 0, len(manager.resets))
	for _, intent := range manager.resets {
		copy := *intent
		intents = append(intents, &copy)
	}
	manager.resetMu.RUnlock()
	for _, intent := range intents {
		applied := manager.resetApplied(intent)
		if intent.Purpose == pdpb.KeyspaceResetPurpose_CREATE && !intent.Reused {
			applied = manager.freshCreateSplitReady(intent) &&
				(!isCMEKCreate(intent.Config) || manager.freshEncryptedCreateReady(intent))
		}
		if !applied {
			continue
		}
		if intent.Purpose == pdpb.KeyspaceResetPurpose_RECLAIM {
			_ = manager.finishReclaim(intent)
		} else {
			_ = manager.finishCreate(intent)
		}
	}
}

func (manager *Manager) finishReclaim(expected *resetIntent) error {
	manager.metaLock.Lock(expected.KeyspaceID)
	defer manager.metaLock.Unlock(expected.KeyspaceID)
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		intent, value, err := manager.loadResetIntent(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		if intent == nil {
			return errors.New("keyspace reclaim intent is no longer active")
		}
		expectedValue, err := marshalResetIntent(expected)
		if err != nil {
			return err
		}
		if value != expectedValue {
			return errors.New("keyspace reclaim intent changed")
		}
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		if meta == nil || meta.GetName() != expected.Name || meta.GetState() != keyspacepb.KeyspaceState_TOMBSTONE ||
			meta.GetStateChangedAt() != expected.TombstoneChangedAt {
			return errors.New("tombstone identity changed during keyspace reclaim")
		}
		if current, err := txn.Load(keypath.KeyspaceFreePath(expected.KeyspaceID)); err != nil || current != "" {
			if err != nil {
				return err
			}
			return errors.New("free keyspace record already exists")
		}
		if err := manager.store.RemoveKeyspace(txn, expected.KeyspaceID, expected.Name); err != nil {
			return err
		}
		if err := txn.Remove(keypath.KeyspaceResetPath(expected.KeyspaceID)); err != nil {
			return err
		}
		return txn.Save(keypath.KeyspaceFreePath(expected.KeyspaceID), encodeFreeID(expected.Generation, expected.RestoreVersion))
	})
	if err != nil {
		return err
	}
	manager.removeCachedResetIntent(expected.KeyspaceID)
	manager.keyspaceNameLookup.Delete(expected.KeyspaceID)
	manager.keyspaceStateLookup.Delete(expected.KeyspaceID)
	return nil
}

func (manager *Manager) finishCreate(expected *resetIntent) error {
	manager.metaLock.Lock(expected.KeyspaceID)
	defer manager.metaLock.Unlock(expected.KeyspaceID)
	newlyFinished := false
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		intent, value, err := manager.loadResetIntent(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		if intent == nil {
			meta, loadErr := manager.store.LoadKeyspaceMeta(txn, expected.KeyspaceID)
			if loadErr != nil {
				return loadErr
			}
			if meta != nil && meta.GetName() == expected.Name && meta.GetState() == keyspacepb.KeyspaceState_ENABLED &&
				reflect.DeepEqual(meta.GetConfig(), expected.Config) {
				return nil
			}
			return errors.New("keyspace create intent is no longer active")
		}
		expectedValue, err := marshalResetIntent(expected)
		if err != nil {
			return err
		}
		if value != expectedValue {
			return errors.New("keyspace create intent changed")
		}
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		if meta == nil || meta.GetName() != expected.Name || meta.GetState() != keyspacepb.KeyspaceState_DISABLED ||
			!reflect.DeepEqual(meta.GetConfig(), expected.Config) {
			return errors.New("pending keyspace identity or config changed")
		}
		meta.State = keyspacepb.KeyspaceState_ENABLED
		meta.StateChangedAt = manager.now().Unix()
		if err := manager.store.SaveKeyspaceMeta(txn, meta); err != nil {
			return err
		}
		newlyFinished = true
		return txn.Remove(keypath.KeyspaceResetPath(expected.KeyspaceID))
	})
	if err != nil {
		return err
	}
	if !newlyFinished {
		return nil
	}
	manager.removeCachedResetIntent(expected.KeyspaceID)
	manager.keyspaceStateLookup.Store(expected.KeyspaceID, keyspacepb.KeyspaceState_ENABLED)
	userKind := endpointUserKind(expected.Config)
	if manager.kgm != nil {
		if err := manager.kgm.UpdateKeyspaceForGroup(userKind, expected.Config[TSOKeyspaceGroupIDKey], expected.KeyspaceID, opAdd); err != nil {
			return err
		}
	}
	return nil
}

func endpointUserKind(config map[string]string) endpoint.UserKind {
	return endpoint.StringUserKind(config[UserKindKey])
}

func (manager *Manager) waitReset(intent *resetIntent) error {
	ticker := time.NewTicker(manager.config.GetCheckRegionSplitInterval())
	timer := time.NewTimer(manager.config.GetWaitRegionSplitTimeout())
	defer ticker.Stop()
	defer timer.Stop()
	for {
		manager.resetMu.RLock()
		current := manager.resets[intent.KeyspaceID]
		if current != nil {
			copy := *current
			current = &copy
		}
		manager.resetMu.RUnlock()
		if current == nil {
			return manager.finishCreate(intent)
		}
		if manager.resetApplied(current) {
			return manager.finishCreate(current)
		}
		select {
		case <-manager.ctx.Done():
			return errors.New("waiting for keyspace reset was canceled")
		case <-ticker.C:
		case <-timer.C:
			return errors.New("timed out waiting for all keyspace peers to apply the reset")
		}
	}
}

func (manager *Manager) freshEncryptedCreateReady(intent *resetIntent) bool {
	region, err := manager.exactKeyspaceRegion(intent.KeyspaceID)
	if err != nil || region.GetID() != intent.RegionID ||
		region.GetRegionEpoch().GetVersion() != intent.RegionVersion ||
		region.GetRegionEpoch().GetConfVer() != intent.ConfVer {
		return false
	}
	manager.resetStatusMu.RLock()
	defer manager.resetStatusMu.RUnlock()
	var encryptionFileID uint64
	for _, peer := range region.GetPeers() {
		status := manager.resetStatuses[peer.GetStoreId()][peer.GetId()]
		if status == nil || status.GetRegionId() != region.GetID() || status.GetKeyspaceId() != intent.KeyspaceID ||
			status.GetRegionVersion() != intent.RegionVersion || status.GetConfVer() != intent.ConfVer ||
			status.GetEncryptionMetaFileId() == 0 || !status.GetKeyReady() {
			return false
		}
		if encryptionFileID == 0 {
			encryptionFileID = status.GetEncryptionMetaFileId()
		} else if encryptionFileID != status.GetEncryptionMetaFileId() {
			return false
		}
	}
	return true
}

func (manager *Manager) freshCreateSplitReady(intent *resetIntent) bool {
	if intent.RegionID == 0 {
		return false
	}
	region, err := manager.exactKeyspaceRegion(intent.KeyspaceID)
	return err == nil && region.GetID() == intent.RegionID &&
		region.GetRegionEpoch().GetVersion() == intent.RegionVersion &&
		region.GetRegionEpoch().GetConfVer() == intent.ConfVer
}

func (manager *Manager) waitFreshEncryptedCreate(intent *resetIntent) error {
	ticker := time.NewTicker(manager.config.GetCheckRegionSplitInterval())
	timer := time.NewTimer(manager.config.GetWaitRegionSplitTimeout())
	defer ticker.Stop()
	defer timer.Stop()
	for {
		if manager.freshEncryptedCreateReady(intent) {
			return manager.finishCreate(intent)
		}
		select {
		case <-manager.ctx.Done():
			return errors.New("waiting for encrypted keyspace readiness was canceled")
		case <-ticker.C:
		case <-timer.C:
			return errors.New("timed out waiting for every keyspace peer to load its encryption key")
		}
	}
}

func isCMEKCreate(config map[string]string) bool {
	var encryption struct {
		Enabled bool `json:"enabled"`
	}
	return json.Unmarshal([]byte(config["encryption"]), &encryption) == nil && encryption.Enabled
}

func parseLineage(config map[string]string) (uint64, uint64, error) {
	generation, err := strconv.ParseUint(config[KeyspaceGenerationKey], 10, 64)
	if config[KeyspaceGenerationKey] == "" {
		generation, err = 0, nil
	}
	if err != nil {
		return 0, 0, errors.Wrap(err, "invalid keyspace generation")
	}
	restoreVersion, err := strconv.ParseUint(config[KeyspaceRestoreVersionKey], 10, 64)
	if config[KeyspaceRestoreVersionKey] == "" {
		restoreVersion, err = 0, nil
	}
	return generation, restoreVersion, errors.Wrap(err, "invalid keyspace restore version")
}

func isKeyspaceReuseConfigKey(key string) bool {
	return key == KeyspaceGenerationKey || key == KeyspaceRestoreVersionKey
}
