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
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcsconstant "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	freeIDScanLimit        = 32
	reuseProtocolVersion   = 6
	nameChangeHistoryLimit = 1024
)

type nameChange struct {
	revision uint64
	id       uint32
}

// A split grant uses IDs already allocated by AskBatchSplit. TiKV reads the
// keyspace configuration after receiving this grant, then reports applied children
// through ReportBatchSplit. This evidence is deliberately not persisted: a lost
// creation attempt is canceled rather than reconstructed from Region boundaries.
type reuseSplitGrant struct {
	source    *metapb.Region
	children  map[uint64]struct{}
	confirmed bool
}

type reuseState struct {
	mu           sync.RWMutex
	pending      map[uint32]*keyspacepb.KeyspaceMeta
	grants       map[uint32]*reuseSplitGrant
	capable      map[uint64]bool
	watermark    uint64
	scanStart    string
	nameRevision uint64
	nameChanges  []nameChange
	now          func() time.Time
}

func newReuseState() *reuseState {
	return &reuseState{pending: make(map[uint32]*keyspacepb.KeyspaceMeta), grants: make(map[uint32]*reuseSplitGrant), capable: make(map[uint64]bool), now: time.Now}
}

func cloneKeyspaceConfig(config map[string]string) map[string]string {
	result := make(map[string]string, len(config))
	for k, v := range config {
		result[k] = v
	}
	return result
}

// isReuseInitializing protects creation until the existing split flow completes.
func isReuseInitializing(meta *keyspacepb.KeyspaceMeta) bool {
	return meta != nil && meta.State == keyspacepb.KeyspaceState_DISABLED && meta.PendingInitialization
}

func reusableKeyspaceID(id uint32) bool {
	return id != constant.DefaultKeyspaceID && uint64(id) < constant.ReservedKeyspaceIDStart
}
func (manager *Manager) transactionalReuseConfig(config map[string]string) bool {
	bound := config[RegionBoundType]
	if bound == "" {
		return manager.getRegionBoundType() == txnRegionBound
	}
	return bound == txnRegionBound.String()
}

// Reuse is limited to plaintext and Starter legacy encryption. CMEK requests
// retain the existing increasing-ID and split initialization path.
func (manager *Manager) supportsReuseEncryption(config map[string]string) bool {
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
	return !value.Enabled || (manager.config.IsEncryptedIDReuseEnabled() && value.CMEKID == "" && value.Vendor == "")
}
func (manager *Manager) cacheReuseMeta(meta *keyspacepb.KeyspaceMeta) {
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	if isReuseInitializing(meta) {
		copy := *meta
		copy.Config = cloneKeyspaceConfig(meta.Config)
		manager.reuse.pending[meta.GetId()] = &copy
	} else {
		delete(manager.reuse.pending, meta.GetId())
		delete(manager.reuse.grants, meta.GetId())
	}
	manager.keyspaceStateLookup.Store(meta.GetId(), meta.State)
}

// StartIDReuse reloads allocator/cache revisions and cancels interrupted creations.
// Without the original split grant, boundaries alone cannot prove initialization.
func (manager *Manager) StartIDReuse() error {
	// The allocator's in-memory window starts at zero after restart. Read its
	// existing durable counter once, rather than requiring a new allocation.
	var watermark, nameRevision uint64
	if err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		var err error
		nameRevision, err = loadNameRevision(txn)
		if err != nil {
			return err
		}
		value, err := txn.Load(keypath.KeyspaceAllocIDPath())
		if err != nil || value == "" {
			return err
		}
		watermark, err = typeutil.BytesToUint64([]byte(value))
		return err
	}); err != nil {
		return err
	}
	manager.reuse.mu.Lock()
	manager.reuse.nameRevision = nameRevision
	manager.reuse.nameChanges = nil
	manager.reuse.watermark = watermark
	manager.reuse.scanStart = bitmapPrefix()
	manager.reuse.pending = make(map[uint32]*keyspacepb.KeyspaceMeta)
	manager.reuse.grants = make(map[uint32]*reuseSplitGrant)
	manager.reuse.capable = make(map[uint64]bool)
	manager.reuse.mu.Unlock()
	if !manager.config.IsIDReuseEnabled() {
		return nil
	}
	var start uint32
	for {
		metas, err := manager.LoadRangeKeyspace(start, etcdutil.MaxEtcdTxnOps)
		if err != nil {
			return err
		}
		for _, meta := range metas {
			if isReuseInitializing(meta) {
				if err := manager.cancelReusedCreate(meta); err != nil {
					return err
				}
			}
		}
		if len(metas) < etcdutil.MaxEtcdTxnOps {
			break
		}
		start = metas[len(metas)-1].GetId() + 1
	}
	return manager.ensureBitmapLayout()
}

// RecordIDReuseCapability rejects stores speaking the earlier POC protocol.
func (manager *Manager) RecordIDReuseCapability(storeID uint64, version uint32) {
	manager.reuse.mu.Lock()
	manager.reuse.capable[storeID] = version >= reuseProtocolVersion
	manager.reuse.mu.Unlock()
}

// RecordReuseSplitGrant records only grants issued while this creation is pending.
// TiKV protocol v6 reads keyspace metadata after AskBatchSplit returns; older
// grants therefore cannot initialize a newly claimed ID.
func (manager *Manager) RecordReuseSplitGrant(source *metapb.Region, splits []*pdpb.SplitID) {
	if source == nil || len(splits) == 0 {
		return
	}
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	for id := range manager.reuse.pending {
		bound := MakeRegionBound(id)
		if !regionCoversKeyspace(source, bound) || regionMatchesKeyspace(source, bound) {
			continue
		}
		children := make(map[uint64]struct{}, len(splits)+1)
		children[source.GetId()] = struct{}{}
		for _, split := range splits {
			children[split.GetNewRegionId()] = struct{}{}
		}
		manager.reuse.grants[id] = &reuseSplitGrant{source: typeutil.DeepClone(source, core.RegionFactory), children: children}
	}
}

// RecordReuseSplitResult accepts the applied split reported through the existing
// RPC. Matching boundaries without these allocated child IDs is not completion.
func (manager *Manager) RecordReuseSplitResult(regions []*metapb.Region) {
	if len(regions) < 2 {
		return
	}
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	for id, grant := range manager.reuse.grants {
		if len(regions) != len(grant.children) || !bytes.Equal(regions[0].GetStartKey(), grant.source.GetStartKey()) || !bytes.Equal(regions[len(regions)-1].GetEndKey(), grant.source.GetEndKey()) {
			continue
		}
		valid, exact := true, false
		seen := make(map[uint64]struct{}, len(regions))
		for index, region := range regions {
			_, known := grant.children[region.GetId()]
			_, duplicate := seen[region.GetId()]
			if !known || duplicate || region.GetRegionEpoch().GetVersion() != grant.source.GetRegionEpoch().GetVersion()+uint64(len(regions)-1) || region.GetRegionEpoch().GetConfVer() != grant.source.GetRegionEpoch().GetConfVer() || (index > 0 && !bytes.Equal(regions[index-1].GetEndKey(), region.GetStartKey())) {
				valid = false
				break
			}
			seen[region.GetId()] = struct{}{}
			if regionMatchesKeyspace(region, MakeRegionBound(id)) {
				exact = true
			}
		}
		if valid && exact {
			grant.confirmed = true
		}
	}
}

func regionCoversKeyspace(region *metapb.Region, bound *RegionBound) bool {
	return bytes.Compare(region.GetStartKey(), bound.TxnLeftBound) <= 0 && (len(region.GetEndKey()) == 0 || bytes.Compare(region.GetEndKey(), bound.TxnRightBound) >= 0)
}

func regionMatchesKeyspace(region *metapb.Region, bound *RegionBound) bool {
	return bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) && bytes.Equal(region.GetEndKey(), bound.TxnRightBound)
}

func (manager *Manager) rejectIndependentScheduling() error {
	if cluster, ok := manager.cluster.(interface{ IsServiceIndependent(string) bool }); ok && cluster.IsServiceIndependent(mcsconstant.SchedulingServiceName) {
		return errors.New("keyspace ID reuse is unsupported with independent scheduling service")
	}
	return nil
}
func (manager *Manager) reuseAdmissionReady(region *core.RegionInfo) error {
	if !manager.config.IsIDReuseEnabled() {
		return errors.New("keyspace ID reuse is disabled")
	}
	if err := manager.rejectIndependentScheduling(); err != nil {
		return err
	}
	allocator, ok := manager.idAllocator.(interface{ GetReservedWindowEnd() uint64 })
	if !ok {
		return errors.New("allocator does not expose its reuse watermark")
	}
	currentWindow := allocator.GetReservedWindowEnd()
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	if max(currentWindow, manager.reuse.watermark) < manager.config.GetIDReuseThreshold() {
		return errors.New("allocator watermark is below reuse threshold")
	}
	// Name-cache invalidation is cluster-wide, including stores with no target replica.
	for _, store := range manager.cluster.GetBasicCluster().GetStores() {
		if store.IsTiKV() && !store.IsRemoved() && !manager.reuse.capable[store.GetID()] {
			return errors.Errorf("store %d has not reported reuse protocol v6", store.GetID())
		}
	}
	for _, peer := range region.GetPeers() {
		if !manager.reuse.capable[peer.GetStoreId()] {
			return errors.Errorf("store %d has not reported reuse protocol v6", peer.GetStoreId())
		}
	}
	return nil
}
func (manager *Manager) coveringKeyspaceRegion(id uint32) *core.RegionInfo {
	if manager.cluster == nil {
		return nil
	}
	bound := MakeRegionBound(id)
	regions := manager.cluster.GetBasicCluster().ScanRegions(bound.TxnLeftBound, bound.TxnRightBound, 2)
	if len(regions) != 1 {
		return nil
	}
	region := regions[0]
	if bytes.Compare(region.GetStartKey(), bound.TxnLeftBound) > 0 ||
		(len(region.GetEndKey()) != 0 && bytes.Compare(region.GetEndKey(), bound.TxnRightBound) < 0) {
		return nil
	}
	return region
}
func (manager *Manager) exactKeyspaceRegion(id uint32) (*core.RegionInfo, error) {
	region := manager.coveringKeyspaceRegion(id)
	bound := MakeRegionBound(id)
	if region == nil || !bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) || !bytes.Equal(region.GetEndKey(), bound.TxnRightBound) {
		return nil, errors.Errorf("keyspace %d does not have one complete Region", id)
	}
	return region, nil
}

// splittableKeyspaceRegion requires at least one missing keyspace boundary.
// Exact isolated Regions retain tenant metadata and cannot be reused without reset.
func (manager *Manager) splittableKeyspaceRegion(id uint32) *core.RegionInfo {
	region := manager.coveringKeyspaceRegion(id)
	if region == nil {
		return nil
	}
	bound := MakeRegionBound(id)
	if bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) && bytes.Equal(region.GetEndKey(), bound.TxnRightBound) {
		return nil
	}
	return region
}

// RequestKeyspaceReclaim releases an ID without modifying TiKV. Cleaner can
// retry while waiting for a cross-keyspace merge.
func (manager *Manager) RequestKeyspaceReclaim(id uint32) error {
	if !reusableKeyspaceID(id) {
		return newModifyProtectedKeyspaceError()
	}
	if !manager.config.IsIDReuseEnabled() {
		return errors.New("keyspace ID reuse is disabled")
	}
	meta, err := manager.LoadKeyspaceByID(id)
	if err != nil {
		if errors.ErrorEqual(err, errs.ErrKeyspaceNotFound) {
			return nil
		}
		return err
	}
	if meta.State != keyspacepb.KeyspaceState_TOMBSTONE {
		return errors.Errorf("cannot reclaim keyspace in state %s", meta.State)
	}
	if !manager.transactionalReuseConfig(meta.Config) {
		return errors.New("ID reuse only supports transactional keyspaces")
	}
	region := manager.splittableKeyspaceRegion(id)
	if region == nil {
		return nil
	}
	if err := manager.reuseAdmissionReady(region); err != nil {
		return err
	}
	return manager.releaseReclaimedID(meta)
}
func (manager *Manager) tryCompleteReusedCreate(expected *keyspacepb.KeyspaceMeta) error {
	if !manager.config.IsIDReuseEnabled() {
		return errors.New("keyspace ID reuse is disabled")
	}
	region, err := manager.exactKeyspaceRegion(expected.GetId())
	if err != nil {
		return nil
	}
	if err := manager.reuseAdmissionReady(region); err != nil {
		return err
	}
	manager.reuse.mu.RLock()
	grant := manager.reuse.grants[expected.GetId()]
	confirmed := grant != nil && grant.confirmed
	belongs := false
	if grant != nil {
		_, belongs = grant.children[region.GetID()]
		belongs = belongs && region.GetRegionEpoch().GetVersion() == grant.source.GetRegionEpoch().GetVersion()+uint64(len(grant.children)-1)
	}
	manager.reuse.mu.RUnlock()
	if !belongs {
		return errors.New("reused keyspace no longer requires this creation's split")
	}
	// Heartbeat may arrive before ReportBatchSplit. Wait for that applied result,
	// but never treat an exact range alone as successful initialization.
	if !confirmed {
		return nil
	}
	return manager.completeReusedCreate(expected)
}

// Install the normal split rule only while the claimed range still needs split.
// Serialize with Archive so canceled work cannot reinstall a deleted rule.
func (manager *Manager) ensureReuseRegionRule(expected *keyspacepb.KeyspaceMeta) error {
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	current, err := manager.LoadKeyspaceByID(expected.GetId())
	if err != nil {
		return err
	}
	if !isReuseInitializing(current) || current.Name != expected.Name || current.CreatedAt != expected.CreatedAt || !maps.Equal(current.Config, expected.Config) {
		return errors.New("pending creation changed before installing boundaries")
	}
	cluster, ok := manager.cluster.(interface{ GetRegionLabeler() *labeler.RegionLabeler })
	if !ok {
		return errors.New("cluster does not support region label")
	}
	if cluster.GetRegionLabeler().GetLabelRule(buildLabelRule(expected.GetId(), txnRegionBound).ID) != nil {
		return nil
	}
	if manager.splittableKeyspaceRegion(expected.GetId()) == nil {
		return errors.New("claimed keyspace no longer requires a split")
	}
	return manager.splitKeyspaceRegion(expected.GetId(), false, txnRegionBound)
}

// Complete only the same DISABLED owner with the same pending configuration. Archive
// cancellation and late heartbeats cannot enable a later use of this ID.
func (manager *Manager) completeReusedCreate(expected *keyspacepb.KeyspaceMeta) error {
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	var completed *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId())
		if err != nil {
			return err
		}
		if meta == nil || meta.Name != expected.Name || meta.CreatedAt != expected.CreatedAt || !isReuseInitializing(meta) || !maps.Equal(meta.Config, expected.Config) {
			return errors.New("pending creation changed")
		}
		if manager.kgm != nil {
			if err := manager.kgm.UpdateKeyspaceForGroup(endpoint.StringUserKind(meta.Config[UserKindKey]), meta.Config[TSOKeyspaceGroupIDKey], meta.GetId(), opAdd); err != nil {
				return err
			}
		}
		meta.State = keyspacepb.KeyspaceState_ENABLED
		meta.PendingInitialization = false
		meta.StateChangedAt = manager.reuse.now().Unix()
		completed = meta
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err == nil {
		manager.cacheReuseMeta(completed)
	}
	return err
}

// cancelReusedCreate keeps the ID claimed and sends it through normal deletion.
// It never frees an ID or removes data while a split may still be in flight.
func (manager *Manager) cancelReusedCreate(expected *keyspacepb.KeyspaceMeta) error {
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	var canceled *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId())
		if err != nil {
			return err
		}
		if meta == nil || meta.Name != expected.Name || meta.CreatedAt != expected.CreatedAt || !isReuseInitializing(meta) {
			return nil
		}
		if err := manager.transformKeyspaceState(txn, meta, keyspacepb.KeyspaceState_ARCHIVED, manager.reuse.now().Unix()); err != nil {
			return err
		}
		canceled = meta
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err == nil && canceled != nil {
		manager.cacheReuseMeta(canceled)
	}
	return err
}

func keyspaceBitmapLayoutPath() string    { return keypath.KeyspaceFreePrefix() + "layout" }
func bitmapPrefix() string                { return keypath.KeyspaceFreePrefix() + "buckets/" }
func bitmapPath(bucket uint32) string     { return fmt.Sprintf("%s%08d", bitmapPrefix(), bucket) }
func (manager *Manager) bitmapBytes() int { return manager.config.GetIDReuseBucketBytes() }
func (manager *Manager) ensureBitmapLayout() error {
	return manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		key := keypath.KeyspaceFreePrefix() + "layout"
		layout := fmt.Sprintf("v1:%d", manager.bitmapBytes())
		value, err := txn.Load(key)
		if err != nil {
			return err
		}
		if value != "" && value != layout {
			return errors.New("free bitmap layout differs from configured bucket size")
		}
		if value == "" {
			return txn.Save(key, layout)
		}
		return nil
	})
}
func (manager *Manager) changeFreeBit(txn kv.Txn, id uint32, free bool) error {
	layout, err := txn.Load(keyspaceBitmapLayoutPath())
	if err != nil {
		return err
	}
	if layout != fmt.Sprintf("v1:%d", manager.bitmapBytes()) {
		return errors.New("free bitmap layout changed")
	}
	width := manager.bitmapBytes()
	bucket := id / uint32(width*8)
	bit := id % uint32(width*8)
	path := bitmapPath(bucket)
	value, err := txn.Load(path)
	if err != nil {
		return err
	}
	bits := make([]byte, width)
	if value != "" {
		if len(value) != width {
			return errors.New("invalid free bitmap bucket")
		}
		copy(bits, value)
	}
	mask := byte(1 << uint(bit%8))
	present := bits[bit/8]&mask != 0
	if present == free {
		return errs.ErrEtcdTxnConflict
	}
	if free {
		bits[bit/8] |= mask
	} else {
		bits[bit/8] &= ^mask
	}
	if bytes.Equal(bits, make([]byte, width)) {
		return txn.Remove(path)
	}
	return txn.Save(path, string(bits))
}
func (manager *Manager) releaseReclaimedID(expected *keyspacepb.KeyspaceMeta) error {
	var revision uint64
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId())
		if err != nil {
			return err
		}
		if meta == nil || meta.State != keyspacepb.KeyspaceState_TOMBSTONE || meta.StateChangedAt != expected.StateChangedAt {
			return errors.New("reclaimed state changed")
		}
		if err := manager.unassignKeyspaceFromMetaServiceGroup(txn, meta); err != nil {
			return err
		}
		if err := manager.store.RemoveKeyspace(txn, meta.GetId(), meta.Name); err != nil {
			return err
		}
		if err := manager.changeFreeBit(txn, meta.GetId(), true); err != nil {
			return err
		}
		revision, err = advanceNameRevision(txn)
		return err
	})
	if err == nil {
		manager.publishNameRevision(revision, expected.GetId())
		manager.reuse.mu.Lock()
		delete(manager.reuse.pending, expected.GetId())
		delete(manager.reuse.grants, expected.GetId())
		manager.reuse.mu.Unlock()
		manager.keyspaceNameLookup.Delete(expected.GetId())
		manager.keyspaceStateLookup.Delete(expected.GetId())
	}
	return err
}
func (manager *Manager) tryCreateReusedKeyspace(name string, config map[string]string, createTime int64) (*keyspacepb.KeyspaceMeta, error) {
	var existing *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		found, id, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil || !found {
			return err
		}
		existing, err = manager.store.LoadKeyspaceMeta(txn, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, errs.ErrKeyspaceExists
	}
	if !manager.config.IsIDReuseEnabled() || !manager.transactionalReuseConfig(config) || !manager.supportsReuseEncryption(config) {
		return nil, nil
	}
	if err := manager.ensureBitmapLayout(); err != nil {
		return nil, err
	}
	manager.reuse.mu.RLock()
	scanStart := manager.reuse.scanStart
	manager.reuse.mu.RUnlock()
	if scanStart == "" {
		scanStart = bitmapPrefix()
	}
	var nextScanStart string
	for attempt := 0; attempt < 16; attempt++ {
		var meta *keyspacepb.KeyspaceMeta
		var revision uint64
		err = manager.runTxnWithMetaGroupLock(func(txn kv.Txn) error {
			keys, values, err := txn.LoadRange(scanStart, clientv3.GetPrefixRangeEnd(bitmapPrefix()), freeIDScanLimit)
			if err != nil {
				return err
			}
			if len(keys) == 0 && scanStart != bitmapPrefix() {
				keys, values, err = txn.LoadRange(bitmapPrefix(), clientv3.GetPrefixRangeEnd(bitmapPrefix()), freeIDScanLimit)
				if err != nil {
					return err
				}
			}
			// Rotate bounded pages across requests, including pages with no eligible IDs.
			nextScanStart = bitmapPrefix()
			if len(keys) == freeIDScanLimit {
				nextScanStart = keys[len(keys)-1] + "\x00"
			}
			for index, key := range keys {
				bucket, err := strconv.ParseUint(key[len(bitmapPrefix()):], 10, 32)
				if err != nil || len(values[index]) != manager.bitmapBytes() {
					return errors.New("invalid free bitmap encoding")
				}
				for offset, bits := range []byte(values[index]) {
					for bit := uint32(0); bit < 8; bit++ {
						if bits&(1<<bit) == 0 {
							continue
						}
						id := uint32(bucket)*uint32(manager.bitmapBytes()*8) + uint32(offset*8) + bit
						if !reusableKeyspaceID(id) {
							return errors.New("protected ID in free bitmap")
						}
						region := manager.splittableKeyspaceRegion(id)
						if region == nil || manager.reuseAdmissionReady(region) != nil {
							continue
						}
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
							return errs.ErrEtcdTxnConflict
						}
						meta = &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: name, Config: cloneKeyspaceConfig(config), State: keyspacepb.KeyspaceState_DISABLED, PendingInitialization: true, CreatedAt: createTime, StateChangedAt: createTime}
						if err := manager.changeFreeBit(txn, id, false); err != nil {
							return err
						}
						if err := manager.store.SaveKeyspaceID(txn, id, name); err != nil {
							return err
						}
						if manager.mgm != nil && manager.mgm.hasGroupsLocked() {
							groupID, groupErr := manager.mgm.findMinMetaGroup(txn)
							if groupErr != nil && !errors.ErrorEqual(groupErr, errNoAvailableMetaServiceGroups) {
								return groupErr
							}
							if groupErr == nil {
								meta.Config[MetaServiceGroupIDKey] = groupID
								if err := manager.mgm.updateAssignmentTxn(txn, "", groupID); err != nil {
									return err
								}
							}
						}
						if err := manager.store.SaveKeyspaceMeta(txn, meta); err != nil {
							return err
						}
						revision, err = advanceNameRevision(txn)
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			if errors.ErrorEqual(err, errs.ErrKeyspaceExists) {
				return nil, err
			}
			if errors.ErrorEqual(err, errs.ErrEtcdTxnConflict) {
				continue
			}
			return nil, err
		}
		manager.reuse.mu.Lock()
		manager.reuse.scanStart = nextScanStart
		manager.reuse.mu.Unlock()
		if meta == nil {
			return nil, nil
		}
		manager.publishNameRevision(revision, meta.GetId())
		manager.cacheReuseMeta(meta)
		manager.keyspaceNameLookup.Store(meta.GetId(), name)
		if err := manager.ensureReuseRegionRule(meta); err != nil {
			if cancelErr := manager.cancelReusedCreate(meta); cancelErr != nil {
				return nil, cancelErr
			}
			return nil, err
		}
		return meta, nil
	}
	return nil, errors.New("free bitmap remained contended")
}
func (manager *Manager) waitReusedCreate(meta *keyspacepb.KeyspaceMeta) (result *keyspacepb.KeyspaceMeta, err error) {
	defer func() {
		if err != nil {
			if cancelErr := manager.cancelReusedCreate(meta); cancelErr != nil {
				err = errors.Annotatef(cancelErr, "cancel failed creation: %v", err)
			}
		}
	}()
	ticker := time.NewTicker(manager.config.GetCheckRegionSplitInterval())
	defer ticker.Stop()
	timer := time.NewTimer(manager.config.GetWaitRegionSplitTimeout())
	defer timer.Stop()
	for {
		current, err := manager.LoadKeyspaceByID(meta.GetId())
		if err != nil {
			return nil, err
		}
		if current.Name != meta.Name || current.CreatedAt != meta.CreatedAt {
			return nil, errors.New("pending creation identity changed")
		}
		if current.State == keyspacepb.KeyspaceState_ENABLED {
			return current, nil
		}
		if current.State != keyspacepb.KeyspaceState_DISABLED {
			return nil, errors.New("pending reused creation was abandoned")
		}
		select {
		case <-manager.ctx.Done():
			return nil, errors.New("pending reused creation canceled")
		case <-timer.C:
			return nil, errors.New("reused creation timed out and was canceled")
		case <-ticker.C:
			if err := manager.tryCompleteReusedCreate(meta); err != nil {
				return nil, err
			}
		}
	}
}

// A single durable revision fences ID-to-name caches on every Store. It changes
// in the same CAS transaction as ownership/free bits, and survives PD restart.
func nameRevisionPath() string {
	return fmt.Sprintf("/pd/%d/keyspaces/name_revision", keypath.ClusterID())
}
func loadNameRevision(txn kv.Txn) (uint64, error) {
	value, err := txn.Load(nameRevisionPath())
	if err != nil || value == "" {
		return 0, err
	}
	return strconv.ParseUint(value, 10, 64)
}
func advanceNameRevision(txn kv.Txn) (uint64, error) {
	revision, err := loadNameRevision(txn)
	if err != nil {
		return 0, err
	}
	if revision == ^uint64(0) {
		return 0, errors.New("keyspace name revision exhausted")
	}
	revision++
	return revision, txn.Save(nameRevisionPath(), strconv.FormatUint(revision, 10))
}
func (manager *Manager) publishNameRevision(revision uint64, id uint32) {
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	if revision <= manager.reuse.nameRevision {
		return
	}
	// Concurrent transactions may publish out of order. A gap requires a full
	// invalidation; never pretend the bounded history covers missing changes.
	if revision != manager.reuse.nameRevision+1 {
		manager.reuse.nameChanges = nil
	}
	manager.reuse.nameRevision = revision
	manager.reuse.nameChanges = append(manager.reuse.nameChanges, nameChange{revision, id})
	if len(manager.reuse.nameChanges) > nameChangeHistoryLimit {
		manager.reuse.nameChanges = manager.reuse.nameChanges[len(manager.reuse.nameChanges)-nameChangeHistoryLimit:]
	}
}

// KeyspaceNameRevision is broadcast in every Store heartbeat response so even
// Stores without replicas of the reused keyspace discard stale metric names.
func (manager *Manager) KeyspaceNameRevision() uint64 {
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	return manager.reuse.nameRevision
}

// KeyspaceNameUpdates provides loss-tolerant, cluster-wide cache invalidation.
// A missed bounded history or leadership restart falls back to a full clear.
func (manager *Manager) KeyspaceNameUpdates(since uint64) (uint64, []uint32, bool) {
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	revision := manager.reuse.nameRevision
	if since >= revision {
		return revision, nil, false
	}
	history := manager.reuse.nameChanges
	if len(history) == 0 || since < history[0].revision-1 {
		return revision, nil, true
	}
	ids := make([]uint32, 0, len(history))
	seen := make(map[uint32]bool)
	for _, change := range history {
		if change.revision > since && !seen[change.id] {
			seen[change.id] = true
			ids = append(ids, change.id)
		}
	}
	return revision, ids, false
}
