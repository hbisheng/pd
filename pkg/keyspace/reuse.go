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
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcsconstant "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	MinimumKeyspaceReuseTombstoneDuration = 7 * 24 * time.Hour
	freeIDScanLimit                       = 32
	reuseProtocolVersion                  = 2
	storageReportTTL                      = 10 * time.Minute
)

type storageReport struct {
	version, confVer, leader uint64
	state                    pdpb.RegionStorageState
	receivedAt               time.Time
}

// Only lifecycle metadata and bitmap buckets are durable. This index is rebuilt
// on leadership changes; heartbeat handlers never load metadata from etcd.
type reuseState struct {
	mu         sync.RWMutex
	pending    map[uint32]*keyspacepb.KeyspaceMeta
	workQueue  *list.List
	workItems  map[uint32]*list.Element
	byRegion   map[uint64]uint32
	reports    map[uint64]storageReport
	capable    map[uint64]bool
	watermark  uint64
	notify     func(*core.RegionInfo)
	now        func() time.Time
	retention  time.Duration
	workerStop context.CancelFunc
	workerDone chan struct{}
}

func newReuseState() *reuseState {
	return &reuseState{pending: make(map[uint32]*keyspacepb.KeyspaceMeta), workQueue: list.New(), workItems: make(map[uint32]*list.Element), byRegion: make(map[uint64]uint32),
		reports: make(map[uint64]storageReport), capable: make(map[uint64]bool), now: time.Now,
		retention: MinimumKeyspaceReuseTombstoneDuration}
}
func cloneKeyspaceConfig(config map[string]string) map[string]string {
	result := make(map[string]string, len(config))
	for k, v := range config {
		result[k] = v
	}
	return result
}
func configForRetry(config map[string]string) map[string]string {
	result := cloneKeyspaceConfig(config)
	delete(result, MetaServiceGroupIDKey)
	delete(result, MetaServiceGroupAddressesKey)
	return result
}
func isReusePending(state keyspacepb.KeyspaceState) bool {
	return state == keyspacepb.KeyspaceState_RECLAIMING || state == keyspacepb.KeyspaceState_RECLAIMED || state == keyspacepb.KeyspaceState_CREATING
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
func isKeyspaceEncrypted(config map[string]string) bool {
	if config["encryption"] == "" {
		return false
	}
	var value struct {
		Enabled bool `json:"enabled"`
	}
	return json.Unmarshal([]byte(config["encryption"]), &value) != nil || value.Enabled
}
func (manager *Manager) cacheReuseMeta(meta *keyspacepb.KeyspaceMeta) {
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	for region, id := range manager.reuse.byRegion {
		if id == meta.GetId() && meta.State != keyspacepb.KeyspaceState_RECLAIMED {
			delete(manager.reuse.byRegion, region)
		}
	}
	if isReusePending(meta.State) {
		if manager.reuse.workItems[meta.GetId()] == nil {
			manager.reuse.workItems[meta.GetId()] = manager.reuse.workQueue.PushBack(meta.GetId())
		}
		copy := *meta
		copy.Config = cloneKeyspaceConfig(meta.Config)
		manager.reuse.pending[meta.GetId()] = &copy
	} else {
		delete(manager.reuse.pending, meta.GetId())
		if item := manager.reuse.workItems[meta.GetId()]; item != nil {
			manager.reuse.workQueue.Remove(item)
			delete(manager.reuse.workItems, meta.GetId())
		}
	}
	manager.keyspaceStateLookup.Store(meta.GetId(), meta.State)
}
func (manager *Manager) currentReuseMeta(id uint32) *keyspacepb.KeyspaceMeta {
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	return manager.reuse.pending[id]
}

// StartIDReuse reloads pending lifecycle states in pages after leader election.
func (manager *Manager) StartIDReuse() error {
	// The allocator's in-memory window starts at zero after restart. Read its
	// existing durable counter once, rather than requiring a new allocation.
	var watermark uint64
	if err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
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
	manager.reuse.watermark = watermark
	manager.reuse.pending = make(map[uint32]*keyspacepb.KeyspaceMeta)
	manager.reuse.workQueue = list.New()
	manager.reuse.workItems = make(map[uint32]*list.Element)
	manager.reuse.byRegion = make(map[uint64]uint32)
	manager.reuse.reports = make(map[uint64]storageReport)
	manager.reuse.capable = make(map[uint64]bool)
	manager.reuse.mu.Unlock()
	var start uint32
	for {
		metas, err := manager.LoadRangeKeyspace(start, 256)
		if err != nil {
			return err
		}
		for _, meta := range metas {
			if isReusePending(meta.State) {
				manager.cacheReuseMeta(meta)
			}
		}
		if len(metas) < 256 {
			break
		}
		start = metas[len(metas)-1].GetId() + 1
	}
	if !manager.config.IsIDReuseEnabled() {
		return nil
	}
	if err := manager.ensureBitmapLayout(); err != nil {
		return err
	}
	manager.startReuseWorker()
	return nil
}

// StopIDReuse joins leader-only retry work.
func (manager *Manager) StopIDReuse() {
	manager.reuse.mu.Lock()
	stop, done := manager.reuse.workerStop, manager.reuse.workerDone
	manager.reuse.workerStop = nil
	manager.reuse.workerDone = nil
	manager.reuse.mu.Unlock()
	if stop != nil {
		stop()
		<-done
	}
}

// SetKeyspaceUpdateNotifier installs the generic Region notification callback.
func (manager *Manager) SetKeyspaceUpdateNotifier(notify func(*core.RegionInfo)) {
	manager.reuse.mu.Lock()
	manager.reuse.notify = notify
	manager.reuse.mu.Unlock()
}

// KeyspaceUpdateRequiredForRegion is an O(1) lookup of pending work.
func (manager *Manager) KeyspaceUpdateRequiredForRegion(regionID uint64) bool {
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	id, ok := manager.reuse.byRegion[regionID]
	meta := manager.reuse.pending[id]
	if !ok || meta == nil {
		return false
	}
	if meta.State == keyspacepb.KeyspaceState_RECLAIMED {
		report, known := manager.reuse.reports[regionID]
		return !known || manager.reuse.now().Sub(report.receivedAt) > storageReportTTL
	}
	return meta.State == keyspacepb.KeyspaceState_RECLAIMING || meta.State == keyspacepb.KeyspaceState_CREATING
}

// RecordIDReuseCapability rejects stores speaking the earlier POC protocol.
func (manager *Manager) RecordIDReuseCapability(storeID uint64, version uint32) {
	manager.reuse.mu.Lock()
	manager.reuse.capable[storeID] = version >= reuseProtocolVersion
	manager.reuse.mu.Unlock()
}

// RecordRegionStorageState records actual applied state from accepted leader
// heartbeats. Reports are scoped to an epoch and are never durable.
func (manager *Manager) RecordRegionStorageState(region *core.RegionInfo, state *pdpb.RegionStorageState) {
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	if _, pending := manager.reuse.byRegion[region.GetID()]; !pending {
		return
	}
	if state == nil {
		delete(manager.reuse.reports, region.GetID())
		return
	}
	manager.reuse.reports[region.GetID()] = storageReport{version: region.GetRegionEpoch().GetVersion(), confVer: region.GetRegionEpoch().GetConfVer(), leader: region.GetLeader().GetId(), state: *state, receivedAt: manager.reuse.now()}
}
func (manager *Manager) appliedStorageState(region *core.RegionInfo) *pdpb.RegionStorageState {
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	report, ok := manager.reuse.reports[region.GetID()]
	if !ok || manager.reuse.now().Sub(report.receivedAt) > storageReportTTL || report.version != region.GetRegionEpoch().GetVersion() || report.confVer != region.GetRegionEpoch().GetConfVer() ||
		report.leader != region.GetLeader().GetId() || len(region.GetPendingPeers()) != 0 || len(region.GetDownPeers()) != 0 {
		return nil
	}
	copy := report.state
	return &copy
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
	for _, peer := range region.GetPeers() {
		if !manager.reuse.capable[peer.GetStoreId()] {
			return errors.Errorf("store %d has not reported reuse protocol v2", peer.GetStoreId())
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

// Reclaim only resets an isolated range. Unknown neighbors, missing metadata,
// and unused gaps all leave room for merge and must not authorize Restore.
func (manager *Manager) isolatedKeyspace(id uint32, region *core.RegionInfo) bool {
	left, right := manager.cluster.GetBasicCluster().GetAdjacentRegions(region)
	if left == nil || right == nil || id == 0 || id >= 0xffffff {
		return false
	}
	for _, neighborID := range []uint32{id - 1, id + 1} {
		meta, err := manager.LoadKeyspaceByID(neighborID)
		if err != nil || meta == nil || (meta.State != keyspacepb.KeyspaceState_ENABLED && meta.State != keyspacepb.KeyspaceState_DISABLED && meta.State != keyspacepb.KeyspaceState_CREATING) {
			return false
		}
		// Verify the touching Region overlaps that active keyspace. Its tenant may
		// have several Regions, so this neighbor need not cover the whole keyspace.
		bound := MakeRegionBound(neighborID)
		neighbor := left
		if neighborID > id {
			neighbor = right
		}
		if bytes.Compare(neighbor.GetStartKey(), bound.TxnRightBound) >= 0 ||
			(len(neighbor.GetEndKey()) != 0 && bytes.Compare(neighbor.GetEndKey(), bound.TxnLeftBound) <= 0) {
			return false
		}
	}
	return true
}
func (manager *Manager) transitionReuse(id uint32, from, to keyspacepb.KeyspaceState) error {
	manager.metaLock.Lock(id)
	defer manager.metaLock.Unlock(id)
	var meta *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		var err error
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil || meta.State != from {
			return errors.New("keyspace lifecycle changed")
		}
		meta.State = to
		meta.StateChangedAt = manager.reuse.now().Unix()
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err == nil {
		manager.cacheReuseMeta(meta)
	}
	return err
}

// RequestKeyspaceReclaim is idempotent and can be revisited by Data Cleaner.
func (manager *Manager) RequestKeyspaceReclaim(id uint32) error {
	if !reusableKeyspaceID(id) {
		return newModifyProtectedKeyspaceError()
	}
	if !manager.config.IsIDReuseEnabled() {
		return errors.New("keyspace ID reuse is disabled")
	}
	meta, err := manager.LoadKeyspaceByID(id)
	if err != nil {
		return err
	}
	if !manager.transactionalReuseConfig(meta.Config) {
		return errors.New("ID reuse only supports transactional keyspaces")
	}
	if meta.State == keyspacepb.KeyspaceState_TOMBSTONE {
		region := manager.coveringKeyspaceRegion(id)
		if region == nil {
			bound := MakeRegionBound(id)
			region = manager.cluster.GetBasicCluster().GetRegionByKey(bound.TxnLeftBound)
		}
		if region == nil {
			return errors.New("Region layout is unknown")
		}
		if err := manager.reuseAdmissionReady(region); err != nil {
			return err
		}
		// The Region version does not change on ordinary writes. A clean report
		// cached from a previous creation must not complete this reclamation.
		manager.reuse.mu.Lock()
		delete(manager.reuse.reports, region.GetID())
		manager.reuse.mu.Unlock()
		if err := manager.transitionReuse(id, keyspacepb.KeyspaceState_TOMBSTONE, keyspacepb.KeyspaceState_RECLAIMING); err != nil {
			return err
		}
	} else if !isReusePending(meta.State) || meta.State == keyspacepb.KeyspaceState_CREATING {
		return errors.Errorf("cannot reclaim keyspace in state %s", meta.State)
	} else {
		manager.cacheReuseMeta(meta)
	}
	manager.advanceReuseTask(id)
	return nil
}
func (manager *Manager) indexReuseRegion(id uint32, region *core.RegionInfo, shouldNotify bool) {
	manager.reuse.mu.Lock()
	for r, k := range manager.reuse.byRegion {
		if k == id && r != region.GetID() {
			delete(manager.reuse.byRegion, r)
		}
	}
	manager.reuse.byRegion[region.GetID()] = id
	notify := manager.reuse.notify
	manager.reuse.mu.Unlock()
	if shouldNotify && notify != nil {
		notify(region)
	}
}
func (manager *Manager) unindexReuseRegion(id uint32) {
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	for regionID, keyspaceID := range manager.reuse.byRegion {
		if keyspaceID == id {
			delete(manager.reuse.byRegion, regionID)
		}
	}
}
func (manager *Manager) advanceReuseTask(id uint32) {
	cached := manager.currentReuseMeta(id)
	if cached == nil || !manager.config.IsIDReuseEnabled() {
		return
	}
	// Cleaner may race a worker transition; reload the source of truth rather
	// than leaving an older in-memory state pinned until another Cleaner run.
	meta, err := manager.LoadKeyspaceByID(id)
	if err != nil {
		return
	}
	if cached.State != meta.State || cached.StateChangedAt != meta.StateChangedAt || cached.Name != meta.Name {
		manager.cacheReuseMeta(meta)
	}
	if !isReusePending(meta.State) {
		return
	}
	if meta.State == keyspacepb.KeyspaceState_CREATING {
		rule := buildLabelRule(id, txnRegionBound)
		if manager.cluster.GetRegionLabeler().GetLabelRule(rule.ID) == nil {
			if err := manager.splitKeyspaceRegion(id, false, txnRegionBound); err != nil {
				return
			}
		}
	}
	if meta.State == keyspacepb.KeyspaceState_RECLAIMED {
		region := manager.coveringKeyspaceRegion(id)
		if region == nil || manager.reuseAdmissionReady(region) != nil {
			return
		}
		bound := MakeRegionBound(id)
		if bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) && bytes.Equal(region.GetEndKey(), bound.TxnRightBound) {
			state := manager.appliedStorageState(region)
			manager.indexReuseRegion(id, region, state == nil)
			if state == nil {
				return
			}
			if !state.ReclaimReady {
				// Continue observing during quarantine. Dirty state invalidates
				// completion, and the next successful cleanup starts a new wait.
				_ = manager.transitionReuse(id, meta.State, keyspacepb.KeyspaceState_RECLAIMING)
				return
			}
		}
		if manager.reuse.now().Sub(time.Unix(meta.StateChangedAt, 0)) >= manager.reuse.retention {
			_ = manager.releaseReclaimedID(meta)
		}
		return
	}
	region := manager.coveringKeyspaceRegion(id)
	if region == nil {
		manager.unindexReuseRegion(id)
		return
	}
	if manager.reuseAdmissionReady(region) != nil {
		return
	}
	bound := MakeRegionBound(id)
	exact := bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) && bytes.Equal(region.GetEndKey(), bound.TxnRightBound)
	if meta.State == keyspacepb.KeyspaceState_RECLAIMING {
		if !exact {
			_ = manager.transitionReuse(id, meta.State, keyspacepb.KeyspaceState_RECLAIMED)
			return
		}
		if !manager.isolatedKeyspace(id, region) {
			manager.unindexReuseRegion(id)
			return
		}
		manager.indexReuseRegion(id, region, true)
		if state := manager.appliedStorageState(region); state != nil && state.ReclaimReady {
			_ = manager.transitionReuse(id, meta.State, keyspacepb.KeyspaceState_RECLAIMED)
		}
	} else if meta.State == keyspacepb.KeyspaceState_CREATING {
		if !exact {
			_ = manager.splitKeyspaceRegion(id, false, txnRegionBound)
			return
		}
		manager.indexReuseRegion(id, region, true)
		state := manager.appliedStorageState(region)
		if state == nil {
			return
		}
		if isKeyspaceEncrypted(meta.Config) {
			if state.EncryptionMetaFileId == 0 {
				return
			}
		} else if !state.ReclaimReady {
			return
		}
		if manager.kgm != nil {
			if err := manager.kgm.UpdateKeyspaceForGroup(endpoint.StringUserKind(meta.Config[UserKindKey]), meta.Config[TSOKeyspaceGroupIDKey], id, opAdd); err != nil {
				log.Warn("failed to bind reused keyspace group", zap.Uint32("keyspace-id", id), zap.Error(err))
				return
			}
		}
		_ = manager.transitionReuse(id, meta.State, keyspacepb.KeyspaceState_ENABLED)
	}
}
func (manager *Manager) startReuseWorker() {
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	if manager.reuse.workerStop != nil {
		return
	}
	ctx, stop := context.WithCancel(manager.ctx)
	done := make(chan struct{})
	manager.reuse.workerStop = stop
	manager.reuse.workerDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(manager.config.GetIDReuseWorkerInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				manager.runOneReuseTask()
			}
		}
	}()
}
func (manager *Manager) runOneReuseTask() {
	manager.reuse.mu.Lock()
	item := manager.reuse.workQueue.Front()
	if item == nil {
		manager.reuse.mu.Unlock()
		return
	}
	id := item.Value.(uint32)
	manager.reuse.workQueue.MoveToBack(item)
	manager.reuse.mu.Unlock()
	manager.advanceReuseTask(id)
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
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId())
		if err != nil {
			return err
		}
		if meta == nil || meta.State != keyspacepb.KeyspaceState_RECLAIMED || meta.StateChangedAt != expected.StateChangedAt {
			return errors.New("reclaimed state changed")
		}
		if manager.reuse.now().Sub(time.Unix(meta.StateChangedAt, 0)) < manager.reuse.retention {
			return errors.New("reuse delay has not elapsed")
		}
		if err := manager.unassignKeyspaceFromMetaServiceGroup(txn, meta); err != nil {
			return err
		}
		if err := manager.store.RemoveKeyspace(txn, meta.GetId(), meta.Name); err != nil {
			return err
		}
		return manager.changeFreeBit(txn, meta.GetId(), true)
	})
	if err == nil {
		manager.reuse.mu.Lock()
		delete(manager.reuse.pending, expected.GetId())
		if item := manager.reuse.workItems[expected.GetId()]; item != nil {
			manager.reuse.workQueue.Remove(item)
			delete(manager.reuse.workItems, expected.GetId())
		}
		manager.reuse.mu.Unlock()
		manager.unindexReuseRegion(expected.GetId())
		manager.keyspaceNameLookup.Delete(expected.GetId())
		manager.keyspaceStateLookup.Delete(expected.GetId())
	}
	return err
}
func (manager *Manager) reserveMetaServiceGroup(config map[string]string) (map[string]string, string, error) {
	config = cloneKeyspaceConfig(config)
	if manager.mgm == nil || len(manager.mgm.GetGroups()) == 0 {
		return config, "", nil
	}
	manager.mgm.RLock()
	defer manager.mgm.RUnlock()
	if !manager.mgm.hasGroupsLocked() {
		return config, "", nil
	}
	groupID, err := manager.mgm.pickGroupLocked(manager.ctx)
	if errors.ErrorEqual(err, errNoAvailableMetaServiceGroups) {
		return config, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	config[MetaServiceGroupIDKey] = groupID
	return config, groupID, nil
}

func (manager *Manager) tryCreateReusedKeyspace(name string, config map[string]string, createTime int64) (*keyspacepb.KeyspaceMeta, *keyspacepb.KeyspaceMeta, error) {
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
		return nil, nil, err
	}
	if existing != nil {
		if existing.State == keyspacepb.KeyspaceState_CREATING && reflect.DeepEqual(configForRetry(existing.Config), configForRetry(config)) {
			manager.cacheReuseMeta(existing)
			return existing, existing, nil
		}
		return nil, nil, errs.ErrKeyspaceExists
	}
	if !manager.config.IsIDReuseEnabled() || !manager.transactionalReuseConfig(config) || (isKeyspaceEncrypted(config) && !manager.config.IsEncryptedIDReuseEnabled()) {
		return nil, nil, nil
	}
	if err := manager.ensureBitmapLayout(); err != nil {
		return nil, nil, err
	}
	config, groupID, err := manager.reserveMetaServiceGroup(config)
	if err != nil {
		return nil, nil, err
	}
	claimed := false
	defer func() {
		if !claimed && groupID != "" {
			manager.rollbackMetaServiceGroupAssignment(groupID)
		}
	}()
	for attempt := 0; attempt < 16; attempt++ {
		var meta *keyspacepb.KeyspaceMeta
		err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
			keys, values, err := txn.LoadRange(bitmapPrefix(), clientv3.GetPrefixRangeEnd(bitmapPrefix()), freeIDScanLimit)
			if err != nil {
				return err
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
						region := manager.coveringKeyspaceRegion(id)
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
						meta = &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: name, Config: cloneKeyspaceConfig(config), State: keyspacepb.KeyspaceState_CREATING, CreatedAt: createTime, StateChangedAt: createTime}
						if err := manager.changeFreeBit(txn, id, false); err != nil {
							return err
						}
						if err := manager.store.SaveKeyspaceID(txn, id, name); err != nil {
							return err
						}
						return manager.store.SaveKeyspaceMeta(txn, meta)
					}
				}
			}
			return nil
		})
		if err != nil {
			if errors.ErrorEqual(err, errs.ErrKeyspaceExists) {
				return nil, nil, err
			}
			if errors.ErrorEqual(err, errs.ErrEtcdTxnConflict) {
				continue
			}
			return nil, nil, err
		}
		if meta == nil {
			return nil, nil, nil
		}
		claimed = true
		manager.cacheReuseMeta(meta)
		manager.keyspaceNameLookup.Store(meta.GetId(), name)
		manager.reuse.mu.Lock()
		if region := manager.coveringKeyspaceRegion(meta.GetId()); region != nil {
			delete(manager.reuse.reports, region.GetID())
		}
		manager.reuse.mu.Unlock()
		if err := manager.splitKeyspaceRegion(meta.GetId(), false, txnRegionBound); err != nil {
			return nil, nil, err
		}
		manager.advanceReuseTask(meta.GetId())
		return meta, meta, nil
	}
	return nil, nil, errors.New("free bitmap remained contended")
}
func (manager *Manager) waitReusedCreate(meta *keyspacepb.KeyspaceMeta) (*keyspacepb.KeyspaceMeta, error) {
	ticker := time.NewTicker(manager.config.GetCheckRegionSplitInterval())
	defer ticker.Stop()
	timer := time.NewTimer(manager.config.GetWaitRegionSplitTimeout())
	defer timer.Stop()
	for {
		current, err := manager.LoadKeyspaceByID(meta.GetId())
		if err != nil {
			return nil, err
		}
		if current.Name != meta.Name {
			return nil, errors.New("pending creation identity changed")
		}
		if current.State == keyspacepb.KeyspaceState_ENABLED {
			return current, nil
		}
		select {
		case <-manager.ctx.Done():
			return nil, errors.New("pending reused creation canceled")
		case <-timer.C:
			return nil, errors.New("reused creation is still pending; retry with the same name and configuration")
		case <-ticker.C:
			manager.advanceReuseTask(meta.GetId())
		}
	}
}
