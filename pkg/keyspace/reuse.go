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
	"encoding/binary"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	mcsconstant "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

const (
	// MinimumKeyspaceReuseTombstoneDuration is the mandatory quarantine after
	// Cleaner finishes its ordinary cleanup and records TOMBSTONE.
	MinimumKeyspaceReuseTombstoneDuration = 30 * 24 * time.Hour
	freeIDScanLimit                       = 32
	keyspaceStateRequestLimit             = 64
)

type reuseTask struct {
	Phase                  keyspacepb.KeyspaceTaskPhase `json:"phase"`
	KeyspaceID             uint32                       `json:"keyspace_id"`
	Name                   string                       `json:"name"`
	Config                 map[string]string            `json:"config,omitempty"`
	TombstoneChangedAt     int64                        `json:"tombstone_changed_at,omitempty"`
	ExpectedGeneration     uint64                       `json:"expected_generation"`
	Generation             uint64                       `json:"generation"`
	ExpectedRestoreVersion uint64                       `json:"expected_restore_version"`
	RestoreVersion         uint64                       `json:"restore_version"`
	RegionID               uint64                       `json:"region_id"`
	RegionVersion          uint64                       `json:"region_version"`
	ConfVer                uint64                       `json:"conf_ver"`
	EncryptionMetaFileID   uint64                       `json:"encryption_meta_file_id,omitempty"`
}

func (task *reuseTask) proto() *keyspacepb.KeyspaceTask {
	if task == nil {
		return nil
	}
	return &keyspacepb.KeyspaceTask{
		Phase:                  task.Phase,
		KeyspaceId:             task.KeyspaceID,
		ExpectedGeneration:     task.ExpectedGeneration,
		Generation:             task.Generation,
		ExpectedRestoreVersion: task.ExpectedRestoreVersion,
		RestoreVersion:         task.RestoreVersion,
		RegionId:               task.RegionID,
		RegionVersion:          task.RegionVersion,
		ConfVer:                task.ConfVer,
		EncryptionMetaFileId:   task.EncryptionMetaFileID,
	}
}

type reuseState struct {
	reclaimMu  sync.Mutex
	mu         sync.RWMutex
	tasks      map[uint32]*reuseTask
	byRegion   map[uint64]uint32
	statuses   map[uint64]*pdpb.KeyspaceStateStatus
	capable    map[uint64]bool
	notify     func(*core.RegionInfo)
	now        func() time.Time
	retention  time.Duration
	workerNext uint32
	storeNext  map[uint64]uint32
	workerStop context.CancelFunc
	workerDone chan struct{}
}

func newReuseState() *reuseState {
	return &reuseState{
		tasks:     make(map[uint32]*reuseTask),
		byRegion:  make(map[uint64]uint32),
		statuses:  make(map[uint64]*pdpb.KeyspaceStateStatus),
		capable:   make(map[uint64]bool),
		now:       time.Now,
		retention: MinimumKeyspaceReuseTombstoneDuration,
		storeNext: make(map[uint64]uint32),
	}
}

func marshalReuseTask(task *reuseTask) (string, error) {
	value, err := json.Marshal(task)
	return string(value), err
}

func unmarshalReuseTask(value string) (*reuseTask, error) {
	task := &reuseTask{}
	if err := json.Unmarshal([]byte(value), task); err != nil {
		return nil, err
	}
	return task, nil
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

func cloneKeyspaceConfig(config map[string]string) map[string]string {
	cloned := make(map[string]string, len(config))
	for key, value := range config {
		cloned[key] = value
	}
	return cloned
}

func configForRetry(config map[string]string) map[string]string {
	cloned := cloneKeyspaceConfig(config)
	delete(cloned, MetaServiceGroupIDKey)
	delete(cloned, MetaServiceGroupAddressesKey)
	return cloned
}

func freeIDFromPath(path string) (uint32, error) {
	id := strings.TrimPrefix(path, keypath.KeyspaceFreePrefix())
	value, err := strconv.ParseUint(id, 10, 32)
	return uint32(value), err
}

func (manager *Manager) cacheReuseTask(task *reuseTask) {
	manager.reuse.mu.Lock()
	if old := manager.reuse.tasks[task.KeyspaceID]; old != nil {
		delete(manager.reuse.byRegion, old.RegionID)
	}
	// A status is evidence for exactly one installed task phase. In particular,
	// reclaim-applied reports have the same lineage as the later reused-Create
	// validation task, so retaining them would let Create skip a fresh peer
	// report round.
	for peerID, status := range manager.reuse.statuses {
		if status.GetKeyspaceId() == task.KeyspaceID {
			delete(manager.reuse.statuses, peerID)
		}
	}
	copy := *task
	copy.Config = cloneKeyspaceConfig(task.Config)
	manager.reuse.tasks[task.KeyspaceID] = &copy
	manager.reuse.byRegion[task.RegionID] = task.KeyspaceID
	manager.reuse.mu.Unlock()
}

func (manager *Manager) removeCachedReuseTask(id uint32) {
	manager.reuse.mu.Lock()
	if task := manager.reuse.tasks[id]; task != nil {
		delete(manager.reuse.byRegion, task.RegionID)
	}
	delete(manager.reuse.tasks, id)
	manager.reuse.mu.Unlock()
}

func (manager *Manager) loadReuseTasks() error {
	loaded := make(map[uint32]*reuseTask)
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		prefix := keypath.KeyspaceResetPrefix()
		_, values, err := txn.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), 0)
		if err != nil {
			return err
		}
		for _, value := range values {
			task, err := unmarshalReuseTask(value)
			if err != nil {
				return err
			}
			loaded[task.KeyspaceID] = task
		}
		return nil
	})
	if err != nil {
		return err
	}
	manager.reuse.mu.Lock()
	manager.reuse.tasks = loaded
	manager.reuse.byRegion = make(map[uint64]uint32, len(loaded))
	manager.reuse.statuses = make(map[uint64]*pdpb.KeyspaceStateStatus)
	manager.reuse.capable = make(map[uint64]bool)
	manager.reuse.storeNext = make(map[uint64]uint32)
	for id, task := range loaded {
		manager.reuse.byRegion[task.RegionID] = id
	}
	manager.reuse.mu.Unlock()
	return nil
}

// StartIDReuse reloads durable tasks and starts their retry worker. PD calls it
// whenever this server becomes the cluster leader so tasks created before a
// restart or leadership transfer are retried from persistent state.
func (manager *Manager) StartIDReuse() error {
	if err := manager.loadReuseTasks(); err != nil {
		return err
	}
	manager.startReuseWorker()
	return nil
}

// StopIDReuse stops leader-only retry work before the raft cluster is stopped.
func (manager *Manager) StopIDReuse() {
	manager.reuse.mu.Lock()
	stop := manager.reuse.workerStop
	done := manager.reuse.workerDone
	manager.reuse.workerStop = nil
	manager.reuse.workerDone = nil
	manager.reuse.mu.Unlock()
	if stop == nil {
		return
	}
	stop()
	<-done
}

func (manager *Manager) loadReuseTask(txn kv.Txn, id uint32) (*reuseTask, string, error) {
	value, err := txn.Load(keypath.KeyspaceResetPath(id))
	if err != nil || value == "" {
		return nil, value, err
	}
	task, err := unmarshalReuseTask(value)
	return task, value, err
}

// SetKeyspaceUpdateNotifier installs the direct Region-heartbeat notification
// callback. It is invoked only after durable task state is committed.
func (manager *Manager) SetKeyspaceUpdateNotifier(notify func(*core.RegionInfo)) {
	manager.reuse.mu.Lock()
	manager.reuse.notify = notify
	manager.reuse.mu.Unlock()
}

func (manager *Manager) notifyReuseTask(task *reuseTask) {
	region := manager.cluster.GetBasicCluster().GetRegion(task.RegionID)
	if region == nil {
		return
	}
	manager.reuse.mu.RLock()
	notify := manager.reuse.notify
	manager.reuse.mu.RUnlock()
	if notify != nil {
		notify(region)
	}
}

// KeyspaceUpdateRequiredForRegion is an O(1) heartbeat-path lookup.
func (manager *Manager) KeyspaceUpdateRequiredForRegion(regionID uint64) bool {
	manager.reuse.mu.RLock()
	_, ok := manager.reuse.byRegion[regionID]
	manager.reuse.mu.RUnlock()
	return ok
}

// LoadKeyspaceWithTask loads metadata and the current durable task in one
// transaction so CSE never observes a mixed control-plane snapshot.
func (manager *Manager) LoadKeyspaceWithTask(id uint32) (*keyspacepb.KeyspaceMeta, *keyspacepb.KeyspaceTask, error) {
	var meta *keyspacepb.KeyspaceMeta
	var task *reuseTask
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		var err error
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil {
			return errs.ErrKeyspaceNotFound
		}
		task, _, err = manager.loadReuseTask(txn, id)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if manager.mgm != nil {
		manager.mgm.AttachEndpoints(meta.GetConfig())
	}
	return meta, task.proto(), nil
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
		cluster.IsServiceIndependent(mcsconstant.SchedulingServiceName) {
		return errors.New("keyspace ID reuse is unsupported with independent scheduling service")
	}
	return nil
}

func isKeyspaceEncrypted(config map[string]string) bool {
	value := config["encryption"]
	if value == "" {
		return false
	}
	var encryption struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.Unmarshal([]byte(value), &encryption); err != nil {
		return true
	}
	return encryption.Enabled
}

func (manager *Manager) validateReclaimMeta(id uint32, meta *keyspacepb.KeyspaceMeta) error {
	if meta == nil {
		return errs.ErrKeyspaceNotFound
	}
	if meta.GetState() != keyspacepb.KeyspaceState_TOMBSTONE {
		return errors.Errorf("cannot reclaim keyspace in state %s", meta.GetState().String())
	}
	if manager.reuse.now().Sub(time.Unix(meta.GetStateChangedAt(), 0)) < manager.reuse.retention {
		return errors.Errorf("keyspace %d has not completed the %s tombstone quarantine", id, manager.reuse.retention)
	}
	if isKeyspaceEncrypted(meta.GetConfig()) {
		return errors.New("phase-one keyspace ID reuse does not reclaim encrypted keyspaces")
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
		return errors.New("keyspace allocator does not expose its reserved window end")
	}
	if end := allocator.GetReservedWindowEnd(); end < manager.config.GetIDReuseThreshold() {
		return errors.Errorf("keyspace allocator reserved window end %d is below reuse threshold %d", end, manager.config.GetIDReuseThreshold())
	}
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	for _, peer := range region.GetPeers() {
		if !manager.reuse.capable[peer.GetStoreId()] {
			return errors.Errorf("store %d has not reported keyspace ID reuse capability", peer.GetStoreId())
		}
	}
	return nil
}

// RecordKeyspaceStateStatuses records only task-scoped reports explicitly
// requested by PD. Reports remain ephemeral across PD restarts.
func (manager *Manager) RecordKeyspaceStateStatuses(storeID uint64, capable bool, statuses []*pdpb.KeyspaceStateStatus) {
	manager.reuse.mu.Lock()
	manager.reuse.capable[storeID] = capable
	changed := make(map[uint32]struct{})
	for _, status := range statuses {
		task := manager.reuse.tasks[status.GetKeyspaceId()]
		if task == nil || task.RegionID != status.GetRegionId() {
			continue
		}
		region := manager.cluster.GetBasicCluster().GetRegion(status.GetRegionId())
		if region == nil || region.GetRegionEpoch().GetVersion() != status.GetRegionVersion() ||
			region.GetRegionEpoch().GetConfVer() != status.GetConfVer() {
			continue
		}
		for _, peer := range region.GetPeers() {
			if peer.GetId() == status.GetPeerId() && peer.GetStoreId() == storeID {
				copy := *status
				manager.reuse.statuses[peer.GetId()] = &copy
				changed[status.GetKeyspaceId()] = struct{}{}
				break
			}
		}
	}
	manager.reuse.mu.Unlock()
	for id := range changed {
		manager.advanceReuseTask(id)
	}
}

func statusMatchesTask(status *pdpb.KeyspaceStateStatus, task *reuseTask, region *core.RegionInfo) bool {
	if status == nil || status.GetRegionId() != region.GetID() || status.GetKeyspaceId() != task.KeyspaceID ||
		status.GetRegionVersion() != region.GetRegionEpoch().GetVersion() ||
		status.GetConfVer() != region.GetRegionEpoch().GetConfVer() {
		return false
	}
	switch task.Phase {
	case keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_PREPARE_RECLAIM:
		return true
	case keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_RECLAIM,
		keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_VALIDATE_REUSE:
		if status.GetGeneration() != task.Generation || status.GetRestoreVersion() != task.RestoreVersion {
			return false
		}
		if task.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_VALIDATE_REUSE {
			return task.EncryptionMetaFileID != 0 &&
				status.GetEncryptionMetaFileId() == task.EncryptionMetaFileID && status.GetKeyReady()
		}
		return true
	default:
		return false
	}
}

// KeyspaceStateRequests returns a bounded set of missing/stale reports for one
// store. It iterates active tasks, never the cluster's Region set.
func (manager *Manager) KeyspaceStateRequests(storeID uint64) []*pdpb.KeyspaceStateRequest {
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	ids := make([]uint32, 0, len(manager.reuse.tasks))
	for id := range manager.reuse.tasks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	requests := make([]*pdpb.KeyspaceStateRequest, 0, min(len(ids), keyspaceStateRequestLimit))
	start := sort.Search(len(ids), func(index int) bool { return ids[index] > manager.reuse.storeNext[storeID] })
	for offset := range ids {
		id := ids[(start+offset)%len(ids)]
		manager.reuse.storeNext[storeID] = id
		task := manager.reuse.tasks[id]
		region := manager.cluster.GetBasicCluster().GetRegion(task.RegionID)
		if region == nil {
			continue
		}
		for _, peer := range region.GetPeers() {
			if peer.GetStoreId() == storeID && !statusMatchesTask(manager.reuse.statuses[peer.GetId()], task, region) {
				requests = append(requests, &pdpb.KeyspaceStateRequest{RegionId: task.RegionID, KeyspaceId: id})
				break
			}
		}
		if len(requests) == keyspaceStateRequestLimit {
			break
		}
	}
	return requests
}

func (manager *Manager) currentTask(id uint32) *reuseTask {
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	task := manager.reuse.tasks[id]
	if task == nil {
		return nil
	}
	copy := *task
	copy.Config = cloneKeyspaceConfig(task.Config)
	return &copy
}

func (manager *Manager) currentAppliedState(task *reuseTask, region *core.RegionInfo) (uint64, uint64, error) {
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	var generation, restoreVersion uint64
	for i, peer := range region.GetPeers() {
		status := manager.reuse.statuses[peer.GetId()]
		if !statusMatchesTask(status, task, region) {
			return 0, 0, errors.Errorf("missing current applied status for peer %d", peer.GetId())
		}
		if i == 0 {
			generation, restoreVersion = status.GetGeneration(), status.GetRestoreVersion()
		} else if generation != status.GetGeneration() || restoreVersion != status.GetRestoreVersion() {
			return 0, 0, errors.Errorf("keyspace %d peers disagree on applied lineage", task.KeyspaceID)
		}
	}
	return generation, restoreVersion, nil
}

func (manager *Manager) taskApplied(task *reuseTask, region *core.RegionInfo) bool {
	wantVersion := task.RegionVersion
	if task.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_RECLAIM ||
		task.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_VALIDATE_REUSE {
		wantVersion++
	}
	if region.GetID() != task.RegionID || region.GetRegionEpoch().GetVersion() != wantVersion {
		return false
	}
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	for _, peer := range region.GetPeers() {
		if !statusMatchesTask(manager.reuse.statuses[peer.GetId()], task, region) {
			return false
		}
	}
	return true
}

func (manager *Manager) currentPeersAtExpectedState(task *reuseTask, region *core.RegionInfo) bool {
	if (task.Phase != keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_RECLAIM &&
		task.Phase != keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_VALIDATE_REUSE) ||
		region.GetID() != task.RegionID || region.GetRegionEpoch().GetVersion() != task.RegionVersion {
		return false
	}
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	for _, peer := range region.GetPeers() {
		status := manager.reuse.statuses[peer.GetId()]
		if status == nil || status.GetRegionId() != region.GetID() || status.GetKeyspaceId() != task.KeyspaceID ||
			status.GetRegionVersion() != region.GetRegionEpoch().GetVersion() ||
			status.GetConfVer() != region.GetRegionEpoch().GetConfVer() ||
			status.GetGeneration() != task.ExpectedGeneration ||
			status.GetRestoreVersion() != task.ExpectedRestoreVersion {
			return false
		}
	}
	return true
}

func (manager *Manager) startReuseWorker() {
	manager.reuse.mu.Lock()
	if manager.reuse.workerStop != nil {
		manager.reuse.mu.Unlock()
		return
	}
	workerCtx, stop := context.WithCancel(manager.ctx)
	done := make(chan struct{})
	manager.reuse.workerStop = stop
	manager.reuse.workerDone = done
	manager.reuse.mu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(manager.config.GetIDReuseWorkerInterval())
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				manager.runOneReuseTask()
			}
		}
	}()
}

func (manager *Manager) runOneReuseTask() {
	manager.reuse.mu.Lock()
	ids := make([]uint32, 0, len(manager.reuse.tasks))
	for id := range manager.reuse.tasks {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		manager.reuse.mu.Unlock()
		return
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	index := sort.Search(len(ids), func(index int) bool { return ids[index] > manager.reuse.workerNext })
	if index == len(ids) {
		index = 0
	}
	manager.reuse.workerNext = ids[index]
	manager.reuse.mu.Unlock()
	task := manager.currentTask(ids[index])
	if task == nil {
		return
	}
	manager.advanceReuseTask(task.KeyspaceID)
	if current := manager.currentTask(task.KeyspaceID); current != nil {
		manager.notifyReuseTask(current)
	}
}

func (manager *Manager) advanceReuseTask(id uint32) {
	task := manager.currentTask(id)
	if task == nil {
		return
	}
	region, err := manager.exactKeyspaceRegion(id)
	if err != nil || region.GetID() != task.RegionID {
		return
	}
	switch task.Phase {
	case keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_PREPARE_RECLAIM:
		manager.authorizeReclaim(task, region)
	case keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_RECLAIM:
		if manager.taskApplied(task, region) {
			if err := manager.finishReclaim(task); err != nil {
				log.Warn("failed to finalize keyspace reclamation", zap.Uint32("keyspace-id", id), zap.Error(err))
			}
		} else if region.GetRegionEpoch().GetConfVer() != task.ConfVer &&
			manager.currentPeersAtExpectedState(task, region) {
			if err := manager.rebindResetMembership(task, region); err != nil {
				log.Warn("failed to rebind keyspace reclaim membership", zap.Uint32("keyspace-id", id), zap.Error(err))
			}
		}
	case keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_VALIDATE_REUSE:
		if manager.taskApplied(task, region) {
			if err := manager.finishReusedCreate(task); err != nil {
				log.Warn("failed to finalize reused keyspace creation", zap.Uint32("keyspace-id", id), zap.Error(err))
			}
		} else if region.GetRegionEpoch().GetConfVer() != task.ConfVer &&
			manager.currentPeersAtExpectedState(task, region) {
			if err := manager.rebindResetMembership(task, region); err != nil {
				log.Warn("failed to rebind reused-Create membership", zap.Uint32("keyspace-id", id), zap.Error(err))
			}
		}
	}
}

// rebindResetMembership recovers an authorized but not-yet-applied reset
// after a membership-only change. Fresh reports from every current peer must
// prove that the reset has not started; destructive lineage and Region version
// are never rebased.
func (manager *Manager) rebindResetMembership(expected *reuseTask, region *core.RegionInfo) error {
	if region.GetRegionEpoch().GetVersion() != expected.RegionVersion ||
		region.GetRegionEpoch().GetConfVer() == expected.ConfVer {
		return errors.New("reset membership did not change independently")
	}
	currentRegion, err := manager.exactKeyspaceRegion(expected.KeyspaceID)
	if err != nil || currentRegion.GetID() != region.GetID() ||
		currentRegion.GetRegionEpoch().GetVersion() != region.GetRegionEpoch().GetVersion() ||
		currentRegion.GetRegionEpoch().GetConfVer() != region.GetRegionEpoch().GetConfVer() {
		return errors.New("keyspace Region changed during membership recovery")
	}
	updated := *expected
	updated.ConfVer = region.GetRegionEpoch().GetConfVer()
	value, err := marshalReuseTask(&updated)
	if err != nil {
		return err
	}
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		current, currentValue, err := manager.loadReuseTask(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		expectedValue, err := marshalReuseTask(expected)
		if err != nil || current == nil || currentValue != expectedValue {
			return errors.New("keyspace reset authorization changed")
		}
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		if meta == nil || meta.GetName() != expected.Name || meta.GetGeneration() != expected.ExpectedGeneration ||
			meta.GetRestoreVersion() > expected.ExpectedRestoreVersion {
			return errors.New("keyspace identity or lineage changed during membership recovery")
		}
		if expected.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_RECLAIM {
			if meta.GetState() != keyspacepb.KeyspaceState_TOMBSTONE || meta.GetStateChangedAt() != expected.TombstoneChangedAt {
				return errors.New("tombstone identity changed during membership recovery")
			}
		} else if expected.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_VALIDATE_REUSE {
			if meta.GetState() != keyspacepb.KeyspaceState_DISABLED || !reflect.DeepEqual(meta.GetConfig(), expected.Config) {
				return errors.New("pending reused-Create identity changed during membership recovery")
			}
		} else {
			return errors.New("task phase cannot rebind membership")
		}
		return txn.Save(keypath.KeyspaceResetPath(expected.KeyspaceID), value)
	})
	if err != nil {
		return err
	}
	manager.cacheReuseTask(&updated)
	manager.notifyReuseTask(&updated)
	return nil
}

func (manager *Manager) authorizeReclaim(expected *reuseTask, region *core.RegionInfo) {
	if region.GetRegionEpoch().GetVersion() != expected.RegionVersion {
		return
	}
	generation, restoreVersion, err := manager.currentAppliedState(expected, region)
	if err != nil || generation != expected.ExpectedGeneration || restoreVersion < expected.ExpectedRestoreVersion ||
		generation == ^uint64(0) || restoreVersion == ^uint64(0) {
		return
	}
	updated := *expected
	updated.Phase = keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_RECLAIM
	updated.ExpectedRestoreVersion = restoreVersion
	updated.Generation = generation + 1
	updated.RestoreVersion = restoreVersion + 1
	updated.RegionVersion = region.GetRegionEpoch().GetVersion()
	updated.ConfVer = region.GetRegionEpoch().GetConfVer()
	value, err := marshalReuseTask(&updated)
	if err != nil {
		return
	}
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		current, currentValue, err := manager.loadReuseTask(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		expectedValue, err := marshalReuseTask(expected)
		if err != nil || current == nil || currentValue != expectedValue {
			return errors.New("keyspace reclaim preparation changed")
		}
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		if meta == nil || meta.GetName() != expected.Name || meta.GetState() != keyspacepb.KeyspaceState_TOMBSTONE ||
			meta.GetStateChangedAt() != expected.TombstoneChangedAt || meta.GetGeneration() != generation ||
			meta.GetRestoreVersion() > restoreVersion {
			return errors.New("tombstone identity or lineage changed during reclaim preparation")
		}
		return txn.Save(keypath.KeyspaceResetPath(expected.KeyspaceID), value)
	})
	if err == nil {
		manager.cacheReuseTask(&updated)
		manager.notifyReuseTask(&updated)
	}
}

// RequestKeyspaceReclaim is the explicit Cleaner-facing admission operation.
// It persists a notification-only preparation task before collecting peer state.
func (manager *Manager) RequestKeyspaceReclaim(id uint32) error {
	manager.reuse.reclaimMu.Lock()
	defer manager.reuse.reclaimMu.Unlock()
	if existing := manager.currentTask(id); existing != nil {
		if existing.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_PREPARE_RECLAIM ||
			existing.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_RECLAIM {
			manager.notifyReuseTask(existing)
			return nil
		}
		return errors.New("keyspace has another active reuse task")
	}
	if isProtectedKeyspaceID(id) {
		return newModifyProtectedKeyspaceError()
	}
	manager.reuse.mu.RLock()
	for _, task := range manager.reuse.tasks {
		if task.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_PREPARE_RECLAIM ||
			task.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_RECLAIM {
			manager.reuse.mu.RUnlock()
			return errors.New("another keyspace reclamation is already active")
		}
	}
	manager.reuse.mu.RUnlock()
	region, err := manager.exactKeyspaceRegion(id)
	if err != nil {
		return err
	}
	if err := manager.reuseAdmissionReady(region); err != nil {
		return err
	}
	if err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		return manager.validateReclaimMeta(id, meta)
	}); err != nil {
		return err
	}
	if labeler := manager.cluster.GetRegionLabeler(); labeler != nil {
		if err := labeler.SetLabelRule(buildLabelRule(id, txnRegionBound)); err != nil {
			return err
		}
	}
	var task *reuseTask
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if err := manager.validateReclaimMeta(id, meta); err != nil {
			return err
		}
		if existing, _, err := manager.loadReuseTask(txn, id); err != nil || existing != nil {
			return err
		}
		task = &reuseTask{
			Phase:                  keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_PREPARE_RECLAIM,
			KeyspaceID:             id,
			Name:                   meta.GetName(),
			TombstoneChangedAt:     meta.GetStateChangedAt(),
			ExpectedGeneration:     meta.GetGeneration(),
			ExpectedRestoreVersion: meta.GetRestoreVersion(),
			RegionID:               region.GetID(),
			RegionVersion:          region.GetRegionEpoch().GetVersion(),
			ConfVer:                region.GetRegionEpoch().GetConfVer(),
		}
		value, err := marshalReuseTask(task)
		if err != nil {
			return err
		}
		return txn.Save(keypath.KeyspaceResetPath(id), value)
	})
	if err != nil {
		return err
	}
	manager.cacheReuseTask(task)
	manager.notifyReuseTask(task)
	return nil
}

func (manager *Manager) finishReclaim(expected *reuseTask) error {
	manager.metaLock.Lock(expected.KeyspaceID)
	defer manager.metaLock.Unlock(expected.KeyspaceID)
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		current, value, err := manager.loadReuseTask(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		expectedValue, err := marshalReuseTask(expected)
		if err != nil || current == nil || value != expectedValue {
			return errors.New("keyspace reclaim task changed")
		}
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		if meta == nil || meta.GetName() != expected.Name || meta.GetState() != keyspacepb.KeyspaceState_TOMBSTONE ||
			meta.GetStateChangedAt() != expected.TombstoneChangedAt || meta.GetGeneration() != expected.ExpectedGeneration ||
			meta.GetRestoreVersion() > expected.ExpectedRestoreVersion {
			return errors.New("tombstone identity changed during keyspace reclaim")
		}
		if free, err := txn.Load(keypath.KeyspaceFreePath(expected.KeyspaceID)); err != nil || free != "" {
			if err != nil {
				return err
			}
			return errors.New("free keyspace record already exists")
		}
		if err := manager.store.RemoveKeyspace(txn, expected.KeyspaceID, expected.Name); err != nil {
			return err
		}
		if err := manager.unassignKeyspaceFromMetaServiceGroup(txn, meta); err != nil {
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
	manager.removeCachedReuseTask(expected.KeyspaceID)
	manager.keyspaceNameLookup.Delete(expected.KeyspaceID)
	manager.keyspaceStateLookup.Delete(expected.KeyspaceID)
	return nil
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

func (manager *Manager) tryCreateReusedKeyspace(name string, config map[string]string, createTime int64) (*keyspacepb.KeyspaceMeta, *reuseTask, error) {
	var existingMeta *keyspacepb.KeyspaceMeta
	var existingTask *reuseTask
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		exists, id, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil || !exists {
			return err
		}
		existingMeta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		existingTask, _, err = manager.loadReuseTask(txn, id)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if existingMeta != nil {
		if existingTask != nil && existingTask.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_VALIDATE_REUSE &&
			((existingMeta.GetState() == keyspacepb.KeyspaceState_DISABLED &&
				existingMeta.GetRestoreVersion() == existingTask.ExpectedRestoreVersion) ||
				(existingMeta.GetState() == keyspacepb.KeyspaceState_ENABLED &&
					existingMeta.GetRestoreVersion() == existingTask.RestoreVersion)) &&
			existingMeta.GetGeneration() == existingTask.Generation &&
			reflect.DeepEqual(configForRetry(existingMeta.GetConfig()), configForRetry(config)) {
			manager.cacheReuseTask(existingTask)
			return existingMeta, existingTask, nil
		}
		if existingTask == nil && existingMeta.GetState() == keyspacepb.KeyspaceState_ENABLED &&
			existingMeta.GetGeneration() != 0 && existingMeta.GetRestoreVersion() != 0 &&
			isKeyspaceEncrypted(existingMeta.GetConfig()) &&
			reflect.DeepEqual(configForRetry(existingMeta.GetConfig()), configForRetry(config)) {
			return existingMeta, nil, nil
		}
		return nil, nil, errs.ErrKeyspaceExists
	}
	// The first end-to-end prototype reuses only CMEK keyspaces. Plaintext
	// creates keep the established allocator path instead of silently bypassing
	// the encryption-readiness contract.
	if !manager.config.IsIDReuseEnabled() || !isKeyspaceEncrypted(config) {
		return nil, nil, nil
	}
	if err := manager.rejectIndependentScheduling(); err != nil {
		return nil, nil, err
	}
	prefix := keypath.KeyspaceFreePrefix()
	var keys, values []string
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		var err error
		keys, values, err = txn.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), freeIDScanLimit)
		return err
	})
	if err != nil || len(keys) == 0 {
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
	for index, key := range keys {
		id, parseErr := freeIDFromPath(key)
		generation, restoreVersion, decodeErr := decodeFreeID(values[index])
		if parseErr != nil || decodeErr != nil {
			continue
		}
		region, regionErr := manager.exactKeyspaceRegion(id)
		if regionErr != nil || generation == ^uint64(0) || restoreVersion == ^uint64(0) {
			continue
		}
		if manager.reuseAdmissionReady(region) != nil {
			continue
		}
		meta := &keyspacepb.KeyspaceMeta{
			Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: id},
			Name:           name,
			State:          keyspacepb.KeyspaceState_DISABLED,
			CreatedAt:      createTime,
			StateChangedAt: createTime,
			Config:         cloneKeyspaceConfig(config),
			Generation:     generation,
			RestoreVersion: restoreVersion,
		}
		task := &reuseTask{
			Phase:                  keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_VALIDATE_REUSE,
			KeyspaceID:             id,
			Name:                   name,
			Config:                 cloneKeyspaceConfig(config),
			ExpectedGeneration:     generation,
			Generation:             generation,
			ExpectedRestoreVersion: restoreVersion,
			RestoreVersion:         restoreVersion + 1,
			RegionID:               region.GetID(),
			RegionVersion:          region.GetRegionEpoch().GetVersion(),
			ConfVer:                region.GetRegionEpoch().GetConfVer(),
		}
		value, err := marshalReuseTask(task)
		if err != nil {
			return nil, nil, err
		}
		err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
			free, err := txn.Load(key)
			if err != nil || free != values[index] {
				return errors.New("free keyspace candidate changed")
			}
			exists, _, err := manager.store.LoadKeyspaceID(txn, name)
			if err != nil || exists {
				if exists {
					return errs.ErrKeyspaceExists
				}
				return err
			}
			oldMeta, err := manager.store.LoadKeyspaceMeta(txn, id)
			if err != nil || oldMeta != nil {
				return errors.New("free keyspace candidate still has metadata")
			}
			if err := manager.store.SaveKeyspaceID(txn, id, name); err != nil {
				return err
			}
			if err := manager.store.SaveKeyspaceMeta(txn, meta); err != nil {
				return err
			}
			if err := txn.Save(keypath.KeyspaceResetPath(id), value); err != nil {
				return err
			}
			return txn.Remove(key)
		})
		if err != nil {
			if errors.ErrorEqual(err, errs.ErrKeyspaceExists) {
				return manager.tryCreateReusedKeyspace(name, config, createTime)
			}
			continue
		}
		claimed = true
		manager.cacheReuseTask(task)
		manager.keyspaceNameLookup.Store(id, name)
		manager.keyspaceStateLookup.Store(id, keyspacepb.KeyspaceState_DISABLED)
		manager.notifyReuseTask(task)
		return meta, task, nil
	}
	return nil, nil, nil
}

// SelectKeyspaceEncryptionMetaFile performs the single-winner CAS for a CSE
// candidate. Losing candidates remain unused and callers receive the durable
// selected file ID.
func (manager *Manager) SelectKeyspaceEncryptionMetaFile(
	id uint32,
	generation, restoreVersion, candidate uint64,
) (uint64, error) {
	if candidate == 0 {
		return 0, errors.New("encryption metadata candidate file ID must be non-zero")
	}
	var selected uint64
	var updated *reuseTask
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		task, _, err := manager.loadReuseTask(txn, id)
		if err != nil {
			return err
		}
		if task == nil || task.Phase != keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_VALIDATE_REUSE ||
			task.Generation != generation || task.RestoreVersion != restoreVersion {
			return errors.New("no matching pending reused-keyspace Create")
		}
		meta, err := manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil || meta.GetState() != keyspacepb.KeyspaceState_DISABLED ||
			meta.GetName() != task.Name || !reflect.DeepEqual(meta.GetConfig(), task.Config) ||
			meta.GetGeneration() != generation || meta.GetRestoreVersion() != task.ExpectedRestoreVersion {
			return errors.New("pending reused-keyspace Create identity changed")
		}
		if task.EncryptionMetaFileID != 0 {
			selected = task.EncryptionMetaFileID
			return nil
		}
		task.EncryptionMetaFileID = candidate
		value, err := marshalReuseTask(task)
		if err != nil {
			return err
		}
		if err := txn.Save(keypath.KeyspaceResetPath(id), value); err != nil {
			return err
		}
		selected = candidate
		updated = task
		return nil
	})
	if err != nil {
		return 0, err
	}
	if updated != nil {
		manager.cacheReuseTask(updated)
		manager.notifyReuseTask(updated)
	}
	return selected, nil
}

func (manager *Manager) finishReusedCreate(expected *reuseTask) error {
	manager.metaLock.Lock(expected.KeyspaceID)
	defer manager.metaLock.Unlock(expected.KeyspaceID)
	alreadyFinished := false
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		current, value, err := manager.loadReuseTask(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		expectedValue, err := marshalReuseTask(expected)
		if err != nil {
			return err
		}
		if current == nil {
			meta, err := manager.store.LoadKeyspaceMeta(txn, expected.KeyspaceID)
			if err != nil {
				return err
			}
			if meta != nil && meta.GetName() == expected.Name && meta.GetState() == keyspacepb.KeyspaceState_ENABLED &&
				meta.GetGeneration() == expected.Generation && meta.GetRestoreVersion() == expected.RestoreVersion {
				alreadyFinished = true
				return nil
			}
			return errors.New("reused keyspace create task is no longer active")
		}
		if value != expectedValue {
			return errors.New("reused keyspace create task changed")
		}
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		if meta == nil || meta.GetName() != expected.Name ||
			(meta.GetState() != keyspacepb.KeyspaceState_DISABLED && meta.GetState() != keyspacepb.KeyspaceState_ENABLED) ||
			!reflect.DeepEqual(meta.GetConfig(), expected.Config) || meta.GetGeneration() != expected.Generation {
			return errors.New("reused keyspace identity or lineage changed")
		}
		if meta.GetState() == keyspacepb.KeyspaceState_DISABLED {
			if meta.GetRestoreVersion() != expected.ExpectedRestoreVersion {
				return errors.New("pending reused-keyspace Restore version changed")
			}
			meta.State = keyspacepb.KeyspaceState_ENABLED
			meta.StateChangedAt = manager.reuse.now().Unix()
			meta.RestoreVersion = expected.RestoreVersion
			return manager.store.SaveKeyspaceMeta(txn, meta)
		}
		if meta.GetRestoreVersion() != expected.RestoreVersion {
			return errors.New("completed reused-keyspace Restore version changed")
		}
		return nil
	})
	if err != nil {
		return err
	}
	if alreadyFinished {
		manager.removeCachedReuseTask(expected.KeyspaceID)
		return nil
	}
	if manager.kgm != nil {
		userKind := endpoint.StringUserKind(expected.Config[UserKindKey])
		if err := manager.kgm.UpdateKeyspaceForGroup(userKind, expected.Config[TSOKeyspaceGroupIDKey], expected.KeyspaceID, opAdd); err != nil {
			return err
		}
	}
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		current, value, err := manager.loadReuseTask(txn, expected.KeyspaceID)
		if err != nil {
			return err
		}
		expectedValue, err := marshalReuseTask(expected)
		if err != nil || current == nil || value != expectedValue {
			return errors.New("reused keyspace create task changed before completion")
		}
		return txn.Remove(keypath.KeyspaceResetPath(expected.KeyspaceID))
	})
	if err != nil {
		return err
	}
	manager.removeCachedReuseTask(expected.KeyspaceID)
	manager.keyspaceStateLookup.Store(expected.KeyspaceID, keyspacepb.KeyspaceState_ENABLED)
	return nil
}

func (manager *Manager) waitReusedCreate(task *reuseTask) (*keyspacepb.KeyspaceMeta, error) {
	ticker := time.NewTicker(manager.config.GetCheckRegionSplitInterval())
	timer := time.NewTimer(manager.config.GetWaitRegionSplitTimeout())
	defer ticker.Stop()
	defer timer.Stop()
	for {
		if manager.currentTask(task.KeyspaceID) == nil {
			return manager.LoadKeyspaceByID(task.KeyspaceID)
		}
		select {
		case <-manager.ctx.Done():
			return nil, errors.New("waiting for reused keyspace validation was canceled")
		case <-ticker.C:
			manager.advanceReuseTask(task.KeyspaceID)
		case <-timer.C:
			return nil, errors.New("timed out waiting for current peers to validate the reused keyspace")
		}
	}
}
