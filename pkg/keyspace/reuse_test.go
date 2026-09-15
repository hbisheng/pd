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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

func newReuseTestRegion(id uint32, version, confVer uint64, peers ...*metapb.Peer) *core.RegionInfo {
	bound := MakeRegionBound(id)
	meta := &metapb.Region{
		Id:          10,
		StartKey:    bound.TxnLeftBound,
		EndKey:      bound.TxnRightBound,
		RegionEpoch: &metapb.RegionEpoch{Version: version, ConfVer: confVer},
		Peers:       peers,
	}
	return core.NewRegionInfo(meta, peers[0])
}

func reportKeyspaceState(manager *Manager, region *core.RegionInfo, keyspaceID uint32, generation, restoreVersion uint64) {
	reportKeyspaceStateWithEncryption(manager, region, keyspaceID, generation, restoreVersion, 0, false)
}

func reportKeyspaceStateWithEncryption(
	manager *Manager,
	region *core.RegionInfo,
	keyspaceID uint32,
	generation, restoreVersion, encryptionMetaFileID uint64,
	keyReady bool,
) {
	for _, peer := range region.GetPeers() {
		manager.RecordKeyspaceStateStatuses(peer.GetStoreId(), true, []*pdpb.KeyspaceStateStatus{{
			RegionId:             region.GetID(),
			PeerId:               peer.GetId(),
			KeyspaceId:           keyspaceID,
			Generation:           generation,
			RestoreVersion:       restoreVersion,
			RegionVersion:        region.GetRegionEpoch().GetVersion(),
			ConfVer:              region.GetRegionEpoch().GetConfVer(),
			EncryptionMetaFileId: encryptionMetaFileID,
			KeyReady:             keyReady,
		}})
	}
}

func newReuseTestManager(t *testing.T) (context.Context, *endpoint.StorageEndpoint, *mockcluster.Cluster, *Manager) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	for storeID := uint64(1); storeID <= 4; storeID++ {
		cluster.AddRegionStore(storeID, 1)
	}
	allocator := mockid.NewIDAllocator()
	require.NoError(t, allocator.SetBase(10))
	kgm := NewKeyspaceGroupManager(ctx, storage, nil)
	require.NoError(t, kgm.Bootstrap(ctx))
	manager := NewKeyspaceManager(ctx, storage, cluster, allocator, &mockConfig{
		EnableIDReuse:            true,
		IDReuseThreshold:         10,
		IDReuseWorkerInterval:    typeutil.NewDuration(time.Hour),
		WaitRegionSplit:          false,
		WaitRegionSplitTimeout:   typeutil.NewDuration(2 * time.Second),
		CheckRegionSplitInterval: typeutil.NewDuration(5 * time.Millisecond),
	}, kgm, nil)
	manager.reuse.now = func() time.Time { return time.Unix(4_000_000, 0) }
	return ctx, storage, cluster, manager
}

func TestReclaimNotifyPullAndReusedCreateReset(t *testing.T) {
	ctx, storage, cluster, manager := newReuseTestManager(t)
	const keyspaceID = uint32(42)
	peers := []*metapb.Peer{{Id: 101, StoreId: 1}, {Id: 102, StoreId: 2}, {Id: 103, StoreId: 3}}
	region := newReuseTestRegion(keyspaceID, 5, 3, peers...)
	cluster.PutRegion(region)
	meta := &keyspacepb.KeyspaceMeta{
		Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: keyspaceID},
		Name:           "retired-tenant",
		State:          keyspacepb.KeyspaceState_TOMBSTONE,
		CreatedAt:      1,
		StateChangedAt: manager.reuse.now().Add(-MinimumKeyspaceReuseTombstoneDuration - time.Hour).Unix(),
		Generation:     3,
		RestoreVersion: 2,
	}
	require.NoError(t, manager.saveNewKeyspace(meta))
	for _, peer := range peers {
		manager.RecordKeyspaceStateStatuses(peer.GetStoreId(), true, nil)
	}
	notifications := make(chan *core.RegionInfo, 8)
	manager.SetKeyspaceUpdateNotifier(func(region *core.RegionInfo) { notifications <- region })

	// Cleaner admission durably creates PREPARE and directly notifies CSE;
	// no incoming Region heartbeat is needed to discover the task.
	require.NoError(t, manager.RequestKeyspaceReclaim(keyspaceID))
	select {
	case notified := <-notifications:
		require.Equal(t, region.GetID(), notified.GetID())
	case <-time.After(time.Second):
		t.Fatal("durable reclaim admission did not directly notify CSE")
	}
	// A duplicate Cleaner request is idempotent and re-notifies the same task.
	require.NoError(t, manager.RequestKeyspaceReclaim(keyspaceID))
	select {
	case <-notifications:
	case <-time.After(time.Second):
		t.Fatal("duplicate reclaim request did not re-notify CSE")
	}

	// PD restart rebuilds the task index from durable storage. Its worker can
	// retry notification without scanning the Region set or changing the task.
	restarted := NewKeyspaceManager(ctx, storage, cluster, manager.idAllocator, manager.config, manager.kgm, manager.mgm)
	restarted.reuse.now = manager.reuse.now
	require.NoError(t, restarted.StartIDReuse())
	restarted.SetKeyspaceUpdateNotifier(func(region *core.RegionInfo) { notifications <- region })
	restarted.runOneReuseTask()
	select {
	case <-notifications:
	case <-time.After(time.Second):
		t.Fatal("restarted PD did not retry durable task notification")
	}
	manager = restarted
	for _, peer := range peers {
		manager.RecordKeyspaceStateStatuses(peer.GetStoreId(), true, nil)
	}
	loaded, pulled, err := manager.LoadKeyspaceWithTask(keyspaceID)
	require.NoError(t, err)
	require.Equal(t, meta.GetName(), loaded.GetName())
	require.Equal(t, keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_PREPARE_RECLAIM, pulled.GetPhase())
	for _, peer := range peers {
		requests := manager.KeyspaceStateRequests(peer.GetStoreId())
		require.Equal(t, []*pdpb.KeyspaceStateRequest{{RegionId: region.GetID(), KeyspaceId: keyspaceID}}, requests)
	}

	// The preparation reports establish the actual applied restore floor. PD
	// then persists one immutable destructive authorization and notifies again.
	reportKeyspaceState(manager, region, keyspaceID, 3, 7)
	task := manager.currentTask(keyspaceID)
	require.Equal(t, keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_RECLAIM, task.Phase)
	require.Equal(t, uint64(3), task.ExpectedGeneration)
	require.Equal(t, uint64(4), task.Generation)
	require.Equal(t, uint64(7), task.ExpectedRestoreVersion)
	require.Equal(t, uint64(8), task.RestoreVersion)
	select {
	case <-notifications:
	case <-time.After(time.Second):
		t.Fatal("authorized reclaim did not directly notify CSE")
	}

	// A membership-only change before proposal can recover only after every
	// current peer proves that the immutable reset has not started.
	currentPeers := []*metapb.Peer{{Id: 101, StoreId: 1}, {Id: 102, StoreId: 2}, {Id: 104, StoreId: 4, Role: metapb.PeerRole_Learner}}
	region = newReuseTestRegion(keyspaceID, 5, 4, currentPeers...)
	cluster.PutRegion(region)
	reportKeyspaceState(manager, region, keyspaceID, 3, 7)
	task = manager.currentTask(keyspaceID)
	require.Equal(t, uint64(4), task.ConfVer)
	require.Equal(t, uint64(4), task.Generation)
	require.Equal(t, uint64(8), task.RestoreVersion)

	// After reset application, a delayed learner prevents completion.
	region = newReuseTestRegion(keyspaceID, 6, 4, currentPeers...)
	cluster.PutRegion(region)
	for _, peer := range currentPeers[:2] {
		manager.RecordKeyspaceStateStatuses(peer.GetStoreId(), true, []*pdpb.KeyspaceStateStatus{{
			RegionId:       region.GetID(),
			PeerId:         peer.GetId(),
			KeyspaceId:     keyspaceID,
			Generation:     4,
			RestoreVersion: 8,
			RegionVersion:  6,
			ConfVer:        4,
		}})
	}
	require.NotNil(t, manager.currentTask(keyspaceID))

	// A second membership change refreshes the required peers. Fresh target
	// reports from the replacement learner complete verification without reset.
	finalPeers := []*metapb.Peer{{Id: 101, StoreId: 1}, {Id: 102, StoreId: 2}, {Id: 105, StoreId: 4, Role: metapb.PeerRole_Learner}}
	region = newReuseTestRegion(keyspaceID, 6, 5, finalPeers...)
	cluster.PutRegion(region)
	reportKeyspaceState(manager, region, keyspaceID, 4, 8)
	_, err = manager.LoadKeyspaceByID(keyspaceID)
	require.Error(t, err)
	var free string
	require.NoError(t, storage.RunInTxn(ctx, func(txn kv.Txn) error {
		var err error
		free, err = txn.Load(keypath.KeyspaceFreePath(keyspaceID))
		return err
	}))
	generation, restoreVersion, err := decodeFreeID(free)
	require.NoError(t, err)
	require.Equal(t, uint64(4), generation)
	require.Equal(t, uint64(8), restoreVersion)

	// A malformed lower-ID candidate cannot starve the valid reclaimed ID.
	require.NoError(t, storage.RunInTxn(ctx, func(txn kv.Txn) error {
		return txn.Save(keypath.KeyspaceFreePath(keyspaceID-1), "malformed")
	}))
	result := make(chan struct {
		meta *keyspacepb.KeyspaceMeta
		err  error
	}, 1)
	go func() {
		created, createErr := manager.CreateKeyspace(&CreateKeyspaceRequest{
			Name:       "new-tenant",
			CreateTime: manager.reuse.now().Unix(),
			Config: map[string]string{
				"encryption": `{"enabled":true,"cmek_id":"test-key","vendor":"test"}`,
			},
		})
		result <- struct {
			meta *keyspacepb.KeyspaceMeta
			err  error
		}{created, createErr}
	}()
	require.Eventually(t, func() bool {
		current := manager.currentTask(keyspaceID)
		return current != nil && current.Phase == keyspacepb.KeyspaceTaskPhase_KEYSPACE_TASK_VALIDATE_REUSE
	}, time.Second, 5*time.Millisecond)
	validateTask := manager.currentTask(keyspaceID)
	require.Equal(t, uint64(6), validateTask.RegionVersion)
	require.Equal(t, uint64(4), validateTask.Generation)
	require.Equal(t, uint64(8), validateTask.ExpectedRestoreVersion)
	require.Equal(t, uint64(9), validateTask.RestoreVersion)
	require.Zero(t, validateTask.EncryptionMetaFileID)
	_, err = manager.UpdateKeyspaceConfig("new-tenant", []*Mutation{{
		Op:    OpPut,
		Key:   "bypass",
		Value: "forbidden",
	}})
	require.ErrorContains(t, err, "reset is pending")
	_, err = manager.UpdateKeyspaceState("new-tenant", keyspacepb.KeyspaceState_ENABLED, manager.reuse.now().Unix())
	require.ErrorContains(t, err, "reset is pending")
	selected, err := manager.SelectKeyspaceEncryptionMetaFile(keyspaceID, 4, 9, 777)
	require.NoError(t, err)
	require.Equal(t, uint64(777), selected)
	selected, err = manager.SelectKeyspaceEncryptionMetaFile(keyspaceID, 4, 9, 778)
	require.NoError(t, err)
	require.Equal(t, uint64(777), selected, "the first durable CMEK file selection must win")
	validateTask = manager.currentTask(keyspaceID)
	require.Equal(t, uint64(777), validateTask.EncryptionMetaFileID)

	// A crash after enabling metadata but before removing the durable task must
	// leave same-name/config Create retries attached to the original operation.
	require.NoError(t, storage.RunInTxn(ctx, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, keyspaceID)
		if err != nil {
			return err
		}
		meta.State = keyspacepb.KeyspaceState_ENABLED
		meta.RestoreVersion = validateTask.RestoreVersion
		return manager.store.SaveKeyspaceMeta(txn, meta)
	}))
	retryResult := make(chan error, 1)
	go func() {
		_, retryErr := manager.CreateKeyspace(&CreateKeyspaceRequest{
			Name:       "new-tenant",
			CreateTime: manager.reuse.now().Unix(),
			Config: map[string]string{
				"encryption": `{"enabled":true,"cmek_id":"test-key","vendor":"test"}`,
			},
		})
		retryResult <- retryErr
	}()

	// The reused Create performs a second empty Restore: generation remains 4,
	// while both Region and schema restore versions advance exactly once. Only
	// matching applied and key-ready reports can complete Create.
	region = newReuseTestRegion(keyspaceID, 7, 5, finalPeers...)
	cluster.PutRegion(region)
	reportKeyspaceStateWithEncryption(manager, region, keyspaceID, 4, 9, 777, false)
	require.NotNil(t, manager.currentTask(keyspaceID))
	reportKeyspaceStateWithEncryption(manager, region, keyspaceID, 4, 9, 777, true)
	created := <-result
	require.NoError(t, created.err)
	require.NoError(t, <-retryResult)
	require.Equal(t, keyspaceID, created.meta.GetId())
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, created.meta.GetState())
	require.Equal(t, uint64(4), created.meta.GetGeneration())
	require.Equal(t, uint64(9), created.meta.GetRestoreVersion())
	require.Nil(t, manager.currentTask(keyspaceID))
	require.Equal(t, uint64(7), region.GetRegionEpoch().GetVersion(), "reused Create must execute exactly one second reset")
	_, err = manager.CreateKeyspace(&CreateKeyspaceRequest{
		Name:       "new-tenant",
		CreateTime: manager.reuse.now().Unix(),
		Config: map[string]string{
			"encryption": `{"enabled":true,"cmek_id":"another-key","vendor":"test"}`,
		},
	})
	require.ErrorContains(t, err, "keyspace already exists")
}

func TestIDReuseWorkerCanRestartAcrossLeadership(t *testing.T) {
	_, _, _, manager := newReuseTestManager(t)
	manager.RecordKeyspaceStateStatuses(1, true, nil)
	require.NoError(t, manager.StartIDReuse())
	manager.reuse.mu.RLock()
	require.NotNil(t, manager.reuse.workerStop)
	require.Empty(t, manager.reuse.capable, "leadership acquisition must discard stale reports")
	manager.reuse.mu.RUnlock()

	manager.StopIDReuse()
	manager.reuse.mu.RLock()
	require.Nil(t, manager.reuse.workerStop)
	manager.reuse.mu.RUnlock()

	require.NoError(t, manager.StartIDReuse())
	manager.StopIDReuse()
}

func seedReclaimCandidate(t *testing.T, manager *Manager, cluster *mockcluster.Cluster, id uint32, age time.Duration) {
	t.Helper()
	peer := &metapb.Peer{Id: 101, StoreId: 1}
	cluster.PutRegion(newReuseTestRegion(id, 5, 1, peer))
	require.NoError(t, manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{
		Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: id},
		Name:           "candidate",
		State:          keyspacepb.KeyspaceState_TOMBSTONE,
		StateChangedAt: manager.reuse.now().Add(-age).Unix(),
	}))
	manager.RecordKeyspaceStateStatuses(peer.GetStoreId(), true, nil)
}

func TestReuseAdmissionGates(t *testing.T) {
	t.Run("feature off", func(t *testing.T) {
		_, _, cluster, manager := newReuseTestManager(t)
		seedReclaimCandidate(t, manager, cluster, 42, MinimumKeyspaceReuseTombstoneDuration+time.Hour)
		manager.config.(*mockConfig).EnableIDReuse = false
		require.ErrorContains(t, manager.RequestKeyspaceReclaim(42), "disabled")
	})

	t.Run("below allocator threshold", func(t *testing.T) {
		_, _, cluster, manager := newReuseTestManager(t)
		seedReclaimCandidate(t, manager, cluster, 42, MinimumKeyspaceReuseTombstoneDuration+time.Hour)
		require.NoError(t, manager.idAllocator.(*mockid.IDAllocator).SetBase(9))
		require.ErrorContains(t, manager.RequestKeyspaceReclaim(42), "below reuse threshold")
	})

	t.Run("at allocator threshold", func(t *testing.T) {
		_, _, cluster, manager := newReuseTestManager(t)
		seedReclaimCandidate(t, manager, cluster, 42, MinimumKeyspaceReuseTombstoneDuration+time.Hour)
		require.NoError(t, manager.RequestKeyspaceReclaim(42))
	})

	t.Run("retention", func(t *testing.T) {
		_, _, cluster, manager := newReuseTestManager(t)
		seedReclaimCandidate(t, manager, cluster, 42, MinimumKeyspaceReuseTombstoneDuration-time.Hour)
		require.ErrorContains(t, manager.RequestKeyspaceReclaim(42), "quarantine")
	})

	t.Run("protected ID", func(t *testing.T) {
		_, _, _, manager := newReuseTestManager(t)
		require.Error(t, manager.RequestKeyspaceReclaim(0))
	})
}

func TestReuseAdmissionAndPlaintextCreateGates(t *testing.T) {
	ctx, storage, cluster, manager := newReuseTestManager(t)
	const keyspaceID = uint32(42)
	peer := &metapb.Peer{Id: 101, StoreId: 1}
	region := newReuseTestRegion(keyspaceID, 5, 1, peer)
	cluster.PutRegion(region)
	meta := &keyspacepb.KeyspaceMeta{
		Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: keyspaceID},
		Name:           "encrypted-retired",
		State:          keyspacepb.KeyspaceState_TOMBSTONE,
		StateChangedAt: manager.reuse.now().Add(-MinimumKeyspaceReuseTombstoneDuration - time.Hour).Unix(),
		Config:         map[string]string{"encryption": `{"enabled":true}`},
	}
	require.NoError(t, manager.saveNewKeyspace(meta))
	manager.RecordKeyspaceStateStatuses(peer.GetStoreId(), true, nil)
	require.ErrorContains(t, manager.RequestKeyspaceReclaim(keyspaceID), "does not reclaim encrypted")
	require.Nil(t, cluster.GetRegionLabeler().GetLabelRule(buildLabelRule(keyspaceID, txnRegionBound).ID))

	// Plaintext Create bypasses free IDs and keeps the ordinary new-ID flow.
	require.NoError(t, storage.RunInTxn(ctx, func(txn kv.Txn) error {
		return txn.Save(keypath.KeyspaceFreePath(keyspaceID+1), encodeFreeID(2, 3))
	}))
	created, err := manager.CreateKeyspace(&CreateKeyspaceRequest{
		Name:       "plaintext-new",
		CreateTime: manager.reuse.now().Unix(),
	})
	require.NoError(t, err)
	require.NotEqual(t, keyspaceID+1, created.GetId())
	var free string
	require.NoError(t, storage.RunInTxn(ctx, func(txn kv.Txn) error {
		var err error
		free, err = txn.Load(keypath.KeyspaceFreePath(keyspaceID + 1))
		return err
	}))
	require.NotEmpty(t, free)
}
