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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

func newReuseTestRegion(id uint32, regionID uint64) *core.RegionInfo {
	bound := MakeRegionBound(id)
	peer := &metapb.Peer{Id: regionID + 100, StoreId: 1}
	return core.NewRegionInfo(&metapb.Region{Id: regionID, StartKey: bound.TxnLeftBound, EndKey: bound.TxnRightBound, RegionEpoch: &metapb.RegionEpoch{Version: 5, ConfVer: 3}, Peers: []*metapb.Peer{peer}}, peer)
}
func newReuseTestManager(t *testing.T) (*endpoint.StorageEndpoint, *mockcluster.Cluster, *Manager) {
	return newReuseTestManagerWithKV(t, kv.NewMemoryKV())
}
func newReuseTestManagerWithKV(t *testing.T, backend kv.Base) (*endpoint.StorageEndpoint, *mockcluster.Cluster, *Manager) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	storage := endpoint.NewStorageEndpoint(backend, nil)
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	cluster.AddRegionStore(1, 1)
	allocator := mockid.NewIDAllocator()
	require.NoError(t, allocator.SetBase(100))
	kgm := NewKeyspaceGroupManager(ctx, storage, nil)
	require.NoError(t, kgm.Bootstrap(ctx))
	manager := NewKeyspaceManager(ctx, storage, cluster, allocator, &mockConfig{EnableIDReuse: true, IDReuseThreshold: 10, IDReuseWorkerInterval: typeutil.NewDuration(time.Hour), WaitRegionSplitTimeout: typeutil.NewDuration(100 * time.Millisecond), CheckRegionSplitInterval: typeutil.NewDuration(time.Millisecond)}, kgm, nil)
	manager.reuse.now = func() time.Time { return time.Unix(4_000_000, 0) }
	manager.RecordIDReuseCapability(1, 3)
	var version atomic.Uint64
	version.Store(1_000_000)
	manager.SetResetVersionAllocator(func(context.Context) (uint64, error) { return version.Add(1), nil })
	require.NoError(t, manager.ensureBitmapLayout())
	return storage, cluster, manager
}
func saveReuseTestMeta(t *testing.T, manager *Manager, id uint32, state keyspacepb.KeyspaceState) {
	require.NoError(t, manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: state.String() + "-" + string(rune(id+'a')), State: state, Config: map[string]string{}, CreatedAt: 1, StateChangedAt: 2}))
}

func prepareIsolatedReuse(t *testing.T, manager *Manager, cluster *mockcluster.Cluster) {
	for id := uint32(41); id <= 43; id++ {
		state := keyspacepb.KeyspaceState_ENABLED
		if id == 42 {
			state = keyspacepb.KeyspaceState_TOMBSTONE
		}
		saveReuseTestMeta(t, manager, id, state)
		cluster.PutRegion(newReuseTestRegion(id, uint64(id)))
	}
}
func TestReclaimReleasesIDWithoutResetAfterMergeDelay(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	prepareIsolatedReuse(t, manager, cluster)
	meta, _ := manager.LoadKeyspaceByID(42)
	manager.reuse.now = func() time.Time { return time.Unix(meta.StateChangedAt, 0).Add(2 * 24 * time.Hour) }
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	_, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.False(t, manager.KeyspaceUpdateRequiredForRegion(42))
	manager.reuse.now = func() time.Time { return time.Unix(meta.StateChangedAt, 0).Add(3 * 24 * time.Hour) }
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	_, err = manager.LoadKeyspaceByID(42)
	require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		bits, e := txn.Load(bitmapPath(0))
		require.NotZero(t, bits[42/8]&(1<<(42%8)))
		return e
	}))
	require.Empty(t, manager.reuse.pending)
}
func TestReclaimWaitsForDeletedNeighborAndMultipleRegions(t *testing.T) {
	_, cluster, manager := newReuseTestManager(t)
	for id := uint32(41); id <= 43; id++ {
		saveReuseTestMeta(t, manager, id, keyspacepb.KeyspaceState_TOMBSTONE)
		cluster.PutRegion(newReuseTestRegion(id, uint64(id)))
	}
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	_, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	covering := cluster.GetRegion(42).Clone(core.WithStartKey(MakeRegionBound(41).TxnLeftBound))
	cluster.PutRegion(covering)
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	_, err = manager.LoadKeyspaceByID(42)
	require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
}
func TestNoSplitCreateRequiresMatchingAppliedVersionAndAllowsArchive(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 42))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 42, true) }))
	created, pending, err := manager.tryCreateReusedKeyspace("new-tenant", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, manager.reuse.now().Unix())
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.Equal(t, keyspacepb.KeyspaceState_DISABLED, created.State)
	require.NotZero(t, created.PendingResetVersion)
	target := created.PendingResetVersion
	region := cluster.GetRegion(42)
	manager.RecordRegionStorageState(region, &pdpb.RegionStorageState{RestoreVersion: target - 1})
	manager.advanceReuseTask(42)
	current, _ := manager.LoadKeyspaceByID(42)
	require.Equal(t, target, current.PendingResetVersion)
	_, err = manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_ENABLED, 3)
	require.Error(t, err)
	// Timeout preserves the same claim; manager can query DISABLED and abandon it.
	_, err = manager.waitReusedCreate(created)
	require.Error(t, err)
	current, _ = manager.LoadKeyspace("new-tenant")
	require.Equal(t, target, current.PendingResetVersion)
	_, _, err = manager.tryCreateReusedKeyspace("new-tenant", created.Config, 4)
	require.ErrorIs(t, err, errs.ErrKeyspaceExists)
	require.NoError(t, manager.StartIDReuse())
	t.Cleanup(manager.StopIDReuse)
	manager.RecordIDReuseCapability(1, 3)
	manager.advanceReuseTask(42)
	require.True(t, manager.KeyspaceUpdateRequiredForRegion(42))
	_, err = manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_ARCHIVED, 5)
	require.NoError(t, err)
	manager.RecordRegionStorageState(region, &pdpb.RegionStorageState{RestoreVersion: target})
	manager.advanceReuseTask(42)
	current, _ = manager.LoadKeyspaceByID(42)
	require.Equal(t, keyspacepb.KeyspaceState_ARCHIVED, current.State)
	require.Zero(t, current.PendingResetVersion)
	require.False(t, manager.KeyspaceUpdateRequiredForRegion(42))
}
func TestAppliedResetEnablesSameOwnerAndClearsPendingVersion(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 42))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 42, true) }))
	created, _, err := manager.tryCreateReusedKeyspace("ready-tenant", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, manager.reuse.now().Unix())
	require.NoError(t, err)
	old := cluster.GetRegion(42)
	manager.RecordRegionStorageState(old, &pdpb.RegionStorageState{RestoreVersion: created.PendingResetVersion})
	region := old.Clone(core.WithIncVersion())
	cluster.PutRegion(region)
	manager.advanceReuseTask(42)
	current, _ := manager.LoadKeyspaceByID(42)
	require.Equal(t, keyspacepb.KeyspaceState_DISABLED, current.State)
	manager.RecordRegionStorageState(region, &pdpb.RegionStorageState{RestoreVersion: created.PendingResetVersion})
	// An ordinary heartbeat without the optional payload does not erase a report.
	manager.RecordRegionStorageState(region, nil)
	manager.advanceReuseTask(42)
	current, _ = manager.LoadKeyspaceByID(42)
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, current.State)
	require.Zero(t, current.PendingResetVersion)
	require.False(t, manager.KeyspaceUpdateRequiredForRegion(42))
	require.Error(t, manager.completeReusedCreate(created))
}
func TestSplitReuseDoesNotAllocateResetVersion(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	larger := newReuseTestRegion(42, 42).Clone(core.WithStartKey(MakeRegionBound(41).TxnLeftBound), core.WithEndKey(MakeRegionBound(43).TxnRightBound))
	cluster.PutRegion(larger)
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 42, true) }))
	manager.SetResetVersionAllocator(func(context.Context) (uint64, error) { t.Error("split must not allocate T"); return 0, nil })
	// Simulate the existing split scheduler forming both boundaries while Create waits.
	go func() {
		time.Sleep(10 * time.Millisecond)
		cluster.PutRegion(newReuseTestRegion(42, 42))
		cluster.PutRegion(newReuseTestRegion(43, 43))
	}()
	created, pending, err := manager.tryCreateReusedKeyspace("split-tenant", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, manager.reuse.now().Unix())
	require.NoError(t, err)
	require.Nil(t, pending)
	require.Zero(t, created.PendingResetVersion)
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, created.State)
	require.False(t, manager.KeyspaceUpdateRequiredForRegion(42))
}

func TestBitmapUpdatesPreserveOtherIDsAndLayout(t *testing.T) {
	storage, _, manager := newReuseTestManager(t)
	for _, id := range []uint32{42, 43, 999} {
		require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, id, true) }))
	}
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 42, false) }))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		bits, e := txn.Load(bitmapPath(0))
		require.Zero(t, bits[42/8]&(1<<(42%8)))
		require.NotZero(t, bits[43/8]&(1<<(43%8)))
		return e
	}))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return txn.Save(keyspaceBitmapLayoutPath(), "v1:64") }))
	require.Error(t, manager.ensureBitmapLayout())
}
func TestConcurrentClaimsUseDifferentBits(t *testing.T) {
	// MemoryKV does not enforce transaction read conditions; use real etcd CAS.
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	storage, cluster, manager := newReuseTestManagerWithKV(t, kv.NewEtcdKVBase(client))
	for _, id := range []uint32{42, 43} {
		cluster.PutRegion(newReuseTestRegion(id, uint64(id)))
		require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, id, true) }))
	}
	type result struct {
		id  uint32
		err error
	}
	results := make(chan result, 2)
	for _, name := range []string{"tenant-one", "tenant-two"} {
		go func(name string) {
			meta, _, err := manager.tryCreateReusedKeyspace(name, map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, manager.reuse.now().Unix())
			results <- result{meta.GetId(), err}
		}(name)
	}
	one, two := <-results, <-results
	require.NoError(t, one.err)
	require.NoError(t, two.err)
	require.NotEqual(t, one.id, two.id)
}

func TestReclaimRejectsProtectedIDsAndRawKeyspaces(t *testing.T) {
	_, cluster, manager := newReuseTestManager(t)
	for _, id := range []uint32{0, uint32(constant.ReservedKeyspaceIDStart), constant.SystemKeyspaceID, constant.MaxValidKeyspaceID} {
		require.Error(t, manager.RequestKeyspaceReclaim(id))
	}
	require.NoError(t, manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42}, Name: "raw",
		State:  keyspacepb.KeyspaceState_TOMBSTONE,
		Config: map[string]string{RegionBoundType: "raw"},
	}))
	cluster.PutRegion(newReuseTestRegion(42, 42))
	require.ErrorContains(t, manager.RequestKeyspaceReclaim(42), "transactional")
}

func TestReuseWatermarkSurvivesRestartWithoutNewAllocation(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	region := newReuseTestRegion(42, 42)
	cluster.PutRegion(region)
	require.NoError(t, manager.idAllocator.SetBase(0))
	require.Error(t, manager.reuseAdmissionReady(region))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		return txn.Save(keypath.KeyspaceAllocIDPath(), string(typeutil.Uint64ToBytes(1000)))
	}))
	require.NoError(t, manager.StartIDReuse())
	t.Cleanup(manager.StopIDReuse)
	manager.RecordIDReuseCapability(1, 3)
	require.NoError(t, manager.reuseAdmissionReady(region))
}

func TestCMEKRequestLeavesFreeIDsForStarterReuse(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 42))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 42, true) }))
	created, pending, err := manager.tryCreateReusedKeyspace("cmek-tenant", map[string]string{"encryption": `{"enabled":true,"vendor":"test","cmek_id":"test-customer-key"}`}, 1)
	require.NoError(t, err)
	require.Nil(t, created)
	require.Nil(t, pending)
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		bits, e := txn.Load(bitmapPath(0))
		require.NotZero(t, bits[42/8]&(1<<(42%8)))
		return e
	}))
}
