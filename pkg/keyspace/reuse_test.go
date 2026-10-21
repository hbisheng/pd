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

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
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
	manager := NewKeyspaceManager(ctx, storage, cluster, allocator, &mockConfig{EnableIDReuse: true, IDReuseThreshold: 10, WaitRegionSplitTimeout: typeutil.NewDuration(100 * time.Millisecond), CheckRegionSplitInterval: typeutil.NewDuration(time.Millisecond)}, kgm, nil)
	manager.reuse.now = func() time.Time { return time.Unix(4_000_000, 0) }
	manager.RecordIDReuseCapability(1, reuseProtocolVersion)
	require.NoError(t, manager.ensureBitmapLayout())
	return storage, cluster, manager
}
func saveReuseTestMeta(t *testing.T, manager *Manager, id uint32, state keyspacepb.KeyspaceState) {
	require.NoError(t, manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: state.String() + "-" + string(rune(id+'a')), State: state, Config: map[string]string{}, CreatedAt: 1, StateChangedAt: 2}))
}

func prepareIsolatedReuse(t *testing.T, manager *Manager, cluster *mockcluster.Cluster) {
	for id := uint32(41); id <= 43; id++ {
		state := keyspacepb.KeyspaceState_ENABLED
		if id >= 42 {
			state = keyspacepb.KeyspaceState_TOMBSTONE
		}
		saveReuseTestMeta(t, manager, id, state)
		cluster.PutRegion(newReuseTestRegion(id, uint64(id)))
	}
}
func TestReclaimMergedIDWithoutAdditionalDelay(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	prepareIsolatedReuse(t, manager, cluster)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	meta, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	manager.reuse.now = func() time.Time { return time.Unix(meta.StateChangedAt, 0) }
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
	require.NoError(t, err) // A merge with only the left neighbor is insufficient.
	cluster.PutRegion(covering.Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	_, err = manager.LoadKeyspaceByID(42)
	require.NoError(t, err) // Cleaner must first release the lower tombstone.
	require.NoError(t, manager.RequestKeyspaceReclaim(41))
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	_, err = manager.LoadKeyspaceByID(42)
	require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
	require.NoError(t, manager.RequestKeyspaceReclaim(43))
	_, err = manager.LoadKeyspaceByID(43)
	require.NoError(t, err) // The final ID in the merged Region is retained.
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
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(44).TxnRightBound)))
	for _, id := range []uint32{42, 43} {
		require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, id, true) }))
	}
	type result struct {
		id  uint32
		err error
	}
	results := make(chan result, 2)
	for _, name := range []string{"tenant-one", "tenant-two"} {
		go func(name string) {
			meta, err := manager.tryCreateReusedKeyspace(name, map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, manager.reuse.now().Unix())
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
	manager.RecordIDReuseCapability(1, reuseProtocolVersion)
	require.NoError(t, manager.reuseAdmissionReady(region))
}

func TestCMEKRequestLeavesFreeIDsForStarterReuse(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 42))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 42, true) }))
	created, err := manager.tryCreateReusedKeyspace("cmek-tenant", map[string]string{"encryption": `{"enabled":true,"vendor":"test","cmek_id":"test-customer-key"}`}, 1)
	require.NoError(t, err)
	require.Nil(t, created)
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		bits, e := txn.Load(bitmapPath(0))
		require.NotZero(t, bits[42/8]&(1<<(42%8)))
		return e
	}))
}

func TestReclaimDefersIsolatedRegion(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	prepareIsolatedReuse(t, manager, cluster)
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	meta, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_TOMBSTONE, meta.State)
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		bits, err := txn.Load(bitmapPath(0))
		require.Empty(t, bits)
		return err
	}))
}

func TestClaimSkipsFreeIDThatNoLongerNeedsSplit(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	prepareIsolatedReuse(t, manager, cluster)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	// A later unrelated split isolates the range before Create claims the bit.
	cluster.PutRegion(newReuseTestRegion(42, 42))
	cluster.PutRegion(newReuseTestRegion(43, 43))
	meta, err := manager.tryCreateReusedKeyspace("isolated", nil, 1)
	require.NoError(t, err)
	require.Nil(t, meta)
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		bits, err := txn.Load(bitmapPath(0))
		require.NotZero(t, bits[42/8]&(1<<(42%8)))
		return err
	}))
}
