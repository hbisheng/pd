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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func newReclaimFixture(t *testing.T) (*Manager, *freeIDBitmap, *mockcluster.Cluster, *bitmapTestStore) {
	t.Helper()
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(cleanup)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	backend := kv.NewEtcdKVBase(client)
	store := endpoint.NewStorageEndpoint(backend, nil)
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	manager := NewKeyspaceManager(ctx, store, cluster, mockid.NewIDAllocator(), &mockConfig{}, nil, nil)
	bitmapStore := &bitmapTestStore{freeIDBitmapStore: backend}
	b, err := newFreeIDBitmap(bitmapStore, "/reclaim/free/", 64)
	require.NoError(t, err)
	require.NoError(t, b.initialize(ctx))
	return manager, b, cluster, bitmapStore
}

func seedReclaimMeta(t *testing.T, manager *Manager, id uint32, state keyspacepb.KeyspaceState) {
	t.Helper()
	meta := &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: fmt.Sprintf("tenant-%d", id), State: state,
		CreatedAt: 1, StateChangedAt: 2, Config: map[string]string{RegionBoundType: txnRegionBound.String()},
	}
	require.NoError(t, manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		if err := manager.store.SaveKeyspaceID(txn, id, meta.Name); err != nil {
			return err
		}
		return manager.store.SaveKeyspaceMeta(txn, meta)
	}))
	manager.cache.Save(id, meta.Name, state)
	manager.keyspaceNameLookup.Store(id, meta.Name)
	manager.keyspaceStateLookup.Store(id, state)
}

func reclaimRegion(first, last uint32) *core.RegionInfo {
	peer := &metapb.Peer{Id: 10000 + uint64(first), StoreId: 1}
	return core.NewRegionInfo(&metapb.Region{Id: uint64(first), StartKey: MakeRegionBound(first).TxnLeftBound,
		EndKey: MakeRegionBound(last).TxnRightBound, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}, Peers: []*metapb.Peer{peer}}, peer)
}

func TestReclaimEligibility(t *testing.T) {
	for _, tc := range []struct {
		name        string
		first, last uint32
		next        keyspacepb.KeyspaceState
		want        bool
	}{
		{"isolated", 42, 42, keyspacepb.KeyspaceState_TOMBSTONE, false},
		{"merged only left", 41, 42, keyspacepb.KeyspaceState_TOMBSTONE, false},
		{"active neighbor", 42, 43, keyspacepb.KeyspaceState_ENABLED, false},
		{"merged right", 42, 43, keyspacepb.KeyspaceState_TOMBSTONE, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, b, cluster, _ := newReclaimFixture(t)
			seedReclaimMeta(t, m, 42, keyspacepb.KeyspaceState_TOMBSTONE)
			seedReclaimMeta(t, m, 43, tc.next)
			cluster.PutRegion(reclaimRegion(tc.first, tc.last))
			invalidations := 0
			m.SetGCBarrierInvalidator(func(id uint32) { require.Equal(t, uint32(42), id); invalidations++ })
			got, err := m.reclaimKeyspace(m.ctx, b, 42)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			meta, _, err := m.store.LoadKeyspaceMetaWithRevision(m.ctx, 42)
			require.NoError(t, err)
			if tc.want {
				require.Nil(t, meta)
				_, exists := m.keyspaceNameLookup.Load(uint32(42))
				require.False(t, exists)
				require.Equal(t, 1, invalidations)
				id, ok := b.firstFree(0, 512)
				require.True(t, ok)
				require.Equal(t, uint32(42), id)
				again, err := m.reclaimKeyspace(m.ctx, b, 42)
				require.NoError(t, err)
				require.False(t, again)
			} else {
				require.NotNil(t, meta)
				require.Zero(t, invalidations)
			}
		})
	}
}

func TestReclaimWaitsForCompleteRegion(t *testing.T) {
	m, b, cluster, _ := newReclaimFixture(t)
	seedReclaimMeta(t, m, 42, keyspacepb.KeyspaceState_TOMBSTONE)
	seedReclaimMeta(t, m, 43, keyspacepb.KeyspaceState_TOMBSTONE)
	got, err := m.reclaimKeyspace(m.ctx, b, 42)
	require.NoError(t, err)
	require.False(t, got)
	mid := append(append([]byte(nil), MakeRegionBound(42).TxnLeftBound...), byte('x'))
	cluster.PutRegion(reclaimRegion(42, 42).Clone(core.WithEndKey(mid)))
	cluster.PutRegion(reclaimRegion(43, 43).Clone(core.WithStartKey(mid)))
	got, err = m.reclaimKeyspace(m.ctx, b, 42)
	require.NoError(t, err)
	require.False(t, got)
}

func TestReclaimNeighborAlreadyFree(t *testing.T) {
	m, b, cluster, _ := newReclaimFixture(t)
	for _, id := range []uint32{511, 512, 513} {
		seedReclaimMeta(t, m, id, keyspacepb.KeyspaceState_TOMBSTONE)
	}
	cluster.PutRegion(reclaimRegion(511, 513))
	// Reclamation can arrive out of order, including across bitmap keys.
	for _, id := range []uint32{512, 511} {
		got, err := m.reclaimKeyspace(m.ctx, b, id)
		require.NoError(t, err)
		require.True(t, got)
	}
	got, err := m.reclaimKeyspace(m.ctx, b, 513)
	require.NoError(t, err)
	require.False(t, got)
	require.NoError(t, b.loadNextPage(m.ctx, 2))
	first, ok := b.firstFree(0, 1024)
	require.True(t, ok)
	require.Equal(t, uint32(511), first)
}

func TestReclaimRejectsProtectedAndLiveKeyspaces(t *testing.T) {
	m, b, cluster, _ := newReclaimFixture(t)
	for _, id := range []uint32{0, uint32(constant.ReservedKeyspaceIDStart)} {
		_, err := m.reclaimKeyspace(m.ctx, b, id)
		require.Error(t, err)
	}
	seedReclaimMeta(t, m, 42, keyspacepb.KeyspaceState_ENABLED)
	seedReclaimMeta(t, m, 43, keyspacepb.KeyspaceState_TOMBSTONE)
	cluster.PutRegion(reclaimRegion(42, 43))
	_, err := m.reclaimKeyspace(m.ctx, b, 42)
	require.Error(t, err)
	seedReclaimMeta(t, m, 42, keyspacepb.KeyspaceState_TOMBSTONE)
	require.NoError(t, m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		meta, err := m.store.LoadKeyspaceMeta(txn, 42)
		if err != nil {
			return err
		}
		meta.Config[RegionBoundType] = rawRegionBound.String()
		return m.store.SaveKeyspaceMeta(txn, meta)
	}))
	_, err = m.reclaimKeyspace(m.ctx, b, 42)
	require.Error(t, err)
}

func TestReclaimPreservesOtherNameMapping(t *testing.T) {
	m, b, cluster, _ := newReclaimFixture(t)
	seedReclaimMeta(t, m, 42, keyspacepb.KeyspaceState_TOMBSTONE)
	seedReclaimMeta(t, m, 43, keyspacepb.KeyspaceState_TOMBSTONE)
	cluster.PutRegion(reclaimRegion(42, 43))
	require.NoError(t, m.store.RunInTxn(m.ctx, func(txn kv.Txn) error { return m.store.SaveKeyspaceID(txn, 99, "tenant-42") }))
	_, err := m.reclaimKeyspace(m.ctx, b, 42)
	require.ErrorContains(t, err, "name mapping changed")
	meta, _, err := m.store.LoadKeyspaceMetaWithRevision(m.ctx, 42)
	require.NoError(t, err)
	require.NotNil(t, meta)
	_, ok := b.firstFree(0, 512)
	require.False(t, ok)
}

func TestReclaimGroupBindingIsAtomic(t *testing.T) {
	m, b, cluster, store := newReclaimFixture(t)
	m.mgm = NewMetaServiceGroupManager(m.store.(*endpoint.StorageEndpoint), map[string]string{"group": "http://127.0.0.1:1"}, nil)
	seedReclaimMeta(t, m, 42, keyspacepb.KeyspaceState_TOMBSTONE)
	seedReclaimMeta(t, m, 43, keyspacepb.KeyspaceState_TOMBSTONE)
	cluster.PutRegion(reclaimRegion(42, 43))
	require.NoError(t, m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		meta, err := m.store.LoadKeyspaceMeta(txn, 42)
		if err != nil {
			return err
		}
		meta.Config[MetaServiceGroupIDKey] = "group"
		if err := m.mgm.updateAssignmentTxn(txn, "", "group"); err != nil {
			return err
		}
		return m.store.SaveKeyspaceMeta(txn, meta)
	}))
	store.loseReply = true
	got, err := m.reclaimKeyspace(m.ctx, b, 42)
	require.NoError(t, err)
	require.True(t, got)
	store.loseReply = false
	// The failed response cannot roll back a committed deletion or decrement twice.
	_, err = m.reclaimKeyspace(m.ctx, b, 42)
	require.NoError(t, err)
	counts, err := m.mgm.GetAssignmentCounts(m.ctx)
	require.NoError(t, err)
	require.Zero(t, counts["group"])
	require.NoError(t, b.loadNextPage(m.ctx, 1))
	id, ok := b.firstFree(0, 512)
	require.True(t, ok)
	require.Equal(t, uint32(42), id)
	require.NoError(t, m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		value, err := txn.Load(keypath.KeyspaceIDPath("tenant-42"))
		require.Empty(t, value)
		return err
	}))
}
