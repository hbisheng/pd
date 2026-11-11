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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
)

const reuseTestLeaderKey = "/reuse-test-leader"

func newReclaimServiceFixture(t *testing.T) (*Manager, *mockcluster.Cluster, kv.Base) {
	t.Helper()
	m, _, cluster, _ := newReclaimFixture(t)
	store := m.store.(*endpoint.StorageEndpoint)
	m.kgm = NewKeyspaceGroupManager(m.ctx, store, nil)
	require.NoError(t, m.kgm.Bootstrap(m.ctx))
	m.config = &config.KeyspaceConfig{
		EnableIDReuse:            true,
		IDReuseLoadInterval:      typeutil.NewDuration(time.Hour),
		IDReuseReclaimInterval:   typeutil.NewDuration(time.Nanosecond),
		WaitRegionSplitTimeout:   typeutil.NewDuration(time.Second),
		CheckRegionSplitInterval: typeutil.NewDuration(time.Millisecond),
	}
	require.NoError(t, store.Save(reuseTestLeaderKey, "leader"))
	require.NoError(t, m.StartIDReuse(store.Base, reuseTestLeaderKey, "leader"))
	t.Cleanup(m.StopIDReuse)
	return m, cluster, store.Base
}

func reclaimForReuse(t *testing.T, m *Manager, cluster *mockcluster.Cluster) {
	t.Helper()
	const first, last uint32 = 42, 43
	for id := first; id <= last; id++ {
		seedReclaimMeta(t, m, id, keyspacepb.KeyspaceState_TOMBSTONE)
	}
	cluster.PutRegion(reclaimRegion(first, last))
	for id := first; id < last; id++ {
		ok, err := m.RequestKeyspaceReclaim(m.ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
	}
}

func TestIDReuseDisabledHasNoStorageEffects(t *testing.T) {
	m, _, _, _ := newReclaimFixture(t)
	m.config = &config.KeyspaceConfig{}
	require.NoError(t, m.StartIDReuse(nil, "", ""))
	require.Nil(t, m.reuse.Load())
	_, err := m.RequestKeyspaceReclaim(m.ctx, 42)
	require.ErrorContains(t, err, "disabled")
	// Explicit-ID checks add no storage reads when reuse is off.
	require.NoError(t, m.rejectFreeID(nil, 42))
	store := m.store.(*endpoint.StorageEndpoint)
	m.kgm = NewKeyspaceGroupManager(m.ctx, store, nil)
	require.NoError(t, m.kgm.Bootstrap(m.ctx))
	require.NoError(t, failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion")) })
	meta, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "ordinary", CreateTime: 10})
	require.NoError(t, err)
	require.Equal(t, uint32(1), meta.GetId())
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, meta.State)
	keys, _, err := store.LoadRange(keyspaceFreePrefix(), keyspaceFreePrefix()+"~", 1)
	require.NoError(t, err)
	require.Empty(t, keys)
}

func TestIDReuseThresholdUsesPersistedAllocator(t *testing.T) {
	m, cluster, backend := newReclaimServiceFixture(t)
	m.StopIDReuse()
	cfg := m.config.(*config.KeyspaceConfig)
	cfg.IDReuseThreshold = 100
	require.NoError(t, m.StartIDReuse(backend, reuseTestLeaderKey, "leader"))
	_, err := m.RequestKeyspaceReclaim(m.ctx, 42)
	require.ErrorContains(t, err, "threshold")
	m.StopIDReuse()
	require.NoError(t, backend.Save(keypath.KeyspaceAllocIDPath(), string(typeutil.Uint64ToBytes(100))))
	require.NoError(t, m.StartIDReuse(backend, reuseTestLeaderKey, "leader"))
	reclaimForReuse(t, m, cluster)
}

func TestReclaimServiceRejectsOldLeader(t *testing.T) {
	m, cluster, backend := newReclaimServiceFixture(t)
	seedReclaimMeta(t, m, 42, keyspacepb.KeyspaceState_TOMBSTONE)
	seedReclaimMeta(t, m, 43, keyspacepb.KeyspaceState_TOMBSTONE)
	cluster.PutRegion(reclaimRegion(42, 43))
	require.NoError(t, backend.Save(reuseTestLeaderKey, "next-leader"))
	reclaimed, err := m.RequestKeyspaceReclaim(m.ctx, 42)
	require.Error(t, err)
	require.False(t, reclaimed)
	meta, _, err := m.store.LoadKeyspaceMetaWithRevision(m.ctx, 42)
	require.NoError(t, err)
	require.NotNil(t, meta)
	old := m.reuse.Load()
	m.StopIDReuse()
	require.Error(t, old.ctx.Err())
	require.NoError(t, m.StartIDReuse(backend, reuseTestLeaderKey, "next-leader"))
	reclaimed, err = m.RequestKeyspaceReclaim(m.ctx, 42)
	require.NoError(t, err)
	require.True(t, reclaimed)
}

// Track bucket reads in the metadata transaction independently of the loader.
type freeIDReadStore struct {
	kv.Base
	reads int
}

type freeIDReadTxn struct {
	kv.Txn
	store *freeIDReadStore
}

func (s *freeIDReadStore) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	return s.Base.RunInTxn(ctx, func(txn kv.Txn) error { return f(&freeIDReadTxn{Txn: txn, store: s}) })
}

func (txn *freeIDReadTxn) Load(key string) (string, error) {
	if strings.HasPrefix(key, keyspaceFreePrefix()+"buckets/") {
		txn.store.reads++
	}
	return txn.Txn.Load(key)
}

func TestReclaimServiceChecksFreeBitsOnlyForExplicitIDs(t *testing.T) {
	m, cluster, backend := newReclaimServiceFixture(t)
	reclaimForReuse(t, m, cluster)
	// Keep this Create on the increasing allocator, including with reuse integrated.
	m.reuse.Load().threshold = 1000
	counted := &freeIDReadStore{Base: backend}
	m.store.(*endpoint.StorageEndpoint).Base = counted
	require.NoError(t, failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion")) })
	meta, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "fresh", CreateTime: 10})
	require.NoError(t, err)
	require.Equal(t, uint32(1), meta.GetId())
	require.Zero(t, counted.reads, "fresh allocation must not read the free bitmap")

	id := uint32(42)
	_, err = m.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &id, Name: "bypass", CreateTime: 10})
	require.ErrorContains(t, err, "free keyspace IDs must be claimed")
	require.Equal(t, 1, counted.reads)
	current, _, err := endpoint.NewStorageEndpoint(backend, nil).LoadKeyspaceMetaWithRevision(m.ctx, id)
	require.NoError(t, err)
	require.Nil(t, current)
}
