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

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestReclaimRequiresCompleteDeletedRightNeighbor(t *testing.T) {
	for _, state := range []keyspacepb.KeyspaceState{keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE} {
		t.Run(state.String(), func(t *testing.T) {
			_, cluster, manager := newReuseTestManager(t)
			saveReuseTestMeta(t, manager, 42, keyspacepb.KeyspaceState_TOMBSTONE)
			saveReuseTestMeta(t, manager, 43, state)
			// Covering only the beginning of the next ID cannot release this ID.
			region := newReuseTestRegion(42, 420).Clone(core.WithEndKey(append(MakeRegionBound(43).TxnLeftBound, byte(1))))
			cluster.PutRegion(region)
			require.NoError(t, manager.RequestKeyspaceReclaim(42))
			_, err := manager.LoadKeyspaceByID(42)
			require.NoError(t, err)
			cluster.PutRegion(region.Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
			require.NoError(t, manager.RequestKeyspaceReclaim(42))
			_, err = manager.LoadKeyspaceByID(42)
			if state == keyspacepb.KeyspaceState_TOMBSTONE {
				require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReclaimDistinguishesFreeNeighborFromMissingMetadata(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	saveReuseTestMeta(t, manager, 42, keyspacepb.KeyspaceState_TOMBSTONE)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	_, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 43, true) }))
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	_, err = manager.LoadKeyspaceByID(42)
	require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
}

func TestReclaimOrdersTombstonesAcrossMetadataHoles(t *testing.T) {
	_, cluster, manager := newReuseTestManager(t)
	for _, id := range []uint32{42, 43, 80, 81} {
		saveReuseTestMeta(t, manager, id, keyspacepb.KeyspaceState_TOMBSTONE)
	}
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(81).TxnRightBound)))
	require.NoError(t, manager.RequestKeyspaceReclaim(80))
	_, err := manager.LoadKeyspaceByID(80)
	require.NoError(t, err)
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	// Retain 43 before the unallocated hole, but allow the next deleted group.
	require.NoError(t, manager.RequestKeyspaceReclaim(80))
	_, err = manager.LoadKeyspaceByID(80)
	require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
	require.NoError(t, manager.RequestKeyspaceReclaim(43))
	_, err = manager.LoadKeyspaceByID(43)
	require.NoError(t, err)
	first, err := manager.tryCreateReusedKeyspace("before-hole", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(42), first.GetId())
	next, err := manager.tryCreateReusedKeyspace("after-hole", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(80), next.GetId())
}

func TestLowerMetadataScanDefersAtBound(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	for id := uint32(1); id <= freeIDScanLimit; id++ {
		saveReuseTestMeta(t, manager, id, keyspacepb.KeyspaceState_ENABLED)
	}
	region := newReuseTestRegion(1, 420).Clone(core.WithEndKey(MakeRegionBound(100).TxnRightBound))
	cluster.PutRegion(region)
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		ready, err := manager.lowerTombstonesReclaimed(txn, region, 99, false)
		require.False(t, ready)
		return err
	}))
}

func TestClaimsPreserveRegionOrderAcrossBitmapPagesWithoutWaitingForSplits(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	const last = uint32(32*256 + 42)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(last + 1).TxnRightBound)))
	for bucket := uint32(0); bucket <= 32; bucket++ {
		id := bucket*256 + 42
		require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, id, true) }))
	}
	// Begin after a complete page; the higher candidate must not overtake ID 42.
	manager.reuse.scanStart = bitmapPath(32)
	created, err := manager.tryCreateReusedKeyspace("later-page", nil, 1)
	require.NoError(t, err)
	require.Nil(t, created)
	for bucket := uint32(0); bucket <= 1; bucket++ {
		// Put the cursor on the lowest remaining bucket. Pending splits never finish.
		manager.reuse.scanStart = bitmapPath(bucket)
		created, err = manager.tryCreateReusedKeyspace(fmt.Sprintf("tenant-%d", bucket), nil, 1)
		require.NoError(t, err)
		require.NotNil(t, created)
		require.Equal(t, bucket*256+42, created.GetId())
	}
	require.Len(t, manager.reuse.pending, 2)
}

func TestClaimWaitsForLowerTombstoneAfterRegionsMerge(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	for _, id := range []uint32{43, 45} {
		saveReuseTestMeta(t, manager, id, keyspacepb.KeyspaceState_TOMBSTONE)
	}
	for _, id := range []uint32{42, 44} {
		require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, id, true) }))
	}
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(45).TxnRightBound)))
	first, err := manager.tryCreateReusedKeyspace("first-run", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(42), first.GetId())
	next, err := manager.tryCreateReusedKeyspace("second-run", nil, 1)
	require.NoError(t, err)
	require.Nil(t, next)
	require.NoError(t, manager.RequestKeyspaceReclaim(43))
	next, err = manager.tryCreateReusedKeyspace("bridge", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(43), next.GetId())
	next, err = manager.tryCreateReusedKeyspace("second-run", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(44), next.GetId())
}

type orderedReuseKV struct {
	kv.Base
	afterRange  func(string)
	afterLoad   func(string)
	afterCommit func()
}
type orderedReuseTxn struct {
	kv.Txn
	owner   *orderedReuseKV
	claimed bool
}

func (b *orderedReuseKV) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	var wrapped *orderedReuseTxn
	err := b.Base.RunInTxn(ctx, func(txn kv.Txn) error {
		wrapped = &orderedReuseTxn{Txn: txn, owner: b}
		return f(wrapped)
	})
	if err == nil && wrapped.claimed && b.afterCommit != nil {
		b.afterCommit()
	}
	return err
}
func (t *orderedReuseTxn) Save(key, value string) error {
	if key == keypath.KeyspaceMetaPath(42) {
		t.claimed = true
	}
	return t.Txn.Save(key, value)
}
func (t *orderedReuseTxn) LoadRange(key, end string, limit int) ([]string, []string, error) {
	keys, values, err := t.Txn.LoadRange(key, end, limit)
	if err == nil && t.owner.afterRange != nil {
		t.owner.afterRange(key)
	}
	return keys, values, err
}

func TestClaimRevisionRejectsNewLowerBitmapBucket(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	backend := &orderedReuseKV{Base: kv.NewEtcdKVBase(client)}
	storage, cluster, manager := newReuseTestManagerWithKV(t, backend)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(300).TxnRightBound)))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 298, true) }))
	manager.reuse.scanStart = bitmapPath(1)
	injected := false
	backend.afterRange = func(key string) {
		if key != bitmapPath(0) || injected {
			return
		}
		injected = true
		// Insert a previously absent lower bucket after the range read. Only the
		// revision read before scans can detect this phantom at transaction commit.
		require.NoError(t, backend.Base.RunInTxn(manager.ctx, func(txn kv.Txn) error {
			if err := manager.changeFreeBit(txn, 42, true); err != nil {
				return err
			}
			_, err := advanceNameRevision(txn)
			return err
		}))
	}
	created, err := manager.tryCreateReusedKeyspace("higher", nil, 1)
	require.NoError(t, err)
	require.True(t, injected)
	require.Nil(t, created)
	_, err = manager.LoadKeyspaceByID(298)
	require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
}

func TestClaimMutexCoversCommittedMetadataBeforePendingCache(t *testing.T) {
	backend := &orderedReuseKV{Base: kv.NewMemoryKV()}
	storage, cluster, manager := newReuseTestManagerWithKV(t, backend)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(44).TxnRightBound)))
	for _, id := range []uint32{42, 43} {
		require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, id, true) }))
	}
	checked := false
	backend.afterCommit = func() {
		checked = true
		locked := manager.reuse.claimMu.TryLock()
		if locked {
			manager.reuse.claimMu.Unlock()
		}
		require.False(t, locked)
		require.NotContains(t, manager.reuse.pending, uint32(42))
	}
	first, err := manager.tryCreateReusedKeyspace("lower", nil, 1)
	require.NoError(t, err)
	require.True(t, checked)
	require.Equal(t, uint32(42), first.GetId())
	require.Contains(t, manager.reuse.pending, uint32(42))
	second, err := manager.tryCreateReusedKeyspace("higher", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(43), second.GetId())
	require.Contains(t, manager.reuse.pending, uint32(43))
}

func TestClaimBoundsEtcdComparisonsAcrossBlockedRegions(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	storage, cluster, manager := newReuseTestManagerWithKV(t, kv.NewEtcdKVBase(client))
	// Each blocked Region exhausts the bounded lower-metadata scan. Combining
	// these checks in one transaction would exceed etcd's default 128-op limit.
	for bucket := uint32(0); bucket < 5; bucket++ {
		first := bucket*256 + 1
		for id := first; id < first+freeIDScanLimit; id++ {
			saveReuseTestMeta(t, manager, id, keyspacepb.KeyspaceState_ENABLED)
		}
		candidate := bucket*256 + 42
		cluster.PutRegion(newReuseTestRegion(first, uint64(first)).Clone(core.WithEndKey(MakeRegionBound(candidate + 1).TxnRightBound)))
		require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, candidate, true) }))
	}
	const eligible = uint32(5*256 + 42)
	cluster.PutRegion(newReuseTestRegion(eligible, uint64(eligible)).Clone(core.WithEndKey(MakeRegionBound(eligible + 1).TxnRightBound)))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, eligible, true) }))
	created, err := manager.tryCreateReusedKeyspace("after-blocked-regions", nil, 1)
	require.NoError(t, err)
	require.NotNil(t, created)
	require.Equal(t, eligible, created.GetId())
}

func (t *orderedReuseTxn) Load(key string) (string, error) {
	value, err := t.Txn.Load(key)
	if err == nil && t.owner.afterLoad != nil {
		t.owner.afterLoad(key)
	}
	return value, err
}

func TestClaimRejectsConcurrentCreationInMetadataHole(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	backend := &orderedReuseKV{Base: kv.NewEtcdKVBase(client)}
	storage, cluster, manager := newReuseTestManagerWithKV(t, backend)
	saveReuseTestMeta(t, manager, 42, keyspacepb.KeyspaceState_TOMBSTONE)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(81).TxnRightBound)))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 80, true) }))
	injected := false
	backend.afterLoad = func(key string) {
		if key != keypath.KeyspaceMetaPath(43) || injected {
			return
		}
		injected = true
		// Ordinary creation fills the hole without changing name_revision. The
		// claim must conflict on its absent-neighbor read, then defer on retry.
		require.NoError(t, backend.Base.RunInTxn(manager.ctx, func(txn kv.Txn) error {
			return manager.store.SaveKeyspaceMeta(txn, &keyspacepb.KeyspaceMeta{
				Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 43}, Name: "fills-hole", State: keyspacepb.KeyspaceState_DISABLED,
			})
		}))
	}
	created, err := manager.tryCreateReusedKeyspace("after-hole", nil, 1)
	require.NoError(t, err)
	require.True(t, injected)
	require.Nil(t, created)
	require.Zero(t, manager.KeyspaceNameRevision())
	_, err = manager.LoadKeyspaceByID(80)
	require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
}
