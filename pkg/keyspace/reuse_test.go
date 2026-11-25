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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/server/config"
)

func newReuseFixture(t *testing.T, encrypted bool) (*Manager, *mockcluster.Cluster, kv.Base) {
	t.Helper()
	m, cluster, backend := newReclaimServiceFixture(t)
	if encrypted {
		m.StopIDReuse()
		m.config.(*config.KeyspaceConfig).EnableEncryptedIDReuse = true
		require.NoError(t, m.StartIDReuse(backend, reuseTestLeaderKey, "leader"))
	}
	return m, cluster, backend
}

func waitReuseRule(t *testing.T, cluster *mockcluster.Cluster, id uint32) {
	t.Helper()
	require.Eventually(t, func() bool {
		return cluster.GetRegionLabeler().GetLabelRule(buildLabelRule(id, txnRegionBound).ID) != nil
	}, 5*time.Second, time.Millisecond)
}

func TestIDReuseCreateUsesExistingSplit(t *testing.T) {
	for _, encryption := range []string{"", `{"enabled":true}`} {
		t.Run(fmt.Sprintf("encryption=%s", encryption), func(t *testing.T) {
			m, cluster, _ := newReuseFixture(t, true)
			m.mgm = NewMetaServiceGroupManager(m.store.(*endpoint.StorageEndpoint), map[string]string{"group": "http://127.0.0.1:1"}, nil)
			enabled := true
			require.NoError(t, m.mgm.PatchStatus(m.ctx, "group", &MetaServiceGroupStatusPatch{Enabled: &enabled}))
			reclaimForReuse(t, m, cluster)
			finished := make(chan error, 1)
			go func() {
				meta, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "new-tenant", CreateTime: 10, Config: map[string]string{"encryption": encryption}})
				if err == nil && (meta.GetId() != 42 || meta.State != keyspacepb.KeyspaceState_ENABLED) {
					err = fmt.Errorf("unexpected result: %v", meta)
				}
				finished <- err
			}()
			waitReuseRule(t, cluster, 42)
			pending, _, err := m.store.LoadKeyspaceMetaWithRevision(m.ctx, 42)
			require.NoError(t, err)
			require.Equal(t, keyspacepb.KeyspaceState_DISABLED, pending.State)
			require.Equal(t, encryption, pending.Config["encryption"])
			// Simulate the ordinary Region heartbeat after splitting the merged range.
			cluster.PutRegion(reclaimRegion(42, 42))
			cluster.PutRegion(reclaimRegion(43, 43))
			require.NoError(t, <-finished)
			_, free := m.reuse.Load().bitmap.candidates().Next()
			require.False(t, free)
			name, err := m.GetKeyspaceNameByID(42)
			require.NoError(t, err)
			require.Equal(t, "new-tenant", name)
			counts, err := m.mgm.GetAssignmentCounts(m.ctx)
			require.NoError(t, err)
			require.Equal(t, 1, counts["group"])
		})
	}
}

func TestIDReuseTimeoutAndCancellation(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("archive=%t", cancel), func(t *testing.T) {
			m, cluster, _ := newReuseFixture(t, false)
			reclaimForReuse(t, m, cluster)
			done := make(chan error, 1)
			go func() {
				_, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "unfinished", CreateTime: 10})
				done <- err
			}()
			waitReuseRule(t, cluster, 42)
			if cancel {
				_, err := m.UpdateKeyspaceState("unfinished", keyspacepb.KeyspaceState_ARCHIVED, 11)
				require.NoError(t, err)
				cluster.PutRegion(reclaimRegion(42, 42))
				cluster.PutRegion(reclaimRegion(43, 43))
			}
			require.Error(t, <-done)
			meta, _, err := m.store.LoadKeyspaceMetaWithRevision(m.ctx, 42)
			require.NoError(t, err)
			require.Equal(t, keyspacepb.KeyspaceState_ARCHIVED, meta.State)
			_, free := m.reuse.Load().bitmap.candidates().Next()
			require.False(t, free)
		})
	}
}

func TestIDReuseSkipsUnreclaimedLowerID(t *testing.T) {
	m, cluster, _ := newReuseFixture(t, false)
	for _, id := range []uint32{100, 101, 102} {
		seedReclaimMeta(t, m, id, keyspacepb.KeyspaceState_TOMBSTONE)
	}
	cluster.PutRegion(reclaimRegion(100, 102))
	ok, err := m.RequestKeyspaceReclaim(m.ctx, 101)
	require.NoError(t, err)
	require.True(t, ok)
	meta, err := m.claimFreeKeyspace(m.reuse.Load(), "new-101", nil, 10)
	require.NoError(t, err)
	require.Equal(t, uint32(101), meta.GetId())
}

func TestIDReuseBucketStartIgnoresNeighborCache(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid=%t", invalid), func(t *testing.T) {
			m, cluster, _ := newReuseFixture(t, false)
			for _, id := range []uint32{511, 512, 513} {
				seedReclaimMeta(t, m, id, keyspacepb.KeyspaceState_TOMBSTONE)
			}
			cluster.PutRegion(reclaimRegion(511, 513))
			r := m.reuse.Load()
			ok, err := m.RequestKeyspaceReclaim(m.ctx, 512)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = m.RequestKeyspaceReclaim(m.ctx, 511)
			require.NoError(t, err)
			require.False(t, ok)
			if invalid {
				r.bitmap.mu.Lock()
				r.bitmap.invalidateBucketLocked(0)
				r.bitmap.mu.Unlock()
			}
			// Claim 512 without loading or repairing the preceding bucket.
			meta, err := m.claimFreeKeyspace(r, "first", nil, 10)
			require.NoError(t, err)
			require.NotNil(t, meta)
			require.Equal(t, uint32(512), meta.GetId())
			r.bitmap.mu.Lock()
			defer r.bitmap.mu.Unlock()
			require.Zero(t, r.bitmap.nextLoad)
			require.NotContains(t, r.bitmap.buckets, uint32(0))
		})
	}
}

func TestIDReuseConcurrentClaimsAndLostReply(t *testing.T) {
	m, cluster, _ := newReuseFixture(t, false)
	reclaimForReuse(t, m, cluster)
	r := m.reuse.Load()
	wrapped := &bitmapTestStore{freeIDBitmapStore: r.store.freeIDBitmapStore, loseReply: true}
	r.store.freeIDBitmapStore = wrapped
	var wg sync.WaitGroup
	results := make(chan *keyspacepb.KeyspaceMeta, 2)
	failures := make(chan error, 2)
	for _, name := range []string{"first", "second"} {
		wg.Go(func() {
			meta, err := m.claimFreeKeyspace(r, name, nil, 10)
			results <- meta
			failures <- err
		})
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	claimed := 0
	for meta := range results {
		if meta != nil {
			claimed++
		}
	}
	require.Equal(t, 1, claimed)
}

func TestIDReuseLeaderChangeAndCacheReload(t *testing.T) {
	m, cluster, backend := newReuseFixture(t, false)
	reclaimForReuse(t, m, cluster)
	old := m.reuse.Load()
	require.NoError(t, backend.Save(reuseTestLeaderKey, "next-leader"))
	meta, err := m.claimFreeKeyspace(old, "stale", nil, 10)
	require.NoError(t, err)
	require.Nil(t, meta)
	m.StopIDReuse()
	require.Error(t, old.ctx.Err())
	require.NoError(t, m.StartIDReuse(backend, reuseTestLeaderKey, "next-leader"))
	r := m.reuse.Load()
	require.NotSame(t, old, r)
	require.NoError(t, r.bitmap.loadNextPage(context.Background(), 1))
	meta, err = m.claimFreeKeyspace(r, "current", nil, 10)
	require.NoError(t, err)
	require.Equal(t, uint32(42), meta.GetId())
}

func TestIDReuseRejectsExactRegionAndExplicitCreate(t *testing.T) {
	m, cluster, _ := newReuseFixture(t, false)
	reclaimForReuse(t, m, cluster)
	id := uint32(42)
	_, err := m.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &id, Name: "bypass", CreateTime: 10})
	require.ErrorContains(t, err, "must be claimed")
	cluster.PutRegion(reclaimRegion(42, 42))
	cluster.PutRegion(reclaimRegion(43, 43))
	meta, err := m.claimFreeKeyspace(m.reuse.Load(), "exact", nil, 10)
	require.NoError(t, err)
	require.Nil(t, meta)
}

func TestIDReuseEncryptionAdmission(t *testing.T) {
	for _, tc := range []struct {
		value         string
		enabled, want bool
	}{
		{"", false, true}, {`{"enabled":false}`, false, true},
		{`{"enabled":true}`, false, false}, {`{"enabled":true}`, true, true},
		{`{"enabled":true,"cmek_id":"customer"}`, true, false}, {"invalid", true, false},
	} {
		require.Equal(t, tc.want, supportsReuseEncryption(map[string]string{"encryption": tc.value}, tc.enabled))
	}
}

func TestIDReuseCancellationBeforeRuleInstallation(t *testing.T) {
	m, cluster, _ := newReuseFixture(t, false)
	reclaimForReuse(t, m, cluster)
	r := m.reuse.Load()
	meta, err := m.claimFreeKeyspace(r, "cancelled", nil, 10)
	require.NoError(t, err)
	require.NotNil(t, meta)
	_, err = m.UpdateKeyspaceState("cancelled", keyspacepb.KeyspaceState_ARCHIVED, 11)
	require.NoError(t, err)
	require.ErrorContains(t, m.initializeReusedKeyspace(r, meta), "cancelled")
	require.Nil(t, cluster.GetRegionLabeler().GetLabelRule(buildLabelRule(42, txnRegionBound).ID))
}

func TestIDReuseCancelledFinishDoesNotAssignTSOGroup(t *testing.T) {
	for _, mode := range []string{"archive", "leader-stop", "leader-change"} {
		t.Run(mode, func(t *testing.T) {
			m, cluster, backend := newReuseFixture(t, false)
			reclaimForReuse(t, m, cluster)
			r := m.reuse.Load()
			meta, err := m.claimFreeKeyspace(r, "cancelled", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, 10)
			require.NoError(t, err)
			group, err := m.kgm.GetKeyspaceGroupByID(0)
			require.NoError(t, err)
			require.NotContains(t, group.Keyspaces, uint32(42))
			switch mode {
			case "archive":
				_, err = m.UpdateKeyspaceState("cancelled", keyspacepb.KeyspaceState_ARCHIVED, 11)
				require.NoError(t, err)
			case "leader-stop":
				m.StopIDReuse()
			default:
				require.NoError(t, backend.Save(reuseTestLeaderKey, "next-leader"))
			}
			require.Error(t, m.finishReusedKeyspace(r, meta, keyspacepb.KeyspaceState_ENABLED))
			group, err = m.kgm.GetKeyspaceGroupByID(0)
			require.NoError(t, err)
			require.NotContains(t, group.Keyspaces, uint32(42), "a rejected completion must not change TSO membership")
		})
	}
}

func TestIDReuseCreateRecordsTotalMetric(t *testing.T) {
	m, cluster, _ := newReuseFixture(t, false)
	reclaimForReuse(t, m, cluster)
	before := getCreateKeyspaceStepCounts(require.New(t))[StepTotal]
	done := make(chan error, 1)
	go func() {
		_, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "metric-reuse", CreateTime: 10})
		done <- err
	}()
	waitReuseRule(t, cluster, 42)
	cluster.PutRegion(reclaimRegion(42, 42))
	cluster.PutRegion(reclaimRegion(43, 43))
	require.NoError(t, <-done)
	after := getCreateKeyspaceStepCounts(require.New(t))[StepTotal]
	require.Equal(t, before+1, after)
}

func TestIDReuseNeighborSplitCompletesOrderedCreates(t *testing.T) {
	m, cluster, _ := newReuseFixture(t, false)
	for _, id := range []uint32{42, 43, 44} {
		seedReclaimMeta(t, m, id, keyspacepb.KeyspaceState_TOMBSTONE)
	}
	cluster.PutRegion(reclaimRegion(42, 44))
	for _, id := range []uint32{42, 43} {
		ok, err := m.RequestKeyspaceReclaim(m.ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
	}
	done := make(chan error, 2)
	for _, id := range []uint32{42, 43} {
		go func() {
			meta, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: fmt.Sprintf("reuse-%d", id), CreateTime: 10})
			if err == nil && meta.GetId() != id {
				err = fmt.Errorf("claimed %d, expected %d", meta.GetId(), id)
			}
			done <- err
		}()
		waitReuseRule(t, cluster, id)
	}
	// The right boundary arrives first; a later split completes both Creates.
	cluster.PutRegion(reclaimRegion(42, 43))
	cluster.PutRegion(reclaimRegion(44, 44))
	select {
	case err := <-done:
		t.Fatalf("Create finished before its boundaries existed: %v", err)
	default:
	}
	cluster.PutRegion(reclaimRegion(42, 42))
	cluster.PutRegion(reclaimRegion(43, 43))
	require.NoError(t, <-done)
	require.NoError(t, <-done)
}

func TestIDReuseFallbackPreservesFreeID(t *testing.T) {
	for _, mode := range []string{"cold-cache", "cmek"} {
		t.Run(mode, func(t *testing.T) {
			m, cluster, backend := newReuseFixture(t, true)
			reclaimForReuse(t, m, cluster)
			config := map[string]string{"encryption": `{"enabled":true,"cmek_id":"customer"}`}
			if mode == "cold-cache" {
				m.StopIDReuse()
				require.NoError(t, m.StartIDReuse(backend, reuseTestLeaderKey, "leader"))
				config = nil
			}
			require.NoError(t, failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion")) })
			meta, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "ordinary-fallback", Config: config, CreateTime: 10})
			require.NoError(t, err)
			require.Equal(t, uint32(1), meta.GetId())
			bitmap := m.reuse.Load().bitmap
			require.NoError(t, bitmap.loadNextPage(m.ctx, 1))
			id, ok := bitmap.candidates().Next()
			require.True(t, ok)
			require.Equal(t, uint32(42), id)
		})
	}
}

func TestIDReuseSameNameClaimsPreserveOtherFreeID(t *testing.T) {
	m, cluster, _ := newReuseFixture(t, false)
	for _, id := range []uint32{42, 43, 44} {
		seedReclaimMeta(t, m, id, keyspacepb.KeyspaceState_TOMBSTONE)
	}
	cluster.PutRegion(reclaimRegion(42, 44))
	for _, id := range []uint32{42, 43} {
		ok, err := m.RequestKeyspaceReclaim(m.ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
	}
	r := m.reuse.Load()
	results := make(chan *keyspacepb.KeyspaceMeta, 2)
	failures := make(chan error, 2)
	for range 2 {
		go func() {
			meta, err := m.claimFreeKeyspace(r, "same-name", nil, 10)
			results <- meta
			failures <- err
		}()
	}
	claimed := 0
	for range 2 {
		if meta := <-results; meta != nil {
			claimed++
		}
		if err := <-failures; err != nil {
			require.ErrorIs(t, err, errs.ErrKeyspaceExists)
		}
	}
	require.Equal(t, 1, claimed)
	// The failed transaction invalidates its bucket; reload before reuse.
	require.NoError(t, r.bitmap.loadNextPage(m.ctx, 1))
	id, ok := r.bitmap.candidates().Next()
	require.True(t, ok)
	require.Equal(t, uint32(43), id)
}

func TestIDReuseInterruptedClaimRemainsOwned(t *testing.T) {
	for _, mode := range []string{"restart", "split-before-initialization"} {
		t.Run(mode, func(t *testing.T) {
			m, cluster, backend := newReuseFixture(t, false)
			reclaimForReuse(t, m, cluster)
			r := m.reuse.Load()
			meta, err := m.claimFreeKeyspace(r, "unfinished", nil, 10)
			require.NoError(t, err)
			if mode == "restart" {
				m.StopIDReuse()
				require.NoError(t, m.StartIDReuse(backend, reuseTestLeaderKey, "leader"))
				r = m.reuse.Load()
			} else {
				cluster.PutRegion(reclaimRegion(42, 42))
				cluster.PutRegion(reclaimRegion(43, 43))
				require.ErrorContains(t, m.initializeReusedKeyspace(r, meta), "no longer requires split")
			}
			current, _, err := m.store.LoadKeyspaceMetaWithRevision(m.ctx, 42)
			require.NoError(t, err)
			require.Equal(t, keyspacepb.KeyspaceState_DISABLED, current.State)
			require.NoError(t, m.finishReusedKeyspace(r, meta, keyspacepb.KeyspaceState_ARCHIVED))
			require.NoError(t, r.bitmap.loadNextPage(m.ctx, 1))
			_, free := r.bitmap.candidates().Next()
			require.False(t, free)
		})
	}
}

func TestIDReuseBusyFallsBackWithoutWaiting(t *testing.T) {
	for _, mode := range []string{"another-claim", "bitmap-io"} {
		t.Run(mode, func(t *testing.T) {
			m, cluster, _ := newReuseFixture(t, false)
			reclaimForReuse(t, m, cluster)
			r := m.reuse.Load()
			require.NoError(t, failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion")) })
			lock := &r.claimMu
			if mode == "bitmap-io" {
				lock = &r.bitmap.mu
			}
			// Keep the competing lock held until Create finishes. The new owner
			// must come from the increasing allocator without waiting for reuse.
			func() {
				lock.Lock()
				defer lock.Unlock()
				type result struct {
					meta *keyspacepb.KeyspaceMeta
					err  error
				}
				done := make(chan result, 1)
				go func() {
					meta, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "busy-fallback", CreateTime: 10})
					done <- result{meta: meta, err: err}
				}()
				select {
				case got := <-done:
					require.NoError(t, got.err)
					require.Equal(t, uint32(1), got.meta.GetId())
					require.Equal(t, keyspacepb.KeyspaceState_ENABLED, got.meta.State)
				case <-time.After(5 * time.Second):
					t.Fatal("Create waited for the busy reuse path")
				}
			}()
			id, ok := r.bitmap.candidates().Next()
			require.True(t, ok)
			require.Equal(t, uint32(42), id)
			// Once contention ends, the same free ID can still be claimed.
			meta, err := m.claimFreeKeyspace(r, "after-busy", nil, 11)
			require.NoError(t, err)
			require.Equal(t, uint32(42), meta.GetId())
		})
	}
}

func TestIDReuseSkipsClaimWhenBitmapBecomesBusy(t *testing.T) {
	m, cluster, _ := newReuseFixture(t, false)
	reclaimForReuse(t, m, cluster)
	r := m.reuse.Load()
	id, ok := r.bitmap.candidates().Next()
	require.True(t, ok)
	// The loader or reclamation may take the lock after candidate selection.
	func() {
		r.bitmap.mu.Lock()
		defer r.bitmap.mu.Unlock()
		done := make(chan error, 1)
		go func() {
			meta, err := m.claimFreeKeyspaceID(r, id, "busy-claim", nil, 10)
			if meta != nil {
				err = fmt.Errorf("unexpected claim: %v", meta)
			}
			done <- err
		}()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("claim waited for bitmap I/O")
		}
	}()
	meta, _, err := m.store.LoadKeyspaceMetaWithRevision(m.ctx, id)
	require.NoError(t, err)
	require.Nil(t, meta)
	stillFree, ok := r.bitmap.candidates().Next()
	require.True(t, ok)
	require.Equal(t, id, stillFree)
}

// Inject failures after staging or committing the final state, without changing
// the read-only store used to reconcile the outcome.
type reuseCompletionTestStore struct {
	freeIDBitmapStore
	abort  bool
	cancel context.CancelFunc
}

func (s *reuseCompletionTestStore) RunInTxnWithConditions(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	err := s.freeIDBitmapStore.RunInTxnWithConditions(ctx, conditions, func(txn kv.Txn) error {
		if err := f(txn); err != nil {
			return err
		}
		if s.abort {
			return errors.New("injected abort before commit")
		}
		return nil
	})
	if err != nil {
		return err
	}
	if s.cancel != nil {
		s.cancel()
	}
	return errors.New("injected lost commit reply")
}

func TestIDReuseCompletionReconcilesCommitOutcome(t *testing.T) {
	for _, tc := range []struct {
		name          string
		state         keyspacepb.KeyspaceState
		abort, cancel bool
	}{
		{name: "enabled", state: keyspacepb.KeyspaceState_ENABLED},
		{name: "archived", state: keyspacepb.KeyspaceState_ARCHIVED},
		{name: "expired request", state: keyspacepb.KeyspaceState_ENABLED, cancel: true},
		{name: "aborted transaction", state: keyspacepb.KeyspaceState_ENABLED, abort: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, cluster, _ := newReuseFixture(t, false)
			reclaimForReuse(t, m, cluster)
			r := m.reuse.Load()
			meta, err := m.claimFreeKeyspace(r, "new-owner", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, 10)
			require.NoError(t, err)
			require.NotNil(t, meta)
			ctx, cancel := context.WithCancel(r.ctx)
			defer cancel()
			store := *r.store
			fault := &reuseCompletionTestStore{freeIDBitmapStore: store.freeIDBitmapStore, abort: tc.abort}
			if tc.cancel {
				fault.cancel = cancel
			}
			store.freeIDBitmapStore = fault
			err = m.finishReusedKeyspace(&idReuse{ctx: ctx, store: &store}, meta, tc.state)
			want := tc.state
			if tc.abort {
				require.Error(t, err)
				want = keyspacepb.KeyspaceState_DISABLED
			} else {
				require.NoError(t, err)
			}
			persisted, _, err := m.store.LoadKeyspaceMetaWithRevision(m.ctx, 42)
			require.NoError(t, err)
			require.Equal(t, want, persisted.State)
			cached, err := m.GetKeyspaceStateByID(42)
			require.NoError(t, err)
			require.Equal(t, want, cached)
			group, err := m.kgm.GetKeyspaceGroupByID(0)
			require.NoError(t, err)
			cachedGroup, cacheErr := m.kgm.GetGroupByKeyspaceID(42)
			if want == keyspacepb.KeyspaceState_ENABLED {
				require.Contains(t, group.Keyspaces, uint32(42))
				require.NoError(t, cacheErr)
				require.Zero(t, cachedGroup)
				name, err := m.GetEnabledKeyspaceNameByID(42)
				require.NoError(t, err)
				require.Equal(t, "new-owner", name)
			} else {
				require.NotContains(t, group.Keyspaces, uint32(42))
				require.Error(t, cacheErr)
			}
		})
	}
}

func TestIDReuseMovesTSOMembershipToNewGroup(t *testing.T) {
	m, cluster, _ := newReuseFixture(t, false)
	require.NoError(t, m.kgm.UpdateKeyspaceForGroup(endpoint.Basic, "0", 42, opAdd))
	reclaimForReuse(t, m, cluster)
	require.NoError(t, m.kgm.CreateKeyspaceGroups([]*endpoint.KeyspaceGroup{{ID: 1, UserKind: "basic"}}))
	r := m.reuse.Load()
	meta, err := m.claimFreeKeyspace(r, "new-owner", map[string]string{TSOKeyspaceGroupIDKey: "1", UserKindKey: "basic"}, 10)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.NoError(t, m.finishReusedKeyspace(r, meta, keyspacepb.KeyspaceState_ENABLED))
	oldGroup, err := m.kgm.GetKeyspaceGroupByID(0)
	require.NoError(t, err)
	require.NotContains(t, oldGroup.Keyspaces, uint32(42))
	newGroup, err := m.kgm.GetKeyspaceGroupByID(1)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, newGroup.Keyspaces)
	cachedGroup, err := m.kgm.GetGroupByKeyspaceID(42)
	require.NoError(t, err)
	require.Equal(t, uint32(1), cachedGroup)
}

func TestIDReuseCancellationRefreshesCommittedCompletion(t *testing.T) {
	m, cluster, _ := newReuseFixture(t, false)
	reclaimForReuse(t, m, cluster)
	r := m.reuse.Load()
	meta, err := m.claimFreeKeyspace(r, "late-completion", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, 10)
	require.NoError(t, err)
	// Model a completion that commits after readback, before cancellation.
	// Keep the claim's DISABLED cache to reproduce the lost completion result.
	func() {
		m.kgm.Lock()
		defer m.kgm.Unlock()
		require.NoError(t, m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
			if _, err := m.kgm.updateKeyspaceForGroupTxn(txn, endpoint.Basic, 0, 42, opAdd); err != nil {
				return err
			}
			persisted, err := m.store.LoadKeyspaceMeta(txn, 42)
			if err != nil {
				return err
			}
			persisted.State = keyspacepb.KeyspaceState_ENABLED
			return m.store.SaveKeyspaceMeta(txn, persisted)
		}))
	}()
	require.NoError(t, m.finishReusedKeyspace(r, meta, keyspacepb.KeyspaceState_ARCHIVED))
	persisted, _, err := m.store.LoadKeyspaceMetaWithRevision(m.ctx, 42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, persisted.State)
	name, err := m.GetEnabledKeyspaceNameByID(42)
	require.NoError(t, err)
	require.Equal(t, "late-completion", name)
	group, err := m.kgm.GetGroupByKeyspaceID(42)
	require.NoError(t, err)
	require.Zero(t, group)
}
