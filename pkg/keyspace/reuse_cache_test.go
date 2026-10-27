// Copyright 2026 PingCAP, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

// Record actual embedded-etcd client operations, not just storage fake calls.
type bitmapEtcdRecorder struct {
	clientv3.KV
	mu           sync.Mutex
	reads        []clientv3.Op
	comparisons  []clientv3.Cmp
	beforeCommit func() error
	afterCommit  func() error
	beforeRead   func(context.Context)
}

func (r *bitmapEtcdRecorder) recordRead(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if request, ok := req.(*etcdserverpb.RangeRequest); ok {
		options := []clientv3.OpOption{clientv3.WithLimit(request.Limit)}
		if len(request.RangeEnd) > 0 {
			options = append(options, clientv3.WithRange(string(request.RangeEnd)))
		}
		op := clientv3.OpGet(string(request.Key), options...)
		r.mu.Lock()
		r.reads = append(r.reads, op)
		before := r.beforeRead
		r.mu.Unlock()
		if before != nil && strings.HasPrefix(string(request.Key), bitmapPrefix()) {
			before(ctx)
		}
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

type bitmapRecordedTxn struct {
	clientv3.Txn
	owner         *bitmapEtcdRecorder
	mutatesBitmap bool
}

func (r *bitmapEtcdRecorder) Txn(ctx context.Context) clientv3.Txn {
	return &bitmapRecordedTxn{Txn: r.KV.Txn(ctx), owner: r}
}
func (t *bitmapRecordedTxn) If(cmps ...clientv3.Cmp) clientv3.Txn {
	t.owner.mu.Lock()
	t.owner.comparisons = append(t.owner.comparisons, cmps...)
	t.owner.mu.Unlock()
	t.Txn = t.Txn.If(cmps...)
	return t
}
func (t *bitmapRecordedTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	for _, op := range ops {
		if strings.HasPrefix(string(op.KeyBytes()), bitmapPrefix()) && !op.IsGet() {
			t.mutatesBitmap = true
		}
	}
	t.Txn = t.Txn.Then(ops...)
	return t
}
func (t *bitmapRecordedTxn) Commit() (*clientv3.TxnResponse, error) {
	t.owner.mu.Lock()
	before, after := t.owner.beforeCommit, t.owner.afterCommit
	t.owner.mu.Unlock()
	if t.mutatesBitmap && before != nil {
		if err := before(); err != nil {
			return nil, err
		}
	}
	response, err := t.Txn.Commit()
	if err == nil && response.Succeeded && t.mutatesBitmap && after != nil {
		if err := after(); err != nil {
			return nil, err
		}
	}
	return response, err
}

func newRecordedReuseManager(t *testing.T) (*bitmapEtcdRecorder, *clientv3.Client, *mockcluster.Cluster, *Manager) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	recorder := &bitmapEtcdRecorder{}
	recordedClient, err := clientv3.New(clientv3.Config{Endpoints: client.Endpoints(), Context: client.Ctx(), DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(recorder.recordRead)}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recordedClient.Close()) })
	recorder.KV = recordedClient.KV
	recordedClient.KV = recorder
	_, cluster, manager := newReuseTestManagerWithKV(t, kv.NewEtcdKVBase(recordedClient))
	t.Cleanup(manager.StopIDReuse)
	return recorder, recordedClient, cluster, manager
}

func TestFreeBitmapCacheClaimUsesRevisionWithoutDiscoveryRead(t *testing.T) {
	recorder, client, cluster, manager := newRecordedReuseManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(44).TxnRightBound)))
	seedReuseTestBit(t, manager, 42, true)
	seedReuseTestBit(t, manager, 43, true)
	previous := manager.reuse.buckets[0].ModRevision
	recorder.reads = nil
	recorder.comparisons = nil
	created, err := manager.tryCreateReusedKeyspace("cached", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(42), created.GetId())
	require.NotEmpty(t, recorder.reads, "the interceptor must observe actual etcd reads")
	for _, read := range recorder.reads {
		require.False(t, strings.HasPrefix(string(read.KeyBytes()), bitmapPrefix()), "hot claim read bitmap: %q", read.KeyBytes())
		require.NotEqual(t, keyspaceBitmapLayoutPath(), string(read.KeyBytes()))
	}
	found := false
	for _, cmp := range recorder.comparisons {
		if string(cmp.Key) == bitmapPath(0) {
			require.Equal(t, etcdserverpb.Compare_MOD, cmp.Target)
			comparison := etcdserverpb.Compare(cmp)
			require.Equal(t, previous, comparison.GetModRevision())
			found = true
		}
	}
	require.True(t, found)
	t.Logf("loaded-cache claim: bitmap/layout reads=0, target ModRevision CAS=%d", previous)
	response, err := client.Get(manager.ctx, bitmapPath(0))
	require.NoError(t, err)
	require.Equal(t, response.Kvs[0].ModRevision, manager.reuse.buckets[0].ModRevision)
	require.Greater(t, manager.reuse.buckets[0].ModRevision, previous)
	// Clearing the last bit deletes the bucket and caches absence revision zero.
	created, err = manager.tryCreateReusedKeyspace("cached-next", nil, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(43), created.GetId())
	require.Zero(t, manager.reuse.buckets[0].ModRevision)
	require.Empty(t, manager.reuse.freeBuckets)
}

func TestFreeBitmapCacheConflictReloadPreservesDistinctBits(t *testing.T) {
	_, client, cluster, manager := newRecordedReuseManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(45).TxnRightBound)))
	seedReuseTestBit(t, manager, 42, true)
	bits, err := changedFreeBit(manager.reuse.buckets[0].Value, 32, 43, true)
	require.NoError(t, err)
	_, err = client.Put(manager.ctx, bitmapPath(0), bits)
	require.NoError(t, err)
	created, err := manager.tryCreateReusedKeyspace("conflicted", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(42), created.GetId())
	require.NotZero(t, manager.reuse.buckets[0].Value[43/8]&(1<<(43%8)))
	created, err = manager.tryCreateReusedKeyspace("preserved", nil, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(43), created.GetId())
}

func TestFreeBitmapCacheReconcilesLostClaimResponse(t *testing.T) {
	recorder, _, cluster, manager := newRecordedReuseManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(44).TxnRightBound)))
	seedReuseTestBit(t, manager, 42, true)
	seedReuseTestBit(t, manager, 43, true)
	recorder.afterCommit = func() error { return context.DeadlineExceeded }
	created, err := manager.tryCreateReusedKeyspace("lost-response", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(42), created.GetId())
	require.Contains(t, manager.reuse.invalidBuckets, uint32(0))
	recorder.afterCommit = nil
	require.NoError(t, manager.loadNextFreeBucketPage(manager.ctx))
	require.Zero(t, manager.reuse.buckets[0].Value[42/8]&(1<<(42%8)))
	_, err = manager.tryCreateReusedKeyspace("lost-response", nil, 1)
	require.ErrorIs(t, err, errs.ErrKeyspaceExists)
	created, err = manager.tryCreateReusedKeyspace("other-owner", nil, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(43), created.GetId())
}

func TestFreeBitmapCacheFailedTransactionKeepsOwnerAndBitAtomic(t *testing.T) {
	recorder, _, cluster, manager := newRecordedReuseManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	seedReuseTestBit(t, manager, 42, true)
	recorder.beforeCommit = func() error { return errors.New("injected transport failure") }
	created, err := manager.tryCreateReusedKeyspace("failed", nil, 1)
	require.ErrorContains(t, err, "injected transport failure")
	require.Nil(t, created)
	_, err = manager.LoadKeyspaceByID(42)
	require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
	require.Contains(t, manager.reuse.invalidBuckets, uint32(0))
	recorder.beforeCommit = nil
	require.NoError(t, manager.loadNextFreeBucketPage(manager.ctx))
	created, err = manager.tryCreateReusedKeyspace("retry", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(42), created.GetId())
}

type reuseCacheConfig struct {
	Config
	page            int
	interval        time.Duration
	reclaimInterval time.Duration
}

func (c *reuseCacheConfig) GetIDReuseLoadPageBuckets() int           { return c.page }
func (c *reuseCacheConfig) GetIDReuseLoadInterval() time.Duration    { return c.interval }
func (c *reuseCacheConfig) GetIDReuseReclaimInterval() time.Duration { return c.reclaimInterval }

func TestFreeBitmapCacheLoadsBoundedEmptyIntervalsAndRestarts(t *testing.T) {
	recorder, _, cluster, manager := newRecordedReuseManager(t)
	manager.config = &reuseCacheConfig{Config: manager.config, page: 2, interval: 30 * time.Millisecond, reclaimInterval: time.Nanosecond}
	const id = uint32(4*256 + 42)
	cluster.PutRegion(newReuseTestRegion(id, 420).Clone(core.WithEndKey(MakeRegionBound(id + 1).TxnRightBound)))
	seedReuseTestBit(t, manager, id, true)
	recorder.reads = nil
	require.NoError(t, manager.StartIDReuse())
	created, err := manager.tryCreateReusedKeyspace("cold", nil, 1)
	require.NoError(t, err)
	require.Nil(t, created)
	require.Empty(t, manager.reuse.buckets)
	require.Eventually(t, func() bool {
		manager.reuse.claimMu.Lock()
		defer manager.reuse.claimMu.Unlock()
		return manager.reuse.loadNext >= 6
	}, time.Second, time.Millisecond)
	manager.StopIDReuse()
	recorder.mu.Lock()
	var intervals []clientv3.Op
	for _, read := range recorder.reads {
		if strings.HasPrefix(string(read.KeyBytes()), bitmapPrefix()) {
			intervals = append(intervals, read)
		}
	}
	recorder.mu.Unlock()
	require.GreaterOrEqual(t, len(intervals), 3)
	for i, read := range intervals {
		require.Equal(t, bitmapPath(uint32(i*2)), string(read.KeyBytes()))
		require.Equal(t, bitmapPath(uint32(i*2+2)), string(read.RangeBytes()))
	}
	created, err = manager.tryCreateReusedKeyspace("loaded", nil, 2)
	require.NoError(t, err)
	require.Equal(t, id, created.GetId())
	require.NoError(t, manager.StartIDReuse())
	require.Empty(t, manager.reuse.buckets)
	require.Zero(t, manager.reuse.loadNext)
	manager.StopIDReuse()
}

func TestFreeBitmapReclaimLoadsUncachedBucketBeforeChangingBit(t *testing.T) {
	_, _, cluster, manager := newRecordedReuseManager(t)
	saveReuseTestMeta(t, manager, 42, keyspacepb.KeyspaceState_TOMBSTONE)
	saveReuseTestMeta(t, manager, 43, keyspacepb.KeyspaceState_TOMBSTONE)
	seedReuseTestBit(t, manager, 80, true)
	manager.reuse.buckets = make(map[uint32]kv.RevisionedValue)
	manager.reuse.freeBuckets = nil
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	value := manager.reuse.buckets[0]
	require.NotZero(t, value.Value[42/8]&(1<<(42%8)))
	require.NotZero(t, value.Value[80/8]&(1<<(80%8)))
	require.Positive(t, value.ModRevision)
}

func TestFreeBitmapConcurrentLoadReclaimAndClaimPreserveBits(t *testing.T) {
	_, _, cluster, manager := newRecordedReuseManager(t)
	for _, id := range []uint32{42, 43, 80, 81} {
		saveReuseTestMeta(t, manager, id, keyspacepb.KeyspaceState_TOMBSTONE)
	}
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	cluster.PutRegion(newReuseTestRegion(80, 800).Clone(core.WithEndKey(MakeRegionBound(81).TxnRightBound)))
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	start := make(chan struct{})
	results := make(chan error, 3)
	go func() { <-start; results <- manager.loadNextFreeBucketPage(manager.ctx) }()
	go func() { <-start; results <- manager.RequestKeyspaceReclaim(80) }()
	go func() {
		<-start
		meta, err := manager.tryCreateReusedKeyspace("concurrent", nil, 1)
		if err == nil && meta.GetId() != 42 {
			err = errors.New("concurrent claim did not allocate the first ID")
		}
		results <- err
	}()
	close(start)
	for range 3 {
		require.NoError(t, <-results)
	}
	require.Zero(t, manager.reuse.buckets[0].Value[42/8]&(1<<(42%8)))
	require.NotZero(t, manager.reuse.buckets[0].Value[80/8]&(1<<(80%8)))
	created, err := manager.tryCreateReusedKeyspace("other-region", nil, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(80), created.GetId())
}

func TestFreeBitmapReclaimBudgetWaitsOutsideCacheLockAndCancels(t *testing.T) {
	_, _, cluster, manager := newRecordedReuseManager(t)
	manager.config = &reuseCacheConfig{Config: manager.config, page: 1, interval: time.Hour, reclaimInterval: time.Second}
	for _, id := range []uint32{42, 43} {
		saveReuseTestMeta(t, manager, id, keyspacepb.KeyspaceState_TOMBSTONE)
	}
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, manager.StartIDReuse())
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	result := make(chan error, 1)
	go func() { result <- manager.RequestKeyspaceReclaim(43) }()
	require.Eventually(t, func() bool { return manager.reuse.reclaimLimiter.Tokens() < 0 }, time.Second, time.Millisecond)
	locked := manager.reuse.claimMu.TryLock()
	if locked {
		manager.reuse.claimMu.Unlock()
	}
	require.True(t, locked)
	manager.StopIDReuse()
	require.ErrorIs(t, <-result, context.Canceled)
}

func TestFreeBitmapCanceledLoaderDoesNotPublishOldRead(t *testing.T) {
	recorder, _, _, manager := newRecordedReuseManager(t)
	manager.config = &reuseCacheConfig{Config: manager.config, page: 1, interval: time.Millisecond, reclaimInterval: time.Nanosecond}
	seedReuseTestBit(t, manager, 42, true)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	recorder.beforeRead = func(ctx context.Context) {
		once.Do(func() { close(entered) })
		<-release
	}
	require.NoError(t, manager.StartIDReuse())
	<-entered
	stopped := make(chan struct{})
	go func() { manager.StopIDReuse(); close(stopped) }()
	// Stop cancels the lifecycle context while the old loader holds claimMu.
	<-manager.reuse.loaderCtx.Done()
	close(release)
	<-stopped
	require.Empty(t, manager.reuse.buckets)
	require.Empty(t, manager.reuse.freeBuckets)
	require.Zero(t, manager.reuse.loadNext)
	recorder.beforeRead = nil
	require.NoError(t, manager.StartIDReuse())
	require.Eventually(t, func() bool {
		manager.reuse.claimMu.Lock()
		defer manager.reuse.claimMu.Unlock()
		return manager.reuse.buckets[0].Value != ""
	}, time.Second, time.Millisecond)
	manager.StopIDReuse()
}

func TestFreeBitmapReconcilesLostReclaimResponse(t *testing.T) {
	recorder, _, cluster, manager := newRecordedReuseManager(t)
	for _, id := range []uint32{42, 43} {
		saveReuseTestMeta(t, manager, id, keyspacepb.KeyspaceState_TOMBSTONE)
	}
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	recorder.afterCommit = func() error { return context.DeadlineExceeded }
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	recorder.afterCommit = nil
	_, err := manager.LoadKeyspaceByID(42)
	require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
	require.NotZero(t, manager.reuse.buckets[0].Value[42/8]&(1<<(42%8)))
	require.Positive(t, manager.reuse.buckets[0].ModRevision)
	created, err := manager.tryCreateReusedKeyspace("after-lost-reclaim", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(42), created.GetId())
}

func TestFreeBitmapColdCreateUsesOrdinaryAllocator(t *testing.T) {
	_, cluster, manager := newReuseTestManager(t)
	manager.config = &reuseCacheConfig{Config: manager.config, page: 1, interval: time.Hour, reclaimInterval: time.Nanosecond}
	seedReuseTestBit(t, manager, 42, true)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, manager.StartIDReuse())
	t.Cleanup(manager.StopIDReuse)
	created, err := manager.CreateKeyspace(&CreateKeyspaceRequest{Name: "cold-ordinary", CreateTime: 1, Config: map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}})
	require.NoError(t, err)
	require.Greater(t, created.GetId(), uint32(100))
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, created.State)
	require.Empty(t, manager.reuse.buckets)
}
