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
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"

	"github.com/pingcap/errors"

	mcsconstant "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

type idReuseConfig interface {
	IsIDReuseEnabled() bool
	GetIDReuseThreshold() uint64
	GetIDReuseBucketBytes() int
	GetIDReuseLoadPageBuckets() int
	GetIDReuseLoadInterval() time.Duration
	GetIDReuseReclaimInterval() time.Duration
}

type idReuse struct {
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	bitmap    *freeIDBitmap
	limiter   *rate.Limiter
	store     *reuseLeaderStore
	watermark uint64
	threshold uint64
}

type reuseLeaderStore struct {
	freeIDBitmapStore
	leaderKey, leaderValue string
}

// RunInTxnWithConditions fences writes against the current PD leader key.
func (s *reuseLeaderStore) RunInTxnWithConditions(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	guarded := append([]clientv3.Cmp{clientv3.Compare(clientv3.Value(s.leaderKey), "=", s.leaderValue)}, conditions...)
	return s.freeIDBitmapStore.RunInTxnWithConditions(ctx, guarded, f)
}

func keyspaceFreePrefix() string { return fmt.Sprintf("/pd/%d/keyspaces/free/", keypath.ClusterID()) }

// StartIDReuse starts the shared free-ID service for reclamation and reuse.
// It owns the leader-local bitmap loader and request admission. When disabled it
// returns before any storage access. Call StopIDReuse when leadership ends.
func (manager *Manager) StartIDReuse(backend kv.Base, leaderKey, leaderValue string) error {
	manager.reuseLifecycleMu.Lock()
	defer manager.reuseLifecycleMu.Unlock()
	manager.stopIDReuseLocked()
	cfg, ok := manager.config.(idReuseConfig)
	if !ok || !cfg.IsIDReuseEnabled() {
		return nil
	}
	if cluster, ok := manager.cluster.(interface{ IsServiceIndependent(string) bool }); ok && cluster.IsServiceIndependent(mcsconstant.SchedulingServiceName) {
		return errors.New("keyspace ID reuse requires integrated scheduling")
	}
	store, ok := backend.(freeIDBitmapStore)
	if !ok || leaderKey == "" || leaderValue == "" {
		return errors.New("keyspace ID reuse requires revision-aware storage and a leader identity")
	}
	ctx, cancel := context.WithCancel(manager.ctx)
	guarded := &reuseLeaderStore{freeIDBitmapStore: store, leaderKey: leaderKey, leaderValue: leaderValue}
	bitmap, err := newFreeIDBitmap(guarded, keyspaceFreePrefix(), cfg.GetIDReuseBucketBytes())
	if err != nil {
		cancel()
		return err
	}
	if err = bitmap.initialize(ctx); err != nil {
		cancel()
		return err
	}
	response, err := store.CreateRawTxnWithContext(ctx).Then(kv.RawTxnOp{Key: keypath.KeyspaceAllocIDPath(), OpType: kv.RawTxnOpGet}).Commit()
	if err != nil {
		cancel()
		return err
	}
	var watermark uint64
	if len(response.Responses) == 1 && len(response.Responses[0].KeyValuePairs) == 1 {
		watermark, err = typeutil.BytesToUint64([]byte(response.Responses[0].KeyValuePairs[0].Value))
		if err != nil {
			cancel()
			return err
		}
	}
	r := &idReuse{ctx: ctx, cancel: cancel, done: make(chan struct{}), bitmap: bitmap, store: guarded, watermark: watermark,
		threshold: cfg.GetIDReuseThreshold(), limiter: rate.NewLimiter(rate.Every(cfg.GetIDReuseReclaimInterval()), 1)}
	manager.reuse.Store(r)
	go func() {
		defer close(r.done)
		_ = bitmap.runLoader(ctx, uint32(cfg.GetIDReuseLoadPageBuckets()), cfg.GetIDReuseLoadInterval())
	}()
	return nil
}

// StopIDReuse cancels and joins the loader before a new leader cache is installed.
func (manager *Manager) StopIDReuse() {
	manager.reuseLifecycleMu.Lock()
	defer manager.reuseLifecycleMu.Unlock()
	manager.stopIDReuseLocked()
}
func (manager *Manager) stopIDReuseLocked() {
	if r := manager.reuse.Swap(nil); r != nil {
		r.cancel()
		<-r.done
	}
}

func (manager *Manager) reuseAdmitted(r *idReuse) bool {
	high := r.watermark
	if alloc, ok := manager.idAllocator.(interface{ GetReservedWindowEnd() uint64 }); ok {
		high = max(high, alloc.GetReservedWindowEnd())
	}
	return manager.reuse.Load() == r && r.ctx.Err() == nil && high >= r.threshold
}

// RequestKeyspaceReclaim handles an external reclamation request after checking
// admission and rate limits. It returns true when the old metadata is removed
// and the ID is persisted as free; false means the request made no change.
func (manager *Manager) RequestKeyspaceReclaim(ctx context.Context, id uint32) (bool, error) {
	r := manager.reuse.Load()
	if r == nil || !manager.reuseAdmitted(r) {
		return false, errors.New("keyspace ID reuse is disabled or below its allocation threshold")
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := r.limiter.Wait(ctx); err != nil {
		return false, err
	}
	return manager.reclaimKeyspace(ctx, r.bitmap, id)
}

// rejectFreeID prevents explicit-ID Create from bypassing the bitmap claim.
// It is used only when reuse is enabled; the comparison joins the metadata txn.
func (manager *Manager) rejectFreeID(txn kv.Txn, id uint32) error {
	r := manager.reuse.Load()
	if r == nil {
		return nil
	}
	width := uint32(r.bitmap.bucketBytes * 8)
	value, err := txn.Load(r.bitmap.bucketKey(id / width))
	if err != nil {
		return err
	}
	if value != "" && (len(value) != r.bitmap.bucketBytes || value[(id%width)/8]&(1<<(id%8)) != 0) {
		return errors.New("free keyspace IDs must be claimed through CreateKeyspace")
	}
	return nil
}
