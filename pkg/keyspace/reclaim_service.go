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
)

// Load small numeric ranges at a fixed pace to limit background etcd reads.
const (
	freeIDLoadPageBuckets = 64
	freeIDLoadInterval    = 100 * time.Millisecond
)

type idReclaimConfig interface {
	IsIDReclaimEnabled() bool
	GetFreeIDBucketBytes() int
	GetIDReclaimInterval() time.Duration
}

// freeIDService owns the bitmap cache and loader for one PD leadership term.
// Reclamation fills the bitmap; Create continues using the increasing allocator.
type freeIDService struct {
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	bitmap  *freeIDBitmap
	limiter *rate.Limiter
}

type freeIDLeaderStore struct {
	freeIDBitmapStore
	leaderKey, leaderValue string
}

// RunInTxnWithConditions fences writes against the current PD leader key.
func (s *freeIDLeaderStore) RunInTxnWithConditions(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	guarded := append([]clientv3.Cmp{clientv3.Compare(clientv3.Value(s.leaderKey), "=", s.leaderValue)}, conditions...)
	return s.freeIDBitmapStore.RunInTxnWithConditions(ctx, guarded, f)
}

func keyspaceFreePrefix() string { return fmt.Sprintf("/pd/%d/keyspaces/free/", keypath.ClusterID()) }

// StartFreeIDService starts the leader-local bitmap loader and reclamation limiter.
// When reclamation is disabled, it returns without accessing storage or starting
// a goroutine. Call StopFreeIDService when leadership ends.
func (manager *Manager) StartFreeIDService(backend kv.Base, leaderKey, leaderValue string) error {
	manager.freeIDLifecycleMu.Lock()
	defer manager.freeIDLifecycleMu.Unlock()
	manager.stopFreeIDServiceLocked()
	cfg, ok := manager.config.(idReclaimConfig)
	if !ok || !cfg.IsIDReclaimEnabled() {
		return nil
	}
	if cluster, ok := manager.cluster.(interface{ IsServiceIndependent(string) bool }); ok && cluster.IsServiceIndependent(mcsconstant.SchedulingServiceName) {
		return errors.New("keyspace ID reclamation requires integrated scheduling")
	}
	store, ok := backend.(freeIDBitmapStore)
	if !ok || leaderKey == "" || leaderValue == "" {
		return errors.New("keyspace ID reclamation requires revision-aware storage and a leader identity")
	}
	guarded := &freeIDLeaderStore{freeIDBitmapStore: store, leaderKey: leaderKey, leaderValue: leaderValue}
	bitmap, err := newFreeIDBitmap(guarded, keyspaceFreePrefix(), cfg.GetFreeIDBucketBytes())
	if err != nil {
		return err
	}
	if err = bitmap.initialize(manager.ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(manager.ctx)
	service := &freeIDService{
		ctx: ctx, cancel: cancel, done: make(chan struct{}), bitmap: bitmap,
		limiter: rate.NewLimiter(rate.Every(cfg.GetIDReclaimInterval()), 1),
	}
	manager.freeIDs.Store(service)
	go func() {
		defer close(service.done)
		_ = bitmap.runLoader(ctx, freeIDLoadPageBuckets, freeIDLoadInterval)
	}()
	return nil
}

// StopFreeIDService cancels and joins the loader before a new leader cache is installed.
func (manager *Manager) StopFreeIDService() {
	manager.freeIDLifecycleMu.Lock()
	defer manager.freeIDLifecycleMu.Unlock()
	manager.stopFreeIDServiceLocked()
}

func (manager *Manager) stopFreeIDServiceLocked() {
	if service := manager.freeIDs.Swap(nil); service != nil {
		service.cancel()
		<-service.done
	}
}

// RequestKeyspaceReclaim handles an external reclamation request after checking
// the startup switch and rate limit. It returns true when the old metadata is
// removed and the ID is persisted as free; false means the request made no change.
func (manager *Manager) RequestKeyspaceReclaim(ctx context.Context, id uint32) (bool, error) {
	service := manager.freeIDs.Load()
	if service == nil || service.ctx.Err() != nil {
		return false, errors.New("keyspace ID reclamation is disabled")
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(service.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := service.limiter.Wait(ctx); err != nil {
		return false, err
	}
	return manager.reclaimKeyspace(ctx, service.bitmap, id)
}

// rejectFreeID prevents explicit-ID Create from bypassing the bitmap claim.
// It is inactive when the service is disabled; the read joins the metadata txn.
func (manager *Manager) rejectFreeID(txn kv.Txn, id uint32) error {
	service := manager.freeIDs.Load()
	if service == nil {
		return nil
	}
	width := uint32(service.bitmap.bucketBytes * 8)
	value, err := txn.Load(service.bitmap.bucketKey(id / width))
	if err != nil {
		return err
	}
	if value != "" && (len(value) != service.bitmap.bucketBytes || value[(id%width)/8]&(1<<(id%8)) != 0) {
		return errors.New("free keyspace IDs must be claimed through CreateKeyspace")
	}
	return nil
}
