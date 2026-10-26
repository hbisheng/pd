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

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

type reuseHookKV struct {
	kv.Base
	onSave func(string)
	scans  int
}
type reuseHookTxn struct {
	kv.Txn
	owner *reuseHookKV
}

func (b *reuseHookKV) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	return b.Base.RunInTxn(ctx, func(txn kv.Txn) error { return f(&reuseHookTxn{txn, b}) })
}
func (t *reuseHookTxn) Save(key, value string) error {
	if err := t.Txn.Save(key, value); err != nil {
		return err
	}
	if t.owner.onSave != nil {
		t.owner.onSave(key)
	}
	return nil
}
func (t *reuseHookTxn) LoadRange(key, end string, limit int) ([]string, []string, error) {
	if strings.HasPrefix(key, keypath.KeyspaceMetaPrefix()) {
		t.owner.scans++
	}
	return t.Txn.LoadRange(key, end, limit)
}

func TestFreeBitmapScanReachesLaterEligibleBucket(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	for bucket := uint32(0); bucket <= 32; bucket++ {
		id := bucket*256 + 42
		require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, id, true) }))
	}
	eligible := uint32(32*256 + 42)
	cluster.PutRegion(newReuseTestRegion(eligible, uint64(eligible)).Clone(core.WithEndKey(MakeRegionBound(eligible + 1).TxnRightBound)))
	created, err := manager.tryCreateReusedKeyspace("first-page", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, 1)
	require.NoError(t, err)
	require.Nil(t, created)
	created, err = manager.tryCreateReusedKeyspace("later-page", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, 1)
	require.NoError(t, err)
	require.NotNil(t, created)
	require.Equal(t, eligible, created.GetId())
}

func TestReuseStartupDoesNotScanMetadata(t *testing.T) {
	backend := &reuseHookKV{Base: kv.NewMemoryKV()}
	_, _, manager := newReuseTestManagerWithKV(t, backend)
	for _, enabled := range []bool{true, false} {
		manager.config.(*mockConfig).EnableIDReuse = enabled
		require.NoError(t, manager.StartIDReuse())
		require.Zero(t, backend.scans)
	}
}

func TestFreeIDRevisionChangesWithReclaimAndClaim(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	prepareIsolatedReuse(t, manager, cluster)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		revision, err := loadFreeIDRevision(txn)
		require.Equal(t, uint64(1), revision)
		return err
	}))
	_, err := manager.tryCreateReusedKeyspace("new-name", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, 1)
	require.NoError(t, err)
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		revision, err := loadFreeIDRevision(txn)
		require.Equal(t, uint64(2), revision)
		return err
	}))
}

func TestSplitBeforeClaimIsCanceledWithoutInitialization(t *testing.T) {
	backend := &reuseHookKV{Base: kv.NewMemoryKV()}
	storage, cluster, manager := newReuseTestManagerWithKV(t, backend)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 42, true) }))
	backend.onSave = func(key string) {
		if key != keypath.KeyspaceMetaPath(42) {
			return
		}
		backend.onSave = nil
		// A previously prepared split wins before this claim becomes visible.
		cluster.PutRegion(newReuseTestRegion(42, 42))
		cluster.PutRegion(newReuseTestRegion(43, 43))
	}
	_, err := manager.tryCreateReusedKeyspace("racing-split", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, 1)
	require.ErrorContains(t, err, "no longer requires a split")
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ARCHIVED, current.State)
}
