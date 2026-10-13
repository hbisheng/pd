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

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
)

func TestSplitInitializationSurvivesTimeoutAndRestart(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 42, true) }))
	manager.SetResetVersionAllocator(func(context.Context) (uint64, error) {
		t.Error("normal split must not allocate a reset version")
		return 0, nil
	})
	config := map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}
	_, _, err := manager.tryCreateReusedKeyspace("split-timeout", config, 1)
	require.Error(t, err)
	pending, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.True(t, pending.PendingInitialization)
	require.Zero(t, pending.PendingResetVersion)
	require.Equal(t, keyspacepb.KeyspaceState_DISABLED, pending.State)
	ruleID := buildLabelRule(42, txnRegionBound).ID
	require.NotNil(t, cluster.GetRegionLabeler().GetLabelRule(ruleID), "timeout must keep the split rule")

	// The guard is persisted, so losing every process-local map cannot bypass it.
	manager.reuse = newReuseState()
	manager.SetResetVersionAllocator(func(context.Context) (uint64, error) {
		t.Error("recovered split must not allocate a reset version")
		return 0, nil
	})
	_, err = manager.UpdateKeyspaceState("split-timeout", keyspacepb.KeyspaceState_ENABLED, 2)
	require.Error(t, err)
	_, err = manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_ENABLED, 2)
	require.Error(t, err)
	mutations := []*Mutation{{Op: OpPut, Key: "encryption", Value: `{"enabled":true}`}}
	_, err = manager.UpdateKeyspaceConfig("split-timeout", mutations)
	require.Error(t, err)
	_, err = manager.UpdateKeyspaceConfigWithPreconditions("split-timeout", mutations, map[string]*string{"encryption": nil})
	require.Error(t, err)

	// Also cover a crash before the initial boundary rule was installed.
	require.NoError(t, cluster.GetRegionLabeler().DeleteLabelRule(ruleID))
	require.NoError(t, manager.StartIDReuse())
	t.Cleanup(manager.StopIDReuse)
	manager.RecordIDReuseCapability(1, 4)
	manager.advanceReuseTask(42)
	require.NotNil(t, cluster.GetRegionLabeler().GetLabelRule(ruleID))
	manager.SetKeyspaceUpdateNotifier(func(region *core.RegionInfo) {
		manager.RecordRegionStorageState(region, &pdpb.RegionStorageState{Encrypted: false})
	})
	cluster.PutRegion(newReuseTestRegion(42, 42))
	cluster.PutRegion(newReuseTestRegion(43, 43))
	manager.advanceReuseTask(42)
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, current.State)
	require.False(t, current.PendingInitialization)
	require.Zero(t, current.PendingResetVersion)
	require.False(t, manager.KeyspaceUpdateRequiredForRegion(42))
	_, _, err = manager.tryCreateReusedKeyspace("split-timeout", config, 3)
	require.ErrorIs(t, err, errs.ErrKeyspaceExists)
}

func TestArchiveCancelsPendingSplitAndLateCompletion(t *testing.T) {
	storage, cluster, manager := newReuseTestManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 42, true) }))
	_, _, err := manager.tryCreateReusedKeyspace("canceled-split", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, 1)
	require.Error(t, err)
	pending, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	canceled, err := manager.UpdateKeyspaceState("canceled-split", keyspacepb.KeyspaceState_ARCHIVED, 2)
	require.NoError(t, err)
	require.False(t, canceled.PendingInitialization)
	ruleID := buildLabelRule(42, txnRegionBound).ID
	require.NoError(t, cluster.GetRegionLabeler().DeleteLabelRule(ruleID))
	cluster.PutRegion(newReuseTestRegion(42, 42))
	manager.RecordRegionStorageState(cluster.GetRegion(42), &pdpb.RegionStorageState{})
	manager.advanceReuseTask(42)
	require.Error(t, manager.ensureReuseRegionRule(pending))
	require.Error(t, manager.completeReusedCreate(pending))
	require.Error(t, manager.prepareSplitFallbackReset(pending))
	require.Nil(t, cluster.GetRegionLabeler().GetLabelRule(ruleID))
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ARCHIVED, current.State)
	require.Zero(t, current.PendingResetVersion)
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		bits, err := txn.Load(bitmapPath(0))
		require.Empty(t, bits, "cancellation must not immediately free the claimed ID")
		return err
	}))
}

func TestSplitFallbackKeepsPersistedResetTarget(t *testing.T) {
	_, _, manager := newReuseTestManager(t)
	meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42}, Name: "split-fallback", State: keyspacepb.KeyspaceState_DISABLED, PendingInitialization: true, CreatedAt: 1}
	require.NoError(t, manager.saveNewKeyspace(meta))
	require.NoError(t, manager.prepareSplitFallbackReset(meta))
	first, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.NotZero(t, first.PendingResetVersion)
	require.Error(t, manager.prepareSplitFallbackReset(meta))
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, first.PendingResetVersion, current.PendingResetVersion)
}
