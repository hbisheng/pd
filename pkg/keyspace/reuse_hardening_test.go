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
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/storage/kv"
)

func claimSplitReuse(t *testing.T) (*mockcluster.Cluster, *Manager, *keyspacepb.KeyspaceMeta) {
	_, cluster, manager := newReuseTestManager(t)
	source := newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound))
	cluster.PutRegion(source)
	seedReuseTestBit(t, manager, 42, true)
	meta, err := manager.tryCreateReusedKeyspace("split-tenant", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, 1)
	require.NoError(t, err)
	return cluster, manager, meta
}

func applyReuseTestSplit(cluster *mockcluster.Cluster, childID uint64) {
	left := newReuseTestRegion(42, childID).Clone(core.WithIncVersion())
	right := newReuseTestRegion(43, 420).Clone(core.WithIncVersion())
	cluster.PutRegion(left)
	cluster.PutRegion(right)
}

func TestReusedCreateCompletesFromRegionBoundaries(t *testing.T) {
	cluster, manager, pending := claimSplitReuse(t)
	require.NoError(t, manager.tryCompleteReusedCreate(pending))
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_DISABLED, current.State)

	// Region heartbeats are sufficient; no split grant or report is recorded.
	applyReuseTestSplit(cluster, 42)
	current, err = manager.waitReusedCreate(pending)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, current.State)
	require.Empty(t, manager.reuse.pending)
}

func TestNeighborSplitCanCompleteOrderedCreates(t *testing.T) {
	_, cluster, manager := newReuseTestManager(t)
	cluster.PutRegion(newReuseTestRegion(41, 420).Clone(core.WithEndKey(MakeRegionBound(44).TxnRightBound)))
	for _, id := range []uint32{42, 43} {
		seedReuseTestBit(t, manager, id, true)
	}
	config := map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}
	first, err := manager.tryCreateReusedKeyspace("first-tenant", config, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(42), first.GetId())
	next, err := manager.tryCreateReusedKeyspace("next-tenant", config, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(43), next.GetId())

	// Both configurations are durable before the later Create's split runs.
	for _, expected := range []*keyspacepb.KeyspaceMeta{first, next} {
		stored, err := manager.LoadKeyspaceByID(expected.GetId())
		require.NoError(t, err)
		require.Equal(t, expected, stored)
	}
	cluster.PutRegion(newReuseTestRegion(41, 500).Clone(core.WithEndKey(MakeRegionBound(42).TxnRightBound)))
	cluster.PutRegion(newReuseTestRegion(43, 501))
	cluster.PutRegion(newReuseTestRegion(44, 420))
	_, err = manager.waitReusedCreate(next)
	require.NoError(t, err)
	require.NoError(t, manager.tryCompleteReusedCreate(first))
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_DISABLED, current.State)

	// A second split forms the missing boundary; 42 retains intermediate ID 500.
	cluster.PutRegion(newReuseTestRegion(41, 502))
	cluster.PutRegion(newReuseTestRegion(42, 500).Clone(core.WithIncVersion()))
	current, err = manager.waitReusedCreate(first)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, current.State)
}

func TestReusedCreateTimeoutArchivesAndKeepsIDClaimed(t *testing.T) {
	cluster, manager, pending := claimSplitReuse(t)
	// Without the requested split, Create times out and follows normal deletion.
	_, err := manager.waitReusedCreate(pending)
	require.ErrorContains(t, err, "timed out")
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ARCHIVED, current.State)
	require.NotNil(t, cluster.GetRegionLabeler().GetLabelRule(buildLabelRule(42, txnRegionBound).ID))
	require.NoError(t, manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		bits, err := txn.Load(bitmapPath(0))
		require.Empty(t, bits)
		return err
	}))
}

func TestRestartLeavesInterruptedCreateDisabled(t *testing.T) {
	_, manager, pending := claimSplitReuse(t)
	manager.reuse = newReuseState()
	require.NoError(t, manager.StartIDReuse())
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_DISABLED, current.State)
	require.Empty(t, manager.reuse.pending)
	require.Error(t, manager.ensureReuseRegionRule(pending))
	require.Error(t, manager.completeReusedCreate(pending))
	_, err = manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_ARCHIVED, 2)
	require.NoError(t, err)
}

func TestDisabledKeyspaceUsesOrdinaryStateAndConfigUpdates(t *testing.T) {
	_, manager, pending := claimSplitReuse(t)
	mutations := []*Mutation{{Op: OpPut, Key: "encryption", Value: `{"enabled":true}`}}
	_, err := manager.UpdateKeyspaceConfig(pending.Name, mutations)
	require.NoError(t, err)
	// A delayed completion must not apply the old creation configuration.
	require.Error(t, manager.completeReusedCreate(pending))
	_, err = manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_ENABLED, 2)
	require.NoError(t, err)
	_, err = manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_DISABLED, 3)
	require.NoError(t, err)
	require.Empty(t, manager.reuse.pending)
}

func TestArchiveRejectsLateAppliedSplit(t *testing.T) {
	cluster, manager, pending := claimSplitReuse(t)
	_, err := manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_ARCHIVED, 2)
	require.NoError(t, err)
	applyReuseTestSplit(cluster, 42)
	require.Error(t, manager.tryCompleteReusedCreate(pending))
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ARCHIVED, current.State)
	require.Empty(t, manager.reuse.pending)
}
