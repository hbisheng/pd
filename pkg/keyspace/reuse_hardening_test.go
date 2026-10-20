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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/storage/kv"
)

func claimSplitReuse(t *testing.T) (*mockcluster.Cluster, *Manager, *keyspacepb.KeyspaceMeta, *metapb.Region) {
	storage, cluster, manager := newReuseTestManager(t)
	source := newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound))
	cluster.PutRegion(source)
	require.NoError(t, storage.RunInTxn(manager.ctx, func(txn kv.Txn) error { return manager.changeFreeBit(txn, 42, true) }))
	meta, err := manager.tryCreateReusedKeyspace("split-tenant", map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: "basic"}, 1)
	require.NoError(t, err)
	return cluster, manager, meta, source.GetMeta()
}

func applyReuseTestSplit(cluster *mockcluster.Cluster, childID uint64) []*metapb.Region {
	left := newReuseTestRegion(42, childID).Clone(core.WithIncVersion())
	right := newReuseTestRegion(43, 420).Clone(core.WithIncVersion())
	cluster.PutRegion(left)
	cluster.PutRegion(right)
	return []*metapb.Region{left.GetMeta(), right.GetMeta()}
}

func TestSplitCompletionRequiresMatchingGrantAndReport(t *testing.T) {
	cluster, manager, pending, source := claimSplitReuse(t)
	manager.RecordReuseSplitGrant(source, []*pdpb.SplitID{{NewRegionId: 42}})
	result := applyReuseTestSplit(cluster, 42)
	// A child heartbeat can arrive before the asynchronous applied split report.
	require.NoError(t, manager.tryCompleteReusedCreate(pending))
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_DISABLED, current.State)
	manager.RecordReuseSplitResult(result)
	current, err = manager.waitReusedCreate(pending)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, current.State)
	require.False(t, current.PendingInitialization)
	require.Empty(t, manager.reuse.grants)
}

func TestSplitReportBeforeHeartbeatCompletesAfterRegionAppears(t *testing.T) {
	cluster, manager, pending, source := claimSplitReuse(t)
	manager.RecordReuseSplitGrant(source, []*pdpb.SplitID{{NewRegionId: 42}})
	result := []*metapb.Region{newReuseTestRegion(42, 42).Clone(core.WithIncVersion()).GetMeta(), newReuseTestRegion(43, 420).Clone(core.WithIncVersion()).GetMeta()}
	manager.RecordReuseSplitResult(result)
	require.NoError(t, manager.tryCompleteReusedCreate(pending))
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_DISABLED, current.State)
	applyReuseTestSplit(cluster, 42)
	current, err = manager.waitReusedCreate(pending)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ENABLED, current.State)
}

func TestReusedCreateTimeoutArchivesAndKeepsIDClaimed(t *testing.T) {
	cluster, manager, pending, source := claimSplitReuse(t)
	manager.RecordReuseSplitGrant(source, []*pdpb.SplitID{{NewRegionId: 42}})
	applyReuseTestSplit(cluster, 42)
	// A lost ReportBatchSplit must not be replaced by observing exact boundaries.
	_, err := manager.waitReusedCreate(pending)
	require.ErrorContains(t, err, "timed out")
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ARCHIVED, current.State)
	require.False(t, current.PendingInitialization)
	require.NotNil(t, cluster.GetRegionLabeler().GetLabelRule(buildLabelRule(42, txnRegionBound).ID))
	require.NoError(t, manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		bits, err := txn.Load(bitmapPath(0))
		require.Empty(t, bits)
		return err
	}))
}

func TestOldSplitCannotCompleteNewClaim(t *testing.T) {
	cluster, manager, pending, source := claimSplitReuse(t)
	manager.RecordReuseSplitGrant(source, []*pdpb.SplitID{{NewRegionId: 44}})
	// A different split allocation won the Raft race.
	manager.RecordReuseSplitResult(applyReuseTestSplit(cluster, 42))
	_, err := manager.waitReusedCreate(pending)
	require.ErrorContains(t, err, "no longer requires")
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ARCHIVED, current.State)
}

func TestPendingSplitCannotBypassInitializationAndRestartCancels(t *testing.T) {
	_, manager, pending, _ := claimSplitReuse(t)
	_, err := manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_ENABLED, 2)
	require.Error(t, err)
	mutations := []*Mutation{{Op: OpPut, Key: "encryption", Value: `{"enabled":true}`}}
	_, err = manager.UpdateKeyspaceConfig(pending.Name, mutations)
	require.Error(t, err)
	_, err = manager.UpdateKeyspaceConfigWithPreconditions(pending.Name, mutations, map[string]*string{"encryption": nil})
	require.Error(t, err)
	manager.reuse = newReuseState()
	require.NoError(t, manager.StartIDReuse())
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ARCHIVED, current.State)
	require.False(t, current.PendingInitialization)
	require.Error(t, manager.ensureReuseRegionRule(pending))
	require.Error(t, manager.completeReusedCreate(pending))
}

func TestArchiveRejectsLateAppliedSplit(t *testing.T) {
	cluster, manager, pending, source := claimSplitReuse(t)
	manager.RecordReuseSplitGrant(source, []*pdpb.SplitID{{NewRegionId: 42}})
	_, err := manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_ARCHIVED, 2)
	require.NoError(t, err)
	manager.RecordReuseSplitResult(applyReuseTestSplit(cluster, 42))
	require.Error(t, manager.completeReusedCreate(pending))
	current, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	require.Equal(t, keyspacepb.KeyspaceState_ARCHIVED, current.State)
	require.Empty(t, manager.reuse.grants)
}
