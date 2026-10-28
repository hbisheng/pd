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
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestConcurrentSameNameClaimsHaveOneOwner(t *testing.T) {
	_, _, cluster, manager := newRecordedReuseManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(44).TxnRightBound)))
	seedReuseTestBit(t, manager, 42, true)
	seedReuseTestBit(t, manager, 43, true)
	results := make(chan error, 2)
	for range 2 {
		go func() {
			meta, err := manager.tryCreateReusedKeyspace("same-name", nil, 1)
			if err == nil && meta == nil {
				// A racing snapshot may defer reuse. The public Create then uses
				// this ordinary save path, whose name check must still reject it.
				err = manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{Name: "same-name"})
			}
			results <- err
		}()
	}
	first, second := <-results, <-results
	if first == nil {
		require.ErrorIs(t, second, errs.ErrKeyspaceExists)
	} else {
		require.ErrorIs(t, first, errs.ErrKeyspaceExists)
		require.NoError(t, second)
	}
	meta, err := manager.LoadKeyspace("same-name")
	require.NoError(t, err)
	require.Equal(t, uint32(42), meta.GetId())
	warmReuseTestCoverage(t, manager, 44) // Duplicate-name rejection conservatively invalidates the candidate bucket.
	require.True(t, manager.reuse.buckets[0].Value[43/8]&(1<<(43%8)) != 0)
}

func TestMemoryWaitObservesCommittedConfigAndArchive(t *testing.T) {
	for _, change := range []string{"config", "archive"} {
		t.Run(change, func(t *testing.T) {
			recorder, _, cluster, manager := newRecordedReuseManager(t)
			cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
			seedReuseTestBit(t, manager, 42, true)
			meta, err := manager.tryCreateReusedKeyspace("changed", nil, 1)
			require.NoError(t, err)
			if change == "config" {
				_, err = manager.UpdateKeyspaceConfig(meta.Name, []*Mutation{{Op: OpPut, Key: "tenant-config", Value: "new"}})
			} else {
				_, err = manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_ARCHIVED, 2)
			}
			require.NoError(t, err)
			recorder.resetCounts()
			_, err = manager.waitReusedCreate(meta)
			require.Error(t, err)
			require.Len(t, recorder.reads, 1, "only final cancellation validation may read etcd")
			require.Empty(t, manager.reuse.progress)
			require.Error(t, manager.completeReusedCreate(meta))
		})
	}
}

func TestFreeOrderingDefersUnknownBitmapWithoutReadingMetadata(t *testing.T) {
	recorder, _, cluster, manager := newRecordedReuseManager(t)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(299).TxnRightBound)))
	seedReuseTestBit(t, manager, 298, true)
	delete(manager.reuse.buckets, 0)
	recorder.resetCounts()
	meta, err := manager.tryCreateReusedKeyspace("unknown-bitmap", nil, 1)
	require.NoError(t, err)
	require.Nil(t, meta)
	require.Empty(t, recorder.reads)
	require.NoError(t, manager.readFreeBucketIntervalLocked(manager.ctx, 0, 1))
	meta, err = manager.tryCreateReusedKeyspace("known-bitmap", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(298), meta.GetId())
}

func TestReuseRejectsOldLeaderWithoutWritingSharedRevision(t *testing.T) {
	recorder, client, cluster, manager := newRecordedReuseManager(t)
	const leaderKey = "/test/keyspace-reuse-leader"
	_, err := client.Put(manager.ctx, leaderKey, "old")
	require.NoError(t, err)
	manager.SetReuseLeader(leaderKey, "old")
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	seedReuseTestBit(t, manager, 42, true)
	recorder.beforeCommit = func() error {
		_, err := client.Put(manager.ctx, leaderKey, "new")
		return err
	}
	created, err := manager.tryCreateReusedKeyspace("old-leader", nil, 1)
	require.NoError(t, err)
	require.Nil(t, created)
	_, err = manager.LoadKeyspaceByID(42)
	require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
	recorder.beforeCommit = nil
	manager.SetReuseLeader(leaderKey, "new")
	warmReuseTestCoverage(t, manager, 43)
	recorder.resetCounts()
	created, err = manager.tryCreateReusedKeyspace("new-leader", nil, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(42), created.GetId())
	for _, read := range recorder.reads {
		require.NotEqual(t, leaderKey, string(read.KeyBytes()), "leader check is a transaction condition, not another Get")
	}
}

func TestOrdinaryMetadataWritesDoNotUseReuseRevision(t *testing.T) {
	recorder, client, cluster, manager := newRecordedReuseManager(t)
	revisionKey := keypath.KeyspaceFreePrefix() + "revision"
	_, err := client.Put(manager.ctx, revisionKey, "legacy-unused")
	require.NoError(t, err)
	before, err := client.Get(manager.ctx, revisionKey)
	require.NoError(t, err)
	saveReuseTestMeta(t, manager, 42, keyspacepb.KeyspaceState_DISABLED)
	owner, err := manager.LoadKeyspaceByID(42)
	require.NoError(t, err)
	recorder.resetCounts()
	_, err = manager.UpdateKeyspaceConfig(owner.Name, []*Mutation{{Op: OpPut, Key: "sample", Value: "value"}})
	require.NoError(t, err)
	_, err = manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_ARCHIVED, 2)
	require.NoError(t, err)
	_, err = manager.UpdateKeyspaceStateByID(42, keyspacepb.KeyspaceState_TOMBSTONE, 3)
	require.NoError(t, err)
	saveReuseTestMeta(t, manager, 43, keyspacepb.KeyspaceState_TOMBSTONE)
	cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(43).TxnRightBound)))
	require.NoError(t, manager.RequestKeyspaceReclaim(42))
	_, err = manager.tryCreateReusedKeyspace("no-revision", nil, 4)
	require.NoError(t, err)
	for _, read := range recorder.reads {
		require.NotEqual(t, revisionKey, string(read.KeyBytes()))
	}
	for _, cmp := range recorder.comparisons {
		require.NotEqual(t, revisionKey, string(cmp.Key))
	}
	for _, op := range recorder.writes {
		require.NotEqual(t, revisionKey, string(op.KeyBytes()))
	}
	after, err := client.Get(manager.ctx, revisionKey)
	require.NoError(t, err)
	require.Equal(t, before.Kvs[0].ModRevision, after.Kvs[0].ModRevision)
}
