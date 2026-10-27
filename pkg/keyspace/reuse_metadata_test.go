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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestMemoryOrderingDefersMissingLowerCoverageWithoutReads(t *testing.T) {
	for _, missing := range []string{"bitmap", "metadata"} {
		t.Run(missing, func(t *testing.T) {
			recorder, _, cluster, manager := newRecordedReuseManager(t)
			cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(299).TxnRightBound)))
			seedReuseTestBit(t, manager, 298, true)
			if missing == "bitmap" {
				delete(manager.reuse.buckets, 0)
			} else {
				delete(manager.reuse.metadataPages, 0)
			}
			recorder.resetCounts()
			meta, err := manager.tryCreateReusedKeyspace("unknown-lower", nil, 1)
			require.NoError(t, err)
			require.Nil(t, meta)
			require.Empty(t, recorder.reads, "cold proof must not discover ordering synchronously")
			require.Zero(t, recorder.transactions)
			// The ordinary allocator remains available while reuse is deferred.
			meta, err = manager.CreateKeyspace(&CreateKeyspaceRequest{Name: "ordinary", CreateTime: 1})
			require.NoError(t, err)
			require.Equal(t, uint32(101), meta.GetId())
		})
	}
}

func TestMemoryOrderingLoaderProgressIsBoundedAndFair(t *testing.T) {
	recorder, _, cluster, manager := newRecordedReuseManager(t)
	for id := uint32(1); id <= 130; id++ {
		require.NoError(t, manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: fmt.Sprintf("live-%d", id), State: keyspacepb.KeyspaceState_ENABLED,
		}))
	}
	cluster.PutRegion(newReuseTestRegion(1, 420).Clone(core.WithEndKey(MakeRegionBound(191).TxnRightBound)))
	cluster.PutRegion(newReuseTestRegion(1000, 1000).Clone(core.WithEndKey(MakeRegionBound(1001).TxnRightBound)))
	seedReuseTestBit(t, manager, 190, true)
	seedReuseTestBit(t, manager, 1000, true)
	manager.reuse.metadataPages = make(map[uint32]reuseMetadataPage)
	meta, err := manager.tryCreateReusedKeyspace("cold", nil, 1)
	require.NoError(t, err)
	require.Nil(t, meta)
	recorder.resetCounts()
	// Numeric pages advance without more Creates. The unrelated interval is
	// serviced before the three-page merged range finishes loading.
	for range 4 {
		require.NoError(t, manager.loadNextFreeBucketPage(manager.ctx))
	}
	require.Contains(t, manager.reuse.metadataPages, uint32(0))
	require.Contains(t, manager.reuse.metadataPages, uint32(15))
	require.NotContains(t, manager.reuse.metadataPages, uint32(2))
	meta, err = manager.tryCreateReusedKeyspace("unrelated", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(1000), meta.GetId())
	for range 4 {
		require.NoError(t, manager.loadNextFreeBucketPage(manager.ctx))
	}
	meta, err = manager.tryCreateReusedKeyspace("large-range", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(190), meta.GetId())
	metadataReads := 0
	for _, read := range recorder.reads {
		if strings.HasPrefix(string(read.KeyBytes()), keypath.KeyspaceMetaPrefix()) && len(read.RangeBytes()) != 0 {
			start, err := strconv.ParseUint(strings.TrimPrefix(string(read.KeyBytes()), keypath.KeyspaceMetaPrefix()), 10, 32)
			require.NoError(t, err)
			require.Equal(t, keypath.KeyspaceMetaPath(uint32(start)+reuseMetadataPageIDs), string(read.RangeBytes()))
			metadataReads++
		}
	}
	require.Equal(t, 4, metadataReads)
}

func TestMemoryOrderingMergeRequiresNewCoverage(t *testing.T) {
	_, cluster, manager := newReuseTestManager(t)
	seedReuseTestBit(t, manager, 190, true)
	delete(manager.reuse.metadataPages, 0)
	delete(manager.reuse.metadataPages, 1)
	region := newReuseTestRegion(128, 420).Clone(core.WithEndKey(MakeRegionBound(191).TxnRightBound))
	cluster.PutRegion(region)
	_, ready := manager.memoryOrderingProof(region, 190, false)
	require.True(t, ready)
	region = region.Clone(core.WithStartKey(MakeRegionBound(1).TxnLeftBound))
	cluster.PutRegion(region)
	meta, err := manager.tryCreateReusedKeyspace("after-merge", nil, 1)
	require.NoError(t, err)
	require.Nil(t, meta)
	for range 2 {
		require.NoError(t, manager.loadReuseMetadataPageLocked(manager.ctx))
	}
	meta, err = manager.tryCreateReusedKeyspace("after-merge", nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(190), meta.GetId())
}

func TestMemoryOrderingFenceIncludesOtherManagerCreates(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit-%t", explicit), func(t *testing.T) {
			_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
			t.Cleanup(clean)
			backend := &orderedReuseKV{Base: kv.NewEtcdKVBase(client)}
			_, cluster, manager := newReuseTestManagerWithKV(t, backend)
			_, _, writer := newReuseTestManagerWithKV(t, backend.Base)
			require.NoError(t, writer.idAllocator.SetBase(42))
			saveReuseTestMeta(t, manager, 42, keyspacepb.KeyspaceState_TOMBSTONE)
			cluster.PutRegion(newReuseTestRegion(42, 420).Clone(core.WithEndKey(MakeRegionBound(81).TxnRightBound)))
			seedReuseTestBit(t, manager, 80, true)
			injected := false
			backend.afterLoad = func(key string) {
				if key != keypath.KeyspaceMetaPath(80) || injected {
					return
				}
				injected = true
				var meta *keyspacepb.KeyspaceMeta
				var err error
				if explicit {
					id := uint32(43)
					meta, err = writer.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &id, Name: "fills-hole", CreateTime: 1})
				} else {
					meta, err = writer.CreateKeyspace(&CreateKeyspaceRequest{Name: "fills-hole", CreateTime: 1})
				}
				require.NoError(t, err)
				require.Equal(t, uint32(43), meta.GetId())
			}
			meta, err := manager.tryCreateReusedKeyspace("after-hole", nil, 1)
			require.NoError(t, err)
			require.True(t, injected)
			require.Nil(t, meta, "a different manager's write must invalidate the cached proof")
			_, err = manager.LoadKeyspaceByID(80)
			require.ErrorIs(t, err, errs.ErrKeyspaceNotFound)
		})
	}
}

func TestMemoryOrderingCanceledMetadataReadDoesNotPublish(t *testing.T) {
	backend := &orderedReuseKV{Base: kv.NewMemoryKV()}
	_, _, manager := newReuseTestManagerWithKV(t, backend)
	manager.metadataCoverageLocked(0, 130)
	ctx, cancel := context.WithCancel(manager.ctx)
	defer cancel()
	backend.afterRange = func(string) { cancel() }
	require.Error(t, manager.loadReuseMetadataPageLocked(ctx))
	require.Empty(t, manager.reuse.metadataPages)
}

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
	require.True(t, manager.freeBitLocked(43))
}

func TestMemoryMetadataFencePatrolRespectsEtcdTxnLimit(t *testing.T) {
	_, _, _, manager := newRecordedReuseManager(t)
	for id := uint32(1); id <= 130; id++ {
		require.NoError(t, manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: fmt.Sprintf("patrol-%d", id), State: keyspacepb.KeyspaceState_ENABLED,
		}))
	}
	require.NoError(t, manager.PatrolKeyspaceAssignment(0, 0))
	for _, id := range []uint32{1, 126, 127, 130} {
		meta, err := manager.LoadKeyspaceByID(id)
		require.NoError(t, err)
		require.Equal(t, "0", meta.Config[TSOKeyspaceGroupIDKey])
	}
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
