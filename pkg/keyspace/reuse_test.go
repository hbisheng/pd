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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

func newReuseTestRegion(id uint32, version uint64) *core.RegionInfo {
	bound := MakeRegionBound(id)
	peers := []*metapb.Peer{
		{Id: 101, StoreId: 1},
		{Id: 102, StoreId: 2},
		{Id: 103, StoreId: 3},
	}
	meta := &metapb.Region{
		Id:          10,
		StartKey:    bound.TxnLeftBound,
		EndKey:      bound.TxnRightBound,
		RegionEpoch: &metapb.RegionEpoch{Version: version, ConfVer: 3},
		Peers:       peers,
	}
	return core.NewRegionInfo(meta, peers[0])
}

func reportResetStatus(manager *Manager, region *core.RegionInfo, keyspaceID uint32, generation, restoreVersion, fileID uint64, keyReady bool, stores ...uint64) {
	for _, storeID := range stores {
		var peerID uint64
		for _, peer := range region.GetPeers() {
			if peer.GetStoreId() == storeID {
				peerID = peer.GetId()
				break
			}
		}
		manager.RecordKeyspaceResetStatuses(storeID, []*pdpb.KeyspaceResetStatus{{
			RegionId:             region.GetID(),
			PeerId:               peerID,
			KeyspaceId:           keyspaceID,
			Generation:           generation,
			RestoreVersion:       restoreVersion,
			EncryptionMetaFileId: fileID,
			KeyReady:             keyReady,
			RegionVersion:        region.GetRegionEpoch().GetVersion(),
			ConfVer:              region.GetRegionEpoch().GetConfVer(),
		}})
	}
}

func TestReclaimAndReusedCreateHeartbeatProtocol(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	for storeID := uint64(1); storeID <= 3; storeID++ {
		cluster.AddRegionStore(storeID, 1)
	}
	const keyspaceID = uint32(42)
	region := newReuseTestRegion(keyspaceID, 5)
	cluster.PutRegion(region)
	bound := MakeRegionBound(keyspaceID)
	nextPeer := &metapb.Peer{Id: 201, StoreId: 1}
	cluster.PutRegion(core.NewRegionInfo(&metapb.Region{
		Id:          11,
		StartKey:    bound.TxnRightBound,
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		Peers:       []*metapb.Peer{nextPeer},
	}, nextPeer))
	kgm := NewKeyspaceGroupManager(ctx, storage, nil)
	re.NoError(kgm.Bootstrap(ctx))
	manager := NewKeyspaceManager(ctx, storage, cluster, mockid.NewIDAllocator(), &mockConfig{
		WaitRegionSplit:          true,
		WaitRegionSplitTimeout:   typeutil.NewDuration(3 * time.Second),
		CheckRegionSplitInterval: typeutil.NewDuration(10 * time.Millisecond),
	}, kgm, nil)
	manager.now = func() time.Time { return time.Unix(4_000_000, 0) }

	meta := &keyspacepb.KeyspaceMeta{
		Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: keyspaceID},
		Name:           "retired-tenant",
		State:          keyspacepb.KeyspaceState_TOMBSTONE,
		CreatedAt:      1,
		StateChangedAt: manager.now().Add(-MinimumKeyspaceReuseTombstoneDuration - time.Hour).Unix(),
		Config:         map[string]string{},
	}
	re.NoError(manager.saveNewKeyspace(meta))
	re.NoError(cluster.GetRegionLabeler().SetLabelRule(buildLabelRule(keyspaceID, txnRegionBound)))

	// Reclamation cannot start from PD metadata alone: all current peers first
	// prove the same applied generation and restore-version floor.
	re.Error(storage.RunInTxn(ctx, func(txn kv.Txn) error { return manager.RemoveKeyspace(txn, keyspaceID) }))
	reportResetStatus(manager, region, keyspaceID, 0, 7, 0, true, 1, 2, 3)
	re.NoError(storage.RunInTxn(ctx, func(txn kv.Txn) error { return manager.RemoveKeyspace(txn, keyspaceID) }))
	reclaimCommand := manager.KeyspaceResetForRegion(region)
	re.NotNil(reclaimCommand)
	re.Equal(pdpb.KeyspaceResetPurpose_RECLAIM, reclaimCommand.GetPurpose())
	re.Equal(uint64(0), reclaimCommand.GetExpectedGeneration())
	re.Equal(uint64(1), reclaimCommand.GetGeneration())
	re.Equal(uint64(7), reclaimCommand.GetExpectedRestoreVersion())
	re.Equal(uint64(8), reclaimCommand.GetRestoreVersion())

	manager.resetMu.RLock()
	reclaimIntent := *manager.resets[keyspaceID]
	manager.resetMu.RUnlock()

	// Durable intents are reloaded after a PD process restart; applied reports
	// are intentionally recollected from the current peer set.
	manager = NewKeyspaceManager(ctx, storage, cluster, mockid.NewIDAllocator(), &mockConfig{
		WaitRegionSplit:          true,
		WaitRegionSplitTimeout:   typeutil.NewDuration(3 * time.Second),
		CheckRegionSplitInterval: typeutil.NewDuration(10 * time.Millisecond),
	}, kgm, nil)
	manager.now = func() time.Time { return time.Unix(4_000_000, 0) }
	re.NoError(manager.loadResetIntents())
	re.Equal(reclaimCommand, manager.KeyspaceResetForRegion(region))

	// Old-epoch and partial reports cannot publish the ID as free.
	reportResetStatus(manager, region, keyspaceID, 1, 8, 0, true, 1, 2, 3)
	_, err := manager.LoadKeyspaceByID(keyspaceID)
	re.NoError(err)
	region = newReuseTestRegion(keyspaceID, 6)
	cluster.PutRegion(region)
	reportResetStatus(manager, region, keyspaceID, 1, 8, 0, true, 1, 2)
	_, err = manager.LoadKeyspaceByID(keyspaceID)
	re.NoError(err)
	reportResetStatus(manager, region, keyspaceID, 1, 8, 0, true, 3)
	_, err = manager.LoadKeyspaceByID(keyspaceID)
	re.Error(err)

	var freeValue string
	re.NoError(storage.RunInTxn(ctx, func(txn kv.Txn) error {
		freeValue, err = txn.Load(keypath.KeyspaceFreePath(keyspaceID))
		return err
	}))
	generation, restoreFloor, err := decodeFreeID(freeValue)
	re.NoError(err)
	re.Equal(uint64(1), generation)
	re.Equal(uint64(8), restoreFloor)

	pendingCtx, cancelPending := context.WithCancel(ctx)
	manager = NewKeyspaceManager(pendingCtx, storage, cluster, mockid.NewIDAllocator(), &mockConfig{
		WaitRegionSplit:          true,
		WaitRegionSplitTimeout:   typeutil.NewDuration(3 * time.Second),
		CheckRegionSplitInterval: typeutil.NewDuration(10 * time.Millisecond),
	}, kgm, nil)
	manager.now = func() time.Time { return time.Unix(4_000_000, 0) }
	createResult := make(chan struct {
		meta *keyspacepb.KeyspaceMeta
		err  error
	}, 1)
	go func() {
		created, createErr := manager.CreateKeyspace(&CreateKeyspaceRequest{
			Name: "new-tenant",
			Config: map[string]string{
				"encryption": "{\"enabled\":true,\"cmek_id\":\"test-key\",\"vendor\":\"test\"}",
			},
			CreateTime: manager.now().Unix(),
		})
		createResult <- struct {
			meta *keyspacepb.KeyspaceMeta
			err  error
		}{created, createErr}
	}()

	re.Eventually(func() bool {
		command := manager.KeyspaceResetForRegion(region)
		return command != nil && command.GetPurpose() == pdpb.KeyspaceResetPurpose_CREATE
	}, time.Second, 10*time.Millisecond)
	createCommand := manager.KeyspaceResetForRegion(region)
	re.Equal(uint64(1), createCommand.GetExpectedGeneration())
	re.Equal(uint64(1), createCommand.GetGeneration())
	re.Equal(uint64(8), createCommand.GetExpectedRestoreVersion())
	re.Equal(uint64(9), createCommand.GetRestoreVersion())
	re.Zero(createCommand.GetEncryptionMetaFileId())

	// Simulate PD stopping while Create is pending. The caller sees an error, but
	// the disabled metadata and intent remain durable for an identical retry.
	cancelPending()
	result := <-createResult
	re.Error(result.err)
	manager = NewKeyspaceManager(ctx, storage, cluster, mockid.NewIDAllocator(), &mockConfig{
		WaitRegionSplit:          true,
		WaitRegionSplitTimeout:   typeutil.NewDuration(3 * time.Second),
		CheckRegionSplitInterval: typeutil.NewDuration(10 * time.Millisecond),
	}, kgm, nil)
	manager.now = func() time.Time { return time.Unix(4_000_000, 0) }
	re.NoError(manager.loadResetIntents())
	re.Equal(createCommand, manager.KeyspaceResetForRegion(region))

	// An identical same-name retry joins the reloaded operation instead of
	// allocating another ID or returning an already-exists error.
	retryResult := make(chan struct {
		meta *keyspacepb.KeyspaceMeta
		err  error
	}, 1)
	go func() {
		created, createErr := manager.CreateKeyspace(&CreateKeyspaceRequest{
			Name: "new-tenant",
			Config: map[string]string{
				"encryption": "{\"enabled\":true,\"cmek_id\":\"test-key\",\"vendor\":\"test\"}",
			},
			CreateTime: manager.now().Unix(),
		})
		retryResult <- struct {
			meta *keyspacepb.KeyspaceMeta
			err  error
		}{created, createErr}
	}()

	// Generic mutation APIs cannot bypass the immutable pending create.
	_, err = manager.UpdateKeyspaceStateByID(keyspaceID, keyspacepb.KeyspaceState_ENABLED, manager.now().Unix())
	re.Error(err)
	_, err = manager.UpdateKeyspaceConfig("new-tenant", []*Mutation{{Op: OpPut, Key: "x", Value: "y"}})
	re.Error(err)

	selected, err := manager.SelectKeyspaceEncryptionMetaFile(keyspaceID, 1, 9, 9001)
	re.NoError(err)
	re.Equal(uint64(9001), selected)
	selected, err = manager.SelectKeyspaceEncryptionMetaFile(keyspaceID, 1, 9, 9002)
	re.NoError(err)
	re.Equal(uint64(9001), selected)
	re.Equal(uint64(9001), manager.KeyspaceResetForRegion(region).GetEncryptionMetaFileId())

	// A wrong encryption file and a partial topology are not readiness.
	region = newReuseTestRegion(keyspaceID, 7)
	cluster.PutRegion(region)
	reportResetStatus(manager, region, keyspaceID, 1, 9, 9002, true, 1, 2, 3)
	select {
	case result := <-retryResult:
		t.Fatalf("create retry completed from a wrong-file report: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	reportResetStatus(manager, region, keyspaceID, 1, 9, 9001, true, 1, 2)
	select {
	case result := <-retryResult:
		t.Fatalf("create retry completed without every peer: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	reportResetStatus(manager, region, keyspaceID, 1, 9, 9001, true, 3)
	retry := <-retryResult
	re.NoError(retry.err)
	re.Equal(keyspaceID, retry.meta.GetId())
	re.Equal(keyspacepb.KeyspaceState_ENABLED, retry.meta.GetState())
	re.Equal("1", retry.meta.GetConfig()[KeyspaceGenerationKey])
	re.Equal("9", retry.meta.GetConfig()[KeyspaceRestoreVersionKey])

	// A delayed old reclamation completion cannot recreate free/<id> after the
	// new identity has claimed and initialized it.
	re.Error(manager.finishReclaim(&reclaimIntent))
	re.NoError(storage.RunInTxn(ctx, func(txn kv.Txn) error {
		freeValue, err = txn.Load(keypath.KeyspaceFreePath(keyspaceID))
		return err
	}))
	re.Empty(freeValue)
	loaded, err := manager.LoadKeyspace("new-tenant")
	re.NoError(err)
	re.Equal(keyspaceID, loaded.GetId())
}
