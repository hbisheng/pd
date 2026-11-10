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
	"bytes"
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/kv"
)

var errReclaimDeferred = errors.New("keyspace is not eligible for reclamation")

func reusableKeyspaceID(id uint32) bool {
	return id != constant.DefaultKeyspaceID && uint64(id) < constant.ReservedKeyspaceIDStart
}

func (manager *Manager) transactionalReuseConfig(config map[string]string) bool {
	if bound := config[RegionBoundType]; bound != "" {
		return bound == txnRegionBound.String()
	}
	return manager.getRegionBoundType() == txnRegionBound
}

func regionCoversKeyspace(region *metapb.Region, bound *RegionBound) bool {
	return bytes.Compare(region.GetStartKey(), bound.TxnLeftBound) <= 0 &&
		(len(region.GetEndKey()) == 0 || bytes.Compare(region.GetEndKey(), bound.TxnRightBound) >= 0)
}

// coveringKeyspaceRegion requires one known Region covering the full keyspace.
func (manager *Manager) coveringKeyspaceRegion(id uint32) *core.RegionInfo {
	if manager.cluster == nil {
		return nil
	}
	bound := MakeRegionBound(id)
	regions := manager.cluster.GetBasicCluster().ScanRegions(bound.TxnLeftBound, bound.TxnRightBound, 2)
	if len(regions) != 1 || !regionCoversKeyspace(regions[0].GetMeta(), bound) {
		return nil
	}
	return regions[0]
}

// reclaimKeyspace is an internal capability, with no API or lifecycle wiring.
// The caller admits and rate-limits requests and supplies a leader-fenced bitmap.
// Keep the last ID in each merged range: an eligible ID must share its Region
// with the complete next keyspace. A later Create rechecks this before claiming.
func (manager *Manager) reclaimKeyspace(ctx context.Context, bitmap *freeIDBitmap, id uint32) (bool, error) {
	if !reusableKeyspaceID(id) {
		return false, newModifyProtectedKeyspaceError()
	}
	if manager.mgm != nil {
		manager.mgm.RLock()
		defer manager.mgm.RUnlock()
	}
	manager.metaLock.Lock(id)
	reclaimed, err := manager.reclaimKeyspaceLocked(ctx, bitmap, id)
	if reclaimed {
		manager.evictKeyspacesFromCache([]uint32{id})
	}
	manager.metaLock.Unlock(id)
	if reclaimed {
		manager.invalidateGCBarrierMetrics(id)
	}
	return reclaimed, err
}

func (manager *Manager) reclaimKeyspaceLocked(ctx context.Context, bitmap *freeIDBitmap, id uint32) (bool, error) {
	meta, _, err := manager.store.LoadKeyspaceMetaWithRevision(ctx, id)
	if err != nil || meta == nil {
		return false, err
	}
	if meta.State != keyspacepb.KeyspaceState_TOMBSTONE || !manager.transactionalReuseConfig(meta.Config) {
		return false, errors.New("reclamation requires a tombstoned transactional keyspace")
	}
	region := manager.coveringKeyspaceRegion(id)
	if region == nil || !reusableKeyspaceID(id+1) || !regionCoversKeyspace(region.GetMeta(), MakeRegionBound(id+1)) {
		return false, nil
	}
	err = bitmap.update(ctx, id, true, func(txn kv.Txn) error {
		current, err := manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if current == nil || current.State != keyspacepb.KeyspaceState_TOMBSTONE || current.Name != meta.Name ||
			current.CreatedAt != meta.CreatedAt || !manager.transactionalReuseConfig(current.Config) {
			return errs.ErrEtcdTxnConflict.FastGenByArgs()
		}
		// The neighbor may already be free. Both cases are checked within this
		// transaction, so concurrent allocation cannot turn an active neighbor free.
		next, err := manager.store.LoadKeyspaceMeta(txn, id+1)
		if err != nil {
			return err
		}
		if next != nil {
			if next.State != keyspacepb.KeyspaceState_TOMBSTONE || !manager.transactionalReuseConfig(next.Config) {
				return errReclaimDeferred
			}
		} else {
			width := uint32(bitmap.bucketBytes * 8)
			bits, err := txn.Load(bitmap.bucketKey((id + 1) / width))
			if err != nil {
				return err
			}
			if len(bits) != bitmap.bucketBytes || bits[((id+1)%width)/8]&(1<<((id+1)%8)) == 0 {
				return errReclaimDeferred
			}
		}
		exists, mappedID, err := manager.store.LoadKeyspaceID(txn, current.Name)
		if err != nil {
			return err
		}
		if !exists || mappedID != id {
			return errors.New("keyspace name mapping changed before reclamation")
		}
		if err := manager.unassignKeyspaceFromMetaServiceGroup(txn, current); err != nil {
			return err
		}
		return manager.store.RemoveKeyspace(txn, id, current.Name)
	})
	if errors.ErrorEqual(err, errReclaimDeferred) {
		return false, nil
	}
	if err != nil && !errors.ErrorEqual(err, errs.ErrEtcdTxnConflict) {
		current, _, readErr := manager.store.LoadKeyspaceMetaWithRevision(ctx, id)
		if readErr == nil && current == nil {
			bitmap.mu.Lock()
			bucket := id / uint32(bitmap.bucketBytes*8)
			readErr = bitmap.loadRangeLocked(ctx, bucket, bucket+1)
			bits := bitmap.buckets[bucket].Value
			free := readErr == nil && len(bits) == bitmap.bucketBytes && bits[(id%uint32(bitmap.bucketBytes*8))/8]&(1<<(id%8)) != 0
			bitmap.mu.Unlock()
			if free {
				return true, nil
			}
		}
	}
	// A failed commit remains retryable. The bitmap invalidates its cached bucket;
	// the next request rereads metadata and cannot repeat a committed deletion.
	return err == nil, err
}
