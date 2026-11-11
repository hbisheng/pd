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

// Reclamation makes a deleted keyspace ID available to a future Create. Data
// Cleaner first deletes the tenant's KV data and marks the keyspace TOMBSTONE;
// it can then request reclamation as the empty Regions merge. This file handles
// PD metadata cleanup and the free-ID bitmap, not KV data deletion.
//
// An ID is eligible only when one Region contains both its entire keyspace and
// the entire next keyspace (ID + 1), and that neighbor is also deleted or free.
// For example, after adjacent deleted A and B merge, A can be reclaimed while B
// stays unreclaimed. An isolated keyspace is deferred. Keeping the last ID out of the
// free pool lets reuse follow the existing Create/split initialization path.
//
// Reclamation atomically removes the old metadata, name mapping and meta-service
// group assignment and sets the free bit. It does not create a new owner or
// initialize Regions; the later Create claims the ID and rechecks boundaries.

var errReclaimDeferred = errors.New("keyspace is not eligible for reclamation")

// reusableKeyspaceID excludes the default keyspace and reserved ID range.
func reusableKeyspaceID(id uint32) bool {
	return id != constant.DefaultKeyspaceID && uint64(id) < constant.ReservedKeyspaceIDStart
}

// transactionalReuseConfig falls back to the cluster setting for older metadata
// that has no explicit region_bound_type. Raw keyspaces are excluded from reuse.
func (manager *Manager) transactionalReuseConfig(config map[string]string) bool {
	if bound := config[RegionBoundType]; bound != "" {
		return bound == txnRegionBound.String()
	}
	return manager.getRegionBoundType() == txnRegionBound
}

// regionCoversKeyspace accepts a Region larger than the keyspace after a merge.
func regionCoversKeyspace(region *metapb.Region, bound *RegionBound) bool {
	return bytes.Compare(region.GetStartKey(), bound.TxnLeftBound) <= 0 &&
		(len(region.GetEndKey()) == 0 || bytes.Compare(region.GetEndKey(), bound.TxnRightBound) >= 0)
}

// coveringKeyspaceRegion returns the Region containing the whole keyspace, or
// nil if the cached layout is missing, incomplete, or still split across Regions.
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

// reclaimKeyspace checks a tombstoned ID and records it as free after removing
// its old owner. The caller controls admission/rate limiting and supplies a
// bitmap whose transactions are fenced to the current PD leader.
//
// A true result confirms reclamation, including a reconciled commit whose reply
// was lost. False with no error means the metadata is already absent or the
// current Region/neighbor conditions defer reclamation. Invalid requests and
// storage failures return errors. Deferred or failed requests can be retried.
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

// reclaimKeyspaceLocked requires the caller to hold the ID's metadata lock and,
// when configured, the meta-service group manager's read lock. bitmap.update serializes the bitmap
// mutation and includes the metadata callback in its revision-checked transaction.
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
		// Missing neighbor metadata alone is insufficient: it may be an unused ID
		// or a failed creation. Require either TOMBSTONE metadata or a persisted
		// free bit. Transaction read comparisons reject a concurrent neighbor claim.
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
	// etcd may have committed before its reply was lost. Confirm both effects
	// from storage before reporting success: the old metadata is gone and the
	// ID is free. A transport error alone must not undo or repeat the deletion.
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
