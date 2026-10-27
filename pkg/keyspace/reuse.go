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
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcsconstant "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"golang.org/x/time/rate"
)

const freeIDScanLimit = 32

// A split grant uses IDs already allocated by AskBatchSplit. TiKV reads the
// keyspace configuration after receiving this grant, then reports applied children
// through ReportBatchSplit. This evidence is deliberately not persisted: a lost
// creation attempt is canceled rather than reconstructed from Region boundaries.
type reuseSplitGrant struct {
	source    *metapb.Region
	children  map[uint64]struct{}
	confirmed bool
}

type reuseState struct {
	// claimMu serializes cache reads, free-bit transactions, and publication of
	// committed claims into pending. It is never held while waiting for splits.
	claimMu        sync.Mutex
	lifecycleMu    sync.Mutex
	loaderCancel   context.CancelFunc
	loaderDone     chan struct{}
	buckets        map[uint32]kv.RevisionedValue
	freeBuckets    []uint32
	invalidBuckets map[uint32]struct{}
	loadNext       uint32
	layoutReady    bool
	reclaimLimiter *rate.Limiter
	loaderCtx      context.Context
	mu             sync.RWMutex
	pending        map[uint32]*keyspacepb.KeyspaceMeta
	grants         map[uint32]*reuseSplitGrant
	watermark      uint64
	scanStart      string
	now            func() time.Time
}

func newReuseState() *reuseState {
	return &reuseState{pending: make(map[uint32]*keyspacepb.KeyspaceMeta), grants: make(map[uint32]*reuseSplitGrant), buckets: make(map[uint32]kv.RevisionedValue), invalidBuckets: make(map[uint32]struct{}), now: time.Now}
}

func cloneKeyspaceConfig(config map[string]string) map[string]string {
	result := make(map[string]string, len(config))
	for k, v := range config {
		result[k] = v
	}
	return result
}

// isReuseInitializing recognizes only claims made by this manager. A normal
// DISABLED keyspace is not a pending reuse creation.
func (manager *Manager) isReuseInitializing(meta *keyspacepb.KeyspaceMeta) bool {
	if meta == nil || meta.State != keyspacepb.KeyspaceState_DISABLED {
		return false
	}
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	pending := manager.reuse.pending[meta.GetId()]
	return pending != nil && pending.Name == meta.Name && pending.CreatedAt == meta.CreatedAt
}

func reusableKeyspaceID(id uint32) bool {
	return id != constant.DefaultKeyspaceID && uint64(id) < constant.ReservedKeyspaceIDStart
}
func (manager *Manager) transactionalReuseConfig(config map[string]string) bool {
	bound := config[RegionBoundType]
	if bound == "" {
		return manager.getRegionBoundType() == txnRegionBound
	}
	return bound == txnRegionBound.String()
}

// Reuse is limited to plaintext and Starter legacy encryption. CMEK requests
// retain the existing increasing-ID and split initialization path.
func (manager *Manager) supportsReuseEncryption(config map[string]string) bool {
	if config["encryption"] == "" {
		return true
	}
	var value struct {
		Enabled bool   `json:"enabled"`
		CMEKID  string `json:"cmek_id"`
		Vendor  string `json:"vendor"`
	}
	if json.Unmarshal([]byte(config["encryption"]), &value) != nil {
		return false
	}
	return !value.Enabled || (manager.config.IsEncryptedIDReuseEnabled() && value.CMEKID == "" && value.Vendor == "")
}
func (manager *Manager) cacheReuseMeta(meta *keyspacepb.KeyspaceMeta) {
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	pending := manager.reuse.pending[meta.GetId()]
	if meta.State != keyspacepb.KeyspaceState_DISABLED || (pending != nil && (pending.Name != meta.Name || pending.CreatedAt != meta.CreatedAt || !maps.Equal(pending.Config, meta.Config))) {
		delete(manager.reuse.pending, meta.GetId())
		delete(manager.reuse.grants, meta.GetId())
	}
	manager.keyspaceStateLookup.Store(meta.GetId(), meta.State)
}

// StartIDReuse reloads the allocator watermark. Interrupted Creates remain
// DISABLED for the control plane to handle through the ordinary lifecycle.
func (manager *Manager) StartIDReuse() error {
	manager.reuse.lifecycleMu.Lock()
	defer manager.reuse.lifecycleMu.Unlock()
	manager.stopReuseLoader()
	// The allocator's in-memory window starts at zero after restart. Read its
	// existing durable counter once, rather than requiring a new allocation.
	var watermark uint64
	if err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		var err error
		value, err := txn.Load(keypath.KeyspaceAllocIDPath())
		if err != nil || value == "" {
			return err
		}
		watermark, err = typeutil.BytesToUint64([]byte(value))
		return err
	}); err != nil {
		return err
	}
	manager.reuse.claimMu.Lock()
	defer manager.reuse.claimMu.Unlock()
	manager.reuse.buckets = make(map[uint32]kv.RevisionedValue)
	manager.reuse.freeBuckets = nil
	manager.reuse.invalidBuckets = make(map[uint32]struct{})
	manager.reuse.loadNext = 0
	manager.reuse.layoutReady = false
	manager.reuse.reclaimLimiter = rate.NewLimiter(rate.Every(manager.config.GetIDReuseReclaimInterval()), 1)
	manager.reuse.mu.Lock()
	manager.reuse.watermark = watermark
	manager.reuse.scanStart = bitmapPrefix()
	manager.reuse.pending = make(map[uint32]*keyspacepb.KeyspaceMeta)
	manager.reuse.grants = make(map[uint32]*reuseSplitGrant)
	manager.reuse.mu.Unlock()
	if !manager.config.IsIDReuseEnabled() {
		return nil
	}
	if err := manager.ensureBitmapLayoutLocked(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(manager.ctx)
	manager.reuse.loaderCtx = ctx
	manager.reuse.loaderCancel = cancel
	manager.reuse.loaderDone = make(chan struct{})
	go manager.loadFreeBuckets(ctx, manager.reuse.loaderDone)
	return nil
}

// RecordReuseSplitGrant records only grants issued while this creation is pending.
// TiKV reads keyspace metadata after AskBatchSplit returns; older grants
// therefore cannot initialize a newly claimed ID.
func (manager *Manager) RecordReuseSplitGrant(source *metapb.Region, splits []*pdpb.SplitID) {
	if source == nil || len(splits) == 0 {
		return
	}
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	for id := range manager.reuse.pending {
		bound := MakeRegionBound(id)
		if !regionCoversKeyspace(source, bound) || regionMatchesKeyspace(source, bound) {
			continue
		}
		children := make(map[uint64]struct{}, len(splits)+1)
		children[source.GetId()] = struct{}{}
		for _, split := range splits {
			children[split.GetNewRegionId()] = struct{}{}
		}
		manager.reuse.grants[id] = &reuseSplitGrant{source: typeutil.DeepClone(source, core.RegionFactory), children: children}
	}
}

// RecordReuseSplitResult accepts the applied split reported through the existing
// RPC. Matching boundaries without these allocated child IDs is not completion.
func (manager *Manager) RecordReuseSplitResult(regions []*metapb.Region) {
	if len(regions) < 2 {
		return
	}
	manager.reuse.mu.Lock()
	defer manager.reuse.mu.Unlock()
	for id, grant := range manager.reuse.grants {
		if len(regions) != len(grant.children) || !bytes.Equal(regions[0].GetStartKey(), grant.source.GetStartKey()) || !bytes.Equal(regions[len(regions)-1].GetEndKey(), grant.source.GetEndKey()) {
			continue
		}
		valid, exact := true, false
		seen := make(map[uint64]struct{}, len(regions))
		for index, region := range regions {
			_, known := grant.children[region.GetId()]
			_, duplicate := seen[region.GetId()]
			if !known || duplicate || region.GetRegionEpoch().GetVersion() != grant.source.GetRegionEpoch().GetVersion()+uint64(len(regions)-1) || region.GetRegionEpoch().GetConfVer() != grant.source.GetRegionEpoch().GetConfVer() || (index > 0 && !bytes.Equal(regions[index-1].GetEndKey(), region.GetStartKey())) {
				valid = false
				break
			}
			seen[region.GetId()] = struct{}{}
			if regionMatchesKeyspace(region, MakeRegionBound(id)) {
				exact = true
			}
		}
		if valid && exact {
			grant.confirmed = true
		}
	}
}

func regionCoversKeyspace(region *metapb.Region, bound *RegionBound) bool {
	return bytes.Compare(region.GetStartKey(), bound.TxnLeftBound) <= 0 && (len(region.GetEndKey()) == 0 || bytes.Compare(region.GetEndKey(), bound.TxnRightBound) >= 0)
}

func regionMatchesKeyspace(region *metapb.Region, bound *RegionBound) bool {
	return bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) && bytes.Equal(region.GetEndKey(), bound.TxnRightBound)
}

func (manager *Manager) rejectIndependentScheduling() error {
	if cluster, ok := manager.cluster.(interface{ IsServiceIndependent(string) bool }); ok && cluster.IsServiceIndependent(mcsconstant.SchedulingServiceName) {
		return errors.New("keyspace ID reuse is unsupported with independent scheduling service")
	}
	return nil
}
func (manager *Manager) reuseAdmissionReady() error {
	if !manager.config.IsIDReuseEnabled() {
		return errors.New("keyspace ID reuse is disabled")
	}
	if err := manager.rejectIndependentScheduling(); err != nil {
		return err
	}
	allocator, ok := manager.idAllocator.(interface{ GetReservedWindowEnd() uint64 })
	if !ok {
		return errors.New("allocator does not expose its reuse watermark")
	}
	currentWindow := allocator.GetReservedWindowEnd()
	manager.reuse.mu.RLock()
	defer manager.reuse.mu.RUnlock()
	if max(currentWindow, manager.reuse.watermark) < manager.config.GetIDReuseThreshold() {
		return errors.New("allocator watermark is below reuse threshold")
	}
	return nil
}
func (manager *Manager) coveringKeyspaceRegion(id uint32) *core.RegionInfo {
	if manager.cluster == nil {
		return nil
	}
	bound := MakeRegionBound(id)
	regions := manager.cluster.GetBasicCluster().ScanRegions(bound.TxnLeftBound, bound.TxnRightBound, 2)
	if len(regions) != 1 {
		return nil
	}
	region := regions[0]
	if bytes.Compare(region.GetStartKey(), bound.TxnLeftBound) > 0 ||
		(len(region.GetEndKey()) != 0 && bytes.Compare(region.GetEndKey(), bound.TxnRightBound) < 0) {
		return nil
	}
	return region
}
func (manager *Manager) exactKeyspaceRegion(id uint32) (*core.RegionInfo, error) {
	region := manager.coveringKeyspaceRegion(id)
	bound := MakeRegionBound(id)
	if region == nil || !bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) || !bytes.Equal(region.GetEndKey(), bound.TxnRightBound) {
		return nil, errors.Errorf("keyspace %d does not have one complete Region", id)
	}
	return region, nil
}

// splittableKeyspaceRegion requires at least one missing keyspace boundary.
// Exact isolated Regions retain tenant metadata and cannot be reused without reset.
func (manager *Manager) splittableKeyspaceRegion(id uint32) *core.RegionInfo {
	region := manager.coveringKeyspaceRegion(id)
	if region == nil {
		return nil
	}
	bound := MakeRegionBound(id)
	if bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) && bytes.Equal(region.GetEndKey(), bound.TxnRightBound) {
		return nil
	}
	return region
}

// firstRegionKeyspace finds the first complete keyspace covered by the Region.
func firstRegionKeyspace(region *core.RegionInfo) uint32 {
	return uint32(sort.Search(int(constant.ReservedKeyspaceIDStart), func(id int) bool {
		return bytes.Compare(MakeRegionBound(uint32(id)).TxnLeftBound, region.GetStartKey()) >= 0
	}))
}

// lowerTombstonesReclaimed bounds work even if Cleaner requests IDs out of order.
// A tombstone before an unallocated hole is retained, but does not block the
// next deleted group. lowerFreeChecked means the caller already proved there
// are no free IDs below id; id itself is the free candidate being claimed.
func (manager *Manager) lowerTombstonesReclaimed(txn kv.Txn, region *core.RegionInfo, id uint32, lowerFreeChecked bool) (bool, error) {
	_, values, err := txn.LoadRange(keypath.KeyspaceMetaPath(firstRegionKeyspace(region)), keypath.KeyspaceMetaPath(id), freeIDScanLimit)
	if err != nil {
		return false, err
	}
	for _, value := range values {
		meta := &keyspacepb.KeyspaceMeta{}
		if err := proto.Unmarshal([]byte(value), meta); err != nil {
			return false, err
		}
		if meta.State != keyspacepb.KeyspaceState_TOMBSTONE {
			continue
		}
		// Explicitly read even an absent neighbor: ordinary creation does not
		// advance name_revision, so its insertion needs this missing-key CAS.
		nextID := meta.GetId() + 1
		next, err := manager.store.LoadKeyspaceMeta(txn, nextID)
		if err != nil || next != nil {
			return false, err
		}
		if lowerFreeChecked {
			if nextID == id {
				return false, nil
			}
			continue
		}
		width := uint32(manager.bitmapBytes() * 8)
		bits, err := txn.Load(bitmapPath(nextID / width))
		if err != nil {
			return false, err
		}
		if bits == "" {
			continue
		}
		if len(bits) != manager.bitmapBytes() {
			return false, errors.New("invalid free bitmap encoding")
		}
		if bits[(nextID%width)/8]&(1<<(nextID%8)) != 0 {
			return false, nil
		}
	}
	return len(values) < freeIDScanLimit, nil
}

// lowerFreeIDsClaimed preserves publication order inside one merged Region even
// when the allocation cursor starts on a later bitmap page.
func (manager *Manager) lowerFreeIDsClaimed(txn kv.Txn, region *core.RegionInfo, id uint32) (bool, error) {
	first := firstRegionKeyspace(region)
	width := uint32(manager.bitmapBytes() * 8)
	var keys, values []string
	if first/width < id/width {
		var err error
		keys, values, err = txn.LoadRange(bitmapPath(first/width), bitmapPath(id/width), freeIDScanLimit)
		if err != nil {
			return false, err
		}
	}
	complete := len(keys) < freeIDScanLimit
	// The mutation compares the target bucket's cached ModRevision. Keep the
	// authoritative lower-bucket scan and global revision fence for phantoms.
	value := manager.reuse.buckets[id/width]
	keys = append(keys, bitmapPath(id/width))
	values = append(values, value.Value)
	for i, key := range keys {
		bucket, err := strconv.ParseUint(key[len(bitmapPrefix()):], 10, 32)
		if err != nil || len(values[i]) != manager.bitmapBytes() {
			return false, errors.New("invalid free bitmap encoding")
		}
		for offset, bits := range []byte(values[i]) {
			for bit := uint32(0); bit < 8; bit++ {
				candidate := uint32(bucket)*width + uint32(offset*8) + bit
				if candidate >= first && candidate < id && bits&(1<<bit) != 0 {
					return false, nil
				}
			}
		}
	}
	return complete, nil
}

// RequestKeyspaceReclaim releases an ID without modifying TiKV. Cleaner can
// retry while waiting for a cross-keyspace merge.
func (manager *Manager) RequestKeyspaceReclaim(id uint32) error {
	if !reusableKeyspaceID(id) {
		return newModifyProtectedKeyspaceError()
	}
	if !manager.config.IsIDReuseEnabled() {
		return errors.New("keyspace ID reuse is disabled")
	}
	manager.reuse.claimMu.Lock()
	if manager.reuse.reclaimLimiter == nil {
		manager.reuse.reclaimLimiter = rate.NewLimiter(rate.Every(manager.config.GetIDReuseReclaimInterval()), 1)
	}
	limiter, ctx := manager.reuse.reclaimLimiter, manager.reuse.loaderCtx
	manager.reuse.claimMu.Unlock()
	if ctx == nil {
		ctx = manager.ctx
	}
	if err := limiter.Wait(ctx); err != nil {
		return err
	}
	meta, err := manager.LoadKeyspaceByID(id)
	if err != nil {
		if errors.ErrorEqual(err, errs.ErrKeyspaceNotFound) {
			return nil
		}
		return err
	}
	if meta.State != keyspacepb.KeyspaceState_TOMBSTONE {
		return errors.Errorf("cannot reclaim keyspace in state %s", meta.State)
	}
	if !manager.transactionalReuseConfig(meta.Config) {
		return errors.New("ID reuse only supports transactional keyspaces")
	}
	region := manager.coveringKeyspaceRegion(id)
	if region == nil || !reusableKeyspaceID(id+1) || !regionCoversKeyspace(region.GetMeta(), MakeRegionBound(id+1)) {
		return nil
	}
	if err := manager.reuseAdmissionReady(); err != nil {
		return err
	}
	return manager.releaseReclaimedID(meta, region)
}
func (manager *Manager) tryCompleteReusedCreate(expected *keyspacepb.KeyspaceMeta) error {
	if !manager.config.IsIDReuseEnabled() {
		return errors.New("keyspace ID reuse is disabled")
	}
	region, err := manager.exactKeyspaceRegion(expected.GetId())
	if err != nil {
		return nil
	}
	if err := manager.reuseAdmissionReady(); err != nil {
		return err
	}
	manager.reuse.mu.RLock()
	grant := manager.reuse.grants[expected.GetId()]
	confirmed := grant != nil && grant.confirmed
	belongs := false
	if grant != nil {
		_, belongs = grant.children[region.GetID()]
		belongs = belongs && region.GetRegionEpoch().GetVersion() == grant.source.GetRegionEpoch().GetVersion()+uint64(len(grant.children)-1)
	}
	manager.reuse.mu.RUnlock()
	if !belongs {
		return errors.New("reused keyspace no longer requires this creation's split")
	}
	// Heartbeat may arrive before ReportBatchSplit. Wait for that applied result,
	// but never treat an exact range alone as successful initialization.
	if !confirmed {
		return nil
	}
	return manager.completeReusedCreate(expected)
}

// Install the normal split rule only while the claimed range still needs split.
// Serialize with Archive so canceled work cannot reinstall a deleted rule.
func (manager *Manager) ensureReuseRegionRule(expected *keyspacepb.KeyspaceMeta) error {
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	current, err := manager.LoadKeyspaceByID(expected.GetId())
	if err != nil {
		return err
	}
	if !manager.isReuseInitializing(current) || current.Name != expected.Name || current.CreatedAt != expected.CreatedAt || !maps.Equal(current.Config, expected.Config) {
		return errors.New("pending creation changed before installing boundaries")
	}
	cluster, ok := manager.cluster.(interface{ GetRegionLabeler() *labeler.RegionLabeler })
	if !ok {
		return errors.New("cluster does not support region label")
	}
	if cluster.GetRegionLabeler().GetLabelRule(buildLabelRule(expected.GetId(), txnRegionBound).ID) != nil {
		return nil
	}
	if manager.splittableKeyspaceRegion(expected.GetId()) == nil {
		return errors.New("claimed keyspace no longer requires a split")
	}
	return manager.splitKeyspaceRegion(expected.GetId(), false, txnRegionBound)
}

// Complete only the same DISABLED owner with the same pending configuration. Archive
// cancellation and late heartbeats cannot enable a later use of this ID.
func (manager *Manager) completeReusedCreate(expected *keyspacepb.KeyspaceMeta) error {
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	var completed *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId())
		if err != nil {
			return err
		}
		if meta == nil || meta.Name != expected.Name || meta.CreatedAt != expected.CreatedAt || !manager.isReuseInitializing(meta) || !maps.Equal(meta.Config, expected.Config) {
			return errors.New("pending creation changed")
		}
		if manager.kgm != nil {
			if err := manager.kgm.UpdateKeyspaceForGroup(endpoint.StringUserKind(meta.Config[UserKindKey]), meta.Config[TSOKeyspaceGroupIDKey], meta.GetId(), opAdd); err != nil {
				return err
			}
		}
		meta.State = keyspacepb.KeyspaceState_ENABLED
		meta.StateChangedAt = manager.reuse.now().Unix()
		completed = meta
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err == nil {
		manager.cacheReuseMeta(completed)
	}
	return err
}

// cancelReusedCreate keeps the ID claimed and sends it through normal deletion.
// It never frees an ID or removes data while a split may still be in flight.
func (manager *Manager) cancelReusedCreate(expected *keyspacepb.KeyspaceMeta) error {
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	var canceled *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId())
		if err != nil {
			return err
		}
		if meta == nil || meta.Name != expected.Name || meta.CreatedAt != expected.CreatedAt || !manager.isReuseInitializing(meta) {
			return nil
		}
		if err := manager.transformKeyspaceState(txn, meta, keyspacepb.KeyspaceState_ARCHIVED, manager.reuse.now().Unix()); err != nil {
			return err
		}
		canceled = meta
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err == nil && canceled != nil {
		manager.cacheReuseMeta(canceled)
	}
	return err
}

func keyspaceBitmapLayoutPath() string    { return keypath.KeyspaceFreePrefix() + "layout" }
func bitmapPrefix() string                { return keypath.KeyspaceFreePrefix() + "buckets/" }
func bitmapPath(bucket uint32) string     { return fmt.Sprintf("%s%08d", bitmapPrefix(), bucket) }
func (manager *Manager) bitmapBytes() int { return manager.config.GetIDReuseBucketBytes() }
func (manager *Manager) ensureBitmapLayout() error {
	manager.reuse.claimMu.Lock()
	defer manager.reuse.claimMu.Unlock()
	return manager.ensureBitmapLayoutLocked()
}
func (manager *Manager) ensureBitmapLayoutLocked() error {
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		key := keypath.KeyspaceFreePrefix() + "layout"
		layout := fmt.Sprintf("v1:%d", manager.bitmapBytes())
		value, err := txn.Load(key)
		if err != nil {
			return err
		}
		if value != "" && value != layout {
			return errors.New("free bitmap layout differs from configured bucket size")
		}
		if value == "" {
			return txn.Save(key, layout)
		}
		return nil
	})
	manager.reuse.layoutReady = err == nil
	return err
}
func changedFreeBit(value string, width int, id uint32, free bool) (string, error) {
	bit := id % uint32(width*8)
	bits := make([]byte, width)
	if value != "" {
		if len(value) != width {
			return "", errors.New("invalid free bitmap bucket")
		}
		copy(bits, value)
	}
	mask := byte(1 << uint(bit%8))
	present := bits[bit/8]&mask != 0
	if present == free {
		return "", errs.ErrEtcdTxnConflict
	}
	if free {
		bits[bit/8] |= mask
	} else {
		bits[bit/8] &= ^mask
	}
	if bytes.Equal(bits, make([]byte, width)) {
		return "", nil
	}
	return string(bits), nil
}
func (manager *Manager) releaseReclaimedID(expected *keyspacepb.KeyspaceMeta, region *core.RegionInfo) error {
	released := false
	manager.metaLock.Lock(expected.GetId())
	defer manager.metaLock.Unlock(expected.GetId())
	manager.reuse.claimMu.Lock()
	defer manager.reuse.claimMu.Unlock()
	if !manager.reuse.layoutReady {
		return errors.New("free bitmap layout is not initialized")
	}
	bucket := expected.GetId() / uint32(manager.bitmapBytes()*8)
	value, err := manager.loadFreeBucketLocked(bucket)
	if err != nil {
		return err
	}
	var updated string
	var committed kv.RevisionedTxn
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		// Read before range scans: the revision CAS also detects newly inserted
		// bitmap buckets, which LoadRange alone cannot fence.
		if _, err := loadFreeIDRevision(txn); err != nil {
			return err
		}
		next, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId()+1)
		if err != nil {
			return err
		}
		if next == nil {
			id := expected.GetId() + 1
			bits, err := txn.Load(bitmapPath(id / uint32(manager.bitmapBytes()*8)))
			if err != nil {
				return err
			}
			if len(bits) != manager.bitmapBytes() || bits[(id%uint32(manager.bitmapBytes()*8))/8]&(1<<(id%8)) == 0 {
				return nil
			}
		} else if next.State != keyspacepb.KeyspaceState_TOMBSTONE {
			return nil
		}
		ready, err := manager.lowerTombstonesReclaimed(txn, region, expected.GetId(), false)
		if err != nil || !ready {
			return err
		}

		meta, err := manager.store.LoadKeyspaceMeta(txn, expected.GetId())
		if err != nil {
			return err
		}
		if meta == nil || meta.State != keyspacepb.KeyspaceState_TOMBSTONE || meta.StateChangedAt != expected.StateChangedAt {
			return errors.New("reclaimed state changed")
		}
		if err := manager.unassignKeyspaceFromMetaServiceGroup(txn, meta); err != nil {
			return err
		}
		if err := manager.store.RemoveKeyspace(txn, meta.GetId(), meta.Name); err != nil {
			return err
		}
		updated, err = manager.changeCachedFreeBit(txn, value, meta.GetId(), true)
		if err != nil {
			return err
		}
		committed, err = revisionedTxn(txn)
		if err != nil {
			return err
		}
		released = true
		return advanceFreeIDRevision(txn)
	})
	if err != nil {
		manager.invalidateFreeBucketLocked(bucket)
		if released && !errors.ErrorEqual(err, errs.ErrEtcdTxnConflict) && manager.reconcileReclaimedID(expected) {
			// Reconciliation loaded the authoritative post-commit revision.
			err = nil
			committed = nil
		}
	}
	if err == nil && released {
		if committed != nil {
			manager.publishFreeBucketLocked(bucket, updated, committed.CommitRevision())
		}
		manager.reuse.mu.Lock()
		delete(manager.reuse.pending, expected.GetId())
		delete(manager.reuse.grants, expected.GetId())
		manager.reuse.mu.Unlock()
		manager.keyspaceNameLookup.Delete(expected.GetId())
		manager.keyspaceStateLookup.Delete(expected.GetId())
	}
	return err
}
func (manager *Manager) tryCreateReusedKeyspace(name string, config map[string]string, createTime int64) (*keyspacepb.KeyspaceMeta, error) {
	var existing *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		found, id, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil || !found {
			return err
		}
		existing, err = manager.store.LoadKeyspaceMeta(txn, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, errs.ErrKeyspaceExists
	}
	if !manager.config.IsIDReuseEnabled() || !manager.transactionalReuseConfig(config) || !manager.supportsReuseEncryption(config) {
		return nil, nil
	}
	meta, err := manager.claimCachedFreeID(name, config, createTime)
	if err != nil || meta == nil {
		return nil, err
	}
	// Publish pending under claimMu before allowing the next claim; install the
	// split rule only after releasing it, respecting the metaLock lock order.
	if err := manager.ensureReuseRegionRule(meta); err != nil {
		if cancelErr := manager.cancelReusedCreate(meta); cancelErr != nil {
			return nil, cancelErr
		}
		return nil, err
	}
	return meta, nil
}

func (manager *Manager) claimCachedFreeID(name string, config map[string]string, createTime int64) (*keyspacepb.KeyspaceMeta, error) {
	manager.reuse.claimMu.Lock()
	if !manager.reuse.layoutReady {
		manager.reuse.claimMu.Unlock()
		return nil, nil
	}
	var candidates []kv.RevisionedValue
	for _, bucket := range manager.cachedFreeBucketsLocked() {
		candidates = append(candidates, manager.reuse.buckets[bucket])
	}
	manager.reuse.claimMu.Unlock()
	// Release the lock between candidates, so blocked Regions cannot hold up
	// loading or reclamation for an entire candidate scan.
	for _, candidate := range candidates {
		bucket, err := strconv.ParseUint(candidate.Key[len(bitmapPrefix()):], 10, 32)
		if err != nil {
			return nil, err
		}
		for offset, bits := range []byte(candidate.Value) {
			for bit := uint32(0); bit < 8; bit++ {
				if bits&(1<<bit) == 0 {
					continue
				}
				id := uint32(bucket)*uint32(manager.bitmapBytes()*8) + uint32(offset*8) + bit
				if !reusableKeyspaceID(id) {
					return nil, errors.New("protected ID in free bitmap")
				}
				meta, err := manager.claimAndPublishReusedKeyspace(id, name, config, createTime)
				if err != nil {
					return nil, err
				}
				if meta == nil {
					continue
				}
				return meta, nil
			}
		}
	}
	return nil, nil
}

func (manager *Manager) claimAndPublishReusedKeyspace(id uint32, name string, config map[string]string, createTime int64) (*keyspacepb.KeyspaceMeta, error) {
	manager.reuse.claimMu.Lock()
	defer manager.reuse.claimMu.Unlock()
	meta, err := manager.claimReusedKeyspace(id, name, config, createTime)
	if err != nil || meta == nil {
		return nil, err
	}
	manager.reuse.mu.Lock()
	pending := *meta
	pending.Config = cloneKeyspaceConfig(meta.Config)
	manager.reuse.pending[meta.GetId()] = &pending
	manager.reuse.mu.Unlock()
	manager.cacheReuseMeta(meta)
	manager.keyspaceNameLookup.Store(meta.GetId(), name)
	return meta, nil
}

// claimReusedKeyspace atomically publishes one candidate after all lower IDs in
// its current Region have been reclaimed and claimed. Split completion is not
// required; the caller serializes publication through pending-cache installation.
func (manager *Manager) claimReusedKeyspace(id uint32, name string, config map[string]string, createTime int64) (*keyspacepb.KeyspaceMeta, error) {
	bucket := id / uint32(manager.bitmapBytes()*8)
	for attempt := 0; attempt < 3; attempt++ {
		value, err := manager.loadFreeBucketLocked(bucket)
		if err != nil {
			return nil, err
		}
		if value.Value == "" {
			return nil, nil
		}
		var meta *keyspacepb.KeyspaceMeta
		var updated string
		var committed kv.RevisionedTxn
		err = manager.runTxnWithMetaGroupLock(func(txn kv.Txn) error {
			// Fence newly inserted bitmap buckets before any range scan.
			if _, err := loadFreeIDRevision(txn); err != nil {
				return err
			}
			region := manager.coveringKeyspaceRegion(id)
			if region == nil || !reusableKeyspaceID(id+1) || !regionCoversKeyspace(region.GetMeta(), MakeRegionBound(id+1)) || manager.reuseAdmissionReady() != nil {
				return nil
			}
			width := uint32(manager.bitmapBytes() * 8)
			bits := value.Value
			if len(bits) != manager.bitmapBytes() {
				return errors.New("invalid free bitmap encoding")
			}
			if bits[(id%width)/8]&(1<<(id%8)) == 0 {
				return nil
			}
			// Check free bits first so lower tombstone checks can reuse this
			// proof without extra bitmap reads and stay within etcd's op limit.
			ready, err := manager.lowerFreeIDsClaimed(txn, region, id)
			if err != nil || !ready {
				return err
			}
			ready, err = manager.lowerTombstonesReclaimed(txn, region, id, true)
			if err != nil || !ready {
				return err
			}
			exists, _, err := manager.store.LoadKeyspaceID(txn, name)
			if err != nil {
				return err
			}
			if exists {
				return errs.ErrKeyspaceExists
			}
			old, err := manager.store.LoadKeyspaceMeta(txn, id)
			if err != nil {
				return err
			}
			if old != nil {
				return errs.ErrEtcdTxnConflict
			}
			meta = &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: name, Config: cloneKeyspaceConfig(config), State: keyspacepb.KeyspaceState_DISABLED, CreatedAt: createTime, StateChangedAt: createTime}
			updated, err = manager.changeCachedFreeBit(txn, value, id, false)
			if err != nil {
				return err
			}
			committed, err = revisionedTxn(txn)
			if err != nil {
				return err
			}
			if err := manager.store.SaveKeyspaceID(txn, id, name); err != nil {
				return err
			}
			if manager.mgm != nil && manager.mgm.hasGroupsLocked() {
				groupID, groupErr := manager.mgm.findMinMetaGroup(txn)
				if groupErr != nil && !errors.ErrorEqual(groupErr, errNoAvailableMetaServiceGroups) {
					return groupErr
				}
				if groupErr == nil {
					meta.Config[MetaServiceGroupIDKey] = groupID
					if err := manager.mgm.updateAssignmentTxn(txn, "", groupID); err != nil {
						return err
					}
				}
			}
			if err := manager.store.SaveKeyspaceMeta(txn, meta); err != nil {
				return err
			}
			return advanceFreeIDRevision(txn)
		})
		if err == nil {
			if meta != nil {
				manager.publishFreeBucketLocked(bucket, updated, committed.CommitRevision())
			}
			return meta, nil
		}
		manager.invalidateFreeBucketLocked(bucket)
		if errors.ErrorEqual(err, errs.ErrEtcdTxnConflict) {
			continue
		}
		// A transport failure may hide a successful atomic claim. Reconcile the
		// original owner; never put the bit back merely because Commit failed.
		if meta != nil && manager.reconcileReusedClaim(meta) {
			return meta, nil
		}
		return nil, err
	}
	return nil, errors.New("free bitmap remained contended")
}
func (manager *Manager) waitReusedCreate(meta *keyspacepb.KeyspaceMeta) (result *keyspacepb.KeyspaceMeta, err error) {
	defer func() {
		if err != nil {
			if cancelErr := manager.cancelReusedCreate(meta); cancelErr != nil {
				err = errors.Annotatef(cancelErr, "cancel failed creation: %v", err)
			}
		}
	}()
	ticker := time.NewTicker(manager.config.GetCheckRegionSplitInterval())
	defer ticker.Stop()
	timer := time.NewTimer(manager.config.GetWaitRegionSplitTimeout())
	defer timer.Stop()
	for {
		current, err := manager.LoadKeyspaceByID(meta.GetId())
		if err != nil {
			return nil, err
		}
		if current.Name != meta.Name || current.CreatedAt != meta.CreatedAt {
			return nil, errors.New("pending creation identity changed")
		}
		if current.State == keyspacepb.KeyspaceState_ENABLED {
			return current, nil
		}
		if current.State != keyspacepb.KeyspaceState_DISABLED {
			return nil, errors.New("pending reused creation was abandoned")
		}
		select {
		case <-manager.ctx.Done():
			return nil, errors.New("pending reused creation canceled")
		case <-timer.C:
			return nil, errors.New("reused creation timed out and was canceled")
		case <-ticker.C:
			if err := manager.tryCompleteReusedCreate(meta); err != nil {
				return nil, err
			}
		}
	}
}

// A shared revision fences range scans against concurrently inserted free-ID
// buckets. Existing keys are already protected by the transaction's comparisons.
func freeIDRevisionPath() string {
	return fmt.Sprintf("/pd/%d/keyspaces/free/revision", keypath.ClusterID())
}
func loadFreeIDRevision(txn kv.Txn) (uint64, error) {
	value, err := txn.Load(freeIDRevisionPath())
	if err != nil || value == "" {
		return 0, err
	}
	return strconv.ParseUint(value, 10, 64)
}
func advanceFreeIDRevision(txn kv.Txn) error {
	revision, err := loadFreeIDRevision(txn)
	if err != nil {
		return err
	}
	if revision == ^uint64(0) {
		return errors.New("free ID revision exhausted")
	}
	return txn.Save(freeIDRevisionPath(), strconv.FormatUint(revision+1, 10))
}
