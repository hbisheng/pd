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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/core"
)

func (r *bitmapEtcdRecorder) resetCounts() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads, r.comparisons, r.transactions = nil, nil, 0
}

func (r *bitmapEtcdRecorder) logCounts(t *testing.T, name string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	points, ranges, bitmap, revision := 0, 0, 0, 0
	for _, read := range r.reads {
		key := string(read.KeyBytes())
		if len(read.RangeBytes()) == 0 {
			points++
		} else {
			ranges++
		}
		if strings.HasPrefix(key, bitmapPrefix()) {
			bitmap++
		}
		if key == freeIDRevisionPath() {
			revision++
		}
	}
	t.Logf("READ_COUNTS case=%s point=%d range=%d bitmap=%d free_revision=%d txn=%d", name, points, ranges, bitmap, revision, r.transactions)
}

func TestReuseReadOperationCounts(t *testing.T) {
	for _, sample := range []struct {
		name       string
		first, id  uint32
		tombstones []uint32
	}{
		{"same-bucket", 42, 42, nil},
		{"cross-bucket", 42, 298, nil},
		{"multiple-tombstones", 42, 80, []uint32{42, 44}},
	} {
		t.Run(sample.name, func(t *testing.T) {
			recorder, _, cluster, manager := newRecordedReuseManager(t)
			cluster.PutRegion(newReuseTestRegion(sample.first, 420).Clone(core.WithEndKey(MakeRegionBound(sample.id + 1).TxnRightBound)))
			for _, id := range sample.tombstones {
				saveReuseTestMeta(t, manager, id, keyspacepb.KeyspaceState_TOMBSTONE)
			}
			seedReuseTestBit(t, manager, sample.id, true)
			recorder.resetCounts()
			meta, err := manager.tryCreateReusedKeyspace("counted", nil, 1)
			require.NoError(t, err)
			require.Equal(t, sample.id, meta.GetId())
			recorder.logCounts(t, sample.name)
			require.Len(t, recorder.reads, 3, "only name, target and split-rule safety checks remain")
			for _, read := range recorder.reads {
				require.Empty(t, read.RangeBytes(), "warm Create must not discover ordering through ranges")
				require.NotEqual(t, freeIDRevisionPath(), string(read.KeyBytes()))
				require.False(t, strings.HasPrefix(string(read.KeyBytes()), bitmapPrefix()))
			}
			if sample.name == "same-bucket" {
				recorder.resetCounts()
				_, err = manager.waitReusedCreate(meta)
				require.ErrorContains(t, err, "timed out")
				recorder.logCounts(t, "split-wait-timeout")
				require.Len(t, recorder.reads, 1, "only the final authoritative cancellation reads metadata")
				require.Equal(t, 1, recorder.transactions)
			}
		})
	}
}
