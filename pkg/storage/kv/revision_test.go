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

package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

func TestRevisionCASAndCommittedRevision(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	for name, backend := range map[string]Base{"memory": NewMemoryKV(), "etcd": NewEtcdKVBase(client)} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var committed RevisionedTxn
			err := backend.RunInTxn(ctx, func(txn Txn) error {
				committed = txn.(RevisionedTxn)
				committed.CompareModRevision("bucket", 0)
				return txn.Save("bucket", "one")
			})
			require.NoError(t, err)
			firstRevision := committed.CommitRevision()
			require.Positive(t, firstRevision)
			require.NoError(t, backend.RunInTxn(ctx, func(txn Txn) error {
				values, err := txn.(RevisionedTxn).LoadRangeWithRevision("bucket", "bucket\x00", 1)
				require.Equal(t, []RevisionedValue{{Key: "bucket", Value: "one", ModRevision: firstRevision}}, values)
				return err
			}))
			// Even writing the same value advances ModRevision and rejects an ABA.
			require.NoError(t, backend.Save("bucket", "one"))
			err = backend.RunInTxn(ctx, func(txn Txn) error {
				txn.(RevisionedTxn).CompareModRevision("bucket", firstRevision)
				return txn.Save("owner", "must-not-commit")
			})
			require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
			owner, err := backend.Load("owner")
			require.NoError(t, err)
			require.Empty(t, owner)
			require.NoError(t, backend.Remove("bucket"))
			require.NoError(t, backend.RunInTxn(ctx, func(txn Txn) error {
				committed = txn.(RevisionedTxn)
				committed.CompareModRevision("bucket", 0)
				if err := txn.Save("bucket", "two"); err != nil {
					return err
				}
				return txn.Save("owner", "two")
			}))
			require.Greater(t, committed.CommitRevision(), firstRevision)
			require.NoError(t, backend.RunInTxn(ctx, func(txn Txn) error {
				values, err := txn.(RevisionedTxn).LoadRangeWithRevision("bucket", "owner\x00", 2)
				require.Len(t, values, 2)
				for _, value := range values {
					require.Equal(t, committed.CommitRevision(), value.ModRevision)
				}
				return err
			}))
		})
	}
}
