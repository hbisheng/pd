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

package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestKeyspaceReclaimAPI(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion")) })
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			ctx := t.Context()
			cluster, err := tests.NewTestCluster(ctx, 1, func(cfg *config.Config, _ string) {
				cfg.Keyspace.EnableIDReuse = enabled
				cfg.Keyspace.IDReuseThreshold = 1
				cfg.PDServerCfg.KeyType = "txn"
			})
			require.NoError(t, err)
			defer cluster.Destroy()
			require.NoError(t, cluster.RunInitialServers())
			require.NotEmpty(t, cluster.WaitLeader())
			leader := cluster.GetLeaderServer()
			require.NoError(t, leader.BootstrapCluster())
			manager := leader.GetKeyspaceManager()
			ids := make([]uint32, 0, 2)
			for _, name := range []string{"deleted-a", "deleted-b"} {
				meta, err := manager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{Name: name, CreateTime: 1})
				require.NoError(t, err)
				ids = append(ids, meta.GetId())
				for _, state := range []keyspacepb.KeyspaceState{keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE} {
					_, err = manager.UpdateKeyspaceStateByID(meta.GetId(), state, 2)
					require.NoError(t, err)
				}
			}
			require.Equal(t, ids[0]+1, ids[1])
			// Supply the merged Region that makes the first ID eligible for reclamation.
			leader.GetRaftCluster().GetBasicCluster().PutRegion(core.NewRegionInfo(&metapb.Region{
				Id: 100, StartKey: keyspace.MakeRegionBound(ids[0]).TxnLeftBound,
				EndKey: keyspace.MakeRegionBound(ids[1]).TxnRightBound,
			}, nil))
			client := &http.Client{Timeout: 10 * time.Second}
			url := fmt.Sprintf("%s/pd/api/v2/keyspaces/id/%d/reclaim", leader.GetAddr(), ids[0])
			resp, err := client.Post(url, "application/json", nil)
			require.NoError(t, err)
			defer resp.Body.Close()
			if !enabled {
				require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
				_, err = manager.LoadKeyspace("deleted-a")
				require.NoError(t, err)
				return
			}
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var result struct {
				Reclaimed bool `json:"reclaimed"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			require.True(t, result.Reclaimed)
			_, err = manager.LoadKeyspace("deleted-a")
			require.Error(t, err)
			_, err = manager.LoadKeyspace("deleted-b")
			require.NoError(t, err)
		})
	}
}
