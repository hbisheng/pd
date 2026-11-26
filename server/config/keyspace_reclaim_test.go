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

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestKeyspaceIDReclaimConfiguration(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, cfg.Adjust(nil, false))
	c := cfg.Keyspace
	require.False(t, c.IsIDReclaimEnabled())
	require.Equal(t, 64, c.GetFreeIDBucketBytes())
	require.Equal(t, 10*time.Millisecond, c.GetIDReclaimInterval())
	for _, update := range []func(*KeyspaceConfig){
		func(c *KeyspaceConfig) { c.FreeIDBucketBytes = 63 },
		func(c *KeyspaceConfig) { c.FreeIDBucketBytes = 8192 },
		func(c *KeyspaceConfig) { c.IDReclaimInterval = typeutil.NewDuration(-time.Second) },
	} {
		copy := c.Clone()
		update(copy)
		require.NoError(t, copy.Validate(), "disabled reclamation must ignore its inactive settings")
		copy.EnableIDReclaim = true
		require.Error(t, copy.Validate())
	}
}
