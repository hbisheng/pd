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

func TestKeyspaceIDReuseConfiguration(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, cfg.Adjust(nil, false))
	c := cfg.Keyspace
	require.False(t, c.IsIDReuseEnabled())
	require.Equal(t, uint64(1_000_000), c.GetIDReuseThreshold())
	require.Equal(t, 64, c.GetIDReuseBucketBytes())
	require.Equal(t, 64, c.GetIDReuseLoadPageBuckets())
	require.Equal(t, 100*time.Millisecond, c.GetIDReuseLoadInterval())
	require.Equal(t, 10*time.Millisecond, c.GetIDReuseReclaimInterval())
	for _, update := range []func(*KeyspaceConfig){
		func(c *KeyspaceConfig) { c.IDReuseBucketBytes = 63 },
		func(c *KeyspaceConfig) { c.IDReuseBucketBytes = 8192 },
		func(c *KeyspaceConfig) { c.IDReuseLoadPageBuckets = 257 },
		func(c *KeyspaceConfig) { c.IDReuseLoadInterval = typeutil.NewDuration(-time.Second) },
		func(c *KeyspaceConfig) { c.IDReuseReclaimInterval = typeutil.NewDuration(-time.Second) },
	} {
		copy := c.Clone()
		update(copy)
		require.Error(t, copy.Validate())
	}
}
