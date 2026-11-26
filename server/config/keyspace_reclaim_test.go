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

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestKeyspaceIDReclaimConfiguration(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, cfg.Adjust(nil, false))
	c := cfg.Keyspace
	require.False(t, c.IsIDReclaimEnabled())
	require.False(t, c.IsIDReuseEnabled())
	require.False(t, c.IsEncryptedIDReuseEnabled())
	require.Equal(t, 100, c.GetIDReclaimRateLimit())
	for _, limit := range []int{0, -1} {
		copy := c.Clone()
		copy.IDReclaimRateLimit = limit
		copy.EnableIDReuse = true
		require.NoError(t, copy.Validate(), "disabled reclamation must ignore its inactive settings")
		copy.EnableIDReclaim = true
		require.ErrorContains(t, copy.Validate(), "rate limit must be positive")
	}

	// Explicit rates are requests per second and survive configuration adjustment.
	meta, err := toml.Decode("[keyspace]\nenable-id-reclaim = true\nid-reclaim-rate-limit = 37", cfg)
	require.NoError(t, err)
	require.NoError(t, cfg.Adjust(&meta, false))
	require.NoError(t, cfg.Keyspace.Validate())
	require.Equal(t, 37, cfg.Keyspace.GetIDReclaimRateLimit())
}
