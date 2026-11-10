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

package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/server/config"
)

func TestKeyspaceIDReuseRequiresStartupConfiguration(t *testing.T) {
	cfg := config.NewConfig()
	require.NoError(t, cfg.Adjust(nil, false))
	old := cfg.Keyspace.Clone()
	changed := old.Clone()
	changed.EnableIDReuse = true
	s := &Server{}
	require.ErrorContains(t, s.setKeyspaceConfigInner(old, changed, false), "restarting PD")
	// Startup configuration wins over persisted settings from the previous run.
	s.cfg = cfg
	s.persistOptions = config.NewPersistOptions(cfg)
	s.keyspaceManager = keyspace.NewKeyspaceManager(context.Background(), nil, nil, nil, old, nil, nil)
	for _, enabled := range []bool{true, false} {
		s.cfg.Keyspace.EnableIDReuse = enabled
		stale := old.Clone()
		stale.EnableIDReuse = !enabled
		s.persistOptions.SetKeyspaceConfig(stale)
		s.loadKeyspaceConfig()
		require.Equal(t, enabled, s.persistOptions.GetKeyspaceConfig().EnableIDReuse)
	}
}
