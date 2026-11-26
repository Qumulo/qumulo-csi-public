// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDriver(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "valid config",
			cfg: Config{
				Name:    "csi.qumulo.com",
				Version: "0.1.0",
				//	NodeID:  "node-1",
				Server: "qumulo.example.com",
				Client: &MockClient{},
			},
		},
		{
			name: "missing name",
			cfg: Config{
				Version: "0.1.0",
				Server:  "qumulo.example.com",
				Client:  &MockClient{},
			},
			wantErr: "name",
		},
		{
			name: "missing version",
			cfg: Config{
				Name:   "csi.qumulo.com",
				Server: "qumulo.example.com",
				Client: &MockClient{},
			},
			wantErr: "version",
		},
		{
			// Both are optional since the multi-cluster refactor: the
			// controller resolves an endpoint and a credential per request.
			// When supplied they are the single-cluster fallback that keeps
			// pre-refactor volumes manageable across an upgrade.
			name: "no server and no client is accepted",
			cfg: Config{
				Name:    "csi.qumulo.com",
				Version: "0.1.0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := NewDriver(tt.cfg)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, d)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, d)
			assert.NotNil(t, d.IdentityServer())
			assert.NotNil(t, d.ControllerServer())
		})
	}
}

// TestNewDriverPerMode covers what each role requires and which services it
// ends up serving. The requirements differ on purpose: a node instance holds
// no Qumulo credentials, so demanding them would mean mounting an API token
// on every worker for code that never calls the API.
func TestNewDriverPerMode(t *testing.T) {
	tests := []struct {
		name           string
		cfg            Config
		wantErr        string
		wantController bool
		wantNode       bool
	}{
		{
			name: "controller needs no node id",
			cfg: Config{
				Name:    "csi.qumulo.com",
				Version: "0.1.0",
				Mode:    ModeController,
				Server:  "qumulo.example.com",
				Client:  &MockClient{},
			},
			wantController: true,
		},
		{
			name: "node needs a node id but no credentials",
			cfg: Config{
				Name:    "csi.qumulo.com",
				Version: "0.1.0",
				Mode:    ModeNode,
				NodeID:  "node-1",
			},
			wantNode: true,
		},
		{
			name: "node without a node id is rejected",
			cfg: Config{
				Name:    "csi.qumulo.com",
				Version: "0.1.0",
				Mode:    ModeNode,
			},
			wantErr: "node ID",
		},
		{
			name: "all needs both",
			cfg: Config{
				Name:    "csi.qumulo.com",
				Version: "0.1.0",
				Mode:    ModeAll,
				NodeID:  "node-1",
				Server:  "qumulo.example.com",
				Client:  &MockClient{},
			},
			wantController: true,
			wantNode:       true,
		},
		{
			name: "all without a node id is rejected",
			cfg: Config{
				Name:    "csi.qumulo.com",
				Version: "0.1.0",
				Mode:    ModeAll,
				Server:  "qumulo.example.com",
				Client:  &MockClient{},
			},
			wantErr: "node ID",
		},
		{
			// A multi-cluster controller has neither a cluster address nor a
			// client: it builds one per request from the StorageClass
			// parameters and the request's secrets. Requiring them here would
			// make the multi-cluster deployment unstartable.
			// Multi-cluster: an "all" instance builds both services with no
			// cluster address and no client, resolving both per request.
			name: "all without a server or client still builds both services",
			cfg: Config{
				Name:    "csi.qumulo.com",
				Version: "0.1.0",
				Mode:    ModeAll,
				NodeID:  "node-1",
			},
			wantController: true,
			wantNode:       true,
		},
		{
			name: "unknown mode is rejected",
			cfg: Config{
				Name:    "csi.qumulo.com",
				Version: "0.1.0",
				Mode:    Mode("sidecar"),
			},
			wantErr: "unknown mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := NewDriver(tt.cfg)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, d)
				return
			}

			require.NoError(t, err)
			// Identity is mandatory for every plugin, whatever the role.
			assert.NotNil(t, d.IdentityServer())

			// A nil accessor is what tells main not to register the
			// service, so the absent cases matter as much as the present
			// ones -- registering an unconfigured service would answer RPCs
			// it cannot serve.
			if tt.wantController {
				assert.NotNil(t, d.ControllerServer(), "controller should be served")
			} else {
				assert.Nil(t, d.ControllerServer(), "controller should not be served")
			}
			if tt.wantNode {
				assert.NotNil(t, d.NodeServer(), "node should be served")
			} else {
				assert.Nil(t, d.NodeServer(), "node should not be served")
			}
		})
	}
}
