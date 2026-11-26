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

func TestParseMode(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		want           Mode
		wantController bool
		wantNode       bool
		wantErr        string
	}{
		{
			// The zero value must resolve to the role that needs no node
			// identity, so a Config with no mode set cannot serve a node
			// service advertising an empty node ID.
			name:           "empty resolves to controller",
			input:          "",
			want:           ModeController,
			wantController: true,
		},
		{
			name:           "controller",
			input:          "controller",
			want:           ModeController,
			wantController: true,
		},
		{
			name:     "node",
			input:    "node",
			want:     ModeNode,
			wantNode: true,
		},
		{
			name:           "all",
			input:          "all",
			want:           ModeAll,
			wantController: true,
			wantNode:       true,
		},
		{
			name:    "unknown",
			input:   "sidecar",
			wantErr: "unknown mode",
		},
		{
			name:    "case sensitive",
			input:   "Controller",
			wantErr: "unknown mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMode(tt.input)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantController, got.ServesController(), "ServesController")
			assert.Equal(t, tt.wantNode, got.ServesNode(), "ServesNode")
		})
	}
}

// TestModeStringNamesTheZeroValue covers the zero value reaching a log line
// or an error message, where an empty string would read as a missing mode
// rather than as the default one.
func TestModeStringNamesTheZeroValue(t *testing.T) {
	var unset Mode

	assert.Equal(t, "controller", unset.String())
	assert.Equal(t, "node", ModeNode.String())
	assert.Equal(t, "all", ModeAll.String())
}
