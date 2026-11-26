// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The node pod is Ready only once the kubelet has registered the plugin,
// which the first NodeGetInfo proves. Before that call the readiness check
// fails; after it, it passes, and keeps passing across a new NodeServer (a
// driver-container restart, which brings no new NodeGetInfo).
func TestNodeGetInfoRecordsTheRegistrationForReadiness(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "state", "registered")
	ns := newTestNodeServer(t)
	ns.driver.RegistrationMarker = marker

	require.Error(t, RegistrationRecorded(marker), "Ready before the kubelet called NodeGetInfo")

	_, err := ns.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	require.NoError(t, err)
	require.NoError(t, RegistrationRecorded(marker))

	restarted := newTestNodeServer(t)
	restarted.driver.RegistrationMarker = marker
	assert.NoError(t, RegistrationRecorded(marker), "a driver restart must not make the pod NotReady")
}

// A marker that cannot be written leaves the pod NotReady but never fails the
// registration: NodeGetInfo still answers.
func TestNodeGetInfoAnswersWhenTheMarkerCannotBeWritten(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocker, nil, 0o644))
	marker := filepath.Join(blocker, "registered") // parent is a file

	ns := newTestNodeServer(t)
	ns.driver.RegistrationMarker = marker
	resp, err := ns.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	require.NoError(t, err)
	assert.Equal(t, "node-1", resp.GetNodeId())
	assert.Error(t, RegistrationRecorded(marker))
}

// No marker configured (tests, csi-sanity, all mode): NodeGetInfo writes
// nothing.
func TestNodeGetInfoWithoutAMarkerWritesNothing(t *testing.T) {
	ns := newTestNodeServer(t)
	_, err := ns.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	require.NoError(t, err)
}
