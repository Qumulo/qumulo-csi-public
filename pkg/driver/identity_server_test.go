// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestIdentityServer(t *testing.T, client *MockClient) *IdentityServer {
	t.Helper()

	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		//NodeID:  "node-1",
		Server: "qumulo.example.com",
		Client: client,
	})
	require.NoError(t, err)

	return NewIdentityServer(d)
}

func TestGetPluginInfo(t *testing.T) {
	srv := newTestIdentityServer(t, &MockClient{})

	resp, err := srv.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "csi.qumulo.com", resp.GetName())
	assert.Equal(t, "0.1.0", resp.GetVendorVersion())
}

func TestGetPluginCapabilities(t *testing.T) {
	srv := newTestIdentityServer(t, &MockClient{})

	resp, err := srv.GetPluginCapabilities(context.Background(), &csi.GetPluginCapabilitiesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.GetCapabilities(), 3)

	// Collected by kind rather than read by index, so adding a capability does
	// not silently shift what an index-based assertion is checking.
	services := map[csi.PluginCapability_Service_Type]bool{}
	var expansion *csi.PluginCapability_VolumeExpansion
	for _, capability := range resp.GetCapabilities() {
		if svc := capability.GetService(); svc != nil {
			services[svc.GetType()] = true
		}
		if exp := capability.GetVolumeExpansion(); exp != nil {
			expansion = exp
		}
	}

	assert.True(t, services[csi.PluginCapability_Service_CONTROLLER_SERVICE])

	// Without this the CO ignores the topology NodeGetInfo and CreateVolume
	// report, which is what csi-provisioner's capacity sidecar needs to keep a
	// stable CSIStorageCapacity object. See topology.go.
	assert.True(t, services[csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS],
		"VOLUME_ACCESSIBILITY_CONSTRAINTS must be advertised or the driver's topology is ignored")

	// Expansion has to be advertised here as well as on the controller
	// service, or the CO concludes the plugin cannot grow a volume at all.
	require.NotNil(t, expansion)
	assert.Equal(t, csi.PluginCapability_VolumeExpansion_ONLINE, expansion.GetType())
}

// The two halves of the topology must agree.
//
// NodeGetInfo says which segment a node is in; CreateVolume says which segments
// a volume can be reached from. If they diverge, the PV carries a nodeAffinity
// naming a segment no node reports, and every pod using it stays Pending with
// nothing in the driver's logs to explain it.
func TestTopologyIsConsistentBetweenNodeAndVolume(t *testing.T) {
	node := nodeTopology()
	require.Equal(t, map[string]string{topologyKeyPlugin: topologyValuePresent}, node.GetSegments())

	volume := volumeTopology()
	require.Len(t, volume, 1, "one segment: a per-node value would make one capacity object per node")
	assert.Equal(t, node.GetSegments(), volume[0].GetSegments(),
		"CreateVolume must advertise exactly the segment NodeGetInfo reports")
}

// Probe reports process liveness, not cluster reachability.
//
// One controller serves many clusters chosen per StorageClass and is never told
// the full set, so there is nothing to health-check. The previous behaviour also
// misled: it called an endpoint requiring no authentication, so a controller
// with a revoked token reported healthy while every provision failed 401.
func TestProbeReportsLivenessNotClusterHealth(t *testing.T) {
	for _, tt := range []struct {
		name      string
		clientErr error
	}{
		{name: "reachable cluster"},
		{name: "unreachable cluster", clientErr: errors.New("dial tcp: connection refused")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestIdentityServer(t, &MockClient{err: tt.clientErr})

			resp, err := srv.Probe(context.Background(), &csi.ProbeRequest{})

			require.NoError(t, err, "Probe must not fail on cluster state it no longer checks")
			require.NotNil(t, resp)
			assert.True(t, resp.GetReady().GetValue())
		})
	}
}

// A node instance holds no client at all, and must still answer Ready -- Probe
// is the first RPC the kubelet sends.
func TestProbeNodeModeReadyWithoutClient(t *testing.T) {
	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "0.1.0", Mode: ModeNode, NodeID: "node-1",
	})
	require.NoError(t, err)

	resp, err := NewIdentityServer(d).Probe(context.Background(), &csi.ProbeRequest{})

	require.NoError(t, err)
	assert.True(t, resp.GetReady().GetValue())
}

// Likewise a multi-cluster controller, which has neither a cluster address nor
// a client.
func TestProbeControllerModeReadyWithoutClient(t *testing.T) {
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "0.1.0"})
	require.NoError(t, err)

	resp, err := NewIdentityServer(d).Probe(context.Background(), &csi.ProbeRequest{})

	require.NoError(t, err)
	assert.True(t, resp.GetReady().GetValue())
}
