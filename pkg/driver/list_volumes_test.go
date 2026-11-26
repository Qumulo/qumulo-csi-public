// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The two inventory RPCs must refuse the same way in every deployment that
// has not opted in, and the refusal must name the value that turns them on.
//
// FailedPrecondition rather than Unimplemented, and that distinction matters:
// Unimplemented would say "no configuration of this driver can do this", which
// was true until inventory.go landed and is now false. FailedPrecondition says
// "not as deployed", which is what an operator can act on.
func TestInventoryRPCsRefuseWhenNotEnabled(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})
	ctx := context.Background()

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "ListVolumes",
			call: func() error {
				_, err := srv.ListVolumes(ctx, &csi.ListVolumesRequest{})
				return err
			},
		},
		{
			name: "ListVolumes with a max_entries",
			call: func() error {
				_, err := srv.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 10})
				return err
			},
		},
		{
			name: "ControllerGetVolume",
			call: func() error {
				_, err := srv.ControllerGetVolume(ctx,
					&csi.ControllerGetVolumeRequest{VolumeId: testHandle("pvc-a")})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()

			require.Error(t, err)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err),
				"not as deployed, rather than not implementable")

			msg := status.Convert(err).Message()
			assert.Contains(t, msg, "controller.inventory.enabled",
				"the refusal must name the value that turns this on")
		})
	}
}

// The advertised set must not include a capability whose RPC always refuses.
// A CO that sees LIST_VOLUMES will call it; getting Unimplemented back for
// something the driver claimed is exactly the confusion this avoids.
// TestControllerCapabilitiesMatchWhatIsServed asserts the advertised set is
// exactly the set of RPCs with a real implementation.
//
// The pairing is what matters, in both directions. An advertised capability
// with no implementation makes the CO call an RPC that always errors; an
// implementation with no capability is never called at all. GET_CAPACITY moved
// from the second column to the first when get_capacity.go landed, and this
// test is what would have caught it being served without being advertised.
func TestControllerCapabilitiesMatchWhatIsServed(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})

	resp, err := srv.ControllerGetCapabilities(context.Background(),
		&csi.ControllerGetCapabilitiesRequest{})
	require.NoError(t, err)

	got := make([]csi.ControllerServiceCapability_RPC_Type, 0, len(resp.GetCapabilities()))
	for _, c := range resp.GetCapabilities() {
		got = append(got, c.GetRpc().GetType())
	}

	assert.ElementsMatch(t, []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_GET_VOLUME,
		csi.ControllerServiceCapability_RPC_GET_VOLUME_HEALTH,
	}, got)

	// Advertised even though the last two need controller.inventory.enabled to
	// answer. The set is a property of the build, not of a deployment: a
	// capability that appears and disappears with a value leaves an operator
	// unable to tell a misconfiguration from a bug. This test is here to make
	// that a decision rather than an accident -- if the set ever becomes
	// computed, it fails.
	//
	// LIST_VOLUMES_PUBLISHED_NODES must never appear: there is no
	// ControllerPublishVolume and no VolumeAttachment to read node IDs from,
	// so the field could only be fabricated.
	assert.NotContains(t, got, csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES)
}
