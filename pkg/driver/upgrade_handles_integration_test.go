// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// A PV's volumeHandle is immutable, so an
// upgraded driver meets every handle form an earlier build ever minted, for
// the life of the volume. Nothing tested that end to end: stranded every
// volume made before the three-field handle, and only unit tests of the
// parser caught up with it. This runs the volume lifecycle an upgraded
// cluster does -- validate, expand, delete -- through the real HTTP client
// against the fake cluster, for a volume made by the current build and then
// addressed by each earlier handle form, and checks what is left on the
// array.
func TestAVolumeMadeByAnEarlierBuildStillExpandsAndDeletes(t *testing.T) {
	for _, form := range []struct {
		name   string
		handle func(realPath string) string
	}{
		{name: "the current <server>#<apiUrl>#<path>, endpoint empty", handle: func(p string) string {
			return testServer + "##" + p
		}},
		{name: "the earliest builds: <server>#<path>", handle: func(p string) string {
			return testServer + "#" + p
		}},
	} {
		t.Run(form.name, func(t *testing.T) {
			cluster := newFakeCluster()
			server := httptest.NewServer(cluster.handler())
			defer server.Close()
			srv := newIntegrationControllerServer(t, server.URL)

			_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Secrets: testSecrets(), Name: "pvc-upgraded",
				VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
				CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
				Parameters: map[string]string{
					"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
					"provisioningModel": "dedicated-export", "allowAnyClient": "true",
				},
			})
			require.NoError(t, err)
			handle := form.handle("/csi-volumes/pvc-upgraded")
			fileID := cluster.directories["/csi-volumes/pvc-upgraded"]
			require.NotEmpty(t, fileID)

			_, err = srv.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: handle, Secrets: testSecrets(),
				VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
			})
			require.NoError(t, err, "validate")

			expanded, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
				VolumeId: handle, Secrets: testSecrets(), CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
			})
			require.NoError(t, err, "expand")
			assert.EqualValues(t, 2<<30, expanded.GetCapacityBytes())
			assert.Equal(t, "2147483648", cluster.quotas[fileID], "the array's quota grew")

			_, err = srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})
			require.NoError(t, err, "delete")
			assert.NotContains(t, cluster.directories, "/csi-volumes/pvc-upgraded", "the directory is gone")
			assert.Empty(t, cluster.quotas, "the quota is gone")
			assert.Empty(t, cluster.exports, "the export is gone")
		})
	}
}

// The one form no build can map back to a directory: the retired
// <server>/<fileID>/<name>, from before handles carried a path. It is pinned
// here so a change to how it is treated is deliberate. Delete answers OK and
// removes nothing, as for any handle the driver did not issue (csi-sanity
// requires that); expand refuses it; the directory is untouched.
func TestTheRetiredFileIDHandleChangesNothing(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()
	srv := newIntegrationControllerServer(t, server.URL)
	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets: testSecrets(), Name: "pvc-ancient",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         map[string]string{"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi"},
	})
	require.NoError(t, err)
	fileID := cluster.directories["/csi-volumes/pvc-ancient"]
	retired := testServer + "/" + fileID + "/pvc-ancient"

	_, err = srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: retired, Secrets: testSecrets(), CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
	})
	requireCode(t, err, codes.NotFound)

	// Refused rather than answered "already gone", which dropped the PV and
	// left this volume on the array with nothing pointing at it.
	_, err = srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: retired, Secrets: testSecrets()})
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, cluster.directories, "/csi-volumes/pvc-ancient", "nothing is deleted through a handle that names no path")
	assert.Equal(t, "1073741824", cluster.quotas[fileID])
}
