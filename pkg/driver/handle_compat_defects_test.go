//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// Handles that carry this driver's server and a path or snapshot ID, but in a
// form this build does not decode: a retired form, and one a later build
// might mint. Neither shows that the volume or snapshot is gone.
var (
	dfxUnknownFormVolumeHandles = []string{
		testServer + "/101/pvc-0f9c2a",                                     // <server>/<fileID>/<name>, retired
		testServer + "#https://192.0.2.10:8000#/csi-volumes/pvc-0f9c2a#v2", // a fourth field
	}
	dfxUnknownFormSnapshotHandles = []string{
		testServer + "/101/4242", // <server>/<fileID>/<snapshotID>
		testServer + "##4242#v2", // a fourth field
	}
)

// DeleteVolume must refuse (InvalidArgument or FailedPrecondition)
// a handle carrying this driver's server in a form it cannot decode, not answer OK.
func TestDefect_DeleteVolumeDoesNotReportAnUndecodableHandleAsDeleted(t *testing.T) {
	for _, handle := range dfxUnknownFormVolumeHandles {
		t.Run(handle, func(t *testing.T) {
			m := &MockClient{LookupFileByPathFunc: missingVolume()}
			_, err := newTestControllerServer(t, m).DeleteVolume(context.Background(),
				&csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})
			t.Logf("DeleteVolume answered %v", err)
			if err == nil {
				t.Fatalf("DeleteVolume answered OK for %q without being able to read it", handle)
			}
			if c := status.Code(err); c != codes.InvalidArgument && c != codes.FailedPrecondition {
				t.Errorf("DeleteVolume answered %v; want InvalidArgument or FailedPrecondition", c)
			}
		})
	}
}

// The same for snapshot handles.
func TestDefect_DeleteSnapshotDoesNotReportAnUndecodableHandleAsDeleted(t *testing.T) {
	for _, handle := range dfxUnknownFormSnapshotHandles {
		t.Run(handle, func(t *testing.T) {
			m := &MockClient{}
			_, err := newTestControllerServer(t, m).DeleteSnapshot(context.Background(),
				&csi.DeleteSnapshotRequest{SnapshotId: handle, Secrets: testSecrets()})
			t.Logf("DeleteSnapshot answered %v", err)
			if err == nil {
				t.Fatalf("DeleteSnapshot answered OK for %q without being able to read it", handle)
			}
			if c := status.Code(err); c != codes.InvalidArgument && c != codes.FailedPrecondition {
				t.Errorf("DeleteSnapshot answered %v; want InvalidArgument or FailedPrecondition", c)
			}
		})
	}
}

// A handle in this driver's form whose server is an IPv6 literal
// must be refused like any other unreadable handle, not answered OK: ":" is
// outside the host-name alphabet the shape test used, so the volume and its
// export were left on the array while the PV went.
// See volume_id.go inDriverHandleShape, hostChars.
func TestDefect_DeleteVolumeRefusesAnUndecodableHandleWithAnIPv6Server(t *testing.T) {
	for _, handle := range []string{"fd00::10/101/pvc-0f9c2a", "[fd00::10]##/csi-volumes/pvc-0f9c2a#v2"} {
		t.Run(handle, func(t *testing.T) {
			m := &MockClient{LookupFileByPathFunc: missingVolume()}
			_, err := newTestControllerServer(t, m).DeleteVolume(context.Background(),
				&csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})
			t.Logf("DeleteVolume answered %v", err)
			if err == nil {
				t.Fatalf("DeleteVolume answered OK for %q without being able to read it", handle)
			}
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
}

// the same for snapshot handles. See volume_id.go inDriverHandleShape, hostChars.
func TestDefect_DeleteSnapshotRefusesAnUndecodableHandleWithAnIPv6Server(t *testing.T) {
	for _, handle := range []string{"fd00::10/101/4242", "fd00::10##4242#v2"} {
		t.Run(handle, func(t *testing.T) {
			var deleted []string
			m := &MockClient{DeleteSnapshotFunc: func(_ context.Context, id string) error {
				deleted = append(deleted, id)
				return nil
			}}
			_, err := newTestControllerServer(t, m).DeleteSnapshot(context.Background(),
				&csi.DeleteSnapshotRequest{SnapshotId: handle, Secrets: testSecrets()})
			t.Logf("DeleteSnapshot answered %v", err)
			if err == nil {
				t.Fatalf("DeleteSnapshot answered OK for %q without being able to read it", handle)
			}
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Empty(t, deleted)
		})
	}
}

// A volume handle in the earlier build's form <server>#<path> must
// keep working end to end: validate, expand and delete, and the delete must
// remove the directory, quota and export rather than answer OK and leak.
func TestDefect_EarlierBuildVolumeHandleWorksEndToEnd(t *testing.T) {
	cluster := newFakeCluster()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()
	srv := newIntegrationControllerServer(t, server.URL)
	ctx := context.Background()

	req := dfxOptedInCreate("pvc-legacy")
	req.Parameters["allowedClients"] = "192.0.2.0/24"
	created, err := srv.CreateVolume(ctx, req)
	require.NoError(t, err)
	current := created.GetVolume().GetVolumeId()
	require.Equal(t, testServer+"##/csi-volumes/pvc-legacy", current)
	require.Len(t, cluster.exports, 1)

	legacy := testServer + "#/csi-volumes/pvc-legacy" // <server>#<path>, the earlier build's form

	_, err = srv.ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: legacy, VolumeCapabilities: mountCapabilities(), Secrets: testSecrets()})
	assert.NoError(t, err, "validate of an earlier-build handle")

	expanded, err := srv.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId: legacy, Secrets: testSecrets(), CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB}})
	if assert.NoError(t, err, "expand of an earlier-build handle") {
		assert.Equal(t, 2*testGiB, expanded.GetCapacityBytes())
	}

	_, err = srv.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: legacy, Secrets: testSecrets()})
	require.NoError(t, err)
	assert.Empty(t, cluster.directories, "the directory was left on the array")
	assert.Empty(t, cluster.quotas, "the quota was left on the array")
	assert.Empty(t, cluster.exports, "the export was left on the array")
}

// A snapshot handle in the earlier build's form <server>/<id> must
// be deleted on the array, not answered OK and left there.
func TestDefect_EarlierBuildSnapshotHandleIsReallyDeleted(t *testing.T) {
	var deleted []string
	var dialled []string
	m := &MockClient{
		GetSnapshotFunc: func(_ context.Context, id string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: id, Name: id + "_snapshot-1", SourceFileID: "101"}, nil
		},
		ListSnapshotsFunc: func(context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{{ID: "4242", Name: "4242_snapshot-1", SourceFileID: "101"}}, nil
		},
		DeleteSnapshotFunc: func(_ context.Context, id string) error {
			deleted = append(deleted, id)
			return nil
		},
	}
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "0.1.0",
		NewClient: func(baseURL, _ string) qumulo.ClientInterface {
			dialled = append(dialled, baseURL)
			return m
		}})
	require.NoError(t, err)

	_, err = NewControllerServer(d).DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: testServer + "/4242", Secrets: testSecrets()})
	t.Logf("DeleteSnapshot answered %v; dialled %v; deleted %v", err, dialled, deleted)
	require.NoError(t, err)
	assert.Equal(t, []string{"4242"}, deleted, "the array snapshot was not deleted")
	for _, u := range dialled {
		assert.Equal(t, "https://"+testServer+":8000", u, "the earlier form implies the server's default endpoint")
	}
}

// A CreateVolume name containing "#" must be refused with
// InvalidArgument, and nothing created on the array.
func TestDefect_CreateVolumeRefusesANameContainingTheHandleSeparator(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "p46a#b", Secrets: testSecrets(), Parameters: testParams(),
		VolumeCapabilities: mountCapabilities(), CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
	})
	t.Logf("CreateVolume answered %v", err)
	assertCode(t, err, codes.InvalidArgument)
	assert.Empty(t, cluster.directories, "a directory was created for a name the handle cannot carry")
	assert.Empty(t, cluster.quotas)
}

// A handle minted from an apiUrl ending in "//" must round-trip:
// the driver's parse-and-render of the PV's handle equals the handle itself
// (or the apiUrl is refused at CreateVolume).
func TestDefect_HandleFromAnAPIURLWithTrailingSlashesRoundTrips(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()

	for _, apiURL := range []string{"https://192.0.2.10:8000//", "https://192.0.2.10:8000///"} {
		t.Run(apiURL, func(t *testing.T) {
			params := testParams()
			params["apiUrl"] = apiURL
			name := "pvc-slash" + string(rune('a'+strings.Count(apiURL, "/")))
			resp, err := newIntegrationControllerServer(t, server.URL).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name: name, Secrets: testSecrets(), Parameters: params,
				VolumeCapabilities: mountCapabilities(), CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
			})
			if err != nil {
				t.Logf("CreateVolume refused the apiUrl: %v", err)
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
				return
			}
			minted := resp.GetVolume().GetVolumeId()
			parsed, err := parseVolumeID(minted)
			require.NoError(t, err)
			t.Logf("minted %q, rendered back as %q", minted, parsed.String())
			assert.Equal(t, minted, parsed.String(), "the PV's handle and the driver's rendering of it differ")
		})
	}
}
