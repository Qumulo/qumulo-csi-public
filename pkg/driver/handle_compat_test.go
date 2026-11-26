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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// Handles that carry this driver's server and a path or snapshot ID, but in a
// form this build does not decode: a retired form, and one a later build
// might mint. Neither shows that the volume or snapshot is gone.
var (
	unknownFormVolumeHandles = []string{
		testServer + "/101/pvc-0f9c2a",                                     // <server>/<fileID>/<name>, retired
		testServer + "#https://192.0.2.10:8000#/csi-volumes/pvc-0f9c2a#v2", // a fourth field
	}
	unknownFormSnapshotHandles = []string{
		testServer + "/101/4242", // <server>/<fileID>/<snapshotID>
		testServer + "##4242#v2", // a fourth field
	}
)

// DeleteVolume must refuse (InvalidArgument or FailedPrecondition) a handle it cannot decode, not answer OK.
// It used to answer OK without contacting the array, and the PV went while the volume stayed.
func TestDeleteVolumeDoesNotReportAnUndecodableHandleAsDeleted(t *testing.T) {
	for _, handle := range unknownFormVolumeHandles {
		t.Run(handle, func(t *testing.T) {
			m := &MockClient{LookupFileByPathFunc: missingVolume()}
			_, err := newTestControllerServer(t, m).DeleteVolume(context.Background(),
				&csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})
			t.Logf("DeleteVolume answered %v; array contacted: %v", err, m.gotCtx != nil)
			if err == nil {
				t.Fatalf("DeleteVolume answered OK for %q without being able to read it", handle)
			}
			if c := status.Code(err); c != codes.InvalidArgument && c != codes.FailedPrecondition {
				t.Errorf("DeleteVolume answered %v; want InvalidArgument or FailedPrecondition", c)
			}
		})
	}
}

// DeleteSnapshot must refuse (InvalidArgument or FailedPrecondition) a handle it cannot decode, not answer OK.
// The same for snapshot handles.
func TestDeleteSnapshotDoesNotReportAnUndecodableHandleAsDeleted(t *testing.T) {
	for _, handle := range unknownFormSnapshotHandles {
		t.Run(handle, func(t *testing.T) {
			m := &MockClient{}
			_, err := newTestControllerServer(t, m).DeleteSnapshot(context.Background(),
				&csi.DeleteSnapshotRequest{SnapshotId: handle, Secrets: testSecrets()})
			t.Logf("DeleteSnapshot answered %v; array contacted: %v", err, m.gotCtx != nil)
			if err == nil {
				t.Fatalf("DeleteSnapshot answered OK for %q without being able to read it", handle)
			}
			if c := status.Code(err); c != codes.InvalidArgument && c != codes.FailedPrecondition {
				t.Errorf("DeleteSnapshot answered %v; want InvalidArgument or FailedPrecondition", c)
			}
		})
	}
}

// An ID not in this driver's shape is not this driver's, so there is nothing
// of its to delete and the answer stays OK, as CSI asks -- csi-sanity sends
// exactly these.
func TestDeleteOfAnIDNotInThisDriversShapeStillAnswersOK(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})
	for _, id := range []string{"fake-vol-id", "reallyfakesnapshotid", "none-exist-id", "#/csi/pvc-x", "has space/1"} {
		_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id, Secrets: testSecrets()})
		assert.NoError(t, err, "DeleteVolume %q", id)
		_, err = srv.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: id, Secrets: testSecrets()})
		assert.NoError(t, err, "DeleteSnapshot %q", id)
	}
}

// Handles in this driver's form whose server is an IPv6 literal. No class can
// mint one -- validateServerHost refuses ":" -- but a hand-imported PV can
// carry one, and parseVolumeID reads the server field as given, so the
// readable forms below are deleted on the array like any other.
var (
	ipv6UnknownFormVolumeHandles = []string{
		"fd00::10/101/pvc-0f9c2a",                                     // retired <server>/<fileID>/<name>
		"fd00::10#https://[fd00::10]:8000#/csi-volumes/pvc-0f9c2a#v2", // a fourth field
		"[fd00::10]/101/pvc-0f9c2a",                                   // bracketed
		"fe80::1%eth0##/csi-volumes/pvc-0f9c2a#v2",                    // zoned
		"::ffff:192.0.2.10/101/pvc-0f9c2a",                            // IPv4-mapped
	}
	ipv6UnknownFormSnapshotHandles = []string{
		"fd00::10/101/4242",
		"fd00::10##4242#v2",
		"[fd00::10]##4242#v2",
	}
)

// An IPv6-literal server is outside the host-name alphabet, and its
// unreadable handle used to answer OK -- the leak through a gap in the
// shape test. It is refused like any other handle in this driver's form,
// before the array is contacted.
func TestDeleteRefusesAnUnreadableHandleWithAnIPv6Server(t *testing.T) {
	for _, handle := range ipv6UnknownFormVolumeHandles {
		t.Run("volume "+handle, func(t *testing.T) {
			var dialled int
			m := &MockClient{LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
				dialled++
				return missingVolume()(ctx, p)
			}}
			_, err := newTestControllerServer(t, m).DeleteVolume(context.Background(),
				&csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})
			t.Logf("DeleteVolume answered %v", err)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err), "DeleteVolume %q", handle)
			assert.Zero(t, dialled, "the array was read for a handle this build cannot read")
		})
	}
	for _, handle := range ipv6UnknownFormSnapshotHandles {
		t.Run("snapshot "+handle, func(t *testing.T) {
			var deleted []string
			m := &MockClient{DeleteSnapshotFunc: func(_ context.Context, id string) error {
				deleted = append(deleted, id)
				return nil
			}}
			_, err := newTestControllerServer(t, m).DeleteSnapshot(context.Background(),
				&csi.DeleteSnapshotRequest{SnapshotId: handle, Secrets: testSecrets()})
			t.Logf("DeleteSnapshot answered %v", err)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err), "DeleteSnapshot %q", handle)
			assert.Empty(t, deleted, "a snapshot was deleted for a handle this build cannot read")
		})
	}
}

// The IPv6 handles this build does read keep reaching the array: widening the
// shape test only changes what an unreadable handle answers.
func TestDeleteOfAReadableHandleWithAnIPv6ServerStillReachesTheArray(t *testing.T) {
	for _, handle := range []string{"fd00::10##/csi-volumes/pvc-0f9c2a", "fd00::10#/csi-volumes/pvc-0f9c2a"} {
		var dialled []string
		m := &MockClient{LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			dialled = append(dialled, p)
			return missingVolume()(ctx, p)
		}}
		_, err := newTestControllerServer(t, m).DeleteVolume(context.Background(),
			&csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})
		assert.NoError(t, err, "DeleteVolume %q of a volume already gone", handle)
		assert.Contains(t, dialled, "/csi-volumes/pvc-0f9c2a", "DeleteVolume %q never looked on the array", handle)
	}

	var deleted []string
	m := &MockClient{
		GetSnapshotFunc: func(_ context.Context, id string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: id, Name: id + "_snapshot-1", SourceFileID: "101"}, nil
		},
		DeleteSnapshotFunc: func(_ context.Context, id string) error {
			deleted = append(deleted, id)
			return nil
		},
	}
	for _, handle := range []string{"fd00::10##4242", "fd00::10/4242"} {
		_, err := newTestControllerServer(t, m).DeleteSnapshot(context.Background(),
			&csi.DeleteSnapshotRequest{SnapshotId: handle, Secrets: testSecrets()})
		assert.NoError(t, err, "DeleteSnapshot %q", handle)
	}
	assert.Equal(t, []string{"4242", "4242"}, deleted, "a readable IPv6 snapshot handle was not deleted on the array")
}

// A colon alone does not make an ID this driver's: only an address that
// parses as IPv6 widens the shape, so a URL, a URN or another driver's
// "<pool>:<id>/..." still answers OK.
func TestDeleteOfAColonIDThatIsNotAnIPv6AddressStillAnswersOK(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})
	for _, id := range []string{"https://example.com/vol", "urn:vol/1", "pool:vol-1/snap", "fd00::zz/101/x", "[fd00::10/101/x"} {
		_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id, Secrets: testSecrets()})
		assert.NoError(t, err, "DeleteVolume %q", id)
		_, err = srv.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: id, Secrets: testSecrets()})
		assert.NoError(t, err, "DeleteSnapshot %q", id)
	}
}
