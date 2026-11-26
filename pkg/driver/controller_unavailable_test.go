// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// An array answering 503 -- an upgrade, a failover, a node rebooting --
// is up and briefly unable to serve, and the CO's answer to Unavailable is to
// come back. Internal is not: it is recorded as a failure on the PVC or the
// VolumeSnapshot and raises an Event, and for a snapshot delete it leaves a
// VolumeSnapshotContent the retry loop backs off on rather than clearing.
//
// CreateVolume and CreateSnapshot already mapped 5xx this way. Expand,
// DeleteSnapshot and ListSnapshots did not, so a cluster upgrade made every
// resize and every snapshot delete look like a driver fault.
//
// DeleteVolume is deliberately not here: its own tests pin 500 to Internal,
// because a delete that has already removed part of a volume is not a
// request the CO should simply repeat.

func TestEveryControllerRPCAnswersUnavailableForA503(t *testing.T) {
	unavailable := apiError(http.StatusServiceUnavailable)
	handle := testHandle("pvc-abc")

	for _, tc := range []struct {
		name   string
		client *MockClient
		call   func(*ControllerServer) error
	}{
		{
			name: "ControllerExpandVolume reading the quota",
			client: &MockClient{
				LookupFileByPathFunc: volumeAt(testStoreRealPath+"/pvc-abc", testFileID),
				GetQuotaFunc:         func(ctx context.Context, fileID string) (uint64, error) { return 0, unavailable },
			},
			call: func(srv *ControllerServer) error {
				_, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
					Secrets:       testSecrets(),
					VolumeId:      handle,
					CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB},
				})
				return err
			},
		},
		{
			name: "ControllerExpandVolume writing the quota",
			client: &MockClient{
				LookupFileByPathFunc: volumeAt(testStoreRealPath+"/pvc-abc", testFileID),
				GetQuotaFunc:         func(ctx context.Context, fileID string) (uint64, error) { return uint64(testGiB), nil },
				UpdateQuotaFunc: func(ctx context.Context, fileID string, limitBytes uint64) error {
					return unavailable
				},
			},
			call: func(srv *ControllerServer) error {
				_, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
					Secrets:       testSecrets(),
					VolumeId:      handle,
					CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB},
				})
				return err
			},
		},
		{
			name: "CreateSnapshot",
			client: &MockClient{
				CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
					return qumulo.SnapshotInfo{}, unavailable
				},
			},
			call: func(srv *ControllerServer) error {
				_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
					Secrets:        testSecrets(),
					Name:           "snapshot-snap-1",
					SourceVolumeId: handle,
				})
				return err
			},
		},
		{
			name: "DeleteSnapshot looking the snapshot up",
			client: &MockClient{
				GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
					return qumulo.SnapshotInfo{}, unavailable
				},
			},
			call: func(srv *ControllerServer) error {
				_, err := srv.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
					Secrets:    testSecrets(),
					SnapshotId: testServer + "##42",
				})
				return err
			},
		},
		{
			name: "DeleteSnapshot deleting it",
			client: &MockClient{
				GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
					return qumulo.SnapshotInfo{ID: "42", Name: "42_snapshot-snap-1", SourceFileID: testFileID}, nil
				},
				ResolveFileIDsFunc: resolvesVolume(testFileID, "pvc-abc"),
				DeleteSnapshotFunc: func(ctx context.Context, snapshotID string) error { return unavailable },
			},
			call: func(srv *ControllerServer) error {
				_, err := srv.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
					Secrets:    testSecrets(),
					SnapshotId: testServer + "##42",
				})
				return err
			},
		},
		{
			name: "ListSnapshots by source volume",
			client: &MockClient{
				LookupFileByPathFunc: volumeAt(testStoreRealPath+"/pvc-source", testFileID),
				ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
					return nil, unavailable
				},
			},
			call: func(srv *ControllerServer) error {
				_, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
					Secrets:        testSecrets(),
					SourceVolumeId: allSnapshotsSource,
				})
				return err
			},
		},
		{
			name: "ListSnapshots by snapshot ID",
			client: &MockClient{
				GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
					return qumulo.SnapshotInfo{}, unavailable
				},
			},
			call: func(srv *ControllerServer) error {
				_, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
					Secrets:    testSecrets(),
					SnapshotId: testServer + "##42",
				})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestControllerServer(t, tc.client)

			assertCode(t, tc.call(srv), codes.Unavailable)
		})
	}
}
