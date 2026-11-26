// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

const (
	testServer = "qumulo.example.com"
	// allSnapshotsSource is the source-volume filter the snapshot listing
	// tests pass. Unfiltered listing is no longer supported: one controller
	// serves many clusters and cannot enumerate them, so a filter is what
	// tells ListSnapshots which cluster to ask.
	allSnapshotsSource = testServer + volumeIDSeparator + volumeIDSeparator + testStoreRealPath + "/pvc-source"
	testFileID         = "12884901888"
	testGiB            = int64(10 * 1024 * 1024 * 1024)
	// testStoreRealPath is the store path testParams provisions under, so a
	// volume named pvc-abc lives at /csi-volumes/pvc-abc.
	testStoreRealPath = "/csi-volumes"
)

// testHandle is a volume handle for a volume of this name under the store
// path testParams provisions into. The handle names a PATH (volume_id.go), so
// a test that wants the cluster to agree has the mock resolve that same path
// -- see volumeAt and the MockClient default.
func testHandle(name string) string {
	return newVolumeID(testServer, "", path.Join(testStoreRealPath, name)).String()
}

// volumeAt makes the mock answer LookupFileByPath for exactly one path, as a
// directory with the given file ID, and NotFound for anything else. It is the
// path-handle counterpart of resolvesVolume.
func volumeAt(realPath, fileID string) func(context.Context, string) (qumulo.FileInfo, error) {
	return func(_ context.Context, p string) (qumulo.FileInfo, error) {
		if p != realPath {
			return qumulo.FileInfo{}, fakeAPIError(http.StatusNotFound, qumulo.ErrorClassNoSuchEntry, "no such path")
		}
		return qumulo.FileInfo{ID: fileID, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
	}
}

// noRelationships answers a relationship-status listing with none, for a
// test in strict mode (MockClient.Strict) that has to script the
// replication guard's reads.
func noRelationships(context.Context) ([]qumulo.RelationshipStatus, error) { return nil, nil }

// coveredByDefaultExport answers the export listing with
// defaultCoveringExports, MockClient's own default, for a strict-mode test.
func coveredByDefaultExport(context.Context) ([]qumulo.ExportInfo, error) {
	return defaultCoveringExports(), nil
}

// missingVolume makes every LookupFileByPath answer NotFound: the volume is
// gone, which DeleteVolume reads as success and the others as NotFound.
func missingVolume() func(context.Context, string) (qumulo.FileInfo, error) {
	return func(context.Context, string) (qumulo.FileInfo, error) {
		return qumulo.FileInfo{}, fakeAPIError(http.StatusNotFound, qumulo.ErrorClassNoSuchEntry, "no such path")
	}
}

// resolvesVolume makes the mock resolve fileID to the canonical path of a
// volume of that name.
//
// DeleteVolume needs this: it will not delete anything until the file ID
// resolves to a path whose last element is the name the volume ID carries,
// which is what proves the ID still names this volume rather than one the
// cluster has since handed the ID to. A mock with no ResolveFileIDsFunc
// resolves nothing, which DeleteVolume reads as "already gone" -- so a test
// that means to exercise the delete itself has to say where the volume is.
func resolvesVolume(fileID, name string) func(context.Context, []string) (map[string]string, error) {
	return func(context.Context, []string) (map[string]string, error) {
		return map[string]string{fileID: path.Join(testStoreRealPath, name)}, nil
	}
}

func newTestControllerServer(t *testing.T, client *MockClient) *ControllerServer {
	t.Helper()

	// There is no process-wide client any more: the controller only ever gets
	// one from NewClient, per request. Handing back the same mock whatever
	// cluster is named keeps these tests focused on controller behaviour --
	// the tests that care which cluster was reached assert on the arguments.
	d, err := NewDriver(Config{
		Name:      "csi.qumulo.com",
		Version:   "0.1.0",
		NewClient: func(baseURL, token string) qumulo.ClientInterface { return client },
	})
	require.NoError(t, err)

	return NewControllerServer(d)
}

// testSecrets is the credential every controller RPC now has to carry: with the
// fallback client gone, a request without one is refused before any cluster is
// contacted.
func testSecrets() map[string]string {
	return map[string]string{secretKeyToken: "test-token"}
}

func testParams() map[string]string {
	return map[string]string{
		"server":          testServer,
		"storeRealPath":   "/csi-volumes",
		"storeExportPath": "/csi",
	}
}

func mountCapabilities() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
}

// apiError builds the typed error the real client returns for a status code.
func apiError(code int) error {
	return &qumulo.APIError{StatusCode: code, Path: "/test"}
}

// assertCode asserts err is a gRPC status error carrying want.
func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "expected a gRPC status error, got %v", err)
	assert.Equal(t, want, st.Code(), "message: %s", st.Message())
}

// ControllerPublishVolume/ControllerUnpublishVolume are deliberately
// unimplemented (see controller_server.go): an NFS export has no
// attach/detach step for the controller to arbitrate. This pins that
// behavior so a future change can't silently start answering OK for an RPC
// the driver never advertises via ControllerGetCapabilities.
func TestControllerPublishVolume_Unimplemented(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})

	resp, err := srv.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         testHandle("pvc-1"),
		NodeId:           "node-1",
		VolumeCapability: mountCapabilities()[0],
	})

	assertCode(t, err, codes.Unimplemented)
	assert.Nil(t, resp)
}

func TestControllerUnpublishVolume_Unimplemented(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})

	resp, err := srv.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: testHandle("pvc-1"),
		NodeId:   "node-1",
	})

	assertCode(t, err, codes.Unimplemented)
	assert.Nil(t, resp)
}

// Strict: every array call a plain CreateVolume makes is scripted here, so
// one it starts making shows up as a failure.
func TestCreateVolume(t *testing.T) {
	var gotMode string
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			assert.Equal(t, "/csi-volumes", parentPath)
			assert.Equal(t, "pvc-abc", name)
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		// The shared-subdirectory class's export coverage check.
		ListExportsFunc: coveredByDefaultExport,
		// The new directory's mode.
		SetFileAttributesByIDFunc: func(_ context.Context, fileID string, attrs qumulo.FileAttributes) error {
			assert.Equal(t, testFileID, fileID)
			gotMode = attrs.Mode
			return nil
		},
	}

	var gotQuota uint64
	client.CreateQuotaFunc = func(ctx context.Context, fileID string, limitBytes uint64) error {
		assert.Equal(t, testFileID, fileID)
		gotQuota = limitBytes
		return nil
	}

	srv := newTestControllerServer(t, client.Strict(t))

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp.GetVolume())
	assert.Equal(t, testHandle("pvc-abc"), resp.GetVolume().GetVolumeId())
	assert.Equal(t, testGiB, resp.GetVolume().GetCapacityBytes())
	assert.Equal(t, uint64(testGiB), gotQuota)
	assert.NotEmpty(t, gotMode, "the directory's mode was never set")
	for method, want := range map[string]int{
		"CreateDirectory": 1, "CreateQuota": 1, "SetFileAttributesByID": 1, "StartTreeDelete": 0,
	} {
		assert.Equal(t, want, client.Calls(method), "%s calls", method)
	}

	// The node service learns where to mount only through VolumeContext.
	ctxMap := resp.GetVolume().GetVolumeContext()
	assert.Equal(t, testServer, ctxMap["server"])
	assert.Equal(t, "/csi/pvc-abc", ctxMap["exportPath"])
}

func TestCreateVolumeIsIdempotent(t *testing.T) {
	client := &MockClient{
		// A retry after a successful first attempt: the directory is there.
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusConflict)
		},
		ListExportsFunc: coveredByDefaultExport,
		// Adoption re-applies the directory's mode.
		SetFileAttributesByIDFunc: func(context.Context, string, qumulo.FileAttributes) error { return nil },
		LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
			assert.Equal(t, "/csi-volumes/pvc-abc", path)
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) {
			return uint64(testGiB), nil
		},
		CreateQuotaFunc: func(ctx context.Context, fileID string, limitBytes uint64) error {
			t.Error("quota must not be recreated for an existing volume")
			return nil
		},
	}
	srv := newTestControllerServer(t, client.Strict(t))

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	require.NoError(t, err)
	assert.Equal(t, testHandle("pvc-abc"), resp.GetVolume().GetVolumeId())
	assert.Equal(t, testGiB, resp.GetVolume().GetCapacityBytes())
}

func TestCreateVolumeAlreadyExistsWithDifferentCapacity(t *testing.T) {
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusConflict)
		},
		LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) {
			return uint64(testGiB * 2), nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	assertCode(t, err, codes.AlreadyExists)
}

func TestCreateVolumeRollsBackDirectoryWhenQuotaFails(t *testing.T) {
	var deleted string
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		CreateQuotaFunc: func(ctx context.Context, fileID string, limitBytes uint64) error {
			return errors.New("quota service unavailable")
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error {
			deleted = fileID
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	assertCode(t, err, codes.Internal)
	// Without the rollback a retry would find a quota-less directory and
	// fail the capacity-match check forever.
	assert.Equal(t, testFileID, deleted, "directory should have been rolled back")
}

func TestCreateVolumeInvalidRequests(t *testing.T) {
	tests := []struct {
		name     string
		req      *csi.CreateVolumeRequest
		wantCode codes.Code
	}{
		{
			name: "missing name",
			req: &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
				VolumeCapabilities: mountCapabilities(),
				Parameters:         testParams(),
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing capabilities",
			req: &csi.CreateVolumeRequest{
				Secrets:       testSecrets(),
				Name:          "pvc-abc",
				CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
				Parameters:    testParams(),
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "block volume rejected",
			req: &csi.CreateVolumeRequest{
				Secrets:       testSecrets(),
				Name:          "pvc-abc",
				CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
				},
				Parameters: testParams(),
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing storage class parameters",
			req: &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-abc",
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
				VolumeCapabilities: mountCapabilities(),
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing capacity range",
			req: &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-abc",
				VolumeCapabilities: mountCapabilities(),
				Parameters:         testParams(),
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "required exceeds limit",
			req: &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-abc",
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB, LimitBytes: testGiB / 2},
				VolumeCapabilities: mountCapabilities(),
				Parameters:         testParams(),
			},
			wantCode: codes.InvalidArgument,
		},
		{
			// Snapshots restore; a live volume has no consistent point to copy
			// from, so cloning stays unimplemented.
			name: "volume clone is unsupported",
			req: &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-abc",
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
				VolumeCapabilities: mountCapabilities(),
				Parameters:         testParams(),
				VolumeContentSource: &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Volume{
						Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: testHandle("pvc-src")},
					},
				},
			},
			wantCode: codes.Unimplemented,
		},
		{
			name: "malformed snapshot source",
			req: &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-abc",
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
				VolumeCapabilities: mountCapabilities(),
				Parameters:         testParams(),
				VolumeContentSource: &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Snapshot{
						Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "not-an-id"},
					},
				},
			},
			wantCode: codes.NotFound,
		},
		{
			// copy-chunk cannot reach across clusters, and an empty volume
			// that looks restored is the worst possible outcome.
			name: "snapshot from another cluster",
			req: &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-abc",
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
				VolumeCapabilities: mountCapabilities(),
				Parameters:         testParams(),
				VolumeContentSource: &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Snapshot{
						Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "other-cluster.example.com##42"},
					},
				},
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestControllerServer(t, &MockClient{})

			_, err := srv.CreateVolume(context.Background(), tt.req)

			assertCode(t, err, tt.wantCode)
		})
	}
}

// Strict: every array call a plain DeleteVolume makes is scripted here.
func TestDeleteVolume(t *testing.T) {
	var deletedQuota, deletedTree string
	client := &MockClient{
		LookupFileByPathFunc: volumeAt("/csi-volumes/pvc-abc", testFileID),
		// The replication guard: no relationship at or below the volume.
		ListSourceStatusFunc: noRelationships,
		ListTargetStatusFunc: noRelationships,
		// The identity check: no other export at or below the directory.
		ListExportsFunc:        coveredByDefaultExport,
		GetQuotaFunc:           func(context.Context, string) (uint64, error) { return uint64(testGiB), nil },
		ListPoliciesFunc:       func(context.Context) ([]qumulo.PolicyInfo, error) { return nil, nil },
		TreeDeleteFinishedFunc: func(context.Context, string) (bool, error) { return true, nil },
		DeleteQuotaFunc: func(ctx context.Context, fileID string) error {
			deletedQuota = fileID
			return nil
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error {
			deletedTree = fileID
			return nil
		},
	}
	srv := newTestControllerServer(t, client.Strict(t))

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, testFileID, deletedQuota)
	assert.Equal(t, testFileID, deletedTree)
	for method, want := range map[string]int{
		"DeleteQuota": 1, "StartTreeDelete": 1, "DeleteExport": 0, "DeletePolicy": 0,
	} {
		assert.Equal(t, want, client.Calls(method), "%s calls", method)
	}
}

func TestDeleteVolumeIsIdempotent(t *testing.T) {
	tests := []struct {
		name     string
		volumeID string
		client   *MockClient
	}{
		{
			name:     "already deleted on the cluster",
			volumeID: testHandle("pvc-abc"),
			client: &MockClient{
				// The path is found, so the delete is really attempted and
				// the cluster's 404 is what has to be tolerated -- with the
				// path missing the volume reads as already gone and neither
				// call is made.
				LookupFileByPathFunc:   volumeAt("/csi-volumes/pvc-abc", testFileID),
				ListSourceStatusFunc:   noRelationships,
				ListTargetStatusFunc:   noRelationships,
				ListExportsFunc:        coveredByDefaultExport,
				GetQuotaFunc:           func(context.Context, string) (uint64, error) { return uint64(testGiB), nil },
				ListPoliciesFunc:       func(context.Context) ([]qumulo.PolicyInfo, error) { return nil, nil },
				TreeDeleteFinishedFunc: func(context.Context, string) (bool, error) { return true, nil },
				DeleteQuotaFunc: func(ctx context.Context, fileID string) error {
					return apiError(http.StatusNotFound)
				},
				StartTreeDeleteFunc: func(ctx context.Context, fileID string) error {
					return apiError(http.StatusNotFound)
				},
			},
		},
		{
			// An ID this driver never issued names nothing it can delete.
			name:     "unparseable volume ID",
			volumeID: "not-a-qumulo-volume-id",
			client:   &MockClient{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestControllerServer(t, tt.client.Strict(t))

			resp, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: tt.volumeID, Secrets: testSecrets()})

			require.NoError(t, err, "deleting an absent volume must succeed")
			require.NotNil(t, resp)
		})
	}
}

// A handle the driver could not have issued names no volume it can delete,
// so DeleteVolume reports success -- the spec's answer for "already gone".
//
// That is a real trade, and it is logged at Warn for the reason: the volume
// may well still be on the array. It is the right trade only because no such
// handle can exist outside a pre-release lab. Returning an error instead
// would strand the PV, because the CO retries DeleteVolume forever and no
// retry makes an unparseable handle parse.
func TestDeleteVolumeOfAnEarlierEncodingIsAlreadyGone(t *testing.T) {
	for _, handle := range []string{
		// The retired <server>/<fileID>/<name> form, which has no safe
		// mapping. (The two-field <server>#<path> form is decoded now:,
		// TestDeleteVolumeOfATwoFieldHandleDeletesIt.)
		testServer + "/" + testFileID + "/pvc-abc",
	} {
		t.Run(handle, func(t *testing.T) {
			var touched bool
			client := &MockClient{
				DeleteQuotaFunc:     func(context.Context, string) error { touched = true; return nil },
				StartTreeDeleteFunc: func(context.Context, string) error { touched = true; return nil },
			}
			srv := newTestControllerServer(t, client)

			_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
				VolumeId: handle, Secrets: testSecrets(),
			})

			// Refused, not "already gone": the handle is this driver's, so the
			// volume may still be on the array, and OK would drop its PV.
			requireCode(t, err, codes.FailedPrecondition)
			assert.False(t, touched, "nothing on the array may be acted on from a handle that did not parse")
		})
	}
}

// A PV an older build provisioned carries <server>#<path>. DeleteVolume
// used to answer OK for it without touching the array, so Kubernetes dropped
// the PV and the directory and quota leaked. It is decoded now, so the delete
// really happens.
func TestDeleteVolumeOfATwoFieldHandleDeletesIt(t *testing.T) {
	var treeDeleted string
	client := &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY", Path: p}, nil
		},
		StartTreeDeleteFunc: func(_ context.Context, id string) error { treeDeleted = id; return nil },
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: testServer + "#" + testStoreRealPath + "/pvc-abc", Secrets: testSecrets(),
	})

	require.NoError(t, err)
	assert.Equal(t, testFileID, treeDeleted, "the volume's directory is deleted")
}

func TestDeleteVolumeRequiresVolumeID(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{})

	assertCode(t, err, codes.InvalidArgument)
}

func TestDeleteVolumeSurfacesRealFailures(t *testing.T) {
	client := &MockClient{
		ResolveFileIDsFunc: resolvesVolume(testFileID, "pvc-abc"),
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error {
			return apiError(http.StatusInternalServerError)
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	assertCode(t, err, codes.Internal)
}

func TestValidateVolumeCapabilities(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) {
			return uint64(testGiB), nil
		},
	})

	resp, err := srv.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		Secrets:            testSecrets(),
		VolumeId:           testHandle("pvc-abc"),
		VolumeCapabilities: mountCapabilities(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp.GetConfirmed())
	assert.Len(t, resp.GetConfirmed().GetVolumeCapabilities(), 1)
}

func TestValidateVolumeCapabilitiesUnsupportedIsNotAnError(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) {
			return uint64(testGiB), nil
		},
	})

	resp, err := srv.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
	})

	// The RPC itself succeeded; the answer is "no", carried by omitting
	// Confirmed rather than by returning an error.
	require.NoError(t, err)
	assert.Nil(t, resp.GetConfirmed())
	assert.NotEmpty(t, resp.GetMessage())
}

func TestValidateVolumeCapabilitiesNotFound(t *testing.T) {
	tests := []struct {
		name     string
		volumeID string
		client   *MockClient
	}{
		{
			name:     "volume absent on the cluster",
			volumeID: testHandle("pvc-abc"),
			client: &MockClient{
				// The handle names a path, so the directory is what proves
				// the volume exists. This used to be a quota lookup, one step
				// further away from the question.
				LookupFileByPathFunc: missingVolume(),
			},
		},
		{
			name:     "unparseable volume ID",
			volumeID: "not-a-qumulo-volume-id",
			client:   &MockClient{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestControllerServer(t, tt.client)

			_, err := srv.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
				Secrets:            testSecrets(),
				VolumeId:           tt.volumeID,
				VolumeCapabilities: mountCapabilities(),
			})

			assertCode(t, err, codes.NotFound)
		})
	}
}

// Strict: every array call a plain ControllerExpandVolume makes is scripted.
func TestControllerExpandVolume(t *testing.T) {
	var updated uint64
	client := &MockClient{
		// Expand proves the handle's path is still the volume's directory
		// before it acts on the file ID found there.
		LookupFileByPathFunc: volumeAt("/csi-volumes/pvc-abc", testFileID),
		// The replication guard: no relationship covering the volume.
		ListSourceStatusFunc: noRelationships,
		ListTargetStatusFunc: noRelationships,
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) {
			return uint64(testGiB), nil
		},
		UpdateQuotaFunc: func(ctx context.Context, fileID string, limitBytes uint64) error {
			assert.Equal(t, testFileID, fileID)
			updated = limitBytes
			return nil
		},
	}
	srv := newTestControllerServer(t, client.Strict(t))

	resp, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		Secrets:       testSecrets(),
		VolumeId:      testHandle("pvc-abc"),
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB * 2},
	})

	require.NoError(t, err)
	assert.Equal(t, testGiB*2, resp.GetCapacityBytes())
	assert.Equal(t, uint64(testGiB*2), updated)
	assert.Equal(t, 1, client.Calls("UpdateQuota"))
	// An NFS mount has no client-side filesystem to grow.
	assert.False(t, resp.GetNodeExpansionRequired())
}

func TestControllerExpandVolumeIsIdempotent(t *testing.T) {
	client := &MockClient{
		// Expand proves the handle's path is still the volume's directory
		// before it acts on the file ID found there.
		LookupFileByPathFunc: volumeAt("/csi-volumes/pvc-abc", testFileID),
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) {
			return uint64(testGiB * 2), nil
		},
		UpdateQuotaFunc: func(ctx context.Context, fileID string, limitBytes uint64) error {
			t.Error("quota must not be rewritten when it is already large enough")
			return nil
		},
	}
	srv := newTestControllerServer(t, client.Strict(t))

	// A repeat of an expansion whose response was lost.
	resp, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		Secrets:       testSecrets(),
		VolumeId:      testHandle("pvc-abc"),
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB * 2},
	})

	require.NoError(t, err)
	assert.Equal(t, testGiB*2, resp.GetCapacityBytes())
}

func TestControllerExpandVolumeRejectsShrink(t *testing.T) {
	client := &MockClient{
		// Expand now proves the handle still names the volume before it
		// acts on the file ID.
		ResolveFileIDsFunc: resolvesVolume(testFileID, "pvc-abc"),
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) {
			return uint64(testGiB * 4), nil
		},
		UpdateQuotaFunc: func(ctx context.Context, fileID string, limitBytes uint64) error {
			t.Error("a shrink must never reach the cluster")
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	// Shrinking is not an error in CSI -- the volume already satisfies the
	// request -- but it must not reduce the quota.
	resp, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		Secrets:       testSecrets(),
		VolumeId:      testHandle("pvc-abc"),
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
	})

	require.NoError(t, err)
	assert.Equal(t, testGiB*4, resp.GetCapacityBytes(), "capacity must not shrink")
}

func TestControllerExpandVolumeInvalidRequests(t *testing.T) {
	tests := []struct {
		name     string
		req      *csi.ControllerExpandVolumeRequest
		wantCode codes.Code
	}{
		{
			name:     "missing volume ID",
			req:      &csi.ControllerExpandVolumeRequest{CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB}},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "unparseable volume ID",
			req:      &csi.ControllerExpandVolumeRequest{VolumeId: "bogus", CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB}},
			wantCode: codes.NotFound,
		},
		{
			name:     "missing capacity range",
			req:      &csi.ControllerExpandVolumeRequest{VolumeId: testHandle("pvc-abc"), Secrets: testSecrets()},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestControllerServer(t, &MockClient{})

			_, err := srv.ControllerExpandVolume(context.Background(), tt.req)

			assertCode(t, err, tt.wantCode)
		})
	}
}

func TestCreateSnapshot(t *testing.T) {
	client := &MockClient{
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			assert.Equal(t, testFileID, fileID)
			assert.Equal(t, "snap-1", name)
			return qumulo.SnapshotInfo{ID: "42", Name: name, SourceFileID: fileID}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	sourceID := testHandle("pvc-abc")
	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: sourceID,
	})

	require.NoError(t, err)
	assert.Equal(t, testServer+"##42", resp.GetSnapshot().GetSnapshotId())
	assert.Equal(t, sourceID, resp.GetSnapshot().GetSourceVolumeId())
	assert.True(t, resp.GetSnapshot().GetReadyToUse())
}

func TestCreateSnapshotIsIdempotent(t *testing.T) {
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			// Qumulo stores "<id>_<suffix>", never the bare suffix.
			return []qumulo.SnapshotInfo{
				{ID: "42", Name: "42_snap-1", SourceFileID: testFileID},
			}, nil
		},
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			t.Error("an existing snapshot must not be recreated")
			return qumulo.SnapshotInfo{}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, testServer+"##42", resp.GetSnapshot().GetSnapshotId())
}

func TestCreateSnapshotNameReusedForDifferentSource(t *testing.T) {
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{
				{ID: "42", Name: "42_snap-1", SourceFileID: "999"},
			}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	assertCode(t, err, codes.AlreadyExists)
}

func TestCreateSnapshotInvalidRequests(t *testing.T) {
	tests := []struct {
		name     string
		req      *csi.CreateSnapshotRequest
		wantCode codes.Code
	}{
		{
			name:     "missing name",
			req:      &csi.CreateSnapshotRequest{SourceVolumeId: testHandle("pvc-abc"), Secrets: testSecrets()},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "missing source volume ID",
			req:      &csi.CreateSnapshotRequest{Name: "snap-1", Secrets: testSecrets()},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "unparseable source volume ID",
			req:      &csi.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: "bogus", Secrets: testSecrets()},
			wantCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestControllerServer(t, &MockClient{})

			_, err := srv.CreateSnapshot(context.Background(), tt.req)

			assertCode(t, err, tt.wantCode)
		})
	}
}

func TestDeleteSnapshotIsIdempotent(t *testing.T) {
	tests := []struct {
		name       string
		snapshotID string
		client     *MockClient
	}{
		{
			name:       "already deleted on the cluster",
			snapshotID: testServer + "##42",
			client: &MockClient{
				DeleteSnapshotFunc: func(ctx context.Context, snapshotID string) error {
					return apiError(http.StatusNotFound)
				},
			},
		},
		{
			name:       "unparseable snapshot ID",
			snapshotID: "not-a-qumulo-snapshot-id",
			client:     &MockClient{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestControllerServer(t, tt.client)

			_, err := srv.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: tt.snapshotID, Secrets: testSecrets()})

			require.NoError(t, err)
		})
	}
}

func TestDeleteSnapshotRequiresID(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})

	_, err := srv.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{})

	assertCode(t, err, codes.InvalidArgument)
}

func TestListSnapshots(t *testing.T) {
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{
				{ID: "42", Name: "42_snap-1", SourceFileID: testFileID},
				{ID: "43", Name: "43_snap-2", SourceFileID: testFileID},
			}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{SourceVolumeId: allSnapshotsSource, Secrets: testSecrets()})

	require.NoError(t, err)
	assert.Len(t, resp.GetEntries(), 2)
	assert.Empty(t, resp.GetNextToken())
}

// Resolution runs on the snapshot-ID route, where the request names no source
// volume and the driver must reconstruct it from the snapshot's file ID.
func TestListSnapshotsResolvesSourceVolumeName(t *testing.T) {
	client := &MockClient{
		GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: "42", Name: "snap-1", SourceFileID: testFileID}, nil
		},
		ResolveFileIDsFunc: func(ctx context.Context, fileIDs []string) (map[string]string, error) {
			assert.Equal(t, []string{testFileID}, fileIDs)
			return map[string]string{testFileID: "/csi-volumes/pvc-abc"}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		Secrets:    testSecrets(),
		SnapshotId: testServer + "##42",
	})

	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	// The CO matches this against its own volume handles, so it has to be
	// the real volume ID rather than one built from the snapshot's name.
	assert.Equal(t, testHandle("pvc-abc"),
		resp.GetEntries()[0].GetSnapshot().GetSourceVolumeId())
}

func TestListSnapshotsWhenSourceNameCannotBeResolved(t *testing.T) {
	client := &MockClient{
		GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: "42", Name: "snap-1", SourceFileID: testFileID}, nil
		},
		ResolveFileIDsFunc: func(ctx context.Context, fileIDs []string) (map[string]string, error) {
			return nil, apiError(http.StatusNotFound)
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		Secrets:    testSecrets(),
		SnapshotId: testServer + "##42",
	})

	// A snapshot whose source directory has been deleted still gets listed;
	// dropping it would hide it from the CO entirely. Its source volume ID is
	// empty rather than approximate: a handle names a path, and with no path
	// to name there is no handle to give. The retired format could fall back
	// to the file ID as a stand-in name, which read as a volume that could
	// never be looked up.
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Empty(t, resp.GetEntries()[0].GetSnapshot().GetSourceVolumeId())
	assert.Equal(t, testServer+"##42", resp.GetEntries()[0].GetSnapshot().GetSnapshotId(),
		"the snapshot's own ID does not depend on its source resolving")
}

// With a source-volume filter the source ID is already known, so the driver
// must not spend a ResolveFileIDs round trip rediscovering it.
func TestListSnapshotsSkipsResolutionWhenFiltered(t *testing.T) {
	var calls int
	var resolved []string

	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{
				{ID: "1", Name: "1_snap-1", SourceFileID: testFileID},
				{ID: "2", Name: "2_snap-2", SourceFileID: testFileID},
				{ID: "3", Name: "3_snap-3", SourceFileID: testFileID},
			}, nil
		},
		ResolveFileIDsFunc: func(ctx context.Context, fileIDs []string) (map[string]string, error) {
			calls++
			resolved = fileIDs

			paths := map[string]string{}
			for _, id := range fileIDs {
				paths[id] = "/csi-volumes/pvc-" + id
			}
			return paths, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{SourceVolumeId: allSnapshotsSource, MaxEntries: 1, Secrets: testSecrets()})

	require.NoError(t, err)
	assert.Zero(t, calls, "a filtered listing already knows the source volume")
	assert.Nil(t, resolved)
}

func TestListSnapshotsFiltersBySourceVolume(t *testing.T) {
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{
				{ID: "42", Name: "snap-1", SourceFileID: testFileID},
				{ID: "43", Name: "snap-2", SourceFileID: "999"},
			}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	sourceID := testHandle("pvc-abc")
	resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{SourceVolumeId: sourceID, Secrets: testSecrets()})

	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Equal(t, testServer+"##42", resp.GetEntries()[0].GetSnapshot().GetSnapshotId())
	assert.Equal(t, sourceID, resp.GetEntries()[0].GetSnapshot().GetSourceVolumeId())
}

func TestListSnapshotsFiltersBySnapshotID(t *testing.T) {
	client := &MockClient{
		GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
			assert.Equal(t, "42", snapshotID)
			return qumulo.SnapshotInfo{ID: "42", Name: "snap-1", SourceFileID: testFileID}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		Secrets:    testSecrets(),
		SnapshotId: testServer + "##42",
	})

	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Equal(t, testServer+"##42", resp.GetEntries()[0].GetSnapshot().GetSnapshotId())
}

// The lookup by ID is how the snapshotter fills restoreSize for an
// imported VolumeSnapshotContent, and the provisioner compares that with the
// claim. Reporting 0 ("unspecified") skipped the check; the snapshot's tree
// size makes it work. A size the array will not report stays 0.
func TestListSnapshotsBySnapshotIDReportsTheSnapshotsSize(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want int64
	}{
		{name: "measured", want: 5 << 30},
		{name: "not measurable", err: apiError(http.StatusForbidden), want: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &MockClient{
				GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
					return qumulo.SnapshotInfo{ID: "42", Name: "snap-1", SourceFileID: testFileID}, nil
				},
				GetTreeUsageFunc: func(ctx context.Context, fileID, snapshotID string) (qumulo.TreeUsage, error) {
					assert.Equal(t, testFileID, fileID)
					assert.Equal(t, "42", snapshotID)
					return qumulo.TreeUsage{Capacity: 5 << 30}, tt.err
				},
			}
			srv := newTestControllerServer(t, client)

			resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
				Secrets:    testSecrets(),
				SnapshotId: testServer + "##42",
			})

			require.NoError(t, err)
			require.Len(t, resp.GetEntries(), 1)
			assert.Equal(t, tt.want, resp.GetEntries()[0].GetSnapshot().GetSizeBytes())
		})
	}
}

func TestListSnapshotsUnknownFiltersReturnEmpty(t *testing.T) {
	tests := []struct {
		name   string
		req    *csi.ListSnapshotsRequest
		client *MockClient
	}{
		{
			name:   "snapshot ID from another driver",
			req:    &csi.ListSnapshotsRequest{SnapshotId: "someone-elses-handle", Secrets: testSecrets()},
			client: &MockClient{},
		},
		{
			// Core's answer for a well-formed ID that names no snapshot. A bare
			// 404 is not this: it is Unavailable.
			name: "snapshot absent on the cluster",
			req:  &csi.ListSnapshotsRequest{SnapshotId: testServer + "##999", Secrets: testSecrets()},
			client: &MockClient{
				GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
					return qumulo.SnapshotInfo{}, &qumulo.APIError{StatusCode: http.StatusNotFound, Class: qumulo.ErrorClassSnapshotNotFound}
				},
			},
		},
		{
			// Core answers a well-formed but absent ID with
			// snapshot_not_found_error and an ID it cannot parse at all with
			// HTTP 400 decode_error. Both name no snapshot, so both are an
			// empty list; the driver used to return Internal for this one,
			// which a CO would retry forever.
			name: "snapshot ID the cluster cannot parse",
			req:  &csi.ListSnapshotsRequest{SnapshotId: testServer + "##999999999999", Secrets: testSecrets()},
			client: &MockClient{
				GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
					// The path matters now: the tolerance is scoped to the
					// lookup this RPC made, so a stub naming some other
					// route models an error from somewhere else. It was
					// /v2/snapshots/ here, a route the client does not use.
					return qumulo.SnapshotInfo{}, &qumulo.APIError{
						StatusCode: http.StatusBadRequest,
						Path:       "/v3/snapshots/999999999999",
						Class:      qumulo.ErrorClassDecode,
					}
				},
			},
		},
		{
			name:   "source volume ID from another driver",
			req:    &csi.ListSnapshotsRequest{SourceVolumeId: "someone-elses-handle", Secrets: testSecrets()},
			client: &MockClient{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestControllerServer(t, tt.client)

			resp, err := srv.ListSnapshots(context.Background(), tt.req)

			// A filter that matches nothing is an empty list, not an error.
			require.NoError(t, err)
			assert.Empty(t, resp.GetEntries())
		})
	}
}

// decode_error is matched by class alone, so ListSnapshots maps every one of
// them to an empty list -- "this snapshot does not exist".
//
// On Core 7.9.2.2 that class is also what a missing JSON field looks like:
// the array answers a PUT without an id with decode_error "field 'id' not
// found" (measured twice on the lab arrays; see
// TestSetExportReadOnlyKeepsTheIDInTheBody). So a decode_error can mean the
// driver sent a malformed request, which is a real failure, and reporting it
// as an absent snapshot tells the CO a snapshot was deleted when it was not.
//
// The tell is the URL: a decode_error raised against some other endpoint did
// not come from the snapshot id this RPC was handed, so the tolerance must
// not cover it.
func TestListSnapshotsDoesNotTreatEveryDecodeErrorAsAnAbsentSnapshot(t *testing.T) {
	client := &MockClient{
		GetSnapshotFunc: func(context.Context, string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{}, &qumulo.APIError{
				StatusCode: http.StatusBadRequest,
				Path:       "/v3/nfs/exports/",
				Class:      qumulo.ErrorClassDecode,
				Msg:        "field 'id' not found",
			}
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SnapshotId: testServer + "##42",
		Secrets:    testSecrets(),
	})

	assertCode(t, err, codes.Internal)
}

// TestListSnapshotsStillReportsARealFailure is the other half of the
// decode_error mapping above: an ID that names nothing is an empty list, but a
// cluster that could not answer is still an error. Reporting an empty list
// here would tell the CO the snapshot had been deleted.
//
// Unavailable rather than Internal: the array answered 5xx, which is
// transient and retried, not a driver fault.
func TestListSnapshotsStillReportsARealFailure(t *testing.T) {
	client := &MockClient{
		GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{}, apiError(http.StatusInternalServerError)
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SnapshotId: testServer + "##42",
		Secrets:    testSecrets(),
	})

	assertCode(t, err, codes.Unavailable)
}

func TestListSnapshotsPaging(t *testing.T) {
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{
				{ID: "1", Name: "snap-1", SourceFileID: testFileID},
				{ID: "2", Name: "snap-2", SourceFileID: testFileID},
				{ID: "3", Name: "snap-3", SourceFileID: testFileID},
			}, nil
		},
	}
	srv := newTestControllerServer(t, client)
	ctx := context.Background()

	first, err := srv.ListSnapshots(ctx, &csi.ListSnapshotsRequest{
		Secrets:        testSecrets(),
		SourceVolumeId: allSnapshotsSource, MaxEntries: 2,
	})
	require.NoError(t, err)
	require.Len(t, first.GetEntries(), 2)
	// The token is the ID of the next page's first snapshot, not an offset
	// into the list, so a snapshot deleted between the pages cannot shift
	// the rest of them past it.
	require.Equal(t, "3", first.GetNextToken())

	second, err := srv.ListSnapshots(ctx, &csi.ListSnapshotsRequest{
		Secrets:        testSecrets(),
		SourceVolumeId: allSnapshotsSource,
		MaxEntries:     2,
		StartingToken:  first.GetNextToken(),
	})
	require.NoError(t, err)
	assert.Len(t, second.GetEntries(), 1)
	assert.Empty(t, second.GetNextToken(), "last page must not advertise another")
}

func TestListSnapshotsInvalidStartingToken(t *testing.T) {
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{{ID: "1", Name: "snap-1", SourceFileID: testFileID}}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	for _, token := range []string{"not-a-number", "-1"} {
		t.Run(token, func(t *testing.T) {
			_, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
				Secrets:        testSecrets(),
				SourceVolumeId: allSnapshotsSource, StartingToken: token,
			})

			assertCode(t, err, codes.Aborted)
		})
	}

	// A token naming a snapshot ID past everything on the cluster is not a
	// malformed token: it is what the CO holds when every snapshot from the
	// anchor on has been deleted since the previous page. The last page is
	// empty, which is the truth, rather than Aborted.
	t.Run("an ID past the end is an empty last page", func(t *testing.T) {
		resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
			Secrets:        testSecrets(),
			SourceVolumeId: allSnapshotsSource, StartingToken: "99",
		})

		require.NoError(t, err)
		assert.Empty(t, resp.GetEntries())
		assert.Empty(t, resp.GetNextToken())
	})
}

// The cluster calls must run under the caller's context, so a cancelled RPC
// stops the work rather than leaving it running against the array.
//
// Descent rather than identity: the driver adds values to the context on the
// way down -- the subject a quota reading is labelled with, for one -- so the
// context the client sees is derived from the caller's, not the same object.
// What has to hold is that cancelling the caller's cancels the client's.
func TestControllerUsesRequestContext(t *testing.T) {
	client := &MockClient{}
	srv := newTestControllerServer(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := srv.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	require.NotNil(t, client.gotCtx)
	require.NoError(t, client.gotCtx.Err(), "the client's context is live while the caller's is")

	cancel()
	assert.Error(t, client.gotCtx.Err(), "cancelling the request must cancel the work it started")
}

// The per-volume quota gauges are joined by the subject the driver puts on
// the request context, so a quota read that reaches the client unlabelled is
// a metric that silently never appears. This asserts the label is actually
// set, on the context the client is handed, for each RPC that reads a quota.
//
// It is the driver half of the join; pkg/metrics tests the other half. The
// two used to meet through the handle's middle field, which stopped carrying
// a file ID and took every per-volume gauge with it.
func TestQuotaReadsAreLabelledWithTheVolumeHandle(t *testing.T) {
	handle := testHandle("pvc-abc")

	tests := []struct {
		name string
		call func(*testing.T, *ControllerServer) error
	}{
		{
			name: "controllerexpandvolume",
			call: func(_ *testing.T, srv *ControllerServer) error {
				_, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
					VolumeId:      handle,
					CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
					Secrets:       testSecrets(),
				})
				return err
			},
		},
		{
			// CreateSnapshot reads the source's usage to fill restoreSize,
			// which is a different quota call (GetQuotaInfo) on a different
			// path through the RPC.
			name: "createsnapshot",
			call: func(_ *testing.T, srv *ControllerServer) error {
				_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
					Name:           "snap-1",
					SourceVolumeId: handle,
					Secrets:        testSecrets(),
				})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotSubject string
			client := &MockClient{
				LookupFileByPathFunc: volumeAt(testStoreRealPath+"/pvc-abc", testFileID),
				GetQuotaFunc: func(ctx context.Context, _ string) (uint64, error) {
					gotSubject = qumulo.SubjectFrom(ctx)
					return uint64(testGiB), nil
				},
				CreateSnapshotFunc: func(_ context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
					return qumulo.SnapshotInfo{ID: "42", Name: name, SourceFileID: fileID}, nil
				},
			}
			srv := newTestControllerServer(t, client)

			require.NoError(t, tt.call(t, srv))
			assert.Equal(t, handle, gotSubject,
				"the quota read reached the client unlabelled, so its gauge would never be exported")
		})
	}
}

func TestCreateSnapshotMatchesClusterRewrittenName(t *testing.T) {
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			// Qumulo prepends the snapshot ID to the requested suffix, so
			// the stored name never equals what CreateSnapshot asked for.
			// An equality check here would miss the retry and create a
			// duplicate snapshot on every attempt.
			return []qumulo.SnapshotInfo{
				{ID: "42", Name: "42_snap-1", SourceFileID: testFileID},
			}, nil
		},
		CreateSnapshotFunc: func(ctx context.Context, fileID, nameSuffix string) (qumulo.SnapshotInfo, error) {
			t.Error("an existing snapshot must not be recreated")
			return qumulo.SnapshotInfo{}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, testServer+"##42", resp.GetSnapshot().GetSnapshotId())
}

func TestCreateSnapshotIgnoresInDeleteSnapshots(t *testing.T) {
	var created bool
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{
				{ID: "42", Name: "42_snap-1", SourceFileID: testFileID, InDelete: true},
			}, nil
		},
		CreateSnapshotFunc: func(ctx context.Context, fileID, nameSuffix string) (qumulo.SnapshotInfo, error) {
			created = true
			return qumulo.SnapshotInfo{ID: "43", Name: "43_snap-1", SourceFileID: fileID}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	// Adopting a snapshot the cluster is deleting would hand the CO one
	// that is about to vanish.
	require.NoError(t, err)
	assert.True(t, created, "a snapshot being deleted must not be adopted")
	assert.Equal(t, testServer+"##43", resp.GetSnapshot().GetSnapshotId())
}

func TestCreateSnapshotPassesNameAsSuffix(t *testing.T) {
	var gotSuffix string
	client := &MockClient{
		CreateSnapshotFunc: func(ctx context.Context, fileID, nameSuffix string) (qumulo.SnapshotInfo, error) {
			gotSuffix = nameSuffix
			return qumulo.SnapshotInfo{ID: "42", Name: "42_" + nameSuffix, SourceFileID: fileID}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, "snap-1", gotSuffix)
}

func TestDeleteVolumeWaitsForTreeDelete(t *testing.T) {
	var polls int
	client := &MockClient{
		ResolveFileIDsFunc: resolvesVolume(testFileID, "pvc-abc"),
		TreeDeleteFinishedFunc: func(ctx context.Context, fileID string) (bool, error) {
			polls++
			return polls >= 3, nil
		},
	}
	srv := newTestControllerServer(t, client)
	srv.treeDeletePollWait = time.Millisecond

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, 3, polls, "should poll until the job reports finished")
}

func TestDeleteVolumeAbortsWhileTreeDeleteRuns(t *testing.T) {
	client := &MockClient{
		ResolveFileIDsFunc: resolvesVolume(testFileID, "pvc-abc"),
		TreeDeleteFinishedFunc: func(ctx context.Context, fileID string) (bool, error) {
			return false, nil // never finishes
		},
	}
	srv := newTestControllerServer(t, client)
	// Exercise the timeout path without waiting out the real budget.
	srv.treeDeleteBudget = 20 * time.Millisecond
	srv.treeDeletePollWait = 5 * time.Millisecond

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	// Reporting success here would let the CO drop the PV while the tree
	// still exists; Aborted asks it to call DeleteVolume again.
	assertCode(t, err, codes.Aborted)
}

// The tree-delete status was polled every 500 ms for the whole 10 s
// budget -- twenty calls per DeleteVolume of a large tree, and again on
// every retry. The wait now doubles from the first one to ten times it, and
// the last check is made at the deadline, so the budget is still used whole.
func TestDeleteVolumeBacksOffWhilePollingATreeDelete(t *testing.T) {
	var polls atomic.Int32
	client := &MockClient{
		ResolveFileIDsFunc: resolvesVolume(testFileID, "pvc-abc"),
		TreeDeleteFinishedFunc: func(ctx context.Context, fileID string) (bool, error) {
			polls.Add(1)
			return false, nil
		},
	}
	srv := newTestControllerServer(t, client)
	srv.treeDeleteBudget = time.Second
	srv.treeDeletePollWait = 10 * time.Millisecond

	start := time.Now()
	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})
	assertCode(t, err, codes.Aborted)
	// 0, 10, 30, 70, 150 ms, then every 100 ms to the deadline: about 13.
	// A fixed 10 ms poll made about 100.
	assert.LessOrEqual(t, polls.Load(), int32(16), "the status checks back off")
	assert.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond, "and the whole budget is still waited out")
}

func TestDeleteVolumeSurfacesPollFailures(t *testing.T) {
	client := &MockClient{
		ResolveFileIDsFunc: resolvesVolume(testFileID, "pvc-abc"),
		TreeDeleteFinishedFunc: func(ctx context.Context, fileID string) (bool, error) {
			return false, apiError(http.StatusInternalServerError)
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	assertCode(t, err, codes.Internal)
}

func TestCreateVolumeAdoptsWithoutAConflictCode(t *testing.T) {
	// Qumulo documents no error codes, so idempotency must not depend on
	// the cluster answering 409 for a name collision.
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusBadRequest)
		},
		LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) {
			return uint64(testGiB), nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	require.NoError(t, err)
	assert.Equal(t, testHandle("pvc-abc"), resp.GetVolume().GetVolumeId())
}

func TestCreateVolumeSurfacesCreateErrorWhenNothingExists(t *testing.T) {
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusForbidden)
		},
		LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusNotFound)
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	// Nothing to adopt, so the original failure is what the CO should see.
	assertCode(t, err, codes.Internal)
	assert.Contains(t, status.Convert(err).Message(), "403")
}

// A StorageClass can name a storeRealPath nobody ever created on the
// cluster. That is an operator precondition, not a driver fault or a name
// collision -- adopting cannot help (there is nothing to adopt under a
// parent that does not exist), so this must be told apart from every other
// CreateDirectory failure and given its own status rather than falling into
// the generic Internal the "nothing to adopt" path returns.
func TestCreateVolumeStoreRootMissing(t *testing.T) {
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusNotFound)
		},
		// Set so the test stays meaningful even without the fix: nothing to
		// adopt either, so the pre-fix code would fall to codes.Internal
		// rather than something that happens to look like success.
		LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusNotFound)
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, status.Convert(err).Message(), "/csi-volumes")
}

// VolumeContext is the only channel from the controller to the node, so the
// protocol has to survive it -- and must be absent, not empty, when unset.
func TestCreateVolumeStampsProtocolIntoVolumeContext(t *testing.T) {
	tests := []struct {
		name        string
		protocol    string
		wantPresent bool
	}{
		{name: "protocol set", protocol: "nfs4.1", wantPresent: true},
		{name: "protocol unset", protocol: "", wantPresent: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestControllerServer(t, &MockClient{
				CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{ID: "12884901888", Path: parentPath + "/" + name, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
				},
			})

			params := map[string]string{
				"server":          testServer,
				"storeRealPath":   "/csi-volumes",
				"storeExportPath": "/csi",
			}
			if tt.protocol != "" {
				params["protocol"] = tt.protocol
			}

			resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-protocol",
				VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
				CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
				Parameters:         params,
			})

			require.NoError(t, err)
			volumeContext := resp.GetVolume().GetVolumeContext()

			if !tt.wantPresent {
				assert.NotContains(t, volumeContext, volumeContextProtocol,
					"an unset protocol must leave the key absent rather than empty")
				return
			}
			assert.Equal(t, tt.protocol, volumeContext[volumeContextProtocol])
		})
	}
}

// The parameters csi-provisioner injects must not fail a real provision.
func TestCreateVolumeAcceptsCOInjectedParameters(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	})

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-injected",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":                           testServer,
			"storeRealPath":                    "/csi-volumes",
			"storeExportPath":                  "/csi",
			"csi.storage.k8s.io/pvc/name":      "my-claim",
			"csi.storage.k8s.io/pvc/namespace": "default",
			"csi.storage.k8s.io/pv/name":       "pvc-injected",
		},
	})

	require.NoError(t, err)
	assert.NotEmpty(t, resp.GetVolume().GetVolumeId())
}

// A class asking for something the driver cannot do must fail at provision
// time rather than producing a volume that quietly lacks it.
func TestCreateVolumeRejectsUnsupportedParameters(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{name: "ad domain that is a path", key: "adDomain", value: "../escape", wantErr: "not a path"},
		{name: "malformed snapshot schedule", key: "snapshotSchedule", value: "every-other-tuesday", wantErr: "unrecognized snapshot schedule"},
		{name: "unknown key", key: "totalNonsense", value: "xyzzy", wantErr: "unknown StorageClass parameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			srv := newTestControllerServer(t, &MockClient{
				CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
					called = true
					return qumulo.FileInfo{ID: "1", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
				},
			})

			resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-rejected",
				VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
				CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
				Parameters: map[string]string{
					"server":          testServer,
					"storeRealPath":   "/csi-volumes",
					"storeExportPath": "/csi",
					tt.key:            tt.value,
				},
			})

			require.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.False(t, called, "validation must reject before any cluster call")
		})
	}
}

// An export that cannot be created must not leave a volume behind: the next
// retry would otherwise find a directory and quota it considers complete, and
// never create the export.
func TestCreateVolumeRollsBackWhenExportFails(t *testing.T) {
	var quotaDeleted, treeDeleted bool
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) {
			return nil, nil
		},
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			return qumulo.ExportInfo{}, errors.New("cluster said no")
		},
		DeleteQuotaFunc: func(ctx context.Context, fileID string) error {
			quotaDeleted = true
			return nil
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error {
			treeDeleted = true
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-rollback",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":            testServer,
			"storeRealPath":     "/csi-volumes",
			"storeExportPath":   "/csi",
			"provisioningModel": "dedicated-export", "allowAnyClient": "true",
		},
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.True(t, quotaDeleted, "the quota must be rolled back")
	assert.True(t, treeDeleted, "the directory must be rolled back")
}

// An export_path taken by an export serving some *other* directory is a
// collision the administrator has to resolve, not something to adopt.
func TestCreateVolumeExportPathTakenByForeignExport(t *testing.T) {
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) {
			// Nothing serves this volume's directory...
			return []qumulo.ExportInfo{{ID: "7", ExportPath: "/csi/pvc-collide", FSPath: "/elsewhere"}}, nil
		},
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			// But the export path is already in use.
			return qumulo.ExportInfo{}, &qumulo.APIError{
				StatusCode: 409, Class: qumulo.ErrorClassNFSExportAlreadyExists,
			}
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-collide",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":            testServer,
			"storeRealPath":     "/csi-volumes",
			"storeExportPath":   "/csi",
			"provisioningModel": "dedicated-export", "allowAnyClient": "true",
		},
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Contains(t, err.Error(), "already in use")
}

// A delete whose file ID no longer resolves must not scan the export table:
// the directory is gone, so nothing identifies its export, and a repeated
// delete has to stay cheap and successful.
// A handle whose path is not there names nothing to delete, which the CSI
// spec calls success.
//
// This replaces the file-ID reuse guard the retired handle needed. That
// hazard was: a tree delete outlasts its budget, DeleteVolume returns
// Aborted, the CO retries -- and between the two the cluster hands the file
// ID the handle carried to a directory created since, so the retry
// tree-deletes whatever now holds it. A path-identified handle cannot suffer
// it, because the handle names the directory rather than an identifier the
// cluster is free to reuse.
func TestDeleteVolumeOfAMissingPathIsSuccessAndTouchesNothing(t *testing.T) {
	var deletedQuota, deletedTree, deletedExport string
	client := &MockClient{
		LookupFileByPathFunc: missingVolume(),
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) {
			return []qumulo.ExportInfo{
				{ID: "9", ExportPath: "/pvc-somebody-else", FSPath: testStoreRealPath + "/pvc-somebody-else"},
			}, nil
		},
		DeleteExportFunc:       func(ctx context.Context, id string) error { deletedExport = id; return nil },
		DeleteQuotaFunc:        func(ctx context.Context, fileID string) error { deletedQuota = fileID; return nil },
		StartTreeDeleteFunc:    func(ctx context.Context, fileID string) error { deletedTree = fileID; return nil },
		TreeDeleteFinishedFunc: func(ctx context.Context, fileID string) (bool, error) { return true, nil },
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err, "the volume is gone, which the spec calls success")
	assert.Empty(t, deletedTree, "must not tree-delete anything for a path that is not there")
	assert.Empty(t, deletedQuota, "must not remove a quota for a path that is not there")
	assert.Empty(t, deletedExport, "must not remove an export for a path that is not there")
}

// A path that exists but is not a directory was never provisioned by this
// driver -- CreateVolume makes directories -- so nothing is deleted.
func TestDeleteVolumeRefusesAPathThatIsNotADirectory(t *testing.T) {
	var deletedTree string
	client := &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "777", Path: p, Type: "FS_FILE_TYPE_FILE"}, nil
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error { deletedTree = fileID; return nil },
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Empty(t, deletedTree, "a path that is not a directory must not reach StartTreeDelete")
}

// The filesystem root can never be a volume. It is now refused at parse, so
// no cluster is contacted at all -- stronger than the basename comparison the
// retired handle relied on.
func TestDeleteVolumeRefusesTheFilesystemRoot(t *testing.T) {
	var deletedTree string
	var lookedUp bool
	client := &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			lookedUp = true
			return qumulo.FileInfo{ID: "1", Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error { deletedTree = fileID; return nil },
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testServer + volumeIDSeparator + "/",
	})

	requireCode(t, err, codes.FailedPrecondition)
	assert.False(t, lookedUp, "the root is refused at parse, before any cluster call")
	assert.Empty(t, deletedTree, "must never tree-delete the filesystem root")
}

// A volume already gone has its own staging sibling looked for -- a restore
// whose claim was deleted before it committed leaves only that --
// and with none there, nothing is removed. This test used to assert the
// sibling was not even looked up, which is the staging leak itself: the
// staging path is derived from the handle's own path, the exact
// `.csi-restoring-<name>` beside it, so no unverified file ID is involved.
func TestDeleteVolumeRemovesNothingBesideAMissingPathWithNoStaging(t *testing.T) {
	var lookedUp []string
	var deletedTree string
	client := &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			lookedUp = append(lookedUp, p)
			return qumulo.FileInfo{}, fakeAPIError(http.StatusNotFound, qumulo.ErrorClassNoSuchEntry, "no such path")
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error { deletedTree = fileID; return nil },
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, []string{testStoreRealPath + "/pvc-abc", testStoreRealPath + "/.csi-restoring-pvc-abc"}, lookedUp,
		"only the volume and its own staging sibling are looked up")
	assert.Empty(t, deletedTree)
}

// A cluster that cannot answer is not permission to delete, and not proof
// there is nothing to delete either. This is the one resolution outcome that
// must surface as an error rather than as success.
func TestDeleteVolumeSurfacesAResolveFailure(t *testing.T) {
	var deletedTree string
	client := &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusInternalServerError)
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error { deletedTree = fileID; return nil },
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	assertCode(t, err, codes.Internal)
	assert.Empty(t, deletedTree, "an unreadable cluster must not be treated as a licence to delete")
}

// The snapshot policy is keyed on the volume's name, not its file ID, so it is
// swept whatever the ID resolved to -- and the case where the directory is
// already gone is exactly the case where a leftover policy would otherwise
// keep firing on a schedule against nothing.
func TestDeleteVolumeStillRemovesThePolicyWhenTheDirectoryIsGone(t *testing.T) {
	var deletedPolicy string
	client := &MockClient{
		ResolveFileIDsFunc: func(ctx context.Context, ids []string) (map[string]string, error) {
			return map[string]string{}, nil // already gone
		},
		ListPoliciesFunc: func(ctx context.Context) ([]qumulo.PolicyInfo, error) {
			return []qumulo.PolicyInfo{{ID: "77", Name: "csi-pvc-abc", SourceFileID: testFileID}}, nil
		},
		DeletePolicyFunc:       func(ctx context.Context, id string) error { deletedPolicy = id; return nil },
		TreeDeleteFinishedFunc: func(ctx context.Context, fileID string) (bool, error) { return true, nil },
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, "77", deletedPolicy, "a name-keyed policy is swept even when the directory is gone")
}

// One resolution serves the whole RPC. The export lookup and the staging
// sweep each used to resolve the volume again, which was both wasteful and
// the reason two call sites could disagree about what the handle meant.
func TestDeleteVolumeResolvesTheVolumeOnce(t *testing.T) {
	var lookups int
	client := &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			if p == testStoreRealPath+"/pvc-abc" {
				lookups++
			}
			return qumulo.FileInfo{ID: testFileID, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) { return nil, nil },
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, 1, lookups, "the volume's path should be resolved once per DeleteVolume")
}

func TestDeleteVolumeSkipsExportLookupWhenThePathIsGone(t *testing.T) {
	listed := false
	client := &MockClient{
		LookupFileByPathFunc: missingVolume(),
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) {
			listed = true
			return nil, nil
		},
		TreeDeleteFinishedFunc: func(ctx context.Context, fileID string) (bool, error) {
			return true, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-gone"),
	})

	require.NoError(t, err)
	assert.False(t, listed, "a path that is not there should not trigger an export listing")
}

// Suffix matching once adopted a snapshot whose stored name merely ended with
// the requested name -- an administrator's snapshot, or once snapshot
// policies exist, a scheduled one. Anchoring on "<id>_<name>" makes an
// accidental match impossible, because the prefix is the snapshot's own ID.
func TestCreateSnapshotDoesNotAdoptASuffixMatch(t *testing.T) {
	var created bool
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{
				// Ends with "snap-1", but is not "<id>_snap-1".
				{ID: "42", Name: "42_nightly-snap-1", SourceFileID: testFileID},
				{ID: "43", Name: "43_prefixed-snap-1", SourceFileID: testFileID},
				// A snapshot of a different volume that happens to match the
				// exact form must not be adopted either.
				{ID: "44", Name: "44_snap-1", SourceFileID: "999"},
			}, nil
		},
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			created = true
			return qumulo.SnapshotInfo{ID: "50", Name: "50_" + name, SourceFileID: fileID}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	// The exact-form snapshot on file 999 belongs to another volume, so this
	// is the name-reuse case rather than an adoption.
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.False(t, created, "no snapshot should be created for a name already used elsewhere")
	assert.Nil(t, resp)
}

// A scheduled snapshot is not the driver's to adopt: handing the CO a handle to
// it means deleting the VolumeSnapshot destroys an administrator's backup.
func TestCreateSnapshotIgnoresPolicyCreatedSnapshots(t *testing.T) {
	var createdFrom string
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{
				// Same source volume, and the exact name form -- adopted were
				// it not for the policy ID.
				{ID: "42", Name: "42_snap-1", SourceFileID: testFileID, PolicyID: "7"},
			}, nil
		},
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			createdFrom = fileID
			return qumulo.SnapshotInfo{ID: "50", Name: "50_" + name, SourceFileID: fileID}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, testFileID, createdFrom, "a fresh snapshot must be created instead")
	assert.Equal(t, testServer+"##50", resp.GetSnapshot().GetSnapshotId())
}

// ListSnapshots must not report snapshots the plugin does not manage: with a
// retention policy their number is unbounded, and every one is something the
// CO could try to delete but never created.
func TestListSnapshotsExcludesPolicyCreatedSnapshots(t *testing.T) {
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{
				{ID: "42", Name: "42_snap-1", SourceFileID: testFileID},
				{ID: "43", Name: "43_hourly", SourceFileID: testFileID, PolicyID: "7"},
				{ID: "44", Name: "44_hourly", SourceFileID: testFileID, PolicyID: "7"},
				// A zero policy ID means "none": a numeric field rendered as 0
				// must not hide a snapshot the driver owns.
				{ID: "45", Name: "45_snap-2", SourceFileID: testFileID, PolicyID: "0"},
			}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{SourceVolumeId: allSnapshotsSource, Secrets: testSecrets()})

	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 2, "only the driver's own snapshots should be listed")

	ids := []string{
		resp.GetEntries()[0].GetSnapshot().GetSnapshotId(),
		resp.GetEntries()[1].GetSnapshot().GetSnapshotId(),
	}
	assert.Equal(t, []string{testServer + "##42", testServer + "##45"}, ids)
}

// Looking one up by ID takes the same view: an ID that resolves to a scheduled
// snapshot is not a snapshot this plugin knows about.
func TestListSnapshotsByIDExcludesPolicyCreated(t *testing.T) {
	client := &MockClient{
		GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{
				ID: "43", Name: "43_hourly", SourceFileID: testFileID, PolicyID: "7",
			}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		Secrets:    testSecrets(),
		SnapshotId: testServer + "##43",
	})

	require.NoError(t, err)
	assert.Empty(t, resp.GetEntries())
}

// The test that proves the refactor: one controller, two clusters, and a call
// naming one must never touch the other's client.
//
// Asserted through the client factory rather than the fallback, because the
// fallback is exactly the path that would hide a routing bug -- if the request
// were ignored, every call would land on the single configured client and every
// other test would still pass.
func TestCreateVolumeRoutesToTheClusterTheRequestNames(t *testing.T) {
	const (
		clusterA = "cluster-a.example.com"
		clusterB = "cluster-b.example.com"
	)

	var built []string // baseURLs handed to NewClient, in order
	clients := map[string]*MockClient{}

	newMock := func(name string) *MockClient {
		return &MockClient{
			CreateDirectoryFunc: func(ctx context.Context, parentPath, dirName string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			},
			SetFileAttributesByIDFunc: func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error { return nil },
			CreateQuotaFunc:           func(ctx context.Context, fileID string, limit uint64) error { return nil },
			// This class exports and stores at the same path, which the
			// default table does not cover.
			ListExportsFunc: exportsCovering("/csi/volumes", "/csi/volumes"),
		}
	}
	clients[clusterA] = newMock(clusterA)
	clients[clusterB] = newMock(clusterB)

	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		// Deliberately no Server and no Client: a multi-cluster controller has
		// neither, and this asserts NewDriver no longer demands them.
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			built = append(built, baseURL+"|"+token)
			for host, c := range clients {
				if strings.Contains(baseURL, host) {
					return c
				}
			}
			t.Fatalf("no fake client for %q", baseURL)
			return nil
		},
	})
	require.NoError(t, err)
	srv := NewControllerServer(d)

	provision := func(server, token string) error {
		_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "pvc-" + server,
			VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
			Parameters: map[string]string{
				"server":          server,
				"storeRealPath":   "/csi/volumes",
				"storeExportPath": "/csi/volumes",
			},
			Secrets: map[string]string{"token": token},
		})
		return err
	}

	require.NoError(t, provision(clusterA, "token-a"))
	require.NoError(t, provision(clusterB, "token-b"))

	require.Len(t, built, 2)
	assert.Equal(t, "https://"+clusterA+":8000|token-a", built[0],
		"cluster A's call must use A's endpoint and A's credential")
	assert.Equal(t, "https://"+clusterB+":8000|token-b", built[1],
		"cluster B's call must use B's endpoint and B's credential")
}

// A pre-refactor volume: its StorageClass carries no secret reference, so the
// request arrives with none. The configured client is the fallback that keeps
// such volumes deletable after an upgrade.
func TestDeleteVolumeFallsBackToTheConfiguredClient(t *testing.T) {
	deleted := false
	mock := &MockClient{
		DeleteQuotaFunc:        func(ctx context.Context, fileID string) error { return nil },
		StartTreeDeleteFunc:    func(ctx context.Context, fileID string) error { deleted = true; return nil },
		TreeDeleteFinishedFunc: func(ctx context.Context, fileID string) (bool, error) { return true, nil },
		ListExportsFunc:        func(ctx context.Context) ([]qumulo.ExportInfo, error) { return nil, nil },
		ResolveFileIDsFunc: func(ctx context.Context, ids []string) (map[string]string, error) {
			return map[string]string{"12884901888": "/csi/volumes/pvc-old"}, nil
		},
	}
	srv := newTestControllerServer(t, mock)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-old"),
	})

	require.NoError(t, err)
	assert.True(t, deleted, "an upgraded controller must still delete volumes provisioned before the refactor")
}
func TestCreateVolumeSetsDirectoryMode(t *testing.T) {
	tests := []struct {
		name     string
		param    string
		wantMode string
		wantCall bool
	}{
		{name: "default", param: "", wantMode: "0777", wantCall: true},
		{name: "explicit narrower mode", param: "0770", wantMode: "0770", wantCall: true},
		{name: "inherit sets no mode", param: "inherit", wantCall: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotFileID string
			var gotAttrs qumulo.FileAttributes
			var calls int
			client := &MockClient{
				CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
				},
				SetFileAttributesByIDFunc: func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
					calls++
					gotFileID, gotAttrs = fileID, attrs
					return nil
				},
			}
			srv := newTestControllerServer(t, client)

			params := map[string]string{
				"server":          testServer,
				"storeRealPath":   "/csi-volumes",
				"storeExportPath": "/csi",
			}
			if tt.param != "" {
				params["mountPermissions"] = tt.param
			}

			_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-mode",
				VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
				CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
				Parameters:         params,
			})

			require.NoError(t, err)
			// One PATCH either way: the mode rides with the NFS owner, and
			// inherit sends the owner alone.
			assert.Equal(t, 1, calls)
			assert.Equal(t, "12884901888", gotFileID, "the attributes are set on the new directory")
			assert.Equal(t, nfsOwnership().OwnerDetails, gotAttrs.OwnerDetails)
			assert.Equal(t, nfsOwnership().GroupDetails, gotAttrs.GroupDetails)
			if !tt.wantCall {
				assert.Empty(t, gotAttrs.Mode, "inherit must leave the cluster's default mode untouched")
				return
			}
			assert.Equal(t, tt.wantMode, gotAttrs.Mode)
		})
	}
}

// A volume that exists but cannot be written to is worse than one that was
// never created: the CO reports success and the workload absorbs the failure.
func TestCreateVolumeRollsBackWhenModeCannotBeSet(t *testing.T) {
	var treeDeleted bool
	var quotaCreated bool
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		SetFileAttributesByIDFunc: func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
			return errors.New("cluster refused")
		},
		CreateQuotaFunc: func(ctx context.Context, fileID string, limitBytes uint64) error {
			quotaCreated = true
			return nil
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error {
			treeDeleted = true
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-mode-fail",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":          testServer,
			"storeRealPath":   "/csi-volumes",
			"storeExportPath": "/csi",
		},
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "setting mode 0777")
	assert.True(t, treeDeleted, "the directory must be rolled back")
	assert.False(t, quotaCreated, "the mode is set before the quota, so no quota should exist yet")
}

// A retry that adopts an existing directory has to re-apply the mode: the
// attempt it is retrying may have died between creating the directory and
// setting it.
// A create that died after the directory and quota but before the export must
// converge on retry, not return a volume that is bound and incomplete.
//
// This is the defect's worst shape. Adoption re-applied the mode and nothing
// else, so the retry returned a PV the CO considered bound, with no export at
// all -- and since allowedClients and tenantID live on the export, with none
// of the access scoping its StorageClass asked for. Nothing anywhere reported
// a problem, because from the CO's side the retry succeeded.
func TestCreateVolumeAdoptionReappliesTheExportAndPolicy(t *testing.T) {
	requireScheduleEnabled(t)

	var gotSpec qumulo.ExportSpec
	var exportCalls, policyCalls int
	client := &MockClient{
		// The directory and quota survive from the attempt that died.
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, &qumulo.APIError{StatusCode: 409, Class: qumulo.ErrorClassEntryExists}
		},
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) { return uint64(testGiB), nil },
		// The export and policy did not.
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) { return nil, nil },
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			exportCalls++
			gotSpec = spec
			return qumulo.ExportInfo{ID: "5"}, nil
		},
		ListPoliciesFunc: func(ctx context.Context) ([]qumulo.PolicyInfo, error) { return nil, nil },
		CreatePolicyFunc: func(ctx context.Context, name, sourceFileID string,
			schedule qumulo.Schedule,
		) (qumulo.PolicyInfo, error) {
			policyCalls++
			return qumulo.PolicyInfo{ID: "77", Name: name}, nil
		},
	}
	params := scheduleParams("daily@02:00,keep=7days")
	params["provisioningModel"] = "dedicated-export"
	params["allowAnyClient"] = "true"
	params["allowedClients"] = "10.0.0.5,192.168.1.0/24"
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-adopted",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         params,
	})

	require.NoError(t, err)
	assert.NotEmpty(t, resp.GetVolume().GetVolumeId())
	assert.Equal(t, 1, exportCalls, "an adopted volume must get the export the dying attempt never made")
	assert.Equal(t, []string{"10.0.0.5", "192.168.1.0/24"}, gotSpec.AllowedClients,
		"the adopted volume's export must carry its class's IP restrictions")
	assert.Equal(t, 1, policyCalls, "an adopted volume must get its snapshot policy")
}

// Adoption must not duplicate what the dying attempt did finish. Both steps
// adopt rather than recreate, which is what makes calling them on a retry
// safe in the first place.
func TestCreateVolumeAdoptionDoesNotDuplicateExportOrPolicy(t *testing.T) {
	requireScheduleEnabled(t)

	var exportCalls, policyCalls int
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, &qumulo.APIError{StatusCode: 409, Class: qumulo.ErrorClassEntryExists}
		},
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) { return uint64(testGiB), nil },
		// Everything already exists: this attempt got all the way through.
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) {
			return []qumulo.ExportInfo{
				{ID: "5", ExportPath: "/csi/pvc-adopted", FSPath: testStoreRealPath + "/pvc-adopted"},
			}, nil
		},
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			exportCalls++
			return qumulo.ExportInfo{}, nil
		},
		ListPoliciesFunc: func(ctx context.Context) ([]qumulo.PolicyInfo, error) {
			return []qumulo.PolicyInfo{{ID: "77", Name: "csi-pvc-adopted", SourceFileID: testFileID}}, nil
		},
		CreatePolicyFunc: func(ctx context.Context, name, sourceFileID string,
			schedule qumulo.Schedule,
		) (qumulo.PolicyInfo, error) {
			policyCalls++
			return qumulo.PolicyInfo{}, nil
		},
	}
	params := scheduleParams("daily@02:00,keep=7days")
	params["provisioningModel"] = "dedicated-export"
	params["allowAnyClient"] = "true"
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-adopted",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         params,
	})

	require.NoError(t, err)
	assert.Zero(t, exportCalls, "an export already serving this volume is adopted, not recreated")
	assert.Zero(t, policyCalls, "a policy already named for this volume is adopted, not duplicated")
}

func TestCreateVolumeAdoptionReappliesMode(t *testing.T) {
	var modeCalls int
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, &qumulo.APIError{StatusCode: 409, Class: qumulo.ErrorClassEntryExists}
		},
		LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Path: path, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) {
			return 1 << 30, nil
		},
		SetFileAttributesByIDFunc: func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
			modeCalls++
			assert.Equal(t, "0777", attrs.Mode)
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-adopted",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":          testServer,
			"storeRealPath":   "/csi-volumes",
			"storeExportPath": "/csi",
		},
	})

	require.NoError(t, err)
	assert.NotEmpty(t, resp.GetVolume().GetVolumeId())
	assert.Equal(t, 1, modeCalls, "an adopted volume must have its mode re-applied")
}

// KI-1: on a cluster with NFSv4 enabled, a dedicated export cannot be created
// beneath an existing export, and the driver must say so in a way an operator
// can act on.
//
// The failure mode this guards is not the refusal itself -- the cluster does
// that -- but how it is reported. The refusal arrives as HTTP 409, so the
// generic conflict branch would treat it as "this export already exists" and
// return AlreadyExists, which the external-provisioner reads as an idempotent
// hit: the PVC would be reported Bound with no export behind it. Nothing else
// in the tree tests the tree-prefix class at all.
func TestCreateVolumeReportsAnAncestorExportAsFailedPrecondition(t *testing.T) {
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		// Nothing already serves this volume's directory, so the driver tries
		// to create the export and the cluster refuses the nesting.
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) { return nil, nil },
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			return qumulo.ExportInfo{}, &qumulo.APIError{
				StatusCode: 409,
				Class:      qumulo.ErrorClassNFSExportTreePrefix,
				Msg:        "an export already exists for a prefix of this path",
			}
		},
	}
	params := testParams()
	params["provisioningModel"] = "dedicated-export"
	params["allowAnyClient"] = "true"
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-nested",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         params,
	})

	require.Error(t, err)
	assert.Nil(t, resp)

	// FailedPrecondition, not AlreadyExists: no retry can succeed, and the fix
	// is on the cluster rather than in the request.
	assert.Equal(t, codes.FailedPrecondition, status.Code(err),
		"a nested-export refusal must not be reported as an idempotent hit")

	// The message has to carry the way out, because the cluster's own text
	// talks about an export the operator never asked for.
	assert.Contains(t, err.Error(), "NFSv4",
		"the message must name the constraint, or it reads as a driver bug")
	assert.Contains(t, err.Error(), "shared-subdirectory",
		"the message must name the workaround")
	assert.Contains(t, err.Error(), "remove the ancestor export",
		"and the other way out, which needs an administrator")

	// SIM-02. Naming the removal without naming its cost is what makes this
	// message dangerous: an operator reads "remove the ancestor export", does
	// it on a live cluster, and every mount beneath that export goes stale
	// mid-write. The warning and the ordering are part of the remedy, not
	// decoration, so they are pinned here rather than left to review.
	assert.Contains(t, err.Error(), "stale file handle",
		"the message must say what removing the export does to live mounts")
	assert.Contains(t, err.Error(), "drain",
		"the message must give the safe order, not just the action")
}

// The neighbouring case, so the ordering of the two 409 branches is pinned
// from both sides: a genuine already-exists on the export path is still
// AlreadyExists, not FailedPrecondition.
func TestCreateVolumeStillReportsATakenExportPathAsAlreadyExists(t *testing.T) {
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) { return nil, nil },
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			return qumulo.ExportInfo{}, &qumulo.APIError{
				StatusCode: 409,
				Class:      qumulo.ErrorClassNFSExportAlreadyExists,
			}
		},
	}
	params := testParams()
	params["provisioningModel"] = "dedicated-export"
	params["allowAnyClient"] = "true"
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-taken",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         params,
	})

	assertCode(t, err, codes.AlreadyExists)
}

// TestCheckSharedExportCoverage covers the export precondition directly,
// including the path arithmetic no end-to-end test exercises.
//
// The behaviour under test: a shared-subdirectory volume that no
// export reaches used to provision, Bind, and then fail to mount forever, so
// the operator saw a healthy PVC and a broken pod with nothing connecting the
// two. Coverage is now a precondition, and these are the cases that decide it.
func TestCheckSharedExportCoverage(t *testing.T) {
	tests := []struct {
		name string
		// storeRealPath and storeExportPath as the class sets them.
		realPath   string
		exportPath string
		model      string
		adDomain   string
		exports    []qumulo.ExportInfo
		wantErr    string
	}{
		{
			name:       "an export at the store path covers every volume under it",
			realPath:   "/csi-volumes",
			exportPath: "/csi",
			exports:    []qumulo.ExportInfo{{ID: "1", ExportPath: "/csi", FSPath: "/csi-volumes"}},
		},
		{
			// An ancestor export reaches the path but its own mapping does
			// not lead back to where the volume is provisioned, so reaching
			// the path is not on its own enough.
			name:       "an ancestor export whose mapping does not lead back",
			realPath:   "/csi-volumes/team-a",
			exportPath: "/exports/csi/team-a",
			exports:    []qumulo.ExportInfo{{ID: "1", ExportPath: "/exports", FSPath: "/"}},
			wantErr:    "out of step",
		},
		{
			// The ancestor case that does work: an identity mapping, so the
			// path under the export is the path on the filesystem.
			name:       "an identity-mapped ancestor export covers it",
			realPath:   "/csi/volumes",
			exportPath: "/csi/volumes",
			exports:    []qumulo.ExportInfo{{ID: "1", ExportPath: "/", FSPath: "/"}},
		},
		{
			// The reported defect: a basePath with no export and no ancestor
			// export.
			name:       "no export reaches the volume",
			realPath:   "/csi-volumes",
			exportPath: "/csi",
			exports:    []qumulo.ExportInfo{{ID: "1", ExportPath: "/other", FSPath: "/other"}},
			wantErr:    "no NFS export",
		},
		{
			name:       "an empty export table reaches nothing",
			realPath:   "/csi-volumes",
			exportPath: "/csi",
			exports:    nil,
			wantErr:    "no NFS export",
		},
		{
			// A string-prefix match would wrongly accept this: "/csi-old" is
			// not an ancestor of "/csi", and neither is the reverse.
			name:       "a sibling whose name shares a prefix does not cover it",
			realPath:   "/csi-volumes",
			exportPath: "/csi",
			exports:    []qumulo.ExportInfo{{ID: "1", ExportPath: "/csi-old", FSPath: "/csi-volumes"}},
			wantErr:    "no NFS export",
		},
		{
			// ARCHITECTURE.md's rule 2 failure, caught here rather than at mount:
			// the export exists and covers the path, but resolves it to a
			// different directory than the one being provisioned.
			name:       "an export that covers the path but maps it elsewhere",
			realPath:   "/csi-volumes",
			exportPath: "/csi",
			exports:    []qumulo.ExportInfo{{ID: "1", ExportPath: "/csi", FSPath: "/somewhere-else"}},
			wantErr:    "out of step",
		},
		{
			// Nested exports resolve by longest match, the way the NFS server
			// itself resolves them. The shorter entry maps this volume
			// somewhere else, so picking it would report a false mismatch.
			name:       "the longest matching export wins",
			realPath:   "/csi-volumes/team-a",
			exportPath: "/csi/team-a",
			exports: []qumulo.ExportInfo{
				{ID: "1", ExportPath: "/csi", FSPath: "/elsewhere"},
				{ID: "2", ExportPath: "/csi/team-a", FSPath: "/csi-volumes/team-a"},
			},
		},
		{
			// The domain directory is inserted into both paths, so coverage
			// has to be judged on the nested path rather than the store root.
			name:       "a domain-nested volume is covered through its domain directory",
			realPath:   "/csi-volumes",
			exportPath: "/csi",
			adDomain:   "corp.example.com",
			exports:    []qumulo.ExportInfo{{ID: "1", ExportPath: "/csi", FSPath: "/csi-volumes"}},
		},
		{
			// dedicated-export makes its own export, so coverage is not a
			// precondition -- an ancestor export is in fact what breaks it.
			name:       "the dedicated model needs no covering export",
			realPath:   "/csi-volumes",
			exportPath: "/csi",
			model:      provisioningModelDedicatedExport,
			exports:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &MockClient{
				ListExportsFunc: func(context.Context) ([]qumulo.ExportInfo, error) {
					return tt.exports, nil
				},
			}
			srv := newTestControllerServer(t, client)

			params := map[string]string{
				"server":          testServer,
				"storeRealPath":   tt.realPath,
				"storeExportPath": tt.exportPath,
			}
			if tt.model != "" {
				params["provisioningModel"] = tt.model
			}
			if tt.adDomain != "" {
				params["adDomain"] = tt.adDomain
			}
			parsed, err := parseVolumeParams(params)
			require.NoError(t, err)

			err = srv.checkSharedExportCoverage(context.Background(), client, parsed, "pvc-abc")

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err),
				"a missing export is fixable on the cluster without editing Kubernetes, "+
					"so the provisioner should keep retrying")
			assert.Contains(t, status.Convert(err).Message(), tt.wantErr)
		})
	}
}

// A shared-model volume that no export covers must be refused before anything
// exists on the cluster. Provisioning first and failing later is what produced
// the Bound-but-unmountable PVC, and it also leaves a directory and a quota
// behind for somebody to find.
func TestCreateVolumeRefusesAnUncoveredSharedVolumeBeforeCreatingAnything(t *testing.T) {
	// Every mutating call fails the test rather than being counted: the point
	// is not how often the cluster was touched but that it was not touched at
	// all, and a fatal here names the call that broke the rule.
	client := &MockClient{
		ListExportsFunc: noCoveringExport,
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			t.Error("a refused provision must create no directory")
			return qumulo.FileInfo{}, nil
		},
		CreateQuotaFunc: func(context.Context, string, uint64) error {
			t.Error("a refused provision must create no quota")
			return nil
		},
		CreateExportFunc: func(context.Context, qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			t.Error("a refused provision must create no export")
			return qumulo.ExportInfo{}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-uncovered",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         testParams(),
	})

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	message := status.Convert(err).Message()
	assert.Contains(t, message, "/csi/pvc-uncovered", "the message must name the path that is unreachable")
	assert.Contains(t, message, "dedicated-export", "the message must name both remedies")
}

// The RWX-on-NFSv3 warning: logged, never a refusal.
//
// NFSv3 mounts here carry nolock, so locking is node-local and two pods on
// different nodes can hold the same file lock. That is worth telling somebody
// about, but it is not grounds for refusing the volume: plenty of multi-node
// workloads never take a file lock, and refusing them would break working
// deployments over a risk the driver cannot evaluate.
func TestCreateVolumeWarnsButAllowsMultiWriterOnNFS3(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		mode     csi.VolumeCapability_AccessMode_Mode
		wantWarn bool
	}{
		{
			name:     "nfs3 multi-node multi-writer warns",
			protocol: protocolNFS3,
			mode:     csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			wantWarn: true,
		},
		{
			name:     "nfs3 multi-node single-writer warns too",
			protocol: protocolNFS3,
			mode:     csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
			wantWarn: true,
		},
		{
			// One node, so node-local locking is the whole world and there is
			// nothing to warn about.
			name:     "nfs3 single-node writer is silent",
			protocol: protocolNFS3,
			mode:     csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			wantWarn: false,
		},
		{
			name:     "nfs4.1 multi-writer is silent",
			protocol: protocolNFS41,
			mode:     csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			wantWarn: false,
		},
		{
			// An unset protocol negotiates its version, so the driver does not
			// know it will land on v3 -- the same reasoning that stops it
			// injecting nolock there.
			name:     "an unset protocol is silent",
			protocol: "",
			mode:     csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			wantWarn: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logged bytes.Buffer
			restore := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(restore) })

			client := &MockClient{
				CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
				},
			}
			srv := newTestControllerServer(t, client)

			params := testParams()
			if tt.protocol != "" {
				params["protocol"] = tt.protocol
			}

			resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Secrets:       testSecrets(),
				Name:          "pvc-locking",
				CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
				Parameters:    params,
				VolumeCapabilities: []*csi.VolumeCapability{{
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: tt.mode},
				}},
			})

			require.NoError(t, err, "the warning must never become a refusal")
			require.NotNil(t, resp.GetVolume())

			if tt.wantWarn {
				assert.Contains(t, logged.String(), "node-local locking",
					"a multi-node nfs3 volume must say so")
				assert.Contains(t, logged.String(), "nfs4.1",
					"the warning must name the protocol that fixes it")
				return
			}
			assert.NotContains(t, logged.String(), "node-local locking")
		})
	}
}

// SizeBytes was hardcoded to zero, which the CSI spec permits but which makes
// VolumeSnapshot.status.restoreSize unusable: a backup tool sizing a restore
// PVC from it asks for a volume of zero bytes, and an operator reading the
// VolumeSnapshot cannot tell a snapshot of an empty volume from one the driver
// simply declined to measure.
//
// The figure is the source volume's quota usage, read before the snapshot is
// taken so it describes the tree the snapshot captured.
func TestCreateSnapshotReportsTheSourceSize(t *testing.T) {
	const used = 300 << 20

	var measuredBefore bool
	client := &MockClient{
		GetQuotaInfoFunc: func(_ context.Context, fileID string) (qumulo.QuotaInfo, error) {
			assert.Equal(t, testFileID, fileID, "the size must come from the source volume")
			return qumulo.QuotaInfo{Limit: 1 << 30, Usage: used, UsageKnown: true}, nil
		},
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			measuredBefore = true
			return qumulo.SnapshotInfo{ID: "42", Name: name, SourceFileID: fileID}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, int64(used), resp.GetSnapshot().GetSizeBytes())
	assert.True(t, measuredBefore)
}

// A cluster that reports no usage leaves the field unset rather than failing
// the snapshot. size_bytes is optional in the spec; the backup path is not.
func TestCreateSnapshotOmitsTheSizeWhenUsageIsUnknown(t *testing.T) {
	client := &MockClient{
		GetQuotaInfoFunc: func(context.Context, string) (qumulo.QuotaInfo, error) {
			// Limit known, usage not -- an older Core release, or a token
			// without the privilege.
			return qumulo.QuotaInfo{Limit: 1 << 30}, nil
		},
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: "42", Name: name, SourceFileID: fileID}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err, "an unmeasurable source must not fail the snapshot")
	assert.Zero(t, resp.GetSnapshot().GetSizeBytes())
}

// Reading the quota is best-effort in the other direction too: an error must
// cost the size field, never the snapshot.
func TestCreateSnapshotSurvivesAnUnreadableQuota(t *testing.T) {
	client := &MockClient{
		GetQuotaInfoFunc: func(context.Context, string) (qumulo.QuotaInfo, error) {
			return qumulo.QuotaInfo{}, errors.New("quota read refused")
		},
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: "42", Name: name, SourceFileID: fileID}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err, "a reporting field must never take the backup path down with it")
	assert.Zero(t, resp.GetSnapshot().GetSizeBytes())
}

// And the path elements beside it. Each of these names would either mint
// a handle the driver cannot decode (so DeleteVolume answers OK and leaks) or
// resolve outside the class's store path -- ".." to its parent, which exists,
// and which DeleteVolume would then tree-delete. Refused before any array call.
func TestCreateVolumeRefusesANameThatIsNotOnePathElement(t *testing.T) {
	for _, name := range []string{"a#b", "a/b", "../escape", "..", ".", "a\x00b"} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			called := false
			srv := newTestControllerServer(t, &MockClient{
				CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
					called = true
					return qumulo.FileInfo{}, nil
				},
				LookupFileByPathFunc: func(_ context.Context, p string) (qumulo.FileInfo, error) {
					called = true
					return qumulo.FileInfo{ID: testFileID, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
				},
			})

			_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               name,
				VolumeCapabilities: mountCapabilities(),
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
				Parameters:         map[string]string{"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi"},
			})

			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.False(t, called, "refused before the array is asked anything")
		})
	}
}

// What Kubernetes actually sends is unaffected.
func TestCreateVolumeAcceptsAKubernetesVolumeName(t *testing.T) {
	require.NoError(t, validateVolumeName("pvc-7b3c1a8e-0f2d-4c5e-9a1b-2d3e4f5a6b7c"))
}

// A direct CSI caller with a hand-built handle for a scheduled snapshot
// must not delete it: ListSnapshots hides policy snapshots so Kubernetes never
// reaches one, and DeleteSnapshot now holds the same line itself. A snapshot
// already gone still answers OK, as CSI requires.
func TestDeleteSnapshotRefusesAScheduledSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name       string
		snapshot   qumulo.SnapshotInfo
		lookupErr  error
		wantCode   codes.Code
		wantDelete bool
	}{
		{name: "a policy snapshot is refused", snapshot: qumulo.SnapshotInfo{ID: "42", PolicyID: "7"}, wantCode: codes.FailedPrecondition},
		{name: "a CSI snapshot is deleted", snapshot: qumulo.SnapshotInfo{ID: "42", Name: "42_snapshot-1"}, wantCode: codes.OK, wantDelete: true},
		{name: "one already gone is OK", lookupErr: fakeAPIError(http.StatusNotFound, "snapshot_not_found_error", "This snapshot no longer exists."), wantCode: codes.OK},
		// A 404 that is not the cluster's "no such snapshot" -- a
		// proxy's, or an unknown route's -- proves nothing is gone.
		{name: "a bare 404 is not proof it is gone", lookupErr: apiError(http.StatusNotFound), wantCode: codes.Unavailable},
		{name: "an unknown route's 404 is not either", lookupErr: fakeAPIError(http.StatusNotFound, "http_not_found_error", "no route"), wantCode: codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deleted := false
			srv := newTestControllerServer(t, &MockClient{
				GetSnapshotFunc: func(context.Context, string) (qumulo.SnapshotInfo, error) {
					return tc.snapshot, tc.lookupErr
				},
				DeleteSnapshotFunc: func(context.Context, string) error {
					deleted = true
					return nil
				},
			})

			_, err := srv.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
				SnapshotId: testServer + "##42",
				Secrets:    testSecrets(),
			})

			assert.Equal(t, tc.wantCode, status.Code(err), status.Convert(err).Message())
			assert.Equal(t, tc.wantDelete, deleted)
		})
	}
}

// ListSnapshots by ID took any 404 as "no such snapshot" and answered
// an empty list. From an endpoint that is not the cluster's API -- an unknown
// route answers http_not_found_error -- a backup tool then concludes the
// snapshot is gone. Only the cluster's snapshot not-found is an empty list.
func TestListSnapshotsByIDAForeign404IsUnavailable(t *testing.T) {
	array := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"module": "server", "error_class": "http_not_found_error", "description": r.Method + " " + r.URL.Path,
		})
	}))
	defer array.Close()
	d := tlsDriverFor(t, array)

	resp, err := NewControllerServer(d).ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SnapshotId: newSnapshotID(testServer, array.URL, "42").String(), Secrets: testSecrets(),
	})

	requireCode(t, err, codes.Unavailable)
	assert.Empty(t, resp.GetEntries())
}

// The client no longer retries the snapshot POST, so a 503 now reaches
// this RPC. It must answer Unavailable -- csi-snapshotter retries that and
// findSnapshotByName adopts whatever landed -- and not Internal, which would
// record a hard error on the VolumeSnapshot for a transient array answer.
func TestCreateSnapshotAnswersUnavailableForA503(t *testing.T) {
	client := &MockClient{
		CreateSnapshotFunc: func(ctx context.Context, fileID, nameSuffix string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{}, &qumulo.APIError{
				StatusCode: http.StatusServiceUnavailable, Class: "http_service_unavailable_error",
			}
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	assert.Equal(t, codes.Unavailable, status.Code(err), "a 503 on the snapshot create answered %v", err)
}

// A build that still retried the snapshot POST can have left two
// snapshots under one name. The lookup adopts one deterministically rather
// than failing, and reports the rest -- it must not delete them, since
// nothing here can tell which one a VolumeSnapshotContent references.
func TestCreateSnapshotAdoptsOneOfTwoSameNamedSnapshots(t *testing.T) {
	var created bool
	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return []qumulo.SnapshotInfo{
				{ID: "55379", Name: "55379_snap-1", SourceFileID: testFileID},
				{ID: "55380", Name: "55380_snap-1", SourceFileID: testFileID},
			}, nil
		},
		CreateSnapshotFunc: func(ctx context.Context, fileID, nameSuffix string) (qumulo.SnapshotInfo, error) {
			created = true
			return qumulo.SnapshotInfo{ID: "55381", Name: "55381_snap-1", SourceFileID: fileID}, nil
		},
		DeleteSnapshotFunc: func(ctx context.Context, id string) error {
			t.Fatalf("the duplicate %s was deleted; duplicates are reported, never removed here", id)
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.False(t, created, "a snapshot already under this name must be adopted, not added to")
	assert.Equal(t, testServer+"##55379", resp.GetSnapshot().GetSnapshotId())
}
