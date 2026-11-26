// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// Found on an OpenShift cluster: an array answering 503
// to the export listing a CreateVolume makes -- the shared-subdirectory
// coverage check, or the dedicated export's lookup -- came back Internal,
// which csi-provisioner records as a final failure on the claim. Every other
// create step already answers a 5xx as Unavailable (internalUnless5xx); these
// two must as well.
func TestCreateVolumeAnswersUnavailableWhenTheExportListingGetsA503(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{"shared subdirectory coverage check", map[string]string{"protocol": "nfs4.1"}},
		{"dedicated export lookup", map[string]string{"provisioningModel": "dedicated-export", "allowAnyClient": "true"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &MockClient{
				CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
				},
				CreateQuotaFunc:           func(context.Context, string, uint64) error { return nil },
				SetFileAttributesByIDFunc: func(context.Context, string, qumulo.FileAttributes) error { return nil },
				ListExportsFunc: func(context.Context) ([]qumulo.ExportInfo, error) {
					return nil, &qumulo.APIError{StatusCode: http.StatusServiceUnavailable, Class: "http_service_unavailable_error"}
				},
			}
			srv := newTestControllerServer(t, client)

			_, err := srv.CreateVolume(context.Background(), unreachableCreateRequest(tc.params))

			require.Error(t, err)
			assert.Equal(t, codes.Unavailable, status.Code(err), status.Convert(err).Message())
			assert.Contains(t, status.Convert(err).Message(), "listing exports")
		})
	}
}

// The same listing refused for a reason that is the request's, not the
// array's (a 4xx), stays Internal: only a 5xx is the array failing.
func TestCreateVolumeKeepsInternalForA4xxOnTheExportListing(t *testing.T) {
	client := &MockClient{
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		CreateQuotaFunc:           func(context.Context, string, uint64) error { return nil },
		SetFileAttributesByIDFunc: func(context.Context, string, qumulo.FileAttributes) error { return nil },
		ListExportsFunc: func(context.Context) ([]qumulo.ExportInfo, error) {
			return nil, &qumulo.APIError{StatusCode: http.StatusForbidden, Class: "http_forbidden_error"}
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), unreachableCreateRequest(map[string]string{"protocol": "nfs4.1"}))

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err), status.Convert(err).Message())
}
