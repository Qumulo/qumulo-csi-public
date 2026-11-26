// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// transportError is what pkg/qumulo returns when the HTTP round trip itself
// did not complete: the *url.Error from net/http, wrapped by Client.do.
func transportError(path string) error {
	return fmt.Errorf("calling %s: %w", path, &url.Error{
		Op: "Get", URL: "https://192.0.2.1:8000" + path, Err: context.DeadlineExceeded,
	})
}

// deadArray answers every call CreateVolume makes with a transport failure,
// and counts them: each one is a full qumulo.apiTimeout on a real cluster.
func deadArray(calls *atomic.Int32) *MockClient {
	fail := func(path string) error { calls.Add(1); return transportError(path) }
	return &MockClient{
		GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
			return qumulo.NFSSettings{}, fail("/v2/nfs/settings")
		},
		ListTenantsFunc: func(context.Context) ([]qumulo.TenantInfo, error) {
			return nil, fail("/v1/multitenancy/tenants/")
		},
		ListNetworksFunc: func(context.Context) ([]qumulo.NetworkInfo, error) {
			return nil, fail("/v3/network")
		},
		ListExportsFunc: func(context.Context) ([]qumulo.ExportInfo, error) {
			return nil, fail("/v3/nfs/exports/")
		},
		LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, fail("/v1/files/info/attributes")
		},
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, fail("/v1/files/entries/")
		},
	}
}

func unreachableCreateRequest(params map[string]string) *csi.CreateVolumeRequest {
	base := map[string]string{
		"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
	}
	for k, v := range params {
		base[k] = v
	}
	return &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-dead-array",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         base,
	}
}

// Against an array that does not answer, the first call that times out
// ends CreateVolume as Unavailable -- whichever step it is for this class --
// instead of each later step waiting out the same timeout and the RPC ending
// Internal, which csi-provisioner records as final.
func TestCreateVolumeStopsAtTheFirstCallAnUnreachableArrayDoesNotAnswer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{"shared subdirectory, nfs3", map[string]string{"protocol": "nfs3"}},
		{"shared subdirectory, nfs4.1", map[string]string{"protocol": "nfs4.1"}},
		{"dedicated export, no tenant", map[string]string{"provisioningModel": "dedicated-export", "allowAnyClient": "true"}},
		{"dedicated export, numeric tenant", map[string]string{"provisioningModel": "dedicated-export", "allowAnyClient": "true", "tenantID": "1"}},
		{"dedicated export, named tenant", map[string]string{"provisioningModel": "dedicated-export", "allowAnyClient": "true", "tenantID": "finance"}},
		{"kerberos", map[string]string{"securityFlavor": "krb5", "protocol": "nfs4.1", "provisioningModel": "dedicated-export", "allowAnyClient": "true", "tenantID": "1"}},
		{"ad domain", map[string]string{"adDomain": "corp.example.com"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := newTestControllerServer(t, deadArray(&calls))

			_, err := srv.CreateVolume(context.Background(), unreachableCreateRequest(tc.params))

			require.Error(t, err)
			assert.Equal(t, codes.Unavailable, status.Code(err), status.Convert(err).Message())
			assert.Equal(t, int32(1), calls.Load(), "one timeout, not one per step")
		})
	}
}

// The directory POST timing out may still have created the directory, and
// asking the same endpoint whether it did would only wait again. Unavailable
// makes csi-provisioner retry, and the retry adopts it.
func TestCreateVolumeDoesNotAskADeadArrayWhetherTheTimedOutPostLanded(t *testing.T) {
	looked := false
	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, transportError("/v1/files/entries/")
		},
		LookupFileByPathFunc: func(_ context.Context, path string) (qumulo.FileInfo, error) {
			looked = true
			return qumulo.FileInfo{ID: testFileID, Path: path, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	})

	_, err := srv.CreateVolume(context.Background(), unreachableCreateRequest(map[string]string{"protocol": "nfs3"}))

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.False(t, looked)
}

// The adopt lookup after an answered refusal is the one place the array may
// go quiet mid-RPC. That is an outage too, not "there is no directory".
func TestCreateVolumeReportsAnAdoptLookupThatGotNoAnswerAsUnavailable(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, fakeAPIError(http.StatusConflict, "fs_entry_exists_error", "exists")
		},
		LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, transportError("/v1/files/info/attributes")
		},
	})

	_, err := srv.CreateVolume(context.Background(), unreachableCreateRequest(map[string]string{"protocol": "nfs3"}))

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

// Fail-open stays fail-open for a cluster that answers: an NFS-settings read
// the client could not decode is not an outage, and must not start blocking
// provisioning on a cluster whose settings document differs.
func TestAnAnsweredButUnreadableSettingsReadStillFailsOpen(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
			return qumulo.NFSSettings{}, errors.New("decoding response from /v2/nfs/settings: unexpected EOF")
		},
	})

	_, err := srv.CreateVolume(context.Background(), unreachableCreateRequest(map[string]string{"protocol": "nfs4.1"}))

	require.NoError(t, err)
}
