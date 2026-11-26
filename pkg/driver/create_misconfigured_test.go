// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// arrayFailingWith answers every call CreateVolume makes with the transport
// failure err produces for its path.
func arrayFailingWith(err func(path string) error) *MockClient {
	return &MockClient{
		GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
			return qumulo.NFSSettings{}, err("/v2/nfs/settings")
		},
		ListTenantsFunc:  func(context.Context) ([]qumulo.TenantInfo, error) { return nil, err("/v1/multitenancy/tenants/") },
		ListNetworksFunc: func(context.Context) ([]qumulo.NetworkInfo, error) { return nil, err("/v3/network") },
		ListExportsFunc:  func(context.Context) ([]qumulo.ExportInfo, error) { return nil, err("/v3/nfs/exports/") },
		LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, err("/v1/files/info/attributes")
		},
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, err("/v1/files/entries/")
		},
	}
}

func dialFailure(cause error) func(string) error {
	return func(path string) error {
		return fmt.Errorf("calling %s: %w", path, &url.Error{
			Op: "Get", URL: "https://" + testServer + ":8000" + path,
			Err: &net.OpError{Op: "dial", Net: "tcp", Err: cause},
		})
	}
}

// A transport failure that no retry can fix --
// the endpoint's name does not exist, its certificate does not verify -- was
// Unavailable like any other, and csi-provisioner retried it for ever, even
// after the PVC was deleted. It is InvalidArgument, final, naming what to
// fix. A failure an array restarting also produces stays Unavailable.
func TestCreateVolumeTellsAMisconfiguredEndpointFromADownOne(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		want  codes.Code
	}{
		{name: "NXDOMAIN", cause: &net.DNSError{Name: testServer, Err: "no such host", IsNotFound: true}, want: codes.InvalidArgument},
		{name: "DNS timeout", cause: &net.DNSError{Name: testServer, Err: "i/o timeout", IsTimeout: true}, want: codes.Unavailable},
		{name: "connection refused", cause: os.NewSyscallError("connect", syscall.ECONNREFUSED), want: codes.Unavailable},
	} {
		for _, class := range []struct {
			name   string
			params map[string]string
		}{
			{"shared subdirectory", map[string]string{"protocol": "nfs3"}},
			{"dedicated export, named tenant", map[string]string{"provisioningModel": "dedicated-export", "allowAnyClient": "true", "tenantID": "finance"}},
		} {
			t.Run(tc.name+", "+class.name, func(t *testing.T) {
				srv := newTestControllerServer(t, arrayFailingWith(dialFailure(tc.cause)))

				_, err := srv.CreateVolume(context.Background(), unreachableCreateRequest(class.params))

				require.Error(t, err)
				assert.Equal(t, tc.want, status.Code(err), status.Convert(err).Message())
			})
		}
	}
}

// The same through the real HTTP client: a TLS endpoint whose certificate the
// controller does not trust fails every call the same way, so it is final.
func TestCreateVolumeAgainstAnUntrustedCertificateIsFinal(t *testing.T) {
	array := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("no request should get past the TLS handshake")
	}))
	defer array.Close()
	array.Config.ErrorLog = nil
	srv := newIntegrationControllerServer(t, array.URL)

	_, err := srv.CreateVolume(context.Background(), unreachableCreateRequest(map[string]string{"protocol": "nfs3"}))

	requireCode(t, err, codes.InvalidArgument)
	assert.Contains(t, status.Convert(err).Message(), "certificate does not verify")
}

// Behind an HTTPS proxy the controller never resolves the array's
// name, so an unknown or disallowed host comes back as the proxy refusing the
// CONNECT. 403 and 407 are final, naming the proxy; a proxy whose upstream is
// down or slow (502, 503, 504) is retried like an array that is down.
func TestCreateVolumeTellsAProxyRefusalFromAProxyOutage(t *testing.T) {
	for _, tc := range []struct {
		code int
		want codes.Code
	}{
		{http.StatusForbidden, codes.InvalidArgument},
		{http.StatusProxyAuthRequired, codes.InvalidArgument},
		{http.StatusBadGateway, codes.Unavailable},
		{http.StatusServiceUnavailable, codes.Unavailable},
		{http.StatusGatewayTimeout, codes.Unavailable},
	} {
		t.Run(http.StatusText(tc.code), func(t *testing.T) {
			refused := &qumulo.ProxyRefusedError{
				Proxy: "10.0.10.4:3128", Target: testServer + ":8000",
				StatusCode: tc.code, Status: fmt.Sprintf("%d %s", tc.code, http.StatusText(tc.code)),
			}
			srv := newTestControllerServer(t, arrayFailingWith(func(path string) error {
				return fmt.Errorf("calling %s: %w", path, &url.Error{
					Op: "Get", URL: "https://" + testServer + ":8000" + path, Err: refused,
				})
			}))

			_, err := srv.CreateVolume(context.Background(), unreachableCreateRequest(map[string]string{"protocol": "nfs3"}))

			require.Error(t, err)
			assert.Equal(t, tc.want, status.Code(err), status.Convert(err).Message())
			if tc.want == codes.InvalidArgument {
				assert.Contains(t, status.Convert(err).Message(), "HTTPS proxy 10.0.10.4:3128 refused to connect")
			}
		})
	}
}
