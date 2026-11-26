//go:build defects

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
	"net/http/httptest"
	"net/url"
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

// dfxTLSController is a controller whose default per-request clients trust
// srv, a TLS test server, with the given per-request timeout. TLS so that an
// endpoint written into a handle or an apiUrl is https:// (refuses http://
// on a fixed build, which would otherwise mask what these tests check).
func dfxTLSController(t *testing.T, srv *httptest.Server, timeout time.Duration) *ControllerServer {
	t.Helper()
	hc := srv.Client()
	hc.Timeout = timeout
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "0.1.0", HTTPClient: hc})
	require.NoError(t, err)
	return NewControllerServer(d)
}

// An HTTP 503 on the directory create must be retried inside the
// RPC or answered Unavailable, not Internal.
func TestDefect_CreateVolumeTreatsA503OnTheDirectoryCreateAsRetryable(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	var refused atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.EscapedPath(), "/entries/") && refused.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error_class": "http_service_unavailable_error", "description": "Service Unavailable"})
			return
		}
		cluster.handler()(w, r)
	}))
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-503", Secrets: testSecrets(), Parameters: testParams(),
		VolumeCapabilities: mountCapabilities(), CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
	})
	t.Logf("CreateVolume answered: %v", err)
	if err != nil && status.Code(err) != codes.Unavailable {
		t.Errorf("a 503 on the directory create answered %v; want a retry inside the RPC, or Unavailable",
			status.Code(err))
	}
}

// The same for the quota step: a 503 on the quota create must be
// retried or answered Unavailable, and must not strand the claim.
func TestDefect_CreateVolumeTreatsA503OnTheQuotaCreateAsRetryable(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	var refused atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.EscapedPath() == "/v1/files/quotas/" && refused.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error_class": "http_service_unavailable_error", "description": "Service Unavailable"})
			return
		}
		cluster.handler()(w, r)
	}))
	defer server.Close()

	req := &csi.CreateVolumeRequest{
		Name: "pvc-503q", Secrets: testSecrets(), Parameters: testParams(),
		VolumeCapabilities: mountCapabilities(), CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
	}
	_, err := newIntegrationControllerServer(t, server.URL).CreateVolume(context.Background(), req)
	t.Logf("CreateVolume answered: %v", err)
	if err != nil && status.Code(err) != codes.Unavailable {
		t.Errorf("a 503 on the quota create answered %v; want a retry inside the RPC, or Unavailable",
			status.Code(err))
	}
	if err != nil {
		_, err = newIntegrationControllerServer(t, server.URL).CreateVolume(context.Background(), req)
		assert.NoError(t, err, "the retry after a 503 on the quota create must finish the volume")
	}
}

// A CreateVolume whose own context is cancelled must answer
// Canceled or Aborted, not "cluster is unreachable".
func TestDefect_CreateVolumeReportsACancelledCallAsCancelled(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.EscapedPath(), "/entries/") {
			cancel() // the caller goes away while the array works on the create
			select {
			case <-r.Context().Done():
			case <-time.After(time.Second):
			}
			return
		}
		cluster.handler()(w, r)
	}))
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: "pvc-cancel", Secrets: testSecrets(), Parameters: testParams(),
		VolumeCapabilities: mountCapabilities(), CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
	})
	t.Logf("CreateVolume answered: %v", err)
	if c := status.Code(err); c != codes.Canceled && c != codes.Aborted {
		t.Errorf("a cancelled CreateVolume answered %v; want Canceled or Aborted", c)
	}
	if err != nil && strings.Contains(err.Error(), "unreachable") {
		t.Errorf("a cancelled CreateVolume reported the array unreachable: %v", err)
	}
}

// A GetCapacity whose Secret read is cut short by its own
// cancellation must answer Canceled, not Internal. CreateVolume got this with
// the fix; GetCapacity's Secret read happens before any array client
// exists and still answered Internal "reading Secret ... context canceled" on
// every controller restart under load.
func TestDefect_GetCapacityReportsACancelledSecretReadAsCancelled(t *testing.T) {
	reader := &fakeSecretReader{err: fmt.Errorf("Get \"https://10.96.0.1:443/api/v1/namespaces/ns/secrets/s\": %w", context.Canceled)}
	srv := newCapacityServer(t, &MockClient{}, reader)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := srv.GetCapacity(ctx, &csi.GetCapacityRequest{Parameters: capacityParams("array-a", "/csi", "ns", "s")})
	t.Logf("GetCapacity answered: %v", err)
	require.Error(t, err)
	assert.Equal(t, codes.Canceled, status.Code(err), "a cancelled Secret read is not a driver fault")
	assert.NotContains(t, err.Error(), "unreachable")

	// Control: any other untyped Secret read error is still the driver's to report.
	reader.err = errors.New("etcdserver: request timed out")
	_, err = srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{Parameters: capacityParams("array-a", "/csi", "ns", "s")})
	assert.Equal(t, codes.Internal, status.Code(err), "a transient API-server error still answers Internal")
}

// A permanent misconfiguration (an endpoint whose TLS certificate
// does not verify) must get a final answer, not the retryable Unavailable that
// keeps a deleted claim being provisioned.
func TestDefect_CreateVolumeAnswersAnUnverifiableEndpointFinally(t *testing.T) {
	var hits atomic.Int32
	array := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.NotFound(w, r)
	}))
	defer array.Close()

	// The controller does NOT trust the test server's certificate.
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "0.1.0",
		HTTPClient: &http.Client{Timeout: 5 * time.Second}})
	require.NoError(t, err)
	params := testParams()
	params["apiUrl"] = array.URL
	_, err = NewControllerServer(d).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-misconf", Secrets: testSecrets(), Parameters: params,
		VolumeCapabilities: mountCapabilities(), CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
	})
	t.Logf("CreateVolume answered: %v", err)
	require.Error(t, err)
	c := status.Code(err)
	assert.NotEqual(t, codes.Unavailable, c, "an unverifiable endpoint is not transient")
	assert.NotEqual(t, codes.DeadlineExceeded, c, "an unverifiable endpoint is not transient")
	assert.Contains(t, []codes.Code{codes.InvalidArgument, codes.FailedPrecondition}, c,
		"want a final answer naming the configuration")
}

// Against an array that does not answer, CreateVolume must end at
// the first transport timeout with Unavailable or DeadlineExceeded, not wait
// out three timeouts and answer a final Internal.
func TestDefect_CreateVolumeEndsAtTheFirstTimeoutAsRetryable(t *testing.T) {
	const timeout = 400 * time.Millisecond
	var hits atomic.Int32
	array := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
		// Never answers, not even on the way out. A handler that returned
		// normally let the server send an empty 200, which a client that
		// had not yet given up on a loaded machine decoded as EOF -- a
		// malformed reply, Internal -- and the test flaked. Aborting
		// drops the connection with no response at all.
		panic(http.ErrAbortHandler)
	}))
	defer array.Close()

	params := testParams()
	params["apiUrl"] = array.URL
	start := time.Now()
	_, err := dfxTLSController(t, array, timeout).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-dead", Secrets: testSecrets(), Parameters: params,
		VolumeCapabilities: mountCapabilities(), CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
	})
	elapsed := time.Since(start)
	t.Logf("CreateVolume answered after %v with %d array calls: %v", elapsed, hits.Load(), err)
	c := status.Code(err)
	assert.True(t, c == codes.Unavailable || c == codes.DeadlineExceeded,
		"want Unavailable or DeadlineExceeded, got %v", c)
	assert.Less(t, elapsed, 2*timeout, "CreateVolume waited out more than one timeout against a dead array")
}

// ValidateVolumeCapabilities must report an unreachable array as
// FailedPrecondition, as delete and expand do, not re-wrap it as Internal.
func TestDefect_ValidateReportsAnUnreachableArrayAsFailedPrecondition(t *testing.T) {
	client := &MockClient{
		LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, &url.Error{Op: "Get", URL: "https://qumulo.example.com:8000/v1/files",
				Err: context.DeadlineExceeded}
		},
	}
	_, err := newTestControllerServer(t, client).ValidateVolumeCapabilities(context.Background(),
		&csi.ValidateVolumeCapabilitiesRequest{
			VolumeId:           testServer + "##/csi-volumes/pvc-x",
			VolumeCapabilities: mountCapabilities(),
			Secrets:            testSecrets(),
		})
	t.Logf("ValidateVolumeCapabilities answered: %v", err)
	assertCode(t, err, codes.FailedPrecondition)
	assert.NotContains(t, status.Convert(err).Message(), "code = ", "a status was re-wrapped inside another")
}

// A timeout listing tenants while resolving a tenant NAME is a
// transient failure (Unavailable), not a class error (InvalidArgument).
func TestDefect_TenantNameListingTimeoutIsUnavailable(t *testing.T) {
	client := &MockClient{
		ListTenantsFunc: func(context.Context) ([]qumulo.TenantInfo, error) {
			return nil, &url.Error{Op: "Get", URL: "https://qumulo.example.com:8000/v1/multitenancy/tenants/",
				Err: context.DeadlineExceeded}
		},
	}
	_, err := newTestControllerServer(t, client).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-tenant-name",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Secrets:            testSecrets(),
		Parameters: map[string]string{
			"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
			"provisioningModel": "dedicated-export", "tenantID": "Default", "allowAnyClient": "true",
		},
	})
	t.Logf("CreateVolume answered: %v", err)
	c := status.Code(err)
	assert.True(t, c == codes.Unavailable || c == codes.DeadlineExceeded,
		"a tenant-listing timeout answered %v; want Unavailable (or DeadlineExceeded), not a class error", c)
}

// A 429 from the tenant listing
// is transient too, not a class error.
func TestDefect_TenantListing429IsRetryable(t *testing.T) {
	array := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/multitenancy/tenants") {
			writeJSON(w, http.StatusTooManyRequests, map[string]any{"error_class": "x", "description": "Too Many Requests"})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error_class": "http_not_found_error", "description": r.URL.Path})
	}))
	defer array.Close()
	params := testParams()
	params[paramAPIURL] = array.URL
	params[paramTenantID] = "Default"
	params[paramProvisioningModel] = provisioningModelDedicatedExport
	params[paramAllowAnyClient] = "true" // past the open-export refusal, to the tenant listing
	_, err := dfxTLSController(t, array, 10*time.Second).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-t", Secrets: testSecrets(), Parameters: params,
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB}, VolumeCapabilities: mountCapabilities(),
	})
	c := status.Code(err)
	assert.True(t, c == codes.Unavailable || c == codes.ResourceExhausted,
		"429 on the tenant listing answered %s (%v); the Event tells the operator to fix a correct class", c, err)
}

// A 404 that is not a Qumulo file not-found (a proxy answering for
// a route it does not have) must not read as "volume gone": DeleteVolume must
// not answer OK, and expand must not answer NotFound.
func TestDefect_Proxy404OnResolveIsNotVolumeGone(t *testing.T) {
	var arrayWrites atomic.Int32
	proxy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			arrayWrites.Add(1)
		}
		http.Error(w, "404 page not found", http.StatusNotFound)
	}))
	defer proxy.Close()
	srv := dfxTLSController(t, proxy, 5*time.Second)

	handle := newVolumeID("10.0.0.5", proxy.URL, "/csi-volumes/pvc-live").String()
	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})
	t.Logf("DeleteVolume answered: %v", err)
	assert.Error(t, err, "a proxy 404 was reported as a successful delete")
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Zero(t, arrayWrites.Load(), "something was written through the proxy")

	_, err = srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: handle, Secrets: testSecrets(),
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB},
	})
	t.Logf("ControllerExpandVolume answered: %v", err)
	assert.NotEqual(t, codes.NotFound, status.Code(err), "expand was told the volume is gone")
}

// ListVolumes must not answer OK
// with a class's volumes silently missing when its store-root listing gets a
// foreign 404.
func TestDefect_ListVolumesForeign404FailsInsteadOfOmitting(t *testing.T) {
	arr := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return nil, fakeAPIError(http.StatusNotFound, "http_not_found_error", "GET /v1/files/...")
		},
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("a", "qumulo-a.example.com", "/csi/a", "tok"),
			classFor("b", "qumulo-b.example.com", "/csi/b", "tok"),
		}},
		oneToken,
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): arr, baseURLFor("qumulo-b.example.com"): oneVolumeAt("pvc-2", "202")},
	)
	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	assert.Equal(t, codes.Unavailable, status.Code(err),
		"answered %v with %d entries: class a's volumes silently missing", err, len(resp.GetEntries()))
}

// ListSnapshots by ID must not
// answer an empty list for a foreign 404.
func TestDefect_ListSnapshotsForeign404FailsInsteadOfEmpty(t *testing.T) {
	array := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]any{"module": "server", "error_class": "http_not_found_error",
			"description": r.Method + " " + r.URL.Path})
	}))
	defer array.Close()
	resp, err := dfxTLSController(t, array, 5*time.Second).ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SnapshotId: newSnapshotID("10.0.0.5", array.URL, "42").String(), Secrets: testSecrets(),
	})
	assert.Equal(t, codes.Unavailable, status.Code(err),
		"answered %v with %d entries for a route the array does not have", err, len(resp.GetEntries()))
}
