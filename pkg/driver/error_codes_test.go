// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// It used to answer Unavailable, "cluster ... is unreachable".
func TestCreateVolumeReportsACancelledCallAsCancelled(t *testing.T) {
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

// An HTTP 503 on the directory create must be retried inside the RPC or answered Unavailable, not Internal.
// It used to answer Internal, a final failure, with no retry.
func TestCreateVolumeTreatsA503OnTheDirectoryCreateAsRetryable(t *testing.T) {
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

// A CreateVolume whose own context is cancelled must answer Canceled or Aborted, not "cluster is unreachable".

// An array still answering 503 after the client's retries is
// Unavailable, which csi-provisioner retries, not a final Internal.
func TestCreateVolumeAnswersUnavailableForAPersistent503(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	array := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.EscapedPath(), "/entries/") {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error_class": "http_service_unavailable_error", "description": "Service Unavailable"})
			return
		}
		cluster.handler()(w, r)
	}))
	defer array.Close()

	_, err := newIntegrationControllerServer(t, array.URL).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-503", Secrets: testSecrets(), Parameters: testParams(),
		VolumeCapabilities: mountCapabilities(), CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("a persistent 503 answered %v; want Unavailable", err)
	}
}
