//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
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

// Findings from the controller half of the test-suite audit. Each test
// failed before its fix.

// dfxB5Array is a TLS array that answers every request with a 503, or, when
// hang is set, never answers. Either way the first request an RPC makes --
// the volume path lookup -- is the one that fails.
func dfxB5Array(t *testing.T, hang bool) *httptest.Server {
	t.Helper()
	array := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hang {
			select {
			case <-r.Context().Done():
			case <-time.After(10 * time.Second):
			}
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"module": "server", "error_class": "service_unavailable_error", "description": "upgrading"})
	}))
	t.Cleanup(array.Close)
	return array
}

// dfxB5Outage runs one RPC against an array in each outage shape and checks
// it answers Unavailable from the lookup, naming the endpoint it dialled.
func dfxB5Outage(t *testing.T, call func(srv *ControllerServer, handle string) error) {
	for _, tc := range []struct {
		name string
		hang bool
	}{
		{name: "503", hang: false},
		{name: "transport timeout", hang: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			array := dfxB5Array(t, tc.hang)
			srv := dfxTLSController(t, array, 300*time.Millisecond)
			handle := newVolumeID("10.0.0.5", array.URL, "/csi-volumes/pvc-live").String()

			err := call(srv, handle)
			t.Logf("answered: %v", err)
			assertCode(t, err, codes.Unavailable)
			msg := status.Convert(err).Message()
			assert.Contains(t, msg, "resolving volume", "the failure was not the path lookup")
			assert.Contains(t, msg, array.URL, "the message must name the endpoint dialled")
		})
	}
}

// ControllerExpandVolume during an array
// outage must answer Unavailable when its first step, the volume path
// lookup, gets a 503 or times out -- not Internal (503) or
// FailedPrecondition (timeout). An expand is always safe to retry.
func TestDefect_ExpandLookupDuringAnOutageIsUnavailable(t *testing.T) {
	dfxB5Outage(t, func(srv *ControllerServer, handle string) error {
		_, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId: handle, Secrets: testSecrets(),
			CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		})
		return err
	})
}

// The same for ListSnapshots filtered by source volume, whose first
// step is the same lookup.
func TestDefect_ListSnapshotsBySourceLookupDuringAnOutageIsUnavailable(t *testing.T) {
	dfxB5Outage(t, func(srv *ControllerServer, handle string) error {
		_, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
			SourceVolumeId: handle, Secrets: testSecrets(),
		})
		return err
	})
}

// dfxB5Timeout is the error the HTTP client returns when its own timeout
// passes: a *url.Error whose Timeout() is true.
type dfxB5Timeout struct{}

func (dfxB5Timeout) Error() string {
	return "context deadline exceeded (Client.Timeout exceeded while awaiting headers)"
}
func (dfxB5Timeout) Timeout() bool   { return true }
func (dfxB5Timeout) Temporary() bool { return true }

// Expand's second step, the quota read, answered FailedPrecondition
// for a transport timeout -- the answer for an address that does not serve
// the API -- while a 503 on the same step was already Unavailable.
func TestDefect_ExpandQuotaReadTimeoutIsUnavailable(t *testing.T) {
	client := &MockClient{
		LookupFileByPathFunc: volumeAt(testStoreRealPath+"/pvc-abc", testFileID),
		GetQuotaFunc: func(context.Context, string) (uint64, error) {
			return 0, &url.Error{Op: "Get", URL: "https://qumulo.example.com:8000/v1/files/" + testFileID + "/quota",
				Err: dfxB5Timeout{}}
		},
	}
	_, err := newTestControllerServer(t, client).ControllerExpandVolume(context.Background(),
		&csi.ControllerExpandVolumeRequest{
			VolumeId: testHandle("pvc-abc"), Secrets: testSecrets(),
			CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		})
	t.Logf("ControllerExpandVolume answered: %v", err)
	assertCode(t, err, codes.Unavailable)
	assert.Contains(t, status.Convert(err).Message(), "reading quota")
}

// An export found only by the re-list after CreateExport
// answered 409 was adopted without the restriction comparison, so a class asking
// for 10.0.0.5 got a volume whose export admits 0.0.0.0/0, with a PV saying
// otherwise. It must be AlreadyExists, as on the first lookup.
func TestDefect_ExportAdoptedAfterAConflictIsComparedLikeAnyOther(t *testing.T) {
	var created atomic.Bool
	wide := volumeExport("pvc-abc")
	wide.AllowedClients = []string{"0.0.0.0/0"}

	client := adoptableVolume(wide)
	client.ListExportsFunc = func(context.Context) ([]qumulo.ExportInfo, error) {
		if !created.Load() {
			return nil, nil // the first lookup races the other attempt's create
		}
		return []qumulo.ExportInfo{wide}, nil
	}
	client.CreateExportFunc = func(context.Context, qumulo.ExportSpec) (qumulo.ExportInfo, error) {
		created.Store(true)
		return qumulo.ExportInfo{}, &qumulo.APIError{StatusCode: http.StatusConflict}
	}

	_, err := newTestControllerServer(t, client).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets: testSecrets(), Name: "pvc-abc", VolumeCapabilities: mountCapabilities(),
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:    dedicatedParams(map[string]string{"allowedClients": "10.0.0.5"}),
	})
	t.Logf("CreateVolume answered: %v", err)
	require.True(t, created.Load(), "the test did not reach the conflict path")
	assertCode(t, err, codes.AlreadyExists)
	assert.Contains(t, status.Convert(err).Message(), paramAllowedClients)
}

// storageClassAPIURL took whichever of two case-variant apiurl
// keys map iteration reached first, so an expand dialled one endpoint or the
// other at random. parseVolumeParams refuses the same class.
func TestDefect_ExpandRefusesTwoCaseVariantAPIURLKeys(t *testing.T) {
	params := map[string]string{
		"apiUrl": "https://a.example.com:8000",
		"APIURL": "https://b.example.com:8000",
	}
	_, err := storageClassAPIURL(params)
	t.Logf("storageClassAPIURL answered: %v", err)
	assertCode(t, err, codes.InvalidArgument)
	if err != nil {
		assert.Contains(t, err.Error(), "apiUrl")
		assert.Contains(t, err.Error(), "APIURL")
	}
	_, perr := parseVolumeParams(params)
	assert.Error(t, perr, "parseVolumeParams refuses the same class")
}

// A VolumeSnapshotClass key is compared exactly, so " apiurl"
// (a leading space) is refused -- and the refusal printed it trimmed, as
// "apiurl", which is the spelling it then told the administrator to use.
func TestDefect_SnapshotClassRefusalQuotesTheKeyAsWritten(t *testing.T) {
	_, err := validateSnapshotParams(map[string]string{" apiurl": "https://a.example.com:8000"})
	require.Error(t, err)
	t.Logf("validateSnapshotParams answered: %v", err)
	assert.Contains(t, err.Error(), `" apiurl"`, "the refused key must be shown as written")
	assert.False(t, strings.HasPrefix(err.Error(), "unknown VolumeSnapshotClass parameter(s): apiurl"),
		"the refused key was shown trimmed")
}
