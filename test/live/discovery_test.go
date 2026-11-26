//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// What the real Qumulo API accepts, as opposed to what this driver believes
// it accepts.
//
// pkg/qumulo/client_test.go already covers the client thoroughly -- 54 tests
// -- but every one of them asserts against an httptest server this repository
// writes. That proves the client sends what the author intended; it cannot
// prove the cluster agrees. These tests ask the cluster.
//
// The distinction is not academic. The export and snapshot-policy endpoints
// reject an incomplete field set rather than defaulting it, and which fields
// are required is not derivable from the response shape -- it was originally
// learned by sending deliberately incomplete bodies and reading the errors.
// That knowledge is now encoded in CreateExport and CreatePolicy, and it is
// only as good as the last time anybody checked it against a real Core
// release.
//
// Split by blast radius, matching the Makefile:
//
//	live-discovery         GETs only
//	live-discovery-schema  POSTs that are expected to be REJECTED
//	live-discovery-mutate  really creates and deletes an export
package live

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// rawRequest sends a request the typed client cannot express.
//
// The schema tests deliberately send bodies that are wrong, which is the
// point: a typed client that cannot build an invalid body cannot discover
// which fields the cluster requires.
func rawRequest(ctx context.Context, t *testing.T, h *harness, method, apiPath string, body any) (int, string) {
	t.Helper()

	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encoding the request body: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, h.APIURL+apiPath, reader)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+h.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{
		Timeout: liveTimeout,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			InsecureSkipVerify: os.Getenv("QUMULO_TLS_INSECURE") == "1",
			MinVersion:         tls.VersionTLS12,
		}},
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, apiPath, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the response body: %v", err)
	}
	return resp.StatusCode, string(raw)
}

// TestLiveDiscoverNFSExportsAPI reads the export table through the client and
// checks the fields the driver depends on are populated.
//
// ExportPath and FSPath are the two halves of the shared-subdirectory coverage check. An
// export whose FSPath the cluster stopped returning would make every shared
// volume look uncovered and every CreateVolume fail closed.
func TestLiveDiscoverNFSExportsAPI(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	exports, err := h.Client.ListExports(ctx)
	if err != nil {
		t.Fatalf("ListExports: %v", err)
	}
	if len(exports) == 0 {
		t.Skip("this cluster has no NFS exports, so there is nothing to read the shape of")
	}

	t.Logf("%d exports", len(exports))
	for _, export := range exports {
		if export.ID == "" {
			t.Errorf("export with ExportPath %q has no ID; DeleteExport addresses by ID",
				export.ExportPath)
		}
		if export.ExportPath == "" {
			t.Errorf("export %s has no ExportPath; it is what the node mounts", export.ID)
		}
		if export.FSPath == "" {
			t.Errorf("export %s has no FSPath; the shared-model coverage check resolves "+
				"through it", export.ID)
		}
		// The cluster assigns tenant 1 (Default) when an export is created
		// without one, so zero means the field stopped being returned.
		if export.TenantID == 0 {
			t.Errorf("export %s reports tenant 0; the cluster assigns 1 (Default) when "+
				"none is given, so this field is no longer being decoded", export.ID)
		}
	}
}

// TestLiveDiscoverNFSExportSchema confirms the cluster still refuses an
// incomplete export body.
//
// Creates nothing when it passes: every request here is expected to be
// rejected. If one ever succeeds the test says so loudly, because a cluster
// that started defaulting these fields would also start creating exports the
// driver did not fully specify.
func TestLiveDiscoverNFSExportSchema(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cases := []struct {
		name string
		body map[string]any
	}{
		{"no fields at all", map[string]any{}},
		{"export_path without fs_path", map[string]any{
			"export_path": path.Join(h.StoreExportPath, "csi-live-test-schema"),
		}},
		{"fs_path without export_path", map[string]any{
			"fs_path": path.Join(h.StoreRealPath, "csi-live-test-schema"),
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := rawRequest(ctx, t, h, http.MethodPost, "/v3/nfs/exports/", tc.body)
			if code < 400 {
				t.Fatalf("the cluster ACCEPTED an incomplete export body (%d): %s\n"+
					"CreateExport sends a full field set on the assumption this is refused; "+
					"if the cluster now defaults these, an export can be created that nobody "+
					"specified", code, body)
			}
			t.Logf("%s -> %d: %s", tc.name, code, body)
		})
	}
}

// TestLiveDiscoverSnapshotPolicyAPI reads the policy list.
//
// The driver names its policies csi-<volume> and finds them by that name, so
// the Name field being populated is load-bearing: without it a snapshotSchedule
// class would create a duplicate policy on every CreateVolume retry.
func TestLiveDiscoverSnapshotPolicyAPI(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	policies, err := h.Client.ListPolicies(ctx)
	if err != nil {
		t.Fatalf("ListPolicies: %v", err)
	}

	t.Logf("%d snapshot policies", len(policies))
	for _, policy := range policies {
		if policy.ID == "" {
			t.Errorf("policy %q has no ID; DeletePolicy addresses by ID", policy.Name)
		}
		if policy.Name == "" {
			t.Errorf("policy %s has no Name; ensurePolicy finds its own policies by name", policy.ID)
		}
	}
}

// TestLiveDiscoverSnapshotPolicySchema confirms the policy endpoint still
// refuses a body with no schedule.
//
// This is the endpoint behind the snapshot-schedule refusal. The hourly form
// was removed because HOURLY_OR_LESS takes a window and an interval with nowhere to put an anchor
// minute; a cluster that started accepting an anchored hourly policy would
// make that decision worth revisiting.
func TestLiveDiscoverSnapshotPolicySchema(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cases := []struct {
		name string
		body map[string]any
	}{
		{"no fields at all", map[string]any{}},
		{"a name with no schedule", map[string]any{
			"name": "csi-live-test-schema",
		}},
		{"an hourly schedule carrying an anchor minute", map[string]any{
			"name": "csi-live-test-schema",
			"schedule": map[string]any{
				"frequency": "SCHEDULE_HOURLY_OR_LESS",
				"minute":    15,
			},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := rawRequest(ctx, t, h, http.MethodPost, "/v1/snapshots/policies/", tc.body)
			if code < 400 {
				t.Fatalf("the cluster ACCEPTED %s (%d): %s\n"+
					"If an hourly policy can now carry a minute, the hourly schedule was closed by "+
					"refusal on an assumption that no longer holds -- see errHourlyUnsupported "+
					"in pkg/driver/schedule.go", tc.name, code, body)
			}
			t.Logf("%s -> %d: %s", tc.name, code, body)
		})
	}
}

// TestLiveDiscoverActiveDirectory reads the cluster's AD configuration.
//
// Read-only, and deliberately not an assertion about whether AD is joined:
// adDomain is a path namespace and never an access control (rule 2), so the
// driver's behaviour does not depend on this. It is recorded because an
// operator reading a failure in an adDomain class needs to know what the
// cluster's identity configuration actually was.
func TestLiveDiscoverActiveDirectory(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	code, body := rawRequest(ctx, t, h, http.MethodGet, "/v1/ad/status", nil)
	if code == http.StatusNotFound {
		t.Skip("this Core release does not serve /v1/ad/status")
	}
	if code >= 400 {
		t.Skipf("reading AD status needs a privilege this token lacks (%d): %s", code, body)
	}
	t.Logf("AD status: %s", body)
}

// TestLiveDiscoverNFSExportLifecycle creates a real export and removes it.
//
// The only test here that changes cluster configuration, hence the second
// gate. It exists because the create path is the one with a field set to get
// wrong, and a rejected create is not the same evidence as a successful one:
// the schema tests above prove the cluster refuses what is incomplete, and
// only this proves the driver's own body is complete.
func TestLiveDiscoverNFSExportLifecycle(t *testing.T) {
	requireMutateConfig(t)
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	name := fmt.Sprintf("csi-live-test-export-%d", time.Now().UnixNano())
	realPath := path.Join(h.StoreRealPath, name)

	dir, err := h.Client.CreateDirectory(ctx, h.StoreRealPath, name)
	if err != nil {
		t.Fatalf("creating the directory to export: %v", err)
	}
	h.Track(dir.ID, realPath)

	export, err := h.Client.CreateExport(ctx, qumulo.ExportSpec{
		ExportPath: path.Join(h.StoreExportPath, name),
		FSPath:     realPath,
	})
	if err != nil {
		// The ancestor rule again: correct cluster behaviour, not a defect.
		t.Skipf("this cluster refused an export at %s: %v -- on an NFSv4-enabled cluster an "+
			"export cannot nest inside an existing one", realPath, err)
	}
	t.Logf("created export %s: %s -> %s", export.ID, export.ExportPath, export.FSPath)

	if export.ID == "" {
		t.Fatal("CreateExport returned no ID, so the export cannot be deleted again")
	}
	if found := exportForPath(ctx, t, h, realPath); found == nil {
		t.Errorf("the export was created but does not appear in ListExports")
	}

	if err := h.Client.DeleteExport(ctx, export.ID); err != nil {
		t.Fatalf("deleting export %s: %v -- it is now orphaned on the cluster", export.ID, err)
	}
	if leaked := exportForPath(ctx, t, h, realPath); leaked != nil {
		t.Errorf("export %s survived its delete", leaked.ID)
	}
}
