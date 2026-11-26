// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// fakeCluster is a stand-in for a Qumulo cluster's REST API, just complete
// enough to serve a volume lifecycle. It exists so the controller can be
// exercised against the real qumulo.Client -- JSON encoding, the mixed
// string/number ID conventions, percent-encoded file references and
// APIError mapping all included -- rather than against the in-process
// MockClient used elsewhere in this package.
//
// Its routes are transcribed from the Qumulo REST API Guide, and it 404s
// anything it does not recognize. An earlier version of this fake mirrored
// whatever paths the driver happened to send, which let a wrong "/api/v1"
// prefix pass every test while being unable to reach a real cluster; the
// unknown-route rejection and the explicit /api guard below exist to stop
// that recurring.
type fakeCluster struct {
	// directories maps a real path to its file ID.
	directories map[string]string
	// quotas maps a file ID to its limit in bytes, as a decimal string.
	quotas map[string]string
	// treeDeletePolls maps a file ID with a running deletion job to the
	// number of status polls remaining before it reports finished.
	treeDeletePolls map[string]int

	// exports maps an export ID to its record.
	exports map[string]fakeExport
	// modes maps a file ID to its mode, as the cluster renders it. A directory
	// starts at 0755, which is the whole reason the driver sets it.
	modes map[string]string

	// pendingPolls is how many polls a newly started job takes to finish.
	pendingPolls int
	// lingerUntilFinished keeps a tree-deleted directory at its path until
	// its job finishes, as a real cluster does for a large tree.
	lingerUntilFinished bool

	nextID       int
	nextExportID int
	// exportListings counts GET /v3/nfs/exports/.
	exportListings int

	// policies maps a snapshot policy ID to its record; policyListings
	// counts GET /v1/snapshots/policies/.
	policies       map[string]qumulo.PolicyInfo
	nextPolicyID   int
	policyListings int
}

// fakeExport is one NFS export, shaped after what a real cluster returns.
type fakeExport struct {
	ExportPath  string
	FSPath      string
	Description string
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		directories:     map[string]string{},
		quotas:          map[string]string{},
		treeDeletePolls: map[string]int{},
		exports:         map[string]fakeExport{},
		modes:           map[string]string{},
		nextID:          100,
		nextExportID:    1,
	}
}

// seedSharedExport gives the fake cluster the export a shared-subdirectory
// class needs to be mountable, mapping /csi on the NFS side to /csi-volumes on
// the filesystem side -- the storeExportPath and storeRealPath pair these
// tests provision with.
//
// CreateVolume refuses a shared-model volume that no export covers, because
// provisioning one produces a Bound PVC and a pod that can never mount. So an
// otherwise-unrelated test of the shared model has to start from a cluster
// where the administrator did that prerequisite, which is what a working
// deployment looks like.
//
// The ID is fixed at "csi-store" rather than allocated, so a test asserting on
// the export table can tell this from an export the driver created.
func (c *fakeCluster) seedSharedExport() {
	c.exports["csi-store"] = fakeExport{
		ExportPath:  "/csi",
		FSPath:      "/csi-volumes",
		Description: "pre-existing export covering the CSI store path",
	}
}

func (c *fakeCluster) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// EscapedPath keeps the %2F-encoded path reference intact; Path
		// would decode it and merge it into the surrounding route.
		path := r.URL.EscapedPath()

		if strings.HasPrefix(path, "/api/") {
			panic("driver sent an /api-prefixed path: " + path)
		}

		switch {
		case path == "/v1/version":
			writeJSON(w, http.StatusOK, map[string]string{"revision_id": "Qumulo Core 7.2.0"})
		case path == "/v1/files/resolve":
			c.resolve(w, r)
		// DeleteVolume reads the relationship table on every call now
		// (replication_guard.go). This cluster has no relationships; it says
		// so rather than 404ing, because a 404 here would mean "this array
		// cannot answer whether the volume is protected", which is not what
		// an unprotected volume looks like.
		case path == "/v2/replication/source-relationships/status/",
			path == "/v2/replication/target-relationships/status/":
			writeJSON(w, http.StatusOK, []map[string]any{})
		case path == "/v1/tree-delete/jobs/" && r.Method == http.MethodPost:
			c.startTreeDelete(w, r)
		case strings.HasPrefix(path, "/v1/tree-delete/jobs/"):
			c.treeDeleteStatus(w, path)
		case strings.HasPrefix(path, "/v1/files/quotas/"):
			c.handleQuota(w, r, path)
		case strings.HasSuffix(path, "/entries/") && r.Method == http.MethodPost:
			c.createDirectory(w, r, path)
		case strings.HasSuffix(path, "/info/attributes") && r.Method == http.MethodPatch:
			c.patchAttributes(w, r, path)
		case strings.HasSuffix(path, "/info/attributes"):
			c.lookupFile(w, path)
		case path == "/v3/nfs/exports/" && r.Method == http.MethodPost:
			c.createExport(w, r)
		case path == "/v3/nfs/exports/" && r.Method == http.MethodGet:
			c.exportListings++
			c.listExports(w)
		case strings.HasPrefix(path, "/v3/nfs/exports/") && r.Method == http.MethodGet:
			c.getExport(w, strings.TrimPrefix(path, "/v3/nfs/exports/"))
		case strings.HasPrefix(path, "/v3/nfs/exports/") && r.Method == http.MethodDelete:
			c.deleteExport(w, path)
		case strings.HasPrefix(path, "/v1/snapshots/policies/"):
			c.handlePolicy(w, r, strings.TrimPrefix(path, "/v1/snapshots/policies/"))
		default:
			http.NotFound(w, r)
		}
	}
}

// handlePolicy serves the snapshot policy collection and one policy by ID,
// in the v1 wire shape.
func (c *fakeCluster) handlePolicy(w http.ResponseWriter, r *http.Request, id string) {
	if c.policies == nil {
		c.policies = map[string]qumulo.PolicyInfo{}
	}
	wire := func(p qumulo.PolicyInfo) map[string]any {
		n, _ := strconv.Atoi(p.ID)
		return map[string]any{"id": n, "name": p.Name, "source_file_ids": []string{p.SourceFileID}, "enabled": p.Enabled}
	}
	switch {
	case id == "" && r.Method == http.MethodGet:
		c.policyListings++
		entries := []map[string]any{}
		for _, p := range c.policies {
			entries = append(entries, wire(p))
		}
		writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
	case id == "" && r.Method == http.MethodPost:
		var req struct {
			Name          string   `json:"name"`
			SourceFileIDs []string `json:"source_file_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"description": err.Error()})
			return
		}
		c.nextPolicyID++
		p := qumulo.PolicyInfo{ID: strconv.Itoa(c.nextPolicyID), Name: req.Name, SourceFileID: req.SourceFileIDs[0], Enabled: true}
		c.policies[p.ID] = p
		writeJSON(w, http.StatusOK, wire(p))
	default:
		p, ok := c.policies[id]
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error_class": "snapshot_policy_not_found_error", "description": "no policy " + id})
			return
		}
		if r.Method == http.MethodDelete {
			delete(c.policies, id)
			w.WriteHeader(http.StatusOK)
			return
		}
		writeJSON(w, http.StatusOK, wire(p))
	}
}

func (c *fakeCluster) createDirectory(w http.ResponseWriter, r *http.Request, path string) {
	var req struct {
		Name   string `json:"name"`
		Action string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"description": err.Error()})
		return
	}

	// Recover the parent from ".../files/<escaped parent>/entries/".
	parent := unescapeFileRef(strings.TrimSuffix(strings.TrimPrefix(path, "/v1/files/"), "/entries/"))
	realPath := parent + "/" + req.Name

	if _, exists := c.directories[realPath]; exists {
		writeJSON(w, http.StatusConflict, map[string]string{"description": "name already exists"})
		return
	}

	c.nextID++
	id := strconv.Itoa(c.nextID)
	c.directories[realPath] = id
	// The cluster's default, and the reason a pod cannot write to a volume the
	// driver has not adjusted.
	c.modes[id] = "0755"

	writeJSON(w, http.StatusOK, map[string]string{
		"id":   id,
		"path": realPath,
		"type": "FS_FILE_TYPE_DIRECTORY",
		"mode": c.modes[id],
	})
}

func (c *fakeCluster) lookupFile(w http.ResponseWriter, path string) {
	realPath := unescapeFileRef(strings.TrimSuffix(strings.TrimPrefix(path, "/v1/files/"), "/info/attributes"))

	id, ok := c.directories[realPath]
	if !ok {
		// The class is what a real cluster sends, and what tells a missing
		// file from a 404 some other endpoint gave.
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error_class": "fs_no_such_entry_error", "description": "no such file",
		})
		return
	}

	// child_count is what Core reports with every directory's attributes;
	// CreateVolume reads it to recognise its own interrupted create.
	writeJSON(w, http.StatusOK, map[string]any{
		"id":          id,
		"path":        realPath,
		"type":        "FS_FILE_TYPE_DIRECTORY",
		"child_count": c.childCount(realPath),
	})
}

// childCount is the number of directories directly under p.
func (c *fakeCluster) childCount(p string) int {
	n := 0
	for other := range c.directories {
		if other != p && path.Dir(other) == p {
			n++
		}
	}
	return n
}

func (c *fakeCluster) resolve(w http.ResponseWriter, r *http.Request) {
	var ids []string
	if err := json.NewDecoder(r.Body).Decode(&ids); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"description": err.Error()})
		return
	}

	resolved := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		entry := map[string]string{"id": id, "path": ""}
		for realPath, fileID := range c.directories {
			if fileID == id {
				entry["path"] = realPath
				break
			}
		}
		resolved = append(resolved, entry)
	}
	writeJSON(w, http.StatusOK, resolved)
}

func (c *fakeCluster) handleQuota(w http.ResponseWriter, r *http.Request, path string) {
	id := strings.TrimPrefix(path, "/v1/files/quotas/")

	switch r.Method {
	case http.MethodPost, http.MethodPut:
		var req struct {
			ID    string `json:"id"`
			Limit string `json:"limit"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"description": err.Error()})
			return
		}
		c.quotas[req.ID] = req.Limit
		writeJSON(w, http.StatusOK, req)

	case http.MethodGet:
		// /v1/files/quotas/status/<id> is the status document, which -- as on
		// Core -- carries the limit as well as the usage.
		statusDoc := strings.HasPrefix(id, "status/")
		id = strings.TrimPrefix(id, "status/")
		limit, ok := c.quotas[id]
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"description": "no such quota"})
			return
		}
		if statusDoc {
			writeJSON(w, http.StatusOK, map[string]string{"id": id, "limit": limit, "capacity_usage": "0"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": id, "limit": limit})

	case http.MethodDelete:
		if _, ok := c.quotas[id]; !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"description": "no such quota"})
			return
		}
		delete(c.quotas, id)
		writeJSON(w, http.StatusOK, map[string]string{"id": id})

	default:
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
	}
}

// startTreeDelete accepts a deletion job. The directory is unlinked
// immediately, but the job only reports finished after pendingPolls status
// checks, mirroring the cluster's asynchronous reclaim.
func (c *fakeCluster) startTreeDelete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"description": err.Error()})
		return
	}

	if !c.lingerUntilFinished || c.pendingPolls <= 0 {
		c.unlink(req.ID)
	}

	if c.pendingPolls > 0 {
		c.treeDeletePolls[req.ID] = c.pendingPolls
	}
	w.WriteHeader(http.StatusAccepted)
}

// treeDeleteStatus reports on a running job. A finished job -- and one that
// never existed -- is a 404 carrying the cluster's own error body, which is
// how the client detects completion; a 404 without one is a proxy, and the
// client refuses to read that as finished.
func (c *fakeCluster) treeDeleteStatus(w http.ResponseWriter, path string) {
	id := path[strings.LastIndex(path, "/")+1:]

	remaining, running := c.treeDeletePolls[id]
	if !running {
		writeJSON(w, http.StatusNotFound, map[string]string{
			// What a real array answers for a finished job.
			"error_class": "http_not_found_error", "description": "Not found",
		})
		return
	}

	remaining--
	if remaining <= 0 {
		delete(c.treeDeletePolls, id)
		c.unlink(id)
	} else {
		c.treeDeletePolls[id] = remaining
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id, "remaining_files": strconv.Itoa(remaining)})
}

// unlink removes the directory with this file ID from its path.
func (c *fakeCluster) unlink(fileID string) {
	for realPath, id := range c.directories {
		if id == fileID {
			delete(c.directories, realPath)
			return
		}
	}
}

func TestControllerVolumeLifecycleAgainstRealHTTPClient(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()

	srv := newIntegrationControllerServer(t, server.URL)
	ctx := context.Background()

	createReq := &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	}

	created, err := srv.CreateVolume(ctx, createReq)
	require.NoError(t, err)
	volumeID := created.GetVolume().GetVolumeId()
	assert.Equal(t, testGiB, created.GetVolume().GetCapacityBytes())
	assert.Len(t, cluster.directories, 1)
	assert.Len(t, cluster.quotas, 1)

	// A retry of the same request must return the same volume, not a
	// second directory.
	retried, err := srv.CreateVolume(ctx, createReq)
	require.NoError(t, err)
	assert.Equal(t, volumeID, retried.GetVolume().GetVolumeId())
	assert.Len(t, cluster.directories, 1, "retry must not create a second directory")

	// The same name at a different size is a genuine conflict.
	conflicting := &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB * 2},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	}
	_, err = srv.CreateVolume(ctx, conflicting)
	assertCode(t, err, codes.AlreadyExists)

	expanded, err := srv.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		Secrets:       testSecrets(),
		VolumeId:      volumeID,
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB * 2},
	})
	require.NoError(t, err)
	assert.Equal(t, testGiB*2, expanded.GetCapacityBytes())

	validated, err := srv.ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
		Secrets:            testSecrets(),
		VolumeId:           volumeID,
		VolumeCapabilities: mountCapabilities(),
	})
	require.NoError(t, err)
	assert.NotNil(t, validated.GetConfirmed())

	_, err = srv.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volumeID, Secrets: testSecrets()})
	require.NoError(t, err)
	assert.Empty(t, cluster.directories)
	assert.Empty(t, cluster.quotas)

	// Deleting again must still succeed: the cluster now answers 404 for
	// both the quota and the job, and that is the idempotent path.
	_, err = srv.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volumeID, Secrets: testSecrets()})
	require.NoError(t, err)
}

func TestDeleteVolumeWaitsForTreeDeleteAgainstRealHTTPClient(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	// Finishes on the second status poll, inside the driver's budget.
	cluster.pendingPolls = 2

	server := httptest.NewServer(cluster.handler())
	defer server.Close()

	srv := newIntegrationControllerServer(t, server.URL)
	ctx := context.Background()

	created, err := srv.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})
	require.NoError(t, err)

	_, err = srv.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: created.GetVolume().GetVolumeId(),
	})

	// The driver polled until the job reported finished rather than
	// returning success while the tree was still being reclaimed.
	require.NoError(t, err)
	assert.Empty(t, cluster.treeDeletePolls)
}

// A delete of a large tree outlives its budget and answers Aborted,
// having already removed the quota. The directory stays at its path until the
// job ends, so the retry found a directory with neither export nor quota, and
// 's identity check refused it as not this volume's -- with advice to set
// the PV to Retain -- on every retry until the tree was gone (867 s live). The
// retry now finds its own job still running and waits on it.
func TestDeleteVolumeRetryWaitsOnItsOwnRunningTreeDelete(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	cluster.pendingPolls = 1000
	cluster.lingerUntilFinished = true
	l := &destructiveCallLog{fakeCluster: cluster}

	server := httptest.NewServer(l.handler())
	defer server.Close()

	srv := newIntegrationControllerServer(t, server.URL)
	srv.treeDeleteBudget = 20 * time.Millisecond
	srv.treeDeletePollWait = 5 * time.Millisecond
	ctx := context.Background()

	created, err := srv.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})
	require.NoError(t, err)
	del := &csi.DeleteVolumeRequest{Secrets: testSecrets(), VolumeId: created.GetVolume().GetVolumeId()}

	_, err = srv.DeleteVolume(ctx, del)
	assertCode(t, err, codes.Aborted)
	require.Len(t, cluster.treeDeletePolls, 1)
	require.Len(t, l.treeDeletes, 1)

	_, err = srv.DeleteVolume(ctx, del)
	assertCode(t, err, codes.Aborted)
	assert.NotContains(t, err.Error(), "Retain")
	assert.Len(t, l.treeDeletes, 1, "the retry waits on the running job, it does not start another")

	// Two polls left: the identity check's look at the job, then the wait's.
	for id := range cluster.treeDeletePolls {
		cluster.treeDeletePolls[id] = 2
	}
	_, err = srv.DeleteVolume(ctx, del)
	require.NoError(t, err)
	assert.Empty(t, cluster.treeDeletePolls)
}

func TestCreateVolumeRollsBackAgainstRealHTTPClient(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()

	// Serve directory creation, but fail every quota write, so the
	// controller has to undo the directory it just made.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.EscapedPath(), "/files/quotas/") {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"description": "quota service down"})
			return
		}
		cluster.handler()(w, r)
	}))
	defer server.Close()

	srv := newIntegrationControllerServer(t, server.URL)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	// A 5xx is the array failing, so the answer is retryable; the
	// rollback still has to have happened.
	assertCode(t, err, codes.Unavailable)
	assert.Empty(t, cluster.directories, "the half-created directory must not be left behind")
}

func newIntegrationControllerServer(t *testing.T, baseURL string) *ControllerServer {
	t.Helper()

	// The factory ignores the address the request names and always points at
	// the test server. These tests exercise the *real* HTTP client against a
	// fake Qumulo, so what matters is that a real client is built per request
	// -- not which host it would have dialled.
	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		NewClient: func(_, token string) qumulo.ClientInterface {
			return qumulo.New(baseURL, token)
		},
	})
	require.NoError(t, err)

	return NewControllerServer(d)
}

// tlsDriverFor is a driver whose clients trust srv, a TLS test server: an
// endpoint in a handle must be https://, so a test that writes the
// array's address into a handle serves it over TLS.
func tlsDriverFor(t *testing.T, srv *httptest.Server) *Driver {
	t.Helper()
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "0.1.0",
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			c := qumulo.New(baseURL, token)
			c.HTTPClient = srv.Client()
			return c
		}})
	require.NoError(t, err)
	return d
}

// writeJSON replies with a JSON body and status code.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// unescapeFileRef reverses the percent-encoding the client applies to a path
// used as a Qumulo file reference. An input that will not decode is returned
// unchanged, which surfaces as a 404 rather than a panic in a handler.
func unescapeFileRef(ref string) string {
	decoded, err := url.PathUnescape(ref)
	if err != nil {
		return ref
	}
	return decoded
}

// createExport mirrors POST /v3/nfs/exports/ on Qumulo Core 7.9.3.
//
// The mandatory-field check is the point of this fake. The real cluster
// requires export_path, fs_path, description and restrictions to be *present*
// and answers a missing one with a decode error naming it -- description
// included, even when empty. Enforcing that here is what would catch an
// omitempty creeping onto the request struct, which is exactly the bug that
// made CreateSnapshot unusable before its expiration field was sent.
func (c *fakeCluster) createExport(w http.ResponseWriter, r *http.Request) {
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error_class": "decode_error", "description": err.Error(),
		})
		return
	}

	for _, field := range []string{"export_path", "fs_path", "description", "restrictions"} {
		if _, present := raw[field]; !present {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error_class": "decode_error",
				"description": "field '" + field + "' not found",
			})
			return
		}
	}

	var req struct {
		ExportPath  string `json:"export_path"`
		FSPath      string `json:"fs_path"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(mustMarshal(raw), &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error_class": "decode_error"})
		return
	}

	// The cluster will not create the directory an export serves.
	if _, exists := c.directories[req.FSPath]; !exists {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error_class": "nfs_export_fs_path_doesnt_exist_error",
			"description": "The directory does not exist.",
		})
		return
	}

	for _, existing := range c.exports {
		if existing.ExportPath == req.ExportPath {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error_class": "nfs_export_already_exists_error",
				"description": "Export path already exists.",
			})
			return
		}
	}

	id := strconv.Itoa(c.nextExportID)
	c.nextExportID++
	c.exports[id] = fakeExport{ExportPath: req.ExportPath, FSPath: req.FSPath, Description: req.Description}

	// A real cluster answers 200, not 201, and echoes the whole export.
	writeJSON(w, http.StatusOK, map[string]any{
		"id": id, "export_path": req.ExportPath, "fs_path": req.FSPath,
		"description": req.Description, "tenant_id": 1,
	})
}

// getExport mirrors GET /v3/nfs/exports/<id>.
func (c *fakeCluster) getExport(w http.ResponseWriter, id string) {
	export, ok := c.exports[id]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error_class": "nfs_export_not_found_error", "description": "no such export",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": id, "export_path": export.ExportPath, "fs_path": export.FSPath, "description": export.Description,
	})
}

// listExports mirrors GET /v3/nfs/exports/, which uses the entries envelope --
// unlike v2, which answers with a bare array.
func (c *fakeCluster) listExports(w http.ResponseWriter) {
	entries := make([]map[string]any, 0, len(c.exports))
	for id, export := range c.exports {
		entries = append(entries, map[string]any{
			"id": id, "export_path": export.ExportPath, "fs_path": export.FSPath,
			"description": export.Description,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (c *fakeCluster) deleteExport(w http.ResponseWriter, path string) {
	id := strings.TrimPrefix(path, "/v3/nfs/exports/")
	if _, exists := c.exports[id]; !exists {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error_class": "nfs_export_not_found_error", "description": "no such export",
		})
		return
	}
	delete(c.exports, id)
	writeJSON(w, http.StatusOK, map[string]any{})
}

func mustMarshal(v any) []byte {
	encoded, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return encoded
}

// The dedicated-export lifecycle against the real qumulo.Client: the export is
// created with the volume and removed with it, and nothing is left behind.
func TestDedicatedExportLifecycleAgainstRealHTTPClient(t *testing.T) {
	cluster := newFakeCluster()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()

	srv := newIntegrationControllerServer(t, server.URL)

	params := map[string]string{
		"server":            testServer,
		"storeRealPath":     "/csi-volumes",
		"storeExportPath":   "/csi",
		"provisioningModel": "dedicated-export", "allowAnyClient": "true",
	}

	created, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-dedicated",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         params,
	})
	require.NoError(t, err)

	require.Len(t, cluster.exports, 1, "the volume should have its own export")
	var export fakeExport
	for _, e := range cluster.exports {
		export = e
	}
	assert.Equal(t, "/csi/pvc-dedicated", export.ExportPath)
	assert.Equal(t, "/csi-volumes/pvc-dedicated", export.FSPath)
	assert.Contains(t, export.Description, "pvc-dedicated",
		"the description is mandatory anyway, so it should say what created the export")

	// A retry must adopt the existing export rather than failing or making a
	// second one.
	retried, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-dedicated",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         params,
	})
	require.NoError(t, err)
	assert.Equal(t, created.GetVolume().GetVolumeId(), retried.GetVolume().GetVolumeId())
	assert.Len(t, cluster.exports, 1, "a retry must not create a second export")

	_, err = srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: created.GetVolume().GetVolumeId(),
	})
	require.NoError(t, err)

	assert.Empty(t, cluster.exports, "the export must go with the volume")
	assert.Empty(t, cluster.quotas, "the quota must go with the volume")
}

// A shared-subdirectory volume has no export, and deleting it must not fail
// merely because the driver looked for one.
func TestSharedSubdirectoryDeleteTouchesNoExport(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()

	srv := newIntegrationControllerServer(t, server.URL)

	// An unrelated export exists, to prove the match is on this volume's own
	// directory rather than "delete whatever export is around".
	cluster.directories["/somewhere-else"] = "999"
	cluster.exports["42"] = fakeExport{ExportPath: "/other", FSPath: "/somewhere-else"}

	created, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-shared",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":          testServer,
			"storeRealPath":   "/csi-volumes",
			"storeExportPath": "/csi",
		},
	})
	require.NoError(t, err)
	// Asserted by identity rather than by count: the table starts with the
	// seeded store export and the unrelated one, so what matters is that no
	// third entry appeared for this volume, not how many entries there are.
	assert.Len(t, cluster.exports, 2, "the shared model must not create an export")
	assert.NotContains(t, exportPaths(cluster), "/csi/pvc-shared",
		"the shared model must not create an export for the volume itself")

	_, err = srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: created.GetVolume().GetVolumeId(),
	})
	require.NoError(t, err)

	assert.Len(t, cluster.exports, 2, "delete must remove no export in the shared model")
	assert.Contains(t, cluster.exports, "42", "an unrelated export must survive")
	assert.Contains(t, cluster.exports, "csi-store",
		"the export covering the store path is shared by every volume in the class "+
			"and must outlive any one of them")
}

// patchAttributes mirrors PATCH /v1/files/{ref}/info/attributes, which the
// driver uses to set a new volume directory's mode.
//
// The ref is either a percent-encoded path or a file ID -- the driver sends an
// ID here, since that is what it has after creating the directory -- so both
// are resolved. The request body is partial: only the fields present are
// applied, which is why the driver can send mode alone.
func (c *fakeCluster) patchAttributes(w http.ResponseWriter, r *http.Request, path string) {
	ref := unescapeFileRef(strings.TrimSuffix(strings.TrimPrefix(path, "/v1/files/"), "/info/attributes"))

	// A ref that names a path is translated to its ID; anything else is taken
	// to be an ID already.
	fileID := ref
	if id, ok := c.directories[ref]; ok {
		fileID = id
	}
	if _, known := c.modes[fileID]; !known {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error_class": "fs_no_such_inode_error", "description": "no such file " + fileID,
		})
		return
	}

	var req struct {
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error_class": "decode_error"})
		return
	}
	if req.Mode != "" {
		c.modes[fileID] = req.Mode
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"id":   fileID,
		"type": "FS_FILE_TYPE_DIRECTORY",
		"mode": c.modes[fileID],
	})
}

// End to end against the real qumulo.Client: the directory the cluster creates
// at 0755 must not stay that way, or no pod can write to the volume.
func TestCreateVolumeModeAgainstRealHTTPClient(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()

	srv := newIntegrationControllerServer(t, server.URL)

	created, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-mode",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":          testServer,
			"storeRealPath":   "/csi-volumes",
			"storeExportPath": "/csi",
		},
	})
	require.NoError(t, err)

	fileID := cluster.directories["/csi-volumes/pvc-mode"]
	require.NotEmpty(t, fileID)
	assert.Equal(t, "0777", cluster.modes[fileID],
		"the volume directory must end up writable, not at the cluster's 0755 default")
	assert.NotEmpty(t, created.GetVolume().GetVolumeId())
}

// inherit is the escape hatch for an operator whose export maps identities so
// that ownership already lines up.
func TestCreateVolumeModeInheritAgainstRealHTTPClient(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()

	srv := newIntegrationControllerServer(t, server.URL)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-inherit",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":           testServer,
			"storeRealPath":    "/csi-volumes",
			"storeExportPath":  "/csi",
			"mountPermissions": "inherit",
		},
	})
	require.NoError(t, err)

	fileID := cluster.directories["/csi-volumes/pvc-inherit"]
	assert.Equal(t, "0755", cluster.modes[fileID], "inherit must leave the cluster default alone")
}

// exportPaths lists the NFS paths in the fake cluster's export table.
func exportPaths(c *fakeCluster) []string {
	paths := make([]string, 0, len(c.exports))
	for _, e := range c.exports {
		paths = append(paths, e.ExportPath)
	}
	return paths
}

// A 404 from something in front of the array -- a proxy or ingress
// named in apiUrl, or a route that changed after the PV was made -- used to
// read as "the volume is gone": DeleteVolume answered OK having touched
// nothing, Kubernetes dropped the PV, and the directory, quota and export
// leaked; expand was told the volume was gone. Only the cluster's own file
// not-found proves that. Anything else is Unavailable, naming the endpoint,
// with nothing changed.
func TestA404ThatIsNotAFileNotFoundDeletesNothingAndIsUnavailable(t *testing.T) {
	var arrayWrites int
	proxy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			arrayWrites++
		}
		// What nginx / an ingress answers for a route it no longer has.
		http.Error(w, "404 page not found", http.StatusNotFound)
	}))
	defer proxy.Close()

	srv := NewControllerServer(tlsDriverFor(t, proxy))
	handle := newVolumeID("10.0.0.5", proxy.URL, "/csi-volumes/pvc-live").String()

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: handle, Secrets: testSecrets(),
	})
	require.Equal(t, codes.Unavailable, status.Code(err), "message: %v", err)
	assert.Contains(t, err.Error(), proxy.URL, "the message names the endpoint")
	assert.Zero(t, arrayWrites, "nothing was deleted")

	_, err = srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: handle, Secrets: testSecrets(),
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB},
	})
	assert.Equal(t, codes.Unavailable, status.Code(err), "expand is not told the volume is gone")
}

// The other half: the cluster's own not-found still means gone, so a
// retried DeleteVolume of a deleted volume answers OK, as CSI requires.
func TestAFileNotFoundStillMeansTheVolumeIsGone(t *testing.T) {
	array := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error_class": "fs_no_such_entry_error", "description": "no such file",
		})
	}))
	defer array.Close()

	d := tlsDriverFor(t, array)
	srv := NewControllerServer(d)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: newVolumeID("10.0.0.5", array.URL, "/csi-volumes/pvc-gone").String(), Secrets: testSecrets(),
	})
	require.NoError(t, err)
}

// once a volume's export has been found, the next lookup reads it
// by ID instead of listing every export on the array -- and still sees an
// export that has gone.
func TestAnExportLookupReadsTheKnownExportByID(t *testing.T) {
	cluster := newFakeCluster()
	cluster.directories["/csi-volumes"] = "10"
	server := httptest.NewServer(cluster.handler())
	defer server.Close()
	client := qumulo.New(server.URL, "t")
	cluster.exports["e1"] = fakeExport{ExportPath: "/pvc-a", FSPath: "/csi-volumes/pvc-a", Description: exportDescriptionFor("pvc-a")}

	first, err := findExportByFSPath(context.Background(), client, "/csi-volumes/pvc-a")
	require.NoError(t, err)
	require.NotNil(t, first)
	listings := cluster.exportListings

	again, err := findExportByFSPath(context.Background(), client, "/csi-volumes/pvc-a")
	require.NoError(t, err)
	require.NotNil(t, again)
	assert.Equal(t, listings, cluster.exportListings, "the second lookup must not list the export table")

	delete(cluster.exports, "e1")
	gone, err := findExportByFSPath(context.Background(), client, "/csi-volumes/pvc-a")
	require.NoError(t, err)
	assert.Nil(t, gone, "a removed export must not be answered from the hint")
}

// /: the export CreateVolume just made is remembered, so the volume's
// next lookup -- rollback, expand, every replication RPC -- reads it by ID
// instead of listing the export table once more.
func TestTheExportCreateVolumeMadeIsReadByID(t *testing.T) {
	cluster := newFakeCluster()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-hinted",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":            testServer,
			"storeRealPath":     "/csi-volumes",
			"storeExportPath":   "/csi",
			"provisioningModel": "dedicated-export", "allowAnyClient": "true",
		},
	})
	require.NoError(t, err)
	require.Len(t, cluster.exports, 1)
	listings := cluster.exportListings

	found, err := findExportByFSPath(context.Background(), qumulo.New(server.URL, "t"), "/csi-volumes/pvc-hinted")
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, listings, cluster.exportListings, "the lookup after create must not list the export table")
}

// /. A shared-subdirectory volume has no export of its own, and a
// group's root is found by its covering export, so every replication Info
// poll of either listed the whole export table. Info -- which only reports --
// now trusts "no export at this path" for a few minutes and reads the
// covering export it found last by ID. Every other RPC still lists, so an
// export created meanwhile is seen at once.
func TestInfoLookupsDoNotRelistTheExportTable(t *testing.T) {
	cluster := newFakeCluster()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()
	client := qumulo.New(server.URL, "t")
	cluster.exports["e1"] = fakeExport{ExportPath: "/shared", FSPath: "/csi-shared", Description: "the class's covering export"}
	info := withReportLookups(context.Background())

	for i := 0; i < 2; i++ {
		own, err := findExportByFSPath(info, client, "/csi-shared/pvc-a")
		require.NoError(t, err)
		require.Nil(t, own)
		covering, err := findCoveringExport(info, client, "/csi-shared/pvc-a")
		require.NoError(t, err)
		require.NotNil(t, covering)
		assert.Equal(t, "/csi-shared", covering.FSPath)
	}
	assert.Equal(t, 2, cluster.exportListings, "one listing for each question, the first time only")

	// A mutating RPC does not trust the negative answer.
	_, err := findExportByFSPath(context.Background(), client, "/csi-shared/pvc-a")
	require.NoError(t, err)
	assert.Equal(t, 3, cluster.exportListings)

	// An export this driver creates there is seen by the next report at once.
	rememberExport(client, "/csi-shared/pvc-a", "e9")
	cluster.exports["e9"] = fakeExport{ExportPath: "/pvc-a", FSPath: "/csi-shared/pvc-a"}
	own, err := findExportByFSPath(info, client, "/csi-shared/pvc-a")
	require.NoError(t, err)
	assert.NotNil(t, own)

	// A covering export that went away is not answered from the hint.
	delete(cluster.exports, "e1")
	delete(cluster.exports, "e9")
	gone, err := findCoveringExport(info, client, "/csi-shared/pvc-a")
	require.NoError(t, err)
	assert.Nil(t, gone)
}
