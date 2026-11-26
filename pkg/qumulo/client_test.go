// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthCheck(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string
	}{
		{
			name: "api reachable",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"revision_id":"Qumulo Core 7.2.0","build_id":"12345"}`))
			},
		},
		{
			name: "non-200 status",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantErr: "HTTP 500",
		},
		{
			name: "unauthorized",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
			},
			wantErr: "HTTP 401",
		},
		{
			name: "invalid json body",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("not json"))
			},
			wantErr: "decoding",
		},
		{
			name: "wrong path",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.NotFound(w, r)
			},
			wantErr: "HTTP 404",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient(t, tt.handler, "")

			err := client.HealthCheck(context.Background())

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestHealthCheckSetsAuthorizationHeader(t *testing.T) {
	var gotAuth string

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"revision_id":"Qumulo Core 7.2.0"}`))
	}, "my-token")

	require.NoError(t, client.HealthCheck(context.Background()))
	assert.Equal(t, "Bearer my-token", gotAuth)
}

func TestHealthCheckOmitsAuthorizationHeaderWhenNoToken(t *testing.T) {
	var gotAuth string

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"revision_id":"Qumulo Core 7.2.0"}`))
	}, "")

	require.NoError(t, client.HealthCheck(context.Background()))
	assert.Empty(t, gotAuth)
}

func TestHealthCheckRespectsContextCancellation(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	err := client.HealthCheck(ctx)
	assert.Error(t, err)
}

func TestHealthCheckUnreachableServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := server.URL
	server.Close() // nothing listening anymore

	client := New(addr, "")

	err := client.HealthCheck(context.Background())
	assert.Error(t, err)
}

// newTestClient points a real Client at an httptest server running handler.
func newTestClient(t *testing.T, handler http.HandlerFunc, token string) *Client {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return New(server.URL, token)
}

// recordingHandler captures the method, path and decoded body of the request
// it serves, then replies with status and responseBody.
type recordingHandler struct {
	status       int
	responseBody string

	gotMethod string
	gotPath   string
	gotBody   map[string]any
}

func (h *recordingHandler) serve(w http.ResponseWriter, r *http.Request) {
	h.gotMethod = r.Method
	h.gotPath = r.URL.Path
	// A body is optional; failing to decode simply leaves gotBody nil.
	_ = json.NewDecoder(r.Body).Decode(&h.gotBody)

	w.WriteHeader(h.status)
	if h.responseBody != "" {
		w.Write([]byte(h.responseBody))
	}
}

func TestCreateDirectory(t *testing.T) {
	handler := &recordingHandler{
		status:       http.StatusOK,
		responseBody: `{"id":"12884901888","path":"/csi-volumes/pvc-abc","type":"FS_FILE_TYPE_DIRECTORY"}`,
	}
	client := newTestClient(t, handler.serve, "")

	info, err := client.CreateDirectory(context.Background(), "/csi-volumes", "pvc-abc")

	require.NoError(t, err)
	assert.Equal(t, "12884901888", info.ID)
	assert.True(t, info.IsDirectory())

	assert.Equal(t, http.MethodPost, handler.gotMethod)
	// The parent path is percent-encoded into the file-reference position.
	assert.Contains(t, handler.gotPath, "/files/")
	assert.Equal(t, "pvc-abc", handler.gotBody["name"])
	assert.Equal(t, "CREATE_DIRECTORY", handler.gotBody["action"])
}

func TestCreateDirectoryConflictIsTyped(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"description":"name already exists"}`))
	}, "")

	_, err := client.CreateDirectory(context.Background(), "/csi-volumes", "pvc-abc")

	require.Error(t, err)
	assert.True(t, IsConflict(err), "expected a conflict error, got %v", err)
	assert.False(t, IsNotFound(err))
	assert.Contains(t, err.Error(), "name already exists")
}

func TestLookupFileByPathNotFoundIsTyped(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}, "")

	_, err := client.LookupFileByPath(context.Background(), "/csi-volumes/missing")

	require.Error(t, err)
	assert.True(t, IsNotFound(err), "expected a not-found error, got %v", err)
	assert.False(t, IsConflict(err))
}

func TestStartTreeDelete(t *testing.T) {
	handler := &recordingHandler{status: http.StatusAccepted}
	client := newTestClient(t, handler.serve, "")

	require.NoError(t, client.StartTreeDelete(context.Background(), "12884901888"))

	// The file ID travels in the body, not the path: the job collection
	// endpoint takes no ID segment.
	assert.Equal(t, http.MethodPost, handler.gotMethod)
	assert.Equal(t, "/v1/tree-delete/jobs/", handler.gotPath)
	assert.Equal(t, "12884901888", handler.gotBody["id"])
}

func TestStartTreeDeleteAlreadyStartedIsSuccess(t *testing.T) {
	// Verbatim from Core 7.9.3, on the CO's retry after DeleteVolume answered
	// Aborted: the job asked for is already running.
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"module":"qinternal.tree_delete","error_class":"tree_delete_already_started_error",` +
			`"description":"Tree delete job already started for root (5001183)"}`))
	}, "")

	require.NoError(t, client.StartTreeDelete(context.Background(), "5001183"))
}

func TestStartTreeDeleteSurfacesOtherServerErrors(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error_class":"some_other_error","description":"boom"}`))
	}, "")

	err := client.StartTreeDelete(context.Background(), "1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")
}

func TestTreeDeleteFinished(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		body         string
		wantFinished bool
	}{
		{
			name:         "job still running",
			status:       http.StatusOK,
			body:         `{"id":"12884901888","remaining_files":"42"}`,
			wantFinished: false,
		},
		{
			// The cluster 404s both a completed job and one that never
			// existed, and both mean the tree is gone. The body is the
			// cluster's own, which is what tells this 404 from a proxy's.
			// The class is what a real array sends (measured on
			// the lab array): the generic one, not a job-specific one.
			name:         "job finished",
			status:       http.StatusNotFound,
			body:         `{"error_class":"http_not_found_error","description":"Not found"}`,
			wantFinished: true,
		},
		{
			// Any Qumulo class counts. A check narrowed to one class would
			// read every finished job as running the day the array sent
			// another, and every DeleteVolume would poll to its timeout.
			name:         "job finished, another class",
			status:       http.StatusNotFound,
			body:         `{"error_class":"fs_no_such_entry_error","description":"Not found"}`,
			wantFinished: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/v1/tree-delete/jobs/12884901888", r.URL.Path)
				w.WriteHeader(tt.status)
				w.Write([]byte(tt.body))
			}, "")

			finished, err := client.TreeDeleteFinished(context.Background(), "12884901888")

			require.NoError(t, err)
			assert.Equal(t, tt.wantFinished, finished)
		})
	}
}

func TestTreeDeleteFinishedSurfacesRealErrors(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}, "")

	_, err := client.TreeDeleteFinished(context.Background(), "1")

	// A 500 must not be mistaken for "finished".
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")
}

func TestGetCapacity(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/file-system", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"block_size_bytes":4096,"total_size_bytes":"162569281536","free_size_bytes":"162234798080","snapshot_size_bytes":"0"}`))
	}, "")

	capacity, err := client.GetCapacity(context.Background())

	require.NoError(t, err)
	// Sizes arrive as decimal strings because they exceed 53 bits.
	assert.Equal(t, uint64(162569281536), capacity.TotalSizeBytes)
	assert.Equal(t, uint64(162234798080), capacity.FreeSizeBytes)
}

func TestGetCapacityRejectsUnparseableSize(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "total", body: `{"total_size_bytes":"not-a-number","free_size_bytes":"1"}`},
		{name: "free", body: `{"total_size_bytes":"1","free_size_bytes":"not-a-number"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(tt.body))
			}, "")

			_, err := client.GetCapacity(context.Background())

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.name+"_size_bytes")
		})
	}
}

func TestGetCapacitySurfacesRealErrors(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}, "")

	_, err := client.GetCapacity(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")
}

func TestQuotaRoundTrip(t *testing.T) {
	handler := &recordingHandler{
		status:       http.StatusOK,
		responseBody: `{"id":"12884901888","limit":"10737418240"}`,
	}
	client := newTestClient(t, handler.serve, "")

	limit, err := client.GetQuota(context.Background(), "12884901888")

	require.NoError(t, err)
	// Quota limits arrive as decimal strings because they exceed 53 bits.
	assert.Equal(t, uint64(10737418240), limit)
}

func TestGetQuotaRejectsUnparseableLimit(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"1","limit":"not-a-number"}`))
	}, "")

	_, err := client.GetQuota(context.Background(), "1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing quota limit")
}

func TestCreateQuotaSendsLimitAsString(t *testing.T) {
	handler := &recordingHandler{status: http.StatusOK}
	client := newTestClient(t, handler.serve, "")

	require.NoError(t, client.CreateQuota(context.Background(), "12884901888", 10737418240))

	assert.Equal(t, http.MethodPost, handler.gotMethod)
	assert.Equal(t, "12884901888", handler.gotBody["id"])
	assert.Equal(t, "10737418240", handler.gotBody["limit"])
}

func TestUpdateQuota(t *testing.T) {
	handler := &recordingHandler{status: http.StatusOK}
	client := newTestClient(t, handler.serve, "")

	require.NoError(t, client.UpdateQuota(context.Background(), "12884901888", 20))

	assert.Equal(t, http.MethodPut, handler.gotMethod)
	assert.Equal(t, "20", handler.gotBody["limit"])
}

func TestDeleteQuota(t *testing.T) {
	handler := &recordingHandler{status: http.StatusNoContent}
	client := newTestClient(t, handler.serve, "")

	require.NoError(t, client.DeleteQuota(context.Background(), "12884901888"))
	assert.Equal(t, http.MethodDelete, handler.gotMethod)
}

func TestCreateSnapshot(t *testing.T) {
	handler := &recordingHandler{
		status:       http.StatusOK,
		responseBody: `{"id":42,"name":"42_snap-1","source_file_id":"12884901888","timestamp":"2026-08-19T10:30:00.000000000Z"}`,
	}
	client := newTestClient(t, handler.serve, "")

	snapshot, err := client.CreateSnapshot(context.Background(), "12884901888", "snap-1")

	require.NoError(t, err)
	assert.Equal(t, "42", snapshot.ID)
	// The cluster prepends the snapshot ID to the requested suffix.
	assert.Equal(t, "42_snap-1", snapshot.Name)
	assert.Equal(t, "12884901888", snapshot.SourceFileID)
	assert.Equal(t, 2026, snapshot.CreatedAt.Year())

	assert.Equal(t, "12884901888", handler.gotBody["source_file_id"])
	assert.Equal(t, "snap-1", handler.gotBody["name_suffix"])

	// expiration must be sent explicitly, as an empty string meaning "never".
	// Qumulo Core rejects the request when the field is absent and again when
	// it is null, so its mere presence is the thing under test -- a fake
	// server that accepts anything let this ship broken once already.
	expiration, present := handler.gotBody["expiration"]
	assert.True(t, present, "expiration must be present; the cluster rejects a request without it")
	assert.Equal(t, "", expiration, "expiration must be empty so the snapshot never expires")
}

func TestCreateSnapshotToleratesUnparseableTimestamp(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":42,"name":"42_snap-1","source_file_id":"1","timestamp":"nonsense"}`))
	}, "")

	snapshot, err := client.CreateSnapshot(context.Background(), "1", "snap-1")

	// The snapshot is still usable; only its creation time is unknown.
	require.NoError(t, err)
	assert.Equal(t, "42", snapshot.ID)
	assert.True(t, snapshot.CreatedAt.IsZero())
}

func TestListSnapshots(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"entries":[
			{"id":42,"name":"42_snap-1","source_file_id":"100","timestamp":"2026-08-19T10:30:00Z"},
			{"id":43,"name":"43_snap-2","source_file_id":"200","timestamp":"2026-08-19T11:30:00Z"}
		]}`))
	}, "")

	snapshots, err := client.ListSnapshots(context.Background())

	require.NoError(t, err)
	require.Len(t, snapshots, 2)
	assert.Equal(t, "42_snap-1", snapshots[0].Name)
	assert.Equal(t, "200", snapshots[1].SourceFileID)
}

func TestListSnapshotsEmpty(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"entries":[]}`))
	}, "")

	snapshots, err := client.ListSnapshots(context.Background())

	require.NoError(t, err)
	assert.Empty(t, snapshots)
}

func TestGetSnapshotNotFoundIsTyped(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}, "")

	_, err := client.GetSnapshot(context.Background(), "999")

	require.Error(t, err)
	assert.True(t, IsNotFound(err))
}

func TestDeleteSnapshot(t *testing.T) {
	handler := &recordingHandler{status: http.StatusNoContent}
	client := newTestClient(t, handler.serve, "")

	require.NoError(t, client.DeleteSnapshot(context.Background(), "42"))
	assert.Equal(t, http.MethodDelete, handler.gotMethod)
	assert.Contains(t, handler.gotPath, "42")
}

func TestAPIErrorWithoutJSONBody(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("<html>gateway error</html>"))
	}, "")

	err := client.StartTreeDelete(context.Background(), "1")

	// A proxy error with no JSON envelope must still surface the status.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 502")
	assert.False(t, IsNotFound(err))
}

// TestEndpointPaths pins the method and path of every ClientInterface call
// against the Qumulo REST API Guide.
//
// This exists because the integration tests drive a fake cluster that serves
// whatever the client asks for -- a permissive fake absorbs a wrong path
// silently, which is exactly how an earlier "/api/v1/..." prefix survived a
// green test suite while being unable to reach a real cluster.
func TestEndpointPaths(t *testing.T) {
	tests := []struct {
		name       string
		call       func(*Client) error
		wantMethod string
		wantPath   string
		stub       string
	}{
		{
			name:       "HealthCheck",
			stub:       `{"revision_id":"x"}`,
			call:       func(c *Client) error { return c.HealthCheck(context.Background()) },
			wantMethod: http.MethodGet,
			wantPath:   "/v1/version",
		},
		{
			name: "CreateDirectory",
			stub: `{"id":"100","path":"/csi-volumes/pvc-abc","type":"FS_FILE_TYPE_DIRECTORY"}`,
			call: func(c *Client) error {
				_, err := c.CreateDirectory(context.Background(), "/csi-volumes", "pvc-abc")
				return err
			},
			wantMethod: http.MethodPost,
			// The parent path is percent-encoded, so the decoded path the
			// server sees re-expands the separator.
			wantPath: "/v1/files//csi-volumes/entries/",
		},
		{
			name: "LookupFileByPath",
			stub: `{"id":"100","path":"/csi-volumes/pvc-abc","type":"FS_FILE_TYPE_DIRECTORY"}`,
			call: func(c *Client) error {
				_, err := c.LookupFileByPath(context.Background(), "/csi-volumes/pvc-abc")
				return err
			},
			wantMethod: http.MethodGet,
			wantPath:   "/v1/files//csi-volumes/pvc-abc/info/attributes",
		},
		{
			name: "ResolveFileIDs",
			stub: `[{"id":"100","path":"/csi-volumes/pvc-abc"}]`,
			call: func(c *Client) error {
				_, err := c.ResolveFileIDs(context.Background(), []string{"100"})
				return err
			},
			wantMethod: http.MethodPost,
			wantPath:   "/v1/files/resolve",
		},
		{
			name:       "StartTreeDelete",
			call:       func(c *Client) error { return c.StartTreeDelete(context.Background(), "100") },
			wantMethod: http.MethodPost,
			wantPath:   "/v1/tree-delete/jobs/",
		},
		{
			name: "TreeDeleteFinished",
			stub: `{"id":"100"}`,
			call: func(c *Client) error {
				_, err := c.TreeDeleteFinished(context.Background(), "100")
				return err
			},
			wantMethod: http.MethodGet,
			wantPath:   "/v1/tree-delete/jobs/100",
		},
		{
			name:       "CreateQuota",
			call:       func(c *Client) error { return c.CreateQuota(context.Background(), "100", 1) },
			wantMethod: http.MethodPost,
			wantPath:   "/v1/files/quotas/",
		},
		{
			// Two GETs: the quota object for the limit, then the status
			// document for the usage. The table records the last request, so
			// the status path is what lands here; TestGetQuotaInfoReadsBoth
			// pins the order and the pair.
			name: "GetQuota",
			stub: `{"id":"100","limit":"1"}`,
			call: func(c *Client) error {
				_, err := c.GetQuota(context.Background(), "100")
				return err
			},
			wantMethod: http.MethodGet,
			wantPath:   "/v1/files/quotas/status/100",
		},
		{
			name:       "UpdateQuota",
			call:       func(c *Client) error { return c.UpdateQuota(context.Background(), "100", 1) },
			wantMethod: http.MethodPut,
			wantPath:   "/v1/files/quotas/100",
		},
		{
			name:       "DeleteQuota",
			call:       func(c *Client) error { return c.DeleteQuota(context.Background(), "100") },
			wantMethod: http.MethodDelete,
			wantPath:   "/v1/files/quotas/100",
		},
		{
			name: "CreateSnapshot",
			stub: `{"id":42,"name":"42_snap-1","source_file_id":"100"}`,
			call: func(c *Client) error {
				_, err := c.CreateSnapshot(context.Background(), "100", "snap-1")
				return err
			},
			wantMethod: http.MethodPost,
			wantPath:   "/v3/snapshots/",
		},
		{
			name: "ListSnapshots",
			stub: `{"entries":[]}`,
			call: func(c *Client) error {
				_, err := c.ListSnapshots(context.Background())
				return err
			},
			wantMethod: http.MethodGet,
			wantPath:   "/v3/snapshots/",
		},
		{
			name: "GetSnapshot",
			stub: `{"id":42,"name":"42_snap-1","source_file_id":"100"}`,
			call: func(c *Client) error {
				_, err := c.GetSnapshot(context.Background(), "42")
				return err
			},
			wantMethod: http.MethodGet,
			wantPath:   "/v3/snapshots/42",
		},
		{
			name:       "DeleteSnapshot",
			call:       func(c *Client) error { return c.DeleteSnapshot(context.Background(), "42") },
			wantMethod: http.MethodDelete,
			wantPath:   "/v3/snapshots/42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMethod, gotPath string

			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotPath = r.Method, r.URL.Path
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(tt.stub))
			}, "")

			require.NoError(t, tt.call(client))

			assert.Equal(t, tt.wantMethod, gotMethod)
			assert.Equal(t, tt.wantPath, gotPath)
			assert.NotContains(t, gotPath, "/api/", "Qumulo paths carry no /api prefix")
		})
	}
}

func TestListSnapshotsExcludesInDelete(t *testing.T) {
	var gotQuery string

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"entries":[]}`))
	}, "")

	_, err := client.ListSnapshots(context.Background())

	// The API marks filter required, and in-delete snapshots must not be
	// adopted as idempotent hits.
	require.NoError(t, err)
	assert.Equal(t, "filter=exclude_in_delete", gotQuery)
}

func TestSnapshotIDDecodesFromBareNumber(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Snapshot IDs come back as bare JSON numbers, unlike file and
		// quota IDs, which are strings.
		w.Write([]byte(`{"id":42,"name":"42_snap-1","source_file_id":"100","timestamp":"2026-08-19T10:30:00Z"}`))
	}, "")

	snapshot, err := client.CreateSnapshot(context.Background(), "100", "snap-1")

	require.NoError(t, err)
	assert.Equal(t, "42", snapshot.ID)
	assert.Equal(t, "100", snapshot.SourceFileID)
}

func TestSnapshotIDDecodesFromQuotedNumber(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"42","name":"42_snap-1","source_file_id":"100"}`))
	}, "")

	snapshot, err := client.GetSnapshot(context.Background(), "42")

	require.NoError(t, err)
	assert.Equal(t, "42", snapshot.ID)
}

func TestCreateSnapshotSendsNameSuffix(t *testing.T) {
	handler := &recordingHandler{
		status:       http.StatusOK,
		responseBody: `{"id":42,"name":"42_snap-1","source_file_id":"100"}`,
	}
	client := newTestClient(t, handler.serve, "")

	snapshot, err := client.CreateSnapshot(context.Background(), "100", "snap-1")

	require.NoError(t, err)
	// The API field is name_suffix; sending "name" would be ignored.
	assert.Equal(t, "snap-1", handler.gotBody["name_suffix"])
	assert.NotContains(t, handler.gotBody, "name")
	// And the cluster prepends the ID, so the stored name is not the suffix.
	assert.Equal(t, "42_snap-1", snapshot.Name)
}

func TestListSnapshotsReportsInDelete(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"entries":[{"id":42,"name":"42_snap-1","source_file_id":"100","in_delete":true}]}`))
	}, "")

	snapshots, err := client.ListSnapshots(context.Background())

	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	assert.True(t, snapshots[0].InDelete)
}

func TestResolveFileIDs(t *testing.T) {
	var gotBody []string

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[
			{"id":"100","path":"/csi-volumes/pvc-abc"},
			{"id":"200","path":""}
		]`))
	}, "")

	paths, err := client.ResolveFileIDs(context.Background(), []string{"100", "200"})

	require.NoError(t, err)
	assert.Equal(t, []string{"100", "200"}, gotBody, "IDs are sent as a bare JSON array")
	assert.Equal(t, "/csi-volumes/pvc-abc", paths["100"])
	// An unresolvable ID is omitted rather than failing the batch.
	assert.NotContains(t, paths, "200")
}

func TestResolveFileIDsEmptyMakesNoCall(t *testing.T) {
	var called bool

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}, "")

	paths, err := client.ResolveFileIDs(context.Background(), nil)

	require.NoError(t, err)
	assert.Empty(t, paths)
	assert.False(t, called, "an empty batch should not hit the API")
}

func TestTokenIsReadPerRequest(t *testing.T) {
	var seen []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"revision_id":"x"}`))
	}))
	defer server.Close()

	token := "first"
	client := NewWithTokenFunc(server.URL, func() (string, error) { return token, nil })

	require.NoError(t, client.HealthCheck(context.Background()))
	token = "rotated"
	require.NoError(t, client.HealthCheck(context.Background()))

	// A rotated Secret must take effect without rebuilding the client.
	assert.Equal(t, []string{"Bearer first", "Bearer rotated"}, seen)
}

func TestTokenErrorFailsRequest(t *testing.T) {
	client := NewWithTokenFunc("http://unused", func() (string, error) {
		return "", assert.AnError
	})

	err := client.HealthCheck(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "token")
}

// The mandatory-field assertion is the point of this test. Qumulo Core 7.9.3
// requires export_path, fs_path, description and restrictions to be *present*;
// an omitempty on any of them would make every create fail with a decode
// error, which is precisely how CreateSnapshot was broken before its
// expiration field was sent explicitly.
func TestCreateExportSendsEveryRequiredField(t *testing.T) {
	var body map[string]json.RawMessage
	var gotMethod, gotPath string

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Write([]byte(`{"id":"2","export_path":"/csi/pvc-a","fs_path":"/vol/pvc-a"}`))
	}, "token")

	created, err := client.CreateExport(context.Background(), ExportSpec{ExportPath: "/csi/pvc-a", FSPath: "/vol/pvc-a"})

	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "/v3/nfs/exports/", gotPath)
	assert.Equal(t, ExportInfo{ID: "2", ExportPath: "/csi/pvc-a", FSPath: "/vol/pvc-a"}, created)

	for _, field := range []string{"export_path", "fs_path", "description", "restrictions"} {
		assert.Contains(t, body, field, "%s must be present even when empty", field)
	}
	// An empty description is still sent, as an empty string.
	assert.JSONEq(t, `""`, string(body["description"]))
	// host_restrictions must be [] rather than null, the field being required.
	assert.Contains(t, string(body["restrictions"]), `"host_restrictions":[]`)
}

// v3 answers a listing with the entries envelope, unlike v2's bare array.
func TestListExports(t *testing.T) {
	var gotPath string

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte(`{"entries":[
			{"id":"1","export_path":"/","fs_path":"/"},
			{"id":"2","export_path":"/csi/pvc-a","fs_path":"/vol/pvc-a"}
		]}`))
	}, "token")

	exports, err := client.ListExports(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "/v3/nfs/exports/", gotPath)
	require.Len(t, exports, 2)
	assert.Equal(t, "1", exports[0].ID)
	assert.Equal(t, "/vol/pvc-a", exports[1].FSPath)
}

func TestListExportsEmpty(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"entries":[]}`))
	}, "token")

	exports, err := client.ListExports(context.Background())

	require.NoError(t, err)
	assert.Empty(t, exports)
}

func TestDeleteExport(t *testing.T) {
	var gotMethod, gotPath string

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.WriteHeader(http.StatusOK)
	}, "token")

	err := client.DeleteExport(context.Background(), "2")

	require.NoError(t, err)
	assert.Equal(t, http.MethodDelete, gotMethod)
	assert.Equal(t, "/v3/nfs/exports/2", gotPath)
}

// A duplicate export_path is a 409 carrying its own error class, and callers
// depend on IsConflict recognizing it to stay idempotent.
func TestCreateExportConflictIsRecognized(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"error_class":"nfs_export_already_exists_error","description":"Export path already exists."}`))
	}, "token")

	_, err := client.CreateExport(context.Background(), ExportSpec{ExportPath: "/csi/a", FSPath: "/vol/a"})

	require.Error(t, err)
	assert.True(t, IsConflict(err), "a duplicate export_path must satisfy IsConflict")
	assert.False(t, IsNotFound(err))
	assert.Equal(t, ErrorClassNFSExportAlreadyExists, ErrorClassOf(err))
}

// A missing fs_path arrives as 400, not 404, and must NOT look like "already
// gone": a caller treating it that way would report a failed create as success.
func TestCreateExportMissingFSPathIsNotNotFound(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error_class":"nfs_export_fs_path_doesnt_exist_error","description":"The directory does not exist."}`))
	}, "token")

	_, err := client.CreateExport(context.Background(), ExportSpec{ExportPath: "/csi/a", FSPath: "/vol/a"})

	require.Error(t, err)
	assert.False(t, IsNotFound(err), "a failed create must not be mistaken for an absent object")
	assert.False(t, IsConflict(err))
	assert.Equal(t, ErrorClassNFSExportFSPathNotFound, ErrorClassOf(err))
}

// The cluster renders a directory's path with a trailing slash from
// /v1/files/resolve and from nowhere else. Left alone, a resolved path never
// string-matches an NFS export's fs_path, so a volume's export is never found
// and never deleted -- which is exactly how this surfaced.
func TestResolveFileIDsStripsTrailingSlash(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[
			{"id":"100","path":"/csi/volumes/pvc-a/"},
			{"id":"101","path":"/csi/volumes/pvc-b"},
			{"id":"102","path":"/"},
			{"id":"103","path":""}
		]`))
	}, "token")

	paths, err := client.ResolveFileIDs(context.Background(), []string{"100", "101", "102", "103"})

	require.NoError(t, err)
	assert.Equal(t, "/csi/volumes/pvc-a", paths["100"], "a trailing slash must be stripped")
	assert.Equal(t, "/csi/volumes/pvc-b", paths["101"], "a path without one is unchanged")
	assert.Equal(t, "/", paths["102"], "the root must stay the root")
	assert.NotContains(t, paths, "103", "an unresolved ID stays absent")
}

// policy_id is what distinguishes a scheduled snapshot from one the driver
// created, and the API renders it as null for the latter. Decoding null into a
// json.Number leaves it empty, which is the wanted result -- this test pins
// that behavior so a change of field type cannot quietly break the
// distinction.
func TestListSnapshotsDecodesPolicyID(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"entries":[
			{"id":42,"name":"42_snap-1","source_file_id":"100","timestamp":"2026-08-21T00:00:00Z","policy_id":null},
			{"id":43,"name":"43_hourly","source_file_id":"100","timestamp":"2026-08-21T01:00:00Z","policy_id":7},
			{"id":44,"name":"44_snap-2","source_file_id":"100","timestamp":"2026-08-21T02:00:00Z"},
			{"id":45,"name":"45_snap-3","source_file_id":"100","timestamp":"2026-08-21T03:00:00Z","policy_id":0}
		]}`))
	}, "token")

	snapshots, err := client.ListSnapshots(context.Background())

	require.NoError(t, err)
	require.Len(t, snapshots, 4)

	assert.Equal(t, "", snapshots[0].PolicyID, "null must decode to no policy")
	assert.False(t, snapshots[0].CreatedByPolicy())

	assert.Equal(t, "7", snapshots[1].PolicyID)
	assert.True(t, snapshots[1].CreatedByPolicy(), "a policy-created snapshot must be identifiable")

	assert.Equal(t, "", snapshots[2].PolicyID, "an absent field must decode to no policy")
	assert.False(t, snapshots[2].CreatedByPolicy())

	// A numeric zero means "unset". Reading it as a real policy would stop the
	// driver recognizing its own snapshot on a retry.
	assert.False(t, snapshots[3].CreatedByPolicy(), "policy_id 0 must count as no policy")
}

// The stored name is "<id>_<suffix>", so matching has to anchor on both parts.
func TestSnapshotInfoMatchesNameSuffix(t *testing.T) {
	tests := []struct {
		name     string
		snapshot SnapshotInfo
		suffix   string
		want     bool
	}{
		{
			name:     "exact stored form",
			snapshot: SnapshotInfo{ID: "16", Name: "16_snapshot-abc"},
			suffix:   "snapshot-abc", want: true,
		},
		{
			name:     "a name that merely ends with the suffix",
			snapshot: SnapshotInfo{ID: "16", Name: "16_nightly-snapshot-abc"},
			suffix:   "snapshot-abc", want: false,
		},
		{
			name:     "another snapshot's ID as the prefix",
			snapshot: SnapshotInfo{ID: "16", Name: "17_snapshot-abc"},
			suffix:   "snapshot-abc", want: false,
		},
		{
			name:     "the bare suffix, which no cluster stores",
			snapshot: SnapshotInfo{ID: "16", Name: "snapshot-abc"},
			suffix:   "snapshot-abc", want: false,
		},
		{
			name:     "a different suffix entirely",
			snapshot: SnapshotInfo{ID: "16", Name: "16_snapshot-abc"},
			suffix:   "snapshot-xyz", want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.snapshot.MatchesNameSuffix(tt.suffix))
		})
	}
}

// The mode is set by file ID, not by path: that is what the driver has after
// creating a directory, and it must not be percent-escaped the way a path
// reference is.
func TestSetFileAttributesByIDSetsTheModeAlone(t *testing.T) {
	var gotMethod, gotPath string
	var body map[string]any

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.EscapedPath()
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Write([]byte(`{"id":"12884901888","mode":"0777","type":"FS_FILE_TYPE_DIRECTORY"}`))
	}, "token")

	err := client.SetFileAttributesByID(context.Background(), "12884901888", FileAttributes{Mode: "0777"})

	require.NoError(t, err)
	assert.Equal(t, http.MethodPatch, gotMethod)
	assert.Equal(t, "/v1/files/12884901888/info/attributes", gotPath)
	// A partial update: sending anything else would overwrite ownership or
	// timestamps the caller did not ask to change.
	assert.Equal(t, map[string]any{"mode": "0777"}, body)
}

// The owner and group go as owner_details and group_details, an id_type
// and an id_value, in the same PATCH as the mode. Measured on Core 7.9.3:
// this body, sent with the driver's token, gave a directory the NFS owner
// replication needs.
func TestSetFileAttributesByIDSendsNFSOwnership(t *testing.T) {
	var body map[string]any

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Write([]byte(`{}`))
	}, "token")

	err := client.SetFileAttributesByID(context.Background(), "7", FileAttributes{
		Mode:         "0777",
		OwnerDetails: &Identity{IDType: IDTypeNFSUID, IDValue: "0"},
		GroupDetails: &Identity{IDType: IDTypeNFSGID, IDValue: "0"},
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"mode":          "0777",
		"owner_details": map[string]any{"id_type": "NFS_UID", "id_value": "0"},
		"group_details": map[string]any{"id_type": "NFS_GID", "id_value": "0"},
	}, body)
}

func TestSetFileAttributesByIDSendsNothingForAnEmptyUpdate(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
	}, "token")

	require.NoError(t, client.SetFileAttributesByID(context.Background(), "7", FileAttributes{}))
}

func TestSetFileAttributesByIDSurfacesAPIErrors(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_class":"fs_no_such_inode_error","description":"does not exist"}`))
	}, "token")

	err := client.SetFileAttributesByID(context.Background(), "999", FileAttributes{Mode: "0777"})

	require.Error(t, err)
	assert.True(t, IsNotFound(err))
}

// The attribute endpoint renders the owner and group by kind as well as by
// auth ID, and the lookup carries them: the driver re-owns only what is
// local.
func TestLookupFileByPathReadsOwnerAndGroupDetails(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"id":"5","path":"/csi/v/","type":"FS_FILE_TYPE_DIRECTORY","owner":"500",` +
			`"owner_details":{"id_type":"LOCAL_USER","id_value":"500"},"group":"513",` +
			`"group_details":{"id_type":"LOCAL_GROUP","id_value":"513"}}`))
	}, "token")

	info, err := client.LookupFileByPath(context.Background(), "/csi/v")

	require.NoError(t, err)
	assert.Equal(t, Identity{IDType: IDTypeLocalUser, IDValue: "500"}, info.Owner)
	assert.Equal(t, Identity{IDType: IDTypeLocalGroup, IDValue: "513"}, info.Group)
	assert.True(t, info.Owner.IsLocal())
	assert.True(t, info.Group.IsLocal())
	assert.False(t, Identity{IDType: IDTypeNFSUID, IDValue: "0"}.IsLocal())
}

// tenant_id must be omitted, not sent as 0: the cluster rejects tenant 0, so
// sending it would break every caller that does not care which tenant it lands
// in -- which is every caller that existed before the field did.
func TestCreateExportOmitsTenantWhenUnset(t *testing.T) {
	var body map[string]json.RawMessage

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Write([]byte(`{"id":"2","export_path":"/csi/a","fs_path":"/vol/a","tenant_id":1}`))
	}, "token")

	created, err := client.CreateExport(context.Background(), ExportSpec{ExportPath: "/csi/a", FSPath: "/vol/a"})
	require.NoError(t, err)

	assert.NotContains(t, body, "tenant_id", "an unset tenant must not appear on the wire at all")
	assert.Equal(t, 1, created.TenantID, "the cluster's own default is read back")

	// And the allowlist stays empty rather than becoming null.
	var restrictions []map[string]any
	require.NoError(t, json.Unmarshal(body["restrictions"], &restrictions))
	require.Len(t, restrictions, 1)
	assert.Equal(t, []any{}, restrictions[0]["host_restrictions"])
}

// The other direction: both fields present and correct when asked for.
func TestCreateExportSendsTenantAndAllowedClients(t *testing.T) {
	var body map[string]json.RawMessage

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Write([]byte(`{"id":"3","export_path":"/csi/b","fs_path":"/vol/b","tenant_id":4}`))
	}, "token")

	created, err := client.CreateExport(context.Background(), ExportSpec{
		ExportPath:     "/csi/b",
		FSPath:         "/vol/b",
		TenantID:       4,
		AllowedClients: []string{"10.0.0.5", "192.168.1.0/24"},
	})
	require.NoError(t, err)

	assert.JSONEq(t, `4`, string(body["tenant_id"]))
	assert.Equal(t, 4, created.TenantID)

	var restrictions []map[string]any
	require.NoError(t, json.Unmarshal(body["restrictions"], &restrictions))
	require.Len(t, restrictions, 1, "one restriction block carries the whole allowlist")
	assert.Equal(t, []any{"10.0.0.5", "192.168.1.0/24"}, restrictions[0]["host_restrictions"])
	// The rest of the block must keep the defaults the driver has always sent.
	assert.Equal(t, false, restrictions[0]["read_only"])
	assert.Equal(t, "NFS_MAP_NONE", restrictions[0]["user_mapping"])
}

func TestListTenants(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/multitenancy/tenants/", r.URL.Path)
		w.Write([]byte(`{"entries":[
			{"id":1,"name":"Default","nfs_enabled":true},
			{"id":7,"name":"smb-only","nfs_enabled":false}]}`))
	}, "token")

	tenants, err := client.ListTenants(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []TenantInfo{
		{ID: 1, Name: "Default", NFSEnabled: true},
		{ID: 7, Name: "smb-only", NFSEnabled: false},
	}, tenants)
}

// The payload is the one the two-tenant simnode returned from GET /v3/network
// on 2026-09-22, trimmed to the fields the driver reads.
func TestListNetworks(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v3/network", r.URL.Path)
		w.Write([]byte(`{
			"managed_interfaces": null,
			"frontend_networks": [
				{"id":1,"name":"default-net","tenant_id":1,
				 "addresses":{"type":"HOST","host_addresses":{
				   "floating_ip_ranges":["198.51.100.50"],"netmask":"255.255.0.0"}}},
				{"id":2,"name":"pod1-net","tenant_id":3,
				 "addresses":{"type":"HOST","host_addresses":{
				   "floating_ip_ranges":["203.0.113.60"],"netmask":"255.255.0.0"}}}
			]}`))
	}, "token")

	networks, err := client.ListNetworks(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []NetworkInfo{
		{ID: 1, Name: "default-net", TenantID: 1, AddressType: "HOST",
			FloatingAddresses: []string{"198.51.100.50"}},
		{ID: 2, Name: "pod1-net", TenantID: 3, AddressType: "HOST",
			FloatingAddresses: []string{"203.0.113.60"}},
	}, networks)
}

// The live DHCP cluster (xx.xxx.xx.xx, Core 7.10.0) reports its one frontend
// network this way: DHCP addressing with no floating ranges configured. Its
// nodes are reachable, at their own addresses, which this call cannot see --
// so "no floating ranges" has to read as unknown, not as a tenant with no
// addresses, or every volume on that cluster would be refused.
func TestListNetworksOnADHCPClusterWithNoFloatingRanges(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"managed_interfaces":{"frontend_bond_config":{"interface_name":"bond0"}},
			"frontend_networks":[{"id":1,"name":"Default-frontend","tenant_id":1,
			 "addresses":{"type":"DHCP","dhcp_addresses":{"floating_ip_ranges":[],"netmask":null}}}]}`))
	}, "token")

	networks, err := client.ListNetworks(context.Background())
	require.NoError(t, err)

	require.Len(t, networks, 1)
	assert.Equal(t, "DHCP", networks[0].AddressType)
	assert.Empty(t, networks[0].FloatingAddresses)
}

// A DHCP network that *does* configure floating ranges spells them the same
// way HOST does, so they are read.
func TestListNetworksReadsFloatingRangesFromAnySchemeThatHasThem(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"frontend_networks":[
			{"id":1,"tenant_id":1,"addresses":{"type":"DHCP",
			 "dhcp_addresses":{"floating_ip_ranges":["10.0.0.5-10.0.0.9"]}}},
			{"id":2,"tenant_id":2,"addresses":{"type":"STATIC",
			 "static_addresses":{"floating_ip_ranges":["10.1.0.5"]}}}]}`))
	}, "token")

	networks, err := client.ListNetworks(context.Background())
	require.NoError(t, err)

	require.Len(t, networks, 2)
	assert.Equal(t, []string{"10.0.0.5-10.0.0.9"}, networks[0].FloatingAddresses)
	assert.Equal(t, []string{"10.1.0.5"}, networks[1].FloatingAddresses)
}

// An address scheme this decoder does not know comes back named and empty.
// The caller refuses a volume on this answer, so "I did not understand" must
// not be indistinguishable from "this network has no addresses".
func TestListNetworksReportsAnUndecodedAddressSchemeAsUnknown(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"frontend_networks":[
			{"id":5,"name":"static-net","tenant_id":3,
			 "addresses":{"type":"STATIC","static_addresses":{"ip_ranges":["10.0.0.5"]}}}]}`))
	}, "token")

	networks, err := client.ListNetworks(context.Background())
	require.NoError(t, err)

	require.Len(t, networks, 1)
	assert.Equal(t, "STATIC", networks[0].AddressType)
	assert.Empty(t, networks[0].FloatingAddresses)
}

// The authentication mode must reach the cluster verbatim: it is what makes the
// export *demand* a flavour, and a client offering something weaker is refused.
func TestCreateExportSendsAuthenticationMode(t *testing.T) {
	var body map[string]json.RawMessage

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Write([]byte(`{"id":"3","export_path":"/csi/b","fs_path":"/vol/b"}`))
	}, "token")

	_, err := client.CreateExport(context.Background(), ExportSpec{
		ExportPath: "/csi/b", FSPath: "/vol/b", AuthenticationMode: AuthenticationModeKrb5p,
	})
	require.NoError(t, err)

	var restrictions []map[string]any
	require.NoError(t, json.Unmarshal(body["restrictions"], &restrictions))
	require.Len(t, restrictions, 1)
	assert.Equal(t, AuthenticationModeKrb5p, restrictions[0]["required_authentication_mode"])
}

// An unset mode must keep sending None, so every export created before this
// field existed is byte-identical.
func TestCreateExportDefaultsToNoAuthenticationRequirement(t *testing.T) {
	var body map[string]json.RawMessage

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Write([]byte(`{"id":"4","export_path":"/csi/c","fs_path":"/vol/c"}`))
	}, "token")

	_, err := client.CreateExport(context.Background(), ExportSpec{ExportPath: "/csi/c", FSPath: "/vol/c"})
	require.NoError(t, err)

	var restrictions []map[string]any
	require.NoError(t, json.Unmarshal(body["restrictions"], &restrictions))
	assert.Equal(t, AuthenticationModeNone, restrictions[0]["required_authentication_mode"])
}

// ListDirectory must follow paging.next to the end.
//
// This loop had no coverage anywhere: the mock server returned an empty next
// token unconditionally, so its body ran exactly once in every test and in
// csi-sanity too. A listing that stopped at the first page would silently
// restore a partial volume, which is the one outcome a restore must never
// produce -- so "it happens to work" was not good enough for it.
func TestListDirectoryFollowsPaging(t *testing.T) {
	var requested []string

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.RequestURI())
		w.WriteHeader(http.StatusOK)

		switch r.URL.Query().Get("after") {
		case "":
			// First page hands back a relative URI, which is what a real
			// cluster returns and what the client must feed back verbatim.
			w.Write([]byte(`{
				"files": [{"name":"a","path":"/vol/a","id":"1","type":"FS_FILE_TYPE_FILE","size":"10","mode":"0644"}],
				"paging": {"next": "/v1/files/%2Fvol/entries/?limit=1000&snapshot=42&after=%2Fvol%2Fa"}
			}`))
		case "/vol/a":
			w.Write([]byte(`{
				"files": [{"name":"b","path":"/vol/b","id":"2","type":"FS_FILE_TYPE_FILE","size":"20","mode":"0644"}],
				"paging": {"next": "/v1/files/%2Fvol/entries/?limit=1000&snapshot=42&after=%2Fvol%2Fb"}
			}`))
		default:
			// Third response closes the listing.
			w.Write([]byte(`{"files": [], "paging": {"next": ""}}`))
		}
	}, "")

	entries, err := client.ListDirectory(context.Background(), "/vol", "42")

	require.NoError(t, err)
	require.Len(t, entries, 2, "entries from every page must be accumulated, not just the first")
	assert.Equal(t, "a", entries[0].Name)
	assert.Equal(t, uint64(10), entries[0].Size)
	assert.Equal(t, "b", entries[1].Name)
	assert.Equal(t, uint64(20), entries[1].Size)

	require.Len(t, requested, 3, "the loop must keep going until next is empty")
	// The first request is built by the client; the rest are the cluster's own
	// next URIs, so the snapshot parameter has to survive being handed back --
	// a page that dropped it would read the directory at head, mid-restore.
	assert.Contains(t, requested[0], "limit=1000")
	assert.Contains(t, requested[0], "snapshot=42")
	assert.Contains(t, requested[1], "snapshot=42")
	assert.Contains(t, requested[2], "snapshot=42")
}

// An error on a later page must fail the whole listing rather than returning
// what was collected so far, which would look like a complete short directory.
func TestListDirectorySurfacesAFailureOnALaterPage(t *testing.T) {
	var calls int

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"files": [{"name":"a","path":"/vol/a","id":"1","type":"FS_FILE_TYPE_FILE","size":"10","mode":"0644"}],
				"paging": {"next": "/v1/files/%2Fvol/entries/?limit=1000&after=%2Fvol%2Fa"}
			}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error_class":"internal_error","description":"boom"}`))
	}, "")

	entries, err := client.ListDirectory(context.Background(), "/vol", "")

	require.Error(t, err, "a partial listing must not be reported as a complete one")
	assert.Nil(t, entries)
	assert.Equal(t, 2, calls)
}

// GetQuotaInfo reads capacity_usage alongside the limit. That one field is
// what lets GetCapacity report space remaining instead of a ceiling, and what
// gives CreateSnapshot a size to put on VolumeSnapshot.status.restoreSize.
// The limit and the usage come from two different documents, and the order
// matters: the limit is required and the usage is best-effort, so the call
// that can fail without failing the RPC goes second.
func TestGetQuotaInfoReadsTheStatusDocumentOnce(t *testing.T) {
	var gotPaths []string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPaths = append(gotPaths, r.URL.Path)
		w.WriteHeader(http.StatusOK)
		if strings.HasPrefix(r.URL.Path, "/v1/files/quotas/status/") {
			w.Write([]byte(`{"id":"1","limit":"1024","capacity_usage":"256"}`))
			return
		}
		w.Write([]byte(`{"id":"1","limit":"1024"}`))
	}, "")

	info, err := client.GetQuotaInfo(context.Background(), "1")

	// One GET: the status document carries the limit too.
	require.NoError(t, err)
	assert.Equal(t, []string{"/v1/files/quotas/status/1"}, gotPaths)
	assert.Equal(t, QuotaInfo{Limit: 1024, Usage: 256, UsageKnown: true}, info)
}

// When the status document cannot be read, the quota object still gives the
// limit, with the usage unknown.
func TestGetQuotaInfoFallsBackToTheQuotaObject(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/files/quotas/status/") {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Write([]byte(`{"id":"1","limit":"1024"}`))
	}, "")

	info, err := client.GetQuotaInfo(context.Background(), "1")

	require.NoError(t, err)
	assert.Equal(t, QuotaInfo{Limit: 1024}, info)
}

// A cluster or a token that will not serve the status document leaves the
// usage unknown and the limit intact, rather than failing the provision.
func TestGetQuotaInfoSurvivesAnUnreadableStatusDocument(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/files/quotas/status/") {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"error_class":"http_forbidden_error"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"1","limit":"1024"}`))
	}, "")

	info, err := client.GetQuotaInfo(context.Background(), "1")

	require.NoError(t, err)
	assert.Equal(t, uint64(1024), info.Limit)
	assert.False(t, info.UsageKnown)
	assert.Equal(t, uint64(1024), info.Available(), "an unknown usage bounds by the limit")
}

func TestGetQuotaInfoReadsCapacityUsage(t *testing.T) {
	// The quota object and the quota *status* object are two documents on
	// Core 7.9.2.2. Only the status document carries capacity_usage; the
	// plain object is id and limit (confirmed live against the 7.9.2.2 lab
	// arrays, and by a PUT whose response is
	// {id, limit}). This handler serves each URL what the array serves it,
	// which the previous handler did not: it attached capacity_usage to
	// whatever was asked for, so the test passed without the driver ever
	// reading the document that has the field.
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if strings.HasPrefix(r.URL.Path, "/v1/files/quotas/status/") {
			w.Write([]byte(`{"id":"1","path":"/csi/volumes/pvc-a/",` +
				`"limit":"10737418240","capacity_usage":"3221225472"}`))
			return
		}
		w.Write([]byte(`{"id":"1","limit":"10737418240"}`))
	}, "")

	info, err := client.GetQuotaInfo(context.Background(), "1")

	require.NoError(t, err)
	assert.Equal(t, uint64(10737418240), info.Limit)
	assert.Equal(t, uint64(3221225472), info.Usage)
	assert.True(t, info.UsageKnown)
	assert.Equal(t, uint64(10737418240-3221225472), info.Available())
}

// A cluster that omits capacity_usage -- an older Core release, or a token
// without the privilege -- must not be mistaken for an empty directory.
// UsageKnown is what keeps "reported zero" apart from "reported nothing", and
// getting that backwards would advertise a full volume's whole limit as free.
func TestGetQuotaInfoWithoutCapacityUsage(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"1","limit":"10737418240"}`))
	}, "")

	info, err := client.GetQuotaInfo(context.Background(), "1")

	require.NoError(t, err)
	assert.Equal(t, uint64(10737418240), info.Limit)
	assert.False(t, info.UsageKnown)
	assert.Equal(t, uint64(10737418240), info.Available(),
		"with no usage figure the limit is the fallback, as it was before usage was read")
}

// An unparseable usage is treated as absent rather than as an error: the limit
// is still a usable answer, and failing the call would take GetCapacity and
// CreateSnapshot down over a reporting field.
func TestGetQuotaInfoIgnoresAnUnparseableCapacityUsage(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"1","limit":"1024","capacity_usage":"not-a-number"}`))
	}, "")

	info, err := client.GetQuotaInfo(context.Background(), "1")

	require.NoError(t, err)
	assert.Equal(t, uint64(1024), info.Limit)
	assert.False(t, info.UsageKnown)
}

// Usage above the limit is reachable: a quota can be lowered below what is
// already stored, and writes then fail. The subtraction must clamp rather
// than wrap to an enormous number.
func TestQuotaInfoAvailableClampsAtZero(t *testing.T) {
	over := QuotaInfo{Limit: 100, Usage: 400, UsageKnown: true}
	assert.Equal(t, uint64(0), over.Available())

	exact := QuotaInfo{Limit: 100, Usage: 100, UsageKnown: true}
	assert.Equal(t, uint64(0), exact.Available())
}

// The request body must not grow a capacity_usage field. quotaWire and
// quotaResponseWire are separate types precisely so that reading a field the
// driver must never send cannot leak into a POST or PUT.
func TestQuotaWritesDoNotSendCapacityUsage(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*Client) error
	}{
		{"create", func(c *Client) error { return c.CreateQuota(context.Background(), "1", 1024) }},
		{"update", func(c *Client) error { return c.UpdateQuota(context.Background(), "1", 1024) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := &recordingHandler{status: http.StatusOK}
			client := newTestClient(t, handler.serve, "")

			require.NoError(t, tc.call(client))
			assert.NotContains(t, handler.gotBody, "capacity_usage",
				"the quota endpoint is strict about its field set; a read-only field "+
					"on a write body is how that breaks")
		})
	}
}

// GetTreeUsage reads a tree's total_capacity -- data plus metadata, the
// measure a quota counts -- and its counts, as of a snapshot when one is
// named.
func TestGetTreeUsageReadsTheSnapshotsAggregates(t *testing.T) {
	var gotPath, gotSnapshot string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotSnapshot = r.URL.Path, r.URL.Query().Get("snapshot")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"path":"/csi/volumes/pvc-1","id":"900","total_capacity":"82911232","total_data":"0",` +
			`"total_files":"3","total_directories":"20001"}`))
	}, "")

	usage, err := client.GetTreeUsage(context.Background(), "900", "42")

	require.NoError(t, err)
	assert.Equal(t, TreeUsage{Capacity: 82911232, Files: 3, Directories: 20001}, usage)
	assert.Equal(t, "/v1/files/900/aggregates/", gotPath)
	assert.Equal(t, "42", gotSnapshot)
}

// CreateLink posts CREATE_LINK to the new name's parent, naming the existing
// file in old_path.
func TestCreateLinkPostsCreateLink(t *testing.T) {
	h := &recordingHandler{status: http.StatusOK, responseBody: `{"id":"7","path":"/stage/b","type":"FS_FILE_TYPE_FILE"}`}
	client := newTestClient(t, h.serve, "")

	_, err := client.CreateLink(context.Background(), "/stage", "b", "/stage/a")

	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, h.gotMethod)
	// The parent is percent-encoded into the file reference; the server sees it decoded.
	assert.Equal(t, "/v1/files//stage/entries/", h.gotPath)
	assert.Equal(t, map[string]any{"name": "b", "action": "CREATE_LINK", "old_path": "/stage/a"}, h.gotBody)
}

// SetFileAttributes PATCHes only the fields it is given: an empty
// field would otherwise overwrite the entry's value with nothing.
func TestSetFileAttributesSendsOnlyTheGivenFields(t *testing.T) {
	h := &recordingHandler{status: http.StatusOK, responseBody: `{}`}
	client := newTestClient(t, h.serve, "")

	err := client.SetFileAttributes(context.Background(), "/stage/a", FileAttributes{Mode: "0700", Owner: "12884901001"})

	require.NoError(t, err)
	assert.Equal(t, http.MethodPatch, h.gotMethod)
	assert.Equal(t, "/v1/files//stage/a/info/attributes", h.gotPath)
	assert.Equal(t, map[string]any{"mode": "0700", "owner": "12884901001"}, h.gotBody)
}

// A listing carries the attributes a restore copies.
func TestListDirectoryReadsOwnerGroupAndMtime(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"files":[{"name":"f","id":"7","type":"FS_FILE_TYPE_FILE","size":"5","mode":"0600",` +
			`"owner":"12884901001","group":"12884901002","modification_time":"2020-01-01T00:00:00Z"}],"paging":{"next":""}}`))
	}, "")

	entries, err := client.ListDirectory(context.Background(), "7", "42")

	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "12884901001", entries[0].Owner)
	assert.Equal(t, "12884901002", entries[0].Group)
	assert.Equal(t, "2020-01-01T00:00:00Z", entries[0].ModificationTime)
}

// A response whose body stalls past the client timeout, or whose
// connection drops mid-body, is the array not finishing its answer: the
// transport failing, and reported as a *url.Error like any other transport
// failure, so the driver answers Unavailable rather than a final Internal. A
// body that arrives and is not the JSON expected is a malformed answer, and
// is not.
func TestAResponseBodyThatStallsIsATransportFailure(t *testing.T) {
	t.Run("stalls past the timeout", func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"name": "a", `))
			w.(http.Flusher).Flush()
			<-release
		}, "tok")
		c.HTTPClient = &http.Client{Timeout: 100 * time.Millisecond}

		_, err := c.ListDirectory(context.Background(), "/csi", "")

		var transport *url.Error
		require.ErrorAs(t, err, &transport)
	})

	for _, tc := range []struct{ name, body string }{
		{name: "not JSON", body: `<html>proxy error</html>`},
		{name: "the wrong JSON", body: `{"files": "nope"}`},
		{name: "empty", body: ``},
	} {
		t.Run(tc.name+" is a malformed answer, not a transport failure", func(t *testing.T) {
			c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.body))
			}, "tok")

			_, err := c.ListDirectory(context.Background(), "/csi", "")

			require.Error(t, err)
			var transport *url.Error
			assert.False(t, errors.As(err, &transport), "%v", err)
		})
	}
}

// ListDirectoryPages hands each page over as it arrives, so a caller
// that handles one before asking for the next holds a page at a time; an
// error from the caller stops the listing there.
func TestListDirectoryPagesHandsOverOnePageAtATime(t *testing.T) {
	var requested int
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		requested++
		w.WriteHeader(http.StatusOK)
		switch r.URL.Query().Get("after") {
		case "":
			w.Write([]byte(`{"files": [{"name":"a","path":"/vol/a","id":"1","type":"FS_FILE_TYPE_FILE","size":"10"}],
				"paging": {"next": "/v1/files/%2Fvol/entries/?limit=1000&after=%2Fvol%2Fa"}}`))
		default:
			w.Write([]byte(`{"files": [{"name":"b","path":"/vol/b","id":"2","type":"FS_FILE_TYPE_FILE","size":"20"}],
				"paging": {"next": ""}}`))
		}
	}, "")

	var pages [][]string
	err := client.ListDirectoryPages(context.Background(), "/vol", "", func(page []DirEntry) error {
		var names []string
		for _, e := range page {
			names = append(names, e.Name)
		}
		pages = append(pages, names)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"a"}, {"b"}}, pages)
	assert.Equal(t, 2, requested)

	stopped := errors.New("stop")
	requested = 0
	err = client.ListDirectoryPages(context.Background(), "/vol", "", func([]DirEntry) error { return stopped })
	assert.ErrorIs(t, err, stopped)
	assert.Equal(t, 1, requested, "the second page is not read once the caller stops")
}

// An export's host list is the union of every restriction, not the first
// one's. The driver writes a single restriction carrying the whole list, but
// the cluster may answer it normalised into one block per host; reading only
// Restrictions[0] truncated a two-network class to its first network, and a
// replica pre-staged from that read admitted no node that runs pods.
func TestListExportsUnionsHostRestrictionsAcrossEveryBlock(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"entries":[{"id":"1","export_path":"/csi/pvc-a","fs_path":"/vol/pvc-a",
			"restrictions":[
				{"read_only":false,"required_authentication_mode":"NFS_AUTHENTICATION_MODE_SYSTEM","host_restrictions":["198.51.100.0/24"]},
				{"read_only":false,"required_authentication_mode":"NFS_AUTHENTICATION_MODE_SYSTEM","host_restrictions":["203.0.113.0/24"]}
			]}]}`))
	}, "token")

	exports, err := client.ListExports(context.Background())

	require.NoError(t, err)
	require.Len(t, exports, 1)
	assert.Equal(t, []string{"198.51.100.0/24", "203.0.113.0/24"}, exports[0].AllowedClients)
	assert.Equal(t, "NFS_AUTHENTICATION_MODE_SYSTEM", exports[0].AuthenticationMode)
	assert.False(t, exports[0].ReadOnly)
}

// The same list in one block, which is the shape the driver writes, is read
// back whole and in order.
func TestListExportsKeepsAWholeHostListFromOneBlock(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"entries":[{"id":"1","export_path":"/csi/pvc-a","fs_path":"/vol/pvc-a",
			"restrictions":[{"read_only":false,"host_restrictions":["10.0.0.5","192.168.1.0/24","10.0.0.6"]}]}]}`))
	}, "token")

	exports, err := client.ListExports(context.Background())

	require.NoError(t, err)
	require.Len(t, exports, 1)
	assert.Equal(t, []string{"10.0.0.5", "192.168.1.0/24", "10.0.0.6"}, exports[0].AllowedClients)
}

// A host named by more than one block is reported once. The union is a set on
// the cluster, and a duplicate would be sent back on a replica's create.
func TestListExportsDeduplicatesHostsNamedBySeveralBlocks(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"entries":[{"id":"1","export_path":"/csi/pvc-a","fs_path":"/vol/pvc-a",
			"restrictions":[
				{"read_only":false,"host_restrictions":["10.0.0.5","10.0.0.6"]},
				{"read_only":true,"host_restrictions":["10.0.0.6","10.0.0.7"]}
			]}]}`))
	}, "token")

	exports, err := client.ListExports(context.Background())

	require.NoError(t, err)
	require.Len(t, exports, 1)
	assert.Equal(t, []string{"10.0.0.5", "10.0.0.6", "10.0.0.7"}, exports[0].AllowedClients)
	// One read-write block is enough to make the export writable, which is
	// what keeps a promote from mistaking it for a pre-staged replica.
	assert.False(t, exports[0].ReadOnly)
}

// A restriction with no host list admits every host, so it widens the export
// rather than narrowing it: the union is empty, which is what CreateExport
// writes for "no host restriction".
func TestListExportsReportsNoHostsWhenAnyBlockIsUnrestricted(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"entries":[{"id":"1","export_path":"/csi/pvc-a","fs_path":"/vol/pvc-a",
			"restrictions":[
				{"read_only":false,"host_restrictions":["10.0.0.5"]},
				{"read_only":false,"host_restrictions":[]}
			]}]}`))
	}, "token")

	exports, err := client.ListExports(context.Background())

	require.NoError(t, err)
	require.Len(t, exports, 1)
	assert.Empty(t, exports[0].AllowedClients)
}

// An export with no restrictions at all, and one whose only restriction names
// no host, both read back as unrestricted -- unchanged by the union.
func TestListExportsReportsNoHostsForAnUnrestrictedExport(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"entries":[
			{"id":"1","export_path":"/a","fs_path":"/a"},
			{"id":"2","export_path":"/b","fs_path":"/b","restrictions":[{"read_only":false,"host_restrictions":[]}]}
		]}`))
	}, "token")

	exports, err := client.ListExports(context.Background())

	require.NoError(t, err)
	require.Len(t, exports, 2)
	assert.Empty(t, exports[0].AllowedClients)
	assert.Empty(t, exports[1].AllowedClients)
	// No restrictions at all is not read-only; one read-write restriction is
	// not either.
	assert.False(t, exports[0].ReadOnly)
	assert.False(t, exports[1].ReadOnly)
}

// Read-only is still every block's verdict, and the authentication mode is
// still the first block's. The union changes neither.
func TestListExportsReadsReadOnlyFromEveryBlockAndAuthFromTheFirst(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"entries":[{"id":"1","export_path":"/csi/pvc-a","fs_path":"/vol/pvc-a",
			"restrictions":[
				{"read_only":true,"required_authentication_mode":"NFS_AUTHENTICATION_MODE_SYSTEM","host_restrictions":["10.0.0.5"]},
				{"read_only":true,"required_authentication_mode":"NFS_AUTHENTICATION_MODE_SYSTEM","host_restrictions":["10.0.0.6"]}
			]}]}`))
	}, "token")

	exports, err := client.ListExports(context.Background())

	require.NoError(t, err)
	require.Len(t, exports, 1)
	assert.True(t, exports[0].ReadOnly)
	assert.Equal(t, "NFS_AUTHENTICATION_MODE_SYSTEM", exports[0].AuthenticationMode)
	assert.Equal(t, []string{"10.0.0.5", "10.0.0.6"}, exports[0].AllowedClients)
}

// An expiration is set with a PATCH carrying only the expiration, as
// measured on the lab arrays (2026-11-16), and the answer is decoded.
func TestSetSnapshotExpirationPatchesOnlyTheExpiration(t *testing.T) {
	var gotMethod, gotPath string
	var body map[string]any
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.EscapedPath()
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Write([]byte(`{"id":53580,"name":"53580_revert_to_53578_for_csi-qfs-z2","source_file_id":"7",` +
			`"policy_id":null,"expiration":"2026-11-23T08:30:28Z","in_delete":false}`))
	}, "token")
	when := time.Date(2026, 11, 23, 8, 30, 28, 0, time.UTC)

	got, err := client.SetSnapshotExpiration(context.Background(), "53580", when)

	require.NoError(t, err)
	assert.Equal(t, http.MethodPatch, gotMethod)
	assert.Equal(t, "/v3/snapshots/53580", gotPath)
	assert.Equal(t, map[string]any{"expiration": "2026-11-23T08:30:28Z"}, body)
	assert.Equal(t, when, got.Expiration)
	assert.True(t, got.IsReplicationRevertSnapshot())
}

// "" is the API's "never expires", decoded as the zero time.
func TestSnapshotExpirationDecodes(t *testing.T) {
	assert.True(t, snapshotWire{ID: "1", Expiration: ""}.toSnapshotInfo().Expiration.IsZero())
	assert.Equal(t, time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC),
		snapshotWire{ID: "1", Expiration: "2030-01-02T03:04:05Z"}.toSnapshotInfo().Expiration)
}

func TestIsReplicationRevertSnapshot(t *testing.T) {
	for _, tt := range []struct {
		snap SnapshotInfo
		want bool
	}{
		{SnapshotInfo{ID: "53580", Name: "53580_revert_to_53578_for_csi-qfs-z2"}, true},
		{SnapshotInfo{ID: "9", Name: "revert_to_8_for_x"}, true},
		{SnapshotInfo{ID: "9", Name: "9_revert_to_8_for_x", PolicyID: "4"}, false},
		{SnapshotInfo{ID: "9", Name: "9_snapshot-abc"}, false},
		{SnapshotInfo{ID: "9", Name: "9_my_revert_to_backup"}, false},
		{SnapshotInfo{ID: "9", Name: "10_revert_to_8_for_x"}, false},
	} {
		assert.Equal(t, tt.want, tt.snap.IsReplicationRevertSnapshot(), tt.snap.Name)
	}
}
