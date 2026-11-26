// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The restore copy and the REST client.

// --- A size the listing cannot state is an error, not zero ---

// listingHandler serves one page of directory entries.
func listingHandler(t *testing.T, entries string) http.HandlerFunc {
	t.Helper()

	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"files":[` + entries + `],"paging":{"next":""}}`))
	}
}

func TestListDirectoryRefusesAFileWhoseSizeIsNotANumber(t *testing.T) {
	for name, size := range map[string]string{
		"missing":      `""`,
		"absent":       `null`,
		"not a number": `"a lot"`,
	} {
		t.Run(name, func(t *testing.T) {
			client := newTestClient(t, listingHandler(t, fmt.Sprintf(
				`{"name":"big.bin","path":"/v/big.bin","id":"5","type":"FS_FILE_TYPE_FILE","size":%s}`, size)), "")

			_, err := client.ListDirectory(context.Background(), "/v", "")

			require.Error(t, err, "a 0-byte restored file is what reading this as zero produced")
			assert.True(t, IsMalformedReply(err), "the array answered; its answer is the problem: %v", err)
			assert.Contains(t, err.Error(), "big.bin")
		})
	}
}

// A directory has no size worth reading, and the array does not always send
// one. Refusing those would refuse every listing.
func TestListDirectoryAcceptsADirectoryWithNoSize(t *testing.T) {
	client := newTestClient(t, listingHandler(t,
		`{"name":"sub","path":"/v/sub","id":"6","type":"FS_FILE_TYPE_DIRECTORY","size":""}`), "")

	entries, err := client.ListDirectory(context.Background(), "/v", "")

	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Zero(t, entries[0].Size)
}

func TestListDirectoryReadsAFileSize(t *testing.T) {
	client := newTestClient(t, listingHandler(t,
		`{"name":"big.bin","path":"/v/big.bin","id":"5","type":"FS_FILE_TYPE_FILE","size":"4096"}`), "")

	entries, err := client.ListDirectory(context.Background(), "/v", "")

	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(4096), entries[0].Size)
}

// --- A copy whose offsets cycle or go backwards stops ---

// copyChunkServer answers copy-chunk with the offsets the script gives, one
// per call; a nil entry is the API's "complete".
func copyChunkServer(t *testing.T, script []*[2]string) (*Client, *atomic.Int64) {
	t.Helper()

	var calls atomic.Int64
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1) - 1
		if int(n) >= len(script) || script[n] == nil {
			w.Write([]byte(`null`))
			return
		}
		fmt.Fprintf(w, `{"source_offset":%q,"target_offset":%q}`, script[n][0], script[n][1])
	}, "")
	return client, &calls
}

func offsets(source, target string) *[2]string { return &[2]string{source, target} }

func TestCopyFileStopsWhenTheClusterCyclesItsOffsets(t *testing.T) {
	// 32M -> 0 -> 32M -> 0 ... No pair repeats the one before it, so the
	// no-progress guard never fires; the forward-only rule catches the
	// first step back, which is how a cycle has to return.
	script := make([]*[2]string, 64)
	for i := range script {
		if i%2 == 0 {
			script[i] = offsets("33554432", "33554432")
		} else {
			script[i] = offsets("0", "0")
		}
	}
	client, calls := copyChunkServer(t, script)

	err := client.CopyFile(context.Background(), "/v/big.bin", "5", "42", 0)

	require.Error(t, err)
	assert.True(t, IsMalformedReply(err), "the array answered; its answer is the problem: %v", err)
	assert.Less(t, calls.Load(), int64(8), "the cycle must be caught at once, not at the iteration cap")
}

func TestCopyFileStopsWhenTheClusterGoesBackwards(t *testing.T) {
	client, _ := copyChunkServer(t, []*[2]string{
		offsets("33554432", "33554432"),
		offsets("16777216", "16777216"),
	})

	err := client.CopyFile(context.Background(), "/v/big.bin", "5", "42", 0)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "backwards")
}

func TestCopyFileFollowsTheClusterForward(t *testing.T) {
	client, calls := copyChunkServer(t, []*[2]string{
		offsets("33554432", "33554432"),
		offsets("67108864", "67108864"),
		nil,
	})

	require.NoError(t, client.CopyFile(context.Background(), "/v/big.bin", "5", "42", 0))
	assert.Equal(t, int64(3), calls.Load())
}

// --- Every identifier in a route is escaped ---

// TestEveryRouteEscapesItsIdentifiers calls each route that interpolates an
// identifier with one carrying a slash and a query separator, and asserts on
// EscapedPath: an unescaped one changes which route is called, and on a
// delete that means deleting something else or nothing at all.
func TestEveryRouteEscapesItsIdentifiers(t *testing.T) {
	const nasty = "1/../2?x=y"

	for _, tc := range []struct {
		name     string
		call     func(*Client) error
		wantPath string
	}{
		{
			name:     "GetQuota",
			call:     func(c *Client) error { _, err := c.GetQuota(context.Background(), nasty); return err },
			wantPath: "/v1/files/quotas/" + url.PathEscape(nasty),
		},
		{
			name:     "UpdateQuota",
			call:     func(c *Client) error { return c.UpdateQuota(context.Background(), nasty, 1) },
			wantPath: "/v1/files/quotas/" + url.PathEscape(nasty),
		},
		{
			name:     "DeleteQuota",
			call:     func(c *Client) error { return c.DeleteQuota(context.Background(), nasty) },
			wantPath: "/v1/files/quotas/" + url.PathEscape(nasty),
		},
		{
			name:     "DeleteSnapshot",
			call:     func(c *Client) error { return c.DeleteSnapshot(context.Background(), nasty) },
			wantPath: "/v3/snapshots/" + url.PathEscape(nasty),
		},
		{
			name:     "GetSnapshot",
			call:     func(c *Client) error { _, err := c.GetSnapshot(context.Background(), nasty); return err },
			wantPath: "/v3/snapshots/" + url.PathEscape(nasty),
		},
		{
			name:     "DeleteExport",
			call:     func(c *Client) error { return c.DeleteExport(context.Background(), nasty) },
			wantPath: "/v3/nfs/exports/" + url.PathEscape(nasty),
		},
		{
			name: "SetFileAttributesByID",
			call: func(c *Client) error {
				return c.SetFileAttributesByID(context.Background(), nasty, FileAttributes{Mode: "0777"})
			},
			wantPath: "/v1/files/" + url.PathEscape(nasty) + "/info/attributes",
		},
		{
			name:     "TreeDeleteFinished",
			call:     func(c *Client) error { _, err := c.TreeDeleteFinished(context.Background(), nasty); return err },
			wantPath: "/v1/tree-delete/jobs/" + url.PathEscape(nasty),
		},
		// The replication and snapshot-policy routes. On DeleteSource
		// and DeleteTarget an unescaped ID ends a different relationship.
		{
			name:     "GetSourceStatus",
			call:     func(c *Client) error { _, err := c.GetSourceStatus(context.Background(), nasty); return err },
			wantPath: "/v2/replication/source-relationships/" + url.PathEscape(nasty) + "/status",
		},
		{
			name:     "GetTargetStatus",
			call:     func(c *Client) error { _, err := c.GetTargetStatus(context.Background(), nasty); return err },
			wantPath: "/v2/replication/target-relationships/" + url.PathEscape(nasty) + "/status",
		},
		{
			name: "PatchSource",
			call: func(c *Client) error {
				_, err := c.PatchSource(context.Background(), nasty, SourcePatch{})
				return err
			},
			wantPath: "/v2/replication/source-relationships/" + url.PathEscape(nasty),
		},
		{
			name:     "DeleteSource",
			call:     func(c *Client) error { return c.DeleteSource(context.Background(), nasty) },
			wantPath: "/v2/replication/source-relationships/" + url.PathEscape(nasty),
		},
		{
			name:     "Replicate",
			call:     func(c *Client) error { return c.Replicate(context.Background(), nasty) },
			wantPath: "/v2/replication/source-relationships/" + url.PathEscape(nasty) + "/replicate",
		},
		{
			name: "Authorize",
			call: func(c *Client) error {
				_, err := c.Authorize(context.Background(), nasty, false, false)
				return err
			},
			wantPath: "/v2/replication/target-relationships/" + url.PathEscape(nasty) + "/authorize",
		},
		{
			name:     "MakeTargetWritable",
			call:     func(c *Client) error { _, err := c.MakeTargetWritable(context.Background(), nasty); return err },
			wantPath: "/v2/replication/target-relationships/" + url.PathEscape(nasty) + "/make-writable",
		},
		{
			name:     "Reconnect",
			call:     func(c *Client) error { _, err := c.Reconnect(context.Background(), nasty); return err },
			wantPath: "/v2/replication/target-relationships/" + url.PathEscape(nasty) + "/reconnect",
		},
		{
			name:     "DeleteTarget",
			call:     func(c *Client) error { return c.DeleteTarget(context.Background(), nasty) },
			wantPath: "/v2/replication/target-relationships/" + url.PathEscape(nasty) + "/delete",
		},
		{
			name:     "GetPolicySchedule",
			call:     func(c *Client) error { _, err := c.GetPolicySchedule(context.Background(), nasty); return err },
			wantPath: "/v1/snapshots/policies/" + url.PathEscape(nasty),
		},
		{
			// Only the read happens: "{}" has no restrictions to flip.
			name:     "SetExportReadOnly",
			call:     func(c *Client) error { return c.SetExportReadOnly(context.Background(), nasty, true) },
			wantPath: "/v3/nfs/exports/" + url.PathEscape(nasty),
		},
		{
			name:     "LookupFileByPath",
			call:     func(c *Client) error { _, err := c.LookupFileByPath(context.Background(), "/a b/c?d"); return err },
			wantPath: "/v1/files/" + url.PathEscape("/a b/c?d") + "/info/attributes",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotQuery string
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				gotPath, gotQuery = r.URL.EscapedPath(), r.URL.RawQuery
				w.Write([]byte(`{}`))
			}, "")

			_ = tc.call(client)

			assert.Equal(t, tc.wantPath, gotPath)
			assert.Empty(t, gotQuery, "an identifier must not become a query string")
		})
	}
}

// --- The breaker counts a body that never finishes ---

// TestTheBreakerCountsAStalledBodyAsAFailure: a reply whose headers arrive
// and whose body then drops is the array failing, and the breaker has to see
// it. It used to be recorded as a healthy call, so an array answering
// headers and nothing else reset the failure count on every attempt and the
// breaker never opened.
func TestTheBreakerCountsAStalledBodyAsAFailure(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "64")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"revi`)) // short of what it promised
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler) // drop the connection mid-body
	}, "")
	client.Breaker = NewBreaker()

	var lastErr error
	for range breakerThreshold {
		lastErr = client.HealthCheck(context.Background())
		require.Error(t, lastErr)
	}

	// Once the breaker has seen them, the next call never leaves the process.
	err := client.HealthCheck(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, errEndpointDown,
		"a body that never finishes must count towards the breaker: %v", lastErr)
}

// A reply the client cannot parse is not the array failing: it answered, in
// full, with something unusable. Counting it would open the breaker on a
// cluster that is up.
func TestTheBreakerIgnoresAnUnparseableButCompleteReply(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`not json at all`))
	}, "")
	client.Breaker = NewBreaker()

	for range breakerThreshold + 1 {
		err := client.HealthCheck(context.Background())
		require.Error(t, err)
		assert.True(t, IsMalformedReply(err), "%v", err)
		assert.NotErrorIs(t, err, errEndpointDown)
	}
}

// --- Trailing data and an unbounded body are refused ---

func TestADecodedReplyWithTrailingDataIsRefused(t *testing.T) {
	for name, body := range map[string]string{
		"a second document": `{"revision_id":"a","build_id":"1"}{"revision_id":"b","build_id":"2"}`,
		"trailing junk":     `{"revision_id":"a","build_id":"1"} and then some`,
	} {
		t.Run(name, func(t *testing.T) {
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(body))
			}, "")

			err := client.HealthCheck(context.Background())

			require.Error(t, err)
			assert.True(t, IsMalformedReply(err), "%v", err)
		})
	}
}

// Trailing whitespace is what a pretty-printing server sends, and is not
// trailing data.
func TestADecodedReplyMayEndWithWhitespace(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{\"revision_id\":\"a\",\"build_id\":\"1\"}\n\n"))
	}, "")

	require.NoError(t, client.HealthCheck(context.Background()))
}

// The body is finite and well formed, and only its size is wrong: one that
// never terminated, or never closed its JSON, failed with or without the cap
// and so proved nothing about it. Past the cap it is a malformed answer, not
// a transport failure.
func TestABodyPastTheCapIsRefused(t *testing.T) {
	pad := strings.Repeat("a", maxResponseBytes)
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"files":[],"paging":{"next":""},"pad":"` + pad + `"}`))
	}, "")

	_, err := client.ListDirectory(context.Background(), "/v", "")

	require.Error(t, err, "a body past the cap must be cut off, not read until memory runs out")
	assert.True(t, IsMalformedReply(err), "the array answered; its answer is too big: %.200v", err)
	assert.Contains(t, err.Error(), "64 MiB")
}

// --- The readers nothing covered ---

func TestGetNFSSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want NFSSettings
	}{
		{
			name: "everything on",
			body: `{"v4_enabled":true,"krb5_enabled":true,"krb5p_enabled":true,"krb5i_enabled":true,
			        "auth_sys_enabled":true,"idmap_domain":"example.com"}`,
			want: NFSSettings{
				V4Enabled: true, Krb5Enabled: true, Krb5pEnabled: true, Krb5iEnabled: true,
				AuthSysEnabled: true, IdmapDomain: "example.com",
			},
		},
		{
			// The normal case on a cluster with no Kerberos: null, not "".
			name: "no idmap domain",
			body: `{"v4_enabled":true,"auth_sys_enabled":true,"idmap_domain":null}`,
			want: NFSSettings{V4Enabled: true, AuthSysEnabled: true},
		},
		{
			name: "v3 only",
			body: `{"v4_enabled":false,"auth_sys_enabled":true}`,
			want: NFSSettings{AuthSysEnabled: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath string
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.EscapedPath()
				w.Write([]byte(tc.body))
			}, "")

			got, err := client.GetNFSSettings(context.Background())

			require.NoError(t, err)
			assert.Equal(t, "/v2/nfs/settings", gotPath)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestGetNFSSettingsSurfacesAFailure(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}, "")

	_, err := client.GetNFSSettings(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 403")
}

func TestClusterID(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		w.Write([]byte(`{"cluster_id":"3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90"}`))
	}, "")

	got, err := client.ClusterID(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "/v1/node/state", gotPath)
	assert.Equal(t, "3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90", got)
}

// An answer with no cluster_id is not an identity: requireSameCluster
// compares what this returns, and "" on both sides would read as a match.
func TestClusterIDRefusesAnAnswerWithoutOne(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{}`))
	}, "")

	_, err := client.ClusterID(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster_id")
}

func TestTokenFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(path, []byte("  secret-token\n"), 0o600))

	source := TokenFromFile(path)

	got, err := source()
	require.NoError(t, err)
	assert.Equal(t, "secret-token", got, "a hand-written Secret's newline must not reach the header")

	// Re-read per call: a rotated Secret takes effect without a restart,
	// which is the whole reason this is a function and not a string.
	require.NoError(t, os.WriteFile(path, []byte("rotated-token"), 0o600))
	got, err = source()
	require.NoError(t, err)
	assert.Equal(t, "rotated-token", got)
}

func TestTokenFromFileRefusesAnEmptyOrMissingFile(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	require.NoError(t, os.WriteFile(empty, []byte("\n  \n"), 0o600))

	_, err := TokenFromFile(empty)()
	require.Error(t, err, "an empty token file must not send an empty bearer header")
	assert.Contains(t, err.Error(), "empty")

	_, err = TokenFromFile(filepath.Join(dir, "absent"))()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "absent")
}

// The token reaches the request, and a token source that fails stops the
// call rather than sending an unauthenticated one.
func TestTheTokenSourceDecidesTheAuthorizationHeader(t *testing.T) {
	var gotAuth string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Write([]byte(`{"revision_id":"a","build_id":"1"}`))
	}, "")

	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(path, []byte("file-token"), 0o600))
	client.TokenFunc = TokenFromFile(path)

	require.NoError(t, client.HealthCheck(context.Background()))
	assert.Equal(t, "Bearer file-token", gotAuth)

	client.TokenFunc = TokenFromFile(filepath.Join(dir, "gone"))
	err := client.HealthCheck(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token")
}
