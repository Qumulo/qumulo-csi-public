//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Review findings on the restore copy and the REST client.

// stalledBody answers status with headers that promise more body than it
// sends, then drops the connection.
func stalledBody(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "64")
		w.WriteHeader(status)
		w.Write([]byte(`{"revi`))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}
}

// A call that wants no reply (DeleteQuota, DeleteSnapshot, a PATCH...)
// still has to see the body end. One whose body dropped was recorded as a
// healthy call, so an array answering headers and nothing else never opened
// the breaker on those routes.
func TestDefect_TheBreakerCountsAStalledBodyOnACallThatWantsNoReply(t *testing.T) {
	client := newTestClient(t, stalledBody(http.StatusOK), "")
	client.Breaker = NewBreaker()

	for i := range breakerThreshold {
		err := client.DeleteQuota(context.Background(), "5")
		t.Logf("DeleteQuota %d: %v", i+1, err)
	}

	err := client.DeleteQuota(context.Background(), "5")
	t.Logf("DeleteQuota after the threshold: %v", err)
	assert.ErrorIs(t, err, errEndpointDown, "stalled bodies on a no-reply call must count towards the breaker")
}

// The same for an error reply, whose body newAPIError read with every
// read error ignored.
func TestDefect_TheBreakerCountsAStalledErrorBody(t *testing.T) {
	client := newTestClient(t, stalledBody(http.StatusInternalServerError), "")
	client.Breaker = NewBreaker()

	for i := range breakerThreshold {
		err := client.HealthCheck(context.Background())
		t.Logf("HealthCheck %d: %v", i+1, err)
		require.Error(t, err)
	}

	err := client.HealthCheck(context.Background())
	t.Logf("HealthCheck after the threshold: %v", err)
	assert.ErrorIs(t, err, errEndpointDown, "a stalled error body must count towards the breaker")
}

// An error body is read through the same cap as any other. It was
// decoded without one, so an error reply of any size was held in full.
func TestDefect_AnErrorBodyIsReadThroughTheCap(t *testing.T) {
	huge := strings.Repeat("a", maxResponseBytes+1024)
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error_class":"x","description":"` + huge + `"}`))
	}, "")
	client.Breaker = NewBreaker()

	for range breakerThreshold + 1 {
		err := client.HealthCheck(context.Background())
		require.Error(t, err)
		var apiErr *APIError
		require.True(t, errors.As(err, &apiErr), "the status still arrives: %v", err)
		assert.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
		assert.Less(t, len(apiErr.Msg), maxResponseBytes, "an error body past the cap was read in full")
		assert.NotErrorIs(t, err, errEndpointDown, "an over-size error body is an answer, not an outage")
	}
}

// A complete, finite reply over the cap is the array answering with
// something unusable, not the transport failing. It surfaced as an
// "unexpected EOF" transport error and opened the breaker on the second one.
func TestDefect_AnOversizedReplyIsMalformedAndLeavesTheBreakerClosed(t *testing.T) {
	huge := strings.Repeat("a", maxResponseBytes+1024)
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"revision_id":"` + huge + `","build_id":"1"}`))
	}, "")
	client.Breaker = NewBreaker()

	for i := range breakerThreshold + 1 {
		err := client.HealthCheck(context.Background())
		t.Logf("HealthCheck %d: %.200v", i+1, err)
		require.Error(t, err)
		assert.True(t, IsMalformedReply(err), "an over-size reply is malformed, not a transport failure: %.200v", err)
		assert.Contains(t, err.Error(), "64 MiB")
		assert.NotErrorIs(t, err, errEndpointDown, "an over-size reply must not open the breaker")
	}
}

// The replication and snapshot-policy routes escape their identifiers
// like every other route. An unescaped one changes which route is called --
// on DeleteSource or DeleteTarget, deleting something else.
func TestDefect_ReplicationRoutesEscapeTheirIdentifiers(t *testing.T) {
	const nasty = "1/../2?x=y"
	esc := url.PathEscape(nasty)
	ctx := context.Background()

	for _, tc := range []struct {
		name     string
		call     func(*Client) error
		wantPath string
	}{
		{"GetSourceStatus", func(c *Client) error { _, err := c.GetSourceStatus(ctx, nasty); return err },
			"/v2/replication/source-relationships/" + esc + "/status"},
		{"GetTargetStatus", func(c *Client) error { _, err := c.GetTargetStatus(ctx, nasty); return err },
			"/v2/replication/target-relationships/" + esc + "/status"},
		{"PatchSource", func(c *Client) error { _, err := c.PatchSource(ctx, nasty, SourcePatch{}); return err },
			"/v2/replication/source-relationships/" + esc},
		{"DeleteSource", func(c *Client) error { return c.DeleteSource(ctx, nasty) },
			"/v2/replication/source-relationships/" + esc},
		{"Replicate", func(c *Client) error { return c.Replicate(ctx, nasty) },
			"/v2/replication/source-relationships/" + esc + "/replicate"},
		{"Authorize", func(c *Client) error { _, err := c.Authorize(ctx, nasty, false, false); return err },
			"/v2/replication/target-relationships/" + esc + "/authorize"},
		{"MakeTargetWritable", func(c *Client) error { _, err := c.MakeTargetWritable(ctx, nasty); return err },
			"/v2/replication/target-relationships/" + esc + "/make-writable"},
		{"Reconnect", func(c *Client) error { _, err := c.Reconnect(ctx, nasty); return err },
			"/v2/replication/target-relationships/" + esc + "/reconnect"},
		{"DeleteTarget", func(c *Client) error { return c.DeleteTarget(ctx, nasty) },
			"/v2/replication/target-relationships/" + esc + "/delete"},
		{"GetPolicySchedule", func(c *Client) error { _, err := c.GetPolicySchedule(ctx, nasty); return err },
			"/v1/snapshots/policies/" + esc},
		{"SetExportReadOnly", func(c *Client) error { return c.SetExportReadOnly(ctx, nasty, true) },
			"/v3/nfs/exports/" + esc},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotQuery string
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if gotPath == "" {
					gotPath, gotQuery = r.URL.EscapedPath(), r.URL.RawQuery
				}
				w.Write([]byte(`{}`))
			}, "")

			_ = tc.call(client)

			assert.Equal(t, tc.wantPath, gotPath)
			assert.Empty(t, gotQuery, "an identifier must not become a query string")
		})
	}
}

// IsMalformedSnapshotID matches the path GetSnapshot sends, which is
// escaped. It built the unescaped one, so a refused ID that needed escaping
// was never recognised.
func TestDefect_IsMalformedSnapshotIDMatchesTheEscapedPath(t *testing.T) {
	const id = "not a number"
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error_class":"decode_error","description":"could not parse"}`))
	}, "")

	_, err := client.GetSnapshot(context.Background(), id)

	require.Error(t, err)
	assert.True(t, IsMalformedSnapshotID(err, id), "the cluster refused this ID in this lookup: %v", err)
}
