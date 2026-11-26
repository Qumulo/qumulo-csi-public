// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// arrayAnswering answers each call with the next status in turn, and 200
// with a file's attributes after the list runs out.
func arrayAnswering(retryAfter string, statuses ...int) (*httptest.Server, *atomic.Int64) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := int(calls.Add(1)) - 1
		if n < len(statuses) {
			if retryAfter != "" {
				w.Header().Set("Retry-After", retryAfter)
			}
			w.WriteHeader(statuses[n])
			return
		}
		_, _ = w.Write([]byte(`{"id":"1","path":"/v","type":"FS_FILE_TYPE_DIRECTORY"}`))
	}))
	return srv, &calls
}

// A 503 is retried, after the Retry-After the array asked for.
func TestA503IsRetriedAfterItsRetryAfter(t *testing.T) {
	srv, calls := arrayAnswering("1", http.StatusServiceUnavailable)
	defer srv.Close()

	start := time.Now()
	if _, err := New(srv.URL, "t").LookupFileByPath(context.Background(), "/v"); err != nil {
		t.Fatalf("a 503 then a 200 answered %v", err)
	}
	if calls.Load() != 2 || time.Since(start) < time.Second {
		t.Fatalf("calls %d after %v; want 2, after the 1 s Retry-After", calls.Load(), time.Since(start))
	}
}

// Three attempts in all, and a 500 -- not "try again" -- is not retried.
func TestRetriesAreBoundedAndOnlyFor429And503(t *testing.T) {
	srv, calls := arrayAnswering("", http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusTooManyRequests)
	defer srv.Close()
	_, err := New(srv.URL, "t").LookupFileByPath(context.Background(), "/v")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusTooManyRequests || calls.Load() != retryAttempts {
		t.Fatalf("four 429s answered %v after %d calls; want the 429 after %d", err, calls.Load(), retryAttempts)
	}

	srv500, calls500 := arrayAnswering("", http.StatusInternalServerError)
	defer srv500.Close()
	if _, err := New(srv500.URL, "t").LookupFileByPath(context.Background(), "/v"); err == nil || calls500.Load() != 1 {
		t.Fatalf("a 500 answered %v after %d calls; want the error after 1", err, calls500.Load())
	}
}

// A retry never waits past the caller's deadline.
func TestARetryDoesNotOutliveTheDeadline(t *testing.T) {
	srv, calls := arrayAnswering("5", http.StatusServiceUnavailable)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err := New(srv.URL, "t").LookupFileByPath(ctx, "/v")
	if err == nil || calls.Load() != 1 || time.Since(start) > time.Second {
		t.Fatalf("answered %v after %d calls in %v; want the 503 at once, the 5 s wait not fitting", err, calls.Load(), time.Since(start))
	}
}

// postsAnswering answers POSTs with the next status in turn and counts how
// many of them the array actually applied; anything past the list, and any
// other method, gets a 200 with a snapshot body.
func postsAnswering(statuses ...int) (*httptest.Server, *atomic.Int64) {
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			_, _ = w.Write([]byte(`{"id":"1","path":"/v","type":"FS_FILE_TYPE_DIRECTORY"}`))
			return
		}
		n := int(posts.Add(1)) - 1
		if n < len(statuses) {
			w.WriteHeader(statuses[n])
			return
		}
		_, _ = fmt.Fprintf(w, `{"id":%d,"name":"%d_snap","source_file_id":"2","created_by_policy":false}`, n+1, n+1)
	}))
	return srv, &posts
}

// The shape that caused it: the array applied the POST and the reply
// came back 503. Retrying it would take a second snapshot under the same
// name, and the first would be orphaned -- no content, no expiry, no policy.
// So the POST is sent once and the 503 is the caller's to handle.
func TestASnapshotCreateIsNotRetried(t *testing.T) {
	srv, posts := postsAnswering(http.StatusServiceUnavailable)
	defer srv.Close()

	_, err := New(srv.URL, "t").CreateSnapshot(context.Background(), "2", "snap")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("CreateSnapshot answered %v; want the array's 503 back", err)
	}
	if posts.Load() != 1 {
		t.Fatalf("the array saw %d snapshot POSTs; want 1, since a repeat duplicates the snapshot", posts.Load())
	}
}

// The companion to the above, and the original probe: a snapshot POST the
// array applied before answering 503 must leave exactly one snapshot behind.
func TestSnapshotCreateLandedThen503IsNotDuplicated(t *testing.T) {
	var created atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		n := created.Add(1)
		if n == 1 {
			// Applied on the array, then the reply is lost behind a 503.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintf(w, `{"id":%d,"name":"%d_snap","source_file_id":"2","created_by_policy":false}`, n, n)
	}))
	defer srv.Close()

	if _, err := New(srv.URL, "t").CreateSnapshot(context.Background(), "2", "snap"); err == nil {
		t.Fatal("CreateSnapshot hid the array's 503; the RPC above needs it to answer Unavailable")
	}
	if created.Load() != 1 {
		t.Fatalf("one CreateSnapshot call created %d snapshots on the array; want 1", created.Load())
	}
}

// A rename that succeeded before its 503 must not be sent again: the repeat
// 404s on a path the first one already moved, which would report a rename
// that worked as a failure.
func TestARenameIsNotRetried(t *testing.T) {
	srv, posts := postsAnswering(http.StatusServiceUnavailable)
	defer srv.Close()

	if _, err := New(srv.URL, "t").Rename(context.Background(), "/p", "new", "/p/old"); err == nil {
		t.Fatal("Rename hid the array's 503")
	}
	if posts.Load() != 1 {
		t.Fatalf("the array saw %d rename POSTs; want 1", posts.Load())
	}
}

// The POSTs whose repeat the array refuses, or which
// do the same thing twice over, keep's retry. A directory create is the
// call site was filed about.
func TestAnOptedInPostIsStillRetried(t *testing.T) {
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && posts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"id":"1","path":"/v/d","type":"FS_FILE_TYPE_DIRECTORY"}`))
	}))
	defer srv.Close()

	if _, err := New(srv.URL, "t").CreateDirectory(context.Background(), "/v", "d"); err != nil {
		t.Fatalf("a 503 then a 200 on a directory create answered %v; the retry must survive", err)
	}
	if posts.Load() != 2 {
		t.Fatalf("the array saw %d directory POSTs; want 2", posts.Load())
	}
}
