// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

// How the client handles replies it must not take at face value: a paging
// cursor that repeats or points at another host, a redirect, a null export
// restriction, and the link count a listing carries. (This file was
// token_scope_test.go, a name that described none of it.)

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A listing used to follow the array's paging cursor until it was
// empty, so a repeated cursor looped for ever.
func TestListDirectoryStopsWhenThePagingCursorRepeats(t *testing.T) {
	var pages atomic.Int64
	var b strings.Builder
	b.WriteString(`{"files":[`)
	for i := 0; i < 100; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":"f%03d","path":"/v/f%03d","id":"%d","type":"FS_FILE_TYPE_FILE","size":"1"}`, i, i, i+10)
	}
	b.WriteString(`],"paging":{"next":"/v1/files/%2Fv/entries/?limit=1000&after=f099"}}`)
	page := b.String()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(page))
	}))
	defer srv.Close()

	// No deadline on the context, as on a restore walk's own budget running
	// long: only the client itself can end the loop.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		entries, err := New(srv.URL, "t").ListDirectory(ctx, "/v", "")
		done <- result{len(entries), err}
	}()

	select {
	case r := <-done:
		if r.err == nil {
			t.Errorf("ListDirectory returned %d entries and no error for a cursor that never moves", r.n)
		}
	case <-time.After(3 * time.Second):
		cancel()
		<-done
		t.Fatalf("ListDirectory was still following the same cursor after 3 s (%d requests)", pages.Load())
	}
}

// A cursor that turns the base host into userinfo must not send the
// next page, or the token, to the other host.
func TestListDirectoryDoesNotFollowACursorToAnotherHost(t *testing.T) {
	var stolen atomic.Value
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stolen.Store(r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"files":[],"paging":{"next":""}}`))
	}))
	defer other.Close()
	otherHost := strings.TrimPrefix(other.URL, "http://")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"files":[],"paging":{"next":"@` + otherHost + `/v1/files/%2Fv/entries/?after=x"}}`))
	}))
	defer srv.Close()

	_, err := New(srv.URL, "secret-token").ListDirectory(context.Background(), "/v", "")
	if err == nil {
		t.Fatal("a cursor naming another host was followed")
	}
	if got, _ := stolen.Load().(string); got != "" {
		t.Fatalf("the token reached %s: %q", otherHost, got)
	}
}

// A redirect is not followed, so the token is not re-sent -- here to a
// plain-HTTP listener on the same host name, which Go forwards it to.
func TestClientDoesNotFollowARedirect(t *testing.T) {
	var reached atomic.Value
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Store(r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{}`))
	}))
	defer plain.Close()
	tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer tlsSrv.Close()

	c := New(tlsSrv.URL, "secret-token")
	c.HTTPClient = tlsSrv.Client()
	_, err := c.LookupFileByPath(context.Background(), "/v")
	if err == nil {
		t.Fatal("a redirect answered as success")
	}
	if got, _ := reached.Load().(string); got != "" {
		t.Fatalf("the redirect was followed and carried the token in cleartext: %q", got)
	}
}

// SetExportReadOnly must answer an export whose restrictions hold a JSON null with an error, not a panic.
// It used to write into the null entry's nil map and panic.
func TestSetExportReadOnlyRejectsANullRestriction(t *testing.T) {
	var patched atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			patched.Store(true)
		}
		_, _ = w.Write([]byte(`{"id":"5","export_path":"/csi/volumes/pvc-r","fs_path":"/csi/volumes/pvc-r",` +
			`"description":"","restrictions":[null]}`))
	}))
	defer srv.Close()

	var err error
	panicked := func() (p any) {
		defer func() { p = recover() }()
		err = New(srv.URL, "t").SetExportReadOnly(context.Background(), "5", true)
		return nil
	}()
	if panicked != nil {
		t.Fatalf("SetExportReadOnly panicked on a null restriction: %v", panicked)
	}
	if err == nil {
		t.Error("SetExportReadOnly accepted a null restriction; want an error")
	}
	if patched.Load() {
		t.Error("SetExportReadOnly wrote the export back although a restriction was null")
	}
}

// ListDirectory must end by itself, with an error, when the array hands back the same paging cursor again.

// ListDirectory decodes each entry's link count.
func TestListDirectoryDecodesTheLinkCount(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"files":[{"name":"a","path":"/v/a","id":"1","type":"FS_FILE_TYPE_FILE","size":"1","num_links":2},` +
			`{"name":"b","path":"/v/b","id":"2","type":"FS_FILE_TYPE_FILE","size":"1"}],"paging":{"next":""}}`))
	}))
	defer srv.Close()
	entries, err := New(srv.URL, "t").ListDirectory(context.Background(), "/v", "")
	if err != nil || len(entries) != 2 {
		t.Fatalf("listing answered %v, %v", entries, err)
	}
	if entries[0].NumLinks == nil || *entries[0].NumLinks != 2 || entries[1].NumLinks != nil {
		t.Fatalf("link counts %v, %v; want 2 and none", entries[0].NumLinks, entries[1].NumLinks)
	}
}
