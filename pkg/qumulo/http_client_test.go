// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// A call whose answer is discarded must still read the body, so its connection is reused by the next call.
func TestClientReusesTheConnectionAfterACallWhoseAnswerItDiscards(t *testing.T) {
	var conns atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// What the array answers a PATCH of a file's attributes with.
		_, _ = w.Write([]byte(`{"id":"11","path":"/csi/volumes/pvc-a","type":"FS_FILE_TYPE_DIRECTORY",` +
			`"mode":"0777","owner":"500","group":"500","size":"0"}`))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	c := New(srv.URL, "t")
	c.HTTPClient = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	const calls = 20
	for i := 0; i < calls; i++ {
		if err := c.SetFileAttributesByID(context.Background(), "11", FileAttributes{Mode: "0777"}); err != nil {
			t.Fatal(err)
		}
	}
	if n := conns.Load(); n > 1 {
		t.Errorf("%d sequential SetFileAttributesByID calls opened %d connections; want 1, reused", calls, n)
	}
}
