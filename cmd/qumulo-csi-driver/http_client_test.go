// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The shared HTTP client must keep enough idle connections per array that a second burst of calls reuses them.
func TestHTTPClientKeepsIdleConnectionsForConcurrentCalls(t *testing.T) {
	// Below the default worker count of the resizer and snapshotter sidecars
	// (10) and far below the provisioner's (100).
	const burst = 8

	for _, tc := range []struct {
		name     string
		insecure bool
	}{{"verified TLS (plain HTTP here)", false}, {"insecure-skip-tls-verify", true}} {
		t.Run(tc.name, func(t *testing.T) {
			var conns atomic.Int64
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(100 * time.Millisecond) // keeps the burst's calls in flight together
				_, _ = w.Write([]byte(`{}`))
			})
			srv := httptest.NewUnstartedServer(handler)
			srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
				if s == http.StateNew {
					conns.Add(1)
				}
			}
			if tc.insecure {
				srv.StartTLS()
			} else {
				srv.Start()
			}
			defer srv.Close()

			client := newHTTPClient(config{requestTimeout: 10 * time.Second, insecureTLS: tc.insecure})
			if client.Transport == nil {
				// The process-wide default transport: start it clean.
				http.DefaultTransport.(*http.Transport).CloseIdleConnections()
			}
			fire := func() {
				var wg sync.WaitGroup
				for i := 0; i < burst; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						resp, err := client.Get(srv.URL + "/v1/version")
						if err != nil {
							t.Error(err)
							return
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}()
				}
				wg.Wait()
			}
			fire()
			first := conns.Load()
			fire()
			second := conns.Load() - first
			t.Logf("first burst of %d opened %d connections, second burst opened %d more", burst, first, second)
			if second > 1 {
				t.Errorf("a second burst of %d calls opened %d new connections; want at most 1, the rest reused from the idle pool",
					burst, second)
			}
		})
	}
}
