// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// The shared client reports a proxy's refusal of the CONNECT to an
// array as a typed *qumulo.ProxyRefusedError inside the usual *url.Error, so
// the driver can tell it from an array that is down.
func TestHTTPClientTypesAProxyRefusal(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			t.Errorf("proxy got %s, want CONNECT", r.Method)
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer proxy.Close()
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}

	client := newHTTPClient(config{requestTimeout: 10 * time.Second})
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", client.Transport)
	}
	// newHTTPClient reads the proxy from the environment, once per process;
	// point this transport at the test proxy directly.
	tr.Proxy = http.ProxyURL(proxyURL)

	resp, err := client.Get("https://array.example.invalid:8000/v1/version")
	if err == nil {
		resp.Body.Close()
		t.Fatal("call through a refusing proxy succeeded")
	}
	var transport *url.Error
	if !errors.As(err, &transport) {
		t.Errorf("error %T (%v) is not a *url.Error", err, err)
	}
	var refused *qumulo.ProxyRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("error %v carries no *qumulo.ProxyRefusedError", err)
	}
	if refused.StatusCode != http.StatusForbidden || !refused.Final() {
		t.Errorf("refusal = %+v, want a final 403", refused)
	}
	if refused.Target != "array.example.invalid:8000" || refused.Proxy != proxyURL.Host {
		t.Errorf("refusal names proxy %q target %q, want %q and array.example.invalid:8000", refused.Proxy, refused.Target, proxyURL.Host)
	}
}
