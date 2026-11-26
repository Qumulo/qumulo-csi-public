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
)

// ProxyRefusedError is an HTTPS proxy's answer other than 200 to the CONNECT
// that would have opened a tunnel to the array: the request never reached the
// array, the proxy said why.
//
// Go's transport reports that answer as an untyped error holding only the
// status text ("Forbidden"), so it looked like any other transport failure
// and was retried as Unavailable for ever -- a StorageClass naming a host the
// proxy refuses kept its PVC Pending with no hint that the proxy, not the
// array, was the problem. Typed, a caller can tell a refusal from a
// proxy that is itself down or whose upstream timed out.
type ProxyRefusedError struct {
	// Proxy is the proxy's host:port, never its credentials.
	Proxy string
	// Target is the host:port the tunnel was asked for.
	Target string
	// StatusCode and Status are the proxy's answer.
	StatusCode int
	Status     string
}

func (e *ProxyRefusedError) Error() string {
	return fmt.Sprintf("proxy %s refused to connect to %s: %s", e.Proxy, e.Target, e.Status)
}

// Final reports whether retrying the same request through the same proxy
// cannot succeed: the proxy forbids the destination (403) or wants
// credentials this driver does not send (407). Either is fixed by changing
// the proxy's rules or the controller's proxy settings (controller.proxy,
// NO_PROXY), not by waiting. Every other status -- 502, 503 and 504 above
// all, a proxy whose upstream is restarting or slow -- may pass on a retry.
func (e *ProxyRefusedError) Final() bool {
	return e.StatusCode == http.StatusForbidden || e.StatusCode == http.StatusProxyAuthRequired
}

// OnProxyConnectResponse is an http.Transport.OnProxyConnectResponse hook
// that turns a non-200 CONNECT answer into a *ProxyRefusedError. The
// transport wraps it, like any dial failure, in the *url.Error a caller
// already reads as "the array did not answer", so nothing that only asks
// that question changes.
func OnProxyConnectResponse(_ context.Context, proxyURL *url.URL, connectReq *http.Request, resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	proxy := ""
	if proxyURL != nil {
		proxy = proxyURL.Host
	}
	target := ""
	if connectReq != nil {
		// The transport's CONNECT carries the target as Host (and as an
		// opaque URL), not as URL.Host.
		target = connectReq.Host
	}
	return &ProxyRefusedError{Proxy: proxy, Target: target, StatusCode: resp.StatusCode, Status: resp.Status}
}
