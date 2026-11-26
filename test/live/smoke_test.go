//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package live

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"

	"qumulo-csi-driver/pkg/qumulo"
)

// TestLiveClusterReachable dials the cluster at the network level only --
// no HTTP request, no token -- so a failure here means "the network or the
// port is the problem," as distinct from every other test in this file,
// where a failure means "the API answered but did not like something."
// Separating the two matters in the field: QUMULO_API_URL pointing at an
// unreachable host and an expired token produce the same symptom
// (CreateVolume fails) but need entirely different fixes.
func TestLiveClusterReachable(t *testing.T) {
	h := newHarness(t)

	host := strings.TrimPrefix(strings.TrimPrefix(h.APIURL, "https://"), "http://")
	if !strings.Contains(host, ":") {
		host = net.JoinHostPort(host, "8000")
	}

	dialer := &net.Dialer{Timeout: liveTimeout}

	var (
		conn net.Conn
		err  error
	)
	if strings.HasPrefix(h.APIURL, "http://") {
		conn, err = dialer.Dial("tcp", host)
	} else {
		tlsConfig := &tls.Config{}
		if transport, ok := h.Client.HTTPClient.Transport.(*http.Transport); ok && transport.TLSClientConfig != nil {
			tlsConfig = transport.TLSClientConfig
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", host, tlsConfig)
	}
	if err != nil {
		t.Fatalf("dialing %s: %v -- this is a network/routing/TLS problem, not a credential one", host, err)
	}
	conn.Close()
}

// clientWithToken builds a *qumulo.Client against h's cluster using token in
// place of h's own, sharing h's TLS transport. For a test that deliberately
// wants a missing or wrong credential against an otherwise-real cluster.
func clientWithToken(h *harness, token string) *qumulo.Client {
	client := qumulo.New(h.APIURL, token)
	client.HTTPClient = h.Client.HTTPClient
	return client
}

// TestLiveVersionEndpointAuthRequirement is a discovery/pin test, not a
// security assertion, and its history is worth keeping: it was originally
// written to confirm /v1/version requires a credential, on the strength of
// pkg/qumulo/client.go's own comment calling it "the cheapest authenticated
// endpoint" and HealthCheck's doc comment claiming HealthCheck "accepts this
// client's credentials." Run for real against Qumulo Core 7.9.3, it found
// both of those false: /v1/version returns 200 with no Authorization header
// at all, and with a garbage one alike (confirmed independently with curl).
// See CONFIRMED-DEFECTS.md's HealthCheck entry.
//
// So this test now pins the actual behavior instead of the assumed one: it
// fails loudly if a future Qumulo Core release, or a future change to
// versionPath, ever starts requiring auth here -- which would make
// HealthCheck's doc comment true again and this test (and
// TestLiveInvalidTokenRejected below, which exists because of this finding)
// worth revisiting together.
func TestLiveVersionEndpointAuthRequirement(t *testing.T) {
	h := newHarness(t)
	client := clientWithToken(h, "")

	if err := client.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck with no token failed against %s: %v -- if /v1/version now requires "+
			"auth, HealthCheck's doc comment ('accepts this client's credentials') just became true; "+
			"update this test and pkg/qumulo/client.go's comments together, and see if "+
			"TestLiveInvalidTokenRejected can move back to HealthCheck", h.APIURL, err)
	}
	t.Logf("confirmed: %s/v1/version answers with no Authorization header -- HealthCheck cannot, "+
		"by itself, verify a credential is valid; see CONFIRMED-DEFECTS.md", h.APIURL)
}

// TestLiveTokenAccepted confirms the configured token is valid and current.
// This is the test whose failure means "go rotate QUMULO_TOKEN_FILE," as
// opposed to TestLiveClusterReachable (network) or a downstream test
// failing for an unrelated reason after this one silently also failed.
//
// GetCapacity, not HealthCheck, is what actually proves the token: it hits
// /v1/file-system, confirmed by hand (curl, no token) to return 401 on this
// cluster, where HealthCheck's /v1/version answers regardless of credential
// -- see TestLiveVersionEndpointAuthRequirement. HealthCheck is still called
// here too, because a real Probe implementation would call it, and its
// failure is worth distinguishing from GetCapacity's.
func TestLiveTokenAccepted(t *testing.T) {
	h := newHarness(t)

	if err := h.Client.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck with the configured token failed against %s: %v", h.APIURL, err)
	}
	if _, err := h.Client.GetCapacity(context.Background()); err != nil {
		t.Fatalf("GetCapacity with the configured token failed against %s: %v -- this is the call "+
			"that actually exercises the credential", h.APIURL, err)
	}
}

// TestLiveInvalidTokenRejected confirms a wrong credential is rejected
// somewhere reachable from this driver, even though HealthCheck's own
// endpoint does not check it (TestLiveVersionEndpointAuthRequirement).
// GetCapacity is used instead of HealthCheck for the same reason
// TestLiveTokenAccepted checks it: /v1/file-system is where "is this token
// any good" actually gets answered today.
func TestLiveInvalidTokenRejected(t *testing.T) {
	h := newHarness(t)
	client := clientWithToken(h, "deliberately-invalid-token-for-csi-live-test")

	_, err := client.GetCapacity(context.Background())
	if err == nil {
		t.Fatalf("GetCapacity with an invalid token succeeded against %s", h.APIURL)
	}

	var apiErr *qumulo.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("GetCapacity with an invalid token failed, but not with an APIError: %v", err)
	}
	if apiErr.StatusCode != http.StatusUnauthorized && apiErr.StatusCode != http.StatusForbidden {
		t.Fatalf("GetCapacity with an invalid token returned HTTP %d; expected 401 or 403", apiErr.StatusCode)
	}
}
