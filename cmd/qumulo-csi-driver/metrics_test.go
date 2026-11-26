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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/metrics"
	"qumulo-csi-driver/pkg/version"
)

// Off unless asked for. The flag defaults empty and reads METRICS_ADDRESS,
// exactly as -enable-inventory reads ENABLE_INVENTORY, so a chart sets it
// the same way every other opt-in feature is set.
func TestMetricsAddressIsOffByDefaultAndEnvWired(t *testing.T) {
	cfg, err := parseFlags(nil)
	require.NoError(t, err)
	assert.Empty(t, cfg.metricsAddress, "metrics must be off unless asked for")

	cfg, err = parseFlags([]string{"-metrics-address=:29654"})
	require.NoError(t, err)
	assert.Equal(t, ":29654", cfg.metricsAddress)

	t.Setenv("METRICS_ADDRESS", ":29655")
	cfg, err = parseFlags(nil)
	require.NoError(t, err)
	assert.Equal(t, ":29655", cfg.metricsAddress)

	// An explicit flag still wins over the environment, so a chart that sets
	// both does not depend on which one the binary read first.
	cfg, err = parseFlags([]string{"-metrics-address=:29656"})
	require.NoError(t, err)
	assert.Equal(t, ":29656", cfg.metricsAddress)
}

func TestMetricsServerServesMetricsAndHealthz(t *testing.T) {
	registry := metrics.New(version.Get())
	server := httptest.NewServer(newMetricsServer("", registry).Handler)
	defer server.Close()

	for _, tc := range []struct {
		path     string
		contains string
	}{
		{path: "/metrics", contains: "csi_qumulo_build_info"},
		{path: "/healthz", contains: "ok"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := http.Get(server.URL + tc.path)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Contains(t, string(body), tc.contains)
		})
	}
}

// /healthz means "this process is alive" and nothing more, for the same
// reason Probe does: one controller serves many arrays and is never told the
// full set, so a health endpoint that failed on one unreachable array would
// restart a controller still serving every other one.
func TestHealthzIsProcessLivenessOnly(t *testing.T) {
	server := httptest.NewServer(newMetricsServer("", metrics.New(version.Get())).Handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"healthz must not depend on any array; reachability is a gauge")
}

// A port already in use must log and carry on. Metrics are observability;
// taking provisioning down for them would be the wrong trade, and it is the
// same judgement -enable-capacity makes about missing Kubernetes access.
func TestAnUnusableMetricsPortDoesNotStopTheDriver(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()

	server := newMetricsServer(occupied.Addr().String(), metrics.New(version.Get()))

	done := make(chan struct{})
	go func() {
		serveMetrics(server)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serveMetrics did not return after failing to listen")
	}
}

func TestStopMetricsServerToleratesNoServer(t *testing.T) {
	assert.NotPanics(t, func() { stopMetricsServer(nil) })
}

func TestStopMetricsServerDrainsTheListener(t *testing.T) {
	server := newMetricsServer("127.0.0.1:0", metrics.New(version.Get()))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	served := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(served)
	}()

	stopMetricsServer(server)

	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("the metrics listener did not drain")
	}
	// Shutdown is idempotent, which matters because the signal handler may
	// run alongside a process that is already exiting.
	assert.NotPanics(t, func() { stopMetricsServer(server) })
}

// The typed-nil trap: a nil *metrics.Registry assigned straight into an
// interface field is not a nil interface, and every guarded call site in
// pkg/qumulo would dereference it.
func TestObserverOrNilIsATrulyNilInterface(t *testing.T) {
	assert.Nil(t, observerOrNil(nil))
	assert.NotNil(t, observerOrNil(metrics.New(version.Get())))
}

// The metrics drain must stay well inside the RPC drain, because the two run
// one after the other inside one terminationGracePeriodSeconds.
func TestMetricsShutdownFitsInsideTheRPCDrain(t *testing.T) {
	assert.Less(t, metricsShutdownTimeout, shutdownGracePeriod)
}

func TestMetricsServerSetsAReadHeaderTimeout(t *testing.T) {
	server := newMetricsServer(":29654", metrics.New(version.Get()))
	assert.Equal(t, ":29654", server.Addr)
	assert.Positive(t, server.ReadHeaderTimeout,
		"a server with no header timeout is a slowloris away from holding a connection forever")
}
