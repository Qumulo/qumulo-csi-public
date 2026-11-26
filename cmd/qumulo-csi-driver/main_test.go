// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/driver"
	"qumulo-csi-driver/pkg/metrics"
	"qumulo-csi-driver/pkg/version"
)

// fakeServer stands in for the gRPC server so the escalation can be tested
// without standing one up. GracefulStop blocks until Stop is called, which is
// how the real one behaves for a wedged RPC.
type fakeServer struct {
	drainFor time.Duration // how long the drain takes; 0 returns at once
	forced   atomic.Bool
	release  chan struct{}
}

func newFakeServer(drainFor time.Duration) *fakeServer {
	return &fakeServer{drainFor: drainFor, release: make(chan struct{})}
}

func (f *fakeServer) GracefulStop() {
	if f.drainFor == 0 {
		return
	}
	select {
	case <-time.After(f.drainFor):
	case <-f.release: // Stop() unblocks a drain that would never finish
	}
}

func (f *fakeServer) Stop() {
	f.forced.Store(true)
	close(f.release)
}

// A drain that finishes in time must not be interrupted. Interrupting a
// CreateVolume midway leaves a directory the CO has no volume ID for, which
// is the whole reason graceful comes first.
func TestShutdownLetsInFlightRPCsFinish(t *testing.T) {
	server := newFakeServer(10 * time.Millisecond)

	drained := shutdown(server, time.Second, nil, 0)

	assert.True(t, drained, "a drain that completes in time must be allowed to")
	assert.False(t, server.forced.Load(), "Stop must not be called when the drain finished")
}

// A drain that does not finish must be forced.
//
// This was a bare GracefulStop with no deadline, so one wedged mount held the
// process for the whole terminationGracePeriodSeconds and the pod was
// SIGKILLed at the end of it anyway -- the drain achieved nothing and the
// shutdown looked like a hang.
func TestShutdownForcesAWedgedDrain(t *testing.T) {
	// Longer than any test would wait: this drain never finishes on its own.
	server := newFakeServer(time.Hour)

	start := time.Now()
	drained := shutdown(server, 20*time.Millisecond, nil, 0)
	elapsed := time.Since(start)

	assert.False(t, drained, "a drain that outlasts the grace period must be reported as forced")
	assert.True(t, server.forced.Load(), "Stop must be called once the grace period expires")
	assert.Less(t, elapsed, 5*time.Second, "shutdown must not wait on a drain that never finishes")
}

// fakePromotes is the replication server's promote count, for shutdown.
type fakePromotes struct {
	inFlight int
	doneIn   time.Duration // how long the promotes take; 0 never finish
}

func (f *fakePromotes) PromotesInFlight() int { return f.inFlight }

func (f *fakePromotes) WaitForPromotes(timeout time.Duration) bool {
	if f.doneIn == 0 || f.doneIn > timeout {
		time.Sleep(timeout)
		return false
	}
	time.Sleep(f.doneIn)
	return true
}

// A planned promote can outlast the drain: stopped after the freeze,
// both copies stay read-only until the retry lands on a new controller. So
// with one in flight, shutdown waits for it -- here the drain then completes
// on its own, the promote having been the RPC it was waiting on.
func TestShutdownWaitsForAnInFlightPromote(t *testing.T) {
	server := newFakeServer(80 * time.Millisecond)
	promotes := &fakePromotes{inFlight: 1, doneIn: 80 * time.Millisecond}

	drained := shutdown(server, 10*time.Millisecond, promotes, time.Second)

	assert.True(t, drained, "the drain completed once the promote did")
	assert.False(t, server.forced.Load(), "nothing was cut off")
}

// A promote that never ends is cut off at the promote wait, not held for
// ever: the kubelet's SIGKILL would end it anyway, with nothing logged.
func TestShutdownCutsOffAWedgedPromote(t *testing.T) {
	server := newFakeServer(time.Hour)
	promotes := &fakePromotes{inFlight: 1}

	start := time.Now()
	drained := shutdown(server, 10*time.Millisecond, promotes, 30*time.Millisecond)

	assert.False(t, drained)
	assert.True(t, server.forced.Load())
	assert.Less(t, time.Since(start), 5*time.Second)
}

// The escalation deadline has to stay comfortably inside the grace period the
// chart sets, or forcing never happens and the kubelet SIGKILLs instead --
// which is the behaviour this replaced. The drain, then a promote's wait,
// then the forced stop and exit all fit inside it.
func TestShutdownGracePeriodIsInsideTheChartsTerminationGrace(t *testing.T) {
	const chartTerminationGrace = 90 * time.Second

	require.Less(t, shutdownGracePeriod, chartTerminationGrace,
		"shutdownGracePeriod must be less than the chart's terminationGracePeriodSeconds")
	assert.LessOrEqual(t, shutdownGracePeriod+promoteShutdownWait, chartTerminationGrace-5*time.Second,
		"leave real headroom: the drain, a promote's wait, the forced stop and process exit all happen inside it")
	assert.GreaterOrEqual(t, promoteShutdownWait, driver.PlannedPromoteBudget,
		"a promote that started just before the signal can still finish")
}

// A handler that panics answers Internal, is counted, and the process
// goes on. Any handler: this one has no reachable panic to fix.
func TestRecoverRPCAnswersInternalForAPanickingHandler(t *testing.T) {
	registry := metrics.New(version.Info{})
	intercept := recoverRPC(registry)
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/CreateVolume"}

	resp, err := intercept(context.Background(), nil, info, func(context.Context, any) (any, error) {
		panic("handler exploded")
	})
	if resp != nil || status.Code(err) != codes.Internal {
		t.Fatalf("a panicking handler answered (%v, %v), want Internal", resp, err)
	}
	if err := testutil.GatherAndCompare(registry.Gatherer(), strings.NewReader(`
# HELP csi_qumulo_rpc_panics_total RPC handlers that panicked, by full gRPC method. Each was recovered and answered Internal; any increase is a driver bug.
# TYPE csi_qumulo_rpc_panics_total counter
csi_qumulo_rpc_panics_total{method="/csi.v1.Controller/CreateVolume"} 1
`), "csi_qumulo_rpc_panics_total"); err != nil {
		t.Fatal(err)
	}

	resp, err = intercept(context.Background(), nil, info, func(context.Context, any) (any, error) { return "ok", nil })
	if resp != "ok" || err != nil {
		t.Fatalf("a handler that does not panic answered (%v, %v)", resp, err)
	}
}

// Both transports keep Go's defaults -- the proxy, an idle expiry, a
// handshake bound -- and both bound the dial.
func TestHTTPClientTransportIsBoundedOnBothPaths(t *testing.T) {
	for _, insecure := range []bool{false, true} {
		c := newHTTPClient(config{requestTimeout: 10 * time.Second, insecureTLS: insecure})
		tr, ok := c.Transport.(*http.Transport)
		require.True(t, ok, "insecure=%t: no *http.Transport", insecure)
		assert.NotNil(t, tr.Proxy, "insecure=%t: HTTPS_PROXY is ignored", insecure)
		assert.NotZero(t, tr.IdleConnTimeout, "insecure=%t: idle connections never expire", insecure)
		assert.Equal(t, tlsHandshakeTimeout, tr.TLSHandshakeTimeout, "insecure=%t", insecure)
		assert.NotNil(t, tr.DialContext, "insecure=%t", insecure)
		assert.Equal(t, insecure, tr.TLSClientConfig != nil && tr.TLSClientConfig.InsecureSkipVerify, "insecure=%t", insecure)
	}
}

// The snapshot name prefix defaults to csi-snapshotter's own, follows
// the flag, and refuses a value that would match every name or none.
func TestSnapshotNamePrefixDefaultsToTheSnapshottersAndIsChecked(t *testing.T) {
	cfg, err := parseFlags(nil)
	require.NoError(t, err)
	assert.Equal(t, driver.DefaultSnapshotNamePrefix, cfg.snapshotPrefix)

	cfg, err = parseFlags([]string{"-snapshot-name-prefix=backup"})
	require.NoError(t, err)
	assert.Equal(t, "backup", cfg.snapshotPrefix)

	for _, bad := range []string{"", "a b", "a/b"} {
		_, err := parseFlags([]string{"-snapshot-name-prefix=" + bad})
		assert.Error(t, err, "prefix %q", bad)
	}
}

// A duration from the environment is used when it parses, and a value that
// does not parse is reported on stderr and the default kept, so a typo in a
// tuning knob does not crash-loop every node plugin. UNMOUNT_TIMEOUT is the
// variable that reaches it through parseFlags.
func TestEnvDurationOrUsesAValidValueAndFallsBackOnGarbage(t *testing.T) {
	const key = "QUMULO_CSI_TEST_DURATION"

	t.Setenv(key, "")
	assert.Equal(t, 7*time.Second, envDurationOr(key, 7*time.Second), "unset")

	t.Setenv(key, "45s")
	assert.Equal(t, 45*time.Second, envDurationOr(key, 7*time.Second), "a valid value")

	t.Setenv(key, "forty-five")
	var got time.Duration
	warning := captureStderr(t, func() { got = envDurationOr(key, 7*time.Second) })
	assert.Equal(t, 7*time.Second, got, "garbage keeps the default")
	assert.Contains(t, warning, key, "the warning names the variable")
	assert.Contains(t, warning, "forty-five")

	t.Setenv("UNMOUNT_TIMEOUT", "2m")
	cfg, err := parseFlags(nil)
	require.NoError(t, err)
	assert.Equal(t, 2*time.Minute, cfg.unmountTimeout)

	t.Setenv("UNMOUNT_TIMEOUT", "2 minutes")
	captureStderr(t, func() { cfg, err = parseFlags(nil) })
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, cfg.unmountTimeout)
}

// captureStderr returns what fn wrote to os.Stderr.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	saved := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = saved }()
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	require.NoError(t, w.Close())
	return <-done
}

// Replication revert snapshots expire after 7 days by default,
// the flag and REVERT_SNAPSHOT_RETENTION change it, 0 turns it off, and a
// negative value -- an expiration in the past, deleting the snapshot at
// once -- is refused.
func TestRevertSnapshotRetentionDefaultsToAWeekAndIsChecked(t *testing.T) {
	cfg, err := parseFlags(nil)
	require.NoError(t, err)
	assert.Equal(t, 7*24*time.Hour, cfg.revertRetention)

	cfg, err = parseFlags([]string{"-revert-snapshot-retention=0s"})
	require.NoError(t, err)
	assert.Zero(t, cfg.revertRetention)

	t.Setenv("REVERT_SNAPSHOT_RETENTION", "48h")
	cfg, err = parseFlags(nil)
	require.NoError(t, err)
	assert.Equal(t, 48*time.Hour, cfg.revertRetention)

	_, err = parseFlags([]string{"-revert-snapshot-retention=-1h"})
	assert.Error(t, err)
}
