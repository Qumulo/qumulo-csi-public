//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"qumulo-csi-driver/pkg/qumulo"
)

// dfxStopAfter forwards the first `allow` array calls to the fake cluster and
// drops every later one, closing the connection without an answer: what a
// controller that died after that many calls leaves the array with. allow < 0
// forwards everything. delay, when set, holds each call first; a caller that
// goes away during the hold lands nothing.
type dfxStopAfter struct {
	mu      sync.Mutex
	allow   int
	seen    int
	calls   []string
	delay   time.Duration
	inner   http.HandlerFunc
	onServe func(method, path string)
}

func (s *dfxStopAfter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	delay := s.delay
	s.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	s.mu.Lock()
	s.seen++
	if s.allow >= 0 && s.seen > s.allow {
		s.mu.Unlock()
		if hj, ok := w.(http.Hijacker); ok {
			if c, _, err := hj.Hijack(); err == nil {
				_ = c.Close()
			}
		}
		return
	}
	s.calls = append(s.calls, r.Method+" "+r.URL.EscapedPath())
	s.inner(w, r)
	cb := s.onServe
	s.mu.Unlock()
	if cb != nil {
		cb(r.Method, r.URL.EscapedPath())
	}
}

func (s *dfxStopAfter) set(allow int, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allow, s.delay = allow, delay
}

func dfxDedicatedExportCreate(name string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name: name, Secrets: testSecrets(),
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters: map[string]string{
			"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
			"provisioningModel": "dedicated-export",
		},
	}
}

// dfxOptedInCreate is dfxDedicatedExportCreate with the class saying its
// exports are open on purpose, for the tests about what happens after the
// preflight (refuses the open class itself).
func dfxOptedInCreate(name string) *csi.CreateVolumeRequest {
	req := dfxDedicatedExportCreate(name)
	req.Parameters["allowAnyClient"] = "true"
	return req
}

func dfxArrayState(c *fakeCluster) string {
	return fmt.Sprintf("directories=%d quotas=%d exports=%d", len(c.directories), len(c.quotas), len(c.exports))
}

// A CreateVolume stopped after any one of its array calls must be
// finished by a fresh controller's retry.
// Internal on the 409 once adoptVolume finds the stopped attempt's directory without a quota.
func TestDefect_CreateVolumeRetrySucceedsAfterAStopAtAnyStep(t *testing.T) {
	// A clean run, to learn the steps.
	clean := newFakeCluster()
	probe := &dfxStopAfter{allow: -1, inner: clean.handler()}
	srv := httptest.NewServer(probe)
	_, err := newIntegrationControllerServer(t, srv.URL).CreateVolume(context.Background(), dfxOptedInCreate("pvc-k"))
	srv.Close()
	require.NoError(t, err)
	steps := append([]string(nil), probe.calls...)
	require.NotEmpty(t, steps)

	for k := 1; k < len(steps); k++ {
		t.Run(fmt.Sprintf("stop after %d %s", k, steps[k-1]), func(t *testing.T) {
			cluster := newFakeCluster()
			sw := &dfxStopAfter{allow: k, inner: cluster.handler()}
			srv := httptest.NewServer(sw)
			defer srv.Close()
			_, _ = newIntegrationControllerServer(t, srv.URL).CreateVolume(context.Background(), dfxOptedInCreate("pvc-k"))
			sw.mu.Lock()
			left := dfxArrayState(cluster)
			sw.mu.Unlock()

			sw.set(-1, 0)
			var lastErr error
			for attempt := 1; attempt <= 3; attempt++ {
				if _, lastErr = newIntegrationControllerServer(t, srv.URL).CreateVolume(
					context.Background(), dfxOptedInCreate("pvc-k")); lastErr == nil {
					break
				}
			}
			if lastErr != nil {
				t.Errorf("stopped with %s left; 3 retries on a fresh controller all failed: %v", left, lastErr)
			}
		})
	}
}

// When the provisioner disconnects mid-create (its
// SIGTERM), the new leader's retry must succeed.
// directory on the cancelled context, so the undo never lands, and the retry fails.
func TestDefect_CreateVolumeRetrySucceedsAfterTheProvisionerDisconnects(t *testing.T) {
	cluster := newFakeCluster()
	dirCreated := make(chan struct{})
	var once sync.Once
	sw := &dfxStopAfter{allow: -1, inner: cluster.handler(), delay: 300 * time.Millisecond,
		onServe: func(method, path string) {
			if method == http.MethodPost && strings.HasSuffix(path, "/entries/") {
				once.Do(func() { close(dirCreated) })
			}
		}}
	api := httptest.NewServer(sw)
	defer api.Close()

	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeController,
		NewClient: func(_, token string) qumulo.ClientInterface { return qumulo.New(api.URL, token) }})
	require.NoError(t, err)
	handlerDone := make(chan error, 1)
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(
		func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			resp, err := h(ctx, req)
			handlerDone <- err
			return resp, err
		}))
	csi.RegisterControllerServer(gs, d.ControllerServer())
	ln, err := net.Listen("unix", filepath.Join(t.TempDir(), "csi.sock"))
	require.NoError(t, err)
	go func() { _ = gs.Serve(ln) }()
	defer gs.Stop()

	conn, err := grpc.NewClient("unix://"+ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	go func() {
		_, _ = csi.NewControllerClient(conn).CreateVolume(context.Background(), dfxOptedInCreate("pvc-sigterm"))
	}()
	select {
	case <-dirCreated:
	case <-time.After(10 * time.Second):
		t.Fatal("CreateVolume never created its directory")
	}
	_ = conn.Close() // the provisioner exits
	select {
	case err := <-handlerDone:
		t.Logf("the handler returned: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the handler never returned after the provisioner disconnected")
	}
	time.Sleep(500 * time.Millisecond) // anything still in flight lands
	sw.mu.Lock()
	t.Logf("array calls: %v; left: %s", sw.calls, dfxArrayState(cluster))
	sw.mu.Unlock()

	// The new leader's retry, on a fresh controller, against a healthy array.
	sw.set(-1, 0)
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if _, lastErr = newIntegrationControllerServer(t, api.URL).CreateVolume(
			context.Background(), dfxOptedInCreate("pvc-sigterm")); lastErr == nil {
			break
		}
	}
	if lastErr != nil {
		t.Errorf("3 retries after the disconnect all failed: %v", lastErr)
	}
}
