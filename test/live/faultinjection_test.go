//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Failure injection against a live cluster.
//
// Everything else in this package tests paths the cluster is willing to
// produce on request. The paths that matter most are the ones it will not: a
// quota create that fails after its directory already exists, a credential
// revoked between two calls, a proxy that answers with HTML instead of JSON,
// a connection dropped with the request in flight.
//
// The unit suite already injects these through MockClient, deterministically,
// and those tests are the ones that pin the branch behaviour. What they cannot
// do is inject a failure into a *real* conversation and then check the real
// state afterwards: a mock rollback deletes a mock directory, and agrees with
// itself about the result. Here the directory is real, and the assertion after
// the fault is a fresh read of the cluster.
//
// Every request that is not deliberately broken is forwarded untouched, so the
// surrounding behaviour is exactly production's.
package live

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/driver"
	"qumulo-csi-driver/pkg/qumulo"
)

// fault is a synthetic response returned instead of forwarding a request.
//
// hangup models the case no status code can: a load balancer or a cluster node
// dropping the connection with the request already in flight, where the driver
// cannot know whether the operation took effect. That ambiguity is the whole
// difficulty of the retry paths, so it has to be reproducible.
type fault struct {
	status int
	body   string
	hangup bool
}

// injector decides whether the nth matching request should fail. Returning nil
// forwards it untouched.
type injector func(method, target string, seen int) *fault

// faultProxy sits between the driver's client and the real cluster.
type faultProxy struct {
	server   *httptest.Server
	upstream string
	client   *http.Client
	inject   injector

	mu       sync.Mutex
	requests []string
	counts   map[string]int
}

// newFaultProxy starts a proxy in front of the harness's cluster.
//
// It speaks TLS on both sides: to the cluster as production does, and to the
// driver because the driver refuses an apiUrl that is not https:// -- every
// call to it carries the API token, which http:// would send in cleartext.
// This proxy used to serve plain HTTP "to keep the test free of
// certificate plumbing", and when the https requirement landed the driver rejected its URL
// before issuing a single REST call: no fault ever reached the code under
// test, four of these tests failed for a reason that had nothing to do with
// faults, and the three that only assert "some error happened" went on
// reporting PASS while testing nothing.
//
// The certificate is httptest's, generated per server, so the driver must be
// given Client() to talk to it -- see controllerThrough. Nothing here depends
// on QUMULO_TLS_INSECURE; that variable governs only the cluster side below.
//
// Do not enable HTTP/2 on this server. The hangup fault hijacks the
// connection to drop it mid-request, and http.Hijacker is HTTP/1.1 only.
func newFaultProxy(t *testing.T, h *harness, inject injector) *faultProxy {
	t.Helper()

	p := &faultProxy{
		upstream: h.APIURL,
		client: &http.Client{
			Timeout: liveTimeout,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				InsecureSkipVerify: os.Getenv("QUMULO_TLS_INSECURE") == "1",
				MinVersion:         tls.VersionTLS12,
			}},
		},
		inject: inject,
		counts: map[string]int{},
	}
	p.server = httptest.NewTLSServer(http.HandlerFunc(p.handle))
	t.Cleanup(p.server.Close)
	return p
}

// URL is the address to point a driver at.
func (p *faultProxy) URL() string { return p.server.URL }

// Client trusts this proxy's certificate. The driver under test has to use it:
// the certificate is self-signed and generated per server, so a client built
// from the default transport would fail the handshake, and one built with
// InsecureSkipVerify would make these tests depend on QUMULO_TLS_INSECURE for
// no reason.
func (p *faultProxy) Client() *http.Client {
	c := p.server.Client()
	c.Timeout = liveTimeout
	return c
}

// handle forwards a request unless the injector says otherwise.
func (p *faultProxy) handle(w http.ResponseWriter, r *http.Request) {
	// RequestURI, not URL.Path: Qumulo addresses files by percent-encoded
	// path, and re-encoding a decoded path would send /csi-live-test/... as
	// separate segments and address the wrong file.
	target := r.RequestURI

	p.mu.Lock()
	key := r.Method + " " + target
	p.requests = append(p.requests, key)
	p.counts[key]++
	seen := p.counts[key]
	p.mu.Unlock()

	if f := p.inject(r.Method, target, seen); f != nil {
		if f.hangup {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "cannot hijack", http.StatusInternalServerError)
				return
			}
			conn, _, err := hijacker.Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = io.WriteString(w, f.body)
		return
	}

	p.forward(w, r, target)
}

// forward proxies one request to the real cluster and copies the answer back.
func (p *faultProxy) forward(w http.ResponseWriter, r *http.Request, target string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "reading the request body: "+err.Error(), http.StatusBadGateway)
		return
	}

	outbound, err := http.NewRequestWithContext(
		r.Context(), r.Method, p.upstream+target, strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, "building the upstream request: "+err.Error(), http.StatusBadGateway)
		return
	}
	for name, values := range r.Header {
		for _, value := range values {
			outbound.Header.Add(name, value)
		}
	}

	resp, err := p.client.Do(outbound)
	if err != nil {
		http.Error(w, "upstream: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	for name, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// saw reports whether any request matching substr passed through.
func (p *faultProxy) saw(substr string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, request := range p.requests {
		if strings.Contains(request, substr) {
			return true
		}
	}
	return false
}

// controllerThrough builds a controller whose API calls go via the proxy.
//
// The StorageClass parameters it returns carry the proxy as apiUrl, which is
// the only lever a test has: the driver holds no cluster address and resolves
// one per request.
func controllerThrough(t *testing.T, h *harness, proxy *faultProxy) (csi.ControllerServer, map[string]string) {
	t.Helper()

	// The proxy's client, not a default one: it is the only client that trusts
	// the certificate httptest generated for this server.
	d, err := driver.NewDriver(driver.Config{
		Name:       "csi.qumulo.com",
		Version:    "live-test",
		HTTPClient: proxy.Client(),
	})
	if err != nil {
		t.Fatalf("building a driver pointed at the proxy: %v", err)
	}

	params := h.Params()
	params["apiUrl"] = proxy.URL()
	return d.ControllerServer(), params
}

// requireGone fails unless the path is absent from the cluster, polling until
// a deadline rather than looking once.
//
// The rollback it checks calls StartTreeDelete, which -- as its own doc
// comment says -- asks the cluster to *begin* a recursive delete and returns
// as soon as the job is queued. Nothing in the driver awaits the job, so how
// long the directory survives CreateVolume's return is a property of the
// cluster, not of the rollback. A single look made this test pass or fail on
// that timing: it passed three archived runs and failed on both arrays in the
// 2026-09-20 round, where polling showed the directory already absent at
// t+1 s (F1).
//
// The deadline is generous because the failure it must still catch -- a
// rollback that never ran -- leaves the directory there forever, so waiting
// longer costs nothing but a slow failure.
func requireGone(ctx context.Context, t *testing.T, h *harness, realPath, why string) {
	t.Helper()

	const (
		deadline = 30 * time.Second
		interval = time.Second
	)

	stop := time.Now().Add(deadline)
	for attempt := 1; ; attempt++ {
		_, err := h.Client.LookupFileByPath(ctx, realPath)
		switch {
		case qumulo.IsNotFound(err):
			if attempt > 1 {
				t.Logf("%s: %s gone after %d checks -- the tree delete is "+
					"asynchronous, so this is expected", why, realPath, attempt)
			}
			return
		case err != nil:
			t.Errorf("checking whether %s survived: %v", realPath, err)
			return
		}

		if time.Now().After(stop) {
			t.Errorf("%s left %s behind on the cluster: still present %s after "+
				"the RPC returned, so the rollback did not run", why, realPath, deadline)
			return
		}

		select {
		case <-ctx.Done():
			t.Errorf("waiting for %s to disappear: %v", realPath, ctx.Err())
			return
		case <-time.After(interval):
		}
	}
}

// TestLiveRollbackOnQuotaFailure is the ordering guarantee that keeps the
// array clean.
//
// CreateVolume makes a directory, then attaches a quota. If the quota fails
// the directory must go, or every failed provision leaves an unbounded
// directory nobody in Kubernetes knows about. The unit suite covers the
// branch; this covers whether the rollback actually removed anything.
func TestLiveRollbackOnQuotaFailure(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	proxy := newFaultProxy(t, h, func(method, target string, _ int) *fault {
		if method == http.MethodPost && strings.Contains(target, "/v1/files/quotas") {
			return &fault{status: http.StatusInternalServerError,
				body: `{"description":"injected quota failure"}`}
		}
		return nil
	})

	ctrl, params := controllerThrough(t, h, proxy)
	name := h.VolumeName("rollback-quota")
	realPath := path.Join(h.StoreRealPath, name)

	req := h.CreateRequest(name, gib)
	req.Parameters = params
	_, err := ctrl.CreateVolume(ctx, req)
	if err == nil {
		t.Fatal("CreateVolume succeeded despite the quota call failing")
	}
	t.Logf("CreateVolume failed as expected: %v", err)

	if !proxy.saw("/v1/files/quotas") {
		t.Fatal("the quota endpoint was never called; the fault did not land where it was aimed")
	}
	requireGone(ctx, t, h, realPath, "a CreateVolume whose quota failed")
}

// TestLiveRollbackFailureStrandsVolume: when the rollback itself cannot run,
// the driver must still report the original failure rather than claim success.
//
// The directory is stranded by definition here -- that is what the test is
// named for -- so it is tracked for cleanup and the assertion is about the
// answer the CO gets, which is the part the operator acts on.
func TestLiveRollbackFailureStrandsVolume(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	proxy := newFaultProxy(t, h, func(method, target string, _ int) *fault {
		if method == http.MethodPost && strings.Contains(target, "/v1/files/quotas") {
			return &fault{status: http.StatusInternalServerError,
				body: `{"description":"injected quota failure"}`}
		}
		// And the cleanup path too, so the rollback cannot complete.
		if strings.Contains(target, "/v1/tree-delete/jobs") {
			return &fault{status: http.StatusInternalServerError,
				body: `{"description":"injected rollback failure"}`}
		}
		return nil
	})

	ctrl, params := controllerThrough(t, h, proxy)
	name := h.VolumeName("rollback-fail")
	realPath := path.Join(h.StoreRealPath, name)

	req := h.CreateRequest(name, gib)
	req.Parameters = params
	_, err := ctrl.CreateVolume(ctx, req)
	if err == nil {
		t.Fatal("CreateVolume reported success while both the quota and its rollback failed")
	}

	// Whatever it leaves behind is this test's to clean up.
	if info, lookupErr := h.Client.LookupFileByPath(ctx, realPath); lookupErr == nil {
		t.Logf("stranded %s as expected; registering it for cleanup", realPath)
		h.Track(info.ID, realPath)
	}
}

// TestLiveCredentialRevokedMidOperation: a token that stops working between
// two calls must surface as an authentication failure, not as a generic
// Internal the operator cannot act on.
func TestLiveCredentialRevokedMidOperation(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var calls int
	proxy := newFaultProxy(t, h, func(method, target string, _ int) *fault {
		// Let the first few calls through, then revoke everything.
		calls++
		if calls > 2 {
			return &fault{status: http.StatusUnauthorized,
				body: `{"description":"injected credential revocation"}`}
		}
		return nil
	})

	ctrl, params := controllerThrough(t, h, proxy)
	name := h.VolumeName("revoked")
	req := h.CreateRequest(name, gib)
	req.Parameters = params

	_, err := ctrl.CreateVolume(ctx, req)
	if err == nil {
		t.Fatal("CreateVolume succeeded while the credential was being refused")
	}
	// The directory is created before the revocation lands, and the rollback
	// runs on the same revoked credential, so it stays. That is inherent, not
	// a driver defect; clean it up with the harness's own working credential.
	realPath := path.Join(h.StoreRealPath, name)
	if info, lookupErr := h.Client.LookupFileByPath(ctx, realPath); lookupErr == nil {
		t.Logf("stranded %s as expected; registering it for cleanup", realPath)
		h.Track(info.ID, realPath)
	}
	// Unauthenticated is the ideal answer; Internal is acceptable as long as
	// the message carries the 401, because that is what an operator greps for.
	if code := status.Code(err); code != codes.Unauthenticated && code != codes.Internal {
		t.Errorf("a revoked credential produced %v; want Unauthenticated or an Internal "+
			"naming the 401", code)
	}
	t.Logf("revoked credential surfaced as: %v", err)
}

// TestLiveErrorWithoutJSONBody: a proxy or load balancer in front of the
// cluster answers with HTML, not the API's JSON error envelope.
//
// The failure mode this guards is a decoder that panics or reports something
// misleading when the body is not what it expects. The driver has to degrade
// to "the call failed, here is the status".
func TestLiveErrorWithoutJSONBody(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	proxy := newFaultProxy(t, h, func(method, target string, _ int) *fault {
		if method == http.MethodPost {
			return &fault{status: http.StatusBadGateway,
				body: "<html><head><title>502 Bad Gateway</title></head><body>nope</body></html>"}
		}
		return nil
	})

	ctrl, params := controllerThrough(t, h, proxy)
	req := h.CreateRequest(h.VolumeName("html-error"), gib)
	req.Parameters = params

	_, err := ctrl.CreateVolume(ctx, req)
	if err == nil {
		t.Fatal("CreateVolume succeeded despite every POST returning a 502 HTML page")
	}
	if msg := status.Convert(err).Message(); msg == "" {
		t.Error("the error carries no message; an operator has nothing to act on")
	}
	t.Logf("non-JSON error surfaced as: %v", err)
}

// TestLiveConnectionDropped: the connection goes away with the request in
// flight, so the driver cannot know whether the operation took effect.
//
// The requirement is not that it succeed -- it cannot -- but that it fail in a
// way the CO will retry, and that the retry then converge.
func TestLiveConnectionDropped(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	var dropped bool
	proxy := newFaultProxy(t, h, func(method, target string, _ int) *fault {
		if !dropped && method == http.MethodPost && strings.Contains(target, "/entries/") {
			dropped = true
			return &fault{hangup: true}
		}
		return nil
	})

	ctrl, params := controllerThrough(t, h, proxy)
	name := h.VolumeName("dropped")
	realPath := path.Join(h.StoreRealPath, name)

	req := h.CreateRequest(name, gib)
	req.Parameters = params

	if _, err := ctrl.CreateVolume(ctx, req); err == nil {
		t.Fatal("CreateVolume reported success though its connection was dropped")
	} else {
		t.Logf("dropped connection surfaced as: %v", err)
	}

	// The retry: the proxy now forwards everything, and CreateVolume has to
	// converge on one volume whether or not the dropped call took effect.
	resp, err := ctrl.CreateVolume(ctx, req)
	if err != nil {
		t.Fatalf("the retry after a dropped connection failed: %v -- this is the path "+
			"csi-provisioner always takes after a timeout", err)
	}
	h.Track(h.FileIDOf(t, resp.GetVolume().GetVolumeId()), realPath)
}

// TestLiveDeleteVolumeQuotaFailureAborts.
//
// DeleteVolume must not tree-delete a directory it could not first confirm.
// If the quota call fails, the safe answer is to abort and let the CO retry --
// deleting anyway risks removing data on a wrong or stale file ID.
func TestLiveDeleteVolumeQuotaFailureAborts(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "del-quota-fault", gib)

	proxy := newFaultProxy(t, h, func(method, target string, _ int) *fault {
		if method == http.MethodDelete && strings.Contains(target, "/v1/files/quotas") {
			return &fault{status: http.StatusInternalServerError,
				body: `{"description":"injected quota delete failure"}`}
		}
		return nil
	})

	ctrl, _ := controllerThrough(t, h, proxy)

	_, err := ctrl.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: volume.VolumeID,
		Secrets:  h.Secrets(),
	})
	if err == nil {
		t.Fatal("DeleteVolume reported success though removing the quota failed")
	}
	t.Logf("quota delete failure surfaced as: %v", err)

	// The directory must still be there: aborting is the point.
	if _, lookupErr := h.Client.LookupFileByPath(ctx, volume.RealPath); lookupErr != nil {
		t.Errorf("%s was removed even though DeleteVolume aborted: %v", volume.RealPath, lookupErr)
	}
}

// TestLiveTransientFailureThenRetry: the cluster answers 503 until it recovers.
//
// The common real failure -- a node restarting, a brief leader election -- and
// the one that must not leave anything behind or need an operator.
//
// The fault stays on until this test turns it off, rather than firing once.
// A single 503 proves nothing any more: the client retries a 429 or 503 up to
// retryAttempts times inside the call, so one injected 503 is absorbed
// and CreateVolume succeeds on its first attempt -- which is what this test
// used to assert was impossible. Keeping the fault on until the first
// CreateVolume has returned exhausts that budget however large it is, so the
// call fails as a transient failure should and the retry is what recovers.
func TestLiveTransientFailureThenRetry(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	var recovered atomic.Bool
	proxy := newFaultProxy(t, h, func(method, target string, _ int) *fault {
		if !recovered.Load() && method == http.MethodPost && strings.Contains(target, "/entries/") {
			return &fault{status: http.StatusServiceUnavailable,
				body: `{"description":"injected transient failure"}`}
		}
		return nil
	})

	ctrl, params := controllerThrough(t, h, proxy)
	name := h.VolumeName("transient")
	realPath := path.Join(h.StoreRealPath, name)

	req := h.CreateRequest(name, gib)
	req.Parameters = params

	if _, err := ctrl.CreateVolume(ctx, req); err == nil {
		t.Fatal("CreateVolume succeeded on the attempt that was supposed to fail")
	}

	// The cluster recovers; nothing else about the conversation changes.
	recovered.Store(true)

	resp, err := ctrl.CreateVolume(ctx, req)
	if err != nil {
		t.Fatalf("the retry after a transient failure failed: %v", err)
	}

	volumeID := resp.GetVolume().GetVolumeId()
	fileID := h.FileIDOf(t, volumeID)
	h.Track(fileID, realPath)

	// And the volume is real, not merely reported: the quota is what proves
	// the retry completed the whole sequence rather than half of it.
	if _, err := h.Client.GetQuota(ctx, fileID); err != nil {
		t.Errorf("the retried volume has no quota: %v", err)
	}
}
