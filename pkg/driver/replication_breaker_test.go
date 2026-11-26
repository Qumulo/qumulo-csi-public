// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Promote against a peer behind the real REST client and circuit breaker
// (with), and the classification of a malformed reply.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// breakerWire puts one fabric array behind a real REST client and a real
// circuit breaker, the one-per-process kind the binary builds: every
// call the fabric serves for that array first makes one real round trip
// through them. A cut in the network, the breaker's fail-fast and the
// client's own error wrapping are then exactly what the driver sees.
//
// Other volumes' traffic to the same array -- which opened and held the
// breaker in the live run -- is played by calls on client directly.
type breakerWire struct {
	t       *testing.T
	breaker *qumulo.Breaker
	client  *qumulo.Client

	// cut fails every round trip in transport. cutAsks, when positive, ends
	// the cut once that many of the array's own (fabric) round trips have
	// failed in it, so a test can end a blip between two asks without
	// racing a timer.
	cut     atomic.Bool
	cutAsks atomic.Int32
	// hang makes a fabric round trip block that long, or until its context
	// ends, and then fail: an API that accepts the connection and never
	// answers.
	hang time.Duration
	// malformed answers every fabric round trip 200 with a body that is not
	// the listing asked for.
	malformed atomic.Bool
	// asks counts the fabric round trips that reached the transport.
	asks atomic.Int32

	// late calls are in flight from before the cut and fail once release
	// is closed, which happens right after the first answered fabric call:
	// the request timeouts of other volumes' calls landing after the array
	// is back.
	late     sync.WaitGroup
	entered  chan struct{}
	release  chan struct{}
	released sync.Once
}

func newBreakerWire(t *testing.T, a *fakeArray) *breakerWire {
	w := &breakerWire{t: t, breaker: qumulo.NewBreaker(), entered: make(chan struct{}, 16), release: make(chan struct{})}
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("i/o timeout")}
	w.client = qumulo.New("https://"+a.addr+":8000", "t")
	w.client.Breaker = w.breaker
	w.client.HTTPClient = &http.Client{Transport: breakerWireTransport(func(req *http.Request) (*http.Response, error) {
		u := req.URL.String()
		fabric := strings.Contains(u, "/replication/")
		switch {
		case strings.Contains(u, "late"):
			w.entered <- struct{}{}
			<-w.release
			return nil, dialErr
		case fabric:
			w.asks.Add(1)
		}
		if w.cut.Load() {
			if fabric && w.cutAsks.Load() > 0 && w.cutAsks.Add(-1) == 0 {
				w.cut.Store(false)
			}
			return nil, dialErr
		}
		if fabric && w.hang > 0 {
			timer := time.NewTimer(w.hang)
			defer timer.Stop()
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-timer.C:
				return nil, dialErr
			}
		}
		body := `[]`
		if fabric && w.malformed.Load() {
			body = `{"not":"a list"}`
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	a.wire = func(ctx context.Context) error {
		_, err := w.client.ListSourceStatus(ctx)
		if err == nil {
			w.released.Do(func() {
				close(w.release)
				w.late.Wait()
			})
		}
		return err
	}
	return w
}

type breakerWireTransport func(*http.Request) (*http.Response, error)

func (f breakerWireTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// startLate starts n other-volume calls that are in flight until the array
// first answers again, and returns once each has gone out.
func (w *breakerWire) startLate(n int) {
	for i := 0; i < n; i++ {
		w.late.Add(1)
		go func() {
			defer w.late.Done()
			_, _ = w.client.LookupFileByPath(context.Background(), "/late")
		}()
		<-w.entered
	}
}

// openBreaker fails two other-volume calls in the cut, which opens the
// breaker, and checks that the next ordinary call is failed fast.
func (w *breakerWire) openBreaker() {
	w.t.Helper()
	require.True(w.t, w.cut.Load(), "the breaker opens on calls the cut fails")
	for i := 0; i < 2; i++ {
		_, err := w.client.LookupFileByPath(context.Background(), "/other")
		require.Error(w.t, err)
	}
	require.True(w.t, w.failsFast(), "two unanswered calls open the breaker")
}

// failsFast reports whether an ordinary call is answered by the breaker.
func (w *breakerWire) failsFast() bool {
	_, err := w.client.LookupFileByPath(context.Background(), "/other")
	return err != nil && strings.Contains(err.Error(), "not calling it again yet")
}

// promoteAsCSIAddons is one failover through the csi-addons controller: a
// planned promote, and on FailedPrecondition the forced re-call it makes at
// once. It returns the error and whether the forced re-call was made.
func (s *replScenario) promoteAsCSIAddons(t *testing.T) (error, bool) {
	t.Helper()
	err := s.promote(t, s.handleB(t), classToA(), false)
	if status.Code(err) != codes.FailedPrecondition {
		return err, false
	}
	t.Logf("planned promote answered %v; csi-addons re-calls with force", err)
	return s.promote(t, s.handleB(t), classToA(), true), true
}

// breakerScenario is a protected volume whose promote runs on B with A, the
// source, behind a real breaker, and the promote's waits scaled down.
func breakerScenario(t *testing.T) (*replScenario, *fakeRel, *breakerWire) {
	s := newReplScenario(t)
	s.server.drainBudget = 5 * time.Second
	s.server.peerRetryWait = 20 * time.Millisecond
	s.server.peerCallTimeout = 50 * time.Millisecond
	rel := s.protect(t, classToB())
	rel.write() // acknowledged on A, not yet replicated
	return s, rel, newBreakerWire(t, s.a)
}

// with, the live sequence: the source's REST API
// went silent inside the promoting controller; other volumes' calls opened
// the breaker; calls in flight from before the cut failed after it ended.
// Every ask of the promote's retry loop was answered by the breaker, the
// planned promote answered FailedPrecondition, and csi-addons' forced
// re-call skipped the freeze and the final sync. A blip shorter than the
// promote's retries must give the planned promote.
func TestPromoteReachesASourceWhoseBreakerIsOpen(t *testing.T) {
	s, rel, w := breakerScenario(t)
	w.startLate(2)
	w.cut.Store(true)
	w.openBreaker()
	w.cutAsks.Store(2) // the first two asks meet the cut, the third the array

	err := s.promote(t, s.handleB(t), classToA(), false)

	require.NoError(t, err, "the asks outlast the blip, so no FailedPrecondition and no forced re-call")
	assert.True(t, rel.sourceRO, "A was frozen")
	assert.False(t, rel.targetRO, "B promoted")
	assert.Equal(t, rel.gen, rel.replicatedGen, "A's last writes reached B before the promote")
	assert.False(t, w.failsFast(), "the array answered, and the failures of calls older than that answer "+
		"did not reopen the breaker")
}

// kept, with: a source that stays silent is still promoted by
// force, every ask a real call, and the promote's asks do not hold the
// breaker shut against the rest of the controller: ordinary calls are still
// failed fast throughout.
func TestPromoteOfASourceThatStaysDownBehindItsBreaker(t *testing.T) {
	s, rel, w := breakerScenario(t)
	w.cut.Store(true)
	w.openBreaker()

	err := s.promote(t, s.handleB(t), classToA(), false)
	requireCode(t, err, codes.FailedPrecondition)
	planned := w.asks.Load()
	assert.EqualValues(t, 1+s.server.peerRetries, planned, "each ask reached the array")
	assert.True(t, w.failsFast(), "ordinary calls are still failed fast")

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	assert.EqualValues(t, 2*(1+s.server.peerRetries), w.asks.Load(), "the forced call asked for real too")
	assert.False(t, rel.targetRO, "B promoted at the retained recovery point")
	assert.True(t, w.failsFast(), "ordinary calls are still failed fast")
}

// The asks are real calls now, so a hung source costs each of them the whole
// call timeout. The promote still answers with its own code before the
// caller's deadline.
func TestPromoteAgainstAHungSourceBehindItsBreakerAnswersInTime(t *testing.T) {
	s, _, w := breakerScenario(t)
	s.server.drainBudget = 10 * time.Second
	s.server.peerRetryWait = 40 * time.Millisecond
	s.server.peerCallTimeout = 100 * time.Millisecond
	s.server.replyMargin = 50 * time.Millisecond
	w.cut.Store(true)
	w.openBreaker()
	w.cut.Store(false)
	w.hang = s.server.peerCallTimeout

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	_, err := s.server.PromoteVolume(ctx, &replication.PromoteVolumeRequest{
		VolumeId: s.handleB(t), Parameters: classToA(), Secrets: replSecrets(),
	})

	requireCode(t, err, codes.FailedPrecondition)
	assert.True(t, time.Now().Before(deadline), "answered before the caller's deadline")
}

// With the breaker in the path: a frozen source whose peer stays
// silent keeps its freeze, force or not. The asks now reach the peer, and
// one that does not answer is still never a licence to unfreeze.
func TestAFrozenSourceKeepsItsFreezeWhenThePeerBehindItsBreakerStaysDown(t *testing.T) {
	s := newReplScenario(t)
	s.server.peerRetryWait = 5 * time.Millisecond
	rel := s.protect(t, classToB())
	rel.sourceRO = true // a planned promote froze A, and B never took over
	w := newBreakerWire(t, s.b)
	w.cut.Store(true)
	w.openBreaker()

	for _, force := range []bool{false, true} {
		err := s.promote(t, s.handleA, classToB(), force)
		requireCode(t, err, codes.Unavailable)
		assert.True(t, rel.sourceRO, "force=%v: the freeze is held while the peer cannot be read", force)
	}
}

// A reply the client cannot decode is the array answering, not the
// array missing. Classed as unreachable it was retried as a silent peer and
// then answered by the local view, which makes a planned promote
// FailedPrecondition and the forced re-call skip the freeze: the
// outcome from an array that answered every call.
func TestPromoteWithAMalformedSourceReplyIsNeverForced(t *testing.T) {
	s, rel, w := breakerScenario(t)
	w.malformed.Store(true)

	err, forced := s.promoteAsCSIAddons(t)

	requireCode(t, err, codes.Internal)
	assert.False(t, forced, "a malformed reply is not a reason to force")
	assert.NotContains(t, err.Error(), "unreachable")
	assert.Contains(t, err.Error(), "cannot use")
	assert.EqualValues(t, 1, w.asks.Load(), "a malformed reply is not asked again as a silent peer")
	assert.True(t, rel.targetRO, "B was not made writable")

	// Nor does a hand-written forced call take the forced path.
	requireCode(t, s.promote(t, s.handleB(t), classToA(), true), codes.Internal)
	assert.True(t, rel.targetRO, "B was not made writable")
	assert.False(t, w.failsFast(), "an array that answers, however badly, never opens the breaker")
}

// malformedReverseArray answers Reverse with a reply the client could not
// decode, after the array may well have reversed the relationship.
type malformedReverseArray struct{ *fakeArray }

func (m *malformedReverseArray) Reverse(context.Context, string, string, int) (qumulo.Relationship, error) {
	m.record("Reverse")
	return qumulo.Relationship{}, &qumulo.MalformedReplyError{Err: errors.New("decoding response from /reverse: unexpected EOF")}
}

// The force path the fix must not open: a Reverse whose reply is
// malformed may have reversed the relationship, so even a forced resync must
// not take it as a refusal and rebuild from the peer -- which tears down the
// relationship and overwrites this copy. Internal, and nothing torn down.
func TestResyncWithAMalformedReverseReplyNeverRebuilds(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	orig := s.driver.NewClient
	s.driver.NewClient = func(baseURL, token string) qumulo.ClientInterface {
		c := orig(baseURL, token)
		if strings.Contains(baseURL, "//"+arrayB+":") {
			return &malformedReverseArray{fakeArray: c.(*fakeArray)}
		}
		return c
	}
	s.a.calls, s.b.calls = nil, nil

	for _, force := range []bool{false, true} {
		_, err := s.resync(t, s.handleA, classToB(), force)
		requireCode(t, err, codes.Internal)
		assert.NotContains(t, err.Error(), "unreachable")
	}
	assert.Equal(t, rel.id, s.fabric.onlyRel().id, "the relationship was not torn down")
	assert.Equal(t, []string{"Reverse", "Reverse"}, s.b.calls, "no rebuild from the peer")
}

// What each kind of failure from the real client is reported as. Only
// a round trip that did not complete -- the breaker's fail-fast included,
// which still carries the failure that opened it -- is the array
// unreachable; a reply the client cannot use is Internal and says the array
// answered.
func TestUnreachableOrTellsAMalformedReplyFromNoAnswer(t *testing.T) {
	serve := func(body string) *qumulo.Client {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, body)
		}))
		t.Cleanup(srv.Close)
		return qumulo.New(srv.URL, "t")
	}
	ctx := context.Background()

	_, notJSON := serve(`<html>proxy error</html>`).ListSourceStatus(ctx)
	_, wrongShape := serve(`{"not":"a list"}`).ListSourceStatus(ctx)
	nullRestriction := serve(`{"id":"15946","restrictions":[null]}`).SetExportReadOnly(ctx, "15946", true)
	noRestrictions := serve(`{"id":"15946","restrictions":[]}`).SetExportReadOnly(ctx, "15946", true)

	down := qumulo.New("https://127.0.0.1:1", "t")
	down.Breaker = qumulo.NewBreaker()
	down.HTTPClient = &http.Client{Transport: breakerWireTransport(func(*http.Request) (*http.Response, error) {
		return nil, &net.DNSError{Name: "qumulo-a.example.com", Err: "no such host", IsNotFound: true}
	})}
	var noAnswer, failedFast error
	for i := 0; i < 3; i++ {
		_, err := down.ListSourceStatus(ctx)
		noAnswer, failedFast = failedFast, err
	}

	for name, tc := range map[string]struct {
		err  error
		want codes.Code
	}{
		"a body that is not JSON":         {notJSON, codes.Internal},
		"a body that is not the listing":  {wrongShape, codes.Internal},
		"a null export restriction":       {nullRestriction, codes.Internal},
		"an export with no restrictions":  {noRestrictions, codes.Internal},
		"a transport failure":             {noAnswer, codes.Unavailable},
		"the breaker's fail-fast":         {failedFast, codes.Unavailable},
		"an HTTP error is still answered": {fakeAPIError(http.StatusInternalServerError, "", "boom"), codes.Internal},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, tc.err)
			err := unreachableOr("a.example.com", tc.err, "doing it")
			requireCode(t, err, tc.want)
			if tc.want == codes.Internal {
				assert.NotContains(t, err.Error(), "unreachable")
			}
		})
	}

	// the fail-fast still carries the NXDOMAIN that opened it.
	var dnsErr *net.DNSError
	assert.True(t, errors.As(failedFast, &dnsErr) && dnsErr.IsNotFound, "got %v", failedFast)
	assert.Contains(t, failedFast.Error(), "not calling it again yet")
}
