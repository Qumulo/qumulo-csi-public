// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// An array that stops answering is failed fast after two misses, as a
// transport error; after breakerOpenFor one call goes out again, and any
// answer closes the breaker.
func TestTheBreakerFailsFastForAnArrayThatStoppedAnswering(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens: every dial is refused

	b := NewBreaker()
	clock := time.Now()
	b.now = func() time.Time { return clock }
	c := New("http://"+addr, "t")
	c.Breaker = b

	for i := 0; i < breakerThreshold; i++ {
		if _, err := c.LookupFileByPath(context.Background(), "/v"); err == nil {
			t.Fatal("a refused dial answered")
		}
	}
	_, err = c.LookupFileByPath(context.Background(), "/v")
	var urlErr *url.Error
	if !errors.Is(err, errEndpointDown) || !errors.As(err, &urlErr) {
		t.Fatalf("the third call answered %v; want the breaker's fail-fast, as a *url.Error", err)
	}

	// The array comes back on the same address; after the open period one
	// call goes out, and its answer closes the breaker.
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("could not re-listen on %s: %v", addr, err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"1","path":"/v","type":"FS_FILE_TYPE_DIRECTORY"}`))
	}))
	srv.Listener = ln2
	srv.Start()
	defer srv.Close()

	clock = clock.Add(breakerOpenFor + time.Second)
	if _, err := c.LookupFileByPath(context.Background(), "/v"); err != nil {
		t.Fatalf("the half-open call answered %v", err)
	}
	if _, err := c.LookupFileByPath(context.Background(), "/v"); err != nil {
		t.Fatalf("after an answer the breaker is closed, but a call answered %v", err)
	}
}

// A call its own caller cancelled says nothing about the array.
func TestTheBreakerIgnoresACancelledCall(t *testing.T) {
	b := NewBreaker()
	for i := 0; i < 5; i++ {
		b.record(breakerCall{host: "a:1", started: b.now()}, errors.New("cancelled"), false)
	}
	if _, err := b.allow("a:1", true); err != nil {
		t.Fatalf("cancelled calls opened the breaker: %v", err)
	}
}

// roundTripFunc is an http.RoundTripper made of a function.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// while the breaker is open, its fail-fast keeps the failure that
// opened it. A bare "not calling it again yet" lost a name that does not
// resolve, so a caller that answers InvalidArgument for one -- final, naming
// what to fix -- answered Unavailable on every other call instead.
func TestTheBreakerKeepsTheFailureThatOpenedIt(t *testing.T) {
	dials := 0
	c := New("https://qumulo-a.example.com:8000", "t")
	c.Breaker = NewBreaker()
	c.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		dials++
		return nil, &net.DNSError{Name: "qumulo-a.example.com", Err: "no such host", IsNotFound: true}
	})}

	for i := 0; i < breakerThreshold+2; i++ {
		_, err := c.LookupFileByPath(context.Background(), "/v")
		var dnsErr *net.DNSError
		if !errors.As(err, &dnsErr) || !dnsErr.IsNotFound {
			t.Fatalf("call %d answered %v; want the NXDOMAIN kept", i+1, err)
		}
		if i >= breakerThreshold && !errors.Is(err, errEndpointDown) {
			t.Fatalf("call %d answered %v; want the breaker's fail-fast", i+1, err)
		}
	}
	if dials != breakerThreshold {
		t.Fatalf("%d calls went out; want %d, then fail-fast", dials, breakerThreshold)
	}
}

// breakerRig is one array behind a Breaker on a clock the test moves. A call
// for a path containing "late" blocks in the transport until release is
// closed and then fails, as a call already in flight when the array went
// silent does; a call for a path containing "probe" blocks until its context
// ends. Every other call fails in transport until answering is set.
type breakerRig struct {
	t       *testing.T
	b       *Breaker
	c       *Client
	mu      sync.Mutex
	clock   time.Time
	answer  atomic.Bool
	sent    atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func newBreakerRig(t *testing.T) *breakerRig {
	r := &breakerRig{t: t, b: NewBreaker(), clock: time.Now(),
		entered: make(chan struct{}, 16), release: make(chan struct{})}
	r.b.now = func() time.Time {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.clock
	}
	r.c = New("https://qumulo-a.example.com:8000", "t")
	r.c.Breaker = r.b
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("i/o timeout")}
	r.c.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		r.sent.Add(1)
		switch p := req.URL.String(); {
		case strings.Contains(p, "late"):
			r.entered <- struct{}{}
			<-r.release
			return nil, dialErr
		case strings.Contains(p, "probe"):
			r.entered <- struct{}{}
			<-req.Context().Done()
			return nil, req.Context().Err()
		case r.answer.Load():
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{},
				Body: io.NopCloser(strings.NewReader(`{"id":"1","path":"/v","type":"FS_FILE_TYPE_DIRECTORY"}`))}, nil
		default:
			return nil, dialErr
		}
	})}
	return r
}

func (r *breakerRig) advance(d time.Duration) {
	r.mu.Lock()
	r.clock = r.clock.Add(d)
	r.mu.Unlock()
}

func (r *breakerRig) call(ctx context.Context, path string) error {
	_, err := r.c.LookupFileByPath(ctx, path)
	return err
}

// startLate starts n calls that are in flight until release is closed, and
// returns once each has reached the transport.
func (r *breakerRig) startLate(n int) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.call(context.Background(), "/late")
		}()
		<-r.entered
	}
	return &wg
}

// open fails breakerThreshold calls, which opens the breaker.
func (r *breakerRig) open() {
	r.t.Helper()
	for i := 0; i < breakerThreshold; i++ {
		if err := r.call(context.Background(), "/v"); err == nil || errors.Is(err, errEndpointDown) {
			r.t.Fatalf("opening call %d answered %v; want a transport failure", i+1, err)
		}
	}
	if err := r.call(context.Background(), "/v"); !errors.Is(err, errEndpointDown) {
		r.t.Fatalf("after %d failures a call answered %v; want the fail-fast", breakerThreshold, err)
	}
}

// Calls in flight when the array went silent fail late -- a dial or a
// request timeout after the fact. Once the array has answered a call that
// started after them, those failures are old news; counting them reopened the
// breaker on an array that had just answered, and a planned promote's peer
// asks were answered by the breaker for another 15 s.
func TestTheBreakerIgnoresFailuresOfCallsThatStartedBeforeAnAnswer(t *testing.T) {
	r := newBreakerRig(t)
	late := r.startLate(breakerThreshold)
	r.open()

	r.advance(breakerOpenFor + time.Second)
	r.answer.Store(true)
	if err := r.call(context.Background(), "/v"); err != nil {
		t.Fatalf("the half-open call answered %v", err)
	}
	close(r.release)
	late.Wait()

	if err := r.call(context.Background(), "/v"); err != nil {
		t.Fatalf("calls that started before the array answered reopened the breaker: %v", err)
	}
}

// The window is armed by the failure that opens the breaker. Calls
// already in flight then fail up to a request timeout later; each of them
// pushed the window out again, so a 20 s blip kept it open from its last late
// failure, not its first.
func TestTheBreakerWindowIsNotExtendedByCallsInFlightWhenItOpened(t *testing.T) {
	r := newBreakerRig(t)
	late := r.startLate(2)
	r.open()

	r.advance(breakerOpenFor - 5*time.Second)
	close(r.release)
	late.Wait()
	if err := r.call(context.Background(), "/v"); !errors.Is(err, errEndpointDown) {
		t.Fatalf("inside the window a call answered %v; want the fail-fast", err)
	}

	r.advance(5*time.Second + time.Second)
	r.answer.Store(true)
	if err := r.call(context.Background(), "/v"); err != nil {
		t.Fatalf("after the window opened by the first failures, a call answered %v; "+
			"want it sent, as the half-open probe", err)
	}
}

// A half-open probe its own caller cancelled says nothing about the
// array. It used to re-arm the window when it went out, so the next caller
// was failed fast for another 15 s with no probe outstanding.
func TestACancelledProbeLetsTheNextCallProbe(t *testing.T) {
	r := newBreakerRig(t)
	r.open()
	r.advance(breakerOpenFor + time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.call(ctx, "/probe") }()
	<-r.entered
	// While the probe is out, everyone else is still failed fast: one call
	// at a time tests a silent array.
	if err := r.call(context.Background(), "/v"); !errors.Is(err, errEndpointDown) {
		t.Fatalf("with a probe out a call answered %v; want the fail-fast", err)
	}
	cancel()
	<-done

	r.answer.Store(true)
	if err := r.call(context.Background(), "/v"); err != nil {
		t.Fatalf("after a cancelled probe a call answered %v; want it sent as the next probe", err)
	}
}

// A probe that fails reopens the breaker for the full window from its failure.
func TestAFailedProbeReopensTheBreaker(t *testing.T) {
	r := newBreakerRig(t)
	r.open()
	r.advance(breakerOpenFor + time.Second)
	sent := r.sent.Load()
	if err := r.call(context.Background(), "/v"); err == nil || errors.Is(err, errEndpointDown) {
		t.Fatalf("the probe answered %v; want it sent and failed", err)
	}
	if r.sent.Load() != sent+1 {
		t.Fatal("the probe was not sent")
	}
	r.advance(breakerOpenFor - time.Second)
	if err := r.call(context.Background(), "/v"); !errors.Is(err, errEndpointDown) {
		t.Fatalf("after a failed probe a call answered %v; want the fail-fast", err)
	}
}

// Keeping: a call made WithoutFailFast reaches an array whose
// breaker is open -- a promote's ask of its peer must reach the array, or
// the breaker's own verdict licenses a promote with no final sync -- and its
// answer closes the breaker for everyone. Its failure is counted but does
// not extend the window, and ordinary calls are still failed fast, so
// provisioning against a dead array does not queue behind it.
func TestWithoutFailFastReachesAnArrayWhoseBreakerIsOpen(t *testing.T) {
	r := newBreakerRig(t)
	r.open()
	bypass := WithoutFailFast(context.Background())

	sent := r.sent.Load()
	err := r.call(bypass, "/v")
	var urlErr *url.Error
	if err == nil || errors.Is(err, errEndpointDown) || !errors.As(err, &urlErr) {
		t.Fatalf("a WithoutFailFast call answered %v; want it sent, and its transport failure", err)
	}
	if r.sent.Load() != sent+1 {
		t.Fatal("a WithoutFailFast call was not sent")
	}
	if err := r.call(context.Background(), "/v"); !errors.Is(err, errEndpointDown) {
		t.Fatalf("an ordinary call answered %v; want the fail-fast still", err)
	}

	r.answer.Store(true)
	if err := r.call(bypass, "/v"); err != nil {
		t.Fatalf("a WithoutFailFast call to an answering array answered %v", err)
	}
	if err := r.call(context.Background(), "/v"); err != nil {
		t.Fatalf("after the array answered, an ordinary call answered %v; want the breaker closed", err)
	}
}

// is kept: an array that dies right after answering is still failed
// fast after breakerThreshold calls that started after its last answer.
func TestTheBreakerOpensOnAnArrayThatDiesAfterAnswering(t *testing.T) {
	r := newBreakerRig(t)
	r.answer.Store(true)
	if err := r.call(context.Background(), "/v"); err != nil {
		t.Fatal(err)
	}
	r.answer.Store(false)
	r.advance(time.Millisecond)
	r.open()
}
