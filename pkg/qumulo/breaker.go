// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// errEndpointDown is what a call to an array the breaker has open answers. It
// is wrapped in a *url.Error, like the transport failure it stands for, so
// every caller already classifies it as "the array did not answer". It also
// wraps the failure that opened the breaker, so a caller that tells a
// misconfigured endpoint from an outage still can.
var errEndpointDown = errors.New("the array did not answer its last calls; not calling it again yet")

const (
	// breakerThreshold consecutive calls an array does not answer open its
	// breaker.
	breakerThreshold = 2
	// breakerOpenFor is how long calls fail fast before one is let through.
	breakerOpenFor = 15 * time.Second
)

// Breaker is a per-endpoint circuit breaker, shared by every client built
// with it -- one per process in the driver (cmd/qumulo-csi-driver).
//
// The driver itself keeps arrays apart, but csi-provisioner does not: every
// class shares its workers, and each attempt on a dead array held one for the
// whole request timeout, so creates on healthy arrays queued behind the
// retries. Once an array has failed to answer twice in a row, calls to
// it fail at once for breakerOpenFor, then one is let through; any answer
// at all -- an HTTP error included -- closes it again.
//
// Only fresh evidence moves it. The window is armed by the failure
// that opens the breaker and by a failed probe, never by a call that was
// already in flight when it opened; and a call that went out before the
// array last answered is not counted at all. Calls in flight when an array
// goes silent fail up to a request timeout later, and counting them kept the
// breaker open well past the array's return -- long enough to answer every
// ask of a planned promote and turn it into a forced one.
type Breaker struct {
	mu    sync.Mutex
	hosts map[string]*hostState
	now   func() time.Time
}

type hostState struct {
	failures  int
	openUntil time.Time
	// probing is set while the one half-open call is out, until
	// probeUntil: a probe that never reports back does not hold the
	// breaker shut for ever.
	probing    bool
	probeUntil time.Time
	// answered is when the array last answered a call. A failure of a call
	// that went out before then is stale.
	answered time.Time
	// lastErr is the last transport failure counted. While the breaker is
	// open, its fail-fast wraps it: answering a bare "not calling it again
	// yet" turned a name that does not resolve -- InvalidArgument, final --
	// into Unavailable on every other call, and the provisioner retried a
	// class no retry can fix.
	lastErr error
}

// breakerCall is one call the breaker let out, for record.
type breakerCall struct {
	host    string
	started time.Time
	// probe is the half-open call: its failure re-arms the window.
	probe bool
}

// NewBreaker returns a Breaker with no endpoint open.
func NewBreaker() *Breaker {
	return &Breaker{hosts: map[string]*hostState{}, now: time.Now}
}

// noFailFastKey marks a context whose calls are never failed fast.
type noFailFastKey struct{}

// WithoutFailFast returns a context whose calls always go out, even to an
// array whose breaker is open. Their outcomes are still recorded -- an answer
// closes the breaker for everyone -- but a failure does not extend its
// window.
//
// It is for the few calls whose answer decides something the breaker's own
// verdict must not: a promote's ask of its peer, where "did not answer"
// licenses a promote with no freeze and no final sync. Those are a
// handful per promote and bounded by its deadline, so provisioning still
// fails fast against a dead array.
func WithoutFailFast(ctx context.Context) context.Context {
	return context.WithValue(ctx, noFailFastKey{}, true)
}

func failFastAllowed(ctx context.Context) bool {
	skip, _ := ctx.Value(noFailFastKey{}).(bool)
	return !skip
}

// allow says whether a call to host may go out, and returns the call to pass
// to record. A nil Breaker allows all; failFast false (WithoutFailFast)
// allows a call whatever the breaker's state.
func (b *Breaker) allow(host string, failFast bool) (breakerCall, error) {
	if b == nil {
		return breakerCall{}, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	call := breakerCall{host: host, started: now}
	s := b.hosts[host]
	if !failFast || s == nil || s.failures < breakerThreshold {
		return call, nil
	}
	if now.Before(s.openUntil) || (s.probing && now.Before(s.probeUntil)) {
		return call, fmt.Errorf("%s: %w (%d in a row; retrying after %s); last failure: %w", host, errEndpointDown,
			s.failures, s.retryAfter().Format(time.RFC3339), s.lastErr)
	}
	// Half-open: this call goes out, alone; every other waits for its
	// outcome. The window is not re-armed here: a probe that fails re-arms
	// it, one that is answered closes the breaker, and one its own caller
	// cancelled lets the next call probe.
	s.probing = true
	s.probeUntil = now.Add(breakerOpenFor)
	call.probe = true
	return call, nil
}

// retryAfter is when the next call may go out.
func (s *hostState) retryAfter() time.Time {
	if s.probing && s.probeUntil.After(s.openUntil) {
		return s.probeUntil
	}
	return s.openUntil
}

// record notes a call's outcome. err is the transport failure, nil when the
// array answered at all; counted is false for a call its own caller
// cancelled, which says nothing about the array.
func (b *Breaker) record(call breakerCall, err error, counted bool) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	s := b.hosts[call.host]
	if s == nil {
		s = &hostState{}
		b.hosts[call.host] = s
	}
	if err == nil {
		*s = hostState{answered: now}
		return
	}
	if call.probe {
		s.probing = false
	}
	if !counted || call.started.Before(s.answered) {
		// Cancelled by its caller, or out before the array last answered:
		// says nothing about the array now.
		return
	}
	s.failures++
	s.lastErr = err
	if call.probe || s.failures == breakerThreshold {
		s.openUntil = now.Add(breakerOpenFor)
	}
}
