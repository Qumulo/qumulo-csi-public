// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// keyedLocks serialises work on one key inside one controller process.
//
// Two CSI RPCs need it, and both races are the same shape: a check
// against the cluster, then an action decided from what the check found.
//
//   - CreateSnapshot looks for a snapshot already carrying the CO's name and
//     takes one if there is none. Two calls for one name -- the snapshotter
//     retrying while its first call is still in flight -- both found none and
//     both created one, leaving a second snapshot on the array that nothing
//     in Kubernetes ever deletes, because the VolumeSnapshotContent records
//     only one ID.
//   - ControllerExpandVolume reads the quota, decides the request is a grow,
//     and writes the new limit. Two calls with different sizes interleaved
//     between the read and the write, and the smaller one lands last: the
//     CO believes the volume is at the larger size and the array enforces
//     the smaller.
//
// In memory and per process, exactly like restoreLocks: it closes a window of
// milliseconds inside one controller, and two controllers do not share it.
// That is the whole claim -- the array's own idempotency (adoption by name,
// a quota compared before it is written) is still what makes a retry after a
// restart safe.
//
// Waiting rather than refusing. A second call for the same key is a retry of
// the first in all but name, so holding it for the few hundred milliseconds
// the first takes gets it the answer it wanted; Aborted would make the CO
// come back for the same work a moment later. A caller whose context ends
// while it waits gets Aborted, which is the spec's code for "another
// operation on this volume is in progress" and tells the CO to retry.
type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]chan struct{}
}

func newKeyedLocks() *keyedLocks {
	return &keyedLocks{locks: make(map[string]chan struct{})}
}

// acquire blocks until the key is free, and returns the release for it.
//
// A key is held while it has an entry in the map, and that entry is a channel
// closed when the key is let go. Waiters wait on the channel rather than on a
// sync.Mutex per key, because a mutex cannot be waited on with a deadline: a
// controller shutting down must not leave RPCs parked in an uncancellable
// wait.
func (l *keyedLocks) acquire(ctx context.Context, key string) (release func(), err error) {
	for {
		l.mu.Lock()
		if l.locks == nil {
			l.locks = make(map[string]chan struct{})
		}
		if released, busy := l.locks[key]; busy {
			l.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, status.Errorf(codes.Aborted,
					"another operation on %q is already in progress and this request's context "+
						"ended while it waited: %v", key, ctx.Err())
			case <-released:
				// Let go of. Round again rather than assuming it is ours:
				// another waiter may have taken it first.
			}
			continue
		}
		released := make(chan struct{})
		l.locks[key] = released
		l.mu.Unlock()

		var once sync.Once
		return func() {
			once.Do(func() {
				l.mu.Lock()
				delete(l.locks, key)
				l.mu.Unlock()
				close(released)
			})
		}, nil
	}
}

// held reports whether the key is locked now. Tests only.
func (l *keyedLocks) held(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, busy := l.locks[key]
	return busy
}
