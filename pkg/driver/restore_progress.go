// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"strings"
	"sync"
	"time"
)

// restoreProgressTTL is how long an unfinished restore's progress is kept
// after its last attempt. csi-provisioner retries a restore that answered
// Aborted within its backoff, five minutes at most, so a record this old
// belongs to a claim that was abandoned. It was 24 h, which kept a memo --
// up to maxRestoreFiles entries -- for a day after its claim was deleted.
const restoreProgressTTL = time.Hour

// maxRestoreMemos bounds how many restores' progress is kept at once: four
// times the restores that may run at once, so a burst of claims abandoned
// mid-restore cannot hold more. The least recently touched goes first; losing
// one costs that restore one attempt that walks its finished part again.
const maxRestoreMemos = 4 * maxConcurrentRestores

// restoreProgress remembers, between the attempts of one restore, what the
// walk no longer needs to do.
//
// A restore runs as a sequence of CreateVolume attempts, each bounded by a
// budget, each resuming from what the previous one staged. The array holds
// the staged tree but not the walk's own bookkeeping, and rebuilding that
// from the array cost a listing per directory, which is what stalled a
// 20,000-directory restore. The fix for that recognised a finished
// subtree by its attributes and skipped it, which lost the hard-link map for
// everything inside it, so a link whose first name was in a skipped subtree
// was copied as an independent file again, and a subtree whose
// attributes the array refused to set was never recognised, so the restore
// re-walked it on every attempt for ever.
//
// This is a hint, not state the driver depends on: a controller restart, a
// second controller replica, or an expired record only costs one attempt
// that walks the finished part again and records it as it goes. It holds no
// credential and no cluster address beyond the key, so Rule 0 stands. It is
// bounded by maxRestoreFiles entries per restore and maxRestoreMemos
// restores, and dropped on commit or after restoreProgressTTL.
type restoreProgress struct {
	mu    sync.Mutex
	memos map[string]*restoreMemo
	now   func() time.Time
}

// restoreMemo is one restore's progress.
type restoreMemo struct {
	// stagingID is the file ID of the staging directory the memo describes.
	// A staging directory that was reclaimed and made again is a new tree,
	// and a memo for the old one would skip directories it does not have.
	stagingID string
	// completed holds each staged directory whose whole subtree, and its own
	// attributes, an attempt finished -- whether or not the array accepted
	// those attributes.
	completed map[string]bool
	// staged maps each regular file's source file ID to the path its first
	// name was staged at, across attempts.
	staged  map[string]string
	touched time.Time
}

func newRestoreProgress() *restoreProgress {
	return &restoreProgress{memos: make(map[string]*restoreMemo), now: time.Now}
}

// take returns the memo for key, or a fresh one when there is none, when it
// describes another staging directory, or when it has expired. It also drops
// every other expired memo, and the least recently touched ones past
// maxRestoreMemos, which is what bounds the map.
func (p *restoreProgress) take(key, stagingID string) *restoreMemo {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	for k, m := range p.memos {
		if now.Sub(m.touched) > restoreProgressTTL {
			delete(p.memos, k)
		}
	}
	m, ok := p.memos[key]
	if !ok || m.stagingID != stagingID {
		m = &restoreMemo{stagingID: stagingID, completed: make(map[string]bool), staged: make(map[string]string)}
		p.memos[key] = m
	}
	m.touched = now
	for len(p.memos) > maxRestoreMemos {
		oldest := ""
		for k, other := range p.memos {
			if k != key && (oldest == "" || other.touched.Before(p.memos[oldest].touched)) {
				oldest = k
			}
		}
		delete(p.memos, oldest)
	}
	return m
}

// forget drops key's memo, once the restore has committed.
func (p *restoreProgress) forget(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.memos, key)
}

// holds reports whether an unexpired memo exists for a restore into staging
// on endpoint, from any snapshot: the CO may be about to retry it, so its
// staging directory is not abandoned.
func (p *restoreProgress) holds(endpoint, staging string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	prefix := endpoint + "\x00" + staging + "\x00"
	for k, m := range p.memos {
		if strings.HasPrefix(k, prefix) && now.Sub(m.touched) <= restoreProgressTTL {
			return true
		}
	}
	return false
}

// restoreProgressKey names one restore: the array, the staging path on it,
// and the snapshot being restored. Two arrays can share a staging path, and a
// claim re-created from another snapshot is another restore.
func restoreProgressKey(endpoint, staging, snapshotID string) string {
	return endpoint + "\x00" + staging + "\x00" + snapshotID
}
