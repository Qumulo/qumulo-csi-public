// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The hints (ARCHITECTURE.md rule 0) are maps keyed by array and path, and nothing
// used to remove an entry whose volume was gone: a controller that
// provisioned and deleted volumes for months kept one entry per volume it had
// ever seen, in every map. Each map is now swept of entries past their
// use, by whichever store comes next once hintSweepEvery has passed -- no
// goroutine to start or stop, and a controller that stores nothing has
// nothing to sweep.

// hintSweepEvery is the least time between two sweeps of one set of maps.
const hintSweepEvery = 10 * time.Minute

// idleHintExpiry is how long a hint that is read back by ID, and so has no
// expiry of its own, is kept without being stored or confirmed. Dropping one
// costs a single listing.
const idleHintExpiry = time.Hour

// idHint is the ID of an object found or created at a key, and when it was
// last stored or confirmed against the array. The time only decides when the
// sweep drops it: the ID is read back and checked on every use.
type idHint struct {
	id string
	at time.Time
}

func newIDHint(id string) idHint { return idHint{id: id, at: time.Now()} }

// idleSince is the sweep's test for an idHint.
func idleSince(cutoff time.Time) func(any) bool {
	return func(v any) bool { return v.(idHint).at.Before(cutoff) }
}

// hintSweeper decides when a sweep is due, so concurrent stores run one.
type hintSweeper struct{ last atomic.Int64 }

// due reports whether the caller is the one to sweep now.
func (s *hintSweeper) due(now time.Time) bool {
	last := s.last.Load()
	if now.UnixNano()-last < int64(hintSweepEvery) {
		return false
	}
	return s.last.CompareAndSwap(last, now.UnixNano())
}

// sweepMap deletes every entry of m that stale says is past its use. An
// entry replaced while the sweep ran is left alone: CompareAndDelete only
// removes the value that was judged.
func sweepMap(m *sync.Map, stale func(any) bool) {
	m.Range(func(k, v any) bool {
		if stale(v) {
			m.CompareAndDelete(k, v)
		}
		return true
	})
}

// forgetUnder deletes every entry of m whose key is prefix + path, or a path
// below it. Keys are "<array>\x00<path>".
func forgetUnder(m *sync.Map, prefix, path string) {
	exact := prefix + path
	below := strings.TrimSuffix(exact, "/") + "/"
	m.Range(func(k, _ any) bool {
		if s, ok := k.(string); ok && (s == exact || strings.HasPrefix(s, below)) {
			m.Delete(k)
		}
		return true
	})
}

// packageHints sweeps the hints kept per process rather than per server:
// export, covering-export, coverage, policy and snapshot IDs.
var packageHints hintSweeper

// notePackageHintStored is called after a store into one of the per-process
// hint maps, and sweeps them all when a sweep is due.
func notePackageHintStored() {
	now := time.Now()
	if !packageHints.due(now) {
		return
	}
	sweepPackageHints(now)
}

// sweepPackageHints drops every per-process hint past its use at now.
func sweepPackageHints(now time.Time) {
	idle := idleSince(now.Add(-idleHintExpiry))
	sweepMap(&exportHints, idle)
	sweepMap(&coveringExportHints, idle)
	sweepMap(&policyHints, idle)
	sweepMap(&snapshotHints, idle)
	sweepMap(&coverageHints, idleSince(now.Add(-coverageHintTTL)))
	sweepMap(&noExportHints, func(v any) bool { return now.Sub(v.(time.Time)) >= noExportHintTTL })
	sweepMap(&tenantListings, func(v any) bool { return now.Sub(v.(*tenantListing).at) >= tenantListingTTL })
}
