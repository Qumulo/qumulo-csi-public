// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// revertSnaps returns the array's revert snapshots.
func revertSnaps(a *fakeArray) []qumulo.SnapshotInfo {
	var out []qumulo.SnapshotInfo
	for _, s := range a.snapshots {
		if strings.Contains(s.Name, "_revert_to_") {
			out = append(out, s)
		}
	}
	return out
}

func countCalls(a *fakeArray, verb string) int {
	n := 0
	for _, c := range a.calls {
		if c == verb {
			n++
		}
	}
	return n
}

// requireExpiresInAboutAWeek checks every snapshot carries an expiration
// retention after the test started.
func requireExpiresInAboutAWeek(t *testing.T, snaps []qumulo.SnapshotInfo, start time.Time) {
	t.Helper()
	for _, s := range snaps {
		require.False(t, s.Expiration.IsZero(), "revert snapshot %s (%s) was left never expiring", s.ID, s.Name)
		assert.WithinRange(t, s.Expiration, start.Add(DefaultRevertSnapshotRetention),
			time.Now().Add(DefaultRevertSnapshotRetention), "snapshot %s", s.ID)
	}
}

// A promote makes the target array revert the target root, and the
// array keeps what was there in a snapshot with no expiration. The promote
// now gives it one: 7 days (decision).
func TestB1PromoteGivesTheRevertSnapshotAnExpiration(t *testing.T) {
	s := newReplScenario(t)
	s.driver.RevertSnapshotRetention = DefaultRevertSnapshotRetention
	s.b.revertSnapshots = true
	s.protect(t, classToB())
	start := time.Now()

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	snaps := revertSnaps(s.b)
	require.Len(t, snaps, 1, "the fake array took its revert snapshot")
	assert.Equal(t, s.b.dirs[replVolumePath], snaps[0].SourceFileID)
	requireExpiresInAboutAWeek(t, snaps, start)
}

// A whole failover and failback: make-writable on B, reconnect on A (the
// reprotect), make-writable on A, reconnect on B. Every revert snapshot on
// both arrays ends with an expiration.
func TestB1FailoverAndFailbackLeaveNoNeverExpiringRevertSnapshot(t *testing.T) {
	s := newReplScenario(t)
	s.driver.RevertSnapshotRetention = DefaultRevertSnapshotRetention
	s.a.revertSnapshots, s.b.revertSnapshots = true, true
	rel := s.protect(t, classToB())
	start := time.Now()

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleA, classToB()))
	_, err := s.resync(t, s.handleA, classToB(), false)
	require.NoError(t, err)
	require.NoError(t, s.promote(t, s.handleA, classToB(), false))
	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	_, err = s.resync(t, s.handleB(t), classToA(), false)
	require.NoError(t, err)
	require.Equal(t, s.a, rel.source, "A is the source again")

	require.NotEmpty(t, revertSnaps(s.a), "A was reverted on the reprotect and on the failback")
	require.NotEmpty(t, revertSnaps(s.b), "B was reverted on the failover and on the failback")
	requireExpiresInAboutAWeek(t, revertSnaps(s.a), start)
	requireExpiresInAboutAWeek(t, revertSnaps(s.b), start)
}

// Zero retention leaves the array's snapshots as it made them, and costs no
// extra call.
func TestB1ZeroRetentionLeavesRevertSnapshotsAlone(t *testing.T) {
	s := newReplScenario(t)
	s.b.revertSnapshots = true
	s.protect(t, classToB())

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	require.Len(t, revertSnaps(s.b), 1)
	assert.True(t, revertSnaps(s.b)[0].Expiration.IsZero())
	assert.Zero(t, countCalls(s.b, "SetSnapshotExpiration"))
}

// Only the array's own never-expiring revert snapshots of this root are
// touched: not another directory's, not one an administrator already gave an
// expiration, not a policy's that happens to be named alike, not a CSI one.
func TestB1OnlyThisRootsNeverExpiringRevertSnapshotsAreTouched(t *testing.T) {
	s := newReplScenario(t)
	s.driver.RevertSnapshotRetention = DefaultRevertSnapshotRetention
	s.protect(t, classToB())
	root := s.b.dirs[replVolumePath]
	other := s.b.mkdir("/csi/volumes/pvc-other")
	set := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	s.b.snapshots = append(s.b.snapshots,
		qumulo.SnapshotInfo{ID: "901", Name: "901_revert_to_900_for_pvc-repl", SourceFileID: root},
		qumulo.SnapshotInfo{ID: "902", Name: "902_revert_to_1_for_pvc-other", SourceFileID: other},
		qumulo.SnapshotInfo{ID: "903", Name: "903_revert_to_2_for_pvc-repl", SourceFileID: root, Expiration: set},
		qumulo.SnapshotInfo{ID: "904", Name: "904_revert_to_3_for_pvc-repl", SourceFileID: root, PolicyID: "7"},
		qumulo.SnapshotInfo{ID: "905", Name: "905_snapshot-abc", SourceFileID: root},
		qumulo.SnapshotInfo{ID: "906", Name: "906_revert_to_4_for_pvc-repl", SourceFileID: root, InDelete: true},
	)

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	got := map[string]time.Time{}
	for _, sn := range s.b.snapshots {
		got[sn.ID] = sn.Expiration
	}
	assert.False(t, got["901"].IsZero(), "this root's never-expiring revert snapshot is stamped")
	assert.True(t, got["902"].IsZero(), "another directory's is not")
	assert.Equal(t, set, got["903"], "an expiration already set is kept")
	assert.True(t, got["904"].IsZero(), "a policy's snapshot is not the array's revert snapshot")
	assert.True(t, got["905"].IsZero(), "a CSI snapshot is not")
	assert.True(t, got["906"].IsZero(), "one being deleted is not")
	assert.Equal(t, 1, countCalls(s.b, "SetSnapshotExpiration"))
}

// Best effort: the revert has happened, so an array that refuses the
// expiration does not fail the promote, and the next resync tries again.
func TestB1AnExpirationFailureDoesNotFailThePromoteAndIsRetried(t *testing.T) {
	s := newReplScenario(t)
	s.driver.RevertSnapshotRetention = DefaultRevertSnapshotRetention
	s.b.revertSnapshots = true
	s.protect(t, classToB())
	s.b.expirationFault = fakeAPIError(http.StatusForbidden, "api_access_denied_error", "no privilege")

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.True(t, revertSnaps(s.b)[0].Expiration.IsZero())

	// The array is given the privilege; the promote is retried -- the
	// target is already writable, so it takes the "already promoted" row.
	s.b.expirationFault = nil
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	assert.False(t, revertSnaps(s.b)[0].Expiration.IsZero(), "stamped by the retry")
}

// A client that cannot set an expiration (a test double, say) is left
// alone rather than failing.
func TestB1AClientWithoutExpirationSupportIsSkipped(t *testing.T) {
	d := &Driver{RevertSnapshotRetention: time.Hour}
	r := &ReplicationServer{driver: d}
	listed := false
	client := &MockClient{ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
		listed = true
		return nil, nil
	}}

	r.expireRevertSnapshots(context.Background(), "test", client, "x", "1")

	assert.False(t, listed, "nothing to do without SetSnapshotExpiration, so nothing is listed")
}

// A failback's reconnect makes the array revert, and it takes the
// revert snapshot after the Reconnect call has returned. The reconnected
// side already held a recovery point, so the first resync poll answered
// ready -- before the snapshot existed -- and the csi-addons controller,
// which stops polling at ready, never let the driver see it: kept for ever.
// Ready is now held back until that snapshot has its expiration.
func TestResyncWaitsForALateRevertSnapshot(t *testing.T) {
	s := newReplScenario(t)
	s.driver.RevertSnapshotRetention = DefaultRevertSnapshotRetention
	s.a.revertSnapshots, s.b.revertSnapshots = true, true
	s.protect(t, classToB())
	start := time.Now()
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleA, classToB()))

	// The snapshot appears at the 4th listing after the reconnect: after the
	// reverse's own listings and the first poll's.
	s.a.revertSnapshotLate = 4
	ready, err := s.resync(t, s.handleA, classToB(), false)
	require.NoError(t, err)
	require.False(t, ready, "the reverse and reconnect answer not ready")
	require.Empty(t, revertSnaps(s.a), "the array has not taken it yet")

	polls := 0
	for !ready && polls < 10 {
		ready, err = s.resync(t, s.handleA, classToB(), false)
		require.NoError(t, err)
		polls++
	}
	require.True(t, ready, "ready once the snapshot is stamped")
	assert.Greater(t, polls, 1, "the first poll, before the snapshot existed, did not answer ready")
	require.Len(t, revertSnaps(s.a), 1, "the late revert snapshot appeared")
	requireExpiresInAboutAWeek(t, revertSnaps(s.a), start)
}

// An array that takes no revert snapshot at all (or one that never accepts
// the expiration) holds ready back for the wait only, then reports ready.
func TestResyncStopsWaitingForARevertSnapshotThatNeverComes(t *testing.T) {
	s := newReplScenario(t)
	s.driver.RevertSnapshotRetention = DefaultRevertSnapshotRetention
	s.server.revertSnapshotWait = 50 * time.Millisecond
	s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleA, classToB()))

	ready, err := s.resync(t, s.handleA, classToB(), false)
	require.NoError(t, err)
	require.False(t, ready)
	ready, err = s.resync(t, s.handleA, classToB(), false)
	require.NoError(t, err)
	assert.False(t, ready, "still within the wait")

	time.Sleep(60 * time.Millisecond)
	ready, err = s.resync(t, s.handleA, classToB(), false)
	require.NoError(t, err)
	assert.True(t, ready, "the wait ran out")
	_, waiting := s.server.revertWaits.Load(revertWaitKey(arrayA, s.a.dirs[replVolumePath]))
	assert.False(t, waiting, "the wait is dropped")
}

// Retention off: nothing is waited for and nothing extra is listed, as
// before.
func TestZeroRetentionDoesNotHoldResyncReadyBack(t *testing.T) {
	s := newReplScenario(t)
	s.a.revertSnapshots, s.b.revertSnapshots = true, true
	s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleA, classToB()))
	s.a.revertSnapshotLate = 4

	_, err := s.resync(t, s.handleA, classToB(), false)
	require.NoError(t, err)
	ready, err := s.resync(t, s.handleA, classToB(), false)
	require.NoError(t, err)
	assert.True(t, ready)
	assert.Zero(t, countCalls(s.a, "SetSnapshotExpiration"))
}

// Disabling replication is the driver's last call for a relationship, so a
// revert snapshot that appeared after the last resync poll -- on either
// array, each having been the reverted target at some point -- is given its
// expiration there.
func TestDisableStampsRevertSnapshotsOnBothArrays(t *testing.T) {
	s := newReplScenario(t)
	s.driver.RevertSnapshotRetention = DefaultRevertSnapshotRetention
	s.protect(t, classToB())
	rootA, rootB := s.a.dirs[replVolumePath], s.b.dirs[replVolumePath]
	require.NotEmpty(t, rootA)
	require.NotEmpty(t, rootB)
	other := s.a.mkdir("/csi/volumes/pvc-other")
	s.a.snapshots = append(s.a.snapshots,
		qumulo.SnapshotInfo{ID: "801", Name: "801_revert_to_800_for_pvc-repl", SourceFileID: rootA},
		qumulo.SnapshotInfo{ID: "802", Name: "802_revert_to_1_for_pvc-other", SourceFileID: other})
	s.b.snapshots = append(s.b.snapshots,
		qumulo.SnapshotInfo{ID: "811", Name: "811_revert_to_810_for_pvc-repl", SourceFileID: rootB})
	start := time.Now()

	require.NoError(t, s.disable(t, s.handleA, classToB()))

	got := map[string]qumulo.SnapshotInfo{}
	for _, sn := range append(append([]qumulo.SnapshotInfo(nil), s.a.snapshots...), s.b.snapshots...) {
		got[sn.ID] = sn
	}
	requireExpiresInAboutAWeek(t, []qumulo.SnapshotInfo{got["801"], got["811"]}, start)
	assert.True(t, got["802"].Expiration.IsZero(), "another directory's revert snapshot is not touched")
}
