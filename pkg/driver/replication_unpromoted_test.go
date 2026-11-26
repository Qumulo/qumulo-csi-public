// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// Once Disable has removed both ends, a read-write replica export over
// data is all Enable has to go on, and it cannot tell a promoted copy from an
// export an operator left read-write. So Disable, while the peer's end still
// says the copy is a read-only replica, snapshots the replica root (the
// unpromoted marker), and Enable re-protects over a read-write export only
// when the copy is unchanged since that snapshot: overwriting it then loses
// nothing. A copy written since, no marker, or a comparison that fails is
// refused as before.

// unpromotedMarkersOn is every unpromoted marker snapshot on an array.
func unpromotedMarkersOn(a *fakeArray) []qumulo.SnapshotInfo {
	var out []qumulo.SnapshotInfo
	for _, snap := range a.snapshots {
		if strings.Contains(snap.Name, "_csi-unpromoted-") {
			out = append(out, snap)
		}
	}
	return out
}

// reprotectChecksOn is every snapshot Enable took to compare with a marker
// and did not remove.
func reprotectChecksOn(a *fakeArray) []qumulo.SnapshotInfo {
	var out []qumulo.SnapshotInfo
	for _, snap := range a.snapshots {
		if strings.Contains(snap.Name, "_csi-reprotect-check-") {
			out = append(out, snap)
		}
	}
	return out
}

// captureWarnings sends the default logger's WARN and above to a buffer for
// the rest of the test. Not for parallel tests.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logged bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(restore) })
	return &logged
}

// unchangedReprotectWarnings counts the one data-loss warning Enable gives
// when it re-protects over a read-write export.
func unchangedReprotectWarnings(logged *bytes.Buffer) int {
	n := 0
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, "unchanged since Disable") {
			n++
		}
	}
	return n
}

func (s *sharedScenario) disableGroup(t *testing.T, class map[string]string) error {
	t.Helper()
	_, err := s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
		VolumeId: s.memberHandle("pvc-tenant-a"), Parameters: class, Secrets: replSecrets(),
	})
	return err
}

// readWriteGroupDisabled is a group protected while the operator's DR
// covering export was read-only, the export then set read-write by hand, and
// replication disabled through the driver.
func readWriteGroupDisabled(t *testing.T) (*sharedScenario, map[string]string) {
	t.Helper()
	s := newSharedScenario(t)
	s.withCoveringExportOnB(true)
	class := groupClass("allowNonEmptyTarget", "true")
	require.NoError(t, s.enableGroup(t, class, ""))
	s.b.exports[0].ReadOnly = false // the operator's hand edit
	require.NoError(t, s.disableGroup(t, class))
	require.Zero(t, s.fabric.relCount(), "Disable removed both ends")
	return s, class
}

func TestDisableMarksTheGroupsReplicaRootUnpromoted(t *testing.T) {
	s, _ := readWriteGroupDisabled(t)

	markers := unpromotedMarkersOn(s.b)
	require.Len(t, markers, 1, "Disable marked the replica root")
	assert.Equal(t, s.b.dirs[sharedBasePath], markers[0].SourceFileID, "the marker is of the group's covering root on the DR array")
	assert.Empty(t, unpromotedMarkersOn(s.a), "nothing is marked on the source")
	assert.False(t, s.b.exports[0].ReadOnly, "Disable changed the operator's export")
}

func TestReEnableReprotectsAGroupUnchangedSinceDisableOverItsReadWriteCoveringExport(t *testing.T) {
	s, class := readWriteGroupDisabled(t)
	logged := captureWarnings(t)
	before := len(s.b.calls)

	require.NoError(t, s.enableGroup(t, class, ""), "a copy unchanged since Disable is re-protected")

	assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
	assert.Empty(t, unpromotedMarkersOn(s.b), "the marker went once the relationship was re-created")
	assert.Empty(t, reprotectChecksOn(s.b), "the comparison snapshot was removed")
	assert.False(t, s.b.exports[0].ReadOnly, "Enable changed the operator's export")
	assert.NotContains(t, s.b.calls[before:], "SetExportReadOnly", "Enable wrote the operator's export")
	assert.Equal(t, 1, unchangedReprotectWarnings(logged), "one warning names the read-write export: %s", logged.String())
	assert.Contains(t, logged.String(), sharedExportPath)
}

func TestReEnableRefusesAGroupWrittenSinceDisable(t *testing.T) {
	s, class := readWriteGroupDisabled(t)
	s.b.mkdir(sharedBasePath + "/pvc-tenant-a/written-through-the-export")
	s.b.calls = nil

	err := s.enableGroup(t, class, "")

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "changed since")
	assert.Contains(t, err.Error(), "read-write")
	assert.Contains(t, err.Error(), "was promoted")
	assert.Zero(t, s.fabric.relCount(), "a copy written since Disable was adopted as a replica")
	assert.NotContains(t, s.b.calls, "Authorize")
	assert.Empty(t, reprotectChecksOn(s.b), "the comparison snapshot was removed")
	_, ok := s.b.dirs[sharedBasePath+"/pvc-tenant-a/written-through-the-export"]
	assert.True(t, ok, "the copy's data is untouched")
}

// A pair disabled by an earlier build has no marker: refused as before, and
// the message puts the remedy for a copy never failed over first.
func TestReEnableRefusesAReadWriteCopyWithNoMarker(t *testing.T) {
	s, class := readWriteGroupDisabled(t)
	s.b.snapshots = nil // as an earlier build's Disable left it

	err := s.enableGroup(t, class, "")

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "no marker snapshot")
	assert.Contains(t, err.Error(), "was promoted")
	assert.Contains(t, err.Error(), "read-write")
	msg := err.Error()
	remedy := strings.Index(msg, "never failed over")
	promoted := strings.Index(msg, "If it was promoted")
	require.True(t, remedy >= 0 && promoted >= 0, "message: %s", msg)
	assert.Less(t, remedy, promoted, "the remedy for a copy never failed over comes first: %s", msg)
	assert.Contains(t, msg, "set export "+sharedExportPath+" read-only on "+arrayB)
	assert.Zero(t, s.fabric.relCount())
}

// Disable is never refused over the marker: it runs from the finalizer.
func TestDisableIsNotBlockedByAMarkerItCannotTake(t *testing.T) {
	s := newSharedScenario(t)
	s.withCoveringExportOnB(true)
	class := groupClass("allowNonEmptyTarget", "true")
	require.NoError(t, s.enableGroup(t, class, ""))
	s.b.exports[0].ReadOnly = false
	s.b.snapshotFault = func(verb, _ string) error {
		if verb == "CreateExpiringSnapshot" {
			return fakeAPIError(403, "snapshot_access_denied", "no snapshot write privilege")
		}
		return nil
	}

	require.NoError(t, s.disableGroup(t, class), "Disable must not be refused over the marker")
	assert.Zero(t, s.fabric.relCount(), "Disable removed both ends")
	assert.Empty(t, unpromotedMarkersOn(s.b))

	s.b.snapshotFault = nil
	err := s.enableGroup(t, class, "")
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "no marker snapshot")
	assert.Zero(t, s.fabric.relCount())
}

func TestReEnableRefusesWhenTheMarkerCannotBeCompared(t *testing.T) {
	for _, tc := range []struct {
		name, verb, suffix string
	}{
		{"the fresh snapshot fails", "CreateExpiringSnapshot", "csi-reprotect-check-"},
		{"the comparison fails", "SnapshotDiffEmpty", ""},
		{"the snapshot listing fails", "ListSnapshots", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, class := readWriteGroupDisabled(t)
			s.b.snapshotFault = func(verb, suffix string) error {
				if verb == tc.verb && strings.HasPrefix(suffix, tc.suffix) {
					return fakeAPIError(500, "fs_internal_error", "the array broke in "+tc.verb)
				}
				return nil
			}

			err := s.enableGroup(t, class, "")

			requireCode(t, err, codes.FailedPrecondition)
			assert.Contains(t, err.Error(), "could not be compared")
			assert.Contains(t, err.Error(), "the array broke in "+tc.verb)
			assert.Contains(t, err.Error(), "was promoted")
			assert.Zero(t, s.fabric.relCount())
			assert.Empty(t, reprotectChecksOn(s.b), "the comparison snapshot was left behind")
		})
	}
}

// A promote landing between Disable's read of the peer's end and the marker
// leaves no marker: the copy may take writes before the ends are removed.
func TestDisableDropsTheMarkerWhenTheTargetIsPromotedMeanwhile(t *testing.T) {
	s := newSharedScenario(t)
	s.withCoveringExportOnB(true)
	class := groupClass("allowNonEmptyTarget", "true")
	require.NoError(t, s.enableGroup(t, class, ""))
	s.b.exports[0].ReadOnly = false
	s.b.onTargetStatus = func(r *fakeRel) { r.targetRO = false } // the promote, landing now

	require.NoError(t, s.disableGroup(t, class))
	assert.Empty(t, unpromotedMarkersOn(s.b), "a marker survived a promote")

	s.b.onTargetStatus = nil
	err := s.enableGroup(t, class, "")
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "was promoted")
}

// Two Disable and re-Enable cycles: each Disable replaces older markers of
// the root, and Enable compares with the newest.
func TestTwoDisableCyclesUseTheNewestMarker(t *testing.T) {
	s, class := readWriteGroupDisabled(t)
	require.NoError(t, s.enableGroup(t, class, ""), "first cycle")
	require.Empty(t, unpromotedMarkersOn(s.b))

	// A marker an earlier cycle could not remove, of a tree since changed.
	root := s.b.dirs[sharedBasePath]
	stale, err := s.b.CreateExpiringSnapshot(context.Background(), root, "csi-unpromoted-earlier", time.Now().Add(time.Hour))
	require.NoError(t, err)
	s.b.mkdir(sharedBasePath + "/pvc-tenant-a/replicated-since")

	require.NoError(t, s.disableGroup(t, class))
	markers := unpromotedMarkersOn(s.b)
	require.Len(t, markers, 1, "the second Disable replaced the older marker")
	assert.NotEqual(t, stale.ID, markers[0].ID)

	require.NoError(t, s.enableGroup(t, class, ""), "second cycle")
	assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
	assert.Empty(t, unpromotedMarkersOn(s.b))
}

func TestReEnableComparesWithTheNewestMarkerWhenAnOlderOneIsLeft(t *testing.T) {
	s := newSharedScenario(t)
	s.withCoveringExportOnB(true)
	class := groupClass("allowNonEmptyTarget", "true")
	require.NoError(t, s.enableGroup(t, class, ""))
	s.b.exports[0].ReadOnly = false
	root := s.b.dirs[sharedBasePath]
	_, err := s.b.CreateExpiringSnapshot(context.Background(), root, "csi-unpromoted-earlier", time.Now().Add(time.Hour))
	require.NoError(t, err)
	s.b.mkdir(sharedBasePath + "/pvc-tenant-a/replicated-since")
	// The older marker cannot be removed this time.
	s.b.snapshotFault = func(verb, _ string) error {
		if verb == "DeleteSnapshot" {
			return fakeAPIError(500, "fs_internal_error", "busy")
		}
		return nil
	}
	require.NoError(t, s.disableGroup(t, class))
	require.Len(t, unpromotedMarkersOn(s.b), 2)
	s.b.snapshotFault = nil

	require.NoError(t, s.enableGroup(t, class, ""), "the newest marker is the one compared")
	assert.Empty(t, unpromotedMarkersOn(s.b), "both markers of the root went with the re-protect")
}

// The fenced-dedicated case: a dedicated volume whose driver-made replica export was
// read-write is fenced by Disable, and the marker it also takes goes with
// the re-protect.
func TestDisableStillFencesTheDriversExportAndTheMarkerGoesWithTheReprotect(t *testing.T) {
	s := newReplScenario(t)
	class := classToB("allowNonEmptyTarget", "true")
	s.protect(t, class)
	s.b.mkdir(replVolumePath + "/data")
	s.b.exports[0].ReadOnly = false
	require.True(t, madeByThisDriver(&s.b.exports[0]))

	require.NoError(t, s.disable(t, s.handleA, class))
	assert.True(t, s.b.exports[0].ReadOnly, "Disable fenced the driver's export")
	markers := unpromotedMarkersOn(s.b)
	require.Len(t, markers, 1)
	assert.Equal(t, s.b.dirs[replVolumePath], markers[0].SourceFileID, "the marker is of the volume's replica directory")

	require.NoError(t, s.enable(t, class))
	assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
	assert.Empty(t, unpromotedMarkersOn(s.b))
}

// A driver-made export Disable could not fence: the marker re-protects it,
// and prestage fences the export as before.
func TestReEnableReprotectsADriverExportDisableCouldNotFence(t *testing.T) {
	s := newReplScenario(t)
	class := classToB("allowNonEmptyTarget", "true")
	s.protect(t, class)
	s.b.mkdir(replVolumePath + "/data")
	s.b.exports[0].ReadOnly = false
	s.b.refuseExportWrites = true

	require.NoError(t, s.disable(t, s.handleA, class))
	require.False(t, s.b.exports[0].ReadOnly)
	s.b.refuseExportWrites = false

	require.NoError(t, s.enable(t, class))
	assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
	assert.True(t, s.b.exports[0].ReadOnly, "prestage fenced the driver's own export")
	assert.Empty(t, unpromotedMarkersOn(s.b))
}

// A dedicated volume whose replica export an operator made: the same rule.
func TestReEnableOfADedicatedVolumeWithAnOperatorsReadWriteExport(t *testing.T) {
	setup := func(t *testing.T) (*replScenario, map[string]string) {
		s := newReplScenario(t)
		s.b.mkdir(replVolumePath + "/data")
		s.b.exports = append(s.b.exports, qumulo.ExportInfo{
			ID: "77", ExportPath: replExportPath, FSPath: replVolumePath, TenantID: 1,
			AuthenticationMode: qumulo.AuthenticationModeNone, AllowedClients: []string{"10.0.0.0/8"},
			ReadOnly: true, Description: "made by the storage team",
		})
		class := classToB("allowNonEmptyTarget", "true")
		s.protect(t, class)
		s.b.exports[0].ReadOnly = false
		require.NoError(t, s.disable(t, s.handleA, class))
		require.Zero(t, s.fabric.relCount())
		return s, class
	}

	t.Run("unchanged since Disable", func(t *testing.T) {
		s, class := setup(t)
		require.NoError(t, s.enable(t, class))
		assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
		assert.False(t, s.b.exports[0].ReadOnly, "the operator's export was changed")
		assert.Empty(t, unpromotedMarkersOn(s.b))
	})

	t.Run("written since Disable", func(t *testing.T) {
		s, class := setup(t)
		s.b.mkdir(replVolumePath + "/written-through-the-export")
		err := s.enable(t, class)
		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), "changed since")
		assert.Zero(t, s.fabric.relCount())
	})
}

// An Enable that re-protected over the export and was answered Aborted after
// the create (the peer not listing the relationship yet) is retried against
// a relationship AWAITING_AUTHORIZATION. The marker is still there for it,
// so the retry is not refused over the export the first attempt accepted;
// with no marker the retry refuses as it always has.
func TestARetriedReprotectStillFindsTheMarker(t *testing.T) {
	for _, marked := range []bool{true, false} {
		t.Run(map[bool]string{true: "marker kept", false: "no marker"}[marked], func(t *testing.T) {
			s, class := readWriteGroupDisabled(t)
			if !marked {
				s.b.snapshots = nil
			}
			s.fabric.seedRel(s.a, s.b, sharedBasePath, sharedBasePath, qumulo.RelationshipAwaitingAuthorization,
				func(r *fakeRel) { r.targetRO = false })

			err := s.enableGroup(t, class, "")

			if !marked {
				requireCode(t, err, codes.FailedPrecondition)
				assert.Contains(t, err.Error(), "read-write")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
			assert.False(t, s.b.exports[0].ReadOnly, "the operator's export was changed")
			assert.Empty(t, unpromotedMarkersOn(s.b), "the marker went once the relationship was staged")
		})
	}
}

// A marker is recognised by its stored "<id>_csi-unpromoted-" name on its
// root, never by a name that merely contains the tag, a policy's snapshot,
// or one being deleted; a promote's snapshot check skips markers and
// Enable's comparison snapshots alike.
func TestUnpromotedMarkerNamesAreAnchored(t *testing.T) {
	marker := qumulo.SnapshotInfo{ID: "41", Name: "41_" + unpromotedMarkerName("0123456789ab"), SourceFileID: "7"}
	assert.Equal(t, "41_csi-unpromoted-01234567", marker.Name)
	assert.True(t, isUnpromotedMarker(marker, "7"))
	assert.False(t, isUnpromotedMarker(marker, "8"), "another root's")
	for _, other := range []qumulo.SnapshotInfo{
		{ID: "42", Name: "42_snapshot-csi-unpromoted-x", SourceFileID: "7"},
		{ID: "43", Name: "43_csi-unpromoted-x", SourceFileID: "7", PolicyID: "3"},
		{ID: "44", Name: "44_csi-unpromoted-x", SourceFileID: "7", InDelete: true},
		{ID: "45", Name: "41_csi-unpromoted-x", SourceFileID: "7"},
	} {
		assert.False(t, isUnpromotedMarker(other, "7"), other.Name)
	}
	assert.True(t, isReprotectSnapshot(marker))
	assert.True(t, isReprotectSnapshot(qumulo.SnapshotInfo{ID: "46", Name: "46_" + unpromotedCheckTag + "41"}))
	assert.False(t, isReprotectSnapshot(qumulo.SnapshotInfo{ID: "47", Name: "47_replication_from_..."}))
	assert.Equal(t, 30*24*time.Hour, unpromotedMarkerTTL)
}
