// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"testing"

	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// Once Disable has removed both ends, a read-write replica export over
// data is the only evidence Enable has that the copy was promoted, so a pair
// whose replica export was read-write at Disable -- left so by an earlier
// build, or changed by hand -- was refused on every re-Enable and left
// unprotected. While the relationship still exists its read-only target end
// says the copy is a replica, so Disable sets the driver's own export
// read-only first, and a plain re-Enable re-protects.
func TestDisableFencesTheDriversReadWriteReplicaExportSoAPlainReEnableReprotects(t *testing.T) {
	s := newReplScenario(t)
	class := classToB("allowNonEmptyTarget", "true")
	s.protect(t, class)
	require.Len(t, s.b.exports, 1)
	s.b.mkdir(replVolumePath + "/data")
	s.b.exports[0].ReadOnly = false // as an earlier build left it
	require.True(t, madeByThisDriver(&s.b.exports[0]))

	require.NoError(t, s.disable(t, s.handleA, class))
	require.Zero(t, s.fabric.relCount(), "Disable removed both ends")
	assert.True(t, s.b.exports[0].ReadOnly, "Disable left the driver's replica export read-write")

	require.NoError(t, s.enable(t, class), "a pair never failed over is re-protected by a plain re-enable")
	assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
}

// An operator's export -- a group's covering export -- is never changed,
// by Disable either. Disable still succeeds (it runs from a
// finalizer), and a re-Enable over a copy written through that export since
// is refused: adopting it as a replica would discard its writes. (One
// unchanged since Disable is re-protected: replication_unpromoted_test.go.)
func TestDisableLeavesAnOperatorsReadWriteCoveringExportAndReEnableStillRefuses(t *testing.T) {
	s := newSharedScenario(t)
	s.withCoveringExportOnB(true)
	class := groupClass("allowNonEmptyTarget", "true")
	require.NoError(t, s.enableGroup(t, class, ""))
	s.b.exports[0].ReadOnly = false // the operator's hand edit
	before := len(s.b.calls)

	_, err := s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
		VolumeId: s.memberHandle("pvc-tenant-a"), Parameters: class, Secrets: replSecrets(),
	})
	require.NoError(t, err, "Disable must not be refused over an export setting")
	require.Zero(t, s.fabric.relCount())
	assert.False(t, s.b.exports[0].ReadOnly, "Disable changed the operator's export")
	assert.NotContains(t, s.b.calls[before:], "SetExportReadOnly", "Disable wrote the operator's export")

	s.b.mkdir(sharedBasePath + "/pvc-tenant-a/written-through-the-export") //
	err = s.enableGroup(t, class, "")
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "read-write")
	assert.Zero(t, s.fabric.relCount(), "a copy whose history is unknown was adopted as a replica")
}

// The fence never blocks Disable: an export the array will not let this
// driver change leaves Disable as it was before the fence existed.
func TestDisableIsNotBlockedByAReplicaExportItCannotFence(t *testing.T) {
	s := newReplScenario(t)
	class := classToB("allowNonEmptyTarget", "true")
	s.protect(t, class)
	s.b.mkdir(replVolumePath + "/data")
	s.b.exports[0].ReadOnly = false
	s.b.refuseExportWrites = true

	require.NoError(t, s.disable(t, s.handleA, class))
	assert.Zero(t, s.fabric.relCount(), "Disable removed both ends")
	assert.False(t, s.b.exports[0].ReadOnly)
	assert.Contains(t, s.b.calls, "SetExportReadOnly")
}

// The target end is read again just before the fence: a promote that lands
// between the relationship read and the fence made the export read-write on
// purpose, and it is left so.
func TestDisableDoesNotFenceAReplicaExportWhoseTargetWasPromotedMeanwhile(t *testing.T) {
	s := newReplScenario(t)
	class := classToB("allowNonEmptyTarget", "true")
	s.protect(t, class)
	s.b.exports[0].ReadOnly = false
	reads := 0
	s.b.onTargetStatus = func(r *fakeRel) {
		reads++
		r.targetRO = false // the promote, landing now
	}
	before := len(s.b.calls)

	_ = s.disable(t, s.handleA, class)
	t.Logf("target-status reads during Disable: %d; B calls %v", reads, s.b.calls[before:])
	assert.False(t, s.b.exports[0].ReadOnly, "the export of a copy promoted meanwhile was set read-only")
	assert.NotContains(t, s.b.calls[before:], "SetExportReadOnly")
}

// A promoted peer keeps its writable export: Disable on the old primary
// removes only its own end and never reaches the fence.
func TestDisableOnTheOldPrimaryLeavesThePromotedCopysExportWritable(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.False(t, s.b.exports[0].ReadOnly, "promote made the replica's export writable")
	before := len(s.b.calls)

	require.NoError(t, s.disable(t, s.handleA, classToB()))
	assert.False(t, s.b.exports[0].ReadOnly, "Disable fenced the promoted live copy's export")
	assert.NotContains(t, s.b.calls[before:], "SetExportReadOnly")
}
