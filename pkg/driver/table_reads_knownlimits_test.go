//go:build defects && knownlimits

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The table reads accepted as a known limitation (defects.yaml
// known_limit, docs/KNOWN-ISSUES.md whole-table row): the
// Qumulo API has no lookup by path or by name, so these reads remain and
// these tests fail by design. They are kept behind their own tag so that
// `make test`, which runs every other TestDefect_*, stays green; the
// regression runner's defects-code case passes `knownlimits` and reports
// them as XFAIL. The fixed reads, and the helpers these use, are in
// table_reads_defects_test.go.

// TestDefect_ExpandDoesNotListRelationshipTables: known limitation
// (relationshipsAt must see a group relationship rooted above the volume).
func TestDefect_ExpandDoesNotListRelationshipTables(t *testing.T) {
	reads := dfxTableReads(t)
	dfxAtMostHalf(t, reads, "ControllerExpandVolume/source relationships")
	dfxAtMostHalf(t, reads, "ControllerExpandVolume/target relationships")
}

// TestDefect_DeleteVolumeIdentityCheckDoesNotListExports: known
// limitation (another volume's export below the directory must be seen).
func TestDefect_DeleteVolumeIdentityCheckDoesNotListExports(t *testing.T) {
	reads := dfxTableReads(t)
	dfxAtMostHalf(t, reads, "DeleteVolume(dedicated)/exports")
	dfxAtMostHalf(t, reads, "DeleteVolume(shared)/exports")
}

// TestDefect_DeleteVolumeGuardDoesNotListRelationshipTables: known
// limitation (guardReplicatedDelete must see relationships at or below the
// path).
func TestDefect_DeleteVolumeGuardDoesNotListRelationshipTables(t *testing.T) {
	reads := dfxTableReads(t)
	dfxAtMostHalf(t, reads, "DeleteVolume(shared)/source relationships")
	dfxAtMostHalf(t, reads, "DeleteVolume(shared)/target relationships")
}

// TestDefect_UnscheduledDeleteVolumeDoesNotListPolicies: known
// limitation (a volume whose class set no schedule has no policy ID to read,
// and "no policy" is not trusted by a delete, which sweeps orphans).
func TestDefect_UnscheduledDeleteVolumeDoesNotListPolicies(t *testing.T) {
	dfxAtMostHalf(t, dfxTableReads(t), "DeleteVolume(shared)/snapshot policies")
}

// TestDefect_NewCreateSnapshotDoesNotListSnapshots: known limitation --
// a name not seen before is looked for by listing, since an earlier attempt
// may have taken it before its reply was lost.
func TestDefect_NewCreateSnapshotDoesNotListSnapshots(t *testing.T) {
	n := dfxCreateSnapshots(t, func(i int) string { return fmt.Sprintf("snap-%d", i) })
	assert.LessOrEqual(t, n, dfxOps/2, "each CreateSnapshot of a new name listed every snapshot")
}

// TestDefect_DrainPollsDoNotListTheTargetSnapshotTable: known
// limitation -- the replicated copy is renamed and found by timestamp only.
func TestDefect_DrainPollsDoNotListTheTargetSnapshotTable(t *testing.T) {
	_, target := dfxDrainListings(t)
	require.Greater(t, target, 3)
	assert.LessOrEqual(t, target, 2, "each drain poll listed the target's whole snapshot table")
}
