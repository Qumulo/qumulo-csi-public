// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// newMockClient stands a cluster up behind httptest and points the real
// client at it, so this test exercises the wire shapes the driver will see
// rather than the mock's own maps.
func newMockClient(t *testing.T) (*cluster, *qumulo.Client) {
	t.Helper()

	c := newCluster("Qumulo Core test", 0, true, 0)
	server := httptest.NewServer(c)
	t.Cleanup(server.Close)
	return c, qumulo.New(server.URL, "")
}

// TestReplicationStateMachine walks the measured transitions in order:
// create, authorize, make-writable (twice), reverse, reconnect, delete.
func TestReplicationStateMachine(t *testing.T) {
	c, client := newMockClient(t)
	ctx := context.Background()

	source, err := client.CreateDirectory(ctx, "/csi/volumes", "pvc-a")
	require.NoError(t, err)

	rel, err := client.CreateSource(ctx, qumulo.SourceSpec{
		SourceRootPath:     "/csi/volumes/pvc-a",
		TargetRootPath:     "/csi-dr/pvc-a",
		TargetAddress:      "10.0.0.2",
		ReplicationEnabled: true,
		ReplicationMode:    qumulo.ReplicationModeSnapshotPolicyContinuous,
		SnapshotPolicies:   []qumulo.SnapshotPolicyLink{{ID: "3", TargetExpiration: "same_as_policy"}},
	})
	require.NoError(t, err)
	assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, rel.ReplicationMode)

	// Before authorize: awaiting, no target directory, no recovery point.
	statuses, err := client.ListSourceStatus(ctx)
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	assert.Equal(t, qumulo.RelationshipAwaitingAuthorization, statuses[0].State)
	assert.Equal(t, "/csi/volumes/pvc-a", statuses[0].SourceRootPath, "trailing slash must be cleaned by the client")
	assert.Equal(t, source.ID, statuses[0].SourceRootID)
	assert.False(t, statuses[0].HasRecoveryPoint())
	_, err = client.LookupFileByPath(ctx, "/csi-dr/pvc-a")
	assert.True(t, qumulo.IsNotFound(err), "the target directory exists only after authorize")

	// A second create for the same pair is refused, which is what lets a
	// retried Enable adopt rather than duplicate.
	_, err = client.CreateSource(ctx, qumulo.SourceSpec{
		SourceRootPath: "/csi/volumes/pvc-a", TargetRootPath: "/csi-dr/pvc-a", TargetAddress: "10.0.0.2",
	})
	assert.True(t, qumulo.IsConflict(err))

	// Authorize without allow-fs-path-create is refused because the directory
	// does not exist; with it, the directory appears and the relationship is
	// established with a recovery point.
	_, err = client.Authorize(ctx, rel.ID, false, false)
	require.Error(t, err)
	status, err := client.Authorize(ctx, rel.ID, true, false)
	require.NoError(t, err)
	assert.Equal(t, qumulo.RelationshipEstablished, status.State)
	assert.True(t, status.TargetRootReadOnly)
	assert.True(t, status.HasRecoveryPoint())
	target, err := client.LookupFileByPath(ctx, "/csi-dr/pvc-a")
	require.NoError(t, err)
	assert.NotEqual(t, source.ID, target.ID, "the replica has its own file ID")

	// Pre-staging a quota and an export on the read-only target works.
	require.NoError(t, client.CreateQuota(ctx, target.ID, 1<<30))
	export, err := client.CreateExport(ctx, qumulo.ExportSpec{
		ExportPath: "/dr/pvc-a", FSPath: "/csi-dr/pvc-a", ReadOnly: true,
	})
	require.NoError(t, err)
	exports, err := client.ListExports(ctx)
	require.NoError(t, err)
	require.Len(t, exports, 1)
	assert.True(t, exports[0].ReadOnly)

	// A manual replicate is refused while enabled.
	err = client.Replicate(ctx, rel.ID)
	assert.True(t, qumulo.IsManualReplicationDisallowed(err))

	// A freeze is a partial patch: the mode and policies survive it.
	frozen := true
	patched, err := client.PatchSource(ctx, rel.ID, qumulo.SourcePatch{SourceRootReadOnly: &frozen})
	require.NoError(t, err)
	assert.True(t, patched.SourceRootReadOnly)
	assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, patched.ReplicationMode)
	require.Len(t, patched.SnapshotPolicies, 1)

	// The array clock is what the recovery point is compared against.
	now, err := client.ClusterTime(ctx)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), now, 5*time.Second)
	after, err := client.GetSourceStatus(ctx, rel.ID)
	require.NoError(t, err)
	assert.False(t, after.RecoveryPoint.Before(now.Add(-time.Second)), "an established continuous relationship drains")

	// make-writable once: DISCONNECTED and writable. Twice: the measured 400.
	status, err = client.MakeTargetWritable(ctx, rel.ID)
	require.NoError(t, err)
	assert.Equal(t, qumulo.RelationshipDisconnected, status.State)
	assert.False(t, status.TargetRootReadOnly)
	_, err = client.MakeTargetWritable(ctx, rel.ID)
	assert.True(t, qumulo.IsAlreadyDisconnected(err), "second make-writable must be the measured error class")

	// The pre-staged export flips to read-write at promote.
	require.NoError(t, client.SetExportReadOnly(ctx, export.ID, false))
	exports, err = client.ListExports(ctx)
	require.NoError(t, err)
	assert.False(t, exports[0].ReadOnly)

	// Reverse keeps the ID, swaps roles, resets mode and policies.
	reversed, err := client.Reverse(ctx, rel.ID, "10.0.0.1", 0)
	require.NoError(t, err)
	assert.Equal(t, rel.ID, reversed.ID)
	assert.Equal(t, "10.0.0.1", reversed.TargetAddress)
	assert.Equal(t, qumulo.ReplicationModeContinuous, reversed.ReplicationMode)
	assert.Empty(t, reversed.SnapshotPolicies)
	src, err := client.GetSourceStatus(ctx, rel.ID)
	require.NoError(t, err)
	assert.Equal(t, "/csi-dr/pvc-a", src.SourceRootPath)
	assert.Equal(t, "/csi/volumes/pvc-a", src.TargetRootPath)
	assert.Equal(t, qumulo.RelationshipDisconnected, src.State)
	assert.True(t, src.TargetRootReadOnly, "the frozen old source is the read-only new target")

	// Reconnect re-establishes.
	status, err = client.Reconnect(ctx, rel.ID)
	require.NoError(t, err)
	assert.Equal(t, qumulo.RelationshipEstablished, status.State)
	assert.True(t, status.TargetRootReadOnly)

	// Deleting one end leaves the other ENDED; deleting both removes it.
	require.NoError(t, client.DeleteSource(ctx, rel.ID))
	_, err = client.GetSourceStatus(ctx, rel.ID)
	assert.True(t, qumulo.IsNotFound(err))
	tgt, err := client.GetTargetStatus(ctx, rel.ID)
	require.NoError(t, err)
	assert.Equal(t, qumulo.RelationshipEnded, tgt.State)
	require.NoError(t, client.DeleteTarget(ctx, rel.ID))
	c.mu.Lock()
	assert.Empty(t, c.relationships)
	c.mu.Unlock()
}

// A policy-only relationship's recovery point does not move until the mode is
// switched to include continuous replication -- the drain a planned promote
// relies on.
func TestPolicyOnlyRecoveryPointIsPinnedUntilModeSwitch(t *testing.T) {
	_, client := newMockClient(t)
	ctx := context.Background()

	_, err := client.CreateDirectory(ctx, "/csi/volumes", "pvc-b")
	require.NoError(t, err)
	rel, err := client.CreateSource(ctx, qumulo.SourceSpec{
		SourceRootPath: "/csi/volumes/pvc-b", TargetRootPath: "/csi-dr/pvc-b", TargetAddress: "10.0.0.2",
		ReplicationEnabled: true, ReplicationMode: qumulo.ReplicationModeSnapshotPolicy,
	})
	require.NoError(t, err)
	authorized, err := client.Authorize(ctx, rel.ID, true, false)
	require.NoError(t, err)

	time.Sleep(20 * time.Millisecond)
	pinned, err := client.GetSourceStatus(ctx, rel.ID)
	require.NoError(t, err)
	assert.Equal(t, authorized.RecoveryPoint, pinned.RecoveryPoint, "policy-only: recovery point waits for a policy fire")

	_, err = client.PatchSource(ctx, rel.ID, qumulo.SourcePatch{ReplicationMode: qumulo.ReplicationModeSnapshotPolicyContinuous})
	require.NoError(t, err)
	moved, err := client.GetSourceStatus(ctx, rel.ID)
	require.NoError(t, err)
	assert.True(t, moved.RecoveryPoint.After(pinned.RecoveryPoint), "with continuous added the recovery point advances")
}

// A freeze starts no job and the recovery point keeps moving on the cycle
// (measured on Core 7.9.2.2). The mock models
// the cycle as a point trailing the clock by -replication-lag, frozen or not,
// so a drain gate that stamps before the freeze passes once the lag has
// elapsed and never on a stale point.
func TestFreezeStartsNoJobAndTheCycleKeepsMoving(t *testing.T) {
	c, client := newMockClient(t)
	ctx := context.Background()

	_, err := client.CreateDirectory(ctx, "/csi/volumes", "pvc-c")
	require.NoError(t, err)
	rel, err := client.CreateSource(ctx, qumulo.SourceSpec{
		SourceRootPath: "/csi/volumes/pvc-c", TargetRootPath: "/csi-dr/pvc-c", TargetAddress: "10.0.0.2",
		ReplicationEnabled: true, ReplicationMode: qumulo.ReplicationModeContinuous,
	})
	require.NoError(t, err)
	_, err = client.Authorize(ctx, rel.ID, true, false)
	require.NoError(t, err)

	c.mu.Lock()
	c.replicationLag = time.Hour // the cycle has not come round since the freeze
	c.mu.Unlock()
	stamp, err := client.ClusterTime(ctx)
	require.NoError(t, err)
	frozen := true
	_, err = client.PatchSource(ctx, rel.ID, qumulo.SourcePatch{SourceRootReadOnly: &frozen})
	require.NoError(t, err)

	after, err := client.GetSourceStatus(ctx, rel.ID)
	require.NoError(t, err)
	assert.True(t, after.JobIdle(), "a freeze starts no job")
	assert.Equal(t, qumulo.ReplicationJobNotRunning, after.JobState)
	assert.True(t, after.RecoveryPoint.Before(stamp), "the point is where the last cycle left it")

	c.mu.Lock()
	c.replicationLag = 0 // the cycle comes round
	c.mu.Unlock()
	moved, err := client.GetSourceStatus(ctx, rel.ID)
	require.NoError(t, err)
	assert.False(t, moved.RecoveryPoint.Before(stamp), "the frozen source's point advances on the cycle")
	assert.True(t, moved.JobIdle())

	time.Sleep(20 * time.Millisecond)
	later, err := client.GetSourceStatus(ctx, rel.ID)
	require.NoError(t, err)
	assert.True(t, later.RecoveryPoint.After(moved.RecoveryPoint), "and keeps advancing while frozen")
}

// Core 7.9.2.2 requires the export's id in a PUT body even though it is in
// the path, and answers a 400 without it (measured on a lab array).
// The mock's refusal had no test of its own, so a mock that
// went back to accepting the body would have let the client bug through
// again.
func TestExportPutWithoutIDIsRefusedLikeCore(t *testing.T) {
	c := newCluster("Qumulo Core test", 0, true, 0)
	server := httptest.NewServer(c)
	t.Cleanup(server.Close)
	client := qumulo.New(server.URL, "")
	ctx := context.Background()

	_, err := client.CreateDirectory(ctx, "/csi/volumes", "pvc-d")
	require.NoError(t, err)
	export, err := client.CreateExport(ctx, qumulo.ExportSpec{ExportPath: "/exports/pvc-d", FSPath: "/csi/volumes/pvc-d"})
	require.NoError(t, err)

	put := func(body string) (int, string) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+"/v3/nfs/exports/"+export.ID, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		out, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(out)
	}

	code, body := put(`{"description": "no id"}`)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, body, "field 'id' not found")

	code, body = put(`{"id": "` + export.ID + `999", "description": "wrong id"}`)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, body, "does not match the path")

	code, _ = put(`{"id": "` + export.ID + `", "description": "with id"}`)
	assert.Equal(t, http.StatusOK, code)

	// And the real client always sends it, which is what the live run fixed.
	require.NoError(t, client.SetExportReadOnly(ctx, export.ID, true))
}

func TestPoliciesRoundTripSchedules(t *testing.T) {
	_, client := newMockClient(t)
	ctx := context.Background()

	dir, err := client.CreateDirectory(ctx, "/csi/volumes", "pvc-c")
	require.NoError(t, err)
	fire := 15
	schedule := qumulo.Schedule{
		CreationSchedule:     qumulo.CreationSchedule{Frequency: qumulo.ScheduleHourlyOrLess, Timezone: "UTC", FireEvery: fire, FireEveryInterval: qumulo.FireInMinutes},
		ExpirationTimeToLive: "1hours",
	}
	created, err := client.CreatePolicy(ctx, "csi-pvc-c", dir.ID, schedule)
	require.NoError(t, err)

	listed, err := client.ListPolicies(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Equal(t, "csi-pvc-c", listed[0].Name)
	assert.Equal(t, dir.ID, listed[0].SourceFileID)

	got, err := client.GetPolicySchedule(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, schedule, got)

	require.NoError(t, client.DeletePolicy(ctx, created.ID))
	_, err = client.GetPolicySchedule(ctx, created.ID)
	assert.True(t, qumulo.IsNotFound(err))
}

// TestTreeDeleteOfAReplicationRootEndsTheRelationship pins the hazard
// DeleteVolume's guard exists for: Core
// does not refuse a tree-delete of a replication root. It performs it, and
// the relationship goes ENDED on both arrays with the root path emptied and
// the copy on the other array orphaned.
//
// The mock has to reproduce this, not be kinder than the array: a guard that
// fired too late would otherwise still pass its test.
func TestTreeDeleteOfAReplicationRootEndsTheRelationship(t *testing.T) {
	for _, tt := range []struct {
		name       string
		deleteRoot string
		wantReason string
	}{
		{"source root", "/csi/volumes/pvc-a", "SOURCE_DIRECTORY_DELETED"},
		{"target root", "/csi-dr/pvc-a", "TARGET_DIRECTORY_DELETED"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, client := newMockClient(t)
			ctx := context.Background()

			_, err := client.CreateDirectory(ctx, "/csi/volumes", "pvc-a")
			require.NoError(t, err)
			rel, err := client.CreateSource(ctx, qumulo.SourceSpec{
				SourceRootPath:     "/csi/volumes/pvc-a",
				TargetRootPath:     "/csi-dr/pvc-a",
				TargetAddress:      "10.0.0.2",
				ReplicationEnabled: true,
				ReplicationMode:    qumulo.ReplicationModeContinuous,
			})
			require.NoError(t, err)
			_, err = client.Authorize(ctx, rel.ID, true, false)
			require.NoError(t, err)

			doomed, err := client.LookupFileByPath(ctx, tt.deleteRoot)
			require.NoError(t, err)
			require.NoError(t, client.StartTreeDelete(ctx, doomed.ID))
			finished, err := client.TreeDeleteFinished(ctx, doomed.ID)
			require.NoError(t, err)
			require.True(t, finished)

			// ENDED on both ends -- the relationship object survives the
			// directory, which is the orphan support has to clean up.
			sources, err := client.ListSourceStatus(ctx)
			require.NoError(t, err)
			require.Len(t, sources, 1)
			assert.Equal(t, qumulo.RelationshipEnded, sources[0].State)
			assert.Contains(t, sources[0].EndReason, tt.wantReason)

			targets, err := client.ListTargetStatus(ctx)
			require.NoError(t, err)
			require.Len(t, targets, 1)
			assert.Equal(t, qumulo.RelationshipEnded, targets[0].State)

			// The copy on the other side is still there, unprotected.
			survivor := "/csi-dr/pvc-a"
			if tt.deleteRoot == survivor {
				survivor = "/csi/volumes/pvc-a"
			}
			_, err = client.LookupFileByPath(ctx, survivor)
			assert.NoError(t, err, "the other copy is orphaned, not cleaned up")
			assert.NotEmpty(t, c.relationships)
		})
	}
}

// TestAuthorizeMirrorsTheSourceSubtree covers the group form's precondition:
// pre-staging a group means putting a quota on each member directory under
// the replica root, and those directories exist on the target only because
// replication created them. The mock's authorize completes the initial sync
// instantly, so it must create them too.
func TestAuthorizeMirrorsTheSourceSubtree(t *testing.T) {
	_, client := newMockClient(t)
	ctx := context.Background()

	for _, member := range []string{"pvc-tenant-a", "pvc-tenant-b"} {
		_, err := client.CreateDirectory(ctx, "/csi/shared", member)
		require.NoError(t, err)
	}

	rel, err := client.CreateSource(ctx, qumulo.SourceSpec{
		SourceRootPath:     "/csi/shared",
		TargetRootPath:     "/csi/shared-dr",
		TargetAddress:      "10.0.0.2",
		ReplicationEnabled: true,
		ReplicationMode:    qumulo.ReplicationModeContinuous,
	})
	require.NoError(t, err)
	_, err = client.Authorize(ctx, rel.ID, true, false)
	require.NoError(t, err)

	for _, member := range []string{"pvc-tenant-a", "pvc-tenant-b"} {
		replica, err := client.LookupFileByPath(ctx, "/csi/shared-dr/"+member)
		require.NoError(t, err, "member %q did not replicate", member)
		// A quota can be staged on it while the target is still read-only.
		require.NoError(t, client.CreateQuota(ctx, replica.ID, 1<<30))
	}
}

// The marker gate asks the array whether anything changed between the
// marker snapshot of the frozen tree and the last replicated snapshot. The
// mock answers from captured subtrees: identical trees yield no entries, a
// file created after the marker yields one.
func TestChangesSinceComparesAMarkerWithTheReplicatedSnapshot(t *testing.T) {
	c, client := newMockClient(t)
	ctx := context.Background()

	dir, err := client.CreateDirectory(ctx, "/csi/volumes", "pvc-d")
	require.NoError(t, err)
	rel, err := client.CreateSource(ctx, qumulo.SourceSpec{
		SourceRootPath: "/csi/volumes/pvc-d", TargetRootPath: "/csi-dr/pvc-d", TargetAddress: "10.0.0.2",
		ReplicationEnabled: true, ReplicationMode: qumulo.ReplicationModeContinuous,
	})
	require.NoError(t, err)
	_, err = client.Authorize(ctx, rel.ID, true, false)
	require.NoError(t, err)
	c.mu.Lock()
	c.replicationLag = 0
	c.mu.Unlock()

	replicated, err := client.GetSourceStatus(ctx, rel.ID)
	require.NoError(t, err)
	require.NotEmpty(t, replicated.RecoveryPointSnapshotID)
	marker, err := client.CreateExpiringSnapshot(ctx, dir.ID, "csi-freeze-test", time.Now().Add(time.Hour))
	require.NoError(t, err)

	same, err := client.SnapshotDiffEmpty(ctx, marker.ID, replicated.RecoveryPointSnapshotID)
	require.NoError(t, err)
	assert.True(t, same, "nothing changed between the replicated snapshot and the marker")

	_, err = client.CreateDirectory(ctx, "/csi/volumes/pvc-d", "after-marker")
	require.NoError(t, err)
	later, err := client.GetSourceStatus(ctx, rel.ID) // the cycle picks the file up
	require.NoError(t, err)
	assert.NotEqual(t, replicated.RecoveryPointSnapshotID, later.RecoveryPointSnapshotID, "a changed tree gets a new replicated snapshot")
	same, err = client.SnapshotDiffEmpty(ctx, later.RecoveryPointSnapshotID, marker.ID)
	require.NoError(t, err)
	assert.False(t, same, "the marker predates the file: a stale marker compares different")

	_, err = client.SnapshotDiffEmpty(ctx, "999999", marker.ID)
	assert.Error(t, err, "an unknown snapshot is an error, not an empty diff")
}
