//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// The snapshot lifecycle RPCs against a real cluster.
//
// The Makefile's live-snapshot target names CreateSnapshot, DeleteSnapshot and
// ListSnapshots. Until now it matched only the two snapshot *policy* tests,
// through the bare "Snapshot" alternative in its pattern -- so it ran, passed,
// and exercised none of the three RPCs it advertises. That is a worse state
// than a target matching nothing, because the pattern audit cannot see it and
// the run looks like coverage.
package live

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// snapshotOnCluster looks one up by its cluster-side ID.
func snapshotOnCluster(ctx context.Context, t *testing.T, h *harness, csiSnapshotID string) bool {
	t.Helper()

	id := clusterSnapshotID(t, csiSnapshotID)
	snapshots, err := h.Client.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("listing snapshots on the cluster: %v", err)
	}
	for _, snapshot := range snapshots {
		if snapshot.ID == id && !snapshot.InDelete {
			return true
		}
	}
	return false
}

// createSnapshot takes one and registers its removal.
//
// Cleanup is ordered deliberately: a snapshot pins what it captured, so it has
// to go before the directory it was taken of. t.Cleanup runs last-registered
// first, and the volume's own cleanup was registered by provision() before
// this, which puts them in the right order.
func createSnapshot(ctx context.Context, t *testing.T, h *harness, volume provisionedVolume, name string) *csi.Snapshot {
	t.Helper()

	resp, err := h.Ctrl().CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           name,
		SourceVolumeId: volume.VolumeID,
		Secrets:        h.Secrets(),
	})
	if err != nil {
		t.Fatalf("CreateSnapshot(%s): %v", name, err)
	}

	snapshot := resp.GetSnapshot()
	if snapshot == nil {
		t.Fatalf("CreateSnapshot(%s) returned no snapshot", name)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		if _, err := h.Ctrl().DeleteSnapshot(cleanupCtx, &csi.DeleteSnapshotRequest{
			SnapshotId: snapshot.GetSnapshotId(),
			Secrets:    h.Secrets(),
		}); err != nil {
			t.Logf("cleanup: deleting snapshot %s: %v -- it may need removing by hand",
				snapshot.GetSnapshotId(), err)
		}
	})

	return snapshot
}

// TestLiveCreateSnapshotMakesASnapshotOnTheCluster is the base case.
func TestLiveCreateSnapshotMakesASnapshotOnTheCluster(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "snap-src", gib)
	name := h.SnapshotName("snap")
	snapshot := createSnapshot(ctx, t, h, volume, name)

	if got := snapshot.GetSourceVolumeId(); got != volume.VolumeID {
		t.Errorf("SourceVolumeId = %q, want %q", got, volume.VolumeID)
	}
	// Qumulo snapshots are usable the moment they exist, so anything else
	// would leave the VolumeSnapshot waiting forever.
	if !snapshot.GetReadyToUse() {
		t.Error("ReadyToUse is false; a Qumulo snapshot is usable as soon as it is taken")
	}
	if snapshot.GetCreationTime() == nil {
		t.Error("CreationTime is unset; the CO surfaces it on the VolumeSnapshot")
	}
	if want := h.SnapshotHandle(clusterSnapshotID(t, snapshot.GetSnapshotId())); snapshot.GetSnapshotId() != want {
		t.Errorf("snapshot handle %q, want %q (<server>#<apiUrl>#<snapshotID>)", snapshot.GetSnapshotId(), want)
	}

	if !snapshotOnCluster(ctx, t, h, snapshot.GetSnapshotId()) {
		t.Fatalf("CreateSnapshot reported success but the cluster has no snapshot %s",
			snapshot.GetSnapshotId())
	}
}

// TestLiveCreateSnapshotIsIdempotent.
//
// The snapshotter retries, so the same name arrives twice. The spec requires
// the existing snapshot back -- and taking a second one would quietly consume
// capacity on the array for every retry.
func TestLiveCreateSnapshotIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "snap-idem-src", gib)
	name := h.SnapshotName("snap-idem")
	first := createSnapshot(ctx, t, h, volume, name)

	resp, err := h.Ctrl().CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           name,
		SourceVolumeId: volume.VolumeID,
		Secrets:        h.Secrets(),
	})
	if err != nil {
		t.Fatalf("the retry of CreateSnapshot(%s) failed: %v", name, err)
	}
	if got := resp.GetSnapshot().GetSnapshotId(); got != first.GetSnapshotId() {
		t.Errorf("retry returned %q, want the existing %q -- a second snapshot was taken",
			got, first.GetSnapshotId())
	}
}

// TestLiveCreateSnapshotRefusesADifferentSourceForTheSameName.
//
// Same name, different source volume, is a different snapshot. Returning the
// existing one would hand a restore the wrong data.
func TestLiveCreateSnapshotRefusesADifferentSourceForTheSameName(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	first := provision(ctx, t, h, "snap-conflict-a", gib)
	second := provision(ctx, t, h, "snap-conflict-b", gib)

	name := h.SnapshotName("snap-conflict")
	createSnapshot(ctx, t, h, first, name)

	_, err := h.Ctrl().CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           name,
		SourceVolumeId: second.VolumeID,
		Secrets:        h.Secrets(),
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateSnapshot with a conflicting source returned %v, want AlreadyExists", err)
	}
}

// TestLiveCreateSnapshotRejectsAMissingSourceVolume.
func TestLiveCreateSnapshotRejectsAMissingSourceVolume(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	_, err := h.Ctrl().CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           h.SnapshotName("snap-missing"),
		SourceVolumeId: h.NFSServer + "/999999999999/csi-live-does-not-exist",
		Secrets:        h.Secrets(),
	})
	if code := status.Code(err); code != codes.NotFound && code != codes.Internal {
		t.Fatalf("CreateSnapshot of a missing volume returned %v, want NotFound", code)
	}
}

// TestLiveListSnapshotsFindsWhatCreateSnapshotMade.
func TestLiveListSnapshotsFindsWhatCreateSnapshotMade(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "snap-list-src", gib)
	snapshot := createSnapshot(ctx, t, h, volume, h.SnapshotName("snap-list"))

	// By ID: the narrow lookup a CO uses to refresh one VolumeSnapshot.
	byID, err := h.Ctrl().ListSnapshots(ctx, &csi.ListSnapshotsRequest{
		SnapshotId: snapshot.GetSnapshotId(),
		Secrets:    h.Secrets(),
	})
	if err != nil {
		t.Fatalf("ListSnapshots by ID: %v", err)
	}
	if len(byID.GetEntries()) != 1 {
		t.Fatalf("ListSnapshots by ID returned %d entries, want exactly 1", len(byID.GetEntries()))
	}
	if got := byID.GetEntries()[0].GetSnapshot().GetSnapshotId(); got != snapshot.GetSnapshotId() {
		t.Errorf("ListSnapshots by ID returned %q, want %q", got, snapshot.GetSnapshotId())
	}

	// By source volume: what a CO asks when reconciling one PVC's snapshots.
	bySource, err := h.Ctrl().ListSnapshots(ctx, &csi.ListSnapshotsRequest{
		SourceVolumeId: volume.VolumeID,
		Secrets:        h.Secrets(),
	})
	if err != nil {
		t.Fatalf("ListSnapshots by source volume: %v", err)
	}
	found := false
	for _, entry := range bySource.GetEntries() {
		if entry.GetSnapshot().GetSnapshotId() == snapshot.GetSnapshotId() {
			found = true
		}
		if got := entry.GetSnapshot().GetSourceVolumeId(); got != volume.VolumeID {
			t.Errorf("an entry filtered by source names source %q, want %q", got, volume.VolumeID)
		}
	}
	if !found {
		t.Errorf("ListSnapshots for %s did not include %s", volume.VolumeID, snapshot.GetSnapshotId())
	}
}

// TestLiveListSnapshotsForAnUnknownIDIsEmptyNotAnError.
//
// The spec is explicit: a snapshot that does not exist is an empty list. An
// error here makes a CO retry forever over a snapshot somebody deleted.
func TestLiveListSnapshotsForAnUnknownIDIsEmptyNotAnError(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	resp, err := h.Ctrl().ListSnapshots(ctx, &csi.ListSnapshotsRequest{
		SnapshotId: h.NFSServer + "/999999999999",
		Secrets:    h.Secrets(),
	})
	if err != nil {
		t.Fatalf("ListSnapshots for an unknown ID returned %v, want an empty list", err)
	}
	if len(resp.GetEntries()) != 0 {
		t.Errorf("ListSnapshots for an unknown ID returned %d entries, want 0", len(resp.GetEntries()))
	}
}

// TestLiveDeleteSnapshotRemovesItFromTheCluster.
func TestLiveDeleteSnapshotRemovesItFromTheCluster(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "snap-del-src", gib)
	snapshot := createSnapshot(ctx, t, h, volume, h.SnapshotName("snap-del"))

	if !snapshotOnCluster(ctx, t, h, snapshot.GetSnapshotId()) {
		t.Fatal("the snapshot is not on the cluster before the delete; nothing else here means anything")
	}

	if _, err := h.Ctrl().DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{
		SnapshotId: snapshot.GetSnapshotId(),
		Secrets:    h.Secrets(),
	}); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	if snapshotOnCluster(ctx, t, h, snapshot.GetSnapshotId()) {
		t.Errorf("DeleteSnapshot reported success but %s is still on the cluster",
			snapshot.GetSnapshotId())
	}
}

// TestLiveDeleteSnapshotIsIdempotent: the second delete must succeed too, or a
// retried DeleteSnapshot wedges the VolumeSnapshotContent in Terminating.
func TestLiveDeleteSnapshotIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "snap-del-idem-src", gib)
	snapshot := createSnapshot(ctx, t, h, volume, h.SnapshotName("snap-del-idem"))

	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := h.Ctrl().DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{
			SnapshotId: snapshot.GetSnapshotId(),
			Secrets:    h.Secrets(),
		}); err != nil {
			t.Fatalf("attempt %d of DeleteSnapshot: %v -- the second must succeed too", attempt, err)
		}
	}
}

// TestLiveDeleteSnapshotOnAMalformedHandle covers the two answers a bad
// handle gets, which are different for a reason.
//
// An absent ID is a malformed *request*: the CO left out a required field, and
// InvalidArgument tells it so. An ID that is present but unparseable names
// nothing this driver could have issued, so there is nothing of its to delete
// -- and "nothing to delete" is what DeleteSnapshot reports as success, the
// same reading DeleteVolume applies to an unparseable volume ID and
// TestDeleteSnapshotIsIdempotent pins in the unit suite.
//
// This test demanded InvalidArgument for all three until the 2026-09-20 round
// (F5). Returning it for the unparseable cases would wedge a
// VolumeSnapshotContent in Terminating over a handle no retry can fix.
func TestLiveDeleteSnapshotOnAMalformedHandle(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tests := []struct {
		name     string
		handle   string
		wantCode codes.Code
	}{
		{name: "empty", handle: "", wantCode: codes.InvalidArgument},
		{name: "no separator", handle: "no-separator", wantCode: codes.OK},
		// This driver's shape, unreadable: refused, not reported deleted.
		{name: "no snapshot ID", handle: h.SnapshotHandle(""), wantCode: codes.FailedPrecondition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := h.Ctrl().DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{
				SnapshotId: tt.handle,
				Secrets:    h.Secrets(),
			})
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("DeleteSnapshot(%q) returned %v (%v), want %v",
					tt.handle, got, err, tt.wantCode)
			}
		})
	}
}
