//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package live

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/replication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Endpoint pinning after a failover.
//
// A volume handle carries the REST endpoint of the array that created it
// (pkg/driver/volume_id.go). When a StorageClass sets apiUrl explicitly and
// `server` is a floating name that moves to the DR array at failover, the
// PV keeps that handle: every controller RPC on it still dials the OLD
// source. This test reproduces exactly that state without DNS -- the
// original handle names the source array, the peer has been promoted -- and
// records what expand, snapshot and delete do through it.
//
// The safe outcome is a refusal. The unsafe one is an RPC that acts on the
// frozen, demoted source: a snapshot of stale data, or a quota changed on
// the copy nobody writes to any more.
func TestLiveReplicationOldHandleAfterFailoverIsRefused(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	volume := r.protect(ctx, t, "pin")

	dest, err := r.repl.ResolveReplicationDestination(ctx, volume.VolumeID, "", r.classTowardPeer(), r.replSecrets())
	if err != nil {
		t.Fatalf("ResolveReplicationDestination: %v", err)
	}
	waitFor(ctx, t, "a planned promote of the replica", 2*time.Second, func() (bool, error) {
		_, err := r.repl.PromoteVolume(ctx, &replication.PromoteVolumeRequest{
			VolumeId: dest.VolumeID, Parameters: r.classTowardLocal(), Secrets: r.replSecrets(),
		})
		if status.Code(err) == codes.Aborted {
			return false, nil
		}
		return true, err
	})
	t.Logf("failed over: replica %s promoted; the original handle %s still names the source", dest.VolumeID, volume.VolumeID)

	srcQuota := quotaOn(ctx, t, r.Client, "source", volume.FileID)
	peerQuota := quotaOn(ctx, t, r.peer, "peer", volume.replicaFileID)

	// Expand through the old handle.
	err = r.expand(ctx, volume.VolumeID, int64(srcQuota)*2, r.replSecrets())
	t.Logf("RESULT expand via old handle: code=%s err=%v", status.Code(err), err)
	srcAfter := quotaOn(ctx, t, r.Client, "source", volume.FileID)
	peerAfter := quotaOn(ctx, t, r.peer, "peer", volume.replicaFileID)
	t.Logf("RESULT quotas: source %d -> %d, promoted peer %d -> %d", srcQuota, srcAfter, peerQuota, peerAfter)
	if err == nil {
		t.Errorf("expand through the old handle succeeded after a failover (source %d -> %d, promoted peer %d -> %d); it should be refused",
			srcQuota, srcAfter, peerQuota, peerAfter)
	}
	if srcAfter != srcQuota && peerAfter == peerQuota {
		t.Errorf("expand changed the demoted source's quota only; the copy in use was not grown")
	}

	// Snapshot through the old handle.
	snap, err := r.Ctrl().CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: r.VolumeName("pinsnap"), SourceVolumeId: volume.VolumeID, Secrets: r.replSecrets(),
	})
	t.Logf("RESULT snapshot via old handle: code=%s err=%v", status.Code(err), err)
	if err == nil {
		id := snap.GetSnapshot().GetSnapshotId()
		t.Errorf("a snapshot through the old handle succeeded after a failover (%s): it is of the demoted source, not the live copy", id)
		if _, derr := r.Ctrl().DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: id, Secrets: r.replSecrets()}); derr != nil {
			t.Logf("cleanup: deleting snapshot %s: %v -- remove it by hand", id, derr)
		}
	}

	// Delete through the old handle.
	_, err = r.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volume.VolumeID, Secrets: r.replSecrets()})
	t.Logf("RESULT delete via old handle: code=%s err=%v", status.Code(err), err)
	if err == nil {
		t.Errorf("delete through the old handle succeeded after a failover; it must be refused while a relationship exists")
	}
}
