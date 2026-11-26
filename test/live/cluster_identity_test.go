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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestLiveCreateSnapshotRefusesAnOverrideToAnotherCluster runs on two real
// arrays: a volume on the local array, and a
// VolumeSnapshotClass apiurl naming the peer, with the peer's token. The peer
// used to snapshot whatever it held at the same path. The two arrays' cluster
// UUIDs (GET /v1/node/state) now differ, so the snapshot is refused and
// nothing is created on the peer.
func TestLiveCreateSnapshotRefusesAnOverrideToAnotherCluster(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	local, err := r.Client.ClusterID(ctx)
	if err != nil {
		t.Fatalf("reading the local cluster's ID: %v", err)
	}
	peer, err := r.peer.ClusterID(ctx)
	if err != nil {
		t.Fatalf("reading the peer cluster's ID: %v", err)
	}
	if local == peer {
		t.Skipf("both endpoints are one cluster (%s): this test needs two", local)
	}

	volume := r.provisionDedicated(ctx, t, "d14")
	before, err := r.peer.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("listing the peer's snapshots: %v", err)
	}

	_, refused := r.Ctrl().CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           r.VolumeName("d14-snap"),
		SourceVolumeId: volume.VolumeID,
		Secrets:        map[string]string{"token": r.peerToken},
		Parameters:     map[string]string{"apiurl": r.peerAPIURL},
	})

	if status.Code(refused) != codes.InvalidArgument {
		t.Fatalf("CreateSnapshot through the peer's endpoint returned %v, want InvalidArgument", refused)
	}
	after, err := r.peer.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("listing the peer's snapshots: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("the peer has %d snapshots, %d before: one was taken", len(after), len(before))
	}
	t.Logf("PROVED: refused (%s vs %s): %v", local, peer, refused)
}
