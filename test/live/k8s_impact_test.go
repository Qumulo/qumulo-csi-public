//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package live

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/replication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// Kubernetes-facing safety checks on real arrays: DeleteVolume handed a
// handle that names something other than its volume, a handle in a form this
// build does not read, and a create stopped before its quota.
//
// Each test works in a directory of its own below QUMULO_STORE_REAL_PATH,
// created first and removed last, so a regression can reach nothing but that
// directory.

// privateStore creates <array's storeRealPath>/<sub> on each array and
// registers its removal, which runs after every other cleanup the test
// registers later.
func privateStore(ctx context.Context, t *testing.T, parent, sub string, arrays ...*qumulo.Client) string {
	t.Helper()
	parent = strings.TrimSuffix(parent, "/")
	dir := parent + "/" + sub
	if !strings.Contains(dir, safetyMarker) {
		t.Fatalf("refusing to use %q: it is outside %q", dir, safetyMarker)
	}
	for _, array := range arrays {
		created, err := array.CreateDirectory(ctx, parent, sub)
		if err != nil {
			t.Fatalf("creating %s: %v", dir, err)
		}
		id := created.ID
		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := array.StartTreeDelete(cctx, id); err != nil && !qumulo.IsNotFound(err) {
				t.Logf("cleanup: tree-deleting %s (%s): %v -- it may need removing by hand", dir, id, err)
			}
		})
	}
	return dir
}

func requireExists(ctx context.Context, t *testing.T, array *qumulo.Client, p, when string) {
	t.Helper()
	if _, err := array.LookupFileByPath(ctx, p); err != nil {
		t.Fatalf("%s is gone %s: %v", p, when, err)
	}
}

func requireCodeLive(t *testing.T, err error, want codes.Code, what string) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("%s answered %v, want %s", what, err, want)
	}
	t.Logf("PROVED: %s refused: %v", what, err)
}

// TestLiveK8sImpactDeleteRefusesWhatIsNotItsVolume: edited, retired-form and
// store-naming handles are refused, and an interrupted create is finished.
func TestLiveK8sImpactDeleteRefusesWhatIsNotItsVolume(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	sub := fmt.Sprintf("csi-live-k8sfix-%d", time.Now().UnixNano())
	store := privateStore(ctx, t, h.StoreRealPath, sub, h.Client)
	h.StoreRealPath = store
	h.StoreExportPath = strings.TrimSuffix(h.StoreExportPath, "/") + "/" + sub

	create := func(name string) (volumeID, fileID, realPath string) {
		t.Helper()
		resp, err := h.Ctrl().CreateVolume(ctx, h.CreateRequest(name, gib))
		if err != nil {
			t.Fatalf("CreateVolume(%s): %v", name, err)
		}
		volumeID = resp.GetVolume().GetVolumeId()
		fileID = h.FileIDOf(t, volumeID)
		realPath = store + "/" + name
		h.Track(fileID, realPath)
		return volumeID, fileID, realPath
	}
	nameA, nameB := h.VolumeName("a"), h.VolumeName("b")
	idA, fileA, pathA := create(nameA)
	_, _, pathB := create(nameB)
	del := func(handle string) error {
		_, err := h.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: handle, Secrets: h.Secrets()})
		return err
	}
	bothThere := func(when string) {
		t.Helper()
		requireExists(ctx, t, h.Client, pathA, when)
		requireExists(ctx, t, h.Client, pathB, when)
	}

	// The edited handles of a sibling and a parent, and the dot form.
	requireCodeLive(t, del(h.VolumeHandle(pathA+"/../"+nameB)), codes.InvalidArgument, "sibling handle")
	bothThere("after the sibling handle")
	requireCodeLive(t, del(h.VolumeHandle(pathA+"/..")), codes.InvalidArgument, "parent handle")
	bothThere("after the parent handle")
	requireCodeLive(t, del(h.VolumeHandle(store+"/.")), codes.InvalidArgument, "dot handle")
	bothThere("after the dot handle")
	// A clean handle naming the directory that holds both volumes.
	requireCodeLive(t, del(h.VolumeHandle(store)), codes.FailedPrecondition, "clean handle naming the store")
	bothThere("after the clean store handle")

	// The retired <server>/<fileID>/<name> form.
	requireCodeLive(t, del(h.NFSServer+"/"+fileA+"/"+nameA), codes.FailedPrecondition, "retired-form handle")
	requireExists(ctx, t, h.Client, pathA, "after the retired-form handle")

	// An empty directory with no quota at the volume's path -- what a
	// controller stopped between the directory and the quota leaves.
	nameC := h.VolumeName("c")
	stub, err := h.Client.CreateDirectory(ctx, store, nameC)
	if err != nil {
		t.Fatalf("creating the interrupted create's directory: %v", err)
	}
	h.Track(stub.ID, store+"/"+nameC)
	resp, err := h.Ctrl().CreateVolume(ctx, h.CreateRequest(nameC, gib))
	if err != nil {
		t.Fatalf("CreateVolume over an interrupted create's directory: %v", err)
	}
	limit, err := h.Client.GetQuota(ctx, stub.ID)
	if err != nil || limit != uint64(gib) {
		t.Fatalf("the adopted directory's quota is %d (%v), want %d", limit, err, gib)
	}
	t.Logf("PROVED: the retry finished the interrupted create (%s, quota %d)", resp.GetVolume().GetVolumeId(), limit)

	// And an ordinary delete still deletes.
	if err := del(idA); err != nil {
		t.Fatalf("deleting volume A by its own handle: %v", err)
	}
	waitFor(ctx, t, "volume A's directory to go", time.Second, func() (bool, error) {
		_, err := h.Client.LookupFileByPath(ctx, pathA)
		return qumulo.IsNotFound(err), nil
	})
	requireExists(ctx, t, h.Client, pathB, "after volume A was deleted")
	t.Logf("PROVED: volume A deleted by its own handle; B untouched")
}

// TestLiveK8sImpactDeleteRefusesADirectoryHoldingAReplicatedVolume checks that
// a handle naming the directory that holds a protected source, on the local
// array, or its replica, on the peer, is refused, and the relationship stays.
func TestLiveK8sImpactDeleteRefusesADirectoryHoldingAReplicatedVolume(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	sub := fmt.Sprintf("csi-live-k8sfix-%d", time.Now().UnixNano())
	private := privateStore(ctx, t, r.StoreRealPath, sub, r.Client, r.peer)
	r.StoreRealPath = private
	r.StoreExportPath = strings.TrimSuffix(r.StoreExportPath, "/") + "/" + sub

	volume := r.protect(ctx, t, "d61")

	_, err := r.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: r.VolumeHandle(private), Secrets: r.replSecrets(),
	})
	requireCodeLive(t, err, codes.FailedPrecondition, "the source's parent on the local array")
	if !strings.Contains(err.Error(), volume.relationshipID) || !strings.Contains(err.Error(), "the protected source") {
		t.Errorf("the refusal does not name relationship %s and the protected source: %v", volume.relationshipID, err)
	}

	peerHost := hostOf(t, r.peerAPIURL)
	_, err = r.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		// A DR-side class's Secret carries the DR array's token as "token".
		VolumeId: peerHost + "#" + r.peerAPIURL + "#" + private, Secrets: map[string]string{"token": r.peerToken},
	})
	requireCodeLive(t, err, codes.FailedPrecondition, "the replica's parent on the peer")
	if !strings.Contains(err.Error(), "the replica") {
		t.Errorf("the refusal does not say it holds the replica: %v", err)
	}

	assertStillProtected(ctx, t, r, volume, "after the refused deletes of its parents")
	t.Logf("PROVED: both parents refused; relationship %s still established, replica intact", volume.relationshipID)
}

// TestLiveK8sImpactDeleteSnapshotIsScopedToThisDriversVolumes checks that a
// snapshot of a directory that is not one of this driver's volumes, or one
// not named as csi-snapshotter names this driver's, is not deleted without
// the deletion Secret's consent; one of a volume, so named, is.
func TestLiveK8sImpactDeleteSnapshotIsScopedToThisDriversVolumes(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	sub := fmt.Sprintf("csi-live-k8sfix-%d", time.Now().UnixNano())
	store := privateStore(ctx, t, h.StoreRealPath, sub, h.Client)
	h.StoreRealPath = store
	h.StoreExportPath = strings.TrimSuffix(h.StoreExportPath, "/") + "/" + sub

	snapshotOf := func(fileID, name string) string {
		t.Helper()
		snap, err := h.Client.CreateSnapshot(ctx, fileID, name)
		if err != nil {
			t.Fatalf("taking snapshot %s: %v", name, err)
		}
		t.Cleanup(func() {
			cctx, ccancel := context.WithTimeout(context.Background(), time.Minute)
			defer ccancel()
			if err := h.Client.DeleteSnapshot(cctx, snap.ID); err != nil && !qumulo.IsNotFound(err) {
				t.Logf("cleanup: deleting snapshot %s: %v", snap.ID, err)
			}
		})
		return h.SnapshotHandle(snap.ID)
	}
	del := func(handle string, secrets map[string]string) error {
		_, err := h.Ctrl().DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: handle, Secrets: secrets})
		return err
	}

	// A plain directory, made by the test, not by the driver.
	plain, err := h.Client.CreateDirectory(ctx, store, "not-a-volume")
	if err != nil {
		t.Fatalf("creating a plain directory: %v", err)
	}
	// Named as the driver names its snapshots, so only the source refuses it.
	foreign := snapshotOf(plain.ID, "snapshot-csi-live-k8sfix-foreign")
	requireCodeLive(t, del(foreign, h.Secrets()), codes.FailedPrecondition, "delete of a snapshot of a non-volume")
	consent := h.Secrets()
	consent["allowForeignSnapshotDelete"] = "true"
	if err := del(foreign, consent); err != nil {
		t.Fatalf("delete with the deletion Secret's consent: %v", err)
	}
	t.Logf("PROVED: the same snapshot deletes with allowForeignSnapshotDelete")

	resp, err := h.Ctrl().CreateVolume(ctx, h.CreateRequest(h.VolumeName("snap"), gib))
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	fileID := h.FileIDOf(t, resp.GetVolume().GetVolumeId())
	h.Track(fileID, store+"/"+volumeNameFromID(t, resp.GetVolume().GetVolumeId()))
	// A volume carries a quota, which is all the source check asks of a
	// directory without its own export -- so an administrator's snapshot of
	// it is told apart by its name.
	requireCodeLive(t, del(snapshotOf(fileID, "csi-live-k8sfix-admin"), h.Secrets()), codes.FailedPrecondition,
		"delete of a snapshot of a volume not named as this driver's")
	t.Logf("PROVED: a snapshot of a quota'd directory with a foreign name is refused")
	if err := del(snapshotOf(fileID, "snapshot-csi-live-k8sfix-volume"), h.Secrets()); err != nil {
		t.Fatalf("delete of a snapshot of one of this driver's volumes: %v", err)
	}
	t.Logf("PROVED: a snapshot of the driver's own volume deletes as before")
}

// TestLiveK8sImpactInfoFencesTheDriversOwnReadWriteReplicaExport checks that a
// replica export this driver made that reads read-write is set read-only by
// the next source-side status poll.
func TestLiveK8sImpactInfoFencesTheDriversOwnReadWriteReplicaExport(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	sub := fmt.Sprintf("csi-live-k8sfix-%d", time.Now().UnixNano())
	private := privateStore(ctx, t, r.StoreRealPath, sub, r.Client, r.peer)
	r.StoreRealPath = private
	r.StoreExportPath = strings.TrimSuffix(r.StoreExportPath, "/") + "/" + sub

	volume := r.protect(ctx, t, "d76")
	requireExportReadOnly(ctx, t, r.peer, volume.RealPath, true)

	// What an earlier build left: the driver's own replica export, read-write.
	exports, err := r.peer.ListExports(ctx)
	if err != nil {
		t.Fatalf("listing the peer's exports: %v", err)
	}
	for _, e := range exports {
		if e.FSPath == volume.RealPath {
			if err := r.peer.SetExportReadOnly(ctx, e.ID, false); err != nil {
				t.Fatalf("making the replica export read-write: %v", err)
			}
		}
	}
	requireExportReadOnly(ctx, t, r.peer, volume.RealPath, false)

	if _, err := r.repl.GetVolumeReplicationInfo(ctx, &replication.GetVolumeReplicationInfoRequest{
		VolumeId: volume.VolumeID, Secrets: r.replSecrets(),
	}); err != nil {
		t.Fatalf("GetVolumeReplicationInfo: %v", err)
	}
	requireExportReadOnly(ctx, t, r.peer, volume.RealPath, true)
	t.Logf("PROVED: one status poll set the driver's read-write replica export read-only")
}

// TestLiveK8sImpactCreateSnapshotRefusesTheReplica checks that a
// snapshot through a handle naming the read-only replica is refused; the source's is
// taken.
func TestLiveK8sImpactCreateSnapshotRefusesTheReplica(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	sub := fmt.Sprintf("csi-live-k8sfix-%d", time.Now().UnixNano())
	private := privateStore(ctx, t, r.StoreRealPath, sub, r.Client, r.peer)
	r.StoreRealPath = private
	r.StoreExportPath = strings.TrimSuffix(r.StoreExportPath, "/") + "/" + sub
	volume := r.protect(ctx, t, "d117")

	peerHost := hostOf(t, r.peerAPIURL)
	_, err := r.Ctrl().CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: peerHost + "#" + r.peerAPIURL + "#" + volume.RealPath,
		Name:           "csi-live-k8sfix-replica", Secrets: map[string]string{"token": r.peerToken},
	})
	requireCodeLive(t, err, codes.FailedPrecondition, "a snapshot of the replica")

	snap, err := r.Ctrl().CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: volume.VolumeID, Name: "snapshot-csi-live-k8sfix-source", Secrets: map[string]string{"token": r.Token},
	})
	if err != nil {
		t.Fatalf("a snapshot of the source: %v", err)
	}
	if _, err := r.Ctrl().DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{
		SnapshotId: snap.GetSnapshot().GetSnapshotId(), Secrets: map[string]string{"token": r.Token},
	}); err != nil {
		t.Logf("cleanup: deleting the source snapshot: %v", err)
	}
	t.Logf("PROVED: the source's snapshot is taken as before")
}
