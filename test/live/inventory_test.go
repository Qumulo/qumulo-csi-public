//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package live

import (
	"context"
	"path"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/driver"
)

// fakeClassLister serves one synthetic StorageClass pointing at the real
// cluster.
//
// The Kubernetes half is covered by unit tests and by the minikube target,
// where a real API server and real StorageClasses exist. What cannot be faked
// is the array's answers -- an unordered directory listing, a quota that is
// there or is not -- so this stands in for the API and lets the Qumulo half run
// for real.
type fakeClassLister struct {
	classes []driver.StorageClassInfo
	calls   int
}

func (f *fakeClassLister) ListStorageClasses(
	_ context.Context,
	_ string,
) ([]driver.StorageClassInfo, error) {
	f.calls++
	return f.classes, nil
}

// inventoryHarness returns a controller whose ListVolumes can reach the array.
func inventoryHarness(t *testing.T) (*harness, csi.ControllerServer, *fakeClassLister) {
	t.Helper()

	h := newHarness(t)

	params := h.Params()
	params["csi.storage.k8s.io/provisioner-secret-name"] = "qumulo-api-token"
	params["csi.storage.k8s.io/provisioner-secret-namespace"] = "qumulo-csi"

	lister := &fakeClassLister{classes: []driver.StorageClassInfo{
		{Name: "live-class", Parameters: params},
	}}

	d, err := driver.NewDriver(driver.Config{
		Name:               "csi.qumulo.com",
		Version:            "live-test",
		HTTPClient:         h.Client.HTTPClient,
		SecretReader:       &fileSecretReader{token: h.Token},
		StorageClassLister: lister,
	})
	if err != nil {
		t.Fatalf("building the driver: %v", err)
	}

	return h, d.ControllerServer(), lister
}

// createVolume provisions through the driver's own CreateVolume and registers
// the result for cleanup, returning the volume ID.
//
// Provisioning goes through h.Ctrl() -- the harness's controller, which has
// secrets on every request -- while the assertions use the inventory
// controller. That split is the point: a volume created the ordinary way must
// be visible to a listing that was given no request-scoped credential at all.
func (h *harness) createVolume(t *testing.T, prefix string, capacity int64) string {
	t.Helper()

	name := h.VolumeName(prefix)
	resp, err := h.Ctrl().CreateVolume(context.Background(), h.CreateRequest(name, capacity))
	if err != nil {
		t.Fatalf("CreateVolume(%s): %v", name, err)
	}

	id := resp.GetVolume().GetVolumeId()
	h.Track(h.FileIDOf(t, id), path.Join(h.StoreRealPath, name))
	return id
}

// deleteVolume removes a volume through DeleteVolume, which is what a listing
// must then stop reporting.
func (h *harness) deleteVolume(t *testing.T, id string) {
	t.Helper()

	_, err := h.Ctrl().DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: id,
		Secrets:  h.Secrets(),
	})
	if err != nil {
		t.Fatalf("DeleteVolume(%s): %v", id, err)
	}
}

// TestLiveListVolumesFindsWhatCreateVolumeMade is the end-to-end check, and it
// is the one csi-sanity cannot run because it has no Kubernetes.
//
// Mutating: it provisions a volume under QUMULO_STORE_REAL_PATH and deletes it
// again. The harness refuses to run unless that path carries the safety marker.
func TestLiveListVolumesFindsWhatCreateVolumeMade(t *testing.T) {
	h, ctrl, lister := inventoryHarness(t)
	ctx := context.Background()

	before, err := ctrl.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes against the live cluster: %v", err)
	}
	baseline := len(before.GetEntries())
	t.Logf("MEASURED baseline listing under %q: %d volume(s)", h.StoreRealPath, baseline)

	created := h.createVolume(t, "inv", gib)

	after, err := ctrl.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes after creating a volume: %v", err)
	}
	if got := len(after.GetEntries()); got != baseline+1 {
		t.Errorf("listing has %d entries after creating one volume, want %d", got, baseline+1)
	}

	var found *csi.Volume
	for _, entry := range after.GetEntries() {
		if entry.GetVolume().GetVolumeId() == created {
			found = entry.GetVolume()
		}
	}
	if found == nil {
		ids := make([]string, 0, len(after.GetEntries()))
		for _, e := range after.GetEntries() {
			ids = append(ids, e.GetVolume().GetVolumeId())
		}
		t.Fatalf("the volume just created (%s) is not in the listing; got: %s",
			created, strings.Join(ids, ", "))
	}
	if found.GetCapacityBytes() != gib {
		t.Errorf("listed capacity is %d, want %d -- the quota is what the listing reads",
			found.GetCapacityBytes(), gib)
	}
	t.Logf("MEASURED the new volume appears with its quota: %s = %d bytes",
		found.GetVolumeId(), found.GetCapacityBytes())

	h.deleteVolume(t, created)

	final, err := ctrl.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes after deleting the volume: %v", err)
	}
	if got := len(final.GetEntries()); got != baseline {
		t.Errorf("listing has %d entries after deleting the volume, want the baseline %d",
			got, baseline)
	}
	for _, entry := range final.GetEntries() {
		if entry.GetVolume().GetVolumeId() == created {
			t.Errorf("the deleted volume %s is still listed", created)
		}
	}

	if lister.calls == 0 {
		t.Error("the StorageClass lister was never consulted")
	}
	t.Logf("MEASURED listing returned to the baseline after deletion; "+
		"StorageClasses were listed %d time(s)", lister.calls)
}

// TestLiveListVolumesSkipsADirectoryWithNoQuota measures the ownership rule
// against the array.
//
// A bare directory under the store path is not a volume: the driver did not
// provision it, or a provision failed before setting a quota. This is the one
// place that rule can be checked against a real cluster rather than a fake,
// and it matters because reporting such a directory invites a reconciler to
// delete it.
func TestLiveListVolumesSkipsADirectoryWithNoQuota(t *testing.T) {
	h, ctrl, _ := inventoryHarness(t)
	ctx := context.Background()

	before, err := ctrl.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes: %v", err)
	}
	baseline := len(before.GetEntries())

	// Created through the cluster API directly, so it has no quota -- exactly
	// what a provision that died after mkdir leaves behind.
	name := h.VolumeName("noquota")
	info, err := h.Client.CreateDirectory(ctx, h.StoreRealPath, name)
	if err != nil {
		t.Fatalf("creating a bare directory: %v", err)
	}
	t.Cleanup(func() {
		if err := h.Client.StartTreeDelete(context.Background(), info.ID); err != nil {
			t.Errorf("CLEANUP FAILED: remove %s/%s (file ID %s) by hand: %v",
				h.StoreRealPath, name, info.ID, err)
		}
	})

	after, err := ctrl.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes after creating a bare directory: %v", err)
	}
	if got := len(after.GetEntries()); got != baseline {
		t.Errorf("listing grew to %d after adding a directory with no quota, want %d; "+
			"a directory this driver did not provision must not be reported as a volume",
			got, baseline)
	}
	t.Logf("MEASURED a directory with no quota is skipped: listing stayed at %d", baseline)
}

// TestLiveListVolumesPagesThroughEveryVolume walks the listing one entry at a
// time against the array, whose directory listing promises no ordering.
func TestLiveListVolumesPagesThroughEveryVolume(t *testing.T) {
	h, ctrl, _ := inventoryHarness(t)
	ctx := context.Background()

	// Three volumes, so paging has boundaries to cross.
	want := map[string]bool{}
	for _, suffix := range []string{"page-a", "page-b", "page-c"} {
		id := h.createVolume(t, suffix, gib)
		want[id] = false
	}

	seen := map[string]int{}
	token := ""
	for pages := 0; ; pages++ {
		if pages > 200 {
			t.Fatal("paging did not terminate")
		}
		resp, err := ctrl.ListVolumes(ctx, &csi.ListVolumesRequest{
			MaxEntries:    1,
			StartingToken: token,
		})
		if err != nil {
			t.Fatalf("ListVolumes page %d: %v", pages, err)
		}
		for _, e := range resp.GetEntries() {
			seen[e.GetVolume().GetVolumeId()]++
		}
		token = resp.GetNextToken()
		if token == "" {
			break
		}
	}

	for id := range want {
		switch seen[id] {
		case 1:
		case 0:
			t.Errorf("volume %s was never returned across pages", id)
		default:
			t.Errorf("volume %s was returned %d times across pages", id, seen[id])
		}
	}
	t.Logf("MEASURED paged one entry at a time over %d volume(s), each exactly once", len(seen))
}

// TestLiveListVolumesInvalidTokenIsAborted is the paging contract: Aborted
// tells the CO to restart the listing.
func TestLiveListVolumesInvalidTokenIsAborted(t *testing.T) {
	_, ctrl, _ := inventoryHarness(t)

	_, err := ctrl.ListVolumes(context.Background(), &csi.ListVolumesRequest{
		StartingToken: "not-a-number",
	})
	if err == nil {
		t.Fatal("expected an error for an invalid starting token")
	}
	if status.Code(err) != codes.Aborted {
		t.Errorf("want Aborted, got %s: %v", status.Code(err), err)
	}
}

// TestLiveControllerGetVolumeReportsARealVolume resolves a volume ID to the
// array it names and reads its quota back.
func TestLiveControllerGetVolumeReportsARealVolume(t *testing.T) {
	h, ctrl, _ := inventoryHarness(t)
	ctx := context.Background()

	id := h.createVolume(t, "get", 2*gib)

	resp, err := ctrl.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: id})
	if err != nil {
		t.Fatalf("ControllerGetVolume: %v", err)
	}
	if resp.GetVolume().GetVolumeId() != id {
		t.Errorf("got volume ID %q, want %q", resp.GetVolume().GetVolumeId(), id)
	}
	if resp.GetVolume().GetCapacityBytes() != 2*gib {
		t.Errorf("got capacity %d, want %d", resp.GetVolume().GetCapacityBytes(), 2*gib)
	}
	if len(resp.GetStatus().GetPublishedNodeIds()) != 0 {
		t.Error("published_node_ids must be empty: there is no attach state to report")
	}
	t.Logf("MEASURED %s reports %d bytes", id, resp.GetVolume().GetCapacityBytes())

	// After deletion the array no longer has the quota, which is the one case
	// that genuinely is NotFound.
	h.deleteVolume(t, id)
	_, err = ctrl.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: id})
	if err == nil {
		t.Fatal("expected NotFound for a deleted volume")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("want NotFound for a deleted volume, got %s: %v", status.Code(err), err)
	}
	t.Logf("MEASURED a deleted volume reports: %v", err)
}

// TestLiveControllerGetVolumeWithNoRouteIsNotNotFound is the distinction that
// matters most in this RPC: a volume whose class is gone may still exist, so
// saying NotFound would tell a reconciler to clean up after something that is
// still there.
func TestLiveControllerGetVolumeWithNoRouteIsNotNotFound(t *testing.T) {
	_, ctrl, _ := inventoryHarness(t)

	_, err := ctrl.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{
		VolumeId: "no-such-cluster.example.com#/csi/volumes/pvc-x",
	})
	if err == nil {
		t.Fatal("expected an error for a volume on an unknown cluster")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("want FailedPrecondition, got %s: %v", status.Code(err), err)
	}
	t.Logf("MEASURED an unroutable volume reports: %v", err)
}
