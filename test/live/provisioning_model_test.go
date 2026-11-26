//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// The two provisioning models against a real export table.
//
// These exist rather than being left to the unit suite because the thing they
// test is a property of the cluster, not of the driver. The NFSv4 ancestor
// rule -- a cluster with NFSv4 enabled refuses an export nested inside an
// existing one -- lives in the array's export validation, and
// cmd/mock-qumulo-server does not implement it. So the shape of the failure
// the dedicated model hits in the field is precisely the shape no mock can
// produce, and TestDedicatedExportLifecycleAgainstRealHTTPClient passes
// against a fake that would accept anything.
//
// Mutating: these create and delete real NFS exports, which is cluster
// configuration rather than data. Gated on QUMULO_LIVE_MUTATE_CONFIG=1 on top
// of the usual QUMULO_LIVE_TEST, the same second gate live-discovery-mutate
// carried.
package live

import (
	"context"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// requireMutateConfig skips unless the caller has opted into changing cluster
// configuration, not just cluster data.
//
// An export is visible to everything on the array, so a stray run here is a
// different order of mistake from leaving a directory behind.
func requireMutateConfig(t *testing.T) {
	t.Helper()

	if os.Getenv("QUMULO_LIVE_MUTATE_CONFIG") != "1" {
		t.Skip("this test creates and deletes real NFS exports; set QUMULO_LIVE_MUTATE_CONFIG=1 " +
			"(the live-dedicated-export target does) to allow it")
	}
}

// dedicatedParams is the harness's StorageClass switched to the per-volume
// export model.
func dedicatedParams(h *harness) map[string]string {
	params := h.Params()
	params["provisioningModel"] = "dedicated-export"
	// The harness's exports were always open; say so.
	params["allowAnyClient"] = "true"
	return params
}

// exportForPath returns the export whose filesystem path is exactly realPath.
func exportForPath(ctx context.Context, t *testing.T, h *harness, realPath string) *qumulo.ExportInfo {
	t.Helper()

	exports, err := h.Client.ListExports(ctx)
	if err != nil {
		t.Fatalf("listing exports: %v", err)
	}
	for i := range exports {
		if strings.TrimSuffix(exports[i].FSPath, "/") == strings.TrimSuffix(realPath, "/") {
			return &exports[i]
		}
	}
	return nil
}

// TestLiveDedicatedExportCreatesAndRemovesAnExport is the model's whole
// contract: one export per volume, and it goes when the volume goes.
//
// The delete half matters more than the create half. An export left behind
// after DeleteVolume is invisible from Kubernetes and consumes an entry in the
// array's export table, which is finite -- so a leak here is a slow outage
// nobody attributes to the CSI driver.
func TestLiveDedicatedExportCreatesAndRemovesAnExport(t *testing.T) {
	requireMutateConfig(t)
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	name := h.VolumeName("dedicated")
	req := h.CreateRequest(name, gib)
	req.Parameters = dedicatedParams(h)

	resp, err := h.Ctrl().CreateVolume(ctx, req)
	if err != nil {
		// The ancestor rule is a cluster property, not a driver fault: if the
		// store root is itself exported on an NFSv4-enabled cluster, the
		// dedicated model cannot work there and the driver says so. That is
		// correct behaviour and a skip, not a failure.
		if status.Code(err) == codes.FailedPrecondition {
			t.Skipf("this cluster cannot host a dedicated export under %s: %v -- "+
				"see the NFSv4 ancestor rule in docs/provision/troubleshoot.md", h.StoreRealPath, err)
		}
		t.Fatalf("CreateVolume with provisioningModel=dedicated-export: %v", err)
	}

	volume := resp.GetVolume()
	fileID := h.FileIDOf(t, volume.GetVolumeId())
	realPath := path.Join(h.StoreRealPath, name)
	h.Track(fileID, realPath)

	export := exportForPath(ctx, t, h, realPath)
	if export == nil {
		t.Fatalf("no export on the cluster serves %s; the dedicated model created a volume "+
			"that nothing can mount", realPath)
	}
	t.Logf("export %s serves %s at %s", export.ID, export.FSPath, export.ExportPath)

	// The node mounts the export path from VolumeContext, so it has to be the
	// export that actually exists -- the two drifting apart is exactly the
	// class of bug the coverage check exists for.
	// The key is spelled out rather than imported: volumeContextExportPath is
	// unexported in package driver, and asserting on the literal is the point
	// -- it is what the node reads out of the PV.
	if got := volume.GetVolumeContext()["exportPath"]; got != export.ExportPath {
		t.Errorf("VolumeContext exportPath is %q but the export serves %q", got, export.ExportPath)
	}

	if _, err := h.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: volume.GetVolumeId(),
		Secrets:  h.Secrets(),
	}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	if leaked := exportForPath(ctx, t, h, realPath); leaked != nil {
		t.Errorf("export %s for %s outlived its volume; the export table now holds an entry "+
			"no Kubernetes object accounts for", leaked.ID, realPath)
	}
}

// TestLiveSharedSubdirectoryCreatesNoExport is the other model's contract, and
// the reason the coverage check exists.
//
// The shared model deliberately creates nothing in the export table: it relies
// on an administrator's export covering the store root. Creating one here
// would be a cluster-wide grant the driver was never asked to make.
func TestLiveSharedSubdirectoryCreatesNoExport(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	before, err := h.Client.ListExports(ctx)
	if err != nil {
		t.Fatalf("listing exports before: %v", err)
	}

	volume := provision(ctx, t, h, "shared", gib)

	after, err := h.Client.ListExports(ctx)
	if err != nil {
		t.Fatalf("listing exports after: %v", err)
	}
	if len(after) != len(before) {
		t.Errorf("the export table went from %d to %d entries; the shared model must add none",
			len(before), len(after))
	}
	if own := exportForPath(ctx, t, h, volume.RealPath); own != nil {
		t.Errorf("the shared model created export %s for %s", own.ID, volume.RealPath)
	}
}

// TestLiveSharedSubdirectoryRefusesAnUncoveredPath is the coverage check's
// acceptance test, executed against a real cluster.
//
// The reported defect: for the shared model the driver created the directory
// and quota but never checked that any export reached the path, so CreateVolume
// succeeded, the PVC went Bound, and the pod then failed forever with ENOENT.
// The operator saw a healthy volume and a broken workload with nothing
// connecting the two.
//
// Two assertions, and the second is the one that was missing: the call is
// refused, *and* nothing was created on the array before it was refused.
func TestLiveSharedSubdirectoryRefusesAnUncoveredPath(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// A path under the store root that no export can plausibly cover. Kept
	// inside the safety marker so that if the refusal ever regresses and
	// something *is* created, it lands somewhere the cleanup guards allow.
	uncovered := path.Join(h.StoreExportPath, "csi-live-test-no-export-here")

	name := h.VolumeName("uncovered")
	req := h.CreateRequest(name, gib)
	req.Parameters["storeExportPath"] = uncovered

	_, err := h.Ctrl().CreateVolume(ctx, req)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateVolume on a path no export covers returned %v, want FailedPrecondition", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "export") {
		t.Errorf("the message must tell the operator an export is missing; got %q", msg)
	}

	// Nothing left behind. A refusal that still creates the directory leaves
	// the array holding volumes Kubernetes never knew about.
	if _, err := h.Client.LookupFileByPath(ctx, path.Join(h.StoreRealPath, name)); err == nil {
		t.Errorf("the refused volume left a directory at %s", path.Join(h.StoreRealPath, name))
	}
}

// TestLiveVolumeDirectoryIsWritableMode checks the permission a pod actually
// lands on.
//
// mountPermissions defaults to 0777 for a specific reason: the cluster creates
// a directory owned by the API user at 0755, and an NFS-mapped pod running as
// some other uid cannot write to that. A default that regressed to 0755 would
// not fail any RPC -- it would fail every workload, at first write.
func TestLiveVolumeDirectoryIsWritableMode(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "mode", gib)

	entries, err := h.Client.ListDirectory(ctx, h.StoreRealPath, "")
	if err != nil {
		t.Fatalf("listing %s: %v", h.StoreRealPath, err)
	}

	for _, entry := range entries {
		if entry.Name != volume.Name {
			continue
		}
		// The cluster reports the mode as an octal string. 0777 is the
		// documented default; anything more restrictive is the regression.
		if !strings.HasSuffix(entry.Mode, "777") {
			t.Errorf("volume directory %s has mode %q, want the documented 0777 default -- "+
				"a non-root pod cannot write to a directory the API user owns at 0755",
				volume.RealPath, entry.Mode)
		}
		return
	}
	t.Fatalf("the volume directory %s is not in a listing of %s", volume.Name, h.StoreRealPath)
}
