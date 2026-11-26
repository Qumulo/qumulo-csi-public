//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// The volume lifecycle against a real cluster: create, expand, validate,
// delete.
//
// These are the suites the Makefile's live-createvolume, live-deletevolume and
// live-expandvolume targets have always named and never had. Each target ran
// `go test -run` against a pattern matching nothing, which exits 0, so all
// three reported a pass while running no assertion at all.
//
// What makes them worth having rather than duplicating the unit tests: every
// assertion here reads the cluster back through its own API rather than
// through the driver that just wrote it. A mock agrees with whatever the
// driver believes; a cluster does not. The cases that matter are the ones
// where the two could disagree -- a quota the cluster rounded, a directory
// the driver thinks it deleted, an expansion that returned success without
// changing anything.
package live

import (
	"context"
	"errors"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// TestLiveCreateVolumeMakesADirectoryAndQuota is the base case: what the
// driver reports and what the cluster holds have to agree.
func TestLiveCreateVolumeMakesADirectoryAndQuota(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	name := h.VolumeName("create")
	resp, err := h.Ctrl().CreateVolume(ctx, h.CreateRequest(name, gib))
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	volume := resp.GetVolume()
	if volume == nil {
		t.Fatal("CreateVolume returned no volume")
	}
	if got := volume.GetCapacityBytes(); got != gib {
		t.Errorf("CapacityBytes = %d, want %d", got, gib)
	}
	if volumeNameFromID(t, volume.GetVolumeId()) != name {
		t.Errorf("volume handle %q does not carry the requested name %q", volume.GetVolumeId(), name)
	}

	fileID := h.FileIDOf(t, volume.GetVolumeId())
	realPath := strings.TrimSuffix(h.StoreRealPath, "/") + "/" + name
	h.Track(fileID, realPath)

	// The directory exists, and the file ID in the handle is that directory's
	// -- not merely some ID the driver remembered.
	info, err := h.Client.LookupFileByPath(ctx, realPath)
	if err != nil {
		t.Fatalf("the cluster has no directory at %s after a successful CreateVolume: %v", realPath, err)
	}
	if info.ID != fileID {
		t.Errorf("the handle names file ID %s but %s is ID %s", fileID, realPath, info.ID)
	}

	// The quota is the capacity. A volume whose directory exists without one
	// is unbounded, which is the failure worth catching: nothing downstream
	// would notice until the cluster filled up.
	limit, err := h.Client.GetQuota(ctx, fileID)
	if err != nil {
		t.Fatalf("reading the quota on %s: %v -- a volume with no quota is unbounded", realPath, err)
	}
	if limit != uint64(gib) {
		t.Errorf("quota limit = %d, want %d", limit, gib)
	}
}

// TestLiveCreateVolumeIsIdempotent covers the retry the provisioner actually
// performs.
//
// csi-provisioner retries CreateVolume whenever it does not see a response --
// a timeout, a restart, a lost connection -- so the second call arrives for a
// volume that already exists. The spec requires the existing volume back, not
// AlreadyExists, and certainly not a second directory.
func TestLiveCreateVolumeIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	first := provision(ctx, t, h, "idem", gib)

	second, err := h.Ctrl().CreateVolume(ctx, h.CreateRequest(first.Name, gib))
	if err != nil {
		t.Fatalf("the retry of CreateVolume(%s) failed: %v", first.Name, err)
	}
	if got := second.GetVolume().GetVolumeId(); got != first.VolumeID {
		t.Errorf("retry returned handle %q, want the existing %q", got, first.VolumeID)
	}
}

// TestLiveCreateVolumeRefusesADifferentSizeForTheSameName is the other half of
// idempotency, and the half that protects data.
//
// Same name, different capacity, means the CO is asking for a volume that is
// not the one on the cluster. Returning the existing volume would silently
// give a workload a size it did not ask for.
func TestLiveCreateVolumeRefusesADifferentSizeForTheSameName(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	existing := provision(ctx, t, h, "conflict", gib)

	_, err := h.Ctrl().CreateVolume(ctx, h.CreateRequest(existing.Name, 2*gib))
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateVolume with a conflicting size returned %v, want AlreadyExists", err)
	}
}

// TestLiveCreateVolumeRejectsAnUnknownParameter proves the driver's parameter
// allowlist is enforced against a real request, not only in unit tests.
//
// This is the counterpart to the chart's closed values schema: a hand-written
// StorageClass never passes through Helm, so the driver is the only thing
// standing between a typo and a class that silently does something else.
func TestLiveCreateVolumeRejectsAnUnknownParameter(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	req := h.CreateRequest(h.VolumeName("badparam"), gib)
	req.Parameters["storeRealPth"] = h.StoreRealPath

	_, err := h.Ctrl().CreateVolume(ctx, req)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateVolume with an unknown parameter returned %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "storeRealPth") {
		t.Errorf("the error must name the offending parameter so an operator can fix it; got %q", msg)
	}
}

// TestLiveExpandVolumeGrowsTheQuota checks the resize path end to end.
//
// Expansion on this driver is a quota update and nothing else -- there is no
// filesystem to grow -- so the assertion that matters is the cluster's quota
// limit afterwards, read back through its own API.
func TestLiveExpandVolumeGrowsTheQuota(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "expand", gib)

	resp, err := h.Ctrl().ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      volume.VolumeID,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 3 * gib},
		Secrets:       h.Secrets(),
	})
	if err != nil {
		t.Fatalf("ControllerExpandVolume: %v", err)
	}
	if got := resp.GetCapacityBytes(); got != 3*gib {
		t.Errorf("CapacityBytes = %d, want %d", got, 3*gib)
	}
	// There is no node-side filesystem to resize, so claiming otherwise would
	// leave the PVC waiting on a NodeExpandVolume that never comes.
	if resp.GetNodeExpansionRequired() {
		t.Error("NodeExpansionRequired is true, but a quota change needs nothing from the node")
	}

	limit, err := h.Client.GetQuota(ctx, volume.FileID)
	if err != nil {
		t.Fatalf("reading the quota after expansion: %v", err)
	}
	if limit != uint64(3*gib) {
		t.Errorf("quota limit on the cluster = %d, want %d -- the RPC reported success "+
			"without changing anything", limit, 3*gib)
	}
}

// TestLiveExpandVolumeIsIdempotent: re-expanding to the current size is a
// no-op, not an error. csi-resizer retries, so this happens routinely.
func TestLiveExpandVolumeIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "expand-idem", 2*gib)

	for attempt := 1; attempt <= 2; attempt++ {
		resp, err := h.Ctrl().ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
			VolumeId:      volume.VolumeID,
			CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * gib},
			Secrets:       h.Secrets(),
		})
		if err != nil {
			t.Fatalf("attempt %d of expanding to the current size: %v", attempt, err)
		}
		if got := resp.GetCapacityBytes(); got < 2*gib {
			t.Errorf("attempt %d reported %d, which is below the volume's existing %d",
				attempt, got, 2*gib)
		}
	}
}

// TestLiveExpandVolumeNeverShrinksTheQuota guards the one direction that loses
// data: a quota set below current usage would make every subsequent write
// fail.
//
// The guarantee is about the quota, not the status code. CSI has no shrink,
// and the spec's answer for a volume already at or above the requested size is
// success -- so a request for less than the volume already has is reported OK
// and the quota is left alone. This test asserted InvalidArgument/OutOfRange
// until the 2026-09-20 round (F3), which contradicted both the spec and
// TestControllerExpandVolumeRejectsShrink in the unit suite; the quota
// assertion below is the part that was always the point, and it is unchanged.
func TestLiveExpandVolumeNeverShrinksTheQuota(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "shrink", 4*gib)

	resp, err := h.Ctrl().ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      volume.VolumeID,
		CapacityRange: &csi.CapacityRange{RequiredBytes: gib},
		Secrets:       h.Secrets(),
	})
	if err != nil {
		t.Fatalf("asking for less than the volume already has returned %v; the spec's "+
			"answer for a volume already large enough is success", err)
	}
	if got := resp.GetCapacityBytes(); got < 4*gib {
		t.Errorf("the RPC reported %d, below the volume's existing %d -- a CO that "+
			"believes this has a volume that shrank", got, 4*gib)
	}

	limit, err := h.Client.GetQuota(ctx, volume.FileID)
	if err != nil {
		t.Fatalf("reading the quota after the shrink request: %v", err)
	}
	if limit != uint64(4*gib) {
		t.Errorf("quota limit = %d after a shrink request, want the original %d -- "+
			"the RPC reported success and then reduced the quota anyway", limit, 4*gib)
	}
}

// TestLiveValidateVolumeCapabilitiesAcceptsWhatWasProvisioned.
//
// The RPC exists so a CO can check a pre-provisioned PV before binding it. An
// answer of "confirmed" for a capability the driver cannot serve is worse than
// no answer, because the PVC binds and the pod then fails to mount.
func TestLiveValidateVolumeCapabilitiesAcceptsWhatWasProvisioned(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "validate", gib)

	resp, err := h.Ctrl().ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId:           volume.VolumeID,
		VolumeCapabilities: h.CreateRequest(volume.Name, gib).GetVolumeCapabilities(),
		VolumeContext:      map[string]string{},
		Parameters:         h.Params(),
		Secrets:            h.Secrets(),
	})
	if err != nil {
		t.Fatalf("ValidateVolumeCapabilities: %v", err)
	}
	if resp.GetConfirmed() == nil {
		t.Fatalf("the driver would not confirm the capabilities it just provisioned with: %q",
			resp.GetMessage())
	}
}

// TestLiveValidateVolumeCapabilitiesRejectsBlockAccess.
//
// Block is the capability this driver can never serve: an NFS export is a
// filesystem. Confirming it would bind a PVC that cannot work.
func TestLiveValidateVolumeCapabilitiesRejectsBlockAccess(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "validate-block", gib)

	resp, err := h.Ctrl().ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: volume.VolumeID,
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}},
		Secrets: h.Secrets(),
	})
	// Either shape is correct per the spec: an error, or a response with
	// Confirmed unset and a message. What must not happen is a confirmation.
	if err == nil && resp.GetConfirmed() != nil {
		t.Fatal("block access was confirmed, but every volume this driver serves is an NFS mount")
	}
}

// TestLiveValidateVolumeCapabilitiesRejectsAnUnknownVolume: the handle names a
// file ID that is not there.
func TestLiveValidateVolumeCapabilitiesRejectsAnUnknownVolume(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err := h.Ctrl().ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId:           h.VolumeHandle(path.Join(h.StoreRealPath, "csi-live-does-not-exist")),
		VolumeCapabilities: h.CreateRequest("x", gib).GetVolumeCapabilities(),
		Secrets:            h.Secrets(),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ValidateVolumeCapabilities on a missing volume returned %v, want NotFound", err)
	}
}

// TestLiveDeleteVolumeRemovesTheDirectoryAndQuota.
//
// The assertion is against the cluster, after the RPC returns: a DeleteVolume
// that reports success while leaving the directory behind is how an array
// fills up with volumes Kubernetes believes are gone.
func TestLiveDeleteVolumeRemovesTheDirectoryAndQuota(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "delete", gib)

	if _, err := h.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: volume.VolumeID,
		Secrets:  h.Secrets(),
	}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	// Tree-delete is a job, not a synchronous call, so the directory goes away
	// shortly after the RPC returns rather than during it.
	waitUntilGone(ctx, t, h, volume.RealPath)

	if _, err := h.Client.GetQuota(ctx, volume.FileID); err == nil {
		t.Errorf("the quota on %s outlived its volume", volume.RealPath)
	}
}

// TestLiveDeleteVolumeIsIdempotent: deleting twice is not an error.
//
// Required by the spec, and routine in practice -- the provisioner retries a
// DeleteVolume whose response it never saw. A NotFound here would leave the PV
// stuck in Terminating for good.
func TestLiveDeleteVolumeIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "delete-idem", gib)

	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := h.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: volume.VolumeID,
			Secrets:  h.Secrets(),
		}); err != nil {
			t.Fatalf("attempt %d of DeleteVolume: %v -- the second must succeed too", attempt, err)
		}
	}
}

// TestLiveDeleteVolumeRefusesAMalformedHandle.
//
// A handle the driver did not issue must be refused before anything is
// deleted. The worry is not the error code but what a lenient parse would
// tree-delete if it guessed wrong.
// DeleteVolume's answer to a handle it could not have issued.
//
// Only an EMPTY volume ID is InvalidArgument; everything else unparseable is
// success. That is the CSI spec -- "if the volume does not exist, the plugin
// MUST reply OK" -- and a handle this driver never issued names no volume of
// its, so there is nothing to delete. csi-sanity pins the same behaviour in
// "DeleteVolume should succeed when an invalid volume id is used".
//
// This test previously asserted InvalidArgument for every case. It had never
// been run -- live-deletevolume was one of the twelve unexecuted targets -- and
// the first run showed it contradicting both the spec and csi-sanity. The
// driver is right; the assertion was not.
func TestLiveDeleteVolumeAnswersAMalformedHandle(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	t.Run("empty is InvalidArgument", func(t *testing.T) {
		_, err := h.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "", Secrets: h.Secrets()})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("DeleteVolume(\"\") returned %v, want InvalidArgument", err)
		}
	})

	// Not in this driver's shape at all: no volume of its, so already-absent,
	// which is success.
	t.Run("not-a-handle", func(t *testing.T) {
		if _, err := h.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "not-a-handle", Secrets: h.Secrets()}); err != nil {
			t.Fatalf("DeleteVolume(\"not-a-handle\") returned %v, want success", err)
		}
	})

	// In this driver's shape, <server>#<apiUrl>#<realPath>, but unreadable:
	// an empty path, a relative path, the root, and the retired
	// <server>/<fileID>/<name> form. Refused, not reported deleted -- the
	// volume a handle like that names may still be on the array.
	for _, handle := range []string{
		h.NFSServer + "##",
		h.NFSServer + "##relative/path",
		h.NFSServer + "##/",
		h.NFSServer + "/12884901888/retired-format",
	} {
		t.Run(handle, func(t *testing.T) {
			_, err := h.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: handle, Secrets: h.Secrets()})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("DeleteVolume(%q) returned %v, want FailedPrecondition", handle, err)
			}
		})
	}
}

// waitUntilGone polls the cluster until the path is absent, or fails.
//
// Tree-delete runs asynchronously on the array: DeleteVolume starts the job
// and does not block until every child is unlinked, so a test asserting
// removal has to wait rather than read once.
func waitUntilGone(ctx context.Context, t *testing.T, h *harness, realPath string) {
	t.Helper()

	const (
		budget = 4 * time.Minute
		every  = 5 * time.Second
	)

	deadline := time.Now().Add(budget)
	for {
		_, err := h.Client.LookupFileByPath(ctx, realPath)
		if err != nil {
			if qumulo.IsNotFound(err) {
				return
			}
			// Anything else is a real failure: the cluster answered, and not
			// with "gone".
			t.Fatalf("looking up %s while waiting for it to be deleted: %v", realPath, err)
		}

		if time.Now().After(deadline) {
			t.Fatalf("%s still exists %s after DeleteVolume returned success", realPath, budget)
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("ran out of time waiting for %s to be deleted", realPath)
			}
			t.Fatalf("waiting for %s to be deleted: %v", realPath, ctx.Err())
		case <-time.After(every):
		}
	}
}
