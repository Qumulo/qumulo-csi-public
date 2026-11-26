// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

//go:build live

package live

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"qumulo-csi-driver/pkg/qumulo"
)

// requireScheduleEnabledLive used to skip these tests behind
// QUMULO_LIVE_SCHEDULE=1, while it was unknown whether a policy create worked
// against a real cluster.
//
// The gate is gone because its condition was met: both tests in this file were
// run against Qumulo Core 7.9.3 on 2026-09-10 and passed, including
// TestLiveSnapshotPolicyFires, which waits for the cluster to actually take a
// scheduled snapshot. snapshotScheduleEnabled in pkg/driver/params.go is true,
// so the feature ships -- a skip that hid working coverage of a shipping
// feature was the wrong default.
//
// It survives as a no-op rather than being deleted at every call site, so the
// reason the gate existed and the evidence that removed it stay next to the
// tests it governed.
func requireScheduleEnabledLive(t *testing.T) {
	t.Helper()
}

// policyNameFor mirrors the driver's own naming. Duplicated rather than
// exported: the test asserts the convention rather than inheriting it, so a
// change to the driver's naming shows up here as a failure instead of
// silently agreeing with itself.
func policyNameFor(volumeName string) string { return "csi-" + volumeName }

// findPolicy returns the named policy, or nil.
func findPolicy(ctx context.Context, t *testing.T, h *harness, name string) *qumulo.PolicyInfo {
	t.Helper()

	policies, err := h.Client.ListPolicies(ctx)
	if err != nil {
		t.Fatalf("ListPolicies: %v", err)
	}
	for i := range policies {
		if policies[i].Name == name {
			return &policies[i]
		}
	}
	return nil
}

// TestLiveSnapshotPolicyLifecycle is L1 and L4: the policy is created against
// the volume's own directory, and DeleteVolume takes it away again.
func TestLiveSnapshotPolicyLifecycle(t *testing.T) {
	requireScheduleEnabledLive(t)

	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	before, err := h.Client.ListPolicies(ctx)
	if err != nil {
		t.Fatalf("ListPolicies: %v", err)
	}
	t.Logf("policies on the cluster before: %d", len(before))

	name := h.VolumeName("policy")
	req := h.CreateRequest(name, gib)
	req.Parameters["snapshotSchedule"] = "daily@02:00,keep=7days"

	resp, err := h.Ctrl().CreateVolume(ctx, req)
	if err != nil {
		t.Fatalf("CreateVolume with a schedule: %v", err)
	}
	vol := resp.GetVolume()
	fileID := h.FileIDOf(t, vol.GetVolumeId())
	h.Track(fileID, vol.GetVolumeContext()["realPath"])

	policy := findPolicy(ctx, t, h, policyNameFor(name))
	if policy == nil {
		t.Fatalf("no policy named %q after CreateVolume", policyNameFor(name))
	}
	t.Logf("PROVED: policy %s (id=%s) attached, enabled=%t", policy.Name, policy.ID, policy.Enabled)

	// The policy must point at this volume's directory and no other.
	if policy.SourceFileID != fileID {
		t.Errorf("policy source_file_id is %q, want the volume's own %q", policy.SourceFileID, fileID)
	}
	if !policy.Enabled {
		t.Error("policy was created disabled, so it will never fire")
	}

	// A retried provision must adopt, not stack a second policy.
	if _, err := h.Ctrl().CreateVolume(ctx, req); err != nil {
		t.Fatalf("repeat CreateVolume: %v", err)
	}
	after, err := h.Client.ListPolicies(ctx)
	if err != nil {
		t.Fatalf("ListPolicies: %v", err)
	}
	if len(after) != len(before)+1 {
		t.Errorf("after a repeat provision the cluster holds %d policies, want %d -- a retry duplicated one",
			len(after), len(before)+1)
	}

	// L4: DeleteVolume sweeps it.
	// Secrets, like every controller RPC: the driver keeps no fallback
	// credential, so a DeleteVolume without one is refused before it reaches
	// the cluster and the policy would survive.
	if _, err := h.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: vol.GetVolumeId(),
		Secrets:  h.Secrets(),
	}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if gone := findPolicy(ctx, t, h, policyNameFor(name)); gone != nil {
		t.Errorf("policy %q survived DeleteVolume (id=%s)", gone.Name, gone.ID)
	} else {
		t.Log("PROVED: DeleteVolume removed the policy")
	}
}

// TestLiveSnapshotPolicyFires is L2, and the reason this file exists.
//
// A 200 on policy creation proves only that the cluster accepted the request.
// FINDINGS 18 is the precedent: a snapshot feature once passed a full unit
// suite and could never have worked. The only assertion that settles it is a
// snapshot the policy actually took.
func TestLiveSnapshotPolicyFires(t *testing.T) {
	requireScheduleEnabledLive(t)

	if testing.Short() {
		t.Skip("waits for a scheduled snapshot; skipped under -short")
	}

	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	name := h.VolumeName("policy-fires")
	req := h.CreateRequest(name, gib)
	// The shortest schedule the grammar allows, so the wait is minutes.
	req.Parameters["snapshotSchedule"] = "every=1m,keep=1hours"

	resp, err := h.Ctrl().CreateVolume(ctx, req)
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	vol := resp.GetVolume()
	fileID := h.FileIDOf(t, vol.GetVolumeId())
	h.Track(fileID, vol.GetVolumeContext()["realPath"])

	policy := findPolicy(ctx, t, h, policyNameFor(name))
	if policy == nil {
		t.Fatalf("no policy after CreateVolume")
	}

	// The harness cleans up volumes by tree-deleting them directly, not
	// through DeleteVolume, so nothing would sweep this policy or the
	// snapshots it takes. Registered here rather than relied on.
	t.Cleanup(func() {
		cleanup := context.Background()
		if err := h.Client.DeletePolicy(cleanup, policy.ID); err != nil {
			t.Logf("cleanup: could not delete policy %s: %v", policy.ID, err)
		}
		snapshots, err := h.Client.ListSnapshots(cleanup)
		if err != nil {
			return
		}
		for _, s := range snapshots {
			if s.SourceFileID != fileID {
				continue
			}
			if err := h.Client.DeleteSnapshot(cleanup, s.ID); err != nil {
				t.Logf("cleanup: could not delete snapshot %s: %v", s.ID, err)
				continue
			}
			t.Logf("cleanup: removed policy snapshot %s", s.ID)
		}
	})

	t.Logf("waiting for policy %s to fire on file %s", policy.ID, fileID)

	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		snapshots, err := h.Client.ListSnapshots(ctx)
		if err != nil {
			t.Fatalf("ListSnapshots: %v", err)
		}
		for _, s := range snapshots {
			if s.SourceFileID != fileID {
				continue
			}
			if !s.CreatedByPolicy() {
				continue
			}
			t.Logf("PROVED: the policy fired -- snapshot %s (%q) with policy_id=%s",
				s.ID, s.Name, s.PolicyID)

			// L3: a policy snapshot must stay out of the CSI listing, or
			// deleting a VolumeSnapshot could destroy a scheduled backup.
			listed, err := h.Ctrl().ListSnapshots(ctx, &csi.ListSnapshotsRequest{
				SourceVolumeId: vol.GetVolumeId(),
				Secrets:        h.Secrets(),
			})
			if err != nil {
				t.Fatalf("CSI ListSnapshots: %v", err)
			}
			for _, e := range listed.GetEntries() {
				if e.GetSnapshot().GetSnapshotId() == vol.GetVolumeContext()["server"]+"/"+s.ID {
					t.Errorf("FINDING: policy snapshot %s was surfaced to the CO; deleting that "+
						"VolumeSnapshot would destroy a scheduled backup", s.ID)
				}
			}
			t.Logf("PROVED: the scheduled snapshot is not surfaced as a VolumeSnapshot (%d CSI entries)",
				len(listed.GetEntries()))
			return
		}
		time.Sleep(15 * time.Second)
	}

	t.Errorf("FINDING: the policy was accepted but took no snapshot within 5 minutes. "+
		"The cluster reports acceptance without validating that a schedule will ever fire, "+
		"so check /v3/snapshots/policies/status/%s", policy.ID)
}
