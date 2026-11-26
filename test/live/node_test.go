//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// The node service against a real NFS mount.
//
// Everything else in this repository tests NodePublishVolume through a fake
// mounter, which answers whatever the test scripted. That covers the driver's
// decisions -- which options it assembles, how it handles a stale target --
// and cannot cover the one thing the node service exists to do, which is hand
// those options to mount(8) and have the kernel accept them. An option the
// driver is confident about and mount.nfs rejects looks identical to a passing
// test until a pod fails to start.
//
// So these run mount(8) for real. That needs root, the nfs and nfsv3 kernel
// modules, and the mount.nfs helper on the host, which is why the Makefile's
// live-node target compiles the binary as you and runs only the binary under
// sudo, and why this file carries its own QUMULO_LIVE_NODE gate on top of
// QUMULO_LIVE_TEST: a machine that happens to have the live variables set is
// not thereby volunteering its mount table.
package live

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// publishAndUnpublish mounts, runs the assertions, and always unmounts.
//
// The unmount is a t.Cleanup rather than a defer so that a t.Fatal inside the
// assertions still leaves the host's mount table as it was found. A leaked
// NFS mount on a developer's laptop is a hang on the next `df`.
func publishAndUnpublish(
	ctx context.Context,
	t *testing.T,
	h *harness,
	volume provisionedVolume,
	protocol string,
	assert func(t *testing.T, target, mountLine string),
) {
	t.Helper()

	target := filepath.Join(t.TempDir(), "mount")
	if err := os.MkdirAll(target, 0o750); err != nil {
		t.Fatalf("creating the target path: %v", err)
	}

	node := h.Driver.NodeServer()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		if _, err := node.NodeUnpublishVolume(cleanupCtx, &csi.NodeUnpublishVolumeRequest{
			VolumeId:   volume.VolumeID,
			TargetPath: target,
		}); err != nil {
			t.Logf("cleanup: unpublishing %s: %v -- it may need `umount` by hand", target, err)
		}
	})

	if _, err := node.NodePublishVolume(ctx, publishRequest(h, volume, target, protocol)); err != nil {
		t.Fatalf("NodePublishVolume(%s): %v", protocol, err)
	}

	line, mounted := mountTable(t, target)
	if !mounted {
		t.Fatalf("NodePublishVolume reported success but nothing is mounted at %s", target)
	}
	t.Logf("mount: %s", line)

	assert(t, target, line)
}

// TestLiveNodePublishNFS41Mounts is the ordinary path.
func TestLiveNodePublishNFS41Mounts(t *testing.T) {
	requireNodeMounting(t)
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "node-v41", gib)

	publishAndUnpublish(ctx, t, h, volume, "nfs4.1", func(t *testing.T, target, line string) {
		if !strings.Contains(line, "vers=4.1") {
			t.Errorf("mounted without vers=4.1: %s", line)
		}
		// v4 locks in-protocol. nolock here would be accepted and ignored,
		// and would misdescribe the mount in every operator's mount table.
		if strings.Contains(line, "nolock") || strings.Contains(line, "local_lock=all") {
			t.Errorf("an NFSv4.1 mount must not carry nolock: %s", line)
		}
		requireWritable(t, target)
	})
}

// TestLiveNodePublishNFS3AddsNolock is the NFSv3 nolock acceptance
// test, on a real kernel.
//
// The reported defect: the node image ships mount.nfs but runs no rpc.statd,
// so an NFSv3 mount without nolock either fails outright with "rpc.statd is
// not running but is required for remote locking" or -- worse -- mounts and
// breaks locking silently. Neither the driver nor the chart added the option,
// and a hand-written StorageClass bypasses the chart's defaults entirely.
//
// The class here deliberately sets no mount options at all, which is exactly
// the case that used to break. The driver must supply nolock itself.
func TestLiveNodePublishNFS3AddsNolock(t *testing.T) {
	requireNodeMounting(t)
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "node-v3", gib)

	publishAndUnpublish(ctx, t, h, volume, "nfs3", func(t *testing.T, target, line string) {
		if !strings.Contains(line, "vers=3") {
			t.Errorf("mounted without vers=3: %s", line)
		}
		// The kernel renders nolock as local_lock=all in /proc/self/mounts,
		// so that is what proves it arrived -- asserting on the literal
		// "nolock" would pass on a mount where it was never applied.
		if !strings.Contains(line, "local_lock=all") {
			t.Errorf("an NFSv3 mount must carry nolock (local_lock=all); the node image runs "+
				"no rpc.statd, so without it locking is silently broken: %s", line)
		}
		requireWritable(t, target)
	})
}

// TestLiveNodePublishIsIdempotent: kubelet re-sends NodePublishVolume for a
// target that is already mounted, and the second call must succeed without
// stacking a second mount on the same directory.
func TestLiveNodePublishIsIdempotent(t *testing.T) {
	requireNodeMounting(t)
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "node-idem", gib)

	publishAndUnpublish(ctx, t, h, volume, "nfs4.1", func(t *testing.T, target, line string) {
		node := h.Driver.NodeServer()
		if _, err := node.NodePublishVolume(ctx,
			publishRequest(h, volume, target, "nfs4.1")); err != nil {
			t.Fatalf("the second NodePublishVolume for an already-mounted target failed: %v", err)
		}
		if _, mounted := mountTable(t, target); !mounted {
			t.Fatal("the target is no longer mounted after a repeat publish")
		}
	})
}

// TestLiveNodeGetVolumeStatsReportsARealMount.
//
// kubelet turns these numbers into kubelet_volume_stats_*, which is how a
// volume filling up becomes visible before a write fails. Against a real
// mount they have to be non-zero and internally consistent.
func TestLiveNodeGetVolumeStatsReportsARealMount(t *testing.T) {
	requireNodeMounting(t)
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "node-stats", gib)

	publishAndUnpublish(ctx, t, h, volume, "nfs4.1", func(t *testing.T, target, line string) {
		resp, err := h.Driver.NodeServer().NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
			VolumeId:   volume.VolumeID,
			VolumePath: target,
		})
		if err != nil {
			t.Fatalf("NodeGetVolumeStats on a mounted volume: %v", err)
		}

		var sawBytes bool
		for _, usage := range resp.GetUsage() {
			if usage.GetUnit() != csi.VolumeUsage_BYTES {
				continue
			}
			sawBytes = true
			if usage.GetTotal() <= 0 {
				t.Errorf("total bytes = %d on a live mount", usage.GetTotal())
			}
			if usage.GetUsed()+usage.GetAvailable() > usage.GetTotal() {
				t.Errorf("used (%d) + available (%d) exceeds total (%d)",
					usage.GetUsed(), usage.GetAvailable(), usage.GetTotal())
			}
		}
		if !sawBytes {
			t.Error("no BYTES usage reported; kubelet_volume_stats_* would be empty")
		}

	})
}

// TestLiveNodeGetVolumeHealthReportsAHealthyMountAsHealthy.
//
// Health is a separate RPC in CSI v1.13, not a field on NodeGetVolumeStats --
// see the note on NodeGetVolumeHealth in pkg/driver/node_server.go. An empty
// health_statuses list is the spec's spelling of "healthy", so the assertion
// is that a mounted, writable volume reports nothing at all.
//
// Worth its own test because the expensive failure here is the false positive:
// a healthy volume reporting DEGRADED pages somebody, and after the second
// time nobody believes the signal again.
func TestLiveNodeGetVolumeHealthReportsAHealthyMountAsHealthy(t *testing.T) {
	requireNodeMounting(t)
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "node-health", gib)

	publishAndUnpublish(ctx, t, h, volume, "nfs4.1", func(t *testing.T, target, line string) {
		resp, err := h.Driver.NodeServer().NodeGetVolumeHealth(ctx, &csi.NodeGetVolumeHealthRequest{
			VolumeId:          volume.VolumeID,
			VolumePublishPath: target,
		})
		if err != nil {
			t.Fatalf("NodeGetVolumeHealth on a mounted volume: %v", err)
		}

		health := resp.GetVolumeHealth()
		if health == nil {
			t.Fatal("NodeGetVolumeHealth returned no VolumeHealth")
		}
		if got := health.GetVolumeId(); got != volume.VolumeID {
			t.Errorf("VolumeId = %q, want %q", got, volume.VolumeID)
		}
		for _, entry := range health.GetHealthStatuses() {
			t.Errorf("a healthy mount reported %s (%s): %s",
				entry.GetStatus(), entry.GetReason(), entry.GetMessage())
		}
	})
}

// TestLiveNodeGetVolumeHealthReportsAnUnmountedPathAsInaccessible.
//
// The case the whole feature exists for. An export removed on the array, or a
// node that lost the mount, leaves a pod writing into an empty directory on
// local disk -- silently, because nothing fails. This is what turns that into
// a signal a CO can act on.
func TestLiveNodeGetVolumeHealthReportsAnUnmountedPathAsInaccessible(t *testing.T) {
	requireNodeMounting(t)
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	resp, err := h.Driver.NodeServer().NodeGetVolumeHealth(ctx, &csi.NodeGetVolumeHealthRequest{
		VolumeId:          h.NFSServer + "/1/csi-live-test-absent",
		VolumePublishPath: filepath.Join(t.TempDir(), "never-mounted"),
	})
	if err != nil {
		t.Fatalf("NodeGetVolumeHealth on a missing path returned an error: %v -- health is a "+
			"report, not a failure; an error here tells the CO nothing about the volume", err)
	}

	statuses := resp.GetVolumeHealth().GetHealthStatuses()
	if len(statuses) == 0 {
		t.Fatal("a path that is not mounted reported healthy")
	}
	if got := statuses[0].GetStatus(); got != csi.VolumeHealthErrorType_INACCESSIBLE {
		t.Errorf("status = %s, want INACCESSIBLE", got)
	}
	if statuses[0].GetMessage() == "" {
		t.Error("the message is what an operator reads on the pod Event; it must not be empty")
	}
}

// TestLiveNodeGetVolumeStatsOnAnUnmountedPathIsNotFound.
//
// The spec asks for NotFound specifically, so the CO can tell "not mounted"
// from "could not measure" -- the two need different responses from a health
// monitor.
func TestLiveNodeGetVolumeStatsOnAnUnmountedPathIsNotFound(t *testing.T) {
	requireNodeMounting(t)
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	_, err := h.Driver.NodeServer().NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
		VolumeId:   h.NFSServer + "/1/csi-live-test-absent",
		VolumePath: filepath.Join(t.TempDir(), "never-mounted"),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("NodeGetVolumeStats on an unmounted path returned %v, want NotFound", err)
	}
}

// TestLiveNodeUnpublishIsIdempotent: kubelet retries unpublish too, and a
// second call for a target that is already gone must not fail -- otherwise the
// pod stays Terminating.
func TestLiveNodeUnpublishIsIdempotent(t *testing.T) {
	requireNodeMounting(t)
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, h, "node-unpub", gib)
	target := filepath.Join(t.TempDir(), "mount")
	if err := os.MkdirAll(target, 0o750); err != nil {
		t.Fatalf("creating the target path: %v", err)
	}

	node := h.Driver.NodeServer()
	if _, err := node.NodePublishVolume(ctx,
		publishRequest(h, volume, target, "nfs4.1")); err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := node.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
			VolumeId:   volume.VolumeID,
			TargetPath: target,
		}); err != nil {
			t.Fatalf("attempt %d of NodeUnpublishVolume: %v -- the second must succeed too",
				attempt, err)
		}
	}

	if _, mounted := mountTable(t, target); mounted {
		t.Errorf("%s is still mounted after NodeUnpublishVolume reported success", target)
	}
}
