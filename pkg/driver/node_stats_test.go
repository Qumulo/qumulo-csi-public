// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

// mountedNodeServer is a node server whose fake mounter reports volumePath as
// a mount point, so NodeGetVolumeStats measures it rather than refusing a
// plain directory.
func mountedNodeServer(t *testing.T) (*NodeServer, string) {
	t.Helper()
	ns, fm := newTestNodeServerWithFakeMounter(t)
	volumePath := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{Path: volumePath, Device: "qumulo.example.com:/export"}}
	return ns, volumePath
}

func TestNodeGetCapabilities_ReportsVolumeStats(t *testing.T) {
	ns := newTestNodeServer(t)

	resp, err := ns.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})

	require.NoError(t, err)

	// Only that GET_VOLUME_STATS is among them: this file is about the stats
	// RPC, and TestNodeGetCapabilities in node_server_test.go is where the
	// exhaustive set is pinned. Asserting the whole set in both places meant
	// adding one capability failed two tests for one reason.
	var advertised []csi.NodeServiceCapability_RPC_Type
	for _, capability := range resp.GetCapabilities() {
		advertised = append(advertised, capability.GetRpc().GetType())
	}
	assert.Contains(t, advertised, csi.NodeServiceCapability_RPC_GET_VOLUME_STATS)
}

func TestNodeGetVolumeStats_Success(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("statfs is Unix-only; the node service only runs on Linux")
	}

	ns, volumePath := mountedNodeServer(t)

	resp, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   "qumulo.example.com/12884901888/pvc-abc",
		VolumePath: volumePath,
	})

	require.NoError(t, err)
	require.Len(t, resp.GetUsage(), 2, "bytes and inodes")

	byUnit := map[csi.VolumeUsage_Unit]*csi.VolumeUsage{}
	for _, usage := range resp.GetUsage() {
		byUnit[usage.GetUnit()] = usage
	}

	bytes := byUnit[csi.VolumeUsage_BYTES]
	require.NotNil(t, bytes, "a BYTES entry is what kubelet's volume metrics read")
	assert.Positive(t, bytes.GetTotal())
	// CSI requires every figure to be non-negative, and an NFS server
	// reporting more free than total must not underflow into a huge number.
	assert.GreaterOrEqual(t, bytes.GetUsed(), int64(0))
	assert.GreaterOrEqual(t, bytes.GetAvailable(), int64(0))
	assert.LessOrEqual(t, bytes.GetUsed(), bytes.GetTotal())

	inodes := byUnit[csi.VolumeUsage_INODES]
	require.NotNil(t, inodes)
	assert.GreaterOrEqual(t, inodes.GetUsed(), int64(0))
	assert.GreaterOrEqual(t, inodes.GetAvailable(), int64(0))
}

func TestNodeGetVolumeStats_Errors(t *testing.T) {
	existing := t.TempDir()

	tests := []struct {
		name     string
		req      *csi.NodeGetVolumeStatsRequest
		wantCode codes.Code
	}{
		{
			name:     "missing volume id",
			req:      &csi.NodeGetVolumeStatsRequest{VolumePath: existing},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "missing volume path",
			req:      &csi.NodeGetVolumeStatsRequest{VolumeId: "vol-1"},
			wantCode: codes.InvalidArgument,
		},
		{
			// The spec singles this case out so the CO can tell "not
			// mounted" apart from "could not measure".
			name: "path does not exist",
			req: &csi.NodeGetVolumeStatsRequest{
				VolumeId:   "vol-1",
				VolumePath: filepath.Join(existing, "absent"),
			},
			wantCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := newTestNodeServer(t)

			resp, err := ns.NodeGetVolumeStats(context.Background(), tt.req)

			require.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, tt.wantCode, status.Code(err))
		})
	}
}

// A wedged stat(2) must bound NodeGetVolumeStats, not the other way round.
//
// Nothing bounded ns.statFunc before this: a hard-hung NFS mount blocks
// stat(2) exactly as it blocks mount(8), so one wedged volume held every
// NodeGetVolumeStats call against it open forever.
func TestNodeGetVolumeStats_TimesOutOnAWedgedStat(t *testing.T) {
	ns := newTestNodeServer(t)
	ns.statTimeout = 20 * time.Millisecond
	block := make(chan struct{})
	ns.statFunc = func(path string) (os.FileInfo, error) {
		<-block
		return nil, nil
	}

	done := make(chan error, 1)
	go func() {
		_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId:   "vol-1",
			VolumePath: "/does/not/matter",
		})
		done <- err
	}()

	select {
	case err := <-done:
		// DeadlineExceeded says "not yet", which is what the CO should hear
		// about a stat that may still be blocked in the kernel.
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("NodeGetVolumeStats did not return: the stat timeout is not bounding the RPC")
	}

	// Let the blocked stat finish so the goroutine ends rather than being
	// left behind by the test.
	close(block)
}

// statfs(2) is the syscall that actually round-trips to the NFS server, so
// it must be bounded independently of the existence check that runs before it.
func TestNodeGetVolumeStats_TimesOutOnAWedgedStatfs(t *testing.T) {
	ns, volumePath := mountedNodeServer(t)
	ns.statTimeout = 20 * time.Millisecond
	block := make(chan struct{})
	ns.statfsFunc = func(path string) (volumeStats, error) {
		<-block
		return volumeStats{}, nil
	}

	done := make(chan error, 1)
	go func() {
		_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId:   "vol-1",
			VolumePath: volumePath,
		})
		done <- err
	}()

	select {
	case err := <-done:
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("NodeGetVolumeStats did not return: the stat timeout is not bounding the RPC")
	}

	close(block)
}

// A caller that gives up first gets its own cancellation back, not a timeout.
func TestNodeGetVolumeStats_HonoursCallerCancellation(t *testing.T) {
	ns := newTestNodeServer(t)
	ns.statTimeout = time.Hour // never fires; the context must be what ends this
	block := make(chan struct{})
	defer close(block)
	ns.statFunc = func(path string) (os.FileInfo, error) {
		<-block
		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ns.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
			VolumeId:   "vol-1",
			VolumePath: "/does/not/matter",
		})
		done <- err
	}()
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.NotEqual(t, codes.DeadlineExceeded, status.Code(err),
			"a cancelled caller is not a stat timeout")
	case <-time.After(5 * time.Second):
		t.Fatal("NodeGetVolumeStats ignored its context")
	}
}

// A target that is no longer a mount is a plain directory on the node's
// disk, and statfs on it measures that disk. It must not be reported as the
// volume: NotFound, and statfs is never asked.
func TestNodeGetVolumeStats_RefusesATargetThatIsNoLongerAMount(t *testing.T) {
	ns, _ := newTestNodeServerWithFakeMounter(t)
	measured := false
	ns.statfsFunc = func(string) (volumeStats, error) {
		measured = true
		return volumeStats{TotalBytes: 42869960704}, nil
	}

	_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   "qumulo.example.com##/csi/pvc-abc",
		VolumePath: resolvedTempDir(t),
	})

	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.False(t, measured, "the node's own disk must not be measured as the volume")
}

// Kubelet polls NodeGetVolumeStats on its own schedule, and a stat on a
// hard NFS mount whose server is gone blocks in the kernel. Every poll used to
// start another blocked goroutine -- each pinning an OS thread, towards Go's
// 10,000-thread abort -- so twenty polls must join the one call in flight, and
// only once it returns may the next poll start a fresh one.
func TestNodeGetVolumeStats_JoinsTheCallStillBlockedOnAHungMount(t *testing.T) {
	ns, volumePath := mountedNodeServer(t)
	ns.statTimeout = 5 * time.Millisecond
	block := make(chan struct{})
	var started atomic.Int32
	ns.statFunc = func(string) (os.FileInfo, error) {
		started.Add(1)
		<-block
		return nil, nil
	}

	for i := 0; i < 20; i++ {
		_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId: "vol-1", VolumePath: volumePath,
		})
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	}
	assert.Equal(t, int32(1), started.Load(), "one blocked stat per path, not one per poll")

	// The kernel finally gives up; the next poll measures afresh.
	close(block)
	require.Eventually(t, func() bool {
		ns.statCalls.mu.Lock()
		defer ns.statCalls.mu.Unlock()
		return len(ns.statCalls.inflight) == 0
	}, time.Second, time.Millisecond)
	ns.statFunc = func(string) (os.FileInfo, error) { started.Add(1); return nil, nil }
	_, _ = ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{VolumeId: "vol-1", VolumePath: volumePath})
	assert.Equal(t, int32(2), started.Load())
}

// Different paths are different calls: one hung volume must not make a
// healthy one wait on it.
func TestNodeGetVolumeStats_DoesNotJoinAnotherPathsCall(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	hung, healthy := resolvedTempDir(t), resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{Path: hung}, {Path: healthy}}
	ns.statTimeout = 50 * time.Millisecond
	block := make(chan struct{})
	defer close(block)
	ns.statFunc = func(p string) (os.FileInfo, error) {
		if p == hung {
			<-block
		}
		return nil, nil
	}

	_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{VolumeId: "a", VolumePath: hung})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))

	_, err = ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{VolumeId: "b", VolumePath: healthy})
	require.NoError(t, err)
}

// Each kubelet stats poll of a hung volume logged the stat timeout at
// ERROR, about 110 lines an hour per volume for as long as it hung, which
// rotated the first-failure lines out. A flight's timeout is now logged at
// ERROR once; the polls that join it after that log at Debug.
func TestAHungStatIsLoggedAtErrorOncePerFlight(t *testing.T) {
	var logged bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(restore) })

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeNode, NodeID: "n", FakeMounter: true})
	require.NoError(t, err)
	ns := NewNodeServerWithMounter(d, mount.NewFakeMounter(nil))
	ns.statTimeout = 10 * time.Millisecond
	ns.statFunc = func(string) (os.FileInfo, error) { <-release; return nil, nil }

	for range 5 {
		require.ErrorIs(t, ns.statWithTimeout(context.Background(), "/hung"), errStatTimeout)
	}
	assert.Equal(t, 1, strings.Count(logged.String(), "level=ERROR"), logged.String())
}

// inflightKeys is the set of keys still in flight in f.
func inflightKeys(f *statFlights) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.inflight))
	for k := range f.inflight {
		keys = append(keys, k)
	}
	return keys
}

// A call blocked in the kernel on a dead mount never returns, so its
// entry outlived the volume: after unpublish, a hung stat, statfs, probe or
// umount for the target stayed in flight for the life of the node plugin.
// Once unpublish has removed the target, its entries go; another volume's
// hung call is left alone.
func TestUnpublishForgetsTheTargetsHungCalls(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target, other := resolvedTempDir(t), resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{
		{Path: target, Device: "qumulo.example.com:/a"},
		{Path: other, Device: "qumulo.example.com:/b"},
	}
	ns.statTimeout = 5 * time.Millisecond
	block := make(chan struct{})
	defer close(block)
	var hang atomic.Bool
	hang.Store(true)
	ns.statFunc = func(p string) (os.FileInfo, error) {
		if hang.Load() {
			<-block
		}
		return os.Stat(p)
	}
	for _, p := range []string{target, other} {
		_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{VolumeId: "vol", VolumePath: p})
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	}
	require.Len(t, inflightKeys(&ns.statCalls), 2)

	hang.Store(false)
	_, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "vol", TargetPath: target})
	require.NoError(t, err)

	assert.Equal(t, []string{"stat\x00" + other}, inflightKeys(&ns.statCalls),
		"the unpublished target's hung stat goes; the other volume's stays")
}

// An unpublish that fails leaves the target mounted, so its hung calls stay:
// the next poll must join them, not start another blocked call beside them.
func TestAFailedUnpublishKeepsTheTargetsHungCalls(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{Path: target, Device: "qumulo.example.com:/a"}}
	ns.statTimeout = 5 * time.Millisecond
	block := make(chan struct{})
	defer close(block)
	ns.statFunc = func(string) (os.FileInfo, error) { <-block; return nil, nil }
	_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{VolumeId: "vol", VolumePath: target})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))

	fm.UnmountFunc = func(string) error { return syscall.EBUSY }
	_, err = ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "vol", TargetPath: target})
	require.Error(t, err)

	assert.Contains(t, inflightKeys(&ns.statCalls), "stat\x00"+target)
}

// A forgotten call that finally returns must not remove a newer call made
// under the same key since.
func TestALateCallDoesNotRemoveTheCallThatReplacedIt(t *testing.T) {
	var f statFlights
	release := make(chan struct{})
	old := f.join("k", "old", func() (volumeStats, error) { <-release; return volumeStats{}, nil })
	f.forget("k")
	newer := f.join("k", "new", func() (volumeStats, error) { select {} })

	close(release)
	<-old.done
	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Same(t, newer, f.inflight["k"])
}
