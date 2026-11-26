//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Defect tests for the node plugin's publish and stats paths. Each test
// asserts the CORRECT behaviour, so it fails while its defect is present.
// Fakes only: no real mount, no sudo.

import (
	"context"
	"os"
	"runtime"
	"slices"
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

// dfxNodeServer is a node server over the given mounter with the write probe
// replaced by p.
func dfxNodeServer(t *testing.T, m mount.Interface, p *probeRecorder) *NodeServer {
	t.Helper()
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeNode, NodeID: "n", FakeMounter: true})
	require.NoError(t, err)
	ns := NewNodeServerWithMounter(d, m)
	if p != nil {
		ns.writeProbe = p.probe
	}
	return ns
}

// dfxEBADHANDLE builds the error a syscall against a mount whose export was
// removed on the array produces (errno 521).
func dfxEBADHANDLE(op, path string) error {
	return &os.PathError{Op: op, Path: path, Err: syscall.Errno(521)}
}

// dfxReplicaSource is the mount source replicaPublishRequest names.
const dfxReplicaSource = "dr.qumulo.example.com:/csi/volumes/pvc-a"

// dfxROXRequest is replicaPublishRequest with a ReadOnlyMany access mode and
// readonly=false, i.e. a pod on a ReadOnlyMany claim without readOnly: true.
func dfxROXRequest(target string) *csi.NodePublishVolumeRequest {
	req := replicaPublishRequest(target, true, false, nil)
	req.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
	return req
}

// dfxMountedReadOnly reports whether fm lists target with "ro".
func dfxMountedReadOnly(fm *mount.FakeMounter, target string) bool {
	mps, _ := fm.List()
	for _, mp := range mps {
		if mp.Path == target {
			return isReadOnly(mp.Opts)
		}
	}
	return false
}

// dfxHangingMounter blocks every Mount until release is closed, and counts
// the mount(8) calls started: an array that stopped answering mid-mount.
type dfxHangingMounter struct {
	*mount.FakeMounter
	release chan struct{}
	mounts  atomic.Int32
}

func (h *dfxHangingMounter) Mount(source, target, fstype string, options []string) error {
	h.mounts.Add(1)
	<-h.release
	return nil
}

// dfxUnmountHookMounter is a FakeMounter that runs onUnmount after every
// successful Unmount.
type dfxUnmountHookMounter struct {
	*mount.FakeMounter
	onUnmount func()
}

func (h *dfxUnmountHookMounter) Unmount(target string) error {
	err := h.FakeMounter.Unmount(target)
	if err == nil && h.onUnmount != nil {
		h.onUnmount()
	}
	return err
}

// ----------------------------------------------------------------

// A mountOptions entry carrying several options ("hard,ro") mounts
// read-only, so the driver must treat it as read-only: the publish succeeds
// read-only and is not refused as an unpromoted replica.
func TestDefect_CompositeRoOptionIsSeenAsReadOnly(t *testing.T) {
	fm := mount.NewFakeMounter(nil)
	m := &flakyMounter{FakeMounter: fm}
	p := &probeRecorder{err: erofs()} // what a ro mount answers a create
	ns := dfxNodeServer(t, m, p)
	target := resolvedTempDir(t)

	_, err := ns.NodePublishVolume(context.Background(), replicaPublishRequest(target, false, false, []string{"hard,ro"}))
	require.NoError(t, err, "a hard,ro mount is read-only; it must not be refused as an unpromoted replica")
	assert.True(t, dfxMountedReadOnly(fm, target), "the target is mounted read-only")
	assert.True(t, isReadOnly([]string{"hard,ro"}), "isReadOnly must see the ro inside a comma-joined entry")
}

// "hard,lock" on NFSv3 is refused like a lone "lock", and
// "hard,vers=4.1" on protocol nfs3 is refused as a version contradiction.
func TestDefect_CompositeLockAndVersionOptionsAreRefused(t *testing.T) {
	_, err := nfsLockOptionFor(map[string]string{volumeContextProtocol: protocolNFS3}, []string{"hard,lock"})
	assert.Error(t, err, "an explicit lock inside a comma-joined entry must be refused on NFSv3")

	_, err = nfsVersionOptionFor(map[string]string{volumeContextProtocol: protocolNFS3}, []string{"hard,vers=4.1"})
	assert.Error(t, err, "a version inside a comma-joined entry contradicting protocol nfs3 must be refused")
}

// ----------------------------------------------------------------

// A mount answering EBADHANDLE (errno 521) is classified like
// ESTALE: NodeGetVolumeStats names a stale mount (not an anonymous "checking
// volume path"), does not promise a remount the next publish does not make,
// and NodeGetVolumeHealth grades it INACCESSIBLE/StaleMount.
func TestDefect_BadHandleIsClassifiedAsAStaleMount(t *testing.T) {
	fm := mount.NewFakeMounter(nil)
	ns := dfxNodeServer(t, fm, &probeRecorder{})
	target := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{Path: target, Device: dfxReplicaSource, Opts: []string{"rw"}}}
	ns.statFunc = func(p string) (os.FileInfo, error) { return nil, dfxEBADHANDLE("stat", p) }
	ns.statfsFunc = func(p string) (volumeStats, error) { return volumeStats{}, dfxEBADHANDLE("statfs", p) }

	_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{VolumeId: "v", VolumePath: target})
	require.Error(t, err)
	msg := strings.ToLower(status.Convert(err).Message())
	t.Logf("stats: %s: %s", status.Code(err), msg)
	assert.Contains(t, msg, "stale", "EBADHANDLE must be named a stale mount")
	assert.NotContains(t, msg, "will be remounted on the next", "must not promise a remount the next publish does not make")

	resp, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{VolumeId: "v", VolumePublishPath: target})
	require.NoError(t, err)
	entries := resp.GetVolumeHealth().GetHealthStatuses()
	require.Len(t, entries, 1)
	assert.Equal(t, csi.VolumeHealthErrorType_INACCESSIBLE, entries[0].GetStatus())
	assert.Equal(t, "StaleMount", entries[0].GetReason())
}

// A republish of a target whose mount answers EBADHANDLE -- while
// IsLikelyNotMountPoint, which reads cached attributes, still says "mounted"
// -- actually replaces the dead mount: it is unmounted and mounted afresh.
func TestDefect_RepublishRemountsATargetAnsweringBadHandle(t *testing.T) {
	fm := mount.NewFakeMounter(nil)
	var stale atomic.Bool
	stale.Store(true)
	// Unmounting the dead mount is what clears the bad handle; a fresh
	// mount of the re-created export answers normally.
	m := &dfxUnmountHookMounter{FakeMounter: fm, onUnmount: func() { stale.Store(false) }}
	ns := dfxNodeServer(t, m, &probeRecorder{})
	target := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{Path: target, Device: dfxReplicaSource, Opts: []string{"rw"}}}
	ns.statFunc = func(p string) (os.FileInfo, error) {
		if stale.Load() {
			return nil, dfxEBADHANDLE("stat", p)
		}
		return os.Stat(p)
	}
	ns.writeProbe = func(root string) error {
		if stale.Load() {
			return dfxEBADHANDLE("open", root)
		}
		return nil
	}

	_, err := ns.NodePublishVolume(context.Background(), replicaPublishRequest(target, false, false, nil))
	var kinds []string
	for _, a := range fm.GetLog() {
		kinds = append(kinds, a.Action)
	}
	t.Logf("publish: %v; mounter log: %v", err, kinds)
	unmountAt := slices.Index(kinds, mount.FakeActionUnmount)
	require.GreaterOrEqual(t, unmountAt, 0, "the dead mount was never unmounted: the pod keeps errno 521")
	assert.Contains(t, kinds[unmountAt:], mount.FakeActionMount, "the target was not mounted afresh after the dead mount was removed")
	assert.NoError(t, err)
}

// A fresh read-write publish whose write probe answers EBADHANDLE
// must not be answered OK as "inconclusive; publishing anyway": either the
// publish fails (so the retry remounts) or the driver remounts it itself.
// A probe ESTALE/EBADHANDLE means a broken mount.
func TestDefect_BadHandleFromTheWriteProbeIsNotPublishedAsOK(t *testing.T) {
	fm := mount.NewFakeMounter(nil)
	p := &probeRecorder{err: dfxEBADHANDLE("open", "/target/.probe")}
	ns := dfxNodeServer(t, fm, p)
	target := resolvedTempDir(t)

	_, err := ns.NodePublishVolume(context.Background(), replicaPublishRequest(target, false, false, nil))
	mounts := 0
	for _, a := range fm.GetLog() {
		if a.Action == mount.FakeActionMount {
			mounts++
		}
	}
	t.Logf("publish: %v; mounts made: %d", err, mounts)
	assert.True(t, err != nil || mounts > 1,
		"a probe answering EBADHANDLE was published OK on the dead mount, with no remount")
}

// ----------------------------------------------------------------

// Repeated NodeGetVolumeStats polls on a hung mount must not leave
// one blocked goroutine per poll; the count stays bounded.
func TestDefect_StatsPollsOnAHungMountDoNotLeakAGoroutineEach(t *testing.T) {
	ns := dfxNodeServer(t, mount.NewFakeMounter(nil), &probeRecorder{})
	ns.statTimeout = 5 * time.Millisecond
	hang := make(chan struct{})
	defer close(hang)
	ns.statFunc = func(string) (os.FileInfo, error) { <-hang; return nil, nil }

	before := runtime.NumGoroutine()
	const polls = 50
	for range polls {
		_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId: "v", VolumePath: "/var/lib/kubelet/pods/x/volumes/kubernetes.io~csi/pv/mount"})
		require.Error(t, err)
	}
	leaked := runtime.NumGoroutine() - before
	t.Logf("goroutine growth after %d polls: %d", polls, leaked)
	assert.LessOrEqual(t, leaked, 5, "each poll of a hung mount left a goroutine (and an OS thread) behind")
}

// ----------------------------------------------------------------

// A read-write publish whose mount outlived the timeout and then
// completed in the background must still be write-probed on the retry, so a
// read-only replica is refused rather than published read-write.
func TestDefect_RetryAfterATimedOutMountStillProbesTheReplica(t *testing.T) {
	block := make(chan struct{})
	m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil), blockMount: block}
	p := &probeRecorder{err: erofs()} // the array serves this volume read-only
	ns := dfxNodeServer(t, m, p)
	ns.mountTimeout = 20 * time.Millisecond
	target := resolvedTempDir(t)
	req := replicaPublishRequest(target, false, false, nil)

	_, err := ns.NodePublishVolume(context.Background(), req)
	require.Error(t, err)

	close(block) // the kernel finishes the mount after the RPC gave up
	require.Eventually(t, func() bool {
		mps, _ := m.List()
		return len(mps) == 1
	}, 2*time.Second, 5*time.Millisecond)

	_, err = ns.NodePublishVolume(context.Background(), req)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "the retry published a read-only replica read-write: %v", err)
	assert.GreaterOrEqual(t, p.calls.Load(), int32(1), "the write probe never ran")
}

// After an EROFS refusal whose unmount failed, the next publish of
// the same read-only replica must be refused too.
func TestDefect_RetryAfterARefusalWithAFailedUnmountIsStillRefused(t *testing.T) {
	m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil), unmountErr: os.ErrPermission}
	p := &probeRecorder{err: erofs()}
	ns := dfxNodeServer(t, m, p)
	target := resolvedTempDir(t)
	req := replicaPublishRequest(target, false, false, nil)

	_, err := ns.NodePublishVolume(context.Background(), req)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = ns.NodePublishVolume(context.Background(), req)
	assert.Error(t, err, "the second publish of the same read-only replica succeeded")
	assert.EqualValues(t, 2, p.calls.Load(), "the retry must probe the leftover mount again")
}

// ----------------------------------------------------------------

// NodeGetVolumeStats on a target that is no longer a mount must
// refuse (NotFound, or FailedPrecondition); it must never report
// the node's own disk as the volume.
func TestDefect_StatsRefuseATargetThatIsNoLongerAMount(t *testing.T) {
	ns := dfxNodeServer(t, mount.NewFakeMounter(nil), &probeRecorder{})
	target := resolvedTempDir(t) // a plain directory on the node's disk

	resp, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{VolumeId: "v", VolumePath: target})
	// The CSI version this driver builds against has no volume_condition on
	// NodeGetVolumeStatsResponse (it moved to NodeGetVolumeHealth), so a
	// refusal is the only correct answer here.
	require.Error(t, err, "answered OK with the node's own disk as the volume's usage: %v", resp.GetUsage())
	assert.Contains(t, []codes.Code{codes.NotFound, codes.FailedPrecondition}, status.Code(err), "%v", err)
}

// ---------------------------------------------------------------- ReadOnlyMany

// dfxROXPublishMountsReadOnly publishes a ReadOnlyMany claim (no readOnly:
// true) of a replica that answers EROFS, and reports whether the driver
// mounted it read-only and served it.
func dfxROXPublishMountsReadOnly(t *testing.T) (bool, error) {
	t.Helper()
	fm := mount.NewFakeMounter(nil)
	ns := dfxNodeServer(t, fm, &probeRecorder{err: erofs()})
	target := resolvedTempDir(t)
	_, err := ns.NodePublishVolume(context.Background(), dfxROXRequest(target))
	return err == nil && dfxMountedReadOnly(fm, target), err
}

// ReadOnlyMany is either enforced at mount time, or nothing in
// the driver claims it is: the node code comments must not say ReadOnlyMany
// appends "ro" or that the readonly flag reflects the access mode.
// See node_server.go and node_replica.go.
func TestDefect_ReadOnlyManyIsEnforcedOrNotClaimedToBe(t *testing.T) {
	if ro, _ := dfxROXPublishMountsReadOnly(t); ro {
		return
	}
	for _, file := range []string{"pkg/driver/node_server.go", "pkg/driver/node_replica.go"} {
		text := dfxNormalizeComment(dfxReadRepoFile(t, file))
		for _, claim := range []string{
			"readonlymany appended",
			"reflects only the access mode",
			"legitimate readonlymany publish",
		} {
			assert.NotContains(t, text, claim, "%s claims ReadOnlyMany is mounted read-only, which it is not", file)
		}
	}
}

// ----------------------------------------------------------------

// dfxHungProbeServer is a server with target already mounted read-write and a
// write probe that blocks until the test ends, counting its starts.
func dfxHungProbeServer(t *testing.T) (*NodeServer, *csi.NodePublishVolumeRequest, *atomic.Int32) {
	t.Helper()
	fm := mount.NewFakeMounter(nil)
	ns := dfxNodeServer(t, fm, nil)
	ns.statTimeout = 10 * time.Millisecond
	target := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{Path: target, Device: dfxReplicaSource, Opts: []string{"rw"}}}
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	var probes atomic.Int32
	ns.writeProbe = func(string) error { probes.Add(1); <-hang; return nil }
	return ns, replicaPublishRequest(target, false, false, nil), &probes
}

// A write probe that times out on a hung array must not answer
// OK (which skips the replica check); it answers a retryable error such as
// Unavailable.
func TestDefect_ProbeTimeoutIsNotAnsweredOK(t *testing.T) {
	ns, req, _ := dfxHungProbeServer(t)
	_, err := ns.NodePublishVolume(context.Background(), req)
	require.Error(t, err, "a probe that never finished was answered OK; the replica check was skipped")
	assert.Contains(t, []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Aborted}, status.Code(err), "%v", err)
}

// Republishes of a hung mount must share the probe still blocked
// in the kernel, not start (and pin a thread for) another one each.
func TestDefect_RepublishesOfAHungMountShareOneProbe(t *testing.T) {
	ns, req, probes := dfxHungProbeServer(t)
	before := runtime.NumGoroutine()
	const republishes = 40
	for range republishes {
		_, _ = ns.NodePublishVolume(context.Background(), req)
	}
	grown := runtime.NumGoroutine() - before
	t.Logf("%d republishes: %d probes started, goroutine growth %d", republishes, probes.Load(), grown)
	assert.LessOrEqual(t, probes.Load(), int32(1), "each republish started another probe beside the blocked one")
	assert.LessOrEqual(t, grown, 10, "each republish left a blocked goroutine behind")
}

// ---------------------------------------------------------------- (publish)

// A publish retry while the first mount(8) is still blocked must
// wait on it, not start another mount(8) for the same target.
func TestDefect_PublishRetriesDoNotStartAnotherMount(t *testing.T) {
	m := &dfxHangingMounter{FakeMounter: mount.NewFakeMounter(nil), release: make(chan struct{})}
	t.Cleanup(func() { close(m.release) })
	ns := dfxNodeServer(t, m, &probeRecorder{})
	ns.mountTimeout = 20 * time.Millisecond
	target := resolvedTempDir(t)
	req := replicaPublishRequest(target, false, false, nil)

	before := runtime.NumGoroutine()
	const attempts = 20
	for range attempts {
		_, err := ns.NodePublishVolume(context.Background(), req)
		require.Error(t, err)
	}
	grown := runtime.NumGoroutine() - before
	t.Logf("%d publish attempts: %d mount(8) calls, goroutine growth %d", attempts, m.mounts.Load(), grown)
	assert.Equal(t, int32(1), m.mounts.Load(), "each retry started another mount(8) beside the blocked one")
	assert.LessOrEqual(t, grown, 5)
}

// ---------------------------------------------------------------- (node)

// Node part: a volume with no protocol whose mountOptions pin NFSv3
// (vers=3 / nfsvers=3, also inside a comma-joined entry) needs nolock exactly
// as protocol: nfs3 does, because the node image runs no rpc.statd. The
// publish must either mount with locking turned off explicitly (nolock or a
// local_lock=) or refuse the version pin up front; mounting v3 with neither
// fails at mount time.
func TestDefect_VersionPinnedNFSv3WithoutProtocolGetsNolockOrIsRefused(t *testing.T) {
	for _, flags := range [][]string{{"vers=3"}, {"nfsvers=3"}, {"hard,vers=3"}} {
		t.Run(strings.Join(flags, "|"), func(t *testing.T) {
			fm := mount.NewFakeMounter(nil)
			ns := dfxNodeServer(t, fm, &probeRecorder{})
			target := resolvedTempDir(t)
			req := replicaPublishRequest(target, false, false, flags) // no protocol in the volume context

			_, err := ns.NodePublishVolume(context.Background(), req)
			if err != nil {
				assert.Contains(t, []codes.Code{codes.InvalidArgument, codes.FailedPrecondition}, status.Code(err), "%v", err)
				return
			}
			mps, _ := fm.List()
			require.Len(t, mps, 1)
			opts := mountOptionList(mps[0].Opts)
			t.Logf("mounted with %v", opts)
			lockOff := slices.Contains(opts, "nolock") || slices.ContainsFunc(opts, func(o string) bool {
				return strings.HasPrefix(o, "local_lock=")
			})
			assert.True(t, lockOff, "an NFSv3 mount without nolock needs rpc.statd, which the node image does not run")
		})
	}
}
