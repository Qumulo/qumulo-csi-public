// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	mount "k8s.io/mount-utils"
)

// These tests deliberately avoid "Node" in their names.
//
// pkg/driver panics on darwin in anything that constructs a node server
// through NewNodeServer, because newRealMounter is Linux-only, so a Mac runs
// the suite with `-skip '...|Node'`. Everything here builds its NodeServer
// from a fake mounter instead, never touches newRealMounter, and never execs
// umount(8) -- so it runs on darwin as well as on CI's Linux, and the skip
// pattern must not swallow it. The escalation ladder is the one piece of this
// branch that most needs to be exercised on a developer's machine.

// blockingMounter is a mounter whose Unmount can be told to block, to fail, or
// to succeed, so the escalation ladder can be driven without a dead NFS
// server.
//
// It carries its own mount table rather than embedding FakeMounter's, because
// the ladder's exit condition is "gone from the mount table" and the point of
// these tests is to control when that becomes true -- including the case where
// umount(8) exits 0 and the mount is still listed.
type blockingMounter struct {
	mount.FakeMounter

	mu sync.Mutex

	// block, when non-nil, makes Unmount wait for it: the measured case, a
	// hard NFS mount to an array that is powered off.
	block chan struct{}
	// unmountErr, when set, is what the plain Unmount returns.
	unmountErr error
	// clearsMountOnUnmount decides whether a successful plain Unmount also
	// removes the entry from the table.
	clearsMountOnUnmount bool

	// table is the node's mount table as this mounter reports it.
	table []mount.MountPoint
	// devices gives a mount point in table its device number ("major:minor"),
	// which is what says two mounts share an NFS superblock. A mount with no
	// entry gets a device of its own.
	devices map[string]string

	// calls records the plain unmounts and forced escalations in order.
	calls []string
}

func (b *blockingMounter) Unmount(target string) error {
	b.mu.Lock()
	block, err, clears := b.block, b.unmountErr, b.clearsMountOnUnmount
	b.calls = append(b.calls, "umount")
	b.mu.Unlock()

	if block != nil {
		<-block
	}
	if err != nil {
		return err
	}
	if clears {
		b.clear(target)
	}
	return nil
}

func (b *blockingMounter) List() ([]mount.MountPoint, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]mount.MountPoint, len(b.table))
	copy(out, b.table)
	return out, nil
}

func (b *blockingMounter) clear(target string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	kept := b.table[:0]
	for _, mp := range b.table {
		if mp.Path != target {
			kept = append(kept, mp)
		}
	}
	b.table = kept
}

// mountInfo is the node's mountinfo for the same table, for
// NodeServer.mountInfo.
func (b *blockingMounter) mountInfo() ([]mount.MountInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	infos := make([]mount.MountInfo, 0, len(b.table))
	for i, mp := range b.table {
		major, minor := 0, 1000+i
		if dev, ok := b.devices[mp.Path]; ok {
			_, _ = fmt.Sscanf(dev, "%d:%d", &major, &minor)
		}
		infos = append(infos, mount.MountInfo{
			MountPoint: mp.Path, Source: mp.Device, FsType: mp.Type, Major: major, Minor: minor,
		})
	}
	return infos, nil
}

func (b *blockingMounter) record(call string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, call)
}

func (b *blockingMounter) recorded() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]string, len(b.calls))
	copy(out, b.calls)
	return out
}

// newUnmountTestServer builds a NodeServer over the given mounter, with the
// forced rungs replaced by a recorder.
//
// clearsAt names the escalation flag ("-f" or "-l") after which the mount
// vanishes from the table; "" means no forced rung ever clears it.
func newUnmountTestServer(t *testing.T, m *blockingMounter, clearsAt string, forceErr error) *NodeServer {
	t.Helper()

	// FakeMounter: true, so NewDriver does not reach newRealMounter, which
	// panics on any non-Linux host. The server under test is built
	// separately, over the mounter this test controls.
	d, err := NewDriver(Config{
		Name:        "csi.qumulo.com",
		Version:     "test",
		Mode:        ModeNode,
		NodeID:      "node-1",
		FakeMounter: true,
	})
	require.NoError(t, err)

	ns := NewNodeServerWithMounter(d, m)
	ns.mountInfo = m.mountInfo
	ns.unmountTimeout = 20 * time.Millisecond
	ns.statTimeout = 20 * time.Millisecond
	// The array is dead unless a test says otherwise: a statfs on the mount
	// comes back ETIMEDOUT. A timed-out unmount escalates only when this
	// probe also fails; the live-array test replaces it.
	ns.statfsFunc = func(string) (volumeStats, error) {
		return volumeStats{}, &os.PathError{Op: "statfs", Path: "/target", Err: syscall.ETIMEDOUT}
	}
	ns.forceUnmount = func(_ context.Context, flag, target string) error {
		m.record("umount " + flag)
		if forceErr != nil {
			return forceErr
		}
		if clearsAt != "" && flag == clearsAt {
			m.clear(target)
		}
		return nil
	}

	return ns
}

func unpublish(t *testing.T, ns *NodeServer, targetPath string) error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		_, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId:   "qumulo.example.com/1/pvc-a",
			TargetPath: targetPath,
		})
		done <- err
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("NodeUnpublishVolume never returned: the unmount timeout is not bounding the RPC")
		return nil
	}
}

// The measured failure, end to end: a blocked unmount must escalate rather
// than hold the RPC open until the pod is force-deleted.
//
// A disaster-recovery lab test powered the array off for 25 minutes. Pods on hard,
// softerr, softreval and fatal_neterrors mounts all outlived their 120 s grace
// period, because umount(8) blocks whatever the mount options say. This is
// that case with the block made explicit.
func TestUnpublishEscalatesWhenTheUnmountBlocks(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	targetPath := resolvedTempDir(t)
	m := &blockingMounter{block: block, table: []mount.MountPoint{{Path: targetPath}}}
	ns := newUnmountTestServer(t, m, umountForce, nil)

	require.NoError(t, unpublish(t, ns, targetPath))

	assert.Equal(t, []string{"umount", "umount -f"}, m.recorded(),
		"a blocked unmount must escalate to -f, and stop there once the mount is gone")
	assert.Empty(t, m.table, "the RPC may only report OK once the mount table is clear")
	_, err := os.Stat(targetPath)
	assert.True(t, os.IsNotExist(err), "the publish directory must be removed, as before")
}

// A timeout alone does not open the ladder. A live array flushing gigabytes
// of dirty pages at unmount can outlive the bound too, and `umount -f` would
// abort exactly those writes -- so when the server still answers a statfs on
// the mount, the slow unmount is returned to the CO to retry and nothing is
// forced.
func TestUnpublishDoesNotEscalateOnATimeoutWhenTheServerAnswers(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	targetPath := resolvedTempDir(t)
	m := &blockingMounter{block: block, table: []mount.MountPoint{{Path: targetPath}}}
	ns := newUnmountTestServer(t, m, umountForce, nil)
	ns.statfsFunc = func(string) (volumeStats, error) { return volumeStats{TotalBytes: 1}, nil } // the array answers

	err := unpublish(t, ns, targetPath)

	assertCode(t, err, codes.Internal)
	assert.Contains(t, err.Error(), "slow rather than hung")
	assert.Equal(t, []string{"umount"}, m.recorded(), "a slow unmount on a live array is never forced")
	assert.Len(t, m.table, 1, "and the mount is left for the CO's retry")
}

// The dead-array case seen through the probe: the statfs hangs too, the
// probe's own bound expires, and the ladder opens.
func TestUnpublishEscalatesOnATimeoutWhenTheStatfsHangsToo(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	targetPath := resolvedTempDir(t)
	m := &blockingMounter{block: block, table: []mount.MountPoint{{Path: targetPath}}}
	ns := newUnmountTestServer(t, m, umountForce, nil)
	ns.statfsFunc = func(string) (volumeStats, error) { <-block; return volumeStats{}, nil } // hangs like the unmount

	require.NoError(t, unpublish(t, ns, targetPath))

	assert.Equal(t, []string{"umount", "umount -f"}, m.recorded())
	assert.Empty(t, m.table)
}

// The full ladder, and the reason for the third rung: `umount -f` gives up on
// a server that never answers, but it does not detach a mount the kernel still
// holds references to -- which is exactly what force-deleting a pod leaves
// behind, and what the lab found still mounted on the node afterwards.
func TestUnpublishFallsThroughToALazyUnmount(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	targetPath := resolvedTempDir(t)
	m := &blockingMounter{block: block, table: []mount.MountPoint{{Path: targetPath}}}
	ns := newUnmountTestServer(t, m, umountLazy, nil)

	require.NoError(t, unpublish(t, ns, targetPath))

	assert.Equal(t, []string{"umount", "umount -f", "umount -l"}, m.recorded())
	assert.Empty(t, m.table)
}

// Every rung failing is Internal, not a fabricated OK.
//
// Reporting success with the mount still on the node is the worst outcome
// available: the CO stops retrying, the kubelet deletes its bookkeeping, and
// the stale mount is left for the next pod scheduled here to trip over.
func TestUnpublishReportsAMountItCouldNotClear(t *testing.T) {
	targetPath := resolvedTempDir(t)
	m := &blockingMounter{
		unmountErr: errors.New("umount.nfs4: /target: Server not responding"),
		table:      []mount.MountPoint{{Path: targetPath}},
	}
	ns := newUnmountTestServer(t, m, "", errors.New("umount: /target: not mounted?"))

	err := unpublish(t, ns, targetPath)

	assertCode(t, err, codes.Internal)
	assert.Contains(t, err.Error(), "still in the mount table")
	assert.Equal(t, []string{"umount", "umount -f", "umount -l"}, m.recorded())
}

// umount(8) exiting 0 is not the same claim as the mount being gone.
//
// The RPC's contract to the CO is about the node's state, so the mount table
// is re-read after every rung including a successful one. Without this, a
// umount that returns 0 and leaves the mount behind -- which is what a lazy
// unmount of a busy tree looks like from the outside -- would be reported as a
// clean unpublish.
func TestUnpublishDoesNotTrustAZeroExitFromUmount(t *testing.T) {
	targetPath := resolvedTempDir(t)
	m := &blockingMounter{
		clearsMountOnUnmount: false, // succeeds, but the table still lists it
		table:                []mount.MountPoint{{Path: targetPath}},
	}
	ns := newUnmountTestServer(t, m, umountForce, nil)

	require.NoError(t, unpublish(t, ns, targetPath))

	assert.Equal(t, []string{"umount", "umount -f"}, m.recorded(),
		"a successful umount that left the mount listed must still escalate")
}

// A plain unmount that works is the ordinary case and must stay ordinary: no
// force, no lazy, no umount(8) exec at all.
func TestUnpublishDoesNotEscalateWhenThePlainUnmountWorks(t *testing.T) {
	targetPath := resolvedTempDir(t)
	m := &blockingMounter{
		clearsMountOnUnmount: true,
		table:                []mount.MountPoint{{Path: targetPath}},
	}
	ns := newUnmountTestServer(t, m, umountForce, nil)

	require.NoError(t, unpublish(t, ns, targetPath))

	assert.Equal(t, []string{"umount"}, m.recorded())
}

// "target is busy" is a live volume on this node, not a dead array.
//
// This is the risk the escalation creates, guarded directly: a lazy unmount of
// a mount something still holds open detaches it from the tree while the
// writer keeps writing into it. The CO retries NodeUnpublishVolume, and the
// retry is the right answer -- so, while the server still answers a statfs,
// the error is returned and no force is attempted.
func TestUnpublishDoesNotForcePastABusyMount(t *testing.T) {
	targetPath := resolvedTempDir(t)
	m := &blockingMounter{
		unmountErr: errors.New("umount: /target: target is busy"),
		table:      []mount.MountPoint{{Path: targetPath}},
	}
	ns := newUnmountTestServer(t, m, umountForce, nil)
	ns.statfsFunc = func(string) (volumeStats, error) { return volumeStats{TotalBytes: 1}, nil } // the array answers

	err := unpublish(t, ns, targetPath)

	assertCode(t, err, codes.Internal)
	assert.Equal(t, []string{"umount"}, m.recorded(), "a busy mount must never be force-detached")
	assert.Len(t, m.table, 1, "and must still be mounted afterwards")
}

// Measured on RHCOS 9.8 and kind: a volume-stats statfs left blocked in
// the kernel on the dead mount holds it, so umount answers "busy" (exit 16) at
// once instead of hanging, and the pod stayed Terminating until the array
// returned. Busy with a server that fails the statfs is the dead array, and
// the ladder runs.
func TestUnpublishForcesABusyMountWhoseServerIsGone(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		statfs error
	}{
		{"busy, statfs times out", errors.New("exit status 16: umount.nfs4: /target: device is busy"), syscall.ETIMEDOUT},
		{"exit 16 with no output, statfs EIO", errors.New("exit status 16"), syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			targetPath := resolvedTempDir(t)
			m := &blockingMounter{
				unmountErr: tc.err,
				table:      []mount.MountPoint{{Path: targetPath}},
			}
			ns := newUnmountTestServer(t, m, umountForce, nil)
			ns.statfsFunc = func(string) (volumeStats, error) {
				return volumeStats{}, &os.PathError{Op: "statfs", Path: targetPath, Err: tc.statfs}
			}

			require.NoError(t, unpublish(t, ns, targetPath))

			assert.Equal(t, []string{"umount", "umount -f"}, m.recorded())
			assert.Empty(t, m.table)
		})
	}
}

// Any other plain failure on a server that answers is returned unchanged, as
// before: only the server failing the statfs opens the ladder.
func TestUnpublishDoesNotForcePastAnyFailureWhileTheServerAnswers(t *testing.T) {
	targetPath := resolvedTempDir(t)
	m := &blockingMounter{
		unmountErr: errors.New("exit status 32: umount: /target: must be superuser to unmount"),
		table:      []mount.MountPoint{{Path: targetPath}},
	}
	ns := newUnmountTestServer(t, m, umountForce, nil)
	ns.statfsFunc = func(string) (volumeStats, error) {
		return volumeStats{}, &os.PathError{Op: "statfs", Path: targetPath, Err: syscall.EACCES} // an answer
	}

	err := unpublish(t, ns, targetPath)

	assertCode(t, err, codes.Internal)
	assert.Equal(t, []string{"umount"}, m.recorded())
	assert.Len(t, m.table, 1)
}

// A path the mount table does not list needs no unmount at all, and must not
// exec umount(8) to find that out. This is the CO retrying an unpublish that
// already succeeded, which the spec requires to return OK.
func TestUnpublishOfAnUnmountedPathTouchesNothing(t *testing.T) {
	targetPath := resolvedTempDir(t)
	m := &blockingMounter{}
	ns := newUnmountTestServer(t, m, umountForce, nil)

	require.NoError(t, unpublish(t, ns, targetPath))

	assert.Empty(t, m.recorded())
}

// Which failures mean "the server is gone" and which mean "something here is
// still using it". The whole safety of the ladder rests on this predicate, so
// it is tested as a unit rather than only through the RPC.
func TestUnmountEscalationTriggers(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		// A timeout says nothing about the server on its own: the ladder
		// asks the server (mountServerUnresponsive) before treating it as
		// gone. See TestUnpublishDoesNotEscalateOnATimeoutWhenTheServerAnswers.
		{
			name: "the unmount outlived its own timeout",
			err:  fmt.Errorf("unmounting /t: %w after 30s", errMountTimeout),
			want: false,
		},
		{name: "wrapped timeout sentinel", err: errors.Join(errMountTimeout), want: false},
		{name: "umount.nfs server not responding", err: errors.New("umount.nfs4: /t: Server not responding"), want: true},
		{name: "connection timed out", err: errors.New("umount.nfs: /t: Connection timed out"), want: true},
		{name: "stale handle", err: errors.New("umount.nfs4: /t: Stale file handle"), want: true},
		{name: "no route to host", err: errors.New("umount.nfs: No route to host"), want: true},
		{name: "ESTALE errno", err: &os.PathError{Op: "umount", Path: "/t", Err: syscall.ESTALE}, want: true},
		{name: "EBADHANDLE errno", err: &os.PathError{Op: "umount", Path: "/t", Err: errEBADHANDLE}, want: true},
		{name: "ETIMEDOUT errno", err: &os.PathError{Op: "umount", Path: "/t", Err: syscall.ETIMEDOUT}, want: true},
		// The two that must not escalate: both mean a live reference on this
		// node, which forcing would hide rather than fix.
		{name: "target is busy", err: errors.New("umount: /t: target is busy"), want: false},
		{name: "not mounted", err: errors.New("umount: /t: not mounted"), want: false},
		{name: "permission denied", err: errors.New("umount: /t: must be superuser to unmount"), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shouldEscalateUnmount(tc.err))
		})
	}
}

// The timeout is configurable end to end, because 30 s is a default and not a
// law: a distant cluster may deserve longer, and the chart exposes it.
func TestUnmountTimeoutComesFromTheDriverConfig(t *testing.T) {
	d, err := NewDriver(Config{
		Name:           "csi.qumulo.com",
		Version:        "test",
		Mode:           ModeNode,
		NodeID:         "node-1",
		FakeMounter:    true,
		UnmountTimeout: 7 * time.Second,
	})
	require.NoError(t, err)
	assert.Equal(t, 7*time.Second, d.NodeServer().unmountTimeout)

	d, err = NewDriver(Config{
		Name:        "csi.qumulo.com",
		Version:     "test",
		Mode:        ModeNode,
		NodeID:      "node-1",
		FakeMounter: true,
	})
	require.NoError(t, err)
	assert.Equal(t, defaultUnmountTimeout, d.NodeServer().unmountTimeout,
		"an unset timeout must take the package default, not zero")
}

// The kubelet gives NodeUnpublishVolume a fixed two minutes. With
// unmountTimeout at or past that, the deadline ended the call before the
// plain unmount's own timeout did, so the ladder -- which opens on that
// timeout -- never ran, and a pod on a dead array never terminated. The plain
// rung is now cut to leave the escalation room inside the caller's deadline.
func TestUnmountEscalatesInsideTheCallersDeadline(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	target := resolvedTempDir(t)
	m := &blockingMounter{block: block, table: []mount.MountPoint{{Path: target}}}
	ns := newUnmountTestServer(t, m, umountLazy, nil) // -l clears it
	ns.unmountTimeout = time.Hour

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: "v", TargetPath: target})

	require.NoError(t, err)
	assert.Equal(t, []string{"umount", "umount -f", "umount -l"}, m.recorded())
	assert.Empty(t, m.table, "the mount is gone")
}

// The budget itself: the configured timeout when the deadline has room for
// it and the reserve, less when it does not, and never more than the time
// left.
func TestPlainUnmountBudget(t *testing.T) {
	ns := &NodeServer{unmountTimeout: 30 * time.Second, statTimeout: 10 * time.Second}

	assert.Equal(t, 30*time.Second, ns.plainUnmountBudget(context.Background()), "no deadline")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	assert.Equal(t, 30*time.Second, ns.plainUnmountBudget(ctx), "the kubelet's deadline has room for the default")

	ns.unmountTimeout = 2 * time.Minute
	got := ns.plainUnmountBudget(ctx)
	assert.Less(t, got, 2*time.Minute-19*time.Second, "statfs and both forced rungs are kept back")
	assert.Greater(t, got, 90*time.Second)
}

// hangingMountMounter blocks every Mount until release is closed, and counts
// them: a mount(8) against an array that stopped answering mid-negotiation.
type hangingMountMounter struct {
	mount.FakeMounter
	release chan struct{}
	mounts  atomic.Int32
}

func (h *hangingMountMounter) Mount(string, string, string, []string) error {
	h.mounts.Add(1)
	<-h.release
	return nil
}

// A kubelet retry of a publish whose mount(8) outlived its timeout
// started another mount beside the one still blocked, and each retry of an
// unpublish whose umount hung started another umount: one thread, and one
// process, per attempt while the array was slow. A retry now waits on the
// call in flight for the same target.
func TestRetriesJoinTheMountOrUnmountStillInFlight(t *testing.T) {
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeNode, NodeID: "n", FakeMounter: true})
	require.NoError(t, err)

	t.Run("mount", func(t *testing.T) {
		m := &hangingMountMounter{release: make(chan struct{})}
		t.Cleanup(func() { close(m.release) })
		ns := NewNodeServerWithMounter(d, m)
		ns.mountTimeout = 20 * time.Millisecond
		for range 5 {
			err := ns.mountWithTimeout(context.Background(), "192.0.2.10:/v", "/target", "nfs", nil)
			require.ErrorIs(t, err, errMountTimeout)
		}
		assert.Equal(t, int32(1), m.mounts.Load(), "five attempts ran one mount(8)")
	})

	t.Run("unmount", func(t *testing.T) {
		m := &blockingMounter{block: make(chan struct{})}
		t.Cleanup(func() { close(m.block) })
		ns := NewNodeServerWithMounter(d, m)
		for range 5 {
			err := ns.unmountWithTimeoutOf(context.Background(), "/target", 20*time.Millisecond)
			require.ErrorIs(t, err, errMountTimeout)
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		assert.Equal(t, []string{"umount"}, m.calls, "five attempts ran one umount(8)")
	})
}

// Umount(8) forks the umount.nfs helper, which inherits the output
// pipe. When a forced rung's deadline passed, only umount was killed, and
// the call then waited for the orphaned helper to close the pipe -- so the
// rung's bound was not enforced: each call took as long as the helper lived.
// The rung now kills its whole process group, and stops waiting on the pipe
// soon after even for a helper that left the group.
func TestAForcedRungKeepsItsDeadlineWhenUmountForksAHelper(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process groups and a shell umount are what is under test")
	}
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is needed to build a helper that leaves the group")
	}
	for _, tc := range []struct {
		name   string
		helper string
		within time.Duration
	}{
		// Killed with the group: the rung ends at its deadline.
		{name: "a helper in the group", helper: "sleep 30 &", within: 1500 * time.Millisecond},
		// Not killed (a new session), so only WaitDelay lets the pipe go.
		{name: "a helper that left the group", helper: "setsid sleep 8 &", within: 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin := t.TempDir()
			script := "#!/bin/sh\n" + tc.helper + "\nwait\n"
			require.NoError(t, os.WriteFile(filepath.Join(bin, "umount"), []byte(script), 0o755))
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

			d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeNode, NodeID: "n", FakeMounter: true})
			require.NoError(t, err)
			ns := NewNodeServerWithMounter(d, &blockingMounter{})
			ns.unmountTimeout = 200 * time.Millisecond

			start := time.Now()
			err = ns.execUmount(context.Background(), "-f", "/target")
			elapsed := time.Since(start)

			require.Error(t, err)
			assert.Less(t, elapsed, tc.within, "the rung is bounded by its deadline, not by the helper's lifetime")
		})
	}
}
