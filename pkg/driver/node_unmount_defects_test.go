//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Defect tests for the node plugin's unpublish path (the unmount ladder).
// Each asserts the CORRECT behaviour, so it fails while its defect is
// present. Fakes only: no real mount, no sudo. One test execs a
// fake `umount` shell script placed first on PATH; it never runs umount(8).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

// ---------------------------------------------------------------- helpers

// dfxSharedDevice is the device number both mounts in dfxSharedServerTable
// carry: two NFS mounts from one server with the same options share one
// superblock, which mountinfo shows as one major:minor.
const dfxSharedDevice = 177

// dfxSharedServerTable is the volume being unpublished and a second pod's
// volume, both NFS mounts from the same server with the same options.
func dfxSharedServerTable(target string) []mount.MountPoint {
	return []mount.MountPoint{
		{Path: target, Device: "192.0.2.10:/csi/volumes/pvc-a", Type: "nfs4", Opts: []string{"rw", "hard", "timeo=600"}},
		{Path: "/var/lib/kubelet/pods/other-pod/volumes/kubernetes.io~csi/pvc-b/mount",
			Device: "192.0.2.10:/csi/volumes/pvc-b", Type: "nfs4", Opts: []string{"rw", "hard", "timeo=600"}},
	}
}

// dfxInjectSharedMountInfo makes the node server's mountinfo reader, if it
// has one (a func() ([]mount.MountInfo, error) field, whatever its name),
// report every mount in m's table on one device. A fix that tells sharing
// apart by device number then sees what the kernel would show; a fix that
// compares the mount table's server and options sees it from List(). The
// field is set through reflect so this compiles on a tree that has none.
func dfxInjectSharedMountInfo(t *testing.T, ns *NodeServer, m *blockingMounter) {
	t.Helper()
	reader := func() ([]mount.MountInfo, error) {
		mps, _ := m.List()
		infos := make([]mount.MountInfo, 0, len(mps))
		for _, mp := range mps {
			infos = append(infos, mount.MountInfo{
				MountPoint: mp.Path, Source: mp.Device, FsType: mp.Type, Major: 0, Minor: dfxSharedDevice,
			})
		}
		return infos, nil
	}
	want := reflect.TypeOf(reader)
	v := reflect.ValueOf(ns).Elem()
	for i := range v.NumField() {
		f := v.Field(i)
		if f.Type() == want {
			reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(reader))
			t.Logf("injected shared-device mountinfo into NodeServer.%s", v.Type().Field(i).Name)
		}
	}
}

// dfxAssertNoForcedUnmount fails if `umount -f` ran.
func dfxAssertNoForcedUnmount(t *testing.T, m *blockingMounter) {
	t.Helper()
	calls := m.recorded()
	t.Logf("unmount calls: %v", calls)
	if slices.Contains(calls, "umount "+umountForce) {
		t.Errorf("ran `umount %s` while another pod's mount of the same server was listed; "+
			"that aborts the other pod's in-flight I/O. Want `umount %s` alone for the target",
			umountForce, umountLazy)
	}
}

func dfxCount(calls []string, call string) int {
	n := 0
	for _, c := range calls {
		if c == call {
			n++
		}
	}
	return n
}

// ----------------------------------------------------------------

// With unmountTimeout far above the kubelet's NodeUnpublishVolume
// deadline, the escalation must still run inside that deadline and clear the
// mount from a dead array, so the pod terminates. A 200 ms caller deadline
// stands in for the kubelet's 2 min; 1 h for unmountTimeout "2m".
func TestDefect_UnmountTimeoutAboveTheKubeletDeadlineStillEscalates(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	target := resolvedTempDir(t)
	m := &blockingMounter{block: block, table: []mount.MountPoint{{Path: target}}}
	ns := newUnmountTestServer(t, m, umountLazy, nil) // -l clears it
	ns.unmountTimeout = time.Hour

	var lastErr error
	for i := range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, lastErr = ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: "v", TargetPath: target})
		cancel()
		t.Logf("attempt %d: %v", i, lastErr)
		if lastErr == nil {
			break
		}
	}
	calls := m.recorded()
	t.Logf("unmount calls: %v", calls)
	assert.NoError(t, lastErr, "no attempt cleared the mount inside the caller's deadline")
	mps, _ := m.List()
	assert.Empty(t, mps, "the mount is still listed after every attempt: the ladder never ran")
}

// ----------------------------------------------------------------

// A hung unmount on an unreachable array must not run `umount -f`
// while another pod's mount shares the target's NFS superblock.
// Ported from code-tests TestOpen_UnpublishOfAHungMountDoesNotForceASharedSuperblock.
func TestDefect_UnpublishOfAHungMountDoesNotForceASharedSuperblock(t *testing.T) {
	target := resolvedTempDir(t)
	m := &blockingMounter{block: make(chan struct{}), table: dfxSharedServerTable(target)}
	defer close(m.block)
	ns := newUnmountTestServer(t, m, umountLazy, nil) // the array fails its statfs
	dfxInjectSharedMountInfo(t, ns, m)

	_ = unpublish(t, ns, target)
	dfxAssertNoForcedUnmount(t, m)
}

// The same, when the unmount fails with "Stale file handle".
// Ported from code-tests TestOpen_UnpublishAfterAStaleHandleDoesNotForceASharedSuperblock.
func TestDefect_UnpublishAfterAStaleHandleDoesNotForceASharedSuperblock(t *testing.T) {
	target := resolvedTempDir(t)
	m := &blockingMounter{
		unmountErr: errors.New("exit status 16: umount.nfs4: " + target + ": Stale file handle"),
		table:      dfxSharedServerTable(target),
	}
	ns := newUnmountTestServer(t, m, umountLazy, nil)
	dfxInjectSharedMountInfo(t, ns, m)

	_ = unpublish(t, ns, target)
	dfxAssertNoForcedUnmount(t, m)
}

// The same, when umount exits 0 but the target is still listed.
// Ported from code-tests TestOpen_UnpublishWithTheTargetStillListedDoesNotForceASharedSuperblock.
func TestDefect_UnpublishWithTheTargetStillListedDoesNotForceASharedSuperblock(t *testing.T) {
	target := resolvedTempDir(t)
	m := &blockingMounter{clearsMountOnUnmount: false, table: dfxSharedServerTable(target)}
	ns := newUnmountTestServer(t, m, umountLazy, nil)
	dfxInjectSharedMountInfo(t, ns, m)

	_ = unpublish(t, ns, target)
	dfxAssertNoForcedUnmount(t, m)
}

// ---------------------------------------------------------------- (unpublish)

// Unpublish retries while the plain umount is still blocked (and
// the server still answers a statfs, so nothing escalates) must wait on that
// umount, not start another umount(8) each.
func TestDefect_UnpublishRetriesDoNotStartAnotherUmount(t *testing.T) {
	target := resolvedTempDir(t)
	m := &blockingMounter{block: make(chan struct{}), table: []mount.MountPoint{{Path: target}}}
	defer close(m.block)
	ns := newUnmountTestServer(t, m, umountLazy, nil)
	ns.statfsFunc = func(string) (volumeStats, error) { return volumeStats{}, nil } // the server answers

	before := runtime.NumGoroutine()
	const attempts = 10
	for range attempts {
		require.Error(t, unpublish(t, ns, target), "a blocked unmount on a live array is returned to the CO")
	}
	grown := runtime.NumGoroutine() - before
	calls := m.recorded()
	t.Logf("%d unpublish attempts: calls %v, goroutine growth %d", attempts, calls, grown)
	assert.Equal(t, 1, dfxCount(calls, "umount"), "each retry started another umount(8) beside the blocked one")
	assert.LessOrEqual(t, grown, 5)
}

// ----------------------------------------------------------------

// dfxFakeUmount puts a `umount` shell script first on PATH whose body is
// helper (a forked child that inherits the output pipe, as umount.nfs does),
// then waits for it.
func dfxFakeUmount(t *testing.T, helper string) string {
	t.Helper()
	bin := t.TempDir()
	pidFile := filepath.Join(bin, "helper.pid")
	script := "#!/bin/sh\n" + helper + "\necho $! > " + pidFile + "\nwait\n"
	require.NoError(t, os.WriteFile(filepath.Join(bin, "umount"), []byte(script), 0o755))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return pidFile
}

// dfxProcessRunning reports whether pid is alive and not a zombie.
func dfxProcessRunning(pid int) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	s := string(data)
	if i := strings.LastIndexByte(s, ')'); i >= 0 && i+2 < len(s) {
		return s[i+2] != 'Z'
	}
	return true
}

// A forced unmount rung returns at its deadline even when
// umount(8) forked a helper that holds the output pipe: the rung's bound is
// enforced (process-group kill and/or exec.Cmd.WaitDelay), and a helper in the
// group is not left running. The rung is the server's default forceUnmount.
func TestDefect_ForcedRungKeepsItsDeadlineWhenUmountForksAHelper(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Fatalf("this test drives a shell umount and /proc; it needs Linux, got %s", runtime.GOOS)
	}
	_, err := os.Stat("/usr/bin/setsid")
	require.NoError(t, err, "setsid is needed for the helper-outside-the-group case")

	const rungTimeout = 300 * time.Millisecond
	const bound = 4 * time.Second // the deadline plus room for a WaitDelay of a few seconds
	for _, tc := range []struct {
		name, helper string
		inGroup      bool
	}{
		{name: "helper in the process group", helper: "sleep 9 &", inGroup: true},
		{name: "helper that left the group", helper: "setsid sleep 9 &"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pidFile := dfxFakeUmount(t, tc.helper)
			ns := dfxNodeServer(t, &blockingMounter{}, nil)
			ns.unmountTimeout = rungTimeout

			start := time.Now()
			err := ns.forceUnmount(context.Background(), umountForce, "/dfx/target")
			elapsed := time.Since(start)
			t.Logf("rung returned after %s: %v", elapsed.Round(time.Millisecond), err)

			assert.Error(t, err, "the rung outlived its deadline and must say so")
			assert.Less(t, elapsed, bound, "the rung waited on the forked helper instead of its %s deadline", rungTimeout)
			if tc.inGroup {
				data, rerr := os.ReadFile(pidFile)
				require.NoError(t, rerr)
				pid, perr := strconv.Atoi(strings.TrimSpace(string(data)))
				require.NoError(t, perr)
				assert.Eventually(t, func() bool { return !dfxProcessRunning(pid) }, 2*time.Second, 20*time.Millisecond,
					"the helper forked by umount is still running after the rung's deadline")
			}
		})
	}
}

// Orphaned mount/umount helpers are re-parented to the node
// container's PID 1, so PID 1 must reap them: the node image runs an init
// (tini, dumb-init, catatonit) in front of the driver, or the node DaemonSet
// shares the pod's process namespace (pause reaps). The driver as PID 1
// leaves every orphan a zombie.
func TestDefect_NodeImageReapsOrphanedHelpers(t *testing.T) {
	dockerfile := dfxReadRepoFile(t, "Dockerfile")
	stage := dfxDockerStage(dockerfile, "node")
	require.NotEmpty(t, stage, "no `AS node` stage in the Dockerfile")

	var entry []string
	for _, line := range strings.Split(stage, "\n") {
		if l := strings.TrimSpace(line); strings.HasPrefix(l, "ENTRYPOINT") {
			entry = append(entry, l)
		}
	}
	t.Logf("node stage ENTRYPOINT: %v", entry)
	last := ""
	if len(entry) > 0 {
		last = entry[len(entry)-1]
	}
	for _, init := range []string{"tini", "dumb-init", "catatonit", "docker-init"} {
		if strings.Contains(last, init) {
			return
		}
	}

	node := dfxReadRepoFile(t, "charts/qumulo-csi-driver/templates/node.yaml")
	if strings.Contains(node, "shareProcessNamespace: true") {
		return
	}
	t.Errorf("the node image's PID 1 is the driver (%q) and the DaemonSet does not share the process "+
		"namespace: an orphaned umount.nfs helper becomes a permanent zombie", last)
}

// dfxDockerStage returns the Dockerfile text of the stage named name, from its
// FROM line up to the next FROM.
func dfxDockerStage(dockerfile, name string) string {
	lines := strings.Split(dockerfile, "\n")
	start := -1
	for i, line := range lines {
		l := strings.ToLower(strings.TrimSpace(line))
		if !strings.HasPrefix(l, "from ") {
			continue
		}
		if start >= 0 {
			return strings.Join(lines[start:i], "\n")
		}
		if strings.HasSuffix(l, " as "+name) {
			start = i
		}
	}
	if start >= 0 {
		return strings.Join(lines[start:], "\n")
	}
	return ""
}

// ----------------------------------------------------------------

// A target that is no longer a mount but holds files (writes that
// landed on the node's disk) must not fail every unpublish with Internal, and
// must not delete the data. Completing the RPC is not enough on its own: the
// kubelet's TearDownAt then removes vol_data.json and rmdir(2)s the target's
// parent, so the data has to leave the kubelet's pods directory, or the pod
// stays Terminating on the kubelet's error instead of the driver's.
func TestDefect_TargetHoldingDataOnTheNodeDiskCanBeUnpublished(t *testing.T) {
	kubeletDir, pvDir, target := kubeletPublishTarget(t, "0d5c-uid", "pvc-d112")
	const content = "written after the NFS mount went away"
	require.NoError(t, os.WriteFile(filepath.Join(target, "data.txt"), []byte(content), 0o600))
	ns := dfxNodeServer(t, mount.NewFakeMounter(nil), nil) // nothing mounted at target

	var codesSeen []codes.Code
	for range 3 {
		_, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "v", TargetPath: target})
		codesSeen = append(codesSeen, status.Code(err))
		t.Logf("unpublish: %v", err)
		if err == nil {
			break
		}
	}
	require.Equal(t, codes.OK, codesSeen[len(codesSeen)-1],
		"every unpublish of a target holding data answers %v; the pod stays Terminating for ever", codesSeen)
	assertKubeletCanTearDown(t, pvDir)

	found := ""
	_ = filepath.WalkDir(kubeletDir, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if b, rerr := os.ReadFile(p); rerr == nil && string(b) == content {
				found = p
			}
		}
		return nil
	})
	require.NotEmpty(t, found, "the data written to the node's disk was deleted")
	assert.True(t, strings.HasPrefix(found, filepath.Join(kubeletDir, "csi.qumulo.com-orphaned", "pvc-d112-0d5c-uid-")),
		"the data was kept, but at %s rather than the driver's orphaned directory", found)
}
