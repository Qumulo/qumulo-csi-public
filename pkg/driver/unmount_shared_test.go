// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

// These tests avoid "Node" in their names, like node_unmount_test.go, so the
// darwin skip pattern does not swallow them.
//
// In each, the volume being unpublished and a second pod's volume are NFS
// mounts from the same server with the same options, which on Linux share
// one superblock -- one device number in mountinfo: `umount -f` on one aborts
// the in-flight requests of both. Only the first mount is being
// unpublished.

const otherPodMount = "/var/lib/kubelet/pods/other-pod/volumes/kubernetes.io~csi/pvc-b/mount"

// sharedSuperblock is the device map that puts target and the other pod's
// mount on one superblock, as the lab's mountinfo showed (0:357).
func sharedSuperblock(target string) map[string]string {
	return map[string]string{target: "0:357", otherPodMount: "0:357"}
}

func sharedServerTable(target string) []mount.MountPoint {
	return []mount.MountPoint{
		{Path: target, Device: "192.0.2.10:/csi/volumes/pvc-a", Type: "nfs4", Opts: []string{"rw", "hard", "timeo=600"}},
		{Path: otherPodMount,
			Device: "192.0.2.10:/csi/volumes/pvc-b", Type: "nfs4", Opts: []string{"rw", "hard", "timeo=600"}},
	}
}

func assertNoForcedUnmount(t *testing.T, m *blockingMounter) {
	t.Helper()
	calls := m.recorded()
	t.Logf("unmount calls: %v", calls)
	if slices.Contains(calls, "umount "+umountForce) {
		t.Errorf("ran `umount %s` while another pod's mount of the same server was listed; "+
			"that aborts the other pod's in-flight I/O. Want `umount %s` alone for the target",
			umountForce, umountLazy)
	}
}

// A hung unmount on an unreachable array must not run `umount -f` while another mount shares the server.
func TestUnpublishOfAHungMountDoesNotForceASharedSuperblock(t *testing.T) {
	target := resolvedTempDir(t)
	m := &blockingMounter{block: make(chan struct{}), table: sharedServerTable(target), devices: sharedSuperblock(target)}
	defer close(m.block)
	ns := newUnmountTestServer(t, m, umountLazy, nil) // the array fails its statfs

	_ = unpublish(t, ns, target)
	assertNoForcedUnmount(t, m)
}

// An unmount failing with "Stale file handle" must not run `umount -f` while another mount shares the server.
func TestUnpublishAfterAStaleHandleDoesNotForceASharedSuperblock(t *testing.T) {
	target := resolvedTempDir(t)
	m := &blockingMounter{
		unmountErr: errors.New("exit status 16: umount.nfs4: " + target + ": Stale file handle"),
		table:      sharedServerTable(target),
		devices:    sharedSuperblock(target),
	}
	ns := newUnmountTestServer(t, m, umountLazy, nil)

	_ = unpublish(t, ns, target)
	assertNoForcedUnmount(t, m)
}

// An unmount that exits 0 with the target still listed must not run `umount -f` while another mount
// shares the server.
func TestUnpublishWithTheTargetStillListedDoesNotForceASharedSuperblock(t *testing.T) {
	target := resolvedTempDir(t)
	m := &blockingMounter{clearsMountOnUnmount: false, table: sharedServerTable(target), devices: sharedSuperblock(target)}
	ns := newUnmountTestServer(t, m, umountLazy, nil)

	_ = unpublish(t, ns, target)
	assertNoForcedUnmount(t, m)
}

// The same server through other mount options is another superblock, which
// -f does not reach, so the ladder still forces a target that is alone on
// its own device.
func TestUnpublishStillForcesATargetAloneOnItsSuperblock(t *testing.T) {
	target := resolvedTempDir(t)
	m := &blockingMounter{
		unmountErr: errors.New("exit status 16: umount.nfs4: " + target + ": Stale file handle"),
		table:      sharedServerTable(target),
		devices:    map[string]string{target: "0:357", otherPodMount: "0:358"},
	}
	ns := newUnmountTestServer(t, m, umountForce, nil)

	require.NoError(t, unpublish(t, ns, target))
	assert.Contains(t, m.recorded(), "umount "+umountForce)
}

// A mount being made on the node may be joining the target's superblock, so
// -f waits for nothing and is skipped; -l still releases the target.
func TestUnpublishSkipsTheForcedUnmountWhileAMountIsInFlight(t *testing.T) {
	target := resolvedTempDir(t)
	m := &blockingMounter{
		unmountErr: errors.New("exit status 16: umount.nfs4: " + target + ": Stale file handle"),
		table:      []mount.MountPoint{{Path: target, Device: "192.0.2.10:/csi/volumes/pvc-a", Type: "nfs4"}},
	}
	ns := newUnmountTestServer(t, m, umountLazy, nil)
	ns.forceGate.RLock()
	defer ns.forceGate.RUnlock()

	require.NoError(t, unpublish(t, ns, target))
	assertNoForcedUnmount(t, m)
	assert.Contains(t, m.recorded(), "umount "+umountLazy)
}

// When mountinfo cannot be read, sharing cannot be ruled out, so -f is
// skipped rather than risked.
func TestUnpublishSkipsTheForcedUnmountWhenMountinfoIsUnreadable(t *testing.T) {
	target := resolvedTempDir(t)
	m := &blockingMounter{
		unmountErr: errors.New("exit status 16: umount.nfs4: " + target + ": Stale file handle"),
		table:      []mount.MountPoint{{Path: target, Device: "192.0.2.10:/csi/volumes/pvc-a", Type: "nfs4"}},
	}
	ns := newUnmountTestServer(t, m, umountLazy, nil)
	ns.mountInfo = func() ([]mount.MountInfo, error) { return nil, errors.New("permission denied") }

	require.NoError(t, unpublish(t, ns, target))
	assertNoForcedUnmount(t, m)
	assert.Contains(t, m.recorded(), "umount "+umountLazy)
}

// kubeletPublishTarget builds the layout the kubelet publishes a CSI volume
// into, <tmp>/kubelet/pods/<uid>/volumes/kubernetes.io~csi/<pv>/mount with
// the vol_data.json the kubelet writes beside it, and returns the kubelet
// directory, the <pv> directory and the target.
func kubeletPublishTarget(t *testing.T, podUID, pv string) (kubeletDir, pvDir, target string) {
	t.Helper()
	kubeletDir = filepath.Join(resolvedTempDir(t), "kubelet")
	pvDir = filepath.Join(kubeletDir, "pods", podUID, "volumes", "kubernetes.io~csi", pv)
	target = filepath.Join(pvDir, "mount")
	require.NoError(t, os.MkdirAll(target, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(pvDir, "vol_data.json"), []byte(`{"driverName":"csi.qumulo.com"}`), 0o600))
	return kubeletDir, pvDir, target
}

// assertKubeletCanTearDown does what the kubelet's removeMountDir does after
// NodeUnpublishVolume answers OK -- remove vol_data.json, then rmdir(2) the
// <pv> directory -- and fails if anything the driver left there stops it.
func assertKubeletCanTearDown(t *testing.T, pvDir string) {
	t.Helper()
	entries, err := os.ReadDir(pvDir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.Equal(t, []string{"vol_data.json"}, names, "the driver left something in the kubelet's volume directory")
	require.NoError(t, os.Remove(filepath.Join(pvDir, "vol_data.json")))
	assert.NoError(t, os.Remove(pvDir), "the kubelet's teardown would fail and the pod stay Terminating")
}

// A target that is no longer a mount but holds files -- writes that
// reached the node's disk after the mount went away -- is moved out of the
// kubelet's pods directory into the driver's own, its data kept, and the
// unpublish completes; the kubelet's own teardown can then complete too.
func TestUnpublishMovesAsideATargetHoldingDataOnTheNodesDisk(t *testing.T) {
	kubeletDir, pvDir, target := kubeletPublishTarget(t, "uid-1", "pvc-a")
	require.NoError(t, os.WriteFile(filepath.Join(target, "written-after-the-mount-went"), []byte("data"), 0o600))
	m := &blockingMounter{} // nothing mounted at the target
	ns := newUnmountTestServer(t, m, umountLazy, nil)

	require.NoError(t, unpublish(t, ns, target))

	_, err := os.Stat(target)
	assert.True(t, os.IsNotExist(err), "the target itself is gone")
	assertKubeletCanTearDown(t, pvDir)

	orphaned := filepath.Join(kubeletDir, "csi.qumulo.com-orphaned")
	aside, err := filepath.Glob(filepath.Join(orphaned, "pvc-a-uid-1-*"))
	require.NoError(t, err)
	require.Len(t, aside, 1, "the target was moved into the driver's orphaned directory")
	data, err := os.ReadFile(filepath.Join(aside[0], "written-after-the-mount-went"))
	require.NoError(t, err)
	assert.Equal(t, "data", string(data), "nothing was deleted")
	fi, err := os.Stat(orphaned)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), fi.Mode().Perm())

	// Beside the plugin directory, never inside it: <kubeletDir>/plugins/<driver>
	// holds csi.sock and is what an operator removes to clean a node after
	// uninstalling the driver. Rescued data in there is deleted by that.
	_, err = os.Stat(filepath.Join(kubeletDir, "plugins", "csi.qumulo.com"))
	assert.True(t, os.IsNotExist(err), "the rescued data was put inside the plugin socket directory")
}

// A target the kubelet did not publish (no pods directory above it) has no
// parent anybody will rmdir, so it is still moved aside beside itself.
func TestUnpublishMovesAsideBesideATargetOutsideAKubeletTree(t *testing.T) {
	target := filepath.Join(resolvedTempDir(t), "mount")
	require.NoError(t, os.MkdirAll(target, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(target, "f"), []byte("data"), 0o600))
	ns := newUnmountTestServer(t, &blockingMounter{}, umountLazy, nil)

	require.NoError(t, unpublish(t, ns, target))

	aside, err := filepath.Glob(target + ".orphaned-*")
	require.NoError(t, err)
	require.Len(t, aside, 1)
}

// Two orphans of the same volume and pod in the same second get distinct
// names rather than the second replacing or failing on the first.
func TestOrphanDestinationNeverReusesAName(t *testing.T) {
	kubeletDir, _, target := kubeletPublishTarget(t, "uid-1", "pvc-a")
	now := time.Unix(1700000000, 0)
	first, err := orphanDestination(target, "csi.qumulo.com", now)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(kubeletDir, "csi.qumulo.com-orphaned", "pvc-a-uid-1-1700000000"), first)
	require.NoError(t, os.Mkdir(first, 0o700))
	second, err := orphanDestination(target, "csi.qumulo.com", now)
	require.NoError(t, err)
	assert.Equal(t, first+".1", second)
}

// When the rescued data cannot be moved at all, the refusal reaches the
// caller with the code the cleanup chose. NodeUnpublishVolume re-wrapped every
// error as Internal, so the single case that names the target and the remedy
// arrived as an opaque "internal error" -- in precisely the situation where an
// operator needs to be told what to do.
func TestUnpublishRefusesWithFailedPreconditionWhenTheDataCannotBeMoved(t *testing.T) {
	kubeletDir, pvDir, target := kubeletPublishTarget(t, "uid-1", "pvc-a")
	require.NoError(t, os.WriteFile(filepath.Join(target, "written-after-the-mount-went"), []byte("data"), 0o600))
	// A file where the quarantine directory must be created, so MkdirAll fails
	// with ENOTDIR. It stands in for the real failure this path exists for: a
	// rename(2) that cannot be done, EXDEV, on a node whose kubelet tree is
	// split across filesystems.
	require.NoError(t, os.WriteFile(filepath.Join(kubeletDir, "csi.qumulo.com-orphaned"), nil, 0o600))
	ns := newUnmountTestServer(t, &blockingMounter{}, umountLazy, nil)

	err := unpublish(t, ns, target)

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err),
		"the refusal reached the kubelet as %v, losing both the code and the remedy", status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), target, "the message names the target to recover by hand")

	// Nothing was deleted, and the kubelet's own state file survives, so the
	// operator can copy the data out and the next retry can still succeed.
	data, rerr := os.ReadFile(filepath.Join(target, "written-after-the-mount-went"))
	require.NoError(t, rerr)
	assert.Equal(t, "data", string(data), "a refusal must leave the data in place")
	_, rerr = os.Stat(filepath.Join(pvDir, "vol_data.json"))
	require.NoError(t, rerr, "the kubelet's state file must survive a refusal, or the pod can never be torn down")
}
