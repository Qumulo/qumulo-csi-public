// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mount "k8s.io/mount-utils"
)

// The forced-unmount ladder's shared-superblock guard is the one rung
// whose input is *text*: production reads /proc/self/mountinfo and parses it,
// while every other test in this package injects already-parsed MountInfo
// values. That leaves the parse itself unexercised, and the parse is exactly
// what a different container runtime can change.
//
// These cases run a captured OpenShift 4.22 / RHCOS mountinfo -- CRI-O, not
// containerd -- through the same mount.ParseMountInfo production uses, and
// then through superblockSharers. The question they answer is narrow and is
// the only OpenShift-specific question the ladder's logic raises: on an RHCOS
// worker, does the guard still see the sibling it must not force?
//
// No existing case is changed by this file. It adds a parse step in front of
// the existing model rather than replacing it.
//
// Fixture: testdata/rhcos-mountinfo-crio.txt, whose header says what was kept
// and why.

// rhcosMountInfo parses the committed RHCOS capture the way production does.
// Comment lines are stripped first: real mountinfo has none, and the fixture
// needs its provenance recorded next to the data rather than in a test.
func rhcosMountInfo(t *testing.T) []mount.MountInfo {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", "rhcos-mountinfo-crio.txt"))
	require.NoError(t, err, "reading the RHCOS mountinfo fixture")

	var kept []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		kept = append(kept, line)
	}

	path := filepath.Join(t.TempDir(), "mountinfo")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(kept, "\n")+"\n"), 0o600))

	infos, err := mount.ParseMountInfo(path)
	require.NoError(t, err, "parsing the RHCOS mountinfo fixture; the capture's shape is not what the parser expects")
	require.NotEmpty(t, infos)

	return infos
}

// serverOver returns a NodeServer whose only wired dependency is the mountinfo
// reader, which is all superblockSharers touches.
func serverOver(infos []mount.MountInfo) *NodeServer {
	return &NodeServer{mountInfo: func() ([]mount.MountInfo, error) { return infos, nil }}
}

const (
	rhcosPVCA = "/var/lib/kubelet/pods/pod-a/volumes/kubernetes.io~csi/pvc-a/mount"
	rhcosPVCB = "/var/lib/kubelet/pods/pod-b/volumes/kubernetes.io~csi/pvc-b/mount"
	rhcosPVCC = "/var/lib/kubelet/pods/pod-c/volumes/kubernetes.io~csi/pvc-c/mount"
)

// On RHCOS, two volumes from one array through one set of options still share
// one superblock, and the guard must still see it. If this fails, `umount -f`
// on pvc-a aborts pvc-b's in-flight writes on an OpenShift worker.
func TestSuperblockSharersSeeTheSiblingInAnRHCOSMountInfo(t *testing.T) {
	sharers, err := serverOver(rhcosMountInfo(t)).superblockSharers(rhcosPVCA)
	require.NoError(t, err)

	assert.Equal(t, []string{rhcosPVCB}, sharers,
		"the guard found no sibling on pvc-a's superblock in an RHCOS mountinfo, so `umount -f` would run "+
			"and abort the other pod's I/O")
}

// The guard must not over-reach either: a volume from a different array is a
// different superblock, and treating it as a sharer would skip `umount -f`
// for every volume on the node and leave dead mounts to the lazy rung alone.
func TestSuperblockSharersIgnoreAnotherArrayInAnRHCOSMountInfo(t *testing.T) {
	sharers, err := serverOver(rhcosMountInfo(t)).superblockSharers(rhcosPVCC)
	require.NoError(t, err)

	assert.Empty(t, sharers,
		"a volume alone on its superblock was reported as shared, which would skip the forced rung for everything")
}

// CRI-O's own mounts -- the overlay container store and the shm tmpfs -- are
// on other devices and must not be mistaken for sharers. This is the case
// that would fail if the guard ever matched on the source string instead of
// major:minor, because CRI-O's sources look nothing like containerd's.
func TestSuperblockSharersIgnoreCRIOsOwnMounts(t *testing.T) {
	infos := rhcosMountInfo(t)

	var crio []string
	for _, info := range infos {
		if strings.Contains(info.MountPoint, "/var/lib/containers/") || strings.Contains(info.Root, "containers") {
			crio = append(crio, info.MountPoint)
		}
	}
	require.NotEmpty(t, crio, "the fixture no longer carries a CRI-O mount, so this case proves nothing")

	sharers, err := serverOver(infos).superblockSharers(rhcosPVCA)
	require.NoError(t, err)

	for _, path := range crio {
		assert.NotContains(t, sharers, path, "a CRI-O runtime mount was counted as sharing the volume's superblock")
	}
}

// The parse is the point of this file, so assert it produced the device
// numbers rather than silently producing zeroes -- a parser that fails softly
// would make every case above pass for the wrong reason.
func TestRHCOSMountInfoParsesDeviceNumbers(t *testing.T) {
	byPath := map[string]mount.MountInfo{}
	for _, info := range rhcosMountInfo(t) {
		byPath[info.MountPoint] = info
	}

	a, ok := byPath[rhcosPVCA]
	require.True(t, ok, "the fixture no longer lists pvc-a")
	b, ok := byPath[rhcosPVCB]
	require.True(t, ok, "the fixture no longer lists pvc-b")

	assert.Equal(t, 0, a.Major)
	assert.Equal(t, 357, a.Minor, "pvc-a's minor device number did not survive the parse")
	assert.Equal(t, a.Minor, b.Minor, "the fixture's two NFS volumes are no longer on one superblock")
	assert.Equal(t, "nfs4", a.FsType)
}
