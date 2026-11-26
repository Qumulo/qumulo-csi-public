// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/mount-utils"
)

// --- Unpublish through a symlinked target ---
//
// A symlinked target never has this volume's mount behind it: publish refuses
// one (TestNodePublishRefusesASymlinkTarget), and the kernel refuses
// to rename or remove a directory something is mounted on (EBUSY), so a
// published target cannot be swapped for a link afterwards. What a link at
// the target can point to is somebody else's mount. Unpublish therefore
// matches the literal path in the mount table, removes the link, and never
// unmounts what the link points to -- following it would let a link made on
// the node unmount another pod's live volume.

func TestUnpublishThroughASymlinkRemovesOnlyTheLinkAndUnmountsNothing(t *testing.T) {
	block := make(chan struct{})
	close(block)
	dir := resolvedTempDir(t)
	other := filepath.Join(dir, "another-pods-mount")
	require.NoError(t, os.Mkdir(other, 0o750))
	link := filepath.Join(dir, "mount")
	require.NoError(t, os.Symlink(other, link))

	m := &blockingMounter{block: block, table: []mount.MountPoint{{Path: other}}}
	ns := newUnmountTestServer(t, m, umountForce, nil)

	require.NoError(t, unpublish(t, ns, link))

	assert.Empty(t, m.recorded(), "no umount of any kind: the link's destination is not this volume")
	assert.Equal(t, []mount.MountPoint{{Path: other}}, m.table, "the other mount is still there")
	_, err := os.Lstat(link)
	assert.True(t, os.IsNotExist(err), "the link itself is removed, so the kubelet's teardown completes")
	_, err = os.Stat(other)
	assert.NoError(t, err, "the directory the link pointed to is not removed")
}

func TestUnpublishOfADanglingSymlinkRemovesTheLink(t *testing.T) {
	block := make(chan struct{})
	close(block)
	dir := resolvedTempDir(t)
	link := filepath.Join(dir, "mount")
	require.NoError(t, os.Symlink(filepath.Join(dir, "gone"), link))

	m := &blockingMounter{block: block}
	ns := newUnmountTestServer(t, m, umountForce, nil)

	require.NoError(t, unpublish(t, ns, link))

	assert.Empty(t, m.recorded())
	_, err := os.Lstat(link)
	assert.True(t, os.IsNotExist(err))
}
