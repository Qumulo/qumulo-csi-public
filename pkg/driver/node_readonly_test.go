// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

// Reopened. One lab's `hard,ro` class gave a pod a writable volume: the
// pod's mount line read rw, so the NFS superblock under it was read-write.
// mount(2) makes the node's own mount read-only either way, and the mount
// table prints ro when either is, so only mountinfo's superblock options show
// it. These tests stand in for that kernel with a mountinfo stub.

// stubMountInfo makes ns.mountInfo answer with one entry for target, with the
// given per-mount and superblock options, and counts the reads.
func stubMountInfo(ns *NodeServer, target string, mountOpts, superOpts []string) *atomic.Int32 {
	var reads atomic.Int32
	ns.mountInfo = func() ([]mount.MountInfo, error) {
		reads.Add(1)
		return []mount.MountInfo{
			{MountPoint: "/", MountOptions: []string{"rw"}, SuperOptions: []string{"rw"}},
			{MountPoint: target, MountOptions: mountOpts, SuperOptions: superOpts},
		}, nil
	}
	return &reads
}

func readOnlyPublishRequest(t *testing.T, target string, mountFlags []string) *csi.NodePublishVolumeRequest {
	t.Helper()
	return publishRequestFor(t, target, map[string]string{volumeContextProtocol: protocolNFS41}, mountFlags)
}

func TestReadOnlyPublishIsRefusedWhenTheSuperblockIsReadWrite(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	stubMountInfo(ns, target, []string{"ro", "relatime"}, []string{"rw", "vers=4.1", "hard"})

	_, err := ns.NodePublishVolume(context.Background(), readOnlyPublishRequest(t, target, []string{"hard,ro"}))

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "superblock rw,vers=4.1,hard")
	assert.Contains(t, err.Error(), "uname -r")
	assert.Empty(t, fm.MountPoints, "the writable mount is taken away, so a retry mounts again instead of confirming it")
}

func TestReadOnlyPublishIsRefusedWhenTheMountItselfIsReadWrite(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	stubMountInfo(ns, target, []string{"rw", "relatime"}, []string{"rw", "vers=4.1"})

	_, err := ns.NodePublishVolume(context.Background(), readOnlyPublishRequest(t, target, []string{"hard", "ro"}))

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Empty(t, fm.MountPoints)
}

// A readOnly request -- the pod's or the PV's flag -- is checked like "ro" in
// the class's mountOptions: its mount must be read-only too.
func TestReadOnlyRequestIsCheckedLikeAnRoMountOption(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	stubMountInfo(ns, target, []string{"rw"}, []string{"rw"})
	req := readOnlyPublishRequest(t, target, []string{"hard"})
	req.Readonly = true

	_, err := ns.NodePublishVolume(context.Background(), req)

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Empty(t, fm.MountPoints)
}

// Measured on RHCOS 9.8: `mount -o ro` leaves the NFS superblock rw.
// A readOnly request is bound read-only into the pod by the runtime, so its
// read-only mount is enough, on NFS 4.1 and NFS 3 alike.
func TestReadOnlyRequestIsAcceptedOverAReadWriteSuperblock(t *testing.T) {
	for _, protocol := range []string{protocolNFS41, protocolNFS3} {
		t.Run(protocol, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)
			target := filepath.Join(resolvedTempDir(t), "target")
			stubMountInfo(ns, target, []string{"ro", "relatime"}, []string{"rw", "hard"})
			req := publishRequestFor(t, target, map[string]string{volumeContextProtocol: protocol}, []string{"hard"})
			req.Readonly = true

			_, err := ns.NodePublishVolume(context.Background(), req)

			require.NoError(t, err)
			require.Len(t, fm.MountPoints, 1)
		})
	}
}

// The class's "ro" plus a readOnly request: the request's bind still covers
// the pod, so the read-write superblock is accepted here too.
func TestReadOnlyRequestWithAnRoClassIsAcceptedOverAReadWriteSuperblock(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	stubMountInfo(ns, target, []string{"ro"}, []string{"rw"})
	req := readOnlyPublishRequest(t, target, []string{"hard,ro"})
	req.Readonly = true

	_, err := ns.NodePublishVolume(context.Background(), req)

	require.NoError(t, err)
	require.Len(t, fm.MountPoints, 1)
}

// A republish of a readOnly request over a read-write superblock is accepted
// as the first publish was, and the mount is left alone.
func TestReadOnlyRequestRepublishIsAcceptedOverAReadWriteSuperblock(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	stubMountInfo(ns, target, []string{"ro"}, []string{"rw"})
	req := readOnlyPublishRequest(t, target, []string{"hard"})
	req.Readonly = true

	_, err := ns.NodePublishVolume(context.Background(), req)
	require.NoError(t, err)
	_, err = ns.NodePublishVolume(context.Background(), req)

	require.NoError(t, err)
	assert.Len(t, fm.MountPoints, 1)
}

func TestReadOnlyPublishSucceedsWhenTheKernelMountedItReadOnly(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	reads := stubMountInfo(ns, target, []string{"ro", "relatime"}, []string{"ro", "vers=4.1", "hard"})

	_, err := ns.NodePublishVolume(context.Background(), readOnlyPublishRequest(t, target, []string{"hard,ro"}))

	require.NoError(t, err)
	require.Len(t, fm.MountPoints, 1)
	assert.Equal(t, int32(1), reads.Load(), "the check reads mountinfo once")
}

// mountinfo that cannot be read cannot confirm the mount, so the publish
// fails closed -- Unavailable, because nothing says the mount is wrong.
func TestReadOnlyPublishFailsClosedWhenMountinfoCannotBeRead(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	ns.mountInfo = func() ([]mount.MountInfo, error) { return nil, errors.New("permission denied") }

	_, err := ns.NodePublishVolume(context.Background(), readOnlyPublishRequest(t, target, []string{"ro"}))

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Contains(t, err.Error(), "permission denied")
	assert.Empty(t, fm.MountPoints)
}

// The entry a pod sees is the topmost one at the path.
func TestReadOnlyCheckReadsTheTopmostMountAtTheTarget(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	ns.mountInfo = func() ([]mount.MountInfo, error) {
		return []mount.MountInfo{
			{MountPoint: target, MountOptions: []string{"ro"}, SuperOptions: []string{"ro"}},
			{MountPoint: target, MountOptions: []string{"ro"}, SuperOptions: []string{"rw"}},
		}, nil
	}

	_, err := ns.NodePublishVolume(context.Background(), readOnlyPublishRequest(t, target, []string{"ro"}))

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Empty(t, fm.MountPoints)
}

// A target mountinfo does not list -- a fake mounter's, as under csi-sanity --
// is judged by the mount table instead.
func TestReadOnlyCheckFallsBackToTheMountTable(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	ns.mountInfo = func() ([]mount.MountInfo, error) {
		return []mount.MountInfo{{MountPoint: "/", MountOptions: []string{"rw"}, SuperOptions: []string{"rw"}}}, nil
	}

	_, err := ns.NodePublishVolume(context.Background(), readOnlyPublishRequest(t, target, []string{"hard,ro"}))

	require.NoError(t, err, "the fake mounter records the ro it was given")
	require.Len(t, fm.MountPoints, 1)
}

// A republish finds the mount already there. It is refused the same way, but
// the mount stays: a running pod may hold it.
func TestReadOnlyRepublishIsRefusedButTheMountIsKept(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	req := readOnlyPublishRequest(t, target, []string{"hard,ro"})

	stubMountInfo(ns, target, []string{"ro"}, []string{"ro"})
	_, err := ns.NodePublishVolume(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, fm.MountPoints, 1)

	stubMountInfo(ns, target, []string{"ro"}, []string{"rw"})
	_, err = ns.NodePublishVolume(context.Background(), req)

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Len(t, fm.MountPoints, 1, "a republish never unmounts what a pod may be using")
}

// A read-write publish is not this check's business: no extra mountinfo read.
func TestReadWritePublishDoesNotReadMountinfo(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := filepath.Join(resolvedTempDir(t), "target")
	reads := stubMountInfo(ns, target, []string{"rw"}, []string{"rw"})

	_, err := ns.NodePublishVolume(context.Background(), readOnlyPublishRequest(t, target, []string{"hard"}))

	require.NoError(t, err)
	require.Len(t, fm.MountPoints, 1)
	assert.Equal(t, int32(0), reads.Load())
}
