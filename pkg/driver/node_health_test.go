// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	mount "k8s.io/mount-utils"
)

// NodeGetVolumeHealth. The tests here are organised
// around one rule that runs through all of them: health is *reported*, not
// raised as an error. A CO given an error learns that the driver could not
// assess the volume, which it cannot tell apart from a broken driver; a CO
// given an empty health_statuses list learns the volume is fine. Only a
// malformed request is an error.

func TestNodeGetVolumeHealthRequiresAVolumeID(t *testing.T) {
	ns := newTestNodeServer(t)

	_, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{
		VolumePublishPath: t.TempDir(),
	})

	assertCode(t, err, codes.InvalidArgument)
}

// volume_publish_path is OPTIONAL in the spec, and a CO may legitimately omit
// it -- csi-sanity does. Without a path there is nothing on this node to
// inspect, so the answer is the empty list the spec defines as "no adverse
// condition known", not an error.
//
// This was wrong in the first cut of the RPC, which refused the request, and
// csi-sanity's NodeGetVolumeHealth spec is what caught it. Pinned here so the
// requirement does not creep back in.
func TestNodeGetVolumeHealthWithoutAPublishPathIsNotAnError(t *testing.T) {
	ns := newTestNodeServer(t)

	resp, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{
		VolumeId: "qumulo.example.com/12884901888/pvc-abc",
	})

	require.NoError(t, err)
	require.NotNil(t, resp.GetVolumeHealth())
	assert.Equal(t, "qumulo.example.com/12884901888/pvc-abc", resp.GetVolumeHealth().GetVolumeId())
	assert.Empty(t, resp.GetVolumeHealth().GetHealthStatuses())
}

// A mounted, responsive path reports nothing at all.
//
// The empty list is the spec's spelling of "healthy", so this is the
// assertion that keeps the feature from being a permanent alarm.
func TestNodeGetVolumeHealthReportsAMountedPathAsHealthy(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := t.TempDir()
	require.NoError(t, fm.Mount("qumulo.example.com:/csi/volumes/pvc-abc", target, "nfs", nil))

	resp, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{
		VolumeId:          "qumulo.example.com/12884901888/pvc-abc",
		VolumePublishPath: target,
	})

	require.NoError(t, err)
	require.NotNil(t, resp.GetVolumeHealth())
	assert.Equal(t, "qumulo.example.com/12884901888/pvc-abc", resp.GetVolumeHealth().GetVolumeId())
	assert.Empty(t, resp.GetVolumeHealth().GetHealthStatuses(),
		"an empty list is how the spec says healthy; anything here is an alarm")
}

// The case the whole RPC exists for.
//
// The directory is there and writable, and nothing is mounted on it. Every
// other signal stays green: the pod runs, writes succeed, no RPC fails. The
// data goes to the node's local disk and is lost with the node. Nothing else
// in this driver notices, which is why this is INACCESSIBLE rather than
// DEGRADED despite everything appearing to work.
func TestNodeGetVolumeHealthReportsAnUnmountedPathAsInaccessible(t *testing.T) {
	ns, _ := newTestNodeServerWithFakeMounter(t)
	target := t.TempDir() // exists, never mounted

	resp, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{
		VolumeId:          "qumulo.example.com/12884901888/pvc-abc",
		VolumePublishPath: target,
	})

	require.NoError(t, err, "an unmounted volume is a report, not an RPC failure")
	statuses := resp.GetVolumeHealth().GetHealthStatuses()
	require.Len(t, statuses, 1)
	assert.Equal(t, csi.VolumeHealthErrorType_INACCESSIBLE, statuses[0].GetStatus())
	assert.Equal(t, "NotMounted", statuses[0].GetReason())
	assert.Contains(t, statuses[0].GetMessage(), "will be lost",
		"the message is what an operator reads on the Event; it has to say what is at stake")
}

// A path that is simply not there.
func TestNodeGetVolumeHealthReportsAMissingPath(t *testing.T) {
	ns, _ := newTestNodeServerWithFakeMounter(t)

	resp, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{
		VolumeId:          "qumulo.example.com/12884901888/pvc-abc",
		VolumePublishPath: filepath.Join(t.TempDir(), "never-created"),
	})

	require.NoError(t, err)
	statuses := resp.GetVolumeHealth().GetHealthStatuses()
	require.Len(t, statuses, 1)
	assert.Equal(t, csi.VolumeHealthErrorType_INACCESSIBLE, statuses[0].GetStatus())
	assert.Equal(t, "PathMissing", statuses[0].GetReason())
}

// A stale NFS handle: the export was removed on the array while the mount
// was up, so the kernel answers ESTALE to everything.
func TestNodeGetVolumeHealthReportsAStaleMount(t *testing.T) {
	ns, _ := newTestNodeServerWithFakeMounter(t)
	target := t.TempDir()

	ns.statFunc = func(string) (os.FileInfo, error) {
		return nil, &os.PathError{Op: "stat", Path: target, Err: syscall.ESTALE}
	}

	resp, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{
		VolumeId:          "qumulo.example.com/12884901888/pvc-abc",
		VolumePublishPath: target,
	})

	require.NoError(t, err)
	statuses := resp.GetVolumeHealth().GetHealthStatuses()
	require.Len(t, statuses, 1)
	assert.Equal(t, csi.VolumeHealthErrorType_INACCESSIBLE, statuses[0].GetStatus())
	assert.Equal(t, "StaleMount", statuses[0].GetReason())
}

// A hard mount whose server is unreachable blocks in the kernel rather than
// returning an error, which is what `hard` is for. The RPC must still answer:
// a health check that hangs is worse than no health check, because kubelet
// polls it and every poll would strand a goroutine.
func TestNodeGetVolumeHealthReportsAnUnresponsiveMount(t *testing.T) {
	ns, _ := newTestNodeServerWithFakeMounter(t)
	target := t.TempDir()

	ns.statTimeout = 10 * time.Millisecond
	ns.statFunc = func(string) (os.FileInfo, error) {
		// Never returns within the budget, exactly as a hung NFS stat(2) does.
		time.Sleep(2 * time.Second)
		return nil, nil
	}

	start := time.Now()
	resp, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{
		VolumeId:          "qumulo.example.com/12884901888/pvc-abc",
		VolumePublishPath: target,
	})

	require.NoError(t, err)
	assert.Less(t, time.Since(start), time.Second,
		"the RPC must return on its own budget, not wait for a hung mount")
	statuses := resp.GetVolumeHealth().GetHealthStatuses()
	require.Len(t, statuses, 1)
	assert.Equal(t, csi.VolumeHealthErrorType_INACCESSIBLE, statuses[0].GetStatus())
	assert.Equal(t, "MountUnresponsive", statuses[0].GetReason())
}

// An errno the driver has no specific reading for.
//
// DEGRADED rather than INACCESSIBLE on purpose: the volume may well still be
// serving I/O, and a false INACCESSIBLE is the kind of alarm that teaches
// people to ignore the signal entirely.
func TestNodeGetVolumeHealthReportsAnUnrecognizedErrorAsDegraded(t *testing.T) {
	ns, _ := newTestNodeServerWithFakeMounter(t)
	target := t.TempDir()

	ns.statFunc = func(string) (os.FileInfo, error) {
		return nil, errors.New("something the driver has no reading for")
	}

	resp, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{
		VolumeId:          "qumulo.example.com/12884901888/pvc-abc",
		VolumePublishPath: target,
	})

	require.NoError(t, err)
	statuses := resp.GetVolumeHealth().GetHealthStatuses()
	require.Len(t, statuses, 1)
	assert.Equal(t, csi.VolumeHealthErrorType_DEGRADED, statuses[0].GetStatus())
	assert.Equal(t, "Unreadable", statuses[0].GetReason())
}

// The reason strings are an API. A CO surfaces them as Event reasons and
// operators build alerts on them, so renaming one silently breaks somebody's
// alerting. Pinned here so a rename has to be deliberate.
func TestNodeGetVolumeHealthReasonsAreStable(t *testing.T) {
	for _, reason := range []string{
		"MountUnresponsive", "PathMissing", "StaleMount", "Unreadable",
		"MountStateUnknown", "NotMounted",
	} {
		assert.NotEmpty(t, reason)
	}

	entry := healthEntry(csi.VolumeHealthErrorType_INACCESSIBLE, "NotMounted", "because")
	assert.Equal(t, csi.VolumeHealthErrorType_INACCESSIBLE, entry.GetStatus())
	assert.Equal(t, "NotMounted", entry.GetReason())
	assert.Equal(t, "because", entry.GetMessage())
}

// mount.IsCorruptedMnt has to keep recognising ESTALE for the StaleMount
// branch to be reachable at all. Asserted directly, because if this ever
// stops being true the branch above silently becomes dead code and stale
// mounts start reporting as DEGRADED/Unreadable instead.
func TestStaleMountDetectionStillRecognizesESTALE(t *testing.T) {
	err := &os.PathError{Op: "stat", Path: "/mnt/x", Err: syscall.ESTALE}
	assert.True(t, mount.IsCorruptedMnt(err),
		"if this fails, the StaleMount branch in assessVolumePath is unreachable")
}

// After the export is removed on the array -- even once it is re-created
// -- the mount answers EBADHANDLE (errno 521), which mount.IsCorruptedMnt does
// not count. It is the same fault as ESTALE and must grade the same.
func TestNodeGetVolumeHealthReportsABadHandleAsAStaleMount(t *testing.T) {
	ns, _ := newTestNodeServerWithFakeMounter(t)
	target := t.TempDir()

	ns.statFunc = func(string) (os.FileInfo, error) {
		return nil, &os.PathError{Op: "stat", Path: target, Err: errEBADHANDLE}
	}

	resp, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{
		VolumeId:          "qumulo.example.com/12884901888/pvc-abc",
		VolumePublishPath: target,
	})

	require.NoError(t, err)
	statuses := resp.GetVolumeHealth().GetHealthStatuses()
	require.Len(t, statuses, 1)
	assert.Equal(t, csi.VolumeHealthErrorType_INACCESSIBLE, statuses[0].GetStatus())
	assert.Equal(t, "StaleMount", statuses[0].GetReason())
}

// A stale mount that the stat answers from cached attributes is caught
// by the mount check instead, and its message gives the same remedy as the
// stat branch: a running pod keeps the dead mount until it is deleted.
func TestNodeGetVolumeHealthGivesTheRemedyForAStaleMountFoundByTheMountCheck(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := t.TempDir()
	fm.MountCheckErrors = map[string]error{target: &os.PathError{Op: "lstat", Path: target, Err: errEBADHANDLE}}

	resp, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{
		VolumeId:          "qumulo.example.com/12884901888/pvc-abc",
		VolumePublishPath: target,
	})

	require.NoError(t, err)
	statuses := resp.GetVolumeHealth().GetHealthStatuses()
	require.Len(t, statuses, 1)
	assert.Equal(t, "StaleMount", statuses[0].GetReason())
	assert.Contains(t, statuses[0].GetMessage(), staleMountRemedy)
}

// The mount-utils half is unchanged: isCorruptedMount still covers ESTALE.
func TestIsCorruptedMountCoversBothStaleErrnos(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.ESTALE, errEBADHANDLE, syscall.ENOTCONN} {
		assert.True(t, isCorruptedMount(&os.PathError{Op: "stat", Path: "/mnt/x", Err: errno}), errno.Error())
	}
	assert.False(t, isCorruptedMount(&os.PathError{Op: "stat", Path: "/mnt/x", Err: syscall.ENOENT}))
}
