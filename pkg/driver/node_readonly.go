// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// A read-only publish is checked against what the kernel did, not what was
// asked of mount(8).
//
// A volume becomes read-only through "ro" in its class's mountOptions or a
// readOnly request, and the driver passes that to mount(8) unchanged. Until
// this check the driver trusted the result. One lab (kind, 2026-10-06) ran a
// `hard,ro` class whose pod could write: the pod's mount line read rw, which
// the kernel prints only when both the pod's bind and the NFS superblock are
// writable (reopened).
//
// The superblock is the part that matters. mount(2) with MS_RDONLY always
// makes the node's own mount read-only, but that flag belongs to that one
// mount. The container runtime binds the target into the pod, and runc 1.4.3
// was measured not to carry the flag onto that bind: the pod's bind read
// "rw" over a read-only superblock, and only the superblock refused the
// write. So a read-only superblock is the whole of the protection, and a
// read-write one -- whatever caused it on that node -- is a writable volume.
//
// /proc/mounts cannot tell the two apart: it prints "ro" when either is
// read-only. mountinfo lists them separately, so that is what is read.
//
// One exception, for kernels that never make an NFS superblock read-only.
// On RHCOS 9.8 (kernel 5.14.0-687.el9_8, OpenShift 4.22) every
// `mount -o ro` of a Qumulo export -- NFS 4.1 and NFS 3, mount(2) with
// MS_RDONLY, no other mount of the server on the node -- left the superblock
// rw, a later `mount -o remount,ro` answered 0 and left it rw too, and a rw
// and a ro mount of the same export shared one superblock. So every read-only
// publish there was refused. When the request itself is readOnly (the pod's
// or the PV's flag), the kubelet also asks the runtime to bind the volume
// read-only into the container, and that bind is the protection; a read-only
// mount over a read-write superblock is accepted then, and only then. A
// volume read-only through the class's "ro" alone has no such bind (the
// case) and is still refused.

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// readOnlyMountRemedy is what the refusal tells the operator to do.
const readOnlyMountRemedy = "the node's kernel or NFS client did not honour \"ro\" for this mount -- for example by " +
	"sharing a read-write superblock of the same server, or, as on RHCOS, by never making an NFS superblock " +
	"read-only; record `uname -r` and the node's mountinfo for support. Setting readOnly: true on the pod's " +
	"persistentVolumeClaim volume makes the runtime bind it read-only into the pod, and the publish is then accepted"

// readOnlyState reports whether the mount at target, and the superblock under
// it, are read-only.
//
// The topmost mountinfo entry for target is the one a pod sees. A target
// mountinfo does not list -- which a fake mounter's targets never are -- falls
// back to the mount table, whose one ro/rw flag then stands for both.
func (ns *NodeServer) readOnlyState(target string) (mountRO, superRO bool, detail string, err error) {
	infos, err := ns.mountInfo()
	if err != nil {
		return false, false, "", err
	}
	for i := len(infos) - 1; i >= 0; i-- {
		if infos[i].MountPoint == target {
			mountRO = slices.Contains(infos[i].MountOptions, "ro")
			superRO = slices.Contains(infos[i].SuperOptions, "ro")
			return mountRO, superRO, fmt.Sprintf("mount %s; superblock %s",
				strings.Join(infos[i].MountOptions, ","), strings.Join(infos[i].SuperOptions, ",")), nil
		}
	}

	mounted, err := ns.existingMount(target)
	if err != nil {
		return false, false, "", err
	}
	if mounted == nil {
		return false, false, "", fmt.Errorf("%s is in neither mountinfo nor the mount table", target)
	}
	ro := isReadOnly(mounted.Opts)
	return ro, ro, "mount table " + strings.Join(mounted.Opts, ","), nil
}

// requireReadOnlyMount refuses a read-only publish whose mount the kernel
// left writable.
//
// fresh is true for a mount this call has just made: it is unmounted before
// the refusal, so the retry mounts again rather than finding it and answering
// OK. A republish (fresh false) leaves the mount alone, because a running pod
// may hold it; the refusal still stops the publish being reported satisfied.
//
// requestRO is the request's own readOnly flag, not the class's "ro": with it
// the runtime binds the volume read-only into the pod, so a read-only mount
// over a read-write superblock is accepted (see the file comment).
func (ns *NodeServer) requireReadOnlyMount(ctx context.Context, volumeID, targetPath string, fresh, requestRO bool) error {
	mountRO, superRO, detail, err := ns.readOnlyState(targetPath)
	if err == nil && mountRO && superRO {
		return nil
	}
	if err == nil && mountRO && requestRO {
		slog.InfoContext(ctx, "nodepublishvolume: read-only mount over a read-write superblock accepted: the request "+
			"is readOnly, so the runtime binds the volume read-only into the pod",
			"volumeID", volumeID, "targetPath", targetPath, "detail", detail)
		return nil
	}

	// A state that cannot be read is refused too. Unavailable rather than
	// FailedPrecondition: nothing says the mount is wrong, only that it could
	// not be confirmed, and a retry may read it.
	code := codes.FailedPrecondition
	reason := fmt.Sprintf("the kernel reports the mount at %s read-write (%s); %s", targetPath, detail, readOnlyMountRemedy)
	if err != nil {
		code = codes.Unavailable
		reason = fmt.Sprintf("the read-only state of the mount at %s could not be read (%v)", targetPath, err)
	}
	slog.WarnContext(ctx, "nodepublishvolume: refusing a read-only publish the kernel did not mount read-only",
		"volumeID", volumeID, "targetPath", targetPath, "mountReadOnly", mountRO, "superblockReadOnly", superRO,
		"detail", detail, "error", err, "fresh", fresh)

	leftover := ""
	if fresh {
		if uerr := ns.unmountWithTimeout(ctx, targetPath); uerr != nil {
			// The retry finds it through confirmExistingMount, which checks
			// it again, so it stays refused.
			ns.unconfirmed.add(targetPath)
			leftover = fmt.Sprintf("; the mount could not be removed (%v), and every retry is refused until it is", uerr)
		}
	}
	return status.Errorf(code, "volume %s was requested read-only, but %s%s", volumeID, reason, leftover)
}
