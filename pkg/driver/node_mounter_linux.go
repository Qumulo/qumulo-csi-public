//go:build linux

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
	mount "k8s.io/mount-utils"
)

// newRealMounter builds the mounter NewNodeServer uses in production.
//
// Built without systemd support: mount-utils otherwise probes for
// systemd-run before every mount and caches the answer in an unsynchronized
// field, which two concurrent NodePublishVolume calls race on, and the probe
// can only ever fail here, since a CSI node container has no systemd to run
// the mount under.
func newRealMounter() mount.Interface {
	return mount.NewWithoutSystemd("")
}

// targetTypeNoSync reports the file type at path, the final element not
// followed, from what this node already has cached.
//
// statx with AT_STATX_DONT_SYNC rather than lstat(2): on an NFS mount lstat
// revalidates the inode's attributes with the server, so on the stale mount
// a republish exists to recover it failed ESTALE before the recovery could
// run, and on a hard mount whose server is gone it blocked with no deadline.
// The type of a file never changes under it, so a cached answer is as good
// as a fresh one here. mount-utils' IsLikelyNotMountPoint asks the same way.
func targetTypeNoSync(path string) (os.FileMode, error) {
	var stx unix.Statx_t
	err := unix.Statx(unix.AT_FDCWD, path,
		unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_DONT_SYNC, unix.STATX_TYPE, &stx)
	if errors.Is(err, unix.ENOSYS) {
		// A kernel older than 4.11, or a seccomp profile without statx.
		info, lerr := os.Lstat(path)
		if lerr != nil {
			return 0, lerr
		}
		return info.Mode().Type(), nil
	}
	if err != nil {
		return 0, &os.PathError{Op: "statx", Path: path, Err: err}
	}
	switch uint32(stx.Mode) & unix.S_IFMT {
	case unix.S_IFLNK:
		return os.ModeSymlink, nil
	case unix.S_IFDIR:
		return os.ModeDir, nil
	default:
		// Only "symlink", "directory" and "anything else" matter here.
		return os.ModeIrregular, nil
	}
}
