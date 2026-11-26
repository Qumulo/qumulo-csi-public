//go:build unix

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// volumeStatsFor reads usage for a mounted volume with statfs(2).
//
// The numbers describe the NFS export the volume lives in, as the kernel
// reports it. With a Qumulo directory quota in play that is the quota's
// view -- the cluster reports a quota'd directory's size as the quota, not
// as the whole filesystem -- which is what makes the figures meaningful per
// volume rather than per cluster.
func volumeStatsFor(path string) (volumeStats, error) {
	var fs unix.Statfs_t
	if err := unix.Statfs(path, &fs); err != nil {
		return volumeStats{}, fmt.Errorf("statfs %s: %w", path, err)
	}

	// Bsize is int64 on Linux and uint32 on Darwin, so it is widened rather
	// than used directly.
	blockSize := uint64(fs.Bsize)

	return volumeStats{
		TotalBytes: int64(fs.Blocks * blockSize),
		// Bavail, not Bfree: the space an unprivileged workload can actually
		// use. On a filesystem with reserved blocks the two differ, and the
		// workload only ever sees the smaller one.
		AvailableBytes: int64(fs.Bavail * blockSize),
		UsedBytes:      int64(sub(fs.Blocks, fs.Bfree) * blockSize),

		TotalInodes: int64(fs.Files),
		FreeInodes:  int64(fs.Ffree),
		UsedInodes:  int64(sub(fs.Files, fs.Ffree)),
	}, nil
}

// sub subtracts without wrapping. CSI requires these values to be
// non-negative, and NFS servers have been known to report a free count
// above the total, which would otherwise underflow into a vast number.
func sub(total, free uint64) uint64 {
	if free > total {
		return 0
	}
	return total - free
}
