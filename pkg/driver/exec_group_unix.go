//go:build unix

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"os/exec"
	"syscall"
)

// killGroupOnCancel puts cmd in a process group of its own and, when its
// context ends, kills the whole group rather than cmd alone.
//
// umount(8) forks the umount.nfs helper, which inherits the output pipe.
// Killing umount alone left the helper running, holding the pipe, and the
// call waited for it -- so a forced rung's deadline was not enforced.
func killGroupOnCancel(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		// The group's ID is the leader's PID; a negative PID names the group.
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
