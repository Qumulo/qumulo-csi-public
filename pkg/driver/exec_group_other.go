//go:build !unix

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import "os/exec"

// killGroupOnCancel is a no-op where there are no process groups; the node
// plugin runs on Linux.
func killGroupOnCancel(*exec.Cmd) {}
