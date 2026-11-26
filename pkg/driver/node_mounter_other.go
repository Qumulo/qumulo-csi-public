//go:build !linux

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"os"
	"runtime"

	mount "k8s.io/mount-utils"
)

// newRealMounter exists so this package -- and therefore `go test ./...` --
// builds on a non-Linux workstation. The node image is Linux-only (see
// ARCHITECTURE.md's Docker section: mount(8) needs a kernel this driver never has
// on Windows or Darwin), so NewNodeServer calling this outside Linux is not a
// supported deployment; it only panics if something actually tries to run the
// real node server on a build that could never ship.
func newRealMounter() mount.Interface {
	panic("qumulo-csi-driver node server requires linux; built for " + runtime.GOOS)
}

// targetTypeNoSync is lstat(2) off Linux, where statx does not exist; the
// node server never runs there (see newRealMounter).
func targetTypeNoSync(path string) (os.FileMode, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, err
	}
	return info.Mode().Type(), nil
}
