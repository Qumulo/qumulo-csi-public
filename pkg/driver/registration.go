// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// The node pod's readiness has to mean "the kubelet registered this plugin",
// because that is what a node needs to mount anything.
//
// node-driver-registrar's /healthz cannot say that: it checks only that its
// own registration socket exists and answers its own GetInfo, which stays true
// when the socket sits in a directory the kubelet never scans (a wrong
// node.kubeletDir). The kubelet calls NodeGetInfo only after it has picked up
// that socket and before it writes CSINode, so the first NodeGetInfo is the
// registration signal. NodeGetInfo records it in a file; the readiness exec
// probe (-check-registration) checks the file.
//
// The chart keeps the file in an emptyDir: it survives a driver-container
// restart, which does not make the kubelet call NodeGetInfo again, and goes
// with the pod, so a new pod starts NotReady until its own registration.

// recordRegistration writes the marker at path. "" turns it off. Written to a
// temporary name and renamed, so the probe never reads a half-written file.
func recordRegistration(path string) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating the registration marker's directory: %w", err)
	}
	tmp := path + ".tmp"
	stamp := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")
	if err := os.WriteFile(tmp, stamp, 0o644); err != nil {
		return fmt.Errorf("writing the registration marker: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("writing the registration marker: %w", err)
	}
	return nil
}

// RegistrationRecorded reports nil once NodeGetInfo has written the marker at
// path. It backs the node pod's readiness probe (-check-registration).
func RegistrationRecorded(path string) error {
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("the kubelet has not called NodeGetInfo yet (no %s): %w", path, err)
	}
	return nil
}
