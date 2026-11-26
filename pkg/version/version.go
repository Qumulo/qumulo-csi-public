// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package version holds build-time metadata for the Qumulo CSI driver.
//
// DriverVersion, GitCommit and BuildDate are overridden at build time via
// -ldflags (see the Makefile); DriverName identifies the plugin to a
// container orchestrator and should match the CSIDriver object name.
package version

import "runtime"

var (
	DriverName    = "csi.qumulo.com"
	DriverVersion = "dev"
	GitCommit     = "unknown"
	BuildDate     = "unknown"
)

// Info is a snapshot of the driver's build metadata.
type Info struct {
	DriverName    string
	DriverVersion string
	GitCommit     string
	BuildDate     string
	GoVersion     string
}

// Get returns the current build metadata.
func Get() Info {
	return Info{
		DriverName:    DriverName,
		DriverVersion: DriverVersion,
		GitCommit:     GitCommit,
		BuildDate:     BuildDate,
		GoVersion:     runtime.Version(),
	}
}
