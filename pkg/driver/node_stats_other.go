//go:build !unix

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"
	"runtime"
)

// volumeStatsFor is unavailable off Unix. The node service only ever runs on
// a Linux node, so this exists purely to keep the package building on other
// platforms for development and testing.
func volumeStatsFor(path string) (volumeStats, error) {
	return volumeStats{}, fmt.Errorf("volume statistics are not supported on %s", runtime.GOOS)
}
