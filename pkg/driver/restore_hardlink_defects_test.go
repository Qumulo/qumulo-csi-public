//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Defect tests for restored hard links: a restored hard link's second name
// was left truncated when the array refused the link and the copy that
// replaced it was cut, and the restore reported success. Each asserts the
// correct behaviour, so it fails while the defect is present.

import "testing"

// The second name is resumed to its full size on the controller that made
// the cut copy, which remembers the first name.
func TestDefect_FallbackCopyIsResumedOnTheSameController(t *testing.T) {
	checkFallbackCopyOfASecondNameIsResumed(t, false)
}

// The same after a controller restart, when the next attempt finds the first
// name again by walking.
func TestDefect_FallbackCopyIsResumedAfterARestart(t *testing.T) {
	checkFallbackCopyOfASecondNameIsResumed(t, true)
}
