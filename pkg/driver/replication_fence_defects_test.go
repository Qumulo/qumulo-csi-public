//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Defect tests for the replica-export fence: a source-side status poll set the
// just-promoted DR copy's export read-only, deciding from a target status it
// read before the promote. Each asserts the correct behaviour, so it fails
// while the defect is present.

import "testing"

// The poll reads B's end as a read-only replica, a planned promote of B runs
// to completion, and the poll then finds B's export read-write: it must leave
// it so.
func TestDefect_InfoPollLeavesAJustPromotedReplicaExportWritable(t *testing.T) {
	checkInfoPollLeavesAJustPromotedExportWritable(t)
}
