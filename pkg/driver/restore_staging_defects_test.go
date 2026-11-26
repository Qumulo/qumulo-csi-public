//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Defect tests for abandoned restore staging: a restore abandoned mid-copy left
// its staging directory, `.csi-restoring-<name>`, on the array for good. Each
// asserts the correct behaviour, so it fails while the defect is present.

import "testing"

// A volume whose final path was never created -- only its staging sibling
// was -- has that sibling removed by DeleteVolume, which used to return
// "already gone" before reaching the only code that removes one.
func TestDefect_DeleteVolumeOfAGoneVolumeReclaimsItsStaging(t *testing.T) {
	checkDeleteVolumeReclaimsTheStagingOfAGoneVolume(t, &stubPVCLister{})
}

// A claim deleted before its PV existed gets no DeleteVolume at all; a later
// successful CreateVolume's sweep removes its staging directory, once two
// sweeps at least 15 minutes apart have found it with no claim and no PV.
func TestDefect_CreateVolumeSweepRemovesAbandonedStaging(t *testing.T) {
	checkSweepRemovesAbandonedStagingOnTheSecondStrike(t)
}
