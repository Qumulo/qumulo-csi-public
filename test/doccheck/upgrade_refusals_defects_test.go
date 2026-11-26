//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package doccheck

import "testing"

// A test-suite audit made the driver and the chart refuse a set of inputs
// earlier releases accepted -- most of them mistakes resolved silently -- and nothing told an operator upgrading which
// of their classes, snapshot classes or mount options would now fail. The
// chart README carries an "inputs now refused" section with the commands to
// check a cluster first.

// dfxUpgradeRefusals is each item the section must name: a description for
// the failure message and a pattern for the flattened text.
var dfxUpgradeRefusals = []struct{ what, re string }{
	{"VolumeSnapshotClass keys other than apiurl and csi.storage.k8s.io/*", `VolumeSnapshotClass.*apiurl.*csi\.storage\.k8s\.io/\*`},
	{"apiurl must be https://", `apiurl.{0,80}https://`},
	{"CreateVolume retry with different allowedClients/securityFlavor/tenantID is ALREADY_EXISTS", `allowedClients.{0,20}securityFlavor.{0,20}tenantID.{0,200}ALREADY_EXISTS`},
	{"ListSnapshots paging token is a snapshot ID", `ListSnapshots.{0,40}token.{0,40}snapshot ID`},
	{"mountOptions version shorthand contradicting protocol", `v4\.1.{0,200}contradicts.{0,40}protocol`},
	{"sec= contradicting securityFlavor; unset accepted with a warning", `sec=.{0,80}contradicts.{0,40}securityFlavor.{0,200}warning`},
	{"symlinked or relative publish targets", `symlink.{0,200}relative`},
	{"StorageClass keys differing only by case", `differ only by case`},
	{"store paths with . or .. elements", "storeRealPath.{0,80}`\\.\\.`"},
	{"unknown or Local time zones", `IANA.{0,200}tz=Local`},
	{"zone names are case-sensitive", `case-sensitive`},
	{"repeated keep=/tz=", `repeated .?keep=.{0,10}tz=`},
	{"chart schema: snapshotSchedule, allowedClients CIDR, tenantID below 1", `schema.{0,200}snapshotSchedule.{0,200}allowedClients.{0,80}CIDR.{0,80}tenantID.{0,20}below 1`},
	{"kubectl get volumesnapshotclass", `kubectl get volumesnapshotclass`},
	{"kubectl get sc", `kubectl get sc`},
}

// TestDefect_UpgradeSectionNamesWhatIsNowRefused: the chart README's
// upgrade section names every newly refused input and how to check for it.
func TestDefect_UpgradeSectionNamesWhatIsNowRefused(t *testing.T) {
	section := dfxBetween(t, dfxChartREADME, `^### Upgrading to this release: inputs now refused`, `^## `)
	if section == "" {
		t.Fatalf("the chart README has no \"Upgrading to this release: inputs now refused\" section")
	}
	for _, item := range dfxUpgradeRefusals {
		if !dfxMatch(item.re, section) {
			t.Errorf("the chart README's upgrade section does not name %s", item.what)
		}
	}
}
