//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package doccheck

import "testing"

const (
	dfxOCPInstall    = "docs/get-started/openshift.md"
	dfxOCPOperations = "docs/operate/openshift-operations.md"
)

// No OpenShift document
// said that downgrading OpenShift is unsupported, or that going back to an
// earlier driver release is a `helm rollback`. The operations guide's
// Upgrades section now has a Downgrades subsection, and the install guide's
// Supported versions points to it.

// TestDefect_OpenShiftDowngradeIsDocumentedUnsupported: the
// operations guide says an OpenShift downgrade is not supported and gives
// the helm rollback commands; the install guide says so too and links there.
func TestDefect_OpenShiftDowngradeIsDocumentedUnsupported(t *testing.T) {
	section := dfxBetween(t, dfxOCPOperations, `^### Downgrades`, `^##`)
	if section == "" {
		t.Fatalf("%s has no \"### Downgrades\" section", dfxOCPOperations)
	}
	for _, c := range []struct{ what, re string }{
		{"that downgrading OpenShift is not supported", `downgrading OpenShift is not supported`},
		{"helm history to find a revision", `helm history qumulo-csi`},
		{"helm rollback to a revision", `helm rollback qumulo-csi <revision>`},
		{"that volumes and their data are not affected", `Volumes, their data.{0,80}not affected`},
	} {
		if !dfxMatch(c.re, section) {
			t.Errorf("%s's Downgrades section does not say %s:\n%s", dfxOCPOperations, c.what, dfxClip(section))
		}
	}

	supported := dfxBetween(t, dfxOCPInstall, `^## Supported versions`, `^## `)
	if !dfxMatch(`Downgrading OpenShift is not supported`, supported) {
		t.Errorf("%s's Supported versions does not say downgrading OpenShift is unsupported:\n%s", dfxOCPInstall, dfxClip(supported))
	}
	if !dfxMatch(`openshift-operations\.md#downgrades`, supported) {
		t.Errorf("%s's Supported versions does not link to the Downgrades section of %s", dfxOCPInstall, dfxOCPOperations)
	}
}
