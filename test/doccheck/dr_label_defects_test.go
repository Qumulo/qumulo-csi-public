//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package doccheck

import (
	"regexp"
	"strings"
	"testing"
)

// DR label: replication was called a "prototype" in the chart, the startup
// log and the flag help, "Preview" on the docs features page, "production-
// ready" in the FAQ and "GA" on the recover page -- four answers to one
// question. It is now "Supported, opt-in" everywhere a user reads it, and
// qumulo-csi-repoint, which ships in no release image, is "Preview". These
// tests keep the four from drifting apart again.

// dfxDRUserFacing are the files a user reads that describe replication.
var dfxDRUserFacing = []string{
	"charts/qumulo-csi-driver/values.yaml",
	"charts/qumulo-csi-driver/values.schema.json",
	"charts/qumulo-csi-driver/templates/NOTES.txt",
	"charts/qumulo-csi-driver/templates/controller.yaml",
	"charts/qumulo-csi-driver/templates/rbac.yaml",
	"charts/qumulo-csi-driver/templates/volumereplicationclass.yaml",
	"cmd/qumulo-csi-driver/main.go",
	"pkg/driver/replication_server.go",
	"docs/features/index.md",
	"docs/features/disaster-recovery.md",
	"docs/protect/replicate.md",
	"docs/protect/recover.md",
	"docs/reference/faq.md",
	"docs/reference/supported-features.md",
}

var dfxPrototypeWord = regexp.MustCompile(`(?i)\bprototype\b`)

func TestDefect_DRLabelNoPrototypeInUserFacingFiles(t *testing.T) {
	for _, rel := range dfxDRUserFacing {
		for i, line := range strings.Split(dfxRead(t, rel), "\n") {
			if dfxPrototypeWord.MatchString(line) {
				t.Errorf("%s:%d calls replication a prototype; the label is \"Supported, opt-in\": %q",
					rel, i+1, strings.TrimSpace(line))
			}
		}
	}
}

func TestDefect_DRLabelIsTheSameEverywhere(t *testing.T) {
	const label = "Supported, opt-in"

	index := dfxRead(t, "docs/features/index.md")
	var drLine string
	for _, line := range strings.Split(index, "\n") {
		if strings.Contains(line, "disaster-recovery.md") {
			drLine = line
		}
	}
	if drLine == "" {
		t.Fatalf("docs/features/index.md has no disaster-recovery entry")
	}
	if !strings.Contains(drLine, label) || strings.Contains(drLine, "Preview") {
		t.Errorf("docs/features/index.md DR entry should say %q and not Preview: %q", label, drLine)
	}

	recover := dfxRead(t, "docs/protect/recover.md")
	var replRow string
	for _, line := range strings.Split(recover, "\n") {
		if strings.HasPrefix(line, "| Qumulo replication") {
			replRow = line
		}
	}
	if replRow == "" {
		t.Fatalf("docs/protect/recover.md has no Qumulo replication row")
	}
	if !strings.Contains(replRow, label) || strings.Contains(replRow, "| GA |") {
		t.Errorf("docs/protect/recover.md replication maturity should be %q: %q", label, replRow)
	}

	faq := dfxFlat(dfxRead(t, "docs/reference/faq.md"))
	if strings.Contains(faq, "production-ready") {
		t.Errorf("docs/reference/faq.md still asks whether replication is production-ready; it should say %q", label)
	}
	if !strings.Contains(faq, "supported, opt-in") {
		t.Errorf("docs/reference/faq.md never says replication is supported, opt-in")
	}

	features := dfxFlat(dfxRead(t, "docs/reference/supported-features.md"))
	if !strings.Contains(features, label) {
		t.Errorf("docs/reference/supported-features.md section 5 never says %q", label)
	}
	if !strings.Contains(features, "Known limits") {
		t.Errorf("docs/reference/supported-features.md section 5 lost its Known limits list")
	}
}

func TestDefect_DRRepointIsPreview(t *testing.T) {
	for _, rel := range []string{
		"docs/reference/supported-features.md",
		"docs/features/disaster-recovery.md",
		"docs/protect/replicate.md",
	} {
		if !strings.Contains(dfxRead(t, rel), "**Preview**") {
			t.Errorf("%s does not mark qumulo-csi-repoint as **Preview**", rel)
		}
	}
	help := dfxRead(t, "cmd/qumulo-csi-repoint/main.go")
	if !strings.Contains(help, "(Preview)") || strings.Contains(help, "(PROTOTYPE)") {
		t.Errorf("qumulo-csi-repoint help should say (Preview), not (PROTOTYPE)")
	}
}
