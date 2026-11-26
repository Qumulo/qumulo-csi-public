//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package doccheck

import (
	"regexp"
	"testing"
)

// csi-addons re-reads a relationship only when its
// VolumeReplicationClass sets schedulingInterval. A hand-written class
// without it got GetVolumeReplicationInfo twice at Enable -- before the first
// sync -- and never again, so lastSyncTime stayed empty and the replication
// series froze at that first read: the mode series was present and the
// recovery-point age never appeared (seen on a lab cluster). The chart's
// template always sets it, so only a hand-written class is exposed, and
// nothing in the docs said so.

// dfxCommentMarks strips the comment leaders a flattened YAML or Go comment
// keeps between its lines, so a sentence wrapped across them still matches.
var dfxCommentMarks = regexp.MustCompile(`\s*(#|//)\s*`)

func dfxUncomment(s string) string { return dfxCommentMarks.ReplaceAllString(s, " ") }

// TestDefect_HandWrittenClassMustSetSchedulingInterval: the chart
// README key row says a hand-written class must set schedulingInterval, and
// what a class without it looks like.
func TestDefect_HandWrittenClassMustSetSchedulingInterval(t *testing.T) {
	const handWritten = `(by hand|hand-written)`
	const frozen = `(freez|frozen)`

	row := dfxRow(t, dfxChartREADME, "`schedulingInterval`")
	if row == "" {
		t.Fatalf("chart README key table has no schedulingInterval row")
	}
	if !dfxMatch(handWritten, row) || !dfxMatch(frozen, row) || !dfxMatch(`lastSyncTime`, row) {
		t.Errorf("chart README schedulingInterval row does not say a hand-written class must set it, or that without it lastSyncTime stays empty and the replication series freeze: %q", dfxClip(row))
	}
}

// TestDefect_NoLagGradeOutsideTheReadme: another test guards the README
// row and the class template; values.yaml and the parameter's own comment
// kept saying the driver grades lag against schedulingInterval. csi-addons' Info carries no class parameters, so it
// never does.
func TestDefect_NoLagGradeOutsideTheReadme(t *testing.T) {
	const lagGrade = `lag (past|beyond) which[^.|]*Degraded|grades lag against it`
	for _, f := range []string{
		"charts/qumulo-csi-driver/values.yaml",
		"pkg/driver/replication_params.go",
	} {
		if text := dfxUncomment(dfxReadFlat(t, f)); dfxMatch(lagGrade, text) {
			t.Errorf("%s says the driver grades lag against schedulingInterval; csi-addons' GetVolumeReplicationInfo carries no class parameters, so it never does", f)
		}
	}
}
