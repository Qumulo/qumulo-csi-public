//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package doccheck

import (
	"strings"
	"testing"
)

// Snapshot kinds: a customer found the split between "Kubernetes" and
// "Qumulo" snapshots confusing. Every snapshot is a Qumulo snapshot; the docs
// now name four kinds -- Kubernetes, scheduled, imported, array-internal --
// in one table on docs/protect/snapshot.md, and say how a scheduled snapshot
// is restored (copy from .snapshot, or the storage administrator once the
// volume is gone). Before, one page pointed "to restore from one" at a
// section that said nothing about restoring, and the recovery runbook said to
// restore "from the Qumulo policy snapshot on the array" with no steps.

// snapshotKindsCustomerDocs are the customer-facing pages that talk about
// snapshots.
var snapshotKindsCustomerDocs = []string{
	"docs/protect/snapshot.md",
	"docs/protect/restore.md",
	"docs/protect/recover.md",
	"docs/features/snapshots.md",
	"docs/provision/storageclass/scheduled-snapshots.md",
	"docs/reference/faq.md",
	"docs/reference/supported-features.md",
	"deploy/use-cases/04-snapshot-restore/04-snapshot-restore.md",
	dfxChartREADME,
}

// TestDefect_SnapshotKinds_TableNamesAllFour: the comparison table on
// docs/protect/snapshot.md has a column for each kind, and the rows a reader
// needs to tell them apart.
func TestDefect_SnapshotKinds_TableNamesAllFour(t *testing.T) {
	header := dfxRow(t, "docs/protect/snapshot.md", "| Kubernetes snapshot |")
	for _, kind := range []string{"Kubernetes snapshot", "Scheduled snapshot", "Imported snapshot", "Array-internal snapshot"} {
		if !strings.Contains(header, kind) {
			t.Errorf("docs/protect/snapshot.md kinds table has no %q column: %q", kind, dfxClip(header))
		}
	}
	for _, row := range []string{"| Name on the array |", "| Seen with `kubectl` |", "| Restore |", "| Delete |"} {
		if dfxRow(t, "docs/protect/snapshot.md", row) == "" {
			t.Errorf("docs/protect/snapshot.md kinds table has no %s row", row)
		}
	}
	if name := dfxRow(t, "docs/protect/snapshot.md", "| Name on the array |"); !strings.Contains(name, "_csi-<pv-name>_<pv-name>") {
		t.Errorf("docs/protect/snapshot.md does not give a scheduled snapshot's array name, <id>_csi-<pv-name>_<pv-name>: %q", dfxClip(name))
	}
}

// TestDefect_SnapshotKinds_ScheduledRestoreIsDocumented: restore.md says how
// to get data back from a scheduled snapshot in both cases, and the pages
// that used to point nowhere point at it.
func TestDefect_SnapshotKinds_ScheduledRestoreIsDocumented(t *testing.T) {
	sec := dfxBetween(t, "docs/protect/restore.md", `^## Restore from a scheduled snapshot`, `^## `)
	if sec == "" {
		t.Fatalf("docs/protect/restore.md has no \"Restore from a scheduled snapshot\" section")
	}
	for _, want := range []string{`\.snapshot`, `volume still exists`, `volume is deleted`, `copy-chunk`, `source_snapshot`, `keep=`} {
		if !dfxMatch(want, sec) {
			t.Errorf("docs/protect/restore.md \"Restore from a scheduled snapshot\" does not mention %s", want)
		}
	}
	const link = "restore.md#restore-from-a-scheduled-snapshot"
	for _, rel := range []string{
		"docs/protect/snapshot.md",
		"docs/features/snapshots.md",
		"docs/provision/storageclass/scheduled-snapshots.md",
		"docs/reference/faq.md",
		dfxChartREADME,
	} {
		if !strings.Contains(dfxRead(t, rel), link) {
			t.Errorf("%s does not link to %s", rel, link)
		}
	}
}

// TestDefect_SnapshotKinds_NoOldWording: the phrases that made the kinds hard
// to tell apart stay out of the customer docs. The config keys
// allowForeignSnapshotDelete/Restore keep their names, so only the prose
// forms are refused.
func TestDefect_SnapshotKinds_NoOldWording(t *testing.T) {
	banned := []string{
		`foreign-named`,
		`foreign snapshot`,
		`Qumulo policy snapshot on the array`,
		`policy-created snapshot`,
		`Scheduled Qumulo snapshot policies`,
		`A Qumulo snapshot is fast`,
	}
	for _, rel := range snapshotKindsCustomerDocs {
		text := dfxReadFlat(t, rel)
		for _, phrase := range banned {
			if dfxMatch(phrase, text) {
				t.Errorf("%s still says %q; name the snapshot kind instead (docs/protect/snapshot.md)", rel, phrase)
			}
		}
	}
	// The broken pointer: "To restore from one ... see" a section that said
	// nothing about restoring.
	if dfxMatch(`To restore from one, or to take`, dfxReadFlat(t, "docs/provision/storageclass/scheduled-snapshots.md")) {
		t.Errorf("docs/provision/storageclass/scheduled-snapshots.md still has the old restore pointer")
	}
}

// TestDefect_SnapshotKinds_ExampleClassCarriesItsSecret: the example
// VolumeSnapshotClass names the snapshotter Secret. The driver keeps no
// fallback token, so a class without it has every snapshot refused.
func TestDefect_SnapshotKinds_ExampleClassCarriesItsSecret(t *testing.T) {
	ex := dfxRead(t, "deploy/example/08-snapshot.md")
	for _, key := range []string{
		"csi.storage.k8s.io/snapshotter-secret-name:",
		"csi.storage.k8s.io/snapshotter-secret-namespace:",
		"csi.storage.k8s.io/snapshotter-list-secret-name:",
		"csi.storage.k8s.io/snapshotter-list-secret-namespace:",
	} {
		if !strings.Contains(ex, key) {
			t.Errorf("deploy/example/08-snapshot.md VolumeSnapshotClass has no %s", key)
		}
	}
}
