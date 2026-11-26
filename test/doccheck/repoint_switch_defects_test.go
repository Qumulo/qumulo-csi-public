//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package doccheck

import "testing"

// `qumulo-csi-repoint switch -dry-run=false` deleted source claims without
// having read the pods using them -- always with -f, and whenever the client
// or the pod list failed -- and reported a replacement that hit the
// still-Terminating original as "exists". The fix refuses all three and
// reports a claim that did not go as NOT DONE; the usage text has to say so,
// or an operator expects the tool to delete with -f.

// TestDefect_SwitchSaysWhenItDeletesNothing: the command's usage text says
// -f deletes nothing, and runSwitch refuses to delete claims with -f.
func TestDefect_SwitchSaysWhenItDeletesNothing(t *testing.T) {
	src := dfxGoSource(t, "cmd/qumulo-csi-repoint")
	usage := dfxBetween(t, "cmd/qumulo-csi-repoint/main.go", `^switch, for an IP-named server`, `^failover/failback do`)
	if usage == "" || !dfxMatch(`With -f it reads no pods and so deletes nothing`, usage) {
		t.Errorf("the switch usage text does not say -f deletes nothing: %q", dfxClip(usage))
	}
	if !dfxMatch(`refusing to delete claims with -f`, src) {
		t.Errorf("runSwitch no longer refuses to delete claims with -f")
	}
}
