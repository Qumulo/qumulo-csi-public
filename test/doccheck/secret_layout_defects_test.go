//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package doccheck

import "testing"

// The chart README shows the replication Secret with one token
// per array keyed by address and no plain "token" key, and its expand
// paragraph used to ask for the peer token "too", on top of a "token" key
// the example never showed. A lab built from the example had every expand
// refused. The driver now reads the address key first on every RPC; these checks keep the README saying so
// in both places, and keep the refusal naming the host whose key is missing.

const dfxChartReadme = "charts/qumulo-csi-driver/README.md"

// TestDefect_ReadmeSecretExampleSaysOneSecretServesEveryRPC: the
// text under the Secret example says the one Secret serves replication,
// expand and delete, and that the plain "token" key is optional.
func TestDefect_ReadmeSecretExampleSaysOneSecretServesEveryRPC(t *testing.T) {
	text := dfxBetween(t, dfxChartReadme, `name: qumulo-replication-tokens`, `\*\*What a .VolumeReplication. does`)
	if text == "" {
		t.Fatalf("%s has no qumulo-replication-tokens example followed by the VolumeReplication paragraph", dfxChartReadme)
	}
	if !dfxMatch(`one Secret serves replication, expand and delete`, text) {
		t.Errorf("the text under the qumulo-replication-tokens example does not say the one Secret serves replication, expand and delete: %s", dfxClip(text))
	}
	if !dfxMatch(`plain .token. key`, text) {
		t.Errorf("the text under the qumulo-replication-tokens example does not say what the plain token key is for: %s", dfxClip(text))
	}
}

// TestDefect_ReadmeExpandParagraphNamesTheReplicationSecret: the expand
// paragraph points at the replication Secret and no longer asks for the peer
// token "too".
func TestDefect_ReadmeExpandParagraphNamesTheReplicationSecret(t *testing.T) {
	text := dfxParagraph(t, dfxChartReadme, "Expanding a protected PVC")
	if text == "" {
		t.Fatalf("%s has no \"Expanding a protected PVC\" paragraph", dfxChartReadme)
	}
	if dfxMatch(`peer array's token too`, text) {
		t.Errorf("the expand paragraph still asks for the peer token \"too\", implying a plain token key on top of the address keys: %s", dfxClip(text))
	}
	if !dfxMatch(`controller-expand-secret-name[^.]{0,200}replication Secret`, text) {
		t.Errorf("the expand paragraph does not tell the reader to point controller-expand-secret-name at the replication Secret: %s", dfxClip(text))
	}
}

// TestDefect_NoCredentialMessageNamesTheDialledHost: the refusal says
// which host's key would have been accepted, so an operator holding an
// address-keyed Secret knows which key to add.
func TestDefect_NoCredentialMessageNamesTheDialledHost(t *testing.T) {
	src := dfxGoSource(t, "pkg/driver")
	if !dfxMatch(`no credential for cluster[^;]{0,300}the host this call dials`, src) {
		t.Errorf("the \"no credential for cluster\" message does not name the dialled host whose key would be accepted")
	}
}
