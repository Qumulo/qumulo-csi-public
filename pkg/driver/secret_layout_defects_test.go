//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// ===========================================================================
// The chart README's replication Secret -- one token per array,
// keyed by the array's address, no plain "token" key -- must serve expand
// and delete the way it serves the replication service. Found in a DR
// regression run, where a lab built from that example
// had every ControllerExpandVolume refused with "carried no token secret"
// and was scored a regression on it.
// ===========================================================================

// addressKeyedSecrets is the README's layout: no secretKeyToken entry.
func addressKeyedSecrets() map[string]string {
	return map[string]string{arrayA: "token-a", arrayB: "token-b"}
}

// lastToken is the bearer token the fabric was last dialled with for host.
func lastToken(s *replScenario, host string) string {
	tokens := s.fabric.tokens[host]
	if len(tokens) == 0 {
		return ""
	}
	return tokens[len(tokens)-1]
}

// TestDefect_ExpandAcceptsAnAddressKeyedSecret: a protected volume
// expands with the README's Secret, and array A is dialled with A's token.
func TestDefect_ExpandAcceptsAnAddressKeyedSecret(t *testing.T) {
	s := newReplScenario(t)
	require.NoError(t, s.enable(t, classToB()))

	require.NoError(t, s.expand(t, s.handleA, expandTo, addressKeyedSecrets()))

	assert.Equal(t, "token-a", lastToken(s, arrayA), "array A was not dialled with the token keyed by its address")
	assert.Equal(t, "token-b", lastToken(s, arrayB), "the replica-first step did not reach array B with B's token")
}

// TestDefect_DeleteAcceptsAnAddressKeyedSecret: an unprotected volume
// is deleted with the README's Secret; nothing in DeleteVolume still needs
// the plain key.
func TestDefect_DeleteAcceptsAnAddressKeyedSecret(t *testing.T) {
	s := newReplScenario(t)

	_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: s.handleA, Secrets: addressKeyedSecrets(),
	})

	require.NoError(t, err)
	assert.NotContains(t, s.a.dirs, replVolumePath, "the volume directory was not deleted")
	assert.Equal(t, "token-a", lastToken(s, arrayA))
}

// TestDefect_TheAddressKeyWinsOverThePlainToken: with both keys
// present the address key is read first, as the replication service does,
// so one Secret is read by one rule on every RPC.
func TestDefect_TheAddressKeyWinsOverThePlainToken(t *testing.T) {
	s := newReplScenario(t)
	secrets := addressKeyedSecrets()
	secrets[arrayA] = "token-a-by-address"
	secrets[secretKeyToken] = "token-a-plain"

	require.NoError(t, s.expand(t, s.handleA, expandTo, secrets))

	assert.Equal(t, "token-a-by-address", lastToken(s, arrayA))
}

// TestDefect_ThePlainTokenStillWorksAlone: a StorageClass Secret that
// holds only "token" is unchanged by the new rule.
func TestDefect_ThePlainTokenStillWorksAlone(t *testing.T) {
	s := newReplScenario(t)

	require.NoError(t, s.expand(t, s.handleA, expandTo, map[string]string{secretKeyToken: "token-a"}))

	assert.Equal(t, "token-a", lastToken(s, arrayA))
}

// TestDefect_AnotherHostsKeyIsNotACredential: a Secret keyed by the
// StorageClass server does not authenticate a call that dials a different
// apiUrl host. Only the host actually dialled is read, and the
// refusal names that host so the operator knows which key to add.
func TestDefect_AnotherHostsKeyIsNotACredential(t *testing.T) {
	s := newReplScenario(t)
	elsewhere := newVolumeID(arrayA, "https://api.example.com:8000", replVolumePath).String()

	err := s.expand(t, elsewhere, expandTo, map[string]string{arrayA: "token-a"})

	requireCode(t, err, codes.InvalidArgument)
	assert.Contains(t, err.Error(), `"api.example.com"`, "the refusal does not name the host whose key is missing")
	assert.Empty(t, s.fabric.tokens["api.example.com"], "the array was dialled although no credential was found")
}
