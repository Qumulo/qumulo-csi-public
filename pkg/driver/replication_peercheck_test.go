// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"testing"

	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// arrayC is a third array: reachable, answering, and holding no end of the
// A->B relationship. A class whose targetServer names it is a mistyped
// targetServer.
const arrayC = "c.example.com"

// peerCheckSecrets is replSecrets plus a token for C, so a class naming C
// resolves and C is really asked (the refusal must come from C's answer, not
// from a missing credential).
func peerCheckSecrets() map[string]string {
	s := replSecrets()
	s[arrayC] = "token-c"
	return s
}

// mutatingVerbs are the fabric verbs that change an array. Reads are not
// recorded at all; SnapshotDiffEmpty is, and changes nothing.
var mutatingVerbs = map[string]bool{
	"Authorize": true, "CreateExpiringSnapshot": true, "CreateExport": true, "CreatePolicy": true,
	"CreateQuota": true, "CreateSource": true, "DeleteSnapshot": true, "DeleteSource": true,
	"DeleteTarget": true, "MakeTargetWritable": true, "PatchSource": true, "Reconnect": true,
	"Replicate": true, "Reverse": true, "SetExportReadOnly": true, "UpdateQuota": true,
}

func mutations(calls []string) []string {
	var out []string
	for _, c := range calls {
		if mutatingVerbs[c] {
			out = append(out, c)
		}
	}
	return out
}

// misclass is one way a class can disagree with the relationship it is about.
type misclass struct {
	name   string
	params map[string]string
	// param is the class parameter the refusal must name.
	param string
}

// misclassesFromA are A's class for the A->B relationship, mistyped both ways
// the peer check covers: the right peer at the wrong path, and a reachable third array.
func misclassesFromA() []misclass {
	return []misclass{
		{"mistyped targetBasePath", classToB("targetBasePath", "/csi/other"), paramTargetBasePath},
		{"mistyped targetServer", classToward(arrayA, arrayC), paramTargetServer},
	}
}

// newPeerCheckScenario is a replScenario with C on the fabric and the volume
// protected A->B through the correct class.
func newPeerCheckScenario(t *testing.T) (*replScenario, *fakeArray, *fakeRel) {
	t.Helper()
	s := newReplScenario(t)
	c := s.fabric.addArray(arrayC)
	c.mkdir("/csi/volumes")
	rel := s.protect(t, classToB())
	s.a.calls, s.b.calls, c.calls = nil, nil, nil
	return s, c, rel
}

func (s *replScenario) enableWith(handle string, params, secrets map[string]string) error {
	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId: handle, Parameters: params, Secrets: secrets,
	})
	return err
}

func (s *replScenario) disableWith(handle string, params, secrets map[string]string) error {
	_, err := s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
		VolumeId: handle, Parameters: params, Secrets: secrets,
	})
	return err
}

func (s *replScenario) demoteWith(handle string, params, secrets map[string]string) error {
	_, err := s.server.DemoteVolume(context.Background(), &replication.DemoteVolumeRequest{
		VolumeId: handle, Parameters: params, Secrets: secrets,
	})
	return err
}

// assertRelUntouched is "nothing changed": both ends still there, still
// ESTABLISHED, A writable, B a read-only replica, and no array changed.
func assertRelUntouched(t *testing.T, s *replScenario, c *fakeArray, rel *fakeRel, what string) {
	t.Helper()
	assert.True(t, rel.hasSource, "%s: A's source end must not be deleted", what)
	assert.True(t, rel.hasTarget, "%s: B's target end must not be orphaned or deleted", what)
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state, what)
	assert.False(t, rel.sourceRO, "%s: A must not be frozen", what)
	assert.True(t, rel.targetRO, "%s: B stays a read-only replica", what)
	assert.Equal(t, 1, s.fabric.relCount(), "%s: no relationship is created toward the wrong peer", what)
	assert.Empty(t, mutations(s.a.calls), "%s: nothing changes on A", what)
	assert.Empty(t, mutations(s.b.calls), "%s: nothing changes on B", what)
	assert.Empty(t, mutations(c.calls), "%s: nothing changes on C", what)
}

// Found by probe with the two-array fake: only Promote and
// Resync checked that the configured peer holds an end of the relationship.
// With a class that disagrees with the relationship -- a mistyped
// targetBasePath, or a targetServer naming a third array that answers --
// Enable spun on Aborted ("does not exist on peer yet; retrying") forever,
// and Disable answered OK having deleted only A's source end: with C named,
// the delete of the peer end went to C (NotFound, read as done) and B's real
// target end was orphaned. Both now refuse with InvalidArgument naming the
// parameters, and change nothing anywhere.
func TestDisableAndEnableRefuseAClassWhosePeerHoldsNoEndOfTheRelationship(t *testing.T) {
	for _, mc := range misclassesFromA() {
		t.Run(mc.name, func(t *testing.T) {
			s, c, rel := newPeerCheckScenario(t)

			err := s.enableWith(s.handleA, mc.params, peerCheckSecrets())
			requireCode(t, err, codes.InvalidArgument)
			assert.Contains(t, err.Error(), "answered but lists no end")
			assert.Contains(t, err.Error(), mc.param)
			assertRelUntouched(t, s, c, rel, "enable")

			err = s.disableWith(s.handleA, mc.params, peerCheckSecrets())
			requireCode(t, err, codes.InvalidArgument)
			assert.Contains(t, err.Error(), "answered but lists no end")
			assert.Contains(t, err.Error(), mc.param)
			assertRelUntouched(t, s, c, rel, "disable")

			// The correct class still works, both ways.
			require.NoError(t, s.enable(t, classToB()))
			require.NoError(t, s.disable(t, s.handleA, classToB()))
			assert.Zero(t, s.fabric.relCount(), "the correct class removes both ends")
		})
	}
}

// The DR side's view of the same mistake: B's class names the wrong peer for
// B's target end. Disable must not remove B's end and answer OK while A's
// source end -- the real peer's -- is left behind.
func TestDisableOnTheReplicaRefusesAClassWhosePeerHoldsNoEnd(t *testing.T) {
	cases := []misclass{
		{"mistyped targetBasePath", classToA("targetBasePath", "/csi/other"), paramTargetBasePath},
		{"mistyped targetServer", classToward(arrayB, arrayC), paramTargetServer},
	}
	for _, mc := range cases {
		t.Run(mc.name, func(t *testing.T) {
			s, c, rel := newPeerCheckScenario(t)

			err := s.disableWith(s.handleB(t), mc.params, peerCheckSecrets())
			requireCode(t, err, codes.InvalidArgument)
			assert.Contains(t, err.Error(), mc.param)
			assertRelUntouched(t, s, c, rel, "disable on B")
		})
	}
}

// Demote: Demote never consulted the peer, so a class naming the wrong
// path or the wrong array froze A anyway -- and the promote on the other side
// that is meant to follow would then be refused (Promote checks), leaving A
// frozen with nobody taking over. Demote now asks the peer it is configured
// with and refuses when that peer answers holding no end.
func TestDemoteRefusesAClassWhosePeerHoldsNoEndOfTheRelationship(t *testing.T) {
	for _, mc := range misclassesFromA() {
		t.Run(mc.name, func(t *testing.T) {
			s, c, rel := newPeerCheckScenario(t)

			err := s.demoteWith(s.handleA, mc.params, peerCheckSecrets())
			requireCode(t, err, codes.InvalidArgument)
			assert.Contains(t, err.Error(), "answered but lists no end")
			assert.Contains(t, err.Error(), mc.param)
			assertRelUntouched(t, s, c, rel, "demote")

			// The correct class still freezes.
			require.NoError(t, s.demote(t, s.handleA, classToB()))
			assert.True(t, rel.sourceRO)
		})
	}
}

// The promoted DR copy: Demote fences its export read-only. A class naming
// the wrong peer is refused there too, before the fence.
func TestDemoteOfAPromotedTargetRefusesAClassWhosePeerHoldsNoEnd(t *testing.T) {
	s, c, rel := newPeerCheckScenario(t)
	s.a.down = true
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	s.a.down = false
	require.False(t, rel.targetRO)
	s.a.calls, s.b.calls = nil, nil

	err := s.demoteWith(s.handleB(t), classToward(arrayB, arrayC), peerCheckSecrets())
	requireCode(t, err, codes.InvalidArgument)
	assert.Contains(t, err.Error(), paramTargetServer)
	assert.Empty(t, mutations(s.b.calls), "the export is not fenced through a class naming the wrong peer")
	assert.Empty(t, mutations(c.calls))
	assert.False(t, s.b.exports[0].ReadOnly)
}

// What the check must NOT refuse.
//
// An ENDED local end is the corpse an out-of-band delete leaves: the peer
// holds nothing BECAUSE its end was removed. Enable still clears it and
// re-protects, Disable still removes it, Demote still reports it.
func TestPeerCheckLeavesAnEndedLocalEndToTheExistingHandling(t *testing.T) {
	t.Run("enable re-protects", func(t *testing.T) {
		s := newReplScenario(t)
		s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath, qumulo.RelationshipEnded, func(r *fakeRel) { r.hasTarget = false })
		require.NoError(t, s.enable(t, classToB()))
		rel := s.fabric.onlyRel()
		assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	})
	t.Run("disable removes the corpse", func(t *testing.T) {
		s := newReplScenario(t)
		s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath, qumulo.RelationshipEnded, func(r *fakeRel) { r.hasTarget = false })
		require.NoError(t, s.disable(t, s.handleA, classToB()))
		assert.Zero(t, s.fabric.relCount())
	})
	t.Run("demote reports it", func(t *testing.T) {
		s := newReplScenario(t)
		s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath, qumulo.RelationshipEnded, func(r *fakeRel) { r.hasTarget = false })
		require.NoError(t, s.demote(t, s.handleA, classToB()))
		assert.Empty(t, mutations(s.a.calls))
	})
}

// A fresh volume has no relationship anywhere, so there is nothing for the
// peer to hold: Enable toward any answering array -- C included -- creates
// one. The check is about a local end that exists, never about the absence
// of one (that is what a first Enable looks like).
func TestPeerCheckDoesNotRefuseAFirstEnable(t *testing.T) {
	s := newReplScenario(t)
	c := s.fabric.addArray(arrayC)
	c.mkdir("/csi/volumes")

	require.NoError(t, s.enableWith(s.handleA, classToward(arrayA, arrayC), peerCheckSecrets()))
	rel := s.fabric.onlyRel()
	assert.Same(t, c, rel.target)
}

// Demote still works with the peer down: the check needs the peer's
// ANSWER, and an unreachable peer falls back to this side's view, as before.
func TestDemoteWithThePeerDownStillFreezesAfterThePeerCheck(t *testing.T) {
	s, _, rel := newPeerCheckScenario(t)
	s.b.down = true

	require.NoError(t, s.demote(t, s.handleA, classToB()))
	assert.True(t, rel.sourceRO)
}

// A source end AWAITING_AUTHORIZATION that names exactly the class's peer and
// path is a create the peer has not listed yet (Core lists it a moment
// later): Enable keeps answering Aborted, "not yet visible", and the retry
// authorizes it. The same awaiting end naming another array is not this
// class's and is refused.
func TestPeerCheckWaitsOutAnAwaitingEndThePeerHasNotListedYet(t *testing.T) {
	t.Run("names the class's peer and path: retried", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath, qumulo.RelationshipAwaitingAuthorization,
			func(r *fakeRel) { r.hasTarget = false; r.targetRO = false })

		err := s.enable(t, classToB())
		requireCode(t, err, codes.Aborted)
		assert.Contains(t, err.Error(), "not yet visible")

		// The peer lists it now; the retry authorizes and stages.
		rel.hasTarget = true
		require.NoError(t, s.enable(t, classToB()))
		assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	})
	t.Run("names another array: refused", func(t *testing.T) {
		s := newReplScenario(t)
		c := s.fabric.addArray(arrayC)
		rel := s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath, qumulo.RelationshipAwaitingAuthorization,
			func(r *fakeRel) { r.targetRO = false })
		s.a.calls = nil

		err := s.enableWith(s.handleA, classToward(arrayA, arrayC), peerCheckSecrets())
		requireCode(t, err, codes.InvalidArgument)
		assert.Contains(t, err.Error(), paramTargetServer)
		assert.Equal(t, qumulo.RelationshipAwaitingAuthorization, rel.state)
		assert.Empty(t, mutations(s.a.calls))
		assert.Empty(t, mutations(c.calls), "C is never asked to authorize B's relationship")
	})
}
