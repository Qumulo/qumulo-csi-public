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

	"qumulo-csi-driver/pkg/qumulo"
)

// The stale-source shape of and.
//
// The first fix for both keyed on "the array the handle names holds a
// read-only replica", which is what a planned failover leaves behind: the
// relationship is reversed and A is B's replica. An unplanned failover
// leaves something else. The DR side is promoted while A is down, and when
// A comes back it is still the SOURCE end of the relationship -- writable,
// behind the live copy, and (measured live) still reporting ESTABLISHED --
// until someone demotes it. In that window an expand of the unchanged claim
// raised A's quota alone and reported success (the claim said 2 GiB, the
// live copy on B stayed at 1), a snapshot captured A's stale tree as
// ReadyToUse, and a delete took A's directory and ended the relationship
// from the stale side. Every one of those is refused now, naming the
// promoted copy and the remedy. Since A's own end does not say, the peer is
// asked, and a peer that cannot be asked is a refusal too.

// staleSourceScenario is A as the stale source after a reverse-less promote:
// B's end promoted (disconnected and writable) while A's source end is still
// there, reading DISCONNECTED.
func staleSourceScenario(t *testing.T) (*replScenario, *fakeRel, string) {
	t.Helper()
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.targetRO = false
	rel.state = qumulo.RelationshipDisconnected
	return s, rel, s.b.dirs[replVolumePath]
}

// unplannedFailoverScenario is the measured shape: B force-promoted while A
// was unreachable, then A back. A's source end still reads ESTABLISHED.
func unplannedFailoverScenario(t *testing.T) (*replScenario, *fakeRel, string) {
	t.Helper()
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.a.down = true
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	s.a.down = false
	require.Equal(t, s.a, rel.source, "not reversed: A is still the source end")
	require.False(t, rel.targetRO, "B holds the promoted, writable copy")
	return s, rel, s.b.dirs[replVolumePath]
}

// secretsWithoutThePeersToken is the local array's token alone: the Secret of
// a class that was never told about the peer.
func secretsWithoutThePeersToken() map[string]string {
	return map[string]string{secretKeyToken: "token-a", arrayA: "token-a"}
}

// countSnapshots counts the snapshots an array is asked to take. The fabric
// models policy and marker snapshots, not CSI ones, so those go through the
// embedded mock.
func countSnapshots(a *fakeArray) *int {
	taken := 0
	a.CreateSnapshotFunc = func(_ context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
		taken++
		return qumulo.SnapshotInfo{}, nil
	}
	return &taken
}

func TestExpandOfAStaleSourceIsRefusedNamingThePromotedCopy(t *testing.T) {
	s, _, liveID := staleSourceScenario(t)
	s.b.quotas[liveID] = 5 << 30 // the live copy grew during the DR period

	err := s.expand(t, s.handleA, 2<<30, deleteSecrets())

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), arrayB, "the refusal names the promoted side")
	assert.Contains(t, err.Error(), "promoted")
	assert.Equal(t, replQuota, s.a.quotas[s.fileIDA], "the stale source's quota is untouched")
	assert.Equal(t, uint64(5<<30), s.b.quotas[liveID], "the live copy's quota is untouched")
}

// A disconnected source whose peer is still a read-only replica -- a break
// without a promote -- is not stale, and its expand goes ahead as before:
// replica first, then this side.
func TestExpandOfADisconnectedSourceWhosePeerIsStillAReplicaGoesAhead(t *testing.T) {
	s, rel, replicaID := staleSourceScenario(t)
	rel.targetRO = true

	require.NoError(t, s.expand(t, s.handleA, expandTo, deleteSecrets()))

	assert.Equal(t, expandTo, s.b.quotas[replicaID])
	assert.Equal(t, expandTo, s.a.quotas[s.fileIDA])
}

func TestSnapshotOfAStaleSourceIsRefused(t *testing.T) {
	s, _, _ := staleSourceScenario(t)
	taken := countSnapshots(s.a)

	_, err := s.controller(t).CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: s.handleA, Name: "snapshot-of-the-stale-source", Secrets: deleteSecrets(),
	})

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), arrayB, "the refusal names the promoted side")
	assert.Zero(t, *taken, "no snapshot was taken on the stale source")
}

// Without the peer's token the question "has the peer been promoted" cannot
// be asked, and a disconnected source is exactly where the answer matters. The
// snapshot is refused with the remedy rather than taken on faith.
func TestSnapshotOfADisconnectedSourceWithoutThePeersTokenIsRefused(t *testing.T) {
	s, _, _ := staleSourceScenario(t)
	taken := countSnapshots(s.a)

	_, err := s.controller(t).CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: s.handleA, Name: "snapshot-without-peer-token", Secrets: secretsWithoutThePeersToken(),
	})

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "token")
	assert.Zero(t, *taken)
}

func TestSnapshotOfADisconnectedSourceWhosePeerIsUnreachableIsRefused(t *testing.T) {
	s, _, _ := staleSourceScenario(t)
	s.b.down = true
	taken := countSnapshots(s.a)

	_, err := s.controller(t).CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: s.handleA, Name: "snapshot-peer-down", Secrets: deleteSecrets(),
	})

	require.Error(t, err)
	assert.Zero(t, *taken, "nothing is snapshotted while the peer's state is unknown")
}

// The measured unplanned shape: A's end still reads ESTABLISHED, and only
// the peer knows it has been promoted. All three calls are refused.
func TestAnEstablishedSourceWhosePeerWasPromotedIsRefused(t *testing.T) {
	t.Run("expand", func(t *testing.T) {
		s, _, liveID := unplannedFailoverScenario(t)
		err := s.expand(t, s.handleA, expandTo, deleteSecrets())
		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), arrayB)
		assert.Equal(t, replQuota, s.a.quotas[s.fileIDA])
		assert.Equal(t, replQuota, s.b.quotas[liveID])
	})
	t.Run("snapshot", func(t *testing.T) {
		s, _, _ := unplannedFailoverScenario(t)
		taken := countSnapshots(s.a)
		_, err := s.controller(t).CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
			SourceVolumeId: s.handleA, Name: "snapshot-after-unplanned-failover", Secrets: deleteSecrets(),
		})
		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), arrayB)
		assert.Zero(t, *taken)
	})
	t.Run("delete", func(t *testing.T) {
		s, _, _ := unplannedFailoverScenario(t)
		_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: s.handleA, Secrets: deleteSecrets(paramReplicationDeletePolicy, replicationDeletePolicyDisableOrphan),
		})
		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), arrayB)
		_, stillThere := s.a.dirs[replVolumePath]
		assert.True(t, stillThere)
		assert.Equal(t, 1, s.fabric.relCount())
	})
}

// A healthy protected source snapshots as before when the Secret carries the
// peer's token: one status read on the peer says its end is a read-only
// replica, and the snapshot is taken.
func TestSnapshotOfAHealthyProtectedSourceGoesAheadWithThePeersToken(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	taken := countSnapshots(s.a)
	before := s.b.statusListings

	_, err := s.controller(t).CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: s.handleA, Name: "snapshot-of-a-healthy-source", Secrets: deleteSecrets(),
	})

	require.NoError(t, err)
	assert.Equal(t, 1, *taken)
	assert.Equal(t, before, s.b.statusListings, "the peer's end is read by ID, not by listing its table")
}

// Without the peer's token the source end's own state cannot settle whether
// the peer was promoted (it reads ESTABLISHED either way), so a protected
// source is refused with the remedy rather than snapshotted on faith. An
// unprotected volume needs no peer and is snapshotted as always.
func TestSnapshotOfAProtectedSourceWithoutThePeersTokenIsRefused(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	taken := countSnapshots(s.a)

	_, err := s.controller(t).CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: s.handleA, Name: "snapshot-without-peer-token", Secrets: secretsWithoutThePeersToken(),
	})

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "token")
	assert.Zero(t, *taken)
}

func TestSnapshotOfAnUnprotectedVolumeNeedsNoPeerToken(t *testing.T) {
	s := newReplScenario(t)
	taken := countSnapshots(s.a)

	_, err := s.controller(t).CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: s.handleA, Name: "snapshot-of-an-unprotected-volume", Secrets: secretsWithoutThePeersToken(),
	})

	require.NoError(t, err)
	assert.Equal(t, 1, *taken)
}

// A disconnected source whose peer still holds a read-only replica snapshots
// as before.
func TestSnapshotOfADisconnectedSourceWhosePeerIsStillAReplicaGoesAhead(t *testing.T) {
	s, rel, _ := staleSourceScenario(t)
	rel.targetRO = true
	taken := countSnapshots(s.a)

	_, err := s.controller(t).CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: s.handleA, Name: "snapshot-after-a-break", Secrets: deleteSecrets(),
	})

	require.NoError(t, err)
	assert.Equal(t, 1, *taken)
}

// No delete policy lets the stale source be deleted: the directory is the
// relationship's source root, and taking it ends the relationship from the
// side that is not live.
func TestDeleteOfAStaleSourceIsRefusedWhateverThePolicy(t *testing.T) {
	for _, policy := range []string{"", replicationDeletePolicyRefuse, replicationDeletePolicyDisable, replicationDeletePolicyDisableOrphan} {
		t.Run("policy "+policy, func(t *testing.T) {
			s, _, _ := staleSourceScenario(t)
			secrets := deleteSecrets()
			if policy != "" {
				secrets = deleteSecrets(paramReplicationDeletePolicy, policy)
			}

			_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
				VolumeId: s.handleA, Secrets: secrets,
			})

			requireCode(t, err, codes.FailedPrecondition)
			assert.Contains(t, err.Error(), arrayB, "the refusal names the promoted side")
			_, stillThere := s.a.dirs[replVolumePath]
			assert.True(t, stillThere, "the stale source's directory was deleted")
			require.Equal(t, 1, s.fabric.relCount(), "the relationship is still there")
			assert.Equal(t, qumulo.RelationshipDisconnected, s.fabric.onlyRel().state)
		})
	}
}

// A delete that cannot ask the peer is settled by its own policy, as it
// always was: refused by default, and under disable-orphan the operator's
// consent to proceed without the peer stands. Only a peer that answers
// "promoted" is beyond every policy.
func TestDeleteOfASourceWhosePeerCannotBeAskedFollowsThePolicy(t *testing.T) {
	t.Run("refused by default", func(t *testing.T) {
		s, _, _ := staleSourceScenario(t)
		_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: s.handleA, Secrets: secretsWithoutThePeersToken(),
		})
		requireCode(t, err, codes.FailedPrecondition)
		_, stillThere := s.a.dirs[replVolumePath]
		assert.True(t, stillThere)
		assert.Equal(t, 1, s.fabric.relCount())
	})
	t.Run("disable-orphan is the operator's consent", func(t *testing.T) {
		s, _, _ := staleSourceScenario(t)
		secrets := secretsWithoutThePeersToken()
		secrets[paramReplicationDeletePolicy] = replicationDeletePolicyDisableOrphan
		_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: s.handleA, Secrets: secrets,
		})
		require.NoError(t, err)
		_, stillThere := s.a.dirs[replVolumePath]
		assert.False(t, stillThere, "deleted on the operator's consent, as before")
	})
}

// Once the stale side is demoted and the relationship reconnected, A is B's
// read-only replica and the three calls behave as the first fix made them:
// expand grows both copies, snapshot and delete are refused as a replica's.
func TestAfterTheStaleSourceIsDemotedTheReplicaRulesApply(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	_, err := s.resync(t, s.handleA, classToB(), true)
	require.NoError(t, err)
	rel := s.fabric.onlyRel()
	require.Equal(t, s.b, rel.source, "reversed: B is the source")
	require.True(t, rel.targetRO, "A is its read-only replica")
	liveID := s.b.dirs[replVolumePath]

	require.NoError(t, s.expand(t, s.handleA, expandTo,
		deleteSecrets(paramTargetServer, arrayB, paramTargetAPIURL, "https://"+arrayB+":8000")))
	assert.Equal(t, expandTo, s.b.quotas[liveID])

	_, err = s.controller(t).CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: s.handleA, Name: "snapshot-of-the-replica", Secrets: deleteSecrets(),
	})
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "read-only replica")

	requireCode(t, s.deleteVolume(t, s.handleA), codes.FailedPrecondition)
}
