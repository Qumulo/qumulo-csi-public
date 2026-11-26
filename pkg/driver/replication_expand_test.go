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
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// ControllerExpandVolume on a protected volume.
//
// The two halves are tested separately because they fail differently. The
// identity check is about a handle that no longer names the volume --
// the shape a DR-side PV has after a failover, since replication allocates a
// new file ID for the replica. The replica-first ordering is
// about which quota moves when: replication ignores the target quota and
// clients do not, so a source grown ahead of its replica is an ENOSPC the
// next promote delivers.

const expandTo = uint64(2 << 30)

func (s *replScenario) expand(t *testing.T, handle string, bytes uint64, secrets map[string]string) error {
	t.Helper()
	_, err := s.controller(t).ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      handle,
		CapacityRange: &csi.CapacityRange{RequiredBytes: int64(bytes)},
		Secrets:       secrets,
	})
	return err
}

func TestControllerExpandVolumeChecksTheHandleStillNamesTheVolume(t *testing.T) {
	tests := []struct {
		name     string
		handle   func(s *replScenario) string
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "a handle that still names the volume expands",
			handle:   func(s *replScenario) string { return s.handleA },
			wantCode: codes.OK,
		},
		{
			// A neighbour's path names the neighbour, and always did. What
			// changed is that this is no longer reachable by accident: the
			// retired handle carried a file ID, and after a failover that ID
			// usually named a directory like this one, so the case had to be
			// refused with FailedPrecondition and a manual PV repair. A
			// handle that names this path is simply about this volume.
			name: "a handle naming another directory expands THAT directory",
			handle: func(s *replScenario) string {
				s.a.quotas[s.a.mkdir("/csi/volumes/pvc-neighbour")] = replQuota
				return newVolumeID(arrayA, "", "/csi/volumes/pvc-neighbour").String()
			},
			wantCode: codes.OK,
		},
		{
			name: "a path that does not exist is NotFound",
			handle: func(s *replScenario) string {
				return newVolumeID(arrayA, "", "/csi/volumes/pvc-never-made").String()
			},
			wantCode: codes.NotFound,
			wantMsg:  "does not exist",
		},
		{
			// THE regression this whole change exists for. The same handle,
			// pointed at the DR array, used to name nothing there because it
			// carried the source array's file ID and the replica has its own
			// It now resolves by path and the expand
			// succeeds with no PersistentVolume edit.
			name: "the same handle on the peer array resolves and expands",
			handle: func(s *replScenario) string {
				s.b.quotas[s.b.mkdir(replVolumePath)] = replQuota
				// After the failover: B's copy is promoted, the live one.
				rel := s.fabric.onlyRel()
				rel.targetRO = false
				rel.state = qumulo.RelationshipDisconnected
				return newVolumeID(arrayB, "", replVolumePath).String()
			},
			wantCode: codes.OK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newReplScenario(t)
			require.NoError(t, s.enable(t, classToB()))
			handle := tt.handle(s)

			err := s.expand(t, handle, expandTo, deleteSecrets())

			if tt.wantCode == codes.OK {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err), "message was %v", err)
			assert.Contains(t, err.Error(), tt.wantMsg)
			assert.Equal(t, replQuota, s.a.quotas[s.fileIDA], "the source quota moved despite the refusal")
		})
	}
}

func TestControllerExpandVolumeRaisesTheReplicaQuotaFirst(t *testing.T) {
	s := newReplScenario(t)
	require.NoError(t, s.enable(t, classToB()))
	replicaID := s.b.dirs[replVolumePath]
	require.NotEmpty(t, replicaID)
	require.Equal(t, replQuota, s.b.quotas[replicaID], "enable pre-staged the source's limit")

	require.NoError(t, s.expand(t, s.handleA, expandTo, deleteSecrets()))

	assert.Equal(t, expandTo, s.b.quotas[replicaID], "the replica did not grow")
	assert.Equal(t, expandTo, s.a.quotas[s.fileIDA], "the source did not grow")
}

// The ordering is the whole point: if the peer cannot take the new limit, the
// source must not take it either, because the pair is only discovered to be
// wrong at promote.
func TestControllerExpandVolumeLeavesTheSourceAloneWhenThePeerCannotGrow(t *testing.T) {
	tests := []struct {
		name     string
		secrets  map[string]string
		prepare  func(s *replScenario)
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "the peer array does not answer",
			secrets:  deleteSecrets(),
			prepare:  func(s *replScenario) { s.b.down = true },
			wantCode: codes.Unavailable,
			wantMsg:  "unreachable",
		},
		{
			name: "the expand Secret carries no token for the peer",
			secrets: map[string]string{
				secretKeyToken: "token-a",
				arrayA:         "token-a",
			},
			wantCode: codes.FailedPrecondition,
			wantMsg:  "keyed by its address",
		},
		{
			name:    "the peer does not list the relationship as one it is the target of",
			secrets: deleteSecrets(),
			prepare: func(s *replScenario) {
				// The half-gone shape an interrupted disable leaves.
				s.fabric.onlyRel().hasTarget = false
			},
			wantCode: codes.Aborted,
			wantMsg:  "does not list relationship",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newReplScenario(t)
			require.NoError(t, s.enable(t, classToB()))
			replicaID := s.b.dirs[replVolumePath]
			if tt.prepare != nil {
				tt.prepare(s)
			}

			err := s.expand(t, s.handleA, expandTo, tt.secrets)

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err), "message was %v", err)
			assert.Contains(t, err.Error(), tt.wantMsg)
			assert.Equal(t, replQuota, s.a.quotas[s.fileIDA],
				"the source quota grew past the replica's, which is exactly the ENOSPC at promote")
			if !s.b.down {
				assert.Equal(t, replQuota, s.b.quotas[replicaID])
			}
		})
	}
}

// A volume that is nobody's source expands exactly as it did before: two
// status listings and then the quota.
func TestControllerExpandVolumeOnAnUnprotectedVolumeIsUnchanged(t *testing.T) {
	s := newReplScenario(t)

	require.NoError(t, s.expand(t, s.handleA, expandTo, deleteSecrets()))
	assert.Equal(t, expandTo, s.a.quotas[s.fileIDA])
}

// towardA is the expand Secret on the DR side, whose class names A as its
// peer -- which, for a read-only replica on B, is where the live copy is.
func towardA() map[string]string {
	return deleteSecrets(paramTargetServer, arrayA, paramTargetAPIURL, "https://"+arrayA+":8000")
}

// Raising a replica's quota by hand is the documented workaround, and it
// still raises the replica.: the same call is what a DNS-named claim's
// expand reaches after a failover, and raising only the replica left the live
// copy at its old size while the claim reported the new one. The array cannot
// tell the two apart, so the live copy on the source is raised too.
func TestControllerExpandVolumeOnAReplicaRaisesTheLiveCopyToo(t *testing.T) {
	s := newReplScenario(t)
	require.NoError(t, s.enable(t, classToB()))
	replicaID := s.b.dirs[replVolumePath]

	require.NoError(t, s.expand(t, s.handleB(t), expandTo, towardA()))

	assert.Equal(t, expandTo, s.b.quotas[replicaID], "the replica is raised, as the workaround needs")
	assert.Equal(t, expandTo, s.a.quotas[s.fileIDA], "and the live copy with it")
}

// The live copy is never lowered from the replica's side, and a source this
// side cannot find or reach changes nothing.
func TestControllerExpandVolumeOnAReplicaEdges(t *testing.T) {
	t.Run("a larger live copy keeps its limit", func(t *testing.T) {
		s := newReplScenario(t)
		require.NoError(t, s.enable(t, classToB()))
		s.a.quotas[s.fileIDA] = 5 << 30

		require.NoError(t, s.expand(t, s.handleB(t), expandTo, towardA()))

		assert.Equal(t, uint64(5<<30), s.a.quotas[s.fileIDA])
	})

	t.Run("no source named is refused with the remedy", func(t *testing.T) {
		s := newReplScenario(t)
		require.NoError(t, s.enable(t, classToB()))
		replicaID := s.b.dirs[replVolumePath]

		err := s.expand(t, s.handleB(t), expandTo, deleteSecrets())

		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), paramTargetServer)
		assert.Equal(t, replQuota, s.b.quotas[replicaID], "nothing changed")
	})

	t.Run("an unreachable source is Unavailable", func(t *testing.T) {
		s := newReplScenario(t)
		require.NoError(t, s.enable(t, classToB()))
		replicaID := s.b.dirs[replVolumePath]
		s.a.down = true

		err := s.expand(t, s.handleB(t), expandTo, towardA())

		requireCode(t, err, codes.Unavailable)
		assert.Equal(t, replQuota, s.b.quotas[replicaID], "nothing changed")
	})
}

// A promoted copy -- a writable target end -- is the live one, and raising
// its own quota touches nothing else.
func TestControllerExpandVolumeOnAPromotedCopyRaisesOnlyItsOwnQuota(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.targetRO = false
	rel.state = qumulo.RelationshipDisconnected
	replicaID := s.b.dirs[replVolumePath]

	require.NoError(t, s.expand(t, s.handleB(t), expandTo, deleteSecrets()))

	assert.Equal(t, expandTo, s.b.quotas[replicaID])
	assert.Equal(t, replQuota, s.a.quotas[s.fileIDA], "the stale old primary is not grown")
}

// An ENDED relationship names no peer to mirror onto and protects nothing.
func TestControllerExpandVolumeIgnoresAnEndedRelationship(t *testing.T) {
	s := newReplScenario(t)
	s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath, qumulo.RelationshipEnded, nil)

	require.NoError(t, s.expand(t, s.handleA, expandTo, deleteSecrets()))
	assert.Equal(t, expandTo, s.a.quotas[s.fileIDA])
}

// The old primary's claim is expanded, or re-enabled, while the peer
// holds the promoted, writable copy. The peer's quota is the live copy's, not
// a replica's, and the old primary must leave it alone: neither set it down
// to its own size (the live copy grew during the DR period) nor up (the stale
// side does not decide the live copy's size).
//
// The expand used to go on to raise the old primary's own quota and answer
// success, which is in its unplanned-failover shape: a claim reading the
// new size over a live copy that never grew. It is refused now, naming the
// promoted side (replication_stale_source_test.go); the live copy is still
// left alone, which is what this test is about.
func TestTheOldPrimaryLeavesThePromotedCopysQuotaAlone(t *testing.T) {
	for _, tt := range []struct {
		name string
		live uint64 // the promoted copy's quota on B
	}{
		{name: "a live copy grown past the old primary", live: 5 << 30},
		{name: "a live copy smaller than the old primary", live: 1 << 29},
	} {
		promotedScenario := func(t *testing.T) (*replScenario, string) {
			s := newReplScenario(t)
			rel := s.protect(t, classToB())
			rel.targetRO = false
			rel.state = qumulo.RelationshipDisconnected // B promoted and live
			liveID := s.b.dirs[replVolumePath]
			s.b.quotas[liveID] = tt.live
			return s, liveID
		}

		t.Run("expand: "+tt.name, func(t *testing.T) {
			s, liveID := promotedScenario(t)

			err := s.expand(t, s.handleA, 2<<30, deleteSecrets())

			requireCode(t, err, codes.FailedPrecondition)
			assert.Equal(t, tt.live, s.b.quotas[liveID], "the live copy's quota is untouched")
			assert.Equal(t, replQuota, s.a.quotas[s.fileIDA], "and so is the stale primary's")
		})

		// csi-addons calls Enable before every Promote, so setting the old
		// primary's VolumeReplication to primary runs Enable on it. That
		// used to re-stage the old primary's limit onto the live copy before
		// the Promote was refused.
		t.Run("enable: "+tt.name, func(t *testing.T) {
			s, liveID := promotedScenario(t)
			s.a.quotas[s.a.dirs[replVolumePath]] = 2 << 30

			require.NoError(t, s.enable(t, classToB()))
			assert.Equal(t, tt.live, s.b.quotas[liveID], "the live copy's quota is untouched")
			requireCode(t, s.promote(t, s.handleA, classToB(), false), codes.Aborted)
			assert.Equal(t, tt.live, s.b.quotas[liveID])
		})
	}
}

// "never lower a replica's limit". An ordinary read-only replica that
// is already larger than the new size keeps its limit; one that is smaller
// is still raised first, which is what the expand ordering exists for.
func TestExpandNeverLowersAReplicasQuota(t *testing.T) {
	for _, tt := range []struct {
		name          string
		replica, want uint64
	}{
		{name: "a larger replica keeps its limit", replica: 5 << 30, want: 5 << 30},
		{name: "a smaller replica is raised first", replica: 1 << 30, want: 2 << 30},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newReplScenario(t)
			s.protect(t, classToB())
			replicaID := s.b.dirs[replVolumePath]
			s.b.quotas[replicaID] = tt.replica

			require.NoError(t, s.expand(t, s.handleA, 2<<30, deleteSecrets()))

			assert.Equal(t, tt.want, s.b.quotas[replicaID])
		})
	}
}

// As reproduced live: after a failover to B and the reverse that makes
// A B's read-only replica, the original claim's handle still names A. Its
// expand used to grow A's quota only -- the replica -- and leave B, the live
// copy, at the old size while the PVC read the new one.
func TestExpandOfTheOriginalClaimAfterAFailoverGrowsTheLiveCopy(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	_, err := s.resync(t, s.handleA, classToB(), true)
	require.NoError(t, err)
	rel := s.fabric.onlyRel()
	require.Equal(t, s.b, rel.source, "reversed: B is the source")
	require.True(t, rel.targetRO, "and A its read-only replica")
	liveID := s.b.dirs[replVolumePath]

	// A's claim, with A's class Secret: its peer is B.
	require.NoError(t, s.expand(t, s.handleA, expandTo,
		deleteSecrets(paramTargetServer, arrayB, paramTargetAPIURL, "https://"+arrayB+":8000")))

	assert.Equal(t, expandTo, s.b.quotas[liveID], "the live copy on B has the size the claim says")
	assert.Equal(t, expandTo, s.a.quotas[s.fileIDA], "and the replica on A is not smaller")
}

// Expand's guard knows the relationship's ID, and the peer's
// role-specific status endpoint answers 404 for the other role, so it reads
// that one relationship instead of listing the peer's whole table.
func TestExpandReadsThePeersEndByIDNotByListing(t *testing.T) {
	t.Run("on the primary: the replica's end on the peer", func(t *testing.T) {
		s := newReplScenario(t)
		s.protect(t, classToB())
		before := s.b.statusListings
		require.NoError(t, s.expand(t, s.handleA, 2<<30, deleteSecrets()))
		assert.Equal(t, before, s.b.statusListings, "the peer's tables are not listed")
	})
	t.Run("on a replica: the live copy's end on the peer", func(t *testing.T) {
		s := newReplScenario(t)
		s.protect(t, classToB())
		require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
		_, err := s.resync(t, s.handleA, classToB(), true)
		require.NoError(t, err)
		liveID := s.b.dirs[replVolumePath]
		before := s.b.statusListings
		require.NoError(t, s.expand(t, s.handleA, expandTo,
			deleteSecrets(paramTargetServer, arrayB, paramTargetAPIURL, "https://"+arrayB+":8000")))
		assert.Equal(t, expandTo, s.b.quotas[liveID])
		assert.Equal(t, before, s.b.statusListings, "the peer's tables are not listed")
	})
}
