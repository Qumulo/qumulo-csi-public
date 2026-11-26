// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// The group protection fan-out,. See replication_protection.go.
//
// Every test here calls Info once, for the dataSource member only, because
// that is the only call csi-addons makes: a group has one VolumeReplication
// and the sidecar reports on its dataSource. A test that calls Info for each
// member in turn passes whether or not the defect is present, which is how
// this shipped broken.

// withPVs installs a PersistentVolume lister holding the given handles, as a
// controller in a cluster has.
func (s *sharedScenario) withPVs(handles ...string) *stubPVLister {
	pvs := make([]PersistentVolumeInfo, 0, len(handles))
	for i, handle := range handles {
		pvs = append(pvs, PersistentVolumeInfo{
			Name: fmt.Sprintf("pv-%d", i), Handle: handle, Phase: "Bound",
		})
	}
	lister := &stubPVLister{pvs: pvs}
	s.driver.PersistentVolumeLister = lister
	return lister
}

// allMemberHandles is the three members newSharedScenario provisions.
func (s *sharedScenario) allMemberHandles() []string {
	return []string{
		s.memberHandle("pvc-tenant-a"),
		s.memberHandle("pvc-tenant-b"),
		s.memberHandle("pvc-tenant-c"),
	}
}

// verdicts is the protection verdict recorded per subject on A, latest wins.
// Subjects are volume handles: an unlabelled event is dropped by the
// exporter, so a test that ignored the subject would pass on a verdict that
// reaches no series.
func (s *replScenario) verdicts() map[string]bool {
	s.fabric.mu.Lock()
	defer s.fabric.mu.Unlock()
	out := map[string]bool{}
	for i, subject := range s.a.protectionSubjects {
		out[subject] = s.a.protectionVerdicts[i]
	}
	return out
}

// The defect itself: one VolumeReplication, one Info call, and every member
// of the group must end up with a verdict. Before the fix only the
// dataSource had one, and QumuloCSIVolumeUnprotected -- which is
// `… == 0` -- cannot fire for a series that does not exist.
func TestGroupProtectionIsExportedForEveryMember(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	s.withPVs(s.allMemberHandles()...)
	dataSource := s.memberHandle("pvc-tenant-a")

	_, err := s.server.AssessVolumeReplication(context.Background(), dataSource, "", nil, replSecrets())

	require.NoError(t, err)
	verdicts := s.verdicts()
	for _, member := range s.allMemberHandles() {
		require.Contains(t, verdicts, member,
			"member %s has no protected series at all, so the unprotected alert cannot see it", member)
		assert.True(t, verdicts[member], "member %s reads unprotected while its group is replicating", member)
	}
}

// The case the alert exists for. When the group's relationship has gone,
// every member must be set to 0 -- not left without a series, which reads as
// silence rather than as a problem. The members have no relationship of
// their own to find, so this uses the root the last report remembered.
func TestGroupProtectionZeroesEveryMemberWhenTheRelationshipGoes(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	s.withPVs(s.allMemberHandles()...)
	dataSource := s.memberHandle("pvc-tenant-a")
	_, err := s.server.AssessVolumeReplication(context.Background(), dataSource, "", nil, replSecrets())
	require.NoError(t, err)

	require.NoError(t, s.disableGroup(t, groupClass()))
	_, err = s.server.AssessVolumeReplication(context.Background(), dataSource, "", nil, replSecrets())

	requireCode(t, err, codes.FailedPrecondition)
	verdicts := s.verdicts()
	for _, member := range s.allMemberHandles() {
		require.Contains(t, verdicts, member, "member %s lost its series instead of reading 0", member)
		assert.False(t, verdicts[member],
			"member %s still reads protected after the group's relationship went", member)
	}
}

// The regression this fix could introduce. A sibling class whose base
// path merely starts with the group root's characters is not under it:
// /csi/shared-extra is not inside /csi/shared. Reporting an unprotected
// volume as protected is strictly worse than the silence being fixed.
func TestGroupProtectionSkipsAClassWithAPrefixOfTheGroupRoot(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	sibling := newVolumeID(arrayA, "", path.Join(sharedBasePath+"-extra", "pvc-sibling")).String()
	s.withPVs(append(s.allMemberHandles(), sibling)...)

	_, err := s.server.AssessVolumeReplication(
		context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())

	require.NoError(t, err)
	assert.NotContains(t, s.verdicts(), sibling,
		"a volume under a different class was reported as protected by this group's relationship")
}

// The other half of that: the DR copies live at the same paths on the peer
// array. A replica is not protected by its own source's relationship, and
// the handle's server and endpoint are what tell them apart.
func TestGroupProtectionSkipsTheCopyOnThePeerArray(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	drCopy := newVolumeID(arrayB, "", path.Join(sharedBasePath, "pvc-tenant-b")).String()
	s.withPVs(append(s.allMemberHandles(), drCopy)...)

	_, err := s.server.AssessVolumeReplication(
		context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())

	require.NoError(t, err)
	assert.NotContains(t, s.verdicts(), drCopy,
		"the DR copy on the peer array was reported as protected by the source's relationship")
}

// Outside a cluster -- csi-sanity, a local run -- there is no
// PersistentVolume lister. The report must still be made, with the single
// subject it always had: a fan-out that cannot run is not an error.
func TestGroupProtectionWithoutAListerStillReportsTheVolumeItself(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	dataSource := s.memberHandle("pvc-tenant-a")
	require.Nil(t, s.driver.PersistentVolumeLister)

	health, err := s.server.AssessVolumeReplication(context.Background(), dataSource, "", nil, replSecrets())

	require.NoError(t, err, "Info must not fail because it could not reach the Kubernetes API")
	assert.NotEmpty(t, health.RelationshipID)
	verdicts := s.verdicts()
	assert.True(t, verdicts[dataSource], "the volume Info named must still report its own verdict")
	assert.NotContains(t, verdicts, s.memberHandle("pvc-tenant-b"),
		"with no lister there is no member list, so no other member can have a verdict")
}

// A listing that fails is not an empty one. The report is still made, and
// nothing is zeroed on the strength of a Kubernetes API that did not answer.
func TestGroupProtectionSurvivesAFailedPersistentVolumeList(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	lister := s.withPVs(s.allMemberHandles()...)
	lister.err = errors.New("the API server is unreachable")
	dataSource := s.memberHandle("pvc-tenant-a")

	_, err := s.server.AssessVolumeReplication(context.Background(), dataSource, "", nil, replSecrets())

	require.NoError(t, err)
	assert.True(t, s.verdicts()[dataSource], "the volume Info named must still report its own verdict")
}

// Info is polled continuously for every VolumeReplication in the
// cluster, so the PersistentVolume list is cached rather than read per RPC.
func TestGroupProtectionDoesNotListPersistentVolumesPerCall(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	lister := s.withPVs(s.allMemberHandles()...)
	dataSource := s.memberHandle("pvc-tenant-a")

	for range 5 {
		_, err := s.server.AssessVolumeReplication(context.Background(), dataSource, "", nil, replSecrets())
		require.NoError(t, err)
	}

	lister.mu.Lock()
	defer lister.mu.Unlock()
	assert.Equal(t, 1, lister.calls,
		"five polls listed PersistentVolumes %d times; Info is polled continuously and must reuse the listing",
		lister.calls)
}

// A volume with no group covering it is reported on its own, and no fan-out
// happens: there is no root to fan out from.
func TestGroupProtectionDoesNotTouchADedicatedExportVolume(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enable(t, classToB()))
	s.withPVs(append(s.allMemberHandles(), s.handleA)...)

	_, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())

	require.NoError(t, err)
	verdicts := s.verdicts()
	assert.True(t, verdicts[s.handleA])
	for _, member := range s.allMemberHandles() {
		assert.NotContains(t, verdicts, member,
			"a volume with its own relationship fanned its verdict out to an unrelated class")
	}
}
