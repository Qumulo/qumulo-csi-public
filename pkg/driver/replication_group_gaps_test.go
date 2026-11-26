// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"bytes"
	"context"
	"path"
	"strings"
	"testing"

	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// The group paths the unit suite did not reach -- verifyMemberQuotas'
// rows (peer down, member missing on the promoted copy, a replica quota
// smaller than the source's), an empty group, a group root removed after
// protection, and a second VolumeReplication on the same group class.

// verifyMemberQuotas never refuses -- the copy is writable by the time it
// runs -- so what it found is only in the log (captureWarnings).

// logLine returns the one logged line containing msg, or "".
func logLine(logged *bytes.Buffer, msg string) string {
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, msg) {
			return line
		}
	}
	return ""
}

func groupSourceFor(root string) *replication.ReplicationSource {
	return &replication.ReplicationSource{
		Type: &replication.ReplicationSource_Volumegroup{
			Volumegroup: &replication.ReplicationSource_VolumeGroupSource{VolumeGroupId: root},
		},
	}
}

// replicaMemberHandle is a member's handle on B, as the DR-side PV carries it.
func replicaMemberHandle(name string) string {
	return newVolumeID(arrayB, "", path.Join(sharedBasePath, name)).String()
}

// promoteGroupOnB promotes the group from B's side, through tenant-a.
func (s *sharedScenario) promoteGroupOnB(force bool) error {
	_, err := s.server.PromoteVolume(context.Background(), &replication.PromoteVolumeRequest{
		VolumeId:          replicaMemberHandle("pvc-tenant-a"),
		Parameters:        classToA(),
		Secrets:           replSecrets(),
		Force:             force,
		ReplicationSource: groupSourceFor(sharedBasePath),
	})
	return err
}

// rmdir removes a directory and everything under it from a fake array.
func (a *fakeArray) rmdir(p string) {
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	for dir, id := range a.dirs {
		if dir == p || strings.HasPrefix(dir, p+"/") {
			delete(a.dirs, dir)
			delete(a.ids, id)
			delete(a.quotas, id)
		}
	}
}

// The non-forced raise path (replication_group.go, verifyMemberQuotas: a
// member present with a smaller limit). An expand that did not reach the
// replica leaves it at the old size; the planned promote reads the source's
// limits and raises it, and names the member.
func TestPlannedGroupPromoteRaisesAMemberQuotaSmallerThanTheSource(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	small := s.b.dirs[path.Join(sharedBasePath, "pvc-tenant-b")]
	want := s.a.quotas[s.members["pvc-tenant-b"]]
	s.b.quotas[small] = want / 2
	logged := captureWarnings(t)

	require.NoError(t, s.promoteGroupOnB(false))

	assert.Equal(t, want, s.b.quotas[small], "the promoted member was not raised to the source's limit")
	line := logLine(logged, "members carried a smaller quota than the source; raised to match")
	require.NotEmpty(t, line, "the raise is not reported:\n%s", logged)
	assert.Contains(t, line, "pvc-tenant-b")
	assert.NotContains(t, line, "pvc-tenant-a", "a member already at the source's limit is not reported")
	// One raised, none created: the two lists are separate.
	assert.Empty(t, logLine(logged, "members had no quota on the promoted copy"))
}

// The peer-down row (verifyMemberQuotas: rel.peer == nil). A forced promote
// with A unreachable has nothing to compare against: a missing quota stays
// missing and is NAMED, and a smaller one cannot even be detected -- it is
// left as staged. Inventing a limit here would be a limit nobody asked for.
func TestForcedGroupPromoteWithSourceDownNamesMembersWithoutAQuota(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	missing := s.b.dirs[path.Join(sharedBasePath, "pvc-tenant-b")]
	delete(s.b.quotas, missing)
	smaller := s.b.dirs[path.Join(sharedBasePath, "pvc-tenant-c")]
	s.b.quotas[smaller] = 1 << 20
	s.a.down = true
	logged := captureWarnings(t)

	requireCode(t, s.promoteGroupOnB(false), codes.FailedPrecondition)
	require.NoError(t, s.promoteGroupOnB(true))

	rel := s.fabric.onlyRel()
	assert.False(t, rel.targetRO, "the forced promote made the group writable")
	_, has := s.b.quotas[missing]
	assert.False(t, has, "no limit is invented for a member with the source down")
	assert.EqualValues(t, 1<<20, s.b.quotas[smaller], "a smaller limit cannot be checked with the source down, and is left")

	line := logLine(logged, "the peer was not consulted, so member quotas on the promoted copy could not be checked")
	require.NotEmpty(t, line, "the unchecked quotas are not reported:\n%s", logged)
	assert.Contains(t, line, "pvc-tenant-b", "the member with no quota is named")
	assert.Contains(t, line, "membersWithQuota=2")
}

// The missing-member row (verifyMemberQuotas: LookupFileByPath fails). A
// member the source lists that is not on the promoted copy -- provisioned
// after the last job, or removed there by hand -- is named and skipped;
// the others are still checked and the promote succeeds.
func TestPlannedGroupPromoteNamesAMemberMissingOnThePromotedCopy(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	s.b.rmdir(path.Join(sharedBasePath, "pvc-tenant-c"))
	short := s.b.dirs[path.Join(sharedBasePath, "pvc-tenant-b")]
	s.b.quotas[short] = 1 << 20
	logged := captureWarnings(t)

	require.NoError(t, s.promoteGroupOnB(false))

	line := logLine(logged, "a member of the source group is not present on the promoted copy")
	require.NotEmpty(t, line, "the missing member is not reported:\n%s", logged)
	assert.Contains(t, line, "member=pvc-tenant-c")
	assert.NotContains(t, s.b.dirs, path.Join(sharedBasePath, "pvc-tenant-c"), "nothing is made up for it")
	assert.Equal(t, s.a.quotas[s.members["pvc-tenant-b"]], s.b.quotas[short],
		"the members after the missing one are still checked")
}

// An empty group: a shared class protected before any PVC was provisioned.
// Enable has nothing to wait for and protects the root; a promote has no
// member quotas to check and still flips the covering export; a member
// provisioned afterwards is staged by Info.
func TestAnEmptyGroupProtectsPromotesAndLaterStagesItsFirstMember(t *testing.T) {
	s := newReplScenario(t)
	s.a.mkdir(sharedBasePath)
	s.a.exports = append(s.a.exports, qumulo.ExportInfo{
		ID: "9", ExportPath: sharedExportPath, FSPath: sharedBasePath, TenantID: 1,
		AuthenticationMode: qumulo.AuthenticationModeNone, AllowedClients: []string{"10.0.0.0/8"},
	})
	groupHandle := newVolumeID(arrayA, "", sharedBasePath).String()

	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId: groupHandle, Parameters: groupClass(), Secrets: replSecrets(), ReplicationSource: groupSourceFor(sharedBasePath),
	})
	require.NoError(t, err, "an empty group has no member to wait for")
	rel := s.fabric.onlyRel()
	assert.Equal(t, sharedBasePath, rel.sourcePath)
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	require.Len(t, s.b.exports, 1)
	assert.True(t, s.b.exports[0].ReadOnly)

	// The first member arrives after protection and replicates; Info stages it.
	id := s.a.mkdir(path.Join(sharedBasePath, "pvc-first"))
	s.a.quotas[id] = 3 << 30
	replicaID := s.b.mkdir(path.Join(sharedBasePath, "pvc-first"))
	_, err = s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: newVolumeID(arrayA, "", path.Join(sharedBasePath, "pvc-first")).String(),
		Secrets:  replSecrets(), ReplicationSource: groupSourceFor(sharedBasePath),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 3<<30, s.b.quotas[replicaID], "the late first member got its quota staged")
}

// The empty group's promote: nothing to check, and nothing to report as
// unchecked.
func TestPromotingAnEmptyGroupFlipsTheExportAndReportsNoMembers(t *testing.T) {
	s := newReplScenario(t)
	s.a.mkdir(sharedBasePath)
	s.a.exports = append(s.a.exports, qumulo.ExportInfo{
		ID: "9", ExportPath: sharedExportPath, FSPath: sharedBasePath, TenantID: 1,
		AuthenticationMode: qumulo.AuthenticationModeNone, AllowedClients: []string{"10.0.0.0/8"},
	})
	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId:   newVolumeID(arrayA, "", sharedBasePath).String(),
		Parameters: groupClass(), Secrets: replSecrets(), ReplicationSource: groupSourceFor(sharedBasePath),
	})
	require.NoError(t, err)
	logged := captureWarnings(t)

	_, err = s.server.PromoteVolume(context.Background(), &replication.PromoteVolumeRequest{
		VolumeId:   newVolumeID(arrayB, "", sharedBasePath).String(),
		Parameters: classToA(), Secrets: replSecrets(), ReplicationSource: groupSourceFor(sharedBasePath),
	})
	require.NoError(t, err)
	assert.False(t, s.fabric.onlyRel().targetRO)
	require.Len(t, s.b.exports, 1)
	assert.False(t, s.b.exports[0].ReadOnly)
	assert.Empty(t, logLine(logged, "quota"), "an empty group has no member quota to report on:\n%s", logged)
}

// A group root removed out of band after protection -- DeleteVolume refuses
// a directory holding a relationship root, so this is the array's API
// or UI. Pinned as it stands, not as it should be: every member handle stops
// resolving, so Enable, Demote and Info answer NotFound, and Disable answers
// OK ("volume does not resolve; nothing to disable") with the relationship
// still on both arrays. Disable resolves the member before it can find the
// group, and a NotFound there is what lets a VolumeReplication whose volume
// is gone be deleted at all; the relationship left behind is the operator's
// to remove on the arrays (docs/DR-ARCHITECTURE.md §7).
func TestAGroupRootRemovedAfterProtectionLeavesTheRelationshipForTheOperator(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	s.a.rmdir(sharedBasePath)
	member := s.memberHandle("pvc-tenant-a")

	requireCode(t, s.enableGroup(t, groupClass(), ""), codes.NotFound)
	_, err := s.server.DemoteVolume(context.Background(), &replication.DemoteVolumeRequest{
		VolumeId: member, Parameters: groupClass(), Secrets: replSecrets(),
	})
	requireCode(t, err, codes.NotFound)
	_, err = s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: member, Secrets: replSecrets(), ReplicationSource: groupSourceFor(sharedBasePath),
	})
	requireCode(t, err, codes.NotFound)

	_, err = s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
		VolumeId: member, Parameters: groupClass(), Secrets: replSecrets(), ReplicationSource: groupSourceFor(sharedBasePath),
	})
	require.NoError(t, err, "a VolumeReplication whose volume is gone can still be deleted")
	rel := s.fabric.onlyRel()
	assert.True(t, rel.hasSource && rel.hasTarget, "the relationship is left on both arrays for the operator")
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
}

// The last group gap: a second VolumeReplication on the same group class --
// another tenant's PVC as its dataSource. The driver does NOT refuse it, and
// cannot tell it apart: it keeps no state between requests (Rule 0), the
// request names no VolumeReplication, and the first object's own retries
// arrive exactly like this one. Refusing would need a record of which member
// enabled the group, kept on the array, which nothing provides. So the
// second Enable is an idempotent no-op on the one relationship, and every
// later verb from EITHER object acts on the whole class: deleting the second
// object disables replication for every member while the first still
// exists. One VolumeReplication per group class is the operator's rule
// (docs/DR-ARCHITECTURE.md §4.3), pinned here so a change to it is seen.
func TestASecondVolumeReplicationOnAGroupClassSharesTheOneRelationship(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	first := s.fabric.onlyRel().id
	s.a.calls, s.b.calls = nil, nil
	second := s.memberHandle("pvc-tenant-b")

	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId: second, Parameters: groupClass(), Secrets: replSecrets(),
	})
	require.NoError(t, err, "not refused: indistinguishable from the first object's retry")
	assert.Equal(t, 1, s.fabric.relCount(), "no second relationship is stacked on the group root")
	assert.Equal(t, first, s.fabric.onlyRel().id)
	assert.Empty(t, mutations(s.a.calls))
	assert.Empty(t, mutations(s.b.calls))

	_, err = s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
		VolumeId: second, Parameters: groupClass(), Secrets: replSecrets(),
	})
	require.NoError(t, err)
	assert.Zero(t, s.fabric.relCount(), "the second object's Disable removes the whole class's relationship")
}
