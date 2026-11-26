// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// Group replication over the six per-volume RPCs (granularity by
// provisioning model: the shared model and the two-tenant blast radius).
//
// The shared-subdirectory shape these tests build is the measured one: one
// covering export at the class basePath, N member directories under it, one
// quota each, and no export of any member's own.

const (
	sharedBasePath   = "/csi/shared"
	sharedExportPath = "/shared"
)

// sharedScenario is a shared-subdirectory class on A with three tenants'
// volumes under one covering export, and the peer B empty.
type sharedScenario struct {
	*replScenario
	members map[string]string // name -> file ID on A
}

func newSharedScenario(t *testing.T) *sharedScenario {
	t.Helper()

	s := newReplScenario(t)
	// The dedicated-export volume newReplScenario provisions is in the way of
	// nothing here, but its export would be picked as a covering export for
	// paths under it, so the shared class lives elsewhere and keeps its own
	// single export.
	s.a.mkdir(sharedBasePath)
	s.a.exports = append(s.a.exports, qumulo.ExportInfo{
		ID: "9", ExportPath: sharedExportPath, FSPath: sharedBasePath, TenantID: 1,
		AuthenticationMode: qumulo.AuthenticationModeNone, AllowedClients: []string{"10.0.0.0/8"},
	})

	members := map[string]string{}
	for i, name := range []string{"pvc-tenant-a", "pvc-tenant-b", "pvc-tenant-c"} {
		id := s.a.mkdir(path.Join(sharedBasePath, name))
		s.a.quotas[id] = uint64(i+1) << 30
		members[name] = id
	}
	return &sharedScenario{replScenario: s, members: members}
}

// memberHandle is one tenant's PV handle: a real handle for a real directory,
// which is what makes the per-PVC refusal a refusal rather than a NotFound.
func (s *sharedScenario) memberHandle(name string) string {
	return newVolumeID(arrayA, "", path.Join(sharedBasePath, name)).String()
}

// groupClass is the VolumeReplicationClass for the shared class: the same
// class as ever plus replicationScope.
func groupClass(extra ...string) map[string]string {
	return classToB(append([]string{"replicationScope", replicationScopeGroup}, extra...)...)
}

func TestEnableRefusesAPerPVCVolumeReplicationOnASharedClass(t *testing.T) {
	s := newSharedScenario(t)

	// The dedicated volume beside the class protects normally; the shared
	// one is refused.
	require.NoError(t, s.enable(t, classToB()))
	require.Equal(t, replVolumePath, s.fabric.onlyRel().sourcePath)

	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId:   s.memberHandle("pvc-tenant-a"),
		Parameters: classToB(),
		Secrets:    replSecrets(),
	})

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "shared-subdirectory")
	assert.Contains(t, err.Error(), "every other volume in the class")
	assert.Contains(t, err.Error(), paramReplicationScope)
	// Nothing was created for the shared class.
	for _, rel := range relsBySourcePath(s.fabric) {
		assert.NotEqual(t, path.Join(sharedBasePath, "pvc-tenant-a"), rel)
	}
}

func relsBySourcePath(f *fakeFabric) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.rels))
	for _, r := range f.rels {
		out = append(out, r.sourcePath)
	}
	return out
}

// enableGroup runs the group form for one member of the shared class.
func (s *sharedScenario) enableGroup(t *testing.T, params map[string]string, groupID string) error {
	t.Helper()
	req := &replication.EnableVolumeReplicationRequest{
		VolumeId:   s.memberHandle("pvc-tenant-a"),
		Parameters: params,
		Secrets:    replSecrets(),
	}
	if groupID != "" {
		req.ReplicationSource = &replication.ReplicationSource{
			Type: &replication.ReplicationSource_Volumegroup{
				Volumegroup: &replication.ReplicationSource_VolumeGroupSource{VolumeGroupId: groupID},
			},
		}
	}
	_, err := s.server.EnableVolumeReplication(context.Background(), req)
	return err
}

// TestEnableGroupProtectsTheWholeClass is the measured shared model: one
// relationship on basePath, one covering export mirrored read-only, one quota
// per member pre-staged.
func TestEnableGroupProtectsTheWholeClass(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		groupID func(s *sharedScenario) string
	}{
		{
			name:   "replicationScope on the class, group derived from the covering export",
			params: groupClass(),
		},
		{
			name:   "an absolute path as the volume group id",
			params: classToB(),
			groupID: func(*sharedScenario) string {
				return sharedBasePath
			},
		},
		{
			name:   "a driver handle as the volume group id",
			params: classToB(),
			groupID: func(s *sharedScenario) string {
				return newVolumeID(arrayA, "", sharedBasePath).String()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSharedScenario(t)
			groupID := ""
			if tt.groupID != nil {
				groupID = tt.groupID(s)
			}

			require.NoError(t, s.enableGroup(t, tt.params, groupID))

			// One relationship, rooted at the class basePath -- not at any
			// member's directory.
			require.Equal(t, 1, s.fabric.relCount())
			rel := s.fabric.onlyRel()
			assert.Equal(t, sharedBasePath, rel.sourcePath)
			assert.Equal(t, sharedBasePath, rel.targetPath, "same path on both arrays")
			assert.Equal(t, qumulo.RelationshipEstablished, rel.state)

			// One covering export on the peer, read-only until promote.
			var mirrored *qumulo.ExportInfo
			for i := range s.b.exports {
				if s.b.exports[i].ExportPath == sharedExportPath {
					mirrored = &s.b.exports[i]
				}
			}
			require.NotNil(t, mirrored, "the covering export was not mirrored")
			assert.Equal(t, sharedBasePath, mirrored.FSPath)
			assert.True(t, mirrored.ReadOnly, "a writable replica export drops appends silently")
			assert.Len(t, s.b.exports, 1, "exactly one export for the whole class")

			// A quota per member, with each member's own limit.
			for name, sourceID := range s.members {
				replicaID, ok := s.b.dirs[path.Join(sharedBasePath, name)]
				require.True(t, ok, "member %q did not replicate", name)
				assert.Equal(t, s.a.quotas[sourceID], s.b.quotas[replicaID], "member %q quota", name)
			}
		})
	}
}

// Enabling twice must land on the same relationship, not stack a second one
// on the same directory.
func TestEnableGroupIsIdempotent(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	first := s.fabric.onlyRel().id

	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	assert.Equal(t, 1, s.fabric.relCount())
	assert.Equal(t, first, s.fabric.onlyRel().id)
}

// A member provisioned after the last sync has no replica directory yet. That
// is a Warn and a later reconcile, not a failure: refusing would keep Enable
// failing for as long as any member is in flight.
// Measured on the lab cluster (2026-09-14): the csi-addons controller does not
// call Enable again once a primary VolumeReplication reports Completed, so a
// member that had not replicated at enable time never got its quota staged
// until a promote filled it in. Enable now answers Aborted until every member
// carries a quota on the peer, so Completed means protected.
func TestEnableGroupRetriesUntilEveryMemberHasReplicated(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))

	// A fourth tenant appears on the source only.
	late := s.a.mkdir(path.Join(sharedBasePath, "pvc-tenant-d"))
	s.a.quotas[late] = 4 << 30

	err := s.enableGroup(t, groupClass(), "")
	requireCode(t, err, codes.Aborted)
	// The counts are the point: this wait is a first sync completing, and
	// csi-addons shows it as FailedToPromote for however long it takes. What
	// tells an operator it is working is the numbers moving.
	assert.Contains(t, err.Error(), "3 of 4 volumes staged")
	assert.Contains(t, err.Error(), "1 still replicating")
	assert.Contains(t, err.Error(), "not a failure")
	_, staged := s.b.dirs[path.Join(sharedBasePath, "pvc-tenant-d")]
	assert.False(t, staged, "the fake does not replicate after authorize")

	// Once the member has replicated, the same Enable stages its quota and succeeds.
	s.b.mkdir(path.Join(sharedBasePath, "pvc-tenant-d"))
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	assert.EqualValues(t, 4<<30, s.b.quotas[s.b.dirs[path.Join(sharedBasePath, "pvc-tenant-d")]])
}

// A class with adDomain set stores each volume at base/domain/name, and
// ensureADDomainDirectory creates that domain directory deliberately without
// a quota -- a quota there would cap every volume inside it at once.
//
// groupMembers lists one directory level and skips any child carrying no
// quota, so the domain directory is skipped and every volume beneath it is
// never seen. Enable reports the group protected, and after promote those
// volumes have no quota on the replica at all: a write past the limit is
// unbounded rather than ENOSPC.
//
// verifyMemberQuotas walks the same single level, so promote does not fill
// the gap either.
func TestEnableGroupStagesQuotasForMembersUnderADomainDirectory(t *testing.T) {
	s := newSharedScenario(t)

	// The domain directory, as ensureADDomainDirectory creates it: present,
	// and deliberately carrying no quota.
	domainPath := path.Join(sharedBasePath, "corp.example.com")
	s.a.mkdir(domainPath)

	domainMembers := map[string]uint64{"pvc-dom-a": 5 << 30, "pvc-dom-b": 6 << 30}
	for name, limit := range domainMembers {
		id := s.a.mkdir(path.Join(domainPath, name))
		s.a.quotas[id] = limit
	}
	require.NoError(t, s.enableGroup(t, groupClass(), ""))

	for name, limit := range domainMembers {
		replicaID, ok := s.b.dirs[path.Join(domainPath, name)]
		require.True(t, ok, "member %q is missing on the peer", name)
		assert.EqualValues(t, limit, s.b.quotas[replicaID],
			"member %q under the domain directory has no replica quota, so writes past its limit are unbounded after promote", name)
	}
}

// relationshipsAt matches a relationship whose root path EQUALS the volume's
// path. A group relationship is rooted at the class basePath, so a member's
// expand never finds it and expandReplicaFirst leaves the peer alone.
//
// The replica keeps the limit staged at enable time, and promote does not
// raise a quota that already exists -- verifyMemberQuotas skips any name
// carrying any quota rather than comparing limits. The promoted volume then
// runs out of space at the size it had before the expand.
func TestExpandingOneGroupMemberRaisesTheReplicaQuotaToo(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))

	const name = "pvc-tenant-a"
	replicaID, ok := s.b.dirs[path.Join(sharedBasePath, name)]
	require.True(t, ok)
	staged := s.b.quotas[replicaID]
	require.NotZero(t, staged, "enable pre-staged the member's limit")

	grown := staged * 2
	require.NoError(t, s.expand(t, s.memberHandle(name), grown, deleteSecrets()))

	assert.EqualValues(t, grown, s.a.quotas[s.members[name]], "the source did not grow")
	assert.EqualValues(t, grown, s.b.quotas[replicaID],
		"the replica kept the enable-time limit, so the promoted volume runs out of space at the old size")
}

func TestEnableGroupRefusals(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		groupID func(s *sharedScenario) string
		prepare func(s *sharedScenario)
		want    codes.Code
		wantMsg string
	}{
		{
			name:   "a group root with no covering export has nothing to mount after failover",
			params: groupClass(),
			groupID: func(*sharedScenario) string {
				return "/csi" // a real ancestor of the member, with no export of its own
			},
			want:    codes.FailedPrecondition,
			wantMsg: "no NFS export at its root",
		},
		{
			name:    "a group id that is neither a path nor a handle",
			params:  classToB(),
			groupID: func(*sharedScenario) string { return "tenant-group-1" },
			want:    codes.InvalidArgument,
			wantMsg: "neither an absolute path",
		},
		{
			name:   "a group on the other array",
			params: classToB(),
			groupID: func(*sharedScenario) string {
				return newVolumeID(arrayB, "", sharedBasePath).String()
			},
			want:    codes.InvalidArgument,
			wantMsg: "one directory tree on one array",
		},
		{
			name:    "a group the volume does not live under",
			params:  classToB(),
			groupID: func(*sharedScenario) string { return replVolumePath },
			want:    codes.FailedPrecondition,
			wantMsg: "not under the volume group rooted at",
		},
		{
			name:    "a group root that does not exist",
			params:  classToB(),
			groupID: func(*sharedScenario) string { return "/csi/no-such-class" },
			want:    codes.FailedPrecondition,
			wantMsg: "not under the volume group rooted at",
		},
		{
			// Under the retired handle this case was "a group handle whose
			// file ID names another directory" -- a stale ID reaching the
			// wrong tree. A path-identified handle cannot be stale that way,
			// so what is left to refuse is a group root that is not there.
			name:   "a group handle naming a path that does not exist",
			params: classToB(),
			groupID: func(s *sharedScenario) string {
				return newVolumeID(arrayA, "", "/csi/no-such-shared-class").String()
			},
			want:    codes.NotFound,
			wantMsg: "does not name a volume group directory",
		},
		{
			name:   "scope group on a class whose volume no export covers",
			params: groupClass(),
			prepare: func(s *sharedScenario) {
				s.a.exports = nil
			},
			want:    codes.FailedPrecondition,
			wantMsg: "cannot be identified",
		},
		{
			// Reproduced on the fabric before the fix: with the
			// root export the only one covering the member, the group was
			// rooted at "/" and the relationship would have frozen the whole
			// array at promote.
			name:   "the only covering export is the root export",
			params: groupClass(),
			prepare: func(s *sharedScenario) {
				s.a.exports = []qumulo.ExportInfo{{ID: "1", ExportPath: "/", FSPath: "/", TenantID: 1}}
			},
			want:    codes.FailedPrecondition,
			wantMsg: "root export",
		},
		{
			name:    "an explicit group id of the array root",
			params:  classToB(),
			groupID: func(*sharedScenario) string { return "/" },
			want:    codes.FailedPrecondition,
			wantMsg: "rooted at \"/\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSharedScenario(t)
			if tt.prepare != nil {
				tt.prepare(s)
			}
			groupID := ""
			if tt.groupID != nil {
				groupID = tt.groupID(s)
			}

			err := s.enableGroup(t, tt.params, groupID)

			requireCode(t, err, tt.want)
			assert.Contains(t, err.Error(), tt.wantMsg)
			assert.Zero(t, s.fabric.relCount(), "nothing may be created for a refused group")
		})
	}
}

// A covering export above the class's basePath is refused when the group is
// derived from it: the driver's layout puts a volume at most two levels below
// its class root (basePath/<name>, or basePath/<adDomain>/<name>), so an
// export three levels up is some ancestor's, and a group rooted there would
// reach every class under it.
func TestEnableGroupRefusesACoveringExportAboveTheClass(t *testing.T) {
	s := newSharedScenario(t)
	// A volume nested one level deeper than any class layout puts it, under
	// an export at /csi: three levels above the volume.
	deep := s.a.mkdir("/csi/shared/sub/pvc-deep")
	s.a.quotas[deep] = replQuota
	s.a.exports = []qumulo.ExportInfo{{ID: "1", ExportPath: "/csi", FSPath: "/csi", TenantID: 1}}

	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId:   newVolumeID(arrayA, "", "/csi/shared/sub/pvc-deep").String(),
		Parameters: groupClass(),
		Secrets:    replSecrets(),
	})

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "3 levels above")
	assert.Zero(t, s.fabric.relCount())

	// Two levels -- the adDomain shape -- is the layout and is accepted.
	s.a.exports = []qumulo.ExportInfo{{ID: "2", ExportPath: "/shared", FSPath: "/csi/shared", TenantID: 1,
		AuthenticationMode: qumulo.AuthenticationModeNone, AllowedClients: []string{"10.0.0.0/8"}}}
	_, err = s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId:   newVolumeID(arrayA, "", "/csi/shared/sub/pvc-deep").String(),
		Parameters: groupClass(),
		Secrets:    replSecrets(),
	})
	require.NoError(t, err)
	assert.Equal(t, sharedBasePath, s.fabric.onlyRel().sourcePath)
}

// Promote on a group flips the one covering export to read-write, which is
// what makes every member mountable at once (one make-writable).
func TestPromoteGroupFlipsTheCoveringExport(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))

	// From B's side: the class on the DR cluster names A as its peer.
	replicaMember := newVolumeID(arrayB, "", path.Join(sharedBasePath, "pvc-tenant-a")).String()

	_, err := s.server.PromoteVolume(context.Background(), &replication.PromoteVolumeRequest{
		VolumeId:   replicaMember,
		Parameters: classToA(),
		Secrets:    replSecrets(),
		ReplicationSource: &replication.ReplicationSource{
			Type: &replication.ReplicationSource_Volumegroup{
				Volumegroup: &replication.ReplicationSource_VolumeGroupSource{VolumeGroupId: sharedBasePath},
			},
		},
	})
	require.NoError(t, err)

	rel := s.fabric.onlyRel()
	assert.Equal(t, qumulo.RelationshipDisconnected, rel.state)
	assert.False(t, rel.targetRO, "the group root is still read-only after promote")

	require.Len(t, s.b.exports, 1)
	assert.False(t, s.b.exports[0].ReadOnly, "the covering export was not flipped to read-write")

	// Every member kept the quota pre-staging gave it; without one a client
	// write would be unbounded rather than ENOSPC.
	for name, sourceID := range s.members {
		replicaID := s.b.dirs[path.Join(sharedBasePath, name)]
		assert.Equal(t, s.a.quotas[sourceID], s.b.quotas[replicaID], "member %q quota after promote", name)
	}
	// Exactly one make-writable for the whole class.
	writables := 0
	for _, call := range s.b.calls {
		if call == "MakeTargetWritable" {
			writables++
		}
	}
	assert.Equal(t, 1, writables)
}

// A member whose replica was never staged a quota gets one from the peer at
// promote, which is the group form of replumb's gap filling.
func TestPromoteGroupFillsAMissingMemberQuotaFromThePeer(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))

	gap := s.b.dirs[path.Join(sharedBasePath, "pvc-tenant-b")]
	delete(s.b.quotas, gap)

	replicaMember := newVolumeID(arrayB, "", path.Join(sharedBasePath, "pvc-tenant-a")).String()
	_, err := s.server.PromoteVolume(context.Background(), &replication.PromoteVolumeRequest{
		VolumeId:   replicaMember,
		Parameters: classToA(),
		Secrets:    replSecrets(),
		ReplicationSource: &replication.ReplicationSource{
			Type: &replication.ReplicationSource_Volumegroup{
				Volumegroup: &replication.ReplicationSource_VolumeGroupSource{VolumeGroupId: sharedBasePath},
			},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, s.a.quotas[s.members["pvc-tenant-b"]], s.b.quotas[gap],
		"the gap was not filled from the peer's limit")
}

// Disable on a group removes the one relationship and no data: every tenant's
// directory, on both arrays, survives.
func TestDisableGroupRemovesTheRelationshipAndNoData(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))

	_, err := s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
		VolumeId:   s.memberHandle("pvc-tenant-a"),
		Parameters: groupClass(),
		Secrets:    replSecrets(),
	})
	require.NoError(t, err)

	assert.Zero(t, s.fabric.relCount())
	for name := range s.members {
		assert.Contains(t, s.a.dirs, path.Join(sharedBasePath, name))
		assert.Contains(t, s.b.dirs, path.Join(sharedBasePath, name), "the replica must not be deleted")
	}
	assert.Len(t, s.b.exports, 1, "the mirrored export stays; disable deletes no configuration on the peer")
}

// DeleteVolume's guard and the group form meet here: a member's directory is
// not a relationship root, so deleting one member is an ordinary delete whose
// effect replicates. Deleting the class basePath is not.
func TestDeleteVolumeAndGroupRelationships(t *testing.T) {
	t.Run("a member of a protected group deletes normally", func(t *testing.T) {
		s := newSharedScenario(t)
		require.NoError(t, s.enableGroup(t, groupClass(), ""))

		err := s.deleteVolume(t, s.memberHandle("pvc-tenant-a"))
		require.NoError(t, err)
		assert.Equal(t, 1, s.fabric.relCount(), "the class's relationship is untouched")
		assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
	})

	t.Run("the group root itself is refused", func(t *testing.T) {
		s := newSharedScenario(t)
		require.NoError(t, s.enableGroup(t, groupClass(), ""))

		err := s.deleteVolume(t, newVolumeID(arrayA, "", sharedBasePath).String())
		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), "Disable replication first")
		assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
	})

	// A member's mirror PV on the DR array names a directory inside the
	// group's target root. That directory is part of the replica, so it gets
	// the refusal a pair's replica gets, whatever the policy says -- not a
	// tree-delete that only Core's read-only target happened to stop.
	t.Run("a member's DR copy is refused like a replica", func(t *testing.T) {
		s := newSharedScenario(t)
		require.NoError(t, s.enableGroup(t, groupClass(), ""))
		memberOnB := path.Join(sharedBasePath, "pvc-tenant-a")
		require.Contains(t, s.b.dirs, memberOnB)

		_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: newVolumeID(arrayB, "", memberOnB).String(),
			Secrets:  deleteSecrets(paramReplicationDeletePolicy, replicationDeletePolicyDisable),
		})

		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), "replica")
		assert.Contains(t, err.Error(), "Disable replication from the source side first")
		assert.Contains(t, s.b.dirs, memberOnB, "the DR copy must not be tree-deleted")
		assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
	})
}

// The Info a group's VolumeReplication sends names a member PVC, no
// class parameters and no group. The member has no relationship of its own;
// the group's covers it from the class's basePath. Info must assess that one
// -- not answer "no replication relationship" -- and state the member as
// protected, or QumuloCSIVolumeUnprotected fires for a protected volume.
func TestInfoOnAGroupMemberAssessesTheGroupsRelationship(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	group := s.fabric.onlyRel()
	member := s.memberHandle("pvc-tenant-b")

	health, err := s.server.AssessVolumeReplication(context.Background(), member, "", nil, replSecrets())

	require.NoError(t, err)
	assert.Equal(t, group.id, health.RelationshipID)
	require.NotEmpty(t, s.a.protectionVerdicts)
	assert.True(t, s.a.protectionVerdicts[len(s.a.protectionVerdicts)-1], "the member is protected by its group")
	assert.Equal(t, member, s.a.protectionSubjects[len(s.a.protectionSubjects)-1],
		"the verdict is the member's, keyed by the handle Info named")
}

// Through the gRPC Info itself, which is what the csi-addons controller calls.
func TestGetVolumeReplicationInfoOnAGroupMemberSucceeds(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))

	_, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: s.memberHandle("pvc-tenant-a"), Secrets: replSecrets(),
	})

	require.NoError(t, err, "a group member's VolumeReplication must not read \"status unknown\"")
}

// A volume under a class nobody protected is still unprotected: the member
// match only applies when a live relationship actually covers it.
func TestInfoOnAnUnprotectedSharedVolumeStillSaysSo(t *testing.T) {
	s := newSharedScenario(t)

	_, err := s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-c"), "", nil, replSecrets())

	requireCode(t, err, codes.FailedPrecondition)
	assert.Equal(t, []bool{false}, s.a.protectionVerdicts)
}

// csi-addons calls Enable once, and Enable stages the quotas of the
// members that exist then. A PVC created on the class afterwards reached the
// DR array as a directory with no quota -- none 608 s after it bound -- and
// an unplanned failover would have promoted it unbounded. The group's Info,
// which the controller keeps calling, now stages it.
func TestInfoStagesTheQuotaOfAMemberCreatedAfterProtection(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))

	const late = "pvc-tenant-late"
	memberPath := path.Join(sharedBasePath, late)
	s.a.quotas[s.a.mkdir(memberPath)] = 3 << 30
	replicaID := s.b.mkdir(memberPath) // replicated: contents only, no quota
	require.NotContains(t, s.b.quotas, replicaID)

	_, err := s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())

	require.NoError(t, err)
	assert.EqualValues(t, 3<<30, s.b.quotas[replicaID], "the late member's replica is bounded by its own limit")
}

// A member that has not replicated yet is not an Info failure: it is staged
// by a later Info, once it arrives.
func TestInfoWithAMemberStillReplicatingSucceeds(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	s.a.quotas[s.a.mkdir(path.Join(sharedBasePath, "pvc-tenant-late"))] = 3 << 30

	h, err := s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())

	require.NoError(t, err)
	assert.Equal(t, ReplicationHealthy, h.Status, "message: %s", h.Message)
}

// groupSource is the ReplicationSource naming the shared class's group.
func groupSource() *replication.ReplicationSource {
	return &replication.ReplicationSource{Type: &replication.ReplicationSource_Volumegroup{
		Volumegroup: &replication.ReplicationSource_VolumeGroupSource{VolumeGroupId: sharedBasePath},
	}}
}

// withCoveringExportOnB is the DR array as DR-ARCHITECTURE §4.4 has the
// operator prepare it: the base path and a covering export there, created by
// hand, so read-write unless the operator chose otherwise.
func (s *sharedScenario) withCoveringExportOnB(readOnly bool) {
	s.b.mkdir(sharedBasePath)
	s.b.exports = append(s.b.exports, qumulo.ExportInfo{
		ID: "94", ExportPath: sharedExportPath, FSPath: sharedBasePath, TenantID: 1,
		AuthenticationMode: qumulo.AuthenticationModeNone, AllowedClients: []string{"10.0.0.0/8"},
		ReadOnly: readOnly,
	})
}

// The guard reads a read-write replica export over
// data as a promoted copy once no relationship end is left. A group's covering
// export on the DR array is the operator's: with it read-only, a plain Disable
// and re-Enable of a group that was never failed over re-protects it. With it
// read-write, Enable does not change an export it did not make; it refuses,
// naming the change, and once the operator makes it read-only the same
// Disable and re-Enable works.
func TestGroupReEnableAfterAPlainDisableWithAnOperatorCreatedCoveringExport(t *testing.T) {
	for _, readOnly := range []bool{true, false} {
		t.Run(fmt.Sprintf("covering export read-only=%t", readOnly), func(t *testing.T) {
			s := newSharedScenario(t)
			s.withCoveringExportOnB(readOnly)
			class := groupClass("allowNonEmptyTarget", "true")

			err := s.enableGroup(t, class, "")
			if !readOnly {
				requireCode(t, err, codes.FailedPrecondition)
				assert.Contains(t, err.Error(), "not made by this driver")
				assert.Contains(t, err.Error(), "Set that export read-only")
				require.Len(t, s.b.exports, 1)
				assert.False(t, s.b.exports[0].ReadOnly, "an operator's export is never changed by the driver")
				assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state,
					"the group is protected meanwhile")

				s.b.exports[0].ReadOnly = true // the operator's one change
				err = s.enableGroup(t, class, "")
			}
			require.NoError(t, err)
			require.Len(t, s.b.exports, 1)
			assert.True(t, s.b.exports[0].ReadOnly, "the replica's covering export is read-only")

			_, err = s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
				VolumeId: s.memberHandle("pvc-tenant-a"), Parameters: class, Secrets: replSecrets(),
			})
			require.NoError(t, err)
			require.Zero(t, s.fabric.relCount(), "Disable removed the relationship on both arrays")

			require.NoError(t, s.enableGroup(t, class, ""), "re-protecting a group that was never failed over")
			assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
		})
	}
}

// The fence must not weaken for a group. After a failover, a failback
// resync and a Disable on the promoted side, no relationship end is left and
// the promoted copy's covering export is read-write over its data: that copy
// holds writes the old primary never saw, and re-protecting from the old
// primary is still refused.
func TestEnableGroupRefusesTheOldPrimaryAfterAPromoteFailbackAndDisable(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	replicaMember := newVolumeID(arrayB, "", path.Join(sharedBasePath, "pvc-tenant-a")).String()

	_, err := s.server.PromoteVolume(context.Background(), &replication.PromoteVolumeRequest{
		VolumeId: replicaMember, Parameters: classToA(), Secrets: replSecrets(), ReplicationSource: groupSource(),
	})
	require.NoError(t, err)
	_, err = s.server.ResyncVolume(context.Background(), &replication.ResyncVolumeRequest{
		VolumeId: s.memberHandle("pvc-tenant-a"), Parameters: groupClass(), Secrets: replSecrets(),
		Force: true, ReplicationSource: groupSource(),
	})
	require.NoError(t, err)
	require.Equal(t, s.b, s.fabric.onlyRel().source, "the failback resync made B the source")

	_, err = s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
		VolumeId: replicaMember, Parameters: classToA(), Secrets: replSecrets(), ReplicationSource: groupSource(),
	})
	require.NoError(t, err)
	require.Zero(t, s.fabric.relCount())
	require.Len(t, s.b.exports, 1)
	require.False(t, s.b.exports[0].ReadOnly, "the promoted copy's covering export is read-write")

	err = s.enableGroup(t, groupClass("allowNonEmptyTarget", "true"), "")

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "promoted")
	assert.Zero(t, s.fabric.relCount(), "nothing was created toward the promoted copy")
	assert.False(t, s.b.exports[0].ReadOnly, "the promoted copy's export was not fenced")
}

// A replica export this driver made that reads read-write -- made or
// adopted by an earlier build -- is fenced by the next source-side Info,
// rather than reported DEGRADED on every poll with nothing fixing it.
func TestInfoFencesTheDriversOwnReadWriteReplicaExport(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	require.Len(t, s.b.exports, 1)
	require.True(t, madeByThisDriver(&s.b.exports[0]))
	s.b.exports[0].ReadOnly = false

	// Through another member than the one whose name the export carries.
	h, err := s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-b"), "", nil, replSecrets())

	require.NoError(t, err)
	assert.True(t, s.b.exports[0].ReadOnly, "Info set the driver's own replica export read-only")
	assert.Equal(t, ReplicationHealthy, h.Status, "message: %s", h.Message)
}

// An operator's read-write covering export is reported, not changed.
func TestInfoReportsAnOperatorsReadWriteReplicaExportAsDrift(t *testing.T) {
	s := newSharedScenario(t)
	s.withCoveringExportOnB(true)
	require.NoError(t, s.enableGroup(t, groupClass("allowNonEmptyTarget", "true"), ""))
	require.Len(t, s.b.exports, 1)
	s.b.exports[0].ReadOnly = false

	h, err := s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())

	require.NoError(t, err)
	assert.Equal(t, ReplicationDegraded, h.Status, "message: %s", h.Message)
	assert.Contains(t, h.Message, "not this driver's")
	assert.False(t, s.b.exports[0].ReadOnly, "an operator's export is never changed")
}

// Info stages a late group member's quota on the replica side.
// On the stale old primary after a failover the peer's end is the promoted
// live copy, and the staging wrote this side's limits onto it -- what
// forbids. Info now stages nothing onto a promoted peer.
func TestInfoOnTheStaleOldPrimaryLeavesThePromotedCopysQuotaAlone(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	replicaMember := newVolumeID(arrayB, "", path.Join(sharedBasePath, "pvc-tenant-a")).String()
	_, err := s.server.PromoteVolume(context.Background(), &replication.PromoteVolumeRequest{
		VolumeId: replicaMember, Parameters: classToA(), Secrets: replSecrets(), ReplicationSource: groupSource(),
	})
	require.NoError(t, err)

	// A member whose limits differ between the two sides: 7 GiB on the stale
	// old primary, 1 GiB on the live copy.
	late := path.Join(sharedBasePath, "pvc-tenant-late")
	s.a.quotas[s.a.mkdir(late)] = 7 << 30
	liveID := s.b.mkdir(late)
	s.b.quotas[liveID] = 1 << 30
	before := len(s.b.calls)

	_, err = s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())

	require.NoError(t, err)
	assert.NotContains(t, s.b.calls[before:], "UpdateQuota")
	assert.NotContains(t, s.b.calls[before:], "CreateQuota")
	assert.Equal(t, uint64(1<<30), s.b.quotas[liveID], "the promoted copy's quota is left alone")
}

// An unchanged group is not re-staged on every poll; a member added
// since -- here one that replicated after the first poll -- is staged on the
// next.
func TestInfoReStagesAGroupOnlyWhenItsMembershipChanges(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	info := func() {
		t.Helper()
		_, err := s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())
		require.NoError(t, err)
	}
	info()

	// Unchanged membership: a replica quota removed since is not put back
	// on this poll -- nothing was re-read.
	memberOnB := path.Join(sharedBasePath, "pvc-tenant-b")
	delete(s.b.quotas, s.b.dirs[memberOnB])
	info()
	assert.NotContains(t, s.b.quotas, s.b.dirs[memberOnB], "an unchanged group is not re-staged")

	// A new member, still replicating on the first poll after it appears,
	// then arrived: staged as soon as it is there.
	late := path.Join(sharedBasePath, "pvc-tenant-late")
	s.a.quotas[s.a.mkdir(late)] = 3 << 30
	info()
	replicaID := s.b.mkdir(late)
	info()
	assert.EqualValues(t, 3<<30, s.b.quotas[replicaID], "the late member is staged once it has replicated")
}

// The fingerprint carried every directory's child count, a member's
// own included, so any file created in any member re-staged the whole group
// on the next poll. A busy member is not a membership change.
func TestABusyMemberDoesNotReStageItsGroup(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	info := func() {
		t.Helper()
		_, err := s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())
		require.NoError(t, err)
	}
	info()

	s.a.files = map[string]int{path.Join(sharedBasePath, "pvc-tenant-a"): 1234}
	memberOnB := path.Join(sharedBasePath, "pvc-tenant-b")
	delete(s.b.quotas, s.b.dirs[memberOnB])
	info()
	assert.NotContains(t, s.b.quotas, s.b.dirs[memberOnB], "files written in a member are not a membership change")
}

// What the child count is kept for: a member added under an adDomain
// directory changes nothing at the group root but that directory's count,
// and must still be staged.
func TestAMemberAddedUnderADomainDirectoryReStagesItsGroup(t *testing.T) {
	s := newSharedScenario(t)
	domainPath := path.Join(sharedBasePath, "corp.example.com")
	s.a.mkdir(domainPath)
	first := path.Join(domainPath, "pvc-dom-a")
	s.a.quotas[s.a.mkdir(first)] = 5 << 30
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	info := func() {
		t.Helper()
		_, err := s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())
		require.NoError(t, err)
	}
	info()

	// Files inside the domain's member do not count: the domain directory's
	// own child count is unchanged.
	s.a.files = map[string]int{first: 99}
	delete(s.b.quotas, s.b.dirs[first])
	info()
	assert.NotContains(t, s.b.quotas, s.b.dirs[first], "a busy member under a domain is not a membership change")

	late := path.Join(domainPath, "pvc-dom-late")
	s.a.quotas[s.a.mkdir(late)] = 7 << 30
	replicaID := s.b.mkdir(late)
	info()
	assert.EqualValues(t, 7<<30, s.b.quotas[replicaID], "a member added under the domain directory is staged")
	assert.EqualValues(t, 5<<30, s.b.quotas[s.b.dirs[first]], "and the re-stage puts back what had drifted")
}

// The fingerprint itself: members' counts are left out, every other
// directory's kept.
func TestMembershipFingerprintIgnoresOnlyMembersCounts(t *testing.T) {
	n := func(i int) *int { return &i }
	dir := func(name, id string, count int) qumulo.DirEntry {
		return qumulo.DirEntry{Name: name, ID: id, Type: "FS_FILE_TYPE_DIRECTORY", ChildCount: n(count)}
	}
	members := topLevelMembers([]memberQuota{{relPath: "pvc-a"}, {relPath: "corp/pvc-b"}})
	assert.Equal(t, "pvc-a", members, "only members directly under the root")

	base := membershipFingerprint([]qumulo.DirEntry{dir("pvc-a", "1", 3), dir("corp", "2", 1)}, members)
	assert.Equal(t, base, membershipFingerprint([]qumulo.DirEntry{dir("pvc-a", "1", 300), dir("corp", "2", 1)}, members),
		"a member's count is not part of it")
	assert.NotEqual(t, base, membershipFingerprint([]qumulo.DirEntry{dir("pvc-a", "1", 3), dir("corp", "2", 2)}, members),
		"a domain directory's is")
	assert.NotEqual(t, base, membershipFingerprint([]qumulo.DirEntry{dir("pvc-a", "9", 3), dir("corp", "2", 1)}, members),
		"a member replaced under the same name is a change")
	assert.NotEqual(t, base, membershipFingerprint([]qumulo.DirEntry{dir("pvc-a", "1", 3), dir("corp", "2", 1), dir("pvc-c", "3", 0)}, members),
		"a member added is a change")
}

// A group member's Info found its group by listing every relationship
// on the array, twice, and the peer's: on every poll. A repeated report reads
// what it found last.
func TestRepeatedInfoOnAGroupMemberListsNothing(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	group := s.fabric.onlyRel()
	member := s.memberHandle("pvc-tenant-b")
	_, err := s.server.AssessVolumeReplication(context.Background(), member, "", nil, replSecrets())
	require.NoError(t, err)
	a0, b0 := s.a.statusListings, s.b.statusListings

	health, err := s.server.AssessVolumeReplication(context.Background(), member, "", nil, replSecrets())

	require.NoError(t, err)
	assert.Equal(t, group.id, health.RelationshipID)
	assert.Equal(t, a0, s.a.statusListings, "no listing on the local array")
	assert.Equal(t, b0, s.b.statusListings, "no listing on the peer")
}
