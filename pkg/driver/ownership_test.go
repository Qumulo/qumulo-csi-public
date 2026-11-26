// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// Replication refuses an entry owned by a local user or group with no
// NFS ID, and every directory the driver created was owned by the token's
// local user. These pin where the driver now gives its directories an NFS
// owner, and where it leaves an owner alone.

var (
	localTokenUser  = qumulo.Identity{IDType: qumulo.IDTypeLocalUser, IDValue: "1001"}
	localUsersGroup = qumulo.Identity{IDType: qumulo.IDTypeLocalGroup, IDValue: "513"}
	nfsUser1000     = qumulo.Identity{IDType: qumulo.IDTypeNFSUID, IDValue: "1000"}
	nfsGroup1000    = qumulo.Identity{IDType: qumulo.IDTypeNFSGID, IDValue: "1000"}
	nfsRoot         = qumulo.Identity{IDType: qumulo.IDTypeNFSUID, IDValue: "0"}
	nfsRootGroup    = qumulo.Identity{IDType: qumulo.IDTypeNFSGID, IDValue: "0"}
)

func TestLocalReownershipReplacesOnlyLocalIdentities(t *testing.T) {
	tests := []struct {
		name         string
		owner, group qumulo.Identity
		wantOwner    *qumulo.Identity
		wantGroup    *qumulo.Identity
	}{
		{name: "both local", owner: localTokenUser, group: localUsersGroup, wantOwner: &nfsRoot, wantGroup: &nfsRootGroup},
		{name: "local owner, NFS group", owner: localTokenUser, group: nfsGroup1000, wantOwner: &nfsRoot},
		{name: "NFS owner, local group", owner: nfsUser1000, group: localUsersGroup, wantGroup: &nfsRootGroup},
		{name: "both NFS", owner: nfsUser1000, group: nfsGroup1000},
		{
			name:  "SMB and AD identities are someone's choice",
			owner: qumulo.Identity{IDType: "SMB_SID", IDValue: "S-1-5-21-1-2-3-1104"},
			group: qumulo.Identity{IDType: "SMB_SID", IDValue: "S-1-5-21-1-2-3-513"},
		},
		{name: "not reported is not known to be local"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := localReownership(tt.owner, tt.group)
			assert.Equal(t, tt.wantOwner, got.OwnerDetails)
			assert.Equal(t, tt.wantGroup, got.GroupDetails)
			assert.Empty(t, got.Mode)
		})
	}
}

// A fresh volume's directory gets the NFS owner in the same PATCH as its
// mode: CreateVolume makes no more calls than it did.
func TestCreateVolumeOwnsTheDirectoryInTheModePatch(t *testing.T) {
	var patches []qumulo.FileAttributes
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		SetFileAttributesByIDFunc: func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
			patches = append(patches, attrs)
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-owned",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         testParams(),
	})

	require.NoError(t, err)
	require.Len(t, patches, 1, "one attribute PATCH, as before")
	assert.Equal(t, defaultMountPermissions, patches[0].Mode)
	assert.Equal(t, &nfsRoot, patches[0].OwnerDetails)
	assert.Equal(t, &nfsRootGroup, patches[0].GroupDetails)
	assert.Empty(t, patches[0].Owner, "the auth-ID form is a restore's, not this")
}

// A directory an earlier attempt left is adopted: its mode is re-applied, and
// only a local owner or group is replaced. An NFS owner on it -- a pod's
// chown -- is kept.
func TestAdoptedVolumeIsReownedOnlyWhereLocal(t *testing.T) {
	var patches []qumulo.FileAttributes
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, &qumulo.APIError{StatusCode: http.StatusConflict, Class: qumulo.ErrorClassEntryExists}
		},
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{
				ID: "12884901888", Path: p, Type: "FS_FILE_TYPE_DIRECTORY",
				Owner: nfsUser1000, Group: localUsersGroup,
			}, nil
		},
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) { return 1 << 30, nil },
		SetFileAttributesByIDFunc: func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
			patches = append(patches, attrs)
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-adopted",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         testParams(),
	})

	require.NoError(t, err)
	require.Len(t, patches, 1)
	assert.Equal(t, defaultMountPermissions, patches[0].Mode)
	assert.Nil(t, patches[0].OwnerDetails, "an NFS owner is kept")
	assert.Equal(t, &nfsRootGroup, patches[0].GroupDetails, "a local group is replaced")
}

// A class that leaves the mode alone still gets the NFS owner, in a PATCH of
// its own: the one case CreateVolume makes a call it did not make before.
// And a refusal fails the provision, as a refused mode does, rolling the
// directory back.
func TestInheritModeStillOwnsTheDirectoryAndARefusalRollsBack(t *testing.T) {
	var deleted bool
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		SetFileAttributesByIDFunc: func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
			assert.Empty(t, attrs.Mode)
			assert.Equal(t, &nfsRoot, attrs.OwnerDetails)
			return &qumulo.APIError{StatusCode: http.StatusForbidden, Class: "permission_denied_error"}
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error { deleted = true; return nil },
	}
	srv := newTestControllerServer(t, client)
	params := testParams()
	params["mountPermissions"] = mountPermissionsInherit

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-inherit",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         params,
	})

	requireCode(t, err, codes.Internal)
	assert.Contains(t, err.Error(), "NFS owner")
	assert.True(t, deleted, "the directory is rolled back")
}

// A restore's own root is a fresh volume's: NFS-owned outright. The entries
// under it keep the snapshot's owners, which
// TestRestoreKeepsModeOwnerGroupAndMtimeBelowTheRoot pins.
func TestRestoreRootGetsTheNFSOwner(t *testing.T) {
	var rootAttrs []qumulo.FileAttributes
	client := emptyRestoreClient()
	client.SetFileAttributesByIDFunc = func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
		if fileID == "stage-1" {
			rootAttrs = append(rootAttrs, attrs)
		}
		return nil
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), testGiB))

	require.NoError(t, err)
	require.Len(t, rootAttrs, 1)
	assert.Equal(t, &nfsRoot, rootAttrs[0].OwnerDetails)
	assert.Equal(t, &nfsRootGroup, rootAttrs[0].GroupDetails)
	assert.Equal(t, defaultMountPermissions, rootAttrs[0].Mode)
}

// The adDomain directory sits inside a group relationship rooted at the
// class's basePath, so it is NFS-owned too: when it is created, and when an
// older build's local-owned one is found. Its mode is never touched, and a
// refusal is not worth failing the provision over.
func TestADDomainDirectoryOwnership(t *testing.T) {
	tests := []struct {
		name        string
		existing    *qumulo.FileInfo // nil: missing, so created
		refuse      bool
		wantPatched *qumulo.FileAttributes
	}{
		{
			name:        "a new domain directory is NFS-owned",
			wantPatched: &qumulo.FileAttributes{OwnerDetails: &nfsRoot, GroupDetails: &nfsRootGroup},
		},
		{
			name:        "an older build's local-owned one is re-owned",
			existing:    &qumulo.FileInfo{ID: "1", Owner: localTokenUser, Group: localUsersGroup},
			wantPatched: &qumulo.FileAttributes{OwnerDetails: &nfsRoot, GroupDetails: &nfsRootGroup},
		},
		{
			name:     "an NFS-owned one is left alone",
			existing: &qumulo.FileInfo{ID: "1", Owner: nfsUser1000, Group: nfsGroup1000},
		},
		{
			name:        "a refusal is logged and the provision goes on",
			refuse:      true,
			wantPatched: &qumulo.FileAttributes{OwnerDetails: &nfsRoot, GroupDetails: &nfsRootGroup},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var patched []qumulo.FileAttributes
			var patchedIDs []string
			client := &MockClient{
				LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
					if tt.existing == nil {
						return qumulo.FileInfo{}, apiError(http.StatusNotFound)
					}
					return *tt.existing, nil
				},
				CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{ID: "2"}, nil
				},
				SetFileAttributesByIDFunc: func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
					patchedIDs, patched = append(patchedIDs, fileID), append(patched, attrs)
					if tt.refuse {
						return errors.New("refused")
					}
					return nil
				},
			}
			srv := newTestControllerServer(t, client)

			err := srv.ensureADDomainDirectory(context.Background(), client, volumeParams{
				StoreRealPath: "/csi-volumes", ADDomain: "corp.example.com", MountPermissions: "0777",
			})

			require.NoError(t, err)
			if tt.wantPatched == nil {
				assert.Empty(t, patched)
				return
			}
			require.Len(t, patched, 1)
			assert.Equal(t, *tt.wantPatched, patched[0], "the owner and group only, never a mode")
			wantID := "2"
			if tt.existing != nil {
				wantID = tt.existing.ID
			}
			assert.Equal(t, wantID, patchedIDs[0])
		})
	}
}

// An array that does not answer the domain directory's PATCH fails the
// provision as Unavailable, as any unanswered step does.
func TestADDomainDirectoryOwnershipWithNoAnswerIsUnavailable(t *testing.T) {
	client := &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusNotFound)
		},
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "2"}, nil
		},
		SetFileAttributesByIDFunc: func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
			return transportError("/v1/files/2/info/attributes")
		},
	}
	srv := newTestControllerServer(t, client)
	params := testParams()
	params["adDomain"] = "corp.example.com"

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-domain",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         params,
	})

	requireCode(t, err, codes.Unavailable)
}

// Enable re-owns an older build's local-owned volume root before it creates
// the relationship, so the first job has nothing to refuse.
func TestEnableReownsALocalOwnedSourceRootBeforeCreatingTheRelationship(t *testing.T) {
	s := newReplScenario(t)
	s.a.own(replVolumePath, localTokenUser, localUsersGroup)

	s.protect(t, classToB())

	assert.Equal(t, nfsRoot, s.a.owners[s.fileIDA])
	assert.Equal(t, nfsRootGroup, s.a.groups[s.fileIDA])
	reown := callIndex(s.a.calls, "SetAttributes "+replVolumePath)
	create := callIndex(s.a.calls, "CreateSource")
	require.GreaterOrEqual(t, reown, 0, "calls: %v", s.a.calls)
	assert.Less(t, reown, create, "re-owned before the relationship exists: %v", s.a.calls)
}

// An owner someone chose -- an NFS uid a pod set, or an operator's -- is not
// the driver's to replace.
func TestEnableLeavesAnNFSOwnedSourceRootAlone(t *testing.T) {
	s := newReplScenario(t)
	s.a.own(replVolumePath, nfsUser1000, nfsGroup1000)

	s.protect(t, classToB())

	assert.Equal(t, nfsUser1000, s.a.owners[s.fileIDA])
	assert.Equal(t, nfsGroup1000, s.a.groups[s.fileIDA])
	assert.Equal(t, -1, callIndex(s.a.calls, "SetAttributes "+replVolumePath), "calls: %v", s.a.calls)
}

// A relationship an older build created over a local-owned root fails every
// job. A later Enable finds it and re-owns the root, at the cost of nothing
// but the PATCH: the owner was read resolving the volume.
func TestEnableOnAnExistingRelationshipReownsALocalOwnedRoot(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.a.own(replVolumePath, localTokenUser, nfsGroup1000)

	require.NoError(t, s.enable(t, classToB()))

	assert.Equal(t, nfsRoot, s.a.owners[s.fileIDA])
	assert.Equal(t, nfsGroup1000, s.a.groups[s.fileIDA], "the NFS group is kept")
}

// Group scope: the group root, the members and the adDomain directory above
// them are the driver's -- or, for the root, re-owned on the same local-only
// terms -- and are re-owned before the relationship is created. A member's
// NFS owner, and a directory that is no member's, are left alone.
func TestEnableGroupReownsLocalOwnedMembersAndDomainDirectories(t *testing.T) {
	s := newSharedScenario(t)
	s.a.own(sharedBasePath, localTokenUser, localUsersGroup)
	s.a.own(path.Join(sharedBasePath, "pvc-tenant-a"), localTokenUser, localUsersGroup)
	s.a.own(path.Join(sharedBasePath, "pvc-tenant-b"), nfsUser1000, nfsGroup1000)
	domain := path.Join(sharedBasePath, "corp.example.com")
	s.a.own(domain, localTokenUser, localUsersGroup)
	domainMember := path.Join(domain, "pvc-dom-a")
	s.a.quotas[s.a.mkdir(domainMember)] = 1 << 30
	s.a.own(domainMember, localTokenUser, localUsersGroup)
	stray := path.Join(sharedBasePath, "not-a-volume")
	s.a.own(stray, localTokenUser, localUsersGroup)

	err := s.enableGroup(t, groupClass(), "")
	// The first sync leaves members pending on the fabric's peer; Aborted is
	// the normal answer then, and the re-own has happened by then.
	if err != nil {
		requireCode(t, err, codes.Aborted)
	}

	for _, p := range []string{sharedBasePath, path.Join(sharedBasePath, "pvc-tenant-a"), domain, domainMember} {
		id := s.a.dirs[p]
		assert.Equal(t, nfsRoot, s.a.owners[id], "%s owner", p)
		assert.Equal(t, nfsRootGroup, s.a.groups[id], "%s group", p)
		assert.Less(t, callIndex(s.a.calls, "SetAttributes "+p), callIndex(s.a.calls, "CreateSource"),
			"%s re-owned before the relationship: %v", p, s.a.calls)
	}
	tenantB := s.a.dirs[path.Join(sharedBasePath, "pvc-tenant-b")]
	assert.Equal(t, nfsUser1000, s.a.owners[tenantB], "an NFS-owned member is left alone")
	assert.Equal(t, localTokenUser, s.a.owners[s.a.dirs[stray]], "a directory that is no member's is not the driver's")
}

// The job error Core reports for a local owner gets the remedy appended; any
// other job error is reported as it was.
func TestAssessmentNamesTheRemedyForALocalOwner(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.lastJobError = "/csi/volumes/pvc-repl/data cannot be replicated because it belongs to local user csi-driver. " +
		"Either remove all local users and groups from the file or ensure that all local users and groups have " +
		"associated NFS IDs and edit the relationship to enable mapping local IDs to NFS IDs."

	h := s.assess(t, s.handleA, classToB())

	assert.Contains(t, h.Message, "belongs to local user csi-driver")
	assert.Contains(t, h.Message, "re-owned to an NFS identity")
	assert.Contains(t, h.Message, "map_local_ids_to_nfs_ids")

	rel.lastJobError = "connection to target lost"
	h = s.assess(t, s.handleA, classToB())
	assert.NotContains(t, h.Message, "NFS identity")
}

func TestWithLocalOwnershipRemedy(t *testing.T) {
	assert.Equal(t, "connection lost", withLocalOwnershipRemedy("connection lost"))
	for _, msg := range []string{"x belongs to local user u", "x belongs to local group g"} {
		got := withLocalOwnershipRemedy(msg)
		assert.True(t, strings.HasPrefix(got, msg))
		assert.Contains(t, got, "NFS identity")
	}
}

// callIndex is the position of the first call equal to verb, or -1.
func callIndex(calls []string, verb string) int {
	for i, c := range calls {
		if c == verb {
			return i
		}
	}
	return -1
}
