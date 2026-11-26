//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Defect tests for directory ownership: Qumulo refuses to replicate an
// entry owned by a local user or group with no NFS ID, and every directory
// the driver created was owned by the API token's local user. Each asserts
// the correct behaviour, so it fails while the defect is present.

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// A new volume's directory is given an NFS owner and group, so a
// relationship over it has nothing to refuse.
func TestDefect_NewVolumeDirectoryGetsAnNFSOwner(t *testing.T) {
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
		Name:               "pvc-sn1",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         testParams(),
	})

	require.NoError(t, err)
	require.Len(t, patches, 1, "the owner rides in the mode PATCH")
	require.NotNil(t, patches[0].OwnerDetails, "the volume directory keeps the token's local owner")
	require.NotNil(t, patches[0].GroupDetails, "the volume directory keeps the token's local group")
	assert.Equal(t, qumulo.IDTypeNFSUID, patches[0].OwnerDetails.IDType)
	assert.Equal(t, qumulo.IDTypeNFSGID, patches[0].GroupDetails.IDType)
}

// A volume an older build created is owned by the token's local user;
// enabling replication on it re-owns the root before the relationship.
func TestDefect_EnableReownsALocalOwnedVolumeRoot(t *testing.T) {
	s := newReplScenario(t)
	s.a.own(replVolumePath,
		qumulo.Identity{IDType: qumulo.IDTypeLocalUser, IDValue: "1001"},
		qumulo.Identity{IDType: qumulo.IDTypeLocalGroup, IDValue: "513"})

	require.NoError(t, s.enable(t, classToB()))

	assert.Equal(t, qumulo.IDTypeNFSUID, s.a.owners[s.fileIDA].IDType, "the root is still owned by a local user")
	assert.Equal(t, qumulo.IDTypeNFSGID, s.a.groups[s.fileIDA].IDType, "the root is still owned by a local group")
}

// The job error for a local owner says what to do about it.
func TestDefect_LocalOwnerJobErrorNamesTheRemedy(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.lastJobError = "/csi/volumes/pvc-repl cannot be replicated because it belongs to local user csi-driver."

	h := s.assess(t, s.handleA, classToB())

	assert.Contains(t, h.Message, "NFS identity", "the report repeats Core's error with no remedy")
}
