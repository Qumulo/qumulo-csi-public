// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// scheduleParams returns StorageClass parameters carrying a snapshot schedule.
func scheduleParams(schedule string) map[string]string {
	p := testParams()
	p["snapshotSchedule"] = schedule
	return p
}

// requireScheduleEnabled skips a test that provisions with a schedule.
//
// The policy lifecycle is fully implemented; only the create request's wire
// shape is unresolved against a real cluster, so the parameter is gated. These
// tests are kept and skipped rather than deleted: they are what will prove the
// lifecycle the moment the gate opens.
func requireScheduleEnabled(t *testing.T) {
	t.Helper()

	if !snapshotScheduleEnabled {
		t.Skip("snapshotSchedule is gated; see snapshotScheduleEnabled")
	}
}

func TestCreateVolumeAttachesSnapshotPolicy(t *testing.T) {
	requireScheduleEnabled(t)

	var (
		gotName, gotFileID string
		gotSchedule        qumulo.Schedule
		calls              int
	)
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		CreatePolicyFunc: func(ctx context.Context, name, sourceFileID string,
			schedule qumulo.Schedule,
		) (qumulo.PolicyInfo, error) {
			calls++
			gotName, gotFileID, gotSchedule = name, sourceFileID, schedule
			return qumulo.PolicyInfo{ID: "77", Name: name}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         scheduleParams("daily@02:00,keep=7days"),
	})

	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	// The name is the ownership marker: it is what makes deletion safe.
	assert.Equal(t, "csi-pvc-abc", gotName)
	assert.Equal(t, testFileID, gotFileID, "the policy must point at the volume's own directory")
	assert.Equal(t, qumulo.ScheduleDailyOrWeekly, gotSchedule.CreationSchedule.Frequency)
	assert.Equal(t, "7days", gotSchedule.ExpirationTimeToLive)
}

// A class with no schedule must not touch the policy API at all.
func TestCreateVolumeWithoutScheduleAttachesNoPolicy(t *testing.T) {
	var called bool
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		CreatePolicyFunc: func(ctx context.Context, name, sourceFileID string,
			schedule qumulo.Schedule,
		) (qumulo.PolicyInfo, error) {
			called = true
			return qumulo.PolicyInfo{}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	require.NoError(t, err)
	assert.False(t, called, "no schedule means no policy")
}

// A retried CreateVolume must adopt the existing policy, not stack a second
// one firing on the same directory.
func TestCreateVolumeAdoptsExistingPolicy(t *testing.T) {
	requireScheduleEnabled(t)

	var created int
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		ListPoliciesFunc: func(ctx context.Context) ([]qumulo.PolicyInfo, error) {
			return []qumulo.PolicyInfo{{ID: "77", Name: "csi-pvc-abc", SourceFileID: testFileID}}, nil
		},
		CreatePolicyFunc: func(ctx context.Context, name, sourceFileID string,
			schedule qumulo.Schedule,
		) (qumulo.PolicyInfo, error) {
			created++
			return qumulo.PolicyInfo{}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         scheduleParams("daily@02:00"),
	})

	require.NoError(t, err)
	assert.Zero(t, created, "an existing policy for this volume is adopted, not duplicated")
}

// An administrator's policy on the same directory must not be adopted: only
// the name marks a policy as ours.
func TestCreateVolumeIgnoresForeignPolicyOnSameDirectory(t *testing.T) {
	requireScheduleEnabled(t)

	var created int
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		ListPoliciesFunc: func(ctx context.Context) ([]qumulo.PolicyInfo, error) {
			return []qumulo.PolicyInfo{
				{ID: "9", Name: "nightly-backups", SourceFileID: testFileID},
				{ID: "10", Name: "csi-some-other-volume", SourceFileID: "999"},
			}, nil
		},
		CreatePolicyFunc: func(ctx context.Context, name, sourceFileID string,
			schedule qumulo.Schedule,
		) (qumulo.PolicyInfo, error) {
			created++
			return qumulo.PolicyInfo{ID: "77", Name: name}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         scheduleParams("daily@02:00"),
	})

	require.NoError(t, err)
	assert.Equal(t, 1, created, "neither an admin's policy nor another volume's is ours to adopt")
}

// A policy that cannot be attached must roll the whole volume back, so a
// retry starts clean rather than finding a directory it cannot re-adopt.
func TestCreateVolumeRollsBackWhenPolicyFails(t *testing.T) {
	requireScheduleEnabled(t)

	var deletedTree bool
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		CreatePolicyFunc: func(ctx context.Context, name, sourceFileID string,
			schedule qumulo.Schedule,
		) (qumulo.PolicyInfo, error) {
			return qumulo.PolicyInfo{}, errors.New("policy table full")
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error {
			deletedTree = true
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         scheduleParams("daily@02:00"),
	})

	assertCode(t, err, codes.Internal)
	assert.True(t, deletedTree, "a failed policy must not leave the volume behind")
}

func TestDeleteVolumeRemovesItsPolicy(t *testing.T) {
	var deleted string
	client := &MockClient{
		ListPoliciesFunc: func(ctx context.Context) ([]qumulo.PolicyInfo, error) {
			return []qumulo.PolicyInfo{{ID: "77", Name: "csi-pvc-abc", SourceFileID: testFileID}}, nil
		},
		DeletePolicyFunc: func(ctx context.Context, policyID string) error {
			deleted = policyID
			return nil
		},
		TreeDeleteFinishedFunc: func(ctx context.Context, fileID string) (bool, error) { return true, nil },
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
	assert.Equal(t, "77", deleted)
}

// DeleteVolume must not fail because a policy could not be swept: refusing
// would strand the PV over an object the operator cannot see from Kubernetes.
func TestDeleteVolumeSurvivesPolicyFailure(t *testing.T) {
	client := &MockClient{
		ListPoliciesFunc: func(ctx context.Context) ([]qumulo.PolicyInfo, error) {
			return nil, apiError(http.StatusInternalServerError)
		},
		TreeDeleteFinishedFunc: func(ctx context.Context, fileID string) (bool, error) { return true, nil },
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		Secrets:  testSecrets(),
		VolumeId: testHandle("pvc-abc"),
	})

	require.NoError(t, err)
}

func TestPolicyNameFor(t *testing.T) {
	assert.Equal(t, "csi-pvc-abc", policyNameFor("pvc-abc"))
}
