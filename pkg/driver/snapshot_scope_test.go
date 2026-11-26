// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// The names a snapshot 42 can carry: one csi-snapshotter gave it through
// this driver, and one an administrator chose.
const (
	driverSnapshotName  = "42_snapshot-0b7e3c2a-5d1f-4c8e-9a6b-2f4d8e1c7a90"
	foreignSnapshotName = "42_alice-nightly"
)

// snapshotOf is a MockClient holding snapshot 42, stored under name, of the
// directory at sourcePath ("" when that directory is gone); isVolume says
// whether the directory carries a quota, as this driver's volumes do.
func snapshotOf(name, sourcePath string, isVolume bool, deleted *[]string) *MockClient {
	return &MockClient{
		GetSnapshotFunc: func(context.Context, string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: "42", Name: name, SourceFileID: "900"}, nil
		},
		ResolveFileIDsFunc: func(context.Context, []string) (map[string]string, error) {
			return map[string]string{"900": sourcePath}, nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) {
			if isVolume {
				return uint64(testGiB), nil
			}
			return 0, apiError(http.StatusNotFound)
		},
		DeleteSnapshotFunc: func(_ context.Context, id string) error {
			*deleted = append(*deleted, id)
			return nil
		},
	}
}

// A snapshot is this driver's to delete when it is named as the
// driver's snapshots are and, while its source is there, that source is one
// of the driver's volumes. A quota'd directory passes the source check, so
// an administrator's snapshot of a quota'd /home/alice is refused by its
// name; with the source gone the name decides alone. Anything foreign is
// deleted only with the deletion Secret's consent.
func TestDeleteSnapshotIsScopedToThisDriversVolumes(t *testing.T) {
	handle := newSnapshotID(testServer, "", "42").String()
	cases := []struct {
		name        string
		snapshot    string
		source      string
		isVolume    bool
		consent     bool
		wantDeleted bool
		wantWhy     string
	}{
		{"a snapshot of the filesystem root", driverSnapshotName, "/", false, false, false, "filesystem root"},
		{"a snapshot of a directory that is not a volume", driverSnapshotName, "/home/alice", false, false, false,
			"not one of this driver's volumes"},
		{"an administrator's snapshot of a quota'd directory", foreignSnapshotName, "/home/alice", true, false, false,
			`is named "42_alice-nightly"`},
		{"the same, with the deletion Secret's consent", foreignSnapshotName, "/home/alice", true, true, true, ""},
		{"a snapshot of one of this driver's volumes", driverSnapshotName, "/csi/pvc-a", true, false, true, ""},
		{"a snapshot from an earlier build", "42_snapshot-1", "/csi/pvc-a", true, false, true, ""},
		{"a snapshot whose volume is gone", driverSnapshotName, "", false, false, true, ""},
		{"a foreign-named snapshot whose source is gone", foreignSnapshotName, "", false, false, false,
			"its source no longer exists"},
		{"the same, with the deletion Secret's consent, source gone", foreignSnapshotName, "", false, true, true, ""},
		{"a name with the prefix and nothing after it", "42_snapshot-", "", false, false, false, "is named"},
		{"the prefix not right after the ID", "42_x-snapshot-1", "", false, false, false, "is named"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var deleted []string
			srv := newTestControllerServer(t, snapshotOf(tc.snapshot, tc.source, tc.isVolume, &deleted))
			secrets := testSecrets()
			if tc.consent {
				secrets[secretKeyAllowForeignSnapshotDelete] = "true"
			}
			_, err := srv.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: handle, Secrets: secrets})
			if tc.wantDeleted {
				require.NoError(t, err)
				assert.Equal(t, []string{"42"}, deleted)
				return
			}
			assertCode(t, err, codes.FailedPrecondition)
			assert.Contains(t, err.Error(), secretKeyAllowForeignSnapshotDelete)
			assert.Contains(t, err.Error(), tc.wantWhy)
			assert.Empty(t, deleted)
		})
	}
}

// The name a snapshot must carry follows -snapshot-name-prefix, so a
// chart that renames the snapshotter's snapshots renames the driver's too.
func TestDeleteSnapshotFollowsTheConfiguredNamePrefix(t *testing.T) {
	handle := newSnapshotID(testServer, "", "42").String()
	for name, wantDeleted := range map[string]bool{"42_backup-abc": true, driverSnapshotName: false} {
		t.Run(name, func(t *testing.T) {
			var deleted []string
			srv := newTestControllerServer(t, snapshotOf(name, "", false, &deleted))
			srv.driver.SnapshotNamePrefix = "backup"
			_, err := srv.DeleteSnapshot(context.Background(),
				&csi.DeleteSnapshotRequest{SnapshotId: handle, Secrets: testSecrets()})
			if wantDeleted {
				require.NoError(t, err)
				assert.Equal(t, []string{"42"}, deleted)
				return
			}
			assertCode(t, err, codes.FailedPrecondition)
			assert.Contains(t, err.Error(), `"42_backup-<name>"`)
			assert.Empty(t, deleted)
		})
	}
}

// A restore copies only from this driver's volumes, or from under the
// class's own store root -- not from a snapshot of "/".
func TestRestoreRefusesASnapshotOfSomethingElse(t *testing.T) {
	client := emptyRestoreClient()
	client.ResolveFileIDsFunc = func(context.Context, []string) (map[string]string, error) {
		return map[string]string{"900": "/"}, nil
	}
	client.GetQuotaFunc = func(context.Context, string) (uint64, error) { return 0, apiError(http.StatusNotFound) }

	_, err := newTestControllerServer(t, client).CreateVolume(context.Background(),
		restoreRequest("pvc-restored", testParams(), testGiB))
	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "filesystem root")
}

// A snapshot whose source is gone, or does not resolve, is restored
// when it is named as this driver's snapshots are -- snapshot, delete the
// volume, restore is what restore is for -- and otherwise only from a class
// that sets allowForeignSnapshotRestore.
func TestRestoreOfASnapshotWhoseSourceIsGoneIsJudgedByItsName(t *testing.T) {
	cases := []struct {
		name       string
		snapshot   string
		resolveErr bool
		allow      string
		wantErr    bool
	}{
		{"named by this driver, source gone", driverSnapshotName, false, "", false},
		{"named by this driver, source unresolvable", driverSnapshotName, true, "", false},
		{"named by someone else, source gone", foreignSnapshotName, false, "", true},
		{"named by someone else, source unresolvable", foreignSnapshotName, true, "", true},
		{"named by someone else, the class says false", foreignSnapshotName, false, "false", true},
		{"named by someone else, the class allows it", foreignSnapshotName, false, "true", false},
		{"unresolvable, the class allows it", foreignSnapshotName, true, "TRUE", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := emptyRestoreClient()
			client.GetSnapshotFunc = func(context.Context, string) (qumulo.SnapshotInfo, error) {
				return qumulo.SnapshotInfo{ID: "42", Name: tc.snapshot, SourceFileID: "900"}, nil
			}
			client.ResolveFileIDsFunc = func(context.Context, []string) (map[string]string, error) {
				if tc.resolveErr {
					return nil, apiError(http.StatusInternalServerError)
				}
				return map[string]string{}, nil // gone
			}
			params := testParams()
			if tc.allow != "" {
				params["allowForeignSnapshotRestore"] = tc.allow
			}
			_, err := newTestControllerServer(t, client).CreateVolume(context.Background(),
				restoreRequest("pvc-restored", params, testGiB))
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			assertCode(t, err, codes.FailedPrecondition)
			assert.Contains(t, err.Error(), "allowForeignSnapshotRestore")
			assert.Contains(t, err.Error(), `is named "42_alice-nightly"`)
		})
	}
}

// allowForeignSnapshotRestore is "true" or "false"; anything else is refused
// rather than read as false.
func TestAllowForeignSnapshotRestoreMustBeABoolean(t *testing.T) {
	for value, want := range map[string]bool{"": false, "false": false, "True": true, " true ": true} {
		params := testParams()
		params["allowForeignSnapshotRestore"] = value
		parsed, err := parseVolumeParams(params)
		require.NoError(t, err, value)
		assert.Equal(t, want, parsed.AllowForeignSnapshotRestore, value)
	}
	for _, value := range []string{"yes", "1", "on"} {
		params := testParams()
		params["allowForeignSnapshotRestore"] = value
		_, err := parseVolumeParams(params)
		require.Error(t, err, value)
		assert.Contains(t, err.Error(), "allowforeignsnapshotrestore")
	}
}

// DeleteVolume removes the csi-<name> policy only when it is on the
// volume's own directory, or, with the directory gone, when the policy's
// source is gone too. A source of "/" is live: trimmed of its slash it read
// as gone, and a policy on the filesystem root was deleted as an orphan.
func TestDeleteVolumeRemovesOnlyItsOwnPolicy(t *testing.T) {
	cases := []struct {
		name         string
		policySource string
		resolvesTo   string // "" when the policy's source no longer resolves
		volumeExists bool
		wantDeleted  bool
	}{
		{"a policy on the volume's directory", testFileID, "/somewhere/else", true, true},
		{"a policy of that name on another directory", "777", "/somewhere/else", true, false},
		{"the volume gone, the policy's source live", "777", "/somewhere/else", false, false},
		{"the volume gone, the policy on the filesystem root", "2", "/", false, false},
		{"the volume gone, the policy an orphan", "777", "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var deleted []string
			client := &MockClient{
				ListPoliciesFunc: func(context.Context) ([]qumulo.PolicyInfo, error) {
					return []qumulo.PolicyInfo{{ID: "p1", Name: policyNameFor("pvc-x"), SourceFileID: tc.policySource}}, nil
				},
				DeletePolicyFunc: func(_ context.Context, id string) error { deleted = append(deleted, id); return nil },
				ResolveFileIDsFunc: func(context.Context, []string) (map[string]string, error) {
					if tc.resolvesTo != "" {
						return map[string]string{tc.policySource: tc.resolvesTo}, nil
					}
					return map[string]string{}, nil
				},
			}
			if !tc.volumeExists {
				client.LookupFileByPathFunc = missingVolume()
			}
			_, err := newTestControllerServer(t, client).DeleteVolume(context.Background(),
				&csi.DeleteVolumeRequest{VolumeId: testServer + "##/csi/pvc-x", Secrets: testSecrets()})
			require.NoError(t, err)
			if tc.wantDeleted {
				assert.Equal(t, []string{"p1"}, deleted)
			} else {
				assert.Empty(t, deleted)
			}
		})
	}
}

// A file the array reports with one link is not remembered for hard
// link rebuilding; one with more, or with no count, is.
func TestOnlyFilesThatMayBeHardLinkedAreRemembered(t *testing.T) {
	one, two := 1, 2
	assert.False(t, mayBeHardLinked(qumulo.DirEntry{NumLinks: &one}))
	assert.True(t, mayBeHardLinked(qumulo.DirEntry{NumLinks: &two}))
	assert.True(t, mayBeHardLinked(qumulo.DirEntry{}), "no count reported: remembered, as before")
}

// A fifth restore while four run on this controller is Aborted, to be
// started on the CO's retry.
func TestRestoresAreBoundedPerController(t *testing.T) {
	srv := newTestControllerServer(t, emptyRestoreClient())
	for i := 0; i < maxConcurrentRestores; i++ {
		srv.restoreSlots <- struct{}{}
	}
	_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), testGiB))
	assertCode(t, err, codes.Aborted)
	assert.Contains(t, err.Error(), "restores are already running")

	<-srv.restoreSlots
	_, err = srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), testGiB))
	require.NoError(t, err, "with a slot free the restore runs")
}
