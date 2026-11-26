// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// fakeTree scripts a snapshot source and records what a restore did to the
// target, so the assertions are about behaviour rather than call counts.
type fakeTree struct {
	// source is keyed by file ID, not by path: a restore walks the snapshot
	// by ID so that it still works after the source volume has been deleted,
	// which is the whole point of restoring from a snapshot.
	source  map[string][]qumulo.DirEntry // file ID -> entries, as of the snapshot
	target  map[string][]qumulo.DirEntry // path -> entries already present
	copied  []string
	created []string
	dirs    []string
	links   []string // "<new> -> <existing>"
	attrs   []string // "<path> <mode> <owner> <group> <mtime>", in call order
}

func (f *fakeTree) client() *MockClient {
	return &MockClient{
		SetFileAttributesFunc: func(_ context.Context, p string, a qumulo.FileAttributes) error {
			f.attrs = append(f.attrs, strings.Join([]string{p, a.Mode, a.Owner, a.Group, a.ModificationTime}, " "))
			return nil
		},
		CreateLinkFunc: func(_ context.Context, parent, name, existing string) (qumulo.FileInfo, error) {
			f.links = append(f.links, parent+"/"+name+" -> "+existing)
			return qumulo.FileInfo{Path: parent + "/" + name}, nil
		},
		ListDirectoryFunc: func(_ context.Context, ref, snapshotID string) ([]qumulo.DirEntry, error) {
			// A snapshot listing is addressed by file ID; the target listing
			// below is a live path. The real client tells them apart the same
			// way, by whether the reference contains separators.
			if snapshotID != "" {
				return f.source[ref], nil
			}
			if entries, ok := f.target[ref]; ok {
				return entries, nil
			}
			// Not found is how an unvisited target directory reads.
			return nil, apiError(404)
		},
		CreateDirectoryFunc: func(_ context.Context, parent, name string) (qumulo.FileInfo, error) {
			f.dirs = append(f.dirs, parent+"/"+name)
			return qumulo.FileInfo{Path: parent + "/" + name}, nil
		},
		CreateFileFunc: func(_ context.Context, parent, name string) (qumulo.FileInfo, error) {
			f.created = append(f.created, parent+"/"+name)
			return qumulo.FileInfo{Path: parent + "/" + name}, nil
		},
		CopyFileFunc: func(_ context.Context, target, sourceID, snap string, _ uint64) error {
			f.copied = append(f.copied, target)
			return nil
		},
	}
}

func file(name string, size uint64) qumulo.DirEntry {
	return qumulo.DirEntry{Name: name, ID: "id-" + name, Type: "FS_FILE_TYPE_FILE", Size: size}
}

func dir(name string) qumulo.DirEntry {
	return qumulo.DirEntry{Name: name, ID: "id-" + name, Type: "FS_FILE_TYPE_DIRECTORY"}
}

func TestRestoreCopiesTheWholeTree(t *testing.T) {
	tree := &fakeTree{source: map[string][]qumulo.DirEntry{
		"id-vol": {file("a.txt", 10), dir("sub")},
		"id-sub": {file("b.txt", 20)},
	}}
	srv := newTestControllerServer(t, tree.client())

	files, bytes, err := restoreAttempt(context.Background(), srv, tree.client(), "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, 2, files)
	assert.Equal(t, uint64(30), bytes)
}

// The property that makes retrying safe: work already done is not redone.
func TestRestoreSkipsFilesAlreadyAtTheRightSize(t *testing.T) {
	tree := &fakeTree{
		source: map[string][]qumulo.DirEntry{"id-vol": {file("a.txt", 10), file("b.txt", 20)}},
		target: map[string][]qumulo.DirEntry{"/stage": {file("a.txt", 10)}},
	}
	c := tree.client()
	srv := newTestControllerServer(t, c)

	files, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, 1, files, "only the missing file should be copied")
	assert.Equal(t, []string{"/stage/b.txt"}, tree.copied)
	assert.NotContains(t, tree.created, "/stage/a.txt", "a finished file must not be recreated")
}

// A file left truncated by an interrupted copy must be copied again, not
// treated as done -- otherwise a workload silently gets a short file.
func TestRestoreRecopiesATruncatedFile(t *testing.T) {
	tree := &fakeTree{
		source: map[string][]qumulo.DirEntry{"id-vol": {file("a.txt", 100)}},
		target: map[string][]qumulo.DirEntry{"/stage": {file("a.txt", 40)}},
	}
	c := tree.client()
	srv := newTestControllerServer(t, c)

	files, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, 1, files)
	assert.Equal(t, []string{"/stage/a.txt"}, tree.copied, "a short file must be recopied")
}

// An empty file is created but never copied: copy-chunk on a zero-length source
// is a wasted round trip, and a tree can hold many of them.
func TestRestoreCreatesEmptyFilesWithoutCopying(t *testing.T) {
	tree := &fakeTree{source: map[string][]qumulo.DirEntry{"id-vol": {file("empty", 0)}}}
	c := tree.client()
	srv := newTestControllerServer(t, c)

	files, bytes, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, 1, files)
	assert.Zero(t, bytes)
	assert.Empty(t, tree.copied, "an empty file needs no copy call")
	assert.Contains(t, tree.created, "/stage/empty")
}

// Anything that is not a regular file is skipped loudly rather than reported as
// restored -- recreating a symlink needs a different call.
func TestRestoreSkipsNonRegularFiles(t *testing.T) {
	tree := &fakeTree{source: map[string][]qumulo.DirEntry{
		"id-vol": {{Name: "link", Type: "FS_FILE_TYPE_SYMLINK", Size: 5}, file("real", 1)},
	}}
	c := tree.client()
	srv := newTestControllerServer(t, c)

	files, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, 1, files, "only the regular file counts as restored")
	assert.Equal(t, []string{"/stage/real"}, tree.copied)
}

// The budget expiring is a normal outcome, not a failure: the caller turns it
// into Aborted and the next attempt resumes.
func TestRestoreStopsWhenTheBudgetExpires(t *testing.T) {
	tree := &fakeTree{source: map[string][]qumulo.DirEntry{
		"id-vol": {file("a", 1), file("b", 1), file("c", 1)},
	}}
	c := tree.client()
	srv := newTestControllerServer(t, c)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := restoreAttempt(ctx, srv, c, "42", "id-vol", "/stage")

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, tree.copied, "no work should start after the budget is gone")
}

// The staging name is what makes a partial restore invisible and findable.
func TestRestoreStagingPath(t *testing.T) {
	params := volumeParams{StoreRealPath: "/csi/volumes"}

	staging := restoreTempPathFor(params, "pvc-abc")

	assert.Equal(t, "/csi/volumes/.csi-restoring-pvc-abc", staging)
	assert.True(t, isRestoreStaging(".csi-restoring-pvc-abc"))
	assert.False(t, isRestoreStaging("pvc-abc"), "a real volume must not look like staging")
}

// restoreRequest is a CreateVolume request that restores from a snapshot.
func restoreRequest(name string, params map[string]string, capacity int64) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               name,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: capacity},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         params,
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: testServer + "##42"},
			},
		},
	}
}

// A restore of an empty snapshot: enough to reach the steps that run after the
// copy completes, without scripting a tree.
func emptyRestoreClient() *MockClient {
	return &MockClient{
		// Nothing at the volume's final path yet, so this is a fresh restore
		// rather than an adoption.
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusNotFound)
		},
		GetSnapshotFunc: func(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: "42", Name: "42_snapshot-restore-src", SourceFileID: "900"}, nil
		},
		ResolveFileIDsFunc: func(ctx context.Context, ids []string) (map[string]string, error) {
			return map[string]string{"900": testStoreRealPath + "/pvc-source"}, nil
		},
		CreateDirectoryFunc: func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "stage-1", Type: "FS_FILE_TYPE_DIRECTORY", Path: parent + "/" + name}, nil
		},
		ListDirectoryFunc: func(ctx context.Context, p, snapshotID string) ([]qumulo.DirEntry, error) {
			return nil, nil // an empty snapshot
		},
		RenameFunc: func(ctx context.Context, parent, name, oldPath string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "stage-1", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	}
}

// A restored volume must get the snapshot policy its class asked for.
//
// Not a retry bug: ensurePolicy had exactly one caller in the tree, the
// fresh-create path, so a completely successful restore silently dropped
// snapshotSchedule. An operator setting a schedule on the class and restoring
// a PVC from a snapshot got a volume that was never scheduled for backup.
func TestRestoreAttachesTheSnapshotPolicy(t *testing.T) {
	requireScheduleEnabled(t)

	var policyName, policySourceID string
	client := emptyRestoreClient()
	client.CreatePolicyFunc = func(ctx context.Context, name, sourceFileID string,
		schedule qumulo.Schedule,
	) (qumulo.PolicyInfo, error) {
		policyName, policySourceID = name, sourceFileID
		return qumulo.PolicyInfo{ID: "77", Name: name}, nil
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(),
		restoreRequest("pvc-restored", scheduleParams("daily@02:00,keep=7days"), testGiB))

	require.NoError(t, err)
	require.NotNil(t, resp.GetVolume().GetContentSource(), "a restored volume keeps its content source")
	assert.Equal(t, "csi-pvc-restored", policyName, "a restored volume must get its class's snapshot policy")
	assert.Equal(t, "stage-1", policySourceID, "the policy must point at the restored directory")
}

// The restore path must apply the export too, which it already did -- this
// pins it, because the export moved behind the shared helper.
func TestRestoreAppliesTheExport(t *testing.T) {
	var exportedFSPath string
	client := emptyRestoreClient()
	client.ListExportsFunc = func(ctx context.Context) ([]qumulo.ExportInfo, error) { return nil, nil }
	client.CreateExportFunc = func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
		exportedFSPath = spec.FSPath
		return qumulo.ExportInfo{ID: "5"}, nil
	}
	params := testParams()
	params["provisioningModel"] = "dedicated-export"
	params["allowAnyClient"] = "true"
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", params, testGiB))

	require.NoError(t, err)
	assert.Equal(t, testStoreRealPath+"/pvc-restored", exportedFSPath)
}

// adoptedRestoreClient scripts a restore whose previous attempt already
// committed: the volume is at its final path with its quota.
func adoptedRestoreClient(quota uint64) *MockClient {
	return &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "committed-1", Type: "FS_FILE_TYPE_DIRECTORY", Path: p}, nil
		},
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) { return quota, nil },
	}
}

// The restore adoption branch used to return immediately, applying nothing at
// all: no mode, no export, no policy, and without reading the quota. A
// restored volume adopted on retry could be one the workload could not write
// to, or one with no export and therefore none of its class's IP restrictions.
func TestRestoreAdoptionReappliesEverything(t *testing.T) {
	requireScheduleEnabled(t)

	var modeSet, exportedFSPath, policyName string
	client := adoptedRestoreClient(uint64(testGiB))
	client.SetFileAttributesByIDFunc = func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
		modeSet = attrs.Mode
		return nil
	}
	client.ListExportsFunc = func(ctx context.Context) ([]qumulo.ExportInfo, error) { return nil, nil }
	client.CreateExportFunc = func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
		exportedFSPath = spec.FSPath
		return qumulo.ExportInfo{ID: "5"}, nil
	}
	client.CreatePolicyFunc = func(ctx context.Context, name, sourceFileID string,
		schedule qumulo.Schedule,
	) (qumulo.PolicyInfo, error) {
		policyName = name
		return qumulo.PolicyInfo{ID: "77", Name: name}, nil
	}
	params := scheduleParams("daily@02:00,keep=7days")
	params["provisioningModel"] = "dedicated-export"
	params["allowAnyClient"] = "true"
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(),
		restoreRequest("pvc-restored", params, testGiB))

	require.NoError(t, err)
	assert.Equal(t, "0777", modeSet, "an adopted restore must still be writable by the workload")
	assert.Equal(t, testStoreRealPath+"/pvc-restored", exportedFSPath, "an adopted restore must still get its export")
	assert.Equal(t, "csi-pvc-restored", policyName, "an adopted restore must still get its policy")
	require.NotNil(t, resp.GetVolume().GetContentSource(), "adoption must preserve the content source")
}

// The adoption branch reported the *requested* capacity without ever reading
// the quota, so a retry asking for a different size was answered with a
// fictional number instead of AlreadyExists. The non-restore path has guarded
// this all along.
func TestRestoreAdoptionRejectsADifferentCapacity(t *testing.T) {
	client := adoptedRestoreClient(uint64(testGiB)) // committed at 10 GiB
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(),
		restoreRequest("pvc-restored", testParams(), 5*testGiB)) // asked for 50

	assertCode(t, err, codes.AlreadyExists)
	assert.Nil(t, resp)
}

// The quota is set before the rename and the rename is the commit, so a
// committed restore always has one. An unreadable quota is a cluster error,
// not a reason to restore the whole tree again.
func TestRestoreAdoptionSurfacesAnUnreadableQuota(t *testing.T) {
	var restarted bool
	client := adoptedRestoreClient(0)
	client.GetQuotaFunc = func(ctx context.Context, fileID string) (uint64, error) {
		return 0, apiError(http.StatusInternalServerError)
	}
	client.GetSnapshotFunc = func(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
		restarted = true
		return qumulo.SnapshotInfo{ID: "42", Name: "42_snapshot-restore-src", SourceFileID: "900"}, nil
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(),
		restoreRequest("pvc-restored", testParams(), testGiB))

	assertCode(t, err, codes.Internal)
	assert.False(t, restarted, "an unreadable quota must not restart the restore")
}

// An attempt that set the staging directory's quota and then failed
// before the rename leaves the quota behind. The retry used to call
// CreateQuota, get 409 and answer Internal -- every time, so the commit was
// never reached and the PVC never bound. The quota step converges instead:
// an existing quota is read and brought to the requested limit.
func TestRestoreRetryAfterTheQuotaWasSetReachesTheCommit(t *testing.T) {
	for _, tt := range []struct {
		name     string
		existing uint64
		want     []uint64 // UpdateQuota calls
	}{
		{name: "the quota the earlier attempt set", existing: uint64(testGiB)},
		{name: "a quota at another limit is corrected", existing: uint64(testGiB) / 2, want: []uint64{uint64(testGiB)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var renames int
			var updates []uint64
			client := emptyRestoreClient()
			client.CreateDirectoryFunc = func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{}, apiError(http.StatusConflict) // staging left by the earlier attempt
			}
			client.LookupFileByPathFunc = func(ctx context.Context, p string) (qumulo.FileInfo, error) {
				if strings.Contains(p, restoreTempPrefix) {
					return qumulo.FileInfo{ID: "stage-1", Type: "FS_FILE_TYPE_DIRECTORY", Path: p}, nil
				}
				return qumulo.FileInfo{}, apiError(http.StatusNotFound) // not committed
			}
			client.GetQuotaFunc = func(ctx context.Context, fileID string) (uint64, error) { return tt.existing, nil }
			client.CreateQuotaFunc = func(ctx context.Context, fileID string, limit uint64) error {
				return apiError(http.StatusConflict) // the quota already exists
			}
			client.UpdateQuotaFunc = func(ctx context.Context, fileID string, limit uint64) error {
				updates = append(updates, limit)
				return nil
			}
			client.RenameFunc = func(ctx context.Context, parent, name, oldPath string) (qumulo.FileInfo, error) {
				renames++
				return qumulo.FileInfo{ID: "stage-1", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			}
			srv := newTestControllerServer(t, client)

			_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), testGiB))

			require.NoError(t, err)
			assert.Equal(t, 1, renames, "the retry reaches the commit")
			assert.Equal(t, tt.want, updates)
		})
	}
}

// TestRestoreSucceedsAfterTheSourceVolumeIsDeleted is a regression test.
//
// Snapshot a volume, delete the volume, restore from the snapshot: the
// standard backup workflow, and the one this driver refused. The restore
// resolved the snapshot's source file ID to a live path and walked that, so a
// DeleteVolume in between left the snapshot intact and the restore
// impossible -- NotFound, permanently, on a PVC that could never bind.
//
// The mock here reproduces exactly that cluster state: the snapshot answers
// with its source file ID, the ID still lists inside the snapshot, and
// /v1/files/resolve answers with nothing because the live directory is gone.
func TestRestoreSucceedsAfterTheSourceVolumeIsDeleted(t *testing.T) {
	var listedRefs []string

	client := emptyRestoreClient()
	// A tree-deleted directory: the API answers resolve with an empty path
	// rather than an error, which is what made this look unrecoverable.
	client.ResolveFileIDsFunc = func(ctx context.Context, ids []string) (map[string]string, error) {
		return map[string]string{}, nil
	}
	client.ListDirectoryFunc = func(ctx context.Context, ref, snapshotID string) ([]qumulo.DirEntry, error) {
		if snapshotID != "" {
			listedRefs = append(listedRefs, ref)
			if ref == "900" {
				return []qumulo.DirEntry{file("kept.txt", 10)}, nil
			}
			return nil, nil
		}
		return nil, apiError(http.StatusNotFound)
	}
	var copied []string
	client.CreateFileFunc = func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
		return qumulo.FileInfo{Path: parent + "/" + name}, nil
	}
	client.CopyFileFunc = func(ctx context.Context, target, sourceID, snap string, _ uint64) error {
		copied = append(copied, target)
		return nil
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), testGiB))

	require.NoError(t, err, "a snapshot must be restorable after its source volume is deleted")
	require.NotNil(t, resp.GetVolume())
	// Twice: once to add up its files before anything is copied, once
	// to copy them. Both by the source file ID.
	assert.Equal(t, []string{"900", "900"}, listedRefs,
		"the walk must address the snapshot by its source file ID, not by a live path")
	assert.Len(t, copied, 1, "the snapshot's contents must be copied, not silently skipped")
}

// The live path is still resolved, and still used -- as a log field. Pinned
// because the fix could have been made by deleting the resolve call, which
// would have removed the one thing that tells an operator which volume a
// restore came from.
func TestRestoreStillResolvesTheSourcePathWhenItExists(t *testing.T) {
	var resolvedIDs []string
	client := emptyRestoreClient()
	client.ResolveFileIDsFunc = func(ctx context.Context, ids []string) (map[string]string, error) {
		resolvedIDs = append(resolvedIDs, ids...)
		return map[string]string{"900": testStoreRealPath + "/pvc-source"}, nil
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), testGiB))

	require.NoError(t, err)
	assert.Equal(t, []string{"900"}, resolvedIDs)
}

// A resolve that fails outright must not fail the restore either: the walk
// does not need it, so an error there is a missing log field and nothing more.
func TestRestoreSurvivesAResolveError(t *testing.T) {
	client := emptyRestoreClient()
	client.ResolveFileIDsFunc = func(ctx context.Context, ids []string) (map[string]string, error) {
		return nil, apiError(http.StatusInternalServerError)
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), testGiB))

	require.NoError(t, err)
}

// The rename is the restore's commit. A side effect that fails after it
// -- here the snapshot policy, once -- used to go through the fresh-create
// rollback, which tree-deleted the renamed directory: the complete restored
// copy. Every retry then copied the whole snapshot again. Now the committed
// copy stays, and the retry adopts it and applies the side effect.
func TestRestoreSideEffectFailureAfterTheCommitKeepsTheRestoredCopy(t *testing.T) {
	requireScheduleEnabled(t)

	var committed bool
	var copies, treeDeletes, policyCalls int
	client := emptyRestoreClient()
	client.LookupFileByPathFunc = func(ctx context.Context, p string) (qumulo.FileInfo, error) {
		if strings.Contains(p, restoreTempPrefix) {
			return qumulo.FileInfo{ID: "stage-1", Type: "FS_FILE_TYPE_DIRECTORY", Path: p}, nil
		}
		if committed {
			return qumulo.FileInfo{ID: "stage-1", Type: "FS_FILE_TYPE_DIRECTORY", Path: p}, nil
		}
		return qumulo.FileInfo{}, apiError(http.StatusNotFound)
	}
	client.ListDirectoryFunc = func(ctx context.Context, ref, snapshotID string) ([]qumulo.DirEntry, error) {
		if snapshotID != "" {
			return []qumulo.DirEntry{file("data.bin", 10)}, nil
		}
		return nil, apiError(http.StatusNotFound)
	}
	client.CopyFileFunc = func(ctx context.Context, target, sourceID, snap string, _ uint64) error {
		copies++
		return nil
	}
	client.GetQuotaFunc = func(ctx context.Context, fileID string) (uint64, error) { return uint64(testGiB), nil }
	client.RenameFunc = func(ctx context.Context, parent, name, oldPath string) (qumulo.FileInfo, error) {
		committed = true
		return qumulo.FileInfo{ID: "stage-1", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
	}
	client.StartTreeDeleteFunc = func(ctx context.Context, fileID string) error {
		treeDeletes++
		return nil
	}
	client.CreatePolicyFunc = func(ctx context.Context, name, sourceFileID string,
		schedule qumulo.Schedule,
	) (qumulo.PolicyInfo, error) {
		policyCalls++
		if policyCalls == 1 {
			return qumulo.PolicyInfo{}, apiError(http.StatusInternalServerError)
		}
		return qumulo.PolicyInfo{ID: "77", Name: name}, nil
	}
	srv := newTestControllerServer(t, client)
	req := restoreRequest("pvc-restored", scheduleParams("daily@02:00,keep=7days"), testGiB)

	_, err := srv.CreateVolume(context.Background(), req)
	require.Error(t, err, "the policy failure is reported")
	require.True(t, committed)
	assert.Zero(t, treeDeletes, "the committed restore must not be tree-deleted")

	resp, err := srv.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp.GetVolume().GetContentSource())
	assert.Equal(t, 1, copies, "the retry adopts the committed copy instead of copying again")
	assert.Equal(t, 2, policyCalls, "the retry applies the policy")
	assert.Zero(t, treeDeletes)
}

// The fresh-create half. ensureVolumeSideEffects creates the export
// before the policy, so a policy failure rolled back the policy, the quota
// and the directory, and left a dedicated export serving a path that no
// longer existed.
func TestCreateVolumeRollbackRemovesTheExportItCreated(t *testing.T) {
	requireScheduleEnabled(t)

	var exports []qumulo.ExportInfo
	var deletedExports []string
	var treeDeletes int
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) { return exports, nil },
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			e := qumulo.ExportInfo{ID: "5", ExportPath: spec.ExportPath, FSPath: spec.FSPath}
			exports = append(exports, e)
			return e, nil
		},
		DeleteExportFunc: func(ctx context.Context, id string) error {
			deletedExports = append(deletedExports, id)
			return nil
		},
		CreatePolicyFunc: func(ctx context.Context, name, sourceFileID string,
			schedule qumulo.Schedule,
		) (qumulo.PolicyInfo, error) {
			return qumulo.PolicyInfo{}, apiError(http.StatusInternalServerError)
		},
		StartTreeDeleteFunc: func(ctx context.Context, fileID string) error {
			treeDeletes++
			return nil
		},
	}
	params := scheduleParams("daily@02:00,keep=7days")
	params["provisioningModel"] = "dedicated-export"
	params["allowAnyClient"] = "true"
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         params,
	})

	require.Error(t, err)
	require.Len(t, exports, 1, "the export was created before the policy failed")
	assert.Equal(t, []string{"5"}, deletedExports, "the rollback removes the export it created")
	assert.Equal(t, 1, treeDeletes, "an uncommitted fresh create is still rolled back")
}

// A restore from a snapshot holding more than the claim asks for used
// to copy everything, set the quota after, and bind already over it: 1.17
// GiB of data in a 1 GiB claim, and ENOSPC on the first write. It is now
// refused with OutOfRange before anything is staged, naming the size that
// would fit.
func TestRestoreIntoAClaimSmallerThanTheSnapshotIsOutOfRange(t *testing.T) {
	var staged, measuredRef, measuredSnap string
	client := emptyRestoreClient()
	client.GetTreeUsageFunc = func(ctx context.Context, fileID, snapshotID string) (qumulo.TreeUsage, error) {
		measuredRef, measuredSnap = fileID, snapshotID
		return qumulo.TreeUsage{Capacity: 5 << 30}, nil
	}
	client.CreateDirectoryFunc = func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
		staged = name
		return qumulo.FileInfo{ID: "stage-1", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), 1<<30))

	assertCode(t, err, codes.OutOfRange)
	assert.Contains(t, err.Error(), "request at least 5368709120 bytes")
	assert.Equal(t, "900", measuredRef, "the snapshot's source tree is measured")
	assert.Equal(t, "42", measuredSnap, "as of the snapshot, not live")
	assert.Empty(t, staged, "nothing is staged for a restore that cannot fit")
}

// The rest of the size check's table: data that fits restores; an array
// that does not answer is Unavailable; a measurement the array refuses
// (older Core, narrower token) restores without the check, as before.
func TestRestoreSizeCheck(t *testing.T) {
	for _, tt := range []struct {
		name     string
		used     uint64
		err      error
		wantCode codes.Code
	}{
		{name: "data that fits restores", used: 1 << 29},
		{name: "exactly the claim restores", used: 1 << 30},
		{name: "an array that does not answer is Unavailable",
			err: &url.Error{Op: "Get", URL: "https://x", Err: context.DeadlineExceeded}, wantCode: codes.Unavailable},
		{name: "a refused measurement restores without the check", err: apiError(http.StatusForbidden)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := emptyRestoreClient()
			client.GetTreeUsageFunc = func(ctx context.Context, fileID, snapshotID string) (qumulo.TreeUsage, error) {
				return qumulo.TreeUsage{Capacity: tt.used}, tt.err
			}
			srv := newTestControllerServer(t, client)

			_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), 1<<30))

			if tt.wantCode == codes.OK {
				require.NoError(t, err)
			} else {
				assertCode(t, err, tt.wantCode)
			}
		})
	}
}

// Two names with one file ID are one inode with two hard links. The
// restore used to copy both: two independent files, the link lost, and the
// space doubled. The second name is now a link to the first, anywhere in the
// tree, and adds no bytes.
func TestRestoreRebuildsAHardLinkAsOneFile(t *testing.T) {
	linked := qumulo.DirEntry{Name: "data.bin", ID: "id-inode-7", Type: "FS_FILE_TYPE_FILE", Size: 1 << 30}
	alias := linked
	alias.Name = "data-link.bin"
	tree := &fakeTree{source: map[string][]qumulo.DirEntry{
		"id-vol": {linked, alias, dir("sub")},
		"id-sub": {linked}, // a third name, in another directory
	}}
	c := tree.client()
	srv := newTestControllerServer(t, c)

	files, bytes, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, []string{"/stage/data.bin"}, tree.copied, "the data is copied once")
	assert.Equal(t, []string{
		"/stage/data-link.bin -> /stage/data.bin",
		"/stage/sub/data.bin -> /stage/data.bin",
	}, tree.links)
	assert.Equal(t, 3, files)
	assert.Equal(t, uint64(1<<30), bytes, "a link adds no bytes")
}

// The link's edges: a name an earlier attempt already made is left alone; an
// array that refuses the link gets a copy, as before; one that does not
// answer fails the attempt, like any other step.
func TestRestoreHardLinkEdges(t *testing.T) {
	linked := qumulo.DirEntry{Name: "a", ID: "id-inode-7", Type: "FS_FILE_TYPE_FILE", Size: 10}
	alias := linked
	alias.Name = "b"

	t.Run("already made by an earlier attempt", func(t *testing.T) {
		tree := &fakeTree{
			source: map[string][]qumulo.DirEntry{"id-vol": {linked, alias}},
			target: map[string][]qumulo.DirEntry{"/stage": {file("a", 10), file("b", 10)}},
		}
		c := tree.client()
		srv := newTestControllerServer(t, c)

		_, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

		require.NoError(t, err)
		assert.Empty(t, tree.copied)
		assert.Empty(t, tree.links)
	})

	t.Run("a refused link is copied instead", func(t *testing.T) {
		tree := &fakeTree{source: map[string][]qumulo.DirEntry{"id-vol": {linked, alias}}}
		c := tree.client()
		c.CreateLinkFunc = func(context.Context, string, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusBadRequest)
		}
		srv := newTestControllerServer(t, c)

		_, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

		require.NoError(t, err)
		assert.Equal(t, []string{"/stage/a", "/stage/b"}, tree.copied)
	})

	t.Run("an array that does not answer fails the attempt", func(t *testing.T) {
		tree := &fakeTree{source: map[string][]qumulo.DirEntry{"id-vol": {linked, alias}}}
		c := tree.client()
		c.CreateLinkFunc = func(context.Context, string, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, &url.Error{Op: "Post", URL: "https://x", Err: context.DeadlineExceeded}
		}
		srv := newTestControllerServer(t, c)

		_, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

		require.Error(t, err)
		assert.Equal(t, []string{"/stage/a"}, tree.copied, "the second name is not copied either")
	})
}

// owned is a snapshot entry with the attributes a restore must carry over.
func owned(e qumulo.DirEntry, mode, owner, mtime string) qumulo.DirEntry {
	e.Mode, e.Owner, e.Group, e.ModificationTime = mode, owner, owner, mtime
	return e
}

// Every entry below the root used to come back owned by the API user,
// with the cluster's default mode and the restore's own time: uid 1000
// became 0, a 0700 directory 0755, a 0755 script 0644, a 2020 mtime today.
// Each entry now gets its snapshot attributes, a directory's after its
// children, since creating a child moves the directory's mtime.
func TestRestoreKeepsModeOwnerGroupAndMtimeBelowTheRoot(t *testing.T) {
	const old = "2020-01-01T00:00:00Z"
	tree := &fakeTree{source: map[string][]qumulo.DirEntry{
		"id-vol": {
			owned(dir("priv"), "0700", "12884901001", old),
			owned(file("run.sh", 10), "0755", "12884901001", old),
		},
		"id-priv": {owned(file("f", 5), "0600", "12884901001", old)},
	}}
	c := tree.client()
	srv := newTestControllerServer(t, c)

	_, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, []string{
		"/stage/priv/f 0600 12884901001 12884901001 " + old,
		"/stage/priv 0700 12884901001 12884901001 " + old,
		"/stage/run.sh 0755 12884901001 12884901001 " + old,
	}, tree.attrs)
}

// A resumed restore sets attributes only where the staged entry's differ, so
// it does not spend a call per already-finished file; and a directory an
// attempt stopped inside -- it still has the mtime its last child's creation
// gave it -- is finished and then given its attributes.
func TestRestoreResumeSetsOnlyTheAttributesThatDiffer(t *testing.T) {
	const old = "2020-01-01T00:00:00Z"
	finished := owned(file("done", 10), "0644", "1", old)
	tree := &fakeTree{
		source: map[string][]qumulo.DirEntry{
			"id-vol": {finished, owned(file("wrong-mode", 10), "0755", "1", old), owned(dir("sub"), "0750", "1", old)},
			"id-sub": {owned(file("new", 3), "0644", "1", old)},
		},
		target: map[string][]qumulo.DirEntry{
			"/stage": {
				finished, // an earlier attempt finished it
				owned(file("wrong-mode", 10), "0644", "1", old),        // copied, attributes never set
				owned(dir("sub"), "0750", "1", "2026-09-29T10:00:00Z"), // stopped inside: a child is missing
			},
		},
	}
	c := tree.client()
	srv := newTestControllerServer(t, c)

	_, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, []string{
		"/stage/wrong-mode 0755 1 1 " + old,
		"/stage/sub/new 0644 1 1 " + old,
		"/stage/sub 0750 1 1 " + old,
	}, tree.attrs)
	assert.Equal(t, []string{"/stage/sub/new"}, tree.copied)
}

// An array that refuses the update (a token that may not change ownership)
// still gets a complete restore, as before; one that does not answer fails
// the attempt like any other step.
func TestRestoreAttributeFailures(t *testing.T) {
	source := map[string][]qumulo.DirEntry{"id-vol": {owned(file("a", 10), "0600", "1", "2020-01-01T00:00:00Z")}}

	t.Run("refused", func(t *testing.T) {
		tree := &fakeTree{source: source}
		c := tree.client()
		c.SetFileAttributesFunc = func(context.Context, string, qumulo.FileAttributes) error {
			return apiError(http.StatusForbidden)
		}
		srv := newTestControllerServer(t, c)

		files, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

		require.NoError(t, err)
		assert.Equal(t, 1, files)
	})

	t.Run("no answer", func(t *testing.T) {
		tree := &fakeTree{source: source}
		c := tree.client()
		c.SetFileAttributesFunc = func(context.Context, string, qumulo.FileAttributes) error {
			return &url.Error{Op: "Patch", URL: "https://x", Err: context.DeadlineExceeded}
		}
		srv := newTestControllerServer(t, c)

		_, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

		require.ErrorIs(t, err, context.DeadlineExceeded, "an expired budget reads as Aborted, so the CO retries")
	})
}

// stagingArray is a restore target that behaves like the array: directories
// it holds are listed with the attributes last set on them, and every call
// is charged against a per-attempt budget, as the provisioner's timeout
// charges a real attempt.
type stagingArray struct {
	source map[string][]qumulo.DirEntry
	dirs   map[string]qumulo.DirEntry // staged path -> its entry, attributes included
	calls  int
	budget int
	cancel context.CancelFunc
}

func (a *stagingArray) tick(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.calls++
	if a.calls >= a.budget {
		a.cancel()
	}
	return nil
}

func (a *stagingArray) client() *MockClient {
	return &MockClient{
		ListDirectoryFunc: func(ctx context.Context, ref, snap string) ([]qumulo.DirEntry, error) {
			if err := a.tick(ctx); err != nil {
				return nil, err
			}
			if snap != "" {
				return a.source[ref], nil
			}
			if _, ok := a.dirs[ref]; !ok && ref != "/stage" {
				return nil, apiError(http.StatusNotFound)
			}
			var out []qumulo.DirEntry
			for p, e := range a.dirs {
				if path.Dir(p) == ref {
					out = append(out, e)
				}
			}
			return out, nil
		},
		CreateDirectoryFunc: func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
			if err := a.tick(ctx); err != nil {
				return qumulo.FileInfo{}, err
			}
			p := parent + "/" + name
			if _, ok := a.dirs[p]; ok {
				return qumulo.FileInfo{}, apiError(http.StatusConflict)
			}
			// A new directory has the time of its creation.
			a.dirs[p] = qumulo.DirEntry{Name: name, Type: "FS_FILE_TYPE_DIRECTORY", ModificationTime: "now"}
			return qumulo.FileInfo{Path: p}, nil
		},
		SetFileAttributesFunc: func(ctx context.Context, p string, attrs qumulo.FileAttributes) error {
			if err := a.tick(ctx); err != nil {
				return err
			}
			e := a.dirs[p]
			e.Mode, e.Owner, e.Group, e.ModificationTime = attrs.Mode, attrs.Owner, attrs.Group, attrs.ModificationTime
			a.dirs[p] = e
			return nil
		},
	}
}

// A snapshot of many directories used to never finish restoring: every
// attempt re-walked every directory already staged (a 409 and two listings
// each) before doing anything new, so once that outgrew one attempt's budget
// progress stopped -- 20,000 directories stalled at 14,264. A finished
// subtree now costs nothing on a later attempt, so each attempt makes
// progress and the restore completes.
func TestRestoreOfManyDirectoriesCompletes(t *testing.T) {
	const dirs = 600
	const budgetCalls = 600

	source := map[string][]qumulo.DirEntry{"root": {}}
	for i := 0; i < dirs; i++ {
		d := owned(dir(fmt.Sprintf("d%04d", i)), "0755", "1", "2020-01-01T00:00:00Z")
		source["root"] = append(source["root"], d)
		source[d.ID] = []qumulo.DirEntry{}
	}
	array := &stagingArray{source: source, dirs: map[string]qumulo.DirEntry{}, budget: budgetCalls}
	c := array.client()
	srv := newTestControllerServer(t, c)

	var progress []int
	for attempt := 1; ; attempt++ {
		require.LessOrEqual(t, attempt, 10, "no convergence: staged after each attempt %v", progress)
		array.calls = 0
		ctx, cancel := context.WithCancel(context.Background())
		array.cancel = cancel
		_, _, err := restoreAttempt(ctx, srv, c, "42", "root", "/stage")
		cancel()
		progress = append(progress, len(array.dirs))
		if err == nil {
			break
		}
		require.ErrorIs(t, err, context.Canceled)
		if len(progress) > 1 {
			require.Greater(t, progress[len(progress)-1], progress[len(progress)-2], "every attempt makes progress")
		}
	}
	t.Logf("directories staged after each attempt: %v", progress)
	assert.Len(t, array.dirs, dirs)
}

// A directory an attempt stopped inside is not taken as finished, whatever
// its name: only the snapshot's own attributes on it say so, and those are
// set last.
func TestRestoreDoesNotSkipADirectoryItStoppedInside(t *testing.T) {
	sub := owned(dir("sub"), "0755", "1", "2020-01-01T00:00:00Z")
	array := &stagingArray{
		source: map[string][]qumulo.DirEntry{"root": {sub}, "id-sub": {owned(dir("inner"), "0755", "1", "2020-01-01T00:00:00Z")}},
		// Staged, but with the mtime its creation gave it: not finished.
		dirs:   map[string]qumulo.DirEntry{"/stage/sub": {Name: "sub", Type: "FS_FILE_TYPE_DIRECTORY", ModificationTime: "now"}},
		budget: 1000,
	}
	c := array.client()
	srv := newTestControllerServer(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	array.cancel = cancel

	_, _, err := restoreAttempt(ctx, srv, c, "42", "root", "/stage")

	require.NoError(t, err)
	assert.Contains(t, array.dirs, "/stage/sub/inner")
	assert.Equal(t, "2020-01-01T00:00:00Z", array.dirs["/stage/sub"].ModificationTime)
}

// The caps are decided by the snapshot's own counts, directories
// included, before anything is staged. The walk only ever counted one
// attempt's work, so a tree past the caps was never refused.
func TestRestoreRefusesATreePastTheCapsUpFront(t *testing.T) {
	for _, tt := range []struct {
		name  string
		usage qumulo.TreeUsage
	}{
		{name: "too many directories", usage: qumulo.TreeUsage{Directories: maxRestoreFiles + 1}},
		{name: "too many entries", usage: qumulo.TreeUsage{Files: maxRestoreFiles, Directories: 1}},
		{name: "too many bytes", usage: qumulo.TreeUsage{Capacity: maxRestoreBytes + 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var staged bool
			client := emptyRestoreClient()
			client.GetTreeUsageFunc = func(context.Context, string, string) (qumulo.TreeUsage, error) { return tt.usage, nil }
			client.CreateDirectoryFunc = func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
				staged = true
				return qumulo.FileInfo{ID: "stage-1", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			}
			srv := newTestControllerServer(t, client)

			_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), 1<<50))

			assertCode(t, err, codes.ResourceExhausted)
			assert.False(t, staged)
		})
	}
}

// restoreAttempt runs one restore attempt the way createVolumeFromSnapshot
// does: with the server's progress record for the restore, so consecutive
// attempts on one server resume from each other as they do in production.
func restoreAttempt(ctx context.Context, srv *ControllerServer, client qumulo.ClientInterface, snapshotID, sourceID, target string) (int, uint64, error) {
	memo := srv.restoreProgress.take(restoreProgressKey("https://test.example:8000", target, snapshotID), "staging-id")
	return srv.restoreFromSnapshot(ctx, client, snapshotID, sourceID, target, memo)
}

// When the array refuses the attribute update. The skip recognised a
// finished directory by the attributes it had been given, so with every
// update refused nothing was ever recognised and each attempt re-walked the
// whole tree: 600 directories against a 600-call budget never finished. The
// restore's memo now records a directory as finished whether or not its
// attributes were accepted.
func TestRestoreConvergesWhenTheArrayRefusesAttributes(t *testing.T) {
	const dirs = 600
	source := map[string][]qumulo.DirEntry{"root": {}}
	for i := 0; i < dirs; i++ {
		d := owned(dir(fmt.Sprintf("d%04d", i)), "0755", "1", "2020-01-01T00:00:00Z")
		source["root"] = append(source["root"], d)
		source[d.ID] = []qumulo.DirEntry{}
	}
	array := &stagingArray{source: source, dirs: map[string]qumulo.DirEntry{}, budget: 600}
	c := array.client()
	c.SetFileAttributesFunc = func(ctx context.Context, p string, a qumulo.FileAttributes) error {
		if err := array.tick(ctx); err != nil {
			return err
		}
		return apiError(http.StatusForbidden)
	}
	srv := newTestControllerServer(t, c)

	var err error
	for attempt := 1; attempt <= 10; attempt++ {
		array.calls = 0
		ctx, cancel := context.WithCancel(context.Background())
		array.cancel = cancel
		_, _, err = restoreAttempt(ctx, srv, c, "42", "root", "/stage")
		cancel()
		if err == nil {
			break
		}
	}

	require.NoError(t, err, "the restore converges although every attribute update is refused")
	assert.Len(t, array.dirs, dirs)
}

// The skip left a finished subtree unwalked, and the hard-link map
// is filled as the walk visits files, so a link whose first name was in the
// skipped subtree was copied as an independent file -- again. The map is
// part of the restore's memo, which outlives the attempt that filled it.
func TestResumedRestoreKeepsAHardLinkWhoseFirstNameIsInASkippedSubtree(t *testing.T) {
	big := qumulo.DirEntry{Name: "big", ID: "id-inode-7", Type: "FS_FILE_TYPE_FILE", Size: 1 << 30,
		Mode: "0644", Owner: "1", Group: "1", ModificationTime: "2020-01-01T00:00:00Z"}
	lnk := big
	lnk.Name = "big.lnk"
	a := owned(dir("a"), "0755", "1", "2020-01-01T00:00:00Z")
	b := owned(dir("b"), "0755", "1", "2020-01-01T00:00:00Z")
	newTree := func() *fakeTree {
		return &fakeTree{
			source: map[string][]qumulo.DirEntry{"id-vol": {a, b}, "id-a": {big}, "id-b": {lnk}},
			// Attempt 1 finished a/ -- big copied, attributes set -- and
			// was cut off in b/.
			target: map[string][]qumulo.DirEntry{
				"/stage":   {a, {Name: "b", ID: "x", Type: "FS_FILE_TYPE_DIRECTORY", ModificationTime: "now"}},
				"/stage/a": {big},
				"/stage/b": {},
			},
		}
	}

	t.Run("attempt 1 on this controller: a/ is skipped and the link kept", func(t *testing.T) {
		tree := newTree()
		c := tree.client()
		srv := newTestControllerServer(t, c)
		memo := srv.restoreProgress.take(restoreProgressKey("https://test.example:8000", "/stage", "42"), "staging-id")
		memo.completed["/stage/a"] = true
		memo.staged["id-inode-7"] = "/stage/a/big"
		listedA := 0
		list := c.ListDirectoryFunc
		c.ListDirectoryFunc = func(ctx context.Context, p, snap string) ([]qumulo.DirEntry, error) {
			if p == "id-a" || p == "/stage/a" {
				listedA++
			}
			return list(ctx, p, snap)
		}

		_, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

		require.NoError(t, err)
		assert.Zero(t, listedA, "the finished subtree is not walked again")
		assert.Empty(t, tree.copied, "the second name is linked, not copied")
		assert.Equal(t, []string{"/stage/b/big.lnk -> /stage/a/big"}, tree.links)
	})

	t.Run("attempt 1 on a restarted controller: a/ is walked again and the link kept", func(t *testing.T) {
		tree := newTree()
		c := tree.client()
		srv := newTestControllerServer(t, c) // an empty memo

		_, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

		require.NoError(t, err)
		assert.Empty(t, tree.copied)
		assert.Equal(t, []string{"/stage/b/big.lnk -> /stage/a/big"}, tree.links)
	})
}

// A staging directory that was reclaimed and made again is a new tree: a
// memo for the old one would skip directories it does not have.
func TestRestoreProgressIsForgottenForANewStagingDirectory(t *testing.T) {
	p := newRestoreProgress()
	first := p.take("k", "staging-1")
	first.completed["/stage/a"] = true

	assert.True(t, p.take("k", "staging-1").completed["/stage/a"], "the same staging directory resumes")
	assert.Empty(t, p.take("k", "staging-2").completed, "a new staging directory starts over")
}

// Progress older than restoreProgressTTL belongs to an abandoned claim.
func TestRestoreProgressExpires(t *testing.T) {
	p := newRestoreProgress()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	p.now = func() time.Time { return now }
	p.take("k", "s").completed["/stage/a"] = true

	now = now.Add(restoreProgressTTL + time.Minute)

	assert.Empty(t, p.take("k", "s").completed)
}

// The memo outlives the attempt, so a file an earlier attempt started is
// already in it under its own path. Met again, it is that same file being
// resumed, not a second name to link to itself.
func TestResumedRestoreFinishesAFileTheMemoAlreadyNames(t *testing.T) {
	f := qumulo.DirEntry{Name: "f", ID: "id-f", Type: "FS_FILE_TYPE_FILE", Size: 100,
		Mode: "0644", Owner: "1", Group: "1", ModificationTime: "2020-01-01T00:00:00Z"}
	partial := f
	partial.Size = 50
	tree := &fakeTree{
		source: map[string][]qumulo.DirEntry{"id-vol": {f}},
		target: map[string][]qumulo.DirEntry{"/stage": {partial}},
	}
	c := tree.client()
	srv := newTestControllerServer(t, c)
	srv.restoreProgress.take(restoreProgressKey("https://test.example:8000", "/stage", "42"), "staging-id").
		staged["id-f"] = "/stage/f"

	_, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Empty(t, tree.links, "a file is never linked to itself")
	assert.Contains(t, tree.copied, "/stage/f", "the interrupted copy is finished")
}

// A restore warned once per skipped symlink or special file, on every
// attempt: 5,000 symlinks gave 5,000, then 10,000, then 15,000 WARN lines
// over three attempts. One attempt now warns once, with the count and the
// first path.
func TestRestoreWarnsOnceAnAttemptForEntriesItSkips(t *testing.T) {
	var logged bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(restore) })

	entries := []qumulo.DirEntry{file("real", 1)}
	for i := range 50 {
		entries = append(entries, qumulo.DirEntry{Name: fmt.Sprintf("link-%d", i), Type: "FS_FILE_TYPE_SYMLINK"})
	}
	tree := &fakeTree{source: map[string][]qumulo.DirEntry{"id-vol": entries}}
	c := tree.client()
	srv := newTestControllerServer(t, c)

	_, _, err := restoreAttempt(context.Background(), srv, c, "42", "id-vol", "/stage")
	require.NoError(t, err)

	assert.Equal(t, 1, strings.Count(logged.String(), "level=WARN"), logged.String())
	assert.Contains(t, logged.String(), "entries=50")
	assert.Contains(t, logged.String(), "/stage/link-0")
}

// A memo holds a restore's whole walk state -- a directory set and a
// hard-link map up to maxRestoreFiles -- and was kept 24 h, dropped only when
// another restore happened to run. csi-provisioner retries an Aborted restore
// within its backoff (five minutes at most), so the TTL is an hour, and at
// most maxRestoreMemos are kept: the least recently touched goes first.
func TestRestoreProgressIsBoundedInAgeAndCount(t *testing.T) {
	assert.LessOrEqual(t, restoreProgressTTL, time.Hour, "an hour covers every retry of a live restore")

	p := newRestoreProgress()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	p.now = func() time.Time { return now }
	for i := range maxRestoreMemos + 5 {
		now = now.Add(time.Second)
		p.take(fmt.Sprintf("k%d", i), "s").completed["/stage/a"] = true
	}

	assert.Len(t, p.memos, maxRestoreMemos)
	_, oldest := p.memos["k0"]
	assert.False(t, oldest, "the least recently touched memo is evicted")
	_, newest := p.memos[fmt.Sprintf("k%d", maxRestoreMemos+4)]
	assert.True(t, newest)
}

// pagingClient serves directory listings a page at a time, as the real client
// does, and records when each page was read.
type pagingClient struct {
	*MockClient
	pages  map[string][][]qumulo.DirEntry
	events *[]string
}

func (p pagingClient) ListDirectoryPages(_ context.Context, ref, snapshotID string, fn func([]qumulo.DirEntry) error) error {
	pages, ok := p.pages[ref]
	if !ok {
		return apiError(http.StatusNotFound)
	}
	for i, page := range pages {
		*p.events = append(*p.events, fmt.Sprintf("page %s %d", ref, i))
		if err := fn(page); err != nil {
			return err
		}
	}
	return nil
}

// A restore listed each directory whole before copying any of it, and
// held every ancestor's listing while it descended. It now works through a
// listing a page at a time: page 1's entries are restored before page 2 is
// read, so no more than a page per level is held.
func TestRestoreWorksThroughADirectoryAPageAtATime(t *testing.T) {
	var events []string
	tree := &fakeTree{}
	mock := tree.client()
	create := mock.CreateFileFunc
	mock.CreateFileFunc = func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
		events = append(events, "create "+name)
		return create(ctx, parent, name)
	}
	client := pagingClient{MockClient: mock, events: &events, pages: map[string][][]qumulo.DirEntry{
		"id-vol": {{file("a.txt", 10), file("b.txt", 10)}, {file("c.txt", 10)}},
	}}
	srv := newTestControllerServer(t, mock)

	files, _, err := restoreAttempt(context.Background(), srv, client, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, 3, files)
	assert.Equal(t, []string{"page id-vol 0", "create a.txt", "create b.txt", "page id-vol 1", "create c.txt"}, events)
}
