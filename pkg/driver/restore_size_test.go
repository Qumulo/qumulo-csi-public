// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// staleTotalsTree is the lab state: the array's tree totals for snapshot
// 42 have not caught up (24 KiB), while its files hold 1.2 GiB, spread over a
// subdirectory so the walk has to descend.
func staleTotalsTree(client *MockClient, listings *int) {
	client.GetTreeUsageFunc = func(ctx context.Context, fileID, snapshotID string) (qumulo.TreeUsage, error) {
		return qumulo.TreeUsage{Capacity: 24 << 10, Files: 1, Directories: 1}, nil
	}
	client.ListDirectoryFunc = func(ctx context.Context, ref, snapshotID string) ([]qumulo.DirEntry, error) {
		if snapshotID == "" {
			return nil, nil
		}
		if listings != nil {
			*listings++
		}
		switch ref {
		case "900":
			return []qumulo.DirEntry{file("a.bin", 600<<20), dir("sub")}, nil
		case "id-sub":
			return []qumulo.DirEntry{file("b.bin", 629<<20)}, nil
		}
		return nil, nil
	}
}

// A snapshot taken right after a large write has tree totals
// that have not settled, and the restore trusted them: a 1 GiB claim
// restored 1.26 GB. The files, added up, decide now, and nothing is staged.
func TestB2RestoreRefusedWhenTheFilesExceedTheClaimDespiteStaleTotals(t *testing.T) {
	client := emptyRestoreClient()
	staleTotalsTree(client, nil)
	staged := false
	client.CreateDirectoryFunc = func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
		staged = true
		return qumulo.FileInfo{ID: "stage-1", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), 1<<30))

	assertCode(t, err, codes.OutOfRange)
	assert.Contains(t, err.Error(), "holds at least")
	assert.False(t, staged, "nothing is staged for a restore that cannot fit")
}

// The same tree fits a claim large enough for it.
func TestB2RestoreProceedsWhenTheFilesFit(t *testing.T) {
	client := emptyRestoreClient()
	staleTotalsTree(client, nil)
	client.CreateFileFunc = func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
		return qumulo.FileInfo{Path: parent + "/" + name}, nil
	}
	client.CopyFileFunc = func(ctx context.Context, target, sourceID, snap string, _ uint64) error { return nil }
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), 2<<30))

	require.NoError(t, err)
}

// A hard-linked file is restored as one file with two names, so it is
// counted once: two 600 MiB names of one inode fit a 1 GiB claim.
func TestB2HardLinksAreCountedOnce(t *testing.T) {
	two := 2
	linked := func(name string) qumulo.DirEntry {
		e := file(name, 600<<20)
		e.ID, e.NumLinks = "inode-7", &two
		return e
	}
	client := &MockClient{
		ListDirectoryFunc: func(ctx context.Context, ref, snapshotID string) ([]qumulo.DirEntry, error) {
			return []qumulo.DirEntry{linked("one"), linked("two")}, nil
		},
	}

	data, err := measureSnapshotData(context.Background(), client, "42", "900", 0)

	require.NoError(t, err)
	assert.True(t, data.complete)
	assert.Equal(t, uint64(600<<20), data.bytes)
	assert.Equal(t, 2, data.entries)
}

// Symlinks and devices are not restored, so they are not counted.
func TestB2OnlyRegularFilesAreCounted(t *testing.T) {
	client := &MockClient{
		ListDirectoryFunc: func(ctx context.Context, ref, snapshotID string) ([]qumulo.DirEntry, error) {
			return []qumulo.DirEntry{file("f", 10), {Name: "l", ID: "l", Type: "FS_FILE_TYPE_SYMLINK", Size: 1 << 40}}, nil
		},
	}

	data, err := measureSnapshotData(context.Background(), client, "42", "900", 0)

	require.NoError(t, err)
	assert.Equal(t, uint64(10), data.bytes)
}

// The walk stops as soon as the answer is known, so a huge tree does not
// have to be listed whole to refuse a small claim -- and a sum that stopped
// early is never remembered as the snapshot's size.
func TestB2MeasureStopsOnceOverTheLimitAndIsNotRemembered(t *testing.T) {
	var listings int
	client := emptyRestoreClient()
	staleTotalsTree(client, &listings)
	srv := newTestControllerServer(t, client)

	data, err := srv.snapshotData(context.Background(), client, "ep", "42", "900", 100<<20, time.Minute)

	require.NoError(t, err)
	assert.False(t, data.complete)
	assert.Equal(t, 1, listings, "the subdirectory is not listed once the root alone is over the limit")
	_, ok := srv.snapshotSizes.get(snapshotSizeKey("ep", "42"))
	assert.False(t, ok, "a partial sum is not remembered")
}

// A snapshot never changes, so a whole-tree sum is remembered and a retried
// restore, or the next ListSnapshots, does not list the tree again.
func TestB2WholeTreeSumIsRemembered(t *testing.T) {
	var listings int
	client := emptyRestoreClient()
	staleTotalsTree(client, &listings)
	srv := newTestControllerServer(t, client)

	first, err := srv.snapshotData(context.Background(), client, "ep", "42", "900", 0, time.Minute)
	require.NoError(t, err)
	second, err := srv.snapshotData(context.Background(), client, "ep", "42", "900", 0, time.Minute)
	require.NoError(t, err)

	assert.Equal(t, 2, listings, "listed once, root and subdirectory")
	assert.Equal(t, first, second)
	assert.Equal(t, uint64(1229<<20), second.bytes)
	_, ok := srv.snapshotSizes.get(snapshotSizeKey("other-ep", "42"))
	assert.False(t, ok, "keyed by array as well as snapshot ID")
}

// Remembered sums expire and are bounded.
func TestB2SnapshotSizesExpireAndAreBounded(t *testing.T) {
	c := newSnapshotSizes()
	now := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return now }
	c.put("a", snapshotData{bytes: 1, complete: true})
	now = now.Add(snapshotSizeTTL + time.Second)
	_, ok := c.get("a")
	assert.False(t, ok, "past the TTL")

	for i := 0; i < maxSnapshotSizes+10; i++ {
		now = now.Add(time.Millisecond)
		c.put(string(rune('A'+i%26))+time.Duration(i).String(), snapshotData{complete: true})
	}
	assert.LessOrEqual(t, len(c.entries), maxSnapshotSizes)

	var nilCache *snapshotSizes
	nilCache.put("x", snapshotData{complete: true})
	_, ok = nilCache.get("x")
	assert.False(t, ok, "a server built without the cache still works")
}

// A listing the array does not answer is Unavailable on a class that asks
// for that; one it refuses restores on the totals alone, as before.
func TestB2MeasureFailures(t *testing.T) {
	for _, tt := range []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{name: "an array that does not answer is Unavailable",
			err: &url.Error{Op: "Get", URL: "https://x", Err: context.DeadlineExceeded}, wantCode: codes.Unavailable},
		{name: "a refused listing restores on the totals alone", err: apiError(http.StatusForbidden)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := emptyRestoreClient()
			calls := 0
			client.ListDirectoryFunc = func(ctx context.Context, ref, snapshotID string) ([]qumulo.DirEntry, error) {
				if snapshotID != "" {
					calls++
					if calls == 1 {
						return nil, tt.err
					}
				}
				return nil, nil
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

// Decision: an imported snapshot reports the larger of its tree
// totals and its files added up, so the provisioner itself refuses a claim
// too small for it.
func TestB2ImportedSnapshotReportsTheLargerSize(t *testing.T) {
	for _, tt := range []struct {
		name   string
		totals uint64
		want   int64
	}{
		{name: "stale totals: the files decide", totals: 24 << 10, want: 1229 << 20},
		{name: "settled totals larger than the files: the totals decide", totals: 2 << 30, want: 2 << 30},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := emptyRestoreClient()
			staleTotalsTree(client, nil)
			client.GetTreeUsageFunc = func(ctx context.Context, fileID, snapshotID string) (qumulo.TreeUsage, error) {
				return qumulo.TreeUsage{Capacity: tt.totals}, nil
			}
			srv := newTestControllerServer(t, client)

			resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
				Secrets:    testSecrets(),
				SnapshotId: testServer + "##42",
			})

			require.NoError(t, err)
			require.Len(t, resp.GetEntries(), 1)
			assert.Equal(t, tt.want, resp.GetEntries()[0].GetSnapshot().GetSizeBytes())
		})
	}
}
