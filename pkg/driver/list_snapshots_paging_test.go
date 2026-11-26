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

	"qumulo-csi-driver/pkg/qumulo"
)

// The ListSnapshots token was an offset into the list the previous page
// was cut from. A snapshot deleted between two pages -- by an expiring
// policy, by a backup tool, by the CO itself -- shifts every later entry up
// by one, so the offset skips exactly as many snapshots as were removed, and
// a CO paging the list to decide what still exists concludes they are gone.
//
// The token now names a snapshot, so a page resumes at the first snapshot at
// or after it however many have gone in between.

// snapshotsOnArray is a mock whose listing a test can change between pages.
func snapshotsOnArray(snapshots *[]qumulo.SnapshotInfo) *MockClient {
	return &MockClient{
		LookupFileByPathFunc: volumeAt(testStoreRealPath+"/pvc-source", testFileID),
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			return append([]qumulo.SnapshotInfo(nil), *snapshots...), nil
		},
	}
}

func snapshot(id string) qumulo.SnapshotInfo {
	return qumulo.SnapshotInfo{ID: id, Name: id + "_snapshot-snap", SourceFileID: testFileID}
}

// listedIDs walks every page and returns the snapshot IDs in order.
func listedIDs(t *testing.T, srv *ControllerServer, maxEntries int32, between func(afterPage int)) []string {
	t.Helper()

	var ids []string
	token := ""
	for page := 0; ; page++ {
		resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
			Secrets:        testSecrets(),
			SourceVolumeId: allSnapshotsSource,
			MaxEntries:     maxEntries,
			StartingToken:  token,
		})
		require.NoError(t, err)

		for _, entry := range resp.GetEntries() {
			// The handle records the endpoint, so compare the raw ID.
			id := entry.GetSnapshot().GetSnapshotId()
			ids = append(ids, id[len(testServer)+2:])
		}
		token = resp.GetNextToken()
		if between != nil {
			between(page)
		}
		if token == "" {
			return ids
		}
		require.Less(t, page, 10, "paging did not terminate")
	}
}

func TestListSnapshotsTokenSurvivesADeleteBetweenPages(t *testing.T) {
	onArray := []qumulo.SnapshotInfo{
		snapshot("1"), snapshot("2"), snapshot("3"), snapshot("4"), snapshot("5"),
	}
	srv := newTestControllerServer(t, snapshotsOnArray(&onArray))

	// Snapshot 1, already returned, is deleted after the first page. With an
	// offset token the second page started at index 2 of a four-entry list
	// and snapshot 3 was never reported.
	ids := listedIDs(t, srv, 2, func(afterPage int) {
		if afterPage == 0 {
			onArray = onArray[1:]
		}
	})

	assert.Equal(t, []string{"1", "2", "3", "4", "5"}, ids,
		"no snapshot may be skipped because an earlier one was deleted")
}

// The anchor itself going is the harder case: the token names a snapshot that
// no longer exists, and the listing has to resume at the next one rather than
// refusing or starting again.
func TestListSnapshotsTokenSurvivesTheAnchorBeingDeleted(t *testing.T) {
	onArray := []qumulo.SnapshotInfo{
		snapshot("1"), snapshot("2"), snapshot("3"), snapshot("4"),
	}
	srv := newTestControllerServer(t, snapshotsOnArray(&onArray))

	first, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		Secrets:        testSecrets(),
		SourceVolumeId: allSnapshotsSource,
		MaxEntries:     2,
	})
	require.NoError(t, err)
	require.Equal(t, "3", first.GetNextToken())

	// Snapshot 3 -- the one the token names -- is deleted.
	onArray = []qumulo.SnapshotInfo{snapshot("1"), snapshot("2"), snapshot("4")}

	second, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		Secrets:        testSecrets(),
		SourceVolumeId: allSnapshotsSource,
		MaxEntries:     2,
		StartingToken:  first.GetNextToken(),
	})
	require.NoError(t, err)
	require.Len(t, second.GetEntries(), 1)
	assert.Contains(t, second.GetEntries()[0].GetSnapshot().GetSnapshotId(), "4")
	assert.Empty(t, second.GetNextToken())
}

// A snapshot added between the pages is not a reason to repeat one already
// returned: the token is an ID, so a new snapshot with a higher ID simply
// arrives on a later page.
func TestListSnapshotsTokenDoesNotRepeatAfterAnInsert(t *testing.T) {
	onArray := []qumulo.SnapshotInfo{snapshot("1"), snapshot("2"), snapshot("3")}
	srv := newTestControllerServer(t, snapshotsOnArray(&onArray))

	ids := listedIDs(t, srv, 2, func(afterPage int) {
		if afterPage == 0 {
			onArray = append(onArray, snapshot("9"))
		}
	})

	assert.Equal(t, []string{"1", "2", "3", "9"}, ids)
}

// The cluster's listing order is not promised, and the token's meaning
// depends on one. IDs are numbers, so they are compared as numbers: a text
// sort would put "10" before "9" and cut the list somewhere the next page
// does not continue from.
func TestListSnapshotsPagesInNumericIDOrder(t *testing.T) {
	onArray := []qumulo.SnapshotInfo{
		snapshot("10"), snapshot("2"), snapshot("9"), snapshot("1"),
	}
	srv := newTestControllerServer(t, snapshotsOnArray(&onArray))

	ids := listedIDs(t, srv, 2, nil)

	assert.Equal(t, []string{"1", "2", "9", "10"}, ids)
}
