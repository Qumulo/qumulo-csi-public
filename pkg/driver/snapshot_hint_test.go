// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// snapshotArray is a MockClient holding a snapshot table, counting listings.
func snapshotArray() (*MockClient, *[]qumulo.SnapshotInfo, *int) {
	snaps := &[]qumulo.SnapshotInfo{}
	listings := new(int)
	next := 40
	m := &MockClient{
		ListSnapshotsFunc: func(context.Context) ([]qumulo.SnapshotInfo, error) {
			*listings++
			return append([]qumulo.SnapshotInfo(nil), *snaps...), nil
		},
		GetSnapshotFunc: func(_ context.Context, id string) (qumulo.SnapshotInfo, error) {
			for _, s := range *snaps {
				if s.ID == id {
					return s, nil
				}
			}
			return qumulo.SnapshotInfo{}, fakeAPIError(http.StatusNotFound, "fs_no_such_snapshot_error", "no snapshot "+id)
		},
		CreateSnapshotFunc: func(_ context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			next++
			id := strconv.Itoa(next)
			s := qumulo.SnapshotInfo{ID: id, Name: id + "_" + name, SourceFileID: fileID}
			*snaps = append(*snaps, s)
			return s, nil
		},
	}
	return m, snaps, listings
}

func newNamedControllerServer(t *testing.T, m *MockClient) *ControllerServer {
	t.Helper()
	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "0.1.0",
		NewClient: func(string, string) qumulo.ClientInterface {
			return namedMock{MockClient: m, endpoint: "https://hinted.example.com:8000"}
		},
	})
	require.NoError(t, err)
	return NewControllerServer(d)
}

// A retried CreateSnapshot listed every snapshot on the array to find
// the one its first attempt took. That one is now remembered and read back
// by ID while it is still adoptable.
func TestARetriedCreateSnapshotReadsTheSnapshotByID(t *testing.T) {
	m, snaps, listings := snapshotArray()
	srv := newNamedControllerServer(t, m)
	snapshot := func(name string) string {
		t.Helper()
		resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
			Secrets: testSecrets(), Name: name, SourceVolumeId: testHandle("pvc-abc"),
		})
		require.NoError(t, err)
		return resp.GetSnapshot().GetSnapshotId()
	}

	first := snapshot("snap-h")
	assert.Equal(t, 1, *listings, "a name not seen before is listed for")
	again := snapshot("snap-h")
	assert.Equal(t, first, again, "the retry adopts the snapshot")
	assert.Equal(t, 1, *listings, "and reads it by ID")
	assert.Len(t, *snaps, 1)

	// Being deleted: not adoptable, so the hint is not believed.
	(*snaps)[0].InDelete = true
	third := snapshot("snap-h")
	assert.Equal(t, 2, *listings, "a hint naming a snapshot in delete falls back to the listing")
	assert.NotEqual(t, first, third, "and a new snapshot is taken")

	// Gone: likewise.
	*snaps = nil
	snapshot("snap-h")
	assert.Equal(t, 3, *listings)
}

// The listing remembers what it found too: a retry after a restart lists
// once, then reads by ID.
func TestASnapshotFoundByListingIsReadByIDAfterwards(t *testing.T) {
	m, snaps, listings := snapshotArray()
	*snaps = []qumulo.SnapshotInfo{{ID: "7", Name: "7_snap-old", SourceFileID: testFileID}}
	srv := newNamedControllerServer(t, m)
	for i := 0; i < 3; i++ {
		resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
			Secrets: testSecrets(), Name: "snap-old", SourceVolumeId: testHandle("pvc-abc"),
		})
		require.NoError(t, err)
		assert.Equal(t, testServer+"##7", resp.GetSnapshot().GetSnapshotId())
	}
	assert.Equal(t, 1, *listings)
}

// afterListing wraps one fake array so a test can change the fabric right
// after that array's snapshot table is listed: mid-drain, between polls.
type afterListing struct {
	*fakeArray
	n    int
	then func(n int)
}

func (a *afterListing) ListSnapshots(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
	out, err := a.fakeArray.ListSnapshots(ctx)
	a.n++
	a.then(a.n)
	return out, err
}

// onTargetListing installs then on array B's snapshot listings.
func onTargetListing(s *replScenario, then func(n int)) {
	orig := s.driver.NewClient
	s.driver.NewClient = func(baseURL, token string) qumulo.ClientInterface {
		c := orig(baseURL, token)
		if strings.Contains(baseURL, "//"+arrayB+":") {
			return &afterListing{fakeArray: c.(*fakeArray), then: then}
		}
		return c
	}
}

// gateScenario is TestPromotePlannedSnapshotGate's: a policy snapshot on the
// source newer than anything on the target, so the drain waits for it.
func gateScenario(t *testing.T) (*replScenario, *fakeRel, time.Time) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	newer := time.Now().Add(-time.Minute)
	s.a.snapshots = []qumulo.SnapshotInfo{
		{ID: "10", SourceFileID: s.fileIDA, CreatedAt: newer.Add(-time.Hour), PolicyID: "7"},
		{ID: "11", SourceFileID: s.fileIDA, CreatedAt: newer, PolicyID: "7"},
	}
	s.b.snapshots = []qumulo.SnapshotInfo{
		{ID: "2", SourceFileID: s.b.dirs[replVolumePath], CreatedAt: newer.Add(-time.Hour)},
	}
	return s, rel, newer
}

// Once the queue was empty, each drain poll listed the whole snapshot
// table on both arrays until the target held every policy snapshot. The
// source's is now listed once per drain.
func TestTheDrainListsTheSourceSnapshotsOncePerDrain(t *testing.T) {
	s, rel, _ := gateScenario(t)
	a0, b0 := s.a.snapshotListings, s.b.snapshotListings

	requireCode(t, s.promote(t, s.handleB(t), classToA(), false), codes.Aborted)

	polls := s.b.snapshotListings - b0
	require.Greater(t, polls, 3, "the drain polled several times before its budget ran out")
	assert.Equal(t, 2, s.a.snapshotListings-a0, "source: one to find no marker, one for the whole drain")
	assert.True(t, rel.targetRO, "a copy without its restore points is still not promoted")
}

// The snapshot crosses mid-drain: seen on the next poll, and the source is
// not listed again for it.
func TestTheDrainSeesASnapshotCrossMidDrain(t *testing.T) {
	s, rel, newer := gateScenario(t)
	onTargetListing(s, func(n int) {
		if n == 3 {
			s.b.snapshots = append(s.b.snapshots, qumulo.SnapshotInfo{
				ID: "3", SourceFileID: s.b.dirs[replVolumePath], CreatedAt: newer,
			})
		}
	})
	a0 := s.a.snapshotListings

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	assert.False(t, rel.targetRO)
	assert.Equal(t, 2, s.a.snapshotListings-a0)
}

// A snapshot still missing that expires on the source mid-drain is read
// back by ID, found gone, and no longer waited for.
func TestTheDrainStopsWaitingForASnapshotDeletedMidDrain(t *testing.T) {
	s, rel, _ := gateScenario(t)
	onTargetListing(s, func(n int) {
		if n == 2 {
			s.a.snapshots = s.a.snapshots[:1] // 11 expired
		}
	})

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	assert.False(t, rel.targetRO, "the promote completes once the only missing snapshot is gone")
}

// Snapshots queued again after the first check: the source is re-listed
// once they have crossed, so one taken meanwhile is gated on too.
func TestTheDrainRelistsTheSourceAfterTheQueueRefills(t *testing.T) {
	s, rel, newer := gateScenario(t)
	onTargetListing(s, func(n int) {
		if n == 1 {
			rel.queuedPolls = 2
			s.a.snapshots = append(s.a.snapshots, qumulo.SnapshotInfo{
				ID: "12", SourceFileID: s.fileIDA, CreatedAt: newer.Add(time.Second), PolicyID: "7",
			})
		}
	})
	a0 := s.a.snapshotListings

	requireCode(t, s.promote(t, s.handleB(t), classToA(), false), codes.Aborted)

	assert.Equal(t, 3, s.a.snapshotListings-a0, "marker, first check, and once more after the queue refilled")
	assert.True(t, rel.targetRO)
}
