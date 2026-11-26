// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// Two RPCs read the cluster, decided from what they read and then
// acted, with nothing holding the two steps together. Both races are
// reproducible with the mock alone, because the window is the driver's own.
//
// Run these under -race: the lock is also what makes the bookkeeping between
// the two steps safe to touch from two goroutines.

// TestTwoCreateSnapshotsOfOneNameTakeOneSnapshot pins the CreateSnapshot
// race. The second call used to list snapshots while the first was still
// inside its create, find none, and take a second snapshot of the same
// volume. Only one ID reaches the VolumeSnapshotContent, so the other was
// left on the array for good.
func TestTwoCreateSnapshotsOfOneNameTakeOneSnapshot(t *testing.T) {
	var created atomic.Int64
	var mu sync.Mutex
	var onArray []qumulo.SnapshotInfo

	client := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
			mu.Lock()
			defer mu.Unlock()
			return append([]qumulo.SnapshotInfo(nil), onArray...), nil
		},
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			// The window: the array has not recorded the snapshot yet, and
			// a second call listing now sees nothing.
			time.Sleep(20 * time.Millisecond)

			snapshot := qumulo.SnapshotInfo{
				ID:           "4" + string(rune('0'+created.Add(1))),
				Name:         "41_" + name,
				SourceFileID: fileID,
			}
			mu.Lock()
			onArray = append(onArray, snapshot)
			mu.Unlock()
			return snapshot, nil
		},
	}
	srv := newTestControllerServer(t, client)

	var wg sync.WaitGroup
	ids := make([]string, 2)
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
				Secrets:        testSecrets(),
				Name:           "snapshot-snap-1",
				SourceVolumeId: testHandle("pvc-abc"),
			})
			assert.NoError(t, err)
			ids[i] = resp.GetSnapshot().GetSnapshotId()
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), created.Load(), "one name must leave one snapshot on the array")
	assert.Equal(t, ids[0], ids[1], "both calls must answer with the same snapshot")
}

// TestTwoExpandsOfOneVolumeDoNotLoseTheLargerSize pins the expand race. Both
// calls read the old quota, both decided their own size was a grow, and the
// one that wrote last won -- so a volume the CO believed was at 20 GiB was
// enforced at 15.
//
// The interleaving is scripted with channels rather than left to the
// scheduler, so that without the per-volume lock it loses the larger size on
// every run, not on some: the 20 GiB call (A) starts first and reads;
// only then does the 15 GiB call (B) start; A's write waits for B's read, and
// B's write waits for A's. With the lock B cannot read until A has released,
// so A's wait for B's read gives up after a short grace and A writes alone;
// B then reads 20 GiB and leaves it.
func TestTwoExpandsOfOneVolumeDoNotLoseTheLargerSize(t *testing.T) {
	const grace = 200 * time.Millisecond
	var mu sync.Mutex
	quota := uint64(testGiB)

	var reads atomic.Int32
	aRead := make(chan struct{})  // A has read the quota
	bRead := make(chan struct{})  // B has read the quota
	aWrote := make(chan struct{}) // A has written its size

	client := &MockClient{
		LookupFileByPathFunc: volumeAt(testStoreRealPath+"/pvc-abc", testFileID),
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) {
			mu.Lock()
			got := quota
			mu.Unlock()
			switch reads.Add(1) {
			case 1:
				close(aRead)
			case 2:
				close(bRead)
			}
			return got, nil
		},
		UpdateQuotaFunc: func(ctx context.Context, fileID string, limitBytes uint64) error {
			switch limitBytes {
			case uint64(20 * testGiB): // A: write only once B has read too
				select {
				case <-bRead:
				case <-time.After(grace): // the lock is holding B back
				}
				defer close(aWrote)
			case uint64(15 * testGiB): // B: write only after A
				<-aWrote
			}
			mu.Lock()
			defer mu.Unlock()
			quota = limitBytes
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	expand := func(size int64, wg *sync.WaitGroup) {
		defer wg.Done()
		_, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			Secrets:       testSecrets(),
			VolumeId:      testHandle("pvc-abc"),
			CapacityRange: &csi.CapacityRange{RequiredBytes: size},
		})
		assert.NoError(t, err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go expand(20*testGiB, &wg)
	<-aRead // A holds the lock, if there is one, from here until it writes
	go expand(15*testGiB, &wg)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, uint64(20*testGiB), quota,
		"the smaller expand must find the volume already larger and leave it alone")
}

// A caller whose context ends while it waits for the key gets Aborted, which
// is the spec's code for another operation in progress: the CO retries. It
// must not be left parked in an uncancellable wait.
func TestAnRPCWaitingForABusyVolumeAnswersAbortedWhenItsContextEnds(t *testing.T) {
	locks := newKeyedLocks()
	release, err := locks.acquire(context.Background(), "pvc-abc")
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = locks.acquire(ctx, "pvc-abc")
	assertCode(t, err, codes.Aborted)
}

// Different keys do not wait on each other: two volumes expand at once.
func TestKeyedLocksDoNotSerialiseDifferentKeys(t *testing.T) {
	locks := newKeyedLocks()

	first, err := locks.acquire(context.Background(), "pvc-a")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, err := locks.acquire(ctx, "pvc-b")
	require.NoError(t, err)

	first()
	second()
	assert.False(t, locks.held("pvc-a"))
	assert.False(t, locks.held("pvc-b"))
}

// Releasing wakes a waiter, and the key is free again afterwards: a lock that
// leaked would wedge every later call for that volume.
func TestKeyedLocksHandOver(t *testing.T) {
	locks := newKeyedLocks()

	release, err := locks.acquire(context.Background(), "pvc-abc")
	require.NoError(t, err)

	took := make(chan func(), 1)
	go func() {
		next, err := locks.acquire(context.Background(), "pvc-abc")
		assert.NoError(t, err)
		took <- next
	}()

	release()

	select {
	case next := <-took:
		next()
	case <-time.After(2 * time.Second):
		t.Fatal("a waiter was not woken when the key was released")
	}

	assert.False(t, locks.held("pvc-abc"), "the key must be free once everyone has let go")
}
