// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// The tests for abandoned restore staging: a restore abandoned mid-copy left its staging
// directory on the array for good. DeleteVolume now reclaims a gone volume's
// own staging sibling, and a successful CreateVolume sweeps its store root.

// abandonedUID is the claim UID of the abandoned restore these tests sweep.
const abandonedUID = "d2b75204-be29-4854-b02b-4f5be48858d6"

// abandonedVolume is the volume name csi-provisioner gave that claim.
const abandonedVolume = "pvc-" + abandonedUID

// abandonedStaging is its staging directory under the test store root.
const abandonedStaging = testStoreRealPath + "/.csi-restoring-" + abandonedVolume

// stubPVCLister answers ListPersistentVolumeClaimUIDs from a slice a test
// edits between calls.
type stubPVCLister struct {
	mu    sync.Mutex
	uids  []string
	err   error
	calls int
}

func (l *stubPVCLister) ListPersistentVolumeClaimUIDs(context.Context) ([]string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.err != nil {
		return nil, l.err
	}
	return append([]string(nil), l.uids...), nil
}

// stagingObserver counts the restore staging events, and ignores the rest.
type stagingObserver struct {
	nopObserver
	mu     sync.Mutex
	events []qumulo.RestoreStagingEvent
}

func (o *stagingObserver) ObserveRestoreStaging(_ context.Context, e qumulo.RestoreStagingEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
}

func (o *stagingObserver) count(state string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, e := range o.events {
		if e.State == state {
			n++
		}
	}
	return n
}

// stagingStore is a store root holding staging directories, as a MockClient:
// it lists them, resolves them by path, and records what was removed.
type stagingStore struct {
	mu       sync.Mutex
	entries  map[string]string // staging path -> file ID
	listings int
	listErr  error
	removed  []string // file IDs tree-deleted
	quotas   []string // file IDs whose quota was deleted
	// usage is what each file ID's tree holds; absent reads as empty.
	usage    map[string]qumulo.TreeUsage
	usageErr error
}

// write makes the tree with file ID id grow, as a copy into it does.
func (st *stagingStore) write(id string, bytes uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	u := st.usage[id]
	u.Capacity += bytes
	u.Files++
	st.usage[id] = u
}

func newStagingStore(paths ...string) *stagingStore {
	st := &stagingStore{entries: map[string]string{}, usage: map[string]qumulo.TreeUsage{}}
	for i, p := range paths {
		st.entries[p] = "70" + string(rune('0'+i))
	}
	return st
}

func (st *stagingStore) client() *MockClient {
	return &MockClient{
		CreateDirectoryFunc: func(_ context.Context, parent, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY", Path: parent + "/" + name}, nil
		},
		ListDirectoryFunc: func(_ context.Context, p, snapshotID string) ([]qumulo.DirEntry, error) {
			st.mu.Lock()
			defer st.mu.Unlock()
			st.listings++
			if st.listErr != nil {
				return nil, st.listErr
			}
			var out []qumulo.DirEntry
			for staging, id := range st.entries {
				if path.Dir(staging) == p {
					out = append(out, qumulo.DirEntry{Name: path.Base(staging), Path: staging, ID: id, Type: "FS_FILE_TYPE_DIRECTORY"})
				}
			}
			// A volume beside them, which the sweep must not touch.
			out = append(out, qumulo.DirEntry{Name: "pvc-live", ID: "11", Type: "FS_FILE_TYPE_DIRECTORY"})
			return out, nil
		},
		LookupFileByPathFunc: func(_ context.Context, p string) (qumulo.FileInfo, error) {
			st.mu.Lock()
			defer st.mu.Unlock()
			if id, ok := st.entries[p]; ok {
				return qumulo.FileInfo{ID: id, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			}
			if isRestoreStaging(path.Base(p)) {
				return qumulo.FileInfo{}, fakeAPIError(http.StatusNotFound, qumulo.ErrorClassNoSuchEntry, "no such path")
			}
			return qumulo.FileInfo{ID: testFileID, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetTreeUsageFunc: func(_ context.Context, id, snapshotID string) (qumulo.TreeUsage, error) {
			st.mu.Lock()
			defer st.mu.Unlock()
			if st.usageErr != nil {
				return qumulo.TreeUsage{}, st.usageErr
			}
			return st.usage[id], nil
		},
		DeleteQuotaFunc: func(_ context.Context, id string) error {
			st.mu.Lock()
			defer st.mu.Unlock()
			st.quotas = append(st.quotas, id)
			return nil
		},
		StartTreeDeleteFunc: func(_ context.Context, id string) error {
			st.mu.Lock()
			defer st.mu.Unlock()
			st.removed = append(st.removed, id)
			for p, other := range st.entries {
				if other == id {
					delete(st.entries, p)
				}
			}
			return nil
		},
	}
}

func (st *stagingStore) removedIDs() []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]string(nil), st.removed...)
}

func (st *stagingStore) listCount() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.listings
}

// sweepFixture is a controller in a cluster -- both listers wired -- whose
// sweeps run inline, on a clock the test moves.
type sweepFixture struct {
	srv      *ControllerServer
	store    *stagingStore
	pvs      *stubPVLister
	pvcs     *stubPVCLister
	observer *stagingObserver
	now      time.Time
}

func newSweepFixture(t *testing.T, store *stagingStore) *sweepFixture {
	t.Helper()
	f := &sweepFixture{
		store:    store,
		pvs:      &stubPVLister{},
		pvcs:     &stubPVCLister{},
		observer: &stagingObserver{},
		now:      time.Date(2026, 10, 9, 0, 49, 0, 0, time.UTC),
	}
	client := store.client()
	d, err := NewDriver(Config{
		Name:                        "csi.qumulo.com",
		Version:                     "0.1.0",
		NewClient:                   func(string, string) qumulo.ClientInterface { return client },
		PersistentVolumeLister:      f.pvs,
		PersistentVolumeClaimLister: f.pvcs,
		Observer:                    f.observer,
	})
	require.NoError(t, err)
	f.srv = NewControllerServer(d)
	f.srv.stagingSweeper.now = func() time.Time { return f.now }
	f.srv.stagingSweeper.run = func(sweep func()) { sweep() }
	return f
}

// create provisions an ordinary volume, which is what runs the sweep.
func (f *sweepFixture) create(t *testing.T, name string, params map[string]string) {
	t.Helper()
	_, err := f.srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               name,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         params,
	})
	require.NoError(t, err)
}

// sweepAt advances the clock by d and provisions a volume.
func (f *sweepFixture) sweepAt(t *testing.T, d time.Duration) {
	t.Helper()
	f.now = f.now.Add(d)
	f.create(t, "pvc-new", testParams())
}

// checkSweepRemovesAbandonedStagingOnTheSecondStrike is the sweep: a
// staging directory whose claim and PV are gone is found by one sweep and
// removed by a later one at least 15 minutes after -- quota first, then the
// tree -- and only then.
func checkSweepRemovesAbandonedStagingOnTheSecondStrike(t *testing.T) {
	t.Helper()
	f := newSweepFixture(t, newStagingStore(abandonedStaging))

	f.sweepAt(t, 0)
	assert.Empty(t, f.store.removedIDs(), "the first sweep to find it must not remove it")
	assert.Equal(t, 1, f.observer.count(qumulo.RestoreStagingAbandoned))

	f.sweepAt(t, 10*time.Minute)
	assert.Empty(t, f.store.removedIDs(), "10 minutes after the first strike is too soon")

	f.sweepAt(t, 10*time.Minute)
	assert.Equal(t, []string{"700"}, f.store.removedIDs(), "20 minutes after the first strike it is removed")
	assert.Equal(t, []string{"700"}, f.store.quotas, "its quota goes first")
	assert.Equal(t, 1, f.observer.count(qumulo.RestoreStagingAbandoned), "abandoned is counted once")
	assert.Equal(t, 1, f.observer.count(qumulo.RestoreStagingRemoved))
}

func TestSweepRemovesAbandonedStagingOnlyOnTheSecondStrike(t *testing.T) {
	checkSweepRemovesAbandonedStagingOnTheSecondStrike(t)
}

// sweepsNeverRemove runs sweeps 0, 10 and 20 minutes apart and asserts none
// removed anything.
func sweepsNeverRemove(t *testing.T, f *sweepFixture, why string) {
	t.Helper()
	for _, d := range []time.Duration{0, 10 * time.Minute, 10 * time.Minute} {
		f.sweepAt(t, d)
	}
	assert.Empty(t, f.store.removedIDs(), why)
	assert.Empty(t, f.store.quotas, why)
	assert.Zero(t, f.observer.count(qumulo.RestoreStagingRemoved), why)
}

func TestSweepKeepsStagingWhileItsClaimExists(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.pvcs.uids = []string{"0b0e1d62-0000-4000-8000-000000000001", abandonedUID}
	sweepsNeverRemove(t, f, "a claim with the staging directory's UID exists: its restore may still be running")
	assert.Zero(t, f.observer.count(qumulo.RestoreStagingAbandoned))
}

func TestSweepKeepsStagingWhileItsPVExists(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.pvs.set(boundPV(abandonedVolume, testHandle(abandonedVolume), "apps", "data"))
	sweepsNeverRemove(t, f, "a PV with the volume's name exists; DeleteVolume reclaims its sibling")
}

func TestSweepKeepsStagingWhileThisControllerIsRestoringIt(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	release, err := f.srv.restores.acquire(abandonedVolume)
	require.NoError(t, err)
	defer release()
	sweepsNeverRemove(t, f, "a restore of that name is running on this controller")
}

// An earlier attempt's progress memo does not hold a staging directory whose
// claim is gone, and this test replaces one that asserted it did.
//
// The memo is kept restoreProgressTTL -- an hour -- so that an attempt the CO
// retries resumes rather than starting again. The sweep reaches this point
// only once the claim is gone and no PV carries the name, and
// csi-provisioner sends no further CreateVolume for a deleted claim, so that
// retry cannot arrive. Counting the memo here held every abandoned directory
// for the whole hour before its 15 minutes could even begin -- and held it
// silently, because the strike was cleared above the log and the metric that
// report one. A restore actually running is still protected, by the lock:
// TestSweepKeepsStagingWhileThisControllerIsRestoringIt.
func TestSweepRemovesAbandonedStagingThoughAnEarlierAttemptHeldProgress(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.srv.restoreProgress.now = func() time.Time { return f.now }
	key := restoreProgressKey(apiURLFor(testServer, ""), abandonedStaging, "42")
	f.srv.restoreProgress.take(key, "700")

	f.sweepAt(t, 0)
	assert.Empty(t, f.store.removedIDs(), "the first sweep to find it still must not remove it")
	assert.Equal(t, 1, f.observer.count(qumulo.RestoreStagingAbandoned),
		"a leftover is reported by the sweep that first finds it, not an hour later")

	f.sweepAt(t, 10*time.Minute)
	assert.Empty(t, f.store.removedIDs(), "10 minutes after the first strike is too soon")

	f.sweepAt(t, 10*time.Minute)
	assert.Equal(t, []string{"700"}, f.store.removedIDs(),
		"20 minutes after the first strike it is removed, memo or no memo")
	assert.Equal(t, 1, f.observer.count(qumulo.RestoreStagingRemoved))
}

// The memo is still what DeleteVolume's reclaim consults, and that path is
// deliberately unchanged: it checks the claim *after* the progress, so the
// memo is its only guard against a retry still to come.
func TestDeleteVolumeOfAGoneVolumeKeepsStagingWhileItHoldsProgress(t *testing.T) {
	store := deleteGoneVolumeBesideStaging(t, &stubPVCLister{}, func(srv *ControllerServer) {
		key := restoreProgressKey(apiURLFor(testServer, ""), abandonedStaging, "42")
		srv.restoreProgress.take(key, "700")
	})
	assert.Empty(t, store.removedIDs(), "an earlier attempt's progress is held: the CO may retry it")
	assert.Empty(t, store.quotas)
}

func TestSweepLeavesStagingNotNamedAfterAClaim(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(testStoreRealPath+"/.csi-restoring-data", testStoreRealPath+"/.csi-restoring-pvc-abc"))
	sweepsNeverRemove(t, f, "a name with no claim UID cannot be judged")
	assert.Zero(t, f.observer.count(qumulo.RestoreStagingAbandoned))
}

func TestSweepRemovesNothingWhenTheClaimListFails(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.pvcs.err = errors.New("persistentvolumeclaims is forbidden")
	sweepsNeverRemove(t, f, "a failed claim list must read as 'cannot tell', not 'no claims'")
}

func TestSweepRemovesNothingWhenThePVListFails(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.pvs.err = errors.New("persistentvolumes is forbidden")
	sweepsNeverRemove(t, f, "a failed PV list must read as 'cannot tell', not 'no PVs'")
}

func TestSweepRemovesNothingWhenTheStoreRootListFails(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.store.listErr = apiError(http.StatusServiceUnavailable)
	sweepsNeverRemove(t, f, "a store root that did not list is not swept")
	assert.Zero(t, f.pvcs.calls, "nothing to judge, so Kubernetes is not asked")
}

// A store root with no staging directory costs one listing and no
// Kubernetes call.
func TestSweepOfAStoreRootWithNoStagingAsksKubernetesNothing(t *testing.T) {
	f := newSweepFixture(t, newStagingStore())
	f.sweepAt(t, 0)
	assert.Equal(t, 1, f.store.listCount())
	assert.Zero(t, f.pvcs.calls)
	assert.Zero(t, f.pvs.calls)
}

// A directory removed and made again between two sweeps is a new one: the
// clock starts again.
func TestSweepStartsAgainForAStagingDirectoryMadeAgain(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.sweepAt(t, 0)
	f.store.mu.Lock()
	f.store.entries[abandonedStaging] = "799"
	f.store.mu.Unlock()
	f.sweepAt(t, 10*time.Minute)
	f.sweepAt(t, 10*time.Minute)
	assert.Empty(t, f.store.removedIDs(), "the new directory was first seen 10 minutes ago")
	f.sweepAt(t, 10*time.Minute)
	assert.Equal(t, []string{"799"}, f.store.removedIDs())
}

// A strike is cleared once its claim reappears, so a later disappearance
// starts the 15 minutes again.
func TestSweepForgetsTheStrikeOfAStagingDirectoryWhoseClaimAppears(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.sweepAt(t, 0)
	f.pvcs.uids = []string{abandonedUID}
	f.sweepAt(t, 10*time.Minute)
	f.pvcs.uids = nil
	f.sweepAt(t, 10*time.Minute)
	assert.Empty(t, f.store.removedIDs(), "first claimless again only now")
	assert.Equal(t, 2, f.observer.count(qumulo.RestoreStagingAbandoned))
}

// At most one sweep per array and store root every 10 minutes, however many
// volumes are created; another store root is swept on its own schedule.
func TestSweepRunsAtMostOncePerStoreRootEveryTenMinutes(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))

	f.create(t, "pvc-1", testParams())
	f.create(t, "pvc-2", testParams())
	f.now = f.now.Add(9 * time.Minute)
	f.create(t, "pvc-3", testParams())
	assert.Equal(t, 1, f.store.listCount(), "three creates within 10 minutes sweep once")

	other := testParams()
	other["adDomain"] = "corp.example.com" // a store root of its own: /csi-volumes/corp.example.com
	f.create(t, "pvc-4", other)
	assert.Equal(t, 2, f.store.listCount(), "another store root is swept on its own schedule")

	f.now = f.now.Add(time.Minute)
	f.create(t, "pvc-5", testParams())
	assert.Equal(t, 3, f.store.listCount(), "10 minutes after the first sweep, the root is due again")
}

// A staging directory still being written is a restore alive somewhere --
// on another Kubernetes cluster sharing this store root, whose claim this
// cluster cannot see -- and is never removed while it keeps changing. Once
// nothing has written to it for 15 minutes it goes.
func TestSweepKeepsStagingThatIsStillBeingWritten(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.sweepAt(t, 0)
	for range 6 {
		f.store.write("700", 32<<20)
		f.sweepAt(t, 10*time.Minute)
	}
	assert.Empty(t, f.store.removedIDs(), "a restore copying into it for an hour must not be removed")
	assert.Equal(t, 1, f.observer.count(qumulo.RestoreStagingAbandoned), "a change restarts the strike, it is not a new find")

	f.sweepAt(t, 10*time.Minute)
	assert.Empty(t, f.store.removedIDs(), "unchanged for only 10 minutes")
	f.sweepAt(t, 10*time.Minute)
	assert.Equal(t, []string{"700"}, f.store.removedIDs(), "unchanged for 20 minutes: removed")
}

// A sweep that cannot read what the directory holds cannot tell a live copy
// from a dead one, so it removes nothing -- and keeps the strike, so a later
// read that finds it unchanged can still remove it.
func TestSweepRemovesNothingWhenItCannotReadTheStagingContent(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.sweepAt(t, 0)
	f.store.usageErr = apiError(http.StatusForbidden)
	f.sweepAt(t, 10*time.Minute)
	f.sweepAt(t, 10*time.Minute)
	assert.Empty(t, f.store.removedIDs(), "no reading, no removal")
	f.store.usageErr = nil
	f.sweepAt(t, 10*time.Minute)
	assert.Equal(t, []string{"700"}, f.store.removedIDs(), "the strike was kept; unchanged since it was recorded")
}

// Nothing is struck on a first sighting whose content cannot be read.
func TestSweepStrikesNothingItCannotRead(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	f.store.usageErr = apiError(http.StatusServiceUnavailable)
	sweepsNeverRemove(t, f, "no reading, no strike")
	assert.Zero(t, f.observer.count(qumulo.RestoreStagingAbandoned))
}

// An adDomain class's store root lies below another class's. Sweeping the
// outer root must not forget what a sweep of the inner one found.
func TestSweepOfAnOuterRootKeepsTheStrikesOfAnInnerOne(t *testing.T) {
	inner := testStoreRealPath + "/corp.example.com/.csi-restoring-" + abandonedVolume
	f := newSweepFixture(t, newStagingStore(inner))
	domain := testParams()
	domain["adDomain"] = "corp.example.com"

	f.create(t, "pvc-1", domain)
	f.create(t, "pvc-2", testParams())
	f.now = f.now.Add(10 * time.Minute)
	f.create(t, "pvc-3", testParams())
	f.create(t, "pvc-4", domain)
	assert.Empty(t, f.store.removedIDs())
	f.now = f.now.Add(10 * time.Minute)
	f.create(t, "pvc-5", domain)
	assert.Equal(t, []string{"700"}, f.store.removedIDs(), "first found 20 minutes ago by the inner root's sweep")
}

// Outside a cluster nothing can tell an abandoned restore from a running
// one, so there is no sweep at all.
func TestNoSweepWithoutTheKubernetesListers(t *testing.T) {
	store := newStagingStore(abandonedStaging)
	srv := newTestControllerServer(t, store.client())
	srv.stagingSweeper.run = func(sweep func()) { sweep() }
	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets: testSecrets(), Name: "pvc-new", CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(), Parameters: testParams(),
	})
	require.NoError(t, err)
	assert.Zero(t, store.listCount())
}

// A failed CreateVolume does not sweep.
func TestNoSweepAfterAFailedCreateVolume(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	client := f.store.client()
	client.CreateQuotaFunc = func(context.Context, string, uint64) error { return apiError(http.StatusForbidden) }
	f.srv.driver.NewClient = func(string, string) qumulo.ClientInterface { return client }
	_, err := f.srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets: testSecrets(), Name: "pvc-new", CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(), Parameters: testParams(),
	})
	require.Error(t, err)
	assert.Zero(t, f.store.listCount())
}

// The sweep runs off the request: by default on its own goroutine, after
// CreateVolume has answered, on a context the request's end does not cancel.
func TestSweepRunsAfterCreateVolumeHasAnswered(t *testing.T) {
	f := newSweepFixture(t, newStagingStore(abandonedStaging))
	started := make(chan context.Context, 1)
	f.srv.stagingSweeper.run = nil
	client := f.store.client()
	inner := client.ListDirectoryFunc
	client.ListDirectoryFunc = func(ctx context.Context, p, snap string) ([]qumulo.DirEntry, error) {
		started <- ctx
		return inner(ctx, p, snap)
	}
	f.srv.driver.NewClient = func(string, string) qumulo.ClientInterface { return client }

	ctx, cancel := context.WithCancel(context.Background())
	_, err := f.srv.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Secrets: testSecrets(), Name: "pvc-new", CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(), Parameters: testParams(),
	})
	cancel()
	require.NoError(t, err)

	select {
	case sctx := <-started:
		deadline, ok := sctx.Deadline()
		require.True(t, ok, "the sweep's listing is bounded")
		assert.LessOrEqual(t, time.Until(deadline), restoreStagingListBudget)
	case <-time.After(5 * time.Second):
		t.Fatal("the sweep never ran")
	}
}

// checkDeleteVolumeReclaimsTheStagingOfAGoneVolume is the DeleteVolume half:
// the volume's final path was never created, only its staging sibling was,
// and DeleteVolume removes that sibling -- quota, then tree -- and nothing
// else, against the real client and the fake cluster.
func checkDeleteVolumeReclaimsTheStagingOfAGoneVolume(t *testing.T, pvcs PersistentVolumeClaimLister) {
	t.Helper()
	l := storeWithTwoVolumes()
	l.directories[abandonedStaging] = "13"
	l.quotas["13"] = "1073741824"
	server := httptest.NewServer(l.handler())
	defer server.Close()
	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		NewClient: func(_, token string) qumulo.ClientInterface {
			return qumulo.New(server.URL, token)
		},
		PersistentVolumeClaimLister: pvcs,
	})
	require.NoError(t, err)

	_, err = NewControllerServer(d).DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: testHandle(abandonedVolume), Secrets: testSecrets()})

	require.NoError(t, err)
	l.mu.Lock()
	defer l.mu.Unlock()
	assert.Equal(t, []string{"13"}, l.quotaDels, "the staging quota is removed")
	require.Len(t, l.treeDeletes, 1, "the staging directory is tree-deleted, and nothing else")
	assert.Contains(t, l.treeDeletes[0], `"13"`)
	assert.NotContains(t, l.directories, abandonedStaging)
	for _, p := range []string{"/csi-volumes", "/csi-volumes/pvc-mine", "/csi-volumes/pvc-other"} {
		assert.Contains(t, l.directories, p)
	}
}

func TestDeleteVolumeReclaimsTheStagingOfAGoneVolume(t *testing.T) {
	t.Run("claim gone", func(t *testing.T) {
		checkDeleteVolumeReclaimsTheStagingOfAGoneVolume(t, &stubPVCLister{uids: []string{"0b0e1d62-0000-4000-8000-000000000001"}})
	})
	t.Run("outside a cluster", func(t *testing.T) {
		checkDeleteVolumeReclaimsTheStagingOfAGoneVolume(t, nil)
	})
}

// deleteGoneVolumeBesideStaging deletes abandonedVolume, whose directory is
// gone and whose staging sibling is there, and returns what was removed.
func deleteGoneVolumeBesideStaging(t *testing.T, pvcs PersistentVolumeClaimLister, prepare func(*ControllerServer)) *stagingStore {
	t.Helper()
	store := newStagingStore(abandonedStaging)
	client := store.client()
	inner := client.LookupFileByPathFunc
	client.LookupFileByPathFunc = func(ctx context.Context, p string) (qumulo.FileInfo, error) {
		if p == testStoreRealPath+"/"+abandonedVolume {
			return qumulo.FileInfo{}, fakeAPIError(http.StatusNotFound, qumulo.ErrorClassNoSuchEntry, "no such path")
		}
		return inner(ctx, p)
	}
	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "0.1.0",
		NewClient:                   func(string, string) qumulo.ClientInterface { return client },
		PersistentVolumeClaimLister: pvcs,
	})
	require.NoError(t, err)
	srv := NewControllerServer(d)
	if prepare != nil {
		prepare(srv)
	}
	_, err = srv.DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: testHandle(abandonedVolume), Secrets: testSecrets()})
	require.NoError(t, err, "DeleteVolume of a gone volume succeeds whatever happens to the staging directory")
	return store
}

func TestDeleteVolumeOfAGoneVolumeKeepsStagingWhileItsClaimExists(t *testing.T) {
	store := deleteGoneVolumeBesideStaging(t, &stubPVCLister{uids: []string{abandonedUID}}, nil)
	assert.Empty(t, store.removedIDs(), "the claim exists, so its restore may still be running")
	assert.Empty(t, store.quotas)
}

func TestDeleteVolumeOfAGoneVolumeKeepsStagingWhenTheClaimListFails(t *testing.T) {
	store := deleteGoneVolumeBesideStaging(t, &stubPVCLister{err: errors.New("forbidden")}, nil)
	assert.Empty(t, store.removedIDs(), "left for CreateVolume's sweep")
}

func TestDeleteVolumeOfAGoneVolumeKeepsStagingWhileThisControllerIsRestoringIt(t *testing.T) {
	var release func()
	store := deleteGoneVolumeBesideStaging(t, nil, func(srv *ControllerServer) {
		var err error
		release, err = srv.restores.acquire(abandonedVolume)
		require.NoError(t, err)
	})
	defer release()
	assert.Empty(t, store.removedIDs())
}

func TestDeleteVolumeOfAGoneVolumeRemovesItsStagingOnceTheClaimIsGone(t *testing.T) {
	store := deleteGoneVolumeBesideStaging(t, &stubPVCLister{}, nil)
	assert.Equal(t, []string{"700"}, store.removedIDs())
}

// A 503 or 429 the array still answered after the client's retries, and an
// array that did not answer at all, end a restore attempt Unavailable --
// retried, resuming what is staged -- not Internal, which csi-provisioner
// treats as final. Any other failure stays Internal.
func TestRestoreCopyFailureCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"503", fakeAPIError(http.StatusServiceUnavailable, "", "injected"), codes.Unavailable},
		{"429", fakeAPIError(http.StatusTooManyRequests, "", "slow down"), codes.Unavailable},
		{"no answer", &url.Error{Op: "Post", URL: "https://array/v1/files/copy-chunk", Err: errors.New("connection reset by peer")}, codes.Unavailable},
		{"500", fakeAPIError(http.StatusInternalServerError, "", "boom"), codes.Internal},
		{"403", fakeAPIError(http.StatusForbidden, "", "no privilege"), codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := emptyRestoreClient()
			client.ListDirectoryFunc = func(_ context.Context, ref, snapshotID string) ([]qumulo.DirEntry, error) {
				if snapshotID != "" && ref == "900" {
					return []qumulo.DirEntry{file("big", 2<<30)}, nil
				}
				return nil, apiError(http.StatusNotFound)
			}
			client.CreateFileFunc = func(_ context.Context, parent, name string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{Path: parent + "/" + name}, nil
			}
			client.CopyFileFunc = func(context.Context, string, string, string, uint64) error { return tc.err }
			srv := newTestControllerServer(t, client)

			_, err := srv.CreateVolume(context.Background(), restoreRequest("pvc-restored", testParams(), 4*testGiB))

			assertCode(t, err, tc.want)
			assert.True(t, strings.Contains(err.Error(), "big"), "the message names what was being copied: %v", err)
		})
	}
}
