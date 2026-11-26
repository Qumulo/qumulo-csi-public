// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// stubPVLister answers ListPersistentVolumes from a slice a test edits
// between calls, as a cluster's PVs change between a refused delete and its
// retry.
type stubPVLister struct {
	mu    sync.Mutex
	pvs   []PersistentVolumeInfo
	err   error
	calls int
	// driverNames records the driver name each call filtered on.
	driverNames []string
}

func (l *stubPVLister) ListPersistentVolumes(_ context.Context, driverName string) ([]PersistentVolumeInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	l.driverNames = append(l.driverNames, driverName)
	if l.err != nil {
		return nil, l.err
	}
	return append([]PersistentVolumeInfo(nil), l.pvs...), nil
}

func (l *stubPVLister) set(pvs ...PersistentVolumeInfo) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pvs = pvs
}

// ownerHandle is the dedicated-export volume these tests delete, in the
// handle form CreateVolume mints.
const ownerHandle = testServer + "##/csi/volumes/pvc-owner"

// clusterWithOwnerVolume is the array: one dedicated-export volume,
// pvc-owner, with its quota and its own described export, under a store root.
func clusterWithOwnerVolume() *destructiveCallLog {
	c := newFakeCluster()
	c.directories["/csi/volumes"] = "20"
	c.directories["/csi/volumes/pvc-owner"] = "21"
	c.quotas["21"] = "1073741824"
	c.exports["e21"] = fakeExport{ExportPath: "/pvc-owner", FSPath: "/csi/volumes/pvc-owner",
		Description: exportDescriptionFor("pvc-owner")}
	return &destructiveCallLog{fakeCluster: c}
}

// ownerCheckServer is newIntegrationControllerServer with a PV lister.
func ownerCheckServer(t *testing.T, baseURL string, lister PersistentVolumeLister) *ControllerServer {
	t.Helper()
	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		NewClient: func(_, token string) qumulo.ClientInterface {
			return qumulo.New(baseURL, token)
		},
		PersistentVolumeLister: lister,
	})
	require.NoError(t, err)
	return NewControllerServer(d)
}

func boundPV(name, handle, claimNS, claim string) PersistentVolumeInfo {
	return PersistentVolumeInfo{Name: name, Handle: handle, Phase: "Bound", ClaimNamespace: claimNS, ClaimName: claim}
}

func releasedPV(name, handle string) PersistentVolumeInfo {
	return PersistentVolumeInfo{Name: name, Handle: handle, Phase: "Released", ClaimNamespace: "default", ClaimName: name}
}

func deleteHandle(t *testing.T, srv *ControllerServer, handle string) error {
	t.Helper()
	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})
	return err
}

func assertOwnerVolumeIntact(t *testing.T, l *destructiveCallLog) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	assert.Empty(t, l.treeDeletes, "a directory was tree-deleted")
	assert.Empty(t, l.exportDels, "an export was deleted")
	assert.Empty(t, l.quotaDels, "a quota was deleted")
	assert.Contains(t, l.directories, "/csi/volumes/pvc-owner", "the owner's directory is gone")
	assert.Contains(t, l.exports, "e21", "the owner's export is gone")
}

func assertOwnerVolumeDeleted(t *testing.T, l *destructiveCallLog) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	assert.NotEmpty(t, l.treeDeletes, "the volume was not tree-deleted")
	assert.NotContains(t, l.exports, "e21", "the volume's export was left")
}

// (a) A static PV carrying a verbatim copy of a Bound
// claim's handle is released; its DeleteVolume must remove nothing and name
// the PV and claim that still use the directory.
func TestDeleteVolumeRefusesWhileAnotherPVWithTheSameHandleIsBound(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(boundPV("pvc-owner", ownerHandle, "apps", "p93-owner"), releasedPV("p93-copy-pv", ownerHandle))

	err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle)

	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), `"pvc-owner"`)
	assert.Contains(t, err.Error(), "apps/p93-owner")
	assert.Contains(t, err.Error(), "Retain")
	assert.Contains(t, err.Error(), "nothing was deleted")
	assertOwnerVolumeIntact(t, l)
	assert.Equal(t, []string{"csi.qumulo.com"}, lister.driverNames, "the list was not filtered on this driver's name")
}

// Each handle form that names the owner's directory is the same volume:
// the legacy two-field form, a trailing slash, and an apiUrl spelling out
// the default endpoint.
func TestDeleteVolumeMatchesEveryHandleFormOfTheSameDirectory(t *testing.T) {
	for _, other := range []string{
		testServer + "#/csi/volumes/pvc-owner",
		testServer + "##/csi/volumes/pvc-owner/",
		testServer + "#https://" + testServer + ":8000#/csi/volumes/pvc-owner",
		testServer + "#https://" + testServer + ":8000/#/csi/volumes/pvc-owner",
		"QUMULO.example.com##/csi/volumes/pvc-owner",
		// The same NFS address with an explicit endpoint: the owner's
		// handle has none, so it dials whatever the name resolves to.
		testServer + "#https://192.0.2.10:8000#/csi/volumes/pvc-owner",
	} {
		t.Run(other, func(t *testing.T) {
			l := clusterWithOwnerVolume()
			server := httptest.NewServer(l.handler())
			defer server.Close()
			lister := &stubPVLister{}
			lister.set(boundPV("pvc-owner", other, "apps", "p93-owner"), releasedPV("p93-copy-pv", ownerHandle))

			err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle)

			assertCode(t, err, codes.FailedPrecondition)
			assertOwnerVolumeIntact(t, l)
		})
	}
}

// The sequence this leaves behind: the copy is refused while the owner is
// Bound; the owner's claim is deleted later and its own delete proceeds,
// because no PV still in use names the directory; the copy's next retry finds
// the volume gone and succeeds, as CSI asks, so neither PV is stuck Released.
func TestTheOwnerIsDeletedAfterARefusedCopyAndTheCopysRetrySucceeds(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	srv := ownerCheckServer(t, server.URL, lister)

	// 1. The copy's claim is deleted while the owner is Bound.
	lister.set(boundPV("pvc-owner", ownerHandle, "apps", "p93-owner"), releasedPV("p93-copy-pv", ownerHandle))
	assertCode(t, deleteHandle(t, srv, ownerHandle), codes.FailedPrecondition)
	assertOwnerVolumeIntact(t, l)

	// 2. The owner's claim is deleted: both PVs are Released.
	lister.set(releasedPV("pvc-owner", ownerHandle), releasedPV("p93-copy-pv", ownerHandle))
	require.NoError(t, deleteHandle(t, srv, ownerHandle))
	assertOwnerVolumeDeleted(t, l)

	// 3. The owner's PV is gone; the copy retries.
	lister.set(releasedPV("p93-copy-pv", ownerHandle))
	require.NoError(t, deleteHandle(t, srv, ownerHandle), "the copy's retry must be the idempotent success")
}

// Two Released PVs naming one directory are both on their way out: whichever
// delete comes first removes the volume and the other finds it gone.
func TestDeleteVolumeProceedsWhenEveryPVNamingItIsReleased(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(releasedPV("pvc-owner", ownerHandle), releasedPV("p93-copy-pv", testServer+"#/csi/volumes/pvc-owner"))
	srv := ownerCheckServer(t, server.URL, lister)

	require.NoError(t, deleteHandle(t, srv, ownerHandle))
	assertOwnerVolumeDeleted(t, l)
	require.NoError(t, deleteHandle(t, srv, ownerHandle))
}

// The one PV that names the directory is the one being deleted, whatever its
// phase says: a lone PV never blocks its own delete.
func TestDeleteVolumeProceedsWhenOnlyItsOwnPVNamesIt(t *testing.T) {
	for _, own := range []PersistentVolumeInfo{
		releasedPV("pvc-owner", ownerHandle),
		{Name: "pvc-owner", Handle: ownerHandle, Phase: "Bound", Deleting: true},
		{Name: "pvc-owner", Handle: ownerHandle, Phase: "Failed"},
		boundPV("pvc-owner", ownerHandle, "apps", "owner"),
	} {
		t.Run(own.Phase, func(t *testing.T) {
			l := clusterWithOwnerVolume()
			server := httptest.NewServer(l.handler())
			defer server.Close()
			lister := &stubPVLister{}
			lister.set(own)

			require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle))
			assertOwnerVolumeDeleted(t, l)
		})
	}
}

// A PV that is not Released still uses the directory whatever else it is: an
// Available static PV waiting for a claim, or one still Pending.
func TestDeleteVolumeRefusesWhileAnUnclaimedPVNamesIt(t *testing.T) {
	for _, phase := range []string{"Available", "Pending", ""} {
		t.Run(phase, func(t *testing.T) {
			l := clusterWithOwnerVolume()
			server := httptest.NewServer(l.handler())
			defer server.Close()
			lister := &stubPVLister{}
			lister.set(releasedPV("pvc-owner", ownerHandle),
				PersistentVolumeInfo{Name: "imported", Handle: ownerHandle, Phase: phase})

			err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle)

			assertCode(t, err, codes.FailedPrecondition)
			assert.Contains(t, err.Error(), `"imported"`)
			assertOwnerVolumeIntact(t, l)
		})
	}
}

// A PV being deleted is on its way out even if its phase still reads Bound.
func TestDeleteVolumeIgnoresAPVThatIsBeingDeleted(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(releasedPV("pvc-owner", ownerHandle),
		PersistentVolumeInfo{Name: "copy", Handle: ownerHandle, Phase: "Bound", Deleting: true})

	require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle))
	assertOwnerVolumeDeleted(t, l)
}

// A tree delete removes everything below the directory too, so a PV in use
// whose directory lies below it refuses the delete as surely as a copy does.
// One whose directory lies above it does not: deleting a volume inside a
// share someone mounted is what a shared-subdirectory class does every day.
func TestDeleteVolumeRefusesWhileAPVInUseNamesADirectoryBelowIt(t *testing.T) {
	l := clusterWithOwnerVolume()
	l.directories["/csi/volumes/pvc-owner/team"] = "22"
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(releasedPV("pvc-owner", ownerHandle),
		boundPV("team-share", testServer+"##/csi/volumes/pvc-owner/team", "team", "data"),
		boundPV("store-share", testServer+"##/csi/volumes", "ops", "everything"))

	err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle)

	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), `"team-share"`)
	assert.NotContains(t, err.Error(), "store-share")
	assertOwnerVolumeIntact(t, l)
}

// A PV naming the same path on another array, a sibling whose name merely
// starts the same, and a handle that does not parse are not this volume.
func TestDeleteVolumeIgnoresPVsThatNameAnotherDirectory(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(releasedPV("pvc-owner", ownerHandle),
		boundPV("dr-copy", "qumulo-dr.example.com##/csi/volumes/pvc-owner", "apps", "dr"),
		boundPV("sibling", testServer+"##/csi/volumes/pvc-owner-2", "apps", "sib"),
		boundPV("junk", "not-a-handle", "apps", "junk"),
		boundPV("dotted", testServer+"##/csi/volumes/x/../pvc-owner", "apps", "dot"))

	require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle))
	assertOwnerVolumeDeleted(t, l)
}

// A class pinned to each array of a replicated pair behind one floating NFS
// name mints handles that share the server and differ in endpoint.
// They name two arrays: a DR copy's PV in use does not hold up the delete of
// the source's volume.
func TestDeleteVolumeIgnoresAPVPinnedToAnotherArrayBehindTheSameName(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	pinnedA := testServer + "#https://array-a.example.com:8000#/csi/volumes/pvc-owner"
	lister := &stubPVLister{}
	lister.set(releasedPV("pvc-owner", pinnedA),
		boundPV("dr-copy", testServer+"#https://array-b.example.com:8000#/csi/volumes/pvc-owner", "apps", "dr"))

	require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), pinnedA))
	assertOwnerVolumeDeleted(t, l)
}

// The check cannot be made without the list, and a tree delete cannot be
// undone: a failed list refuses with Unavailable, which the provisioner
// retries, and removes nothing.
func TestDeleteVolumeRefusesWhenThePVListFails(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{err: errors.New(`persistentvolumes is forbidden: User "x" cannot list resource`)}

	err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle)

	assertCode(t, err, codes.Unavailable)
	assert.Contains(t, err.Error(), "forbidden")
	assert.Contains(t, err.Error(), "nothing was deleted")
	assertOwnerVolumeIntact(t, l)
}

// With no lister -- csi-sanity, a local run, any controller outside a
// cluster -- DeleteVolume behaves exactly as it did before the check.
func TestDeleteVolumeWithoutAPVListerDeletesAsBefore(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	srv := ownerCheckServer(t, server.URL, nil)

	require.NoError(t, deleteHandle(t, srv, ownerHandle))
	assertOwnerVolumeDeleted(t, l)
}

// A volume already gone is the idempotent success without a list: there is
// nothing left to protect, and a retry must not depend on the API.
func TestDeleteVolumeOfAGoneVolumeDoesNotListPVs(t *testing.T) {
	c := newFakeCluster()
	server := httptest.NewServer(c.handler())
	defer server.Close()
	lister := &stubPVLister{err: errors.New("the API is down")}

	require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle))
	assert.Zero(t, lister.calls)
}

// quotaOnlyHandle names a directory with a quota and no export of its own --
// a shared-subdirectory volume, or an administrator's folder.
const quotaOnlyHandle = testServer + "##/csi/volumes/team-data"

// clusterWithQuotaOnlyDirectory is an array holding /csi/volumes/team-data
// with a quota, no export, and contents.
func clusterWithQuotaOnlyDirectory() *destructiveCallLog {
	c := newFakeCluster()
	c.directories["/csi/volumes"] = "20"
	c.directories["/csi/volumes/team-data"] = "30"
	c.directories["/csi/volumes/team-data/reports"] = "31"
	c.quotas["30"] = "1073741824"
	return &destructiveCallLog{fakeCluster: c}
}

func assertNothingDeleted(t *testing.T, l *destructiveCallLog, dir string) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	assert.Empty(t, l.treeDeletes, "a directory was tree-deleted")
	assert.Empty(t, l.exportDels, "an export was deleted")
	assert.Empty(t, l.quotaDels, "a quota was deleted")
	assert.Contains(t, l.directories, dir, "the directory is gone")
}

func assertTreeDeleted(t *testing.T, l *destructiveCallLog) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	assert.NotEmpty(t, l.treeDeletes, "the directory was not tree-deleted")
}

// withDeleteDirectory is a released PV carrying the opt-in annotation.
func withDeleteDirectory(pv PersistentVolumeInfo, dir string) PersistentVolumeInfo {
	pv.DeleteDirectory = dir
	return pv
}

// A released PV named after its directory -- every dynamically provisioned PV,
// whose name csi-provisioner also hands CreateVolume as the volume's name --
// owns it, whichever model made the volume. A --volume-name-prefix only
// changes the name both are given.
func TestDeleteVolumeDeletesTheDirectoryOfAReleasedPVNamedAfterIt(t *testing.T) {
	t.Run("dedicated export", func(t *testing.T) {
		l := clusterWithOwnerVolume()
		server := httptest.NewServer(l.handler())
		defer server.Close()
		lister := &stubPVLister{}
		lister.set(releasedPV("pvc-owner", ownerHandle))

		require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle))
		assertOwnerVolumeDeleted(t, l)
	})
	for _, name := range []string{"team-data", "myprefix-2f6c9a51-77a4-4c0e-9d0b-6b3a1f0e5c11"} {
		t.Run("quota only "+name, func(t *testing.T) {
			c := newFakeCluster()
			c.directories["/csi/volumes"] = "20"
			c.directories["/csi/volumes/"+name] = "30"
			c.quotas["30"] = "1073741824"
			l := &destructiveCallLog{fakeCluster: c}
			server := httptest.NewServer(l.handler())
			defer server.Close()
			handle := testServer + "##/csi/volumes/" + name
			lister := &stubPVLister{}
			lister.set(releasedPV(name, handle))

			require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), handle))
			assertTreeDeleted(t, l)
		})
	}
}

// A DR-side PV the repoint tool wrote is named <source><suffix> and records
// its source in csi.qumulo.com/mirrored-from. The source's name is the
// directory's, so the mirror owns the replica's directory without an opt-in.
func TestDeleteVolumeDeletesTheDirectoryOfAReleasedMirrorPV(t *testing.T) {
	l := clusterWithQuotaOnlyDirectory()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	mirror := releasedPV("team-data-dr", quotaOnlyHandle)
	mirror.MirroredFrom = "team-data"
	lister := &stubPVLister{}
	lister.set(mirror)

	require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), quotaOnlyHandle))
	assertTreeDeleted(t, l)
}

// The opt-in: an administrator who wants Kubernetes to delete an imported
// directory says so on the PV, naming the directory's real path on the array.
// It is compared cleaned, so a trailing slash still matches; anything else --
// another path, the NFS export path, a path with a ".." element that would
// clean to it -- is not the opt-in.
func TestDeleteVolumeHonoursTheDeleteDirectoryAnnotationOnlyForTheExactPath(t *testing.T) {
	for _, tc := range []struct {
		value   string
		deleted bool
	}{
		{"/csi/volumes/team-data", true},
		{"/csi/volumes/team-data/", true},
		{"/csi/volumes//team-data", true},
		{"/csi/volumes/team-data-2", false},
		{"/csi/volumes", false},
		{"/team-data", false},
		{"/csi/volumes/x/../team-data", false},
		{"/csi/volumes/./team-data", false},
		{"csi/volumes/team-data", false},
		{"", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			l := clusterWithQuotaOnlyDirectory()
			server := httptest.NewServer(l.handler())
			defer server.Close()
			lister := &stubPVLister{}
			lister.set(withDeleteDirectory(releasedPV("finance-import", quotaOnlyHandle), tc.value))

			err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), quotaOnlyHandle)

			if tc.deleted {
				require.NoError(t, err)
				assertTreeDeleted(t, l)
				return
			}
			assertCode(t, err, codes.FailedPrecondition)
			assertNothingDeleted(t, l, "/csi/volumes/team-data")
		})
	}
}

// The export path is what a client mounts, not the directory's path on the
// array, and it is not the opt-in even for a volume with its own export.
func TestDeleteVolumeDoesNotTakeTheExportPathAsTheDeleteDirectory(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(withDeleteDirectory(releasedPV("imported", ownerHandle), "/pvc-owner"))

	assertCode(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle), codes.FailedPrecondition)
	assertOwnerVolumeIntact(t, l)
}

// A hand-written PV, alone, reclaim Delete, aimed at a directory
// with a quota and no export -- an administrator's folder. "Has a quota" is no
// proof this driver made it, and the PV neither is named after it nor carries
// the opt-in, so nothing is deleted and the refusal says both ways out.
func TestDeleteVolumeRefusesAReleasedStaticPVThatDoesNotOwnAQuotaOnlyDirectory(t *testing.T) {
	l := clusterWithQuotaOnlyDirectory()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(releasedPV("finance-import", quotaOnlyHandle))

	err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), quotaOnlyHandle)

	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), `"finance-import"`)
	assert.Contains(t, err.Error(), "nothing was deleted")
	assert.Contains(t, err.Error(), "Retain")
	assert.Contains(t, err.Error(), `csi.qumulo.com/delete-directory: "/csi/volumes/team-data"`)
	assertNothingDeleted(t, l, "/csi/volumes/team-data")
	assert.Contains(t, l.quotas, "30", "the administrator's quota is gone")
}

// The rule is the same for a directory with this volume's own export:
// the export's description is derived from the handle, so a
// static PV copying a released owner's handle carries it as well.
func TestDeleteVolumeRefusesAReleasedStaticPVThatDoesNotOwnADedicatedExportDirectory(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(releasedPV("imported", ownerHandle))

	err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle)

	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), `"imported"`)
	assertOwnerVolumeIntact(t, l)
}

// The same, second form: a copy whose handle pins another REST endpoint of the
// same array is not matched to its owner by sameArray, so the in-use check
// lets it through; the owner rule does not, because the copy is not named
// after the directory.
func TestDeleteVolumeRefusesACopyPinningAnotherEndpointOfTheSameArray(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	ownerPinned := testServer + "#https://10.0.0.1:8000#/csi/volumes/pvc-owner"
	copyPinned := testServer + "#https://10.0.0.2:8000#/csi/volumes/pvc-owner"
	lister := &stubPVLister{}
	lister.set(boundPV("pvc-owner", ownerPinned, "apps", "owner"), releasedPV("p113-copy-pv", copyPinned))

	err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), copyPinned)

	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), `"p113-copy-pv"`)
	assertOwnerVolumeIntact(t, l)
}

// A copy of a PV whose owner was released with Retain: whether the owner's PV
// object was then removed, or is still there Released, the copy is not the
// owner and deletes nothing. A Retain PV is never the one being deleted --
// csi-provisioner calls DeleteVolume only for reclaim Delete -- so it does not
// lend the copy its ownership.
func TestDeleteVolumeRefusesACopyOfAnOwnerReleasedWithRetain(t *testing.T) {
	retained := releasedPV("pvc-owner", ownerHandle)
	retained.ReclaimPolicy = "Retain"
	copied := releasedPV("p113-copy-pv", ownerHandle)
	copied.ReclaimPolicy = "Delete"
	for name, pvs := range map[string][]PersistentVolumeInfo{
		"owner PV removed":  {copied},
		"owner PV retained": {retained, copied},
	} {
		t.Run(name, func(t *testing.T) {
			l := clusterWithOwnerVolume()
			server := httptest.NewServer(l.handler())
			defer server.Close()
			lister := &stubPVLister{}
			lister.set(pvs...)

			err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle)

			assertCode(t, err, codes.FailedPrecondition)
			assert.Contains(t, err.Error(), `"p113-copy-pv"`)
			assertOwnerVolumeIntact(t, l)
		})
	}
}

// The owner itself, released with Delete, is deleted even while a released
// Retain copy of it is still there.
func TestDeleteVolumeDeletesAnOwnerWhoseCopyIsRetained(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	owner := releasedPV("pvc-owner", ownerHandle)
	owner.ReclaimPolicy = "Delete"
	copied := releasedPV("p113-copy-pv", ownerHandle)
	copied.ReclaimPolicy = "Retain"
	lister := &stubPVLister{}
	lister.set(copied, owner)

	require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), ownerHandle))
	assertOwnerVolumeDeleted(t, l)
}

// csi-provisioner deletes a volume no PV carries when saving the PV after
// CreateVolume failed; refusing it would leak the volume.
func TestDeleteVolumeDeletesAVolumeNoPVCarries(t *testing.T) {
	l := clusterWithQuotaOnlyDirectory()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(boundPV("other", testServer+"##/csi/volumes/other", "apps", "other"))

	require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, lister), quotaOnlyHandle))
	assertTreeDeleted(t, l)
	assert.Equal(t, 1, lister.calls, "both checks must share one list")
}

// A PV whose handle is the same directory spelled differently is not the PV
// being deleted: the request names a handle, and only PVs carrying exactly it
// are. A released owner spelled another way lends a copy nothing.
func TestDeleteVolumeMatchesTheReleasedPVByItsExactHandle(t *testing.T) {
	l := clusterWithOwnerVolume()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(releasedPV("pvc-owner", ownerHandle), releasedPV("p113-copy-pv", testServer+"#/csi/volumes/pvc-owner"))

	err := deleteHandle(t, ownerCheckServer(t, server.URL, lister), testServer+"#/csi/volumes/pvc-owner")

	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), `"p113-copy-pv"`)
	assertOwnerVolumeIntact(t, l)
}

// A retry after Aborted finds its own tree delete still running and waits on
// it, as before the owner rule: neither check runs again, so a PV list that
// now fails cannot strand a delete the cluster is already doing.
func TestDeleteVolumeRetryOfARunningTreeDeleteIsNotRefusedByTheOwnerRule(t *testing.T) {
	cluster := newFakeCluster()
	cluster.directories["/csi/volumes"] = "20"
	cluster.directories["/csi/volumes/team-data"] = "30"
	cluster.quotas["30"] = "1073741824"
	cluster.pendingPolls = 1000
	cluster.lingerUntilFinished = true
	l := &destructiveCallLog{fakeCluster: cluster}
	server := httptest.NewServer(l.handler())
	defer server.Close()
	lister := &stubPVLister{}
	lister.set(releasedPV("team-data", quotaOnlyHandle))
	srv := ownerCheckServer(t, server.URL, lister)
	srv.treeDeleteBudget = 20 * time.Millisecond
	srv.treeDeletePollWait = 5 * time.Millisecond

	assertCode(t, deleteHandle(t, srv, quotaOnlyHandle), codes.Aborted)
	require.Len(t, l.treeDeletes, 1)

	lister.mu.Lock()
	lister.err = errors.New("the API is down")
	lister.mu.Unlock()
	calls := lister.calls
	assertCode(t, deleteHandle(t, srv, quotaOnlyHandle), codes.Aborted)
	for id := range cluster.treeDeletePolls {
		cluster.treeDeletePolls[id] = 2
	}
	require.NoError(t, deleteHandle(t, srv, quotaOnlyHandle))
	assert.Len(t, l.treeDeletes, 1)
	assert.Equal(t, calls, lister.calls, "a retry of a running tree delete listed PVs")
}

// With no lister the owner rule is skipped too: csi-sanity's static volumes
// are deleted as before.
func TestDeleteVolumeWithoutAPVListerDeletesAQuotaOnlyDirectoryAsBefore(t *testing.T) {
	l := clusterWithQuotaOnlyDirectory()
	server := httptest.NewServer(l.handler())
	defer server.Close()

	require.NoError(t, deleteHandle(t, ownerCheckServer(t, server.URL, nil), quotaOnlyHandle))
	assertTreeDeleted(t, l)
}
