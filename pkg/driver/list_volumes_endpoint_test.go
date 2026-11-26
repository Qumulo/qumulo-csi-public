// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// withAPIURL returns class with its apiUrl set.
func withAPIURL(class StorageClassInfo, apiURL string) StorageClassInfo {
	class.Parameters[paramAPIURL] = apiURL
	return class
}

// oneVolumeAt serves a store root holding one volume with a quota.
func oneVolumeAt(name, fileID string) *MockClient {
	return &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return dirEntries(map[string]string{name: fileID}), nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
}

var oneToken = map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}}

// A DNS-named class and an address class on the same endpoint see the
// same directory. It is one volume, and it is listed once, not also under a
// handle no PV has, whose gauges would then outlive the volume. With no PV
// to go by -- no VolumeHandleLister -- the handle with no endpoint field is
// kept; TestListVolumesKeepsTheHandleThePVCarries covers the PV case.
func TestListVolumesListsADirectoryOnceWhateverNameItsArrayIsReachedBy(t *testing.T) {
	array := oneVolumeAt("pvc-1", "101")
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			withAPIURL(classFor("a-dns", "dr-name.example.com", "/csi/a", "tok"), "https://192.0.2.10:8000"),
			classFor("b-address", "192.0.2.10", "/csi/a", "tok"),
		}},
		oneToken,
		map[string]*MockClient{"https://192.0.2.10:8000": array},
	)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Equal(t, "192.0.2.10##/csi/a/pvc-1", resp.GetEntries()[0].GetVolume().GetVolumeId())
}

// A Secret that exists but could not be read -- throttled, timed out --
// says nothing about the class. Skipping it shortened the listing, which a
// caller cannot tell from those volumes having been deleted; the call fails,
// retryably, instead.
func TestListVolumesFailsRetryablyWhenASecretCannotBeRead(t *testing.T) {
	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "0.1.0",
		SecretReader: &fakeSecretReader{err: errors.New("client rate limiter Wait returned an error: context deadline exceeded")},
		StorageClassLister: &fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok"),
		}},
		NewClient: func(string, string) qumulo.ClientInterface { return oneVolumeAt("pvc-1", "101") },
	})
	require.NoError(t, err)

	_, err = NewControllerServer(d).ListVolumes(context.Background(), &csi.ListVolumesRequest{})

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err), status.Convert(err).Message())
}

// The skip itself is unchanged for a real misconfiguration: a Secret that does
// not exist. (TestListVolumesSkipsOneBadClassButKeepsTheRest covers it too.)
func TestListVolumesStillSkipsAClassWhoseSecretDoesNotExist(t *testing.T) {
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok"),
			classFor("class-gone", "qumulo-b.example.com", "/csi/b", "gone"),
		}},
		oneToken,
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): oneVolumeAt("pvc-1", "101")},
	)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})

	require.NoError(t, err)
	assert.Len(t, resp.GetEntries(), 1)
}

// Two classes share a DNS server name; each reaches its own array
// through its own apiUrl. A handle names array B's endpoint, so array B
// answers -- with class B's own Secret -- and array A is never asked.
func TestControllerGetVolumeAnswersFromTheArrayTheHandleNames(t *testing.T) {
	arrayA := &MockClient{
		LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
			t.Fatal("array A must not be asked about a volume whose handle names array B")
			return qumulo.FileInfo{}, nil
		},
	}
	arrayB := &MockClient{
		LookupFileByPathFunc: volumeAt("/csi/v/pvc-1", "202"),
		GetQuotaFunc:         func(context.Context, string) (uint64, error) { return 3 << 30, nil },
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			withAPIURL(classFor("a", "csi.example.com", "/csi/v", "tok"), "https://array-a.example.com:8000"),
			withAPIURL(classFor("b", "csi.example.com", "/csi/v", "tok"), "https://array-b.example.com:8000"),
		}},
		oneToken,
		map[string]*MockClient{
			"https://array-a.example.com:8000": arrayA,
			"https://array-b.example.com:8000": arrayB,
		},
	)

	resp, err := srv.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{
		VolumeId: "csi.example.com#https://array-b.example.com:8000#/csi/v/pvc-1",
	})

	require.NoError(t, err)
	assert.Equal(t, int64(3<<30), resp.GetVolume().GetCapacityBytes())
}

// The same two classes: array B holds a replication target, array A
// holds nothing of the kind. Keyed by server name, A's empty target table was
// applied to B and B's DR copy was listed as a volume.
func TestListVolumesNeverListsAReplicaOnAnArraySharingAServerName(t *testing.T) {
	arrayA := oneVolumeAt("pvc-src", "101")
	arrayB := oneVolumeAt("pvc-src", "202")
	arrayB.ListTargetStatusFunc = func(context.Context) ([]qumulo.RelationshipStatus, error) {
		return []qumulo.RelationshipStatus{{ID: "rel-1", State: qumulo.RelationshipEstablished,
			TargetRootPath: "/csi/v/pvc-src"}}, nil
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			withAPIURL(classFor("a", "csi.example.com", "/csi/v", "tok"), "https://array-a.example.com:8000"),
			withAPIURL(classFor("b", "csi.example.com", "/csi/v", "tok"), "https://array-b.example.com:8000"),
		}},
		oneToken,
		map[string]*MockClient{
			"https://array-a.example.com:8000": arrayA,
			"https://array-b.example.com:8000": arrayB,
		},
	)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1, "the DR copy on array B is not a volume")
	assert.Equal(t, "csi.example.com#https://array-a.example.com:8000#/csi/v/pvc-src",
		resp.GetEntries()[0].GetVolume().GetVolumeId())
}

// fakeHandleLister answers ListVolumeHandles from a fixed list.
type fakeHandleLister struct {
	handles []string
	err     error
}

func (f *fakeHandleLister) ListVolumeHandles(context.Context, string) ([]string, error) {
	return f.handles, f.err
}

// twoClassServer is the layout -- a DNS-named class with an apiUrl and
// an address class, both reaching one directory -- with the given PVs.
// dnsFirst sets which class sorts first by name, and so which handle the
// listing meets first.
func twoClassServer(t *testing.T, pvs VolumeHandleLister, dnsFirst bool) *ControllerServer {
	t.Helper()
	dnsName, addressName := "a-dns", "b-address"
	if !dnsFirst {
		dnsName, addressName = "b-dns", "a-address"
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			withAPIURL(classFor(dnsName, "dr-name.example.com", "/csi/a", "tok"), "https://192.0.2.10:8000"),
			classFor(addressName, "192.0.2.10", "/csi/a", "tok"),
		}},
		oneToken,
		map[string]*MockClient{"https://192.0.2.10:8000": oneVolumeAt("pvc-1", "101")},
	)
	srv.driver.VolumeHandleLister = pvs
	return srv
}

func listedHandles(t *testing.T, srv *ControllerServer) []string {
	t.Helper()
	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	var out []string
	for _, e := range resp.GetEntries() {
		out = append(out, e.GetVolume().GetVolumeId())
	}
	return out
}

// The deduplication kept the handle with no endpoint field
// unconditionally, so a PV provisioned through the DNS-named class was listed
// only under a handle no PV has: a reconciler saw the real PV as missing, and
// its gauges followed the other handle. The directory is now listed under the
// handle its PV carries, whichever class minted it.
func TestListVolumesKeepsTheHandleThePVCarries(t *testing.T) {
	const (
		dnsHandle     = "dr-name.example.com#https://192.0.2.10:8000#/csi/a/pvc-1"
		addressHandle = "192.0.2.10##/csi/a/pvc-1"
	)
	for _, tt := range []struct {
		name string
		pvs  []string
		want []string
	}{
		{name: "PV from the DNS-named class", pvs: []string{dnsHandle}, want: []string{dnsHandle}},
		{name: "PV from the address class", pvs: []string{addressHandle}, want: []string{addressHandle}},
		{name: "a PV in the two-field form an older build minted", pvs: []string{"192.0.2.10#/csi/a/pvc-1"}, want: []string{addressHandle}},
		{name: "two static PVs, one per handle", pvs: []string{dnsHandle, addressHandle}, want: []string{addressHandle, dnsHandle}},
		{name: "no PV yet", pvs: nil, want: []string{addressHandle}},
	} {
		for _, dnsFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s, DNS-named class first=%t", tt.name, dnsFirst), func(t *testing.T) {
				got := listedHandles(t, twoClassServer(t, &fakeHandleLister{handles: tt.pvs}, dnsFirst))
				assert.ElementsMatch(t, tt.want, got)
			})
		}
	}
}

// Without the PV list the choice is a guess, and a wrong guess is, so a
// PV list that fails fails the call rather than falling back to the guess.
func TestListVolumesIsUnavailableWhenThePVsCannotBeListed(t *testing.T) {
	srv := twoClassServer(t, &fakeHandleLister{err: errors.New("persistentvolumes is forbidden")}, true)

	_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})

	requireCode(t, err, codes.Unavailable)
}

// fakeRefLister answers ListVolumeRefs (and ListVolumeHandles) from a list.
type fakeRefLister struct{ refs []VolumeRef }

func (f *fakeRefLister) ListVolumeHandles(context.Context, string) ([]string, error) {
	out := make([]string, 0, len(f.refs))
	for _, r := range f.refs {
		out = append(out, r.Handle)
	}
	return out, nil
}

func (f *fakeRefLister) ListVolumeRefs(context.Context, string) ([]VolumeRef, error) {
	return f.refs, nil
}

// A DR copy's PV carries a handle on the DR array while its
// storageClassName names the primary's class -- that is how the failover tool
// makes it -- and ListVolumes walked only each class's own array, so every DR
// copy's PV was missing. A PV on an array no class walks is now listed, once
// the array confirms the directory, using the token its class's Secret keys
// by that array's address. With no such token it is left out, and logged;
// the plain token is never sent to another array.
func TestListVolumesListsTheDRCopiesPVs(t *testing.T) {
	const (
		primaryHandle = "array-a.example.com##/csi/v/pvc-src"
		drHandle      = "array-b.example.com##/csi/v/pvc-src"
		goneHandle    = "array-b.example.com##/csi/v/pvc-gone"
	)
	listing := func(drToken bool) ([]string, []string) {
		var dialled []string
		primary := &MockClient{
			ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
				return dirEntries(map[string]string{"pvc-src": "11"}), nil
			},
			GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
		}
		dr := &MockClient{
			LookupFileByPathFunc: func(_ context.Context, p string) (qumulo.FileInfo, error) {
				if p == "/csi/v/pvc-gone" {
					return qumulo.FileInfo{}, &qumulo.APIError{StatusCode: 404, Class: "fs_no_such_entry_error"}
				}
				return qumulo.FileInfo{ID: "22", Path: p}, nil
			},
			GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
		}
		secret := map[string][]byte{secretKeyToken: []byte("token-a")}
		if drToken {
			secret["array-b.example.com"] = []byte("token-b")
		}
		d, err := NewDriver(Config{
			Name: "csi.qumulo.com", Version: "0.1.0",
			SecretReader: &fakeSecretReader{secrets: map[string]map[string][]byte{"qumulo-csi/tok": secret}},
			StorageClassLister: &fakeClassLister{classes: []StorageClassInfo{
				classFor("primary", "array-a.example.com", "/csi/v", "tok"),
			}},
			VolumeHandleLister: &fakeRefLister{refs: []VolumeRef{
				{Handle: primaryHandle, StorageClass: "primary"},
				{Handle: drHandle, StorageClass: "primary"},
				{Handle: goneHandle, StorageClass: "primary"},
			}},
			NewClient: func(baseURL, token string) qumulo.ClientInterface {
				dialled = append(dialled, baseURL+" "+token)
				if baseURL == baseURLFor("array-b.example.com") {
					return dr
				}
				return primary
			},
		})
		require.NoError(t, err)
		resp, err := NewControllerServer(d).ListVolumes(context.Background(), &csi.ListVolumesRequest{})
		require.NoError(t, err)
		var ids []string
		for _, e := range resp.GetEntries() {
			ids = append(ids, e.GetVolume().GetVolumeId())
		}
		return ids, dialled
	}

	ids, dialled := listing(true)
	assert.ElementsMatch(t, []string{primaryHandle, drHandle}, ids, "the DR copy's PV is listed; a gone one is not")
	assert.Contains(t, dialled, baseURLFor("array-b.example.com")+" token-b", "with the DR array's own token")

	ids, dialled = listing(false)
	assert.Equal(t, []string{primaryHandle}, ids, "no token for the DR array: left out, not an error")
	for _, d := range dialled {
		assert.NotContains(t, d, "array-b", "the primary's token is never sent to the DR array")
	}
}

// The common case. The DR array has classes of its own, so it is
// walked, and the walk drops replica roots. The first fix skipped
// every PV on a walked array, so a DR copy's PV was still missing whenever
// the DR array had a class -- the usual DR lab layout. The
// decision is per PV: one the walk did not list is confirmed on its own
// array, with the token its class's Secret keys by that array's address, or
// else with the credential of the class that walks that array. A replica
// root with no PV stays unlisted.
func TestListVolumesListsTheDRCopiesPVsWhenTheDRArrayHasClasses(t *testing.T) {
	const (
		primaryHandle = "array-a.example.com##/csi/v/pvc-src"
		drHandle      = "array-b.example.com##/csi/v/pvc-src"
		ownHandle     = "array-b.example.com##/csi/v/pvc-own"
	)
	listing := func(drKey bool) ([]string, []string) {
		var dialled []string
		primary := &MockClient{
			ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
				return dirEntries(map[string]string{"pvc-src": "11"}), nil
			},
			GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
		}
		dr := &MockClient{
			ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
				return dirEntries(map[string]string{"pvc-src": "22", "pvc-own": "23", "pvc-orphan": "24"}), nil
			},
			ListTargetStatusFunc: func(context.Context) ([]qumulo.RelationshipStatus, error) {
				return []qumulo.RelationshipStatus{
					{TargetRootPath: "/csi/v/pvc-src", State: qumulo.RelationshipEstablished},
					{TargetRootPath: "/csi/v/pvc-orphan", State: qumulo.RelationshipEstablished},
				}, nil
			},
			LookupFileByPathFunc: func(_ context.Context, p string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{ID: "22", Path: p}, nil
			},
			GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
		}
		primarySecret := map[string][]byte{secretKeyToken: []byte("token-a")}
		if drKey {
			primarySecret["array-b.example.com"] = []byte("token-b")
		}
		d, err := NewDriver(Config{
			Name: "csi.qumulo.com", Version: "0.1.0",
			SecretReader: &fakeSecretReader{secrets: map[string]map[string][]byte{
				"qumulo-csi/tok-a": primarySecret,
				"qumulo-csi/tok-b": {secretKeyToken: []byte("token-b-class")},
			}},
			StorageClassLister: &fakeClassLister{classes: []StorageClassInfo{
				classFor("primary", "array-a.example.com", "/csi/v", "tok-a"),
				classFor("dr", "array-b.example.com", "/csi/v", "tok-b"),
			}},
			VolumeHandleLister: &fakeRefLister{refs: []VolumeRef{
				{Handle: primaryHandle, StorageClass: "primary"},
				{Handle: drHandle, StorageClass: "primary"},
				{Handle: ownHandle, StorageClass: "dr"},
			}},
			NewClient: func(baseURL, token string) qumulo.ClientInterface {
				dialled = append(dialled, baseURL+" "+token)
				if baseURL == baseURLFor("array-b.example.com") {
					return dr
				}
				return primary
			},
		})
		require.NoError(t, err)
		resp, err := NewControllerServer(d).ListVolumes(context.Background(), &csi.ListVolumesRequest{})
		require.NoError(t, err)
		var ids []string
		for _, e := range resp.GetEntries() {
			ids = append(ids, e.GetVolume().GetVolumeId())
		}
		return ids, dialled
	}

	want := []string{primaryHandle, drHandle, ownHandle}
	ids, dialled := listing(true)
	assert.ElementsMatch(t, want, ids, "the DR copy's PV is listed though the DR array is walked; the orphan replica is not")
	assert.Contains(t, dialled, baseURLFor("array-b.example.com")+" token-b", "with the key the PV's class keys by the DR array")

	ids, dialled = listing(false)
	assert.ElementsMatch(t, want, ids, "no key: confirmed with the walking class's credential")
	for _, d := range dialled {
		assert.NotEqual(t, baseURLFor("array-b.example.com")+" token-a", d, "the primary's token is never sent to the DR array")
	}
}

// countingRefLister counts its PV listings.
type countingRefLister struct {
	fakeRefLister
	lists int
}

func (c *countingRefLister) ListVolumeRefs(ctx context.Context, driver string) ([]VolumeRef, error) {
	c.lists++
	return c.fakeRefLister.ListVolumeRefs(ctx, driver)
}

// Every ListVolumes page listed every PersistentVolume in the
// cluster, so a paged walk listed them once per page. The first page of a
// walk reads them; the pages after it, which carry its starting_token,
// reuse that read for up to a minute, so one walk also sees one PV set. A
// new walk reads afresh.
func TestAPagedListVolumesWalkListsPVsOnce(t *testing.T) {
	client := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return dirEntries(map[string]string{"pvc-1": "101", "pvc-2": "102", "pvc-3": "103"}), nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
	lister := &countingRefLister{}
	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "0.1.0",
		SecretReader:       &fakeSecretReader{secrets: map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}}},
		StorageClassLister: &fakeClassLister{classes: []StorageClassInfo{classFor("a", "qumulo-a.example.com", "/csi/a", "tok")}},
		VolumeHandleLister: lister,
		NewClient:          func(string, string) qumulo.ClientInterface { return client },
	})
	require.NoError(t, err)
	srv := NewControllerServer(d)

	token := ""
	for page := 0; ; page++ {
		resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 1, StartingToken: token})
		require.NoError(t, err)
		require.Len(t, resp.GetEntries(), 1, "page %d", page)
		if token = resp.GetNextToken(); token == "" {
			break
		}
	}
	assert.Equal(t, 1, lister.lists, "a three-page walk lists PVs once")

	_, err = srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	assert.Equal(t, 2, lister.lists, "a new walk lists them afresh")
}

// Every page of a walk listed every StorageClass; the pages after the
// first now reuse the first page's list, as they reuse its PV list.
// Secrets are still read on every call.
func TestAPagedListVolumesWalkListsClassesOnce(t *testing.T) {
	client := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return dirEntries(map[string]string{"pvc-1": "101", "pvc-2": "102", "pvc-3": "103"}), nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
	classes := &fakeClassLister{classes: []StorageClassInfo{classFor("a", "qumulo-a.example.com", "/csi/a", "tok")}}
	secrets := &fakeSecretReader{secrets: map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}}}
	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "0.1.0",
		SecretReader:       secrets,
		StorageClassLister: classes,
		VolumeHandleLister: &countingRefLister{},
		NewClient:          func(string, string) qumulo.ClientInterface { return client },
	})
	require.NoError(t, err)
	srv := NewControllerServer(d)

	token, pages := "", 0
	for ; ; pages++ {
		resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 1, StartingToken: token})
		require.NoError(t, err)
		if token = resp.GetNextToken(); token == "" {
			break
		}
	}
	require.Equal(t, 2, pages, "three pages")
	assert.Equal(t, 1, classes.calls, "a three-page walk lists StorageClasses once")
	assert.Len(t, secrets.calls, 3, "but each page still reads the class's Secret")

	_, err = srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	assert.Equal(t, 2, classes.calls, "a new walk lists them afresh")

	_, err = srv.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: "qumulo-a.example.com##/csi/a/pvc-1"})
	require.NoError(t, err)
	assert.Equal(t, 3, classes.calls, "ControllerGetVolume always reads afresh")
}
