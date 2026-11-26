// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// fakeClassLister serves StorageClasses from a slice.
type fakeClassLister struct {
	classes []StorageClassInfo
	err     error
	calls   int
}

func (f *fakeClassLister) ListStorageClasses(
	_ context.Context,
	_ string,
) ([]StorageClassInfo, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.classes, nil
}

// classFor builds the parameter map the chart renders for one class.
func classFor(name, server, basePath, secret string) StorageClassInfo {
	return StorageClassInfo{
		Name: name,
		Parameters: map[string]string{
			paramServer:                       server,
			paramStoreRealPath:                basePath,
			paramStoreExportPath:              basePath,
			coParamProvisionerSecretName:      secret,
			coParamProvisionerSecretNamespace: "qumulo-csi",
		},
	}
}

// inventoryServer wires a controller with a class lister, a secret reader, and
// one Qumulo client per cluster address.
func inventoryServer(
	t *testing.T,
	lister StorageClassLister,
	secrets map[string]map[string][]byte,
	clients map[string]*MockClient,
) *ControllerServer {
	t.Helper()

	d, err := NewDriver(Config{
		Name:               "csi.qumulo.com",
		Version:            "0.1.0",
		SecretReader:       &fakeSecretReader{secrets: secrets},
		StorageClassLister: lister,
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			if c, ok := clients[baseURL]; ok {
				return c
			}
			t.Fatalf("no fake client for %q", baseURL)
			return nil
		},
	})
	require.NoError(t, err)

	return NewControllerServer(d)
}

// dirEntries builds a directory listing of subdirectories.
func dirEntries(names map[string]string) []qumulo.DirEntry {
	out := make([]qumulo.DirEntry, 0, len(names))
	for name, id := range names {
		out = append(out, qumulo.DirEntry{Name: name, ID: id, Type: "FS_FILE_TYPE_DIRECTORY"})
	}
	return out
}

func baseURLFor(server string) string { return "https://" + server + ":8000" }

// TestListVolumesWithoutInventoryIsFailedPrecondition covers the default
// deployment: the capability is advertised, so the CO may call, and the answer
// must name the value that turns it on rather than looking like a bug.
func TestListVolumesWithoutInventoryIsFailedPrecondition(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})

	_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "controller.inventory.enabled")

	_, err = srv.ControllerGetVolume(context.Background(),
		&csi.ControllerGetVolumeRequest{VolumeId: "qumulo-a.example.com##/csi/a/pvc-a"})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestListVolumesSpansEveryClusterItsClassesName is the feature: one release
// serving two arrays reports both, each read with its own credential.
func TestListVolumesSpansEveryClusterItsClassesName(t *testing.T) {
	tokensUsed := map[string]string{}

	clientA := &MockClient{
		ListDirectoryFunc: func(_ context.Context, path, _ string) ([]qumulo.DirEntry, error) {
			require.Equal(t, "/csi/a", path)
			return dirEntries(map[string]string{"pvc-1": "101", "pvc-2": "102"}), nil
		},
		GetQuotaFunc: func(_ context.Context, id string) (uint64, error) { return 1 << 30, nil },
	}
	clientB := &MockClient{
		ListDirectoryFunc: func(_ context.Context, path, _ string) ([]qumulo.DirEntry, error) {
			require.Equal(t, "/csi/b", path)
			return dirEntries(map[string]string{"pvc-3": "203"}), nil
		},
		GetQuotaFunc: func(_ context.Context, id string) (uint64, error) { return 2 << 30, nil },
	}

	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		SecretReader: &fakeSecretReader{secrets: map[string]map[string][]byte{
			"qumulo-csi/tok-a": {secretKeyToken: []byte("secret-a")},
			"qumulo-csi/tok-b": {secretKeyToken: []byte("secret-b")},
		}},
		StorageClassLister: &fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok-a"),
			classFor("class-b", "qumulo-b.example.com", "/csi/b", "tok-b"),
		}},
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			tokensUsed[baseURL] = token
			if baseURL == baseURLFor("qumulo-a.example.com") {
				return clientA
			}
			return clientB
		},
	})
	require.NoError(t, err)
	srv := NewControllerServer(d)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)

	got := make([]string, 0, len(resp.GetEntries()))
	for _, e := range resp.GetEntries() {
		got = append(got, e.GetVolume().GetVolumeId())
	}

	// Sorted by server then path, which is also what makes the offset token
	// safe.
	require.Equal(t, []string{
		"qumulo-a.example.com##/csi/a/pvc-1",
		"qumulo-a.example.com##/csi/a/pvc-2",
		"qumulo-b.example.com##/csi/b/pvc-3",
	}, got)

	// Each cluster read with its own class's credential, never the other's.
	require.Equal(t, map[string]string{
		baseURLFor("qumulo-a.example.com"): "secret-a",
		baseURLFor("qumulo-b.example.com"): "secret-b",
	}, tokensUsed)

	// PublishedNodeIds must stay empty: there is no attach state to report and
	// LIST_VOLUMES_PUBLISHED_NODES is not advertised.
	for _, e := range resp.GetEntries() {
		require.Empty(t, e.GetStatus().GetPublishedNodeIds())
	}
}

// TestListVolumesSkipsDirectoriesWithoutAQuota is the ownership test. A
// directory with no quota was not provisioned by this driver -- or was left by
// a provision that failed before setting one -- and reporting it as a volume
// invites a reconciler to delete it.
func TestListVolumesSkipsDirectoriesWithoutAQuota(t *testing.T) {
	client := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return dirEntries(map[string]string{
				"pvc-real":     "101",
				"someone-else": "999",
			}), nil
		},
		GetQuotaFunc: func(_ context.Context, id string) (uint64, error) {
			if id == "999" {
				return 0, newTestNotFoundError()
			}
			return 1 << 30, nil
		},
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok"),
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): client},
	)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	require.Equal(t, "qumulo-a.example.com##/csi/a/pvc-real", resp.GetEntries()[0].GetVolume().GetVolumeId())
}

// TestListVolumesFailsWhenAClusterCannotBeListed is the partial-listing rule,
// and it is the one behaviour here that is silent if it regresses.
//
// Returning cluster A's volumes while cluster B is unreachable produces a
// listing that cannot be told apart from B's volumes having been deleted. A
// reconciler comparing PersistentVolumes against it may then delete live
// volumes, so no answer is safer than an incomplete one.
func TestListVolumesFailsWhenAClusterCannotBeListed(t *testing.T) {
	healthy := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return dirEntries(map[string]string{"pvc-1": "101"}), nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
	broken := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return nil, errors.New("connection refused")
		},
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok"),
			classFor("class-b", "qumulo-b.example.com", "/csi/b", "tok"),
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{
			baseURLFor("qumulo-a.example.com"): healthy,
			baseURLFor("qumulo-b.example.com"): broken,
		},
	)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.Error(t, err, "a listing missing one cluster must fail, not return the rest")
	require.Nil(t, resp)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, err.Error(), "qumulo-b.example.com")
	require.Contains(t, err.Error(), "partial listing")
}

// TestListVolumesTreatsAnAbsentStoreRootAsEmpty separates "the cluster is
// unreachable" from "nothing has been provisioned yet". The store root, or the
// domain directory under it, is created by the first CreateVolume.
func TestListVolumesTreatsAnAbsentStoreRootAsEmpty(t *testing.T) {
	client := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			// Core's answer for a missing directory; a bare 404 is not this
			// (TestListVolumesAForeign404IsUnavailable).
			return nil, &qumulo.APIError{StatusCode: http.StatusNotFound, Class: qumulo.ErrorClassNoSuchEntry}
		},
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok"),
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): client},
	)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.GetEntries())
}

// TestListVolumesDeduplicatesClassesSharingABasePath covers two classes that
// differ only in protocol: the same directory would otherwise be reported
// twice under one volume ID.
func TestListVolumesDeduplicatesClassesSharingABasePath(t *testing.T) {
	client := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return dirEntries(map[string]string{"pvc-1": "101"}), nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
	shared := classFor("class-nfs3", "qumulo-a.example.com", "/csi/a", "tok")
	shared.Parameters[paramProtocol] = protocolNFS3
	other := classFor("class-nfs41", "qumulo-a.example.com", "/csi/a", "tok")
	other.Parameters[paramProtocol] = protocolNFS41

	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{shared, other}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): client},
	)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1, "one directory must yield one entry, not one per class")
}

// TestListVolumesUsesTheDomainDirectory pins the adDomain half: a class with a
// domain provisions one level deeper, so that is the level to list.
func TestListVolumesUsesTheDomainDirectory(t *testing.T) {
	var listed string
	client := &MockClient{
		ListDirectoryFunc: func(_ context.Context, p, _ string) ([]qumulo.DirEntry, error) {
			listed = p
			return dirEntries(map[string]string{"pvc-1": "101"}), nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
	class := classFor("class-corp", "qumulo-a.example.com", "/csi/a", "tok")
	class.Parameters[paramADDomain] = "corp.example.com"

	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{class}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): client},
	)

	_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.Equal(t, "/csi/a/corp.example.com", listed)
}

// TestListVolumesPagesStablyAcrossClusters walks the whole listing one entry
// at a time and requires every volume exactly once.
//
// CSI's token is an offset, so an order that varies between calls silently
// duplicates and omits entries at a page boundary. Directory listings come
// back unordered and there is now more than one cluster, so this is the test
// that would catch a sort key that does not span both.
func TestListVolumesPagesStablyAcrossClusters(t *testing.T) {
	newClient := func(names map[string]string) *MockClient {
		return &MockClient{
			ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
				// Rebuilt per call from a map, so Go's randomized map order
				// gives a different sequence each time -- which is exactly the
				// condition the sort has to survive.
				return dirEntries(names), nil
			},
			GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
		}
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok"),
			classFor("class-b", "qumulo-b.example.com", "/csi/b", "tok"),
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{
			baseURLFor("qumulo-a.example.com"): newClient(map[string]string{
				"pvc-1": "101", "pvc-2": "102", "pvc-3": "103",
			}),
			baseURLFor("qumulo-b.example.com"): newClient(map[string]string{
				"pvc-4": "204", "pvc-5": "205",
			}),
		},
	)

	seen := map[string]int{}
	token := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, 20, "paging did not terminate")

		resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{
			MaxEntries:    1,
			StartingToken: token,
		})
		require.NoError(t, err)
		for _, e := range resp.GetEntries() {
			seen[e.GetVolume().GetVolumeId()]++
		}
		token = resp.GetNextToken()
		if token == "" {
			break
		}
	}

	require.Len(t, seen, 5, "every volume must appear, across pages")
	for id, count := range seen {
		require.Equal(t, 1, count, "volume %s appeared %d times across pages", id, count)
	}
}

// TestListVolumesInvalidStartingTokenIsAborted is the csi-sanity contract for
// paging: Aborted tells the CO to restart the listing, where InvalidArgument
// would tell it the request itself was wrong.
func TestListVolumesInvalidStartingTokenIsAborted(t *testing.T) {
	client := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return dirEntries(map[string]string{"pvc-1": "101"}), nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok"),
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): client},
	)

	_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{
		StartingToken: "not-a-number",
	})
	require.Error(t, err)
	require.Equal(t, codes.Aborted, status.Code(err))
}

// TestListVolumesRefusesWhenEveryClassIsMisconfigured keeps "no volumes" and
// "nothing could be read" distinguishable. A broken class is skipped so one
// bad class does not hide the others, but if that leaves nothing at all, an
// empty list would be a lie.
func TestListVolumesRefusesWhenEveryClassIsMisconfigured(t *testing.T) {
	noSecret := StorageClassInfo{
		Name: "class-broken",
		Parameters: map[string]string{
			paramServer:          "qumulo-a.example.com",
			paramStoreRealPath:   "/csi/a",
			paramStoreExportPath: "/csi/a",
		},
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{noSecret}},
		map[string]map[string][]byte{},
		map[string]*MockClient{},
	)

	_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "misconfigured")
}

// TestListVolumesSkipsOneBadClassButKeepsTheRest is the other half of the rule
// above: one broken class must not make every other class's volumes invisible.
func TestListVolumesSkipsOneBadClassButKeepsTheRest(t *testing.T) {
	client := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return dirEntries(map[string]string{"pvc-1": "101"}), nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
	broken := classFor("class-broken", "qumulo-b.example.com", "/csi/b", "missing-secret")

	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-good", "qumulo-a.example.com", "/csi/a", "tok"),
			broken,
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): client},
	)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	require.Equal(t, "qumulo-a.example.com##/csi/a/pvc-1", resp.GetEntries()[0].GetVolume().GetVolumeId())
}

// TestListVolumesWhenNoClassNamesThisDriver is a legitimate empty answer: the
// release has no StorageClasses, so it provisioned nothing.
func TestListVolumesWhenNoClassNamesThisDriver(t *testing.T) {
	srv := inventoryServer(t, &fakeClassLister{}, nil, nil)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.GetEntries())
}

// TestListVolumesApiFailureIsInternal separates a Kubernetes API failure from
// a misconfiguration: this one an operator cannot fix by editing a manifest.
func TestListVolumesApiFailureIsInternal(t *testing.T) {
	srv := inventoryServer(t,
		&fakeClassLister{err: errors.New("etcdserver: request timed out")}, nil, nil)

	_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

// TestControllerGetVolumeRoutesByServer is the multi-cluster property: a
// volume must be read from the cluster its own ID names, with that cluster's
// credential, never from another's.
func TestControllerGetVolumeRoutesByServer(t *testing.T) {
	clientA := &MockClient{
		// The handle names a path; this cluster resolves it to its own file
		// ID and the quota is read by that. Pinned to the path rather than to
		// an ID, because the ID is the array's to choose.
		LookupFileByPathFunc: volumeAt("/csi/a/pvc-1", "101"),
		GetQuotaFunc: func(_ context.Context, id string) (uint64, error) {
			require.Equal(t, "101", id)
			return 5 << 30, nil
		},
	}
	clientB := &MockClient{
		GetQuotaFunc: func(context.Context, string) (uint64, error) {
			t.Fatal("cluster B must not be contacted for a volume on cluster A")
			return 0, nil
		},
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok"),
			classFor("class-b", "qumulo-b.example.com", "/csi/b", "tok"),
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{
			baseURLFor("qumulo-a.example.com"): clientA,
			baseURLFor("qumulo-b.example.com"): clientB,
		},
	)

	resp, err := srv.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{
		VolumeId: "qumulo-a.example.com##/csi/a/pvc-1",
	})
	require.NoError(t, err)
	require.Equal(t, "qumulo-a.example.com##/csi/a/pvc-1", resp.GetVolume().GetVolumeId())
	require.Equal(t, int64(5<<30), resp.GetVolume().GetCapacityBytes())
	require.Empty(t, resp.GetStatus().GetPublishedNodeIds())
}

// TestControllerGetVolumeWithNoMatchingClass must not report NotFound.
//
// The volume may well still exist; what is missing is a route to its cluster,
// most likely because the class it was provisioned from has been deleted.
// Answering NotFound would tell a reconciler the volume is gone.
func TestControllerGetVolumeWithNoMatchingClass(t *testing.T) {
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok"),
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): {}},
	)

	_, err := srv.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{
		VolumeId: "qumulo-gone.example.com##/csi/a/pvc-x",
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err),
		"a volume with no route is not a volume that is gone")
	require.Contains(t, err.Error(), "qumulo-gone.example.com")
}

// TestControllerGetVolumeMissingQuotaIsNotFound is the case that *is* NotFound:
// the cluster answered and the volume's quota is not there.
func TestControllerGetVolumeMissingQuotaIsNotFound(t *testing.T) {
	client := &MockClient{
		GetQuotaFunc: func(context.Context, string) (uint64, error) {
			return 0, newTestNotFoundError()
		},
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "qumulo-a.example.com", "/csi/a", "tok"),
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): client},
	)

	_, err := srv.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{
		VolumeId: "qumulo-a.example.com##/csi/a/pvc-1",
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

// TestControllerGetVolumeMalformedIDIsNotFound: an ID this driver could not
// have issued names no volume of its.
func TestControllerGetVolumeMalformedIDIsNotFound(t *testing.T) {
	srv := inventoryServer(t, &fakeClassLister{}, nil, nil)

	for _, id := range []string{"nonsense", "a/b", "server/not-decimal/name"} {
		t.Run(id, func(t *testing.T) {
			_, err := srv.ControllerGetVolume(context.Background(),
				&csi.ControllerGetVolumeRequest{VolumeId: id})
			require.Error(t, err)
			require.Equal(t, codes.NotFound, status.Code(err))
		})
	}

	_, err := srv.ControllerGetVolume(context.Background(),
		&csi.ControllerGetVolumeRequest{VolumeId: ""})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestControllerGetVolumeChoosesDeterministicallyAmongClasses covers the
// ambiguity of two classes naming one server with different Secrets.
// Matching on the volume's path would disambiguate, but resolving the path
// needs a credential first, so the first class by name wins -- and it must be
// the same one on every call.
func TestControllerGetVolumeChoosesDeterministicallyAmongClasses(t *testing.T) {
	used := map[string]int{}
	client := &MockClient{
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}

	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		SecretReader: &fakeSecretReader{secrets: map[string]map[string][]byte{
			"qumulo-csi/tok-alpha": {secretKeyToken: []byte("alpha")},
			"qumulo-csi/tok-zulu":  {secretKeyToken: []byte("zulu")},
		}},
		StorageClassLister: &fakeClassLister{classes: []StorageClassInfo{
			// Returned zulu-first, so the sort has something to do.
			classFor("zulu", "qumulo-a.example.com", "/csi/z", "tok-zulu"),
			classFor("alpha", "qumulo-a.example.com", "/csi/a", "tok-alpha"),
		}},
		NewClient: func(_ string, token string) qumulo.ClientInterface {
			used[token]++
			return client
		},
	})
	require.NoError(t, err)
	srv := NewControllerServer(d)

	for range 3 {
		_, err := srv.ControllerGetVolume(context.Background(),
			&csi.ControllerGetVolumeRequest{VolumeId: "qumulo-a.example.com##/csi/a/pvc-1"})
		require.NoError(t, err)
	}

	require.Positive(t, used["alpha"], "the first class by name must be the one used")
	require.Zero(t, used["zulu"], fmt.Sprintf("choice was not deterministic: %v", used))
}

// TestControllerGetVolumeMatchesServerCaseInsensitively: a hostname is
// case-insensitive, so a class spelled with capitals must still match a volume
// ID that recorded it in lower case.
func TestControllerGetVolumeMatchesServerCaseInsensitively(t *testing.T) {
	client := &MockClient{
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-a", "Qumulo-A.example.com", "/csi/a", "tok"),
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{baseURLFor("Qumulo-A.example.com"): client},
	)

	_, err := srv.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{
		VolumeId: "qumulo-a.example.com##/csi/a/pvc-1",
	})
	require.NoError(t, err)
}

// TestListVolumesSkipsReplicas is the ownership condition the DR work added:
// inventory must skip replicas.
//
// A replica looks exactly like a driver-owned volume to this listing: it sits
// one level under the class's store root, it is a directory, and
// EnableVolumeReplication pre-stages it a quota while it is still a read-only
// target -- and a quota is what the listing takes as proof the driver
// provisioned the directory. So without the relationship table the DR array
// reports the other cluster's volumes as its own, and a reconciler acting on
// the difference deletes the DR copies.
func TestListVolumesSkipsReplicas(t *testing.T) {
	const server = "qumulo-dr.example.com"

	tests := []struct {
		name    string
		targets []qumulo.RelationshipStatus
		want    []string
	}{
		{
			name: "no relationships, everything is this cluster's",
			want: []string{server + "##/csi/dr/pvc-local", server + "##/csi/dr/pvc-replica", server + "##/csi/dr/pvc-shared"},
		},
		{
			name: "a dedicated-export replica is skipped, its neighbours are not",
			targets: []qumulo.RelationshipStatus{
				{ID: "rel-1", State: qumulo.RelationshipEstablished, TargetRootPath: "/csi/dr/pvc-replica"},
			},
			want: []string{server + "##/csi/dr/pvc-local", server + "##/csi/dr/pvc-shared"},
		},
		{
			name: "a group relationship on the class basePath hides every volume under it",
			targets: []qumulo.RelationshipStatus{
				{ID: "rel-2", State: qumulo.RelationshipEstablished, TargetRootPath: "/csi/dr"},
			},
			want: nil,
		},
		{
			name: "an ENDED relationship names no live replica",
			targets: []qumulo.RelationshipStatus{
				{ID: "rel-3", State: qumulo.RelationshipEnded, TargetRootPath: "/csi/dr/pvc-replica"},
			},
			want: []string{server + "##/csi/dr/pvc-local", server + "##/csi/dr/pvc-replica", server + "##/csi/dr/pvc-shared"},
		},
		{
			name:    "a relationship this cluster is the *source* of protects a volume it still owns",
			targets: nil,
			want:    []string{server + "##/csi/dr/pvc-local", server + "##/csi/dr/pvc-replica", server + "##/csi/dr/pvc-shared"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &MockClient{
				ListDirectoryFunc: func(_ context.Context, path, _ string) ([]qumulo.DirEntry, error) {
					return dirEntries(map[string]string{
						"pvc-local": "101", "pvc-replica": "102", "pvc-shared": "103",
					}), nil
				},
				GetQuotaFunc: func(_ context.Context, id string) (uint64, error) { return 1 << 30, nil },
				ListTargetStatusFunc: func(context.Context) ([]qumulo.RelationshipStatus, error) {
					return tt.targets, nil
				},
			}
			srv := inventoryServer(t,
				&fakeClassLister{classes: []StorageClassInfo{classFor("dr", server, "/csi/dr", "tok")}},
				map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("s")}},
				map[string]*MockClient{baseURLFor(server): client},
			)

			resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
			require.NoError(t, err)

			got := make([]string, 0, len(resp.GetEntries()))
			for _, e := range resp.GetEntries() {
				got = append(got, e.GetVolume().GetVolumeId())
			}
			assert.Equal(t, tt.want, nilIfEmpty(got))
		})
	}
}

func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}

// A relationship table that cannot be read fails the call, exactly as a
// directory that cannot be listed does: "I could not tell whether these are
// replicas" must not be answerable with a list that contains them.
func TestListVolumesRefusesWhenReplicasCannotBeIdentified(t *testing.T) {
	const server = "qumulo-dr.example.com"

	for _, tt := range []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "a token without the replication read privilege",
			err:      &qumulo.APIError{StatusCode: 403, Path: "/v2/replication"},
			wantCode: codes.FailedPrecondition,
			wantMsg:  "PRIVILEGE_REPLICATION_SOURCE_READ",
		},
		{
			name:     "an array that did not answer",
			err:      errors.New("dial tcp: connection refused"),
			wantCode: codes.Unavailable,
			wantMsg:  "unreachable",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &MockClient{
				ListDirectoryFunc: func(_ context.Context, path, _ string) ([]qumulo.DirEntry, error) {
					return dirEntries(map[string]string{"pvc-1": "101"}), nil
				},
				ListTargetStatusFunc: func(context.Context) ([]qumulo.RelationshipStatus, error) {
					return nil, tt.err
				},
			}
			srv := inventoryServer(t,
				&fakeClassLister{classes: []StorageClassInfo{classFor("dr", server, "/csi/dr", "tok")}},
				map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("s")}},
				map[string]*MockClient{baseURLFor(server): client},
			)

			_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err))
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}

// The store root's listing took any 404 as "not created yet" and
// skipped the class. A 404 that is not the cluster's file not-found -- an
// unknown route, or a proxy answering for the API -- then answered OK with
// that class's volumes silently missing, which an inventory tool reads as
// deleted. It is Unavailable, like every other listing failure here.
func TestListVolumesAForeign404IsUnavailable(t *testing.T) {
	foreign := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return nil, fakeAPIError(http.StatusNotFound, "http_not_found_error", "GET /v1/files/...")
		},
	}
	srv := inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("a", "qumulo-a.example.com", "/csi/a", "tok"),
			classFor("b", "qumulo-b.example.com", "/csi/b", "tok"),
		}},
		oneToken,
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): foreign, baseURLFor("qumulo-b.example.com"): oneVolumeAt("pvc-2", "202")},
	)

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})

	requireCode(t, err, codes.Unavailable)
	assert.Empty(t, resp.GetEntries(), "no partial listing")
}

// Every class's Secret was read on every ListVolumes call, even when
// every class named the same one: a 2,010-volume walk at 100 entries a page
// made 630 Secret GETs. A Secret is now read once per call however many
// classes name it, and once per call for each distinct one.
func TestListVolumesReadsEachSecretOncePerCall(t *testing.T) {
	client := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return dirEntries(map[string]string{"pvc-1": "101"}), nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
	var classes []StorageClassInfo
	for i := range 6 {
		secret := "shared"
		if i == 5 {
			secret = "other"
		}
		classes = append(classes, classFor(fmt.Sprintf("class-%d", i), "qumulo-a.example.com",
			fmt.Sprintf("/csi/c%d", i), secret))
	}
	srv := inventoryServer(t, &fakeClassLister{classes: classes},
		map[string]map[string][]byte{
			"qumulo-csi/shared": {secretKeyToken: []byte("t")},
			"qumulo-csi/other":  {secretKeyToken: []byte("u")},
		},
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): client},
	)

	_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	reader := srv.driver.SecretReader.(*fakeSecretReader)
	assert.ElementsMatch(t, []string{"qumulo-csi/shared", "qumulo-csi/other"}, reader.calls,
		"six classes naming two Secrets read each once")

	_, err = srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	assert.Len(t, reader.calls, 4, "a later call reads them again, so a rotated Secret takes effect")
}
