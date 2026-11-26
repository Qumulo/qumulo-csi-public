// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// destructiveCallLog wraps fakeCluster and records every call that removes
// something: tree deletes (by request body), export deletes and quota
// deletes (by ID).
type destructiveCallLog struct {
	*fakeCluster

	mu          sync.Mutex
	treeDeletes []string
	exportDels  []string
	quotaDels   []string
}

func (l *destructiveCallLog) handler() http.HandlerFunc {
	inner := l.fakeCluster.handler()
	return func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.EscapedPath()
		l.mu.Lock()
		switch {
		case p == "/v1/tree-delete/jobs/" && r.Method == http.MethodPost:
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(r.Body)
			l.treeDeletes = append(l.treeDeletes, buf.String())
			r.Body = io.NopCloser(strings.NewReader(buf.String()))
		case strings.HasPrefix(p, "/v3/nfs/exports/") && r.Method == http.MethodDelete:
			l.exportDels = append(l.exportDels, strings.TrimPrefix(p, "/v3/nfs/exports/"))
		case strings.HasPrefix(p, "/v1/files/quotas/") && r.Method == http.MethodDelete:
			l.quotaDels = append(l.quotaDels, strings.TrimPrefix(p, "/v1/files/quotas/"))
		}
		l.mu.Unlock()
		inner(w, r)
	}
}

// storeWithTwoVolumes is a shared-subdirectory store: the store root
// /csi-volumes (file ID 10) with its quota and the covering export on it, and
// two volumes beneath it, belonging to two different claims.
func storeWithTwoVolumes() *destructiveCallLog {
	c := newFakeCluster()
	c.seedSharedExport()
	c.directories["/csi-volumes"] = "10"
	c.directories["/csi-volumes/pvc-mine"] = "11"
	c.directories["/csi-volumes/pvc-other"] = "12"
	c.quotas["10"] = "1099511627776"
	c.quotas["11"] = "1073741824"
	c.quotas["12"] = "1073741824"
	return &destructiveCallLog{fakeCluster: c}
}

func assertNothingRemoved(t *testing.T, l *destructiveCallLog) {
	t.Helper()
	assert.Empty(t, l.treeDeletes, "a directory was tree-deleted")
	assert.Empty(t, l.exportDels, "an export was deleted")
	assert.Empty(t, l.quotaDels, "a quota was deleted")
	for _, p := range []string{"/csi-volumes", "/csi-volumes/pvc-mine", "/csi-volumes/pvc-other"} {
		_, ok := l.directories[p]
		assert.True(t, ok, "%s is gone", p)
	}
}

// DeleteVolume must remove nothing for a handle "<volume>/.." that climbs to the class's store root.
// The handle used to be cleaned and acted on, deleting the store root, its
// quota and the shared covering export.
func TestDeleteVolumeRemovesNothingForAHandleThatClimbsToTheStoreRoot(t *testing.T) {
	l := storeWithTwoVolumes()
	server := httptest.NewServer(l.handler())
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: testServer + "##/csi-volumes/pvc-mine/..", Secrets: testSecrets()})
	t.Logf("DeleteVolume answered %v; tree deletes %v, exports deleted %v, quotas deleted %v",
		err, l.treeDeletes, l.exportDels, l.quotaDels)
	assertNothingRemoved(t, l)
}

// DeleteVolume must remove nothing for a handle "<volume>/../<other>" that names another claim's volume.
// The cleaned path named claim B's volume, which was dismantled while B was Bound.
func TestDeleteVolumeRemovesNothingForAHandleThatNamesASiblingVolume(t *testing.T) {
	l := storeWithTwoVolumes()
	server := httptest.NewServer(l.handler())
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: testServer + "##/csi-volumes/pvc-mine/../pvc-other", Secrets: testSecrets()})
	t.Logf("DeleteVolume answered %v; tree deletes %v, exports deleted %v, quotas deleted %v",
		err, l.treeDeletes, l.exportDels, l.quotaDels)
	assertNothingRemoved(t, l)
}

// The same climb spelled with "." reaches the store root as surely as "..".
func TestDeleteVolumeRefusesAHandleWithADotElement(t *testing.T) {
	l := storeWithTwoVolumes()
	server := httptest.NewServer(l.handler())
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: testServer + "##/csi-volumes/./", Secrets: testSecrets()})
	assertCode(t, err, codes.InvalidArgument)
	assertNothingRemoved(t, l)
}

// A clean handle that names the store root itself is refused: the covering
// export on it is the operator's, not this volume's.
func TestDeleteVolumeRefusesAHandleNamingTheStoreRoot(t *testing.T) {
	l := storeWithTwoVolumes()
	server := httptest.NewServer(l.handler())
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: testServer + "##/csi-volumes", Secrets: testSecrets()})
	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "not this volume's")
	assertNothingRemoved(t, l)
}

// A directory that holds another volume's dedicated export is a parent of
// volumes, not a volume, even with a quota on it.
func TestDeleteVolumeRefusesADirectoryHoldingAnotherVolumesExport(t *testing.T) {
	c := newFakeCluster()
	c.directories["/data"] = "30"
	c.directories["/data/pvc-x"] = "31"
	c.quotas["30"] = "1073741824"
	c.quotas["31"] = "1073741824"
	c.exports["e31"] = fakeExport{ExportPath: "/pvc-x", FSPath: "/data/pvc-x", Description: exportDescriptionFor("pvc-x")}
	l := &destructiveCallLog{fakeCluster: c}
	server := httptest.NewServer(l.handler())
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: testServer + "##/data", Secrets: testSecrets()})
	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "another volume's export")
	assert.Empty(t, l.treeDeletes)
	assert.Empty(t, l.exportDels)
	assert.Empty(t, l.quotaDels)
}

// A directory with neither this volume's export nor a quota was not made by
// this driver: a hand-written handle at someone's data is refused.
func TestDeleteVolumeRefusesADirectoryWithNoExportAndNoQuota(t *testing.T) {
	c := newFakeCluster()
	c.directories["/home/alice"] = "40"
	l := &destructiveCallLog{fakeCluster: c}
	server := httptest.NewServer(l.handler())
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: testServer + "##/home/alice", Secrets: testSecrets()})
	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "neither this volume's export nor a quota")
	assert.Empty(t, l.treeDeletes)
	assert.Contains(t, c.directories, "/home/alice")
}

// DeleteVolume must refuse a directory that holds a replication target, as it refuses the target itself.
// The guard matched a relationship only at or above the path, so the
// replica's parent was tree-deleted and the relationship left ENDED.
func TestDeleteVolumeRefusesADirectoryHoldingAReplica(t *testing.T) {
	var treeDeleted []string
	client := &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			ids := map[string]string{"/csi-volumes/group-a": "20", "/csi-volumes/group-a/pvc-replica": "21"}
			if id, ok := ids[p]; ok {
				return qumulo.FileInfo{ID: id, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			}
			return missingVolume()(ctx, p)
		},
		ListTargetStatusFunc: func(context.Context) ([]qumulo.RelationshipStatus, error) {
			return []qumulo.RelationshipStatus{{
				ID: "rel-1", State: "ESTABLISHED",
				TargetRootPath: "/csi-volumes/group-a/pvc-replica", TargetRootReadOnly: true,
			}}, nil
		},
		StartTreeDeleteFunc: func(_ context.Context, id string) error {
			treeDeleted = append(treeDeleted, id)
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	// The replica itself is refused.
	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: testServer + "##/csi-volumes/group-a/pvc-replica", Secrets: testSecrets()})
	assertCode(t, err, codes.FailedPrecondition)
	require.Empty(t, treeDeleted)

	// The directory that holds it must be refused the same way.
	_, err = srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: testServer + "##/csi-volumes/group-a", Secrets: testSecrets()})
	t.Logf("DeleteVolume of the replica's parent answered %v; tree deletes %v", err, treeDeleted)
	assert.Empty(t, treeDeleted, "the directory holding a live replica was tree-deleted")
	assertCode(t, err, codes.FailedPrecondition)
}

// A protected source below the directory goes with the tree delete too, and
// the delete policy is no consent for it: "disable" names this volume's own
// relationship, not every one beneath it.
func TestDeleteVolumeRefusesADirectoryHoldingAProtectedSourceWhateverThePolicy(t *testing.T) {
	var treeDeleted []string
	client := &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			ids := map[string]string{"/csi-volumes/group-a": "20", "/csi-volumes/group-a/pvc-source": "21"}
			if id, ok := ids[p]; ok {
				return qumulo.FileInfo{ID: id, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			}
			return missingVolume()(ctx, p)
		},
		ListSourceStatusFunc: func(context.Context) ([]qumulo.RelationshipStatus, error) {
			return []qumulo.RelationshipStatus{{
				ID: "rel-2", State: "ESTABLISHED", SourceRootPath: "/csi-volumes/group-a/pvc-source",
			}}, nil
		},
		StartTreeDeleteFunc: func(_ context.Context, id string) error {
			treeDeleted = append(treeDeleted, id)
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	secrets := testSecrets()
	secrets[paramReplicationDeletePolicy] = replicationDeletePolicyDisable
	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: testServer + "##/csi-volumes/group-a", Secrets: secrets})
	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "rel-2")
	assert.Contains(t, err.Error(), "the protected source")
	assert.Empty(t, treeDeleted)
}

// DeleteVolume must refuse a handle whose endpoint is http://, before building a client that carries the token.
// The token used to be sent there in cleartext.
func TestDeleteVolumeRefusesACleartextEndpointInTheHandle(t *testing.T) {
	var dialled []string
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test",
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			dialled = append(dialled, baseURL)
			return &MockClient{LookupFileByPathFunc: missingVolume()}
		}})
	require.NoError(t, err)
	srv := NewControllerServer(d)

	_, err = srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: testServer + "#http://192.0.2.99:80#/csi-volumes/pvc-mine",
		Secrets:  testSecrets(),
	})
	t.Logf("DeleteVolume answered %v; clients built for %v", err, dialled)
	for _, u := range dialled {
		assert.False(t, strings.HasPrefix(u, "http://"), "a client carrying the token was built for %s", u)
	}
	assertCode(t, err, codes.InvalidArgument)
}

// The class side of: a class whose apiUrl is http:// would mint handles
// that every later RPC refuses, and send the token in cleartext meanwhile, so
// it is refused up front, as is a replication class's peer endpoint.
func TestAnHTTPEndpointIsRefusedForAClassAndAPeer(t *testing.T) {
	_, err := parseVolumeParams(map[string]string{
		"server": testServer, "apiUrl": "http://192.0.2.10:8000", "storeRealPath": "/csi", "storeExportPath": "/csi",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https://")

	_, err = parseReplicationParams(map[string]string{
		"targetServer": "192.0.2.20", "targetApiUrl": "http://192.0.2.20:8000",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https://")
}

// The peer is sent only the token keyed by the host actually dialled.
// A targetApiUrl on another host than targetServer used to be sent
// targetServer's token.
func TestThePeerIsSentOnlyItsOwnHostsToken(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})
	params := replicationParams{TargetServer: "192.0.2.20", TargetAPIURL: "https://192.0.2.30:8000"}

	_, err := srv.peerReplicationClient("192.0.2.20", params, map[string]string{"192.0.2.20": "token-for-20"})
	require.ErrorIs(t, err, errNoPeerCredential)
}
