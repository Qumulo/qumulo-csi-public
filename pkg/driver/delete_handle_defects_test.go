//go:build defects

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
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// dfxDestructiveCallLog wraps fakeCluster and records every call that removes
// something: tree deletes (by request body), export deletes and quota deletes
// (by ID).
type dfxDestructiveCallLog struct {
	*fakeCluster

	mu          sync.Mutex
	treeDeletes []string
	exportDels  []string
	quotaDels   []string
}

func (l *dfxDestructiveCallLog) handler() http.HandlerFunc {
	inner := l.fakeCluster.handler()
	return func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.EscapedPath()
		l.mu.Lock()
		defer l.mu.Unlock()
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
		inner(w, r)
	}
}

// dfxStoreWithTwoVolumes is a shared-subdirectory store: the store root
// /csi-volumes (file ID 10) with its quota and the covering export on it, and
// two volumes beneath it, belonging to two different claims.
func dfxStoreWithTwoVolumes() *dfxDestructiveCallLog {
	c := newFakeCluster()
	c.seedSharedExport()
	c.directories["/csi-volumes"] = "10"
	c.directories["/csi-volumes/pvc-mine"] = "11"
	c.directories["/csi-volumes/pvc-other"] = "12"
	c.quotas["10"] = "1099511627776"
	c.quotas["11"] = "1073741824"
	c.quotas["12"] = "1073741824"
	return &dfxDestructiveCallLog{fakeCluster: c}
}

func dfxAssertNothingRemoved(t *testing.T, l *dfxDestructiveCallLog) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	assert.Empty(t, l.treeDeletes, "a directory was tree-deleted")
	assert.Empty(t, l.exportDels, "an export was deleted")
	assert.Empty(t, l.quotaDels, "a quota was deleted")
	for _, p := range []string{"/csi-volumes", "/csi-volumes/pvc-mine", "/csi-volumes/pvc-other"} {
		_, ok := l.directories[p]
		assert.True(t, ok, "%s is gone", p)
	}
	_, ok := l.exports["csi-store"]
	assert.True(t, ok, "the shared covering export is gone")
}

func dfxDeleteThroughHandle(t *testing.T, handle string) {
	t.Helper()
	l := dfxStoreWithTwoVolumes()
	server := httptest.NewServer(l.handler())
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})
	l.mu.Lock()
	t.Logf("DeleteVolume(%q) answered %v; tree deletes %v, exports deleted %v, quotas deleted %v",
		handle, err, l.treeDeletes, l.exportDels, l.quotaDels)
	l.mu.Unlock()
	dfxAssertNothingRemoved(t, l)
}

// DeleteVolume must remove nothing for a handle "<volume>/.." that
// climbs to the class's store root.
// path and refuses only "/"; controller_server.go resolveVolumePath checks
// only "is a directory".
func TestDefect_DeleteVolumeRemovesNothingForAHandleThatClimbsToTheStoreRoot(t *testing.T) {
	dfxDeleteThroughHandle(t, testServer+"##/csi-volumes/pvc-mine/..")
}

// DeleteVolume must remove nothing for a handle
// "<volume>/../<other>" that names another claim's volume.
func TestDefect_DeleteVolumeRefusesAHandleNamingASibling(t *testing.T) {
	dfxDeleteThroughHandle(t, testServer+"##/csi-volumes/pvc-mine/../pvc-other")
}

// DeleteVolume must remove nothing for a handle with a "/./"
// element that normalises to the store root.
func TestDefect_DeleteVolumeRemovesNothingForADotHandle(t *testing.T) {
	for _, h := range []string{
		testServer + "##/csi-volumes/./",
		testServer + "##/csi-volumes/./pvc-mine/./..",
	} {
		t.Run(h, func(t *testing.T) { dfxDeleteThroughHandle(t, h) })
	}
}

// DeleteVolume must remove nothing for a clean handle naming the
// class's store root, which no CreateVolume ever minted: the directory is not
// this driver's volume, and the covering export on it is not the volume's own.
// driver's volume for this name"; "delete an export only if it is the volume's own").
func TestDefect_DeleteVolumeRemovesNothingForACleanHandleNamingTheStoreRoot(t *testing.T) {
	dfxDeleteThroughHandle(t, testServer+"##/csi-volumes")
}

// DeleteVolume must refuse a handle whose endpoint is http://,
// before building a client that carries the class's token.
func TestDefect_DeleteVolumeRefusesACleartextEndpointInTheHandle(t *testing.T) {
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

// The same for a snapshot handle: DeleteSnapshot must not send the
// token to an http:// endpoint named in a VolumeSnapshotContent.
func TestDefect_DeleteSnapshotRefusesACleartextEndpointInTheHandle(t *testing.T) {
	var dialled []string
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test",
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			dialled = append(dialled, baseURL)
			return &MockClient{}
		}})
	require.NoError(t, err)

	_, err = NewControllerServer(d).DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: testServer + "#http://192.0.2.99:80#4242",
		Secrets:    testSecrets(),
	})
	t.Logf("DeleteSnapshot answered %v; clients built for %v", err, dialled)
	for _, u := range dialled {
		assert.False(t, strings.HasPrefix(u, "http://"), "a client carrying the token was built for %s", u)
	}
	if c := status.Code(err); c != codes.InvalidArgument && c != codes.FailedPrecondition {
		t.Errorf("DeleteSnapshot of an http:// handle answered %v; want InvalidArgument or FailedPrecondition", c)
	}
}
