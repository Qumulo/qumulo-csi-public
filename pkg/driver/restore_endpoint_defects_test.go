//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// A restore must read the snapshot on the array it was taken on,
// and refuse when the class reaches another. Snapshot IDs are per-array
// numbers, so the dangerous case is the one where the class's array holds a
// snapshot with the same number: reading it copies someone else's data and
// (controller_server.go, sameEndpoint).

// dfxD03Restore is a restore of snapshotID through a class with the given
// apiUrl ("" for none) on server testServer.
func dfxD03Restore(classAPIURL, snapshotID string) (map[string]string, string) {
	params := testParams()
	if classAPIURL != "" {
		params[paramAPIURL] = classAPIURL
	}
	return params, snapshotID
}

// TestDefect_SameNumberedSnapshotOnTheClassArrayIsNeverRead: C1. The
// class reaches array B, which also holds a snapshot 42; the snapshot is A's
// 42. Correct: InvalidArgument naming both endpoints, and nothing is read or
// created on B, and A is never dialled.
func TestDefect_SameNumberedSnapshotOnTheClassArrayIsNeverRead(t *testing.T) {
	arrayB := emptyRestoreClient() // holds a snapshot 42, with data under it
	arrayB.GetSnapshotFunc = func(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
		t.Errorf("snapshot %s was read on the class's array B", id)
		return qumulo.SnapshotInfo{ID: id, Name: "42_snapshot-of-another-tenant", SourceFileID: "900"}, nil
	}
	arrayB.CreateDirectoryFunc = func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
		t.Errorf("created %s/%s on array B", parent, name)
		return qumulo.FileInfo{}, nil
	}
	arrayA := &MockClient{
		GetSnapshotFunc: func(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
			t.Errorf("array A was asked for snapshot %s: the restore must not cross arrays", id)
			return qumulo.SnapshotInfo{}, nil
		},
	}
	srv, dialled := endpointArrayServer(t, arrayA, arrayB)

	params, snap := dfxD03Restore(sharedNameAPIB, newSnapshotID(testServer, sharedNameAPIA, "42").String())
	req := restoreRequest("pvc-d03", params, testGiB)
	req.VolumeContentSource.GetSnapshot().SnapshotId = snap
	_, err := srv.CreateVolume(context.Background(), req)

	assertCode(t, err, codes.InvalidArgument)
	assert.Contains(t, err.Error(), sharedNameAPIA)
	assert.Contains(t, err.Error(), sharedNameAPIB)
	assert.Contains(t, err.Error(), "its own cluster")
	for _, endpoint := range *dialled {
		assert.False(t, strings.EqualFold(strings.TrimSuffix(endpoint, "/"), sharedNameAPIA),
			"the snapshot's array was dialled: %v", *dialled)
	}
}

// TestDefect_AnotherSpellingOfTheSameEndpointRestores: C2. Trailing
// slash and case are the same endpoint.
func TestDefect_AnotherSpellingOfTheSameEndpointRestores(t *testing.T) {
	for _, classAPIURL := range []string{sharedNameAPIA + "/", strings.ToUpper(sharedNameAPIA)} {
		t.Run(classAPIURL, func(t *testing.T) {
			srv, _ := endpointArrayServer(t, emptyRestoreClient(), &MockClient{})
			params, snap := dfxD03Restore(classAPIURL, newSnapshotID(testServer, sharedNameAPIA, "42").String())
			req := restoreRequest("pvc-d03", params, testGiB)
			req.VolumeContentSource.GetSnapshot().SnapshotId = snap
			_, err := srv.CreateVolume(context.Background(), req)
			require.NoError(t, err)
		})
	}
}

// TestDefect_TheDefaultEndpointSpelledOutRestores: C3. A class with no
// apiUrl reaches https://<server>:8000; a snapshot naming that endpoint
// explicitly is on the same array.
func TestDefect_TheDefaultEndpointSpelledOutRestores(t *testing.T) {
	srv := newTestControllerServer(t, emptyRestoreClient())
	params, snap := dfxD03Restore("", testServer+"#https://"+testServer+":8000#42")
	req := restoreRequest("pvc-d03", params, testGiB)
	req.VolumeContentSource.GetSnapshot().SnapshotId = snap
	_, err := srv.CreateVolume(context.Background(), req)
	require.NoError(t, err)
}

// TestDefect_ALegacyIDRestoresThroughADefaultEndpointClass: C4. The
// form before the endpoint field (<server>/<id>,/) means the
// server's default endpoint, which a class with no apiUrl reaches.
func TestDefect_ALegacyIDRestoresThroughADefaultEndpointClass(t *testing.T) {
	srv := newTestControllerServer(t, emptyRestoreClient())
	params, snap := dfxD03Restore("", testServer+"/42")
	req := restoreRequest("pvc-d03", params, testGiB)
	req.VolumeContentSource.GetSnapshot().SnapshotId = snap
	_, err := srv.CreateVolume(context.Background(), req)
	require.NoError(t, err)
}

// TestDefect_ALegacyIDIsRefusedThroughAnotherEndpointWithTheImportForm:
// C5 (Decision 2a). A legacy ID cannot say which array it came from, so a
// class whose apiUrl is not the server's default refuses it rather than
// guessing -- and says how to import it with the endpoint named.
func TestDefect_ALegacyIDIsRefusedThroughAnotherEndpointWithTheImportForm(t *testing.T) {
	arrayA := &MockClient{
		GetSnapshotFunc: func(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
			t.Errorf("snapshot %s was read on the class's array", id)
			return qumulo.SnapshotInfo{}, nil
		},
	}
	srv, _ := endpointArrayServer(t, arrayA, &MockClient{})
	for _, legacy := range []string{testServer + "/42", testServer + "##42"} {
		t.Run(legacy, func(t *testing.T) {
			params, snap := dfxD03Restore(sharedNameAPIA, legacy)
			req := restoreRequest("pvc-d03", params, testGiB)
			req.VolumeContentSource.GetSnapshot().SnapshotId = snap
			_, err := srv.CreateVolume(context.Background(), req)
			assertCode(t, err, codes.InvalidArgument)
			assert.Contains(t, err.Error(), "names no endpoint")
			assert.Contains(t, err.Error(), newSnapshotID(testServer, sharedNameAPIA, "42").String(),
				"the refusal must give the import form that names the class's array")
		})
	}
}

// TestDefect_ASnapshotOnAnotherServerIsRefused: C6. The server check
// that predates still holds.
func TestDefect_ASnapshotOnAnotherServerIsRefused(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		GetSnapshotFunc: func(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
			t.Errorf("snapshot %s was read", id)
			return qumulo.SnapshotInfo{}, nil
		},
	})
	params, snap := dfxD03Restore("", "other-array.example##42")
	req := restoreRequest("pvc-d03", params, testGiB)
	req.VolumeContentSource.GetSnapshot().SnapshotId = snap
	_, err := srv.CreateVolume(context.Background(), req)
	assertCode(t, err, codes.InvalidArgument)
	assert.Contains(t, err.Error(), "its own cluster")
}
