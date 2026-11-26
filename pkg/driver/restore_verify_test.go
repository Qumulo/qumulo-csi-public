// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// The restore's half: a copy is read back and compared with what the
// snapshot says the file holds.
//
// copy-chunk reports completion by answering null, and that answer was taken
// at its word. A copy that ended early -- the API stopping short, a source
// size that was never right -- committed a volume with a truncated file and
// reported success, which is the worst shape a restore failure can take:
// the claim binds, the pod starts, and the data is not there.

// sizedTree is fakeTree with a target the restore can read its copies back
// from: the sizes the array would report after each copy.
type sizedTree struct {
	*fakeTree
	// restored is the size LookupFileByPath reports for a copied file, by
	// path. A path with no entry reports the source's size, which is what a
	// copy that worked leaves behind.
	restored map[string]uint64
	sources  map[string]uint64
}

func (s *sizedTree) client() *MockClient {
	client := s.fakeTree.client()
	client.LookupFileByPathFunc = func(_ context.Context, p string) (qumulo.FileInfo, error) {
		size, ok := s.restored[p]
		if !ok {
			size = s.sources[p]
		}
		return qumulo.FileInfo{Path: p, Type: "FS_FILE_TYPE_FILE", Size: &size}, nil
	}
	return client
}

func TestRestoreRefusesACopyThatCameUpShort(t *testing.T) {
	tree := &sizedTree{
		fakeTree: &fakeTree{source: map[string][]qumulo.DirEntry{
			"id-vol": {file("a.txt", 10), file("big.bin", 4096)},
		}},
		sources:  map[string]uint64{"/stage/a.txt": 10, "/stage/big.bin": 4096},
		restored: map[string]uint64{"/stage/big.bin": 2048}, // the copy stopped halfway
	}
	client := tree.client()
	srv := newTestControllerServer(t, client)

	_, _, err := restoreAttempt(context.Background(), srv, client, "42", "id-vol", "/stage")

	require.Error(t, err, "a short copy must not be committed as a finished volume")
	assert.Contains(t, err.Error(), "big.bin")
	assert.Contains(t, err.Error(), "4096")
}

func TestRestoreAcceptsACopyAtTheRightSize(t *testing.T) {
	tree := &sizedTree{
		fakeTree: &fakeTree{source: map[string][]qumulo.DirEntry{
			"id-vol": {file("a.txt", 10), file("big.bin", 4096)},
		}},
		sources: map[string]uint64{"/stage/a.txt": 10, "/stage/big.bin": 4096},
	}
	client := tree.client()
	srv := newTestControllerServer(t, client)

	files, bytes, err := restoreAttempt(context.Background(), srv, client, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, 2, files)
	assert.Equal(t, uint64(4106), bytes)
}

// An array that will not say how big the copy is leaves nothing to compare.
// The restore goes on: failing it over a field the answer did not carry
// would turn a verification into an outage, and the next attempt re-examines
// the file anyway, because a file is skipped only at the source's size.
func TestRestoreGoesOnWhenTheCopyCannotBeReadBack(t *testing.T) {
	tree := &fakeTree{source: map[string][]qumulo.DirEntry{
		"id-vol": {file("a.txt", 10)},
	}}
	client := tree.client()
	client.LookupFileByPathFunc = func(_ context.Context, p string) (qumulo.FileInfo, error) {
		return qumulo.FileInfo{}, apiError(503)
	}
	srv := newTestControllerServer(t, client)

	files, _, err := restoreAttempt(context.Background(), srv, client, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Equal(t, 1, files)
}
