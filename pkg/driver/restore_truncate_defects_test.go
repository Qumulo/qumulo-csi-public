//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"encoding/json"
	"path"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// A staged file LONGER than its snapshot source. copy-chunk overwrites
// bytes in place and never shortens a file, so re-copying from 0 left the
// tail, and the post-copy size check refused the file on every attempt: the
// restore never converged.

// sizingArray is a target that keeps file lengths the way the array does:
// a copy writes [offset, source size) and leaves anything past it; only an
// attribute update carrying a size changes the length downwards.
type sizingArray struct {
	mu     sync.Mutex
	source []qumulo.DirEntry
	staged map[string]uint64 // path -> length
	truncs []string
}

func (a *sizingArray) client() *MockClient {
	return &MockClient{
		ListDirectoryFunc: func(_ context.Context, ref, snapshotID string) ([]qumulo.DirEntry, error) {
			if snapshotID != "" {
				return a.source, nil
			}
			a.mu.Lock()
			defer a.mu.Unlock()
			var out []qumulo.DirEntry
			for p, size := range a.staged {
				if path.Dir(p) == ref {
					out = append(out, file(path.Base(p), size))
				}
			}
			return out, nil
		},
		CreateFileFunc: func(_ context.Context, parent, name string) (qumulo.FileInfo, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.staged[parent+"/"+name] = 0
			return qumulo.FileInfo{Path: parent + "/" + name}, nil
		},
		CopyFileFunc: func(_ context.Context, target, sourceID, _ string, offset uint64) error {
			a.mu.Lock()
			defer a.mu.Unlock()
			for _, e := range a.source {
				if e.ID == sourceID && e.Size > a.staged[target] {
					a.staged[target] = e.Size
				}
			}
			return nil
		},
		// Read through the wire form, so this compiles whatever fields the
		// attributes struct has: "size" is what the array acts on.
		SetFileAttributesFunc: func(_ context.Context, p string, attrs qumulo.FileAttributes) error {
			raw, err := json.Marshal(attrs)
			if err != nil {
				return err
			}
			var wire map[string]any
			if err := json.Unmarshal(raw, &wire); err != nil {
				return err
			}
			if s, ok := wire["size"].(string); ok {
				n, err := strconv.ParseUint(s, 10, 64)
				if err != nil {
					return err
				}
				a.mu.Lock()
				a.staged[p] = n
				a.truncs = append(a.truncs, p+" "+s)
				a.mu.Unlock()
			}
			return nil
		},
		LookupFileByPathFunc: func(_ context.Context, p string) (qumulo.FileInfo, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			size := a.staged[p]
			return qumulo.FileInfo{Path: p, Type: "FS_FILE_TYPE_FILE", Size: &size}, nil
		},
	}
}

func TestDefect_AStagedFileLongerThanItsSourceIsCutBackAndTheRestoreConverges(t *testing.T) {
	array := &sizingArray{
		source: []qumulo.DirEntry{file("big.bin", 4096)},
		staged: map[string]uint64{"/stage/big.bin": 8192}, // an earlier, longer copy
	}
	client := array.client()
	srv := newTestControllerServer(t, client)

	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		_, _, err = restoreAttempt(context.Background(), srv, client, "42", "id-vol", "/stage")
		t.Logf("attempt %d: %v (staged length %d)", attempt, err, array.staged["/stage/big.bin"])
		if err == nil {
			break
		}
	}

	require.NoError(t, err, "a staged file longer than its source must not fail every attempt")
	assert.Equal(t, uint64(4096), array.staged["/stage/big.bin"])
	t.Logf("truncations: %v", array.truncs)
}

// The empty-source variant: no copy is made at all, so a longer staged file
// was committed with its stale bytes and the restore reported success.
func TestDefect_AStagedFileOverAnEmptySourceIsEmptied(t *testing.T) {
	array := &sizingArray{
		source: []qumulo.DirEntry{file("empty.txt", 0)},
		staged: map[string]uint64{"/stage/empty.txt": 100},
	}
	client := array.client()
	srv := newTestControllerServer(t, client)

	_, _, err := restoreAttempt(context.Background(), srv, client, "42", "id-vol", "/stage")

	require.NoError(t, err)
	assert.Zero(t, array.staged["/stage/empty.txt"], "the snapshot's file is empty; the restored one held 100 stale bytes")
}
