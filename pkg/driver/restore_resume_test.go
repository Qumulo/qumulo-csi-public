// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// resumableArray is a staging area that holds files, directories and hard
// links, and cuts each attempt off after budget calls, the way a restore's
// per-attempt deadline does. staged is keyed by path; inode says which staged
// names are one file.
type resumableArray struct {
	source   map[string][]qumulo.DirEntry
	sizeByID map[string]uint64
	staged   map[string]qumulo.DirEntry
	inode    map[string]int
	copies   map[string]int // target path -> copy-chunk calls
	next     int

	calls, budget int
	cancel        context.CancelFunc
}

func newResumableArray(source map[string][]qumulo.DirEntry) *resumableArray {
	a := &resumableArray{source: source, sizeByID: map[string]uint64{},
		staged: map[string]qumulo.DirEntry{}, inode: map[string]int{}, copies: map[string]int{}}
	for _, entries := range source {
		for _, e := range entries {
			a.sizeByID[e.ID] = e.Size
		}
	}
	return a
}

func (a *resumableArray) tick(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.calls++
	if a.calls >= a.budget {
		a.cancel()
	}
	return nil
}

func (a *resumableArray) client() *MockClient {
	return &MockClient{
		ListDirectoryFunc: func(ctx context.Context, ref, snap string) ([]qumulo.DirEntry, error) {
			if err := a.tick(ctx); err != nil {
				return nil, err
			}
			if snap != "" {
				return a.source[ref], nil
			}
			if _, ok := a.staged[ref]; !ok && ref != "/stage" {
				return nil, apiError(http.StatusNotFound)
			}
			var out []qumulo.DirEntry
			for p, e := range a.staged {
				if path.Dir(p) == ref {
					out = append(out, e)
				}
			}
			return out, nil
		},
		CreateDirectoryFunc: func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
			if err := a.tick(ctx); err != nil {
				return qumulo.FileInfo{}, err
			}
			p := parent + "/" + name
			if _, ok := a.staged[p]; ok {
				return qumulo.FileInfo{}, apiError(http.StatusConflict)
			}
			a.staged[p] = qumulo.DirEntry{Name: name, Type: "FS_FILE_TYPE_DIRECTORY", ModificationTime: "now"}
			return qumulo.FileInfo{Path: p}, nil
		},
		CreateFileFunc: func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
			if err := a.tick(ctx); err != nil {
				return qumulo.FileInfo{}, err
			}
			p := parent + "/" + name
			if _, ok := a.staged[p]; ok {
				return qumulo.FileInfo{}, apiError(http.StatusConflict)
			}
			a.next++
			a.inode[p] = a.next
			a.staged[p] = qumulo.DirEntry{Name: name, Type: "FS_FILE_TYPE_FILE", ModificationTime: "now"}
			return qumulo.FileInfo{Path: p}, nil
		},
		CopyFileFunc: func(ctx context.Context, target, sourceID, _ string, _ uint64) error {
			if err := a.tick(ctx); err != nil {
				return err
			}
			e := a.staged[target]
			e.Size = a.sizeByID[sourceID]
			a.staged[target] = e
			a.copies[target]++
			return nil
		},
		CreateLinkFunc: func(ctx context.Context, parent, name, existing string) (qumulo.FileInfo, error) {
			if err := a.tick(ctx); err != nil {
				return qumulo.FileInfo{}, err
			}
			p := parent + "/" + name
			a.inode[p] = a.inode[existing]
			e := a.staged[existing]
			e.Name = name
			a.staged[p] = e
			return qumulo.FileInfo{Path: p}, nil
		},
		SetFileAttributesFunc: func(ctx context.Context, p string, attrs qumulo.FileAttributes) error {
			if err := a.tick(ctx); err != nil {
				return err
			}
			e := a.staged[p]
			e.Mode, e.Owner, e.Group, e.ModificationTime = attrs.Mode, attrs.Owner, attrs.Group, attrs.ModificationTime
			a.staged[p] = e
			return nil
		},
	}
}

// A restore that takes several attempts must
// come out the same as one that takes one: every file copied exactly once,
// and a hard link kept as a link. The live run could not tell whether
// its result depended on the order the array listed a/ and b/ -- the listing
// is unsorted -- so both orders are run, each with attempts cut off at a
// fixed call budget, and once more with the controller restarted (its memo
// lost) between attempts.
func TestAResumedRestoreEndsTheSameAsAnUninterruptedOne(t *testing.T) {
	stamp := func(e qumulo.DirEntry) qumulo.DirEntry {
		e.Mode, e.Owner, e.Group, e.ModificationTime = "0644", "1", "1", "2020-01-01T00:00:00Z"
		return e
	}
	sourceWith := func(aFirst bool) map[string][]qumulo.DirEntry {
		a := owned(dir("a"), "0755", "1", "2020-01-01T00:00:00Z")
		b := owned(dir("b"), "0755", "1", "2020-01-01T00:00:00Z")
		big := stamp(qumulo.DirEntry{Name: "big", ID: "id-inode-7", Type: "FS_FILE_TYPE_FILE", Size: 1 << 20})
		lnk := big
		lnk.Name = "big.lnk"
		src := map[string][]qumulo.DirEntry{"id-a": {big}, "id-b": {}}
		for i := 0; i < 6; i++ {
			src["id-a"] = append(src["id-a"], stamp(file(fmt.Sprintf("a%d", i), 10)))
			src["id-b"] = append(src["id-b"], stamp(file(fmt.Sprintf("b%d", i), 10)))
		}
		src["id-b"] = append(src["id-b"], lnk)
		if aFirst {
			src["id-vol"] = []qumulo.DirEntry{a, b}
		} else {
			src["id-vol"] = []qumulo.DirEntry{b, a}
		}
		return src
	}

	for _, tc := range []struct {
		name            string
		aFirst, restart bool
	}{
		{name: "a/ listed first", aFirst: true},
		{name: "b/ listed first", aFirst: false},
		{name: "a/ listed first, controller restarted between attempts", aFirst: true, restart: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			array := newResumableArray(sourceWith(tc.aFirst))
			array.budget = 12
			c := array.client()
			srv := newTestControllerServer(t, c)

			attempts := 0
			var err error
			for ; attempts < 20; attempts++ {
				if tc.restart && attempts > 0 {
					srv = newTestControllerServer(t, c)
				}
				array.calls = 0
				ctx, cancel := context.WithCancel(context.Background())
				array.cancel = cancel
				_, _, err = restoreAttempt(ctx, srv, c, "42", "id-vol", "/stage")
				cancel()
				if err == nil {
					break
				}
			}

			require.NoError(t, err, "the restore converges")
			require.Greater(t, attempts, 1, "the budget is small enough to need several attempts")
			for p, n := range array.copies {
				assert.Equal(t, 1, n, "%s copied %d times", p, n)
			}
			require.Contains(t, array.staged, "/stage/a/big")
			require.Contains(t, array.staged, "/stage/b/big.lnk")
			assert.Equal(t, array.inode["/stage/a/big"], array.inode["/stage/b/big.lnk"],
				"the two names are one file: a link, not a copy")
			// Whichever name the walk meets first carries the data -- b/big.lnk
			// when b/ is listed first -- and the other is the link.
			assert.Equal(t, 1, array.copies["/stage/a/big"]+array.copies["/stage/b/big.lnk"],
				"the linked file's data is copied once, under one of its names")
			assert.Len(t, array.copies, 13, "twelve small files and the linked one, each once")
		})
	}
}

// A second name whose link the array refused is copied instead, and an
// attempt cut off in that copy leaves the name present but short. The next
// attempt found the name and took it for a finished link: the copy was never
// resumed, the restore committed, and the claim bound with the file's tail
// missing. A name already there counts as linked only at the source's size,
// as restoreFile counts a file finished; a shorter one is resumed.
func TestResumedRestoreFinishesAFallbackCopyOfASecondName(t *testing.T) {
	t.Run("same controller", func(t *testing.T) { checkFallbackCopyOfASecondNameIsResumed(t, false) })
	t.Run("controller restarted between attempts", func(t *testing.T) { checkFallbackCopyOfASecondNameIsResumed(t, true) })
}

// checkFallbackCopyOfASecondNameIsResumed is the hard-link scenario, shared with its
// defect test: a.bin and b.bin are one file, the array refuses the link, and
// the copy that replaces it is cut once after 4 KiB. restart drops the
// controller's memo between attempts.
func checkFallbackCopyOfASecondNameIsResumed(t *testing.T, restart bool) {
	t.Helper()
	const full = uint64(1 << 20)
	first := qumulo.DirEntry{Name: "a.bin", ID: "id-inode-7", Type: "FS_FILE_TYPE_FILE", Size: full}
	second := first
	second.Name = "b.bin"

	array := newResumableArray(map[string][]qumulo.DirEntry{"id-vol": {first, second}})
	array.budget = 1 << 30 // the cut copy ends the attempt, not the call count
	c := array.client()
	// The array refuses the link, so b.bin is copied; that copy is cut
	// after its first 4 KiB, once.
	c.CreateLinkFunc = func(ctx context.Context, _, _, _ string) (qumulo.FileInfo, error) {
		if err := array.tick(ctx); err != nil {
			return qumulo.FileInfo{}, err
		}
		return qumulo.FileInfo{}, apiError(http.StatusBadRequest)
	}
	var copies []string
	cut := false
	c.CopyFileFunc = func(ctx context.Context, target, sourceID, _ string, offset uint64) error {
		if err := array.tick(ctx); err != nil {
			return err
		}
		copies = append(copies, fmt.Sprintf("%s@%d", target, offset))
		e := array.staged[target]
		e.Size = array.sizeByID[sourceID]
		if target == "/stage/b.bin" && !cut {
			cut = true
			e.Size = 4096
			array.staged[target] = e
			array.cancel()
			return context.Canceled
		}
		array.staged[target] = e
		return nil
	}
	srv := newTestControllerServer(t, c)

	var err error
	attempts := 0
	for ; attempts < 5; attempts++ {
		if restart && attempts > 0 {
			srv = newTestControllerServer(t, c)
		}
		ctx, cancel := context.WithCancel(context.Background())
		array.cancel = cancel
		_, _, err = restoreAttempt(ctx, srv, c, "42", "id-vol", "/stage")
		cancel()
		if err == nil {
			break
		}
	}

	require.NoError(t, err, "the restore converges")
	require.Greater(t, attempts, 0, "the cut copy needs a second attempt")
	assert.Equal(t, full, array.staged["/stage/a.bin"].Size)
	assert.Equal(t, full, array.staged["/stage/b.bin"].Size,
		"the second name is restored whole, not left at the cut")
	assert.Contains(t, copies, "/stage/b.bin@4096", "the cut copy is resumed where it stopped")
}
