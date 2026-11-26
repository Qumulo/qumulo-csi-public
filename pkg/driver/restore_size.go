// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"sync"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// A snapshot's size from its files.
//
// The array's tree totals for a snapshot (GetTreeUsage) are aggregates it
// updates in the background, and a snapshot taken seconds after a large
// write can report almost nothing: 24 KiB for 1.2 GB, measured on the
// OpenShift lab (2026-11-16). Both the restore's own size check and the
// restoreSize an imported snapshot reports trusted that figure, so a 1 GiB
// claim restored 1.26 GB and bound already over its quota. The file sizes in
// a directory listing are not aggregates and are right at once, so the
// snapshot's files are added up too, and the larger of the two is used
// (by decision: report the larger size).
//
// A snapshot never changes, so a sum that covered the whole tree stays right
// and is remembered; see snapshotSizes.

const (
	// restoreMeasureBudget bounds the walk that adds up a snapshot's files
	// before a restore copies anything. A tree too large to list in this
	// time is restored on the array's totals alone, as before, with a
	// warning; a sum already past the claim when the time runs out still
	// refuses.
	restoreMeasureBudget = 60 * time.Second
	// listMeasureBudget is the same bound for ListSnapshots by ID, which a
	// snapshotter calls for an imported snapshot. Shorter: that RPC answers
	// a status poll, and an incomplete sum still only raises the size.
	listMeasureBudget = 20 * time.Second

	// snapshotSizeTTL and maxSnapshotSizes bound the remembered sums.
	snapshotSizeTTL  = time.Hour
	maxSnapshotSizes = 256
)

// snapshotData is what a walk of a snapshot's tree found.
type snapshotData struct {
	// bytes is the sum of the regular files' sizes, each hard-linked file
	// counted once, as a restore recreates it.
	bytes uint64
	// entries counts files and directories, as maxRestoreFiles does.
	entries int
	// complete reports that the walk covered the whole tree. When it is
	// false, bytes and entries are lower bounds.
	complete bool
}

// errMeasureStop ends a measuring walk early, once its answer is known.
var errMeasureStop = errors.New("snapshot measure stopped")

// measureSnapshotData adds up the regular files of snapshot snapshotID under
// the directory rootID, as a restore would copy them. It stops early, with
// complete false, once the sum is past stopAbove (zero for no limit) or the
// entries past maxRestoreFiles: the answer is known by then. A context that
// ends mid-walk returns what was found and the context's error.
func measureSnapshotData(ctx context.Context, client qumulo.ClientInterface, snapshotID, rootID string, stopAbove uint64) (snapshotData, error) {
	var data snapshotData
	linked := make(map[string]bool)
	pending := []string{rootID}
	for len(pending) > 0 {
		dir := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		err := listDirectoryPages(ctx, client, dir, snapshotID, func(page []qumulo.DirEntry) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			for _, entry := range page {
				data.entries++
				switch {
				case entry.IsDirectory():
					pending = append(pending, entry.ID)
				case entry.Type != qumuloFileTypeRegular:
					// Not restored (symlinks, devices), so not counted.
				case mayBeHardLinked(entry) && linked[entry.ID]:
					// Another name for a file already counted.
				default:
					if mayBeHardLinked(entry) {
						linked[entry.ID] = true
					}
					data.bytes += entry.Size
				}
				if data.entries > maxRestoreFiles || (stopAbove > 0 && data.bytes > stopAbove) {
					return errMeasureStop
				}
			}
			return nil
		})
		if errors.Is(err, errMeasureStop) {
			return data, nil
		}
		if err != nil {
			return data, err
		}
	}
	data.complete = true
	return data, nil
}

// snapshotSizes remembers the sums of snapshots measured whole.
//
// A hint, not state: a snapshot's contents never change, so a remembered sum
// is exact for as long as the snapshot exists, and losing it costs one more
// walk. Keyed by the array's API endpoint and the snapshot ID, holding no
// credential; entries go after snapshotSizeTTL and past maxSnapshotSizes,
// the oldest first.
type snapshotSizes struct {
	mu      sync.Mutex
	entries map[string]snapshotSizeEntry
	now     func() time.Time
}

type snapshotSizeEntry struct {
	data snapshotData
	at   time.Time
}

func newSnapshotSizes() *snapshotSizes {
	return &snapshotSizes{entries: make(map[string]snapshotSizeEntry), now: time.Now}
}

func snapshotSizeKey(endpoint, snapshotID string) string {
	return endpoint + "\x00" + snapshotID
}

// get returns a remembered whole-tree sum.
func (c *snapshotSizes) get(key string) (snapshotData, bool) {
	if c == nil {
		return snapshotData{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || c.now().Sub(e.at) > snapshotSizeTTL {
		return snapshotData{}, false
	}
	return e.data, true
}

// put remembers a whole-tree sum; a partial one is never remembered.
func (c *snapshotSizes) put(key string, data snapshotData) {
	if c == nil || !data.complete {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for k, e := range c.entries {
		if now.Sub(e.at) > snapshotSizeTTL {
			delete(c.entries, k)
		}
	}
	c.entries[key] = snapshotSizeEntry{data: data, at: now}
	for len(c.entries) > maxSnapshotSizes {
		oldest := ""
		for k, e := range c.entries {
			if k != key && (oldest == "" || e.at.Before(c.entries[oldest].at)) {
				oldest = k
			}
		}
		delete(c.entries, oldest)
	}
}

// snapshotData measures a snapshot, or returns the sum remembered for it.
// budget bounds a walk; stopAbove is as for measureSnapshotData.
func (s *ControllerServer) snapshotData(
	ctx context.Context, client qumulo.ClientInterface, endpoint, snapshotID, rootID string,
	stopAbove uint64, budget time.Duration,
) (snapshotData, error) {
	key := snapshotSizeKey(endpoint, snapshotID)
	if data, ok := s.snapshotSizes.get(key); ok {
		return data, nil
	}
	walkCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	data, err := measureSnapshotData(walkCtx, client, snapshotID, rootID, stopAbove)
	if err == nil {
		s.snapshotSizes.put(key, data)
	}
	return data, err
}
