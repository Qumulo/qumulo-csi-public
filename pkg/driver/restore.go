// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// Limits on what one restore will attempt.
//
// A restore costs round trips per entry, so a large enough tree cannot finish
// however patiently it is retried. Refusing up front with a reason beats a PVC
// that stays Pending forever while an operator guesses why. maxRestoreFiles
// counts entries -- files and directories alike, since a tree of empty
// directories is as much work -- and both limits are checked against
// the snapshot's own counts before anything is copied. These are constants
// rather than StorageClass parameters for now; moving them is additive.
const (
	maxRestoreFiles = 50_000
	maxRestoreBytes = uint64(2) << 40 // 2 TiB
)

// defaultRestoreBudget bounds a restore attempt when the CO set no deadline.
//
// It is deliberately shorter than csi-provisioner's default 60s timeout: the
// attempt has to return in time for the quota and the rename that follow it.
const defaultRestoreBudget = 40 * time.Second

// errRestoreInProgress reports that another call is already restoring this
// volume. The CSI spec's answer for a concurrent operation on one volume is
// ABORTED, and the CO retries.
var errRestoreInProgress = errors.New("a restore of this volume is already in progress")

// maxConcurrentRestores bounds restore attempts running at once on one
// controller.
const maxConcurrentRestores = 4

// restoreLocks serialises restores per volume name.
//
// Two concurrent CreateVolume calls for one name would otherwise both walk the
// tree and both issue copies -- wasteful, and they would race on the final
// rename. The lock is in-memory only: it guards against a duplicate call inside
// one controller, while the staging directory is what makes the operation safe
// across a controller restart.
type restoreLocks struct {
	mu     sync.Mutex
	active map[string]struct{}
}

func newRestoreLocks() *restoreLocks {
	return &restoreLocks{active: make(map[string]struct{})}
}

// acquire takes the lock for name, or reports that it is held.
func (l *restoreLocks) acquire(name string) (release func(), err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active == nil {
		l.active = make(map[string]struct{})
	}
	if _, busy := l.active[name]; busy {
		return nil, errRestoreInProgress
	}
	l.active[name] = struct{}{}

	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		delete(l.active, name)
	}, nil
}

// busy reports whether a restore of name holds the lock now.
func (l *restoreLocks) busy(name string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, held := l.active[name]
	return held
}

// restoreBudget derives how long this attempt may spend copying.
//
// The CO's own deadline is the authority -- csi-provisioner sets one from its
// --timeout flag -- so the budget is read from the context rather than guessed.
// A margin is held back for the quota and rename that must still happen after
// the copy: running the copy to the deadline would lose the whole attempt's
// work to a killed RPC.
func restoreBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithTimeout(ctx, defaultRestoreBudget)
	}

	remaining := time.Until(deadline)
	margin := remaining / 4
	if margin > 10*time.Second {
		margin = 10 * time.Second
	}
	if margin < 2*time.Second {
		margin = 2 * time.Second
	}
	if remaining <= margin {
		// Too little left to copy anything useful; let the caller retry.
		return context.WithTimeout(ctx, 0)
	}
	return context.WithTimeout(ctx, remaining-margin)
}

// restoreTempPrefix marks a directory that is still being restored.
//
// A restore assembles the volume under this prefix and renames it into place as
// its final act, so the volume's real path either does not exist or is
// complete. Nothing else in the driver creates such a name, so its presence is
// unambiguous: an interrupted restore, resumable or removable.
const restoreTempPrefix = ".csi-restoring-"

// restoreTempPathFor returns the staging path a volume is restored into.
func restoreTempPathFor(params volumeParams, name string) string {
	return path.Join(params.realStoreRoot(), restoreTempPrefix+name)
}

// restoreFromSnapshot populates a new volume from a snapshot, server-side.
//
// The shape is dictated by what the Qumulo API offers: there is no tree clone,
// only a per-file `copy-chunk` that can read from a snapshot. So
// the tree is walked and recreated entry by entry, and the bytes are copied by
// the cluster rather than through this process.
//
// Two properties make that safe to retry, and both come from the source being a
// snapshot rather than a live directory:
//
//   - The source is immutable, so a second attempt sees exactly what the first
//     one saw. A partially copied tree can be completed rather than restarted.
//   - Nothing observes the result until the final rename, so a retry never
//     races a workload.
//
// The caller renames the staging directory into place; this function only fills
// it. That split is deliberate: the rename is the commit point and belongs with
// the rest of CreateVolume's ordering.
//
// The source is identified by file ID, not by path, and that is load-bearing
// rather than a style choice. A snapshot outlives the directory it was taken
// of, so the standard backup workflow -- snapshot, delete the volume, restore
// -- asks this to walk a tree whose live path no longer exists. Keying on the
// ID means the walk reads the snapshot itself, which is intact, instead of
// resolving through a live path that DeleteVolume has already tree-deleted.
// The Qumulo API accepts an ID wherever it accepts a path reference, so this
// costs nothing: see fileRef in pkg/qumulo/client.go.
func (s *ControllerServer) restoreFromSnapshot(
	ctx context.Context,
	client qumulo.ClientInterface,
	snapshotID string,
	sourceID string,
	targetPath string,
	memo *restoreMemo,
) (files int, bytes uint64, err error) {
	w := &restoreWalk{s: s, client: client, snapshotID: snapshotID, staged: memo.staged, completed: memo.completed}
	files, bytes, _, err = w.tree(ctx, sourceID, targetPath, false)
	slog.DebugContext(ctx, "restore: attempt finished",
		"target", targetPath, "subtreesSkipped", w.skipped, "filesCopied", files, "error", err)
	if w.nonRegular > 0 {
		slog.WarnContext(ctx, "restore: skipped entries that are not regular files (symlinks, devices); "+
			"they are not in the restored volume",
			"entries", w.nonRegular, "firstPath", w.firstNonRegularPath, "firstType", w.firstNonRegularType)
	}
	if w.attrsRefused > 0 {
		slog.WarnContext(ctx, "restore: the array refused to set some restored entries' mode, owner, group "+
			"or mtime; those entries keep the attributes the create gave them",
			"entries", w.attrsRefused, "firstPath", w.firstRefusedPath, "firstError", w.firstRefusedErr)
	}
	return files, bytes, err
}

// restoreWalk is the state one restore attempt carries through the tree.
type restoreWalk struct {
	s          *ControllerServer
	client     qumulo.ClientInterface
	snapshotID string

	// staged maps each regular file's source file ID to the path it was
	// staged at, across the whole tree and every attempt: a file ID met a
	// second time is a hard link, and is rebuilt as one. completed
	// holds the staged directories an attempt finished. Both belong to the
	// restore's restoreMemo, so they outlive the attempt.
	staged    map[string]string
	completed map[string]bool
	// skipped counts the finished subtrees this attempt did not walk.
	skipped int

	// entries counts what this attempt created -- files, links and
	// directories -- against maxRestoreFiles. Directories count: a tree of
	// empty directories is as much work as a tree of empty files.
	entries int

	// attrsRefused counts entries whose attributes the array would not set,
	// reported once at the end rather than per entry.
	attrsRefused     int
	firstRefusedPath string
	firstRefusedErr  error

	// nonRegular counts the symlinks, devices and other non-regular entries
	// skipped, also reported once at the end.
	nonRegular          int
	firstNonRegularPath string
	firstNonRegularType string
}

// tree restores the snapshot directory sourceID into targetPath. changed
// reports whether anything in targetPath was created or modified, which is
// what moves its mtime and so decides whether its own attributes must be set
// again once its children are done.
//
// fresh says targetPath was created by this attempt, so it is known to be
// empty and is not listed.
func (w *restoreWalk) tree(
	ctx context.Context,
	sourceID string,
	targetPath string,
	fresh bool,
) (files int, bytes uint64, changed bool, err error) {
	client, snapshotID := w.client, w.snapshotID

	// What a previous attempt already put here, read in one call rather than
	// one attribute lookup per file. On a resumed restore of a wide directory
	// that is the difference between one round trip and thousands. The
	// listing carries each entry's attributes too, so a resumed restore sets
	// them only where they differ. Read first, so it is whole before the
	// source's pages are worked through; kept slim, without paths and IDs.
	done := make(map[string]qumulo.DirEntry)
	if !fresh {
		err := listDirectoryPages(ctx, client, targetPath, "", func(page []qumulo.DirEntry) error {
			for _, e := range page {
				done[e.Name] = slimTargetEntry(e)
			}
			return nil
		})
		if err != nil && !qumulo.IsNotFound(err) {
			return files, bytes, changed, fmt.Errorf("listing the restore target %q: %w", targetPath, err)
		}
	}

	// The source a page at a time, each page restored before the next is
	// read: a directory listed whole, and every ancestor's held while the
	// walk descended, was what made a restore's memory grow with its widest
	// directories.
	var walkErr error
	stop := func(err error) error {
		walkErr = err
		return errStopWalk
	}
	err = listDirectoryPages(ctx, client, sourceID, snapshotID, func(page []qumulo.DirEntry) error {
		for _, entry := range page {
			select {
			case <-ctx.Done():
				// The caller's budget expired. What has been copied stays in the
				// staging directory for the next attempt to continue from.
				return stop(ctx.Err())
			default:
			}

			childTarget := path.Join(targetPath, entry.Name)
			existing, present := done[entry.Name]

			if entry.IsDirectory() {
				if present && existing.IsDirectory() && w.completed[childTarget] {
					// An earlier attempt finished this whole subtree. Walking
					// it again cost a 409 and two listings per directory, and
					// once that outgrew an attempt's budget no attempt made
					// progress: 20,000 directories stalled at 14,264 for good.
					// Known from the restore's memo rather than from the
					// directory's attributes: that evidence failed whenever the
					// array refused the attributes, and skipping on it lost the
					// hard links inside, which the memo keeps.
					w.skipped++
					continue
				}
				created := false
				if !present {
					if _, err := client.CreateDirectory(ctx, targetPath, entry.Name); err != nil {
						if !qumulo.IsConflict(err) {
							return stop(fmt.Errorf("creating directory %q: %w", childTarget, err))
						}
						// Made since the listing; descend into it.
					} else {
						created = true
						w.entries++
						if w.entries > maxRestoreFiles {
							return stop(fmt.Errorf("%w: more than %d entries", errRestoreTooLarge, maxRestoreFiles))
						}
					}
				}
				subFiles, subBytes, subChanged, err := w.tree(ctx, entry.ID, childTarget, created)
				files += subFiles
				bytes += subBytes
				if err != nil {
					return stop(err)
				}
				// After the children: creating one moves the directory's mtime,
				// and a restrictive mode set first could get in their way.
				if created || subChanged || attributesDiffer(entry, existing) {
					if err := w.setAttributes(ctx, childTarget, entry); err != nil {
						return stop(err)
					}
				}
				// Finished, attributes included -- or refused by the array,
				// which setAttributes has counted. Either way nothing is left to
				// do here, so a later attempt skips it.
				w.completed[childTarget] = true
				changed = changed || created || subChanged
				continue
			}

			// Only regular files are copied. A symlink or device node in a
			// snapshot is skipped loudly rather than silently: recreating one
			// needs a different call, and pretending it was restored would be a
			// quiet data loss.
			if entry.Type != qumuloFileTypeRegular {
				// Counted, and warned about once at the end of the attempt: a
				// warning per entry, on every attempt, was 5,000 then 10,000 then
				// 15,000 lines for a tree of 5,000 symlinks. The target
				// path, not the source: there may be no live source path to name,
				// and the target is what the operator will look at.
				if w.nonRegular == 0 {
					w.firstNonRegularPath, w.firstNonRegularType = childTarget, entry.Type
				}
				w.nonRegular++
				continue
			}

			if w.entries >= maxRestoreFiles {
				return stop(fmt.Errorf("%w: more than %d entries", errRestoreTooLarge, maxRestoreFiles))
			}
			if bytes > maxRestoreBytes {
				return stop(fmt.Errorf("%w: more than %d bytes", errRestoreTooLarge, maxRestoreBytes))
			}

			// A second name for a file already staged: a hard link. Copying it
			// again split the link into two independent files, doubling the
			// space it used and possibly pushing the restore over its quota.
			// The first name is found in the restore's memo, which
			// outlives the attempt that staged it, so a skipped subtree does not
			// hide it. A memo entry naming this very path is the first
			// name itself, met again on a resumed attempt.
			// A file the array says has one link is no hard link's other name,
			// and need not be remembered: mapping every file's ID to its path
			// held tens to hundreds of MiB per restore attempt. One the
			// array reports no count for is remembered, as before.
			if !mayBeHardLinked(entry) {
				// Nothing to look up and nothing to stage.
			} else if first, ok := w.staged[entry.ID]; ok && first != childTarget {
				if linked, err := w.s.restoreLink(ctx, client, entry, targetPath, childTarget, first, present, existing.Size); err != nil {
					return stop(err)
				} else if linked {
					// Counted against the file cap, but not the byte cap: a
					// link adds no data. A link shares its inode's
					// attributes, which the first name already set.
					files++
					w.entries++
					changed = changed || !present
					continue
				}
				// The array would not make the link; fall through and copy,
				// which is what a restore did before.
			} else {
				w.staged[entry.ID] = childTarget
			}

			copied, err := w.s.restoreFile(ctx, client, snapshotID, entry, targetPath, childTarget, present, existing.Size)
			if err != nil {
				return stop(err)
			}
			if copied {
				files++
				w.entries++
				bytes += entry.Size
				changed = true
			}
			// After the copy, which moves the mtime. A file an earlier attempt
			// finished is left alone unless its attributes differ, so a resumed
			// restore does not spend a call per file.
			if copied || attributesDiffer(entry, existing) {
				if err := w.setAttributes(ctx, childTarget, entry); err != nil {
					return stop(err)
				}
			}
		}
		return nil
	})
	if walkErr != nil {
		return files, bytes, changed, walkErr
	}
	if err != nil {
		return files, bytes, changed, fmt.Errorf("listing file ID %s in snapshot %s: %w", sourceID, snapshotID, err)
	}

	return files, bytes, changed, nil
}

// setAttributes gives a restored entry the mode, owner, group and mtime it had
// in the snapshot. Created over REST, it would otherwise be owned by
// the API user with the cluster's default mode and the restore's own time: a
// non-root workload could not write its own subdirectories, scripts lost
// their execute bit, and private directories became world-readable.
//
// Only an array that does not answer fails the attempt. One that refuses the
// update -- a token without the privilege to change ownership, say -- is
// counted and reported once, and the restore completes as it did before
// rather than never completing.
func (w *restoreWalk) setAttributes(ctx context.Context, targetPath string, source qumulo.DirEntry) error {
	err := w.client.SetFileAttributes(ctx, targetPath, attributesOf(source))
	switch {
	case err == nil:
		return nil
	case arrayGaveNoAnswer(err):
		return fmt.Errorf("setting the attributes of %q: %w", targetPath, err)
	default:
		if w.attrsRefused == 0 {
			w.firstRefusedPath, w.firstRefusedErr = targetPath, err
		}
		w.attrsRefused++
		return nil
	}
}

// attributesOf is the part of a snapshot entry a restore copies onto its copy.
func attributesOf(e qumulo.DirEntry) qumulo.FileAttributes {
	return qumulo.FileAttributes{
		Mode: e.Mode, Owner: e.Owner, Group: e.Group, ModificationTime: e.ModificationTime,
	}
}

// attributesDiffer reports whether the staged entry lacks an attribute the
// snapshot entry has. A zero staged entry -- nothing there yet -- differs
// from any source that has attributes at all.
func attributesDiffer(source, staged qumulo.DirEntry) bool {
	differs := func(want, got string) bool { return want != "" && want != got }
	return differs(source.Mode, staged.Mode) || differs(source.Owner, staged.Owner) ||
		differs(source.Group, staged.Group) || differs(source.ModificationTime, staged.ModificationTime)
}

// restoreFile creates and fills one file, skipping work an earlier attempt
// already did.
//
// present/size describe what the target directory listing found for this name,
// so the decision costs no extra round trip. Returns whether it copied
// anything, so a resumed restore reports progress honestly rather than counting
// files it merely re-examined.
func (s *ControllerServer) restoreFile(
	ctx context.Context,
	client qumulo.ClientInterface,
	snapshotID string,
	entry qumulo.DirEntry,
	targetDir string,
	targetPath string,
	present bool,
	presentSize uint64,
) (bool, error) {
	// A file already there at the right size was finished by an earlier
	// attempt. copy-chunk is not free, and re-copying everything on every
	// retry would stop a large restore ever converging.
	if present && presentSize == entry.Size {
		return false, nil
	}

	if !present {
		if _, err := client.CreateFile(ctx, targetDir, entry.Name); err != nil && !qumulo.IsConflict(err) {
			return false, fmt.Errorf("creating file %q: %w", targetPath, err)
		}
	}
	// Present but shorter: an earlier attempt's copy ran out of time partway.
	// copy-chunk writes in order, so what is there is the start of the file,
	// and the copy resumes where it stopped. Starting again from byte 0 on
	// every attempt meant a file too large to copy within one attempt's
	// budget never finished, and the claim stayed Pending for ever.
	var offset uint64
	if present && presentSize < entry.Size {
		offset = presentSize
	}

	// Larger than the source is not a prefix of anything, and copy-chunk
	// cannot fix it: it overwrites bytes in place and never shortens a
	// file. Re-copying from 0 left the tail where it was, the size check
	// below refused the file, and the next attempt found it just as long --
	// so the restore failed the same way on every attempt and never
	// converged. The file is emptied first and copied from 0. That
	// covers an empty source too, which is never copied at all: a longer
	// staged file was committed with its stale bytes.
	if present && presentSize > entry.Size {
		if err := client.SetFileAttributes(ctx, targetPath, qumulo.FileAttributes{Size: "0"}); err != nil {
			return false, fmt.Errorf("truncating %q, which holds %d bytes where the snapshot holds %d: %w",
				targetPath, presentSize, entry.Size, err)
		}
	}

	// An empty file needs no copy, and a copy call on a zero-length source is
	// a wasted round trip -- which matters in a tree with many of them.
	if entry.Size == 0 {
		return true, nil
	}

	if err := client.CopyFile(ctx, targetPath, entry.ID, snapshotID, offset); err != nil {
		return false, fmt.Errorf("copying %q from snapshot %s: %w", targetPath, snapshotID, err)
	}

	// What was copied is read back and compared with what the snapshot says
	// the file holds. copy-chunk reports completion by answering null, and
	// that answer was taken at its word: a copy that ended early -- a source
	// size the listing got wrong, a chunk loop cut short -- committed a
	// volume holding a truncated file, with the restore reporting success.
	// The cost is one attribute read per file actually copied, not
	// per file walked: a resumed restore skips the files already at size.
	//
	// Only a size the array states counts, and only a read that succeeded.
	// A read-back that cannot be made says nothing about the copy, and
	// failing the attempt over it would add a failure mode to a path whose
	// whole purpose is to make progress; it is logged and the next attempt
	// re-examines the file anyway, because a file is only skipped once it
	// is at the source's size.
	copied, err := client.LookupFileByPath(ctx, targetPath)
	switch {
	case err != nil:
		slog.WarnContext(ctx, "restore: could not read a copied file back to check its size",
			"path", targetPath, "error", err)
	case copied.Size != nil && *copied.Size != entry.Size:
		return false, fmt.Errorf("copying %q from snapshot %s: the snapshot holds %d bytes but %d were copied",
			targetPath, snapshotID, entry.Size, *copied.Size)
	}
	return true, nil
}

// restoreLink rebuilds a second name of a hard-linked file as a link to the
// first name, already staged at first.
//
// Returns whether the name now exists as a link, or was already there from an
// earlier attempt. false with no error means the array refused the link, and
// the caller copies the file instead: that loses the sharing but none of the
// data, so a restore never fails over it.
//
// present/size describe what the target directory listing found for this name,
// as for restoreFile.
func (s *ControllerServer) restoreLink(
	ctx context.Context,
	client qumulo.ClientInterface,
	entry qumulo.DirEntry,
	targetDir, targetPath, first string,
	present bool,
	presentSize uint64,
) (bool, error) {
	if present {
		// An earlier attempt made this name: a link, or the copy that
		// replaced a refused one. Finished only at the source's size, the
		// test restoreFile makes. A copy cut partway is present but short,
		// and taking it for a link committed the restore with the file's
		// tail missing; the caller's restoreFile resumes it instead.
		return presentSize == entry.Size, nil
	}
	if _, err := client.CreateLink(ctx, targetDir, entry.Name, first); err != nil {
		if qumulo.IsConflict(err) {
			return true, nil
		}
		if arrayGaveNoAnswer(err) {
			return false, fmt.Errorf("linking %q to %q: %w", targetPath, first, err)
		}
		slog.WarnContext(ctx, "restore: could not recreate a hard link; copying the file instead",
			"path", targetPath, "linkTo", first, "fileID", entry.ID, "error", err)
		return false, nil
	}
	return true, nil
}

// errRestoreTooLarge reports that a restore exceeded a configured ceiling. It
// is reported as ResourceExhausted, which tells the CO the request will not
// succeed on retry -- unlike Aborted, which invites one.
var errRestoreTooLarge = errors.New("snapshot is too large to restore")

// qumuloFileTypeRegular is the type a snapshot reports for an ordinary file.
const qumuloFileTypeRegular = "FS_FILE_TYPE_FILE"

// isRestoreStaging reports whether a directory name is a restore in progress,
// so listings and cleanups can tell one from a real volume.
func isRestoreStaging(name string) bool {
	return strings.HasPrefix(name, restoreTempPrefix)
}

// createVolumeFromSnapshot provisions a volume whose contents come from a
// snapshot.
//
// The ordering is what makes this safe to interrupt:
//
//  1. assemble the tree under a staging name, so nothing observes a partial
//     volume;
//  2. copy until the budget runs out, returning Aborted so the CO retries and
//     the next attempt resumes;
//  3. set the mode and the quota only once the copy is complete -- a quota
//     applied early could reject the copy that is still filling the volume;
//  4. rename into place. That rename is the commit point.
//
// A retry that finds the finished volume takes the ordinary adoption path in
// CreateVolume instead of reaching here twice.
func (s *ControllerServer) createVolumeFromSnapshot(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
	name string,
	capacity uint64,
	source snapshotID,
	tenantID int,
) (*csi.CreateVolumeResponse, error) {
	// One restore per volume name at a time. The spec's answer for a
	// concurrent operation on one volume is ABORTED.
	release, err := s.restores.acquire(name)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "%v", err)
	}
	defer release()
	// And only so many restores at once. One attempt holds its directory
	// listings in memory, and a backup tool's namespace restore used to run
	// one per claim in parallel on a controller with no memory request.
	// Aborted, like the per-name lock: the CO retries.
	if s.restoreSlots != nil {
		select {
		case s.restoreSlots <- struct{}{}:
			defer func() { <-s.restoreSlots }()
		default:
			return nil, status.Errorf(codes.Aborted,
				"%d restores are already running on this controller; this one starts on the CO's retry", maxConcurrentRestores)
		}
	}

	// If a previous attempt already committed, adopt it rather than restoring
	// again -- the CO retries when a response is lost, not only when a call
	// failed.
	//
	// Through adoptVolume, which is the same body the non-restore adoption
	// path uses. This branch used to return here directly, applying nothing at
	// all: not the mode, not the export, not the policy, and without reading
	// the quota -- it reported the *requested* capacity as though it were the
	// volume's, which for a retry asking a different size was simply a
	// fictional number. A restored volume adopted this way could be one the
	// workload could not write to, or one with no export.
	//
	// The lookup stays here rather than being delegated because this path has
	// to tell "not restored yet" from "cannot tell": a missing directory means
	// resume the restore, and anything else has to surface. adoptExistingVolume
	// folds both into errNoExistingVolume, which is right for a fresh create
	// and wrong here.
	realPath := params.realPathFor(name)
	if existing, err := client.LookupFileByPath(ctx, realPath); err == nil {
		slog.InfoContext(ctx, "createvolume: snapshot restore already completed; adopting",
			"volume", name, "fileID", existing.ID)

		resp, adoptErr := s.adoptVolume(ctx, client, params, name, capacity, existing, tenantID)
		if errors.Is(adoptErr, errNoExistingVolume) {
			// The directory is there but its quota is not readable. On this
			// path that is not an ambiguous half-create: the quota is set
			// before the rename and the rename is the commit, so a committed
			// restore always has one. An unreadable quota here is a cluster
			// error, not a reason to restore again.
			return nil, status.Errorf(codes.Internal,
				"restored volume %q exists but its quota could not be read", realPath)
		}
		if adoptErr != nil {
			return nil, adoptErr
		}
		return &csi.CreateVolumeResponse{
			Volume: withContentSource(resp.GetVolume(), source),
		}, nil
	} else if !qumulo.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "checking for an existing volume %q: %v", realPath, err)
	}

	// Which directory the snapshot was taken of. A snapshot ID carries only
	// the cluster and the snapshot's own ID, so the source has to be read from
	// the snapshot and then resolved to a path -- that path is what
	// ListDirectory walks.
	snapInfo, err := client.GetSnapshot(ctx, source.ID)
	if err != nil {
		if qumulo.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "source snapshot %q no longer exists", source.String())
		}
		return nil, status.Errorf(codes.Internal, "reading source snapshot %q: %v", source.String(), err)
	}
	// The source's live path, for the log line only -- the walk itself uses
	// the file ID.
	//
	// This used to be the walk's starting point, and resolving to an empty
	// path was a terminal NotFound. That refused the most ordinary snapshot
	// workflow there is: snapshot a volume, delete it, restore it. The
	// snapshot survives a DeleteVolume, but the source directory does not, and
	// POST /v1/files/resolve answers a tree-deleted directory with an empty
	// path -- so the one case the whole feature exists for was the one case
	// that could not work. A failure to resolve is now just a missing log
	// field.
	sourcePath, liveSource, gone := "(deleted)", "", "no longer exists"
	if resolved, err := client.ResolveFileIDs(ctx, []string{snapInfo.SourceFileID}); err != nil {
		slog.WarnContext(ctx, "restore: could not resolve the snapshot's source path; continuing by file ID",
			"snapshot", source.String(), "sourceFileID", snapInfo.SourceFileID, "error", err)
		sourcePath, gone = "(unresolved)", fmt.Sprintf("could not be resolved (%v)", err)
	} else if live := livePath(resolved[snapInfo.SourceFileID]); live != "" {
		sourcePath, liveSource = live, live
	}
	// The snapshot must be of one of this driver's volumes, or of something
	// under this class's own store root; otherwise a hand-written
	// VolumeSnapshotContent restored a snapshot of "/" or of another tenant's
	// tree into a new volume.
	//
	// A source that is gone, or did not resolve, cannot be judged, so the
	// snapshot's name is: one in this driver's form is a snapshot of a
	// volume since deleted, the case restore exists for, and one in any
	// other form needs the class's allowForeignSnapshotRestore -- otherwise
	// deleting a tree first made a snapshot of it restorable anywhere.
	if liveSource == "" && !s.isDriverSnapshotName(snapInfo) && !params.AllowForeignSnapshotRestore {
		return nil, status.Errorf(codes.FailedPrecondition,
			"createvolume: snapshot %s %s, and its source %s, so nothing shows it was taken of one of "+
				"this driver's volumes; nothing was restored. To restore it anyway, use a StorageClass that "+
				"sets allowForeignSnapshotRestore: \"true\"",
			source.ID, s.foreignSnapshotName(snapInfo), gone)
	}
	if why, err := foreignSource(ctx, client, liveSource, snapInfo.SourceFileID); err != nil {
		return nil, err
	} else if why != "" && !pathWithin(params.realStoreRoot(), liveSource) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"createvolume: snapshot %s is of %s and outside this class's store root %q; a restore copies only "+
				"from this driver's volumes or from under the class's own root", source.ID, why, params.realStoreRoot())
	}
	slog.InfoContext(ctx, "createvolume: restoring from a snapshot",
		"volume", name, "snapshot", source.ID,
		"sourceFileID", snapInfo.SourceFileID, "sourcePath", sourcePath)

	// Does the data fit the claim? A Qumulo snapshot carries no size, so the
	// provisioner's own restoreSize check has nothing to compare for an
	// imported snapshot, and the quota is only set after the copy. Without
	// this, a 1 GiB claim restored from 1.17 GiB of data bound already over
	// its quota and the workload's first write failed ENOSPC.
	// OutOfRange is the CSI code for a volume smaller than its source.
	//
	// Best effort beyond an array that does not answer: an older Core or a
	// token that cannot read aggregates restores without the check, as
	// before, rather than turning a refinement into an outage.
	//
	// The same measurement makes the caps real. The walk only counts
	// what one attempt did, and no attempt gets far into a large tree, so a
	// tree past the caps was never refused -- it stayed Pending for ever.
	// The snapshot's own counts, directories included, decide it up front.
	if usage, err := client.GetTreeUsage(ctx, snapInfo.SourceFileID, source.ID); err != nil {
		if u := unavailableIfNoAnswer(params, didNotAnswer("measuring the snapshot's data", err)); u != nil {
			return nil, u
		}
		slog.WarnContext(ctx, "createvolume: could not measure the snapshot's data; restoring without the size checks",
			"volume", name, "snapshot", source.ID, "sourceFileID", snapInfo.SourceFileID, "error", err)
	} else {
		if entries := usage.Files + usage.Directories; entries > maxRestoreFiles {
			return nil, status.Errorf(codes.ResourceExhausted,
				"cannot restore snapshot %q: %v: it holds %d files and %d directories, more than %d entries",
				source.String(), errRestoreTooLarge, usage.Files, usage.Directories, maxRestoreFiles)
		}
		if usage.Capacity > maxRestoreBytes {
			return nil, status.Errorf(codes.ResourceExhausted,
				"cannot restore snapshot %q: %v: it holds %d bytes, more than %d",
				source.String(), errRestoreTooLarge, usage.Capacity, maxRestoreBytes)
		}
		if usage.Capacity > capacity {
			return nil, status.Errorf(codes.OutOfRange,
				"snapshot %q holds %d bytes, more than the %d bytes requested for volume %q; request at least %d bytes",
				source.String(), usage.Capacity, capacity, name, usage.Capacity)
		}
	}

	// The totals above are aggregates the array settles in the background:
	// a snapshot taken seconds after a large write reported 24 KiB for
	// 1.2 GB, and a 1 GiB claim restored all of it. The file
	// sizes a listing gives are right at once, so they are added up too and
	// the larger figure decides. Remembered per snapshot, which never
	// changes, so a retried restore does not list the tree twice.
	if err := s.checkRestoreFits(ctx, client, params, name, source, snapInfo.SourceFileID, capacity); err != nil {
		return nil, err
	}

	staging := restoreTempPathFor(params, name)
	stagingInfo, err := client.CreateDirectory(ctx, params.realStoreRoot(), restoreTempPrefix+name)
	if err != nil {
		if qumulo.IsNotFound(err) {
			return nil, storeRootMissingError(params.realStoreRoot(), params.Server)
		}
		if !qumulo.IsConflict(err) {
			return nil, status.Errorf(codes.Internal, "creating the restore staging directory %q: %v", staging, err)
		}
		// Left by an earlier attempt; continue filling it.
		if stagingInfo, err = client.LookupFileByPath(ctx, staging); err != nil {
			return nil, status.Errorf(codes.Internal, "reading the restore staging directory %q: %v", staging, err)
		}
	}

	budget, cancel := restoreBudget(ctx)
	defer cancel()

	progressKey := restoreProgressKey(apiURLFor(params.Server, params.APIURL), staging, source.ID)
	memo := s.restoreProgress.take(progressKey, stagingInfo.ID)
	files, bytes, restoreErr := s.restoreFromSnapshot(budget, client, source.ID, snapInfo.SourceFileID, staging, memo)
	slog.InfoContext(ctx, "createvolume: restore progress",
		"volume", name, "snapshot", source.ID, "filesCopied", files, "bytesCopied", bytes)

	switch {
	case errors.Is(restoreErr, errRestoreTooLarge):
		// Not retryable: say so. A ResourceExhausted CreateVolume makes no
		// PV, so no DeleteVolume ever comes for this name: the staging
		// directory is removed by CreateVolume's sweep of this store root,
		// once the claim is deleted (restore_staging.go).
		return nil, status.Errorf(codes.ResourceExhausted,
			"cannot restore snapshot %q: %v", source.String(), restoreErr)
	case errors.Is(restoreErr, context.DeadlineExceeded), errors.Is(restoreErr, context.Canceled):
		// Normal for a large tree. What has been copied stays staged.
		return nil, status.Errorf(codes.Aborted,
			"restore of snapshot %q is still in progress: %d files copied so far; retrying will continue",
			source.String(), files)
	case restoreErr != nil:
		// An array that did not answer, or asked to be called back later,
		// says nothing final about the restore: Unavailable, which
		// csi-provisioner retries, and the retry resumes what is staged.
		// Internal was final to it, and a restore stalled under an array's
		// throughput cap ended its claim's provisioning outright.
		if u := unavailableIfNoAnswer(params, didNotAnswer("restoring snapshot "+source.String(), restoreErr)); u != nil {
			return nil, u
		}
		if arrayAskedToRetry(restoreErr) {
			return nil, status.Errorf(codes.Unavailable,
				"restoring snapshot %q: the array asked to be called again later (%v); %d files copied so far, "+
					"and retrying will continue", source.String(), restoreErr, files)
		}
		return nil, status.Errorf(codes.Internal, "restoring snapshot %q: %v", source.String(), restoreErr)
	}

	// Only now is the content complete, so only now do mode and quota apply.
	// The root is the restore's own directory, not an entry of the snapshot,
	// so it gets a fresh volume's NFS owner outright; the entries under
	// it keep the owners the snapshot gave them.
	if err := s.ensureDirectoryAccess(ctx, client, params, name, stagingInfo.ID, nil); err != nil {
		return nil, err
	}
	// ensureQuota, not CreateQuota: an attempt that got this far and then
	// failed before the rename left the quota behind, and a create would
	// answer 409 on every retry, so the commit below would never run.
	if err := ensureQuota(ctx, client, stagingInfo.ID, capacity); err != nil {
		return nil, status.Errorf(codes.Internal, "setting quota on the restored volume %q: %v", staging, err)
	}

	// The commit. Before this the volume does not exist; after it, it is whole.
	if _, err := client.Rename(ctx, params.realStoreRoot(), name, staging); err != nil {
		return nil, status.Errorf(codes.Internal,
			"renaming the restored volume into place (%q -> %q): %v", staging, realPath, err)
	}
	s.restoreProgress.forget(progressKey)

	// After the rename, because both steps address the volume at its final
	// path: the export's fs_path is realPathFor(name), which does not exist
	// until the commit above.
	//
	// This used to call ensureExport alone, which meant a restored volume
	// never got the snapshot policy its class asked for -- not on a retry, but
	// on a completely successful restore. ensurePolicy had exactly one caller
	// in the tree, the fresh-create path. An operator setting snapshotSchedule
	// on a class and restoring a PVC from a snapshot got a volume that was
	// never scheduled for backup, silently.
	//
	// No rollback on failure: the rename above committed the volume, so the
	// directory is now the complete restored copy, and tree-deleting it made
	// every retry copy the whole snapshot again for as long as the side effect
	// kept failing. The next attempt finds the volume at its final path
	// and adopts it, and adoption re-applies the export and the policy, both
	// idempotent.
	if err := s.ensureVolumeSideEffects(ctx, client, params, name, stagingInfo.ID, tenantID); err != nil {
		slog.WarnContext(ctx, "createvolume: restored volume committed, but its export or policy failed; "+
			"the retry adopts it and applies them again", "volume", name, "error", err)
		return nil, err
	}

	slog.InfoContext(ctx, "createvolume: restore complete",
		"volume", name, "snapshot", source.ID, "files", files, "bytes", bytes)

	return &csi.CreateVolumeResponse{
		Volume: withContentSource(newCSIVolume(params, name, stagingInfo.ID, capacity), source),
	}, nil
}

// arrayAskedToRetry reports whether err is the array answering 429 or 503 --
// busy, throttled, or briefly unable to serve -- after the client's own
// retries (qumulo.retryWait) ran out.
func arrayAskedToRetry(err error) bool {
	var apiErr *qumulo.APIError
	return errors.As(err, &apiErr) &&
		(apiErr.StatusCode == http.StatusTooManyRequests || apiErr.StatusCode == http.StatusServiceUnavailable)
}

// withContentSource records which snapshot a volume came from. A CO that asked
// for a restore checks this to consider the request satisfied.
func withContentSource(vol *csi.Volume, source snapshotID) *csi.Volume {
	vol.ContentSource = &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: source.String()},
		},
	}
	return vol
}

// reclaimAbandonedRestore removes the staging directory of an interrupted
// restore, best effort.
//
// It runs from DeleteVolume, which is handed only a volume ID -- no
// StorageClass parameters, so no store path. The staging directory is found
// relative to the volume's own directory: a sibling carrying the staging
// prefix. When the restore never got as far as creating anything the lookup
// simply finds nothing, which is the common case and not an error.
//
// This one runs only when the volume's own directory exists. A restore whose
// claim was deleted before it committed has none, and its staging directory
// is reclaimed by reclaimStagingOfGoneVolume or by CreateVolume's sweep
// (restore_staging.go).
//
// volumePath is passed in from resolveVolumeForDelete rather than resolved
// here, and that is load-bearing rather than a tidy-up. This function derives
// a path to tree-delete from its argument, so deriving it from an unverified
// resolution meant a file ID that had been reused -- or written by hand into a
// static PV's volumeHandle -- produced a staging path beside a directory that
// was never this volume's, and deleted that. The caller now only reaches here
// once the path is confirmed to be this volume's own.
//
// Failures are logged rather than returned: DeleteVolume's contract is about
// the volume, and refusing to delete it because a leftover could not be swept
// would strand the PV.
func (s *ControllerServer) reclaimAbandonedRestore(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeID,
	volumePath string,
) {
	staging := path.Join(path.Dir(volumePath), restoreTempPrefix+parsed.Name)
	info, err := client.LookupFileByPath(ctx, staging)
	if err != nil {
		return // nothing staged: the ordinary case
	}

	slog.InfoContext(ctx, "deletevolume: reclaiming an interrupted restore",
		"volume", parsed.Name, "staging", staging, "fileID", info.ID)
	if removeStaging(ctx, client, staging, info.ID, "deletevolume") {
		s.observeRestoreStaging(ctx, parsed.APIEndpoint(), qumulo.RestoreStagingRemoved)
	}
}

// pathWithin reports whether p is root or lies below it.
func pathWithin(root, p string) bool {
	root, p = path.Clean(root), path.Clean(p)
	if root == p {
		return true
	}
	_, below := relPathUnder(root, p)
	return below
}

// mayBeHardLinked reports whether entry can share its inode with another
// name: not when the array reports one link.
func mayBeHardLinked(entry qumulo.DirEntry) bool {
	return entry.NumLinks == nil || *entry.NumLinks > 1
}

// errStopWalk ends a listing early when the walk over its pages has failed;
// the walk's own error is what is returned.
var errStopWalk = errors.New("restore walk stopped")

// checkRestoreFits refuses a restore whose snapshot's files, added up, are
// more than capacity, or more than the restore caps.
//
// Best effort beyond a sum already too large, like the totals check before
// it: a walk the array did not answer, on a class that asks for that, is
// Unavailable; any other failure, or a tree too large to list within
// restoreMeasureBudget, restores on the array's totals alone, with a warning.
func (s *ControllerServer) checkRestoreFits(
	ctx context.Context, client qumulo.ClientInterface, params volumeParams, name string,
	source snapshotID, sourceFileID string, capacity uint64,
) error {
	stopAbove := capacity
	if stopAbove == 0 || stopAbove > maxRestoreBytes {
		stopAbove = maxRestoreBytes
	}
	data, err := s.snapshotData(ctx, client, source.APIEndpoint(), source.ID, sourceFileID, stopAbove, restoreMeasureBudget)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.Errorf(codes.Aborted, "restore of snapshot %q: measuring its files: %v; retrying will continue",
			source.String(), ctxErr)
	}
	switch {
	case data.bytes > maxRestoreBytes:
		return status.Errorf(codes.ResourceExhausted,
			"cannot restore snapshot %q: %v: its files hold at least %d bytes, more than %d",
			source.String(), errRestoreTooLarge, data.bytes, maxRestoreBytes)
	case data.entries > maxRestoreFiles:
		return status.Errorf(codes.ResourceExhausted,
			"cannot restore snapshot %q: %v: it holds more than %d entries",
			source.String(), errRestoreTooLarge, maxRestoreFiles)
	case capacity > 0 && data.bytes > capacity:
		return status.Errorf(codes.OutOfRange,
			"snapshot %q holds at least %d bytes of files, more than the %d bytes requested for volume %q; "+
				"request at least %d bytes",
			source.String(), data.bytes, capacity, name, data.bytes)
	}
	if err != nil {
		if u := unavailableIfNoAnswer(params, didNotAnswer("adding up the snapshot's files", err)); u != nil {
			return u
		}
		slog.WarnContext(ctx, "createvolume: could not add up the snapshot's files; the size check used the array's totals alone",
			"volume", name, "snapshot", source.ID, "filesCounted", data.entries, "bytesCounted", data.bytes, "error", err)
	}
	return nil
}

// directoryPager is a client that lists a directory a page at a time --
// *qumulo.Client.
type directoryPager interface {
	ListDirectoryPages(ctx context.Context, path, snapshotID string, fn func([]qumulo.DirEntry) error) error
}

// listDirectoryPages lists a directory through fn a page at a time where the
// client can, and as one page otherwise.
func listDirectoryPages(ctx context.Context, client qumulo.ClientInterface, path, snapshotID string, fn func([]qumulo.DirEntry) error) error {
	if pager, ok := client.(directoryPager); ok {
		return pager.ListDirectoryPages(ctx, path, snapshotID, fn)
	}
	entries, err := client.ListDirectory(ctx, path, snapshotID)
	if err != nil {
		return err
	}
	return fn(entries)
}

// slimTargetEntry is what the walk keeps of an entry already in the target:
// its kind, size and attributes. The name is the map's key, and the path and
// ID are never read again.
func slimTargetEntry(e qumulo.DirEntry) qumulo.DirEntry {
	e.Name, e.Path, e.ID = "", "", ""
	return e
}
