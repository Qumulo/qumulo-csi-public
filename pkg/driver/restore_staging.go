// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"log/slog"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// A restore whose claim is deleted mid-copy leaves its staging directory,
// `<store root>/.csi-restoring-<name>`, on the array: up to the snapshot's
// whole size, under no quota, with no PV, no PVC and nothing in Kubernetes
// that shows it. csi-provisioner sends DeleteVolume only for a PV, and
// a claim deleted while its CreateVolume was still failing has none; and
// DeleteVolume used to reach its only remover, reclaimAbandonedRestore, only
// when the volume's final path existed -- which, for a restore that never
// committed, it does not.
//
// Two paths remove one now. DeleteVolume, finding the volume's directory
// already gone, reclaims that volume's own staging sibling
// (reclaimStagingOfGoneVolume). And a successful CreateVolume sweeps its
// class's store root, at most every 10 minutes per array and root, for
// staging directories whose claim is gone (maybeSweepRestoreStaging). There is
// no goroutine running on a timer: a controller that provisions nothing
// sweeps nothing, and a store root nobody provisions into again keeps its
// leftovers until something does (or the operator removes them; the metric
// and the log say where). That is the deliberate limit of this design, and
// it is why the metric matters: a cluster that provisions in bursts, one
// whose classes have separate store roots, or one whose controller restarts
// often enough to lose its strikes may not reclaim for a long time, or at
// all. The strikes and the per-root schedule are in memory and go with the
// process.

// restoreStagingStrikeAge is how long a staging directory must have been
// seen with no claim and no PV, by an earlier sweep, before a sweep removes
// it. Long enough that a claim being re-created, a PV being written, or a
// restore in flight on another controller replica has shown itself.
const restoreStagingStrikeAge = 15 * time.Minute

// restoreStagingListBudget bounds the sweep's directory listing and each of
// its two Kubernetes lists. A store root too large to list in that time is
// not swept, which is the safe failure: nothing is removed.
const restoreStagingListBudget = 2 * time.Second

// restoreStagingSweepBudget bounds a whole sweep, removals included. The
// sweep runs after CreateVolume has answered, on a context detached from the
// request's cancellation and bounded by this.
const restoreStagingSweepBudget = 10 * time.Second

// maxStagingRemovalsPerSweep bounds the removals one sweep starts; the rest
// wait for the next sweep of the same root.
const maxStagingRemovalsPerSweep = 10

// claimNamedVolume matches the name csi-provisioner gives a volume by
// default, pvc-<claim UID>, and captures the UID.
var claimNamedVolume = regexp.MustCompile(`^pvc-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$`)

// claimUIDOf returns the claim UID a volume name carries, or false for a
// name not in csi-provisioner's default form.
func claimUIDOf(volumeName string) (string, bool) {
	m := claimNamedVolume.FindStringSubmatch(volumeName)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// restoreStagingSweeper is the sweep's state: when each store root was last
// swept, and which staging directories an earlier sweep found with no claim.
// Both are hints in Rule 0's sense -- losing them costs one more sweep before
// a leftover is removed -- and neither holds a client, a credential or
// anything from a request beyond the array endpoint and a path.
type restoreStagingSweeper struct {
	now func() time.Time
	// run starts a sweep. Nil runs it on its own goroutine, after
	// CreateVolume has answered; tests run it inline.
	run func(func())

	// roots maps "<endpoint>\x00<store root>" to the *hintSweeper that
	// limits it to one sweep per hintSweepEvery.
	roots sync.Map

	mu sync.Mutex
	// strikes maps "<endpoint>\x00<staging path>" to the first sweep that
	// found that directory with no claim and no PV.
	strikes map[string]stagingStrike
}

// stagingStrike is a staging directory a sweep found claimless: its file ID,
// so a directory removed and made again is a new one; what its tree held
// then, so a copy still filling it is seen to be alive; and when it was first
// found so with that content, and last seen.
type stagingStrike struct {
	fileID      string
	usage       qumulo.TreeUsage
	first, last time.Time
}

func newRestoreStagingSweeper() *restoreStagingSweeper {
	return &restoreStagingSweeper{now: time.Now, strikes: map[string]stagingStrike{}}
}

// due reports whether the store root at key is the caller's to sweep now.
func (w *restoreStagingSweeper) due(key string, now time.Time) bool {
	v, _ := w.roots.LoadOrStore(key, &hintSweeper{})
	return v.(*hintSweeper).due(now)
}

// prune drops the hints past their use: a store root not swept for an hour,
// and a strike not seen for an hour. Called by each sweep, so the maps are
// bounded by the roots and leftovers seen in the last hour.
func (w *restoreStagingSweeper) prune(now time.Time) {
	cutoff := now.Add(-idleHintExpiry).UnixNano()
	w.roots.Range(func(k, v any) bool {
		if v.(*hintSweeper).last.Load() < cutoff {
			w.roots.CompareAndDelete(k, v)
		}
		return true
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	for k, s := range w.strikes {
		if now.Sub(s.last) >= idleHintExpiry {
			delete(w.strikes, k)
		}
	}
}

// strike records that the staging directory at key, with fileID and
// holding usage, was found claimless at now. It reports whether this is the
// first sighting -- the directory is new to the sweep, or was made again
// since -- whether its content changed since the strike was recorded, which
// starts the strike again, and whether an earlier sweep already saw it so,
// with the same content, at least restoreStagingStrikeAge ago.
func (w *restoreStagingSweeper) strike(key, fileID string, usage qumulo.TreeUsage, now time.Time) (first, changed, ripe bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	s, ok := w.strikes[key]
	switch {
	case !ok || s.fileID != fileID:
		w.strikes[key] = stagingStrike{fileID: fileID, usage: usage, first: now, last: now}
		return true, false, false
	case s.usage != usage:
		w.strikes[key] = stagingStrike{fileID: fileID, usage: usage, first: now, last: now}
		return false, true, false
	}
	s.last = now
	w.strikes[key] = s
	return false, false, now.Sub(s.first) >= restoreStagingStrikeAge
}

// keep marks key's strike seen at now without judging it: a sweep that could
// not read the directory's content neither ripens nor drops it.
func (w *restoreStagingSweeper) keep(key string, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if s, ok := w.strikes[key]; ok {
		s.last = now
		w.strikes[key] = s
	}
}

// clear forgets key's strike: its claim or PV exists, a restore is running
// for it, or it is gone.
func (w *restoreStagingSweeper) clear(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.strikes, key)
}

// clearMissing forgets every strike for a directory directly under root on
// the array keyed by prefix whose key is not in seen: one the latest listing
// of root no longer has. A strike under a deeper root -- an adDomain class
// nests its root below another class's -- is that root's sweep's to judge.
func (w *restoreStagingSweeper) clearMissing(prefix, root string, seen map[string]bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for k := range w.strikes {
		staging, ok := strings.CutPrefix(k, prefix)
		if ok && path.Dir(staging) == root && !seen[k] {
			delete(w.strikes, k)
		}
	}
}

// maybeSweepRestoreStaging starts a sweep of the class's store root for the
// staging directories of restores whose claim is gone, when one is due: at
// most once per hintSweepEvery per array and store root. It is called after a
// CreateVolume has succeeded, and never changes that answer.
//
// The sweep uses the request's own client -- this controller holds no other
// (Rule 0) -- on a context detached from the request's cancellation and
// bounded by restoreStagingSweepBudget, and keeps nothing of it afterwards.
// It needs both Kubernetes listers; without them (outside a cluster) there is
// no way to tell an abandoned restore from a running one, so nothing is swept.
func (s *ControllerServer) maybeSweepRestoreStaging(ctx context.Context, client qumulo.ClientInterface, params volumeParams) {
	if s.driver == nil || s.driver.PersistentVolumeLister == nil || s.driver.PersistentVolumeClaimLister == nil {
		return
	}
	w := s.stagingSweeper
	endpoint := apiURLFor(params.Server, params.APIURL)
	root := params.realStoreRoot()
	now := w.now()
	if !w.due(endpoint+"\x00"+root, now) {
		return
	}
	sweep := func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), restoreStagingSweepBudget)
		defer cancel()
		s.sweepRestoreStaging(sctx, client, endpoint, root)
	}
	if w.run != nil {
		w.run(sweep)
		return
	}
	go sweep()
}

// sweepRestoreStaging lists root for restore staging directories and removes
// each one only when all of these hold:
//
//   - its name is `.csi-restoring-pvc-<UID>`, so it names a claim to ask
//     about; any other is logged and left;
//   - no PersistentVolumeClaim has that UID, and no PersistentVolume of this
//     driver has the volume's name -- one paged List of each, and a failed
//     list removes nothing;
//   - this controller is not restoring it now. An earlier attempt's progress
//     memo does not count: the claim is already gone by this point, so the
//     retry the memo is kept for cannot arrive (see the case itself);
//   - an earlier sweep, at least restoreStagingStrikeAge before, found the
//     same directory (by file ID) claimless too, holding exactly what it
//     holds now (its tree's capacity, file and directory counts).
//
// The last is what keeps a restore on another Kubernetes cluster alive. Two
// clusters can share one array's store root -- a DR cluster, or two
// clusters on one class root -- and this cluster's claims and PVs say
// nothing about the other's, nor do this process's restore locks. A restore
// that is copying changes its tree between sweeps, and each change starts
// the 15 minutes again; only a directory nothing has written to for 15
// minutes is removed. A restore elsewhere stalled that long (an array
// answering 503 for 15 minutes) can still be removed, and starts over on its
// next attempt. Qumulo's aggregates can trail a write by a few seconds,
// which only delays a removal.
//
// Best effort throughout: every failure is logged and leaves the directory.
func (s *ControllerServer) sweepRestoreStaging(ctx context.Context, client qumulo.ClientInterface, endpoint, root string) {
	w := s.stagingSweeper
	now := w.now()
	w.prune(now)

	var staged []qumulo.DirEntry
	listCtx, cancel := context.WithTimeout(ctx, restoreStagingListBudget)
	err := listDirectoryPages(listCtx, client, root, "", func(page []qumulo.DirEntry) error {
		for _, e := range page {
			if isRestoreStaging(e.Name) && e.IsDirectory() {
				staged = append(staged, e)
			}
		}
		return nil
	})
	cancel()
	if err != nil {
		slog.InfoContext(ctx, "createvolume: could not list the store root for abandoned restore staging directories; "+
			"none removed, the next sweep tries again", "storeRoot", root, "error", err)
		return
	}
	prefix := endpoint + "\x00"
	seen := make(map[string]bool, len(staged))
	for _, e := range staged {
		seen[prefix+path.Join(root, e.Name)] = true
	}
	w.clearMissing(prefix, path.Clean(root), seen)
	if len(staged) == 0 {
		return
	}

	claims, volumes, ok := s.listClaimsAndVolumes(ctx, root)
	if !ok {
		return
	}

	removed := 0
	for _, e := range staged {
		name := strings.TrimPrefix(e.Name, restoreTempPrefix)
		staging := path.Join(root, e.Name)
		key := prefix + staging
		uid, named := claimUIDOf(name)
		switch {
		case !named:
			slog.WarnContext(ctx, "createvolume: a restore staging directory is not named after a claim, so the "+
				"driver cannot tell whether its restore is abandoned; left alone, remove it by hand if no restore "+
				"of that volume is running", "staging", staging, "fileID", e.ID)
			continue
		case claims[uid], volumes[name]:
			w.clear(key)
			continue
		// Only a restore running here holds the directory. Deliberately not
		// restoreRunningOrPending: that also counts an earlier attempt's
		// progress memo, which lives restoreProgressTTL (an hour) past the
		// attempt that made it. This case is reached only once the claim is
		// gone and no PV carries the name, and csi-provisioner sends no
		// further CreateVolume for a deleted claim -- a UID is never reused
		// -- so the retry the memo is kept for cannot arrive. Counting it
		// here held every abandoned directory for an hour before the 15
		// minutes below could even start, and held it silently: the strike
		// was cleared here, above the log and the metric that report one.
		// The lock is what matters and it is enough:
		// createVolumeFromSnapshot takes it before it creates the staging
		// directory and holds it until the restore commits or fails, and
		// removeAbandonedStaging takes the same lock before it deletes.
		case s.restores.busy(name):
			w.clear(key)
			continue
		}

		// Read just before the strike is judged, and the removal follows at
		// once with only a lookup between, so the content judged unchanged
		// is the content removed; a second read there would add a call and
		// close no window worth closing.
		usageCtx, cancel := context.WithTimeout(ctx, restoreStagingListBudget)
		usage, err := client.GetTreeUsage(usageCtx, e.ID, "")
		cancel()
		if err != nil {
			slog.InfoContext(ctx, "createvolume: could not read what a restore staging directory holds, so it is "+
				"not judged this sweep", "staging", staging, "fileID", e.ID, "error", err)
			w.keep(key, now)
			continue
		}
		first, changed, ripe := w.strike(key, e.ID, usage, now)
		switch {
		case first:
			slog.WarnContext(ctx, "createvolume: found the staging directory of a restore whose claim and volume "+
				"are gone from this cluster; it is removed if nothing writes to it and later sweeps still find it "+
				"so for 15 minutes", "staging", staging, "fileID", e.ID, "claimUID", uid,
				"bytes", usage.Capacity, "files", usage.Files, "directories", usage.Directories)
			s.observeRestoreStaging(ctx, endpoint, qumulo.RestoreStagingAbandoned)
		case changed:
			slog.InfoContext(ctx, "createvolume: a restore staging directory with no claim on this cluster is still "+
				"being written, perhaps by a restore on another cluster sharing this store root; its 15 minutes "+
				"start again", "staging", staging, "fileID", e.ID, "claimUID", uid,
				"bytes", usage.Capacity, "files", usage.Files, "directories", usage.Directories)
		}
		if !ripe || removed >= maxStagingRemovalsPerSweep {
			continue
		}
		if s.removeAbandonedStaging(ctx, client, name, staging, e.ID) {
			removed++
			w.clear(key)
			slog.WarnContext(ctx, "createvolume: removed the staging directory of a restore whose claim was deleted",
				"staging", staging, "fileID", e.ID, "claimUID", uid)
			s.observeRestoreStaging(ctx, endpoint, qumulo.RestoreStagingRemoved)
		}
	}
}

// listClaimsAndVolumes reads every claim UID, and the names of this driver's
// PVs, each in one paged List bounded by restoreStagingListBudget. ok is
// false when either failed: a partial answer would read as "this claim is
// gone".
func (s *ControllerServer) listClaimsAndVolumes(ctx context.Context, root string) (claims, volumes map[string]bool, ok bool) {
	pvcCtx, cancel := context.WithTimeout(ctx, restoreStagingListBudget)
	uids, err := s.driver.PersistentVolumeClaimLister.ListPersistentVolumeClaimUIDs(pvcCtx)
	cancel()
	if err != nil {
		slog.WarnContext(ctx, "createvolume: could not list PersistentVolumeClaims, so no restore staging directory "+
			"is removed; the controller's ServiceAccount needs list on persistentvolumeclaims",
			"storeRoot", root, "error", err)
		return nil, nil, false
	}
	pvCtx, cancel := context.WithTimeout(ctx, restoreStagingListBudget)
	pvs, err := s.driver.PersistentVolumeLister.ListPersistentVolumes(pvCtx, s.driver.Name)
	cancel()
	if err != nil {
		slog.WarnContext(ctx, "createvolume: could not list PersistentVolumes, so no restore staging directory "+
			"is removed; the controller's ServiceAccount needs list on persistentvolumes",
			"storeRoot", root, "error", err)
		return nil, nil, false
	}
	claims = make(map[string]bool, len(uids))
	for _, uid := range uids {
		claims[uid] = true
	}
	volumes = make(map[string]bool, len(pvs))
	for _, pv := range pvs {
		volumes[pv.Name] = true
	}
	return claims, volumes, true
}

// restoreRunningOrPending reports whether this controller is restoring the
// volume now, or holds an earlier attempt's progress for its staging path,
// which means the CO may be about to retry it.
//
// Only reclaimStagingOfGoneVolume uses it. That path checks the claim after
// this, so the memo is its only guard against a retry still to come. The
// sweep checks the claim first and so asks the narrower question -- see its
// restores.busy case.
func (s *ControllerServer) restoreRunningOrPending(endpoint, staging, name string) bool {
	return s.restores.busy(name) || s.restoreProgress.holds(endpoint, staging)
}

// removeAbandonedStaging removes one staging directory, holding the volume's
// restore lock so no attempt of this controller starts on it meanwhile, and
// only while the path still names the directory the sweeps judged. It
// reports whether the tree delete was started.
func (s *ControllerServer) removeAbandonedStaging(
	ctx context.Context, client qumulo.ClientInterface, name, staging, fileID string,
) bool {
	release, err := s.restores.acquire(name)
	if err != nil {
		return false
	}
	defer release()
	info, err := client.LookupFileByPath(ctx, staging)
	if err != nil || info.ID != fileID || !info.IsDirectory() {
		return false
	}
	return removeStaging(ctx, client, staging, fileID, "createvolume")
}

// removeStaging deletes a staging directory's quota, if it has one, and then
// starts its tree delete, as DeleteVolume does a volume's. rpc labels the
// log lines. It reports whether the tree delete was started.
func removeStaging(ctx context.Context, client qumulo.ClientInterface, staging, fileID, rpc string) bool {
	if err := client.DeleteQuota(ctx, fileID); err != nil && !qumulo.IsNotFound(err) {
		slog.WarnContext(ctx, rpc+": could not remove the staging quota", "staging", staging, "error", err)
	}
	if err := client.StartTreeDelete(ctx, fileID); err != nil && !qumulo.IsNotFound(err) {
		slog.ErrorContext(ctx, rpc+": could not remove an interrupted restore; it needs manual cleanup",
			"staging", staging, "fileID", fileID, "error", err)
		return false
	}
	return true
}

// reclaimStagingOfGoneVolume removes the staging sibling of a volume
// DeleteVolume found already gone: `.csi-restoring-<name>` beside the path the
// handle names, and nothing else. A restore whose claim was deleted
// before it committed has no final directory, so this is the case
// reclaimAbandonedRestore, which runs only once the final directory is
// confirmed, can never reach.
//
// It leaves the directory while a restore of that name may still be running:
// while this controller is restoring it or holds an earlier attempt's
// progress for it, and while a claim whose UID the name carries exists. The
// claim is asked about only when there is a staging directory, so an
// ordinary delete costs no Kubernetes call. A failed claim list leaves it for
// CreateVolume's sweep.
//
// Without a claim lister -- csi-sanity, a local run -- and for a name not in
// csi-provisioner's pvc-<UID> form, it reclaims on the in-process checks
// alone, as DeleteVolume's comment always said it did. That is safe because
// DeleteVolume is sent for a PersistentVolume, and csi-provisioner writes one
// only after a restore commits, which renames the staging directory away; a
// staging directory beside a PV's path is a later attempt's leftover, and a
// new restore of the same name needs the same claim -- the one check this
// skips is the one a hand-written PV naming a restore still in flight would
// need, and only a controller outside a cluster lacks it.
//
// It makes no progress check, unlike the sweep: it is reached only for a PV
// of this cluster, and a PV csi-provisioner made here carries the UID of a
// claim of this cluster, which no other cluster sharing the store root can be
// restoring -- claim UIDs are unique. Only a hand-written PV whose handle
// names another cluster's restore in flight could reach one.
//
// Best effort: DeleteVolume's answer is about the volume, and is never
// changed by this.
func (s *ControllerServer) reclaimStagingOfGoneVolume(ctx context.Context, client qumulo.ClientInterface, parsed volumeID) {
	staging := path.Join(path.Dir(parsed.RealPath), restoreTempPrefix+parsed.Name)
	info, err := client.LookupFileByPath(ctx, staging)
	if err != nil || !info.IsDirectory() {
		return // nothing staged: the ordinary case
	}
	endpoint := parsed.APIEndpoint()
	if s.restoreRunningOrPending(endpoint, staging, parsed.Name) {
		slog.InfoContext(ctx, "deletevolume: the volume is gone, but a restore of it may still be running here; "+
			"its staging directory is left", "volume", parsed.Name, "staging", staging)
		return
	}
	if lister := s.driver.PersistentVolumeClaimLister; lister != nil {
		if uid, ok := claimUIDOf(parsed.Name); ok {
			uids, err := lister.ListPersistentVolumeClaimUIDs(ctx)
			if err != nil {
				slog.WarnContext(ctx, "deletevolume: could not list PersistentVolumeClaims, so the staging directory "+
					"of the volume's restore is left for CreateVolume's sweep", "volume", parsed.Name,
					"staging", staging, "error", err)
				return
			}
			for _, u := range uids {
				if u == uid {
					slog.InfoContext(ctx, "deletevolume: the volume is gone, but its claim still exists and its "+
						"restore may still be running; the staging directory is left",
						"volume", parsed.Name, "staging", staging)
					return
				}
			}
		}
	}
	release, err := s.restores.acquire(parsed.Name)
	if err != nil {
		return
	}
	defer release()
	slog.WarnContext(ctx, "deletevolume: the volume is gone; removing the staging directory of its interrupted restore",
		"volume", parsed.Name, "staging", staging, "fileID", info.ID)
	if removeStaging(ctx, client, staging, info.ID, "deletevolume") {
		s.observeRestoreStaging(ctx, endpoint, qumulo.RestoreStagingRemoved)
	}
}

// observeRestoreStaging counts a staging directory found abandoned or
// removed, when metrics are on.
func (s *ControllerServer) observeRestoreStaging(ctx context.Context, endpoint, state string) {
	if s.driver == nil || s.driver.Observer == nil {
		return
	}
	s.driver.Observer.ObserveRestoreStaging(ctx, qumulo.RestoreStagingEvent{
		Cluster: qumulo.ClusterLabel(endpoint),
		State:   state,
	})
}
