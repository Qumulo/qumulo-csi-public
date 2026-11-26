// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// DefaultRevertSnapshotRetention is how long a replication revert snapshot is
// kept before the array deletes it.
//
// Every make-writable (a promote or a DR drill) and every reconnect (a
// resync or failback) makes the target array revert the target root to its
// recovery point, and before it does, the array snapshots the root as it was:
// "<id>_revert_to_<recovery point>_for_<name>". It gives that snapshot no
// expiration and nothing removes it, so each failover or failback left one
// more snapshot holding the discarded blocks for ever -- 125 on the two lab
// arrays in three weeks. It is also the only copy of what the revert threw
// away, which is why it is given a week rather than deleted.
const DefaultRevertSnapshotRetention = 7 * 24 * time.Hour

// snapshotExpirer is a client that can give a snapshot an expiration --
// *qumulo.Client. Asserted rather than added to ReplicationClient so the
// replication fakes need not implement it; one that does not is left alone.
type snapshotExpirer interface {
	SetSnapshotExpiration(ctx context.Context, snapshotID string, expiration time.Time) (qumulo.SnapshotInfo, error)
}

// expireRevertSnapshots gives every revert snapshot the array holds of root
// that has no expiration one, retention from now, and returns root's revert
// snapshots as they stand after it (with the expirations it set), or nil
// when there is nothing to do or the listing failed.
//
// Matched by the array's own naming, by the directory it was taken of, and
// by having no expiration: a revert snapshot an administrator already gave
// an expiration, or one of another directory, is not touched. All of root's
// unexpiring revert snapshots are stamped, not only the one this call made,
// so one a failed or interrupted earlier call missed is caught by the next.
//
// Best effort: the revert itself has happened and is what the RPC reports,
// so a failure here is logged and never fails the RPC. The snapshot is then
// stamped by the next promote, resync or failback of the same root, and at
// the latest when replication is disabled. It is also called on
// every resync poll of a target, because a reconnect's revert runs after the
// call returns and its snapshot can appear later; awaitingRevertSnapshot
// keeps those polls coming until it has.
func (r *ReplicationServer) expireRevertSnapshots(ctx context.Context, rpc string, client qumulo.ClientInterface, addr, root string) []qumulo.SnapshotInfo {
	retention := r.driver.revertSnapshotRetention()
	if retention <= 0 || root == "" {
		return nil
	}
	expirer, ok := client.(snapshotExpirer)
	if !ok {
		return nil
	}
	snaps, err := client.ListSnapshots(ctx)
	if err != nil {
		slog.WarnContext(ctx, rpc+": could not list snapshots to expire the array's replication revert snapshots; "+
			"the next promote or resync of this volume tries again",
			"array", addr, "rootFileID", root, "error", err)
		return nil
	}
	expiration := time.Now().Add(retention)
	var ofRoot []qumulo.SnapshotInfo
	for _, s := range snaps {
		if s.SourceFileID != root || !s.IsReplicationRevertSnapshot() {
			continue
		}
		if !s.InDelete && s.Expiration.IsZero() {
			if _, err := expirer.SetSnapshotExpiration(ctx, s.ID, expiration); err != nil {
				slog.WarnContext(ctx, rpc+": could not set an expiration on the array's replication revert snapshot; "+
					"the next promote or resync of this volume tries again",
					"array", addr, "snapshot", s.ID, "name", s.Name, "error", err)
			} else {
				s.Expiration = expiration
				slog.InfoContext(ctx, rpc+": replication revert snapshot will expire",
					"array", addr, "snapshot", s.ID, "name", s.Name, "expiration", expiration.UTC().Format(time.RFC3339))
			}
		}
		ofRoot = append(ofRoot, s)
	}
	return ofRoot
}

// DefaultRevertSnapshotWait is how long, after a reconnect this controller
// ran, a resync of the reconnected target keeps answering "not ready" while
// the array's revert snapshot has not appeared.
//
// The array takes that snapshot after the Reconnect call has returned, and
// the csi-addons controller stops calling ResyncVolume as soon as it answers
// ready. A failback whose target already held a recovery point answered
// ready on the first poll, before the snapshot existed, and nothing looked
// for it again: it was kept for ever. Measured on a lab array, the snapshot
// appears within seconds; two minutes is a bound, not an expected wait, so
// an array that takes none costs at most that much on one resync.
const DefaultRevertSnapshotWait = 2 * time.Minute

// revertWait is a reconnect this controller ran, waiting for the array's
// revert snapshot of root: one whose ID is above baseline, the highest ID of
// root's revert snapshots before the reconnect (snapshot IDs only grow on an
// array, which needs no clock shared with it).
type revertWait struct {
	since    time.Time
	baseline int64
}

// revertWaitsKeepFor bounds how long a wait is remembered when no resync
// comes to clear it (the VolumeReplication deleted meanwhile, say).
const revertWaitsKeepFor = time.Hour

func revertWaitKey(addr, root string) string { return addr + "\x00" + root }

// noteReconnect records, before a reconnect of root on client, which revert
// snapshot the reconnect must be followed by. Nothing is recorded -- and a
// resync answers as it did before -- when retention is off, the client
// cannot set expirations, the listing fails, or an ID is not a number: the
// snapshot is then still stamped by any later resync, and when replication
// is disabled. A hint in memory only (ARCHITECTURE.md rule 0): a restarted
// controller has none and answers ready as before.
func (r *ReplicationServer) noteReconnect(ctx context.Context, rpc string, client qumulo.ClientInterface, addr, root string) {
	if r.driver.revertSnapshotRetention() <= 0 || root == "" {
		return
	}
	if _, ok := client.(snapshotExpirer); !ok {
		return
	}
	now := time.Now()
	r.revertWaits.Range(func(k, v any) bool {
		if now.Sub(v.(revertWait).since) > revertWaitsKeepFor {
			r.revertWaits.Delete(k)
		}
		return true
	})
	snaps, err := client.ListSnapshots(ctx)
	if err != nil {
		slog.DebugContext(ctx, rpc+": could not list snapshots before the reconnect; its revert snapshot is stamped "+
			"by a later resync or when replication is disabled", "array", addr, "rootFileID", root, "error", err)
		return
	}
	baseline := int64(-1)
	for _, s := range snaps {
		if s.SourceFileID != root || !s.IsReplicationRevertSnapshot() {
			continue
		}
		id, err := strconv.ParseInt(s.ID, 10, 64)
		if err != nil {
			return
		}
		baseline = max(baseline, id)
	}
	r.revertWaits.Store(revertWaitKey(addr, root), revertWait{since: now, baseline: baseline})
}

// awaitingRevertSnapshot reports whether a resync of root should still
// answer "not ready": a reconnect this controller ran is waiting for its
// revert snapshot, none above the baseline carries an expiration yet in
// snaps (expireRevertSnapshots' result, nil when it listed nothing), and the
// wait has not run out. A wait that runs out is dropped with a warning; the
// snapshot, if it comes, is stamped by a later resync or when replication is
// disabled.
func (r *ReplicationServer) awaitingRevertSnapshot(ctx context.Context, rpc, addr, root string, snaps []qumulo.SnapshotInfo) bool {
	key := revertWaitKey(addr, root)
	v, ok := r.revertWaits.Load(key)
	if !ok {
		return false
	}
	w := v.(revertWait)
	for _, s := range snaps {
		id, err := strconv.ParseInt(s.ID, 10, 64)
		if err == nil && id > w.baseline && !s.Expiration.IsZero() {
			r.revertWaits.Delete(key)
			return false
		}
	}
	wait := r.revertSnapshotWait
	if wait <= 0 {
		wait = DefaultRevertSnapshotWait
	}
	if waited := time.Since(w.since); waited >= wait {
		r.revertWaits.Delete(key)
		slog.WarnContext(ctx, rpc+": the array's revert snapshot for the reconnect has not appeared or could not be "+
			"given an expiration; reporting ready. Any that appears later is stamped when replication is disabled",
			"array", addr, "rootFileID", root, "waited", waited.Round(time.Second))
		return false
	}
	slog.InfoContext(ctx, rpc+": waiting for the array's revert snapshot of the reconnect before reporting ready",
		"array", addr, "rootFileID", root, "waited", time.Since(w.since).Round(time.Second))
	return true
}

// expireRevertSnapshotsOnBothEnds stamps root's revert snapshots on both
// arrays as replication is disabled: the last call the driver gets
// for this relationship, so a revert snapshot that appeared after the last
// resync poll is caught here rather than kept for ever. Each array was the
// target, and so reverted, at some point of a failover and failback; each
// end's root is that array's own directory. Best effort, as the rest.
func (r *ReplicationServer) expireRevertSnapshotsOnBothEnds(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) {
	r.expireRevertSnapshots(ctx, rpc, e.local, e.localAddr, localRoot(e, rel))
	if e.peer != nil {
		r.expireRevertSnapshots(ctx, rpc, e.peer, e.peerAddr, peerRoot(rel))
	}
}

// localRoot is the local array's own file ID for the replicated directory,
// whichever end it is.
func localRoot(e *replicationEnds, rel *relationship) string {
	if e.fileID != "" {
		return e.fileID
	}
	if rel.role == roleTarget {
		return rel.local.TargetRootID
	}
	return rel.local.SourceRootID
}

// peerRoot is the peer array's own file ID for the replicated directory, as
// the peer reports it, or as the local end names it when the peer was not
// read; "" when neither says.
func peerRoot(rel *relationship) string {
	if rel.peer != nil {
		if rel.peerRole == roleTarget {
			return rel.peer.TargetRootID
		}
		return rel.peer.SourceRootID
	}
	if rel.role == roleTarget {
		return rel.local.SourceRootID
	}
	return rel.local.TargetRootID
}

// revertSnapshotRetention is the configured retention, zero for none.
func (d *Driver) revertSnapshotRetention() time.Duration {
	return d.RevertSnapshotRetention
}

// revertRoot is the file ID, on the local array, of the directory a revert
// there snapshots: the replicated root. e.fileID is local's own ID for it,
// right in either role -- after a reverse, rel.local still describes the
// source end it was, whose TargetRootID is the peer's directory.
func revertRoot(e *replicationEnds, rel *relationship) string {
	if e.fileID != "" {
		return e.fileID
	}
	if rel != nil && rel.role == roleTarget {
		return rel.local.TargetRootID
	}
	return ""
}
