// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// The unpromoted marker.
//
// Once Disable has removed both ends of a relationship, a read-write replica
// export over data is all Enable has to tell a promoted copy by
// (peerExportMarksPromotedCopy), and it cannot tell one from an export
// an operator left read-write: a group's covering export on the DR array is
// the operator's, never changed by this driver. A plain Disable and
// re-Enable of such a group, never failed over, was refused and left it
// unprotected.
//
// So Disable, while the peer's end still says the copy is a read-only
// replica, snapshots the replica root: the unpromoted marker. Enable, finding
// a read-write export and no end, compares the root with the newest marker,
// and re-protects only when nothing changed since -- adopting an unchanged
// copy as a replica discards nothing. A copy written since, no marker (an
// earlier build's Disable, a snapshot that failed, one expired), or a
// comparison that fails is refused as before. The decision rests on the
// array's own snapshots, so it survives a controller restart and needs no
// clock.

const (
	// unpromotedMarkerTag begins the name of Disable's marker snapshot of a
	// replica root (the array prepends the snapshot id).
	unpromotedMarkerTag = "csi-unpromoted-"
	// unpromotedMarkerTTL is the expiration the cluster enforces on a
	// marker, as it does freezeMarkerTTL's: a pair left disabled longer than
	// this is refused on re-Enable as before, and a marker nothing removed
	// does not hold the root's old blocks for good.
	unpromotedMarkerTTL = 30 * 24 * time.Hour
	// unpromotedCheckTag names Enable's snapshot of the root to compare with
	// a marker. It is removed at once; the expiration covers a crash.
	unpromotedCheckTag = "csi-reprotect-check-"
	unpromotedCheckTTL = time.Hour
)

// unpromotedProof is what Enable's peer check found: a read-write export over
// a copy unchanged since Disable marked it.
type unpromotedProof struct {
	rootID string
	marker qumulo.SnapshotInfo
	export qumulo.ExportInfo
}

func unpromotedMarkerName(relID string) string {
	if len(relID) > 8 {
		relID = relID[:8]
	}
	return unpromotedMarkerTag + relID
}

// isUnpromotedMarker reports whether s is a marker of the directory rootID.
// The array stores a name as "<id>_<suffix>".
func isUnpromotedMarker(s qumulo.SnapshotInfo, rootID string) bool {
	return s.SourceFileID == rootID && !s.InDelete && !s.CreatedByPolicy() &&
		strings.HasPrefix(strings.TrimPrefix(s.Name, s.ID+"_"), unpromotedMarkerTag)
}

// isReprotectSnapshot reports whether s is an unpromoted marker or Enable's
// snapshot to compare with one, of any root.
func isReprotectSnapshot(s qumulo.SnapshotInfo) bool {
	name := strings.TrimPrefix(s.Name, s.ID+"_")
	return !s.CreatedByPolicy() &&
		(strings.HasPrefix(name, unpromotedMarkerTag) || strings.HasPrefix(name, unpromotedCheckTag))
}

// markedUnpromoted is the newest marker of rootID as proof for export, or nil
// when there is none or the listing fails.
func markedUnpromoted(ctx context.Context, e *replicationEnds, rootID string, export *qumulo.ExportInfo) *unpromotedProof {
	if rootID == "" {
		return nil
	}
	markers, err := unpromotedMarkers(ctx, e.peer, rootID)
	if err != nil || len(markers) == 0 {
		return nil
	}
	return &unpromotedProof{rootID: rootID, marker: markers[0], export: *export}
}

// unpromotedMarkers lists the markers of rootID on client, newest first.
func unpromotedMarkers(ctx context.Context, client replicationClient, rootID string) ([]qumulo.SnapshotInfo, error) {
	snaps, err := client.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	var out []qumulo.SnapshotInfo
	for i := range snaps {
		if isUnpromotedMarker(snaps[i], rootID) {
			out = append(out, snaps[i])
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return snapshotIDLess(out[j].ID, out[i].ID) })
	return out, nil
}

// markUnpromotedBeforeDisable takes the unpromoted marker of the peer's
// replica root before the relationship is deleted, when this side is the
// source and the peer's end is a live, read-only replica -- the evidence
// fenceReplicaExportBeforeDisable acts on, and the last moment it exists.
// Older markers of the root are removed: they predate the relationship
// being disabled now.
//
// The end is read again after the snapshot. A read-only target root takes no
// writes, and a promote is the only way out of read-only, so a root still
// read-only after the snapshot held exactly the replica's content when it
// was taken; a promote that lands before that read drops the marker. One
// that lands after it and before the delete is caught at Enable, as any
// write since is: the copy no longer matches.
//
// Never blocking, for the reason fenceReplicaExportBeforeDisable is not:
// Disable runs from the VolumeReplication's finalizer. Any failure is
// logged, and that pair's re-Enable is refused over a read-write export as
// before.
func (r *ReplicationServer) markUnpromotedBeforeDisable(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) {
	if e.export == nil || rel.role != roleSource || rel.peerRole != roleTarget || rel.peer == nil ||
		!rel.peer.TargetRootReadOnly || rel.peer.State == qumulo.RelationshipEnded {
		return
	}
	root, err := e.peer.LookupFileByPath(ctx, e.peerPath)
	if err != nil {
		slog.WarnContext(ctx, rpc+": could not find the replica root to mark it unpromoted before disabling; "+
			"a re-enable over a read-write replica export is refused",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "peerPath", e.peerPath, "error", err)
		return
	}
	if rel.peer.TargetRootID != "" && rel.peer.TargetRootID != root.ID {
		slog.WarnContext(ctx, rpc+": the target end's root is not the directory at the replica path; not marked",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "peerPath", e.peerPath,
			"pathID", root.ID, "targetRootID", rel.peer.TargetRootID)
		return
	}
	marker, err := e.peer.CreateExpiringSnapshot(ctx, root.ID, unpromotedMarkerName(rel.id), time.Now().Add(unpromotedMarkerTTL))
	if err != nil {
		slog.WarnContext(ctx, rpc+": could not snapshot the replica root to mark it unpromoted before disabling; "+
			"a re-enable over a read-write replica export is refused",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "peerPath", e.peerPath, "error", err)
		dropUnpromotedMarkers(ctx, rpc, e.peer, e.peerAddr, root.ID, "")
		return
	}
	end, err := e.peer.GetTargetStatus(ctx, rel.id)
	if err != nil || !end.TargetRootReadOnly || end.State == qumulo.RelationshipEnded {
		slog.WarnContext(ctx, rpc+": the target end could not be confirmed a read-only replica after the marker snapshot; marker dropped",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "marker", marker.ID,
			"state", end.State, "targetRootReadOnly", end.TargetRootReadOnly, "error", err)
		dropUnpromotedMarkers(ctx, rpc, e.peer, e.peerAddr, root.ID, "")
		return
	}
	dropUnpromotedMarkers(ctx, rpc, e.peer, e.peerAddr, root.ID, marker.ID)
	slog.InfoContext(ctx, rpc+": replica root marked unpromoted before disabling",
		"volume", e.vol.Name, "scope", e.scope.String(), "relationship", rel.id, "peer", e.peerAddr,
		"peerPath", e.peerPath, "marker", marker.ID, "expires", unpromotedMarkerTTL.String())
}

// dropUnpromotedMarkers removes the markers of rootID on client, but keep.
// Best effort: a marker left behind expires on its own, and one that no
// longer matches its root only refuses.
func dropUnpromotedMarkers(ctx context.Context, rpc string, client replicationClient, addr, rootID, keep string) {
	markers, err := unpromotedMarkers(ctx, client, rootID)
	if err != nil {
		slog.WarnContext(ctx, rpc+": could not list the unpromoted markers to remove; they expire on their own",
			"peer", addr, "root", rootID, "error", err)
		return
	}
	for _, m := range markers {
		if m.ID == keep {
			continue
		}
		if err := client.DeleteSnapshot(ctx, m.ID); err != nil && !qumulo.IsNotFound(err) {
			slog.WarnContext(ctx, rpc+": could not remove an unpromoted marker; it expires on its own",
				"peer", addr, "marker", m.ID, "error", err)
			continue
		}
		slog.InfoContext(ctx, rpc+": unpromoted marker removed", "peer", addr, "marker", m.ID)
	}
}

// dropUnpromotedMarkersAfterProtect removes the peer root's markers once
// Enable has created and staged the relationship: they described the copy
// before it, and replication is about to change it. Kept until then, so a
// retry after an Aborted still finds them (fenceReplicaExport). Best effort:
// one left behind expires, a later Disable replaces it, and a promote's
// snapshot check skips it (isReprotectSnapshot).
func dropUnpromotedMarkersAfterProtect(ctx context.Context, rpc string, e *replicationEnds) {
	if e.export == nil {
		return
	}
	rootID := ""
	if e.unpromoted != nil {
		rootID = e.unpromoted.rootID
	} else {
		root, err := e.peer.LookupFileByPath(ctx, e.peerPath)
		if err != nil {
			return
		}
		rootID = root.ID
	}
	dropUnpromotedMarkers(ctx, rpc, e.peer, e.peerAddr, rootID, "")
}

// unchangedSinceDisable compares the peer's replica root with the newest
// unpromoted marker of it. It answers the marker when the root is unchanged
// since, and otherwise why it cannot say so, as a clause for the refusal.
// The snapshot it takes to compare is removed whatever the answer.
func unchangedSinceDisable(ctx context.Context, rpc string, e *replicationEnds) (*unpromotedProof, string) {
	cannot := func(err error) (*unpromotedProof, string) {
		return nil, fmt.Sprintf("it could not be compared with a marker snapshot from the Disable that removed the relationship: %v", err)
	}
	root, err := e.peer.LookupFileByPath(ctx, e.peerPath)
	if err != nil {
		return cannot(err)
	}
	markers, err := unpromotedMarkers(ctx, e.peer, root.ID)
	if err != nil {
		return cannot(err)
	}
	if len(markers) == 0 {
		return nil, fmt.Sprintf("no marker snapshot (%s*) from the Disable that removed the relationship was found, "+
			"so nothing says it is unchanged since: that Disable was an earlier build's, its snapshot failed, or it "+
			"expired after %s", unpromotedMarkerTag, unpromotedMarkerTTL)
	}
	marker := markers[0]
	fresh, err := e.peer.CreateExpiringSnapshot(ctx, root.ID, unpromotedCheckTag+marker.ID, time.Now().Add(unpromotedCheckTTL))
	if err != nil {
		return cannot(err)
	}
	defer func() {
		if err := e.peer.DeleteSnapshot(ctx, fresh.ID); err != nil && !qumulo.IsNotFound(err) {
			slog.WarnContext(ctx, rpc+": could not remove the snapshot taken to compare with the unpromoted marker; it expires on its own",
				"volume", e.vol.Name, "peer", e.peerAddr, "snapshot", fresh.ID, "error", err)
		}
	}()
	same, err := e.peer.SnapshotDiffEmpty(ctx, fresh.ID, marker.ID)
	if err != nil {
		return cannot(err)
	}
	if !same {
		return nil, fmt.Sprintf("it has changed since the Disable that removed the relationship (marker snapshot %s, taken %s)",
			marker.ID, marker.CreatedAt.UTC().Format(time.RFC3339))
	}
	return &unpromotedProof{rootID: root.ID, marker: marker}, ""
}

// warnReprotectOverReadWriteExport is the one warning Enable gives before it
// re-protects a copy over a read-write export: what it is doing, and the
// writes that would be lost.
func warnReprotectOverReadWriteExport(ctx context.Context, rpc string, e *replicationEnds, proof *unpromotedProof) {
	owner := fmt.Sprintf("an operator's (description %q), which this driver does not change", proof.export.Description)
	if madeByThisDriver(&proof.export) {
		owner = "this driver's"
	}
	slog.WarnContext(ctx, rpc+": the DR copy is unchanged since Disable marked it, so it is re-protected as a replica over its "+
		"read-write export. Anything written to it through that export from now on is overwritten by replication "+
		"until the export is set read-only",
		"volume", e.vol.Name, "scope", e.scope.String(), "peer", e.peerAddr, "peerPath", e.peerPath,
		"export", proof.export.ExportPath, "exportID", proof.export.ID, "exportOwner", owner,
		"marker", proof.marker.ID, "markedAt", proof.marker.CreatedAt.UTC().Format(time.RFC3339))
}
