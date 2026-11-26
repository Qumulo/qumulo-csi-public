// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"log/slog"

	"qumulo-csi-driver/pkg/qumulo"
)

// The replica-export fence.
//
// A replica's export is read-only until a promote, and three places set a
// read-write one back: Enable's prestage, Disable before it removes the
// relationship, and a source-side status poll. Each runs on the
// source side while the DR side may be promoting, and a promote makes the
// target writable and then sets this same export read-write (replumb). A fence
// landing after that replumb takes the live copy's writes away -- pods get
// EROFS and the node refuses every read-write publish -- and nothing undoes
// it, because csi-addons does not call Promote again on a VolumeReplication
// already Primary (the status poll decided from a target status read
// several round trips earlier).
//
// So every fence goes through fenceIfStillReplica, which reads the target end
// immediately before the write and again after it. The read after closes the
// window rather than narrowing it: a promote makes the target writable before
// it touches the export, so a fence written after the replumb is always
// followed by a read that finds the end writable, and the fence is undone. A
// fence written before the replumb is overwritten by it.

// replicaFence is how fenceIfStillReplica ended.
type replicaFence int

const (
	// fenceKept: the export was set read-only, and the target end still read
	// as a read-only replica afterwards, or could not be read again (logged).
	fenceKept replicaFence = iota
	// fenceUnread: the target end could not be read before the write (a 404
	// included); nothing was written, and the error is returned.
	fenceUnread
	// fenceNotReplica: the target end is writable or ENDED; nothing was
	// written. The end is returned for the caller's log.
	fenceNotReplica
	// fenceDeclined: the caller's mayWrite declined; nothing was written, and
	// its error, if any, is returned.
	fenceDeclined
	// fenceWriteFailed: setting the export read-only failed; the error is
	// returned.
	fenceWriteFailed
	// fenceUndone: a promote landed between the read and the write, and the
	// export was set read-write again (logged).
	fenceUndone
	// fenceUndoFailed: as fenceUndone, but setting the export read-write
	// again failed. The export is read-only over a live copy (logged).
	fenceUndoFailed
)

// fenceIfStillReplica sets peerExport read-only when relationship id's target
// end on the peer reads, just before the write, as a read-only replica that
// is not ENDED; then reads the end again and, if a promote landed meanwhile,
// sets the export read-write again.
//
// mayWrite, when set, runs after the end is confirmed a replica and before
// the write, for a caller whose own checks need that (Enable's operator-export
// handling); false declines the write, with the error it returns, if any.
// Callers log the outcomes they have their own semantics for; the two that
// follow a write that turned out wrong are logged here, at WARN and ERROR.
func fenceIfStillReplica(
	ctx context.Context, rpc string, e *replicationEnds, id string, peerExport *qumulo.ExportInfo,
	mayWrite func(end qumulo.RelationshipStatus) (bool, error),
) (replicaFence, qumulo.RelationshipStatus, error) {
	end, err := e.peer.GetTargetStatus(ctx, id)
	switch {
	case err != nil:
		return fenceUnread, end, err
	case !isReadOnlyReplica(end):
		return fenceNotReplica, end, nil
	}
	if mayWrite != nil {
		if ok, err := mayWrite(end); !ok {
			return fenceDeclined, end, err
		}
	}
	if err := e.peer.SetExportReadOnly(ctx, peerExport.ID, true); err != nil {
		return fenceWriteFailed, end, err
	}

	// The write is done; what follows corrects it, so a caller going away
	// must not cut it short.
	vctx, cancel := detachedCleanup(ctx)
	defer cancel()
	after, err := e.peer.GetTargetStatus(vctx, id)
	if err != nil {
		slog.WarnContext(ctx, rpc+": replica export set read-only, and its target end could not be read again to confirm "+
			"no promote landed meanwhile; if the DR side was promoted just now, check that export is read-write",
			"volume", e.vol.Name, "relationship", id, "peer", e.peerAddr, "export", peerExport.ExportPath,
			"exportID", peerExport.ID, "error", err)
		return fenceKept, end, nil
	}
	if isReadOnlyReplica(after) {
		return fenceKept, end, nil
	}
	if err := e.peer.SetExportReadOnly(vctx, peerExport.ID, false); err != nil {
		slog.ErrorContext(ctx, rpc+": a promote landed while the replica export was being set read-only, and it could not "+
			"be set read-write again: the promoted copy cannot be written through it. Set export "+peerExport.ExportPath+
			" read-write on "+e.peerAddr+"; do not toggle the VolumeReplication through secondary with autoResync, "+
			"which discards every write since the failover",
			"volume", e.vol.Name, "relationship", id, "peer", e.peerAddr, "export", peerExport.ExportPath,
			"exportID", peerExport.ID, "state", after.State, "targetRootReadOnly", after.TargetRootReadOnly, "error", err)
		return fenceUndoFailed, end, nil
	}
	slog.WarnContext(ctx, rpc+": a promote landed while the replica export was being set read-only; set it read-write again",
		"volume", e.vol.Name, "relationship", id, "peer", e.peerAddr, "export", peerExport.ExportPath,
		"exportID", peerExport.ID, "state", after.State, "targetRootReadOnly", after.TargetRootReadOnly)
	return fenceUndone, end, nil
}

// isReadOnlyReplica reports whether a target end is a live, read-only
// replica: the one copy a replica export may be fenced over.
func isReadOnlyReplica(end qumulo.RelationshipStatus) bool {
	return end.TargetRootReadOnly && end.State != qumulo.RelationshipEnded
}
