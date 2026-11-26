// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"log/slog"
	"maps"
	"path"
	"slices"
	"strings"

	"qumulo-csi-driver/pkg/qumulo"
)

// The NFS owner and group the driver gives every directory it creates.
//
// A directory created over REST is owned by the user the API token
// authenticates as, and its group is that user's: a LOCAL_USER and a
// LOCAL_GROUP. Replication refuses any entry owned by a local user or group
// that has no NFS ID ("... cannot be replicated because it belongs to local
// user csi-driver"), and the chart README recommends a dedicated local token
// user -- so following it gave volumes that never replicated. Enabling
// map_local_ids_to_nfs_ids on the relationship did not help (measured, Core
// 7.9.3 to 7.10.0); re-owning the directory to an NFS identity did.
//
// uid and gid 0 are what an admin token gave pods: the built-in admin maps
// to uid 0, and its directories replicate. Access comes from the mode the
// class sets (mountPermissions, 0777 by default), not from the owner.
const (
	volumeOwnerUID = "0"
	volumeOwnerGID = "0"
)

// nfsOwnership is the attribute update that gives a directory the driver's
// NFS owner and group.
func nfsOwnership() qumulo.FileAttributes {
	return qumulo.FileAttributes{
		OwnerDetails: &qumulo.Identity{IDType: qumulo.IDTypeNFSUID, IDValue: volumeOwnerUID},
		GroupDetails: &qumulo.Identity{IDType: qumulo.IDTypeNFSGID, IDValue: volumeOwnerGID},
	}
}

// localReownership is the part of nfsOwnership a directory that already
// exists needs: only a local owner or group is replaced. An NFS, SMB or AD
// identity is someone's choice -- an operator's chown, or a pod's over NFS
// -- and replication carries it, so it is left alone. An identity the
// array did not report is left alone too: there is nothing to say it is
// local.
func localReownership(owner, group qumulo.Identity) qumulo.FileAttributes {
	want := nfsOwnership()
	var out qumulo.FileAttributes
	if owner.IsLocal() {
		out.OwnerDetails = want.OwnerDetails
	}
	if group.IsLocal() {
		out.GroupDetails = want.GroupDetails
	}
	return out
}

// localOwnershipRemedy is appended to a replication job error that names a
// local owner, so the report says what to do about it.
const localOwnershipRemedy = " (the driver gives the volume directories it creates an NFS owner, " +
	"and re-owns a local-owned volume root when replication is enabled; an entry a local user " +
	"created inside the volume through the API or SMB must be re-owned to an NFS identity, or " +
	"that user given an NFS UID and the relationship's map_local_ids_to_nfs_ids enabled)"

// withLocalOwnershipRemedy returns a replication job error with the remedy
// for a local owner appended when that is what it reports.
func withLocalOwnershipRemedy(jobError string) string {
	if strings.Contains(jobError, "belongs to local user") || strings.Contains(jobError, "belongs to local group") {
		return jobError + localOwnershipRemedy
	}
	return jobError
}

// reownLocalRoot gives the replicated root a local owner's NFS replacement
// before replication is relied on: a volume an older build created is
// owned by the token's local user, and every job of a relationship over it
// fails. Only the local owner or group is replaced -- an NFS, SMB or AD one
// is left as it is.
//
// In group scope the root is the class's basePath, which the driver did not
// create. It is re-owned on the same terms: an operator who made it over NFS
// owns it through NFS and keeps it, but one made through the API or the UI by
// a local user stops the whole group replicating, and replacing that local
// owner is the only way the group can.
//
// An array that does not answer fails the call; one that refuses the update
// is logged and the relationship goes ahead, its job error then naming the
// directory and the remedy.
func (r *ReplicationServer) reownLocalRoot(ctx context.Context, rpc string, e *replicationEnds) error {
	attrs := localReownership(e.rootOwner, e.rootGroup)
	if attrs.IsEmpty() {
		return nil
	}
	if err := r.reown(ctx, rpc, e, e.fileID, e.realPath, e.rootOwner, e.rootGroup, attrs); err != nil {
		return err
	}
	if attrs.OwnerDetails != nil {
		e.rootOwner = *attrs.OwnerDetails
	}
	if attrs.GroupDetails != nil {
		e.rootGroup = *attrs.GroupDetails
	}
	return nil
}

// reownLocalRoots is reownLocalRoot, and in group scope also the member
// volumes and the adDomain directories that hold them: the directories the
// driver created inside the group. It runs when the relationship is created,
// on a walk like the one the quota staging makes -- a listing of the root, a
// quota read per directory, and a listing of each quota-less one. Anything
// else in the tree -- a member's own contents, written over NFS and so
// NFS-owned already, or a directory the driver did not make -- is left to
// the job error and the operator.
func (r *ReplicationServer) reownLocalRoots(ctx context.Context, rpc string, e *replicationEnds) error {
	if err := r.reownLocalRoot(ctx, rpc, e); err != nil {
		return err
	}
	if e.scope != scopeGroup {
		return nil
	}

	visited := map[string]qumulo.DirEntry{}
	members, _, err := walkGroup(ctx, e.local, e.realPath, "", maxGroupMemberDepth,
		func(relPath string, entry qumulo.DirEntry) { visited[relPath] = entry })
	if err != nil {
		return unreachableOr(e.localAddr, err, "%s: listing the volumes in group %q on %s", rpc, e.realPath, e.localAddr)
	}

	// A member, and the domain directory above it when there is one.
	ours := map[string]bool{}
	for _, m := range members {
		ours[m.relPath] = true
		if parent := path.Dir(m.relPath); parent != "." {
			ours[parent] = true
		}
	}
	for _, relPath := range slices.Sorted(maps.Keys(ours)) {
		entry := visited[relPath]
		attrs := localReownership(entry.OwnerDetails, entry.GroupDetails)
		if attrs.IsEmpty() {
			continue
		}
		if err := r.reown(ctx, rpc, e, entry.ID, path.Join(e.realPath, relPath),
			entry.OwnerDetails, entry.GroupDetails, attrs); err != nil {
			return err
		}
	}
	return nil
}

// reown applies one re-own and logs it.
func (r *ReplicationServer) reown(
	ctx context.Context,
	rpc string,
	e *replicationEnds,
	fileID, dirPath string,
	owner, group qumulo.Identity,
	attrs qumulo.FileAttributes,
) error {
	err := e.local.SetFileAttributesByID(ctx, fileID, attrs)
	switch {
	case err == nil:
		slog.InfoContext(ctx, rpc+": re-owned a local-owned directory to the NFS owner so it can replicate",
			"volume", e.vol.Name, "path", dirPath, "owner", owner.IDType+":"+owner.IDValue,
			"group", group.IDType+":"+group.IDValue, "nfsUID", volumeOwnerUID, "nfsGID", volumeOwnerGID)
		return nil
	case isUnreachable(err) || qumulo.IsMalformedReply(err):
		// Not answered, or answered with a reply that cannot say whether the
		// owner changed: the RPC fails and is retried, as before.
		return unreachableOr(e.localAddr, err, "%s: re-owning %q on %s", rpc, dirPath, e.localAddr)
	default:
		slog.WarnContext(ctx, rpc+": could not re-own a local-owned directory; replication will refuse it",
			"volume", e.vol.Name, "path", dirPath, "owner", owner.IDType+":"+owner.IDValue,
			"group", group.IDType+":"+group.IDValue, "error", err)
		return nil
	}
}
