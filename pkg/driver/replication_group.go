// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// Group replication: one relationship for a whole shared-subdirectory class.
//
// # Why the group form exists at all
//
// A shared-subdirectory class has ONE covering NFS export at its basePath and
// provisions every volume as a directory under it. Lab testing measured both
// granularities at ten volumes and the shared class can only be protected one
// way: a single relationship rooted at basePath, one covering export mirrored
// to the peer, and one quota per member volume pre-staged there. Freeze to
// reprotected took 44.0 s for the group against 44.7 s for ten per-volume
// relationships, so this is not about speed -- it is about what a relationship
// can be rooted at when the volumes share an export.
//
// The consequence is the safety rule: a per-PVC VolumeReplication on a
// shared-subdirectory volume is refused. The only relationship it could act on
// is the class-level one, so one tenant's promote would freeze every other
// tenant's volume in the class and one tenant's delete would remove the
// relationship protecting them all (the two-tenant blast radius measured on
// the lab arrays).
//
// # What the vendored spec actually provides
//
// csi-addons spec v0.2.0 -- the version in go.mod, and the newest published --
// has NO VolumeGroupController service and no VolumeGroupReplication service.
// Its replication.proto declares exactly one service, Controller, with the six
// per-volume RPCs. What it does have is a ReplicationSource message carrying a
// oneof of VolumeSource{volume_id} and VolumeGroupSource{volume_group_id},
// present as an optional field on all six requests.
//
// So the group form is implemented behind those same six RPCs, entered either
// way round:
//
//   - replication_source.volumegroup.volume_group_id set on the request, which
//     is what a future group-aware controller would send; or
//   - replicationScope: group on the VolumeReplicationClass, which is what
//     works today with the stock csi-addons sidecar, since nothing in it
//     populates replication_source.
//
// GetVolumeReplicationInfoRequest carries no parameters map at all (a known
// narrowness of v0.2.0), so Info can only be put into group scope by the
// volume_group_id route. From a member volume's handle alone it reports on the
// member, finds no relationship, and says so.
//
// This is recorded in docs/reference/supported-features.md as "group form behind the
// per-volume RPCs", not as VolumeGroupReplication, because no such service
// exists to implement.
//
// # What a volume group id is here
//
// Two shapes are accepted, because nothing in csi-addons defines one and this
// driver has no VolumeGroup RPC to have issued one:
//
//   - an absolute path, the class's basePath on the array; or
//   - a handle of this driver's own shape, <server>/<fileID>/<name>, which
//     gets the same file-ID identity check every volume handle gets.
//
// With neither -- replicationScope: group and no group id -- the group root is
// derived from the array itself: the covering export of the member volume
// named in volume_id. That is exactly the measured shared model, it needs no
// Kubernetes read (Rule 4) and no configuration to drift, and it is the same
// trick the per-volume path already uses to tell the two provisioning models
// apart.

// replicationScope is whether an RPC is about one volume or a whole class.
type replicationScope int

const (
	// scopeVolume: one relationship rooted at the volume's own directory.
	scopeVolume replicationScope = iota
	// scopeGroup: one relationship rooted at the class's basePath, covering
	// every volume under it.
	scopeGroup
)

func (s replicationScope) String() string {
	if s == scopeGroup {
		return "group"
	}
	return "volume"
}

// groupRootFor decides which directory a group-scoped RPC is rooted at, and
// returns it along with the handle that names it.
//
// memberPath is the volume named in volume_id, already resolved and proved to
// still be that volume. It is used two ways: as the thing the group must
// contain (a group id naming a directory the volume does not live under is a
// misconfigured class, not a group), and as the way to find the covering
// export when no group id was given.
func groupRootFor(
	ctx context.Context,
	rpc string,
	local replicationClient,
	member volumeID,
	memberPath, groupID, localAddr string,
) (volumeID, resolvedVolume, error) {
	var groupPath string

	switch {
	case groupID == "":
		// Derive it: the class's covering export is the group.
		covering, err := findCoveringExport(ctx, local, memberPath)
		if err != nil {
			return volumeID{}, resolvedVolume{}, unreachableOr(localAddr, err, "%s: listing exports on %s", rpc, localAddr)
		}
		if covering == nil {
			return volumeID{}, resolvedVolume{}, status.Errorf(codes.FailedPrecondition,
				"%s: no NFS export on %s covers %q, so the shared-subdirectory class this volume belongs to "+
					"cannot be identified and there is nothing to root a group relationship at. Either create "+
					"the class's covering export, or name the group explicitly in the request",
				rpc, localAddr, memberPath)
		}
		groupPath = covering.FSPath

	case path.IsAbs(groupID):
		groupPath = path.Clean(groupID)

	default:
		group, err := parseVolumeID(groupID)
		if err != nil {
			return volumeID{}, resolvedVolume{}, status.Errorf(codes.InvalidArgument,
				"%s: volume group id %q is neither an absolute path on the array nor a handle of the form "+
					"<server>#<path>: %v", rpc, groupID, err)
		}
		if !strings.EqualFold(group.Server, member.Server) {
			return volumeID{}, resolvedVolume{}, status.Errorf(codes.InvalidArgument,
				"%s: volume group %q names array %q but volume %q lives on %q; a group and its members "+
					"are one directory tree on one array", rpc, groupID, group.Server, member.Name, member.Server)
		}
		// Same resolution a volume handle gets: a group root is a directory
		// and the handle names it by path.
		resolved, resolution, err := resolveVolumePath(ctx, local, group, rpc)
		if err != nil {
			return volumeID{}, resolvedVolume{}, unreachableOr(localAddr, err, "%s: resolving volume group %q on %s", rpc, groupID, localAddr)
		}
		if resolution != volumeFound {
			return volumeID{}, resolvedVolume{}, status.Errorf(codes.NotFound,
				"%s: %q does not name a volume group directory on array %s",
				rpc, group.RealPath, localAddr)
		}
		groupPath = resolved.RealPath
	}

	rest, under := relativeUnder(groupPath, memberPath)
	if !under {
		return volumeID{}, resolvedVolume{}, status.Errorf(codes.FailedPrecondition,
			"%s: volume %q lives at %q on %s, which is not under the volume group rooted at %q",
			rpc, member.Name, memberPath, localAddr, groupPath)
	}

	// The whole array is never a group. A relationship rooted at "/" would
	// freeze the entire source array at promote and revert the entire peer
	// array to a recovery point at resync, and the root export -- the mock's
	// default, and common on a real array -- is exactly what the derivation
	// above picks when the class has no export of its own (reproduced
	// on the fabric). Refused whichever way the root was named.
	if groupPath == "/" {
		return volumeID{}, resolvedVolume{}, status.Errorf(codes.FailedPrecondition,
			"%s: the volume group for %q on %s would be rooted at \"/\" (the covering export is the root export), "+
				"and a replication relationship on the whole array would freeze it at promote and revert it at "+
				"resync. Create the class's own covering export at its basePath, or name the group explicitly",
			rpc, member.Name, localAddr)
	}
	// Derived from an export, the group root must be the class's basePath,
	// and this driver lays volumes out at basePath/<name> or, with adDomain,
	// basePath/<domain>/<name>. An export more than two levels above the
	// member is an ancestor of the class's directory, not the class's own
	// export, and rooting the group there would replicate -- and freeze --
	// every sibling class under it. The class's basePath itself is not in
	// the request (Rule 4: no StorageClass read), so the layout is the only
	// ruler available; it never chooses an ancestor above basePath.
	if groupID == "" {
		if depth := strings.Count(rest, "/") + 1; depth > 2 {
			return volumeID{}, resolvedVolume{}, status.Errorf(codes.FailedPrecondition,
				"%s: the export covering %q on %s is at %q, %d levels above it; this driver lays volumes out at "+
					"basePath/<name> or basePath/<adDomain>/<name>, so that export is an ancestor above the class's "+
					"basePath rather than the class's own export, and a group rooted there would reach every "+
					"class under it. Create the class's covering export at its basePath, or name the group explicitly",
				rpc, memberPath, localAddr, groupPath, depth)
		}
	}

	// The group root must exist before anything is rooted at it, and this is
	// where its file ID comes from. The handle no longer carries one, but the
	// quota staging and the drift checks still need this array's ID for the
	// root, so it is returned rather than looked up again by the caller.
	info, err := local.LookupFileByPath(ctx, groupPath)
	if err != nil {
		if qumulo.IsNotFound(err) {
			return volumeID{}, resolvedVolume{}, status.Errorf(codes.NotFound,
				"%s: volume group root %q does not exist on %s", rpc, groupPath, localAddr)
		}
		return volumeID{}, resolvedVolume{}, unreachableOr(localAddr, err, "%s: looking up %q on %s", rpc, groupPath, localAddr)
	}

	return newVolumeID(member.Server, member.APIURL, groupPath),
		resolvedVolume{RealPath: groupPath, FileID: info.ID, Owner: info.Owner, Group: info.Group}, nil
}

// findCoveringExport returns the export whose filesystem path is fsPath or an
// ancestor of it, longest match first -- which is the export the NFS server
// itself resolves a mount to when exports nest.
//
// findExportByFSPath answers the *exact* question ("does this volume have an
// export of its own"), which is what distinguishes the two provisioning
// models. This answers the containing one, which is what identifies a
// shared-subdirectory class from one of its volumes.
//
// A report (withReportLookups) reads the covering export it found last by ID
// and believes it while it still covers fsPath; every group Info poll used to
// list the whole table for it. A deeper export added since is
// seen once the hint fails or by any caller that acts, which always lists.
func findCoveringExport(ctx context.Context, client qumulo.ClientInterface, fsPath string) (*qumulo.ExportInfo, error) {
	getter, hinted := client.(exportGetter)
	key := ""
	if hinted {
		key = exportHintKey(getter, fsPath)
		if hint, ok := coveringExportHints.Load(key); ok && reportLookups(ctx) {
			if export, err := getter.GetExport(ctx, hint.(idHint).id); err == nil {
				if _, covers := relativeUnder(export.FSPath, fsPath); covers {
					coveringExportHints.CompareAndSwap(key, hint, newIDHint(export.ID))
					return &export, nil
				}
			}
			coveringExportHints.Delete(key)
		}
	}

	exports, err := client.ListExports(ctx)
	if err != nil {
		return nil, err
	}
	var covering *qumulo.ExportInfo
	for i := range exports {
		if _, ok := relativeUnder(exports[i].FSPath, fsPath); !ok {
			continue
		}
		if covering != nil && len(exports[i].FSPath) <= len(covering.FSPath) {
			continue
		}
		covering = &exports[i]
	}
	if hinted && covering != nil {
		coveringExportHints.Store(key, newIDHint(covering.ID))
		notePackageHintStored()
	}
	return covering, nil
}

// coveringExportHints maps endpoint + a member's path to the export last
// found covering it (an idHint). Read only by reports.
var coveringExportHints sync.Map

// memberQuota is one volume inside a group: where it sits relative to the
// group root, and the limit its quota carries.
//
// A path rather than a name because a group is not always one level deep.
// See groupMembers.
type memberQuota struct {
	relPath string
	limit   uint64
}

// maxGroupMemberDepth is how far below the group root a volume may sit.
//
// Two, because two is what the provisioning models produce: base/name for an
// ordinary shared class, and base/domain/name when the class sets adDomain.
// A deeper walk is not more general, it is an unbounded recursion over a
// customer's tree performed on every Enable and every promote.
const maxGroupMemberDepth = 2

// groupMembers lists the volumes inside a group on one array, with their
// quotas.
//
// A directory with a quota is a member. A directory without one is not
// necessarily "not a member": it may be an intermediate the driver created
// on purpose, and the volumes are one level further down.
//
// That is exactly what adDomain does. ensureADDomainDirectory creates
// base/domain deliberately without a quota -- a quota there would cap every
// volume in the domain at once -- so a walk that skipped quota-less
// directories skipped the domain and never saw the volumes beneath it. Enable
// reported the group protected, and after promote those volumes had no quota
// on the replica at all, which makes a write past the limit unbounded rather
// than ENOSPC.
//
// So a quota-less directory is descended into rather than dismissed, to
// maxGroupMemberDepth. A directory with a quota is a member and is NOT
// descended into: a volume is a leaf as far as this is concerned, and its
// contents are the workload's.
func groupMembers(ctx context.Context, client qumulo.ClientInterface, root string) ([]memberQuota, error) {
	members, _, err := walkGroup(ctx, client, root, "", maxGroupMemberDepth, nil)
	return members, err
}

// walkGroup returns the members it found and the directories it could not
// account for: quota-less, with nothing quota'd beneath them within the depth
// budget. Those are either a gap in this driver's own staging or somebody
// else's directory, and the caller decides which matters.
//
// visit, when set, is called with every directory the walk lists, by its
// path relative to root, so a caller can act on what the walk read without
// reading it again.
func walkGroup(
	ctx context.Context,
	client qumulo.ClientInterface,
	root, relPath string,
	depth int,
	visit func(relPath string, entry qumulo.DirEntry),
) (members []memberQuota, unaccounted []string, err error) {
	if depth <= 0 {
		return nil, nil, nil
	}

	entries, err := client.ListDirectory(ctx, path.Join(root, relPath), "")
	if err != nil {
		if qumulo.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	for _, entry := range entries {
		if !entry.IsDirectory() {
			continue
		}
		childRel := path.Join(relPath, entry.Name)
		if visit != nil {
			visit(childRel, entry)
		}

		limit, err := client.GetQuota(ctx, entry.ID)
		if err == nil {
			members = append(members, memberQuota{relPath: childRel, limit: limit})
			continue
		}
		if !qumulo.IsNotFound(err) {
			return nil, nil, err
		}

		// No quota here. Either an intermediate the driver created (the
		// adDomain case) or a directory that is nothing to do with this
		// driver -- and the two are indistinguishable from here. Looking one
		// level further costs a listing and finds the first; finding nothing
		// with a quota under it settles the second.
		nested, nestedGaps, err := walkGroup(ctx, client, root, childRel, depth-1, visit)
		if err != nil {
			return nil, nil, err
		}
		if len(nested) == 0 && len(nestedGaps) == 0 {
			unaccounted = append(unaccounted, childRel)
			continue
		}
		members = append(members, nested...)
		unaccounted = append(unaccounted, nestedGaps...)
	}
	return members, unaccounted, nil
}

// prestageMemberQuotas mirrors every member volume's quota onto the replica
// side of a group relationship.
//
// Replication carries directory contents and nothing else: not quotas, not
// exports (measured). For a group that means N quotas plus the one
// covering export, staged while the target is still read-only, so an
// unplanned failover needs nothing from the dead array.
//
// A member whose replica directory does not exist yet is counted as pending.
// The group root exists from authorize onward but the directories under
// it appear as the first job copies them. Enable waits for them (see below);
// a member provisioned after Enable is staged by GetVolumeReplicationInfo,
// which csi-addons keeps calling (stageLateMemberQuotas).
func prestageMemberQuotas(ctx context.Context, rpc string, e *replicationEnds, id string) error {
	members, staged, pending, err := stageMemberQuotas(ctx, rpc, e)
	if err != nil {
		return err
	}

	slog.InfoContext(ctx, rpc+": member quotas staged on the replica side of the group",
		"group", e.realPath, "relationship", id, "peer", e.peerAddr,
		"members", len(members), "staged", staged, "notYetReplicated", pending)
	if pending > 0 {
		slog.WarnContext(ctx, rpc+": some group members have not replicated yet and carry no quota on the peer; "+
			"a promote before they do would leave them unbounded",
			"group", e.realPath, "relationship", id, "pending", pending)
	}
	if pending > 0 {
		// Measured on the lab cluster (2026-09-14): the csi-addons controller
		// does not call Enable again once a primary VolumeReplication reports
		// Completed, so "staged on the next reconcile" never came and a
		// promote in that window would have left the members unbounded.
		// Aborted makes the controller retry until every member has
		// replicated and carries its quota; only then is the group protected.
		// The counts are in the message because this wait is normal and can
		// run for minutes on a first sync, and csi-addons surfaces it as
		// FailedToPromote the whole time. Without them the condition is
		// indistinguishable from a broken relationship, and the only
		// difference an operator can act on is whether the number is moving.
		return status.Errorf(codes.Aborted,
			"%s: group %q is %d of %d volumes staged on %s (%d still replicating, none of them protected "+
				"by a quota there yet); this is the first sync completing, not a failure, and it is "+
				"retried until every member has arrived",
			rpc, e.realPath, staged, len(members), e.peerAddr, pending)
	}
	return nil
}

// stageMemberQuotas raises each group member's replica quota on the peer to
// the member's own limit, for every member whose replica directory exists
// there, and counts the ones that do not yet.
func stageMemberQuotas(ctx context.Context, rpc string, e *replicationEnds) (members []memberQuota, staged, pending int, err error) {
	members, err = groupMembers(ctx, e.local, e.realPath)
	if err != nil {
		return nil, 0, 0, unreachableOr(e.localAddr, err, "%s: listing the volumes in group %q on %s", rpc, e.realPath, e.localAddr)
	}
	for _, member := range members {
		replicaPath := path.Join(e.peerPath, member.relPath)
		replica, err := e.peer.LookupFileByPath(ctx, replicaPath)
		if err != nil {
			if qumulo.IsNotFound(err) {
				pending++
				continue
			}
			return nil, 0, 0, unreachableOr(e.peerAddr, err, "%s: looking up %q on peer %s", rpc, replicaPath, e.peerAddr)
		}
		if err := raiseQuota(ctx, e.peer, replica.ID, member.limit); err != nil {
			return nil, 0, 0, unreachableOr(e.peerAddr, err, "%s: staging the quota for %q on peer %s", rpc, replicaPath, e.peerAddr)
		}
		staged++
	}
	return members, staged, pending, nil
}

// stageLateMemberQuotas is prestageMemberQuotas for the members that arrive
// after Enable. csi-addons calls Enable once, so a PVC created on an
// already-protected group's class got its replica directory on the DR array
// but never a quota there -- measured: none 608 s after the PVC bound -- and
// an unplanned failover in that window promoted it unbounded. Info is the call
// the controller keeps making, so it stages them, best effort: a failure here
// is logged and the next Info tries again, because health reporting must not
// fail over a staging step.
func (r *ReplicationServer) stageLateMemberQuotas(ctx context.Context, rpc string, e *replicationEnds, id string) {
	// Walking every member and reading its quota on both arrays cost about
	// five REST calls per member on every poll, all repeats when nothing had
	// changed -- 267 calls for a 50-member group, per Info. One
	// listing of the group root says whether anything did: a member added,
	// removed or replaced, or one added under an adDomain directory (whose
	// child count changes). Unchanged, the staging is skipped, except once
	// every groupRestageEvery as a backstop.
	//
	// A member's own child count is the workload's, not membership: with it
	// in the fingerprint any file created or removed in any member re-staged
	// the whole group. The members the last staging found are left out of
	// the count; every other directory -- an adDomain directory above its
	// members, or one not yet known to be a member -- keeps it.
	key := groupStagingKey{array: e.localAddr, relationship: id}
	var last groupStaged
	if v, ok := r.groupStaging.Load(key); ok {
		last = v.(groupStaged)
	}
	entries, listErr := e.local.ListDirectory(ctx, e.realPath, "")
	if listErr == nil && last.fingerprint != "" && time.Since(last.at) < groupRestageEvery &&
		membershipFingerprint(entries, last.members) == last.fingerprint {
		return
	}
	members, _, pending, err := stageMemberQuotas(ctx, rpc, e)
	if err != nil {
		slog.WarnContext(ctx, rpc+": could not stage group members' quotas on the peer; the next Info tries again",
			"group", e.realPath, "relationship", id, "peer", e.peerAddr, "error", err)
		r.groupStaging.Delete(key)
		return
	}
	// A member that has not replicated yet could not be staged: remember
	// nothing, so the next poll tries it again as soon as it arrives.
	if pending > 0 {
		r.groupStaging.Delete(key)
		return
	}
	if listErr == nil {
		tops := topLevelMembers(members)
		r.groupStaging.Store(key, groupStaged{fingerprint: membershipFingerprint(entries, tops), members: tops, at: time.Now()})
		r.noteHintStored()
	}
}

// groupRestageEvery is how often an unchanged group is re-staged anyway.
const groupRestageEvery = 10 * time.Minute

type groupStagingKey struct{ array, relationship string }

type groupStaged struct {
	fingerprint string
	// members is topLevelMembers of the staging the fingerprint was taken
	// for. A string, not a set, so the entry stays comparable for the sweep.
	members string
	at      time.Time
}

// topLevelMembers names the members that sit directly in the group root --
// base/<name>, not base/<domain>/<name> -- as a "\x00"-joined sorted list.
func topLevelMembers(members []memberQuota) string {
	var names []string
	for _, m := range members {
		if !strings.Contains(m.relPath, "/") {
			names = append(names, m.relPath)
		}
	}
	sort.Strings(names)
	return strings.Join(names, "\x00")
}

// membershipFingerprint summarises a group root's directories: each one's
// name and file ID, in name order, and the entry count of each that is not
// one of members (topLevelMembers). The count is there for a directory that
// holds members -- adDomain's -- and changes when one is added under it; a
// member's own count changes with every file its workload creates.
func membershipFingerprint(entries []qumulo.DirEntry, members string) string {
	isMember := map[string]bool{}
	if members != "" {
		for _, name := range strings.Split(members, "\x00") {
			isMember[name] = true
		}
	}
	var parts []string
	for _, e := range entries {
		if !e.IsDirectory() {
			continue
		}
		count := "?"
		switch {
		case isMember[e.Name]:
			count = "member"
		case e.ChildCount != nil:
			count = strconv.Itoa(*e.ChildCount)
		}
		parts = append(parts, e.Name+"\x00"+e.ID+"\x00"+count)
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x01")
}

// verifyMemberQuotas is the promote-side half of prestageMemberQuotas: every
// member of a promoted group needs a quota here, because replication wrote
// past the replica's limits and a client write past a *missing* limit is
// unbounded rather than ENOSPC.
//
// Gaps are filled from the peer when the peer answers, and named in a Warn
// when it does not. This is the group form of what replumb does for a single
// volume's quota, and it never refuses: the copy is already writable by the
// time this runs, and failing the RPC would not make it less writable.
func verifyMemberQuotas(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) {
	here, unaccounted, err := walkGroup(ctx, e.local, e.realPath, "", maxGroupMemberDepth, nil)
	if err != nil {
		slog.WarnContext(ctx, rpc+": could not list the promoted group's members to check their quotas",
			"group", e.realPath, "relationship", rel.id, "error", err)
		return
	}
	haveLimit := make(map[string]uint64, len(here))
	for _, member := range here {
		haveLimit[member.relPath] = member.limit
	}

	if rel.peer == nil {
		// Nothing to compare against. Say so rather than guess: a limit
		// invented here would be a limit nobody asked for.
		slog.WarnContext(ctx, rpc+": the peer was not consulted, so member quotas on the promoted copy "+
			"could not be checked against the source's",
			"group", e.realPath, "relationship", rel.id,
			"membersWithQuota", len(here), "directoriesWithoutOne", unaccounted)
		return
	}

	want, _, err := walkGroup(ctx, e.peer, e.peerPath, "", maxGroupMemberDepth, nil)
	if err != nil {
		slog.WarnContext(ctx, rpc+": could not read the source's member quotas to compare against",
			"group", e.realPath, "relationship", rel.id, "error", err)
		return
	}

	var created, raised []string
	for _, member := range want {
		current, present := haveLimit[member.relPath]
		if present && current >= member.limit {
			continue
		}

		// Present but smaller is the expand case: ControllerExpandVolume
		// grew the source and could not find the group relationship to grow
		// the replica with it, so the promoted copy would run out of space
		// at the limit it was staged with. Checking presence alone -- which
		// is what this did -- steps straight past that.
		localPath := path.Join(e.realPath, member.relPath)
		info, err := e.local.LookupFileByPath(ctx, localPath)
		if err != nil {
			slog.WarnContext(ctx, rpc+": a member of the source group is not present on the promoted copy",
				"group", e.realPath, "member", member.relPath, "error", err)
			continue
		}
		if err := ensureQuota(ctx, e.local, info.ID, member.limit); err != nil {
			slog.WarnContext(ctx, rpc+": could not put the source's limit on a promoted member",
				"group", e.realPath, "member", member.relPath, "limitBytes", member.limit, "error", err)
			continue
		}
		if present {
			raised = append(raised, member.relPath)
		} else {
			created = append(created, member.relPath)
		}
	}

	if len(created) > 0 {
		slog.WarnContext(ctx, rpc+": members had no quota on the promoted copy; created one from the "+
			"source's (pre-stage drifted)",
			"group", e.realPath, "relationship", rel.id, "members", created)
	}
	if len(raised) > 0 {
		slog.WarnContext(ctx, rpc+": members carried a smaller quota than the source; raised to match "+
			"(an expand that did not reach the replica)",
			"group", e.realPath, "relationship", rel.id, "members", raised)
	}
	if len(unaccounted) > 0 {
		slog.InfoContext(ctx, rpc+": directories under the promoted group with no quota of their own",
			"group", e.realPath, "relationship", rel.id, "directories", unaccounted)
	}
}

// refuseSharedSubdirectory is the per-PVC refusal. A volume
// with no export of its own was provisioned under a shared-subdirectory class, so the only relationship that could exist for it
// is the class-level one -- and acting on that from one PVC reaches every
// other tenant in the class.
//
// The detection is the same one the first pass used and needs no StorageClass:
// the request carries none, and Rule 4 forbids reading the PV.
//
// It is sound in EnableVolumeReplication and nowhere else, which is why only
// Enable calls it. "No export here" means "provisioned under a shared class"
// only when the local array is the intended *source*; on a replica the export
// is the pre-staged one, and a promote is exactly the RPC that has to cope
// with it being absent (replumb). Refusing there would turn a drifted
// pre-stage into an unrecoverable failover. The other five RPCs need no
// equivalent: a per-PVC call on a genuinely shared volume finds no
// relationship rooted at the volume's directory and is refused for that.
func refuseSharedSubdirectory(rpc string, e *replicationEnds) error {
	if e.scope == scopeGroup || e.export != nil {
		return nil
	}
	return status.Errorf(codes.FailedPrecondition,
		"%s: volume %q has no export of its own on %s, so it was provisioned under a shared-subdirectory "+
			"class. The only relationship possible for it is the class-level one rooted at the covering "+
			"export's path, and driving that from a single PVC would replicate -- and at promote freeze -- "+
			"every other volume in the class. Use the group form instead: set %s: %q on the "+
			"VolumeReplicationClass, or send a volume group id; or provision the volume from a "+
			"dedicated-export StorageClass",
		rpc, e.vol.Name, e.localAddr, paramReplicationScope, replicationScopeGroup)
}
