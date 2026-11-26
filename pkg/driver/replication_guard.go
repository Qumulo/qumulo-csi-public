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
	"path/filepath"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// Replication awareness for the two ordinary CSI controller RPCs that can
// destroy or diverge a protected volume.
//
// Both are measured failures on Core 7.9.2.2, not hypotheticals:
//
//   - DeleteVolume tree-deletes a replication source root and Core allows it.
//     The relationship goes ENDED on both arrays with an empty
//     source_root_path, and the replica directory, its export and its quota
//     are left orphaned on the peer. The
//     PVC's protection is destroyed and nothing in Kubernetes says so.
//   - ControllerExpandVolume grows only the source quota. Replication ignores
//     the target quota, so the replica keeps filling past its limit and the
//     first client write after a promote fails ENOSPC. It also acts on the
//     handle's file ID without checking the path still names the volume, and
// replication
//     does not preserve file IDs, so after a failover a stale ID is usually a
//     valid ID for some other directory.
//
// # Where the configuration comes from, and what that costs
//
// DeleteVolumeRequest carries volume_id and secrets. ControllerExpandVolume
// carries volume_id, capacity_range, volume_capability and secrets. Neither
// message has a parameters map in the CSI spec, so neither RPC can see a
// StorageClass, and no csi-addons request is involved at all, so neither can
// see a VolumeReplicationClass either. clientFor already lives with this: it
// reconstructs the endpoint from the handle's server and takes the token from
// secrets[token].
//
// So these two RPCs read their replication configuration from the same Secret
// that already carries their token, under the same key names the
// VolumeReplicationClass uses (replication_params.go). Operators set
// csi.storage.k8s.io/provisioner-secret-name and
// csi.storage.k8s.io/controller-expand-secret-name on the StorageClass today;
// the replication keys go in that Secret beside the token.
//
// Two honest limitations follow, both documented in docs/reference/supported-features.md:
//
//  1. A key in the Secret that is *not* one of the recognized replication
//     parameters is ignored rather than rejected. parseReplicationParams
//     rejects unknown class parameters precisely so a misspelling cannot
//     silently change behaviour, but a Secret legitimately holds keys this
//     code knows nothing about -- token, and one token per array address --
//     so the same strictness cannot apply. A misspelled
//     replicationDeletePolicy therefore falls back to the safe default
//     (refuse) rather than erroring.
//  2. The peer array is identified from the relationship the local array
//     already reports (its target_address), not from configuration, whenever
//     the Secret names no targetServer. That is the only join key available
//     without a class, and it is exactly what the runbook's support bundle
//     uses.
//
// Rule 0 holds throughout: every client here is built from this request's own
// secrets and the volume's own handle, nothing is cached, and no Kubernetes
// object is read (Rule 4).

// protectedRelationship is a relationship rooted exactly at one volume's
// directory, as the array that volume lives on reports it.
type protectedRelationship struct {
	id     string
	role   relRole
	status qumulo.RelationshipStatus
	// memberRel is empty when the relationship is rooted at the volume
	// itself, and otherwise the volume's path relative to the relationship's
	// root -- a group relationship, which covers the whole class and names
	// each volume only as a subdirectory of its root.
	//
	// Callers must opt in. A guard that refuses a delete because the volume
	// IS protected wants the exact match; an expand that has to grow the
	// replica wants both, because a member of a group is just as protected
	// and its replica just as much in need of the new limit.
	memberRel string
}

// relPathUnder returns child's path relative to root when root is an ancestor
// of it, and false otherwise. Both are cluster paths; the array renders a
// relationship root with a trailing slash and a volume handle without one, so
// neither side can be compared raw.
func relPathUnder(root, child string) (string, bool) {
	root, child = path.Clean(root), path.Clean(child)
	if root == child || root == "/" {
		return "", false
	}
	rel, err := filepath.Rel(root, child)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return "", false
	}
	return rel, true
}

// errNoPeerCredential says the request's Secret carries no token for the peer
// array, so the peer cannot be contacted at all. It is a configuration fault,
// not an unreachable array, and the two must not be confused: one is fixed by
// editing a Secret, the other by fixing the network.
var errNoPeerCredential = errors.New("no token for the peer array in this request's secret")

// replicationParamsFromSecrets picks the replication class parameters out of
// a request's secrets map.
//
// Only recognized keys are taken, and they are then validated by the same
// parser a VolumeReplicationClass goes through, so the vocabulary is one
// vocabulary. See the file comment for why a Secret cannot be validated as
// strictly as a class.
func replicationParamsFromSecrets(secrets map[string]string) (replicationParams, error) {
	picked := make(map[string]string, len(secrets))
	for key, value := range secrets {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if slices.Contains(recognizedReplicationParams, normalized) {
			picked[normalized] = value
		}
	}
	return parseReplicationParams(picked)
}

// replicationViewOf asserts that a per-request client can also read the
// relationship table.
//
// *qumulo.Client satisfies ReplicationClient by construction -- replication.go
// asserts it at compile time -- so this cannot fail for a client built by the
// driver's default NewClient. It can only fail for a test double, and a
// silently skipped guard is how an unguarded replicated delete once shipped, so it
// is an error rather than a shrug.
func replicationViewOf(client qumulo.ClientInterface, rpc, server string) (qumulo.ReplicationClient, error) {
	view, ok := client.(qumulo.ReplicationClient)
	if !ok {
		return nil, status.Errorf(codes.Internal,
			"%s: the client built for cluster %q cannot read the replication relationship table, "+
				"so whether this volume is protected cannot be established", rpc, server)
	}
	return view, nil
}

// relationshipsAt returns every live relationship on one array, in either
// role, that is rooted at realPath or whose root contains it.
//
// A relationship rooted exactly at realPath has an empty memberRel. One whose
// root is an ancestor -- a group's root, with realPath a member directory
// under it -- carries the member's path relative to that root, so each caller
// decides what an ancestor match means for it: DeleteVolume refuses a member
// under a target root (it is part of the replica) but deletes one under a
// source root normally (its deletion simply replicates).
//
// ENDED relationships are skipped. An ENDED relationship protects nothing --
// it is the orphan an out-of-band delete leaves -- and Core empties its
// source_root_path, so refusing a delete on account of one would strand the
// PV with no remedy but a manual cleanup on the array.
func relationshipsAt(
	ctx context.Context,
	view qumulo.ReplicationClient,
	rpc, server, realPath string,
) ([]protectedRelationship, error) {
	at, _, err := relationshipsAtOrBelow(ctx, view, rpc, server, realPath)
	return at, err
}

// relationshipsAtOrBelow is relationshipsAt plus, from the same two table
// reads, every live relationship rooted strictly below realPath, in either
// role.
//
// DeleteVolume is the one caller that needs the second list. A tree delete
// takes everything beneath the directory, so a relationship rooted below it
// -- a replica, or a protected source -- is destroyed as surely as one rooted
// at it, and is left ENDED on both arrays. The other callers act on
// the directory itself and keep relationshipsAt.
func relationshipsAtOrBelow(
	ctx context.Context,
	view qumulo.ReplicationClient,
	rpc, server, realPath string,
) (at, below []protectedRelationship, err error) {
	sources, err := view.ListSourceStatus(ctx)
	if err != nil {
		return nil, nil, replicationListError(server, err, "%s: listing source relationships on cluster %q", rpc, server)
	}
	targets, err := view.ListTargetStatus(ctx)
	if err != nil {
		return nil, nil, replicationListError(server, err, "%s: listing target relationships on cluster %q", rpc, server)
	}

	classify := func(root string, rel protectedRelationship) {
		switch {
		case path.Clean(root) == path.Clean(realPath):
			at = append(at, rel)
		case relPathUnderOK(root, realPath, &rel.memberRel):
			at = append(at, rel)
		default:
			if _, ok := relPathUnder(realPath, root); ok {
				below = append(below, rel)
			}
		}
	}
	for _, s := range sources {
		if s.State == qumulo.RelationshipEnded {
			continue
		}
		classify(s.SourceRootPath, protectedRelationship{id: s.ID, role: roleSource, status: s})
	}
	for _, t := range targets {
		if t.State == qumulo.RelationshipEnded {
			continue
		}
		classify(t.TargetRootPath, protectedRelationship{id: t.ID, role: roleTarget, status: t})
	}
	return at, below, nil
}

// relPathUnderOK is relPathUnder(root, child) that stores the relative path
// in *rel when child lies under root.
func relPathUnderOK(root, child string, rel *string) bool {
	r, ok := relPathUnder(root, child)
	if ok {
		*rel = r
	}
	return ok
}

// replicationListError classifies a failure to read the relationship table.
//
// A 403 is singled out because it has exactly one remedy and it is not a
// driver fault: the token in the request's Secret was minted without
// PRIVILEGE_REPLICATION_SOURCE_READ. Reported as FailedPrecondition with the
// privilege named, rather than as an Internal an operator would file a bug
// about.
func replicationListError(server string, err error, format string, args ...any) error {
	var apiErr *qumulo.APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusForbidden {
		return status.Errorf(codes.FailedPrecondition,
			"%s: the token for cluster %q may not read the replication relationship table, so whether "+
				"this volume is protected cannot be established. Grant the token "+
				"PRIVILEGE_REPLICATION_SOURCE_READ and PRIVILEGE_REPLICATION_TARGET_READ: %v",
			fmt.Sprintf(format, args...), server, err)
	}
	return unreachableOr(server, err, format, args...)
}

// guardReplicatedDelete is DeleteVolume's replication check.
//
// It runs after the handle has been proved to still name this volume and
// before anything is dismantled -- including the snapshot policy, which a
// relationship may have linked -- because every branch below either refuses
// the delete outright or has to remove the relationship first.
//
// The default is to refuse. Deleting a PVC is a Kubernetes-side action whose
// blast radius on a protected volume reaches an array on the other side of
// the DR link, and the CSI request carries no way for the operator to say
// they meant it; refusing names the relationship and the remedy instead, and
// the PV stays Released until the operator acts. replicationDeletePolicy:
// disable in the delete Secret is that consent, recorded once by whoever
// configured the class.
func (s *ControllerServer) guardReplicatedDelete(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeID,
	realPath string,
	secrets map[string]string,
) error {
	const rpc = "deletevolume"

	params, err := replicationParamsFromSecrets(secrets)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%s: %v", rpc, err)
	}

	view, err := replicationViewOf(client, rpc, parsed.Server)
	if err != nil {
		return err
	}
	rels, below, err := relationshipsAtOrBelow(ctx, view, rpc, parsed.Server, realPath)
	if err != nil {
		return err
	}
	// Nor is anything that holds one. A relationship rooted below the
	// directory -- a replica, or another volume's protected source -- goes
	// with the tree delete, whatever the policy says: the policy is consent
	// to disable this volume's own relationship, not every one beneath it.
	if len(below) > 0 {
		rel := below[0]
		root, what := rel.status.SourceRootPath, "the protected source"
		if rel.role == roleTarget {
			root, what = rel.status.TargetRootPath, "the replica"
		}
		slog.WarnContext(ctx, rpc+": refusing to delete a directory that holds a replicated directory",
			"volume", parsed.Name, "path", realPath, "relationship", rel.id, "root", root, "role", rel.role)
		return status.Errorf(codes.FailedPrecondition,
			"%s: %q on cluster %q holds %q, %s of replication relationship %s (%s); deleting it would "+
				"destroy that directory and leave the relationship ENDED on both arrays. Disable that "+
				"relationship first; nothing was deleted",
			rpc, realPath, parsed.Server, root, what, rel.id, rel.status.State)
	}
	// A replica is never deleted, whatever the policy says. Its directory is
	// the live target of a relationship the other array drives: tree-deleting
	// it ends that relationship from the wrong side and destroys the copy the
	// source is still writing into. Whoever wants it gone disables
	// replication first, from the side that owns it.
	//
	// That includes a directory beneath a group's target root: a
	// member's DR copy is part of the replica, and its mirror PV is the
	// handle a DR-side delete arrives with.
	for _, rel := range rels {
		if rel.role != roleTarget {
			continue
		}
		slog.WarnContext(ctx, rpc+": refusing to delete a replication target",
			"volume", parsed.Name, "relationship", rel.id, "path", realPath, "member", rel.memberRel,
			"state", rel.status.State)
		what := "is the replica"
		if rel.memberRel != "" {
			what = "is inside the replica"
		}
		return status.Errorf(codes.FailedPrecondition,
			"%s: %q on cluster %q %s of replication relationship %s (%s) and holds the DR "+
				"copy of another cluster's volume; deleting it would destroy that copy and leave the "+
				"relationship ENDED on both arrays. Disable replication from the source side first",
			rpc, realPath, parsed.Server, what, rel.id, rel.status.State)
	}

	// Past the replica check, only a relationship rooted at this volume. A
	// member under a group's source root IS protected, but deleting it is an
	// ordinary delete whose effect replicates, and the group's refusals are
	// the replication service's to make (refuseSharedSubdirectory); refusing
	// here on an ancestor match would start refusing deletes this guard has
	// always allowed.
	// A stale source is never deleted either, whatever the policy says: its
	// directory is the source root of a relationship whose live copy is the
	// peer's promoted end -- or a member under such a root -- and a delete
	// here takes a copy that is behind the live one and, at the root, ends
	// the relationship from the side that is not live (the
	// unplanned-failover shape). No delete policy overrides this; the
	// operator demotes the stale side, or disables replication from the live
	// side, first.
	for _, rel := range rels {
		if err := s.refuseAStaleSource(ctx, rpc, parsed, realPath, rel, params, secrets, false); err != nil {
			return err
		}
	}

	rels = slices.DeleteFunc(rels, func(r protectedRelationship) bool { return r.memberRel != "" })
	if len(rels) == 0 {
		return nil
	}

	if params.DeletePolicy == replicationDeletePolicyRefuse {
		ids := make([]string, 0, len(rels))
		for _, rel := range rels {
			ids = append(ids, rel.id+" ("+rel.status.State+")")
		}
		slog.WarnContext(ctx, rpc+": refusing to delete a protected volume",
			"volume", parsed.Name, "path", realPath, "relationships", ids, "policy", params.DeletePolicy)
		return status.Errorf(codes.FailedPrecondition,
			"%s: volume %q on cluster %q is the source of replication relationship %s; deleting it would "+
				"leave the relationship ENDED on both arrays and orphan the replica on the peer. Disable "+
				"replication first -- delete the VolumeReplication object, or call "+
				"DisableVolumeReplication -- or set %s: %q in the delete Secret to have this driver do it",
			rpc, parsed.Name, parsed.Server, strings.Join(ids, ", "),
			paramReplicationDeletePolicy, replicationDeletePolicyDisable)
	}

	for _, rel := range rels {
		if err := s.disableBeforeDelete(ctx, client, view, parsed, rel, params, secrets); err != nil {
			return err
		}
	}
	return nil
}

// disableBeforeDelete removes one relationship on both arrays: the target
// end on the peer first, then the local source end.
//
// Peer end first, because that is the order in which a refusal costs
// nothing. The first cut deleted the local end first and unconditionally,
// then shrugged when the peer could not be reached; by then the source end
// was gone, a retry found nothing to disable, and the delete went ahead
// leaving the peer an orphaned ENDED end plus a replica with an export and a
// quota that its inventory then listed as a volume. With the
// peer end removed first, "the peer cannot be reached" is discovered before
// anything on either array has changed, and the delete is refused with the
// remedy. Either order stops replication out of the directory before the
// tree-delete: deleting the target end leaves the source end ENDED with no
// job to run (measured), and deleting an ENDED source end is what
// Enable's orphan clean-up already does.
//
// The operator can consent to the orphan instead: replicationDeletePolicy
// disable-orphan removes the local end and deletes the volume whatever the
// peer says, and the log names what was left where.
func (s *ControllerServer) disableBeforeDelete(
	ctx context.Context,
	client qumulo.ClientInterface,
	view qumulo.ReplicationClient,
	parsed volumeID,
	rel protectedRelationship,
	params replicationParams,
	secrets map[string]string,
) error {
	const rpc = "deletevolume"
	orphanAllowed := params.DeletePolicy == replicationDeletePolicyDisableOrphan

	peerRemoved, peerAddr, peerErr := s.removePeerTargetEnd(ctx, parsed, rel, params, secrets)
	if !peerRemoved {
		if !orphanAllowed {
			return status.Errorf(codes.FailedPrecondition,
				"%s: volume %q on cluster %q is the source of replication relationship %s and its target end on "+
					"peer %q could not be removed (%v). Nothing was changed. Deleting now would leave the peer an "+
					"orphaned ENDED end and a replica its inventory lists as a volume: fix the peer's reachability "+
					"or its token in the delete Secret and retry, or set %s: %q in the delete Secret to accept "+
					"the orphan",
				rpc, parsed.Name, parsed.Server, rel.id, peerAddr, peerErr,
				paramReplicationDeletePolicy, replicationDeletePolicyDisableOrphan)
		}
		slog.WarnContext(ctx, rpc+": the peer's target end could not be removed and the delete Secret consents to the orphan; "+
			"the peer keeps an ENDED end, the replica directory, its export and its quota",
			"volume", parsed.Name, "relationship", rel.id, "peer", peerAddr, "error", peerErr,
			"replicaPath", rel.status.TargetRootPath)
	}

	if err := view.DeleteSource(ctx, rel.id); err != nil && !qumulo.IsNotFound(err) {
		return unreachableOr(parsed.Server, err,
			"%s: disabling replication relationship %s on cluster %q before deleting volume %q",
			rpc, rel.id, parsed.Server, parsed.Name)
	}
	slog.InfoContext(ctx, rpc+": replication disabled before deleting the volume",
		"volume", parsed.Name, "relationship", rel.id, "cluster", parsed.Server,
		"peer", peerAddr, "peerEndRemoved", peerRemoved)
	return nil
}

// removePeerTargetEnd deletes the relationship's target end on the peer. It
// reports whether the end is gone (removed now, or already absent), the peer
// it addressed, and why not otherwise. It never touches the local array.
func (s *ControllerServer) removePeerTargetEnd(
	ctx context.Context,
	parsed volumeID,
	rel protectedRelationship,
	params replicationParams,
	secrets map[string]string,
) (removed bool, peerAddr string, reason error) {
	peerAddr = firstNonEmpty(params.TargetServer, rel.status.TargetAddress)
	if peerAddr == "" {
		return false, "", errors.New("the relationship names no peer address and the delete Secret carries no targetServer")
	}
	peer, err := s.peerReplicationClient(peerAddr, params, secrets)
	if err != nil {
		return false, peerAddr, err
	}
	if err := peer.DeleteTarget(ctx, rel.id); err != nil && !qumulo.IsNotFound(err) {
		return false, peerAddr, err
	}
	slog.InfoContext(ctx, "deletevolume: target end removed on the peer",
		"volume", parsed.Name, "relationship", rel.id, "peer", peerAddr)
	return true, peerAddr, nil
}

// peerReplicationClient builds a client for the peer array from this
// request's own Secret. It returns the replicationClient the replication
// service uses -- the ordinary volume verbs plus the replication ones --
// because raising the replica's quota needs both.
//
// The token must be keyed by the peer's address, exactly as the replication
// service requires (replication_server.go resolve). secrets[token] is
// deliberately not a fallback: it is the local array's credential, and using
// it against the peer is how a request naming one cluster gets answered
// against another.
func (s *ControllerServer) peerReplicationClient(
	peerAddr string,
	params replicationParams,
	secrets map[string]string,
) (replicationClient, error) {
	if err := validateServerHost(peerAddr); err != nil {
		return nil, err
	}
	peerURL := apiURLFor(peerAddr, params.TargetAPIURL)
	// Only the token keyed by the host actually dialled. Falling back to the
	// targetServer's key sent that array's token to a targetApiUrl on another
	// host.
	token := firstNonEmpty(secrets[hostOfURL(peerURL)])
	if token == "" {
		return nil, errNoPeerCredential
	}
	peer, ok := s.driver.NewClient(peerURL, token).(replicationClient)
	if !ok {
		return nil, errors.New("the client built for the peer cannot speak the replication API")
	}
	return peer, nil
}

// expandReplicaFirst raises the replica's quota before the source's is
// touched.
//
// Two measured facts make the order mandatory rather than tidy. Replication
// ignores the target quota, so a 1.3 GiB file replicated cleanly into a 1 GiB
// replica directory. Clients do not ignore it, so the first write after a
// promote onto an under-sized replica fails ENOSPC. Growing
// the source first and failing on the peer would leave exactly that pair: a
// source the workload can fill and a replica that cannot take it.
//
// So the peer goes first and a peer that cannot be reached is Unavailable
// with the source quota untouched -- the controller retries, and the volume
// simply has not grown yet, which is a state every CO already handles.
//
// Returns nil when the volume is not a replication source, which is the
// ordinary case and costs two status listings.
func (s *ControllerServer) expandReplicaFirst(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeID,
	realPath string,
	wanted uint64,
	secrets map[string]string,
) error {
	const rpc = "controllerexpandvolume"

	params, err := replicationParamsFromSecrets(secrets)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%s: %v", rpc, err)
	}
	view, err := replicationViewOf(client, rpc, parsed.Server)
	if err != nil {
		return err
	}
	rels, err := relationshipsAt(ctx, view, rpc, parsed.Server, realPath)
	if err != nil {
		return err
	}

	for _, rel := range rels {
		if rel.role != roleSource {
			if !rel.status.TargetRootReadOnly {
				// A writable target end is a promoted copy: the live one.
				// Raising its own quota is harmless, so the caller goes
				// ahead; there is no third array to mirror onto.
				slog.InfoContext(ctx, rpc+": volume is a promoted replication target; raising its own quota",
					"volume", parsed.Name, "relationship", rel.id)
				continue
			}
			if err := s.expandLiveCopyToo(ctx, client, params, secrets, parsed, realPath, rel, wanted); err != nil {
				return err
			}
			continue
		}

		peerAddr := firstNonEmpty(params.TargetServer, rel.status.TargetAddress)
		if peerAddr == "" {
			return status.Errorf(codes.FailedPrecondition,
				"%s: volume %q is the source of replication relationship %s but its status names no target "+
					"address, so the replica's quota cannot be raised first. Set %s in the expand Secret",
				rpc, parsed.Name, rel.id, paramTargetServer)
		}
		peer, err := s.peerReplicationClient(peerAddr, params, secrets)
		if err != nil {
			// Not Unavailable: nothing was dialed. The remedy is a Secret
			// key, and saying so is the difference between an operator
			// editing one line and chasing a network fault.
			return status.Errorf(codes.FailedPrecondition,
				"%s: volume %q is protected by replication relationship %s and its replica's quota must be "+
					"raised before this array's, but the peer %s cannot be contacted: %v. Add that array's "+
					"token to the expand Secret, keyed by its address",
				rpc, parsed.Name, rel.id, peerAddr, err)
		}

		replicaID, replicaPath, promoted, err := replicaRootOnPeer(ctx, peer, rel.id, rpc, peerAddr)
		if err != nil {
			return err
		}
		if promoted {
			// The peer's end is a writable promoted copy, so this side is the
			// stale primary, and the peer's quota belongs to the live copy.
			// It is not this expand's to touch. Nor is this side's:
			// raising the stale copy alone and answering success is how the
			// claim came to say 2 GiB while the live copy stayed at 1
			// (the unplanned-failover shape), so the expand is refused
			// with both copies as they were.
			return staleSourceRefusal(rpc, parsed, realPath, rel, peerAddr)
		}

		// A group relationship is rooted at the class, not at this volume,
		// so its root on the peer is the class directory. Raising THAT to
		// the volume's new limit would cap the whole class at one member's
		// size. The member sits at the same relative path on both arrays, so
		// that is what has to be resolved and grown.
		if rel.memberRel != "" {
			memberPath := path.Join(replicaPath, rel.memberRel)
			member, err := peer.LookupFileByPath(ctx, memberPath)
			if err != nil {
				if qumulo.IsNotFound(err) {
					// Not replicated yet, so there is nothing to raise and
					// nothing to fail. Its quota is staged from the source's
					// once it arrives: by Enable while the group's first sync
					// runs (prestageMemberQuotas), and after that by the
					// group's GetVolumeReplicationInfo (stageLateMemberQuotas) -- not by
					// a "next reconcile", which did not exist.
					slog.WarnContext(ctx, rpc+": volume is a member of group relationship "+
						"but has not replicated to the peer yet; its quota there will be staged "+
						"from the source's once it arrives",
						"volume", parsed.Name, "relationship", rel.id, "peer", peerAddr, "memberPath", memberPath)
					continue
				}
				return unreachableOr(peerAddr, err,
					"%s: locating replica %q of group member %q on peer %s",
					rpc, memberPath, parsed.Name, peerAddr)
			}
			replicaID, replicaPath = member.ID, memberPath
		}

		if err := raiseQuota(ctx, peer, replicaID, wanted); err != nil {
			return unreachableOr(peerAddr, err,
				"%s: raising the quota of replica %q (%s) on peer %s to %d bytes before growing the source",
				rpc, replicaPath, replicaID, peerAddr, wanted)
		}
		slog.InfoContext(ctx, rpc+": replica quota raised before the source's",
			"volume", parsed.Name, "relationship", rel.id, "peer", peerAddr,
			"replicaPath", replicaPath, "replicaFileID", replicaID, "limitBytes", wanted)
	}
	return nil
}

// expandLiveCopyToo handles an expand that lands on a read-only replica.
//
// That is where an expand of a DNS-named claim goes after a failover: its
// handle names the array it was made on, which is now the replica. Raising
// only the replica's quota grew the copy nothing writes to, left the live copy
// at its old size and reported success, so the workload hit ENOSPC at the old
// limit while its claim said otherwise. The same call is also the
// documented way to raise a replica's quota by hand before a promote,
// and the array cannot tell the two apart. So both copies are raised: this
// replica first, then the live copy on the source, each only ever upwards
// (raiseQuota), keeping the replica at least as large as the source at every
// step. The caller then raises this array's quota, which is by then a no-op.
//
// The source is the peer the expand Secret names (targetServer), or the one
// the target end reports after a disconnect. With neither, or no token for
// it, the expand is refused with the remedy; a source that does not answer is
// Unavailable. Nothing is changed in either case.
func (s *ControllerServer) expandLiveCopyToo(
	ctx context.Context,
	client qumulo.ClientInterface,
	params replicationParams,
	secrets map[string]string,
	parsed volumeID,
	realPath string,
	rel protectedRelationship,
	wanted uint64,
) error {
	const rpc = "controllerexpandvolume"

	sourceAddr := firstNonEmpty(params.TargetServer, rel.status.SourceAddress)
	if sourceAddr == "" {
		return status.Errorf(codes.FailedPrecondition,
			"%s: %q on cluster %q is the read-only replica of replication relationship %s, whose live copy "+
				"is on %s; both have to grow, but that array's address is not known here. Set %s in the "+
				"expand Secret to it, with its token keyed by that address",
			rpc, realPath, parsed.Server, rel.id,
			firstNonEmpty(rel.status.SourceClusterName, "the source array"), paramTargetServer)
	}
	source, err := s.peerReplicationClient(sourceAddr, params, secrets)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition,
			"%s: %q on cluster %q is the read-only replica of replication relationship %s, and its live copy "+
				"on %s has to grow with it, but that array cannot be contacted: %v. Add its token to the "+
				"expand Secret, keyed by its address",
			rpc, realPath, parsed.Server, rel.id, sourceAddr, err)
	}
	sourceID, sourcePath, err := sourceRootOnPeer(ctx, source, rel.id, rpc, sourceAddr)
	if err != nil {
		return err
	}
	if rel.memberRel != "" {
		sourcePath = path.Join(sourcePath, rel.memberRel)
		member, err := source.LookupFileByPath(ctx, sourcePath)
		if err != nil {
			return unreachableOr(sourceAddr, err, "%s: locating live copy %q of group member %q on %s",
				rpc, sourcePath, parsed.Name, sourceAddr)
		}
		sourceID = member.ID
	}

	// This replica first, so it is never smaller than the source.
	local, err := client.LookupFileByPath(ctx, realPath)
	if err != nil {
		return unreachableOr(parsed.Server, err, "%s: locating replica %q on %s", rpc, realPath, parsed.Server)
	}
	if err := raiseQuota(ctx, client, local.ID, wanted); err != nil {
		return unreachableOr(parsed.Server, err, "%s: raising the quota of replica %q to %d bytes", rpc, realPath, wanted)
	}
	if err := raiseQuota(ctx, source, sourceID, wanted); err != nil {
		return unreachableOr(sourceAddr, err, "%s: raising the quota of live copy %q on %s to %d bytes",
			rpc, sourcePath, sourceAddr, wanted)
	}
	slog.WarnContext(ctx, rpc+": expand landed on a read-only replica; raised the live copy on the source too",
		"volume", parsed.Name, "relationship", rel.id, "source", sourceAddr, "sourcePath", sourcePath,
		"limitBytes", wanted)
	return nil
}

// refuseAStaleSource answers FailedPrecondition when rel, a source end on the
// array a handle names, is the stale half of an unplanned failover: the
// peer's target end has been promoted (DISCONNECTED and writable), so the
// live copy is the peer's and this directory is behind it.
//
// The source end's own state says nothing about this. Measured live, it
// keeps reporting ESTABLISHED after the target's make-writable; only a
// reverse makes both ends DISCONNECTED. So every source end is asked about
// on the peer, with the token the Secret keys by the peer's address, and the
// question cannot go unanswered: with no peer address or no token for it the
// call is refused with the remedy, and a peer that does not answer is
// Unavailable, retried. Acting on a copy that may be stale is the one thing
// never done. A peer that no longer lists the relationship holds no
// promoted copy of it, and the call goes ahead.
//
// The peer is the one the Secret names (targetServer), or the one the source
// end reports, exactly as the expand and delete guards resolve it. Target
// ends are not this function's concern: a read-only one is refused by the
// caller as a replica, a writable one is the promoted copy itself.
//
// refuseWhenUnknown is what happens when the peer cannot be asked. A
// snapshot has no other guard and no consent to fall back on, so it passes
// true. A delete passes false: its own policy logic already settles the
// unreachable peer -- refuse by default, refuse with the remedy under
// "disable", and under "disable-orphan" the operator's recorded consent to
// proceed without the peer -- and that consent is left as it is. What no
// policy overrides is a peer that answers it has been promoted.
func (s *ControllerServer) refuseAStaleSource(
	ctx context.Context,
	rpc string,
	parsed volumeID,
	realPath string,
	rel protectedRelationship,
	params replicationParams,
	secrets map[string]string,
	refuseWhenUnknown bool,
) error {
	if rel.role != roleSource {
		return nil
	}
	peerAddr := firstNonEmpty(params.TargetServer, rel.status.TargetAddress)
	if peerAddr == "" {
		if !refuseWhenUnknown {
			return nil
		}
		return status.Errorf(codes.FailedPrecondition,
			"%s: %q on cluster %q is the source of replication relationship %s (%s), and whether its peer "+
				"holds the promoted copy cannot be checked because the relationship names no peer address. "+
				"Set %s in this request's Secret to the peer array, with its token keyed by that address; "+
				"nothing was changed",
			rpc, realPath, parsed.Server, rel.id, rel.status.State, paramTargetServer)
	}
	peer, err := s.peerReplicationClient(peerAddr, params, secrets)
	if err != nil {
		if !refuseWhenUnknown {
			return nil
		}
		return status.Errorf(codes.FailedPrecondition,
			"%s: %q on cluster %q is the source of replication relationship %s (%s), and whether its peer %s "+
				"holds the promoted copy cannot be checked: %v. Add that array's token to this request's "+
				"Secret, keyed by its address; nothing was changed",
			rpc, realPath, parsed.Server, rel.id, rel.status.State, peerAddr, err)
	}
	t, err := peer.GetTargetStatus(ctx, rel.id)
	if err != nil {
		if qumulo.IsNotFound(err) {
			// The peer no longer lists the relationship: whatever it holds
			// is not a promoted end of this one.
			return nil
		}
		if !refuseWhenUnknown {
			slog.WarnContext(ctx, rpc+": the peer could not be asked whether it holds the promoted copy; "+
				"the delete policy decides", "volume", parsed.Name, "relationship", rel.id, "peer", peerAddr, "error", err)
			return nil
		}
		return replicationListError(peerAddr, err,
			"%s: reading target relationship %s on peer %s to check it has not been promoted", rpc, rel.id, peerAddr)
	}
	if t.State == qumulo.RelationshipDisconnected && !t.TargetRootReadOnly {
		return staleSourceRefusal(rpc, parsed, realPath, rel, peerAddr)
	}
	return nil
}

// staleSourceRefusal is the answer every RPC gives on a stale source: the
// peer named, and the two ways out.
func staleSourceRefusal(rpc string, parsed volumeID, realPath string, rel protectedRelationship, peerAddr string) error {
	slog.WarnContext(context.Background(), rpc+": refusing to act on a stale source; the peer holds the promoted copy",
		"volume", parsed.Name, "path", realPath, "relationship", rel.id, "peer", peerAddr, "state", rel.status.State)
	return status.Errorf(codes.FailedPrecondition,
		"%s: %q on cluster %q is the stale source of replication relationship %s: its peer %s holds the "+
			"promoted, writable copy, and this directory is behind it. Demote this side (set its "+
			"VolumeReplication to secondary) so it becomes the replica, or re-point the PersistentVolume "+
			"to %s (qumulo-csi-repoint); nothing was changed",
		rpc, realPath, parsed.Server, rel.id, peerAddr, peerAddr)
}

// sourceRootOnPeer finds the source directory of one relationship on the
// array that sources it, by relationship ID as replicaRootOnPeer does.
func sourceRootOnPeer(
	ctx context.Context,
	peer qumulo.ReplicationClient,
	id, rpc, peerAddr string,
) (fileID, rootPath string, err error) {
	src, err := peer.GetSourceStatus(ctx, id)
	if err != nil && !qumulo.IsNotFound(err) {
		return "", "", replicationListError(peerAddr, err, "%s: reading source relationship %s on %s", rpc, id, peerAddr)
	}
	if err == nil && src.SourceRootID != "" {
		return src.SourceRootID, src.SourceRootPath, nil
	}
	return "", "", status.Errorf(codes.FailedPrecondition,
		"%s: %s does not list relationship %s as one it sources, so where the live copy is cannot be "+
			"established; nothing was changed", rpc, peerAddr, id)
}

// replicaRootOnPeer finds the replica directory of one relationship on the
// peer array.
//
// The relationship ID is the join key: it is the one identifier both arrays
// report for the same relationship, and the reason the replication service
// matches on it rather than on paths or addresses (there is no cluster_uuid
// endpoint, and with the same path on both arrays, a path comparison
// says nothing). The role-specific endpoint is what says which role the peer
// holds, which matters after a reverse: an array answers 404 for the ID on
// the other role's endpoint (checked live on both lab arrays, 2026-10-06), so
// one by-ID read replaces listing the peer's whole table.
//
// promoted reports that the peer's end is a writable promoted copy (the
// peerPromoted test), so the caller can leave the live copy alone.
func replicaRootOnPeer(
	ctx context.Context,
	peer qumulo.ReplicationClient,
	id, rpc, peerAddr string,
) (fileID, rootPath string, promoted bool, err error) {
	t, err := peer.GetTargetStatus(ctx, id)
	if err != nil && !qumulo.IsNotFound(err) {
		return "", "", false, replicationListError(peerAddr, err, "%s: reading target relationship %s on peer %s", rpc, id, peerAddr)
	}
	if err == nil {
		if t.TargetRootID == "" {
			// Authorized but not yet materialised: the target directory
			// exists only from authorize onward.
			return "", "", false, status.Errorf(codes.Aborted,
				"%s: peer %s lists relationship %s but reports no replica directory yet; retrying",
				rpc, peerAddr, id)
		}
		// The test peerPromoted applies: disconnected and writable.
		promoted := t.State == qumulo.RelationshipDisconnected && !t.TargetRootReadOnly
		return t.TargetRootID, t.TargetRootPath, promoted, nil
	}
	return "", "", false, status.Errorf(codes.Aborted,
		"%s: peer %s does not list relationship %s as one it is the target of, so which directory holds the "+
			"replica cannot be established; retrying", rpc, peerAddr, id)
}
