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
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/csi-addons/spec/lib/go/identity"
	"github.com/csi-addons/spec/lib/go/replication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"qumulo-csi-driver/pkg/qumulo"
)

// ReplicationServer implements the csi-addons replication.Controller service:
// enable, disable, promote, demote, resync and info for one volume at a time,
// each mapped onto Qumulo directory replication between the array the volume
// lives on and the peer array the VolumeReplicationClass names.
//
// Supported, opt-in. The state tables it implements were measured in the lab
// and the
// mock server reproduces the transitions. It has run on the lab Kubernetes
// cluster with the csi-addons controller and sidecar v0.15.0 driving it
// through VolumeReplication objects: protect, planned
// failover and failback, a forced failover with the source array powered
// off, and the one-command failover tool, over thirteen Helm revisions. It
// is served only behind -enable-replication.
//
// Rule 0 holds in the same way it does for ControllerServer: every RPC builds
// two clients -- local and peer -- from its own parameters and secrets, holds
// nothing between calls, and finds the relationship it is about by listing
// the arrays rather than by remembering an ID. Rule 4 holds too: the sidecar
// watches Kubernetes, this code never does. The one thing kept between
// calls is groupStaging, a hint that only lets work be skipped.
type ReplicationServer struct {
	replication.UnimplementedControllerServer
	driver *Driver

	// drainBudget bounds how long a planned promote waits for the frozen
	// source to drain, and drainPollWait spaces the status checks. Fields
	// rather than constants so tests can exercise the Aborted path without
	// waiting for it.
	drainBudget   time.Duration
	drainPollWait time.Duration

	// peerRetries and peerRetryWait are how often, and from what first
	// backoff, a promote asks an unanswering peer again before concluding it
	// is unreachable. Fields for the same reason as the two above.
	peerRetries   int
	peerRetryWait time.Duration
	// peerCallTimeout is how long one ask of the peer can take before it
	// fails: the API client's timeout. A retry is started only if its wait
	// and its call both fit before the deadline. replyMargin is kept free at
	// the end of the caller's own deadline, so a promote answers with a code
	// of its choosing before the caller gives up on it.
	peerCallTimeout time.Duration
	replyMargin     time.Duration

	// groupStaging remembers, per group relationship, the membership a
	// source-side Info last staged quotas for, so an unchanged group is not
	// re-staged on every poll. A hint only: lost with the process, it
	// costs one full staging; it never stands in for reading the arrays.
	groupStaging sync.Map // groupStagingKey -> groupStaged

	// assessed is the status each relationship's assessment last logged at
	// Info, so an unchanged one is logged at Debug.
	assessed sync.Map // relationship ID -> status string

	// promotes counts the promotes running on a context that ignores
	// cancellation, so a controller shutdown can wait for them.
	promotes promoteTracker

	// hints are this server's read-and-verify shortcuts (ARCHITECTURE.md rule 0).
	hints replicationHints

	// revertWaits are the reconnects this controller ran whose revert
	// snapshot a resync is still waiting for, and revertSnapshotWait how long
	// it waits (zero: DefaultRevertSnapshotWait).
	revertWaits        sync.Map // revertWaitKey -> revertWait
	revertSnapshotWait time.Duration

	// memberList is the PersistentVolume listing the group protection
	// fan-out reuses, so a continuously polled Info does not list per RPC.
	// See replication_protection.go.
	memberList memberListCache
}

// replicationHints is per server, not per process, so two servers -- tests,
// above all -- never answer from each other's.
type replicationHints struct {
	// markers: relationship + source root -> freeze marker snapshot, as an
	// idHint.
	markers sync.Map
	// policyIntervals: relationship + policy -> policyIntervalHint.
	policyIntervals sync.Map
	// relationships: local address + path -> relationshipHint, for reports
	// only.
	relationships sync.Map
	// groupRoots: local address + member path -> groupRootHint, for reports
	// only.
	groupRoots sync.Map
	// groupFanouts: local address + member path -> groupFanoutHint, the
	// group root a report last exported its members' verdict from, so a
	// report that later finds no relationship zeroes the same members
	// rather than leaving their series behind.
	groupFanouts sync.Map
	// sweeper runs the sweep of all of these and groupStaging.
	sweeper hintSweeper
}

// noteHintStored is called after a store into one of this server's hint
// maps, and sweeps them all when a sweep is due.
func (r *ReplicationServer) noteHintStored() {
	now := time.Now()
	if !r.hints.sweeper.due(now) {
		return
	}
	r.sweepHints(now)
}

// sweepHints drops every hint of this server past its use at now.
func (r *ReplicationServer) sweepHints(now time.Time) {
	sweepMap(&r.hints.markers, idleSince(now.Add(-idleHintExpiry)))
	sweepMap(&r.hints.policyIntervals, func(v any) bool {
		return now.Sub(v.(policyIntervalHint).at) >= policyIntervalReuse
	})
	sweepMap(&r.hints.relationships, func(v any) bool { return v.(relationshipHint).expired(now) })
	sweepMap(&r.hints.groupRoots, func(v any) bool { return v.(groupRootHint).expired(now) })
	sweepMap(&r.hints.groupFanouts, func(v any) bool { return v.(groupFanoutHint).expired(now) })
	sweepMap(&r.groupStaging, func(v any) bool { return now.Sub(v.(groupStaged).at) >= groupRestageEvery })
}

// PlannedPromoteBudget is the longest a promote runs once started when the
// caller set no deadline of its own: the drain, then making the target
// writable and re-plumbing it. A shutdown that waits this long lets one that
// began just before the signal finish.
const PlannedPromoteBudget = defaultDrainBudget + promoteFinishBudget

// promoteTracker counts promotes in flight and lets shutdown wait for them.
type promoteTracker struct {
	wg sync.WaitGroup
	n  atomic.Int64

	// byRel counts, per relationship ID, the planned promotes between their
	// freeze and their re-plumb, so a resync on this controller does not
	// lift a freeze a promote here is about to rely on.
	mu    sync.Mutex
	byRel map[string]int
}

// beginFor is begin for a planned promote of relID, also counted by
// relationship (inFlight).
func (t *promoteTracker) beginFor(relID string) func() {
	done := t.begin()
	t.mu.Lock()
	if t.byRel == nil {
		t.byRel = map[string]int{}
	}
	t.byRel[relID]++
	t.mu.Unlock()
	return func() {
		t.mu.Lock()
		if t.byRel[relID]--; t.byRel[relID] <= 0 {
			delete(t.byRel, relID)
		}
		t.mu.Unlock()
		done()
	}
}

// inFlight reports whether a planned promote of relID is running in this
// process.
func (t *promoteTracker) inFlight(relID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.byRel[relID] > 0
}

// begin counts one promote; the func it returns uncounts it.
func (t *promoteTracker) begin() func() {
	t.n.Add(1)
	t.wg.Add(1)
	return func() {
		t.n.Add(-1)
		t.wg.Done()
	}
}

// PromotesInFlight is how many promotes are past their freeze, or past
// make-writable, and not yet returned.
func (r *ReplicationServer) PromotesInFlight() int {
	return int(r.promotes.n.Load())
}

// WaitForPromotes waits up to timeout for the promotes in flight to return,
// and says whether they did. Used by the controller's shutdown: a promote cut
// off after its freeze leaves both copies read-only until csi-addons retries
// it on another controller.
func (r *ReplicationServer) WaitForPromotes(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		r.promotes.wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// NewReplicationServer creates a ReplicationServer backed by the given Driver.
func NewReplicationServer(driver *Driver) *ReplicationServer {
	return &ReplicationServer{
		driver:        driver,
		drainBudget:   defaultDrainBudget,
		drainPollWait: defaultDrainPollWait,
		peerRetries:   defaultPeerRetries,
		peerRetryWait: defaultPeerRetryWait,

		peerCallTimeout: defaultPeerCallTimeout,
		replyMargin:     defaultReplyMargin,
	}
}

const (
	// defaultDrainBudget is well inside the csi-addons sidecar's default RPC
	// timeout, so a slow drain ends in a deliberate Aborted -- which the
	// controller retries, with the freeze holding -- rather than a context
	// deadline the caller cannot tell from a hang.
	defaultDrainBudget   = 45 * time.Second
	defaultDrainPollWait = 2 * time.Second
	// defaultPeerRetries and defaultPeerRetryWait: two more asks, 3 s then
	// 6 s apart, never past the promote's own deadline. Each ask is a real
	// call, past the circuit breaker (consultPeerForPromote), so a peer has
	// to stay silent across 0 s, +3 s and +9 s of one promote call, plus
	// each call's own time to fail -- up to the API's 10 s timeout for a hung
	// API, about 40 s in all -- and again on the forced re-call, before a
	// promote skips the freeze; a real outage still promotes, under a
	// minute later than it did.
	defaultPeerRetries   = 2
	defaultPeerRetryWait = 3 * time.Second
	// defaultPeerCallTimeout is the API client's per-call timeout
	// (pkg/qumulo NewClient), and defaultReplyMargin the time kept free
	// before the caller's deadline for the answer itself.
	defaultPeerCallTimeout = 10 * time.Second
	defaultReplyMargin     = 2 * time.Second
	// defaultFirstPollWait is where a poll loop starts before backing off to
	// drainPollWait. A drain takes as long as the data takes and 2 s between
	// asks is right for it; the target's make-writable transition does not.
	//
	// Measured on the 7.9.2.2 lab arrays: make-writable answered DISCONNECTING
	// in all 62 captured calls, and no status read ever caught the array
	// still in it -- the five pairs with a follow-up settled in 152-163 ms.
	// A flat 2 s first wait therefore spent about 1.84 s of every promote
	// waiting on something already finished. Starting here and doubling finds
	// it on the second ask, around 300 ms, and still waits patiently for an
	// array that is genuinely slow.
	defaultFirstPollWait = 100 * time.Millisecond
	// freezeMarkerSuffix names the snapshot a planned promote takes of the
	// frozen source tree (the array prepends the snapshot id). The marker is
	// the drain gate's reference: the target is complete exactly when the
	// last replicated snapshot differs from it in nothing. It lives on
	// the array, so a retry finds the same marker and no clock or cycle
	// timing enters the decision.
	freezeMarkerSuffix = "csi-freeze-"
	// heldFreezeTag prefixes the marker of a freeze the promote did not make:
	// Demote's, or one the drain found already in place. The drain
	// finds both kinds by the shared suffix; only the DR side's resync asks
	// which kind it is, because a demote's freeze is the operator's and is
	// held for their promote.
	heldFreezeTag = "held-"
	// pendingFreezeTag prefixes the marker a planned promote takes BEFORE it
	// freezes an unfrozen source: the record, on the array, that the freeze
	// about to be made is the promote's own. It is never the drain's
	// reference -- it predates the freeze, and a replicated snapshot taken
	// between it and the freeze would compare equal to it while missing the
	// writes made after -- so once the source is frozen it is replaced by a
	// plain marker of the frozen tree. Like a plain marker it is not held:
	// a freeze found under it is a promote's and an undo may lift it.
	pendingFreezeTag = "pending-"
	// freezeMarkerTTL is the expiration the cluster enforces on a marker, so
	// one that a crash left behind cannot outlive the failover it served.
	freezeMarkerTTL = 24 * time.Hour
)

// replicationClient is what the service needs of an array: the ordinary
// volume operations and the replication verbs. *qumulo.Client is both;
// ClientInterface is deliberately not widened, so this is asserted per call.
type replicationClient interface {
	qumulo.ClientInterface
	qumulo.ReplicationClient
}

// replicationEnds is one RPC's resolved view: which array is local, which is
// the peer, where the volume is on each, and whether it has its own export.
type replicationEnds struct {
	vol    volumeID
	params replicationParams

	local, peer replicationClient
	// localAddr is the host the local API URL names; peerAddr is what the
	// class calls the peer, which is also what the source array is told to
	// replicate to. Both are for logs, error messages and the create body,
	// never for matching a relationship (see findRelationship).
	localAddr, peerAddr string

	// realPath is the volume's directory on local, verified to exist there.
	// peerPath is where its copy lives on the peer.
	realPath, peerPath string
	// rootOwner and rootGroup are realPath's owner and group as the lookup
	// that verified it read them.
	rootOwner, rootGroup qumulo.Identity

	// fileID is LOCAL's own file ID for realPath, resolved once per RPC.
	//
	// It is not in the handle and must not be: a file ID is allocated by the
	// array that made the directory and replication does not preserve it, so
	// the source's ID names a different directory on the peer. That
	// is precisely why the handle carries the path instead. Quota staging,
	// the promote's re-plumbing and the drift check all need an ID, and this
	// is the one that is right for the array this RPC is talking to.
	fileID string

	// export is the volume's own NFS export on local, or nil when the volume
	// is a shared-subdirectory one and has none. Its presence is how the
	// service tells the two provisioning models apart without a StorageClass.
	// In group scope it is the class's covering export, which is the one
	// export the whole group needs mirrored.
	export *qumulo.ExportInfo

	// scope is whether this RPC is about the one volume named in volume_id
	// or the whole class it belongs to. In group scope vol, realPath and
	// peerPath have already been replaced by the group's, so every RPC body
	// below works on the group without knowing it. See replication_group.go.
	scope replicationScope

	// unpromoted is set by Enable's peer check when the peer's replica
	// export is read-write but the copy is unchanged since the Disable that
	// marked it (replication_unpromoted.go): prestage then leaves an
	// operator's export as it is without refusing, and the markers go once
	// the relationship exists.
	unpromoted *unpromotedProof
}

// relRole is which end of a relationship the local array holds.
type relRole int

const (
	roleSource relRole = iota
	roleTarget
)

func (r relRole) String() string {
	if r == roleSource {
		return "source"
	}
	return "target"
}

// relationship is the relationship an RPC is about, as both arrays see it.
type relationship struct {
	id   string
	role relRole
	// local is the local array's status for it; peer is the peer array's,
	// or nil when the peer was not contacted. peerRole is which of the
	// peer's lists it came from -- after a reverse the same ID appears in
	// the other list, and with the same path on both arrays the list
	// is the only thing that says which way the relationship now points.
	local    qumulo.RelationshipStatus
	peer     *qumulo.RelationshipStatus
	peerRole relRole
	// peerConsulted says the peer array ANSWERED the listing this match was
	// made from. It is what tells the two meanings of a nil peer apart:
	// "the peer could not be reached" (peerConsulted false) and "the peer
	// answered and holds no end of this relationship at the path the class
	// implies" (peerConsulted true). The first is what a forced promote is
	// for; the second is a misconfigured class -- a mistyped targetBasePath,
	// a targetServer naming a third array -- and forcing past it is how two
	// writable copies get made.
	peerConsulted bool
}

// peerStatus is one relationship as the peer reports it, with the list it
// was found in.
type peerStatus struct {
	status qumulo.RelationshipStatus
	role   relRole
}

// unmatchedPeerError is the refusal for a relationship the configured peer
// answered about and holds no end of (peerConsulted, peer nil): a class that
// disagrees with the relationship, not a dead peer. why says what was about
// to happen and is refused.
func unmatchedPeerError(rpc string, e *replicationEnds, rel *relationship, why string) error {
	if rel.local.State == qumulo.RelationshipEnded {
		// Not a class that disagrees: this end is ENDED because the other
		// array deleted its end, so of course the peer lists none. Sending
		// the operator to targetBasePath and targetServer was wrong.
		// Still InvalidArgument -- never FailedPrecondition, which on a
		// promote the controller answers with force -- and the remedy is a
		// fresh relationship: Enable removes an ENDED end and protects anew.
		return status.Errorf(codes.InvalidArgument,
			"%s: relationship %s is ENDED on %s (this side was its %s, end reason %q): the other end of the "+
				"relationship was deleted on %s, so nothing replicates. Disable replication for this volume "+
				"(delete the VolumeReplication) and enable it again from the side whose copy is to be kept; "+
				"Enable removes the ENDED end and protects the volume anew. %s",
			rpc, rel.id, e.localAddr, rel.role, rel.local.EndReason, e.peerAddr, why)
	}
	return status.Errorf(codes.InvalidArgument,
		"%s: peer %s answered but lists no end of relationship %s at %q (this side is its %s, %s). "+
			"The VolumeReplicationClass disagrees with the relationship: check %s and %s; %s",
		rpc, e.peerAddr, rel.id, e.peerPath, rel.role, rel.local.State, paramTargetBasePath, paramTargetServer, why)
}

// refuseUnmatchedPeer is the peer-match check for Enable, Disable and Demote --
// Promote and Resync make it inline. It refuses, with unmatchedPeerError, a
// local end of a relationship that the configured peer answered about and
// holds no end of, and returns nil for everything else.
//
// The check is about a local end that EXISTS. A first Enable finds no
// relationship anywhere and creates one toward whatever the class names: the
// absence of an end is what an unprotected volume looks like, and no array
// can say which peer the operator meant. What can be told is the other
// shape: this array holds an end, the configured peer answers, and the peer
// holds nothing of it at the class's path. That is a mistyped
// targetBasePath, or a targetServer naming a third array, and acting on it
// went wrong in every RPC that did not check: Enable staged onto a path that
// never appears and spun on Aborted; Disable deleted this side's end, sent
// the peer delete to the wrong array (NotFound, read as done) and answered
// OK with the real peer's end orphaned; Demote froze a source whose real
// peer was never going to be promoted through this class.
//
// Two shapes are left alone, because the peer holding nothing is what they
// are:
//
//   - An ENDED local end. The peer deleted its half (that is what ENDED
//     means), so it holds nothing at any path; Enable clears the corpse,
//     Disable removes it, Demote reports it.
//   - A source end still AWAITING_AUTHORIZATION that names exactly the
//     class's peer and path. Core lists the new relationship on the peer a
//     moment after the create, and authorize answers NotFound until it does
//     (Aborted, retried); refusing here would turn that window into an
//     InvalidArgument. An awaiting end naming another address or path is not
//     this class's, and is refused like any other.
//
// A peer that did not answer never reaches here as "holds nothing"
// (peerConsulted is false): each caller keeps its own unreachable-peer
// handling.
func refuseUnmatchedPeer(rpc string, e *replicationEnds, rel *relationship, why string) error {
	if rel == nil || rel.peer != nil || !rel.peerConsulted || rel.local.State == qumulo.RelationshipEnded {
		return nil
	}
	if rel.role == roleSource && rel.local.State == qumulo.RelationshipAwaitingAuthorization &&
		rel.local.TargetRootPath == e.peerPath &&
		(rel.local.TargetAddress == "" || rel.local.TargetAddress == e.peerAddr) {
		return nil
	}
	return unmatchedPeerError(rpc, e, rel, why)
}

// coveringGroupRoot finds the root of the one live group relationship that
// covers e's volume from above, for an Info that named no group.
//
// relationshipsAt already makes the member match -- it is what the delete and
// expand guards use -- and skips ENDED ends. More than one covering root is
// ambiguous and answers "none": Info then reports the volume as it did before
// rather than guessing which group it belongs to.
func coveringGroupRoot(ctx context.Context, rpc string, e *replicationEnds) (string, bool, error) {
	found, err := relationshipsAt(ctx, e.local, rpc, e.localAddr, e.realPath)
	if err != nil {
		return "", false, err
	}
	roots := map[string]bool{}
	for _, rel := range found {
		if rel.memberRel == "" {
			continue
		}
		if rel.role == roleSource {
			roots[path.Clean(rel.status.SourceRootPath)] = true
		} else {
			roots[path.Clean(rel.status.TargetRootPath)] = true
		}
	}
	if len(roots) != 1 {
		if len(roots) > 1 {
			slog.WarnContext(ctx, rpc+": more than one group relationship covers the volume; not guessing",
				"volume", e.vol.Name, "path", e.realPath, "roots", len(roots))
		}
		return "", false, nil
	}
	for root := range roots {
		return root, true, nil
	}
	return "", false, nil
}

// resolve turns a request into the two arrays and two paths every RPC needs.
//
// Which array is local is decided by the class's apiUrl, then the REST
// endpoint the handle records, then the handle's server: the class is per
// direction, so the array it dials as "local" is the array the
// VolumeReplication is about, and the array it names as the target is the
// peer. The handle's endpoint comes before its server because under
// multitenancy the server is a tenant's NFS address, which serves no REST.
// Role -- source or target -- is not decided here
// at all; it falls out of which status list the relationship appears in.
//
// peerOptional is for GetVolumeReplicationInfo, whose request carries no
// class parameters in csi-addons spec v0.2.0: with no targetServer the peer
// client is left nil and the caller works from the local array's view alone,
// identifying the peer from the relationship status where it can.
func (r *ReplicationServer) resolve(
	ctx context.Context,
	rpc, volumeID, groupID string,
	params, secrets map[string]string,
	peerOptional bool,
) (*replicationEnds, error) {
	vol, err := parseVolumeID(volumeID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	parsed, err := parseReplicationParams(params)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if parsed.TargetServer == "" && !peerOptional {
		return nil, status.Errorf(codes.InvalidArgument,
			"%s: missing required VolumeReplicationClass parameter: %s", rpc, paramTargetServer)
	}
	// The handle's server is the NFS mount target and may be the floating
	// DNS name the runbook recommends; dialing the API there reaches the
	// other array after a flip. A class-driven RPC therefore
	// needs an explicit endpoint whenever that server is a name: the class's
	// apiUrl, or the one the handle recorded at create time. Only a handle
	// with neither falls back to its server. GetVolumeReplicationInfo
	// (peerOptional) carries no parameters at all in csi-addons spec v0.2.0
	// and cannot be held to this; with no endpoint in the handle it is the
	// one RPC that still dials the handle's server, and its answer after a
	// flip is NotFound rather than anything destructive.
	if !peerOptional && parsed.APIURL == "" && vol.APIURL == "" && !isIPLiteral(vol.Server) {
		return nil, status.Errorf(codes.InvalidArgument,
			"%s: volume %q lives on %q, a DNS name, so the VolumeReplicationClass must set %s: %s",
			rpc, vol.Name, vol.Server, paramReplAPIURL, floatingNameReason(vol.Server))
	}

	localURL := apiURLFor(vol.Server, firstNonEmpty(parsed.APIURL, vol.APIURL))
	localAddr := hostOfURL(localURL)
	if localAddr == "" {
		localAddr = vol.Server
	}

	// Tokens are keyed by the array they were minted on -- the host of the
	// endpoint the controller dials -- so one Secret serves both directions.
	// The plain "token" key stays valid for the local array, which is what
	// a class carrying the StorageClass's own Secret provides.
	localToken := firstNonEmpty(secrets[localAddr], secrets[vol.Server], secrets[secretKeyToken])
	if localToken == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"%s: the replication secret carries no token for array %s. This driver keeps no fallback "+
				"credential: key each array's token by its address, and grant it "+
				"PRIVILEGE_REPLICATION_SOURCE_READ, PRIVILEGE_REPLICATION_TARGET_READ, "+
				"PRIVILEGE_REPLICATION_SOURCE_WRITE, PRIVILEGE_REPLICATION_TARGET_WRITE, "+
				"PRIVILEGE_REPLICATION_REVERSE_RELATIONSHIP, PRIVILEGE_SNAPSHOT_DIFFERENCE_READ "+
				"and PRIVILEGE_TIME_READ (see the chart README's privilege table)",
			rpc, localAddr)
	}
	local, err := r.clientFor(localURL, localToken)
	if err != nil {
		return nil, err
	}

	var peer replicationClient
	peerAddr := parsed.TargetServer
	if peerAddr != "" {
		peerURL := apiURLFor(peerAddr, parsed.TargetAPIURL)

		// The class on the wrong Kubernetes cluster names the volume's own
		// array as the target. Refused before any credential is looked at.
		// InvalidArgument, not FailedPrecondition: this is a misconfigured
		// class, and FailedPrecondition is the one code the csi-addons
		// controller answers by re-calling Promote with force.
		if strings.EqualFold(localURL, peerURL) {
			return nil, status.Errorf(codes.InvalidArgument,
				"%s: the VolumeReplicationClass names %q as the target, which is the array volume %q lives on; "+
					"on this cluster the class must name the peer array",
				rpc, parsed.TargetServer, vol.Name)
		}
		// Only the dialled host's own key: falling back to targetServer's sent
		// that array's token to a targetApiUrl on another host.
		peerToken := firstNonEmpty(secrets[hostOfURL(peerURL)])
		if peerToken == "" {
			return nil, status.Errorf(codes.InvalidArgument,
				"%s: the replication secret carries no token for peer array %s; key it by that address",
				rpc, hostOfURL(peerURL))
		}
		if peer, err = r.clientFor(peerURL, peerToken); err != nil {
			return nil, err
		}
	}

	// The same identity check DeleteVolume makes, and for a sharper reason
	// here: a replica has a different file ID from its source, so a handle
	// from one array names nothing -- or something else -- on the other.
	resolved, resolution, err := resolveVolumePath(ctx, local, vol, rpc)
	if err != nil {
		return nil, unreachableOr(localAddr, err, "%s: resolving volume %q on %s", rpc, vol.Name, localAddr)
	}
	if resolution != volumeFound {
		return nil, status.Errorf(codes.NotFound,
			"%s: %q does not name a volume directory on array %s",
			rpc, vol.RealPath, localAddr)
	}

	// Group scope moves the whole RPC up to the class's basePath: the group
	// root becomes the volume, its covering export becomes the export, and
	// every state table below runs on that one relationship unchanged. The
	// member volume named in volume_id has served its purpose by the time
	// this returns -- it identified the array, the credential and the class.
	scope := scopeVolume
	if groupID != "" || parsed.Scope == replicationScopeGroup {
		scope = scopeGroup
		if vol, resolved, err = groupRootFor(ctx, rpc, local, vol, resolved.RealPath, groupID, localAddr); err != nil {
			return nil, err
		}
	}
	realPath := resolved.RealPath

	base := parsed.TargetBasePath
	if base == "" {
		base = path.Dir(realPath)
	}
	peerPath := path.Join(base, vol.Name)
	if peer == nil {
		peerPath = ""
	}

	export, err := findExportByFSPath(ctx, local, realPath)
	if err != nil {
		return nil, unreachableOr(localAddr, err, "%s: listing exports on %s", rpc, localAddr)
	}

	return &replicationEnds{
		vol:       vol,
		params:    parsed,
		scope:     scope,
		local:     local,
		peer:      peer,
		localAddr: localAddr,
		peerAddr:  peerAddr,
		realPath:  realPath,
		rootOwner: resolved.Owner,
		rootGroup: resolved.Group,
		fileID:    resolved.FileID,
		peerPath:  peerPath,
		export:    export,
	}, nil
}

// clientFor builds a client for one array and asserts it can replicate.
func (r *ReplicationServer) clientFor(baseURL, token string) (replicationClient, error) {
	client, ok := r.driver.NewClient(baseURL, token).(replicationClient)
	if !ok {
		return nil, status.Errorf(codes.Internal,
			"the client built for %s does not implement the replication API", baseURL)
	}
	return client, nil
}

// findRelationship finds the relationship rooted at the volume on the local
// array, and its counterpart on the peer when contactPeer is set.
//
// Matching is by path and by relationship ID, never by address: the target
// end reports source_address as null while connected (measured), and the
// runbook's recommended DNS names float. The local array's lists give every
// relationship rooted at realPath in either role; when the peer is reachable
// its lists give every relationship rooted at peerPath, and the intersection
// of IDs is the one this class is about. With the peer down (forced promote)
// the local match must be unique on its own, and an ambiguous table is
// Aborted with the candidates named rather than guessed at.
//
// When the peer answers and none of the local candidates appears in its
// lists, the candidates are returned with peer nil and peerConsulted set:
// the caller must not read that as "the peer is down". It is the shape a
// mistyped targetBasePath or a targetServer naming a third array produces,
// and the caller refuses it as a misconfiguration.
func (r *ReplicationServer) findRelationship(
	ctx context.Context,
	rpc string,
	e *replicationEnds,
	contactPeer bool,
) (*relationship, error) {
	// A report reads the relationship it found last by ID.
	report := reportLookups(ctx) && !contactPeer
	key := e.localAddr + "\x00" + e.realPath
	if report {
		if rel, ok := r.relationshipFromHint(ctx, e, key); ok {
			return rel, nil
		}
	}
	rel, err := r.listRelationship(ctx, rpc, e, contactPeer)
	if report && err == nil {
		if rel != nil {
			r.hints.relationships.Store(key, newRelationshipHint(rel.id, rel.role))
		} else {
			// None here. A report remembers that for a short while, so
			// an unprotected volume's poll does not list both tables on
			// every requeue; an Enable from this controller clears
			// it at once (forgetNoRelationship).
			r.hints.relationships.Store(key, noRelationshipHint(noRelationshipReuse))
		}
		r.noteHintStored()
	}
	return rel, err
}

// forgetNoRelationship drops what reports remember about the paths of a
// relationship just created -- either end, and a group's members below its
// root -- so the next report lists and finds it.
func (r *ReplicationServer) forgetNoRelationship(e *replicationEnds) {
	for _, end := range [][2]string{{e.localAddr, e.realPath}, {e.peerAddr, e.peerPath}} {
		if end[0] == "" || end[1] == "" {
			continue
		}
		forgetUnder(&r.hints.relationships, end[0]+"\x00", end[1])
		forgetUnder(&r.hints.groupRoots, end[0]+"\x00", end[1])
	}
}

// listRelationship is findRelationship from the status tables.
func (r *ReplicationServer) listRelationship(
	ctx context.Context,
	rpc string,
	e *replicationEnds,
	contactPeer bool,
) (*relationship, error) {
	sources, err := e.local.ListSourceStatus(ctx)
	if err != nil {
		return nil, unreachableOr(e.localAddr, err, "%s: listing source relationships on %s", rpc, e.localAddr)
	}
	targets, err := e.local.ListTargetStatus(ctx)
	if err != nil {
		return nil, unreachableOr(e.localAddr, err, "%s: listing target relationships on %s", rpc, e.localAddr)
	}

	var candidates []relationship
	for i := range sources {
		if sources[i].SourceRootPath == e.realPath {
			candidates = append(candidates, relationship{id: sources[i].ID, role: roleSource, local: sources[i]})
		}
	}
	for i := range targets {
		if targets[i].TargetRootPath == e.realPath {
			candidates = append(candidates, relationship{id: targets[i].ID, role: roleTarget, local: targets[i]})
		}
	}
	// The peer is listed even when local has no candidate: an RPC that asked
	// for the peer needs to know it answers before acting, and Enable must
	// not create a relationship it then cannot authorize. For Enable, with
	// candidates, each is read from the peer by ID instead: the listing was
	// only ever searched for these IDs, and reading one answers whether the
	// peer does. The other RPCs keep the listing, which is what
	// promote's retries against an unanswering peer were measured on.
	if contactPeer && e.peer != nil {
		var peerSide map[string]peerStatus
		var err error
		if len(candidates) > 0 && peerReadsByID(ctx) {
			ids := make([]string, 0, len(candidates))
			for _, c := range candidates {
				ids = append(ids, c.id)
			}
			peerSide, err = r.peerStatusesByID(ctx, rpc, e, ids)
		} else {
			peerSide, err = r.peerStatuses(ctx, rpc, e)
		}
		if err != nil {
			return nil, err
		}
		var matched []relationship
		for i := range candidates {
			candidates[i].peerConsulted = true
			if ps, ok := peerSide[candidates[i].id]; ok {
				candidates[i].peer = &ps.status
				candidates[i].peerRole = ps.role
				matched = append(matched, candidates[i])
			}
		}
		// A match on the peer is what disambiguates; with none, every
		// local candidate stays, peer nil, so the caller can say "the peer
		// answered and has no end of this" rather than "no relationship".
		if len(matched) > 0 {
			candidates = matched
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	// An ENDED end is a corpse: the other array deleted its half. It never
	// disambiguates anything, so when a live relationship shares the path
	// the corpse is ignored. Measured on the lab cluster (2026-09-14): after
	// the DR side removed its target end during an outage and re-protected
	// the volume the other way once the array returned, the old primary
	// carried an ENDED source end beside the new live target end, and
	// Demote refused to guess between them.
	if len(candidates) > 1 {
		var live []relationship
		for _, c := range candidates {
			if c.local.State != qumulo.RelationshipEnded {
				live = append(live, c)
			}
		}
		if len(live) > 0 && len(live) < len(candidates) {
			slog.InfoContext(ctx, rpc+": ignoring ENDED relationship ends beside a live one",
				"volume", e.vol.Name, "path", e.realPath, "ended", len(candidates)-len(live))
			candidates = live
		}
	}

	switch len(candidates) {
	case 0:
		return nil, nil
	case 1:
		return &candidates[0], nil
	default:
		ids := make([]string, 0, len(candidates))
		for _, c := range candidates {
			ids = append(ids, c.id+" ("+c.role.String()+", "+c.local.State+")")
		}
		slog.WarnContext(ctx, rpc+": more than one relationship is rooted at the volume; refusing to guess",
			"volume", e.vol.Name, "path", e.realPath, "candidates", ids)
		return nil, status.Errorf(codes.Aborted,
			"%s: %d relationships are rooted at %q on %s and the peer could not disambiguate them: %s",
			rpc, len(candidates), e.realPath, e.localAddr, strings.Join(ids, ", "))
	}
}

// peerStatuses lists the peer's relationships rooted at peerPath, keyed by ID.
func (r *ReplicationServer) peerStatuses(ctx context.Context, rpc string, e *replicationEnds) (map[string]peerStatus, error) {
	out := map[string]peerStatus{}
	sources, err := e.peer.ListSourceStatus(ctx)
	if err != nil {
		return nil, unreachableOr(e.peerAddr, err, "%s: listing source relationships on peer %s", rpc, e.peerAddr)
	}
	for _, s := range sources {
		if s.SourceRootPath == e.peerPath {
			out[s.ID] = peerStatus{status: s, role: roleSource}
		}
	}
	targets, err := e.peer.ListTargetStatus(ctx)
	if err != nil {
		return nil, unreachableOr(e.peerAddr, err, "%s: listing target relationships on peer %s", rpc, e.peerAddr)
	}
	for _, t := range targets {
		if t.TargetRootPath == e.peerPath {
			out[t.ID] = peerStatus{status: t, role: roleTarget}
		}
	}
	return out, nil
}

// EnableVolumeReplication creates the relationship from local to the peer,
// authorizes it there, and pre-stages the replica's export and quota.
//
// It does not wait for the first sync: the controller polls
// GetVolumeReplicationInfo for that, and the volume is not protected until a
// recovery point exists.
func (r *ReplicationServer) EnableVolumeReplication(
	ctx context.Context,
	req *replication.EnableVolumeReplicationRequest,
) (*replication.EnableVolumeReplicationResponse, error) {
	const rpc = "enablevolumereplication"
	ctx = withPeerReadsByID(ctx)

	e, err := r.resolve(ctx, rpc, requestVolumeID(req), req.GetReplicationSource().GetVolumegroup().GetVolumeGroupId(),
		req.GetParameters(), req.GetSecrets(), false)
	if err != nil {
		return nil, err
	}

	// Under shared-subdirectory the only relationship possible is the
	// class-level one on basePath, and a per-PVC VolumeReplication on it
	// would let one tenant's promote freeze every other tenant's volume.
	if err := refuseSharedSubdirectory(rpc, e); err != nil {
		return nil, err
	}
	// The mirror image: a group whose root carries no covering export has
	// nothing to mirror, and the replica would replicate cleanly and then be
	// unmountable.
	if e.scope == scopeGroup && e.export == nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"%s: volume group %q on %s has no NFS export at its root, so its replica would replicate and "+
				"then have nothing to mount. A shared-subdirectory class's covering export is what makes it "+
				"a group; create it before enabling replication",
			rpc, e.realPath, e.localAddr)
	}

	rel, err := r.findRelationship(ctx, rpc, e, true)
	if err != nil && status.Code(err) == codes.Unavailable {
		// The peer did not answer. Measured on the lab cluster with the
		// source array powered off (2026-09-14): the csi-addons controller
		// calls Enable before every Promote, so an Enable that insists on
		// the peer blocks the unplanned failover it exists for. If this side
		// is already a replica, there is nothing to enable from here and
		// the peer is not needed to know that.
		local, lerr := r.findRelationship(ctx, rpc, e, false)
		if lerr == nil && local != nil && local.role == roleTarget && local.local.State != qumulo.RelationshipEnded {
			slog.WarnContext(ctx, rpc+": peer unreachable; this side is already a replication target, nothing to enable from here",
				"volume", e.vol.Name, "relationship", local.id, "state", local.local.State, "peer", e.peerAddr)
			return &replication.EnableVolumeReplicationResponse{}, nil
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	// A local end the configured peer holds nothing of is a class that
	// disagrees with the relationship. Staging onto it waited for a path that
	// never appears, Aborted on every retry.
	if err := refuseUnmatchedPeer(rpc, e, rel,
		"nothing is created, authorized or staged through a class that does not describe the relationship"); err != nil {
		return nil, err
	}

	// An ENDED relationship is the orphan an out-of-band delete leaves: one
	// end gone, the other still listed. Remove what remains on both arrays and
	// start over, rather than adopting a relationship that replicates nothing.
	if rel != nil && rel.local.State == qumulo.RelationshipEnded {
		slog.WarnContext(ctx, rpc+": removing an ENDED relationship before re-protecting",
			"volume", e.vol.Name, "relationship", rel.id, "endReason", rel.local.EndReason)
		if err := r.deleteBothEnds(ctx, rpc, e, rel); err != nil {
			return nil, err
		}
		rel = nil
	}

	var id string
	// protecting is set when this call creates or authorizes the
	// relationship: a Disable's unpromoted markers described the copy before
	// it, and go once it is staged.
	protecting := false
	switch {
	case rel == nil:
		// Nothing here. Before a relationship is created toward the peer,
		// make sure the directory it would replicate INTO is not somebody's
		// live copy: the promoted DR copy after this side's end was removed,
		// or a source of its own. allowNonEmptyTarget consents to
		// adopting a leftover read-only replica, never to overwriting a
		// writable one. FailedPrecondition: Enable has no force, and the
		// controller shows it as a validation failure with the remedy.
		if why, err := r.peerHoldsWritableCopyAt(ctx, rpc, e); err != nil {
			return nil, err
		} else if why != "" {
			return nil, status.Errorf(codes.FailedPrecondition,
				"%s: refusing to protect volume %q toward %s: %s. Protecting from here would adopt that copy as a replica "+
					"and discard its writes. If the peer's copy is the live one, set this VolumeReplication to secondary "+
					"and let resync rejoin it, or protect from the peer's side; if it is stale, remove its relationship end, "+
					"or set its export read-only, on %s first", rpc, e.vol.Name, e.peerAddr, why, e.peerAddr)
		}
		id, err = r.createAndAuthorize(ctx, rpc, e, e.params.AllowNonEmpty)
		if err != nil {
			return nil, err
		}
		protecting = true

	case rel.role == roleSource && rel.local.State == qumulo.RelationshipAwaitingAuthorization:
		id = rel.id
		protecting = true
		if err := r.authorize(ctx, rpc, e, id, e.params.AllowNonEmpty); err != nil {
			return nil, err
		}

	case rel.role == roleSource:
		// ESTABLISHED, a job running, or DISCONNECTED because the peer
		// promoted: the relationship exists and Info reports its health.
		id = rel.id
		if peerPromoted(rel) {
			// The peer's end is a writable promoted copy: the live one. The
			// prestage below would stage this side's quota and export onto
			// it, and this side is the stale primary. A refused
			// operation must not change the live copy, and Enable here has
			// nothing to do anyway.
			slog.WarnContext(ctx, rpc+": the peer holds the promoted copy; nothing to enable, and nothing is staged onto it",
				"volume", e.vol.Name, "relationship", id, "peer", e.peerAddr)
			return &replication.EnableVolumeReplicationResponse{}, nil
		}
		if rel.local.State == qumulo.RelationshipDisconnected {
			slog.WarnContext(ctx, rpc+": relationship is disconnected; nothing to enable, resync will reconnect it",
				"volume", e.vol.Name, "relationship", id)
		}
		// A relationship an older build created over a local-owned root
		// fails every job. The root's owner was read resolving it, so
		// re-owning it costs nothing unless it is local; the members of a
		// group are re-owned only when the relationship is created.
		if err := r.reownLocalRoot(ctx, rpc, e); err != nil {
			return nil, err
		}

	case rel.role == roleTarget && rel.local.State == qumulo.RelationshipDisconnected && !rel.local.TargetRootReadOnly:
		// A promoted ex-target: writable, replicating to nobody, unprotected.
		// Re-protecting it toward the class's peer tears both ends down and
		// adopts the peer's directory -- the old primary's data -- which the
		// first job then overwrites. Enable is otherwise non-destructive, so
		// that overwrite needs consent, and there are two forms of it: the
		// peer's source end read as frozen (the operator demoted the old
		// primary, so it is taking no writes that would be lost), or
		// the class saying allowNonEmptyTarget. Neither present, refuse. The
		// old primary comes back writable after an unplanned failover,
		// so without this check a re-created VolumeReplication on the
		// promoted side overwrote a copy still taking writes.
		// FailedPrecondition: Enable has no force, and the controller
		// returns it as a validation failure.
		peerFrozen := rel.peer != nil && rel.peerRole == roleSource && rel.peer.SourceRootReadOnly
		if !peerFrozen && !e.params.AllowNonEmpty {
			peerState := "not consulted"
			if rel.peer != nil {
				peerState = fmt.Sprintf("%s, source_root_read_only=%t", rel.peer.State, rel.peer.SourceRootReadOnly)
			}
			return nil, status.Errorf(codes.FailedPrecondition,
				"%s: volume %q on %s is a promoted ex-target of relationship %s, and re-protecting it would delete "+
					"that relationship and overwrite %q on peer %s with this copy. The peer's source end is %s, "+
					"so it may still be taking writes. Set the peer's VolumeReplication to secondary first (its "+
					"source end reads frozen), or set %s: \"true\" on the VolumeReplicationClass to consent to "+
					"the overwrite",
				rpc, e.vol.Name, e.localAddr, rel.id, e.peerPath, e.peerAddr, peerState, paramAllowNonEmptyTarget)
		}
		discarded := "unknown"
		if rel.peer != nil && rel.peer.HasRecoveryPoint() {
			discarded = rel.peer.RecoveryPoint.Format(time.RFC3339)
		}
		slog.WarnContext(ctx, rpc+": volume is a promoted ex-target; re-protecting it toward the class's peer, whose copy will be overwritten",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "peerPath", e.peerPath,
			"peerSourceFrozen", peerFrozen, "classAllowsNonEmpty", e.params.AllowNonEmpty,
			"peerRecoveryPointDiscarded", discarded)
		if err := r.deleteBothEnds(ctx, rpc, e, rel); err != nil {
			return nil, err
		}
		id, err = r.createAndAuthorize(ctx, rpc, e, true)
		if err != nil {
			return nil, err
		}
		// This copy is the source from here on. If it became writable
		// without a completed promote (one answered "retry" and this
		// Enable came first), its export is still the read-only replica
		// export: no later Promote row would flip it.
		if err := r.ensureOwnExportWritable(ctx, rpc, e); err != nil {
			return nil, err
		}

	case rel.role == roleTarget:
		// This side is already protected as a replica -- established, or
		// read-only and awaiting a reconnect after a reverse. Nothing to
		// pre-stage from here: the peer is the source and stages onto us.
		slog.InfoContext(ctx, rpc+": volume is already a replication target",
			"volume", e.vol.Name, "relationship", rel.id, "state", rel.local.State)
		return &replication.EnableVolumeReplicationResponse{}, nil

	default:
		return nil, uncoveredState(ctx, rpc, e, rel)
	}

	if err := r.prestage(ctx, rpc, e, id); err != nil {
		return nil, err
	}
	if protecting || e.unpromoted != nil {
		dropUnpromotedMarkersAfterProtect(ctx, rpc, e)
	}
	return &replication.EnableVolumeReplicationResponse{}, nil
}

// createAndAuthorize creates the relationship on local and authorizes it on
// the peer, adopting an existing one if a previous attempt got as far as
// creating it.
func (r *ReplicationServer) createAndAuthorize(ctx context.Context, rpc string, e *replicationEnds, allowNonEmpty bool) (string, error) {
	spec := qumulo.SourceSpec{
		SourceRootPath:     e.realPath,
		TargetRootPath:     e.peerPath,
		TargetAddress:      e.peerAddr,
		TargetPort:         qumulo.ReplicationPort,
		ReplicationMode:    e.params.arrayMode(),
		ReplicationEnabled: true,
	}
	if e.params.LinkSnapshotPolicy {
		link, err := r.policyLink(ctx, rpc, e.local, e.vol.Name)
		if err != nil {
			return "", unreachableOr(e.localAddr, err, "%s: listing snapshot policies on %s", rpc, e.localAddr)
		}
		if link == nil {
			slog.WarnContext(ctx, rpc+": class asks to link the volume's snapshot policy but the volume has none; "+
				"replicating data only", "volume", e.vol.Name, "policy", policyNameFor(e.vol.Name))
		} else {
			spec.SnapshotPolicies = []qumulo.SnapshotPolicyLink{*link}
		}
	}

	// Before the relationship exists, so its first job finds nothing it
	// must refuse.
	if err := r.reownLocalRoots(ctx, rpc, e); err != nil {
		return "", err
	}

	created, err := e.local.CreateSource(ctx, spec)
	switch {
	case err == nil:
	case qumulo.IsConflict(err):
		// A previous attempt created it and did not get to authorize. Adopt
		// rather than stack a second relationship on the same directory.
		rel, findErr := r.findRelationship(ctx, rpc, e, false)
		if findErr != nil {
			return "", findErr
		}
		if rel == nil {
			return "", status.Errorf(codes.Aborted,
				"%s: the array refused to create a relationship for %q as a duplicate, but none is listed; retrying",
				rpc, e.realPath)
		}
		created.ID = rel.id
	default:
		return "", unreachableOr(e.localAddr, err, "%s: creating the relationship for %q on %s", rpc, e.realPath, e.localAddr)
	}
	r.forgetNoRelationship(e)

	slog.InfoContext(ctx, rpc+": relationship created; authorizing on the peer",
		"volume", e.vol.Name, "relationship", created.ID, "source", e.realPath,
		"target", e.peerPath, "peer", e.peerAddr, "mode", spec.ReplicationMode)

	if err := r.authorize(ctx, rpc, e, created.ID, allowNonEmpty); err != nil {
		return "", err
	}
	return created.ID, nil
}

// authorize accepts the relationship on the peer, creating the target
// directory. That directory is what everything staged afterwards hangs off,
// and it exists only from here.
func (r *ReplicationServer) authorize(ctx context.Context, rpc string, e *replicationEnds, id string, allowNonEmpty bool) error {
	_, err := e.peer.Authorize(ctx, id, true, allowNonEmpty)
	switch {
	case err == nil:
		slog.InfoContext(ctx, rpc+": relationship authorized on the peer",
			"volume", e.vol.Name, "relationship", id, "peer", e.peerAddr, "target", e.peerPath)
		return nil
	case qumulo.IsNotFound(err):
		// The relationship has not propagated to the peer yet. Normal for a
		// moment after create; the controller's retry lands after it has.
		return status.Errorf(codes.Aborted,
			"%s: relationship %s is not yet visible on peer %s; retrying", rpc, id, e.peerAddr)
	case qumulo.IsAlreadyAuthorized(err):
		// The peer accepted this relationship on an earlier call and the
		// source end has not caught up to ESTABLISHED yet. Measured live on
		// Core 7.9.2.2: a repeat Enable a few seconds after the first one saw
		// the source still AWAITING_AUTHORIZATION and the peer answering HTTP
		// 400 relationship_manager_already_authorized_error. Nothing is left
		// to do on this step; pre-staging below is idempotent on its own.
		slog.InfoContext(ctx, rpc+": relationship was already authorized on the peer",
			"volume", e.vol.Name, "relationship", id, "peer", e.peerAddr)
		return nil
	case qumulo.ErrorClassOf(err) != "" && !isUnreachable(err):
		// The array refused, and the remedies are all the operator's: a
		// non-empty target directory, a missing privilege, an ancestor
		// export. Named as such rather than as a retriable Internal.
		return status.Errorf(codes.FailedPrecondition,
			"%s: peer %s refused to authorize relationship %s for %q: %v. If the directory already holds "+
				"data and overwriting it is intended, set %s: \"true\" on the VolumeReplicationClass",
			rpc, e.peerAddr, id, e.peerPath, err, paramAllowNonEmptyTarget)
	default:
		return unreachableOr(e.peerAddr, err, "%s: authorizing relationship %s on peer %s", rpc, id, e.peerAddr)
	}
}

// prestage mirrors the volume's export and quota onto the replica while the
// replica is still a read-only target. Both are accepted in that state
// (measured), and both survive replication and make-writable, so an
// unplanned failover needs nothing from the dead array.
//
// The export is created read-only and flipped at promote: a pod that
// reaches the replica early, through a stale resolver or a mis-bound PV, is
// refused at mount rather than having its appends dropped by a directory the
// filesystem will not let it write to.
//
// Idempotent: a present export or quota is compared, not recreated, and a
// quota that has drifted below the source's is brought back up. One above it
// is left alone (raiseQuota).
func (r *ReplicationServer) prestage(ctx context.Context, rpc string, e *replicationEnds, id string) error {
	replica, err := e.peer.LookupFileByPath(ctx, e.peerPath)
	if err != nil {
		if qumulo.IsNotFound(err) {
			return status.Errorf(codes.Aborted,
				"%s: relationship %s is authorized but %q does not exist on peer %s yet; retrying",
				rpc, id, e.peerPath, e.peerAddr)
		}
		return unreachableOr(e.peerAddr, err, "%s: looking up %q on peer %s", rpc, e.peerPath, e.peerAddr)
	}

	// Quota. For a group the limits live on the member volumes, not on the
	// group root, so the staging is one per member (1 relationship,
	// 1 covering export, N quotas).
	if e.scope == scopeGroup {
		if err := prestageMemberQuotas(ctx, rpc, e, id); err != nil {
			return err
		}
		return r.prestageExport(ctx, rpc, e, id, replica.ID)
	}

	// Quota: the source's limit, on the replica's own file ID.
	limit, err := e.local.GetQuota(ctx, e.fileID)
	switch {
	case err == nil:
		if err := raiseQuota(ctx, e.peer, replica.ID, limit); err != nil {
			return unreachableOr(e.peerAddr, err, "%s: staging the quota on peer %s", rpc, e.peerAddr)
		}
	case qumulo.IsNotFound(err):
		slog.WarnContext(ctx, rpc+": volume has no quota on the source; none staged on the replica",
			"volume", e.vol.Name, "relationship", id)
	default:
		return unreachableOr(e.localAddr, err, "%s: reading the quota of %q on %s", rpc, e.vol.Name, e.localAddr)
	}

	return r.prestageExport(ctx, rpc, e, id, replica.ID)
}

// prestageExport mirrors the source's export definition onto the replica.
//
// One export, whichever the scope: a dedicated-export volume's own, or the
// covering export of a whole shared-subdirectory class. Read-only either way,
// and flipped by promote.
func (r *ReplicationServer) prestageExport(ctx context.Context, rpc string, e *replicationEnds, id, replicaID string) error {
	if e.export != nil {
		if err := ensureReplicaExport(ctx, e.peer, *e.export, e.peerPath, e.vol.Name); err != nil {
			if qumulo.IsExportTreeConflict(err) {
				return status.Errorf(codes.FailedPrecondition,
					"%s: peer %s refuses an export at %q because an ancestor export exists and NFSv4 is enabled; "+
						"the peer's export layout must match the source's (runbook 0.4): %v",
					rpc, e.peerAddr, e.export.ExportPath, err)
			}
			return unreachableOr(e.peerAddr, err, "%s: staging the export on peer %s", rpc, e.peerAddr)
		}
		if err := fenceReplicaExport(ctx, rpc, e, id, replicaID); err != nil {
			return err
		}
	}

	slog.InfoContext(ctx, rpc+": replica pre-staged on the peer",
		"volume", e.vol.Name, "scope", e.scope.String(), "relationship", id, "peer", e.peerAddr,
		"replicaPath", e.peerPath, "replicaFileID", replicaID, "export", e.export != nil)
	return nil
}

// fenceReplicaExport sets the replica's export read-only when it is found
// read-write, if this driver made it, and otherwise refuses Enable with
// the remedy. ensureReplicaExport creates a missing export read-only
// but leaves an existing one as it is, and a group's covering export on the
// DR array is one the operator created, usually read-write. Left that way it
// broke "a read-write replica export marks a promoted copy", the evidence
// peerExportMarksPromotedCopy reads once both ends are gone: a plain Disable
// and re-Enable of a group that was never failed over was refused, and the
// Disable had already removed its protection.
//
// The fence is only ever applied over a read-only target end of this
// relationship, re-read here, so it can never fence a copy that is live: a
// promoted peer is turned away before prestage (Enable's peerPromoted row),
// and any other writable or ENDED end leaves the export alone for Info to
// report. A promote landing between that read and the write is undone by
// fenceIfStillReplica's read after it.
func fenceReplicaExport(ctx context.Context, rpc string, e *replicationEnds, id, replicaID string) error {
	peerExport, err := findExportByFSPath(ctx, e.peer, e.peerPath)
	if err != nil {
		return unreachableOr(e.peerAddr, err, "%s: reading the export of %q on peer %s", rpc, e.peerPath, e.peerAddr)
	}
	if peerExport == nil || peerExport.ReadOnly {
		return nil
	}
	fence, end, err := fenceIfStillReplica(ctx, rpc, e, id, peerExport, func(qumulo.RelationshipStatus) (bool, error) {
		return mayFenceAtEnable(ctx, rpc, e, id, replicaID, peerExport)
	})
	switch fence {
	case fenceUnread:
		if qumulo.IsNotFound(err) {
			slog.WarnContext(ctx, rpc+": replica export is read-write and the peer does not list the target end yet; left for a retry",
				"volume", e.vol.Name, "relationship", id, "peer", e.peerAddr, "export", peerExport.ExportPath)
			return nil
		}
		return unreachableOr(e.peerAddr, err, "%s: reading the target end of relationship %s on peer %s", rpc, id, e.peerAddr)
	case fenceNotReplica:
		slog.WarnContext(ctx, rpc+": replica export is read-write over a target end that is not a read-only replica; left as it is",
			"volume", e.vol.Name, "relationship", id, "peer", e.peerAddr, "export", peerExport.ExportPath,
			"state", end.State, "targetRootReadOnly", end.TargetRootReadOnly)
		return nil
	case fenceDeclined:
		return err
	case fenceWriteFailed:
		return unreachableOr(e.peerAddr, err, "%s: setting the replica export %q read-only on peer %s",
			rpc, peerExport.ExportPath, e.peerAddr)
	case fenceKept:
		slog.WarnContext(ctx, rpc+": replica export was read-write; set read-only, as a replica's export is until a promote",
			"volume", e.vol.Name, "scope", e.scope.String(), "relationship", id, "peer", e.peerAddr,
			"export", peerExport.ExportPath, "exportID", peerExport.ID)
	}
	// fenceUndone and fenceUndoFailed: a promote landed meanwhile, logged by
	// fenceIfStillReplica; the copy is live, and nothing here is refused.
	return nil
}

// mayFenceAtEnable is fenceReplicaExport's check between its read of a
// read-only target end and its write: only an export this driver made is
// fenced. One an operator made -- a group's covering export above all -- may
// be mounted read-write by something else, and this driver does not take
// write access away from an export it does not own. It is refused
// instead, naming the one change that fixes it. That is also what keeps a
// later plain Disable and re-Enable working: once the export is read-only, a
// read-write one again means a promote. Disable's unpromoted marker
// covers an export left read-write (replication_unpromoted.go).
func mayFenceAtEnable(ctx context.Context, rpc string, e *replicationEnds, id, replicaID string, peerExport *qumulo.ExportInfo) (bool, error) {
	if madeByThisDriver(peerExport) {
		return true, nil
	}
	if e.unpromoted == nil {
		// A retry of an Enable that re-protected over this export (an
		// Aborted after the create) finds the marker it has not removed yet.
		e.unpromoted = markedUnpromoted(ctx, e, replicaID, peerExport)
	}
	if e.unpromoted != nil {
		// This is the re-protect of a copy unchanged since Disable marked
		// it, which has warned that the export is read-write. The
		// relationship exists and its target end is read-only, so refusing
		// now would protect nothing; Info goes on reporting the export.
		slog.InfoContext(ctx, rpc+": replica export is read-write and was not made by this driver; left as it is over a copy re-protected since Disable",
			"volume", e.vol.Name, "scope", e.scope.String(), "relationship", id, "peer", e.peerAddr,
			"export", peerExport.ExportPath, "exportID", peerExport.ID, "marker", e.unpromoted.marker.ID)
		return false, nil
	}
	slog.WarnContext(ctx, rpc+": replica export is read-write and was not made by this driver; not changing it",
		"volume", e.vol.Name, "scope", e.scope.String(), "relationship", id, "peer", e.peerAddr,
		"export", peerExport.ExportPath, "exportID", peerExport.ID, "description", peerExport.Description)
	return false, status.Errorf(codes.FailedPrecondition,
		"%s: the replica's export %q on peer %s is read-write, and it was not made by this driver "+
			"(description %q), so this driver will not change it. Replication keeps a replica's export "+
			"read-only until a promote, and a read-write one is how a promoted copy is recognised once "+
			"replication is disabled. Set that export read-only on %s and retry; relationship %s is "+
			"in place and replicating meanwhile",
		rpc, peerExport.ExportPath, e.peerAddr, peerExport.Description, e.peerAddr, id)
}

// ensureQuota makes fileID carry exactly limit, creating or updating as needed.
func ensureQuota(ctx context.Context, client qumulo.ClientInterface, fileID string, limit uint64) error {
	current, err := client.GetQuota(ctx, fileID)
	switch {
	case err == nil && current == limit:
		return nil
	case err == nil:
		return client.UpdateQuota(ctx, fileID, limit)
	case qumulo.IsNotFound(err):
		if err := client.CreateQuota(ctx, fileID, limit); err != nil && !qumulo.IsConflict(err) {
			return err
		}
		return nil
	default:
		return err
	}
}

// raiseQuota is ensureQuota for a replica: it creates a missing quota and
// raises a smaller one, but never lowers one.
//
// A replica's limit is staged from the source's, and replication ignores it,
// so a replica larger than its source costs nothing. A smaller one is the
// hazard: after a promote the workload's first write past it fails ENOSPC.
// And the replica may not be one: a promoted DR copy grown while this side
// was unreachable is larger than the old primary, and re-enabling or
// expanding the old primary used to set it back down to the old primary's
// limit -- lowering the quota on the live copy.
func raiseQuota(ctx context.Context, client qumulo.ClientInterface, fileID string, limit uint64) error {
	current, err := client.GetQuota(ctx, fileID)
	switch {
	case err == nil && current >= limit:
		return nil
	case err == nil:
		return client.UpdateQuota(ctx, fileID, limit)
	case qumulo.IsNotFound(err):
		if err := client.CreateQuota(ctx, fileID, limit); err != nil && !qumulo.IsConflict(err) {
			return err
		}
		return nil
	default:
		return err
	}
}

// ensureReplicaExport creates the replica's export from the source's
// definition if it is absent. An existing export at the path is left as it
// is, whatever its read-only state: promote is what flips it.
func ensureReplicaExport(ctx context.Context, client qumulo.ClientInterface, source qumulo.ExportInfo, fsPath, name string) error {
	// A report must not go on answering "no export here" from before this.
	defer forgetNoExport(client, fsPath)
	exports, err := client.ListExports(ctx)
	if err != nil {
		return err
	}
	for _, existing := range exports {
		if existing.ExportPath == source.ExportPath {
			return nil
		}
	}
	created, err := client.CreateExport(ctx, qumulo.ExportSpec{
		ExportPath:         source.ExportPath,
		FSPath:             fsPath,
		Description:        exportDescriptionFor(name),
		TenantID:           source.TenantID,
		AuthenticationMode: source.AuthenticationMode,
		AllowedClients:     source.AllowedClients,
		ReadOnly:           true,
	})
	if err == nil {
		rememberExport(client, fsPath, created.ID)
	}
	if err != nil && !qumulo.IsConflict(err) {
		return err
	}
	return nil
}

// DisableVolumeReplication removes both ends of the relationship. It deletes
// no data: the replica directory, its export and its quota stay on the peer.
func (r *ReplicationServer) DisableVolumeReplication(
	ctx context.Context,
	req *replication.DisableVolumeReplicationRequest,
) (*replication.DisableVolumeReplicationResponse, error) {
	const rpc = "disablevolumereplication"

	e, err := r.resolve(ctx, rpc, requestVolumeID(req), req.GetReplicationSource().GetVolumegroup().GetVolumeGroupId(),
		req.GetParameters(), req.GetSecrets(), false)
	if err != nil {
		// A volume that no longer resolves has nothing left to disable.
		if status.Code(err) == codes.NotFound {
			slog.WarnContext(ctx, rpc+": volume does not resolve; nothing to disable", "error", err)
			return &replication.DisableVolumeReplicationResponse{}, nil
		}
		return nil, err
	}

	rel, err := r.findRelationship(ctx, rpc, e, true)
	if err != nil && status.Code(err) == codes.Unavailable {
		// The peer did not answer. Measured on the lab cluster with the
		// source array powered off (2026-09-14): a DR-side VolumeReplication
		// could not be deleted, because the csi-addons controller runs
		// Disable from its finalizer and this RPC insisted on the peer. A
		// replica end can be removed without its source; the source end
		// reads ENDED when that array returns and Enable's ENDED handling
		// clears it. A SOURCE end with the peer down stays Unavailable:
		// deleting it would orphan a replica nobody can reach to clean up.
		local, lerr := r.findRelationship(ctx, rpc, e, false)
		if lerr == nil && local != nil && local.role == roleTarget {
			if derr := e.local.DeleteTarget(ctx, local.id); derr != nil && !qumulo.IsNotFound(derr) {
				return nil, unreachableOr(e.localAddr, derr, "%s: deleting the target end of relationship %s on %s", rpc, local.id, e.localAddr)
			}
			slog.WarnContext(ctx, rpc+": peer unreachable; removed this side's target end only. The peer's source end will read ENDED when it returns",
				"volume", e.vol.Name, "relationship", local.id, "peer", e.peerAddr)
			// The peer cannot be reached; this side's are stamped.
			r.expireRevertSnapshots(ctx, rpc, e.local, e.localAddr, localRoot(e, local))
			return &replication.DisableVolumeReplicationResponse{}, nil
		}
		if lerr == nil && local == nil {
			return &replication.DisableVolumeReplicationResponse{}, nil
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if rel == nil {
		return &replication.DisableVolumeReplicationResponse{}, nil
	}
	// With the class naming the wrong peer, the peer delete below goes
	// to an array that holds nothing (NotFound, read as done) and this side's
	// end alone is removed -- OK answered, the real peer's end orphaned. The
	// relationship is left whole for a corrected class to remove.
	if err := refuseUnmatchedPeer(rpc, e, rel,
		"disable is refused because it would remove only this side's end and leave the real peer's end behind; "+
			"correct the class and the next attempt removes both"); err != nil {
		return nil, err
	}
	if rel.role == roleSource && rel.peer != nil && rel.peerRole == roleTarget && !rel.peer.TargetRootReadOnly {
		// The peer's copy is writable: it was promoted (planned or forced)
		// and this side is the old primary, back or never gone. Its end is
		// the only record on the peer that the copy there is the live one.
		// Deleting it and then re-protecting from here -- which is what an
		// operator does who deletes and recreates this object -- rebuilt the
		// relationship onto the promoted copy with allowNonEmptyTarget and
		// adopted it as a replica, discarding every write made there since
		// the promote (measured on the lab cluster). So
		// only this side's end is removed; the peer's end reads ENDED and
		// its copy stays writable, and Enable from here refuses to overwrite
		// it (peerHoldsWritableCopyAt).
		if derr := e.local.DeleteSource(ctx, rel.id); derr != nil && !qumulo.IsNotFound(derr) {
			return nil, unreachableOr(e.localAddr, derr, "%s: deleting the source end of relationship %s on %s", rpc, rel.id, e.localAddr)
		}
		slog.WarnContext(ctx, rpc+": the peer holds the writable copy; removed this side's source end only. "+
			"The peer's end reads ENDED and its copy stays writable; re-protect from the peer side",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "peerState", rel.peer.State)
		r.expireRevertSnapshotsOnBothEnds(ctx, rpc, e, rel)
		return &replication.DisableVolumeReplicationResponse{}, nil
	}
	if err := r.unfreezeBeforeDisable(ctx, rpc, e, rel); err != nil {
		return nil, err
	}
	fenceReplicaExportBeforeDisable(ctx, rpc, e, rel)
	r.markUnpromotedBeforeDisable(ctx, rpc, e, rel)
	if err := r.deleteBothEnds(ctx, rpc, e, rel); err != nil {
		return nil, err
	}
	// The last call for this relationship: a revert snapshot that appeared
	// after the last resync poll is given its expiration here.
	r.expireRevertSnapshotsOnBothEnds(ctx, rpc, e, rel)
	return &replication.DisableVolumeReplicationResponse{}, nil
}

// fenceReplicaExportBeforeDisable sets the replica's export read-only before
// the relationship is deleted, when this driver made it and the peer's end is
// a live, read-only replica -- the one moment that is still known.
//
// Once both ends are gone, a read-write export over data is the only evidence
// Enable has of a promote (peerExportMarksPromotedCopy), and it cannot tell
// a promoted copy from an export left read-write. A plain Disable of a pair
// whose replica export was read-write -- made by an earlier build, or changed
// by hand -- therefore left it refused on every re-Enable, unprotected.
// Here the relationship still says what the copy is: a read-only target root
// takes no writes whatever its export allows, so fencing the export records
// the truth at the last point anything does. A copy written after this --
// which needs the export made read-write again -- is refused at Enable as
// before.
//
// Never blocking. The csi-addons controller runs Disable from the
// VolumeReplication's finalizer, so a refusal here would hold that object,
// and a namespace or claim being torn down with it, behind an export setting
// (a new failure, worse than the one this prevents). Any failure leaves
// Disable exactly as it was: logged, and the export left for the operator.
// An operator's export -- a group's covering export -- is never changed;
// the WARN names it and the one change a later re-Enable needs.
func fenceReplicaExportBeforeDisable(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) {
	if e.export == nil || rel.role != roleSource || rel.peerRole != roleTarget || rel.peer == nil ||
		!rel.peer.TargetRootReadOnly || rel.peer.State == qumulo.RelationshipEnded {
		return
	}
	peerExport, err := findExportByFSPath(ctx, e.peer, e.peerPath)
	if err != nil {
		slog.WarnContext(ctx, rpc+": could not read the replica's export before disabling; not checked",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "error", err)
		return
	}
	if peerExport == nil || peerExport.ReadOnly {
		return
	}
	if !madeByThisDriver(peerExport) {
		slog.WarnContext(ctx, rpc+": the replica's export is read-write and not this driver's, so it is left as it is. "+
			"Once this relationship is gone, a re-enable reads that export as a promoted copy and is refused until "+
			"it is set read-only on the peer; set it read-only before re-enabling",
			"volume", e.vol.Name, "scope", e.scope.String(), "relationship", rel.id, "peer", e.peerAddr,
			"export", peerExport.ExportPath, "exportID", peerExport.ID, "description", peerExport.Description)
		return
	}
	// The end is read again: the relationship above may be a few round trips
	// old, and a promote since then would have made the export read-write on
	// purpose -- as would one landing during the write.
	fence, end, err := fenceIfStillReplica(ctx, rpc, e, rel.id, peerExport, nil)
	switch fence {
	case fenceUnread, fenceNotReplica:
		slog.WarnContext(ctx, rpc+": the replica's export is read-write and its target end could not be confirmed a read-only replica; left as it is",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "export", peerExport.ExportPath,
			"state", end.State, "targetRootReadOnly", end.TargetRootReadOnly, "error", err)
	case fenceWriteFailed:
		slog.WarnContext(ctx, rpc+": could not set the replica's read-write export read-only before disabling; "+
			"a re-enable is refused until it is set read-only on the peer",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "export", peerExport.ExportPath, "error", err)
	case fenceKept:
		slog.WarnContext(ctx, rpc+": replica export was read-write; set read-only before disabling, as a replica's export is until a promote",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "export", peerExport.ExportPath, "exportID", peerExport.ID)
	}
}

// peerHoldsWritableCopyAt reports, as a sentence or "", whether the peer
// array lists an end at the path this class would replicate into whose
// directory is writable: a target end made writable by a promote (live and
// DISCONNECTED, or ENDED after the old primary's end was removed), or a live
// source end that is not frozen. Either means the data there is somebody's
// live copy, and creating a relationship from here with allowNonEmptyTarget
// would adopt it as a replica and overwrite it.
func (r *ReplicationServer) peerHoldsWritableCopyAt(ctx context.Context, rpc string, e *replicationEnds) (string, error) {
	if e.peer == nil || e.peerPath == "" {
		return "", nil
	}
	targets, err := e.peer.ListTargetStatus(ctx)
	if err != nil {
		return "", unreachableOr(e.peerAddr, err, "%s: listing target relationships on peer %s", rpc, e.peerAddr)
	}
	liveReplica := false
	for i := range targets {
		if targets[i].TargetRootPath != e.peerPath {
			continue
		}
		if !targets[i].TargetRootReadOnly {
			return fmt.Sprintf("the target end of relationship %s at %q on %s is writable (%s): that copy was promoted",
				targets[i].ID, e.peerPath, e.peerAddr, targets[i].State), nil
		}
		liveReplica = liveReplica || targets[i].State != qumulo.RelationshipEnded
	}
	sources, err := e.peer.ListSourceStatus(ctx)
	if err != nil {
		return "", unreachableOr(e.peerAddr, err, "%s: listing source relationships on peer %s", rpc, e.peerAddr)
	}
	for i := range sources {
		if sources[i].SourceRootPath == e.peerPath && sources[i].State != qumulo.RelationshipEnded && !sources[i].SourceRootReadOnly {
			return fmt.Sprintf("relationship %s on %s replicates FROM %q (%s): that copy is a live source",
				sources[i].ID, e.peerAddr, e.peerPath, sources[i].State), nil
		}
	}
	if liveReplica {
		// A live read-only target end says what the copy is, whatever its
		// export says -- including a replica made before resync fenced
		// exports read-only (reverseAndReconnect).
		return "", nil
	}
	return peerExportMarksPromotedCopy(ctx, rpc, e)
}

// peerExportMarksPromotedCopy is peerHoldsWritableCopyAt's last check, for a
// peer that lists no end at all: Disable on a promoted copy that had been
// re-protected removes both ends, and a force-promoted copy whose own
// end was removed during an outage leaves nothing either once Enable clears
// the ENDED end on this side. The relationship record is gone, but the
// export is not: prestage creates the replica's export read-only, only
// promote's replumb flips it read-write, and Disable only ever sets this
// driver's own read-only over a read-only target end. A
// read-write export over a non-empty directory therefore marks a copy that
// was promoted and may hold writes this side never saw -- unless the copy is
// unchanged since the Disable that snapshotted it as an unpromoted replica
// (replication_unpromoted.go): then nothing on it can be lost, and
// Enable re-protects it with one warning instead.
//
// Every scope Enable accepts has an export to read: a dedicated-export
// volume's own, or a group's covering export (a per-volume relationship on a
// shared-subdirectory volume is refused before this, and so is a group with no
// covering export). The nil check is for callers that have not been through
// those refusals.
func peerExportMarksPromotedCopy(ctx context.Context, rpc string, e *replicationEnds) (string, error) {
	if e.export == nil {
		return "", nil
	}
	peerExport, err := findExportByFSPath(ctx, e.peer, e.peerPath)
	if err != nil {
		return "", unreachableOr(e.peerAddr, err, "%s: reading the export of %q on peer %s", rpc, e.peerPath, e.peerAddr)
	}
	if peerExport == nil || peerExport.ReadOnly {
		return "", nil
	}
	entries, err := e.peer.ListDirectory(ctx, e.peerPath, "")
	switch {
	case qumulo.IsNotFound(err):
		return "", nil
	case err != nil:
		return "", unreachableOr(e.peerAddr, err, "%s: listing %q on peer %s", rpc, e.peerPath, e.peerAddr)
	case len(entries) == 0:
		return "", nil
	}
	// A copy unchanged since the Disable that marked it unpromoted is not
	// one a promote let anybody write to: re-protecting it loses nothing.
	proof, why := unchangedSinceDisable(ctx, rpc, e)
	if proof != nil {
		proof.export = *peerExport
		e.unpromoted = proof
		warnReprotectOverReadWriteExport(ctx, rpc, e, proof)
		return "", nil
	}
	return fmt.Sprintf("no relationship end remains at %q on %s, but its export %q is read-write over %d entries, and %s. "+
		"If this copy was never failed over: set export %s read-only on %s, then delete and re-create this "+
		"VolumeReplication. If it was promoted -- replication keeps that export read-only and only a promote "+
		"makes it writable -- it may hold writes this side never saw",
		e.peerPath, e.peerAddr, peerExport.ExportPath, len(entries), why, peerExport.ExportPath, e.peerAddr), nil
}

// unfreezeBeforeDisable lifts the freeze on a source whose relationship is
// about to be deleted, when that is safe.
//
// Whether DELETE on a source relationship lifts source_root_read_only is not
// measured. A Disable after an abandoned planned promote could
// therefore leave the directory read-only for good, dropping appends
// silently with no relationship left to unfreeze through. So the
// freeze is lifted here first, but only when the peer's end has been read
// and is a read-only target -- nobody else holds a writable copy, and the
// relationship is being torn down anyway. With the peer's end writable the
// freeze stays: lifting it would make the second writable copy, and that is
// the operator's call, not Disable's. If the DELETE does lift the freeze on
// its own, the PATCH before it was harmless.
func (r *ReplicationServer) unfreezeBeforeDisable(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) error {
	if rel.role != roleSource || !rel.local.SourceRootReadOnly || rel.local.State == qumulo.RelationshipEnded {
		return nil
	}
	if rel.peer == nil || rel.peerRole != roleTarget || !rel.peer.TargetRootReadOnly {
		slog.WarnContext(ctx, rpc+": disabling a FROZEN source whose peer holds a writable copy (or was not read); "+
			"the freeze is kept so this does not become a second writable copy. Whether the delete lifts it is "+
			"not measured: if the directory stays read-only, resync this side or re-enable and promote it",
			"volume", e.vol.Name, "relationship", rel.id, "peerRead", rel.peer != nil)
		return nil
	}
	unfrozen := false
	if _, err := e.local.PatchSource(ctx, rel.id, qumulo.SourcePatch{SourceRootReadOnly: &unfrozen}); err != nil {
		return unreachableOr(e.localAddr, err, "%s: unfreezing source relationship %s on %s before disabling it", rpc, rel.id, e.localAddr)
	}
	r.dropFreezeMarker(ctx, rpc, e.local, rel.id, rel.local.SourceRootID)
	slog.WarnContext(ctx, rpc+": unfroze the source before disabling its relationship (an abandoned planned promote had left it read-only)",
		"volume", e.vol.Name, "relationship", rel.id)
	return nil
}

// deleteBothEnds removes the relationship on local and on the peer, treating
// an end that is already gone as done.
//
// The peer end is attempted even when the peer's view was not fetched, and a
// peer that cannot be reached is Unavailable rather than ignored: an orphaned
// end on one array is exactly the ENDED state Enable then has to clean up.
func (r *ReplicationServer) deleteBothEnds(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) error {
	localDelete, peerDelete := e.local.DeleteSource, e.peer.DeleteTarget
	if rel.role == roleTarget {
		localDelete, peerDelete = e.local.DeleteTarget, e.peer.DeleteSource
	}
	if err := localDelete(ctx, rel.id); err != nil && !qumulo.IsNotFound(err) {
		return unreachableOr(e.localAddr, err, "%s: deleting the %s end of relationship %s on %s", rpc, rel.role, rel.id, e.localAddr)
	}
	if err := peerDelete(ctx, rel.id); err != nil && !qumulo.IsNotFound(err) {
		return unreachableOr(e.peerAddr, err, "%s: deleting the peer end of relationship %s on %s", rpc, rel.id, e.peerAddr)
	}
	slog.InfoContext(ctx, rpc+": relationship removed on both arrays",
		"volume", e.vol.Name, "relationship", rel.id, "localRole", rel.role.String())
	return nil
}

// PromoteVolume makes the local copy the writable one.
//
// On a target it is the failover: planned (force=false) freezes the source,
// drains it against the source array's own clock, checks the policy
// snapshots crossed, then makes the target writable. On a source it is
// either a no-op (already primary), the recovery of an abandoned planned
// promote (unfreeze), or a refusal because the peer is primary. Every row is
// idempotent: the state check in front of make-writable is what makes a
// retry safe, since the array refuses a second make-writable outright.
//
// # The one rule about FailedPrecondition
//
// The csi-addons controller (v0.15.0, measured) answers a FailedPrecondition
// from this RPC by calling it again at once with force=true, and treats every
// other code as "retry later with backoff". So FailedPrecondition leaves this
// function for exactly one condition: the local copy is a read-only
// ESTABLISHED target and the peer array cannot be reached, which is the
// unplanned failover, where a forced promote is the intended remedy. Every
// other refusal -- the peer answered but has no end of the relationship at
// the class's path, the peer holds the writable copy, this side is a
// disconnected source, a frozen source whose peer is writable, an ambiguous
// table -- uses a code the controller does not force on: InvalidArgument for
// a misconfigured class, Aborted for a state the operator has to change, and
// the message names the remedy.
//
// force therefore means "proceed if the peer cannot be reached", never "do
// not look": the peer is always asked for, and only a transport failure lets
// the RPC decide from this side alone. With the peer answering, a forced
// promote of a read-only target drains exactly as a planned one does, and a
// forced promote of a source never unfreezes it or reports it primary while
// the peer's copy is writable.
func (r *ReplicationServer) PromoteVolume(
	ctx context.Context,
	req *replication.PromoteVolumeRequest,
) (*replication.PromoteVolumeResponse, error) {
	const rpc = "promotevolume"
	force := req.GetForce()

	// One budget for the whole promote, shared by the drain and the wait for
	// the target to become writable. They used to arm a fresh drainBudget
	// each, so a slow pair could spend twice it inside one RPC -- past the
	// sidecar timeout the budget's comment says it stays well inside, which
	// turns a deliberate Aborted into a context deadline the caller cannot
	// tell from a hang.
	ctx = r.withPromoteDeadline(ctx)

	e, err := r.resolve(ctx, rpc, requestVolumeID(req), req.GetReplicationSource().GetVolumegroup().GetVolumeGroupId(),
		req.GetParameters(), req.GetSecrets(), false)
	if err != nil {
		return nil, err
	}

	// No shared-subdirectory refusal here, deliberately: see
	// refuseSharedSubdirectory. A missing local export means "this volume was
	// provisioned under a shared class" only when local is the intended
	// source, which is Enable. Here local may be a replica whose pre-staged
	// export drifted or was never created -- exactly the gap replumb exists
	// to fill -- and refusing that would make an unplanned failover
	// unrecoverable. A per-PVC promote on a genuinely shared volume finds no
	// relationship rooted at its directory and is refused below.

	// The peer is asked for whether or not force is set. Only a transport
	// failure -- the peer never answered -- falls back to this side's view,
	// and the fallback leaves peerConsulted false so every row below can
	// tell "unreachable" from "answered, no match". Measured with the source
	// array powered off, 2026-09-14: the fallback is what lets the planned
	// path tell the controller (FailedPrecondition) that only a forced
	// promote is possible.
	rel, err := r.consultPeerForPromote(ctx, rpc, e)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		// Aborted, not FailedPrecondition: nothing here is a reason to
		// force. The controller calls Enable before every Promote, so the
		// usual cause is a relationship that was just created and is not
		// listed yet, and a retry finds it.
		return nil, status.Errorf(codes.Aborted,
			"%s: volume %q has no replication relationship on %s; enable replication first, retrying",
			rpc, e.vol.Name, e.localAddr)
	}
	if rel.peer == nil && rel.peerConsulted {
		// The peer answered and holds no end of this relationship at the
		// path the class implies. That is a class that disagrees with the
		// relationship -- a mistyped targetBasePath, a targetServer naming a
		// third array -- and not a dead array, so nothing below may run:
		// forcing past it made two writable copies with nothing frozen and
		// nothing drained (reproduced on the fabric).
		return nil, unmatchedPeerError(rpc, e, rel, "a forced promote is refused because the peer is not unreachable")
	}

	switch {
	case rel.role == roleSource && rel.local.State == qumulo.RelationshipDisconnected && peerIsReadOnlyEstablishedTarget(rel):
		// After a reconnect the source end keeps saying DISCONNECTED
		// for a few seconds while the target end already says ESTABLISHED
		// and read-only. Nothing on the peer is writable, so this is not
		// the peer-promoted refusal below; it is a retry, after which the
		// ends agree and this side is the writable source.
		return nil, status.Errorf(codes.Aborted,
			"%s: the source end of relationship %s still reports DISCONNECTED while the peer's target end is "+
				"ESTABLISHED and read-only (the ends catch up a few seconds after a reconnect); retrying",
			rpc, rel.id)

	case rel.role == roleSource && (rel.local.State == qumulo.RelationshipDisconnected || peerPromoted(rel)):
		// The peer promoted, whether this end already says DISCONNECTED or
		// still says ESTABLISHED (measured: it does until the reverse). Only
		// a resync can bring this side back, as a target. Never OK, never
		// FailedPrecondition (which the controller would answer with force);
		// Aborted, so the object shows the error until the operator acts.
		// Checked before the "already writable" row so two writable copies
		// are never reported as success.
		return nil, status.Errorf(codes.Aborted,
			"%s: volume %q on %s is a disconnected source and its peer %s holds the writable copy; "+
				"set this VolumeReplication to secondary and resync rather than promoting it",
			rpc, e.vol.Name, e.localAddr, e.peerAddr)

	case rel.role == roleSource && !rel.local.SourceRootReadOnly:
		if rel.peer == nil {
			// A writable source whose peer cannot be asked whether it
			// promoted. After an unplanned failover the old primary comes
			// back exactly like this, and its own end keeps
			// saying ESTABLISHED, so this side's state proves
			// nothing. Not OK until the peer answers.
			return nil, status.Errorf(codes.Unavailable,
				"%s: volume %q on %s is a writable source but peer %s could not be consulted about whether it "+
					"holds a writable copy too; not reporting primary until it answers",
				rpc, e.vol.Name, e.localAddr, e.peerAddr)
		}
		if err := r.ensureOwnExportWritable(ctx, rpc, e); err != nil {
			return nil, err
		}
		slog.InfoContext(ctx, rpc+": volume is already the writable source", "volume", e.vol.Name, "relationship", rel.id)
		return &replication.PromoteVolumeResponse{}, nil

	case rel.role == roleSource && rel.local.SourceRootReadOnly:
		return r.unfreezeAbandonedPromote(ctx, rpc, e, rel)

	case rel.role == roleTarget && rel.local.State == qumulo.RelationshipDisconnected && !rel.local.TargetRootReadOnly:
		// Already promoted. Re-plumb in case an earlier attempt stopped after
		// make-writable, and do NOT call make-writable again. On the
		// same kind of context as the promote itself.
		ctx, cancel := uncancelledUntilDeadline(ctx, promoteFinishBudget)
		defer cancel()
		defer r.promotes.begin()()
		slog.InfoContext(ctx, rpc+": target is already writable; verifying export and quota",
			"volume", e.vol.Name, "relationship", rel.id, "recoveryPoint", rel.local.RecoveryPoint)
		if err := r.replumb(ctx, rpc, e, rel); err != nil {
			return nil, err
		}
		// An earlier attempt may have stopped, or been refused, before it
		// expired the revert snapshot its make-writable left.
		r.expireRevertSnapshots(ctx, rpc, e.local, e.localAddr, revertRoot(e, rel))
		return &replication.PromoteVolumeResponse{}, nil

	case rel.role == roleTarget && rel.local.State == qumulo.RelationshipEstablished && rel.local.TargetRootReadOnly:
		// From the freeze to the re-plumb, nothing may stop halfway: stopped
		// after the freeze, both copies are read-only; after make-writable,
		// the copy is writable but its export is not. A controller shutdown
		// cancels this RPC's context at once, so the section runs on
		// one that ignores cancellation but keeps the caller's deadline --
		// the work never outlives what csi-addons waits for.
		ctx, cancel := uncancelledUntilDeadline(ctx, r.drainBudget+promoteFinishBudget)
		defer cancel()
		defer r.promotes.beginFor(rel.id)()
		switch {
		case rel.peer != nil:
			// The source answers, so it is frozen and drained -- with or
			// without force. A force that arrives while the source is up
			// (the controller re-calling after an outage that just ended,
			// or a hand-written client) must not skip the freeze: an
			// undrained promote of a live source loses every write since
			// the recovery point and then the resync discards them.
			if force {
				slog.WarnContext(ctx, rpc+": forced promote, but the source answers; freezing and draining it rather than skipping the final sync",
					"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr)
			}
			if err := r.drainSource(ctx, rpc, e, rel); err != nil {
				return nil, err
			}
		case !force:
			// The one FailedPrecondition. Measured on the lab cluster with
			// C1 powered off (2026-09-14): as Unavailable the controller
			// retried forever and never promoted; as FailedPrecondition it
			// re-called with force and promoted 24 s after the objects
			// were created.
			return nil, status.Errorf(codes.FailedPrecondition,
				"%s: peer %s did not answer; a planned promote needs the source. Retry with force to promote "+
					"without a final sync", rpc, e.peerAddr)
		default:
			slog.WarnContext(ctx, rpc+": forced promote with the source unreachable; skipping the final sync. The recovery point is the achieved RPO",
				"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "recoveryPoint", rel.local.RecoveryPoint)
		}
		if err := r.makeWritable(ctx, rpc, e, rel); err != nil {
			return nil, err
		}
		if rel.peer != nil {
			// A demote's marker stays: if this promote is undone, the DR
			// side's resync must still see that the source was frozen by the
			// operator, not by this promote. It expires on its own.
			r.dropPromoteFreezeMarker(ctx, rpc, e.peer, rel.id, rel.peer.SourceRootID)
		}
		if err := r.replumb(ctx, rpc, e, rel); err != nil {
			return nil, err
		}
		return &replication.PromoteVolumeResponse{}, nil

	default:
		return nil, uncoveredState(ctx, rpc, e, rel)
	}
}

// consultPeerForPromote is PromoteVolume's lookup: the peer-aware one, asked
// again before a transport failure is taken to mean the peer is gone.
//
// The fallback to this side's view is what turns a promote into a forced
// one: with the peer not consulted, a planned promote answers
// FailedPrecondition, csi-addons re-calls at once with force, and the forced
// row skips the freeze and the final sync. One failed listing on each of the
// two calls used to be enough -- a 10 s network blip in the middle of a
// planned failover, the array itself up, lost every write since the recovery
// point. So the peer is asked again, with backoff, within the
// promote's shared deadline, and only a peer that stays silent throughout is
// treated as unreachable. A peer that answers any retry gets the planned
// path, freeze and drain, force or not.
//
// Every ask is a real call. The process's circuit breaker fails calls
// to an array other calls found silent, and it answered every ask of both
// promote calls itself while the array was already back: the
// retries could not outlast a blip they never reached. The asks therefore go
// out WithoutFailFast -- at most 1 + peerRetries per promote call, each
// bounded by the deadline below -- and an answer closes the breaker for the
// rest of the controller. Only the asks: the fallback to the local view and
// everything after the consult keep the fail-fast. A reply the peer sent but
// that cannot be used is Internal, not Unavailable, so it is never retried
// as silence or answered by the local view.
func (r *ReplicationServer) consultPeerForPromote(ctx context.Context, rpc string, e *replicationEnds) (*relationship, error) {
	ask := qumulo.WithoutFailFast(ctx)
	rel, err := r.findRelationship(ask, rpc, e, true)
	wait := r.peerRetryWait
	deadline := r.promoteDeadlineFrom(ctx)
	for attempt := 1; attempt <= r.peerRetries && err != nil && status.Code(err) == codes.Unavailable; attempt++ {
		// The wait and the call it leads to must both fit: a hung peer takes
		// the whole client timeout to fail, and a retry that cannot finish
		// before the deadline only turns this promote's answer into the
		// caller's DeadlineExceeded.
		if time.Now().Add(wait + r.peerCallTimeout).After(deadline) {
			break
		}
		slog.WarnContext(ctx, rpc+": the peer did not answer; asking again before treating it as unreachable",
			"volume", e.vol.Name, "peer", e.peerAddr, "attempt", attempt, "wait", wait, "error", err)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
		rel, err = r.findRelationship(ask, rpc, e, true)
		wait *= 2
	}
	if err != nil && status.Code(err) == codes.Unavailable {
		rel, err = r.findRelationship(ctx, rpc, e, false)
	}
	return rel, err
}

// unfreezeAbandonedPromote clears the freeze a planned promote left on a
// source whose peer never took over. Nothing else ever unfreezes a source, and
// a frozen source drops appends silently, so this row is what makes an
// aborted failover recoverable.
//
// It never runs blind. Unfreezing is safe only when the peer's end has been
// READ and is a read-only target; with the peer unreachable the freeze is
// held, force or not, because the peer may have promoted in the meantime and
// an unfreeze would then be the second writable copy (reproduced
// on the fabric: a forced promote on the old primary after a planned
// failover unfroze it beside the writable new primary).
func (r *ReplicationServer) unfreezeAbandonedPromote(
	ctx context.Context,
	rpc string,
	e *replicationEnds,
	rel *relationship,
) (*replication.PromoteVolumeResponse, error) {
	switch {
	case rel.peer == nil:
		// Not consulted (the consulted-and-unmatched case was refused by
		// the caller). Unavailable, which the controller retries without
		// force; the freeze holds until the peer can be read.
		return nil, status.Errorf(codes.Unavailable,
			"%s: volume %q on %s is a frozen source and peer %s could not be consulted about whether it promoted; "+
				"the freeze is held until the peer answers, and a forced promote does not lift it blind",
			rpc, e.vol.Name, e.localAddr, e.peerAddr)
	case rel.peerRole != roleTarget || !rel.peer.TargetRootReadOnly:
		return nil, status.Errorf(codes.Aborted,
			"%s: volume %q on %s is a frozen source and its peer %s is already writable; two writable copies "+
				"would be split brain. Set this side to secondary and resync",
			rpc, e.vol.Name, e.localAddr, e.peerAddr)
	case r.promotes.inFlight(rel.id):
		// The peer's promote is running on this controller and is about to
		// make it writable: not abandoned (as liftPromoteFreeze).
		return nil, status.Errorf(codes.Aborted,
			"%s: a promote of relationship %s on peer %s is still running on this controller; the freeze is "+
				"not lifted under it, retrying", rpc, rel.id, e.peerAddr)
	}

	// The drain may have switched a policy-only relationship to the
	// combined mode (drainSource); an abandoned promote must not leave that
	// behind, so the class's mode goes back on in the same PATCH (review
	// L4). Same PATCH, not a second one: a freeze and a mode are one
	// resource on the array and this row is retried as a unit.
	unfrozen := false
	patch := qumulo.SourcePatch{SourceRootReadOnly: &unfrozen}
	if rel.local.ReplicationMode != "" && rel.local.ReplicationMode != e.params.arrayMode() {
		patch.ReplicationMode = e.params.arrayMode()
	}
	if _, err := e.local.PatchSource(ctx, rel.id, patch); err != nil {
		return nil, unreachableOr(e.localAddr, err, "%s: unfreezing source relationship %s on %s", rpc, rel.id, e.localAddr)
	}
	r.dropFreezeMarker(ctx, rpc, e.local, rel.id, rel.local.SourceRootID)
	slog.WarnContext(ctx, rpc+": unfroze a source left read-only by an abandoned planned promote",
		"volume", e.vol.Name, "relationship", rel.id, "peerTargetReadOnly", rel.peer.TargetRootReadOnly,
		"modeReapplied", patch.ReplicationMode)
	if err := r.ensureOwnExportWritable(ctx, rpc, e); err != nil {
		return nil, err
	}
	return &replication.PromoteVolumeResponse{}, nil
}

// drainSource is the planned promote's final sync: freeze the source, then
// wait for its recovery point to pass the freeze -- by the source array's
// clock -- and for the policy snapshots to have crossed.
//
// Never a manual replicate: the array refuses one while replication is
// enabled, in every mode. A policy-only relationship is switched to the
// combined mode for the drain instead, which is what makes a frozen source
// drain at all in that mode.
func (r *ReplicationServer) drainSource(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) error {
	if rel.peer == nil {
		// PromoteVolume decides what an absent peer means (unreachable is
		// its one FailedPrecondition; answered-without-a-match is refused)
		// before it gets here. Internal, so a future caller that forgets
		// cannot turn this into a forcing code by accident.
		return status.Errorf(codes.Internal, "%s: drain requested without the peer's view of relationship %s", rpc, rel.id)
	}
	if rel.peer.State != qumulo.RelationshipEstablished {
		return status.Errorf(codes.Aborted, "%s: the source end on %s is %s, not ESTABLISHED; retrying",
			rpc, e.peerAddr, rel.peer.State)
	}

	// The state-based form. Measured on Core 7.9.2.2: a freeze starts no
	// replication job; the recovery point moves
	// on the array's own 60 s cycle; a manual replicate is refused while
	// replication is enabled. So the gate does not reason about time at all.
	// It freezes the source, takes a MARKER snapshot of the frozen tree, and
	// promotes exactly when the array reports no change between the marker
	// and the last snapshot that is fully on the target. The marker lives on
	// the array, so a retry reads the same marker; a marker taken before the
	// freeze, or before writes (a stale one), shows changes against a newer
	// replicated snapshot and is replaced. Nothing here can promote a target
	// that differs from the frozen source, and nothing waits once it does not.
	//
	// The mode is switched when the ARRAY says the relationship is
	// policy-only, not only when the class does: a class that drifted, or a
	// Demote that already switched it, must both end in a mode that cycles.
	policyOnly := rel.peer.ReplicationMode == qumulo.ReplicationModeSnapshotPolicy || !e.params.drainsContinuously()
	alreadyFrozen := rel.peer.SourceRootReadOnly

	// Whose freeze it is decides whether an undo of this promote may lift it,
	// and the only durable record of that is the marker's name. The
	// freeze and the marker are two calls, so a promote that froze the
	// source and then failed to take its marker -- an HTTP 500, a timeout, a
	// controller killed between the two -- left an unmarked freeze, and its
	// retry, finding the source frozen, marked it held: the operator's. The
	// undo then kept both copies read-only. So the intent is recorded
	// first: an unfrozen source gets a pending marker before it is frozen,
	// and a retry that finds one knows the freeze is its own. An unmarked
	// freeze is then never a promote's, and stays the operator's, as before.
	if !alreadyFrozen {
		if err := r.recordFreezeIntent(ctx, rpc, e, rel); err != nil {
			return err
		}
	}

	frozen := true
	patch := qumulo.SourcePatch{SourceRootReadOnly: &frozen}
	if policyOnly {
		enabled := true
		patch.ReplicationMode = qumulo.ReplicationModeSnapshotPolicyContinuous
		patch.ReplicationEnabled = &enabled
	}
	if _, err := e.peer.PatchSource(ctx, rel.id, patch); err != nil {
		return unreachableOr(e.peerAddr, err, "%s: freezing the source on %s", rpc, e.peerAddr)
	}
	marker, err := r.freezeMarker(ctx, rpc, e, rel, alreadyFrozen)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, rpc+": source frozen; waiting for the target to hold the frozen tree",
		"volume", e.vol.Name, "relationship", rel.id, "marker", marker.ID, "alreadyFrozen", alreadyFrozen,
		"modeSwitched", policyOnly, "modeBefore", rel.peer.ReplicationMode)

	deadline := r.promoteDeadlineFrom(ctx)
	var crossing crossingCheck
	for {
		src, err := e.peer.GetSourceStatus(ctx, rel.id)
		if err != nil {
			return unreachableOr(e.peerAddr, err, "%s: polling the source on %s", rpc, e.peerAddr)
		}
		if src.QueuedSnapshotCount > 0 {
			// Snapshots still queued: the source's table is read afresh
			// once they have crossed, whatever it held before.
			crossing.reset()
		}
		if src.ErrorFromLastJob != "" {
			return status.Errorf(codes.Internal, "%s: the last replication job of %s failed: %s",
				rpc, rel.id, withLocalOwnershipRemedy(src.ErrorFromLastJob))
		}
		// Only a completed snapshot is on the target; only an idle
		// relationship is safe to make writable. Then the one question:
		// does the replicated snapshot differ from the marker?
		if src.JobIdle() && src.RecoveryPointSnapshotID != "" {
			newer, older := marker.ID, src.RecoveryPointSnapshotID
			markerNewer := snapshotIDLess(src.RecoveryPointSnapshotID, marker.ID)
			if !markerNewer {
				newer, older = src.RecoveryPointSnapshotID, marker.ID
			}
			same, err := e.peer.SnapshotDiffEmpty(ctx, newer, older)
			if err != nil {
				return unreachableOr(e.peerAddr, err, "%s: comparing snapshots %s and %s on %s", rpc, newer, older, e.peerAddr)
			}
			switch {
			case same && src.QueuedSnapshotCount > 0:
				// The tree has drained but snapshots are still queued, so the
				// drain cannot end on this poll; listing both arrays' whole
				// snapshot tables to say so cost two full listings every 2 s.
				// They are listed once the queue is empty.
				slog.InfoContext(ctx, rpc+": data drained but snapshots still crossing",
					"volume", e.vol.Name, "relationship", rel.id, "queued", src.QueuedSnapshotCount)
			case same:
				// The target holds exactly the frozen tree, whichever of the
				// two snapshots is newer. Policy snapshots still have to have
				// crossed.
				missing, err := r.snapshotsMissingOnTarget(ctx, e, rel, src, &crossing)
				if err != nil {
					return err
				}
				if src.QueuedSnapshotCount == 0 && len(missing) == 0 {
					slog.InfoContext(ctx, rpc+": target holds the frozen tree; source drained and snapshots crossed",
						"volume", e.vol.Name, "relationship", rel.id, "marker", marker.ID,
						"replicatedSnapshot", src.RecoveryPointSnapshotID, "recoveryPoint", src.RecoveryPoint)
					return nil
				}
				slog.InfoContext(ctx, rpc+": data drained but snapshots still crossing",
					"volume", e.vol.Name, "relationship", rel.id, "queued", src.QueuedSnapshotCount, "missing", missing)
			case !markerNewer:
				// A replicated snapshot NEWER than the marker that differs
				// from it: the tree changed after the marker was taken, so
				// the marker predates this freeze (left by an earlier
				// failover, or taken before a hand unfreeze). Replace it;
				// the next pass compares against the tree as frozen now.
				slog.WarnContext(ctx, rpc+": freeze marker predates writes the target already has; replacing it",
					"volume", e.vol.Name, "relationship", rel.id, "staleMarker", marker.ID, "replicatedSnapshot", src.RecoveryPointSnapshotID)
				// The replacement keeps the kind of the marker it replaces
				// and is taken before the stale one goes, so the freeze is
				// never left unmarked between the two.
				if marker, err = r.replaceFreezeMarker(ctx, rpc, e, rel, marker, isHeldFreezeMarker(marker, rel.id)); err != nil {
					return err
				}
			}
		}

		if !time.Now().Add(r.drainPollWait).Before(deadline) {
			// The freeze holds; the controller retries and lands back here,
			// where the same marker is waiting.
			return status.Errorf(codes.Aborted,
				"%s: source %s still draining: the target does not yet hold the frozen tree (marker snapshot %s, "+
					"replicated snapshot %s at %s, job %s); the source stays frozen, retrying",
				rpc, rel.id, marker.ID, src.RecoveryPointSnapshotID, src.RecoveryPoint.Format(time.RFC3339), src.JobState)
		}
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-time.After(r.drainPollWait):
		}
	}
}

// freezeMarker finds or takes the marker snapshot of the frozen source tree
// for this relationship on the source array (the peer of a promote).
//
// held names who made the freeze when a new marker has to be taken: false
// when this drain froze the source, true when it was frozen already (see
// heldFreezeTag). A marker that is found is reused as it is, so a retry of a
// promote that froze the source keeps its plain marker -- except a pending
// one (pendingFreezeTag), which predates the freeze: it says the freeze is
// this promote's, and is replaced by a plain marker of the frozen tree.
func (r *ReplicationServer) freezeMarker(ctx context.Context, rpc string, e *replicationEnds, rel *relationship, held bool) (qumulo.SnapshotInfo, error) {
	rootID := rel.peer.SourceRootID
	if rootID == "" {
		return qumulo.SnapshotInfo{}, status.Errorf(codes.Internal, "%s: the source end of %s reports no source_root_id", rpc, rel.id)
	}
	found, err := r.findFreezeMarker(ctx, e.peer, rel.id, rootID)
	if err != nil {
		return qumulo.SnapshotInfo{}, unreachableOr(e.peerAddr, err, "%s: listing snapshots on %s", rpc, e.peerAddr)
	}
	switch {
	case found == nil:
		return r.takeFreezeMarker(ctx, rpc, e, rel, markerNameFor(rel.id, held))
	case isPendingFreezeMarker(*found, rel.id):
		return r.replaceFreezeMarker(ctx, rpc, e, rel, *found, false)
	default:
		return *found, nil
	}
}

// recordFreezeIntent takes the pending marker a promote leaves on an unfrozen
// source before it freezes it, unless an earlier attempt's is still the
// newest marker there. See pendingFreezeTag.
func (r *ReplicationServer) recordFreezeIntent(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) error {
	rootID := rel.peer.SourceRootID
	if rootID == "" {
		return status.Errorf(codes.Internal, "%s: the source end of %s reports no source_root_id", rpc, rel.id)
	}
	found, err := r.findFreezeMarker(ctx, e.peer, rel.id, rootID)
	if err != nil {
		return unreachableOr(e.peerAddr, err, "%s: listing snapshots on %s", rpc, e.peerAddr)
	}
	if found != nil && isPendingFreezeMarker(*found, rel.id) {
		return nil
	}
	if _, err := r.takeFreezeMarker(ctx, rpc, e, rel, pendingFreezeTag+freezeMarkerName(rel.id)); err != nil {
		return err
	}
	// A plain marker on an unfrozen source is a leftover -- an earlier
	// failover's, or one taken before a hand unfreeze -- and never this
	// freeze's reference, so it goes now rather than in 24 hours. A held one
	// is left: it is older than the pending marker, so never the one found,
	// and an operator's freeze racing this one may have just taken it.
	if found != nil && !isHeldFreezeMarker(*found, rel.id) {
		if err := e.peer.DeleteSnapshot(ctx, found.ID); err != nil && !qumulo.IsNotFound(err) {
			slog.WarnContext(ctx, rpc+": could not remove a leftover freeze marker snapshot; it expires on its own",
				"relationship", rel.id, "marker", found.ID, "error", err)
		}
	}
	return nil
}

// takeFreezeMarker takes a marker snapshot of the source root under name and
// remembers its ID.
func (r *ReplicationServer) takeFreezeMarker(ctx context.Context, rpc string, e *replicationEnds, rel *relationship, name string) (qumulo.SnapshotInfo, error) {
	rootID := rel.peer.SourceRootID
	created, err := e.peer.CreateExpiringSnapshot(ctx, rootID, name, time.Now().Add(freezeMarkerTTL))
	if err != nil {
		return qumulo.SnapshotInfo{}, unreachableOr(e.peerAddr, err, "%s: taking the freeze marker snapshot of %s on %s", rpc, rel.id, e.peerAddr)
	}
	r.hints.markers.Store(markerHintKey(rel.id, rootID), newIDHint(created.ID))
	r.noteHintStored()
	slog.InfoContext(ctx, rpc+": freeze marker snapshot taken", "volume", e.vol.Name, "relationship", rel.id,
		"marker", created.ID, "name", created.Name)
	return created, nil
}

// replaceFreezeMarker takes a new marker of the given kind, then deletes old.
// In that order, so the source is never frozen with no marker between the
// two: a failed create leaves old, whose kind the retry reads again.
// The delete is best effort; a leftover is older, so never the one found,
// and expires on its own.
func (r *ReplicationServer) replaceFreezeMarker(
	ctx context.Context,
	rpc string,
	e *replicationEnds,
	rel *relationship,
	old qumulo.SnapshotInfo,
	held bool,
) (qumulo.SnapshotInfo, error) {
	created, err := r.takeFreezeMarker(ctx, rpc, e, rel, markerNameFor(rel.id, held))
	if err != nil {
		return qumulo.SnapshotInfo{}, err
	}
	if err := e.peer.DeleteSnapshot(ctx, old.ID); err != nil && !qumulo.IsNotFound(err) {
		slog.WarnContext(ctx, rpc+": could not remove the replaced freeze marker snapshot; it expires on its own",
			"relationship", rel.id, "marker", old.ID, "error", err)
	}
	return created, nil
}

// dropFreezeMarker removes the marker for a relationship on the array that
// holds its source end, if there is one. Called wherever a freeze ends or a
// promote completes; best effort, since a leftover marker expires on its
// own and is replaced if it ever compares stale.
func (r *ReplicationServer) dropFreezeMarker(ctx context.Context, rpc string, client replicationClient, relID, rootID string) {
	if rootID == "" {
		return
	}
	found, err := r.findFreezeMarker(ctx, client, relID, rootID)
	if err != nil || found == nil {
		return
	}
	r.deleteFreezeMarker(ctx, rpc, client, relID, rootID, *found)
}

// deleteFreezeMarker removes a marker already found; best effort, as for
// dropFreezeMarker.
func (r *ReplicationServer) deleteFreezeMarker(ctx context.Context, rpc string, client replicationClient, relID, rootID string, marker qumulo.SnapshotInfo) {
	if err := client.DeleteSnapshot(ctx, marker.ID); err != nil && !qumulo.IsNotFound(err) {
		slog.WarnContext(ctx, rpc+": could not remove the freeze marker snapshot; it expires on its own",
			"relationship", relID, "marker", marker.ID, "error", err)
		return
	}
	r.hints.markers.Delete(markerHintKey(relID, rootID))
	slog.InfoContext(ctx, rpc+": freeze marker snapshot removed", "relationship", relID, "marker", marker.ID)
}

// markerNameFor is the marker name for a freeze of the given kind.
func markerNameFor(relID string, held bool) string {
	if held {
		return heldFreezeTag + freezeMarkerName(relID)
	}
	return freezeMarkerName(relID)
}

// isHeldFreezeMarker reports whether a marker records a freeze the promote
// did not make.
func isHeldFreezeMarker(m qumulo.SnapshotInfo, relID string) bool {
	return strings.HasSuffix(m.Name, heldFreezeTag+freezeMarkerName(relID))
}

// isPendingFreezeMarker reports whether a marker is a promote's record that
// it is about to freeze the source (pendingFreezeTag).
func isPendingFreezeMarker(m qumulo.SnapshotInfo, relID string) bool {
	return strings.HasSuffix(m.Name, pendingFreezeTag+freezeMarkerName(relID))
}

// dropPromoteFreezeMarker is dropFreezeMarker for a completed promote: it
// removes the promote's own marker and leaves a held one (heldFreezeTag).
func (r *ReplicationServer) dropPromoteFreezeMarker(ctx context.Context, rpc string, client replicationClient, relID, rootID string) {
	if rootID == "" {
		return
	}
	found, err := r.findFreezeMarker(ctx, client, relID, rootID)
	if err != nil || found == nil || isHeldFreezeMarker(*found, relID) {
		return
	}
	r.deleteFreezeMarker(ctx, rpc, client, relID, rootID, *found)
}

func freezeMarkerName(relID string) string {
	if len(relID) > 8 {
		relID = relID[:8]
	}
	return freezeMarkerSuffix + relID
}

// findFreezeMarker finds the newest marker of relID on the array whose source
// root is rootID, or nil.
//
// The marker this process last took or found is read back by its ID first,
// and believed only if it is still that relationship's live marker on that
// root; otherwise -- a restart, a marker removed or replaced elsewhere -- the
// snapshot table is listed as before. A promote listed the whole table three
// times for its marker alone.
func (r *ReplicationServer) findFreezeMarker(ctx context.Context, client replicationClient, relID, rootID string) (*qumulo.SnapshotInfo, error) {
	name := freezeMarkerName(relID)
	isMarker := func(s qumulo.SnapshotInfo) bool {
		return s.SourceFileID == rootID && !s.InDelete && strings.HasSuffix(s.Name, name)
	}
	key := markerHintKey(relID, rootID)
	if hint, ok := r.hints.markers.Load(key); ok {
		if snap, err := client.GetSnapshot(ctx, hint.(idHint).id); err == nil && isMarker(snap) {
			return &snap, nil
		}
		r.hints.markers.Delete(key)
	}

	snaps, err := client.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	var newest *qumulo.SnapshotInfo
	for i := range snaps {
		s := snaps[i]
		if !isMarker(s) {
			continue
		}
		if newest == nil || snapshotIDLess(newest.ID, s.ID) {
			newest = &s
		}
	}
	if newest != nil {
		r.hints.markers.Store(key, newIDHint(newest.ID))
		r.noteHintStored()
	}
	return newest, nil
}

// The marker hint (replicationHints.markers) maps a relationship and the
// source root its marker is taken of to the ID of that marker, as last taken
// or found. A hint only, read back and checked on every use. The relationship
// ID is unique across arrays and the root ID names the array's own directory,
// so no endpoint is needed.

func markerHintKey(relID, rootID string) string {
	return relID + "\x00" + rootID
}

// snapshotIDLess orders snapshot ids, which the cluster allocates from one
// increasing integer sequence.
func snapshotIDLess(a, b string) bool {
	ai, aerr := strconv.ParseInt(a, 10, 64)
	bi, berr := strconv.ParseInt(b, 10, 64)
	if aerr != nil || berr != nil {
		return a < b
	}
	return ai < bi
}

// snapshotsMissingOnTarget lists the source's unexpired policy snapshots that
// are newer than the newest snapshot on the target and absent from it.
//
// Matching is by timestamp: a replicated snapshot carries no policy_name on
// the target and is renamed (measured), so the timestamp is the only key both
// arrays share. Live data only -- no record of a previous promote is kept.
//
// The key is the instant (UnixNano), never the time.Time: two time.Time
// values for the same instant with different Location pointers are unequal
// as map keys, and the two arrays are not guaranteed to render their
// timestamps with the same zone suffix. Keyed by time.Time, every snapshot
// would read as missing and the gate would Abort forever.
//
// Each 2 s poll used to list the whole snapshot table on both arrays until
// the target held everything. The source's table is now listed once
// per drain (check), and on later polls only the snapshots still missing are
// read back from it by ID, so one that expired or was deleted meanwhile is
// not waited for. The target's table is still listed on each poll: a
// replicated copy is renamed and has an ID nothing knows in advance.
func (r *ReplicationServer) snapshotsMissingOnTarget(
	ctx context.Context,
	e *replicationEnds,
	rel *relationship,
	src qumulo.RelationshipStatus,
	check *crossingCheck,
) ([]time.Time, error) {
	listed := false
	if check.source == nil {
		if err := check.listSource(ctx, e, src); err != nil {
			return nil, err
		}
		listed = true
	}
	targetSnaps, err := e.local.ListSnapshots(ctx)
	if err != nil {
		return nil, unreachableOr(e.localAddr, err, "promotevolume: listing snapshots on %s", e.localAddr)
	}

	var newest time.Time
	present := map[int64]bool{}
	for _, s := range targetSnaps {
		// A Disable's marker or Enable's comparison snapshot of the root
		// is no replicated snapshot, and its time says nothing.
		if s.SourceFileID != rel.local.TargetRootID || isReprotectSnapshot(s) {
			continue
		}
		present[s.CreatedAt.UnixNano()] = true
		if s.CreatedAt.After(newest) {
			newest = s.CreatedAt
		}
	}
	stillMissing := func() []qumulo.SnapshotInfo {
		var out []qumulo.SnapshotInfo
		for _, s := range check.source {
			if s.CreatedAt.After(newest) && !present[s.CreatedAt.UnixNano()] {
				out = append(out, s)
			}
		}
		return out
	}
	pending := stillMissing()

	switch {
	case listed || len(pending) == 0:
	case len(pending) > maxMissingReadByID:
		// Cheaper to read the table once than this many snapshots.
		if err := check.listSource(ctx, e, src); err != nil {
			return nil, err
		}
		pending = stillMissing()
	default:
		kept := pending[:0]
		for _, s := range pending {
			current, err := e.peer.GetSnapshot(ctx, s.ID)
			switch {
			case qumulo.IsNotFound(err):
				continue
			case err != nil:
				return nil, unreachableOr(e.peerAddr, err, "promotevolume: reading snapshot %s on %s", s.ID, e.peerAddr)
			case current.InDelete:
				continue
			}
			kept = append(kept, s)
		}
		pending = kept
	}
	// What is no longer missing need not be checked again in this drain.
	check.source = append(check.source[:0:0], pending...)

	missing := make([]time.Time, 0, len(pending))
	for _, s := range pending {
		missing = append(missing, s.CreatedAt)
	}
	return missing, nil
}

// maxMissingReadByID is how many still-missing snapshots a drain poll reads
// back one by one before it lists the source's table instead.
const maxMissingReadByID = 8

// crossingCheck is what one drain knows of the source's snapshots for the
// The pending-snapshot check: those still to cross, as of its last listing of
// the source's
// table. Kept for one promote call only.
type crossingCheck struct {
	// source is the source's unexpired policy snapshots of the root not yet
	// seen on the target; nil until the table is listed.
	source []qumulo.SnapshotInfo
}

// reset makes the next check list the source's table again.
func (c *crossingCheck) reset() { c.source = nil }

// listSource lists the source's table for the snapshots the check gates on.
func (c *crossingCheck) listSource(ctx context.Context, e *replicationEnds, src qumulo.RelationshipStatus) error {
	snaps, err := e.peer.ListSnapshots(ctx)
	if err != nil {
		return unreachableOr(e.peerAddr, err, "promotevolume: listing snapshots on %s", e.peerAddr)
	}
	c.source = []qumulo.SnapshotInfo{}
	for _, s := range snaps {
		if s.SourceFileID == src.SourceRootID && s.CreatedByPolicy() && !s.InDelete {
			c.source = append(c.source, s)
		}
	}
	return nil
}

// makeWritable promotes the local target, tolerating a retry that lands after
// an earlier attempt already did it.
func (r *ReplicationServer) makeWritable(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) error {
	after, err := e.local.MakeTargetWritable(ctx, rel.id)
	switch {
	case err == nil:
		// make-writable is asynchronous. The array accepts it and answers
		// DISCONNECTING, and the tree stays read-only until it reaches
		// DISCONNECTED -- measured on 7.10.0 (2026-09-16): a write landing
		// between the two got HTTP 403 fs_read_only_error while this RPC had
		// already reported the volume promoted. Returning here would tell the
		// controller the volume is primary while a pod mounting it gets
		// EROFS, so wait for the state the caller was promised.
		if err := r.awaitWritableTarget(ctx, rpc, e, rel, &after); err != nil {
			return err
		}
		slog.InfoContext(ctx, rpc+": target made writable",
			"volume", e.vol.Name, "relationship", rel.id, "state", after.State, "recoveryPoint", after.RecoveryPoint)
		// The revert this ran left a snapshot of the target with no
		// expiration.
		r.expireRevertSnapshots(ctx, rpc, e.local, e.localAddr, revertRoot(e, rel))
		return nil
	case qumulo.IsAlreadyDisconnected(err):
		slog.InfoContext(ctx, rpc+": target was already writable", "volume", e.vol.Name, "relationship", rel.id)
		// A retry of a promote whose first call stopped before it could
		// expire the revert snapshot.
		r.expireRevertSnapshots(ctx, rpc, e.local, e.localAddr, revertRoot(e, rel))
		return nil
	default:
		return unreachableOr(e.localAddr, err, "%s: making relationship %s writable on %s", rpc, rel.id, e.localAddr)
	}
}

// promoteDeadlineKey carries one promote's shared budget.
type promoteDeadlineKey struct{}

// withPromoteDeadline starts the budget every waiting step of one promote
// draws from. It is a context value rather than a field because the budget
// belongs to the RPC, not to the server, and two promotes run concurrently.
func (r *ReplicationServer) withPromoteDeadline(ctx context.Context) context.Context {
	return context.WithValue(ctx, promoteDeadlineKey{}, r.budgetDeadline(ctx))
}

// promoteDeadlineFrom returns the shared deadline, or a fresh one for a
// caller that set none -- DemoteVolume and the resync paths drain too, and
// each of those is one wait, so a budget of its own is the right answer
// there.
func (r *ReplicationServer) promoteDeadlineFrom(ctx context.Context) time.Time {
	if deadline, ok := ctx.Value(promoteDeadlineKey{}).(time.Time); ok {
		return deadline
	}
	return r.budgetDeadline(ctx)
}

// budgetDeadline is now plus the drain budget, or the caller's own deadline
// less replyMargin when that comes first. The budget used to ignore the
// caller: a caller whose timeout was shorter than the budget saw its promote
// end as DeadlineExceeded, mid-retry or mid-drain, instead of the Aborted or
// FailedPrecondition the controller acts on.
func (r *ReplicationServer) budgetDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(r.drainBudget)
	if callerDeadline, ok := ctx.Deadline(); ok {
		if capped := callerDeadline.Add(-r.replyMargin); capped.Before(deadline) {
			deadline = capped
		}
	}
	return deadline
}

// awaitWritableTarget polls the local target end until the array reports the
// promoted tree actually writable, updating status in place so the caller logs
// the state it settled on. It borrows drainSource's budget and cadence, and
// its exhaustion answer: Aborted, so the controller comes back rather than
// treating a half-finished promote as done. A retry that lands after the
// array finished takes PromoteVolume's "target is already writable" row, which
// re-plumbs without calling make-writable again.
func (r *ReplicationServer) awaitWritableTarget(
	ctx context.Context,
	rpc string,
	e *replicationEnds,
	rel *relationship,
	status_ *qumulo.RelationshipStatus,
) error {
	deadline := r.promoteDeadlineFrom(ctx)
	wait := r.firstPollWait()
	for {
		if status_.State == qumulo.RelationshipDisconnected && !status_.TargetRootReadOnly {
			return nil
		}
		if !time.Now().Add(wait).Before(deadline) {
			slog.WarnContext(ctx, rpc+": make-writable accepted but the target is not writable yet; asking the controller to retry",
				"volume", e.vol.Name, "relationship", rel.id, "state", status_.State, "targetReadOnly", status_.TargetRootReadOnly)
			return status.Errorf(codes.Aborted,
				"%s: relationship %s on %s is %s with the tree read-only %t after make-writable; "+
					"the promoted copy is not writable yet, retrying",
				rpc, rel.id, e.localAddr, status_.State, status_.TargetRootReadOnly)
		}
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-time.After(wait):
		}
		if wait = wait * 2; wait > r.drainPollWait {
			wait = r.drainPollWait
		}
		next, err := e.local.GetTargetStatus(ctx, rel.id)
		if err != nil {
			return unreachableOr(e.localAddr, err, "%s: polling the promoted target on %s", rpc, e.localAddr)
		}
		*status_ = next
	}
}

// firstPollWait is where awaitWritableTarget's backoff starts, never above
// the steady cadence -- a test that shortens drainPollWait to keep itself
// fast must not find the first wait longer than the one it asked for.
func (r *ReplicationServer) firstPollWait() time.Duration {
	if r.drainPollWait < defaultFirstPollWait {
		return r.drainPollWait
	}
	return defaultFirstPollWait
}

// ensureOwnExportWritable makes the volume's own export read-write on a
// volume that is, or is about to be reported as, the writable source.
//
// replumb does this for a promote that completes the make-writable itself.
// Three other rows also end with this side as the writable source and
// answer OK: Promote's "already the writable source", an unfrozen abandoned
// promote, and Enable re-protecting a promoted ex-target. A copy can reach
// any of them without replumb having run -- measured on the lab pair:
// a planned promote whose make-writable was accepted
// but not yet visible (DISCONNECTING) answered "retry"; before the retry the
// controller called Enable, which re-protected the now-writable copy in the
// reverse direction; every later Promote took the "already writable" or
// "abandoned promote" row. The VolumeReplication read Primary while the
// copy's export was still the read-only replica export from pre-stage, so
// the node rightly refused every read-write publish and the application
// never started.
//
// Volume scope only. In group scope the export is the class's covering
// export, which belongs to the operator (see the unpromoted marker), so a
// read-only one is reported rather than changed.
func (r *ReplicationServer) ensureOwnExportWritable(ctx context.Context, rpc string, e *replicationEnds) error {
	if e.export == nil || !e.export.ReadOnly {
		return nil
	}
	if e.scope == scopeGroup {
		slog.WarnContext(ctx, rpc+": the group's covering export is read-only on the writable source; it is the "+
			"operator's and is left as it is, so members cannot be mounted read-write until it is made writable",
			"volume", e.vol.Name, "export", e.export.ExportPath, "exportID", e.export.ID)
		return nil
	}
	if err := e.local.SetExportReadOnly(ctx, e.export.ID, false); err != nil {
		return unreachableOr(e.localAddr, err, "%s: making export %q writable on %s", rpc, e.export.ExportPath, e.localAddr)
	}
	e.export.ReadOnly = false
	slog.WarnContext(ctx, rpc+": the writable source's own export was still read-only (left from its replica "+
		"pre-stage); made read-write",
		"volume", e.vol.Name, "export", e.export.ExportPath, "exportID", e.export.ID)
	return nil
}

// replumb makes the promoted copy usable: its export read-write and its quota
// present. In the normal case Enable pre-staged both and this only flips the
// export; a gap means a volume protected before pre-staging existed, or
// drift, and is filled from the peer when the peer answers.
func (r *ReplicationServer) replumb(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) error {
	if e.export != nil {
		if e.export.ReadOnly {
			if err := e.local.SetExportReadOnly(ctx, e.export.ID, false); err != nil {
				return unreachableOr(e.localAddr, err, "%s: making export %q writable on %s", rpc, e.export.ExportPath, e.localAddr)
			}
			slog.InfoContext(ctx, rpc+": pre-staged export flipped to read-write",
				"volume", e.vol.Name, "export", e.export.ExportPath, "exportID", e.export.ID)
		}
	} else if rel.peer != nil {
		// No export here: pre-stage never ran or drifted. Mirror the peer's.
		sourceExport, err := findExportByFSPath(ctx, e.peer, e.peerPath)
		if err != nil {
			slog.WarnContext(ctx, rpc+": could not read the peer's export to recreate it here", "error", err)
		} else if sourceExport != nil {
			slog.WarnContext(ctx, rpc+": replica had no export; creating one from the peer's definition (pre-stage drifted)",
				"volume", e.vol.Name, "export", sourceExport.ExportPath)
			mirrored := *sourceExport
			mirrored.ReadOnly = false
			if err := ensureReplicaExport(ctx, e.local, mirrored, e.realPath, e.vol.Name); err != nil {
				return unreachableOr(e.localAddr, err, "%s: creating the export on %s", rpc, e.localAddr)
			}
			if fresh, err := findExportByFSPath(ctx, e.local, e.realPath); err == nil && fresh != nil && fresh.ReadOnly {
				if err := e.local.SetExportReadOnly(ctx, fresh.ID, false); err != nil {
					return unreachableOr(e.localAddr, err, "%s: making export %q writable on %s", rpc, fresh.ExportPath, e.localAddr)
				}
			}
		}
	} else {
		slog.WarnContext(ctx, rpc+": replica has no export and the peer was not consulted; the volume is promoted but not mountable",
			"volume", e.vol.Name, "path", e.realPath)
	}

	// A group root is a class basePath, not a volume: it carries no quota of
	// its own and the limits that matter are the members'.
	if e.scope == scopeGroup {
		verifyMemberQuotas(ctx, rpc, e, rel)
		return nil
	}

	if _, err := e.local.GetQuota(ctx, e.fileID); err != nil {
		if !qumulo.IsNotFound(err) {
			return unreachableOr(e.localAddr, err, "%s: reading the quota of %q on %s", rpc, e.vol.Name, e.localAddr)
		}
		if rel.peer == nil {
			slog.WarnContext(ctx, rpc+": replica has no quota and the peer was not consulted; writes are unbounded until one is set",
				"volume", e.vol.Name)
			return nil
		}
		limit, err := e.peer.GetQuota(ctx, rel.peer.SourceRootID)
		if err != nil {
			slog.WarnContext(ctx, rpc+": replica has no quota and the peer's could not be read", "error", err)
			return nil
		}
		slog.WarnContext(ctx, rpc+": replica had no quota; creating one from the peer's (pre-stage drifted)",
			"volume", e.vol.Name, "limitBytes", limit)
		if err := ensureQuota(ctx, e.local, e.fileID, limit); err != nil {
			return unreachableOr(e.localAddr, err, "%s: creating the quota on %s", rpc, e.localAddr)
		}
	}
	return nil
}

// DemoteVolume freezes the local source so no write can diverge before a
// resync. It never runs make-writable, never deletes, never reverses.
func (r *ReplicationServer) DemoteVolume(
	ctx context.Context,
	req *replication.DemoteVolumeRequest,
) (*replication.DemoteVolumeResponse, error) {
	const rpc = "demotevolume"

	e, err := r.resolve(ctx, rpc, requestVolumeID(req), req.GetReplicationSource().GetVolumegroup().GetVolumeGroupId(),
		req.GetParameters(), req.GetSecrets(), false)
	if err != nil {
		return nil, err
	}
	// The peer is not needed to freeze, and demote is exactly what runs on
	// an old primary the moment it comes back, when the peer may be
	// anything -- so a peer that does not answer falls back to this side's
	// view, as before. A peer that DOES answer is read first: with
	// the class naming the wrong path or a third array, the freeze froze a
	// source whose real peer was never going to take over through this
	// class, and the promote meant to follow was refused.
	rel, err := r.findRelationship(ctx, rpc, e, true)
	if err != nil && status.Code(err) == codes.Unavailable {
		rel, err = r.findRelationship(ctx, rpc, e, false)
	}
	if err != nil {
		return nil, err
	}
	if err := refuseUnmatchedPeer(rpc, e, rel,
		"demote is refused because it would freeze or fence a copy through a class that does not describe the relationship"); err != nil {
		return nil, err
	}
	if rel == nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"%s: volume %q has no replication relationship on %s", rpc, e.vol.Name, e.localAddr)
	}

	if rel.role == roleTarget {
		if rel.local.State == qumulo.RelationshipDisconnected && !rel.local.TargetRootReadOnly {
			return r.fencePromotedTarget(ctx, rpc, e, rel)
		}
		// A read-only target is already what "secondary" means.
		return &replication.DemoteVolumeResponse{}, nil
	}
	if rel.local.State == qumulo.RelationshipEnded {
		// The peer deleted its end; there is nothing left to freeze against.
		// Enable's ENDED handling removes the corpse when this side is next
		// protected; Demote only reports that the copy is not a source.
		slog.WarnContext(ctx, rpc+": relationship is ENDED; nothing to demote", "volume", e.vol.Name, "relationship", rel.id)
		return &replication.DemoteVolumeResponse{}, nil
	}
	if rel.local.SourceRootReadOnly {
		return &replication.DemoteVolumeResponse{}, nil
	}
	// A freeze only drains in a mode with a continuous component. A
	// policy-only relationship frozen as it stands would sit at its last
	// policy snapshot until the next fire, and a promote on the other side
	// would have nothing newer to wait for. So the freeze and
	// the switch to the combined mode go in one PATCH, the same way
	// drainSource does it; the array's replication cycle then carries the frozen tree.
	// Info reports the mode as drift until the resync re-applies the
	// class's mode after the reverse, which is the truth of it.
	frozen := true
	patch := qumulo.SourcePatch{SourceRootReadOnly: &frozen}
	if rel.local.ReplicationMode == qumulo.ReplicationModeSnapshotPolicy {
		enabled := true
		patch.ReplicationMode = qumulo.ReplicationModeSnapshotPolicyContinuous
		patch.ReplicationEnabled = &enabled
	}
	if _, err := e.local.PatchSource(ctx, rel.id, patch); err != nil {
		return nil, unreachableOr(e.localAddr, err, "%s: freezing source relationship %s on %s", rpc, rel.id, e.localAddr)
	}
	slog.InfoContext(ctx, rpc+": source frozen", "volume", e.vol.Name, "relationship", rel.id, "state", rel.local.State,
		"modeSwitched", patch.ReplicationMode != "")

	// Mark the freeze as the operator's (heldFreezeTag), so that if the DR
	// side is promoted and then set back to secondary, its resync leaves this
	// freeze for this object's own promote rather than lifting it. The
	// marker is also a snapshot of the frozen tree, which is what a later
	// drain would take anyway. Best effort: without it the resync falls back
	// to lifting the freeze once the DR side reads read-only, which leaves
	// one writable copy. A plain or pending marker found here predates this
	// freeze -- the source was not frozen -- so it is a leftover, and this
	// freeze is marked over it rather than read as a promote's.
	if rootID := rel.local.SourceRootID; rootID != "" {
		if found, err := r.findFreezeMarker(ctx, e.local, rel.id, rootID); err == nil && (found == nil || !isHeldFreezeMarker(*found, rel.id)) {
			if created, err := e.local.CreateExpiringSnapshot(ctx, rootID, markerNameFor(rel.id, true), time.Now().Add(freezeMarkerTTL)); err != nil {
				slog.WarnContext(ctx, rpc+": could not mark the freeze as a demote's", "volume", e.vol.Name,
					"relationship", rel.id, "error", err)
			} else {
				// The hint must name the newest marker: one still naming the
				// leftover would be read back and believed.
				r.hints.markers.Store(markerHintKey(rel.id, rootID), newIDHint(created.ID))
				r.noteHintStored()
			}
		}
	}
	return &replication.DemoteVolumeResponse{}, nil
}

// fencePromotedTarget is Demote on a target that was promoted: writable,
// DISCONNECTED, still taking writes through its read-write export. Nothing
// on the array makes such a copy read-only again except a reconnect (the
// resync), which discards its divergent writes and which the controller
// runs only after this RPC succeeds -- and only with autoResync. So Demote
// fences at the one place it can: the pre-staged export is set read-only,
// the same flip promote made the other way, and the copy stops accepting
// mounts that could write. The directory itself stays writable until the
// resync; the log says so rather than the RPC claiming a state it did not
// produce (reproduced on the fabric: Demote returned OK and
// changed nothing).
func (r *ReplicationServer) fencePromotedTarget(
	ctx context.Context,
	rpc string,
	e *replicationEnds,
	rel *relationship,
) (*replication.DemoteVolumeResponse, error) {
	switch {
	case e.export == nil:
		slog.WarnContext(ctx, rpc+": promoted target has no export to fence; it is not mountable through this driver, "+
			"but its directory stays writable on the array until a resync reconnects it",
			"volume", e.vol.Name, "relationship", rel.id, "path", e.realPath)
	case e.export.ReadOnly:
		slog.InfoContext(ctx, rpc+": promoted target's export is already read-only; the directory stays writable until a resync reconnects it",
			"volume", e.vol.Name, "relationship", rel.id, "export", e.export.ExportPath)
	default:
		if err := e.local.SetExportReadOnly(ctx, e.export.ID, true); err != nil {
			return nil, unreachableOr(e.localAddr, err, "%s: fencing export %q read-only on %s", rpc, e.export.ExportPath, e.localAddr)
		}
		slog.WarnContext(ctx, rpc+": promoted target fenced at its export (set read-only); the directory stays writable "+
			"on the array until a resync reconnects it and discards writes made since the recovery point",
			"volume", e.vol.Name, "relationship", rel.id, "export", e.export.ExportPath, "exportID", e.export.ID,
			"recoveryPoint", rel.local.RecoveryPoint)
	}
	return &replication.DemoteVolumeResponse{}, nil
}

// peerIsReadOnlyEstablishedTarget reports that the peer's end of rel is a
// read-only ESTABLISHED target: the healthy shape of the other side, read
// from the peer rather than inferred from this end.
func peerIsReadOnlyEstablishedTarget(rel *relationship) bool {
	return rel.peer != nil && rel.peerRole == roleTarget &&
		rel.peer.State == qumulo.RelationshipEstablished && rel.peer.TargetRootReadOnly
}

// peerReversedOntoThisSide reports that the peer's end of rel is a
// DISCONNECTED source over this side's DISCONNECTED target: what a reverse
// leaves until the reconnect. A make-writable on this side (a promote or
// a drill) leaves the peer's source end ESTABLISHED (measured, see
// peerPromoted), so an undo of a promote is not this case.
func peerReversedOntoThisSide(rel *relationship) bool {
	return rel.role == roleTarget && rel.local.State == qumulo.RelationshipDisconnected &&
		rel.peer != nil && rel.peerRole == roleSource && rel.peer.State == qumulo.RelationshipDisconnected
}

// peerPromoted reports that the peer's end of rel is a writable, disconnected
// target: the peer ran make-writable (a promote or a drill) while this side
// is the source. Measured live on Core 7.9.2.2 (2026-09-14): after
// make-writable on the target, the SOURCE end keeps reporting ESTABLISHED --
// only the target end says DISCONNECTED -- so a source's own state cannot
// tell "healthy" from "my peer has been promoted". Both ends read
// DISCONNECTED only after a reverse.
func peerPromoted(rel *relationship) bool {
	return rel.peer != nil && rel.peerRole == roleTarget &&
		rel.peer.State == qumulo.RelationshipDisconnected && !rel.peer.TargetRootReadOnly
}

// requestVolumeID is the handle a csi-addons request names. Spec v0.2.0
// carries it in two places: the original volume_id, and
// replication_source.volume.volume_id. The v0.15.0 controller fills only the
// second (measured on the lab cluster, 2026-09-14: every RPC arrived with an
// empty volume_id and the driver answered "volume ID is empty"), so the
// newer field is read first and the legacy one kept for older controllers.
func requestVolumeID(req sourcedReplicationRequest) string {
	if id := req.GetReplicationSource().GetVolume().GetVolumeId(); id != "" {
		return id
	}
	// The deprecated field, read through the interface: csi-addons moved the
	// volume to replication_source, and an older controller still sends it
	// here.
	return req.GetVolumeId()
}

// sourcedReplicationRequest is every csi-addons replication request: each
// names its volume in replication_source and, deprecated, in volume_id.
type sourcedReplicationRequest interface {
	GetVolumeId() string
	GetReplicationSource() *replication.ReplicationSource
}

// ResyncVolume brings the local copy back into step as a target. Returns
// ready=false while the array is still working, and the controller keeps
// calling.
func (r *ReplicationServer) ResyncVolume(
	ctx context.Context,
	req *replication.ResyncVolumeRequest,
) (*replication.ResyncVolumeResponse, error) {
	const rpc = "resyncvolume"
	force := req.GetForce()

	e, err := r.resolve(ctx, rpc, requestVolumeID(req), req.GetReplicationSource().GetVolumegroup().GetVolumeGroupId(),
		req.GetParameters(), req.GetSecrets(), false)
	if err != nil {
		return nil, err
	}
	rel, err := r.findRelationship(ctx, rpc, e, true)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"%s: volume %q has no replication relationship on %s", rpc, e.vol.Name, e.localAddr)
	}
	if rel.peer == nil && rel.peerConsulted {
		// The same shape PromoteVolume refuses: the configured peer answered
		// and holds no end of this relationship. Every row below acts on the
		// relationship -- authorizes it, reconnects it, reverses it -- and
		// with the peer uninvolved that relationship belongs to another
		// array (or the class is mistyped). Authorizing it let that array's
		// data overwrite this volume.
		return nil, unmatchedPeerError(rpc, e, rel,
			"a resync is refused because it would authorize, reconnect or reverse a relationship the peer does not hold")
	}

	switch {
	case rel.role == roleTarget && rel.local.State == qumulo.RelationshipEstablished:
		if err := r.unfreezeAbandonedDrain(ctx, rpc, e, rel); err != nil {
			return nil, err
		}
		// A reconnect's revert runs after the call that started it has
		// returned, so its snapshot is stamped here, on the polls that
		// follow -- and the polls are kept coming until it has been,
		// because the controller stops polling once this says ready.
		root := revertRoot(e, rel)
		reverts := r.expireRevertSnapshots(ctx, rpc, e.local, e.localAddr, root)
		// Ready once a job has completed; before that the replica is empty.
		ready := rel.local.HasRecoveryPoint()
		if ready && r.awaitingRevertSnapshot(ctx, rpc, e.localAddr, root, reverts) {
			ready = false
		}
		return &replication.ResyncVolumeResponse{Ready: ready}, nil

	case rel.role == roleTarget && rel.local.State == qumulo.RelationshipDisconnected:
		// A local make-writable (drill or promote) with the peer still the
		// source, or roles already swapped by a peer reverse. Reconnect
		// discards whatever was written here since the recovery point,
		// so a writable local needs the operator's force.
		if !rel.local.TargetRootReadOnly && !force {
			return nil, status.Errorf(codes.FailedPrecondition,
				"%s: volume %q on %s is writable and a resync discards everything written to it since "+
					"recovery point %s; set force to confirm",
				rpc, e.vol.Name, e.localAddr, rel.local.RecoveryPoint.Format(time.RFC3339))
		}
		if peerReversedOntoThisSide(rel) {
			// A reverse ran on an earlier call -- this side was the source --
			// and that call stopped before its reconnect. The reverse reset
			// the mode and dropped every policy link, so the class's
			// are put back first, as reverseAndReconnect does; reconnected
			// without them, the relationship replicated continuously with no
			// policy while Resync reported ready. The marker this side
			// carried as a source goes too.
			if err := r.reapplyClassConfig(ctx, rpc, e, rel); err != nil {
				return nil, err
			}
			r.dropFreezeMarker(ctx, rpc, e.local, rel.id, e.fileID)
		}
		root := revertRoot(e, rel)
		r.noteReconnect(ctx, rpc, e.local, e.localAddr, root)
		reconnected, err := e.local.Reconnect(ctx, rel.id)
		if err != nil {
			if qumulo.IsAlreadyDisconnected(err) {
				// Reconnect already ran and the array is mid-transition.
				return &replication.ResyncVolumeResponse{Ready: false}, nil
			}
			return nil, unreachableOr(e.localAddr, err, "%s: reconnecting relationship %s on %s", rpc, rel.id, e.localAddr)
		}
		slog.InfoContext(ctx, rpc+": target reconnected; divergent local writes discarded",
			"volume", e.vol.Name, "relationship", rel.id)
		r.awaitingRevertSnapshot(ctx, rpc, e.localAddr, root, r.expireRevertSnapshots(ctx, rpc, e.local, e.localAddr, root))
		if err := r.unfreezeSourceAfterReconnect(ctx, rpc, e, rel, reconnected); err != nil {
			return nil, err
		}
		return &replication.ResyncVolumeResponse{Ready: false}, nil

	case rel.role == roleTarget && rel.local.State == qumulo.RelationshipAwaitingAuthorization &&
		rel.peer != nil && rel.peerRole == roleSource:
		// The peer created a relationship onto this side and it was never
		// authorized here: what rebuildFromPeer leaves when its own Authorize
		// failed once -- a timeout, a 5xx, a restart between the two calls,
		// or a 404 before the new relationship was visible. The retry does
		// not re-enter the rebuild, because the relationship now exists, so
		// this is the only place left to finish it. It used to fall to the
		// default row and answer Ready=false for ever, with nothing
		// replicating.
		return r.authorizeLocalTarget(ctx, rpc, e, rel.id, force || e.params.AllowNonEmpty)

	case rel.role == roleSource && (rel.local.State == qumulo.RelationshipDisconnected || peerPromoted(rel)):
		// The peer promoted. The source end may still say ESTABLISHED (it
		// does on Core 7.9.2.2 until the reverse); the peer end is the
		// evidence.
		return r.reverseAndReconnect(ctx, rpc, e, rel, force)

	case rel.role == roleSource:
		return nil, status.Errorf(codes.FailedPrecondition,
			"%s: volume %q on %s is the %s source; a resync applies to the secondary. Promote the peer first",
			rpc, e.vol.Name, e.localAddr, rel.local.State)

	default:
		// RECONNECTING, REESTABLISHING: the array is working.
		return &replication.ResyncVolumeResponse{Ready: false}, nil
	}
}

// A planned promote freezes the source (drainSource) before it makes this
// side writable. When the DR side is set back to secondary before the old
// primary is reversed, that promote is abandoned or undone: the source stays
// frozen, its own VolumeReplication still reads Primary, and csi-addons never
// calls Promote on an object already Primary, so unfreezeAbandonedPromote
// never runs. No copy anywhere is writable, and a frozen source drops appends
// silently.
//
// The two functions below lift that freeze from the DR side's resync, each
// only on evidence that the freeze is a promote's. Demote freezes the same
// flag -- step 1 of the documented failover -- and a demote's freeze
// must survive the DR side's ordinary resyncs until the promote runs. And
// never blind, the same rule as unfreezeAbandonedPromote: the peer's source
// end must have been read as frozen, and this side must read as a read-only
// target, so the unfreeze leaves exactly one writable copy.

// reconnectSettleReads is how often the target's status is read again after
// a reconnect whose reply was not yet read-only, drainPollWait apart.
const reconnectSettleReads = 5

// unfreezeSourceAfterReconnect: this side was the promoted copy and has just
// been reconnected under its source. The promote itself is being undone, so
// the freeze is its drain's.
func (r *ReplicationServer) unfreezeSourceAfterReconnect(
	ctx context.Context,
	rpc string,
	e *replicationEnds,
	rel *relationship,
	reconnected qumulo.RelationshipStatus,
) error {
	if rel.peer == nil || rel.peerRole != roleSource || !rel.peer.SourceRootReadOnly {
		return nil
	}
	// A demote's freeze is the operator's, and it is held here too: both
	// objects then read Secondary and nothing is writable, which is what
	// they say, and setting the source primary lifts it through its own
	// promote. Only a marker proves that, so a failed lookup holds as well.
	if marker, err := r.findFreezeMarker(ctx, e.peer, rel.id, rel.peer.SourceRootID); err != nil || (marker != nil && isHeldFreezeMarker(*marker, rel.id)) {
		slog.InfoContext(ctx, rpc+": the source was frozen by a demote, not by the promote being undone; leaving the freeze",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "error", err, "remedy", promoteFreezeRemedy)
		return nil
	}
	// The reconnect's own reply may still show the in-between state. Deciding
	// on that one reply alone held the freeze -- a write outage for the
	// primary until someone lifted it by hand -- whenever the array had not
	// settled the instant it answered. The target's status is read
	// again, a few times, before the freeze is held.
	for try := 0; !reconnected.TargetRootReadOnly && try < reconnectSettleReads; try++ {
		select {
		case <-ctx.Done():
			try = reconnectSettleReads
			continue
		case <-time.After(r.drainPollWait):
		}
		if again, err := e.local.GetTargetStatus(ctx, rel.id); err == nil {
			reconnected = again
		}
	}
	if !reconnected.TargetRootReadOnly {
		slog.WarnContext(ctx, rpc+": the source is frozen, but the reconnected target does not read read-only yet; "+
			"the freeze is held rather than lifted beside a copy that may still be writable",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "state", reconnected.State,
			"remedy", promoteFreezeRemedy)
		return nil
	}
	return r.liftPromoteFreeze(ctx, rpc, e, rel, "a promote that was undone")
}

// unfreezeAbandonedDrain: this side never became writable -- the promote's
// drain did not finish and the operator set the DR side back to secondary.
// The drain's freeze marker is still on the source (only a promote that
// completes removes it, and Demote never takes one), and that is what tells
// this freeze from a demote's. A marker older than freezeMarkerTTL has
// expired, and then the freeze is held: the remedy in promoteFreezeRemedy
// still works.
func (r *ReplicationServer) unfreezeAbandonedDrain(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) error {
	if rel.peer == nil || rel.peerRole != roleSource || !rel.peer.SourceRootReadOnly || !rel.local.TargetRootReadOnly {
		return nil
	}
	marker, err := r.findFreezeMarker(ctx, e.peer, rel.id, rel.peer.SourceRootID)
	if err != nil {
		slog.WarnContext(ctx, rpc+": the source is frozen and its freeze marker could not be read; the freeze is held",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "error", err)
		return nil
	}
	if marker == nil || isHeldFreezeMarker(*marker, rel.id) {
		// A demote's freeze -- marked held when Demote or the drain found
		// it -- or a drain's whose marker expired. Held.
		slog.InfoContext(ctx, rpc+": the source is frozen with no promote in evidence; leaving the freeze to the promote",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr)
		return nil
	}
	return r.liftPromoteFreeze(ctx, rpc, e, rel, "an abandoned planned promote")
}

// promoteFreezeRemedy is the operator's way out whenever the freeze is held:
// Promote on a frozen source whose peer reads read-only unfreezes it
// (unfreezeAbandonedPromote), and toggling the object is what makes
// csi-addons call Promote on it again.
const promoteFreezeRemedy = "set the source's VolumeReplication to secondary and back to primary once the DR side " +
	"reads as a read-only target; its promote lifts the freeze"

// liftPromoteFreeze unfreezes the peer's source end, re-applying the class's
// mode in the same PATCH because the drain may have switched a policy-only
// relationship to the combined mode, exactly as unfreezeAbandonedPromote does.
//
// Never under a planned promote of the same relationship still running on
// this controller: the DR side set back to secondary mid-promote found the
// drain's marker and lifted the freeze, and the promote then made the target
// writable beside a writable source. Aborted instead, so the resync
// is retried once the promote has returned and decides on what it left. A
// promote still running on another controller -- the old leader, until its
// deadline -- is not seen here.
func (r *ReplicationServer) liftPromoteFreeze(ctx context.Context, rpc string, e *replicationEnds, rel *relationship, why string) error {
	if r.promotes.inFlight(rel.id) {
		return status.Errorf(codes.Aborted,
			"%s: a promote of relationship %s is still running on this controller; the source's freeze is not "+
				"lifted under it, retrying", rpc, rel.id)
	}
	unfrozen := false
	patch := qumulo.SourcePatch{SourceRootReadOnly: &unfrozen}
	if rel.peer.ReplicationMode != "" && rel.peer.ReplicationMode != e.params.arrayMode() {
		patch.ReplicationMode = e.params.arrayMode()
	}
	if _, err := e.peer.PatchSource(ctx, rel.id, patch); err != nil {
		return unreachableOr(e.peerAddr, err,
			"%s: unfreezing the source of relationship %s on %s, left read-only by %s, failed and it stays "+
				"read-only; %s", rpc, rel.id, e.peerAddr, why, promoteFreezeRemedy)
	}
	r.dropFreezeMarker(ctx, rpc, e.peer, rel.id, rel.peer.SourceRootID)
	slog.WarnContext(ctx, rpc+": unfroze the source left read-only by "+why,
		"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "modeReapplied", patch.ReplicationMode)
	return nil
}

// reverseAndReconnect is the reprotect: the peer, which promoted, becomes the
// source of the same relationship and local reconnects as its target.
//
// Three measured facts shape it. Reverse keeps the ID and leaves both ends
// DISCONNECTED. Reverse resets the mode to continuous and drops every
// policy link, so the class's mode is re-applied and a policy linked on
// the new source straight after. Reconnect discards local's divergent writes,
// so a writable local needs force.
func (r *ReplicationServer) reverseAndReconnect(
	ctx context.Context,
	rpc string,
	e *replicationEnds,
	rel *relationship,
	force bool,
) (*replication.ResyncVolumeResponse, error) {
	if !rel.local.SourceRootReadOnly && !force {
		return nil, status.Errorf(codes.FailedPrecondition,
			"%s: volume %q on %s is a writable disconnected source; a resync reverses the relationship and "+
				"discards everything written here since recovery point %s. Demote it first, or set force",
			rpc, e.vol.Name, e.localAddr, rel.local.RecoveryPoint.Format(time.RFC3339))
	}
	if rel.peer == nil {
		return nil, status.Errorf(codes.Unavailable, "%s: peer %s did not answer; a resync needs both arrays", rpc, e.peerAddr)
	}

	// This side is about to become a replica, and a replica's export is
	// read-only: a pod that reaches it early is refused at mount rather
	// than writing into a directory that drops its appends. CreateVolume made
	// the original primary's export read-write, and nothing else sets it
	// back. Fenced before the reverse, because this side is frozen or its
	// writes are being discarded by consent either way, and a retry then
	// fences it again rather than finding a reconnected target it cannot
	// tell apart. It also keeps "a read-write export marks a promoted copy"
	// true for peerExportMarksPromotedCopy (review of).
	if e.export != nil && !e.export.ReadOnly {
		if err := e.local.SetExportReadOnly(ctx, e.export.ID, true); err != nil {
			return nil, unreachableOr(e.localAddr, err, "%s: setting export %q read-only on %s before it becomes a replica",
				rpc, e.export.ExportPath, e.localAddr)
		}
		slog.InfoContext(ctx, rpc+": export set read-only; this side becomes the replica",
			"volume", e.vol.Name, "export", e.export.ExportPath, "exportID", e.export.ID)
	}

	// The peer may already be the source of this ID: a retry after a
	// reverse that succeeded but whose reconnect did not run. With the same
	// path on both arrays only the list it appears in says so.
	if rel.peerRole != roleSource {
		reversed, err := e.peer.Reverse(ctx, rel.id, e.localAddr, qumulo.ReplicationPort)
		if err != nil {
			// A reply the client could not decode is not a refusal: the
			// reverse may have happened, and the rebuild below tears the
			// relationship down. Internal, retried as it is.
			if isUnreachable(err) || qumulo.IsMalformedReply(err) {
				return nil, unreachableOr(e.peerAddr, err, "%s: reversing relationship %s on %s", rpc, rel.id, e.peerAddr)
			}
			if !force {
				return nil, status.Errorf(codes.FailedPrecondition,
					"%s: peer %s refused to reverse relationship %s: %v. Local recovery point %s, peer recovery point %s. "+
						"Set force to rebuild the relationship from the peer, overwriting this copy",
					rpc, e.peerAddr, rel.id, err, rel.local.RecoveryPoint.Format(time.RFC3339),
					rel.peer.RecoveryPoint.Format(time.RFC3339))
			}
			return r.rebuildFromPeer(ctx, rpc, e, rel)
		}
		slog.InfoContext(ctx, rpc+": relationship reversed; peer is now the source",
			"volume", e.vol.Name, "relationship", reversed.ID, "modeAfterReverse", reversed.ReplicationMode,
			"policiesAfterReverse", len(reversed.SnapshotPolicies))
	}

	if err := r.reapplyClassConfig(ctx, rpc, e, rel); err != nil {
		return nil, err
	}

	// The freeze this side carried as a source ends with the reverse; its
	// marker snapshot goes with it (it would compare stale anyway).
	r.dropFreezeMarker(ctx, rpc, e.local, rel.id, rel.local.SourceRootID)
	root := revertRoot(e, rel)
	r.noteReconnect(ctx, rpc, e.local, e.localAddr, root)
	if _, err := e.local.Reconnect(ctx, rel.id); err != nil && !qumulo.IsAlreadyDisconnected(err) {
		return nil, unreachableOr(e.localAddr, err, "%s: reconnecting relationship %s on %s", rpc, rel.id, e.localAddr)
	}
	slog.InfoContext(ctx, rpc+": local reconnected as the target; its divergent writes are discarded",
		"volume", e.vol.Name, "relationship", rel.id)
	r.awaitingRevertSnapshot(ctx, rpc, e.localAddr, root, r.expireRevertSnapshots(ctx, rpc, e.local, e.localAddr, root))
	return &replication.ResyncVolumeResponse{Ready: false}, nil
}

// reapplyClassConfig puts the class's mode back on a reversed relationship
// and links a csi-<volume> policy on the new source, recreating the policy
// there from the old source's schedule when it does not exist.
func (r *ReplicationServer) reapplyClassConfig(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) error {
	patch := qumulo.SourcePatch{ReplicationMode: e.params.arrayMode()}
	if e.params.LinkSnapshotPolicy {
		link, err := r.policyLink(ctx, rpc, e.peer, e.vol.Name)
		if err != nil {
			return unreachableOr(e.peerAddr, err, "%s: listing snapshot policies on %s", rpc, e.peerAddr)
		}
		if link == nil {
			link, err = r.copyPolicy(ctx, rpc, e, rel)
			if err != nil {
				return err
			}
		}
		links := []qumulo.SnapshotPolicyLink{}
		if link != nil {
			links = append(links, *link)
		}
		patch.SnapshotPolicies = &links
	}
	if _, err := e.peer.PatchSource(ctx, rel.id, patch); err != nil {
		return unreachableOr(e.peerAddr, err, "%s: re-applying the class mode to relationship %s on %s", rpc, rel.id, e.peerAddr)
	}
	slog.InfoContext(ctx, rpc+": class mode and policy re-applied after reverse",
		"volume", e.vol.Name, "relationship", rel.id, "mode", patch.ReplicationMode,
		"policyLinked", patch.SnapshotPolicies != nil && len(*patch.SnapshotPolicies) > 0)
	return nil
}

// copyPolicy recreates the volume's snapshot policy on the peer, from the
// schedule of the one on local, against the peer's copy of the directory.
func (r *ReplicationServer) copyPolicy(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) (*qumulo.SnapshotPolicyLink, error) {
	name := policyNameFor(e.vol.Name)
	original, err := r.policyLink(ctx, rpc, e.local, e.vol.Name)
	if err != nil {
		return nil, unreachableOr(e.localAddr, err, "%s: listing snapshot policies on %s", rpc, e.localAddr)
	}
	if original == nil {
		slog.WarnContext(ctx, rpc+": no snapshot policy to link on the new source and none to copy from the old; "+
			"policy snapshots will not replicate", "volume", e.vol.Name, "policy", name)
		return nil, nil
	}
	schedule, err := e.local.GetPolicySchedule(ctx, original.ID)
	if err != nil {
		return nil, unreachableOr(e.localAddr, err, "%s: reading the schedule of policy %s on %s", rpc, original.ID, e.localAddr)
	}
	replica, err := e.peer.LookupFileByPath(ctx, e.peerPath)
	if err != nil {
		return nil, unreachableOr(e.peerAddr, err, "%s: looking up %q on %s", rpc, e.peerPath, e.peerAddr)
	}
	created, err := e.peer.CreatePolicy(ctx, name, replica.ID, schedule)
	if err != nil && !qumulo.IsConflict(err) {
		return nil, unreachableOr(e.peerAddr, err, "%s: creating policy %q on %s", rpc, name, e.peerAddr)
	}
	if err != nil {
		return r.policyLink(ctx, rpc, e.peer, e.vol.Name)
	}
	slog.InfoContext(ctx, rpc+": snapshot policy recreated on the new source",
		"volume", e.vol.Name, "policy", name, "policyID", created.ID, "relationship", rel.id)
	return &qumulo.SnapshotPolicyLink{ID: created.ID, TargetExpiration: qumulo.TargetExpirationSameAsPolicy}, nil
}

// policyLink finds the volume's csi-<name> policy on an array, as a link.
func (r *ReplicationServer) policyLink(ctx context.Context, rpc string, client qumulo.ClientInterface, volumeName string) (*qumulo.SnapshotPolicyLink, error) {
	p, err := findPolicyNamed(ctx, client, policyNameFor(volumeName))
	if err != nil || p == nil {
		return nil, err
	}
	return &qumulo.SnapshotPolicyLink{ID: p.ID, TargetExpiration: qumulo.TargetExpirationSameAsPolicy}, nil
}

// rebuildFromPeer is the fallback when a reverse is refused: tear the
// relationship down on both arrays and create a new one from the peer toward
// local, adopting local's non-empty directory. Local is overwritten. Same
// outcome as reverse plus reconnect, more steps, new relationship ID.
func (r *ReplicationServer) rebuildFromPeer(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) (*replication.ResyncVolumeResponse, error) {
	slog.WarnContext(ctx, rpc+": reverse refused; rebuilding the relationship from the peer (forced)",
		"volume", e.vol.Name, "relationship", rel.id)
	if err := r.deleteBothEnds(ctx, rpc, e, rel); err != nil {
		return nil, err
	}
	spec := qumulo.SourceSpec{
		SourceRootPath:     e.peerPath,
		TargetRootPath:     e.realPath,
		TargetAddress:      e.localAddr,
		TargetPort:         qumulo.ReplicationPort,
		ReplicationMode:    e.params.arrayMode(),
		ReplicationEnabled: true,
	}
	created, err := e.peer.CreateSource(ctx, spec)
	id := created.ID
	switch {
	case err == nil:
	case qumulo.IsConflict(err):
		// An earlier attempt created it. CreateSource answers nothing on a
		// conflict, so the ID is read back from the peer rather than taken
		// from an empty response.
		if id, err = sourceRelationshipAt(ctx, e.peer, e.peerPath); err != nil {
			return nil, unreachableOr(e.peerAddr, err, "%s: listing source relationships on %s", rpc, e.peerAddr)
		}
		if id == "" {
			return nil, status.Errorf(codes.Aborted,
				"%s: %s refused to create a relationship from %q as a duplicate, but lists none; retrying",
				rpc, e.peerAddr, e.peerPath)
		}
	default:
		return nil, unreachableOr(e.peerAddr, err, "%s: creating the relationship on %s", rpc, e.peerAddr)
	}
	return r.authorizeLocalTarget(ctx, rpc, e, id, true)
}

// authorizeLocalTarget authorizes, on this side, a relationship the peer
// sources onto it: the last step of rebuildFromPeer, and ResyncVolume's
// remedy when that step failed on an earlier attempt. allowNonEmpty
// is the consent to overwrite what this side holds, which a forced resync
// carries.
func (r *ReplicationServer) authorizeLocalTarget(
	ctx context.Context,
	rpc string,
	e *replicationEnds,
	id string,
	allowNonEmpty bool,
) (*replication.ResyncVolumeResponse, error) {
	_, err := e.local.Authorize(ctx, id, true, allowNonEmpty)
	switch {
	case err == nil:
		slog.InfoContext(ctx, rpc+": relationship from the peer authorized on this side",
			"volume", e.vol.Name, "relationship", id, "peer", e.peerAddr)
	case qumulo.IsAlreadyAuthorized(err):
		// Authorized on an earlier call; the state has not caught up yet.
	case qumulo.IsNotFound(err):
		return nil, status.Errorf(codes.Aborted, "%s: relationship %s not yet visible on %s; retrying", rpc, id, e.localAddr)
	default:
		return nil, unreachableOr(e.localAddr, err, "%s: authorizing relationship %s on %s", rpc, id, e.localAddr)
	}
	return &replication.ResyncVolumeResponse{Ready: false}, nil
}

// sourceRelationshipAt returns the ID of the live source relationship an
// array has rooted at rootPath, or "" when it has none.
func sourceRelationshipAt(ctx context.Context, view qumulo.ReplicationClient, rootPath string) (string, error) {
	sources, err := view.ListSourceStatus(ctx)
	if err != nil {
		return "", err
	}
	for _, src := range sources {
		if src.State != qumulo.RelationshipEnded && path.Clean(src.SourceRootPath) == path.Clean(rootPath) {
			return src.ID, nil
		}
	}
	return "", nil
}

// ReplicationHealth is the spec's health assessment of one relationship.
//
// csi-addons spec v0.2.0 carries only last_sync_time on the wire, so the
// rest of what the spec's Info table computes is exposed here for the log,
// the metrics work and tests, and will map onto the response fields when
// the vendored spec grows them.
type ReplicationHealth struct {
	// Status is HEALTHY, DEGRADED, ERROR or UNKNOWN.
	Status  string
	Message string
	// LastSyncTime is the recovery point; zero when there is none.
	LastSyncTime     time.Time
	LastSyncDuration time.Duration
	LastSyncBytes    uint64
	// Lag is the recovery point's age by the local array's own clock.
	Lag time.Duration
	// RelationshipID and Role identify what was assessed.
	RelationshipID string
	Role           string
}

// Health statuses, in csi-addons' vocabulary.
const (
	ReplicationHealthy  = "HEALTHY"
	ReplicationDegraded = "DEGRADED"
	ReplicationError    = "ERROR"
	ReplicationUnknown  = "UNKNOWN"
)

// GetVolumeReplicationInfo reports the relationship's last sync time. The
// full assessment is computed by AssessVolumeReplication and logged.
func (r *ReplicationServer) GetVolumeReplicationInfo(
	ctx context.Context,
	req *replication.GetVolumeReplicationInfoRequest,
) (*replication.GetVolumeReplicationInfoResponse, error) {
	// The Info request carries no class parameters, only the handle and the
	// secrets (in spec v0.2.0 and in the spec csi-addons v0.15.0 builds on). The assessment therefore
	// knows neither the class's mode nor its scheduling interval: it grades
	// state, job error, queued snapshots and split brain, says the interval
	// is unknown, and finds the peer from the relationship status; see
	// AssessVolumeReplication. Group scope can still be reached here,
	// through replication_source -- the one field of the Info request that
	// carries anything beyond the handle.
	health, err := r.AssessVolumeReplication(ctx, requestVolumeID(req),
		req.GetReplicationSource().GetVolumegroup().GetVolumeGroupId(), nil, req.GetSecrets())
	if err != nil {
		return nil, err
	}
	// The whole assessment, not only the sync time. csi-addons sets
	// the VolumeReplication's Replicating condition from status, which spec
	// v0.2.0 had no field for, so the grade reached only this driver's log
	// and the condition read Unknown for ever.
	resp := &replication.GetVolumeReplicationInfoResponse{
		Status:        replicationStatusOf(health.Status),
		StatusMessage: health.Message,
		LastSyncBytes: clampToInt64(health.LastSyncBytes),
	}
	if !health.LastSyncTime.IsZero() {
		resp.LastSyncTime = timestamppb.New(health.LastSyncTime)
	}
	if health.LastSyncDuration > 0 {
		resp.LastSyncDuration = durationpb.New(health.LastSyncDuration)
	}
	return resp, nil
}

// replicationStatusOf maps the assessment's grade to the csi-addons enum.
func replicationStatusOf(grade string) replication.GetVolumeReplicationInfoResponse_Status {
	switch grade {
	case ReplicationHealthy:
		return replication.GetVolumeReplicationInfoResponse_HEALTHY
	case ReplicationDegraded:
		return replication.GetVolumeReplicationInfoResponse_DEGRADED
	case ReplicationError:
		return replication.GetVolumeReplicationInfoResponse_ERROR
	}
	return replication.GetVolumeReplicationInfoResponse_UNKNOWN
}

// AssessVolumeReplication is the spec's GetVolumeReplicationInfo table.
//
// HEALTHY requires all of: ESTABLISHED, no job error, a recovery point whose
// age by the local array's own clock is under the class interval, the peer
// reachable, and the mode and policy link matching the class. State alone is
// never HEALTHY: a lab run showed ESTABLISHED with no error for 25 minutes
// while the source was powered off. Two writable copies is ERROR
// whatever else is true.
//
// The lag verdict needs the class's interval, and params is nil when the
// caller is the gRPC Info, which carries no parameters in csi-addons spec
// v0.2.0. Grading lag against a default the request never stated made every
// policy-mode volume on a 15 m or 1 h schedule DEGRADED then ERROR on every
// poll. With no parameters the lag is reported, not graded, and
// the message says the interval is unknown; the other inputs -- state, job
// error, queued snapshots, split brain, peer reachability -- are graded as
// before. The metrics exporter computes age from the recovery point on its
// own, so nothing downstream loses the number.
func (r *ReplicationServer) AssessVolumeReplication(
	ctx context.Context,
	volumeID, groupID string,
	params, secrets map[string]string,
) (ReplicationHealth, error) {
	// Label the work with the volume it is about, so the protection verdict
	// below has a handle to be exported against. Without this the event
	// belongs to no volume and the exporter drops it.
	ctx = qumulo.WithSubject(ctx, volumeID)
	ctx = withReportLookups(ctx)
	const rpc = "getvolumereplicationinfo"

	e, err := r.resolve(ctx, rpc, volumeID, groupID, params, secrets, true)
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			return ReplicationHealth{Status: ReplicationUnknown, Message: err.Error()}, nil
		}
		return ReplicationHealth{}, err
	}

	// The peer's reachability is a health input, not a precondition, so
	// the match is made without it and the peer is asked separately.
	rel, err := r.findRelationship(ctx, rpc, e, false)
	if err != nil {
		return ReplicationHealth{}, err
	}

	// memberVol and memberAddr identify the volume this report is about,
	// kept because e is reassigned to the group's ends below and the
	// protection fan-out matches members against the volume's own array.
	// groupRoot is set once a group's relationship has been found to
	// cover it.
	memberVol, memberAddr := e.vol, e.localAddr
	memberFanoutKey := memberAddr + "\x00" + e.realPath
	groupRoot := ""

	// A member of a group has no relationship of its own: the group's is
	// rooted at the class's basePath, above it. The Info request carries no
	// class parameters and, from the stock sidecar, no group ID, so resolve
	// never entered group scope and the member read as unprotected -- the
	// group's VolumeReplication reported "status unknown" and the protected
	// gauge 0 while the relationship was ESTABLISHED. When exactly one
	// live relationship covers the volume from above, assess that one.
	if rel == nil && groupID == "" {
		root, found, err := r.coveringGroupRootFor(ctx, rpc, e)
		if err != nil {
			return ReplicationHealth{}, err
		}
		if found {
			ge, err := r.resolve(ctx, rpc, volumeID, root, params, secrets, true)
			if err != nil {
				if status.Code(err) == codes.Unavailable {
					return ReplicationHealth{Status: ReplicationUnknown, Message: err.Error()}, nil
				}
				return ReplicationHealth{}, err
			}
			grel, err := r.findRelationship(ctx, rpc, ge, false)
			if err != nil {
				return ReplicationHealth{}, err
			}
			memberKey := e.localAddr + "\x00" + e.realPath
			if grel != nil {
				// The member has none of its own and its group's covers it:
				// the next report need not look for one at the member,
				// for the full reportHintReuse. A plain volume's "none" is
				// remembered for noRelationshipReuse only, so a relationship
				// enabled from another controller is seen within minutes.
				r.hints.relationships.Store(memberKey, noRelationshipHint(reportHintReuse))
				groupRoot = root
				e, rel = ge, grel
			} else {
				r.hints.groupRoots.Delete(memberKey)
				r.hints.relationships.Delete(memberKey)
			}
		}
	}

	// Say so on the way out, whichever way it went. An absent relationship has
	// no relationship ID to hang a series on, and a series that merely stops
	// being reported is not alertable without knowing the ID in advance -- so
	// this is keyed by the volume instead. It is also the only trustworthy
	// signal for the csi-addons v0.15.0 defect where a Completed primary keeps
	// reading Healthy after the relationship is removed on the arrays.
	//
	// An ENDED end protects nothing: the other array deleted its half. A lone
	// one is still returned by findRelationship (it drops ENDED ends only
	// beside a live one), and reading it as protected kept
	// QumuloCSIVolumeUnprotected silent.
	protected := rel != nil && rel.local.State != qumulo.RelationshipEnded
	e.local.ObserveVolumeProtection(ctx, protected)

	// And for the group's other members, which no Info request ever names.
	// Both directions matter: a group that is protected gives every
	// member a 1, and a group whose relationship has gone sets to 0 the
	// members the last report covered -- the second is the case the alert
	// exists for, and it has no root of its own to find, so it uses the one
	// that report remembered.
	switch {
	case groupRoot != "":
		r.rememberGroupFanout(memberFanoutKey, groupRoot)
		r.observeGroupProtection(ctx, e.local, memberAddr, memberVol, groupRoot, protected)
	case !protected:
		if root, ok := r.lastGroupFanoutRoot(memberFanoutKey); ok {
			r.observeGroupProtection(ctx, e.local, memberAddr, memberVol, root, false)
		}
	}

	if rel == nil {
		return ReplicationHealth{}, status.Errorf(codes.FailedPrecondition,
			"%s: volume %q has no replication relationship on %s", rpc, e.vol.Name, e.localAddr)
	}

	// With no class in the request the peer is identified from the status.
	// Only the source end reports the peer's address; the target end
	// reports null while connected (measured), so from there the peer goes
	// unconsulted and the split-brain check cannot run.
	if e.peer == nil && rel.role == roleSource && rel.local.TargetAddress != "" {
		if token := firstNonEmpty(secrets[rel.local.TargetAddress]); token != "" {
			if peer, err := r.clientFor(apiURLFor(rel.local.TargetAddress, ""), token); err == nil {
				e.peer, e.peerAddr, e.peerPath = peer, rel.local.TargetAddress, rel.local.TargetRootPath
			}
		}
	}
	peerReachable := true
	peerConsulted := e.peer != nil
	if peerConsulted {
		if peerSide, err := r.peerStatusesFor(ctx, rpc, e, rel.id); err != nil {
			peerReachable = false
		} else if ps, ok := peerSide[rel.id]; ok {
			rel.peer, rel.peerRole = &ps.status, ps.role
		}
	}

	r.reportPolicyInterval(ctx, e, rel)

	now, err := e.local.ClusterTime(ctx)
	if err != nil {
		return ReplicationHealth{}, unreachableOr(e.localAddr, err, "%s: reading the clock of %s", rpc, e.localAddr)
	}

	h := ReplicationHealth{
		RelationshipID:   rel.id,
		Role:             rel.role.String(),
		LastSyncTime:     rel.local.RecoveryPoint,
		LastSyncDuration: rel.local.DurationOfLastJob,
		LastSyncBytes:    rel.local.BytesTransferred,
	}
	if rel.local.HasRecoveryPoint() {
		h.Lag = now.Sub(rel.local.RecoveryPoint)
	}

	var degraded, failed []string
	st := rel.local.State
	switch {
	case st == qumulo.RelationshipEstablished:
	case st == qumulo.RelationshipReconnecting, st == qumulo.RelationshipReestablishing, st == qumulo.RelationshipDisconnecting:
		degraded = append(degraded, "state "+st)
	case st == qumulo.RelationshipDisconnected && rel.role == roleSource && peerIsReadOnlyEstablishedTarget(rel):
		// The ends catch up a few seconds after a reconnect; the
		// target end is the one that is right, and it is healthy.
		degraded = append(degraded, "state DISCONNECTED on this end while the peer's target end is ESTABLISHED (ends catching up after a reconnect)")
	default:
		failed = append(failed, "state "+st)
	}
	if rel.local.ErrorFromLastJob != "" {
		if st == qumulo.RelationshipEstablished {
			degraded = append(degraded, "last job: "+withLocalOwnershipRemedy(rel.local.ErrorFromLastJob))
		} else {
			failed = append(failed, "last job: "+withLocalOwnershipRemedy(rel.local.ErrorFromLastJob))
		}
	}
	interval := e.params.SchedulingInterval
	intervalKnown := params != nil
	var notes []string
	switch {
	case !rel.local.HasRecoveryPoint():
		degraded = append(degraded, "initial sync: no recovery point yet")
	case !rel.local.NextActiveTime.IsZero() && now.Before(rel.local.NextActiveTime):
		// Inside a blackout window: lag is expected.
	case !intervalKnown:
		notes = append(notes, fmt.Sprintf("recovery point %s old; scheduling interval not in the request, lag not graded",
			h.Lag.Round(time.Second)))
	case h.Lag > 3*interval:
		failed = append(failed, fmt.Sprintf("recovery point %s old, over three intervals of %s", h.Lag.Round(time.Second), interval))
	case h.Lag > interval:
		degraded = append(degraded, fmt.Sprintf("recovery point %s old, over the interval of %s", h.Lag.Round(time.Second), interval))
	}
	if rel.local.QueuedSnapshotCount > 0 {
		degraded = append(degraded, fmt.Sprintf("%d snapshots queued", rel.local.QueuedSnapshotCount))
	}
	if !peerReachable {
		degraded = append(degraded, "peer "+e.peerAddr+" unreachable")
	}
	if !peerConsulted {
		slog.DebugContext(ctx, rpc+": peer not identifiable from this end; split-brain and drift checks skipped",
			"volume", e.vol.Name, "relationship", rel.id, "role", rel.role.String())
	}
	// Mode and policy drift, visible on the source end only, and only when
	// the request said what the class wants: GetVolumeReplicationInfo in
	// spec v0.2.0 carries no parameters, so Info on a policy-mode
	// relationship reported "class wants REPLICATION_CONTINUOUS" from the
	// default (measured on the lab cluster, 2026-09-14).
	if rel.role == roleSource {
		if e.params.ModeExplicit && rel.local.ReplicationMode != e.params.arrayMode() {
			degraded = append(degraded, fmt.Sprintf("mode %s, class wants %s", rel.local.ReplicationMode, e.params.arrayMode()))
		}
		if e.params.LinkSnapshotPolicy && len(rel.local.SnapshotPolicies) == 0 {
			degraded = append(degraded, "no snapshot policy linked")
		}
	}
	// Both copies writable: split brain, always ERROR.
	if rel.peer != nil {
		switch rel.role {
		case roleSource:
			if !rel.local.SourceRootReadOnly && !rel.peer.TargetRootReadOnly {
				failed = append(failed, "both copies are writable (split brain)")
			}
		case roleTarget:
			if !rel.local.TargetRootReadOnly && !rel.peer.SourceRootReadOnly {
				failed = append(failed, "both copies are writable (split brain)")
			}
		}
	}
	// Pre-staged export and quota drift, checked from the source end. Not
	// against a promoted peer: that is the live copy, this side is the stale
	// old primary, and nothing may be staged onto it -- the late-member
	// quota staging wrote this side's limits onto it -- nor is its
	// state drift from this side's.
	if rel.role == roleSource && rel.peer != nil && !peerPromoted(rel) {
		if e.scope == scopeGroup {
			r.stageLateMemberQuotas(ctx, rpc, e, rel.id)
		}
		degraded = append(degraded, r.prestageDrift(ctx, e, rel)...)
	}

	switch {
	case len(failed) > 0:
		h.Status, h.Message = ReplicationError, strings.Join(append(failed, degraded...), "; ")
	case len(degraded) > 0:
		h.Status, h.Message = ReplicationDegraded, strings.Join(degraded, "; ")
	default:
		h.Status, h.Message = ReplicationHealthy, st
		if !rel.local.JobIdle() {
			h.Message = fmt.Sprintf("%s, job %.0f%% complete", st, rel.local.PercentComplete)
		}
	}
	if len(notes) > 0 {
		h.Message = strings.Join(append([]string{h.Message}, notes...), "; ")
	}

	r.logAssessment(ctx, rpc, e.vol.Name, rel.id, h)
	return h, nil
}

// logAssessment logs an assessment at Info when the relationship's status
// differs from the one last logged, and at Debug otherwise. At Info on every
// poll it was one line per VolumeReplication per requeue -- about 72,000 a
// day for 50 volumes polled each minute -- saying the same thing.
func (r *ReplicationServer) logAssessment(ctx context.Context, rpc, volume, relationshipID string, h ReplicationHealth) {
	level := slog.LevelDebug
	if previous, ok := r.assessed.Swap(relationshipID, h.Status); !ok || previous != h.Status {
		level = slog.LevelInfo
	}
	slog.Log(ctx, level, rpc+": relationship assessed",
		"volume", volume, "relationship", relationshipID, "role", h.Role, "status", h.Status,
		"lag", h.Lag.Round(time.Second), "message", h.Message)
}

// fenceOwnReplicaExport sets a read-write replica export read-only, if this
// driver made it and the peer's end is a live, read-only replica, and returns
// the drift to report for it: "" when it was fenced, or when the end turned
// out not to be a replica.
//
// Its callers have already established that this side is the source and the
// peer was not promoted when its status was read, so the copy behind the
// export was a replica. Replica exports this driver made before an upgrade,
// or that it adopted as it found them, stayed read-write; an earlier build graded
// every such pair DEGRADED on every poll while nothing ever fixed it, because
// only Enable fenced and csi-addons does not re-enable a healthy primary.
// Info now fixes the driver's own export once, and leaves an
// operator's to the operator.
//
// The decision is not taken from rel.peer: that status was read several
// round trips earlier, and a promote of the DR side since then made the
// export read-write on purpose. Fencing on it set the live copy's export
// read-only, and nothing undid it. fenceIfStillReplica reads the end
// before and after the write. An end that is no longer a read-only replica
// is the promoted copy, which a poll leaves alone as it does a peer already
// promoted, so it is not drift either.
func (r *ReplicationServer) fenceOwnReplicaExport(
	ctx context.Context, e *replicationEnds, rel *relationship, peerExport *qumulo.ExportInfo,
) string {
	const rpc = "replication status"
	if !madeByThisDriver(peerExport) {
		// A replica's export is read-only until a promote. One this
		// driver did not make -- a group's covering export -- is the
		// operator's to change.
		return fmt.Sprintf("replica export %q on %s is read-write and not this driver's; set it read-only",
			peerExport.ExportPath, e.peerAddr)
	}
	fence, end, err := fenceIfStillReplica(ctx, rpc, e, rel.id, peerExport, nil)
	switch fence {
	case fenceUnread:
		return fmt.Sprintf("replica export %q on %s is read-write and the target end could not be read to fence it: %v",
			peerExport.ExportPath, e.peerAddr, err)
	case fenceWriteFailed:
		slog.WarnContext(ctx, rpc+": could not set the replica's read-write export read-only; reported as drift",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "export", peerExport.ExportPath, "error", err)
		return fmt.Sprintf("replica export %q on %s is read-write and could not be set read-only: %v",
			peerExport.ExportPath, e.peerAddr, err)
	case fenceUndoFailed:
		return fmt.Sprintf("replica export %q on %s was set read-only as its target end was promoted, and could not be "+
			"set read-write again; set it read-write on %s", peerExport.ExportPath, e.peerAddr, e.peerAddr)
	case fenceKept:
		slog.WarnContext(ctx, rpc+": replica export was read-write; set read-only, as a replica's export is until a promote",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "export", peerExport.ExportPath, "exportID", peerExport.ID)
	case fenceNotReplica:
		slog.DebugContext(ctx, rpc+": replica export is read-write over a target end that is no longer a read-only replica; left as it is",
			"volume", e.vol.Name, "relationship", rel.id, "peer", e.peerAddr, "export", peerExport.ExportPath,
			"state", end.State, "targetRootReadOnly", end.TargetRootReadOnly)
	}
	// fenceUndone is logged by fenceIfStillReplica.
	return ""
}

// prestageDrift compares the peer's export and quota with the source's.
func (r *ReplicationServer) prestageDrift(ctx context.Context, e *replicationEnds, rel *relationship) []string {
	var drift []string
	if e.export != nil {
		// A lab build can hold the poll here, between its read of the peer's
		// status and its read of the peer's export, to force the fence race.
		pauseBeforeInfoExportRead(ctx, e.vol.Name)
		peerExport, err := findExportByFSPath(ctx, e.peer, e.peerPath)
		switch {
		case err != nil:
			drift = append(drift, "peer exports unreadable: "+err.Error())
		case peerExport == nil:
			drift = append(drift, "replica export missing on "+e.peerAddr)
		case peerExport.ExportPath != e.export.ExportPath:
			drift = append(drift, fmt.Sprintf("replica export path %q, source %q", peerExport.ExportPath, e.export.ExportPath))
		case !peerExport.ReadOnly:
			// Fenced now, or reported.
			if d := r.fenceOwnReplicaExport(ctx, e, rel, peerExport); d != "" {
				drift = append(drift, d)
			}
		}
	}
	if rel.peer != nil && rel.peer.TargetRootID != "" {
		limit, err := e.local.GetQuota(ctx, e.fileID)
		if err == nil {
			peerLimit, err := e.peer.GetQuota(ctx, rel.peer.TargetRootID)
			switch {
			case qumulo.IsNotFound(err):
				drift = append(drift, "replica quota missing on "+e.peerAddr)
			case err != nil:
				drift = append(drift, "replica quota unreadable: "+err.Error())
			case peerLimit < limit:
				// Below only: a replica larger than its source is harmless
				// (replication ignores its quota), and raiseQuota never
				// lowers one, so reporting it would be drift nothing fixes.
				drift = append(drift, fmt.Sprintf("replica quota %d, below the source's %d", peerLimit, limit))
			}
		}
	}
	return drift
}

// ReplicationDestination is where a volume's replica lives, in the terms a
// DR-side PersistentVolume needs.
type ReplicationDestination struct {
	// VolumeID is the handle a PV on the DR cluster carries:
	// <targetServer>#<replicaPath>. The replica's file ID is reported
	// separately below; it is no longer part of the handle.
	VolumeID string
	Server   string
	RealPath string
	FileID   string
}

// ResolveReplicationDestination resolves the replica on the peer.
//
// Not a gRPC method. It is what a `qumulo-csi-repoint mirror` tool would call
// to write DR-side PV manifests, and what the live test uses to build the
// replica PV it mounts. It was named GetReplicationDestinationInfo until the
// csi-addons spec gained an RPC of that name with another signature;
// the driver does not serve that RPC, which answers Unimplemented.
func (r *ReplicationServer) ResolveReplicationDestination(
	ctx context.Context,
	handle, groupID string,
	params, secrets map[string]string,
) (ReplicationDestination, error) {
	const rpc = "getreplicationdestinationinfo"

	e, err := r.resolve(ctx, rpc, handle, groupID, params, secrets, false)
	if err != nil {
		return ReplicationDestination{}, err
	}
	rel, err := r.findRelationship(ctx, rpc, e, true)
	if err != nil {
		return ReplicationDestination{}, err
	}
	if rel == nil || !rel.local.HasRecoveryPoint() {
		return ReplicationDestination{}, status.Errorf(codes.Unavailable,
			"%s: volume %q has not completed a first replication to %s yet", rpc, e.vol.Name, e.peerAddr)
	}
	replica, err := e.peer.LookupFileByPath(ctx, e.peerPath)
	if err != nil {
		if qumulo.IsNotFound(err) {
			return ReplicationDestination{}, status.Errorf(codes.Unavailable,
				"%s: %q does not exist on %s yet", rpc, e.peerPath, e.peerAddr)
		}
		return ReplicationDestination{}, unreachableOr(e.peerAddr, err, "%s: looking up %q on %s", rpc, e.peerPath, e.peerAddr)
	}
	// The handle is the peer's address and the replica's PATH. It used to
	// carry the replica's file ID, which is the one thing about the DR copy
	// that is not portable -- and building it was the whole reason this call
	// existed for `qumulo-csi-repoint mirror`. The lookup stays because it is
	// what proves the replica directory is actually there before a PV is
	// written pointing at it, and FileID below is still reported for the
	// caller's own use.
	// The endpoint in the handle is the PEER's, not this array's: the handle
	// names a volume on the peer, and a copy of the local endpoint would send
	// that volume's DeleteVolume and ControllerExpandVolume back to the array
	// it was replicated away from. The class's targetApiUrl is the peer's
	// REST address for exactly this reason, and it is required whenever the
	// peer is named by a DNS name that could move.
	return ReplicationDestination{
		VolumeID: newVolumeID(e.peerAddr, apiURLFor(e.peerAddr, e.params.TargetAPIURL), e.peerPath).String(),
		Server:   e.peerAddr,
		RealPath: e.peerPath,
		FileID:   replica.ID,
	}, nil
}

// uncoveredState is the answer for a relationship in a state none of the
// tables cover: Aborted, so the controller retries, and a Warn with the raw
// state so an operator can see what the array is doing.
func uncoveredState(ctx context.Context, rpc string, e *replicationEnds, rel *relationship) error {
	slog.WarnContext(ctx, rpc+": relationship is in a state this RPC has no row for; asking the controller to retry",
		"volume", e.vol.Name, "relationship", rel.id, "role", rel.role.String(), "state", rel.local.State,
		"sourceReadOnly", rel.local.SourceRootReadOnly, "targetReadOnly", rel.local.TargetRootReadOnly)
	return status.Errorf(codes.Aborted, "%s: relationship %s is %s as %s on %s; retrying",
		rpc, rel.id, rel.local.State, rel.role, e.localAddr)
}

// isUnreachable reports whether err is a transport failure rather than an
// answer from the array. An array that answered with an error -- 401, 403,
// 500 -- is reachable; one that never answered is not. So is one whose reply
// this driver could not use: a body that is not the JSON asked for, or an
// export with a null restriction. Classed as unreachable, that sent operators
// to the network for a healthy array, and in a promote it stood in for a
// silent peer, which licenses skipping the freeze.
func isUnreachable(err error) bool {
	var apiErr *qumulo.APIError
	return err != nil && !errors.As(err, &apiErr) && !qumulo.IsMalformedReply(err)
}

// unreachableOr wraps an array error as Unavailable when the array did not
// answer and as Internal when it did, saying so when its reply was one this
// driver cannot use.
func unreachableOr(addr string, err error, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	switch {
	case isUnreachable(err):
		return status.Errorf(codes.Unavailable, "%s: array %s unreachable: %v", msg, addr, err)
	case qumulo.IsMalformedReply(err):
		return status.Errorf(codes.Internal, "%s: array %s answered with a reply this driver cannot use: %v", msg, addr, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", msg, err)
}

// firstNonEmpty returns the first non-empty string.
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}

// ReplicationIdentityServer implements the csi-addons identity service, which
// the sidecar calls once to learn that this driver serves replication.
type ReplicationIdentityServer struct {
	identity.UnimplementedIdentityServer
	driver *Driver
}

// NewReplicationIdentityServer creates the csi-addons identity service.
func NewReplicationIdentityServer(driver *Driver) *ReplicationIdentityServer {
	return &ReplicationIdentityServer{driver: driver}
}

// GetIdentity returns the driver's name and version, as the CSI identity does.
func (s *ReplicationIdentityServer) GetIdentity(ctx context.Context, req *identity.GetIdentityRequest) (*identity.GetIdentityResponse, error) {
	return &identity.GetIdentityResponse{Name: s.driver.Name, VendorVersion: s.driver.Version}, nil
}

// GetCapabilities advertises the controller service and volume replication.
func (s *ReplicationIdentityServer) GetCapabilities(ctx context.Context, req *identity.GetCapabilitiesRequest) (*identity.GetCapabilitiesResponse, error) {
	return &identity.GetCapabilitiesResponse{Capabilities: []*identity.Capability{
		{Type: &identity.Capability_Service_{Service: &identity.Capability_Service{
			Type: identity.Capability_Service_CONTROLLER_SERVICE}}},
		{Type: &identity.Capability_VolumeReplication_{VolumeReplication: &identity.Capability_VolumeReplication{
			Type: identity.Capability_VolumeReplication_VOLUME_REPLICATION}}},
	}}, nil
}

// Probe mirrors the CSI Probe: liveness of this process, not reachability of
// any array, for the reason IdentityServer.Probe gives.
func (s *ReplicationIdentityServer) Probe(ctx context.Context, req *identity.ProbeRequest) (*identity.ProbeResponse, error) {
	return &identity.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}

// reportPolicyInterval tells the metrics how often a policy-mode
// relationship's snapshot policy takes a snapshot, so the stale alert's
// threshold can follow the schedule rather than one fixed figure.
//
// Only the source end reports the mode and the linked policies, so this runs
// when that end was read: on the source array, or from a target whose peer
// was consulted. From a lone target end nothing is reported and the alert
// keeps its fallback. Best effort: a policy that cannot be read is skipped.
func (r *ReplicationServer) reportPolicyInterval(ctx context.Context, e *replicationEnds, rel *relationship) {
	src, client := rel.local, e.local
	switch {
	case rel.role == roleSource:
	case rel.peer != nil && rel.peerRole == roleSource && e.peer != nil:
		src, client = *rel.peer, e.peer
	default:
		return
	}
	if src.ReplicationMode != qumulo.ReplicationModeSnapshotPolicy || len(src.SnapshotPolicies) == 0 {
		e.local.ObservePolicyInterval(ctx, rel.id, 0)
		return
	}
	// Several linked policies: the recovery point steps whenever any of
	// them replicates, so the shortest gap is the one that holds.
	var shortest time.Duration
	for _, link := range src.SnapshotPolicies {
		interval, ok := r.policyInterval(ctx, client, rel.id, link.ID)
		if !ok {
			return
		}
		if shortest == 0 || interval < shortest {
			shortest = interval
		}
	}
	e.local.ObservePolicyInterval(ctx, rel.id, shortest)
}

// policyIntervalReuse is how long a policy's schedule interval is reused.
// Info polls every few seconds per VolumeReplication, and a schedule changes
// rarely; a changed one is seen within this.
const policyIntervalReuse = 5 * time.Minute

type policyIntervalHint struct {
	interval time.Duration
	at       time.Time
}

// policyInterval is the schedule interval of one snapshot policy, read from
// the array that holds it at most once per policyIntervalReuse.
func (r *ReplicationServer) policyInterval(ctx context.Context, client replicationClient, relID, policyID string) (time.Duration, bool) {
	key := relID + "\x00" + policyID
	if hint, ok := r.hints.policyIntervals.Load(key); ok {
		if h := hint.(policyIntervalHint); time.Since(h.at) < policyIntervalReuse {
			return h.interval, true
		}
	}
	schedule, err := client.GetPolicySchedule(ctx, policyID)
	if err != nil {
		slog.DebugContext(ctx, "getvolumereplicationinfo: could not read the linked snapshot policy; its interval is not reported",
			"relationship", relID, "policy", policyID, "error", err)
		return 0, false
	}
	interval, ok := scheduleInterval(schedule.CreationSchedule)
	if !ok {
		return 0, false
	}
	r.hints.policyIntervals.Store(key, policyIntervalHint{interval: interval, at: time.Now()})
	r.noteHintStored()
	return interval, true
}

// reportHintReuse is how long a report trusts what it found last about a
// relationship before listing again. For a relationship read back by ID it is
// sliding: each read that confirms it -- same ID, still rooted at the path,
// not ENDED -- starts it again, so a protected volume's poll lists nothing
// for as long as its relationship stays put. An answer that cannot be
// read back (a member's "none of its own", its group root) is trusted for
// this long from the listing that found it.
const reportHintReuse = 10 * time.Minute

// reportHintMaxAge bounds the sliding: past it, a confirmed relationship is
// listed for again. The listing is the only thing that sees a second
// relationship rooted at the same path, which a read by ID cannot.
const reportHintMaxAge = time.Hour

// noRelationshipReuse is how long a report trusts that a plain volume has no
// relationship at all -- as long as it trusts "no export here". Nothing
// can be read back to confirm an absence, so it never slides.
const noRelationshipReuse = noExportHintTTL

// relationshipHint is the relationship a report found at a path, by ID and
// the local end's role; an empty id says none is rooted there -- for a member
// of a group, whose group's covers it (see AssessVolumeReplication), or for a
// plain volume with none at all.
type relationshipHint struct {
	id    string
	role  relRole
	at    time.Time     // stored, or last confirmed by ID
	found time.Time     // listed; bounds how far confirming extends it
	ttl   time.Duration // from at
}

func newRelationshipHint(id string, role relRole) relationshipHint {
	now := time.Now()
	return relationshipHint{id: id, role: role, at: now, found: now, ttl: reportHintReuse}
}

// noRelationshipHint says none is rooted at the path, trusted for ttl.
func noRelationshipHint(ttl time.Duration) relationshipHint {
	now := time.Now()
	return relationshipHint{at: now, found: now, ttl: ttl}
}

func (h relationshipHint) expired(now time.Time) bool {
	return now.Sub(h.at) >= h.ttl || now.Sub(h.found) >= reportHintMaxAge
}

// groupRootHint is the group root a report found covering a member; an empty
// root says no group covers it.
type groupRootHint struct {
	root string
	at   time.Time
	ttl  time.Duration
}

func (h groupRootHint) expired(now time.Time) bool { return now.Sub(h.at) >= h.ttl }

// relationshipFromHint reads the relationship a report found last at e's
// path from the role-specific endpoint, and believes it while it is still
// rooted there and not ENDED. Anything else -- a reverse, a delete, an
// array that does not answer -- drops the hint and the caller lists.
func (r *ReplicationServer) relationshipFromHint(ctx context.Context, e *replicationEnds, key string) (*relationship, bool) {
	v, ok := r.hints.relationships.Load(key)
	if !ok {
		return nil, false
	}
	h := v.(relationshipHint)
	now := time.Now()
	if h.expired(now) {
		r.hints.relationships.CompareAndDelete(key, v)
		return nil, false
	}
	if h.id == "" {
		return nil, true
	}
	var st qumulo.RelationshipStatus
	var err error
	var root string
	if h.role == roleSource {
		st, err = e.local.GetSourceStatus(ctx, h.id)
		root = st.SourceRootPath
	} else {
		st, err = e.local.GetTargetStatus(ctx, h.id)
		root = st.TargetRootPath
	}
	if err != nil || root != e.realPath || st.State == qumulo.RelationshipEnded {
		r.hints.relationships.CompareAndDelete(key, v)
		return nil, false
	}
	// Confirmed: trusted for another reportHintReuse from now (sliding),
	// unless something else replaced the hint meanwhile.
	confirmed := h
	confirmed.at = now
	r.hints.relationships.CompareAndSwap(key, v, confirmed)
	return &relationship{id: h.id, role: h.role, local: st}, true
}

// peerStatusesFor is peerStatuses for the one relationship id. A report
// reads it from the peer by ID, the target role first, rather than listing
// both of the peer's tables; any other caller lists.
func (r *ReplicationServer) peerStatusesFor(ctx context.Context, rpc string, e *replicationEnds, id string) (map[string]peerStatus, error) {
	if !reportLookups(ctx) {
		return r.peerStatuses(ctx, rpc, e)
	}
	return r.peerStatusesByID(ctx, rpc, e, []string{id})
}

type peerReadsByIDKey struct{}

// withPeerReadsByID marks ctx as Enable's: a relationship found locally is
// read from the peer by ID rather than by listing both of its tables.
func withPeerReadsByID(ctx context.Context) context.Context {
	return context.WithValue(ctx, peerReadsByIDKey{}, true)
}

func peerReadsByID(ctx context.Context) bool {
	marked, _ := ctx.Value(peerReadsByIDKey{}).(bool)
	return marked
}

// peerStatusesByID is peerStatuses for the given relationship ids only, read
// from the peer one by one, the target role first: the same entries the two
// listings would give for those ids, rooted at peerPath.
func (r *ReplicationServer) peerStatusesByID(ctx context.Context, rpc string, e *replicationEnds, ids []string) (map[string]peerStatus, error) {
	out := map[string]peerStatus{}
	for _, id := range ids {
	roles:
		for _, role := range []relRole{roleTarget, roleSource} {
			var st qumulo.RelationshipStatus
			var err error
			var root string
			if role == roleSource {
				st, err = e.peer.GetSourceStatus(ctx, id)
				root = st.SourceRootPath
			} else {
				st, err = e.peer.GetTargetStatus(ctx, id)
				root = st.TargetRootPath
			}
			switch {
			case err == nil && root == e.peerPath:
				out[id] = peerStatus{status: st, role: role}
				break roles
			case err == nil, qumulo.IsNotFound(err):
			default:
				return nil, unreachableOr(e.peerAddr, err, "%s: reading relationship %s on peer %s", rpc, id, e.peerAddr)
			}
		}
	}
	return out, nil
}

// coveringGroupRootFor is coveringGroupRoot, answered for a report from the
// group root it found last for this member.
func (r *ReplicationServer) coveringGroupRootFor(ctx context.Context, rpc string, e *replicationEnds) (string, bool, error) {
	key := e.localAddr + "\x00" + e.realPath
	if reportLookups(ctx) {
		if v, ok := r.hints.groupRoots.Load(key); ok {
			if h := v.(groupRootHint); !h.expired(time.Now()) {
				return h.root, h.root != "", nil
			}
			r.hints.groupRoots.CompareAndDelete(key, v)
		}
	}
	root, found, err := coveringGroupRoot(ctx, rpc, e)
	if err == nil && reportLookups(ctx) {
		// No group either is remembered as briefly as "no relationship",
		// which it completes: together they let an unprotected volume's
		// poll list nothing.
		h := groupRootHint{root: root, at: time.Now(), ttl: reportHintReuse}
		if !found {
			h = groupRootHint{at: time.Now(), ttl: noRelationshipReuse}
		}
		r.hints.groupRoots.Store(key, h)
		r.noteHintStored()
	}
	return root, found, err
}
