// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"
)

// Qumulo Core directory-replication endpoints. All of them are v2: v1 and v3
// answer 404 for source- and target-relationships (v3 serves only object
// replication, which this driver does not use).
//
// A relationship has two ends and each end is addressed through its own
// resource family. The source array holds the configuration -- mode, linked
// policies, whether the source root is frozen -- and it is created, patched
// and deleted there. The target array holds no configuration list at all
// (GET /v2/replication/target-relationships/ is 404): it is driven only by
// the verbs below, and observed only through its status list. The
// relationship ID is the same UUID on both arrays, and is the join key for
// everything in this file.
const (
	replicationSourceRelationshipsPath = "/v2/replication/source-relationships/"
	replicationSourceRelationshipPath  = "/v2/replication/source-relationships/%s"
	replicationSourceStatusesPath      = "/v2/replication/source-relationships/status/"
	replicationSourceStatusPath        = "/v2/replication/source-relationships/%s/status"
	replicationReplicatePath           = "/v2/replication/source-relationships/%s/replicate"
	// Issued on the *target* array, despite living under source-relationships:
	// it turns this array's target end into a source end.
	replicationReversePath = "/v2/replication/source-relationships/reverse-target-relationship"

	replicationTargetStatusesPath = "/v2/replication/target-relationships/status/"
	replicationTargetStatusPath   = "/v2/replication/target-relationships/%s/status"
	replicationAuthorizePath      = "/v2/replication/target-relationships/%s/authorize"
	replicationMakeWritablePath   = "/v2/replication/target-relationships/%s/make-writable"
	replicationReconnectPath      = "/v2/replication/target-relationships/%s/reconnect"
	// A POST, not a DELETE: the target end is a verb on the status resource.
	replicationTargetDeletePath = "/v2/replication/target-relationships/%s/delete"

	// The array's own clock. Every replication timestamp -- recovery_point
	// above all -- is stamped by the array, so a comparison against the
	// controller's clock is a comparison across two clocks. See ClusterTime.
	timeStatusPath = "/v1/time/status"

	// The full definition of one policy, schedules included. ListPolicies
	// deliberately drops the schedule; re-linking a policy on a reversed
	// relationship needs it back.
	snapshotPolicyByIDPath = "/v1/snapshots/policies/%s"
)

// ReplicationPort is the TCP port a target array listens on for replication.
// Fixed by Core; not configurable per relationship in practice.
const ReplicationPort = 3712

// Relationship states, as the array reports them in the status `state` field.
const (
	RelationshipAwaitingAuthorization = "AWAITING_AUTHORIZATION"
	RelationshipDisconnected          = "DISCONNECTED"
	RelationshipDisconnecting         = "DISCONNECTING"
	RelationshipEnded                 = "ENDED"
	RelationshipEstablished           = "ESTABLISHED"
	RelationshipReconnecting          = "RECONNECTING"
	RelationshipReestablishing        = "REESTABLISHING"
)

// Replication job states.
const (
	ReplicationJobRunning    = "REPLICATION_RUNNING"
	ReplicationJobNotRunning = "REPLICATION_NOT_RUNNING"
)

// Replication modes.
//
// Continuous replicates every change as it lands. SnapshotPolicy replicates
// only the snapshots taken by linked policies, so the recovery point moves in
// steps of the policy interval. The third does both. The mode decides how a
// planned failover drains: a continuous relationship drains on its own once
// the source is frozen, where a policy-only one waits for the next policy fire
// -- or is switched to the combined mode for the duration.
const (
	ReplicationModeContinuous               = "REPLICATION_CONTINUOUS"
	ReplicationModeSnapshotPolicy           = "REPLICATION_SNAPSHOT_POLICY"
	ReplicationModeSnapshotPolicyContinuous = "REPLICATION_SNAPSHOT_POLICY_WITH_CONTINUOUS"
)

// TargetExpirationSameAsPolicy makes a replicated policy snapshot expire on the
// target when it would have on the source.
const TargetExpirationSameAsPolicy = "same_as_policy"

// SnapshotPolicyLink links one snapshot policy to a relationship, so the
// snapshots that policy takes are replicated with their expiration.
type SnapshotPolicyLink struct {
	// ID is the policy's ID on the *source* array, as a decimal string --
	// policies are numbered per array and do not travel.
	ID string
	// TargetExpiration is TargetExpirationSameAsPolicy, "never", or a
	// "<n><units>" interval as Schedule.ExpirationTimeToLive takes.
	TargetExpiration string
}

// snapshotPolicyLinkWire is the wire form of SnapshotPolicyLink. The array
// renders the policy ID as a bare JSON number here, where ListPolicies renders
// the same ID as a number too but this driver carries it as a string --
// json.Number bridges the two without a conversion per call site.
type snapshotPolicyLinkWire struct {
	ID               json.Number `json:"id"`
	TargetExpiration string      `json:"target_expiration"`
}

func (l SnapshotPolicyLink) toWire() snapshotPolicyLinkWire {
	return snapshotPolicyLinkWire{ID: json.Number(l.ID), TargetExpiration: l.TargetExpiration}
}

func (w snapshotPolicyLinkWire) toLink() SnapshotPolicyLink {
	return SnapshotPolicyLink{ID: w.ID.String(), TargetExpiration: w.TargetExpiration}
}

// SourceSpec describes a relationship to create on the source array.
//
// A struct rather than positional arguments for the same reason ExportSpec is:
// two paths and an address next to each other is exactly the shape where a
// transposed pair compiles and then replicates the wrong directory.
type SourceSpec struct {
	// SourceRootPath is the directory on this array to replicate.
	SourceRootPath string
	// TargetRootPath is where the copy lives on the target array. The
	// directory need not exist: Authorize with allowCreate makes it.
	TargetRootPath string
	// TargetAddress is the target array's replication address, a bare host
	// or IPv4 literal. TargetPort is ReplicationPort unless the array says
	// otherwise; zero means ReplicationPort.
	TargetAddress string
	TargetPort    int
	// ReplicationMode is one of the ReplicationMode* constants. Empty means
	// ReplicationModeContinuous.
	ReplicationMode string
	// ReplicationEnabled is sent as given. The array defaults it to false,
	// which creates a relationship that replicates nothing until patched, so
	// callers that want replication must say so.
	ReplicationEnabled bool
	// SnapshotPolicies are the policies whose snapshots the relationship
	// carries. Only meaningful in the two snapshot-policy modes.
	SnapshotPolicies []SnapshotPolicyLink
}

// Relationship is the source-side configuration of a relationship, as the
// array returns it from a create, a patch or a reverse.
//
// It is configuration, not status: state, recovery point and job progress
// live on RelationshipStatus. The two are kept apart because a create returns
// only this shape, and because a status listing is what the driver polls --
// mixing them would make a poll look like it could answer configuration
// questions it cannot.
type Relationship struct {
	ID                 string
	SourceRootID       string
	SourceRootReadOnly bool
	TargetAddress      string
	TargetPort         int
	ReplicationEnabled bool
	ReplicationMode    string
	SnapshotPolicies   []SnapshotPolicyLink
}

// relationshipWire is the API representation of Relationship.
type relationshipWire struct {
	ID                 string                   `json:"id"`
	SourceRootID       string                   `json:"source_root_id"`
	SourceRootReadOnly bool                     `json:"source_root_read_only"`
	TargetAddress      string                   `json:"target_address"`
	TargetPort         int                      `json:"target_port"`
	ReplicationEnabled bool                     `json:"replication_enabled"`
	ReplicationMode    string                   `json:"replication_mode"`
	SnapshotPolicies   []snapshotPolicyLinkWire `json:"snapshot_policies"`
}

func (w relationshipWire) toRelationship() Relationship {
	rel := Relationship{
		ID:                 w.ID,
		SourceRootID:       w.SourceRootID,
		SourceRootReadOnly: w.SourceRootReadOnly,
		TargetAddress:      w.TargetAddress,
		TargetPort:         w.TargetPort,
		ReplicationEnabled: w.ReplicationEnabled,
		ReplicationMode:    w.ReplicationMode,
	}
	for _, link := range w.SnapshotPolicies {
		rel.SnapshotPolicies = append(rel.SnapshotPolicies, link.toLink())
	}
	return rel
}

// RelationshipStatus is one relationship as seen from one array.
//
// The same struct serves both ends. A source status carries the target's
// address and the source root's ID; a target status carries the source's
// address (which the array leaves null while ESTABLISHED, so it is not a
// reliable key) and the target root's ID. Fields an end does not report are
// zero. Callers decide which end they are looking at from which list the
// status came from -- see ListSourceStatus and ListTargetStatus -- not from
// the fields.
type RelationshipStatus struct {
	ID    string
	State string
	// EndReason is set only when State is RelationshipEnded, and names what
	// ended it -- typically that one root directory was deleted.
	EndReason string

	SourceClusterName  string
	SourceClusterUUID  string
	TargetClusterName  string
	TargetClusterUUID  string
	SourceRootPath     string
	SourceRootID       string
	SourceRootReadOnly bool
	TargetRootPath     string
	TargetRootID       string
	TargetRootReadOnly bool
	// TargetAddress is reported by the source end; SourceAddress by the
	// target end, and only after a disconnect.
	TargetAddress string
	SourceAddress string

	ReplicationEnabled bool
	// ReplicationMode is reported by the source end only.
	ReplicationMode  string
	SnapshotPolicies []SnapshotPolicyLink

	JobState string
	// RecoveryPoint is the start time of the last successful job, by the
	// array's clock. Zero when no job has ever completed -- which is the
	// state a freshly authorized relationship is in, and the state in which
	// the target holds an empty directory.
	RecoveryPoint    time.Time
	ErrorFromLastJob string
	// DurationOfLastJob is zero when no job has run.
	DurationOfLastJob time.Duration
	// NextActiveTime is set while a blackout window holds replication off.
	NextActiveTime time.Time
	// QueuedSnapshotCount is policy snapshots waiting to replicate.
	QueuedSnapshotCount int
	// RecoveryPointSnapshotID identifies the snapshot the recovery point was
	// taken from, as a decimal string; "" when there is none. A strictly
	// newer ID after a freeze is proof a job completed after it.
	RecoveryPointSnapshotID string
	// Job progress, present only while a job runs or just after one.
	BytesTransferred uint64
	PercentComplete  float64
}

// HasRecoveryPoint reports whether the relationship has ever completed a job,
// which is the difference between a replica that holds data and one that
// holds an empty directory.
func (s RelationshipStatus) HasRecoveryPoint() bool { return !s.RecoveryPoint.IsZero() }

// JobIdle reports whether no replication job is running.
func (s RelationshipStatus) JobIdle() bool { return s.JobState == ReplicationJobNotRunning }

// relationshipStatusWire is the API representation of RelationshipStatus.
//
// Addresses and ports are pointers because the target end renders them as
// JSON null while connected; a plain string would decode null as "" and hide
// nothing, but a plain int would fail to decode. Timestamps are strings
// because the array renders "" (not null) for an unset one.
type relationshipStatusWire struct {
	ID                 string                   `json:"id"`
	State              string                   `json:"state"`
	EndReason          string                   `json:"end_reason"`
	SourceClusterName  string                   `json:"source_cluster_name"`
	SourceClusterUUID  string                   `json:"source_cluster_uuid"`
	TargetClusterName  string                   `json:"target_cluster_name"`
	TargetClusterUUID  string                   `json:"target_cluster_uuid"`
	SourceRootPath     string                   `json:"source_root_path"`
	SourceRootID       string                   `json:"source_root_id"`
	SourceRootReadOnly bool                     `json:"source_root_read_only"`
	TargetRootPath     string                   `json:"target_root_path"`
	TargetRootID       string                   `json:"target_root_id"`
	TargetRootReadOnly bool                     `json:"target_root_read_only"`
	TargetAddress      *string                  `json:"target_address"`
	SourceAddress      *string                  `json:"source_address"`
	ReplicationEnabled bool                     `json:"replication_enabled"`
	ReplicationMode    string                   `json:"replication_mode"`
	SnapshotPolicies   []snapshotPolicyLinkWire `json:"snapshot_policies"`
	JobState           string                   `json:"job_state"`
	RecoveryPoint      string                   `json:"recovery_point"`
	ErrorFromLastJob   string                   `json:"error_from_last_job"`
	DurationOfLastJob  *struct {
		// A decimal string, like every other 64-bit quantity the API renders.
		Nanoseconds string `json:"nanoseconds"`
	} `json:"duration_of_last_job"`
	NextActiveTime        string `json:"next_active_time"`
	QueuedSnapshotCount   int    `json:"queued_snapshot_count"`
	RecoveryPointSnapshot *struct {
		ID json.Number `json:"id"`
	} `json:"recovery_point_snapshot"`
	ReplicationJobStatus *struct {
		PercentComplete  float64 `json:"percent_complete"`
		BytesTransferred string  `json:"bytes_transferred"`
	} `json:"replication_job_status"`
}

func (w relationshipStatusWire) toStatus() RelationshipStatus {
	s := RelationshipStatus{
		ID:                  w.ID,
		State:               w.State,
		EndReason:           w.EndReason,
		SourceClusterName:   w.SourceClusterName,
		SourceClusterUUID:   w.SourceClusterUUID,
		TargetClusterName:   w.TargetClusterName,
		TargetClusterUUID:   w.TargetClusterUUID,
		SourceRootID:        w.SourceRootID,
		SourceRootReadOnly:  w.SourceRootReadOnly,
		TargetRootID:        w.TargetRootID,
		TargetRootReadOnly:  w.TargetRootReadOnly,
		ReplicationEnabled:  w.ReplicationEnabled,
		ReplicationMode:     w.ReplicationMode,
		JobState:            w.JobState,
		ErrorFromLastJob:    w.ErrorFromLastJob,
		QueuedSnapshotCount: w.QueuedSnapshotCount,
	}
	// The array renders a root path with a trailing slash ("/csi/volumes/pvc-x/")
	// -- the same quirk ResolveFileIDs cleans, and for the same reason: every
	// caller compares these against paths from somewhere else. path.Clean
	// leaves "" alone as ".", so guard the unset case.
	if w.SourceRootPath != "" {
		s.SourceRootPath = path.Clean(w.SourceRootPath)
	}
	if w.TargetRootPath != "" {
		s.TargetRootPath = path.Clean(w.TargetRootPath)
	}
	if w.TargetAddress != nil {
		s.TargetAddress = *w.TargetAddress
	}
	if w.SourceAddress != nil {
		s.SourceAddress = *w.SourceAddress
	}
	for _, link := range w.SnapshotPolicies {
		s.SnapshotPolicies = append(s.SnapshotPolicies, link.toLink())
	}
	// Unparseable timestamps stay zero rather than failing the call: a status
	// with a bad recovery_point is still a status, and "no recovery point" is
	// the conservative reading of one that cannot be parsed.
	if ts, err := time.Parse(time.RFC3339Nano, w.RecoveryPoint); err == nil {
		s.RecoveryPoint = ts
	}
	if ts, err := time.Parse(time.RFC3339Nano, w.NextActiveTime); err == nil {
		s.NextActiveTime = ts
	}
	if w.DurationOfLastJob != nil {
		if ns, err := strconv.ParseInt(w.DurationOfLastJob.Nanoseconds, 10, 64); err == nil {
			s.DurationOfLastJob = time.Duration(ns)
		}
	}
	if w.RecoveryPointSnapshot != nil {
		s.RecoveryPointSnapshotID = w.RecoveryPointSnapshot.ID.String()
	}
	if w.ReplicationJobStatus != nil {
		s.PercentComplete = w.ReplicationJobStatus.PercentComplete
		if n, err := strconv.ParseUint(w.ReplicationJobStatus.BytesTransferred, 10, 64); err == nil {
			s.BytesTransferred = n
		}
	}
	return s
}

// SourcePatch is a partial update to a source relationship. Every field is
// optional; nil or empty means "leave it alone".
//
// Pointers rather than values because false and "" are both meaningful: a
// patch that unfreezes the source sends source_root_read_only: false, and one
// that leaves it alone sends nothing. omitempty on a bool cannot tell the two
// apart.
type SourcePatch struct {
	SourceRootReadOnly *bool
	ReplicationEnabled *bool
	ReplicationMode    string
	// SnapshotPolicies replaces the linked set when non-nil. An empty,
	// non-nil slice unlinks every policy.
	SnapshotPolicies *[]SnapshotPolicyLink
}

// sourcePatchWire is the request body for PATCH .../source-relationships/{id}.
type sourcePatchWire struct {
	SourceRootReadOnly *bool                     `json:"source_root_read_only,omitempty"`
	ReplicationEnabled *bool                     `json:"replication_enabled,omitempty"`
	ReplicationMode    string                    `json:"replication_mode,omitempty"`
	SnapshotPolicies   *[]snapshotPolicyLinkWire `json:"snapshot_policies,omitempty"`
}

// ReplicationClient is the subset of Qumulo REST API operations the csi-addons
// replication service depends on.
//
// It is a separate interface from ClientInterface on purpose: *Client
// implements both, but every existing fake implements only the first, and the
// replication service is opt-in. Widening ClientInterface would make a dozen
// test doubles fail to compile for a feature most deployments never turn on.
// Driver code that needs both asserts to both.
type ReplicationClient interface {
	// ObserveVolumeProtection reports whether one volume was found to be
	// covered by a replication relationship on this array. Best-effort and
	// side-effect free: a client with no Observer does nothing.
	ObserveVolumeProtection(ctx context.Context, protected bool)
	// ObservePolicyInterval reports a policy-mode relationship's snapshot
	// schedule interval, zero for none. Best-effort, like
	// ObserveVolumeProtection.
	ObservePolicyInterval(ctx context.Context, relationshipID string, interval time.Duration)
	// ListSourceStatus lists every relationship this array is the source of.
	ListSourceStatus(ctx context.Context) ([]RelationshipStatus, error)
	// ListTargetStatus lists every relationship this array is the target of.
	ListTargetStatus(ctx context.Context) ([]RelationshipStatus, error)
	// GetSourceStatus returns one relationship's status from the source end.
	// It returns an error satisfying IsNotFound when this array is not the
	// source of that relationship.
	GetSourceStatus(ctx context.Context, id string) (RelationshipStatus, error)
	// GetTargetStatus returns one relationship's status from the target end.
	GetTargetStatus(ctx context.Context, id string) (RelationshipStatus, error)

	// CreateSource creates a relationship with this array as its source. The
	// relationship starts AWAITING_AUTHORIZATION until Authorize is called on
	// the target array.
	CreateSource(ctx context.Context, spec SourceSpec) (Relationship, error)
	// PatchSource changes a source relationship's configuration in place.
	PatchSource(ctx context.Context, id string, patch SourcePatch) (Relationship, error)
	// DeleteSource removes the source end of a relationship. The target end
	// is left ENDED and must be removed with DeleteTarget on the other array.
	DeleteSource(ctx context.Context, id string) error
	// Replicate starts a manual replication job. The array refuses it with
	// IsManualReplicationDisallowed while replication_enabled is true, in
	// every mode, so it is not a drain for a live relationship.
	Replicate(ctx context.Context, id string) error

	// Authorize accepts a relationship on the target array, moving it from
	// AWAITING_AUTHORIZATION to ESTABLISHED and making the target root
	// read-only. allowCreate lets the array create the target directory;
	// allowNonEmpty lets it adopt one that already holds data, which the
	// first job then overwrites to match the source.
	Authorize(ctx context.Context, id string, allowCreate, allowNonEmpty bool) (RelationshipStatus, error)
	// MakeTargetWritable reverts the target root to the latest recovery point,
	// disconnects the relationship and makes the root writable. It is not
	// repeatable: on a relationship that is already disconnected the array
	// answers IsAlreadyDisconnected, so callers check TargetRootReadOnly
	// before calling.
	MakeTargetWritable(ctx context.Context, id string) (RelationshipStatus, error)
	// Reconnect reverts the target root to the recovery point -- discarding
	// whatever was written to it while writable -- makes it read-only and
	// resumes replication from the source.
	Reconnect(ctx context.Context, id string) (RelationshipStatus, error)
	// Reverse, issued on the target array, swaps the roles of a relationship
	// while keeping its ID. Both ends are left DISCONNECTED until the new
	// target Reconnects. sourceAddress is this array's address as the new
	// target should reach it; sourcePort zero means ReplicationPort.
	Reverse(ctx context.Context, id, sourceAddress string, sourcePort int) (Relationship, error)
	// DeleteTarget removes the target end of a relationship. The target root
	// becomes writable.
	DeleteTarget(ctx context.Context, id string) error

	// ClusterTime returns the array's own clock. Replication timestamps are
	// stamped by the array, so a drain gate compares recovery_point to this
	// and never to the controller's clock.
	ClusterTime(ctx context.Context) (time.Time, error)

	// GetPolicySchedule returns the schedule a snapshot policy fires on, which
	// ListPolicies deliberately omits. Needed to recreate a volume's policy on
	// the new source after a reverse, because the reverse drops the link.
	GetPolicySchedule(ctx context.Context, policyID string) (Schedule, error)
	// SetExportReadOnly flips every restriction on an export between
	// read-only and read-write. A replica's export is pre-staged read-only
	// and flipped at promote, so a pod that reaches it early is refused at
	// mount rather than writing into a directory that drops the writes.
	SetExportReadOnly(ctx context.Context, exportID string, readOnly bool) error
}

var _ ReplicationClient = (*Client)(nil)

// ListSourceStatus lists every relationship this array is the source of.
//
// The listing is a bare JSON array, unlike the exports and snapshots
// collections, which use an {"entries": [...]} envelope.
func (c *Client) ListSourceStatus(ctx context.Context) ([]RelationshipStatus, error) {
	return c.listStatuses(ctx, replicationSourceStatusesPath)
}

// ListTargetStatus lists every relationship this array is the target of.
func (c *Client) ListTargetStatus(ctx context.Context) ([]RelationshipStatus, error) {
	return c.listStatuses(ctx, replicationTargetStatusesPath)
}

func (c *Client) listStatuses(ctx context.Context, path string) ([]RelationshipStatus, error) {
	started := time.Now()
	var listed []relationshipStatusWire
	if err := c.do(ctx, http.MethodGet, path, nil, &listed); err != nil {
		return nil, err
	}
	statuses := make([]RelationshipStatus, 0, len(listed))
	for _, entry := range listed {
		statuses = append(statuses, entry.toStatus())
	}
	// Which listing a status came out of is the only reliable statement of
	// the role -- the same join the replication service itself uses, because
	// a target end reports source_address null while connected.
	role := RoleSource
	if path == replicationTargetStatusesPath {
		role = RoleTarget
	}
	c.observeReplication(ctx, role, time.Time{}, statuses, true, started)
	return statuses, nil
}

// GetSourceStatus returns one relationship's status from the source end.
func (c *Client) GetSourceStatus(ctx context.Context, id string) (RelationshipStatus, error) {
	started := time.Now()
	var found relationshipStatusWire
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf(replicationSourceStatusPath, url.PathEscape(id)), nil, &found); err != nil {
		return RelationshipStatus{}, err
	}
	status := found.toStatus()
	c.observeReplication(ctx, RoleSource, time.Time{}, []RelationshipStatus{status}, false, started)
	return status, nil
}

// GetTargetStatus returns one relationship's status from the target end.
func (c *Client) GetTargetStatus(ctx context.Context, id string) (RelationshipStatus, error) {
	started := time.Now()
	var found relationshipStatusWire
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf(replicationTargetStatusPath, url.PathEscape(id)), nil, &found); err != nil {
		return RelationshipStatus{}, err
	}
	status := found.toStatus()
	c.observeReplication(ctx, RoleTarget, time.Time{}, []RelationshipStatus{status}, false, started)
	return status, nil
}

// CreateSource creates a relationship with this array as its source.
//
// replication_enabled is sent unconditionally, with no omitempty, because the
// array's default is false and a relationship created that way replicates
// nothing until patched -- which looks exactly like a relationship that is
// replicating slowly.
func (c *Client) CreateSource(ctx context.Context, spec SourceSpec) (Relationship, error) {
	port := spec.TargetPort
	if port == 0 {
		port = ReplicationPort
	}
	mode := spec.ReplicationMode
	if mode == "" {
		mode = ReplicationModeContinuous
	}
	// An explicit empty slice, not nil: nil marshals to JSON null, and the
	// array wants a list.
	links := make([]snapshotPolicyLinkWire, 0, len(spec.SnapshotPolicies))
	for _, link := range spec.SnapshotPolicies {
		links = append(links, link.toWire())
	}

	req := struct {
		SourceRootPath     string                   `json:"source_root_path"`
		TargetRootPath     string                   `json:"target_root_path"`
		TargetAddress      string                   `json:"target_address"`
		TargetPort         int                      `json:"target_port"`
		ReplicationEnabled bool                     `json:"replication_enabled"`
		ReplicationMode    string                   `json:"replication_mode"`
		SnapshotPolicies   []snapshotPolicyLinkWire `json:"snapshot_policies"`
	}{
		SourceRootPath:     spec.SourceRootPath,
		TargetRootPath:     spec.TargetRootPath,
		TargetAddress:      spec.TargetAddress,
		TargetPort:         port,
		ReplicationEnabled: spec.ReplicationEnabled,
		ReplicationMode:    mode,
		SnapshotPolicies:   links,
	}

	var created relationshipWire
	if err := c.do(ctx, http.MethodPost, replicationSourceRelationshipsPath, req, &created); err != nil {
		return Relationship{}, err
	}
	return created.toRelationship(), nil
}

// PatchSource changes a source relationship's configuration in place.
//
// This is the freeze: a patch of source_root_read_only: true is what a planned
// failover uses to stop writes on the source before draining. Nothing but a
// patch of false ever unfreezes it, so a caller that freezes and then fails
// must be prepared to come back and unfreeze.
func (c *Client) PatchSource(ctx context.Context, id string, patch SourcePatch) (Relationship, error) {
	req := sourcePatchWire{
		SourceRootReadOnly: patch.SourceRootReadOnly,
		ReplicationEnabled: patch.ReplicationEnabled,
		ReplicationMode:    patch.ReplicationMode,
	}
	if patch.SnapshotPolicies != nil {
		links := make([]snapshotPolicyLinkWire, 0, len(*patch.SnapshotPolicies))
		for _, link := range *patch.SnapshotPolicies {
			links = append(links, link.toWire())
		}
		req.SnapshotPolicies = &links
	}

	var patched relationshipWire
	if err := c.do(ctx, http.MethodPatch, fmt.Sprintf(replicationSourceRelationshipPath, url.PathEscape(id)), req, &patched); err != nil {
		return Relationship{}, err
	}
	return patched.toRelationship(), nil
}

// DeleteSource removes the source end of a relationship.
func (c *Client) DeleteSource(ctx context.Context, id string) error {
	err := c.do(ctx, http.MethodDelete, fmt.Sprintf(replicationSourceRelationshipPath, url.PathEscape(id)), nil, nil)
	if err == nil || IsNotFound(err) {
		c.observeRelationshipRemoved(ctx, id)
	}
	return err
}

// Replicate starts a manual replication job.
//
// Kept for the policy-only case where replication has been disabled; on a
// live relationship the array refuses it, and a drain is a freeze plus a wait
// on recovery_point instead. See ReplicationClient.
func (c *Client) Replicate(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf(replicationReplicatePath, url.PathEscape(id)), struct{}{}, nil)
}

// Authorize accepts a relationship on the target array.
//
// Both options are query parameters, not body fields, and both default to
// false on the array. allowCreate is what makes the target directory exist at
// all: nothing can be pre-staged on the target before this call because the
// quota needs the directory's file ID, and the ID is allocated here.
func (c *Client) Authorize(ctx context.Context, id string, allowCreate, allowNonEmpty bool) (RelationshipStatus, error) {
	query := url.Values{}
	if allowCreate {
		query.Set("allow-fs-path-create", "true")
	}
	if allowNonEmpty {
		query.Set("allow-non-empty-directory", "true")
	}
	path := fmt.Sprintf(replicationAuthorizePath, url.PathEscape(id))
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var authorized relationshipStatusWire
	if err := c.do(ctx, http.MethodPost, path, struct{}{}, &authorized); err != nil {
		return RelationshipStatus{}, err
	}
	return authorized.toStatus(), nil
}

// MakeTargetWritable disconnects the relationship and makes the target root
// writable at its latest recovery point.
func (c *Client) MakeTargetWritable(ctx context.Context, id string) (RelationshipStatus, error) {
	var result relationshipStatusWire
	if err := c.do(ctx, http.MethodPost, fmt.Sprintf(replicationMakeWritablePath, url.PathEscape(id)), struct{}{}, &result); err != nil {
		return RelationshipStatus{}, err
	}
	return result.toStatus(), nil
}

// Reconnect reverts the target root and resumes replication from the source.
func (c *Client) Reconnect(ctx context.Context, id string) (RelationshipStatus, error) {
	var result relationshipStatusWire
	if err := c.do(ctx, http.MethodPost, fmt.Sprintf(replicationReconnectPath, url.PathEscape(id)), struct{}{}, &result); err != nil {
		return RelationshipStatus{}, err
	}
	return result.toStatus(), nil
}

// Reverse swaps the roles of a relationship, keeping its ID.
//
// Measured on Core 7.9.2.2: the reversed relationship comes back as
// REPLICATION_CONTINUOUS with no snapshot policies, whatever it was before,
// so a caller that cares about mode or policy links must PatchSource straight
// after. The returned Relationship is the new source end's configuration and
// is where that reset is visible.
func (c *Client) Reverse(ctx context.Context, id, sourceAddress string, sourcePort int) (Relationship, error) {
	if sourcePort == 0 {
		sourcePort = ReplicationPort
	}
	req := struct {
		TargetRelationshipID string `json:"target_relationship_id"`
		SourceAddress        string `json:"source_address"`
		SourcePort           int    `json:"source_port"`
	}{TargetRelationshipID: id, SourceAddress: sourceAddress, SourcePort: sourcePort}

	var reversed relationshipWire
	if err := c.do(ctx, http.MethodPost, replicationReversePath, req, &reversed); err != nil {
		return Relationship{}, err
	}
	return reversed.toRelationship(), nil
}

// DeleteTarget removes the target end of a relationship.
func (c *Client) DeleteTarget(ctx context.Context, id string) error {
	err := c.do(ctx, http.MethodPost, fmt.Sprintf(replicationTargetDeletePath, url.PathEscape(id)), struct{}{}, nil)
	if err == nil || IsNotFound(err) {
		c.observeRelationshipRemoved(ctx, id)
	}
	return err
}

// ClusterTime returns the array's own clock.
//
// A recovery_point is the array's timestamp, so "has the recovery point passed
// the freeze?" is only answerable against the array's clock: a source array
// running ahead of the controller would otherwise pass a pre-freeze recovery
// point as drained. Read this right after the freeze returns and compare with
// no slack.
// ClusterID implements ClientInterface.
func (c *Client) ClusterID(ctx context.Context) (string, error) {
	var state struct {
		ClusterID string `json:"cluster_id"`
	}
	if err := c.do(ctx, http.MethodGet, nodeStatePath, nil, &state); err != nil {
		return "", err
	}
	if state.ClusterID == "" {
		return "", fmt.Errorf("%s answered no cluster_id", nodeStatePath)
	}
	return state.ClusterID, nil
}

func (c *Client) ClusterTime(ctx context.Context) (time.Time, error) {
	var status struct {
		Time string `json:"time"`
	}
	if err := c.do(ctx, http.MethodGet, timeStatusPath, nil, &status); err != nil {
		return time.Time{}, err
	}
	ts, err := time.Parse(time.RFC3339Nano, status.Time)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing the array's clock %q: %w", status.Time, err)
	}
	// Reported with no relationships: this is the array's clock and nothing
	// else. An exporter needs it to age a recovery point against the clock
	// that stamped it rather than against the controller's.
	c.observeReplication(ctx, "", ts, nil, false, time.Time{})
	return ts, nil
}

// GetPolicySchedule returns the first schedule of a snapshot policy.
//
// A driver-created policy has exactly one schedule (CreatePolicy sends one),
// so the first is the whole answer for the policies this is used on.
func (c *Client) GetPolicySchedule(ctx context.Context, policyID string) (Schedule, error) {
	var found policyWire
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf(snapshotPolicyByIDPath, url.PathEscape(policyID)), nil, &found); err != nil {
		return Schedule{}, err
	}
	if len(found.Schedules) == 0 {
		return Schedule{}, fmt.Errorf("snapshot policy %s has no schedule", policyID)
	}
	return found.Schedules[0], nil
}

// SetExportReadOnly flips every restriction on an export.
//
// The export API has no partial update of one restriction field: a PATCH
// replaces the whole restrictions list, so the export is read in full and
// written back with only read_only changed. That round trip is also what
// keeps host lists, authentication mode and identity mapping exactly as the
// operator set them.
func (c *Client) SetExportReadOnly(ctx context.Context, exportID string, readOnly bool) error {
	path := fmt.Sprintf(nfsExportPath, url.PathEscape(exportID))

	// Decoded as raw JSON so fields this client does not model -- and Core
	// 7.9.3 requires every one to be *present* on the write -- travel back
	// untouched. Only restrictions is decoded, changed and re-encoded.
	var export map[string]json.RawMessage
	if err := c.do(ctx, http.MethodGet, path, nil, &export); err != nil {
		return err
	}
	var restrictions []map[string]json.RawMessage
	if raw, ok := export["restrictions"]; ok {
		if err := json.Unmarshal(raw, &restrictions); err != nil {
			return malformedReply(fmt.Errorf("decoding restrictions of export %s: %w", exportID, err))
		}
	}
	if len(restrictions) == 0 {
		return malformedReply(fmt.Errorf("export %s has no restrictions to flip", exportID))
	}
	flag, _ := json.Marshal(readOnly)
	for i := range restrictions {
		// A JSON null decodes to a nil map, and writing into one panicked
		// the controller. Refused, and nothing is written back.
		if restrictions[i] == nil {
			return malformedReply(fmt.Errorf("export %s: restriction %d is null; not changing an export this client cannot read", exportID, i))
		}
		restrictions[i]["read_only"] = flag
	}
	encoded, err := json.Marshal(restrictions)
	if err != nil {
		return fmt.Errorf("encoding restrictions of export %s: %w", exportID, err)
	}
	export["restrictions"] = encoded
	// The ID stays in the body. Core 7.9.2.2 refuses a PUT without it with
	// HTTP 400 decode_error "field 'id' not found" (measured on the lab
	// arrays, and again on the first live run of the promote path). The first cut of this
	// method deleted it on the opposite assumption; the mock accepted both,
	// which is why only the array caught it.

	return c.do(ctx, http.MethodPut, path, export, nil)
}
