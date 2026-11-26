// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Replication endpoints, mirroring the constants in pkg/qumulo/replication.go.
//
// One mock process plays BOTH ends of every relationship. A real relationship
// spans two arrays -- created on the source, authorized on the target -- but
// there is no second process here to authorize on, so a relationship created
// on this mock is immediately visible in its target-status list as well, and
// every target verb (authorize, make-writable, reconnect, reverse, delete)
// acts on the same record. A driver pointed at this one address for both its
// local and its peer array therefore runs the whole enable / promote / resync
// / disable sequence against it, which is what csi-sanity-style coverage
// needs. What it cannot model is one end being unreachable while the other
// answers; that is what test/live is for.
//
// The state machine enforces exactly the transitions the driver's
// correctness depends on, each measured on Core 7.9.2.2:
//
//	create        -> AWAITING_AUTHORIZATION
//	authorize     -> ESTABLISHED, target root read-only, target dir created
//	make-writable -> target end DISCONNECTED, target root writable; the
//	                 SOURCE end keeps reporting ESTABLISHED (measured live);
//	                 a second call is HTTP 400
//	                 relationship_manager_irreplicable_error
//	reverse       -> roles swap, same ID, both ends DISCONNECTED, mode reset
//	                 to REPLICATION_CONTINUOUS and policies unlinked
//	reconnect     -> ESTABLISHED, target root read-only again
//	replicate     -> HTTP 400 replication_manual_replication_disallowed_error
//	                 while replication_enabled is true
const (
	replicationSourcePath  = "/v2/replication/source-relationships/"
	replicationTargetPath  = "/v2/replication/target-relationships/"
	replicationReversePath = "/v2/replication/source-relationships/reverse-target-relationship"
	timeStatusPath         = "/v1/time/status"
	snapshotPoliciesPath   = "/v1/snapshots/policies/"
)

// Relationship states and job states the mock renders. Only the ones the
// state machine above reaches; the transitional states a real array passes
// through (RECONNECTING, REESTABLISHING) are instantaneous here.
const (
	stateAwaitingAuthorization = "AWAITING_AUTHORIZATION"
	stateEstablished           = "ESTABLISHED"
	stateDisconnected          = "DISCONNECTED"
	stateEnded                 = "ENDED"

	jobNotRunning = "REPLICATION_NOT_RUNNING"
	jobRunning    = "REPLICATION_RUNNING"

	modeContinuous = "REPLICATION_CONTINUOUS"
)

// relationship is one directory-replication relationship, holding both the
// source-end configuration and the target-end state, because this mock is
// both arrays.
type relationship struct {
	ID string

	SourceRootPath string
	TargetRootPath string
	TargetAddress  string
	TargetPort     int

	ReplicationEnabled bool
	ReplicationMode    string
	// SnapshotPolicies is kept as the raw JSON the caller sent, so whatever
	// shape the client puts on the wire comes back byte-for-byte.
	SnapshotPolicies json.RawMessage

	SourceRootReadOnly bool
	TargetRootReadOnly bool

	State string
	// sourceEndState, when set, is what the SOURCE list reports for State.
	// Measured on Core 7.9.2.2: after make-writable on the target the source
	// end keeps reporting ESTABLISHED; only the target end says DISCONNECTED,
	// and only a reverse makes both ends agree. A mock that flipped both at
	// once let a driver bug through to the live run (2026-09-14).
	sourceEndState string
	// EndReason is set with State ENDED, when one end has been deleted while
	// the other remains.
	EndReason string

	// hasSource and hasTarget say which ends still exist on this mock. Both
	// start true; DELETE on one side clears its flag and leaves the other
	// ENDED, which is the orphan a real out-of-band delete leaves behind.
	hasSource, hasTarget bool

	// recoveryPoint is the array-clock time of the last completed job. Zero
	// until authorize completes the (instant) initial sync.
	recoveryPoint time.Time
	// jobStart is when the running job began; zero when none runs. A
	// freeze starts none (measured 2026-09-15); a test may start one to
	// model a cycle job in flight. Historical note: the mock once started one at a freeze or when the mode gained a
	// continuous component (measured); here it stays RUNNING for
	// -replication-lag and completes with a recovery point at its start
	// and a new recovery_point_snapshot id.
	jobStart time.Time
	// repSnapID is the last replicated snapshot's id, from the cluster's one
	// snapshot sequence, reported as recovery_point_snapshot; repFrozen is
	// the source subtree as that snapshot captured it, what changes-since
	// compares against a marker snapshot.
	repSnapID int64
	repFrozen map[string][]*fileNode
}

// snapshotPolicy is one snapshot policy, stored as the caller sent it plus
// the ID the mock assigned. The driver reads schedules back to recreate a
// policy on a reversed relationship's new source, so the body round-trips.
type snapshotPolicy struct {
	ID            int64
	Name          string
	SourceFileIDs []string
	Schedules     json.RawMessage
	Enabled       bool
}

// handleReplication routes every path under /v2/replication/. Returns false
// when the path is not a replication path, so ServeHTTP can fall through.
func (c *cluster) handleReplication(w http.ResponseWriter, r *http.Request, reqPath string) bool {
	switch {
	case reqPath == replicationReversePath:
		c.handleReverse(w, r)
	case reqPath == replicationSourcePath:
		c.handleSourceCollection(w, r)
	case reqPath == replicationSourcePath+"status/":
		c.handleListStatus(w, r, true)
	case strings.HasPrefix(reqPath, replicationSourcePath):
		c.handleSourceResource(w, r, strings.TrimPrefix(reqPath, replicationSourcePath))
	case reqPath == replicationTargetPath+"status/":
		c.handleListStatus(w, r, false)
	case strings.HasPrefix(reqPath, replicationTargetPath):
		c.handleTargetResource(w, r, strings.TrimPrefix(reqPath, replicationTargetPath))
	default:
		return false
	}
	return true
}

// handleSourceCollection implements POST /v2/replication/source-relationships/.
//
// Like the real array it wants the source directory to exist and answers
// AWAITING_AUTHORIZATION: nothing is replicated, and no target directory
// exists, until the target end authorizes.
func (c *cluster) handleSourceCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		c.mu.Lock()
		defer c.mu.Unlock()
		out := make([]map[string]any, 0, len(c.relationships))
		for _, rel := range c.sortedRelationshipsLocked() {
			if rel.hasSource {
				out = append(out, relationshipConfig(rel))
			}
		}
		writeJSON(w, http.StatusOK, out)
	case http.MethodPost:
		var body struct {
			SourceRootPath     string          `json:"source_root_path"`
			TargetRootPath     string          `json:"target_root_path"`
			TargetAddress      string          `json:"target_address"`
			TargetPort         int             `json:"target_port"`
			ReplicationEnabled bool            `json:"replication_enabled"`
			ReplicationMode    string          `json:"replication_mode"`
			SnapshotPolicies   json.RawMessage `json:"snapshot_policies"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "decode_error", err.Error())
			return
		}
		for name, value := range map[string]string{
			"source_root_path": body.SourceRootPath,
			"target_root_path": body.TargetRootPath,
			"target_address":   body.TargetAddress,
		} {
			if value == "" {
				writeError(w, http.StatusBadRequest, "decode_error", "field '"+name+"' not found")
				return
			}
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		c.reapFinishedJobsLocked()

		source := normalizePath(body.SourceRootPath)
		if _, exists := c.idsByPath[source]; !exists {
			writeError(w, http.StatusNotFound, "fs_no_such_entry_error", "no such directory "+source)
			return
		}
		// One relationship per source root toward one target root: a second
		// create for the same pair is what a retried Enable must not produce.
		for _, existing := range c.relationships {
			if existing.hasSource && existing.SourceRootPath == source &&
				existing.TargetRootPath == normalizePath(body.TargetRootPath) {
				writeError(w, http.StatusConflict, "replication_relationship_exists_error",
					"a relationship already replicates "+source+" to that target")
				return
			}
		}

		mode := body.ReplicationMode
		if mode == "" {
			mode = modeContinuous
		}
		port := body.TargetPort
		if port == 0 {
			port = 3712
		}
		policies := body.SnapshotPolicies
		if len(policies) == 0 {
			policies = json.RawMessage(`[]`)
		}
		rel := &relationship{
			ID:                 fmt.Sprintf("%08x-0000-4000-8000-%012d", c.nextRelSeq, c.nextRelSeq),
			SourceRootPath:     source,
			TargetRootPath:     normalizePath(body.TargetRootPath),
			TargetAddress:      body.TargetAddress,
			TargetPort:         port,
			ReplicationEnabled: body.ReplicationEnabled,
			ReplicationMode:    mode,
			SnapshotPolicies:   policies,
			State:              stateAwaitingAuthorization,
			hasSource:          true,
			hasTarget:          true,
		}
		c.nextRelSeq++
		c.relationships[rel.ID] = rel
		slog.Info("created replication relationship", "id", rel.ID,
			"source", rel.SourceRootPath, "target", rel.TargetRootPath, "mode", rel.ReplicationMode)
		writeJSON(w, http.StatusOK, relationshipConfig(rel))
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed_error", r.Method+" not allowed")
	}
}

// handleListStatus implements GET .../{source,target}-relationships/status/.
// The listing is a bare JSON array, as the real v2 API renders it.
func (c *cluster) handleListStatus(w http.ResponseWriter, r *http.Request, source bool) {
	if !methodIs(w, r, http.MethodGet) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]map[string]any, 0, len(c.relationships))
	for _, rel := range c.sortedRelationshipsLocked() {
		if (source && rel.hasSource) || (!source && rel.hasTarget) {
			out = append(out, c.relationshipStatusLocked(rel, source))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// handleSourceResource serves GET/PATCH/DELETE on a source relationship and
// POST on its replicate verb.
func (c *cluster) handleSourceResource(w http.ResponseWriter, r *http.Request, rest string) {
	id, verb, _ := strings.Cut(rest, "/")

	c.mu.Lock()
	defer c.mu.Unlock()

	rel, ok := c.relationships[id]
	if !ok || !rel.hasSource {
		writeError(w, http.StatusNotFound, "http_not_found_error", "no source relationship "+id)
		return
	}

	switch {
	case verb == "" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, relationshipConfig(rel))
	case verb == "" && r.Method == http.MethodPatch:
		c.patchSourceLocked(w, r, rel)
	case verb == "" && r.Method == http.MethodDelete:
		rel.hasSource = false
		c.endOrDropLocked(rel, "source relationship deleted")
		slog.Info("deleted source relationship", "id", id)
		writeJSON(w, http.StatusOK, map[string]any{})
	case verb == "status" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, c.relationshipStatusLocked(rel, true))
	case verb == "replicate" && r.Method == http.MethodPost:
		// Measured: refused in every mode while automatic replication is on.
		// A tool that used this as its final sync was told no, and the freeze
		// plus wait became the drain instead.
		if rel.ReplicationEnabled {
			writeError(w, http.StatusBadRequest, "replication_manual_replication_disallowed_error",
				"Unable to start manual replication when replication is enabled.")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed_error",
			fmt.Sprintf("%s %s not allowed here", r.Method, verb))
	}
}

// patchSourceLocked applies a partial update. Only fields present in the body
// change, which is the property the driver leans on when it freezes a source
// without touching its mode or policy links.
func (c *cluster) patchSourceLocked(w http.ResponseWriter, r *http.Request, rel *relationship) {
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "decode_error", err.Error())
		return
	}
	if v, ok := raw["source_root_read_only"]; ok {
		var ro bool
		if err := json.Unmarshal(v, &ro); err != nil {
			writeError(w, http.StatusBadRequest, "decode_error", err.Error())
			return
		}
		rel.SourceRootReadOnly = ro
	}
	if v, ok := raw["replication_enabled"]; ok {
		if err := json.Unmarshal(v, &rel.ReplicationEnabled); err != nil {
			writeError(w, http.StatusBadRequest, "decode_error", err.Error())
			return
		}
	}
	if v, ok := raw["replication_mode"]; ok {
		if err := json.Unmarshal(v, &rel.ReplicationMode); err != nil {
			writeError(w, http.StatusBadRequest, "decode_error", err.Error())
			return
		}
	}
	if v, ok := raw["snapshot_policies"]; ok {
		rel.SnapshotPolicies = v
	}
	// A freeze starts no replication job (measured on Core 7.9.2.2):
	// the recovery point keeps advancing on the
	// array's fixed cycle, which relationshipStatusLocked models.
	slog.Info("patched source relationship", "id", rel.ID, "readOnly", rel.SourceRootReadOnly,
		"mode", rel.ReplicationMode, "enabled", rel.ReplicationEnabled, "jobStarted", !rel.jobStart.IsZero())
	writeJSON(w, http.StatusOK, relationshipConfig(rel))
}

// handleTargetResource serves the target-end verbs and status.
func (c *cluster) handleTargetResource(w http.ResponseWriter, r *http.Request, rest string) {
	id, verb, _ := strings.Cut(rest, "/")

	c.mu.Lock()
	defer c.mu.Unlock()

	rel, ok := c.relationships[id]
	if !ok || !rel.hasTarget {
		// The real class for a relationship the target array has not heard
		// of. The driver retries on it, because on a real pair the relationship
		// may simply not have propagated yet.
		writeError(w, http.StatusNotFound, "http_not_found_error", "no target relationship "+id)
		return
	}

	if verb == "status" {
		if !methodIs(w, r, http.MethodGet) {
			return
		}
		writeJSON(w, http.StatusOK, c.relationshipStatusLocked(rel, false))
		return
	}
	if !methodIs(w, r, http.MethodPost) {
		return
	}

	switch verb {
	case "authorize":
		c.authorizeLocked(w, r, rel)
	case "make-writable":
		if rel.State != stateEstablished {
			// Measured on a lab array. The description is the array's own.
			writeError(w, http.StatusBadRequest, "relationship_manager_irreplicable_error",
				"Disconnect and revert is not allowed on a disconnected relationship")
			return
		}
		rel.State = stateDisconnected
		rel.sourceEndState = stateEstablished
		rel.TargetRootReadOnly = false
		slog.Info("target made writable", "id", rel.ID, "recoveryPoint", rel.recoveryPoint)
		writeJSON(w, http.StatusOK, c.relationshipStatusLocked(rel, false))
	case "reconnect":
		if rel.State != stateDisconnected {
			writeError(w, http.StatusBadRequest, "relationship_manager_irreplicable_error",
				"Reconnect is only allowed on a disconnected relationship")
			return
		}
		// The revert to the recovery point is what a real array does here;
		// the mock has no divergent writes to discard, so only the state
		// moves. A new job completes at once.
		rel.State = stateEstablished
		rel.sourceEndState = ""
		rel.TargetRootReadOnly = true
		rel.recoveryPoint = time.Now().UTC()
		slog.Info("target reconnected", "id", rel.ID)
		writeJSON(w, http.StatusOK, c.relationshipStatusLocked(rel, false))
	case "delete":
		rel.hasTarget = false
		rel.TargetRootReadOnly = false
		c.endOrDropLocked(rel, "target relationship deleted")
		slog.Info("deleted target relationship", "id", rel.ID)
		writeJSON(w, http.StatusOK, map[string]any{})
	default:
		writeError(w, http.StatusNotFound, "http_not_found_error", "unknown verb "+verb)
	}
}

// authorizeLocked implements POST .../target-relationships/{id}/authorize.
//
// This is where the target directory comes into being, and only with
// allow-fs-path-create -- the quota the driver pre-stages needs the file ID
// this allocates, which is why nothing can be staged before this call.
func (c *cluster) authorizeLocked(w http.ResponseWriter, r *http.Request, rel *relationship) {
	if rel.State != stateAwaitingAuthorization {
		writeError(w, http.StatusBadRequest, "relationship_manager_already_authorized_error",
			"relationship "+rel.ID+" is "+rel.State)
		return
	}
	query := r.URL.Query()
	allowCreate := query.Get("allow-fs-path-create") == "true"
	allowNonEmpty := query.Get("allow-non-empty-directory") == "true"

	if _, exists := c.idsByPath[rel.TargetRootPath]; !exists {
		if !allowCreate {
			writeError(w, http.StatusNotFound, "fs_no_such_entry_error",
				"target directory "+rel.TargetRootPath+" does not exist; pass allow-fs-path-create")
			return
		}
		c.addAncestorsLocked(rel.TargetRootPath)
	} else if !allowNonEmpty {
		targetID := c.idsByPath[rel.TargetRootPath]
		for _, node := range c.filesByID {
			if node.Path != rel.TargetRootPath && strings.HasPrefix(node.Path, rel.TargetRootPath+"/") {
				writeError(w, http.StatusBadRequest, "relationship_manager_target_path_not_empty_error",
					fmt.Sprintf("target directory %s (%s) is not empty; pass allow-non-empty-directory",
						rel.TargetRootPath, targetID))
				return
			}
		}
	}

	rel.State = stateEstablished
	rel.TargetRootReadOnly = true
	// The initial sync completes instantly here; on a real array it takes as
	// long as the data needs, and the driver must not declare the volume
	// protected until this is set.
	rel.recoveryPoint = time.Now().UTC()
	c.mirrorSubtreeLocked(rel.SourceRootPath, rel.TargetRootPath)
	slog.Info("authorized replication relationship", "id", rel.ID, "target", rel.TargetRootPath)
	writeJSON(w, http.StatusOK, c.relationshipStatusLocked(rel, false))
}

// handleReverse implements POST .../reverse-target-relationship, issued on
// the target array.
//
// Measured behaviour it reproduces: the ID is kept, roles swap, both ends stay
// DISCONNECTED until the new target reconnects, and the configuration comes
// back as REPLICATION_CONTINUOUS with no snapshot policies regardless of what
// it was. That last reset is Core behaviour observed in every reverse in lab
// testing, and is the reason the driver re-applies the class mode
// straight after.
func (c *cluster) handleReverse(w http.ResponseWriter, r *http.Request) {
	if !methodIs(w, r, http.MethodPost) {
		return
	}
	var body struct {
		TargetRelationshipID string `json:"target_relationship_id"`
		SourceAddress        string `json:"source_address"`
		SourcePort           int    `json:"source_port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "decode_error", err.Error())
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	rel, ok := c.relationships[body.TargetRelationshipID]
	if !ok || !rel.hasTarget {
		writeError(w, http.StatusNotFound, "http_not_found_error", "no target relationship "+body.TargetRelationshipID)
		return
	}
	if rel.State != stateDisconnected {
		writeError(w, http.StatusBadRequest, "relationship_manager_irreplicable_error",
			"Reverse is only allowed on a disconnected relationship")
		return
	}

	rel.SourceRootPath, rel.TargetRootPath = rel.TargetRootPath, rel.SourceRootPath
	rel.TargetAddress = body.SourceAddress
	if body.SourcePort != 0 {
		rel.TargetPort = body.SourcePort
	}
	// The old source's freeze carries over as the new target's read-only
	// bit; the new source is writable, which is the whole point.
	rel.TargetRootReadOnly = rel.SourceRootReadOnly
	rel.SourceRootReadOnly = false
	rel.ReplicationMode = modeContinuous
	rel.SnapshotPolicies = json.RawMessage(`[]`)
	rel.State = stateDisconnected
	rel.sourceEndState = ""
	slog.Info("reversed replication relationship", "id", rel.ID,
		"newSource", rel.SourceRootPath, "newTarget", rel.TargetRootPath)
	writeJSON(w, http.StatusOK, relationshipConfig(rel))
}

// endOrDropLocked removes a relationship once both ends are gone, and marks
// it ENDED while one remains -- the orphan an out-of-band delete leaves.
func (c *cluster) endOrDropLocked(rel *relationship, reason string) {
	if !rel.hasSource && !rel.hasTarget {
		delete(c.relationships, rel.ID)
		return
	}
	rel.State = stateEnded
	rel.EndReason = reason
}

// sortedRelationshipsLocked returns relationships in creation order, so
// listings are stable.
func (c *cluster) sortedRelationshipsLocked() []*relationship {
	out := make([]*relationship, 0, len(c.relationships))
	for _, rel := range c.relationships {
		out = append(out, rel)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// relationshipConfig renders the source-end configuration, the shape a
// create, patch or reverse returns.
func relationshipConfig(rel *relationship) map[string]any {
	return map[string]any{
		"id":                    rel.ID,
		"target_address":        rel.TargetAddress,
		"target_port":           rel.TargetPort,
		"source_root_read_only": rel.SourceRootReadOnly,
		"replication_enabled":   rel.ReplicationEnabled,
		"replication_mode":      rel.ReplicationMode,
		"snapshot_policies":     rel.SnapshotPolicies,
		"blackout_windows":      []any{},
	}
}

// relationshipStatusLocked renders one end's status.
//
// The recovery point is what the driver gates promote on, so its movement is
// the one piece of replication the mock has to fake honestly: while the
// relationship is ESTABLISHED, enabled and in a mode that includes continuous
// replication, the recovery point is "now minus -replication-lag", so a
// freeze passes the gate once the lag has elapsed and not before. In
// policy-only mode it stays put -- exactly the case that made the driver
// switch modes for a planned drain rather than wait for the next fire.
func (c *cluster) relationshipStatusLocked(rel *relationship, source bool) map[string]any {
	now := time.Now().UTC()
	// Tracking the clock needs an established, enabled, continuous-capable
	// relationship with no job running -- frozen or not: measured on Core
	// 7.9.2.2 (2026-09-15), the cycle runs every 60 s whether the source
	// is frozen or idle, and a freeze starts no job.
	if rel.State == stateEstablished && rel.ReplicationEnabled && rel.ReplicationMode != "REPLICATION_SNAPSHOT_POLICY" &&
		rel.jobStart.IsZero() {
		if advanced := now.Add(-c.replicationLag); advanced.After(rel.recoveryPoint) {
			rel.recoveryPoint = advanced
			c.replicateLocked(rel)
		}
	}
	// A job in flight (started by the manual-replicate path when the mock
	// allows it, or by a test): RUNNING for -replication-lag, then complete
	// with the recovery point at its start and a new snapshot id.
	jobState := jobNotRunning
	var jobStatus, recoverySnapshot any
	if !rel.jobStart.IsZero() {
		if now.Before(rel.jobStart.Add(c.replicationLag)) {
			jobState = jobRunning
			jobStatus = map[string]any{"percent_complete": 50.0, "bytes_transferred": "0"}
		} else {
			rel.recoveryPoint = rel.jobStart
			c.replicateLocked(rel)
			rel.jobStart = time.Time{}
		}
	}
	if rel.repSnapID > 0 {
		recoverySnapshot = map[string]any{"id": rel.repSnapID}
	}

	recoveryPoint := ""
	if !rel.recoveryPoint.IsZero() {
		recoveryPoint = rel.recoveryPoint.Format(time.RFC3339Nano)
	}
	// Both root paths are rendered with a trailing slash, as the real array
	// does; the client is expected to clean them.
	status := map[string]any{
		"id":                      rel.ID,
		"state":                   rel.stateAsSeenBy(source),
		"end_reason":              rel.EndReason,
		"source_cluster_name":     c.clusterName,
		"source_cluster_uuid":     c.clusterUUID,
		"target_cluster_name":     c.clusterName,
		"target_cluster_uuid":     c.clusterUUID,
		"source_root_path":        rel.SourceRootPath + "/",
		"source_root_read_only":   rel.SourceRootReadOnly,
		"target_root_path":        rel.TargetRootPath + "/",
		"target_root_read_only":   rel.TargetRootReadOnly,
		"replication_enabled":     rel.ReplicationEnabled,
		"job_state":               jobState,
		"job_start_time":          "",
		"recovery_point":          recoveryPoint,
		"error_from_last_job":     "",
		"duration_of_last_job":    map[string]string{"nanoseconds": "0"},
		"replication_job_status":  jobStatus,
		"recovery_point_snapshot": recoverySnapshot,
	}
	if source {
		status["target_address"] = rel.TargetAddress
		status["target_port"] = rel.TargetPort
		status["replication_mode"] = rel.ReplicationMode
		status["snapshot_policies"] = rel.SnapshotPolicies
		status["queued_snapshot_count"] = 0
		status["next_active_time"] = ""
		if id, ok := c.idsByPath[rel.SourceRootPath]; ok {
			status["source_root_id"] = id
		}
	} else {
		// null while connected, as measured; the driver must not key on it.
		status["source_address"] = nil
		status["source_port"] = nil
		status["lock_key"] = nil
		if id, ok := c.idsByPath[rel.TargetRootPath]; ok {
			status["target_root_id"] = id
		}
	}
	return status
}

// handleTimeStatus implements GET /v1/time/status, the array's own clock.
func (c *cluster) handleTimeStatus(w http.ResponseWriter, r *http.Request) {
	if !methodIs(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"config": map[string]any{"use_ad_for_primary": false, "ntp_servers": []string{}},
		"time":   time.Now().UTC().Format(time.RFC3339Nano),
		"status": "TIME_SYNCHRONIZING",
	})
}

// handlePolicies implements the snapshot-policy collection and resource:
// GET/POST /v1/snapshots/policies/ and GET/DELETE /v1/snapshots/policies/{id}.
//
// Added for the replication service, which links a volume's policy to a
// relationship and recreates it on the new source after a reverse. The
// schedule body is stored raw and echoed back, because the driver reads it
// back to copy it and the mock has no opinion about its contents.
func (c *cluster) handlePolicies(w http.ResponseWriter, r *http.Request, rest string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if rest == "" {
		switch r.Method {
		case http.MethodGet:
			ids := make([]int64, 0, len(c.policies))
			for id := range c.policies {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			entries := make([]map[string]any, 0, len(ids))
			for _, id := range ids {
				entries = append(entries, policyResponse(c.policies[id]))
			}
			writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
		case http.MethodPost:
			var body struct {
				Name          string          `json:"name"`
				SourceFileIDs []string        `json:"source_file_ids"`
				Schedules     json.RawMessage `json:"schedules"`
				Enabled       bool            `json:"enabled"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				writeError(w, http.StatusBadRequest, "decode_error", err.Error())
				return
			}
			for _, fileID := range body.SourceFileIDs {
				if _, exists := c.filesByID[fileID]; !exists {
					writeError(w, http.StatusNotFound, "fs_no_such_inode_error", "no such file "+fileID)
					return
				}
			}
			policy := &snapshotPolicy{
				ID: c.nextPolicyID, Name: body.Name, SourceFileIDs: body.SourceFileIDs,
				Schedules: body.Schedules, Enabled: body.Enabled,
			}
			c.nextPolicyID++
			c.policies[policy.ID] = policy
			slog.Info("created snapshot policy", "id", policy.ID, "name", policy.Name)
			writeJSON(w, http.StatusOK, policyResponse(policy))
		default:
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed_error", r.Method+" not allowed")
		}
		return
	}

	id, err := strconv.ParseInt(strings.TrimSuffix(rest, "/"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "decode_error", "policy ID "+rest+" is not a number")
		return
	}
	policy, ok := c.policies[id]
	if !ok {
		writeError(w, http.StatusNotFound, "api_snapshots_policy_not_found_error", "no such policy "+rest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, policyResponse(policy))
	case http.MethodDelete:
		delete(c.policies, id)
		writeJSON(w, http.StatusOK, map[string]any{})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed_error", r.Method+" not allowed")
	}
}

func policyResponse(policy *snapshotPolicy) map[string]any {
	schedules := policy.Schedules
	if len(schedules) == 0 {
		schedules = json.RawMessage(`[]`)
	}
	return map[string]any{
		// A bare number, as the real API renders policy IDs.
		"id":              policy.ID,
		"name":            policy.Name,
		"source_file_ids": policy.SourceFileIDs,
		"schedules":       schedules,
		"enabled":         policy.Enabled,
	}
}

// mirrorSubtreeLocked copies the directory structure under source to the
// same shape under target, which is what the initial replication job does.
//
// It matters for the group form: a group relationship is rooted at a class's
// basePath and pre-staging it means putting one quota on each *member*
// directory under the replica root. Those directories exist on the target
// only because replication created them, so a mock whose authorize created
// the root alone would make every group pre-stage look like a volume that had
// not replicated yet. Files are not modelled anywhere in this mock, so only
// directories are copied -- which is exactly what a quota needs.
func (c *cluster) mirrorSubtreeLocked(source, target string) {
	if source == "" || target == "" {
		return
	}
	source = normalizePath(source)
	prefix := strings.TrimSuffix(source, "/") + "/"

	paths := make([]string, 0, len(c.idsByPath))
	for p := range c.idsByPath {
		if strings.HasPrefix(p, prefix) {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	for _, p := range paths {
		replica := path.Join(target, strings.TrimPrefix(p, prefix))
		if _, exists := c.idsByPath[replica]; !exists {
			c.addAncestorsLocked(replica)
		}
	}
}

// endRelationshipsUnderLocked reproduces the measured consequence of
// tree-deleting a directory that is one end of a replication relationship.
//
// Core allows that delete. It does not refuse it, does not remove the
// relationship, and does not warn: the relationship simply goes ENDED on
// *both* arrays with an empty source_root_path, and the copy on the other
// array is left behind with its export and its quota.
//
// It is modelled here for one reason: DeleteVolume's guard is supposed to
// refuse before the tree-delete is ever issued, and a mock that quietly
// survived the delete would let a broken guard pass its test. With this, a
// test can delete a protected volume and see the relationship end -- which is
// the failure, not the success.
func (c *cluster) endRelationshipsUnderLocked(root string) {
	root = normalizePath(root)
	under := func(p string) bool {
		return p != "" && (p == root || strings.HasPrefix(p, strings.TrimSuffix(root, "/")+"/"))
	}
	for _, rel := range c.relationships {
		if rel.State == stateEnded {
			continue
		}
		switch {
		case under(rel.SourceRootPath):
			slog.Warn("tree-delete removed a replication source root; relationship ENDED",
				"id", rel.ID, "path", rel.SourceRootPath)
			rel.SourceRootPath = ""
			rel.State = stateEnded
			rel.EndReason = "REPLICATION_END_REASON_SOURCE_DIRECTORY_DELETED"
		case under(rel.TargetRootPath):
			slog.Warn("tree-delete removed a replication target root; relationship ENDED",
				"id", rel.ID, "path", rel.TargetRootPath)
			rel.TargetRootPath = ""
			rel.State = stateEnded
			rel.EndReason = "REPLICATION_END_REASON_TARGET_DIRECTORY_DELETED"
		}
	}
}

// stateAsSeenBy is the state one end's status list reports. The two ends of a
// real relationship do not agree after a make-writable (see sourceEndState).
func (rel *relationship) stateAsSeenBy(source bool) string {
	if source && rel.sourceEndState != "" {
		return rel.sourceEndState
	}
	return rel.State
}

// replicateLocked models a replication cycle completing: the target now holds
// the source subtree as it is, under a new snapshot id from the cluster's one
// sequence. A cycle on an unchanged tree still yields a new id, as on Core.
func (c *cluster) replicateLocked(rel *relationship) {
	rootID, ok := c.idsByPath[rel.SourceRootPath]
	if !ok {
		return
	}
	frozen := c.freezeSubtreeLocked(rootID)
	if rel.repSnapID > 0 && sameSubtree(rel.repFrozen, frozen) {
		return
	}
	rel.repSnapID = c.nextSnapID
	c.nextSnapID++
	rel.repFrozen = frozen
}

// sameSubtree reports whether two captured subtrees hold the same entries.
func sameSubtree(a, b map[string][]*fileNode) bool {
	return strings.Join(subtreeKeys(a), "\n") == strings.Join(subtreeKeys(b), "\n")
}

func subtreeKeys(m map[string][]*fileNode) []string {
	var keys []string
	for parent, children := range m {
		for _, child := range children {
			keys = append(keys, parent+"/"+child.Path+"/"+child.Type)
		}
	}
	sort.Strings(keys)
	return keys
}

// handleChangesSince implements GET /v2/snapshots/{newer}/changes-since/{older}
// for a marker snapshot and a replicated snapshot of the same tree: the
// entries are the paths whose presence differs. Only emptiness is what the
// driver reads.
func (c *cluster) handleChangesSince(w http.ResponseWriter, r *http.Request, newerID, olderID string) {
	if !methodIs(w, r, http.MethodGet) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	lookup := func(id string) (map[string][]*fileNode, bool) {
		if snap, ok := c.snapshots[id]; ok {
			return snap.frozen, true
		}
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			return nil, false
		}
		for _, rel := range c.relationships {
			if rel.repSnapID == n {
				return rel.repFrozen, true
			}
		}
		return nil, false
	}
	newer, ok1 := lookup(newerID)
	older, ok2 := lookup(olderID)
	if !ok1 || !ok2 {
		writeError(w, http.StatusNotFound, "api_snapshot_not_found_error", "no such snapshot")
		return
	}
	set := func(keys []string) map[string]bool {
		m := map[string]bool{}
		for _, k := range keys {
			m[k] = true
		}
		return m
	}
	nk, ok := set(subtreeKeys(newer)), set(subtreeKeys(older))
	entries := []map[string]any{}
	for k := range nk {
		if !ok[k] {
			entries = append(entries, map[string]any{"op": "CREATE", "path": k})
		}
	}
	for k := range ok {
		if !nk[k] {
			entries = append(entries, map[string]any{"op": "DELETE", "path": k})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}
