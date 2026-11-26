// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"path"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"qumulo-csi-driver/pkg/qumulo"
)

// replicationCollector exports the three replication series.
//
// It is a hand-written collector rather than three GaugeVecs for two
// reasons, both of which come straight out of the lab.
//
// Recovery-point age is a function of *now*, so it has to be computed when
// the scrape happens and not when the status was read -- a gauge set at
// observation time would be a frozen age that only looks fresh. And the age
// has to be measured on the array's clock, because the array is what stamps
// recovery_point; the drift between it and the controller's clock is
// recorded here as a skew and added back at collection time.
//
// This is why age is the health signal at all: a target array reported
// ESTABLISHED with no error for 25 minutes while its source was dead. State
// is exported too, but it is the description, not the alarm.
type replicationCollector struct {
	mu sync.Mutex
	// ends is keyed by relationship and role. A relationship read from both
	// ends disagrees about state and about how far behind it is -- that
	// disagreement is the signal, so both are kept.
	ends map[relationshipEnd]endState
	// byArray indexes ends by the array and role that reported them, so a
	// complete listing prunes only that array's ends and retiring an array
	// touches only its own, instead of every event scanning every end under
	// the lock scrapes take.
	byArray map[arrayRole]map[string]struct{}
	// skew is arrayClock - controllerClock, per array, from the last
	// ClusterTime call. Zero for an array whose clock was never read, which
	// degrades the age to the controller's clock rather than to nothing.
	skew map[string]time.Duration
	// removed is when each relationship's Removed was recorded, kept for
	// removedMemory, so a listing whose request started before it -- and so
	// may predate the delete -- does not bring its series back.
	removed map[string]time.Time
	// intervals is each policy-mode relationship's snapshot schedule
	// interval. Emitted only while the relationship has an end, so
	// it retires with the ends.
	intervals map[string]time.Duration
	// now is overridable so the tests can pin a clock.
	now func() time.Time

	ageDesc      *prometheus.Desc
	stateDesc    *prometheus.Desc
	queuedDesc   *prometheus.Desc
	modeDesc     *prometheus.Desc
	intervalDesc *prometheus.Desc
}

type relationshipEnd struct {
	id   string
	role string
}

// arrayRole is the index key: one array's ends of one role.
type arrayRole struct {
	cluster string
	role    string
}

// setEnd stores an end and keeps the index in step, moving it if the same
// end was last reported under another array label.
func (c *replicationCollector) setEnd(end relationshipEnd, state endState) {
	if old, ok := c.ends[end]; ok && old.cluster != state.cluster {
		c.unindex(arrayRole{old.cluster, end.role}, end.id)
	}
	c.ends[end] = state
	key := arrayRole{state.cluster, end.role}
	if c.byArray[key] == nil {
		c.byArray[key] = map[string]struct{}{}
	}
	c.byArray[key][end.id] = struct{}{}
}

// deleteEnd removes an end, if there is one, and its index entry.
func (c *replicationCollector) deleteEnd(end relationshipEnd) {
	state, ok := c.ends[end]
	if !ok {
		return
	}
	delete(c.ends, end)
	c.unindex(arrayRole{state.cluster, end.role}, end.id)
}

func (c *replicationCollector) unindex(key arrayRole, id string) {
	delete(c.byArray[key], id)
	if len(c.byArray[key]) == 0 {
		delete(c.byArray, key)
	}
}

// retireStaleEnds drops every end nothing has refreshed for olderThan.
//
// The recovery-point age is computed at scrape time from the stored recovery
// point, so an end nobody refreshes exports an age that grows by itself and
// eventually trips QumuloCSIReplicationRecoveryPointStale on its own.
// That happens on every csi-addons leadership change: the sidecar elects its
// own leader, so the replica that held it goes on exporting the ends
// it last saw. Before this they were dropped only when the whole endpoint
// had been idle for an hour, which is both far too late and conditional on
// nothing else calling that array.
//
// Safe on age alone, unlike a quota: these ends are refreshed by the
// csi-addons poll of every VolumeReplication, so going unrefreshed is itself
// proof that another replica is serving the relationship. A relationship's
// interval is emitted only while it has an end, so it retires with them.
func (c *replicationCollector) retireStaleEnds(olderThan time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := c.now().Add(-olderThan)
	stale := make([]relationshipEnd, 0, len(c.ends))
	for end, state := range c.ends {
		if state.observedAt.Before(cutoff) {
			stale = append(stale, end)
		}
	}
	for _, end := range stale {
		c.deleteEnd(end)
	}
	return len(stale)
}

// retireCluster drops every end one array reported, and its clock skew. It
// runs when the endpoint's other series retire: an array nothing calls any
// more -- a decommissioned peer -- otherwise kept its ends for ever, their
// recovery-point age growing at every scrape.
func (c *replicationCollector) retireCluster(cluster string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	retired := 0
	for _, role := range []string{qumulo.RoleSource, qumulo.RoleTarget} {
		for id := range c.byArray[arrayRole{cluster, role}] {
			c.deleteEnd(relationshipEnd{id: id, role: role})
			retired++
		}
	}
	delete(c.skew, cluster)
	return retired
}

// relatedRoot reports whether a relationship rooted at root is about the
// volume at volumePath: the volume's own directory, a directory above it (a
// group's root), or one below it.
func relatedRoot(root, volumePath string) bool {
	if root == "" {
		return false
	}
	root, volumePath = path.Clean(root), path.Clean(volumePath)
	if root == "/" || root == volumePath {
		return true
	}
	return strings.HasPrefix(volumePath, root+"/") || strings.HasPrefix(root, volumePath+"/")
}

// idAndState is the label pair of the state series, used as a set key so a
// relationship whose two ends report the same state is emitted once.
type idAndState struct {
	id    string
	state string
}

type endState struct {
	cluster          string
	state            string
	recoveryPoint    time.Time
	hasRecoveryPoint bool
	queuedSnapshots  int
	// mode is the relationship's replication mode. Only the source end
	// reports it; empty on a target end.
	mode       string
	observedAt time.Time
}

func newReplicationCollector() *replicationCollector {
	return &replicationCollector{
		ends:      make(map[relationshipEnd]endState),
		byArray:   make(map[arrayRole]map[string]struct{}),
		skew:      make(map[string]time.Duration),
		removed:   make(map[string]time.Time),
		intervals: make(map[string]time.Duration),
		now:       time.Now,
		ageDesc: prometheus.NewDesc(
			namespace+"_replication_recovery_point_age_seconds",
			"Seconds since the recovery point of a replication relationship, "+
				"measured on the array's own clock. This is the RPO: how much "+
				"data a failover would lose. Absent until a relationship has "+
				"completed one job. Updated when a replication RPC runs, not "+
				"on a timer.",
			[]string{"relationship_id", "role"}, nil),
		stateDesc: prometheus.NewDesc(
			namespace+"_replication_state",
			"Always 1, with the relationship's reported state as a label. One "+
				"series per distinct state seen across the ends that were "+
				"read; a relationship whose two ends disagree therefore shows "+
				"two. Do not alert on this -- a target array reports "+
				"ESTABLISHED with a dead source. Alert on "+
				"recovery_point_age_seconds.",
			[]string{"relationship_id", "state"}, nil),
		queuedDesc: prometheus.NewDesc(
			namespace+"_replication_queued_snapshots",
			"Policy snapshots waiting to replicate, from the most recently "+
				"read end of the relationship. A number that climbs and does "+
				"not fall is a source that cannot keep up.",
			[]string{"relationship_id"}, nil),
		// A policy-mode relationship's recovery point advances only
		// when a scheduled snapshot replicates, so its healthy age climbs to
		// the schedule interval before every step. Without the mode, one
		// threshold cannot tell that from a stalled continuous relationship.
		modeDesc: prometheus.NewDesc(
			namespace+"_replication_mode",
			"Always 1, with the relationship's replication mode as a label "+
				"(REPLICATION_CONTINUOUS, REPLICATION_SNAPSHOT_POLICY or "+
				"REPLICATION_SNAPSHOT_POLICY_WITH_CONTINUOUS). Reported by the "+
				"source end only, so absent for a relationship this controller "+
				"has read only from its target array. Join on relationship_id "+
				"to scope an alert on recovery_point_age_seconds by mode.",
			[]string{"relationship_id", "mode"}, nil),
		// A daily policy's healthy age climbs to a day, which no
		// fixed threshold fits alongside an hourly one.
		intervalDesc: prometheus.NewDesc(
			namespace+"_replication_policy_interval_seconds",
			"The longest gap between two snapshots of the snapshot policy "+
				"that drives a policy-mode relationship "+
				"(REPLICATION_SNAPSHOT_POLICY), read from the policy's schedule "+
				"on the source array. A healthy relationship's recovery-point "+
				"age climbs to about this before every step. Absent for any "+
				"other mode, and until the controller has read the policy. "+
				"Join on relationship_id to scale an alert on "+
				"recovery_point_age_seconds.",
			[]string{"relationship_id"}, nil),
	}
}

// removedMemory is how long a removal is remembered. A listing in flight
// when the delete landed is answered within the client's timeout, far inside
// this.
const removedMemory = time.Hour

// observe records what one replication-status call saw.
func (c *replicationCollector) observe(event qumulo.ReplicationEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	for id, at := range c.removed {
		if now.Sub(at) > removedMemory {
			delete(c.removed, id)
		}
	}

	// A deleted relationship: retire every end it had, whichever array
	// reported it. See ReplicationEvent.Removed.
	if event.Removed != "" {
		c.deleteEnd(relationshipEnd{id: event.Removed, role: qumulo.RoleSource})
		c.deleteEnd(relationshipEnd{id: event.Removed, role: qumulo.RoleTarget})
		delete(c.intervals, event.Removed)
		c.removed[event.Removed] = now
		return
	}

	// A policy schedule interval, or its absence.
	if event.PolicyIntervalOf != "" {
		if event.PolicyInterval > 0 {
			c.intervals[event.PolicyIntervalOf] = event.PolicyInterval
		} else {
			delete(c.intervals, event.PolicyIntervalOf)
		}
		return
	}

	// A clock-only event: the array told us what time it thinks it is.
	if !event.ArrayTime.IsZero() {
		c.skew[event.Cluster] = event.ArrayTime.Sub(now)
	}

	// A complete listing says which relationships this array still has for
	// this role: retire the ends it no longer lists, so a deleted
	// relationship's age stops growing on the dashboard.
	if event.Complete && event.Role != "" {
		listed := make(map[string]struct{}, len(event.Relationships))
		for _, status := range event.Relationships {
			listed[status.ID] = struct{}{}
		}
		for id := range c.byArray[arrayRole{event.Cluster, event.Role}] {
			if _, ok := listed[id]; !ok {
				c.deleteEnd(relationshipEnd{id: id, role: event.Role})
			}
		}
	}

	// A listing made for one volume is every relationship on the array,
	// another cluster's and an administrator's included. Only the volume's
	// own are kept; each used to get series and alerts.
	volumePath, labelled := handlePath(event.Subject)

	for _, status := range event.Relationships {
		if status.ID == "" || event.Role == "" {
			continue
		}
		if labelled && !relatedRoot(status.SourceRootPath, volumePath) && !relatedRoot(status.TargetRootPath, volumePath) {
			continue
		}
		// Listed by a request that went out before this relationship's
		// delete was recorded: the answer may predate the delete. An end
		// that in fact survived is listed again by the next request.
		if at, ok := c.removed[status.ID]; ok && !event.Started.IsZero() && event.Started.Before(at) {
			continue
		}
		c.setEnd(relationshipEnd{id: status.ID, role: event.Role}, endState{
			cluster:          event.Cluster,
			state:            status.State,
			recoveryPoint:    status.RecoveryPoint,
			hasRecoveryPoint: status.HasRecoveryPoint(),
			queuedSnapshots:  status.QueuedSnapshotCount,
			mode:             status.ReplicationMode,
			observedAt:       now,
		})
	}

	// An interval whose relationship has no end left here is not emitted;
	// drop it once a complete listing has had its say, so it does not grow.
	if event.Complete {
		for id := range c.intervals {
			_, source := c.ends[relationshipEnd{id: id, role: qumulo.RoleSource}]
			_, target := c.ends[relationshipEnd{id: id, role: qumulo.RoleTarget}]
			if !source && !target {
				delete(c.intervals, id)
			}
		}
	}
}

// Describe implements prometheus.Collector.
func (c *replicationCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ageDesc
	ch <- c.stateDesc
	ch <- c.queuedDesc
	ch <- c.modeDesc
	ch <- c.intervalDesc
}

// Collect implements prometheus.Collector.
//
// Every series is emitted at most once per label set: a registry rejects a
// gather that produced the same labels twice, and a relationship read from
// both ends would do exactly that for state and for queued snapshots if they
// were emitted per end.
func (c *replicationCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()

	// Distinct (relationship, state) pairs, and the freshest end per
	// relationship for the queue depth.
	states := make(map[idAndState]struct{}, len(c.ends))
	freshest := make(map[string]endState, len(c.ends))
	// The mode per relationship, from the freshest end that reported one.
	modes := make(map[string]endState, len(c.ends))

	for end, state := range c.ends {
		if state.hasRecoveryPoint {
			age := now.Add(c.skew[state.cluster]).Sub(state.recoveryPoint).Seconds()
			// A recovery point in the future means the two clocks disagree by
			// more than the lag. Reporting a negative age would render as a
			// dip below zero on every dashboard; zero is the honest floor.
			if age < 0 {
				age = 0
			}
			ch <- prometheus.MustNewConstMetric(
				c.ageDesc, prometheus.GaugeValue, age, end.id, end.role)
		}

		if state.state != "" {
			states[idAndState{id: end.id, state: state.state}] = struct{}{}
		}

		if previous, ok := freshest[end.id]; !ok || state.observedAt.After(previous.observedAt) {
			freshest[end.id] = state
		}

		if state.mode != "" {
			if previous, ok := modes[end.id]; !ok || state.observedAt.After(previous.observedAt) {
				modes[end.id] = state
			}
		}
	}

	for pair := range states {
		ch <- prometheus.MustNewConstMetric(
			c.stateDesc, prometheus.GaugeValue, 1, pair.id, pair.state)
	}

	for id, state := range freshest {
		ch <- prometheus.MustNewConstMetric(
			c.queuedDesc, prometheus.GaugeValue, float64(state.queuedSnapshots), id)
	}

	for id, state := range modes {
		ch <- prometheus.MustNewConstMetric(
			c.modeDesc, prometheus.GaugeValue, 1, id, state.mode)
	}

	// Only for a relationship that still has an end: an end retired by a
	// listing or a delete takes its interval series with it.
	for id, interval := range c.intervals {
		if _, ok := freshest[id]; ok {
			ch <- prometheus.MustNewConstMetric(
				c.intervalDesc, prometheus.GaugeValue, interval.Seconds(), id)
		}
	}
}
