// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// pinClock freezes the collector's clock so an age is an exact number.
func pinClock(r *Registry, at time.Time) {
	r.replication.now = func() time.Time { return at }
}

const relationshipID = "edfd8da2-dcca-4075-ae5d-bebaa97c1b98"

func TestRecoveryPointAgeIsMeasuredOnTheArraysClock(t *testing.T) {
	r := testRegistry()
	controllerNow := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	pinClock(r, controllerNow)

	// The array is five minutes ahead of the controller. Its recovery point
	// is stamped in its own time, so ageing it against the controller's
	// clock would report five minutes less lag than there is.
	arrayNow := controllerNow.Add(5 * time.Minute)
	r.ObserveReplication(context.Background(), qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", ArrayTime: arrayNow,
	})
	r.ObserveReplication(context.Background(), qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource,
		Relationships: []qumulo.RelationshipStatus{{
			ID:            relationshipID,
			State:         "ESTABLISHED",
			RecoveryPoint: arrayNow.Add(-10 * time.Minute),
		}},
	})

	expected := `
# HELP csi_qumulo_replication_recovery_point_age_seconds Seconds since the recovery point of a replication relationship, measured on the array's own clock. This is the RPO: how much data a failover would lose. Absent until a relationship has completed one job. Updated when a replication RPC runs, not on a timer.
# TYPE csi_qumulo_replication_recovery_point_age_seconds gauge
csi_qumulo_replication_recovery_point_age_seconds{relationship_id="edfd8da2-dcca-4075-ae5d-bebaa97c1b98",role="source"} 600
`
	require.NoError(t, testutil.GatherAndCompare(r.Gatherer(),
		strings.NewReader(expected), "csi_qumulo_replication_recovery_point_age_seconds"))
}

// A relationship that has never completed a job has no recovery point, and
// "0 seconds behind" would be the most dangerous possible lie.
func TestARelationshipWithNoRecoveryPointHasNoAgeSeries(t *testing.T) {
	r := testRegistry()

	r.ObserveReplication(context.Background(), qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource,
		Relationships: []qumulo.RelationshipStatus{{
			ID: relationshipID, State: "AWAITING_AUTHORIZATION",
		}},
	})

	assert.Equal(t, 0, testutil.CollectAndCount(r.replication,
		"csi_qumulo_replication_recovery_point_age_seconds"))
	assert.Equal(t, 1, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_state"))
}

// The target reports ESTABLISHED with no error while the source is
// dead. Both ends must be exportable, and the age must be per role, or the
// disagreement that is the whole signal is invisible.
// Measured on the lab cluster (2026-09-14): a relationship deleted out of
// band kept its series with an ever-growing age. A complete listing from an
// array retires the ends that array no longer has.
func TestACompleteListingRetiresVanishedRelationships(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)
	ctx := context.Background()

	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true,
		Relationships: []qumulo.RelationshipStatus{
			{ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now.Add(-time.Minute)},
			{ID: "gone-soon", State: "ESTABLISHED", RecoveryPoint: now.Add(-time.Minute)},
		},
	})
	assert.Equal(t, 2, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_recovery_point_age_seconds"))

	// A single-relationship read must not retire anything.
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource,
		Relationships: []qumulo.RelationshipStatus{{ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now}},
	})
	assert.Equal(t, 2, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_recovery_point_age_seconds"))

	// The next complete listing no longer has the second one.
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true,
		Relationships: []qumulo.RelationshipStatus{{ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now}},
	})
	assert.Equal(t, 1, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_recovery_point_age_seconds"),
		"the vanished relationship's series is retired")
}

func TestBothEndsOfARelationshipAreKeptSeparately(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)
	ctx := context.Background()

	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource,
		Relationships: []qumulo.RelationshipStatus{{
			ID: relationshipID, State: "DISCONNECTED",
			RecoveryPoint: now.Add(-25 * time.Minute), QueuedSnapshotCount: 3,
		}},
	})
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "b.example.com:8000", Role: qumulo.RoleTarget,
		Relationships: []qumulo.RelationshipStatus{{
			ID: relationshipID, State: "ESTABLISHED",
			RecoveryPoint: now.Add(-25 * time.Minute),
		}},
	})

	assert.Equal(t, 2, testutil.CollectAndCount(r.replication,
		"csi_qumulo_replication_recovery_point_age_seconds"),
		"one age per end")
	assert.Equal(t, 2, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_state"),
		"the two ends disagree, so both states are reported")
	assert.Equal(t, 1, testutil.CollectAndCount(r.replication,
		"csi_qumulo_replication_queued_snapshots"),
		"queue depth is one series per relationship, whichever end was read last")
}

// Two ends agreeing must not produce the same label set twice: a registry
// refuses a gather that does, and /metrics would start returning 500.
func TestAgreeingEndsCollapseToOneStateSeries(t *testing.T) {
	r := testRegistry()
	ctx := context.Background()

	for _, role := range []string{qumulo.RoleSource, qumulo.RoleTarget} {
		r.ObserveReplication(ctx, qumulo.ReplicationEvent{
			Cluster: "a.example.com:8000", Role: role,
			Relationships: []qumulo.RelationshipStatus{{ID: relationshipID, State: "ESTABLISHED"}},
		})
	}

	expected := `
# HELP csi_qumulo_replication_state Always 1, with the relationship's reported state as a label. One series per distinct state seen across the ends that were read; a relationship whose two ends disagree therefore shows two. Do not alert on this -- a target array reports ESTABLISHED with a dead source. Alert on recovery_point_age_seconds.
# TYPE csi_qumulo_replication_state gauge
csi_qumulo_replication_state{relationship_id="edfd8da2-dcca-4075-ae5d-bebaa97c1b98",state="ESTABLISHED"} 1
`
	require.NoError(t, testutil.GatherAndCompare(r.Gatherer(),
		strings.NewReader(expected), "csi_qumulo_replication_state"))
}

// Clock drift larger than the lag would otherwise render a negative age.
func TestAFutureRecoveryPointIsFlooredAtZero(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)

	r.ObserveReplication(context.Background(), qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource,
		Relationships: []qumulo.RelationshipStatus{{
			ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now.Add(time.Hour),
		}},
	})

	expected := `
# HELP csi_qumulo_replication_recovery_point_age_seconds Seconds since the recovery point of a replication relationship, measured on the array's own clock. This is the RPO: how much data a failover would lose. Absent until a relationship has completed one job. Updated when a replication RPC runs, not on a timer.
# TYPE csi_qumulo_replication_recovery_point_age_seconds gauge
csi_qumulo_replication_recovery_point_age_seconds{relationship_id="edfd8da2-dcca-4075-ae5d-bebaa97c1b98",role="source"} 0
`
	require.NoError(t, testutil.CollectAndCompare(r.replication,
		strings.NewReader(expected), "csi_qumulo_replication_recovery_point_age_seconds"))
}

// A status with no ID, or read through a call that does not say which end it
// came from, names no relationship and must be ignored rather than exported
// under an empty label.
func TestStatusesWithNoIdentityAreIgnored(t *testing.T) {
	r := testRegistry()
	ctx := context.Background()

	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource,
		Relationships: []qumulo.RelationshipStatus{{ID: "", State: "ESTABLISHED"}},
	})
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster:       "a.example.com:8000",
		Relationships: []qumulo.RelationshipStatus{{ID: relationshipID, State: "ESTABLISHED"}},
	})

	assert.Equal(t, 0, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_state"))
}

// Deleting the last relationship on an array pair leaves no complete
// listing to retire it -- the peer's end in particular is never listed again
// -- so the delete itself has to retire every end of it.
func TestADeletedRelationshipIsRetiredOnBothArrays(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)
	ctx := context.Background()

	rel := qumulo.RelationshipStatus{ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now.Add(-17 * time.Second)}
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true,
		Relationships: []qumulo.RelationshipStatus{rel},
	})
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "b.example.com:8000", Role: qumulo.RoleTarget, Complete: true,
		Relationships: []qumulo.RelationshipStatus{rel},
	})
	require.Equal(t, 2, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_recovery_point_age_seconds"))

	// Disable deletes the source end on A. Nothing lists B afterwards.
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{Cluster: "a.example.com:8000", Removed: relationshipID})

	pinClock(r, now.Add(2*time.Hour))
	assert.Equal(t, 0, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_recovery_point_age_seconds"),
		"neither end's age may outlive the relationship")
	assert.Equal(t, 0, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_state"))
}

// A removal names one relationship and retires nothing else.
func TestARemovalLeavesOtherRelationshipsAlone(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)
	ctx := context.Background()

	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true,
		Relationships: []qumulo.RelationshipStatus{
			{ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now},
			{ID: "stays", State: "ESTABLISHED", RecoveryPoint: now},
		},
	})
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{Cluster: "a.example.com:8000", Removed: relationshipID})

	assert.Equal(t, 1, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_recovery_point_age_seconds"))
}

// The mode is exported once per relationship, from the source end that
// reports it, so an alert can give policy-mode relationships their own
// threshold. A target end reports no mode and adds nothing.
func TestTheReplicationModeIsExportedFromTheSourceEnd(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)
	ctx := context.Background()

	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true,
		Relationships: []qumulo.RelationshipStatus{{
			ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now.Add(-20 * time.Minute),
			ReplicationMode: qumulo.ReplicationModeSnapshotPolicy,
		}},
	})
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "b.example.com:8000", Role: qumulo.RoleTarget, Complete: true,
		Relationships: []qumulo.RelationshipStatus{{
			ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now.Add(-20 * time.Minute),
		}},
	})

	expected := `
# HELP csi_qumulo_replication_mode Always 1, with the relationship's replication mode as a label (REPLICATION_CONTINUOUS, REPLICATION_SNAPSHOT_POLICY or REPLICATION_SNAPSHOT_POLICY_WITH_CONTINUOUS). Reported by the source end only, so absent for a relationship this controller has read only from its target array. Join on relationship_id to scope an alert on recovery_point_age_seconds by mode.
# TYPE csi_qumulo_replication_mode gauge
csi_qumulo_replication_mode{mode="REPLICATION_SNAPSHOT_POLICY",relationship_id="` + relationshipID + `"} 1
`
	require.NoError(t, testutil.CollectAndCompare(r.replication, strings.NewReader(expected),
		"csi_qumulo_replication_mode"))
}

// A relationship read only from its target array has no mode series: the
// alert then falls back to the continuous threshold rather than guessing.
func TestATargetOnlyRelationshipHasNoModeSeries(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)

	r.ObserveReplication(context.Background(), qumulo.ReplicationEvent{
		Cluster: "b.example.com:8000", Role: qumulo.RoleTarget, Complete: true,
		Relationships: []qumulo.RelationshipStatus{{ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now}},
	})

	assert.Equal(t, 0, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_mode"))
}

// The mode series goes with the relationship: a delete retires it.
func TestADeletedRelationshipTakesItsModeSeriesWithIt(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)
	ctx := context.Background()

	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true,
		Relationships: []qumulo.RelationshipStatus{{
			ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now,
			ReplicationMode: qumulo.ReplicationModeContinuous,
		}},
	})
	require.Equal(t, 1, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_mode"))

	r.ObserveReplication(ctx, qumulo.ReplicationEvent{Cluster: "a.example.com:8000", Removed: relationshipID})

	assert.Equal(t, 0, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_mode"))
}

// A complete listing and a delete of the same relationship
// can run at once (csi-addons polls Info while a Disable or DeleteVolume
// runs). A listing that started before the delete but is recorded after it
// still lists the relationship, and used to bring back every series the
// delete had just retired, its age growing, for as long as no later listing
// of that array came -- after the last relationship on a pair, never. A
// listing is now told apart from a fresh one by when its request began.
func TestAListingThatStartedBeforeADeleteDoesNotBringItsSeriesBack(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)
	ctx := context.Background()
	rel := qumulo.RelationshipStatus{ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now.Add(-time.Minute)}
	listing := func(started time.Time) qumulo.ReplicationEvent {
		return qumulo.ReplicationEvent{
			Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true,
			Relationships: []qumulo.RelationshipStatus{rel}, Started: started,
		}
	}
	r.ObserveReplication(ctx, listing(now.Add(-time.Minute)))

	// The listing's request goes out; the delete finishes; then the
	// listing's answer, taken before the delete, is recorded.
	started := now
	pinClock(r, now.Add(time.Second))
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{Cluster: "a.example.com:8000", Removed: relationshipID})
	pinClock(r, now.Add(2*time.Second))
	r.ObserveReplication(ctx, listing(started))

	assert.Equal(t, 0, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_recovery_point_age_seconds"),
		"a listing older than the delete does not resurrect the relationship")

	// A listing that began after the delete and still lists it is an end
	// that survived, and is exported again.
	r.ObserveReplication(ctx, listing(now.Add(3*time.Second)))
	assert.Equal(t, 1, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_recovery_point_age_seconds"))
}

// A listing made for one volume is every relationship on the array,
// another cluster's and an administrator's included, and each used to get
// series and alerts. A listing labelled with a volume now exports only the
// relationships whose root is that volume's directory, a directory above it
// (a group), or one below it. An unlabelled listing still updates and prunes.
func TestALabelledListingExportsOnlyTheVolumesOwnRelationships(t *testing.T) {
	r := testRegistry()
	pinClock(r, time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC))
	const subject = "a.example.com#https://a.example.com:8000#/csi/vols/pvc-1"

	r.ObserveReplication(context.Background(), qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true, Subject: subject,
		Relationships: []qumulo.RelationshipStatus{
			{ID: "own", State: "ESTABLISHED", SourceRootPath: "/csi/vols/pvc-1/"},
			{ID: "group", State: "ESTABLISHED", SourceRootPath: "/csi/vols"},
			{ID: "below", State: "ESTABLISHED", SourceRootPath: "/csi/vols/pvc-1/sub"},
			{ID: "foreign", State: "ESTABLISHED", SourceRootPath: "/home/admin"},
			{ID: "sibling", State: "ESTABLISHED", SourceRootPath: "/csi/vols/pvc-10"},
		},
	})
	r.ObserveReplication(context.Background(), qumulo.ReplicationEvent{
		Cluster: "b.example.com:8000", Role: qumulo.RoleTarget, Complete: true, Subject: subject,
		Relationships: []qumulo.RelationshipStatus{
			{ID: "own", State: "ESTABLISHED", TargetRootPath: "/dr/vols/pvc-1", SourceRootPath: "/csi/vols/pvc-1"},
			{ID: "foreign-target", State: "ESTABLISHED", TargetRootPath: "/dr/other", SourceRootPath: "/other"},
		},
	})

	ids := map[string]bool{}
	for _, end := range r.replication.endKeys() {
		ids[end.id] = true
	}
	assert.Equal(t, map[string]bool{"own": true, "group": true, "below": true}, ids)
}

// An array the controller has stopped calling -- a decommissioned
// peer -- kept its relationship ends and clock skew for ever, and their
// recovery-point age grew at every scrape. They are retired with the
// endpoint's other series.
func TestAnIdleEndpointsReplicationSeriesAreRetired(t *testing.T) {
	r := testRegistry()
	at := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	pinClock(r, at)
	advance := pinEndpointClock(r, at)
	for _, cluster := range []string{"gone.example.com:8000", "kept.example.com:8000"} {
		r.ObserveRequest(context.Background(), qumulo.RequestEvent{Cluster: cluster, Reachable: true})
		r.ObserveReplication(context.Background(), qumulo.ReplicationEvent{Cluster: cluster, ArrayTime: at})
		r.ObserveReplication(context.Background(), qumulo.ReplicationEvent{
			Cluster: cluster, Role: qumulo.RoleSource,
			Relationships: []qumulo.RelationshipStatus{{ID: cluster, State: "ESTABLISHED", RecoveryPoint: at}},
		})
	}

	advance(endpointIdleWindow)
	r.ObserveRequest(context.Background(), qumulo.RequestEvent{Cluster: "kept.example.com:8000", Reachable: true})
	r.RetireIdleEndpoints(context.Background())

	keys := r.replication.endKeys()
	require.Len(t, keys, 1)
	assert.Equal(t, "kept.example.com:8000", keys[0].id)
	_, skewKept := r.replication.skew["gone.example.com:8000"]
	assert.False(t, skewKept, "the idle array's clock skew is retired too")
}

// endKeys is every end the collector holds.
func (c *replicationCollector) endKeys() []relationshipEnd {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]relationshipEnd, 0, len(c.ends))
	for end := range c.ends {
		keys = append(keys, end)
	}
	return keys
}

// The schedule interval of a policy-mode relationship's snapshot
// policy, one series per relationship, so the stale alert can scale its
// threshold to the schedule. It is reported for a relationship the
// controller sees only from its target end too, which has no mode series.
func TestThePolicyIntervalIsExportedPerRelationship(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)
	ctx := context.Background()

	r.ObserveReplication(ctx, qumulo.ReplicationEvent{Cluster: "b.example.com:8000", PolicyIntervalOf: relationshipID, PolicyInterval: 24 * time.Hour})
	assert.Equal(t, 0, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_policy_interval_seconds"),
		"no series for a relationship with no end observed")

	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "b.example.com:8000", Role: qumulo.RoleTarget, Complete: true,
		Relationships: []qumulo.RelationshipStatus{{ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now}},
	})
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true,
		Relationships: []qumulo.RelationshipStatus{{ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now,
			ReplicationMode: qumulo.ReplicationModeSnapshotPolicy}},
	})

	expected := `
# HELP csi_qumulo_replication_policy_interval_seconds The longest gap between two snapshots of the snapshot policy that drives a policy-mode relationship (REPLICATION_SNAPSHOT_POLICY), read from the policy's schedule on the source array. A healthy relationship's recovery-point age climbs to about this before every step. Absent for any other mode, and until the controller has read the policy. Join on relationship_id to scale an alert on recovery_point_age_seconds.
# TYPE csi_qumulo_replication_policy_interval_seconds gauge
csi_qumulo_replication_policy_interval_seconds{relationship_id="` + relationshipID + `"} 86400
`
	require.NoError(t, testutil.CollectAndCompare(r.replication, strings.NewReader(expected),
		"csi_qumulo_replication_policy_interval_seconds"))

	// A relationship no longer policy-mode is reported with no interval.
	r.ObserveReplication(ctx, qumulo.ReplicationEvent{Cluster: "a.example.com:8000", PolicyIntervalOf: relationshipID})
	assert.Equal(t, 0, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_policy_interval_seconds"))
}

// The interval series goes with the relationship: a delete retires it, and
// so do complete listings that no longer hold any end of it.
func TestThePolicyIntervalGoesWithItsRelationship(t *testing.T) {
	for _, tt := range []struct {
		name   string
		retire qumulo.ReplicationEvent
	}{
		{"deleted", qumulo.ReplicationEvent{Cluster: "a.example.com:8000", Removed: relationshipID}},
		{"no longer listed", qumulo.ReplicationEvent{Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := testRegistry()
			now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
			pinClock(r, now)
			ctx := context.Background()
			r.ObserveReplication(ctx, qumulo.ReplicationEvent{
				Cluster: "a.example.com:8000", Role: qumulo.RoleSource, Complete: true,
				Relationships: []qumulo.RelationshipStatus{{ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now,
					ReplicationMode: qumulo.ReplicationModeSnapshotPolicy}},
			})
			r.ObserveReplication(ctx, qumulo.ReplicationEvent{Cluster: "a.example.com:8000", PolicyIntervalOf: relationshipID, PolicyInterval: time.Hour})
			require.Equal(t, 1, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_policy_interval_seconds"))

			r.ObserveReplication(ctx, tt.retire)
			assert.Equal(t, 0, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_policy_interval_seconds"))
		})
	}
}
