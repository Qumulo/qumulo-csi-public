// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
	"qumulo-csi-driver/pkg/version"
)

// With two controller replicas, a per-volume reading is written by
// whichever pod served the RPC and frozen there.
//
// Every test here uses two registries, because one registry cannot show the
// defect: it is the disagreement *between* pods that reaches Prometheus, and
// the developers' validation ran a single replica, which is how this shipped.

const (
	staleHandle = "a.example.com##/csi/volumes/pvc-stale"
	oneGiB      = int64(1) << 30
	twoGiB      = int64(2) << 30
)

// writeQuota records a quota reading as the RPC path does, with its write
// timestamp pushed back by age so a test need not wait.
func writeQuota(t *testing.T, r *Registry, handle string, limit, used int64, age time.Duration) {
	t.Helper()
	name, ok := volumeNameFromHandle(handle)
	require.True(t, ok)
	id := volumeID(handle)
	r.volumeQuotaBytes.WithLabelValues(id, name).Set(float64(limit))
	r.volumeUsedBytes.WithLabelValues(id, name).Set(float64(used))
	r.noteQuotaWritten(id)
	backdate(r.volumes.quotaAt, id, age)
}

func writeProtection(t *testing.T, r *Registry, handle string, protected bool, age time.Duration) {
	t.Helper()
	r.ObserveVolumeProtection(context.Background(), qumulo.VolumeProtectionEvent{
		Subject: handle, Protected: protected,
	})
	backdate(r.volumes.protectedAt, volumeID(handle), age)
}

func backdate(at map[string]time.Time, id string, age time.Duration) {
	if age > 0 {
		at[id] = at[id].Add(-age)
	}
}

func quotaOf(t *testing.T, r *Registry, handle string) float64 {
	t.Helper()
	name, _ := volumeNameFromHandle(handle)
	return testutil.ToFloat64(r.volumeQuotaBytes.WithLabelValues(volumeID(handle), name))
}

func seriesCount(r *Registry, handle string) int {
	n := 0
	for _, id := range r.volumeIDsWithSeries() {
		if id == volumeID(handle) {
			n++
		}
	}
	return n
}

// The reported damage. An expand served by pod B leaves pod A exporting the
// pre-expand quota against a current usage, so used/quota reads 0.928 in pod
// A against 0.464 in pod B -- and QumuloCSIVolumeNearQuota, which has no
// aggregation, keeps firing from pod A after the operator has expanded the
// volume. The stale pod must stop exporting the pair.
func TestStaleQuotaInANonServingReplicaIsDropped(t *testing.T) {
	podA, podB := New(version.Info{}), New(version.Info{})
	used := int64(996) << 20
	// Pod A read the quota before the expand and has served nothing since.
	writeQuota(t, podA, staleHandle, oneGiB, used, StaleVolumeAfter)
	// Pod B served the expand, so its reading is current.
	writeQuota(t, podB, staleHandle, twoGiB, used, 0)
	live := []VolumeRecord{{Handle: staleHandle, CapacityBytes: twoGiB}}

	assert.Equal(t, 1, podA.ReconcileVolumes(live), "the pre-expand pair must be retired")
	assert.Zero(t, podB.ReconcileVolumes(live), "the replica that served the expand keeps its reading")

	assert.Zero(t, seriesCount(podA, staleHandle), "pod A still exports a pre-expand ratio")
	assert.Equal(t, float64(twoGiB), quotaOf(t, podB, staleHandle),
		"the volume must keep its current reading in the pod that is serving it")
}

// The regression the check above could introduce. A quiet volume's quota is
// written only by the RPCs that read one, which it may not see for days, so
// age alone must never retire it -- that would take
// QumuloCSIVolumeNearQuota, the only early warning before ENOSPC, with it.
func TestAnOldButUncontradictedQuotaIsKept(t *testing.T) {
	r := New(version.Info{})
	writeQuota(t, r, staleHandle, oneGiB, 512<<20, 10*StaleVolumeAfter)

	assert.Zero(t, r.ReconcileVolumes([]VolumeRecord{{Handle: staleHandle, CapacityBytes: oneGiB}}))

	assert.Equal(t, float64(oneGiB), quotaOf(t, r, staleHandle),
		"a volume nobody has touched lost its quota series, and with it its near-quota alert")
}

// A quota an administrator set on the array *below* the PV's size is the
// array's answer, not a stale one, and it is the case where the near-quota
// warning matters most. The check must not mistake it for a missed expand.
func TestAQuotaSmallerThanTheCapacityIsNotTreatedAsStaleWhenFresh(t *testing.T) {
	r := New(version.Info{})
	writeQuota(t, r, staleHandle, oneGiB, 1000<<20, 0)

	assert.Zero(t, r.ReconcileVolumes([]VolumeRecord{{Handle: staleHandle, CapacityBytes: twoGiB}}))

	assert.Equal(t, float64(oneGiB), quotaOf(t, r, staleHandle))
}

// An unknown capacity contradicts nothing.
func TestAnUnknownCapacityRetiresNothing(t *testing.T) {
	r := New(version.Info{})
	writeQuota(t, r, staleHandle, oneGiB, 512<<20, 10*StaleVolumeAfter)

	assert.Zero(t, r.ReconcileVolumes([]VolumeRecord{{Handle: staleHandle}}))

	assert.Equal(t, float64(oneGiB), quotaOf(t, r, staleHandle))
}

// The protected gauge is refreshed continuously by the csi-addons poll of
// every VolumeReplication, so going unrefreshed is itself proof that another
// replica is serving this volume's Info. Age alone is enough there, and it
// has to be: after a group of N volumes puts N readings in whichever pod
// served the last poll, so a stale copy is N series wrong rather than one.
func TestStaleProtectionInANonServingReplicaIsDropped(t *testing.T) {
	podA, podB := New(version.Info{}), New(version.Info{})
	writeProtection(t, podA, staleHandle, true, StaleVolumeAfter)
	writeProtection(t, podB, staleHandle, false, 0)
	live := []VolumeRecord{{Handle: staleHandle, CapacityBytes: oneGiB}}

	assert.Equal(t, 1, podA.ReconcileVolumes(live))
	assert.Zero(t, podB.ReconcileVolumes(live))

	assert.Zero(t, seriesCount(podA, staleHandle),
		"pod A still reports the volume protected after the relationship went")
	assert.Equal(t, 1, seriesCount(podB, staleHandle),
		"the replica serving Info must keep reporting, or the unprotected alert has nothing to fire on")
}

// A protected reading the poll is still refreshing stays.
func TestFreshProtectionIsKept(t *testing.T) {
	r := New(version.Info{})
	writeProtection(t, r, staleHandle, true, 0)

	assert.Zero(t, r.ReconcileVolumes([]VolumeRecord{{Handle: staleHandle, CapacityBytes: oneGiB}}))

	assert.Equal(t, 1, seriesCount(r, staleHandle))
}

// The two checks are independent: a stale protected reading must not take a
// current quota pair with it, or an expand and a failover would interfere.
func TestRetiringAStaleProtectionLeavesACurrentQuota(t *testing.T) {
	r := New(version.Info{})
	writeQuota(t, r, staleHandle, oneGiB, 512<<20, 0)
	writeProtection(t, r, staleHandle, true, StaleVolumeAfter)

	assert.Equal(t, 1, r.ReconcileVolumes([]VolumeRecord{{Handle: staleHandle, CapacityBytes: oneGiB}}))

	assert.Equal(t, float64(oneGiB), quotaOf(t, r, staleHandle), "the quota pair was collateral damage")
}

// still holds: a volume no PersistentVolume carries loses everything,
// and the write timestamps go with it so a handle reused later starts clean.
func TestRetiringAGoneVolumeAlsoForgetsItsWriteTimes(t *testing.T) {
	r := New(version.Info{})
	writeQuota(t, r, staleHandle, oneGiB, 512<<20, 0)
	writeProtection(t, r, staleHandle, true, 0)

	require.Zero(t, r.ReconcileVolumes(nil), "one pass retires nothing")
	require.Equal(t, 1, r.ReconcileVolumes(nil))

	assert.Zero(t, seriesCount(r, staleHandle))
	r.volumes.mu.Lock()
	defer r.volumes.mu.Unlock()
	assert.NotContains(t, r.volumes.quotaAt, volumeID(staleHandle))
	assert.NotContains(t, r.volumes.protectedAt, volumeID(staleHandle))
}

// The relationship-keyed half. The recovery-point age is computed at
// scrape time from the stored recovery point, so an end nobody refreshes
// exports an age that grows by itself and eventually trips
// QumuloCSIReplicationRecoveryPointStale on its own. That happens on every
// csi-addons leadership change: the sidecar elects its own leader, so
// the replica that held it goes on exporting what it last saw.
//
// Safe on age alone, unlike a quota: these ends are refreshed by the poll of
// every VolumeReplication, so going unrefreshed is proof another replica is
// serving the relationship.
func TestStaleReplicationEndsAreRetired(t *testing.T) {
	r := testRegistry()
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	pinClock(r, now)
	r.ObserveReplication(context.Background(), qumulo.ReplicationEvent{
		Cluster: "a.example.com:8000", Role: qumulo.RoleSource,
		Relationships: []qumulo.RelationshipStatus{{
			ID: relationshipID, State: "ESTABLISHED", RecoveryPoint: now.Add(-time.Minute),
		}},
	})
	require.Equal(t, 1, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_state"))

	// Still being refreshed: kept.
	assert.Zero(t, r.replication.retireStaleEnds(StaleVolumeAfter))
	assert.Equal(t, 1, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_state"))

	// Nothing has refreshed it since: another replica is serving it now.
	pinClock(r, now.Add(StaleVolumeAfter+time.Minute))
	assert.Equal(t, 1, r.replication.retireStaleEnds(StaleVolumeAfter))
	assert.Zero(t, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_state"),
		"a replica that stopped serving a relationship still exports its ends")
	assert.Zero(t, testutil.CollectAndCount(r.replication, "csi_qumulo_replication_recovery_point_age_seconds"),
		"and still exports an age that grows by itself")
}
