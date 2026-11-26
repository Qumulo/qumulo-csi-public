// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// VolumeReconcileEvery is how often per-volume series are compared with the
// PersistentVolumes that exist.
const VolumeReconcileEvery = 10 * time.Minute

// VolumeRecord is one of this driver's PersistentVolumes, as much of it as
// the reconcile needs. A type of its own rather than a bare handle, because
// the stale-reading check needs what Kubernetes believes the volume's size to
// be.
type VolumeRecord struct {
	// Handle is spec.csi.volumeHandle.
	Handle string
	// CapacityBytes is spec.capacity.storage, 0 when unknown.
	CapacityBytes int64
}

// StaleVolumeAfter is how long a per-volume reading may go unrefreshed in
// this process before the reconcile stops exporting it.
//
// Three reconcile passes, not one: a series is refreshed by whichever
// controller replica served the RPC, and the window has to be comfortably
// longer than the csi-addons poll that refreshes a protected reading, or a
// pod that is still serving would retire its own series and re-create it on
// the next poll -- a flap, which is worse than the staleness. An install that
// polls less often than this should raise it.
const StaleVolumeAfter = 3 * VolumeReconcileEvery

// volumeReconciler remembers the volume IDs a reconcile pass found with series
// but no PV, so the next pass retires only those still missing: a volume
// CreateVolume has just made has series before csi-provisioner has written
// its PV, and one pass must not retire it.
//
// It also remembers when this process last wrote each kind of per-volume
// reading, which is what lets a replica that no longer serves a volume stop
// exporting a frozen one.
type volumeReconciler struct {
	mu      sync.Mutex
	missing map[string]bool
	// quotaAt and protectedAt are when this process last recorded that kind
	// of reading for a volume ID.
	quotaAt     map[string]time.Time
	protectedAt map[string]time.Time
}

// noteQuotaWritten and noteProtectionWritten record that this process has a
// current reading, so the reconcile can tell a reading it is still refreshing
// from one frozen at whatever the last RPC it served happened to see.
func (r *Registry) noteQuotaWritten(id string) {
	r.volumes.mu.Lock()
	defer r.volumes.mu.Unlock()
	if r.volumes.quotaAt == nil {
		r.volumes.quotaAt = map[string]time.Time{}
	}
	r.volumes.quotaAt[id] = time.Now()
}

func (r *Registry) noteProtectionWritten(id string) {
	r.volumes.mu.Lock()
	defer r.volumes.mu.Unlock()
	if r.volumes.protectedAt == nil {
		r.volumes.protectedAt = map[string]time.Time{}
	}
	r.volumes.protectedAt[id] = time.Now()
}

// ReconcileVolumes retires the per-volume series -- quota, usage, protected
// -- of every volume that no PersistentVolume carried on this pass and the
// last one.
//
// Series were removed only by a successful DeleteVolume in this process, so
// a volume that left any other way -- a Retain PV removed by hand, a DR
// copy's PV, a force-deleted PV, a delete that kept failing until someone
// removed the PV, or a DeleteVolume served by the other controller replica
// -- kept its series until the controller restarted, and the
// near-quota and unprotected alerts fired for ever for a volume that was
// gone. vols is every one of this driver's PVs; the series carry each one's
// volumeID, so they are compared by that.
//
// # Stale readings in a replica that no longer serves the volume
//
// Each controller pod has its own registry, and a per-volume reading is
// written by whichever pod served the RPC. Each sidecar elects its own
// leader and the csi-addons sidecar elects none, so with the two replicas
// the chart now installs by default one volume's readings are spread over
// both pods, each frozen at the last value that pod happened to see. Nothing
// told a pod that another pod had newer data, so after an expand served by
// pod B, pod A went on exporting the pre-expand quota for as long as the PV
// existed -- and QumuloCSIVolumeNearQuota, which has no aggregation and so
// evaluates each pod's pair on its own, kept firing from pod A after the
// operator had already expanded the volume. An alert that does not clear
// when the remedy is applied is an alert operators learn to ignore.
//
// A live volume's readings are therefore dropped here when this process is
// demonstrably not the one keeping them current. The two kinds are judged
// differently, because they are refreshed on completely different cadences:
//
//   - protected is refreshed continuously, by the csi-addons poll of every
//     VolumeReplication, so going unrefreshed for StaleVolumeAfter is itself
//     proof that some other pod is serving this volume's Info. Age alone is
//     enough.
//   - quota and usage are written only by the RPCs that read a quota --
//     CreateVolume, an expand, a snapshot, ControllerGetVolume, ListVolumes
//     -- which a quiet volume may not see for days. Age alone would retire a
//     perfectly good reading and take QumuloCSIVolumeNearQuota with it, so
//     the pair also has to be contradicted: the PV's capacity, which
//     Kubernetes only raises once an expand has completed, must exceed the
//     quota this process last recorded. The pod that served the expand agrees
//     with the PV and keeps its series, so a volume never loses its last
//     reading everywhere.
func (r *Registry) ReconcileVolumes(vols []VolumeRecord) (retired int) {
	keep := make(map[string]int64, len(vols))
	for _, v := range vols {
		keep[volumeID(v.Handle)] = v.CapacityBytes
	}
	r.volumes.mu.Lock()
	defer r.volumes.mu.Unlock()
	now := time.Now()
	next := map[string]bool{}
	for _, id := range r.volumeIDsWithSeries() {
		capacity, live := keep[id]
		if !live {
			if r.volumes.missing[id] {
				r.forgetVolumeID(id)
				delete(r.volumes.quotaAt, id)
				delete(r.volumes.protectedAt, id)
				retired++
				continue
			}
			next[id] = true
			continue
		}
		retired += r.dropStaleReadings(id, capacity, now)
	}
	r.volumes.missing = next
	return retired
}

// dropStaleReadings drops the readings of a live volume that this process is
// no longer the one refreshing. Called with volumes.mu held.
func (r *Registry) dropStaleReadings(id string, capacity int64, now time.Time) (retired int) {
	if at, ok := r.volumes.protectedAt[id]; ok && now.Sub(at) >= StaleVolumeAfter {
		r.volumeProtected.DeletePartialMatch(prometheus.Labels{"volume_id": id})
		delete(r.volumes.protectedAt, id)
		retired++
	}
	at, ok := r.volumes.quotaAt[id]
	if !ok || now.Sub(at) < StaleVolumeAfter {
		return retired
	}
	// Contradicted only by a capacity this process's own reading predates.
	// An unknown capacity (0) contradicts nothing, and neither does one that
	// merely differs: a quota an administrator set on the array below the
	// PV's size is the array's answer, not a stale one, and dropping it would
	// remove the warning that the volume will hit ENOSPC early.
	quota, known := r.quotaValue(id)
	if capacity <= 0 || !known || capacity <= int64(quota) {
		return retired
	}
	labels := prometheus.Labels{"volume_id": id}
	r.volumeQuotaBytes.DeletePartialMatch(labels)
	r.volumeUsedBytes.DeletePartialMatch(labels)
	delete(r.volumes.quotaAt, id)
	return retired + 1
}

// quotaValue is the quota this registry currently exports for a volume ID.
func (r *Registry) quotaValue(id string) (float64, bool) {
	ch := make(chan prometheus.Metric, 64)
	go func() { r.volumeQuotaBytes.Collect(ch); close(ch) }()
	value, found := 0.0, false
	for m := range ch {
		var pb dto.Metric
		if m.Write(&pb) != nil {
			continue
		}
		for _, l := range pb.GetLabel() {
			if l.GetName() == "volume_id" && l.GetValue() == id {
				value, found = pb.GetGauge().GetValue(), true
			}
		}
	}
	return value, found
}

// RunVolumeReconcile calls ReconcileVolumes every VolumeReconcileEvery with
// the handles list returns, until ctx ends. A listing that fails retires
// nothing: an unreadable PV list is not an empty one.
func (r *Registry) RunVolumeReconcile(ctx context.Context, list func(context.Context) ([]VolumeRecord, error)) {
	ticker := time.NewTicker(VolumeReconcileEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			handles, err := list(ctx)
			if err != nil {
				slog.WarnContext(ctx, "metrics: could not list PersistentVolumes to retire gone volumes' series; retrying later",
					"error", err)
				continue
			}
			if n := r.replication.retireStaleEnds(StaleVolumeAfter); n > 0 {
				slog.InfoContext(ctx, "metrics: retired replication ends another controller replica is the one "+
					"keeping current", "ends", n)
			}
			if n := r.ReconcileVolumes(handles); n > 0 {
				slog.InfoContext(ctx, "metrics: retired per-volume series that no PersistentVolume carries any more, "+
					"or that another controller replica is the one keeping current", "volumes", n)
			}
		}
	}
}

// volumeIDsWithSeries is every volume_id label value on a per-volume series.
func (r *Registry) volumeIDsWithSeries() []string {
	seen := map[string]bool{}
	for _, vec := range []*prometheus.GaugeVec{r.volumeQuotaBytes, r.volumeUsedBytes, r.volumeProtected} {
		ch := make(chan prometheus.Metric, 64)
		go func() { vec.Collect(ch); close(ch) }()
		for m := range ch {
			var pb dto.Metric
			if m.Write(&pb) != nil {
				continue
			}
			for _, l := range pb.GetLabel() {
				if l.GetName() == "volume_id" {
					seen[l.GetValue()] = true
				}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for h := range seen {
		out = append(out, h)
	}
	return out
}
