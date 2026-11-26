// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"qumulo-csi-driver/pkg/qumulo"
)

// Registry implements qumulo.Observer. The compile-time assertion is here
// rather than in pkg/qumulo so that package keeps no knowledge of this one.
var _ qumulo.Observer = (*Registry)(nil)

// ObserveRequest records one REST round trip and the array's reachability.
func (r *Registry) ObserveRequest(_ context.Context, event qumulo.RequestEvent) {
	r.apiRequests.WithLabelValues(
		event.Cluster, event.Method, event.PathTemplate, event.Status).Inc()
	r.apiDuration.WithLabelValues(
		event.Cluster, event.Method, event.PathTemplate).Observe(event.Duration.Seconds())

	reachable := 0.0
	if event.Reachable {
		reachable = 1
	}
	r.apiReachable.WithLabelValues(event.Cluster).Set(reachable)
	r.endpoints.seen(event.Cluster)
}

// ObserveCapacity records an array's total and free space.
func (r *Registry) ObserveCapacity(_ context.Context, event qumulo.CapacityEvent) {
	r.clusterCapacityBytes.WithLabelValues(event.Cluster).Set(float64(event.TotalSizeBytes))
	r.clusterFreeBytes.WithLabelValues(event.Cluster).Set(float64(event.FreeSizeBytes))
}

// ObserveReplication records relationship status, or the array clock alone.
func (r *Registry) ObserveReplication(_ context.Context, event qumulo.ReplicationEvent) {
	r.replication.observe(event)
}

// ObserveRestoreStaging counts a restore staging directory the driver found
// abandoned or removed.
func (r *Registry) ObserveRestoreStaging(_ context.Context, event qumulo.RestoreStagingEvent) {
	r.restoreStaging.WithLabelValues(event.Cluster, event.State).Inc()
}

// ObserveVolumeProtection exports whether one volume is covered by a
// replication relationship.
//
// Set directly rather than parked on the sink like a quota reading: the driver
// states this about a volume it has already named, so there is nothing to join
// and nothing to wait for. An event the driver did not label belongs to no
// volume and is dropped.
func (r *Registry) ObserveVolumeProtection(_ context.Context, event qumulo.VolumeProtectionEvent) {
	name, ok := volumeNameFromHandle(event.Subject)
	if !ok {
		return
	}
	// Not for a volume this controller has deleted. A group's report fans
	// its verdict out to every member it finds in a PersistentVolume
	// listing that is up to ten minutes old, so a member deleted inside
	// that window was exported again -- and a deleted volume reading
	// protected=0 is QumuloCSIVolumeUnprotected firing for ever for
	// something that does not exist, which is the shape the quota
	// path already guards against.
	// Checked and written under one lock, so a delete cannot land between
	// the two.
	r.deletions.unlessDeleted(event.Subject, func() {
		protected := 0.0
		if event.Protected {
			protected = 1
		}
		id := volumeID(event.Subject)
		r.volumeProtected.WithLabelValues(id, name).Set(protected)
		// So the reconcile can tell this reading from one frozen in a
		// replica that no longer serves the volume.
		r.noteProtectionWritten(id)
	})
}

// ObserveQuota parks a quota reading on the in-flight RPC rather than
// exporting it directly.
//
// The client knows the Qumulo file ID a quota hangs off; it does not know,
// and must not be taught, the CSI volume handle -- that is the driver's
// vocabulary, and a volume gauge labelled by a bare file ID would be
// unjoinable to anything in Kubernetes. So the reading waits on the request
// context until the interceptor, which does see the handle, can join the
// two. A reading made outside any RPC has nothing to join to and is dropped;
// that is a deliberate no-op, not a lost metric, because every quota call
// this driver makes is made inside an RPC.
//
// The join is the event's Subject, which the driver sets to the handle the
// work belongs to (qumulo.WithSubject). It used to be the file ID, read back
// out of the handle's middle field -- a join that stopped existing
// when handles became path-identified and carried no file ID, and
// took every per-volume gauge down with it without failing anything.
func (r *Registry) ObserveQuota(ctx context.Context, event qumulo.QuotaEvent) {
	sink, ok := ctx.Value(quotaSinkKey{}).(*quotaSink)
	if !ok {
		return
	}
	sink.add(event)
}

// quotaSinkKey keys the per-RPC sink in the request context.
type quotaSinkKey struct{}

// quotaSink collects the quota readings made while one RPC ran.
//
// One RPC can read several quotas -- ListVolumes reads one per volume -- so
// this is a map, keyed by the subject the driver labelled each read with,
// which is the CSI volume handle it belongs to.
type quotaSink struct {
	mu     sync.Mutex
	quotas map[string]qumulo.QuotaEvent
}

func (s *quotaSink) add(event qumulo.QuotaEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.quotas == nil {
		s.quotas = make(map[string]qumulo.QuotaEvent, 1)
	}
	// A reading the driver did not label belongs to no handle and can be
	// joined to nothing, so it is dropped rather than filed under "".
	if event.Subject == "" {
		return
	}
	// Last write wins. Within one RPC a later reading is the fresher one --
	// ControllerExpandVolume reads the old limit and then writes the new.
	// But a write carries no usage, and the usage a read in the same RPC
	// found is still true of the directory, so it is kept: dropping it meant
	// an expand never set volume_used_bytes, and the near-quota alert did not
	// see an expanded volume until something else read it.
	if prev, ok := s.quotas[event.Subject]; ok && prev.UsedKnown && !event.UsedKnown {
		event.UsedBytes, event.UsedKnown = prev.UsedBytes, true
	}
	s.quotas[event.Subject] = event
}

func (s *quotaSink) get(subject string) (qumulo.QuotaEvent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	event, ok := s.quotas[subject]
	return event, ok
}

func (s *quotaSink) empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.quotas) == 0
}

// withQuotaSink returns a context carrying a fresh sink.
func withQuotaSink(ctx context.Context) (context.Context, *quotaSink) {
	sink := &quotaSink{}
	return context.WithValue(ctx, quotaSinkKey{}, sink), sink
}

// ObserveClassCredential records whether the inventory could build a
// credential for one StorageClass.
//
// One series per class, so a class that moves between reasons -- or back to
// usable -- does not leave its old reason behind reading 0 for ever. The
// reason travels as a label rather than a second metric because an operator
// alerting on == 0 needs the remedy in the same place as the alarm: a
// "forbidden" class is fixed in rbac.extraSecrets, a "missing" one is not.
func (r *Registry) ObserveClassCredential(
	_ context.Context,
	class, cluster, reason string,
	usable bool,
) {
	r.inventoryClassCredential.DeletePartialMatch(prometheus.Labels{
		"class":   class,
		"cluster": cluster,
	})
	value := 0.0
	if usable {
		value = 1
	}
	r.inventoryClassCredential.WithLabelValues(class, cluster, reason).Set(value)
}

// ForgetClassesExcept retires the credential series of every class the walk
// that just ran did not see.
//
// Deleted classes are the reason this exists. The forbidden-Secret skip is provoked by
// creating a StorageClass and resolved by deleting it, so without a sweep the
// very case the gauge is there to catch would leave a series reading 0 with
// no class behind it, and the alert would never clear.
func (r *Registry) ForgetClassesExcept(_ context.Context, classes []string) {
	keep := make(map[string]bool, len(classes))
	for _, name := range classes {
		keep[name] = true
	}
	for _, name := range labelValuesOf(r.inventoryClassCredential, "class") {
		if !keep[name] {
			r.inventoryClassCredential.DeletePartialMatch(prometheus.Labels{"class": name})
		}
	}
}

// labelValuesOf reports the distinct values a GaugeVec's series carry for one
// label. Collecting is how a vec is read back: client_golang exposes no
// listing of the label sets it holds.
func labelValuesOf(vec *prometheus.GaugeVec, label string) []string {
	seen := map[string]bool{}
	ch := make(chan prometheus.Metric, 64)
	go func() { vec.Collect(ch); close(ch) }()
	for m := range ch {
		var pb dto.Metric
		if m.Write(&pb) != nil {
			continue
		}
		for _, l := range pb.GetLabel() {
			if l.GetName() == label {
				seen[l.GetValue()] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	return out
}
