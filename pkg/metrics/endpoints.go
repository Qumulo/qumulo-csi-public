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
)

// endpointIdleWindow is how long an endpoint must go uncalled before its
// per-endpoint series are retired.
//
// The window is what keeps a *down* array alerting while anything still
// depends on it. A failing operation keeps its endpoint called: csi-provisioner
// and the resizer retry with a backoff capped at five minutes, and a
// replication peer in use is read on every status poll, a minute or so apart.
// So an endpoint idle for an hour has no failing operation behind it, and its
// last reading -- 0 or 1 -- describes nothing current.
const endpointIdleWindow = time.Hour

// endpointRetirementInterval is how often RunRetirement looks for idle
// endpoints. An endpoint is retired between one idle window and one window
// plus this interval after its last call.
const endpointRetirementInterval = 5 * time.Minute

// endpointTracker records when each endpoint was last called, so the
// per-endpoint series of one nothing uses any more can be retired.
type endpointTracker struct {
	mu       sync.Mutex
	lastSeen map[string]time.Time
	// now, idle and every are overridable so the tests can pin them.
	now   func() time.Time
	idle  time.Duration
	every time.Duration
}

func newEndpointTracker() *endpointTracker {
	return &endpointTracker{
		lastSeen: make(map[string]time.Time),
		now:      time.Now,
		idle:     endpointIdleWindow,
		every:    endpointRetirementInterval,
	}
}

func (t *endpointTracker) seen(cluster string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastSeen[cluster] = t.now()
}

// idle removes and returns every tracked endpoint that has not been called
// within the idle window.
func (t *endpointTracker) idleEndpoints() []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	var retired []string
	for cluster, at := range t.lastSeen {
		if now.Sub(at) < t.idle {
			continue
		}
		delete(t.lastSeen, cluster)
		retired = append(retired, cluster)
	}
	return retired
}

// RetireIdleEndpoints retires every per-endpoint series -- reachability,
// capacity, and the request counters and latency histograms -- for every
// endpoint nothing has called for the idle window. Before this,
// csi_qumulo_api_reachable stayed at 0 for an endpoint nothing used any more
// until the controller restarted, and the critical QumuloCSIArrayUnreachable
// never cleared.
//
// The request and latency series used to be kept as history. But each is
// one series per endpoint, method, path and status -- a histogram is a dozen
// -- and an endpoint the controller no longer uses (a renamed apiUrl, a
// retired array, a class deleted) kept them until a restart. A rate()
// over a retired counter simply ends; a counter that returns starts from
// zero, which Prometheus reads as a reset.
//
// It reads no StorageClass. The first
// version retired only endpoints no class named, and ran only from the
// inventory path -- ListVolumes and ControllerGetVolume -- which is off by
// default and which nothing the chart deploys calls, so on the chart as
// shipped nothing ever retired. RunRetirement calls this on a timer.
func (r *Registry) RetireIdleEndpoints(ctx context.Context) {
	for _, cluster := range r.endpoints.idleEndpoints() {
		r.apiReachable.DeleteLabelValues(cluster)
		r.clusterCapacityBytes.DeleteLabelValues(cluster)
		r.clusterFreeBytes.DeleteLabelValues(cluster)
		requests := r.apiRequests.DeletePartialMatch(prometheus.Labels{"cluster": cluster})
		latencies := r.apiDuration.DeletePartialMatch(prometheus.Labels{"cluster": cluster})
		r.restoreStaging.DeletePartialMatch(prometheus.Labels{"cluster": cluster})
		ends := r.replication.retireCluster(cluster)
		slog.InfoContext(ctx, "metrics: retired the series of an endpoint nothing has called for a while",
			"cluster", cluster, "idleFor", r.endpoints.idle,
			"requestSeries", requests, "latencySeries", latencies, "replicationEnds", ends)
	}
}

// RunRetirement calls RetireIdleEndpoints every endpointRetirementInterval
// until ctx is done. main starts it with the process's lifetime context.
func (r *Registry) RunRetirement(ctx context.Context) {
	ticker := time.NewTicker(r.endpoints.every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.RetireIdleEndpoints(ctx)
		}
	}
}
