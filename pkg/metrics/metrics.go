// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package metrics exports the driver's Prometheus series.
//
// See docs/reference/metrics.md for the full series list and the caveats.
//
// Everything here is fed by work the driver was already doing. There is no
// background poller and no sampler, and that is a design constraint rather
// than an omission: Rule 0 in ARCHITECTURE.md says the controller holds no cluster
// address and no standing API client, so there is no set of arrays it could
// poll and no credential it could poll them with. A figure appears when an
// RPC or an API call produces it, and stays at its last observed value until
// the next one. An operator reading these series is reading the last thing
// the driver actually saw, not a fresh sample -- which is why every gauge
// here is documented with when it updates.
//
// Two collection points feed the whole set:
//
//   - UnaryServerInterceptor, chained onto the gRPC server, times every CSI
//     and csi-addons RPC and joins the array-side readings that RPC produced
//     to the volume handle it was about. No RPC handler is edited.
//   - Registry itself implements qumulo.Observer, which pkg/qumulo calls at
//     its single REST choke point and at the handful of calls whose decoded
//     answer is worth a gauge. A client with no Observer behaves exactly as
//     it did before this package existed.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"qumulo-csi-driver/pkg/version"
)

// namespace prefixes every series this package exports.
//
// csi_qumulo rather than qumulo_csi so the driver's own series sort next to
// the csi_* families kubelet and the sidecars publish, which is where an
// operator building a dashboard looks for them.
const namespace = "csi_qumulo"

// rpcBuckets span a CSI RPC's plausible range.
//
// Wider on the right than a web-service default because two RPCs here are
// legitimately slow: a snapshot restore copies a tree file by file and
// returns Aborted when its budget expires, and a tree delete waits on an
// array job. A bucket set topping out at 10s would put both in +Inf and hide
// the difference between "slow" and "never".
var rpcBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

// apiBuckets span one REST round trip to an array. The client's own default
// timeout is 10s, so anything past that is a timeout rather than a slow call.
var apiBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Registry holds the driver's collectors and the registry they are in.
//
// It is deliberately not the default Prometheus registry: two driver
// instances in one test process would panic on duplicate registration, and
// the Go and process collectors are added explicitly so what /metrics serves
// is a decision rather than an accident of imports.
type Registry struct {
	registry *prometheus.Registry

	rpcTotal    *prometheus.CounterVec
	rpcDuration *prometheus.HistogramVec
	rpcPanics   *prometheus.CounterVec

	apiRequests  *prometheus.CounterVec
	apiDuration  *prometheus.HistogramVec
	apiReachable *prometheus.GaugeVec

	volumeQuotaBytes *prometheus.GaugeVec
	volumeUsedBytes  *prometheus.GaugeVec
	volumeProtected  *prometheus.GaugeVec

	clusterCapacityBytes *prometheus.GaugeVec
	clusterFreeBytes     *prometheus.GaugeVec

	inventoryClassCredential *prometheus.GaugeVec

	restoreStaging *prometheus.CounterVec

	replication *replicationCollector

	endpoints *endpointTracker

	volumes volumeReconciler

	deletions deletionLog
}

// New builds a Registry and registers every collector in it.
func New(info version.Info) *Registry {
	r := &Registry{
		registry: prometheus.NewRegistry(),

		rpcTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rpc_total",
			Help: "CSI and csi-addons RPCs served, by full gRPC method and " +
				"resulting gRPC status code.",
		}, []string{"method", "code"}),

		rpcPanics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rpc_panics_total",
			Help: "RPC handlers that panicked, by full gRPC method. Each was " +
				"recovered and answered Internal; any increase is a driver bug.",
		}, []string{"method"}),

		rpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "rpc_duration_seconds",
			Help: "Time the driver spent serving an RPC, by full gRPC method. " +
				"Unlabelled by status code on purpose: the code lives on " +
				"rpc_total, and a per-code histogram multiplies the series " +
				"count by the size of the gRPC code space for no extra answer.",
			Buckets: rpcBuckets,
		}, []string{"method"}),

		apiRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "api_requests_total",
			Help: "Qumulo REST API calls, by array, HTTP method, path template " +
				"and HTTP status. status=\"transport\" means the array never " +
				"answered at all.",
		}, []string{"cluster", "method", "path_template", "status"}),

		apiDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "api_duration_seconds",
			Help: "Time one Qumulo REST API round trip took. This is the " +
				"series that answers \"is the array slow or is Kubernetes slow\".",
			Buckets: apiBuckets,
		}, []string{"cluster", "method", "path_template"}),

		apiReachable: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "api_reachable",
			Help: "1 when the last REST call to this array got an HTTP answer, " +
				"any status, 0 when it failed at the transport. Retired after an " +
				"hour with no call. A refused credential is the 401/403 rate of " +
				"api_requests_total, not this. " +
				"Last observed, not sampled: it only moves when the driver " +
				"had a reason to call the array. Probe cannot carry this " +
				"signal -- see docs/reference/metrics.md and ARCHITECTURE.md Rule 0.",
		}, []string{"cluster"}),

		inventoryClassCredential: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "inventory_class_credential_usable",
			Help: "1 when the last ListVolumes built a credential for this " +
				"StorageClass, 0 when it skipped the class and left its volumes " +
				"out of the listing. reason is empty at 1, and otherwise says " +
				"which permanent misconfiguration it was: forbidden (the Secret " +
				"is not one this release names -- add it to rbac.extraSecrets), " +
				"missing (no such Secret), unusable (no reference, or no token " +
				"key). CSI has no field for a skipped class, so this is the only " +
				"machine-readable signal that a listing is short; alert on " +
				"== 0. Last observed, not sampled: only ListVolumes " +
				"moves it, and only with controller.inventory.enabled.",
		}, []string{"class", "cluster", "reason"}),

		volumeQuotaBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "volume_quota_bytes",
			Help: "Directory quota limit on a volume, in bytes, as of the last " +
				"RPC that read or wrote it.",
		}, []string{"volume_id", "volume_name"}),

		volumeUsedBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "volume_used_bytes",
			Help: "Bytes used under a volume's directory quota, as of the last " +
				"RPC that read it. Absent for a volume whose quota the array " +
				"reported without capacity_usage.",
		}, []string{"volume_id", "volume_name"}),

		volumeProtected: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "volume_replication_protected",
			Help: "1 when the driver last found a replication relationship " +
				"covering this volume, 0 when it looked and found none. " +
				"Alert on 0 for a volume you expect to be protected: the " +
				"VolumeReplication object cannot be trusted for this, because " +
				"csi-addons v0.15.0 leaves a Completed primary reading Healthy " +
				"after the relationship is removed on the arrays. Updated when " +
				"a replication RPC runs, not on a timer.",
		}, []string{"volume_id", "volume_name"}),

		clusterCapacityBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_capacity_bytes",
			Help: "Total filesystem capacity of an array, as of the last " +
				"GetCapacity-style call the driver made to it.",
		}, []string{"cluster"}),

		clusterFreeBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_free_bytes",
			Help: "Free filesystem capacity of an array, as of the last " +
				"GetCapacity-style call the driver made to it.",
		}, []string{"cluster"}),

		restoreStaging: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "restore_staging_directories_total",
			Help: "Restore staging directories (.csi-restoring-<name>) left by a " +
				"restore whose claim was deleted mid-copy, by array and state: " +
				"\"abandoned\" counts each one CreateVolume's sweep found with no " +
				"PersistentVolumeClaim and no PersistentVolume behind it, when first " +
				"seen; \"removed\" counts each one the driver removed, by that sweep " +
				"or by DeleteVolume.",
		}, []string{"cluster", "state"}),

		replication: newReplicationCollector(),

		endpoints: newEndpointTracker(),
	}

	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "build_info",
		Help: "Always 1. Carries the driver's version, commit and Go version " +
			"as labels, so a dashboard can answer \"which build is running " +
			"where\" without reading a log line.",
	}, []string{"driver_name", "version", "commit", "build_date", "go_version"})
	buildInfo.WithLabelValues(
		info.DriverName, info.DriverVersion, info.GitCommit, info.BuildDate, info.GoVersion,
	).Set(1)

	r.registry.MustRegister(
		buildInfo,
		r.rpcTotal,
		r.rpcDuration,
		r.rpcPanics,
		r.apiRequests,
		r.apiDuration,
		r.apiReachable,
		r.volumeQuotaBytes,
		r.volumeUsedBytes,
		r.volumeProtected,
		r.clusterCapacityBytes,
		r.clusterFreeBytes,
		r.inventoryClassCredential,
		r.restoreStaging,
		r.replication,
		// Go runtime and process series. Not strictly ours, but a driver with
		// no memory or goroutine numbers is the first thing an operator asks
		// for the moment the pod restarts, and every driver we are compared
		// against ships them.
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return r
}

// Gatherer exposes the underlying registry, for tests and for anything that
// wants to gather without going through HTTP.
func (r *Registry) Gatherer() prometheus.Gatherer { return r.registry }

// Handler serves the exposition format on /metrics.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{
		// A collector that errors should say so in the response rather than
		// serving a silently short scrape.
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}

// RecordPanic counts one recovered panic in an RPC handler.
func (r *Registry) RecordPanic(method string) {
	r.rpcPanics.WithLabelValues(method).Inc()
}
