# Monitor the driver

!!! note "On OpenShift, enable user-workload monitoring first"
    OpenShift's Prometheus does not scrape workload namespaces until it is
    turned on, and querying the monitoring API needs a bearer token rather
    than a client certificate. See
    [Monitoring](openshift-operations.md#monitoring).

!!! note "Two sources of metrics"
    The driver's own `/metrics` endpoint is opt-in and covered below. The
    **kubelet's** per-volume metrics need no configuration at all.

## What works with no configuration

Standard `kubelet_volume_stats_*` series work unmodified for every volume this
driver mounts — existing dashboards and alerts apply. The node service's stats
calls are bounded and single-flight, so a slow array cannot stall the kubelet.

| Series | Meaning |
|---|---|
| `kubelet_volume_stats_capacity_bytes` | the volume's quota |
| `kubelet_volume_stats_used_bytes` | usage |
| `kubelet_volume_stats_available_bytes` | remaining within the quota |
| `kubelet_volume_stats_inodes*` | inode counts |
| `csi_operations_seconds` | kubelet-side timing for every node RPC |

!!! tip "Start here"
    Alert on `kubelet_volume_stats_available_bytes` before building anything
    driver-specific. It needs nothing enabled and it is what most
    application-level capacity incidents look like.

## Enable the driver endpoint

```yaml
metrics:
  enabled: true
  serviceMonitor:
    enabled: true
  prometheusRule:
    enabled: true
```

This gives the driver a `/metrics` endpoint in both pods, gives the CSI
sidecars their own `--http-endpoint`, and renders a Service per workload.

| Value | Serves | Default port |
|---|---|---|
| `metrics.port.controller` | the driver in the controller pod | 29654 |
| `metrics.port.node` | the driver in the node pod — a **host** port | 29655 |
| `metrics.port.provisioner` | `csi-provisioner` | 29656 |
| `metrics.port.resizer` | `csi-resizer` | 29657 |
| `metrics.port.snapshotter` | `csi-snapshotter`, when enabled | 29658 |

!!! warning "The node port is a host port"
    The node DaemonSet is `hostNetwork: true` — pick a port nothing else on
    your workers binds. Ports 29652 and 29653 are the livenessprobe sidecars;
    the chart refuses any collision.

!!! warning "`ServiceMonitor` and `PrometheusRule` render only if the Prometheus Operator CRD is present"
    A manifest for an unknown kind fails the whole release. `helm template`
    has no cluster to ask, so set `metrics.forceEnable: true` when rendering
    into a GitOps repository.

!!! warning "Endpoints are unauthenticated"
    As upstream CSI drivers' are. `metrics.networkPolicy.enabled: true` admits
    the controller's ports only from `metrics.networkPolicy.from`; it needs a
    CNI that enforces NetworkPolicy and does **not** cover the hostNetwork
    node pods.

## The key series

| Series | Type | Use |
|---|---|---|
| `csi_qumulo_api_reachable` | gauge 0/1 per cluster | is the array answering |
| `csi_qumulo_api_requests_total` | counter, with `status` | 401s and 403s — credentials and privileges |
| `csi_qumulo_rpc_total` | counter, with `code` | failing RPCs by kind |
| `csi_qumulo_rpc_duration_seconds` | histogram | latency per CSI method |
| `csi_qumulo_volume_quota_bytes` / `_used_bytes` | gauge per volume | per-volume fill |
| `csi_qumulo_cluster_capacity_bytes` / `_free_bytes` | gauge per cluster | array capacity |
| `csi_qumulo_replication_recovery_point_age_seconds` | gauge | RPO |
| `csi_qumulo_inventory_class_credential_usable` | gauge 0/1 | a class silently skipped by `ListVolumes` |

Full list with labels and update conditions: [Metrics](../reference/metrics.md).

!!! warning "Nothing is polled"
    Every array-side figure — reachability, quota usage, recovery point — is
    the last value an RPC produced, so an **idle driver's gauges are as old as
    its last request**. RPC counts and latency carry no such caveat. Do not
    build an alert that assumes a gauge is current.

!!! warning "`/healthz` means 'this process is alive'"
    It says nothing about any array. Alert on `csi_qumulo_api_reachable`
    instead of restarting on it.

## Alerts

`metrics.prometheusRule.enabled` renders eight rules. Each is covered by a
promtool unit test with a case that must fire and one that must not, and a
contract test asserts every expression names a series the driver really
exports — an alert on a non-existent metric never fires, and never firing is
indistinguishable from healthy.

| Alert | Fires on | For |
|---|---|---|
| `QumuloCSIArrayUnreachable` | `csi_qumulo_api_reachable == 0` | 5m |
| `QumuloCSICredentialRejected` | any `401` in 10m, per cluster | 5m |
| `QumuloCSIPrivilegeMissing` | any `403` in 10m, per cluster and path | 10m |
| `QumuloCSIProvisioningFailing` | non-`OK` `CreateVolume` | 10m |
| `QumuloCSIReplicationRecoveryPointStale` | RPO past a mode-aware threshold | 10m |
| `QumuloCSIVolumeNearQuota` | used / quota > 0.9 | 15m |
| `QumuloCSIVolumeUnprotected` | `csi_qumulo_volume_replication_protected == 0` | 10m |
| `QumuloCSIInventoryClassSkipped` | a class's credential unusable | 10m |

Tunable with `metrics.prometheusRule.recoveryPointAgeSeconds`,
`policyModeRecoveryPointAgeSeconds`, `unknownModeRecoveryPointAgeSeconds`,
`policyIntervalFactor` and `volumeNearQuotaRatio`.

!!! note "Why the recovery-point alert has several thresholds"
    A continuous relationship's recovery point keeps up with writes, so a
    growing age means it stalled. A **policy-mode** relationship advances only
    when a scheduled snapshot replicates, so on a healthy pair the age climbs
    to the schedule interval every cycle — with one 900 s threshold, an hourly
    policy pair met the alert's condition for about 35 minutes of every hour.
    Set `policyModeRecoveryPointAgeSeconds` to about 1.5 × your longest policy
    interval; the default 5400 suits an hourly schedule, and a daily schedule
    needs about 129600.

!!! tip "`QumuloCSIVolumeUnprotected` beats the VolumeReplication object"
    On csi-addons v0.15.0 a `Completed` primary keeps reading `Healthy` after
    the relationship is removed on the arrays, so the object cannot be trusted
    for this.

## Replication metrics missing or frozen

**Symptom.** `csi_qumulo_replication_mode` is exported for a relationship but
`csi_qumulo_replication_recovery_point_age_seconds` is not — or the age never
drops after a sync.

**Why.** The replication series move only when csi-addons calls a replication
RPC. Nothing polls the array on a timer.

Check these in order.

```bash
# 1. The class sets schedulingInterval. The chart's classes always do; a class
#    written by hand without it is polled only when the relationship is
#    created, before its first sync, and its series freeze there.
kubectl get volumereplicationclass <class-name> \
  -o jsonpath='{.spec.parameters.schedulingInterval}{"\n"}'

# 2. csi-addons is polling: lastSyncTime is set, and moves.
kubectl -n <namespace> get volumereplication <volumereplication-name> \
  -o jsonpath='{.status.lastSyncTime}{"\n"}'

# 3. You are scraping the replica that serves replication. Only the holder of
#    this lease exports replication series; the others export none.
kubectl -n qumulo-csi get lease csi-qumulo-com-csi-addons \
  -o jsonpath='{.spec.holderIdentity}{"\n"}'
kubectl -n qumulo-csi get --raw \
  "/api/v1/namespaces/qumulo-csi/pods/<controller-pod-name>:29654/proxy/metrics" \
  | grep csi_qumulo_replication_

# 4. The array has a recovery point at all. The age is absent until the first
#    job completes -- for policy mode, the first scheduled snapshot.
#    Look for recovery_point in:
#      GET /v2/replication/source-relationships/status/
```

!!! warning "A class missing `schedulingInterval` cannot be edited"
    VolumeReplicationClass parameters are immutable. Create a new class that
    sets it and point the `VolumeReplication` at that, or recreate the class
    under the same name.

!!! note "Nothing alerts on this"
    `QumuloCSIReplicationRecoveryPointStale` fires on an age that is too
    **high**; an age that is entirely **absent** matches no alert. That is why
    the check above is manual.

## Dashboard

A starter Grafana dashboard ships in `test/grafana/qumulo-csi.json`. Import it
and pick a Prometheus datasource; it has a `datasource` variable and needs no
editing.

## Health checks

| Check | Command | Healthy |
|---|---|---|
| Pods | `kubectl -n qumulo-csi get pods` | all containers ready |
| Node coverage | `kubectl -n qumulo-csi get pods -l app.kubernetes.io/component=node` | one per node |
| Registration | `kubectl get csinodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.drivers[*].name}{"\n"}{end}'` | every line shows `csi.qumulo.com` |
| Leader | `kubectl -n qumulo-csi get lease` | one holder, renewing |
| Array reachability | `csi_qumulo_api_reachable` | `1` per cluster |
| Bound claims | `kubectl get pvc -A \| grep -v Bound` | empty |

## What is not exported

- **No array health metrics.** Node, protection and restriper health are not
  exported by the driver — watch those on the Qumulo cluster itself.
- **No TLS on `/metrics`.** Restrict it at the network layer.
- Per-volume series name a volume by `volume_name` and `volume_id` (a digest
  of its handle), never by array or path.

## Next

- [Observe what the driver is doing](observe.md)
- [Collect logs](logs.md)
- [Metrics](../reference/metrics.md)
