# Metrics

!!! note "This endpoint is opt-in"
    The driver's `/metrics` endpoint is served only with `metrics.enabled`.
    The kubelet's `kubelet_volume_stats_*` series need no configuration.

Namespace `csi_qumulo`. Base units, counters end in `_total`, and no
histogram carries a volume identifier.

Enable with `metrics.enabled: true` — see
[Monitor the driver](../operate/monitor.md).

!!! warning "Nothing is polled"
    Every array-side figure — reachability, quota usage, recovery point — is
    the last value an RPC produced, so an **idle driver's gauges are as old as
    its last request**. RPC counts and latency carry no such caveat.

    Between operator actions, the replication series move on csi-addons'
    `GetVolumeReplicationInfo` poll, paced by the VolumeReplicationClass's
    `schedulingInterval`. A class written by hand without it is polled only
    when the relationship is created, so its series stay frozen at that first
    read — see
    [Replication](#replication).

## Build

| Series | Type | Labels | Updates |
|---|---|---|---|
| `csi_qumulo_build_info` | gauge, always `1` | `driver_name`, `version`, `commit`, `build_date`, `go_version` | once, at startup |

Join it to anything with `* on (instance) group_left(version)`.

## CSI and csi-addons RPCs

| Series | Type | Labels | Updates |
|---|---|---|---|
| `csi_qumulo_rpc_total` | counter | `method`, `code` (gRPC status name, `OK` included) | every RPC, controller and node |
| `csi_qumulo_rpc_panics_total` | counter | `method` | a handler panicked; it was recovered and answered `Internal`, and the process kept serving |
| `csi_qumulo_rpc_duration_seconds` | histogram | `method` | every RPC |

`method` is the **full** gRPC path, e.g.
`/csi.v1.Controller/CreateVolume` — the same form the kubelet's
`csi_operations_seconds` and the sidecars' `csi_sidecar_operations_seconds`
use, so one Grafana panel can span all three layers. Every csi-addons
replication RPC passes through the same interceptor under
`/replication.Controller/...`.

!!! note "The histogram is deliberately not labelled by code"
    The code lives on `rpc_total`; a per-code histogram multiplies the series
    count by the gRPC code space and answers nothing extra. Buckets run to
    300 s because two RPCs are legitimately slow — a restore copies a tree
    file by file, and a tree delete waits on an array job.

!!! warning "Any increase in `rpc_panics_total` is a driver bug"
    Please report it.

## Qumulo REST API

| Series | Type | Labels | Updates |
|---|---|---|---|
| `csi_qumulo_api_requests_total` | counter | `cluster`, `method`, `path_template`, `status` | every REST round trip; retired after an hour with no call |
| `csi_qumulo_api_duration_seconds` | histogram | `cluster`, `method`, `path_template` | same |
| `csi_qumulo_api_reachable` | gauge 0/1 | `cluster` | every REST round trip |

- **`cluster`** is the host (and port) of the client's `apiUrl`, e.g.
  `qumulo.example.com:8000`. It is never a token and never derived from one.
- **`path_template`** is the request path with variable segments replaced by
  `{id}` — `/v1/files/{id}/info/attributes`, `/v1/files/quotas/{id}`. Query
  strings are dropped. Labelling verbatim paths would mint one series per
  volume.
- **`status`** is the HTTP status as a string, or **`transport`** when the
  array never answered at all (DNS, TCP, TLS or timeout). That distinction is
  the value of the series: a 401 proves the array is up.

!!! note "`api_reachable` is `1` for any HTTP answer — 401, 403, 404 and 500 included"
    It used to be `0` for 401/403, but the gauge is one per array while a
    token is per StorageClass Secret: one bad Secret made it flip on every
    call, so `QumuloCSIArrayUnreachable` both misfired and never held. A dead
    token is `QumuloCSICredentialRejected`; a missing privilege is
    `QumuloCSIPrivilegeMissing`.

## Volumes

| Series | Type | Labels |
|---|---|---|
| `csi_qumulo_volume_quota_bytes` | gauge | `volume_id`, `volume_name` |
| `csi_qumulo_volume_used_bytes` | gauge | `volume_id`, `volume_name` |
| `csi_qumulo_volume_replication_protected` | gauge | `volume_id`, `volume_name` |

These are the **array's** view — the quota Qumulo enforces and what it says is
used against it, which the kubelet cannot see. The per-volume disk figures the
kubelet already publishes are not duplicated.

`volume_id` is the first 16 hex digits of the SHA-256 of the volume's CSI
handle **in its canonical form**, `<server>#<apiUrl>#<realPath>` with the path
cleaned. It used to be the handle itself, which told anyone who could read
`/metrics` every volume's array, endpoint and path (D-88); the digest names
none of them, and still keeps a DR copy apart from its source.

The canonical form matters for a volume an older build provisioned: its PV
carries the legacy two-field handle `<server>#<realPath>`, which names the same
volume as `<server>##<realPath>` (an empty endpoint field) and is hashed as
that, so the two spellings are one series. Hashing a legacy handle as written
gives a value no series carries.

```sh
h=$(kubectl get pv pvc-1234 -o jsonpath='{.spec.csi.volumeHandle}')
case "$h" in *#*#*) ;; *) h="${h%%#*}##${h#*#}" ;; esac  # legacy <server>#<path>
h="${h%/}"                                              # path is cleaned: no trailing /
printf %s "$h" | sha256sum | cut -c1-16
```

!!! warning "Upgrade note: legacy-handle volumes change `volume_id` once"
    An earlier build hashed the handle as written, so on upgrade every
    legacy-handle volume's series moves, once, to a new `volume_id`; volumes
    with a current three-field handle keep theirs. A dashboard or recording
    rule pinned to an old `volume_id` value must be updated; one that joins
    through `volume_name` is unaffected. The old series is not written again
    and goes when the controller restarts, which an upgrade does.

`volume_name` is the last path component — for a dynamically provisioned
volume, the PV's name — so these join to `kube_persistentvolume_info` and
`kube_persistentvolumeclaim_info` from kube-state-metrics, and per-namespace
views need no extra labels here.

**When they update:**

| RPC | What it sets |
|---|---|
| `CreateVolume` | the quota limit, at creation |
| `ControllerExpandVolume` | the old limit, then the new one, and the usage read before writing it |
| `ControllerGetVolume` | limit and usage |
| `ListVolumes` | limit and usage, for every volume on the page returned |
| `CreateSnapshot` | limit and usage of the source volume |
| `DeleteVolume` | **retires** every series for that handle, on success only |

`volume_replication_protected` updates on every
`GetVolumeReplicationInfo` — for the volume named, and for every member of a
group when the verdict came from the group's relationship.

## Arrays

| Series | Type | Labels | Updates |
|---|---|---|---|
| `csi_qumulo_cluster_capacity_bytes` | gauge | `cluster` | whenever a call reads `/v1/file-system` |
| `csi_qumulo_cluster_free_bytes` | gauge | `cluster` | same |

In practice that is `GetCapacity`, which `csi-provisioner` polls once per
StorageClass when `sidecars.capacity.enabled` is on. With capacity reporting
off these move only when something else reads cluster capacity.

!!! note "Cluster-wide, not per class"
    A Qumulo directory quota is a logical limit with no pool behind it, so
    every class on one array reports against the same number.

## Replication

| Series | Type | Labels |
|---|---|---|
| `csi_qumulo_replication_recovery_point_age_seconds` | gauge | `relationship_id`, `role` (`source`/`target`) |
| `csi_qumulo_replication_state` | gauge, always `1` | `relationship_id`, `state` |
| `csi_qumulo_replication_queued_snapshots` | gauge | `relationship_id` |
| `csi_qumulo_replication_mode` | gauge, always `1` | `relationship_id`, `mode` — from the **source end only** |
| `csi_qumulo_replication_policy_interval_seconds` | gauge | `relationship_id` |

The first four update on every replication-status read. The interval series
appears only for a `REPLICATION_SNAPSHOT_POLICY` relationship whose source end
this controller read: it is the longest gap between two snapshots of the
linked policy, re-read at most every 5 minutes.

Populated whenever any csi-addons replication RPC runs — `Enable`, `Disable`,
`Promote`, `Demote`, `Resync` and `GetVolumeReplicationInfo` all read
relationship status as their first move. Requires
`sidecars.csiAddons.enabled`.

!!! warning "Scrape the lease holder"
    Only the controller replica holding the `csi-qumulo-com-csi-addons` lease
    serves these RPCs. The other replicas export **no** replication series, so
    scrape that one — or all of them.

!!! warning "Mode present, age absent"
    The age appears only once the driver has read a recovery point. A
    relationship exporting `csi_qumulo_replication_mode` with no
    `csi_qumulo_replication_recovery_point_age_seconds` past its first sync
    has not been read again since it was created — usually a
    VolumeReplicationClass with no `schedulingInterval`. **Nothing alerts on
    this.** See
    [Replication metrics missing or frozen](../operate/monitor.md#replication-metrics-missing-or-frozen).

## Restore staging

| Series | Type | Labels |
|---|---|---|
| `csi_qumulo_restore_staging_directories_total` | counter | `cluster`, `state` (`abandoned`/`removed`) |

`abandoned` counts a staging directory a `CreateVolume` sweep first found with
no claim and no PV behind it; `removed` counts one the driver removed.

## Inventory

| Series | Type | Labels |
|---|---|---|
| `csi_qumulo_inventory_class_credential_usable` | gauge 0/1 | `class`, `cluster`, `reason` |

Only with `controller.inventory.enabled`. `1` means the call built a
credential for that class and listed its volumes. `0` means the class was
**skipped** and its volumes are absent from the answer.

!!! warning "Alert on this one"
    `ListVolumes` skips a class whose credential is permanently unusable
    rather than failing the whole call, so one misconfigured class cannot hide
    every other class's volumes — but `ListVolumesResponse` has no field for a
    class that was left out. Without this gauge the only trace of a short
    listing is a log line, and a reconciler comparing PVs against the listing
    is told nothing at all.

`reason` is empty at `1`, and otherwise:

| `reason` | Meaning | Fix |
|---|---|---|
| `forbidden` | the class names a Secret this release does not, so the Role does not grant `get` — by far the most likely, because a class created outside the chart is ordinary operation | add it to `rbac.extraSecrets`, or set `rbac.clusterWideSecrets: true` |
| `missing` | the Secret does not exist | create it |
| `unusable` | the class names no Secret, or its Secret has no token key | fix the class or the Secret |

All three are permanent — a transient failure fails the RPC instead and shows
up as a `ListVolumes` error in `csi_qumulo_rpc_total`, so this gauge never
flickers on a throttled API server.

## Go and process

Standard `go_*` and `process_*` collectors, registered on purpose: the first
thing anyone asks after a controller restart is what its memory and goroutines
were doing.

## Series this driver does not produce

| Series | From | Scraped by |
|---|---|---|
| `csi_sidecar_operations_seconds` | `csi-provisioner`, `csi-resizer`, `csi-snapshotter` — how long the **sidecar** waited, its own retries included | this chart's `ServiceMonitor`, once `metrics.enabled` gives the sidecars `--http-endpoint` |
| `csi_operations_seconds` | the kubelet, for every node RPC | your kubelet scrape job |
| `kubelet_volume_stats_*` | the kubelet, because this driver implements `NodeGetVolumeStats` | your kubelet scrape job |

!!! tip "Compare the two timings"
    `csi_qumulo_rpc_duration_seconds` for `CreateVolume` is **one attempt**;
    `csi_sidecar_operations_seconds` for the same operation is every attempt
    plus the backoff between them. A provisioning problem usually shows in the
    second before the first.

## Cardinality

Bounded by design. `path_template` is a fixed set of about thirty templates,
`method` a fixed set of CSI and csi-addons methods, `cluster` one per array a
class names, `code` the gRPC code space.

The two unbounded-in-principle label sets are `volume_id`/`volume_name` — one
series pair per volume the controller has touched since it started — and
`relationship_id`. `DeleteVolume` retires a volume's series; a relationship's
series are retired when a complete listing of its array no longer has it, or
when the driver deletes it.

## Alerts

`metrics.prometheusRule.enabled` renders eight rules, each covered by a
promtool unit test with a case that must fire and one that must not.

| Alert | Expression | For |
|---|---|---|
| `QumuloCSIArrayUnreachable` | `csi_qumulo_api_reachable == 0` | 5m |
| `QumuloCSICredentialRejected` | `sum by (cluster) (increase(csi_qumulo_api_requests_total{status="401"}[10m])) > 0` | 5m |
| `QumuloCSIPrivilegeMissing` | the same for `status="403"`, excluding `/v1/files/resolve` and the aggregates endpoint | 10m |
| `QumuloCSIProvisioningFailing` | `sum by (code) (rate(csi_qumulo_rpc_total{method="/csi.v1.Controller/CreateVolume",code!="OK"}[10m])) > 0` | 10m |
| `QumuloCSIReplicationRecoveryPointStale` | mode-aware thresholds — see below | 10m |
| `QumuloCSIVolumeNearQuota` | `csi_qumulo_volume_used_bytes / csi_qumulo_volume_quota_bytes > 0.9` | 15m |
| `QumuloCSIVolumeUnprotected` | `csi_qumulo_volume_replication_protected == 0` | 10m |
| `QumuloCSIInventoryClassSkipped` | `csi_qumulo_inventory_class_credential_usable == 0` | 10m |

Thresholds: `metrics.prometheusRule.recoveryPointAgeSeconds` (900),
`policyModeRecoveryPointAgeSeconds` (5400),
`unknownModeRecoveryPointAgeSeconds` (defaults to the policy threshold),
`policyIntervalFactor` (1.5) and `volumeNearQuotaRatio` (0.9).

!!! note "Why the recovery-point alert has several thresholds"
    A continuous relationship's recovery point keeps up with writes, so a
    growing age means it stalled. A policy-mode one advances only when a
    scheduled snapshot replicates, so on a healthy pair the age climbs to the
    schedule interval every cycle — with one 900 s threshold an hourly policy
    pair met the condition for about 35 minutes of every hour (D-25). Where
    `csi_qumulo_replication_policy_interval_seconds` exists, the alert uses
    `policyIntervalFactor × interval` instead, never below
    `recoveryPointAgeSeconds`. A DR-side controller sees no `mode` series —
    only the source end reports one — so it gets
    `unknownModeRecoveryPointAgeSeconds`. A daily schedule needs about
    129600.

!!! tip "`QumuloCSIVolumeUnprotected` beats the `VolumeReplication` object"
    On csi-addons v0.15.0 a `Completed` primary keeps reading `Healthy` after
    the relationship is removed on the arrays. The gauge is keyed by volume
    rather than by relationship because an absent relationship has no ID to
    hang a series on.

## Dashboard

`test/grafana/qumulo-csi.json`. Import it and pick a Prometheus datasource; it
has a `datasource` variable and needs no editing.

## What is not exported

- **No array health metrics** — node, protection and restriper health.
- **No TLS on `/metrics`**, and no authentication. Restrict it at the network
  layer; `metrics.networkPolicy.enabled` covers the controller only, not the
  hostNetwork node pods.

## Next

- [Monitor the driver](../operate/monitor.md)
- [Observe what the driver is doing](../operate/observe.md)
