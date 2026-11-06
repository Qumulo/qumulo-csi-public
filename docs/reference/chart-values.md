# Chart values

Release-wide settings, set as Helm values. Per-volume settings are
[StorageClass parameters](storageclass-parameters.md).

!!! note "There is no chart-wide cluster address or token"
    `server`, `apiUrl` and `secretName` are per StorageClass, which is what
    lets one release serve many Qumulo clusters. The old top-level forms were
    removed; the controller logs a warning naming any it finds in the
    environment.

## Global

| Value | Type | Default | Description | Upgrade consideration |
|---|---|---|---|---|
| `driverName` | string | `csi.qumulo.com` | the CSI plugin name | **cannot change** — every existing PV names the old driver; a second name needs a second release |
| `nameOverride`, `fullnameOverride` | string | `""` | object naming | changing renames objects |
| `commonLabels` | map | `{}` | labels added to every object | |
| `fsGroupPolicy` | enum | `None` | `None`, `File`, `ReadWriteOnceWithFSType` | mutable on the CSIDriver; `helm upgrade` can change it in place. With `None` the kubelet never changes file owners, so a pod's `fsGroup` grants it nothing — access is decided by `mountPermissions` and the pod's uid/gid alone. That is what makes `0750` read-only for an OpenShift `restricted-v2` pod. |
| `requireAllowedClients` | bool | `true` | refuse a `dedicated-export` class that restricts nobody | an existing open class stops the upgrade |
| `allowClassRemoval` | bool | `false` | permit an upgrade that drops classes the release manages | |

## Images

| Value | Type | Default | Description |
|---|---|---|---|
| `image.controller.repository` | string | `ghcr.io/qumulo/qumulo-csi-driver-controller` | |
| `image.controller.tag` | string | the chart's `appVersion` | may carry `@sha256:<digest>` |
| `image.node.repository` | string | `ghcr.io/qumulo/qumulo-csi-driver-node` | |
| `image.node.tag` | string | the chart's `appVersion` | |
| `image.pullSecrets` | list | `[]` | `[{name: ghcr-pull}]` |

!!! tip "Pin the digest for a reproducible install"
    `tag: "0.1.0@sha256:<digest>"`. A locally built image has no registry in
    its name, so set `repository` to the bare name.

## Qumulo API

| Value | Type | Default | Description |
|---|---|---|---|
| `qumulo.apiTimeout` | duration | `10s` | per-request timeout for REST calls |
| `qumulo.insecureSkipTlsVerify` | bool | `false` | **development only** |
| `qumulo.caBundle.existingConfigMap` | string | `""` | a ConfigMap holding a private CA in PEM form |
| `qumulo.caBundle.key` | string | `ca.crt` | the key, and the filename it is mounted as |

!!! warning "TLS settings are process-wide"
    One policy and one bundle serve every cluster the release reaches. A
    changed CA needs a controller restart (`SSL_CERT_FILE` is read once per
    process). Setting `caBundle` together with `insecureSkipTlsVerify: true`
    is refused at install.

## Controller

| Value | Type | Default | Description |
|---|---|---|---|
| `controller.replicaCount` | int | `2` | a standby takes the leases in about one lease period |
| `controller.leaderElection` | bool | `true` | more than one replica with it off is refused |
| `controller.leaderElectionTimings` | map | `{}` | `leaseDuration`, `renewDeadline`, `retryPeriod`; unset keeps the sidecars' 15s/10s/5s |
| `controller.logLevel` | string | `info` | `debug` for diagnosis |
| `controller.podDisruptionBudget.enabled` | bool | `true` | `maxUnavailable: 1`, rendered only with >1 replica |
| `controller.strategy` | map | `{}` | verbatim; `Recreate` when leader election is off |
| `controller.topologySpreadConstraints` | list | `[]` | verbatim |
| `controller.resources`, `controller.sidecarResources` | map | driver 1Gi, each sidecar 128Mi; memory limit = request, ephemeral-storage limit 500Mi | no CPU limit; see `values.yaml` for the sizing |
| `controller.priorityClassName` | string | `system-cluster-critical` | |
| `controller.nodeSelector`, `tolerations`, `affinity` | | `{}`/`[]` | the default affinity is a soft hostname anti-affinity |
| `controller.terminationGracePeriodSeconds` | int | `90` | |
| `controller.podSecurityContext`, `securityContext` | map | non-root | |
| `controller.inventory.enabled` | bool | `false` | `ListVolumes`, `ControllerGetVolume` |
| `controller.proxy.httpsProxy`, `httpProxy` | string | `""` | proxy for the controller's Qumulo API calls; empty is no proxy |
| `controller.proxy.noProxy` | string | `""` | hosts reached directly; `.svc`, `.cluster.local` and the Kubernetes API service are always added |

## Node

| Value | Type | Default | Description |
|---|---|---|---|
| `node.kubeletDir` | path | `/var/lib/kubelet` | **non-default on k3s and microk8s** |
| `node.logLevel` | string | `info` | |
| `node.unmountTimeout` | duration | `""` → 30s | how long a plain `umount(8)` is given before escalating |
| `node.minReadySeconds` | int | `10` | |
| `node.healthPort` | int | `29653` | the node livenessprobe's `/healthz`; a host port bound to the node's address. Change it when csi-driver-nfs also runs (it uses 29653) |
| `node.updateStrategy` | map | `{}` | verbatim |
| `node.priorityClass.create` | bool | `true` | needs `get`/`create`/`update` on `priorityclasses` |
| `node.priorityClass.value` | int | `1000000000` | **immutable** once created |
| `node.priorityClass.preemptionPolicy` | enum | `Never` | **immutable**; `PreemptLowerPriority` lets the plugin evict to fit |
| `node.priorityClassName` | string | `""` | name an existing class instead, e.g. `system-node-critical` |
| `node.kerberos.enabled` | bool | `false` | mounts the host's `krb5.conf` and keytab |
| `node.kerberos.keytabPath` | path | `/etc/krb5.keytab` | |
| `node.kerberos.krb5ConfPath` | path | `/etc/krb5.conf` | |
| `node.kerberos.hostPathType` | enum | `FileOrCreate` | `File` refuses the plugin on a node without the files |
| `node.resources` | map | driver 256Mi, registrar and liveness probe 64Mi; memory limit = request, ephemeral-storage limit 500Mi | no CPU limits: a throttled unmount is a stuck pod |
| `node.registrarHealthPort` | int | `0` → `node.healthPort` + 6 (29659) | the registrar's `/healthz`, read by its own probes; a host port on the node's address, distinct from `node.healthPort`. Derived by default so a second release, with its own `node.healthPort`, gets its own |
| `node.terminationGracePeriodSeconds` | int | `30` | |

!!! warning "A PriorityClass's `value` and `preemptionPolicy` are immutable"
    An upgrade that changes one is refused, naming the fix: keep the old
    value, or delete the class and reinstall.

!!! warning "Do not raise `unmountTimeout` past the workload's grace period"
    The point is to finish inside it. The kubelet gives each unmount a fixed
    two minutes, so a value near that is cut — about 100 s in effect.

## Sidecars

| Value | Default | Enables |
|---|---|---|
| `sidecars.provisioner.*` | always on | image, `timeout`, `extraArgs` |
| `sidecars.resizer.*` | always on | |
| `sidecars.nodeDriverRegistrar.*` | always on | |
| `sidecars.livenessProbe.*` | always on | |
| `sidecars.snapshotter.enabled` | `false` | snapshots; needs the snapshot CRDs |
| `sidecars.snapshotter.snapshotNamePrefix` | `snapshot` | passed to **both** the sidecar and the driver |
| `sidecars.capacity.enabled` | `false` | `CSIStorageCapacity` per class |
| `sidecars.csiAddons.enabled` | `false` | replication |
| `sidecars.healthMonitor.enabled` | `false` | polls `ControllerGetVolumeHealth`; needs inventory |

!!! warning "Do not change `snapshotNamePrefix` on a release that has snapshots"
    It is how the driver recognises the snapshots it took; existing ones would
    no longer be recognised. Set it here and not in `extraArgs`, which the
    chart refuses.

!!! tip "Raise `sidecars.provisioner.timeout` for large restores"
    A restore's per-attempt budget derives from it.

## Metrics

| Value | Type | Default | Description |
|---|---|---|---|
| `metrics.enabled` | bool | `false` | `/metrics` on both pods and the sidecars |
| `metrics.forceEnable` | bool | `false` | render `ServiceMonitor`/`PrometheusRule` without the CRD check |
| `metrics.port.controller` | int | `29654` | |
| `metrics.port.node` | int | `29655` | a **host** port |
| `metrics.port.provisioner` | int | `29656` | |
| `metrics.port.resizer` | int | `29657` | |
| `metrics.port.snapshotter` | int | `29658` | |
| `metrics.service.*` | | | the Services rendered to scrape |
| `metrics.networkPolicy.enabled` | bool | `false` | restrict who may scrape the controller |
| `metrics.networkPolicy.from` | list | | peers permitted |
| `metrics.serviceMonitor.enabled` | bool | `false` | needs the Prometheus Operator CRD |
| `metrics.prometheusRule.enabled` | bool | `false` | the eight alerts |
| `metrics.prometheusRule.recoveryPointAgeSeconds` | int | `900` | continuous-mode RPO threshold |
| `metrics.prometheusRule.policyModeRecoveryPointAgeSeconds` | int | `5400` | policy-mode threshold |
| `metrics.prometheusRule.unknownModeRecoveryPointAgeSeconds` | int | the policy threshold | for a DR-side controller that cannot see the mode |
| `metrics.prometheusRule.policyIntervalFactor` | float | `1.5` | × the measured policy interval, when known |
| `metrics.prometheusRule.volumeNearQuotaRatio` | float | `0.9` | |

!!! warning "The node metrics port is a host port"
    The node DaemonSet is `hostNetwork: true`. 29652 and `node.healthPort`
    (29653) are the livenessprobe sidecars; the chart refuses any collision.
    An install or upgrade is also refused when another DaemonSet on the
    cluster (a second release of this chart, `csi-driver-nfs`) already holds
    `node.healthPort`, `node.registrarHealthPort` or `metrics.port.node`.

## RBAC

| Value | Type | Default | Description |
|---|---|---|---|
| `rbac.extraSecrets` | list | `[]` | Secrets named only outside the chart |
| `rbac.clusterWideSecrets` | bool | `false` | the old cluster-wide `get` on Secrets |
| `rbac.createBindings` | bool | `true` | render the chart's RoleBindings and ClusterRoleBindings; `false` renders none, for a platform team to apply ([Bindings applied by a platform team](../operate/openshift-operations.md#bindings-applied-by-a-platform-team)) |
| `rbac.checkBindings` | bool | `true` | with `createBindings: false`, stop the install while a required binding is missing or wrong; read-only. `false` skips the check, for an install identity that may not read bindings |

!!! warning "Each Secret's namespace must exist at install time"
    The Role is created in it.

## OpenShift

| Value | Type | Default | Description |
|---|---|---|---|
| `openshift.enabled` | `auto`, `true`, `false` | `auto` | `auto` turns on when the cluster serves `SecurityContextConstraints`; set `true` for `helm template` and GitOps |
| `openshift.controllerSCC` | string | `restricted-v2` | the SCC the controller pods and the `helm test` pods are pinned to, for example your own `restricted-csi` |
| `openshift.assignControllerUID` | `auto`, `true`, `false` | `auto` | leave the controller's fixed uid out so OpenShift assigns one from the namespace range. `auto`: when `controllerSCC` starts with `restricted`; `true`: always (a custom SCC with another name that assigns a uid); `false`: never (an SCC that lets the pod choose, such as `nonroot-v2`) |
| `openshift.nodeSCC.create` | bool | `true` | the chart makes the node SCC and grants it to the node service account only |
| `openshift.nodeSCC.name` | string | `""` → `<namespace>-<release>-qumulo-csi-driver-node`, or `<release>-qumulo-csi-driver-node` when the release is named after its namespace | with `create: false`, the name of an SCC your cluster admin made |
| `openshift.trustedCABundle` | bool | `false` | trust the cluster's injected CA bundle for the Qumulo API; not with `qumulo.caBundle` or `insecureSkipTlsVerify` |

See [Install on OpenShift](../get-started/openshift.md).

## `helm test`

| Value | Type | Default | Description |
|---|---|---|---|
| `tests.ready.enabled` | bool | `true` | the readiness test and its read-only ServiceAccount and RBAC |
| `tests.ready.image` | string | `""` → `ose-cli-rhel9:v4.22` on OpenShift, `origin-cli:4.22` elsewhere | needs bash and kubectl |
| `tests.storageClass` | string | `""` | turns on the volume test, on this class |
| `tests.size` | quantity | `1Gi` | the volume test's claim size |
| `tests.image` | string | `registry.access.redhat.com/ubi9/ubi-minimal:9.8` | the volume test's image: a POSIX shell, `cat` and `sync` |
| `tests.imagePullPolicy` | `Always`, `IfNotPresent`, `Never` | `IfNotPresent` | |

The readiness test checks the controller is Available, every node pod is Ready
and the driver is registered in CSINode, waiting up to 240 s for a rollout
still in progress; it provisions nothing. The volume test
pod runs as a non-root user, so its class must let one write
(`mountPermissions: "0777"`, the default). Run `helm test` without `--logs`.

## Class lists

| Value | Type | Default |
|---|---|---|
| `storageClasses` | list | `[]` — see [StorageClass parameters](storageclass-parameters.md) |
| `volumeSnapshotClasses` | list | `[]` |
| `volumeReplicationClasses` | list | `[]` |

### `volumeSnapshotClasses[]`

| Key | Notes |
|---|---|
| `name` | required |
| `deletionPolicy` | `Delete` \| `Retain` |
| `secretName`, `secretNamespace` | required: snapshot RPCs carry their own credential |
| `apiurl` | take snapshots through another REST endpoint of the volume's **own** cluster |

!!! warning "An `apiurl` override is not followed by restore"
    The snapshot handle records the endpoint, so a later delete reaches it —
    but `CreateVolume` dials the **StorageClass's** endpoint, so a restore
    needs a class whose `apiUrl` is that same override. The driver compares
    cluster UUIDs and refuses a different cluster; when either endpoint cannot
    be read, the check is skipped and the snapshot taken with a warning
    (D-14).

### `volumeReplicationClasses[]`

| Key | Notes |
|---|---|
| `name`, `targetServer`, `secretName` | required; `targetServer` is the peer's bare host or IPv4 |
| `targetApiUrl`, `apiUrl` | **required** whenever the corresponding `server` is not an IP literal |
| `targetBasePath` | defaults to the source volume's own parent |
| `replicationMode` | `continuous` (default) \| `policy` \| `policy+continuous` |
| `linkSnapshotPolicy` | `policy` modes only; refused with `continuous` |
| `schedulingInterval` | default `5m` — the csi-addons poll pace, **not** a lag threshold. The chart always sets it; a class written by hand **must set it too** (see the warning below) |
| `allowNonEmptyTarget` | let protection adopt a peer directory that already holds data |
| `replicationScope` | `volume` (default) \| `group` |
| `secretNamespace` | defaults to the release namespace |

!!! warning "A hand-written VolumeReplicationClass must set `schedulingInterval`"
    Without it, csi-addons calls `GetVolumeReplicationInfo` only when the
    relationship is **created** — before its first sync. So the
    `VolumeReplication`'s `lastSyncTime` stays empty and the replication
    metrics freeze at that first read:
    `csi_qumulo_replication_mode` is exported while
    `csi_qumulo_replication_recovery_point_age_seconds` never appears.
    Nothing alerts on this. The chart's own classes always set it.

    Because class parameters are immutable, a class missing it cannot be
    edited — create a new class with it and point the `VolumeReplication` at
    that, or recreate the class under the same name. See
    [Replication metrics missing or frozen](../operate/monitor.md#replication-metrics-missing-or-frozen).

!!! warning "VolumeReplicationClass parameters are immutable"
    A different mode, scope or endpoint means a new class name. An upgrade
    that changes one in place fails the release.

## Values the chart refuses

| Combination | Why |
|---|---|
| `caBundle` with `insecureSkipTlsVerify: true` | supplies a CA and then ignores it |
| `leaderElection: false` with more than one replica | every replica would act on every claim |
| a `dedicated-export` class with no restriction | the export would admit anyone, as root |
| `protocol` together with an `nfsvers=` mount option | ambiguous |
| an `nfs3` class asking for `lock` | server-side v3 locking cannot work on this node |
| `tenantID`, `allowedClients` or `securityFlavor` on a shared class | there is no per-volume export to scope |
| a metrics port colliding with a livenessprobe port | |
| `snapshotNamePrefix` in `extraArgs` | the driver needs it too |
| an unknown key in any class list | a typo would otherwise be silently dropped |