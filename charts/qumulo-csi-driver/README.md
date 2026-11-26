# qumulo-csi-driver

Helm chart for the Qumulo CSI driver.

A release can serve **many** Qumulo clusters. Each StorageClass names its own
`server`, and its own Secret holding that cluster's API token; the controller
builds an API client per request rather than holding one. There is no
chart-wide cluster address: `server` and `secretName` are required on every
class, and a class missing either is refused at install.

## Contents

- [Install](#install)
- [What gets deployed](#what-gets-deployed)
- [OpenShift](#openshift)
- [Upgrading](#upgrading)
- [Uninstalling and reinstalling](#uninstalling-and-reinstalling)
- [StorageClass parameters](#storageclass-parameters)
- [TLS](#tls)
- [Serving two clusters from one release](#serving-two-clusters-from-one-release)
- [Snapshots](#snapshots)
- [Scheduled snapshots](#scheduled-snapshots)
- [Replication (csi-addons)](#replication-csi-addons)
- [Prometheus metrics](#prometheus-metrics)
- [Capacity reporting](#capacity-reporting)
- [Volume inventory](#volume-inventory)
- [Multi-tenant exports and IP restrictions](#multi-tenant-exports-and-ip-restrictions)
- [Kerberos](#kerberos)
- [adDomain](#addomain)
- [Migrating from the community driver (`qumulo.csi.k8s.io`)](#migrating-from-the-community-driver-qumulocsik8sio)
- [Static PV handles are not scoped to a managed root](#static-pv-handles-are-not-scoped-to-a-managed-root)
- [Required Qumulo API privileges](#required-qumulo-api-privileges)
- [Development](#development)

## Install

Install the chart published with each release,
`oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver` at version `0.1.2`;
it pulls the images of its own build. The images are pulled from the GitHub Container Registry,
`ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller` and `-node`, for `linux/amd64`
and `linux/arm64`. They are visible to members of the Qumulo GitHub
organization only, so create a pull secret first, from a classic token with
the `read:packages` scope that is SSO-authorized for the organization
([install guide, step 2](../../docs/get-started/install.md#2-get-access-to-the-images)):

```sh
kubectl create namespace qumulo-csi
kubectl -n qumulo-csi create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io --docker-username=<github-user> \
  --docker-password="$(cat ~/.ghcr-pat)"
```

```sh
cat ~/.ghcr-pat | helm registry login ghcr.io -u <github-user> --password-stdin
helm install qumulo-csi oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver \
  --version 0.1.2 \
  --namespace qumulo-csi \
  --set 'image.pullSecrets[0].name=ghcr-pull' \
  --set storageClasses[0].name=qumulo-nfs \
  --set storageClasses[0].server=qumulo.example.com \
  --set storageClasses[0].secretName=qumulo-api-token \
  --set storageClasses[0].basePath=/csi/volumes
```

`basePath` must already exist on the cluster and sit inside an NFS export an
administrator created. The driver provisions directories under it; it does not
create the parent, and under the default provisioning model it does not create
exports either.

### Check the install with `helm test`

```sh
helm test qumulo-csi -n qumulo-csi
```

Leave out `--logs`: a passing test pod is deleted at once, so Helm finds no log
to print and exits 1 on a pass. Two tests:

- **Readiness, always on** (`<release>-qumulo-csi-driver-test-ready`). Checks
  that the controller Deployment is Available, that every node pod is Ready,
  and that the kubelet has registered the driver in CSINode on as many nodes
  as run a node pod. Each check waits for a rollout still in progress, all
  within 240 s, so it can run straight after `helm install/upgrade --wait`,
  which returns with one node pod still starting. It provisions nothing and
  calls no Qumulo cluster, so it passes on any cluster the chart installed on. It runs as a read-only
  ServiceAccount the chart creates, `<release>-qumulo-csi-driver-test`, which
  may read the controller Deployment, the node DaemonSet and CSINodes, and
  nothing else. It runs from the controller image, as
  `qumulo-csi-driver ready-check`, so it pulls no image the release does not
  already run; `tests.ready.image` points it at a mirror of the controller
  image. `tests.ready.enabled: false` removes the test and its
  ServiceAccount.
- **Volume, opt-in** (`tests.storageClass`). A pod with a 1 GiB generic
  ephemeral volume on that class writes a file, reads it back and removes it,
  so a pass means the controller, the node plugin and the array's export all
  work for an ordinary application. It runs only where the node plugin runs
  (it takes `node.nodeSelector`, `node.tolerations` and `node.affinity`),
  under the controller's SCC on OpenShift (`openshift.testSCC`, which follows
  `openshift.controllerSCC`, or is `restricted-v2` with `controller.hostNetwork`). It runs
  as a non-root user (OpenShift assigns its uid under `restricted-v2` and
  copies of it; elsewhere it is 65532), so the
  class's `mountPermissions` must let a non-root user write: the default
  `0777` does, a `0750` class fails, as it fails real non-root applications.
  `tests.image` (default `registry.access.redhat.com/ubi9/ubi-minimal:9.9`, pinned by digest)
  needs a POSIX shell, `cat` and `sync`.

A failed test keeps its pod (and the volume test its claim):
`kubectl -n qumulo-csi logs <pod>` shows why, and the next `helm test`
replaces them.

**Do not install from this folder outside development.** The chart in the
repo is `0.1.2`, and its default image tag is its appVersion, `0.1.2`, which
every publish of that version moves. It is pulled `Always` for that reason --
a node never keeps an earlier cached `0.1.2` -- but `helm history` cannot tell
two publishes of one version apart, and the image can be from another commit
than the chart you are holding. A released chart,
`make chart-package`'s `bin/qumulo-csi-driver-<VERSION>.tgz`, carries the same
version and appVersion, so it names the same images; what makes it the one to
install is that `make release REGISTRY=...` publishes the images and then that
chart together, and CI runs it on every merge to `main`. To install one exact
build rather than whatever `0.1.2` now points at, set the image tag to a
digest: `--set image.controller.tag=0.1.2@sha256:<digest>`. A build that was only built
locally (`make docker-build`) has no registry in its name, so point the chart
at it, and say to use the copy the node has:
`--set image.controller.repository=qumulo-csi-driver-controller --set image.node.repository=qumulo-csi-driver-node --set image.controller.pullPolicy=IfNotPresent --set image.node.pullPolicy=IfNotPresent`.

## What gets deployed

| Object | Purpose |
|---|---|
| `Deployment` (controller) | driver + `csi-provisioner` + `csi-resizer` + `livenessprobe` |
| `DaemonSet` (node) | driver + `csi-node-driver-registrar` + `livenessprobe`. Every container in both workloads has a liveness and a readiness probe, a CPU request, a memory limit equal to its memory request, and an ephemeral-storage limit, and no CPU limit (a throttled mount or unmount is a stuck pod) |
| `PriorityClass` (`<release>-qumulo-csi-driver-node-critical`) | given to the node pods, so ordinary workloads cannot preempt the plugin that unmounts their volumes. Value 1000000000, `preemptionPolicy: Never` by default: the plugin never evicts a workload to fit, but a node plugin pod re-rolled onto a packed node can stay Pending, and that node's volumes cannot mount or unmount until room is made. `node.priorityClass.preemptionPolicy: PreemptLowerPriority`, which `system-node-critical` has, lets it evict a lower-priority pod instead. Both fields are immutable: an upgrade that changes one is refused, naming the fix. Cluster-scoped: **installing and upgrading need `get`, `create` and `update` on `priorityclasses`**, and an installer without them fails the upgrade cleanly. Not created when `node.priorityClassName` names another class (such as `system-node-critical`, where the cluster admits it in this namespace), or with `node.priorityClass.create: false`, which is how to install without those rights |
| `CSIDriver` | `attachRequired: false`, `podInfoOnMount: false` |
| RBAC | one ServiceAccount per role; the node's has no permissions. The controller has no cluster-wide pods access: the sidecars that read a pod read their own, through a namespaced Role. It reads only the Secrets the release names, through one Role per namespace (`rbac.extraSecrets` for others, `rbac.clusterWideSecrets` for the old cluster-wide `get`) |

The chart renders **no** Secret. Each StorageClass names one that already
exists (`secretName`), for two reasons that happen to point the same way: a
token written into a values file would end up in `helm get values` and in
whatever stores that file, and a Secret the chart does not own survives an
uninstall along with everything else outside the release.

**Two images, deliberately.** The controller runs a minimal UBI micro image; the node
image carries an NFS client userland, because `mount(8)` and `mount.nfs` are
what actually perform the mount. Only the controller receives the API token —
the node service never calls the Qumulo API, so a cluster-wide credential never
reaches a worker.

**No `csi-attacher`,** because the driver implements no `ControllerPublishVolume`.
With `attachRequired: true` every pod would block on a `VolumeAttachment`
nothing fulfils.

**`csi-snapshotter` is optional and off by default.** Set
`sidecars.snapshotter.enabled` and add a `volumeSnapshotClasses` entry to turn
snapshots on. It is off by default because it cannot start without the
cluster-wide snapshot CRDs, and a container that will not start takes the whole
controller pod -- provisioning included -- with it. See "Snapshots" below.
`sidecars.snapshotter.snapshotNamePrefix` (default `snapshot`, the sidecar's
own default) is passed to both the sidecar, as `--snapshot-name-prefix`, and
the driver: it is how the driver recognises the snapshots it took. Set
it there and not in `sidecars.snapshotter.extraArgs`, which the chart refuses,
and do not change it on a release that has snapshots -- theirs would no longer
be recognised.

### Sizing the controller for many volumes

`csi-provisioner` and `csi-resizer` keep every claim and PersistentVolume of
the cluster in memory, so their memory grows with the number of volumes. Each
has its own resources, `controller.provisionerResources` and
`controller.resizerResources`; `controller.sidecarResources` covers the other
sidecars, whose memory does not grow this way.

Measured on OpenShift 4.22, creating and then deleting 5,000 volumes:

| Container | Idle | 5,000 volumes bound | Peak (during the delete) | Default limit |
|---|---|---|---|---|
| `csi-provisioner` | 19Mi | 134Mi | 147Mi | 512Mi |
| `csi-resizer` | 17Mi | 84Mi | 128Mi | 256Mi |
| other sidecars | 12-22Mi | under 30Mi | under 30Mi | 128Mi |

`csi-provisioner` grows by about 25Mi per 1,000 volumes, so the defaults
leave room for about 15,000 volumes. For more, raise both in proportion, for
example for 30,000 volumes:

```yaml
controller:
  provisionerResources:
    requests: {cpu: 5m, memory: 1Gi, ephemeral-storage: 16Mi}
    limits: {memory: 1Gi, ephemeral-storage: 500Mi}
  resizerResources:
    requests: {cpu: 5m, memory: 512Mi, ephemeral-storage: 16Mi}
    limits: {memory: 512Mi, ephemeral-storage: 500Mi}
```

A sidecar that runs out of memory is restarted by the kubelet
(`OOMKilled`): new claims, expansions and deletes wait until it is back, and
mounted volumes are not affected. `kubectl top pod --containers -n <namespace>`
shows each container's use.

## OpenShift

OpenShift Container Platform **4.22** (Kubernetes 1.35, RHCOS 9.8) is
supported, on `x86_64` and `aarch64`, over NFS 4.1 and NFS 3. 4.21 and older
ship Kubernetes 1.34 and are refused by `kubeVersion`.

The install is the same `helm install`. There is no SecurityContextConstraint
step for the operator: the chart detects OpenShift and renders what the node
plugin needs.

```yaml
openshift:
  # auto: on when the cluster serves security.openshift.io/v1
  # SecurityContextConstraints. Offline renders (helm template, GitOps,
  # the golden tests) see no cluster -- set true there.
  enabled: auto
  # The SCC the controller pods (and the helm test pods) are pinned to.
  # Empty: restricted-v2, or hostnetwork-v2 with controller.hostNetwork.
  controllerSCC: ""
  # auto: leave the controller's uid out when controllerSCC starts with
  # "restricted" or "hostnetwork"; true: always (a custom uid-assigning
  # SCC); false: never.
  assignControllerUID: auto
  nodeSCC:
    # false: render no SCC and no grant, and pin the node pods to nodeSCC.name.
    create: true
    # Empty: <namespace>-<fullname>-node, or <fullname>-node when the
    # release is named after its namespace.
    name: ""
  # Trust the cluster's injected CA bundle for the Qumulo API, instead of
  # qumulo.caBundle. Refused together with qumulo.insecureSkipTlsVerify.
  trustedCABundle: false

controller:
  # Arrays reachable only from the nodes' own routes (a storage network the
  # pod network has no path to): run the controller on the node's network.
  # Selects and grants hostnetwork-v2; replicas kept on different nodes.
  hostNetwork: false
  # Pods do not inherit the cluster Proxy object; copy it here if the arrays
  # are only reachable through one.
  proxy:
    httpsProxy: ""
    httpProxy: ""
    noProxy: ""
```

What the chart does:

| Pod | SCC | Why |
|---|---|---|
| Controller | `restricted-v2` | It only speaks HTTPS to the Qumulo REST API. The chart drops its fixed uid 65532, which is outside the per-namespace range, so OpenShift assigns one; `runAsNonRoot` stays. The uid is dropped only for a `restricted*` or `hostnetwork*` SCC. With `controller.hostNetwork` it is `hostnetwork-v2` instead (no privilege, host network only), granted to the controller service account alone. |
| Node | its own, `<namespace>-<fullname>-node` (`<fullname>-node` when the release is named after its namespace: `qumulo-csi-qumulo-csi-driver-node` for the documented install) | It mounts NFS on the host, so it needs privileged, host network, host ports and host paths. The SCC allows that and nothing more -- no host PID, no host IPC, no extra capabilities -- and is granted through RBAC to the node service account alone, not the built-in `privileged` SCC, whose use CIS OpenShift 5.2 asks to keep minimal. |

Both pods carry `openshift.io/required-scc`, so admission never falls through
to another SCC a cluster admin granted for something else.

A policy engine (Gatekeeper, Kyverno) is a separate admission check and flags
the node pod's privileged settings. Each setting, why the node needs it, and a
narrow exemption for each engine are in
[Security: policy engines](../../docs/features/security.md#policy-engines-gatekeeper-kyverno).

`node.healthPort` (default 29653) is a **host** port, because the node
DaemonSet is `hostNetwork: true`. It is also the upstream `csi-driver-nfs`
node health port, so a cluster running both needs a different number.
`node.registrarHealthPort` follows it (+ 6) unless set.

The `charts.openshift.io/*` annotations in `Chart.yaml` are metadata only: no
template reads them, so a rendered install is identical with or without them.
`testedOpenShiftVersion` is maintained by hand and must be updated with each
OpenShift version the chart is validated on.

Full documentation: [Install on OpenShift](../../docs/get-started/openshift.md)
and [Operate on OpenShift](../../docs/operate/openshift-operations.md).

## Upgrading

Pass your values file (`-f` or `--reuse-values`) on every upgrade -- see below
for what a values-less `--set` upgrade would do.

**Some fields cannot change on an existing object, and the chart refuses an
upgrade that would change one.** The API server refuses an update to a
StorageClass's `provisioner`, `reclaimPolicy`, `volumeBindingMode` or
`parameters`, a VolumeReplicationClass's parameters, a PriorityClass's
`value`, and the CSIDriver's `attachRequired` and `volumeLifecycleModes`.
Helm applies every object and collects the errors, so such an upgrade used to
roll the new images first and then report `UPGRADE FAILED`, leaving the release
`failed`. The chart now compares each of these with the live object
before anything is applied and fails the render naming the object, the field,
both values and the remedy: keep the old value, give the class a new name, or
delete the old object with kubectl once nothing uses it (PVs are kept) and
upgrade again. A `driverName` change is refused the same way -- every existing
PV names the old driver -- so a second name needs a second release.
Two classes of one kind with one name are refused at any render. These checks
read the live cluster, so `helm template` and a client-side dry run render as
before; `helm upgrade --dry-run=server` runs them.

With Helm 3, `--atomic` also rolls a release back when an upgrade fails for
any other reason; Helm 4 calls it `--rollback-on-failure`.

**A dedicated-export class with no `allowedClients` is now refused.**
`requireAllowedClients` is on by default, so an install or upgrade fails naming
each dedicated-export class whose exports would admit any NFS client, as root,
with no authentication. For each class it names, either:

- set `allowAnyClient: true` on it in your values, if an open export is really
  intended. The class then carries the parameter `allowAnyClient: "true"`; or
- set `allowedClients` to your nodes' addresses or CIDRs.

Either way the class's `parameters` change, which the API server refuses on an
existing class, so give the class a new name, or delete the old one with
kubectl once nothing uses it (PVs are kept), as for any parameter change above.
This includes a class an earlier chart rendered with `allowAnyClient: true`,
which did not carry the parameter: the driver now checks too (below).

A Kerberos class (`securityFlavor` `krb5`, `krb5i` or `krb5p`) is not open and
is not refused. `requireAllowedClients: false` restores the old behaviour for
every class at once; it is kept for one release, to let an upgrade go through
before the classes are fixed. The same value sets the controller's
`-require-allowed-clients`, so the driver refuses `CreateVolume`
(InvalidArgument, naming the three ways out) on an open class the chart did not
check -- one made with kubectl, or rendered by an earlier chart. Volumes already
provisioned keep the exports they have, and still expand, delete, snapshot and
report capacity: both checks govern only new volumes.

**A CRD the release uses must not be removed while it is installed.** If the
VolumeSnapshotClass or VolumeReplicationClass CRD is removed (csi-addons
uninstalled, or a snapshot-controller upgrade that stops serving the version),
Helm can no longer read the current release's manifest and every upgrade and
rollback fails, a rollback leaving the release `pending-rollback`. The
chart now refuses a render that sets those classes when the cluster does not
serve the version it renders (`snapshot.storage.k8s.io/v1` VolumeSnapshotClass,
`replication.storage.openshift.io/v1alpha1` VolumeReplicationClass), whether the
CRD is missing or no longer serves that version, and names what to install. To
recover a release already wedged: re-install the CRD (serving that version),
`helm rollback` to the last deployed revision, then upgrade. If the feature is
really being removed, that upgrade empties `volumeSnapshotClasses` /
`volumeReplicationClasses` **and sets `allowClassRemoval=true`** (the chart
otherwise refuses an upgrade that drops classes it manages, below); the classes
are kept on the cluster, so delete them with kubectl, and only then remove the
CRD.

**On OpenShift, a release named after its namespace gets a shorter node SCC
name.** The documented `helm install qumulo-csi ... -n qumulo-csi` used to
create `qumulo-csi-qumulo-csi-qumulo-csi-driver-node`; it is now
`qumulo-csi-qumulo-csi-driver-node`. The upgrade creates the new SCC, its
ClusterRole and RoleBinding (both now named `<SCC>-use`), rolls the node pods
onto it once (each node's pod restarts; mounted volumes stay mounted), and then
removes the old SCC and its grant. A node pod may be refused for a few seconds
while the new SCC is created; the DaemonSet retries. Any other release keeps
its SCC name, and an `openshift.nodeSCC.name` you set is used as it is. If a
policy or an audit names the old SCC, update it, or set
`openshift.nodeSCC.name` to the old name to keep it.

**Resource defaults now include limits.** Every container has a memory limit
equal to its memory request and a 500Mi ephemeral-storage limit; there is still
no CPU limit. The memory requests are larger than before, so the pods reserve
more: per controller replica 1Gi for the driver and 128Mi per sidecar, per node
256Mi for the driver and 64Mi each for the registrar and liveness probe. An
upgrade with your own `controller.resources`, `controller.sidecarResources` or
`node.resources` keeps yours. A `--reuse-values` upgrade keeps the old
requests-only values; pass `-f values.yaml` to take the new defaults.

**`csi-provisioner` and `csi-resizer` have their own resources.** They used
`controller.sidecarResources` (128Mi) like every other sidecar, and with 5,000
volumes `csi-provisioner` was OOM-killed and `csi-resizer` reached the limit.
They now default to 512Mi and 256Mi, from `controller.provisionerResources` and
`controller.resizerResources`; see [Sizing the controller for many
volumes](#sizing-the-controller-for-many-volumes). A `controller.sidecarResources`
you set no longer reaches them: set those two as well if you sized them through
it. Each controller replica reserves 512Mi more than before.

**`helm test` changed.** The readiness test is new and always on. The volume
test runs only when `tests.storageClass` names a class; it no longer falls back
to the first entry of `storageClasses`.

**The node-driver-registrar serves `/healthz` on `node.registrarHealthPort`, a
host port on the node's address,** which its liveness and readiness probes
read. Its default, `0`, is `node.healthPort` + 6: 29659 with the default
29653. A second release of the chart on the same cluster already needs its
own `node.healthPort`, so it gets its own registrar port with no new setting.
The install or upgrade is refused, naming the DaemonSet, when another
DaemonSet on the cluster already holds one of this release's node host ports
(`node.healthPort`, `node.registrarHealthPort`, `metrics.port.node`), instead
of leaving the node pods Pending with "didn't have free ports".

If a second release's upgrade to a chart with this check already failed this way,
roll it back to its last good revision first (`helm history`, then
`helm rollback <release> <revision>`), then upgrade. Helm diffs a failed
upgrade from the last successful revision, so upgrading straight over it is
refused with `Duplicate value: "registrar"`. Until it is rolled back its
Pending node pods keep asking for 29659 and can take that port on a node
where the first release's node pod is being replaced.

### Upgrading to this release: inputs now refused

This release refuses, or answers differently, a number of inputs earlier
releases accepted -- most of them mistakes that used to be resolved silently
one way or the other. A class, claim or snapshot that relied on one of them
fails after the upgrade, so check for them first. Each refusal is
`InvalidArgument` naming what is wrong unless it says otherwise.

**VolumeSnapshotClass parameters.**

- Any key other than `apiurl` and the `csi.storage.k8s.io/*` keys the sidecar
  reads is refused at `CreateSnapshot`. The key is spelled exactly `apiurl`
  here, all lower case -- `apiUrl`, which a StorageClass accepts, used to be
  ignored on a VolumeSnapshotClass and is now refused.
- `apiurl` must be an `https://` URL naming a host. An `http://` one sent the
  class's API token in cleartext.

**StorageClass parameters.**

- Two keys that differ only by case or surrounding space (`server` beside
  `Server`) are refused: only one could take effect, and which one was not
  defined.
- `storeRealPath` and `storeExportPath` with a `.` or `..` element are
  refused; write the path it resolves to.
- `snapshotSchedule`: a `tz=` the IANA database does not name is refused, and
  so is `tz=Local` (the controller's own zone, which the cluster cannot be
  told). Zone names are case-sensitive -- `tz=UTC`, not `tz=utc`; the
  refusal suggests the right spelling when only the case is wrong. A
  repeated `keep=` or `tz=` is refused rather than letting the later one win.
  A day name or `last`/`never` spelled with a non-ASCII letter is refused.
- **Chart values schema.** `helm install` and `helm upgrade` now refuse, at
  render, a `snapshotSchedule` the driver cannot parse (the pattern admits
  exactly what the driver parses, spaces, one-digit hours and a trailing
  comma included), an `allowedClients` entry that is a host name or a
  malformed CIDR, and a `tenantID` below 1 (`0` or negative). An empty
  `tenantID: ""` is the same as leaving it out.
- **Chart: unknown `snapshotSchedule` zones.** `helm install` and
  `helm upgrade` now refuse a `tz=` the driver's IANA zone database does not
  have -- a made-up name, a wrong case (`utc`, suggesting `UTC`) or a zone
  the database has since dropped (`US/Pacific-New`) -- instead of installing
  a class whose every claim then stays Pending. A release that already has
  such a class fails `helm upgrade` until the zone is corrected; no claim can
  have been provisioned on that class, and its parameters cannot change in
  place, so delete it (`kubectl delete sc <name>`) and upgrade again.

**Provisioning.**

- A `CreateVolume` retry whose class was edited between attempts, so that
  its `allowedClients`, `securityFlavor` or `tenantID` differ from the export
  the first attempt made, answers `ALREADY_EXISTS` naming the difference,
  instead of binding a PV whose export does not enforce what the class now
  says. Delete the claim, or put the class back.

**Snapshots.**

- The `ListSnapshots` paging token is now a snapshot ID, not an offset, so a
  delete between pages no longer skips entries. A token handed out by an
  earlier release resumes at the wrong place; start the listing again.

**Mounting.**

- A version shorthand in `mountOptions` (`v3`, `v4`, `v4.1`, `v4.2`) that
  contradicts the class's `protocol` is refused, as `vers=` and `nfsvers=`
  always were. One that matches is accepted.
- A `sec=` in `mountOptions` that contradicts a `securityFlavor` the class
  sets is refused. With no `securityFlavor`, a `sec=` is accepted with a
  warning in the node log.
- A publish target that is a symlink (dangling or not), that exists and is
  not a directory, that is relative, or that has a `.` or `..` element is
  refused, as are NUL, newline and carriage return in a target path or a
  volume handle. The kubelet never sends these; a hand-driven CSI client
  might.

**Check before upgrading.**

```sh
# VolumeSnapshotClasses of this driver: any parameter key but apiurl and
# csi.storage.k8s.io/*, and any apiurl that is not https://
kubectl get volumesnapshotclass \
  -o custom-columns='NAME:.metadata.name,DRIVER:.driver,PARAMETERS:.parameters'

# StorageClasses of this driver: keys differing only by case, store paths with.
# or .., snapshotSchedule zones (case, Local), repeated keep=/tz=, and
# mountOptions carrying v3/v4/v4.1/v4.2 or sec=
kubectl get sc \
  -o custom-columns='NAME:.metadata.name,PROVISIONER:.provisioner,PARAMETERS:.parameters,MOUNT:.mountOptions'

# The full objects, if a column is cut short
kubectl get volumesnapshotclass -o yaml
kubectl get sc -o yaml

# Then render the new chart against your values: the values schema refuses a
# bad snapshotSchedule, allowedClients or tenantID here, before anything changes
helm upgrade <release> oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver \
  --version 0.1.2 -f your-values.yaml --dry-run=server
```

A StorageClass's parameters and mount options cannot be changed in place, so
a class that needs fixing is given a new name, or deleted with kubectl once
nothing uses it (PVs are kept) and created again, as for any parameter change
above.

## Uninstalling and reinstalling

Operators uninstall and reinstall this chart far more often than they upgrade
it -- a node pool rotation, a namespace rebuild, a Helm values change that
needs `--set` flags a plain `upgrade` cannot add cleanly. What `helm
uninstall` actually removes is the table above: the controller Deployment,
the node DaemonSet, the `CSIDriver` object, RBAC, and the node PriorityClass
this chart renders. It does **not** remove
the StorageClasses, VolumeSnapshotClasses and VolumeReplicationClasses this
chart rendered: they carry `helm.sh/resource-policy: keep`, so Helm
leaves them on uninstall and on an upgrade that stops rendering them. A
reinstall under the **same release name and namespace** takes them back over;
under another name it is refused until you delete them with kubectl. Nor does
it remove PVs, PVCs, or the credential Secrets a StorageClass names -- none of
those are chart-owned, so none of them are Helm's to delete.

**Before cleaning a node by hand, check `<kubeletDir>/<driver name>-orphaned/`**
(by default `/var/lib/kubelet/csi.qumulo.com-orphaned/`). It is normally empty.
When it is not, it holds **a pod's only copy of data** that reached the node's
own disk after its NFS mount went away -- the node plugin moves it there so the
pod can finish terminating, logs both paths at ERROR, and deletes nothing. Nothing ever removes it, and no Qumulo object or metric counts it. It
sits beside the plugin directory rather than inside it precisely so that
removing `<kubeletDir>/plugins/<driver name>/` after an uninstall cannot delete
it -- but `rm -rf` aimed higher will. Copy anything needed back to the volume
before removing it.

**Pass your values file on every upgrade** (`-f <values>` or
`--reuse-values`). `helm upgrade --set <one value>` alone renders the chart's
defaults, which hold no classes and reset every image and sidecar setting.
The chart refuses an upgrade that would render none of the classes the release
manages and names them. To remove classes deliberately, set
`allowClassRemoval=true`; they stay on the cluster for you to delete once
nothing uses them.

That split in ownership is what determines behavior during and after the gap:

- **A pod with an already-mounted volume keeps working, uninstalled or not.**
  The NFS mount is a real mount in the node's kernel, established once by
  `mount(8)` when `NodePublishVolume` ran. It does not belong to the driver
  container and is not torn down when that container -- or the whole
  DaemonSet -- goes away. `TestE2ENodeRestartWithMountedVolume` in
  `test/e2e/e2e_test.go` already proves the weaker case (a node pod restart);
  removing the DaemonSet entirely relies on the identical mechanism.
- **A new PVC stays `Pending` for the whole gap.** With the controller
  Deployment gone, `csi-provisioner` is not running to watch it. The
  StorageClass stays, so the claim is provisioned once the controller is
  back.
- **A PV mid-delete gets stuck, not lost.** If a PVC with `reclaimPolicy:
  Delete` is removed while the controller is down, `DeleteVolume` cannot run,
  so the PV sits `Terminating` behind its finalizer. It resolves itself once
  the controller comes back: `external-provisioner` retries the delete on its
  own schedule, with no operator action needed.
- **Reinstalling with the same values resumes service, not a fresh start.**
  `helm upgrade --install` with an unchanged `values.yaml` recreates objects
  with the same names, `csi-provisioner` immediately picks up whatever PVCs
  went `Pending` during the gap, and `node-driver-registrar` re-registers the
  plugin with kubelet on every node -- new pods needing this driver's volumes
  work again as soon as that registration completes, with no action needed on
  volumes that were already mounted.

## StorageClass parameters

**An unrecognised key in a class fails the install.** `storageClasses[]` and
`volumeSnapshotClasses[]` are closed against unknown properties, so a typo such
as `protocl: nfs4.1` or `baseePath:` stops the install and names the offending
key. It used to be dropped in silence: Helm exited 0 and rendered a class
without the setting, so `kubectl get sc` showed something the operator had not
written and nothing anywhere reported it. A key removed in an earlier release
fails the same way, which is what makes an old values file safe to reuse -- it
tells you rather than quietly ignoring the difference.

The table below is therefore the complete list of what a class accepts.

| Values key | Rendered parameter | Notes |
|---|---|---|
| `basePath` | `storeRealPath` | required, absolute |
| `exportBasePath` | `storeExportPath` | defaults to `basePath` |
| `server` | `server` | **required**, no default. Bare hostname or IPv4 -- not a URL, no port. Set it to a floating IP when NFS traffic uses one |
| `protocol` | `protocol` | `nfs3` \| `nfs4.1`; empty lets the kernel negotiate |
| `provisioningModel` | `provisioningModel` | `shared-subdirectory` (default) \| `dedicated-export` |
| `mountPermissions` | `mountPermissions` | octal mode for the volume directory, default `0777`; `inherit` keeps the cluster default |
| `apiUrl` | `apiUrl` | REST endpoint for this class's cluster; defaults to `https://<server>:8000`. Set it for a non-default port or a proxy. **`https://` only**: every call carries the API token, so `http://` is refused here, in `targetApiUrl`, and in any volume or snapshot handle |
| `snapshotSchedule` | `snapshotSchedule` | attaches a Qumulo snapshot policy to every volume the class provisions — see [Scheduled snapshots](#scheduled-snapshots) |
| `allowForeignSnapshotRestore` | `allowForeignSnapshotRestore` | `true` lets the class restore a snapshot that is not named as this driver's are and whose source directory is gone; refused otherwise. Default off, rendered only when `true` — see [Backup tools](#backup-tools-velero-and-similar) |
| `adDomain` | `adDomain` | nests this class's volumes under a directory named for an AD domain. **Layout only, not access control** — see [adDomain](#addomain) |
| `tenantID` | `tenantID` | attaches this class's exports to a Qumulo network tenant, by id or name. `dedicated-export` only — see [Multi-tenant exports](#multi-tenant-exports-and-ip-restrictions) |
| `allowedClients` | `allowedClients` | list of IPs/CIDRs permitted to mount. `dedicated-export` only |
| `securityFlavor` | `securityFlavor` | `sys` (default behaviour) \| `krb5` \| `krb5i` \| `krb5p`. Makes the export *require* that flavour and the node mount with it. `dedicated-export` only. The chart refuses `protocol: nfs3`; an unset `protocol` is accepted (the mount negotiates NFSv4.1) — see [Kerberos](#kerberos) |
| `secretName`, `secretNamespace` | `csi.storage.k8s.io/provisioner-secret-*` and `controller-expand-secret-*` | **`secretName` required**, no default: the Secret holding this cluster's token, under key `token`. `secretNamespace` defaults to the release namespace |
| `reclaimPolicy`, `allowVolumeExpansion`, `mountOptions`, `isDefault` | native StorageClass fields | |

**Why `mountPermissions` defaults to `0777`.** The driver creates a volume
directory through the REST API with the cluster's default mode `0755`, and gives
it NFS uid 0 and gid 0 so that replication accepts it (see
[Required Qumulo API privileges](#required-qumulo-api-privileges)). An NFS client
writes as its own uid and gid: an export this driver
creates maps no identity (`NFS_MAP_NONE`), root included -- see
[Multi-tenant exports and IP restrictions](#multi-tenant-exports-and-ip-restrictions)
-- so a non-root pod is neither the owner nor in the owning group, and every
write fails after the mount has already succeeded. Nothing
narrower can be chosen without knowing the workload's uid, which a StorageClass
does not. Set `inherit` if your export maps identities so ownership already
lines up.

### Non-root pods, `subPath`, and `fsGroup`

**If you tighten `mountPermissions`, read this before you do.** The default
`0777` is what makes the common case work, and it is doing more of the work
than it looks like.

Kubernetes has one mechanism for making a volume writable by a pod that is not
root: the kubelet applies the pod's `fsGroup` to the volume at mount time. This
chart sets `fsGroupPolicy: None`, so **it never does that here**. The reason is
in `values.yaml`: `File` has the kubelet walk the entire volume applying group
ownership on every mount, which on a shared NFS tree is both slow and wrong —
other pods hold the same export, and their files would be re-owned underneath
them.

That leaves the volume's own mode as the only thing deciding whether a non-root
pod can write, and one detail makes it sharper than it sounds: **the kubelet
creates `subPath` directories as root**, mode `0755`. So a pod running as uid
1001 with a `subPath` gets a directory it does not own, inside a volume whose
permissions it also does not control.

On a default `0777` volume this works — the pod can create its own `subPath`
and write in it. On `mountPermissions: 0755`, or `inherit` under a restrictive
parent, it does not: the pod gets `EACCES` on its own directory, after the
volume mounted successfully and the pod started.

Two ways to fix it, in the order worth trying.

**1. An `initContainer` that chowns the path.** Recommended, because it changes
one directory rather than a whole shared tree, and needs no chart-wide setting.
The init container runs as root, the app container does not:

```yaml
spec:
  containers:
    - name: app
      image: myapp:1.0
      securityContext: {runAsUser: 1001, runAsGroup: 1001}
      volumeMounts:
        - {name: data, mountPath: /data, subPath: user-1001}
  initContainers:
    - name: fix-ownership
      image: busybox:1.36
      # Runs as root on purpose: it exists to hand the directory to the uid the
      # app container runs as, and nothing else can.
      securityContext: {runAsUser: 0}
      command: ["sh", "-c", "chown 1001:1001 /data && chmod 0700 /data"]
      volumeMounts:
        - {name: data, mountPath: /data, subPath: user-1001}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: home-dirs}
```

Note both containers mount the **same `subPath`**. Without that, the init
container chowns the volume root instead of the pod's own directory, which is
the shared-tree problem again.

**2. `fsGroupPolicy: File`.** A chart-wide value, so it applies to every volume
this driver serves — appropriate only when your volumes are effectively
single-writer, and never on a tree several workloads share. Set `fsGroup` on
the pod and the kubelet applies it at mount:

```yaml
# values.yaml
fsGroupPolicy: File
```

It is a mutable field on the CSIDriver object, so `helm upgrade` can change it
on a running release without reinstalling.

**Know what it costs before turning it on.** The driver does not
advertise `VOLUME_MOUNT_GROUP`, so the kubelet applies `fsGroup` itself: it
walks the whole volume and sets group and mode on every entry -- two NFS
SETATTRs per file, with no timeout -- and does so again on every pod start,
because each pod has its own mount target. Measured in a kernel lab, 5,051
entries took 10,102 SETATTRs, 4.17 ms an entry with 0.5 ms of added latency;
a volume of a million files would take roughly 20 to 70 minutes per pod start,
past the kubelet's two-minute mount wait. The exports this driver makes squash
no root (see [Multi-tenant exports](#multi-tenant-exports-and-ip-restrictions)),
so every one of those calls really rewrites ownership, and on a volume shared
by pods with different `fsGroup` values each start re-owns the others' files.
Set `fsGroupChangePolicy: OnRootMismatch` in the pod's `securityContext` so
the walk runs only when the volume root's ownership does not already match,
and keep `File` off for any volume more than one workload mounts.

**What not to do:** do not reach for `mountPermissions: 0777` to get around a
`subPath` permission error if you had a reason to tighten it. The volume root
being world-writable is what the default trades away, and the two mechanisms
above get the same result without it.

`dedicated-export` gives every volume its own NFS export. That is one entry in
the cluster's export table per volume, where the shared model adds none — worth
knowing before using it at scale.

**`nolock` is an NFSv3 requirement, not an NFS one, and you no longer have to
remember it.** The node image ships `mount.nfs` but runs no `rpc.statd`, so an
`nfs3` mount without `nolock` fails outright. It is now supplied for you from
two directions, because neither alone is enough:

* An `nfs3` class that sets no `mountOptions` renders
  `[hard, timeo=600, nolock]` -- the default is protocol-aware.
* An `nfs3` class that sets `mountOptions` explicitly is rendered verbatim, so
  it may not contain `nolock`; the node adds it at mount time. This is also
  what fixes volumes provisioned before this behaviour existed, whose
  `mountOptions` are frozen in the PV.
* A class with no `protocol` whose `mountOptions` pin `vers=3` or
  `nfsvers=3` is NFSv3 too, and gets `nolock` the same way. With no
  version pinned anywhere the node adds nothing: the kernel negotiates.

Either way `/proc/mounts` shows `nolock`, and the node logs when it added one.
An `nfs3` class that explicitly asks for `lock` is *refused* rather than
overridden -- at `helm install` and again at mount time -- because server-side
v3 locking cannot work on this node and silently inverting the request would
hide the mistake. Setting `nolock` or `local_lock=` yourself is respected as-is.

What `nolock` costs is real: locks become node-local (`local_lock=all`), so no
two pods on different nodes can safely share a file lock over `nfs3`. See the
protocol table in [docs/reference/supported-features.md](../../docs/reference/supported-features.md).

`nfs4.1` locks in-protocol and needs nothing: passing `nolock` on a v4.1 class
is accepted and then ignored (`local_lock=none` either way), so the driver adds
nothing there. v4.1 also negotiates `sec=sys` unasked, even
against a cluster with Kerberos enabled, so no `sec=` option is required --
**unless the class sets `securityFlavor`**, which is exactly why that parameter
has to drive both halves. See [Kerberos](#kerberos).

**`shared-subdirectory` needs `basePath` covered by an export, and the driver
now checks.** A shared-model volume is a directory beneath an export somebody
else owns, so the driver creates no export of its own -- it needs one that
already reaches the volume's path. Create an export whose NFS path is
`exportBasePath` (or an ancestor of it) pointing at `basePath`, before the
first PVC.

If none exists, `CreateVolume` fails with `FailedPrecondition` naming the path
and nothing is created on the cluster. It used to succeed: the PVC went
`Bound`, and the pod then failed forever with `mount.nfs4: ... No such file or
directory`, which showed a healthy volume next to a broken workload with
nothing connecting them. The same check also catches an `exportBasePath` and
`basePath` that have drifted out of step -- an export that reaches the path but
maps it to a different directory -- which produced the identical symptom for a
different reason.

The driver verifies rather than creates. An export at `basePath` is a
cluster-wide grant, it collides with the NFSv4 rule below, and removing a
volume would then have to reason about an export other volumes still sit under.

**`dedicated-export` needs an unexported `exportBasePath` when NFSv4 is on.**
NFSv4 presents a single pseudo-filesystem tree, so a v4-enabled cluster refuses
to create an export nested inside an existing one. If any ancestor of
`exportBasePath` is already exported -- and a cluster exporting `/` always has
one -- every `dedicated-export` provision fails with `FailedPrecondition` naming
the ancestor. The same nesting is permitted while v4 is disabled, so this can
appear as a regression the moment NFSv4 is switched on.

The consequence for planning: under NFSv4 the two provisioning models want
**opposite** things. `shared-subdirectory` needs the parent exported;
`dedicated-export` needs it unexported. They cannot both serve one `basePath`.
Give each model its own subtree.

**`protocol: nfs4.1` is checked against the cluster before provisioning.** If the
cluster reports `v4_enabled: false`, the PVC fails with `InvalidArgument` naming
that setting instead of binding and then wedging a pod at mount time. The result
is cached per cluster for a few minutes. If the array answers but the setting
cannot be read (a 403, say), the volume is allowed rather than blocked. If the
array does not answer at all, `CreateVolume` returns `Unavailable` and the
provisioner retries.

**An array that does not answer ends `CreateVolume` with `Unavailable`:** a
call that gets no answer at all (a timeout, a refused connection) stops the
attempt there, and the provisioner retries; the retry adopts whatever the attempt left. Three
transport failures no retry can fix are final instead, `InvalidArgument` naming
what to correct: a REST endpoint whose name does not resolve (NXDOMAIN, not a
DNS timeout), one whose TLS certificate does not verify, and, behind
`controller.proxy`, a proxy that refuses the tunnel to it (`403`, or `407` for
credentials the driver does not send). A proxy answering `502`, `503` or `504`
is retried like an array that is down. There is no
fallback credential or cluster to try instead. A volume name containing `#`,
`/` or NUL, or one that is `.` or `..`, is refused with `InvalidArgument`;
only a direct CSI caller can send one, since Kubernetes names volumes
`pvc-<uid>`.

Set `protocol` **or** an `nfsvers=` mount option, never both. The chart refuses
the combination at install time; otherwise the driver refuses it at mount time,
by which point a pod is already stuck.

### Choosing mount options for planned maintenance

A class that sets no `mountOptions` renders `[hard, timeo=600]` -- this is the
chart's default, not just a `values.yaml` comment, so leaving the key out is
itself a choice with the following behaviour:

* **`hard`** (the default): during a Qumulo rolling upgrade or a node
  evacuation, in-flight I/O pauses and retries every `timeo` tenths of a
  second, and resumes transparently once the array answers again. A write
  can block for as long as the array is unreachable. It is not aborted by the
  driver either: the unmount ladder runs `umount -f`, which aborts in-flight
  requests on the whole NFS superblock, only for a mount that is the last on
  its superblock. Mounts from the same array address with the same options
  share one, so a pod deleted during an outage is released with `umount -l`
  instead, and the other pods' writes on that array carry on. The one
  mount not protected is a mount of the same array made on the host outside
  the kubelet's pods directory, which the node plugin cannot see.
* **`soft`**: the same outage returns `EIO` to the application after `retrans`
  retries instead of blocking. Only choose this for a workload that already
  handles I/O errors and retries at the application layer -- most do not, and
  `soft` on NFS has a long history of silent data loss on write.

Setting `mountOptions` explicitly is respected verbatim, so
`mountOptions: [soft, timeo=600]` opts all the way out of the default.
`timeo=600` (60 seconds) and the kernel's default `retrans=2` are the values
validation ran this release against; a cluster with a longer expected upgrade
window may want a larger `timeo`.

**Two consequences of "verbatim" worth planning around.** An explicit list
replaces the default rather than adding to it, so a class that sets
`mountOptions` for one reason gives up the rest -- `mountOptions: [nolock]` on
an `nfs3` class is a working mount with no `timeo=600`, which is probably not
what was intended. Write the full list you want:
`[hard, timeo=600, nolock]`.

The exception is `nolock` itself. The node adds it to any `nfs3` mount that
does not already decide locking, so an explicit list that forgets it still
mounts -- see the `nolock` note under
[StorageClass parameters](#storageclass-parameters). That happens at mount
time, not render time, so the PV's own `mountOptions` stay exactly what you
wrote.

### Read-only volumes

A class with `ro` in `mountOptions` -- one entry or comma-joined, `[hard, ro]`
and `["hard,ro"]` are the same -- or a PV with `readOnly: true` is mounted
read-only on the node. That alone is less than it looks:

* The pod does not use the node's mount. The container runtime binds it into
  the container, and that bind need not keep the read-only flag: runc 1.4.3
  (containerd 2.3.4) was measured giving the pod a bind that reads `rw`. What
  then refuses writes is the NFS superblock under it, which `ro` also makes
  read-only.
* The node plugin checks that superblock after every read-only mount
  (`/proc/self/mountinfo`; `/proc/mounts` cannot tell the mount's flag from
  the superblock's). If the kernel left either read-write, the publish is
  refused with `FailedPrecondition` naming both option lists, and the mount
  is taken away, so the pod does not start on a writable volume. A republish
  of a mount already in use is refused the same way but left mounted.
  The message asks for the node's `uname -r`: one lab's kernel was seen to do
  this, and why is not yet known.

**For a volume that must not be written, also set `readOnly: true` on the
pod's `volumeMount`.** That makes the kubelet ask the runtime for a read-only
bind, which holds whatever the superblock does:

```yaml
containers:
- name: app
  volumeMounts:
  - name: data
    mountPath: /data
    readOnly: true
```

### One connection per array address, per node

The Linux NFS client keeps one client, and one TCP connection, per server
address, NFS version and transport. So every pod on a node that reaches an
array through one address shares one TCP flow and one slot table: a heavy
writer delays everyone's metadata on that node. Measured in a kernel
lab: 40 NFSv4.1 mounts used one connection, and 40 NFSv3 mounts added one more.

* **`nconnect` takes effect only on the first mount to an address.** A later
  mount to the same address -- another class, another option -- joins the
  existing connection and its `nconnect` is ignored, silently. Set it on every
  class that mounts from an address, so whichever mounts first sets it.
* **To spread load, spread addresses.** Classes that name different floating
  addresses of the array get separate connections.
* The driver adds no `nconnect` itself. NFSv3 mounts here negotiate
  `mountproto=udp`; forcing `mountproto=tcp` leaves a reserved-port socket in
  `TIME_WAIT` per mount.

## TLS

`insecureSkipTlsVerify: false` (the default) needs two things, and a **stock**
Qumulo certificate satisfies neither — it is `CN=localhost` with no SAN, and Go
has ignored `CN` since 1.15.

1. **A cluster certificate whose SAN matches what the driver dials.** Generate
   with `openssl`, install with `qq ssl_modify_certificate -c chain.pem -k
   cluster.key` — that command installs, it does not generate. Cover every node
   address: any node may serve the Web UI and REST.
2. **Its CA trusted by the controller.** A public CA needs nothing. A private or
   self-signed CA needs `qumulo.caBundle`:

```sh
kubectl -n qumulo-csi create configmap qumulo-ca --from-file=ca.crt=ca.crt
```

```yaml
qumulo:
  insecureSkipTlsVerify: false
  caBundle:
    existingConfigMap: qumulo-ca
    key: ca.crt              # default
```

The chart mounts that ConfigMap on the controller and sets `SSL_CERT_FILE`,
which Go's `crypto/x509` reads in place of the image's own bundle. The driver
needs no CA flag. **A self-signed CA is enough** — a public CA is not required.

Three things to know:

- **Controller only.** The node plugin performs NFS mounts and never calls the
  REST API, so it gets no CA mount.
- **Process-wide.** One bundle serves every cluster the release talks to. A
  bundle may hold several CAs, so several clusters can be verified; what cannot
  be expressed is verifying one cluster and skipping another.
- **A changed CA needs a controller restart.** `SSL_CERT_FILE` is read once per
  process.

Setting `caBundle` together with `insecureSkipTlsVerify: true` is refused at
install time: it supplies a CA and then ignores it.

TLS covers the REST API only. NFS data traffic is unencrypted either way.

## Serving two clusters from one release

One Secret per cluster, and one class per cluster naming it. The sidecars read
the Secret a class names and pass its contents into every RPC for that class, so
no cluster's credential is shared with another.

```yaml
storageClasses:
  - name: qumulo-a
    server: cluster-a.example.com
    secretName: qumulo-cluster-a
    basePath: /csi/a
    protocol: nfs3
    # The full list: an explicit mountOptions replaces the chart's
    # [hard, timeo=600] default rather than adding to it. nolock is optional
    # here -- the node adds it to any nfs3 mount -- but spelling it out keeps
    # the PV honest about what it mounts with.
    mountOptions: [hard, "timeo=600", nolock]
  - name: qumulo-b
    server: cluster-b.example.com
    secretName: qumulo-cluster-b
    basePath: /csi/b
    apiUrl: https://cluster-b.example.com:9000   # optional: default is https://<server>:8000
    protocol: nfs4.1
```

Both classes name a `server` and a `secretName` because both are required:
there is nothing chart-wide for either to fall back to.

Create each Secret with the token under the key `token`:

```sh
kubectl -n qumulo-csi create secret generic qumulo-cluster-b \
  --from-file=token=/path/to/cluster-b-token
```

Two notes. The controller's ServiceAccount may `get` exactly the Secrets this
release's classes name -- one Role per namespace, limited by name -- and no
others. A Secret named only outside the chart, such as the deletion
Secret a static PV's or an imported VolumeSnapshotContent's annotations name,
goes in `rbac.extraSecrets`; a class created outside Helm needs its Secret
listed there too, or `rbac.clusterWideSecrets: true`, the old cluster-wide
`get`. The namespace of each Secret must exist when the chart is installed,
because the Role lives in it. And TLS verification is still
process-wide: `insecureSkipTlsVerify` applies to every cluster the release talks
to, so clusters cannot be verified selectively.

## Snapshots

This section is about *Kubernetes snapshots* (`VolumeSnapshot`). A class's
`snapshotSchedule` takes *scheduled snapshots*, which Kubernetes never sees --
see [Scheduled snapshots](#scheduled-snapshots). Both are Qumulo snapshots on
the array; the four kinds, their names there and how to tell them apart:
[Snapshot a volume](../../docs/protect/snapshot.md).

Taking, deleting and **restoring** from snapshots all work. Two things are the
operator's to arrange.

**The cluster needs the snapshot machinery first.** The CRDs and
`snapshot-controller` are shared by every CSI driver, installed once by an
administrator, and are not this chart's to own:

```sh
kubectl get crd | grep snapshot          # expect three CRDs
kubectl get pods -A | grep snapshot-controller
```

**Then enable it and declare a class:**

```yaml
sidecars:
  snapshotter:
    enabled: true

volumeSnapshotClasses:
  - name: qumulo-snapshots
    deletionPolicy: Delete        # Delete | Retain
    secretName: qumulo-cluster-a  # required: snapshot RPCs carry their own secret
```

Enabling the sidecar also grants the controller `get`/`list` on
`volumesnapshots` and read/patch on `volumesnapshotcontents` -- `csi-provisioner`
resolves a PVC's `dataSource` itself, so a restore fails without it.

A VolumeSnapshotClass may set `apiurl` to take snapshots through another REST
endpoint of the volume's own cluster; the snapshot ID then records that
endpoint, so a later delete of that snapshot reaches it too. **Restore is
not covered by that recording:** `CreateVolume` from the snapshot dials the
endpoint of the *StorageClass* the new PVC names, not the endpoint in the
snapshot ID, so restoring through the override needs a StorageClass whose own
`apiUrl` is that same override. A restore through a class without it reaches
the volume's default endpoint and fails to find the snapshot. The cluster-UUID
comparison below is skipped whenever it cannot be made -- not only when the
volume's own endpoint is down, but equally when the override endpoint does not
answer or fails, when either endpoint reports no cluster UUID, and when either
call runs past the RPC's deadline.

It must be the **same** cluster: the driver compares the two endpoints'
cluster UUIDs (`GET /v1/node/state`, which needs no privilege) and refuses a
snapshot through a different cluster with `InvalidArgument`, since that
cluster would snapshot whatever it holds at the volume's path.

The check is skipped -- and the snapshot taken anyway, with a warning in the
controller log -- in every case where the comparison cannot be made,
not only when the volume's own endpoint is down. All of them:

* the volume's own endpoint does not answer, or answers an error;
* the override endpoint does not answer, or answers an error;
* either endpoint answers `GET /v1/node/state` without a cluster UUID field;
* either call exceeds the RPC's remaining deadline.

In each case the driver cannot tell the two clusters apart, and chooses to
take the snapshot rather than refuse it.

### What a restore costs

Qumulo has no tree-clone API, so a restore recreates the tree and copies each
file server-side. The bytes never pass through the driver, but every entry
costs several round trips: a create, the copy (one or more `copy-chunk` calls
for a non-empty file), and a PATCH that sets the snapshot's mode, owner, group
and mtime. A directory gets its attributes after its children.

| | |
|---|---|
| bounded at | 50,000 entries (files and directories) / 2 TiB, then `ResourceExhausted`, decided from the snapshot's own counts before anything is copied |
| large trees | may need several provisioning attempts; the PVC stays `Pending` and each attempt resumes where the last stopped. The controller remembers which staged directories are finished, in memory, so a restart of the controller costs one attempt that re-reads the finished part |
| if that is slow | raise `sidecars.provisioner.timeout` -- the per-attempt budget derives from it |
| claim too small | `OutOfRange` naming the bytes needed, decided from the snapshot's own size before anything is staged |
| not copied | symlinks and device nodes (skipped, one warning each); ACLs beyond the mode; extended attributes; named streams. Hard links are restored as links |
| owner not changeable | a token the array will not let set mode, owner, group or mtime still restores; those entries keep the attributes the create gave them, and one warning counts them |

A restored volume never appears partially: it is assembled under a hidden
staging name and renamed into place as the final step.

### Backup tools (Velero and similar)

A backup tool that restores by creating a pre-provisioned
`VolumeSnapshotContent` with an existing `snapshotHandle` -- Velero's model,
among others -- works against this driver. `CreateVolume`'s restore path
resolves a snapshot by ID against the Qumulo API and does not care whether
this driver's own `CreateSnapshot` RPC produced it, so an externally taken or
externally imported snapshot restores the same way a CSI-driven one does.
Verified against a real cluster: a snapshot taken directly against the Qumulo
API (bypassing this driver's `CreateSnapshot` entirely) restored correctly
from a hand-built handle -- see `TestLiveRestoreFromAnExternallyTakenSnapshot`
in `test/live/restore_test.go`.

Build the handle as `<server>##<snapshotID>`: the StorageClass's `server`, two
`#`, and the snapshot's numeric ID on the array. If the class sets `apiUrl`,
put it between the two `#`: `<server>#<apiUrl>#<snapshotID>`. The earlier
`<server>/<snapshotID>` form is still read, as `<server>##<snapshotID>`, so an
import built from an older runbook works.

The handle's `server` and endpoint must be those of the StorageClass the
restoring PVC uses: the same `server` (case aside), and the same REST endpoint
-- its `apiUrl`, or `https://<server>:8000` when the field is empty; case and
a trailing `/` do not count. A mismatch is refused with `InvalidArgument`,
because snapshot IDs are per-array numbers: two arrays behind one name number
their snapshots independently, and two addresses of one array cannot be proven
to be one array. A handle with no endpoint -- `<server>##<snapshotID>`
or the older `<server>/<snapshotID>` -- means `https://<server>:8000`, so a
class with another `apiUrl` refuses it, and the message gives the handle to
use instead. A handle that does not parse is `NotFound`.

**Give the content its deletion credential.** The driver keeps no credential
of its own: every call authenticates with the Secret its request carries. For
a `VolumeSnapshotContent` the external-snapshotter created, that is the
VolumeSnapshotClass's `csi.storage.k8s.io/snapshotter-secret-*` parameters;
for a pre-provisioned one it is the two annotations below, and nothing else.
Without them `DeleteSnapshot` arrives with no token and is refused
(`InvalidArgument: no credential for cluster ...`) every few minutes, so with
`deletionPolicy: Delete` the content stays `Terminating` and the snapshot stays
on the array. Adding the annotations later is enough: the next retry
deletes it. The prefix is `snapshot.storage.kubernetes.io/`, the one the
external-snapshotter reads; `snapshot.storage.k8s.io/` is ignored, and the
content then stays `Terminating` exactly as with no annotation.

**An imported snapshot -- one this driver did not take -- is not deleted
unless the deletion Secret says so.** The array records nothing about who took a snapshot except
its name, so the driver goes by that: `csi-snapshotter` names every snapshot
`<prefix>-<VolumeSnapshot UID>` (the prefix is
`sidecars.snapshotter.snapshotNamePrefix`, default `snapshot`), and the array
stores it as `<id>_<prefix>-<uid>`. `DeleteSnapshot` deletes a snapshot only
when its name has that shape **and**, while its source directory still
exists, the source is one of this driver's volumes (it carries the driver's
export, or a quota). Anything else -- a snapshot an administrator or a backup
tool took under its own name, even of a quota'd directory or of a driver
volume; a snapshot of the filesystem root or of a directory that is not a
volume; an imported snapshot whose source is gone -- is answered
`FailedPrecondition` and left on the array, with the reason in the
message. To delete such a snapshot through Kubernetes, add
`allowForeignSnapshotDelete: "true"` to the Secret the annotations name, or
use `deletionPolicy: Retain` and delete it on the array. A snapshot taken
through this chart and then imported keeps its `<prefix>-<uid>` name, so it
needs no consent.

A restore judges a snapshot by its source while the source exists: one that
is not a driver volume is refused unless it lies under the restoring class's
own `basePath`. When the source is gone, or cannot be resolved, it judges by
the name: a `<prefix>-<uid>` snapshot restores (a volume deleted after its
snapshot, the case restore exists for), and any other name is refused
`FailedPrecondition` unless the restoring StorageClass sets
`allowForeignSnapshotRestore: true`.

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotContent
metadata:
  name: imported-nightly-0930
  annotations:
    snapshot.storage.kubernetes.io/deletion-secret-name: qumulo-api-token
    snapshot.storage.kubernetes.io/deletion-secret-namespace: qumulo-csi
spec:
  deletionPolicy: Delete        # or Retain, to leave the array's snapshot alone
  driver: csi.qumulo.com
  source:
    snapshotHandle: "qumulo.example.com##35372"
  volumeSnapshotRef:
    name: imported-nightly-0930
    namespace: my-app
```

A scheduled snapshot -- one a snapshot policy took, from a `snapshotSchedule`
class or one made on the array -- cannot be imported this way: `ListSnapshots`
by ID reports nothing for it, so the content never becomes ready, and `DeleteSnapshot`
refuses one, because the policy owns its lifetime.

Restoring after the source volume is already gone -- the common case for a
real disaster recovery, and exactly what Velero restores into -- works
because this driver walks the snapshot by file ID rather than by the source's
live path, so a deleted source does not block the restore. A snapshot the
backup tool or an administrator took under a name of its own, whose source is
gone, needs a class with `allowForeignSnapshotRestore: true` (see above);
one taken through `csi-snapshotter` does not. What is untested
is a snapshot whose *source volume* this driver never provisioned at all (as
opposed to one it provisioned and later deleted): the imported-handle test
above proves the snapshot ID itself is honored regardless of who took it, not
that every possible external snapshot ancestry restores identically.

## Scheduled snapshots

A StorageClass can attach a Qumulo snapshot policy to every volume it
provisions, so volumes get *scheduled snapshots* with no `VolumeSnapshot` object
per PVC. The policy is named `csi-<pv-name>` and its snapshots
`<id>_csi-<pv-name>_<pv-name>`; the `csi-` prefix marks the policy as the
driver's, not the snapshots as Kubernetes snapshots. They are restored by
copying from the volume's hidden `.snapshot` folder, not with a PVC
`dataSource`: [Restore from a scheduled snapshot](../../docs/protect/restore.md#restore-from-a-scheduled-snapshot).

```yaml
storageClasses:
  - name: qumulo-nightly
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /k8s/volumes
    snapshotSchedule: "daily@02:00,keep=7days,tz=UTC"
```

Forms: `every=Nm`, `every=Nh`, `daily@HH:MM`, `weekly@DAY,HH:MM`,
`monthly@D,HH:MM` (days 1-27, or `last`), each optionally followed by
`,keep=<duration>` and `,tz=<zone>`. The retention quantity must be below 100,
so a year is `12months`.

Each modifier may be given once: `keep=7days,keep=30days` is refused rather
than silently keeping one of the two. `tz=` must name an IANA zone such as
`UTC` or `America/Los_Angeles`; an abbreviation or an offset is refused here
rather than stored on a policy that then fires at a time nobody chose, and so
is `Local`, the controller's own zone, which the cluster cannot be told. Zone
names are case-sensitive: `tz=utc` is refused, and the refusal names `UTC`.
The chart refuses the same schedules at install, so a typo fails
`helm upgrade` instead of the first PVC: its values schema admits exactly
what the driver parses (spaces around terms, one-digit hours, leading zeros,
a trailing comma), and the zone name is checked against the driver's own
IANA zone list (`templates/_tzzones.tpl`, generated by
`make chart-timezones`), suggesting the right spelling when only the case is
wrong.

There is no hourly form. `hourly:MM` used to be accepted and is now refused:
Qumulo's hourly frequency takes an interval, not an anchor, so the minute was
validated and then discarded -- `hourly:15` produced a policy firing on the
cluster's own hour boundary with no warning. Use `every=1h` for the same
effect, or `daily@HH:MM` to anchor a snapshot to a time. **A class carrying
`hourly:MM` will fail its next PVC after upgrading**, which is deliberate: that
class never had the schedule it asked for.

**These snapshots are deliberately invisible to Kubernetes.** They do not appear
as `VolumeSnapshot` objects and a PVC cannot restore from one — handing the CO a
snapshot it never asked for would mean `kubectl delete volumesnapshot` could
destroy a scheduled backup. `DeleteSnapshot` refuses one too, with
`FailedPrecondition`, so a CSI client calling the driver directly with a
hand-built handle cannot delete it either. They are an administrator's
safety net on the cluster; use `VolumeSnapshot` for anything Kubernetes needs to
restore from. Both work on the same volume.

One policy per volume, named `csi-<volume>`. The driver passes it no
snapshot-name template (the v1 policy object has none), so its snapshots take
the cluster's default name. A policy an administrator created on the same
directory is never adopted or deleted. The driver tells any policy's snapshot
from one it took itself by the `policy_id` the array records, never by name, so
no scheduled snapshot, from either kind of policy, appears as a VolumeSnapshot.

## Replication (csi-addons)

**Off by default.** The driver can serve the
[csi-addons](https://github.com/csi-addons/kubernetes-csi-addons)
`replication.Controller` service, mapping `VolumeReplication` objects onto
Qumulo directory replication between the array a volume lives on and a peer
array. The RPC state tables were measured against two lab arrays and are
reproduced by the mock server, and the driver has been deployed on a lab EKS
cluster with the csi-addons controller v0.15.0 and driven by
`VolumeReplication` objects (planned and forced failover, failback, the group
form). See `docs/reference/supported-features.md`.

**The cluster needs the csi-addons machinery first**, installed once by an
administrator like the snapshot CRDs:

```sh
kubectl get crd | grep csiaddons
kubectl get pods -A | grep csi-addons-controller
```

**Then enable the sidecar and declare a class per direction.** On each
Kubernetes cluster the class names the *peer* array:

```yaml
sidecars:
  csiAddons:
    enabled: true

volumeReplicationClasses:
  - name: qumulo-to-dr
    targetServer: dr.qumulo.example.com   # bare host or IPv4, like a StorageClass server
    targetApiUrl: https://dr.qumulo.example.com:8000   # required when targetServer is a DNS name
    apiUrl: https://array-a.example.com:8000            # required when the StorageClass server is a DNS name
    replicationMode: continuous           # continuous | policy | policy+continuous
    schedulingInterval: 5m
    secretName: qumulo-replication-tokens
```

A VolumeReplicationClass you write yourself instead of through
`volumeReplicationClasses` needs `schedulingInterval` too; see its row in the
key table below.

**To turn replication off again**, in this order: delete or disable the
`VolumeReplication` objects; upgrade with `volumeReplicationClasses: []`,
`sidecars.csiAddons.enabled=false` and `allowClassRemoval=true`; delete the
VolumeReplicationClasses with kubectl (they carry `helm.sh/resource-policy:
keep`); only then uninstall csi-addons or its CRDs. Removing the CRD first
wedges the release (see "A CRD the release uses must not be removed while it
is installed").

`volumeReplicationClasses[]` is closed against unknown keys, like
`storageClasses[]`. The complete list:

| Values key | Notes |
|---|---|
| `name`, `targetServer`, `secretName` | required. `targetServer` is the peer's bare host or IPv4 |
| `targetApiUrl`, `apiUrl` | the peer's and this array's REST endpoints; see below for when each is required |
| `targetBasePath` | directory on the peer the replica goes under; defaults to the source volume's own parent, so the replica has the same path on both arrays |
| `replicationMode` | `continuous` (default) \| `policy` \| `policy+continuous` |
| `linkSnapshotPolicy` | replicate the volume's scheduled snapshots (policy `csi-<volume>`) with its data; Kubernetes snapshots never replicate; a `policy` mode only, refused with `continuous` |
| `schedulingInterval` | default `5m`: the csi-addons controller's poll pace. It is **not** a lag threshold -- the driver never grades a relationship Degraded on how far behind it is; it reports the measured lag as a metric and grades only on the relationship's own state. The chart always sets it; a VolumeReplicationClass written by hand must set it too. Without it csi-addons calls `GetVolumeReplicationInfo` only when the relationship is created, so the VolumeReplication's `lastSyncTime` stays empty and the replication series freeze at that first read -- `csi_qumulo_replication_recovery_point_age_seconds` never appears when the first sync finished after it (see the troubleshooting guide, "Replication metrics missing or frozen") |
| `allowNonEmptyTarget` | let protection adopt a peer directory that already holds data, which the first job then overwrites. Off by default |
| `replicationScope` | `volume` (default) \| `group`; see "Protecting a `shared-subdirectory` class" below |
| `secretNamespace` | defaults to the release namespace |

**Fixed REST endpoints are required for DNS-named arrays.** The runbook
recommends a DNS name for the StorageClass `server` precisely so it can be
flipped at failover and pods follow it. That is also why the controller must
never dial the REST API at it: after the flip the name is the other array, and
every replication RPC on the original side would fail to resolve its own volume
("the handle may belong to the other array") until somebody adds `apiUrl` and
recreates the class. So the chart and the driver both require `apiUrl` whenever
a StorageClass `server` in the release is not an IP literal, and `targetApiUrl`
whenever `targetServer` is not one. Point each at an endpoint that never moves:
an IP literal, or a per-array name. IP literals need neither.

**Class parameters are immutable.** Once created, a `VolumeReplicationClass`'s
`spec.parameters` cannot be edited: a different `replicationMode`,
`replicationScope`, `allowNonEmptyTarget`, `apiUrl` or `targetApiUrl` means a
new class name, or deleting and recreating the class; a `helm upgrade` that
changes one in place fails the release. Existing `VolumeReplication` objects
keep the class they were created with.

**One Secret, symmetric.** Either array can be the primary after a failover, so
the Secret holds a token per array, keyed by the host the controller dials --
the host of the class's `apiUrl` (or the StorageClass `server`) and of
`targetApiUrl` (or `targetServer`). The plain `token` key still works for the
local array. Both tokens need the five replication privileges listed under
"Required Qumulo API privileges" below on top of the provisioning set, which
already includes the two `PRIVILEGE_REPLICATION_*_READ`: without
`PRIVILEGE_TIME_READ` every `GetVolumeReplicationInfo` fails, without
`PRIVILEGE_SNAPSHOT_DIFFERENCE_READ` a planned promote cannot confirm its
drain, and without `PRIVILEGE_REPLICATION_REVERSE_RELATIONSHIP` a resync cannot
reverse the relationship.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: qumulo-replication-tokens
stringData:
  qumulo.example.com: <token minted on the primary array>
  dr.qumulo.example.com: <token minted on the DR array>
```

This one Secret serves replication, expand and delete, and the snapshot of a
protected volume: every controller call reads the key named after the host it
dials, and falls back to a plain `token` key for the local array. A StorageClass Secret that holds only
`token` keeps working for a class that is not replicated; a Secret keyed by
address needs no `token` key at all. A call that finds neither is refused
naming the host whose key it looked for. A snapshot of a *protected* volume
reads the peer's key too, to confirm the peer has not been promoted (see
"Snapshotting a protected PVC" below), so point the VolumeSnapshotClass's
`csi.storage.k8s.io/snapshotter-secret-name` at this Secret as well.

**What a `VolumeReplication` does.** `primary` on the source cluster creates
the relationship, authorizes it on the peer and pre-stages the replica's export
(read-only) and quota there, so a failover needs nothing from the source array.
`primary` on the DR cluster is the failover: the source is frozen, drained
against the source array's own clock, the target made writable and its export
flipped to read-write; with the source unreachable (asked twice more, 3 s then
6 s apart, within the call's deadline) the controller retries with force and
the drain is skipped -- and only then: a forced promote whose source
answers still freezes and drains it, and a class whose `targetBasePath` or
`targetServer` does not describe the relationship is refused rather than
forced past. `secondary` freezes a source; `resync` reverses
the relationship on the same ID, re-applies the class mode, and reconnects the
old primary as the replica -- discarding whatever was written to it since, which
is why a writable side needs `force`. An Enable is refused when no
relationship end is left on the peer but its export over the replica is
read-write over a non-empty directory: only a promote makes that export
writable, so the peer is taken to hold a promoted copy.

**Protecting a `shared-subdirectory` class.** A per-volume `VolumeReplication`
on such a class is refused: the only relationship possible is the class-wide
one, and driving it from one PVC would freeze every other tenant's volume at
promote. Set `replicationScope: group` on the VolumeReplicationClass instead
and one relationship is created on the class's `basePath`, with the covering
export mirrored read-only and a quota staged per volume; promote flips that one
export to read-write and every volume in the class comes back together. If the
DR array already has a covering export at that path, create it read-only. The
driver does not change a covering export it did not make (it is yours, and
something else may mount it): if it is read-write, Enable is refused naming it
until you set it read-only, and the relationship is created and replicates
meanwhile. Keep it read-only afterwards too. Once a Disable has removed
the relationship, a read-write replica export over data is what marks a
promoted copy. Disable therefore snapshots the DR root while it is still a
read-only replica (a `csi-unpromoted-` snapshot, expiring after 30 days), and
a re-enable over your read-write export re-protects the class only when the DR
copy is unchanged since, with one warning that anything written through that
export is overwritten by replication until you set it read-only. A copy
written since, or a Disable that left no marker (an earlier build, a failed
snapshot, more than 30 days ago), is refused, and the class stays unprotected
until you set that export read-only on the DR array and re-create the
VolumeReplication; Disable logs a warning naming it. An export this
driver made itself is set read-only by Disable before it removes the
relationship, so this applies only to yours. A PVC
created on the class after it is protected gets its quota on the DR array once
its directory has replicated there, at the group's next
`GetVolumeReplicationInfo`, which the csi-addons controller keeps calling. The
whole class is then the unit of failover, which is the point -- and the cost.

csi-addons spec v0.2.0 has no `VolumeGroupReplication` service, so the group
form runs behind the same six RPCs. A `volume_group_id` on the request (an
absolute path, or a driver volume handle) selects it too, for a future
group-aware controller.

**Deleting a protected PVC.** `DeleteVolume` refuses while a relationship is
rooted at the volume's directory, naming the relationship: deleting a source
root is allowed by Core and ends the relationship on both arrays with the
replica orphaned. Disable replication first, or put
`replicationDeletePolicy: disable` in the StorageClass's **delete Secret**
(alongside the token, and the peer's token keyed by its address) to have the
driver remove the relationship itself -- on both arrays, peer end first, so a
peer that cannot be reached or has no token in the Secret refuses the delete
before anything is dismantled. `disable-orphan` is the consent to proceed
anyway, leaving the peer an ENDED end and the replica directory, export and
quota; the log names what was left where. It goes in the Secret, not the
StorageClass, because the CSI `DeleteVolume` request carries no parameters
map. A volume that is a *replica* is refused whatever the policy says, and so
is a group member's DR copy: its directory is inside the group's replica. So
is a *stale source*: a volume whose peer answers that its end has been
promoted (an unplanned failover the old side has not been demoted from); the
refusal names the promoted array, and no policy overrides it.

**Expanding a protected PVC.** `ControllerExpandVolume` raises the replica's
quota first and this array's second, so the source never grows past its copy.
Point the class's `csi.storage.k8s.io/controller-expand-secret-name` at the
replication Secret above: it already holds both arrays' tokens keyed by
address, and the driver reads this array's key for the expand and the peer's
key for the replica-first step. An expand that lands on a read-only replica (after a DNS flip, the
claim's handle names the old array) raises this replica and then the live copy
on the source the expand Secret names (`targetServer` and its token); without
them it is refused, and with the source not answering it is `Unavailable`,
with nothing changed. A promoted, writable copy raises only its own quota. A
stale primary -- the source end of a relationship whose peer has been
promoted, which is what an unplanned failover leaves until the old side is
demoted -- is refused naming the promoted array, with both quotas as they
were: raising the stale copy alone and reporting success left the claim
reading a size the live copy never had. Replication ignores the
target quota but clients do not, so a replica left behind fails the first
write after a promote with `ENOSPC`.

**Snapshotting a protected PVC.** `CreateSnapshot` refuses, with
`FailedPrecondition`, a volume that is behind the live copy: a read-only
replica, or a stale source whose peer has been promoted. The
second needs the peer's token in the snapshot Secret, keyed by the peer's
address, because the source end's own state does not show a promote on the
other side; a protected volume whose peer cannot be asked -- no token for it,
or no answer -- is refused with the remedy rather than snapshotted on faith.
An unprotected volume needs no peer and snapshots as before.

**Quiesce workloads before a planned promote**: a frozen source refuses new
files but silently drops appends to existing ones.

## Prometheus metrics

Off by default. Enabling it gives the driver a `/metrics` endpoint in the
controller and node pods, gives the three CSI sidecars their own
`--http-endpoint`, and renders a Service per workload to scrape.

```yaml
metrics:
  enabled: true
  serviceMonitor:
    enabled: true
  prometheusRule:
    enabled: true
```

| Value | Serves | Default |
|---|---|---|
| `metrics.port.controller` | the driver in the controller pod | 29654 |
| `metrics.port.node` | the driver in the node pod; a **host** port | 29655 |
| `metrics.port.provisioner` | `csi-provisioner` | 29656 |
| `metrics.port.resizer` | `csi-resizer` | 29657 |
| `metrics.port.snapshotter` | `csi-snapshotter`, when enabled | 29658 |
| `metrics.forceEnable` | render ServiceMonitor/PrometheusRule without the CRD check | `false` |
| `metrics.networkPolicy.enabled` | restrict who may scrape the controller | `false` |

Things that bite:

- **`ServiceMonitor` and `PrometheusRule` render only if the Prometheus
  Operator's `monitoring.coreos.com/v1` CRD is present** — a manifest for an
  unknown kind fails the whole release. `helm template` has no cluster to ask,
  so set `metrics.forceEnable: true` when rendering into a GitOps repo.
- **The node port is a host port** (the DaemonSet is `hostNetwork: true`) —
  pick one nothing else on your workers binds. Ports 29652 and 29653 are the
  livenessprobe sidecars; the chart refuses any collision.
- **Endpoints are unauthenticated**, as upstream CSI drivers' are. Per-volume
  series name a volume by `volume_name` and `volume_id` (a digest of its
  handle), not by array or path. `metrics.networkPolicy.enabled: true`
  admits the controller's metrics ports only from
  `metrics.networkPolicy.from`; it needs a CNI that enforces NetworkPolicy and
  does not cover the hostNetwork node pods.
- **Nothing is polled.** Every array-side figure — reachability, quota usage,
  recovery point — is the last value an RPC produced, so an idle driver's
  gauges are as old as its last request. RPC counts and latency carry no such
  caveat.
- **`/healthz` means "this process is alive"**, nothing about any array. Alert
  on `csi_qumulo_api_reachable` instead of restarting on it.

There is no `csi-attacher` to wire: the driver implements no
`ControllerPublishVolume`, so the chart deploys none.

Every series, its labels and exactly when it updates:
[docs/reference/metrics.md](../../docs/reference/metrics.md). The Grafana dashboard is
`dashboards/qumulo-csi.json` in this chart; `metrics.dashboard.enabled` ships it
as a ConfigMap (see [docs/operate/monitor.md](../../docs/operate/monitor.md#dashboard)).

## Capacity reporting

Off by default. Turn it on with one value:

```yaml
sidecars:
  capacity:
    enabled: true
```

You then get one `CSIStorageCapacity` object per StorageClass, each answered
from that class's own Qumulo cluster:

```console
$ kubectl get csistoragecapacity
NAME             STORAGECLASS   CAPACITY
csisc-4k2xp      qumulo-a       80Ti
csisc-9m7rd      qumulo-b       4Ti
```

`GetCapacity` is the one management RPC a multi-cluster controller can answer.
The CSI spec gives it no `secrets` map, but csi-provisioner passes it the
StorageClass's parameters *verbatim* -- secret-reference keys included -- so the
request names both the cluster and the Secret holding that cluster's token. The
driver resolves that one named Secret per call and keeps nothing between calls,
so the same per-request routing that provisioning uses applies here. This needs
no extra RBAC: the controller may already `get` each class's named Secret for
the sidecars, and every container in the pod shares one ServiceAccount.

**The number is `min(cluster free space, the basePath quota's limit minus
its usage)`.** A full quota reports zero. If the array does not report the
quota's usage, the limit is used instead (logged at debug level). One caveat:

- With **no** quota on the basePath, every class on one array reports the same
  number, because a Qumulo directory quota is a logical limit with no separate
  pool of space behind it. Put a quota on each basePath and the classes
  diverge.

A class with `adDomain` set has its quota looked up on `basePath/<domain>`,
which is where that class's volumes actually live.

**It is informational, not a scheduling input.** This chart sets
`volumeBindingMode: Immediate` on every class -- an NFS export is reachable
identically from every node, so there is no topology for the scheduler to
resolve -- and Kubernetes consults capacity only for `WaitForFirstConsumer`
binding. Turn this on to *see* per-class free space, not to change placement.

Per-class fault isolation comes with it: if one cluster is unreachable, that
class's object goes stale while the others keep updating.

A class whose secret reference is templated (`${pvc.namespace}`) cannot report
capacity -- there is no PersistentVolumeClaim to expand the template against.
That class's `GetCapacity` fails with a message saying so, and every other
class keeps working.

## Volume inventory

`ListVolumes` and `ControllerGetVolume`. Off by default:

```yaml
controller:
  inventory:
    enabled: true
```

**These two are the only RPCs where the controller acts on a cluster no request
named.** Every other operation is routed by the StorageClass that owns it.
`ListVolumes` is handed nothing at all -- `max_entries` and `starting_token`
-- and `ControllerGetVolume` gets a volume ID which names the cluster, in this
driver's `<server>#<apiUrl>#<realPath>` encoding, but no credential for it. So both
answer by listing this driver's StorageClasses through the Kubernetes API to
find the clusters they point at, then reading each class's provisioner Secret.
That is the line Rule 0 in `ARCHITECTURE.md` draws, which is why this is opt-in.

It needs **no extra RBAC**: the ClusterRole already grants `storageclasses`
list/get and `secrets` get, because the sidecars need them, and every container
in the pod shares one ServiceAccount. Enabling this changes which container
uses rights the pod already had. Nothing is cached, and no informer is created
-- Rule 4 reserves watching to the sidecars -- so a new class or a rotated
Secret takes effect on the next call.

With it off, both RPCs return `FailedPrecondition` naming the value. The
capabilities are advertised either way, deliberately: a capability that appears
and disappears with a value is worse to operate than one that answers with the
reason it cannot.

### What it reports

One entry per directory one level under each class's `basePath` -- or under
`basePath/<domain>` for a class with `adDomain`, which is where that class's
volumes actually live. Volumes are deduplicated by array and path: two classes
sharing a `basePath` and differing only in protocol yield one entry, and so do
a DNS-named class and an address class whose endpoints (`apiUrl`, or the one
derived from `server`) reach the same array. The entry carries the handle the
directory's PersistentVolume carries, whichever class minted it, so
ListVolumes also lists this driver's PVs: one paged read on a walk's first
page, which the walk's later pages reuse for up to a minute. The
StorageClass list is read once per walk the same way.
With no PV for the directory yet, it carries the handle with no endpoint field; two static PVs naming one directory by both handles are both listed. A DNS-named class
*without* `apiUrl` has a different endpoint from the address class, and the two
cannot be recognised as one array.

**DR copies' PVs.** A DR copy's PV, as `qumulo-csi-repoint` makes it, carries a
handle on the DR array while its `storageClassName` names the primary's class,
so walking each class's own array does not list it: the walk never reaches the
DR array, or, when the DR array has classes of its own, the walk leaves the
replica out. ListVolumes also lists every PV the walk did not list, once
the PV's own array confirms the directory. It reads that array with the
token the PV's class's provisioner Secret keys by that array's address -- the
same key the replication Secret uses:

```yaml
stringData:
  token: <array A token>          # the class's own array
  array-b.example.com: <array B token>
```

Without that key, a class that walks the DR array lends its own credential for
it. A PV with neither is left out, with a warning naming the key to add. The
class's plain `token` is never sent to the DR array.

A directory that is the target of a replication relationship on its array --
a DR copy -- is never listed for want of a PV, including under a class that shares its
`server` name with a class on the other array.

**A directory with no quota is skipped.** No quota means this driver did not
provision it, or a provision failed between creating the directory and setting
one. Reporting such a directory as a volume would invite a reconciler to delete
it. The trade-off is explicit: a volume left behind by a half-failed provision
is deletable but not listable.

`published_node_ids` is always empty, and `LIST_VOLUMES_PUBLISHED_NODES` is
never advertised. There is no `ControllerPublishVolume`, `attachRequired` is
false, so no `VolumeAttachment` objects exist, and the only other source would
be watching pods. Fabricating node IDs would be worse than omitting them.

### One behaviour to know before enabling

**If any cluster a class names cannot be listed, the whole call fails** with
`Unavailable`, rather than returning the clusters that did answer. CSI has no
partial success, so a listing missing one cluster's volumes cannot be told
apart from those volumes having been deleted -- and `ListVolumes` exists to be
consumed by things that act on absence. No answer is safer than a quietly
incomplete one.

A class that is merely misconfigured -- unparseable parameters, no Secret
reference, a Secret that does not exist, or one without a token -- is skipped
with a warning instead, so one bad class does not hide every other class's
volumes. But if that leaves *nothing* listable, the call fails rather than
returning an empty list. A Secret that exists but **could not be read** -- the
Kubernetes API throttled or timed out -- is not a misconfiguration: the call
fails with `Unavailable`, like an unlistable cluster.

### `ControllerGetVolume` reports capacity, not health

Its `volume_condition` field was removed in CSI spec v1.13.0. Health moved to
`ControllerGetVolumeHealth`, which carries a secrets map of its own and needs
none of this machinery. What is left is inspection and reconciliation by tools
that call the RPC directly.

The class used is one whose endpoint -- its `apiUrl`, or the one derived from
its `server` -- is the endpoint the volume handle carries, so the answer comes
from the array the volume is on, with that class's own Secret. A handle
with no endpoint field falls back to the classes naming its `server`. Where
several classes qualify, the first by class name is used: matching on the
volume's path would disambiguate, but the path cannot be resolved without a
credential first.

A volume whose StorageClass has since been deleted has no route to its cluster.
That is reported as `FailedPrecondition`, **not** `NotFound` -- the volume may
well still exist, and saying otherwise would tell a reconciler to clean up
after something that is still there.

### Or don't enable it

For reconciliation across several clusters, listing quotas through the Qumulo
API directly is a better answer, because it is not limited to the classes one
Kubernetes cluster happens to define. `kubectl get pv` gives the Kubernetes
view. Enable this for the `external-health-monitor` sidecar, or for tooling
that speaks CSI and cannot use either.

## Multi-tenant exports and IP restrictions

Two parameters scope a volume's export. Both need
`provisioningModel: dedicated-export`.

```yaml
storageClasses:
  - name: qumulo-finance
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /k8s/finance
    provisioningModel: dedicated-export     # required for both parameters
    tenantID: tenant-finance                # or the integer id, e.g. "4"
    allowedClients:
      - 10.0.0.5
      - 192.168.1.0/24
```

**Without `allowedClients` an export is open.** A dedicated export this driver
creates admits any NFS client that can reach the array address: no host
restriction, no identity mapping (`NFS_MAP_NONE`, so root is root), any
source port, no authentication. That includes hosts outside the
cluster, and any pod running a userspace NFS client. Set `allowedClients` to
the nodes' addresses or CIDRs on every dedicated-export class. The install
notes warn about each class that sets none, and the chart refuses such a
class (`requireAllowedClients`, on by default) unless it sets
`allowAnyClient: true` or a Kerberos `securityFlavor` -- see
[Upgrading](#upgrading) for existing classes. The driver refuses to provision
from such a class too, so one made outside the chart is not open by default. A `shared-subdirectory`
class mounts through a covering export you created, whose access rules are
yours.

`tenantID` attaches the export to a Qumulo **network tenant** — a network
namespace, so the export is reachable only on that tenant's networks. Give it
either the integer id the REST API uses or the name shown in the Web UI; a name
costs one extra API call per provision to resolve, and a tenant with NFS
disabled is refused rather than producing an export that serves nothing.

**`server` must be one of that tenant's own addresses**, and this is the part
that is easy to get wrong, because getting it wrong looks like it worked. A
class naming tenant 4 with a `server` on some other tenant's network
provisions cleanly — the export really is created in tenant 4, the PVC really
does Bind — and then every pod using it fails with `mount.nfs: ... No such
file or directory`, because tenant 4's export is invisible from that address.
The cluster is enforcing isolation correctly; the class is asking for
something that cannot work. `CreateVolume` therefore reads the cluster's
network configuration and refuses up front with `FailedPrecondition`, naming
the addresses that would have worked, before anything is created.

That check never refuses on a guess: it refuses only when `server` is a
floating address of a *different* tenant. If the network configuration cannot
be read (a 403, say), or the tenant has networks using an address scheme this
driver does not decode, or `server` is on no tenant's floating range (a node's
own address, which the check cannot see), it logs a warning and provisions; a
DNS-name `server` is not checked at all. A correctly configured cluster must
not be blocked by something the driver could not see. An array that does not
answer ends the call `Unavailable`, which the provisioner retries.

**The REST endpoint is a separate address.** On a multi-tenant cluster the
tenant network carries NFS while the REST API answers on the management
tenant, so a tenant class almost always needs its own `apiUrl`:

```yaml
    server: 10.20.4.60                       # the tenant's floating address, for NFS
    apiUrl: https://10.20.0.10:8000          # the management address, for REST
```

The driver records that endpoint in each volume and snapshot handle, so
`DeleteVolume`, `ControllerExpandVolume` and the snapshot RPCs — none of which
are given StorageClass parameters — reach it too. Without it they would derive
`https://<server>:8000` and dial the tenant's NFS address, which serves no
REST: volumes would provision and mount and then be undeletable.

`allowedClients` becomes the export's `host_restrictions` — the list of hosts
permitted to mount. A client outside it gets
`mount.nfs: access denied by server`.

Both are recorded on the PV, so `kubectl get pv -o yaml` shows what a volume was
actually restricted to even after the StorageClass has changed:

```yaml
volumeAttributes:
  tenantID: tenant-finance
  allowedClients: 10.0.0.5,192.168.1.0/24
```

Four things to know:

- **`shared-subdirectory` cannot support either, and the chart refuses them
  there.** Such a volume is a directory beneath an export somebody else owns, so
  there is no per-volume export to scope. Accepting the parameters and dropping
  them would report an access restriction that is not applied — worse than not
  offering one.
- **Neither can be changed later.** StorageClass parameters are immutable, so a
  different tenant or allowlist means a new class. Existing volumes keep what
  they were created with.
- **IPs and CIDRs only — host names are refused.** A name resolves at mount time
  on the cluster, so a typo becomes an intermittent access failure long after
  provisioning.
- **Omitting `tenantID` works only on a cluster with one tenant.** There the
  field is not sent and the cluster uses its only tenant, as before. A cluster
  with more than one tenant refuses an export with no tenant, so a
  `dedicated-export` class without `tenantID` is refused there before anything
  is created (`FailedPrecondition`, listing the tenants to choose from).
- **A Kubernetes `NetworkPolicy` does not restrict who can mount a volume.**
  The NFS mount is performed by the **node's kernel**, not by the pod, so a
  `NetworkPolicy` selecting the pod controls nothing about the traffic between
  the node and the Qumulo array -- that connection exists before the pod is
  scheduled and is shared by every pod the node runs. `allowedClients` above is
  the control that actually scopes which hosts can reach an export; a
  `NetworkPolicy` is not a substitute for it and creates a false sense of
  isolation if assumed to be one.

Remember `dedicated-export`'s own constraint: on an NFSv4-enabled cluster it
cannot create an export nested inside an existing one, so the base path needs a
subtree no ancestor export covers.

## Kerberos

`securityFlavor` takes `sys`, `krb5`, `krb5i` or `krb5p`. A `krb5*` flavour
requires `provisioningModel: dedicated-export` and `node.kerberos.enabled:
true`, and cannot be combined with `protocol: nfs3`; the chart refuses such a
class at install rather than silently ignoring the flavour. Set
`protocol: nfs4.1`, as below.

```yaml
storageClasses:
  - name: qumulo-krb5p
    server: qumulo.example.com
    secretName: qumulo-token
    basePath: /csi/secure
    exportBasePath: /csi/secure      # must have no ancestor export; see above
    provisioningModel: dedicated-export
    protocol: nfs4.1
    securityFlavor: krb5p
    # tenantID: tenant-krb             # required if the cluster has more than one tenant
node:
  kerberos:
    enabled: true
```

A Kerberos class is a `dedicated-export` class, so the tenant rule from
[Multi-tenant exports](#multi-tenant-exports-and-ip-restrictions) applies: on a
cluster with more than one tenant, set `tenantID`, or every PVC is refused.

**Both halves are required, and either alone is useless.** The controller
creates the export with `required_authentication_mode` set to the matching
Qumulo mode; the node mounts with the matching `sec=` option. An export
requiring `krb5p` rejects a `sec=sys` client outright, and a `sec=krb5p` mount
against a permissive export negotiates quietly back down to `sys` -- giving
none of the protection that was asked for. One parameter drives both so they
cannot drift apart.

`krb5` authenticates, `krb5i` adds integrity, `krb5p` adds wire encryption. The
identity is the cluster's and the encryption is the NFS protocol's; the driver
only requires them.

A class asking for `krb5*` is preflighted: if the cluster reports
`krb5_enabled: false`, or has no idmap domain configured, the PVC fails with
that reason rather than binding and then wedging a pod at mount.

The controller caches the cluster's NFS settings for up to 5 minutes. After a
change to `krb5*_enabled` or `idmap_domain` on the cluster, a new PVC can see
the old values for that long. No restart is needed; retry after the window.

### What the operator has to supply

`node.kerberos.enabled` hostPath-mounts the host's keytab and `krb5.conf` into
the node pod (`keytabPath` and `krb5ConfPath`, defaulting to
`/etc/krb5.keytab` and `/etc/krb5.conf`). That is all the chart can do.

**The GSS upcall a `sec=krb5p` mount triggers is answered by `rpc.gssd` in the
host's userland**, which this chart neither runs nor can install. So every node
that will mount a Kerberos volume needs:

- a machine-account keytab for `nfs/<node-hostname>@<REALM>`, exported from
  your KDC;
- `rpc.gssd` running on the host;
- a `krb5.conf` naming the realm and its KDC.

hostPath rather than a Secret is deliberate: the machine account's keytab
belongs to the machine, so the driver never holds a credential it could leak,
and there is no per-tenant keytab for it to get wrong.

**Nodes without those files still get a node plugin.** The keytab and
`krb5.conf` are mounted with hostPath type `FileOrCreate`
(`node.kerberos.hostPathType`), so on a node that is not domain-joined the
kubelet creates empty files in their place and the node plugin runs, serving
every non-Kerberos volume. A `sec=krb5*` volume scheduled there is refused
with `FailedPrecondition`, naming the empty or missing keytab, before anything
is mounted. Note that this leaves empty `/etc/krb5.keytab` and
`/etc/krb5.conf` files on such hosts. `hostPathType: File` keeps the strict
behaviour instead -- no node plugin on a node missing either file, so no volume
of any class can mount there -- and is the choice when every node is
domain-joined.

`test/e2e-krb5p/` holds a working end-to-end harness, including a
`stage-host.sh` that prepares a Minikube VM. Read its README before pointing
this at a real realm: it records which prerequisites the scripts cannot supply,
and why the test needs a VM driver rather than `--driver=docker`.

## adDomain

`adDomain` nests a class's volumes under a subdirectory named for an Active
Directory domain:

```yaml
storageClasses:
  - name: qumulo-corp
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /csi/volumes
    adDomain: corp.example.com     # volumes land in /csi/volumes/corp.example.com/
  - name: qumulo-eng
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /csi/volumes
    adDomain: eng.example.com      # volumes land in /csi/volumes/eng.example.com/
```

Use one class per domain. The driver creates the domain directory on the first
provision, so nothing has to be pre-created.

**It is a layout parameter, not an access-control one.** It does not change who
can read a volume. A Qumulo export maps identities by SID, so a domain name
alone does not name a principal to map to, and Qumulo documents that user
mapping has no effect under Kerberos. Restricting access per domain needs
Kerberos on the export, which is a separate concern.

What it does buy is separation: each domain's volumes sit in their own tree, so
a listing on the cluster shows which domain a volume belongs to and an
administrator can apply cluster-side policy per domain directory.

One interaction to know, and it is now handled rather than a caveat:
`GetCapacity` looks a class's quota up on `basePath/<domain>`, not on the bare
`basePath`, because that is where a domain's volumes actually live. So a quota
placed on the domain directory bounds that class correctly.

The two path helpers must insert the domain **in step** -- the real path is what
the REST API acts on and the export path is what the node mounts, so a domain
applied to one and not the other provisions cleanly and then fails to mount.
Provisioning, mounting, expanding and deleting are otherwise unaffected by this
parameter: it changes layout and nothing else.

(An earlier version of this section explained how to add each domain directory
to a top-level list of managed paths so `ListVolumes` would find it. That list
no longer exists -- `ListVolumes` now derives each class's root, domain
included, directly from its own StorageClass parameters, so there is nothing
to keep in step by hand. It is opt-in behind `controller.inventory.enabled` --
see [Volume inventory](#volume-inventory).)

## Migrating from the community driver (`qumulo.csi.k8s.io`)

This driver (`csi.qumulo.com`) does not adopt volumes provisioned by the
earlier community CSI driver automatically -- the two use different volume
handle formats, and neither reads the other's state. Migrating a volume means
creating a **static PV** that points this driver at the same on-cluster
directory the old driver already created, rather than provisioning a new one.

**What is and is not touched.** Nothing about the directory, its quota, or its
NFS export changes. `CreateVolume` is never called for a static PV, so
whatever the old driver (or an administrator) set up stays exactly as it is --
**reused, not recreated.** The only new things are the Kubernetes objects that
tell `csi.qumulo.com` to serve requests against a path it did not provision.

### Steps

1. **Set the old PV's reclaim policy to `Retain`**, if it is not already, so
   deleting the old PVC does not delete the directory:
   ```sh
   kubectl patch pv <old-pv-name> -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
   ```

2. **Record the directory's real path and the array's address** from the old
   PV. Where these live depends on which community driver version provisioned
   it -- check `kubectl get pv <old-pv-name> -o yaml` for `spec.csi.server` /
   `spec.csi.share` (or `spec.nfs.server` / `spec.nfs.path` for a plain NFS-type
   PV). You need the array's hostname and the absolute path the directory is
   reachable at over NFS.

3. **Delete the old PVC, then the old PV object.** The directory itself is
   untouched under `Retain`:
   ```sh
   kubectl delete pvc <old-pvc-name> -n <namespace>
   kubectl delete pv <old-pv-name>
   ```

4. **Note the directory's path on the cluster.** This driver's volume handle
   is `<server>#<apiUrl>#<realPath>` (see `pkg/driver/volume_id.go`) -- the path the
   directory has on the array, not the NFS export path. No file-ID lookup is
   needed: the handle used to carry one, and this step used to be a `curl`
   against `/v1/files/.../info/attributes`, but a file ID is array-local and
   the driver now resolves it from the path at each use.

5. **Create a static PV and a matching PVC**, using this driver's handle
   format and the real NFS-visible path as `exportPath`:
   ```yaml
   apiVersion: v1
   kind: PersistentVolume
   metadata:
     name: migrated-legacy-volume
     # Only when Kubernetes may delete the directory and everything in it --
     # see "Letting Kubernetes delete an imported directory" below:
     # annotations:
     #   csi.qumulo.com/delete-directory: "/<real-cluster-path-to-the-directory>"
   spec:
     capacity:
       storage: 100Gi          # match the directory's existing quota
     accessModes: [ReadWriteMany]
     persistentVolumeReclaimPolicy: Retain   # keep Retain until you are sure
     storageClassName: ""                    # empty: not dynamically provisioned
     csi:
       driver: csi.qumulo.com
       volumeHandle: "<server>##/<real-cluster-path-to-the-directory>"   # the class's apiUrl, if any, between the two #
       volumeAttributes:
         server: "<server>"
         exportPath: "/<real-nfs-path-to-the-directory>"
   ---
   apiVersion: v1
   kind: PersistentVolumeClaim
   metadata:
     name: migrated-legacy-volume
     namespace: <namespace>
   spec:
     accessModes: [ReadWriteMany]
     storageClassName: ""
     volumeName: migrated-legacy-volume
     resources:
       requests:
         storage: 100Gi
   ```
   `exportPath` is the only field `NodePublishVolume` actually needs to mount
   (`mountSourceFor` in `pkg/driver/node_server.go` joins `server` and
   `exportPath` verbatim into the NFS mount source) -- it is not derived from
   anything else, so it must be the exact path a client mounts today, export
   remapping included (see the export-path note under
   [StorageClass parameters](#storageclass-parameters) if this cluster's export
   is not mounted at the filesystem root).

6. **Verify.** Mount the PVC from a pod and confirm the existing data is
   visible and writable:
   ```sh
   kubectl exec <pod> -- sh -c 'ls /data; echo test >> /data/probe && cat /data/probe'
   ```

**Quotas and exports are reused, not recreated,** as stated above -- this is
worth restating because it is the answer to the question this guide exists to
settle. If you later want this driver to expand or delete the volume through
Kubernetes rather than by hand, the secret references go **on the
PersistentVolume**, not on "the class": an imported PV has
`storageClassName: ""`, so there is no class for a sidecar to read them from.
Name them explicitly -- `spec.csi.controllerExpandSecretRef` (`name` and
`namespace`) for expansion, and the
`csi.storage.k8s.io/provisioner-deletion-secret-name` and
`csi.storage.k8s.io/provisioner-deletion-secret-namespace` annotations on the
PV for delete. Only a PV that does name a StorageClass picks these up from
that class's `csi.storage.k8s.io/provisioner-secret-name` / `-namespace` and
`controller-expand-secret-*` parameters. Add whichever applies once you are
ready for this driver to manage the volume's lifecycle. Until then, `Retain` is the safer
reclaim policy: this driver's `DeleteVolume` tree-deletes, and a static PV's
capacity and handle were entered by hand rather than read from the cluster.

### Letting Kubernetes delete an imported directory

`DeleteVolume` deletes a directory only for a released PersistentVolume that
**owns** it. csi-provisioner calls `DeleteVolume` for every released PV
with reclaim `Delete` that names this driver -- a hand-written one too, with or
without a `pv.kubernetes.io/provisioned-by` annotation -- and the array cannot
tell a volume this driver made from an administrator's directory, so the PV
decides. A PV carrying exactly the handle being deleted owns the directory
when:

- its name is the directory's last path element -- every dynamically
  provisioned PV, whose name is also its volume's name (with any
  `--volume-name-prefix`);
- its `csi.qumulo.com/mirrored-from` annotation is that name -- a DR-side PV
  `qumulo-csi-repoint mirror` wrote for a dynamically provisioned source; or
- it carries the opt-in annotation
  `csi.qumulo.com/delete-directory: "<real path>"`, where `<real path>` is the
  directory's path on the array -- the path in the handle, not the NFS export
  path. It is compared cleaned, so a trailing slash does not matter; a value
  with a `.` or `..` element, or any other path, is not the opt-in.

Anything else is refused, with `FailedPrecondition` and nothing deleted,
whatever the directory carries -- its own export, or only a quota. The
refusal names the PV and both ways out: set its
`persistentVolumeReclaimPolicy` to `Retain` and delete the PV, or, if
Kubernetes really should delete the directory and everything in it, add the
annotation to the PV; the provisioner's next retry then deletes it. A PV with
reclaim `Retain` is never deleted through, and lends nothing to a copy of its
handle. So a migrated volume is never deleted by Kubernetes unless you add the
annotation -- `Retain` remains the safer default -- and rewriting the export's
description to `csi.qumulo.com volume <name>` is **no longer** the opt-in:
after an upgrade, an import that relied on it is refused, and kept, until the
annotation is added. The annotation loosens no other check. A directory with
an NFS export on it is still deleted only when that export carries this
driver's description (below), so for an import that kept its own export the
relabel is still needed, alongside the annotation, before Kubernetes can
delete it; an export the driver did not label is never deleted.

`DeleteVolume` refuses as well, with `FailedPrecondition` and nothing deleted,
a directory it cannot tell is a volume at all: one whose export does not carry
this driver's description `csi.qumulo.com volume <name>` (where `<name>` is the
handle's last path element), one holding another volume's export, or one with
neither an export nor a quota.

It refuses too, again with `FailedPrecondition` and nothing deleted,
while **another PersistentVolume still in use** names the same directory (in
any handle form) or a directory below it -- "in use" meaning not `Released`,
not `Failed` and not being deleted. So a PV made by copying another claim's
`volumeHandle` cannot delete that claim's volume when its own claim goes; the
refusal names the other PV and its claim. Set the copy to `Retain` and delete
it. The owner's own delete is never blocked by a released copy: once the owner
is released too it proceeds, and the copy's next retry finds the volume gone
and succeeds. To make both checks the controller lists this driver's
PersistentVolumes once per delete, with the `list` on `persistentvolumes`
every install already grants; if that list fails the delete answers
`Unavailable` and is retried. A volume no PV carries -- csi-provisioner
cleaning up after it failed to save the PV of a volume it had just created --
is deleted as before.

So for every hand-written PV: use `persistentVolumeReclaimPolicy: Retain`
unless Kubernetes should delete its directory, add the annotation only then,
and keep the right to create PersistentVolumes with cluster administrators.

## Static PV handles are not scoped to a managed root

The migration guide above works precisely because this driver imposes no
notion of a "managed root" on a static PV: `NodePublishVolume` mounts
whatever `server`/`exportPath` a `PersistentVolume`'s `csi.volumeAttributes`
name, and `ControllerExpandVolume` acts on whatever directory path a
`csi.volumeHandle` names (any directory except `/`), with no check that
either one falls under any StorageClass's `basePath`. `DeleteVolume` is
narrower: it refuses a handle whose path has a `.` or `..` element, a
directory whose export is not this driver's for that volume, a directory
holding another volume's export, and a directory with neither an export nor a
quota, and any delete while another PersistentVolume still in use names the
same directory or one below it; and it deletes a directory only for a released
PV that owns it -- named after it, a mirror of one that is, or carrying
`csi.qumulo.com/delete-directory` with its real path (see
[Letting Kubernetes delete an imported directory](#letting-kubernetes-delete-an-imported-directory)).
So a hand-written PV cannot have an administrator's directory deleted unless
it carries that opt-in. Mount and expand are not scoped at all. That is a
feature for migration and a risk otherwise: **anyone who can create a `PersistentVolume` object can mount, and
with the right handle expand, any directory on the array this driver's
credential can reach** -- not only directories this driver provisioned.

Verified directly: a static PV built by hand and pointed at a directory
`CreateVolume` never touched -- one entirely outside every configured class's
`basePath` -- mounted and served I/O exactly like a dynamically provisioned
one. There is no separate code path or check that treats an unrecognized
path differently.

This is why PersistentVolume creation is an RBAC concern, not a driver one.
**Restrict `create` on the `persistentvolumes` resource to the identities
that should be able to name arbitrary cluster paths** -- typically cluster
administrators and the `csi-provisioner` ServiceAccount this chart already
scopes narrowly (see [RBAC](#what-gets-deployed)) -- the same way any other
cluster-scoped, unnamespaced resource that can reach outside its intended
boundary needs to be governed. A namespaced user with only PVC-level access
cannot exploit this: creating a `PersistentVolumeClaim` never lets its author
choose the handle a resulting PV gets, only a hand-written `PersistentVolume`
does.

## Required Qumulo API privileges

Security teams will not issue an administrator token for a workload
credential, and this driver does not need one. The table below is every
distinct REST endpoint `pkg/qumulo/client.go` calls, and the privilege each
one requires -- derived from mapping each call site to the endpoint it hits,
cross-referenced against the built-in roles' privilege sets
(`GET /v1/auth/roles/`) on Qumulo Core 7.9.3.

| Operation | Endpoint(s) | Privilege |
|---|---|---|
| Liveness probe (`Probe`) | none | **none** -- `Probe` reports the driver's own liveness and contacts no cluster (it has no credential to use). `GET /v1/version` requires no credential at all on Core 7.9.3, which is why it could never have proved a token works |
| Capacity (`GetCapacity`) | `GET /v1/file-system` | `PRIVILEGE_CAPACITY_READ` |
| Directory / file create, list, rename, hard link (`CreateVolume`, restore) | `POST`/`GET /v1/files/{path}/entries/` | `PRIVILEGE_FILE_FULL_ACCESS` (write), `PRIVILEGE_FS_ATTRIBUTES_READ` (list) |
| File attributes / mode and owner (`CreateVolume`'s `mountPermissions` and NFS owner; the adDomain directory's owner; `EnableVolumeReplication` re-owning a local-owned volume root; restore's mode, owner, group and mtime) | `GET`/`PATCH /v1/files/{path}/info/attributes` | `PRIVILEGE_FS_ATTRIBUTES_READ` (read), `PRIVILEGE_FILE_FULL_ACCESS` (mode, owner and time changes) |
| Resolving a file ID to a path (`ListSnapshots`' `source_volume_id`, restore logging) | `POST /v1/files/resolve` | `PRIVILEGE_ANALYTICS_READ` -- **not** `PRIVILEGE_FS_ATTRIBUTES_READ`, which is what the endpoint looks like it should need. Without it `ListSnapshots` reports an empty `source_volume_id` and restore logs the source by file ID; both log a warning and continue. `DeleteVolume` no longer calls it: it looks the volume up by the path its handle carries. Verified against Core 7.9.3 with a role carrying every other privilege in this table |
| Replication relationship tables (`DeleteVolume`, `ControllerExpandVolume`, `ListVolumes`, and every replication RPC) | `GET /v2/replication/source-relationships/status/`, `GET /v2/replication/target-relationships/status/` | `PRIVILEGE_REPLICATION_SOURCE_READ`, `PRIVILEGE_REPLICATION_TARGET_READ` -- **needed even if you never use replication.** `DeleteVolume` and `ControllerExpandVolume` read both tables for every volume, to refuse deleting a replicated copy and to raise a replica's quota before its source's; without the privileges nothing can prove the volume is unprotected, so both fail `FailedPrecondition` naming them. `ListVolumes` reads the target table to leave out DR copies |
| Copying file content (restore) | `POST /v1/files/{id}/copy-chunk` | `PRIVILEGE_FILE_FULL_ACCESS` |
| Measuring a snapshot's data (restore size check, `ListSnapshots` by ID), and a restore staging directory's live usage (`CreateVolume`'s sweep of abandoned restores) | `GET /v1/files/{id}/aggregates/?snapshot={id}`, `GET /v1/files/{id}/aggregates/` | not yet verified on the lab. A token refused here restores without the size check and reports no snapshot size, rather than failing, and its sweep removes no abandoned staging directory |
| NFS protocol/version check (`CreateVolume`) | `GET /v2/nfs/settings` | `PRIVILEGE_NFS_SETTINGS_READ` |
| Tree-delete (`DeleteVolume`) | `POST`/`GET /v1/tree-delete/jobs/` | `PRIVILEGE_FS_DELETE_TREE_WRITE` (start), `PRIVILEGE_FS_DELETE_TREE_READ` (poll) |
| Quotas (`CreateVolume`, `ControllerExpandVolume`, `GetCapacity`) | `POST`/`GET`/`PUT`/`DELETE /v1/files/quotas/{id}`, `GET /v1/files/quotas/status/{id}` (usage) | `PRIVILEGE_QUOTA_READ`, `PRIVILEGE_QUOTA_WRITE`. A failed usage read is not an error: `GetCapacity` falls back to the limit and `CreateSnapshot` reports no size |
| Snapshots (`CreateSnapshot`, `DeleteSnapshot`, `ListSnapshots`, restore) | `POST`/`GET`/`DELETE /v3/snapshots/` | `PRIVILEGE_SNAPSHOT_READ`, `PRIVILEGE_SNAPSHOT_WRITE` |
| Cluster identity (`CreateSnapshot` with a VolumeSnapshotClass `apiurl`) | `GET /v1/node/state` | **none** -- Core answers it without a credential |
| Snapshot policies (`snapshotSchedule`) | `POST`/`GET`/`DELETE /v1/snapshots/policies/` | `PRIVILEGE_SNAPSHOT_POLICY_READ`, `PRIVILEGE_SNAPSHOT_POLICY_WRITE` |
| NFS exports (`dedicated-export`, `shared-subdirectory`'s covering-export check) | `POST`/`GET`/`DELETE /v3/nfs/exports/` | `PRIVILEGE_NFS_EXPORT_READ`, `PRIVILEGE_NFS_EXPORT_WRITE` |
| Tenant networks (`tenantID` with an IP-literal `server`) | `GET /v3/network` | `PRIVILEGE_NETWORK_READ`. Without it the check that `server` is not another tenant's address is skipped with a warning, and a class on the wrong tenant's address Binds and then fails at mount. Core's 403 names the privilege |
| Replication relationships (the csi-addons RPCs) | `POST`/`PATCH`/`DELETE /v2/replication/source-relationships/`, `POST .../{id}/replicate`; `POST /v2/replication/target-relationships/{id}/authorize`, `.../make-writable`, `.../reconnect`, `.../delete` | `PRIVILEGE_REPLICATION_SOURCE_WRITE` (source side), `PRIVILEGE_REPLICATION_TARGET_WRITE` (target side) |
| Reversing a relationship (`ResyncVolume` after a failover) | `POST /v2/replication/source-relationships/reverse-target-relationship` | `PRIVILEGE_REPLICATION_REVERSE_RELATIONSHIP` -- not implied by the WRITE pair |
| Confirming a planned promote's drain | `GET /v2/snapshots/{id}/changes-since/{id}` | `PRIVILEGE_SNAPSHOT_DIFFERENCE_READ` -- not implied by `PRIVILEGE_SNAPSHOT_READ` |
| The array's clock (`GetVolumeReplicationInfo`'s recovery-point age) | `GET /v1/time/status` | `PRIVILEGE_TIME_READ`. Without it every Info fails |
| Network tenants (`tenantID`, and `dedicated-export` without one) | `GET /v1/multitenancy/tenants/` | `PRIVILEGE_TENANT_READ`. For a `dedicated-export` class with no `tenantID`, a missing privilege only skips the multi-tenant check: the cluster's own refusal is still reported as `FailedPrecondition`, but after the directory and quota exist |

So the full set, if a class uses every feature (snapshots, scheduled
snapshots, `dedicated-export`, `tenantID`) is:

```
PRIVILEGE_ANALYTICS_READ
PRIVILEGE_CAPACITY_READ
PRIVILEGE_FILE_FULL_ACCESS
PRIVILEGE_FS_ATTRIBUTES_READ
PRIVILEGE_FS_DELETE_TREE_READ
PRIVILEGE_FS_DELETE_TREE_WRITE
PRIVILEGE_NFS_SETTINGS_READ
PRIVILEGE_QUOTA_READ
PRIVILEGE_QUOTA_WRITE
PRIVILEGE_SNAPSHOT_READ
PRIVILEGE_SNAPSHOT_WRITE
PRIVILEGE_SNAPSHOT_POLICY_READ
PRIVILEGE_SNAPSHOT_POLICY_WRITE
PRIVILEGE_NFS_EXPORT_READ
PRIVILEGE_NFS_EXPORT_WRITE
PRIVILEGE_NETWORK_READ
PRIVILEGE_TENANT_READ
PRIVILEGE_REPLICATION_SOURCE_READ
PRIVILEGE_REPLICATION_TARGET_READ
```

Replicating (the csi-addons VolumeReplication support) additionally needs,
on both arrays' tokens:

```
PRIVILEGE_REPLICATION_SOURCE_WRITE
PRIVILEGE_REPLICATION_TARGET_WRITE
PRIVILEGE_REPLICATION_REVERSE_RELATIONSHIP
PRIVILEGE_SNAPSHOT_DIFFERENCE_READ
PRIVILEGE_SNAPSHOT_READ
PRIVILEGE_SNAPSHOT_WRITE
PRIVILEGE_TIME_READ
```

`PRIVILEGE_SNAPSHOT_READ` and `PRIVILEGE_SNAPSHOT_WRITE` are on that list
because a *planned* promote takes and reads a freeze-marker snapshot on the
source before it drains, and recreates the linked snapshot policy on the new
primary during reprotect. A token without them replicates and force-promotes,
but every planned promote fails.

Each was named by Core 7.9.2.2's 403 when the route was called without it;
`TestEveryRouteHasItsPrivileges` fails if the driver calls a route whose
privilege this section does not name.

A class that never sets `tenantID` or `snapshotSchedule`, never uses
`dedicated-export`, and never takes snapshots can drop the corresponding
rows -- `PRIVILEGE_TENANT_READ`, the two `PRIVILEGE_SNAPSHOT_POLICY_*`,
`PRIVILEGE_NFS_EXPORT_WRITE`, and the two `PRIVILEGE_SNAPSHOT_*` privileges,
respectively -- except on a **replicated** class (one named by a
VolumeReplicationClass), which cannot drop the two `PRIVILEGE_SNAPSHOT_*`
privileges even though it takes no snapshots of its own, because a planned
promote needs them. `PRIVILEGE_ANALYTICS_READ` can be dropped too, at the
cost of an empty `source_volume_id` in `ListSnapshots`.

Two exceptions to that rule, both of which fail only later, in production:

* `PRIVILEGE_NFS_EXPORT_READ` is **not** droppable, even for a class that
  never uses `dedicated-export`. Every provision lists the export table to
  find or confirm the export covering the volume's path, and every delete
  checks the export at the path before removing anything, so dropping it
  breaks `CreateVolume` and `DeleteVolume` on *every* class. Only
  `PRIVILEGE_NFS_EXPORT_WRITE` is tied to `dedicated-export`.
* A **replicated** class -- one named by a VolumeReplicationClass -- cannot
  drop the two `PRIVILEGE_SNAPSHOT_*` privileges even though it takes no
  snapshots of its own: a planned promote needs them for its freeze marker
  and for the snapshot policy it recreates on reprotect (see the Replicating
  list above).

`PRIVILEGE_REPLICATION_SOURCE_READ` and `PRIVILEGE_REPLICATION_TARGET_READ` are
**not** droppable, whatever the class does: without them no volume can be
deleted or expanded, only created and mounted.

**None of the built-in roles match this set exactly.** `Data-Administrators`
looks like the obvious starting point and is close, but is missing
`PRIVILEGE_NFS_SETTINGS_READ` and `PRIVILEGE_TENANT_READ` -- both of which
this driver needs (the first for every `protocol: nfs4.1` class, the second
for any class setting `tenantID`, and for the multi-tenant check on any
`dedicated-export` class). A role built from `Data-Administrators`
needs both added explicitly; a role built from `Administrators` or
`Observers` already has both but is far broader than this driver needs.

**A dedicated local user for the token replicates.** A directory created
through the REST API is owned by the token's user, a local user and group,
and Qumulo refuses to replicate an entry owned by a local user or group that
has no NFS ID ("... cannot be replicated because it belongs to local user
..."). So the driver gives every directory it creates -- a volume, a
restore's root, an `adDomain` directory -- NFS uid 0 and gid 0, in the same
call that sets its mode, and `EnableVolumeReplication` re-owns a volume root
an older build left local-owned (in group scope also the member volumes and
their domain directories) before it creates the relationship. Only a local
owner or group is replaced: an NFS, SMB or AD owner someone set is kept.
`PRIVILEGE_FILE_FULL_ACCESS`, already in this role, covers the owner change.
Access still comes from `mountPermissions` (0777 by default), not from the
owner; with an administrator token pods saw the same uid 0.

To validate a role before rolling it out, create it with exactly the
privileges above (`POST /v1/auth/roles/`, then assign the role to a user and
mint that user an access token), and run the live suites against it:

```sh
make live-smoke live-getcapacity live-getcapacity-quota \
     live-inventory live-restore live-snapshot \
     live-createvolume live-deletevolume live-expandvolume live-dedicated-export
```

`live-deletevolume` and `live-expandvolume` are the ones that exercise the
replication READ privileges: both RPCs read the relationship tables on every
call. The Qumulo Core
REST API names the specific missing privilege on a `403`, so narrowing a role
that turns out to be too broad is mechanical -- and that is exactly how
`PRIVILEGE_ANALYTICS_READ` was found missing from this table.

## Development

```sh
make helm-lint       # lint, and check appVersion matches VERSION
make helm-template   # render with example values
make helm-verify     # feed rendered parameters through the driver's validation
```

`make helm-verify` is the one that matters: it renders the chart and runs its
StorageClass parameters through the driver's real validation code, so a
parameter the chart writes but the driver rejects fails there rather than as a
PVC stuck `Pending`.
