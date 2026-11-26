# Changelog

All notable changes to the Qumulo CSI Driver are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Each release covers the driver images and the Helm chart together: the chart
`version` and `appVersion` track the driver version.

## [Unreleased]

Nothing yet.

## [0.1.2] - unreleased

The first release built and published from this repository. The driver code
is that of 0.1.1.

### Added

- `release.yml` builds both driver images (`linux/amd64`, `linux/arm64`) and
  the Helm chart from this repository and publishes them to
  `ghcr.io/qumulo/qumulo-csi-public` on a `v<VERSION>` tag. It refuses a
  version that is already published, so a released tag is never moved.
- Signed build provenance (SLSA, Sigstore) for both images, the chart and the
  csi-addons sidecar, pushed next to each artifact. Verify with
  `gh attestation verify oci://<image>:<tag> --owner Qumulo`.
- `release-sidecar.yml` publishes the csi-addons sidecar rebuild on its own
  tag, `v0.15.1-qumulo.2`, which the chart pins by digest.

## [0.1.1] - unreleased

<!--
Date to be set at publication. Everything below is on `main` and is what 0.1.1
will carry; nothing has been published yet and neither repository has tags.
-->

### Added

- Red Hat OpenShift support: the chart creates and binds the node plugin's
  SecurityContextConstraints, leaves the controller UID to the namespace's
  restricted SCC, and has test pods follow the controller's SCC. New values
  `openshift.enabled`, `openshift.nodeSCC.create`, `openshift.nodeSCC.name`,
  `openshift.controllerSCC`, `openshift.controllerSCCRole`,
  `openshift.testSCC`, `openshift.assignControllerUID`,
  `openshift.trustedCABundle`.
- Red Hat certification annotations and a chart icon.
- `helm test` readiness check: the controller Deployment is Available, the node
  DaemonSet rollout is complete, and the kubelet has registered the driver on
  every node. Runs as `qumulo-csi-driver ready-check` from the controller image.
  New values `tests.ready.enabled`, `tests.ready.image`.
- `helm test` volume check: writes and reads a real volume. New values
  `tests.image`, `tests.imagePullPolicy`, `tests.size`, `tests.storageClass`.
- `controller.hostNetwork` and `controller.dnsPolicy`, for arrays reachable only
  from the nodes and not from the pod network.
- `controller.proxy.httpProxy`, `controller.proxy.httpsProxy` and
  `controller.proxy.noProxy`, to reach the array through an HTTP proxy.
- The Grafana dashboard ships in the chart as a ConfigMap a Grafana sidecar can
  collect. New values `metrics.dashboard.enabled`, `metrics.dashboard.namespace`,
  `metrics.dashboard.labels.grafana_dashboard`.
- `node.healthPort` and `node.registrarHealthPort`, so a second release in the
  same cluster can take its own ports.
- `rbac.createBindings` and `rbac.checkBindings`, for clusters where
  RoleBindings are managed outside Helm.
- `sidecars.csiAddons.revertSnapshotRetention` (default `168h`), the expiry set
  on the array's revert snapshots.
- Memory and ephemeral-storage requests and limits on every container, and
  liveness and readiness probes on every container.
- `controller.provisionerResources` (512Mi) and `controller.resizerResources`
  (256Mi), separating csi-provisioner and csi-resizer from the shared
  `controller.sidecarResources` (128Mi). Both cache every claim and
  PersistentVolume in the cluster, so their memory grows with the volume count;
  the defaults leave room for roughly 15,000 volumes.
- Documentation: "Get started → OpenShift" and "Operate → OpenShift operations",
  including that an OpenShift downgrade is unsupported and that `helm rollback`
  is the supported way back.

### Changed

- All images rebuilt on Red Hat UBI 9. The controller and node plugin now use
  `ubi9/ubi-micro`; the node image takes `nfs-utils` from RHEL and builds `tini`
  from source. Previously the controller used a distroless base and the node
  plugin a Debian base.
- Every sidecar and base image is pinned by tag and digest, so an install
  resolves to exactly the images the release was tested with.
- Sidecar versions:
  - csi-provisioner `v5.3.0` → `v6.4.1`
  - csi-resizer `v1.14.0` → `v2.3.1`
  - csi-snapshotter `v8.2.0` → `v8.7.1`
  - csi-node-driver-registrar `v2.14.0` → `v2.18.1`
  - livenessprobe `v2.16.0` → `v2.20.1`
  - csi-addons k8s-sidecar `v0.15.0` → `v0.15.1-qumulo.1`, published as
    `ghcr.io/qumulo/qumulo-csi-public/csi-addons-k8s-sidecar`: upstream `v0.15.1` rebuilt from
    its own source with Go 1.27.4 and `golang.org/x/net` v0.62.0 on UBI 9,
    which clears the 12 fixable HIGH findings in the upstream image. The
    chart switches back to the upstream image once a release fixes them.
  - external-health-monitor-controller `v0.19.0` (unchanged, now digest-pinned)
- The metrics are no longer labelled a prototype. No metric was renamed, added
  or removed; dashboards built against 0.1.0 continue to work.
- Node-plugin logs from the Kubernetes mount library are routed through the
  driver's structured logger, so one log format covers the whole node pod.
- A host-port clash between two releases in the same cluster is refused at
  install time instead of producing a half-working install.
- The node SCC's RoleBinding is named after the SCC and no longer repeats the
  release name.
- The dashboard renders in the OpenShift console, not only in Grafana.
- A release is named by its version alone. The published controller image, the
  published node image and the published chart all carry `0.1.1` and nothing
  else. 0.1.0 published each image twice — `0.1.0` and `0.1.0-<commit>` — and
  packaged the chart as version `0.1.0+<commit>` with appVersion
  `0.1.0-<commit>`, so a packaged chart defaulted to the commit-tagged image
  and `helm install` needed the commit to name a version. Installing is now
  `--version 0.1.1`. The commit is still compiled into the driver and reported
  by `qumulo-csi-driver -version`; it is no longer part of any published name.
  See *Upgrading*.
- Internal tracker identifiers no longer appear in anything the driver emits.
  0.1.0 carried them in log messages, command-line help and the chart's
  installation refusals, as a bracketed code at the end of a sentence. The
  wording is otherwise unchanged; only those codes are gone. Any alerting or
  log parsing that matched on them needs updating.
- Go 1.27.3 → 1.27.4.

### Removed

- The "Get started → Air-gapped install" documentation page.
- The `helm test` readiness check no longer uses an OpenShift CLI image
  (`ose-cli` / `origin-cli`). Any value previously set for that test image no
  longer applies; see *Upgrading* below.

### Fixed

- A 5xx from the array while listing exports is reported as `Unavailable`
  instead of `Internal`. The external provisioner treats `Internal` as a final
  failure, so a claim created during a brief array or proxy outage could record
  a misleading failure event even though it later bound.
- With `controller.proxy` set, a StorageClass naming a host the proxy refuses
  (403 or 407 on CONNECT) now fails with `InvalidArgument` naming the proxy and
  the host to allow. It previously returned `Unavailable` indefinitely and the
  PVC stayed Pending with no indication the proxy was the cause. Gateway errors
  (502, 503, 504) still retry.
- A snapshot's restore size is no longer under-reported. The array's tree totals
  can lag a large write by a wide margin, so the driver now also adds up the
  snapshot's files and uses the larger figure. A claim too small for its
  snapshot is refused with `OutOfRange` before anything is staged, and
  `ListSnapshots` reports the larger size. Previously a 1 GiB claim could
  restore more than its quota and bind over it.
- A cancelled restore's staging directory is removed on schedule. A restore that
  never committed left a progress record that told the cleanup sweep the restore
  might still return, delaying removal by about an hour.
- A replica export is readable on every network its StorageClass names. The
  array may answer an export's allowed-client list as several blocks and the
  driver read only the first, so a class naming two networks staged the replica
  admitting one network only — after a failover no pod could mount an otherwise
  correct replica while replication reported healthy. The driver now reads the
  union of every restriction block.
- The array's revert snapshots expire. Making a target writable (promote, DR
  drill) or reconnecting it (resync, failback) makes the array snapshot the
  target root first with no expiry, and nothing removed them. The driver now
  sets `sidecars.csiAddons.revertSnapshotRetention` on these snapshots, as a
  best effort that never fails the call.
- csi-provisioner is no longer OOM-killed on a cluster with many volumes. Every
  controller sidecar shared one 128Mi limit, which the provisioner exceeded at
  about 5,000 volumes; it now has its own 512Mi default and the resizer 256Mi.
- A revert snapshot taken during a failback is no longer kept indefinitely. The
  array takes that snapshot after the reconnect call returns, and a failback
  whose target already had a recovery point reported ready on the first poll —
  before the snapshot existed — so nothing ever stamped its expiry. A resync now
  reports not ready until a new revert snapshot carries an expiry, for up to two
  minutes, and the final call of a replication relationship stamps any that
  slipped past.
- A pod is no longer stuck `Terminating` when its array is unreachable. A
  blocked filesystem stat held the mount, so the plain unmount returned "device
  is busy" immediately and the forced-unmount escalation never ran. Any plain
  unmount failure now checks the server and escalates.
- A read-only publish is no longer refused on RHCOS. On RHCOS 9.8 the kernel
  leaves the NFS superblock read-write even for a read-only mount, which the
  driver's read-only guard rejected — refusing every read-only publish on
  OpenShift. When the request itself is read-only the guard now accepts it,
  because the container runtime binds the volume read-only regardless.
- Turning `controller.hostNetwork` off, or rolling back to a chart without it,
  no longer hangs. The anti-affinity rule matched every controller pod; it now
  matches host-network replicas only.
- Documentation states that the replication recovery-point age is extrapolated
  between reads rather than measured continuously.

### Security

- CI fails the build on any fixable HIGH or CRITICAL CVE in any image the chart
  deploys — driver images, sidecars and test images alike.
- Go 1.27.4 for a `net/http` advisory.
- `golang.org/x/net` `v0.60.0` → `v0.62.0` for two advisories. Also
  `golang.org/x/sys` `v0.49.0`, `golang.org/x/term` `v0.47.0`,
  `golang.org/x/text` `v0.43.0`.
- The `helm test` readiness check no longer pulls an OpenShift CLI image, which
  carried CVEs of its own and added a third image to every install for the sake
  of three API reads.

### Upgrading

- No chart value was renamed or removed, the Kubernetes floor is unchanged
  (`>=1.35.0-0`), and no metric or StorageClass parameter changed. A normal
  upgrade needs no action beyond the two points below.
- New resource limits apply on upgrade. Review `controller.resources`,
  `controller.sidecarResources`, `controller.provisionerResources`,
  `controller.resizerResources` and `node.resources.*` against your workload
  before upgrading a busy cluster. The controller pod's total memory request
  rises, because csi-provisioner and csi-resizer no longer share the 128Mi
  sidecar allowance.
- If you overrode the `helm test` image, that value no longer applies. Set
  `tests.ready.image` only to a mirror of the controller image.
- If you pin sidecar images, note the major bumps: csi-provisioner 5 → 6 and
  csi-resizer 1 → 2.
- The image tag you pin, if you pin one, changes shape. A values file holding
  `image.controller.tag: "0.1.0-<commit>"` names a tag that 0.1.1 does not
  publish; use `0.1.1`, or `0.1.1@sha256:<digest>` to pin one exact build. An
  install that sets no tag needs no change — the chart names its own release.
- `helm install --version` takes the plain version: `--version 0.1.1`, not
  `0.1.1+<commit>`.
- Install with an exact chart version.

## [0.1.0] - 2026-10-26

### Added

- Initial release of the Qumulo CSI Driver and its Helm chart: dynamic
  provisioning, importing an existing folder, volume expansion, snapshots and
  restore, replication and disaster recovery, mount options, access control,
  and Prometheus metrics. Requires Kubernetes 1.35.0 or later.

<!--
No release tags exist in either repository yet, so the compare links below
resolve only once v0.1.0 and v0.1.1 are tagged.
-->

[Unreleased]: https://github.com/Qumulo/qumulo-csi-public/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/Qumulo/qumulo-csi-public/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/Qumulo/qumulo-csi-public/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Qumulo/qumulo-csi-public/releases/tag/v0.1.0
