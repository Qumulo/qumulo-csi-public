# Supported features

What works and what it does not do — the authoritative capability list for
driver **0.1.0**.

Driver **0.1.0** · CSI spec **v1.13.0** · Kubernetes **1.35+** ·
Platforms **linux/amd64**, **linux/arm64** (both validated natively) ·
Plugin name `csi.qumulo.com`

The driver provisions Kubernetes persistent volumes on a Qumulo cluster over
NFS. Each volume is a directory beneath an exported path, with an enforced
Qumulo quota — either shared under one export (`shared-subdirectory`, the
default) or as its own dedicated NFS export (`dedicated-export`).

---

## 0. Platforms

| Platform | Supported | Notes |
|---|---|---|
| Kubernetes | 1.35 and later | `kubeVersion: ">=1.35.0-0"`; the end-to-end matrix is 1.35.6 and 1.36.0 |
| OpenShift Container Platform | 4.22 | Same chart, same `helm install`. The chart detects OpenShift and renders its own least-privilege SCC for the node plugin. 4.21 and older ship Kubernetes 1.34 and are refused. |
| Node operating system | RHCOS 9.8 validated on OpenShift; any Linux with `mount.nfs` elsewhere | |
| Architectures | `linux/amd64`, `linux/arm64` | both validated natively |

Two behaviours differ on OpenShift and are documented rather than worked
around:

| Behaviour | Detail |
|---|---|
| User-namespace pods | A pod with `hostUsers: false` cannot use an NFS volume: `mount_setattr ... doesn't support idmap mounts on this kernel`. The RHCOS 9.8 kernel cannot ID-map NFS mounts — a platform limit for every NFS volume, not this driver's. |
| Read-only by `mountOptions` alone | A class that adds only `ro` to `mountOptions` is refused on RHCOS. Set `readOnly: true` on the pod's volume or `volumeMounts` instead. |

See [Install on OpenShift](../get-started/openshift.md).

## 1. Volume lifecycle

| Capability | Detail |
|---|---|
| Dynamic provisioning | A PVC creates a Qumulo directory, a quota and, in `dedicated-export`, an export. |
| Deletion | Tree-delete plus quota, export and policy cleanup. |
| Online expansion | Quota change only — no unmount, no pod restart, no node-side step. |
| Quota enforcement | A Qumulo directory quota is a firm limit **over time**, not an instantaneous one. A fast writer can run past it briefly before the array answers `Disk quota exceeded` — measured at about 3.5 GiB of overshoot on a 1 GiB volume written at 336 MB/s, consistently across all four storage-class shapes. The driver sets the quota correctly and is not in the data path once a volume is mounted. Size volumes with that headroom in mind rather than relying on the quota as a hard instantaneous cap. |
| Idempotent create | A retry after a timeout or restart adopts and completes the earlier attempt rather than duplicating it. **A create cut between making the directory and setting its quota is not resumed by the retry:** the retry finds the directory already there, with no quota, refuses to adopt it as not driver-provisioned, and fails `already exists` — on that attempt and on every later one, so the PVC stays Pending indefinitely (D-60). The directory has to be removed on the array, or given a quota, by hand. |
| Idempotent delete | A volume counts as deleted only when the cluster itself reports it absent, and a PV is never dropped while its directory still exists — **except for a handle this driver cannot parse**, which `DeleteVolume` answers OK without touching the array, because there is nothing it could safely act on and refusing would strand the PV forever (D-58). Such a handle's directory is left on the array. |
| Rollback on partial failure | A `CreateVolume` that fails after some array state exists unwinds what it made where it can. The directory-then-quota window above is the gap: the directory is left behind, and the retry fails `already exists` rather than finishing it (D-60). |
| Volume name validation | Names containing `#`, `/` or NUL, and the names `.` and `..`, are refused before any array call. |
| Delete of a protected volume | Refused by default if the volume **is** a replication source or replica. The guard matches the relationship's own root, so it is not a full containment check: a handle naming a *parent* of a protected root — a directory that merely **contains** a relationship — is not checked against that relationship and is not refused on its account (D-61). Use `Retain` and the PV-ownership guard rather than relying on this to stop a delete above a protected tree. |
| Deletion of a replicated volume | Refused by default if the volume is a replication source or replica, naming the relationship and the remedy. `replicationDeletePolicy: disable` removes both ends first — peer end first, so an unreachable peer is found before anything is dismantled. Verified live. |
| Expansion of a replicated volume | The replica's quota is raised first and the source's second, so a source can never grow past its copy; an unreachable peer leaves both unchanged. Verified live. |
| Static provisioning | An existing Qumulo directory can be presented as a PV. It must carry its own credential Secret reference. |

## 2. Access modes, protocols and permissions

| Capability | Detail |
|---|---|
| `ReadWriteMany` | Many pods across many nodes read and write one volume concurrently. |
| `ReadWriteOncePod` | **Enforced**, by Kubernetes itself in the scheduler and the kubelet — no attach step is involved. The supported single-writer mode. |
| `ReadWriteOnce`, `ReadOnlyMany` | Accepted, **not enforced**. This driver publishes no `VolumeAttachment` (an NFS export needs no attach), and Kubernetes enforces the single-node rule only for drivers that have one. Measured: two pods on two nodes both wrote one `ReadWriteOnce` volume. Enforcing it would require adding a `ControllerPublishVolume` stage. Treat them as intent. |
| NFSv4.1 | Recommended default. `local_lock=none` — locking is handled by the Qumulo cluster, so it is correct across nodes. |
| NFSv3 | Fully functional; `nolock` is supplied automatically rather than required of the operator. |
| Protocol preflight | A class asking for NFSv4.1 against a v4-disabled cluster fails the PVC with a stated reason, instead of leaving a pod hung at mount. |
| Read-only volumes | Mounted read-only and verified after mount: the publish is refused unless both the mount and the NFS superblock are read-only. |
| POSIX permissions (`mountPermissions`) | Octal mode per class, default `0777`, or `inherit` to keep the cluster default. |
| Kerberos (`sec=krb5`, `krb5i`, `krb5p`) | Both halves applied: the export is created with the required authentication mode and the node mount carries the matching `sec=`. Requires `dedicated-export` and NFSv4.1; refused on `shared-subdirectory` or `nfs3` rather than silently ignored. |
| Kerberos preflight | A `krb5*` class against a cluster reporting Kerberos disabled, or with no idmap domain, fails the PVC with the reason. Fails closed if the settings cannot be read. |

## 3. Snapshots

| Capability | Detail |
|---|---|
| Create, delete and list | Qumulo snapshots driven from Kubernetes. Sidecar is opt-in (`sidecars.snapshotter.enabled`). |
| Restore to a new volume | Resumable across attempts: a later attempt skips subtrees an earlier one finished, and a part-copied file resumes where it stopped. |
| Scheduled snapshots (`snapshotSchedule`) | Attaches a Qumulo snapshot policy per volume. These are deliberately **not** surfaced as `VolumeSnapshot` objects, so `kubectl delete` cannot destroy a scheduled backup, and `DeleteSnapshot` refuses a policy-created snapshot. |
| Restore under `adDomain` | Staging and rename happen inside the domain directory; no leftovers in the parent. |
| Reported snapshot size | A snapshot taken immediately after a large write can be reported by the array far smaller than the data it holds — 24 KiB for about 1.2 GB in testing — because the figure has not settled. The capacity check on a restore uses that figure, so a restore into a claim smaller than the real content is **accepted**, and the new volume then exceeds its own quota. The restored data itself is correct and complete. Size a restore claim from the **source volume**, not from the snapshot's reported size. |

## 4. Multi-cluster and multi-tenancy

| Capability | Detail |
|---|---|
| One driver, many Qumulo clusters | A client is built per request from StorageClass parameters plus that request's Secret. One StorageClass per cluster. |
| Per-cluster credentials | Via `csi.storage.k8s.io/*-secret-name`. Each cluster's API token is a separate Kubernetes Secret. |
| Per-class API endpoint (`apiUrl`) | `https://` only; recorded in each volume and snapshot handle, so delete, expand and snapshot operations reach the right array even when the NFS and REST addresses differ. Transient array errors (429/503, timeouts) are retried and reported as retryable; unresolvable names and TLS failures are reported as final. |
| VolumeSnapshotClass `apiurl` override | A VolumeSnapshotClass may set `apiurl` to take snapshots through another REST endpoint of the volume's *own* cluster; the driver compares cluster UUIDs (`GET /v1/node/state`) and refuses a different cluster. The snapshot handle records that endpoint, so a later delete reaches it — but **restore does not follow it**: `CreateVolume` from the snapshot dials the StorageClass's endpoint, so a restore needs a StorageClass whose `apiUrl` is the same override. The UUID check is skipped, and the snapshot taken with a warning, whenever either endpoint cannot be read (D-14). |
| Qumulo network tenants (`tenantID`) | `dedicated-export` only. By integer ID or by name. The driver also verifies that `server` is one of that tenant's own floating addresses and refuses otherwise. A tenant with NFS disabled is refused. |
| Client restrictions (`allowedClients`) | `dedicated-export` only. IPs/CIDRs written into the export's `host_restrictions`. The chart refuses a class with no restriction by default unless `allowAnyClient: true` or a Kerberos `securityFlavor` is set. |
| AD domain layout (`adDomain`) | Nests a class's volumes under a per-domain directory. Layout only — it grants and restricts nothing. |
| Capacity reporting (`GetCapacity`) | Opt-in. Reports `min(cluster free, basePath quota remaining)` per class. |
| Volume inventory (`ListVolumes`, `ControllerGetVolume`) | Opt-in. Enumerates the volumes this driver provisioned, per cluster, for reconciliation and audit. A directory with no quota is skipped as not driver-provisioned. |

## 5. Replication and disaster recovery

> **Everything in this section is opt-in** via `sidecars.csiAddons.enabled`,
> and exercised with live runs against a Qumulo array pair in both directions.
> The delete and expand guards in §1 are not opt-in: they apply even when
> replication is disabled.

| Capability | Detail |
|---|---|
| Replication via csi-addons | Enable, Disable, Promote, Demote, Resync and Info mapped onto Qumulo directory replication between the array a volume lives on and the peer its `VolumeReplicationClass` names. |
| Planned failover | Freezes the source, drains on **data** — a marker snapshot compared against the last replicated snapshot, not a timer — promotes only once the target holds everything, then flips the export read-write. |
| Forced failover | The peer is always consulted first, with bounded retries, so a transport blip cannot silently turn a planned failover into a forced one. With the peer unreachable, promotes at the retained recovery point. |
| Failback and reprotect | Supported, including fencing the old primary's export so a promoted copy cannot be overwritten. |
| Pre-stage | Enable pre-stages the replica's export read-only and its quota while the target is still read-only, so a replica is never reachable read-write before a promote. **Re-enabling after a plain Disable is refused, not working:** a Disable leaves the DR covering export read-write, and the next Enable finds a read-write export it did not make and refuses rather than re-protecting over it, so the volume stays unprotected until the export is set read-only by hand (D-78). Only an Enable that re-protects a copy unchanged since the Disable that marked it unpromoted is allowed through. |
| Group replication (`replicationScope: group`) | One relationship covering a class's `basePath`, with a quota per member volume and one promote for the group. A group root of `/` is refused in every form. |
| Health and RPO reporting (`Info`) | Grades relationship state, job error, queued snapshots, split brain and peer reachability, and reports replication lag. Two writable copies is reported as an error. |
| `qumulo-csi-repoint` | A separate binary that repoints DNS-named claims and restarts pods after a failover. |

## 6. Security

| Capability | Detail |
|---|---|
| TLS to the Qumulo REST API | Verified end to end, including against a private or self-signed CA (`qumulo.caBundle.existingConfigMap`). |
| Fails closed | A wrong CA produces a certificate error and the PVC stays Pending. There is no fallback to an unverified connection. |
| Credential handling | API tokens live in Kubernetes Secrets and are resolved per request; no credential is cached across clusters. |

## 7. Node service and availability

| Capability | Detail |
|---|---|
| Mount and unmount | Real `mount(8)` via `k8s.io/mount-utils`. |
| Forced unmount on a dead array | The plain unmount is bounded (`node.unmountTimeout`, default 30 s) and then escalates `umount -f` → `umount -l`, returning success only once the target has left `/proc/mounts`. |
| Per-volume statistics | Standard `kubelet_volume_stats_*` metrics and alerts work unmodified. Bounded and single-flight, so a slow array cannot stall the kubelet. |
| Controller high availability | Two replicas by default with leader election, soft anti-affinity and a PodDisruptionBudget. |
| Scheduling protection | The node DaemonSet runs under a chart-created non-preempting PriorityClass with explicit resource requests; the controller runs as `system-cluster-critical`. Neither is BestEffort-evictable. A PriorityClass's `value` is **immutable** in Kubernetes: once the chart has created it, `node.priorityClass.value` cannot be changed by an upgrade — the upgrade fails on the rejected update, and the class must be deleted and the release reinstalled to change it. |
| Rollout gating | The node pod is Ready only once the kubelet has registered the plugin; the DaemonSet rolls with `minReadySeconds` and a bounded update strategy. |

## 8. Observability

| Capability | Detail |
|---|---|
| Per-volume capacity metrics | Via the kubelet — existing dashboards and alerts work with no additional configuration. |
| Driver `/metrics` endpoint | Per-RPC counters and latency, Qumulo API counters and latency, per-array reachability, per-volume quota and usage, array capacity, replication RPO, and sidecar metrics. |
| `Service`, `ServiceMonitor` and Grafana dashboard | Shipped with the chart, behind a flag. |
