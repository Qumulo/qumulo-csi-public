# FAQ

## General

??? question "Which Kubernetes versions are supported?"
    1.35 and later. The chart enforces `kubeVersion: ">=1.35.0-0"`, and the
    end-to-end matrix for this release is 1.35.6 and 1.36.0. See
    [Prerequisites](../get-started/prerequisites.md).

??? question "Can one installation serve more than one Qumulo cluster?"
    Yes — that is the design. The controller holds no cluster address and no
    standing API client; it builds one per request from the StorageClass
    parameters and the request's Secret. One StorageClass and one Secret per
    cluster.

??? question "Do I need `csi-attacher`?"
    No, and the chart deploys none. The driver implements no
    `ControllerPublishVolume`, so `attachRequired` is `false`. With
    `attachRequired: true` every pod would block on a `VolumeAttachment`
    nothing fulfils.

??? question "Why are there two images?"
    The controller only speaks HTTPS and touches no filesystem, so it runs a
    distroless image as a non-root user. The node image **cannot** be
    distroless: `mount-utils` mounts by exec'ing `mount(8)`, and the kernel
    needs the `mount.nfs` helper — so it needs a userland, and root, to mount
    in the host namespace.

??? question "Does the node ever get the Qumulo API token?"
    No. The node service never calls the Qumulo API, so a cluster-wide
    credential never reaches a worker node.

## OpenShift

??? question "Does the driver run on OpenShift?"
    Yes. OpenShift Container Platform 4.22, RHCOS 9.8, NFS 4.1 and NFS 3, on
    `x86_64` and `aarch64`. 4.21 and older ship Kubernetes 1.34, below the
    chart's floor, and are refused. Same chart, same `helm install` — see
    [Install on OpenShift](../get-started/openshift.md).

??? question "Do I have to create a SecurityContextConstraint first?"
    No. The chart detects OpenShift (`openshift.enabled: auto`), renders an SCC
    allowing exactly what the node plugin needs, and grants it to the node
    service account alone. If your organisation does not allow a workload chart
    to create cluster-scoped objects, set `openshift.nodeSCC.create: false` and
    name your admin's SCC — the manifest to hand them is in
    [Operate on OpenShift](../operate/openshift-operations.md#let-your-cluster-admin-manage-the-scc).

??? question "Why does the chart not use the built-in `privileged` SCC?"
    It would work and it grants far more than the node plugin uses. CIS
    OpenShift 5.2 asks that use of `privileged` be kept minimal, and a
    chart-owned SCC keeps the grant auditable and scoped to one service
    account.

??? question "My pod gets a random user ID and cannot write. Why?"
    `restricted-v2` assigns a uid from the project's range and gid 0, and the
    driver sets `fsGroupPolicy: None`, so `fsGroup` is never applied and does
    not grant access. What decides it is the folder mode — `mountPermissions`
    on the class. `0777` (the default) and `0770` work; `0750` is read-only for
    such a pod. See
    [What application pods need](../get-started/openshift.md#what-application-pods-need).

??? question "My pod fails with `doesn't support idmap mounts on this kernel`."
    It runs with `hostUsers: false` (a user-namespace pod, for example under
    `restricted-v3`). The RHCOS 9.8 kernel cannot ID-map NFS mounts, so this
    affects every NFS volume, not only Qumulo's. Run the pod with `hostUsers`
    unset.

??? question "Why did `helm template` produce no SCC?"
    `openshift.enabled: auto` detects OpenShift from the live cluster's API
    list, and an offline render sees no cluster. Set `openshift.enabled: true`
    for `helm template` and GitOps.

## Provisioning

??? question "Does the driver create the parent directory or export?"
    No. `basePath` must already exist, and for `shared-subdirectory` must
    already be covered by an NFS export. The driver verifies rather than
    creates, because an export at `basePath` would be a cluster-wide grant and
    removing a volume would then have to reason about an export other volumes
    still sit under.

??? question "Why did my PVC bind but the pod cannot mount?"
    Most often a `tenantID` class whose `server` is on another tenant's
    network — the export is created correctly and is simply invisible from
    that address. The driver now refuses this up front where it can see it.
    See [Troubleshoot Guide](../operate/troubleshoot-mounts.md).

??? question "A PVC is stuck at `already exists` forever."
    Defect D-60: a create interrupted between making the directory and setting
    its quota. The leftover directory has to be removed on the array, or given
    a quota, by hand. See
    [Troubleshoot provisioning](../provision/troubleshoot.md#a-pvc-stuck-at-already-exists).

??? question "Can I change a StorageClass parameter?"
    No. `parameters` is immutable to the API server, and the chart refuses the
    upgrade before anything is applied. Give the class a new name, or delete
    the old one once nothing uses it. Existing volumes keep what they were
    created with regardless.

??? question "Why does the driver reject a parameter I think is valid?"
    It rejects unrecognised parameters rather than ignoring them, so a typo
    fails loudly instead of silently doing nothing. The
    [parameter reference](../reference/storageclass-parameters.md) is the
    complete list.

## Capacity

??? question "Does a 100Gi PVC reserve 100 GiB on the array?"
    No. A Qumulo directory quota is a logical limit with no pool behind it.
    Volumes consume space as they are written, out of the cluster's free
    space. Plan real capacity on the array and watch
    `csi_qumulo_cluster_free_bytes`.

??? question "Can I shrink a volume?"
    No. Kubernetes itself does not permit shrink.

??? question "Why do all my StorageClasses report the same free space?"
    Because `basePath` has no quota, so the figure falls back to the cluster's
    free space. Put a quota on each `basePath` and the classes diverge.

??? question "Does `CSIStorageCapacity` affect scheduling?"
    No. Every class uses `volumeBindingMode: Immediate`, and Kubernetes
    consults capacity only for `WaitForFirstConsumer` binding. It is there to
    be *seen*.

## Access and permissions

??? question "Is `ReadWriteOnce` enforced?"
    No. This driver publishes no `VolumeAttachment`, and Kubernetes enforces
    the single-node rule only for drivers that have an attach step. Measured:
    two pods on two nodes both wrote the same `ReadWriteOnce` volume. Use
    `ReadWriteOncePod`, which Kubernetes enforces itself in the scheduler and
    the kubelet, or give each workload its own PVC.

??? question "Is `ReadWriteOncePod` enforced?"
    Yes — by Kubernetes, not by the driver, and it needs no attach step. A
    second pod claiming the same PVC stays Pending. It is the supported
    single-writer mode.

??? question "Why is the default `mountPermissions` `0777`?"
    The driver creates the directory with the cluster's default mode and NFS
    uid/gid 0 so that replication accepts it, and an export this driver makes
    maps no identity — so a non-root pod is neither the owner nor in the
    owning group, and every write would fail *after* the mount succeeded.
    Nothing narrower can be chosen without knowing the workload's uid, which a
    StorageClass does not.

??? question "My non-root pod gets `EACCES` on its own `subPath`."
    The kubelet creates `subPath` directories as root, mode `0755`, and the
    chart sets `fsGroupPolicy: None`. Use an `initContainer` that chowns the
    **same `subPath`** — see
    [Non-root pods and `subPath`](../features/mount-options.md#non-root-pods-and-subpath).

??? question "Should I set `fsGroupPolicy: File`?"
    Only for effectively single-writer volumes, and never for one several
    workloads share. The kubelet then walks the whole volume on **every** pod
    start: measured at 4.17 ms per entry, a million-file volume would take 20
    to 70 minutes per start.

??? question "Does the driver manage ACLs?"
    No. There is no ACL code; the resulting cluster ACL is whatever
    `mountPermissions` implies, and the `0777` default yields an
    Everyone-full-control ACE.

## Protocols and locking

??? question "NFSv3 or NFSv4.1?"
    NFSv4.1, unless NFSv4 is switched off on the cluster. Over `nfs3` locks
    are node-local, so two pods on different nodes cannot safely share a file
    lock — measured, with SQLite failing. NFSv4.0 and 4.2 are not accepted.

??? question "Do I need to set `nolock` myself on NFSv3?"
    No. The node image runs no `rpc.statd`, so the driver supplies `nolock`
    for any NFSv3 mount that does not already decide locking. A class that
    explicitly asks for `lock` is refused rather than silently overridden.

??? question "Why did my explicit `mountOptions` lose `timeo=600`?"
    An explicit list is rendered **verbatim** and replaces the default
    (`[hard, timeo=600]`) rather than adding to it. Write the full list.

## Snapshots and DR

??? question "Is PVC-to-PVC cloning supported?"
    No. Use snapshot-and-restore — see [Copy a volume](../protect/restore.md#copy-a-volume).

??? question "Why can't I see my scheduled snapshots as `VolumeSnapshot` objects?"
    Deliberately. Handing the CO a snapshot it never asked for would mean
    `kubectl delete volumesnapshot` could destroy a scheduled backup.
    `DeleteSnapshot` refuses a policy snapshot too.

??? question "How big a volume can be restored?"
    50,000 entries (files and directories) and 2 TiB per restore, decided
    before anything is copied, with at most 4 restores per controller.
    Qumulo has no tree-clone API, so a restore is a real server-side copy.

??? question "Does Velero work?"
    Yes — a pre-provisioned `VolumeSnapshotContent` with an existing
    `snapshotHandle` restores correctly, verified against a real cluster. Give
    every imported content its deletion credential, with the prefix
    `snapshot.storage.kubernetes.io/`.

??? question "Is replication production-ready?"
    Yes. It has been exercised against a live array pair in both directions,
    including planned and forced failover and failback. The delete and expand
    *guards* for replicated volumes are always active, even with replication
    disabled.

## Operations

??? question "Does `/healthz` tell me the array is reachable?"
    No. It means the process is alive; `Probe` contacts no cluster. Alert on
    `csi_qumulo_api_reachable` instead of restarting on it.

??? question "Why are my metrics gauges stale?"
    Nothing is polled. Every array-side figure is the last value an RPC
    produced, so an idle driver's gauges are as old as its last request. RPC
    counts and latency carry no such caveat.

??? question "Do I have to restart anything after rotating a token?"
    No. Credentials are resolved per request and never cached.

??? question "What happens to running pods if I uninstall the driver?"
    They keep working. The NFS mount is a real mount in the node's kernel and
    does not belong to the driver container. New PVCs stay `Pending`, and a PV
    mid-delete sits `Terminating` until the controller is back.

??? question "Does `helm uninstall` delete my data?"
    No. It removes the Deployment, DaemonSet, `CSIDriver`, RBAC and the node
    PriorityClass. StorageClasses carry `resource-policy: keep`; PVs, PVCs,
    the credential Secrets and everything on the Qumulo cluster are not
    Helm's to delete.

??? question "What is `/var/lib/kubelet/csi.qumulo.com-orphaned/`?"
    Normally empty. When it is not, it holds **a pod's only copy of data**
    that reached the node's disk after its NFS mount went away. Nothing ever
    removes it and no metric counts it. Copy anything needed back before
    removing it.

## Security

??? question "Is NFS traffic encrypted?"
    No. TLS covers the Qumulo REST API only. Isolate the data path at the
    network layer — `tenantID` and `allowedClients` support this.

??? question "Does a Kubernetes `NetworkPolicy` restrict who can mount a volume?"
    No. The mount is performed by the **node's kernel**, not by the pod, so a
    policy selecting the pod controls nothing about that traffic.

??? question "Why does the chart refuse my `dedicated-export` class?"
    Because it restricts nobody. Such an export admits any NFS client that can
    reach the array, as root, with no authentication. Set `allowedClients`, or
    `allowAnyClient: true` if an open export really is intended, or a Kerberos
    `securityFlavor`.

??? question "Does the driver need an administrator token?"
    No. It needs the privileges to manage directories, quotas, exports and
    snapshots, and nothing more — each one was named by Core's own 403.