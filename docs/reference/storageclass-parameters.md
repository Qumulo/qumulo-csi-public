# StorageClass parameters

The complete list of what a `csi.qumulo.com` StorageClass accepts.

!!! warning "Unrecognised parameters are rejected, not ignored"
    A typo fails provisioning with `unknown StorageClass parameter(s)`, and
    the chart refuses an unknown key at install time. The table below is
    therefore exhaustive.

!!! warning "`parameters` is immutable"
    The API server refuses an update to an existing class's `parameters`. Any
    change below means a new class name, or deleting the old class once
    nothing uses it. **Volumes already provisioned keep what they were created
    with** — the parameters are copied into the PV.

## Required

| Parameter | Chart key | Type | Default | Allowed values |
|---|---|---|---|---|
| `server` | `server` | string | *required* | a bare hostname or IPv4 |
| `secretName` | `secretName` | string | *required* | the name of a Secret holding the key `token` |
| `storeRealPath` | `basePath` | string | *required* | an absolute path that already exists |

### `server`

The NFS host the node mounts from, and the first field of the volume handle.

!!! warning "Not a URL — no scheme, no port, no path"
    `https://host:8000` and `host:8000` are both refused, by `helm install`
    and by the driver: `/` or `:` would make the handle undecodable. If REST
    is not on `:8000`, set `apiUrl`.

Set it to a floating IP when NFS traffic uses one. On a tenant class it must
be one of that tenant's own floating addresses.

**Upgrade consideration.** Changing it means a new class; existing PVs carry
the old address in their handles and keep mounting from it.

### `secretName`, `secretNamespace`

Rendered into `csi.storage.k8s.io/provisioner-secret-*` and
`controller-expand-secret-*`. `secretNamespace` defaults to the release
namespace.

The Secret must hold the key `token`, or — for replication and multi-array
work — a key named after each host the controller dials. See
the Qumulo API token Secret, which holds the token under the key `token`.

### `basePath` → `storeRealPath`

The directory volumes are created inside. It must already exist; for
`shared-subdirectory` it must also already be covered by an NFS export.

## Addressing and model

| Parameter | Chart key | Type | Default | Allowed values |
|---|---|---|---|---|
| `apiUrl` | `apiUrl` | URL | `https://<server>:8000` | `https://` only |
| `storeExportPath` | `exportBasePath` | string | the value of `basePath` | an absolute NFS export path |
| `provisioningModel` | `provisioningModel` | enum | `shared-subdirectory` | `shared-subdirectory`, `dedicated-export` |
| `protocol` | `protocol` | enum | `nfs4.1` | `nfs4.1`, `nfs3`; empty lets the kernel negotiate |

### `apiUrl`

The REST endpoint for this class's cluster. It is recorded in every volume and
snapshot handle, so delete, expand and snapshot operations reach the right
array even when the NFS and REST addresses differ.

!!! warning "`https://` only"
    Every call carries the API token, so `http://` is refused here, in
    `targetApiUrl`, and in any handle.

!!! tip "Set it explicitly whenever `server` is a DNS name"
    It is *required* if you ever use replication — a DNS name may be flipped
    at failover, and the controller must never dial REST at a moving name —
    and harmless otherwise.

**Behaviour.** Transient array errors (429/503, timeouts) are retried and
reported as retryable. An unresolvable name or a TLS failure is reported as
final (`InvalidArgument`).

### `provisioningModel`

| | `shared-subdirectory` | `dedicated-export` |
|---|---|---|
| Creates an export | no | one per volume |
| `basePath` must be exported | yes | **no — and no ancestor, under NFSv4** |
| Supports `allowedClients`, `tenantID`, `securityFlavor` | no (refused) | yes |
| Per-volume `VolumeReplication` | refused; use `replicationScope: group` | supported |
| Export-table cost | none | one entry per volume |

!!! warning "The two models want opposite things under NFSv4"
    They cannot both serve one `basePath`. Give each its own subtree. See
    [KI-1](../provision/troubleshoot.md#dedicated-export-refused-under-nfsv4).

### `protocol`

Checked against the cluster before provisioning: a class asking for NFSv4.1
against a cluster reporting `v4_enabled: false` fails the PVC with
`InvalidArgument` naming that setting, rather than binding and then wedging a
pod. The result is cached per cluster for a few minutes. If the array answers
but the setting cannot be read, the volume is allowed rather than blocked.

!!! warning "Set `protocol` **or** an `nfsvers=` mount option, never both"
    The chart refuses the combination at install; otherwise the driver refuses
    it at mount time, by which point a pod is stuck.

## Permissions and layout

| Parameter | Chart key | Type | Default | Allowed values |
|---|---|---|---|---|
| `mountPermissions` | `mountPermissions` | octal string | `"0777"` | an octal mode, or `inherit` |
| `adDomain` | `adDomain` | string | — | a domain name |

### `mountPermissions`

The mode of the volume root. `inherit` keeps the cluster default.

!!! note "Why the default is `0777`"
    The driver creates the directory with the cluster's default mode and NFS
    uid/gid 0 (so replication accepts it), and an export this driver creates
    maps no identity — so a non-root pod is neither the owner nor in the
    owning group, and every write would fail *after* a successful mount.
    Nothing narrower can be chosen without knowing the workload's uid.

!!! warning "Tightening it has consequences for `subPath`"
    The kubelet creates `subPath` directories as root, mode `0755`. Read
    [Non-root pods and `subPath`](../features/mount-options.md#non-root-pods-and-subpath)
    before changing this.

**Limitation.** The driver sets a POSIX mode, not ACLs. The resulting cluster
ACL is whatever the mode implies — `0777` yields an Everyone-full-control ACE.

!!! note "On OpenShift this decides which pods can write"
    `fsGroupPolicy: None` means `fsGroup` grants a pod nothing, so the mode and
    the pod's own uid/gid are the whole story. Under `restricted-v2`, which
    gives each pod a random uid and gid 0:

    | `mountPermissions` | `restricted-v2` (random uid) | `nonroot-v2` (fixed non-root uid) | `anyuid`, `privileged` (root) |
    |---|---|---|---|
    | `0777` (default) | read and write | read and write | read and write |
    | `0770` | read and write (gid 0) | read and write (gid 0) | read and write |
    | `0750` | read only | read only | read and write |

    See [What application pods need](../get-started/openshift.md#what-application-pods-need).

### `adDomain`

Nests this class's volumes under a directory named for an AD domain.

!!! warning "Layout only — it grants and restricts nothing"
    An NFS export maps identities by SID, so a domain name names no principal,
    and Qumulo ignores export user mapping under Kerberos. This parameter
    cannot become a permission.

**Behaviour.** The domain folds into the real path, the export path, restore
staging and the inventory walk alike, so a class's volumes stay listable and
restorable. Capacity is looked up on `basePath/<domain>`.

## Access control — `dedicated-export` only

| Parameter | Chart key | Type | Default | Allowed values |
|---|---|---|---|---|
| `allowedClients` | `allowedClients` | CSV of IPs/CIDRs | — | IPs and CIDRs only |
| `allowAnyClient` | `allowAnyClient` | `"true"` | — | rendered only when `true` |
| `tenantID` | `tenantID` | string | — | an integer id of 1 or greater, or a tenant name; `""` in chart values is the same as unset |
| `securityFlavor` | `securityFlavor` | enum | `sys` | `sys`, `krb5`, `krb5i`, `krb5p` |

!!! warning "A dedicated export with no restriction is refused"
    Such an export admits any NFS client that can reach the array, as root,
    with no authentication (D-62) — including hosts outside the cluster and
    any pod running a userspace NFS client. `requireAllowedClients` is on by
    default, so the class must set `allowedClients`, `allowAnyClient: true`,
    or a Kerberos `securityFlavor`. The driver enforces the same rule at
    `CreateVolume`, so a class made outside the chart is not open either.

### `allowedClients`

Becomes the export's `host_restrictions`. A client outside it gets
`mount.nfs: access denied by server`.

!!! warning "IPs and CIDRs only — host names are refused"
    A name resolves at mount time on the cluster, so a typo becomes an
    intermittent access failure long after provisioning.

Recorded on each PV, so `kubectl get pv -o yaml` shows what a volume was
actually restricted to even after the class has changed:

```yaml
volumeAttributes:
  tenantID: tenant-finance
  allowedClients: 10.0.0.5,192.168.1.0/24
```

### `tenantID`

Attaches the export to a Qumulo network tenant — a network namespace, so the
export is reachable only on that tenant's networks. By integer id or by name
(a name costs one extra API call to resolve). A tenant with NFS disabled is
refused.

!!! warning "`server` must be one of that tenant's own addresses"
    Getting this wrong *looks* like it worked: the export is created in the
    tenant, the PVC Binds, and every pod then fails with `mount.nfs: ... No
    such file or directory`. The driver reads the cluster's network
    configuration and refuses up front with `FailedPrecondition`, naming the
    addresses that would have worked.

    The check never refuses on a guess: if the configuration cannot be read,
    the addressing scheme is one it cannot decode, or `server` is a DNS name,
    it warns and provisions.

!!! note "A tenant class almost always needs its own `apiUrl`"
    The tenant network carries NFS while REST answers on the management
    tenant.

!!! note "Omitting `tenantID` works only on a single-tenant cluster"
    A cluster with more than one refuses an export with no tenant, so a
    `dedicated-export` class without it is refused there, listing the tenants
    to choose from.

### `securityFlavor`

Makes the export *require* that flavour and the node mount with it — both
halves, which is the point.

Requires `dedicated-export` and NFSv4.1; refused on `shared-subdirectory` or
`nfs3` rather than silently ignored. A `krb5*` class against a cluster
reporting Kerberos disabled, or with no idmap domain, fails the PVC with the
reason, and fails closed if the settings cannot be read.

!!! warning "Kerberos needs host-side setup the chart cannot supply"
    Each node needs a machine-account keytab for `nfs/<node>@<REALM>` and a
    running `rpc.gssd` **in the host userland**. The chart mounts
    `/etc/krb5.conf` and `/etc/krb5.keytab` but cannot provide them, and
    refuses a Kerberos class unless `node.kerberos.enabled` is set.

## Snapshots

| Parameter | Chart key | Type | Default | Allowed values |
|---|---|---|---|---|
| `snapshotSchedule` | `snapshotSchedule` | string | — | see below |
| `allowForeignSnapshotRestore` | `allowForeignSnapshotRestore` | bool | `false` | rendered only when `true` |

### `snapshotSchedule`

Attaches a Qumulo snapshot policy, named `csi-<volume>`, to every volume the
class provisions.

Forms: `every=Nm`, `every=Nh`, `daily@HH:MM`, `weekly@DAY,HH:MM`,
`monthly@D,HH:MM` (days 1–27, or `last`), each optionally followed by
`,keep=<duration>` and `,tz=<zone>`. The retention quantity must be below 100,
so a year is `12months`.

!!! warning "`tz=` is a case-sensitive IANA zone name, and `Local` is refused"
    `UTC` and `Europe/Berlin` are accepted; `utc` and `europe/berlin` are not.
    `tz=Local` is refused outright — it would mean whatever zone the
    controller pod happened to run in. The chart refuses an unknown or
    wrong-case zone, and `Local`, at `helm install`, as the driver does at
    provision time; for a case mistake both name the spelling that works.
    `keep=` and `tz=` may each appear only once.

!!! warning "There is no hourly form, and `hourly:MM` is now refused"
    Qumulo's hourly frequency takes an interval, not an anchor, so the minute
    was validated and then discarded — `hourly:15` produced a policy firing on
    the cluster's own hour boundary with no warning. Use `every=1h`, or
    `daily@HH:MM`. A class still carrying `hourly:MM` fails its next PVC after
    upgrading, deliberately.

!!! note "These snapshots are invisible to Kubernetes"
    They are not `VolumeSnapshot` objects, a PVC cannot restore from one, and
    `DeleteSnapshot` refuses one. They are an administrator's safety net.

### `allowForeignSnapshotRestore`

Lets the class restore a snapshot that is not named as this driver's are and
whose source directory is gone. Refused otherwise (D-86).

## Native StorageClass fields

Set through the chart, not as `parameters`:

| Chart key | Default | Note |
|---|---|---|
| `reclaimPolicy` | `Delete` | `Retain` for data you cannot re-create |
| `allowVolumeExpansion` | — | set it from the start; it cannot be added later |
| `mountOptions` | `[hard, timeo=600]` (plus `nolock` on `nfs3`) | rendered **verbatim**; an explicit list replaces the default |
| `isDefault` | `false` | makes it the cluster's default class |
| `volumeBindingMode` | `Immediate` | set by the chart; there is no topology to resolve |

See [Mount options](../features/mount-options.md) for the full discussion
of `mountOptions`.

## Impact on existing PVs

| Change | Effect on existing volumes |
|---|---|
| Any `parameters` change | none — they keep what they were created with |
| `mountOptions` change | none — frozen in each PV (except `nolock`, which the node adds at mount time) |
| `reclaimPolicy` change | none — the PV carries its own |
| Deleting the class | existing PVs keep working; `ControllerGetVolume` loses its route and reports `FailedPrecondition` |