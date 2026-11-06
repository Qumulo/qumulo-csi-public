# Dedicated exports and access control

By default each volume is a directory beneath one export an administrator
created (`shared-subdirectory`). With `provisioningModel: dedicated-export`
the driver creates **an NFS export per volume** instead.

That is what makes per-volume access control possible: `allowedClients`,
`tenantID` and Kerberos all require it, and the last
two halves of this page cover the first two.

## Before you start

!!! warning "Check for an ancestor export first, if NFSv4 is enabled"
    NFSv4 presents the cluster as a single pseudo-filesystem tree, so exports
    may not overlap. If **any** ancestor of `exportBasePath` is already
    exported — and a cluster exporting `/` always has one — every
    `dedicated-export` provision fails. See
    [Troubleshoot provisioning](troubleshoot.md#dedicated-export-refused-under-nfsv4).

```bash
# Is NFSv4 on?
curl -sk -H "Authorization: Bearer $TOKEN" https://qumulo.example.com:8000/v2/nfs/settings
#    -> {"v4_enabled": true, ...}

# Is an ancestor of basePath exported? Compare export_path, not fs_path.
curl -sk -H "Authorization: Bearer $TOKEN" https://qumulo.example.com:8000/v3/nfs/exports/ \
  | python3 -m json.tool | grep -E 'export_path|fs_path'
```

!!! warning "The two provisioning models want opposite things under NFSv4"
    `shared-subdirectory` needs `basePath` **exported**; `dedicated-export`
    needs it **unexported**. They cannot both serve one `basePath` — give each
    model its own subtree.

## Create the class

```yaml
storageClasses:
  - name: qumulo-dedicated
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /k8s/dedicated            # no ancestor of this may be exported
    provisioningModel: dedicated-export
    protocol: nfs4.1
    allowedClients:                     # required -- see below
      - 10.0.0.0/24
    allowVolumeExpansion: true
```

!!! warning "A dedicated-export class must say who may mount it"
    An export this driver creates admits any NFS client that can reach the
    array address: no host restriction, no identity mapping (`NFS_MAP_NONE`,
    so root is root), no authentication. That includes hosts outside the
    cluster and any pod running a userspace NFS client.

    The chart **refuses to install** the class unless it sets
    `allowedClients`, `allowAnyClient: true`, or a Kerberos `securityFlavor`.
    The driver enforces the same rule at `CreateVolume`, so a class created
    outside the chart is not open by default either.

## Provision a volume

Exactly as for any other class:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: finance-data
  namespace: finance
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-dedicated
  resources:
    requests:
      storage: 100Gi
```

The driver creates the directory, its quota, **and** its own export carrying
the class's tenant, host restrictions and security flavour.

## What it costs

| | `shared-subdirectory` | `dedicated-export` |
|---|---|---|
| Export-table entries | none added | **one per volume** |
| Per-volume access control | no | yes |
| `basePath` must be exported | yes | no |
| Per-volume `VolumeReplication` | refused | supported |

!!! tip "Know the scale before using it broadly"
    One entry in the cluster's export table per volume is worth planning for.
    A `shared-subdirectory` class adds none.

## Restrict which clients can mount

`allowedClients` writes a host restriction into the NFS export the driver
creates for each volume.

### Set it

```yaml
storageClasses:
  - name: qumulo-finance
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /k8s/finance
    provisioningModel: dedicated-export
    allowedClients:
      - 10.0.0.5
      - 192.168.1.0/24
```

The list becomes the export's `host_restrictions`. A client outside it gets
`mount.nfs: access denied by server`.

!!! tip "List the nodes' addresses, or the node subnet"
    The mount is performed by the **node's kernel**, so what must be allowed
    is every node that may run a pod using the class — not the pod network.

### The chart refuses an unrestricted class

`requireAllowedClients` is on by default, so an install or upgrade fails
naming each `dedicated-export` class that sets none. For each class, one of:

```yaml
    allowedClients: ["10.0.0.0/24"]   # your nodes' addresses, or
    allowAnyClient: true              # an open export really is intended, or
    securityFlavor: krb5p             # Kerberos authenticates instead
```

The driver enforces the same rule at `CreateVolume`, so a class created with
`kubectl` rather than Helm is not open by default either.

!!! warning "Fixing an existing class changes its `parameters`, which are immutable"
    So the class also needs a new name, or the old one must be deleted once
    nothing uses it (PVs are kept). Volumes already provisioned keep the
    exports they have, and still mount, expand, delete and snapshot — both
    checks govern only **new** volumes.

    `requireAllowedClients: false` restores the old behaviour release-wide,
    and is kept for one release to let an upgrade through before the classes
    are fixed.

### Rules

| | |
|---|---|
| Accepted | IP addresses and CIDRs |
| **Refused** | host names — a name resolves at mount time on the cluster, so a typo becomes an intermittent access failure long after provisioning |
| Changeable later | no; parameters are immutable |
| Recorded on the PV | yes, so `kubectl get pv -o yaml` shows what a volume was restricted to even after the class changed |

```yaml
volumeAttributes:
  allowedClients: 10.0.0.5,192.168.1.0/24
```

### Limitations

!!! warning "A Kubernetes `NetworkPolicy` does not restrict who can mount a volume"
    The NFS mount is performed by the **node's kernel**, not by the pod, so a
    `NetworkPolicy` selecting the pod controls nothing about that traffic.

!!! note "A `shared-subdirectory` class cannot use this, and the chart refuses it"
    Such a volume is a directory beneath an export somebody else owns, so
    there is no per-volume export to scope — and its access rules are yours.
    Accepting the parameter and dropping it would report a restriction that is
    not applied.

!!! note "This is a host restriction, not authentication"
    Any host in the list mounts as root, unmapped. For authentication, use
    Kerberos; for network isolation, use
    [network tenants](#use-network-tenants).

### NFS-side access denial

`mount.nfs: access denied by server` is **not** an API credential problem. It
means the node's address is outside the export's `host_restrictions` — see
[Troubleshoot Guide](../operate/troubleshoot-mounts.md#pod-stuck-in-containercreating).

!!! note "Kerberos is the only authentication on the data path"
    An export this driver creates maps no identity (`NFS_MAP_NONE`, so root is
    root) and, without `allowedClients`, restricts no host. NFS traffic is
    also unencrypted. Isolate the data path at the network layer.

## Use network tenants

`tenantID` attaches a volume's export to a Qumulo **network tenant** — a
network namespace, so the export is reachable only on that tenant's networks.
!!! note "Where the boundary is"
    **Creating a network tenant and assigning its addresses are Qumulo
    cluster operations** — see the [Qumulo documentation](https://docs.qumulo.com).
    This page covers the StorageClass parameters that attach a volume's export
    to a tenant that already exists, and the addressing mistake the driver
    checks for.

### Set it

```yaml
storageClasses:
  - name: qumulo-finance
    server: 10.20.4.60                   # the tenant's floating address, for NFS
    apiUrl: https://10.20.0.10:8000      # the management address, for REST
    secretName: qumulo-api-token
    basePath: /k8s/finance
    provisioningModel: dedicated-export
    tenantID: tenant-finance             # or the integer id, e.g. "4"
    allowedClients:
      - 10.20.4.0/24
```

Give either the integer id the REST API uses or the name shown in the Web UI.
A name costs one extra API call per provision to resolve, and a tenant with
NFS disabled is refused rather than producing an export that serves nothing.

### The two addresses

!!! warning "`server` must be one of that tenant's own addresses"
    This is the part that is easy to get wrong, because getting it wrong
    **looks like it worked**. A class naming tenant 4 with a `server` on
    another tenant's network provisions cleanly — the export really is created
    in tenant 4, the PVC really does Bind — and then every pod fails with
    `mount.nfs: ... No such file or directory`, because tenant 4's export is
    invisible from that address. The cluster is enforcing isolation correctly;
    the class is asking for something that cannot work.

    `CreateVolume` therefore reads the cluster's network configuration and
    refuses up front with `FailedPrecondition`, naming the addresses that
    would have worked.

!!! note "The check never refuses on a guess"
    It refuses only when `server` is a floating address of a *different*
    tenant. If the network configuration cannot be read (a 403), the tenant
    uses an address scheme the driver cannot decode, or `server` is on no
    tenant's floating range, it logs a warning and provisions. A DNS-name
    `server` is not checked at all. A correctly configured cluster must not be
    blocked by something the driver could not see.

!!! warning "A tenant class almost always needs its own `apiUrl`"
    On a multi-tenant cluster the tenant network carries NFS while the REST
    API answers on the management tenant. The driver records that endpoint in
    every volume and snapshot handle, so `DeleteVolume`,
    `ControllerExpandVolume` and the snapshot RPCs reach it too. Without it
    they would derive `https://<server>:8000` and dial the tenant's NFS
    address, which serves no REST — volumes would provision and mount, and
    then be **undeletable**.

### Privileges

| Endpoint | Privilege | Without it |
|---|---|---|
| `GET /v1/multitenancy/tenants/` | `PRIVILEGE_TENANT_READ` | a tenant name cannot be resolved |
| `GET /v3/network` | `PRIVILEGE_NETWORK_READ` | the wrong-address check is skipped with a warning, and the class Binds and then fails at mount |

### Single-tenant and multi-tenant clusters

!!! note "Omitting `tenantID` works only on a cluster with one tenant"
    There the field is not sent and the cluster uses its only tenant. A
    cluster with **more than one** refuses an export with no tenant, so a
    `dedicated-export` class without `tenantID` is refused there before
    anything is created — `FailedPrecondition`, listing the tenants to choose
    from.

### Rules

| | |
|---|---|
| Requires | `dedicated-export` |
| Changeable later | no; parameters are immutable |
| Recorded on the PV | yes, in `volumeAttributes.tenantID` |
| Refused on a shared class | yes — there is no per-volume export to scope |

## What is recorded on the volume

Both scoping parameters are written onto each PV, so you can see what a volume
was actually restricted to even after the StorageClass has changed:

```bash
kubectl get pv <pv-name> -o jsonpath='{.spec.csi.volumeAttributes}' | python3 -m json.tool
```

```yaml
volumeAttributes:
  tenantID: tenant-finance
  allowedClients: 10.0.0.5,192.168.1.0/24
```

!!! note "Neither can be changed later"
    StorageClass parameters are immutable, so a different tenant or allowlist
    means a new class. Existing volumes keep what they were created with.

## Next

- [Troubleshoot provisioning](troubleshoot.md)
- [Troubleshoot Guide](../operate/troubleshoot-mounts.md)
