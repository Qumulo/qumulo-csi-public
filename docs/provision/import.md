# Import an existing directory

An existing Qumulo directory can be presented to Kubernetes as a
PersistentVolume. Use this to adopt data that already exists, or to hand a
specific directory to a specific workload.

## The volume handle

A static PV names the directory with the driver's handle format:

```
<server>#<apiUrl>#<realPath>
```

| Field | Meaning |
|---|---|
| `server` | the bare NFS host or IPv4 the node mounts from |
| `apiUrl` | the REST endpoint; leave empty for `https://<server>:8000` |
| `realPath` | the absolute path of the directory on the cluster |

An empty middle field is written as two `#` together:
`qumulo.example.com##/k8s/legacy/reports`.

## A static PV

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: legacy-reports
spec:
  capacity:
    storage: 500Gi
  accessModes: [ReadWriteMany]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  mountOptions: [hard, "timeo=600"]
  csi:
    driver: csi.qumulo.com
    volumeHandle: "qumulo.example.com##/k8s/legacy/reports"
    volumeAttributes:
      server: qumulo.example.com
      protocol: nfs4.1
    # Every request must carry its own credential -- there is no fallback.
    nodePublishSecretRef:
      name: qumulo-api-token
      namespace: qumulo-csi
    controllerExpandSecretRef:
      name: qumulo-api-token
      namespace: qumulo-csi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: legacy-reports
  namespace: my-app
spec:
  accessModes: [ReadWriteMany]
  storageClassName: ""
  volumeName: legacy-reports
  resources:
    requests:
      storage: 500Gi
```

!!! warning "A static PV must carry its own credential Secret reference"
    The driver keeps no credential of its own, so a static PV that names none
    fails every controller operation with `no credential for cluster ...`.
    The Secret must also be reachable by RBAC: a Secret named only outside the
    chart goes in `rbac.extraSecrets`.

!!! note "`capacity.storage` on a static PV is a label, not an enforcement"
    What limits the directory is its Qumulo quota, if it has one. Set the PV's
    capacity to match the quota so Kubernetes does not report something the
    cluster will not honour.

## Deleting a statically provisioned volume

!!! warning "Reclaim `Delete` on a hand-written PV does not delete through, by default"
    `DeleteVolume` deletes a directory only for a released PV that **owns**
    it: one named after the directory, as every dynamically provisioned PV is;
    a `qumulo-csi-repoint` mirror of one; or a PV carrying the administrator's
    opt-in annotation. Otherwise the directory is kept and the delete refused.

    This exists because nothing on the array can tell a copied handle, or a
    hand-written PV aimed at an administrator's directory, from a volume the
    driver owns.

To opt a specific PV in:

```yaml
metadata:
  annotations:
    csi.qumulo.com/delete-directory: "/k8s/legacy/reports"
```

The annotation's value must be the directory's real path.

!!! tip "`Retain` is the safer default for imported data"
    Then deleting the PVC leaves both the PV and the directory, and you remove
    the data deliberately on the array.

## Handles the driver will not read

The older `<server>/<fileID>/<name>` form is **not** read. `DeleteVolume`
refuses such a handle with `FailedPrecondition` and deletes nothing, so the PV
stays `Released` — a deliberate refusal, because answering OK would drop the
PV while the directory stayed on the array.

A handle this driver cannot parse at all is answered OK by `DeleteVolume`
without touching the array, because there is nothing it could safely act on and
refusing would strand the PV forever (D-58). Such a handle's directory is left
on the array.

## Not scoped to a managed root

!!! warning "A static handle can name any path on the cluster"
    Static PV handles are not restricted to a StorageClass's `basePath`. That
    is what makes importing arbitrary directories possible, and it is also why
    the delete-ownership rules above exist. Treat the ability to create PVs for
    this driver as equivalent to filesystem access on the array.

## Capabilities on a static volume

| Operation | Works |
|---|---|
| Mount, `ReadWriteMany` | yes |
| Expansion | yes, if the directory has a quota and the PV names an expand Secret |
| Snapshots | yes, through a VolumeSnapshotClass with its own Secret |
| Appearing in `ListVolumes` | only if the directory has a quota |

## Next

- [Provision a volume dynamically](dynamic.md)
