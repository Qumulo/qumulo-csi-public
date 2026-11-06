# Adopt an existing folder

Most volumes are created by the driver when someone asks for one. You can also
go the other way: take a folder that is **already** on your Qumulo cluster and
hand it to Kubernetes.

Use this to adopt data that predates Kubernetes, or to give one particular
workload one particular folder.

## How it differs from an ordinary volume

| | Created by the driver | A folder you already have |
|---|---|---|
| Who makes the folder | the driver | you, or it was always there |
| What you write | a claim | a PersistentVolume **and** a claim |
| Size limit | set by the driver | whatever quota the folder already has |
| On delete | the folder is removed | the folder is kept, unless you opt in |

## Name the folder

The driver identifies a folder with three fields joined by `#`:

```
<server>#<apiUrl>#<path>
```

| Field | What to put |
|---|---|
| `server` | the host name or IP your machines mount from |
| `apiUrl` | the management address — leave empty for the usual `https://<server>:8000` |
| `path` | the folder's full path on the Qumulo cluster |

Leaving the middle field empty means writing two `#` together:

```
qumulo.example.com##/k8s/legacy/reports
```

## Write the PersistentVolume and claim

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

```bash
kubectl apply -f legacy-reports.yaml
kubectl -n my-app get pvc legacy-reports     # Bound
```

Both sides set `storageClassName: ""` deliberately — that is what stops
Kubernetes trying to create storage for this claim.

!!! warning "It must carry its own credential"
    The driver keeps no credential of its own and has no fallback, so a
    PersistentVolume written by hand must point at the Qumulo API token
    Secret itself. That is the **same Secret your storage classes use** — the
    one holding the token under the key `token`, created in
    [Create your first volume](../get-started/first-volume.md). In the example
    above it is `qumulo-api-token` in the `qumulo-csi` namespace.

    It has to be named in two places, because two different components need
    it:

    | Field | Used for |
    |---|---|
    | `nodePublishSecretRef` | mounting the volume on a machine |
    | `controllerExpandSecretRef` | growing the volume later |

    Both name the Secret and the namespace it lives in. Leave one out and
    that operation fails with `no credential for cluster ...` — so a volume
    missing `controllerExpandSecretRef` mounts perfectly well and then refuses
    to grow.

    If this Secret is not one your storage classes already name, add it to
    `rbac.extraSecrets` as well, or the driver is not permitted to read it.

!!! note "The size you write is a label, not a limit"
    What actually limits the folder is its Qumulo quota, if it has one. Set
    `capacity.storage` to match that quota, so Kubernetes does not advertise
    space the cluster will not give.

## Deleting it later

`Retain` — used above — is the safer choice for data you did not create.
Deleting the claim then leaves both the volume object and the folder alone,
and you remove them yourself when you are ready.

!!! warning "Even `Delete` will not remove a folder you handed over"
    The driver only deletes a folder for a volume it can prove it owns. A
    hand-written volume is not that, so the folder is kept and the delete is
    refused — because nothing on the array can tell a hand-written volume
    pointed at an administrator's folder from one the driver created.

    To deliberately allow it, annotate the volume with the folder's real path:

    ```yaml
    metadata:
      annotations:
        csi.qumulo.com/delete-directory: "/k8s/legacy/reports"
    ```

## What works on it afterwards

Mounting, sharing between pods, and growing all behave normally. Snapshots
work if you have them turned on.