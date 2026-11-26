# Restore from a snapshot

Restoring creates a **new** volume from a `VolumeSnapshot`. The source volume
is untouched, and it does not have to still exist.

Take the snapshot first: [Snapshot a volume](snapshot.md).

This covers Kubernetes snapshots and imported snapshots. A scheduled snapshot
(from a class's `snapshotSchedule`) is restored by copying from `.snapshot`
instead — see [Restore from a scheduled snapshot](#restore-from-a-scheduled-snapshot).
Which kind is which: [the four kinds of snapshot](snapshot.md).

## Restore to a new volume

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: shared-data-restored
  namespace: my-app
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-nfs
  dataSource:
    name: shared-data-2026-10-16
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  resources:
    requests:
      storage: 100Gi
```

### What a restore costs

Qumulo has no tree-clone API, so a restore **recreates the tree and copies
each file** server-side. The bytes never pass through the driver, but every
entry costs several round trips: a create, the copy, and a PATCH that sets the
snapshot's mode, owner, group and mtime.

| | |
|---|---|
| Bounded at | 50,000 entries (files and directories) / 2 TiB, then `ResourceExhausted` — decided from the snapshot's own counts **before** anything is copied |
| Large trees | may need several provisioning attempts; the PVC stays `Pending` and each attempt resumes where the last stopped |
| If that is slow | raise `sidecars.provisioner.timeout` — the per-attempt budget derives from it |
| Claim too small | `OutOfRange` naming the bytes needed, before anything is staged — but see the warning below: the check trusts the array's reported snapshot size |
| Concurrency | at most 4 restores per controller |
| Not copied | symlinks and device nodes (skipped, one warning each); ACLs beyond the mode; extended attributes; named streams. Hard links **are** restored as links |

!!! note "A restored volume never appears partially"
    It is assembled under a hidden staging name and renamed into place as the
    final step.

!!! warning "The `OutOfRange` check trusts the array's reported snapshot size"
    That size can be far too small for a snapshot taken immediately after a
    large write — 24 KiB reported for about 1.2 GB of content in testing —
    because it has not settled. A claim smaller than the real content then
    passes the check, the restore succeeds, and the new volume sits over its
    own quota (1.26 GB used against a 1 GiB request, measured). The restored
    data is correct and complete; it is the sizing check that is fooled.

    Size the claim from the **source volume**, not from
    `.status.restoreSize` on the `VolumeSnapshot`. The same behaviour occurs on
    plain Kubernetes; it is not OpenShift-specific.

!!! warning "A restore cannot cross REST endpoints"
    `CreateVolume` dials the endpoint of the **StorageClass the new PVC
    names**, not the endpoint recorded in the snapshot ID. Restoring through a
    VolumeSnapshotClass `apiurl` override therefore needs a StorageClass whose
    own `apiUrl` is that same override.

## Restoring after the source is gone

This works, and it is the case restore exists for: the driver walks the
snapshot by **file ID** rather than by the source's live path, so a deleted
source does not block the restore.

!!! note "An imported snapshot whose source is gone needs consent"
    A Kubernetes snapshot restores normally. An
    [imported snapshot](snapshot.md) — one an administrator or a backup tool
    took under its own name — needs a StorageClass with
    `allowForeignSnapshotRestore: true`.

## Copy a volume

!!! warning "PVC-to-PVC cloning is not implemented in 0.1.0"
    The driver does not advertise `CLONE_VOLUME`. A PVC with a
    `dataSource` of `kind: PersistentVolumeClaim` is refused.

    ```yaml
    # NOT supported
    spec:
      dataSource:
        kind: PersistentVolumeClaim
        name: shared-data
    ```

Snapshot-and-restore covers the common case, and is the supported way to make
a copy of a volume.

### Copy a volume with a snapshot

```mermaid
flowchart LR
  A["PVC: shared-data"] -->|VolumeSnapshot| B["Snapshot on Qumulo"]
  B -->|PVC with dataSource| C["PVC: shared-data-copy<br/>a new directory, server-side copy"]
```

**1. Snapshot the source.**

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: shared-data-clone-source
  namespace: my-app
spec:
  volumeSnapshotClassName: qumulo-snapshots
  source:
    persistentVolumeClaimName: shared-data
```

**2. Create the copy from it.**

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: shared-data-copy
  namespace: my-app
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-nfs
  dataSource:
    name: shared-data-clone-source
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  resources:
    requests:
      storage: 100Gi
```

**3. Delete the intermediate snapshot** once the copy is `Bound`, if you do not
want to keep it.

```bash
kubectl -n my-app delete volumesnapshot shared-data-clone-source
```

### What this costs

A restore is a real copy, not a reflink: Qumulo has no tree-clone API, so the
tree is recreated and each file copied server-side with `copy-chunk`.

| | |
|---|---|
| Bounded at | 50,000 entries / 2 TiB |
| Time | proportional to entry count — several round trips per entry |
| Space on the array | the copy is a second full set of data |
| Concurrency | at most 4 restores per controller |

Full detail and the attributes that are not copied:
[What a restore costs](#what-a-restore-costs).

!!! tip "For many identical copies, consider not copying"
    This is an NFS `ReadWriteMany` driver: many pods on many nodes can mount
    the **same** volume concurrently. A read-mostly dataset shared by many
    workloads usually wants one volume and `readOnly: true` volume mounts,
    not N copies.

    ```yaml
    volumeMounts:
      - name: dataset
        mountPath: /data
        readOnly: true
    ```

### Alternatives for bulk copying

| Need | Approach |
|---|---|
| A point-in-time copy inside Kubernetes | snapshot-and-restore, above |
| A copy larger than the restore bounds | copy on the array, or with a Job running `rsync` between two mounted PVCs, then import the result as a [static PV](../provision/import.md) |
| A copy on another array | Qumulo replication — see [Recover from data loss](recover.md) |

## Restore from a backup tool's snapshot

A tool that restores by creating a pre-provisioned `VolumeSnapshotContent`
with an existing `snapshotHandle` — Velero's model — works against this
driver. Verified against a real cluster with a snapshot taken directly through
the Qumulo API.

Build the handle as `<server>##<snapshotID>`, or
`<server>#<apiUrl>#<snapshotID>` when the class sets `apiUrl`.

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotContent
metadata:
  name: imported-nightly-0930
  annotations:
    snapshot.storage.kubernetes.io/deletion-secret-name: qumulo-api-token
    snapshot.storage.kubernetes.io/deletion-secret-namespace: qumulo-csi
spec:
  deletionPolicy: Delete
  driver: csi.qumulo.com
  source:
    snapshotHandle: "qumulo.example.com##35372"
  volumeSnapshotRef:
    name: imported-nightly-0930
    namespace: my-app
```

!!! warning "The handle's endpoint must match the restoring StorageClass's"
    Snapshot IDs are per-array numbers: two arrays behind one name number them
    independently. A mismatch is refused with `InvalidArgument`, and the
    message gives the handle to use instead.

!!! warning "Give the content its deletion credential, with the right prefix"
    The prefix must be `snapshot.storage.kubernetes.io/`.
    `snapshot.storage.k8s.io/` is ignored, and the content then stays
    `Terminating` while the snapshot stays on the array.

!!! note "The size reported for an imported snapshot"
    The array's own size figure for a snapshot taken seconds after a large
    write can be far too low for a while. So the driver also adds up the
    snapshot's file sizes and reports the larger of the two as the
    snapshot's `restoreSize`, and a restore into a claim smaller than that is
    refused with `OutOfRange` before anything is copied. A sparse
    file counts at its full length.

!!! note "A scheduled snapshot cannot be imported"
    `ListSnapshots` by ID reports nothing for one, so the content never becomes
    ready, and `DeleteSnapshot` refuses it — the policy owns its lifetime.
    Restore it as in [Restore from a scheduled snapshot](#restore-from-a-scheduled-snapshot).

Restoring an imported snapshot whose source volume is gone needs a class
with `allowForeignSnapshotRestore: true`. A Kubernetes snapshot does not.

## Restore from a scheduled snapshot

A [scheduled snapshot](snapshot.md) cannot be a PVC `dataSource`: Kubernetes
never sees it. How you get data back depends on whether the volume still
exists.

### The volume still exists: copy from `.snapshot`

Every directory of the volume has a hidden, read-only `.snapshot` folder that
lists its snapshots. It does not appear in `ls -a` but opens by name, over
NFSv3 and NFSv4.1, from any pod that mounts the volume.

**1. Find the snapshot.** Scheduled ones are named
`<id>_csi-<pv-name>_<pv-name>`; IDs grow with time, so the highest is the
newest. The first is taken the moment the volume is created, so it is empty.

```bash
kubectl -n my-app exec deploy/my-app -- ls /data/.snapshot
```

**2. Copy back what you need,** into the live volume or any other:

```bash
kubectl -n my-app exec deploy/my-app -- \
  cp -a /data/.snapshot/419486_csi-pvc-3f6c…_pvc-3f6c…/reports/2026-11.csv /data/reports/
```

**To recover the whole volume as it was,** create an empty PVC and copy into it
from a pod that mounts both:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: copy-from-scheduled-snapshot
  namespace: my-app
spec:
  restartPolicy: Never
  containers:
    - name: copy
      image: busybox:1.36
      command: ["sh", "-c", "cp -a /src/.snapshot/419486_csi-pvc-3f6c…_pvc-3f6c…/. /dst/"]
      volumeMounts:
        - {name: src, mountPath: /src, readOnly: true}
        - {name: dst, mountPath: /dst}
  volumes:
    - name: src
      persistentVolumeClaim: {claimName: shared-data}
    - name: dst
      persistentVolumeClaim: {claimName: shared-data-recovered}
```

!!! note "This copy goes through the node"
    Unlike a `dataSource` restore, which copies on the array, this is an
    ordinary NFS read and write, so it has no entry or size limit but takes the
    time and network that implies. `cp -a` keeps owners only when it runs as a
    user allowed to set them.

### The volume is deleted: ask the storage administrator

Deleting the volume deletes its policy, but the snapshots the policy already
took stay on the array until their `keep=` expires. No `.snapshot` folder
reaches them any more, so the copy is done on the array:

1. Find the snapshot in the array's snapshot list: named
   `<id>_csi-<pv-name>_<pv-name>`, marked as created by a policy. Note its ID
   and its `source_file_id`, the deleted directory.
2. Create an empty PVC to receive the data and let it bind.
3. Copy each file server-side into the new volume's directory: list the old
   tree with `GET /v1/files/<source_file_id>/entries/?snapshot=<id>`, create
   each file in the target, then
   `POST /v1/files/<new-file-id>/copy-chunk` with
   `{"source_id": "<old-file-id>", "source_snapshot": <id>}`. One call copies
   at most 32 MiB: repeat with the reply as the next body until the reply is
   `null`, or a large file is cut short.

!!! tip "Do it before `keep=` runs out"
    Nothing keeps those snapshots past their expiry. For data you cannot
    re-create, also take Kubernetes snapshots, which restore with a PVC and
    outlive the volume, or use `reclaimPolicy: Retain`.

## Next

- [Snapshot a volume](snapshot.md)
- [Recover from data loss](recover.md)
- [Import an existing directory](../provision/import.md)

