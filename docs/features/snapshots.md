# Snapshots and restore

Point-in-time copies of a volume, kept on the Qumulo cluster. Two kinds
matter day to day:

- **Kubernetes snapshots** — you create a `VolumeSnapshot`, and restore it
  into a new PVC. The first half of this page.
- **Scheduled snapshots** — a StorageClass `snapshotSchedule` takes them on
  the array; Kubernetes never sees them. See
  [Snapshots on a schedule](#snapshots-on-a-schedule).

Both are Qumulo snapshots underneath. All four kinds, their names on the
array and how to tell them apart: [Snapshot a volume](../protect/snapshot.md).

## Turn Kubernetes snapshots on

Snapshots need two things that are not on by default: the snapshot CRDs in
your cluster, and the snapshotter helper.

```yaml
sidecars:
  snapshotter:
    enabled: true
```

!!! warning "Install the snapshot CRDs first"
    They are a cluster-wide add-on, not part of this chart. Turning the
    snapshotter on without them leaves the controller permanently not-Ready,
    which also stops ordinary volume creation.

## Take a snapshot

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: my-data-monday
spec:
  volumeSnapshotClassName: qumulo-snapshots
  source:
    persistentVolumeClaimName: my-data
```

```bash
kubectl get volumesnapshot my-data-monday
```

## Restore into a new volume

A restore makes a **new** volume; it does not roll the original back.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-data-restored
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-nfs
  dataSource:
    name: my-data-monday
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  resources:
    requests:
      storage: 10Gi
```

Qumulo has no instant-clone, so a restore copies the files. It is resumable —
an interrupted restore picks up where it stopped — and the volume is assembled
out of sight and moved into place, so it is never visible half-finished.

!!! warning "Restores are capped"
    50,000 files and folders, or 2 TiB, whichever comes first, checked before
    any copying starts. Up to four restores run at once.

!!! warning "A snapshot restores only onto its own cluster"
    Cross-cluster restore is refused. Two arrays behind one name number their
    snapshots independently, so the driver will not guess.

!!! warning "Size the restore claim from the source volume"
    A snapshot taken immediately after a large write can be reported by the
    array far smaller than the data it holds — 24 KiB for about 1.2 GB in
    testing — because the figure has not settled yet. The capacity check uses
    that figure, so a claim smaller than the real content is accepted, and the
    restored volume then sits over its own quota. The data restores correctly;
    only the sizing check is fooled. Ask for at least as much as the source
    volume held.

## Snapshots on a schedule

Any storage class can attach a Qumulo snapshot policy, so every volume it
creates is snapshotted automatically.

```yaml
storageClasses:
  - name: qumulo-nightly
    snapshotSchedule: "daily@02:00,keep=7days,tz=UTC"
```

That takes a snapshot each night at 2 am and keeps it a week.

!!! note "Scheduled snapshots are protected from Kubernetes"
    They are kept on the Qumulo cluster and are deliberately not shown as
    Kubernetes objects, so nobody can delete your backups with `kubectl` by
    accident. The driver also refuses to delete a scheduled snapshot.

    To get data back from one, copy it from the volume's hidden `.snapshot`
    folder: [Restore from a scheduled snapshot](../protect/restore.md#restore-from-a-scheduled-snapshot).

!!! warning "`tz=` is case-sensitive, and `Local` is refused"
    `UTC` and `Europe/Berlin` work; `utc` does not. `tz=Local` is rejected
    outright, because it would mean whatever zone the controller happened to
    be running in.
