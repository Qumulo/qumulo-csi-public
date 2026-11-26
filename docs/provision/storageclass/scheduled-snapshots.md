# Scheduled snapshots on every volume

Any class can take snapshots automatically, on a schedule you set. Every
volume the class creates gets the same schedule.

```yaml
storageClasses:
  - name: qumulo-nightly
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /k8s/volumes
    snapshotSchedule: "daily@02:00,keep=7days,tz=UTC"
    allowVolumeExpansion: true
```

The example above takes a snapshot every night at 2 am and keeps each one for
seven days.

!!! note "These snapshots are protected from Kubernetes"
    Scheduled snapshots are kept on the Qumulo cluster and are not shown as
    Kubernetes objects. That is deliberate: it means nobody can delete your
    backups with `kubectl` by accident.

On the array, the class gives each volume a snapshot policy named
`csi-<pv-name>`, and its snapshots are named `<id>_csi-<pv-name>_<pv-name>`.
The `csi-` prefix marks the policy as the driver's; the snapshots are still
scheduled snapshots, not Kubernetes snapshots —
[the four kinds of snapshot](../../protect/snapshot.md) side by side.

- To get data back from one:
  [Restore from a scheduled snapshot](../../protect/restore.md#restore-from-a-scheduled-snapshot).
- To take snapshots you *can* manage and restore from Kubernetes:
  [Snapshot a volume](../../protect/snapshot.md).
