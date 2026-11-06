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
    backups with `kubectl` by accident. To restore from one, or to take
    snapshots you *can* manage from Kubernetes, see
    [Snapshot a volume](../../protect/snapshot.md#schedule-snapshots-with-a-qumulo-policy).
