# Snapshot a volume

Two independent mechanisms, which work on the same volume:

| | CSI `VolumeSnapshot` | Scheduled (`snapshotSchedule`) |
|---|---|---|
| Driven by | Kubernetes | a Qumulo snapshot policy |
| Visible to Kubernetes | yes | **deliberately not** |
| Restorable by a PVC | yes | no |
| Deletable with `kubectl` | yes | no |
| Purpose | application-driven points in time | an administrator's safety net |

This page covers both. For getting data back, see
[Restore from a snapshot](restore.md).

## Enable CSI snapshots

The cluster needs the shared snapshot machinery first — it is installed once by
an administrator and is not this chart's to own:

```bash
kubectl get crd | grep snapshot          # expect three CRDs
kubectl get pods -A | grep snapshot-controller
```

Then:

```yaml
sidecars:
  snapshotter:
    enabled: true

volumeSnapshotClasses:
  - name: qumulo-snapshots
    deletionPolicy: Delete          # Delete | Retain
    secretName: qumulo-api-token    # required: snapshot RPCs carry their own
```

!!! warning "Without the CRDs, both halves fail differently"
    `volumeSnapshotClasses` makes `helm install` itself fail with
    `no matches for kind "VolumeSnapshotClass"`. `sidecars.snapshotter.enabled`
    alone leaves the controller pod un-Ready, which stops ordinary
    provisioning too — every container shares the one pod.

## Take a snapshot

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: shared-data-2026-10-16
  namespace: my-app
spec:
  volumeSnapshotClassName: qumulo-snapshots
  source:
    persistentVolumeClaimName: shared-data
```

```bash
kubectl -n my-app get volumesnapshot shared-data-2026-10-16
```

## Delete a snapshot

!!! warning "A snapshot this driver did not take is not deleted unless you say so"
    The array records nothing about who took a snapshot except its name.
    `csi-snapshotter` names every snapshot `<prefix>-<volumesnapshot-uid>`, and
    `DeleteSnapshot` deletes only a snapshot with that shape whose source is
    one of this driver's volumes. Anything else is answered
    `FailedPrecondition` and left on the array, with the reason in the message.

    To delete such a snapshot through Kubernetes, add
    `allowForeignSnapshotDelete: "true"` to the Secret the deletion annotations
    name, or use `deletionPolicy: Retain` and delete it on the array.

## Snapshots of a protected (replicated) volume

!!! warning "A copy behind the live one is refused"
    `CreateSnapshot` refuses a read-only replica, and a stale source whose peer
    has been promoted. The second needs the peer's token in the snapshot
    Secret, keyed by the peer's address — a protected volume whose peer cannot
    be asked is refused with the remedy rather than snapshotted on faith.

## Schedule snapshots with a Qumulo policy

A StorageClass can attach a Qumulo snapshot policy to every volume it
provisions, so volumes get scheduled snapshots with no `VolumeSnapshot` object
per PVC:

```yaml
storageClasses:
  - name: qumulo-nightly
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /k8s/volumes
    snapshotSchedule: "daily@02:00,keep=7days,tz=UTC"
```

Forms: `every=Nm`, `every=Nh`, `daily@HH:MM`, `weekly@DAY,HH:MM`,
`monthly@D,HH:MM` (days 1–27, or `last`), each optionally followed by
`,keep=<duration>` and `,tz=<zone>`. The retention quantity must be below 100,
so a year is `12months`.

!!! warning "There is no hourly form"
    `hourly:MM` is refused: Qumulo's hourly frequency takes an interval, not an
    anchor, so the minute was validated and then discarded. Use `every=1h`, or
    `daily@HH:MM` to anchor to a time. A class still carrying `hourly:MM` will
    fail its next PVC after upgrading — deliberately, because that class never
    had the schedule it asked for.

!!! note "These snapshots are invisible to Kubernetes on purpose"
    Handing the CO a snapshot it never asked for would mean
    `kubectl delete volumesnapshot` could destroy a scheduled backup.
    `DeleteSnapshot` refuses a policy-created snapshot too. One policy per
    volume, named `csi-<volume>`; a policy an administrator created on the same
    directory is never adopted or deleted.

## Limits

- `ListSnapshots` requires a filter; an unfiltered listing is refused, a
  consequence of the stateless multi-cluster model.
- A VolumeSnapshotClass `apiurl` override must reach the volume's **own**
  cluster; the driver compares cluster UUIDs and refuses a different cluster.
  When either endpoint cannot be read, the check is skipped and the snapshot
  taken with a warning (D-14).

## Next

- [Restore from a snapshot](restore.md)
- [Recover from data loss](recover.md)
