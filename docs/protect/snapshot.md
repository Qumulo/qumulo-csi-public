# Snapshot a volume

Every snapshot of a volume is a snapshot on the Qumulo array. What differs is
who takes it, whether Kubernetes can see it, and who may delete it. This driver
uses four names for them, and so do these docs:

| | Kubernetes snapshot | Scheduled snapshot | Imported snapshot | Array-internal snapshot |
|---|---|---|---|---|
| Taken by | `csi-snapshotter`, for a `VolumeSnapshot` | the array, from the StorageClass `snapshotSchedule` | an administrator or a backup tool, on the array | the array or the driver, during replication |
| Name on the array | `<id>_snapshot-<VolumeSnapshot UID>` | `<id>_csi-<pv-name>_<pv-name>` | whatever its taker chose | `<id>_revert_to_…`, `<id>_replication_…`, `<id>_csi-unpromoted-…`, `<id>_csi-reprotect-check-…` |
| Seen with `kubectl` | yes | **no, deliberately** | once you create a `VolumeSnapshotContent` for it | no |
| Restore | a PVC `dataSource` | copy from `.snapshot` — [Restore from a scheduled snapshot](restore.md#restore-from-a-scheduled-snapshot) | a PVC `dataSource` | not for restore |
| Delete | `kubectl delete volumesnapshot`, with class `deletionPolicy: Delete` | expires by the schedule's `keep=`; the driver never deletes one | needs `allowForeignSnapshotDelete` | expires on its own |
| Replicated with `linkSnapshotPolicy` | no | yes | no | — |
| After the PVC is deleted | kept until you delete the `VolumeSnapshot` | kept until `keep=` expires | kept | — |
| Purpose | application-driven points in time | an administrator's safety net | backups taken outside Kubernetes | none of yours |

"Kubernetes snapshot" and "scheduled snapshot" are independent and work on the
same volume; use both. For getting data back, see
[Restore from a snapshot](restore.md).

## Tell them apart

**From a pod.** Every directory of a volume has a hidden `.snapshot` folder,
readable over NFSv3 and NFSv4.1. It does not appear in `ls -a`, but opens by
name and lists every snapshot of the volume, of every kind:

```bash
kubectl -n my-app exec deploy/my-app -- ls /data/.snapshot
# 419486_csi-pvc-3f6c…_pvc-3f6c…          <- scheduled
# 419510_snapshot-9d2e41b0-…              <- Kubernetes snapshot
```

**From Kubernetes.** A Kubernetes snapshot's array ID is the last field of its
content's handle (`<server>##<id>`, or `<server>#<apiUrl>#<id>`):

```bash
kubectl get volumesnapshotcontent \
  -o custom-columns=NAME:.metadata.name,HANDLE:.status.snapshotHandle
```

**On the array.** The snapshot list marks a scheduled snapshot as created by a
policy (`created_by_policy: true`, or a `policy_id`); that, not the name, is how
the driver tells one from its own. The policy for a volume is named
`csi-<pv-name>`. The `csi-` prefix marks the driver as the policy's owner; the
snapshots it takes are still scheduled snapshots, not Kubernetes snapshots.

!!! note "`.snapshot` shows old data to anyone who can mount the volume"
    A file deleted from the volume stays readable under `.snapshot` until every
    snapshot holding it expires or is deleted. Plan retention with that in mind.

## Enable Kubernetes snapshots

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

!!! warning "An imported snapshot is not deleted unless you say so"
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
    `DeleteSnapshot` refuses a scheduled snapshot too. One policy per
    volume, named `csi-<volume>`; a policy an administrator created on the same
    directory is never adopted or deleted.

    To get data back from a scheduled snapshot, see
    [Restore from a scheduled snapshot](restore.md#restore-from-a-scheduled-snapshot).
    When the volume is deleted, its policy goes with it, but the snapshots the
    policy already took stay on the array until their `keep=` expires.

## Limits

- `ListSnapshots` requires a filter; an unfiltered listing is refused, a
  consequence of the stateless multi-cluster model.
- A VolumeSnapshotClass `apiurl` override must reach the volume's **own**
  cluster; the driver compares cluster UUIDs and refuses a different cluster.
  When either endpoint cannot be read, the check is skipped and the snapshot
  taken with a warning.

## Next

- [Restore from a snapshot](restore.md)
- [Recover from data loss](recover.md)
