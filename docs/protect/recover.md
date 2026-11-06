# Recover from data loss

Three layers, which solve different problems. Use more than one.

| Layer | Protects against | Maturity |
|---|---|---|
| CSI `VolumeSnapshot` | accidental deletion, a bad application release | GA |
| Scheduled Qumulo snapshot policies | the same, without Kubernetes being involved | GA |
| Qumulo replication to a peer array | loss of a site or an array | GA |

Set each layer up first — [Snapshot a volume](snapshot.md),
[Replicate to a DR array](replicate.md) — then use this page when something
has to be recovered.

## Snapshot-based recovery

See [Snapshot a volume](snapshot.md) for the full mechanics. The operational
points:

!!! tip "Use both snapshot mechanisms"
    A `VolumeSnapshot` can be destroyed with `kubectl delete`. A
    `snapshotSchedule` policy snapshot **cannot** — it is deliberately not
    surfaced as a Kubernetes object, and `DeleteSnapshot` refuses it. That is
    what makes it a safety net rather than another thing an operator can drop.

    ```yaml
    storageClasses:
      - name: qumulo-nfs
        snapshotSchedule: "daily@02:00,keep=14days,tz=UTC"
    ```

!!! warning "Restores are bounded"
    50,000 entries / 2 TiB per restore, at most 4 concurrently per controller.
    A volume larger than that cannot be recovered through a PVC `dataSource` —
    plan a cluster-side copy for those, and know which of your volumes are in
    that category **before** you need to recover one.

### Recovering a deleted PVC

With `reclaimPolicy: Delete`, deleting the PVC removes the directory. Recovery
is from a snapshot:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: shared-data
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

Restoring after the source volume is already gone works: the driver walks the
snapshot by **file ID** rather than by the source's live path.

!!! tip "Use `reclaimPolicy: Retain` for data you cannot re-create"
    Then deleting the PVC leaves both the PV and the directory, and the
    recovery is a [static PV](../provision/import.md) pointing back at
    it rather than a restore.

## Backup tools

Velero and similar tools that restore by creating a pre-provisioned
`VolumeSnapshotContent` with an existing `snapshotHandle` work against this
driver. Verified against a real cluster, including a snapshot taken directly
through the Qumulo API.

!!! warning "Give every imported content its deletion credential"
    Two annotations, with the prefix `snapshot.storage.kubernetes.io/`.
    Without them `DeleteSnapshot` arrives with no token and is refused every
    few minutes, so with `deletionPolicy: Delete` the content stays
    `Terminating` and the snapshot stays on the array. Adding them later is
    enough — the next retry deletes it.

Details and the handle format: [Restore from a backup tool's snapshot](restore.md#restore-from-a-backup-tools-snapshot).

## Recovery runbook summary

| Scenario | Action |
|---|---|
| A file or directory deleted by the application | restore a snapshot to a new PVC, copy the needed data back |
| A PVC deleted by mistake | restore from the most recent `VolumeSnapshot`, or from the Qumulo policy snapshot on the array |
| A corrupt volume | restore to a **new** PVC and cut over; do not restore over the live one |
| An array unreachable | the `hard` mount pauses and resumes; pods block rather than losing data |
| An array or site lost | promote the DR copy, then repoint claims |
| A driver release gone wrong | roll the Helm release back with `helm rollback` |

!!! note "What is **not** backed up by any of this"
    The Helm values file, the Secrets, and the StorageClass definitions. Keep
    the values file in version control; recreate Secrets from your secret
    store. The classes themselves survive an uninstall (`resource-policy:
    keep`), but not a cluster rebuild.

## Next

- [Snapshot a volume](snapshot.md)
- [Monitor the driver](../operate/monitor.md)
