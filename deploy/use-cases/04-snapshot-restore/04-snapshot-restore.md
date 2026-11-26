# 4. Save a copy before a risky change

**When to use:** you are about to upgrade or migrate. You want a way back.

This takes a *Kubernetes snapshot*: a `VolumeSnapshot` object, backed by a
snapshot on the Qumulo cluster. It is fast. It uses almost no space until the
data changes.

A class with `snapshotSchedule` takes *scheduled snapshots* as well. Those do
not show up in `kubectl`; see
[the four kinds of snapshot](../../../docs/protect/snapshot.md).

## Before you start

Your cluster needs the snapshot CRDs and `snapshot-controller`.
Your driver needs `sidecars.snapshotter.enabled: true`.

```sh
kubectl get crd | grep volumesnapshot            # expect 3 lines
kubectl get pods -A | grep snapshot-controller    # expect a running pod
```

## Run

```sh
kubectl apply -f deploy/use-cases/04-snapshot-restore/volumesnapshotclass.yaml
kubectl apply -f deploy/use-cases/04-snapshot-restore/volumesnapshot.yaml
```

If the change goes wrong, restore into a new PVC:

```sh
kubectl apply -f deploy/use-cases/04-snapshot-restore/pvc-from-snapshot.yaml
```

Then point your workload at `app-data-restored`.

## Check

```sh
kubectl get volumesnapshot before-upgrade
# NAME             READYTOUSE   SOURCEPVC   CREATIONTIME
# before-upgrade   true         app-data    2s
```

`READYTOUSE true` in a few seconds is normal.

## Note

**You cannot restore on top of the old volume.** A restore always makes a new
one. Move your workload, then delete the old PVC when you are happy.

A restore copies files one by one. Qumulo has no tree-copy API.
So a big volume takes time. The PVC may need a few tries before it binds.

The limit is 50,000 entries (files and directories together) and 2 TiB. Above that, the restore is refused before anything is copied.

The volume is never half-restored. The driver builds it under a hidden name
first, then renames it.

Deleting the VolumeSnapshot also deletes the snapshot on the cluster.
Use `deletionPolicy: Retain` if you do not want that.

Files: [`deploy/use-cases/04-snapshot-restore/`](.)
