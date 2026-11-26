# Use cases

Five things people do with this driver. Each one has a page and runnable files.

Every example was checked with `kubectl apply --dry-run`.

| # | Use case | What it needs |
|---|---|---|
| [1](01-shared-storage/01-shared-storage.md) | Shared storage for many pods | `ReadWriteMany` |
| [2](02-statefulset/02-statefulset.md) | One volume per replica | `volumeClaimTemplates` |
| [3](03-expand-volume/03-expand-volume.md) | Make a volume bigger | `allowVolumeExpansion` |
| [4](04-snapshot-restore/04-snapshot-restore.md) | Save a copy before a risky change | snapshot CRDs |
| [5](05-team-separation/05-team-separation.md) | Give each team its own directory and token | one Secret per team |

## Before you start

The driver must be installed. See [the installation guide](../../docs/get-started/install.md).

Every example uses a StorageClass named `qumulo-nfs41`. Change the name if
yours is different.

Edit the StorageClass before you apply it. You must set:

- `server` — your Qumulo cluster. A bare host name. Not a URL.
- `storeRealPath` — a directory that already exists on the cluster.
- `storeExportPath` — how NFS clients see that directory.

## The example files

```sh
deploy/use-cases/
  01-shared-storage/
    01-shared-storage.md    the page above, and:
    storageclass.yaml, pvc.yaml, deployment.yaml
  02-statefulset/
    02-statefulset.md
    statefulset.yaml
  03-expand-volume/
    03-expand-volume.md
    pvc-before.yaml, pvc-after.yaml
  04-snapshot-restore/
    04-snapshot-restore.md
    volumesnapshotclass.yaml, volumesnapshot.yaml, pvc-from-snapshot.yaml
  05-team-separation/
    05-team-separation.md
    storageclass-team-a.yaml, storageclass-team-b.yaml, resourcequota.yaml
  pod-with-pvc.yaml       a test pod, used by several pages
  storageclass-nfs3.yaml  NFSv3, if NFSv4 is off on your cluster
```

Run `make example-check` to confirm they all still apply.

These live beside the guided walkthrough in
[`deploy/example/`](../example/README.md), which is a single start-to-end
path for a first install. These are one folder per use case, to copy from.

## Which protocol to choose

| | NFSv4.1 | NFSv3 |
|---|---|---|
| use it? | **yes** | only if NFSv4 is off |
| file locks | work between nodes | work inside one node only |
| extra mount options | none | **`nolock` is required** |

NFSv3 without `nolock` fails every mount. The node image runs no `rpc.statd`.

## Watching disk use

The driver reports real numbers per volume. We tested this.
So the normal kubelet metrics work:

```promql
kubelet_volume_stats_available_bytes / kubelet_volume_stats_capacity_bytes < 0.10
```

## More detail

| For | Read |
|---|---|
| every StorageClass parameter | [../../charts/qumulo-csi-driver/README.md](../../charts/qumulo-csi-driver/README.md#storageclass-parameters) |
| open problems and workarounds | [../../docs/reference/supported-features.md](../../docs/reference/supported-features.md) |
| how many clusters work at once | [../../charts/qumulo-csi-driver/README.md](../../charts/qumulo-csi-driver/README.md#serving-two-clusters-from-one-release) |
