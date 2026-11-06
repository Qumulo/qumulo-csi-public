# Storage class

A StorageClass is how you tell Kubernetes about a Qumulo cluster: where it is,
how to log in to it, and which folder to put volumes in. Once a class exists,
people can ask for storage by naming it, and never have to know any of those
details.

You will want **at least one class per Qumulo cluster**.

Classes go in your Helm values file, under `storageClasses`. The pages below
walk through the common setups, one at a time. If you would rather see the
full list of settings with their types and defaults, that is in
[StorageClass parameters](../../reference/storageclass-parameters.md).

## In this section

- [A complete values file](values-file.md) — a whole values file, so you can see where a class fits.
- [Shared subdirectory — the default](shared-subdirectory.md) — the simple setup: one shared folder, a sub-folder per volume.
- [Dedicated export — one export per volume](dedicated-export.md) — a separate NFS share per volume, when you need access control.
- [Dedicated export on a network tenant](network-tenant.md) — for a cluster split into separate team networks.
- [Scheduled snapshots on every volume](scheduled-snapshots.md) — automatic snapshots on a schedule you choose.
- [Two Qumulo clusters from one release](two-clusters.md) — one installation, as many clusters as you like.
