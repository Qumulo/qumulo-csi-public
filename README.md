# Qumulo CSI Driver

Provision Kubernetes persistent volumes on Qumulo storage over NFS. Each volume
is a directory with an enforced quota, and one release serves any number of
Qumulo clusters.

Supported on Kubernetes 1.35 and later, and on OpenShift 4.22.

## Documentation

This repository holds the driver's documentation. The rendered site is the
best way to read it:

**<https://qumulo.github.io/qumulo-csi-public/>**

| | |
|---|---|
| New here | [Prerequisites](docs/get-started/prerequisites.md) → [Install the driver](docs/get-started/install.md) → [Create your first volume](docs/get-started/first-volume.md) |
| OpenShift | [Install on OpenShift](docs/get-started/openshift.md), [Operate on OpenShift](docs/operate/openshift-operations.md) |
| What it can do | [Features](docs/features/index.md) |
| Settings | [StorageClass parameters](docs/reference/storageclass-parameters.md), [Chart values](docs/reference/chart-values.md) |
| Something is wrong | [Troubleshoot Guide](docs/operate/troubleshoot-mounts.md), [FAQ](docs/reference/faq.md) |

## What is published

The chart and both images are published as packages on this repository:

| | |
|---|---|
| Chart | `oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver` |
| Controller image | `ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller` |
| Node image | `ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-node` |

Pass `--version` on every install: every 0.1.0 build carries the same release
number, so Helm cannot tell which is newest on its own. See
[Install the driver](docs/get-started/install.md) for the exact command.

## Contributing

This repository is a publishing target. The documentation is written in
Qumulo's development repository and copied here, so a change made on this
repository alone is overwritten by the next update. Please open an issue
describing what is wrong or missing rather than a pull request.

## Support

Open a case at [care.qumulo.com](https://care.qumulo.com).

## License

See [LICENSE](LICENSE). The driver is not open-source software; the license
grants use with Qumulo storage and does not permit redistribution or
derivative works.
