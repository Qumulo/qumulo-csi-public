# Qumulo CSI Driver for Kubernetes

![build status](https://github.com/Qumulo/qumulo-csi-public/actions/workflows/ci.yml/badge.svg)

## About

This driver allows Kubernetes to provision persistent volumes on
[Qumulo](https://qumulo.com) storage over NFS. It requires an existing Qumulo
cluster with a directory that is already NFS-exported; each volume is created as
a subdirectory beneath it, or as its own NFS export.

- **CSI plugin name:** `csi.qumulo.com`
- **Project status:** 0.1.2
- **License:** [Qumulo CSI Driver License](LICENSE)

## Documentation

| | |
|---|---|
| New here | [Prerequisites](docs/get-started/prerequisites.md) → [Install the driver](docs/get-started/install.md) → [Create your first volume](docs/get-started/first-volume.md) |
| OpenShift | [Install on OpenShift](docs/get-started/openshift.md), [Operate on OpenShift](docs/operate/openshift-operations.md) |
| What it can do | [Features](docs/features/index.md) |
| Settings | [StorageClass parameters](docs/reference/storageclass-parameters.md), [Chart values](docs/reference/chart-values.md) |
| Something is wrong | [Troubleshoot Guide](docs/operate/troubleshoot-mounts.md), [FAQ](docs/reference/faq.md) |

## Container Images & Kubernetes Compatibility

| Driver Version | Supported K8s Version | Supported OpenShift | CSI Spec | Status |
|---|---|---|---|---|
| 0.1.2 | 1.35+ | OCP 4.22 (RHCOS 9.8) | v1.13.0 | First release built from this repository |
| 0.1.1 | 1.35+ | OCP 4.22 (RHCOS 9.8) | v1.13.0 | Patch release |
| 0.1.0 | 1.35+ | OCP 4.22 (RHCOS 9.8) | v1.13.0 | Initial release |

| Image | Tag | Platforms |
|---|---|---|
| `ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller` | `0.1.2` | `linux/amd64`, `linux/arm64` |
| `ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-node` | `0.1.2` | `linux/amd64`, `linux/arm64` |

The images are visible to members of the Qumulo GitHub organization only;
pulling them needs a pull secret, set up in
[step 2 of the installation guide](docs/get-started/install.md#2-get-access-to-the-images).

## Driver Parameters

Please refer to [`csi.qumulo.com` StorageClass parameters](docs/reference/storageclass-parameters.md).

## Installation

Install the driver on a Kubernetes cluster:

OpenShift uses the same chart and the same `helm install`; the chart renders
its own least-privilege SecurityContextConstraint for the node plugin, so
there is no SCC step. See [Install on OpenShift](docs/get-started/openshift.md)
and [Operate on OpenShift](docs/operate/openshift-operations.md).

<!--- Install by [Helm chart](charts/qumulo-csi-driver/README.md)
-->
- [Install](deploy/example/README.md) — a hands-on walkthrough
<!--## Examples

- [Installation guide](docs/get-started/install.md) — the crisp,
  step-by-step path: requirements, the API token, your values file, TLS,
  install, verify and upgrade
- [Basic usage](deploy/example/README.md) — a hands-on walkthrough, one file
  per step
- [Use cases](deploy/use-cases/USE-CASES.md) — runnable manifests, one folder
  per case:
  [shared storage](deploy/use-cases/01-shared-storage/01-shared-storage.md) ·
  [StatefulSet](deploy/use-cases/02-statefulset/02-statefulset.md) ·
  [volume expansion](deploy/use-cases/03-expand-volume/03-expand-volume.md) ·
  [snapshot and restore](deploy/use-cases/04-snapshot-restore/04-snapshot-restore.md) ·
  [team separation](deploy/use-cases/05-team-separation/05-team-separation.md)
-->
## Troubleshooting

- [Troubleshoot provisioning](docs/provision/troubleshoot.md) and
  [troubleshoot mounts](docs/operate/troubleshoot-mounts.md)
- [Supported features and known limits](docs/reference/supported-features.md) —
  what is supported, and the limits you can hit in a supported configuration

## Community, Discussion, Contribution, and Support

- Report a bug or request a feature:
  [GitHub issues](https://github.com/Qumulo/qumulo-csi-public/issues)
- Qumulo support: [care.qumulo.com](https://care.qumulo.com)

## Links

- [Kubernetes CSI Documentation](https://kubernetes-csi.github.io/docs/)
- [Container Storage Interface (CSI) Specification](https://github.com/container-storage-interface/spec)
- [Qumulo documentation](https://docs.qumulo.com)
