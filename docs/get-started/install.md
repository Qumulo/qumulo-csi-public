# Install the driver

The supported installation path. The chart and its two images are published on
the GitHub Container Registry; you do not need to clone this repository, build
anything or install a Go toolchain.

Before starting, work through [Prerequisites](prerequisites.md).

!!! note "On OpenShift, follow this page and then [Install on OpenShift](openshift.md)"
    The steps are the same — `oc new-project` in place of
    `kubectl create namespace`, and `oc` for `kubectl` throughout. The
    OpenShift page covers only what differs: SCC admission, the host port,
    the cluster proxy and the trusted CA bundle.

Work through the steps below in order. Lines you must edit are marked
**`# <-- EDIT`**, and anything in angle brackets — `<github-user>`,
`<api-token>` — is a value you replace, brackets included.

## 1. Create the namespace

Skip this if you already created it along with the token Secret in
[Prerequisites](prerequisites.md#secure-access).

```bash
kubectl create namespace qumulo-csi
```

## 2. The published chart and images

Each release publishes a chart and two images to the GitHub Container Registry.
All three are named after the release — `0.1.2` — and nothing else. Qumulo
gives you that version with the release; if you do not have it, ask through
[care.qumulo.com](https://care.qumulo.com).

| What | Where | Version |
|---|---|---|
| Chart | `oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver` | `0.1.2` |
| Controller image | `ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller` | `0.1.2` |
| Node image | `ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-node` | `0.1.2` |

All three are public: there is no registry login, no pull secret and no
mirroring step. The chart carries those image repositories as its defaults and
pulls the images of its own release, so you set neither a repository nor a tag.

Two machines need to reach `ghcr.io`: the one running `helm`, which pulls the
chart, and every worker node, which pulls the images.

For a private registry or a disconnected cluster, see
[Install in an air-gapped environment](air-gapped.md).

## 3. Write your values file

This is the only file you author; everything else has a working default. At
install time it holds a couple of driver settings and nothing more — **no
Qumulo cluster address, no token and no StorageClass**, which all come later,
when you [create your first volume](first-volume.md).

```yaml
# values.yaml
qumulo:
  # No cluster address or token here: both are set per StorageClass, because
  # one installation serves many Qumulo clusters.
  insecureSkipTlsVerify: false

# The chart's default kubelet directory is /var/lib/kubelet, which is correct
# on most distributions, k3s included. MicroK8s is the exception:
# node:
#   kubeletDir: /var/snap/microk8s/common/var/lib/kubelet   # MicroK8s only
```

Every value: [Chart values](../reference/chart-values.md).

!!! note "Installing without a StorageClass is normal"
    The driver installs and runs perfectly well with no storage class. It just
    has nothing to do yet — a volume request would wait until you add one.
    Adding it later is a plain `helm upgrade`; nothing restarts.

### TLS

Qumulo clusters ship a self-signed certificate with subject `CN=localhost` and
no SAN, which Go cannot verify — installing it as a trusted root does not help.
So either replace the cluster certificate and give the controller its CA, or,
for an evaluation only, set `qumulo.insecureSkipTlsVerify: true` to disable
verification on API traffic. The NFS data path is unencrypted either way.

Either way, the cluster certificate must be replaced before TLS will
verify.

### If you want snapshots

Snapshots are off by default, and turning them on takes three things: the
snapshot CRDs already installed in the cluster, the snapshotter sidecar, and a
`VolumeSnapshotClass`.

!!! warning "Turn the sidecar on only once the CRDs are there"
    Without them the controller pod never becomes Ready, which stops ordinary
    provisioning as well — so this is worth doing deliberately, not in passing.


## 4. Install the Driver using Helm chart

Install the published chart, at the version from *step 2*. It pulls exactly the
images it was built with.

```bash
# <-- EDIT: --version is the one from step 2
helm install qumulo-csi oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver \
  --version 0.1.2 \
  --namespace qumulo-csi \
  --values values.yaml
```

Always pass `--version`, so the release you install is the one you chose rather
than whichever is newest at the time.

!!! warning "Install the published chart, not the `charts/` folder of a checkout"
    If you have a clone of the source, `charts/qumulo-csi-driver` is the
    development chart. It follows the branch you checked out, which can be
    ahead of or behind any release, so what it installs is not a release and
    `helm history` cannot tell one checkout from another. The published chart
    in the table above is the released artifact, and `--version` names it.
