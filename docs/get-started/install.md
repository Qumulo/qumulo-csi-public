# Install the driver

The supported installation path. You need access to the published chart and
its two images, all on the GitHub Container Registry; you do not need to clone
this repository, build anything or install a Go toolchain.

Before starting, work through [Prerequisites](prerequisites.md).

!!! note "On OpenShift, follow this page and then [Install on OpenShift](openshift.md)"
    The steps are the same — `oc new-project` in place of
    `kubectl create namespace`, and `oc` for `kubectl` throughout. The
    OpenShift page covers only what differs: SCC admission, the host port,
    the cluster proxy and the trusted CA bundle.

Work through the steps below in order. Lines you must edit are marked
**`# <-- EDIT`**, and anything in angle brackets — `<github-user>`,
`<commit>`, `<api-token>` — is a value you replace, brackets included.

## 1. Create the namespace

Skip this if you already created it along with the token Secret in
[Prerequisites](prerequisites.md#secure-access).

```bash
kubectl create namespace qumulo-csi
```

## 2. Get access to the images

Each release publishes a chart and two images to the GitHub Container Registry.
All three carry the build they came from, `<commit>` below. Qumulo gives you
that version with the release; if you do not have it, ask through
[care.qumulo.com](https://care.qumulo.com).

| What | Where | Version |
|---|---|---|
| Chart | `oci://ghcr.io/qumulo/charts/qumulo-csi-driver` | `0.1.0+<commit>` |
| Controller image | `ghcr.io/qumulo/qumulo-csi-driver-controller` | `0.1.0-<commit>` |
| Node image | `ghcr.io/qumulo/qumulo-csi-driver-node` | `0.1.0-<commit>` |

The chart pulls the images of its own build, so you never set an image tag.
All three are visible to members of the Qumulo GitHub organization only, so
every node pulls with a credential.

- **Create a GitHub token.** A *classic* personal access token with only the
  **`read:packages`** scope. Then use **Configure SSO** on that token and
  authorize it for the `Qumulo` organization — an unauthorized token is
  refused with 403 even though its scope is right.

- **Save it** without it reaching the screen or shell history:

    ```bash
    read -rsp 'GitHub token: ' T && printf '%s' "$T" > ~/.ghcr-pat && chmod 600 ~/.ghcr-pat && unset T
    ```

- **Create the pull secret** in the release namespace:

    ```bash
    kubectl -n qumulo-csi create secret docker-registry ghcr-pull \
      --docker-server=ghcr.io \
      --docker-username=<github-user> \
      --docker-password="$(cat ~/.ghcr-pat)"
    ```

- **Log Helm in**, so it can fetch the chart with the same token:

    ```bash
    cat ~/.ghcr-pat | helm registry login ghcr.io -u <github-user> --password-stdin   # <-- EDIT
    ```

Once the pods are running, `rm ~/.ghcr-pat`.

!!! note "The token keeps the driver installable, not running"
    Pods already started are unaffected when it expires, but a restarted or
    newly scheduled pod can no longer pull. Note its expiry.

For a private registry or a disconnected cluster, see
[Install in an air-gapped environment](air-gapped.md).

## 3. Write your values file

This is the only file you author; everything else has a working default. At
install time it holds nothing but the pull secret and a couple of driver
settings — **no Qumulo cluster address, no token and no StorageClass**, which
all come later, when you
[create your first volume](first-volume.md).

```yaml
# qumulo-values.yaml
image:
  # The chart already pulls ghcr.io/qumulo/qumulo-csi-driver-{controller,node}
  # at the chart's version. Set repository only for your own registry.
  pullSecrets:
    - name: ghcr-pull

qumulo:
  # No cluster address or token here: both are set per StorageClass, because
  # one installation serves many Qumulo clusters.
  insecureSkipTlsVerify: false

# node:
#   kubeletDir: /var/lib/rancher/k3s/agent/kubelet    # k3s / microk8s ONLY
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
helm install qumulo-csi oci://ghcr.io/qumulo/charts/qumulo-csi-driver \
  --version 0.1.0+<commit> \
  --namespace qumulo-csi \
  --values qumulo-values.yaml
```

Always pass `--version`: every 0.1.0 build carries the same release number, so
Helm cannot tell which is newest on its own.

!!! warning "Do not install from the `charts/` folder of a checkout"
    That chart is for development. It is always version `0.1.0` whatever was
    built, and asks for the moving `0.1.0` image tag rather than one build, so
    `helm history` cannot tell builds apart and the image can come from a
    different commit than the chart. Install a released chart from the registry
    instead, as shown above.
