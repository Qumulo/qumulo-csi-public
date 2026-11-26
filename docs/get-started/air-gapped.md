# Install in an air-gapped environment

Installing on a cluster that cannot reach `ghcr.io`. The driver itself needs no
internet access at runtime — it talks only to the Qumulo cluster and the
Kubernetes API — so the whole problem is getting the two images and the chart
inside. All three are named after the release, `0.1.2` below; keep the tags
identical on your mirror, because the chart asks for the images of its own
release.

!!! note "On a disconnected OpenShift cluster"
    Mirror as described here, then redirect the cluster's pulls with an
    `ImageDigestMirrorSet`. See [Install on OpenShift](openshift.md) for the
    rest of the OpenShift install.

## 1. Mirror the images

From a host that can reach both registries:

```bash
# <-- EDIT: your registry
docker buildx imagetools create \
  -t registry.example.com/qumulo-csi-driver-controller:0.1.2 \
  ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller:0.1.2

docker buildx imagetools create \
  -t registry.example.com/qumulo-csi-driver-node:0.1.2 \
  ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-node:0.1.2
```

!!! warning "Use `imagetools create`, not pull-and-push"
    `imagetools create` copies the multi-platform image as a whole. `docker
    pull` followed by `docker push` copies only your machine's platform, and an
    arm64 node then fails to start the node plugin with `no match for platform
    in manifest`.

Confirm both platforms arrived:

```bash
docker buildx imagetools inspect registry.example.com/qumulo-csi-driver-node:0.1.2 \
  | grep -E 'Digest|Platform'
```

### Through a true air gap

Where no host can reach both sides, move an OCI layout on media:

```bash
# outside
skopeo copy --all \
  docker://ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-node:0.1.2 \
  oci-archive:qumulo-csi-driver-node-0.1.2.tar

# inside
skopeo copy --all \
  oci-archive:qumulo-csi-driver-node-0.1.2.tar \
  docker://registry.example.com/qumulo-csi-driver-node:0.1.2
```

`--all` is what carries every platform. Repeat for the controller image, for
every sidecar image ([step 3](#3-mirror-the-sidecar-images)), and for the chart
with `helm pull` ([step 2](#2-bring-the-chart-in)).

## 2. Bring the chart in

```bash
# outside: pull the published chart as a file
helm pull oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver --version 0.1.2 # <-- EDIT
```

Carry `qumulo-csi-driver-0.1.2.tgz` inside with the images. Quote the
`--version` explicitly: without it Helm resolves whichever version the
registry happens to serve, which need not be the one you mirrored.

Pull the released chart rather than packaging the `charts/` folder, so the
chart you install is exactly the one that was released with the images you
just mirrored.

## 3. Mirror the sidecar images

Most of the chart's sidecar defaults point at `registry.k8s.io`, but not all of
them: the csi-addons sidecar comes from `quay.io`. An air-gapped cluster needs
every one of them mirrored. List exactly what your values file resolves to
rather than working from memory.

Do this with the chart you have already brought inside ([step 2](#2-bring-the-chart-in)),
so the listing itself needs no network:

```bash
helm template qumulo-csi ./qumulo-csi-driver-0.1.2.tgz \
  --values qumulo-values.yaml \
  | grep -E '^\s+image:' | sort -u
```

Mirror each image the listing names, with the same `imagetools create` or
`skopeo copy --all` commands used for the driver images in
[step 1](#1-mirror-the-images). These are the chart's defaults for this
release:

| Sidecar | Default image |
| --- | --- |
| `provisioner` | `registry.k8s.io/sig-storage/csi-provisioner:v5.3.0` |
| `resizer` | `registry.k8s.io/sig-storage/csi-resizer:v1.14.0` |
| `nodeDriverRegistrar` | `registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.14.0` |
| `livenessProbe` | `registry.k8s.io/sig-storage/livenessprobe:v2.16.0` |
| `snapshotter` | `registry.k8s.io/sig-storage/csi-snapshotter:v8.2.0` |
| `healthMonitor` | `registry.k8s.io/sig-storage/csi-external-health-monitor-controller:v0.19.0` |
| `csiAddons` | `quay.io/csiaddons/k8s-sidecar:v0.15.0` |

Then override each one. Each `image` is a **single, complete reference
string**, not an object:

```yaml
sidecars:
  provisioner:
    image: registry.example.com/sig-storage/csi-provisioner:v5.3.0
  resizer:
    image: registry.example.com/sig-storage/csi-resizer:v1.14.0
  nodeDriverRegistrar:
    image: registry.example.com/sig-storage/csi-node-driver-registrar:v2.14.0
  livenessProbe:
    image: registry.example.com/sig-storage/livenessprobe:v2.16.0
  snapshotter:
    image: registry.example.com/sig-storage/csi-snapshotter:v8.2.0
  healthMonitor:
    image: registry.example.com/sig-storage/csi-external-health-monitor-controller:v0.19.0
  csiAddons:
    image: registry.example.com/csiaddons/k8s-sidecar:v0.15.0
```

!!! warning "Keep the tag on a sidecar image"
    A sidecar's `image` is used exactly as written. Unlike the driver images,
    nothing fills in a tag from the chart's version, so
    `registry.example.com/sig-storage/csi-provisioner` with no tag resolves to
    `:latest` — a tag your mirror almost certainly does not have, and the
    sidecar then fails to pull.

!!! tip "Override only the sidecars you run"
    `snapshotter`, `healthMonitor` and `csiAddons` run only when their features
    are enabled. Mirroring them anyway costs little and means enabling a
    feature later does not strand you without an image.

## 4. Point the chart at your registry

```yaml
image:
  controller:
    repository: registry.example.com/qumulo-csi-driver-controller
  node:
    repository: registry.example.com/qumulo-csi-driver-node
  pullSecrets:
    - name: internal-registry
```

!!! tip "Pin the digest, not just the tag"
    A mirrored tag can be overwritten. Pinning makes an install reproducible:

    ```yaml
    image:
      controller:
        tag: "0.1.2@sha256:<digest>"
      node:
        tag: "0.1.2@sha256:<digest>"
    ```

## 5. Private CA

An air-gapped estate usually has its own CA for the Qumulo cluster's
certificate. That is fully supported and needs no public CA:

```bash
kubectl -n qumulo-csi create configmap qumulo-ca --from-file=ca.crt=ca.crt
```

```yaml
qumulo:
  insecureSkipTlsVerify: false
  caBundle:
    existingConfigMap: qumulo-ca
```

The chart mounts the ConfigMap on the controller and sets `SSL_CERT_FILE`.
The node plugin never calls the REST API, so it gets no CA mount.

## 6. Install

```bash
helm install qumulo-csi ./qumulo-csi-driver-0.1.2.tgz \
  --namespace qumulo-csi --values qumulo-values.yaml
```

## What still needs no network

| Thing | Reachability needed |
|---|---|
| Controller | Qumulo REST API (HTTPS), Kubernetes API |
| Node plugin | Qumulo NFS, the kubelet socket |
| Snapshots, replication | the relevant CRDs, installed from your own mirror |
| Metrics | your in-cluster Prometheus only |

## Next

- [Verify the installation](verify.md)
- [Image pull failures](../provision/troubleshoot.md#image-pull-failures)
