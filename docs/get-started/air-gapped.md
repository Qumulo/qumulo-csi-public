# Install in an air-gapped environment

Installing on a cluster that cannot reach `ghcr.io`. The driver itself needs no
internet access at runtime — it talks only to the Qumulo cluster and the
Kubernetes API — so the whole problem is getting the two images and the chart
inside. All three carry the build they came from, `<commit>` below; keep the
tags identical on your mirror, because the chart asks for the images of its own
build.

!!! note "On a disconnected OpenShift cluster"
    Mirror as described here, then redirect the cluster's pulls with an
    `ImageDigestMirrorSet`. See [Install on OpenShift](openshift.md) for the
    rest of the OpenShift install.

## 1. Mirror the images

From a host that can reach both registries:

```bash
cat ~/.ghcr-pat | docker login ghcr.io -u <github-user> --password-stdin

# <-- EDIT: your registry, and <commit> from the release
docker buildx imagetools create \
  -t registry.example.com/qumulo-csi-driver-controller:0.1.0-<commit> \
  ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller:0.1.0-<commit>

docker buildx imagetools create \
  -t registry.example.com/qumulo-csi-driver-node:0.1.0-<commit> \
  ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-node:0.1.0-<commit>
```

!!! warning "Use `imagetools create`, not pull-and-push"
    `imagetools create` copies the multi-platform image as a whole. `docker
    pull` followed by `docker push` copies only your machine's platform, and an
    arm64 node then fails to start the node plugin with `no match for platform
    in manifest`.

Confirm both platforms arrived:

```bash
docker buildx imagetools inspect registry.example.com/qumulo-csi-driver-node:0.1.0-<commit> \
  | grep -E 'Digest|Platform'
```

### Through a true air gap

Where no host can reach both sides, move an OCI layout on media:

```bash
# outside
skopeo copy --all \
  docker://ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-node:0.1.0-<commit> \
  oci-archive:qumulo-csi-driver-node-0.1.0-<commit>.tar

# inside
skopeo copy --all \
  oci-archive:qumulo-csi-driver-node-0.1.0-<commit>.tar \
  docker://registry.example.com/qumulo-csi-driver-node:0.1.0-<commit>
```

`--all` is what carries every platform. Repeat for the controller image, and
for the chart with `helm pull` (step 4).

## 2. Mirror the sidecar images

The chart's defaults point at `registry.k8s.io`. An air-gapped cluster needs
those mirrored too. List exactly what your values file resolves to rather than
working from memory:

```bash
helm template qumulo-csi oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver \
  --version 0.1.0+<commit> --values qumulo-values.yaml \
  | grep -E '^\s+image:' | sort -u
```

Then override each one:

```yaml
sidecars:
  provisioner:
    image:
      repository: registry.example.com/sig-storage/csi-provisioner
  resizer:
    image:
      repository: registry.example.com/sig-storage/csi-resizer
  nodeDriverRegistrar:
    image:
      repository: registry.example.com/sig-storage/csi-node-driver-registrar
  livenessProbe:
    image:
      repository: registry.example.com/sig-storage/livenessprobe
```

## 3. Point the chart at your registry

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
        tag: "0.1.0-<commit>@sha256:<digest>"
      node:
        tag: "0.1.0-<commit>@sha256:<digest>"
    ```

## 4. Bring the chart in

```bash
# outside: pull the published chart as a file
cat ~/.ghcr-pat | helm registry login ghcr.io -u <github-user> --password-stdin   # <-- EDIT
helm pull oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver --version 0.1.0+<commit> # <-- EDIT

# inside
helm install qumulo-csi ./qumulo-csi-driver-0.1.0+<commit>.tgz \
  --namespace qumulo-csi --values qumulo-values.yaml
```

Pull the released chart rather than packaging the `charts/` folder: a chart
packaged from a checkout is always version `0.1.0` and asks for the moving
`0.1.0` image tag, so it need not match the images you just mirrored.

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
