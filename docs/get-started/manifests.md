# Install from rendered manifests

!!! note "Helm is the supported installation path"
    This project ships **no hand-maintained plain-YAML bundle**. The chart is
    the single source of truth for every object the driver needs, and several
    install-time safety checks — the unknown-parameter check, the open-export
    check, the immutable-field preview — live in the chart's templates. A
    hand-written manifest set has none of them.
    See [Install the driver](install.md).

If your environment cannot run Helm against the cluster — a GitOps repository
that stores rendered YAML, a change process that reviews manifests, or a
cluster where Tiller-style access is not permitted — render the chart locally
and apply the output.

!!! warning "On OpenShift, set `openshift.enabled: true` before rendering"
    `openshift.enabled: auto` detects OpenShift from the live cluster, and
    `helm template` sees no cluster. Left on `auto`, the rendered set contains
    no SecurityContextConstraint, the node DaemonSet creates no pods, and
    nothing reports an error. See [Install on OpenShift](openshift.md).

## Render the manifests

```bash
# <-- EDIT: --version is the release's, from Qumulo or the chart package page
cat ~/.ghcr-pat | helm registry login ghcr.io -u <github-user> --password-stdin

helm template qumulo-csi oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver \
  --version 0.1.0+<commit> \
  --namespace qumulo-csi \
  --values qumulo-values.yaml \
  > qumulo-csi.yaml
```

Render the published chart, not the `charts/` folder of a checkout: the folder
chart asks for the moving `0.1.0` image tag, so the manifests you commit would
not name one build.

Review, commit, and apply:

```bash
kubectl create namespace qumulo-csi
kubectl -n qumulo-csi apply -f qumulo-csi.yaml
```

The pull secret and the API token Secret are **not** in that output — the chart
renders no Secret. Create them first, as in
[Install with Helm](install.md#2-get-access-to-the-images).

## What `helm template` cannot check

`helm template` has no cluster to ask, so these run only during a real
`helm install` or `helm upgrade --dry-run=server`:

| Check | What it would have caught |
|---|---|
| Immutable-field preview | an upgrade that changes a StorageClass's `parameters`, `provisioner`, `reclaimPolicy` or `volumeBindingMode`, a PriorityClass's `value`, or the CSIDriver's `attachRequired` — the API server refuses these, and the apply fails part-way |
| CRD presence | `volumeSnapshotClasses` or `volumeReplicationClasses` rendered into a cluster that does not serve the API version |
| Prometheus Operator CRD | a `ServiceMonitor` or `PrometheusRule` whose kind is unknown fails the whole apply |

!!! warning "Rendering metrics objects into a GitOps repository"
    `ServiceMonitor` and `PrometheusRule` are skipped by `helm template`
    because the CRD check cannot run. Set `metrics.forceEnable: true` to render
    them anyway, and make sure the Prometheus Operator CRDs really are present
    on the target cluster.

## Keep the rendered output in step

Re-render from the same values file on every upgrade, and diff before applying:

```bash
helm template qumulo-csi oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver \
  --version 0.1.0+<commit> \
  --namespace qumulo-csi --values qumulo-values.yaml > qumulo-csi.new.yaml
diff -u qumulo-csi.yaml qumulo-csi.new.yaml
```

!!! warning "`kubectl apply` will not delete what you stopped rendering"
    Objects removed from the manifest are left on the cluster. Use
    `kubectl diff -f` and prune deliberately. Note that StorageClasses,
    VolumeSnapshotClasses and VolumeReplicationClasses carry
    `helm.sh/resource-policy: keep` precisely because they outlive the release —
    deleting one while PVs still reference it strands those volumes.

## Objects the manifest set contains

| Object | Purpose |
|---|---|
| `Deployment` (controller) | driver + `csi-provisioner` + `csi-resizer` + `livenessprobe` |
| `DaemonSet` (node) | driver + `node-driver-registrar` + `livenessprobe` |
| `PriorityClass` | so ordinary workloads cannot preempt the plugin that unmounts their volumes |
| `CSIDriver` | `attachRequired: false`, `podInfoOnMount: false` |
| RBAC | one ServiceAccount per role; the node's has no permissions |
| `StorageClass` (per class) | what pods request |
| `VolumeSnapshotClass`, `VolumeReplicationClass` | only when enabled |

!!! note "Installing without `priorityclasses` rights"
    The PriorityClass is cluster-scoped, so installing and upgrading need
    `get`, `create` and `update` on `priorityclasses`. Without them, set
    `node.priorityClass.create: false`, or point `node.priorityClassName` at a
    class that already exists, such as `system-node-critical`.

## Next

- [Verify the installation](verify.md)
- [Chart values](../reference/chart-values.md)
