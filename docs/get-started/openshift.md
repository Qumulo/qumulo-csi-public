# Install on OpenShift

The driver installs on OpenShift with the same `helm install` as on any other
cluster. There is no SCC step: the chart makes the security settings the node
plugin needs and grants them to the node plugin alone.

This page covers installing from the **public registry**, which is how the
chart and images are distributed: the cluster pulls them straight from the
internet, with no mirror and no pull secret.

Work through [Install the driver](install.md) first. This page lists only what
is different on OpenShift. For day-two work — auditing the SCC, upgrades,
proxy and CA failures, monitoring and uninstall — see
[Operate on OpenShift](../operate/openshift-operations.md).

## Supported versions

| | Supported |
|---|---|
| OpenShift Container Platform | 4.22 |
| Kubernetes | 1.35 and later |
| Node operating system | RHCOS 9.8 |
| NFS | 4.1 and 3 |
| Architectures | `x86_64`, `aarch64` |
| Driver | 0.1.0 |

!!! warning "OpenShift 4.21 and older are not supported"
    The chart enforces `kubeVersion: ">=1.35.0-0"`, and OpenShift 4.21 ships
    Kubernetes 1.34. `helm install` refuses the chart rather than installing
    something that cannot work.

Validated on 28 October 2026 against OpenShift 4.22 with SELinux enforcing,
on both NFS 4.1 and NFS 3.

## What the chart does on OpenShift

OpenShift admits every pod through a SecurityContextConstraint (SCC). The chart
finds OpenShift by itself (`openshift.enabled: auto`) and then:

| Pod | SCC | Why |
|---|---|---|
| Controller | `restricted-v2`, OpenShift's default | It only talks HTTPS to the Qumulo API. The chart drops its fixed user ID so OpenShift can give it one from the project's range. |
| Node plugin | its own SCC, `qumulo-csi-qumulo-csi-driver-node` for the install above (in general `<namespace>-<release>-qumulo-csi-driver-node`; the namespace is left out when the release is named after it) | It mounts NFS on the host, so it must be privileged, on the host network and use host paths. The SCC allows that and nothing more, and only the node service account may use it. |

Each pod names its SCC (`openshift.io/required-scc`), so OpenShift never picks
another SCC that a cluster admin granted for something else.

The exact permission set, and how to hand SCC management to a cluster admin,
are in [Operate on OpenShift](../operate/openshift-operations.md#audit-what-the-charts-scc-allows).

## Before you start

The chart and both images are pulled from a public registry, so there is no
pull secret and no mirroring step. What the cluster needs:

- The workers can reach the registry. An OpenShift cluster behind a proxy
  reaches it through the cluster-wide Proxy, which the kubelet already uses —
  that is separate from the controller's own proxy settings, below.
- The machine running `helm` can reach the same registry. Helm pulls the
  chart itself; the kubelet pulls the images.
- A project to install into, and a `values.yaml` with your Qumulo cluster
  and StorageClass settings, from [Install the driver](install.md).

## Install

```bash
oc new-project qumulo-csi

helm install qumulo-csi \
  oci://<public-registry>/<org>/charts/qumulo-csi-driver \
  --version 0.1.0+<commit> -n qumulo-csi -f values.yaml
```

Qumulo support gives you the registry and the exact `--version` for your
release.

The chart published to the public registry carries the public image
repositories as its defaults, so nothing in `values.yaml` points at images:
leave `image.controller.repository`, `image.node.repository` and
`image.pullSecrets` unset. Confirm what the install will pull before you run
it:

```bash
helm show values oci://<public-registry>/<org>/charts/qumulo-csi-driver \
  --version 0.1.0+<commit> | grep -A2 'repository:'
```

That is all there is to it. There is no SCC step: the chart creates the node
plugin's SCC and grants it to the node service account.

!!! note "Rendering offline for GitOps"
    `helm template`, Argo CD and any other render that cannot see the cluster
    does not detect OpenShift, so the SCC is silently left out. Set
    `openshift.enabled: true` in `values.yaml` for those.

## Check the install

```bash
oc -n qumulo-csi get pods \
  -o custom-columns='NAME:.metadata.name,SCC:.metadata.annotations.openshift\.io/scc'
```

The controller pods show `restricted-v2`; the node pods show the chart's SCC.
The install notes (`helm status qumulo-csi -n qumulo-csi`) print both commands
with your names filled in.

Then run the chart's tests:

```bash
helm test qumulo-csi -n qumulo-csi
```

The readiness test always runs: it checks that the controller is Available,
every node pod is Ready and the driver is registered on the nodes, using
Red Hat's `ose-cli` image under the controller's SCC (`restricted-v2` unless
you set `openshift.controllerSCC`). To also prove a volume works end to end,
set `tests.storageClass` to a class first: a pod under the same SCC then
writes a file to a 1 GiB volume on it and reads it back. That pod runs only
on nodes the node plugin runs on (it takes `node.nodeSelector`,
`node.tolerations` and `node.affinity`). The class must
let a non-root user write (`mountPermissions: "0777"`, the default). A pass
deletes the test pods, so leave out `--logs` (Helm would find no log and exit
1). A failure keeps the pod: `oc -n qumulo-csi logs <pod>` shows why.

!!! note "Node pods may be refused for a few seconds"
    On a fresh project the DaemonSet can be created before the SCC and its
    grant are in effect. The DaemonSet retries and the pods are admitted.
    See [The SCC settles after the install starts](../operate/openshift-operations.md#the-scc-settles-after-the-install-starts).

## Clusters with their own SCCs and a policy engine

Some clusters do not let workloads use `restricted-v2`, do not let a workload
chart create SCCs, or use a policy engine such as Gatekeeper so that only a
platform team may create RoleBindings, ClusterRoleBindings or Helm's release
Secrets. The chart supports all of these. A full example is in
`test/openshift/values/openshift-values-gatekeeper.yaml`:

```yaml
openshift:
  enabled: true
  # Your cluster's restricted SCC. A name starting with "restricted" makes
  # the chart leave the controller's uid out, so OpenShift assigns one from
  # the namespace's range. For another name that also assigns a uid, add
  # assignControllerUID: true.
  controllerSCC: restricted-csi
  nodeSCC:
    create: false            # your platform team makes and grants it
    name: qumulo-csi-node
rbac:
  createBindings: false      # your platform team applies every binding
node:
  nodeSelector:
    hydra_storage: "true"    # quoted: a node selector value is a string
controller:
  nodeSelector:
    hydra_storage: "true"
```

Before the install, the platform team:

1. Creates the node SCC and grants it to the node service account
   ([Let your cluster admin manage the SCC](../operate/openshift-operations.md#let-your-cluster-admin-manage-the-scc)).
2. Applies the bindings the chart would have made
   ([Bindings applied by a platform team](../operate/openshift-operations.md#bindings-applied-by-a-platform-team)).

Then install as usual. If Helm's release Secrets are also blocked, install
with `HELM_DRIVER=configmap`
([Helm release storage](../operate/openshift-operations.md#helm-release-storage)).
The install stops before it changes anything, naming each binding that is
missing or wrong.

!!! warning "Use a values file or `--set-string` for node selectors"
    `--set node.nodeSelector.hydra_storage=true` makes the value a boolean,
    which the API server refuses. Quote it in a values file, as above, or use
    `--set-string`.

## The node plugin binds a host port

The node DaemonSet runs with `hostNetwork: true`, so the livenessprobe's and
the registrar's `/healthz` ports are **host** ports on every worker:

```yaml
node:
  healthPort: 29653            # the default
  registrarHealthPort: 0       # the default: healthPort + 6, so 29659
```

They must not collide with anything else on your workers. A second release of
this chart on the same cluster needs its own `healthPort` (and, with metrics
on, its own `metrics.port.node`); its registrar port then follows as
`healthPort + 6`. The install or upgrade is refused, naming the DaemonSet
that holds the port, if another DaemonSet already uses one of these ports.
29653 is also the upstream `csi-driver-nfs` node health port — a cluster
running both needs a different number there. The chart's node SCC allows host ports; an SCC supplied
by your cluster admin must do the same.

## Clusters behind a proxy

Pods installed with Helm do not get the cluster's proxy settings. Read them
from the cluster and give them to the controller:

```bash
oc get proxy cluster -o jsonpath='{.status}'
```

```yaml
controller:
  proxy:
    httpsProxy: http://proxy.example.com:3128
    noProxy: 10.0.0.13        # arrays reached directly, if any
```

The Kubernetes API, `.svc` and `.cluster.local` are always added to the
no-proxy list.

## Trust the cluster's CA bundle

If the Qumulo cluster's certificate is signed by a CA your OpenShift cluster
already trusts (its Proxy `trustedCA`), set `openshift.trustedCABundle: true`.
OpenShift fills a ConfigMap the chart creates, and the controller uses it. This
replaces `qumulo.caBundle`; do not set both.

## Upgrade from a release made before OpenShift support

`helm upgrade --reuse-values` works. The node pods of the old release never
started on OpenShift; after the upgrade they start without any SCC step.

An upgrade that fails part-way needs `--force` to repair
(`--force-replace --server-side=false` for a release Helm 4 installed), or a
delete-and-upgrade by hand — see
[If an upgrade fails part-way](../operate/openshift-operations.md#if-an-upgrade-fails-part-way).

## A StorageClass names a Secret that does not exist

The controller may read only the Secrets its classes name, so for any other name
the API answers "forbidden", not "not found". A claim on a class whose Secret
name is misspelt stays Pending with an event like
`secrets "<name>" is forbidden: ... cannot get resource "secrets"`. Check the
Secret's name and namespace in the class.

## What application pods need

Tested on 28 Oct 2026 with NFS 4.1 and NFS 3, one claim per case, on
OpenShift 4.22 with SELinux enforcing.

**No special settings for most apps.** A pod under OpenShift's default SCC,
`restricted-v2`, runs with a random user ID and group 0. It can read and write
a Qumulo volume with the default class settings. It needs no `fsGroup`, no
`supplementalGroups` and no SELinux option. Two pods with different SELinux
labels can share one volume.

**The folder permission decides who can write.** Set it per StorageClass with
`mountPermissions`:

| `mountPermissions` | `restricted-v2` (random user ID) | `nonroot-v2` (fixed non-root user) | `anyuid` or `privileged` (root) |
|---|---|---|---|
| `0777` (default) | read and write | read and write | read and write |
| `0770` | read and write (group 0) | read and write (group 0) | read and write |
| `0750` | read only | read only | read and write |

The driver sets `fsGroupPolicy: None`, so Kubernetes never changes file
owners. A pod's `fsGroup` does not grant it access: with `0750`, a pod with
`fsGroup` still cannot write. Root pods write as root on the array unless the
export squashes root.

**User-namespace pods cannot use NFS volumes.** A pod with `hostUsers: false`
(for example under `restricted-v3`) fails to start:
`mount_setattr ... doesn't support idmap mounts on this kernel`. The RHCOS 9.8
kernel cannot ID-map NFS mounts. This is a platform limit for every NFS volume,
not only Qumulo's. Run such apps with `hostUsers` unset.

**Read-only volumes.** Set `readOnly: true` on the pod's volume or
`volumeMounts`. A class that only adds `ro` to `mountOptions` is refused on
RHCOS; see [Read-only volumes](../features/mount-options.md#read-only-volumes).

**Single-writer volumes.** `ReadWriteOnce` does not stop a second pod on
another node from writing — this driver has no attach stage, so Kubernetes does
not enforce it. Use `ReadWriteOncePod`, which Kubernetes enforces itself. See
[Access modes](../features/access.md#share-one-volume-between-many-pods).

## Next

- [Operate on OpenShift](../operate/openshift-operations.md) — SCC auditing, upgrades, monitoring, uninstall
- [Chart values: OpenShift](../reference/chart-values.md#openshift) — every `openshift.*` value
- [Security](../features/security.md#openshift-scc-admission) — why the node plugin is privileged and the controller is not
