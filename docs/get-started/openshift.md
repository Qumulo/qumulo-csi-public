# Install on OpenShift

The driver installs on OpenShift with the same `helm install` as on any other
cluster. There is no SCC step: the chart makes the security settings the node
plugin needs and grants them to the node plugin alone.

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
| Node plugin | its own SCC, `<namespace>-<release>-qumulo-csi-driver-node` | It mounts NFS on the host, so it must be privileged, on the host network and use host paths. The SCC allows that and nothing more, and only the node service account may use it. |

Each pod names its SCC (`openshift.io/required-scc`), so OpenShift never picks
another SCC that a cluster admin granted for something else.

The exact permission set, and how to hand SCC management to a cluster admin,
are in [Operate on OpenShift](../operate/openshift-operations.md#audit-what-the-charts-scc-allows).

## Install

```bash
oc new-project qumulo-csi
helm install qumulo-csi oci://ghcr.io/qumulo/charts/qumulo-csi-driver \
  --version 0.1.0+<commit> -n qumulo-csi -f values.yaml
```

That is all. When `helm template` renders for GitOps, it cannot see the
cluster, so set `openshift.enabled: true` there.

## Check the install

```bash
oc -n qumulo-csi get pods \
  -o custom-columns='NAME:.metadata.name,SCC:.metadata.annotations.openshift\.io/scc'
```

The controller pods show `restricted-v2`; the node pods show the chart's SCC.
The install notes (`helm status qumulo-csi -n qumulo-csi`) print both commands
with your names filled in.

!!! note "Node pods may be refused for a few seconds"
    On a fresh project the DaemonSet can be created before the SCC and its
    grant are in effect. The DaemonSet retries and the pods are admitted.
    See [The SCC settles after the install starts](../operate/openshift-operations.md#the-scc-settles-after-the-install-starts).

## The node plugin binds a host port

The node DaemonSet runs with `hostNetwork: true`, so its livenessprobe's
`/healthz` port is a **host** port on every worker:

```yaml
node:
  healthPort: 29653     # the default
```

It must not collide with anything else on your workers, and 29653 is also the
upstream `csi-driver-nfs` node health port — a cluster running both needs a
different number here. The chart's node SCC allows host ports; an SCC supplied
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

## Disconnected clusters and mirrored registries

A disconnected OpenShift cluster pulls the chart and both images from your
mirror. Follow [Install in an air-gapped environment](air-gapped.md) for the
mirroring itself, then tell the cluster where the images moved to:

```yaml
apiVersion: config.openshift.io/v1
kind: ImageDigestMirrorSet
metadata:
  name: qumulo-csi
spec:
  imageDigestMirrors:
    - source: ghcr.io/qumulo
      mirrors:
        - mirror.example.com/qumulo
```

`ImageContentSourcePolicy` works on clusters that still use it; both are applied
by the Machine Config Operator and roll the nodes.

The chart itself is pulled by Helm, not by the kubelet, so a mirror set does not
redirect it — give `helm install` the mirrored OCI reference directly.

!!! note "The mirror's own CA"
    If your mirror presents a certificate signed by the cluster's trusted CA,
    the kubelet already trusts it. That is separate from
    `openshift.trustedCABundle`, which is only about the controller's calls to
    the **Qumulo REST API**.

## Upgrade from a release made before OpenShift support

`helm upgrade --reuse-values` works. The node pods of the old release never
started on OpenShift; after the upgrade they start without any SCC step.

An upgrade that fails part-way needs `--force` to repair — see
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
