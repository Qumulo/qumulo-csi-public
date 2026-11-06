# Operate on OpenShift

Day-two tasks that differ on OpenShift: checking which SCC admitted each pod,
auditing what the chart's SCC asks for, handing SCC management to a cluster
admin, upgrades, and the proxy and CA failures that look like driver faults but
are not.

Install first — see [Install on OpenShift](../get-started/openshift.md).

Everything on this page was exercised on OpenShift Container Platform 4.22
with RHCOS 9.8 workers.

## Which SCC is each pod using

This is the first check for anything that looks like an admission problem.

```bash
oc -n qumulo-csi get pods \
  -o custom-columns='NAME:.metadata.name,SCC:.metadata.annotations.openshift\.io/scc'
```

```console
NAME                                        SCC
qumulo-csi-qumulo-csi-driver-abc12-xxxxx    restricted-v2
qumulo-csi-qumulo-csi-driver-abc12-yyyyy    restricted-v2
qumulo-csi-qumulo-csi-driver-node-2k8vp     qumulo-csi-qumulo-csi-driver-node
```

Controller pods must show `restricted-v2` (or whatever `openshift.controllerSCC`
names). Node pods must show the chart's SCC, `<namespace>-<release>-qumulo-csi-driver-node`.

| What you see | What it means |
|---|---|
| Node pods show `privileged` | The chart's SCC was not matched and a broader one admitted the pod. Usually `openshift.nodeSCC.create: false` with no `nodeSCC.name`, or an admin grant of `privileged` to the namespace's default service account. |
| No node pods at all | `openshift.enabled` is `false` on a cluster that enforces SCCs. The DaemonSet creates nothing and `helm install` still reports success. The install notes print a warning for exactly this case. |
| Controller pods `CreateContainerConfigError` | The controller kept its fixed uid 65532, which `restricted-v2` refuses. Normally a part-failed upgrade — see [If an upgrade fails](#if-an-upgrade-fails-part-way). |

Each pod also carries `openshift.io/required-scc`, so admission never falls
through to another SCC a cluster admin happened to grant for something else.
If the annotation and the admitted SCC disagree, the pod is not admitted at all.

Confirm the node service account may use its SCC:

```bash
oc -n qumulo-csi auth can-i use securitycontextconstraints/qumulo-csi-qumulo-csi-driver-node \
  --as=system:serviceaccount:qumulo-csi:qumulo-csi-qumulo-csi-driver-node
```

## Audit what the chart's SCC allows

```bash
oc describe scc qumulo-csi-qumulo-csi-driver-node
```

The chart asks for exactly what an NFS mount on the host needs, and nothing
more:

| Setting | Value | Why the node plugin needs it |
|---|---|---|
| `allowPrivilegedContainer` | `true` | `mount(8)` in the host mount namespace |
| `allowHostNetwork`, `allowHostPorts` | `true` | the DaemonSet is `hostNetwork: true`; the livenessprobe binds `node.healthPort` |
| `allowHostDirVolumePlugin` | `true` | the kubelet plugin directory and `/var/lib/kubelet` |
| `volumes` | `hostPath`, `emptyDir` only | no `secret`, no `projected`, no `configMap` beyond what the pod needs |
| `runAsUser` | `RunAsAny` | the mount helper runs as root |
| `seccompProfiles` | `runtime/default` | not `unconfined` |
| `allowHostPID`, `allowHostIPC` | `false` | never needed |
| `allowedCapabilities` | not widened | privileged already covers the mount |

It is granted through RBAC to the node service account alone, not to the
namespace and not to `system:authenticated`.

!!! note "Why not the built-in `privileged` SCC"
    It would work, and it grants far more than the node plugin uses — host PID,
    host IPC, every volume plugin, every capability. CIS OpenShift 5.2 asks that
    use of `privileged` be kept minimal, and a cluster that audits SCC bindings
    will flag it. A chart-owned SCC keeps the grant auditable and scoped to one
    service account.

The controller gets no SCC of its own. It only speaks HTTPS to the Qumulo REST
API and touches no filesystem, so `restricted-v2` admits it unchanged once the
chart drops its fixed uid.

## Let your cluster admin manage the SCC

Some organisations do not let a workload chart create cluster-scoped objects.
Set:

```yaml
openshift:
  nodeSCC:
    create: false
    name: qumulo-csi-node      # the SCC your admin created
```

The chart then renders no SCC and no RBAC grant, and pins the node pods to the
named SCC through `openshift.io/required-scc`.

Your admin applies an SCC equivalent to the chart's:

```yaml
apiVersion: security.openshift.io/v1
kind: SecurityContextConstraints
metadata:
  name: qumulo-csi-node
allowPrivilegedContainer: true
allowHostNetwork: true
allowHostPorts: true
allowHostDirVolumePlugin: true
allowHostPID: false
allowHostIPC: false
readOnlyRootFilesystem: false
runAsUser:
  type: RunAsAny
seLinuxContext:
  type: RunAsAny
fsGroup:
  type: RunAsAny
supplementalGroups:
  type: RunAsAny
seccompProfiles:
  - runtime/default
volumes:
  - hostPath
  - emptyDir
  - projected
users: []
groups: []
```

and grants it to the node service account only:

```bash
oc adm policy add-scc-to-user qumulo-csi-node \
  -z qumulo-csi-qumulo-csi-driver-node -n qumulo-csi
```

The service account name is `<release>-qumulo-csi-driver-node`. If the SCC
allows less than the list above, the node pods are refused and the DaemonSet
reports no pods — check the DaemonSet's events, not the pod logs, because no
pod is created.

`helm uninstall` never touches an SCC it did not create.

## The SCC settles after the install starts

On a fresh project the node DaemonSet can be created before the SCC and its
RBAC grant are fully in effect. The first node pods are refused, the DaemonSet
retries, and they are admitted on a later attempt — usually within a few
seconds.

What you see while it resolves:

```console
Warning  FailedCreate  daemonset/qumulo-csi-...-node
  Error creating: pods "..." is forbidden: unable to validate against any
  security context constraint
```

This is self-correcting. Treat it as a failure only if node pods are still
absent after a minute or two, and then check the grant with the `auth can-i`
command above.

!!! note "Not yet a pre-install hook"
    Creating the SCC in a Helm `pre-install` hook would remove the window
    entirely. That change is open and not made in 0.1.0.

## Upgrades

`helm upgrade --reuse-values` works from a release installed before the chart
had OpenShift support. The old release's node pods never started on OpenShift;
after the upgrade they start with no SCC step from you.

Node pods roll one at a time, and `node.minReadySeconds` (default 10) keeps the
rollout from counting a pod before the kubelet has registered the plugin.
Running pods keep their mounts throughout — the mount belongs to the node's
kernel, not to the node pod.

### If an upgrade fails part-way

An upgrade that fails mid-flight — for example one run by mistake with
`openshift.enabled: false` — can leave the failed revision's settings on the
live objects. Helm compares the next upgrade against the last *successful*
revision, so it does not remove them: the controller keeps a fixed uid that
`restricted-v2` refuses, and its new pods are never created.

Pods already running keep serving. Volumes are not affected.

```bash
helm upgrade qumulo-csi <chart> -n qumulo-csi -f values.yaml --force
```

`--force` replaces the objects instead of patching them. `helm rollback` does
**not** repair it, for the same reason.

## Proxy and CA failures

### The controller cannot reach the array

Pods installed by Helm do not inherit the cluster's proxy settings, so a
cluster that reaches the arrays only through a proxy needs them copied in.

```bash
oc get proxy cluster -o jsonpath='{.status}'
```

```yaml
controller:
  proxy:
    httpsProxy: http://proxy.example.com:3128
    noProxy: 10.0.0.13        # arrays reached directly, if any
```

Only the driver container gets these. `.svc`, `.cluster.local` and the
Kubernetes API service address are always added to the no-proxy list, so the
driver's own API calls never go through the proxy.

| Symptom | Likely cause |
|---|---|
| Claims Pending, `dial tcp … i/o timeout` to the array | No proxy set on a cluster that requires one. |
| Claims Pending, proxy returns `403` or `502` | The array is not permitted by the proxy; add it to `noProxy` if it is reachable directly. |
| Sidecar errors talking to the Kubernetes API | A `noProxy` that was overwritten rather than extended — the chart adds the cluster entries itself, so list only arrays here. |

### The trusted CA bundle has not been injected

With `openshift.trustedCABundle: true` the chart creates an empty ConfigMap
labelled `config.openshift.io/inject-trusted-cabundle`, OpenShift fills it with
the system CAs plus anything in the cluster Proxy's `trustedCA`, and the
controller reads it through `SSL_CERT_FILE`.

The controller pod waits for the fill, so it never starts with an empty CA
list. If it stays unready, the injection has not happened:

```bash
oc -n qumulo-csi get configmap qumulo-csi-qumulo-csi-driver-trusted-ca \
  -o jsonpath='{.data.ca-bundle\.crt}' | head -1
```

Empty output means the Cluster Network Operator has not filled it — check that
the label is present and that the operator is healthy.

!!! warning "`trustedCABundle` replaces `qumulo.caBundle`"
    Setting both is refused, as is combining it with
    `qumulo.insecureSkipTlsVerify`.

## Monitoring

OpenShift's own Prometheus does not scrape workload namespaces until
user-workload monitoring is enabled:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-monitoring-config
  namespace: openshift-monitoring
data:
  config.yaml: |
    enableUserWorkload: true
```

Then turn on the chart's metrics and `ServiceMonitor` as on any other cluster —
see [Monitor the driver](monitor.md). The `ServiceMonitor` is picked up by the
user-workload Prometheus in `openshift-user-workload-monitoring`.

!!! warning "Querying the monitoring API needs a bearer token"
    The OpenShift monitoring API authenticates with a bearer token. A session
    logged in with a client certificate cannot query it, and `oc` will return
    `Unauthorized` even though the same user can read every object. Create a
    service account with `cluster-monitoring-view` and use its token:

    ```bash
    oc create sa metrics-reader -n qumulo-csi
    oc adm policy add-cluster-role-to-user cluster-monitoring-view \
      -z metrics-reader -n qumulo-csi
    oc create token metrics-reader -n qumulo-csi
    ```

    Or query the Thanos querier route directly, which accepts the same token.

The node plugin binds `node.healthPort` (default 29653) as a **host** port on
every worker. The node SCC must allow host ports, which the chart's does. If
you also run the upstream `csi-driver-nfs`, which uses the same port, change
one of them.

## Uninstall

```bash
helm uninstall qumulo-csi -n qumulo-csi
oc delete project qumulo-csi
```

What goes and what stays:

| Object | Removed by `helm uninstall`? |
|---|---|
| Deployment, DaemonSet, `CSIDriver`, Service, RBAC | Yes |
| The chart's node SCC and its RBAC grant | Yes — both are cluster-scoped, so they outlive the project if you delete the project first |
| An SCC your admin created (`nodeSCC.create: false`) | No, never touched |
| The trusted-CA ConfigMap | Yes, with the release |
| StorageClasses | No — they carry `resource-policy: keep` |
| PVs, PVCs, Secrets, and anything on the Qumulo cluster | No |

!!! warning "Delete the project last"
    The SCC and its `ClusterRoleBinding` are cluster-scoped and are removed by
    `helm uninstall`, not by deleting the project. Deleting the project first
    leaves them behind, and a later install of the same release name in the
    same namespace then adopts them.

Running pods keep their mounts after an uninstall; the mount is the node
kernel's. New claims stay Pending. See
[Uninstall the driver](uninstall.md) for the platform-independent detail.

### Uninstall is refused while a DR set is in use

If the release still backs a standing disaster-recovery set, the uninstall is
refused. That is the guard working, not a fault: removing the driver would
leave replication relationships nothing can manage. Disable replication on
those volumes first, or uninstall a release that does not back them.
