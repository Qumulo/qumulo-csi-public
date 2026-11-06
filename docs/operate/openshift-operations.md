# Operate on OpenShift

Day-two tasks that differ on OpenShift: checking which SCC admitted each pod,
auditing what the chart's SCC asks for, handing SCC management and RBAC
bindings to a cluster admin, Helm release storage, recovering a release a
policy refused, upgrades, and the proxy and CA failures that look like driver faults but
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
names). Node pods must show the chart's SCC: `<namespace>-<release>-qumulo-csi-driver-node`, or
`<release>-qumulo-csi-driver-node` when the release is named after its namespace
(`qumulo-csi-qumulo-csi-driver-node` for the documented install).

| What you see | What it means |
|---|---|
| Node pods show `privileged` | The chart's SCC was not matched and a broader one admitted the pod. Usually `openshift.nodeSCC.create: false` with no `nodeSCC.name`, or an admin grant of `privileged` to the namespace's default service account. |
| No node pods at all | `openshift.enabled` is `false` on a cluster that enforces SCCs. The DaemonSet creates nothing and `helm install` still reports success. The install notes print a warning for exactly this case. |
| Controller pods `CreateContainerConfigError` | The controller kept its fixed uid 65532, which `restricted-v2` refuses. Normally a part-failed upgrade — see [If an upgrade fails](#if-an-upgrade-fails-part-way). |
| Controller pods refused: `unable to validate against any security context constraint`, uid 65532 not in the namespace range | Your cluster gives workloads its own restricted SCC (for example `restricted-csi`) rather than `restricted-v2`. Set `openshift.controllerSCC` to it. If its name does not start with `restricted`, also set `openshift.assignControllerUID: true` so the chart leaves the uid out. The `helm test` pods follow the same setting. |

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
allowPrivilegeEscalation: true
allowedCapabilities: []
defaultAddCapabilities: []
requiredDropCapabilities: []
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
users: []
groups: []
```

This is exactly what the chart's own SCC allows. The node pod mounts no
service account token, so it needs no `projected` volume. `privileged` already
gives the driver container every capability, `SYS_ADMIN` included, so no
capability is listed: Kubernetes allows the `Bidirectional` mount propagation
the driver needs only in a privileged container, so a `SYS_ADMIN`-only SCC
cannot work.

Then grant it to the node service account only. Either run:

```bash
oc adm policy add-scc-to-user qumulo-csi-node \
  -z qumulo-csi-qumulo-csi-driver-node -n qumulo-csi
```

or, where a platform team applies reviewed YAML, apply the same grant as a
ClusterRole and a RoleBinding:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: qumulo-csi-node-use
rules:
  - apiGroups: ["security.openshift.io"]
    resources: ["securitycontextconstraints"]
    resourceNames: ["qumulo-csi-node"]
    verbs: ["use"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: qumulo-csi-node-use
  namespace: qumulo-csi
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: qumulo-csi-node-use
subjects:
  - kind: ServiceAccount
    name: qumulo-csi-qumulo-csi-driver-node
    namespace: qumulo-csi
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

!!! warning "Helm 4"

    Helm 4 renamed `--force` to `--force-replace`; the old name still works,
    with a deprecation warning. What matters is how the release applies its
    objects. Helm 4 installs a **new** release with server-side apply and keeps
    using it on later upgrades, and it refuses a replace on that path before
    changing anything:

    ```text
    invalid operation: cannot use server-side apply and force replace together
    ```

    For a release Helm 4 installed, ask for client-side apply for this one
    repair:

    ```bash
    helm upgrade qumulo-csi <chart> -n qumulo-csi -f values.yaml \
      --force-replace --server-side=false
    ```

    A release Helm 3 created stays on client-side apply after you move to
    Helm 4, so the command at the top of this section works for it as written.

    Or repair the release by hand, which works on Helm 3 and Helm 4 alike:
    delete the objects that carry the failed revision's settings and let the
    next upgrade recreate them.

    ```bash
    oc delete deployment qumulo-csi-qumulo-csi-driver-controller -n qumulo-csi
    helm upgrade qumulo-csi <chart> -n qumulo-csi -f values.yaml
    ```

    Deleting the controller Deployment stops provisioning, resize and snapshot
    calls until the new pods are Ready. Mounted volumes and running workloads
    are not affected. Name the node DaemonSet instead when it is the node
    plugin's settings that are stuck.

## Bindings applied by a platform team

On some clusters a policy engine (Gatekeeper, for example) lets only a
platform team create, change or delete RoleBindings and ClusterRoleBindings.
Set `rbac.createBindings: false`. The chart then renders no binding at all,
and Helm never creates, patches or adopts one. It still renders the
ServiceAccounts, Roles and ClusterRoles the bindings point at.

**1. The platform team applies the bindings first.** Render exactly the
bindings this release needs, with the same release name, namespace and values
you will install with:

```bash
helm template qumulo-csi <chart> -n qumulo-csi -f values.yaml \
  --set rbac.createBindings=true \
  --show-only templates/rbac-bindings.yaml > qumulo-csi-bindings.yaml
oc apply -f qumulo-csi-bindings.yaml        # as the platform team
```

For release `qumulo-csi` in namespace `qumulo-csi` they are:

| Kind | Name | Namespace | Points at | ServiceAccount | When |
|---|---|---|---|---|---|
| ClusterRoleBinding | `qumulo-csi-qumulo-csi-driver-controller` | (cluster) | ClusterRole of the same name | `…-controller` | always |
| RoleBinding | `qumulo-csi-qumulo-csi-driver-controller` | release namespace | Role of the same name (leader-election leases) | `…-controller` | `controller.leaderElection` (default on) or csi-addons |
| RoleBinding | `qumulo-csi-qumulo-csi-driver-controller-secrets` | each namespace that holds a class's Secret | Role of the same name | `…-controller` | default (`rbac.clusterWideSecrets: false`) |
| RoleBinding | `qumulo-csi-qumulo-csi-driver-controller-self` | release namespace | Role of the same name | `…-controller` | capacity or csi-addons sidecar on |
| RoleBinding | `<node SCC>-use` | release namespace | ClusterRole of the same name | `…-node` | `openshift.nodeSCC.create: true` |
| RoleBinding | `qumulo-csi-qumulo-csi-driver-test` | release namespace | Role of the same name | `…-test` | `tests.ready.enabled` (default on) |
| ClusterRoleBinding | `qumulo-csi-qumulo-csi-qumulo-csi-driver-test` | (cluster) | ClusterRole of the same name | `…-test` | `tests.ready.enabled` (default on) |

The ServiceAccounts are `<release>-qumulo-csi-driver-controller`,
`-node` and `-test`, all in the release namespace. The install notes
(`helm status`) print the same list with your names filled in.

**2. Install as usual.** Before anything is applied, the install checks each
binding: that it exists, points at the right role and names the right
ServiceAccount. If one is not right, nothing is changed and the error names
it:

```text
rbac.createBindings is false, so the platform team must apply this release's
bindings before the install, and 1 are not right: RoleBinding
qumulo-csi/qumulo-csi-qumulo-csi-driver-controller-secrets is missing. ...
```

The check only reads bindings. If the install identity may not read them,
set `rbac.checkBindings: false` to skip it.

**When to apply bindings again.** The list depends on your values. Adding a
StorageClass whose Secret is in a new namespace needs one more
`-controller-secrets` binding there; turning on the capacity sidecar needs
`-controller-self`. Render the list again after changing values; the next
upgrade stops and names any that is missing.

!!! warning "Switching an installed release to `createBindings: false`"
    Helm deletes every object a new revision no longer renders. A release
    installed with the default `true` made its bindings itself, so an upgrade
    to `false` would delete them and the controller would lose its
    permissions. The upgrade refuses this and names each binding. The
    platform team first marks them so Helm keeps them:

    ```bash
    helm template qumulo-csi <chart> -n qumulo-csi -f values.yaml \
      --set rbac.createBindings=true --show-only templates/rbac-bindings.yaml \
      | oc annotate -f - helm.sh/resource-policy=keep --overwrite
    ```

    Then upgrade with `rbac.createBindings: false`. The bindings stay.

!!! note "A binding that already exists, with `createBindings: true`"
    Helm refuses to install over a binding it did not create ("exists and
    cannot be imported into the current release"). Either delete the
    platform's copy or use `createBindings: false`.

## Helm release storage

Helm keeps each release's history in the release namespace, as Secrets by
default (`HELM_DRIVER=secret`). Where a policy blocks Secret writes by the
install identity, use ConfigMaps instead:

```bash
export HELM_DRIVER=configmap
helm install qumulo-csi <chart> -n qumulo-csi -f values.yaml
```

The chart works the same with either backend: it never reads Helm's own
records. Things to know:

- **Pick one backend for a release and keep it.** Every person and pipeline
  that runs `helm` against the release must set the same `HELM_DRIVER`. Helm
  does not share history between backends: with the other one set, the
  release is not there at all (`helm list` shows nothing, and an install
  would start a second history over the same objects).
- **What a ConfigMap record exposes.** A record holds the release's values
  and rendered manifests, readable by anyone who can read ConfigMaps in the
  namespace. This chart's values hold no tokens or passwords by default (the
  Qumulo token is in a Secret you create). Do not put a secret value, such as
  a proxy URL with a password, in the values file of a ConfigMap-backed
  release.
- **Moving an existing release from Secrets to ConfigMaps.** Copy each record,
  keeping its name and labels. Run as someone who may read the Secrets:

  ```bash
  for s in $(oc -n qumulo-csi get secret -l owner=helm,name=qumulo-csi -o name); do
    oc -n qumulo-csi get "$s" -o json | python3 -c '
  import base64, json, sys
  o = json.load(sys.stdin)
  print(json.dumps({"apiVersion": "v1", "kind": "ConfigMap",
    "metadata": {"name": o["metadata"]["name"], "namespace": o["metadata"]["namespace"],
                 "labels": o["metadata"]["labels"]},
    "data": {"release": base64.b64decode(o["data"]["release"]).decode()}}))' \
    | oc apply -f -
  done
  HELM_DRIVER=configmap helm history qumulo-csi -n qumulo-csi   # same revisions
  ```

  The record format is the same in both backends, so the history,
  `helm upgrade` and `helm rollback` carry on. Once that works, the platform
  team deletes the old Secrets
  (`oc -n qumulo-csi delete secret -l owner=helm,name=qumulo-csi`).

## Recover a release that admission refused

When a policy refuses part of an install or upgrade, Helm stops and marks the
revision `failed`. Objects applied before the refusal stay. Running pods keep
serving.

1. **See what Helm recorded:** `helm status qumulo-csi -n qumulo-csi` and
   `helm history qumulo-csi -n qumulo-csi` (with the same `HELM_DRIVER`).
2. **Find what was refused:** the Helm error names the object and the policy.
   For pods, look at the controller ReplicaSet's and the node DaemonSet's
   events (`oc -n qumulo-csi get events --sort-by=.lastTimestamp`); a refused
   pod is never created, so it has no logs.
3. **Fix the cause:** have the platform team apply the bindings, set
   `rbac.createBindings: false`, `openshift.controllerSCC`, or the node SCC
   values as above.
4. **Run the same `helm upgrade` (or `helm upgrade --install`) again.** A
   `failed` release can be upgraded; the next revision applies everything,
   including what the failed one left out.

**A release stuck in `pending-install` or `pending-upgrade`** (Helm was
stopped part-way, or could not write its final status) refuses every new
command with `another operation (install/upgrade/rollback) is in progress`.

- `pending-install`: nothing depends on it yet. Run `helm uninstall`
  and install again. A first install has made no volumes, so this loses
  nothing.
- `pending-upgrade`: `helm rollback qumulo-csi <last deployed revision>`
  clears it, then upgrade again.

Each of these writes Helm's release record. Where Helm's release Secrets are
blocked for the install identity, the platform team runs it, or the release
moves to ConfigMap storage first (above).

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

The node plugin binds two **host** ports on every worker: `node.healthPort`
(default 29653, the livenessprobe) and `node.registrarHealthPort` (default
`node.healthPort` + 6, so 29659, the registrar's own `/healthz`), plus
`metrics.port.node` with metrics on. The node SCC must allow host ports,
which the chart's does. A second release of this chart on the same cluster
needs its own `node.healthPort` and `metrics.port.node`; its registrar port
follows. The install or upgrade is refused, naming the DaemonSet, if another
DaemonSet already holds one of them. A second release whose upgrade to the
4d386b0 chart already failed on the port must be rolled back
(`helm rollback <release> <last good revision>`) before the next upgrade;
upgrading straight over it is refused with `Duplicate value: "registrar"`. If you also run the upstream
`csi-driver-nfs`, which uses 29653, change one of them.

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
