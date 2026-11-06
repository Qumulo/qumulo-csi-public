# Security

How the driver authenticates to Qumulo, and what is and is not encrypted.

## The API token

The driver logs in to Qumulo with an API token held in a Kubernetes Secret,
one per cluster, under the key `token`.

```bash
kubectl -n qumulo-csi create secret generic qumulo-api-token \
  --from-literal=token='<api-token>'
```

```yaml
storageClasses:
  - name: qumulo-nfs
    secretName: qumulo-api-token
```

Tokens are read fresh on each request and never cached between them, so
rotating one takes effect on the next call with no restart. The driver keeps
no credential of its own and no fallback cluster — a request that does not
name a class and a Secret simply fails.

The token is created on the Qumulo cluster, and needs the privileges to
manage directories, quotas, exports and snapshots.

## TLS to the Qumulo API

Management traffic is encrypted and the certificate is verified, including
against a private or self-signed authority.

```yaml
qumulo:
  caBundle:
    existingConfigMap: qumulo-ca
```

!!! warning "The built-in Qumulo certificate cannot be verified"
    It is issued to `CN=localhost` with no subject alternative name, which Go
    has refused since 1.15 — installing it as a trusted root does not help.
    Replace the cluster's certificate before enabling TLS.

!!! note "It fails closed"
    A certificate the driver cannot verify stops the request and the claim
    stays Pending. There is no silent fall back to an unverified connection.

For evaluation only, `qumulo.insecureSkipTlsVerify: true` turns verification
off entirely. Do not leave it on in production.

## What is not encrypted

!!! warning "NFS traffic is in the clear"
    TLS covers the driver's management calls to Qumulo, not the data your
    pods read and write. Separate that at the network layer — network tenants
    and `allowedClients` are the tools for it — or use Kerberos with `krb5p`,
    which encrypts the NFS traffic itself.

## Running with least privilege

The controller runs as a non-root user and touches no filesystem. The node
pods do need root and privilege, because mounting on the host requires it —
that is a property of NFS mounting, not a choice this driver made.
A policy engine will flag those settings; see
[Policy engines](#policy-engines-gatekeeper-kyverno) for each one and a narrow
exemption.

The controller is allowed to read exactly the Secrets your classes name,
nothing wider. If you create a class outside the chart, add its Secret to
`rbac.extraSecrets`.

## OpenShift SCC admission

OpenShift admits every pod through a SecurityContextConstraint. The chart
detects OpenShift by itself and asks for the least each half of the driver
needs — which is not the same amount.

| | SCC | What it is allowed |
|---|---|---|
| Controller | `restricted-v2`, OpenShift's own default | Nothing extra. It speaks HTTPS to the Qumulo REST API and touches no filesystem, so it needs no privilege, no host access and no fixed identity. The chart drops its fixed uid 65532 so OpenShift assigns one from the project's range; `runAsNonRoot` stays. |
| Node plugin | its own SCC, created by the chart | Privileged, host network, host ports, `hostPath` and `emptyDir`, any uid, `runtime/default` seccomp. Nothing else — not host PID, not host IPC, not extra capabilities, not other volume types. |

The node plugin needs that set because it runs `mount(8)` in the host's mount
namespace. That is a property of mounting NFS, not a choice this driver made,
and it is the reason the two halves are separate pods with separate images in
the first place: the privilege stops at the node plugin and never reaches the
component that holds API credentials.

!!! note "A chart-owned SCC, not the built-in `privileged` one"
    The built-in `privileged` SCC would admit the node pod and grant far more
    than it uses. CIS OpenShift 5.2 asks that its use be kept minimal. The
    chart's SCC is granted through RBAC to the **node service account alone** —
    not to the namespace, not to `system:authenticated` — so the grant stays
    auditable and scoped. Set `openshift.nodeSCC.create: false` to supply your
    own instead.

Each pod also carries `openshift.io/required-scc`, naming the SCC it must be
admitted under. Without it, OpenShift picks the first SCC the service account
may use, which on a cluster where an admin has granted something broad is not
necessarily the one the chart intended.

### SELinux

Volumes work with SELinux enforcing and need no `seLinuxOptions` on your pods.
Two pods with different SELinux labels can share one volume — NFS mounts carry
a single context for the whole mount rather than per-file labels.

### What the node still does not get

The node service never calls the Qumulo REST API and never receives an API
token. A cluster-wide credential therefore never reaches a worker node, whether
or not the node pod is privileged.

See [Operate on OpenShift](../operate/openshift-operations.md#audit-what-the-charts-scc-allows)
for the exact SCC and how to audit it.

## Policy engines (Gatekeeper, Kyverno)

A policy engine that enforces the Kubernetes Pod Security Standards flags the
node plugin, and it is right to: the node pod mounts NFS in the host's mount
namespace, which no unprivileged pod can do. Exempt the node pod from the rules
below and nothing else. The controller and the `helm test` pods pass the
`restricted` standard as they are and need no exemption.

What the node pod (DaemonSet `<release>-qumulo-csi-driver-node`) asks for, and why:

| Setting | Container | Why it is needed |
|---|---|---|
| `privileged: true` | `qumulo-csi-driver` | `mount(8)` and `umount(8)` in the host's mount namespace. Privileged includes `SYS_ADMIN`; it cannot be narrowed to `SYS_ADMIN` alone, because Kubernetes allows `Bidirectional` mount propagation only in a privileged container. |
| `hostPath` `/var/lib/kubelet`, mount propagation `Bidirectional` | `qumulo-csi-driver` | The NFS mount made in the pod must appear on the host, where the kubelet hands it to the application pod. `Bidirectional` requires a privileged container. |
| `hostPath` `/var/lib/kubelet/plugins/csi.qumulo.com` | all three | The CSI socket the kubelet calls. |
| `hostPath` `/var/lib/kubelet/plugins_registry` | `node-driver-registrar` | Registers the driver with the kubelet. |
| `hostNetwork: true`, so its ports are host ports: `node.healthPort` (29653), `node.registrarHealthPort` (29659) and, with metrics on, `metrics.port.node` (29655) | pod | The NFS mounts are made from the node's own addresses, the ones the array's export allows. |
| runs as uid 0 | `qumulo-csi-driver`, `node-driver-registrar` | Mounting needs root; the registrar writes its socket into the kubelet's root-owned `plugins_registry`. |
| writable root filesystem | `qumulo-csi-driver` | `mount(8)` and `mount.nfs` write state inside the image. |

What it does **not** ask for: host PID or IPC, added capabilities (the two
sidecars drop all of them, with no privilege escalation and a read-only root),
or any volume type besides `hostPath` and `emptyDir`. Its seccomp profile is
`RuntimeDefault`. A policy that flags one of those has found a change, not this
list.

Everything else a pod-security or best-practice policy usually asks for is
already in the chart, on every container of both pods: a liveness and a
readiness probe, a CPU and a memory request, a memory limit equal to the
request, and an ephemeral-storage limit. On the default OpenShift render the
Gatekeeper library's `general` and `pod-security-policy` policies (their
sample constraints) all pass except the six node rules above and one more:

- `containerresourceratios`: its sample constraint (`cpuRatio: "10"`) needs a
  CPU limit, and the chart sets none on purpose. CFS throttling makes a mount,
  an unmount or a `CreateVolume` slow rather than failed, which on a node
  plugin means a stuck application pod. The memory half of the policy
  (`ratio: "1"`) passes. Exempt the driver's namespace from this policy too,
  or run it without `cpuRatio`.

kube-linter reports only the node settings above. The
drivers' probes read their liveness-probe sidecar's port, which the sidecar
declares, and the workloads carry `ignore-check.kube-linter.io/liveness-port`
saying so (declaring the port twice makes the API server warn about a
duplicate port on every install).

Keep the exemption to the driver's namespace, and keep that namespace for the
driver alone, so the exemption cannot be borrowed by another workload.

**Gatekeeper** (the `gatekeeper-library` pod-security constraints): add the
driver's namespace to `spec.match.excludedNamespaces` of the constraints the
node pod trips -- `K8sPSPPrivilegedContainer`, `K8sPSPHostFilesystem`,
`K8sPSPHostNetworkingPorts`, `K8sPSPAllowedUsers`,
`K8sPSPAllowPrivilegeEscalationContainer` and `K8sPSPReadOnlyRootFilesystem`
-- and leave every other constraint applying to it:

```yaml
apiVersion: constraints.gatekeeper.sh/v1beta1
kind: K8sPSPPrivilegedContainer
metadata:
  name: psp-privileged-container
spec:
  match:
    kinds: [{apiGroups: [""], kinds: ["Pod"]}]
    excludedNamespaces: ["qumulo-csi"]
```

**Kyverno**: a `PolicyException` for the node pods only (Kyverno must have
policy exceptions enabled; the API is `kyverno.io/v2` from Kyverno 1.13,
`kyverno.io/v2beta1` before):

```yaml
apiVersion: kyverno.io/v2
kind: PolicyException
metadata:
  name: qumulo-csi-node
  namespace: qumulo-csi
spec:
  exceptions:
    - policyName: disallow-privileged-containers
      ruleNames: ["*"]
    - policyName: disallow-host-path
      ruleNames: ["*"]
    - policyName: disallow-host-namespaces
      ruleNames: ["*"]
    - policyName: disallow-host-ports
      ruleNames: ["*"]
    - policyName: require-run-as-nonroot
      ruleNames: ["*"]
  match:
    any:
      - resources:
          kinds: [Pod, DaemonSet]
          namespaces: [qumulo-csi]
          selector:
            matchLabels:
              app.kubernetes.io/name: qumulo-csi-driver
              app.kubernetes.io/component: node
```

Use the names of the policies your cluster actually runs: these are the ones
the Kyverno pod-security policy set uses. On OpenShift the chart's own SCC
already admits the node pod (above); a policy engine is a second, separate
admission check and needs its own exemption.
