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
