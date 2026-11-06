# Prerequisites

Everything to have ready before [installing](install.md).

!!! note "On OpenShift"
    OpenShift Container Platform 4.22 is supported and needs nothing extra
    here — no SecurityContextConstraint to create first. 4.21 and older ship
    Kubernetes 1.34, below the chart's floor, and are refused. See
    [Install on OpenShift](openshift.md).

## Checklist

- [ ] Kubernetes 1.35.0 or later
- [ ] Helm 3.13 or later
- [ ] Cluster-admin rights on the Kubernetes cluster
- [ ] A Qumulo cluster your worker nodes can reach over **NFS**
- [ ] The same cluster's **REST API** reachable from the controller pods
- [ ] A Qumulo API token that can manage directories, quotas, exports and
      snapshots
- [ ] One Secret per Qumulo cluster, with the token under the key `token`
- [ ] A parent directory on the cluster, already created
- [ ] That directory already NFS-exported, for the default provisioning model
- [ ] A cluster certificate whose SAN covers every node address, or
      `insecureSkipTlsVerify: true` for an evaluation
- [ ] For Kerberos only: every node AD-joined, with a keytab, `krb5.conf` and
      `rpc.gssd` on the host
- [ ] Access to the container images, or your own copy of them

## Pre-Installation Validation


```bash
# a. Can a worker node reach the Qumulo REST API? Run from a pod, not your laptop.
kubectl run netcheck --rm -it --restart=Never --image=busybox:1.36 -- \
  nc -zv qumulo.example.com 8000

# b. Is the parent directory exported? Compare export_path, not fs_path.
curl -sk -H "Authorization: Bearer $TOKEN" \
  https://qumulo.example.com:8000/v3/nfs/exports/ \
  | grep -o '"export_path": "[^"]*"'
```

!!! warning "The driver does not create the parent export"
    Each volume is created **inside** a directory that already exists and, for
    `shared-subdirectory`, is already covered by an export. A class whose
    `basePath` is not covered fails every provision with `FailedPrecondition`
    naming the path.

## Secure access

Create one Secret per Qumulo cluster, with the token under the key `token`.
The release namespace has to exist first:

```bash
kubectl create namespace qumulo-csi

kubectl -n qumulo-csi create secret generic qumulo-api-token \
  --from-literal=token='<api-token>'                 # <-- EDIT
```

!!! warning "The key must be `token`"
    A Secret without it produces `no credential for cluster ...` on every
    provision.

The chart renders **no** Secret, deliberately: a token in a values file would
end up in `helm get values`, and a Secret the chart does not own survives an
uninstall.

The three security items on the checklist, in short:

**Driver credentials** — a Qumulo API token per cluster, held in a Kubernetes
Secret under the key `token`, with the privileges to manage directories,
quotas, exports and snapshots.

**TLS** — the stock `CN=localhost` certificate can never verify, so it has to
be replaced, or verification turned off with
`qumulo.insecureSkipTlsVerify: true` for an evaluation.

**Kerberos** — optional, and it needs host-side setup the chart cannot
supply: `rpc.gssd` runs on the node, not in the plugin pod.

!!! warning "Two replication privileges are required even if you never replicate"
    `PRIVILEGE_REPLICATION_SOURCE_READ` and `PRIVILEGE_REPLICATION_TARGET_READ`
    are read by **every** `DeleteVolume` and `ControllerExpandVolume`. Without
    them volumes can be created but never deleted or expanded — a trap that
    only shows up later.

!!! note "Qumulo tokens are not long-lived"
    An expired token shows up as `401 Unauthorized`. Rotating it needs no
    restart — the driver caches no credential, so the next call picks it up.

## Optional feature prerequisites

=== "Snapshots"

    ```bash
    kubectl get crd | grep volumesnapshot          # expect 3 CRDs
    kubectl get pods -A | grep snapshot-controller # expect a running pod
    ```

    The CRDs and `snapshot-controller` are shared by every CSI driver and are
    installed once by an administrator. This chart does not own them.

=== "Replication"

    ```bash
    kubectl get crd | grep csiaddons
    kubectl get pods -A | grep csi-addons-controller
    ```

    Plus a peer Qumulo array, and a Secret holding **both** arrays' tokens
    keyed by host.

=== "Metrics"

    The Prometheus Operator's `monitoring.coreos.com/v1` CRD, if you want the
    chart to render a `ServiceMonitor` and `PrometheusRule`. A free host port
    on every worker for the node endpoint (default 29655).

## Next

[Install the driver](install.md)
