# Collect logs

## Where the logs are

```bash
# controller: provisioning, deletion, expansion, snapshots, replication
kubectl -n qumulo-csi logs -l app.kubernetes.io/component=controller \
  -c qumulo-csi-driver --tail=100

# node: mounting and unmounting, on the node the pod was scheduled to
kubectl -n qumulo-csi logs -l app.kubernetes.io/component=node \
  -c qumulo-csi-driver --tail=100

# the sidecars, when the driver itself logs nothing
kubectl -n qumulo-csi logs -l app.kubernetes.io/component=controller \
  -c csi-provisioner --tail=50
```

!!! tip "If the driver logged nothing, read the sidecar"
    A PVC that never reaches the driver — a missing Secret, an RBAC refusal, a
    class the provisioner would not act on — leaves the driver silent and the
    sidecar talkative.

## Which container to read

| Symptom | Container |
|---|---|
| PVC `Pending` | controller `qumulo-csi-driver`, then `csi-provisioner` |
| PVC stuck `Terminating` | controller `qumulo-csi-driver` |
| Resize not happening | controller `csi-resizer`, then the driver |
| Snapshot not ready | controller `csi-snapshotter`, then the driver |
| Pod `ContainerCreating` | node `qumulo-csi-driver` **on that node** |
| Pod will not terminate | node `qumulo-csi-driver` on that node |
| Pod never started, no logs at all | `kubectl describe pod` — an image pull failure |

To read the node plugin on a specific node:

```bash
NODE=<node-name>
POD=$(kubectl -n qumulo-csi get pods -l app.kubernetes.io/component=node \
  --field-selector spec.nodeName="$NODE" -o name)
kubectl -n qumulo-csi logs "$POD" -c qumulo-csi-driver --tail=200
```

## Log levels

```yaml
controller:
  logLevel: info     # debug for diagnosis
node:
  logLevel: info
```

!!! tip "Raise the level on one side only"
    Controller and node are configured separately, and a change restarts only
    that workload. Raising the node level rolls the DaemonSet — which is safe
    for already-mounted volumes (the mount is a real kernel mount and does not
    belong to the driver container) but will briefly stop new mounts on each
    node as it rolls.

!!! warning "Return to `info` afterwards"
    `debug` logs every Qumulo REST round trip. On a busy controller that is a
    lot of volume, and it makes the interesting lines harder to find.

## What is and is not logged

| | |
|---|---|
| Logged | the CSI method, the volume handle, the Qumulo path, the endpoint, the HTTP status and the driver's own reason |
| Logged at ERROR, always | a recovered panic; orphaned pod data moved aside by the node plugin |
| **Not** logged | API tokens. Credentials are resolved per request and never printed |

!!! warning "Volume handles contain the array address and the real path"
    `<server>#<apiUrl>#<realPath>` appears in log lines. Treat the driver's
    logs as containing storage-topology information, and scope access to them
    accordingly.

## Lines worth alerting on

| Pattern | Meaning |
|---|---|
| `moved orphaned pod data to ...` | **a pod's only copy of data** reached the node's disk after its NFS mount went away. See below |
| `panic` / a rise in `csi_qumulo_rpc_panics_total` | a driver bug; the RPC was answered `Internal` and the process kept serving |
| `401 Unauthorized` | an expired or wrong token |
| `403 Forbidden` | a missing Qumulo privilege |
| `adding nolock` | an NFSv3 mount that did not decide locking — informational |
| `skipping class ... credential` | a class silently left out of `ListVolumes` |

!!! warning "Orphaned pod data is never deleted, and nothing counts it"
    It lands in `<kubeletDir>/<driver-name>-orphaned/` — by default
    `/var/lib/kubelet/csi.qumulo.com-orphaned/` — which is normally empty.
    When it is not, it holds a pod's only copy of data; the node plugin logs
    both paths at ERROR and deletes nothing. It sits *beside* the plugin
    directory precisely so that cleaning up
    `<kubeletDir>/plugins/<driver-name>/` cannot delete it — but an `rm -rf`
    aimed higher will. Check it before cleaning a node by hand.

## Collecting logs for a support case

```bash
NS=qumulo-csi
mkdir -p csi-support && cd csi-support

kubectl -n $NS get pods -o wide            > pods.txt
kubectl -n $NS describe pods               > pods-describe.txt
kubectl -n $NS logs -l app.kubernetes.io/component=controller --all-containers --tail=2000 > controller.log
kubectl -n $NS logs -l app.kubernetes.io/component=node --all-containers --tail=2000       > node.log
kubectl get csinodes -o yaml               > csinodes.yaml
kubectl get sc -o yaml                     > storageclasses.yaml
kubectl get pv -o yaml                     > pv.yaml
kubectl get pvc -A -o yaml                 > pvc.yaml
kubectl get events -A --sort-by=.lastTimestamp > events.txt
helm -n $NS get values qumulo-csi          > helm-values.yaml
helm -n $NS history qumulo-csi             > helm-history.txt
kubectl version                            > versions.txt
```

!!! warning "Review before sending"
    `helm get values` can contain registry usernames and your cluster
    addresses, and the PV dump contains every volume's real path on the array.
    It will not contain Qumulo API tokens — the chart renders no Secret — but
    check anyway.

Open a case at [care.qumulo.com](https://care.qumulo.com).

## Next

- [Monitor the driver](monitor.md)
- [Troubleshoot provisioning](../provision/troubleshoot.md)
- [Troubleshoot Guide](troubleshoot-mounts.md)
