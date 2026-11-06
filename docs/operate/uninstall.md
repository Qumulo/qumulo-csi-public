# Uninstall the driver

!!! note "On OpenShift"
    The chart's SecurityContextConstraint and its RBAC grant are
    cluster-scoped, and the order matters — see
    [Uninstall](openshift-operations.md#uninstall).

## What is removed, and what is kept

Operators uninstall and reinstall far more often than they upgrade — a node
pool rotation, a namespace rebuild, a values change that needs `--set` flags a
plain upgrade cannot add cleanly.

```bash
# Delete workloads and PVCs FIRST. Removing the driver first leaves PVs that
# nothing can clean up, on the cluster and in Kubernetes.
helm uninstall qumulo-csi -n qumulo-csi
kubectl delete namespace qumulo-csi
```

| `helm uninstall` removes | `helm uninstall` leaves |
|---|---|
| the controller Deployment | StorageClasses, VolumeSnapshotClasses, VolumeReplicationClasses (`helm.sh/resource-policy: keep`) |
| the node DaemonSet | PVs and PVCs |
| the `CSIDriver` object | the credential Secrets a class names |
| RBAC | **everything already provisioned on the Qumulo cluster** |
| the node PriorityClass | |

A reinstall under the **same release name and namespace** takes the kept
classes back over. Under another name it is refused until you delete them with
`kubectl`.

!!! warning "Check the orphaned-data directory before cleaning a node by hand"
    `<kubeletDir>/<driver-name>-orphaned/` — by default
    `/var/lib/kubelet/csi.qumulo.com-orphaned/` — is normally empty. When it is
    not, it holds **a pod's only copy of data**. Nothing ever removes it, and
    no metric counts it. It sits beside the plugin directory so that removing
    `<kubeletDir>/plugins/<driver-name>/` cannot delete it — but an `rm -rf`
    aimed higher will.