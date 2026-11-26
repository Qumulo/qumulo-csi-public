# Step 8 -- snapshot and restore (optional)

Skip this step if you haven't enabled snapshots. It's off by default
because it needs the cluster-wide `VolumeSnapshot` CRDs and the external
snapshot controller installed once per cluster -- not something this chart
should do on your behalf.

## One-time cluster setup, if not already done

```sh
# The CRDs and controller, from the external-snapshotter project:
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-8.2/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-8.2/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-8.2/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-8.2/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-8.2/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml

# Then turn on this chart's csi-snapshotter sidecar and re-apply:
helm upgrade qumulo-csi charts/qumulo-csi-driver -n qumulo-csi \
  --reuse-values --set sidecars.snapshotter.enabled=true
```

## Take a snapshot

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: qumulo-example-snapclass
driver: csi.qumulo.com
deletionPolicy: Delete
# Required: snapshot calls carry their own credential, and the driver has no
# fallback token. Name the Secret your StorageClass uses.
parameters:
  csi.storage.k8s.io/snapshotter-secret-name: qumulo-api-token
  csi.storage.k8s.io/snapshotter-secret-namespace: qumulo-csi
  csi.storage.k8s.io/snapshotter-list-secret-name: qumulo-api-token
  csi.storage.k8s.io/snapshotter-list-secret-namespace: qumulo-csi
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: qumulo-example-snapshot
  namespace: qumulo-csi
spec:
  volumeSnapshotClassName: qumulo-example-snapclass
  source:
    persistentVolumeClaimName: qumulo-example-pvc
```

```sh
kubectl -n qumulo-csi get volumesnapshot qumulo-example-snapshot -w
# wait for READYTOUSE to be true
```

## Restore it as a new volume

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: qumulo-example-restored-pvc
  namespace: qumulo-csi
spec:
  storageClassName: qumulo-example
  accessModes: [ReadWriteMany]
  resources: {requests: {storage: 1Gi}}
  dataSource:
    name: qumulo-example-snapshot
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
```

The restored PVC provisions a brand-new directory with the snapshot's
contents copied in, file by file, server-side. For a volume with more than
a handful of files this can take several `CreateVolume` retries to finish
(each attempt reports `Aborted` and the next one resumes) -- that's the
driver working correctly, not stuck; watch `kubectl describe pvc` for
progress.
