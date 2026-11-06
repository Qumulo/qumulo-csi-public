# Volumes

Creating storage, removing it, making it bigger, and taking over a folder that already exists.

## Create a volume

Ask for storage by naming a storage class. The driver creates a folder on the
Qumulo cluster and puts a size limit on it.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-data
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-nfs
  resources:
    requests:
      storage: 10Gi
```

```bash
kubectl apply -f pvc.yaml
kubectl get pvc my-data        # Bound within a few seconds
```

The size you ask for becomes a Qumulo quota on that folder. Nothing is
reserved up front — the space is used as the application writes.

## Make a volume bigger

Edit the claim. Nothing unmounts and no pod restarts, because it is only the
quota changing.

```bash
kubectl patch pvc my-data -p '{"spec":{"resources":{"requests":{"storage":"50Gi"}}}}'
```

```bash
kubectl get pvc my-data -o jsonpath='{.status.capacity.storage}'
```

The storage class must have `allowVolumeExpansion: true`, which the examples
set.

!!! warning "Volumes cannot be made smaller"
    Kubernetes itself refuses it. Asking for less than the current size is
    rejected, and the quota is never lowered.

!!! note "A quota is a firm limit over time, not an instant one"
    A fast writer can run past the quota briefly before the array answers
    `Disk quota exceeded`. Measured on a 1 GiB volume written at 336 MB/s: the
    write reached about 4.9 GB — roughly 3.5 GiB of overshoot — before it was
    refused, consistently across every storage-class shape. The driver sets the
    quota correctly and is not in the data path once a volume is mounted; this
    is how the array enforces quotas. Leave that headroom in your capacity
    planning rather than treating the quota as a hard instantaneous cap.

## Delete a volume

```bash
kubectl delete pvc my-data
```

With the default reclaim policy the folder and its quota are removed from the
Qumulo cluster. If you want the data kept, set `reclaimPolicy: Retain` on the
storage class — then deleting the claim leaves the folder alone.

!!! note "A volume is only gone when Qumulo says so"
    The driver does not report a deletion as finished until the cluster
    confirms the folder is absent, so a claim never disappears while its data
    is still there.

## Retries are safe

If a request is interrupted — a timeout, a restarted controller — the retry
finds what the first attempt made and finishes it rather than creating a
second folder.

!!! warning "One interruption needs manual help"
    If a create is cut off in the moment between making the folder and setting
    its quota, the retry sees a folder with no quota, decides it is not one of
    its own, and refuses it. The claim then stays Pending. Remove the folder
    on the array, or give it a quota, and it proceeds.