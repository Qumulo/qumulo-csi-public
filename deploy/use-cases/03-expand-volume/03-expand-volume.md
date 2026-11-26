# 3. Make a volume bigger

**When to use:** the volume is full and the pod needs more space.

The pod keeps running. There is no restart and no remount.

## Run

Ask for more space in the PVC.

```sh
kubectl patch pvc app-data \
  -p '{"spec":{"resources":{"requests":{"storage":"100Gi"}}}}'
```

Or edit the file and apply it:

```sh
kubectl apply -f deploy/use-cases/03-expand-volume/pvc-after.yaml
```

## Check

```sh
kubectl get pvc app-data \
  -o custom-columns='WANTED:.spec.resources.requests.storage,ACTUAL:.status.capacity.storage'
# WANTED   ACTUAL
# 100Gi    100Gi

kubectl exec test-pod -- df -h /data      # shows the new size
```

## Note

You cannot make a volume smaller. A smaller request is accepted and ignored.

The StorageClass needs `allowVolumeExpansion: true`. Our examples set it.

Files: [`deploy/use-cases/03-expand-volume/`](.)
