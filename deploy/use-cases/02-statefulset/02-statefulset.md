# 2. One volume per replica

**When to use:** each pod needs its own storage. Examples: a database, a queue.

Each replica gets its own volume. The volume stays with the pod across restarts.

## Run

```sh
kubectl apply -f deploy/use-cases/01-shared-storage/storageclass.yaml
kubectl apply -f deploy/use-cases/02-statefulset/statefulset.yaml
```

## Check

Three PVCs, one for each pod.

```sh
kubectl get pvc
# data-worker-0   Bound   pvc-...   10Gi   RWO   qumulo-nfs41
# data-worker-1   Bound   ...
# data-worker-2   Bound   ...
```

Each PVC is a separate directory on the Qumulo cluster.

## Note

Deleting the StatefulSet does not delete the PVCs. This is on purpose.

To free the storage, delete the PVCs yourself. Until then the directories stay
on the cluster.

Files: [`deploy/use-cases/02-statefulset/`](.)
