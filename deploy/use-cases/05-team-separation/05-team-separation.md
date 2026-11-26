# 5. Give each team its own directory and token

**When to use:** several teams share one cluster. Each needs its own space.

Each team gets a StorageClass. Each class has its own directory and its own
Qumulo API token.

## Run

Create one Secret per team. The key must be `token`.

```sh
kubectl -n qumulo-csi create secret generic qumulo-team-a --from-file=token=./a.token
kubectl -n qumulo-csi create secret generic qumulo-team-b --from-file=token=./b.token

kubectl apply -f deploy/use-cases/05-team-separation/storageclass-team-a.yaml
kubectl apply -f deploy/use-cases/05-team-separation/storageclass-team-b.yaml
kubectl apply -f deploy/use-cases/05-team-separation/resourcequota.yaml
```

## Check

Team A can use its class:

```sh
kubectl -n team-a apply -f deploy/use-cases/03-expand-volume/pvc-before.yaml   # edit the class name
```

Team A cannot use team B's class. The quota is set to zero.

## Note

**A StorageClass is cluster-wide.** Anyone who can create a PVC can name any
class. The quota is what stops them, not the class itself.

If a token leaks, it only opens one directory. This is the main benefit.

The controller can read Secrets in any namespace. This is wider than a
single-cluster install needs. Check it when you review RBAC.

Files: [`deploy/use-cases/05-team-separation/`](.)
