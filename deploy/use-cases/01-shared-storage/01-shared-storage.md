# 1. Shared storage for many pods

**When to use:** several pods must read and write the same files.
Examples: uploads, a shared cache, rendered images.

This is the main reason to use NFS. Block storage cannot do it.

## Run

```sh
kubectl apply -f deploy/use-cases/01-shared-storage/storageclass.yaml
kubectl apply -f deploy/use-cases/01-shared-storage/pvc.yaml
kubectl apply -f deploy/use-cases/01-shared-storage/deployment.yaml
```

Edit the StorageClass first. Set `server` and the two paths.

## Check

Write from one pod. Read from another.

```sh
A=$(kubectl get pod -l app=web -o jsonpath='{.items[0].metadata.name}')
B=$(kubectl get pod -l app=web -o jsonpath='{.items[1].metadata.name}')

kubectl exec $A -- sh -c 'echo hello > /usr/share/nginx/html/test'
kubectl exec $B -- cat /usr/share/nginx/html/test      # prints: hello
```

## Note

The driver does not lock files for you. Every pod can write at any time.

On NFSv4.1, file locks work between nodes. Your application must ask for them.
On NFSv3, locks work only inside one node.

Access modes are not enforced. `ReadWriteOnce` will not stop a second pod
mounting the volume. Treat access modes as a plan, not a rule.

Files: [`deploy/use-cases/01-shared-storage/`](.)
