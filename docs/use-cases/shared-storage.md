# Shared Storage for Multiple Pods

**When to use:** several pods must read and write the same files — uploads, a
shared cache, rendered images.

This is the main reason to use NFS. Block storage cannot do it: a block volume
attaches to one node at a time, while an NFS export is reachable from every
node the same way.

## What you need

- The driver installed, and a StorageClass. The examples below use one named
  `qumulo-nfs41` — see [Create your first volume](../get-started/first-volume.md).
- A directory on the Qumulo cluster that **already exists and is already
  exported**. The driver creates volume directories under it; it does not
  create the path or the export.

## The claim

`ReadWriteMany` is what lets many pods mount the same volume.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: shared-data
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: qumulo-nfs41
  resources:
    requests:
      storage: 10Gi
```

## Two pods sharing it

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 2
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
        - name: nginx
          image: nginx:1.27
          volumeMounts:
            - name: data
              mountPath: /usr/share/nginx/html
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: shared-data
```

Both replicas land wherever the scheduler puts them, and both mount the same
directory on the array.

## Check that it works

Write from one pod, read from another.

```bash
A=$(kubectl get pod -l app=web -o jsonpath='{.items[0].metadata.name}')
B=$(kubectl get pod -l app=web -o jsonpath='{.items[1].metadata.name}')

kubectl exec $A -- sh -c 'echo hello > /usr/share/nginx/html/test'
kubectl exec $B -- cat /usr/share/nginx/html/test      # prints: hello
```

## What to watch out for

!!! warning "The driver does not lock files for you"
    Every pod can write at any time. On NFSv4.1 file locks work between
    nodes, but your application has to ask for them. On NFSv3 locks work only
    within one node.

!!! note "`ReadWriteOnce` is a plan, not a rule"
    It will not stop a second pod, on a second node, from mounting **and
    writing** the volume: this driver has no attach step, so Kubernetes does
    not enforce the single-node rule. `ReadWriteOncePod` is the one mode that
    does hold, because Kubernetes enforces it itself — see
    [Access modes](../features/access.md#share-one-volume-between-many-pods).

Runnable copies of these files are in `deploy/use-cases/01-shared-storage/`.
