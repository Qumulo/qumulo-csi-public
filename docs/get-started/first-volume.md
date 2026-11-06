# Create your first volume

Installing the driver puts the software on your cluster, but it does not give
it anything to do. This page is the second half: you point it at a Qumulo
cluster, create a storage class, and prove the whole path works by running a
pod that writes a file to a real volume.

Work through it in order. Everything here is additive — if a step goes wrong,
fix it and run the step again.

Before starting, you need [the driver installed](install.md) and its pods
Ready.

## What you are about to build

| Piece | What it is for |
|---|---|
| A **Secret** with a Qumulo API token | How the driver logs in to the cluster |
| A **StorageClass** | Names the Qumulo cluster and the folder volumes go in |
| A **PersistentVolumeClaim** | A request for storage, made by whoever needs it |
| A **Pod** | Something that actually uses the storage |

You create the first two once. After that, anyone on the cluster can make
claims and pods without involving you.

## 1. Give the driver a token

The driver logs in to Qumulo with an API token, kept in a Kubernetes Secret.
You need one Secret per Qumulo cluster, with the token under the key `token`:

```bash
kubectl -n qumulo-csi create secret generic qumulo-api-token \
  --from-literal=token='<api-token>'
```

Check it is there:

```bash
kubectl -n qumulo-csi get secret qumulo-api-token
```

Create the token on the Qumulo cluster first. It needs the privileges to
manage directories, quotas, exports and snapshots.

## 2. Create a storage class

The storage class is how you tell Kubernetes where the Qumulo cluster is.
Add it to the values file you installed with, then upgrade:

```yaml
# qumulo-values.yaml -- add this section
storageClasses:
  - name: qumulo-nfs                 # <-- EDIT (the name people will ask for)
    server: qumulo.example.com       # <-- EDIT (host name or IP; no https://, no port)
    secretName: qumulo-api-token     # <-- EDIT if you named the Secret differently
    basePath: /k8s/volumes           # <-- EDIT (must exist, and be shared over NFS)
    protocol: nfs4.1
    isDefault: false
    allowVolumeExpansion: true
    mountOptions:
      - hard
      - timeo=600
```

```bash
helm upgrade qumulo-csi oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver \
  --version 0.1.0+<commit> \
  --namespace qumulo-csi \
  --values qumulo-values.yaml
```

Nothing restarts — the upgrade just adds the storage class.

```bash
kubectl get storageclass qumulo-nfs
```

!!! warning "Always pass your full values file"
    Use `-f qumulo-values.yaml` on every upgrade. Running `helm upgrade --set`
    on its own resets every other setting back to the chart's default.

!!! warning "Delete the `[]` when you add classes"
    The chart ships `storageClasses: []`. Leaving the empty brackets in place
    above your list is a YAML error, not an override:

    ```yaml
    storageClasses: []          # WRONG -- remove the [] first
      - name: qumulo-nfs
    ```

!!! warning "`server` is a host name, not a URL"
    `https://host:8000` and `host:8000` are both rejected. If the Qumulo
    management interface is not on the default port, leave `server` as the
    bare host and set the class's own `apiUrl` instead.

!!! tip "A misspelled setting stops the upgrade"
    Type `protocl` instead of `protocol` and the upgrade stops and names the
    word it did not recognise, rather than ignoring it quietly.

Other setups — one share per volume, separate team networks, scheduled
snapshots, more than one Qumulo cluster — are under
[Storage class](../provision/storageclass/index.md).

## 3. Ask for a volume

A PersistentVolumeClaim is a request for storage. This is what application
teams write; they only need the class name.

```yaml
# pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-data
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: qumulo-nfs
  resources:
    requests:
      storage: 10Gi
```

```bash
kubectl apply -f pvc.yaml
kubectl get pvc my-data
```

Within a few seconds it should say `Bound`. That means the driver has created
a folder on the Qumulo cluster with a 10 GiB limit on it.

!!! note "If it stays `Pending`"
    `kubectl describe pvc my-data` and read the events at the bottom — the
    reason is almost always written there in plain words. The usual causes are
    a `basePath` that is not shared over NFS yet, or a token the Qumulo
    cluster will not accept.

## 4. Use it from a pod

A claim on its own proves the driver can talk to Qumulo. Mounting it proves
the whole path works, including the network between your nodes and the
cluster.

```yaml
# pod.yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-app
spec:
  containers:
    - name: app
      image: busybox
      command: ["sh", "-c", "echo hello from $HOSTNAME > /data/hello.txt && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: my-data
```

```bash
kubectl apply -f pod.yaml
kubectl wait --for=condition=Ready pod/my-app --timeout=60s
kubectl exec my-app -- cat /data/hello.txt
```

If you see `hello from my-app`, the driver is working end to end: Kubernetes
asked for storage, the driver created it on Qumulo, and a pod mounted it and
wrote to it.

!!! tip "The real test of shared storage"
    Start a second pod with the same `claimName` and it will see the same
    file. That is the point of `ReadWriteMany`: many pods, on many machines,
    sharing one volume.

## 5. Clean up the test

```bash
kubectl delete pod my-app
kubectl delete pvc my-data
```

Deleting the claim removes the folder on the Qumulo cluster too, so do this
only with the test volume.