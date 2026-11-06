# Verify the installation

Use these checks to confirm that the Qumulo CSI driver is ready to use

!!! note "On OpenShift, check the SCCs too"
    Every check here applies, with `oc` for `kubectl`. Add the SCC check from
    [Which SCC is each pod using](../operate/openshift-operations.md#which-scc-is-each-pod-using) —
    a wrong SCC is the one failure mode these checks do not surface.
## Check the pods are running

```bash
kubectl -n qumulo-csi get pods
```

Expect two controller pods (one leader, one standby) and one node pod per
node, all with every container ready.

```console
NAME                                        READY   STATUS    RESTARTS   AGE
qumulo-csi-controller-7d8faaaaaa-k2xp9      4/4     Running   0          2m
qumulo-csi-controller-7d8faaaaaa-m7rdq      4/4     Running   0          2m
qumulo-csi-node-4vh2n                       3/3     Running   0          2m
qumulo-csi-node-8ltkz                       3/3     Running   0          2m
```

A pod in `ErrImagePull` or `ImagePullBackOff` never started, so its logs are
empty — the reason is in its events.

## Check every node has a plugin pod

```bash
kubectl -n qumulo-csi get pods -l app.kubernetes.io/component=node -o wide
```

Compare the count with `kubectl get nodes`. A node without a plugin pod cannot
mount this driver's volumes.

## Check every node registered the driver

The check people skip, and the one that explains a pod stuck in
`ContainerCreating` on one node but not another:

```bash
kubectl get csinodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.drivers[*].name}{"\n"}{end}'
```

Every line must show `csi.qumulo.com`.

!!! warning "An empty entry means that node cannot mount"
    Any pod scheduled there hangs in `ContainerCreating`. The usual cause is a
    non-default `kubeletDir` — k3s and microk8s in particular. See
    [Troubleshoot Guide](../operate/troubleshoot-mounts.md).

!!! note "Provisioning needs at least one registered node plugin"
    The driver advertises CSI topology, so with no node plugin ever registered,
    provisioning does not proceed at all. Every new PV also carries a
    `nodeAffinity` satisfied by any node running the DaemonSet.

## Check which images were pulled

```bash
kubectl -n qumulo-csi get pods \
  -o custom-columns='POD:.metadata.name,IMAGE:.status.containerStatuses[?(@.name=="qumulo-csi-driver")].imageID'
```

```console
POD                                      IMAGE
qumulo-csi-controller-7d8f9c6b4d-k2xp9   ghcr.io/qumulo/qumulo-csi-driver-controller@sha256:1a2b3c...
qumulo-csi-node-4vh2n                    ghcr.io/qumulo/qumulo-csi-driver-node@sha256:4d5e6f...
```

The digest is what was really pulled, whatever tag was asked for — worth
checking after a mirrored or per-build install.

## Create a test volume

!!! note "This step needs a storage class"
    The checks above test the installation itself and work straight after
    `helm install`. This one needs a Qumulo cluster configured, which is
    [Create your first volume](first-volume.md) — do that first, or skip to it now, since it
    walks through the same test with more explanation.


A PVC that binds proves the controller and the token work. Only a pod that
**writes** proves the mount does.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: qumulo-test
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-nfs          # <-- EDIT to match your class
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: qumulo-test
spec:
  containers:
    - name: app
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: qumulo-test}
```

```bash
kubectl apply -f qumulo-test.yaml
kubectl wait --for=condition=Ready pod/qumulo-test --timeout=120s
```

Then three checks:

- **The mount reached the kernel** with the protocol you asked for:

    ```bash
    kubectl exec qumulo-test -- mount | grep /data
    #    -> qumulo.example.com:/k8s/volumes/pvc-... /data nfs4 rw,...,vers=4.1
    ```

- **The pod can write** — where a permissions problem surfaces:

    ```bash
    kubectl exec qumulo-test -- sh -c 'echo hello > /data/test && cat /data/test'
    ```

- **The bytes are on the Qumulo cluster.** The check nothing can fake:

    ```bash
    PV=$(kubectl get pvc qumulo-test -o jsonpath='{.spec.volumeName}')
    curl -sk -H "Authorization: Bearer $TOKEN" \
      "https://qumulo.example.com:8000/v1/files/%2Fk8s%2Fvolumes%2F$PV/entries/"   # <-- EDIT
    #    -> an entry named "test"
    ```

## Clean up and watch the delete

```bash
kubectl delete pod/qumulo-test pvc/qumulo-test
```

With the default `reclaimPolicy: Delete` the directory disappears from the
Qumulo cluster too. Worth watching once, so you trust it.