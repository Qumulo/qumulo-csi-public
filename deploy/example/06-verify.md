# Step 6 -- prove it actually works

Installing is not the same as working -- check both ends.

```sh
NS=qumulo-csi

# The PVC should bind quickly. If it sits Pending, the events say why:
kubectl -n $NS get pvc qumulo-example-pvc
kubectl -n $NS describe pvc qumulo-example-pvc | tail -10

# The pod should reach Running.
kubectl -n $NS get pod qumulo-example-pod
```

Once the pod is `Running`:

```sh
# Confirm the mount actually landed, and with which NFS version.
kubectl -n $NS exec qumulo-example-pod -- grep /data /proc/mounts

# Write from the pod...
kubectl -n $NS exec qumulo-example-pod -- sh -c 'echo hello-from-the-pod > /data/proof.txt'

# ...and read it back.
kubectl -n $NS exec qumulo-example-pod -- cat /data/proof.txt
```

If you want to confirm the write really reached the Qumulo cluster and not
just the pod's own cache, read the same file back through the REST API:

```sh
PV=$(kubectl -n $NS get pvc qumulo-example-pvc -o jsonpath='{.spec.volumeName}')
REAL_PATH=$(kubectl get pv "$PV" -o jsonpath='{.spec.csi.volumeAttributes.realPath}')
echo "volume directory on the cluster: $REAL_PATH"

# The REST API addresses a file by percent-encoding every "/" in its path.
ENCODED_PATH=$(echo "${REAL_PATH}/proof.txt" | sed 's#/#%2F#g')
curl -sk -H "Authorization: Bearer $(cat /path/to/token.txt)" \
  "https://<qumulo-cluster-address>:8000/v1/files/${ENCODED_PATH}/data"
```

That should print `hello-from-the-pod`. If it does, the whole path -- CSI
RPCs, the NFS mount, and the REST API -- is proven end to end.
