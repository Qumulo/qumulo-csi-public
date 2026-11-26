# Step 9 -- clean up

Delete in this order -- a pod still using a PVC will block its deletion, and
the PVC's directory on the Qumulo cluster is only removed once the PVC
itself is gone.

```sh
NS=qumulo-csi

kubectl -n $NS delete pod qumulo-example-pod --force --grace-period=0

# If you ran step 8:
kubectl -n $NS delete pvc qumulo-example-restored-pvc
kubectl -n $NS delete volumesnapshot qumulo-example-snapshot

kubectl -n $NS delete pvc qumulo-example-pvc
kubectl delete storageclass qumulo-example
```

Confirm the directory is actually gone from the cluster (deletion is
asynchronous -- the driver polls a tree-delete job, so this can take a
few seconds):

```sh
curl -sk -H "Authorization: Bearer $(cat /path/to/token.txt)" \
  "https://<qumulo-cluster-address>:8000/v1/files/%2Fcsi%2Fvolumes%2Fqumulo-example-pvc-.../info/attributes"
# expect a 404 fs_no_such_entry_error once it's finished
```

To remove the driver itself:

```sh
kubectl delete secret -n qumulo-csi qumulo-api-token
helm uninstall qumulo-csi -n qumulo-csi
kubectl delete namespace qumulo-csi    # also removes the ghcr-pull secret
rm -f ~/.ghcr-pat
```
