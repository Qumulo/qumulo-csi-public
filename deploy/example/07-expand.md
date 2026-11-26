# Step 7 -- grow the volume while the pod keeps running

Expansion is a quota change on the Qumulo cluster, not a node-side
operation, so the pod never needs to restart or unmount.

```sh
NS=qumulo-csi

kubectl -n $NS patch pvc qumulo-example-pvc -p \
  '{"spec":{"resources":{"requests":{"storage":"3Gi"}}}}'

# Watch the PVC's capacity catch up -- this can take a few seconds.
kubectl -n $NS get pvc qumulo-example-pvc -w
```

Once it shows `3Gi`, confirm the pod's own view followed it (the mount's
`df` reads the quota, not the cluster's total capacity):

```sh
kubectl -n $NS exec qumulo-example-pod -- df -h /data
```
