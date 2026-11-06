# Observability

What the driver tells you about itself, and what you already get without
turning anything on.

## What you get for free

Per-volume disk usage is published by Kubernetes itself, through the kubelet.
Your existing dashboards and alerts keep working with no changes, and nothing
here has to be enabled for that.

```bash
kubectl get --raw /metrics | grep kubelet_volume_stats
```

## Turn on the driver's own metrics

```yaml
metrics:
  enabled: true
  serviceMonitor:
    enabled: true     # if you run the Prometheus operator
  prometheusRule:
    enabled: true     # ships ready-made alerts
```

That adds what the kubelet cannot see, because it comes from the Qumulo
cluster rather than from the mount:

| | |
|---|---|
| Request timings and counts | per operation, so you can see what is slow |
| Qumulo API latency | how long the cluster itself is taking |
| Array reachability | per cluster, from the driver's point of view |
| Quota and usage per volume | the array's view, not the filesystem's |
| Array capacity | free space per cluster |
| Replication lag | how far behind each copy is |

A Grafana dashboard ships with the chart, and `prometheusRule` installs
alerts for the conditions worth waking up for.

## Check it is working

```bash
kubectl -n qumulo-csi port-forward deploy/qumulo-csi-controller 8080:8080
curl -s localhost:8080/metrics | grep csi_qumulo
```

## Logs

```bash
kubectl -n qumulo-csi logs deploy/qumulo-csi-controller
kubectl -n qumulo-csi logs ds/qumulo-csi-node
```

Turn up the detail while diagnosing something, then put it back — `debug` is
verbose:

```yaml
controller:
  logLevel: debug
node:
  logLevel: debug
```
