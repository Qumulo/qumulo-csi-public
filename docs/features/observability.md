# Observability

What the driver tells you about itself, and what you already get without
turning anything on.

## What you get for free

Per-volume disk usage is published by Kubernetes itself, through the kubelet.
Your existing dashboards and alerts keep working with no changes, and nothing
here has to be enabled for that.

```bash
kubectl get --raw "/api/v1/nodes/<node>/proxy/metrics" | grep kubelet_volume_stats
```

Two delays add up before a write shows here. The Qumulo cluster reports new
data to the NFS client about 15 seconds after it is written, and the kubelet
re-reads each volume only every one to two minutes and shows its last reading
in between. So a volume written to just now can read low, even zero, for up to
about two minutes.

## Turn on the driver's own metrics

```yaml
metrics:
  enabled: true
  serviceMonitor:
    enabled: true     # if you run the Prometheus operator
  prometheusRule:
    enabled: true     # ships ready-made alerts
  dashboard:
    enabled: true     # ships the Grafana dashboard as a ConfigMap
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

`prometheusRule` installs alerts for the conditions worth waking up for.

!!! note "Array figures are as fresh as the last request"
    The driver never polls an array. Reachability, quota usage, capacity and
    replication age are updated when a request touches that array or volume,
    so on an idle cluster they can be hours old. Request counts and latency
    have no such delay. See [Monitoring](../operate/monitor.md) before you
    build an alert on an array-side figure.

## The dashboard

`metrics.dashboard.enabled` ships the chart's Grafana dashboard as a
ConfigMap. Where it goes depends on who imports it:

| You use | Set | Result |
|---|---|---|
| Grafana from kube-prometheus-stack | nothing more (the default label is `grafana_dashboard: "1"`) | Grafana's sidecar imports it |
| OpenShift console | `metrics.dashboard.namespace: openshift-config-managed` and `metrics.dashboard.labels: {console.openshift.io/dashboard: "true"}` | appears under Observe > Dashboards |
| Any other Grafana | leave it off; import `dashboards/qumulo-csi.json` from the chart by hand | pick a Prometheus datasource when importing |

## Check it is working

```bash
POD=$(kubectl -n qumulo-csi get pod -l app.kubernetes.io/component=controller -o name | head -1)
kubectl -n qumulo-csi port-forward "$POD" 29654:29654 &
curl -s localhost:29654/metrics | grep csi_qumulo_build_info
```

29654 is `metrics.port.controller`; the node pods serve theirs on
`metrics.port.node` (29655) on each node's own address.

## Logs

```bash
kubectl -n qumulo-csi logs -l app.kubernetes.io/component=controller -c qumulo-csi-driver
kubectl -n qumulo-csi logs -l app.kubernetes.io/component=node -c qumulo-csi-driver
```

Turn up the detail while diagnosing something, then put it back — `debug` is
verbose:

```yaml
controller:
  logLevel: debug
node:
  logLevel: debug
```
