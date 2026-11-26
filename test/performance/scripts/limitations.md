## Limits the driver and chart impose

These are fixed in code or in the chart. Every number in this report sits inside them, and a load that crosses one changes behavior, not just speed.

| Limit | Value | What happens at the limit | Where |
|---|---|---|---|
| Sidecar RPC timeout (provisioner, resizer, snapshotter) | 60 s | the sidecar cancels the call and retries it, so the work is repeated | `charts/qumulo-csi-driver/values.yaml` `sidecars.*.timeout` |
| REST request timeout | 10 s | one REST call fails, and with it the CSI call | `cmd/qumulo-csi-driver/main.go` `-qumulo-api-timeout`; chart `apiTimeout` |
| DeleteVolume tree-delete wait | 10 s, polled every 500 ms | answers `Aborted`; the CO retries and the retry waits on the same job | `pkg/driver/controller_server.go` `defaultTreeDeleteBudget` |
| Restore from snapshot | 50,000 entries / 2 TiB | `ResourceExhausted` before anything is copied | `pkg/driver/restore.go` `maxRestoreFiles`, `maxRestoreBytes` |
| Restore per-call budget | deadline minus a quarter (margin clamped 2–10 s); 40 s when the CO sets none | answers `Aborted`; the next call resumes where this one stopped | `pkg/driver/restore.go` |
| One `copy-chunk` call | 32 MiB | a file is copied by looping until the array answers null | `pkg/qumulo/client.go` |
| Mount | 90 s | NodePublishVolume fails | `pkg/driver/node_server.go` `defaultMountTimeout` |
| Stat / statfs | 10 s | NodeGetVolumeStats reports the volume abnormal | `pkg/driver/node_server.go` `defaultStatTimeout` |
| Unmount before escalating to a forced unmount | 30 s | escalates to `umount -f`, then a lazy unmount | `cmd/qumulo-csi-driver/main.go` `-unmount-timeout` |
| Idle REST connections kept per array | 32 (`maxIdleConnsPerHost`), expiring after the cloned default transport's 90 s | a burst wider than 32 per array pays a new TCP and TLS session per call | `cmd/qumulo-csi-driver/main.go` `newHTTPClient`, `maxIdleConnsPerHost` |
| Controller replicas | 1 (no HA); `resources: {}` (BestEffort) | the controller is the first pod evicted under node pressure | chart `controller.replicaCount`, `controller.resources` |
| Node driver container | requests 20m CPU / 64Mi, no limits | none: no limit to hit, by design | chart `node.resources.driver` |
| Kubernetes client | client-go defaults: 5 QPS, burst 10 | ListVolumes / inventory reads queue behind that rate | `pkg/k8s` (no QPS set) |

**The driver has no backoff, rate limit or concurrency cap of its own.** Concurrency is whatever the sidecars send, by default 100 csi-provisioner workers. This suite reaches concurrency 32. It does retry one class of failure inside the call: a 429 or 503 is tried up to `retryAttempts` = 3 times (`pkg/qumulo/client.go`), so a single transient refusal never reaches the CO.

## Known performance and scale limitations

These come from earlier Kubernetes-impact testing and the resource consumption report. Statuses are as reported there, for an earlier build. They have not been re-verified on this build unless a row in this report covers them.

| Limitation | Status (earlier build) | Effect on the figures here |
|---|---|---|
| `DeleteVolume` answers **`FailedPrecondition`** — a terminal code — when the array simply did not answer the resolve call. `CreateVolume` reports the same condition as `Unavailable` | **open, this build** | 28 of 28 deletes in `A-sweep-c32` and part of the 14 in `A-burst200`. A CO treats `FailedPrecondition` as final, so the PV is stranded rather than retried. Distinct from the correct `FailedPrecondition` below |
| `DeleteVolume` refuses a directory that has neither the volume's export nor a quota — a half-finished create — with `FailedPrecondition` | **working as designed** | the conservative half of the 14 in `A-burst200` and 2 in `A-sweep-c16`. Correct: it will not tree-delete a directory it cannot confirm is the volume. Operational cost is that a create interrupted mid-flight leaves a directory only an operator can remove |
| idle REST connections never close with `-insecure-skip-tls-verify` | **fixed, measured** | `newHTTPClient` now clones `http.DefaultTransport` and keeps its 90 s expiry: **0** idle connections after the full run's 120 s idle, against 4 on the earlier build. A check with an idle window shorter than that 90 s expiry still sees them |
| REST connections are reused poorly: 949 calls over 138 TCP connections, 131 in TIME_WAIT | open | each new connection adds a TLS handshake (2.4 ms CPU on A, 3.8 ms on B) to some calls |
| create, delete and expand read whole collections (exports, snapshot policies, replication status); cost grows with the array's object count | open | **the lab arrays hold few objects, so these figures are a best case.** The mock-array tests measured 1.6–3.2 MB on the wire per call at 5,001 exports |
| a slow tree delete is polled: 20 polls and an `Aborted` after 9.6 s in one test | open | DeleteVolume latency and REST/RPC rise with the array's tree-delete speed; see the Aborted retry counts |
| one unreachable array slows calls to every array; each dead class holds the only capacity worker for 10 s | open | not exercised: every array here is healthy |
| a 503 from the array is not retried; array A answered 503 for about a minute during a 30-claim burst | partly fixed | a 503 here would show as a non-OK call under T-01 |
| restore re-copies an unfinished file from 0; restore memory is unbounded (126 MiB heap per attempt measured) | open | restore is not in this suite's load |
| idle GetCapacity polling: about 160 REST calls/min across two arrays, 34/min of them always-404 | open | not exercised: no csi-provisioner capacity worker runs here |
| one TCP connection per node per array address carries every mount | open | §6 mounts all share one NFS connection to B |
| ListVolumes is O(volumes² / page) | open | not exercised |
| metric series grow with endpoints and volumes (300,171 series, 123 MiB heap in one test) | partly fixed | not exercised at this scale |
| array A (`198.51.100.10`, cluster cluster-a) refuses NFS mounts from this host on every node (`Remote I/O error`) | lab defect, not the driver | mount scenarios run on B only; A's controller-side figures are valid |

## What this validation does not measure

- **Kubernetes overhead.** There are no sidecars, kubelet, API server or scheduler. A PVC's time to Bound is these CreateVolume figures plus csi-provisioner queueing (5 QPS / burst 10 against the API server by default) plus the PV write. Measuring that needs a Kubernetes cluster with workers, which the lab does not have yet. The chart does not expose csi-provisioner's `--worker-threads` or `--kube-api-qps`.
- **Data-path throughput.** Volumes stay empty. NFS read/write bandwidth and IOPS are the array's and the network's, not the driver's.
- **Mount density across nodes.** One node process on one host. The density in §6 is per node; 100 is the ceiling tested, not a limit found.
- **High object counts on the array.** See the whole-collection reads row above.
- **Failover, HA controller, array outage, Kerberos, SELinux, multi-tenant.** None are part of this load.
- **Replication and restore throughput.** Their limits are in the table above; their speed depends on the array's replication and copy-chunk rate.

## Environment limitations of this run

- **Shared arrays.** Other clients may use the same clusters during a run. Their load lands in the array's latency and therefore in these figures. A single slow phase is more likely the array than the driver; rerun before calling a regression.
- **Two clusters, not four.** A is cluster cluster-a; B, C and D are three node addresses of one cluster, cluster-b. The §4 address test shows how the driver spreads over node addresses, not how a second cluster behaves.
- **A and B differ.** They run different Qumulo Core versions (7.9.3 and 7.10.0, a debug build), and their TLS keys differ (2048 vs 4096 bit). Compare each array with its own baseline, not with the other array.
- **One client host.** Both driver processes and csiload run on this machine. Its CPU and network are shared with the load generator, which costs little: csiload only waits on gRPC.
- **Small samples in `--quick`.** A quarter of each load. Percentiles over a handful of calls are close to the maximum, and the baseline ratios (T-19, T-20) and throughput floors are not computed.
