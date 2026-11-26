# Performance validation suite

Measures how fast the Qumulo CSI driver serves CSI calls, how that scales, and where its limits are, then writes `PERFORMANCE-VALIDATION-REPORT.md`. Every result is printed next to its limit in [`scenarios.yaml`](scenarios.yaml), and the scenarios that repeat its loads are compared with a baseline from an earlier resource-report run (`baseline` in `scenarios.yaml`). `./run.sh run --publish` copies the report to `docs/PERFORMANCE-VALIDATION-REPORT.md`.

It is meant to be **run by hand**. It reports numbers, not a verdict, and it is not a CI gate: the arrays are shared, so latency is noisy, and a full run takes about 25 minutes.

## Quick start

```sh
cd test/performance
./run.sh check            # every line OK: tools, node image, binaries, arrays A-D, tokens, store paths
./run.sh list             # what a run does
./run.sh run --quick      # ~8 min, a quarter of each load: a smoke test of the setup
./run.sh run --publish    # ~25 min, the full report; copies it to docs/
```

Run the full report under `nohup` or `tmux`. If the run is killed, it skips its own cleanup (see Troubleshooting):

```sh
nohup ./run.sh run --publish > runs/console.log 2>&1 &   tail -f runs/console.log
```

Other commands:

- `./run.sh run --only=B-serial,B-burst200` runs only the scenarios named.
- `./run.sh render runs/<run>` re-renders an existing run, for example after editing a threshold.

## Configuration

The suite reads `PERF_CONFIG=<path>` if set, else `performance.env` in this folder (git-ignored; copy `performance.env.example`). The keys are the resource report's (`../resource-report/resource-report.env.example`) plus the arrays-and-placement keys below; each is documented in `performance.env.example`.

| Setting | Effect here |
|---|---|
| `ARRAY_A_*`, `ARRAY_B_*` | two **different** clusters. Every single-array scenario uses one of them |
| `ARRAY_C_*`, `ARRAY_D_*` | further node addresses, used by `BCD-addresses`. In the lab, B, C and D are three node addresses of one cluster |
| `NODE_ARRAYS` | arrays that may run mount scenarios. In the lab: B, C, D. Array A refuses NFS mounts from this host |
| `NODE_RUNTIME`, `NODE_IMAGE` | as in the resource report: the node process runs in the node image (default) or on the host |
| `PERF_NAME_PREFIX` | name prefix of every object created (default `csi-perf`, apart from the resource report's `csi-rcr`) |
| `PERF_CONTROLLER_METRICS_PORT`, `PERF_NODE_METRICS_PORT` | local metrics ports (default 29674 / 29675) |

Lab setup on 2026-10-05:

| Array | Address | Cluster |
|---|---|---|
| A | `198.51.100.10` | cluster-a (Core 7.9.3) |
| B | `203.0.113.78` | cluster-b (Core 7.10.0) |
| C | `203.0.113.76` | cluster-b |
| D | `203.0.113.56` | cluster-b |

## What it runs

`run.sh run` does the following, in order:

1. builds the driver and csiload from this checkout;
2. plans the scenarios from `scenarios.yaml`;
3. records the build and each array's identity (REST GET only);
4. runs the load through `test/resource-report/scripts/driver_footprint.py --scenarios=<plan>`. That needs sudo: the node process mounts NFS;
5. lists leftovers (GET only);
6. analyses the run, evaluates the thresholds and renders the report.

The run folder is `runs/<date>-<commit>[-quick]/`:

| File | Contents |
|---|---|
| `plan.json` | the scenarios as run |
| `meta.json` | the build and the arrays |
| `footprint/` | raw samples, per-call records and driver logs |
| `results.json` | the analysed figures |
| `checks.json` | every limit with its measured value |
| `PERFORMANCE-VALIDATION-REPORT.md` | the report |

| Group | Scenarios | Question |
|---|---|---|
| latency | `{A,B}-serial`, `{A,B}-serial-dedicated` | what one call costs with nothing else running |
| baseline | `{A,B}-shared`, `{A,B}-dedicated`, `{A,B}-burst200` | the resource report's loads. Is this build slower than the baseline? |
| scaling | `B-sweep-c{1,4,16,32}`, `A-sweep-c{1,16,32}` (A only in full runs) | where more parallel calls stop paying |
| addresses | `BCD-addresses` | one cluster through three node addresses at once |
| isolation | `AB-concurrent-shared` | two clusters from one controller: does one slow the other? |
| mounts | `B-mounts100` | 100 live NFS 4.1 mounts held on one node |

## Limits

`scenarios.yaml` holds each limit's ID, value and basis. The report prints every limit with the value the run measured for it and where it was measured. **It computes no pass/fail verdict.** `run.sh` exits non-zero only when a step itself breaks.

- **Absolute limits.** No call ends non-OK. No call outlives the sidecars' 60 s timeout. Also: p95 latency per call, throughput floors at concurrency 16, REST calls per call, and RSS against the chart.
- **Relative to the baseline or the expected shape.** The p95 ratio and the RPC/s ratio against the baseline. The create rate from concurrency 1 to 16. One cluster's p95 while the other is loaded.

`--quick` computes no baseline ratios or throughput floors, because a quarter-size load is not comparable. Those rows show `–`.

The create/delete latency and delete-cost limits (T-03, T-04, T-15) are measured only up to 16 calls in flight per cluster (`max_cluster_inflight`), counted across all of a cluster's node addresses. That is the most the baseline measured. Rows past it are listed with their values in the "Where" column, and in §3–§4.

To change a limit, edit `scenarios.yaml` and keep its basis text accurate. Then re-render: `./run.sh render runs/<run>`.

## Limitations

The report's limitations section is `scripts/limitations.md`, and the report copies it verbatim. It covers three things:

- the hard limits the driver and chart impose;
- the known performance limitations still open;
- what this suite does not measure. The main gaps are Kubernetes overhead, data-path throughput, multi-node mount density and arrays with many objects.

Keep it in step when a defect is fixed or a new limit lands.

## Safety on shared arrays

- Everything is created under the configured store paths, named `csi-perf-*`, and deleted by the run.
- The leftover check lists anything still carrying the prefix.
- Nothing outside the prefix is touched.
- The run never uses Kubernetes, so the dev `minikube` cluster is never involved.

## Troubleshooting

| Symptom | Fix |
|---|---|
| run killed midway | `sudo docker rm -f csi-perf-node`; `sudo pkill -f test/resource-report/bin/qumulo-csi-driver`; then `(cd ../resource-report/scripts && RR_CONFIG=<config> python3 leftovers.py csi-perf)` to see what is left |
| `check` fails on the node image | `(cd ../.. && make docker-build-node)` |
| mount scenarios fail on an array | leave that array out of `NODE_ARRAYS` |
| one latency figure far above the rest | the arrays are shared: rerun that scenario with `--only=<name>` before reading it as a regression |
