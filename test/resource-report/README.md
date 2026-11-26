# Resource Consumption Report suite

This suite measures what the driver costs to run, on the driver side and on the arrays:

- **Driver side:** CPU, memory, goroutines, file descriptors and connections of the controller and node processes.
- **Array side:** REST calls, connections, IOPS, latency, and the objects created.

It drives the driver through a scripted volume lifecycle against two Qumulo clusters and writes `RESOURCE-CONSUMPTION-REPORT.md`. `./run.sh report --publish` copies it to `docs/RESOURCE-CONSUMPTION-REPORT.md`.

The suite needs no Kubernetes. It runs the driver binary built from this checkout, as two local processes, the way the chart deploys it, and calls the CSI gRPC sockets directly, the way the sidecars and kubelet would.

## Quick start (any Linux machine)

```sh
cd test/resource-report
cp resource-report.env.example resource-report.env && chmod 600 resource-report.env
$EDITOR resource-report.env          # two arrays: addresses, token files, store paths
./run.sh build                       # bin/qumulo-csi-driver + bin/csiload from this checkout
./run.sh check                       # every line should be OK
./run.sh report --quick              # ~10 min smoke run at 20% size
./run.sh report --publish            # ~40 min full run; copies the report to docs/
```

Run the full report under `nohup` or `tmux`. A killed run skips its cleanup (see [Troubleshooting](#troubleshooting)).

```sh
nohup ./run.sh report --publish > runs/report.log 2>&1 &   tail -f runs/report.log
```

## Requirements

| Need | Why |
|---|---|
| Linux, `sudo` | the node process mounts NFS. The controller runs as your user |
| Go (the version in `go.mod`) | `./run.sh build` |
| Python 3.10+ with PyYAML | collectors, analysis, rendering |
| Docker plus the node image (`make docker-build-node` from the repo root) | **`NODE_RUNTIME=docker`** (default). The node process runs in the image for its `mount.nfs`, with the freshly built binary bind-mounted over the image's own. The image never needs rebuilding for code changes |
| *or* `nfs-common` installed | **`NODE_RUNTIME=host`**. The node process runs directly on the machine |
| Network to both arrays | REST on `:8000`, NFS on `:2049` |
| Two Qumulo clusters, each with an admin-class token and existing scratch directories | see Configuration |

## Configuration: `resource-report.env`

Copy `resource-report.env.example` to `resource-report.env`. The copy is git-ignored. Environment variables of the same name override the file, and `RR_CONFIG=<path>` points the suite at a different file.

| Key | Meaning |
|---|---|
| `ARRAY_{A,B}_SERVER` | NFS server address, the StorageClass `server` |
| `ARRAY_{A,B}_API` | REST URL, the StorageClass `apiUrl` (`https://<address>:8000`) |
| `ARRAY_{A,B}_TOKEN_FILE` | file holding the token, either the raw bearer token or the JSON login response `{"bearer_token": "..."}`. Relative paths are relative to this folder. Keep it chmod 600 and out of git |
| `ARRAY_{A,B}_SHARED_REAL` / `_SHARED_EXPORT` | an **existing** directory for shared-subdirectory volumes, and the NFS path it is seen through. An export must already cover it |
| `ARRAY_{A,B}_DEDICATED_REAL` / `_EXPORT` | an **existing** directory for dedicated-export volumes. The driver creates one export per volume there |
| `NODE_RUNTIME`, `NODE_IMAGE` | `docker` (default) with `qumulo-csi-driver-node:0.1.0`, or `host` |
| `NAME_PREFIX` | every object the run creates starts with this (default `csi-rcr`). Cleanup and the leftover check key on it |
| `OUT_DIR` | where runs are written (default `runs/`, git-ignored) |
| `CONTROLLER_METRICS_PORT`, `NODE_METRICS_PORT` | local metrics ports (default 29654 / 29655) |

`./run.sh check` checks each item it can:

- the tools, and the node image or `mount.nfs`;
- both binaries;
- that each array answers;
- that each token is accepted;
- that every store path exists.

## What `./run.sh report` does

| Step | Script | Touches the arrays |
|---|---|---|
| 1. Baseline inventory | `scripts/array_inventory.py` | REST GET only: capacity, nodes, network, exports, quotas, snapshots and policies, replication, directory aggregates, connections, analytics |
| 2. Driver footprint | `scripts/driver_footprint.py` (sudo) → `bin/csiload` | creates and deletes `<NAME_PREFIX>-*` volumes, snapshots, quotas and exports in the configured store paths, and NFS-mounts them |
| 3. After inventory | `scripts/array_inventory.py` | GET only |
| 4. Leftover check | `scripts/leftovers.py` | GET only. Lists any `<NAME_PREFIX>` object still present |
| 5. Analyse | `scripts/analyze.py` → `summary.json` | GET only: each array's analytics for the run window |
| 6. Render | `scripts/render.py` → `RESOURCE-CONSUMPTION-REPORT.md` | none |

Step 2 runs these scenarios in order (`--quick` runs 20% of each size):

| Scenario | Load |
|---|---|
| `idle-baseline` | both processes up, no RPCs, 120 s |
| `A-shared`, `B-shared` | 50 creates at concurrency 8 → expand 10 → snapshot create and delete 10 → publish 10 (real NFS 4.1 mounts, held 60 s), stats, unpublish → delete 50 |
| `A-dedicated`, `B-dedicated` | 20 dedicated-export volumes at concurrency 4, publish 5, delete |
| `AB-concurrent-shared` | 50 creates and deletes on each array at once (one controller, two clusters) |
| `A-burst200`, `B-burst200` | 200 creates and deletes at concurrency 16 |
| `idle-after` | 120 s idle after cleanup. Memory, goroutines and connections should return to their plateau |

Phases inside a scenario are separated by 30 s of idle (5 s with `--quick`). To run a subset, repeat `--only=` once per scenario:

```sh
./run.sh report --only=B-shared --only=idle-after
```

### Sampling

| What | Source |
|---|---|
| Each driver process, every 1 s | `/proc`: CPU time, RSS and high-water mark, threads, FDs, ESTABLISHED TCP to `:8000` (REST) and `:2049` (NFS), NFS mounts in the node's mount namespace |
| Driver metrics | `/metrics`: Go runtime and driver counters. A full scrape is saved every 30 s, and it carries the per-endpoint REST and per-RPC counts |
| Arrays, every 10 s | `GET /v2/network/connections/` |
| Per RPC | one JSON line from `csiload`: phase, call, volume, latency, gRPC code, error |

## Output

```
runs/<date>-<time>-<commit>/
  RESOURCE-CONSUMPTION-REPORT.md   the report
  findings.md                      optional written summary, placed after the header (see below)
  meta.json                        build, times, host, chart resource requests
  baseline/array-{A,B}.json        array inventory before
  after/array-{A,B}.json           and after
  leftovers.txt                    must say 0 objects for both arrays
  footprint/
    summary.json                   every figure the report quotes
    scenarios.json                 scenario start/end times
    samples-proc.jsonl             1 s process samples
    samples-array.jsonl            10 s array samples
    ops-<scenario>-<A|B>.jsonl     one line per RPC
    metrics-full/*.prom            full /metrics scrapes
    analytics-{A,B}.json           array analytics for the window
    driver-{controller,node}.log   driver logs
```

`runs/` is git-ignored because its files carry array addresses. The published report under `docs/` is the durable record.

### The written summary

The generated tables say *what* was measured. The *Summary* section of the published report says what it *means*: causes, code references, recommendations. That section is `findings.md` in the run folder, and the renderer places it after the header.

To update the analysis for new code:

1. Copy the previous summary (the text between `## Summary` and the first `---` in the last published `RESOURCE-CONSUMPTION-REPORT.md`) to a file.
2. Run `./run.sh report`, compare the new tables, and edit the file.
3. Re-render and publish without rerunning the load:

```sh
./run.sh render runs/<run> --findings my-findings.md --publish
```

`render` re-analyses an existing run and creates nothing on the arrays.

## Comparing builds

```sh
git checkout <new code> && ./run.sh build && ./run.sh report
python3 - runs/<old>/footprint/summary.json runs/<new>/footprint/summary.json <<'EOF'
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:3])
for x, y in zip(a["scenarios"], b["scenarios"]):
    for role in ("controller", "node"):
        for k in ("rss_mb_peak", "cpu_s", "goroutines_end", "rest_conns_end"):
            print(f'{x["name"]:22} {role:10} {k:15} {x[role][k]!s:>8} -> {y[role][k]!s:>8}')
EOF
```

## Troubleshooting

| Symptom | Meaning / fix |
|---|---|
| `NodePublishVolume` fails `mount.nfs: Remote I/O error` on one array | the array refused the mount (EREMOTEIO). On the 2026-10-04 lab, array A did this for every mount. The other phases on that array are still valid |
| `bad option … need a /sbin/mount.nfs helper` | `NODE_RUNTIME=host` without `nfs-common`. Install it or use `docker` |
| `token rejected` in `check` | the token expired. Log in again and replace the token file |
| `store path … missing` in `check` | CreateVolume needs the store root to exist already. Point the config at an existing directory |
| `leftovers.txt` lists objects | a run was interrupted. `python3 scripts/leftovers.py` lists them. Delete only those (`<NAME_PREFIX>-*`) objects, in the array UI or by REST |
| run killed midway | `sudo docker rm -f <NAME_PREFIX>-node; sudo pkill -f test/resource-report/bin/qumulo-csi-driver`, then check leftovers |
| metrics port already in use | another driver with metrics runs locally. Stop it, or change the ports in the config |

## Ground rules on shared arrays

- The suite creates objects **only** inside the configured store paths, and **only** with `NAME_PREFIX`. It deletes everything it creates.
- Everything outside step 2 is read-only.
- Other people's objects on the arrays are reported, never touched.
- Other clients' activity during a run shows up in the array-side figures. The 2026-10-04 run had one; its report says so.

## Files

| File | Role |
|---|---|
| `run.sh` | entry point |
| `resource-report.env.example` | configuration template |
| `csiload/` | Go load generator: CSI calls over the driver's sockets, one JSON line per RPC. Built by `./run.sh build` (`go build ./test/resource-report/csiload`) |
| `scripts/rr.py` | configuration loader and read-only REST client shared by the scripts |
| `scripts/array_inventory.py` | before/after array inventory |
| `scripts/driver_footprint.py` | starts both driver processes, samples them and the arrays, runs the scenarios, cleans up |
| `scripts/leftovers.py` | leftover check |
| `scripts/analyze.py` | samples → `summary.json` |
| `scripts/render.py` | `summary.json` → markdown report |
| `scripts/method.md` | the Method section embedded in every report |
