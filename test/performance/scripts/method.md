- **What runs.** The driver binary is built from this checkout (`test/resource-report/run.sh build`) and started as two processes, the way the chart deploys it:
  - a controller (`-mode=controller`, as the invoking user, like the chart's non-root controller);
  - a node (`-mode=node`). With `NODE_RUNTIME=docker` the node runs inside the node image with the fresh binary bind-mounted over the image's own, privileged on the host network like the DaemonSet pod.

  `csiload` calls their unix sockets directly, as csi-provisioner, csi-resizer, csi-snapshotter and the kubelet would. Both processes run with `-insecure-skip-tls-verify`, because the lab arrays use self-signed certificates.
- **What each scenario does.** Each one creates `n` volumes at the given concurrency, then optionally expands some, snapshots some (create and delete), publishes some (real NFS 4.1 mounts), stats them, holds the mounts, and unpublishes. Last, it deletes every volume. Runs listed in one scenario go at the same time, all through the one controller.
- **Retries.** `Aborted` is CSI's "call again" answer: a tree delete that outlived its 10 s budget, or a restore that outlived its call. csiload retries it up to 20 times with 1–5 s backoff, as a sidecar does. The recorded latency of such a call is the time until it ended OK, including the retries, and the retry count is reported with it.
- **How each figure is measured.**
  - **Latency:** per call, measured by csiload.
  - **RPC/s:** calls divided by the phase's wall time.
  - **REST/RPC:** the controller's own `csi_qumulo_api_requests_total` over the phase, divided by every RPC in that phase across the scenario's arrays. One controller serves them all.
  - **Footprint:** `/proc` and `/metrics`, sampled once a second for each process.
  - **Mounts:** NFS entries under the target root in the node process's mount table.
- **Baseline.** Every scenario in §2 (and AB-concurrent-shared in §5) repeats a load from an earlier resource consumption report on the same array, so its p95 and RPC/s are compared one-to-one. The other scenarios have no baseline counterpart and are shown only against the absolute limits.
- **Cleanup.** Everything is named `<NAME_PREFIX>-<scenario>-<array>-NNN` (default prefix `csi-perf`) and deleted by csiload. Mounts left under `/var/tmp/<NAME_PREFIX>/targets` are force-unmounted on exit. `scripts/leftovers.py` then lists any object still carrying the prefix on either cluster (§8).
- **Reproduce.** Run `test/performance/run.sh run` (add `--quick` for about a quarter of the load). For one scenario, add `--only B-serial,B-burst200`. To re-render from an existing run folder, use `run.sh render runs/<run>`. See `test/performance/README.md`.
