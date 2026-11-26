#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

"""Performance validation for the Qumulo CSI driver. Driven by ../run.sh; the subcommands are its steps.

    perf.py plan    <run-dir> [--quick] [--only a,b]   scenarios.yaml -> <run>/plan.json (driver_footprint input)
    perf.py meta    <run-dir> [--quick]                build, host and array identity -> <run>/meta.json (REST GET only)
    perf.py analyze <run-dir>                          <run>/footprint/* -> <run>/results.json
    perf.py render  <run-dir>                          results + thresholds + baseline -> PERFORMANCE-VALIDATION-REPORT.md

The load itself is test/resource-report/scripts/driver_footprint.py with --scenarios=<run>/plan.json: the driver
binary as separate controller and node processes, csiload calling their sockets, both processes sampled every 1 s.
"""
import json, os, socket, statistics, subprocess, sys, time
from collections import Counter, defaultdict
from pathlib import Path

import yaml

SUITE = Path(__file__).resolve().parents[1]
REPO = SUITE.parents[1]
RR = REPO / "test/resource-report"
sys.path.insert(0, str(RR / "scripts"))
import rr  # noqa: E402
from analyze import counter_at, jl, pct, phase_windows, role_stats  # noqa: E402

SPEC = yaml.safe_load((SUITE / "scenarios.yaml").read_text())
NODE_OPS = ("NodePublishVolume", "NodeGetVolumeStats", "NodeUnpublishVolume")
OP_ORDER = ["CreateVolume", "ControllerExpandVolume", "CreateSnapshot", "DeleteSnapshot", *NODE_OPS, "DeleteVolume"]


def cfg_list(cfg, key, default):
    return [x.strip() for x in cfg.get(key, default).split(",") if x.strip()]


# ------------------------------------------------------------------------------------------------------------- plan
def plan(run, quick, only):
    cfg = rr.config()
    node_arrays = cfg_list(cfg, "NODE_ARRAYS", "B")
    sides = {s for s in "ABCDEFGH" if cfg.get(f"ARRAY_{s}_API")}
    k = SPEC["quick_scale"] if quick else 1.0
    out = []
    for sc in SPEC["scenarios"]:
        if only and sc["name"] not in only:
            continue
        if quick and sc.get("quick") is False:
            continue
        runs = []
        for r in sc["runs"]:
            if r["array"] not in sides:
                break
            o = {key: (max(2, int(r[key] * k)) if key in ("n", "expand", "snapshots", "publish") else r[key])
                 for key in ("n", "concurrency", "expand", "snapshots", "publish", "hold") if r.get(key)}
            if r["array"] not in node_arrays:
                o.pop("publish", None), o.pop("hold", None)
            o["publish"] = min(o.get("publish", 0), o["n"]) or None
            o = {key: v for key, v in o.items() if v}
            runs.append([r["array"], r["model"], o])
        else:
            out.append({"name": sc["name"], "group": sc["group"], "runs": runs})
            continue
        print(f"skip {sc['name']}: array {r['array']} is not configured")
    (run / "plan.json").write_text(json.dumps(out, indent=1))
    print(f"{len(out)} scenario(s) -> {run / 'plan.json'}")


# ------------------------------------------------------------------------------------------------------------- meta
def meta(run, quick):
    cfg = rr.config()
    m = {"run_id": run.name, "quick": quick, "host": socket.gethostname(), "started": time.strftime("%Y-%m-%d %H:%M %Z"),
         "name_prefix": cfg.get("NAME_PREFIX"), "node_arrays": cfg_list(cfg, "NODE_ARRAYS", "B"),
         "node_runtime": cfg.get("NODE_RUNTIME", "docker"), "config": str(rr.config_path())}
    out = subprocess.run([str(RR / "bin/qumulo-csi-driver"), "-version"], capture_output=True, text=True).stdout
    m["driver"] = {k.strip(): v.strip() for k, v in (ln.split(":", 1) for ln in out.splitlines() if ":" in ln)}
    m["arrays"] = {}
    for s in sorted(x for x in "ABCDEFGH" if cfg.get(f"ARRAY_{x}_API")):
        a, ac = rr.Array(s, cfg, timeout=10), rr.array_cfg(cfg, s)
        v, cs, nodes = a.get("/v1/version"), a.get("/v1/cluster/settings"), a.get("/v1/cluster/nodes/")
        m["arrays"][s] = {"server": ac["server"], "api": ac["api"],
                          "cluster": cs.get("cluster_name") if isinstance(cs, dict) else None,
                          "version": v.get("revision_id") if isinstance(v, dict) else None,
                          "build": v.get("build_id") if isinstance(v, dict) else None,
                          "nodes": f"{sum(1 for n in nodes if n.get('node_status') == 'online')}/{len(nodes)}"
                          if isinstance(nodes, list) else "?",
                          "shared": ac["shared_real"], "dedicated": ac["dedicated_real"]}
    (run / "meta.json").write_text(json.dumps(m, indent=1))


# ---------------------------------------------------------------------------------------------------------- analyze
def analyze(run):
    fp = run / "footprint"
    plan_ = {p["name"]: p for p in json.loads((run / "plan.json").read_text())}
    log = json.loads((fp / "scenarios.json").read_text())
    samples = jl(fp / "samples-proc.jsonl")
    res = []
    for sc in log:
        name, spec = sc["name"], plan_.get(sc["name"], {})
        # Exact names from the plan: a glob on the scenario name would also match a longer one (B-sweep-c1 / -c16)
        files = [fp / f"ops-{name}-{side}.jsonl" for side, _, _ in spec.get("runs", [])]
        files = [f for f in files if f.exists()]
        # REST calls are counted by the one controller, so a phase's REST/RPC covers every array running it at once
        phase_rpcs = Counter(r["phase"] for f in files for r in jl(f) if r.get("op"))
        runs = {}
        for f in files:
            side = f.stem.rsplit("-", 1)[1]
            ops = jl(f)
            pw = phase_windows(ops)
            model, opts = next(((m, o) for s, m, o in spec.get("runs", []) if s == side), (None, {}))
            per_op = {}
            by = defaultdict(list)
            for r in ops:
                if r.get("op"):
                    by[r["op"]].append(r)
            for op, rs in by.items():
                ms = [r["ms"] for r in rs]
                ph = rs[0]["phase"]
                a0, a1 = pw.get(ph, (None, None))
                wall = (a1 - a0) if a0 else None
                rest = None
                if a0 and op not in NODE_OPS:
                    api = counter_at(samples, "controller", a1 + 1.5) - counter_at(samples, "controller", a0 - 1.5)
                    rest = round(api / phase_rpcs[ph], 2) if phase_rpcs[ph] else None
                codes = Counter(r["code"] for r in rs)
                per_op[op] = {"n": len(rs), "codes": dict(codes), "errors": len(rs) - codes.get("OK", 0),
                              "retries": sum(r.get("retries", 0) for r in rs),
                              "p50": round(pct(ms, 50), 1), "p95": round(pct(ms, 95), 1), "p99": round(pct(ms, 99), 1),
                              "max": round(max(ms), 1), "mean": round(statistics.mean(ms), 1),
                              "wall_s": round(wall, 2) if wall else None,
                              "rps": round(len(rs) / wall, 2) if wall else None, "rest_per_rpc": rest,
                              "sample_err": sorted({r["err"][:160] for r in rs if r.get("err")})[:3]}
            runs[side] = {"model": model, "opts": opts, "ops": per_op}
        mounts = [s.get("mounts") for s in samples if sc["start"] <= s["t"] <= sc["end"] and s.get("mounts") is not None]
        res.append({"name": name, "group": spec.get("group"), "start": sc["start"], "end": sc["end"],
                    "duration_s": round(sc["end"] - sc["start"], 1), "runs": runs,
                    "controller": role_stats(samples, "controller", sc["start"], sc["end"]),
                    "node": role_stats(samples, "node", sc["start"], sc["end"]),
                    "mounts_peak": max(mounts) if mounts else None})
    lo = (run / "leftovers.txt").read_text() if (run / "leftovers.txt").exists() else ""
    (run / "results.json").write_text(json.dumps({"scenarios": res, "leftovers": lo}, indent=1))
    print(f"{len(res)} scenario(s) -> {run / 'results.json'}")


# --------------------------------------------------------------------------------------------------------- evaluate
def rows(results):
    for sc in results["scenarios"]:
        for side, r in sc["runs"].items():
            for op, o in r["ops"].items():
                yield sc, side, r, op, o


def evaluate(results, quick, clusters=None):
    base = SPEC["baseline"]["results"]
    by_name = {s["name"]: s for s in results["scenarios"]}
    checks = []
    clusters = clusters or {}

    def inflight(sc, side):
        """Calls in flight on side's cluster: every run in the scenario that reaches the same cluster, through any of
        its node addresses (BCD-addresses is 3 x 8 = 24 on one cluster; AB-concurrent-shared is 8 on each)."""
        cl = clusters.get(side, side)
        return sum(r["opts"].get("concurrency", 1) for s2, r in sc["runs"].items() if clusters.get(s2, s2) == cl)

    def scoped(t, sc, side):
        return inflight(sc, side) <= t.get("max_cluster_inflight", 10**6)

    def beyond(t, vals):
        """Rows a max_cluster_inflight scope leaves out, so the report still shows them."""
        if not vals:
            return ""
        return (f"beyond the limit's scope of {t['max_cluster_inflight']} in flight per cluster: "
                + ", ".join(f"{s} {v:,g}" for v, s in sorted(vals, reverse=True)))

    def add(t, scope, value, verdict, note=""):
        checks.append({"id": t["id"], "metric": t["metric"], "op": t.get("op", ""), "scope": scope, "value": value,
                       "limit": t["limit"], "verdict": verdict, "why": t["why"], "note": note})

    def judge(t, scope, value, ok, note=""):
        add(t, scope, value, "within" if ok else "over", note)

    for t in SPEC["thresholds"]:
        m, op = t["metric"], t.get("op", "*")
        match = lambda o: op == "*" or o == op
        if m == "error_rate":
            bad = [(f"{sc['name']}/{side} {o_}", o["codes"]) for sc, side, r, o_, o in rows(results) if o["errors"]]
            n = sum(o["n"] for *_, o in rows(results))
            judge(t, f"all {n} calls", sum(sum(v for k, v in c.items() if k != "OK") for _, c in bad), not bad,
                  "; ".join(f"{s}: {c}" for s, c in bad))
        elif m == "latency_max_ms":
            worst = max(((o["max"], f"{sc['name']}/{side} {o_}") for sc, side, r, o_, o in rows(results)), default=(0, "-"))
            judge(t, f"slowest: {worst[1]}", worst[0], worst[0] <= t["limit"])
        elif m == "latency_p95_ms":
            conc = lambda r: r["opts"].get("concurrency", 1)
            cand = [(o["p95"], f"{sc['name']}/{side} c{conc(r)}", scoped(t, sc, side)) for sc, side, r, o_, o in rows(results)
                    if match(o_) and conc(r) <= t.get("max_concurrency", 10**6) and conc(r) >= t.get("min_concurrency", 0)]
            hits = [(v, s) for v, s, ok in cand if ok]
            out = beyond(t, [(v, s) for v, s, ok in cand if not ok])
            if not hits:
                add(t, "not run", None, "-", out)
                continue
            worst = max(hits)
            over = [s for v, s in hits if v > t["limit"]]
            judge(t, f"worst of {len(hits)}: {worst[1]}", worst[0], not over, "; ".join(x for x in (", ".join(over), out) if x))
        elif m == "rps_min":
            hits = [(o["rps"], f"{sc['name']}/{side} c{r['opts'].get('concurrency')}")
                    for sc, side, r, o_, o in rows(results)
                    if match(o_) and len(sc["runs"]) == 1 and r["opts"].get("concurrency", 0) >= t["min_concurrency"]
                    and o["n"] >= 2 * r["opts"]["concurrency"] and o["rps"]]
            if not hits:
                add(t, "no single-array run at this concurrency with n >= 2x concurrency", None, "-",
                    "the --quick sizes are too small for a throughput floor" if quick else "")
                continue
            worst = min(hits)
            judge(t, f"lowest of {len(hits)}: {worst[1]}", worst[0], worst[0] >= t["limit"])
        elif m == "rest_per_rpc_max":
            cand = [(o["rest_per_rpc"], f"{sc['name']}/{side}", scoped(t, sc, side)) for sc, side, r, o_, o in rows(results)
                    if match(o_) and o["rest_per_rpc"] is not None
                    and ("model" not in t or r["model"] == t["model"])
                    and ("burst" not in t or ("burst" in sc["name"]) == t["burst"])]
            hits = [(v, s) for v, s, ok in cand if ok]
            out = beyond(t, [(v, s) for v, s, ok in cand if not ok])
            if not hits:
                add(t, "not run", None, "-", out)
                continue
            worst = max(hits)
            judge(t, f"worst of {len(hits)}: {worst[1]}", worst[0], worst[0] <= t["limit"], out)
        elif m == "rss_mb_peak":
            hits = [((sc[t["role"]] or {}).get("rss_mb_peak") or 0, sc["name"]) for sc in results["scenarios"]]
            worst = max(hits, default=(0, "-"))
            judge(t, f"{t['role']}, peak in {worst[1]}", worst[0], worst[0] <= t["limit"])
        elif m in ("p95_vs_baseline", "rps_vs_baseline"):
            key = "p95" if m == "p95_vs_baseline" else "rps"
            idx = 4 if key == "p95" else 1
            if quick:
                add(t, "baseline loads", None, "-", "--quick runs a quarter of each load, which is not comparable")
                continue
            ratios = []
            for sc_spec in SPEC["scenarios"]:
                b = sc_spec.get("baseline")
                sc = by_name.get(sc_spec["name"])
                if not b or not sc:
                    continue
                for side, ops in base.get(b, {}).items():
                    for o_, vals in ops.items():
                        o = sc["runs"].get(side, {}).get("ops", {}).get(o_)
                        if not o or not o.get(key) or not vals[idx]:
                            continue
                        ratios.append((round(o[key] / vals[idx], 2), f"{sc['name']}/{side} {o_}"))
            if not ratios:
                add(t, "no baseline load ran", None, "-")
                continue
            if key == "p95":
                worst = max(ratios)
                off = [f"{s} {v}x" for v, s in sorted(ratios, reverse=True) if v > t["limit"]]
                judge(t, f"worst of {len(ratios)}: {worst[1]}", worst[0], not off, ", ".join(off))
            else:
                worst = min(ratios)
                off = [f"{s} {v}x" for v, s in sorted(ratios) if v < t["limit"]]
                judge(t, f"worst of {len(ratios)}: {worst[1]}", worst[0], not off, ", ".join(off))
        elif m == "scaling":
            for side in "AB":
                c1 = by_name.get(f"{side}-sweep-c1", {}).get("runs", {}).get(side, {}).get("ops", {}).get(op)
                c16 = by_name.get(f"{side}-sweep-c16", {}).get("runs", {}).get(side, {}).get("ops", {}).get(op)
                if not (c1 and c16 and c1["rps"] and c16["rps"]):
                    continue
                if c16["n"] < 32:
                    add(t, f"{side}: {c16['n']} volumes at concurrency 16 is one wave", None, "-",
                        "needs n >= 2x concurrency for a rate")
                    continue
                ratio = round(c16["rps"] / c1["rps"], 2)
                judge(t, f"{side}: c16 {c16['rps']}/s vs c1 {c1['rps']}/s", ratio, ratio >= t["limit"])
        elif m == "isolation":
            alone = by_name.get("B-shared", {}).get("runs", {}).get("B", {}).get("ops", {}).get(op)
            both = by_name.get("AB-concurrent-shared", {}).get("runs", {}).get("B", {}).get("ops", {}).get(op)
            if not (alone and both):
                add(t, "needs B-shared and AB-concurrent-shared", None, "-")
                continue
            ratio = round(both["p95"] / alone["p95"], 2)
            judge(t, f"B p95 with A loaded {both['p95']} ms vs alone {alone['p95']} ms", ratio, ratio <= t["limit"])
    return checks


# ----------------------------------------------------------------------------------------------------------- render
def fmt(v, d=1):
    if v is None:
        return "–"
    if isinstance(v, float):
        return f"{v:,.{d}f}"
    return f"{v:,}" if isinstance(v, int) else str(v)


def table(head, body):
    out = ["| " + " | ".join(head) + " |", "|" + "|".join("---" for _ in head) + "|"]
    out += ["| " + " | ".join(fmt(c) for c in r) + " |" for r in body]
    return "\n".join(out)




def op_rows(results, group, with_base=False):
    base = SPEC["baseline"]["results"]
    spec = {s["name"]: s for s in SPEC["scenarios"]}
    body = []
    for sc in results["scenarios"]:
        if sc["group"] != group:
            continue
        for side in sorted(sc["runs"]):
            r = sc["runs"][side]
            for op in sorted(r["ops"], key=lambda x: OP_ORDER.index(x) if x in OP_ORDER else 99):
                o = r["ops"][op]
                codes = ", ".join(f"{k} {v}" for k, v in o["codes"].items()) + (f" (+{o['retries']} Aborted retried)" if o["retries"] else "")
                row = [sc["name"], side, r["model"], r["opts"].get("concurrency"), op, o["n"], codes,
                       o["p50"], o["p95"], o["p99"], o["max"], o["rps"], o["rest_per_rpc"]]
                if with_base:
                    b = base.get(spec.get(sc["name"], {}).get("baseline"), {}).get(side, {}).get(op)
                    row += [b[4] if b else None, f"{o['p95'] / b[4]:.2f}x" if b and b[4] else None,
                            b[1] if b else None]
                body.append(row)
    head = ["Scenario", "Array", "Model", "Conc", "RPC", "Calls", "Result", "p50 ms", "p95 ms", "p99 ms", "max ms",
            "RPC/s", "REST/RPC"]
    if with_base:
        head += ["baseline p95 ms", "p95 vs baseline", "baseline RPC/s"]
    return table(head, body) if body else "_Not run._"


def render(run):
    m = json.loads((run / "meta.json").read_text())
    results = json.loads((run / "results.json").read_text())
    checks = evaluate(results, m.get("quick"), {s: a["cluster"] for s, a in m["arrays"].items()})
    (run / "checks.json").write_text(json.dumps([{k: v for k, v in c.items() if k != "verdict"} for c in checks], indent=1))
    by_name = {s["name"]: s for s in results["scenarios"]}
    d = m.get("driver", {})
    L = []
    w = L.append
    w("# Qumulo CSI driver: performance validation report\n")
    w(f"**Build:** `{d.get('commit')}` (driver {d.get('version')}, {d.get('go')}) · **Run:** `{m['run_id']}`"
      f"{' (**--quick**: a quarter of each load)' if m.get('quick') else ''} · **Host:** {m['host']}\n")
    w(table(["Array", "Address", "Cluster", "Software", "Nodes", "Shared store", "Dedicated store"],
            [[s, f"`{a['server']}`", a["cluster"], a["version"], a["nodes"], f"`{a['shared']}`", f"`{a['dedicated']}`"]
             for s, a in m["arrays"].items()]))
    clusters = defaultdict(list)
    for s, a in m["arrays"].items():
        clusters[a["cluster"]].append(s)
    w("\n" + " ".join(f"Arrays {', '.join(v)} are node addresses of one cluster, `{k}`." for k, v in clusters.items() if len(v) > 1)
      + f" Mount tests run only on {', '.join(m['node_arrays'])} (`NODE_ARRAYS`).\n")
    w(f"> Generated by `test/performance/run.sh` from run `{m['run_id']}`. The baseline is "
      f"{SPEC['baseline']['build']}, {SPEC['baseline']['run']} ({SPEC['baseline']['source']}). Thresholds, "
      "their reasons and the baseline live in `test/performance/scenarios.yaml`. Raw samples and logs stay in the run "
      "folder and are not committed, because they carry array addresses.\n")

    w(f"{sum(o['n'] for *_, o in rows(results))} CSI calls across {len(results['scenarios'])} scenarios.\n")
    summ = (run / "summary.md")
    if summ.exists():
        w(summ.read_text().strip() + "\n")

    w("## Limits and measured values\n")
    w("Each limit from `scenarios.yaml` next to the value this run measured for it and where. Ratios against the "
      "baseline are this run divided by the baseline figure.\n")
    w(table(["ID", "Check", "RPC", "Limit", "Measured", "Where", "Basis for the limit"],
            [[c["id"], c["metric"], c["op"] or "", f"{c['limit']:,g}", "–" if c["value"] is None else f"{c['value']:,g}",
              c["scope"] + (f" ({c['note']})" if c["note"] else ""), c["why"]] for c in checks]))

    w("\n## 1. Unloaded latency (one call at a time)\n")
    w("The floor every loaded figure sits on: each call waits only for the array, not for other calls.\n")
    w(op_rows(results, "latency"))
    w("\n## 2. Against the baseline (same loads as the resource report)\n")
    w(op_rows(results, "baseline", with_base=True))
    w("\n## 3. Throughput against concurrency\n")
    body = []
    for side in "AB":
        for c in (1, 4, 16, 32):
            sc = by_name.get(f"{side}-sweep-c{c}")
            if not sc:
                continue
            ops = sc["runs"].get(side, {}).get("ops", {})
            cr, dl = ops.get("CreateVolume", {}), ops.get("DeleteVolume", {})
            body.append([side, c, cr.get("n"), cr.get("rps"), cr.get("p50"), cr.get("p95"), dl.get("rps"), dl.get("p50"),
                         dl.get("p95"), dl.get("retries"), (sc["controller"] or {}).get("goroutines_peak")])
    w(table(["Array", "Concurrency", "Volumes", "Create/s", "Create p50 ms", "Create p95 ms", "Delete/s", "Delete p50 ms",
             "Delete p95 ms", "Delete Aborted retries", "Controller goroutines peak"], body) if body else "_Not run._")
    w("\n## 4. One cluster through several node addresses at once\n")
    sc, alone = by_name.get("BCD-addresses"), by_name.get("B-shared")
    if sc:
        body = []
        for side, r in sorted(sc["runs"].items()):
            for op in ("CreateVolume", "DeleteVolume"):
                o = r["ops"].get(op, {})
                body.append([side, m["arrays"].get(side, {}).get("server"), op, o.get("n"), o.get("rps"), o.get("p50"), o.get("p95"), o.get("rest_per_rpc")])
        w(table(["Array", "Address", "RPC", "Calls", "RPC/s", "p50 ms", "p95 ms", "REST/RPC"], body))
        tot = sum(r["ops"].get("CreateVolume", {}).get("rps") or 0 for r in sc["runs"].values())
        one = (alone or {}).get("runs", {}).get("B", {}).get("ops", {}).get("CreateVolume", {}).get("rps")
        w(f"\nCombined create rate through {len(sc['runs'])} addresses: **{tot:.1f}/s**"
          + (f", against {one}/s through B alone at the same per-address load (B-shared)." if one else ".") + "\n")
    else:
        w("_Not run._\n")
    w("## 5. Two clusters from one controller at once\n")
    w(op_rows(results, "isolation", with_base=True))
    w("\n## 6. Mount density on one node\n")
    sc = by_name.get("B-mounts100")
    if sc:
        w(op_rows(results, "mounts"))
        nd = sc["node"] or {}
        pub = sc["runs"].get("B", {}).get("ops", {}).get("NodePublishVolume")
        conc = sc["runs"].get("B", {}).get("opts", {}).get("concurrency")
        if pub:
            ms = sorted(r["ms"] for r in jl(run / "footprint" / "ops-B-mounts100-B.jsonl") if r.get("op") == "NodePublishVolume")
            w(f"\nWith {conc} mounts in flight at once, mount latency spreads from **{ms[0]:,.0f} ms** (first to finish) to "
              f"**{ms[-1]:,.0f} ms** (last), against {fmt(by_name.get('B-serial', {}).get('runs', {}).get('B', {}).get('ops', {}).get('NodePublishVolume', {}).get('p50'))} ms "
              "for one mount alone (B-serial). The node server holds no lock across a mount, so the queueing is below the "
              "driver: the kernel NFS client and `mount.nfs`, with every mount to one address sharing one TCP connection "
              "per node. That is the likely cause; this run does not isolate it.")
        w(f"\nPeak live NFS mounts held by the node process: **{fmt(sc['mounts_peak'])}**. Node process peak RSS "
          f"{fmt(nd.get('rss_mb_peak'))} MB, goroutines {fmt(nd.get('goroutines_peak'))}, FDs {fmt(nd.get('fds_peak'))}.\n")
    else:
        w("_Not run._\n")
    w("## 7. Driver footprint per scenario\n")
    w(table(["Scenario", "Duration s", "Controller RSS peak MB", "Controller CPU avg %", "Controller CPU peak %",
             "Controller goroutines peak", "Node RSS peak MB", "Node goroutines peak", "Peak mounts"],
            [[s["name"], s["duration_s"], (s["controller"] or {}).get("rss_mb_peak"), (s["controller"] or {}).get("cpu_avg_pct"),
              (s["controller"] or {}).get("cpu_peak_pct"), (s["controller"] or {}).get("goroutines_peak"),
              (s["node"] or {}).get("rss_mb_peak"), (s["node"] or {}).get("goroutines_peak"), s["mounts_peak"]]
             for s in results["scenarios"]]))
    w("\n## 8. Leftovers\n")
    lo = results.get("leftovers", "").strip()
    w("```\n" + (lo or "leftover check did not run") + "\n```\n")
    w((SUITE / "scripts/limitations.md").read_text())
    w("\n## Method\n")
    w((SUITE / "scripts/method.md").read_text())
    (run / "PERFORMANCE-VALIDATION-REPORT.md").write_text("\n".join(L).rstrip() + "\n")
    print(f"report -> {run / 'PERFORMANCE-VALIDATION-REPORT.md'}")
    return 0


def main():
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    cmd, run = sys.argv[1], Path(sys.argv[2]).resolve()
    quick = "--quick" in sys.argv
    only = next((a.split("=", 1)[1].split(",") for a in sys.argv if a.startswith("--only=")), None)
    if cmd == "plan":
        plan(run, quick, only)
    elif cmd == "meta":
        meta(run, quick)
    elif cmd == "analyze":
        analyze(run)
    elif cmd == "render":
        sys.exit(render(run))
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
