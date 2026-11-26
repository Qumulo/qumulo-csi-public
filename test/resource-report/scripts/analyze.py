#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

"""Aggregate a driver_footprint.py run into summary.json (per-scenario and per-phase figures).
usage: scripts/analyze.py <footprint-dir>
Also fetches each array's analytics time series for the run window (REST GET) into analytics-<A|B>.json."""
import glob, json, re, statistics, sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from rr import Array, config


def jl(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def ts(s):
    # Go writes nanoseconds; Python 3.10's fromisoformat takes at most 6 fractional digits
    s = re.sub(r"(\.\d{6})\d+", r"\1", s.replace("Z", "+00:00"))
    return datetime.fromisoformat(s).timestamp()


def pct(vals, p):
    if not vals:
        return None
    v = sorted(vals)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


def window(samples, t0, t1):
    return [s for s in samples if t0 <= s["t"] <= t1]


def role_stats(samples, role, t0, t1):
    w = [s for s in window(samples, t0, t1) if s.get(role, {}).get("proc")]
    if len(w) < 2:
        return None
    P = lambda s: s[role]["proc"]
    M = lambda s: s[role].get("metrics") or {}
    cpu = P(w[-1])["cpu_s"] - P(w[0])["cpu_s"]
    dur = w[-1]["t"] - w[0]["t"]
    # per-second CPU% between consecutive samples
    cpu_pct = [100 * (P(b)["cpu_s"] - P(a)["cpu_s"]) / max(1e-6, b["t"] - a["t"]) for a, b in zip(w, w[1:])]
    g = [M(s).get("go_goroutines") for s in w if M(s).get("go_goroutines") is not None]
    heap = [M(s).get("go_memstats_heap_inuse_bytes") for s in w if M(s).get("go_memstats_heap_inuse_bytes")]
    api0 = (M(w[0]).get("counters") or {}).get("csi_qumulo_api_requests_total", 0)
    api1 = (M(w[-1]).get("counters") or {}).get("csi_qumulo_api_requests_total", 0)
    rpc0 = (M(w[0]).get("counters") or {}).get("csi_qumulo_rpc_total", 0)
    rpc1 = (M(w[-1]).get("counters") or {}).get("csi_qumulo_rpc_total", 0)
    return {
        "duration_s": round(dur, 1), "cpu_s": round(cpu, 3), "cpu_avg_pct": round(100 * cpu / dur, 2) if dur else 0,
        "cpu_peak_pct": round(max(cpu_pct), 1),
        "rss_mb_start": round(P(w[0])["rss_kb"] / 1024, 1), "rss_mb_end": round(P(w[-1])["rss_kb"] / 1024, 1),
        "rss_mb_peak": round(max(P(s)["rss_kb"] for s in w) / 1024, 1),
        "heap_inuse_mb_peak": round(max(heap) / 2**20, 1) if heap else None,
        "goroutines_start": g[0] if g else None, "goroutines_end": g[-1] if g else None,
        "goroutines_peak": max(g) if g else None,
        "threads_peak": max(P(s)["threads"] for s in w), "fds_peak": max(P(s)["fds"] for s in w),
        "fds_end": P(w[-1])["fds"],
        "rest_conns_peak": max(P(s)["rest8000"] for s in w), "rest_conns_end": P(w[-1])["rest8000"],
        "nfs_conns_peak": max(P(s)["nfs2049"] for s in w),
        "api_requests": api1 - api0, "rpcs": rpc1 - rpc0,
        "mounts_peak": max((s.get("mounts") or 0) for s in w),
    }


def phase_windows(ops):
    """phase -> (start, end) from csiload start/end events."""
    st, out = {}, {}
    for r in ops:
        if r.get("event") == "start":
            st[r["phase"]] = ts(r["ts"])
        elif r.get("event") == "end" and r["phase"] in st:
            out[r["phase"]] = (st[r["phase"]], ts(r["ts"]))
    return out


def counter_at(samples, role, t, name="csi_qumulo_api_requests_total"):
    best = None
    for s in samples:
        if s["t"] <= t:
            best = s
        else:
            break
    if not best:
        return 0
    return ((best.get(role, {}).get("metrics") or {}).get("counters") or {}).get(name, 0)


def prom_series(path, metric):
    out = {}
    for ln in open(path):
        if ln.startswith(metric + "{") or ln.startswith(metric + " "):
            name, _, v = ln.rpartition(" ")
            out[name] = float(v)
    return out


def main():
    run = Path(sys.argv[1])
    scen = json.load(open(run / "scenarios.json"))
    samples = jl(run / "samples-proc.jsonl")
    arr = jl(run / "samples-array.jsonl")
    summary = {"run": str(run), "scenarios": []}
    t_start, t_end = scen[0]["start"], scen[-1]["end"]

    # array analytics for the whole run window
    analytics = {}
    cfg = config()
    for side in "AB":
        a = Array(side, cfg, timeout=60)
        doc = a.get(f"/v1/analytics/time-series/?begin-time={int(t_start) - 60}&end-time={int(t_end) + 60}")
        (run / f"analytics-{side}.json").write_text(json.dumps(doc))
        analytics[side] = {s["id"]: list(zip(s["times"], s["values"])) for s in doc} if isinstance(doc, list) else {}

    for sc in scen:
        t0, t1 = sc["start"], sc["end"]
        entry = {"name": sc["name"], "start": t0, "end": t1, "duration_s": round(t1 - t0, 1)}
        entry["controller"] = role_stats(samples, "controller", t0, t1)
        entry["node"] = role_stats(samples, "node", t0, t1)
        aw = window(arr, t0, t1)
        entry["array_sampler"] = {
            side: {"rest_conns_peak": max((x[side]["conns"].get("REST", 0) for x in aw), default=None),
                   "nfs_conns_peak": max((x[side]["conns"].get("NFS", 0) for x in aw), default=None),
                   "rest_probe_ms_p50": pct([x[side]["rest_ms"] for x in aw], 50),
                   "rest_probe_ms_max": max((x[side]["rest_ms"] for x in aw), default=None),
                   "errors": sum(1 for x in aw if x[side].get("err"))} for side in "AB"}
        entry["array_analytics"] = {}
        for side in "AB":
            d = {}
            for sid, pts in analytics[side].items():
                vals = [v for t, v in pts if t0 <= t <= t1 and v is not None]
                if vals:
                    d[sid] = {"avg": round(sum(vals) / len(vals), 3), "max": round(max(vals), 3)}
            entry["array_analytics"][side] = d

        # ops
        ops_files = sorted(glob.glob(str(run / f"ops-{sc['name']}-*.jsonl")))
        entry["ops"] = {}
        # RPCs per phase across every array in this scenario: the driver's counters are process-wide, so when two
        # arrays run at once a phase's REST calls and CPU belong to both arrays' RPCs together.
        all_phase_rpcs = Counter(r["phase"] for f in ops_files for r in jl(f) if r.get("op"))
        for f in ops_files:
            side = f.rsplit("-", 1)[1].split(".")[0]
            ops = jl(f)
            pw = phase_windows(ops)
            by = defaultdict(list)
            codes = defaultdict(Counter)
            retries = Counter()
            errs = Counter()
            for r in ops:
                if r.get("op"):
                    by[r["op"]].append(r["ms"])
                    codes[r["op"]][r["code"]] += 1
                    retries[r["op"]] += r.get("retries", 0)
                    if r.get("err"):
                        errs[re.sub(r"[\w.]*-?rcr-[\w-]+", "<vol>", r["err"].splitlines()[-1] if r["err"].splitlines() else r["err"])[:200]] += 1
            phases = {}
            for ph, (a0, a1) in pw.items():
                if ph.endswith("-idle") or ph == "hold-mounted":
                    continue
                opn = [r for r in ops if r.get("phase") == ph and r.get("op")]
                role = "node" if ph in ("publish", "stats", "unpublish") else "controller"
                api = counter_at(samples, role, a1 + 1.5) - counter_at(samples, role, a0 - 1.5)
                st = role_stats(samples, role, a0 - 1, a1 + 1)
                tot = all_phase_rpcs[ph] or len(opn)
                phases[ph] = {"rpcs": len(opn), "wall_s": round(a1 - a0, 2),
                              "rpc_per_s": round(len(opn) / (a1 - a0), 2) if a1 > a0 else None,
                              "api_requests": api, "api_per_rpc": round(api / tot, 1) if tot else None,
                              "role": role, "cpu_s": st["cpu_s"] if st else None,
                              "cpu_ms_per_rpc": round(1000 * st["cpu_s"] / tot, 1) if st and tot else None,
                              "rss_mb_peak": st["rss_mb_peak"] if st else None,
                              "goroutines_peak": st["goroutines_peak"] if st else None,
                              "rest_conns_peak": st["rest_conns_peak"] if st else None}
            entry["ops"][side] = {
                "latency_ms": {op: {"n": len(v), "p50": round(pct(v, 50), 1), "p95": round(pct(v, 95), 1),
                                    "max": round(max(v), 1), "mean": round(statistics.mean(v), 1)} for op, v in by.items()},
                "codes": {op: dict(c) for op, c in codes.items()},
                "aborted_retries": {op: n for op, n in retries.items() if n}, "errors": dict(errs.most_common(5)), "phases": phases}
        summary["scenarios"].append(entry)

    # final per-endpoint REST breakdown from the last full scrape
    proms = sorted(glob.glob(str(run / "metrics-full" / "controller-*.prom")))
    proms = [p for p in proms if Path(p).stat().st_size > 0]
    if proms:
        last = proms[-1]
        summary["controller_api_breakdown"] = prom_series(last, "csi_qumulo_api_requests_total")
        summary["controller_rpc_breakdown"] = prom_series(last, "csi_qumulo_rpc_total")
    nproms = [p for p in sorted(glob.glob(str(run / "metrics-full" / "node-*.prom"))) if Path(p).stat().st_size > 0]
    if nproms:
        summary["node_rpc_breakdown"] = prom_series(nproms[-1], "csi_qumulo_rpc_total")
        summary["node_api_breakdown"] = prom_series(nproms[-1], "csi_qumulo_api_requests_total")
    (run / "summary.json").write_text(json.dumps(summary, indent=1))
    print(f"wrote {run / 'summary.json'}")


if __name__ == "__main__":
    main()
