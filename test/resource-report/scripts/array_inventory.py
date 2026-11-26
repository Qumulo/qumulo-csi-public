#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

"""Read-only inventory + load snapshot of both arrays (REST GET only). Writes <outdir>/array-<A|B>.json.
usage: scripts/array_inventory.py <outdir> [--window-min N]     (analytics window, default 60)"""
import json, sys, time
from collections import Counter
from pathlib import Path
from rr import Array, config

CSI_HINTS = ("csi", "pvc-")


def is_csi(text):
    t = (text or "").lower()
    return any(h in t for h in CSI_HINTS)


def series_summary(ts):
    out = {}
    for s in ts if isinstance(ts, list) else []:
        vals = [v for v in s.get("values", []) if v is not None]
        if vals:
            out[s["id"]] = {"samples": len(vals), "avg": sum(vals) / len(vals), "max": max(vals),
                            "nonzero_samples": sum(1 for v in vals if v)}
    return out


def walk_aggregates(a, path, depth, out):
    """Directory capacity/inode aggregates for path and its children, to `depth` levels."""
    from urllib.parse import quote
    doc = a.get(f"/v1/files/{quote(path, safe='')}/aggregates/?max-entries=1000")
    if not isinstance(doc, dict) or "_error" in doc:
        return
    for f in doc.get("files", []):
        if f["type"] != "FS_FILE_TYPE_DIRECTORY":
            continue
        p = path.rstrip("/") + "/" + f["name"]
        out.append({"path": p, "capacity_bytes": int(f["capacity_usage"]), "data_bytes": int(f["data_usage"]),
                    "files": int(f["num_files"]), "dirs": int(f["num_directories"])})
        if depth > 1:
            walk_aggregates(a, p, depth - 1, out)


def collect(side, window_min, cfg):
    a = Array(side, cfg)
    now = int(time.time())
    inv = {"side": side, "api": a.api, "collected_at": now}
    inv["version"] = a.get("/v1/version")
    inv["cluster"] = a.get("/v1/cluster/settings")
    inv["nodes"] = a.get("/v1/cluster/nodes/")
    inv["protection"] = a.get("/v1/cluster/protection/status")
    inv["restriper"] = a.get("/v1/cluster/restriper/status")
    inv["file_system"] = a.get("/v1/file-system")
    inv["capacity_history_daily"] = a.get(f"/v1/analytics/capacity-history/?begin-time={now - 30 * 86400}&interval=daily")
    inv["capacity_history_hourly"] = a.get(f"/v1/analytics/capacity-history/?begin-time={now - 2 * 86400}&interval=hourly")

    net = a.get("/v2/network/interfaces/1/status/")
    inv["network"] = [{"node": n["node_name"], "speed_mbps": n["interface_details"].get("speed"),
                       "bytes_sent": int(n["interface_details"].get("bytes_sent", 0)),
                       "bytes_received": int(n["interface_details"].get("bytes_received", 0)),
                       "addresses": [{"address": s.get("address"), "floating": s.get("floating_addresses", [])}
                                     for s in n.get("network_statuses", [])]}
                      for n in net] if isinstance(net, list) else net

    exports = a.paged("/v3/nfs/exports/")
    inv["exports"] = exports
    inv["exports_csi"] = [e for e in exports if is_csi(e.get("description")) or is_csi(e.get("export_path"))] \
        if isinstance(exports, list) else []

    quotas = a.paged("/v1/files/quotas/status/", key="quotas")
    inv["quotas"] = quotas

    snaps = a.paged("/v3/snapshots/?filter=all")
    inv["snapshots"] = snaps
    inv["snapshot_name_kinds"] = Counter(
        ("csi" if is_csi(s["name"]) else "replication" if "replication" in s["name"] else
         "policy" if s.get("policy_id") else "other") for s in snaps) if isinstance(snaps, list) else {}
    inv["snapshot_policies"] = a.paged("/v1/snapshots/policies/")
    inv["snapshot_capacity"] = a.get("/v1/snapshots/total-used-capacity")

    inv["replication_source"] = a.get("/v2/replication/source-relationships/status/")
    inv["replication_target"] = a.get("/v2/replication/target-relationships/status/")
    inv["tree_delete_jobs"] = a.get("/v1/tree-delete/jobs/")
    inv["tenants"] = a.get("/v1/multitenancy/tenants/")

    dirs = []
    walk_aggregates(a, "/", 3, dirs)
    inv["dir_aggregates"] = dirs

    conns = a.get("/v2/network/connections/")
    per_node, by_type, by_addr = {}, Counter(), Counter()
    for node in conns if isinstance(conns, list) else []:
        per_node[node["id"]] = len(node["connections"])
        for c in node["connections"]:
            by_type[c["type"]] += 1
            by_addr[c["network_address"]] += 1
    inv["connections"] = {"per_node": per_node, "by_type": dict(by_type), "by_address": dict(by_addr.most_common(20))}

    ts = a.get(f"/v1/analytics/time-series/?begin-time={now - window_min * 60}")
    inv["analytics_window_min"] = window_min
    inv["analytics"] = series_summary(ts)
    inv["activity_current"] = a.get("/v1/analytics/activity/current")
    return inv


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    out = Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
    win = int(sys.argv[sys.argv.index("--window-min") + 1]) if "--window-min" in sys.argv else 60
    cfg = config()
    for side in ("A", "B"):
        t0 = time.time()
        inv = collect(side, win, cfg)
        (out / f"array-{side}.json").write_text(json.dumps(inv, indent=1, default=str))
        errs = [k for k, v in inv.items() if isinstance(v, dict) and "_error" in v]
        print(f"array {side}: {len(json.dumps(inv))} bytes in {time.time() - t0:.1f}s; endpoint errors: {errs or 'none'}")
