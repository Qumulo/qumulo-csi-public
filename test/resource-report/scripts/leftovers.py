#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

"""List every array object a run could have left behind: directories under the configured store paths, exports,
quotas, snapshots and snapshot policies whose name or path contains the run's NAME_PREFIX. Read-only (REST GET).
usage: scripts/leftovers.py [prefix]        exit 1 when anything is found"""
import sys
from urllib.parse import quote
from rr import Array, array_cfg, config

cfg = config()
pre = sys.argv[1] if len(sys.argv) > 1 else cfg.get("NAME_PREFIX", "csi-rcr")
total = 0
for side in "AB":
    a, ac = Array(side, cfg), array_cfg(cfg, side)
    found = []
    for d in {ac["shared_real"], ac["dedicated_real"]}:
        doc = a.get(f"/v1/files/{quote(d, safe='')}/entries/?limit=1000")
        found += [f"dir {f['path']}" for f in (doc.get("files", []) if isinstance(doc, dict) else []) if pre in f["name"]]
    found += [f"export {e['id']} {e['export_path']}" for e in a.paged("/v3/nfs/exports/") if pre in e["export_path"]]
    found += [f"quota {q['id']} {q['path']}" for q in a.paged("/v1/files/quotas/status/", key="quotas") if pre in q["path"]]
    found += [f"snapshot {x['id']} {x['name']}" for x in a.paged("/v3/snapshots/?filter=all") if pre in x["name"]]
    found += [f"policy {p['id']} {p['name']}" for p in a.paged("/v1/snapshots/policies/") if pre in p["name"]]
    print(f"array {side}: {len(found)} object(s) matching {pre!r}")
    for f in found:
        print("  ", f)
    total += len(found)
sys.exit(1 if total else 0)
