#!/usr/bin/env bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# Performance validation for the Qumulo CSI driver: latency, throughput, scaling, multi-address, multi-cluster and
# mount density of the driver binary built from this checkout, against the lab arrays, judged against the thresholds
# and the baseline in scenarios.yaml. Writes PERFORMANCE-VALIDATION-REPORT.md. Run by hand. See README.md.
#
#   ./run.sh check                               preflight: tools, config, every array, tokens, store paths, node image
#   ./run.sh build                               build the driver and csiload from this checkout
#   ./run.sh run [--quick] [--only=a,b] [--publish]
#                                                build, run every scenario (~25 min; --quick ~8 min), analyse, render.
#                                                Results: runs/<date>-<commit>/
#   ./run.sh render <run-dir> [--publish]        re-analyse and re-render an existing run
#   ./run.sh list [--quick]                      what a run would do
#
# Configuration: PERF_CONFIG=<path>, else performance.env in this folder (copy performance.env.example). Objects are
# named csi-perf-* (PERF_NAME_PREFIX), apart from the resource report's csi-rcr-*.
set -u
SUITE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$SUITE/../.." && pwd)"
RR="$REPO/test/resource-report"
PUBLISHED="$REPO/docs/PERFORMANCE-VALIDATION-REPORT.md"
PERF_CONFIG="${PERF_CONFIG:-$SUITE/performance.env}"
export RR_CONFIG="$PERF_CONFIG" PYTHONDONTWRITEBYTECODE=1
export NAME_PREFIX="${PERF_NAME_PREFIX:-csi-perf}"
export CONTROLLER_METRICS_PORT="${PERF_CONTROLLER_METRICS_PORT:-29674}" NODE_METRICS_PORT="${PERF_NODE_METRICS_PORT:-29675}"

cmd_check() {
  echo "Config: $RR_CONFIG (prefix $NAME_PREFIX)"
  [ -f "$RR_CONFIG" ] || { echo "  FAIL  missing: cp performance.env.example performance.env, or set PERF_CONFIG"; return 1; }
  python3 -c 'import yaml' 2>/dev/null || { echo "  FAIL  PyYAML missing (pip install pyyaml)"; return 1; }
  "$RR/run.sh" check || return 1
  echo "Every configured array"
  python3 - <<'EOF'
import os, re, sys
sys.path.insert(0, os.path.join(os.environ["RR_SUITE"], "scripts"))
import rr
cfg = rr.config()
bad = 0
for s in sorted({m.group(1) for k in cfg if (m := re.fullmatch(r"ARRAY_([A-Z])_API", k))}):
    a = rr.Array(s, cfg, timeout=8)
    v, cs = a.get("/v1/version"), a.get("/v1/cluster/settings")
    ok = isinstance(v, dict) and "revision_id" in v
    bad += not ok
    print(f"  {'OK  ' if ok else 'FAIL'}  array {s} {a.api}: {v.get('revision_id', v) if isinstance(v, dict) else v}, "
          f"cluster {cs.get('cluster_name') if isinstance(cs, dict) else '?'}")
print(f"  INFO  NODE_ARRAYS={cfg.get('NODE_ARRAYS', 'B')}: mount scenarios run only there")
sys.exit(1 if bad else 0)
EOF
}

cmd_render() {   # analyse + render (+ publish) run dir $1
  local R="$1" publish="${2:-}" code
  python3 "$SUITE/scripts/perf.py" analyze "$R" || return 2
  python3 "$SUITE/scripts/perf.py" render "$R"; code=$?
  [ -n "$publish" ] && cp "$R/PERFORMANCE-VALIDATION-REPORT.md" "$PUBLISHED" && echo "published: $PUBLISHED"
  return $code
}

cmd_run() {
  local quick="" only="" publish="" R idle=10s
  while [ $# -gt 0 ]; do case "$1" in
    --quick) quick=--quick; idle=5s ;; --only=*) only="$1" ;; --publish) publish=1 ;;
    *) echo "unknown option $1"; exit 2 ;; esac; shift; done
  cmd_check >/dev/null || { cmd_check; echo "fix the failing items first"; exit 2; }
  "$RR/run.sh" build || exit 2
  sudo -n true 2>/dev/null || sudo -v || exit 2
  local out="$SUITE/runs" c; c="$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  [ -n "$(git -C "$REPO" status --porcelain --untracked-files=no 2>/dev/null)" ] && c="$c-dirty"
  R="$out/$(date +%F-%H%M)-$c${quick:+-quick}"; mkdir -p "$R/footprint"; echo "run: $R"
  python3 "$SUITE/scripts/perf.py" plan "$R" $quick $only || exit 2
  python3 "$SUITE/scripts/perf.py" meta "$R" $quick || exit 2
  echo "== load (sudo: the node process mounts NFS)"
  sudo env RR_CONFIG="$RR_CONFIG" NAME_PREFIX="$NAME_PREFIX" CONTROLLER_METRICS_PORT="$CONTROLLER_METRICS_PORT" \
    NODE_METRICS_PORT="$NODE_METRICS_PORT" PYTHONDONTWRITEBYTECODE=1 \
    python3 "$RR/scripts/driver_footprint.py" "$R/footprint" --scenarios="$R/plan.json" --idle="$idle" \
    || echo "WARN  load run exited non-zero; analysing what it recorded"
  echo "== leftover check (GET only)"
  (cd "$RR/scripts" && python3 leftovers.py "$NAME_PREFIX") > "$R/leftovers.txt"; cat "$R/leftovers.txt"
  echo "== analyse and render"
  cmd_render "$R" "$publish"
}

case "${1:-help}" in
  check)  export RR_SUITE="$RR"; cmd_check ;;
  build)  "$RR/run.sh" build ;;
  run)    shift; export RR_SUITE="$RR"; cmd_run "$@" ;;
  render) shift; [ -d "${1:-}" ] || { echo "usage: ./run.sh render <run-dir> [--publish]"; exit 2; }
          R="$(cd "$1" && pwd)"; cmd_render "$R" "$([ "${2:-}" = --publish ] && echo 1)" ;;
  list)   shift; T="$(mktemp -d)"; python3 "$SUITE/scripts/perf.py" plan "$T" "$@" >/dev/null &&
          python3 -c "import json,sys; [print(f\"{s['group']:<10} {s['name']:<22} \" + '  +  '.join(f'{a}:{m} ' + ' '.join(f'{k}={v}' for k,v in o.items()) for a,m,o in s['runs'])) for s in json.load(open(sys.argv[1]))]" "$T/plan.json"; rm -rf "$T" ;;
  *)      sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//' ;;
esac
