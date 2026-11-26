#!/usr/bin/env bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# Resource Consumption Report suite: measures the driver's own CPU, memory, goroutines, FDs and connections, and what
# it costs the arrays, across a scripted volume lifecycle against two Qumulo clusters. See README.md.
#
#   ./run.sh check                                  preflight: tools, config, token files, both arrays, binaries
#   ./run.sh build                                  build bin/qumulo-csi-driver and bin/csiload from this checkout
#   ./run.sh report [--quick] [--only=<scenario>]... [--findings <file>] [--publish]
#                                                   full run (sudo); --publish copies the report to docs/
#   ./run.sh render <run-dir> [--findings <file>] [--publish]
#                                                   re-analyse and re-render an existing run; creates nothing on arrays
#
# Configuration: resource-report.env in this folder (copy resource-report.env.example), or RR_CONFIG=<path>.
set -u
SUITE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$SUITE/../.." && pwd)"
export RR_CONFIG="${RR_CONFIG:-$SUITE/resource-report.env}"
export PYTHONDONTWRITEBYTECODE=1        # scripts also run under sudo; never leave root-owned __pycache__ behind
PUBLISHED="$REPO/docs/RESOURCE-CONSUMPTION-REPORT.md"

ok()   { printf '  \033[32mOK\033[0m    %s\n' "$*"; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$*"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$*"; FAILS=$((FAILS+1)); }
FAILS=0

# value from the config file (inline comments stripped, <placeholders> empty), overridden by the environment
cfg() {
  local v
  v="$(sed -n "s/^$1=//p" "$RR_CONFIG" 2>/dev/null | tail -1 | sed -E 's/[[:space:]]+#.*$//; s/^"//; s/"$//')"
  case "$v" in *"<"*">"*) v="" ;; esac
  printf '%s' "${!1:-$v}"
}

cmd_check() {
  echo "Config: $RR_CONFIG"
  [ -f "$RR_CONFIG" ] || { bad "missing: cp $SUITE/resource-report.env.example $RR_CONFIG and fill it in"; return 1; }
  echo "Tools"
  for t in go python3 curl sudo; do command -v "$t" >/dev/null && ok "$t" || bad "$t missing"; done
  python3 -c 'import yaml' 2>/dev/null && ok "PyYAML" || bad "PyYAML missing (pip install pyyaml)"
  if [ "$(cfg NODE_RUNTIME)" = host ]; then
    command -v mount.nfs >/dev/null || [ -x /sbin/mount.nfs ] && ok "mount.nfs (NODE_RUNTIME=host)" || bad "mount.nfs missing: install nfs-common or set NODE_RUNTIME=docker"
  else
    command -v docker >/dev/null && ok "docker" || bad "docker missing (or set NODE_RUNTIME=host)"
    img="$(cfg NODE_IMAGE)"; img="${img:-qumulo-csi-driver-node:0.1.0}"
    sudo -n docker image inspect "$img" >/dev/null 2>&1 || docker image inspect "$img" >/dev/null 2>&1 \
      && ok "node image $img" || bad "node image $img missing: (cd $REPO && make docker-build-node)"
  fi
  echo "Binaries"
  for b in qumulo-csi-driver csiload; do [ -x "$SUITE/bin/$b" ] && ok "bin/$b" || bad "bin/$b missing: ./run.sh build"; done
  [ -x "$SUITE/bin/qumulo-csi-driver" ] && ok "$("$SUITE/bin/qumulo-csi-driver" -version | tr '\n' ' ')"
  echo "Arrays"
  python3 - <<'EOF' || FAILS=$((FAILS+1))
import sys
sys.path.insert(0, __import__("os").environ["RR_SUITE_SCRIPTS"])
from rr import Array, array_cfg, config
cfg, fails = config(), 0
for side in "AB":
    try:
        ac = array_cfg(cfg, side)
        a = Array(side, cfg, timeout=8)
    except (SystemExit, OSError, KeyError, ValueError) as e:
        print(f"  \033[31mFAIL\033[0m  array {side}: {e}"); fails += 1; continue
    v, who = a.get("/v1/version"), a.get("/v1/session/who-am-i")
    if isinstance(v, dict) and "revision_id" in v:
        print(f"  \033[32mOK\033[0m    array {side} {a.api}: {v['revision_id']}")
    else:
        print(f"  \033[31mFAIL\033[0m  array {side} {a.api} unreachable: {v}"); fails += 1; continue
    if isinstance(who, dict) and "_error" not in who:
        print(f"  \033[32mOK\033[0m    array {side} token accepted")
    else:
        print(f"  \033[31mFAIL\033[0m  array {side} token rejected ({who.get('_error')})"); fails += 1
    for k in ("shared_real", "dedicated_real"):
        from urllib.parse import quote
        d = a.get(f"/v1/files/{quote(ac[k], safe='')}/info/attributes")
        if isinstance(d, dict) and d.get("type") == "FS_FILE_TYPE_DIRECTORY":
            print(f"  \033[32mOK\033[0m    array {side} store path {ac[k]} exists")
        else:
            print(f"  \033[31mFAIL\033[0m  array {side} store path {ac[k]} missing (CreateVolume needs it)"); fails += 1
sys.exit(1 if fails else 0)
EOF
  echo; [ "$FAILS" = 0 ] && echo "check: all OK" || echo "check: $FAILS failing item(s)"
  [ "$FAILS" = 0 ]
}
export RR_SUITE_SCRIPTS="$SUITE/scripts"

cmd_build() {
  local c dirty M LD
  c="$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo unknown)"; dirty=""
  [ -n "$(git -C "$REPO" status --porcelain --untracked-files=no 2>/dev/null)" ] && dirty="-dirty"
  echo "building from $REPO at $c$dirty"
  M="$(cd "$REPO" && go list -m)"
  LD="-s -w -X $M/pkg/version.DriverVersion=$(cat "$REPO/VERSION") -X $M/pkg/version.GitCommit=$c$dirty -X $M/pkg/version.BuildDate=$(date -u +%FT%TZ)"
  mkdir -p "$SUITE/bin"
  (cd "$REPO" && CGO_ENABLED=0 go build -trimpath -ldflags "$LD" -o "$SUITE/bin/qumulo-csi-driver" ./cmd/qumulo-csi-driver \
              && CGO_ENABLED=0 go build -trimpath -o "$SUITE/bin/csiload" ./test/resource-report/csiload) || exit 1
  "$SUITE/bin/qumulo-csi-driver" -version
}

# meta.json: build, times, host, chart resource requests, method text
write_meta() {
  python3 - "$1" "$SUITE" "$REPO" "$(cfg NAME_PREFIX)" <<'EOF'
import json, socket, subprocess, sys, time
from pathlib import Path
R, suite, repo, prefix = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4] or "csi-rcr"
p = R / "meta.json"
meta = json.loads(p.read_text()) if p.exists() else {}
if "driver_commit" not in meta:
    out = subprocess.run([str(suite / "bin/qumulo-csi-driver"), "-version"], capture_output=True, text=True).stdout
    v = {k.strip(): x.strip() for k, x in (l.split(":", 1) for l in out.splitlines() if ":" in l)}
    req = {}
    try:
        import yaml
        vals = yaml.safe_load(open(repo / "charts/qumulo-csi-driver/values.yaml"))
        rc = vals["controller"].get("resources") or {}
        req["controller"] = json.dumps(rc.get("driver", rc).get("requests")) if rc else "none (resources: {})"
        req["node"] = json.dumps(vals["node"]["resources"]["driver"]["requests"])
    except Exception as e:
        req = {"controller": f"unread ({e})", "node": "unread"}
    meta.update({"run_id": R.name, "driver_commit": v.get("commit"), "driver_version": v.get("version"), "go": v.get("go"),
                 "started": time.strftime("%Y-%m-%d %H:%M %Z"), "host": socket.gethostname(), "name_prefix": prefix,
                 "chart_requests": req})
meta["method"] = (suite / "scripts/method.md").read_text()
if len(sys.argv) > 5 or (R / "footprint/scenarios.json").exists():
    sc = json.loads((R / "footprint/scenarios.json").read_text())
    meta["finished"] = time.strftime("%Y-%m-%d %H:%M %Z", time.localtime(sc[-1]["end"]))
    meta.setdefault("started", time.strftime("%Y-%m-%d %H:%M %Z", time.localtime(sc[0]["start"])))
p.write_text(json.dumps(meta, indent=1))
EOF
}

finish() {   # analyse + render + optional publish for run dir $1
  local R="$1" findings="$2" publish="$3"
  [ -n "$findings" ] && { cp "$findings" "$R/findings.md" && echo "findings: $findings -> $R/findings.md"; }
  echo "== analyse";  python3 "$SUITE/scripts/analyze.py" "$R/footprint" || exit 1
  write_meta "$R"
  echo "== render";   python3 "$SUITE/scripts/render.py" "$R" || exit 1
  if [ -n "$publish" ]; then
    [ -f "$R/findings.md" ] || warn "no findings.md in the run: the published report will have tables but no written summary"
    cp "$R/RESOURCE-CONSUMPTION-REPORT.md" "$PUBLISHED" && echo "published: $PUBLISHED"
  fi
}

cmd_report() {
  local quick="" publish="" findings="" only=() R
  while [ $# -gt 0 ]; do case "$1" in
    --quick) quick=--quick ;; --publish) publish=1 ;; --only=*) only+=("$1") ;;
    --findings) shift; findings="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")" ;;
    *) echo "unknown option $1"; exit 2 ;; esac; shift; done
  cmd_check >/dev/null || { cmd_check; echo "fix the failing items first"; exit 2; }
  sudo -n true 2>/dev/null || sudo -v || exit 2   # passwordless sudo (CI) never needs -v, which can demand a password anyway
  local out; out="$(cfg OUT_DIR)"; out="${out:-runs}"; case "$out" in /*) ;; *) out="$SUITE/$out" ;; esac
  R="$out/$(date +%F-%H%M)-$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  mkdir -p "$R/footprint"; echo "run: $R"
  write_meta "$R"
  echo "== 1/4 baseline array inventory (GET only)"; python3 "$SUITE/scripts/array_inventory.py" "$R/baseline" || exit 1
  echo "== 2/4 driver footprint (sudo; ~35 min full, ~10 min --quick)"
  sudo env RR_CONFIG="$RR_CONFIG" PYTHONDONTWRITEBYTECODE=1 python3 "$SUITE/scripts/driver_footprint.py" "$R/footprint" $quick "${only[@]}" \
    || warn "footprint run exited non-zero; continuing to the inventory and leftover check"
  echo "== 3/4 after array inventory (GET only)";  python3 "$SUITE/scripts/array_inventory.py" "$R/after"
  echo "== 4/4 leftover check (GET only)";         python3 "$SUITE/scripts/leftovers.py" > "$R/leftovers.txt"; cat "$R/leftovers.txt"
  finish "$R" "$findings" "$publish"
}

cmd_render() {
  local R="" publish="" findings=""
  while [ $# -gt 0 ]; do case "$1" in
    --publish) publish=1 ;; --findings) shift; findings="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")" ;;
    *) R="$(cd "$1" && pwd)" ;; esac; shift; done
  [ -n "$R" ] && [ -f "$R/footprint/scenarios.json" ] || { echo "usage: ./run.sh render <run-dir> [--findings f] [--publish]"; exit 2; }
  finish "$R" "$findings" "$publish"
}

case "${1:-help}" in
  check)  cmd_check ;;
  build)  cmd_build ;;
  report) shift; cmd_report "$@" ;;
  render) shift; cmd_render "$@" ;;
  *)      sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//' ;;
esac
