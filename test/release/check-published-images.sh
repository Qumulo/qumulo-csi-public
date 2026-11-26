#!/usr/bin/env bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# Runs the PUBLISHED driver images with the exact args a chart passes them.
#
# TestChartDriverArgsAreAcceptedByTheDriver checks the chart against the
# driver built from the same commit. It cannot see the other way the two meet:
# a chart from one commit pulling an image published from another. That is
# how a broken release once shipped -- the release branch's chart passed the node
# -registration-marker, while the 0.1.0 on ghcr.io was an older build, which exits
# with "flag provided but not defined" -- so every install from the branch
# crash-looped. This pulls what the chart would pull and asks it.
#
# Usage: check-published-images.sh <chart dir or .tgz> [helm --set ...]
# Needs helm, docker, python3 with PyYAML, and a login to the registry.
set -euo pipefail

chart=${1:?usage: $0 <chart dir or .tgz> [--set k=v ...]}
shift

# Every option that adds a driver flag, so a flag gated on a value is asked
# about too. The class is only there because the chart refuses to render
# without one.
rendered=$(helm template qumulo-csi "$chart" --kube-version 1.36.0 \
  --set storageClasses[0].name=check \
  --set storageClasses[0].server=qumulo.example.com \
  --set storageClasses[0].secretName=check \
  --set storageClasses[0].basePath=/csi \
  --set metrics.enabled=true \
  --set sidecars.capacity.enabled=true \
  --set controller.inventory.enabled=true \
  --set sidecars.csiAddons.enabled=true \
  "$@")

# One line per driver container: <workload>\t<image>\t<args, NUL-free, \x1f-separated>.
lines=$(python3 -I -c '
import sys, yaml
for doc in yaml.safe_load_all(sys.stdin):
    if not doc or doc.get("kind") not in ("Deployment", "DaemonSet"):
        continue
    for c in doc["spec"]["template"]["spec"]["containers"]:
        if c["name"] == "qumulo-csi-driver":
            print(doc["kind"], c["image"], "\x1f".join(c.get("args", [])), sep="\t")
' <<<"$rendered")
[ -n "$lines" ] || { echo "the chart rendered no driver container"; exit 1; }

fail=0
while IFS=$'\t' read -r kind image argstr; do
  IFS=$'\x1f' read -r -a args <<<"$argstr"
  # $(NODE_ID) is expanded by the kubelet, not here; any value parses.
  args=("${args[@]//\$(NODE_ID)/check-node}")
  echo "== $kind: $image"
  docker pull -q "$image" >/dev/null
  # -version returns straight after parsing, so this exercises the flag
  # handling and nothing else.
  if out=$(docker run --rm "$image" "${args[@]}" -version 2>&1); then
    echo "$out" | sed 's/^/   /'
  else
    echo "   FAIL: the published image refuses the args this chart passes it:"
    echo "$out" | head -3 | sed 's/^/   /'
    fail=1
  fi
done <<<"$lines"

[ "$fail" -eq 0 ] && echo "check-published-images: every published image accepts its chart's args"
exit "$fail"
