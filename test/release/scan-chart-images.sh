#!/usr/bin/env bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# Scans every third-party image the chart pins -- the CSI sidecars, the
# optional ones included, and the helm test's volume image -- for HIGH and
# CRITICAL CVEs that have a fix, and fails if any image has one.
#
# Why: the CI image-scan job covered only the two images this repo builds, so
# the sidecars aged unseen until a customer's scan found 10 criticals in them
# (0.1.1, 2026-11-23). Unfixed findings are not counted (--ignore-unfixed):
# nothing a bump here could clear should turn CI red. Images listed in
# image-scan-exceptions.txt are scanned and printed but do not fail the run
# until their exception expires.
#
# The images are read from values.yaml: every `image:` pinned by digest
# there. Our own driver images are scanned by the image-scan job, which
# builds them.
#
# Usage: test/release/scan-chart-images.sh [values.yaml] [exceptions.txt]
# Needs docker; Trivy runs from a pinned image. TRIVY_IMAGE overrides it, and
# TRIVY_CACHE_DIR is mounted as Trivy's cache (default: a temp dir).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
values="${1:-$root/charts/qumulo-csi-driver/values.yaml}"
exceptions="${2:-$root/test/release/image-scan-exceptions.txt}"
trivy_image="${TRIVY_IMAGE:-aquasec/trivy:0.76.2@sha256:8f6bb947dbc528902e11f2f5be52f7b519f6a39d5d148f41166f2040499768fb}"
cache="${TRIVY_CACHE_DIR:-$(mktemp -d)}"
today="$(date -u +%F)"

mapfile -t images < <(sed -n -E 's/^[[:space:]]*image:[[:space:]]+"?([^"[:space:]]+@sha256:[a-f0-9]{64})"?[[:space:]]*$/\1/p' "$values" | sort -u)
if [ "${#images[@]}" -eq 0 ]; then
  echo "FAIL: no digest-pinned image found in $values" >&2
  exit 1
fi

# TRIVY_USERNAME / TRIVY_PASSWORD, when set, are handed to Trivy for private
# registries: the chart's csi-addons sidecar is our own rebuild on
# ghcr.io/qumulo, which is not anonymously pullable. `-e NAME` passes a
# variable only when it is set, and never puts its value on a command line.
trivy() {
  docker run --rm -e TRIVY_USERNAME -e TRIVY_PASSWORD -v "$cache:/root/.cache" "$trivy_image" "$@"
}

trivy image --download-db-only --quiet

failed=0
summary="| Image | Fixable HIGH/CRITICAL | Result |
|---|---|---|"
for img in "${images[@]}"; do
  ref="${img%@*}"
  exc="$(awk -v r="$ref" '$1 == r { print $2; exit }' "$exceptions" 2>/dev/null || true)"
  echo "=== $img"
  # Three tries: registry.k8s.io's CDN drops a TLS handshake now and then, and
  # a flake is not a finding.
  out=""
  for try in 1 2 3; do
    if out="$(trivy image --skip-db-update --quiet --scanners vuln --severity HIGH,CRITICAL \
                --ignore-unfixed --format json "$img")"; then
      break
    fi
    out=""
    [ "$try" -lt 3 ] && { echo "  scan attempt $try failed; retrying"; sleep 10; }
  done
  if [ -z "$out" ]; then
    echo "FAIL: scan of $img did not complete in 3 attempts"
    failed=1
    summary+=$'\n'"| \`$ref\` | ? | scan failed |"
    continue
  fi
  n="$(printf '%s' "$out" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(len(r.get("Vulnerabilities") or []) for r in d.get("Results") or []))')"
  printf '%s' "$out" | python3 -c '
import json, sys
for r in json.load(sys.stdin).get("Results") or []:
    for v in r.get("Vulnerabilities") or []:
        print("  %-8s %-20s %-40s %s -> %s" % (v["Severity"], v["VulnerabilityID"], v["PkgName"][:40], v.get("InstalledVersion",""), v.get("FixedVersion","")))'
  if [ "$n" -eq 0 ]; then
    result="pass"
  elif [ -n "$exc" ] && [[ "$today" < "$exc" || "$today" == "$exc" ]]; then
    result="excepted until $exc"
  elif [ -n "$exc" ]; then
    result="FAIL: exception expired $exc"
    failed=1
  else
    result="FAIL"
    failed=1
  fi
  echo "  -> $n fixable HIGH/CRITICAL: $result"
  summary+=$'\n'"| \`$ref\` | $n | $result |"
done

# An exception for an image the chart no longer pins is stale: say so, so the
# file does not collect entries nobody can match.
while read -r ref _; do
  case "$ref" in ''|'#'*) continue ;; esac
  printf '%s\n' "${images[@]}" | grep -qF -- "$ref@" || { echo "FAIL: exception for $ref, which values.yaml does not pin"; failed=1; }
done < "$exceptions"

echo
echo "$summary"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  { echo "### Chart image CVE scan (fixable HIGH/CRITICAL)"; echo; echo "$summary"; } >> "$GITHUB_STEP_SUMMARY"
fi
exit "$failed"
