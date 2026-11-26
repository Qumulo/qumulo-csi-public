# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# Shared settings and helpers for the OpenShift live checks. Sourced.
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
[ -f "$HERE/live.env" ] || { echo "missing $HERE/live.env (copy live.env.example)"; exit 2; }
set -a; source "$HERE/live.env"; set +a
[ "$(oc config current-context)" = "$KUBE_CONTEXT" ] || { echo "context is not $KUBE_CONTEXT"; exit 2; }
RUN=$(eval echo "$RUN_DIR"); CHART=$(eval echo "$CHART"); OLD_CHART=$(eval echo "$OLD_CHART"); BASE_CHART=$(eval echo "${BASE_CHART:-}")
REG=$REGISTRY
SCCAPI=security.openshift.io/v1/SecurityContextConstraints
ts(){ date -u +%H:%M:%S; }
say(){ echo "[$(ts)] $*"; }
pass(){ echo "[$(ts)] PASS $*"; echo "PASS $*" >> "$OUT/verdicts.txt"; }
fail(){ echo "[$(ts)] FAIL $*"; echo "FAIL $*" >> "$OUT/verdicts.txt"; }
check(){ local name=$1; shift; if "$@"; then pass "$name"; else fail "$name"; fi; }
imgsets(){ echo "--set image.controller.repository=$REG/qumulo-csi-driver-controller --set image.controller.tag=$TAG --set image.controller.pullPolicy=IfNotPresent --set image.node.repository=$REG/qumulo-csi-driver-node --set image.node.tag=$TAG --set image.node.pullPolicy=IfNotPresent"; }
wait_up(){ local ns=$1 rel=$2 t=${3:-300}; local fn=$rel-qumulo-csi-driver
  oc -n $ns rollout status ds/$fn-node --timeout=${t}s && oc -n $ns rollout status deploy/$fn-controller --timeout=${t}s; }
scc_of(){ oc -n $1 get pods -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.annotations.openshift\.io/scc}{"\n"}{end}'; }
# Listening port in /proc/net/tcp form (hex, upper case).
hexport(){ printf '%04X' "$1"; }
