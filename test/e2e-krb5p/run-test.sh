#!/usr/bin/env bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License
#
# run-test.sh -- end-to-end validation of sec=krb5p against a live Qumulo cluster.
#
# Installs the chart with a no-fallback cluster mapping, provisions a krb5p
# volume, mounts it, and asserts on three things that are easy to get silently
# wrong:
#
#   1. the mount really carries sec=krb5p (not sec=sys after a quiet downgrade),
#   2. the *export* on the cluster really requires KRB5P, and
#   3. the pod can actually write through it.
#
# Assertion 2 is the one that matters most. A mount option alone proves nothing:
# NFSv4.1 negotiates the weakest flavour both ends accept, so a client asking
# for krb5p against a permissive export lands on sec=sys and looks fine.
#
# Run stage-host.sh first.
#
# Usage:
#   QUMULO_SERVER=10.0.0.5 QUMULO_TOKEN_FILE=~/token.txt \
#   BASE_PATH=/k8s/krb5p ./run-test.sh
#
#   KEEP=1 ./run-test.sh     # leave everything up for inspection
set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

# --- configuration ----------------------------------------------------------
readonly PROFILE="${MINIKUBE_PROFILE:-qumulo-krb5p}"
readonly NAMESPACE="${NAMESPACE:-qumulo-csi}"
readonly RELEASE="${RELEASE:-qumulo-csi}"
readonly CHART_DIR="${CHART_DIR:-${REPO_ROOT}/charts/qumulo-csi-driver}"

readonly QUMULO_SERVER="${QUMULO_SERVER:-}"
readonly QUMULO_API_URL="${QUMULO_API_URL:-}"        # optional; defaults to https://<server>:8000
readonly QUMULO_TOKEN_FILE="${QUMULO_TOKEN_FILE:-}"
readonly SECRET_NAME="${SECRET_NAME:-qumulo-api-token}"

# A path with NO ancestor export. NFSv4 forbids an export nested inside another,
# so dedicated-export -- which securityFlavor requires -- fails beneath one.
readonly BASE_PATH="${BASE_PATH:-}"
readonly EXPORT_BASE_PATH="${EXPORT_BASE_PATH:-${BASE_PATH}}"

readonly CA_BUNDLE_FILE="${CA_BUNDLE_FILE:-}"        # optional; else TLS verification is skipped
readonly IMAGE_TAG="${IMAGE_TAG:-0.1.0}"
readonly TIMEOUT="${TIMEOUT:-300}"

readonly WORK_DIR="$(mktemp -d)"
KEEP="${KEEP:-0}"
FAILED=0

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m  PASS\033[0m %s\n' "$*"; }
bad()  { printf '\033[1;31m  FAIL\033[0m %s\n' "$*" >&2; FAILED=1; }
warn() { printf '\033[1;33m  !!  \033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

kc() { kubectl --context "$PROFILE" -n "$NAMESPACE" "$@"; }

# --- teardown ---------------------------------------------------------------
# A single trap, so an abort halfway leaves no volume on the Qumulo cluster.
# Order matters: the pod releases the mount, the PVC triggers DeleteVolume (and
# with it the export and the directory), and only then does the driver go --
# deleting the driver first would strand PVCs with no controller to finalize
# them, and they would hang in Terminating.
cleanup() {
  local rc=$?
  if [[ "$KEEP" == "1" ]]; then
    log "KEEP=1: leaving the cluster as it is"
    printf '  namespace %s, release %s, workdir %s\n' "$NAMESPACE" "$RELEASE" "$WORK_DIR"
    return $rc
  fi

  log "Teardown"
  kc delete pod qumulo-secure-app --ignore-not-found --wait=true --timeout=90s >/dev/null 2>&1 || true
  ok "pod removed"

  # --wait so DeleteVolume completes before the driver is uninstalled.
  kc delete pvc qumulo-krb5p-pvc --ignore-not-found --wait=true --timeout=180s >/dev/null 2>&1 \
    || warn "PVC did not delete cleanly; check for a leftover directory on the cluster"
  ok "PVC removed"

  kubectl --context "$PROFILE" delete storageclass qumulo-krb5p-test-sc \
    --ignore-not-found >/dev/null 2>&1 || true
  ok "StorageClass removed"

  helm --kube-context "$PROFILE" -n "$NAMESPACE" uninstall "$RELEASE" \
    --wait --timeout 3m >/dev/null 2>&1 || true
  ok "Helm release removed"

  kc delete secret "$SECRET_NAME" --ignore-not-found >/dev/null 2>&1 || true
  rm -rf "$WORK_DIR"
  ok "workspace removed"

  if [[ $rc -ne 0 || $FAILED -ne 0 ]]; then
    printf '\n\033[1;31mRESULT: FAILED\033[0m\n' >&2
    return 1
  fi
  printf '\n\033[1;32mRESULT: PASSED\033[0m\n'
  return 0
}
trap cleanup EXIT

# --- preflight --------------------------------------------------------------
log "Preflight"
for tool in kubectl helm minikube; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool is required but not on PATH"
done
[[ -n "$QUMULO_SERVER"     ]] || die "set QUMULO_SERVER to the Qumulo address pods will mount from"
[[ -n "$QUMULO_TOKEN_FILE" ]] || die "set QUMULO_TOKEN_FILE to a file holding a Qumulo REST API token"
[[ -f "$QUMULO_TOKEN_FILE" ]] || die "token file not found: $QUMULO_TOKEN_FILE"
[[ -s "$QUMULO_TOKEN_FILE" ]] || die "token file is empty: $QUMULO_TOKEN_FILE"
[[ -n "$BASE_PATH"         ]] || die "set BASE_PATH to a cluster directory with NO ancestor NFS export"
[[ -d "$CHART_DIR"         ]] || die "chart not found: $CHART_DIR"

minikube -p "$PROFILE" status >/dev/null 2>&1 \
  || die "minikube profile '$PROFILE' is not running -- run stage-host.sh first"

# The whole test is meaningless if the node cannot do Kerberos.
if ! minikube -p "$PROFILE" ssh -- 'pgrep -x rpc.gssd >/dev/null 2>&1 || systemctl is-active --quiet rpc-gssd' >/dev/null 2>&1; then
  die "rpc.gssd is not running on the node -- run stage-host.sh first.
  Without it a sec=krb5p mount cannot complete its GSS handshake."
fi
ok "toolchain, node and rpc.gssd"

# --- install ----------------------------------------------------------------
log "Installing the driver"
kubectl --context "$PROFILE" create namespace "$NAMESPACE" \
  --dry-run=client -o yaml | kubectl --context "$PROFILE" apply -f - >/dev/null

# The credential the sidecars pass per RPC. The chart manages no Secret of its
# own -- there is no cluster-wide token in this driver.
kc create secret generic "$SECRET_NAME" \
  --from-file=token="$QUMULO_TOKEN_FILE" \
  --dry-run=client -o yaml | kc apply -f - >/dev/null
ok "secret ${SECRET_NAME}"

cat >"${WORK_DIR}/values.yaml" <<EOF
image:
  controller: {repository: qumulo-csi-driver-controller, tag: "${IMAGE_TAG}", pullPolicy: IfNotPresent}
  node:       {repository: qumulo-csi-driver-node,       tag: "${IMAGE_TAG}", pullPolicy: IfNotPresent}

controller:
  logLevel: debug

node:
  logLevel: debug
  # Mounts the node's own /etc/krb5.keytab and /etc/krb5.conf into the plugin.
  # The chart refuses a krb5 StorageClass unless this is set, so that the host
  # prerequisites are a deliberate act rather than a surprise at mount time.
  kerberos:
    enabled: true

qumulo:
  apiTimeout: 30s
EOF

if [[ -n "$CA_BUNDLE_FILE" ]]; then
  [[ -f "$CA_BUNDLE_FILE" ]] || die "CA bundle not found: $CA_BUNDLE_FILE"
  kc create configmap qumulo-ca --from-file=ca.crt="$CA_BUNDLE_FILE" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  cat >>"${WORK_DIR}/values.yaml" <<'EOF'
  insecureSkipTlsVerify: false
  caBundle:
    existingConfigMap: qumulo-ca
    key: ca.crt
EOF
  ok "TLS verification on, CA from ${CA_BUNDLE_FILE}"
else
  # Said out loud: a stock Qumulo certificate is CN=localhost with no SAN and
  # can never verify, so this is the usual lab posture -- but it is a posture.
  warn "no CA_BUNDLE_FILE: installing with insecureSkipTlsVerify=true (REST API only; the krb5p data path is still encrypted)"
  printf '  insecureSkipTlsVerify: true\n' >>"${WORK_DIR}/values.yaml"
fi

helm --kube-context "$PROFILE" -n "$NAMESPACE" upgrade --install "$RELEASE" "$CHART_DIR" \
  -f "${WORK_DIR}/values.yaml" --wait --timeout "${TIMEOUT}s" >/dev/null
ok "helm release ${RELEASE}"

kc rollout status deploy/"${RELEASE}-qumulo-csi-driver-controller" --timeout="${TIMEOUT}s" >/dev/null
kc rollout status ds/"${RELEASE}-qumulo-csi-driver-node" --timeout="${TIMEOUT}s" >/dev/null
ok "controller and node plugin ready"

# --- workload ---------------------------------------------------------------
log "Applying the test workload"
sed -e "s|__QUMULO_SERVER__|${QUMULO_SERVER}|g" \
    -e "s|__QUMULO_SECRET__|${SECRET_NAME}|g" \
    -e "s|__NAMESPACE__|${NAMESPACE}|g" \
    -e "s|__BASE_PATH__|${BASE_PATH}|g" \
    -e "s|__EXPORT_BASE_PATH__|${EXPORT_BASE_PATH}|g" \
    "${SCRIPT_DIR}/test-manifests.yaml" >"${WORK_DIR}/manifests.yaml"
kubectl --context "$PROFILE" apply -f "${WORK_DIR}/manifests.yaml" >/dev/null
ok "StorageClass, PVC and Pod applied"

log "Waiting for the PVC to bind"
if ! kc wait --for=jsonpath='{.status.phase}'=Bound pvc/qumulo-krb5p-pvc --timeout="${TIMEOUT}s" >/dev/null 2>&1; then
  bad "PVC did not bind within ${TIMEOUT}s"
  kc describe pvc qumulo-krb5p-pvc | sed -n '/Events:/,$p' | head -20
  exit 1
fi
ok "PVC Bound ($(kc get pvc qumulo-krb5p-pvc -o jsonpath='{.spec.volumeName}'))"

log "Waiting for the pod to run"
if ! kc wait --for=condition=Ready pod/qumulo-secure-app --timeout="${TIMEOUT}s" >/dev/null 2>&1; then
  bad "pod did not become Ready within ${TIMEOUT}s"
  kc describe pod qumulo-secure-app | sed -n '/Events:/,$p' | head -25
  printf '\n  node plugin log:\n'
  kc logs -l app.kubernetes.io/component=node -c qumulo-csi-driver --tail=25 2>/dev/null | sed 's/^/    /'
  exit 1
fi
ok "pod Running"

# --- assertions -------------------------------------------------------------
log "Verifying the mount"

mount_line="$(kc exec qumulo-secure-app -- mount 2>/dev/null | grep ' /data ' || true)"
[[ -n "$mount_line" ]] || { bad "/data is not mounted"; exit 1; }
printf '  %s\n' "$mount_line"

if grep -q 'sec=krb5p' <<<"$mount_line"; then
  ok "mount carries sec=krb5p"
else
  bad "mount is NOT krb5p -- the flavour was negotiated down. Actual: $(grep -o 'sec=[a-z0-9]*' <<<"$mount_line" || echo 'no sec= option')"
fi

grep -q 'vers=4.1'      <<<"$mount_line" && ok "NFSv4.1"                || bad "not NFSv4.1"
grep -q 'type nfs4'     <<<"$mount_line" && ok "nfs4 filesystem type"   || bad "unexpected filesystem type"

log "Verifying the pod can write through it"
sleep 3   # the workload appends once a second
if kc exec qumulo-secure-app -- sh -c 'test -s /data/krb5p-timestamps.log' >/dev/null 2>&1; then
  ok "data written: $(kc exec qumulo-secure-app -- sh -c 'wc -l < /data/krb5p-timestamps.log' 2>/dev/null | tr -d ' \r') lines"
  kc exec qumulo-secure-app -- sh -c 'tail -2 /data/krb5p-timestamps.log' 2>/dev/null | sed 's/^/    /'
else
  bad "no data written to /data"
fi

log "Verifying the EXPORT requires krb5p (not just the client asking for it)"
if [[ -n "$QUMULO_API_URL" || -n "$QUMULO_SERVER" ]]; then
  api="${QUMULO_API_URL:-https://${QUMULO_SERVER}:8000}"
  token="$(tr -d '\n\r ' <"$QUMULO_TOKEN_FILE")"
  curl_tls=(-k); [[ -n "$CA_BUNDLE_FILE" ]] && curl_tls=(--cacert "$CA_BUNDLE_FILE")
  pv="$(kc get pvc qumulo-krb5p-pvc -o jsonpath='{.spec.volumeName}')"

  mode="$(curl -s "${curl_tls[@]}" -H "Authorization: Bearer ${token}" --max-time 20 \
    "${api}/v3/nfs/exports/" \
    | python3 -c "
import json,sys
d=json.load(sys.stdin); ents=d['entries'] if isinstance(d,dict) and 'entries' in d else d
print(next((e['restrictions'][0]['required_authentication_mode']
            for e in ents if '${pv}' in (e.get('fs_path') or '')), 'NOT_FOUND'))" 2>/dev/null || echo QUERY_FAILED)"

  case "$mode" in
    AUTHENTICATION_MODE_KRB5P) ok "export requires ${mode}" ;;
    NOT_FOUND)    bad "no export found for ${pv} -- dedicated-export did not create one" ;;
    QUERY_FAILED) warn "could not query the Qumulo API; skipping the export assertion" ;;
    *)            bad "export requires ${mode}, expected AUTHENTICATION_MODE_KRB5P" ;;
  esac
else
  warn "no Qumulo API endpoint; skipping the export assertion"
fi

log "Summary"
kc get pvc,pod -o wide 2>/dev/null | sed 's/^/  /'
