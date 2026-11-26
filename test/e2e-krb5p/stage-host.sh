#!/usr/bin/env bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License
#
# stage-host.sh -- prepare a Minikube VM for NFSv4.1 Kerberos (sec=krb5p) tests.
#
# Runs on the engineer's host. Starts Minikube on a real VM driver, puts a
# krb5.conf and a keytab inside the VM, installs the NFS client userland, and
# starts rpc.gssd.
#
# WHY A VM DRIVER IS NOT OPTIONAL
#
#   The GSS upcall that turns a sec=krb5p mount into a Kerberos handshake is
#   resolved by the *kernel* of whatever host owns the mount namespace, and it
#   is answered by rpc.gssd running in that host's userland. With
#   --driver=docker the "node" shares your workstation's kernel, so the upcall
#   would be answered -- or not -- by your laptop, and the test would be
#   measuring the wrong machine. A VM driver gives the node a kernel and an
#   init system of its own.
#
# WHAT THIS SCRIPT CANNOT DO
#
#   It stages a *test* Kerberos identity. It cannot join the VM to your Active
#   Directory domain, and an unjoined node cannot obtain a real service ticket
#   for the Qumulo cluster. Supply a keytab exported from your KDC for the
#   principal nfs/<vm-hostname>@<REALM>; without one, mounts will fail at the
#   GSS handshake no matter how correct the driver is.
#
# Usage:
#   KRB5_KEYTAB=./minikube.keytab KRB5_REALM=AD.ENG.EXAMPLE.COM ./stage-host.sh
set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# --- configuration ----------------------------------------------------------
readonly PROFILE="${MINIKUBE_PROFILE:-qumulo-krb5p}"
readonly DRIVER="${MINIKUBE_DRIVER:-qemu2}"        # qemu2 | virtualbox | kvm2
readonly K8S_VERSION="${K8S_VERSION:-v1.35.6}"
readonly CPUS="${MINIKUBE_CPUS:-4}"
readonly MEMORY="${MINIKUBE_MEMORY:-6144}"

# A keytab for nfs/<vm>@<REALM>. No default: a wrong keytab fails deep inside
# the GSS handshake, where the error is far less legible than this one.
readonly KEYTAB="${KRB5_KEYTAB:-}"
readonly REALM="${KRB5_REALM:-}"
readonly KDC="${KRB5_KDC:-}"                       # optional; defaults to the realm

readonly VM_KRB5_CONF=/etc/krb5.conf
readonly VM_KEYTAB=/etc/krb5.keytab

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m  ok\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  !!\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# --- preflight --------------------------------------------------------------
log "Checking the host toolchain"
for tool in minikube kubectl; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool is required but not on PATH"
  ok "$tool $(command -v "$tool")"
done

[[ -n "$KEYTAB" ]] || die "set KRB5_KEYTAB to a keytab for nfs/<vm-hostname>@<REALM>.
  Export one from your KDC, e.g.:
    ktutil addent -password -p nfs/${PROFILE}@REALM -k 1 -e aes256-cts
  This script will not invent one: a placeholder keytab produces a mount that
  fails inside the GSS handshake, which is much harder to diagnose than this."
[[ -f "$KEYTAB" ]] || die "keytab not found: $KEYTAB"
[[ -s "$KEYTAB" ]] || die "keytab is empty: $KEYTAB -- an empty keytab authenticates nothing"
[[ -n "$REALM"  ]] || die "set KRB5_REALM, e.g. AD.ENG.EXAMPLE.COM"
ok "keytab $KEYTAB ($(wc -c <"$KEYTAB") bytes), realm $REALM"

if [[ "$DRIVER" == "docker" || "$DRIVER" == "podman" ]]; then
  die "driver '$DRIVER' shares your host kernel, so rpc.gssd would run on your
  workstation rather than on the node. Use qemu2, virtualbox or kvm2."
fi

# --- start the VM -----------------------------------------------------------
if minikube -p "$PROFILE" status >/dev/null 2>&1; then
  log "Minikube profile '$PROFILE' is already running; reusing it"
else
  log "Starting Minikube (profile=$PROFILE driver=$DRIVER k8s=$K8S_VERSION)"
  # containerd, not the docker runtime: on Kubernetes 1.36 cri-dockerd breaks
  # kubectl exec cluster-wide, and every verification below needs exec.
  minikube start \
    --profile="$PROFILE" \
    --driver="$DRIVER" \
    --kubernetes-version="$K8S_VERSION" \
    --container-runtime=containerd \
    --cpus="$CPUS" \
    --memory="$MEMORY"
fi
ok "node: $(kubectl --context "$PROFILE" get nodes -o jsonpath='{.items[0].metadata.name}')"

# --- stage the Kerberos identity -------------------------------------------
log "Writing krb5.conf"
tmp_conf="$(mktemp)"
trap 'rm -f "$tmp_conf"' EXIT
cat >"$tmp_conf" <<EOF
[libdefaults]
  default_realm = ${REALM}
  dns_lookup_realm = false
  dns_lookup_kdc = true
  rdns = false
  # NFSv4 with AES. Qumulo's AD integration expects these enctypes.
  default_tkt_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96
  default_tgs_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96

[realms]
  ${REALM} = {
    kdc = ${KDC:-${REALM,,}}
    admin_server = ${KDC:-${REALM,,}}
  }

[domain_realm]
  .${REALM,,} = ${REALM}
  ${REALM,,} = ${REALM}
EOF

minikube -p "$PROFILE" cp "$tmp_conf" "$VM_KRB5_CONF"
minikube -p "$PROFILE" cp "$KEYTAB"   "$VM_KEYTAB"
# minikube cp lands files as root-owned but world-readable. A keytab must not be.
minikube -p "$PROFILE" ssh -- "sudo chmod 600 ${VM_KEYTAB} && sudo chown root:root ${VM_KEYTAB}"
ok "staged ${VM_KRB5_CONF} and ${VM_KEYTAB} (0600)"

# --- NFS client userland and rpc.gssd ---------------------------------------
log "Installing the NFS client userland in the VM"
# Minikube's ISO is buildroot-based and ships nfs-utils already; apt exists only
# on the (deprecated) none/kvm Ubuntu images. Try a package manager if one is
# there, but do not fail when the binaries are already present -- which is the
# normal case.
minikube -p "$PROFILE" ssh -- 'sudo sh -s' <<'REMOTE' || warn "package install skipped; checking for the binaries anyway"
set -e
if command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq && apt-get install -y -qq nfs-common
elif command -v yum >/dev/null 2>&1; then
  yum install -y -q nfs-utils
fi
REMOTE

log "Starting rpc.gssd"
minikube -p "$PROFILE" ssh -- 'sudo sh -s' <<'REMOTE'
set -e
# The keytab must be readable before the daemon starts, or it caches the
# failure and every mount then fails with a misleading permission error.
test -s /etc/krb5.keytab || { echo "keytab missing or empty on the VM" >&2; exit 1; }

if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files 2>/dev/null | grep -q '^rpc-gssd'; then
  systemctl enable --now rpc-gssd
else
  # Buildroot minikube has no rpc-gssd unit. Start the daemon directly; it is
  # the daemon the kernel upcall needs, not the unit wrapping it.
  pgrep -x rpc.gssd >/dev/null 2>&1 || {
    mkdir -p /var/lib/nfs/rpc_pipefs
    mountpoint -q /var/lib/nfs/rpc_pipefs || mount -t rpc_pipefs sunrpc /var/lib/nfs/rpc_pipefs
    /usr/sbin/rpc.gssd -v
  }
fi
REMOTE

# --- validation -------------------------------------------------------------
log "Validating rpc.gssd"
gssd_state="$(minikube -p "$PROFILE" ssh -- '
  if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet rpc-gssd 2>/dev/null; then
    echo "active (systemd)"
  elif pgrep -x rpc.gssd >/dev/null 2>&1; then
    echo "active (process)"
  else
    echo "inactive"
  fi' 2>/dev/null | tr -d '\r')"

if [[ "$gssd_state" == inactive ]]; then
  die "rpc.gssd is not running in the VM.

  Without it the kernel cannot perform the GSS upcall, and a sec=krb5p mount
  fails with 'an incorrect mount option was specified' -- or, if partially
  configured, hangs. Investigate with:
    minikube -p ${PROFILE} ssh -- sudo journalctl -u rpc-gssd --no-pager | tail
    minikube -p ${PROFILE} ssh -- sudo /usr/sbin/rpc.gssd -f -vvv"
fi
ok "rpc.gssd is ${gssd_state}"

log "Validating the staged keytab"
minikube -p "$PROFILE" ssh -- 'sudo klist -k /etc/krb5.keytab 2>/dev/null | head -20' \
  || warn "klist unavailable in the VM; skipping the keytab listing"

cat <<EOF

$(ok "Host staging complete")

  profile   ${PROFILE}
  driver    ${DRIVER}
  realm     ${REALM}
  rpc.gssd  ${gssd_state}

Next:
  ${SCRIPT_DIR}/run-test.sh
EOF
