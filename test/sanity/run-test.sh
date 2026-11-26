#!/bin/bash

# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

#
# Runs the official kubernetes-csi/csi-test csi-sanity suite against this
# driver's real binary. See README.md for what is real and what is faked.
#
# Expects `bin/qumulo-csi-driver` and `bin/mock-qumulo-server` to already be
# built (the Makefile's sanity-test target depends on `build mock-server`).

set -euo pipefail

# Pinned so a run today and a run in a year exercise the same conformance
# suite; bump deliberately, not as a side effect of an unrelated change.
readonly CSI_SANITY_VERSION='v5.6.0'

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly WORK_DIR="$(mktemp -d /tmp/qumulo-csi-sanity.XXXXXX)"
readonly SOCK="${WORK_DIR}/csi.sock"
readonly MOCK_LOG="${WORK_DIR}/mock-qumulo-server.log"
readonly DRIVER_LOG="${WORK_DIR}/qumulo-csi-driver.log"

MOCK_PID=''
DRIVER_PID=''

cleanup() {
  local status=$?
  if [[ -n "${DRIVER_PID}" ]]; then
    echo 'Stopping qumulo-csi-driver'
    kill "${DRIVER_PID}" 2>/dev/null || true
    wait "${DRIVER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${MOCK_PID}" ]]; then
    echo 'Stopping mock-qumulo-server'
    kill "${MOCK_PID}" 2>/dev/null || true
    wait "${MOCK_PID}" 2>/dev/null || true
  fi
  if [[ ${status} -ne 0 ]]; then
    echo "--- mock-qumulo-server log (${MOCK_LOG}) ---"
    cat "${MOCK_LOG}" 2>/dev/null || true
    echo "--- qumulo-csi-driver log (${DRIVER_LOG}) ---"
    cat "${DRIVER_LOG}" 2>/dev/null || true
  fi
  echo "Removing scratch directory ${WORK_DIR}"
  rm -rf "${WORK_DIR}"
  exit "${status}"
}
trap cleanup EXIT

# waits for a probe command to succeed, or fails loudly instead of hanging
# on a driver/server that died on startup.
wait_for() {
  local what="$1" probe="$2" tries="${3:-50}"
  for ((i = 0; i < tries; i++)); do
    if eval "${probe}"; then
      return 0
    fi
    sleep 0.2
  done
  echo "timed out waiting for ${what}" >&2
  return 1
}

echo "Installing csi-sanity ${CSI_SANITY_VERSION} into ${WORK_DIR} (repo's own go.mod is untouched)"
GOBIN="${WORK_DIR}" go install "github.com/kubernetes-csi/csi-test/v5/cmd/csi-sanity@${CSI_SANITY_VERSION}"

# The token every RPC's secrets carry (test/sanity/secrets.yaml). Required
# rather than accepted, so the driver's per-request authentication is actually
# verified: with the mock accepting anything, a driver that sent no
# Authorization header at all would have passed this suite.
readonly MOCK_TOKEN='sanity-test-token'

# Three settings that decide how much of the driver this suite really reaches.
# Left at their defaults, csi-sanity goes green over code paths it never
# executes:
#
#   -token             makes authentication real, as above.
#   -tree-delete-delay keeps a tree-delete job in progress, so DeleteVolume
#                      polls instead of finding it finished on the first
#                      call -- the async branch of awaitTreeDelete, including
#                      the CO-retry path. Comfortably under the driver's
#                      10s budget, so deletes still succeed.
#   -entries-page-size splits directory listings. Set to 1 so that any restore
#                      of more than one entry crosses a page boundary. Note
#                      what this does NOT currently achieve: csi-sanity
#                      snapshots a volume it just created, which is an empty
#                      directory, so ListDirectory returns nothing and the
#                      paging loop is still never reached here. The flag is
#                      armed for the day that changes; the loop's actual
#                      coverage is TestListDirectoryFollowsPaging in
#                      pkg/qumulo. Verified by counting the mock's requests,
#                      not assumed.
echo 'Starting mock-qumulo-server on https://127.0.0.1:8000 (self-signed)'
"${REPO_ROOT}/bin/mock-qumulo-server" \
  -addr=127.0.0.1:8000 \
  -tls \
  -token="${MOCK_TOKEN}" \
  -tree-delete-delay=1s \
  -entries-page-size=1 \
  -log-level=info >"${MOCK_LOG}" 2>&1 &
MOCK_PID=$!
# The health probe carries the token too: -token guards every route,
# /v1/version included, so an unauthenticated probe would never see the
# server come up.
wait_for 'mock-qumulo-server to answer' \
  "curl -fsSk -o /dev/null -H 'Authorization: Bearer ${MOCK_TOKEN}' https://127.0.0.1:8000/v1/version"

echo "Starting qumulo-csi-driver (-fake-mounter: see README.md) on unix://${SOCK}"
"${REPO_ROOT}/bin/qumulo-csi-driver" \
  -mode=all \
  -endpoint="unix://${SOCK}" \
  -nodeid=sanity-node \
  -insecure-skip-tls-verify \
  -fake-mounter \
  -log-level=info \
  >"${DRIVER_LOG}" 2>&1 &
DRIVER_PID=$!
wait_for 'the CSI unix socket to appear' "[[ -S '${SOCK}' ]]"

# Two RPCs are skipped, for two different reasons. Every other unimplemented
# RPC is simply not advertised, so csi-sanity skips it without an entry here.
#
# ListSnapshots without a snapshot_id/source_volume_id filter is refused by
# design (InvalidArgument): a multi-cluster controller cannot enumerate
# clusters it was never told about. See ListSnapshots's doc comment in
# pkg/driver/controller_server.go.
#
# ListVolumes is a different case, and the distinction matters. It *is*
# implemented -- but it answers by listing this driver's StorageClasses through
# the Kubernetes API to find the clusters they name, and this harness runs the
# real binary over a unix socket with no Kubernetes anywhere. So all three of
# its specs would fail on FailedPrecondition, and one of them functionally:
# "check the presence of new volumes" creates a volume and requires the listing
# to grow by one.
#
# Skipped rather than worked around, deliberately. The two alternatives are
# both worse. A test-only flag feeding synthetic StorageClasses from a file
# would make the driver hold a standing credential read from disk, which is the
# exact thing Rule 0 exists to prevent -- a test hook that weakens the security
# model is the wrong precedent even when it is guarded. And advertising
# LIST_VOLUMES only when Kubernetes access is present would make the capability
# set depend on the deployment, which controllerCapabilities deliberately
# refuses.
#
# So ListVolumes is verified where Kubernetes actually exists: unit tests in
# pkg/driver/list_volumes_inventory_test.go drive every path against fakes, and
# the minikube target exercises it against a real cluster with real classes.
# This means the csi-sanity figure does NOT move when ListVolumes lands. If it
# does, the skip stopped matching and something is being hidden.
#
# ControllerGetVolumeHealth is skipped for the same reason: its spec
# sends no secrets, as the real caller, external-health-monitor, does not, so
# the driver answers it from the classes -- the inventory path -- and this
# harness has no Kubernetes. pkg/driver/volume_health_test.go covers it.
readonly GINKGO_SKIP='(ListSnapshots.*(should return appropriate values \(no optional values added\)|check the presence of new snapshots in the snapshot list|should return next token when a limited number of entries are requested))|(ListVolumes)|(ControllerGetVolumeHealth)'

echo 'Running csi-sanity...'
"${WORK_DIR}/csi-sanity" \
  --ginkgo.v \
  --ginkgo.skip="${GINKGO_SKIP}" \
  --csi.endpoint="unix://${SOCK}" \
  --csi.testvolumeparameters="${REPO_ROOT}/test/sanity/params.yaml" \
  --csi.secrets="${REPO_ROOT}/test/sanity/secrets.yaml" \
  --csi.mountdir="${WORK_DIR}/mnt" \
  --csi.stagingdir="${WORK_DIR}/staging"
