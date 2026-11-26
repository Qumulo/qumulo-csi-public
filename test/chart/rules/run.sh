#!/usr/bin/env bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License
#
# make alert-rules-test: check the chart's alert rules and run their promtool
# unit tests (test/chart/rules/alerts_test.yaml) against the rules exactly as
# the chart renders them -- extracted from test/chart/golden/metrics.yaml,
# which make helm-golden keeps current.
#
# promtool is fetched at a pinned version and checked against its published
# sha256 before it runs. It cannot be `go run` like the other tools: the
# prometheus module carries replace directives.
set -euo pipefail

PROMTOOL_VERSION="${PROMTOOL_VERSION:-3.5.0}"
root="$(cd "$(dirname "$0")/../../.." && pwd)"
bin="${root}/bin/promtool-${PROMTOOL_VERSION}"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "alert-rules-test: unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

# From https://github.com/prometheus/prometheus/releases/download/v3.5.0/sha256sums.txt
case "${PROMTOOL_VERSION}/${os}-${arch}" in
  3.5.0/linux-amd64)  sum=e811827af26d822afb09a4f28314f61b618b12cff5369835a67f674d8b46f39a ;;
  3.5.0/linux-arm64)  sum=173389cc42bf09c4e6e54cb53fa07a5a835d7c261e14775d2183181d6e385d1c ;;
  3.5.0/darwin-amd64) sum=5856d0c306e7d852d9a79788b20acb630501b3fab120b69f5317d6b0732dea57 ;;
  3.5.0/darwin-arm64) sum=c17bc2c992f0b5515eebe2b4ccd626214f8e309a0fe6ec40ed13c42ec13d05fb ;;
  *) echo "alert-rules-test: no pinned checksum for promtool ${PROMTOOL_VERSION} on ${os}-${arch}" >&2; exit 1 ;;
esac

if [[ ! -x "${bin}" ]]; then
  work="$(mktemp -d)"
  trap 'rm -rf "${work}"' EXIT
  name="prometheus-${PROMTOOL_VERSION}.${os}-${arch}"
  curl -fsSL -o "${work}/${name}.tar.gz" \
    "https://github.com/prometheus/prometheus/releases/download/v${PROMTOOL_VERSION}/${name}.tar.gz"
  if command -v sha256sum >/dev/null; then
    echo "${sum}  ${work}/${name}.tar.gz" | sha256sum -c - >/dev/null
  else
    echo "${sum}  ${work}/${name}.tar.gz" | shasum -a 256 -c - >/dev/null
  fi
  tar -xzf "${work}/${name}.tar.gz" -C "${work}" "${name}/promtool"
  mkdir -p "$(dirname "${bin}")"
  mv "${work}/${name}/promtool" "${bin}"
fi

run="$(mktemp -d)"
trap 'rm -rf "${run}"' EXIT
(cd "${root}" && go run ./test/chart/rules/extract < test/chart/golden/metrics.yaml > "${run}/rules.yaml")
cp "${root}/test/chart/rules/alerts_test.yaml" "${run}/"
cd "${run}"
"${bin}" check rules rules.yaml
"${bin}" test rules alerts_test.yaml
