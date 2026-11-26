#!/usr/bin/env bash
#
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License
#
# Fails if any live-* Makefile target names a test that does not exist.
#
# This gate exists because `go test -run` treats "no test matched" as success.
# A target whose pattern matches nothing prints "ok ... [no tests to run]" and
# exits 0, so `make live-createvolume` reported a pass while running not one
# assertion -- and did so for every mutating target at once, which is how
# test/live/ came to be cited in a status report as a suite that covered
# CreateVolume, DeleteVolume, ExpandVolume, faults, node mounting, discovery
# and the dedicated-export model while containing none of them.
#
# Nothing here needs a cluster, so it belongs in `make ci` alongside the other
# gates rather than with the live targets it checks.

set -euo pipefail

cd "$(dirname "$0")/../.."

makefile="Makefile"

# Two directories now, not one. The storage-outage cases moved to
# test/openshift so that a nightly can run the whole feature with a single
# command, and this gate followed them -- when it did not, its count dropped
# from 19 patterns to 18 and reported "all matched", which is precisely the
# silence it was written to break.
live_dirs="test/live test/openshift"

# Every test function those suites declare. Subtests are deliberately not
# collected: a -run pattern naming a subtest still has to match its parent.
declared="$(grep -rhoE '^func (Test[A-Za-z0-9_]+)' $live_dirs | sed 's/^func //' | sort -u)"

if [ -z "$declared" ]; then
    echo "check-live-targets: found no Test functions in $live_dirs" >&2
    echo "  Either a directory moved or its build tag changed." >&2
    exit 1
fi

# Pull the -run pattern out of every live target. Two spellings are in use:
# `-run 'PATTERN'` for the ordinary targets, and `-test.run PATTERN` for
# live-node, which compiles a binary first so it can run it under sudo.
#
# A pattern counts as this script's business when the command carrying it names
# one of those suites -- `./test/live/` or `./test/openshift/`, or the
# `./bin/live.test` or `./bin/openshift.test` binary built from them. Selecting by command rather than by how the test happens to be spelled is
# deliberate: this gate used to collect every pattern in the Makefile and keep
# only those starting with a literal "TestLive", which quietly excused
# `live-defects` and its `-run '^TestDefect_'`. That target matched nothing and
# passed for it on every array, while this script reported "all matched" -- the
# exact failure it exists to catch, missed because it only ever inspected the
# names it already expected.
# Comment lines are skipped: a comment explaining a removed target would
# otherwise read as the target still being there.
live_cmds="$(grep -vE '^[[:space:]]*#' "$makefile" \
    | grep -E '\./test/live/|\./test/openshift/|\./bin/live\.test|\./bin/openshift\.test' || true)"

patterns="$(
    printf '%s\n' "$live_cmds" | grep -oE "(-run|-test\.run) +'[^']+'" | sed -E "s/^(-run|-test\.run) +'(.*)'$/\2/" || true
    printf '%s\n' "$live_cmds" | grep -oE "(-run|-test\.run) +[A-Za-z][A-Za-z0-9_()|]*" | sed -E 's/^(-run|-test\.run) +//' || true
)"

# `live-k8s-impact` spells its pattern `$(or $(RUN),TestLiveK8sImpact)` so one
# test can be selected from the command line. Audit the default it falls back to.
patterns="$(printf '%s\n' "$patterns" | sed -E 's/^\$\(or \$\([A-Za-z_]+\),(.*)\)$/\1/' | sort -u || true)"

if [ -z "$patterns" ]; then
    echo "check-live-targets: found no -run patterns on the $live_dirs commands in $makefile" >&2
    echo "  The targets moved, or their spelling changed; this gate is now blind." >&2
    exit 1
fi

# Anything still carrying a make expansion cannot be checked by reading the
# Makefile. Say so and fail, rather than skipping it the way the old name filter
# skipped live-defects.
unresolved="$(printf '%s\n' "$patterns" | grep -F '$(' || true)"
if [ -n "$unresolved" ]; then
    echo "check-live-targets: these -run patterns contain an unresolved make expansion:" >&2
    printf '%s\n' "$unresolved" | sed 's/^/  /' >&2
    echo "  This gate cannot tell what they match. Give the target a literal default." >&2
    exit 1
fi

dangling=""
checked=0
while IFS= read -r pattern; do
    [ -z "$pattern" ] && continue
    checked=$((checked + 1))

    # Go matches -run unanchored against the test name, so grep -E is the same
    # question the toolchain asks.
    if ! printf '%s\n' "$declared" | grep -qE "$pattern"; then
        dangling="${dangling}${pattern}"$'\n'
    fi
done <<<"$patterns"

if [ -n "$dangling" ]; then
    echo "check-live-targets: these Makefile -run patterns match no test in $live_dirs:" >&2
    printf '%s' "$dangling" | sed 's/^/  /' >&2
    echo >&2
    echo "Each one is a target that exits 0 having run nothing. Write the suite," >&2
    echo "or delete the target -- either closes it, but a target that reports a" >&2
    echo "pass without testing anything is worse than no target at all." >&2
    exit 1
fi

echo "check-live-targets: $checked live -run pattern(s), all matched"
