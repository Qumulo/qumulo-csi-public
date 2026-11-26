#!/bin/bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# I-13: after a failed `helm upgrade` (here the stock render, openshift.enabled=false,
# with --wait), a plain upgrade back does not remove the controller's runAsUser:
# Helm diffs against the last *deployed* revision. `--force` repairs it. Own
# namespace qcsi-i13, own driver name and health port, no StorageClasses.
set -u; source "$(dirname "$0")/common.sh"
OUT=$RUN/failed-upgrade; mkdir -p $OUT; : > $OUT/verdicts.txt; exec > >(tee $OUT/log.txt) 2>&1
NS=qcsi-i13; REL=i13; FN=$REL-qumulo-csi-driver
SETS="--set driverName=i13.csi.qumulo.com --set node.healthPort=29755 --set node.registrarHealthPort=29761 --set qumulo.insecureSkipTlsVerify=true $(imgsets)"
uid(){ oc -n $NS get deploy $FN-controller -o jsonpath='{.spec.template.spec.containers[0].securityContext.runAsUser}'; }
oc create ns $NS
helm install $REL $CHART -n $NS $SETS --wait --timeout 300s > $OUT/install.txt 2>&1
check "install: every pod Ready" wait_up $NS $REL 300
helm upgrade $REL $CHART -n $NS $SETS --set openshift.enabled=false --wait --timeout 60s > $OUT/upgrade-stock.txt 2>&1; rc=$?
check "the stock-render upgrade fails (pods refused by SCC)" test $rc -ne 0
helm upgrade $REL $CHART -n $NS $SETS --wait --timeout 90s > $OUT/upgrade-plain.txt 2>&1; rc=$?
say "plain upgrade rc=$rc, controller runAsUser=[$(uid)]"
check "a plain upgrade leaves the failed revision's runAsUser 65532 (I-13, documented)" test "$(uid)" = 65532
helm upgrade $REL $CHART -n $NS $SETS --force --wait --timeout 300s > $OUT/upgrade-force.txt 2>&1; rc=$?
check "helm upgrade --force repairs it: rc 0, no runAsUser" bash -c "test $rc -eq 0 && test -z \"\$(oc -n $NS get deploy $FN-controller -o jsonpath='{.spec.template.spec.containers[0].securityContext.runAsUser}')\""
check "after --force every pod Ready" wait_up $NS $REL 300
helm uninstall $REL -n $NS --wait --timeout 300s; oc delete ns $NS --wait=true --timeout=300s
cat $OUT/verdicts.txt
