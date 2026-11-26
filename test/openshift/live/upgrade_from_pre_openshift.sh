#!/bin/bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# Phase 1 / I-1: upgrade from a release made with the pre-OpenShift chart.
# The stock pre-OpenShift chart installs on OpenShift with 0 pods (baseline). Then
# `helm upgrade --reuse-values` with the base chart (expected to fail, I-1) and
# with the branch chart (expected to pass and bring every pod up, no SCC step).
# Own namespace qcsi-upg, own driver name, no StorageClasses.
set -u; source "$(dirname "$0")/common.sh"
OUT=$RUN/upgrade; mkdir -p $OUT; : > $OUT/verdicts.txt; exec > >(tee $OUT/log.txt) 2>&1
NS=qcsi-upg; REL=upg; DRV=upg.csi.qumulo.com; FN=$REL-qumulo-csi-driver
oc create ns $NS
say "baseline: stock pre-OpenShift chart"
helm install $REL $OLD_CHART -n $NS --set driverName=$DRV --set qumulo.insecureSkipTlsVerify=true --set image.controller.repository=$REG/qumulo-csi-driver-controller --set image.controller.tag=$OLD_TAG --set image.node.repository=$REG/qumulo-csi-driver-node --set image.node.tag=$OLD_TAG 2>&1 | tail -3
sleep 45
oc -n $NS get ds,deploy,rs 2>&1 | tee $OUT/baseline.txt
oc -n $NS get events --field-selector reason=FailedCreate -o custom-columns=M:.message --no-headers | sort -u | cut -c1-300 | head -4 | tee $OUT/baseline-events.txt
check "baseline: stock chart creates no node pods on OpenShift" test "$(oc -n $NS get ds $FN-node -o jsonpath='{.status.currentNumberScheduled}')" = 0
if [ -n "$BASE_CHART" ]; then
say "upgrade --reuse-values with BASE_CHART, expected to fail (I-1 before the fix)"
helm upgrade $REL $BASE_CHART -n $NS --reuse-values > $OUT/upgrade-base.txt 2>&1; rc=$?
tail -3 $OUT/upgrade-base.txt
check "BASE_CHART: --reuse-values upgrade fails with the nil pointer (I-1 reproduced)" bash -c "test $rc -ne 0 && grep -q 'nil pointer evaluating interface {}.enabled' $OUT/upgrade-base.txt"
fi
say "upgrade --reuse-values with CHART, new images"
helm upgrade $REL $CHART -n $NS --reuse-values --set image.controller.tag=$TAG --set image.node.tag=$TAG --set node.healthPort=29754 --set node.registrarHealthPort=29760 > $OUT/upgrade-new.txt 2>&1; rc=$?
tail -3 $OUT/upgrade-new.txt
check "CHART: --reuse-values upgrade succeeds (I-1)" test $rc -eq 0
check "after upgrade every pod is Ready with no SCC step" wait_up $NS $REL 300
scc_of $NS | tee $OUT/pod-scc.txt
check "node pods under the chart's SCC after upgrade" bash -c "grep -- -node- $OUT/pod-scc.txt | awk '{print \$2}' | sort -u | grep -qx $NS-$REL-qumulo-csi-driver-node"
say "uninstall"; helm uninstall $REL -n $NS --wait --timeout 300s
oc get scc,clusterrole,clusterrolebinding,csidriver,priorityclass -o name | grep -E "$FN|$DRV|$NS-$REL" | tee $OUT/leftovers.txt
check "uninstall leaves nothing cluster-scoped" test ! -s $OUT/leftovers.txt
oc delete ns $NS --wait=true --timeout=300s
cat $OUT/verdicts.txt
