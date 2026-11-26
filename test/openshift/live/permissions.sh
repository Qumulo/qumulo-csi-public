#!/bin/bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# What each driver service account may do, read from the live
# cluster. Read-only.
set -u; source "$(dirname "$0")/common.sh"
OUT=$RUN/permissions; mkdir -p $OUT; : > $OUT/verdicts.txt; exec > >(tee $OUT/log.txt) 2>&1
NS=$DRIVER_NS; FN=$DRIVER_RELEASE-qumulo-csi-driver
# The SCC the node pods are pinned to, read from the DaemonSet rather than
# rebuilt here: the derived name drops the namespace prefix when the release is
# named after its namespace, and openshift.nodeSCC.name can name any SCC.
SCC=$(oc -n $NS get ds $FN-node -o jsonpath='{.spec.template.metadata.annotations.openshift\.io/required-scc}')
[ -n "$SCC" ] || { echo "no openshift.io/required-scc on ds/$FN-node in $NS"; exit 1; }
for sa in controller node; do
  oc auth can-i --list -n $NS --as=system:serviceaccount:$NS:$FN-$sa > $OUT/can-i-$sa-$NS.txt
  oc auth can-i --list -n default --as=system:serviceaccount:$NS:$FN-$sa > $OUT/can-i-$sa-default.txt
done
oc get scc $SCC -o yaml > $OUT/node-scc.yaml
oc get clusterrole,role -A -o json | python3 -c '
import json,sys
for r in json.load(sys.stdin)["items"]:
    n=r["metadata"]["name"]
    if "qumulo-csi-driver" in n:
        for rule in r.get("rules",[]):
            print(r["kind"], r["metadata"].get("namespace","-"), n, ",".join(rule.get("apiGroups",[]) or ["\"\""]), ",".join(rule.get("resources",[])), ",".join(rule.get("verbs",[])), ",".join(rule.get("resourceNames",[])))
' | sort > $OUT/rules.txt
cat $OUT/rules.txt
# Node account: nothing in the core API at all, only `use` on its own SCC.
ask(){ oc auth can-i "$@" 2>/dev/null; }
n="--as=system:serviceaccount:$NS:$FN-node"
c="--as=system:serviceaccount:$NS:$FN-controller"
check "node SA: may use its own SCC" bash -c "[ \"\$(oc auth can-i use securitycontextconstraints/$SCC -n $NS $n)\" = yes ]"
for q in "use securitycontextconstraints/privileged" "use securitycontextconstraints/anyuid" "get secrets" "list pods" "get nodes" "list persistentvolumes" "create pods" "get configmaps"; do
  check "node SA: cannot $q" bash -c "[ \"\$(oc auth can-i $q -n $NS $n)\" = no ]"
done
check "node SA: cannot use its SCC from another namespace" bash -c "[ \"\$(oc auth can-i use securitycontextconstraints/$SCC -n default $n)\" = no ]"
for q in "list secrets" "get secrets -n kube-system" "use securitycontextconstraints/privileged" "use securitycontextconstraints/$SCC" "delete pods" "create clusterrolebindings" "escalate clusterroles" "bind clusterroles" "update daemonsets"; do
  check "controller SA: cannot $q" bash -c "[ \"\$(oc auth can-i $q -n $NS $c)\" = no ]"
done
for q in "create persistentvolumes" "get storageclasses" "list csinodes" "update leases"; do
  check "controller SA: can $q" bash -c "[ \"\$(oc auth can-i $q -n $NS $c)\" = yes ]"
done
# The grant is one namespaced RoleBinding to the node SA; nothing cluster-wide.
oc get rolebinding -A -o json | python3 -c "import json,sys;[print(b['metadata']['namespace'],b['metadata']['name'],[x['name'] for x in b['subjects']]) for b in json.load(sys.stdin)['items'] if b['roleRef']['name']=='$SCC-use']" > $OUT/scc-bindings.txt
oc get clusterrolebinding -o json | python3 -c "import json,sys;[print(b['metadata']['name']) for b in json.load(sys.stdin)['items'] if b['roleRef']['name']=='$SCC-use']" > $OUT/scc-clusterbindings.txt
cat $OUT/scc-bindings.txt
check "node SCC granted only to the node SA, in its namespace, never cluster-wide" bash -c "test \$(wc -l < $OUT/scc-bindings.txt) = 1 && grep -q \"^$NS .*\['$FN-node'\]\" $OUT/scc-bindings.txt && test ! -s $OUT/scc-clusterbindings.txt"
cat $OUT/verdicts.txt
