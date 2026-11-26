#!/bin/bash
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# A clean install on OpenShift with default values and no SCC
# step, a PVC written by a restricted-v2 pod, then uninstall leaves nothing.
# Own namespace qcsi-fresh, own driver name, so the lab release is untouched.
set -u; source "$(dirname "$0")/common.sh"
OUT=$RUN/fresh-install; mkdir -p $OUT; : > $OUT/verdicts.txt; exec > >(tee $OUT/log.txt) 2>&1
NS=qcsi-fresh; REL=fresh; DRV=fresh.csi.qumulo.com; APP=qcsi-fresh-app
say "fresh install of $CHART, images $TAG"
oc create ns $NS; oc create ns $APP
oc -n $DRIVER_NS get secret $TOKEN_SECRET -o json | python3 -c 'import json,sys;d=json.load(sys.stdin);m=d["metadata"];d["metadata"]={"name":m["name"],"namespace":"qcsi-fresh"};print(json.dumps(d))' | oc apply -f -
cat > $OUT/values.yaml <<V
driverName: $DRV
# The lab release already holds 29653 and 29659 on every node (hostNetwork): a
# second driver needs its own health ports (I-2, the value this checks), or
# its node pods are never scheduled.
node:
  healthPort: 29753
  registrarHealthPort: 29759
qumulo:
  insecureSkipTlsVerify: true
storageClasses:
  - name: fresh-nfs41
    server: $ARRAY
    apiUrl: $API_URL
    secretName: $TOKEN_SECRET
    secretNamespace: $NS
    basePath: $BASE_PATH
    protocol: nfs4.1
    provisioningModel: dedicated-export
    allowedClients: [$ALLOWED_CLIENTS]
    mountOptions: [hard, timeo=600]
  - name: fresh-nfs3
    server: $ARRAY
    apiUrl: $API_URL
    secretName: $TOKEN_SECRET
    secretNamespace: $NS
    basePath: $BASE_PATH
    protocol: nfs3
    provisioningModel: dedicated-export
    allowedClients: [$ALLOWED_CLIENTS]
    mountOptions: [nolock, hard, timeo=600]
V
start=$(date +%s)
helm install $REL $CHART -n $NS -f $OUT/values.yaml $(imgsets) 2>&1 | tee $OUT/helm-install.txt
check "helm install exits 0" test ${PIPESTATUS[0]} -eq 0
check "all node and controller pods Ready, no SCC step" wait_up $NS $REL 300
say "pods up in $(( $(date +%s) - start )) s"
scc_of $NS | tee $OUT/pod-scc.txt
check "controller pods admitted under restricted-v2" bash -c "grep controller $OUT/pod-scc.txt | awk '{print \$2}' | sort -u | grep -qx restricted-v2"
check "node pods admitted under the chart's own SCC" bash -c "grep -- -node- $OUT/pod-scc.txt | awk '{print \$2}' | sort -u | grep -qx $NS-$REL-qumulo-csi-driver-node"
ds=$(oc -n $NS get ds $REL-qumulo-csi-driver-node -o json)
check "node pods mount no ServiceAccount token (I-4)" bash -c "echo '$ds' | python3 -c 'import json,sys;print(json.load(sys.stdin)[\"spec\"][\"template\"][\"spec\"].get(\"automountServiceAccountToken\"))' | grep -qx False"
oc get scc $NS-$REL-qumulo-csi-driver-node -o jsonpath='{.volumes}' | tee $OUT/scc-volumes.txt; echo
check "node SCC allows only emptyDir and hostPath (I-4)" bash -c "grep -qx '\[\"emptyDir\",\"hostPath\"\]' $OUT/scc-volumes.txt"
pod=$(oc -n $NS get pod -l app.kubernetes.io/component=node -o jsonpath='{.items[0].metadata.name}'); nip=$(oc -n $NS get pod $pod -o jsonpath='{.status.podIP}')
check "node health endpoint bound to the node address $nip:29753 (I-2)" bash -c "oc -n $NS get pod $pod -o yaml | grep -q -- '--http-endpoint=\$(POD_IP):29753' && oc -n $NS exec $pod -c qumulo-csi-driver -- sh -c 'cat /proc/net/tcp' | awk 'NR>1 && \$4==\"0A\"{print \$2}' | grep -qi ':$(hexport 29753)\$' && ! oc -n $NS exec $pod -c qumulo-csi-driver -- sh -c 'cat /proc/net/tcp' | awk 'NR>1 && \$4==\"0A\"{print \$2}' | grep -qi '^00000000:$(hexport 29753)\$'"
for c in fresh-nfs41 fresh-nfs3; do
  cat <<Y | oc apply -f - >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $c, namespace: $APP}
spec: {accessModes: [ReadWriteMany], storageClassName: $c, resources: {requests: {storage: 1Gi}}}
---
apiVersion: v1
kind: Pod
metadata: {name: $c, namespace: $APP}
spec:
  securityContext: {runAsNonRoot: true, seccompProfile: {type: RuntimeDefault}}
  containers:
  - name: app
    image: $APP_IMAGE
    command: [sh, -c, 'id; echo hello > /data/f && cat /data/f && echo WRITE_OK; sleep 3600']
    securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
    volumeMounts: [{name: d, mountPath: /data}]
  volumes: [{name: d, persistentVolumeClaim: {claimName: $c}}]
Y
done
for c in fresh-nfs41 fresh-nfs3; do
  oc -n $APP wait --for=condition=Ready pod/$c --timeout=240s >/dev/null
  oc -n $APP logs $c > $OUT/app-$c.log 2>&1; oc -n $APP get pod $c -o jsonpath='{.metadata.annotations.openshift\.io/scc}' > $OUT/app-$c.scc
  check "$c: restricted-v2 app with random uid writes and reads" bash -c "grep -q WRITE_OK $OUT/app-$c.log && grep -qx restricted-v2 $OUT/app-$c.scc"
done
cat $OUT/app-*.log
say "cleanup app objects"; oc delete ns $APP --wait=true --timeout=300s
say "uninstall"; helm uninstall $REL -n $NS --wait --timeout 300s 2>&1 | tee $OUT/helm-uninstall.txt
oc get scc,clusterrole,clusterrolebinding,csidriver,priorityclass -o name 2>/dev/null | grep -E "$REL-qumulo|$DRV|$NS-$REL" | tee $OUT/leftovers.txt
check "uninstall removes the SCC, its grant, CSIDriver and PriorityClass" test ! -s $OUT/leftovers.txt
oc get sc -o name | grep fresh- | tee $OUT/leftover-classes.txt
say "classes are kept by design (helm.sh/resource-policy: keep); deleting them"; oc delete sc fresh-nfs41 fresh-nfs3 --ignore-not-found
oc delete ns $NS --wait=true --timeout=300s
say "done"; cat $OUT/verdicts.txt
