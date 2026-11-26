#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

"""Can an application pod use a Qumulo volume under each SCC
and security setting OpenShift workloads run with?

For each protocol (NFS 4.1, NFS 3) and each volume directory mode the class
can ask for (mountPermissions 0777 = default, 0770, 0750), one RWX claim and
one pod per workload profile below. Each pod prints its identity, writes,
reads back and stats what it wrote. Every row is recorded; the rows the plan
grades are marked in EXPECT. Own namespace and own classes, all deleted.
"""
import json, os, subprocess, sys, time, yaml

OUT = os.path.join(os.path.expanduser(os.environ.get("RUN_DIR", "~/run-ocp-live")), "app-matrix")
NS = "qcsi-p4"
IMAGE = os.environ.get("APP_IMAGE", "registry.access.redhat.com/ubi9/ubi-minimal:9.5")
BASE_CLASSES = dict(kv.split("=", 1) for kv in os.environ.get("MATRIX_CLASSES", "nfs41=qumulo-nfs41,nfs3=qumulo-nfs3").split(","))
MODES = ["0777", "0770", "0750"]
# name -> (scc to grant the pod's service account, pod securityContext, extra pod spec)
PROFILES = {
    "rv2":          ("restricted-v2", {}, {}),
    "rv2-fsgroup":  ("restricted-v2", {"fsGroup": None}, {}),      # fsGroup from the project range
    "rv2-suppl":    ("restricted-v2", {"supplementalGroups": None}, {}),
    "rv3-userns":   ("restricted-v3", {}, {"hostUsers": False}),
    "nonroot-1001": ("nonroot-v2", {"runAsUser": 1001}, {}),
    "anyuid-root":  ("anyuid", {"runAsUser": 0}, {}),
    "privileged":   ("privileged", {"runAsUser": 0}, {}),
}
# (protocol, mode, profile) -> expected write result; others are recorded only.
EXPECT = {}
for p in BASE_CLASSES:
    for prof in PROFILES:
        EXPECT[(p, "0777", prof)] = True          # default mode: every profile writes
    EXPECT[(p, "0770", "rv2")] = True              # gid 0 is the group: writes
    EXPECT[(p, "0750", "rv2")] = False             # read-only for the group: refused, cleanly


def sh(cmd, check=True, inp=None):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, input=inp)
    if check and r.returncode:
        raise RuntimeError(f"{cmd}: {r.stderr.strip()}")
    return r.stdout


def apply(obj):
    sh("oc apply -f -", inp=yaml.safe_dump(obj))


def main():
    os.makedirs(OUT, exist_ok=True)
    log = open(f"{OUT}/log.txt", "w")
    def say(m):
        line = time.strftime("[%H:%M:%S] ", time.gmtime()) + m
        print(line, flush=True); log.write(line + "\n"); log.flush()

    sh(f"oc create ns {NS}", check=False)
    ann = json.loads(sh(f"oc get ns {NS} -o json"))["metadata"]["annotations"]
    uid_start = int(ann["openshift.io/sa.scc.uid-range"].split("/")[0])
    sup_start = int(ann["openshift.io/sa.scc.supplemental-groups"].split("/")[0])
    say(f"namespace {NS}: uid range start {uid_start}, supplemental start {sup_start}")
    for prof, (scc, _, _) in PROFILES.items():
        sh(f"oc -n {NS} create sa {prof}", check=False)
        if scc != "restricted-v2":
            # The test's own app accounts only; nothing about the driver.
            sh(f"oc adm policy add-scc-to-user {scc} -z {prof} -n {NS}")

    classes = []
    for proto, base in BASE_CLASSES.items():
        src = json.loads(sh(f"oc get sc {base} -o json"))
        for mode in MODES:
            name = f"p4-{proto}-{mode}"
            params = dict(src["parameters"]); params["mountPermissions"] = mode
            apply({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                   "metadata": {"name": name, "labels": {"qcsi-test": "p4"}},
                   "provisioner": src["provisioner"], "parameters": params,
                   "mountOptions": src.get("mountOptions", []), "reclaimPolicy": "Delete",
                   "allowVolumeExpansion": True, "volumeBindingMode": "Immediate"})
            classes.append((proto, mode, name))
            apply({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                   "metadata": {"name": name, "namespace": NS},
                   "spec": {"accessModes": ["ReadWriteMany"], "storageClassName": name,
                            "resources": {"requests": {"storage": "1Gi"}}}})

    pods = []
    for proto, mode, cname in classes:
        for prof, (scc, psc, extra) in PROFILES.items():
            pod = f"{proto}-{mode}-{prof}"
            # anyuid allows no seccomp field at all; the others want RuntimeDefault.
            sc = {} if scc == "anyuid" else {"seccompProfile": {"type": "RuntimeDefault"}}
            for k, v in psc.items():
                # The first value of the project's range: restricted-v2's
                # MustRunAs accepts it (a value further in was refused).
                sc[k] = v if v is not None else ([sup_start] if k == "supplementalGroups" else sup_start)
            if scc in ("restricted-v2", "restricted-v3", "nonroot-v2"):
                sc["runAsNonRoot"] = True
            csc = {"allowPrivilegeEscalation": scc == "privileged", "capabilities": {"drop": ["ALL"]}}
            if scc == "privileged":
                csc = {"privileged": True}
            script = ("id; f=/data/w.$POD; "
                      "if echo hello > $f 2>/tmp/e; then echo WRITE=ok; cat $f >/dev/null && echo READ=ok; "
                      "stat -c 'OWNER=%u:%g MODE=%a' $f; else echo WRITE=fail; cat /tmp/e; fi; "
                      "stat -c 'DIR=%u:%g %a' /data; grep ' /data ' /proc/self/mountinfo | cut -d' ' -f5,6,9-11 | cut -c1-200; "
                      "cat /proc/self/attr/current 2>/dev/null; echo; echo DONE; sleep 300")
            spec = {"serviceAccountName": prof, "securityContext": sc, "restartPolicy": "Never",
                    "containers": [{"name": "app", "image": IMAGE, "command": ["sh", "-c", script],
                                    "env": [{"name": "POD", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}],
                                    "securityContext": csc, "volumeMounts": [{"name": "d", "mountPath": "/data"}]}],
                    "volumes": [{"name": "d", "persistentVolumeClaim": {"claimName": cname}}]}
            spec.update(extra)
            meta = {"name": pod, "namespace": NS, "labels": {"qcsi-test": "p4"},
                    "annotations": {"openshift.io/required-scc": scc}}
            r = subprocess.run("oc apply -f -", shell=True, capture_output=True, text=True,
                               input=yaml.safe_dump({"apiVersion": "v1", "kind": "Pod", "metadata": meta, "spec": spec}))
            pods.append((proto, mode, prof, scc, pod, r.returncode, r.stderr.strip()))

    say(f"{len(pods)} pods submitted; waiting for DONE")
    deadline = time.time() + 300
    results = []
    for proto, mode, prof, scc, pod, rc, err in pods:
        row = {"protocol": proto, "mode": mode, "profile": prof, "scc_requested": scc, "pod": pod}
        if rc:
            row.update(admitted=False, error=err[:300]); results.append(row); continue
        # At least one read per pod: a shared deadline used up by one pod that
        # never starts must not leave the later pods unread.
        while True:
            logs = sh(f"oc -n {NS} logs {pod}", check=False)
            if "DONE" in logs or time.time() > deadline:
                break
            why = sh(f"oc -n {NS} get pod {pod} -o jsonpath='{{.status.phase}} {{.status.containerStatuses[0].state.waiting.reason}}'", check=False)
            if why.startswith("Failed") or "CreateContainerError" in why:
                break
            time.sleep(5)
        got = json.loads(sh(f"oc -n {NS} get pod {pod} -o json", check=False) or "{}")
        row.update(admitted=True, scc=got.get("metadata", {}).get("annotations", {}).get("openshift.io/scc"),
                   started="DONE" in logs, write="WRITE=ok" in logs, read="READ=ok" in logs,
                   log=logs.strip().splitlines()[:8])
        if "DONE" not in logs:
            row["events"] = sh(f"oc -n {NS} get events --field-selector involvedObject.name={pod} "
                               "-o custom-columns=R:.reason,M:.message --no-headers", check=False)[-600:]
        results.append(row)

    verdicts = []
    for row in results:
        key = (row["protocol"], row["mode"], row["profile"])
        exp = EXPECT.get(key)
        ok_admit = row.get("admitted") and row.get("scc") == row["scc_requested"]
        if exp is None:
            v = "RECORDED"
        elif not ok_admit or not row.get("started"):
            v = "FAIL"
        else:
            v = "PASS" if row["write"] == exp else "FAIL"
        row["verdict"] = v; row["expected_write"] = exp
        verdicts.append(f"{v} {row['protocol']} {row['mode']} {row['profile']:13s} scc={row.get('scc')} "
                        f"write={row.get('write')} read={row.get('read')}")
    json.dump(results, open(f"{OUT}/results.json", "w"), indent=1)
    open(f"{OUT}/verdicts.txt", "w").write("\n".join(verdicts) + "\n")
    for v in verdicts:
        say(v)

    say("cleanup")
    sh(f"oc delete ns {NS} --wait=true --timeout=600s", check=False)
    for _, _, cname in classes:
        sh(f"oc delete sc {cname} --ignore-not-found", check=False)
    left = sh("oc get pv -o json", check=False)
    leftover = [p["metadata"]["name"] for p in json.loads(left)["items"]
                if p["spec"].get("storageClassName", "").startswith("p4-")]
    say(f"leftover PVs: {leftover or 'none'}")
    graded = [v for v in verdicts if not v.startswith("RECORDED")]
    say(f"graded {len(graded)}: {sum(v.startswith('PASS') for v in graded)} pass")
    sys.exit(1 if any(v.startswith("FAIL") for v in graded) else 0)


main()
