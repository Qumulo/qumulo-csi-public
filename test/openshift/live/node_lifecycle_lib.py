#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# SAFETY: this module performs no node change on import. The changes live in the scripts that use it.
"""Shared helpers for the node-lifecycle checks: nodes added, removed and
rebooted while applications are running.

Every check asks the same three questions of a cluster change, and only the
change differs:

* `data`     : did any write the application had already synced go missing?
               Each writer appends `<incarnation> <seq> <epoch>` to its own log
               and syncs every line, and keeps a reference file with its
               sha256 beside it. A hole in an incarnation's sequence, a line
               that is not a line, or a reference file whose hash moved is lost
               data. A hard reset may tear the one line that was in flight; it
               may not lose one that was synced.
* `resume`   : how long the application was stopped -- from the last line one
               incarnation wrote to the first line the next one wrote.
* `node`     : what the change left on the node and in the API -- mounts of the
               check's volumes on a node that no longer runs their pods, kubelet
               volume directories for pods that no longer exist, the plugin's
               registration (CSINode) and its topology label.

The driver sets attachRequired: false, so there is no VolumeAttachment to
detach or to leak: the node side of a change is mounts and registration, and
that is what is measured.

Evidence goes to <RUN_DIR>/<check>/: verdicts.txt, log.txt, and one
<writer>.log copy per writer.
"""
import json
import os
import re
import subprocess
import threading
import time

from outage_lib import APP_IMAGE, DRIVER_NS, Check, apply, namespace, node_shell, oc, require_destructive, sh
from outage_lib import jget as _jget

__all__ = [
    "Check", "apply", "jget", "namespace", "node_shell", "oc", "require_destructive", "sh",
    "DRIVER_NAME", "TOPOLOGY_KEY", "classes", "claim", "writer_deployment", "wait_bound", "pv_of", "app_pods",
    "wait_app_ready", "read_writer", "analyse_log", "grade_writer", "node_state", "wait_node", "boot_id",
    "plugin_pod", "wait_plugin_ready", "csinode_driver", "capacity_counts", "pv_mounts", "kubelet_volume_dirs",
    "controller_pods", "lease_holders", "leader_node", "schedule_power", "drain", "uncordon", "masters",
    "machine_of_node", "machineset_replicas", "scale_machineset", "wait_new_node", "ProvisioningLoop", "env_int",
    "workers", "annotate_machine", "add_node", "remove_added_node", "volumeattachments", "node_labels",
]

DRIVER_NAME = os.environ.get("DRIVER_NAME", "csi.qumulo.com")
# pkg/driver/topology.go topologyKeyPlugin: one value, set on every node that runs the node plugin.
TOPOLOGY_KEY = "topology.csi.qumulo.com/plugin"
MAPI = "openshift-machine-api"
REF_MB = int(os.environ.get("NL_REF_MB", "64"))


def jget(args, tries=12, pause=5):
    """outage_lib.jget, retried. These checks reboot nodes, a master among
    them, so the API answering an error for a few seconds is part of the run,
    not a reason to abandon it halfway with the cluster mid-change."""
    for attempt in range(tries):
        try:
            return _jget(args)
        except (RuntimeError, ValueError):
            if attempt == tries - 1:
                raise
            time.sleep(pause)


def env_int(name, default):
    return int(os.environ.get(name, str(default)))


def workers():
    """Schedulable worker node names, sorted, without the control plane.

    On a compact cluster the masters are also workers; they are left out here
    so a check never reboots or drains a control-plane node by accident --
    that is what controller_disruption.py's NL-08 does, on purpose, alone.
    """
    data = jget("get nodes -l node-role.kubernetes.io/worker=")
    out = []
    for n in data.get("items", []):
        labels = n["metadata"].get("labels", {})
        if n["spec"].get("unschedulable"):
            continue
        if "node-role.kubernetes.io/master" in labels or "node-role.kubernetes.io/control-plane" in labels:
            continue
        out.append(n["metadata"]["name"])
    return sorted(out)


def masters():
    data = jget("get nodes -l node-role.kubernetes.io/master=")
    return sorted(n["metadata"]["name"] for n in data.get("items", []))


def classes():
    """The StorageClasses the checks provision from: {protocol: class}.

    NL_CLASSES, or MATRIX_CLASSES as the app matrix uses. RWX claims go on the
    NFS 4.1 class and RWO claims on the NFS 3 class, so every check covers both
    protocols. The classes must allow the worker subnet -- a node added by the
    MachineSet lands in it -- and must give a folder an arbitrary-UID pod can
    write (directory mode 0777, I-12).
    """
    raw = os.environ.get("NL_CLASSES") or os.environ.get("MATRIX_CLASSES", "nfs41=qumulo-nfs41,nfs3=qumulo-nfs3")
    out = dict(p.split("=", 1) for p in raw.split(",") if "=" in p)
    if "nfs41" not in out or "nfs3" not in out:
        raise SystemExit("NL_CLASSES must name an nfs41 and an nfs3 class, e.g. nfs41=qumulo-nfs41,nfs3=qumulo-nfs3")
    for sc in out.values():
        mode = oc("get storageclass %s -o jsonpath={.volumeBindingMode}" % sc).strip()
        if mode == "WaitForFirstConsumer":
            # The checks bind each claim before they start its writer, so that a
            # provisioning failure is reported as one rather than as a pod that
            # never starts.
            raise SystemExit("StorageClass %s is WaitForFirstConsumer; the node-lifecycle checks need Immediate "
                             "classes in NL_CLASSES" % sc)
    return out


def claim(name, ns, storage_class, mode="ReadWriteMany", size="2Gi"):
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": ns},
        "spec": {"accessModes": [mode], "storageClassName": storage_class, "resources": {"requests": {"storage": size}}},
    }


# The writer. `trap` because sh as PID 1 ignores SIGTERM: without it every pod
# would write on through its whole grace period, and a drain would measure the
# grace period rather than the driver. A real application stops on SIGTERM.
WRITER_SCRIPT = r"""
set -u
trap 'echo "TERM $i"; exit 0' TERM
cd /data
INC="${HOSTNAME}-$(date +%s)"
REF="ref-${WRITER}.bin"
if [ ! -f "$REF" ]; then
  head -c "${REF_MB}M" /dev/urandom > "$REF.tmp" && sync "$REF.tmp" &&
  sha256sum "$REF.tmp" | cut -d' ' -f1 > "ref-${WRITER}.sha256.tmp" && sync "ref-${WRITER}.sha256.tmp" &&
  mv "ref-${WRITER}.sha256.tmp" "ref-${WRITER}.sha256" && mv "$REF.tmp" "$REF" && sync . ||
  { echo "IOERR ref $(date +%s)"; }
fi
touch /tmp/ready
i=0
while true; do
  i=$((i+1))
  if printf '%s %d %s\n' "$INC" "$i" "$(date +%s)" >> "seq-${WRITER}.log" 2>/tmp/err && sync "seq-${WRITER}.log" 2>>/tmp/err
  then :; else echo "IOERR $i $(date +%s) $(cat /tmp/err)"; fi
  sleep 1 & wait $!
done
"""


def writer_deployment(name, ns, claim_name, node_selector=None, affinity=None, grace=30):
    """A one-replica Deployment that writes to its claim once a second, syncing
    every line, under `restricted-v2` -- the SCC an ordinary OpenShift
    workload gets. A Deployment rather than a bare pod, because a node change
    is answered by the ReplicaSet re-creating the pod, and that path is part of
    what is graded. Recreate, so a rollout never runs two pods on an RWO claim
    (a drain is not a rollout; see grade_writer's overlap check).
    """
    spec = {
        "terminationGracePeriodSeconds": grace,
        "containers": [{
            "name": "writer",
            "image": APP_IMAGE,
            "command": ["/bin/sh", "-c", WRITER_SCRIPT],
            "env": [{"name": "WRITER", "value": name}, {"name": "REF_MB", "value": str(REF_MB)}],
            "readinessProbe": {"exec": {"command": ["test", "-f", "/tmp/ready"]}, "periodSeconds": 2},
            "volumeMounts": [{"name": "data", "mountPath": "/data"}],
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
                "runAsNonRoot": True,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
        }],
        "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": claim_name}}],
    }
    if node_selector:
        spec["nodeSelector"] = node_selector
    if affinity:
        spec["affinity"] = affinity
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": ns},
        "spec": {
            "replicas": 1,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": {"app": name}},
            "template": {"metadata": {"labels": {"app": name}}, "spec": spec},
        },
    }


def wait_bound(ns, name, timeout=300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if oc("get pvc %s -n %s -o jsonpath={.status.phase}" % (name, ns), check=False).strip() == "Bound":
            return True
        time.sleep(3)
    return False


def pv_of(ns, name):
    return oc("get pvc %s -n %s -o jsonpath={.spec.volumeName}" % (name, ns), check=False).strip()


def app_pods(ns, app):
    """[(name, node, ready, deleting, uid)] for the app's pods."""
    out = []
    for p in jget("get pods -n %s -l app=%s" % (ns, app)).get("items", []):
        conds = {c["type"]: c["status"] for c in p.get("status", {}).get("conditions", [])}
        out.append((p["metadata"]["name"], p["spec"].get("nodeName", ""), conds.get("Ready") == "True",
                    bool(p["metadata"].get("deletionTimestamp")), p["metadata"]["uid"]))
    return out


def wait_app_ready(ns, app, timeout=300, on=None, not_on=None):
    """Wait for one Ready, not-deleting pod of the app, optionally on (or off)
    a node. Returns (pod, node, seconds waited) or None."""
    started = time.time()
    while time.time() - started < timeout:
        for name, node, ready, deleting, _ in app_pods(ns, app):
            if ready and not deleting and (on is None or node == on) and (not_on is None or node != not_on):
                return name, node, time.time() - started
        time.sleep(3)
    return None


def read_writer(ns, pod, writer):
    """(sequence log text, stored sha256, computed sha256) read through a pod
    that mounts the writer's volume."""
    script = ("cat /data/seq-{w}.log; echo '@@SHA@@'; cat /data/ref-{w}.sha256 2>/dev/null; echo '@@NOW@@'; "
              "sha256sum /data/ref-{w}.bin 2>/dev/null | cut -d' ' -f1").format(w=writer)
    out = oc("exec -n %s %s -- sh -c %s" % (ns, pod, _quote(script)), check=False, timeout=600)
    log, _, rest = out.partition("@@SHA@@\n")
    stored, _, now = rest.partition("@@NOW@@\n")
    return log, stored.strip(), now.strip()


LINE = re.compile(r"^(\S+) (\d+) (\d+)$")


def analyse_log(text):
    """Incarnations, holes, bad lines and timing from a writer's sequence log.

    A hole is a sequence number missing inside one incarnation: a write the
    application had synced, gone. A bad line is anything that is not a whole
    record; `torn` counts the bad lines that sit at the end of an incarnation,
    which is the one line a hard reset is allowed to tear.
    """
    incs, order, bad, torn = {}, [], [], 0
    lines = text.splitlines()
    last_inc = None
    for idx, line in enumerate(lines):
        m = LINE.match(line)
        if not m:
            bad.append(line[:80])
            nxt = next((LINE.match(x) for x in lines[idx + 1:] if LINE.match(x)), None)
            if last_inc and (nxt is None or nxt.group(1) != last_inc):
                torn += 1
            continue
        inc, seq, ts = m.group(1), int(m.group(2)), int(m.group(3))
        if inc not in incs:
            incs[inc] = []
            order.append(inc)
        incs[inc].append((seq, ts))
        last_inc = inc
    holes, dups, stall = [], 0, 0
    for inc in order:
        seqs = [s for s, _ in incs[inc]]
        dups += len(seqs) - len(set(seqs))
        missing = sorted(set(range(1, max(seqs) + 1)) - set(seqs))
        holes += ["%s#%d" % (inc, s) for s in missing]
        ts = [t for _, t in incs[inc]]
        stall = max([stall] + [b - a for a, b in zip(ts, ts[1:])])
    gaps, overlaps = [], []
    for a, b in zip(order, order[1:]):
        a_last, b_first = incs[a][-1][1], incs[b][0][1]
        gaps.append(b_first - a_last)
        if b_first < a_last:
            overlaps.append("%s started at %d, before %s stopped at %d" % (b, b_first, a, a_last))
    return {"incarnations": order, "holes": holes, "dups": dups, "bad": bad, "torn": torn, "gaps": gaps,
            "max_gap": max(gaps) if gaps else 0, "stall": stall, "overlaps": overlaps,
            "lines": sum(len(v) for v in incs.values())}


def grade_writer(C, case, ns, pod, writer, hard=False, resume_bar=None, stall_bar=None, single_writer=False):
    """Read one writer's volume through `pod` and record its data verdicts.

    Returns the analysis so a check can add its own bars. The log is copied
    into the evidence directory either way.
    """
    log, stored, now = read_writer(ns, pod, writer)
    with open(os.path.join(C.out, "%s-%s.log" % (case, writer)), "w") as fh:
        fh.write(log)
    a = analyse_log(log)
    C.record(bool(stored) and stored == now, "%s/%s-reference-intact" % (case, writer),
             "sha256 stored=%s now=%s" % (stored[:12], now[:12]))
    C.record(not a["holes"] and not a["dups"], "%s/%s-no-synced-write-lost" % (case, writer),
             "%d lines, %d incarnations, holes=%s dups=%d" % (a["lines"], len(a["incarnations"]), a["holes"][:5],
                                                               a["dups"]))
    allowed = a["torn"] if hard else 0
    C.record(len(a["bad"]) <= allowed, "%s/%s-no-garbage" % (case, writer),
             "bad lines=%d (torn at a reset=%d, allowed=%d) %s" % (len(a["bad"]), a["torn"], allowed, a["bad"][:2]))
    if resume_bar is not None:
        C.record(len(a["incarnations"]) >= 1 and a["max_gap"] <= resume_bar, "%s/%s-resumed" % (case, writer),
                 "longest stop %ds (bar %ds), gaps %s" % (a["max_gap"], resume_bar, a["gaps"]))
    if stall_bar is not None:
        C.record(a["stall"] <= stall_bar, "%s/%s-never-stalled" % (case, writer),
                 "longest pause inside one incarnation %ds (bar %ds)" % (a["stall"], stall_bar))
    if single_writer:
        # RWO: the driver has no ControllerPublish, so nothing but the kubelet
        # stops a replacement pod mounting an RWO claim on another node while
        # the old pod is still terminating. Two incarnations writing at once
        # is the result. ReadWriteOncePod is what prevents it.
        C.record(not a["overlaps"], "%s/%s-rwo-one-writer-at-a-time" % (case, writer), "; ".join(a["overlaps"][:2]))
    return a


def _quote(s):
    return "'" + s.replace("'", "'\"'\"'") + "'"


# --------------------------------------------------------------------------------------------- nodes
def node_state(name):
    """'Ready', 'NotReady', or None when the Node object is gone.

    Gone means the API said NotFound. Any other failure -- an API blip while a
    master reboots -- reads as NotReady, so a wait for removal never ends on a
    timeout that only looked like a deletion.
    """
    r = subprocess.run("oc get node %s -o json" % name, shell=True, capture_output=True, text=True)
    if r.returncode:
        return None if "NotFound" in r.stderr else "NotReady"
    conds = {c["type"]: c["status"] for c in json.loads(r.stdout).get("status", {}).get("conditions", [])}
    return "Ready" if conds.get("Ready") == "True" else "NotReady"


def wait_node(name, want, timeout):
    """Wait for 'Ready', 'NotReady' or None (gone). Returns seconds or None."""
    started = time.time()
    while time.time() - started < timeout:
        if node_state(name) == want:
            return time.time() - started
        time.sleep(5)
    return None


def node_labels(name):
    return jget("get node %s" % name)["metadata"].get("labels", {})


def boot_id(name):
    return oc("get node %s -o jsonpath={.status.nodeInfo.bootID}" % name, check=False).strip()


def plugin_pod(node):
    """(name, ready, restarts) of the driver's node pod on that node, or None."""
    data = jget("get pods -n %s -l app.kubernetes.io/component=node --field-selector spec.nodeName=%s"
                % (DRIVER_NS, node))
    for p in data.get("items", []):
        if p["metadata"].get("deletionTimestamp"):
            continue
        conds = {c["type"]: c["status"] for c in p.get("status", {}).get("conditions", [])}
        restarts = sum(c.get("restartCount", 0) for c in p.get("status", {}).get("containerStatuses", []))
        return p["metadata"]["name"], conds.get("Ready") == "True", restarts
    return None


def wait_plugin_ready(node, timeout):
    started = time.time()
    while time.time() - started < timeout:
        p = plugin_pod(node)
        if p and p[1]:
            return time.time() - started
        time.sleep(5)
    return None


def csinode_driver(node):
    """The nodeID this driver registered on the node's CSINode, or None (no
    CSINode, or this driver not on it)."""
    raw = sh("oc get csinode %s -o json 2>/dev/null" % node, check=False)
    data = json.loads(raw) if raw.strip() else {}
    for d in (data.get("spec") or {}).get("drivers") or []:
        if d.get("name") == DRIVER_NAME:
            return d.get("nodeID") or ""
    return None


def capacity_counts(class_names):
    """CSIStorageCapacity objects per class. The driver's topology is one
    segment, so this is one per class and must not grow with the node count."""
    counts = {c: 0 for c in class_names}
    for o in jget("get csistoragecapacities -A").get("items", []):
        if o.get("storageClassName") in counts:
            counts[o["storageClassName"]] += 1
    return counts


def pv_mounts(node, pvs):
    """{pv: number of kubelet target mounts of it on the node}."""
    out = node_shell(node, "cat /proc/mounts")
    return {pv: sum(1 for line in out.splitlines() if "/volumes/kubernetes.io~csi/%s/mount " % pv in line)
            for pv in pvs}


def kubelet_volume_dirs(node, pvs):
    """[(pod uid, pv)] for every kubelet volume directory of these PVs on the node."""
    pattern = "|".join(re.escape(p) for p in pvs) or "^$"
    out = node_shell(node, "ls -d /var/lib/kubelet/pods/*/volumes/kubernetes.io~csi/* 2>/dev/null; true")
    hits = []
    for line in out.splitlines():
        m = re.match(r"/var/lib/kubelet/pods/([^/]+)/volumes/kubernetes\.io~csi/(.+)$", line.strip())
        if m and re.fullmatch(pattern, m.group(2)):
            hits.append((m.group(1), m.group(2)))
    return hits


def schedule_power(node, action, hard=False, delay=5):
    """Reboot or power off a node a few seconds from now, through the host's systemd.

    systemd-run, not a backgrounded shell: `oc debug` deletes its pod when the
    session ends, and a `nohup sleep; reboot` inside it dies with the pod. The
    transient timer belongs to the host. `--force --force` skips the orderly
    shutdown -- no unit stops, no unmount -- which is as close to pulling the
    power as a guest can get without the cloud API.
    """
    assert action in ("reboot", "poweroff")
    cmd = "systemctl %s%s" % (action, " --force --force" if hard else "")
    return node_shell(node, "systemd-run --on-active=%ds --unit=qcsi-nl-%s-%d %s && echo scheduled"
                      % (delay, action, int(time.time()), cmd))


def drain(node, timeout):
    """`oc adm drain` the way an administrator does it: honouring every PDB
    (no --disable-eviction) and never --force, so a pod the drain cannot evict
    stops it rather than being deleted. Returns (ok, seconds, output).

    Judged by the exit status and the closing "drained" line, not by the word
    "error" in between: a drain held back by a PDB prints "error when evicting
    pods ... will retry" and then finishes, and that is the behaviour NL-03 is
    there to pass.
    """
    started = time.time()
    try:
        r = subprocess.run("oc adm drain %s --ignore-daemonsets --delete-emptydir-data --timeout=%ds" % (node, timeout),
                           shell=True, capture_output=True, text=True, timeout=timeout + 60)
        out, code = r.stdout + r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        out, code = "oc adm drain outlived its own --timeout", -1
    last = [line for line in out.splitlines() if line.strip()][-1:] or [""]
    return code == 0 and "drained" in last[0], time.time() - started, out


def uncordon(node):
    oc("adm uncordon %s" % node, check=False)


# --------------------------------------------------------------------------------------------- controller
def controller_pods():
    """[(name, node, ready)] of the controller Deployment's pods."""
    out = []
    for p in jget("get pods -n %s -l app.kubernetes.io/component=controller" % DRIVER_NS).get("items", []):
        if p["metadata"].get("deletionTimestamp"):
            continue
        conds = {c["type"]: c["status"] for c in p.get("status", {}).get("conditions", [])}
        out.append((p["metadata"]["name"], p["spec"].get("nodeName", ""), conds.get("Ready") == "True"))
    return out


def lease_holders():
    """{lease: holder pod} for the sidecars' leader-election Leases. Their
    holderIdentity is the holding pod's name."""
    pods = {p[0] for p in controller_pods()}
    out = {}
    for lease in jget("get leases -n %s" % DRIVER_NS).get("items", []):
        holder = (lease.get("spec") or {}).get("holderIdentity") or ""
        for pod in pods:
            if holder.startswith(pod):
                out[lease["metadata"]["name"]] = pod
    return out


def leader_node():
    """(pod, node) holding the csi-provisioner lease -- the one CreateVolume
    waits on -- or (None, None). csi-provisioner names its Lease after the
    driver with dots made dashes (csi-qumulo-com); the resizer's and the
    snapshotter's carry their own prefixes."""
    holders = lease_holders()
    nodes = {p[0]: p[1] for p in controller_pods()}
    pod = holders.get(DRIVER_NAME.replace(".", "-"))
    return (pod, nodes.get(pod)) if pod else (None, None)


# --------------------------------------------------------------------------------------------- machine API
def machine_of_node(node):
    ref = oc("get node %s -o jsonpath={.metadata.annotations.machine\\.openshift\\.io/machine}" % node,
             check=False).strip()
    return ref.split("/", 1)[1] if "/" in ref else ""


def machineset_replicas(ms):
    return int(oc("get machineset %s -n %s -o jsonpath={.spec.replicas}" % (ms, MAPI)).strip() or 0)


def scale_machineset(ms, n):
    oc("scale machineset %s -n %s --replicas=%d" % (ms, MAPI, n))


def annotate_machine(machine, key, value=""):
    oc("annotate machine %s -n %s %s=%s --overwrite" % (machine, MAPI, key, value))


def add_node(ms, timeout):
    """Scale the MachineSet up by one and wait for the new node to be Ready.

    Returns (node, seconds, replicas before). A node the check adds is a node
    the check may remove, so no check ever takes away a worker the cluster had
    before it started.
    """
    before_replicas = machineset_replicas(ms)
    before = {n["metadata"]["name"] for n in jget("get nodes").get("items", [])}
    scale_machineset(ms, before_replicas + 1)
    node, secs = wait_new_node(before, timeout)
    return node, secs, before_replicas


def remove_added_node(ms, node, replicas, skip_drain=False):
    """Give the MachineSet back its starting size, deleting exactly the
    check's node: delete-machine makes the scale-down pick that Machine, and
    exclude-node-draining makes the Machine API skip its drain (a powered-off
    node cannot be drained)."""
    machine = machine_of_node(node) if node else ""
    if machine:
        annotate_machine(machine, "machine.openshift.io/delete-machine", "true")
        if skip_drain:
            annotate_machine(machine, "machine.openshift.io/exclude-node-draining", "")
    if machineset_replicas(ms) != replicas:
        scale_machineset(ms, replicas)


def volumeattachments():
    """VolumeAttachments naming this driver. attachRequired is false, so the
    right number is always zero, before and after any node change."""
    return [v["metadata"]["name"] for v in jget("get volumeattachments").get("items", [])
            if v["spec"].get("attacher") == DRIVER_NAME]


def wait_new_node(before, timeout):
    """The first node that was not in `before` to report Ready. Returns
    (name, seconds) or (None, None)."""
    started = time.time()
    while time.time() - started < timeout:
        now = {n["metadata"]["name"] for n in jget("get nodes").get("items", [])}
        for n in sorted(now - before):
            if node_state(n) == "Ready":
                return n, time.time() - started
        time.sleep(10)
    return None, None


# --------------------------------------------------------------------------------------------- provisioning
class ProvisioningLoop(threading.Thread):
    """Claims created, bound, expanded and deleted in a loop while a change is
    under way -- the controller's half of the question. Each round's outcome is
    kept; nothing is retried by the loop itself, because "the driver recovered
    on its own" is what is being measured.
    """

    def __init__(self, ns, storage_class, rounds, bind_s=300, prefix="op"):
        super().__init__(daemon=True)
        self.ns, self.sc, self.rounds, self.bind_s, self.prefix = ns, storage_class, rounds, bind_s, prefix
        self.results = []          # (round, ok, seconds, detail)
        self.stop = threading.Event()

    def run(self):
        for i in range(self.rounds):
            if self.stop.is_set():
                break
            name = "%s-%d" % (self.prefix, i)
            t0 = time.time()
            try:
                apply(claim(name, self.ns, self.sc, size="1Gi"))
                if not wait_bound(self.ns, name, self.bind_s):
                    self.results.append((i, False, time.time() - t0, "not Bound in %ds" % self.bind_s))
                    continue
                oc("patch pvc %s -n %s -p '{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"2Gi\"}}}}'"
                   % (name, self.ns))
                pv = pv_of(self.ns, name)
                grown = False
                deadline = time.time() + self.bind_s
                while time.time() < deadline:
                    if oc("get pv %s -o jsonpath={.spec.capacity.storage}" % pv, check=False).strip() == "2Gi":
                        grown = True
                        break
                    time.sleep(3)
                oc("delete pvc %s -n %s --wait=false" % (name, self.ns), check=False)
                gone = False
                deadline = time.time() + self.bind_s
                while time.time() < deadline:
                    if not sh("oc get pv %s -o name" % pv, check=False).strip():
                        gone = True
                        break
                    time.sleep(3)
                self.results.append((i, grown and gone, time.time() - t0,
                                     "expanded=%s pv-deleted=%s pv=%s" % (grown, gone, pv)))
            except Exception as e:  # noqa: BLE001 -- one bad round is a result, not the end of the loop
                self.results.append((i, False, time.time() - t0, str(e)[:200]))
