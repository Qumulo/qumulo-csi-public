#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# SAFETY: DESTRUCTIVE, NODE CHANGE -- scales NL_MACHINESET up by one (a new VM, billed while it exists), runs writer
# SAFETY: pods on it, drains it with `oc adm drain` (PDBs honoured, no --force), then scales the MachineSet back,
# SAFETY: deleting exactly the Machine it added. No node the cluster had before the run is drained, rebooted or
# SAFETY: removed. Namespace qcsi-nl-addrm and its claims are deleted afterwards; the MachineSet is restored to its
# SAFETY: starting size even if the script fails. Refuses unless I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1.
"""A worker added, then drained and removed, while applications run -- OpenShift 4.22.

Cases:

  NL-01  a worker joins through the Machine API. The node plugin must be Ready
         on it, registered (CSINode) and carrying the driver's topology label
         before a pod with an existing volume can land there -- the PV's
         nodeAffinity names that label. Then a pod on the new node mounts an
         existing RWX volume and reads what the old node wrote, a new claim
         provisions and mounts there, and the CSIStorageCapacity objects stay
         one per class. A writer on an old node sees none of it.
  NL-02  the same worker is drained and removed. RWX and RWO writers move to
         another node on their own; nothing of theirs stays mounted on the
         drained node; the Node and its CSINode go; no synced write is lost.

Timeline: writers up -> scale +1 (about 5 min on Azure) -> NL-01 checks ->
writers moved onto the new node -> drain -> scale back -> node gone. About
30 minutes.

Needs live.env with NL_MACHINESET (a worker MachineSet; scaled +1 and back).
Evidence: <RUN_DIR>/node-add-remove/.
"""
import os
import sys
import time

from node_lifecycle_lib import (
    TOPOLOGY_KEY, Check, add_node, app_pods, apply, capacity_counts, claim, classes, csinode_driver, drain, env_int,
    kubelet_volume_dirs, machineset_replicas, namespace, node_labels, oc, pv_mounts, pv_of, read_writer, remove_added_node,
    require_destructive, grade_writer, volumeattachments, wait_app_ready, wait_bound, wait_node, wait_plugin_ready,
    workers, writer_deployment,
)

MS = os.environ.get("NL_MACHINESET", "")
NS = "qcsi-nl-addrm"
JOIN_S = env_int("NL_NODE_JOIN_S", 1500)
PLUGIN_S = env_int("NL_PLUGIN_READY_S", 300)
RESCHEDULE_S = env_int("NL_RESCHEDULE_S", 300)
UNMOUNT_S = env_int("NL_UNMOUNT_S", 60)
DRAIN_S = env_int("NL_DRAIN_S", 900)
GONE_S = env_int("NL_NODE_GONE_S", 1200)
STALL_S = env_int("NL_STALL_S", 30)

require_destructive("adds a worker through the Machine API, drains it and removes it")
if not MS:
    sys.exit("NL_MACHINESET is not set: name a worker MachineSet this check may scale up by one and back")

C = Check("node-add-remove")


def hostname(node):
    return {"kubernetes.io/hostname": node}


def prefer(node, allowed):
    """Required: one of `allowed`. Preferred: `node`. So the pod starts on the
    new node and, once that node is drained, has somewhere to go."""
    term = {"key": "kubernetes.io/hostname", "operator": "In"}
    return {"nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchExpressions": [dict(term, values=sorted(allowed))]}]},
        "preferredDuringSchedulingIgnoredDuringExecution": [
            {"weight": 100, "preference": {"matchExpressions": [dict(term, values=[node])]}}],
    }}


def main():
    cls = classes()
    old = workers()
    if not old:
        sys.exit("no schedulable worker to run the undisturbed writer on")
    C.say("existing workers", old, "| MachineSet", MS)

    apply(namespace(NS))
    new, replicas = None, None
    try:
        for name, sc, mode in (("shared-rwx", cls["nfs41"], "ReadWriteMany"), ("move-rwx", cls["nfs41"], "ReadWriteMany"),
                               ("move-rwo", cls["nfs3"], "ReadWriteOnce"), ("fresh-rwo", cls["nfs3"], "ReadWriteOnce")):
            apply(claim(name, NS, sc, mode))
        for name in ("shared-rwx", "move-rwx", "move-rwo"):
            if not wait_bound(NS, name):
                C.record(False, "setup/%s-bound" % name, "claim never Bound; nothing below is meaningful")
                return C.summary()

        # The writer that must not notice anything: on an old worker for the whole run.
        apply(writer_deployment("w-old", NS, "shared-rwx", node_selector=hostname(old[0])))
        if not wait_app_ready(NS, "w-old", 300):
            C.record(False, "setup/w-old-running", "writer on %s never Ready" % old[0])
            return C.summary()
        caps = capacity_counts(cls.values())
        C.say("capacity objects before", caps)

        # ---- NL-01: add
        C.say("scaling", MS, "up by one")
        replicas = machineset_replicas(MS)      # known before the scale, so `finally` can restore it whatever fails
        new, secs, _ = add_node(MS, JOIN_S)
        if not new:
            C.record(False, "NL-01/node-joins", "no new Ready node within %ds" % JOIN_S)
            return C.summary()
        C.record(True, "NL-01/node-joins", "%s Ready after %.0fs" % (new, secs))

        took = wait_plugin_ready(new, PLUGIN_S)
        C.record(took is not None, "NL-01/plugin-ready-on-new-node",
                 "%s after node Ready (bar %ds)" % ("%.0fs" % took if took is not None else "never", PLUGIN_S))
        node_id = csinode_driver(new)
        C.record(bool(node_id), "NL-01/driver-registered", "CSINode %s nodeID=%r" % (new, node_id))
        label = node_labels(new).get(TOPOLOGY_KEY)
        C.record(bool(label), "NL-01/topology-label", "%s=%r" % (TOPOLOGY_KEY, label))

        # An existing volume, written from an old node, mounted on the new one.
        apply(writer_deployment("w-new", NS, "shared-rwx", node_selector=hostname(new)))
        got = wait_app_ready(NS, "w-new", RESCHEDULE_S, on=new)
        C.record(got is not None, "NL-01/existing-volume-mounts-on-new-node",
                 "pod %s" % ("Running after %.0fs" % got[2] if got else "not Ready in %ds" % RESCHEDULE_S))
        if got:
            _, stored, now = read_writer(NS, got[0], "w-old")
            C.record(bool(stored) and stored == now, "NL-01/new-node-reads-old-node-data",
                     "w-old reference sha256 stored=%s read on %s=%s" % (stored[:12], new, now[:12]))

        # A claim provisioned for, and first mounted on, the new node.
        fresh_ok = wait_bound(NS, "fresh-rwo")
        apply(writer_deployment("w-fresh", NS, "fresh-rwo", node_selector=hostname(new)))
        got = wait_app_ready(NS, "w-fresh", RESCHEDULE_S, on=new) if fresh_ok else None
        C.record(got is not None, "NL-01/new-claim-on-new-node",
                 "bound=%s pod %s" % (fresh_ok, "Running" if got else "not Ready in %ds" % RESCHEDULE_S))

        time.sleep(60)          # one capacity-poll period for a duplicate object to appear, if it is going to
        after = capacity_counts(cls.values())
        C.record(after == caps, "NL-01/capacity-objects-unchanged", "before=%s after=%s" % (caps, after))
        C.record(not volumeattachments(), "NL-01/no-volumeattachments", str(volumeattachments()))

        # ---- NL-02: drain and remove
        # The two NL-01 probes are pinned to the new node; they are removed rather than left Pending forever.
        for app in ("w-new", "w-fresh"):
            oc("delete deployment %s -n %s --wait=true --timeout=180s" % (app, NS), check=False)
        allowed = [new] + old
        apply(writer_deployment("w-move-rwx", NS, "move-rwx", affinity=prefer(new, allowed)))
        apply(writer_deployment("w-move-rwo", NS, "move-rwo", affinity=prefer(new, allowed)))
        for app in ("w-move-rwx", "w-move-rwo"):
            if not wait_app_ready(NS, app, RESCHEDULE_S, on=new):
                C.record(False, "setup/%s-on-new-node" % app,
                         "the pod did not start on %s (where: %s); NL-02 would prove nothing" % (new, app_pods(NS, app)))
                return C.summary()
        time.sleep(30)          # a few synced lines from the new node before it goes

        pvs = [pv_of(NS, c) for c in ("shared-rwx", "move-rwx", "move-rwo", "fresh-rwo")]
        C.say("draining", new)
        started = time.time()
        ok, secs, out = drain(new, DRAIN_S)
        C.record(ok, "NL-02/drain-completes", "%.0fs: %s" % (secs, out.strip().splitlines()[-1][:160] if out else ""))

        for app in ("w-move-rwx", "w-move-rwo"):
            got = wait_app_ready(NS, app, max(0, RESCHEDULE_S - (time.time() - started)) + 5, not_on=new)
            C.record(got is not None, "NL-02/%s-rescheduled" % app,
                     "Running on %s %.0fs after the drain started (bar %ds)" % (got[1], time.time() - started,
                                                                               RESCHEDULE_S)
                     if got else "no Ready pod off %s within %ds" % (new, RESCHEDULE_S))

        left, deadline = None, time.time() + UNMOUNT_S
        while time.time() < deadline:
            left = {pv: n for pv, n in pv_mounts(new, pvs).items() if n}
            if not left:
                break
            time.sleep(5)
        C.record(not left, "NL-02/no-mount-left-on-drained-node", "mounts of this check's PVs on %s: %s" % (new, left))
        dirs = kubelet_volume_dirs(new, pvs)
        C.record(not dirs, "NL-02/no-volume-dir-left-on-drained-node", str(dirs))
        stuck = [p for p in app_pods(NS, "w-move-rwx") + app_pods(NS, "w-move-rwo") if p[1] == new]
        C.record(not stuck, "NL-02/no-pod-left-on-drained-node", str(stuck))

        C.say("removing", new)
        remove_added_node(MS, new, replicas)
        gone = wait_node(new, None, GONE_S)
        C.record(gone is not None, "NL-02/node-removed", "%s" % ("%.0fs" % gone if gone is not None else "still there"))
        C.record(csinode_driver(new) is None, "NL-02/registration-removed", "CSINode %s" % new)
        after = capacity_counts(cls.values())
        C.record(after == caps, "NL-02/capacity-objects-unchanged", "before=%s after=%s" % (caps, after))
        new = None

        # ---- data, read through the pods that hold each volume now
        reader = wait_app_ready(NS, "w-old", 60)
        if reader:
            grade_writer(C, "NL-01", NS, reader[0], "w-old", stall_bar=STALL_S)
            grade_writer(C, "NL-01", NS, reader[0], "w-new")
        for app, rwo in (("w-move-rwx", False), ("w-move-rwo", True)):
            got = wait_app_ready(NS, app, 60)
            if got:
                grade_writer(C, "NL-02", NS, got[0], app, resume_bar=RESCHEDULE_S, single_writer=rwo)
    finally:
        if new and replicas is not None:
            C.say("restoring", MS, "to", replicas, "replicas")
            remove_added_node(MS, new, replicas)
        elif replicas is not None:
            remove_added_node(MS, None, replicas)
        C.say("cleanup")
        oc("delete ns %s --wait=false" % NS, check=False)

    return C.summary()


if __name__ == "__main__":
    sys.exit(main())
