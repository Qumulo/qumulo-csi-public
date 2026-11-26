#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# SAFETY: DESTRUCTIVE, NODE CHANGE -- scales NL_MACHINESET up by one, runs writer pods on the new node, then powers
# SAFETY: that node off with `systemctl poweroff --force --force` (no drain, no unmount), applies the out-of-service
# SAFETY: taint to it, and removes its Machine with draining skipped. Only the node this check added is lost; no node
# SAFETY: the cluster had before the run is touched. The MachineSet is restored to its starting size even if the
# SAFETY: script fails. Namespace qcsi-nl-loss is deleted afterwards. Refuses unless I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1.
"""A worker lost without warning while applications run -- OpenShift 4.22.

Case (plan NL-04):

  NL-04  a worker running RWX and RWO writers is powered off: no drain, no
         unmount, nothing the driver is told about. After the node is NotReady
         the administrator applies `node.kubernetes.io/out-of-service`, the
         Kubernetes non-graceful node shutdown path (GA since 1.28), which
         deletes the pods the dead kubelet can no longer stop. The writers must
         come back on another node within NL_LOSS_RESUME_S of that taint with
         no synced write lost (the in-flight line may be torn), a writer on
         another node sharing the RWX volume must never stall, and once the
         Machine is gone the Node, its CSINode and its pods must be gone too.

The driver publishes no VolumeAttachment (attachRequired: false), so there is
nothing for the out-of-service path to detach: what is graded is that the
replacement pods mount on another node and that nothing of the lost node's is
left in the API.

NL_OUT_OF_SERVICE=0 skips the taint and waits out the default 300 s
tolerations instead; NL_LOSS_RESUME_S must then allow for that.

About 25 minutes. Evidence: <RUN_DIR>/node-loss/.
"""
import os
import sys
import time

from node_lifecycle_lib import (
    Check, add_node, app_pods, apply, claim, classes, csinode_driver, env_int, grade_writer, jget,
    machineset_replicas, namespace, oc, remove_added_node, require_destructive, schedule_power, wait_app_ready,
    wait_bound, wait_node, workers, writer_deployment,
)

MS = os.environ.get("NL_MACHINESET", "")
NS = "qcsi-nl-loss"
JOIN_S = env_int("NL_NODE_JOIN_S", 1500)
NOTREADY_S = env_int("NL_NOTREADY_S", 300)
RESUME_S = env_int("NL_LOSS_RESUME_S", 600)
GONE_S = env_int("NL_NODE_GONE_S", 1200)
STALL_S = env_int("NL_STALL_S", 30)
TAINT = os.environ.get("NL_OUT_OF_SERVICE", "1") == "1"
OOS = "node.kubernetes.io/out-of-service=nodeshutdown:NoExecute"

require_destructive("adds a worker and powers it off under running writers")
if not MS:
    sys.exit("NL_MACHINESET is not set: name a worker MachineSet this check may scale up by one and back")

C = Check("node-loss")


def affinity(new, allowed):
    term = {"key": "kubernetes.io/hostname", "operator": "In"}
    return {"nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchExpressions": [dict(term, values=sorted(allowed))]}]},
        "preferredDuringSchedulingIgnoredDuringExecution": [
            {"weight": 100, "preference": {"matchExpressions": [dict(term, values=[new])]}}],
    }}


def main():
    cls = classes()
    old = workers()
    if not old:
        sys.exit("no schedulable worker for the bystander and the replacement pods")

    apply(namespace(NS))
    new, replicas = None, None
    try:
        apply(claim("l-rwx", NS, cls["nfs41"], "ReadWriteMany"))
        apply(claim("l-rwo", NS, cls["nfs3"], "ReadWriteOnce"))
        for c in ("l-rwx", "l-rwo"):
            if not wait_bound(NS, c):
                C.record(False, "setup/%s-bound" % c, "claim never Bound")
                return C.summary()
        apply(writer_deployment("w-by", NS, "l-rwx", node_selector={"kubernetes.io/hostname": old[0]}))

        replicas = machineset_replicas(MS)
        new, secs, _ = add_node(MS, JOIN_S)
        if not new:
            C.record(False, "setup/node-joins", "no new Ready node within %ds" % JOIN_S)
            return C.summary()
        C.say("added", new, "after %.0fs" % secs)

        allowed = [new] + old
        apply(writer_deployment("w-rwx", NS, "l-rwx", affinity=affinity(new, allowed)))
        apply(writer_deployment("w-rwo", NS, "l-rwo", affinity=affinity(new, allowed)))
        for app in ("w-rwx", "w-rwo"):
            if not wait_app_ready(NS, app, 300, on=new):
                C.record(False, "setup/%s-on-new-node" % app, "did not start on %s: %s" % (new, app_pods(NS, app)))
                return C.summary()
        if not wait_app_ready(NS, "w-by", 300):
            C.record(False, "setup/w-by-running", "bystander never Ready")
            return C.summary()
        time.sleep(60)

        C.say("powering", new, "off -- no drain, no unmount")
        schedule_power(new, "poweroff", hard=True)
        lost_at = time.time()
        nr = wait_node(new, "NotReady", NOTREADY_S)
        C.record(nr is not None, "NL-04/node-goes-notready", "%s" % ("%.0fs" % nr if nr is not None else "still Ready"))
        if nr is None:
            return C.summary()

        if TAINT:
            oc("adm taint node %s %s --overwrite" % (new, OOS))
            C.say("applied", OOS)
        from_t = time.time()
        for app in ("w-rwx", "w-rwo"):
            got = wait_app_ready(NS, app, RESUME_S, not_on=new)
            C.record(got is not None, "NL-04/%s-replaced" % app,
                     "Running on %s %.0fs after the %s (bar %ds)" % (got[1], time.time() - from_t,
                                                                      "taint" if TAINT else "NotReady", RESUME_S)
                     if got else "no Ready pod off %s within %ds" % (new, RESUME_S))

        C.say("removing the lost node's Machine, draining skipped")
        remove_added_node(MS, new, replicas, skip_drain=True)
        gone = wait_node(new, None, GONE_S)
        C.record(gone is not None, "NL-04/lost-node-removed", "%s" % ("%.0fs" % gone if gone is not None else "still there"))
        C.record(csinode_driver(new) is None, "NL-04/registration-removed", "CSINode %s" % new)
        left = [p["metadata"]["name"] for p in jget("get pods -n %s" % NS)["items"] if p["spec"].get("nodeName") == new]
        C.record(not left, "NL-04/no-pod-left-on-lost-node", str(left))
        lost = new
        new = None

        # The stop a writer may show: the time to NotReady (from the power-off) plus the resume bar.
        bar = int(from_t - lost_at) + RESUME_S
        for app, rwo in (("w-rwx", False), ("w-rwo", True)):
            got = wait_app_ready(NS, app, 60, not_on=lost)
            if got:
                grade_writer(C, "NL-04", NS, got[0], app, hard=True, resume_bar=bar, single_writer=rwo)
        by = wait_app_ready(NS, "w-by", 60)
        if by:
            grade_writer(C, "NL-04", NS, by[0], "w-by", stall_bar=STALL_S)
    finally:
        if replicas is not None:
            remove_added_node(MS, new, replicas, skip_drain=True)
        C.say("cleanup")
        oc("delete ns %s --wait=false" % NS, check=False)

    return C.summary()


if __name__ == "__main__":
    sys.exit(main())
