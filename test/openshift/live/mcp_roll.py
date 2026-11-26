#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# SAFETY: DESTRUCTIVE, NODE CHANGE, LONG -- applies a MachineConfig (one marker file, /etc/qcsi-node-lifecycle) to the
# SAFETY: NL_MCP pool (default worker), which makes the Machine Config Operator drain and reboot every node in that
# SAFETY: pool one after another; then deletes it, which rolls the pool a second time. Every pod on every node of the
# SAFETY: pool moves twice. Namespace qcsi-nl-mcp is deleted afterwards. Refuses unless
# SAFETY: I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1 and NL_ALLOW_MCP_ROLL=1.
"""A MachineConfigPool rolling drain and reboot under running applications -- OpenShift 4.22.

Case (plan NL-09):

  NL-09  OpenShift's own node-maintenance path: a MachineConfig change makes the
         Machine Config Operator cordon, drain and reboot each node of a pool in
         turn -- what every z-stream upgrade does. The pool must finish
         updating without going Degraded (a drain the driver blocks degrades
         it), every node of the pool must have rebooted, and the writers --
         unpinned, so they move with each drain -- must lose no synced write
         and never stop for longer than NL_MCP_GAP_S. Graded for both rolls:
         applying the marker, and removing it again.

Two rolls of a two-worker pool take about 30-40 minutes on Azure; more nodes,
longer. Opt-in twice over (NL_ALLOW_MCP_ROLL=1) because of that length and
because a pool that degrades halfway needs an administrator.

Evidence: <RUN_DIR>/mcp-roll/.
"""
import os
import sys
import time

from node_lifecycle_lib import (
    Check, apply, boot_id, claim, classes, controller_pods, env_int, grade_writer, jget, namespace, oc,
    require_destructive, sh, wait_app_ready, wait_bound, writer_deployment,
)

NS = "qcsi-nl-mcp"
POOL = os.environ.get("NL_MCP", "worker")
MC = "99-qcsi-node-lifecycle-%s" % POOL
ROLL_S = env_int("NL_MCP_S", 5400)
GAP_S = env_int("NL_MCP_GAP_S", 300)

require_destructive("rolls a whole MachineConfigPool twice (drain and reboot of every node in it)")
if os.environ.get("NL_ALLOW_MCP_ROLL") != "1":
    sys.exit("refusing to run: NL_ALLOW_MCP_ROLL=1 is also required -- this check reboots every node of the %s pool "
             "twice and takes the better part of an hour" % POOL)

C = Check("mcp-roll")


def marker():
    return {
        "apiVersion": "machineconfiguration.openshift.io/v1",
        "kind": "MachineConfig",
        "metadata": {"name": MC, "labels": {"machineconfiguration.openshift.io/role": POOL}},
        "spec": {"config": {
            "ignition": {"version": "3.4.0"},
            "storage": {"files": [{"path": "/etc/qcsi-node-lifecycle", "mode": 420, "overwrite": True,
                                   "contents": {"source": "data:,qcsi-node-lifecycle-check"}}]},
        }},
    }


def pool_conditions():
    mcp = jget("get mcp %s" % POOL)
    return {c["type"]: (c["status"], c.get("message", "")) for c in mcp.get("status", {}).get("conditions", [])}


def pool_nodes():
    return sorted(n["metadata"]["name"] for n in jget("get nodes -l node-role.kubernetes.io/%s=" % POOL)["items"])


def roll(phase, change):
    nodes = pool_nodes()
    boots = {n: boot_id(n) for n in nodes}
    change()
    started = time.time()
    began = False
    degraded, min_ctrl = "", 99
    while time.time() - started < ROLL_S:
        cond = pool_conditions()
        if cond.get("Updating", ("False",))[0] == "True":
            began = True
        if cond.get("Degraded", ("False",))[0] == "True" or cond.get("NodeDegraded", ("False",))[0] == "True":
            degraded = cond.get("Degraded", ("", ""))[1] or cond.get("NodeDegraded", ("", ""))[1]
            break
        min_ctrl = min(min_ctrl, sum(1 for p in controller_pods() if p[2]))
        if began and cond.get("Updated", ("False",))[0] == "True":
            break
        if not began and time.time() - started > 600:
            break
        time.sleep(15)
    secs = time.time() - started
    cond = pool_conditions()
    C.record(began and cond.get("Updated", ("False",))[0] == "True", "NL-09/%s-pool-updated" % phase,
             "%.0fs, began=%s, Updated=%s" % (secs, began, cond.get("Updated")))
    C.record(not degraded, "NL-09/%s-pool-not-degraded" % phase, degraded[:200])
    rebooted = [n for n in nodes if boot_id(n) not in ("", boots[n])]
    C.record(set(rebooted) == set(nodes), "NL-09/%s-every-node-rebooted" % phase,
             "%d/%d: %s" % (len(rebooted), len(nodes), sorted(set(nodes) - set(rebooted))))
    C.record(min_ctrl >= 1, "NL-09/%s-controller-never-down" % phase,
             "fewest Ready controller replicas during the roll: %d" % min_ctrl)


def main():
    if not jget("get mcp %s" % POOL):
        sys.exit("no MachineConfigPool %s" % POOL)
    cond = pool_conditions()
    if cond.get("Updated", ("False",))[0] != "True" or cond.get("Degraded", ("False",))[0] == "True":
        sys.exit("pool %s is not settled before the run (%s); a roll on top of another proves nothing" % (POOL, cond))
    cls = classes()

    apply(namespace(NS))
    applied = False
    try:
        apply(claim("m-rwx", NS, cls["nfs41"], "ReadWriteMany"))
        apply(claim("m-rwo", NS, cls["nfs3"], "ReadWriteOnce"))
        for c in ("m-rwx", "m-rwo"):
            if not wait_bound(NS, c):
                C.record(False, "setup/%s-bound" % c, "claim never Bound")
                return C.summary()
        apply(writer_deployment("w-rwx", NS, "m-rwx"))
        apply(writer_deployment("w-rwo", NS, "m-rwo"))
        for app in ("w-rwx", "w-rwo"):
            if not wait_app_ready(NS, app, 300):
                C.record(False, "setup/%s-running" % app, "writer never Ready")
                return C.summary()
        time.sleep(30)

        C.say("applying", MC, "to pool", POOL)
        roll("apply", lambda: apply(marker()))
        applied = True
        C.say("removing", MC)
        roll("remove", lambda: oc("delete machineconfig %s" % MC))
        applied = False

        for app, rwo in (("w-rwx", False), ("w-rwo", True)):
            got = wait_app_ready(NS, app, 300)
            if got:
                grade_writer(C, "NL-09", NS, got[0], app, resume_bar=GAP_S, single_writer=rwo)
    finally:
        if applied or sh("oc get machineconfig %s -o name" % MC, check=False).strip():
            C.say("deleting", MC, "-- the pool rolls once more to remove it; watch `oc get mcp %s`" % POOL)
            oc("delete machineconfig %s" % MC, check=False)
        C.say("cleanup")
        oc("delete ns %s --wait=false" % NS, check=False)

    return C.summary()


if __name__ == "__main__":
    sys.exit(main())
