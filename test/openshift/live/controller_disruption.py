#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# SAFETY: DESTRUCTIVE, NODE CHANGE -- reboots the node running the controller leader (NL-07), drains the node running
# SAFETY: the most controller replicas with `oc adm drain` (PDBs honoured, no --force) and uncordons it afterwards
# SAFETY: (NL-03), and, unless NL_MASTER_REBOOT=0, reboots one control-plane node gracefully (NL-08; the API stays up
# SAFETY: on the other two). Every pod on those nodes restarts or moves. Namespace qcsi-nl-ctrl and its claims are
# SAFETY: deleted afterwards. Refuses unless I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1.
"""The controller's nodes changed while volumes are being provisioned -- OpenShift 4.22.

Cases:

  NL-07  the node holding the csi-provisioner lease is rebooted while claims
         are created, expanded and deleted. Leadership moves to the standby
         replica within NL_LEADER_S, every operation finishes without anyone
         touching it, no PV is left behind, and the controller is back to its
         full replica count once the node returns.
  NL-03  the node running the most controller replicas is drained. The PDB
         (maxUnavailable 1) must let the drain finish -- one replica at a time
         -- rather than block it, both replicas end up Ready elsewhere, and
         claims made during the drain still bind. The chart's anti-affinity is
         only preferred (INS-01), so both replicas can share a node; the
         detail line says which situation was tested.
  NL-08  a control-plane node is rebooted. The driver is not on the path, but
         its sidecars renew leases through that API server and its node plugin
         runs there too: operations continue, the plugin on that master comes
         back Ready, and the controller ends at full strength.

A writer runs throughout and is graded for data only: it is unpinned, so it may
be moved by the drain or the reboot, and that move is not this check's subject.

About 40 minutes. Evidence: <RUN_DIR>/controller-disruption/.
"""
import collections
import os
import sys
import time

from node_lifecycle_lib import (
    Check, ProvisioningLoop, apply, boot_id, claim, classes, controller_pods, drain, env_int, grade_writer, jget,
    leader_node, masters, namespace, node_state, oc, require_destructive, schedule_power, uncordon, wait_app_ready,
    wait_bound, wait_plugin_ready, writer_deployment, DRIVER_NS,
)

NS = "qcsi-nl-ctrl"
LEADER_S = env_int("NL_LEADER_S", 120)
READY_S = env_int("NL_REBOOT_READY_S", 900)
DRAIN_S = env_int("NL_DRAIN_S", 900)
RESCHEDULE_S = env_int("NL_RESCHEDULE_S", 300)
PLUGIN_S = env_int("NL_PLUGIN_READY_S", 300)
OPS = env_int("NL_OPS", 8)
RESUME_S = env_int("NL_CTRL_RESUME_S", 900)
MASTER = os.environ.get("NL_MASTER_REBOOT", "1") == "1"

require_destructive("reboots the controller leader's node, drains a controller node and reboots a master")

C = Check("controller-disruption")


def replicas():
    return int(oc("get deploy -n %s -l app.kubernetes.io/component=controller -o jsonpath={.items[0].spec.replicas}"
                  % DRIVER_NS).strip() or 0)


def wait_controllers(want, timeout, not_on=None):
    started = time.time()
    while time.time() - started < timeout:
        ready = [p for p in controller_pods() if p[2] and (not_on is None or p[1] != not_on)]
        if len(ready) >= want:
            return time.time() - started, ready
        time.sleep(5)
    return None, controller_pods()


def wait_rebooted(node, before, timeout):
    started = time.time()
    while time.time() - started < timeout:
        bid = boot_id(node)
        if bid and bid != before and node_state(node) == "Ready":
            return time.time() - started
        time.sleep(5)
    return None


def grade_ops(case, loop):
    loop.join(timeout=loop.rounds * 900)
    bad = [r for r in loop.results if not r[1]]
    slow = max([r[2] for r in loop.results] or [0])
    C.record(len(loop.results) == loop.rounds and not bad, "%s/every-operation-completes" % case,
             "%d/%d rounds ok, slowest %.0fs, failed: %s" % (len(loop.results) - len(bad), loop.rounds, slow, bad[:3]))
    left = [pv["metadata"]["name"] for pv in jget("get pv")["items"]
            if (pv["spec"].get("claimRef") or {}).get("namespace") == NS
            and (pv["spec"]["claimRef"].get("name") or "").startswith(loop.prefix)]
    C.record(not left, "%s/no-pv-left" % case, str(left))


def main():
    cls = classes()
    want = replicas()
    if want < 2:
        sys.exit("controller.replicaCount is %d: with one replica there is no standby to take the leases, and this "
                 "check would only measure the outage" % want)
    took, ready = wait_controllers(want, 120)
    if took is None:
        sys.exit("the controller is not at full strength before the run (%s); fix that first" % ready)

    apply(namespace(NS))
    try:
        apply(claim("c-rwx", NS, cls["nfs41"], "ReadWriteMany"))
        if not wait_bound(NS, "c-rwx"):
            C.record(False, "setup/c-rwx-bound", "claim never Bound")
            return C.summary()
        apply(writer_deployment("w-ctrl", NS, "c-rwx"))
        if not wait_app_ready(NS, "w-ctrl", 300):
            C.record(False, "setup/w-ctrl-running", "writer never Ready")
            return C.summary()

        # ---- NL-07: reboot the leader's node mid-provisioning
        pod, node = leader_node()
        if not pod:
            C.record(False, "NL-07/leader-found", "no Lease in %s is held by a controller pod" % DRIVER_NS)
        else:
            C.say("leader", pod, "on", node)
            loop = ProvisioningLoop(NS, cls["nfs41"], OPS, prefix="reboot")
            loop.start()
            time.sleep(20)
            before = boot_id(node)
            schedule_power(node, "reboot")
            started = time.time()
            moved = None
            while time.time() - started < LEADER_S + 60:
                p2, n2 = leader_node()
                if p2 and p2 != pod:
                    moved = (time.time() - started, p2, n2)
                    break
                time.sleep(3)
            C.record(moved is not None and moved[0] <= LEADER_S, "NL-07/leader-moves",
                     "to %s on %s after %.0fs (bar %ds)" % (moved[1], moved[2], moved[0], LEADER_S) if moved
                     else "no new leader within %ds" % (LEADER_S + 60))
            grade_ops("NL-07", loop)
            back = wait_rebooted(node, before, READY_S)
            C.record(back is not None, "NL-07/node-back", "%s" % ("%.0fs" % back if back else "never"))
            took, ready = wait_controllers(want, RESCHEDULE_S)
            C.record(took is not None, "NL-07/controller-full-strength", "%d/%d Ready: %s" % (len(ready), want, ready))

        # ---- NL-03: drain the node with the most controller replicas
        per_node = collections.Counter(p[1] for p in controller_pods())
        target, count = per_node.most_common(1)[0]
        if target in masters():
            C.say("controller replicas are on a master (%s); NL-03 drains it anyway, as an upgrade would" % target)
        C.say("draining", target, "which runs", count, "of", want, "controller replicas")
        loop = ProvisioningLoop(NS, cls["nfs41"], 3, prefix="drain")
        loop.start()
        try:
            ok, secs, out = drain(target, DRAIN_S)
            C.record(ok, "NL-03/drain-completes-with-pdb",
                     "%s replica(s) on %s, %.0fs: %s" % (count, target, secs,
                                                         out.strip().splitlines()[-1][:160] if out else ""))
            took, ready = wait_controllers(want, RESCHEDULE_S, not_on=target)
            C.record(took is not None, "NL-03/controllers-ready-elsewhere", "%d/%d off %s: %s" % (
                len(ready), want, target, ready))
            p2, n2 = leader_node()
            C.record(bool(p2) and n2 != target, "NL-03/leader-off-drained-node", "%s on %s" % (p2, n2))
            grade_ops("NL-03", loop)
        finally:
            uncordon(target)

        # ---- NL-08: reboot a control-plane node
        if MASTER:
            m = os.environ.get("NL_MASTER") or masters()[0]
            C.say("rebooting control-plane node", m)
            loop = ProvisioningLoop(NS, cls["nfs41"], 3, prefix="master")
            loop.start()
            before = boot_id(m)
            schedule_power(m, "reboot")
            back = wait_rebooted(m, before, READY_S)
            C.record(back is not None, "NL-08/master-back", "%s %s" % (m, "%.0fs" % back if back else "never"))
            took = wait_plugin_ready(m, PLUGIN_S) if back else None
            C.record(took is not None, "NL-08/plugin-ready-on-master", "%s" % ("%.0fs" % took if took is not None
                                                                               else "never"))
            grade_ops("NL-08", loop)
            took, ready = wait_controllers(want, RESCHEDULE_S)
            C.record(took is not None, "NL-08/controller-full-strength", "%d/%d Ready" % (len(ready), want))

        got = wait_app_ready(NS, "w-ctrl", 300)
        if got:
            grade_writer(C, "NL-07", NS, got[0], "w-ctrl", hard=False, resume_bar=RESUME_S)
    finally:
        C.say("cleanup")
        oc("delete ns %s --wait=false" % NS, check=False)

    return C.summary()


if __name__ == "__main__":
    sys.exit(main())
