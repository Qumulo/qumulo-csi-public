#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# SAFETY: DESTRUCTIVE, NODE CHANGE -- reboots one worker (NL_REBOOT_NODE, default the first worker) once per mode in
# SAFETY: NL_REBOOT_MODES: `graceful` is `systemctl reboot`, `hard` is `systemctl reboot --force --force` (no unit
# SAFETY: stops, no unmount -- a power cut as far as the guest can make one). Every pod on that worker restarts.
# SAFETY: Never a control-plane node. Namespace qcsi-nl-reboot and its claims are deleted afterwards. Refuses unless
# SAFETY: I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1.
"""A worker rebooted under running applications -- OpenShift 4.22.

Cases:

  NL-05  graceful reboot. The node comes back, the node plugin is Ready and
         registered again, and the writers pinned to that node run again with
         exactly one mount per volume and no kubelet volume directory left for
         a pod that no longer exists. No synced write is lost and the reference
         files are intact. A writer on another node sharing the RWX volume
         never stalls.
  NL-06  the same with a hard reset. The one line a writer had in flight may be
         torn; a synced line may not be lost.

The pinned writers are the point: after a reboot the kubelet must mount their
volumes again on the same node, through a node plugin that has itself only just
started. A writer rescheduled elsewhere would test the scheduler, not that.

About 15 minutes per mode on Azure. Evidence: <RUN_DIR>/node-reboot/.
"""
import os
import sys
import time

from node_lifecycle_lib import (
    Check, app_pods, apply, boot_id, claim, classes, csinode_driver, env_int, grade_writer, kubelet_volume_dirs,
    masters, namespace, oc, plugin_pod, pv_mounts, pv_of, require_destructive, schedule_power, wait_app_ready,
    wait_bound, wait_plugin_ready, workers, writer_deployment, node_state,
)

NS = "qcsi-nl-reboot"
MODES = [m.strip() for m in os.environ.get("NL_REBOOT_MODES", "graceful,hard").split(",") if m.strip()]
READY_S = env_int("NL_REBOOT_READY_S", 900)
PLUGIN_S = env_int("NL_PLUGIN_READY_S", 300)
RESUME_S = env_int("NL_RESUME_S", 180)
STALL_S = env_int("NL_STALL_S", 30)

require_destructive("reboots a worker under running writer pods")
if set(MODES) - {"graceful", "hard"}:
    sys.exit("NL_REBOOT_MODES may hold only graceful and hard, got %s" % MODES)

C = Check("node-reboot")


def hostname(node):
    return {"kubernetes.io/hostname": node}


def wait_rebooted(node, before, timeout):
    """Seconds until the node reports a new bootID and Ready, or None. The
    bootID is what proves a reboot happened; NotReady alone can be missed by a
    fast hard reset between two polls."""
    started = time.time()
    while time.time() - started < timeout:
        bid = boot_id(node)
        if bid and bid != before and node_state(node) == "Ready":
            return time.time() - started
        time.sleep(5)
    return None


def main():
    nodes = workers()
    target = os.environ.get("NL_REBOOT_NODE") or (nodes[0] if nodes else "")
    if not target or target in masters():
        sys.exit("NL_REBOOT_NODE must be a worker (got %r); control-plane reboots are controller_disruption.py's NL-08"
                 % target)
    others = [n for n in nodes if n != target]
    if not others:
        sys.exit("need a second schedulable worker for the bystander writer")
    cls = classes()
    C.say("rebooting", target, "| bystander on", others[0], "| modes", MODES)

    apply(namespace(NS))
    try:
        apply(claim("r-rwx", NS, cls["nfs41"], "ReadWriteMany"))
        apply(claim("r-rwo", NS, cls["nfs3"], "ReadWriteOnce"))
        for c in ("r-rwx", "r-rwo"):
            if not wait_bound(NS, c):
                C.record(False, "setup/%s-bound" % c, "claim never Bound")
                return C.summary()
        pvs = [pv_of(NS, "r-rwx"), pv_of(NS, "r-rwo")]
        apply(writer_deployment("w-rwx", NS, "r-rwx", node_selector=hostname(target)))
        apply(writer_deployment("w-rwo", NS, "r-rwo", node_selector=hostname(target)))
        apply(writer_deployment("w-by", NS, "r-rwx", node_selector=hostname(others[0])))
        for app in ("w-rwx", "w-rwo", "w-by"):
            if not wait_app_ready(NS, app, 300):
                C.record(False, "setup/%s-running" % app, "writer never Ready")
                return C.summary()
        time.sleep(30)

        for mode in MODES:
            case = "NL-05" if mode == "graceful" else "NL-06"
            hard = mode == "hard"
            before = boot_id(target)
            C.say(case, mode, "reboot of", target)
            schedule_power(target, "reboot", hard=hard)
            back = wait_rebooted(target, before, READY_S)
            C.record(back is not None, "%s/node-back" % case,
                     "new bootID and Ready after %s (bar %ds)" % ("%.0fs" % back if back else "never", READY_S))
            if back is None:
                break
            took = wait_plugin_ready(target, PLUGIN_S)
            C.record(took is not None, "%s/plugin-ready" % case,
                     "%s after node Ready, pod %s" % ("%.0fs" % took if took is not None else "never", plugin_pod(target)))
            C.record(bool(csinode_driver(target)), "%s/driver-registered" % case, "CSINode %s" % target)

            for app in ("w-rwx", "w-rwo"):
                got = wait_app_ready(NS, app, RESUME_S, on=target)
                C.record(got is not None, "%s/%s-running-again" % (case, app),
                         "%s (bar %ds after node Ready)" % ("%.0fs" % got[2] if got else "not Ready", RESUME_S))

            time.sleep(20)      # let the kubelet finish any cleanup it is going to do on its own
            mounts = pv_mounts(target, pvs)
            C.record(all(n == 1 for n in mounts.values()), "%s/one-mount-per-volume" % case, str(mounts))
            live = {p[4] for app in ("w-rwx", "w-rwo") for p in app_pods(NS, app) if p[1] == target}
            stale = [d for d in kubelet_volume_dirs(target, pvs) if d[0] not in live]
            C.record(not stale, "%s/no-stale-volume-dir" % case, "dirs for pods that no longer exist: %s" % stale)

            # The longest stop a pinned writer may show is the reboot itself plus the resume bar.
            bar = int(back) + RESUME_S
            for app, rwo in (("w-rwx", False), ("w-rwo", True)):
                got = wait_app_ready(NS, app, 60, on=target)
                if got:
                    grade_writer(C, case, NS, got[0], app, hard=hard, resume_bar=bar, single_writer=rwo)
            by = wait_app_ready(NS, "w-by", 60)
            if by:
                grade_writer(C, case, NS, by[0], "w-by", stall_bar=STALL_S)
            time.sleep(30)
    finally:
        C.say("cleanup")
        oc("delete ns %s --wait=false" % NS, check=False)

    return C.summary()


if __name__ == "__main__":
    sys.exit(main())
