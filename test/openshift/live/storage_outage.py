#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# SAFETY: DESTRUCTIVE, NODE CHANGE -- drops TCP 2049 from one worker to SPARE_FLOATING_IP for OUTAGE_S seconds
# SAFETY: (self-removing rule; refused if that worker already mounts the address), then deletes a pod into the
# SAFETY: forced-unmount ladder during the outage. Runs a read-only sampler through `oc debug node`. No kubelet
# SAFETY: restart, no host umount. Namespace qcsi-outage and its class and claims are deleted afterwards; a
# SAFETY: safety net removes the rule even if the script dies. Refuses unless I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1.
"""Clean application shutdown when the Qumulo array is unreachable -- OpenShift 4.22, blackhole fault.

The OpenShift port of the Kubernetes hung-NFS node check. Same shape, same
fault, same measurements; `oc debug node --profile=sysadmin`
instead of a privileged debug pod, an SCC-labelled namespace for the workload
pods, and the nft/iptables question settled at run time rather than assumed.

Cases:

  O-2  a pod whose volume is on the unreachable address is deleted mid-outage
       and must terminate on its own, inside its grace period, with no --force,
       leaving no mount behind on the worker.
  O-3  a second pod writing to a different volume on the SAME NFS superblock
       must see no I/O error while that happens. This is the critical case.
  O-5  node-plugin threads, zombies and D-state tasks stay bounded during the
       outage and return to baseline after it.

Timeline: baseline 120 s -> two writers up -> blackhole -> +30 s a third pod on
a new claim (never mounts; the kubelet retries) -> +150 s delete writer A ->
rule lifts -> 300 s recovery. About 20 minutes.

Needs live.env (see live.env.example) with SPARE_FLOATING_IP, OUTAGE_SC_* and
the usual array settings. Evidence: <RUN_DIR>/storage-outage/.
"""
import os
import sys
import time

from outage_lib import (
    Check, apply, delete_and_time, io_errors, jget, mounts_through, namespace, node_shell, oc,
    plugin_resources, pvc, require_destructive, wait_running, workers, writer_pod,
)

FIP = os.environ.get("SPARE_FLOATING_IP", "")
API_URL = os.environ.get("API_URL", "")
TOKEN_SECRET = os.environ.get("TOKEN_SECRET", "qumulo-api-token")
DRIVER_NS = os.environ.get("DRIVER_NS", "qumulo-csi")
BASE_PATH = os.environ.get("BASE_PATH", "/csi-ocp/volumes")
NS = "qcsi-outage"
SC = "qcsi-outage-fip"
GRACE = int(os.environ.get("OUTAGE_GRACE_S", "120"))
OUTAGE_S = int(os.environ.get("OUTAGE_S", "600"))
BASE_S = int(os.environ.get("OUTAGE_BASELINE_S", "120"))
RECOVER_S = int(os.environ.get("OUTAGE_RECOVER_S", "300"))
SAMPLE_S = 15

require_destructive("blackholes NFS from a worker to SPARE_FLOATING_IP and drives a pod into the forced-unmount ladder")

if not FIP:
    sys.exit("SPARE_FLOATING_IP is not set. This check must not be pointed at the array address the rest of the\n"
             "cluster uses: the fault would take every volume on every worker away, not just this test's.")

C = Check("storage-outage")


def firewall_backend(node):
    """RHCOS 4.22 runs nftables. The iptables command may be the nft shim, or
    may not be there at all -- which is the single most likely bring-up
    blocker, so it is settled here and reported rather than assumed."""
    out = node_shell(node, "command -v iptables >/dev/null && iptables --version || echo none")
    if "nf_tables" in out:
        return "iptables-nft"
    if "legacy" in out:
        return "iptables-legacy"
    if node_shell(node, "command -v nft >/dev/null && echo yes || echo no").strip().endswith("yes"):
        return "nft"
    return None


def drop_rule(node, backend, seconds):
    """Install a self-removing rule blackholing NFS to the spare address.

    Self-removing because a script that dies between adding and removing must
    not leave a worker permanently unable to reach the array. The shell is
    detached, so the rule outlives this `oc debug` session and lifts on its own.
    """
    if backend.startswith("iptables"):
        add = "iptables -I OUTPUT -d %s -p tcp --dport 2049 -j DROP" % FIP
        rm = "iptables -D OUTPUT -d %s -p tcp --dport 2049 -j DROP" % FIP
    else:
        add = "nft add rule inet filter output ip daddr %s tcp dport 2049 drop" % FIP
        rm = "nft flush chain inet filter output"
    node_shell(node, "%s && (nohup sh -c 'sleep %d; %s' >/dev/null 2>&1 &) ; true" % (add, seconds, rm))
    return rm


def lift(node, rm):
    """Safety net. Removing a rule that is already gone is not an error here."""
    node_shell(node, "%s 2>/dev/null; true" % rm)


def main():
    nodes = workers()
    if len(nodes) < 2:
        sys.exit("need at least two schedulable workers: one takes the fault, one stays clean")
    target, clean = nodes[0], nodes[1]
    C.say("fault worker", target, "| control worker", clean)

    existing = mounts_through(target, FIP)
    if existing:
        sys.exit("worker %s already mounts %s (%s). Blackholing it would break volumes this check does not own."
                 % (target, FIP, existing))

    backend = firewall_backend(target)
    if not backend:
        sys.exit("no iptables or nft on %s -- the blackhole fault cannot be installed. Settle this before the run;"
                 " the array power-off check (storage_outage_poweroff.py) needs no node firewall." % target)
    C.say("firewall backend on", target, "is", backend)

    apply(namespace(NS))
    apply({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "StorageClass",
        "metadata": {"name": SC},
        "provisioner": "csi.qumulo.com",
        "parameters": {
            "server": FIP,
            "apiUrl": API_URL,
            "storeRealPath": BASE_PATH,
            "protocol": "nfs4",
            "csi.storage.k8s.io/provisioner-secret-name": TOKEN_SECRET,
            "csi.storage.k8s.io/provisioner-secret-namespace": DRIVER_NS,
            "csi.storage.k8s.io/controller-expand-secret-name": TOKEN_SECRET,
            "csi.storage.k8s.io/controller-expand-secret-namespace": DRIVER_NS,
        },
        "reclaimPolicy": "Delete",
        "volumeBindingMode": "Immediate",
    })

    rm = None
    try:
        for claim in ("outage-a", "outage-b"):
            apply(pvc(claim, NS, SC))
        apply(writer_pod("writer-a", NS, "outage-a", target, grace=GRACE))
        apply(writer_pod("bystander-b", NS, "outage-b", target, grace=GRACE))

        for pod in ("writer-a", "bystander-b"):
            if not wait_running(NS, pod):
                C.record(False, "setup/%s-running" % pod, "pod never reached Running; nothing below is meaningful")
                return C.summary()
        C.record(True, "setup/pods-running", "writer and bystander are up on %s" % target)

        # Both volumes must actually share one superblock, or O-3 proves nothing.
        shared = mounts_through(target, FIP)
        C.record(len(shared) >= 2, "setup/two-mounts-on-the-fault-address", str(shared))

        C.say("baseline for", BASE_S, "s")
        base = plugin_resources(target)
        C.sample(["t", "phase", "threads", "zombies", "dstate", "rss"])
        C.sample([0, "baseline", base.get("threads"), base.get("zombies"), base.get("dstate"), base.get("rss")])
        time.sleep(BASE_S)

        C.say("blackholing", FIP, "from", target, "for", OUTAGE_S, "s")
        rm = drop_rule(target, backend, OUTAGE_S)
        outage_started = time.time()

        time.sleep(30)
        # A claim whose first mount meets the dead server: the kubelet retries,
        # and the pod must recover on its own once the rule lifts.
        apply(pvc("outage-c", NS, SC))
        apply(writer_pod("late-c", NS, "outage-c", target, grace=GRACE))

        while time.time() - outage_started < 150:
            r = plugin_resources(target)
            C.sample([int(time.time() - outage_started), "outage", r.get("threads"), r.get("zombies"),
                      r.get("dstate"), r.get("rss")])
            time.sleep(SAMPLE_S)

        bystander_before = io_errors(NS, "bystander-b")

        C.say("deleting writer-a into the forced-unmount ladder (grace %ds, no --force)" % GRACE)
        took = delete_and_time(NS, "writer-a", timeout=GRACE + 180)

        # O-2: it must go on its own, inside the grace period.
        if took is None:
            C.record(False, "O-2/pod-terminates", "writer-a outlived %ds and was still in the API" % (GRACE + 180))
        else:
            C.record(took <= GRACE, "O-2/pod-terminates", "%.0fs (grace %ds)" % (took, GRACE))

        # O-2: and the node must be clean afterwards.
        left = [m for m in mounts_through(target, FIP) if "outage-a" in m or "pvc-" in m]
        C.record(not mounts_through(target, FIP) or "writer-a" not in " ".join(left),
                 "O-2/no-stale-mount", "mounts through %s now: %s" % (FIP, left))

        # O-3: the bystander on the same superblock saw nothing.
        bystander_after = io_errors(NS, "bystander-b")
        C.record(bystander_after == bystander_before, "O-3/bystander-has-no-io-error",
                 "IOERR lines before=%d after=%d" % (bystander_before, bystander_after))

        # O-5: resources during.
        during = plugin_resources(target)
        C.record(during.get("zombies", 0) == 0, "O-5/no-zombies-during", str(during))
        C.record(during.get("threads", 0) <= base.get("threads", 0) + 64, "O-5/threads-bounded-during",
                 "baseline=%s during=%s" % (base.get("threads"), during.get("threads")))

        remaining = OUTAGE_S - (time.time() - outage_started)
        if remaining > 0:
            C.say("waiting out the rest of the outage:", int(remaining), "s")
            time.sleep(remaining)
        lift(target, rm)
        rm = None

        C.say("recovery window:", RECOVER_S, "s")
        time.sleep(RECOVER_S)

        after = plugin_resources(target)
        C.sample([int(time.time() - outage_started), "recovered", after.get("threads"), after.get("zombies"),
                  after.get("dstate"), after.get("rss")])
        C.record(after.get("zombies", 0) == 0, "O-5/no-zombies-after", str(after))
        C.record(after.get("threads", 0) <= base.get("threads", 0) + 8, "O-5/threads-return-to-baseline",
                 "baseline=%s after=%s" % (base.get("threads"), after.get("threads")))
        C.record(after.get("dstate", 0) == 0, "O-5/no-d-state-tasks", str(after))

        # The pod that never managed its first mount must now be running.
        C.record(wait_running(NS, "late-c", timeout=300), "O-2/late-pod-recovers",
                 "a pod whose first mount met the dead server came up after recovery")

        # O-7, in miniature: the worker is still usable.
        C.record("Ready" in oc("get node %s --no-headers" % target, check=False), "O-7/worker-ready", target)

    finally:
        if rm:
            lift(target, rm)
        C.say("cleanup")
        oc("delete ns %s --wait=false" % NS, check=False)
        oc("delete storageclass %s" % SC, check=False)

    return C.summary()


if __name__ == "__main__":
    sys.exit(main())
