#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# SAFETY: DESTRUCTIVE -- requires the Qumulo array under ARRAY to be powered off by the operator while this runs,
# SAFETY: and deletes application pods into the forced-unmount ladder during that outage. No node change, no
# SAFETY: firewall rule, no kubelet restart: this script touches only its own namespace, class and claims, which
# SAFETY: it deletes afterwards. Refuses unless I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1.
"""Clean application shutdown when the array is powered off -- OpenShift 4.22 (plan O-1).

The headline number. A blackhole rule hangs the connection; a powered-off array
is the real outage the fix was measured against on Kubernetes (pods terminated
on their own in 25-81 s, no stale mounts). This reproduces that measurement on
OpenShift, across the four NFS mount flavours that each behave differently when
the server stops answering -- `hard`, `softerr`, `softreval` and
`fatal_neterrors` -- because before the fix every one of them outlived its
grace period.

It cannot power the array off itself, and deliberately does not try: it prompts,
waits for the operator to confirm the array is down, and prompts again to bring
it back. That keeps the dangerous step in a human's hands and keeps this script
free of lab-specific power control.

Evidence: <RUN_DIR>/storage-outage-poweroff/, including shutdown.tsv -- one row
per pod, which is the distribution the release gate reads.
"""
import os
import sys
import time

from outage_lib import (
    Check, apply, delete_and_time, mounts_through, namespace, oc, plugin_resources, pvc,
    require_destructive, wait_running, workers, writer_pod,
)

ARRAY = os.environ.get("ARRAY", "")
API_URL = os.environ.get("API_URL", "")
TOKEN_SECRET = os.environ.get("TOKEN_SECRET", "qumulo-api-token")
DRIVER_NS = os.environ.get("DRIVER_NS", "qumulo-csi")
BASE_PATH = os.environ.get("BASE_PATH", "/csi-ocp/volumes")
NS = "qcsi-outage-poweroff"
GRACE = int(os.environ.get("OUTAGE_GRACE_S", "120"))

# The four flavours, and why each is here. A `hard` mount retries forever and is
# the default; the three soft variants were each proposed at some point as "the
# fix" for hanging pods, and none of them worked -- which is why the driver
# unmounts rather than relying on mount options.
FLAVOURS = {
    "hard": ["hard", "timeo=600", "retrans=2"],
    "softerr": ["softerr", "timeo=50", "retrans=2"],
    "softreval": ["soft", "softreval", "timeo=50", "retrans=2"],
    "fatal": ["hard", "fatal_neterrors=enotconn", "timeo=50", "retrans=2"],
}

require_destructive("drives application pods into the forced-unmount ladder while the array is powered off")

if not ARRAY:
    sys.exit("ARRAY is not set (see live.env.example)")

C = Check("storage-outage-poweroff")


def confirm(prompt):
    """Block until the operator confirms the lab step. Not a yes/no default:
    getting this wrong means measuring a healthy array and reporting a pass."""
    if os.environ.get("OUTAGE_ASSUME_YES") == "1":
        C.say("OUTAGE_ASSUME_YES=1 --", prompt, "assumed done")
        return
    reply = input("\n>>> %s\n>>> type 'confirmed' to continue: " % prompt).strip()
    if reply != "confirmed":
        sys.exit("not confirmed -- stopping before anything is measured")


def main():
    nodes = workers()
    if not nodes:
        sys.exit("no schedulable workers")
    target = nodes[0]
    C.say("worker under test:", target)

    apply(namespace(NS))

    classes = {}
    for name, opts in FLAVOURS.items():
        sc = "qcsi-outage-%s" % name
        classes[name] = sc
        apply({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": {"name": sc},
            "provisioner": "csi.qumulo.com",
            "parameters": {
                "server": ARRAY,
                "apiUrl": API_URL,
                "storeRealPath": BASE_PATH,
                "protocol": "nfs4",
                "csi.storage.k8s.io/provisioner-secret-name": TOKEN_SECRET,
                "csi.storage.k8s.io/provisioner-secret-namespace": DRIVER_NS,
            },
            "mountOptions": opts,
            "reclaimPolicy": "Delete",
            "volumeBindingMode": "Immediate",
        })

    try:
        for name, sc in classes.items():
            apply(pvc("po-%s" % name, NS, sc))
            apply(writer_pod("po-%s" % name, NS, "po-%s" % name, target, grace=GRACE))

        up = True
        for name in classes:
            ok = wait_running(NS, "po-%s" % name)
            C.record(ok, "setup/%s-running" % name, "" if ok else "never reached Running")
            up = up and ok
        if not up:
            return C.summary()

        before = mounts_through(target, ARRAY)
        C.record(len(before) >= len(classes), "setup/all-flavours-mounted", str(before))
        base = plugin_resources(target)
        C.say("baseline resources:", base)

        confirm("POWER OFF the Qumulo array at %s now, and wait until it stops answering." % ARRAY)
        outage_started = time.time()
        C.say("outage begins; letting the mounts notice for 60 s")
        time.sleep(60)

        with open(os.path.join(C.out, "shutdown.tsv"), "a") as fh:
            fh.write("flavour\tseconds\tgrace\tforced\n")

        results = {}
        for name in classes:
            pod = "po-%s" % name
            C.say("deleting", pod, "(no --force)")
            took = delete_and_time(NS, pod, timeout=GRACE + 180)
            results[name] = took
            with open(os.path.join(C.out, "shutdown.tsv"), "a") as fh:
                fh.write("%s\t%s\t%d\tno\n" % (name, "timeout" if took is None else "%.0f" % took, GRACE))

            if took is None:
                C.record(False, "O-1/%s-terminates" % name,
                         "outlived %ds -- this is the pre-fix behaviour" % (GRACE + 180))
            else:
                C.record(took <= GRACE, "O-1/%s-terminates" % name, "%.0fs (grace %ds)" % (took, GRACE))

        # The node must be clean: this is the half that force-deleting a pod
        # never achieves, and the reason the ladder ends in `umount -l`.
        left = mounts_through(target, ARRAY)
        C.record(not left, "O-1/no-stale-mounts", "still mounted through %s: %s" % (ARRAY, left or "none"))

        during = plugin_resources(target)
        C.record(during.get("zombies", 0) == 0, "O-5/no-zombies-during-poweroff", str(during))

        confirm("POWER THE ARRAY BACK ON at %s, and wait until it answers." % ARRAY)
        C.say("recovery: 300 s")
        time.sleep(300)

        after = plugin_resources(target)
        C.record(after.get("threads", 0) <= base.get("threads", 0) + 8, "O-5/threads-return-to-baseline",
                 "baseline=%s after=%s" % (base.get("threads"), after.get("threads")))
        C.record(after.get("dstate", 0) == 0, "O-5/no-d-state-tasks", str(after))

        # O-7: ordinary provisioning still works on the same worker.
        apply(pvc("po-after", NS, classes["hard"]))
        apply(writer_pod("po-after", NS, "po-after", target, grace=30))
        C.record(wait_running(NS, "po-after", timeout=300), "O-7/provisioning-works-after-the-storm",
                 "a fresh claim and pod on the worker that took the outage")

        C.say("shutdown times:", {k: (None if v is None else round(v)) for k, v in results.items()})

    finally:
        C.say("cleanup")
        oc("delete ns %s --wait=false" % NS, check=False)
        for sc in classes.values():
            oc("delete storageclass %s" % sc, check=False)

    return C.summary()


if __name__ == "__main__":
    sys.exit(main())
