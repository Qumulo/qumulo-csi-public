#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# SAFETY: DESTRUCTIVE, TWO ARRAYS -- creates a throwaway replication relationship between the array behind DR_CLASS
# SAFETY: and the peer named by the DR_VRC_AB class, promotes the copy on the peer, and leaves that relationship's
# SAFETY: ends and the replica directory on the peer array to be removed by hand. It never reads or changes a
# SAFETY: standing relationship. Namespace qcsi-dr-failover and its claims are deleted afterwards; the mirrored PV is
# SAFETY: Retain, so its directory on the peer stays. Refuses unless I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1.
"""What a customer does after a disaster-recovery failover -- OpenShift 4.22.

Cases (plan DR-01 to DR-04):

  DR-01  an application mounts the recovered copy. A writer pod on the promoted
         claim reaches Ready and writes, on a worker chosen for the network the
         *application* servers sit on rather than the control plane's.
  DR-02  how long recovery took. The writer's own log is the clock: the gap
         between the last line it synced on the old primary and the first line
         the new incarnation syncs on the promoted copy.
  DR-03  the recovered data is the data. Every line the application had synced
         before the failover is on the promoted copy, in order, with no hole,
         and the 64 MiB reference file's sha256 is the one that was stored
         beside it.
  DR-04  the old primary is frozen (not gating). A writer on the demoted claim
         should not be able to append to it.

Why a single writer across both claims: `WRITER` names the file, not the pod,
so the promoted copy carries the log the old primary wrote. One `analyse_log`
over it then answers all three gating cases at once -- the incarnation boundary
is the failover, the gap across it is the recovery time, and a hole anywhere in
it is a synced write replication did not carry.

**This check is also the live gate for the multi-network replica export.** Before the fix the driver staged
the replica's export with only the first entry of the class's allowed-clients
list, so no pod could mount the promoted copy at all. A single-network cluster
cannot tell the fixed driver from the broken one: DR_CLASS must name at least
two networks and DR_APP_NODE must sit on one that is *not* first in that list.
The check refuses to grade DR-01 without both, and says so.

About 40 minutes. Evidence: <RUN_DIR>/dr-failover/.
"""
import json
import os
import subprocess
import sys
import time

from node_lifecycle_lib import (
    Check, apply, env_int, grade_writer, jget, namespace, oc, pv_of, require_destructive, wait_app_ready,
    wait_bound, workers, writer_deployment,
)

NS = "qcsi-dr-failover"
WRITER = "dr"                       # the file name both incarnations append to
CLAIM = "dr-data"
DR_CLAIM = CLAIM + "-dr"            # what `qumulo-csi-repoint mirror -claim-suffix -dr` makes

CLASS = os.environ.get("DR_CLASS", "")
VRC_AB = os.environ.get("DR_VRC_AB", "")
VRC_BA = os.environ.get("DR_VRC_BA", "")
APP_NODE = os.environ.get("DR_APP_NODE", "")
REPOINT = os.environ.get("DR_REPOINT", "go run ./cmd/qumulo-csi-repoint")
REPO = os.path.expanduser(os.environ.get("DR_REPO", os.path.join(os.path.dirname(__file__), "..", "..", "..")))
PEER_SERVER = os.environ.get("DR_PEER_SERVER", "")
PEER_API_URL = os.environ.get("DR_PEER_API_URL", "")
PEER_TOKEN_FILE = os.path.expanduser(os.environ.get("DR_PEER_TOKEN_FILE", ""))
PEER_SECRET = os.environ.get("DR_PEER_SECRET", "qumulo-tokens-dr")
SECRET_NS = os.environ.get("DRIVER_NS", "qumulo-csi")

SYNC_S = env_int("DR_SYNC_S", 600)          # Enable to Primary with a recovery point
PROMOTE_S = env_int("DR_PROMOTE_S", 300)    # each state change to the state it asked for
MOUNT_S = env_int("DR_MOUNT_S", 300)        # promoted claim to a Ready writer on it
RTO_S = env_int("DR_RTO_S", 600)            # the DR-02 bar: longest stop the application may show
WRITE_S = env_int("DR_WRITE_S", 90)         # how long each incarnation writes before it is judged

require_destructive("creates and promotes a throwaway replication relationship across two Qumulo arrays")
for name, value in (("DR_CLASS", CLASS), ("DR_VRC_AB", VRC_AB), ("DR_VRC_BA", VRC_BA),
                    ("DR_PEER_SERVER", PEER_SERVER), ("DR_PEER_API_URL", PEER_API_URL),
                    ("DR_PEER_TOKEN_FILE", PEER_TOKEN_FILE)):
    if not value:
        sys.exit("%s must be set -- see live.env.example" % name)
if not os.path.isfile(PEER_TOKEN_FILE):
    sys.exit("DR_PEER_TOKEN_FILE %r does not exist" % PEER_TOKEN_FILE)

C = Check("dr-failover")


def vr(name, claim_name, vrc, state):
    return {
        "apiVersion": "replication.storage.openshift.io/v1alpha1",
        "kind": "VolumeReplication",
        "metadata": {"name": name, "namespace": NS},
        "spec": {
            "volumeReplicationClass": vrc,
            "replicationState": state,
            "autoResync": True,
            "dataSource": {"kind": "PersistentVolumeClaim", "name": claim_name},
        },
    }


def vr_status(name):
    s = jget("get volumereplication %s -n %s" % (name, NS)).get("status", {})
    return s.get("state"), s.get("lastSyncTime"), (s.get("message") or "")[:200]


def wait_vr(name, want, timeout, need_sync=False):
    """Seconds until the VolumeReplication reports `want`, or None."""
    started = time.time()
    while time.time() - started < timeout:
        state, synced, _ = vr_status(name)
        if state == want and (synced or not need_sync):
            return time.time() - started
        time.sleep(5)
    return None


def allowed_clients():
    """The DR_CLASS StorageClass's allowedClients, as the class spells them."""
    raw = jget("get storageclass %s" % CLASS).get("parameters", {}).get("allowedClients", "")
    return [c.strip() for c in raw.split(",") if c.strip()]


def app_node(clients):
    """The worker DR-01's writer runs on, and whether it earns the multi-network gate.

    A cluster whose class names one network, or whose chosen worker is on the
    first entry of the list, cannot tell the fixed driver from the broken one:
    keeping only the first entry produces a working mount there either way.
    """
    nodes = workers()
    node = APP_NODE or (nodes[-1] if nodes else "")
    if node and node not in nodes:
        sys.exit("DR_APP_NODE %r is not a schedulable worker (%s)" % (node, nodes))
    return node, len(clients) > 1 and bool(APP_NODE)


def writer_on(name, claim_name, node=None):
    """A writer deployment whose log file is WRITER rather than its own name, so
    both incarnations append to the one file the relationship carries."""
    d = writer_deployment(name, NS, claim_name, node_selector={"kubernetes.io/hostname": node} if node else None)
    for env in d["spec"]["template"]["spec"]["containers"][0]["env"]:
        if env["name"] == "WRITER":
            env["value"] = WRITER
    return d


def scale(name, replicas):
    oc("scale deployment %s -n %s --replicas=%d" % (name, NS, replicas), check=False)


def mirror(pv):
    """Run `qumulo-csi-repoint mirror` over the claim and its PV, which is what
    makes a claim on this cluster that points at the copy on the peer array."""
    objs = oc("get pvc/%s pv/%s -n %s -o json" % (CLAIM, pv, NS))
    src = os.path.join(C.out, "mirror-source.json")
    with open(src, "w") as fh:
        fh.write(objs)
    cmd = ("%s mirror -f %s -claim-suffix -dr -peer-server %s -peer-api-url %s -peer-token-file %s "
           "-secret-name %s -secret-namespace %s -insecure-skip-tls-verify -dry-run=false"
           % (REPOINT, src, PEER_SERVER, PEER_API_URL, PEER_TOKEN_FILE, PEER_SECRET, SECRET_NS))
    r = subprocess.run(cmd, shell=True, cwd=REPO, capture_output=True, text=True, timeout=600)
    with open(os.path.join(C.out, "mirror-output.txt"), "w") as fh:
        fh.write(r.stdout + r.stderr)
    return r.returncode == 0, (r.stderr or r.stdout).strip()[:300]


def main():
    clients = allowed_clients()
    node, d1_gate = app_node(clients)
    C.say("class", CLASS, "allows", clients, "| writer on", node or "(any worker)",
          "| multi-network gate" if d1_gate else "| NOT a multi-network gate")
    if not node:
        sys.exit("no schedulable worker")

    pv = None
    try:
        apply(namespace(NS))
        apply({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
               "metadata": {"name": CLAIM, "namespace": NS},
               "spec": {"accessModes": ["ReadWriteMany"], "storageClassName": CLASS,
                        "resources": {"requests": {"storage": "2Gi"}}}})
        if not wait_bound(NS, CLAIM):
            return C.record(False, "setup/claim-bound", "claim never Bound") or C.summary()
        pv = pv_of(NS, CLAIM)

        apply(writer_on("w-src", CLAIM, node))
        if not wait_app_ready(NS, "w-src", 300):
            return C.record(False, "setup/source-writer", "writer never Ready") or C.summary()
        C.say("source writer up; writing for", WRITE_S, "s before enabling replication")
        time.sleep(WRITE_S)

        # --- protect -------------------------------------------------------
        apply(vr("dr-src", CLAIM, VRC_AB, "primary"))
        took = wait_vr("dr-src", "Primary", SYNC_S, need_sync=True)
        if not C.record(took is not None, "setup/replication-established",
                        "Primary with a lastSyncTime after %s (bar %ds); %s"
                        % ("%.0fs" % took if took else "never", SYNC_S, vr_status("dr-src")[2])):
            return C.summary()
        time.sleep(WRITE_S)     # let the application write into a live relationship

        ok, detail = mirror(pv)
        if not C.record(ok, "setup/mirror-claim", "qumulo-csi-repoint mirror: %s" % (detail or "ok")):
            return C.summary()
        if not wait_bound(NS, DR_CLAIM):
            return C.record(False, "setup/dr-claim-bound", "mirrored claim never Bound") or C.summary()
        apply(vr("dr-peer", DR_CLAIM, VRC_BA, "secondary"))
        if not C.record(wait_vr("dr-peer", "Secondary", PROMOTE_S) is not None, "setup/peer-secondary",
                        "peer end Secondary; %s" % vr_status("dr-peer")[2]):
            return C.summary()

        # --- planned failover ----------------------------------------------
        C.say("quiescing the application, then failing over by CRs")
        scale("w-src", 0)
        time.sleep(10)
        started = time.time()
        apply(vr("dr-src", CLAIM, VRC_AB, "secondary"))
        demote = wait_vr("dr-src", "Secondary", PROMOTE_S)
        C.record(demote is not None, "DR-01/source-demoted",
                 "old primary Secondary after %s (bar %ds); %s"
                 % ("%.0fs" % demote if demote else "never", PROMOTE_S, vr_status("dr-src")[2]))
        apply(vr("dr-peer", DR_CLAIM, VRC_BA, "primary"))
        promote = wait_vr("dr-peer", "Primary", PROMOTE_S)
        C.record(promote is not None, "DR-01/copy-promoted",
                 "copy Primary after %s (bar %ds); %s"
                 % ("%.0fs" % promote if promote else "never", PROMOTE_S, vr_status("dr-peer")[2]))
        if promote is None:
            return C.summary()

        # --- DR-01: an application mounts the recovered copy -----------------
        apply(writer_on("w-dr", DR_CLAIM, node))
        got = wait_app_ready(NS, "w-dr", MOUNT_S, on=node)
        mounted = C.record(got is not None, "DR-01/app-mounts-recovered-copy",
                           "writer Ready on %s after %s (bar %ds)%s"
                           % (node, "%.0fs" % got[2] if got else "never", MOUNT_S,
                              "" if d1_gate else " -- NOT a multi-network gate, see below"))
        if not d1_gate:
            C.record(False, "DR-01/d1-gate-arranged",
                     "DR_CLASS allows %d network(s) and DR_APP_NODE is %s: a single-network class, or a worker on "
                     "the class's first network, cannot tell the fixed driver from the broken one. Set DR_CLASS to a "
                     "class naming two networks and DR_APP_NODE to a worker on the second."
                     % (len(clients), APP_NODE or "unset"))
        if not mounted:
            C.say("the promoted copy could not be mounted -- the signature of a replica export with too few networks; see events")
            with open(os.path.join(C.out, "dr-events.txt"), "w") as fh:
                fh.write(oc("get events -n %s --sort-by=.lastTimestamp" % NS, check=False))
            return C.summary()

        C.say("recovered copy mounted; writing for", WRITE_S, "s on the new primary")
        time.sleep(WRITE_S)

        # --- DR-02 and DR-03: the clock and the data -------------------------
        pod = got[0]
        a = grade_writer(C, "DR-03", NS, pod, WRITER, resume_bar=None)
        C.record(len(a["incarnations"]) >= 2, "DR-03/writes-land-on-the-new-primary",
                 "%d incarnations in the recovered log, %d lines: the copy carries what the old primary wrote and "
                 "the new one appends to it" % (len(a["incarnations"]), a["lines"]))
        C.record(a["max_gap"] <= RTO_S, "DR-02/recovery-time",
                 "application stopped %ds across the failover (bar %ds); gaps %s; CR promote took %.0fs"
                 % (a["max_gap"], RTO_S, a["gaps"], promote))
        C.sample(["recovery", a["max_gap"], RTO_S, "%.0f" % (promote or 0), "%.0f" % (demote or 0),
                  "%.0f" % (time.time() - started), a["lines"], len(a["holes"])])

        # --- DR-04: the old primary is frozen (not gating) -------------------
        scale("w-src", 1)
        back = wait_app_ready(NS, "w-src", 120, on=node)
        if back:
            time.sleep(20)
            errs = oc("logs -n %s %s --tail=40" % (NS, back[0]), check=False)
            C.record("IOERR" in errs, "DR-04/old-primary-frozen",
                     "a writer on the demoted claim reports %s (not gating: the array may answer a cached handle)"
                     % ("I/O errors, as a read-only replica should" if "IOERR" in errs else "no error"))
        else:
            C.record(True, "DR-04/old-primary-frozen", "the writer could not start on the demoted claim at all")

        with open(os.path.join(C.out, "result.json"), "w") as fh:
            json.dump({"allowed_clients": clients, "app_node": node, "d1_gate": d1_gate, "pv": pv,
                       "demote_s": demote, "promote_s": promote, "analysis": a}, fh, indent=2, default=str)
    finally:
        C.say("cleanup -- the replica directory and relationship ends on", PEER_SERVER, "are removed by hand")
        oc("delete volumereplication --all -n %s --ignore-not-found --wait=true" % NS, check=False, timeout=600)
        oc("delete ns %s --wait=false" % NS, check=False)
        if pv:
            oc("delete pv %s-dr --ignore-not-found --wait=false" % pv, check=False)
        with open(os.path.join(C.out, "cleanup-note.txt"), "w") as fh:
            fh.write("The mirrored PV is Retain, so the replica directory of %s stays on %s, as does any relationship\n"
                     "end left behind. Remove both by hand before the next run: a replica export staged by an earlier\n"
                     "driver is NOT rewritten (ensureReplicaExport leaves an existing export alone), so a stale one\n"
                     "makes a fixed driver fail DR-01 exactly as a broken one does.\n" % (pv, PEER_SERVER))

    return C.summary()


if __name__ == "__main__":
    sys.exit(main())
