#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

# SAFETY: this module performs no fault on import. The faults live in the scripts that use it.
"""Shared helpers for the two storage-outage checks.

Both scripts ask the same question -- when the Qumulo array stops answering,
does an application pod shut down cleanly and leave the node clean? -- and
differ only in how the array is taken away (a blackhole rule on one worker, or
the array itself powered off). Everything else is here.

What is measured, in both:

* `shutdown`  : wall-clock from `oc delete pod` to the pod leaving the API,
                with no --force. The bar is the pod's grace period.
* `stale`     : mounts through the array still in /proc/mounts on the worker
                afterwards. The bar is zero.
* `bystander` : I/O errors seen by a pod writing to a *different* volume on the
                same NFS superblock during the forced unmount. Bar: zero.
* `resources` : node-plugin threads, goroutines, fds, RSS and zombie children,
                sampled through the outage. Bar: bounded during,
                back to baseline after, no zombies.

Evidence goes to <RUN_DIR>/<check>/: verdicts.txt, log.txt, samples.tsv.
"""
import json
import os
import re
import subprocess
import sys
import time

import yaml

RUN_DIR = os.path.expanduser(os.environ.get("RUN_DIR", "~/run-ocp-live"))
APP_IMAGE = os.environ.get("APP_IMAGE", "registry.access.redhat.com/ubi9/ubi-minimal:9.5")
DEBUG_IMAGE = os.environ.get("DEBUG_IMAGE", "registry.access.redhat.com/ubi9/ubi-minimal:9.5")
DRIVER_NS = os.environ.get("DRIVER_NS", "qumulo-csi")
DRIVER_RELEASE = os.environ.get("DRIVER_RELEASE", "qumulo-csi")
KUBE_CONTEXT = os.environ.get("KUBE_CONTEXT", "")


def require_destructive(what):
    """Refuse to run unless the operator has said, in the environment, that they
    understand what is about to happen to this cluster."""
    if os.environ.get("I_UNDERSTAND_THIS_IS_DESTRUCTIVE") != "1":
        sys.exit(
            "refusing to run: this check %s.\n"
            "It is for a disposable lab cluster only -- never a shared or production one, and never the\n"
            "dev minikube. Set I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1 to proceed." % what
        )


class Check:
    """One outage check: its output directory, its verdict list, its log."""

    def __init__(self, name):
        self.name = name
        self.out = os.path.join(RUN_DIR, name)
        os.makedirs(self.out, exist_ok=True)
        self.log_path = os.path.join(self.out, "log.txt")
        self.verdicts_path = os.path.join(self.out, "verdicts.txt")
        self.samples_path = os.path.join(self.out, "samples.tsv")
        self.verdicts = []
        self._require_context()

    def _require_context(self):
        if not KUBE_CONTEXT:
            return
        current = sh("oc config current-context", check=False).strip()
        if current != KUBE_CONTEXT:
            sys.exit("context is %r, not KUBE_CONTEXT=%r -- refusing" % (current, KUBE_CONTEXT))

    def say(self, *parts):
        line = "[%s] %s" % (time.strftime("%H:%M:%S", time.gmtime()), " ".join(str(p) for p in parts))
        print(line, flush=True)
        with open(self.log_path, "a") as fh:
            fh.write(line + "\n")

    def record(self, ok, name, detail=""):
        verdict = "PASS" if ok else "FAIL"
        self.verdicts.append((verdict, name, detail))
        self.say(verdict, name, detail)
        with open(self.verdicts_path, "a") as fh:
            fh.write("%s %s %s\n" % (verdict, name, detail))
        return ok

    def sample(self, row):
        with open(self.samples_path, "a") as fh:
            fh.write("\t".join(str(c) for c in row) + "\n")

    def summary(self):
        failed = [v for v in self.verdicts if v[0] == "FAIL"]
        self.say("%d checks, %d FAIL -- evidence in %s" % (len(self.verdicts), len(failed), self.out))
        return 1 if failed else 0


def sh(cmd, check=True, inp=None, timeout=None):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, input=inp, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError("%s: %s" % (cmd, (r.stderr or r.stdout).strip()))
    return r.stdout


def oc(args, check=True, inp=None, timeout=300):
    return sh("oc " + args, check=check, inp=inp, timeout=timeout)


def apply(obj):
    return oc("apply -f -", inp=yaml.safe_dump(obj))


def jget(args):
    return json.loads(oc(args + " -o json"))


def workers():
    """Schedulable worker node names, sorted."""
    data = jget("get nodes -l node-role.kubernetes.io/worker=")
    return sorted(n["metadata"]["name"] for n in data.get("items", []) if not n["spec"].get("unschedulable"))


def node_shell(node, script, timeout=180):
    """Run a shell snippet in the node's own root namespace.

    `oc debug node/... --profile=sysadmin` is the OpenShift equivalent of the
    privileged debug pod the Kubernetes proofs use; chroot /host is what makes
    it the host's filesystem and not the debug image's.
    """
    cmd = (
        "debug node/%s -n default --profile=sysadmin --image=%s --quiet -- "
        "chroot /host /bin/sh -c %s" % (node, DEBUG_IMAGE, quote(script))
    )
    return oc(cmd, check=False, timeout=timeout)


def quote(s):
    return "'" + s.replace("'", "'\"'\"'") + "'"


def node_plugin_pod(node):
    """The driver's node pod on that worker."""
    data = jget(
        "get pods -n %s -l app.kubernetes.io/component=node --field-selector spec.nodeName=%s" % (DRIVER_NS, node)
    )
    items = data.get("items", [])
    if not items:
        raise RuntimeError("no node plugin pod on %s in namespace %s" % (node, DRIVER_NS))
    return items[0]["metadata"]["name"]


def mounts_through(node, address):
    """Mount points on the worker whose source is that array address."""
    out = node_shell(node, "cat /proc/mounts")
    return [line.split()[1] for line in out.splitlines() if line.startswith(address + ":")]


def plugin_resources(node):
    """Threads, zombie children and D-state tasks under the node plugin, read
    from the host. Cheap enough to sample every 15 s."""
    pod = node_plugin_pod(node)
    script = (
        "pid=$(pgrep -f 'qumulo-csi-driver .*-mode=node' | head -1); "
        '[ -n "$pid" ] || { echo "threads=0 zombies=0 dstate=0 rss=0"; exit 0; }; '
        "threads=$(ls /proc/$pid/task | wc -l); "
        "zombies=$(ps -o stat= --ppid $pid 2>/dev/null | grep -c Z); "
        "dstate=$(ps -eo stat= | grep -c '^D'); "
        "rss=$(awk '/VmRSS/{print $2}' /proc/$pid/status); "
        'echo "threads=$threads zombies=$zombies dstate=$dstate rss=$rss"'
    )
    out = node_shell(node, script)
    vals = dict(re.findall(r"(\w+)=(\d+)", out))
    vals["pod"] = pod
    return {k: (int(v) if v.isdigit() else v) for k, v in vals.items()}


def pvc(name, ns, storage_class, size="1Gi"):
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": ns},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "storageClassName": storage_class,
            "resources": {"requests": {"storage": size}},
        },
    }


def writer_pod(name, ns, claim, node, grace=120):
    """A pod that writes to its volume once a second and logs every failure.

    It runs under `restricted-v2` -- the SCC an ordinary OpenShift workload
    gets -- because the question is what happens to a *normal* application when
    the array goes away, not to a privileged one.
    """
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": ns},
        "spec": {
            "nodeName": node,
            "terminationGracePeriodSeconds": grace,
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "writer",
                    "image": APP_IMAGE,
                    "command": ["/bin/sh", "-c"],
                    "args": [
                        "i=0; while true; do i=$((i+1)); "
                        'if echo "$i $(date -u +%s)" >> /data/writer.log 2>/tmp/err; then '
                        'echo "ok $i"; else echo "IOERR $i $(cat /tmp/err)"; fi; sleep 1; done'
                    ],
                    "volumeMounts": [{"name": "data", "mountPath": "/data"}],
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "runAsNonRoot": True,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                }
            ],
            "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": claim}}],
        },
    }


def wait_running(ns, name, timeout=300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        phase = oc("get pod %s -n %s -o jsonpath={.status.phase}" % (name, ns), check=False).strip()
        if phase == "Running":
            return True
        time.sleep(5)
    return False


def delete_and_time(ns, name, timeout=600):
    """Delete a pod without --force and return how long it took to leave the API.

    No --force is the whole point: forcing deletes the API object and leaves the
    mount on the node, which is the behaviour before the fix. Returns None if
    the pod outlived the timeout.
    """
    started = time.time()
    oc("delete pod %s -n %s --wait=false" % (name, ns), check=False)
    while time.time() - started < timeout:
        rc = subprocess.run(
            "oc get pod %s -n %s" % (name, ns), shell=True, capture_output=True, text=True
        ).returncode
        if rc != 0:
            return time.time() - started
        time.sleep(2)
    return None


def io_errors(ns, name):
    """Count IOERR lines a writer pod has logged."""
    out = oc("logs %s -n %s --tail=-1" % (name, ns), check=False)
    return len([line for line in out.splitlines() if line.startswith("IOERR")])


def namespace(name):
    """A namespace the writer pods can run in under restricted-v2."""
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": {
                "pod-security.kubernetes.io/enforce": "restricted",
                "pod-security.kubernetes.io/enforce-version": "latest",
            },
        },
    }
