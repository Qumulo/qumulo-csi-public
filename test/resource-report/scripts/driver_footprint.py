#!/usr/bin/env python3
# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

"""Driver resource-footprint run against two arrays. Run as root (the node process mounts NFS):

    sudo python3 scripts/driver_footprint.py <run-dir> [--quick] [--only=<scenario> ...]
                                             [--scenarios=<file.json>] [--idle=<duration>]

--scenarios replaces the built-in list below with a JSON list of the same shape (test/performance uses it; any
configured array letter may appear in "runs"). --idle sets csiload's gap between phases (default 30s, 5s --quick).

Starts bin/qumulo-csi-driver as two processes like a deployment: controller (-mode controller, non-root) and node
(-mode node; in NODE_IMAGE when NODE_RUNTIME=docker, else on the host), each on its own socket with metrics. Samples
both every 1 s (CPU, RSS, threads, FDs, TCP to :8000 and :2049, Go metrics) and the arrays every 10 s (REST GET:
connections), then runs bin/csiload scenarios. Everything created is named <NAME_PREFIX>-*; csiload deletes it, and
this script unmounts anything left under the target root on exit.
Writes: samples-proc.jsonl, samples-array.jsonl, ops-<scenario>.jsonl, scenarios.json, driver-*.log."""
import json, os, re, signal, subprocess, sys, threading, time, urllib.request
from pathlib import Path

SUITE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SUITE / "scripts"))
from rr import Array, array_cfg, config  # noqa: E402

CFG = config()
BIN = SUITE / "bin"
PREFIX = CFG.get("NAME_PREFIX", "csi-rcr")
WORK_ROOT = Path(f"/var/tmp/{PREFIX}")
TARGET_ROOT = WORK_ROOT / "targets"
# Sockets live here, not in the run folder: a unix socket path is limited to ~107 bytes, which a run folder nested
# inside another suite's output (or a CI work directory) easily exceeds.
SOCK_DIR = WORK_ROOT / "sock"
NODE_RUNTIME = CFG.get("NODE_RUNTIME", "docker")
NODE_CTR, NODE_IMAGE = f"{PREFIX}-node", CFG.get("NODE_IMAGE", "qumulo-csi-driver-node:0.1.0")
CTRL_PORT, NODE_PORT = int(CFG.get("CONTROLLER_METRICS_PORT", 29654)), int(CFG.get("NODE_METRICS_PORT", 29655))
TICK = os.sysconf("SC_CLK_TCK")
PORTS = {0x1F40: "rest8000", 0x0801: "nfs2049"}


def _array(side):
    a = array_cfg(CFG, side)
    return dict(server=a["server"], apiurl=a["api"], token=a["token_file"],
                shared=(a["shared_real"], a["shared_export"]), dedicated=(a["dedicated_real"], a["dedicated_export"]))


SIDES = sorted({m.group(1) for k in CFG if (m := re.fullmatch(r"ARRAY_([A-Z])_API", k))})
ARRAYS = {s: _array(s) for s in SIDES}


def scenarios(quick):
    s = 0.2 if quick else 1.0
    n = lambda x: max(2, int(x * s))
    return [
        dict(name="idle-baseline", idle_only=120 if not quick else 20),
        dict(name="A-shared", runs=[("A", "shared", dict(n=n(50), concurrency=8, expand=n(10), snapshots=n(10), publish=n(10), hold="60s"))]),
        dict(name="B-shared", runs=[("B", "shared", dict(n=n(50), concurrency=8, expand=n(10), snapshots=n(10), publish=n(10), hold="60s"))]),
        dict(name="A-dedicated", runs=[("A", "dedicated", dict(n=n(20), concurrency=4, publish=n(5), hold="30s"))]),
        dict(name="B-dedicated", runs=[("B", "dedicated", dict(n=n(20), concurrency=4, publish=n(5), hold="30s"))]),
        dict(name="AB-concurrent-shared", runs=[("A", "shared", dict(n=n(50), concurrency=8)),
                                                ("B", "shared", dict(n=n(50), concurrency=8))]),
        dict(name="A-burst200", runs=[("A", "shared", dict(n=n(200), concurrency=16))]),
        dict(name="B-burst200", runs=[("B", "shared", dict(n=n(200), concurrency=16))]),
        dict(name="idle-after", idle_only=120 if not quick else 20),
    ]


# ---------------------------------------------------------------- process sampling
def sock_inodes(pid):
    out = set()
    try:
        for fd in os.listdir(f"/proc/{pid}/fd"):
            try:
                l = os.readlink(f"/proc/{pid}/fd/{fd}")
                if l.startswith("socket:["):
                    out.add(l[8:-1])
            except OSError:
                pass
    except OSError:
        pass
    return out


def tcp_by_port(inodes):
    counts = {v: 0 for v in PORTS.values()}
    for f in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            lines = open(f).read().splitlines()[1:]
        except OSError:
            continue
        for ln in lines:
            p = ln.split()
            rport = int(p[2].split(":")[1], 16)
            if p[3] == "01" and p[9] in inodes and rport in PORTS:      # ESTABLISHED
                counts[PORTS[rport]] += 1
    return counts


def scrape(port):
    try:
        txt = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2).read().decode()
    except Exception:
        return {}
    m, api = {}, {}
    for ln in txt.splitlines():
        if ln.startswith("#") or not ln:
            continue
        name, _, val = ln.rpartition(" ")
        base = name.split("{")[0]
        try:
            v = float(val)
        except ValueError:
            continue
        if base in ("go_goroutines", "go_threads", "go_memstats_heap_inuse_bytes", "go_memstats_heap_alloc_bytes",
                    "go_memstats_sys_bytes", "process_resident_memory_bytes", "process_open_fds",
                    "go_gc_duration_seconds_count", "process_cpu_seconds_total"):
            m[base] = v
        elif base.endswith("_total") and ("api" in base or "rpc" in base or "grpc" in base) and "bucket" not in base:
            api[base] = api.get(base, 0) + v
    m["counters"] = api
    return m


def proc_sample(pid):
    try:
        st = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        status = open(f"/proc/{pid}/status").read()
    except OSError:
        return None
    g = lambda k: int(re.search(rf"^{k}:\s+(\d+)", status, re.M).group(1))
    inodes = sock_inodes(pid)
    return {"cpu_s": (int(st[11]) + int(st[12])) / TICK, "rss_kb": g("VmRSS"), "hwm_kb": g("VmHWM"),
            "threads": g("Threads"), "fds": len(os.listdir(f"/proc/{pid}/fd")), **tcp_by_port(inodes)}


class Sampler(threading.Thread):
    def __init__(self, procs, out):
        super().__init__(daemon=True)
        self.procs, self.out, self.stop = procs, open(out, "a"), threading.Event()

    def save_full(self):
        """Full /metrics text from each process, every 30 s: per-endpoint REST and per-RPC counters live here."""
        d = Path(self.out.name).parent / "metrics-full"
        d.mkdir(exist_ok=True)
        for role, (_, port) in self.procs.items():
            try:
                txt = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3).read()
                (d / f"{role}-{int(time.time())}.prom").write_bytes(txt)
            except Exception:
                pass

    def run(self):
        n = 0
        while not self.stop.is_set():
            if n % 30 == 0:
                self.save_full()
            n += 1
            rec = {"t": time.time(), "phase": CURRENT["phase"],
                   "mounts": count_mounts(self.procs["node"][0])}
            for role, (pid, port) in self.procs.items():
                rec[role] = {"proc": proc_sample(pid), "metrics": scrape(port)}
            self.out.write(json.dumps(rec) + "\n"); self.out.flush()
            self.stop.wait(1)


class ArraySampler(threading.Thread):
    def __init__(self, out):
        super().__init__(daemon=True)
        self.out, self.stop = open(out, "a"), threading.Event()
        self.arrays = {s: Array(s, CFG, timeout=8) for s in "AB"}

    def run(self):
        while not self.stop.is_set():
            rec = {"t": time.time(), "phase": CURRENT["phase"]}
            for s, a in self.arrays.items():
                t0 = time.time()
                conns = a.get("/v2/network/connections/")
                lat = (time.time() - t0) * 1000
                by = {}
                for node in conns if isinstance(conns, list) else []:
                    for c in node["connections"]:
                        k = c["type"].replace("CONNECTION_TYPE_", "")
                        by[k] = by.get(k, 0) + 1
                rec[s] = {"conns": by, "rest_ms": round(lat, 1),
                          "err": conns.get("_error") if isinstance(conns, dict) else None}
            self.out.write(json.dumps(rec) + "\n"); self.out.flush()
            self.stop.wait(10)


CURRENT = {"phase": "startup"}
IDLE = ["30s"]


def start_driver(role, run, port):
    sock = SOCK_DIR / f"{role}.sock"
    if sock.exists():
        sock.unlink()
    args = [str(BIN / "qumulo-csi-driver"), f"-endpoint=unix://{sock}", f"-mode={role}",
            "-insecure-skip-tls-verify", "-log-level=info", f"-metrics-address=127.0.0.1:{port}"]
    if role == "node":
        args += [f"-nodeid={PREFIX}-workstation", "-unmount-timeout=20s"]
    log = open(run / f"driver-{role}.log", "w")
    if role == "node" and NODE_RUNTIME == "docker":
        # Run the node process in the chart's node image (nfs-common) with the freshly built binary bind-mounted
        # over it: privileged + host network, like the DaemonSet pod. Needs no NFS userland on this machine.
        subprocess.run(["docker", "rm", "-f", NODE_CTR], capture_output=True)
        args = ["docker", "run", "--rm", "--name", NODE_CTR, "--privileged", "--net=host",
                "-v", f"{BIN}:/jbin:ro", "-v", f"{run}:{run}", "-v", f"{WORK_ROOT}:{WORK_ROOT}",
                "--entrypoint", "/jbin/qumulo-csi-driver", NODE_IMAGE] + args[1:]
    pre = []
    if role == "controller" and os.geteuid() == 0 and os.environ.get("SUDO_UID"):
        # controller does not need root; run it as the invoking user, as the chart does (nonroot)
        pre = ["setpriv", f"--reuid={os.environ['SUDO_UID']}", f"--regid={os.environ['SUDO_GID']}", "--clear-groups"]
    p = subprocess.Popen(pre + args, stdout=log, stderr=subprocess.STDOUT)
    for _ in range(100):
        if sock.exists():
            break
        time.sleep(0.1)
    pid = p.pid
    if role == "node" and NODE_RUNTIME == "docker":      # the driver is PID 1 in the container; sample its host PID
        pid = int(subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", NODE_CTR],
                                 capture_output=True, text=True).stdout.strip())
    return p, sock, pid


def run_csiload(run, scen, side, model, opts, ctrl_sock, node_sock):
    a = ARRAYS[side]
    real, exp = a[model]
    prefix = f"{PREFIX}-{scen['name'].lower()}-{side.lower()}"
    args = [str(BIN / "csiload"), f"-endpoint=unix://{ctrl_sock}", f"-node-endpoint=unix://{node_sock}",
            f"-server={a['server']}", f"-apiurl={a['apiurl']}", f"-token-file={a['token']}",
            f"-realpath={real}", f"-exportpath={exp}",
            f"-model={'dedicated-export' if model == 'dedicated' else 'shared-subdirectory'}",
            f"-prefix={prefix}", f"-target-root={TARGET_ROOT}", f"-idle={IDLE[0]}"]
    args += [f"-{k}={v}" for k, v in opts.items()]
    out = open(run / f"ops-{scen['name']}-{side}.jsonl", "w")
    return subprocess.Popen(args, stdout=out, stderr=subprocess.STDOUT)


def count_mounts(pid):
    try:
        return sum(1 for l in open(f"/proc/{pid}/mounts") if str(TARGET_ROOT) in l and " nfs" in l)
    except OSError:
        return None


def cleanup_mounts():
    """Unmount anything csiload left under the target root (inside the node container in docker mode)."""
    sh = f"grep ' {TARGET_ROOT}/' /proc/mounts | cut -d' ' -f2 | xargs -r -n1 umount -f -l"
    cmd = ["docker", "exec", NODE_CTR, "sh", "-c", sh] if NODE_RUNTIME == "docker" else ["sh", "-c", sh]
    subprocess.run(cmd, capture_output=True)


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    if os.geteuid() != 0:
        raise SystemExit("run with sudo: the node process mounts NFS")
    run = Path(sys.argv[1]).resolve(); run.mkdir(parents=True, exist_ok=True)
    if os.environ.get("SUDO_UID"):          # the controller runs as the invoking user and creates its socket here
        os.chown(run, int(os.environ["SUDO_UID"]), int(os.environ["SUDO_GID"]))
    quick = "--quick" in sys.argv
    if quick:
        IDLE[0] = "5s"
    only = [a.split("=", 1)[1] for a in sys.argv if a.startswith("--only=")]
    for a in sys.argv:
        if a.startswith("--idle="):
            IDLE[0] = a.split("=", 1)[1]
    plan = next((json.loads(Path(a.split("=", 1)[1]).read_text()) for a in sys.argv if a.startswith("--scenarios=")),
                None)
    TARGET_ROOT.mkdir(parents=True, exist_ok=True)
    SOCK_DIR.mkdir(parents=True, exist_ok=True)
    if os.environ.get("SUDO_UID"):          # the non-root controller creates its socket here
        os.chown(SOCK_DIR, int(os.environ["SUDO_UID"]), int(os.environ["SUDO_GID"]))

    for b in ("qumulo-csi-driver", "csiload"):
        if not (BIN / b).exists():
            raise SystemExit(f"{BIN / b} missing: run ./run.sh build first")
    ctrl, ctrl_sock, ctrl_pid = start_driver("controller", run, CTRL_PORT)
    node, node_sock, node_pid = start_driver("node", run, NODE_PORT)
    os.chmod(node_sock, 0o666)
    procs = {"controller": (ctrl_pid, CTRL_PORT), "node": (node_pid, NODE_PORT)}
    sp = Sampler(procs, run / "samples-proc.jsonl"); sp.start()
    ap = ArraySampler(run / "samples-array.jsonl"); ap.start()
    log = []
    try:
        time.sleep(3)
        for scen in plan if plan is not None else scenarios(quick):
            if only and scen["name"] not in only:
                continue
            CURRENT["phase"] = scen["name"]
            t0 = time.time()
            print(f"{time.strftime('%T')} scenario {scen['name']}", flush=True)
            if "idle_only" in scen:
                time.sleep(scen["idle_only"])
            else:
                ps = [run_csiload(run, scen, side, model, opts, ctrl_sock, node_sock) for side, model, opts in scen["runs"]]
                for p in ps:
                    p.wait()
                cleanup_mounts()
            log.append({"name": scen["name"], "start": t0, "end": time.time(), "spec": scen})
            (run / "scenarios.json").write_text(json.dumps(log, indent=1))
    finally:
        CURRENT["phase"] = "shutdown"
        time.sleep(2)
        sp.save_full()
        sp.stop.set(); ap.stop.set()
        cleanup_mounts()
        if NODE_RUNTIME == "docker":
            subprocess.run(["docker", "stop", "-t", "15", NODE_CTR], capture_output=True)
        for p in (ctrl, node):
            p.send_signal(signal.SIGTERM)
        for p in (ctrl, node):
            try:
                p.wait(15)
            except subprocess.TimeoutExpired:
                p.kill()
        if os.environ.get("SUDO_UID"):
            subprocess.run(["chown", "-R", f"{os.environ['SUDO_UID']}:{os.environ['SUDO_GID']}", str(run)])
        print("done", flush=True)


if __name__ == "__main__":
    main()
