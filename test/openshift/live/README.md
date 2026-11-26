# OpenShift live checks — `test/openshift/live`

The live half of the OpenShift tier. `test/openshift` checks the chart against
*models* of OpenShift admission, offline; these scripts check the same three
claims on a real OpenShift cluster with a real Qumulo array. Each issue found
while bringing the chart up on OpenShift 4.22 (October 2026) has a check here,
so a later build can be validated the same way.

| Script | Requirement | What it proves | Issues it guards |
|---|---|---|---|
| `fresh_install.sh` | Installation | `helm install` with default values and no SCC step: every pod Ready, controller under `restricted-v2`, node under the chart's SCC; a `restricted-v2` app writes and reads on NFS 4.1 and NFS 3; `helm uninstall` leaves no SCC, grant, CSIDriver or PriorityClass | I-2 (health port bound to the node address, a second driver on the same cluster), I-4 (no node token, SCC without `projected`) |
| `upgrade_from_pre_openshift.sh` | Installation | a release made with a chart from before OpenShift support creates no node pods; `helm upgrade --reuse-values` to the chart under test succeeds and every pod comes up with no SCC step | I-1 (`--reuse-values` nil pointer); with `BASE_CHART` set, also reproduces it on the old chart |
| `permissions.sh` | Permissions | `oc auth can-i` for both service accounts: the node account may use only its own SCC in its own namespace and nothing else; the controller cannot list Secrets, use privileged SCCs, delete pods or grant itself rights; the node SCC is granted by one namespaced RoleBinding | I-4 |
| `failed_upgrade_recovery.sh` | Installation | after a failed upgrade (the stock render, refused by SCC), a plain `helm upgrade` leaves the controller's `runAsUser 65532` and its new pods refused; `helm upgrade --force` repairs it | I-13 (Helm diffs against the last deployed revision; the documented recovery) |
| `app_matrix.py` | Application security | 7 workload profiles (`restricted-v2` plain, with `fsGroup`, with `supplementalGroups`; `restricted-v3` user namespace; `nonroot-v2`; `anyuid`; `privileged`) × folder modes `0777`, `0770`, `0750` × NFS 4.1 and NFS 3, 42 pods | I-11 (user-namespace pods cannot mount NFS on RHCOS 9.8: recorded as FAIL on the `rv3-userns` rows until the platform changes), I-12 (`0750` refuses non-root writers) |
| `storage_outage.py` | Storage outages | a pod whose volume is on a blackholed array address terminates on its own inside its grace period with no `--force` and leaves no mount on the worker; a second pod on the same NFS superblock sees no I/O error; node-plugin threads and zombies stay bounded and return to baseline | a forced unmount reaching a healthy volume on the same superblock; an unmount timeout near the kubelet's deadline; unbounded node-plugin threads and zombie helpers |
| `storage_outage_poweroff.py` | Storage outages | the same, with the array powered off rather than blackholed, across `hard`, `softerr`, `softreval` and `fatal_neterrors` mounts -- the measurement the fix was originally graded on (25-81 s on Kubernetes) | the same superblock and unmount-timeout issues |
| `node_add_remove.py` | Cluster changes | a worker added through the Machine API gets a Ready, registered node plugin and the topology label; an existing volume mounts on it and reads back intact; a new claim provisions there; the same worker drained and removed moves its RWX and RWO writers elsewhere and keeps no mount of theirs | NL-01, NL-02 |
| `node_loss.py` | Cluster changes | a worker powered off under its writers: after the out-of-service taint they come back on another node with no synced write lost, and the lost node leaves nothing in the API | NL-04 |
| `node_reboot.py` | Cluster changes | a worker rebooted, gracefully and by hard reset, under writers pinned to it: they run again on it with one mount per volume, no stale volume directory and no synced write lost | NL-05, NL-06 |
| `controller_disruption.py` | Cluster changes | the controller leader's node rebooted mid-provisioning, the node with the controller replicas drained, a master rebooted: leadership moves, the PDB lets the drain finish, every operation completes | NL-03, NL-07, NL-08 |
| `mcp_roll.py` | Cluster changes | a MachineConfigPool rolled twice (drain and reboot of every node): the pool never degrades, writers lose nothing and stop at most 300 s | NL-09 |
| `dr_failover.py` | Disaster recovery | everything *after* a planned failover: an application mounts the promoted copy on a worker off the class's first network, how long it was stopped, and that the recovered log has no hole and the reference file's hash still matches | DR-01 to DR-04; the live gate for a replica export that carries every allowed-clients network |

## Running

From a machine with cluster-admin `oc` and `helm`:

```sh
cp live.env.example live.env && $EDITOR live.env
./fresh_install.sh
./upgrade_from_pre_openshift.sh
./permissions.sh                        # against the installed release
./failed_upgrade_recovery.sh
set -a; . ./live.env; set +a; ./app_matrix.py

# Storage outages. Destructive at node level -- a disposable lab cluster only.
set -a; . ./live.env; set +a
I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1 ./storage_outage.py            # ~20 min, blackhole fault
I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1 ./storage_outage_poweroff.py   # prompts you to power the array off and back on
```

### The two outage checks

They are kept apart from the five above because they are the only ones that
break something on purpose, and because they need lab arrangements the others
do not:

- **`storage_outage.py` needs `SPARE_FLOATING_IP`** -- an array address nothing
  else on the cluster mounts. It refuses to run if the worker it picked already
  mounts that address, because blackholing the address the rest of the cluster
  uses would take every volume on every worker away rather than this check's
  two. It settles the nftables/iptables question on the worker at run time and
  says which backend it found; a worker with neither stops the run with that
  reason rather than failing obscurely later.
- **`storage_outage_poweroff.py` needs a Qumulo array you can power off.** It
  does not power it off itself -- it prompts and waits for `confirmed`, so the
  dangerous step stays in a human's hands. `OUTAGE_ASSUME_YES=1` skips the
  prompts for an automated lab that handles power itself.

Both run their workload pods under `restricted-v2` in a PSA-`restricted`
namespace, because the question is what happens to an **ordinary** OpenShift
application when the array goes away. Both delete pods **without `--force`**:
forcing removes the API object and leaves the mount on the node, which is the
behaviour from before the fix, so a check that forced would pass no matter what
the driver did.

### The node-lifecycle checks

Cluster changes -- nodes added, removed and rebooted while applications run.
All five are destructive at node level and belong on a disposable lab cluster;
each refuses without `I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1`, and `mcp_roll.py`
also needs `NL_ALLOW_MCP_ROLL=1`.

```sh
set -a; . ./live.env; set +a
I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1 ./node_add_remove.py        # ~30 min, needs NL_MACHINESET
I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1 ./node_reboot.py            # ~15 min per mode
I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1 ./controller_disruption.py  # ~40 min
I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1 ./node_loss.py              # ~25 min, needs NL_MACHINESET
I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1 NL_ALLOW_MCP_ROLL=1 ./mcp_roll.py   # 30-40 min for a two-worker pool
```

### The disaster-recovery check

Everything after a planned failover, starting with a pod mounting the promoted
copy. It needs **two Qumulo arrays** and the csi-addons
sidecar, and it leaves a replica directory on the peer array for you to remove.

```sh
set -a; . ./live.env; set +a
I_UNDERSTAND_THIS_IS_DESTRUCTIVE=1 ./dr_failover.py            # ~40 min, needs two arrays
```

Each script prints one verdict per case (`DR-01` to `DR-04`) and keeps its
evidence under the run directory.

What it relies on, and why:

- **The build must include the multi-network replica export fix.** Before it
  the driver staged the replica's export from the first entry of the class's
  allowed-clients list alone, and DR-01 measures that defect rather than the fix.
- **`DR_CLASS` must name two networks and `DR_APP_NODE` must be a worker on the
  second.** On a single-network cluster -- or with the writer on the class's
  *first* network -- keeping only the first entry still produces a working
  mount, so the check cannot tell the fixed driver from the broken one. It
  records `DR-01/d1-gate-arranged` FAIL when that arrangement is missing, so a
  run that proves nothing says so rather than reading as a pass.
- **Start from a clean peer.** `ensureReplicaExport` deliberately leaves an
  existing export alone, so a replica staged by an earlier driver still carries
  the truncated list and a *fixed* driver fails DR-01 exactly as a broken one
  does. Remove the previous run's replica directory and relationship ends on
  the peer first; the check writes a `cleanup-note.txt` naming them.
- **One writer, two claims.** `WRITER` names the file rather than the pod, so
  the promoted copy carries the log the old primary wrote and the new
  incarnation appends to it. One `analyse_log` over that file then answers all
  three gating cases: the incarnation boundary is the failover, the gap across
  it is the recovery time, and a hole anywhere in it is a synced write
  replication did not carry.
- **Recovery time is measured from the application, not the CRs.** The CR
  demote and promote durations are recorded beside it; a large gap with fast
  CRs is a mount or scheduling cost, not a replication one.
- **A planned failover, not a forced one.** The source is quiesced and demoted
  before the copy is promoted, which is the path the runbook documents. An
  unplanned failover (the source array genuinely gone) is not covered here.

What they rely on, and why:

- **A node they add is the only node they remove.** `node_add_remove.py` and
  `node_loss.py` scale `NL_MACHINESET` up by one, and give it back its starting
  size by deleting exactly that Machine (`machine.openshift.io/delete-machine`),
  even when they fail. A worker the cluster had before is rebooted, drained
  and uncordoned, never removed.
- **Writers sync every line.** Each writer appends `<incarnation> <seq> <epoch>`
  to its own log and syncs it, beside a 64 MiB reference file and its sha256.
  A hole in an incarnation's sequence is a write the application had synced
  and the cluster change lost; a moved hash is corrupted data; the gap between
  two incarnations is how long the application was stopped. A hard reset or a
  power-off may tear the one line in flight -- that is allowed, a lost synced
  line is not.
- **Writers stop on SIGTERM**, as real applications do. A shell as PID 1
  ignores SIGTERM, and a writer that wrote on through its grace period would
  make every drain measure the grace period instead of the driver.
- **No VolumeAttachment.** The driver sets `attachRequired: false`, so the node
  side of a change is mounts and registration: the checks read the node's
  `/proc/mounts` and kubelet volume directories through `oc debug node`, and
  the CSINode, the topology label and the CSIStorageCapacity objects through
  the API.
- **RWO hand-over.** With no attach step, nothing but the kubelet stops a
  replacement pod mounting an RWO claim on another node while the old pod is
  still terminating. `rwo-one-writer-at-a-time` reports it if two
  incarnations ever wrote at once; `ReadWriteOncePod` is what prevents it.
- **Reboots go through the host's systemd** (`systemd-run`), because `oc debug`
  deletes its pod when the session ends and a backgrounded `reboot` inside it
  would die with it.
- **Classes.** `NL_CLASSES` names one NFS 4.1 class (RWX claims) and one NFS 3
  class (RWO claims). Both must allow the subnet a new worker lands in -- a
  class allowing only the old workers' subnet refuses the new node's mounts --
  and give a `0777` folder, since the writers run under `restricted-v2` with an
  arbitrary UID (I-12).

Each script prints one verdict per case (`NL-01` to `NL-09`) and keeps its
evidence under the run directory.

Verdict names map to the validation plan's cases (`O-1`, `O-2`, `O-3`, `O-5`,
`O-7`). Each script prints its verdicts and keeps its evidence under the run
directory.

Each writes `<RUN_DIR>/<check>/verdicts.txt` (one `PASS`/`FAIL` line per
check) and a full log. Every script creates its own namespaces, classes and
volumes and deletes them at the end; `fresh_install.sh` and the upgrade check
use their own driver names (`fresh.csi.qumulo.com`, `upg.csi.qumulo.com`) and
health ports (29753, 29754), because the installed release already holds
29653 on every node.

## Expected results (OCP 4.22.18)

- `fresh_install.sh`: 10/10 PASS, pods Ready in about 35 s.
- `upgrade_from_pre_openshift.sh`: 6/6 PASS with `BASE_CHART` set to a chart from before the `--reuse-values` fix.
- `permissions.sh`: 24/24 PASS.
- `failed_upgrade_recovery.sh`: 5/5 PASS (the third check passing means I-13 still reproduces; if Helm changes, it fails and the doc note can go).
- `app_matrix.py`: 16/18 graded PASS. The two FAILs are the `rv3-userns`
  rows on `0777` (I-11, a platform limit, documented in
  `docs/get-started/openshift.md`). Anything else failing is a regression.
