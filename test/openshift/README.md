# OpenShift support tier — `test/openshift`

Unit cases for the three OpenShift features, runnable on a laptop:

- **Installation** — the driver runs inside OpenShift's permitted security
  settings with **no manual workaround**: no `oc adm policy add-scc-to-user`,
  no hand-written SecurityContextConstraints.
- **Permissions** — the install asks for only the access it needs, including
  the SCC it grants itself.
- **Application security compatibility** — volumes are usable across the SCCs
  OpenShift workloads actually run under, and the requirements for that are
  written down below.

It needs `helm`. It needs **no OpenShift cluster, no Qumulo array and no
network**. Target: **OpenShift Container Platform 4.22** (Kubernetes 1.35.x),
RHCOS workers.

One exception, and it announces itself: `node_outage_test.go` drives the real
driver against a real NFS mount on the host running it. It **skips** unless
that host has opted in three times over (`QUMULO_LIVE_TEST`,
`QUMULO_LIVE_NODE`, `QUMULO_LIVE_OUTAGE`) and is root, so
`go test -tags openshift ./test/openshift/` stays safe to run anywhere. It
lives here rather than in `test/live/` so that one command covers the whole
storage-outage feature; see **Running** below.

## How it differs from `test/chart/openshift_test.go`

That file asserts the templates emit the right YAML, by substring. This tier
parses the install and evaluates it against models of OpenShift's admission
machinery — SecurityContextConstraints and Pod Security Admission — so the
question is whether admission would **accept** the install, not whether the
text looks right. The headline case reads the SCC the chart renders and admits
the node pod against it; nothing in `test/chart` can do that.

The two are complementary and neither replaces the other.

## Storage outages

`outage_test.go` covers a fourth thing: **clean application shutdown when the
Qumulo array is unreachable.** The driver answers a dead array with a bounded
unmount, then `umount -f`, then `umount -l`, so a pod on that array terminates
inside its grace period and the node keeps no stale mount. That logic is
distribution-blind and is tested in `pkg/driver`.

What is not distribution-blind is the install it runs inside, and three things
the chart renders are what let `umount(8)` reach the host at all: Bidirectional
propagation on the kubelet directory, a driver container privileged enough to
unmount in the host namespace, and the `-unmount-timeout` plumbing. None of
them was asserted anywhere before. An install that loses one rolls out, reports
healthy, provisions and mounts normally, and then hangs every pod the first
time an array goes away.

`node_outage_test.go` is the other half, and the only case in this repository
that runs the forced-unmount ladder against a real mount with the array
genuinely unreachable. It needs no cluster: it makes the array unreachable with
a firewall rule on the host, which is also why it is destructive and must not
overlap another live case on the same machine. Run it with
`make live-node-outage`.

Those cases assert the install as it ships. **A failure there is a finding
about the install, not a test to relax** — and, as above, they are checks
against a model. The live half is `live/storage_outage.py` and
`live/storage_outage_poweroff.py`, and the `pkg/driver` side parses a captured
RHCOS/CRI-O mountinfo (`pkg/driver/testdata/rhcos-mountinfo-crio.txt`) so the
shared-superblock guard is exercised against real RHCOS text rather than only
against injected values.

## Cluster changes

`node_lifecycle_test.go` covers a fifth: **nodes added, removed and rebooted
while applications run.** The behaviour is checked live, by the node-lifecycle
scripts in `live/` (NL-01 to NL-09). What those lean on is a handful of install
properties that roll out cleanly whether they are right or not, and only show
up the day a node changes, so they are asserted here:

- a node pod counts as available only once the kubelet has registered it
  (readiness probe plus `minReadySeconds`), because a PV's nodeAffinity names
  the topology label registration sets on a new node;
- the node pods carry a PriorityClass in the system range, so a full node
  still runs the plugin;
- the controller runs two replicas behind a `maxUnavailable: 1` PDB that
  selects them, so a drain always leaves one holding the leases and is never
  blocked outright, and renders no PDB with one replica;
- the replicas prefer separate nodes (softly, so both can still share one);
- `attachRequired` is false, so a lost node leaves nothing to detach.

## It is a tier of its own

- every file is behind the **`openshift` build tag**, so `make test` and a
  plain `go test ./...` neither run nor compile it -- the tag is what keeps it
  a tier rather than a set of cases mixed into another suite;
- `make ci` *does* run it, as its own gate, because it needs only helm and
  finishes in under four seconds;
- its own package, sharing no fixture, golden file or helper with
  `test/chart`, `test/e2e` or `pkg/driver`;
- no case was added to an existing suite, and nothing existing changed to
  accommodate it.

It reads the chart. It modifies nothing.

## Running it

From the repository root:

```sh
make openshift-test            # the tier
make openshift-test-verbose    # plus the SCC verdicts and the derived facts
```

It is also one of the gates in `make ci`, and a step in the `chart + manifests`
job of `.github/workflows/ci.yml` -- so a change that breaks OpenShift
admission fails the PR rather than waiting for someone to run this by hand.
**That is not the same as validation.** Every verdict is against a *model* of
OpenShift's SCCs in `scc_test.go`, so a wrong model is confidently wrong in
both places. The gate catches regressions against what we believe; only a
4.22 cluster checks the belief.

Or directly, which is also how to point it at a different chart:

```sh
go test -tags openshift ./test/openshift/ -count=1
QCSI_CHART=/path/to/charts/qumulo-csi-driver go test -tags openshift ./test/openshift/
```

Every render declares `--api-versions security.openshift.io/v1/SecurityContextConstraints`,
because `openshift.enabled: auto` detects the platform from the cluster's API
list and an offline render sees no cluster. A render without it silently
produces the **plain-Kubernetes** install — which is why the values file pins
`openshift.enabled: true` and one case asserts the non-OpenShift path on
purpose.

## Installing on OpenShift

```sh
oc apply -f test/openshift/values/openshift-namespace.yaml

helm install qcsi oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver -n qumulo-csi \
  -f test/openshift/values/openshift-values.yaml \
  --set storageClasses[0].name=qumulo \
  --set storageClasses[0].server=<array> \
  --set storageClasses[0].secretName=qumulo-token \
  --set storageClasses[0].basePath=/csi/volumes
```

**That is the whole install.** There is no SCC step: the chart renders the node
plugin's SCC, grants `use` on it by name through a ClusterRole, and binds that
to the node ServiceAccount with a RoleBinding in the release namespace. The
controller's fixed uid is dropped so `restricted-v2` assigns one.

The namespace manifest is still required and is not something the chart can do
for you — see the PSA note under *Requirements* below.

Where SCCs are centrally governed, add `-f test/openshift/values/openshift-values-admin-scc.yaml`:
the chart then creates neither the SCC nor the grant and pins the node pods to
the administrator's. `make openshift-test-verbose` prints what that SCC must
allow.

Where a policy engine (Gatekeeper) also keeps RoleBindings and
ClusterRoleBindings for a platform team, start from
`test/openshift/values/openshift-values-gatekeeper.yaml`: a custom
`restricted-csi` controller SCC, the platform's node SCC, `hydra_storage`
nodes only, and `rbac.createBindings: false`. `gatekeeper_test.go` checks that
the install then holds no binding, and that the platform's bindings plus the
install are object for object the default install.

## Requirements for volume access

The four things that decide whether a workload can read and write a Qumulo
volume on OpenShift, and the one that cannot be settled offline.

### 1. Arbitrary UIDs and `mountPermissions`

OpenShift's `restricted-v2` — the configuration nearly every workload runs in —
assigns the pod a **random uid from the namespace's
`openshift.io/sa.scc.uid-range`**, with gid 0. That uid matches no owner on the
Qumulo export.

The driver ships `mountPermissions: 0777`, which is what makes an arbitrary uid
able to write. **Anything stricter, and `inherit`, is permission-denied for
ordinary OpenShift workloads.** It fails inside the application rather than at
the mount, so it reads as an application bug.

If a customer's policy forbids 0777 on the export, the answer is not a stricter
mode — it is a workload pinned to a known uid (its own SCC, or
`runAsUser` under `nonroot-v2`/`anyuid`) with the export owned by that uid.

### 2. `fsGroupPolicy: None`

The CSIDriver ships `fsGroupPolicy: None`, so the kubelet performs **no
recursive chown and no relabel** on the volume. Nothing fixes the permissions
up after the mount; the export's own mode is the whole story, which is what
makes (1) decisive rather than merely advisory.

It is a **mutable** CSIDriver field, so `File` can be evaluated on a cluster
without reinstalling — but note it would then chown every file in a restored
tree on first mount, which on a large volume is not free.

### 3. Pod Security Admission on the namespace

OpenShift runs PSA alongside SCC. The node DaemonSet is privileged,
hostNetwork and hostPath, so its namespace must be labelled
`pod-security.kubernetes.io/{enforce,audit,warn}=privileged`, with
`security.openshift.io/scc.podSecurityLabelSync: "false"` so the syncer does
not rewrite the labels from the bound SCCs a few minutes later.

`values/openshift-namespace.yaml` does both, and the tier derives the required
level from the rendered pods rather than trusting the file.

The controller is restricted-level on its own, so a customer who wants it in a
separate, restricted namespace can have that.

### 4. Which SCC the controller is pinned to

`openshift.controllerSCC` decides whether the controller keeps its fixed uid
(with `openshift.assignControllerUID: auto`, the default). The chart drops it
for any `restricted*` SCC -- a custom `restricted-csi` included -- because
those assign one and refuse any other, and keeps it otherwise, because an SCC
that assigns no uid would leave the sidecar images running as root and
`runAsNonRoot` would refuse them. A custom uid-assigning SCC with another name
sets `assignControllerUID: true`. The `helm test` pods follow the same SCC and
the same choice. The supported values are evaluated by the tier
(`appsecurity_test.go`, and `gatekeeper_test.go` for custom SCCs):

| `openshift.controllerSCC` | uid | why |
|---|---|---|
| `restricted-v2` (default) | assigned by OpenShift | `MustRunAsRange`; 65532 is outside the namespace range |
| `restricted-v3` | assigned by OpenShift | same strategy |
| `nonroot-v2` | 65532 kept | assigns none; without the uid the sidecars run as root |
| `anyuid` | 65532 kept | assigns none |
| `privileged` | 65532 kept | assigns none |

### 5. SELinux — settled on a live cluster, 28 Oct 2026

This was this tier's largest open risk and is no longer open. Measured on
OpenShift 4.22, RHCOS 9.8, SELinux **enforcing**, on NFS 4.1 and NFS 3:

- A workload pod under `restricted-v2`, with an arbitrary uid and gid 0, reads
  and writes a Qumulo volume with the default class settings.
- No `seLinuxOptions`, no `container_use_nfs`/`virt_use_nfs` boolean and no
  mount context option were needed.
- Two pods with **different** SELinux labels share one volume. NFS mounts carry
  one context for the whole mount rather than per-file labels, so there is no
  relabel to fail.

`fsGroupPolicy: None` continues to avoid the usual relabel failures.

**This still cannot be tested offline and nothing in this tier covers it** —
the statements above come from the live run, not from these tests. Re-confirm
on any cluster whose SELinux policy has been customised.

One real platform limit did come out of that run: a pod with
`hostUsers: false` (a user-namespace pod, for example under `restricted-v3`)
cannot use an NFS volume at all —
`mount_setattr ... doesn't support idmap mounts on this kernel`. The RHCOS 9.8
kernel cannot ID-map NFS mounts, so this affects every NFS CSI driver. Nothing
in the chart or the driver can work around it.

## What else this tier cannot tell you

- **csi-addons against ODF.** Replication is off in the shipped values. Despite
  the `*.openshift.io` API group names, csi-addons is not part of OpenShift; it
  arrives with OpenShift Data Foundation, cluster-scoped and at ODF's version.
  Determine the customer's ODF status before enabling it.
- Kerberos end to end, which needs a MachineConfig for `rpc.gssd`, a keytab
  distribution decision, a KDC and a reboot cycle.
- Machine Config Operator rolling reboots, forced unmount with the array
  unreachable, and a mid-restore controller eviction.
- User-workload monitoring. Still uncovered: the 28 Oct run could not execute
  its monitoring check either, because the OpenShift monitoring API wants a
  bearer token and the lab logs in with a client certificate. It needs a
  service-account token, or the Thanos querier route. Note also that OpenShift
  has shipped no Grafana since 4.11.
- **Any built-in SCC a customer has edited.** The SCC models in `scc_test.go`
  are of the stock OpenShift 4.x definitions. Confirm with
  `oc get scc restricted-v2 nonroot-v2 anyuid privileged -o yaml` before
  relying on a verdict here. The one SCC that is *not* a model is the chart's
  own — that one is read back from the install.

## Layout

```
render_test.go          helm render, manifest model
pod_test.go             pod template extraction, effective security contexts
scc_test.go             SCC and PSA evaluators, and the reader for the chart's own SCC
installation_test.go    Installation
permissions_test.go     Permissions
appsecurity_test.go     Application security compatibility
outage_test.go          Storage outages: the install properties the forced-unmount ladder needs
node_outage_test.go     Storage outages, live: real mounts against an unreachable array (SKIP unless opted in)
node_lifecycle_test.go  Cluster changes: the install properties node add, removal and reboot rely on
values/                 the OpenShift values files and the namespace manifest
```

The first three hold no `Test` functions — they are this tier's helpers, named
`_test.go` so the `openshift` tag keeps them out of every other build, the same
shape `test/chart` uses.

## Live checks

`live/` holds the same three claims checked on a real cluster and array:
install and uninstall, the upgrade from a pre-OpenShift release, the service
accounts' rights, and an application-security matrix. It also holds the
storage-outage checks and the node-lifecycle checks (nodes added, removed and
rebooted under running writers); the regression suite runs the node-lifecycle
checks as its `openshift-live` profile. See [live/README.md](live/README.md).

## Running

```sh
make openshift-test     # the offline tier: helm only, seconds, safe anywhere
make openshift-night    # the whole folder, verbose -- what the nightly runs
make live-node-outage   # the live outage cases, under sudo. DESTRUCTIVE
```

`openshift-night` is `openshift-test` on any machine that has not opted into
live mounting: the two outage cases report `SKIP` and name the variable they
wanted. That is deliberate — a skipped case in an unattended run must be
legible, never silent.

The harness the live cases drive is [`test/livekit`](../livekit), shared with
`test/live/`. It was lifted out of `test/live/` when this tier needed it: a
`_test.go` file cannot be imported, so the only way for two packages to drive
one cluster the same way is for the harness to be an ordinary package.
