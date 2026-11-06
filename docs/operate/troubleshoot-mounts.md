# Troubleshoot Guide

A PVC that bound but a pod that will not start, a pod that will not terminate,
or a mount with the wrong properties.

!!! note "On OpenShift, rule out SCC admission first"
    A node pod that was never created, or a pod admitted under the wrong SCC,
    produces symptoms that look like mount failures. Start at
    [Which SCC is each pod using](openshift-operations.md#which-scc-is-each-pod-using),
    then come back here.

## Start here

```bash
kubectl describe pod <pod-name> | tail -20

# the node plugin on the node the pod was scheduled to
NODE=$(kubectl get pod <pod-name> -o jsonpath='{.spec.nodeName}')
POD=$(kubectl -n qumulo-csi get pods -l app.kubernetes.io/component=node \
  --field-selector spec.nodeName="$NODE" -o name)
kubectl -n qumulo-csi logs "$POD" -c qumulo-csi-driver --tail=100
```

---

## Pod stuck in `ContainerCreating`

**Problem.** The volume is not being published on the node.

**Symptoms.** The pod never reaches `Running`; `describe pod` shows a
`FailedMount` event, sometimes only after the kubelet's two-minute wait.

### Check 1 — did the node register the driver?

```bash
kubectl get csinodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.drivers[*].name}{"\n"}{end}'
```

Every line must show `csi.qumulo.com`.

**Root cause, most common by a wide margin:** a non-default `kubeletDir` —
k3s and microk8s in particular.

**Resolution.**

```yaml
node:
  kubeletDir: /var/lib/rancher/k3s/agent/kubelet
```

!!! note "An empty `csinodes` entry means that node cannot mount"
    Any pod scheduled there hangs. Pods on other nodes are unaffected, which
    is why this looks intermittent.

### Check 2 — is the node plugin pod running on that node?

```bash
kubectl -n qumulo-csi get pods -l app.kubernetes.io/component=node -o wide
```

!!! warning "A node plugin pod can be left Pending on a packed node"
    The chart's PriorityClass is `preemptionPolicy: Never` by default — the
    plugin never evicts a workload to fit. A pod re-rolled onto a full node
    can stay Pending, and **that node's volumes cannot mount or unmount**
    until room is made. Set
    `node.priorityClass.preemptionPolicy: PreemptLowerPriority` if you would
    rather it evicted something.

### Check 3 — the mount message

| `mount.nfs` message | Cause | Resolution |
|---|---|---|
| `No such file or directory` | the export does not reach that path from that address — a `tenantID` class whose `server` is on another tenant's network, or an `exportBasePath`/`basePath` drift | fix the class's `server`, or the export |
| `access denied by server` | the client is outside the export's `allowedClients` | add the node's address or CIDR |
| `Connection timed out` | the node's kernel cannot reach the NFS address | network/firewall, not the driver |
| `Protocol not supported` / version errors | a protocol the cluster does not serve | check `protocol` and `/v2/nfs/settings` |
| GSS handshake failure | Kerberos: no keytab, or `rpc.gssd` is not running on the host | see [Kerberos](#kerberos-mounts-fail) |

!!! note "`nolock` is no longer the usual suspect"
    An NFSv3 class with no `nolock` used to lead this list. The driver now
    adds the option itself — check the node log for `adding nolock` before
    suspecting it.

**Prevention.** Run the reachability check from a **pod, not your laptop**:

```bash
kubectl run netcheck --rm -it --restart=Never --image=busybox:1.36 -- \
  nc -zv qumulo.example.com 2049
```

---

## Mount timeout

**Problem.** The mount neither succeeds nor fails promptly.

**Symptoms.** `FailedMount ... context deadline exceeded`, or a pod that sits
in `ContainerCreating` for minutes and then retries.

**Root cause, in order of likelihood:**

1. The node's kernel cannot reach the NFS address at all — the mount hangs
   until the kubelet's own timeout.
2. The array is up but slow, or in the middle of a rolling upgrade; a `hard`
   mount waits.
3. `fsGroupPolicy: File` with a large volume — the kubelet walks every entry
   applying group ownership before the pod starts. Measured at 4.17 ms per
   entry, so a million-file volume takes 20 to 70 minutes **per pod start**,
   far past the kubelet's two-minute mount wait.

**Resolution for (3).** Set `fsGroupChangePolicy: OnRootMismatch` on the pod,
or return to `fsGroupPolicy: None` and chown the pod's own `subPath` with an
`initContainer` — see
[Non-root pods and `subPath`](../features/mount-options.md#non-root-pods-and-subpath).

---

## Pod will not terminate

**Problem.** A pod stays `Terminating`, usually while the Qumulo cluster is
unreachable.

**Root cause.** `umount(8)` blocks indefinitely on a mount whose cluster is
unreachable, on **every** NFS mount option. With an array powered off for 25
minutes, pods on `hard`, `softerr`, `softreval` and `fatal_neterrors` mounts
all failed to terminate inside their 120 s grace period, and force-deleting
them left the mounts behind on the node.

**What the driver does.** The node plugin bounds the plain unmount
(`node.unmountTimeout`, default 30 s) and then escalates `umount -f` →
`umount -l`, returning success only once the target has left `/proc/mounts`.

!!! warning "`umount -f` is used only when safe"
    It aborts in-flight requests on the **whole NFS superblock**, and mounts
    from the same array address with the same options share one. So a pod
    deleted during an outage is released with `umount -l` instead, and the
    other pods' writes on that array carry on (D-57).

**Resolution.** Usually none needed — wait for the ladder. If terminations
legitimately take longer than 30 s on your network:

```yaml
node:
  unmountTimeout: "60s"
```

!!! tip "Keep it inside the workload's `terminationGracePeriodSeconds`"
    The whole point is to finish inside it. The kubelet also gives each
    unmount a fixed two minutes, so a value near that is cut — in effect about
    100 s at most.

!!! warning "Check for orphaned data after a forced unmount"
    `<kubeletDir>/csi.qumulo.com-orphaned/` holds **a pod's only copy of
    data** that reached the node's disk after its NFS mount went away. The
    node plugin logs both paths at ERROR and deletes nothing. Copy anything
    needed back to the volume before removing it.

---

## The volume mounted read-write when it should be read-only

**Problem.** A pod can write to a volume the class or PV marked `ro`.

**Diagnostics.**

```bash
kubectl exec <pod-name> -- mount | grep /data
```

**Root cause.** The pod does not use the node's mount. The container runtime
binds it in, and that bind need not keep the read-only flag — runc 1.4.3
(containerd 2.3.4) was measured giving the pod a bind that reads `rw`. The NFS
superblock underneath is what actually refuses writes.

**What the driver does.** It verifies the superblock after every read-only
mount via `/proc/self/mountinfo`, and refuses the publish with
`FailedPrecondition` if the kernel left either read-write — so a pod never
starts on a writable volume it asked to be read-only. The message asks for the
node's `uname -r`: one lab's kernel was seen to do this, and why is not yet
known.

**Resolution.** Also set `readOnly: true` on the pod's `volumeMount`.

---

## Permission denied inside the pod

**Problem.** The mount succeeded and the pod started, but writes fail with
`EACCES`.

**Root cause.** The volume's mode is the only thing deciding, because the
chart sets `fsGroupPolicy: None`. And the kubelet creates `subPath`
directories **as root**, mode `0755` — so a pod running as uid 1001 with a
`subPath` gets a directory it does not own.

On a default `0777` volume this works. On `mountPermissions: 0755` or
`inherit` under a restrictive parent, it does not.

**Resolution.** An `initContainer` that chowns the **same `subPath`** the app
container mounts. Full example:
[Non-root pods and `subPath`](../features/mount-options.md#non-root-pods-and-subpath).

!!! warning "Do not reach for `0777` to work around it"
    A world-writable volume root is exactly what tightening the mode traded
    away.

---

## File locking does not work across nodes

**Problem.** Two pods on different nodes both acquire what should be an
exclusive lock; a database reports corruption.

**Root cause.** The class is `nfs3`, where `nolock` makes locks node-local
(`local_lock=all`) — the server never sees a lock. Measured:
`flock(LOCK_EX|LOCK_NB)` from a second node *succeeded* while the first held
the lock, and SQLite failed with `disk I/O error`.

**Resolution.** Use `protocol: nfs4.1`. Because `parameters` is immutable,
that means a new class and migrating the workload.

**Prevention.** Treat `nfs3` as single-writer or lock-free only. "Uses file
locks" is often a property of a library rather than a choice the application
made.

---

## Kerberos mounts fail

**Problem.** A `securityFlavor: krb5*` class mounts nowhere, or on some nodes
only.

**Root cause.** Kerberos needs host-side setup the chart cannot supply. Every
node that may run such a pod needs:

1. to be joined to the same AD domain as the Qumulo cluster,
2. a readable keytab for `nfs/<node>@<REALM>` and a `krb5.conf`,
3. **`rpc.gssd` running on the host** — the GSS upcall is resolved by the
   host's kernel, so the daemon cannot live in the plugin pod.

Point 3 is the one that catches people.

!!! note "`hostPathType` decides how a missing keytab fails"
    With `FileOrCreate` (the default) a node without the two host files still
    runs the plugin, so every non-Kerberos volume mounts and a Kerberos volume
    there is refused naming the missing keytab. With `File`, the plugin is
    refused on such a node and **no** volume of any class mounts there.

**Also check.** Kerberos requires `dedicated-export` and NFSv4.1 — it is
refused on `shared-subdirectory` or `nfs3` rather than silently ignored. A
`krb5*` class against a cluster reporting Kerberos disabled, or with no idmap
domain, fails the PVC with the reason.

---

## One node is slow while others are fine

**Root cause.** The Linux NFS client keeps **one TCP connection per server
address, NFS version and transport**. Every pod on that node reaching the
array through one address shares one flow and one slot table, so a heavy
writer delays everyone's metadata there.

**Resolution.** Spread addresses: classes naming different floating addresses
get separate connections. `nconnect` only takes effect on the *first* mount to
an address, so if you set it, set it on every class that mounts from that
address.

---