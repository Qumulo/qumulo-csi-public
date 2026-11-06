# Choose a protocol and mount options

`mountOptions` is a native StorageClass field, rendered **verbatim** into the
PersistentVolume and passed to `mount(8)` by the node plugin.

## The default

A class that sets no `mountOptions` renders:

```yaml
mountOptions: [hard, "timeo=600"]
```

An `nfs3` class that sets none renders `[hard, timeo=600, nolock]` — the
default is protocol-aware.

!!! warning "An explicit list replaces the default, it does not add to it"
    `mountOptions: [nolock]` on an `nfs3` class is a working mount with no
    `timeo=600`, which is probably not what was intended. Write the full list:
    `[hard, "timeo=600", nolock]`.

## `hard` versus `soft`

| | `hard` (default) | `soft` |
|---|---|---|
| During a Qumulo rolling upgrade or node evacuation | in-flight I/O pauses, retries every `timeo` tenths of a second, resumes transparently | returns `EIO` to the application after `retrans` retries |
| A write can block | for as long as the array is unreachable | no |
| Risk | a pod can hang on an outage | **silent data loss on write** — NFS `soft` has a long history of it |

!!! tip "Keep `hard` unless the application already handles I/O errors"
    Most do not. `timeo=600` (60 seconds) with the kernel's default
    `retrans=2` is what this release was validated against; a cluster with a
    longer expected upgrade window may want a larger `timeo`.

!!! note "`hard` does not mean a pod can never be released"
    The unmount ladder bounds it: `umount(8)`, then `umount -f`, then
    `umount -l`. `umount -f` aborts in-flight requests on the whole NFS
    superblock, so it is used only for a mount that is the last on its
    superblock — a pod deleted during an outage is otherwise released with
    `umount -l`, and the other pods' writes on that array carry on.

## `nolock` on NFSv3

The node image ships `mount.nfs` but runs no `rpc.statd`, so an `nfs3` mount
without `nolock` fails outright. It is supplied for you from two directions:

- an `nfs3` class with no `mountOptions` renders it;
- an `nfs3` class with an explicit list gets it added by the node at mount
  time, which is also what fixes volumes provisioned before this behaviour
  existed, whose `mountOptions` are frozen in the PV;
- a class with no `protocol` whose `mountOptions` pin `vers=3` or `nfsvers=3`
  is NFSv3 too, and gets it the same way.

Either way `/proc/mounts` shows `nolock`, and the node logs when it added one.

!!! warning "An `nfs3` class that explicitly asks for `lock` is refused"
    At `helm install` and again at mount time. Server-side v3 locking cannot
    work on this node, and silently inverting the request would hide the
    mistake.

What `nolock` costs is real: locks become node-local (`local_lock=all`), so no
two pods on different nodes can safely share a file lock over `nfs3`.

!!! warning "`nfs3` is for single-writer or lock-free workloads only"
    Measured on two nodes writing one RWX volume over `nfs3`,
    `flock(LOCK_EX|LOCK_NB)` from the second node *succeeded* while the first
    held the lock, and SQLite failed with `disk I/O error`. Over `nfs4.1` the
    same test returned `EAGAIN` and wrote all rows. "Uses file locks" is often
    a property of a library, not a choice the application made.

!!! note "On `nfs4.1`, `nolock` is accepted and then ignored"
    v4.1 locks in-protocol (`local_lock=none` either way), and negotiates
    `sec=sys` unasked — so no `sec=` option is required, **unless** the class
    sets `securityFlavor`.

## Options that contradict the class

An option that disagrees with the StorageClass is refused rather than silently
losing to it, so the mistake surfaces at install rather than at mount:

| In `mountOptions` | Against | Result |
|---|---|---|
| `vers=` / `nfsvers=` | a different `protocol` | refused |
| version shorthand `v3`, `v4`, `v4.1`, `v4.2` | a different `protocol` | refused; a matching one is accepted |
| `sec=` | a set `securityFlavor` that differs | refused |
| `sec=` | no `securityFlavor` set | accepted, with a warning |

The shorthand forms are new in this release; `vers=`/`nfsvers=` were always
checked.

## Read-only volumes

A class with `ro` in `mountOptions` — one entry or comma-joined, `[hard, ro]`
and `["hard,ro"]` are the same — or a PV with `readOnly: true` is mounted
read-only on the node.

That alone is less than it looks: the pod does not use the node's mount. The
container runtime binds it into the container, and that bind need not keep the
read-only flag — runc 1.4.3 (containerd 2.3.4) was measured giving the pod a
bind that reads `rw`. What refuses writes is the NFS superblock under it.

The node plugin checks that superblock after every read-only mount via
`/proc/self/mountinfo`. If the kernel left either read-write, the publish is
refused with `FailedPrecondition` naming both option lists, and the mount is
taken away — so a pod never starts on a writable volume it asked to be
read-only.

!!! tip "Also set `readOnly: true` on the pod's `volumeMount`"
    ```yaml
    containers:
      - name: app
        volumeMounts:
          - name: data
            mountPath: /data
            readOnly: true
    ```
    That makes the kubelet ask the runtime for a read-only bind as well.

!!! warning "On RHCOS, `ro` in `mountOptions` alone is refused"
    On RHCOS 9.8 the kernel does not leave the superblock read-only for a
    `mountOptions`-only `ro`, so the check above refuses the publish. Set
    `readOnly: true` on the pod's volume or `volumeMounts` — the supported way
    to get a read-only volume on OpenShift. This is the check working, not a
    defect.

## One connection per array address, per node

The Linux NFS client keeps one client, and one TCP connection, per server
address, NFS version and transport. Every pod on a node reaching an array
through one address shares one TCP flow and one slot table: a heavy writer
delays everyone's metadata on that node. Measured in a kernel lab, 40 NFSv4.1
mounts used one connection, and 40 NFSv3 mounts added one more.

!!! warning "`nconnect` takes effect only on the first mount to an address"
    A later mount to the same address — another class, another option — joins
    the existing connection and its `nconnect` is ignored, silently. Set it on
    *every* class that mounts from an address, so whichever mounts first sets
    it.

!!! tip "To spread load, spread addresses"
    Classes naming different floating addresses of the array get separate
    connections. The driver adds no `nconnect` itself.

!!! note "NFSv3 mounts here negotiate `mountproto=udp`"
    Forcing `mountproto=tcp` leaves a reserved-port socket in `TIME_WAIT` per
    mount.

## Non-root pods and `subPath`

Kubernetes has one mechanism for making a volume writable by a non-root pod:
the kubelet applies the pod's `fsGroup` at mount time. This chart sets
`fsGroupPolicy: None`, so it **never does that here**: on a shared NFS tree
the kubelet would walk the whole volume re-owning files on every pod start,
which is slow and would re-own other pods' files underneath them.

That leaves the volume's own mode deciding, and one detail sharpens it: **the
kubelet creates `subPath` directories as root**, mode `0755`. A pod running as
uid 1001 with a `subPath` gets a directory it does not own, inside a volume
whose permissions it also does not control.

On a default `0777` volume this works. On `mountPermissions: 0755`, or
`inherit` under a restrictive parent, the pod gets `EACCES` on its own
directory — after the volume mounted successfully and the pod started.

!!! tip "Recommended: an `initContainer` that chowns the path"
    It changes one directory rather than a whole shared tree, and needs no
    chart-wide setting.

    ```yaml
    spec:
      containers:
        - name: app
          image: myapp:1.0
          securityContext: {runAsUser: 1001, runAsGroup: 1001}
          volumeMounts:
            - {name: data, mountPath: /data, subPath: user-1001}
      initContainers:
        - name: fix-ownership
          image: busybox:1.36
          securityContext: {runAsUser: 0}
          command: ["sh", "-c", "chown 1001:1001 /data && chmod 0700 /data"]
          volumeMounts:
            - {name: data, mountPath: /data, subPath: user-1001}
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: home-dirs}
    ```

    Both containers must mount the **same `subPath`**. Without that, the init
    container chowns the volume root instead — the shared-tree problem again.

!!! warning "What not to do"
    Do not reach for `mountPermissions: 0777` to get around a `subPath`
    permission error if you had a reason to tighten it. A world-writable
    volume root is exactly what the default trades away.

## Planned maintenance checklist

| Situation | Setting |
|---|---|
| Qumulo rolling upgrade expected | keep `hard`; consider a larger `timeo` |
| Workload must fail fast instead of blocking | `soft` — only if it retries at the application layer |
| Pods must terminate promptly during an array outage | tune `node.unmountTimeout`, not the mount options |
| Heavy writer starving other pods on a node | spread classes across floating addresses |