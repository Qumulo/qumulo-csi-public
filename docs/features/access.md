# Storage Connectivity and Access Control

Which NFS version the mount uses, who is allowed to mount, and how file
permissions work.

## Choose the NFS version

NFSv4.1 is the default and the right answer for nearly everyone.

```yaml
storageClasses:
  - name: qumulo-nfs
    protocol: nfs4.1      # or nfs3
```

!!! warning "Use NFSv4.1 if anything depends on file locking"
    On NFSv3 locks never leave the machine, so two pods on different machines
    can both believe they hold the same lock. Measured on a shared volume:
    a second machine's `flock` succeeded while the first still held it, and
    SQLite failed with a disk I/O error. On NFSv4.1 the same test behaved
    correctly.

If you ask for a version your Qumulo cluster has turned off, the claim fails
with the reason, instead of leaving a pod hanging at mount.

## Share one volume between many pods

Ask for `ReadWriteMany` and point several pods at the same claim. They can all
read and write it at once, from any machine.

```yaml
spec:
  accessModes: [ReadWriteMany]
```

!!! warning "`ReadWriteOnce` and `ReadOnlyMany` are accepted but not enforced"
    Both are allowed, and neither is enforced. This driver publishes no
    `VolumeAttachment` — an NFS export needs no attach step — and Kubernetes
    enforces the single-node rule only for drivers that have one. Measured: a
    volume requested `ReadWriteOnce` was mounted **and written** from pods on
    two different nodes at the same time. Treat these two as a statement of
    intent, not a guarantee.

!!! tip "`ReadWriteOncePod` is the single-writer mode that does hold"
    `ReadWriteOncePod` is enforced by Kubernetes itself, in the scheduler and
    the kubelet, with no attach step involved. Use it when exactly one pod must
    be able to write:

    ```yaml
    spec:
      accessModes: [ReadWriteOncePod]
    ```

    A second pod claiming the same PVC stays Pending.

## What a pod runs as

The driver sets `fsGroupPolicy: None`, so Kubernetes never walks the volume to
change file owners at mount. What a pod can do is decided entirely by the
folder's own mode — `mountPermissions` on the class — and the uid and gid the
pod runs with.

That matters most under OpenShift, where the default `restricted-v2` SCC gives
each pod a **random uid from the project's range and gid 0**. With the default
`0777` it reads and writes; with `0750` it can only read, and adding `fsGroup`
does not change that, because `fsGroupPolicy: None` means `fsGroup` is never
applied. The full matrix is in
[Install on OpenShift](../get-started/openshift.md#what-application-pods-need).

## Limit which machines can mount

Only on a class that creates one share per volume. List your nodes'
addresses or ranges:

```yaml
storageClasses:
  - name: qumulo-dedicated
    provisioningModel: dedicated-export
    allowedClients:
      - 10.0.0.0/24
```

!!! warning "An unrestricted share is open to everyone"
    Without this, the share admits any machine that can reach the Qumulo
    cluster, with full rights and no password. The install stops unless you
    set `allowedClients`, say `allowAnyClient: true`, or use Kerberos.

## Kerberos

Needs one share per volume and NFSv4.1. The driver sets the authentication
mode on the Qumulo share and matches it on the mount, so both halves agree.

```yaml
storageClasses:
  - name: qumulo-krb
    provisioningModel: dedicated-export
    protocol: nfs4.1
    securityFlavor: krb5p      # or krb5, krb5i
```

Every node also needs a keytab, and `node.kerberos.enabled` set so the node
pods can read the host's Kerberos configuration.

A class asking for Kerberos against a cluster with it turned off, or with no
idmap domain, fails with that reason rather than silently falling back.

## File permissions

```yaml
storageClasses:
  - name: qumulo-nfs
    mountPermissions: "0770"     # or "inherit" to keep the cluster's own
```

For pods that do not run as root, the recommended approach is an init
container that takes ownership of the pod's own sub-folder 
