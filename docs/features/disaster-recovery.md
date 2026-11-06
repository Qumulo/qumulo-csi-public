# Disaster recovery

Replicating volumes to a second Qumulo array, and bringing them up there when you need to.

## What it is

Qumulo's own directory replication, driven from Kubernetes. The driver maps
the usual DR verbs — protect, fail over, fail back, re-protect — onto
relationships between your array and its peer.

## Turn it on

```yaml
sidecars:
  csiAddons:
    enabled: true

volumeReplicationClasses:
  - name: qumulo-dr
    targetServer: array-b.example.com
    targetApiUrl: https://array-b.example.com:8000
    targetBasePath: /k8s/dr
```
## Protect a volume

```yaml
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeReplication
metadata:
  name: my-data-vr
spec:
  volumeReplicationClass: qumulo-dr
  replicationState: primary
  dataSource:
    kind: PersistentVolumeClaim
    name: my-data
```

The copy's share is staged read-only straight away, so a replica is never
reachable read-write before you promote it.

## Protect a whole storage class

On a shared-subdirectory class, a per-volume relationship is refused — the
only relationship possible covers the whole class, and driving it from one
claim would freeze everyone else's volumes at failover.

```yaml
volumeReplicationClasses:
  - name: qumulo-group-dr
    replicationScope: group
```

One relationship, one failover, every volume in the class.

## Fail over

Set the DR side to `primary`. A **planned** failover loses nothing: the source
is frozen and fully drained — compared on data, not on a timer — and the copy
is promoted only once it holds everything.

If the source cannot be reached, the driver asks it twice more with a short
backoff before accepting that it is gone, so a network blip cannot quietly
turn a planned failover into a forced one. Forced, it promotes at the last
retained recovery point.

Then repoint the claims and restart the pods:

```bash
go run ./cmd/qumulo-csi-repoint failover -namespace apps          # plans only
go run ./cmd/qumulo-csi-repoint failover -namespace apps -dry-run=false
```

!!! warning "Stop workloads before deleting volumes"
    A claim a pod still mounts cannot be deleted, and the tool will not try —
    it checks the pods first and stops if any still hold a claim.

## Fail back

Once the original array is healthy again, failing back is the same procedure
in reverse: re-protect in the new direction, then move the workloads home.

**1. Re-protect, so the original array catches up.** Set the side you want to
become the replica to `resync`:

```yaml
spec:
  replicationState: resync
```

This reverses the relationship, re-applies the class's settings, and
reconnects the old primary as the replica. Its share is set read-only first,
so pods there are refused until it is promoted again — a promoted copy can
never be overwritten by the side that used to be live.

Wait for it to report healthy before going further. Reconnecting discards
anything written on the old primary after the failover, so a side that is
still writable has to be forced deliberately rather than resynced by
accident.

**2. Move the workloads back.** This is a different subcommand, not the
failover one run backwards:

```bash
# plan it first -- dry-run is the default
go run ./cmd/qumulo-csi-repoint failback -namespace apps

# then apply
go run ./cmd/qumulo-csi-repoint failback -namespace apps \
    -dns-target 192.0.2.30 -dry-run=false
```

**3. Promote the original side** by setting it back to `primary`, which runs
the same frozen, fully drained failover as before — so, like any planned
failover, it loses nothing.

!!! warning "`-dns-target` is required on a failback"
    On a failover the tool works out the new address from the promoted side.
    On a failback it cannot: the side being promoted *is* the one the DNS
    record already names, so there is nothing to derive from. Give the
    original array's address explicitly.

!!! note "The original claims come back unchanged"
    Failback puts the original PersistentVolumeClaims back into service as
    they were. Nothing has to be recreated.

## Three things to plan around

!!! warning "Pods always restart on failover"
    A promoted array issues new NFS file handles. That is Qumulo behaviour;
    nothing the driver or DNS can hide.

!!! warning "A person triggers failover, never the driver"
    There is no automatic failover, deliberately.

!!! warning "Snapshots do not follow replication"
    Replication is the DR mechanism here, not snapshots, and restoring a
    snapshot across arrays is refused.