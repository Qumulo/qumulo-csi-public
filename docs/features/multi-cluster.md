# Working with several Qumulo clusters

One installation of the driver serves any number of Qumulo clusters. You do
not install it twice.

## A storage class per cluster

Each cluster gets its own class and its own credential. No cluster address
lives in the installation itself.

```yaml
storageClasses:
  - name: qumulo-site-a
    server: array-a.example.com
    apiUrl: https://array-a.example.com:8000
    secretName: qumulo-token-a
    basePath: /k8s/volumes
    isDefault: true

  - name: qumulo-site-b
    server: array-b.example.com
    apiUrl: https://array-b.example.com:8000
    secretName: qumulo-token-b
    basePath: /k8s/volumes
```

People then choose a cluster by naming a class. Every volume remembers which
cluster it came from, so deletes, resizes and snapshots always go back to the
right array.

!!! note "Rotating a token takes effect immediately"
    Credentials are read fresh on every request and never cached between
    them, so updating a Secret is picked up on the next call. No restart.

## Separate storage and management addresses

Some clusters answer NFS and management traffic on different networks. Set
both:

```yaml
    server: 10.20.4.60                  # storage traffic
    apiUrl: https://10.20.0.10:8000     # management traffic
```

Without `apiUrl`, the driver tries to manage the cluster over the storage
address, which does not answer — so deletes and resizes stop working.

## Qumulo network tenants

A tenant keeps a team on its own network. Needs one share per volume.

```yaml
storageClasses:
  - name: qumulo-finance
    provisioningModel: dedicated-export
    server: 10.20.4.60
    apiUrl: https://10.20.0.10:8000
    tenantID: tenant-finance      # name or number
    allowedClients:
      - 10.20.4.0/24
```

Both addresses have to be right:

- **`server`** must be one of the tenant's own addresses. The driver checks
  this and refuses otherwise — a tenant with NFS turned off is refused too.
- **`apiUrl`** must be the management address. Without it the driver tries to
  manage the cluster over the tenant's storage address, which does not answer,
  so deleting and resizing stop working.

## See free space from Kubernetes

```yaml
sidecars:
  capacity:
    enabled: true
```

```console
$ kubectl get csistoragecapacity
NAME          STORAGECLASS   CAPACITY
csisc-4k2xp   qumulo-a       80Ti
csisc-9m7rd   qumulo-b       4Ti
```

!!! note "This is for looking at, not for scheduling"
    An NFS share is reachable identically from every machine, so there is no
    placement decision for Kubernetes to make. Turn it on to see free space,
    not to change where pods land.

!!! note "Put a quota on each `basePath`"
    Without one, every class on the same array reports the same number — the
    array's free space. A quota per `basePath` makes them meaningful.

## List what the driver created

```yaml
controller:
  inventory:
    enabled: true
```

Lets you reconcile what Kubernetes thinks exists against what is really on
each array. Off by default, because it is the only case where the driver
looks at a cluster no request named.

!!! warning "If one cluster is unreachable, the whole listing fails"
    There is no partial answer — a listing that quietly returned less than
    the truth would be worse than one that fails.
