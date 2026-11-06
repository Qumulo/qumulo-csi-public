# Volume Layout Architecture

There are two ways the driver can place volumes on your Qumulo cluster. You
choose per storage class, and the choice decides what access control you can
apply later.

## Shared subdirectory — the default

Your storage admin shares one folder, and the driver makes a sub-folder inside
it per volume. Nothing is added to the cluster's share list.

```yaml
storageClasses:
  - name: qumulo-nfs
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /k8s/volumes       # must already be shared over NFS
    protocol: nfs4.1
```

Use this unless you need per-volume access control. It is simpler, and it
leaves the cluster's share list untouched.

!!! warning "Share the folder on Qumulo first"
    `basePath` has to already exist on the cluster **and** already be shared
    over NFS. If it is not, your first volume request fails, and the error
    message names the folder.

## Dedicated Export

The driver creates a separate NFS share for each volume. This is the only way
to limit which machines can mount a particular volume, to use network tenants,
or to use Kerberos.

```yaml
storageClasses:
  - name: qumulo-dedicated
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /k8s/dedicated     # must NOT be shared
    provisioningModel: dedicated-export
    allowedClients:
      - 10.0.0.0/24
```

!!! warning "A share per volume must say who may mount it"
    Without `allowedClients`, the share admits any machine that can reach the
    Qumulo cluster, with full rights and no password. The install stops unless
    you set it, say `allowAnyClient: true`, or use Kerberos. See
    [Storage Connectivity and Access Control](access.md).

!!! warning "The two models need opposite things, so keep them apart"
    Shared subdirectory needs `basePath` shared over NFS. Dedicated export
    needs it *not* shared — and on NFSv4, none of the folders above it either.
    Give each its own folder tree.

## Folders per AD domain

`adDomain` sorts a class's volumes into a folder per domain, which keeps a
multi-domain estate tidy on the array.

```yaml
storageClasses:
  - name: qumulo-corp
    adDomain: corp.example.com
```

!!! warning "This is layout only"
    It grants nothing and restricts nothing. An NFS share maps identities by
    SID, so a domain name here names no one. Use `allowedClients` or Kerberos
    for actual access control.

## Which should I use?

| | Shared subdirectory | Dedicated Export |
|---|---|---|
| Must `basePath` be shared? | Yes | No — and nothing above it, on NFSv4 |
| Limit who can mount | Not available | Yes |
| Network tenants | Not available | Yes |
| Kerberos | Not available | Yes |
| Entries added to Qumulo's share list | None | One per volume |
| Replicate a single volume | No — the whole class fails over together | Yes |
