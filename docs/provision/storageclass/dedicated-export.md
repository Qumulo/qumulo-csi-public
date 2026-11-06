# Dedicated export — one export per volume

With this model, the driver creates a separate NFS share for every volume.
That takes more setup, but it is the only way to control access per volume —
for example, to limit which machines can mount a particular volume.

```yaml
storageClasses:
  - name: qumulo-dedicated
    server: qumulo.example.com
    secretName: qumulo-api-token
    basePath: /k8s/dedicated       # this folder must NOT be shared -- see below
    provisioningModel: dedicated-export
    protocol: nfs4.1
    allowedClients:                # required -- the addresses of your nodes
      - 10.0.0.0/24
    allowVolumeExpansion: true
```

!!! warning "You must say who is allowed to mount"
    If you leave `allowedClients` out, the share is open to any machine that
    can reach the Qumulo cluster, with full admin rights and no password. To
    avoid that, the install stops unless you list the addresses in
    `allowedClients`, or you say you really do want it open with
    `allowAnyClient: true`, or you turn on Kerberos with `securityFlavor`.

!!! warning "Keep the two models in separate folders"
    The two models need opposite things, so they cannot share a folder tree:

    - **Shared subdirectory** needs `basePath` to be shared over NFS.
    - **Dedicated export** needs `basePath` *not* to be shared — and none of
      the folders above it either, if you are using NFSv4.

    Give each model its own folder tree and you will not hit this.
