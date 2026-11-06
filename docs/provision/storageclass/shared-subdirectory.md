# Shared subdirectory — the default

With this model, your storage admin shares one folder on the Qumulo cluster,
and the driver makes a sub-folder inside it for each volume. The driver does
not create any new NFS shares.

This is the simplest option, and the one to use unless you need the extra
control that [a dedicated export](dedicated-export.md) gives you.

```yaml
storageClasses:
  - name: qumulo-nfs
    server: qumulo.example.com     # just the host name or IP -- no https://, no port
    secretName: qumulo-api-token   # the Secret holding your API token
    basePath: /k8s/volumes         # the shared folder, see the note below
    protocol: nfs4.1
    allowVolumeExpansion: true
    mountOptions: [hard, "timeo=600"]
```

!!! warning "Share the folder on Qumulo first"
    `basePath` has to already exist on the cluster **and** already be shared
    over NFS. If it is not, your first volume request fails, and the error
    message names the folder.
