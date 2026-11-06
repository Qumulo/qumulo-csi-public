# A complete values file

The other pages in this section show only the `storageClasses` part, to keep
the examples short. This page shows a full `qumulo-values.yaml` so you can see
where that part fits.

```yaml
# qumulo-values.yaml
image:
  controller:
    repository: ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller
    tag: ""                        # leave empty -- see the note below
    pullPolicy: ""                 # leave empty
  node:
    repository: ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-node
    tag: ""
    pullPolicy: ""
  pullSecrets:                     # required: the images are private
    - name: ghcr-pull

qumulo:
  apiTimeout: 10s
  insecureSkipTlsVerify: false     # only set true while evaluating

controller:
  replicaCount: 2
  logLevel: info

node:
  logLevel: info
  # kubeletDir: /var/lib/rancher/k3s/agent/kubelet    # only on k3s / microk8s

storageClasses:
  - name: qumulo-nfs
    server: qumulo.example.com     # just the host name or IP -- no https://, no port
    secretName: qumulo-api-token   # the Secret holding your API token
    basePath: /k8s/volumes         # must already exist and be shared over NFS
    protocol: nfs4.1
    isDefault: false
    allowVolumeExpansion: true
    mountOptions: [hard, "timeo=600"]
```

Install it with:

```bash
helm install qumulo-csi oci://ghcr.io/qumulo/qumulo-csi-public/charts/qumulo-csi-driver \
  --version 0.1.0+<commit> \
  --namespace qumulo-csi \
  --values qumulo-values.yaml
```

!!! note "Leave `tag` empty unless you have a reason not to"
    Left empty, you get the images that belong to the version of the driver
    you are installing, which is what you want nearly always. Set it only if
    you need to lock on to one exact image build, or you are testing an image
    you built yourself — and in that case set `pullPolicy` to `IfNotPresent`
    or `Never` as well.
