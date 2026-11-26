# Try the driver: a simple, start-to-end example

A minimal, hands-on walkthrough -- install the driver, provision a volume,
write and read a file from a real pod, then grow and clean it up. Each step
is its own file so you can read (and run) them one at a time.

This is deliberately the **simple** path: one StorageClass, one protocol, no
troubleshooting. For everything this glosses over -- every StorageClass
parameter, every chart value, what to do when a step fails 

## What you need before starting

- A Kubernetes cluster (1.36.0+) you can `kubectl apply` to, with NFS kernel
  modules loaded on every worker (`sudo modprobe nfs nfsv3 nfsv4`)
- A Qumulo cluster reachable from that Kubernetes cluster, with:
  - a REST API token that can create directories, set attributes, and
    manage quotas
  - a directory that already exists (e.g. `/csi/volumes`) and is covered by
    an NFS export -- the driver creates volumes *under* it, not the
    directory itself
- `helm` 3.13+ and `kubectl`, both pointed at that cluster
- Access to the driver images on the GitHub Container Registry: a GitHub
  account in the Qumulo organization, and a classic personal access token
  with the `read:packages` scope, SSO-authorized for that organization. Step 1
  turns it into a pull secret.

Don't have a real Qumulo cluster to test against? [`test/sanity/`](../../test/sanity/)
runs the driver's own protocol conformance suite against an in-memory stand-in
instead -- see its README. This walkthrough is for a real deployment.

## The steps, in order

| File | What it does |
|---|---|
| [`01-install.md`](01-install.md) | Install the driver with Helm |
| [`02-secret.md`](02-secret.md) | Create the Qumulo API token as a Kubernetes Secret, from your token file |
| [`03-storageclass.yaml`](03-storageclass.yaml) | A StorageClass naming your Qumulo cluster and path |
| [`04-pvc.yaml`](04-pvc.yaml) | A PersistentVolumeClaim -- this is what actually provisions a volume |
| [`05-pod.yaml`](05-pod.yaml) | A pod that mounts the volume |
| [`06-verify.md`](06-verify.md) | Prove the pod can read and write through it |
| [`07-expand.md`](07-expand.md) | Grow the volume while the pod keeps running |
| [`08-snapshot.md`](08-snapshot.md) | Take a snapshot and restore it as a new volume (optional feature) |
| [`09-cleanup.md`](09-cleanup.md) | Delete everything this walkthrough created |

Everything here lives in one namespace, `qumulo-csi`, created in step 1.
Apply each file in order (`kubectl apply -f <file>` for the `.yaml` ones;
the `.md` ones are short instructions, not manifests). Nothing needs editing
except the two placeholders used throughout, `<qumulo-cluster-address>` and
`<path-that-already-exists-and-is-exported>`, and the token file path in
step 2.
