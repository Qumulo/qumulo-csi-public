# Kerberos (`sec=krb5p`) end-to-end test

Validates NFSv4.1 with Kerberos privacy against a live Qumulo cluster, on a
Minikube VM.

| File | Runs where | Does |
|---|---|---|
| `stage-host.sh` | your host | starts Minikube on a VM driver, stages `krb5.conf` + keytab, starts `rpc.gssd`, validates it |
| `test-manifests.yaml` | — | StorageClass, 5Gi PVC, Alpine pod writing timestamps |
| `run-test.sh` | your host | installs the chart, waits for Bound and Running, asserts the mount **and the export**, then tears everything down |

```sh
KRB5_KEYTAB=./minikube.keytab KRB5_REALM=AD.ENG.EXAMPLE.COM ./stage-host.sh

QUMULO_SERVER=10.0.0.5 \
QUMULO_TOKEN_FILE=~/token.txt \
BASE_PATH=/k8s/krb5p \
CA_BUNDLE_FILE=~/qumulo-ca.crt \
./run-test.sh
```

`KEEP=1` leaves the cluster up for inspection.

## Why a VM driver, not `--driver=docker`

The GSS upcall behind a `sec=krb5p` mount is resolved by the kernel that owns
the mount namespace and answered by `rpc.gssd` in that host's userland. Under
the docker driver the "node" shares your workstation's kernel, so the test would
be measuring your laptop. `stage-host.sh` refuses `docker` and `podman`.

## What the test actually proves

Asserting on the mount option alone would prove almost nothing: NFSv4.1
negotiates the weakest flavour both ends accept, so a client asking for `krb5p`
against a permissive export lands on `sec=sys` and looks healthy. So `run-test.sh`
checks **both ends**:

1. the mount carries `sec=krb5p`, `vers=4.1`, type `nfs4`;
2. the pod can write through it;
3. the export on the cluster reports
   `required_authentication_mode: AUTHENTICATION_MODE_KRB5P`.

Assertion 3 is the one that catches a silent downgrade.

## Prerequisites the scripts cannot supply

- **A real keytab.** `stage-host.sh` refuses to invent one — a placeholder fails
  deep inside the GSS handshake, which is far harder to diagnose than a refusal.
  Export one for `nfs/<vm-hostname>@<REALM>` from your KDC.
- **`idmap_domain` set on the Qumulo cluster.** Unset, every principal maps to
  `nobody`: the mount succeeds and then a pod cannot read its own files. The
  driver preflights this and refuses provisioning, so you will see it as a
  Pending PVC with the reason.
- **A `BASE_PATH` with no ancestor NFS export.** `securityFlavor` requires
  `provisioningModel: dedicated-export`, and NFSv4 forbids an export nested
  inside another. On a cluster exporting `/`, no path beneath it will work.

## Cleanup order

`run-test.sh` tears down on any exit path, in this order: pod → PVC →
StorageClass → Helm release → Secret. The PVC is deleted **with `--wait` before
the driver goes**, because removing the controller first strands PVCs with
nothing to finalize them and they hang in `Terminating` — leaving the directory
and its export behind on the array.
