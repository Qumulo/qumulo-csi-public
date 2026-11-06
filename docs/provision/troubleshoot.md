# Troubleshoot provisioning

A PVC that does not bind, a volume that will not delete, a resize that does
nothing. Every issue below follows the same shape: **problem, symptoms,
diagnostics, expected output, root cause, resolution, prevention.**

## First, read the message

```bash
kubectl describe pvc <claim-name> | tail -20
kubectl -n qumulo-csi logs -l app.kubernetes.io/component=controller \
  -c qumulo-csi-driver --tail=50
```

The driver's gRPC status and text are passed through verbatim to the PVC's
events. That message is usually the whole diagnosis, including the fix.

### Message index

| Message contains | Cause | Section |
|---|---|---|
| `secrets ... is forbidden` | the Secret is in another namespace than the class expects, or RBAC does not cover it | check the Secret and the token |
| `401 Unauthorized` | token expired or wrong | check the Secret and the token |
| `403 Forbidden` | a missing Qumulo privilege | check the Secret and the token |
| `x509: certificate ... SAN` | TLS | replace the cluster certificate |
| `no credential for cluster ...` | the class names no Secret, or the Secret has no `token` key | check the Secret and the token |
| `managed path "…" does not exist on cluster` | `basePath` does not exist | [basePath missing](#basepath-does-not-exist) |
| `unknown StorageClass parameter(s)` | a typo in a parameter | [Unknown parameter](#unknown-storageclass-parameter) |
| `no NFS export on cluster … covers …` | shared class with no covering export | [No covering export](#no-covering-export) |
| `nfs_export_tree_name_is_prefix_error` | nested export under NFSv4 | [KI-1](#dedicated-export-refused-under-nfsv4) |
| `Unavailable: cluster … is unreachable` | the array did not answer | [Array unreachable](#the-array-did-not-answer) |
| `InvalidArgument: … cannot be reached as configured` | DNS does not resolve, or TLS does not verify | check DNS and the certificate |
| `already exists` on every retry | an interrupted create | [Stuck at already exists](#a-pvc-stuck-at-already-exists) |
| `FailedPrecondition` naming a relationship | the volume is replicated | [Delete refused](#deletevolume-refused) |

---

## Image pull failures

**Problem.** The driver pods never start, so there are no driver logs at all.

**Symptoms.** `ErrImagePull` or `ImagePullBackOff`; provisioning never
happens because the controller is not running.

**Diagnostics.**

```bash
kubectl -n qumulo-csi get pods
kubectl -n qumulo-csi describe pod <pod-name> | grep -A3 -E 'Failed|BackOff'
```

**Root cause**, by message:

| Message contains | Cause |
|---|---|
| `failed to fetch anonymous token` … `401 Unauthorized` | no credential reached the registry: `image.pullSecrets` is unset, or the Secret it names is missing from the release namespace |
| `failed to fetch oauth token` … `403 Forbidden` | the token is wrong or expired, lacks `read:packages`, is not SSO-authorized for the Qumulo organization, or its owner is not a member |
| `not found` | a wrong `repository` or `tag`; a packaged chart asks for `<version>-<commit>`, which exists only for a build `make release` published — one packaged by hand with `make chart-package` may name a build that was never pushed |
| `no match for platform in manifest` | an image copied with `docker pull` + `docker push`, which keeps one platform |

**Resolution.**

```bash
# is the Secret the one the pods name, in the same namespace?
kubectl -n qumulo-csi get deploy,ds -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.template.spec.imagePullSecrets}{"\n"}{end}'
kubectl -n qumulo-csi get secret ghcr-pull -o jsonpath='{.type}{"\n"}'
#    -> kubernetes.io/dockerconfigjson
```

After fixing the Secret, delete the failing pods so they retry at once rather
than at the end of their back-off.

**Prevention.** Note the GitHub token's expiry — the token keeps the driver
*installable*, not running, so this surfaces at the worst moment. Copy images
with `docker buildx imagetools create`.

---

## `basePath` does not exist

**Problem.** The class names a directory that is not on the cluster.

**Symptoms.** Every PVC on the class stays `Pending` with
`managed path "/k8s/volumes" does not exist on cluster`.

**Diagnostics.**

```bash
curl -sk -H "Authorization: Bearer $TOKEN" \
  "https://qumulo.example.com:8000/v1/files/%2Fk8s%2Fvolumes/info/attributes"
```

**Root cause.** The driver provisions *inside* an existing directory. It does
not create the parent.

**Resolution.** Create the directory on the Qumulo cluster, then let the
provisioner retry.

**Prevention.** Verify both prerequisites before installing — see
[Prerequisites](../get-started/prerequisites.md#pre-installation-validation).

---

## No covering export

**Problem.** A `shared-subdirectory` class whose `basePath` is not reachable
through any export.

**Symptoms.** `FailedPrecondition`: `no NFS export on cluster … covers …`.
Nothing was created on the cluster.

**Diagnostics.**

```bash
curl -sk -H "Authorization: Bearer $TOKEN" \
  https://qumulo.example.com:8000/v3/nfs/exports/ \
  | python3 -m json.tool | grep -E 'export_path|fs_path'
```

**Root cause.** A shared-model volume is a directory beneath an export
somebody else owns, so the driver creates no export of its own — it needs one
that already reaches the volume's path. The same check also catches an
`exportBasePath` and `basePath` that have drifted out of step.

!!! note "This check exists because the old behaviour was worse"
    It used to succeed: the PVC went `Bound`, and the pod then failed forever
    with `mount.nfs4: ... No such file or directory` — a healthy volume beside
    a broken workload with nothing connecting them.

**Resolution.** Create an export whose NFS path is `exportBasePath` (or an
ancestor) pointing at `basePath`, before the first PVC. Or use
`provisioningModel: dedicated-export`.

**Prevention.** Check (b) in the prerequisites.

---

## `dedicated-export` refused under NFSv4

**Problem.** Known issue **KI-1**. Open, not fixable in the driver.

**Symptoms.** Every PVC on a `dedicated-export` class stays `Pending`:

```
FailedPrecondition desc = cannot create a dedicated export at "/…/pvc-…":
  qumulo api /v3/nfs/exports/: HTTP 409: nfs_export_tree_name_is_prefix_error.
  An existing export is an ancestor of this path, which NFSv4 does not permit.
```

**Are you affected?** All three must be true:

```bash
# 1. does any class ask for dedicated exports?
kubectl get sc -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.parameters.provisioningModel}{"\n"}{end}'
# 2. is NFSv4 on?
curl -sk -H "Authorization: Bearer $TOKEN" https://<cluster-address>:8000/v2/nfs/settings
#    -> {"v4_enabled": true, ...}
# 3. is an ancestor of basePath exported? Compare export_path, not fs_path.
curl -sk -H "Authorization: Bearer $TOKEN" https://<cluster-address>:8000/v3/nfs/exports/ \
  | python3 -m json.tool | grep -E 'export_path|fs_path'
```

**A cluster that exports `/` always satisfies (3)** — and exporting `/` is a
common default. You are **not** affected if your classes use
`shared-subdirectory`, which is the default.

**Root cause.** NFSv4 presents the whole cluster as a single pseudo-filesystem
tree, so exports may not overlap. This is a constraint of the Qumulo export
model, not a driver defect. NFSv3 has no equivalent restriction.

**Resolution.** Use `shared-subdirectory`. Every other feature behaves
identically — quotas, expansion, snapshots, restore, `ReadWriteMany`, both
protocols. What you give up is per-volume export granularity.

!!! warning "Do not remove the ancestor export to make room unless you know what mounts it"
    Measured: removing an ancestor export while volumes beneath it are mounted
    gives every one of those mounts a **stale file handle at once**. A running
    pod loses its volume mid-write, and a replacement does not recover until a
    covering export exists again — about 90 seconds in the observed case.

    If you must, the order is: (1) create the exports the shared classes will
    need, (2) drain the pods holding mounts beneath the ancestor, (3) only
    then remove it.

**Prevention.** Give each provisioning model its own subtree. Nothing is left
half-created by the refusal: the directory and quota are rolled back and the
export table is untouched.

---

## Unknown StorageClass parameter

**Problem.** A typo in a class parameter.

**Symptoms.** `unknown StorageClass parameter(s)` on every provision — or,
for a chart-rendered class, the install itself fails naming the key.

**Root cause.** The driver rejects unrecognised parameters rather than
ignoring them, and `storageClasses[]` is closed against unknown properties.

!!! tip "This is a feature"
    A dropped key used to leave Helm exiting 0 and `kubectl get sc` showing
    something the operator had not written, with nothing anywhere reporting
    it.

**Resolution.** Fix the key. Because `parameters` is immutable, give the class
a new name or delete the old one once nothing uses it.

**Prevention.** The [parameter reference](../reference/storageclass-parameters.md)
is the complete list of what a class accepts.

---

## The array did not answer

**Problem.** A transport failure reaching the Qumulo REST API.

**Symptoms.** `Unavailable: cluster … is unreachable`. The provisioner
retries.

**Diagnostics.**

```bash
kubectl run netcheck --rm -it --restart=Never --image=busybox:1.36 -- \
  nc -zv qumulo.example.com 8000
```

**Root cause.** A timeout, a refused connection, or a DNS timeout. The attempt
stops there and the retry adopts whatever it left.

!!! warning "Two transport failures are final, not retried"
    `InvalidArgument: … cannot be reached as configured` means either the
    endpoint's name does not resolve (NXDOMAIN, not a DNS timeout), or its TLS
    certificate does not verify. No retry can fix those — correct `server`,
    `apiUrl`, or the CA bundle.

**Prevention.** Alert on `csi_qumulo_api_reachable == 0`
(`QumuloCSIArrayUnreachable`).

---

## A PVC stuck at `already exists`

**Problem.** Defect **D-60**. A `CreateVolume` interrupted between creating
the directory and setting its quota.

**Symptoms.** The PVC stays `Pending` **indefinitely**, failing `already
exists` on every retry — not just the one after the interruption.

**Root cause.** The retry finds the directory there with no quota, refuses to
adopt it as not driver-provisioned, and fails. No quota means this driver did
not provision it, and adopting an arbitrary directory would be worse.

**Resolution.** On the array, either remove the leftover directory (named
`pvc-<uid>`, matching the PVC's UID) or give it a quota. Then delete and
recreate the PVC.

```bash
kubectl get pvc <claim-name> -o jsonpath='{.metadata.uid}{"\n"}'
# look for pvc-<uid> under basePath on the cluster
```

**Prevention.** None available in 0.1.0; this is the known gap in otherwise
idempotent creation.

---

## `DeleteVolume` refused

**Problem.** A PV stays `Released` or a PVC stays `Terminating`.

**Symptoms and causes.**

| Refusal | Cause | Resolution |
|---|---|---|
| names a replication relationship | the volume is a replication source or replica | disable replication first, or set `replicationDeletePolicy: disable` in the **delete Secret** |
| names a promoted array | a stale source after an unplanned failover | demote or reprotect the old side; no policy overrides this |
| names another PV using the directory | another PV in use names the same directory or one below it | remove that claim first |
| the PV does not own its directory | a hand-written or imported PV | add `csi.qumulo.com/delete-directory: "<real-path>"` |
| `FailedPrecondition`, handle not read | an old `<server>/<fileID>/<name>` handle | delete the PVC on a build that understands it, or set the PV to `Retain` and clean up on the array |
| `FailedPrecondition` naming replication privileges | the token lacks the two replication READ privileges | add them |

!!! note "A volume counts as deleted only when the cluster reports it absent"
    A PV is never dropped while its directory still exists — except for a
    handle this driver cannot parse at all, which is answered OK because
    refusing would strand the PV forever (D-58). That handle's directory is
    left on the array.

---

## A StorageClass change seems ignored

**Problem.** The class was edited and nothing changed.

**Root cause.** StorageClass parameters are copied into the PV at provision
time. Editing the class changes nothing for volumes that already exist — only
for new ones. (And most fields cannot be edited at all: `parameters` is
immutable to the API server.)

**Resolution.** Create a new class and migrate workloads to it.

---

## Next

- [Troubleshoot Guide](../operate/troubleshoot-mounts.md)
- [FAQ](../reference/faq.md)
