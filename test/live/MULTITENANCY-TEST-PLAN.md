# Test plan — many StorageClasses, one cluster and many

Two scenarios, sharing one harness:

- **S1** one Qumulo cluster, several StorageClasses, each with **its own
  directory and its own access token**
- **S2** several Qumulo clusters, several StorageClasses

S1 is executable today. S2 needs a second cluster, which does not exist yet, so
it is specified to run unchanged when one appears — plus what can be approximated
without it.

Related: the chart README's
[Serving two clusters from one release](../../charts/qumulo-csi-driver/README.md#serving-two-clusters-from-one-release)
for the two addresses and how a credential reaches an RPC, and rule 0 of
[../../ARCHITECTURE.md](../../ARCHITECTURE.md) for the stateless multi-cluster model these
scenarios exercise.

---

## What is already proven, and what these scenarios add

M4.1 established the mechanism: two classes, two Secrets, a deliberately invalid
token in one — class A bound, class B failed 401, and A was unaffected.

That proved **routing**. It did not prove **isolation**, because both classes used
the same real path and the same identity. The distinction matters:

| Proven | Not yet proven |
|---|---|
| each class uses the credential it names | a class's token **cannot** reach another class's directory |
| a bad credential fails only its own class | quota and delete act only within the owning class |
| the PV records the class's server and paths | a token with reduced privilege fails the *specific* operation it lacks |

S1 is about the right-hand column. It needs genuinely different identities, not
one token used twice.

---

## Prerequisites

### Current cluster state — **revised, NFSv4 re-enabled 2026-08-26**

| | |
|---|---|
| NFS | **`v4_enabled: true`** — `nfs4.1` works again (verified: a pod mounted `vers=4.1`), and the preflight allows it. Both protocols are testable, so every tenant below gets **one class per protocol** |
| exports | one: `/qumulo-nfs41-csi` → `/root`. `basePath` lives under `/root`, `exportBasePath` under `/qumulo-nfs41-csi` |
| store path | `/root/csi-live-test/volumes` |
| **`dedicated-export`** | **blocked again.** With v4 on, the ancestor export makes nested exports illegal — verified directly: `Export '/qumulo-nfs41-csi' is a prefix of '/qumulo-nfs41-csi/csi-live-test'` |

The v4 state has now flipped three times during this work, so every expectation
below that depends on it is labelled. The two that do are `dedicated-export`
(S1.4.3) and the protocol matrix (S1.1, S2.2.6).

### The identities S1 needs — and they can be created through the API

Verified present on this cluster:

```
POST /v1/users/                      {name, primary_group, uid, home_directory, password}
POST /v1/auth/access-tokens/         {user: {...}, expiration_time}
GET  /v1/auth/roles/                 existing roles and their privileges
POST /v1/auth/roles/                 a custom role
POST /v1/auth/roles/{role}/members   assign a user
GET  /v1/auth/privileges/            the full privilege list
```

So "different user access tokens" is a real, testable configuration rather than
two copies of one token.

**The minimum privileges a CSI token needs**, from the cluster's own privilege
list:

| Privilege | Why the driver needs it |
|---|---|
| `PRIVILEGE_FS_ATTRIBUTES_READ` | read directory attributes, resolve paths |
| `PRIVILEGE_FS_SETTINGS_WRITE` | set `mountPermissions` (mode `0777`) on a new volume |
| `PRIVILEGE_FS_DELETE_TREE_WRITE` + `_READ` | `DeleteVolume` uses tree-delete, and polls its status |
| `PRIVILEGE_QUOTA_READ` + `_WRITE` | create, read and expand a volume's quota |
| `PRIVILEGE_NFS_EXPORT_READ` | find the export serving a volume (needed even in `shared-subdirectory`) |
| `PRIVILEGE_NFS_EXPORT_WRITE` | **only** for `provisioningModel: dedicated-export` |
| `PRIVILEGE_SNAPSHOT_READ` + `_WRITE` | only when snapshots are enabled |

That table is itself worth validating: a role granting exactly this should
provision, and dropping any one entry should fail the corresponding operation and
nothing else. It is the difference between "we think these are the privileges"
and knowing.

---

## S1 — one cluster, many classes, separate directories and tokens

### S1.0 Fixture

Three tenants, so "leaks into another" has more than one direction to fail in.

| Class | `basePath` (filesystem) | `exportBasePath` | Protocol | Identity | Secret |
|---|---|---|---|---|---|
| `qumulo-team-a-nfs3` | `/root/csi-live-test/team-a` | `/qumulo-nfs41-csi/csi-live-test/team-a` | `nfs3` + `nolock` | `csi-team-a` | `qumulo-team-a` |
| `qumulo-team-a-nfs41` | same as above | same as above | `nfs4.1`, no lock option | `csi-team-a` | `qumulo-team-a` |
| `qumulo-team-b-nfs41` | `/root/csi-live-test/team-b` | `/qumulo-nfs41-csi/csi-live-test/team-b` | `nfs4.1` | `csi-team-b` | `qumulo-team-b` |
| `qumulo-readonly` | `/root/csi-live-test/team-a` | same as team-a | `nfs4.1` | `csi-observer` (role without `QUOTA_WRITE`) | `qumulo-observer` |

Team A gets **both protocols against one directory and one identity**, which
isolates "the protocol differs" from "the tenant differs". Without that pairing,
a v4.1-only failure could be mistaken for a tenancy problem.

The third class deliberately points at team-a's directory with a weaker
identity. That is how a *privilege* failure is told apart from a *path* failure —
without it, every negative result could be explained by either.

Directories are created by an administrator, each owned by its own identity,
mode `0755` so the driver's own `0777` on volume directories is what makes pods
able to write.

### S1.1 Provisioning isolation

| # | Test | Expected |
|---|---|---|
| S1.1.1 | one PVC per class | each binds; each PV's `volumeAttributes.realPath` is under **its own** `basePath` |
| S1.1.1b | the two team-a classes (nfs3 and nfs4.1) | both bind into team-a's directory; each pod's `/proc/mounts` shows the version its class asked for. Same tenant, same path, different protocol |
| S1.1.2 | after all three, list each `basePath` on the cluster | team-a holds its own volumes only; team-b holds its own; **no directory appears under the wrong parent** |
| S1.1.3 | the PV handle | `<server>#<apiUrl>#<realPath>` — same server and endpoint for all three, distinct paths; no file IDs |
| S1.1.4 | quotas | each volume's quota equals its own request, read back by file ID |

### S1.2 Credential isolation — the substance of S1

| # | Test | Expected |
|---|---|---|
| S1.2.1 | replace team-b's Secret with team-a's token, provision on `qumulo-team-b` | **records what happens.** If team-a's identity can write team-b's directory, isolation rests on nothing but the path in the class — worth knowing explicitly |
| S1.2.2 | give team-b's Secret a token whose role lacks `QUOTA_WRITE` | `CreateVolume` fails **after** the directory is created, at the quota step, and rolls the directory back. The error must name the quota, not something generic |
| S1.2.3 | corrupt team-b's token; provision on both A and B | B fails `401`; **A keeps provisioning throughout** (re-confirms M4.1 with real identities) |
| S1.2.4 | a token valid for the cluster but with **no** `FS_DELETE_TREE_WRITE`; delete a PVC | `DeleteVolume` fails and the PV stays `Terminating` rather than the PV disappearing while the directory survives |
| S1.2.5 | expand a PVC whose class Secret lacks `QUOTA_WRITE` | `ControllerExpandVolume` fails with a credential error, and the old quota is unchanged |

S1.2.4 is the one with a data-consequence: a PV that disappears while its
directory remains is a silent leak, and the opposite — refusing to delete — is
the safe failure.

### S1.3 Operations use the owning class's credential

The controller resolves a credential per request, so each RPC has to be checked
separately: `DeleteVolume` and `ControllerExpandVolume` get **no** StorageClass
parameters, only a volume ID and secrets.

| # | Test | Expected |
|---|---|---|
| S1.3.1 | expand a team-a PVC while team-b's Secret is invalid | succeeds — one class's broken credential must not affect another's expansion |
| S1.3.2 | delete a team-b PVC while team-a's Secret is invalid | succeeds, for the same reason |
| S1.3.3 | delete a class, then delete a PVC created from it | still succeeds: the secret reference was recorded on the PV at provision time, not re-read from the class |

S1.3.3 is the subtle one. It is why `provisioner-secret-name` must not be
templated on PVC identity — a template that cannot be resolved after the PVC is
gone makes every volume undeletable.

### S1.4 Shared parent, two classes

Two classes on the **same** `basePath` but different quotas and identities —
a realistic "two teams, one project directory" arrangement.

| # | Test | Expected |
|---|---|---|
| S1.4.1 | provision from both | both bind; both directories sit under the one parent |
| S1.4.2 | delete one | the other is untouched, and the parent survives |
| S1.4.3 | `dedicated-export` on one of them | **refused** with `FailedPrecondition` naming `/qumulo-nfs41-csi` as the ancestor, and the export table unchanged. Confirmed directly against the API on 2026-08-26 |
| S1.4.4 | the same class with `exportBasePath` moved **outside** any export, e.g. `/csi-dedicated/team-a` | the per-volume export is created, the pod mounts it, and `DeleteVolume` removes it. This is the only layout in which `dedicated-export` works while v4 is on |

S1.4.3's expectation has now flipped twice with the cluster's v4 setting. S1.4.4
is the more useful test: it establishes the layout under which the feature is
actually usable, rather than re-confirming the refusal. Note it needs the two
provisioning models on **disjoint** export subtrees — `shared-subdirectory` wants
its parent exported, `dedicated-export` wants it unexported, and under NFSv4 they
cannot share one.

---

## S2 — many clusters, many classes

Structurally the same fixture with `server` and `apiUrl` differing per class.
Unit tests already prove the routing; what a second cluster adds is proof that
two genuinely independent systems stay independent.

### S2.1 Blocked, and honestly so

**No second Qumulo cluster exists.** Until one does, S2 cannot be run, and any
approximation must be labelled as such:

| Approximation | Proves | Does **not** prove |
|---|---|---|
| two addresses for one cluster (IP and DNS, or one via a TLS proxy) | the address in a class is the address used | that two clusters are isolated — the data is one filesystem |
| the unit fan-out test (`TestCreateVolumeRoutesToTheClusterTheRequestNames`) | the endpoint and token come from the request | anything about real clusters |

### S2.2 The matrix, for when a second cluster exists

| # | Test | Expected |
|---|---|---|
| S2.2.1 | class A → cluster A, class B → cluster B | each volume exists on **its own** cluster and is **absent** from the other, checked through each cluster's own API |
| S2.2.2 | pods on both, each writing | bytes verified through the respective cluster's REST API |
| S2.2.3 | expand on A | A's quota moves; B's is untouched |
| S2.2.4 | cluster B unreachable | only B's PVCs fail; A keeps provisioning and mounting |
| S2.2.5 | B's token placed in A's Secret | A fails `401`; **no operation lands on B** — the sharpest cross-cluster test |
| S2.2.6 | different protocols per cluster (A `nfs3`, B `nfs4.1`) | both mount correctly. Requires v4 on B — and note the two clusters may differ, so the preflight must consult **each** cluster's own settings, not a shared cached value. That per-cluster caching is unit-tested; this is the live confirmation |
| S2.2.7 | snapshot on A, restore on B | refused with `InvalidArgument` naming both clusters — already unit-tested, worth confirming live |
| S2.2.8 | `apiUrl` on a non-default port for one cluster | that class provisions; the other is unaffected |
| S2.2.9 | teardown | both clusters return to zero volumes; neither has leftovers from the other's classes |

S2.2.5 is the test a shared client cannot pass. S2.2.7 is the one where a wrong
answer means silent data loss — an empty volume presented as a restore.

---

## Harness

Extend `test/e2e` rather than starting over. `TestE2EProtocolMatrix` already
loops StorageClasses and verifies through the REST API; these scenarios are that
loop with a per-case endpoint, token and base path. Concretely:

1. `config` grows from one cluster to a slice of `{name, apiURL, tokenFile, basePath, exportBasePath, secretName}`.
2. A `tenant` fixture creates the directory, user, role, token and Secret, and
   registers teardown for each **before** asserting anything.
3. Verification always goes through the tenant's **own** client, so a test cannot
   accidentally confirm isolation using an over-privileged token.

Point 3 is easy to get wrong and would invalidate every negative result.

## Cleanup obligations

Heavier than earlier phases, because these tests create cluster **configuration**,
not just data:

| Object | Removed by |
|---|---|
| volumes and quotas | the driver, via PVC deletion; verified by listing each `basePath` |
| tenant directories | the test, by tree-delete |
| access tokens | `DELETE /v1/auth/access-tokens/{id}` — **a leaked token is a live credential** |
| users and custom roles | `DELETE /v1/users/{id}`, `DELETE /v1/auth/roles/{role}` |
| Kubernetes Secrets, classes, namespaces | the test |
| NFS exports (if S1.4.3 ran) | assert the table returns to its prior count |

Access tokens are the item worth being strict about: everything else is
recoverable, a forgotten token is a standing grant.

## Risks

| Risk | Mitigation |
|---|---|
| a test "proves" isolation using an admin token | verification goes through each tenant's own client (harness point 3) |
| creating users/roles is a cluster-config change | confine names to a `csi-test-` prefix; delete in teardown; get the cluster owner's agreement before the first run |
| a leaked access token outlives the test | enumerate and delete tokens in teardown, and assert the list is empty |
| NFSv4 state changes mid-plan | it has changed three times already. Read `/v2/nfs/settings` at the start of every run and log it; the e2e suite already skips v4.1 cases when v4 is off. The two v4-dependent expectations are S1.4.3/S1.4.4 and S2.2.6 |
| the preflight caches `v4_enabled` for 5 minutes | after flipping the cluster setting, either wait out the TTL or restart the controller before asserting the new behaviour |
| S1.2.1 shows tokens can cross directories | that is a **finding**, not a failure — isolation would then rest on the class's path alone, which the docs must say plainly |
| a partial fixture leaves an unusable tenant | create directory → user → role → token → Secret in that order, and tear down in reverse |

## Definition of done

- Each class provisions **only** into its own `basePath`, verified on the cluster.
- A credential failure in one class provably does not affect another, with real
  distinct identities rather than one token used twice.
- Delete and expand each use the owning class's credential, including after the
  StorageClass is deleted.
- The least-privilege table above is validated: the listed set provisions, and
  removing any entry fails exactly the matching operation.
- S1.2.1's answer is written down either way — whether a token can reach another
  class's directory is the single most important thing an operator planning
  multi-tenancy needs to know.
- Zero users, roles, tokens, directories or exports left behind.
