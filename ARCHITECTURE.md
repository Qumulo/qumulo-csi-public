# Architecture & Development Guidelines

## Project Overview
This repository contains a custom Kubernetes Container Storage Interface (CSI) Driver built in Go. It follows the official Kubernetes SIG Storage specifications, utilizing standard decoupled architecture (Identity, Controller, and Node services) alongside official Kubernetes CSI sidecar containers.

## Build & Toolchain Environment
* **Go Version:** `1.27.4` minimum, enforced by the `go` directive in `go.mod`.
  That directive is the floor: `GOTOOLCHAIN=auto` fetches a matching toolchain
  on an older machine, and `GOTOOLCHAIN=local` fails outright. The Dockerfile's
  build stage is pinned to the same patch release and must be bumped with it.
  A Go bump also needs `make chart-timezones` (the chart's zone list comes
  from the toolchain) and a `GOLANGCI_VERSION` in the Makefile built for the
  new Go -- an older linter cannot read the new export data and `make lint`
  fails with one typecheck error on every package.
* **Helm Version:** `v3.13.0+` required (`--dry-run=server`, used by the upgrade preview, landed in 3.13). **Kubernetes 1.35.0+**, enforced via
  `kubeVersion: ">=1.35.0-0"` in `Chart.yaml` (lowered from 1.36.0 on
  2026-10-13 to support 1.35.x clusters; the `-0` admits distribution suffixes
  like `v1.35.6-eks-...` and must not be dropped). The e2e matrix is 1.35.6
  and 1.36.0 -- the pin and the matrix move together, so neither is lowered
  without the other. Do not raise that floor above 1.36.0 without also passing
  `--kube-version` everywhere the chart is rendered: the pinned Helm's default
  capabilities version is 1.36.0, so a higher floor breaks `helm template`,
  the golden tests and the chart contract tests even though `helm install`
  against a new enough cluster still works.
* **Driver Version:** The release version lives in the `VERSION` file at the repo
  root and nowhere else. `make build`/`make docker-build` read it and stamp it
  into the binary via `-ldflags`, so it is also the image tag and the Helm
  `appVersion`. It is unrelated to the Go version above -- bump it per driver
  release, not per toolchain upgrade. `make version` prints what a build resolves to.
  **`VERSION` is the only name a release has.** Published images carry that tag
  and nothing else -- `0.1.1`, never `0.1.1-<commit>` -- and `make
  chart-package` stamps `VERSION` into both the chart version and the
  appVersion, so a packaged chart is identical to the folder chart and
  defaults to the images of its own release. `chart-version-check` holds
  `Chart.yaml` to `VERSION`. `$(VERSION)-$(GIT_COMMIT)` (`BUILD_TAG`) survives
  for local work only: `make docker-build` applies it beside `VERSION` and
  `make e2e-minikube` installs by it, because a tag carrying a `-` renders
  `IfNotPresent` and a minikube-daemon image cannot be pulled. Nothing pushes
  it.
  The consequence to hold on to: `release.yml` runs `make release` when a
  `v$(VERSION)` tag is pushed, and refuses a `VERSION` that is already
  published -- released tags are never moved. A release is identified by its
  version, so **`VERSION` must be bumped for every release**. The csi-addons
  sidecar has its own `-qumulo.N` tag and is published first by
  `release-sidecar.yml`; `release.yml` refuses to start until the sidecar the
  chart pins is published. What protects a
  node is the chart's pull policy: an empty `image.*.pullPolicy` resolves to
  `Always` for a bare release tag (so a cached older image is never served
  under the same name) and `IfNotPresent` for a digest or a local build
  tag. The commit is still in the binary (`pkg/version.GitCommit`) and in
  `make version`. Customers install the packaged chart that `make release`
  pushes to `oci://ghcr.io/qumulo/qumulo-csi-public/charts`, never the folder.
* **Docker Multi-stage Pattern:** One Dockerfile, one build stage, two named
  runtime targets. There is no default image -- a bare `docker build` would pick
  whichever stage is last -- so always build through `make docker-build`, which
  passes `--target`.
  * Build stage uses `golang:1.27.4`, pinned by digest too (currently Debian
    13 trixie -- the unsuffixed tag follows the official image's default
    distro, so it will move when that default moves; the digest does not, so
    a Go bump changes the tag and the digest together). The binary is unaffected: `CGO_ENABLED=0` and
    `-trimpath` make it byte-identical to one built on `-bookworm`, verified by
    comparing sha256 across both bases.
  * `--target controller` -> `registry.access.redhat.com/ubi9/ubi-micro` (Red
    Hat UBI, for OpenShift certification's preflight `BasedOnUbi`). The
    controller only speaks HTTPS to the Qumulo REST API and touches no
    filesystem, so micro suffices -- but micro ships no CA bundle, and that
    bundle is what verifies a cluster certificate from a public CA, so the
    Dockerfile adds the `ca-certificates` RPM with `dnf --installroot` from
    the full UBI image. `make docker-verify` asserts the bundle is there,
    because an image without it builds and starts cleanly and then fails
    every TLS handshake.
  * `--target node` -> the same `ubi9/ubi-micro` base plus `nfs-utils` and
    `util-linux` (and `setup`, already in micro, for `/etc/services`), added
    with `dnf --installroot`. `nfs-utils` is **not** in the public UBI
    repositories, only RHEL BaseOS, so the node build registers with Red Hat
    for one `RUN` using an activation key: `RH_ORG_ID` and
    `RH_ACTIVATION_KEY` from the environment (CI: repository secrets) or
    `~/.config/qumulo-csi/rh-build.env`, passed as BuildKit secrets and
    unregistered in the same `RUN`, so no credential or entitlement reaches
    a layer. Without them `make docker-build-node` stops with a message;
    fork PRs skip the node image in CI. `tini` is in neither UBI nor RHEL
    (RHEL's `catatonit` is only `podman-catatonit`, which drags in podman),
    so it is compiled from its source tarball, checked by sha256, in a
    `tini` stage. **The node image cannot be distroless:**
    `k8s.io/mount-utils` mounts by exec'ing `mount(8)`, and the kernel needs the
    `mount.nfs` helper, so the image requires a userland. `make docker-verify`
    asserts all five binaries are present, because a node image missing them
    builds cleanly and then fails every `NodePublishVolume`. `tini` is the
    node image's PID 1, with the driver its child, so a mount or umount helper
    orphaned by a killed forced rung is reaped instead of left a zombie;
    the controller image runs no helpers and has none.
  * Root is a *node* requirement (host mounting), not a blanket one, and the
    images now say so: the controller is `:nonroot` with `USER 65532:65532`,
    the node stays root because mounting in the host namespace needs it.
    Privilege and root are separable and the chart treats them separately --
    only the node's driver container is `privileged`, while
    `node-driver-registrar` drops privilege and every capability and
    deliberately *keeps* uid 0, because it writes a socket into the kubelet's
    root-owned `plugins_registry`. A non-root registrar fails to register and
    the DaemonSet still reports healthy, so that one is asserted by
    `TestChartSecurityContextsAreWhatEachContainerNeeds` rather than left to a
    comment.
  * **Images are multi-arch** (`linux/amd64,linux/arm64`, the `PLATFORMS`
    variable). `make docker-build` stays single-arch and local; releases go
    through `make docker-buildx`, which must be one `buildx --push` invocation
    because building each architecture separately and pushing to the same tag
    silently replaces rather than accumulates. `make docker-push` refuses by
    default for that reason. The build stage is pinned to `$BUILDPLATFORM` and
    cross-compiles from `TARGETARCH`; both runtime images need QEMU, because
    the node's runtime stage runs dnf and gcc and the controller's runs dnf (adding
    `ca-certificates`) for the target architecture.
  * **Every base image and every chart sidecar is pinned by digest** -- the
    multi-arch index digest, from `docker buildx imagetools inspect
    <image>:<tag>`, written as `<image>:<tag>@sha256:<digest>`. Change the tag
    and digest together (a stage `FROM` an earlier stage, such as `ubi-micro`,
    inherits its pin). `TestDockerfilePinsEveryBaseImageByDigest` and
    `TestChartPinsEveryImage` fail on a tag-only pin; the chart refuses a
    user-set sidecar image with no tag and no digest (schema) or the tag
    `latest` (`_validate.tpl`). Every pinned image is also CVE-gated: CI's
    `image-scan` (our two images) and `chart-image-scan` (every image in
    `values.yaml`, via `make chart-image-scan`) fail on a fixable HIGH or
    CRITICAL; an image held on purpose goes in
    `test/release/image-scan-exceptions.txt` with a reason and an expiry.
    The runtime stages `dnf upgrade` their root, so a rebuild takes the
    newest errata even while the UBI pin stays put.
  * Each image sets its own `CMD` (`-mode=controller` / `-mode=node`), so no
    deployment inherits the binary's `all` default -- which is for local
    development and would put cluster-wide API credentials on every worker.

## Key Architectural Rules
0. **Stateless multi-cluster:** one deployed controller serves many Qumulo
   clusters. It holds no cluster address and no *standing* API client: it
   builds a client per request from the StorageClass parameters (`server`,
   `apiUrl`) and a credential resolved for that request, and every request
   without those fails rather than falling back to a process-wide credential
   (that fallback was removed). `Probe` therefore reports
   liveness only -- it contacts no cluster. See the chart README's
   ["Serving two clusters from one release"](charts/qumulo-csi-driver/README.md#serving-two-clusters-from-one-release)
   section for what the two addresses are for.

   **Where the credential comes from is a spectrum, and the rule is about the
   far end of it.** Three RPCs are handed no `secrets` map by the CSI spec, and
   they sit at different points:

   * Most RPCs get `secrets` populated by a sidecar from the class's
     `csi.storage.k8s.io/*-secret-name` parameters. Nothing to resolve.
   * `GetCapacity` gets the class's parameters verbatim, secret-reference keys
     included, so it resolves the one Secret its own request named. Still
     driven entirely by the request.
   * `ListVolumes` and `ControllerGetVolume` name no Secret. Serving them means
     **listing StorageClasses through the Kubernetes API to discover clusters
     no request mentioned** -- and, for `ListVolumes`, listing this driver's
     PersistentVolumes, so a directory two classes reach is reported under the
     handle its PV carries. That is the line this rule draws, and it is
     crossed deliberately, not accidentally: it is opt-in behind
     `controller.inventory.enabled`, default off, and a controller without it
     behaves exactly as before.

   One PersistentVolume read sits on the near side of that line and is **not**
   opt-in: `DeleteVolume` lists this driver's PVs (a paged List, once per
   delete that has something to remove, shared by two checks) and refuses
   while another PV still in use names the same directory or one below it,
   and unless the released PV carrying the request's handle owns the
   directory -- is named after it, is a `qumulo-csi-repoint` mirror of one
   that is (`csi.qumulo.com/mirrored-from`), or carries the administrator's
   `csi.qumulo.com/delete-directory: "<real path>"` opt-in. It
   discovers no cluster and resolves no credential -- the cluster and the
   Secret are still the request's own -- it only stops a copied handle, or a
   hand-written PV aimed at an administrator's directory, from deleting data
   its PV does not own, which nothing on the array can tell apart. So a
   controller in a cluster always has them, with the `list` on
   `persistentvolumes` the chart grants the provisioner anyway; outside a
   cluster (csi-sanity) there is no lister and both are skipped; a failed
   list refuses the delete
   (`Unavailable`, retried) rather than skipping it.

   PersistentVolumeClaims are read on the same near side, for one purpose:
   a restore's staging directory, `.csi-restoring-pvc-<claim UID>`, is
   removed only once its claim is gone. `DeleteVolume`, finding the
   volume's directory already gone and its own staging sibling there, lists
   claims (a paged List, only when that sibling exists) and leaves it while a
   claim with that UID exists or the list fails; outside a cluster it
   reclaims on the in-process checks alone. And a successful `CreateVolume`
   sweeps its class's store root, at most every 10 minutes per array and
   root, using the request's own client on a context detached from the
   request and bounded (2 s per list, 10 s in all), after the RPC has
   answered: it lists the root and, only if a staging directory is there,
   this driver's PVs and every claim (one paged List each), and removes one
   only when no claim has its UID, no PV has its name, this controller is
   not restoring it, and an earlier sweep found the same directory (by file
   ID) so at least 15 minutes before, holding exactly what it holds now --
   its live tree usage (capacity, files, directories), read per candidate
   with one aggregates call. That last check is what spares a restore on
   another Kubernetes cluster sharing the store root, whose claim this one
   cannot see: a copy in progress changes the usage and starts the 15
   minutes again, so only a restore elsewhere stalled for 15 minutes (an
   array answering 503 that long) can be removed, and it starts over on its
   next attempt. A failed list or usage read removes nothing; no lister
   (outside a cluster) means no sweep. It discovers no cluster -- the
   array and credential are the request's own -- and there is no timer: a
   controller that provisions nothing sweeps nothing. The chart grants `list`
   on `persistentvolumeclaims` to the controller because csi-provisioner
   needs it.

   What stays true in every case: no credential is cached between requests, no
   cluster address is stored, and a rotated Secret takes effect on the next
   call. Hints are kept only to skip work; each is read back and checked, or
   bounded in time, and losing any of them costs one full read:

   * the ID of the export last found or created at a path, read back by ID
     and believed only if it still serves that path;
   * a volume's `csi-<name>` snapshot policy ID, read back by ID and believed
     only if it still carries that name; "no policy" is never remembered, so
     a delete still sweeps an orphan;
   * for a shared-subdirectory class, the export that last covered one of
     its volumes, read back by ID and trusted for 5 minutes from the listing
     that found it -- a deeper export added under the class within
     that window is not seen by CreateVolume's coverage check;
   * a cluster's tenant listing, reused for 5 minutes by CreateVolume's
     tenant checks -- a tenant the listing lacks is read again before
     it is refused, and an export create the array refuses over its tenant
     drops the listing; a rename, or NFS turned off on a tenant, is seen
     within the 5;
   * a group's membership fingerprint, which only lets an unchanged group
     skip re-staging;
   * for `GetCapacity`, that a store root had no quota, trusted for 5 minutes
     so the poll skips two lookups -- a quota added to a store root
     bounds its class within that window;
   * for `GetCapacity`, an array's last successful free-space read, reused
     for 30 seconds by any class on the same endpoint with the same token,
     keyed by a digest of the token and never the token -- a failed
     read is not remembered and a rotated Secret reads afresh;
   * the PersistentVolume list and the StorageClass list a `ListVolumes`
     walk's first page read, reused by that walk's later pages for up to a
     minute -- a first page always reads afresh, and Secrets
     are still read on every call;
   * a promote's freeze-marker snapshot ID per relationship and source root,
     read back by ID and believed only if it is still that relationship's
     live marker;
   * the snapshot last found or taken under a CSI snapshot name, read back
     by ID and adopted only while it still carries the name, is not being
     deleted and is not a policy's -- a name not seen before is listed for;
   * within one planned promote's drain, the source's policy snapshots not
     yet on the target, listed once and read back by ID on later polls;
   * a linked snapshot policy's schedule interval, reused for 5 minutes by
     `GetVolumeReplicationInfo`, which only reports it as a metric;
   * for `GetVolumeReplicationInfo` alone, which acts on nothing it reads: a
     "no export at this path" answer trusted for 5 minutes, and the export
     last found covering a group member, read back by ID. Every
     RPC that acts lists the export table, and an export this driver creates
     clears the "no export" answer at once;
   * for `GetVolumeReplicationInfo` alone, per controller: the relationship
     found at a path (ID and role, read back by ID on both arrays and dropped
     on a 404, a path change or ENDED), trusted for 10 minutes from the last
     read that confirmed it and relisted at least hourly; for a group member,
     for 10 minutes, that it has none of its own and the group root covering
     it; and for a plain volume, for 5 minutes, that it has no
     relationship and no group -- an Enable from this controller
     clears that at once, one from another controller is seen within the 5;
   * for `CreateVolume`'s restore staging sweep, when each array and
     store root was last swept, and each staging directory a sweep found with
     no claim and no PV, by path and file ID, with its tree usage then and
     when it was first seen so with that usage -- a directory made again,
     or written to since, starts again, and losing the record costs 15 more
     minutes before a leftover is removed;
   * a snapshot's file sizes added up, for the restore size check and
     an imported snapshot's reported size: a snapshot never changes, so
     only a walk that covered the whole tree is kept, for an hour, at most
     256 snapshots, keyed by array and snapshot ID;
   * for `ResyncVolume`, per controller, a reconnect it ran and the highest
     revert snapshot ID of the reconnected root before it: the
     resync answers "not ready" until a revert snapshot above that ID carries
     an expiration, for at most 2 minutes, kept an hour at most. Lost with
     the process, the resync answers as before, and `DisableVolumeReplication`
     still stamps any revert snapshot left without an expiration on both
     arrays.

   Every hint map is swept of entries past their use, at most every 10
   minutes, by whichever store comes next; an ID hint unused for an hour is
   dropped, and a deleted volume's export hints go with it.

   What changed: with inventory enabled the controller may *find* a cluster
   rather than only being *told* one. Rule 4 still holds -- this reads
   the API, it never watches it, so there is no informer and no cache.
1. **Snapshot restore is bounded and atomic:** Qumulo has no tree-clone API, so
   `CreateVolume` from a snapshot recreates the tree and copies each file with
   `copy-chunk` -- server-side, but several round trips per entry (create,
   copy, attribute update). A restore therefore (a) is capped (50,000 entries,
   files plus directories, / 2 TiB, decided from the snapshot's own counts
   before anything is copied, then `ResourceExhausted`), (b) returns `Aborted`
   when its per-call budget expires so the CO retries and the next attempt
   resumes -- skipping the subtrees an earlier attempt finished, which the
   controller remembers in memory -- and (c) assembles under
   `.csi-restoring-<name>` and renames
   into place, so a volume is never observable half-restored. **One
   `copy-chunk` call copies only 32 MiB and returns the next request** -- copying
   a file means looping until the API answers null, or larger files truncate
   silently.
2. **`adDomain` is a path namespace, never an access control:** it nests a
   class's volumes under a per-domain subdirectory and nothing more. An NFS
   export maps identities by SID, so a domain name names no principal, and
   Qumulo ignores export user mapping under Kerberos -- so this parameter
   cannot become a permission. `realPathFor` and `exportPathFor` must insert
   the domain **in step** (one without the other provisions cleanly and then
   fails to mount -- including on the restore path, which stages and renames
   under the same root). There is no separate list to keep in step with the
   domain, either: `ListVolumes` reads `managedClass.root`, which already has
   the domain folded in (`realStoreRoot()`), so a class's volumes are listable
   the moment the class itself is. See `pkg/driver/addomain.go` and
   `pkg/driver/inventory.go`.
3. **Decoupled CSI Services:** Keep Identity, Controller, and Node server implementations cleanly separated under internal packages.
4. **No Direct K8s API Watching:** The driver binary should never directly watch the K8s API for volume hooks; rely entirely on official sidecars (`csi-provisioner`, `csi-resizer`, `csi-snapshotter`, `node-driver-registrar`, `livenessprobe`). There is deliberately no `csi-attacher`: this driver publishes no `VolumeAttachment`, so the chart deploys none.
5. **CGO Disabled:** Statically link the binary with `CGO_ENABLED=0` unless a
   storage engine C-library explicitly requires CGO. `GOARCH` is *not* fixed:
   `make build` produces a host-architecture binary for local work, and the
   container build sets `GOOS`/`GOARCH` from BuildKit's `TARGETOS`/`TARGETARCH`
   so one Dockerfile cross-compiles for every entry in `PLATFORMS`. Hardcoding
   `GOARCH=amd64` here would put an amd64 binary in an arm64 image, which fails
   at exec rather than at build.
6. **Binary Hardening:** Always build with `-trimpath` and `-ldflags="-s -w"` to minimize size and strip debugging paths.
7. **Secret Handling:** Cloud provider access tokens must be ingested as `type: Opaque` K8s Secrets, resolved via the gRPC request `Secrets` map payload or read from secure `tmpfs` volume mounts.

## Core Commands

### Development & Verification
* Run local units: `go test -v ./...`
* Run the CSI conformance suite: `make sanity-test`. Runs the official
  `csi-sanity` (pinned v5.6.0) against the real driver binary over a real gRPC
  unix socket, with `cmd/mock-qumulo-server` standing in for the cluster.
  Current result: **52 passed, 0 failed, 50 skipped** (the skips are RPCs this
  driver does not advertise). The history is worth keeping, because both moves
  were capability changes showing up in the numbers rather than conformance
  regressions: it ran 53/49 until `ListVolumes`, `GetCapacity` and
  `ControllerGetVolume` stopped being advertised, which moved four specs from
  passed to skipped; then `GetCapacity` was implemented and `GET_CAPACITY`
  re-advertised, moving one back; then `GET_VOLUME_HEALTH` was advertised,
  moving two more (the node health spec and its InvalidArgument
  case) from skipped to passed. Advertising the controller `GET_VOLUME_HEALTH`
  moves nothing: its specs call `ControllerGetVolumeHealth` with no secrets,
  which the driver answers only through inventory, so `run-test.sh` skips them
  as it skips ListVolumes, for the same reason. That last move also earned its keep
  immediately: the spec calls `NodeGetVolumeHealth` with no
  `volume_publish_path`, which the first cut of the RPC refused -- the field
  is OPTIONAL, and nothing but csi-sanity was exercising that. Which controller
  capabilities are advertised -- and so which specs run rather than skip -- is
  decided in `pkg/driver/list_volumes.go` and `pkg/driver/controller_server.go`;
  the counts above are what `make sanity-test` reports, not a prediction kept
  in a comment. CI reruns this on every PR, so the figure above is checkable
  rather than a remembered one. See [test/sanity/README.md](test/sanity/README.md)
  for what is real and what is faked.
* **CI runs the gates, `make ci` runs them locally.** Nine gates need no
  cluster, no registry and no root -- secret scan, lint, unit+race,
  csi-sanity, helm lint, chart goldens, chart/driver contract, example
  manifests, govulncheck -- and
  `.github/workflows/ci.yml` runs those same Makefile targets on every PR.
  The workflow invokes the targets rather than reimplementing them, because a
  gate that exists only as a hand-run command with a committed result is how
  the numbers in these docs went stale in the first place.
  `make coverage-startup` is one of them: it fails if `cmd/qumulo-csi-driver`
  (flags, `run()`, shutdown) drops below 80% statement coverage.
* Tidy modules: `go mod tidy`
* Format code: `go fmt ./...`
* Lint workspace: `make lint`, or `golangci-lint run` directly. Configured by
  `.golangci.yaml` -- a deliberately small linter set, because a broad one
  produced 88 findings of which most were noise and four actively contradicted
  the CSI spec.

### Build & Packaging
* Local binary compilation: `CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/csi-driver ./cmd/main.go`
* Build container image: `make docker-build` (tags `qumulo-csi-driver:$(cat VERSION)`)
* Lint Helm chart: `make helm-lint` (also fails if `Chart.yaml` appVersion has
  drifted from `VERSION`)
* Dry-run Helm rendering: `make helm-template`
* **Chart/driver contract:** `make helm-verify` renders the chart and feeds its
  StorageClass parameters through the driver's own validation. This is the check
  that matters -- the driver rejects unknown parameters, so a chart emitting one
  key it does not recognize fails every provision, and neither `helm lint` nor
  `go test ./...` would catch it.

## Code Style & Conventions
* **Error Handling:** Avoid generic panics. Use explicit gRPC status errors (`status.Error(codes.InvalidArgument, ...)`) inside `ControllerServer` and `NodeServer` methods to pass descriptive lifecycle errors back to the Kubelet.
* **Context Propagation:** Ensure `context.Context` is passed downstream to all backend storage engine API client calls for proper cancellation management.
* **Helm Values Protection:** Enclose the application version string in quotes inside `Chart.yaml` (`appVersion: "0.1.0"`, taken from `VERSION`) to avoid numeric YAML parsing anomalies.
