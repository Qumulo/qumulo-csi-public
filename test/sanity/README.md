## Sanity tests

Runs the official [`csi-sanity`](https://github.com/kubernetes-csi/csi-test/tree/master/pkg/sanity)
CSI conformance suite against this driver's real binary, over a real gRPC
unix socket -- the same shape as
[csi-driver-nfs's `test/sanity`](https://github.com/kubernetes-csi/csi-driver-nfs/tree/master/test/sanity).

## What's real and what's not

- **Real**: the `qumulo-csi-driver` binary (`make build`), the gRPC/CSI
  protocol handling, request validation, and the Qumulo REST API calls --
  those go over real HTTP to [`mock-qumulo-server`](../../cmd/mock-qumulo-server),
  an in-memory stand-in for a Qumulo cluster's REST API (see its package
  doc comment for what it does and does not simulate).
- **Not real**: there is no NFS server for `NodePublishVolume` to mount
  from. The driver is started with `-fake-mounter`, which swaps in
  [`mount.FakeMounter`](https://pkg.go.dev/k8s.io/mount-utils#FakeMounter)
  in place of a real `mount(8)` call -- see the flag's own `-help` text and
  `driver.Config.FakeMounter`'s doc comment in [`pkg/driver/driver.go`](../../pkg/driver/driver.go).
  Everything up to that final syscall (target path creation, idempotent
  republish detection, unmount/cleanup) is still exercised for real.

## How hard the mock is made to push

A test double can be a valid implementation of a wire format and still let
every interesting branch go unexecuted. Three of the mock's settings decide
that, and `run-test.sh` sets all three deliberately:

| Setting | What it makes real |
|---|---|
| `-token` | Authentication. Left empty the mock accepts anything, so a driver sending no `Authorization` header at all would pass. |
| `-tree-delete-delay=1s` | `DeleteVolume`'s asynchronous branch. Left at 0 every tree delete reports finished on the first poll, so `awaitTreeDelete`'s loop never iterated. This is most of the suite's ~27s runtime, and it is worth it. |
| `-entries-page-size=1` | Directory listing paging -- **aspirationally**. See below. |

Two things this suite still does not reach, stated so the green result is not
read as covering them:

- **The paging loop in `ListDirectory`.** csi-sanity snapshots a volume it has
  just created, which is an empty directory, so the listing returns no entries
  and there is no page boundary to cross regardless of the page size. The flag
  is armed for the day a restore here has content; the loop's real coverage is
  `TestListDirectoryFollowsPaging` in `pkg/qumulo`.
- **Quota error classes.** The mock now answers a missing quota with
  `api_quotas_quota_limit_not_found_error`, which is what a real cluster
  returns and what `pkg/qumulo/errors.go` matches on. It previously returned
  `fs_no_such_quota_error`, a class the driver has never heard of -- harmless
  only because `IsNotFound` also matches on HTTP 404, which masked the
  mismatch entirely.

This trade-off is what keeps the suite self-contained: no Docker, no real
NFS server, no `sudo`, and no system package installs, at the cost of not
proving the actual `mount(8)` invocation works. That invocation is proven
separately by `test/live` (a real cluster) and `test/e2e` (a real minikube
deployment, real kubelet mounts).

## Known, deliberate skips

`run-test.sh` passes `--ginkgo.skip` for the one class of test this driver
cannot pass by design, not by bug: `ListSnapshots` called with **no**
`snapshot_id` or `source_volume_id` filter. This controller can serve many
Qumulo clusters and is never told the full set, so it has no way to answer
"every snapshot everywhere" -- it refuses with `InvalidArgument` rather than
inventing an empty list. See `ListSnapshots`'s doc comment in
[`pkg/driver/controller_server.go`](../../pkg/driver/controller_server.go).

Every other capability this driver does not implement
(`ControllerPublishVolume`, `NodeStageVolume`, `NodeExpandVolume`, ...) is
simply not advertised via `Get*Capabilities`, so `csi-sanity` skips those
specs on its own -- no explicit skip needed for those.

## Run locally

```
make sanity-test
```

This builds the driver and `mock-qumulo-server` (`make build mock-server`),
installs the standalone `csi-sanity` CLI (`go install .../csi-test/v5/cmd/csi-sanity`,
pinned by version in `run-test.sh`, into a scratch `GOBIN` -- this repo's own
`go.mod` is never touched), starts both binaries, runs the suite, and tears
everything down on exit (`trap ... EXIT`), pass or fail.

## Files

- `run-test.sh` -- orchestrates the run; see its own comments for the
  process lifecycle
- `params.yaml` -- the `--csi.testvolumeparameters` StorageClass parameters
  `CreateVolume` is called with. No `secrets` are supplied anywhere in this
  suite, so every request falls back to the driver's process-wide client
  (`-qumulo-server`/`-qumulo-api-url`, pointed at `mock-qumulo-server`)
  regardless of what `server`/`apiUrl` say here -- see `clientFor` in
  `pkg/driver/controller_server.go`. They only need to satisfy
  `parseVolumeParams`'s presence/absolute-path checks.
