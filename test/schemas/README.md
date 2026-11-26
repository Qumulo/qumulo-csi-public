# Vendored JSON schemas for `make example-check`

`example-check` validates every manifest under `deploy/` with
`kubeconform`, which needs a JSON schema per `apiVersion`/`kind`. Core
Kubernetes kinds are covered by kubeconform's default schema source, selected
by `-kubernetes-version` (pinned in the Makefile to the chart's
`kubeVersion` floor, 1.35.0). Custom resources are not, so the ones this repo's
examples use live here.

## What is here and why

`snapshot.storage.k8s.io/` -- `VolumeSnapshot` and `VolumeSnapshotClass`,
used by `deploy/use-cases/04-snapshot-restore/`. These are defined by the
external-snapshotter CRDs, which are installed in a cluster rather than being
part of the Kubernetes API, so no released Kubernetes schema set contains them.

## Why vendored rather than fetched

kubeconform can read CRD schemas straight from the datreeio/CRDs-catalog over
HTTPS, and that is where these two came from:

    https://github.com/datreeio/CRDs-catalog
    commit 866b2653a5334db9aed20ad74701e20fd464471b

They are committed instead of fetched for two reasons. The catalog publishes
from a branch, so a URL pointing at it is a dependency on a mutable third-party
ref -- the same reason the GitHub Actions in `.github/workflows/ci.yml` are
pinned to commit SHAs rather than tags. And it drops the gate's third-party
network dependencies from two to one: only the version-pinned core schemas are
still fetched, and a schema set for a released Kubernetes version does not
change. Total cost of vendoring: 11 KB.

## Refreshing

Only needed when the external-snapshotter CRDs change shape, which is rare.
Re-download both files from a newer catalog commit and update the SHA above:

    for k in volumesnapshot volumesnapshotclass; do
      curl -sfL -o "test/schemas/snapshot.storage.k8s.io/${k}_v1.json" \
        "https://raw.githubusercontent.com/datreeio/CRDs-catalog/<sha>/snapshot.storage.k8s.io/${k}_v1.json"
    done

Then run `make example-check` and read the diff before committing it.
