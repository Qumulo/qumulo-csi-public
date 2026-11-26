# Build stage. The base image is pinned to the same patch release as the `go`
# directive in go.mod (1.27.4). A floating `golang:1.27` tag can lag behind that
# directive, and GOTOOLCHAIN=auto would then download the required toolchain
# mid-build -- after `go mod download` has already run -- turning a version bump
# into a slower build with a network dependency. Bump this together with go.mod.
#
# Pinned by digest as well as by tag. The tag golang:1.27.4 is re-pushed
# whenever Docker rebuilds it (a new Debian base, a security update), so the
# same Dockerfile could build on different bytes from one day to the next. The
# digest is the multi-arch *index* (linux/amd64, linux/arm64/v8 and others),
# so it does not undo the cross-platform build. On a Go bump, change the tag
# and the digest together: `docker buildx imagetools inspect golang:<ver>`,
# top-level "Digest:" line.
#
# --platform=$BUILDPLATFORM pins this stage to the machine doing the building,
# never the machine being built for. Without it an arm64 target pulls the arm64
# Go image and compiles the whole tree under QEMU -- minutes instead of seconds,
# to produce a binary Go cross-compiles for free.
FROM --platform=$BUILDPLATFORM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS build

# Supplied by BuildKit from --platform. Declared here because an ARG is only in
# scope in the stage that declares it.
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

# Its own layer, before the source is copied, so the module download is cached
# across every build that does not change go.mod or go.sum.
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Build metadata arrives as build args so an image reports the same version the
# Makefile stamps into a local binary; `make docker-build` passes all three. The
# defaults match pkg/version, so a bare `docker build` still yields a working
# binary -- just an unstamped one.
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown
# Go build tags; empty for every real image. `make docker-build GO_TAGS=testhooks`
# builds a lab image with the test hooks in pkg/driver/testhooks_on.go.
ARG GO_TAGS=

# GOOS/GOARCH come from BuildKit's TARGETOS/TARGETARCH rather than being left
# unset. Unset was the thing that forced emulation: with the build stage pinned
# to BUILDPLATFORM above, Go must be told what to target or it would produce a
# host-architecture binary and drop it into a foreign runtime image, which fails
# at exec with "exec format error". CGO_ENABLED=0 and -trimpath keep the output
# reproducible per architecture.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
      -trimpath \
      -tags "${GO_TAGS}" \
      -ldflags="-s -w \
        -X qumulo-csi-driver/pkg/version.DriverVersion=${VERSION} \
        -X qumulo-csi-driver/pkg/version.GitCommit=${GIT_COMMIT} \
        -X qumulo-csi-driver/pkg/version.BuildDate=${BUILD_DATE}" \
      -o /out/qumulo-csi-driver ./cmd/qumulo-csi-driver

# ---------------------------------------------------------------------------
# Runtime stages. There is no default image: both are named targets, and a
# `docker build` without --target would silently build whichever stage comes
# last. Use `make docker-build`, which always passes one.
#
# The two roles get different images because they need different things. The
# controller talks HTTPS to the Qumulo REST API and touches no filesystem, so
# it needs no userland at all. The node service mounts NFS, and
# k8s.io/mount-utils does that by exec'ing mount(8) -- so its image needs one.
# ---------------------------------------------------------------------------

# Controller: Red Hat UBI 9 micro, so the image can pass Red Hat's preflight
# BasedOnUbi check (OpenShift certification). ubi-micro is the smallest UBI and
# enough for a static binary, but unlike the distroless base it replaced it
# carries no CA bundle -- and the controller verifies a cluster with a public
# CA certificate against the image's bundle (qumulo.caBundle and
# openshift.trustedCABundle replace it through SSL_CERT_FILE; without either,
# this bundle is all there is). ubi-micro has no package manager, so
# ca-certificates goes in the way Red Hat documents for micro: the full UBI
# image's dnf installs it into a copy of the micro root, from the public UBI
# repositories, and the result is copied over micro. The package stays in the
# RPM database, so preflight and CVE scanners see it.
#
# Both bases are pinned by version tag and by the digest of the OCI *index*,
# not one platform's manifest, so pinning does not undo the multi-arch build --
# the index covers linux/amd64 and linux/arm64/v8 among others. Bump the two
# together, to the same UBI minor. The node image is built on the same two.
#
# The pinned micro base is a snapshot: its glibc, libgcc and the rest are the
# versions Red Hat shipped on the day it was built, and `dnf install` adds
# packages without upgrading the ones already in the root. So each rootfs
# stage also runs `dnf upgrade` against the copy of the micro root, which
# brings every package -- the base's and the ones just installed -- to the
# newest errata in the repositories at build time. Without it a rebuild
# carries the base's fixable CVEs for as long as the pin stays put (the
# 0.1.1 scan: 17 fixable glibc/libgcc findings in both images, and fixable
# curl/expat highs in the node image).
FROM registry.access.redhat.com/ubi9/ubi-micro:9.9-1794478307@sha256:dbd325b96bcd432dd42816d31412bdf1dc0fb973b79ed310344bc23c76c068db AS ubi-micro

FROM registry.access.redhat.com/ubi9/ubi:9.9-1795197514@sha256:6636b79fba9e5044360366f189333c30b5ce528efd92c40c68b074592b971fcd AS controller-rootfs
COPY --from=ubi-micro / /mnt/rootfs
RUN dnf install -y --installroot /mnt/rootfs --releasever 9 \
      --setopt install_weak_deps=false --nodocs ca-certificates \
 && dnf upgrade -y --installroot /mnt/rootfs --releasever 9 \
      --setopt install_weak_deps=false --nodocs \
 && dnf --installroot /mnt/rootfs clean all \
 && rm -rf /mnt/rootfs/var/cache/* /mnt/rootfs/var/log/dnf* /mnt/rootfs/var/log/hawkey.log

FROM ubi-micro AS controller

COPY --from=controller-rootfs /mnt/rootfs/ /
COPY LICENSE /licenses/LICENSE
COPY --from=build /out/qumulo-csi-driver /qumulo-csi-driver

# Our own identity, replacing the Red Hat labels inherited from the UBI base
# (preflight HasRequiredLabel and HasNoProhibitedLabels).
ARG VERSION=dev
ARG GIT_COMMIT=unknown
LABEL name="qumulo-csi-driver-controller" \
      vendor="Qumulo, Inc." \
      maintainer="Qumulo, Inc." \
      version="${VERSION}" \
      release="${GIT_COMMIT}" \
      summary="Qumulo CSI driver, controller service" \
      description="Controller service of the Qumulo CSI driver: provisions, snapshots and replicates volumes on Qumulo clusters." \
      io.k8s.display-name="Qumulo CSI driver (controller)" \
      io.k8s.description="Controller service of the Qumulo CSI driver: provisions, snapshots and replicates volumes on Qumulo clusters." \
      url="https://github.com/qumulo/qumulo-csi-driver" \
      vcs-ref="${GIT_COMMIT}"

# A uid of the same value distroless :nonroot shipped, stated numerically
# because that is what Kubernetes needs: runAsNonRoot cannot be proved from a
# user *name*, so the kubelet refuses to start a container whose image
# declares one. Numeric here is what lets the chart set runAsNonRoot at all.
# It needs no /etc/passwd entry. On OpenShift restricted-v2 assigns its own
# uid from the namespace range and this one is not used (the chart drops it).
#
# The controller can do this and the node cannot. The controller only speaks
# HTTPS to the REST API and its socket lives in an emptyDir the kubelet creates
# world-writable; the node mounts filesystems in the host's namespace, which is
# a real root requirement rather than an oversight.
USER 65532:65532

ENTRYPOINT ["/qumulo-csi-driver"]
# Make the role explicit per image rather than relying on the binary's own
# default, which is "all" -- a mode meant for local development, and one that
# would have a node pod holding cluster-wide API credentials.
CMD ["-mode=controller"]

# Node: needs a working NFS client, on the same UBI 9 micro base as the
# controller (preflight BasedOnUbi). mount-utils shells out to `mount` and
# `umount` by bare name and the kernel needs the `mount.nfs` helper, so these
# are load-bearing:
#   nfs-utils  -> /usr/sbin/mount.nfs, /usr/sbin/mount.nfs4
#   util-linux -> /usr/bin/mount, /usr/bin/umount
#   setup      -> /etc/services, /etc/protocols (already in ubi-micro)
#   tini       -> /usr/bin/tini, PID 1 (built below; see the ENTRYPOINT)
#
# nfs-utils is not in the public UBI repositories, only in RHEL BaseOS, so
# this stage needs a RHEL subscription. It registers with an activation key
# for exactly one RUN: register, install into a copy of the micro root,
# unregister, clean. The org ID and key arrive as BuildKit secrets, which are
# mounted for that RUN only and never written to a layer, and unregistering in
# the same RUN means neither an entitlement certificate nor a consumer
# identity survives into any layer (and no build leaves a system behind in the
# organisation's inventory). Only /mnt/rootfs is copied into the image; this
# builder's own filesystem is thrown away. Red Hat allows Partner Connect
# members to redistribute RHEL packages in UBI-based images (UBI FAQ #26).
#
# `make docker-build-node` passes the two secrets from RH_ORG_ID and
# RH_ACTIVATION_KEY (or ~/.config/qumulo-csi/rh-build.env); without them the
# build stops at this RUN with "secret rh_org: not found".
#
# nfs-utils pulls in rpcbind, gssproxy, python3 and systemd as hard RPM
# requirements. None of them runs -- the driver is the only process besides
# tini -- but they stay installed, since dropping them with --nodeps would
# leave an RPM database that disagrees with the files and that scanners and
# preflight read. --disablerepo '*' keeps every package to the two RHEL
# repositories, so the image's provenance is one place.
FROM registry.access.redhat.com/ubi9/ubi:9.9-1795197514@sha256:6636b79fba9e5044360366f189333c30b5ce528efd92c40c68b074592b971fcd AS node-rootfs
COPY --from=ubi-micro / /mnt/rootfs
RUN --mount=type=secret,id=rh_org,required=true \
    --mount=type=secret,id=rh_key,required=true \
    set -eu; \
    arch="$(uname -m)"; \
    export SMDEV_CONTAINER_OFF=1; \
    subscription-manager register --org "$(cat /run/secrets/rh_org)" \
      --activationkey "$(cat /run/secrets/rh_key)" \
      --name "qumulo-csi-image-build-$arch" >/dev/null; \
    rc=0; \
    dnf install -y --installroot /mnt/rootfs --releasever 9 \
      --setopt reposdir=/etc/yum.repos.d \
      --disablerepo '*' \
      --enablerepo "rhel-9-for-$arch-baseos-rpms" \
      --enablerepo "rhel-9-for-$arch-appstream-rpms" \
      --setopt install_weak_deps=false --nodocs \
      nfs-utils util-linux \
      && dnf upgrade -y --installroot /mnt/rootfs --releasever 9 \
           --setopt reposdir=/etc/yum.repos.d \
           --disablerepo '*' \
           --enablerepo "rhel-9-for-$arch-baseos-rpms" \
           --enablerepo "rhel-9-for-$arch-appstream-rpms" \
           --setopt install_weak_deps=false --nodocs \
      && dnf --installroot /mnt/rootfs clean all >/dev/null || rc=$?; \
    subscription-manager unregister >/dev/null 2>&1 || true; \
    subscription-manager clean >/dev/null 2>&1 || true; \
    [ "$rc" -eq 0 ]; \
    rm -rf /mnt/rootfs/var/cache/dnf /mnt/rootfs/var/log/dnf* \
      /mnt/rootfs/var/log/hawkey.log /mnt/rootfs/var/log/rhsm \
      /mnt/rootfs/etc/pki/entitlement /mnt/rootfs/etc/pki/consumer

# tini, built from source: it is in neither UBI nor RHEL (Debian packaged it),
# and the RHEL alternative, catatonit, ships only as podman-catatonit, which
# pulls podman and ~100 MB into the image. tini is one C file with no
# dependencies beyond libc, so it is compiled here with UBI's gcc, with the
# hardening flags its own CMakeLists.txt uses, and dynamically linked against
# the glibc the runtime image already has. The tarball is checked against a
# fixed sha256. Same version (0.19.0) the Debian image shipped.
FROM registry.access.redhat.com/ubi9/ubi:9.9-1795197514@sha256:6636b79fba9e5044360366f189333c30b5ce528efd92c40c68b074592b971fcd AS tini
ARG TINI_VERSION=0.19.0
ADD --checksum=sha256:0fd35a7030052acd9f58948d1d900fe1e432ee37103c5561554408bdac6bbf0d \
    https://github.com/krallin/tini/archive/refs/tags/v${TINI_VERSION}.tar.gz /tmp/tini.tar.gz
RUN dnf install -y --setopt install_weak_deps=false --nodocs gcc \
 && mkdir -p /tmp/tini /out \
 && tar -xzf /tmp/tini.tar.gz -C /tmp/tini --strip-components=1 \
 && printf '#define TINI_VERSION "%s"\n#define TINI_GIT     ""\n' "${TINI_VERSION}" > /tmp/tini/src/tiniConfig.h \
 && gcc -std=gnu99 -O2 -Wall -Wextra -Werror -pedantic-errors -Wformat \
      -D_FORTIFY_SOURCE=2 -fstack-protector-strong \
      -Wl,-z,relro -Wl,-z,now -s \
      -o /out/tini /tmp/tini/src/tini.c \
 && cp /tmp/tini/LICENSE /out/LICENSE \
 && /out/tini --version

FROM ubi-micro AS node

COPY --from=node-rootfs /mnt/rootfs/ /
COPY --from=tini /out/tini /usr/bin/tini
COPY --from=tini /out/LICENSE /licenses/tini/LICENSE
COPY LICENSE /licenses/LICENSE
COPY --from=build /out/qumulo-csi-driver /qumulo-csi-driver

# Our own identity, replacing the Red Hat labels inherited from the UBI base
# (preflight HasRequiredLabel and HasNoProhibitedLabels).
ARG VERSION=dev
ARG GIT_COMMIT=unknown
LABEL name="qumulo-csi-driver-node" \
      vendor="Qumulo, Inc." \
      maintainer="Qumulo, Inc." \
      version="${VERSION}" \
      release="${GIT_COMMIT}" \
      summary="Qumulo CSI driver, node service" \
      description="Node service of the Qumulo CSI driver: mounts Qumulo NFS exports into pods." \
      io.k8s.display-name="Qumulo CSI driver (node)" \
      io.k8s.description="Node service of the Qumulo CSI driver: mounts Qumulo NFS exports into pods." \
      url="https://github.com/qumulo/qumulo-csi-driver" \
      vcs-ref="${GIT_COMMIT}"

# tini is PID 1 and the driver its child. umount(8) and mount(8) fork
# helpers; one orphaned by a killed forced rung is re-parented to PID 1, and
# with the driver as PID 1 nothing reaped it, so each such event left a
# zombie for the life of the pod. tini reaps them and forwards
# SIGTERM to the driver. A reaper in the driver itself would race the
# exec.Cmd.Wait of every mount and umount it runs.
ENTRYPOINT ["/usr/bin/tini", "--", "/qumulo-csi-driver"]
CMD ["-mode=node"]
