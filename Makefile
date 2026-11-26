MODULE   := qumulo-csi-driver
BINARY   := qumulo-csi-driver
CMD_DIR  := ./cmd/qumulo-csi-driver
MOCK_DIR := ./cmd/mock-qumulo-server

# VERSION is authoritative: the release version lives in the VERSION file at
# the repo root rather than in git tags, so a build is reproducible from a
# checkout alone and the same string can feed an image tag and a Helm
# appVersion. An empty value would silently ship an unversioned binary, so a
# missing or unreadable file is a hard error instead of a default.
VERSION    ?= $(shell cat VERSION 2>/dev/null)
ifeq ($(strip $(VERSION)),)
$(error VERSION file is missing or empty)
endif
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

# BUILD_TAG names one local build: the release version plus the commit. It is
# NOT published. Released images carry the release version alone -- `0.1.1`,
# never `0.1.1-<commit>` -- because a release is identified by its VERSION and
# a commit in the tag was a second name for the same thing.
#
# It stays for local work, where two builds of one VERSION do coexist:
# `docker-build` applies it beside $(VERSION), and `e2e-minikube` installs by
# it, because a tag carrying a "-" renders pullPolicy IfNotPresent and an
# image built inside minikube's daemon cannot be pulled.
#
# What it used to buy, and what now has to come from elsewhere: with every
# published image called $(VERSION), a node with a cached $(VERSION) can run
# an older binary than the chart expects. The chart answers that
# by pulling a bare release tag Always -- see the pullPolicy helper. That
# holds only while one VERSION means one published build, so **VERSION must be
# bumped for every release published from main**.
BUILD_TAG  ?= $(VERSION)-$(GIT_COMMIT)

LDFLAGS := -X $(MODULE)/pkg/version.DriverVersion=$(VERSION) \
           -X $(MODULE)/pkg/version.GitCommit=$(GIT_COMMIT) \
           -X $(MODULE)/pkg/version.BuildDate=$(BUILD_DATE)

# Controller and node ship as separate images: the node needs an NFS userland
# the controller has no use for. Set REGISTRY to push somewhere.
REGISTRY         ?=
IMAGE_PREFIX     := $(if $(REGISTRY),$(REGISTRY)/,)
IMAGE_CONTROLLER ?= $(IMAGE_PREFIX)$(BINARY)-controller
IMAGE_NODE       ?= $(IMAGE_PREFIX)$(BINARY)-node

# GO_TAGS adds Go build tags to `make build` and `make docker-build`. Empty
# for every real build. `GO_TAGS=testhooks` compiles in the lab-only test
# hooks (pkg/driver/testhooks_on.go, e.g. QUMULO_CSI_TEST_PAUSE_INFO_FENCE
# for the Info fence pause); docker-buildx, the release path, refuses any tag.
GO_TAGS ?=

DOCKER_BUILD_ARGS := --build-arg VERSION=$(VERSION) \
                     --build-arg GIT_COMMIT=$(GIT_COMMIT) \
                     --build-arg BUILD_DATE=$(BUILD_DATE) \
                     --build-arg GO_TAGS=$(GO_TAGS)

.PHONY: build
build:
	go build -tags '$(GO_TAGS)' -ldflags '$(LDFLAGS)' -o bin/$(BINARY) $(CMD_DIR)

.PHONY: mock-server
mock-server:
	go build -o bin/mock-qumulo-server $(MOCK_DIR)

# Runs the official kubernetes-csi/csi-test csi-sanity conformance suite
# against the real driver binary and mock-qumulo-server; see
# test/sanity/README.md for exactly what is real and what is faked.
.PHONY: sanity-test
sanity-test: build mock-server
	./test/sanity/run-test.sh

# Print the resolved build metadata. CI reads this rather than re-deriving the
# version, so there is one definition of what a build is called. "published"
# is the tag a release puts on both images and on the chart; "local" is the
# extra tag docker-build applies, which is never pushed.
.PHONY: version
version:
	@echo "version:   $(VERSION)"
	@echo "commit:    $(GIT_COMMIT)"
	@echo "published: $(VERSION)"
	@echo "local:     $(BUILD_TAG)"
	@echo "built:     $(BUILD_DATE)"

# TEST_TAGS is what `make test` and `make coverage` build with. `defects`
# compiles in every TestDefect_* (pkg/driver, cmd/*, test/doccheck): without
# it CI built none of them, so a fixed defect could come back unnoticed.
# Deliberately not here: `knownlimits` (the table reads accepted as a known
# limitation, which fail by design; run them by hand with `-tags
# defects,knownlimits`) and `chart` (needs the CI-pinned helm; the chart job
# runs the chart tests through helm-golden, and the ones tagged
# `chart && defects` run with `-tags chart,defects`).
# TestDefectSuiteIsPartOfMakeTest (test/doccheck) fails if this target stops
# passing `defects`.
TEST_TAGS ?= defects

.PHONY: test
test:
	go test -tags '$(TEST_TAGS)' ./... -race -count=1

.PHONY: coverage
coverage:
	go test -tags '$(TEST_TAGS)' ./... -race -count=1 -coverprofile=coverage.out
	go tool cover -func=coverage.out

# The driver's startup wiring -- flags, run(), shutdown, all in
# cmd/qumulo-csi-driver -- must stay at least STARTUP_COVERAGE_MIN percent
# covered. It sat at 43.6% while every RPC behind it was tested, because
# run() was only ever reached from a child process whose coverage is not
# collected.
STARTUP_COVERAGE_MIN ?= 80

.PHONY: coverage-startup
coverage-startup:
	go test -tags '$(TEST_TAGS)' ./cmd/qumulo-csi-driver/ -race -count=1 -coverprofile=coverage.startup.out
	@go tool cover -func=coverage.startup.out | awk -v min=$(STARTUP_COVERAGE_MIN) '/^total:/ { \
		pct = $$3; sub("%", "", pct); \
		if (pct + 0 < min) { printf "coverage-startup: cmd/qumulo-csi-driver at %s%%, below the %s%% floor\n", pct, min; exit 1 } \
		printf "coverage-startup: cmd/qumulo-csi-driver at %s%% (floor %s%%)\n", pct, min }'

# Pinned so a run today and a run in a year use the same linter. An unpinned
# @latest lets a new release fail the build with no code change. Check it on
# every Go bump, like chart-timezones below: a linter older than the go.mod
# toolchain cannot read its export data and fails every package with one
# typecheck error (Go 1.27 needed v2.15.0; v2.13.2 stopped at format 4).
GOLANGCI_VERSION ?= v2.15.0

# Installs the pinned linter if it is missing, then runs it. The linter set is
# deliberately small -- see .golangci.yaml and docs/GOLANGCI-PLAN.md.
.PHONY: lint
lint:
	@command -v golangci-lint >/dev/null || \
	  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)
	golangci-lint run ./...

.PHONY: lint-fix
lint-fix:
	golangci-lint run --fix ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: fmt
fmt:
	gofmt -l -w .

# Pinned like every other tool in this file. Note what the pin does and does
# not fix: the binary is fixed, but the vulnerability database it queries is
# live, so this gate can turn red with no change to the tree. That is the
# point of it -- a new CVE in a dependency is news whether or not anyone
# committed that week -- so it runs as its own CI job rather than buried
# among gates that only move when the code does.
GOVULNCHECK_VERSION ?= v1.7.0

# `go run` rather than `go install`: nothing here needs the binary on PATH,
# and this way the pinned version is the only version that can run.
.PHONY: govulncheck
govulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

# The same idea for the third-party images the chart deploys: every image
# pinned in values.yaml (sidecars, optional ones included, and the helm test's
# volume image) is scanned by Trivy and the gate fails on a HIGH or CRITICAL
# finding that has a fix. Images held on purpose are listed, with an expiry,
# in test/release/image-scan-exceptions.txt. Needs docker and the network, so
# it is not part of `ci`; it runs as its own CI job. Our own two images are
# gated by the image-scan job, which builds them.
.PHONY: chart-image-scan
chart-image-scan:
	./test/release/scan-chart-images.sh

# A Qumulo bearer token reached a branch of this repository in a plain
# `token.txt` (performance_benchmarking, 2026-10-01) and was found by hand two
# weeks later: nothing in the pipeline looked at what a commit adds. This gate
# does. It scans commits, not the working tree, because a secret that a later
# commit deletes is still in the history anyone can fetch.
#
# Pinned here and nowhere else, like every other tool in this file; `go run`
# for the same reason as govulncheck. The rules are gitleaks' defaults plus
# .gitleaks.toml, whose allowlist names the few fake tokens tests use -- by
# value, never by commit or path, so a real secret next to one is still found.
#
# SCAN_RANGE is a git revision range (e.g. origin/release_0_1_0..HEAD). Empty,
# the default, scans every commit reachable from HEAD, which is what `make ci`
# runs. CI passes the PR's or the push's own range so a finding names the
# change that introduced it. --redact keeps the secret out of the log.
GITLEAKS_VERSION ?= v8.21.2
GITLEAKS = go run github.com/zricethezav/gitleaks/v8@$(GITLEAKS_VERSION)

.PHONY: secret-scan
secret-scan:
	$(GITLEAKS) git --config .gitleaks.toml --redact --no-banner \
	  $(if $(SCAN_RANGE),--log-opts="$(SCAN_RANGE)",) .

# The same rules over what is staged, for a local pre-commit hook:
#   printf '#!/bin/sh\nexec make secret-scan-staged\n' > .git/hooks/pre-commit
#   chmod +x .git/hooks/pre-commit
.PHONY: secret-scan-staged
secret-scan-staged:
	$(GITLEAKS) git --pre-commit --staged --config .gitleaks.toml --redact --no-banner .

# A host with no `user.email` configured -- a fresh Coder workspace, in
# particular -- falls back to `$(whoami)@$(hostname)`. That is how this
# repository's history ended up carrying Tailscale MagicDNS workspace names,
# a personal machine's mDNS hostname, and a contributor's previous employer's
# domain: nothing ever checked a commit's authorship before it merged.
#
# BASE_REF has no default, unlike every other gate here. "New commits" only
# means something relative to a specific base, and this repo has no one
# branch that would be the right base for every PR -- main and release_0_1_0
# diverged early and main is not what any of these land on. The workflow
# passes the PR's actual base SHA; a local run has to say what it means by
# "new" too.
#
# The range is BASE_REF..HEAD and nothing is excluded from it. For a while it
# excluded commits already on a release branch, because main and the release
# line shared only the root commit: a release_* -> main PR put the whole
# project in the range (788 commits for release_0_1_1 -> main) and the gate
# failed on authorship from commits merged months earlier, which nobody
# reviewing that PR could act on. Merging release_0_1_1 into main ended that
# -- main now contains the release history, so every branch's range against it
# is the handful of commits it actually adds -- and the exclusion was dropped
# with it, because it also meant a commit pushed straight to a release branch
# was never checked anywhere. Every commit a PR would land is checked again.
#
# If a branch with unrelated history ever needs merging in again, do not widen
# this: give that one PR an exemption and keep the gate strict, so the next
# ordinary PR is not quietly exempt too.
.PHONY: check-commit-authors
check-commit-authors:
	@if [ -z "$(BASE_REF)" ]; then \
	  echo "BASE_REF is required, e.g. make check-commit-authors BASE_REF=origin/main" >&2; \
	  exit 1; \
	fi
	@bad=$$(git log --format='%ae' $(BASE_REF)..HEAD | sort -u | \
	  grep -viE '@qumulo\.com$$|@users\.noreply\.github\.com$$' || true); \
	if [ -n "$$bad" ]; then \
	  echo "commit author email(s) not allowed -- must be @qumulo.com or a GitHub noreply address:" >&2; \
	  echo "$$bad" | sed 's/^/  /' >&2; \
	  exit 1; \
	fi
	@echo "commit authors: all allowed"

# Every gate CI enforces, ordered so the cheapest failure surfaces first.
# This is the single definition: .github/workflows/ci.yml invokes these same
# targets, so a green `make ci` on a laptop and a green PR mean the same
# thing -- which is the whole point of having it, since the alternative is a
# workflow file that slowly drifts from what the Makefile does.
#
# Deliberately excludes docker-*, live-* and e2e-*: those need a registry,
# a real Qumulo cluster, or root on the host, and none of the three is
# available to a hosted runner. check-commit-authors is excluded for a
# different reason: it needs a real BASE_REF to mean anything, and there is
# no branch worth hardcoding here -- it runs as its own CI job instead, where
# the PR's base SHA is always known.
.PHONY: ci
ci: secret-scan lint test coverage-startup sanity-test helm-lint helm-golden openshift-test alert-rules-test helm-verify example-check live-targets-audit govulncheck
	@echo
	@echo "ci: all gates passed"

# Asserts every live-* target actually names a test that exists.
#
# In `make ci` despite auditing targets CI cannot run, because it needs no
# cluster and catches the failure those targets cannot report themselves:
# `go test -run` exits 0 when its pattern matches nothing, so a target whose
# suite was never written passes loudly and forever. Nine of them were in that
# state at once.
.PHONY: live-targets-audit
live-targets-audit:
	@./test/live/check-targets.sh

# Architectures a release image must serve.
#
# arm64 is here because the node DaemonSet has no nodeSelector, so it schedules
# onto every node regardless of architecture. All five sidecars are multi-arch
# sig-storage images and pull fine, so on an arm64 node it was only the
# qumulo-csi-driver container that failed -- leaving the pod 2/3 Ready
# indefinitely, mounting nothing, while kubectl showed something
# partially-healthy rather than obviously broken.
#
# A variable, so adding one is a one-line change. The bases allow arm, ppc64le
# and s390x too; each roughly doubles release build time under emulation for
# the node image, for platforms nobody has asked for.
PLATFORMS ?= linux/amd64,linux/arm64

# Local builds: host architecture, loaded into the local image store. Fast, and
# what minikube work needs.
#
# Deliberately NOT multi-arch. The default buildx driver is `docker`, which
# cannot build multi-platform at all, and multi-platform does not combine with
# --load; making everyday local builds go through a container driver under
# emulation would cost an order of magnitude for nothing. Releases use the
# buildx targets below.
#
# Both images share the Dockerfile's build stage, so BuildKit compiles once
# and the second target is a cache hit.
.PHONY: docker-build
docker-build: docker-build-controller docker-build-node

# The Dockerfile's build stage is `FROM --platform=$$BUILDPLATFORM`, which is a
# BuildKit construct: it pins the compile to the building machine so that an
# arm64 target cross-compiles instead of running the whole Go build under QEMU.
# The legacy builder does not define BUILDPLATFORM, expands it to the empty
# string, and dies on the first line of the Dockerfile with
#
#   failed to parse platform : "" is an invalid OS component of ""
#
# which says nothing about the actual problem -- that this daemon has no buildx.
# Docker 23+ routes `docker build` through buildx when the plugin is installed
# and silently falls back to the legacy builder when it is not, so the failure
# depends on how the host was packaged rather than on anything in this repo.
# Reproduced on Docker 29.1.3 with `DOCKER_BUILDKIT=0`, and reported
# independently from a host with no buildx plugin at all.
#
# Checked once here rather than in each target, and stated as a prerequisite so
# the answer is "install this" instead of a parse error. Removing --platform
# would also silence it, and would quietly make every release image build under
# emulation -- so the flag stays and the requirement is named.
# Two ways to end up on the legacy builder, and they need different remedies,
# so both are checked: the plugin can be missing, or BuildKit can be switched
# off with DOCKER_BUILDKIT=0 on a host that has it. The symptom is identical --
# the same parse error on Step 1 -- which is why testing only for the plugin
# is not enough.
.PHONY: docker-buildkit-check
docker-buildkit-check:
	@if [ "$$DOCKER_BUILDKIT" = "0" ]; then \
	  echo "docker-build needs BuildKit, but DOCKER_BUILDKIT=0 is set in this environment."; \
	  echo "  The Dockerfile's build stage uses --platform=\$$BUILDPLATFORM, which the"; \
	  echo "  legacy builder cannot parse; it fails on Step 1 with an unhelpful message."; \
	  echo "  Unset DOCKER_BUILDKIT, or set it to 1."; \
	  exit 1; \
	fi
	@docker buildx version >/dev/null 2>&1 || { \
	  echo "docker-build needs BuildKit, and this daemon has no buildx plugin."; \
	  echo "  The Dockerfile's build stage uses --platform=\$$BUILDPLATFORM, which the"; \
	  echo "  legacy builder cannot parse; it fails on Step 1 with an unhelpful message."; \
	  echo "  Install it:  sudo apt-get install docker-buildx-plugin"; \
	  echo "               (or see https://docs.docker.com/go/buildx/)"; \
	  exit 1; }

# The node image installs nfs-utils from the RHEL repositories, so its build
# registers with Red Hat for one RUN (see the Dockerfile's node-rootfs stage).
# The organisation ID and activation key come from RH_ORG_ID and
# RH_ACTIVATION_KEY in the environment -- CI sets them from repository
# secrets -- or, when those are unset, from RH_BUILD_ENV, a file holding the
# two lines RH_ORG_ID=... and RH_ACTIVATION_KEY=... (mode 600). They reach
# the build as BuildKit secrets, read from the environment by name, so the
# values never appear on a command line, in `docker history` or in a layer.
RH_BUILD_ENV ?= $(HOME)/.config/qumulo-csi/rh-build.env
RH_SECRETS   := --secret id=rh_org,env=RH_ORG_ID --secret id=rh_key,env=RH_ACTIVATION_KEY
RH_ENV = set -a; \
	if [ -z "$$RH_ORG_ID$$RH_ACTIVATION_KEY" ] && [ -f "$(RH_BUILD_ENV)" ]; then . "$(RH_BUILD_ENV)"; fi; \
	set +a; \
	if [ -z "$$RH_ORG_ID" ] || [ -z "$$RH_ACTIVATION_KEY" ]; then \
	  echo "The node image needs a RHEL subscription to install nfs-utils."; \
	  echo "  Set RH_ORG_ID and RH_ACTIVATION_KEY, or put both in $(RH_BUILD_ENV) (mode 600)."; \
	  exit 1; \
	fi;

.PHONY: docker-build-controller
docker-build-controller: docker-buildkit-check
	docker build --target controller $(DOCKER_BUILD_ARGS) -t $(IMAGE_CONTROLLER):$(VERSION) -t $(IMAGE_CONTROLLER):$(BUILD_TAG) .

.PHONY: docker-build-node
docker-build-node: docker-buildkit-check
	@$(RH_ENV) set -x; \
	docker build --target node $(DOCKER_BUILD_ARGS) $(RH_SECRETS) -t $(IMAGE_NODE):$(VERSION) -t $(IMAGE_NODE):$(BUILD_TAG) .

# --- Release images (multi-arch) ----------------------------------------
# One invocation per image, building every platform and pushing a manifest
# list. This has to be a single buildx --push: building each architecture
# separately and pushing them to the same tag makes the second push *replace*
# the first, leaving a tag that inspects cleanly and serves one architecture.
#
# Prerequisites, both one-time per machine:
#   1. a driver that can build multi-platform, since the default cannot:
#        docker buildx create --name qumulo --driver docker-container --use
#   2. QEMU for any foreign architecture:
#        docker run --privileged --rm tonistiigi/binfmt --install arm64
#      Needed for both runtime images: each runs dnf for the target
#      architecture (the node's also compiles tini there). The Go build itself always cross-compiles, unemulated.
# The csi-addons replication sidecar, rebuilt from upstream source with its
# CVE fixes (build/csi-addons-sidecar/Containerfile says what changes and
# why). Not part of docker-build or docker-buildx: it versions with the
# upstream tag it rebuilds, not with VERSION, and is published once per
# rebuild rather than on every release. The tag never moves: a new rebuild
# bumps the -qumulo.N suffix.
CSI_ADDONS_SIDECAR_TAG   ?= v0.15.1-qumulo.2
IMAGE_CSI_ADDONS_SIDECAR ?= $(IMAGE_PREFIX)csi-addons-k8s-sidecar

CSI_ADDONS_REPO   ?= https://github.com/csi-addons/kubernetes-csi-addons.git
CSI_ADDONS_COMMIT ?= f7f0778d4f532ebfdbade80b693b0c8161520e9c
CSI_ADDONS_SRC    := build/csi-addons-sidecar/.src/$(CSI_ADDONS_COMMIT)

# Upstream's tree at CSI_ADDONS_COMMIT, fetched by hash on the build host and
# checked. One folder per commit, git-ignored, so a new commit is a new folder
# and nothing is ever overwritten. Fetched here rather than in the build
# because build containers reach the Go module proxy but not GitHub -- and the
# proxy's v0.15.1 is not this commit (see the Containerfile).
.PHONY: csi-addons-sidecar-src
csi-addons-sidecar-src:
	@test -f $(CSI_ADDONS_SRC)/.qumulo-source-commit || { set -e; \
	  mkdir -p $(CSI_ADDONS_SRC); cd $(CSI_ADDONS_SRC); \
	  git init -q .; git remote add origin $(CSI_ADDONS_REPO); \
	  git fetch -q --depth 1 origin $(CSI_ADDONS_COMMIT); git checkout -q FETCH_HEAD; \
	  test "$$(git rev-parse HEAD)" = "$(CSI_ADDONS_COMMIT)"; \
	  echo $(CSI_ADDONS_COMMIT) > .qumulo-source-commit; }

.PHONY: csi-addons-sidecar-build
csi-addons-sidecar-build: docker-buildkit-check csi-addons-sidecar-src
	docker build -f build/csi-addons-sidecar/Containerfile --target sidecar \
	  -t $(IMAGE_CSI_ADDONS_SIDECAR):$(CSI_ADDONS_SIDECAR_TAG) $(CSI_ADDONS_SRC)

.PHONY: csi-addons-sidecar-buildx
csi-addons-sidecar-buildx: csi-addons-sidecar-src
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required: a manifest list can only be produced by pushing"; exit 1; }
	@! docker buildx imagetools inspect $(IMAGE_CSI_ADDONS_SIDECAR):$(CSI_ADDONS_SIDECAR_TAG) >/dev/null 2>&1 || { \
	  echo "$(IMAGE_CSI_ADDONS_SIDECAR):$(CSI_ADDONS_SIDECAR_TAG) is already published; bump CSI_ADDONS_SIDECAR_TAG (-qumulo.N) rather than moving it"; exit 1; }
	docker buildx build --platform $(PLATFORMS) -f build/csi-addons-sidecar/Containerfile --target sidecar \
	  -t $(IMAGE_CSI_ADDONS_SIDECAR):$(CSI_ADDONS_SIDECAR_TAG) --push $(CSI_ADDONS_SRC)

# What release-sidecar.yml runs: publish the sidecar once, then never again.
# Like the driver images (never republished under a released VERSION), a
# rebuild's tag is fixed and the chart pins its digest, so an already
# published tag is left alone and the step succeeds.
.PHONY: csi-addons-sidecar-publish
csi-addons-sidecar-publish:
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required, e.g. REGISTRY=ghcr.io/qumulo"; exit 1; }
	@if docker buildx imagetools inspect $(IMAGE_CSI_ADDONS_SIDECAR):$(CSI_ADDONS_SIDECAR_TAG) >/dev/null 2>&1; then \
	  echo "$(IMAGE_CSI_ADDONS_SIDECAR):$(CSI_ADDONS_SIDECAR_TAG) already published; leaving it as it is"; \
	else \
	  $(MAKE) --no-print-directory csi-addons-sidecar-buildx REGISTRY=$(REGISTRY) && \
	  docker buildx imagetools inspect $(IMAGE_CSI_ADDONS_SIDECAR):$(CSI_ADDONS_SIDECAR_TAG) | grep -E "^(Name|Digest):"; \
	fi

.PHONY: docker-buildx
docker-buildx: docker-buildx-controller docker-buildx-node

.PHONY: docker-buildx-controller
docker-buildx-controller:
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required: a manifest list can only be produced by pushing"; exit 1; }
	@test -z "$(GO_TAGS)" || { echo "GO_TAGS=$(GO_TAGS): a release image is built without build tags"; exit 1; }
	docker buildx build --platform $(PLATFORMS) --target controller \
	  $(DOCKER_BUILD_ARGS) -t $(IMAGE_CONTROLLER):$(VERSION) --push .

.PHONY: docker-buildx-node
docker-buildx-node:
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required: a manifest list can only be produced by pushing"; exit 1; }
	@test -z "$(GO_TAGS)" || { echo "GO_TAGS=$(GO_TAGS): a release image is built without build tags"; exit 1; }
	@$(RH_ENV) set -x; \
	docker buildx build --platform $(PLATFORMS) --target node \
	  $(DOCKER_BUILD_ARGS) $(RH_SECRETS) -t $(IMAGE_NODE):$(VERSION) --push .

# Assert the pushed tags really serve every architecture in PLATFORMS.
#
# Same spirit as the mount.nfs assertion in docker-verify: a single-arch
# release image pulls fine on the machine that built it and then fails on an
# arm64 node, which is the same class of defect as a node image with no
# mount.nfs -- it builds perfectly and breaks somewhere else.
.PHONY: docker-verify-manifest
docker-verify-manifest:
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required"; exit 1; }
	@for image in $(IMAGE_CONTROLLER):$(VERSION) $(IMAGE_NODE):$(VERSION); do \
	  echo "== $$image =="; \
	  have=$$(docker buildx imagetools inspect "$$image" --format '{{range .Manifest.Manifests}}{{.Platform.OS}}/{{.Platform.Architecture}} {{end}}'); \
	  for want in $$(echo '$(PLATFORMS)' | tr ',' ' '); do \
	    case " $$have " in *" $$want "*) echo "  OK   $$want" ;; \
	      *) echo "  MISSING $$want (manifest lists: $$have)"; exit 1 ;; \
	    esac; \
	  done; \
	done; \
	echo "docker-verify-manifest: every platform in PLATFORMS is present"

# Assert what the images are actually for: that each reports the version it was
# stamped with, and that the node image can really mount NFS. A node image
# missing mount.nfs builds perfectly and then fails every NodePublishVolume.
.PHONY: docker-verify
docker-verify: docker-build
	@echo "== $(IMAGE_CONTROLLER):$(VERSION) =="
	@docker run --rm $(IMAGE_CONTROLLER):$(VERSION) -version
	@echo "== $(IMAGE_NODE):$(VERSION) =="
	@docker run --rm $(IMAGE_NODE):$(VERSION) -version
	@echo "== controller runs as a non-root uid =="
	@user=$$(docker inspect --format '{{.Config.User}}' $(IMAGE_CONTROLLER):$(VERSION)); \
	  case "$$user" in \
	    ""|0|0:*|root|root:*) echo "controller image runs as root (USER=$${user:-unset}); the chart's runAsNonRoot will not admit it"; exit 1 ;; \
	    *) echo "USER=$$user" ;; \
	  esac
	@echo "== controller CA bundle =="
	@docker run --rm --entrypoint sh $(IMAGE_CONTROLLER):$(VERSION) -c \
	  'test -s /etc/pki/tls/certs/ca-bundle.crt || { echo "MISSING: CA bundle; HTTPS to a cluster with a public CA would fail"; exit 1; }; \
	   echo "CA bundle present: $$(grep -c BEGIN /etc/pki/tls/certs/ca-bundle.crt) certificates"'
	@echo "== node mount tooling =="
	@docker run --rm --entrypoint sh $(IMAGE_NODE):$(VERSION) -c \
	  'for b in mount umount mount.nfs mount.nfs4 tini; do \
	     command -v $$b >/dev/null || { echo "MISSING: $$b"; exit 1; }; \
	   done; echo "all mount binaries present"; /usr/sbin/mount.nfs -V'
	@echo "docker-verify: both images pass"

# Pushing what `docker build` produced publishes ONE architecture under a tag
# that looks complete, and on an arm64 node that presents as a driver bug
# rather than a packaging one. So this refuses by default and names the target
# that does it properly.
#
# The escape hatch exists because a single-arch push is genuinely useful for a
# scratch registry during development; it just must not be how a release
# happens. Setting it is a statement that you know which architecture you are
# publishing.
SINGLE_ARCH_PUSH_GUARD = \
	@test -n "$(ALLOW_SINGLE_ARCH_PUSH)" || { \
	  echo "refusing to push a single-architecture image."; \
	  echo "  release:     make docker-buildx REGISTRY=... (builds $(PLATFORMS) and pushes one manifest list)"; \
	  echo "  development: make $@ REGISTRY=... ALLOW_SINGLE_ARCH_PUSH=1"; \
	  exit 1; \
	}

.PHONY: docker-push
docker-push: docker-push-controller docker-push-node

.PHONY: docker-push-controller
docker-push-controller:
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required to push"; exit 1; }
	$(SINGLE_ARCH_PUSH_GUARD)
	docker push $(IMAGE_CONTROLLER):$(VERSION)

.PHONY: docker-push-node
docker-push-node:
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required to push"; exit 1; }
	$(SINGLE_ARCH_PUSH_GUARD)
	docker push $(IMAGE_NODE):$(VERSION)

# --- Live-cluster tests -------------------------------------------------
# Guarded by the "live" build tag AND QUMULO_LIVE_TEST=1, because these
# talk to a real cluster. Read-only unless the target says otherwise.
QUMULO_API_URL   ?=
QUMULO_TOKEN_FILE ?=

.PHONY: live-smoke
live-smoke:
	@test -n "$(QUMULO_API_URL)"   || { echo "QUMULO_API_URL is required";   exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)" || { echo "QUMULO_TOKEN_FILE is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLive(ClusterReachable|VersionEndpointAuthRequirement|TokenAccepted|InvalidTokenRejected)' -v -count=1

# Read-only: GetCapacity against a real cluster, plus the store-root quota
# behaviour storeRootQuotaLimit depends on. Creates nothing.
.PHONY: live-getcapacity
live-getcapacity:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLiveGetCapacityReportsRealFreeSpace|TestLiveGetCapacityEmptyRequestReportsZero|TestLiveGetCapacityRejectsAMisconfiguredClass|TestLiveGetCapacityWithABadTokenIsNotSilent|TestLiveStoreRootQuotaBehaviour' -v -count=1

# Mutating: provisions and deletes volumes under QUMULO_STORE_REAL_PATH to
# prove ListVolumes reports what CreateVolume made and stops reporting what
# DeleteVolume removed. This is the coverage csi-sanity cannot provide, because
# ListVolumes answers by reading StorageClasses from the Kubernetes API and the
# sanity harness has none -- see the GINKGO_SKIP note in test/sanity/run-test.sh.
.PHONY: live-inventory
live-inventory:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLiveListVolumes|TestLiveControllerGetVolume' -v -count=1 -timeout 20m

# Mutating: sets a quota on QUMULO_STORE_REAL_PATH and removes it again, to
# prove GetCapacity reports the tighter of cluster free space and that quota.
# Split from live-getcapacity so the read-only target stays read-only.
.PHONY: live-getcapacity-quota
live-getcapacity-quota:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLiveGetCapacityIsBoundedByARealQuota' -v -count=1

# Mutating: the restore-after-delete acceptance. Provisions a volume, writes content into
# it, snapshots it, deletes the volume, restores from the snapshot and reads
# the content back off the cluster.
#
# This one needs a real cluster and cannot be faked: cmd/mock-qumulo-server
# implements no copy-chunk endpoint, so nothing there ever copies a byte of
# file content, and a restore against it can only ever prove the tree
# structure. The bytes are the assertion.
#
# The store path must be covered by an NFS export or the tests skip with
# instructions -- a shared-subdirectory volume no export reaches is refused by
# design.
.PHONY: live-restore
live-restore:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLiveRestore' -v -count=1 -timeout 30m

# Mutating: creates directories and quotas under QUMULO_STORE_REAL_PATH and
# tree-deletes them afterwards. The path must contain "csi-live-test" or the
# harness refuses to run.
.PHONY: live-createvolume
live-createvolume:
	@test -n "$(QUMULO_API_URL)"          || { echo "QUMULO_API_URL is required";          exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"       || { echo "QUMULO_TOKEN_FILE is required";       exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"  || { echo "QUMULO_STORE_REAL_PATH is required";  exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run TestLiveCreateVolume -v -count=1 -timeout 20m

# Mutating: same guards as live-createvolume. Every test here provisions a
# volume and then deletes it, so a clean run leaves the store path empty.
.PHONY: live-deletevolume
live-deletevolume:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run TestLiveDeleteVolume -v -count=1 -timeout 20m

# Mutating: expansion and capability validation. Both need a provisioned
# volume, so the same guards apply.
.PHONY: live-expandvolume
live-expandvolume:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLive(ExpandVolume|ValidateVolumeCapabilities)' -v -count=1 -timeout 20m

# Mutating: creates snapshots as well as volumes. Snapshots are removed
# before their source directories, since a snapshot pins what it captured.
.PHONY: live-snapshot
live-snapshot:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLive(CreateSnapshot|DeleteSnapshot|ListSnapshots|Snapshot)' -v -count=1 -timeout 30m

# Mutating: runs the driver against a fault-injecting proxy in front of the
# real cluster, so failures the cluster will not produce on demand -- a failed
# rollback, a revoked token, a dropped connection -- can be exercised.
.PHONY: live-faults
live-faults:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLive(Rollback|Credential|ErrorWithout|ConnectionDropped|DeleteVolumeQuotaFailure|TransientFailure)' -v -count=1 -timeout 20m

# Clean shutdown when the array is unreachable, against real mounts
# (test/openshift/node_outage_test.go).
#
# Split from live-node because of what it does to the machine: it drops TCP
# 2049 to the array from THIS HOST for the length of one unpublish, so any
# other live test running here at the same time sees the array vanish. Run it
# on its own. The rule is removed by the test, by a detached timer if the test
# dies, and by t.Cleanup either way.
#
# QUMULO_LIVE_OUTAGE=1 is set here rather than left to the caller: invoking
# this target IS the consent. The test still refuses without it, so a stray
# `go test ./test/live/` cannot acquire a firewall rule by accident.
#
# This is the half of the storage-outage feature a nightly can cover -- it
# needs no Kubernetes cluster, no OpenShift, and no array power control.
#
# It lives in test/openshift so that one command covers the whole feature;
# the harness it drives is test/livekit, shared with test/live.
.PHONY: live-node-outage
live-node-outage:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	@command -v mount.nfs >/dev/null || command -v /usr/sbin/mount.nfs >/dev/null || \
	  { echo "mount.nfs is missing: install nfs-common"; exit 1; }
	@grep -q '^nodev\s*nfs$$' /proc/filesystems || \
	  { echo "the host kernel has no NFS support: run 'sudo modprobe nfs nfsv3' first"; exit 1; }
	@command -v iptables >/dev/null || command -v nft >/dev/null || \
	  { echo "neither iptables nor nft is present: the array cannot be made unreachable from this host"; exit 1; }
	go test -c -tags openshift -o bin/openshift.test ./test/openshift/
	sudo -E env \
	  QUMULO_LIVE_TEST=1 \
	  QUMULO_LIVE_NODE=1 \
	  QUMULO_LIVE_OUTAGE=1 \
	  QUMULO_OUTAGE_BUDGET='$(QUMULO_OUTAGE_BUDGET)' \
	  QUMULO_API_URL='$(QUMULO_API_URL)' \
	  QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	  QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	  QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	  QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	  QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	  ./bin/openshift.test -test.run TestLiveNodeUnpublishOfAnUnreachableArray -test.v -test.count=1 -test.timeout 40m

# There is deliberately no live-defects target. One existed, running
# `-run '^TestDefect_'` over ./test/live/, but that directory declares no such
# test: it exited 0 having run nothing, exactly the failure check-targets.sh was
# written to prevent -- and that gate could not see it, because it only audited
# patterns beginning with a literal "TestLive". Both the target and the gate's
# blind spot were fixed on 2026-10-06. Every TestDefect_* lives in pkg/driver,
# test/chart, test/doccheck and cmd/*. `make test` (and so CI) runs those built
# with `-tags defects` alone; the ones also tagged `chart` (test/chart and
# pkg/driver's chart render tests) need helm and run with `-tags chart,defects`,
# and the table-read known limitations need `knownlimits` -- both are run by
# hand with those tags, not in `make test`.
# Writing live defect tests means adding them to test/live/ and reinstating a
# target; the break tests the old one promised also need the live-faults fault
# proxy to serve https, which it does not yet.

# --- Helm chart ---------------------------------------------------------
CHART_DIR ?= charts/qumulo-csi-driver
# Values that only exist to make the chart renderable; every one of them is
# required, so lint and template both need them.
CHART_TEST_VALUES := --set storageClasses[0].name=qumulo-nfs \
                     --set storageClasses[0].server=qumulo.example.com \
                     --set storageClasses[0].secretName=qumulo-token \
                     --set storageClasses[0].basePath=/csi/volumes

.PHONY: helm-lint
helm-lint: chart-version-check
	helm lint $(CHART_DIR) $(CHART_TEST_VALUES)

# The chart's appVersion is the driver version it deploys, so it has to track
# the VERSION file. Drift here ships a chart that pulls the wrong image tag.
.PHONY: chart-version-check
chart-version-check:
	@chart_version=$$(grep '^appVersion:' $(CHART_DIR)/Chart.yaml | sed 's/.*"\(.*\)".*/\1/'); \
	if [ "$$chart_version" != "$(VERSION)" ]; then \
	  echo "Chart.yaml appVersion ($$chart_version) does not match VERSION ($(VERSION))"; \
	  exit 1; \
	fi; \
	echo "appVersion $$chart_version matches VERSION"

# A chart for one release: version and appVersion are both $(VERSION), so the
# packaged chart defaults to the images a release publishes -- `0.1.1`, the
# only tag they carry -- and is identical to the chart in the repo, which
# chart-version-check holds to VERSION.
#
# It used to package $(VERSION)+$(GIT_COMMIT) / appVersion $(BUILD_TAG), so
# helm history could tell two builds of one VERSION apart. Nothing published
# names a commit any more, so that version could only point at an image tag
# that does not exist. Telling releases apart is now VERSION's job.
.PHONY: chart-package
chart-package:
	@mkdir -p bin
	helm package $(CHART_DIR) --destination bin \
	  --version $(VERSION) --app-version $(VERSION)

# --- Release ----------------------------------------------------------
# What a customer installs: the packaged chart, from the same registry as the
# images, naming this release's images by default. In that order -- images,
# then the chart -- so a published chart never names an image that is not
# there. The chart repository is $(REGISTRY)/charts, and the chart is stored
# under the OCI tag $(VERSION): `helm install --version $(VERSION)`.
#
# A release is one commit, so a dirty tree is refused: what is published would
# not be what the tree holds, and with no commit in any name there would be
# nothing to tell them apart afterwards.
CHART_REPOSITORY ?= oci://$(REGISTRY)/charts
# e.g. --plain-http, to rehearse a release against a local registry.
HELM_PUSH_FLAGS  ?=

.PHONY: release-guard
release-guard:
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required, e.g. REGISTRY=ghcr.io/qumulo"; exit 1; }
	@git diff --quiet HEAD -- || { echo "refusing to release a dirty tree: $(VERSION) would be published from $(GIT_COMMIT) plus uncommitted changes, and nothing published would record that"; exit 1; }

# The chart only: refuses unless both images for this release are already
# published, which is the whole point of pushing them first.
.PHONY: chart-push
chart-push: release-guard chart-package
	@for image in $(IMAGE_CONTROLLER):$(VERSION) $(IMAGE_NODE):$(VERSION); do \
	  docker buildx imagetools inspect "$$image" >/dev/null 2>&1 || { \
	    echo "refusing to publish a chart whose image is missing: $$image"; \
	    echo "  publish the images first: make docker-buildx REGISTRY=$(REGISTRY)"; exit 1; }; \
	done
	helm push $(HELM_PUSH_FLAGS) bin/$(BINARY)-$(VERSION).tgz $(CHART_REPOSITORY)

# Runs the published images with the args the chart passes them. It used to
# check the packaged chart and the chart folder separately, because they
# defaulted to different image tags: the packaged one to $(BUILD_TAG), the
# folder to the bare $(VERSION). They now both default to $(VERSION), so the
# two runs were the same run; the folder is what is checked, and it no longer
# needs chart-package first. See test/release/check-published-images.sh for
# what this catches that the in-tree contract test cannot.
.PHONY: release-check
release-check:
	./test/release/check-published-images.sh $(CHART_DIR) \
	  --set image.controller.repository=$(IMAGE_CONTROLLER) --set image.node.repository=$(IMAGE_NODE)

# The whole release: multi-arch images, every platform present, the chart,
# then the published images run with their charts' args.
.PHONY: release
release: release-guard docker-buildx docker-verify-manifest chart-push release-check
	@echo "released $(VERSION): helm install <name> $(CHART_REPOSITORY)/$(BINARY) --version $(VERSION)"

.PHONY: helm-template
helm-template:
	helm template qumulo-csi $(CHART_DIR) $(CHART_TEST_VALUES) --debug

# The chart writes StorageClass parameters and the driver validates them, and
# nothing else checks the two agree. This renders the chart for real and feeds
# the result through the driver's own validation, so a parameter the chart
# emits but the driver rejects fails here rather than as a stuck PVC.
# The chart's alert rules, run through promtool: `check rules`, then the unit
# tests in test/chart/rules/alerts_test.yaml against the rules as the chart
# renders them (the metrics golden). Each alert has a case that must fire and
# one that must not -- including the policy-mode threshold and the retired
# and credential-refusing endpoints -- so an expression that stops
# matching the series the driver exports fails here, not in production.
.PHONY: alert-rules-test
alert-rules-test:
	./test/chart/rules/run.sh

.PHONY: helm-golden-update
helm-golden-update:
	go test -tags chart ./test/chart/ -update -count=1
	@echo "golden files rewritten -- read the diff before committing it"

# Every example manifest must stay valid. A doc example nobody runs will break
# silently; this is the cheapest guard against that.
#
# Covers all of deploy/: the guided walkthrough in deploy/example and the
# per-use-case manifests in deploy/use-cases, which the pages under
# docs/use-cases link to.
#
# kubeconform, not `kubectl apply --dry-run=client`. That looks like an offline
# check and is not one: `apply` resolves kinds through API discovery and pulls
# the OpenAPI schema from the server, so with no cluster reachable it fails
# every file identically -- and `--validate=false` does not help, because the
# discovery call happens first. It passed on developer machines only because
# one had a minikube left running from `make e2e-minikube`, which is precisely
# the kind of gate that is green for a reason unrelated to what it claims to
# test. kubeconform validates against JSON schemas instead, so the result
# depends on the manifests and nothing else.
#
# -strict rejects unknown fields, which is what catches the realistic mistake:
# a misspelled key in a doc example is silently ignored by a non-strict
# validator and by the API server alike.
KUBECONFORM_VERSION ?= v0.7.0

# Pinned to the chart's own kubeVersion floor, so the examples are checked
# against the oldest Kubernetes this chart claims to install on. Moved to
# 1.35.0 with the floor on 2026-10-13; this is deliberately the floor and not
# the version e2e runs (E2E_K8S_VERSION), because a manifest that validates
# only on the newest supported Kubernetes is exactly the bug this gate exists
# to catch.
KUBECONFORM_K8S_VERSION ?= 1.35.0

# Core kinds come from kubeconform's default schema source, selected by the
# version above. Custom resources are not in any released Kubernetes schema
# set, so the two this repo's examples use are committed under test/schemas --
# see the README there for why they are vendored rather than fetched.
KUBECONFORM_FLAGS := -strict -summary \
                     -kubernetes-version $(KUBECONFORM_K8S_VERSION) \
                     -schema-location default \
                     -schema-location 'test/schemas/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'

.PHONY: example-check
example-check:
	@command -v kubeconform >/dev/null || \
	  go install github.com/yannh/kubeconform/cmd/kubeconform@$(KUBECONFORM_VERSION)
	find deploy -name '*.yaml' | sort | xargs kubeconform $(KUBECONFORM_FLAGS)

# What example-check used to be, kept because it tests something the schema
# check cannot: that a real API server, with this repo's CRDs installed,
# accepts these manifests. Needs a reachable cluster, so it stays hand-run and
# out of CI -- see the `ci` target.
.PHONY: example-check-cluster
example-check-cluster:
	@command -v kubectl >/dev/null || { echo "kubectl is required"; exit 1; }
	@kubectl cluster-info >/dev/null 2>&1 || \
	  { echo "no reachable cluster: this target applies against a live API server (use example-check for the offline check)"; exit 1; }
	@fail=0; for f in $$(find deploy -name '*.yaml' | sort); do \
	  if kubectl apply --dry-run=client -f "$$f" >/dev/null 2>&1; then \
	    printf '  OK   %s\n' "$$f"; \
	  else printf '  FAIL %s\n' "$$f"; fail=1; fi; done; \
	[ $$fail -eq 0 ] && echo "example-check-cluster: all manifests valid" || exit 1

.PHONY: helm-golden
helm-golden:
	go test -tags chart ./test/chart/ -count=1

# The OpenShift tier: does the installer put the driver inside OpenShift's
# permitted security settings without a manual workaround, does it ask for only
# the access it needs, and are volumes usable across the SCCs workloads run
# under. It parses the install and evaluates it against models of OpenShift's
# SecurityContextConstraints and Pod Security Admission -- including the SCC
# the chart renders itself -- so it needs helm and nothing else: no cluster, no
# array, no network.
#
# Separate from test/chart's openshift_test.go rather than a duplicate of it:
# that one asserts the templates emit the right YAML, this one asserts
# admission would accept the result.
#
# In `make ci` because it needs only helm and runs in under four seconds, and
# because the installer's central claim -- that the driver runs inside
# OpenShift's permitted security settings with no manual SCC binding -- is
# otherwise asserted nowhere that a PR would notice breaking.
#
# Out of `make test`, which is the no-helm tier. A green run here is still not
# OpenShift support: every admission verdict is against a MODEL of OpenShift's
# SCCs in test/openshift/scc_test.go, not against OpenShift, so a wrong model
# is confidently wrong. See test/openshift/README.md for what it cannot settle,
# SELinux on RHCOS first among them. The -verbose target prints the SCC
# verdicts and the facts the cases derive.
.PHONY: openshift-test
openshift-test:
	go test -tags openshift ./test/openshift/ -count=1

.PHONY: openshift-test-verbose
openshift-test-verbose:
	go test -tags openshift ./test/openshift/ -count=1 -v

# The whole OpenShift tier in one command, for a nightly: the offline chart and
# admission models, plus the live outage cases.
#
# The live cases skip unless this host has opted in (QUMULO_LIVE_TEST,
# QUMULO_LIVE_NODE, QUMULO_LIVE_OUTAGE, and root), so this target is safe to
# run anywhere -- on a runner without those it is exactly `openshift-test`,
# with the two cases reporting SKIP and why. Where they ARE set, it drops the
# route to the array for one unpublish; see live-node-outage.
.PHONY: openshift-night
openshift-night:
	go test -tags openshift ./test/openshift/ -count=1 -v -timeout 45m

# Mutating, and against TWO arrays: the csi-addons replication service run
# end to end -- protect, planned failover, reprotect, failback, unprotect --
# with whole-file md5 compared on both arrays at each step. Beyond the usual
# variables it needs the peer's endpoint and token; QUMULO_PEER_SERVER is the
# address the local array replicates to on port 3712 (defaults to the peer
# API URL's host). Provisions a dedicated-export volume and skips where the
# cluster cannot serve that model (KI-1). See test/live/replication_test.go.
.PHONY: live-replication
live-replication:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	@test -n "$(QUMULO_PEER_API_URL)"      || { echo "QUMULO_PEER_API_URL is required";      exit 1; }
	@test -n "$(QUMULO_PEER_TOKEN_FILE)"   || { echo "QUMULO_PEER_TOKEN_FILE is required";   exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_PEER_API_URL='$(QUMULO_PEER_API_URL)' \
	QUMULO_PEER_TOKEN_FILE='$(QUMULO_PEER_TOKEN_FILE)' \
	QUMULO_PEER_SERVER='$(QUMULO_PEER_SERVER)' \
	QUMULO_LOCAL_SERVER='$(QUMULO_LOCAL_SERVER)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLiveReplication(RoundTrip|RefusesSharedSubdirectoryVolumes|GroupReEnablesAfterAPlainDisable)' -v -count=1 -timeout 45m

# The three replication-aware guards on the ORDINARY CSI RPCs -- DeleteVolume,
# ControllerExpandVolume and ListVolumes -- against two arrays. Split from
# live-replication because those three are unconditional: they run for every
# user of the driver, not only for one who enabled replication, since a volume
# can be protected by a relationship an administrator made by hand.
#
# QUMULO_UNPRIVILEGED_TOKEN_FILE is optional: a token on the PEER array with
# the ordinary CSI privileges and no PRIVILEGE_REPLICATION_*, which is what
# the ListVolumes refusal case needs. Without it that one case skips.
#
# Mutating: provisions, protects, expands and deletes under
# QUMULO_STORE_REAL_PATH on both arrays.
.PHONY: live-replication-guards
live-replication-guards:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	@test -n "$(QUMULO_PEER_API_URL)"      || { echo "QUMULO_PEER_API_URL is required";      exit 1; }
	@test -n "$(QUMULO_PEER_TOKEN_FILE)"   || { echo "QUMULO_PEER_TOKEN_FILE is required";   exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_PEER_API_URL='$(QUMULO_PEER_API_URL)' \
	QUMULO_PEER_TOKEN_FILE='$(QUMULO_PEER_TOKEN_FILE)' \
	QUMULO_PEER_SERVER='$(QUMULO_PEER_SERVER)' \
	QUMULO_LOCAL_SERVER='$(QUMULO_LOCAL_SERVER)' \
	QUMULO_UNPRIVILEGED_TOKEN_FILE='$(QUMULO_UNPRIVILEGED_TOKEN_FILE)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLiveReplication(Expand|Delete|ListVolumes|OldHandleAfterFailover)|TestLiveCreateSnapshotRefusesAnOverrideToAnotherCluster' -v -count=1 -timeout 30m

# The Kubernetes-facing delete and create safety checks on real arrays.
# Mutating, each test in a directory of its own under the store path, removed
# at the end. The replicated-volume case needs the peer variables
# live-replication takes; without them it skips.
.PHONY: live-k8s-impact
live-k8s-impact:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_PEER_API_URL='$(QUMULO_PEER_API_URL)' \
	QUMULO_PEER_TOKEN_FILE='$(QUMULO_PEER_TOKEN_FILE)' \
	QUMULO_PEER_SERVER='$(QUMULO_PEER_SERVER)' \
	QUMULO_LOCAL_SERVER='$(QUMULO_LOCAL_SERVER)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run '$(or $(RUN),TestLiveK8sImpact)' -v -count=1 -timeout 30m

# Node service against a real NFS mount. Three gates, not two: the live tag,
# QUMULO_LIVE_TEST, and QUMULO_LIVE_NODE -- because unlike every other live
# target this one mounts filesystems on THIS machine and needs root.
#
# The test binary is compiled as you and only the binary runs under sudo, so
# root never owns anything in the build cache.
.PHONY: live-node
live-node:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	@command -v mount.nfs >/dev/null || command -v /usr/sbin/mount.nfs >/dev/null || 	  { echo "mount.nfs is missing: install nfs-common"; exit 1; }
	@grep -q '^nodev\s*nfs$$' /proc/filesystems || 	  { echo "the host kernel has no NFS support: run 'sudo modprobe nfs nfsv3' first"; exit 1; }
	go test -c -tags live -o bin/live.test ./test/live/
	sudo -E env \
	  QUMULO_LIVE_TEST=1 \
	  QUMULO_LIVE_NODE=1 \
	  QUMULO_API_URL='$(QUMULO_API_URL)' \
	  QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	  QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	  QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	  QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	  QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	  ./bin/live.test -test.run TestLiveNode -test.v -test.count=1 -test.timeout 20m

# --- End-to-end on a local cluster --------------------------------------
# Brings up minikube, deploys the chart, and provisions against a real Qumulo
# cluster. Needs QUMULO_API_URL, QUMULO_TOKEN_FILE and QUMULO_STORE_REAL_PATH,
# and two host-level prerequisites:
#
#   1. NFS kernel modules: sudo modprobe nfs nfsv3 nfsv4
#      The mount syscall is served by the host kernel, which minikube's docker
#      driver shares, so the node image's mount.nfs is necessary but not
#      sufficient on its own.
#   2. Docker access for your user.
#
# Images are built INSIDE minikube's docker daemon on purpose. `minikube image
# load` silently leaves an existing repository:tag in place, which presents as
# a driver bug -- a StorageClass parameter that the running binary predates.
# The install names the build tag: under the bare 0.1.0 the chart pulls Always,
# and an image that exists only in minikube's daemon cannot be pulled.
.PHONY: e2e-minikube
e2e-minikube: e2e-preflight
	minikube start --driver=docker --container-runtime=containerd --kubernetes-version=$(E2E_K8S_VERSION) --cpus=2 --memory=4096
	eval $$(minikube docker-env) && $(MAKE) docker-build
	kubectl create namespace $(E2E_NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n $(E2E_NAMESPACE) create secret generic qumulo-api-token \
	  --from-file=token=$(QUMULO_TOKEN_FILE) --dry-run=client -o yaml | kubectl apply -f -
	helm upgrade --install qumulo-csi $(CHART_DIR) -n $(E2E_NAMESPACE) \
	  --set image.controller.repository=$(IMAGE_CONTROLLER) \
	  --set image.node.repository=$(IMAGE_NODE) \
	  --set image.controller.tag=$(BUILD_TAG) \
	  --set image.node.tag=$(BUILD_TAG) \
	  --set qumulo.insecureSkipTlsVerify=$(E2E_TLS_INSECURE) \
	  --set storageClasses[0].name=qumulo-nfs3 \
	  --set storageClasses[0].server=$(E2E_CLUSTER_ADDRESS) \
	  --set storageClasses[0].apiUrl=$(QUMULO_API_URL) \
	  --set storageClasses[0].secretName=qumulo-api-token \
	  --set storageClasses[0].basePath=$(QUMULO_STORE_REAL_PATH) \
	  --set storageClasses[0].protocol=nfs3 \
	  --set 'storageClasses[0].mountOptions[0]=nolock'
	kubectl -n $(E2E_NAMESPACE) rollout status deploy/qumulo-csi-qumulo-csi-driver-controller --timeout=180s
	kubectl -n $(E2E_NAMESPACE) rollout status ds/qumulo-csi-qumulo-csi-driver-node --timeout=180s
	@echo
	@echo "Driver registered with the kubelet:"
	@kubectl get csinodes -o jsonpath='{range .items[*]}  {.metadata.name}: {range .spec.drivers[*]}{.name}{end}{"\n"}{end}'
	@echo
	@echo "Deployed. Volume directories are created mode 0777 so a pod mapped to"
	@echo "another identity can write to them; set mountPermissions on a"
	@echo "StorageClass to narrow that, or to 'inherit' to keep the cluster default."

E2E_NAMESPACE       ?= qumulo-csi
# The e2e matrix is 1.35.6 and 1.36.0, and both have to stay green: 1.35.6 is
# the customer version the floor was lowered for (2026-10-13), 1.36.0 is what
# every measurement in docs/reference/supported-features.md was taken on. The default is the
# older of the two, so a bare `make e2e-minikube` exercises the version least
# likely to have been tried. Override to run the other arm:
#   make e2e-minikube E2E_K8S_VERSION=v1.36.0
# containerd rather than the docker runtime: on these Kubernetes versions
# cri-dockerd's streaming endpoint breaks `kubectl exec`, which every e2e
# assertion depends on.
E2E_K8S_VERSION     ?= v1.35.6
E2E_K8S_MATRIX      ?= v1.35.6 v1.36.0
E2E_TLS_INSECURE    ?= false
E2E_CLUSTER_ADDRESS ?= $(shell echo '$(QUMULO_API_URL)' | sed -E 's#^https?://##; s#:.*##')

# Assert what the deployed driver actually does: real chart, real sidecars,
# real kubelet mounts, real pods, and every effect verified against the live
# Qumulo cluster's REST API. Runs both nfs3 and nfs4.1 in one pass so a
# difference can be attributed to the protocol rather than the environment.
#
# Assumes the chart is already installed (make e2e-minikube) with the three
# StorageClasses. Nothing here is mocked.
.PHONY: e2e-verify
e2e-verify:
	@test -n "$(QUMULO_API_URL)"          || { echo "QUMULO_API_URL is required";          exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"       || { echo "QUMULO_TOKEN_FILE is required";       exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"  || { echo "QUMULO_STORE_REAL_PATH is required";  exit 1; }
	@command -v kubectl >/dev/null || { echo "kubectl is required"; exit 1; }
	QUMULO_E2E=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	go test -tags e2e ./test/e2e/ -v -count=1 -timeout 30m

.PHONY: e2e-preflight
e2e-preflight:
	@test -n "$(QUMULO_API_URL)"          || { echo "QUMULO_API_URL is required";          exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"       || { echo "QUMULO_TOKEN_FILE is required";       exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"  || { echo "QUMULO_STORE_REAL_PATH is required";  exit 1; }
	@command -v minikube >/dev/null || { echo "minikube is required"; exit 1; }
	@command -v helm     >/dev/null || { echo "helm is required";     exit 1; }
	@grep -q '^nodev\s*nfs$$' /proc/filesystems || { \
	  echo "the host kernel has no NFS support: run 'sudo modprobe nfs nfsv3 nfsv4' first"; exit 1; }
	@echo "preflight OK: minikube, helm, and NFS kernel support present"

.PHONY: e2e-teardown
e2e-teardown:
	-helm uninstall qumulo-csi -n $(E2E_NAMESPACE)
	-kubectl delete namespace $(E2E_NAMESPACE)
	minikube delete

.PHONY: helm-verify
helm-verify:
	@command -v helm >/dev/null || { echo "helm is required for the chart contract tests"; exit 1; }
	go test -tags chart ./pkg/driver/ -run TestChart -v -count=1

# The chart's copy of the IANA zone names (templates/_tzzones.tpl), which
# _validate.tpl checks a snapshotSchedule's tz= against at install. Rewritten
# from the Go toolchain's zone database -- the one the driver embeds -- so run
# it after a Go bump; helm-verify fails until the two agree. The other step a
# Go bump needs is `make lint` with GOLANGCI_VERSION raised to a release built
# for the new Go, or lint fails with a typecheck error before checking anything.
.PHONY: chart-timezones
chart-timezones:
	go test -tags chart ./pkg/driver/ -run '^TestChartTimezoneListIsTheDriversDatabase$$' -update-timezones -count=1
	@echo "chart zone list rewritten -- read the diff before committing it"

# Phase 1 API discovery. Split by blast radius, because these three do very
# different amounts of damage if something is wrong:
#   live-discovery         GETs only -- reads export/policy/AD configuration
#   live-discovery-schema  POSTs that are expected to be REJECTED, to learn
#                          required fields; creates nothing when it passes
#   live-discovery-mutate  really creates and deletes an NFS export, so it
#                          needs QUMULO_LIVE_MUTATE_CONFIG=1 as a second gate
.PHONY: live-discovery
live-discovery:
	@test -n "$(QUMULO_API_URL)"    || { echo "QUMULO_API_URL is required";    exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)" || { echo "QUMULO_TOKEN_FILE is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLiveDiscover(NFSExportsAPI|SnapshotPolicyAPI|ActiveDirectory)' -v -count=1

.PHONY: live-discovery-schema
live-discovery-schema:
	@test -n "$(QUMULO_API_URL)"    || { echo "QUMULO_API_URL is required";    exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)" || { echo "QUMULO_TOKEN_FILE is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLiveDiscover(NFSExportSchema|SnapshotPolicySchema)' -v -count=1

.PHONY: live-discovery-mutate
live-discovery-mutate:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_LIVE_MUTATE_CONFIG=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run TestLiveDiscoverNFSExportLifecycle -v -count=1 -timeout 10m

# Mutating: the dedicated-export provisioning model end to end against a real
# cluster. Creates and removes NFS exports, so it carries the same
# cluster-config gate as live-discovery-mutate.
.PHONY: live-dedicated-export
live-dedicated-export:
	@test -n "$(QUMULO_API_URL)"           || { echo "QUMULO_API_URL is required";           exit 1; }
	@test -n "$(QUMULO_TOKEN_FILE)"        || { echo "QUMULO_TOKEN_FILE is required";        exit 1; }
	@test -n "$(QUMULO_STORE_REAL_PATH)"   || { echo "QUMULO_STORE_REAL_PATH is required";   exit 1; }
	@test -n "$(QUMULO_STORE_EXPORT_PATH)" || { echo "QUMULO_STORE_EXPORT_PATH is required"; exit 1; }
	QUMULO_LIVE_TEST=1 \
	QUMULO_LIVE_MUTATE_CONFIG=1 \
	QUMULO_API_URL='$(QUMULO_API_URL)' \
	QUMULO_TOKEN_FILE='$(QUMULO_TOKEN_FILE)' \
	QUMULO_STORE_REAL_PATH='$(QUMULO_STORE_REAL_PATH)' \
	QUMULO_STORE_EXPORT_PATH='$(QUMULO_STORE_EXPORT_PATH)' \
	QUMULO_TLS_INSECURE='$(QUMULO_TLS_INSECURE)' \
	QUMULO_TLS_CA_FILE='$(QUMULO_TLS_CA_FILE)' \
	go test -tags live ./test/live/ -run 'TestLive(DedicatedExport|SharedSubdirectory|VolumeDirectoryIsWritableMode)' -v -count=1 -timeout 15m

.PHONY: clean
clean:
	rm -rf bin coverage.out
