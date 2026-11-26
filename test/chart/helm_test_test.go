//go:build chart

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chart-verifier's contains-test check failed: the chart had no `helm test`.
// It now carries two:
//
//   - test-driver-ready, always on: the controller is Available, every node
//     pod is Ready and the driver is registered in CSINode. It provisions
//     nothing, so it passes on any cluster the chart installed on, including
//     chart-verifier's own chart-testing run.
//   - test-volume, opt-in (tests.storageClass): a non-root pod writes a file to
//     a volume and reads it back, so a pass means the controller, the node
//     plugin and the array's export all work for an ordinary application.

func render(t *testing.T, show string, args ...string) (string, error) {
	t.Helper()
	all := append([]string{"template", "qumulo-csi", chartPath, "--namespace", "qumulo-csi", "--show-only", show}, args...)
	out, err := exec.Command("helm", all...).CombinedOutput()
	return string(out), err
}

func renderHelmTest(t *testing.T, args ...string) string {
	t.Helper()
	out, err := render(t, "templates/tests/test-volume.yaml", args...)
	require.NoError(t, err, "helm template failed: %s", out)
	return out
}

func renderReadyTest(t *testing.T, args ...string) string {
	t.Helper()
	out, err := render(t, "templates/tests/test-driver-ready.yaml", args...)
	require.NoError(t, err, "helm template failed: %s", out)
	return out
}

var oneClass = []string{
	"--set", "storageClasses[0].name=chart-class", "--set", "storageClasses[0].basePath=/csi/volumes",
	"--set", "storageClasses[0].secretName=qumulo-api-token", "--set", "storageClasses[0].server=10.0.0.13",
}

func TestHelmTestFilesAreInTheChart(t *testing.T) {
	for _, f := range []string{"test-driver-ready.yaml", "test-volume.yaml"} {
		_, err := os.Stat(filepath.Join(chartPath, "templates", "tests", f))
		require.NoError(t, err, "chart-verifier's contains-test looks for a template under templates/tests")
	}
}

// The readiness test renders with default values and no class at all: it is
// what chart-verifier's chart-testing run executes, on a cluster with no array.
func TestHelmTestReadyIsOnByDefault(t *testing.T) {
	out := renderReadyTest(t)
	require.Equal(t, 1, strings.Count(out, "\nkind: "), "one Pod")
	assert.Contains(t, out, "kind: Pod")
	assert.Contains(t, out, "helm.sh/hook: test")
	assert.Contains(t, out, "helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded")
	assert.Contains(t, out, "serviceAccountName: qumulo-csi-qumulo-csi-driver-test")
	assert.Contains(t, out, "automountServiceAccountToken: true", "the check reads the API")
	// The check is the driver's own `ready-check` (k8s.ReadyCheck, unit-tested
	// there): scaled-to-0, the rollout wait and the CSINode count
	// live in Go now, not in a script here.
	assert.Contains(t, out, "command:\n        - /qumulo-csi-driver\n        - ready-check\n")
	assert.Contains(t, out, "- -namespace=qumulo-csi\n")
	assert.Contains(t, out, "- -driver-name=csi.qumulo.com\n")
	assert.Contains(t, out, "- -deployment=qumulo-csi-qumulo-csi-driver-controller\n")
	assert.Contains(t, out, "- -daemonset=qumulo-csi-qumulo-csi-driver-node\n")
	// One budget for all three checks, under Helm's own 300 s.
	assert.Contains(t, out, "- -timeout=240s\n")
	assert.NotContains(t, out, "kubectl", "no CLI image, no script")

	_, err := render(t, "templates/tests/test-driver-ready.yaml", "--set", "tests.ready.enabled=false")
	assert.Error(t, err, "tests.ready.enabled=false renders no readiness test")
}

// The readiness test runs from the controller image on every platform, so it
// pulls nothing the release does not already run (the 0.1.1 image scan: the
// OpenShift CLI images it used before carried their own CVEs). Its pull policy
// is the controller's. tests.ready.image overrides it with a mirror.
func TestHelmTestReadyRunsFromTheControllerImage(t *testing.T) {
	for _, args := range [][]string{nil, {"--set", "openshift.enabled=true"}} {
		out := renderReadyTest(t, args...)
		assert.Contains(t, out, "image: ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller:0.1.1\n", args)
		assert.Contains(t, out, "imagePullPolicy: Always\n", "the bare release tag moves, as for the controller")
		assert.NotContains(t, out, "ose-cli")
		assert.NotContains(t, out, "origin-cli")
	}
	out := renderReadyTest(t, "--set", "image.controller.tag=0.1.1-abc1234")
	assert.Contains(t, out, "image: ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller:0.1.1-abc1234\n")
	assert.Contains(t, out, "imagePullPolicy: IfNotPresent\n")
	out = renderReadyTest(t, "--set", "tests.ready.image=mirror.example/qumulo-csi-driver-controller:0.1.1")
	assert.Contains(t, out, `image: "mirror.example/qumulo-csi-driver-controller:0.1.1"`)
	assert.Contains(t, out, "imagePullPolicy: IfNotPresent\n")
}

// The volume test's image is pinned by tag and multi-arch index digest, like
// the sidecars. The readiness test's default is the controller image, which
// follows image.controller and is covered by the controller's own pin rules.
func TestHelmTestImagesArePinned(t *testing.T) {
	pinned := regexp.MustCompile(`image: "[^"\s@]+:[A-Za-z0-9_.-]+@sha256:[a-f0-9]{64}"`)
	// slices.Concat, never append: append onto a shared slice can write into
	// its spare capacity, so two cases would share (and overwrite) one array.
	volume := slices.Concat(oneClass, []string{"--set", "tests.storageClass=chart-class"})

	assert.Regexp(t, pinned, renderHelmTest(t, volume...), "the volume test's default image")

	// The template's fallback (a --reuse-values upgrade with no tests.image)
	// and values.yaml must name the same image, or one copy drifts unpinned.
	withValue := renderHelmTest(t, volume...)
	withFallback := renderHelmTest(t, slices.Concat(volume, []string{"--set", "tests.image=null"})...)
	assert.Equal(t, pinned.FindString(withValue), pinned.FindString(withFallback),
		"values.yaml tests.image and the template's fallback differ")

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"volume image latest", slices.Concat(volume, []string{"--set", "tests.image=example.com/x:latest"}), "tests.image"},
		{"volume image untagged", slices.Concat(volume, []string{"--set", "tests.image=example.com/x"}), "does not match pattern"},
		{"ready image latest", []string{"--set", "tests.ready.image=example.com/cli:latest"}, "tests.ready.image"},
		{"ready image untagged", []string{"--set", "tests.ready.image=example.com/cli"}, "does not match pattern"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := render(t, "templates/csidriver.yaml", slices.Concat(oneClass, tc.args)...)
			require.Error(t, err, "must be refused: %s", out)
			assert.Contains(t, out, tc.want)
		})
	}
}

// Read-only, and only what the check reads; no pods access. Ordinary
// release objects, not hooks: a non-pod hook counts as succeeded the moment
// it exists, so hook-succeeded would delete the grant mid-test.
func TestHelmTestReadyRBACIsReadOnly(t *testing.T) {
	out, err := render(t, "templates/test-rbac.yaml")
	require.NoError(t, err, "%s", out)
	assert.NotContains(t, out, "helm.sh/hook", "the test's identity lives with the release, not as a hook")
	for _, verb := range []string{"create", "update", "patch", "delete", `"*"`} {
		assert.NotContains(t, out, verb, "the readiness check only reads")
	}
	assert.NotContains(t, out, `resources: ["pods"]`, "no pods access")
	assert.Contains(t, out, `resourceNames: ["qumulo-csi-qumulo-csi-driver-controller"]`)
	assert.Contains(t, out, `resourceNames: ["qumulo-csi-qumulo-csi-driver-node"]`)
	assert.Contains(t, out, "kind: ClusterRole\nmetadata:\n  name: qumulo-csi-qumulo-csi-qumulo-csi-driver-test\n",
		"cluster-scoped, so named with the namespace")
	assert.Contains(t, out, `resources: ["csinodes"]`)

	_, err = render(t, "templates/test-rbac.yaml", "--set", "tests.ready.enabled=false")
	assert.Error(t, err, "no readiness test, no identity for it")
}

// The volume test provisions a real volume, so it is opt-in: a chart class is
// not enough, tests.storageClass must name one.
func TestHelmTestVolumeIsOptIn(t *testing.T) {
	_, err := render(t, "templates/tests/test-volume.yaml", oneClass...)
	assert.Error(t, err, "a chart StorageClass alone must not turn the volume test on")
	out := renderHelmTest(t, append(oneClass, "--set", "tests.storageClass=chart-class")...)
	assert.Contains(t, out, `storageClassName: "chart-class"`)
}

func TestHelmTestUsesTheNamedClass(t *testing.T) {
	out := renderHelmTest(t, append(oneClass, "--set", "tests.storageClass=made-outside", "--set", "tests.size=2Gi")...)
	require.Equal(t, 1, strings.Count(out, "\nkind: "), "one Pod; its claim is a generic ephemeral volume, not a hook of its own")

	assert.Contains(t, out, "kind: Pod")
	assert.NotContains(t, out, "kind: PersistentVolumeClaim",
		"a PVC hook is deleted by hook-succeeded the moment it exists, and after a failed run it blocks the next one")
	assert.Contains(t, out, "helm.sh/hook: test")
	assert.Contains(t, out, "helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded",
		"a pass cleans up; a failure is kept for inspection and replaced by the next run")
	assert.Contains(t, out, "ephemeral:\n        volumeClaimTemplate:")
	assert.Contains(t, out, `storageClassName: "made-outside"`)
	assert.Contains(t, out, `storage: "2Gi"`)
	assert.Contains(t, out, "restartPolicy: Never")
	assert.Contains(t, out, "automountServiceAccountToken: false", "the test needs no API access")
	assert.NotContains(t, out, "$$", "Kubernetes turns $$ in a command into a lone $")
}

// --reuse-values from a release made before the tests block keeps the old
// values, which have no `tests` key: the renders must not nil-pointer (I-1).
func TestHelmTestRendersWithoutTheTestsBlock(t *testing.T) {
	out := renderReadyTest(t, "--set", "tests=null")
	assert.Contains(t, out, "image: ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller:0.1.1\n")
	_, err := render(t, "templates/tests/test-volume.yaml", "--set", "tests=null")
	assert.Error(t, err, "no tests.storageClass, no volume test")
}

// Both test pods are ordinary applications: non-root, no privilege, no
// capabilities, read-only root. On OpenShift restricted-v2 assigns the uid and
// refuses a fixed one, so the uid is set only off OpenShift.
func TestHelmTestPodsAreUnprivileged(t *testing.T) {
	vol := []string{"--set", "tests.storageClass=c"}
	for _, tc := range []struct {
		name string
		fn   func(t *testing.T, args ...string) string
		args []string
	}{
		{"ready", renderReadyTest, nil},
		{"volume", renderHelmTest, vol},
	} {
		plain := tc.fn(t, tc.args...)
		ocp := tc.fn(t, append(tc.args, "--set", "openshift.enabled=true")...)
		for name, out := range map[string]string{"kubernetes": plain, "openshift": ocp} {
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				for _, want := range []string{
					"runAsNonRoot: true", "type: RuntimeDefault", "allowPrivilegeEscalation: false",
					"readOnlyRootFilesystem: true", "drop: [ALL]", "limits: {memory: 64Mi, ephemeral-storage: 64Mi}",
				} {
					assert.Contains(t, out, want)
				}
				assert.NotContains(t, out, "privileged: true")
				assert.NotContains(t, out, "hostNetwork")
			})
		}
		assert.Contains(t, plain, "runAsUser: 65532")
		assert.NotContains(t, plain, "openshift.io/required-scc")
		assert.NotContains(t, ocp, "runAsUser:", "restricted-v2 refuses a uid outside the namespace range")
		assert.Contains(t, ocp, `openshift.io/required-scc: "restricted-v2"`)
	}
}
