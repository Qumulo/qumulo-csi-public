//go:build chart

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const sccAPI = "security.openshift.io/v1/SecurityContextConstraints"

func renderOpenShift(t *testing.T, args ...string) string {
	t.Helper()
	all := append([]string{"template", "qumulo-csi", chartPath, "--namespace", "qumulo-csi"}, args...)
	out, err := exec.Command("helm", all...).CombinedOutput()
	require.NoError(t, err, "helm template failed: %s", out)
	return string(out)
}

// On OpenShift today's chart installed "successfully" and ran nothing:
// restricted-v2 refused the controller's fixed uid 65532, and no SCC the node
// service account could use allowed a privileged, host-network, hostPath pod.
// openshift.enabled=auto turns both fixes on from the cluster's API list alone.
func TestOpenShiftDetectedFromSCCAPI(t *testing.T) {
	out := renderOpenShift(t, "--api-versions", sccAPI)

	assert.Contains(t, out, "kind: SecurityContextConstraints")
	assert.Contains(t, out, `openshift.io/required-scc: "qumulo-csi-qumulo-csi-driver-node"`)
	assert.Contains(t, out, `openshift.io/required-scc: "restricted-v2"`)
	assert.NotContains(t, out, "runAsUser: 65532", "restricted-v2 refuses a uid outside the namespace range")
	assert.Contains(t, out, "runAsNonRoot: true", "dropping the uid must not drop the non-root guarantee")
}

// Plain Kubernetes renders exactly as before: no SCC, no annotation, the uid.
func TestOpenShiftOffWithoutSCCAPI(t *testing.T) {
	for _, args := range [][]string{nil, {"--api-versions", sccAPI, "--set", "openshift.enabled=false"}} {
		out := renderOpenShift(t, args...)
		assert.NotContains(t, out, "SecurityContextConstraints")
		assert.NotContains(t, out, "openshift.io/required-scc")
		assert.Contains(t, out, "runAsUser: 65532")
	}
}

// Only a restricted SCC assigns a uid. Pinned to one that does not (nonroot-v2,
// anyuid, privileged), the controller keeps its uid: without it every sidecar
// image runs as root and runAsNonRoot refuses it (measured on OCP 4.22 with
// privileged: 5 of 6 containers CreateContainerConfigError).
func TestOpenShiftKeepsUIDOutsideRestrictedSCC(t *testing.T) {
	for _, scc := range []string{"nonroot-v2", "anyuid", "privileged"} {
		out := renderOpenShift(t, "--set", "openshift.enabled=true", "--set", "openshift.controllerSCC="+scc)
		assert.Contains(t, out, `openshift.io/required-scc: "`+scc+`"`)
		assert.Contains(t, out, "runAsUser: 65532", scc)
	}
	out := renderOpenShift(t, "--set", "openshift.enabled=true", "--set", "openshift.controllerSCC=restricted-v3")
	assert.NotContains(t, out, "runAsUser: 65532")
}

// An admin who manages SCCs centrally brings one: no SCC or grant rendered, and
// the node pods are pinned to theirs.
func TestOpenShiftAdminSuppliedNodeSCC(t *testing.T) {
	out := renderOpenShift(t, "--set", "openshift.enabled=true",
		"--set", "openshift.nodeSCC.create=false", "--set", "openshift.nodeSCC.name=corp-csi-node")

	assert.NotContains(t, out, "kind: SecurityContextConstraints")
	assert.NotContains(t, out, "corp-csi-node-use", "no grant for an SCC the chart did not make")
	assert.Contains(t, out, `openshift.io/required-scc: "corp-csi-node"`)
}

// The node SCC is cluster-scoped, so its derived name carries the namespace.
// The documented install (release qumulo-csi in namespace qumulo-csi) used to
// get the release name twice over, qumulo-csi-qumulo-csi-qumulo-csi-driver-node,
// because the fullname already starts with the namespace. That case now drops
// the repeated prefix; every other release keeps <namespace>-<fullname>-node,
// and the SCC, its use-role and the pod's required-scc always agree.
func TestOpenShiftNodeSCCNameDoesNotRepeatTheRelease(t *testing.T) {
	for _, tc := range []struct{ release, namespace, want string }{
		{"qumulo-csi", "qumulo-csi", "qumulo-csi-qumulo-csi-driver-node"},
		{"qumulo-csi-driver", "qumulo-csi-driver", "qumulo-csi-driver-qumulo-csi-driver-node"},
		{"qcsi", "qumulo-csi", "qumulo-csi-qcsi-qumulo-csi-driver-node"},
		{"qumulo-csi", "storage", "storage-qumulo-csi-qumulo-csi-driver-node"},
		{"a-x", "a", "a-a-x-qumulo-csi-driver-node"},
	} {
		t.Run(tc.release+"/"+tc.namespace, func(t *testing.T) {
			out, err := exec.Command("helm", "template", tc.release, chartPath, "--namespace", tc.namespace,
				"--set", "openshift.enabled=true").CombinedOutput()
			require.NoError(t, err, "helm template failed: %s", out)
			s := string(out)
			assert.Contains(t, s, "kind: SecurityContextConstraints\nmetadata:\n  name: "+tc.want+"\n")
			assert.Contains(t, s, "  name: "+tc.want+"-use\n")
			assert.Contains(t, s, `resourceNames: ["`+tc.want+`"]`)
			assert.Contains(t, s, `openshift.io/required-scc: "`+tc.want+`"`)
			assertBindingFollowsRole(t, s, tc.want+"-use")
		})
	}
}

// roleRef is immutable. A RoleBinding with a fixed name could not follow a
// renamed SCC: on OCP 4.22 the upgrade that shortened the SCC name failed with
// "cannot change roleRef" after the DaemonSet already required the new SCC,
// and the node pod it had replaced could not be admitted. The binding is named
// after the role it binds, so any rename -- derived or openshift.nodeSCC.name
// -- creates a new binding instead of patching the old one.
func TestOpenShiftNodeSCCBindingIsRenamedWithTheSCC(t *testing.T) {
	for _, name := range []string{"", "corp-csi-node"} {
		t.Run("name="+name, func(t *testing.T) {
			out := renderOpenShift(t, "--set", "openshift.enabled=true", "--set", "openshift.nodeSCC.name="+name)
			want := "qumulo-csi-qumulo-csi-driver-node"
			if name != "" {
				want = name
			}
			assertBindingFollowsRole(t, out, want+"-use")
		})
	}
}

func assertBindingFollowsRole(t *testing.T, out, role string) {
	t.Helper()
	i := strings.Index(out, "kind: RoleBinding\nmetadata:\n  name: "+role+"\n")
	require.GreaterOrEqual(t, i, 0, "the node SCC's RoleBinding must be named %s, after the ClusterRole it binds", role)
	binding := out[i:]
	if j := strings.Index(binding, "\n---"); j >= 0 {
		binding = binding[:j]
	}
	assert.Contains(t, binding, "kind: ClusterRole\n  name: "+role+"\n")
}

// The SCC allows what the node pod needs and nothing it does not: no host PID
// or IPC, no added capabilities, only the pod's volume types, and the grant
// goes through RBAC in the release namespace rather than a users list.
func TestOpenShiftNodeSCCIsTight(t *testing.T) {
	out := renderOpenShift(t, "--set", "openshift.enabled=true")
	i := strings.Index(out, "kind: SecurityContextConstraints")
	require.GreaterOrEqual(t, i, 0)
	scc := out[i:]
	if j := strings.Index(scc, "\n---"); j >= 0 {
		scc = scc[:j]
	}

	for _, want := range []string{
		"allowHostPID: false", "allowHostIPC: false", "allowedCapabilities: []",
		"users: []", "groups: []", "  - runtime/default",
	} {
		assert.Contains(t, scc, want)
	}
	assert.NotContains(t, scc, "  - secret", "the node pod mounts no Secret")
	assert.Contains(t, out, "kind: RoleBinding\nmetadata:\n  name: qumulo-csi-qumulo-csi-driver-node-use\n  namespace: qumulo-csi")
}

// chart-verifier's required-annotations-present failed: Chart.yaml carried no
// annotations at all, so Red Hat's tooling had nothing to read.
// Read from `helm show chart`, so a packaging step that dropped the block would
// fail here too.
func TestOpenShiftCertificationAnnotations(t *testing.T) {
	out, err := exec.Command("helm", "show", "chart", chartPath).CombinedOutput()
	require.NoError(t, err, "helm show chart failed: %s", out)
	var chart struct {
		Icon        string            `yaml:"icon"`
		Annotations map[string]string `yaml:"annotations"`
	}
	require.NoError(t, yaml.Unmarshal(out, &chart))

	assert.True(t, strings.HasPrefix(chart.Icon, "https://"), "icon must be a public https URL, got %q", chart.Icon)
	for key, want := range map[string]string{
		"charts.openshift.io/name":                   "Qumulo CSI Driver",
		"charts.openshift.io/provider":               "Qumulo",
		"charts.openshift.io/providerType":           "partner",
		"charts.openshift.io/supportURL":             "https://care.qumulo.com",
		"charts.openshift.io/archs":                  "x86_64,aarch64",
		"charts.openshift.io/testedOpenShiftVersion": "4.22",
	} {
		assert.Equal(t, want, chart.Annotations[key], key)
	}
}

// The annotations are metadata only: adding them must not change one byte of
// what gets installed, on OpenShift or off it.
func TestOpenShiftCertificationAnnotationsDoNotRender(t *testing.T) {
	for _, args := range [][]string{nil, {"--api-versions", sccAPI}} {
		assert.NotContains(t, renderOpenShift(t, args...), "charts.openshift.io/")
	}
}
