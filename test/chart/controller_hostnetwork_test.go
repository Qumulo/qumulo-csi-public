//go:build chart

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// controller.hostNetwork: a cluster whose
// arrays are reachable only from the nodes' own routes needs the controller
// on the node's network. An earlier schema refused the value outright.

func renderController(t *testing.T, args ...string) string {
	t.Helper()
	out, err := render(t, "templates/controller.yaml", args...)
	require.NoError(t, err, "helm template failed: %s", out)
	return out
}

// Off by default: the pod network, no dnsPolicy, the soft anti-affinity and
// Kubernetes' default rollout, exactly as before.
func TestControllerHostNetworkOffByDefault(t *testing.T) {
	out := renderController(t)
	assert.NotContains(t, out, "hostNetwork:")
	assert.NotContains(t, out, "dnsPolicy:")
	assert.NotContains(t, out, "maxSurge")
	assert.Contains(t, out, "preferredDuringSchedulingIgnoredDuringExecution")
	assert.NotContains(t, out, "requiredDuringSchedulingIgnoredDuringExecution")
	assert.NotContains(t, out, "controller-host-network")
}

func TestControllerHostNetworkRendersThePodSpec(t *testing.T) {
	out := renderController(t, "--set", "controller.hostNetwork=true")
	assert.Contains(t, out, "\n      hostNetwork: true\n")
	assert.Contains(t, out, "\n      dnsPolicy: ClusterFirstWithHostNet\n", "the sidecars must still resolve the Kubernetes API")
	// Two replicas on one node would both bind the same node ports.
	assert.Contains(t, out, "requiredDuringSchedulingIgnoredDuringExecution")
	assert.NotContains(t, out, "preferredDuringSchedulingIgnoredDuringExecution")
	// Only between host-network replicas: the rule matches the pod label,
	// which a pod-network replica does not carry, so turning hostNetwork off
	// is not kept off every node by the old pods' rule.
	assert.Equal(t, 2, strings.Count(out, `csi.qumulo.com/controller-host-network: "true"`), "pod label and anti-affinity selector")
	// A surge pod would need a node no replica holds the ports on.
	assert.Contains(t, out, "maxSurge: 0")
	assert.Contains(t, out, "maxUnavailable: 1")
}

func TestControllerHostNetworkExplicitValuesWin(t *testing.T) {
	out := renderController(t, "--set", "controller.hostNetwork=true", "--set", "controller.dnsPolicy=Default",
		"--set", "controller.strategy.type=Recreate", "--set", "controller.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key=storage",
		"--set", "controller.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator=Exists")
	assert.Contains(t, out, "dnsPolicy: Default")
	assert.Contains(t, out, "type: Recreate")
	assert.NotContains(t, out, "maxSurge")
	assert.NotContains(t, out, "podAntiAffinity", "controller.affinity is passed verbatim")
}

func TestControllerHostNetworkValuesAreTyped(t *testing.T) {
	for _, set := range []string{"controller.hostNetwork=yes", "controller.dnsPolicy=Bogus"} {
		out, err := render(t, "templates/controller.yaml", "--set", set)
		require.Error(t, err, set)
		assert.Contains(t, out, "controller", set)
	}
}

// On OpenShift host networking selects hostnetwork-v2 (no privilege, uid from
// the namespace's range), drops the fixed uid, and grants the SCC to the
// controller service account alone. The helm test pods stay on restricted-v2.
func TestControllerHostNetworkOnOpenShift(t *testing.T) {
	out := renderOpenShift(t, "--set", "openshift.enabled=true", "--set", "controller.hostNetwork=true", "--set", "tests.storageClass=x")
	ctrl := docFor(t, out, "Deployment", "qumulo-csi-qumulo-csi-driver-controller")
	assert.Contains(t, ctrl, `openshift.io/required-scc: "hostnetwork-v2"`)
	assert.NotContains(t, ctrl, "runAsUser: 65532", "hostnetwork-v2 assigns a uid from the namespace's range and refuses any other")

	grant := docFor(t, out, "RoleBinding", "qumulo-csi-qumulo-csi-driver-controller-scc")
	assert.Contains(t, grant, "name: system:openshift:scc:hostnetwork-v2")
	assert.Contains(t, grant, "name: qumulo-csi-qumulo-csi-driver-controller\n")
	assert.NotContains(t, grant, "driver-node", "the controller's grant names only the controller")
	assert.Contains(t, grant, "# Source: qumulo-csi-driver/templates/rbac-bindings.yaml", "every binding comes from one file")

	for _, name := range []string{"qumulo-csi-qumulo-csi-driver-test-ready", "qumulo-csi-qumulo-csi-driver-test"} {
		pod := docFor(t, out, "Pod", name)
		assert.Contains(t, pod, `openshift.io/required-scc: "restricted-v2"`, "%s never uses the host network", name)
		assert.NotContains(t, pod, "runAsUser: 65532", name)
	}

	// The node's SCC grant is unchanged and separate.
	node := docFor(t, out, "RoleBinding", "qumulo-csi-qumulo-csi-driver-node-use")
	assert.NotContains(t, node, "driver-controller")
}

func TestControllerHostNetworkGrantOnlyWithHostNetworkOnOpenShift(t *testing.T) {
	assert.NotContains(t, renderOpenShift(t, "--set", "openshift.enabled=true"), "controller-scc")
	assert.NotContains(t, renderOpenShift(t, "--set", "openshift.enabled=false", "--set", "controller.hostNetwork=true"), "controller-scc")
}

func TestControllerHostNetworkCustomSCCAndRole(t *testing.T) {
	out := renderOpenShift(t, "--set", "openshift.enabled=true", "--set", "controller.hostNetwork=true",
		"--set", "openshift.controllerSCC=hostnet-csi", "--set", "openshift.assignControllerUID=true",
		"--set", "openshift.controllerSCCRole=use-hostnet-csi", "--set", "openshift.testSCC=restricted-csi", "--set", "tests.storageClass=x")
	assert.Contains(t, docFor(t, out, "Deployment", "qumulo-csi-qumulo-csi-driver-controller"), `openshift.io/required-scc: "hostnet-csi"`)
	assert.Contains(t, docFor(t, out, "RoleBinding", "qumulo-csi-qumulo-csi-driver-controller-scc"), "name: use-hostnet-csi")
	assert.Contains(t, docFor(t, out, "Pod", "qumulo-csi-qumulo-csi-driver-test-ready"), `openshift.io/required-scc: "restricted-csi"`)
}

// rbac.createBindings=false: the chart renders no binding, the controller's
// grant included; the platform team gets it from the same list.
func TestControllerHostNetworkGrantSkippedWithoutBindings(t *testing.T) {
	args := []string{"--set", "openshift.enabled=true", "--set", "controller.hostNetwork=true", "--set", "rbac.createBindings=false"}
	assert.NotContains(t, renderOpenShift(t, args...), "kind: RoleBinding")
	// NOTES.txt and the live check read the same list (bindingList), so the
	// grant is listed for the platform team exactly as rbac-bindings.yaml
	// would render it with createBindings=true.
	on := renderOpenShift(t, "--set", "openshift.enabled=true", "--set", "controller.hostNetwork=true", "--set", "rbac.createBindings=true",
		"--show-only", "templates/rbac-bindings.yaml")
	assert.Contains(t, on, "name: qumulo-csi-qumulo-csi-driver-controller-scc")
}

// restricted-v2 refuses host networking: an explicit restricted SCC (most
// often carried by --reuse-values) is refused at install, not left Pending.
func TestControllerHostNetworkRefusesARestrictedSCC(t *testing.T) {
	for _, scc := range []string{"restricted-v2", "restricted-csi"} {
		out, err := render(t, "templates/controller.yaml", "--set", "openshift.enabled=true", "--set", "controller.hostNetwork=true", "--set", "openshift.controllerSCC="+scc)
		require.Error(t, err, scc)
		assert.Contains(t, out, "refuses host networking", scc)
	}
}

// The csi-addons port becomes a node port too, so it must not share a number
// with the node plugin's host ports.
func TestControllerHostNetworkCSIAddonsPortClash(t *testing.T) {
	out, err := render(t, "templates/controller.yaml", "--set", "controller.hostNetwork=true",
		"--set", "sidecars.csiAddons.enabled=true", "--set", "sidecars.csiAddons.controllerPort=29653")
	require.Error(t, err)
	assert.Contains(t, out, "sidecars.csiAddons.controllerPort is 29653")
	_, err = render(t, "templates/controller.yaml", "--set", "sidecars.csiAddons.enabled=true", "--set", "sidecars.csiAddons.controllerPort=29653")
	assert.NoError(t, err, "on the pod network the port is the pod's own")
}

// docFor returns the rendered document of that kind and metadata.name.
func docFor(t *testing.T, out, kind, name string) string {
	t.Helper()
	for _, doc := range strings.Split(out, "\n---\n") {
		if strings.Contains(doc, "\nkind: "+kind+"\n") && strings.Contains(doc, "\n  name: "+name+"\n") {
			return doc
		}
	}
	require.Failf(t, "not rendered", "%s %s", kind, name)
	return ""
}
