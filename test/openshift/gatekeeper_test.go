//go:build openshift

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package openshift

// Clusters where a policy engine owns RBAC and the SCCs (the client request
// of 2026-11-05: OpenShift 4.22, a namespace UID range, a custom restricted
// SCC "restricted-csi", Gatekeeper refusing RoleBinding changes by the
// deployment identity, and Helm release Secrets also policed).

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	bindingsFile = "templates/rbac-bindings.yaml"
	gkSCC        = "qumulo-csi-node"
)

func isBinding(o Object) bool {
	return o.Kind() == "RoleBinding" || o.Kind() == "ClusterRoleBinding"
}

func objectKey(o Object) string {
	ns, _ := o.Dig("metadata", "namespace").(string)
	return o.Kind() + " " + ns + "/" + o.Name()
}

// The client's values: their restricted SCC for the controller, an SCC the
// platform team owns for the node plugin, and no bindings from the chart.
func gatekeeperValues(t *testing.T) Options {
	opts := openShiftValues(t)
	opts.ValuesFiles = append(opts.ValuesFiles, ValuesFile(t, "openshift-values-gatekeeper.yaml"))
	return opts
}

// TestCreateBindingsFalseRendersNoBinding: the install the deployment identity
// applies holds no RoleBinding and no ClusterRoleBinding at all, so a policy
// that refuses binding changes by that identity has nothing to refuse. Every
// ServiceAccount, Role and ClusterRole stays, so the platform team's bindings
// have something to point at.
func TestCreateBindingsFalseRendersNoBinding(t *testing.T) {
	for _, sets := range [][]string{
		{"rbac.createBindings=false"},
		{"rbac.createBindings=false", "sidecars.capacity.enabled=true", "tests.storageClass=x"},
		{"rbac.createBindings=false", "openshift.nodeSCC.create=false", "openshift.nodeSCC.name=corp"},
	} {
		t.Run(strings.Join(sets, ","), func(t *testing.T) {
			opts := openShiftValues(t)
			opts.Sets = sets
			off := Render(t, opts)
			for _, o := range off {
				assert.False(t, isBinding(o), "rbac.createBindings=false still rendered %s", objectKey(o))
			}

			opts.Sets = append(sets[1:], "rbac.createBindings=true")
			on := Render(t, opts)
			kept := map[string]bool{}
			for _, o := range off {
				kept[objectKey(o)] = true
			}
			for _, o := range on {
				if !isBinding(o) {
					assert.True(t, kept[objectKey(o)], "rbac.createBindings=false dropped %s, which is not a binding", objectKey(o))
				}
			}
		})
	}
}

// TestPlatformAppliedBindingsRestoreTheSameInstall: the bindings the platform
// team applies (rendered on their own with --show-only rbac-bindings.yaml),
// plus the install with createBindings=false, are exactly the default install,
// object for object. So nothing the controller, the node plugin or helm test
// is granted differs, and the admission and permission cases in this suite
// hold for both.
func TestPlatformAppliedBindingsRestoreTheSameInstall(t *testing.T) {
	for _, sets := range [][]string{
		nil,
		{"sidecars.capacity.enabled=true"},
		{"openshift.nodeSCC.create=false", "openshift.nodeSCC.name=corp"},
		{"tests.ready.enabled=false"},
	} {
		t.Run(strings.Join(sets, ","), func(t *testing.T) {
			opts := openShiftValues(t)
			opts.Sets = append(append([]string{}, sets...),
				"rbac.extraSecrets[0].secretName=legacy", "rbac.extraSecrets[0].secretNamespace=storage")
			full := Render(t, opts)

			opts.Sets = append(opts.Sets, "rbac.createBindings=false")
			install := Render(t, opts)

			// What the docs tell the platform team to run.
			opts.Sets = append(opts.Sets, "rbac.createBindings=true")
			platform := renderShowOnly(t, opts, bindingsFile)

			got := map[string]Object{}
			for _, o := range append(append(Manifest{}, install...), platform...) {
				_, dup := got[objectKey(o)]
				require.False(t, dup, "%s rendered twice", objectKey(o))
				got[objectKey(o)] = o
			}
			require.Len(t, got, len(full))
			for _, o := range full {
				assert.Equal(t, o, got[objectKey(o)], "%s differs", objectKey(o))
			}
			for _, o := range platform {
				assert.True(t, isBinding(o), "%s holds %s, which is not a binding", bindingsFile, objectKey(o))
			}
		})
	}
}

// TestClientValuesAreAdmitted: under the client's settings the controller runs
// under restricted-csi (a copy of restricted-v2: a uid from the namespace
// range, nothing privileged) with no fixed uid, and the node plugin is pinned
// to the platform team's SCC, on hydra_storage nodes only.
func TestClientValuesAreAdmitted(t *testing.T) {
	manifest := Render(t, gatekeeperValues(t))

	ctl := manifest.Named(t, "Deployment", "-controller")
	assert.Equal(t, "restricted-csi", requiredSCC(t, ctl))
	scc := RestrictedV2()
	scc.Name = "restricted-csi"
	pod := PodTemplate(t, ctl)
	assert.Empty(t, scc.Admit(pod), "restricted-csi refuses the controller")
	for _, name := range pod.ContainerNames() {
		assert.Nil(t, pod.Effective(t, name).RunAsUser, "container %s pins a uid restricted-csi refuses", name)
	}

	node := manifest.Named(t, "DaemonSet", "-node")
	assert.Equal(t, gkSCC, requiredSCC(t, node))
	assert.Empty(t, manifest.Kinds("SecurityContextConstraints"))
	for _, w := range []Object{ctl, node} {
		assert.Equal(t, "true", w.Dig("spec", "template", "spec", "nodeSelector", "hydra_storage"),
			"%s is not kept on hydra_storage nodes", w.Name())
	}

	// helm test runs under the same SCC as the controller, without a uid.
	for _, o := range manifest.Kinds("Pod") {
		ann, _ := asMap(o.Dig("metadata", "annotations"))
		assert.Equal(t, "restricted-csi", ann["openshift.io/required-scc"], "test pod %s", o.Name())
		_, pinned := asMap(o.Dig("spec", "securityContext"))
		require.True(t, pinned)
		assert.Nil(t, o.Dig("spec", "securityContext", "runAsUser"), "test pod %s pins a uid", o.Name())
	}
}

// TestAssignControllerUIDOverridesTheName: a custom SCC that assigns a uid
// from the range but is not named restricted* needs assignControllerUID=true;
// a restricted* SCC that does not assign one can say false.
func TestAssignControllerUIDOverridesTheName(t *testing.T) {
	for _, tc := range []struct {
		scc, assign string
		dropped     bool
	}{
		{"csi-assigned", "auto", false},
		{"csi-assigned", "true", true},
		{"restricted-v2", "auto", true},
		{"restricted-v2", "false", false},
		{"restricted-csi", "auto", true},
	} {
		t.Run(tc.scc+"/"+tc.assign, func(t *testing.T) {
			opts := openShiftValues(t)
			opts.Sets = []string{"openshift.controllerSCC=" + tc.scc, "openshift.assignControllerUID=" + tc.assign}
			manifest := Render(t, opts)
			pod := PodTemplate(t, manifest.Named(t, "Deployment", "-controller"))
			for _, name := range pod.ContainerNames() {
				sc := pod.Effective(t, name)
				if tc.dropped {
					assert.Nil(t, sc.RunAsUser, "container %s", name)
				} else {
					require.NotNil(t, sc.RunAsUser, "container %s", name)
					assert.EqualValues(t, 65532, *sc.RunAsUser)
				}
				assert.True(t, boolValue(sc.RunAsNonRoot))
			}
			if tc.dropped {
				scc := RestrictedV2()
				scc.Name = tc.scc
				assert.Empty(t, scc.Admit(pod), "a uid-assigning SCC refuses the controller")
			}
		})
	}
}

// TestUpgradeFromAChartWithoutTheNewValues: --reuse-values from a chart that
// predates them carries no rbac.createBindings, rbac.checkBindings or
// openshift.assignControllerUID. The render must be the default install.
func TestUpgradeFromAChartWithoutTheNewValues(t *testing.T) {
	opts := openShiftValues(t)
	want := Render(t, opts)
	opts.Sets = []string{"rbac.createBindings=null", "rbac.checkBindings=null", "openshift.assignControllerUID=null"}
	got := Render(t, opts)
	require.Equal(t, len(want), len(got))
	for i := range want {
		assert.Equal(t, want[i], got[i])
	}
}

// renderShowOnly renders one template file of the chart, as
// `helm template --show-only` does for the platform team.
func renderShowOnly(t *testing.T, opts Options, file string) Manifest {
	t.Helper()
	args := []string{"template", opts.release(), ChartPath(t), "--namespace", opts.namespace(),
		"--kube-version", opts.kubeVersion(), "--api-versions", SCCAPI, "--show-only", file}
	for _, f := range opts.ValuesFiles {
		args = append(args, "-f", f)
	}
	for _, v := range opts.Sets {
		args = append(args, "--set", v)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	require.NoError(t, err, "helm template --show-only %s: %s", file, out)
	return parse(t, string(out))
}
