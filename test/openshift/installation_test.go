//go:build openshift

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package openshift is the OpenShift support tier: whether the installer puts
// the driver inside OpenShift's permitted security settings on its own,
// whether it asks for only the access it needs, and whether volumes are usable
// across the application security configurations OpenShift workloads run under.
//
// It is a tier of its own, behind the `openshift` build tag, sharing no
// package or fixture with test/chart or any other tier, so
// `make test` and a plain `go test ./...` neither run nor compile it. It is a
// gate in `make ci` and in CI's chart job, since it needs only helm. Run it
// alone with `make openshift-test`.
//
// It differs from test/chart/openshift_test.go in kind, not in degree. That
// file asserts the templates emit the right YAML. This one parses the install
// and evaluates it against models of OpenShift's SecurityContextConstraints
// and Pod Security Admission -- including the SCC the chart renders itself --
// so the question it answers is whether admission would accept the install,
// not whether the text looks right.
//
// It needs helm. It needs no cluster, no Qumulo array and no network, which
// also bounds it: README.md lists what it cannot settle, and the list is not
// short. A green run here is not OpenShift support.
package openshift

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openShiftValues is the recommended install, used by most cases here.
func openShiftValues(t *testing.T) Options {
	t.Helper()

	return Options{
		Release:     "qcsi",
		Namespace:   "qumulo-csi",
		ValuesFiles: []string{ValuesFile(t, "openshift-values.yaml")},
	}
}

// TestNodePluginIsAdmittedByTheSCCTheInstallShips is the headline case.
//
// The node plugin mounts NFS in the host namespace: privileged, hostNetwork,
// hostPath into the kubelet directory. No SCC OpenShift ships to an ordinary
// ServiceAccount admits that, and the old answer was to have an administrator
// run `oc adm policy add-scc-to-user privileged -z ...`, which hands the node
// pods every privilege in the cluster and is exactly the manual workaround
// this installer exists to remove.
//
// So: read the SCC the chart renders, and evaluate the node pod against it.
// Empty means the install carries the permission it needs and an administrator
// runs nothing.
func TestNodePluginIsAdmittedByTheSCCTheInstallShips(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	scc := SCCFromManifest(t, manifest)
	pod := PodTemplate(t, manifest.Named(t, "DaemonSet", "-node"))

	assert.Empty(t, scc.Admit(pod),
		"the SCC the chart ships does not admit the node pod the chart ships, so the install still needs a manual binding")

	// And it is the SCC the pod is actually pinned to -- an SCC that admits
	// the pod but is not the one OpenShift selects proves nothing.
	assert.Equal(t, scc.Name, requiredSCC(t, manifest.Named(t, "DaemonSet", "-node")),
		"the node pod is pinned to a different SCC than the one the chart renders")
}

// TestControllerIsAdmittedByTheSCCItIsPinnedTo.
//
// The controller ships hardened but pinned `runAsUser: 65532`, and
// restricted-v2 is MustRunAsRange over the namespace's assigned uid range,
// which does not contain it. The installer drops the fixed uid for any
// restricted* SCC so OpenShift assigns one, and keeps runAsNonRoot so dropping
// the uid does not quietly allow root.
func TestControllerIsAdmittedByTheSCCItIsPinnedTo(t *testing.T) {
	manifest := Render(t, openShiftValues(t))
	deployment := manifest.Named(t, "Deployment", "-controller")
	pod := PodTemplate(t, deployment)

	require.Equal(t, "restricted-v2", requiredSCC(t, deployment),
		"the recommended install no longer pins restricted-v2")

	assert.Empty(t, RestrictedV2().Admit(pod),
		"restricted-v2 refuses the controller, so the install needs an SCC binding it does not ship")

	for _, name := range pod.ContainerNames() {
		sc := pod.Effective(t, name)
		assert.Nil(t, sc.RunAsUser,
			"container %s still pins a uid; restricted-v2 assigns one and refuses any other", name)
		assert.True(t, boolValue(sc.RunAsNonRoot),
			"container %s lost runAsNonRoot along with the uid, which weakens the pod rather than adapting it", name)
	}
}

// TestInstallLeavesNothingForAnAdministratorToRun.
//
// The complete chain, asserted end to end: the SCC exists, a ClusterRole
// grants `use` on it by name, and a RoleBinding gives that to the node
// ServiceAccount in the release namespace. With all three present there is no
// `oc adm policy` step; with any one missing the DaemonSet is refused and the
// cause is somewhere else entirely.
func TestInstallLeavesNothingForAnAdministratorToRun(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	scc := SCCFromManifest(t, manifest)
	node := PodTemplate(t, manifest.Named(t, "DaemonSet", "-node"))

	var granted, bound bool
	for _, role := range manifest.Kinds("ClusterRole") {
		for _, r := range rulesOf(t, role) {
			if r.Resource != "securitycontextconstraints" || !r.Verbs["use"] {
				continue
			}
			assert.Equal(t, []string{scc.Name}, r.ResourceNames,
				"the SCC grant is not confined to the chart's own SCC")
			granted = true

			for _, b := range manifest.Kinds("RoleBinding") {
				ref, _ := asMap(b["roleRef"])
				if ref.str("name") != role.Name() {
					continue
				}
				for _, s := range subjectsOf(b) {
					if s == node.ServiceAccount {
						bound = true
					}
				}
			}
		}
	}

	assert.True(t, granted, "no ClusterRole grants `use` on the chart's SCC, so nothing can select it")
	assert.True(t, bound, "the SCC grant is not bound to the node ServiceAccount, so the DaemonSet is still refused")
}

// TestPlainKubernetesInstallIsUnchanged.
//
// `openshift.enabled: auto` decides from the cluster's API list. The risk in a
// detection like that is not the OpenShift path -- it is a non-OpenShift
// cluster that starts rendering SCCs, or a controller that quietly loses its
// uid on a platform where nothing assigns one.
func TestPlainKubernetesInstallIsUnchanged(t *testing.T) {
	opts := openShiftValues(t)
	opts.ValuesFiles = nil // the recommended file pins openshift.enabled=true
	opts.PlainKubernetes = true

	manifest := Render(t, opts)

	assert.Empty(t, manifest.Kinds("SecurityContextConstraints"),
		"a cluster that serves no SCC API got an SCC anyway")

	pod := PodTemplate(t, manifest.Named(t, "Deployment", "-controller"))
	for _, name := range pod.ContainerNames() {
		sc := pod.Effective(t, name)
		require.NotNil(t, sc.RunAsUser, "container %s lost its uid off OpenShift, where nothing assigns one", name)
		assert.EqualValues(t, 65532, *sc.RunAsUser, "container %s", name)
	}

	for _, kind := range []string{"Deployment", "DaemonSet"} {
		for _, o := range manifest.Kinds(kind) {
			assert.Empty(t, requiredSCC(t, o), "%s/%s is pinned to an SCC off OpenShift", kind, o.Name())
		}
	}
}

// TestAdminSuppliedSCCStopsShortOfCreatingOrGranting.
//
// Where SCCs are centrally governed, a chart that creates one is refused at
// install. The chart then renders neither the SCC nor the grant and pins the
// node pods to the administrator's. What it must not do is pin them to an SCC
// and also quietly keep creating its own.
func TestAdminSuppliedSCCStopsShortOfCreatingOrGranting(t *testing.T) {
	opts := openShiftValues(t)
	opts.ValuesFiles = append(opts.ValuesFiles, ValuesFile(t, "openshift-values-admin-scc.yaml"))

	manifest := Render(t, opts)

	assert.Empty(t, manifest.Kinds("SecurityContextConstraints"),
		"the chart still creates an SCC when the administrator supplies one")

	for _, role := range manifest.Kinds("ClusterRole") {
		for _, r := range rulesOf(t, role) {
			assert.NotEqual(t, "securitycontextconstraints", r.Resource,
				"the chart still grants SCC use when the administrator manages the grant")
		}
	}

	assert.Equal(t, "corp-csi-node", requiredSCC(t, manifest.Named(t, "DaemonSet", "-node")),
		"the node pods are not pinned to the administrator's SCC")

	// What the administrator has to allow, derived from the pod rather than
	// described, so the hand-off is checkable.
	pod := PodTemplate(t, manifest.Named(t, "DaemonSet", "-node"))
	t.Logf("the admin-supplied SCC must allow: privileged=%v hostNetwork=%v hostPorts=%v volumes=%v",
		pod.IsPrivileged(), pod.HostNetwork, pod.HostPorts, pod.VolumeTypes)
}

// TestChartRendersOnEveryOpenShift422KubeVersion.
//
// OpenShift reports its Kubernetes version with a distribution suffix, and the
// `-0` in the chart's `>=1.35.0-0` floor is what admits one. Without it a
// 1.35.6 cluster is refused by a chart that supports it perfectly well.
func TestChartRendersOnEveryOpenShift422KubeVersion(t *testing.T) {
	for _, v := range []string{"1.35.0", "1.35.6", "1.36.0"} {
		t.Run(v, func(t *testing.T) {
			opts := openShiftValues(t)
			opts.KubeVersion = v

			assert.NotEmpty(t, Render(t, opts), "render produced no objects at %s", v)
		})
	}
}

// TestInstallNeedsNoAPIGroupOpenShiftLacks. A group OpenShift does not serve
// would fail at apply, after part of the install has already landed.
func TestInstallNeedsNoAPIGroupOpenShiftLacks(t *testing.T) {
	shipped := map[string]bool{
		"v1":                           true,
		"apps/v1":                      true,
		"policy/v1":                    true,
		"rbac.authorization.k8s.io/v1": true,
		"storage.k8s.io/v1":            true,
		"scheduling.k8s.io/v1":         true,
		"networking.k8s.io/v1":         true,
		"security.openshift.io/v1":     true, // the SCC the chart renders
		"snapshot.storage.k8s.io/v1":   true, // via cluster-storage-operator
	}

	for _, group := range Render(t, openShiftValues(t)).APIGroups() {
		assert.True(t, shipped[group],
			"the install renders %s, which plain OpenShift does not serve -- it needs an operator or CRD installed first", group)
	}
}

// TestInstallCreatesNoCustomResourceDefinition.
//
// OpenShift ships the external-snapshotter CRDs and a cluster snapshot
// controller itself. A chart that also created them would fight
// cluster-storage-operator over cluster-scoped objects.
func TestInstallCreatesNoCustomResourceDefinition(t *testing.T) {
	opts := openShiftValues(t)
	opts.Sets = []string{
		"volumeSnapshotClasses[0].name=qumulo-snapclass",
		"volumeSnapshotClasses[0].deletionPolicy=Delete",
		// The driver keeps no fallback token, so a snapshot class names its
		// own Secret. A chart requirement, not an OpenShift one.
		"volumeSnapshotClasses[0].secretName=qumulo-token",
	}
	opts.APIVersions = []string{"snapshot.storage.k8s.io/v1"}

	manifest := Render(t, opts)

	assert.Empty(t, manifest.Kinds("CustomResourceDefinition"),
		"the chart creates snapshot CRDs; on OpenShift they are owned by cluster-storage-operator")
	assert.NotEmpty(t, manifest.Kinds("VolumeSnapshotClass"),
		"the snapshot class was not created, so snapshots are not actually enabled by this values file")
}

// TestReplicationStaysOffUntilTheODFQuestionIsSettled.
//
// The API groups are named openshift but csi-addons is not part of OpenShift;
// it arrives with OpenShift Data Foundation, cluster-scoped and at ODF's
// version. On by default is how a second copy ends up fighting ODF's.
func TestReplicationStaysOffUntilTheODFQuestionIsSettled(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	for _, o := range manifest {
		apiVersion, _ := o["apiVersion"].(string)
		assert.False(t, strings.Contains(apiVersion, "replication.storage.openshift.io"),
			"the recommended install emits %s/%s, which needs csi-addons that plain OpenShift does not ship",
			apiVersion, o.Name())
	}

	for _, o := range manifest.Kinds("ClusterRole") {
		for _, r := range rulesOf(t, o) {
			assert.NotEqual(t, "csiaddons.openshift.io", r.Group,
				"%s grants rights on csi-addons while replication is off", o.Name())
		}
	}
}

// TestNodePluginUsesTheRHCOSKubeletDirectory.
//
// RHCOS runs a stock kubelet at /var/lib/kubelet. A wrong path produces a
// DaemonSet that rolls out, reports healthy and never registers the plugin
// -- the most expensive way to get this wrong.
func TestNodePluginUsesTheRHCOSKubeletDirectory(t *testing.T) {
	raw, err := RenderRaw(t, openShiftValues(t))
	require.NoError(t, err)

	assert.Contains(t, raw, "--kubelet-registration-path=/var/lib/kubelet/plugins/csi.qumulo.com/csi.sock",
		"the registration path is not the RHCOS kubelet directory")
}

// TestEveryImageIsMirrorableForADisconnectedCluster.
//
// Disconnected OpenShift mirrors by registry, through an ImageDigestMirrorSet.
// An image with no registry host resolves to docker.io and is missed by every
// mirror rule, so the install half-works: the mirrored images pull, that one
// does not.
func TestEveryImageIsMirrorableForADisconnectedCluster(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	registries := map[string]bool{}
	for _, kind := range []string{"Deployment", "DaemonSet"} {
		for _, o := range manifest.Kinds(kind) {
			for _, c := range PodTemplate(t, o).Containers {
				host := strings.SplitN(c.Image, "/", 2)[0]
				assert.True(t, strings.ContainsAny(host, ".:"),
					"container %s uses image %q with no registry host; it cannot be mirrored", c.Name, c.Image)
				registries[host] = true
			}
		}
	}

	t.Logf("registries to mirror for a disconnected install: %v", keys(registries))
}

// TestControllerHARidesOutMachineConfigOperatorReboots.
//
// The Machine Config Operator drains and reboots every worker on a routine
// cluster update, serially. Two replicas with a PodDisruptionBudget keeps
// provisioning alive across that -- but a PDB that cannot be satisfied blocks
// the drain and stalls the update instead, which is worse.
func TestControllerHARidesOutMachineConfigOperatorReboots(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	replicas, ok := manifest.Named(t, "Deployment", "-controller").Dig("spec", "replicas").(int)
	require.True(t, ok, "the controller has no replica count")
	assert.GreaterOrEqual(t, replicas, 2, "one controller replica cannot survive an MCO node drain")

	pdbs := manifest.Kinds("PodDisruptionBudget")
	require.Len(t, pdbs, 1, "expected exactly one PodDisruptionBudget")

	if maxUnavailable := pdbs[0].Dig("spec", "maxUnavailable"); maxUnavailable != nil {
		assert.Equal(t, 1, maxUnavailable,
			"a PDB allowing no disruption blocks every MCO drain and stalls cluster updates")
	}
}

// requiredSCC reads the openshift.io/required-scc annotation off a workload's
// pod template. OpenShift selects the named SCC rather than the highest-ranked
// one the ServiceAccount could use, which is what makes the admission verdicts
// in this tier about the SCC that will actually be applied.
func requiredSCC(t *testing.T, o Object) string {
	t.Helper()

	v, _ := o.Dig("spec", "template", "metadata", "annotations", "openshift.io/required-scc").(string)
	return v
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestUpgradeFromAChartWithoutTheOpenShiftBlock (I-1).
//
// `helm upgrade --reuse-values` keeps the old release's values and ignores the
// new chart's values.yaml, so an upgrade from a release made before the
// openshift block existed renders with no `openshift` key at all. That was a
// nil pointer in isOpenShift and the upgrade failed. `--set openshift=null`
// removes the key the same way. The render must succeed and give the same
// install the defaults give.
func TestUpgradeFromAChartWithoutTheOpenShiftBlock(t *testing.T) {
	opts := openShiftValues(t)
	opts.ValuesFiles = nil
	opts.Sets = []string{"openshift=null", "node.healthPort=null"}

	out, err := RenderRaw(t, opts)
	require.NoError(t, err, "a --reuse-values upgrade from a pre-OpenShift release fails to render")
	manifest := parse(t, out)

	require.Len(t, manifest.Kinds("SecurityContextConstraints"), 1,
		"openshift.enabled defaults to auto, and the cluster serves SCCs")
	assert.Contains(t, out, `openshift.io/required-scc: "restricted-v2"`)
	assert.Contains(t, out, `openshift.io/required-scc: "qumulo-csi-qcsi-qumulo-csi-driver-node"`)
	assert.Contains(t, out, "--http-endpoint=$(POD_IP):29653", "node.healthPort defaults to 29653")

	// And an explicit false still wins over the default.
	opts.Sets = []string{"openshift.nodeSCC.create=false"}
	assert.Empty(t, Render(t, opts).Kinds("SecurityContextConstraints"),
		"nodeSCC.create=false still rendered the chart's SCC")
}

// TestNodeHealthPortIsAValueBoundToTheNodeAddress (I-2).
//
// The node pod is hostNetwork, so its livenessprobe /healthz is a host port on
// every node. It was a fixed :29653 on every interface, which is also the
// upstream csi-driver-nfs node health port. Now a value, bound to the node's
// own address, and the probe follows it.
func TestNodeHealthPortIsAValueBoundToTheNodeAddress(t *testing.T) {
	opts := openShiftValues(t)
	opts.Sets = []string{"node.healthPort=29700"}

	all, err := RenderRaw(t, opts)
	require.NoError(t, err)
	// The node DaemonSet alone: the controller's sidecars bind every interface
	// of their own pod, which is not the host's.
	i := strings.Index(all, "kind: DaemonSet")
	require.GreaterOrEqual(t, i, 0)
	out := all[i:]
	if j := strings.Index(out, "\n---"); j >= 0 {
		out = out[:j]
	}
	assert.Contains(t, out, "--http-endpoint=$(POD_IP):29700")
	assert.Contains(t, out, "containerPort: 29700")
	assert.Contains(t, out, "port: 29700", "the driver's livenessProbe still probes the old port")
	assert.NotContains(t, out, "29653", "the old fixed port is still rendered somewhere")
	assert.NotContains(t, out, "--http-endpoint=:", "a health endpoint binds every interface of the host")

	opts.Sets = []string{"node.healthPort=29655", "metrics.enabled=true"}
	_, err = RenderRaw(t, opts)
	require.Error(t, err, "a health port equal to the node metrics port rendered")
	assert.Contains(t, err.Error(), "node.healthPort")
}

// TestProxyAndTrustedCAAreOptional (I-6).
//
// A cluster behind a proxy, or with its own CA, needs both; every other
// cluster must get exactly the install it got before. Off by default, the
// controller has no proxy variables, no CA file and no extra ConfigMap.
func TestProxyAndTrustedCAAreOptional(t *testing.T) {
	controllerEnv := func(m Manifest) string {
		t.Helper()
		pod, _ := asMap(m.Named(t, "Deployment", "-controller").Dig("spec", "template", "spec"))
		for _, c := range sliceOf(pod["containers"]) {
			co, _ := asMap(c)
			if co.str("name") == "qumulo-csi-driver" {
				return fmt.Sprint(co["env"])
			}
		}
		t.Fatal("no qumulo-csi-driver container in the controller")
		return ""
	}

	off := Render(t, openShiftValues(t))
	assert.NotContains(t, controllerEnv(off), "PROXY", "the default install sets a proxy")
	assert.NotContains(t, controllerEnv(off), "SSL_CERT_FILE", "the default install replaces the CA bundle")
	for _, cm := range off.Kinds("ConfigMap") {
		assert.NotContains(t, cm.Name(), "trusted-ca", "the default install creates the trusted-CA ConfigMap")
	}

	opts := openShiftValues(t)
	opts.Sets = []string{
		"controller.proxy.httpsProxy=http://proxy.example:3128",
		"controller.proxy.noProxy=10.0.0.13",
		"openshift.trustedCABundle=true",
	}
	on := Render(t, opts)
	env := controllerEnv(on)
	assert.Contains(t, env, "http://proxy.example:3128")
	assert.Contains(t, env, "10.0.0.13,.svc,.cluster.local,$(KUBERNETES_SERVICE_HOST)",
		"NO_PROXY must keep the driver's own Kubernetes API calls direct")
	assert.Contains(t, env, "/etc/qumulo-ca/ca-bundle.crt")

	cm := on.Named(t, "ConfigMap", "-trusted-ca")
	meta, _ := asMap(cm["metadata"])
	labels, _ := asMap(meta["labels"])
	assert.Equal(t, "true", labels.str("config.openshift.io/inject-trusted-cabundle"))
	assert.Nil(t, cm["data"], "the chart must leave the bundle to OpenShift, or an upgrade would empty it")

	// The trusted CA is OpenShift's; off OpenShift it renders nothing.
	plain := openShiftValues(t)
	plain.ValuesFiles = nil
	plain.PlainKubernetes = true
	plain.Sets = []string{"openshift.trustedCABundle=true"}
	assert.NotContains(t, controllerEnv(Render(t, plain)), "SSL_CERT_FILE")

	for _, conflict := range []string{"qumulo.caBundle.existingConfigMap=my-ca", "qumulo.insecureSkipTlsVerify=true"} {
		opts.Sets = []string{"openshift.trustedCABundle=true", conflict}
		_, err := RenderRaw(t, opts)
		assert.Error(t, err, "openshift.trustedCABundle rendered together with %s", conflict)
	}
}

// TestMetricsPolicyAdmitsOpenShiftMonitoring (I-5).
//
// The NetworkPolicy's default peer is a namespace called `monitoring`, which
// OpenShift does not have: left as is, it blocks every scrape. On OpenShift
// the default becomes the two monitoring namespaces it does have; a peer the
// operator chose is kept.
func TestMetricsPolicyAdmitsOpenShiftMonitoring(t *testing.T) {
	opts := openShiftValues(t)
	opts.Sets = []string{"metrics.enabled=true", "metrics.networkPolicy.enabled=true"}
	np := fmt.Sprint(Render(t, opts).Named(t, "NetworkPolicy", "-controller-metrics")["spec"])
	assert.Contains(t, np, "openshift-user-workload-monitoring")
	assert.Contains(t, np, "openshift-monitoring")
	assert.NotContains(t, np, "name:monitoring]")

	opts.Sets = append(opts.Sets, "metrics.networkPolicy.from[0].namespaceSelector.matchLabels.team=obs")
	np = fmt.Sprint(Render(t, opts).Named(t, "NetworkPolicy", "-controller-metrics")["spec"])
	assert.Contains(t, np, "team:obs")
	assert.NotContains(t, np, "openshift-user-workload-monitoring", "an operator-set peer was replaced")
}
