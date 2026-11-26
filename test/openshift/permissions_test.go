//go:build openshift

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package openshift

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rule is one RBAC policy rule, flattened to one entry per group/resource.
type rule struct {
	Group         string
	Resource      string
	ResourceNames []string
	Verbs         map[string]bool
}

func (r rule) String() string {
	verbs := make([]string, 0, len(r.Verbs))
	for v := range r.Verbs {
		verbs = append(verbs, v)
	}
	sort.Strings(verbs)

	group := r.Group
	if group == "" {
		group = "core"
	}
	return fmt.Sprintf("%s/%s %v", group, r.Resource, verbs)
}

func rulesOf(t *testing.T, o Object) []rule {
	t.Helper()

	raw, ok := o["rules"].([]any)
	require.True(t, ok, "%s/%s has no rules", o.Kind(), o.Name())

	var out []rule
	for _, item := range raw {
		m, ok := asMap(item)
		if !ok {
			continue
		}

		verbs := map[string]bool{}
		for _, v := range stringList(m["verbs"]) {
			verbs[v] = true
		}

		groups := stringList(m["apiGroups"])
		if len(groups) == 0 {
			groups = []string{""}
		}
		for _, g := range groups {
			for _, res := range stringList(m["resources"]) {
				out = append(out, rule{
					Group:         g,
					Resource:      res,
					ResourceNames: stringList(m["resourceNames"]),
					Verbs:         verbs,
				})
			}
		}
	}

	return out
}

func stringList(v any) []string {
	raw, _ := v.([]any)
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// subjectsOf returns the ServiceAccount names a binding grants to.
func subjectsOf(o Object) []string {
	raw, _ := o["subjects"].([]any)

	var out []string
	for _, item := range raw {
		s, ok := asMap(item)
		if !ok {
			continue
		}
		if s.str("kind") == "ServiceAccount" {
			out = append(out, s.str("name"))
		}
	}
	return out
}

// TestNoRBACRuleUsesAWildcard.
//
// A wildcard group, resource or verb is what a customer security review stops
// on first, and on OpenShift that review is usually a condition of the install
// rather than a formality.
func TestNoRBACRuleUsesAWildcard(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	for _, kind := range []string{"ClusterRole", "Role"} {
		for _, o := range manifest.Kinds(kind) {
			for _, r := range rulesOf(t, o) {
				assert.NotEqual(t, "*", r.Group, "%s %s uses a wildcard apiGroup", kind, o.Name())
				assert.NotEqual(t, "*", r.Resource, "%s %s uses a wildcard resource", kind, o.Name())
				assert.False(t, r.Verbs["*"], "%s %s uses a wildcard verb on %s", kind, o.Name(), r.Resource)
			}
		}
	}
}

// TestControllerClusterRoleGrantsOnlyWhatTheSidecarsAndDriverNeed.
//
// An exact allow-list, not a subset check, and that is the point: a rule the
// chart grows later fails here and has to be justified in this file before it
// reaches an OpenShift cluster.
func TestControllerClusterRoleGrantsOnlyWhatTheSidecarsAndDriverNeed(t *testing.T) {
	expected := map[string]string{
		// csi-provisioner creates and deletes PVs; DeleteVolume lists them to
		// refuse a delete whose PV does not own the directory.
		"core/persistentvolumes": "create,delete,get,list,patch,watch",
		// csi-provisioner and csi-resizer bind and resize claims; the restore
		// staging sweep lists them.
		"core/persistentvolumeclaims":        "get,list,update,watch",
		"core/persistentvolumeclaims/status": "patch",
		// The driver reads class parameters; inventory discovers clusters.
		"storage.k8s.io/storageclasses": "get,list,watch",
		// Sidecars record provisioning outcomes on the claim.
		"core/events": "create,list,patch,update,watch",
		// Topology, used by the provisioner's node selection.
		"core/nodes":              "get,list,watch",
		"storage.k8s.io/csinodes": "get,list,watch",
		// csi-provisioner v6 / csi-resizer v2 enable VolumeAttributesClass on
		// any cluster serving its v1 API (1.35+) and read the classes through
		// an informer. Read-only; the driver advertises no MODIFY_VOLUME.
		"storage.k8s.io/volumeattributesclasses": "get,list,watch",
		// csi-snapshotter, present because the OpenShift values file enables
		// it. Rights on the objects, never on the definitions -- the CRDs are
		// OpenShift's, installed by cluster-storage-operator.
		// watch and update: csi-provisioner v6.2+'s source-protection
		// finalizer, which keeps a snapshot from being deleted mid-restore.
		"snapshot.storage.k8s.io/volumesnapshots":               "get,list,watch,update",
		"snapshot.storage.k8s.io/volumesnapshotclasses":         "get,list,watch",
		"snapshot.storage.k8s.io/volumesnapshotcontents":        "get,list,watch,update,patch",
		"snapshot.storage.k8s.io/volumesnapshotcontents/status": "update,patch",
	}

	manifest := Render(t, openShiftValues(t))
	role := manifest.Named(t, "ClusterRole", "-controller")

	seen := map[string]bool{}
	for _, r := range rulesOf(t, role) {
		key := r.Group
		if key == "" {
			key = "core"
		}
		key += "/" + r.Resource
		seen[key] = true

		want, known := expected[key]
		if !assert.True(t, known, "the controller ClusterRole grants %s, which this tier has no justification for", r) {
			continue
		}
		for _, verb := range strings.Split(want, ",") {
			assert.True(t, r.Verbs[verb], "%s is missing the %s verb", key, verb)
		}
		assert.Len(t, r.Verbs, len(strings.Split(want, ",")), "%s grants more verbs than %s", key, want)
	}

	for key := range expected {
		assert.True(t, seen[key], "the controller ClusterRole no longer grants %s", key)
	}
}

// TestSCCUseIsGrantedAsNarrowlyAsItCanBe.
//
// The installer grants itself a permission, which is the part of it most worth
// constraining. Four separate narrowings, each of which an obvious
// implementation would miss:
//
//   - `use` and nothing else, on `securitycontextconstraints` and nothing else;
//   - confined by resourceNames to the chart's own SCC, so the grant cannot
//     select `privileged` or any other SCC on the cluster;
//   - delivered by a RoleBinding, so it applies to pods in the release
//     namespace only -- a ClusterRoleBinding would let that ServiceAccount run
//     a privileged pod anywhere;
//   - to the node ServiceAccount only, never the controller's.
func TestSCCUseIsGrantedAsNarrowlyAsItCanBe(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	scc := SCCFromManifest(t, manifest)
	node := PodTemplate(t, manifest.Named(t, "DaemonSet", "-node"))
	controller := PodTemplate(t, manifest.Named(t, "Deployment", "-controller"))

	var grantingRoles []string
	for _, role := range manifest.Kinds("ClusterRole") {
		rules := rulesOf(t, role)

		var grantsSCC bool
		for _, r := range rules {
			if r.Group != "security.openshift.io" {
				continue
			}
			grantsSCC = true

			assert.Equal(t, "securitycontextconstraints", r.Resource,
				"%s grants %s in the SCC API group", role.Name(), r.Resource)
			assert.Equal(t, []string{scc.Name}, r.ResourceNames,
				"%s is not confined to the chart's own SCC, so it can select any SCC on the cluster", role.Name())
			assert.Len(t, r.Verbs, 1, "%s grants more than `use` on the SCC: %s", role.Name(), r)
			assert.True(t, r.Verbs["use"], "%s does not grant `use`", role.Name())
		}

		if grantsSCC {
			grantingRoles = append(grantingRoles, role.Name())
			assert.Len(t, rules, 1,
				"%s mixes the SCC grant with other rights; it should carry nothing else", role.Name())
		}
	}

	require.Len(t, grantingRoles, 1, "expected exactly one ClusterRole to grant SCC use, found %v", grantingRoles)

	for _, b := range manifest.Kinds("ClusterRoleBinding") {
		ref, _ := asMap(b["roleRef"])
		assert.NotContains(t, grantingRoles, ref.str("name"),
			"the SCC grant is bound cluster-wide by %s; that ServiceAccount could then run a privileged pod in any namespace", b.Name())
	}

	var bound []string
	for _, b := range manifest.Kinds("RoleBinding") {
		ref, _ := asMap(b["roleRef"])
		if contains(grantingRoles, ref.str("name")) {
			bound = append(bound, subjectsOf(b)...)
		}
	}

	assert.Equal(t, []string{node.ServiceAccount}, bound,
		"the SCC grant goes to %v; only the node ServiceAccount needs it and the controller must never have it", bound)
	assert.NotContains(t, bound, controller.ServiceAccount,
		"the controller can select the node's privileged SCC")
}

// TestTheSCCAllowsNothingThePodDoesNotUse.
//
// The grant above is only as narrow as the SCC it names, so the SCC is the
// other half of the same question. Each allowance is checked against the pod
// that uses it rather than against a list written down beside it -- a volume
// type the pod stopped mounting should stop being permitted.
func TestTheSCCAllowsNothingThePodDoesNotUse(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	scc := SCCFromManifest(t, manifest)
	pod := PodTemplate(t, manifest.Named(t, "DaemonSet", "-node"))

	// pod.VolumeTypes already holds the injected token volume when the pod
	// mounts one, so an SCC that permits `projected` for a pod that mounts no
	// token fails here (I-4).
	used := map[string]bool{}
	for _, v := range pod.VolumeTypes {
		used[v] = true
	}
	for _, v := range scc.Volumes {
		assert.NotEqual(t, "*", v, "the SCC permits every volume type")
		assert.True(t, used[v], "the SCC permits the %s volume type, which the node pod does not mount", v)
	}

	assert.False(t, scc.AllowHostPID, "the SCC permits hostPID, which the node pod does not use")
	assert.False(t, scc.AllowHostIPC, "the SCC permits hostIPC, which the node pod does not use")

	assert.True(t, pod.IsPrivileged(), "the node pod is not privileged, so the SCC need not allow it")
	assert.True(t, pod.HostNetwork, "the node pod does not use hostNetwork, so the SCC need not allow it")
}

// TestNodePluginGetsNoRightsBeyondItsSCC.
//
// The node plugin mounts; it never calls the Qumulo REST API and never reads a
// Secret. It runs privileged on every worker, so any Kubernetes right on its
// ServiceAccount is a right an attacker gets from one compromised node pod.
// The SCC `use` grant is the only one it may hold.
func TestNodePluginGetsNoRightsBeyondItsSCC(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	node := PodTemplate(t, manifest.Named(t, "DaemonSet", "-node"))
	require.NotEmpty(t, node.ServiceAccount, "the node DaemonSet runs as the namespace default ServiceAccount")

	for _, kind := range []string{"ClusterRoleBinding", "RoleBinding"} {
		for _, b := range manifest.Kinds(kind) {
			if !contains(subjectsOf(b), node.ServiceAccount) {
				continue
			}

			ref, _ := asMap(b["roleRef"])
			role := findRole(manifest, ref.str("kind"), ref.str("name"))
			require.NotNil(t, role, "%s %s binds a role this install does not define", kind, b.Name())

			for _, r := range rulesOf(t, role) {
				assert.Equal(t, "security.openshift.io", r.Group,
					"the node ServiceAccount is granted %s through %s; it needs no Kubernetes rights but its own SCC", r, b.Name())
			}
		}
	}
}

// TestNodePodMountsNoServiceAccountToken (I-4).
//
// Holding no rights is half of it; holding no credential is the other. The
// node service builds no Kubernetes client, so a token mounted into a
// privileged pod on every worker is an API credential nothing uses. Off on
// plain Kubernetes too, not only on OpenShift.
func TestNodePodMountsNoServiceAccountToken(t *testing.T) {
	plain := openShiftValues(t)
	plain.ValuesFiles = nil
	plain.PlainKubernetes = true

	for name, manifest := range map[string]Manifest{
		"openshift":  Render(t, openShiftValues(t)),
		"kubernetes": Render(t, plain),
	} {
		node := PodTemplate(t, manifest.Named(t, "DaemonSet", "-node"))
		assert.False(t, node.MountsToken, "%s: the node pod mounts a ServiceAccount token it never uses", name)
		assert.NotContains(t, node.VolumeTypes, "projected", "%s: the node pod mounts a projected volume", name)
	}
}

// TestControllerCannotDeletePodsOrGrantItselfPrivilege.
//
// Two review questions, one absence. qumulo-csi-repoint restarts workload pods
// during a failover, so it needs `delete` on pods across namespaces -- that is
// an admin-run tool with its own narrow role, and it must not ride along here.
// The rest is privilege escalation through RBAC.
func TestControllerCannotDeletePodsOrGrantItselfPrivilege(t *testing.T) {
	manifest := Render(t, openShiftValues(t))
	controller := PodTemplate(t, manifest.Named(t, "Deployment", "-controller"))

	forbidden := []struct{ group, resource, verb, why string }{
		{"", "pods", "delete", "that is qumulo-csi-repoint's right, not the controller's"},
		{"", "pods", "create", "the controller runs no pods of its own"},
		{"rbac.authorization.k8s.io", "clusterroles", "create", "RBAC create is a privilege-escalation path"},
		{"rbac.authorization.k8s.io", "clusterrolebindings", "create", "RBAC create is a privilege-escalation path"},
		{"", "serviceaccounts/token", "create", "token minting lets the controller act as any identity"},
		{"", "secrets", "list", "the driver resolves the one Secret its request names; listing them all is a different blast radius"},
		{"security.openshift.io", "securitycontextconstraints", "create", "a workload that can create an SCC can grant itself anything"},
	}

	for _, kind := range []string{"ClusterRole", "Role"} {
		for _, o := range manifest.Kinds(kind) {
			// Only what the controller can actually reach.
			if !boundTo(manifest, o, controller.ServiceAccount) {
				continue
			}
			for _, r := range rulesOf(t, o) {
				for _, f := range forbidden {
					if r.Group == f.group && r.Resource == f.resource && r.Verbs[f.verb] {
						t.Errorf("%s %s grants %s on %s/%s -- %s",
							kind, o.Name(), f.verb, f.group, f.resource, f.why)
					}
				}
			}
		}
	}
}

// TestSecretAccessIsNamespacedUnlessTheOperatorAsksOtherwise.
//
// The chart can grant cluster-wide Secret reads, for classes whose Secrets
// live in other namespaces. On OpenShift that is the grant most likely to be
// refused, so it stays opt-in and stays visible when taken.
func TestSecretAccessIsNamespacedUnlessTheOperatorAsksOtherwise(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		for _, o := range Render(t, openShiftValues(t)).Kinds("ClusterRole") {
			for _, r := range rulesOf(t, o) {
				assert.NotEqual(t, "secrets", r.Resource,
					"ClusterRole %s reads Secrets cluster-wide by default", o.Name())
			}
		}
	})

	t.Run("opt-in", func(t *testing.T) {
		opts := openShiftValues(t)
		opts.Sets = []string{"rbac.clusterWideSecrets=true"}

		var found bool
		for _, o := range Render(t, opts).Kinds("ClusterRole") {
			for _, r := range rulesOf(t, o) {
				if r.Resource != "secrets" {
					continue
				}
				found = true
				assert.False(t, r.Verbs["list"],
					"the cluster-wide Secret grant includes list; get is what resolving a named Secret needs")
				assert.False(t, r.Verbs["delete"], "the cluster-wide Secret grant includes delete")
			}
		}
		assert.True(t, found, "rbac.clusterWideSecrets=true granted nothing, so the option does not work")
	})
}

// TestLeaderElectionRightsAreNamespacedNotClusterWide.
//
// A Lease grant is unremarkable in a namespace. Cluster-wide it reaches
// kube-system's, which on OpenShift are the control plane's own.
func TestLeaderElectionRightsAreNamespacedNotClusterWide(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	for _, o := range manifest.Kinds("ClusterRole") {
		for _, r := range rulesOf(t, o) {
			assert.NotEqual(t, "leases", r.Resource,
				"ClusterRole %s grants Leases cluster-wide; leader election is namespaced", o.Name())
		}
	}

	var found bool
	for _, r := range rulesOf(t, manifest.Named(t, "Role", "-controller")) {
		if r.Resource == "leases" {
			found = true
		}
	}
	assert.True(t, found, "no namespaced Lease grant, so leader election cannot work")
}

func findRole(m Manifest, kind, name string) Object {
	for _, o := range m.Kinds(kind) {
		if o.Name() == name {
			return o
		}
	}
	return nil
}

// boundTo reports whether a ServiceAccount can reach the rules in a role.
func boundTo(m Manifest, role Object, account string) bool {
	for _, kind := range []string{"ClusterRoleBinding", "RoleBinding"} {
		for _, b := range m.Kinds(kind) {
			ref, _ := asMap(b["roleRef"])
			if ref.str("kind") == role.Kind() && ref.str("name") == role.Name() &&
				contains(subjectsOf(b), account) {
				return true
			}
		}
	}
	return false
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
