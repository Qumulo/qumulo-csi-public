//go:build chart && defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

// Render-only defects in the chart's workloads, RBAC and values validation.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dfxController(t *testing.T, objs []dfxObj) dfxObj {
	t.Helper()
	return dfxOne(t, objs, "Deployment", "-controller")
}

func dfxNode(t *testing.T, objs []dfxObj) dfxObj {
	t.Helper()
	return dfxOne(t, objs, "DaemonSet", "-node")
}

// TestDefect_NodePluginHasAPriorityClassAndRequests: A default install must
// give the node plugin a priority class and
// resource requests, so it is not BestEffort and first in line for eviction;
// node.priorityClassName must be settable.
func TestDefect_NodePluginHasAPriorityClassAndRequests(t *testing.T) {
	node := dfxNode(t, dfxRender(t, "", dfxBaseClass...))
	assert.NotEmpty(t, dfxStr(dfxGet(node, "spec", "template", "spec", "priorityClassName")),
		"a default install gives the node pods no priority class")
	for name, c := range dfxContainers(node) {
		assert.True(t, dfxHasRequests(c), "node container %s has no resource requests (BestEffort)", name)
	}

	node = dfxNode(t, dfxRender(t, "", dfxSets(dfxBaseClass, []string{"node.priorityClassName=system-node-critical"})...))
	assert.Equal(t, "system-node-critical", dfxStr(dfxGet(node, "spec", "template", "spec", "priorityClassName")))
}

// TestDefect_ControllerCannotListPodsClusterWide: No ClusterRole the chart
// renders may grant list or watch on pods, and
// the csi-addons sidecar's reads (csiaddonsnodes, replicasets, deployments)
// must be namespaced. (templates/rbac.yaml.)
func TestDefect_ControllerCannotListPodsClusterWide(t *testing.T) {
	objs := dfxRender(t, "", dfxSets(dfxBaseClass, []string{
		"sidecars.csiAddons.enabled=true", "sidecars.snapshotter.enabled=true", "sidecars.capacity.enabled=true",
	})...)
	for _, cr := range dfxFind(objs, "ClusterRole") {
		for _, r := range dfxList(cr["rules"]) {
			rule := dfxMap(r)
			for _, res := range dfxList(rule["resources"]) {
				resource := dfxStr(res)
				verbs := dfxStrings(rule["verbs"])
				if resource == "pods" {
					assert.NotContains(t, verbs, "list", "ClusterRole %s lists pods cluster-wide", cr.name())
					assert.NotContains(t, verbs, "watch", "ClusterRole %s watches pods cluster-wide", cr.name())
				}
				if resource == "csiaddonsnodes" || resource == "replicasets" || resource == "deployments" {
					t.Errorf("ClusterRole %s grants %v on %s cluster-wide; the sidecar reads only its own namespace",
						cr.name(), verbs, resource)
				}
			}
		}
	}
}

func dfxStrings(v any) []string {
	var out []string
	for _, x := range dfxList(v) {
		out = append(out, dfxStr(x))
	}
	return out
}

// TestDefect_OpenDedicatedExportClassIsRefused covers the chart part.
//
// A dedicated-export class that sets no allowedClients gives every volume
// an export any NFS client can mount as root. The chart must refuse such a
// class (naming allowedClients), or default allowedClients for it.
// (values.yaml class documentation.)
func TestDefect_OpenDedicatedExportClassIsRefused(t *testing.T) {
	sets := dfxSets(dfxBaseClass, []string{"storageClasses[0].provisioningModel=dedicated-export"})
	out, err := dfxTryRender(t, "", sets...)
	if err != nil {
		assert.Contains(t, out, "allowedClients", "refused, but not for the open export:\n%s", dfxTail(out))
		return
	}
	sc := dfxOne(t, dfxParse(t, out), "StorageClass", "qumulo-nfs")
	assert.NotEmpty(t, dfxStr(dfxGet(sc, "parameters", "allowedClients")),
		"a dedicated-export class with no allowedClients rendered with no client restriction: its "+
			"exports admit any NFS client, as root, from any port")

	// The restricted class must still render: a refusal of every
	// dedicated-export class would pass the check above vacuously.
	_ = dfxRender(t, "", dfxSets(sets, []string{"storageClasses[0].allowedClients[0]=10.0.0.0/24"})...)
}

// TestDefect_ControllerIsNotBestEffort: controller.resources must reach the
// qumulo-csi-driver container, and a
// default install must give the controller pod requests on that container
// and a priority class, and the node pods requests, so no driver pod is
// BestEffort. (templates/controller.yaml:272.)
func TestDefect_ControllerIsNotBestEffort(t *testing.T) {
	t.Run("controller.resources reaches the driver container", func(t *testing.T) {
		ctrl := dfxController(t, dfxRender(t, "", dfxSets(dfxBaseClass, []string{"controller.resources.requests.cpu=123m"})...))
		driver := dfxContainers(ctrl)["qumulo-csi-driver"]
		require.NotNil(t, driver, "no qumulo-csi-driver container in the controller")
		assert.Equal(t, "123m", dfxStr(dfxGet(driver, "resources", "requests", "cpu")),
			"controller.resources.requests.cpu did not reach the driver container")
	})
	t.Run("default controller has requests and a priority class", func(t *testing.T) {
		ctrl := dfxController(t, dfxRender(t, "", dfxBaseClass...))
		driver := dfxContainers(ctrl)["qumulo-csi-driver"]
		require.NotNil(t, driver)
		assert.True(t, dfxHasRequests(driver), "the controller's driver container has no requests by default")
		assert.NotEmpty(t, dfxStr(dfxGet(ctrl, "spec", "template", "spec", "priorityClassName")),
			"the controller pod has no priority class by default")
	})
	t.Run("default node pods have requests", func(t *testing.T) {
		node := dfxNode(t, dfxRender(t, "", dfxBaseClass...))
		for name, c := range dfxContainers(node) {
			assert.True(t, dfxHasRequests(c), "node container %s has no requests", name)
		}
	})
}

// TestDefect_ControllerSurvivesADrain: The chart must expose a
// PodDisruptionBudget, topologySpreadConstraints,
// the Deployment strategy and the sidecars' lease timings, and by default run
// more than one controller replica spread over nodes, so one drain does not
// stop every storage operation. (templates/controller.yaml:11, values.schema.json.)
func TestDefect_ControllerSurvivesADrain(t *testing.T) {
	t.Run("PDB with more than one replica", func(t *testing.T) {
		objs := dfxRender(t, "", dfxSets(dfxBaseClass, []string{"controller.replicaCount=2"})...)
		ctrl := dfxController(t, objs)
		pdbs := dfxFind(objs, "PodDisruptionBudget")
		require.NotEmpty(t, pdbs, "two controller replicas render no PodDisruptionBudget")
		podLabels := dfxMap(dfxGet(ctrl, "spec", "template", "metadata", "labels"))
		covered := false
		for _, pdb := range pdbs {
			sel := dfxMap(dfxGet(pdb, "spec", "selector", "matchLabels"))
			match := len(sel) > 0
			for k, v := range sel {
				match = match && dfxStr(podLabels[k]) == dfxStr(v)
			}
			covered = covered || match
		}
		assert.True(t, covered, "no PodDisruptionBudget selects the controller pods")
	})
	t.Run("topologySpreadConstraints settable", func(t *testing.T) {
		out, err := dfxTryRender(t, "", dfxSets(dfxBaseClass, []string{
			"controller.topologySpreadConstraints[0].maxSkew=1",
			"controller.topologySpreadConstraints[0].topologyKey=topology.kubernetes.io/zone",
			"controller.topologySpreadConstraints[0].whenUnsatisfiable=ScheduleAnyway",
		})...)
		require.NoError(t, err, "controller.topologySpreadConstraints is refused:\n%s", dfxTail(out))
		ctrl := dfxController(t, dfxParse(t, out))
		assert.Equal(t, "topology.kubernetes.io/zone",
			dfxStr(dfxGet(ctrl, "spec", "template", "spec", "topologySpreadConstraints", 0, "topologyKey")))
	})
	t.Run("strategy settable", func(t *testing.T) {
		out, err := dfxTryRender(t, "", dfxSets(dfxBaseClass, []string{
			"controller.strategy.type=RollingUpdate",
			"controller.strategy.rollingUpdate.maxSurge=0",
			"controller.strategy.rollingUpdate.maxUnavailable=1",
		})...)
		require.NoError(t, err, "controller.strategy is refused:\n%s", dfxTail(out))
		ctrl := dfxController(t, dfxParse(t, out))
		assert.Equal(t, "0", dfxStr(dfxGet(ctrl, "spec", "strategy", "rollingUpdate", "maxSurge")))
	})
	t.Run("lease timings settable", func(t *testing.T) {
		const flag = "--leader-election-lease-duration=30s"
		tries := [][]string{
			{"controller.leaderElectionTimings.leaseDuration=30s"},
			{"controller.leaseDuration=30s"},
			{"sidecars.provisioner.extraArgs[0]=" + flag},
		}
		for _, extra := range tries {
			out, err := dfxTryRender(t, "", dfxSets(dfxBaseClass, extra)...)
			if err != nil {
				continue
			}
			prov := dfxContainers(dfxController(t, dfxParse(t, out)))["csi-provisioner"]
			if prov != nil && strings.Contains(strings.Join(dfxStrings(prov["args"]), " "), flag) {
				return
			}
		}
		t.Errorf("no setting puts %s on csi-provisioner (tried %v)", flag, tries)
	})
	t.Run("default spreads more than one replica", func(t *testing.T) {
		ctrl := dfxController(t, dfxRender(t, "", dfxBaseClass...))
		replicas := dfxStr(dfxGet(ctrl, "spec", "replicas"))
		assert.NotEqual(t, "1", replicas, "the default is one controller replica: a drain stops every storage operation")
		spec := dfxMap(dfxGet(ctrl, "spec", "template", "spec"))
		spread := dfxGet(spec, "affinity", "podAntiAffinity") != nil || len(dfxList(spec["topologySpreadConstraints"])) > 0
		assert.True(t, spread, "the default puts nothing between two replicas and one node")
	})
}

// TestDefect_LeaderElectionOffIsSafeOrRefused: With controller.leaderElection
// false: the csi-addons sidecar (which
// elects regardless) must still get Lease RBAC; more than one replica must be
// refused; and the Deployment must roll with Recreate so two un-elected
// controllers never run at once. (templates/rbac.yaml:195-208, _validate.tpl.)
func TestDefect_LeaderElectionOffIsSafeOrRefused(t *testing.T) {
	off := dfxSets(dfxBaseClass, []string{"controller.leaderElection=false", "controller.replicaCount=1"})
	t.Run("csi-addons keeps Lease rights", func(t *testing.T) {
		objs := dfxRender(t, "", dfxSets(off, []string{"sidecars.csiAddons.enabled=true"})...)
		found := false
		for _, kind := range []string{"Role", "ClusterRole"} {
			for _, role := range dfxFind(objs, kind) {
				for _, r := range dfxList(role["rules"]) {
					rule := dfxMap(r)
					if dfxContains(dfxStrings(rule["apiGroups"]), "coordination.k8s.io") &&
						dfxContains(dfxStrings(rule["resources"]), "leases") {
						found = true
					}
				}
			}
		}
		assert.True(t, found, "csi-addons on with leaderElection false renders no Lease rule; the sidecar's "+
			"election gets 403 for ever and no VolumeReplication is served")
	})
	t.Run("more than one replica refused", func(t *testing.T) {
		out, err := dfxTryRender(t, "", dfxSets(dfxBaseClass, []string{"controller.leaderElection=false", "controller.replicaCount=2"})...)
		assert.Error(t, err, "two replicas without leader election rendered: both provision and resize the same claims")
		if err != nil {
			assert.NotContains(t, out, "additional properties", "refused for an unrelated reason:\n%s", dfxTail(out))
		}
	})
	t.Run("rollout does not overlap", func(t *testing.T) {
		ctrl := dfxController(t, dfxRender(t, "", off...))
		assert.Equal(t, "Recreate", dfxStr(dfxGet(ctrl, "spec", "strategy", "type")),
			"leaderElection false keeps the default RollingUpdate surge: two un-elected controllers on every rollout")
	})
}

func dfxContains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// TestDefect_SchemaRefusesSettingsThatBreakStorage covers the chart part.
//
// Settings that render and then break or weaken storage must be refused
// at render: a non-duration or zero apiTimeout, a zero sidecar timeout, "#" in
// basePath, and a metrics port equal to a livenessprobe port.
// (values.schema.json:147, _validate.tpl.)
// The report's `mountOptions: [vers=3]` without `protocol` item is the node
// plugin's nolock decision (node_server.go), not a chart render, and is not
// covered here.
func TestDefect_SchemaRefusesSettingsThatBreakStorage(t *testing.T) {
	cases := map[string][]string{
		"apiTimeout not a duration":      {"qumulo.apiTimeout=ten"},
		"apiTimeout zero":                {"qumulo.apiTimeout=0s"},
		"sidecar timeout zero":           {"sidecars.provisioner.timeout=0s"},
		"hash in basePath":               {"storageClasses[0].basePath=/csi/vol#1"},
		"metrics port on liveness port":  {"metrics.enabled=true", "metrics.port.controller=29652"},
		"node metrics on liveness port":  {"metrics.enabled=true", "metrics.port.node=29653"},
		"two metrics ports share number": {"metrics.enabled=true", "metrics.port.provisioner=29700", "metrics.port.resizer=29700"},
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := dfxTryRender(t, "", dfxSets(dfxBaseClass, extra)...)
			assert.Error(t, err, "%v rendered (exit 0) and fails only later, at start-up, mount or provisioning", extra)
		})
	}
	// Control: the valid forms of the same settings still render.
	_ = dfxRender(t, "", dfxSets(dfxBaseClass, []string{
		"qumulo.apiTimeout=30s", "sidecars.provisioner.timeout=60s", "metrics.enabled=true",
	})...)
}

// TestDefect_ControllerReadsOnlyItsOwnSecrets: By default the controller must
// not be able to get every Secret in the
// cluster: any Secret rule must be limited by resourceNames, and every Secret
// a class names must still be readable, in its own namespace. Events need no
// `get`. (templates/rbac.yaml:49-51, :75.)
func TestDefect_ControllerReadsOnlyItsOwnSecrets(t *testing.T) {
	objs := dfxRender(t, "", dfxSets(dfxBaseClass, []string{
		"storageClasses[1].name=qumulo-other",
		"storageClasses[1].server=other.example.com",
		"storageClasses[1].secretName=other-token",
		"storageClasses[1].secretNamespace=storage-secrets",
		"storageClasses[1].basePath=/csi/other",
	})...)

	type grant struct{ ns, name string }
	readable := map[grant]bool{}
	for _, kind := range []string{"ClusterRole", "Role"} {
		for _, role := range dfxFind(objs, kind) {
			for _, r := range dfxList(role["rules"]) {
				rule := dfxMap(r)
				if !dfxContains(dfxStrings(rule["resources"]), "secrets") || !dfxContains(dfxStrings(rule["verbs"]), "get") {
					continue
				}
				names := dfxStrings(rule["resourceNames"])
				if kind == "ClusterRole" && len(names) == 0 {
					t.Errorf("ClusterRole %s grants get on every Secret in the cluster", role.name())
					readable[grant{"*", "*"}] = true
				}
				for _, n := range names {
					readable[grant{role.namespace(), n}] = true
					if kind == "ClusterRole" {
						readable[grant{"*", n}] = true
					}
				}
			}
			if kind == "ClusterRole" {
				for _, r := range dfxList(role["rules"]) {
					rule := dfxMap(r)
					if dfxContains(dfxStrings(rule["resources"]), "events") {
						assert.NotContains(t, dfxStrings(rule["verbs"]), "get",
							"ClusterRole %s keeps events get, which nothing uses", role.name())
					}
				}
			}
		}
	}
	// Each Secret a class names must still be readable (a fix that drops the
	// rule outright would pass the check above and break every provision).
	for _, want := range []grant{{dfxNamespace, "qumulo-token"}, {"storage-secrets", "other-token"}} {
		assert.True(t, readable[want] || readable[grant{"*", want.name}] || readable[grant{"*", "*"}],
			"the controller can no longer read Secret %s/%s, which a class names", want.ns, want.name)
	}
}

// TestDefect_MetricsAreNotOpenToEveryPod: With metrics on, the node plugin's
// listener must not bind every host
// interface, and the chart must offer a NetworkPolicy limiting who can read
// the controller's per-volume series. (templates/node.yaml:110.)
func TestDefect_MetricsAreNotOpenToEveryPod(t *testing.T) {
	sets := dfxSets(dfxBaseClass, []string{"metrics.enabled=true"})
	t.Run("node listener not on every interface", func(t *testing.T) {
		node := dfxNode(t, dfxRender(t, "", sets...))
		args := dfxStrings(dfxContainers(node)["qumulo-csi-driver"]["args"])
		var addr string
		for _, a := range args {
			if strings.HasPrefix(a, "-metrics-address=") || strings.HasPrefix(a, "--metrics-address=") {
				addr = a[strings.Index(a, "=")+1:]
			}
		}
		require.NotEmpty(t, addr, "no -metrics-address on the node driver with metrics on: %v", args)
		assert.False(t, strings.HasPrefix(addr, ":") || strings.HasPrefix(addr, "0.0.0.0:") || strings.HasPrefix(addr, "[::]:"),
			"the node metrics listener binds every interface of the host network (%s)", addr)
	})
	t.Run("a NetworkPolicy is available", func(t *testing.T) {
		path := dfxSchemaKeyPath(t, regexp.MustCompile(`(?i)^networkpolic(y|ies)$`), "metrics")
		require.NotEmpty(t, path, "the values schema offers no NetworkPolicy setting under metrics")
		objs := dfxRender(t, "", dfxSets(sets, []string{path + ".enabled=true"})...)
		ctrlLabels := dfxMap(dfxGet(dfxController(t, objs), "spec", "template", "metadata", "labels"))
		ok := false
		for _, np := range dfxFind(objs, "NetworkPolicy") {
			sel := dfxMap(dfxGet(np, "spec", "podSelector", "matchLabels"))
			match := len(sel) > 0
			for k, v := range sel {
				match = match && dfxStr(ctrlLabels[k]) == dfxStr(v)
			}
			ok = ok || match
		}
		assert.True(t, ok, "%s.enabled=true renders no NetworkPolicy selecting the controller pods", path)
	})
}

// dfxSchemaKeyPath finds the dotted values path of the first schema property,
// under the top-level key `under`, whose name matches re.
func dfxSchemaKeyPath(t *testing.T, re *regexp.Regexp, under string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(chartPath, "values.schema.json"))
	require.NoError(t, err)
	var schema map[string]any
	require.NoError(t, json.Unmarshal(raw, &schema))
	var walk func(node map[string]any, prefix string) string
	walk = func(node map[string]any, prefix string) string {
		props := dfxMap(node["properties"])
		for name, sub := range props {
			p := name
			if prefix != "" {
				p = prefix + "." + name
			}
			if re.MatchString(name) {
				return p
			}
			if found := walk(dfxMap(sub), p); found != "" {
				return found
			}
		}
		return ""
	}
	return walk(dfxMap(dfxGet(schema, "properties", under)), under)
}

// TestDefect_NodeRolloutIsGatedOnThePlugin: The node DaemonSet must not count a
// pod available before the kubelet
// has registered the plugin: a readiness probe that proves registration, a
// minReadySeconds above zero, and a settable update strategy. The registrar's
// /healthz does not prove it -- it checks only the registrar's own socket,
// and stayed green on every node with a wrong kubeletDir -- so readiness
// must not rest on it alone. The registrar may still carry its own /healthz
// readiness probe (every container has one; TestEveryContainerHasLivenessAndReadiness):
// a pod is Ready only when every container is, so that probe cannot make the
// pod Ready by itself. What gates the rollout is the driver container's
// probe, which must check the registration marker. (templates/node.yaml.)
func TestDefect_NodeRolloutIsGatedOnThePlugin(t *testing.T) {
	node := dfxNode(t, dfxRender(t, "", dfxBaseClass...))
	containers := dfxContainers(node)
	if probe, ok := containers["node-driver-registrar"]["readinessProbe"].(map[string]any); ok {
		assert.Nil(t, probe["exec"],
			"the registrar's readiness is its own /healthz; the registration check belongs to the driver container")
	}
	drv := containers["qumulo-csi-driver"]
	cmd := strings.Join(dfxStrings(dfxGet(drv, "readinessProbe", "exec", "command")), " ")
	assert.Contains(t, cmd, "-check-registration=",
		"the driver's readiness does not check that the kubelet registered the plugin")
	marker := strings.TrimPrefix(cmd[strings.Index(cmd, "-check-registration="):], "-check-registration=")
	assert.Contains(t, dfxStrings(drv["args"]), "-registration-marker="+marker,
		"the readiness probe checks a marker the driver never writes")
	minReady := dfxStr(dfxGet(node, "spec", "minReadySeconds"))
	assert.False(t, minReady == "" || minReady == "0", "node DaemonSet has no minReadySeconds")

	out, err := dfxTryRender(t, "", dfxSets(dfxBaseClass, []string{
		"node.updateStrategy.type=RollingUpdate", "node.updateStrategy.rollingUpdate.maxUnavailable=2",
	})...)
	require.NoError(t, err, "node.updateStrategy is refused:\n%s", dfxTail(out))
	node = dfxNode(t, dfxParse(t, out))
	assert.Equal(t, "2", dfxStr(dfxGet(node, "spec", "updateStrategy", "rollingUpdate", "maxUnavailable")))
}

// TestDefect_KerberosDoesNotStripNodesOfThePlugin: With Kerberos on, a node
// without /etc/krb5.keytab or /etc/krb5.conf
// must still get a node plugin: the two hostPath volumes must not be
// `type: File` (which the kubelet refuses when the file is missing), unless
// the chart confines the DaemonSet to Kerberos nodes by default.
// (templates/node.yaml:183-190.)
func TestDefect_KerberosDoesNotStripNodesOfThePlugin(t *testing.T) {
	node := dfxNode(t, dfxRender(t, "", dfxSets(dfxBaseClass, []string{"node.kerberos.enabled=true"})...))
	spec := dfxMap(dfxGet(node, "spec", "template", "spec"))
	confined := len(dfxMap(spec["nodeSelector"])) > 0 || dfxGet(spec, "affinity", "nodeAffinity") != nil
	var strict []string
	for _, v := range dfxList(spec["volumes"]) {
		vol := dfxMap(v)
		p := dfxStr(dfxGet(vol, "hostPath", "path"))
		if strings.Contains(p, "krb5") && dfxStr(dfxGet(vol, "hostPath", "type")) == "File" {
			strict = append(strict, p)
		}
	}
	assert.True(t, len(strict) == 0 || confined,
		"Kerberos on: %v are hostPath type File on a DaemonSet that runs on every node, so a node "+
			"missing either file gets no node plugin and no volume of any class mounts there", strict)
}

// TestDefect_NodePriorityClassCanPreemptOrIsDocumented: A node plugin pod with
// requests and preemptionPolicy Never can stay
// Pending on a packed node, leaving it without a plugin. Either the default
// preempts, or the policy is settable and the chart README says what Never
// costs. (priorityclass.yaml:14.)
func TestDefect_NodePriorityClassCanPreemptOrIsDocumented(t *testing.T) {
	pcs := dfxFind(dfxRender(t, "", dfxBaseClass...), "PriorityClass")
	require.Len(t, pcs, 1)
	if dfxStr(pcs[0]["preemptionPolicy"]) != "Never" {
		return
	}
	out, err := dfxTryRender(t, "", dfxSets(dfxBaseClass, []string{"node.priorityClass.preemptionPolicy=PreemptLowerPriority"})...)
	require.NoError(t, err, "the default is preemptionPolicy Never and it cannot be changed:\n%s", dfxTail(out))
	pcs = dfxFind(dfxParse(t, out), "PriorityClass")
	require.Len(t, pcs, 1)
	assert.Equal(t, "PreemptLowerPriority", dfxStr(pcs[0]["preemptionPolicy"]))

	readme := dfxReadme(t)
	assert.True(t, dfxNear(readme, "preemptionPolicy", "Pending", 600),
		"the chart README does not say that preemptionPolicy Never can leave a node plugin Pending")
}

// TestDefect_PriorityClassRightsAreDocumented: The PriorityClass the chart
// creates makes install and upgrade need
// rights on priorityclasses; the README must say so, for upgrades too, and
// name node.priorityClass.create false as the way round it -- which must
// render no PriorityClass.
func TestDefect_PriorityClassRightsAreDocumented(t *testing.T) {
	objs := dfxRender(t, "", dfxSets(dfxBaseClass, []string{"node.priorityClass.create=false"})...)
	assert.Empty(t, dfxFind(objs, "PriorityClass"), "node.priorityClass.create=false still renders a PriorityClass")

	readme := dfxReadme(t)
	assert.True(t, dfxNear(readme, "priorityclasses", "upgrad", 400),
		"the chart README does not say an upgrade needs rights on priorityclasses")
	assert.Regexp(t, `priorityClass\.create(: |=)false`, readme,
		"the chart README does not name node.priorityClass.create false as the way to install without them")
}

func dfxReadme(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(chartPath, "README.md"))
	require.NoError(t, err)
	return string(b)
}

// dfxNear reports whether a and b both occur within window bytes of each
// other somewhere in text.
func dfxNear(text, a, b string, window int) bool {
	for i := 0; ; {
		j := strings.Index(text[i:], a)
		if j < 0 {
			return false
		}
		at := i + j
		lo, hi := at-window, at+len(a)+window
		if lo < 0 {
			lo = 0
		}
		if hi > len(text) {
			hi = len(text)
		}
		if strings.Contains(text[lo:hi], b) {
			return true
		}
		i = at + len(a)
	}
}

// TestDefect_SnapshotNamePrefixReachesTheDriverAndTheSidecar: the driver takes
// a snapshot for its own by csi-snapshotter's name prefix,
// so the chart must give both the same one, refuse a prefix set through the
// sidecar's extraArgs alone, and render allowForeignSnapshotRestore only for
// a class that asks for it.
func TestDefect_SnapshotNamePrefixReachesTheDriverAndTheSidecar(t *testing.T) {
	on := dfxSets(dfxBaseClass, []string{"sidecars.snapshotter.enabled=true"})
	args := func(objs []dfxObj, container string) []string {
		return dfxStrings(dfxContainers(dfxController(t, objs))[container]["args"])
	}

	objs := dfxRender(t, "", on...)
	assert.Contains(t, args(objs, "csi-snapshotter"), "--snapshot-name-prefix=snapshot")
	assert.Contains(t, args(objs, "qumulo-csi-driver"), "-snapshot-name-prefix=snapshot")
	sc := dfxOne(t, objs, "StorageClass", "qumulo-nfs")
	assert.Nil(t, dfxGet(sc, "parameters", "allowForeignSnapshotRestore"), "rendered for a class that did not ask")

	objs = dfxRender(t, "", dfxSets(on, []string{"sidecars.snapshotter.snapshotNamePrefix=backup",
		"storageClasses[0].allowForeignSnapshotRestore=true"})...)
	assert.Contains(t, args(objs, "csi-snapshotter"), "--snapshot-name-prefix=backup")
	assert.Contains(t, args(objs, "qumulo-csi-driver"), "-snapshot-name-prefix=backup")
	sc = dfxOne(t, objs, "StorageClass", "qumulo-nfs")
	assert.Equal(t, "true", dfxStr(dfxGet(sc, "parameters", "allowForeignSnapshotRestore")))

	out, err := dfxTryRender(t, "", dfxSets(on, []string{"sidecars.snapshotter.extraArgs[0]=--snapshot-name-prefix=backup"})...)
	require.Error(t, err, "a prefix in extraArgs reaches the sidecar alone, and the driver refuses every snapshot it names")
	assert.Contains(t, out, "snapshotNamePrefix", "refused, but not for the prefix:\n%s", dfxTail(out))

	_, err = dfxTryRender(t, "", dfxSets(on, []string{"sidecars.snapshotter.snapshotNamePrefix=a/b"})...)
	assert.Error(t, err, "a prefix with a slash rendered")
}
