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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// The Gatekeeper library on an earlier OpenShift render failed
// requiredprobes (sidecars had no probes) and containerlimits,
// containerresources, containerresourceratios and ephemeralstoragelimit (no
// memory or ephemeral-storage limits); kube-linter added
// pdb-unhealthy-pod-eviction-policy and a false liveness-port on both drivers.

// Every container rendered with every sidecar this chart can turn on.
var allSidecars = []string{
	"--set", "openshift.enabled=true",
	"--set", "sidecars.snapshotter.enabled=true",
	"--set", "sidecars.csiAddons.enabled=true",
	"--set", "metrics.enabled=true",
}

type workload struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name        string            `yaml:"name"`
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	Spec struct {
		UnhealthyPodEvictionPolicy string `yaml:"unhealthyPodEvictionPolicy"`
		Template                   struct {
			Spec struct {
				Containers []map[string]any `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func renderObjects(t *testing.T, args ...string) []workload {
	t.Helper()
	all := append([]string{"template", "qumulo-csi", chartPath, "--namespace", "qumulo-csi",
		"--api-versions", "snapshot.storage.k8s.io/v1/VolumeSnapshotClass",
		"--api-versions", "csiaddons.openshift.io/v1alpha1/CSIAddonsNode"}, args...)
	out, err := exec.Command("helm", all...).CombinedOutput()
	require.NoError(t, err, "helm template failed: %s", out)
	var objs []workload
	for _, doc := range strings.Split(string(out), "\n---\n") {
		var w workload
		if yaml.Unmarshal([]byte(doc), &w) == nil && w.Kind != "" {
			objs = append(objs, w)
		}
	}
	return objs
}

func workloadsOf(objs []workload) []workload {
	var out []workload
	for _, o := range objs {
		if o.Kind == "Deployment" || o.Kind == "DaemonSet" {
			out = append(out, o)
		}
	}
	return out
}

func field(m map[string]any, path ...string) any {
	var cur any = m
	for _, p := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[p]
	}
	return cur
}

// Gatekeeper requiredprobes: liveness and readiness on every container, of a
// type it accepts. A named probe port must be one the container declares
// (kube-linter liveness-port / readiness-port); the drivers' numeric ports
// are their liveness-probe sidecar's and are the one documented exception.
func TestEveryContainerHasLivenessAndReadiness(t *testing.T) {
	ws := workloadsOf(renderObjects(t, allSidecars...))
	require.Len(t, ws, 2)
	for _, w := range ws {
		for _, c := range w.Spec.Template.Spec.Containers {
			name := c["name"].(string)
			declared := map[string]bool{}
			if ports, ok := c["ports"].([]any); ok {
				for _, p := range ports {
					if n, ok := p.(map[string]any)["name"].(string); ok {
						declared[n] = true
					}
				}
			}
			for _, kind := range []string{"livenessProbe", "readinessProbe"} {
				probe, ok := c[kind].(map[string]any)
				if !assert.True(t, ok, "%s/%s has no %s", w.Kind, name, kind) {
					continue
				}
				var typed bool
				for _, pt := range []string{"httpGet", "tcpSocket", "exec"} {
					if h, ok := probe[pt].(map[string]any); ok {
						typed = true
						if port, ok := h["port"].(string); ok {
							assert.True(t, declared[port], "%s/%s %s reads port %q, which it does not declare", w.Kind, name, kind, port)
						}
					}
				}
				assert.True(t, typed, "%s/%s %s is not httpGet, tcpSocket or exec", w.Kind, name, kind)
			}
		}
	}
}

// With leader election, csi-lib-utils serves /healthz/leader-election on the
// sidecar's --http-endpoint; without it that path does not exist and the
// probes fall back to the listener.
func TestSidecarProbesFollowLeaderElection(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "httpGet"},
		{[]string{"--set", "controller.leaderElection=false", "--set", "controller.replicaCount=1"}, "tcpSocket"},
	} {
		for _, w := range workloadsOf(renderObjects(t, tc.args...)) {
			if w.Kind != "Deployment" {
				continue
			}
			for _, c := range w.Spec.Template.Spec.Containers {
				name := c["name"].(string)
				if name != "csi-provisioner" && name != "csi-resizer" {
					continue
				}
				assert.NotNil(t, field(c, "livenessProbe", tc.want), "%s liveness should be %s", name, tc.want)
				if tc.want == "httpGet" {
					assert.Equal(t, "/healthz/leader-election", field(c, "livenessProbe", "httpGet", "path"))
				}
				var endpoint bool
				for _, a := range c["args"].([]any) {
					endpoint = endpoint || strings.HasPrefix(a.(string), "--http-endpoint=:")
				}
				assert.True(t, endpoint, "%s needs --http-endpoint for its probes, metrics on or off", name)
			}
		}
	}
}

// Memory limit equal to request (Gatekeeper containerlimits, containerresources,
// containerresourceratios with ratio 1), an ephemeral-storage limit of at most
// 500Mi (ephemeralstoragelimit), and no CPU limit: a throttled mount, unmount
// or CreateVolume is a stuck pod or claim, not a slow one.
func TestEveryContainerHasMemoryAndDiskLimitsButNoCPULimit(t *testing.T) {
	for _, w := range workloadsOf(renderObjects(t, allSidecars...)) {
		for _, c := range w.Spec.Template.Spec.Containers {
			name := c["name"].(string)
			req, _ := field(c, "resources", "requests").(map[string]any)
			lim, _ := field(c, "resources", "limits").(map[string]any)
			require.NotNil(t, lim, "%s/%s has no limits", w.Kind, name)
			assert.NotEmpty(t, req["cpu"], "%s/%s has no CPU request", w.Kind, name)
			assert.NotEmpty(t, lim["memory"], "%s/%s has no memory limit", w.Kind, name)
			assert.Equal(t, req["memory"], lim["memory"], "%s/%s: memory limit must equal the request", w.Kind, name)
			assert.Contains(t, []any{"500Mi"}, lim["ephemeral-storage"], "%s/%s: ephemeral-storage limit", w.Kind, name)
			assert.NotContains(t, lim, "cpu", "%s/%s: no CPU limit", w.Kind, name)
		}
	}
}

// A --reuse-values upgrade from a chart before these defaults carries the old
// requests-only values. The helpers' fallbacks (an emptied value) are the
// defaults above, so an emptied resources value is not left without limits.
func TestEmptiedResourcesFallBackToTheLimitedDefaults(t *testing.T) {
	for _, w := range workloadsOf(renderObjects(t, "--set", "controller.resources=null",
		"--set", "controller.sidecarResources=null", "--set", "node.resources=null")) {
		for _, c := range w.Spec.Template.Spec.Containers {
			assert.NotNil(t, field(c, "resources", "limits", "memory"), "%s/%s", w.Kind, c["name"])
		}
	}
}

// kube-linter pdb-unhealthy-pod-eviction-policy: a Running-but-unready
// controller may always be evicted, so a drain does not hang on it.
func TestControllerPDBAlwaysAllowsEvictingUnhealthyPods(t *testing.T) {
	var found bool
	for _, o := range renderObjects(t) {
		if o.Kind == "PodDisruptionBudget" {
			found = true
			assert.Equal(t, "AlwaysAllow", o.Spec.UnhealthyPodEvictionPolicy)
		}
	}
	assert.True(t, found, "two replicas render a PDB")
}

// kube-linter reads a probe's port against the probed container's own ports.
// The drivers probe their liveness-probe sidecar's port, which the sidecar
// declares; declaring it twice makes the API server warn ("duplicate port
// definition", OCP 4.22). So the finding is ignored on the workload, with the
// reason, and the port is declared exactly once.
func TestDriverProbePortIsDeclaredOnceAndTheLinterIsTold(t *testing.T) {
	for _, w := range workloadsOf(renderObjects(t)) {
		assert.Contains(t, w.Metadata.Annotations["ignore-check.kube-linter.io/liveness-port"], "liveness-probe sidecar",
			"%s must say why kube-linter's liveness-port finding is a false one", w.Kind)
		seen := map[float64]string{}
		for _, c := range w.Spec.Template.Spec.Containers {
			ports, _ := c["ports"].([]any)
			for _, p := range ports {
				n := float64(p.(map[string]any)["containerPort"].(int))
				assert.Empty(t, seen[n], "%s: port %v declared by %s and %s", w.Kind, n, seen[n], c["name"])
				seen[n] = c["name"].(string)
			}
		}
	}
}

// Both node health ports are host ports on every node.
func TestNodeHealthPortsMustDiffer(t *testing.T) {
	out, err := exec.Command("helm", "template", "qumulo-csi", chartPath, "--namespace", "qumulo-csi",
		"--set", "node.registrarHealthPort=29653").CombinedOutput()
	require.Error(t, err)
	assert.Contains(t, string(out), "node.registrarHealthPort and node.healthPort are both 29653")

	out, err = exec.Command("helm", "template", "qumulo-csi", chartPath, "--namespace", "qumulo-csi",
		"--set", "metrics.port.provisioner=29659").CombinedOutput()
	require.Error(t, err, "the sidecars' endpoints are bound with metrics off too")
	assert.Contains(t, string(out), "metrics.port.provisioner is 29659, which the node's registrar")
}

// A fixed registrar port of 29659 in every release left a second
// release's node pods unschedulable ("didn't have free ports"). Unset, it is
// node.healthPort + 6, so a second release, which already needs its own
// healthPort, gets its own registrar port; set, it is taken as given.
func TestNodeRegistrarPortFollowsHealthPort(t *testing.T) {
	node := func(args ...string) string {
		all := append([]string{"template", "qumulo-csi", chartPath, "--namespace", "qumulo-csi",
			"--show-only", "templates/node.yaml"}, args...)
		out, err := exec.Command("helm", all...).CombinedOutput()
		require.NoError(t, err, "%s", out)
		return string(out)
	}
	assert.Contains(t, node(), "--http-endpoint=$(POD_IP):29659", "unchanged for the default healthPort")
	second := node("--set", "node.healthPort=29755")
	assert.Contains(t, second, "--http-endpoint=$(POD_IP):29761")
	assert.Contains(t, second, "containerPort: 29761")
	assert.NotContains(t, second, "29659", "a second release must not ask for the first one's port")
	assert.Contains(t, node("--set", "node.healthPort=29755", "--set", "node.registrarHealthPort=29800"),
		"--http-endpoint=$(POD_IP):29800", "an explicit port wins")
}

// The cross-release check runs only against a live API server (lookup), so
// offline it can only be shown to be there and to skip our own DaemonSet.
func TestNodeHostPortsCheckedAgainstOtherDaemonSets(t *testing.T) {
	src, err := os.ReadFile(filepath.Join(chartPath, "templates", "_validate.tpl"))
	require.NoError(t, err)
	s := string(src)
	assert.Contains(t, s, `lookup "apps/v1" "DaemonSet" "" ""`)
	assert.Contains(t, s, "already holds it there; this release's pods would never start there")
	assert.Contains(t, s, `(eq $ds.metadata.name $ownDS)`, "an upgrade must not refuse its own DaemonSet")
}
