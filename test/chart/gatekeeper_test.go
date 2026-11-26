//go:build chart

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

import (
	"bytes"
	"errors"
	"io"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var sourceLine = regexp.MustCompile(`(?m)^# Source: qumulo-csi-driver/(\S+)$`)

// Every RoleBinding and ClusterRoleBinding comes from rbac-bindings.yaml and
// nothing else does, so `--show-only templates/rbac-bindings.yaml` hands a
// platform team exactly the bindings and nothing more, for every combination
// of the switches that add one.
func TestBindingsAllComeFromOneFile(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--set", "openshift.enabled=true"},
		{"--set", "openshift.enabled=true", "--set", "sidecars.capacity.enabled=true", "--set", "tests.storageClass=x"},
		{"--set", "controller.replicaCount=1", "--set", "controller.leaderElection=false", "--set", "rbac.clusterWideSecrets=true"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			out := renderOpenShift(t, args...)
			bindings := 0
			for _, doc := range strings.Split(out, "\n---\n") {
				m := sourceLine.FindStringSubmatch(doc)
				if m == nil {
					continue
				}
				isBinding := strings.Contains(doc, "\nkind: RoleBinding\n") || strings.Contains(doc, "\nkind: ClusterRoleBinding\n")
				assert.Equal(t, isBinding, m[1] == "templates/rbac-bindings.yaml", "%s in %s", firstKindName(doc), m[1])
				if isBinding {
					bindings++
				}
			}
			assert.Positive(t, bindings)

			off := renderOpenShift(t, append(args, "--set", "rbac.createBindings=false")...)
			assert.NotContains(t, off, "kind: RoleBinding")
			assert.NotContains(t, off, "kind: ClusterRoleBinding")
			assert.NotContains(t, off, "templates/rbac-bindings.yaml")
		})
	}
}

func firstKindName(doc string) string {
	kind := regexp.MustCompile(`(?m)^kind: (\S+)`).FindStringSubmatch(doc)
	name := regexp.MustCompile(`(?m)^  name: (\S+)`).FindStringSubmatch(doc)
	if kind == nil || name == nil {
		return "?"
	}
	return kind[1] + " " + name[1]
}

// The three new values are typed: a typo is refused, not read as "on".
func TestGatekeeperValuesAreTyped(t *testing.T) {
	for _, set := range []string{
		"rbac.createBindings=no",
		"rbac.checkBindings=maybe",
		"openshift.assignControllerUID=yes",
	} {
		out, err := exec.Command("helm", "template", "qumulo-csi", chartPath, "--set-string", set).CombinedOutput()
		require.Error(t, err, "%s was accepted: %s", set, out)
		assert.Contains(t, string(out), "schema")
	}
}

// Off OpenShift the controller and the helm test pods keep uid 65532 whatever
// assignControllerUID says: only an OpenShift SCC assigns one.
func TestAssignControllerUIDIsOpenShiftOnly(t *testing.T) {
	out := renderOpenShift(t, "--set", "openshift.assignControllerUID=true", "--set", "tests.storageClass=x")
	assert.NotContains(t, out, "openshift.io/required-scc")
	assert.Equal(t, 6, strings.Count(out, "runAsUser: 65532"), "4 controller containers and 2 test pods")
}

// The helm test pods follow the controller's SCC and uid choice, so a cluster
// that admits the driver admits its tests.
func TestHelmTestPodsFollowTheControllerSCC(t *testing.T) {
	for _, tc := range []struct {
		scc  string
		uids int
	}{
		{"restricted-v2", 0},
		{"restricted-csi", 0},
		{"nonroot-v2", 2 + 4},
		{"anyuid", 2 + 4},
	} {
		out := renderOpenShift(t, "--set", "openshift.enabled=true", "--set", "openshift.controllerSCC="+tc.scc, "--set", "tests.storageClass=x")
		assert.Equal(t, 1+2, strings.Count(out, `openshift.io/required-scc: "`+tc.scc+`"`), "%s: controller + 2 test pods", tc.scc)
		assert.Equal(t, tc.uids, strings.Count(out, "runAsUser: 65532"), tc.scc)
	}
}

// The volume test pod mounts through the node plugin, so it must land only
// where the node DaemonSet runs. With node.nodeSelector set (the client's
// hydra_storage pool) a pod scheduled elsewhere waited on "driver name ... not
// found in the list of registered CSI drivers" until helm test timed out
// (OCP 4.22, 2026-11-06).
func TestVolumeTestPodRunsWhereTheNodePluginRuns(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--set-string", "node.nodeSelector.hydra_storage=true"},
		{"--set-string", "node.nodeSelector.hydra_storage=true", "--set", "node.tolerations=null",
			"--set", "node.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key=zone",
			"--set", "node.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator=Exists"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			all := append([]string{"template", "qumulo-csi", chartPath, "--set", "tests.storageClass=x"}, args...)
			out, err := exec.Command("helm", all...).CombinedOutput()
			require.NoError(t, err, "%s", out)
			var ds, pod map[string]any
			dec := yaml.NewDecoder(bytes.NewReader(out))
			for {
				var doc map[string]any
				if err := dec.Decode(&doc); errors.Is(err, io.EOF) {
					break
				} else {
					require.NoError(t, err)
				}
				switch {
				case doc["kind"] == "DaemonSet":
					ds = doc["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
				case doc["kind"] == "Pod" && strings.HasSuffix(doc["metadata"].(map[string]any)["name"].(string), "-test"):
					pod = doc["spec"].(map[string]any)
				}
			}
			require.NotNil(t, ds)
			require.NotNil(t, pod)
			for _, k := range []string{"nodeSelector", "tolerations", "affinity"} {
				assert.Equal(t, ds[k], pod[k], k)
			}
		})
	}
}
