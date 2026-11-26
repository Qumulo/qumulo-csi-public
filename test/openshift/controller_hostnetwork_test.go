//go:build openshift

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package openshift

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// controller.hostNetwork. The controller on the
// node's network must be admitted by hostnetwork-v2 -- OpenShift's own SCC
// for this, with no privilege and a uid from the namespace's range -- and
// must not need the node plugin's privileged SCC. restricted-v2 must refuse
// it, which is why the chart refuses that pairing at install.
func TestControllerOnHostNetworkIsAdmittedByHostNetworkV2(t *testing.T) {
	opts := openShiftValues(t)
	// The test values pin controllerSCC: restricted-v2; empty is the default
	// that selects hostnetwork-v2 with host networking.
	opts.Sets = []string{"controller.hostNetwork=true", "openshift.controllerSCC="}
	manifest := Render(t, opts)
	ctrl := manifest.Named(t, "Deployment", "-controller")
	pod := PodTemplate(t, ctrl)
	require.True(t, pod.HostNetwork)

	ann, _ := asMap(ctrl.Dig("spec", "template", "metadata", "annotations"))
	assert.Equal(t, "hostnetwork-v2", ann["openshift.io/required-scc"])

	assert.Empty(t, HostNetworkV2().Admit(pod), "hostnetwork-v2 must admit the host-network controller")
	assert.NotEmpty(t, RestrictedV2().Admit(pod), "restricted-v2 forbids host networking")
	for _, name := range pod.ContainerNames() {
		sc := pod.Effective(t, name)
		assert.Nil(t, sc.RunAsUser, "container %s: hostnetwork-v2 assigns the uid", name)
		assert.False(t, boolValue(sc.Privileged), "container %s", name)
	}

	// The grant names the controller alone, and the node's SCC grant does
	// not name the controller.
	grant := manifest.Named(t, "RoleBinding", "-controller-scc")
	assert.Equal(t, "system:openshift:scc:hostnetwork-v2", grant.Dig("roleRef", "name"))
	subjects, _ := grant.Dig("subjects").([]any)
	require.Len(t, subjects, 1)
	subject, _ := asMap(subjects[0])
	assert.Equal(t, "qcsi-qumulo-csi-driver-controller", subject["name"])
}

// The pod-network controller is unchanged: restricted-v2, no grant.
func TestControllerOnPodNetworkIsUnchanged(t *testing.T) {
	manifest := Render(t, openShiftValues(t))
	pod := PodTemplate(t, manifest.Named(t, "Deployment", "-controller"))
	assert.False(t, pod.HostNetwork)
	assert.Empty(t, RestrictedV2().Admit(pod))
	for _, o := range manifest {
		if o.Kind() == "RoleBinding" {
			assert.NotContains(t, o.Name(), "-controller-scc")
		}
	}
}
