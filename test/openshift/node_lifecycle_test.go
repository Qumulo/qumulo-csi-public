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

// Cluster changes: nodes added, removed and rebooted while applications run.
//
// live/node_add_remove.py, node_loss.py, node_reboot.py, controller_disruption.py
// and mcp_roll.py check the behaviour on a real cluster. What each of them
// leans on, though, is a handful of install properties that roll out cleanly
// whether they are right or not, and only show up the day a node changes:
//
//   - a new node must not count the node plugin until the kubelet has
//     registered it, because a PV's nodeAffinity names the topology label that
//     registration sets;
//   - the node plugin must keep its place on a node that fills up;
//   - a drain must always leave one controller replica holding the leases, and
//     must never be blocked outright by the driver;
//   - a node that is lost must leave nothing to detach.
//
// As in outage_test.go, these assert the install as it ships. A failure here is
// a finding about the install, not a reason to change the test.

// TestNewNodeCountsThePluginOnlyOnceRegistered.
//
// A PV this driver provisions carries nodeAffinity on
// topology.csi.qumulo.com/plugin, which the kubelet sets on a node only after
// the node plugin's first NodeGetInfo. On a node that has just joined, a pod
// with an existing volume can therefore schedule there only once that has
// happened -- and a node pod that reports Ready before it has registered lets a
// rollout or an autoscaler treat the node as able to mount when it cannot.
// The registration readiness probe and minReadySeconds are what close
// that window.
func TestNewNodeCountsThePluginOnlyOnceRegistered(t *testing.T) {
	daemonSet := Render(t, openShiftValues(t)).Named(t, "DaemonSet", "-node")

	minReady, _ := intOf(daemonSet.Dig("spec", "minReadySeconds"))
	assert.Positive(t, minReady,
		"the node DaemonSet has no minReadySeconds, so a node pod counts as available the instant it is Ready, "+
			"before the kubelet has registered it on a newly added node")

	spec, ok := asMap(daemonSet.Dig("spec", "template", "spec"))
	require.True(t, ok)
	found := false
	for _, c := range sliceOf(spec["containers"]) {
		co, ok := asMap(c)
		if !ok || co.str("name") != driverContainer {
			continue
		}
		found = true
		assert.NotNil(t, co["readinessProbe"],
			"the driver container has no readiness probe, so a node pod on a new node reports Ready before the "+
				"kubelet's first NodeGetInfo -- before the topology label that every PV's nodeAffinity needs exists")
	}
	require.True(t, found, "the node DaemonSet has no %s container", driverContainer)
}

// TestNodePluginKeepsItsPlaceOnAFullNode.
//
// A node that joins and fills with workloads, or a node plugin re-created on a
// packed node after a reboot, must still run the plugin: without it, no Qumulo
// volume on that node can mount or unmount. The chart gives the node pods a
// PriorityClass of their own for that.
func TestNodePluginKeepsItsPlaceOnAFullNode(t *testing.T) {
	manifest := Render(t, openShiftValues(t))
	daemonSet := manifest.Named(t, "DaemonSet", "-node")

	name, _ := daemonSet.Dig("spec", "template", "spec", "priorityClassName").(string)
	require.NotEmpty(t, name,
		"the node pods have no priorityClassName, so on a full node an ordinary workload can keep the node plugin "+
			"Pending and every Qumulo volume on that node stops mounting and unmounting")

	for _, pc := range manifest.Kinds("PriorityClass") {
		if pc.Name() != name {
			continue
		}
		value, _ := intOf(pc["value"])
		assert.GreaterOrEqual(t, value, int64(1000000000),
			"PriorityClass %s has value %d, below the system range, so ordinary workloads can preempt the node plugin",
			name, value)
		return
	}
	// Not rendered: then it must be a class the cluster already has.
	assert.Contains(t, []string{"system-node-critical", "system-cluster-critical"}, name,
		"the node pods name PriorityClass %q, which the chart does not render and is not a built-in class", name)
}

// TestADrainAlwaysLeavesAControllerReplica.
//
// Draining the node that runs a controller replica -- which every
// MachineConfigPool roll and every z-stream upgrade does -- must leave a
// standby to take the leases, and must still be able to finish. A PDB of
// maxUnavailable 1 does both: the drain evicts one replica, waits for its
// replacement, then the next. With one replica the chart renders no PDB at all,
// because a PDB there would block every drain of that node forever.
func TestADrainAlwaysLeavesAControllerReplica(t *testing.T) {
	manifest := Render(t, openShiftValues(t))
	deployment := manifest.Named(t, "Deployment", "-controller")

	replicas, _ := intOf(deployment.Dig("spec", "replicas"))
	require.GreaterOrEqual(t, replicas, int64(2),
		"the recommended OpenShift install runs %d controller replica(s): draining its node stops every "+
			"provision, expand and snapshot until the pod is rescheduled", replicas)

	pdb := manifest.Named(t, "PodDisruptionBudget", "-controller")
	maxUnavailable, _ := intOf(pdb.Dig("spec", "maxUnavailable"))
	assert.Equal(t, int64(1), maxUnavailable,
		"the controller PDB is not maxUnavailable 1, so a drain either takes both replicas at once or is blocked")

	selector, ok := asMap(pdb.Dig("spec", "selector", "matchLabels"))
	require.True(t, ok, "the controller PDB has no matchLabels selector")
	labels, ok := asMap(deployment.Dig("spec", "template", "metadata", "labels"))
	require.True(t, ok)
	for k, v := range selector {
		assert.Equal(t, v, labels[k],
			"the controller PDB selects %s=%v, which the controller pods do not carry: it protects nothing", k, v)
	}

	single := Render(t, Options{
		Release:     "qcsi",
		Namespace:   "qumulo-csi",
		ValuesFiles: []string{ValuesFile(t, "openshift-values.yaml")},
		Sets:        []string{"controller.replicaCount=1"},
	})
	assert.Empty(t, single.Kinds("PodDisruptionBudget"),
		"with one controller replica the chart still renders a PDB, which blocks every drain of that node")
}

// TestControllerReplicasPreferSeparateNodes.
//
// Two replicas on one node make that node's drain or loss an outage for every
// storage operation. The chart asks the scheduler to spread them, softly
// -- a hard rule would leave the second replica Pending on a single-worker
// cluster. Soft means it can still happen (seen on the OCP lab, INS-01), which
// is why live/controller_disruption.py drains the node with the most replicas
// rather than assuming one each.
func TestControllerReplicasPreferSeparateNodes(t *testing.T) {
	deployment := Render(t, openShiftValues(t)).Named(t, "Deployment", "-controller")

	terms := sliceOf(deployment.Dig("spec", "template", "spec", "affinity", "podAntiAffinity",
		"preferredDuringSchedulingIgnoredDuringExecution"))
	require.NotEmpty(t, terms, "the controller has no pod anti-affinity: both replicas may land on one node")

	labels, ok := asMap(deployment.Dig("spec", "template", "metadata", "labels"))
	require.True(t, ok)
	spread := false
	for _, term := range terms {
		m, ok := asMap(term)
		if !ok {
			continue
		}
		affinityTerm, ok := asMap(m["podAffinityTerm"])
		if !ok || affinityTerm.str("topologyKey") != "kubernetes.io/hostname" {
			continue
		}
		match, _ := asMap(affinityTerm.Dig("labelSelector", "matchLabels"))
		selectsSelf := len(match) > 0
		for k, v := range match {
			if labels[k] != v {
				selectsSelf = false
			}
		}
		spread = spread || selectsSelf
	}
	assert.True(t, spread,
		"no anti-affinity term spreads the controller pods over kubernetes.io/hostname by their own labels")
}

// TestALostNodeHasNothingToDetach.
//
// When a node is lost, the non-graceful shutdown path (the out-of-service
// taint) force-deletes its pods and detaches their VolumeAttachments. This
// driver publishes none -- attachRequired is false -- so a replacement pod on
// another node mounts at once and nothing is left behind to time out. That is
// what live/node_loss.py relies on, and why it checks mounts and registration
// rather than attachments. An attachRequired: true would also need a
// csi-attacher the chart deliberately does not deploy.
func TestALostNodeHasNothingToDetach(t *testing.T) {
	driver := Render(t, openShiftValues(t)).Named(t, "CSIDriver", "csi.qumulo.com")

	attach := boolPtr(driver.Dig("spec", "attachRequired"))
	require.NotNil(t, attach, "the CSIDriver does not state attachRequired; Kubernetes then defaults it to true")
	assert.False(t, *attach,
		"attachRequired is true: every pod would wait on a VolumeAttachment no attacher serves, and a lost node's "+
			"attachments would have to time out before its pods could move")
}
