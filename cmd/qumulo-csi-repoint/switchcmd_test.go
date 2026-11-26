// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// pairFor builds one mirrored pair with the given source server, which is the
// only input planSwitch branches on.
func pairFor(t *testing.T, name, server string, reclaim corev1.PersistentVolumeReclaimPolicy) mirrorPair {
	t.Helper()

	src := sourcePV()
	src.Name = name
	src.Spec.PersistentVolumeReclaimPolicy = reclaim
	src.Spec.CSI.VolumeAttributes["server"] = server
	src.Spec.CSI.VolumeHandle = server + "##/csi/volumes/" + name
	src.Spec.ClaimRef.Name = "claim-" + name

	pv, pvc, err := mirrorVolume(src, path.Join("/csi/volumes", name), testMirrorOptions())
	require.NoError(t, err)

	return mirrorPair{Source: src, Volume: pv, Claim: pvc}
}

// The DNS case, which is the one the runbook asks customers to be in.
//
// Measured on the lab cluster: with a DNS name for `server` and a 60 s TTL, the pod
// came back on the same PVC with no object changes at all once the record was
// flipped and the pod recreated. So the plan must say "flip the record
// and restart the pods" and must not propose editing anything.
func TestSwitchPlanForADNSNamedClassChangesNoObjects(t *testing.T) {
	pairs := []mirrorPair{
		pairFor(t, "pvc-one", "qumulo.example.com", corev1.PersistentVolumeReclaimRetain),
		pairFor(t, "pvc-two", "qumulo.example.com", corev1.PersistentVolumeReclaimRetain),
	}

	plan, err := planSwitch(pairs, "c2.example.com")
	require.NoError(t, err)

	assert.Equal(t, topologyDNS, plan.Topology)
	assert.Empty(t, plan.Switches, "a DNS-named class needs no PVC rebind")
	// One record, both volumes: an operator needs the list of records, not the
	// list of volumes that share one.
	require.Len(t, plan.Flips, 1)
	assert.Equal(t, "qumulo.example.com", plan.Flips[0].Record)
	assert.Equal(t, "c2.example.com", plan.Flips[0].To)
	assert.Equal(t, 2, plan.Flips[0].Volumes)

	// And the pods still have to be recreated: a live mount does not follow a
	// DNS change, because mount.nfs resolved the name once and the kernel
	// holds only the address.
	assert.ElementsMatch(t, []string{"apps/claim-pvc-one", "apps/claim-pvc-two"}, plan.Claims)
	assert.Contains(t, plan.String(), "must still be recreated")
}

// The IP case: spec.csi is immutable, so the claim has to move to the mirrored
// PV.
func TestSwitchPlanForAnIPNamedClassRebindsTheClaim(t *testing.T) {
	pairs := []mirrorPair{pairFor(t, "pvc-one", "192.0.2.20", corev1.PersistentVolumeReclaimRetain)}

	plan, err := planSwitch(pairs, "c2.example.com")
	require.NoError(t, err)

	assert.Equal(t, topologyIP, plan.Topology)
	assert.Empty(t, plan.Flips)
	require.Len(t, plan.Switches, 1)
	assert.Equal(t, "pvc-one", plan.Switches[0].SourcePV)
	assert.Equal(t, "pvc-one-dr", plan.Switches[0].MirroredPV)
	assert.Equal(t, "claim-pvc-one", plan.Switches[0].SourcePVC)
	assert.Equal(t, "apps", plan.Switches[0].Namespace)
	assert.Empty(t, plan.Warnings)
}

// The one condition that turns a failover into data loss.
//
// The IP path deletes the source PVC. If its PV still says Delete, that
// tree-deletes the source volume on an array that may hold the only copy of
// the pre-incident data. So it is a warning in the plan, and runSwitch refuses
// to act on a plan carrying one.
func TestSwitchPlanWarnsAboutASourcePVThatIsNotRetain(t *testing.T) {
	pairs := []mirrorPair{pairFor(t, "pvc-one", "192.0.2.20", corev1.PersistentVolumeReclaimDelete)}

	plan, err := planSwitch(pairs, "c2.example.com")
	require.NoError(t, err)

	require.Len(t, plan.Warnings, 1)
	assert.Contains(t, plan.Warnings[0], "is Delete, not Retain")
	assert.Contains(t, plan.String(), "WARNING")
}

// A cluster part way through adopting DNS names is a real state, and the
// operator must not read "flip the record" and stop there.
func TestSwitchPlanFlagsAMixedSet(t *testing.T) {
	pairs := []mirrorPair{
		pairFor(t, "pvc-named", "qumulo.example.com", corev1.PersistentVolumeReclaimRetain),
		pairFor(t, "pvc-numbered", "192.0.2.20", corev1.PersistentVolumeReclaimRetain),
	}

	plan, err := planSwitch(pairs, "c2.example.com")
	require.NoError(t, err)

	assert.Len(t, plan.Flips, 1)
	assert.Len(t, plan.Switches, 1)
	require.NotEmpty(t, plan.Warnings)
	assert.Contains(t, strings.Join(plan.Warnings, " "), "mixes DNS-named and IP-named volumes")
}

// The plan shares stdout with the manifests the IP case emits, so every line
// of it has to be a YAML comment.
func TestSwitchPlanIsValidAsAYAMLPreamble(t *testing.T) {
	pairs := []mirrorPair{pairFor(t, "pvc-one", "192.0.2.20", corev1.PersistentVolumeReclaimDelete)}
	plan, err := planSwitch(pairs, "c2.example.com")
	require.NoError(t, err)

	for _, line := range strings.Split(strings.TrimSpace(plan.String()), "\n") {
		assert.True(t, strings.HasPrefix(line, "#"), "not a comment: %q", line)
	}
}

// Nothing to switch is an error, not an empty plan that reads like success.
func TestSwitchPlanRefusesAnEmptySet(t *testing.T) {
	_, err := planSwitch(nil, "c2.example.com")
	require.Error(t, err)
}

// "The pods to restart" has to be a list, because after a failover the
// operator recreates exactly these and working them out by hand across
// namespaces is how one gets missed.
func TestPodsUsingClaims(t *testing.T) {
	pods := []corev1.Pod{
		podWithClaims("apps", "web-0", "data"),
		podWithClaims("apps", "web-1", "data", "cache"),
		podWithClaims("apps", "unrelated", "logs"),
		podWithClaims("other", "web-0", "data"), // same claim name, different namespace
		{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "no-volumes"}},
	}

	got := podsUsingClaims(pods, []string{"apps/data"})

	assert.Equal(t, []string{"apps/web-0 (apps/data)", "apps/web-1 (apps/data)"}, got,
		"a claim is namespaced; a pod in another namespace mounting the same name is a different volume")
}

func podWithClaims(namespace, name string, claims ...string) corev1.Pod {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	for _, claim := range claims {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: claim,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
			},
		})
	}
	// A non-PVC volume must not be mistaken for one.
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name:         "tmp",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
	return pod
}
