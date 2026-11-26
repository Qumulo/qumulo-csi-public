// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Step 0, the most important ordering rule of a failover: a promote freezes
// the source, and a frozen Qumulo source silently drops every append the NFS
// client already acknowledged. Everything here is about *what stops*
// before the promote is allowed to run.

// ownedBy attaches a controller ownerReference, as a workload controller does.
func ownedBy(pod corev1.Pod, kind, name string) corev1.Pod {
	controller := true
	pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{
		Kind: kind, Name: name, Controller: &controller,
	})
	return pod
}

// aWorkload is one scalable owner as the cluster reports it.
func aWorkload(kind, name string, replicas int32) workload {
	return workload{Kind: kind, Namespace: "apps", Name: name, Replicas: replicas}
}

// A Deployment-owned pod: the pod's controller is the ReplicaSet, and the
// ReplicaSet's controller is the Deployment. Scaling the ReplicaSet would be
// undone by the Deployment controller on its next reconcile -- and this command
// would have believed it quiesced, and gone on to freeze the source.
func TestPlanQuiesceResolvesAPodThroughItsReplicaSetToItsDeployment(t *testing.T) {
	pods := []corev1.Pod{ownedBy(podWithClaims("apps", "web-7c9-abcde", "data"), kindReplicaSet, "web-7c9")}
	replicaSet := aWorkload(kindReplicaSet, "web-7c9", 3)
	replicaSet.ControllerKind, replicaSet.ControllerName = kindDeployment, "web"
	workloads := []workload{replicaSet, aWorkload(kindDeployment, "web", 3)}

	plan := planQuiesce(pods, workloads, []string{"data", "data-dr"}, false)

	require.NoError(t, plan.refusal())
	require.Len(t, plan.Owners, 1)
	assert.Equal(t, "apps/Deployment/web", plan.Owners[0].object())
	assert.Equal(t, int32(3), plan.Owners[0].Replicas)
	assert.Equal(t, int32(3), plan.Owners[0].Restore)
	assert.Equal(t, []string{"apps/web-7c9-abcde"}, plan.Owners[0].Pods)
	assert.Empty(t, plan.BarePods)
}

// A StatefulSet owns its pods directly, and every replica of it mounting an
// affected claim folds into one edit.
func TestPlanQuiesceScalesAStatefulSetOnce(t *testing.T) {
	pods := []corev1.Pod{
		ownedBy(podWithClaims("apps", "db-0", "data"), kindStatefulSet, "db"),
		ownedBy(podWithClaims("apps", "db-1", "data"), kindStatefulSet, "db"),
	}
	plan := planQuiesce(pods, []workload{aWorkload(kindStatefulSet, "db", 2)}, []string{"data"}, false)

	require.NoError(t, plan.refusal())
	require.Len(t, plan.Owners, 1, "two pods, one owner, one edit")
	assert.Equal(t, "apps/StatefulSet/db", plan.Owners[0].object())
	assert.Equal(t, []string{"apps/db-0", "apps/db-1"}, plan.Owners[0].Pods)
}

// A bare ReplicaSet -- one nothing owns -- is scaled itself.
func TestPlanQuiesceScalesAnUnownedReplicaSet(t *testing.T) {
	pods := []corev1.Pod{ownedBy(podWithClaims("apps", "rs-abcde", "data"), kindReplicaSet, "rs")}
	plan := planQuiesce(pods, []workload{aWorkload(kindReplicaSet, "rs", 1)}, []string{"data"}, false)

	require.NoError(t, plan.refusal())
	require.Len(t, plan.Owners, 1)
	assert.Equal(t, "apps/ReplicaSet/rs", plan.Owners[0].object())
}

// A bare pod stops the command, and the refusal names it. Deleting one destroys
// the only copy of its definition; leaving it writing through the promote
// destroys the writes. Neither is this command's decision to make silently.
func TestPlanQuiesceRefusesABarePod(t *testing.T) {
	pods := []corev1.Pod{podWithClaims("apps", "hand-made", "data")}
	plan := planQuiesce(pods, nil, []string{"data"}, false)

	require.Empty(t, plan.Owners)
	assert.Equal(t, []string{"apps/hand-made"}, plan.BarePods)

	err := plan.refusal()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apps/hand-made")
	assert.Contains(t, err.Error(), "-allow-bare-pods")
	assert.Contains(t, err.Error(), "dropped silently")
	assert.Contains(t, plan.String("apps"), "REFUSAL: bare pod apps/hand-made")
}

// And with -allow-bare-pods it is deleted, with the plan saying in advance
// that nothing will bring it back.
func TestPlanQuiesceDeletesABarePodWhenAllowed(t *testing.T) {
	pods := []corev1.Pod{podWithClaims("apps", "hand-made", "data")}
	plan := planQuiesce(pods, nil, []string{"data"}, true)

	require.NoError(t, plan.refusal())
	assert.Equal(t, []string{"apps/hand-made"}, plan.BarePods)
	assert.Contains(t, plan.String("apps"), "DELETE bare pod apps/hand-made (-allow-bare-pods)")
	assert.Contains(t, plan.restoreString(), "is NOT recreated")
}

// A kind this command cannot scale always stops it, flag or no flag: guessing
// at how to stop a DaemonSet is how a DR run takes a cluster service down.
func TestPlanQuiesceRefusesAnUnscalableOwner(t *testing.T) {
	pods := []corev1.Pod{ownedBy(podWithClaims("apps", "agent-xyz", "data"), "DaemonSet", "agent")}
	plan := planQuiesce(pods, nil, []string{"data"}, true)

	require.Len(t, plan.Unscalable, 1)
	assert.Contains(t, plan.Unscalable[0], "apps/agent-xyz")
	assert.Contains(t, plan.Unscalable[0], "DaemonSet agent")

	err := plan.refusal()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "will not scale")
}

// An owner whose object could not be read is not assumed to be scalable: an
// RBAC gap must not become a silent skip past step 0.
func TestPlanQuiesceRefusesAnOwnerItCannotRead(t *testing.T) {
	pods := []corev1.Pod{ownedBy(podWithClaims("apps", "web-0", "data"), kindDeployment, "web")}
	plan := planQuiesce(pods, nil, []string{"data"}, true)

	require.Len(t, plan.Unscalable, 1)
	assert.Contains(t, plan.Unscalable[0], "not readable here")
	require.Error(t, plan.refusal())
}

// The restore, which is the half that has to survive a re-run.
//
// On a second run of the same direction the owner reads 0 replicas: the count
// to come back to is only in the annotation this command wrote the first time,
// and recording the live count again would turn "scale back to 3" into "scale
// back to 0".
func TestPlanQuiesceRestoresTheRememberedReplicasOnARerun(t *testing.T) {
	pods := []corev1.Pod{ownedBy(podWithClaims("apps", "web-0", "data"), kindStatefulSet, "web")}
	owner := aWorkload(kindStatefulSet, "web", 0)
	owner.Remembered = "3"

	plan := planQuiesce(pods, []workload{owner}, []string{"data"}, false)

	require.Len(t, plan.Owners, 1)
	assert.Equal(t, int32(0), plan.Owners[0].Replicas, "it reads zero now")
	assert.Equal(t, int32(3), plan.Owners[0].Restore, "and comes back to what step 0 recorded")
	assert.True(t, plan.Owners[0].Remembered)
	assert.Contains(t, plan.String("apps"), annotationQuiescedReplicas+" already records 3")
	assert.Contains(t, plan.restoreString(), "scale apps/StatefulSet/web back to 3")
}

// Measured on the lab cluster: the first run was
// interrupted after step 0 (a promote did not complete inside -wait); the
// Deployments were at zero with the annotation on them, and there was no pod
// left to find them by. The resumed run planned nothing for step 0 and step 4
// said "nothing was scaled down, so nothing is brought back": the workloads
// stayed down. Any workload still carrying the annotation is now an owner of
// the plan, with nothing to stop and a restore to do.
func TestPlanQuiesceRestoresOwnersAnEarlierRunLeftAtZero(t *testing.T) {
	web := aWorkload(kindDeployment, "web", 0)
	web.Remembered = "2"
	other := aWorkload(kindDeployment, "other", 1) // no annotation: not ours
	junk := aWorkload(kindDeployment, "junk", 0)
	junk.Remembered = "two" // not this command's annotation

	plan := planQuiesce(nil, []workload{web, other, junk}, []string{"data"}, false)

	require.Len(t, plan.Owners, 1)
	assert.Equal(t, "web", plan.Owners[0].Name)
	assert.True(t, plan.Owners[0].LeftAtZero)
	assert.Equal(t, int32(2), plan.Owners[0].Restore)
	assert.Contains(t, plan.String("apps"), "left there by an earlier run")
	assert.Contains(t, plan.restoreString(), "scale apps/Deployment/web back to 2")
	assert.False(t, plan.empty(), "there is a restore to do even with nothing to stop")
}

// An annotation somebody edited into nonsense is ignored rather than trusted:
// the live count is the better guess, and the plan prints both.
func TestPlanQuiesceIgnoresAnUnparseableRememberedCount(t *testing.T) {
	pods := []corev1.Pod{ownedBy(podWithClaims("apps", "web-0", "data"), kindStatefulSet, "web")}
	owner := aWorkload(kindStatefulSet, "web", 2)
	owner.Remembered = "three"

	plan := planQuiesce(pods, []workload{owner}, []string{"data"}, false)

	require.Len(t, plan.Owners, 1)
	assert.Equal(t, int32(2), plan.Owners[0].Restore)
	assert.False(t, plan.Owners[0].Remembered)
}

// An unset spec.replicas means one, which is what the restore has to put back.
func TestWorkloadFromDefaultsReplicasToOne(t *testing.T) {
	meta := metav1.ObjectMeta{Namespace: "apps", Name: "web"}
	assert.Equal(t, int32(1), workloadFrom(kindDeployment, &meta, nil).Replicas)

	three := int32(3)
	assert.Equal(t, int32(3), workloadFrom(kindDeployment, &meta, &three).Replicas)
}

// A pod that mounts nothing affected is not stopped. A failover is not a
// namespace restart.
func TestPlanQuiesceIgnoresPodsThatDoNotMountAnAffectedClaim(t *testing.T) {
	pods := []corev1.Pod{
		ownedBy(podWithClaims("apps", "other-0", "logs"), kindStatefulSet, "other"),
	}
	plan := planQuiesce(pods, []workload{aWorkload(kindStatefulSet, "other", 1)}, []string{"data"}, false)

	assert.True(t, plan.empty())
	require.NoError(t, plan.refusal())
	assert.Contains(t, plan.String("apps"), "no pod mounts an affected claim")
}

// The plan shares stdout with whatever else is piped alongside it, so step 0
// and step 4 have to stay YAML comments like the rest.
func TestQuiescePlanIsValidAsAYAMLPreamble(t *testing.T) {
	pods := []corev1.Pod{
		ownedBy(podWithClaims("apps", "web-0", "data"), kindStatefulSet, "web"),
		podWithClaims("apps", "hand-made", "data-dr"),
	}
	plan := planQuiesce(pods, []workload{aWorkload(kindStatefulSet, "web", 2)}, []string{"data", "data-dr"}, true)

	rendered := plan.String("apps") + plan.restoreString()
	for _, line := range strings.Split(strings.TrimSpace(rendered), "\n") {
		assert.True(t, strings.HasPrefix(line, "#"), "not a comment: %q", line)
	}
}
