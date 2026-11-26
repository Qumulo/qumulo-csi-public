// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// The live executor's cluster half: each method against fake
// clientsets, so what it sends to the API server is asserted rather than
// assumed. The order of the calls is executeFailover's and is tested against
// recordingExecutor; these pin what each call does.

// unstructuredVR is a VolumeReplication as the API server returns it.
func unstructuredVR(name, desired, state string, conditions ...map[string]any) *unstructured.Unstructured {
	conds := make([]any, 0, len(conditions))
	for _, c := range conditions {
		conds = append(conds, c)
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": volumeReplicationAPIVersion,
		"kind":       volumeReplicationKind,
		"metadata":   map[string]any{"namespace": "apps", "name": name},
		"spec": map[string]any{
			"volumeReplicationClass": "qumulo-replication",
			"replicationState":       desired,
			"autoResync":             false,
			"dataSource":             map[string]any{"kind": "PersistentVolumeClaim", "name": "data"},
		},
		"status": map[string]any{"state": state, "conditions": conds},
	}}
}

func condition(kind, status string) map[string]any {
	return map[string]any{"type": kind, "status": status}
}

// liveExecutor is a clusterExecutor over fake clientsets, with a wait short
// enough for a unit test.
func liveExecutor(t *testing.T, objects []runtime.Object, dynObjects ...runtime.Object) (
	*clusterExecutor, *fake.Clientset, *dynamicfake.FakeDynamicClient, *bytes.Buffer,
) {
	t.Helper()
	clientset := fake.NewClientset(objects...)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			volumeReplicationGVR:      "VolumeReplicationList",
			volumeReplicationClassGVR: "VolumeReplicationClassList",
		}, dynObjects...)
	var out bytes.Buffer
	ex := newClusterExecutor(&kubeClient{clientset: clientset, dynamic: dyn}, options{
		pollInterval: time.Millisecond,
		waitFor:      20 * time.Millisecond,
		awsProfile:   "dr",
	}, &out)
	return ex, clientset, dyn, &out
}

func readVR(t *testing.T, dyn *dynamicfake.FakeDynamicClient, name string) *unstructured.Unstructured {
	t.Helper()
	obj, err := dyn.Resource(volumeReplicationGVR).Namespace("apps").Get(context.Background(), name,
		metav1.GetOptions{})
	require.NoError(t, err)
	return obj
}

func TestNewClusterExecutorTakesTheWaitAndTheProfileFromTheOptions(t *testing.T) {
	ex, _, _, _ := liveExecutor(t, nil)
	assert.Equal(t, time.Millisecond, ex.poll)
	assert.Equal(t, 20*time.Millisecond, ex.bound)
	assert.Equal(t, "dr", ex.awsProfile)
	assert.Empty(t, ex.route53Zone, "no zone, no DNS call")
	assert.NotNil(t, ex.runCommand)
}

// SetReplicationState merge-patches the two spec fields and leaves every other
// field, spec or not, as it was.
func TestClusterExecutorSetReplicationStatePatchesOnlyTheTwoFields(t *testing.T) {
	ex, _, dyn, _ := liveExecutor(t, nil, unstructuredVR("data-dr-vr", "secondary", "Secondary"))

	require.NoError(t, ex.SetReplicationState(context.Background(), replicationChange{
		Namespace: "apps", Name: "data-dr-vr", To: "primary", AutoResync: true,
	}))

	obj := readVR(t, dyn, "data-dr-vr")
	state, _, _ := unstructured.NestedString(obj.Object, "spec", "replicationState")
	resync, _, _ := unstructured.NestedBool(obj.Object, "spec", "autoResync")
	class, _, _ := unstructured.NestedString(obj.Object, "spec", "volumeReplicationClass")
	status, _, _ := unstructured.NestedString(obj.Object, "status", "state")
	assert.Equal(t, "primary", state)
	assert.True(t, resync)
	assert.Equal(t, "qumulo-replication", class, "a field the patch does not name is kept")
	assert.Equal(t, "Secondary", status, "status is the controller's")
}

func TestClusterExecutorSetReplicationStateReportsAMissingObject(t *testing.T) {
	ex, _, _, _ := liveExecutor(t, nil)
	err := ex.SetReplicationState(context.Background(), replicationChange{
		Namespace: "apps", Name: "gone", To: "primary",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "patching VolumeReplication apps/gone")
}

// NudgeReplication is two patches, and autoResync ends where it was wanted.
func TestClusterExecutorNudgeTogglesAutoResyncAndLeavesItOn(t *testing.T) {
	ex, _, dyn, _ := liveExecutor(t, nil, unstructuredVR("data-vr", "secondary", "Secondary"))

	require.NoError(t, ex.NudgeReplication(context.Background(), "apps", "data-vr"))

	patches := 0
	for _, a := range dyn.Actions() {
		if a.GetVerb() == "patch" {
			patches++
		}
	}
	assert.Equal(t, 2, patches, "false then true: two spec changes, each forcing a reconcile")
	resync, _, _ := unstructured.NestedBool(readVR(t, dyn, "data-vr").Object, "spec", "autoResync")
	assert.True(t, resync)
}

func TestClusterExecutorNudgeStopsAtTheFirstFailedPatch(t *testing.T) {
	ex, _, dyn, _ := liveExecutor(t, nil)
	err := ex.NudgeReplication(context.Background(), "apps", "gone")
	require.Error(t, err)
	assert.Len(t, dyn.Actions(), 1, "the second patch is not sent after the first failed")
}

// AwaitReplication reads the object through the dynamic client until it
// reaches the role with Completed=True.
func TestClusterExecutorAwaitReplicationReadsTheLiveObject(t *testing.T) {
	ex, _, _, _ := liveExecutor(t, nil,
		unstructuredVR("data-dr-vr", "primary", "Primary", condition(conditionCompleted, "True")),
		unstructuredVR("stuck-vr", "primary", "Secondary", condition(conditionCompleted, "False")))

	got, err := ex.AwaitReplication(context.Background(), "apps", "data-dr-vr", replicationStatusPrimary)
	require.NoError(t, err)
	assert.Equal(t, "Primary", got.State)

	got, err = ex.AwaitReplication(context.Background(), "apps", "stuck-vr", replicationStatusPrimary)
	require.Error(t, err, "bounded: it gives up and says what it saw")
	assert.Equal(t, "Secondary", got.State)

	_, err = ex.AwaitReplication(context.Background(), "apps", "gone", replicationStatusPrimary)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading VolumeReplication apps/gone")
}

// AwaitSettled returns at once for an object that is not reconciling, and
// gives up on one that stays Resyncing without calling it progress.
func TestClusterExecutorAwaitSettledReadsTheLiveObject(t *testing.T) {
	ex, _, _, _ := liveExecutor(t, nil,
		unstructuredVR("settled-vr", "secondary", "Secondary", condition(conditionCompleted, "True")),
		unstructuredVR("resyncing-vr", "secondary", "Secondary", condition(conditionResyncing, "True")))

	got, progressed, err := ex.AwaitSettled(context.Background(), "apps", "settled-vr")
	require.NoError(t, err)
	assert.False(t, progressed)
	assert.Equal(t, "Secondary", got.State)

	got, progressed, err = ex.AwaitSettled(context.Background(), "apps", "resyncing-vr")
	require.NoError(t, err)
	assert.False(t, progressed, "an object that never changed made no progress")
	assert.True(t, got.resyncing())
}

// QuiesceOwner writes the replica count and the annotation that remembers it
// in one patch; RestoreOwner scales back and clears the annotation.
func TestClusterExecutorQuiescesAndRestoresAnOwnerInOnePatchEach(t *testing.T) {
	three := int32(3)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "web"},
		Spec:       appsv1.DeploymentSpec{Replicas: &three},
	}
	ex, clientset, _, _ := liveExecutor(t, []runtime.Object{deploy})
	owner := quiesceOwner{Kind: kindDeployment, Namespace: "apps", Name: "web", Replicas: 3, Restore: 3}

	require.NoError(t, ex.QuiesceOwner(context.Background(), owner))
	got, err := clientset.AppsV1().Deployments("apps").Get(context.Background(), "web", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int32(0), *got.Spec.Replicas)
	assert.Equal(t, "3", got.Annotations[annotationQuiescedReplicas])

	// A re-run reads zero and must not record it over the remembered 3.
	owner.Replicas, owner.Remembered = 0, true
	require.NoError(t, ex.QuiesceOwner(context.Background(), owner))
	got, err = clientset.AppsV1().Deployments("apps").Get(context.Background(), "web", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "3", got.Annotations[annotationQuiescedReplicas])

	require.NoError(t, ex.RestoreOwner(context.Background(), owner))
	got, err = clientset.AppsV1().Deployments("apps").Get(context.Background(), "web", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int32(3), *got.Spec.Replicas)
	assert.NotContains(t, got.Annotations, annotationQuiescedReplicas)

	patches := 0
	for _, a := range clientset.Actions() {
		if a.GetVerb() == "patch" {
			patches++
		}
	}
	assert.Equal(t, 3, patches, "one patch per call: the count and its record never separate")
}

func TestClusterExecutorQuiesceRefusesAKindItDoesNotScale(t *testing.T) {
	ex, _, _, _ := liveExecutor(t, nil)
	err := ex.QuiesceOwner(context.Background(), quiesceOwner{Kind: "DaemonSet", Namespace: "apps", Name: "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a kind this command scales")
}

// DeletePod treats a pod already gone as done.
func TestClusterExecutorDeletePod(t *testing.T) {
	ex, clientset, _, _ := liveExecutor(t, []runtime.Object{
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "bare"}},
	})

	require.NoError(t, ex.DeletePod(context.Background(), "apps", "bare"))
	_, err := clientset.CoreV1().Pods("apps").Get(context.Background(), "bare", metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err))

	require.NoError(t, ex.DeletePod(context.Background(), "apps", "bare"), "already gone is success")

	clientset.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "bare",
			errors.New("no RBAC"))
	})
	require.Error(t, ex.DeletePod(context.Background(), "apps", "bare"))
}

// AwaitNoPodsMounting returns nothing once the pods are gone, names the ones
// still there when the bound expires, and fails on a list it could not read.
func TestClusterExecutorAwaitNoPodsMounting(t *testing.T) {
	ex, clientset, _, _ := liveExecutor(t, []runtime.Object{
		podMounting("writer", "data"), podMounting("bystander", "other"),
	})

	remaining, err := ex.AwaitNoPodsMounting(context.Background(), "apps", []string{"data"})
	require.NoError(t, err)
	assert.Equal(t, []string{"apps/writer"}, remaining, "bounded: the writer is still there")

	require.NoError(t, clientset.CoreV1().Pods("apps").Delete(context.Background(), "writer",
		metav1.DeleteOptions{}))
	remaining, err = ex.AwaitNoPodsMounting(context.Background(), "apps", []string{"data"})
	require.NoError(t, err)
	assert.Empty(t, remaining)

	clientset.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("etcdserver: request timed out"))
	})
	_, err = ex.AwaitNoPodsMounting(context.Background(), "apps", []string{"data"})
	require.Error(t, err, "a list that failed is not an empty list")
}

// Settle skips a zero TTL, and is bounded by the context otherwise.
func TestClusterExecutorSettle(t *testing.T) {
	ex, _, _, out := liveExecutor(t, nil)
	require.NoError(t, ex.Settle(context.Background(), 0))
	assert.Empty(t, out.String())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ex.Settle(ctx, 60), context.Canceled)
	assert.Contains(t, out.String(), "waiting one TTL (60s)")
}

// The listing side failover reads its pairs from.
func TestKubeClientListsVolumeReplicationsAndClasses(t *testing.T) {
	class := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": volumeReplicationAPIVersion,
		"kind":       volumeReplicationClassKind,
		"metadata":   map[string]any{"name": "qumulo-replication"},
		"spec": map[string]any{
			"provisioner": "csi.qumulo.com",
			"parameters":  map[string]any{"replicationScope": "group"},
		},
	}}
	ex, _, _, _ := liveExecutor(t, nil, unstructuredVR("data-vr", "primary", "Primary"), class)

	vrs, err := ex.client.listVolumeReplications(context.Background(), "apps")
	require.NoError(t, err)
	require.Len(t, vrs, 1)
	assert.Equal(t, "data", vrs[0].ClaimName)

	classes, err := ex.client.listVolumeReplicationClasses(context.Background())
	require.NoError(t, err)
	require.Len(t, classes, 1)
	assert.Equal(t, "qumulo-replication", classes[0].Name)
}
