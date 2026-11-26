// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package k8s

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
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	rcNS  = "qumulo-csi"
	rcDrv = "csi.qumulo.com"
	rcDep = "qcsi-controller"
	rcDS  = "qcsi-node"
)

func readyDeployment(replicas, ready int32, available bool) *appsv1.Deployment {
	status := corev1.ConditionFalse
	if available {
		status = corev1.ConditionTrue
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: rcDep, Namespace: rcNS, Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			ReadyReplicas:      ready,
			Conditions:         []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: status}},
		},
	}
}

func readyDaemonSet(want, updated, ready, available int32) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: rcDS, Namespace: rcNS, Generation: 2},
		Status: appsv1.DaemonSetStatus{
			ObservedGeneration:     2,
			DesiredNumberScheduled: want,
			UpdatedNumberScheduled: updated,
			NumberReady:            ready,
			NumberAvailable:        available,
		},
	}
}

func csiNode(name string, drivers ...string) *storagev1.CSINode {
	n := &storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: name}}
	for _, d := range drivers {
		n.Spec.Drivers = append(n.Spec.Drivers, storagev1.CSINodeDriver{Name: d, NodeID: name})
	}
	return n
}

// newCheck returns a check over objs whose sleep advances nothing but counts
// polls and, after each, runs between (to change the cluster mid-wait). The
// timeout is enforced by maxPolls rather than the clock, so no test sleeps.
func newCheck(t *testing.T, maxPolls int, between func(poll int, c *fake.Clientset), objs ...runtime.Object) (ReadyCheck, *fake.Clientset, *bytes.Buffer) {
	t.Helper()
	client := fake.NewClientset(objs...)
	out := &bytes.Buffer{}
	polls := 0
	return ReadyCheck{
		Client: client, Namespace: rcNS, DriverName: rcDrv, Deployment: rcDep, DaemonSet: rcDS,
		Timeout: 240 * time.Second, Interval: time.Second, Out: out,
		sleep: func(ctx context.Context, _ time.Duration) error {
			polls++
			if polls > maxPolls {
				return context.DeadlineExceeded
			}
			if between != nil {
				between(polls, client)
			}
			return nil
		},
	}, client, out
}

func TestReadyCheckPassesOnAHealthyInstall(t *testing.T) {
	rc, _, out := newCheck(t, 0, nil,
		readyDeployment(2, 2, true),
		readyDaemonSet(3, 3, 3, 3),
		csiNode("n1", rcDrv), csiNode("n2", "other.csi", rcDrv), csiNode("n3", rcDrv), csiNode("n4", "other.csi"))
	require.NoError(t, rc.Run(context.Background()))
	assert.Contains(t, out.String(), "ok: controller deployment/qcsi-controller is Available, 2 of 2 replicas Ready")
	assert.Contains(t, out.String(), "ok: daemonset/qcsi-node: 3 of 3 node pods Ready")
	assert.Contains(t, out.String(), "ok: csi.qumulo.com registered in 3 CSINode(s): n1 n2 n3")
	assert.Contains(t, out.String(), "PASS:")
}

// A Deployment scaled to 0 reports Available (0 of 0 required) and would
// serve nothing; that is a failure at once, not something to wait out.
func TestReadyCheckFailsAtOnceOnAControllerScaledToZero(t *testing.T) {
	rc, _, _ := newCheck(t, 100, nil, readyDeployment(0, 0, true), readyDaemonSet(1, 1, 1, 1), csiNode("n1", rcDrv))
	err := rc.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deployment/qcsi-controller: scaled to 0")
}

func TestReadyCheckWaitsForTheControllerAndReportsTheLastStateOnTimeout(t *testing.T) {
	rc, _, out := newCheck(t, 3, nil, readyDeployment(2, 1, true), readyDaemonSet(1, 1, 1, 1), csiNode("n1", rcDrv))
	err := rc.Run(context.Background())
	require.Error(t, err)
	assert.Equal(t, "controller deployment/qcsi-controller: 1 of 2 replicas Ready after 4m0s", err.Error())
	// An unchanged state is printed once, not once per poll.
	assert.Equal(t, 1, bytes.Count(out.Bytes(), []byte("waiting:")))
}

// `helm upgrade --wait` hands over with node pods still starting.
// The check waits for the rollout instead of failing on its first read.
func TestReadyCheckWaitsForTheNodeRolloutToFinish(t *testing.T) {
	between := func(poll int, c *fake.Clientset) {
		if poll == 2 {
			_, err := c.AppsV1().DaemonSets(rcNS).UpdateStatus(context.Background(), readyDaemonSet(5, 5, 5, 5), metav1.UpdateOptions{})
			require.NoError(t, err)
		}
	}
	rc, _, out := newCheck(t, 10, between,
		readyDeployment(1, 1, true), readyDaemonSet(5, 5, 4, 4),
		csiNode("a", rcDrv), csiNode("b", rcDrv), csiNode("c", rcDrv), csiNode("d", rcDrv), csiNode("e", rcDrv))
	require.NoError(t, rc.Run(context.Background()))
	assert.Contains(t, out.String(), "waiting: 4 of 5 node pods Ready (5 updated, 4 available)")
	assert.Contains(t, out.String(), "ok: daemonset/qcsi-node: 5 of 5 node pods Ready")
}

func TestReadyCheckDoesNotTrustARolloutTheControllerHasNotObserved(t *testing.T) {
	ds := readyDaemonSet(1, 1, 1, 1)
	ds.Status.ObservedGeneration = 1
	rc, _, _ := newCheck(t, 2, nil, readyDeployment(1, 1, true), ds, csiNode("n1", rcDrv))
	err := rc.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "daemonset/qcsi-node: rollout not yet observed")
}

func TestReadyCheckFailsOnADaemonSetThatSchedulesNothing(t *testing.T) {
	rc, _, _ := newCheck(t, 100, nil, readyDeployment(1, 1, true), readyDaemonSet(0, 0, 0, 0))
	err := rc.Run(context.Background())
	require.Error(t, err)
	assert.Equal(t, "daemonset/qcsi-node: schedules no node pods", err.Error())
}

// A node pod can be Ready before the kubelet has registered the driver.
func TestReadyCheckRequiresTheDriverInAsManyCSINodesAsNodePods(t *testing.T) {
	rc, _, _ := newCheck(t, 2, nil,
		readyDeployment(1, 1, true), readyDaemonSet(3, 3, 3, 3),
		csiNode("n1", rcDrv), csiNode("n2", rcDrv), csiNode("n3", "other.csi"))
	err := rc.Run(context.Background())
	require.Error(t, err)
	assert.Equal(t, "csi.qumulo.com: registered in 2 CSINode(s): n1 n2, but 3 node pods run after 4m0s", err.Error())
}

// A read refused by RBAC will not be fixed by waiting: fail at once, saying why.
func TestReadyCheckFailsAtOnceOnAForbiddenRead(t *testing.T) {
	rc, client, _ := newCheck(t, 100, nil, readyDeployment(1, 1, true), readyDaemonSet(1, 1, 1, 1))
	client.PrependReactor("list", "csinodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "storage.k8s.io", Resource: "csinodes"}, "", errors.New("no"))
	})
	err := rc.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "csi.qumulo.com: cannot read:")
	assert.True(t, apierrors.IsForbidden(errors.Unwrap(errors.Unwrap(err))))
}

func TestReadyCheckFailsAtOnceOnAMissingDeployment(t *testing.T) {
	rc, _, _ := newCheck(t, 100, nil, readyDaemonSet(1, 1, 1, 1))
	err := rc.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "controller deployment/qcsi-controller: cannot read:")
}

// The check only reads: no watch, no write (Rule 4), and only the verbs the
// test's RBAC grants -- get on the two workloads, list on CSINodes.
func TestReadyCheckOnlyGetsAndLists(t *testing.T) {
	rc, client, _ := newCheck(t, 0, nil, readyDeployment(1, 1, true), readyDaemonSet(1, 1, 1, 1), csiNode("n1", rcDrv))
	require.NoError(t, rc.Run(context.Background()))
	got := map[string]bool{}
	for _, a := range client.Actions() {
		got[a.GetVerb()+" "+a.GetResource().Resource] = true
	}
	assert.Equal(t, map[string]bool{"get deployments": true, "get daemonsets": true, "list csinodes": true}, got)
}

func TestReadyCheckDefaultsAndRealSleepHonourTheTimeout(t *testing.T) {
	client := fake.NewClientset(readyDeployment(1, 0, false))
	rc := ReadyCheck{Client: client, Namespace: rcNS, DriverName: rcDrv, Deployment: rcDep, DaemonSet: rcDS,
		Timeout: 50 * time.Millisecond, Interval: 10 * time.Millisecond}
	start := time.Now()
	err := rc.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not Available, 0 of 1 replicas Ready after 50ms")
	assert.Less(t, time.Since(start), 5*time.Second)
	// Interval <= 0 falls back to 5 s; sleepCtx returns at once on a done context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Error(t, sleepCtx(ctx, time.Hour))
	assert.NoError(t, sleepCtx(context.Background(), time.Millisecond))
}
