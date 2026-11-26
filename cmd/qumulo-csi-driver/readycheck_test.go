// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

var readyArgs = []string{"ready-check", "-namespace=ns", "-driver-name=csi.qumulo.com",
	"-deployment=q-controller", "-daemonset=q-node", "-timeout=200ms", "-interval=10ms"}

func withReadyClient(t *testing.T, f func() (kubernetes.Interface, error)) {
	t.Helper()
	old := readyCheckClient
	readyCheckClient = f
	t.Cleanup(func() { readyCheckClient = old })
}

func healthyCluster() *fake.Clientset {
	one := int32(1)
	return fake.NewClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "q-controller", Namespace: "ns"},
			Spec:       appsv1.DeploymentSpec{Replicas: &one},
			Status: appsv1.DeploymentStatus{ReadyReplicas: 1,
				Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue}}},
		},
		&appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Name: "q-node", Namespace: "ns"},
			Status:     appsv1.DaemonSetStatus{DesiredNumberScheduled: 1, UpdatedNumberScheduled: 1, NumberReady: 1, NumberAvailable: 1},
		},
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "n1"},
			Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: "csi.qumulo.com", NodeID: "n1"}}}},
	)
}

func TestReadyCheckSubcommandPasses(t *testing.T) {
	withReadyClient(t, func() (kubernetes.Interface, error) { return healthyCluster(), nil })
	var out, errOut bytes.Buffer
	assert.Equal(t, 0, realMain(readyArgs, &out, &errOut))
	assert.Contains(t, out.String(), "PASS:")
	assert.Empty(t, errOut.String())
}

func TestReadyCheckSubcommandFailsWithTheCause(t *testing.T) {
	withReadyClient(t, func() (kubernetes.Interface, error) { return fake.NewClientset(), nil })
	var out, errOut bytes.Buffer
	assert.Equal(t, 1, realMain(readyArgs, &out, &errOut))
	assert.Contains(t, out.String(), "FAIL: controller deployment/q-controller: cannot read:")
}

func TestReadyCheckSubcommandOutsideAPod(t *testing.T) {
	withReadyClient(t, func() (kubernetes.Interface, error) { return nil, errors.New("not in a pod") })
	var out, errOut bytes.Buffer
	assert.Equal(t, 1, realMain(readyArgs, &out, &errOut))
	assert.Equal(t, "FAIL: not in a pod\n", out.String())
}

func TestReadyCheckSubcommandRefusesBadFlags(t *testing.T) {
	withReadyClient(t, func() (kubernetes.Interface, error) {
		t.Fatal("no client may be built for a refused command line")
		return nil, nil
	})
	for name, args := range map[string][]string{
		"unknown flag":      {"ready-check", "-nope"},
		"missing names":     {"ready-check", "-namespace=ns"},
		"zero timeout":      append(append([]string{}, readyArgs...), "-timeout=0s"),
		"stray argument":    append(append([]string{}, readyArgs...), "extra"),
		"negative interval": append(append([]string{}, readyArgs...), "-interval=-1s"),
	} {
		var out, errOut bytes.Buffer
		assert.Equal(t, 2, realMain(args, &out, &errOut), name)
		assert.NotEmpty(t, errOut.String(), name)
	}
}
