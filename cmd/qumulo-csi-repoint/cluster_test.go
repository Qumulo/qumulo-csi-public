// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// pagedPVs serves PersistentVolume lists in pages of two, as an API server
// does when the caller sets a limit, and records each request's limit.
func pagedPVs(t *testing.T, total int) (*fake.Clientset, *[]int64) {
	t.Helper()
	client := fake.NewClientset()
	var limits []int64
	client.PrependReactor("list", "persistentvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		opts := action.(k8stesting.ListActionImpl).GetListOptions()
		limits = append(limits, opts.Limit)
		start := 0
		if opts.Continue != "" {
			_, _ = fmt.Sscanf(opts.Continue, "%d", &start)
		}
		list := &corev1.PersistentVolumeList{}
		for i := start; i < total && i < start+2; i++ {
			list.Items = append(list.Items, corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pv-%d", i)},
				Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{Driver: "csi.qumulo.com", VolumeHandle: fmt.Sprintf("h-%d", i)},
				}},
			})
		}
		if start+2 < total {
			list.Continue = fmt.Sprintf("%d", start+2)
		}
		return true, list, nil
	})
	return client, &limits
}

// The tool listed every PV in the cluster, for every driver, in one
// unpaged request -- 15.5 MiB in one answer for 20,000 PVs -- on each mirror,
// failover and failback, in the middle of an incident. It now pages.
func TestListVolumesPagesThroughEveryPV(t *testing.T) {
	client, limits := pagedPVs(t, 5)
	k := &kubeClient{clientset: client}

	pvs, err := k.listVolumes(context.Background(), "csi.qumulo.com")

	require.NoError(t, err)
	assert.Len(t, pvs, 5, "every page is read")
	require.NotEmpty(t, *limits)
	for _, limit := range *limits {
		assert.Equal(t, int64(listPageSize), limit, "each request is bounded")
	}
}

// Switch listed every pod in every namespace to find the ones using
// its claims. It now lists only the claims' namespaces.
func TestSwitchListsPodsOnlyInTheClaimsNamespaces(t *testing.T) {
	client := fake.NewClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "a"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "b"}},
	)
	k := &kubeClient{clientset: client}

	pods, err := k.listPodsForClaims(context.Background(), []string{"apps/data", "apps/logs"})

	require.NoError(t, err)
	require.Len(t, pods, 1)
	assert.Equal(t, "apps", pods[0].Namespace)
	for _, action := range client.Actions() {
		if action.GetVerb() == "list" {
			assert.Equal(t, "apps", action.GetNamespace(), "no all-namespaces list")
		}
	}
}
