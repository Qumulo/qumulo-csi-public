// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package k8s

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func pvc(namespace, name, uid string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(uid)}}
}

// Every namespace's claims are listed: a restore's claim can be in
// any of them.
func TestListPersistentVolumeClaimUIDsReadsEveryNamespace(t *testing.T) {
	client := fake.NewClientset(pvc("apps", "data", "uid-1"), pvc("backup", "restore", "uid-2"))

	uids, err := NewPersistentVolumeClaimClientFor(client).ListPersistentVolumeClaimUIDs(context.Background())

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"uid-1", "uid-2"}, uids)
}

// Every page is read: a claim on page two missed would read as gone.
func TestListPersistentVolumeClaimUIDsFollowsContinueTokens(t *testing.T) {
	client := fake.NewClientset()
	var limits []int64
	client.PrependReactor("list", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		opts := action.(k8stesting.ListActionImpl).GetListOptions()
		limits = append(limits, opts.Limit)
		page := &corev1.PersistentVolumeClaimList{}
		switch opts.Continue {
		case "":
			page.Items = []corev1.PersistentVolumeClaim{*pvc("a", "one", "uid-1")}
			page.Continue = "page-2"
		case "page-2":
			page.Items = []corev1.PersistentVolumeClaim{*pvc("b", "two", "uid-2")}
		}
		return true, page, nil
	})

	uids, err := NewPersistentVolumeClaimClientFor(client).ListPersistentVolumeClaimUIDs(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"uid-1", "uid-2"}, uids)
	assert.Equal(t, []int64{persistentVolumePageSize, persistentVolumePageSize}, limits)
}

// A failed list is an error, never an empty answer, which would read as
// "every claim is gone".
func TestListPersistentVolumeClaimUIDsReportsAFailedList(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("list", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New(`persistentvolumeclaims is forbidden`)
	})

	uids, err := NewPersistentVolumeClaimClientFor(client).ListPersistentVolumeClaimUIDs(context.Background())

	require.Error(t, err)
	assert.Nil(t, uids)
	assert.Contains(t, err.Error(), "forbidden")
}
