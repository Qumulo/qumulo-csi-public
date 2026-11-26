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
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"qumulo-csi-driver/pkg/driver"
)

func storageClass(name, provisioner string, params map[string]string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: provisioner,
		Parameters:  params,
	}
}

// Only this driver's classes are returned, each with its name and parameters.
func TestListStorageClassesReturnsOnlyThisProvisionersClasses(t *testing.T) {
	client := fake.NewClientset(
		storageClass("qumulo-a", "csi.qumulo.com", map[string]string{"server": "a.example.com"}),
		storageClass("ebs", "ebs.csi.aws.com", map[string]string{"type": "gp3"}),
		storageClass("qumulo-b", "csi.qumulo.com", nil),
	)

	classes, err := NewStorageClassClientFor(client).ListStorageClasses(context.Background(), "csi.qumulo.com")

	require.NoError(t, err)
	assert.ElementsMatch(t, []driver.StorageClassInfo{
		{Name: "qumulo-a", Parameters: map[string]string{"server": "a.example.com"}},
		{Name: "qumulo-b", Parameters: map[string]string{}},
	}, classes)
}

// The parameters handed back are a copy: changing them does not reach the
// object the list returned.
func TestListStorageClassesCopiesParameters(t *testing.T) {
	params := map[string]string{"server": "a.example.com"}
	client := fake.NewClientset()
	client.PrependReactor("list", "storageclasses", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &storagev1.StorageClassList{Items: []storagev1.StorageClass{
			*storageClass("qumulo-a", "csi.qumulo.com", params),
		}}, nil
	})

	classes, err := NewStorageClassClientFor(client).ListStorageClasses(context.Background(), "csi.qumulo.com")
	require.NoError(t, err)
	require.Len(t, classes, 1)
	classes[0].Parameters["server"] = "changed"

	assert.Equal(t, "a.example.com", params["server"])
}

// Paged: every page is requested with the page size, and a class on a later
// page is not dropped.
func TestListStorageClassesFollowsContinueTokens(t *testing.T) {
	client := fake.NewClientset()
	var limits []int64
	client.PrependReactor("list", "storageclasses", func(action k8stesting.Action) (bool, runtime.Object, error) {
		opts := action.(k8stesting.ListActionImpl).GetListOptions()
		limits = append(limits, opts.Limit)
		page := &storagev1.StorageClassList{}
		switch opts.Continue {
		case "":
			page.Items = []storagev1.StorageClass{*storageClass("one", "csi.qumulo.com", nil)}
			page.Continue = "page-2"
		case "page-2":
			page.Items = []storagev1.StorageClass{*storageClass("two", "csi.qumulo.com", nil)}
		}
		return true, page, nil
	})

	classes, err := NewStorageClassClientFor(client).ListStorageClasses(context.Background(), "csi.qumulo.com")

	require.NoError(t, err)
	require.Len(t, classes, 2)
	assert.Equal(t, "two", classes[1].Name)
	assert.Equal(t, []int64{storageClassPageSize, storageClassPageSize}, limits)
}

// A failed list is an error, never an empty answer, which ListVolumes would
// read as "this driver has no classes" and list no volumes.
func TestListStorageClassesReportsAFailedList(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("list", "storageclasses", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("storageclasses.storage.k8s.io is forbidden")
	})

	classes, err := NewStorageClassClientFor(client).ListStorageClasses(context.Background(), "csi.qumulo.com")

	require.Error(t, err)
	assert.Nil(t, classes)
	assert.Contains(t, err.Error(), "forbidden")
}

// Outside a pod every in-cluster constructor answers an error and no client,
// which main reads as "this feature is unavailable" and starts anyway. The
// success path needs a real ServiceAccount token at its fixed path, so it is
// left to a cluster.
func TestInClusterConstructorsFailOutsideAPod(t *testing.T) {
	// rest.InClusterConfig reports ErrNotInCluster when either is empty,
	// whatever the files on this machine.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	for name, construct := range map[string]func() (any, error){
		"NewSecretClient":                func() (any, error) { return NewSecretClient() },
		"NewStorageClassClient":          func() (any, error) { return NewStorageClassClient() },
		"NewPersistentVolumeClient":      func() (any, error) { return NewPersistentVolumeClient() },
		"NewPersistentVolumeClaimClient": func() (any, error) { return NewPersistentVolumeClaimClient() },
	} {
		t.Run(name, func(t *testing.T) {
			client, err := construct()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "in-cluster Kubernetes config")
			assert.Nil(t, client)
		})
	}
}
