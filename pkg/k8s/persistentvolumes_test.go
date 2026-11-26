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
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"qumulo-csi-driver/pkg/driver"
)

func csiPV(name, driverName, handle string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
			CSI: &corev1.CSIPersistentVolumeSource{Driver: driverName, VolumeHandle: handle},
		}},
	}
}

func TestListVolumeHandlesReturnsOnlyThisDriversCSIHandles(t *testing.T) {
	client := fake.NewClientset(
		csiPV("mine", "csi.qumulo.com", "a.example##/csi/v/pvc-1"),
		csiPV("other-driver", "ebs.csi.aws.com", "vol-123"),
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "nfs-in-tree"}, Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{NFS: &corev1.NFSVolumeSource{Server: "x", Path: "/y"}},
		}},
	)

	handles, err := NewPersistentVolumeClientFor(client).ListVolumeHandles(context.Background(), "csi.qumulo.com")

	require.NoError(t, err)
	assert.Equal(t, []string{"a.example##/csi/v/pvc-1"}, handles)
}

// ListVolumes needs each PV's StorageClass as well as its handle, to
// find the credential for a DR copy's PV on an array no class walks.
func TestListVolumeRefsCarriesTheStorageClass(t *testing.T) {
	pv := csiPV("dr-copy", "csi.qumulo.com", "b.example##/csi/v/pvc-1")
	pv.Spec.StorageClassName = "primary"
	client := fake.NewClientset(pv, csiPV("other-driver", "ebs.csi.aws.com", "vol-123"))

	refs, err := NewPersistentVolumeClientFor(client).ListVolumeRefs(context.Background(), "csi.qumulo.com")

	require.NoError(t, err)
	assert.Equal(t, []driver.VolumeRef{{Handle: "b.example##/csi/v/pvc-1", StorageClass: "primary"}}, refs)
}

// DeleteVolume's ownership check needs each PV's name, phase, claim and
// whether it is being deleted, to tell a copy still in use from one on its way
// out.
func TestListPersistentVolumesCarriesPhaseClaimAndDeletion(t *testing.T) {
	bound := csiPV("pvc-owner", "csi.qumulo.com", "a.example##/csi/v/pvc-owner")
	bound.Status.Phase = corev1.VolumeBound
	bound.Spec.ClaimRef = &corev1.ObjectReference{Namespace: "apps", Name: "owner"}
	copied := csiPV("copy", "csi.qumulo.com", "a.example##/csi/v/pvc-owner")
	copied.Status.Phase = corev1.VolumeReleased
	now := metav1.Now()
	copied.DeletionTimestamp = &now
	copied.Finalizers = []string{"external-provisioner.volume.kubernetes.io/finalizer"}
	unclaimed := csiPV("imported", "csi.qumulo.com", "a.example##/data")
	client := fake.NewClientset(bound, copied, unclaimed, csiPV("other-driver", "ebs.csi.aws.com", "vol-123"))

	pvs, err := NewPersistentVolumeClientFor(client).ListPersistentVolumes(context.Background(), "csi.qumulo.com")

	require.NoError(t, err)
	assert.ElementsMatch(t, []driver.PersistentVolumeInfo{
		{Name: "pvc-owner", Handle: "a.example##/csi/v/pvc-owner", Phase: "Bound", ClaimNamespace: "apps", ClaimName: "owner"},
		{Name: "copy", Handle: "a.example##/csi/v/pvc-owner", Phase: "Released", Deleting: true},
		{Name: "imported", Handle: "a.example##/data"},
	}, pvs)
}

// DeleteVolume's owner rule also needs each PV's reclaim policy and the
// two annotations that let a PV not named after its directory own it: the
// repoint tool's mirrored-from and the administrator's delete-directory.
// Values are passed through verbatim; DeleteVolume normalises them.
func TestListPersistentVolumesCarriesReclaimPolicyAndOwnerAnnotations(t *testing.T) {
	mirror := csiPV("pvc-1-dr", "csi.qumulo.com", "b.example##/csi/v/pvc-1")
	mirror.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	mirror.Annotations = map[string]string{"csi.qumulo.com/mirrored-from": "pvc-1", "unrelated": "x"}
	imported := csiPV("finance", "csi.qumulo.com", "a.example##/data/finance")
	imported.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	imported.Annotations = map[string]string{"csi.qumulo.com/delete-directory": "/data/finance/"}
	client := fake.NewClientset(mirror, imported, csiPV("plain", "csi.qumulo.com", "a.example##/csi/v/plain"))

	pvs, err := NewPersistentVolumeClientFor(client).ListPersistentVolumes(context.Background(), "csi.qumulo.com")

	require.NoError(t, err)
	assert.ElementsMatch(t, []driver.PersistentVolumeInfo{
		{Name: "pvc-1-dr", Handle: "b.example##/csi/v/pvc-1", ReclaimPolicy: "Retain", MirroredFrom: "pvc-1"},
		{Name: "finance", Handle: "a.example##/data/finance", ReclaimPolicy: "Delete", DeleteDirectory: "/data/finance/"},
		{Name: "plain", Handle: "a.example##/csi/v/plain"},
	}, pvs)
}

// A large cluster's PVs come back over several pages, and every page is read:
// a check that saw only the first 500 would miss the copy on page two.
func TestListPersistentVolumesFollowsContinueTokens(t *testing.T) {
	client := fake.NewClientset()
	var limits []int64
	client.PrependReactor("list", "persistentvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		opts := action.(k8stesting.ListActionImpl).GetListOptions()
		limits = append(limits, opts.Limit)
		page := &corev1.PersistentVolumeList{}
		switch opts.Continue {
		case "":
			page.Items = []corev1.PersistentVolume{*csiPV("one", "csi.qumulo.com", "a##/x/1")}
			page.Continue = "page-2"
		case "page-2":
			page.Items = []corev1.PersistentVolume{*csiPV("two", "csi.qumulo.com", "a##/x/2")}
		}
		return true, page, nil
	})

	pvs, err := NewPersistentVolumeClientFor(client).ListPersistentVolumes(context.Background(), "csi.qumulo.com")

	require.NoError(t, err)
	assert.Len(t, pvs, 2)
	assert.Equal(t, []int64{persistentVolumePageSize, persistentVolumePageSize}, limits)
}

// A failed list is an error, never an empty answer: an empty answer would
// read as "no other PV names this directory" and let the delete through.
func TestListPersistentVolumesReportsAFailedList(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("list", "persistentvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New(`persistentvolumes is forbidden`)
	})

	pvs, err := NewPersistentVolumeClientFor(client).ListPersistentVolumes(context.Background(), "csi.qumulo.com")

	require.Error(t, err)
	assert.Nil(t, pvs)
	assert.Contains(t, err.Error(), "forbidden")
}
