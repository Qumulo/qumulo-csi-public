// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package k8s

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"qumulo-csi-driver/pkg/driver"
)

// PersistentVolumeClient lists this driver's PersistentVolume handles as the
// pod's ServiceAccount. It implements driver.VolumeHandleLister.
type PersistentVolumeClient struct {
	client kubernetes.Interface
}

var _ driver.VolumeHandleLister = (*PersistentVolumeClient)(nil)

// NewPersistentVolumeClient builds a PersistentVolumeClient from the pod's
// in-cluster credentials.
func NewPersistentVolumeClient() (*PersistentVolumeClient, error) {
	client, err := inClusterClientset()
	if err != nil {
		return nil, err
	}
	return &PersistentVolumeClient{client: client}, nil
}

// NewPersistentVolumeClientFor builds one over an existing clientset, for
// tests that use k8s.io/client-go/kubernetes/fake.
func NewPersistentVolumeClientFor(client kubernetes.Interface) *PersistentVolumeClient {
	return &PersistentVolumeClient{client: client}
}

// ListVolumeHandles returns the volume handle of every PersistentVolume whose
// CSI driver is driverName.
//
// A paged List, never a Watch, for the same reasons as ListStorageClasses:
// Rule 4 in ARCHITECTURE.md, and a default server-side limit that would otherwise
// truncate a large cluster's answer silently.
func (c *PersistentVolumeClient) ListVolumeHandles(ctx context.Context, driverName string) ([]string, error) {
	refs, err := c.ListVolumeRefs(ctx, driverName)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ref.Handle)
	}
	return out, nil
}

var _ driver.VolumeRefLister = (*PersistentVolumeClient)(nil)

// ListVolumeRefs is ListVolumeHandles with each PV's StorageClass name.
func (c *PersistentVolumeClient) ListVolumeRefs(ctx context.Context, driverName string) ([]driver.VolumeRef, error) {
	var out []driver.VolumeRef
	err := c.eachPersistentVolume(ctx, driverName, func(pv *corev1.PersistentVolume) {
		out = append(out, driver.VolumeRef{Handle: pv.Spec.CSI.VolumeHandle, StorageClass: pv.Spec.StorageClassName})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _ driver.PersistentVolumeLister = (*PersistentVolumeClient)(nil)

// ListPersistentVolumes returns this driver's PVs with what DeleteVolume's
// ownership checks read: name, handle, phase, claim, reclaim policy, whether
// the PV is being deleted, and the two annotations that let a PV not named
// after its directory own it. The same paged List as ListVolumeRefs:
// one per DeleteVolume that finds a directory to remove, 500 PVs a page.
func (c *PersistentVolumeClient) ListPersistentVolumes(ctx context.Context, driverName string) ([]driver.PersistentVolumeInfo, error) {
	var out []driver.PersistentVolumeInfo
	err := c.eachPersistentVolume(ctx, driverName, func(pv *corev1.PersistentVolume) {
		info := driver.PersistentVolumeInfo{
			Name:     pv.Name,
			Handle:   pv.Spec.CSI.VolumeHandle,
			Phase:    string(pv.Status.Phase),
			Deleting: pv.DeletionTimestamp != nil,

			ReclaimPolicy:   string(pv.Spec.PersistentVolumeReclaimPolicy),
			MirroredFrom:    pv.Annotations[driver.AnnotationMirroredFrom],
			DeleteDirectory: pv.Annotations[driver.AnnotationDeleteDirectory],
		}
		if ref := pv.Spec.ClaimRef; ref != nil {
			info.ClaimNamespace, info.ClaimName = ref.Namespace, ref.Name
		}
		if storage, ok := pv.Spec.Capacity[corev1.ResourceStorage]; ok {
			info.CapacityBytes, _ = storage.AsInt64()
		}
		out = append(out, info)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// eachPersistentVolume calls visit for every PersistentVolume whose CSI
// driver is driverName and whose handle is set, reading every page. A failed
// page fails the whole list: a partial answer would read as "no other PV",
// which is the one answer DeleteVolume must never get by accident.
func (c *PersistentVolumeClient) eachPersistentVolume(
	ctx context.Context, driverName string, visit func(*corev1.PersistentVolume),
) error {
	var continueToken string
	for {
		page, err := c.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{
			Limit:    persistentVolumePageSize,
			Continue: continueToken,
		})
		if err != nil {
			return fmt.Errorf("listing PersistentVolumes: %w", err)
		}
		for i := range page.Items {
			pv := &page.Items[i]
			if csi := pv.Spec.CSI; csi != nil && csi.Driver == driverName && csi.VolumeHandle != "" {
				visit(pv)
			}
		}
		continueToken = page.Continue
		if continueToken == "" {
			return nil
		}
	}
}

// persistentVolumePageSize bounds one List response. A PV is a small object,
// and a large cluster holds thousands.
const persistentVolumePageSize = 500
