// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package k8s

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"qumulo-csi-driver/pkg/driver"
)

// PersistentVolumeClaimClient lists the cluster's PersistentVolumeClaim UIDs
// as the pod's ServiceAccount. It implements
// driver.PersistentVolumeClaimLister.
type PersistentVolumeClaimClient struct {
	client kubernetes.Interface
}

var _ driver.PersistentVolumeClaimLister = (*PersistentVolumeClaimClient)(nil)

// NewPersistentVolumeClaimClient builds a PersistentVolumeClaimClient from
// the pod's in-cluster credentials.
func NewPersistentVolumeClaimClient() (*PersistentVolumeClaimClient, error) {
	client, err := inClusterClientset()
	if err != nil {
		return nil, err
	}
	return &PersistentVolumeClaimClient{client: client}, nil
}

// NewPersistentVolumeClaimClientFor builds one over an existing clientset,
// for tests that use k8s.io/client-go/kubernetes/fake.
func NewPersistentVolumeClaimClientFor(client kubernetes.Interface) *PersistentVolumeClaimClient {
	return &PersistentVolumeClaimClient{client: client}
}

// ListPersistentVolumeClaimUIDs returns the UID of every claim in every
// namespace. A restore's staging directory is named after its
// claim's UID, and a UID cannot be fetched by Get, so this lists.
//
// A paged List, never a Watch, as for PersistentVolumes: Rule 4 in
// ARCHITECTURE.md, and a server-side limit that would otherwise truncate a large
// cluster's answer silently. A failed page fails the whole list: a partial
// answer would read as "this claim is gone", which is the one answer that
// removes a restore still running.
func (c *PersistentVolumeClaimClient) ListPersistentVolumeClaimUIDs(ctx context.Context) ([]string, error) {
	var out []string
	var continueToken string
	for {
		page, err := c.client.CoreV1().PersistentVolumeClaims(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
			Limit:    persistentVolumePageSize,
			Continue: continueToken,
		})
		if err != nil {
			return nil, fmt.Errorf("listing PersistentVolumeClaims: %w", err)
		}
		for i := range page.Items {
			out = append(out, string(page.Items[i].UID))
		}
		continueToken = page.Continue
		if continueToken == "" {
			return out, nil
		}
	}
}
