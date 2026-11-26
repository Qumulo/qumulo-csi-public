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

// StorageClassClient lists StorageClasses as the pod's ServiceAccount. It
// implements driver.StorageClassLister.
type StorageClassClient struct {
	client kubernetes.Interface
}

var _ driver.StorageClassLister = (*StorageClassClient)(nil)

// NewStorageClassClient builds a StorageClassClient from the pod's in-cluster
// credentials.
func NewStorageClassClient() (*StorageClassClient, error) {
	client, err := inClusterClientset()
	if err != nil {
		return nil, err
	}
	return &StorageClassClient{client: client}, nil
}

// NewStorageClassClientFor builds one over an existing clientset, for tests
// that use k8s.io/client-go/kubernetes/fake.
func NewStorageClassClientFor(client kubernetes.Interface) *StorageClassClient {
	return &StorageClassClient{client: client}
}

// ListStorageClasses returns every StorageClass whose provisioner matches.
//
// A List, never a Watch: Rule 4 in ARCHITECTURE.md reserves volume lifecycle
// watching to the official sidecars, and an informer here would be the driver
// reimplementing them. The cost is one API call per ListVolumes, over a
// resource a cluster holds a handful of.
//
// Paged rather than fetched whole. A cluster with many classes -- from other
// drivers as well as this one -- would otherwise be one large response, and
// the API server's default limit would silently truncate it, which for a
// volume listing means quietly omitting whole clusters.
func (c *StorageClassClient) ListStorageClasses(
	ctx context.Context,
	provisioner string,
) ([]driver.StorageClassInfo, error) {
	var (
		out           []driver.StorageClassInfo
		continueToken string
	)

	for {
		page, err := c.client.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{
			Limit:    storageClassPageSize,
			Continue: continueToken,
		})
		if err != nil {
			return nil, fmt.Errorf("listing StorageClasses: %w", err)
		}

		for _, class := range page.Items {
			if class.Provisioner != provisioner {
				continue
			}
			// Copied rather than aliased: page.Items is reused as the loop
			// advances, and the driver keeps these maps for the length of the
			// call.
			params := make(map[string]string, len(class.Parameters))
			for k, v := range class.Parameters {
				params[k] = v
			}
			out = append(out, driver.StorageClassInfo{
				Name:       class.Name,
				Parameters: params,
			})
		}

		continueToken = page.Continue
		if continueToken == "" {
			return out, nil
		}
	}
}

// storageClassPageSize bounds one List response.
//
// StorageClasses are small objects and a cluster has few, so this is about
// bounding a pathological case rather than an expected one.
const storageClassPageSize = 500
