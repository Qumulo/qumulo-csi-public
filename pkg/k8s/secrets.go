// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package k8s holds the driver's only contact with the Kubernetes API.
//
// It exists to read one thing: the Secret a StorageClass names, so GetCapacity
// can authenticate to the cluster that class points at. Nothing here watches
// anything, which is the architectural rule this package must not break --
// volume lifecycle hooks belong to the official sidecars, and a driver that
// grew an informer would be reimplementing them.
//
// It is a separate package so that client-go's imports stay out of pkg/driver.
// The driver depends on the narrow driver.SecretReader interface instead, which
// is what lets every GetCapacity test run against a map.
package k8s

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"qumulo-csi-driver/pkg/driver"
)

// SecretClient reads Secrets from the Kubernetes API as the pod's
// ServiceAccount. It implements driver.SecretReader.
type SecretClient struct {
	client kubernetes.Interface
}

// Compile-time proof that this satisfies what the driver asks for, so a change
// to either side fails here rather than at wiring time in main.
var _ driver.SecretReader = (*SecretClient)(nil)

// NewSecretClient builds a SecretClient from the pod's in-cluster credentials.
//
// It returns an error when the process is not running in a pod, which is the
// normal case for csi-sanity and local development. The caller treats that as
// "capacity reporting is unavailable" and starts anyway, rather than refusing
// to serve: every other RPC works without Kubernetes access, so failing to
// start would turn an optional feature into an outage.
func NewSecretClient() (*SecretClient, error) {
	client, err := inClusterClientset()
	if err != nil {
		return nil, err
	}
	return &SecretClient{client: client}, nil
}

// inClusterClientset builds the clientset every reader in this package shares.
//
// Shared so there is one place that decides how the driver authenticates to
// the API, and one place to look when it stops working. The default
// rest.Config already re-reads the projected ServiceAccount token from disk as
// it rotates, so a client built here keeps working past the token's expiry
// without a restart -- the same reason the Qumulo token is read per request
// rather than captured once.
func inClusterClientset() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("building in-cluster Kubernetes config: %w", err)
	}
	withClientLimits(config)

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("building Kubernetes client: %w", err)
	}
	return client, nil
}

// clientQPS and clientBurst are the driver's Kubernetes client rate limits,
// at the 20 QPS the aws-ebs-csi-driver chart gives its sidecars. Left unset,
// client-go throttles to 5 a second with a burst of 10, and one ListVolumes
// page -- a StorageClass list, a Secret per class, a PersistentVolume list --
// spent 4 to 6 s waiting on its own client.
const (
	clientQPS   = 20
	clientBurst = 50
)

// withClientLimits sets the rate limits on config.
func withClientLimits(config *rest.Config) {
	config.QPS = clientQPS
	config.Burst = clientBurst
}

// NewSecretClientFor builds a SecretClient over an existing clientset, for
// tests that use k8s.io/client-go/kubernetes/fake.
func NewSecretClientFor(client kubernetes.Interface) *SecretClient {
	return &SecretClient{client: client}
}

// GetSecret returns the named Secret's data.
//
// An absent Secret is translated into the driver's own not-found error, and a
// refused one into its forbidden error, so the controller can tell a
// misconfigured StorageClass from an API failure without importing client-go
// to do it. Everything else is passed through raw and read as a fault of the
// API server itself.
//
// Both translations are needed, and the second is not a special case of the
// first. The controller holds `get` on only the Secrets the release names,
// through a Role with resourceNames -- and the API server authorizes
// *before* it looks an object up. So for any Secret the release does not name,
// a missing one and a permitted-to-nobody one arrive identically as Forbidden,
// and translating only IsNotFound left every such class reported as an outage
// of the Kubernetes API.
func (c *SecretClient) GetSecret(
	ctx context.Context,
	namespace, name string,
) (map[string][]byte, error) {
	secret, err := c.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, driver.NewSecretNotFoundError(namespace, name)
		}
		if apierrors.IsForbidden(err) {
			return nil, driver.NewSecretForbiddenError(namespace, name)
		}
		return nil, err
	}
	return secret.Data, nil
}
