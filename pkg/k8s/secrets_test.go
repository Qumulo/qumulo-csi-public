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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"qumulo-csi-driver/pkg/driver"
)

// This is the translation the ListVolumes skip turns on. The driver cannot ask client-go what
// kind of API error it was -- it deliberately does not import it -- so only
// this package can tell a refusal from a timeout, and the driver classifies
// what it is handed. Translating only IsNotFound left every class naming a
// Secret this release does not list reported as an outage of the Kubernetes
// API, which failed every ListVolumes in the cluster.

func TestGetSecretReturnsTheData(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "tok", Namespace: "qumulo-csi"},
		Data:       map[string][]byte{"token": []byte("t")},
	})

	data, err := NewSecretClientFor(client).GetSecret(context.Background(), "qumulo-csi", "tok")

	require.NoError(t, err)
	assert.Equal(t, []byte("t"), data["token"])
}

func TestGetSecretTranslatesNotFound(t *testing.T) {
	client := fake.NewSimpleClientset()

	_, err := NewSecretClientFor(client).GetSecret(context.Background(), "qumulo-csi", "gone")

	require.Error(t, err)
	assert.True(t, driver.IsSecretNotFound(err), "a missing Secret is configuration")
	assert.False(t, driver.IsSecretForbidden(err))
}

// TestGetSecretTranslatesForbidden is the one the ListVolumes skip needs. Under the Role
// the API server authorizes before it looks an object up, so for any Secret
// the release does not name this is also what a *missing* one answers.
func TestGetSecretTranslatesForbidden(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "secrets",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Resource: "secrets"}, "unnamed", errors.New("RBAC"))
		})

	_, err := NewSecretClientFor(client).GetSecret(context.Background(), "qumulo-csi", "unnamed")

	require.Error(t, err)
	assert.True(t, driver.IsSecretForbidden(err), "a refusal is configuration, and permanent")
	assert.False(t, driver.IsSecretNotFound(err))
	assert.Contains(t, err.Error(), "rbac.extraSecrets", "the error must carry the remedy")
}

// TestGetSecretPassesThroughATransientError: everything else must stay raw, so
// the driver still treats it as the API-server fault it is and retries.
func TestGetSecretPassesThroughATransientError(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "secrets",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewTooManyRequestsError("slow down")
		})

	_, err := NewSecretClientFor(client).GetSecret(context.Background(), "qumulo-csi", "tok")

	require.Error(t, err)
	assert.False(t, driver.IsSecretForbidden(err))
	assert.False(t, driver.IsSecretNotFound(err))
}
