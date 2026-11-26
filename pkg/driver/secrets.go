// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// SecretReader reads one Kubernetes Secret's data.
//
// It exists because GetCapacity is the one controller RPC whose request names
// the Secret holding a cluster's credential without carrying its contents:
// csi-provisioner passes the StorageClass's parameter map through untouched
// (pkg/capacity/capacity.go builds the request with `Parameters:
// sc.Parameters`), where for CreateVolume it strips the secret-reference keys
// and resolves them itself. So the reference arrives and the token does not.
//
// The interface is deliberately this narrow. It is one read-only Get of one
// resource kind, which is what keeps the Kubernetes dependency to a corner of
// the controller instead of spreading a clientset through the driver -- and it
// is what lets every GetCapacity test run against a map rather than a fake
// API server. Only the controller service is ever given one; a node instance's
// is nil, exactly as its Qumulo client is.
type SecretReader interface {
	// GetSecret returns the named Secret's data keys. It must return an error
	// satisfying IsSecretNotFound when no such Secret exists, so a class
	// pointing at a deleted Secret can be reported as configuration rather
	// than as a cluster fault.
	//
	// It must likewise return an error satisfying IsSecretForbidden when the
	// API server refuses the read. A refusal is configuration for the same
	// reason an absence is -- the release does not name that Secret -- and it
	// is just as permanent, so it must not be reported as a cluster fault
	// either.
	GetSecret(ctx context.Context, namespace, name string) (map[string][]byte, error)
}

// errSecretNotFound is returned by a SecretReader when the Secret is absent.
//
// Declared here rather than in the implementation so the driver can classify
// the error without importing the Kubernetes client packages, keeping
// client-go out of every package that merely handles the failure.
type errSecretNotFound struct {
	Namespace, Name string
}

func (e *errSecretNotFound) Error() string {
	return fmt.Sprintf("secret %s/%s not found", e.Namespace, e.Name)
}

// NewSecretNotFoundError builds the error a SecretReader must return for an
// absent Secret.
func NewSecretNotFoundError(namespace, name string) error {
	return &errSecretNotFound{Namespace: namespace, Name: name}
}

// IsSecretNotFound reports whether err says the Secret does not exist.
func IsSecretNotFound(err error) bool {
	var notFound *errSecretNotFound
	return errors.As(err, &notFound)
}

// errSecretForbidden is returned by a SecretReader when RBAC refuses the read.
//
// Declared beside errSecretNotFound, and for the same reason: the driver must
// be able to classify the failure without importing the Kubernetes client
// packages, since only the implementation can see what kind of API error it
// was.
//
// It is a separate sentinel rather than a flag on errSecretNotFound because
// the two say different things to an operator -- "you did not create it"
// versus "you did not grant it" -- and the remedy differs. What they share is
// that both are **permanent**: no retry clears either, and that is the
// property the inventory classifies on.
type errSecretForbidden struct {
	Namespace, Name string
}

func (e *errSecretForbidden) Error() string {
	return fmt.Sprintf(
		"secret %s/%s is not readable by this controller: add it to rbac.extraSecrets, "+
			"or set rbac.clusterWideSecrets=true",
		e.Namespace, e.Name)
}

// NewSecretForbiddenError builds the error a SecretReader must return when the
// API server refuses to let the controller read the Secret.
func NewSecretForbiddenError(namespace, name string) error {
	return &errSecretForbidden{Namespace: namespace, Name: name}
}

// IsSecretForbidden reports whether err says the controller may not read the
// Secret. Unlike a throttle or a timeout, this never clears on retry.
func IsSecretForbidden(err error) bool {
	var forbidden *errSecretForbidden
	return errors.As(err, &forbidden)
}

// Secret-reference keys the CO injects. These are the same keys the chart
// writes onto every StorageClass, and the same ones csi-provisioner consumes
// to build the secrets map for CreateVolume -- so a class that can provision
// can also report capacity, with no extra configuration.
const (
	coParamProvisionerSecretName      = coParamPrefix + "provisioner-secret-name"
	coParamProvisionerSecretNamespace = coParamPrefix + "provisioner-secret-namespace"
)

// secretRef names a Kubernetes Secret.
type secretRef struct {
	Namespace string
	Name      string
}

func (r secretRef) String() string { return r.Namespace + "/" + r.Name }

// secretRefFromParams extracts the provisioner Secret reference from a
// StorageClass parameter map.
//
// Both keys are required. Defaulting the namespace to the driver's own would
// read a Secret the administrator did not name, which for a credential is the
// wrong way to be helpful.
func secretRefFromParams(params map[string]string) (secretRef, error) {
	var ref secretRef
	for key, value := range params {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case coParamProvisionerSecretName:
			ref.Name = strings.TrimSpace(value)
		case coParamProvisionerSecretNamespace:
			ref.Namespace = strings.TrimSpace(value)
		}
	}

	var missing []string
	if ref.Name == "" {
		missing = append(missing, coParamProvisionerSecretName)
	}
	if ref.Namespace == "" {
		missing = append(missing, coParamProvisionerSecretNamespace)
	}
	if len(missing) > 0 {
		return secretRef{}, fmt.Errorf("the StorageClass sets no %s", strings.Join(missing, " and no "))
	}

	// csi-provisioner supports templates such as ${pvc.namespace} in these
	// values, and substitutes them per volume from the PVC being provisioned.
	// GetCapacity has no PVC, and receives the class's parameters verbatim, so
	// a templated reference arrives here unsubstituted. Reading a Secret
	// literally named "${pvc.namespace}" would fail with a confusing NotFound,
	// so the template is named as the cause instead.
	for _, value := range []string{ref.Name, ref.Namespace} {
		if strings.Contains(value, "${") {
			return secretRef{}, fmt.Errorf(
				"the StorageClass's provisioner Secret reference %q is templated; capacity "+
					"reporting has no PersistentVolumeClaim to expand a template against, so "+
					"this class needs a literal secret name and namespace to report capacity",
				value)
		}
	}

	return ref, nil
}

// tokenFromSecret pulls the Qumulo API token out of a Secret's data.
func tokenFromSecret(data map[string][]byte, ref secretRef) (string, error) {
	token := strings.TrimSpace(string(data[secretKeyToken]))
	if token == "" {
		keys := make([]string, 0, len(data))
		for key := range data {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		return "", fmt.Errorf("secret %s has no %q key (it has: %s)",
			ref, secretKeyToken, strings.Join(keys, ", "))
	}
	return token, nil
}
