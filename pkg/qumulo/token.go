// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"fmt"
	"os"
	"strings"
)

// TokenFromFile returns a token source that re-reads path on every call.
//
// Kubernetes updates a mounted Secret in place, so reading per request is
// what lets a rotated token take effect without restarting the pod. The
// read is a few microseconds against the kubelet's tmpfs mount, which is
// negligible next to the REST round trip it precedes.
func TokenFromFile(path string) func() (string, error) {
	return func() (string, error) {
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("reading qumulo api token from %s: %w", path, err)
		}

		// Secrets written by hand routinely carry a trailing newline, which
		// would otherwise be sent as part of the header value.
		token := strings.TrimSpace(string(raw))
		if token == "" {
			return "", fmt.Errorf("qumulo api token file %s is empty", path)
		}
		return token, nil
	}
}
