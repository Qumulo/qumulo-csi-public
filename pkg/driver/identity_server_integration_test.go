// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// Probe must not touch the cluster at all.
//
// It used to call HealthCheck, which made a Ready controller look like a
// reachable, authenticated cluster -- it was neither. With
// many clusters per controller there is also no single cluster to check. This
// asserts the absence of the call by failing if the server is ever hit.
func TestProbeDoesNotContactTheCluster(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		Server:  "qumulo.example.com",
		Client:  qumulo.New(server.URL, ""),
	})
	require.NoError(t, err)

	resp, err := NewIdentityServer(d).Probe(context.Background(), &csi.ProbeRequest{})

	require.NoError(t, err, "Probe must succeed even when the cluster would answer 401")
	assert.True(t, resp.GetReady().GetValue())
	assert.Zero(t, atomic.LoadInt32(&hits), "Probe made a request to the cluster")
}
