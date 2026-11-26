//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A real array answers a finished or unknown
// tree-delete job with 404 "http_not_found_error". The mock answered a class
// no array sends, "no_such_job_error", so a client narrowed to the mock's
// class would pass csi-sanity and then poll every real DeleteVolume until it
// timed out.
func TestDefect_AnUnknownTreeDeleteJobAnswersTheRealArraysClass(t *testing.T) {
	server := httptest.NewServer(newCluster("Qumulo Core test", 0, true, 0))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/v1/tree-delete/jobs/12345")
	require.NoError(t, err)
	defer resp.Body.Close()

	var body struct {
		ErrorClass string `json:"error_class"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	t.Logf("HTTP %d, error_class %q", resp.StatusCode, body.ErrorClass)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "http_not_found_error", body.ErrorClass)
}
