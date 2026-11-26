//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// dfxExportRestriction is one entry of an export's restrictions, as the
// driver sends it to POST /v3/nfs/exports/.
type dfxExportRestriction struct {
	HostRestrictions      []string `json:"host_restrictions"`
	RequirePrivilegedPort bool     `json:"require_privileged_port"`
	UserMapping           string   `json:"user_mapping"`
	AuthMode              string   `json:"required_authentication_mode"`
}

// A dedicated export made for a class that
// sets no allowed clients must not admit any NFS client as root: the driver
// must refuse the class, or restrict the export's hosts, or squash root.
// pkg/qumulo/client.go defaultExportRestriction. Fix: refuse unless the class opts in.
func TestDefect_DedicatedExportDoesNotAdmitAnyClientAsRootByDefault(t *testing.T) {
	cluster := newFakeCluster()
	var mu sync.Mutex
	var posted [][]dfxExportRestriction
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.EscapedPath() == "/v3/nfs/exports/" {
			body, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			var req struct {
				Restrictions []dfxExportRestriction `json:"restrictions"`
			}
			_ = json.Unmarshal(body, &req)
			mu.Lock()
			posted = append(posted, req.Restrictions)
			mu.Unlock()
		}
		mu.Lock()
		defer mu.Unlock()
		cluster.handler()(w, r)
	}))
	defer server.Close()

	_, err := newIntegrationControllerServer(t, server.URL).CreateVolume(context.Background(),
		dfxDedicatedExportCreate("pvc-open"))
	mu.Lock()
	defer mu.Unlock()
	t.Logf("CreateVolume answered %v; exports posted with restrictions %+v", err, posted)

	if err != nil {
		if c := status.Code(err); c != codes.InvalidArgument && c != codes.FailedPrecondition {
			t.Errorf("CreateVolume of an open dedicated class answered %v; a refusal should be "+
				"InvalidArgument or FailedPrecondition", c)
		}
		if len(posted) != 0 {
			t.Errorf("the class was refused, but an export was still created: %+v", posted)
		}
		return
	}
	if len(posted) == 0 {
		t.Fatal("CreateVolume succeeded for a dedicated-export class without creating an export")
	}
	for _, restrictions := range posted {
		if len(restrictions) == 0 {
			t.Errorf("an export was created with no restriction entry at all")
		}
		for _, r := range restrictions {
			anyHost := len(r.HostRestrictions) == 0
			rootAsRoot := r.UserMapping == "" || r.UserMapping == "NFS_MAP_NONE"
			if anyHost && rootAsRoot {
				t.Errorf("the dedicated export admits any client (host_restrictions %v) as root (user_mapping %q), "+
					"from any port (require_privileged_port %v), with authentication %q",
					r.HostRestrictions, r.UserMapping, r.RequirePrivilegedPort, r.AuthMode)
			}
		}
	}
}
