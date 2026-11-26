// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"qumulo-csi-driver/pkg/driver"
)

// slowCreateArray answers a dedicated-export CreateVolume, slowly once the
// directory exists: each later create step takes createDelay. Undo calls (tree
// delete, quota and export delete) are answered at once.
type slowCreateArray struct {
	createDelay time.Duration

	mu      sync.Mutex
	dirs    map[string]string // name -> file ID
	quotas  map[string]bool   // file ID
	exports int
}

func (a *slowCreateArray) state() (dirs, quotas, exports int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.dirs), len(a.quotas), a.exports
}

func (a *slowCreateArray) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.EscapedPath()
	undo := r.Method == http.MethodDelete || strings.HasPrefix(p, "/v1/tree-delete/")
	dirs, _, _ := a.state()
	if dirs > 0 && !undo {
		select {
		case <-time.After(a.createDelay):
		case <-r.Context().Done():
			return // the caller went away: nothing lands
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case strings.HasSuffix(p, "/entries/") && r.Method == http.MethodPost:
		var body struct{ Name string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		if _, ok := a.dirs[body.Name]; ok {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error_class":"fs_entry_exists_error","description":"name already exists"}`))
			return
		}
		a.dirs[body.Name] = "101"
		_ = json.NewEncoder(w).Encode(map[string]string{"id": "101", "path": "/v/" + body.Name, "type": "FS_FILE_TYPE_DIRECTORY"})
	case strings.HasSuffix(p, "/info/attributes") && r.Method == http.MethodPatch:
		_, _ = w.Write([]byte(`{}`))
	case p == "/v1/files/quotas/" && r.Method == http.MethodPost:
		var body struct{ ID string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		a.quotas[body.ID] = true
		_, _ = w.Write([]byte(`{}`))
	case strings.HasPrefix(p, "/v1/files/quotas/") && r.Method == http.MethodDelete:
		delete(a.quotas, strings.TrimPrefix(p, "/v1/files/quotas/"))
		_, _ = w.Write([]byte(`{}`))
	case p == "/v1/tree-delete/jobs/" && r.Method == http.MethodPost:
		var body struct{ ID string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		for name, id := range a.dirs {
			if id == body.ID {
				delete(a.dirs, name)
			}
		}
		w.WriteHeader(http.StatusAccepted)
	case p == "/v3/nfs/exports/" && r.Method == http.MethodGet:
		_, _ = w.Write([]byte(`{"entries":[],"paging":{"next":""}}`))
	case p == "/v3/nfs/exports/" && r.Method == http.MethodPost:
		a.exports++
		_, _ = w.Write([]byte(`{"id":"1","export_path":"/v/pvc-drain","fs_path":"/v/pvc-drain"}`))
	case strings.HasPrefix(p, "/v3/nfs/exports/") && r.Method == http.MethodDelete:
		a.exports = 0
		_, _ = w.Write([]byte(`{}`))
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error_class":"fs_no_such_entry_error","description":"not modelled"}`))
	}
}

// A shutdown that cuts an in-flight CreateVolume short must leave nothing on the array, or the whole volume.
// /: the server stops with the handler's context cancelled, and the undo steps used to run
// on that cancelled context, leaving the directory behind.
func TestShutdownLeavesNoHalfMadeVolume(t *testing.T) {
	arr := &slowCreateArray{createDelay: 2 * time.Second, dirs: map[string]string{}, quotas: map[string]bool{}}
	// TLS: a class's apiUrl must be https://.
	api := httptest.NewTLSServer(arr)
	defer api.Close()
	httpClient := api.Client()
	httpClient.Timeout = 10 * time.Second

	d, err := driver.NewDriver(driver.Config{Name: "csi.qumulo.com", Version: "test", Mode: driver.ModeController,
		HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", filepath.Join(t.TempDir(), "csi.sock"))
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	csi.RegisterControllerServer(gs, d.ControllerServer())
	go func() { _ = gs.Serve(ln) }()

	conn, err := grpc.NewClient("unix://"+ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	go func() {
		_, _ = csi.NewControllerClient(conn).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name: "pvc-drain", Secrets: map[string]string{"token": "t"},
			VolumeCapabilities: []*csi.VolumeCapability{{
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}}},
			CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
			Parameters: map[string]string{"server": "192.0.2.10", "apiUrl": api.URL, "storeRealPath": "/v",
				"storeExportPath": "/v", "provisioningModel": "dedicated-export", "allowAnyClient": "true"},
		})
	}()
	for deadline := time.Now().Add(10 * time.Second); ; {
		if dirs, _, _ := arr.state(); dirs > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("CreateVolume never created its directory")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// SIGTERM, with a grace period shorter than the create has left.
	drained := shutdown(gs, time.Second, nil, 0)
	t.Logf("shutdown drained=%v", drained)

	// Undo on the array is immediate; give any still running a moment.
	var dirs, quotas, exports int
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		dirs, quotas, exports = arr.state()
		if dirs == 0 || (quotas == 1 && exports == 1) {
			break
		}
	}
	t.Logf("left on the array: directories=%d quotas=%d exports=%d", dirs, quotas, exports)
	if dirs != 0 && (quotas != 1 || exports != 1) {
		t.Errorf("the interrupted create left a half-made volume: directories=%d quotas=%d exports=%d; "+
			"want nothing, or the whole volume", dirs, quotas, exports)
	}
}
