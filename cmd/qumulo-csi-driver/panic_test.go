// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/driver"
	"qumulo-csi-driver/pkg/metrics"
	"qumulo-csi-driver/pkg/version"
)

// childSocketEnv, when set, makes this test binary serve the driver on that
// socket through run() instead of running the test, so the parent can watch
// what a panic in a handler does to the process.
const childSocketEnv = "QUMULO_CSI_TEST_CHILD_SOCKET"

func TestMain(m *testing.M) {
	if sock := os.Getenv(childSocketEnv); sock != "" {
		err := runUntilSignalled(config{
			endpoint:       "unix://" + sock,
			driverName:     "csi.qumulo.com",
			mode:           "controller",
			replication:    true,
			insecureTLS:    true,
			requestTimeout: 5 * time.Second,
			unmountTimeout: 30 * time.Second,
			logLevel:       "error",
		}, productionDeps())
		if err != nil {
			os.Stderr.WriteString("run: " + err.Error() + "\n")
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// replicationArray is one end of a replication relationship, answering just
// what ResyncVolume reads before it flips the volume's export read-only.
// Its export's restrictions list holds a JSON null.
type replicationArray struct {
	source bool

	mu      sync.Mutex
	unknown []string
}

func (a *replicationArray) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.EscapedPath()
	w.Header().Set("Content-Type", "application/json")
	reply := func(body string) { _, _ = w.Write([]byte(body)) }
	switch {
	case a.source && r.Method == http.MethodGet && p == "/v1/files/%2Fcsi%2Fvolumes%2Fpvc-r/info/attributes":
		reply(`{"id":"11","path":"/csi/volumes/pvc-r","type":"FS_FILE_TYPE_DIRECTORY"}`)
	case a.source && r.Method == http.MethodGet && p == "/v3/nfs/exports/":
		reply(`{"entries":[{"id":"5","export_path":"/csi/volumes/pvc-r","fs_path":"/csi/volumes/pvc-r",` +
			`"restrictions":[{"read_only":false,"host_restrictions":[]}]}],"paging":{"next":""}}`)
	case a.source && r.Method == http.MethodGet && p == "/v3/nfs/exports/5":
		reply(`{"id":"5","export_path":"/csi/volumes/pvc-r","fs_path":"/csi/volumes/pvc-r","description":"",` +
			`"restrictions":[null]}`)
	case a.source && p == "/v2/replication/source-relationships/status/":
		reply(`[{"id":"rel-1","state":"DISCONNECTED","source_root_path":"/csi/volumes/pvc-r","source_root_id":"11",` +
			`"source_root_read_only":true,"target_address":"192.0.2.20","target_root_path":"/replica/pvc-r"}]`)
	case !a.source && p == "/v2/replication/target-relationships/status/":
		reply(`[{"id":"rel-1","state":"DISCONNECTED","target_root_path":"/replica/pvc-r","target_root_id":"21",` +
			`"target_root_read_only":false,"source_address":"192.0.2.10","source_root_path":"/csi/volumes/pvc-r"}]`)
	case strings.HasSuffix(p, "-relationships/status/"):
		reply(`[]`)
	default:
		a.mu.Lock()
		a.unknown = append(a.unknown, r.Method+" "+p)
		a.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		reply(`{"error_class":"fs_no_such_entry_error","description":"not modelled"}`)
	}
}

// An export whose restrictions list holds a JSON null, met by ResyncVolume
// when it flips the export read-only, is answered as an error naming the
// null restriction, and the controller process goes on serving. This was
// 's reproduction: SetExportReadOnly dereferenced the null and the
// panic took the process down. It no longer panics at all -- the client
// refuses the reply -- so this guards that error path through the real
// run(), not the recovery interceptor; TestServedChainRecoversAndCountsAPanic
// is the one that makes a handler really panic.
func TestResyncVolumeOnANullExportRestrictionIsAnErrorAndTheControllerKeepsServing(t *testing.T) {
	local := &replicationArray{source: true}
	peer := &replicationArray{}
	localSrv := httptest.NewTLSServer(local)
	defer localSrv.Close()
	peerSrv := httptest.NewTLSServer(peer)
	defer peerSrv.Close()

	sock := filepath.Join(t.TempDir(), "csi.sock")
	child := exec.Command(os.Args[0], "-test.run=^$")
	child.Env = append(os.Environ(), childSocketEnv+"="+sock)
	var out bytes.Buffer
	child.Stdout, child.Stderr = &out, &out
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	var exitErr error
	go func() { exitErr = child.Wait(); close(exited) }()
	defer func() {
		_ = child.Process.Kill()
		<-exited
	}()

	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the driver never listened on %s:\n%s", sock, out.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	conn, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, resyncErr := replication.NewControllerClient(conn).ResyncVolume(ctx, &replication.ResyncVolumeRequest{
		VolumeId: "192.0.2.10#" + localSrv.URL + "#/csi/volumes/pvc-r",
		Parameters: map[string]string{
			"apiUrl": localSrv.URL, "targetServer": "192.0.2.20", "targetApiUrl": peerSrv.URL,
			"targetBasePath": "/replica", "replicationDeletePolicy": "disable",
		},
		Secrets: map[string]string{"token": "t", "127.0.0.1": "t", "192.0.2.20": "t"},
		Force:   true,
	})
	t.Logf("ResyncVolume answered: %v", resyncErr)
	local.mu.Lock()
	peer.mu.Lock()
	t.Logf("calls the fakes did not model: local %v, peer %v", local.unknown, peer.unknown)
	peer.mu.Unlock()
	local.mu.Unlock()

	probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer probeCancel()
	_, probeErr := csi.NewIdentityClient(conn).Probe(probeCtx, &csi.ProbeRequest{})

	// A process that died on the panic is reaped within moments of Probe
	// failing; one that is merely slow is not waited for.
	wait := time.After(0)
	if probeErr != nil {
		wait = time.After(3 * time.Second)
	}
	select {
	case <-exited:
		text := out.String()
		if i := strings.Index(text, "panic: "); i >= 0 {
			text = text[i:min(len(text), i+1500)]
		}
		t.Fatalf("the controller process exited (%v) on one RPC's panic:\n%s", exitErr, text)
	case <-wait:
	}
	if probeErr != nil {
		t.Fatalf("the controller stopped answering after one RPC's panic: Probe: %v", probeErr)
	}
	if resyncErr == nil {
		t.Fatal("ResyncVolume succeeded; want the null restriction reported as an error")
	}
	if !strings.Contains(resyncErr.Error(), "restriction 0 is null") {
		t.Errorf("ResyncVolume answered %v; want the error to name the null restriction", resyncErr)
	}
	if strings.Contains(resyncErr.Error(), "recovered panic") {
		t.Errorf("ResyncVolume panicked and was recovered (%v); want an error without a panic", resyncErr)
	}
}

// panickingIdentity is an Identity service whose Probe really panics.
type panickingIdentity struct {
	csi.UnimplementedIdentityServer
}

func (panickingIdentity) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	panic("probe exploded")
}

func (panickingIdentity) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: "csi.qumulo.com", VendorVersion: "test"}, nil
}

// A handler that really panics, served through the server run() builds
// (newGRPCServer, the whole interceptor chain) over a real unix socket,
// answers Internal, is logged and counted, and the server goes on serving.
// TestRecoverRPCAnswersInternalForAPanickingHandler tests the interceptor
// alone; this one fails if run()'s chain stops including it -- the panic then
// escapes the handler goroutine and takes this test binary down, which is
// exactly what it did to the controller.
func TestServedChainRecoversAndCountsAPanic(t *testing.T) {
	var logs bytes.Buffer
	var logsMu sync.Mutex
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(lockedWriter{&logs, &logsMu}, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	registry := metrics.New(version.Info{})
	srv := newGRPCServer(registry)
	csi.RegisterIdentityServer(srv, panickingIdentity{})
	sock := filepath.Join(t.TempDir(), "csi.sock")
	lis, err := driver.Listen("unix://" + sock)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := csi.NewIdentityClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 2; i++ {
		_, err := client.Probe(ctx, &csi.ProbeRequest{})
		if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "recovered panic: probe exploded") {
			t.Fatalf("Probe #%d answered %v; want Internal naming the recovered panic", i+1, err)
		}
	}
	// The same server still answers a handler that does not panic.
	if _, err := client.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{}); err != nil {
		t.Fatalf("after two panics GetPluginInfo answered %v; want the server still serving", err)
	}

	if err := testutil.GatherAndCompare(registry.Gatherer(), strings.NewReader(`
# HELP csi_qumulo_rpc_panics_total RPC handlers that panicked, by full gRPC method. Each was recovered and answered Internal; any increase is a driver bug.
# TYPE csi_qumulo_rpc_panics_total counter
csi_qumulo_rpc_panics_total{method="/csi.v1.Identity/Probe"} 2
`), "csi_qumulo_rpc_panics_total"); err != nil {
		t.Error(err)
	}
	logsMu.Lock()
	text := logs.String()
	logsMu.Unlock()
	if n := strings.Count(text, "rpc panicked"); n != 2 {
		t.Errorf("logged %d panics, want 2:\n%s", n, text)
	}
	if !strings.Contains(text, "panic_test.go") {
		t.Errorf("the panic log carries no stack pointing at the handler:\n%s", text)
	}
}

// lockedWriter serialises the log handler's writes with the test's read.
type lockedWriter struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
