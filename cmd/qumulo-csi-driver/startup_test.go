// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2"

	"qumulo-csi-driver/pkg/k8s"
	"qumulo-csi-driver/pkg/metrics"
	"qumulo-csi-driver/pkg/version"
)

// syncBuffer is a log sink safe to write from run's goroutines while a test
// reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// keepDefaultLogger restores the process-wide logger setupLogging replaces.
func keepDefaultLogger(t *testing.T) {
	t.Helper()
	saved := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(saved)
		klog.ClearLogger()
	})
}

// shortSocket returns a unix endpoint under a short directory: t.TempDir()
// names can pass the 108-byte limit on a socket path.
func shortSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "csi")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return "unix://" + filepath.Join(dir, "csi.sock")
}

var errNoCluster = errors.New("not running in a cluster")

// testDeps is productionDeps with the Kubernetes API either a fake clientset
// or unreachable, and the log going to logs.
func testDeps(clientset *fake.Clientset, logs *syncBuffer) startupDeps {
	deps := startupDeps{
		hostname:     func() (string, error) { return "test-host", nil },
		secretClient: func() (*k8s.SecretClient, error) { return nil, errNoCluster },
		classClient:  func() (*k8s.StorageClassClient, error) { return nil, errNoCluster },
		volumeClient: func() (*k8s.PersistentVolumeClient, error) { return nil, errNoCluster },
		claimClient:  func() (*k8s.PersistentVolumeClaimClient, error) { return nil, errNoCluster },
		logOutput:    logs,
	}
	if clientset != nil {
		deps.secretClient = func() (*k8s.SecretClient, error) { return k8s.NewSecretClientFor(clientset), nil }
		deps.classClient = func() (*k8s.StorageClassClient, error) { return k8s.NewStorageClassClientFor(clientset), nil }
		deps.volumeClient = func() (*k8s.PersistentVolumeClient, error) {
			return k8s.NewPersistentVolumeClientFor(clientset), nil
		}
		deps.claimClient = func() (*k8s.PersistentVolumeClaimClient, error) {
			return k8s.NewPersistentVolumeClaimClientFor(clientset), nil
		}
	}
	return deps
}

func TestRealMainExitCodes(t *testing.T) {
	keepDefaultLogger(t)
	marker := filepath.Join(t.TempDir(), "registered")
	require.NoError(t, os.WriteFile(marker, nil, 0o600))

	tests := []struct {
		name       string
		args       []string
		code       int
		stdout     string
		stderr     string
		stdoutNone bool
	}{
		{name: "unknown flag", args: []string{"-no-such-flag"}, code: 2, stderr: "flag provided but not defined"},
		{name: "bad flag value", args: []string{"-snapshot-name-prefix=a b"}, code: 2, stderr: "-snapshot-name-prefix"},
		{name: "version", args: []string{"-version"}, code: 0, stdout: "driver:  " + version.Get().DriverName},
		{name: "registration recorded", args: []string{"-check-registration=" + marker}, code: 0, stdoutNone: true},
		{
			name: "registration not recorded", args: []string{"-check-registration=" + marker + ".missing"},
			code: 1, stderr: "has not called NodeGetInfo yet",
		},
		{name: "driver fails to start", args: []string{"-mode=bogus", "-log-level=error"}, code: 1, stderr: "invalid -mode"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			assert.Equal(t, tc.code, realMain(tc.args, &stdout, &stderr), "stderr: %s", stderr.String())
			assert.Contains(t, stdout.String(), tc.stdout)
			assert.Contains(t, stderr.String(), tc.stderr)
			if tc.stdoutNone {
				assert.Empty(t, stdout.String())
			}
		})
	}
}

func TestPrintVersionNamesEveryBuildField(t *testing.T) {
	var out bytes.Buffer
	printVersion(&out)
	for _, field := range []string{"driver:", "version:", "commit:", "built:", "go:"} {
		assert.Contains(t, out.String(), field)
	}
}

func TestSetupLoggingHonoursTheLevel(t *testing.T) {
	keepDefaultLogger(t)
	for _, level := range []string{"debug", "info", "warn", "error", "DEBUG"} {
		var out bytes.Buffer
		require.NoError(t, setupLogging(level, &out), level)
		slog.Debug("debug line")
		slog.Error("error line")
		assert.Contains(t, out.String(), "error line", level)
		assert.Equal(t, strings.EqualFold(level, "debug"), strings.Contains(out.String(), "debug line"), level)
	}

	err := setupLogging("loud", &bytes.Buffer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid -log-level "loud"`)
}

// k8s.io/mount-utils reports a failed mount through klog, with the mount
// command and its output on lines of their own. Through setupLogging that is
// one key=value line at ERROR, with a time= field, like every other line.
func TestSetupLoggingRoutesKlogThroughTheStructuredLogger(t *testing.T) {
	keepDefaultLogger(t)
	var out bytes.Buffer
	require.NoError(t, setupLogging("info", &out))

	klog.Errorf("Mount failed: exit status 32\nMounting command: mount\nOutput: mount.nfs: access denied by server")
	klog.Flush()

	got := strings.TrimRight(out.String(), "\n")
	require.NotEmpty(t, got, "klog output did not reach the structured logger")
	assert.NotContains(t, got, "\n", "one record, one line")
	assert.Regexp(t, `^time=\S+ level=ERROR msg="Mount failed: exit status 32\\nMounting command: mount`, got)
}

func TestWarnOnLegacyFallbackConfigNamesOnlyWhatIsSet(t *testing.T) {
	keepDefaultLogger(t)
	for _, name := range legacyFallbackEnv {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}
	var out bytes.Buffer
	require.NoError(t, setupLogging("debug", &out))

	warnOnLegacyFallbackConfig()
	assert.Contains(t, out.String(), "no legacy fallback configuration present")
	assert.NotContains(t, out.String(), "level=WARN")

	out.Reset()
	t.Setenv("QUMULO_SERVER", "10.0.0.1")
	t.Setenv("QUMULO_API_TOKEN_FILE", "/etc/token")
	warnOnLegacyFallbackConfig()
	assert.Contains(t, out.String(), "level=WARN")
	assert.Contains(t, out.String(), "QUMULO_SERVER")
	assert.Contains(t, out.String(), "QUMULO_API_TOKEN_FILE")
	assert.NotContains(t, out.String(), "QUMULO_API_URL")
}

func TestInventoryObserverOrNilIsATrulyNilInterface(t *testing.T) {
	assert.Nil(t, inventoryObserverOrNil(nil))
	registry := metrics.New(version.Get())
	assert.Equal(t, registry, inventoryObserverOrNil(registry))
}

func TestVolumeRecordListerReadsThisDriversVolumes(t *testing.T) {
	pv := func(name, driverName string, size string) *corev1.PersistentVolume {
		return &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{Driver: driverName, VolumeHandle: "handle-" + name},
				},
			},
		}
	}
	clientset := fake.NewClientset(pv("a", "csi.qumulo.com", "1Gi"), pv("b", "other.csi.example.com", "2Gi"))

	records, err := volumeRecordLister(k8s.NewPersistentVolumeClientFor(clientset), "csi.qumulo.com")(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []metrics.VolumeRecord{{Handle: "handle-a", CapacityBytes: 1 << 30}}, records)

	clientset.PrependReactor("list", "persistentvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})
	_, err = volumeRecordLister(k8s.NewPersistentVolumeClientFor(clientset), "csi.qumulo.com")(t.Context())
	require.Error(t, err)
}

func TestRunRefusesAConfigItCannotServe(t *testing.T) {
	keepDefaultLogger(t)
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = busy.Close() })

	base := config{
		driverName: "csi.qumulo.com", mode: "controller", logLevel: "error",
		requestTimeout: time.Second, snapshotPrefix: "snapshot",
	}
	tests := []struct {
		name   string
		edit   func(*config, *startupDeps)
		errSub string
	}{
		{name: "log level", edit: func(c *config, _ *startupDeps) { c.logLevel = "loud" }, errSub: "invalid -log-level"},
		{name: "mode", edit: func(c *config, _ *startupDeps) { c.mode = "bogus" }, errSub: "invalid -mode"},
		{
			name: "all mode without a hostname",
			edit: func(c *config, d *startupDeps) {
				c.mode = "all"
				d.hostname = func() (string, error) { return "", errors.New("uts namespace gone") }
			},
			errSub: "hostname unavailable",
		},
		{name: "node mode without a node ID", edit: func(c *config, _ *startupDeps) { c.mode = "node" }, errSub: "configuring driver"},
		{name: "endpoint scheme", edit: func(c *config, _ *startupDeps) { c.endpoint = "ftp://127.0.0.1:1" }, errSub: "ftp"},
		{
			name:   "endpoint in use",
			edit:   func(c *config, _ *startupDeps) { c.endpoint = "tcp://" + busy.Addr().String() },
			errSub: "listening on",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.endpoint = shortSocket(t)
			deps := testDeps(nil, &syncBuffer{})
			tc.edit(&cfg, &deps)

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			err := run(ctx, cfg, deps)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errSub)
		})
	}
}

func TestRunServesWhatItWasConfiguredFor(t *testing.T) {
	keepDefaultLogger(t)

	tests := []struct {
		name        string
		cfg         config
		k8s         bool
		controller  bool
		node        bool
		replication bool
		nodeID      string
		logs        []string
	}{
		{
			name: "controller with every feature and the Kubernetes API",
			cfg: config{
				mode: "controller", capacity: true, inventory: true, replication: true,
				metricsAddress: "127.0.0.1:0",
			},
			k8s: true, controller: true, replication: true,
			logs: []string{
				"capacity reporting enabled",
				"volume inventory enabled",
				"csi-addons replication service enabled",
				"serving prometheus metrics",
				"routing=",
			},
		},
		{
			name: "controller with every feature and no Kubernetes API",
			cfg: config{
				mode: "controller", capacity: true, inventory: true,
				metricsAddress: "127.0.0.1:0", insecureTLS: true,
			},
			controller: true,
			logs: []string{
				"-enable-capacity is set but no in-cluster Kubernetes config is available",
				"-enable-inventory is set but no in-cluster Kubernetes config is available",
				"DeleteVolume cannot check",
				"CreateVolume will not sweep",
				"cannot reach the Kubernetes API",
				"TLS certificate verification is disabled",
			},
		},
		{
			name: "node with the fake mounter",
			cfg:  config{mode: "node", nodeID: "worker-1", fakeMounter: true, replication: true},
			node: true, nodeID: "worker-1",
			logs: []string{"-fake-mounter is set", "nodeID=worker-1"},
		},
		{
			name:       "all mode takes the hostname for a node ID",
			cfg:        config{mode: "all", fakeMounter: true},
			controller: true, node: true, nodeID: "test-host",
			logs: []string{"defaulting to hostname", "services=identity,controller,node"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.endpoint = shortSocket(t)
			cfg.driverName = "csi.qumulo.com"
			cfg.logLevel = "info"
			cfg.requestTimeout = time.Second
			cfg.unmountTimeout = time.Second
			cfg.snapshotPrefix = "snapshot"

			var clientset *fake.Clientset
			if tc.k8s {
				clientset = fake.NewClientset()
			}
			logs := &syncBuffer{}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- run(ctx, cfg, testDeps(clientset, logs)) }()

			conn, err := grpc.NewClient(cfg.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()

			// The socket appears once run reaches Listen; Probe answers once
			// Serve has started.
			require.Eventually(t, func() bool {
				rctx, rcancel := context.WithTimeout(ctx, time.Second)
				defer rcancel()
				_, err := csi.NewIdentityClient(conn).Probe(rctx, &csi.ProbeRequest{})
				return err == nil
			}, 10*time.Second, 20*time.Millisecond, "driver never answered Probe; logs:\n%s", logs.String())

			rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
			defer rcancel()

			_, err = csi.NewControllerClient(conn).ControllerGetCapabilities(rctx, &csi.ControllerGetCapabilitiesRequest{})
			assertServed(t, tc.controller, err, "controller")

			info, err := csi.NewNodeClient(conn).NodeGetInfo(rctx, &csi.NodeGetInfoRequest{})
			assertServed(t, tc.node, err, "node")
			if tc.node && err == nil {
				assert.Equal(t, tc.nodeID, info.GetNodeId())
			}

			_, err = identity.NewIdentityClient(conn).GetIdentity(rctx, &identity.GetIdentityRequest{})
			assertServed(t, tc.replication, err, "csi-addons identity")

			require.NoError(t, conn.Close())
			cancel()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(shutdownGracePeriod + 5*time.Second):
				t.Fatal("run did not return after its context was cancelled")
			}

			out := logs.String()
			for _, want := range append(tc.logs, "starting qumulo csi driver", "driver stopped") {
				assert.Contains(t, out, want)
			}
		})
	}
}

// assertServed checks that an RPC was answered when its service is
// registered, and Unimplemented when it is not.
func assertServed(t *testing.T, served bool, err error, service string) {
	t.Helper()
	if served {
		assert.NotEqual(t, codes.Unimplemented, status.Code(err), "%s should be served: %v", service, err)
		return
	}
	assert.Equal(t, codes.Unimplemented, status.Code(err), "%s should not be served: %v", service, err)
}
