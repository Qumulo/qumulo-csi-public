// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		name        string
		endpoint    string
		wantNetwork string
		wantAddr    string
		wantErr     string
	}{
		{
			name:        "unix with three slashes",
			endpoint:    "unix:///csi/csi.sock",
			wantNetwork: "unix",
			wantAddr:    "/csi/csi.sock",
		},
		{
			// Sidecars and COs disagree on the slash count, so both forms
			// have to work; rejecting one would fail to start for a reason
			// nobody would think to look for.
			name:        "unix with one slash",
			endpoint:    "unix:/csi/csi.sock",
			wantNetwork: "unix",
			wantAddr:    "/csi/csi.sock",
		},
		{
			name:        "tcp",
			endpoint:    "tcp://127.0.0.1:10000",
			wantNetwork: "tcp",
			wantAddr:    "127.0.0.1:10000",
		},
		{
			name:        "tcp wildcard",
			endpoint:    "tcp://:10000",
			wantNetwork: "tcp",
			wantAddr:    ":10000",
		},
		{
			name:     "unsupported scheme",
			endpoint: "http://localhost:8080",
			wantErr:  "unsupported endpoint",
		},
		{
			name:     "no scheme",
			endpoint: "/csi/csi.sock",
			wantErr:  "unsupported endpoint",
		},
		{
			name:     "empty",
			endpoint: "",
			wantErr:  "unsupported endpoint",
		},
		{
			name:     "scheme but no address",
			endpoint: "unix://",
			wantErr:  "no address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			network, addr, err := ParseEndpoint(tt.endpoint)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantNetwork, network)
			assert.Equal(t, tt.wantAddr, addr)
		})
	}
}

func TestListenTCP(t *testing.T) {
	listener, err := Listen("tcp://127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	assert.Equal(t, "tcp", listener.Addr().Network())
}

func TestListenRejectsBadEndpoint(t *testing.T) {
	listener, err := Listen("http://localhost:8080")

	require.Error(t, err)
	assert.Nil(t, listener)
}

// TestListenClearsStaleSocket covers a restarting pod: the socket file from
// the previous instance is still in the shared volume, and Listen must not
// mistake it for a live listener.
func TestListenClearsStaleSocket(t *testing.T) {
	socket := shortLivedSocketPath(t)

	require.NoError(t, os.WriteFile(socket, []byte("stale"), 0o600))

	listener, err := Listen("unix://" + socket)
	require.NoError(t, err)
	defer listener.Close()

	assert.Equal(t, "unix", listener.Addr().Network())
	assert.Equal(t, socket, listener.Addr().String())
}

// TestListenCreatesSocketDirectory covers the first start on a node, where
// the plugin directory may not exist yet.
func TestListenCreatesSocketDirectory(t *testing.T) {
	socket := filepath.Join(shortLivedDir(t), "nested", "csi.sock")

	listener, err := Listen("unix://" + socket)
	require.NoError(t, err)
	defer listener.Close()

	_, err = os.Stat(filepath.Dir(socket))
	assert.NoError(t, err, "the socket's directory should have been created")
}

func TestListenUnixIsDialable(t *testing.T) {
	socket := shortLivedSocketPath(t)

	listener, err := Listen("unix://" + socket)
	require.NoError(t, err)
	defer listener.Close()

	conn, err := net.Dial("unix", socket)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func shortLivedSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(shortLivedDir(t), "csi.sock")
}

// shortLivedDir returns a temp directory short enough to hold a unix socket
// path. A sockaddr_un is capped at around 104 bytes on macOS and 108 on
// Linux, and the default TMPDIR on macOS is long enough that t.TempDir()
// plus a filename can exceed it -- which fails as a bare "invalid argument"
// with nothing pointing at the length.
func shortLivedDir(t *testing.T) string {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("unix sockets are not supported on Windows")
	}

	dir, err := os.MkdirTemp("/tmp", "qcsi")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	return dir
}
