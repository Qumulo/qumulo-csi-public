// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The handle parser, against the same cases pkg/driver/volume_id_test.go
// covers. This tool re-implements the parser (see handle.go for why), so the
// two have to be pinned to the same contract independently.
func TestParseVolumeHandle(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handle  string
		want    volumeHandle
		wantErr bool
	}{
		{
			name:   "the ordinary form, no explicit endpoint",
			handle: "qumulo.example.com##/csi/volumes/pvc-abc123",
			want:   volumeHandle{Server: "qumulo.example.com", RealPath: "/csi/volumes/pvc-abc123"},
		},
		{
			name:   "a tenant class, whose REST endpoint is not its NFS address",
			handle: "tenant.example.com#https://mgmt.example.com:8000#/tenants/pod1/pvc-abc123",
			want: volumeHandle{
				Server:   "tenant.example.com",
				APIURL:   "https://mgmt.example.com:8000",
				RealPath: "/tenants/pod1/pvc-abc123",
			},
		},
		{
			name:   "an IP-named array",
			handle: "192.0.2.20##/csi/volumes/pvc-abc123",
			want:   volumeHandle{Server: "192.0.2.20", RealPath: "/csi/volumes/pvc-abc123"},
		},
		{
			name:   "a path with an adDomain level in it",
			handle: "c1##/csi/volumes/CORP.EXAMPLE.COM/pvc-a",
			want:   volumeHandle{Server: "c1", RealPath: "/csi/volumes/CORP.EXAMPLE.COM/pvc-a"},
		},
		{name: "empty", handle: "", wantErr: true},
		{name: "no separator", handle: "c1/csi/volumes/pvc-a", wantErr: true},
		{name: "one separator", handle: "c1#/csi/volumes/pvc-a", wantErr: true},
		{name: "three separators", handle: "c1###/pvc-a", wantErr: true},
		{name: "an empty path", handle: "c1##", wantErr: true},
		{name: "an empty server", handle: "##/csi/volumes/pvc-a", wantErr: true},
		{name: "a relative path", handle: "c1##csi/volumes/pvc-a", wantErr: true},
		{name: "the cluster root", handle: "c1##/", wantErr: true},
		// The retired <server>/<fileID>/<name> form, which this tool must not
		// silently accept: it would rewrite the server and leave a file ID
		// that names a different directory on the peer.
		{name: "the retired file-ID format", handle: "c1/12884901888/pvc-a", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseVolumeHandle(tc.handle)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.handle, got.String(), "the handle must round-trip")
			assert.Equal(t, path.Base(tc.want.RealPath), got.Name())
		})
	}
}

// The rewrite itself: the server replaced, the path kept.
//
// This test used to assert that the peer's own file ID replaced the source's,
// which was this tool's whole reason to exist. The handle no longer carries a
// file ID, so the rewrite is a server substitution and the class of bug it
// used to guard -- a stale ID naming an unrelated directory on the peer --
// cannot be expressed.
func TestVolumeHandleOnPeer(t *testing.T) {
	handle, err := parseVolumeHandle("c1.example.com##/csi/volumes/pvc-abc123")
	require.NoError(t, err)

	t.Run("the path is carried across unchanged", func(t *testing.T) {
		peer, err := handle.onPeer("c2.example.com", "", "")
		require.NoError(t, err)
		assert.Equal(t, "c2.example.com##/csi/volumes/pvc-abc123", peer.String())
		assert.Equal(t, handle.RealPath, peer.RealPath, "the replica lives at the same path")
	})

	t.Run("a class with its own peerBasePath moves the path", func(t *testing.T) {
		peer, err := handle.onPeer("c2.example.com", "", "/dr/volumes/pvc-abc123")
		require.NoError(t, err)
		assert.Equal(t, "c2.example.com##/dr/volumes/pvc-abc123", peer.String())
	})

	t.Run("a relative replica path is refused", func(t *testing.T) {
		_, err := handle.onPeer("c2.example.com", "", "csi/volumes/pvc-abc123")
		require.Error(t, err)
	})

	t.Run("an empty peer server is refused", func(t *testing.T) {
		_, err := handle.onPeer("", "", "/csi/volumes/pvc-abc123")
		require.Error(t, err)
	})

	// The tool must mint the handle the driver would, and the driver
	// drops every trailing slash of an endpoint, not one.
	t.Run("every trailing slash of the peer endpoint is dropped", func(t *testing.T) {
		for _, apiURL := range []string{"https://c2.example.com:9000/", "https://c2.example.com:9000//"} {
			peer, err := handle.onPeer("c2.example.com", apiURL, "")
			require.NoError(t, err)
			assert.Equal(t, "c2.example.com#https://c2.example.com:9000#/csi/volumes/pvc-abc123", peer.String(), apiURL)
		}
	})
}

// Which topology a volume is in is decided by one question: is `server` an
// address or a name. Everything `switch` does follows from it.
func TestIsIPLiteral(t *testing.T) {
	for _, tc := range []struct {
		server string
		want   bool
	}{
		{server: "192.0.2.20", want: true},
		{server: "192.168.0.1", want: true},
		{server: "fd00::1", want: true},
		{server: "qumulo.example.com", want: false},
		{server: "qumulo", want: false},
		// A name that merely looks numeric is still a name: DNS-1123 allows
		// it, and it resolves, so it can be flipped.
		{server: "10-12-1-145.example.com", want: false},
		{server: "", want: false},
	} {
		t.Run(tc.server, func(t *testing.T) {
			assert.Equal(t, tc.want, isIPLiteral(tc.server))
		})
	}
}

// The endpoint is array-specific, so a mirror has to substitute it the way it
// substitutes the server. A mirrored tenant volume that kept the source's
// endpoint would send its DeleteVolume back to the array it failed over from.
func TestOnPeerReplacesTheAPIEndpoint(t *testing.T) {
	handle, err := parseVolumeHandle("tenant-a.example.com#https://mgmt-a.example.com:8000#/tenants/pod1/pvc-abc")
	require.NoError(t, err)

	t.Run("the peer's endpoint replaces the source's", func(t *testing.T) {
		peer, err := handle.onPeer("tenant-b.example.com", "https://mgmt-b.example.com:8000", "")
		require.NoError(t, err)
		assert.Equal(t,
			"tenant-b.example.com#https://mgmt-b.example.com:8000#/tenants/pod1/pvc-abc",
			peer.String())
	})

	t.Run("an endpoint equal to the peer's default is not recorded", func(t *testing.T) {
		peer, err := handle.onPeer("c2.example.com", "https://c2.example.com:8000", "")
		require.NoError(t, err)
		assert.Empty(t, peer.APIURL)
		assert.Equal(t, "c2.example.com##/tenants/pod1/pvc-abc", peer.String())
	})
}
