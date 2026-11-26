// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

func nfs41Request(name string) *csi.CreateVolumeRequest {
	params := testParams()
	params["protocol"] = "nfs4.1"
	return &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               name,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         params,
	}
}

// The point of the preflight: fail on the PVC with a reason, rather than in the
// kernel with "Protocol not supported" once a pod is already stuck mounting.
func TestCreateVolumeRefusesNFS41WhenTheClusterHasV4Off(t *testing.T) {
	created := false
	srv := newTestControllerServer(t, &MockClient{
		GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
			return qumulo.NFSSettings{V4Enabled: false, AuthSysEnabled: true}, nil
		},
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			created = true
			return qumulo.FileInfo{ID: testFileID}, nil
		},
	})

	_, err := srv.CreateVolume(context.Background(), nfs41Request("pvc-v4off"))

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "v4_enabled",
		"the message must name the cluster setting an operator has to change")
	assert.Contains(t, err.Error(), "nfs3", "and offer the way forward")
	assert.False(t, created, "nothing may be created before the protocol is known to work")
}

func TestCreateVolumeAllowsNFS41WhenV4IsOn(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
			return qumulo.NFSSettings{V4Enabled: true, AuthSysEnabled: true}, nil
		},
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	})

	resp, err := srv.CreateVolume(context.Background(), nfs41Request("pvc-v4on"))

	require.NoError(t, err)
	assert.NotNil(t, resp.GetVolume())
}

// nfs3 is always available, so the preflight must not spend a call on it.
func TestCreateVolumeDoesNotCheckSettingsForNFS3(t *testing.T) {
	asked := false
	client := &MockClient{
		GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
			asked = true
			return qumulo.NFSSettings{}, nil
		},
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	}
	srv := newTestControllerServer(t, client)
	req := nfs41Request("pvc-nfs3")
	req.Parameters["protocol"] = "nfs3"

	_, err := srv.CreateVolume(context.Background(), req)

	require.NoError(t, err)
	assert.False(t, asked, "nfs3 needs no capability check")
}

// An unreachable settings endpoint must not stop provisioning. Failing closed
// would turn one API blip into a cluster-wide outage; failing open restores the
// behaviour that existed before the preflight, which is a mount error at worst.
func TestCreateVolumeFailsOpenWhenSettingsCannotBeRead(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
			return qumulo.NFSSettings{}, errors.New("connection refused")
		},
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	})

	resp, err := srv.CreateVolume(context.Background(), nfs41Request("pvc-blip"))

	require.NoError(t, err, "an unreadable settings endpoint must not block provisioning")
	assert.NotNil(t, resp.GetVolume())
}

func TestNFSSettingsCacheAvoidsRepeatedReads(t *testing.T) {
	calls := 0
	client := &MockClient{GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
		calls++
		return qumulo.NFSSettings{V4Enabled: true}, nil
	}}
	cache := newNFSSettingsCache()

	for range 5 {
		_, err := cache.get(context.Background(), client, "cluster-a")
		require.NoError(t, err)
	}

	assert.Equal(t, 1, calls, "a value that changes by administrative action needs one read")
}

// One controller serves many clusters, so a single cached value would let one
// cluster's configuration decide what another may provision.
func TestNFSSettingsCacheIsPerCluster(t *testing.T) {
	asked := map[string]int{}
	cache := newNFSSettingsCache()

	for _, host := range []string{"cluster-a", "cluster-b", "cluster-a"} {
		h := host
		client := &MockClient{GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
			asked[h]++
			return qumulo.NFSSettings{V4Enabled: h == "cluster-a"}, nil
		}}
		settings, err := cache.get(context.Background(), client, h)
		require.NoError(t, err)
		assert.Equal(t, h == "cluster-a", settings.V4Enabled)
	}

	assert.Equal(t, 1, asked["cluster-a"], "the second read of cluster A is cached")
	assert.Equal(t, 1, asked["cluster-b"])
}

func TestNFSSettingsCacheExpires(t *testing.T) {
	calls := 0
	client := &MockClient{GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
		calls++
		return qumulo.NFSSettings{V4Enabled: true}, nil
	}}
	cache := newNFSSettingsCache()
	clock := time.Now()
	cache.now = func() time.Time { return clock }

	_, err := cache.get(context.Background(), client, "cluster-a")
	require.NoError(t, err)
	clock = clock.Add(nfsSettingsTTL + time.Second)
	_, err = cache.get(context.Background(), client, "cluster-a")
	require.NoError(t, err)

	assert.Equal(t, 2, calls,
		"an operator who enables NFSv4 must not have to restart the controller")
}

// A failed read must not be remembered as fact for the whole TTL.
func TestNFSSettingsCacheDoesNotCacheFailures(t *testing.T) {
	calls := 0
	client := &MockClient{GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
		calls++
		return qumulo.NFSSettings{}, errors.New("boom")
	}}
	cache := newNFSSettingsCache()

	for range 3 {
		_, err := cache.get(context.Background(), client, "cluster-a")
		require.Error(t, err)
	}

	assert.Equal(t, 3, calls)
}

// TestRefusalsExplainTheStalenessWindow is SIM-01's acceptance.
//
// The reported defect was that the settings cache is held for the controller's
// lifetime and needs a restart. It is not: it has a TTL, and a live re-run
// measured a PVC binding on the provisioner's own retry 4m47s after the cache
// was warmed, with the original controller still running. What was real is
// that the refusal names an action -- "enable NFSv4 on the cluster" -- and
// then keeps failing for the rest of the window after the operator does it,
// saying nothing about why.
//
// So the fix is the sentence, and this pins it. The assertion is deliberately
// on the *note* rather than on wording: it must state the interval and it must
// say a restart is not needed, because "restart the controller" is exactly the
// wrong conclusion the silence used to invite.
func TestRefusalsExplainTheStalenessWindow(t *testing.T) {
	t.Parallel()

	assert.Contains(t, nfsSettingsStalenessNote, nfsSettingsTTL.String(),
		"the note must state the real interval, built from the constant so it cannot drift")
	assert.Contains(t, nfsSettingsStalenessNote, "no restart is needed",
		"the note exists to stop an operator reaching for a restart that does nothing")
}
