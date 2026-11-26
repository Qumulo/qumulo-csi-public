// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// A VolumeSnapshotClass apiurl override went unchecked: it was dialled
// as written, so an http:// one carried the class's bearer token in cleartext
// to whatever address it named, and only then failed. The StorageClass's own
// apiUrl has been refused for exactly this since.
//
// "Before dialling" is the load-bearing half, so the test proves it by
// building the server with a NewClient that fails the test if it is ever
// called.

// noClientServer is a controller whose NewClient fails the test: nothing may
// reach a cluster.
func noClientServer(t *testing.T) *ControllerServer {
	t.Helper()

	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			t.Errorf("a client was built for %q: the request should have been refused before anything was dialled", baseURL)
			return &MockClient{}
		},
	})
	require.NoError(t, err)

	return NewControllerServer(d)
}

func TestCreateSnapshotRefusesAnHTTPOverrideBeforeDialling(t *testing.T) {
	srv := noClientServer(t)

	_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
		Parameters:     map[string]string{"apiurl": "http://qumulo.example.com:8000"},
	})

	assertCode(t, err, codes.InvalidArgument)
	assert.Contains(t, err.Error(), "https://")
	assert.Contains(t, err.Error(), "cleartext")
}

// Every other shape of apiurl the StorageClass refuses, refused here too:
// the two are the same value, read by the same validateAPIURL.
func TestCreateSnapshotRefusesAMalformedOverride(t *testing.T) {
	for name, apiURL := range map[string]string{
		"no scheme":        "qumulo.example.com:8000",
		"no host":          "https://",
		"handle separator": "https://qumulo.example.com:8000#",
	} {
		t.Run(name, func(t *testing.T) {
			srv := noClientServer(t)

			_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
				Secrets:        testSecrets(),
				Name:           "snap-1",
				SourceVolumeId: testHandle("pvc-abc"),
				Parameters:     map[string]string{"apiurl": apiURL},
			})

			assertCode(t, err, codes.InvalidArgument)
		})
	}
}

// A key this driver does not read is refused rather than ignored, "apiUrl"
// among them. Ignoring it is how a class naming an override took its
// snapshots through the handle's endpoint instead, with nothing saying so.
func TestCreateSnapshotRefusesUnknownSnapshotClassParameters(t *testing.T) {
	for name, params := range map[string]map[string]string{
		"the StorageClass spelling": {"apiUrl": "https://qumulo.example.com:8000"},
		"a typo":                    {"apiurlx": "https://qumulo.example.com:8000"},
		"another driver's key":      {"server": testServer},
	} {
		t.Run(name, func(t *testing.T) {
			srv := noClientServer(t)

			_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
				Secrets:        testSecrets(),
				Name:           "snap-1",
				SourceVolumeId: testHandle("pvc-abc"),
				Parameters:     params,
			})

			assertCode(t, err, codes.InvalidArgument)
			assert.Contains(t, err.Error(), "apiurl")
		})
	}
}

// The CO injects its own parameters, which are not the administrator's to get
// right, so they are allowed through as they are on a StorageClass.
func TestCreateSnapshotAllowsTheCOsOwnParameters(t *testing.T) {
	client := &MockClient{
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: "42", Name: name, SourceFileID: fileID}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
		Parameters: map[string]string{
			"csi.storage.k8s.io/volumesnapshot/name":      "snap-1",
			"csi.storage.k8s.io/volumesnapshot/namespace": "default",
		},
	})

	require.NoError(t, err)
}

// A valid https:// override is still honoured, and the snapshot ID records
// it: DeleteSnapshot and a restore read the ID alone.
func TestCreateSnapshotHonoursAValidOverride(t *testing.T) {
	// The first client built is the one the snapshot is taken through;
	// requireSameCluster builds a second for the volume's own endpoint.
	var dialled string
	client := &MockClient{
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: "42", Name: name, SourceFileID: fileID}, nil
		},
	}
	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			if dialled == "" {
				dialled = baseURL
			}
			return client
		},
	})
	require.NoError(t, err)
	srv := NewControllerServer(d)

	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Secrets:        testSecrets(),
		Name:           "snap-1",
		SourceVolumeId: testHandle("pvc-abc"),
		Parameters:     map[string]string{"apiurl": "https://rest.example.com:8000"},
	})

	require.NoError(t, err)
	assert.Equal(t, "https://rest.example.com:8000", dialled)
	assert.Contains(t, resp.GetSnapshot().GetSnapshotId(), "rest.example.com")
}
