// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// SIM-07. A Qumulo network tenant carries NFS on its own address while REST
// answers on the management tenant alone, which is the secure configuration
// and the one a multi-tenant cluster is actually deployed in. So a tenant
// StorageClass names two different addresses, and everything in this file is
// about the driver reaching the right one of them after CreateVolume, which
// is the only RPC the class is handed to.
//
// Measured shape, from the simnode run of 2026-09-22: server on the tenant
// network, apiUrl on the management network. Provision and mount succeeded;
// delete and expand dialled https://<server>:8000 and got connection refused,
// leaving a Released PV and a leaked export behind on both Core 7.9.3 and
// 7.10.2.
const (
	tenantNFSServer   = "tenant.example.com"
	tenantAPIEndpoint = "https://mgmt.example.com:8000"
)

func tenantParams() map[string]string {
	params := testParams()
	params["server"] = tenantNFSServer
	params[paramAPIURL] = tenantAPIEndpoint
	return params
}

// recordingServer is a controller whose every client records the endpoint it
// was built for. The shared harness hands back one mock whatever address is
// named, which is what most tests want and is exactly what hid this defect:
// the wrong endpoint reached the same mock and every assertion passed.
func recordingServer(t *testing.T, client *MockClient) (*ControllerServer, *[]string) {
	t.Helper()

	dialled := &[]string{}
	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			*dialled = append(*dialled, baseURL)
			return client
		},
	})
	require.NoError(t, err)

	return NewControllerServer(d), dialled
}

// The handle is the only thing DeleteVolume and ControllerExpandVolume are
// given, so if the endpoint is not in it, it is nowhere.
func TestCreateVolumeRecordsTheTenantEndpointInTheHandle(t *testing.T) {
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	}
	srv, dialled := recordingServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         tenantParams(),
	})
	require.NoError(t, err)

	handle := resp.GetVolume().GetVolumeId()
	assert.Equal(t, tenantNFSServer+"#"+tenantAPIEndpoint+"#/csi-volumes/pvc-abc", handle)
	assert.Equal(t, []string{tenantAPIEndpoint}, *dialled,
		"CreateVolume has the class, so it must use the class's endpoint")

	parsed, err := parseVolumeID(handle)
	require.NoError(t, err)
	assert.Equal(t, tenantAPIEndpoint, parsed.APIEndpoint())
	assert.Equal(t, tenantNFSServer, parsed.Server,
		"the NFS address stays the mount target; it is not replaced by the endpoint")
	assert.False(t, parsed.tooLong(), "a tenant handle still fits the CSI spec's 128 bytes")
}

// The regression proper: these two RPCs receive no parameters at all, so
// before the endpoint travelled in the handle they derived
// https://<server>:8000 and dialled the NFS address, which serves no REST.
func TestTenantVolumeDeleteAndExpandUseTheHandleEndpoint(t *testing.T) {
	handle := tenantNFSServer + "#" + tenantAPIEndpoint + "#/csi-volumes/pvc-abc"

	t.Run("DeleteVolume", func(t *testing.T) {
		client := &MockClient{
			LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			},
		}
		srv, dialled := recordingServer(t, client)

		_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			Secrets:  testSecrets(),
			VolumeId: handle,
		})

		require.NoError(t, err)
		require.NotEmpty(t, *dialled)
		for _, addr := range *dialled {
			assert.Equal(t, tenantAPIEndpoint, addr)
			assert.NotContains(t, addr, tenantNFSServer,
				"dialling the NFS address for REST is SIM-07")
		}
	})

	t.Run("ControllerExpandVolume", func(t *testing.T) {
		client := &MockClient{
			LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			},
			GetQuotaFunc:    func(ctx context.Context, fileID string) (uint64, error) { return uint64(testGiB), nil },
			UpdateQuotaFunc: func(ctx context.Context, fileID string, limitBytes uint64) error { return nil },
		}
		srv, dialled := recordingServer(t, client)

		_, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			Secrets:       testSecrets(),
			VolumeId:      handle,
			CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		})

		require.NoError(t, err)
		require.NotEmpty(t, *dialled)
		for _, addr := range *dialled {
			assert.Equal(t, tenantAPIEndpoint, addr)
		}
	})
}

// CreateSnapshot is handed the VolumeSnapshotClass's parameters, which carry
// no endpoint, and DeleteSnapshot and ListSnapshots are handed none at all --
// so a snapshot of a tenant volume was as unreachable as the volume was.
func TestTenantSnapshotRPCsUseTheHandleEndpoint(t *testing.T) {
	volumeHandle := tenantNFSServer + "#" + tenantAPIEndpoint + "#/csi-volumes/pvc-abc"
	snapshotHandle := tenantNFSServer + "#" + tenantAPIEndpoint + "#42"

	t.Run("CreateSnapshot mints a handle carrying the endpoint", func(t *testing.T) {
		client := &MockClient{
			LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			},
			CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
				return qumulo.SnapshotInfo{ID: "42", Name: name, SourceFileID: testFileID}, nil
			},
		}
		srv, dialled := recordingServer(t, client)

		resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
			Secrets:        testSecrets(),
			Name:           "snap-1",
			SourceVolumeId: volumeHandle,
		})

		require.NoError(t, err)
		assert.Equal(t, snapshotHandle, resp.GetSnapshot().GetSnapshotId())
		for _, addr := range *dialled {
			assert.Equal(t, tenantAPIEndpoint, addr)
		}
	})

	t.Run("DeleteSnapshot", func(t *testing.T) {
		client := &MockClient{
			DeleteSnapshotFunc: func(ctx context.Context, id string) error {
				assert.Equal(t, "42", id)
				return nil
			},
		}
		srv, dialled := recordingServer(t, client)

		_, err := srv.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
			Secrets:    testSecrets(),
			SnapshotId: snapshotHandle,
		})

		require.NoError(t, err)
		assert.Equal(t, []string{tenantAPIEndpoint}, *dialled)
	})

	t.Run("ListSnapshots by snapshot ID", func(t *testing.T) {
		client := &MockClient{
			GetSnapshotFunc: func(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
				return qumulo.SnapshotInfo{ID: "42", Name: "snap-1", SourceFileID: testFileID}, nil
			},
			ResolveFileIDsFunc: func(ctx context.Context, ids []string) (map[string]string, error) {
				return map[string]string{testFileID: "/csi-volumes/pvc-abc"}, nil
			},
		}
		srv, dialled := recordingServer(t, client)

		resp, err := srv.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
			Secrets:    testSecrets(),
			SnapshotId: snapshotHandle,
		})

		require.NoError(t, err)
		require.Len(t, resp.GetEntries(), 1)
		assert.Equal(t, snapshotHandle, resp.GetEntries()[0].GetSnapshot().GetSnapshotId())
		assert.Equal(t, volumeHandle, resp.GetEntries()[0].GetSnapshot().GetSourceVolumeId(),
			"the reconstructed source handle must carry the endpoint too, or deleting "+
				"the volume a snapshot names would fail the way the snapshot did")
		assert.Equal(t, []string{tenantAPIEndpoint}, *dialled)
	})
}

// The single-network case is the common one and must pay nothing for this:
// same short handle, same derived endpoint.
func TestSingleNetworkClassStillDerivesItsEndpoint(t *testing.T) {
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	}
	srv, dialled := recordingServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	require.NoError(t, err)
	assert.Equal(t, testServer+"##/csi-volumes/pvc-abc", resp.GetVolume().GetVolumeId(),
		"no apiUrl means an empty endpoint field, not the derived address spelled out")
	assert.Equal(t, []string{"https://" + testServer + ":8000"}, *dialled)
}

// A class that spells out the address the driver would have derived anyway
// must mint the same handle as one that leaves apiUrl unset. Otherwise two
// classes naming one cluster produce handles that differ in a field meaning
// nothing, and a volume looks like it lives somewhere else.
func TestAnEndpointEqualToTheDefaultIsNotRecorded(t *testing.T) {
	spelledOut := newVolumeID(testServer, "https://"+testServer+":8000", "/csi-volumes/pvc-abc")
	unset := newVolumeID(testServer, "", "/csi-volumes/pvc-abc")

	assert.Equal(t, unset, spelledOut)
	assert.Empty(t, spelledOut.APIURL)
	assert.Equal(t, "https://"+testServer+":8000", spelledOut.APIEndpoint())

	trailingSlash := newVolumeID(tenantNFSServer, tenantAPIEndpoint+"/", "/csi-volumes/pvc-abc")
	assert.Equal(t, tenantAPIEndpoint, trailingSlash.APIURL, "a trailing slash is not a different cluster")
}

// An endpoint that is unreachable is a configuration fact, not a driver
// fault. Internal reads as "the driver broke" and a CO retries it silently
// forever, which is exactly what the simnode run observed: the same
// connection-refused every few seconds against a permanently Released PV.
func TestAnUnreachableEndpointIsFailedPreconditionNamingTheAddress(t *testing.T) {
	refused := errors.New("dial tcp: connect: connection refused")

	t.Run("DeleteVolume", func(t *testing.T) {
		client := &MockClient{
			LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{}, refused
			},
		}
		srv, _ := recordingServer(t, client)

		_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			Secrets:  testSecrets(),
			VolumeId: tenantNFSServer + "#" + tenantAPIEndpoint + "#/csi-volumes/pvc-abc",
		})

		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), tenantAPIEndpoint, "the address dialled is the whole report")
	})

	// The same answer from ValidateVolumeCapabilities, which used to
	// re-wrap it as Internal.
	t.Run("ValidateVolumeCapabilities", func(t *testing.T) {
		client := &MockClient{
			LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{}, refused
			},
		}
		srv, _ := recordingServer(t, client)

		_, err := srv.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
			Secrets:            testSecrets(),
			VolumeId:           tenantNFSServer + "#" + tenantAPIEndpoint + "#/csi-volumes/pvc-abc",
			VolumeCapabilities: mountCapabilities(),
		})

		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), tenantAPIEndpoint)
	})

	// And where the endpoint was derived rather than given, the message has
	// to say so: "connection refused to the address you set as server" is the
	// sentence that would have identified SIM-07 from one PVC event.
	t.Run("a derived endpoint says where it came from", func(t *testing.T) {
		client := &MockClient{
			LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			},
			GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) { return 0, refused },
		}
		srv, _ := recordingServer(t, client)

		_, err := srv.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			Secrets:       testSecrets(),
			VolumeId:      tenantNFSServer + "##/csi-volumes/pvc-abc",
			CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		})

		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "https://"+tenantNFSServer+":8000")
		assert.Contains(t, err.Error(), "derived")
		assert.Contains(t, err.Error(), paramAPIURL)
	})

	// An array that *answered*, with a 500 or a 403, is reachable. That is a
	// different failure and must keep its old code, or a real cluster fault
	// would be reported as an operator's misconfiguration.
	t.Run("an array that answered is still Internal", func(t *testing.T) {
		client := &MockClient{
			LookupFileByPathFunc: func(ctx context.Context, path string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{}, apiError(500)
			},
		}
		srv, _ := recordingServer(t, client)

		_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			Secrets:  testSecrets(),
			VolumeId: tenantNFSServer + "#" + tenantAPIEndpoint + "#/csi-volumes/pvc-abc",
		})

		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
	})
}

// An apiUrl containing the handle separator would split into fields that
// parse back as a different address, so it is refused where the class is
// read -- before CreateVolume has made anything on the cluster.
func TestAClassEndpointThatWouldCorruptAHandleIsRefused(t *testing.T) {
	params := tenantParams()
	params[paramAPIURL] = "https://mgmt.example.com:8000#/x"

	_, err := parseVolumeParams(params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), volumeIDSeparator)
	assert.Contains(t, err.Error(), "could not be decoded")
}

// SIM-08. The measured case: a class with tenantID "3" and a server on the
// Default tenant's network. The export was created in tenant 3, the PVC
// Bound, and the pod then failed with `mount.nfs: mounting
// 198.51.100.50:/tenants/pod1/volumes/pvc-...: No such file or directory`.
//
// The address shapes here are the ones the simnode reported from GET
// /v3/network: one floating address per frontend network, each naming its
// tenant.
func tenantNetworks() []qumulo.NetworkInfo {
	return []qumulo.NetworkInfo{
		{ID: 1, Name: "default-net", TenantID: 1, AddressType: "HOST",
			FloatingAddresses: []string{"198.51.100.50"}},
		{ID: 2, Name: "pod1-net", TenantID: 3, AddressType: "HOST",
			FloatingAddresses: []string{"203.0.113.60"}},
	}
}

func tenantAwareClient(networks []qumulo.NetworkInfo) *MockClient {
	return &MockClient{
		ListNetworksFunc: func(ctx context.Context) ([]qumulo.NetworkInfo, error) {
			return networks, nil
		},
		ListTenantsFunc: func(ctx context.Context) ([]qumulo.TenantInfo, error) {
			return []qumulo.TenantInfo{
				{ID: 1, Name: "Default", NFSEnabled: true},
				{ID: 3, Name: "tenant-pod1", NFSEnabled: true},
			}, nil
		},
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	}
}

func tenantCreateRequest(server, tenant string) *csi.CreateVolumeRequest {
	params := testParams()
	params["server"] = server
	params[paramTenantID] = tenant
	params[paramProvisioningModel] = provisioningModelDedicatedExport
	params[paramAllowAnyClient] = "true"

	return &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         params,
	}
}

func TestCreateVolumeRefusesAServerTheTenantDoesNotAnswerOn(t *testing.T) {
	var created bool
	client := tenantAwareClient(tenantNetworks())
	client.CreateDirectoryFunc = func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
		created = true
		return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), tenantCreateRequest("198.51.100.50", "3"))

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "203.0.113.60",
		"the message must name the address that would have worked")
	assert.False(t, created, "nothing may be created on the cluster for a class that cannot mount")
}

func TestCreateVolumeAcceptsAServerOnTheTenantsNetwork(t *testing.T) {
	srv := newTestControllerServer(t, tenantAwareClient(tenantNetworks()))

	_, err := srv.CreateVolume(context.Background(), tenantCreateRequest("203.0.113.60", "3"))

	require.NoError(t, err)
}

// A tenant named rather than numbered resolves first, and the check runs
// against the id it resolved to.
func TestTheNetworkCheckRunsOnAResolvedTenantName(t *testing.T) {
	srv := newTestControllerServer(t, tenantAwareClient(tenantNetworks()))

	_, err := srv.CreateVolume(context.Background(), tenantCreateRequest("198.51.100.50", "tenant-pod1"))

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "203.0.113.60")
}

// The check is a guard on a misconfiguration, not a new way to fail a
// provision. Every case where the tenant's addresses are unknown rather than
// known-not-to-match has to provision, or this breaks clusters that work.
func TestTheNetworkCheckProceedsWhenTheAnswerIsUnknown(t *testing.T) {
	tests := []struct {
		name     string
		server   string
		networks []qumulo.NetworkInfo
		listErr  error
	}{
		{
			name:    "the network configuration cannot be read",
			server:  "198.51.100.50",
			listErr: errors.New("qumulo api /v3/network: HTTP 500"),
		},
		{
			name:   "the tenant's networks use an address scheme this driver does not decode",
			server: "198.51.100.50",
			networks: []qumulo.NetworkInfo{
				{ID: 2, TenantID: 3, AddressType: "STATIC"},
			},
		},
		{
			name:   "the tenant has no networks the configuration mentions",
			server: "198.51.100.50",
			networks: []qumulo.NetworkInfo{
				{ID: 1, TenantID: 1, AddressType: "HOST", FloatingAddresses: []string{"198.51.100.50"}},
			},
		},
		{
			// Resolving it would put this pod's resolver between a class and
			// a provision, and a split-horizon name would refuse volumes that
			// mount perfectly well.
			name:     "the class names the cluster by DNS name",
			server:   "tenant.example.com",
			networks: tenantNetworks(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tenantAwareClient(tt.networks)
			if tt.listErr != nil {
				client.ListNetworksFunc = func(ctx context.Context) ([]qumulo.NetworkInfo, error) {
					return nil, tt.listErr
				}
			}
			srv := newTestControllerServer(t, client)

			_, err := srv.CreateVolume(context.Background(), tenantCreateRequest(tt.server, "3"))

			require.NoError(t, err, "an unknown answer must not refuse a volume")
		})
	}
}

// A tenant with both a network this driver reads and one it does not: the
// refusal has to say the list it printed may be incomplete, or it sends an
// operator to change a value that was right.
func TestARefusalSaysWhenItCouldNotSeeEveryNetwork(t *testing.T) {
	client := tenantAwareClient([]qumulo.NetworkInfo{
		{ID: 1, TenantID: 1, AddressType: "HOST", FloatingAddresses: []string{"198.51.100.50"}},
		{ID: 2, TenantID: 3, AddressType: "HOST", FloatingAddresses: []string{"203.0.113.60"}},
		{ID: 4, TenantID: 3, AddressType: "STATIC"},
	})
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), tenantCreateRequest("198.51.100.50", "3"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "203.0.113.60")
	assert.Contains(t, err.Error(), "STATIC")
}

// A class with no tenantID is every class provisioned before the parameter
// existed. It must not acquire a network check, or a cluster with no
// multitenancy configured at all would start failing provisions.
func TestAClassWithNoTenantIDIsNotChecked(t *testing.T) {
	var listed bool
	client := tenantAwareClient(tenantNetworks())
	client.ListNetworksFunc = func(ctx context.Context) ([]qumulo.NetworkInfo, error) {
		listed = true
		return tenantNetworks(), nil
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: mountCapabilities(),
		Parameters:         testParams(),
	})

	require.NoError(t, err)
	assert.False(t, listed, "a class naming no tenant must not read the network configuration")
}

func TestAddressSpecContains(t *testing.T) {
	for _, tt := range []struct {
		spec, addr string
		want       bool
	}{
		{spec: "203.0.113.60", addr: "203.0.113.60", want: true},
		{spec: "203.0.113.60", addr: "203.0.113.61", want: false},
		// Qumulo writes a range both ways.
		{spec: "203.0.113.60-203.0.113.70", addr: "203.0.113.65", want: true},
		{spec: "203.0.113.60-203.0.113.70", addr: "203.0.113.71", want: false},
		{spec: "203.0.113.60-70", addr: "203.0.113.65", want: true},
		{spec: "203.0.113.60-70", addr: "203.0.113.59", want: false},
		{spec: "203.0.113.60-70", addr: "192.0.2.65", want: false},
		// A spec this code cannot parse must cover nothing: it decides
		// whether to refuse, so it must never answer "yes" by accident.
		{spec: "not-an-address", addr: "203.0.113.60", want: false},
		{spec: "", addr: "203.0.113.60", want: false},
		{spec: "203.0.113.60-", addr: "203.0.113.60", want: false},
		{spec: "203.0.113.60", addr: "not-an-address", want: false},
	} {
		t.Run(tt.spec+"/"+tt.addr, func(t *testing.T) {
			assert.Equal(t, tt.want, addressSpecContains(tt.spec, tt.addr))
		})
	}
}

// A node's own address is served by the tenant that owns the node's
// network, but this check reads only floating ranges and cannot see it. An
// address that is no tenant's floating address is therefore unknown, not
// wrong, and must provision -- the documented "refuse only on positive
// evidence". The lab network was DHCP with twelve floating addresses and the
// class named node 1's own address; the control without tenantID mounted it.
func TestTheNetworkCheckAcceptsAnAddressItCannotPlace(t *testing.T) {
	client := tenantAwareClient([]qumulo.NetworkInfo{
		{ID: 1, Name: "Default", TenantID: 1, AddressType: "DHCP",
			FloatingAddresses: []string{"192.0.2.10-192.0.2.21"}},
	})
	client.ListTenantsFunc = func(context.Context) ([]qumulo.TenantInfo, error) {
		return []qumulo.TenantInfo{{ID: 1, Name: "Default", NFSEnabled: true}}, nil
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), tenantCreateRequest("192.0.2.101", "1"))

	require.NoError(t, err, "a node's own address is not evidence the tenant does not serve it")
}

// The positive evidence the check keeps: the server is another tenant's
// floating address (SIM-08), and the message says whose.
func TestTheNetworkCheckNamesTheTenantAServerBelongsTo(t *testing.T) {
	srv := newTestControllerServer(t, tenantAwareClient(tenantNetworks()))

	_, err := srv.CreateVolume(context.Background(), tenantCreateRequest("198.51.100.50", "3"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "floating address of tenant 1, not of tenant 3")
}

// Two arrays that share one data name -- the floating DNS name the DR runbook
// puts in front of both -- and differ only by REST endpoint.
const (
	sharedNameAPIA = "https://array-a.example:8000"
	sharedNameAPIB = "https://array-b.example:8000"
)

// endpointArrayServer is a controller whose clients are the given mock per REST
// endpoint, recording every endpoint dialled. An endpoint it was not given
// fails the test, so a call that lands on the wrong array cannot pass
// silently by reaching a shared mock.
func endpointArrayServer(t *testing.T, a, b *MockClient) (*ControllerServer, *[]string) {
	t.Helper()

	dialled := &[]string{}
	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			*dialled = append(*dialled, baseURL)
			switch endpoint := strings.TrimSuffix(baseURL, "/"); {
			case strings.EqualFold(endpoint, sharedNameAPIA):
				return a
			case strings.EqualFold(endpoint, sharedNameAPIB):
				return b
			}
			t.Fatalf("dialled an endpoint no array answers on: %q", baseURL)
			return nil
		},
	})
	require.NoError(t, err)

	return NewControllerServer(d), dialled
}

// A snapshot ID carries the endpoint it was
// taken through, and snapshot IDs are per-cluster numbers. Restored through a
// class with the same server but the other array's apiUrl, the snapshot was
// read on the class's array -- a different snapshot 42, someone else's data.
func TestRestoreRefusesASnapshotTakenThroughAnotherEndpoint(t *testing.T) {
	arrayA := &MockClient{
		GetSnapshotFunc: func(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
			t.Errorf("snapshot %s was looked up on array A", id)
			return qumulo.SnapshotInfo{}, nil
		},
	}
	arrayB := emptyRestoreClient()
	arrayB.GetSnapshotFunc = func(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
		t.Errorf("snapshot %s was looked up on array B, which never took it", id)
		return qumulo.SnapshotInfo{ID: id, SourceFileID: "b-unrelated"}, nil
	}
	arrayB.CreateDirectoryFunc = func(ctx context.Context, parent, name string) (qumulo.FileInfo, error) {
		t.Errorf("created %s/%s on array B", parent, name)
		return qumulo.FileInfo{}, nil
	}
	srv, _ := endpointArrayServer(t, arrayA, arrayB)

	params := testParams()
	params[paramAPIURL] = sharedNameAPIB
	req := restoreRequest("pvc-restored", params, testGiB)
	req.VolumeContentSource.GetSnapshot().SnapshotId = newSnapshotID(testServer, sharedNameAPIA, "42").String()

	_, err := srv.CreateVolume(context.Background(), req)

	assertCode(t, err, codes.InvalidArgument)
	assert.Contains(t, err.Error(), sharedNameAPIA)
	assert.Contains(t, err.Error(), sharedNameAPIB)
}

// The control: the same restore through a class that reaches the snapshot's
// own endpoint goes ahead, however the endpoint is spelled.
func TestRestoreThroughTheSnapshotsOwnEndpointProceeds(t *testing.T) {
	for _, classAPIURL := range []string{sharedNameAPIA, sharedNameAPIA + "/", "HTTPS://ARRAY-A.example:8000"} {
		t.Run(classAPIURL, func(t *testing.T) {
			var asked []string
			arrayA := emptyRestoreClient()
			snap := arrayA.GetSnapshotFunc
			arrayA.GetSnapshotFunc = func(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
				asked = append(asked, id)
				return snap(ctx, id)
			}
			srv, _ := endpointArrayServer(t, arrayA, &MockClient{})

			params := testParams()
			params[paramAPIURL] = classAPIURL
			req := restoreRequest("pvc-restored", params, testGiB)
			req.VolumeContentSource.GetSnapshot().SnapshotId = newSnapshotID(testServer, sharedNameAPIA, "42").String()

			_, err := srv.CreateVolume(context.Background(), req)

			require.NoError(t, err)
			assert.Equal(t, []string{"42"}, asked, "the snapshot is read on its own array")
		})
	}
}

// An apiurl in the VolumeSnapshotClass picks
// the array the snapshot is taken on, and the ID has to name that array, not
// the handle's. Otherwise DeleteSnapshot and a restore act on a snapshot with
// the same number on the other array.
func TestCreateSnapshotRecordsTheEndpointItWasTakenThrough(t *testing.T) {
	var deletedOnB string
	arrayA := &MockClient{
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			t.Errorf("snapshot taken on array A, but the request named B")
			return qumulo.SnapshotInfo{}, nil
		},
		DeleteSnapshotFunc: func(ctx context.Context, id string) error {
			t.Errorf("DeleteSnapshot removed snapshot %s on array A, which never took it", id)
			return nil
		},
	}
	arrayB := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) { return nil, nil },
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: "77", Name: "77_" + name, SourceFileID: fileID}, nil
		},
		DeleteSnapshotFunc: func(ctx context.Context, id string) error { deletedOnB = id; return nil },
	}
	srv, _ := endpointArrayServer(t, arrayA, arrayB)

	handle := newVolumeID(testServer, sharedNameAPIA, testStoreRealPath+"/pvc-1").String()
	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snapshot-1",
		SourceVolumeId: handle,
		Secrets:        testSecrets(),
		Parameters:     map[string]string{paramAPIURL: sharedNameAPIB},
	})
	require.NoError(t, err)

	parsed, err := parseSnapshotID(resp.GetSnapshot().GetSnapshotId())
	require.NoError(t, err)
	assert.Equal(t, sharedNameAPIB, parsed.APIEndpoint(), "the ID names the array that holds the snapshot")
	assert.Equal(t, handle, resp.GetSnapshot().GetSourceVolumeId(), "the source is still the volume's own handle")

	_, err = srv.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: resp.GetSnapshot().GetSnapshotId(), Secrets: testSecrets(),
	})
	require.NoError(t, err)
	assert.Equal(t, "77", deletedOnB, "DeleteSnapshot reaches the array that took it")
}

// Without an override the ID records the handle's endpoint, as before.
func TestCreateSnapshotWithoutAnOverrideRecordsTheHandlesEndpoint(t *testing.T) {
	arrayA := &MockClient{
		ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) { return nil, nil },
		CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{ID: "78", Name: "78_" + name, SourceFileID: fileID}, nil
		},
	}
	srv, dialled := endpointArrayServer(t, arrayA, &MockClient{})

	handle := newVolumeID(testServer, sharedNameAPIA, testStoreRealPath+"/pvc-1").String()
	resp, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name: "snapshot-1", SourceVolumeId: handle, Secrets: testSecrets(),
	})
	require.NoError(t, err)

	assert.Equal(t, newSnapshotID(testServer, sharedNameAPIA, "78").String(), resp.GetSnapshot().GetSnapshotId())
	assert.Equal(t, []string{sharedNameAPIA}, *dialled)
}

// An apiurl override is meant for another
// endpoint of the volume's own cluster, but one that reached a different
// cluster was accepted: that cluster snapshotted whatever it held at the same
// path -- in the retest, the DR copy of a volume on the other array. The two
// endpoints' cluster UUIDs are now compared, and only a proven mismatch
// refuses; an endpoint that cannot say lets the snapshot through, because a
// volume's own endpoint being down is when an override is most likely used.
func TestCreateSnapshotRefusesAnOverrideThatReachesAnotherCluster(t *testing.T) {
	uuid := func(id string, err error) func(context.Context) (string, error) {
		return func(context.Context) (string, error) { return id, err }
	}
	down := &url.Error{Op: "Get", URL: sharedNameAPIA + "/v1/node/state", Err: context.DeadlineExceeded}
	for _, tc := range []struct {
		name      string
		a, b      func(context.Context) (string, error)
		want      codes.Code
		wantTaken bool
	}{
		{name: "another cluster", a: uuid("uuid-a", nil), b: uuid("uuid-b", nil), want: codes.InvalidArgument},
		{name: "another endpoint of the same cluster", a: uuid("uuid-a", nil), b: uuid("uuid-a", nil), want: codes.OK, wantTaken: true},
		{name: "the volume's own endpoint does not answer", a: uuid("", down), b: uuid("uuid-b", nil), want: codes.OK, wantTaken: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			taken := false
			arrayA := &MockClient{ClusterIDFunc: tc.a}
			arrayB := &MockClient{
				ClusterIDFunc:     tc.b,
				ListSnapshotsFunc: func(ctx context.Context) ([]qumulo.SnapshotInfo, error) { return nil, nil },
				CreateSnapshotFunc: func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
					taken = true
					return qumulo.SnapshotInfo{ID: "77", Name: "77_" + name, SourceFileID: fileID}, nil
				},
			}
			srv, _ := endpointArrayServer(t, arrayA, arrayB)

			_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
				Name:           "snapshot-1",
				SourceVolumeId: newVolumeID(testServer, sharedNameAPIA, testStoreRealPath+"/pvc-1").String(),
				Secrets:        testSecrets(),
				Parameters:     map[string]string{paramAPIURL: sharedNameAPIB},
			})

			assert.Equal(t, tc.want, status.Code(err), "%v", err)
			assert.Equal(t, tc.wantTaken, taken)
			if tc.want == codes.InvalidArgument {
				assert.Contains(t, err.Error(), "uuid-a")
				assert.Contains(t, err.Error(), "uuid-b")
			}
		})
	}
}
