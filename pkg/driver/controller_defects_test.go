//go:build defects

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

	"qumulo-csi-driver/pkg/qumulo"
)

const (
	dfxDNSServer = "csi-dr.example.com" // floating name shared by both arrays
	dfxAPIA      = "https://10.0.0.1:8000"
	dfxAPIB      = "https://10.0.0.2:8000"
)

// A dedicated-export class with tenantID whose server is a node's
// own (DHCP) address on the tenant's network must provision (or warn), not be
// refused: the tenant serves NFS on that address.
// inverted from TestDefectTenantCheckRefusesNodeAddress.
func TestDefect_TenantCheckAcceptsANodesOwnAddress(t *testing.T) {
	var exportsCreated []qumulo.ExportSpec
	client := &MockClient{
		ListTenantsFunc: func(context.Context) ([]qumulo.TenantInfo, error) {
			return []qumulo.TenantInfo{{ID: 1, Name: "Default", NFSEnabled: true}}, nil
		},
		ListNetworksFunc: func(context.Context) ([]qumulo.NetworkInfo, error) {
			// A typical shape: DHCP node addresses plus floating IPs.
			return []qumulo.NetworkInfo{{
				ID: 1, Name: "Default", TenantID: 1, AddressType: "DHCP",
				FloatingAddresses: []string{"192.0.2.100-192.0.2.111"},
			}}, nil
		},
		CreateDirectoryFunc: func(_ context.Context, parent, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "101", Path: parent + "/" + name, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		CreateExportFunc: func(_ context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			exportsCreated = append(exportsCreated, spec)
			return qumulo.ExportInfo{ID: "7", ExportPath: spec.ExportPath, FSPath: spec.FSPath}, nil
		},
	}
	_, err := newTestControllerServer(t, client).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-node-ip",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Secrets:            testSecrets(),
		Parameters: map[string]string{
			"server":            "192.0.2.21", // node 1's own DHCP address, not a floating one
			"storeRealPath":     "/csi-volumes",
			"storeExportPath":   "/csi",
			"provisioningModel": "dedicated-export",
			"allowedClients":    "192.0.2.0/24",
			"tenantID":          "1",
		},
	})
	t.Logf("CreateVolume answered %v; exports created %d", err, len(exportsCreated))
	require.NoError(t, err, "a node's own address on the tenant's network was refused")
	if assert.Len(t, exportsCreated, 1) {
		assert.Equal(t, 1, exportsCreated[0].TenantID)
	}
}

// ControllerGetVolume must answer from the array the handle names,
// not from the first StorageClass (by name) whose server matches.
// and TestDefectControllerGetVolumeDialsTheHandlesEndpoint.
func TestDefect_ControllerGetVolumeAnswersFromTheHandlesArray(t *testing.T) {
	var aLookups, bLookups int
	arrayA := &MockClient{
		LookupFileByPathFunc: func(_ context.Context, p string) (qumulo.FileInfo, error) {
			aLookups++
			return qumulo.FileInfo{ID: "1", Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil }, // A's replica: 1 GiB
	}
	arrayB := &MockClient{
		LookupFileByPathFunc: func(_ context.Context, p string) (qumulo.FileInfo, error) {
			bLookups++
			return qumulo.FileInfo{ID: "2", Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 4 << 30, nil }, // B's live copy: 4 GiB
	}
	classA := classFor("a-class", dfxDNSServer, "/csi/v", "tok-a")
	classA.Parameters[paramAPIURL] = dfxAPIA
	classB := classFor("b-class", dfxDNSServer, "/csi/v", "tok-b")
	classB.Parameters[paramAPIURL] = dfxAPIB

	srv := inventoryServer(t,
		// A's class sorts first by name, whatever order the lister gives.
		&fakeClassLister{classes: []StorageClassInfo{classB, classA}},
		map[string]map[string][]byte{
			"qumulo-csi/tok-a": {secretKeyToken: []byte("a")},
			"qumulo-csi/tok-b": {secretKeyToken: []byte("b")},
		},
		map[string]*MockClient{dfxAPIA: arrayA, dfxAPIB: arrayB},
	)

	handleOnB := newVolumeID(dfxDNSServer, dfxAPIB, "/csi/v/pvc-1").String()
	resp, err := srv.ControllerGetVolume(context.Background(),
		&csi.ControllerGetVolumeRequest{VolumeId: handleOnB})
	t.Logf("ControllerGetVolume answered %v; A lookups %d, B lookups %d", err, aLookups, bLookups)
	require.NoError(t, err)
	assert.Zero(t, aLookups, "array A was read for a handle naming array B")
	assert.Equal(t, 1, bLookups, "array B, which the handle names, was not read")
	assert.Equal(t, int64(4<<30), resp.GetVolume().GetCapacityBytes(), "B's volume reported with another array's size")
}
