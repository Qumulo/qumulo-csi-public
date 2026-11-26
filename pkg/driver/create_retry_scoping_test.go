// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"slices"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// A CreateVolume retry whose class has since been edited used to be
// adopted: the directory and the quota matched, so the export already serving
// the volume was adopted as it stood and the response described a volume with
// the class's *new* allowedClients, securityFlavor and tenantID -- none of
// which the export enforces, because an export is not rewritten in place.
//
// The CSI spec's answer for a name that exists with different settings is
// ALREADY_EXISTS, and it is the better one: a PV showing access scoping
// the array does not apply is worse than a refusal naming the difference.

// adoptableVolume is a mock for a volume that already exists, complete, with
// the export described.
func adoptableVolume(export qumulo.ExportInfo) *MockClient {
	return &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, &qumulo.APIError{StatusCode: 409, Class: qumulo.ErrorClassEntryExists}
		},
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetQuotaFunc: func(ctx context.Context, fileID string) (uint64, error) { return uint64(testGiB), nil },
		ListExportsFunc: func(ctx context.Context) ([]qumulo.ExportInfo, error) {
			return []qumulo.ExportInfo{export}, nil
		},
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			return qumulo.ExportInfo{}, assertNoExportCreate
		},
		GetNFSSettingsFunc: func(ctx context.Context) (qumulo.NFSSettings, error) {
			return qumulo.NFSSettings{V4Enabled: true, AuthSysEnabled: true, Krb5Enabled: true, Krb5iEnabled: true, Krb5pEnabled: true, IdmapDomain: "example.com"}, nil
		},
		ListTenantsFunc: func(ctx context.Context) ([]qumulo.TenantInfo, error) {
			return []qumulo.TenantInfo{
				{ID: 1, Name: "Default", NFSEnabled: true},
				{ID: 4, Name: "tenant-finance", NFSEnabled: true},
			}, nil
		},
	}
}

// assertNoExportCreate is what CreateExport answers in these tests: an
// adoption that reaches it has already got the decision wrong.
var assertNoExportCreate = errNoExistingVolume

// volumeExport is the export this driver made for a volume of this name.
func volumeExport(name string) qumulo.ExportInfo {
	return qumulo.ExportInfo{
		ID:                 "5",
		ExportPath:         "/csi/" + name,
		FSPath:             testStoreRealPath + "/" + name,
		Description:        exportDescriptionFor(name),
		TenantID:           1,
		AuthenticationMode: qumulo.AuthenticationModeNone,
		AllowedClients:     []string{"10.0.0.5", "192.168.1.0/24"},
	}
}

func dedicatedParams(extra map[string]string) map[string]string {
	params := testParams()
	params["provisioningModel"] = "dedicated-export"
	params["allowedClients"] = "10.0.0.5,192.168.1.0/24"
	// The mock cluster has two tenants, so a dedicated-export class has to
	// name one; tenant 1 is what the export in these tests belongs to.
	params["tenantID"] = "1"
	for k, v := range extra {
		params[k] = v
	}
	return params
}

func TestCreateVolumeRetryWithDifferentExportRestrictionsIsAlreadyExists(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra map[string]string
		// names is what the refusal must mention, so the operator is told
		// which parameter moved rather than only that something did.
		names string
	}{
		{
			name:  "allowedClients narrowed",
			extra: map[string]string{"allowedClients": "10.0.0.5"},
			names: paramAllowedClients,
		},
		{
			name:  "allowedClients widened",
			extra: map[string]string{"allowedClients": "10.0.0.5,192.168.1.0/24,172.16.0.0/16"},
			names: paramAllowedClients,
		},
		{
			name:  "allowedClients dropped for allowAnyClient",
			extra: map[string]string{"allowedClients": "", "allowAnyClient": "true"},
			names: paramAllowedClients,
		},
		{
			name:  "securityFlavor raised to krb5",
			extra: map[string]string{"securityFlavor": "krb5", "protocol": "nfs4.1"},
			names: paramSecurityFlavor,
		},
		{
			name:  "tenantID moved",
			extra: map[string]string{"tenantID": "4"},
			names: paramTenantID,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := adoptableVolume(volumeExport("pvc-abc"))
			srv := newTestControllerServer(t, client)

			_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-abc",
				VolumeCapabilities: mountCapabilities(),
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
				Parameters:         dedicatedParams(tc.extra),
			})

			assertCode(t, err, codes.AlreadyExists)
			assert.Contains(t, err.Error(), tc.names)
		})
	}
}

// The retry that asks for what the volume already has is still adopted. This
// is the common case by far -- the provisioner retrying a call whose answer
// was lost -- and refusing it would strand every claim whose first attempt
// timed out.
func TestCreateVolumeRetryWithTheSameExportRestrictionsStillSucceeds(t *testing.T) {
	client := adoptableVolume(volumeExport("pvc-abc"))
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		// The same two CIDRs, written the other way round: the cluster holds
		// a set, so the order is not a difference.
		Parameters: dedicatedParams(map[string]string{"allowedClients": "192.168.1.0/24,10.0.0.5"}),
	})

	require.NoError(t, err)
	assert.NotEmpty(t, resp.GetVolume().GetVolumeId())
}

// An export this driver did not make for this volume is not compared at all:
// a replica's covering export is staged from the source rather than from a
// class, and an administrator's is not the driver's to judge. Comparing
// either would refuse creates that have always worked.
func TestCreateVolumeAdoptionIgnoresAnExportItDidNotMakeForThisVolume(t *testing.T) {
	foreign := volumeExport("pvc-abc")
	foreign.Description = "an administrator's export"
	foreign.AllowedClients = []string{"172.16.0.1"}

	client := adoptableVolume(foreign)
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         dedicatedParams(nil),
	})

	require.NoError(t, err)
}

// An export made before securityFlavor was sent carries no authentication
// mode, which is the same thing sys means. An upgrade must not turn every
// retry into AlreadyExists.
func TestCreateVolumeAdoptionTreatsAnUnsetAuthModeAsSys(t *testing.T) {
	export := volumeExport("pvc-abc")
	export.AuthenticationMode = ""

	client := adoptableVolume(export)
	srv := newTestControllerServer(t, client)

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         dedicatedParams(map[string]string{"securityFlavor": "sys"}),
	})

	require.NoError(t, err)
}

// The comparison accepts what the driver itself wrote. parseAllowedClients
// keeps each entry as written (trimmed, never re-canonicalised), and a live
// array was checked to store host_restrictions verbatim, so a retry of a
// class whose entries are not in canonical form -- an upper-case IPv6
// prefix, a /32 -- must still find the export it made identical. A
// normalisation on either side alone would turn every such retry into
// AlreadyExists.
func TestCreateVolumeRetryAdoptsTheExportItWroteVerbatim(t *testing.T) {
	var made []qumulo.ExportInfo
	client := adoptableVolume(qumulo.ExportInfo{})
	client.ListExportsFunc = func(ctx context.Context) ([]qumulo.ExportInfo, error) { return made, nil }
	client.CreateExportFunc = func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
		// What the array holds afterwards: the entries exactly as sent.
		export := qumulo.ExportInfo{
			ID: "7", ExportPath: spec.ExportPath, FSPath: spec.FSPath, Description: spec.Description,
			TenantID: spec.TenantID, AuthenticationMode: spec.AuthenticationMode,
			AllowedClients: slices.Clone(spec.AllowedClients),
		}
		made = append(made, export)
		return export, nil
	}
	srv := newTestControllerServer(t, client)
	req := &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters: dedicatedParams(map[string]string{
			"allowedClients": " 10.0.0.5 ,192.168.1.0/24,2001:DB8::/32,10.1.2.3/32"}),
	}

	_, err := srv.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, made, 1, "the first call must create the export")
	assert.Equal(t, []string{"10.0.0.5", "192.168.1.0/24", "2001:DB8::/32", "10.1.2.3/32"}, made[0].AllowedClients,
		"entries are written as the class gave them, trimmed only")

	_, err = srv.CreateVolume(context.Background(), req)
	require.NoError(t, err, "the retry must adopt the export it wrote")
	assert.Len(t, made, 1, "the retry must not create a second export")
}
