// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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

func TestParseAllowedClients(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []string
		wantErr string
	}{
		{name: "one IPv4", raw: "10.0.0.5", want: []string{"10.0.0.5"}},
		{name: "one CIDR", raw: "10.0.0.0/24", want: []string{"10.0.0.0/24"}},
		{
			name: "a mixed list, order preserved",
			raw:  "10.0.0.5,192.168.1.0/24,172.16.4.9",
			want: []string{"10.0.0.5", "192.168.1.0/24", "172.16.4.9"},
		},
		{name: "surrounding whitespace is trimmed", raw: " 10.0.0.5 , 10.0.0.6 ", want: []string{"10.0.0.5", "10.0.0.6"}},
		{name: "IPv6", raw: "2001:db8::1", want: []string{"2001:db8::1"}},
		{name: "IPv6 CIDR", raw: "2001:db8::/32", want: []string{"2001:db8::/32"}},
		{name: "a single host as /32", raw: "10.0.0.5/32", want: []string{"10.0.0.5/32"}},

		{
			// The dangerous one: ",,," must not read as an empty allowlist,
			// because an empty list means "allow everything".
			name: "an empty entry is refused", raw: "10.0.0.5,,10.0.0.6",
			wantErr: "empty entry",
		},
		{name: "a trailing comma is refused", raw: "10.0.0.5,", wantErr: "empty entry"},
		{name: "a hostname is refused", raw: "nfs-client.example.com", wantErr: "Host names are not accepted"},
		{name: "a malformed address", raw: "10.0.0.999", wantErr: "neither an IP address nor a CIDR"},
		{name: "a malformed CIDR", raw: "10.0.0.0/33", wantErr: "not a valid CIDR"},
		{name: "a range, which NFS does not take", raw: "10.0.0.1-10.0.0.9", wantErr: "neither an IP address nor a CIDR"},
		{
			name:    "longer than the cap",
			raw:     strings.TrimSuffix(strings.Repeat("10.0.0.1,", maxAllowedClients+1), ","),
			wantErr: "the limit is",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAllowedClients(tt.raw)

			if tt.wantErr != "" {
				require.Error(t, err, "%q must be refused", tt.raw)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateTenantID(t *testing.T) {
	tests := []struct {
		name    string
		tenant  string
		wantErr string
	}{
		{name: "an integer id", tenant: "4"},
		{name: "tenant 1, the default", tenant: "1"},
		{name: "a name", tenant: "tenant-finance"},
		{name: "a name with spaces", tenant: "Finance Tenant"},
		{name: "a name with a dot", tenant: "finance.corp"},

		{name: "empty", tenant: "", wantErr: "must not be empty"},
		{name: "zero is not a tenant", tenant: "0", wantErr: "positive tenant id"},
		{name: "negative", tenant: "-1", wantErr: "positive tenant id"},
		{name: "leading whitespace", tenant: " 4", wantErr: "whitespace"},
		{name: "a comma, which would be ambiguous", tenant: "a,b", wantErr: "separator"},
		{name: "a slash", tenant: "a/b", wantErr: "separator"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTenantID(tt.tenant)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestResolveTenantID(t *testing.T) {
	tenants := []qumulo.TenantInfo{
		{ID: 1, Name: "Default", NFSEnabled: true},
		{ID: 4, Name: "tenant-finance", NFSEnabled: true},
		{ID: 7, Name: "smb-only", NFSEnabled: false},
	}

	tests := []struct {
		name     string
		tenant   string
		want     int
		wantErr  string
		wantList bool // whether the cluster should have been consulted
	}{
		{
			// Unset must send no tenant at all, so the cluster applies its own
			// default. That is what every pre-existing export already got.
			name:   "unset resolves to zero and consults nothing",
			tenant: "", want: 0,
		},
		{
			// The id is used as written, but the listing is still made: not
			// to confirm the tenant exists -- the cluster refuses an unknown
			// one itself -- but to catch one that exists with NFS off. That
			// is the property no error from the cluster would name.
			name: "an integer is used as written", tenant: "4", want: 4, wantList: true,
		},
		{
			// Unknown to the listing, so there is no NFS flag to object to.
			// Passed through, and the cluster refuses it by name.
			name:   "an integer that does not exist is still passed through",
			tenant: "99", want: 99, wantList: true,
		},
		{name: "a name is resolved", tenant: "tenant-finance", want: 4, wantList: true},
		{name: "name matching is case-insensitive", tenant: "TENANT-FINANCE", want: 4, wantList: true},
		{
			name:   "an unknown name lists what does exist",
			tenant: "tenant-nope", wantErr: "no tenant named", wantList: true,
		},
		{
			// An export here would be created and then serve nothing.
			name:   "a tenant with NFS disabled is refused",
			tenant: "smb-only", wantErr: "has NFS disabled", wantList: true,
		},
		{
			// The same tenant, written as its id. A number is taken at face
			// value and never listed, so the NFS-disabled check that the
			// name branch performs is skipped entirely: the export is
			// created, the PVC binds, and the mount serves nothing.
			//
			// "The cluster is the authority on existence" is a fair reason
			// not to look up whether a tenant EXISTS. It is not a reason to
			// skip a property of it the driver refuses on when it is spelled
			// the other way. Same tenant, same outcome, either spelling.
			name:   "a tenant with NFS disabled is refused by id as well as by name",
			tenant: "7", wantErr: "has NFS disabled", wantList: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var listed bool
			client := &MockClient{
				ListTenantsFunc: func(ctx context.Context) ([]qumulo.TenantInfo, error) {
					listed = true
					return tenants, nil
				},
			}
			srv := newTestControllerServer(t, client)

			got, err := srv.resolveTenantID(context.Background(), client, tt.tenant)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			assert.Equal(t, tt.wantList, listed,
				"whether the cluster was consulted decides the per-provision cost")
		})
	}
}

// An unknown name must name the tenants that do exist: the operator's next
// action is to pick one, and making them go and look it up is a wasted round
// trip through a human.
// The listing is consulted for an id to catch NFS-disabled, not to decide
// whether the tenant exists -- so a listing that cannot be made must not
// block the provision. A name has no such fallback: it cannot be sent to the
// export API unresolved.
func TestResolveTenantIDUsesAnIDAsWrittenWhenTheTenantListCannotBeRead(t *testing.T) {
	client := &MockClient{
		ListTenantsFunc: func(context.Context) ([]qumulo.TenantInfo, error) {
			return nil, fakeAPIError(http.StatusForbidden, "http_forbidden_error", "no privilege")
		},
	}
	srv := newTestControllerServer(t, client)

	got, err := srv.resolveTenantID(context.Background(), client, "4")
	require.NoError(t, err)
	assert.Equal(t, 4, got)

	_, err = srv.resolveTenantID(context.Background(), client, "tenant-finance")
	require.Error(t, err, "a name cannot be resolved without the listing")
}

func TestResolveTenantIDErrorNamesTheAlternatives(t *testing.T) {
	client := &MockClient{
		ListTenantsFunc: func(ctx context.Context) ([]qumulo.TenantInfo, error) {
			return []qumulo.TenantInfo{
				{ID: 1, Name: "Default", NFSEnabled: true},
				{ID: 4, Name: "tenant-finance", NFSEnabled: true},
			}, nil
		},
	}
	srv := newTestControllerServer(t, client)

	_, err := srv.resolveTenantID(context.Background(), client, "typo")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Default (id 1)")
	assert.Contains(t, err.Error(), "tenant-finance (id 4)")
}

// The load-bearing rule: neither parameter may be silently dropped on a class
// that creates no export of its own.
func TestExportScopingRequiresDedicatedExport(t *testing.T) {
	tests := []struct {
		name    string
		extra   map[string]string
		wantErr string
	}{
		{
			name:    "allowedClients under the shared model",
			extra:   map[string]string{"allowedClients": "10.0.0.5"},
			wantErr: "allowedclients requires provisioningmodel=dedicated-export",
		},
		{
			name:    "tenantID under the shared model",
			extra:   map[string]string{"tenantID": "4"},
			wantErr: "tenantid requires provisioningmodel=dedicated-export",
		},
		{
			name:    "both named in one message",
			extra:   map[string]string{"tenantID": "4", "allowedClients": "10.0.0.5"},
			wantErr: "tenantid and allowedclients requires",
		},
		{
			name: "both are fine with a dedicated export",
			extra: map[string]string{
				"tenantID": "4", "allowedClients": "10.0.0.5",
				"provisioningModel": "dedicated-export", "allowAnyClient": "true",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]string{
				"server":          testServer,
				"storeRealPath":   "/csi-volumes",
				"storeExportPath": "/csi",
			}
			for k, v := range tt.extra {
				params[k] = v
			}

			_, err := parseVolumeParams(params)

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// End to end: what reaches the cluster, and what lands on the PV.
func TestCreateVolumeAppliesTenantAndAllowedClients(t *testing.T) {
	var gotSpec qumulo.ExportSpec

	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		ListTenantsFunc: func(ctx context.Context) ([]qumulo.TenantInfo, error) {
			return []qumulo.TenantInfo{{ID: 4, Name: "tenant-finance", NFSEnabled: true}}, nil
		},
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			gotSpec = spec
			return qumulo.ExportInfo{ID: "9", TenantID: spec.TenantID}, nil
		},
	})

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-tenant",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":            testServer,
			"storeRealPath":     "/csi-volumes",
			"storeExportPath":   "/csi",
			"provisioningModel": "dedicated-export", "allowAnyClient": "true",
			"tenantID":       "tenant-finance",
			"allowedClients": "10.0.0.5,192.168.1.0/24",
		},
	})
	require.NoError(t, err)

	// What was committed to the cluster.
	assert.Equal(t, 4, gotSpec.TenantID, "the name must be resolved to its integer id")
	assert.Equal(t, []string{"10.0.0.5", "192.168.1.0/24"}, gotSpec.AllowedClients)

	// What a `kubectl get pv -o yaml` will show.
	volumeContext := resp.GetVolume().GetVolumeContext()
	assert.Equal(t, "tenant-finance", volumeContext[volumeContextTenantID],
		"the reference as written, so the PV matches the StorageClass")
	assert.Equal(t, "10.0.0.5,192.168.1.0/24", volumeContext[volumeContextAllowedClients])
}

// Neither key may appear when unset: an empty string in the volume context is
// indistinguishable from "restricted to nothing" to a later reader.
func TestCreateVolumeOmitsScopingKeysWhenUnset(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	})

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-plain",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
		},
	})
	require.NoError(t, err)

	vc := resp.GetVolume().GetVolumeContext()
	assert.NotContains(t, vc, volumeContextTenantID)
	assert.NotContains(t, vc, volumeContextAllowedClients)
}

// A tenant the cluster does not have is the operator's mistake, and no retry
// fixes it -- so it must not come back as a retriable Internal.
func TestCreateVolumeMapsTenantNotFoundToInvalidArgument(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			return qumulo.ExportInfo{}, &qumulo.APIError{
				StatusCode: http.StatusBadRequest,
				Class:      qumulo.ErrorClassTenantNotFound,
				Path:       "/v3/nfs/exports/",
			}
		},
	})

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-bad-tenant",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
			"provisioningModel": "dedicated-export", "allowAnyClient": "true", "tenantID": "99",
		},
	})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "no such tenant")
}

// An unset tenant must send zero, which the client turns into an omitted field
// so the cluster applies its own default. Sending 0 would be rejected outright.
//
// This used to also assert that an unset tenant never lists tenants. The
// multi-tenant preflight reversed that for dedicated-export: the preflight now lists them once to
// refuse a multi-tenant cluster before anything is created. A single-tenant
// cluster, as here, still gets no tenant_id.
func TestCreateVolumeSendsNoTenantWhenUnset(t *testing.T) {
	var gotSpec qumulo.ExportSpec

	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		ListTenantsFunc: func(ctx context.Context) ([]qumulo.TenantInfo, error) {
			return []qumulo.TenantInfo{{ID: 1, Name: "Default", NFSEnabled: true}}, nil
		},
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			gotSpec = spec
			return qumulo.ExportInfo{ID: "9"}, nil
		},
	})

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-default-tenant",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
			"provisioningModel": "dedicated-export", "allowAnyClient": "true",
		},
	})
	require.NoError(t, err)

	assert.Zero(t, gotSpec.TenantID, "zero is what makes the client omit tenant_id")
	assert.Empty(t, gotSpec.AllowedClients, "an unset allowlist must stay unrestricted")
}

// dedicatedNoTenantRequest is a dedicated-export class that names no tenant:
// the class the multi-tenant refusal is about.
func dedicatedNoTenantRequest(name string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               name,
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
			"provisioningModel": "dedicated-export", "allowAnyClient": "true",
		},
	}
}

// On a cluster with more than one tenant, a dedicated-export
// class with no tenantID can never get its export. It must be refused before
// the directory and quota exist, with a message that names the fix.
func TestCreateVolumeRefusesAnUnsetTenantOnAMultiTenantClusterBeforeCreatingAnything(t *testing.T) {
	var created bool
	srv := newTestControllerServer(t, &MockClient{
		ListTenantsFunc: func(ctx context.Context) ([]qumulo.TenantInfo, error) {
			return []qumulo.TenantInfo{
				{ID: 1, Name: "Default", NFSEnabled: true},
				{ID: 2, Name: "krb-tenant", NFSEnabled: true},
			}, nil
		},
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			created = true
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	})

	_, err := srv.CreateVolume(context.Background(), dedicatedNoTenantRequest("pvc-no-tenant"))

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	msg := status.Convert(err).Message()
	assert.Contains(t, msg, paramTenantID, "the message must name the parameter to set")
	assert.Contains(t, msg, "krb-tenant (id 2)", "the message must list the tenants to choose from")
	assert.False(t, created, "nothing may be created for a class that can never get its export")
}

// When the preflight cannot read the tenant list, the
// cluster's own refusal is the backstop, and it must not come back as a
// retriable Internal with no hint of the fix.
func TestCreateVolumeMapsEmptyTenantInMultitenantToFailedPrecondition(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		ListTenantsFunc: func(ctx context.Context) ([]qumulo.TenantInfo, error) {
			return nil, fakeAPIError(http.StatusForbidden, "http_forbidden_error", "no privilege")
		},
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			return qumulo.ExportInfo{}, &qumulo.APIError{
				StatusCode: http.StatusBadRequest,
				Class:      qumulo.ErrorClassEmptyTenantInMultitenant,
				Msg:        "Tenant must be specified when more than one tenant configured.",
				Path:       "/v3/nfs/exports/",
			}
		},
	})

	_, err := srv.CreateVolume(context.Background(), dedicatedNoTenantRequest("pvc-no-tenant"))

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), paramTenantID)
}

// The check is for classes that create an export. A shared-subdirectory class
// creates none, so it must not pay for a tenant listing.
func TestASharedSubdirectoryClassDoesNotListTenants(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		ListTenantsFunc: func(ctx context.Context) ([]qumulo.TenantInfo, error) {
			t.Fatal("a class that creates no export must not list tenants")
			return nil, nil
		},
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	})

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-shared",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         testParams(),
	})
	require.NoError(t, err)
}

// A tenant name that cannot be resolved because the listing failed is
// an outage, not a wrong class: the provisioner has to be told to retry. Only
// a listing the array answered and refused, or a name it does not know, is
// the class's fault.
func TestCreateVolumeReportsAFailedTenantListingByCause(t *testing.T) {
	for _, tc := range []struct {
		name    string
		listErr error
		tenants []qumulo.TenantInfo
		want    codes.Code
	}{
		{"timeout", fmt.Errorf("calling /v1/multitenancy/tenants/: %w", &url.Error{
			Op: "Get", URL: "https://192.0.2.1:8000/v1/multitenancy/tenants/", Err: context.DeadlineExceeded,
		}), nil, codes.Unavailable},
		{"unreadable answer", errors.New("decoding response from /v1/multitenancy/tenants/: unexpected EOF"), nil, codes.InvalidArgument},
		{"server error", fakeAPIError(http.StatusServiceUnavailable, "http_service_unavailable_error", "busy"), nil, codes.Unavailable},
		// Throttled, or timed out on the array's side: try again later, not
		// a statement about the class (residual).
		{"throttled", fakeAPIError(http.StatusTooManyRequests, "http_too_many_requests_error", "slow down"), nil, codes.Unavailable},
		{"request timeout", fakeAPIError(http.StatusRequestTimeout, "http_request_timeout_error", "timed out"), nil, codes.Unavailable},
		{"refused", fakeAPIError(http.StatusForbidden, "http_forbidden_error", "no privilege"), nil, codes.InvalidArgument},
		{"no such name", nil, []qumulo.TenantInfo{{ID: 1, Name: "Default", NFSEnabled: true}}, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			created := false
			srv := newTestControllerServer(t, &MockClient{
				ListTenantsFunc: func(context.Context) ([]qumulo.TenantInfo, error) {
					return tc.tenants, tc.listErr
				},
				CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
					created = true
					return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
				},
			})

			_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Secrets:            testSecrets(),
				Name:               "pvc-tenant-listing",
				VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
				CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
				Parameters: map[string]string{
					"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
					"provisioningModel": "dedicated-export", "allowAnyClient": "true", "tenantID": "tenant-finance",
				},
			})

			require.Error(t, err)
			assert.Equal(t, tc.want, status.Code(err), status.Convert(err).Message())
			assert.False(t, created, "nothing may be created before the tenant resolves")
		})
	}
}
