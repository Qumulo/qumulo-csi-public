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
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// openExportRefusal refuses only a dedicated-export class that is open:
// no allowedClients, an AUTH_SYS flavour, and no allowAnyClient.
func TestOpenExportRefusal(t *testing.T) {
	for _, tt := range []struct {
		name   string
		extra  map[string]string
		refuse bool
	}{
		{"open dedicated export", map[string]string{"provisioningModel": "dedicated-export"}, true},
		{"open dedicated export, sys flavour", map[string]string{"provisioningModel": "dedicated-export", "securityFlavor": "sys"}, true},
		{"allowAnyClient false is still open", map[string]string{"provisioningModel": "dedicated-export", "allowAnyClient": "false"}, true},
		{"allowedClients", map[string]string{"provisioningModel": "dedicated-export", "allowedClients": "10.0.0.0/24"}, false},
		{"allowAnyClient", map[string]string{"provisioningModel": "dedicated-export", "allowAnyClient": "TRUE"}, false},
		{"krb5p", map[string]string{"provisioningModel": "dedicated-export", "securityFlavor": "krb5p", "protocol": "nfs4.1"}, false},
		{"shared subdirectory: the export is the operator's", map[string]string{}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := testParams()
			for k, v := range tt.extra {
				params[k] = v
			}
			parsed, err := parseVolumeParams(params)
			require.NoError(t, err)
			err = openExportRefusal(parsed)
			if !tt.refuse {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, way := range []string{"allowedClients", "securityFlavor", "allowAnyClient", "re-create the class"} {
				assert.Contains(t, err.Error(), way, "the refusal must name every way out")
			}
		})
	}
}

func TestAllowAnyClientMustBeABoolean(t *testing.T) {
	params := testParams()
	params["provisioningModel"] = "dedicated-export"
	params["allowAnyClient"] = "yes"
	_, err := parseVolumeParams(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "allowanyclient must be \"true\" or \"false\"")
}

// openDedicatedRequest is a dedicated-export class with no access scoping.
func openDedicatedRequest() *csi.CreateVolumeRequest {
	params := testParams()
	params["provisioningModel"] = "dedicated-export"
	return &csi.CreateVolumeRequest{
		Name: "pvc-open", Parameters: params, Secrets: testSecrets(),
		VolumeCapabilities: mountCapabilities(),
	}
}

// The refusal comes before the array is called at all, so nothing is left to
// roll back.
func TestCreateVolumeRefusesAnOpenDedicatedClassBeforeCallingTheArray(t *testing.T) {
	client := &MockClient{
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			t.Error("CreateDirectory called for a refused class")
			return qumulo.FileInfo{}, nil
		},
		ListTenantsFunc: func(context.Context) ([]qumulo.TenantInfo, error) {
			t.Error("ListTenants called for a refused class")
			return nil, nil
		},
	}
	_, err := newTestControllerServer(t, client).CreateVolume(context.Background(), openDedicatedRequest())
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	// Asserted on the refusal's own wording, not on an internal defect ID.
	assert.Contains(t, status.Convert(err).Message(), "set allowedClients to your nodes' addresses")
}

// -require-allowed-clients=false (the chart's requireAllowedClients: false)
// restores the old behaviour.
func TestCreateVolumeAcceptsAnOpenDedicatedClassWhenTheCheckIsOff(t *testing.T) {
	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "0.1.0",
		NewClient:                 func(string, string) qumulo.ClientInterface { return &MockClient{} },
		AllowOpenDedicatedExports: true,
	})
	require.NoError(t, err)
	// The bare mock fails a later step; only the refusal matters here.
	_, err = NewControllerServer(d).CreateVolume(context.Background(), openDedicatedRequest())
	if err != nil {
		assert.NotContains(t, status.Convert(err).Message(), "set allowedClients to your nodes' addresses",
			"refused with the check off")
	}
}

// Provision-time only: the inventory and GetCapacity still read an open
// class, so existing volumes of one keep their capacity and listing.
func TestAnOpenDedicatedClassStillParses(t *testing.T) {
	params := testParams()
	params["provisioningModel"] = "dedicated-export"
	_, err := parseVolumeParams(params)
	assert.NoError(t, err)
}
