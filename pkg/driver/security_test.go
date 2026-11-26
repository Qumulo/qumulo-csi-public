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

func TestValidateSecurityFlavor(t *testing.T) {
	for _, ok := range []string{"sys", "krb5", "krb5i", "krb5p"} {
		t.Run("accepts "+ok, func(t *testing.T) {
			require.NoError(t, validateSecurityFlavor(ok))
		})
	}
	for _, bad := range []string{"KRB5", "krb", "krb5x", "none", "sec=krb5p", " krb5p", "krb5p "} {
		t.Run("refuses "+bad, func(t *testing.T) {
			require.Error(t, validateSecurityFlavor(bad), "%q must be refused", bad)
		})
	}
}

// The flavour must reach the cluster as the enum it accepts. AUTHENTICATION_MODE_SYS
// does not exist -- probed on Core 7.9.3 -- so sys maps to None, "require nothing".
func TestExportAuthModeMapping(t *testing.T) {
	assert.Equal(t, qumulo.AuthenticationModeNone, exportAuthModes["sys"])
	assert.Equal(t, qumulo.AuthenticationModeKrb5, exportAuthModes["krb5"])
	assert.Equal(t, qumulo.AuthenticationModeKrb5i, exportAuthModes["krb5i"])
	assert.Equal(t, qumulo.AuthenticationModeKrb5p, exportAuthModes["krb5p"])

	for _, f := range securityFlavors {
		assert.NotEmpty(t, exportAuthModes[f], "every accepted flavour needs a cluster enum")
	}
}

// The two combinations that cannot work must be refused, not ignored: an
// operator who believes traffic is encrypted when it is not is worse off than
// one told plainly that this class cannot do it.
func TestSecurityFlavorScoping(t *testing.T) {
	tests := []struct {
		name    string
		extra   map[string]string
		wantErr string
	}{
		{
			name:  "krb5p with a dedicated export on nfs4.1",
			extra: map[string]string{"securityFlavor": "krb5p", "provisioningModel": "dedicated-export", "allowAnyClient": "true", "protocol": "nfs4.1"},
		},
		{
			// sys is the default behaviour, so it needs no dedicated export.
			name:  "sys under the shared model is fine",
			extra: map[string]string{"securityFlavor": "sys"},
		},
		{
			name:    "krb5 under the shared model",
			extra:   map[string]string{"securityFlavor": "krb5"},
			wantErr: "requires provisioningmodel=dedicated-export",
		},
		{
			name:    "krb5i on nfs3",
			extra:   map[string]string{"securityFlavor": "krb5i", "provisioningModel": "dedicated-export", "allowAnyClient": "true", "protocol": "nfs3"},
			wantErr: "requires protocol=nfs4.1",
		},
		{
			name:    "an unknown flavour",
			extra:   map[string]string{"securityFlavor": "krb9"},
			wantErr: "must be one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]string{
				"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
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

func TestMountSecurityOptionFor(t *testing.T) {
	tests := []struct {
		name   string
		flavor string
		want   string
	}{
		// Unset and sys both produce nothing: NFSv4.1 negotiates sec=sys
		// unasked, so emitting it would change the mount line of every volume
		// provisioned before this parameter existed.
		{name: "unset", flavor: "", want: ""},
		{name: "sys", flavor: "sys", want: ""},
		{name: "krb5", flavor: "krb5", want: "sec=krb5"},
		{name: "krb5i", flavor: "krb5i", want: "sec=krb5i"},
		{name: "krb5p", flavor: "krb5p", want: "sec=krb5p"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mountSecurityOptionFor(map[string]string{volumeContextSecurityFlavor: tt.flavor})
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A hand-authored PV can carry anything. Refuse it here rather than hand an
// unknown token to mount(8), which fails far less legibly.
func TestMountSecurityOptionRejectsGarbage(t *testing.T) {
	_, err := mountSecurityOptionFor(map[string]string{volumeContextSecurityFlavor: "krb5; rm -rf /"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), volumeContextSecurityFlavor)
}

// krb5, krb5i and krb5p are three separate switches on the cluster, and
// checkSecurityFlavorSupported consults only the first. A cluster with
// Kerberos on but krb5p off takes the provision, creates the export demanding
// AUTHENTICATION_MODE_KRB5P, and binds the PVC -- and then no client can
// mount it, which surfaces as a pod stuck in ContainerCreating rather than as
// anything naming the class.
//
// The whole point of the preflight is to refuse here instead.
func TestCreateVolumeRefusesAKerberosFlavorTheClusterHasOff(t *testing.T) {
	for _, flavor := range []string{"krb5p", "krb5i"} {
		t.Run(flavor, func(t *testing.T) {
			var created bool
			srv := newTestControllerServer(t, &MockClient{
				CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
				},
				GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
					// Kerberos is on; this particular flavor is not.
					return qumulo.NFSSettings{
						V4Enabled: true, Krb5Enabled: true,
						Krb5pEnabled: false, Krb5iEnabled: false,
						IdmapDomain: "corp.example.com",
					}, nil
				},
				CreateExportFunc: func(context.Context, qumulo.ExportSpec) (qumulo.ExportInfo, error) {
					created = true
					return qumulo.ExportInfo{ID: "9"}, nil
				},
			})

			_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:               "pvc-krb",
				VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
				CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
				Secrets:            testSecrets(),
				Parameters: map[string]string{
					"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
					"provisioningModel": "dedicated-export", "allowAnyClient": "true", "protocol": "nfs4.1",
					"securityFlavor": flavor,
				},
			})

			require.Error(t, err, "the volume was provisioned against a flavor the cluster refuses")
			assert.Contains(t, err.Error(), flavor)
			assert.False(t, created, "an unmountable export was created")
		})
	}
}

// auth_sys_enabled is read off the cluster and stored, and then nothing ever
// looks at it. A class with securityFlavor sys provisions against a cluster
// with AUTH_SYS off, and the pod cannot mount.
func TestCreateVolumeRefusesSysOnAClusterWithAuthSysOff(t *testing.T) {
	var created bool
	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(context.Context, string, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetNFSSettingsFunc: func(context.Context) (qumulo.NFSSettings, error) {
			return qumulo.NFSSettings{V4Enabled: true, AuthSysEnabled: false}, nil
		},
		CreateExportFunc: func(context.Context, qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			created = true
			return qumulo.ExportInfo{ID: "9"}, nil
		},
	})

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-sys",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Secrets:            testSecrets(),
		Parameters: map[string]string{
			"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
			"provisioningModel": "dedicated-export", "allowAnyClient": "true", "securityFlavor": "sys",
		},
	})

	require.Error(t, err, "the volume was provisioned for a mount the cluster will refuse")
	assert.False(t, created, "an unmountable export was created")
}

// End to end: what the export demands, and what the PV tells the node.
func TestCreateVolumeAppliesSecurityFlavorToTheExport(t *testing.T) {
	var gotSpec qumulo.ExportSpec

	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetNFSSettingsFunc: func(ctx context.Context) (qumulo.NFSSettings, error) {
			return qumulo.NFSSettings{
				V4Enabled: true, Krb5Enabled: true, Krb5pEnabled: true,
				IdmapDomain: "corp.example.com",
			}, nil
		},
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			gotSpec = spec
			return qumulo.ExportInfo{ID: "9"}, nil
		},
	})

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-krb",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Secrets:            testSecrets(),
		Parameters: map[string]string{
			"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
			"provisioningModel": "dedicated-export", "allowAnyClient": "true", "protocol": "nfs4.1",
			"securityFlavor": "krb5p",
		},
	})
	require.NoError(t, err)

	assert.Equal(t, qumulo.AuthenticationModeKrb5p, gotSpec.AuthenticationMode,
		"the export must DEMAND the flavour, or the protocol negotiates back down to sys")
	assert.Equal(t, "krb5p", resp.GetVolume().GetVolumeContext()[volumeContextSecurityFlavor],
		"the node needs it to mount with the matching sec= option")
}

// sys must leave both halves exactly as they were before the parameter existed.
func TestCreateVolumeSysFlavorChangesNothing(t *testing.T) {
	var gotSpec qumulo.ExportSpec

	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		CreateExportFunc: func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			gotSpec = spec
			return qumulo.ExportInfo{ID: "9"}, nil
		},
	})

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-sys",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Secrets:            testSecrets(),
		Parameters: map[string]string{
			"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
			"provisioningModel": "dedicated-export", "allowAnyClient": "true", "securityFlavor": "sys",
		},
	})
	require.NoError(t, err)

	assert.Equal(t, qumulo.AuthenticationModeNone, gotSpec.AuthenticationMode)
	assert.NotContains(t, resp.GetVolume().GetVolumeContext(), volumeContextSecurityFlavor,
		"sys must leave the key absent, so a later reader cannot mistake it for a deliberate choice")
}

// The two cluster preconditions. Both fail the provision: a security control
// that might not be in force is not something to be optimistic about.
func TestCreateVolumeRefusesKerberosTheClusterCannotServe(t *testing.T) {
	tests := []struct {
		name     string
		settings qumulo.NFSSettings
		wantErr  string
	}{
		{
			name:     "kerberos disabled on the cluster",
			settings: qumulo.NFSSettings{V4Enabled: true, Krb5Enabled: false, IdmapDomain: "corp.example.com"},
			wantErr:  "krb5_enabled false",
		},
		{
			// The quiet one: the mount succeeds and then every principal is
			// nobody, which is far harder to diagnose from a pod.
			//
			// Krb5iEnabled is set because this row is about the idmap domain
			// and nothing else. Left false it models a cluster that refuses
			// krb5i outright, and the flavour check would refuse first -- a
			// pass for the wrong reason.
			name: "no idmap domain",
			settings: qumulo.NFSSettings{
				V4Enabled: true, Krb5Enabled: true, Krb5iEnabled: true, IdmapDomain: "",
			},
			wantErr: "idmap_domain",
		},
		{
			// Kerberos is on and this flavour is not. krb5_enabled alone
			// would have let this through.
			name: "the flavour itself is off",
			settings: qumulo.NFSSettings{
				V4Enabled: true, Krb5Enabled: true, Krb5iEnabled: false,
				IdmapDomain: "corp.example.com",
			},
			wantErr: "krb5i_enabled false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestControllerServer(t, &MockClient{
				GetNFSSettingsFunc: func(ctx context.Context) (qumulo.NFSSettings, error) {
					return tt.settings, nil
				},
				CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
					t.Fatal("nothing may be created when the cluster cannot serve the flavour")
					return qumulo.FileInfo{}, nil
				},
			})

			_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:               "pvc-krb-bad",
				VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
				CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
				Secrets:            testSecrets(),
				Parameters: map[string]string{
					"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
					"provisioningModel": "dedicated-export", "allowAnyClient": "true", "protocol": "nfs4.1",
					"securityFlavor": "krb5i",
				},
			})

			require.Error(t, err)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantErr)
		})
	}
}
