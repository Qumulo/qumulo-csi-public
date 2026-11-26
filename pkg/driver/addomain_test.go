// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

func TestValidateADDomain(t *testing.T) {
	tests := []struct {
		name    string
		domain  string
		wantErr string
	}{
		{name: "a two-label domain", domain: "corp.example.com"},
		{name: "the minimum, two labels", domain: "example.com"},
		{name: "upper case, as AD names are usually written", domain: "CORP.EXAMPLE.COM"},
		{name: "mixed case", domain: "Corp.Example.Com"},
		{name: "interior hyphens", domain: "my-corp.east-1.example.com"},
		{name: "digits", domain: "ad1.example2.com"},
		{name: "many labels", domain: "a.b.c.d.e.example.com"},

		{name: "empty is not reached here but must not panic", domain: "", wantErr: "fully-qualified"},
		{
			// The single most likely mistake: an AD short name.
			name: "a bare hostname", domain: "corp",
			wantErr: "fully-qualified domain name",
		},
		{name: "a trailing dot", domain: "corp.example.com.", wantErr: "fully-qualified"},
		{name: "a leading dot", domain: ".corp.example.com", wantErr: "fully-qualified"},
		{name: "an empty label", domain: "corp..example.com", wantErr: "not a path"},
		{name: "a leading hyphen", domain: "-corp.example.com", wantErr: "fully-qualified"},
		{name: "a trailing hyphen", domain: "corp-.example.com", wantErr: "fully-qualified"},
		{name: "an underscore", domain: "corp_ad.example.com", wantErr: "fully-qualified"},
		{name: "a space", domain: "corp ad.example.com", wantErr: "fully-qualified"},

		// Path traversal. These are the reason the value is validated at all:
		// it becomes a directory name on shared storage.
		{name: "parent traversal", domain: "../etc", wantErr: "not a path"},
		{name: "traversal deeper in", domain: "corp.example.com/../../etc", wantErr: "not a path"},
		{name: "an absolute path", domain: "/etc/passwd", wantErr: "not a path"},
		{name: "a bare separator", domain: "a/b", wantErr: "not a path"},
		{name: "a backslash", domain: `corp\example`, wantErr: "not a path"},
		{name: "just dots", domain: "..", wantErr: "not a path"},

		{name: "leading whitespace", domain: " corp.example.com", wantErr: "whitespace"},
		{name: "trailing whitespace", domain: "corp.example.com\n", wantErr: "whitespace"},
		{
			name:    "longer than a DNS name can be",
			domain:  strings.Repeat("a", 250) + ".com",
			wantErr: "cannot exceed 253",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateADDomain(tt.domain)

			if tt.wantErr == "" {
				require.NoError(t, err, "%q is a valid domain and must be accepted", tt.domain)
				return
			}
			require.Error(t, err, "%q must be rejected", tt.domain)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// A rejected domain must reach the CO as InvalidArgument, not Internal: no
// retry will fix a malformed StorageClass, and the code is what tells the
// provisioner to stop retrying.
func TestCreateVolumeRejectsAnInvalidADDomainAsInvalidArgument(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			t.Fatalf("nothing may be created on the cluster when the domain is invalid, "+
				"but CreateDirectory(%q, %q) was called", parentPath, name)
			return qumulo.FileInfo{}, nil
		},
	})

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-bad-domain",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":          testServer,
			"storeRealPath":   "/csi-volumes",
			"storeExportPath": "/csi",
			"adDomain":        "not a domain",
		},
	})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	// The normalized (lower-case) key, matching how every other parameter
	// names itself in an error.
	assert.Contains(t, status.Convert(err).Message(), paramADDomain)
}

// The two path helpers must nest identically. A domain applied to the real
// path but not the export path provisions cleanly and then fails to mount,
// which is the worst possible failure mode for this parameter.
func TestADDomainNestsBothPathsInStep(t *testing.T) {
	tests := []struct {
		name       string
		domain     string
		wantReal   string
		wantExport string
	}{
		{
			name:       "unset keeps the flat layout every existing volume uses",
			domain:     "",
			wantReal:   "/csi-volumes/pvc-1",
			wantExport: "/csi/pvc-1",
		},
		{
			name:       "set nests one level under both roots",
			domain:     "corp.example.com",
			wantReal:   "/csi-volumes/corp.example.com/pvc-1",
			wantExport: "/csi/corp.example.com/pvc-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := volumeParams{
				StoreRealPath:   "/csi-volumes",
				StoreExportPath: "/csi",
				ADDomain:        tt.domain,
			}

			assert.Equal(t, tt.wantReal, params.realPathFor("pvc-1"))
			assert.Equal(t, tt.wantExport, params.exportPathFor("pvc-1"))
		})
	}
}

// End to end through CreateVolume: the directory must be created under the
// nested root, and the response must describe the nested paths. Anything less
// and the node mounts the wrong place.
func TestCreateVolumeProvisionsUnderTheDomainDirectory(t *testing.T) {
	var created [][2]string

	srv := newTestControllerServer(t, &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			// The domain directory does not exist yet, which is the first
			// provision for a new domain.
			return qumulo.FileInfo{}, apiError(http.StatusNotFound)
		},
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			created = append(created, [2]string{parentPath, name})
			return qumulo.FileInfo{
				ID:   "12884901888",
				Path: parentPath + "/" + name,
				Type: "FS_FILE_TYPE_DIRECTORY",
			}, nil
		},
	})

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-domain",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":          testServer,
			"storeRealPath":   "/csi-volumes",
			"storeExportPath": "/csi",
			"adDomain":        "corp.example.com",
		},
	})
	require.NoError(t, err)

	// The domain directory first, then the volume inside it.
	require.Equal(t, [][2]string{
		{"/csi-volumes", "corp.example.com"},
		{"/csi-volumes/corp.example.com", "pvc-domain"},
	}, created)

	volumeContext := resp.GetVolume().GetVolumeContext()
	assert.Equal(t, "corp.example.com", volumeContext[volumeContextADDomain],
		"the node must be told which domain the volume belongs to")
	assert.Equal(t, "/csi/corp.example.com/pvc-domain", volumeContext[volumeContextExportPath])
	assert.Equal(t, "/csi-volumes/corp.example.com/pvc-domain", volumeContext[volumeContextRealPath])
}

// The fallback path: no domain must behave exactly as it did before the
// parameter existed, including creating no extra directory.
func TestCreateVolumeWithoutADDomainIsUnchanged(t *testing.T) {
	var created [][2]string

	srv := newTestControllerServer(t, &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			t.Fatalf("no domain means no domain directory to look up, but %q was looked up", p)
			return qumulo.FileInfo{}, nil
		},
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			created = append(created, [2]string{parentPath, name})
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	})

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-flat",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":          testServer,
			"storeRealPath":   "/csi-volumes",
			"storeExportPath": "/csi",
		},
	})
	require.NoError(t, err)

	assert.Equal(t, [][2]string{{"/csi-volumes", "pvc-flat"}}, created,
		"only the volume itself may be created")
	assert.NotContains(t, resp.GetVolume().GetVolumeContext(), volumeContextADDomain,
		"an unset domain must leave the key absent rather than empty")
}

func TestEnsureADDomainDirectory(t *testing.T) {
	tests := []struct {
		name       string
		domain     string
		lookupErr  error
		createErr  error
		wantCreate bool
		wantErr    string
	}{
		{
			name:   "no domain does nothing",
			domain: "",
		},
		{
			// The common case: every provision after the first.
			name:       "an existing directory is adopted",
			domain:     "corp.example.com",
			lookupErr:  nil,
			wantCreate: false,
		},
		{
			name:       "a missing directory is created",
			domain:     "corp.example.com",
			lookupErr:  apiError(http.StatusNotFound),
			wantCreate: true,
		},
		{
			// Two provisions for a new domain racing each other. Both wanted
			// the directory to exist; it now does.
			name:       "losing a create race is success",
			domain:     "corp.example.com",
			lookupErr:  apiError(http.StatusNotFound),
			createErr:  &qumulo.APIError{StatusCode: http.StatusConflict, Class: "fs_entry_exists_error", Path: "/test"},
			wantCreate: true,
		},
		{
			name:       "a real create failure is reported",
			domain:     "corp.example.com",
			lookupErr:  apiError(http.StatusNotFound),
			createErr:  apiError(http.StatusForbidden),
			wantCreate: true,
			wantErr:    "creating the domain directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var createCalled bool

			client := &MockClient{
				LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
					if tt.domain == "" {
						t.Fatalf("no domain means no lookup, but %q was looked up", p)
					}
					assert.Equal(t, "/csi-volumes/corp.example.com", p)
					return qumulo.FileInfo{ID: "1"}, tt.lookupErr
				},
				CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
					createCalled = true
					assert.Equal(t, "/csi-volumes", parentPath)
					assert.Equal(t, "corp.example.com", name)
					return qumulo.FileInfo{ID: "2"}, tt.createErr
				},
			}
			srv := newTestControllerServer(t, client)

			err := srv.ensureADDomainDirectory(context.Background(), client, volumeParams{
				StoreRealPath: "/csi-volumes",
				ADDomain:      tt.domain,
			})

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
			assert.Equal(t, tt.wantCreate, createCalled)
		})
	}
}

// A failure to create the domain directory must abort the provision rather
// than fall back to the flat layout: a volume created outside its domain is a
// volume in the wrong place, and the CO would treat it as a success.
func TestCreateVolumeFailsWhenTheDomainDirectoryCannotBeCreated(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, apiError(http.StatusNotFound)
		},
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			if name == "corp.example.com" {
				return qumulo.FileInfo{}, apiError(http.StatusForbidden)
			}
			t.Fatalf("the volume must not be created after the domain directory failed, "+
				"but CreateDirectory(%q, %q) was called", parentPath, name)
			return qumulo.FileInfo{}, nil
		},
	})

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-no-domain-dir",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":          testServer,
			"storeRealPath":   "/csi-volumes",
			"storeExportPath": "/csi",
			"adDomain":        "corp.example.com",
		},
	})

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "domain directory")
}

// A restore stages under `.csi-restoring-<name>` and renames into place. Both
// must happen inside the domain directory: staging outside it and renaming in
// would be a cross-directory rename, and staging in the flat root would leave
// the finished volume where the node does not look for it.
func TestADDomainNestsARestoreStagingPath(t *testing.T) {
	tests := []struct {
		name   string
		domain string
		want   string
	}{
		{name: "flat", domain: "", want: "/csi-volumes/.csi-restoring-pvc-1"},
		{
			name:   "nested",
			domain: "corp.example.com",
			want:   "/csi-volumes/corp.example.com/.csi-restoring-pvc-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := volumeParams{
				StoreRealPath:   "/csi-volumes",
				StoreExportPath: "/csi",
				ADDomain:        tt.domain,
			}

			assert.Equal(t, tt.want, restoreTempPathFor(params, "pvc-1"))
		})
	}
}

// The domain directory must be created for a restore too, not only for an
// empty provision -- the restore branch returns before the normal create path.
func TestCreateVolumeFromSnapshotEnsuresTheDomainDirectory(t *testing.T) {
	var lookedUp []string

	srv := newTestControllerServer(t, &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			lookedUp = append(lookedUp, p)
			return qumulo.FileInfo{}, apiError(http.StatusNotFound)
		},
		GetSnapshotFunc: func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
			return qumulo.SnapshotInfo{}, apiError(http.StatusNotFound)
		},
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "1", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
	})

	// The restore itself fails on the missing snapshot, which is fine: what is
	// under test is that the domain directory was ensured before that point.
	_, _ = srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-restore",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"server":          testServer,
			"storeRealPath":   "/csi-volumes",
			"storeExportPath": "/csi",
			"adDomain":        "corp.example.com",
		},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: testServer + "##12345",
				},
			},
		},
	})

	assert.Contains(t, lookedUp, "/csi-volumes/corp.example.com",
		"the domain directory must be ensured on the restore path too")
}
