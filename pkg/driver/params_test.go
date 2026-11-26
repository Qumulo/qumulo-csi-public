// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseVolumeParams(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		wantErr string
	}{
		{
			name: "all parameters present",
			params: map[string]string{
				"server":          "qumulo.example.com",
				"storeRealPath":   "/csi-volumes",
				"storeExportPath": "/csi",
			},
		},
		{
			name: "keys are matched case-insensitively",
			params: map[string]string{
				"SERVER":          "qumulo.example.com",
				"StoreRealPath":   "/csi-volumes",
				"STOREEXPORTPATH": "/csi",
			},
		},
		{
			name:    "all parameters missing",
			params:  map[string]string{},
			wantErr: "server, storeexportpath, storerealpath",
		},
		{
			name: "one parameter missing",
			params: map[string]string{
				"server":        "qumulo.example.com",
				"storeRealPath": "/csi-volumes",
			},
			wantErr: "storeexportpath",
		},
		{
			name: "blank value counts as missing",
			params: map[string]string{
				"server":          "  ",
				"storeRealPath":   "/csi-volumes",
				"storeExportPath": "/csi",
			},
			wantErr: "server",
		},
		{
			name: "relative real path rejected",
			params: map[string]string{
				"server":          "qumulo.example.com",
				"storeRealPath":   "csi-volumes",
				"storeExportPath": "/csi",
			},
			wantErr: "absolute path",
		},
		{
			name: "relative export path rejected",
			params: map[string]string{
				"server":          "qumulo.example.com",
				"storeRealPath":   "/csi-volumes",
				"storeExportPath": "csi",
			},
			wantErr: "absolute path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parseVolumeParams(tt.params)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, "qumulo.example.com", parsed.Server)
			assert.Equal(t, "/csi-volumes", parsed.StoreRealPath)
			assert.Equal(t, "/csi", parsed.StoreExportPath)
		})
	}
}

func TestVolumeParamsPaths(t *testing.T) {
	params := volumeParams{
		Server:          "qumulo.example.com",
		StoreRealPath:   "/csi-volumes/",
		StoreExportPath: "/csi/",
	}

	// Trailing separators in StorageClass values must not double up.
	assert.Equal(t, "/csi-volumes/pvc-abc", params.realPathFor("pvc-abc"))
	assert.Equal(t, "/csi/pvc-abc", params.exportPathFor("pvc-abc"))
}

// validParams is the minimum a class must carry, for tests that vary one
// parameter at a time.
func validParams() map[string]string {
	return map[string]string{
		"server":          "qumulo.example.com",
		"storeRealPath":   "/csi-volumes",
		"storeExportPath": "/csi",
	}
}

// An unrecognized key used to be dropped silently, so a class asking for a
// dedicated export with a snapshot schedule provisioned a plain subdirectory
// and reported success.
func TestParseVolumeParamsRejectsUnknownKeys(t *testing.T) {
	params := validParams()
	params["totalNonsense"] = "xyzzy"
	params["anotherOne"] = "1"

	_, err := parseVolumeParams(params)

	require.Error(t, err)
	// Named as written: the operator greps their StorageClass for what the
	// message says, and the folded form is not in it.
	assert.Contains(t, err.Error(), "unknown StorageClass parameter(s): anotherOne, totalNonsense")
	// The message names what is accepted, since the usual cause is a typo.
	assert.Contains(t, err.Error(), "recognized:")
	assert.Contains(t, err.Error(), paramStoreRealPath)
}

// csi-provisioner injects these when it runs with --extra-create-metadata.
// They are not StorageClass parameters, and rejecting them would fail every
// provision in a real cluster while every other test here still passed.
func TestParseVolumeParamsAcceptsCOInjectedKeys(t *testing.T) {
	params := validParams()
	params["csi.storage.k8s.io/pvc/name"] = "my-claim"
	params["csi.storage.k8s.io/pvc/namespace"] = "default"
	params["csi.storage.k8s.io/pv/name"] = "pvc-abc123"

	parsed, err := parseVolumeParams(params)

	require.NoError(t, err)
	assert.Equal(t, "qumulo.example.com", parsed.Server)
}

func TestParseVolumeParamsProtocol(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		want     string
		wantErr  string
	}{
		{name: "absent leaves the version to the kernel", protocol: "", want: ""},
		{name: "nfs3", protocol: "nfs3", want: protocolNFS3},
		{name: "nfs4.1", protocol: "nfs4.1", want: protocolNFS41},
		{name: "surrounding space is trimmed", protocol: "  nfs4.1  ", want: protocolNFS41},
		{name: "unsupported version", protocol: "nfs4.2", wantErr: `protocol must be "nfs3" or "nfs4.1", got "nfs4.2"`},
		{name: "bare version number", protocol: "3", wantErr: "protocol must be"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := validParams()
			if tt.protocol != "" {
				params["protocol"] = tt.protocol
			}

			parsed, err := parseVolumeParams(params)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, parsed.Protocol)
		})
	}
}

// Recognized-but-unimplemented parameters have to fail loudly. An empty value
// is treated as unset, because a Helm template renders an unset value that way
// and a chart's defaults must remain usable.
func TestParseVolumeParamsRejectsUnimplemented(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{
			name: "provisioningModel shared-subdirectory is the implemented one",
			key:  "provisioningModel", value: "shared-subdirectory",
		},
		{
			name: "provisioningModel dedicated-export is now implemented",
			key:  "provisioningModel", value: "dedicated-export",
		},
		{
			name: "provisioningModel nonsense",
			key:  "provisioningModel", value: "whatever",
			wantErr: `provisioningmodel must be "shared-subdirectory" or "dedicated-export"`,
		},
		{
			// Implemented as a path namespace, so a valid FQDN is accepted.
			name: "adDomain is accepted",
			key:  "adDomain", value: "CORP.EXAMPLE.COM",
		},
		{
			name: "adDomain rejects a bare hostname",
			key:  "adDomain", value: "corp",
			wantErr: "fully-qualified domain name",
		},
		{
			// Implemented as of 2026-08-28, so what is rejected now is a
			// malformed schedule rather than the parameter itself. "retain="
			// was never the spelling -- it is "keep=".
			name: "snapshotSchedule with an unknown term",
			key:  "snapshotSchedule", value: "daily@02:30,retain=7d",
			wantErr: "unrecognized snapshot schedule term",
		},
		{
			name: "snapshotSchedule is accepted",
			key:  "snapshotSchedule", value: "daily@02:30,keep=7days",
		},
		{
			// Per-cluster REST endpoint, for a cluster whose API is not on
			// the default port or sits behind a proxy.
			name: "apiUrl is accepted",
			key:  "apiUrl", value: "https://qumulo.example.com:9000",
		},
		{
			// Removed with the multi-cluster refactor: one controller no
			// longer serves one cluster, so there is nothing to compare to.
			name: "qumuloClusterAddress is now rejected",
			key:  "qumuloClusterAddress", value: "qumulo.example.com",
			wantErr: "unknown StorageClass parameter",
		},
		{name: "empty adDomain is treated as unset", key: "adDomain", value: ""},
		{name: "empty snapshotSchedule is treated as unset", key: "snapshotSchedule", value: ""},
		{name: "empty provisioningModel is treated as unset", key: "provisioningModel", value: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := validParams()
			params[tt.key] = tt.value

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

// The model is resolved at parse time so no downstream code has to know what
// an unset parameter means.
func TestParseVolumeParamsProvisioningModel(t *testing.T) {
	tests := []struct {
		name          string
		value         string
		wantModel     string
		wantDedicated bool
	}{
		{name: "unset defaults to shared", value: "", wantModel: provisioningModelSharedSubdirectory},
		{name: "explicit shared", value: "shared-subdirectory", wantModel: provisioningModelSharedSubdirectory},
		{name: "dedicated", value: "dedicated-export", wantModel: provisioningModelDedicatedExport, wantDedicated: true},
		{name: "mixed case", value: "Dedicated-Export", wantModel: provisioningModelDedicatedExport, wantDedicated: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := validParams()
			if tt.value != "" {
				params["provisioningModel"] = tt.value
			}

			parsed, err := parseVolumeParams(params)

			require.NoError(t, err)
			assert.Equal(t, tt.wantModel, parsed.ProvisioningModel)
			assert.Equal(t, tt.wantDedicated, parsed.DedicatedExport())
		})
	}
}

func TestParseVolumeParamsKeepsAPIURL(t *testing.T) {
	params := validParams()
	params["apiUrl"] = "  https://qumulo.example.com:9000  "

	parsed, err := parseVolumeParams(params)

	require.NoError(t, err)
	assert.Equal(t, "https://qumulo.example.com:9000", parsed.APIURL, "surrounding space is trimmed")
}

// The default is what makes a volume writable at all, so it has to apply when
// the parameter is absent -- not only when someone opts in.
func TestParseVolumeParamsMountPermissions(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		set      bool
		want     string
		wantSets bool
		wantErr  string
	}{
		{name: "absent defaults to 0777", set: false, want: "0777", wantSets: true},
		{name: "empty is treated as absent", set: true, value: "", want: "0777", wantSets: true},
		{name: "explicit 0777", set: true, value: "0777", want: "0777", wantSets: true},
		{name: "narrower mode", set: true, value: "0770", want: "0770", wantSets: true},
		{name: "three digits", set: true, value: "755", want: "755", wantSets: true},
		{name: "sticky bit", set: true, value: "1777", want: "1777", wantSets: true},
		{name: "inherit leaves the cluster default", set: true, value: "inherit", want: "inherit", wantSets: false},
		{name: "inherit is case-insensitive", set: true, value: "INHERIT", want: "inherit", wantSets: false},
		{name: "non-octal digit", set: true, value: "0778", wantErr: "octal mode"},
		{name: "not a number", set: true, value: "rwxrwxrwx", wantErr: "octal mode"},
		{name: "too short", set: true, value: "07", wantErr: "octal mode"},
		{name: "too long", set: true, value: "07777", wantErr: "octal mode"},
		{name: "just zero", set: true, value: "0", wantErr: "octal mode"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := validParams()
			if tt.set {
				params["mountPermissions"] = tt.value
			}

			parsed, err := parseVolumeParams(params)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, parsed.MountPermissions)
			assert.Equal(t, tt.wantSets, parsed.SetsMode())
		})
	}
}

// TestParseVolumeParamsServer pins the shape of the server parameter.
//
// The reason this is validated at all, rather than left to fail at mount time,
// is that server is the first field of the volume handle: a value carrying "/"
// or ":" Binds a PVC and then makes DeleteVolume and ControllerExpandVolume
// fail permanently on a handle the driver issued itself. So the rejections
// below are about handle integrity, not tidiness.
func TestParseVolumeParamsServer(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    string // defaults to value; set only where the parser rewrites it
		wantErr string
	}{
		{name: "fqdn", value: "qumulo.example.com"},
		{name: "ipv4", value: "192.0.2.10"},
		{name: "unqualified in-cluster name", value: "qumulo"},
		{name: "interior hyphens", value: "qumulo-node-1.storage.example.com"},
		{name: "digits in a label", value: "q1.example2.com"},
		// Trimmed by the normalizer before validation, so YAML padding is not
		// the operator's problem. Asserted so the trim is not removed.
		{name: "surrounding whitespace is trimmed", value: "  qumulo.example.com  ", want: "qumulo.example.com"},

		// The reported defect: apiUrl next to it is a URL, so this is the
		// value operators actually write.
		{name: "rest url", value: "https://192.0.2.10:8000", wantErr: "not a URL"},
		{name: "scheme only", value: "nfs://qumulo.example.com", wantErr: "not a URL"},
		{name: "host and port", value: "qumulo.example.com:8000", wantErr: "no port belongs here"},
		{name: "ipv6 literal", value: "fd00::1", wantErr: "IPv6"},
		{name: "trailing path", value: "qumulo.example.com/csi", wantErr: `must not contain "/"`},
		{name: "bare separator", value: "/", wantErr: `must not contain "/"`},
		{name: "interior whitespace", value: "qumulo example.com", wantErr: "must be a hostname"},
		{name: "trailing dot", value: "qumulo.example.com.", wantErr: "must be a hostname"},
		{name: "leading hyphen", value: "-qumulo", wantErr: "must be a hostname"},
		{name: "underscore", value: "qumulo_a.example.com", wantErr: "must be a hostname"},
		{name: "too long", value: strings.Repeat("a", 254), wantErr: "cannot exceed 253"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := validParams()
			params["server"] = tt.value

			parsed, err := parseVolumeParams(params)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				// Every rejection names the parameter, because a Pending PVC's
				// event is the only feedback the operator gets.
				assert.Contains(t, err.Error(), "server")
				return
			}
			require.NoError(t, err)
			want := tt.want
			if want == "" {
				want = tt.value
			}
			assert.Equal(t, want, parsed.Server)
		})
	}
}

// TestParseVolumeParamsServerRoundTripsThroughVolumeID is the check that
// justifies the one above: every server the parser accepts must produce a
// handle parseVolumeID can decode back. If these two ever disagree, the
// symptom is an undeletable PV rather than a failed test, so they are asserted
// against each other rather than each against a fixture.
func TestParseVolumeParamsServerRoundTripsThroughVolumeID(t *testing.T) {
	for _, server := range []string{
		"qumulo.example.com",
		"192.0.2.10",
		"qumulo",
		"qumulo-node-1.storage.example.com",
	} {
		t.Run(server, func(t *testing.T) {
			params := validParams()
			params["server"] = server
			parsed, err := parseVolumeParams(params)
			require.NoError(t, err)

			id := newVolumeID(parsed.Server, "", "/csi/volumes/pvc-abc123")
			decoded, err := parseVolumeID(id.String())
			require.NoError(t, err, "an accepted server produced an undecodable handle")
			assert.Equal(t, id, decoded)
		})
	}
}
