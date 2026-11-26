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

// The capacity range's two fields are both optional in the CSI spec,
// and a request naming only a limit asks for a volume no larger than it --
// which csi-sanity sends. It was refused as "required bytes must be greater
// than zero". A negative limit, meanwhile, fell straight through the
// "limit > 0" test and provisioned the required size against a limit that
// says no size is acceptable.

func TestRequiredCapacityAcceptsALimitOnlyRange(t *testing.T) {
	got, err := requiredCapacity(&csi.CapacityRange{LimitBytes: testGiB})

	require.NoError(t, err)
	assert.Equal(t, uint64(testGiB), got, "a limit-only range provisions the largest volume it allows")
}

func TestRequiredCapacityRefusesANegativeLimit(t *testing.T) {
	_, err := requiredCapacity(&csi.CapacityRange{RequiredBytes: testGiB, LimitBytes: -1})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit bytes")
}

func TestRequiredCapacityRanges(t *testing.T) {
	for _, tc := range []struct {
		name      string
		rng       *csi.CapacityRange
		want      uint64
		wantError bool
	}{
		{name: "absent", rng: nil, wantError: true},
		{name: "empty", rng: &csi.CapacityRange{}, wantError: true},
		{name: "required only", rng: &csi.CapacityRange{RequiredBytes: testGiB}, want: uint64(testGiB)},
		{name: "limit only", rng: &csi.CapacityRange{LimitBytes: testGiB}, want: uint64(testGiB)},
		{
			name: "required within limit",
			rng:  &csi.CapacityRange{RequiredBytes: testGiB, LimitBytes: 2 * testGiB},
			want: uint64(testGiB),
		},
		{
			name:      "required above limit",
			rng:       &csi.CapacityRange{RequiredBytes: 2 * testGiB, LimitBytes: testGiB},
			wantError: true,
		},
		{name: "negative required", rng: &csi.CapacityRange{RequiredBytes: -1}, wantError: true},
		{name: "negative limit", rng: &csi.CapacityRange{LimitBytes: -1}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := requiredCapacity(tc.rng)

			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// End to end, because the refusal the operator saw came out of CreateVolume:
// a limit-only claim now provisions a volume of that size.
func TestCreateVolumeAcceptsALimitOnlyRange(t *testing.T) {
	var gotQuota uint64
	client := &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: testFileID, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		CreateQuotaFunc: func(ctx context.Context, fileID string, limitBytes uint64) error {
			gotQuota = limitBytes
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{LimitBytes: testGiB},
		Parameters:         testParams(),
	})

	require.NoError(t, err)
	assert.Equal(t, uint64(testGiB), gotQuota)
	assert.Equal(t, testGiB, resp.GetVolume().GetCapacityBytes())
}

func TestCreateVolumeRefusesANegativeLimit(t *testing.T) {
	srv := newTestControllerServer(t, &MockClient{})

	_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-abc",
		VolumeCapabilities: mountCapabilities(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB, LimitBytes: -1},
		Parameters:         testParams(),
	})

	assertCode(t, err, codes.InvalidArgument)
}
