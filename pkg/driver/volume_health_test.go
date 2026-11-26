// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// ControllerGetVolumeHealth reports what the array can say about a
// volume: gone, not a directory, not exported (so not mountable: the
// trigger), or without its quota. external-health-monitor v0.19.0 polls it
// and records the answer on the claim.
func TestControllerGetVolumeHealthReportsWhatTheArraySees(t *testing.T) {
	for _, tt := range []struct {
		name   string
		client func(*MockClient)
		want   []string // status/reason
	}{
		{"healthy", func(*MockClient) {}, nil},
		{"directory gone", func(m *MockClient) { m.LookupFileByPathFunc = missingVolume() },
			[]string{"INACCESSIBLE/VolumeMissing"}},
		{"not a directory", func(m *MockClient) {
			m.LookupFileByPathFunc = func(_ context.Context, p string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{ID: testFileID, Path: p, Type: "FS_FILE_TYPE_FILE"}, nil
			}
		}, []string{"INACCESSIBLE/NotADirectory"}},
		{"no export covers it", func(m *MockClient) {
			m.ListExportsFunc = func(context.Context) ([]qumulo.ExportInfo, error) { return nil, nil }
		}, []string{"INACCESSIBLE/NotExported"}},
		{"no quota", func(m *MockClient) {
			m.GetQuotaFunc = func(context.Context, string) (uint64, error) { return 0, apiError(http.StatusNotFound) }
		}, []string{"DEGRADED/QuotaMissing"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &MockClient{GetQuotaFunc: func(context.Context, string) (uint64, error) { return uint64(testGiB), nil }}
			tt.client(client)

			resp, err := newTestControllerServer(t, client).ControllerGetVolumeHealth(context.Background(),
				&csi.ControllerGetVolumeHealthRequest{VolumeId: testHandle("pvc-abc"), Secrets: testSecrets()})

			require.NoError(t, err, "a condition is a report, not an RPC failure")
			assert.Equal(t, testHandle("pvc-abc"), resp.GetVolumeHealth().GetVolumeId())
			var got []string
			for _, h := range resp.GetVolumeHealth().GetHealthStatuses() {
				got = append(got, h.GetStatus().String()+"/"+h.GetReason())
				assert.NotEmpty(t, h.GetMessage())
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// The monitor sends no Secret (its RBAC cannot read one), so without the
// request's own the credential comes from the classes, as for
// ControllerGetVolume, which needs controller.inventory.enabled.
func TestControllerGetVolumeHealthWithoutSecretsNeedsInventory(t *testing.T) {
	_, err := newTestControllerServer(t, &MockClient{}).ControllerGetVolumeHealth(context.Background(),
		&csi.ControllerGetVolumeHealthRequest{VolumeId: testHandle("pvc-abc")})
	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "inventory")
}

func TestControllerGetVolumeHealthRefusesAnEmptyID(t *testing.T) {
	_, err := newTestControllerServer(t, &MockClient{}).ControllerGetVolumeHealth(context.Background(),
		&csi.ControllerGetVolumeHealthRequest{})
	assertCode(t, err, codes.InvalidArgument)
}
