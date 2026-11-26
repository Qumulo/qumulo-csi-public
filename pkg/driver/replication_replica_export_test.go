// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// ensureReplicaExport stages the replica's export from the source export's
// own definition, so whatever the source read-back lost is lost on the
// replica too. The host list is what a failover depends on: a replica staged
// with only the first of a class's networks admits no node on the others, and
// every pod that tries to mount the failed-over copy is refused by the array.
func TestEnsureReplicaExportCarriesEveryAllowedClient(t *testing.T) {
	var got qumulo.ExportSpec
	peer := (&MockClient{
		ListExportsFunc: func(context.Context) ([]qumulo.ExportInfo, error) { return nil, nil },
		CreateExportFunc: func(_ context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			got = spec
			return qumulo.ExportInfo{ID: "7"}, nil
		},
	}).Strict(t)

	source := qumulo.ExportInfo{
		ExportPath:         "/csi/pvc-a",
		TenantID:           2,
		AuthenticationMode: "NFS_AUTHENTICATION_MODE_SYSTEM",
		AllowedClients:     []string{"198.51.100.0/24", "203.0.113.0/24"},
	}

	require.NoError(t, ensureReplicaExport(context.Background(), peer, source, "/vol/pvc-a", "pvc-a"))

	assert.Equal(t, []string{"198.51.100.0/24", "203.0.113.0/24"}, got.AllowedClients,
		"the replica must admit every network the source admits, not just the first")
	assert.Equal(t, "/csi/pvc-a", got.ExportPath)
	assert.Equal(t, 2, got.TenantID)
	assert.Equal(t, "NFS_AUTHENTICATION_MODE_SYSTEM", got.AuthenticationMode)
	assert.True(t, got.ReadOnly, "a staged replica export is read-only until promote flips it")
}

// A source with no host restriction stages a replica with none either --
// unchanged by the union, and the case a single-network test cluster has
// always exercised.
func TestEnsureReplicaExportStagesAnUnrestrictedSourceUnrestricted(t *testing.T) {
	var got qumulo.ExportSpec
	peer := (&MockClient{
		ListExportsFunc: func(context.Context) ([]qumulo.ExportInfo, error) { return nil, nil },
		CreateExportFunc: func(_ context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			got = spec
			return qumulo.ExportInfo{ID: "7"}, nil
		},
	}).Strict(t)

	source := qumulo.ExportInfo{ExportPath: "/csi/pvc-a"}

	require.NoError(t, ensureReplicaExport(context.Background(), peer, source, "/vol/pvc-a", "pvc-a"))

	assert.Empty(t, got.AllowedClients)
}

// The shape the OpenShift failover produced, carried the rest of the way: a
// class naming two networks whose export the cluster answers as one
// restriction block per network reads back as both networks (proved at the
// wire in TestListExportsUnionsHostRestrictionsAcrossEveryBlock), and the
// replica staged from it admits both. Before the union this ended with the
// replica admitting the control-plane network alone -- a correct copy no
// application could open.
func TestReplicaExportSurvivesAnExportTheClusterSplitPerNetwork(t *testing.T) {
	source := qumulo.ExportInfo{
		ID:                 "3",
		ExportPath:         "/csi/pvc-a",
		FSPath:             "/vol/pvc-a",
		TenantID:           2,
		Description:        exportDescriptionFor("pvc-a"),
		AuthenticationMode: "NFS_AUTHENTICATION_MODE_SYSTEM",
		AllowedClients:     []string{"198.51.100.0/24", "203.0.113.0/24"},
	}

	var got qumulo.ExportSpec
	peer := (&MockClient{
		ListExportsFunc: func(context.Context) ([]qumulo.ExportInfo, error) { return nil, nil },
		CreateExportFunc: func(_ context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
			got = spec
			return qumulo.ExportInfo{ID: "9"}, nil
		},
	}).Strict(t)

	require.NoError(t, ensureReplicaExport(context.Background(), peer, source, "/vol/pvc-a", "pvc-a"))

	assert.Equal(t, []string{"198.51.100.0/24", "203.0.113.0/24"}, got.AllowedClients)
}

// An export already at the source's path is left exactly as it is, whatever
// its host list: promote is what rewrites a replica's export, not this.
func TestEnsureReplicaExportLeavesAnExistingExportAlone(t *testing.T) {
	peer := (&MockClient{
		ListExportsFunc: func(context.Context) ([]qumulo.ExportInfo, error) {
			return []qumulo.ExportInfo{{ID: "3", ExportPath: "/csi/pvc-a", AllowedClients: []string{"10.0.0.1"}}}, nil
		},
	}).Strict(t)

	source := qumulo.ExportInfo{ExportPath: "/csi/pvc-a", AllowedClients: []string{"10.0.0.1", "10.0.0.2"}}

	require.NoError(t, ensureReplicaExport(context.Background(), peer, source, "/vol/pvc-a", "pvc-a"))
	assert.Zero(t, peer.Calls("CreateExport"))
}

// exportScopingDiffers compares the class's list against the export's as
// sets. It reads the same ExportInfo the union now fills, so a class naming
// two networks no longer looks different from the export it created -- which
// is the second thing the truncated read broke: a re-provision of an existing
// volume was refused with AlreadyExists over a list that in fact matched.
func TestExportScopingDoesNotDifferForAnExportSplitPerNetwork(t *testing.T) {
	existing := qumulo.ExportInfo{
		Description: exportDescriptionFor("pvc-a"),
		// As the union reports it: both networks, in the order the cluster
		// listed its blocks, which need not be the class's order.
		AllowedClients: []string{"203.0.113.0/24", "198.51.100.0/24"},
	}
	params := volumeParams{AllowedClients: []string{"198.51.100.0/24", "203.0.113.0/24"}}

	assert.Empty(t, exportScopingDiffers(existing, params, "pvc-a", 0))
}

// And a class that really does ask for a network the export does not admit is
// still reported, so the widened read has not turned the check off.
func TestExportScopingStillDiffersWhenANetworkIsMissing(t *testing.T) {
	existing := qumulo.ExportInfo{
		Description:    exportDescriptionFor("pvc-a"),
		AllowedClients: []string{"198.51.100.0/24"},
	}
	params := volumeParams{AllowedClients: []string{"198.51.100.0/24", "203.0.113.0/24"}}

	assert.Contains(t, exportScopingDiffers(existing, params, "pvc-a", 0), paramAllowedClients)
}
