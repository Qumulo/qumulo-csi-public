// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// The volume_id label's metrics half.

// TestAGroupReportAfterADeleteDoesNotReviveTheMembersSeries is the
// shape in the group fan-out.
//
// A group's report exports its verdict for every member it finds in a
// PersistentVolume listing that is reused for ten minutes. A volume deleted
// inside that window is still in the listing, so the next report exported a
// protected gauge for it -- and a deleted volume reading protected=0 is
// QumuloCSIVolumeUnprotected firing, for ever, for something that is gone.
// The quota path has guarded against this since; this is the same
// guard on the verdict.
func TestAGroupReportAfterADeleteDoesNotReviveTheMembersSeries(t *testing.T) {
	r := testRegistry()

	// The volume is protected, and has a series.
	r.ObserveVolumeProtection(context.Background(), qumulo.VolumeProtectionEvent{Subject: testHandle, Protected: true})
	require.Equal(t, 1, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"))

	// It is deleted.
	_, err := call(t, r, "/csi.v1.Controller/DeleteVolume",
		&csi.DeleteVolumeRequest{VolumeId: testHandle},
		func(context.Context, any) (any, error) { return &csi.DeleteVolumeResponse{}, nil })
	require.NoError(t, err)
	require.Equal(t, 0, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"))

	// A group report, fanning out from the listing it still holds, says the
	// deleted volume is unprotected.
	r.ObserveVolumeProtection(context.Background(), qumulo.VolumeProtectionEvent{Subject: testHandle, Protected: false})

	assert.Equal(t, 0, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"),
		"a deleted volume must not get its series back from a stale member listing")
}

// A volume provisioned again at the same handle is a live volume: the
// deletion must not silence it.
func TestAVolumeProvisionedAgainGetsItsSeriesBack(t *testing.T) {
	r := testRegistry()

	_, err := call(t, r, "/csi.v1.Controller/DeleteVolume",
		&csi.DeleteVolumeRequest{VolumeId: testHandle},
		func(context.Context, any) (any, error) { return &csi.DeleteVolumeResponse{}, nil })
	require.NoError(t, err)

	_, err = call(t, r, "/csi.v1.Controller/CreateVolume",
		&csi.CreateVolumeRequest{Name: "pvc-abc123"},
		func(context.Context, any) (any, error) {
			return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: testHandle}}, nil
		})
	require.NoError(t, err)

	r.ObserveVolumeProtection(context.Background(), qumulo.VolumeProtectionEvent{Subject: testHandle, Protected: true})

	assert.Equal(t, 1, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"))
}

// TestVolumeIDIsTheSameForLegacyAndCanonicalHandles.
//
// One volume has two handles after an upgrade: the current
// "<server>#<apiUrl>#<path>" and the legacy "<server>#<path>" the driver
// still decodes. They name the same directory on the same array, so
// they must label the same series -- hashing them as written gave a volume
// counted twice, with whichever RPC carried the other spelling writing the
// second.
func TestVolumeIDIsTheSameForLegacyAndCanonicalHandles(t *testing.T) {
	const (
		canonical = "qumulo.example.com##/csi/volumes/pvc-abc123"
		legacy    = "qumulo.example.com#/csi/volumes/pvc-abc123"
		trailing  = "qumulo.example.com##/csi/volumes/pvc-abc123/"
	)

	assert.Equal(t, volumeID(canonical), volumeID(legacy),
		"the legacy and current spellings of one handle are one volume")
	assert.Equal(t, volumeID(canonical), volumeID(trailing),
		"a trailing slash does not make a second volume")

	// And the distinctions that are real are kept: another array, another
	// endpoint, another path.
	assert.NotEqual(t, volumeID(canonical), volumeID("other.example.com##/csi/volumes/pvc-abc123"))
	assert.NotEqual(t, volumeID(canonical),
		volumeID("qumulo.example.com#https://rest.example.com:8000#/csi/volumes/pvc-abc123"))
	assert.NotEqual(t, volumeID(canonical), volumeID("qumulo.example.com##/csi/volumes/pvc-other"))
}

// The same volume reported under both spellings writes one series, not two.
func TestTwoSpellingsOfOneHandleWriteOneSeries(t *testing.T) {
	r := testRegistry()

	r.ObserveVolumeProtection(context.Background(), qumulo.VolumeProtectionEvent{
		Subject: "qumulo.example.com##/csi/volumes/pvc-abc123", Protected: true,
	})
	r.ObserveVolumeProtection(context.Background(), qumulo.VolumeProtectionEvent{
		Subject: "qumulo.example.com#/csi/volumes/pvc-abc123", Protected: true,
	})

	assert.Equal(t, 1, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"),
		"one volume, however its handle is spelled, is one series")
}

// A handle this driver did not mint is hashed as it stands rather than
// having a spelling invented for it.
func TestAnUnparseableHandleStillGetsAStableLabel(t *testing.T) {
	assert.Equal(t, volumeID("not-a-handle"), volumeID("not-a-handle"))
	assert.NotEqual(t, volumeID("not-a-handle"), volumeID("also-not-a-handle"))
}

// TestAVolumeDeletedUnderOneSpellingAndProvisionedUnderTheOtherIsLive: the
// canonical deletion key does not over-silence -- a volume deleted under one
// spelling and provisioned again under the other is live.
func TestAVolumeDeletedUnderOneSpellingAndProvisionedUnderTheOtherIsLive(t *testing.T) {
	r := testRegistry()
	_, err := call(t, r, "/csi.v1.Controller/DeleteVolume",
		&csi.DeleteVolumeRequest{VolumeId: "qumulo.example.com#/csi/volumes/pvc-legacy"},
		func(context.Context, any) (any, error) { return &csi.DeleteVolumeResponse{}, nil })
	require.NoError(t, err)
	_, err = call(t, r, "/csi.v1.Controller/CreateVolume",
		&csi.CreateVolumeRequest{Name: "pvc-legacy"},
		func(context.Context, any) (any, error) {
			return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: "qumulo.example.com##/csi/volumes/pvc-legacy"}}, nil
		})
	require.NoError(t, err)

	r.ObserveVolumeProtection(context.Background(),
		qumulo.VolumeProtectionEvent{Subject: "qumulo.example.com##/csi/volumes/pvc-legacy", Protected: true})
	assert.Equal(t, 1, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"))
}

// docs/METRICS.md tells an operator to hash a legacy handle in its canonical
// form; this is the value that recipe prints for its example, so the recipe
// and the label cannot drift apart unnoticed.
func TestMetricsDocVolumeIDRecipeMatchesALegacyHandle(t *testing.T) {
	// printf %s 'qumulo.example.com##/csi/volumes/pvc-abc123' | sha256sum | cut -c1-16
	assert.Equal(t, "22153d6a0607754a", volumeID("qumulo.example.com#/csi/volumes/pvc-abc123"))
	assert.Equal(t, "22153d6a0607754a", volumeID("qumulo.example.com##/csi/volumes/pvc-abc123/"))
}
