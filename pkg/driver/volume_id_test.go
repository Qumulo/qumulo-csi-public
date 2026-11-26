// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVolumeIDRoundTrip(t *testing.T) {
	original := newVolumeID("qumulo.example.com", "", "/csi/volumes/pvc-abc123")

	encoded := original.String()
	assert.Equal(t, "qumulo.example.com##/csi/volumes/pvc-abc123", encoded)

	// The CSI spec asks that a volume ID stay within 128 bytes.
	assert.LessOrEqual(t, len(encoded), maxVolumeHandleBytes)
	assert.False(t, original.tooLong())

	decoded, err := parseVolumeID(encoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
	assert.Equal(t, "pvc-abc123", decoded.Name, "the name is derived from the path, not encoded beside it")
}

// The handle carries no file ID, which is the whole point: a file ID is
// allocated by the array that made the directory and replication does not
// preserve it, so a handle carrying one names a different directory
// on the replica. A path is the same on both arrays.
//
// This is the assertion that would fail first if anyone put an array-local
// identifier back into the handle.
func TestVolumeIDIsTheSameOnBothArraysExceptForTheServer(t *testing.T) {
	const realPath = "/csi/volumes/pvc-abc123"

	source := newVolumeID("array-a.example.com", "", realPath)
	replica := newVolumeID("array-b.example.com", "", realPath)

	assert.NotEqual(t, source.String(), replica.String())
	assert.Equal(t, source.RealPath, replica.RealPath)
	assert.Equal(t, source.Name, replica.Name)

	// And with one floating DNS name -- the topology the runbook recommends --
	// the handle does not change at all across a failover.
	floating := newVolumeID("csi-dr.example.com", "", realPath)
	assert.Equal(t, floating.String(), newVolumeID("csi-dr.example.com", "", realPath).String())
}

// A deep basePath plus an adDomain plus a long name can pass 128 bytes, which
// CreateVolume warns about. It does not refuse: the spec puts the same
// 128-byte SHOULD on the name, and csi-sanity requires a 128-byte name to
// provision, so the two cannot both be honoured. tooLong is the predicate the
// warning is keyed on.
func TestVolumeIDTooLongIsDetected(t *testing.T) {
	assert.False(t, newVolumeID("qumulo.example.com", "", "/csi/volumes/pvc-abc123").tooLong())

	deep := newVolumeID("a-rather-long-array-name.storage.example.com", "", "/very/deep/base/path/for/this/class/CORP.EXAMPLE.COM/pvc-3f2504e0-4f89-11d3-9a0c-0305e82c3301")
	assert.Greater(t, len(deep.String()), maxVolumeHandleBytes)
	assert.True(t, deep.tooLong())
}

// The separator must not be reachable from a StorageClass. server is already
// constrained to a hostname; the store paths and adDomain are not, and a "#"
// in any of them would produce a handle this driver could not decode -- a
// volume that provisions, binds, and can then never be deleted or expanded.
func TestParametersCannotSmuggleTheHandleSeparator(t *testing.T) {
	// The keys are spelled as validParams spells them. parseVolumeParams
	// lower-cases every key, so setting the lower-case constant here instead
	// would leave TWO entries that normalise to one, and which of them won
	// would depend on Go's map iteration order -- a test that passes about
	// half the time.
	for _, tc := range []struct{ key, value string }{
		{"storeRealPath", "/csi#volumes"},
		{"storeExportPath", "/export#path"},
		{"adDomain", "CORP#EXAMPLE.COM"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			params := validParams()
			params[tc.key] = tc.value
			_, err := parseVolumeParams(params)
			require.Error(t, err)
			assert.Contains(t, err.Error(), volumeIDSeparator)
		})
	}
}

func TestParseVolumeIDRejectsMalformed(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{name: "empty", id: ""},
		{name: "no separator", id: "qumulo.example.com/csi/volumes/pvc-abc"},
		{name: "a legacy handle with a relative path", id: "qumulo.example.com#csi/volumes/pvc-abc"},
		{name: "a legacy handle naming the root", id: "qumulo.example.com#/"},
		{name: "too many separators", id: "qumulo.example.com###/csi/volumes/pvc-abc"},
		{name: "empty server", id: "##/csi/volumes/pvc-abc"},
		{name: "empty path", id: "qumulo.example.com##"},
		{name: "relative path", id: "qumulo.example.com##csi/volumes/pvc-abc"},
		{name: "the cluster root", id: "qumulo.example.com##/"},
		{name: "the cluster root, uncleaned", id: "qumulo.example.com##/../"},
		{name: "an endpoint that is not a URL", id: "qumulo.example.com#not-a-url#/csi/volumes/pvc-abc"},
		{name: "an endpoint with no scheme", id: "qumulo.example.com#qumulo.example.com:8000#/csi/volumes/pvc-abc"},
		{name: "an endpoint with no host", id: "qumulo.example.com#https:///#/csi/volumes/pvc-abc"},
		{name: "not an ID at all", id: "some-other-drivers-handle"},
		// The retired <server>/<fileID>/<name> form. Every pre-release lab
		// volume carries one, and the driver has never been released, so it
		// is refused rather than decoded -- see volume_id.go.
		{name: "the retired file-ID format", id: "qumulo.example.com/12884901888/pvc-abc123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseVolumeID(tt.id)
			assert.Error(t, err)
		})
	}
}

// The two-field <server>#<path> handle an older build minted maps
// one-to-one onto the current form with the endpoint empty, so it is decoded:
// refusing it stranded every PV that build provisioned, since DeleteVolume
// answers OK for an ID it cannot parse and the directory, quota and export
// stayed behind.
func TestParseVolumeIDDecodesTheTwoFieldHandle(t *testing.T) {
	got, err := parseVolumeID("qumulo.example.com#/csi/volumes/pvc-abc123")

	require.NoError(t, err)
	assert.Equal(t, newVolumeID("qumulo.example.com", "", "/csi/volumes/pvc-abc123"), got)
	assert.Equal(t, "https://qumulo.example.com:8000", got.APIEndpoint(), "the endpoint that build dialled")
}

// The retired file-ID form has no safe mapping -- it names a file ID, which
// replication does not preserve -- so it, and anything else in no known form,
// is still refused.
func TestParseVolumeIDRefusesEveryEarlierEncoding(t *testing.T) {
	for _, id := range []string{
		// The retired file-ID form.
		"qumulo.example.com/12884901888/pvc-abc123",
		// Hand-written strings that resemble neither.
		"qumulo.example.com/not-a-file-id/pvc-abc",
		"a/b/c",
		"qumulo.example.com//pvc-abc",
	} {
		t.Run(id, func(t *testing.T) {
			_, err := parseVolumeID(id)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "malformed",
				"an earlier encoding is malformed now, not a special case")
		})
	}
}

// A zero volume ID renders as "", never as a bare separator: ListSnapshots
// emits it when a snapshot's source file ID does not resolve, and a handle
// that cannot be parsed back would be worse than an empty field.
func TestZeroVolumeIDRendersEmpty(t *testing.T) {
	assert.Empty(t, volumeID{}.String())
	assert.Empty(t, volumeID{Server: "qumulo.example.com"}.String())
	assert.Empty(t, volumeID{RealPath: "/csi/volumes/pvc-abc"}.String())
}

func TestSnapshotIDRoundTrip(t *testing.T) {
	original := newSnapshotID("qumulo.example.com", "", "42")

	encoded := original.String()
	assert.Equal(t, "qumulo.example.com##42", encoded)

	decoded, err := parseSnapshotID(encoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestParseSnapshotIDRejectsMalformed(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{name: "empty", id: ""},
		{name: "too few fields", id: "qumulo.example.com#42"},
		{name: "too many fields", id: "qumulo.example.com##42#extra"},
		{name: "empty server", id: "##42"},
		{name: "empty ID", id: "qumulo.example.com##"},
		{name: "a slash form whose ID is not a number", id: "qumulo.example.com/abc"},
		{name: "an endpoint that is not a URL", id: "qumulo.example.com#not-a-url#42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSnapshotID(tt.id)
			assert.Error(t, err)
		})
	}
}

// The <server>/<id> snapshot handle an older build minted -- and the
// chart README once documented for imports -- is decoded as
// <server>##<id>, the endpoint the server implies.
func TestTheTwoFieldSnapshotHandleIsDecoded(t *testing.T) {
	got, err := parseSnapshotID("qumulo.example.com/42")

	require.NoError(t, err)
	assert.Equal(t, newSnapshotID("qumulo.example.com", "", "42"), got)
}

// An apiUrl with more than one trailing slash had only one stripped,
// so the minted handle carried "...:8000/#..." and re-parsing it rendered
// "...:8000#...": the PV's handle and the driver's rendering of the same
// volume differed as strings (the metrics' volume label, ListVolumes against
// PVs). Every trailing slash is dropped, so a minted handle round-trips.
func TestAnAPIURLWithTrailingSlashesMintsAHandleThatRoundTrips(t *testing.T) {
	for _, apiURL := range []string{
		"https://qumulo-a.example.com:9000/",
		"https://qumulo-a.example.com:9000//",
		"https://qumulo-a.example.com:9000///",
	} {
		minted := newVolumeID("qumulo-a.example.com", apiURL, "/csi/volumes/pvc-1").String()
		require.Equal(t, "qumulo-a.example.com#https://qumulo-a.example.com:9000#/csi/volumes/pvc-1", minted, apiURL)

		parsed, err := parseVolumeID(minted)
		require.NoError(t, err)
		require.Equal(t, minted, parsed.String(), apiURL)
	}
}
