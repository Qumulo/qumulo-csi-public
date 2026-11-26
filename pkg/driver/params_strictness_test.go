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

// Four parameter mistakes the driver used to resolve silently rather
// than refuse. Each produced a class that provisioned something other than
// what it said.

// Keys are matched case-insensitively, so two spellings of one key are two
// values for one parameter and the winner is map iteration order -- which is
// random per process, so the same class provisioned against a different
// array on different calls.
func TestTwoParametersDifferingOnlyByCaseAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]string
		names  []string
	}{
		{
			name: "server",
			params: map[string]string{
				"server": "a.example.com", "Server": "b.example.com",
				"storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
			},
			names: []string{"Server", "server"},
		},
		{
			name: "storeRealPath",
			params: map[string]string{
				"server": "a.example.com", "storeRealPath": "/one",
				"STOREREALPATH": "/two", "storeExportPath": "/csi",
			},
			names: []string{"STOREREALPATH", "storeRealPath"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseVolumeParams(tc.params)

			require.Error(t, err)
			for _, name := range tc.names {
				assert.Contains(t, err.Error(), name, "the refusal must name both spellings")
			}
		})
	}
}

// One spelling of each is the normal case and must still work, in whatever
// case the administrator wrote it.
func TestOneSpellingOfEachParameterIsStillAccepted(t *testing.T) {
	parsed, err := parseVolumeParams(map[string]string{
		"Server": "a.example.com", "STOREREALPATH": "/csi-volumes", "storeexportpath": "/csi",
	})

	require.NoError(t, err)
	assert.Equal(t, "a.example.com", parsed.Server)
	assert.Equal(t, "/csi-volumes", parsed.StoreRealPath)
}

// A store root that climbs provisions somewhere other than where it reads,
// and the handles minted under it are cleaned -- so the PV names a directory
// that is not under the root the class declares, which is what every
// ownership check compares against.
func TestAStoreRootThatClimbsIsRefused(t *testing.T) {
	for _, tc := range []struct{ name, real, export string }{
		{name: "real path climbs", real: "/csi-volumes/..", export: "/csi"},
		{name: "export path climbs", real: "/csi-volumes", export: "/csi/../etc"},
		{name: "a dot element", real: "/csi-volumes/./here", export: "/csi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseVolumeParams(map[string]string{
				"server": "a.example.com", "storeRealPath": tc.real, "storeExportPath": tc.export,
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "..")
		})
	}
}

// The zone is stored on the cluster's policy. One the IANA database does not
// name makes the policy fire at a time nobody chose, and the cluster's own
// refusal names nothing.
func TestAnUnknownTimezoneIsRefused(t *testing.T) {
	for name, schedule := range map[string]string{
		"not a zone":       "daily@02:00,tz=Mars/Olympus",
		"an abbreviation":  "daily@02:00,tz=PST8PDT7",
		"an offset":        "daily@02:00,tz=+05:30",
		"the empty string": "daily@02:00,tz=",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseSnapshotSchedule(schedule)

			require.Error(t, err)
			assert.Contains(t, strings.ToLower(err.Error()), "tz")
		})
	}
}

// And the zones an operator actually writes are accepted -- from the
// embedded database, so this passes in the UBI micro controller image too.
// "Local" is not one of them: it named the controller's own zone and reached
// the array as the literal string, so it is refused (see
// TestDefect_TimezoneLocalIsRefused).
func TestTheZonesOperatorsWriteAreAccepted(t *testing.T) {
	for _, tz := range []string{"UTC", "America/Los_Angeles", "Europe/London", "Asia/Kolkata"} {
		t.Run(tz, func(t *testing.T) {
			schedule, err := parseSnapshotSchedule("daily@02:00,tz=" + tz)

			require.NoError(t, err)
			assert.Equal(t, tz, schedule.CreationSchedule.Timezone)
		})
	}
}

// A modifier given twice had the later one win, with nothing saying which of
// the two took effect.
func TestARepeatedScheduleModifierIsRefused(t *testing.T) {
	for name, schedule := range map[string]string{
		"keep twice": "daily@02:00,keep=7days,keep=30days",
		"tz twice":   "daily@02:00,tz=UTC,tz=Europe/London",
		"both twice": "daily@02:00,keep=7days,tz=UTC,keep=30days,tz=Europe/London",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseSnapshotSchedule(schedule)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "more than once")
		})
	}
}

func TestOneOfEachScheduleModifierIsAccepted(t *testing.T) {
	schedule, err := parseSnapshotSchedule("daily@02:00,keep=7days,tz=Europe/London")

	require.NoError(t, err)
	assert.Equal(t, "7days", schedule.ExpirationTimeToLive)
	assert.Equal(t, "Europe/London", schedule.CreationSchedule.Timezone)
}
