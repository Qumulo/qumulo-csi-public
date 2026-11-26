//go:build chart

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// The chart and the driver have to refuse the same values, and the
// only way to know they do is to render one and parse with the other.
//
// Three gaps this closes, each of which showed up as a class that installed
// cleanly and then failed, or quietly did something else:
//
//   - a snapshotSchedule the driver cannot parse, or an allowedClients entry
//     that is a host name, installed happily and failed on the first
//     provision, by which time the claim is Pending and the mistake is a
//     cluster away from the person who made it;
//   - tenantID: 0 was dropped by the template -- `with` treats 0 as unset --
//     so the class provisioned against the cluster's default tenant with
//     nothing saying so.
//
// Each case below is rendered with helm and then handed to the driver's own
// parseVolumeParams, so neither half can drift from the other.

// renderClassValues renders the chart from a values file and returns its
// StorageClasses, or the install-time refusal.
func renderClassValues(t *testing.T, values string) ([]renderedStorageClass, error) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "values.yaml")
	require.NoError(t, os.WriteFile(path, []byte(values), 0o600))

	out, err := exec.Command("helm", "template", "test-release", chartPath, "-f", path).CombinedOutput()
	if err != nil {
		return nil, &helmRefusal{out: string(out)}
	}

	var classes []renderedStorageClass
	for _, doc := range strings.Split(string(out), "\n---") {
		var parsed renderedStorageClass
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
			continue
		}
		if parsed.Kind == "StorageClass" {
			classes = append(classes, parsed)
		}
	}
	return classes, nil
}

type helmRefusal struct{ out string }

func (e *helmRefusal) Error() string { return e.out }

// classValues is a minimal class with the one field under test appended.
func classValues(field string) string {
	return `storageClasses:
  - name: sc1
    server: qumulo.example.com
    secretName: api-token
    basePath: /csi
    provisioningModel: dedicated-export
    allowAnyClient: true
` + field
}

// TestTheChartRefusesWhatTheDriverRefuses renders each value and asserts
// that the chart and the driver agree: either helm refuses it at install, or
// it renders and the driver parses it.
func TestTheChartRefusesWhatTheDriverRefuses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		valid bool
	}{
		{name: "a schedule the driver parses", field: `    snapshotSchedule: "daily@02:00,keep=7days"`, valid: true},
		{name: "a schedule with a zone", field: `    snapshotSchedule: "daily@02:00,keep=7days,tz=UTC"`, valid: true},
		{name: "an interval schedule", field: `    snapshotSchedule: "every=30m"`, valid: true},
		{name: "a weekly schedule", field: `    snapshotSchedule: "weekly@SUN,02:00"`, valid: true},
		// The form the driver removed deliberately, because the minute was
		// accepted and then discarded.
		{name: "an hourly schedule", field: `    snapshotSchedule: "hourly:15"`, valid: false},
		{name: "a schedule at an impossible time", field: `    snapshotSchedule: "daily@25:00"`, valid: false},
		{name: "a schedule that is not one", field: `    snapshotSchedule: "nightly"`, valid: false},
		{name: "a repeated modifier", field: `    snapshotSchedule: "daily@02:00,keep=7days,keep=30days"`, valid: false},
		{name: "a full weekday name", field: `    snapshotSchedule: "weekly@SUNDAY,02:00"`, valid: true},
		{name: "a lowercase weekday", field: `    snapshotSchedule: "weekly@sunday,02:00"`, valid: true},
		{name: "the last day of the month", field: `    snapshotSchedule: "monthly@last,02:00"`, valid: true},
		{name: "the modifiers the other way round", field: `    snapshotSchedule: "daily@02:00,tz=UTC,keep=7days"`, valid: true},
		{name: "retention that never expires", field: `    snapshotSchedule: "daily@02:00,keep=never"`, valid: true},
		{name: "no schedule at all", field: `    snapshotSchedule: ""`, valid: true},
		// The API takes 1-27 or "last", so that a schedule can never
		// silently skip a short month.
		{name: "a day a short month lacks", field: `    snapshotSchedule: "monthly@28,02:00"`, valid: false},
		{name: "a retention quantity of 100 or more", field: `    snapshotSchedule: "daily@02:00,keep=365days"`, valid: false},
		// Spellings the driver parses and the chart installed before
		// the schedule pattern, which its first version refused at `helm upgrade`.
		{name: "a one-digit hour", field: `    snapshotSchedule: "daily@2:00"`, valid: true},
		{name: "a space after the comma", field: `    snapshotSchedule: "daily@02:00, keep=7days"`, valid: true},
		{name: "a space before the weekly time", field: `    snapshotSchedule: "weekly@SUN, 02:00"`, valid: true},
		{name: "an interval with a leading zero", field: `    snapshotSchedule: "every=05m"`, valid: true},
		{name: "a day of month with a leading zero", field: `    snapshotSchedule: "monthly@01,02:00"`, valid: true},
		{name: "a trailing comma", field: `    snapshotSchedule: "daily@02:00,"`, valid: true},
		{name: "a leading space", field: `    snapshotSchedule: " daily@02:00"`, valid: true},
		{name: "an interval past an hour", field: `    snapshotSchedule: "every=60m"`, valid: false},
		{name: "a zone the driver cannot be told", field: `    snapshotSchedule: "daily@02:00,tz=Local"`, valid: false},
		{name: "a zone in the wrong case", field: `    snapshotSchedule: "daily@02:00,tz=utc"`, valid: false},

		{name: "an IP", field: "    allowedClients: [\"10.0.0.5\"]", valid: true},
		{name: "a CIDR block", field: "    allowedClients: [\"192.168.1.0/24\"]", valid: true},
		{name: "an IPv6 address", field: "    allowedClients: [\"2001:db8::1\"]", valid: true},
		{name: "a host name", field: "    allowedClients: [\"nfs-client.example.com\"]", valid: false},
		{name: "a CIDR with an impossible prefix", field: "    allowedClients: [\"10.0.0.5/33\"]", valid: false},

		{name: "a tenant id", field: "    tenantID: 4", valid: true},
		{name: "a tenant name", field: `    tenantID: "finance"`, valid: true},
		{name: "tenant 0", field: "    tenantID: 0", valid: false},
		{name: "a negative tenant id", field: "    tenantID: -1", valid: false},
		{name: "an empty tenant", field: `    tenantID: ""`, valid: true},

		{name: "a store path that climbs", field: "    basePath: /csi/volumes/..", valid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			classes, err := renderClassValues(t, classValues(tc.field))

			if err != nil {
				assert.False(t, tc.valid, "the chart refused a value the driver accepts: %v", err)
				return
			}
			require.Len(t, classes, 1)

			_, parseErr := parseVolumeParams(classes[0].Parameters)
			if tc.valid {
				require.NoError(t, parseErr, "the chart rendered a class the driver refuses")
				return
			}
			require.Error(t, parseErr,
				"the chart rendered a value neither half refuses, so the class installs and then fails to provision")
		})
	}
}

// tenantID: 0 is the one the template itself dropped: `with` treats 0 as
// unset, so the parameter never reached the StorageClass and the class
// provisioned against the cluster's default tenant. The schema refuses it
// now, and this pins that the refusal is about the tenant rather than the
// parameter having silently vanished.
func TestTheChartDoesNotDropTenantZero(t *testing.T) {
	_, err := renderClassValues(t, classValues("    tenantID: 0"))

	require.Error(t, err, "tenantID: 0 names a tenant the cluster cannot have")
	assert.Contains(t, err.Error(), "tenantID")
}

// And a tenant the class does name reaches the parameters, whatever its
// type in values.yaml.
func TestTheChartRendersEveryTenantItIsGiven(t *testing.T) {
	for _, tc := range []struct{ field, want string }{
		{field: "    tenantID: 4", want: "4"},
		{field: `    tenantID: "4"`, want: "4"},
		{field: `    tenantID: "finance"`, want: "finance"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			classes, err := renderClassValues(t, classValues(tc.field))
			require.NoError(t, err)
			require.Len(t, classes, 1)

			assert.Equal(t, tc.want, classes[0].Parameters["tenantID"])

			parsed, err := parseVolumeParams(classes[0].Parameters)
			require.NoError(t, err)
			assert.Equal(t, tc.want, parsed.TenantID)
		})
	}
}
