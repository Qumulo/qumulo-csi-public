//go:build chart && defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"strconv"
	"strings"
	"testing"
)

// TestDefect_ChartAndDriverAgreeOnScheduleSpellings renders every
// spelling in scheduleSpellings through helm itself and hands what renders
// to parseVolumeParams: helm installs a schedule exactly when the driver
// parses it -- zone names included, which the schema cannot check and
// _validate.tpl checks against the driver's own zone list. The first seven are the originally reported ones: each was parsed by the driver and
// installed by the chart before the schema pattern, and failed `helm upgrade` after it.
func TestDefect_ChartAndDriverAgreeOnScheduleSpellings(t *testing.T) {
	for _, tc := range scheduleSpellings {
		t.Run(subtestName(tc.value), func(t *testing.T) {
			classes, renderErr := renderClassValues(t, classValues("    snapshotSchedule: "+strconv.Quote(tc.value)))
			chart := renderErr == nil

			var driverErr error
			if chart {
				if len(classes) != 1 {
					t.Fatalf("rendered %d StorageClasses, want 1", len(classes))
				}
				_, driverErr = parseVolumeParams(classes[0].Parameters)
			} else {
				driverErr = driverAcceptsSchedule(tc.value)
			}
			driver := driverErr == nil

			// A zone the driver refuses is refused at install too: tz=Local by the schema, any
			// other by _validate.tpl's zone list.
			if chart != tc.valid {
				t.Errorf("helm install accepts=%v, want %v: %v", chart, tc.valid, renderErr)
			}
			if !tc.zoneOnly && driver != tc.valid {
				t.Errorf("the driver accepts=%v, want %v: %v", driver, tc.valid, driverErr)
			}
			if tc.zoneOnly && driver {
				t.Errorf("the driver accepts a zone it should refuse")
			}
		})
	}
}

// The template treats tenantID: "" as unset and renders no
// parameter, but the schema's minLength refused it at install -- so a values
// file that blanks the tenant to clear it could not be applied at all.
func TestDefect_ChartTreatsAnEmptyTenantIDAsUnset(t *testing.T) {
	classes, err := renderClassValues(t, classValues(`    tenantID: ""`))
	if err != nil {
		t.Fatalf("the chart refuses tenantID: \"\", which its own template treats as unset: %v", err)
	}
	if len(classes) != 1 {
		t.Fatalf("rendered %d StorageClasses, want 1", len(classes))
	}
	if v, ok := classes[0].Parameters["tenantID"]; ok {
		t.Errorf("tenantID: \"\" rendered a tenantID parameter %q; an empty tenant is no tenant", v)
	}
	if _, err := parseVolumeParams(classes[0].Parameters); err != nil {
		t.Errorf("the driver refuses the class rendered with an empty tenant: %v", err)
	}
}

// TestDefect_ChartRefusesTimezoneLocal: tz=Local names the controller's own clock, which the array cannot use;
// the driver refuses it, so the chart refuses it at install instead of leaving every claim Pending.
func TestDefect_ChartRefusesTimezoneLocal(t *testing.T) {
	for _, v := range []string{"daily@02:00,tz=Local", "daily@02:00, tz=Local ,keep=7days", "weekly@SUN,02:00,keep=never,tz=Local"} {
		if _, err := renderClassValues(t, classValues("    snapshotSchedule: "+strconv.Quote(v))); err == nil {
			t.Errorf("the chart renders snapshotSchedule %q, which the driver refuses", v)
		}
	}
	if _, err := renderClassValues(t, classValues("    snapshotSchedule: "+strconv.Quote("daily@02:00,tz=UTC"))); err != nil {
		t.Errorf("the chart refuses snapshotSchedule tz=UTC: %v", err)
	}
	// The schema's rule refuses Local by name and nothing longer: Localtime is no zone either, but it is the zone
	// list that refuses it, with the zone list's reason.
	_, err := renderClassValues(t, classValues("    snapshotSchedule: "+strconv.Quote("daily@02:00,tz=Localtime")))
	if err == nil || !strings.Contains(err.Error(), "tz=Localtime is not an IANA time zone") {
		t.Errorf("tz=Localtime: want the zone list's refusal, got %v", err)
	}
}
