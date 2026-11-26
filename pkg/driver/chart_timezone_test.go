//go:build chart

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"archive/zip"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var updateTimezones = flag.Bool("update-timezones", false,
	"rewrite the chart's zone list from the Go toolchain's IANA database instead of comparing against it")

// chartTimezonesPath is the chart's copy of the zone names, which
// _validate.tpl checks a snapshotSchedule's tz= against at install.
const chartTimezonesPath = chartPath + "/templates/_tzzones.tpl"

const chartTimezonesHeader = `{{- /*
The IANA zone names the driver accepts in a snapshotSchedule's tz=, one per
line. Generated from the Go toolchain's zone database -- the one the driver
embeds (time/tzdata) -- by ` + "`make chart-timezones`" + `; do not edit by hand.
_validate.tpl refuses a tz= not listed here, so a zone the driver would refuse
on every claim fails ` + "`helm install`" + ` instead.
*/ -}}
{{- define "qumulo-csi-driver.timezones" -}}
`

// goTimezones lists the zone names in the toolchain's database, which
// time/tzdata embeds in the driver. The database has no API that lists its
// names, so this reads the archive the embedded copy is generated from.
func goTimezones(t *testing.T) []string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOROOT").Output()
	require.NoError(t, err, "go env GOROOT")
	archive, err := zip.OpenReader(filepath.Join(strings.TrimSpace(string(out)), "lib", "time", "zoneinfo.zip"))
	require.NoError(t, err)
	defer archive.Close()

	var zones []string
	for _, f := range archive.File {
		if !f.FileInfo().IsDir() {
			zones = append(zones, f.Name)
		}
	}
	sort.Strings(zones)
	return zones
}

func renderChartTimezones(zones []string) string {
	return chartTimezonesHeader + strings.Join(zones, "\n") + "\n{{- end -}}\n"
}

// TestChartTimezoneListIsTheDriversDatabase pins the chart's zone list to the
// driver's. A list missing a zone refuses at install a class the driver would
// serve; a list with an extra one installs a class the driver refuses on every
// claim, which is the gap the list closes. A Go upgrade that changes the
// database fails here until `make chart-timezones` is run.
func TestChartTimezoneListIsTheDriversDatabase(t *testing.T) {
	zones := goTimezones(t)
	require.Greater(t, len(zones), 400, "the toolchain's zone database looks truncated")
	for _, zone := range zones {
		_, err := time.LoadLocation(zone)
		require.NoError(t, err, "%s is in the toolchain's archive but not in the database the driver embeds", zone)
		require.NoError(t, validateTimezone(zone), "the driver refuses %s", zone)
	}

	want := renderChartTimezones(zones)
	if *updateTimezones {
		require.NoError(t, os.WriteFile(chartTimezonesPath, []byte(want), 0o644))
		return
	}
	got, err := os.ReadFile(chartTimezonesPath)
	require.NoError(t, err, "missing %s; run: make chart-timezones", chartTimezonesPath)
	require.Equal(t, want, string(got),
		"%s does not list the zones the driver accepts (Go %s); run `make chart-timezones` and review the diff",
		chartTimezonesPath, strings.TrimPrefix(goVersion(t), "go"))
}

func goVersion(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOVERSION").Output()
	require.NoError(t, err)
	return strings.TrimSpace(string(out))
}

// TestChartRefusesAZoneTheDriverRefuses: the schema checks a schedule's shape
// but cannot hold the zone database, so before the list a made-up zone, a
// wrong-case one and a zone the database has since dropped all installed, and
// every claim on the class then failed with "is not an IANA timezone".
// _validate.tpl now refuses them at install, naming the class
// and, for a case mistake, the spelling that works -- as the driver does.
func TestChartRefusesAZoneTheDriverRefuses(t *testing.T) {
	for _, tc := range []struct {
		schedule string
		hint     string
	}{
		{schedule: "daily@02:00,tz=Mars/Base"},
		{schedule: "daily@02:00,tz=US/Pacific-New"},
		{schedule: "daily@02:00,tz=UTCX"},
		{schedule: "daily@02:00,tz=utc", hint: `did you mean "UTC"?`},
		{schedule: "daily@02:00,keep=7days,tz=America/New_york", hint: `did you mean "America/New_York"?`},
		{schedule: "weekly@SUN,02:00, tz=europe/london ,keep=4weeks", hint: `did you mean "Europe/London"?`},
	} {
		t.Run(subtestName(tc.schedule), func(t *testing.T) {
			require.Error(t, validateTimezone(scheduleZone(tc.schedule)), "the driver accepts this zone; the case is wrong")
			_, err := renderClassValues(t, classValues(`    snapshotSchedule: "`+tc.schedule+`"`))
			require.Error(t, err, "helm installs a class whose zone the driver refuses on every claim")
			msg := err.Error()
			require.Contains(t, msg, "storageClasses[sc1]: snapshotSchedule tz="+scheduleZone(tc.schedule)+" is not an IANA time zone")
			if tc.hint != "" {
				require.Contains(t, msg, tc.hint)
			}
		})
	}
}

// TestChartInstallsEveryZoneTheDriverAccepts renders a class with zones of
// each shape the database uses -- a bare name, area/city, three levels, a
// signed offset -- with the modifier on either side and spaces around it.
func TestChartInstallsEveryZoneTheDriverAccepts(t *testing.T) {
	for _, schedule := range []string{
		"daily@02:00,tz=UTC",
		"daily@02:00,keep=7days,tz=America/Los_Angeles",
		"daily@02:00, tz=Asia/Kolkata ,keep=7days",
		"monthly@last,02:00,tz=America/Argentina/Buenos_Aires",
		"every=30m,tz=Etc/GMT+5",
		"every=30m,tz=Etc/GMT-14",
		"daily@02:00,keep=7days",
	} {
		t.Run(subtestName(schedule), func(t *testing.T) {
			classes, err := renderClassValues(t, classValues(`    snapshotSchedule: "`+schedule+`"`))
			require.NoError(t, err, "helm refuses a zone the driver accepts")
			require.Len(t, classes, 1)
			_, err = parseVolumeParams(classes[0].Parameters)
			require.NoError(t, err)
		})
	}
}

// subtestName keeps a schedule usable as a subtest name: t.TempDir builds a
// path from the name, where a zone's "/" is a missing directory and a
// comma splits the -f path helm is given.
func subtestName(schedule string) string {
	return strings.NewReplacer("/", "_", ",", "_", " ", "", "@", "_", ":", "").Replace(schedule)
}

// scheduleZone is the tz= value of a schedule, trimmed as the driver trims it.
func scheduleZone(schedule string) string {
	for _, term := range strings.Split(schedule, ",") {
		if zone, ok := strings.CutPrefix(strings.TrimSpace(term), "tz="); ok {
			return strings.TrimSpace(zone)
		}
	}
	return ""
}
