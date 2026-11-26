//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Regression: the snapshotSchedule pattern first put in the
// chart's values schema was stricter than parseSnapshotSchedule. A class the
// driver parsed, and the chart before it installed, failed `helm upgrade`:
// "daily@2:00" (a one-digit hour), "daily@02:00, keep=7days" (a space after
// the comma), "every=05m" (a leading zero), "daily@02:00," (a trailing
// comma), " daily@02:00" (a leading space), and so on. The pattern is meant
// to admit exactly what the driver parses, so these tests read it from the
// schema and hold it against the parser itself.
//
// Helm validates values with santhosh-tekuri/jsonschema, which compiles a
// pattern with Go's regexp -- so compiling it here with the same package is
// what helm does, with no helm needed.

// scheduleSpelling is one snapshotSchedule value and what both halves
// should make of it.
type scheduleSpelling struct {
	value string
	valid bool
	// zoneOnly marks a value whose only fault is its zone name. The schema
	// cannot hold the IANA database, so its pattern admits any zone-shaped
	// name; _validate.tpl refuses an unknown one at install from the
	// driver's own zone list, and the driver when it parses the class.
	zoneOnly bool
}

// scheduleSpellings is the table both the pattern test and the helm render
// test run. The first block is the originally reported list.
var scheduleSpellings = []scheduleSpelling{
	// Parsed by the driver and installed by the chart before the pattern.
	{value: "daily@2:00", valid: true},
	{value: "daily@02:00, keep=7days", valid: true},
	{value: "weekly@SUN, 02:00", valid: true},
	{value: "every=05m", valid: true},
	{value: "monthly@01,02:00", valid: true},
	{value: "daily@02:00,", valid: true},
	{value: " daily@02:00", valid: true},

	// The documented forms.
	{value: "", valid: true},
	{value: "   ", valid: true},
	{value: "every=30m", valid: true},
	{value: "every=1m", valid: true},
	{value: "every=59m", valid: true},
	{value: "every=1h", valid: true},
	{value: "every=23h", valid: true},
	{value: "daily@00:00", valid: true},
	{value: "daily@23:59", valid: true},
	{value: "weekly@SUN,02:00", valid: true},
	{value: "weekly@sunday,02:00", valid: true},
	{value: "weekly@Wednesday,02:00", valid: true},
	{value: "weekly@thu,2:5", valid: true},
	{value: "monthly@1,02:00", valid: true},
	{value: "monthly@27,02:00", valid: true},
	{value: "monthly@last,02:00", valid: true},
	{value: "monthly@LAST,02:00", valid: true},
	{value: "daily@02:00,keep=7days", valid: true},
	{value: "daily@02:00,keep=99months", valid: true},
	{value: "daily@02:00,keep=never", valid: true},
	{value: "daily@02:00,keep=NEVER", valid: true},
	{value: "daily@02:00,keep=7days,tz=UTC", valid: true},
	{value: "daily@02:00,tz=UTC,keep=7days", valid: true},
	{value: "daily@02:00,tz=America/Los_Angeles", valid: true},
	{value: "daily@02:00,tz=Etc/GMT+5", valid: true},
	{value: "daily@02:00,tz=America/Port-au-Prince", valid: true},

	// Whitespace and empty terms the parser trims or skips.
	{value: "daily@02:00 ,keep=7days", valid: true},
	{value: "daily@ 02 : 00", valid: true},
	{value: "weekly@ SUN ,02:00", valid: true},
	{value: "monthly@ last , 02:00", valid: true},
	{value: "daily@02:00,,keep=7days", valid: true},
	{value: "daily@02:00, ,keep=7days , tz=UTC ,", valid: true},
	{value: "daily@02:00\t,\tkeep=7days\n", valid: true},
	{value: " daily@02:00 ", valid: true},

	// Numbers as strconv.Atoi reads them.
	{value: "daily@002:000", valid: true},
	{value: "daily@+2:+0", valid: true},
	{value: "daily@-0:-00", valid: true},
	{value: "every=+5m", valid: true},
	{value: "every=0006h", valid: true},
	{value: "monthly@+1,02:00", valid: true},
	{value: "daily@02:00,keep=07days", valid: true},
	{value: "daily@02:00,keep=+7days", valid: true},

	// Refused by both.
	{value: "nightly"},
	{value: "hourly:15"},
	{value: "hourly@15"},
	{value: "Daily@02:00"},
	{value: "every=0m"},
	{value: "every=60m"},
	{value: "every=24h"},
	{value: "every=-5m"},
	{value: "every=5"},
	{value: "every=5s"},
	{value: "every=5M"},
	{value: "every= 5m"},
	{value: "every=5 m"},
	{value: "every="},
	{value: "daily@24:00"},
	{value: "daily@25:00"},
	{value: "daily@02:60"},
	{value: "daily@-1:00"},
	{value: "daily@0200"},
	{value: "daily@02:00:00"},
	{value: "daily@0 2:00"},
	{value: "daily@"},
	{value: "weekly@SUN"},
	{value: "weekly@SUN 02:00"},
	{value: "weekly@SUNDY,02:00"},
	{value: "weekly@FOO,02:00"},
	{value: "weekly@SUN,,02:00"},
	{value: "weekly@ſun,02:00"},    // a long s, which strings.ToUpper folds to S
	{value: "weekly@frıday,02:00"}, // a dotless i, which strings.ToUpper folds to I
	{value: "monthly@0,02:00"},
	{value: "monthly@28,02:00"},
	{value: "monthly@laſt,02:00"}, // a long s, which strings.EqualFold matches to s
	{value: "monthly@1"},
	{value: "daily@02:00,keep=7days,keep=30days"},
	{value: "daily@02:00,tz=UTC,tz=UTC"},
	{value: "daily@02:00,keep=7days,tz=UTC,keep=7days"},
	{value: "daily@02:00,keep=365days"},
	{value: "daily@02:00,keep=100days"},
	{value: "daily@02:00,keep=0days"},
	{value: "daily@02:00,keep=-7days"},
	{value: "daily@02:00,keep=7"},
	{value: "daily@02:00,keep=7Days"},
	{value: "daily@02:00,keep=7years"},
	{value: "daily@02:00,keep= 7days"},
	{value: "daily@02:00,keep="},
	{value: "daily@02:00,keep=nevKer"}, // not "never" under any folding
	{value: "daily@02:00,tz="},
	{value: "daily@02:00,tz= UTC"},
	{value: "daily@02:00,tz=UTC x"},
	{value: "daily@02:00,TZ=UTC"},
	{value: "daily@02:00,Keep=7days"},
	{value: "daily@02:00,foo=bar"},
	{value: "daily@02:00,02:00"},
	{value: ",daily@02:00"},

	// Zone names: the schema pattern admits the shape; the zone list in
	// _validate.tpl and the driver check the name.
	{value: "daily@02:00,tz=Mars/Olympus_Mons", zoneOnly: true},
	{value: "daily@02:00,tz=utc", zoneOnly: true},
	{value: "daily@02:00,tz=Local", zoneOnly: true},
}

// schemaSchedulePattern returns the snapshotSchedule pattern from the
// chart's values schema, compiled as helm compiles it.
func schemaSchedulePattern(t *testing.T) *regexp.Regexp {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "qumulo-csi-driver", "values.schema.json"))
	if err != nil {
		t.Fatalf("reading the values schema: %v", err)
	}
	var schema struct {
		Properties struct {
			StorageClasses struct {
				Items struct {
					Properties map[string]struct {
						Pattern string `json:"pattern"`
					} `json:"properties"`
				} `json:"items"`
			} `json:"storageClasses"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parsing the values schema: %v", err)
	}
	pattern := schema.Properties.StorageClasses.Items.Properties["snapshotSchedule"].Pattern
	if pattern == "" {
		t.Fatalf("the values schema has no snapshotSchedule pattern")
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("the snapshotSchedule pattern does not compile as helm compiles it: %v", err)
	}
	return re
}

// driverAcceptsSchedule is what parseVolumeParams makes of a
// snapshotSchedule value: trimmed, and empty means no schedule.
func driverAcceptsSchedule(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	_, err := parseSnapshotSchedule(value)
	return err
}

// TestDefect_SchedulePatternAgreesWithTheParser: for every spelling in
// the table, and for every combination of the generated terms below, the
// schema's pattern matches exactly when the driver parses it -- apart from
// a zone name, which the pattern leaves to the zone list in _validate.tpl.
func TestDefect_SchedulePatternAgreesWithTheParser(t *testing.T) {
	re := schemaSchedulePattern(t)

	for _, tc := range scheduleSpellings {
		chart := re.MatchString(tc.value)
		driverErr := driverAcceptsSchedule(tc.value)
		driver := driverErr == nil

		switch {
		case tc.zoneOnly:
			if !chart {
				t.Errorf("%q: the schema pattern refuses a zone-shaped name; the zone list checks the name", tc.value)
			}
			if driver {
				t.Errorf("%q: the driver accepts a zone it should refuse", tc.value)
			}
		default:
			if driver != tc.valid {
				t.Errorf("%q: driver accepts=%v, want %v (%v)", tc.value, driver, tc.valid, driverErr)
			}
			if chart != driver {
				t.Errorf("%q: the chart schema accepts=%v but the driver accepts=%v (%v)", tc.value, chart, driver, driverErr)
			}
		}
	}

	// Every head with every tail, so a separator or modifier the table
	// happens to pair with one head only is still held to all of them.
	heads := []string{
		"every=5m", "every=12h", "every=60m", "daily@2:00", "daily@ 02 :00", "daily@24:00",
		"weekly@SUN,02:00", "weekly@ tue , 2:00", "weekly@xyz,02:00",
		"monthly@last,02:00", "monthly@ 07 ,02:00", "monthly@28,02:00", "hourly:05",
	}
	tails := []string{
		"", ",", " , ", ",,", ",keep=7days", ", keep=7days ", ",keep=7days,", ",tz=UTC",
		",keep=7days,tz=UTC", ",tz=UTC , keep=never", ",keep=7days,keep=7days", ",tz=UTC,,tz=UTC",
		",keep=100days", ",keep=7days,tz=", ",bogus", " x", ",keep=7days x",
	}
	for _, head := range heads {
		for _, tail := range tails {
			for _, pad := range []string{"", " ", "\t"} {
				value := pad + head + tail + pad
				chart := re.MatchString(value)
				driverErr := driverAcceptsSchedule(value)
				if chart != (driverErr == nil) {
					t.Errorf("%q: the chart schema accepts=%v but the driver accepts=%v (%v)",
						value, chart, driverErr == nil, driverErr)
				}
			}
		}
	}
}

// schedulePatternGrammar builds the schema's snapshotSchedule pattern from
// the parser's rules, one piece per rule, because the pattern itself is one
// unreadable line of JSON. To change the grammar, change this and paste
// what it prints into values.schema.json (the test below shows it on a
// mismatch).
func schedulePatternGrammar() string {
	// Case-insensitive, ASCII only, as asciiUpper and asciiEqualFold read.
	ci := func(word string) string {
		var b strings.Builder
		for _, c := range word {
			b.WriteString("[" + strings.ToUpper(string(c)) + string(c) + "]")
		}
		return b.String()
	}
	// strings.TrimSpace trims what unicode.IsSpace calls space.
	const ws = `[\t\n\v\f\r ` + "\u0085   -     　" + `]*`
	// strconv.Atoi: an optional sign, then ASCII digits, leading zeros
	// allowed; "-0" is zero, so a field that may be 0 admits it.
	const (
		hour      = `(\+?0*(1?[0-9]|2[0-3])|-0+)`
		minute    = `(\+?0*[1-5]?[0-9]|-0+)`
		everyMin  = `\+?0*([1-9]|[1-5][0-9])m`
		everyHour = `\+?0*([1-9]|1[0-9]|2[0-3])h`
		dayOfMon  = `\+?0*([1-9]|1[0-9]|2[0-7])`
	)
	// parseClock: each field trimmed, so space either side of the colon.
	clock := ws + hour + ws + ":" + ws + minute
	var days []string
	for _, d := range [][2]string{{"sun", "day"}, {"mon", "day"}, {"tue", "sday"}, {"wed", "nesday"},
		{"thu", "rsday"}, {"fri", "day"}, {"sat", "urday"}} {
		days = append(days, ci(d[0])+"("+ci(d[1])+")?")
	}
	day := "(" + strings.Join(days, "|") + ")"
	head := "(every=(" + everyMin + "|" + everyHour + ")" +
		"|daily@" + clock +
		"|weekly@" + ws + day + ws + "," + clock +
		"|monthly@" + ws + "(" + dayOfMon + "|" + ci("last") + ")" + ws + "," + clock + ")"
	// validateRetention: 1-99 of a unit, or never. tz=: any zone-shaped
	// name; validateTimezone checks it against the database.
	keep := `keep=(\+?0*[1-9][0-9]?(minutes|hours|days|weeks|months)|` + ci("never") + ")"
	tz := `tz=[A-Za-z0-9_+./-]+`
	// Terms are split on commas and trimmed, an empty one is skipped, and
	// each modifier may appear once, in either order.
	comma := "(" + ws + "," + ws + ")"
	tail := comma + "*(" + comma + keep + comma + "*(" + comma + tz + comma + "*)?|" +
		comma + tz + comma + "*(" + comma + keep + comma + "*)?)?"
	// Empty, or only space, is no schedule.
	return "^" + ws + "(" + head + tail + ")?" + ws + "$"
}

// TestDefect_SchedulePatternIsTheDocumentedGrammar keeps the schema's
// pattern and the readable grammar above the same text.
func TestDefect_SchedulePatternIsTheDocumentedGrammar(t *testing.T) {
	want := schedulePatternGrammar()
	if got := schemaSchedulePattern(t).String(); got != want {
		encoded, _ := json.Marshal(want)
		t.Errorf("values.schema.json's snapshotSchedule pattern is not the grammar schedulePatternGrammar builds; "+
			"it should read:\n\"pattern\": %s", encoded)
	}
}
