// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	// The IANA timezone database, embedded in the binary. validateTimezone
	// needs it: the controller image is UBI micro and has no /usr/share/
	// zoneinfo, so without this every tz= would be refused there and
	// accepted everywhere else. About 450 KiB.
	_ "time/tzdata"

	"qumulo-csi-driver/pkg/qumulo"
)

// The snapshotSchedule grammar.
//
// A StorageClass parameter is a flat string, but a Qumulo snapshot policy
// schedule is a nested object with a three-way frequency enum. The alternative
// designs were naming a template that already exists on the cluster -- which
// the driver can neither create nor validate, so a typo silently yields no
// schedule at all -- and embedding raw JSON, which is exact but puts vendor
// enum spellings into user-facing configuration. This grammar covers every
// frequency the API accepts while staying readable in a StorageClass:
//
//	every=30m                      every 30 minutes
//	every=6h                       every 6 hours
//	daily@02:00                    02:00 every day
//	weekly@SUN,02:00               Sundays at 02:00
//	monthly@1,02:00                the 1st at 02:00
//
// with two optional modifiers, in any order after the first term:
//
//	keep=7days                     snapshot retention
//	tz=America/Los_Angeles         schedule timezone, default UTC
//
// so a complete value reads "daily@02:00,keep=7days,tz=UTC".
//
// There is deliberately no hourly form. This grammar advertised "hourly:MM"
// and described it as "every hour at :MM", which it never was: the minute was
// validated and then dropped, because the API's HOURLY_OR_LESS frequency takes
// a window and an interval with nowhere to put an anchor. "every=1h" is the
// honest spelling of the same schedule, and anything starting "hourly" is
// refused with that explanation -- see errHourlyUnsupported.
const (
	scheduleKeepKey = "keep="
	scheduleTZKey   = "tz="

	// defaultScheduleTimezone is UTC deliberately, and documented.
	//
	// The cluster interprets a schedule in whatever timezone the policy names.
	// Inheriting the cluster's local zone instead would fire snapshots at an
	// hour the operator did not choose, and nothing surfaces that until
	// somebody reads a snapshot timestamp.
	defaultScheduleTimezone = "UTC"
)

// weekdays maps the grammar's day names to the cluster's enum. Both are the
// full English uppercase name, but the mapping is explicit so a future
// shorthand ("MON") has one place to live.
// The API takes three-letter abbreviations. Full names are accepted from the
// operator and mapped, because a StorageClass reads better with them -- but
// SUNDAY on the wire is a decode error.
var weekdays = map[string]string{
	"SUNDAY": "SUN", "SUN": "SUN",
	"MONDAY": "MON", "MON": "MON",
	"TUESDAY": "TUE", "TUE": "TUE",
	"WEDNESDAY": "WED", "WED": "WED",
	"THURSDAY": "THU", "THU": "THU",
	"FRIDAY": "FRI", "FRI": "FRI",
	"SATURDAY": "SAT", "SAT": "SAT",
}

// retentionUnits are the units expiration_time_to_live accepts. The quantity
// must also be below 100, so a year is "12months", not "365days".
var retentionUnits = []string{"months", "weeks", "days", "hours", "minutes"}

// scheduleSyntax is the message every rejection ends with. Naming the accepted
// forms is the difference between an operator fixing a typo and guessing.
const scheduleSyntax = `accepted forms: "every=Nm", "every=Nh", ` +
	`"daily@HH:MM", "weekly@DAY,HH:MM", "monthly@D,HH:MM", ` +
	`each optionally followed by ",keep=<duration>" and ",tz=<zone>"`

// parseSnapshotSchedule turns a snapshotSchedule parameter into the Schedule
// the Qumulo policy API takes.
//
// It is pure and total: every rejection names what was wrong and how to spell
// it correctly. The cluster itself rejects a malformed schedule with an opaque
// "Invalid fields for schedule" that names nothing, so validating here is what
// makes a mistake diagnosable at all.
func parseSnapshotSchedule(value string) (qumulo.Schedule, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return qumulo.Schedule{}, fmt.Errorf("snapshot schedule must not be empty; %s", scheduleSyntax)
	}

	terms := strings.Split(value, ",")
	head := strings.TrimSpace(terms[0])

	// weekly@SUN,02:00 and monthly@1,02:00 carry a comma inside the first
	// term, so they consume the next one before the modifiers are read.
	rest := terms[1:]

	var (
		creation qumulo.CreationSchedule
		err      error
	)
	switch {
	case strings.HasPrefix(head, "hourly"):
		// Matched on the bare prefix rather than "hourly:" so that every
		// spelling an operator might try lands on the explanation below
		// instead of the generic "unrecognized schedule".
		return qumulo.Schedule{}, errHourlyUnsupported(head)
	case strings.HasPrefix(head, "every="):
		creation, err = parseEvery(head)
	case strings.HasPrefix(head, "daily@"):
		creation, err = parseDaily(head)
	case strings.HasPrefix(head, "weekly@"):
		creation, rest, err = parseWeekly(head, rest)
	case strings.HasPrefix(head, "monthly@"):
		creation, rest, err = parseMonthly(head, rest)
	default:
		return qumulo.Schedule{}, fmt.Errorf("unrecognized snapshot schedule %q; %s", value, scheduleSyntax)
	}
	if err != nil {
		return qumulo.Schedule{}, err
	}

	schedule := qumulo.Schedule{CreationSchedule: creation}
	schedule.CreationSchedule.Timezone = defaultScheduleTimezone

	// Each modifier may be given once. Repeated, the later one silently won
	// and the earlier was discarded -- so "keep=7days,keep=30days" kept a
	// week, with nothing saying which of the two the operator got.
	seen := map[string]bool{}
	for _, term := range rest {
		term = strings.TrimSpace(term)
		switch {
		case term == "":
			continue
		case strings.HasPrefix(term, scheduleKeepKey):
			if seen[scheduleKeepKey] {
				return qumulo.Schedule{}, fmt.Errorf("snapshot schedule gives %s more than once; "+
					"only one of them would take effect", scheduleKeepKey)
			}
			seen[scheduleKeepKey] = true
			keep := strings.TrimPrefix(term, scheduleKeepKey)
			if err := validateRetention(keep); err != nil {
				return qumulo.Schedule{}, err
			}
			schedule.ExpirationTimeToLive = keep
		case strings.HasPrefix(term, scheduleTZKey):
			if seen[scheduleTZKey] {
				return qumulo.Schedule{}, fmt.Errorf("snapshot schedule gives %s more than once; "+
					"only one of them would take effect", scheduleTZKey)
			}
			seen[scheduleTZKey] = true
			tz := strings.TrimPrefix(term, scheduleTZKey)
			if err := validateTimezone(tz); err != nil {
				return qumulo.Schedule{}, err
			}
			schedule.CreationSchedule.Timezone = tz
		default:
			return qumulo.Schedule{}, fmt.Errorf("unrecognized snapshot schedule term %q; %s", term, scheduleSyntax)
		}
	}

	return schedule, nil
}

// validateTimezone refuses a tz= the IANA database does not name.
//
// The zone is sent to the cluster, which stores it on the policy; a name it
// does not know makes the policy fire at a time nobody chose, or not at all,
// and the cluster's own refusal is the opaque "Invalid fields for schedule"
// that names nothing. Checked here, where the mistake can be described.
//
// The database is embedded (time/tzdata, imported in this package) rather
// than read from the filesystem: the controller image is UBI micro and
// carries no zoneinfo, so a filesystem lookup would refuse every zone there
// while accepting them all in a test run on a developer's machine.
//
// "Local" is refused by name: LoadLocation answers it with the
// controller's own zone, so it passed, and then the literal string "Local"
// went to the array -- a zone the array does not know, and not one the
// operator chose either. The schedule's zone has to be written out.
//
// Zone names are case-sensitive, as the database is, and are not folded:
// "utc" is refused rather than read as "UTC". The chart's schema cannot hold
// the database and admits any zone-shaped name, so this refusal is where a
// wrong case surfaces -- which is why it names the spelling that would have
// worked when there is one.
func validateTimezone(tz string) error {
	if tz == "" {
		return fmt.Errorf("tz= must name a timezone such as %q", "UTC")
	}
	if tz == "Local" {
		return fmt.Errorf("tz=Local names the controller's own timezone, which the cluster cannot be told; "+
			"write the zone out, such as %q or %q", "UTC", "America/Los_Angeles")
	}
	if _, err := time.LoadLocation(tz); err != nil {
		if suggestion := zoneSpelling(tz); suggestion != "" {
			return fmt.Errorf("tz=%s is not an IANA timezone; zone names are case-sensitive -- did you mean %q?",
				tz, suggestion)
		}
		return fmt.Errorf("tz=%s is not an IANA timezone such as %q or %q (zone names are case-sensitive): %w",
			tz, "UTC", "America/Los_Angeles", err)
	}
	return nil
}

// zoneSpelling returns the database's spelling of a zone written in the
// wrong case, or "" when no recasing names one. It only suggests: the name
// the operator wrote is still refused.
//
// The database has no case-insensitive lookup and no list of its names, so
// this tries the casings zone names use -- each "/"-separated element either
// as a title ("Los_Angeles") or all upper case ("UTC", "GMT+5") -- and
// returns the first the database knows. A zone cased otherwise
// ("Port_of_Spain") gets no suggestion, only the refusal.
func zoneSpelling(tz string) string {
	elements := strings.Split(tz, "/")
	if len(elements) > 4 {
		return ""
	}
	for mask := 0; mask < 1<<len(elements); mask++ {
		recased := make([]string, len(elements))
		for i, element := range elements {
			if mask&(1<<i) != 0 {
				recased[i] = strings.ToUpper(element)
			} else {
				recased[i] = titleZoneElement(element)
			}
		}
		candidate := strings.Join(recased, "/")
		if candidate == tz || candidate == "Local" {
			continue
		}
		if _, err := time.LoadLocation(candidate); err == nil {
			return candidate
		}
	}
	return ""
}

// titleZoneElement upper-cases the first letter of each word of a zone name
// element and lower-cases the rest, words being separated by "_" or "-".
func titleZoneElement(element string) string {
	b := []byte(strings.ToLower(element))
	start := true
	for i, c := range b {
		if start && c >= 'a' && c <= 'z' {
			b[i] = c - 'a' + 'A'
		}
		start = c == '_' || c == '-'
	}
	return string(b)
}

// asciiUpper upper-cases ASCII letters and leaves every other byte alone,
// so a name with a non-ASCII letter in it matches no weekday.
func asciiUpper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 'a' + 'A'
		}
	}
	return string(b)
}

// asciiEqualFold is strings.EqualFold limited to ASCII. strings.EqualFold
// folds Unicode, so "laſt" (with a long s) equals "last" -- and the chart's
// schema, which spells each letter as an ASCII pair, refuses it. A schedule
// is ASCII; the two halves agree by refusing the rest.
func asciiEqualFold(s, t string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return strings.EqualFold(s, t)
}

// errHourlyUnsupported explains why there is no hourly form.
//
// "hourly:MM" used to be accepted: the minute was validated as 0-59 and then
// thrown away, so an operator who wrote hourly:15 got a policy firing every
// hour on the cluster's own boundary, with no warning that the 15 had been
// ignored. Silently altering configuration is worse than refusing it, and the
// minute cannot be honoured -- Qumulo's HOURLY_OR_LESS frequency takes a
// window and an interval, not an anchor, so an hourly policy has nowhere to
// put one. See hourlySchedule below for the field group it does take.
//
// The remedy is spelled out because this refusal breaks classes that were
// previously accepted, and the operator needs to know both what to write and
// that the behaviour they had was never what they asked for.
func errHourlyUnsupported(term string) error {
	return fmt.Errorf("%q is not supported: an hourly Qumulo policy takes an interval, not an anchor "+
		"minute, so a minute here was accepted and then silently discarded. Use %q for the same "+
		"effect, or %q to anchor a daily snapshot to a time",
		term, "every=1h", "daily@HH:MM")
}

// hourlySchedule builds the HOURLY_OR_LESS field group: every day, a window
// covering the whole day, and the requested interval within it.
func hourlySchedule(interval string, every int) qumulo.CreationSchedule {
	return qumulo.CreationSchedule{
		Frequency:         qumulo.ScheduleHourlyOrLess,
		OnDays:            []string{qumulo.DayEveryDay},
		WindowStartHour:   intPtr(0),
		WindowStartMinute: intPtr(0),
		WindowEndHour:     intPtr(23),
		WindowEndMinute:   intPtr(59),
		FireEveryInterval: interval,
		FireEvery:         every,
	}
}

func intPtr(v int) *int { return &v }

// parseEvery handles "every=Nm" and "every=Nh".
func parseEvery(term string) (qumulo.CreationSchedule, error) {
	spec := strings.TrimPrefix(term, "every=")
	if spec == "" {
		return qumulo.CreationSchedule{}, fmt.Errorf("every= must name an interval such as %q or %q", "30m", "6h")
	}

	unit := spec[len(spec)-1]
	count, err := strconv.Atoi(spec[:len(spec)-1])
	if err != nil || count < 1 {
		return qumulo.CreationSchedule{}, fmt.Errorf(
			"every=%s must be a positive number of minutes or hours, such as %q or %q", spec, "30m", "6h")
	}

	interval := qumulo.FireInMinutes
	switch unit {
	case 'm':
		if count > 59 {
			return qumulo.CreationSchedule{}, fmt.Errorf(
				"every=%s exceeds an hour; use hours instead, such as %q", spec, "1h")
		}
	case 'h':
		interval = qumulo.FireInHours
		if count > 23 {
			return qumulo.CreationSchedule{}, fmt.Errorf(
				"every=%s exceeds a day; use %q for a daily snapshot", spec, "daily@HH:MM")
		}
	default:
		return qumulo.CreationSchedule{}, fmt.Errorf(
			"every=%s must end in %q for minutes or %q for hours", spec, "m", "h")
	}

	return hourlySchedule(interval, count), nil
}

// parseDaily handles "daily@HH:MM".
//
// The API has no daily frequency: a daily schedule is the weekly one with
// every day selected.
func parseDaily(term string) (qumulo.CreationSchedule, error) {
	hour, minute, err := parseClock(strings.TrimPrefix(term, "daily@"))
	if err != nil {
		return qumulo.CreationSchedule{}, err
	}
	return qumulo.CreationSchedule{
		Frequency: qumulo.ScheduleDailyOrWeekly,
		OnDays:    []string{qumulo.DayEveryDay},
		Hour:      intPtr(hour),
		Minute:    intPtr(minute),
	}, nil
}

// parseWeekly handles "weekly@DAY,HH:MM", consuming the clock from the next
// term because the day and the time are separated by the same comma that
// separates modifiers.
func parseWeekly(head string, rest []string) (qumulo.CreationSchedule, []string, error) {
	dayName := strings.TrimSpace(strings.TrimPrefix(head, "weekly@"))
	// Upper-cased as ASCII only: strings.ToUpper folds "ſ" to S and "ı" to
	// I, so "ſun" was a Sunday to the driver and a typo to the chart's
	// schema.
	day, ok := weekdays[asciiUpper(dayName)]
	if !ok {
		return qumulo.CreationSchedule{}, nil, fmt.Errorf(
			"weekly@%s: unknown day; use a name such as %q or %q", dayName, "SUNDAY", "SUN")
	}
	if len(rest) == 0 {
		return qumulo.CreationSchedule{}, nil, fmt.Errorf(
			"weekly@%s needs a time; write it as %q", dayName, "weekly@SUN,02:00")
	}

	hour, minute, err := parseClock(strings.TrimSpace(rest[0]))
	if err != nil {
		return qumulo.CreationSchedule{}, nil, err
	}
	return qumulo.CreationSchedule{
		Frequency: qumulo.ScheduleDailyOrWeekly,
		OnDays:    []string{day},
		Hour:      intPtr(hour),
		Minute:    intPtr(minute),
	}, rest[1:], nil
}

// parseMonthly handles "monthly@D,HH:MM".
func parseMonthly(head string, rest []string) (qumulo.CreationSchedule, []string, error) {
	dayField := strings.TrimSpace(strings.TrimPrefix(head, "monthly@"))

	// The API allows 1-27 for a specific day, and a sentinel for the last day
	// -- so a schedule can never silently skip a short month.
	day := qumulo.DayOfMonthLast
	if !asciiEqualFold(dayField, "last") {
		var err error
		if day, err = parseUnit(dayField, 1, 27, "day of month"); err != nil {
			return qumulo.CreationSchedule{}, nil, fmt.Errorf(
				`%w (1-27, or "last" for the last day of the month)`, err)
		}
	}
	if len(rest) == 0 {
		return qumulo.CreationSchedule{}, nil, fmt.Errorf(
			"monthly@%s needs a time; write it as %q", dayField, "monthly@1,02:00")
	}

	hour, minute, err := parseClock(strings.TrimSpace(rest[0]))
	if err != nil {
		return qumulo.CreationSchedule{}, nil, err
	}
	return qumulo.CreationSchedule{
		Frequency:  qumulo.ScheduleMonthly,
		DayOfMonth: day,
		Hour:       intPtr(hour),
		Minute:     intPtr(minute),
	}, rest[1:], nil
}

// parseClock reads "HH:MM".
func parseClock(value string) (hour, minute int, err error) {
	h, m, found := strings.Cut(value, ":")
	if !found {
		return 0, 0, fmt.Errorf("%q is not a time; write it as %q", value, "02:00")
	}
	if hour, err = parseUnit(h, 0, 23, "hour"); err != nil {
		return 0, 0, err
	}
	if minute, err = parseUnit(m, 0, 59, "minute"); err != nil {
		return 0, 0, err
	}
	return hour, minute, nil
}

// parseUnit reads a bounded decimal field.
func parseUnit(value string, min, max int, what string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s %q is not a number", what, value)
	}
	if n < min || n > max {
		return 0, fmt.Errorf("%s %d is out of range %d-%d", what, n, min, max)
	}
	return n, nil
}

// validateRetention checks an expiration_time_to_live before it reaches the
// cluster.
//
// The bound is not arbitrary: the API takes "<quantity><units>" with the
// quantity **below 100**, so a year has to be written "12months" and "365days"
// is refused with a time_interval_parse_error that names nothing useful.
// Catching it here is what turns that into an actionable message.
func validateRetention(value string) error {
	if value == "" {
		return fmt.Errorf("keep= must name a duration such as %q, or %q", "7days", "never")
	}
	if asciiEqualFold(value, "never") {
		return nil
	}

	digits := strings.TrimRight(value, "abcdefghijklmnopqrstuvwxyz")
	unit := strings.TrimPrefix(value, digits)

	if !slices.Contains(retentionUnits, unit) {
		return fmt.Errorf("keep=%s: units must be one of %s", value, strings.Join(retentionUnits, ", "))
	}

	quantity, err := strconv.Atoi(digits)
	if err != nil || quantity < 1 {
		return fmt.Errorf("keep=%s must start with a positive number, such as %q", value, "7days")
	}
	if quantity >= 100 {
		return fmt.Errorf(
			"keep=%s: the quantity must be below 100, so write a year as %q rather than %q",
			value, "12months", "365days")
	}
	return nil
}

// scheduleInterval is the longest gap between two snapshots a policy's
// creation schedule takes: how far behind a policy-mode relationship's
// recovery point is expected to fall before its next step. Not
// knowing is false, never a guess.
//
// An hourly schedule fires only inside its daily window and on its days, so
// the night outside a narrow window, or a weekend off, is the gap when it is
// longer than the interval. A monthly schedule is taken as 31 days.
func scheduleInterval(cs qumulo.CreationSchedule) (time.Duration, bool) {
	const day = 24 * time.Hour
	switch cs.Frequency {
	case qumulo.ScheduleMonthly:
		return 31 * day, true
	case qumulo.ScheduleDailyOrWeekly:
		return longestDayGap(cs.OnDays)
	case qumulo.ScheduleHourlyOrLess:
		var unit time.Duration
		switch cs.FireEveryInterval {
		case qumulo.FireInMinutes:
			unit = time.Minute
		case qumulo.FireInHours:
			unit = time.Hour
		default:
			return 0, false
		}
		if cs.FireEvery < 1 {
			return 0, false
		}
		interval := time.Duration(cs.FireEvery) * unit
		gap := interval
		// The last snapshot before a quiet stretch can fall up to one
		// interval before it starts.
		if window, ok := windowLength(cs); ok && day-window > gap {
			gap = day - window + interval
		}
		if days, ok := longestDayGap(cs.OnDays); ok && days > day && days-day > gap {
			gap = days - day + interval
		}
		return gap, true
	}
	return 0, false
}

// windowLength is how long an hourly schedule's daily window is open, from
// its first minute to its last.
func windowLength(cs qumulo.CreationSchedule) (time.Duration, bool) {
	if cs.WindowStartHour == nil || cs.WindowStartMinute == nil || cs.WindowEndHour == nil || cs.WindowEndMinute == nil {
		return 0, false
	}
	start := time.Duration(*cs.WindowStartHour)*time.Hour + time.Duration(*cs.WindowStartMinute)*time.Minute
	end := time.Duration(*cs.WindowEndHour)*time.Hour + time.Duration(*cs.WindowEndMinute)*time.Minute
	if end < start {
		end += 24 * time.Hour
	}
	return end - start, true
}

// longestDayGap is the longest run from one scheduled day to the next, a
// day for EVERY_DAY and a week for a single day.
func longestDayGap(days []string) (time.Duration, bool) {
	const day = 24 * time.Hour
	order := map[string]int{"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}
	var on []int
	for _, d := range days {
		if d == qumulo.DayEveryDay {
			return day, true
		}
		if i, ok := order[d]; ok && !slices.Contains(on, i) {
			on = append(on, i)
		}
	}
	if len(on) == 0 {
		return 0, false
	}
	slices.Sort(on)
	longest := 0
	for i, d := range on {
		next := on[(i+1)%len(on)]
		gap := next - d
		if gap <= 0 {
			gap += 7
		}
		longest = max(longest, gap)
	}
	return time.Duration(longest) * day, true
}
