// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// TestParseSnapshotScheduleAccepts pins the grammar an operator writes against.
//
// Each case asserts the exact API shape, not just that parsing succeeded: the
// cluster rejects a malformed schedule with an opaque "Invalid fields for
// schedule" that names nothing, so a wrong mapping here would only ever
// surface as an unexplained 400 against a real cluster.
func TestParseSnapshotScheduleAccepts(t *testing.T) {
	hourly := func(interval string, every int) qumulo.CreationSchedule {
		return qumulo.CreationSchedule{
			Frequency: qumulo.ScheduleHourlyOrLess, Timezone: "UTC",
			OnDays:            []string{qumulo.DayEveryDay},
			WindowStartHour:   intPtr(0),
			WindowStartMinute: intPtr(0),
			WindowEndHour:     intPtr(23),
			WindowEndMinute:   intPtr(59),
			FireEveryInterval: interval, FireEvery: every,
		}
	}

	tests := []struct {
		name  string
		input string
		want  qumulo.Schedule
	}{
		{
			name:  "every N minutes",
			input: "every=30m",
			want:  qumulo.Schedule{CreationSchedule: hourly(qumulo.FireInMinutes, 30)},
		},
		{
			name:  "every N hours",
			input: "every=6h",
			want:  qumulo.Schedule{CreationSchedule: hourly(qumulo.FireInHours, 6)},
		},
		{
			// The API has no daily frequency: it is DAILY_OR_WEEKLY with the
			// EVERY_DAY sentinel, and no window fields.
			name:  "daily uses the EVERY_DAY sentinel",
			input: "daily@02:00",
			want: qumulo.Schedule{CreationSchedule: qumulo.CreationSchedule{
				Frequency: qumulo.ScheduleDailyOrWeekly, Timezone: "UTC",
				OnDays: []string{qumulo.DayEveryDay}, Hour: intPtr(2), Minute: intPtr(0),
			}},
		},
		{
			// Full names are accepted from the operator and sent as the
			// three-letter form the API decodes.
			name:  "weekly maps a full day name to its abbreviation",
			input: "weekly@SUNDAY,02:30",
			want: qumulo.Schedule{CreationSchedule: qumulo.CreationSchedule{
				Frequency: qumulo.ScheduleDailyOrWeekly, Timezone: "UTC",
				OnDays: []string{"SUN"}, Hour: intPtr(2), Minute: intPtr(30),
			}},
		},
		{
			name:  "weekly with a short day name, lowercase",
			input: "weekly@wed,23:45",
			want: qumulo.Schedule{CreationSchedule: qumulo.CreationSchedule{
				Frequency: qumulo.ScheduleDailyOrWeekly, Timezone: "UTC",
				OnDays: []string{"WED"}, Hour: intPtr(23), Minute: intPtr(45),
			}},
		},
		{
			name:  "monthly",
			input: "monthly@1,02:00",
			want: qumulo.Schedule{CreationSchedule: qumulo.CreationSchedule{
				Frequency: qumulo.ScheduleMonthly, Timezone: "UTC",
				DayOfMonth: 1, Hour: intPtr(2), Minute: intPtr(0),
			}},
		},
		{
			// The sentinel, so a schedule never silently skips a short month.
			name:  "monthly on the last day",
			input: "monthly@last,02:00",
			want: qumulo.Schedule{CreationSchedule: qumulo.CreationSchedule{
				Frequency: qumulo.ScheduleMonthly, Timezone: "UTC",
				DayOfMonth: qumulo.DayOfMonthLast, Hour: intPtr(2), Minute: intPtr(0),
			}},
		},
		{
			name:  "retention",
			input: "daily@02:00,keep=7days",
			want: qumulo.Schedule{
				CreationSchedule: qumulo.CreationSchedule{
					Frequency: qumulo.ScheduleDailyOrWeekly, Timezone: "UTC",
					OnDays: []string{qumulo.DayEveryDay}, Hour: intPtr(2), Minute: intPtr(0),
				},
				ExpirationTimeToLive: "7days",
			},
		},
		{
			name:  "explicit timezone overrides the UTC default",
			input: "daily@02:00,tz=America/Los_Angeles",
			want: qumulo.Schedule{CreationSchedule: qumulo.CreationSchedule{
				Frequency: qumulo.ScheduleDailyOrWeekly, Timezone: "America/Los_Angeles",
				OnDays: []string{qumulo.DayEveryDay}, Hour: intPtr(2), Minute: intPtr(0),
			}},
		},
		{
			name:  "weekly with both modifiers, reversed",
			input: "weekly@FRI,18:00,tz=UTC,keep=30days",
			want: qumulo.Schedule{
				CreationSchedule: qumulo.CreationSchedule{
					Frequency: qumulo.ScheduleDailyOrWeekly, Timezone: "UTC",
					OnDays: []string{"FRI"}, Hour: intPtr(18), Minute: intPtr(0),
				},
				ExpirationTimeToLive: "30days",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSnapshotSchedule(tt.input)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestParseSnapshotScheduleRejects checks that every rejection is diagnosable.
//
// The message matters as much as the refusal: an operator who cannot tell what
// was wrong will guess, and the cluster will not help them.
func TestParseSnapshotScheduleRejects(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{"empty", "", "must not be empty"},
		{"gibberish", "every-other-tuesday", "unrecognized snapshot schedule"},
		{"unknown modifier", "daily@02:00,retain=7d", "unrecognized snapshot schedule term"},
		{"hour out of range", "daily@25:00", "hour 25 is out of range 0-23"},
		{"minute out of range", "daily@02:60", "minute 60 is out of range 0-59"},
		{"non-numeric time", "daily@ab:cd", "is not a number"},
		{"time without a colon", "daily@0200", "is not a time"},
		{"unknown weekday", "weekly@FUNDAY,02:00", "unknown day"},
		{"weekly without a time", "weekly@SUN", "needs a time"},
		{"monthly without a time", "monthly@1", "needs a time"},
		// 1-28 rather than 1-31: a schedule on the 30th silently skips
		// February, which is a gap a backup policy must not have.
		{"day of month beyond 27", "monthly@30,02:00", "out of range 1-27"},
		{"every without a unit", "every=30", "must end in"},
		{"every zero", "every=0m", "positive number"},
		{"every beyond an hour", "every=90m", "exceeds an hour"},
		{"every beyond a day", "every=48h", "exceeds a day"},
		{"empty keep", "daily@02:00,keep=", "must name a duration"},
		// The cluster caps the quantity below 100 and refuses anything else
		// with an unhelpful time_interval_parse_error.
		{"retention quantity too large", "daily@02:00,keep=365days", "must be below 100"},
		{"unknown retention unit", "daily@02:00,keep=7fortnights", "units must be one of"},
		{"empty tz", "daily@02:00,tz=", "must name a timezone"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSnapshotSchedule(tt.input)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// A malformed schedule must be refused during parameter validation, before
// CreateVolume has created anything on the cluster.
func TestVolumeParamsRejectsMalformedSchedule(t *testing.T) {
	params := testParams()
	params["snapshotSchedule"] = "daily@99:00"

	_, err := parseVolumeParams(params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "snapshotschedule")
	assert.Contains(t, err.Error(), "out of range")
}

// An absent schedule leaves the field nil, which is what tells CreateVolume to
// attach no policy at all.
func TestVolumeParamsScheduleIsOptional(t *testing.T) {
	parsed, err := parseVolumeParams(testParams())

	require.NoError(t, err)
	assert.Nil(t, parsed.SnapshotSchedule)
}

// A well-formed schedule reaches the parsed parameters as exactly what
// parseSnapshotSchedule makes of it, so CreateVolume attaches that policy.
// This replaced a test of the gated state, which always skipped: the
// parameter has been open since 2026-08-28 (snapshotScheduleEnabled).
func TestVolumeParamsCarryAWellFormedSchedule(t *testing.T) {
	const schedule = "daily@02:00,keep=7days"
	want, err := parseSnapshotSchedule(schedule)
	require.NoError(t, err)

	params := testParams()
	params["snapshotSchedule"] = schedule
	parsed, err := parseVolumeParams(params)

	require.NoError(t, err)
	require.NotNil(t, parsed.SnapshotSchedule)
	assert.Equal(t, want, *parsed.SnapshotSchedule)
}

// TestParseSnapshotScheduleRejectsHourly pins the hourly refusal.
//
// "hourly:MM" was accepted: the minute was validated as 0-59 and then
// discarded, so hourly:15 produced a policy firing on the cluster's own hour
// boundary and nothing said so. The value cannot be honoured -- Qumulo's
// HOURLY_OR_LESS frequency takes a window and an interval, with nowhere to put
// an anchor minute -- so it is refused instead, which is the only outcome that
// does not silently alter what the operator configured.
//
// This breaks classes that were previously accepted. That is the intent: those
// classes never had the schedule they asked for.
func TestParseSnapshotScheduleRejectsHourly(t *testing.T) {
	for _, input := range []string{
		"hourly:15",
		"hourly:0",
		"hourly:00",
		// Out of range before, so already refused -- but now for the right
		// reason rather than as a range error on a value about to be dropped.
		"hourly:60",
		"hourly:",
		// A bare "hourly" was never accepted either, but it used to fall
		// through to the generic "unrecognized schedule". It now gets the
		// explanation.
		"hourly",
		"hourly:15,keep=1hours",
	} {
		t.Run(input, func(t *testing.T) {
			_, err := parseSnapshotSchedule(input)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "not supported")
			assert.Contains(t, err.Error(), "every=1h",
				"the message must name the form that produces the same effect")
			assert.Contains(t, err.Error(), "silently discarded",
				"an operator upgrading into this refusal has to learn that the "+
					"schedule they had was not the one they wrote")
		})
	}
}

// The replacement form must actually produce the schedule hourly:MM only
// pretended to. Asserted here so the refusal above is not the whole story.
func TestParseSnapshotScheduleEveryOneHourReplacesHourly(t *testing.T) {
	schedule, err := parseSnapshotSchedule("every=1h")

	require.NoError(t, err)
	assert.Equal(t, qumulo.ScheduleHourlyOrLess, schedule.CreationSchedule.Frequency)
	assert.Equal(t, qumulo.FireInHours, schedule.CreationSchedule.FireEveryInterval)
	assert.Equal(t, 1, schedule.CreationSchedule.FireEvery)
}

// The advertised grammar must not offer a form the parser refuses. This is the
// mismatch that produced the defect in the first place -- the syntax string
// promised hourly:MM and the parser honoured half of it.
func TestScheduleSyntaxOffersNoHourlyForm(t *testing.T) {
	assert.NotContains(t, scheduleSyntax, "hourly")
}

// The interval a policy-mode relationship's recovery point steps at
// is the longest gap between two of its policy's snapshots.
func TestScheduleIntervalIsTheLongestGapBetweenSnapshots(t *testing.T) {
	hourly := func(every int, unit string, days []string, from, to [2]int) qumulo.CreationSchedule {
		return qumulo.CreationSchedule{
			Frequency: qumulo.ScheduleHourlyOrLess, OnDays: days, FireEvery: every, FireEveryInterval: unit,
			WindowStartHour: intPtr(from[0]), WindowStartMinute: intPtr(from[1]),
			WindowEndHour: intPtr(to[0]), WindowEndMinute: intPtr(to[1]),
		}
	}
	everyDay := []string{qumulo.DayEveryDay}
	allDay := [2][2]int{{0, 0}, {23, 59}}
	for _, tt := range []struct {
		name     string
		schedule qumulo.CreationSchedule
		want     time.Duration
		ok       bool
	}{
		{"every=30m", hourly(30, qumulo.FireInMinutes, everyDay, allDay[0], allDay[1]), 30 * time.Minute, true},
		{"every=6h", hourly(6, qumulo.FireInHours, everyDay, allDay[0], allDay[1]), 6 * time.Hour, true},
		{"hourly inside a 08:00-17:59 window: 17:00 to 08:00 is the gap",
			hourly(1, qumulo.FireInHours, everyDay, [2]int{8, 0}, [2]int{17, 59}), 15*time.Hour + time.Minute, true},
		{"hourly on weekdays only: Friday 23:00 to Monday 00:00 is the gap",
			hourly(1, qumulo.FireInHours, []string{"MON", "TUE", "WED", "THU", "FRI"}, allDay[0], allDay[1]), 49 * time.Hour, true},
		{"daily", qumulo.CreationSchedule{Frequency: qumulo.ScheduleDailyOrWeekly, OnDays: everyDay}, 24 * time.Hour, true},
		{"weekly", qumulo.CreationSchedule{Frequency: qumulo.ScheduleDailyOrWeekly, OnDays: []string{"SUN"}}, 7 * 24 * time.Hour, true},
		{"MON and WED: WED to MON is the gap",
			qumulo.CreationSchedule{Frequency: qumulo.ScheduleDailyOrWeekly, OnDays: []string{"WED", "MON"}}, 5 * 24 * time.Hour, true},
		{"monthly", qumulo.CreationSchedule{Frequency: qumulo.ScheduleMonthly, DayOfMonth: 1}, 31 * 24 * time.Hour, true},
		{"no fire interval", hourly(0, qumulo.FireInHours, everyDay, allDay[0], allDay[1]), 0, false},
		{"unknown unit", hourly(1, "FIRE_IN_FORTNIGHTS", everyDay, allDay[0], allDay[1]), 0, false},
		{"daily with no days", qumulo.CreationSchedule{Frequency: qumulo.ScheduleDailyOrWeekly}, 0, false},
		{"unknown frequency", qumulo.CreationSchedule{Frequency: "SCHEDULE_YEARLY"}, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := scheduleInterval(tt.schedule)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}
