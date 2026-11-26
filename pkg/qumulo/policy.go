// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// Snapshot policy collection and single-policy resource.
//
// v1, not v3. The plan originally chose v3 for its single source_file_id, but
// v3's create decoder refuses every request body that can be constructed from
// its own OpenAPI schema. v1 is the documented API, and its
// arrays are constrained to exactly one entry each -- "The array must contain
// exactly one file ID", "must contain exactly one schedule" -- so it gives the
// same one-policy-per-volume shape the driver needs.
const (
	snapshotPoliciesPath = "/v1/snapshots/policies/"
	snapshotPolicyPath   = "/v1/snapshots/policies/%d"
)

// Schedule frequencies, as the cluster's enum spells them.
const (
	ScheduleMonthly       = "SCHEDULE_MONTHLY"
	ScheduleDailyOrWeekly = "SCHEDULE_DAILY_OR_WEEKLY"
	ScheduleHourlyOrLess  = "SCHEDULE_HOURLY_OR_LESS"
)

// Fire intervals, for SCHEDULE_HOURLY_OR_LESS only.
const (
	FireInMinutes = "FIRE_IN_MINUTES"
	FireInHours   = "FIRE_IN_HOURS"
)

// DayEveryDay selects all seven days in one value. The API takes three-letter
// day abbreviations -- SUN, MON, ... -- not full names, despite the cluster's
// own OpenAPI enum listing SUNDAY.
const DayEveryDay = "EVERY_DAY"

// DayOfMonthLast is the sentinel for "the last day of the month", whichever
// that is. Specific days are 1-27 only, so a schedule cannot silently skip
// February.
const DayOfMonthLast = 128

// CreationSchedule is when a policy takes snapshots.
//
// Which fields matter depends on Frequency, and the cluster rejects the whole
// object with an opaque "Invalid fields for schedule" rather than naming what
// it disliked -- so callers build this through the driver's schedule parser
// rather than by hand.
// The field set is exclusive to the frequency, and sending a field that does
// not belong to it is rejected outright with "Invalid fields for schedule" --
// not ignored. That is why every optional field carries omitempty and why the
// driver's schedule parser sets only the group that applies:
//
//	SCHEDULE_DAILY_OR_WEEKLY   hour, minute, on_days
//	SCHEDULE_MONTHLY           hour, minute, day_of_month
//	SCHEDULE_HOURLY_OR_LESS    on_days, window_*, fire_every_interval, fire_every
type CreationSchedule struct {
	Frequency string `json:"frequency"`
	Timezone  string `json:"timezone"`

	// Hour and Minute: MONTHLY and DAILY_OR_WEEKLY only. Pointers because 0
	// is a legitimate value (midnight) that omitempty would drop.
	Hour   *int `json:"hour,omitempty"`
	Minute *int `json:"minute,omitempty"`

	// OnDays: DAILY_OR_WEEKLY and HOURLY_OR_LESS only.
	OnDays []string `json:"on_days,omitempty"`

	// DayOfMonth: MONTHLY only. 1-27, or DayOfMonthLast.
	DayOfMonth int `json:"day_of_month,omitempty"`

	// The window and fire interval: HOURLY_OR_LESS only. Pointers for the
	// same reason as Hour -- a window starting at 00:00 is normal.
	WindowStartHour   *int   `json:"window_start_hour,omitempty"`
	WindowStartMinute *int   `json:"window_start_minute,omitempty"`
	WindowEndHour     *int   `json:"window_end_hour,omitempty"`
	WindowEndMinute   *int   `json:"window_end_minute,omitempty"`
	FireEveryInterval string `json:"fire_every_interval,omitempty"`
	FireEvery         int    `json:"fire_every,omitempty"`
}

// Schedule couples a creation schedule with how long its snapshots live.
type Schedule struct {
	CreationSchedule CreationSchedule `json:"creation_schedule"`
	// ExpirationTimeToLive is "<quantity><units>" where quantity is a
	// positive integer **below 100** and units is one of months, weeks, days,
	// hours, minutes -- so "12months", not "365days", which the cluster
	// rejects with time_interval_parse_error. Empty or "never" means the
	// snapshots never expire, and a policy left that way accumulates them
	// forever.
	ExpirationTimeToLive string `json:"expiration_time_to_live"`
}

// PolicyInfo is a snapshot policy as the driver uses it.
type PolicyInfo struct {
	ID           string
	Name         string
	SourceFileID string
	Enabled      bool
}

// policyWire is the API representation of a snapshot policy.
//
// The arrays are the v1 shape, and the API constrains both to exactly one
// entry, so the driver flattens them at this boundary rather than carrying a
// cardinality it cannot use.
type policyWire struct {
	ID            json.Number `json:"id"`
	Name          string      `json:"name"`
	SourceFileIDs []string    `json:"source_file_ids"`
	Schedules     []Schedule  `json:"schedules"`
	Enabled       bool        `json:"enabled"`
}

func (p policyWire) toPolicyInfo() PolicyInfo {
	info := PolicyInfo{
		ID:      p.ID.String(),
		Name:    p.Name,
		Enabled: p.Enabled,
	}
	if len(p.SourceFileIDs) > 0 {
		info.SourceFileID = p.SourceFileIDs[0]
	}
	return info
}

// CreatePolicy attaches a snapshot policy to a directory.
//
// Both arrays carry exactly one entry, which is what the API requires: a
// policy names one directory and one schedule.
//
// It takes no snapshot-name template. The v1 policy object has no such field,
// so one passed here used to be discarded, and the snapshots got the
// cluster's default name. Nothing depends on the name: the driver
// tells a scheduled snapshot from its own by policy_id
// (SnapshotInfo.CreatedByPolicy), never by name.
func (c *Client) CreatePolicy(
	ctx context.Context,
	name, sourceFileID string,
	schedule Schedule,
) (PolicyInfo, error) {
	req := struct {
		Name          string     `json:"name"`
		SourceFileIDs []string   `json:"source_file_ids"`
		Schedules     []Schedule `json:"schedules"`
		Enabled       bool       `json:"enabled"`
	}{
		Name:          name,
		SourceFileIDs: []string{sourceFileID},
		Schedules:     []Schedule{schedule},
		Enabled:       true,
	}

	var created policyWire
	if err := c.do(ctx, http.MethodPost, snapshotPoliciesPath, req, &created); err != nil {
		return PolicyInfo{}, err
	}
	return created.toPolicyInfo(), nil
}

// ListPolicies returns every snapshot policy on the cluster.
func (c *Client) ListPolicies(ctx context.Context) ([]PolicyInfo, error) {
	var listed struct {
		Entries []policyWire `json:"entries"`
	}
	if err := c.do(ctx, http.MethodGet, snapshotPoliciesPath, nil, &listed); err != nil {
		return nil, err
	}

	policies := make([]PolicyInfo, 0, len(listed.Entries))
	for _, entry := range listed.Entries {
		policies = append(policies, entry.toPolicyInfo())
	}
	return policies, nil
}

// GetPolicy reads one snapshot policy by ID: what a caller that knows the ID
// reads instead of listing every policy on the cluster.
func (c *Client) GetPolicy(ctx context.Context, policyID string) (PolicyInfo, error) {
	if _, err := strconv.ParseInt(policyID, 10, 64); err != nil {
		return PolicyInfo{}, fmt.Errorf("policy ID %q is not a number: %w", policyID, err)
	}
	var found policyWire
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf(snapshotPolicyByIDPath, url.PathEscape(policyID)), nil, &found); err != nil {
		return PolicyInfo{}, err
	}
	return found.toPolicyInfo(), nil
}

// DeletePolicy removes a snapshot policy by ID.
//
// The ID is a JSON number in this API, unlike file and quota IDs, so it is
// parsed and formatted as one in the path: a parsed integer has nothing to
// escape, which is why this route alone has no url.PathEscape.
func (c *Client) DeletePolicy(ctx context.Context, policyID string) error {
	id, err := strconv.ParseInt(policyID, 10, 64)
	if err != nil {
		return fmt.Errorf("policy ID %q is not a number: %w", policyID, err)
	}
	return c.do(ctx, http.MethodDelete, fmt.Sprintf(snapshotPolicyPath, id), nil, nil)
}
