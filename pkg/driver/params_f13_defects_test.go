//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"strings"
	"testing"
)

// The edges the parameter checks left.

// TestDefect_TimezoneLocalIsRefused: time.LoadLocation("Local") answers
// the controller's own zone, so tz=Local passed validation and the literal
// string "Local" was sent to the array -- which is not a zone the array
// knows, and not the zone the operator meant either.
func TestDefect_TimezoneLocalIsRefused(t *testing.T) {
	for _, value := range []string{"daily@02:00,tz=Local", "every=1h,keep=7days,tz=Local"} {
		if _, err := parseSnapshotSchedule(value); err == nil {
			t.Errorf("%q was accepted; Local is the controller's zone, not one the array can be told", value)
		}
	}
}

// TestDefect_WrongCaseZoneNamesTheRightSpelling: zone names are
// case-sensitive in the IANA database the driver embeds, so tz=utc is
// refused although the chart's schema admits it. The refusal must say so,
// and name the spelling that would have worked.
func TestDefect_WrongCaseZoneNamesTheRightSpelling(t *testing.T) {
	for _, tc := range []struct{ tz, want string }{
		{tz: "utc", want: "UTC"},
		{tz: "america/los_angeles", want: "America/Los_Angeles"},
		{tz: "EUROPE/LONDON", want: "Europe/London"},
		{tz: "etc/gmt+5", want: "Etc/GMT+5"},
	} {
		_, err := parseSnapshotSchedule("daily@02:00,tz=" + tc.tz)
		if err == nil {
			t.Errorf("tz=%s was accepted; the zone database is case-sensitive", tc.tz)
			continue
		}
		msg := err.Error()
		if !strings.Contains(msg, "case-sensitive") {
			t.Errorf("tz=%s: the refusal does not say zone names are case-sensitive: %v", tc.tz, err)
		}
		if !strings.Contains(msg, `did you mean "`+tc.want+`"`) {
			t.Errorf("tz=%s: the refusal does not suggest %q: %v", tc.tz, tc.want, err)
		}
	}

	// A name with no case-insensitive match gets no suggestion.
	_, err := parseSnapshotSchedule("daily@02:00,tz=Mars/Olympus_Mons")
	if err == nil || strings.Contains(err.Error(), "did you mean") {
		t.Errorf("an unknown zone is refused with no guess, got %v", err)
	}
}

// TestDefect_CaseCollisionQuotesBothKeys: two keys that differ only in
// whitespace were reported as "server and server", which tells the operator
// nothing about which two lines of the class collided.
func TestDefect_CaseCollisionQuotesBothKeys(t *testing.T) {
	params := validParams()
	params[" server"] = "other.example.com"

	_, err := parseVolumeParams(params)
	if err == nil {
		t.Fatalf("two spellings of server were accepted")
	}
	if !strings.Contains(err.Error(), `" server" and "server"`) {
		t.Errorf("the refusal does not quote the two keys as written: %v", err)
	}
}

// TestDefect_DotElementErrorNamesTheKeyAsWritten: the refusal of a
// store path with a ".." element named the folded key "storerealpath",
// which grep does not find in the class that caused it.
func TestDefect_DotElementErrorNamesTheKeyAsWritten(t *testing.T) {
	params := validParams()
	params["storeRealPath"] = "/csi-volumes/.."

	_, err := parseVolumeParams(params)
	if err == nil {
		t.Fatalf("a store path that climbs was accepted")
	}
	if !strings.Contains(err.Error(), "storeRealPath ") {
		t.Errorf("the refusal does not name storeRealPath as the class writes it: %v", err)
	}
}
