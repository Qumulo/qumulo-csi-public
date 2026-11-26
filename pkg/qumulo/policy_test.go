// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreatePolicySendsTheRequiredFields is the test that matters here.
//
// The cluster rejects an incomplete policy with a decode error naming one
// missing field at a time, and rejects a malformed schedule with an opaque
// "Invalid fields for schedule" that names nothing. So the wire shape has to
// be asserted here, where it can be read, rather than discovered against a
// real cluster one 400 at a time.
func TestCreatePolicySendsTheRequiredFields(t *testing.T) {
	var got map[string]any
	var gotPath, gotMethod string

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &got))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":3,"name":"csi-pvc-a","source_file_ids":["123"],"enabled":true}`))
	}, "tok")

	hour, minute := 2, 30
	schedule := Schedule{
		CreationSchedule: CreationSchedule{
			Frequency: ScheduleDailyOrWeekly, Timezone: "UTC",
			OnDays: []string{"SUN"}, Hour: &hour, Minute: &minute,
		},
		ExpirationTimeToLive: "7days",
	}

	info, err := client.CreatePolicy(context.Background(), "csi-pvc-a", "123", schedule)

	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "/v1/snapshots/policies/", gotPath)

	// All four required fields present. Both arrays carry exactly one entry,
	// which is what the API requires.
	for _, key := range []string{"name", "source_file_ids", "schedules", "enabled"} {
		assert.Contains(t, got, key, "the cluster rejects a create missing %q", key)
	}
	assert.Equal(t, "csi-pvc-a", got["name"])
	assert.Equal(t, []any{"123"}, got["source_file_ids"])
	assert.Equal(t, true, got["enabled"])

	schedules := got["schedules"].([]any)
	require.Len(t, schedules, 1, "the API requires exactly one schedule")
	sched := schedules[0].(map[string]any)
	assert.Equal(t, "7days", sched["expiration_time_to_live"])

	creation := sched["creation_schedule"].(map[string]any)
	assert.Equal(t, "SCHEDULE_DAILY_OR_WEEKLY", creation["frequency"])
	assert.Equal(t, "UTC", creation["timezone"])
	// Three-letter abbreviation: SUNDAY is a decode error on the wire.
	assert.Equal(t, []any{"SUN"}, creation["on_days"])
	assert.EqualValues(t, 2, creation["hour"])
	assert.EqualValues(t, 30, creation["minute"])
	// The field set is exclusive to the frequency: a window field here is
	// rejected outright rather than ignored.
	for _, forbidden := range []string{"window_start_hour", "window_end_hour", "fire_every", "day_of_month"} {
		assert.NotContains(t, creation, forbidden,
			"%q belongs to a different frequency and must not be sent", forbidden)
	}

	assert.Equal(t, PolicyInfo{ID: "3", Name: "csi-pvc-a", SourceFileID: "123", Enabled: true}, info)
}

// An empty name template must be omitted rather than sent as "", which the
// cluster would take as an explicit empty naming pattern.
func TestCreatePolicyOmitsAnEmptyNameTemplate(t *testing.T) {
	var got map[string]any
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &got))
		w.Write([]byte(`{"id":"3"}`))
	}, "tok")

	_, err := client.CreatePolicy(context.Background(), "csi-a", "1", Schedule{})

	require.NoError(t, err)
	// v1 has no naming template field at all.
	assert.NotContains(t, got, "snapshot_name_template")
}

func TestListPolicies(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/snapshots/policies/", r.URL.Path)
		w.Write([]byte(`{"entries":[
			{"id":1,"name":"csi-a","source_file_ids":["10"],"enabled":true},
			{"id":2,"name":"nightly","source_file_ids":["20"],"enabled":false}]}`))
	}, "tok")

	got, err := client.ListPolicies(context.Background())

	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, PolicyInfo{ID: "1", Name: "csi-a", SourceFileID: "10", Enabled: true}, got[0])
	assert.Equal(t, PolicyInfo{ID: "2", Name: "nightly", SourceFileID: "20", Enabled: false}, got[1])
}

// An empty collection is the normal state on a cluster with no policies, and
// must decode to an empty slice rather than an error.
func TestListPoliciesEmpty(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"entries":[]}`))
	}, "tok")

	got, err := client.ListPolicies(context.Background())

	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestDeletePolicy(t *testing.T) {
	var gotPath, gotMethod string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		w.WriteHeader(http.StatusOK)
	}, "tok")

	require.NoError(t, client.DeletePolicy(context.Background(), "77"))
	assert.Equal(t, http.MethodDelete, gotMethod)
	assert.Equal(t, "/v1/snapshots/policies/77", gotPath)
}

func TestDeletePolicyNotFound(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}, "tok")

	err := client.DeletePolicy(context.Background(), "77")

	require.Error(t, err)
	assert.True(t, IsNotFound(err), "callers branch on IsNotFound to treat a gone policy as swept")
}

// One policy read by ID, with its name and source, so a caller that
// knows the ID need not list every policy on the cluster.
func TestGetPolicy(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/snapshots/policies/7", r.URL.Path)
		w.Write([]byte(`{"id":7,"name":"csi-a","source_file_ids":["10"],"schedules":[],"enabled":true}`))
	}, "tok")

	got, err := client.GetPolicy(context.Background(), "7")

	require.NoError(t, err)
	assert.Equal(t, PolicyInfo{ID: "7", Name: "csi-a", SourceFileID: "10", Enabled: true}, got)

	_, err = client.GetPolicy(context.Background(), "7?x")
	assert.Error(t, err, "an ID that is not a number never reaches a path")
}
