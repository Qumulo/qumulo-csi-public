// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sourceStatusBody is a source-end status as Core 7.9.2.2 rendered it in the
// lab, trimmed to the fields the driver reads. Note the trailing slash on
// both root paths and the string-typed nanoseconds -- both are real.
const sourceStatusBody = `{
  "id": "edfd8da2-dcca-4075-ae5d-bebaa97c1b98",
  "target_address": "192.0.2.20",
  "target_port": 3712,
  "source_root_read_only": false,
  "replication_enabled": true,
  "replication_mode": "REPLICATION_CONTINUOUS",
  "snapshot_policies": [{"id": 7, "target_expiration": "same_as_policy"}],
  "state": "ESTABLISHED",
  "end_reason": "",
  "source_cluster_name": "CSI-TEST-1",
  "source_cluster_uuid": "5453f61e-19e2-4000-a9bd-71fc9eba3e6b",
  "source_root_path": "/csi/volumes/pvc-25f8328e/",
  "target_cluster_name": "CSI-TEST-2",
  "target_cluster_uuid": "b7e1642d-a379-4784-829d-46f6a6d2d9e8",
  "target_root_path": "/csi/volumes/pvc-25f8328e/",
  "target_root_read_only": true,
  "job_state": "REPLICATION_NOT_RUNNING",
  "job_start_time": "",
  "recovery_point": "2026-09-11T18:36:03.001016496Z",
  "error_from_last_job": "",
  "duration_of_last_job": {"nanoseconds": "1500000000"},
  "source_root_id": "104876396806445471986890824623",
  "next_active_time": "",
  "replication_job_status": {"percent_complete": 42.5, "bytes_transferred": "1048576"},
  "queued_snapshot_count": 0,
  "recovery_point_snapshot": {"id": 499, "name": "replication_to_CSI-TEST-2"}
}`

// targetStatusBody is the same relationship from the target end. The source
// address and port are JSON null while connected, which is why the wire
// struct carries them as pointers.
const targetStatusBody = `{
  "id": "edfd8da2-dcca-4075-ae5d-bebaa97c1b98",
  "state": "ESTABLISHED",
  "source_cluster_uuid": "5453f61e-19e2-4000-a9bd-71fc9eba3e6b",
  "source_root_path": "/csi/volumes/pvc-25f8328e/",
  "source_root_read_only": false,
  "source_address": null,
  "source_port": null,
  "target_cluster_uuid": "b7e1642d-a379-4784-829d-46f6a6d2d9e8",
  "target_root_path": "/csi/volumes/pvc-25f8328e/",
  "target_root_read_only": true,
  "job_state": "REPLICATION_NOT_RUNNING",
  "recovery_point": "",
  "error_from_last_job": "",
  "duration_of_last_job": null,
  "target_root_id": "133376728980805811083003327059",
  "replication_enabled": true,
  "replication_job_status": null,
  "recovery_point_snapshot": null
}`

func TestListSourceStatusDecodesTheLabShape(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte("[" + sourceStatusBody + "]"))
	}, "token")

	statuses, err := client.ListSourceStatus(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "/v2/replication/source-relationships/status/", gotPath)
	require.Len(t, statuses, 1)
	s := statuses[0]
	assert.Equal(t, "edfd8da2-dcca-4075-ae5d-bebaa97c1b98", s.ID)
	assert.Equal(t, RelationshipEstablished, s.State)
	assert.True(t, s.JobIdle())
	assert.True(t, s.HasRecoveryPoint())
	assert.Equal(t, time.Date(2026, 9, 11, 18, 36, 3, 1016496, time.UTC), s.RecoveryPoint)
	assert.Equal(t, 1500*time.Millisecond, s.DurationOfLastJob)
	assert.Equal(t, "192.0.2.20", s.TargetAddress)
	assert.Equal(t, ReplicationModeContinuous, s.ReplicationMode)
	assert.Equal(t, "499", s.RecoveryPointSnapshotID)
	assert.Equal(t, uint64(1048576), s.BytesTransferred)
	assert.Equal(t, 42.5, s.PercentComplete)
	require.Len(t, s.SnapshotPolicies, 1)
	assert.Equal(t, SnapshotPolicyLink{ID: "7", TargetExpiration: "same_as_policy"}, s.SnapshotPolicies[0])
	// The trailing slash the array renders is cleaned at the boundary, so a
	// caller comparing against a volume's realPath gets a match.
	assert.Equal(t, "/csi/volumes/pvc-25f8328e", s.SourceRootPath)
	assert.Equal(t, "/csi/volumes/pvc-25f8328e", s.TargetRootPath)
}

func TestListTargetStatusToleratesNulls(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v2/replication/target-relationships/status/", r.URL.Path)
		w.Write([]byte("[" + targetStatusBody + "]"))
	}, "token")

	statuses, err := client.ListTargetStatus(context.Background())

	require.NoError(t, err)
	require.Len(t, statuses, 1)
	s := statuses[0]
	assert.Empty(t, s.SourceAddress)
	assert.False(t, s.HasRecoveryPoint(), "an empty recovery_point must read as none, not as the zero time being set")
	assert.Zero(t, s.DurationOfLastJob)
	assert.Empty(t, s.RecoveryPointSnapshotID)
	assert.True(t, s.TargetRootReadOnly)
	assert.Equal(t, "133376728980805811083003327059", s.TargetRootID)
}

func TestListStatusEmpty(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[]`))
	}, "token")

	statuses, err := client.ListSourceStatus(context.Background())
	require.NoError(t, err)
	assert.Empty(t, statuses)
}

func TestGetSourceAndTargetStatusPaths(t *testing.T) {
	var gotPaths []string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPaths = append(gotPaths, r.URL.Path)
		w.Write([]byte(sourceStatusBody))
	}, "token")

	_, err := client.GetSourceStatus(context.Background(), "abc")
	require.NoError(t, err)
	_, err = client.GetTargetStatus(context.Background(), "abc")
	require.NoError(t, err)

	assert.Equal(t, []string{
		"/v2/replication/source-relationships/abc/status",
		"/v2/replication/target-relationships/abc/status",
	}, gotPaths)
}

func TestCreateSourceSendsEveryFieldTheArrayRequires(t *testing.T) {
	handler := &recordingHandler{
		status: http.StatusOK,
		responseBody: `{"id":"rel-1","target_address":"192.0.2.20","target_port":3712,
			"source_root_id":"1048","source_root_read_only":false,"replication_enabled":true,
			"replication_mode":"REPLICATION_SNAPSHOT_POLICY_WITH_CONTINUOUS",
			"snapshot_policies":[{"id":7,"target_expiration":"same_as_policy"}]}`,
	}
	client := newTestClient(t, handler.serve, "token")

	rel, err := client.CreateSource(context.Background(), SourceSpec{
		SourceRootPath:     "/csi/volumes/pvc-a",
		TargetRootPath:     "/csi/volumes/pvc-a",
		TargetAddress:      "192.0.2.20",
		ReplicationMode:    ReplicationModeSnapshotPolicyContinuous,
		ReplicationEnabled: true,
		SnapshotPolicies:   []SnapshotPolicyLink{{ID: "7", TargetExpiration: TargetExpirationSameAsPolicy}},
	})

	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, handler.gotMethod)
	assert.Equal(t, "/v2/replication/source-relationships/", handler.gotPath)
	assert.Equal(t, "/csi/volumes/pvc-a", handler.gotBody["source_root_path"])
	assert.Equal(t, "/csi/volumes/pvc-a", handler.gotBody["target_root_path"])
	assert.Equal(t, "192.0.2.20", handler.gotBody["target_address"])
	// The port defaults to 3712 and is always sent.
	assert.Equal(t, float64(ReplicationPort), handler.gotBody["target_port"])
	assert.Equal(t, true, handler.gotBody["replication_enabled"])
	assert.Equal(t, ReplicationModeSnapshotPolicyContinuous, handler.gotBody["replication_mode"])
	// The policy ID travels as a JSON number, as the array renders it.
	policies, ok := handler.gotBody["snapshot_policies"].([]any)
	require.True(t, ok)
	require.Len(t, policies, 1)
	assert.Equal(t, float64(7), policies[0].(map[string]any)["id"])

	assert.Equal(t, "rel-1", rel.ID)
	assert.Equal(t, ReplicationModeSnapshotPolicyContinuous, rel.ReplicationMode)
	require.Len(t, rel.SnapshotPolicies, 1)
	assert.Equal(t, "7", rel.SnapshotPolicies[0].ID)
}

func TestCreateSourceDefaultsModeAndSendsAnEmptyPolicyList(t *testing.T) {
	handler := &recordingHandler{status: http.StatusOK, responseBody: `{"id":"rel-1"}`}
	client := newTestClient(t, handler.serve, "token")

	_, err := client.CreateSource(context.Background(), SourceSpec{
		SourceRootPath: "/a", TargetRootPath: "/a", TargetAddress: "10.0.0.2", ReplicationEnabled: true,
	})

	require.NoError(t, err)
	assert.Equal(t, ReplicationModeContinuous, handler.gotBody["replication_mode"])
	// [] rather than null: nil would marshal to JSON null.
	assert.Equal(t, []any{}, handler.gotBody["snapshot_policies"])
}

func TestPatchSourceSendsOnlyWhatChanged(t *testing.T) {
	var gotRaw map[string]json.RawMessage
	var gotMethod, gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotRaw))
		w.Write([]byte(`{"id":"rel-1","source_root_read_only":true,"replication_mode":"REPLICATION_CONTINUOUS","snapshot_policies":[]}`))
	}, "token")

	frozen := true
	rel, err := client.PatchSource(context.Background(), "rel-1", SourcePatch{SourceRootReadOnly: &frozen})

	require.NoError(t, err)
	assert.Equal(t, http.MethodPatch, gotMethod)
	assert.Equal(t, "/v2/replication/source-relationships/rel-1", gotPath)
	assert.Equal(t, `true`, string(gotRaw["source_root_read_only"]))
	// A partial update: the fields not named must not be on the wire at all,
	// or a freeze would also reset the mode and unlink every policy.
	_, hasMode := gotRaw["replication_mode"]
	_, hasPolicies := gotRaw["snapshot_policies"]
	_, hasEnabled := gotRaw["replication_enabled"]
	assert.False(t, hasMode || hasPolicies || hasEnabled, "unset patch fields leaked onto the wire: %v", gotRaw)
	assert.True(t, rel.SourceRootReadOnly)
}

func TestPatchSourceUnfreezeSendsExplicitFalse(t *testing.T) {
	var gotRaw map[string]json.RawMessage
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotRaw))
		w.Write([]byte(`{"id":"rel-1"}`))
	}, "token")

	unfrozen := false
	_, err := client.PatchSource(context.Background(), "rel-1", SourcePatch{SourceRootReadOnly: &unfrozen})

	require.NoError(t, err)
	// This is the whole reason the field is a pointer: false must be sent.
	assert.Equal(t, `false`, string(gotRaw["source_root_read_only"]))
}

func TestPatchSourceReappliesModeAndPolicyAfterReverse(t *testing.T) {
	var gotRaw map[string]json.RawMessage
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotRaw))
		w.Write([]byte(`{"id":"rel-1"}`))
	}, "token")

	links := []SnapshotPolicyLink{{ID: "9", TargetExpiration: TargetExpirationSameAsPolicy}}
	_, err := client.PatchSource(context.Background(), "rel-1", SourcePatch{
		ReplicationMode:  ReplicationModeSnapshotPolicyContinuous,
		SnapshotPolicies: &links,
	})

	require.NoError(t, err)
	assert.JSONEq(t, `"REPLICATION_SNAPSHOT_POLICY_WITH_CONTINUOUS"`, string(gotRaw["replication_mode"]))
	assert.JSONEq(t, `[{"id":9,"target_expiration":"same_as_policy"}]`, string(gotRaw["snapshot_policies"]))
	_, hasReadOnly := gotRaw["source_root_read_only"]
	assert.False(t, hasReadOnly)
}

func TestAuthorizeEncodesOptionsAsQueryParameters(t *testing.T) {
	tests := []struct {
		name          string
		allowCreate   bool
		allowNonEmpty bool
		wantQuery     string
	}{
		{name: "neither", wantQuery: ""},
		{name: "create only", allowCreate: true, wantQuery: "allow-fs-path-create=true"},
		{name: "both", allowCreate: true, allowNonEmpty: true,
			wantQuery: "allow-fs-path-create=true&allow-non-empty-directory=true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotQuery, gotMethod string
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotPath, gotQuery = r.Method, r.URL.Path, r.URL.RawQuery
				w.Write([]byte(targetStatusBody))
			}, "token")

			status, err := client.Authorize(context.Background(), "rel-1", tt.allowCreate, tt.allowNonEmpty)

			require.NoError(t, err)
			assert.Equal(t, http.MethodPost, gotMethod)
			assert.Equal(t, "/v2/replication/target-relationships/rel-1/authorize", gotPath)
			assert.Equal(t, tt.wantQuery, gotQuery)
			assert.Equal(t, RelationshipEstablished, status.State)
		})
	}
}

func TestTargetVerbsHitTheirPaths(t *testing.T) {
	var gotMethod, gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Write([]byte(targetStatusBody))
	}, "token")
	ctx := context.Background()

	_, err := client.MakeTargetWritable(ctx, "rel-1")
	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "/v2/replication/target-relationships/rel-1/make-writable", gotPath)

	_, err = client.Reconnect(ctx, "rel-1")
	require.NoError(t, err)
	assert.Equal(t, "/v2/replication/target-relationships/rel-1/reconnect", gotPath)

	require.NoError(t, client.DeleteTarget(ctx, "rel-1"))
	assert.Equal(t, http.MethodPost, gotMethod, "the target end is deleted with a POST verb, not DELETE")
	assert.Equal(t, "/v2/replication/target-relationships/rel-1/delete", gotPath)

	require.NoError(t, client.Replicate(ctx, "rel-1"))
	assert.Equal(t, "/v2/replication/source-relationships/rel-1/replicate", gotPath)

	require.NoError(t, client.DeleteSource(ctx, "rel-1"))
	assert.Equal(t, http.MethodDelete, gotMethod)
	assert.Equal(t, "/v2/replication/source-relationships/rel-1", gotPath)
}

func TestMakeTargetWritableOnDisconnectedIsTyped(t *testing.T) {
	// Measured on Core 7.9.2.2: the second make-writable is HTTP 400
	// with this class. The driver treats it as "already promoted".
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error_class":"relationship_manager_irreplicable_error",
			"description":"Disconnect and revert is not allowed on a disconnected relationship"}`))
	}, "token")

	_, err := client.MakeTargetWritable(context.Background(), "rel-1")

	require.Error(t, err)
	assert.True(t, IsAlreadyDisconnected(err))
	assert.False(t, IsNotFound(err))
}

func TestReplicateWhileEnabledIsTyped(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error_class":"replication_manual_replication_disallowed_error",
			"description":"Unable to start manual replication when replication is enabled."}`))
	}, "token")

	err := client.Replicate(context.Background(), "rel-1")

	require.Error(t, err)
	assert.True(t, IsManualReplicationDisallowed(err))
}

func TestReverseIsPostedToTheTargetArray(t *testing.T) {
	handler := &recordingHandler{
		status: http.StatusOK,
		// The measured shape: the reversed relationship comes back continuous
		// with no policies, whatever it was before.
		responseBody: `{"id":"rel-1","target_address":"192.0.2.10","target_port":3712,
			"source_root_id":"1333","source_root_read_only":false,"replication_enabled":true,
			"replication_mode":"REPLICATION_CONTINUOUS","snapshot_policies":[]}`,
	}
	client := newTestClient(t, handler.serve, "token")

	rel, err := client.Reverse(context.Background(), "rel-1", "192.0.2.10", 0)

	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, handler.gotMethod)
	assert.Equal(t, "/v2/replication/source-relationships/reverse-target-relationship", handler.gotPath)
	assert.Equal(t, "rel-1", handler.gotBody["target_relationship_id"])
	assert.Equal(t, "192.0.2.10", handler.gotBody["source_address"])
	assert.Equal(t, float64(ReplicationPort), handler.gotBody["source_port"])
	assert.Equal(t, "rel-1", rel.ID, "reverse keeps the relationship ID")
	assert.Equal(t, ReplicationModeContinuous, rel.ReplicationMode)
	assert.Empty(t, rel.SnapshotPolicies)
}

func TestClusterTime(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/time/status", r.URL.Path)
		w.Write([]byte(`{"config":{"use_ad_for_primary":false,"ntp_servers":[]},
			"time":"2026-09-14T16:53:53.904228123Z","status":"TIME_SYNCHRONIZING"}`))
	}, "token")

	now, err := client.ClusterTime(context.Background())

	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 9, 14, 16, 53, 53, 904228123, time.UTC), now)
}

func TestClusterTimeRejectsAnUnparseableClock(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"time":"yesterday"}`))
	}, "token")

	_, err := client.ClusterTime(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing the array's clock")
}

func TestGetPolicySchedule(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/snapshots/policies/7", r.URL.Path)
		w.Write([]byte(`{"id":7,"name":"csi-pvc-a","source_file_ids":["1048"],"enabled":true,
			"schedules":[{"creation_schedule":{"frequency":"SCHEDULE_HOURLY_OR_LESS","timezone":"UTC",
			"fire_every":15,"fire_every_interval":"FIRE_IN_MINUTES","on_days":["EVERY_DAY"]},
			"expiration_time_to_live":"1hours"}]}`))
	}, "token")

	schedule, err := client.GetPolicySchedule(context.Background(), "7")

	require.NoError(t, err)
	assert.Equal(t, ScheduleHourlyOrLess, schedule.CreationSchedule.Frequency)
	assert.Equal(t, 15, schedule.CreationSchedule.FireEvery)
	assert.Equal(t, "1hours", schedule.ExpirationTimeToLive)
}

func TestSetExportReadOnlyRewritesOnlyTheFlag(t *testing.T) {
	var gotMethods []string
	var gotBody map[string]json.RawMessage
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethods = append(gotMethods, r.Method)
		switch r.Method {
		case http.MethodGet:
			w.Write([]byte(`{"id":"4","export_path":"/csi/pvc-a","fs_path":"/csi/volumes/pvc-a",
				"description":"csi volume pvc-a","tenant_id":1,"fields_to_present_as_32_bit":[],
				"restrictions":[{"host_restrictions":["10.0.0.0/8"],"read_only":false,
				"require_privileged_port":false,"required_authentication_mode":"AUTHENTICATION_MODE_NONE",
				"user_mapping":"NFS_MAP_NONE"}]}`))
		case http.MethodPut:
			require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
			w.WriteHeader(http.StatusOK)
		}
	}, "token")

	require.NoError(t, client.SetExportReadOnly(context.Background(), "4", true))

	assert.Equal(t, []string{http.MethodGet, http.MethodPut}, gotMethods)
	// Fields this client does not model travel back untouched -- the array
	// requires every one to be present -- and the ID stays IN the body: Core
	// 7.9.2.2 answers a PUT without it with decode_error "field 'id' not
	// found" (measured twice on the lab arrays).
	assert.JSONEq(t, `"4"`, string(gotBody["id"]))
	assert.JSONEq(t, `[]`, string(gotBody["fields_to_present_as_32_bit"]))
	assert.JSONEq(t, `"csi volume pvc-a"`, string(gotBody["description"]))
	assert.JSONEq(t, `[{"host_restrictions":["10.0.0.0/8"],"read_only":true,
		"require_privileged_port":false,"required_authentication_mode":"AUTHENTICATION_MODE_NONE",
		"user_mapping":"NFS_MAP_NONE"}]`, string(gotBody["restrictions"]))
}

func TestCreateExportSendsReadOnly(t *testing.T) {
	handler := &recordingHandler{status: http.StatusOK, responseBody: `{"id":"5","export_path":"/x","fs_path":"/x"}`}
	client := newTestClient(t, handler.serve, "token")

	_, err := client.CreateExport(context.Background(), ExportSpec{ExportPath: "/x", FSPath: "/x", ReadOnly: true})

	require.NoError(t, err)
	restrictions := handler.gotBody["restrictions"].([]any)
	require.Len(t, restrictions, 1)
	assert.Equal(t, true, restrictions[0].(map[string]any)["read_only"])
}

func TestListExportsReportsReadOnly(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"entries":[
			{"id":"1","export_path":"/a","fs_path":"/a","restrictions":[{"read_only":true}]},
			{"id":"2","export_path":"/b","fs_path":"/b","restrictions":[{"read_only":true},{"read_only":false}]},
			{"id":"3","export_path":"/c","fs_path":"/c"}
		]}`))
	}, "token")

	exports, err := client.ListExports(context.Background())

	require.NoError(t, err)
	require.Len(t, exports, 3)
	assert.True(t, exports[0].ReadOnly)
	assert.False(t, exports[1].ReadOnly, "one read-write restriction admits writes, so the export is not read-only")
	assert.False(t, exports[2].ReadOnly, "an export with no restrictions listed is not known to be read-only")
}
