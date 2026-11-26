// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingObserver keeps every event, under a lock because the real one is
// shared by every per-request client.
type recordingObserver struct {
	mu           sync.Mutex
	requests     []RequestEvent
	quotas       []QuotaEvent
	capacities   []CapacityEvent
	replications []ReplicationEvent
	protections  []VolumeProtectionEvent
	stagings     []RestoreStagingEvent
}

func (o *recordingObserver) ObserveRestoreStaging(_ context.Context, e RestoreStagingEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stagings = append(o.stagings, e)
}

func (o *recordingObserver) ObserveVolumeProtection(_ context.Context, e VolumeProtectionEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.protections = append(o.protections, e)
}

func (o *recordingObserver) ObserveRequest(_ context.Context, e RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, e)
}

func (o *recordingObserver) ObserveQuota(_ context.Context, e QuotaEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.quotas = append(o.quotas, e)
}

func (o *recordingObserver) ObserveCapacity(_ context.Context, e CapacityEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.capacities = append(o.capacities, e)
}

func (o *recordingObserver) ObserveReplication(_ context.Context, e ReplicationEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.replications = append(o.replications, e)
}

func TestPathTemplateBoundsTheLabel(t *testing.T) {
	for _, tc := range []struct{ path, want string }{
		{"/v1/version", "/v1/version"},
		{"/v1/file-system", "/v1/file-system"},
		{"/v2/nfs/settings", "/v2/nfs/settings"},
		{"/v3/snapshots/", "/v3/snapshots/"},
		{"/v1/files/quotas/", "/v1/files/quotas/"},
		{"/v1/multitenancy/tenants/", "/v1/multitenancy/tenants/"},
		// The four substituted shapes.
		{"/v1/files/12884901888/info/attributes", "/v1/files/{id}/info/attributes"},
		{"/v1/files/quotas/104876396806445471986885804611", "/v1/files/quotas/{id}"},
		{"/v3/snapshots/499", "/v3/snapshots/{id}"},
		{"/v1/snapshots/policies/7", "/v1/snapshots/policies/{id}"},
		{"/v1/files/%2Fcsi%2Fvolumes/entries/", "/v1/files/{id}/entries/"},
		{
			"/v2/replication/source-relationships/edfd8da2-dcca-4075-ae5d-bebaa97c1b98/status",
			"/v2/replication/source-relationships/{id}/status",
		},
		{"/v2/replication/source-relationships/status/", "/v2/replication/source-relationships/status/"},
		{"/v2/replication/target-relationships/status/", "/v2/replication/target-relationships/status/"},
		// A query string is a value, not a path, and carries limits and
		// cursors that would each mint their own series.
		{"/v1/files/%2Fcsi/entries/?limit=1000", "/v1/files/{id}/entries/"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			assert.Equal(t, tc.want, PathTemplate(tc.path))
		})
	}
}

func TestClusterLabelIsTheHostAndNothingElse(t *testing.T) {
	for _, tc := range []struct{ baseURL, want string }{
		{"https://qumulo.example.com:8000", "qumulo.example.com:8000"},
		{"https://qumulo.example.com", "qumulo.example.com"},
		{"http://192.0.2.20:8000", "192.0.2.20:8000"},
		// Userinfo is dropped, so a base URL that somehow carried one could
		// not put it in a label.
		{"https://user:secret@qumulo.example.com:8000", "qumulo.example.com:8000"},
		{"qumulo.example.com:8000/", "qumulo.example.com:8000"},
		{"", ""},
	} {
		t.Run(tc.baseURL, func(t *testing.T) {
			got := ClusterLabel(tc.baseURL)
			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, "secret")
		})
	}
}

// A client with no Observer must behave exactly as it did before this
// existed. Nothing here asserts a metric; it asserts the absence of a crash
// and of a behaviour change on the path every deployment takes by default.
func TestANilObserverChangesNothing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"total_size_bytes":"100","free_size_bytes":"40"}`))
	}))
	defer server.Close()

	client := New(server.URL, "token")
	capacity, err := client.GetCapacity(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(100), capacity.TotalSizeBytes)
}

func TestEveryRoundTripIsObservedWithItsStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"total_size_bytes":"100","free_size_bytes":"40"}`))
	}))
	defer server.Close()

	observer := &recordingObserver{}
	client := New(server.URL, "token")
	client.Observer = observer

	_, err := client.GetCapacity(context.Background())
	require.NoError(t, err)

	require.Len(t, observer.requests, 1)
	event := observer.requests[0]
	assert.Equal(t, http.MethodGet, event.Method)
	assert.Equal(t, "/v1/file-system", event.PathTemplate)
	assert.Equal(t, "200", event.Status)
	assert.True(t, event.Reachable)
	assert.Equal(t, ClusterLabel(server.URL), event.Cluster)

	require.Len(t, observer.capacities, 1)
	assert.Equal(t, uint64(40), observer.capacities[0].FreeSizeBytes)
}

// The reachability rule, stated as a table because it is the one judgement
// this package makes rather than reports. The 401 row is the case where the array
// was up and the token was dead, and the controller reported Ready for 36
// minutes.
func TestReachabilityFollowsWhetherTheArrayCanBeUsed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		reachable bool
	}{
		{name: "200 is reachable", status: http.StatusOK, reachable: true},
		{name: "404 proves the array answered", status: http.StatusNotFound, reachable: true},
		{name: "500 proves the array answered", status: http.StatusInternalServerError, reachable: true},
		// A refused credential proves the array answered. A dead token and
		// a missing privilege are alerted on by their request rates instead.
		{name: "401 proves the array answered", status: http.StatusUnauthorized, reachable: true},
		{name: "403 proves the array answered", status: http.StatusForbidden, reachable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer server.Close()

			observer := &recordingObserver{}
			client := New(server.URL, "token")
			client.Observer = observer

			_, _ = client.GetCapacity(context.Background())

			require.Len(t, observer.requests, 1)
			assert.Equal(t, tc.reachable, observer.requests[0].Reachable)
		})
	}
}

func TestATransportFailureIsObservedWithNoStatusCode(t *testing.T) {
	observer := &recordingObserver{}
	// A port nothing listens on: the round trip fails before any status.
	client := New("http://127.0.0.1:1", "token")
	client.Observer = observer

	_, err := client.GetCapacity(context.Background())
	require.Error(t, err)

	require.Len(t, observer.requests, 1)
	assert.Equal(t, StatusTransport, observer.requests[0].Status)
	assert.False(t, observer.requests[0].Reachable)
	assert.Empty(t, observer.capacities, "a failed call reports no capacity")
}

func TestQuotaReadsAndWritesAreObserved(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"id":"12884901888","limit":"5368709120","capacity_usage":"69632"}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	observer := &recordingObserver{}
	client := New(server.URL, "token")
	client.Observer = observer
	ctx := context.Background()

	info, err := client.GetQuotaInfo(ctx, "12884901888")
	require.NoError(t, err)
	require.NoError(t, client.UpdateQuota(ctx, "12884901888", 10737418240))
	require.NoError(t, client.CreateQuota(ctx, "999", 1024))

	assert.True(t, info.UsageKnown)
	require.Len(t, observer.quotas, 3)

	assert.Equal(t, QuotaEvent{
		Cluster: ClusterLabel(server.URL), FileID: "12884901888",
		LimitBytes: 5368709120, UsedBytes: 69632, UsedKnown: true,
	}, observer.quotas[0])
	// An expansion reports the new limit, so the gauge does not sit at the
	// pre-expansion figure until something else reads the quota.
	assert.Equal(t, uint64(10737418240), observer.quotas[1].LimitBytes)
	assert.False(t, observer.quotas[1].UsedKnown)
	assert.Equal(t, "999", observer.quotas[2].FileID)
}

// A quota the array reports without capacity_usage must leave UsedKnown
// false rather than reporting zero bytes used.
func TestAQuotaWithNoUsageIsObservedAsUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"12884901888","limit":"5368709120"}`))
	}))
	defer server.Close()

	observer := &recordingObserver{}
	client := New(server.URL, "token")
	client.Observer = observer

	_, err := client.GetQuotaInfo(context.Background(), "12884901888")
	require.NoError(t, err)

	require.Len(t, observer.quotas, 1)
	assert.False(t, observer.quotas[0].UsedKnown)
	assert.Zero(t, observer.quotas[0].UsedBytes)
}

// The role must come from which listing the status was read out of. It
// cannot come from the status itself: a connected target reports
// source_address null, which is the measured fact the replication service
// already works around.
func TestReplicationStatusIsObservedWithTheRoleOfTheListingItCameFrom(t *testing.T) {
	const status = `[{"id":"edfd8da2-dcca-4075-ae5d-bebaa97c1b98","state":"ESTABLISHED",` +
		`"recovery_point":"2026-09-14T11:50:00Z","queued_snapshot_count":2}]`

	for _, tc := range []struct {
		name string
		call func(*Client) error
		want string
	}{
		{
			name: "source listing",
			call: func(c *Client) error { _, err := c.ListSourceStatus(context.Background()); return err },
			want: RoleSource,
		},
		{
			name: "target listing",
			call: func(c *Client) error { _, err := c.ListTargetStatus(context.Background()); return err },
			want: RoleTarget,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(status))
			}))
			defer server.Close()

			observer := &recordingObserver{}
			client := New(server.URL, "token")
			client.Observer = observer
			before := time.Now()

			require.NoError(t, tc.call(client))
			require.Len(t, observer.replications, 1)
			assert.Equal(t, tc.want, observer.replications[0].Role)
			// When the request went out, so a listing that raced a delete
			// cannot bring the deleted relationship back.
			started := observer.replications[0].Started
			assert.False(t, started.Before(before), "Started is when this request was sent")
			assert.False(t, started.After(time.Now()))
			require.Len(t, observer.replications[0].Relationships, 1)
			assert.Equal(t, 2, observer.replications[0].Relationships[0].QueuedSnapshotCount)
		})
	}
}

// The array's clock arrives as an event of its own, with no relationships on
// it, because ageing a recovery point against the controller's clock would
// report the drift between the two as lag.
func TestTheArrayClockIsObservedOnItsOwn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"time":"2026-09-14T12:00:00Z"}`))
	}))
	defer server.Close()

	observer := &recordingObserver{}
	client := New(server.URL, "token")
	client.Observer = observer

	when, err := client.ClusterTime(context.Background())
	require.NoError(t, err)

	require.Len(t, observer.replications, 1)
	assert.Empty(t, observer.replications[0].Role)
	assert.Empty(t, observer.replications[0].Relationships)
	assert.Equal(t, when, observer.replications[0].ArrayTime)
}

// A relationship delete reports the removal, so the metrics can retire
// it; a delete the array refused reports nothing.
func TestDeletingARelationshipReportsItsRemoval(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   bool
	}{
		{"deleted", http.StatusOK, true},
		{"already gone", http.StatusNotFound, true},
		{"refused", http.StatusInternalServerError, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer server.Close()

			for _, del := range []func(*Client) error{
				func(c *Client) error { return c.DeleteSource(context.Background(), "rel-1") },
				func(c *Client) error { return c.DeleteTarget(context.Background(), "rel-1") },
			} {
				observer := &recordingObserver{}
				client := New(server.URL, "token")
				client.Observer = observer

				_ = del(client)

				if !tc.want {
					assert.Empty(t, observer.replications)
					continue
				}
				require.Len(t, observer.replications, 1)
				assert.Equal(t, "rel-1", observer.replications[0].Removed)
				assert.Empty(t, observer.replications[0].Relationships)
			}
		})
	}
}

// A replication listing made for a labelled request carries the label,
// so the exporter can tell the driver's relationships from everyone else's.
func TestAReplicationListingCarriesTheRequestsSubject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	observer := &recordingObserver{}
	client := New(server.URL, "token")
	client.Observer = observer

	const handle = "qumulo.example.com##/csi/volumes/pvc-abc"
	_, err := client.ListSourceStatus(WithSubject(context.Background(), handle))
	require.NoError(t, err)
	require.Len(t, observer.replications, 1)
	assert.Equal(t, handle, observer.replications[0].Subject)
}
