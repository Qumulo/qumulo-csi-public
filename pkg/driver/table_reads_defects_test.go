//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The whole-array tables is about, by the route that lists each one.
var dfxWholeTables = map[string]string{
	"exports":              "/v3/nfs/exports/",
	"tenants":              "/v1/multitenancy/tenants/",
	"snapshot policies":    "/v1/snapshots/policies/",
	"source relationships": "/v2/replication/source-relationships/status/",
	"target relationships": "/v2/replication/target-relationships/status/",
}

// dfxTableCounter is fakeCluster with the tenant listing it does not model,
// counting every GET of a whole table.
type dfxTableCounter struct {
	*fakeCluster
	mu       sync.Mutex
	listings map[string]int // route -> GETs
}

func (c *dfxTableCounter) handler() http.HandlerFunc {
	inner := c.fakeCluster.handler()
	return func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		p := r.URL.EscapedPath()
		if r.Method == http.MethodGet {
			for _, route := range dfxWholeTables {
				if p == route {
					c.listings[route]++
				}
			}
		}
		switch {
		case p == "/v1/multitenancy/tenants/" && r.Method == http.MethodGet:
			writeJSON(w, http.StatusOK, map[string]any{"entries": []map[string]any{
				{"id": 1, "name": "Default", "nfs_enabled": true}}})
		default:
			inner(w, r)
		}
	}
}

func (c *dfxTableCounter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for k, v := range c.listings {
		out[k] = v
	}
	return out
}

const dfxOps = 4

// dfxTableReads runs each RPC dfxOps times in quick succession on one
// controller against a fake array, and returns the full listings of each
// table it read, as "<phase>/<table>" -> listings over the dfxOps calls.
func dfxTableReads(t *testing.T) map[string]int {
	t.Helper()
	c := &dfxTableCounter{fakeCluster: newFakeCluster(), listings: map[string]int{}}
	c.seedSharedExport()
	server := httptest.NewServer(c.handler())
	t.Cleanup(server.Close)
	srv := newIntegrationControllerServer(t, server.URL)
	ctx := context.Background()
	out := map[string]int{}

	phase := func(name string, op func(i int) error) {
		before := c.snapshot()
		for i := 0; i < dfxOps; i++ {
			require.NoError(t, op(i), "%s #%d", name, i)
		}
		after := c.snapshot()
		for table, route := range dfxWholeTables {
			out[name+"/"+table] = after[route] - before[route]
		}
	}
	create := func(req *csi.CreateVolumeRequest, into []string, i int) error {
		resp, err := srv.CreateVolume(ctx, req)
		if err == nil {
			into[i] = resp.GetVolume().GetVolumeId()
		}
		return err
	}
	remove := func(handles []string) func(i int) error {
		return func(i int) error {
			_, err := srv.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: handles[i], Secrets: testSecrets()})
			return err
		}
	}

	shared := make([]string, dfxOps)
	phase("CreateVolume(shared)", func(i int) error {
		return create(&csi.CreateVolumeRequest{
			Name: fmt.Sprintf("pvc-t%d", i), Secrets: testSecrets(), Parameters: testParams(),
			VolumeCapabilities: mountCapabilities(), CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
		}, shared, i)
	})
	dedicated := make([]string, dfxOps)
	phase("CreateVolume(dedicated)", func(i int) error {
		req := dfxDedicatedExportCreate(fmt.Sprintf("pvc-d%d", i))
		req.Parameters["allowedClients"] = "192.0.2.0/24"
		return create(req, dedicated, i)
	})
	scheduled := make([]string, dfxOps)
	phase("CreateVolume(scheduled)", func(i int) error {
		req := dfxDedicatedExportCreate(fmt.Sprintf("pvc-s%d", i))
		req.Parameters["snapshotSchedule"] = "daily@02:00"
		req.Parameters["allowAnyClient"] = "true" // an open class must say so
		return create(req, scheduled, i)
	})
	phase("ControllerExpandVolume", func(i int) error {
		_, err := srv.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
			VolumeId: shared[i], Secrets: testSecrets(), CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB}})
		return err
	})
	phase("DeleteVolume(dedicated)", remove(dedicated))
	phase("DeleteVolume(scheduled)", remove(scheduled))
	phase("DeleteVolume(shared)", remove(shared))
	return out
}

// dfxAtMostHalf fails when key's listings show the table read on every call.
func dfxAtMostHalf(t *testing.T, reads map[string]int, key string) {
	t.Helper()
	n := reads[key]
	t.Logf("%s: %d listings for %d calls", key, n, dfxOps)
	assert.LessOrEqual(t, n, dfxOps/2, "%s: the whole table is listed on every call (%d listings for %d calls)", key, n, dfxOps)
}

// Create, Delete and Expand must not read whole tables of objects
// they are not about on every call. Each RPC is run dfxOps times in quick
// succession on one controller against a fake array, counting full listings of
// each table: a table listed once per call (cost growing with the array, times
// every call) fails; a lookup by ID, or a short-lived shared read, passes.
// tenant.go, policy.go); fix: cache per array with single-flight, or look up by ID.
//
// One test per table read, so a failure names the read that remains. The
// shared-create, scheduled-delete, tenant and repeat-Enable reads are fixed
// (the tenant table is reused for 5 minutes per cluster). The rest are the
// known limitation: the Qumulo API has no lookup by
// path or by name, and the identity check, the delete guard and Expand must
// see every export or relationship at or below a path, which only the whole
// table shows; a volume with no policy has no ID to read. Those tests are in
// table_reads_knownlimits_test.go, behind the knownlimits tag, so that
// `make test` stays green while they remain red by design.

// TestDefect_SharedCreateVolumeDoesNotListExports: the coverage check
// reads the covering export by ID (fixed).
func TestDefect_SharedCreateVolumeDoesNotListExports(t *testing.T) {
	dfxAtMostHalf(t, dfxTableReads(t), "CreateVolume(shared)/exports")
}

// TestDefect_ScheduledDeleteVolumeDoesNotListPolicies: the policy
// CreateVolume made is read back by ID (fixed).
func TestDefect_ScheduledDeleteVolumeDoesNotListPolicies(t *testing.T) {
	dfxAtMostHalf(t, dfxTableReads(t), "DeleteVolume(scheduled)/snapshot policies")
}

// TestDefect_TenantCreateVolumeDoesNotListTenants: known limitation
// (tenants are matched by name; no lookup by name).
func TestDefect_TenantCreateVolumeDoesNotListTenants(t *testing.T) {
	dfxAtMostHalf(t, dfxTableReads(t), "CreateVolume(dedicated)/tenants")
}

// TestDefect_RepeatEnableDoesNotListThePeerRelationshipTables: a repeat
// Enable reads the relationship it found locally from the peer by ID (fixed).
func TestDefect_RepeatEnableDoesNotListThePeerRelationshipTables(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	b0 := s.b.statusListings
	for i := 0; i < dfxOps; i++ {
		require.NoError(t, s.enable(t, classToB()))
	}
	n := s.b.statusListings - b0
	t.Logf("%d repeat Enables listed the peer's relationship tables %d times", dfxOps, n)
	assert.LessOrEqual(t, n, dfxOps/2, "each Enable listed both of the peer's relationship tables")
}

// DeleteVolume must back off while polling a tree delete that is
// still running, rather than polling every 500 ms for the whole 10 s budget,
// and must answer a still-running delete with the retryable Aborted, not an
// error that reads as a failure.
// (defaultTreeDeleteBudget 10 s, defaultTreeDeletePollWait 500 ms, fixed interval).
func TestDefect_TreeDeletePollBacksOff(t *testing.T) {
	var mu sync.Mutex
	var polls []time.Time
	m := &MockClient{
		LookupFileByPathFunc: volumeAt("/csi-volumes/pvc-big", "101"),
		TreeDeleteFinishedFunc: func(context.Context, string) (bool, error) {
			mu.Lock()
			polls = append(polls, time.Now())
			mu.Unlock()
			return false, nil // a large tree: still running
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	start := time.Now()
	_, err := newTestControllerServer(t, m).DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: testServer + "##/csi-volumes/pvc-big", Secrets: testSecrets()})
	elapsed := time.Since(start)

	mu.Lock()
	defer mu.Unlock()
	var gaps []time.Duration
	for i := 1; i < len(polls); i++ {
		gaps = append(gaps, polls[i].Sub(polls[i-1]).Round(10*time.Millisecond))
	}
	t.Logf("DeleteVolume answered after %v: %v; %d polls, gaps %v", elapsed.Round(10*time.Millisecond), err, len(polls), gaps)

	assert.Equal(t, codes.Aborted, status.Code(err), "a still-running tree delete must be answered Aborted, so the CO retries")
	assert.LessOrEqual(t, len(polls), 10, "polled %d times in one attempt; the fixed 500 ms poll gives about 20", len(polls))
	if len(gaps) >= 2 {
		assert.Greater(t, gaps[len(gaps)-1], gaps[0], "the interval between polls never grew")
	}
}

// CreateSnapshot and a planned promote's drain must not read the
// whole snapshot table on every call or poll
// (controller_server.go findSnapshotByName, replication_server.go
// snapshotsMissingOnTarget). One test per read; the first and third are
// fixed, the second and fourth a known limitation
// (no lookup by name; a replicated snapshot is renamed on the target and
// has no ID known in advance). The second and fourth are in
// table_reads_knownlimits_test.go.

func dfxCreateSnapshots(t *testing.T, name func(i int) string) int {
	t.Helper()
	m, _, listings := snapshotArray()
	srv := newNamedControllerServer(t, m)
	for i := 0; i < dfxOps; i++ {
		_, err := srv.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
			Secrets: testSecrets(), Name: name(i), SourceVolumeId: testHandle("pvc-abc"),
		})
		require.NoError(t, err)
	}
	t.Logf("%d CreateSnapshot calls listed the snapshot table %d times", dfxOps, *listings)
	return *listings
}

// TestDefect_RetriedCreateSnapshotDoesNotListSnapshots: a retry reads
// the snapshot its first attempt took by ID (fixed).
func TestDefect_RetriedCreateSnapshotDoesNotListSnapshots(t *testing.T) {
	n := dfxCreateSnapshots(t, func(int) string { return "snap-retried" })
	assert.LessOrEqual(t, n, dfxOps/2, "each retried CreateSnapshot listed every snapshot")
}

// dfxDrainListings runs a drain that waits out its budget for a policy
// snapshot, and counts each array's snapshot listings; the target is listed
// once per poll, so its count is the number of polls.
func dfxDrainListings(t *testing.T) (source, target int) {
	t.Helper()
	s, _, _ := gateScenario(t)
	a0, b0 := s.a.snapshotListings, s.b.snapshotListings
	err := s.promote(t, s.handleB(t), classToA(), false)
	require.Equal(t, codes.Aborted, status.Code(err), "%v", err)
	source, target = s.a.snapshotListings-a0-1, s.b.snapshotListings-b0 // less the marker lookup
	t.Logf("a drain of %d polls listed the source's snapshots %d times, the target's %d", target, source, target)
	return source, target
}

// TestDefect_DrainPollsDoNotListTheSourceSnapshotTable: the source's
// table is listed once per drain (fixed).
func TestDefect_DrainPollsDoNotListTheSourceSnapshotTable(t *testing.T) {
	source, polls := dfxDrainListings(t)
	require.Greater(t, polls, 3)
	assert.LessOrEqual(t, source, 1, "each drain poll listed the source's whole snapshot table")
}
