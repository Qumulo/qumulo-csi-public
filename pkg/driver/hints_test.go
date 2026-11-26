// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// relationshipHintAt returns the one relationship hint stored for key.
func relationshipHintAt(t *testing.T, s *replScenario, key string) relationshipHint {
	t.Helper()
	v, ok := s.server.hints.relationships.Load(key)
	require.True(t, ok, "no relationship hint for %q", key)
	return v.(relationshipHint)
}

// ageRelationshipHint moves the stored hint's clocks back, as if the poll
// that stored or last confirmed it had happened that long ago.
func ageRelationshipHint(s *replScenario, key string, sinceUse, sinceFound time.Duration) {
	v, _ := s.server.hints.relationships.Load(key)
	h := v.(relationshipHint)
	h.at = h.at.Add(-sinceUse)
	h.found = h.found.Add(-sinceFound)
	s.server.hints.relationships.Store(key, h)
}

// A relationship read back by ID kept being trusted only for
// reportHintReuse from the listing, so a protected volume relisted both
// status tables every 10 minutes however often it was confirmed. Each
// confirming read now starts the window again, up to reportHintMaxAge.
func TestAConfirmedRelationshipHintSlides(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	key := arrayA + "\x00" + replVolumePath
	s.assess(t, s.handleA, nil)

	// Confirmed 9 minutes after the listing: no listing, and trusted for
	// another full window from now.
	ageRelationshipHint(s, key, 9*time.Minute, 9*time.Minute)
	a0 := s.a.statusListings
	s.assess(t, s.handleA, nil)
	assert.Equal(t, a0, s.a.statusListings, "a hint inside its window is read by ID")
	assert.WithinDuration(t, time.Now(), relationshipHintAt(t, s, key).at, time.Second, "confirming restarts the window")

	// 9 more minutes: 18 since the listing, 9 since it was confirmed.
	ageRelationshipHint(s, key, 9*time.Minute, 9*time.Minute)
	s.assess(t, s.handleA, nil)
	assert.Equal(t, a0, s.a.statusListings, "18 minutes after the listing, still read by ID")

	// Unconfirmed past the window: listed.
	ageRelationshipHint(s, key, reportHintReuse, 0)
	s.assess(t, s.handleA, nil)
	assert.Equal(t, a0+2, s.a.statusListings, "a hint not confirmed within its window is listed for")

	// Confirmed throughout, but found longer ago than the cap: listed, which
	// is what would see a second relationship rooted at the same path.
	a1 := s.a.statusListings
	ageRelationshipHint(s, key, 0, reportHintMaxAge)
	s.assess(t, s.handleA, nil)
	assert.Equal(t, a1+2, s.a.statusListings, "past reportHintMaxAge the table is listed again")
}

// A plain volume with no relationship listed both status tables -- and
// both again to look for a group covering it -- on every Info poll, because
// "none" was never remembered. A report now remembers it for
// noRelationshipReuse; an Enable from this controller clears it at once.
func TestInfoRemembersBrieflyThatAVolumeHasNoRelationship(t *testing.T) {
	s := newReplScenario(t)
	key := arrayA + "\x00" + replVolumePath
	info := func() {
		t.Helper()
		_, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())
		requireCode(t, err, codes.FailedPrecondition)
	}
	info()
	a0, b0 := s.a.statusListings, s.b.statusListings

	info()
	info()
	assert.Equal(t, a0, s.a.statusListings, "an unprotected volume's repeat poll lists nothing on the local array")
	assert.Equal(t, b0, s.b.statusListings, "nor on the peer")

	// Not slid by use: past the window it is listed again.
	ageRelationshipHint(s, key, noRelationshipReuse, 0)
	v, _ := s.server.hints.groupRoots.Load(key)
	g := v.(groupRootHint)
	g.at = g.at.Add(-noRelationshipReuse)
	s.server.hints.groupRoots.Store(key, g)
	info()
	assert.Greater(t, s.a.statusListings, a0, "past noRelationshipReuse the tables are listed again")

	// An acting lookup never answers from it.
	e, err := s.server.resolve(context.Background(), "test", s.handleA, "", classToB(), replSecrets(), false)
	require.NoError(t, err)
	a1 := s.a.statusListings
	_, err = s.server.findRelationship(context.Background(), "test", e, false)
	require.NoError(t, err)
	assert.Greater(t, s.a.statusListings, a1, "a lookup outside a report lists")

	// Enable here clears it: the next report finds the relationship.
	rel := s.protect(t, classToB())
	assert.Equal(t, rel.id, s.assess(t, s.handleA, nil).RelationshipID)
}

// Enable from the peer's side clears the hint kept for this end too.
func TestAnEnableClearsTheNoRelationshipHintOfBothEnds(t *testing.T) {
	s := newReplScenario(t)
	r := s.server
	r.hints.relationships.Store(arrayA+"\x00/csi/volumes/pvc-repl", noRelationshipHint(time.Hour))
	r.hints.relationships.Store(arrayB+"\x00/csi/volumes/pvc-repl", noRelationshipHint(time.Hour))
	r.hints.groupRoots.Store(arrayA+"\x00/csi/volumes/pvc-repl/inner", groupRootHint{at: time.Now(), ttl: time.Hour})
	r.hints.relationships.Store(arrayA+"\x00/csi/volumes/pvc-replica", noRelationshipHint(time.Hour))

	r.forgetNoRelationship(&replicationEnds{
		localAddr: arrayA, realPath: "/csi/volumes/pvc-repl", peerAddr: arrayB, peerPath: "/csi/volumes/pvc-repl",
	})

	var left []string
	r.hints.relationships.Range(func(k, _ any) bool { left = append(left, k.(string)); return true })
	r.hints.groupRoots.Range(func(k, _ any) bool { left = append(left, k.(string)); return true })
	assert.Equal(t, []string{arrayA + "\x00/csi/volumes/pvc-replica"}, left,
		"both ends and the paths below them are cleared; a sibling sharing the prefix is not")
}

// The hint maps grew by one entry per volume ever seen and were never
// pruned. A sweep drops each entry past its use.
func TestTheHintSweepDropsEntriesPastTheirUse(t *testing.T) {
	r := NewReplicationServer(nil)
	now := time.Now()
	r.hints.markers.Store("old", idHint{id: "1", at: now.Add(-2 * idleHintExpiry)})
	r.hints.markers.Store("new", newIDHint("2"))
	r.hints.policyIntervals.Store("old", policyIntervalHint{at: now.Add(-policyIntervalReuse)})
	r.hints.policyIntervals.Store("new", policyIntervalHint{at: now})
	r.hints.relationships.Store("old", noRelationshipHint(time.Minute))
	r.hints.relationships.Store("new", newRelationshipHint("7", roleSource))
	r.hints.groupRoots.Store("old", groupRootHint{at: now.Add(-time.Hour), ttl: reportHintReuse})
	r.hints.groupRoots.Store("new", groupRootHint{at: now, ttl: reportHintReuse})
	r.groupStaging.Store("old", groupStaged{at: now.Add(-groupRestageEvery)})
	r.groupStaging.Store("new", groupStaged{at: now})

	r.sweepHints(now.Add(2 * time.Minute))

	for name, m := range map[string]*sync.Map{
		"markers": &r.hints.markers, "policyIntervals": &r.hints.policyIntervals,
		"relationships": &r.hints.relationships, "groupRoots": &r.hints.groupRoots, "groupStaging": &r.groupStaging,
	} {
		var keys []string
		m.Range(func(k, _ any) bool { keys = append(keys, k.(string)); return true })
		assert.Equal(t, []string{"new"}, keys, name)
	}
}

// The sweep runs at most once per hintSweepEvery, whoever asks.
func TestAHintSweepIsDueOncePerInterval(t *testing.T) {
	var s hintSweeper
	now := time.Now()
	assert.True(t, s.due(now), "the first store sweeps")
	assert.False(t, s.due(now.Add(time.Minute)), "not again within the interval")
	assert.True(t, s.due(now.Add(hintSweepEvery)), "due once the interval has passed")

	var wg sync.WaitGroup
	var mu sync.Mutex
	won := 0
	later := now.Add(3 * hintSweepEvery)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.due(later) {
				mu.Lock()
				won++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, 1, won, "concurrent stores run one sweep")
}

// The per-process export hints are swept too, and a deleted volume's
// are dropped with it.
func TestDeleteVolumeDropsTheVolumesExportHints(t *testing.T) {
	cluster := newFakeCluster()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()
	srv := newIntegrationControllerServer(t, server.URL)
	params := map[string]string{
		"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
		"provisioningModel": "dedicated-export", "allowAnyClient": "true",
	}
	resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets: testSecrets(), Name: "pvc-swept", Parameters: params,
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
	})
	require.NoError(t, err)
	key := exportHintKey(qumulo.New(server.URL, "t"), "/csi-volumes/pvc-swept")
	_, hinted := exportHints.Load(key)
	require.True(t, hinted, "CreateVolume remembers the export it made")

	_, err = srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: resp.GetVolume().GetVolumeId(), Secrets: testSecrets(),
	})
	require.NoError(t, err)
	_, hinted = exportHints.Load(key)
	assert.False(t, hinted, "a deleted volume's export hint is dropped")
	_, hinted = noExportHints.Load(key)
	assert.False(t, hinted)

	exportHints.Store("stale", idHint{id: "9", at: time.Now().Add(-2 * idleHintExpiry)})
	noExportHints.Store("stale", time.Now().Add(-noExportHintTTL))
	sweepPackageHints(time.Now())
	_, hinted = exportHints.Load("stale")
	assert.False(t, hinted, "an export hint unused past idleHintExpiry is swept")
	_, hinted = noExportHints.Load("stale")
	assert.False(t, hinted, "a \"no export\" answer past its TTL is swept")
}

// Every DeleteVolume listed every snapshot policy on the cluster to
// find the volume's own. The policy CreateVolume made is now remembered and
// read back by ID; a policy no hint knew of is still found by listing.
func TestAVolumesPolicyIsReadByIDNotListed(t *testing.T) {
	cluster := newFakeCluster()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()
	srv := newIntegrationControllerServer(t, server.URL)
	params := map[string]string{
		"server": testServer, "storeRealPath": "/csi-volumes", "storeExportPath": "/csi",
		"provisioningModel": "dedicated-export", "allowAnyClient": "true", "snapshotSchedule": "daily@02:00",
	}
	create := func(name string) string {
		t.Helper()
		resp, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Secrets: testSecrets(), Name: name, Parameters: params,
			VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		})
		require.NoError(t, err)
		return resp.GetVolume().GetVolumeId()
	}
	remove := func(handle string) {
		t.Helper()
		_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})
		require.NoError(t, err)
	}

	first := create("pvc-p1")
	require.Len(t, cluster.policies, 1)
	listed := cluster.policyListings
	remove(first)
	assert.Empty(t, cluster.policies, "the volume's policy is removed")
	assert.Equal(t, listed, cluster.policyListings, "and read by ID, not found by listing")

	// The policy renamed out of band: the hint no longer names it, so the
	// delete lists, finds nothing called csi-pvc-p2, and leaves it alone.
	second := create("pvc-p2")
	for id, p := range cluster.policies {
		p.Name = "renamed"
		cluster.policies[id] = p
	}
	listed = cluster.policyListings
	remove(second)
	assert.Equal(t, listed+1, cluster.policyListings, "a hint that no longer matches falls back to the listing")
	assert.Len(t, cluster.policies, 1, "a policy no longer carrying the volume's name is not the volume's")

	// No hint at all -- a restart -- still finds the policy by listing.
	third := create("pvc-p3")
	policyHints.Clear()
	listed = cluster.policyListings
	remove(third)
	assert.Equal(t, listed+1, cluster.policyListings)
	assert.Len(t, cluster.policies, 1, "the orphan sweep still works without a hint")
}

// Every shared-subdirectory CreateVolume listed the whole export table
// to check its path is covered. The covering export is now read back by ID
// for coverageHintTTL, and believed only while it still maps the volume.
func TestSharedCreateVolumeReadsTheCoveringExportByID(t *testing.T) {
	cluster := newFakeCluster()
	cluster.seedSharedExport()
	server := httptest.NewServer(cluster.handler())
	defer server.Close()
	srv := newIntegrationControllerServer(t, server.URL)
	create := func(name string) error {
		_, err := srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Secrets: testSecrets(), Name: name, Parameters: testParams(),
			VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		})
		return err
	}

	require.NoError(t, create("pvc-c1"))
	listed := cluster.exportListings
	require.NoError(t, create("pvc-c2"))
	require.NoError(t, create("pvc-c3"))
	assert.Equal(t, listed, cluster.exportListings, "later creates read the covering export by ID")

	// The covering export removed: not answered from the hint.
	seeded := cluster.exports["csi-store"]
	delete(cluster.exports, "csi-store")
	requireCode(t, create("pvc-c4"), codes.FailedPrecondition)
	assert.Equal(t, listed+1, cluster.exportListings)

	// Back, and past the TTL: listed again, which is what sees a deeper
	// export added meanwhile.
	cluster.exports["csi-store"] = seeded
	require.NoError(t, create("pvc-c5"))
	listed = cluster.exportListings
	coverageHints.Range(func(k, v any) bool {
		h := v.(idHint)
		h.at = h.at.Add(-coverageHintTTL)
		coverageHints.Store(k, h)
		return true
	})
	require.NoError(t, create("pvc-c6"))
	assert.Equal(t, listed+1, cluster.exportListings, "a coverage hint is trusted for coverageHintTTL only")
}

// Enable listed both relationship tables on the peer to match the
// relationship it had just found locally. It now reads that one by ID.
func TestARepeatEnableReadsThePeerByID(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	b0 := s.b.statusListings

	require.NoError(t, s.enable(t, classToB()))

	assert.Equal(t, b0, s.b.statusListings, "the peer's tables are not listed")
	assert.Equal(t, rel, s.fabric.onlyRel(), "and the same relationship is kept")
}
