// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// Two RPCs about the same volume racing each other, and the 409 and
// "already disconnected" answers the losing one gets. Every interleaving is
// forced: one RPC parks inside a call to an array (callGate) while the other
// runs, then is released -- no sleeps. The parked call holds nothing the
// other needs, and the channels order every access to the fake.

// callGate parks the first call of verb that reaches it until released.
type callGate struct {
	mu      sync.Mutex
	verb    string
	armed   bool
	parked  chan struct{}
	release chan struct{}
}

func newCallGate(verb string) *callGate {
	return &callGate{verb: verb, armed: true, parked: make(chan struct{}), release: make(chan struct{})}
}

func (g *callGate) wait(verb string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	hit := g.armed && verb == g.verb
	if hit {
		g.armed = false
	}
	g.mu.Unlock()
	if hit {
		close(g.parked)
		<-g.release
	}
}

// awaitGate waits for the gated call to park, or fails the test.
func awaitGate(t *testing.T, g *callGate, what string) {
	t.Helper()
	select {
	case <-g.parked:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never reached its %s", what, g.verb)
	}
}

// awaitDone waits for a goroutine's error, or fails the test.
func awaitDone(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never finished", what)
		return nil
	}
}

// hookArray is a fakeArray whose replication verbs pass a callGate first,
// and that can answer the 409s an array gives a request it already applied.
type hookArray struct {
	*fakeArray
	gate *callGate
	// createSourceConflict, when set, makes CreateSource answer 409:
	// "applied" after creating the relationship (a POST retried after its
	// first attempt landed), "refused" creating nothing.
	createSourceConflict string
	// policyConflicts makes CreatePolicy answer 409 for a name the array
	// already has, as Core does.
	policyConflicts bool
}

func (h *hookArray) CreateSource(ctx context.Context, spec qumulo.SourceSpec) (qumulo.Relationship, error) {
	h.gate.wait("CreateSource")
	switch h.createSourceConflict {
	case "applied":
		if _, err := h.fakeArray.CreateSource(ctx, spec); err != nil {
			return qumulo.Relationship{}, err
		}
		return qumulo.Relationship{}, fakeAPIError(http.StatusConflict, "replication_relationship_exists_error", "duplicate")
	case "refused":
		return qumulo.Relationship{}, fakeAPIError(http.StatusConflict, "replication_relationship_exists_error", "duplicate")
	}
	return h.fakeArray.CreateSource(ctx, spec)
}

func (h *hookArray) MakeTargetWritable(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	h.gate.wait("MakeTargetWritable")
	return h.fakeArray.MakeTargetWritable(ctx, id)
}

func (h *hookArray) Reconnect(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	h.gate.wait("Reconnect")
	return h.fakeArray.Reconnect(ctx, id)
}

func (h *hookArray) CreatePolicy(ctx context.Context, name, sourceFileID string, schedule qumulo.Schedule) (qumulo.PolicyInfo, error) {
	h.gate.wait("CreatePolicy")
	if h.policyConflicts {
		for _, p := range h.policies {
			if p.Name == name {
				return qumulo.PolicyInfo{}, fakeAPIError(http.StatusConflict, "api_snapshots_policy_name_conflict_error", "exists")
			}
		}
	}
	return h.fakeArray.CreatePolicy(ctx, name, sourceFileID, schedule)
}

// throughHooks routes every client the driver dials for addr through a
// hookArray over that array.
func throughHooks(s *replScenario, addr string, gate *callGate) *hookArray {
	h := &hookArray{fakeArray: s.fabric.arrays[addr], gate: gate}
	s.driver.NewClient = func(baseURL, token string) qumulo.ClientInterface {
		if hostOfURL(baseURL) == addr {
			return h
		}
		return s.fabric.newClient(baseURL, token)
	}
	return h
}

func count(calls []string, verb string) int {
	n := 0
	for _, c := range calls {
		if c == verb {
			n++
		}
	}
	return n
}

// Two Enables of one volume, both past the lookup that found nothing before
// either created: the second CreateSource is refused 409 and adopts the
// relationship the first made (createAndAuthorize's 409 row), and its
// Authorize finds it already authorized. One relationship, both OK.
func TestTwoConcurrentEnablesMakeOneRelationship(t *testing.T) {
	s := newReplScenario(t)
	gate := newCallGate("CreateSource")
	throughHooks(s, arrayA, gate)

	first := make(chan error, 1)
	go func() { first <- s.enable(t, classToB()) }()
	awaitGate(t, gate, "the first Enable")

	require.NoError(t, s.enable(t, classToB()), "the second Enable creates the relationship")
	close(gate.release)
	require.NoError(t, awaitDone(t, first, "the first Enable"), "the first adopts it")

	require.Equal(t, 1, s.fabric.relCount(), "one relationship, not two")
	rel := s.fabric.onlyRel()
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	assert.Equal(t, 2, count(s.a.calls, "CreateSource"), "the first Enable's create was refused as a duplicate")
	assert.Equal(t, 2, count(s.b.calls, "Authorize"), "and its authorize found it already authorized")
	assert.Len(t, s.b.exports, 1, "one replica export")
}

// A 409 for a relationship the array then does not list is not adopted
// blind: Aborted, retried.
func TestEnableRefusedAsADuplicateItCannotFindIsRetried(t *testing.T) {
	s := newReplScenario(t)
	h := throughHooks(s, arrayA, nil)
	h.createSourceConflict = "refused"

	err := s.enable(t, classToB())

	requireCode(t, err, codes.Aborted)
	assert.Contains(t, err.Error(), "as a duplicate, but none is listed")
	assert.Zero(t, s.fabric.relCount())
}

// Two promotes of one DR copy. The first parks right after freezing A,
// about to take the marker of the frozen tree; the second finds A frozen
// under the first's pending marker, so it knows the freeze is a promote's,
// drains, promotes and drops its marker. The first then finishes against a
// target already writable ("already disconnected"). No marker is left on A,
// and none is held -- the undo of this promote can still lift the freeze.
func TestTwoConcurrentPromotesLeaveNoFreezeMarkerBehind(t *testing.T) {
	for _, parkAt := range []string{"the marker of the frozen tree", "MakeTargetWritable"} {
		t.Run("first parked at "+parkAt, func(t *testing.T) {
			s := newReplScenario(t)
			rel := s.protect(t, classToB())
			var gate *callGate
			if parkAt == "MakeTargetWritable" {
				gate = newCallGate("MakeTargetWritable")
				throughHooks(s, arrayB, gate)
			} else {
				gate = newCallGate("CreateExpiringSnapshot")
				s.a.snapshotFault = func(verb, suffix string) error {
					if verb == "CreateExpiringSnapshot" && !strings.HasPrefix(suffix, pendingFreezeTag) {
						gate.wait(verb)
					}
					return nil
				}
			}

			first := make(chan error, 1)
			go func() { first <- s.promote(t, s.handleB(t), classToA(), false) }()
			awaitGate(t, gate, "the first promote")
			require.True(t, rel.sourceRO, "the first promote froze A")

			require.NoError(t, s.promote(t, s.handleB(t), classToA(), false), "the second promote")
			require.False(t, rel.targetRO, "B promoted by the second")
			close(gate.release)
			require.NoError(t, awaitDone(t, first, "the first promote"))

			assert.False(t, rel.targetRO, "B is the writable copy")
			assert.True(t, rel.sourceRO, "A stays frozen until the reprotect")
			assert.Equal(t, 2, count(s.b.calls, "MakeTargetWritable"), "the loser's make-writable found B already writable")
			assert.Zero(t, freezeMarkers(s.a), "no freeze marker is left behind on A")
			require.Len(t, s.b.exports, 1)
			assert.False(t, s.b.exports[0].ReadOnly, "B's export is read-write")

			// And the freeze is still the promote's: an undo lifts it.
			require.NoError(t, s.demote(t, s.handleB(t), classToA()))
			_, err := s.resync(t, s.handleB(t), classToA(), true)
			require.NoError(t, err)
			assert.False(t, rel.sourceRO, "the undo lifted the promote's freeze")
			assert.True(t, rel.targetRO)
		})
	}
}

// Resync while a promote of the same pair is in flight, parked just before
// make-writable with A frozen and drained.
//
// From the old primary (the reprotect arriving early): A is still the
// established source and B a read-only target, so the resync is refused
// FailedPrecondition and changes nothing; the promote finishes, and the
// resync after it reverses as usual.
//
// From the DR side itself (set back to secondary mid-promote): the resync
// must not lift A's freeze under a promote that is about to make B writable
// -- that is two writable copies.
func TestResyncDuringAnInFlightPromote(t *testing.T) {
	t.Run("of the old primary", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB())
		gate := newCallGate("MakeTargetWritable")
		throughHooks(s, arrayB, gate)

		promoted := make(chan error, 1)
		go func() { promoted <- s.promote(t, s.handleB(t), classToA(), false) }()
		awaitGate(t, gate, "the promote")

		_, err := s.resync(t, s.handleA, classToB(), false)
		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), "a resync applies to the secondary")
		assert.True(t, rel.sourceRO, "A still frozen")
		assert.Equal(t, s.a, rel.source, "nothing reversed")

		close(gate.release)
		require.NoError(t, awaitDone(t, promoted, "the promote"))
		require.False(t, rel.targetRO)

		_, err = s.resync(t, s.handleA, classToB(), false)
		require.NoError(t, err)
		assert.Equal(t, s.b, rel.source, "the reprotect after the promote reverses")
	})

	t.Run("of the DR side being promoted", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB())
		gate := newCallGate("MakeTargetWritable")
		throughHooks(s, arrayB, gate)

		promoted := make(chan error, 1)
		go func() { promoted <- s.promote(t, s.handleB(t), classToA(), false) }()
		awaitGate(t, gate, "the promote")

		_, err := s.resync(t, s.handleB(t), classToA(), false)
		requireCode(t, err, codes.Aborted)
		assert.Contains(t, err.Error(), "still running on this controller")
		assert.True(t, rel.sourceRO, "the freeze is not lifted under the promote")

		close(gate.release)
		require.NoError(t, awaitDone(t, promoted, "the promote"))
		require.False(t, rel.targetRO, "B was promoted")
		assert.True(t, rel.sourceRO, "A must still be frozen: B is writable, and two writable copies are split brain")
	})

	// The same race from the old primary's own object toggled back to
	// primary: its promote would lift the freeze as an abandoned promote's.
	t.Run("a promote of the old primary", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB())
		gate := newCallGate("MakeTargetWritable")
		throughHooks(s, arrayB, gate)

		promoted := make(chan error, 1)
		go func() { promoted <- s.promote(t, s.handleB(t), classToA(), false) }()
		awaitGate(t, gate, "the promote")

		err := s.promote(t, s.handleA, classToB(), false)
		requireCode(t, err, codes.Aborted)
		assert.Contains(t, err.Error(), "still running on this controller")

		close(gate.release)
		require.NoError(t, awaitDone(t, promoted, "the promote"))
		assert.False(t, rel.targetRO, "B was promoted")
		assert.True(t, rel.sourceRO, "A is still frozen: one writable copy")
	})
}

// Two forced undo resyncs of a promoted DR copy: the first parks at its
// reconnect, the second reconnects and lifts the promote's freeze; the first
// then finds the relationship already reconnected and answers Ready=false
// rather than an error.
func TestTwoConcurrentUndoResyncsBothSucceed(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	gate := newCallGate("Reconnect")
	throughHooks(s, arrayB, gate)

	type answer struct {
		ready bool
		err   error
	}
	first := make(chan answer, 1)
	go func() {
		ready, err := s.resync(t, s.handleB(t), classToA(), true)
		first <- answer{ready, err}
	}()
	awaitGate(t, gate, "the first resync")

	_, err := s.resync(t, s.handleB(t), classToA(), true)
	require.NoError(t, err)
	close(gate.release)
	var got answer
	select {
	case got = <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("the first resync never finished")
	}
	require.NoError(t, got.err, "an already-reconnected relationship is not an error")
	assert.False(t, got.ready)
	assert.True(t, rel.targetRO, "B is the read-only target")
	assert.False(t, rel.sourceRO, "A is writable again")
	assert.Equal(t, 2, count(s.b.calls, "Reconnect"))
}

// Two reprotect resyncs of the old primary, with a policy to recreate. The
// first reverses and parks creating the policy on B; the second finds the
// reverse done and not reconnected, recreates the policy, links
// it and reconnects. The first's create is then refused 409 and it links the
// policy that exists (copyPolicy's conflict row), and its reconnect finds the
// relationship already reconnected.
func TestTwoConcurrentReprotectsLinkOnePolicy(t *testing.T) {
	s := newReplScenario(t)
	s.a.policies = append(s.a.policies, fakePolicy{
		PolicyInfo: qumulo.PolicyInfo{ID: "7", Name: policyNameFor(replVolumeName), SourceFileID: s.fileIDA, Enabled: true},
		schedule:   qumulo.Schedule{ExpirationTimeToLive: "7days"},
	})
	class := classToB("replicationMode", "policy+continuous", "linkSnapshotPolicy", "true")
	rel := s.protect(t, class)
	require.NoError(t, s.promote(t, s.handleB(t), classToA("replicationMode", "policy+continuous", "linkSnapshotPolicy", "true"), false))
	gate := newCallGate("CreatePolicy")
	h := throughHooks(s, arrayB, gate)
	h.policyConflicts = true

	first := make(chan error, 1)
	go func() {
		_, err := s.resync(t, s.handleA, class, false)
		first <- err
	}()
	awaitGate(t, gate, "the first reprotect")
	require.Equal(t, s.b, rel.source, "the first reversed")

	_, err := s.resync(t, s.handleA, class, false)
	require.NoError(t, err)
	close(gate.release)
	require.NoError(t, awaitDone(t, first, "the first reprotect"))

	require.Len(t, s.b.policies, 1, "one policy on the new source")
	require.Len(t, rel.policies, 1)
	assert.Equal(t, s.b.policies[0].ID, rel.policies[0].ID)
	assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, rel.mode)
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	assert.Equal(t, 2, count(s.a.calls, "Reconnect"), "the first's reconnect found it already reconnected")
}

// The reconnect after the reverse fails. The retry reconnects,
// with the class's mode and policy re-applied again first.
func TestResyncRetriedAfterAFailedReconnectFinishesTheReprotect(t *testing.T) {
	s := newReplScenario(t)
	s.a.policies = append(s.a.policies, fakePolicy{
		PolicyInfo: qumulo.PolicyInfo{ID: "7", Name: policyNameFor(replVolumeName), SourceFileID: s.fileIDA, Enabled: true},
		schedule:   qumulo.Schedule{ExpirationTimeToLive: "7days"},
	})
	class := classToB("replicationMode", "policy+continuous", "linkSnapshotPolicy", "true")
	rel := s.protect(t, class)
	require.NoError(t, s.promote(t, s.handleB(t), classToA("replicationMode", "policy+continuous", "linkSnapshotPolicy", "true"), false))
	a := throughFault(s, arrayA)
	failed := false
	a.fail = func(verb string) error {
		if verb == "Reconnect" && !failed {
			failed = true
			return errArray500()
		}
		return nil
	}

	_, err := s.resync(t, s.handleA, class, false)
	requireCode(t, err, codes.Internal)
	assert.Contains(t, err.Error(), "reconnecting relationship")
	require.Equal(t, s.b, rel.source, "reversed")
	require.Equal(t, qumulo.RelationshipDisconnected, rel.state, "not reconnected")

	ready := false
	for i := 0; i < 3 && !ready; i++ {
		ready, err = s.resync(t, s.handleA, class, false)
		require.NoError(t, err)
	}
	assert.True(t, ready)
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, rel.mode)
	require.Len(t, rel.policies, 1)
	assert.Len(t, s.b.policies, 1, "the retry linked the policy it recreated, not a second one")
}

// A promote cancelled while it waits to ask a silent source
// again returns the cancellation at once. Nothing was frozen or promoted.
func TestAPromoteCancelledWhileWaitingToAskThePeerAgainStopsThere(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.server.drainBudget = 30 * time.Second
	s.server.peerRetryWait = 10 * time.Second
	s.server.peerRetries = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	asked := 0
	s.a.wire = func(context.Context) error {
		asked++
		cancel() // the RPC is cancelled while the promote waits to ask again
		return errFakeUnreachable
	}

	start := time.Now()
	_, err := s.server.PromoteVolume(ctx, &replication.PromoteVolumeRequest{
		VolumeId: s.handleB(t), Parameters: classToA(), Secrets: replSecrets(),
	})

	requireCode(t, err, codes.Canceled)
	assert.Less(t, time.Since(start), 5*time.Second, "it did not wait out the retry")
	assert.Equal(t, 1, asked, "the source was asked once")
	s.a.wire = nil
	assert.False(t, rel.sourceRO, "nothing frozen")
	assert.True(t, rel.targetRO, "nothing promoted")
	assert.NotContains(t, s.b.calls, "MakeTargetWritable")
}

// The forced rebuild's 409: the peer's CreateSource answers that the
// relationship exists. When it does, its ID is read back from the peer and
// authorized here; when the peer lists none, Aborted.
func TestResyncRebuildAdoptsARelationshipItsCreateWasRefusedFor(t *testing.T) {
	for _, tc := range []struct {
		conflict string
		wantErr  codes.Code
	}{
		{"applied", codes.OK},
		{"refused", codes.Aborted},
	} {
		t.Run(tc.conflict, func(t *testing.T) {
			s := newReplScenario(t)
			rel := s.protect(t, classToB())
			require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
			rel.refuseReverse = true
			h := throughHooks(s, arrayB, nil)
			h.createSourceConflict = tc.conflict

			ready, err := s.resync(t, s.handleA, classToB(), true)

			if tc.wantErr != codes.OK {
				requireCode(t, err, tc.wantErr)
				assert.Contains(t, err.Error(), "as a duplicate, but lists none")
				return
			}
			require.NoError(t, err)
			assert.False(t, ready)
			fresh := s.fabric.onlyRel()
			assert.Equal(t, s.b, fresh.source, "rebuilt B->A")
			assert.Equal(t, qumulo.RelationshipEstablished, fresh.state, "and authorized on A")
		})
	}
}
