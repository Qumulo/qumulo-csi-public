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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// Retries that land on state an earlier attempt left half-done:
// a promote whose freeze marker failed after the freeze, and a resync whose
// first call after the reverse failed. Each retry must finish the job the
// first attempt started, not read the half-done state as somebody else's.

// faultArray is a fakeArray whose fail hook is asked before the verbs these
// tests break; an error it returns is the call's. reversed is set once a
// Reverse has been served through it.
type faultArray struct {
	*fakeArray
	mu       sync.Mutex
	fail     func(verb string) error
	reversed bool
}

func (f *faultArray) hook(verb string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail == nil {
		return nil
	}
	return f.fail(verb)
}

func (f *faultArray) ListPolicies(ctx context.Context) ([]qumulo.PolicyInfo, error) {
	if err := f.hook("ListPolicies"); err != nil {
		return nil, err
	}
	return f.fakeArray.ListPolicies(ctx)
}

func (f *faultArray) PatchSource(ctx context.Context, id string, patch qumulo.SourcePatch) (qumulo.Relationship, error) {
	if err := f.hook("PatchSource"); err != nil {
		return qumulo.Relationship{}, err
	}
	return f.fakeArray.PatchSource(ctx, id, patch)
}

func (f *faultArray) Reconnect(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	if err := f.hook("Reconnect"); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	return f.fakeArray.Reconnect(ctx, id)
}

func (f *faultArray) Reverse(ctx context.Context, id, sourceAddress string, sourcePort int) (qumulo.Relationship, error) {
	if err := f.hook("Reverse"); err != nil {
		return qumulo.Relationship{}, err
	}
	out, err := f.fakeArray.Reverse(ctx, id, sourceAddress, sourcePort)
	if err == nil {
		f.mu.Lock()
		f.reversed = true
		f.mu.Unlock()
	}
	return out, err
}

// throughFault routes every client the driver dials for addr through a
// faultArray over that array.
func throughFault(s *replScenario, addr string) *faultArray {
	f := &faultArray{fakeArray: s.fabric.arrays[addr]}
	s.driver.NewClient = func(baseURL, token string) qumulo.ClientInterface {
		if hostOfURL(baseURL) == addr {
			return f
		}
		return s.fabric.newClient(baseURL, token)
	}
	return f
}

// errArray500 is an answer from an array that is up and failed the call.
func errArray500() error {
	return fakeAPIError(http.StatusInternalServerError, "api_internal_error", "injected")
}

// failMarkerOnce fails the first marker snapshot whose name suffix match
// accepts, with HTTP 500.
func failMarkerOnce(a *fakeArray, match func(suffix string) bool) *bool {
	failed := false
	a.snapshotFault = func(verb, suffix string) error {
		if verb == "CreateExpiringSnapshot" && !failed && strings.Contains(suffix, freezeMarkerSuffix) && match(suffix) {
			failed = true
			return errArray500()
		}
		return nil
	}
	return &failed
}

// A planned promote froze the source and then failed to take its
// freeze marker. The retry found the source frozen with no marker and took
// the marker as held -- an operator's freeze -- so when the promote was
// undone (Demote B, forced Resync B), the reconnect held the freeze: both
// copies read-only while Resync reported ready. The retry must know the
// freeze is the promote's own.
func TestPromoteWhoseMarkerSnapshotFailedAfterTheFreezeStillLetsAnUndoLiftTheFreeze(t *testing.T) {
	afterTheFreeze := func(suffix string) bool { return !strings.HasPrefix(suffix, pendingFreezeTag) }
	anyMarker := func(string) bool { return true }

	for _, tc := range []struct {
		name  string
		match func(string) bool
	}{
		{"the marker after the freeze fails", afterTheFreeze},
		{"the first marker of any kind fails", anyMarker},
	} {
		t.Run(tc.name+"/completed then undone", func(t *testing.T) {
			s := newReplScenario(t)
			rel := s.protect(t, classToB())
			failed := failMarkerOnce(s.a, tc.match)

			require.Error(t, s.promote(t, s.handleB(t), classToA(), false), "the marker snapshot failed")
			require.True(t, *failed)
			require.True(t, rel.targetRO, "nothing promoted yet")

			require.NoError(t, s.promote(t, s.handleB(t), classToA(), false), "the retry")
			require.False(t, rel.targetRO, "B promoted")
			require.True(t, rel.sourceRO, "a planned promote leaves A frozen")

			// Undo: the DR side back to secondary.
			require.NoError(t, s.demote(t, s.handleB(t), classToA()))
			_, err := s.resync(t, s.handleB(t), classToA(), true)
			require.NoError(t, err)
			ready, err := s.resync(t, s.handleB(t), classToA(), false)
			require.NoError(t, err)
			require.True(t, ready)

			assert.True(t, rel.targetRO, "B is a read-only target again")
			assert.False(t, rel.sourceRO, "A must be writable again: the freeze was the undone promote's, not an operator's")
			assert.Zero(t, freezeMarkers(s.a), "no marker outlives the undone promote")
		})

		t.Run(tc.name+"/abandoned before the drain finished", func(t *testing.T) {
			s := newReplScenario(t)
			rel := s.protect(t, classToB())
			failMarkerOnce(s.a, tc.match)
			require.Error(t, s.promote(t, s.handleB(t), classToA(), false))
			// The retry stalls in the drain: the target never catches up.
			s.fabric.frozenPoint = true
			rel.write()
			require.Error(t, s.promote(t, s.handleB(t), classToA(), false))
			require.True(t, rel.sourceRO, "the drain holds the freeze for its retry")

			// The operator abandons: the DR side back to secondary.
			require.NoError(t, s.demote(t, s.handleB(t), classToA()))
			_, err := s.resync(t, s.handleB(t), classToA(), false)
			require.NoError(t, err)

			assert.False(t, rel.sourceRO, "A must be writable again after the abandoned promote")
			assert.True(t, rel.targetRO, "B is still a read-only target: one writable copy")
		})
	}
}

// Resync of the old primary reverses the relationship on the promoted
// peer, then re-applies the class mode and policy link there. When the first
// call to the peer after the reverse failed, the retry found this side a
// DISCONNECTED read-only target, took the plain-reconnect row and never
// re-applied anything: the relationship replicated continuously with no
// policy linked while Resync reported ready.
func TestResyncRetriedAfterTheReverseRestoresTheClassModeAndPolicyLink(t *testing.T) {
	for _, verb := range []string{"ListPolicies", "PatchSource"} {
		t.Run(verb+" fails once after the reverse", func(t *testing.T) {
			s := newReplScenario(t)
			s.a.policies = append(s.a.policies, fakePolicy{
				PolicyInfo: qumulo.PolicyInfo{ID: "7", Name: policyNameFor(replVolumeName), SourceFileID: s.fileIDA, Enabled: true},
				schedule:   qumulo.Schedule{ExpirationTimeToLive: "7days"},
			})
			class := classToB("replicationMode", "policy+continuous", "linkSnapshotPolicy", "true")
			rel := s.protect(t, class)
			require.NoError(t, s.promote(t, s.handleB(t), classToA("replicationMode", "policy+continuous", "linkSnapshotPolicy", "true"), false))

			b := throughFault(s, arrayB)
			failed := false
			b.fail = func(v string) error {
				if b.reversed && !failed && v == verb {
					failed = true
					return errArray500()
				}
				return nil
			}

			_, err := s.resync(t, s.handleA, class, false)
			require.Error(t, err, "the first call after the reverse failed")
			require.True(t, failed)
			require.Equal(t, s.b, rel.source, "the reverse itself ran")

			ready := false
			for i := 0; i < 3 && !ready; i++ {
				ready, err = s.resync(t, s.handleA, class, false)
				require.NoError(t, err)
			}
			require.True(t, ready, "the retry reconnected A and it caught up")

			assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, rel.mode, "the class mode is back")
			require.Len(t, rel.policies, 1, "the volume's policy is linked on the new source")
			require.Len(t, s.b.policies, 1)
			assert.Equal(t, s.b.policies[0].ID, rel.policies[0].ID)
			assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
			assert.True(t, rel.targetRO, "A is the read-only target")
		})
	}
}

// The control: an operator's freeze stays the operator's. Demote's
// held marker failed, so A is frozen with no marker at all; a promote of B
// finds it so and marks it held, and the undo keeps it for A's own promote.
// The retried Demote is a no-op on a frozen source and changes nothing.
func TestDemoteRetryAfterAFailedMarkerStillMarksTheFreezeAsTheOperators(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	failed := failMarkerOnce(s.a, func(suffix string) bool { return strings.HasPrefix(suffix, heldFreezeTag) })
	logged := captureWarnings(t)

	require.NoError(t, s.demote(t, s.handleA, classToB()), "a marker Demote cannot take never fails it")
	require.True(t, *failed)
	require.True(t, rel.sourceRO, "Demote froze A")
	require.Zero(t, freezeMarkers(s.a), "with no marker")
	assert.Contains(t, logged.String(), "could not mark the freeze as a demote's")
	require.NoError(t, s.demote(t, s.handleA, classToB()), "the retry")

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.Equal(t, 1, freezeMarkers(s.a))
	require.True(t, isHeldFreezeMarker(s.a.snapshots[0], rel.id), "the promote found an unmarked freeze: the operator's")

	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	_, err := s.resync(t, s.handleB(t), classToA(), true)
	require.NoError(t, err)
	assert.True(t, rel.targetRO, "B reconnected as the read-only target")
	assert.True(t, rel.sourceRO, "A, demoted by the operator, stays frozen for its own promote")
}

// A promote that recorded its intent and then failed to freeze leaves a
// pending marker on an unfrozen source. A Demote that freezes it afterwards
// marks its own freeze held over that leftover, so the operator's freeze is
// still held by an undo.
func TestADemoteMarksItsFreezeOverALeftoverPendingMarker(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	a := throughFault(s, arrayA)
	failed := false
	a.fail = func(verb string) error {
		if verb == "PatchSource" && !failed {
			failed = true
			return errArray500()
		}
		return nil
	}
	require.Error(t, s.promote(t, s.handleB(t), classToA(), false), "the freeze failed")
	require.False(t, rel.sourceRO)
	require.Equal(t, 1, freezeMarkers(s.a))
	require.True(t, isPendingFreezeMarker(s.a.snapshots[0], rel.id), "the promote's intent is left behind")

	require.NoError(t, s.demote(t, s.handleA, classToB()))
	found, err := s.server.findFreezeMarker(context.Background(), s.a, rel.id, s.a.dirs[replVolumePath])
	require.NoError(t, err)
	require.NotNil(t, found)
	require.True(t, isHeldFreezeMarker(*found, rel.id), "Demote marked its own freeze over the leftover")

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	_, err = s.resync(t, s.handleB(t), classToA(), true)
	require.NoError(t, err)
	assert.True(t, rel.sourceRO, "the operator's freeze is held by the undo")
}

// A frozen source whose marker predates writes the target already has: the
// drain replaces the marker with one of the SAME kind, taken before the old
// one is deleted, so a failed replacement leaves the old one -- and its kind
// -- for the retry.
func TestAStaleMarkerOnAFrozenSourceIsReplacedWithItsOwnKind(t *testing.T) {
	for _, held := range []bool{false, true} {
		for _, failFirst := range []bool{false, true} {
			name := map[bool]string{false: "plain", true: "held"}[held]
			if failFirst {
				name += "/the replacement fails once"
			}
			t.Run(name, func(t *testing.T) {
				s := newReplScenario(t)
				rel := s.protect(t, classToB())
				stale, err := s.a.CreateExpiringSnapshot(context.Background(), s.a.dirs[replVolumePath],
					markerNameFor(rel.id, held), time.Now().Add(time.Hour))
				require.NoError(t, err)
				rel.write()             // writes after the stale marker...
				s.fabric.replicate(rel) // ...that the target already holds
				rel.sourceRO = true     // and the source frozen since
				if failFirst {
					failMarkerOnce(s.a, func(string) bool { return true })
					require.Error(t, s.promote(t, s.handleB(t), classToA(), false))
					require.Equal(t, 1, freezeMarkers(s.a), "the old marker is kept for the retry")
					require.Equal(t, stale.ID, s.a.snapshots[0].ID)
				}

				require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

				assert.False(t, rel.targetRO, "promoted")
				for _, snap := range s.a.snapshots {
					assert.NotEqual(t, stale.ID, snap.ID, "the stale marker is gone")
				}
				if held {
					require.Equal(t, 1, freezeMarkers(s.a), "a held marker outlives the promote")
					assert.True(t, isHeldFreezeMarker(s.a.snapshots[0], rel.id), "and the replacement is held too")
				} else {
					assert.Zero(t, freezeMarkers(s.a), "the promote's own marker is dropped")
				}
			})
		}
	}
}
