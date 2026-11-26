// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"qumulo-csi-driver/pkg/qumulo"
)

// An end left ENDED because the other array deleted its end used to be
// refused as a class that disagrees with the relationship -- "check
// targetbasepath and targetserver" -- which sends the operator to the wrong
// place. Promote and Resync now say what happened and what fixes it, and
// still never answer FailedPrecondition (a forced promote).
func TestPromoteAndResyncOnAnEndedEndSayTheOtherEndWasDeleted(t *testing.T) {
	type side struct {
		name   string
		handle func(s *replScenario) string
		class  map[string]string
		// end deletes the OTHER array's end, leaving this one ENDED.
		end func(s *replScenario, rel *fakeRel) error
	}
	sides := []side{
		{
			name:   "ENDED target (the source end was deleted)",
			handle: func(s *replScenario) string { return s.handleB(t) },
			class:  classToA(),
			end: func(s *replScenario, rel *fakeRel) error {
				return s.a.DeleteSource(context.Background(), rel.id)
			},
		},
		{
			name:   "ENDED source (the target end was deleted)",
			handle: func(s *replScenario) string { return s.handleA },
			class:  classToB(),
			end: func(s *replScenario, rel *fakeRel) error {
				return s.b.DeleteTarget(context.Background(), rel.id)
			},
		},
	}
	rpcs := []struct {
		name string
		call func(t *testing.T, s *replScenario, handle string, class map[string]string) error
	}{
		{"Promote", func(t *testing.T, s *replScenario, h string, c map[string]string) error {
			return s.promote(t, h, c, false)
		}},
		{"forced Promote", func(t *testing.T, s *replScenario, h string, c map[string]string) error {
			return s.promote(t, h, c, true)
		}},
		{"Resync", func(t *testing.T, s *replScenario, h string, c map[string]string) error {
			_, err := s.resync(t, h, c, false)
			return err
		}},
		{"forced Resync", func(t *testing.T, s *replScenario, h string, c map[string]string) error {
			_, err := s.resync(t, h, c, true)
			return err
		}},
	}
	for _, sd := range sides {
		for _, rpc := range rpcs {
			t.Run(sd.name+"/"+rpc.name, func(t *testing.T) {
				s := newReplScenario(t)
				rel := s.protect(t, classToB())
				handle := sd.handle(s)
				require.NoError(t, sd.end(s, rel))
				require.Equal(t, qumulo.RelationshipEnded, rel.state)
				s.a.calls, s.b.calls = nil, nil

				err := rpc.call(t, s, handle, sd.class)

				requireCode(t, err, codes.InvalidArgument)
				msg := err.Error()
				assert.Contains(t, msg, "is ENDED")
				assert.Contains(t, msg, "the other end of the relationship was deleted")
				assert.Contains(t, msg, "enable it again")
				assert.NotContains(t, strings.ToLower(msg), paramTargetBasePath, "the class is not what is wrong")
				assert.Empty(t, s.a.calls, "nothing changed on A")
				assert.Empty(t, s.b.calls, "nothing changed on B")
			})
		}
	}
}

// The control: a peer that answers and holds no end of a LIVE relationship
// is still a class that disagrees with it, and still says to check it.
func TestAPeerWithNoEndOfALiveRelationshipStillNamesTheClass(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	err := s.promote(t, s.handleA, classToB("targetBasePath", "/elsewhere"), true)
	requireCode(t, err, codes.InvalidArgument)
	assert.Contains(t, err.Error(), "answered but lists no end")
	assert.Contains(t, err.Error(), paramTargetBasePath)
}

// The array's in-between states on a target end. Promote has no row for any
// of them and answers Aborted, retried, changing nothing; Resync answers
// Ready=false while the array works. AWAITING_AUTHORIZATION under a peer that
// sources it is Resync's row, so it is only Promote's here.
func TestInBetweenTargetStatesAreRetriedWithoutChangingAnything(t *testing.T) {
	states := []string{
		qumulo.RelationshipAwaitingAuthorization,
		qumulo.RelationshipReconnecting,
		qumulo.RelationshipReestablishing,
		qumulo.RelationshipDisconnecting,
	}
	for _, state := range states {
		t.Run(state+"/Promote", func(t *testing.T) {
			s := newReplScenario(t)
			rel := s.protect(t, classToB())
			rel.state = state
			s.a.calls, s.b.calls = nil, nil
			logged := captureWarnings(t)

			err := s.promote(t, s.handleB(t), classToA(), false)

			requireCode(t, err, codes.Aborted)
			assert.Contains(t, err.Error(), "is "+state+" as target")
			assert.Contains(t, err.Error(), "retrying")
			assert.Contains(t, logged.String(), "has no row for")
			assert.False(t, rel.sourceRO, "the source was not frozen")
			assert.True(t, rel.targetRO, "the target was not made writable")
			assert.Empty(t, s.a.calls)
			assert.Empty(t, s.b.calls)
		})
		if state == qumulo.RelationshipAwaitingAuthorization {
			continue
		}
		t.Run(state+"/Resync", func(t *testing.T) {
			s := newReplScenario(t)
			rel := s.protect(t, classToB())
			rel.state = state
			s.a.calls, s.b.calls = nil, nil

			ready, err := s.resync(t, s.handleB(t), classToA(), true)

			require.NoError(t, err)
			assert.False(t, ready, "the array is still working")
			assert.Empty(t, s.a.calls)
			assert.Empty(t, s.b.calls)
		})
	}
}
