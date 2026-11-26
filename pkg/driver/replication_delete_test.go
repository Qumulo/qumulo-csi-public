// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// DeleteVolume on a protected volume.
//
// One row per row of the guard's table in replication_guard.go. Every row
// that refuses also asserts that the tree-delete was never issued and the
// relationship is still live, because the measured failure is not "an error
// was returned" -- it is that Core happily deletes the source root and ends
// the relationship on both arrays. The fabric reproduces that (endRelsUnder),
// so a guard that ran too late would show up as an ENDED relationship rather
// than as a passing test.

// controllerOn builds a ControllerServer over the same fabric the replication
// scenario uses, so a relationship enabled through the csi-addons service is
// the same relationship DeleteVolume then sees.
func (s *replScenario) controller(t *testing.T) *ControllerServer {
	t.Helper()
	return NewControllerServer(s.driver)
}

// deleteSecrets is the Secret a DeleteVolume carries: the local array's token
// under the plain key clientFor reads, the peer's keyed by its address (the
// replication service's convention), plus any replication parameters.
func deleteSecrets(extra ...string) map[string]string {
	secrets := map[string]string{
		secretKeyToken: "token-a",
		arrayA:         "token-a",
		arrayB:         "token-b",
	}
	for i := 0; i+1 < len(extra); i += 2 {
		secrets[extra[i]] = extra[i+1]
	}
	return secrets
}

func TestDeleteVolumeRefusesAProtectedVolume(t *testing.T) {
	tests := []struct {
		name string
		// secrets carries the replication keys under test.
		secrets map[string]string
		// seed puts the fabric into the row's state and returns the handle
		// DeleteVolume is called with.
		seed func(t *testing.T, s *replScenario) string

		wantCode codes.Code
		wantMsg  []string
		// wantRelState is the relationship's state afterwards; "" means the
		// relationship must be gone entirely.
		wantRelState string
		// wantDirGone says the volume directory was tree-deleted.
		wantDirGone bool
	}{
		{
			name:    "an unprotected volume is deleted as before",
			secrets: deleteSecrets(),
			seed: func(t *testing.T, s *replScenario) string {
				return s.handleA
			},
			wantDirGone: true,
		},
		{
			name:    "a replication source is refused by default, naming the relationship",
			secrets: deleteSecrets(),
			seed: func(t *testing.T, s *replScenario) string {
				require.NoError(t, s.enable(t, classToB()))
				return s.handleA
			},
			wantCode:     codes.FailedPrecondition,
			wantMsg:      []string{"rel-1", "Disable replication first", "replicationdeletepolicy"},
			wantRelState: qumulo.RelationshipEstablished,
		},
		{
			name:    "refuse is the same as the default, said out loud",
			secrets: deleteSecrets(paramReplicationDeletePolicy, replicationDeletePolicyRefuse),
			seed: func(t *testing.T, s *replScenario) string {
				require.NoError(t, s.enable(t, classToB()))
				return s.handleA
			},
			wantCode:     codes.FailedPrecondition,
			wantMsg:      []string{"rel-1"},
			wantRelState: qumulo.RelationshipEstablished,
		},
		{
			name:    "disable removes both ends and then deletes",
			secrets: deleteSecrets(paramReplicationDeletePolicy, replicationDeletePolicyDisable),
			seed: func(t *testing.T, s *replScenario) string {
				require.NoError(t, s.enable(t, classToB()))
				return s.handleA
			},
			wantDirGone: true,
		},
		{
			// The first cut removed the local end, shrugged at the
			// unreachable peer and tree-deleted, leaving the peer an orphaned
			// ENDED end and a replica its inventory listed as a volume. Now
			// the peer end goes first, so an unreachable peer is found before
			// anything changes, and the delete is refused with the remedy.
			name: "disable with the peer unreachable is refused before anything is dismantled",
			secrets: deleteSecrets(paramReplicationDeletePolicy, replicationDeletePolicyDisable,
				paramTargetServer, arrayB, paramTargetAPIURL, "https://"+arrayB+":8000"),
			seed: func(t *testing.T, s *replScenario) string {
				require.NoError(t, s.enable(t, classToB()))
				s.b.down = true
				return s.handleA
			},
			wantCode:     codes.FailedPrecondition,
			wantMsg:      []string{"could not be removed", "Nothing was changed", "disable-orphan"},
			wantRelState: qumulo.RelationshipEstablished,
		},
		{
			name: "disable with no peer credential is refused the same way",
			secrets: map[string]string{
				secretKeyToken:               "token-a",
				arrayA:                       "token-a",
				paramReplicationDeletePolicy: replicationDeletePolicyDisable,
			},
			seed: func(t *testing.T, s *replScenario) string {
				require.NoError(t, s.enable(t, classToB()))
				return s.handleA
			},
			wantCode:     codes.FailedPrecondition,
			wantMsg:      []string{"no token for the peer array", "disable-orphan"},
			wantRelState: qumulo.RelationshipEstablished,
		},
		{
			name: "disable-orphan is the operator's consent: deletes with the peer down, leaving its end ENDED",
			secrets: deleteSecrets(paramReplicationDeletePolicy, replicationDeletePolicyDisableOrphan,
				paramTargetServer, arrayB, paramTargetAPIURL, "https://"+arrayB+":8000"),
			seed: func(t *testing.T, s *replScenario) string {
				require.NoError(t, s.enable(t, classToB()))
				s.b.down = true
				return s.handleA
			},
			wantRelState: qumulo.RelationshipEnded,
			wantDirGone:  true,
		},
		{
			name:    "disable-orphan with the peer up removes both ends like disable",
			secrets: deleteSecrets(paramReplicationDeletePolicy, replicationDeletePolicyDisableOrphan),
			seed: func(t *testing.T, s *replScenario) string {
				require.NoError(t, s.enable(t, classToB()))
				return s.handleA
			},
			wantDirGone: true,
		},
		{
			name:    "a replica is refused whatever the policy says",
			secrets: deleteSecrets(paramReplicationDeletePolicy, replicationDeletePolicyDisable),
			seed: func(t *testing.T, s *replScenario) string {
				require.NoError(t, s.enable(t, classToB()))
				return s.handleB(t)
			},
			wantCode:     codes.FailedPrecondition,
			wantMsg:      []string{"replica", "rel-1", "Disable replication from the source side first"},
			wantRelState: qumulo.RelationshipEstablished,
		},
		{
			name:    "an ENDED relationship protects nothing and does not block the delete",
			secrets: deleteSecrets(),
			seed: func(t *testing.T, s *replScenario) string {
				s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath,
					qumulo.RelationshipEnded, func(r *fakeRel) {
						r.endReason = "REPLICATION_END_REASON_SOURCE_DIRECTORY_DELETED"
					})
				return s.handleA
			},
			wantRelState: qumulo.RelationshipEnded,
			wantDirGone:  true,
		},
		{
			// An unprotected directory beside the protected one. The guard
			// keys off the relationship rooted at the path it is given, so
			// this deletes without ever consulting the protected volume's
			// relationship, which must survive untouched.
			name:    "a handle naming another directory does not disturb this volume's relationship",
			secrets: deleteSecrets(),
			seed: func(t *testing.T, s *replScenario) string {
				require.NoError(t, s.enable(t, classToB()))
				s.a.quotas[s.a.mkdir("/csi/volumes/pvc-someone-else")] = replQuota
				return newVolumeID(arrayA, "", "/csi/volumes/pvc-someone-else").String()
			},
			wantRelState: qumulo.RelationshipEstablished,
		},
		{
			name:    "an unusable delete policy is refused before anything is read",
			secrets: deleteSecrets(paramReplicationDeletePolicy, "delete-the-replica-too"),
			seed: func(t *testing.T, s *replScenario) string {
				require.NoError(t, s.enable(t, classToB()))
				return s.handleA
			},
			wantCode:     codes.InvalidArgument,
			wantMsg:      []string{"replicationdeletepolicy", "refuse", "disable", "disable-orphan"},
			wantRelState: qumulo.RelationshipEstablished,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newReplScenario(t)
			handle := tt.seed(t, s)
			controller := s.controller(t)

			_, err := controller.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
				VolumeId: handle,
				Secrets:  tt.secrets,
			})

			if tt.wantCode == codes.OK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err), "message was %v", err)
				for _, want := range tt.wantMsg {
					assert.Contains(t, err.Error(), want)
				}
			}

			switch tt.wantRelState {
			case "":
				assert.Zero(t, s.fabric.relCount(), "the relationship should be gone from both arrays")
			default:
				require.Equal(t, 1, s.fabric.relCount())
				assert.Equal(t, tt.wantRelState, s.fabric.onlyRel().state)
			}

			_, stillThere := s.a.dirs[replVolumePath]
			assert.Equal(t, tt.wantDirGone, !stillThere, "volume directory on %s", arrayA)
		})
	}
}

// A refusal must land before the tree-delete is issued, not merely produce an
// error afterwards. This is the assertion that matters here.
func TestDeleteVolumeGuardRunsBeforeTheTreeDelete(t *testing.T) {
	s := newReplScenario(t)
	require.NoError(t, s.enable(t, classToB()))
	before := s.fabric.onlyRel().state

	_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: s.handleA, Secrets: deleteSecrets(),
	})
	require.Error(t, err)

	for _, call := range s.a.calls {
		assert.False(t, strings.HasPrefix(call, "tree-delete"),
			"the guard let a tree-delete through: %v", s.a.calls)
	}
	assert.Equal(t, before, s.fabric.onlyRel().state, "the relationship changed state")
	assert.Equal(t, replQuota, s.a.quotas[s.fileIDA], "the quota was removed before the refusal")
	assert.Len(t, s.a.exports, 1, "the export was removed before the refusal")
}

// The relationship table is not optional. A token that cannot read it leaves
// the driver unable to say whether the volume is protected, and the answer to
// that is a named privilege, not a delete.
func TestDeleteVolumeRefusesWhenTheRelationshipTableCannotBeRead(t *testing.T) {
	tests := []struct {
		name     string
		listErr  error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "a token without the replication read privilege",
			listErr:  &qumulo.APIError{StatusCode: 403, Path: "/v2/replication", Class: "authentication_error"},
			wantCode: codes.FailedPrecondition,
			wantMsg:  "PRIVILEGE_REPLICATION_SOURCE_READ",
		},
		{
			name:     "an array that did not answer",
			listErr:  errFakeUnreachable,
			wantCode: codes.Unavailable,
			wantMsg:  "unreachable",
		},
		{
			name:     "an array that answered with an error",
			listErr:  &qumulo.APIError{StatusCode: 500, Path: "/v2/replication"},
			wantCode: codes.Internal,
			wantMsg:  "listing source relationships",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &MockClient{
				ResolveFileIDsFunc:   resolvesVolume("12884901888", "pvc-abc"),
				ListSourceStatusFunc: func(context.Context) ([]qumulo.RelationshipStatus, error) { return nil, tt.listErr },
				StartTreeDeleteFunc: func(context.Context, string) error {
					t.Fatal("the tree-delete ran despite an unreadable relationship table")
					return nil
				},
			}
			srv := newTestControllerServer(t, client)

			_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
				VolumeId: newVolumeID(testServer, "", "/csi/volumes/pvc-abc").String(),
				Secrets:  testSecrets(),
			})
			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err))
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}

// deleteVolume is the CSI DeleteVolume, for tests that care about what the
// guard does rather than about the request shape.
func (s *replScenario) deleteVolume(t *testing.T, handle string) error {
	t.Helper()
	_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: handle, Secrets: deleteSecrets(),
	})
	return err
}
