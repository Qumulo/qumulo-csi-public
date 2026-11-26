// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/identity"
	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// The two lab arrays, by the address the class and the Secret use.
const (
	arrayA = "a.example.com"
	arrayB = "b.example.com"

	replVolumeName = "pvc-repl"
	replVolumePath = "/csi/volumes/" + replVolumeName
	replExportPath = "/exports/" + replVolumeName
	replQuota      = uint64(1 << 30)
)

// replScenario is one test's driver, fabric and the volume provisioned on A.
type replScenario struct {
	fabric *fakeFabric
	a, b   *fakeArray
	driver *Driver
	server *ReplicationServer
	// handleA is the volume's handle on A; fileIDA its file ID there.
	handleA string
	fileIDA string
}

// newReplScenario provisions a dedicated-export volume on A the way
// CreateVolume would -- directory, quota, export -- and builds a controller
// whose NewClient hands out the fabric's arrays by URL host.
func newReplScenario(t *testing.T) *replScenario {
	t.Helper()

	fabric := newFakeFabric()
	a := fabric.addArray(arrayA)
	b := fabric.addArray(arrayB)
	fileID := a.mkdir(replVolumePath)
	a.quotas[fileID] = replQuota
	a.exports = append(a.exports, qumulo.ExportInfo{
		ID: "1", ExportPath: replExportPath, FSPath: replVolumePath, TenantID: 1,
		AuthenticationMode: qumulo.AuthenticationModeNone, AllowedClients: []string{"10.0.0.0/8"},
		Description: exportDescriptionFor(path.Base(replVolumePath)),
	})

	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "test", Mode: ModeController,
		NewClient: fabric.newClient, Replication: true,
	})
	require.NoError(t, err)
	server := d.ReplicationServer()
	require.NotNil(t, server)
	server.drainBudget = 200 * time.Millisecond
	server.drainPollWait = 5 * time.Millisecond
	// The fabric answers at once, so a peer call's timeout scales with the
	// budget: a retry is only started if its wait and its call both fit.
	server.peerCallTimeout = 20 * time.Millisecond

	return &replScenario{
		fabric: fabric, a: a, b: b, driver: d, server: server,
		handleA: newVolumeID(arrayA, "", replVolumePath).String(),
		fileIDA: fileID,
	}
}

// classToB is the VolumeReplicationClass on A's cluster, naming B as the peer.
// Both arrays are DNS-named, so both fixed REST endpoints are required;
// they resolve to the same hosts the defaults would.
func classToB(extra ...string) map[string]string {
	return classToward(arrayA, arrayB, extra...)
}

// classToA is the class on B's cluster, naming A as the peer.
func classToA(extra ...string) map[string]string {
	return classToward(arrayB, arrayA, extra...)
}

func classToward(local, peer string, extra ...string) map[string]string {
	params := map[string]string{
		"targetServer": peer, "targetApiUrl": "https://" + peer + ":8000", "apiUrl": "https://" + local + ":8000",
	}
	for i := 0; i+1 < len(extra); i += 2 {
		params[extra[i]] = extra[i+1]
	}
	return params
}

// bareClass is a class body for the parser tests: a DNS-named peer with its
// fixed endpoint, plus whatever the case is about.
func bareClass(extra ...string) map[string]string {
	params := map[string]string{"targetServer": arrayB, "targetApiUrl": "https://" + arrayB + ":8000"}
	for i := 0; i+1 < len(extra); i += 2 {
		params[extra[i]] = extra[i+1]
	}
	return params
}

// replSecrets is the one symmetric Secret: a token per array, keyed by address.
func replSecrets() map[string]string {
	return map[string]string{arrayA: "token-a", arrayB: "token-b"}
}

func (s *replScenario) enable(t *testing.T, params map[string]string) error {
	t.Helper()
	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId: s.handleA, Parameters: params, Secrets: replSecrets(),
	})
	return err
}

// handleB is the replica's handle on B, as ResolveReplicationDestination
// would report it and as a DR-side PV would carry it.
//
// It differs from handleA in the SERVER ALONE, which is the whole point of a
// path-identified handle: the replica lives at the same path on both arrays,
// so nothing array-local is left in the handle to go stale. Under the
// retired format this had to look the replica's own file ID up on B.
func (s *replScenario) handleB(t *testing.T) string {
	t.Helper()
	_, ok := s.b.dirs[replVolumePath]
	require.True(t, ok, "no replica directory on B at %s", replVolumePath)
	return newVolumeID(arrayB, "", replVolumePath).String()
}

func (s *replScenario) promote(t *testing.T, handle string, params map[string]string, force bool) error {
	t.Helper()
	_, err := s.server.PromoteVolume(context.Background(), &replication.PromoteVolumeRequest{
		VolumeId: handle, Parameters: params, Secrets: replSecrets(), Force: force,
	})
	return err
}

// awaitWritableTarget re-polls on a flat drainPollWait, and on entry it is
// holding the make-writable response -- which is DISCONNECTING in all 62
// calls captured against the 7.9.2.2 lab arrays. So the first check always
// fails and the loop always sleeps a full drainPollWait before asking again.
//
// At the production cadence that is 2 s, every promote, while the array in
// those same captures settled in 152-163 ms (n=5). Roughly 1.84 s per
// promote is spent waiting on a transition that finished long before.
//
// The budget is not the problem: reaching it needs the array to hang some
// 290x longer than anything ever measured. The cadence is. A first poll on a
// short backoff that grows to drainPollWait finds the settled array on the
// second try, around 300 ms, and still waits patiently for a slow one.
//
// Deliberately run at the real defaults rather than the scenario's 5 ms, so
// the figure under test is the one operators actually pay.
func TestPromoteDoesNotWaitAFullPollForAnArrayThatHasAlreadySettled(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.server.drainBudget = defaultDrainBudget
	s.server.drainPollWait = defaultDrainPollWait

	start := time.Now()
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 500*time.Millisecond,
		"promote spent %v waiting for a target the array settles in ~156 ms", elapsed)
}

// The backoff must not have bought speed with patience. An array that really
// does take several polls is still waited for, and the promote succeeds.
func TestPromoteWaitsForAnArrayThatTakesSeveralPollsToSettle(t *testing.T) {
	s := newReplScenario(t)
	s.fabric.makeWritableDisconnectingPolls = 4
	s.protect(t, classToB())

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	assert.False(t, s.fabric.onlyRel().targetRO, "the promoted copy is writable")
}

// And an array that never settles ends in a deliberate Aborted inside the
// budget, which the controller retries -- not a context deadline the caller
// cannot tell from a hang.
func TestPromoteGivesUpOnATargetThatNeverBecomesWritable(t *testing.T) {
	s := newReplScenario(t)
	s.fabric.makeWritableDisconnectingPolls = 1 << 30
	s.protect(t, classToB())

	err := s.promote(t, s.handleB(t), classToA(), false)
	requireCode(t, err, codes.Aborted)
	assert.Contains(t, err.Error(), "not writable yet")
}

// csi-addons v0.15.0 leaves a Completed primary VolumeReplication reading
// Healthy after the relationship is removed on the arrays, and the driver
// cannot reach those conditions. What it can do is state the fact, so the
// exporter has something keyed by VOLUME to alert on -- an absent relationship
// has no relationship ID to hang a series on, and a series that merely stops
// being reported is not alertable without knowing the ID in advance.
//
// Both verdicts have to be stated. Only reporting the bad one would leave the
// series absent for a healthy volume, which is the same unalertable hole.
func TestAssessStatesWhetherTheVolumeIsProtected(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())

	_, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())
	require.NoError(t, err)
	require.Equal(t, []bool{true}, s.a.protectionVerdicts, "a protected volume must be stated as protected")
	require.Equal(t, []string{s.handleA}, s.a.protectionSubjects,
		"the verdict must be labelled with the volume it is about, or the exporter drops it")

	// The relationship is removed on the arrays behind the driver's back.
	s.fabric.mu.Lock()
	s.fabric.rels = map[string]*fakeRel{}
	s.fabric.mu.Unlock()

	_, err = s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())
	requireCode(t, err, codes.FailedPrecondition)
	assert.Equal(t, []bool{true, false}, s.a.protectionVerdicts,
		"the volume is no longer protected and the driver must say so, not go quiet")
}

func (s *replScenario) demote(t *testing.T, handle string, params map[string]string) error {
	t.Helper()
	_, err := s.server.DemoteVolume(context.Background(), &replication.DemoteVolumeRequest{
		VolumeId: handle, Parameters: params, Secrets: replSecrets(),
	})
	return err
}

func (s *replScenario) resync(t *testing.T, handle string, params map[string]string, force bool) (bool, error) {
	t.Helper()
	resp, err := s.server.ResyncVolume(context.Background(), &replication.ResyncVolumeRequest{
		VolumeId: handle, Parameters: params, Secrets: replSecrets(), Force: force,
	})
	if err != nil {
		return false, err
	}
	return resp.GetReady(), nil
}

func (s *replScenario) disable(t *testing.T, handle string, params map[string]string) error {
	t.Helper()
	_, err := s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
		VolumeId: handle, Parameters: params, Secrets: replSecrets(),
	})
	return err
}

func (s *replScenario) assess(t *testing.T, handle string, params map[string]string) ReplicationHealth {
	t.Helper()
	h, err := s.server.AssessVolumeReplication(context.Background(), handle, "", params, replSecrets())
	require.NoError(t, err)
	return h
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	require.Error(t, err)
	assert.Equal(t, want, status.Code(err), "got %v", err)
}

// protect runs Enable once and returns the relationship, as the setup for
// every test that starts from a protected volume.
func (s *replScenario) protect(t *testing.T, params map[string]string) *fakeRel {
	t.Helper()
	require.NoError(t, s.enable(t, params))
	return s.fabric.onlyRel()
}

// --- parameters -----------------------------------------------------------

func TestParseReplicationParams(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		wantErr string
		check   func(t *testing.T, p replicationParams)
	}{
		{
			name:   "defaults",
			params: bareClass(),
			check: func(t *testing.T, p replicationParams) {
				assert.Equal(t, replicationModeContinuous, p.ReplicationMode)
				assert.Equal(t, qumulo.ReplicationModeContinuous, p.arrayMode())
				assert.Equal(t, defaultSchedulingInterval, p.SchedulingInterval)
				assert.True(t, p.drainsContinuously())
				assert.False(t, p.LinkSnapshotPolicy)
				// The safe default for the one parameter that can destroy
				// protection: refuse, never disable.
				assert.Equal(t, replicationDeletePolicyRefuse, p.DeletePolicy)
				assert.Equal(t, replicationScopeVolume, p.Scope)
			},
		},
		{
			name: "every parameter, any casing, CO keys ignored",
			params: map[string]string{
				"TargetServer": arrayB, "targetApiUrl": "https://b.example.com:9000", "apiUrl": "https://a.example.com:9000",
				"targetBasePath": "/dr", "ReplicationMode": "Policy+Continuous", "linkSnapshotPolicy": "true",
				"schedulingInterval": "15m", "allowNonEmptyTarget": "true",
				"replicationDeletePolicy": "Disable",
				"replicationScope":        "Group",
				"replication.storage.openshift.io/replication-secret-name": "s",
			},
			check: func(t *testing.T, p replicationParams) {
				assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, p.arrayMode())
				assert.Equal(t, 15*time.Minute, p.SchedulingInterval)
				assert.True(t, p.LinkSnapshotPolicy)
				assert.True(t, p.AllowNonEmpty)
				assert.Equal(t, "/dr", p.TargetBasePath)
				assert.Equal(t, replicationDeletePolicyDisable, p.DeletePolicy)
				assert.Equal(t, replicationScopeGroup, p.Scope)
			},
		},
		{
			name:   "policy only does not drain on its own",
			params: bareClass("replicationMode", "policy"),
			check: func(t *testing.T, p replicationParams) {
				assert.Equal(t, qumulo.ReplicationModeSnapshotPolicy, p.arrayMode())
				assert.False(t, p.drainsContinuously())
			},
		},
		{
			// An IP literal cannot float, so it needs no fixed endpoint.
			name:   "an IP-literal peer needs no targetApiUrl",
			params: map[string]string{"targetServer": "10.0.0.2"},
			check: func(t *testing.T, p replicationParams) {
				assert.Equal(t, "10.0.0.2", p.TargetServer)
				assert.Empty(t, p.TargetAPIURL)
			},
		},
		{
			// A DNS name may be the one that flips at failover,
			// and the controller must not dial the peer's API at it.
			name:    "a DNS-named peer without targetApiUrl",
			params:  map[string]string{"targetServer": arrayB},
			wantErr: "targetapiurl is required",
		},
		{name: "unknown key", params: bareClass("targetsrv", "x"), wantErr: "unknown VolumeReplicationClass parameter"},
		{name: "target is a URL", params: map[string]string{"targetServer": "https://b"}, wantErr: "not a URL"},
		{name: "bad mode", params: bareClass("replicationMode", "async"), wantErr: "replicationmode must be"},
		{name: "link without a policy mode", params: bareClass("linkSnapshotPolicy", "true"), wantErr: "requires replicationmode"},
		{name: "bad interval", params: bareClass("schedulingInterval", "soon"), wantErr: "positive duration"},
		{name: "relative base path", params: bareClass("targetBasePath", "dr"), wantErr: "absolute path"},
		{name: "bad api url", params: map[string]string{"targetServer": arrayB, "targetApiUrl": "b:9000"}, wantErr: "is not a URL"},
		{
			name:    "bad scope",
			params:  bareClass("replicationScope", "class"),
			wantErr: "replicationscope must be",
		},
		{
			name:    "bad delete policy",
			params:  bareClass("replicationDeletePolicy", "retain"),
			wantErr: "replicationdeletepolicy must be",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := parseReplicationParams(tt.params)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			tt.check(t, p)
		})
	}
}

// --- resolution -------------------------------------------------------------

func TestReplicationResolveRefusals(t *testing.T) {
	tests := []struct {
		name    string
		handle  func(s *replScenario) string
		params  map[string]string
		secrets map[string]string
		prepare func(s *replScenario)
		want    codes.Code
		wantMsg string
	}{
		{
			name:   "malformed handle",
			handle: func(*replScenario) string { return "not-a-handle" },
			params: classToB(), secrets: replSecrets(), want: codes.InvalidArgument,
		},
		{
			name:   "no token for the peer",
			handle: func(s *replScenario) string { return s.handleA },
			params: classToB(), secrets: map[string]string{arrayA: "token-a"},
			want: codes.InvalidArgument, wantMsg: "no token for peer array " + arrayB,
		},
		{
			name:   "no token for the local array",
			handle: func(s *replScenario) string { return s.handleA },
			params: classToB(), secrets: map[string]string{arrayB: "token-b"},
			want: codes.InvalidArgument, wantMsg: "no token for array " + arrayA,
		},
		{
			// The handle's server is a DNS name that may float,
			// so the class must say where this array's API really is.
			name:    "DNS-named volume server without apiUrl on the class",
			handle:  func(s *replScenario) string { return s.handleA },
			params:  map[string]string{"targetServer": arrayB, "targetApiUrl": "https://" + arrayB + ":8000"},
			secrets: replSecrets(),
			want:    codes.InvalidArgument, wantMsg: "must set apiurl",
		},
		{
			name:   "class names the volume's own array",
			handle: func(s *replScenario) string { return s.handleA },
			params: classToward(arrayA, arrayA), secrets: replSecrets(),
			// A misconfigured class, never FailedPrecondition: that is the
			// one code the controller answers by re-calling Promote with
			// force, and this refusal is shared by every RPC.
			want: codes.InvalidArgument, wantMsg: "must name the peer array",
		},
		{
			// The handle names a path that is not there. Under the retired
			// format this case was "stale file ID"; it is the same refusal
			// for a reason that is now legible in the handle itself.
			name: "path does not exist on this array",
			handle: func(s *replScenario) string {
				return newVolumeID(arrayA, "", "/csi/volumes/pvc-never-made").String()
			},
			params: classToB(), secrets: replSecrets(), want: codes.NotFound,
		},
		{
			// A handle in the retired <server>/<fileID>/<name> form. Refused
			// at parse, so no array is dialled at all.
			name: "handle in the retired format",
			handle: func(s *replScenario) string {
				return arrayA + "/999999/" + replVolumeName
			},
			params: classToB(), secrets: replSecrets(), want: codes.InvalidArgument,
		},
		{
			name:    "local array unreachable",
			handle:  func(s *replScenario) string { return s.handleA },
			params:  classToB(),
			secrets: replSecrets(),
			prepare: func(s *replScenario) { s.a.down = true },
			want:    codes.Unavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newReplScenario(t)
			if tt.prepare != nil {
				tt.prepare(s)
			}
			_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
				VolumeId: tt.handle(s), Parameters: tt.params, Secrets: tt.secrets,
			})
			requireCode(t, err, tt.want)
			if tt.wantMsg != "" {
				assert.Contains(t, err.Error(), tt.wantMsg)
			}
		})
	}
}

// Rule 0: each array is dialed with its own token from the request, and only
// that one.
func TestReplicationDialsEachArrayWithItsOwnToken(t *testing.T) {
	s := newReplScenario(t)
	require.NoError(t, s.enable(t, classToB()))

	for _, tok := range s.fabric.tokens[arrayA] {
		assert.Equal(t, "token-a", tok)
	}
	for _, tok := range s.fabric.tokens[arrayB] {
		assert.Equal(t, "token-b", tok)
	}
	assert.NotEmpty(t, s.fabric.tokens[arrayB], "the peer was never dialed")
}

// The "token" key stays valid for the local array, so a class carrying the
// StorageClass's own Secret plus the peer's token works.
func TestReplicationAcceptsPlainTokenKeyForLocal(t *testing.T) {
	s := newReplScenario(t)
	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId: s.handleA, Parameters: classToB(),
		Secrets: map[string]string{"token": "token-a", arrayB: "token-b"},
	})
	require.NoError(t, err)
}

// A token file ends in a newline, and a Secret made from it carries one.
// The plain "token" key was trimmed, the peer's address-keyed one was not: the
// newline reached the Authorization header, net/http refused it as an invalid
// header -- a transport error -- and the peer was reported unreachable, which
// also opened its breaker. Every token key is trimmed now.
func TestReplicationTrimsEveryTokenKey(t *testing.T) {
	s := newReplScenario(t)
	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId: s.handleA, Parameters: classToB(),
		Secrets: map[string]string{arrayA: "token-a\n", arrayB: "token-b\r\n"},
	})
	require.NoError(t, err)
	for _, tok := range s.fabric.tokens[arrayA] {
		assert.Equal(t, "token-a", tok)
	}
	assert.NotEmpty(t, s.fabric.tokens[arrayB], "the peer was never dialed")
	for _, tok := range s.fabric.tokens[arrayB] {
		assert.Equal(t, "token-b", tok)
	}

	// The guard DeleteVolume and ControllerExpandVolume use reads the same key.
	var dialled []string
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeController,
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			dialled = append(dialled, token)
			return s.fabric.newClient(baseURL, token)
		}})
	require.NoError(t, err)
	_, err = NewControllerServer(d).peerReplicationClient(arrayB,
		replicationParams{TargetServer: arrayB, TargetAPIURL: "https://" + arrayB + ":8000"},
		map[string]string{arrayB: "token-b\n"})
	require.NoError(t, err)
	assert.Equal(t, []string{"token-b"}, dialled)
}

// --- EnableVolumeReplication --------------------------------------------------

func TestEnableCreatesAuthorizesAndPrestages(t *testing.T) {
	s := newReplScenario(t)

	rel := s.protect(t, classToB())

	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	assert.Equal(t, s.a, rel.source)
	assert.Equal(t, s.b, rel.target)
	assert.Equal(t, replVolumePath, rel.sourcePath)
	assert.Equal(t, replVolumePath, rel.targetPath, "targetBasePath defaults to the source's parent")
	assert.True(t, rel.enabled)
	assert.True(t, rel.targetRO)

	// The replica directory exists only after authorize -- and it does
	// now, with its own file ID.
	replicaID, ok := s.b.dirs[replVolumePath]
	require.True(t, ok)
	assert.NotEqual(t, s.fileIDA, replicaID)

	// Quota equal, export identical bar the array-local ID, and
	// READ-ONLY.
	assert.Equal(t, replQuota, s.b.quotas[replicaID])
	require.Len(t, s.b.exports, 1)
	staged := s.b.exports[0]
	assert.Equal(t, replExportPath, staged.ExportPath)
	assert.Equal(t, replVolumePath, staged.FSPath)
	assert.True(t, staged.ReadOnly, "the pre-staged export must be read-only until promote")
	assert.Equal(t, []string{"10.0.0.0/8"}, staged.AllowedClients, "host restrictions travel with the definition")
	assert.Equal(t, 1, staged.TenantID)

	assert.Equal(t, []string{"CreateSource"}, s.a.calls)
	assert.Equal(t, []string{"Authorize", "CreateQuota", "CreateExport"}, s.b.calls)
}

// The csi-addons v0.15.0 controller sends the handle only in
// replication_source.volume.volume_id and leaves the legacy volume_id empty
// (measured on the lab cluster, 2026-09-14: "volume ID is empty" on every
// RPC). Both spellings must reach the same volume.
func TestRequestsNameTheVolumeThroughReplicationSource(t *testing.T) {
	s := newReplScenario(t)
	src := &replication.ReplicationSource{Type: &replication.ReplicationSource_Volume{
		Volume: &replication.ReplicationSource_VolumeSource{VolumeId: s.handleA},
	}}

	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		ReplicationSource: src, Parameters: classToB(), Secrets: replSecrets(),
	})
	require.NoError(t, err, "an Enable that names the volume only through replication_source must work")
	assert.Equal(t, 1, s.fabric.relCount())

	_, err = s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		ReplicationSource: src, Secrets: replSecrets(),
	})
	require.NoError(t, err)

	_, err = s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		Parameters: classToB(), Secrets: replSecrets(),
	})
	requireCode(t, err, codes.InvalidArgument)
}

func TestEnableIsIdempotent(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.a.calls, s.b.calls = nil, nil

	require.NoError(t, s.enable(t, classToB()))

	assert.Equal(t, 1, s.fabric.relCount(), "a second Enable must adopt, not create")
	assert.Empty(t, s.a.calls, "nothing to do on the source")
	assert.Empty(t, s.b.calls, "export and quota already staged; nothing recreated")
	require.Len(t, s.b.exports, 1)
}

func TestEnableReassertsADriftedQuota(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	// The source grew (ControllerExpandVolume) and the replica did not.
	s.a.quotas[s.fileIDA] = 2 * replQuota

	require.NoError(t, s.enable(t, classToB()))

	assert.Equal(t, 2*replQuota, s.b.quotas[s.b.dirs[replVolumePath]])
}

func TestEnableRefusesSharedSubdirectoryVolumes(t *testing.T) {
	s := newReplScenario(t)
	s.a.exports = nil // no export of its own: provisioned under shared-subdirectory

	err := s.enable(t, classToB())

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "set replicationscope")
	assert.Contains(t, err.Error(), "shared-subdirectory")
	assert.Zero(t, s.fabric.relCount(), "nothing may be created for a refused class")
}

func TestEnableAuthorizesAnAwaitingRelationship(t *testing.T) {
	s := newReplScenario(t)
	// A previous attempt created the relationship and died before authorize.
	rel := s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath, qumulo.RelationshipAwaitingAuthorization, func(r *fakeRel) {
		r.targetRO = false
	})

	require.NoError(t, s.enable(t, classToB()))

	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	assert.Equal(t, 1, s.fabric.relCount())
	assert.Empty(t, s.a.calls, "the existing relationship is adopted, not recreated")
	assert.Equal(t, []string{"Authorize", "CreateQuota", "CreateExport"}, s.b.calls)
}

// The first live run against two Core 7.9.2.2 arrays (2026-09-14) failed
// here: a repeat Enable a few seconds after the first found the source end
// still listing AWAITING_AUTHORIZATION, re-authorized, and the target answered
// HTTP 400 relationship_manager_already_authorized_error, which the RPC
// surfaced as FailedPrecondition. The two ends of a relationship do not
// update atomically, so "already authorized" is a success on this step.
func TestEnableTreatsAlreadyAuthorizedAsDone(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	rel := s.fabric.relByPath(replVolumePath)
	rel.sourceReportsState = qumulo.RelationshipAwaitingAuthorization
	s.a.calls, s.b.calls = nil, nil

	require.NoError(t, s.enable(t, classToB()))

	assert.Equal(t, 1, s.fabric.relCount(), "the lagging source end must not cause a second relationship")
	assert.Empty(t, s.a.calls, "nothing to do on the source")
	assert.Equal(t, []string{"Authorize"}, s.b.calls,
		"the peer is asked once, answers already-authorized, and nothing is re-staged")
	require.Len(t, s.b.exports, 1)
}

func TestEnableOnAnEstablishedTargetIsANoOp(t *testing.T) {
	s := newReplScenario(t)
	// B is the source; the volume on A is the replica. Enable from A's side
	// (class pointing at B) finds itself already protected.
	s.b.mkdir(replVolumePath)
	s.fabric.seedRel(s.b, s.a, replVolumePath, replVolumePath, qumulo.RelationshipEstablished, nil)

	require.NoError(t, s.enable(t, classToB()))

	assert.Empty(t, s.a.calls)
	assert.Empty(t, s.b.calls)
	assert.Equal(t, 1, s.fabric.relCount())
}

// Enable on a promoted ex-target deletes both ends and rebuilds toward the
// peer, adopting and overwriting the peer's directory. That is destructive,
// so it runs only with consent: the peer's source end read as frozen (the
// old primary was demoted), or the class saying allowNonEmptyTarget.
// Without either, the old primary may still be taking writes and
// the request is refused with nothing changed.
func TestEnableOnAPromotedExTargetNeedsConsentToOverwriteThePeer(t *testing.T) {
	seed := func(t *testing.T) (*replScenario, *fakeRel) {
		s := newReplScenario(t)
		// A was promoted from B and B never came back into the relationship:
		// A is a writable disconnected target, replicating to nobody.
		s.b.mkdir(replVolumePath)
		s.b.mkdir(replVolumePath + "/old-data")
		old := s.fabric.seedRel(s.b, s.a, replVolumePath, replVolumePath, qumulo.RelationshipDisconnected, func(r *fakeRel) {
			r.targetRO = false
		})
		s.a.calls, s.b.calls = nil, nil
		return s, old
	}
	assertReprotected := func(t *testing.T, s *replScenario, old *fakeRel) {
		assert.Equal(t, 1, s.fabric.relCount())
		fresh := s.fabric.onlyRel()
		assert.NotEqual(t, old.id, fresh.id, "the dead relationship is replaced")
		assert.Equal(t, s.a, fresh.source, "A is now the source")
		assert.Equal(t, qumulo.RelationshipEstablished, fresh.state)
		assert.Contains(t, s.a.calls, "DeleteTarget")
		assert.Contains(t, s.b.calls, "DeleteSource")
		assert.Contains(t, s.b.calls, "Authorize")
	}

	t.Run("refused while the peer's source end is writable and the class allows nothing", func(t *testing.T) {
		s, old := seed(t)

		err := s.enable(t, classToB())

		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), "overwrite")
		assert.Contains(t, err.Error(), paramAllowNonEmptyTarget)
		assert.Contains(t, err.Error(), "secondary first")
		assert.Equal(t, 1, s.fabric.relCount())
		assert.Equal(t, old.id, s.fabric.onlyRel().id, "nothing was torn down")
		assert.NotContains(t, s.a.calls, "DeleteTarget")
		assert.NotContains(t, s.b.calls, "DeleteSource")
	})

	t.Run("proceeds once the peer's source end is frozen", func(t *testing.T) {
		s, old := seed(t)
		old.sourceRO = true // the old primary was demoted

		require.NoError(t, s.enable(t, classToB()))
		assertReprotected(t, s, old)
	})

	t.Run("proceeds when the class consents to a non-empty target", func(t *testing.T) {
		s, old := seed(t)

		require.NoError(t, s.enable(t, classToB("allowNonEmptyTarget", "true")))
		assertReprotected(t, s, old)
	})
}

func TestEnableRemovesAnEndedOrphanAndRecreates(t *testing.T) {
	s := newReplScenario(t)
	// The peer's end was deleted out of band; ours is ENDED.
	s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath, qumulo.RelationshipEnded, func(r *fakeRel) {
		r.hasTarget = false
		r.endReason = "target relationship deleted"
	})

	require.NoError(t, s.enable(t, classToB()))

	fresh := s.fabric.onlyRel()
	assert.Equal(t, qumulo.RelationshipEstablished, fresh.state)
	assert.Equal(t, []string{"DeleteSource", "CreateSource"}, s.a.calls)
}

// Measured with C1 powered off (2026-09-14): the csi-addons controller calls
// Enable before every Promote, so the DR side's Enable must succeed without
// the dead peer when this side is already the replica.
func TestEnableOnAReplicaSucceedsWhenThePeerIsDown(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.a.down = true
	s.a.calls, s.b.calls = nil, nil

	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId: s.handleB(t), Parameters: classToA(), Secrets: replSecrets(),
	})
	require.NoError(t, err, "a replica with a dead peer has nothing to enable and must not block the promote that follows")
	assert.Empty(t, s.b.calls, "nothing is created or staged on the replica")
}

func TestEnableWithPeerUnreachableIsUnavailable(t *testing.T) {
	s := newReplScenario(t)
	s.b.down = true

	requireCode(t, s.enable(t, classToB()), codes.Unavailable)
	assert.Zero(t, s.fabric.relCount(), "the relationship must not be created when it cannot be authorized")
}

func TestEnableRefusesANonEmptyPeerDirectoryUnlessAllowed(t *testing.T) {
	s := newReplScenario(t)
	s.b.mkdir(replVolumePath + "/somebody-elses-file")

	err := s.enable(t, classToB())
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "allownonemptytarget")

	// The same request with the class opting in adopts the directory.
	require.NoError(t, s.enable(t, classToB("allowNonEmptyTarget", "true")))
	assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
}

func TestEnableLinksTheVolumesSnapshotPolicy(t *testing.T) {
	s := newReplScenario(t)
	s.a.policies = append(s.a.policies, fakePolicy{PolicyInfo: qumulo.PolicyInfo{ID: "7", Name: policyNameFor(replVolumeName), SourceFileID: s.fileIDA, Enabled: true}})

	rel := s.protect(t, classToB("replicationMode", "policy+continuous", "linkSnapshotPolicy", "true"))

	assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, rel.mode)
	require.Len(t, rel.policies, 1)
	assert.Equal(t, qumulo.SnapshotPolicyLink{ID: "7", TargetExpiration: qumulo.TargetExpirationSameAsPolicy}, rel.policies[0])
}

func TestEnableHonoursTargetBasePath(t *testing.T) {
	s := newReplScenario(t)

	rel := s.protect(t, classToB("targetBasePath", "/csi-dr"))

	assert.Equal(t, "/csi-dr/"+replVolumeName, rel.targetPath)
	_, ok := s.b.dirs["/csi-dr/"+replVolumeName]
	assert.True(t, ok)
}

// --- DisableVolumeReplication -----------------------------------------------

func TestDisableRemovesBothEnds(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())

	require.NoError(t, s.disable(t, s.handleA, classToB()))

	assert.Zero(t, s.fabric.relCount())
	// Disable deletes no data: replica directory, export and quota stay.
	_, ok := s.b.dirs[replVolumePath]
	assert.True(t, ok)
	assert.Len(t, s.b.exports, 1)
	// A second call has nothing to do.
	require.NoError(t, s.disable(t, s.handleA, classToB()))
}

// Whether deleting a source relationship lifts its freeze is not
// measured, and a Disable after an abandoned planned promote could leave the
// directory read-only with nothing left to unfreeze through. So Disable lifts
// the freeze first -- only when the peer's end has been read as a read-only
// target, so no second writable copy is made.
func TestDisableOfAFrozenSourceUnfreezesItWhenThePeerIsReadOnly(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.sourceRO = true // abandoned planned promote
	s.a.calls = nil

	require.NoError(t, s.disable(t, s.handleA, classToB()))

	assert.Zero(t, s.fabric.relCount())
	assert.False(t, rel.sourceRO, "the freeze is lifted before the end is deleted")
	assert.Equal(t, []string{"PatchSource", "DeleteSource"}, s.a.calls)
}

func TestDisableOfAFrozenSourceKeepsTheFreezeWhenThePeerIsWritable(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.sourceRO = true
	rel.targetRO = false // B promoted
	s.a.calls = nil
	s.b.calls = nil

	require.NoError(t, s.disable(t, s.handleA, classToB()))

	assert.True(t, rel.sourceRO, "with a writable peer the freeze is the operator's to lift")
	assert.Equal(t, []string{"DeleteSource"}, s.a.calls)
	// And the promoted peer's end is left alone; see the next test.
	assert.NotContains(t, s.b.calls, "DeleteTarget")
	assert.True(t, rel.hasTarget)
}

// Measured on the lab cluster: with the DR copy
// promoted (target writable) and the old primary's end back, deleting the old
// primary's VolumeReplication removed BOTH ends, and recreating it re-protected
// toward the peer with allowNonEmptyTarget, adopting the promoted copy as a
// replica and discarding its writes. Now Disable on the old primary removes
// only its own end, and Enable from it refuses to overwrite a writable peer
// copy. The way back is the documented one: set the old primary secondary and
// resync, or protect from the peer's side.
func TestDisableOnTheOldPrimaryLeavesThePromotedPeerCopyAlone(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	// B was promoted (forced, or planned while A was away): its target end is
	// writable and DISCONNECTED; A's end is back and writable too.
	rel.targetRO = false
	rel.state = qumulo.RelationshipDisconnected
	s.a.calls, s.b.calls = nil, nil

	require.NoError(t, s.disable(t, s.handleA, classToB()))

	assert.Equal(t, []string{"DeleteSource"}, s.a.calls, "only this side's end")
	assert.NotContains(t, s.b.calls, "DeleteTarget")
	assert.True(t, rel.hasTarget, "the peer's end survives")
	assert.False(t, rel.targetRO, "and its copy stays writable")
	assert.Equal(t, qumulo.RelationshipEnded, rel.state)
	_, ok := s.b.dirs[replVolumePath]
	assert.True(t, ok)
}

func TestEnableRefusesToRebuildOntoAWritablePeerCopy(t *testing.T) {
	t.Run("promoted copy whose old-primary end was removed (ENDED, writable)", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB("allowNonEmptyTarget", "true"))
		rel.targetRO = false
		rel.state = qumulo.RelationshipDisconnected
		require.NoError(t, s.disable(t, s.handleA, classToB()))
		before := s.fabric.relCount()

		err := s.enable(t, classToB("allowNonEmptyTarget", "true"))

		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), "was promoted")
		assert.Contains(t, err.Error(), "secondary")
		assert.Equal(t, before, s.fabric.relCount(), "nothing created")
		assert.False(t, rel.targetRO, "the peer's copy is untouched")
	})

	t.Run("a leftover read-only replica is adopted with consent, as before", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB())
		require.NoError(t, s.disable(t, s.handleA, classToB())) // both ends gone, replica directory stays
		assert.Zero(t, s.fabric.relCount())
		_ = rel

		require.NoError(t, s.enable(t, classToB("allowNonEmptyTarget", "true")))
		assert.Equal(t, 1, s.fabric.relCount())
	})
}

// After a failover, Disable on the promoted
// side removes both relationship ends, so no end on either array says which
// copy is live, and the guard above found nothing. The promoted copy's export
// still says it: prestage creates it read-only, only a promote makes it
// read-write, and Disable leaves it alone.
func TestEnableRefusesToOverwriteAPromotedCopyWhoseEndsAreGone(t *testing.T) {
	requireRefused := func(t *testing.T, s *replScenario, err error, liveData string) {
		t.Helper()
		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), "was promoted")
		assert.Contains(t, err.Error(), "read-write")
		assert.Zero(t, s.fabric.relCount(), "nothing created toward the live copy")
		assert.NotContains(t, s.b.calls, "Authorize")
		_, ok := s.b.dirs[liveData]
		assert.True(t, ok, "the promoted copy's data is untouched")
	}

	t.Run("planned failover, then the DR side's VolumeReplication is deleted", func(t *testing.T) {
		s := newReplScenario(t)
		class := classToB("allowNonEmptyTarget", "true")
		s.protect(t, class)
		require.NoError(t, s.promote(t, s.handleB(t), classToA("allowNonEmptyTarget", "true"), false))
		s.b.mkdir(replVolumePath + "/written-on-dr-after-failover")
		require.NoError(t, s.disable(t, s.handleB(t), classToA("allowNonEmptyTarget", "true")))
		require.Zero(t, s.fabric.relCount(), "Disable on the promoted side removed both ends")
		s.b.calls = nil

		err := s.enable(t, class)

		requireRefused(t, s, err, replVolumePath+"/written-on-dr-after-failover")
	})

	t.Run("unplanned failover, the DR side's end removed during the outage", func(t *testing.T) {
		s := newReplScenario(t)
		class := classToB("allowNonEmptyTarget", "true")
		s.protect(t, class)
		s.a.down = true
		require.NoError(t, s.promote(t, s.handleB(t), classToA("allowNonEmptyTarget", "true"), true))
		s.b.mkdir(replVolumePath + "/written-during-outage")
		require.NoError(t, s.disable(t, s.handleB(t), classToA("allowNonEmptyTarget", "true")))
		s.a.down = false
		corpse := s.fabric.onlyRel()
		require.Equal(t, qumulo.RelationshipEnded, corpse.state, "A's end reads ENDED")
		// On Core the returning source reads ENDED (measured live); drop the
		// fabric's "still ESTABLISHED" override from make-writable.
		corpse.sourceReportsState = ""
		s.b.calls = nil

		err := s.enable(t, class)

		requireRefused(t, s, err, replVolumePath+"/written-during-outage")
	})

	t.Run("a never-promoted replica holding data is still adopted with consent", func(t *testing.T) {
		s := newReplScenario(t)
		s.protect(t, classToB())
		s.b.mkdir(replVolumePath + "/replicated-before-disable")
		require.NoError(t, s.disable(t, s.handleA, classToB()))
		require.Zero(t, s.fabric.relCount())

		require.NoError(t, s.enable(t, classToB("allowNonEmptyTarget", "true")),
			"the replica's export is still the read-only one prestage made")
		assert.Equal(t, 1, s.fabric.relCount())
	})

	t.Run("an empty directory under a read-write export holds nothing to lose", func(t *testing.T) {
		s := newReplScenario(t)
		s.b.mkdir(replVolumePath)
		s.b.exports = append(s.b.exports, qumulo.ExportInfo{ID: "7", ExportPath: replExportPath, FSPath: replVolumePath,
			Description: exportDescriptionFor(path.Base(replVolumePath))})

		require.NoError(t, s.enable(t, classToB("allowNonEmptyTarget", "true")))
		assert.Equal(t, 1, s.fabric.relCount())
	})
}

func TestDisableFromTheTargetSide(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())

	require.NoError(t, s.disable(t, s.handleB(t), classToA()))

	assert.Zero(t, s.fabric.relCount())
	assert.Contains(t, s.b.calls, "DeleteTarget")
	assert.Contains(t, s.a.calls, "DeleteSource")
}

func TestDisableWithPeerUnreachableIsUnavailable(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.b.down = true

	requireCode(t, s.disable(t, s.handleA, classToB()), codes.Unavailable)
}

// Measured with C1 powered off (2026-09-14): deleting the DR side's
// VolumeReplication hung on its finalizer because Disable insisted on the
// dead peer. A replica end is removable on its own.
func TestDisableOnAReplicaWithPeerDownRemovesOnlyItsEnd(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.a.down = true
	s.b.calls = nil

	require.NoError(t, s.disable(t, s.handleB(t), classToA()))
	assert.Contains(t, s.b.calls, "DeleteTarget")
	assert.False(t, rel.hasTarget, "the replica end is gone")
	assert.True(t, rel.hasSource, "the dead source's end was not touched")
}

// Measured on the lab cluster (2026-09-14, second outage): the DR side removed
// its target end while the source array was down and re-protected the volume
// the other way when it returned, leaving the old primary with an ENDED source
// end beside the new live target end. Demote refused to guess between them.
func TestAnEndedEndBesideALiveRelationshipIsIgnored(t *testing.T) {
	s := newReplScenario(t)
	corpse := s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath, qumulo.RelationshipEnded, func(r *fakeRel) {
		r.hasTarget = false
		r.endReason = "deleted by the target cluster"
	})
	live := s.fabric.seedRel(s.b, s.a, replVolumePath, replVolumePath, qumulo.RelationshipEstablished, nil)
	s.b.mkdir(replVolumePath)

	require.NoError(t, s.demote(t, s.handleA, classToB()), "the live target end decides; the corpse is ignored")
	assert.Equal(t, qumulo.RelationshipEnded, corpse.state, "the corpse is left for Enable to clean up")
	assert.Equal(t, qumulo.RelationshipEstablished, live.state)
	assert.False(t, live.sourceRO, "a target end is not frozen")
}

// A lone ENDED source end has nothing to freeze against.
func TestDemoteOfALoneEndedSourceEndSucceeds(t *testing.T) {
	s := newReplScenario(t)
	s.fabric.seedRel(s.a, s.b, replVolumePath, replVolumePath, qumulo.RelationshipEnded, func(r *fakeRel) { r.hasTarget = false })

	require.NoError(t, s.demote(t, s.handleA, classToB()))
	assert.Empty(t, s.a.calls, "nothing is patched on an ENDED relationship")
}

func TestDisableOfAVolumeThatNoLongerResolvesSucceeds(t *testing.T) {
	s := newReplScenario(t)
	gone := newVolumeID(arrayA, "", "/csi/volumes/pvc-424242").String()

	require.NoError(t, s.disable(t, gone, classToB()))
}

// --- PromoteVolume ------------------------------------------------------------

func TestPromoteOnTheWritableSourceIsANoOp(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.a.calls, s.b.calls = nil, nil

	require.NoError(t, s.promote(t, s.handleA, classToB(), false))
	require.NoError(t, s.promote(t, s.handleA, classToB(), true))

	assert.Empty(t, s.a.calls)
	assert.Empty(t, s.b.calls)
}

func TestPromoteUnfreezesAnAbandonedPlannedPromote(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	// A planned promote froze A and then died; B is still a read-only
	// target. Nothing else will ever unfreeze A.
	rel.sourceRO = true

	require.NoError(t, s.promote(t, s.handleA, classToB(), false))

	assert.False(t, rel.sourceRO, "the abandoned freeze must be cleared")
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	assert.Equal(t, []string{"PatchSource"}, s.a.calls[1:])

	// Again: already primary, nothing to do.
	s.a.calls = nil
	require.NoError(t, s.promote(t, s.handleA, classToB(), false))
	assert.Empty(t, s.a.calls)
}

func TestPromoteRefusesAFrozenSourceWhosePeerIsWritable(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.sourceRO = true
	rel.targetRO = false // B promoted

	for _, force := range []bool{false, true} {
		err := s.promote(t, s.handleA, classToB(), force)

		// Aborted, never FailedPrecondition: the controller would answer
		// FailedPrecondition with force, and force must not unfreeze either.
		requireCode(t, err, codes.Aborted)
		assert.Contains(t, err.Error(), "split brain")
		assert.True(t, rel.sourceRO, "must not unfreeze into two writable copies (force=%v)", force)
	}
}

func TestPromoteRefusesADisconnectedSource(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.state = qumulo.RelationshipDisconnected
	rel.targetRO = false

	for _, force := range []bool{false, true} {
		err := s.promote(t, s.handleA, classToB(), force)
		requireCode(t, err, codes.Aborted)
		assert.Contains(t, err.Error(), "secondary and resync")
	}
}

// No relationship is not a reason to force: Aborted, and the message names
// Enable. FailedPrecondition here would have the controller re-call with
// force for nothing.
func TestPromoteWithoutARelationshipIsAbortedNotForced(t *testing.T) {
	s := newReplScenario(t)
	for _, force := range []bool{false, true} {
		err := s.promote(t, s.handleA, classToB(), force)
		requireCode(t, err, codes.Aborted)
		assert.Contains(t, err.Error(), "enable replication first")
	}
}

// The planned failover, from B's side: freeze A, drain against A's clock,
// make B writable, flip B's export to read-write.
func TestPromotePlannedFailover(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.a.calls, s.b.calls = nil, nil

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	assert.True(t, rel.sourceRO, "the source is frozen (3.2)")
	assert.Equal(t, qumulo.RelationshipDisconnected, rel.state)
	assert.False(t, rel.targetRO, "the target is writable (3.6)")
	assert.Equal(t, qumulo.ReplicationModeContinuous, rel.mode, "a continuous relationship needs no mode switch")
	assert.Equal(t, []string{"CreateExpiringSnapshot", "PatchSource", "CreateExpiringSnapshot", "DeleteSnapshot", "SnapshotDiffEmpty", "DeleteSnapshot"}, s.a.calls,
		"pending marker, freeze, marker of the frozen tree replacing it, one comparison, marker removed: never replicate")
	assert.Equal(t, []string{"MakeTargetWritable", "SetExportReadOnly"}, s.b.calls)
	require.Len(t, s.b.exports, 1)
	assert.False(t, s.b.exports[0].ReadOnly, "the pre-staged export is flipped to read-write at promote")

	// A second call lands on the "already promoted" row: no second
	// make-writable, which the array would refuse.
	s.a.calls, s.b.calls = nil, nil
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	assert.Empty(t, s.b.calls)
	assert.Empty(t, s.a.calls)
}

func TestPromotePlannedSwitchesAPolicyOnlyRelationshipToDrain(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB("replicationMode", "policy"))
	assert.Equal(t, qumulo.ReplicationModeSnapshotPolicy, rel.mode)

	require.NoError(t, s.promote(t, s.handleB(t), classToA("replicationMode", "policy"), false))

	assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, rel.mode,
		"policy-only cannot drain a frozen source; the mode is switched for the drain")
	assert.Equal(t, qumulo.RelationshipDisconnected, rel.state)
	assert.NotContains(t, s.a.calls, "Replicate")
}

func TestPromotePlannedAbortsWhileDraining(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	// Writes since the last replication, and no cycle landing during the
	// attempt: the target does not hold the frozen tree.
	rel.write()
	s.fabric.frozenPoint = true

	err := s.promote(t, s.handleB(t), classToA(), false)

	requireCode(t, err, codes.Aborted)
	assert.Contains(t, err.Error(), "still draining")
	assert.True(t, rel.sourceRO, "the freeze holds across the retry (3.4)")
	assert.True(t, rel.targetRO, "the target must not be promoted before the drain completes")
	assert.NotContains(t, s.b.calls, "MakeTargetWritable")

	// The cycle lands; the retry finds the same marker, the diff is empty,
	// and finishes.
	s.fabric.replicate(rel)
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	assert.False(t, rel.targetRO)
	assert.Empty(t, s.a.snapshots, "the marker is removed once the promote completes")
}

func TestPromotePlannedSnapshotGate(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	replicaID := s.b.dirs[replVolumePath]
	// A policy snapshot on the source newer than anything on the target.
	newer := time.Now().Add(-time.Minute)
	s.a.snapshots = []qumulo.SnapshotInfo{
		{ID: "10", SourceFileID: s.fileIDA, CreatedAt: newer.Add(-time.Hour), PolicyID: "7"},
		{ID: "11", SourceFileID: s.fileIDA, CreatedAt: newer, PolicyID: "7"},
		{ID: "12", SourceFileID: s.fileIDA, CreatedAt: newer.Add(time.Second), PolicyID: ""}, // manual: not gated
		{ID: "13", SourceFileID: "other", CreatedAt: newer.Add(time.Second), PolicyID: "9"},  // another volume
	}
	s.b.snapshots = []qumulo.SnapshotInfo{
		{ID: "2", SourceFileID: replicaID, CreatedAt: newer.Add(-time.Hour)},
	}

	err := s.promote(t, s.handleB(t), classToA(), false)
	requireCode(t, err, codes.Aborted)
	assert.True(t, rel.targetRO, "a copy without its restore points is not promoted")

	// The snapshot crosses -- by timestamp, since the name and policy do not
	// travel -- and the retry promotes. The target renders the instant in
	// another zone: the two arrays need not agree on a zone suffix, and the
	// match is on the instant, never on time.Time equality.
	s.b.snapshots = append(s.b.snapshots, qumulo.SnapshotInfo{
		ID: "3", SourceFileID: replicaID, CreatedAt: newer.In(time.FixedZone("array-b", 3600)),
	})
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	assert.False(t, rel.targetRO)
}

// In its state-based form: the promote takes a marker snapshot
// of the frozen tree and promotes when the array reports no change between
// the marker and the last replicated snapshot. A target that already holds
// everything is promoted at once; no cycle is waited for.
func TestPromotePromotesAtOnceWhenTheTargetAlreadyHoldsTheFrozenTree(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.fabric.frozenPoint = true // no cycle will land during the attempt
	started := time.Now()

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	assert.True(t, rel.sourceRO)
	assert.False(t, rel.targetRO)
	assert.Less(t, time.Since(started), time.Second, "nothing to wait for: the replicated snapshot equals the marker")
	assert.Empty(t, s.a.snapshots, "the marker is removed")
}

// Reproduced on the lab cluster: the operator
// set the original secondary and the DR copy primary in one go. Demote froze
// the source; Promote arrived 0.6 s later, found it already frozen, and the
// then-current shortcut accepted the idle recovery point -- 55 s older than
// the freeze -- because "an earlier freeze ran its job". The copy was promoted
// 55 s behind a quiesced source and the resync discarded the difference. Now
// the gate is about data: the marker of the frozen tree differs from what the
// target holds, so the promote waits, however the source came to be frozen.
func TestPromoteAfterADemoteFrozeTheSourceWaitsUntilTheTargetHoldsTheFrozenTree(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.write() // writes since the last cycle
	s.fabric.frozenPoint = true

	require.NoError(t, s.demote(t, s.handleA, classToB()))
	require.True(t, rel.sourceRO, "Demote froze the source")

	err := s.promote(t, s.handleB(t), classToA(), false)
	requireCode(t, err, codes.Aborted)
	assert.Contains(t, err.Error(), "does not yet hold the frozen tree")
	assert.True(t, rel.targetRO, "a target missing writes is not promoted, frozen source or not")
	assert.True(t, rel.sourceRO, "the freeze holds across the retry")
	require.Len(t, s.a.snapshots, 1, "one marker, waiting on the array for the retry")

	// The cycle lands: the retry compares against the SAME marker.
	s.fabric.replicate(rel)
	s.a.calls = nil
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	assert.False(t, rel.targetRO)
	assert.NotContains(t, s.a.calls, "CreateExpiringSnapshot", "the retry reuses the marker")
	// The marker is Demote's, and it outlives the promote so that an undo
	// of this promote still sees whose freeze it was; it expires.
	require.Len(t, s.a.snapshots, 1)
	assert.True(t, isHeldFreezeMarker(s.a.snapshots[0], rel.id))
}

// The same for a retry of Promote itself: the previous attempt's freeze earns
// nothing; what the target holds is what decides.
func TestPromoteRetryOnAnAlreadyFrozenSourceStillWaitsForTheTarget(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.write()
	rel.sourceRO = true
	s.fabric.frozenPoint = true

	err := s.promote(t, s.handleB(t), classToA(), false)
	requireCode(t, err, codes.Aborted)
	assert.True(t, rel.targetRO)

	s.fabric.replicate(rel)
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	assert.False(t, rel.targetRO)
}

// A marker left by an earlier failover, or taken before a hand unfreeze,
// predates writes the target already has, and is never trusted. On an
// unfrozen source it is removed when the promote records its intent,
// and the drain compares against a marker of the frozen tree; on a frozen
// one it compares different against a NEWER replicated snapshot and is
// replaced (TestAStaleMarkerOnAFrozenSourceIsReplacedWithItsOwnKind). The old
// phase lock cannot recur: no attempt stamps anything.
func TestPromoteReplacesAStaleFreezeMarker(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	stale, err := s.a.CreateExpiringSnapshot(context.Background(), s.a.dirs[replVolumePath], freezeMarkerName(rel.id), time.Now().Add(time.Hour))
	require.NoError(t, err)
	rel.write()             // writes after the stale marker...
	s.fabric.replicate(rel) // ...that the target already holds

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	assert.False(t, rel.targetRO, "promoted: the target holds the frozen tree")
	for _, snap := range s.a.snapshots {
		assert.NotEqual(t, stale.ID, snap.ID, "the stale marker is gone")
	}
	assert.Contains(t, s.a.calls, "DeleteSnapshot")
}

// Reproduced on the fabric before the fix: a policy-only class,
// the old side set secondary first (Demote froze A; in policy-only mode no
// job runs), then the DR side primary. The already-frozen shortcut accepted
// the idle recovery point -- the last policy snapshot, up to one interval
// old -- and promoted a copy missing every write since. There is no shortcut
// any more; a policy-only frozen source is additionally switched to
// the combined mode so that a cycle exists for the point to move on.
func TestPromoteDoesNotAcceptAStaleRecoveryPointOnAPolicyOnlyFrozenSource(t *testing.T) {
	clsB := classToA("replicationMode", "policy")

	t.Run("nothing moves the recovery point: still draining, never promoted", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB("replicationMode", "policy"))
		rel.sourceRO = true // frozen by a Demote that did not switch the mode (older driver, or by hand)
		rel.write()         // writes since the last policy fire
		s.fabric.frozenPoint = true

		err := s.promote(t, s.handleB(t), clsB, false)

		requireCode(t, err, codes.Aborted)
		assert.Contains(t, err.Error(), "still draining")
		assert.True(t, rel.targetRO, "a recovery point older than the freeze must not be promoted")
		assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, rel.mode, "the mode is switched so the freeze can drain")
		assert.True(t, rel.sourceRO)
	})

	t.Run("the mode switch starts the cycle and the fresh point is promoted", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB("replicationMode", "policy"))
		rel.sourceRO = true
		rel.write()

		require.NoError(t, s.promote(t, s.handleB(t), clsB, false))

		assert.False(t, rel.targetRO)
		assert.Equal(t, rel.gen, rel.replicatedGen, "promoted only once the target held the frozen tree")
		assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, rel.mode)
	})
}

// The whole planned sequence in policy mode, in the order an operator
// following the runbook uses: Demote the old side first, then promote the
// DR side. Demote switches the mode so its freeze starts a job, and the
// promote finds a drained source rather than a stale snapshot.
func TestDemoteThenPromoteInPolicyModeDrainsThroughTheModeSwitch(t *testing.T) {
	s := newReplScenario(t)
	cls := classToB("replicationMode", "policy")
	rel := s.protect(t, cls)
	stale := time.Now().Add(-10 * time.Minute)
	rel.recoveryPoint = stale

	require.NoError(t, s.demote(t, s.handleA, cls))
	assert.True(t, rel.sourceRO)
	assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, rel.mode,
		"Demote switches a policy-only relationship to the combined mode so the freeze drains")

	require.NoError(t, s.promote(t, s.handleB(t), classToA("replicationMode", "policy"), false))
	assert.False(t, rel.targetRO)
	assert.True(t, rel.recoveryPoint.After(stale))
}

func TestPromotePlannedWithSourceDownIsFailedPreconditionThenForceSucceeds(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.a.down = true
	s.b.calls = nil

	// force=false: the controller learns it must retry with force.
	// FailedPrecondition is what makes the csi-addons controller retry with force.
	requireCode(t, s.promote(t, s.handleB(t), classToA(), false), codes.FailedPrecondition)
	assert.True(t, rel.targetRO)

	// force=true: no call reaches A; B is promoted at its recovery point.
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	assert.Equal(t, qumulo.RelationshipDisconnected, rel.state)
	assert.False(t, rel.targetRO)
	assert.False(t, rel.sourceRO, "the dead source was never touched")
	assert.Equal(t, []string{"MakeTargetWritable", "SetExportReadOnly"}, s.b.calls)

	// Forced again: already promoted, idempotent, still nothing to A.
	s.b.calls = nil
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	assert.Empty(t, s.b.calls)
}

// Reproduced on the fabric before the fix: the DR class carries a
// mistyped targetBasePath, the peer is UP and lists nothing at that path.
// The old code returned FailedPrecondition ("peer did not answer"), the
// controller re-called with force, force skipped the peer, and both copies
// ended writable with nothing frozen and nothing drained. Now: the peer
// answered, so it is a misconfiguration -- InvalidArgument, both ways, and
// nothing changes on either array.
func TestPromoteWithAMistypedTargetBasePathIsRefusedNotForced(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	wrongClass := classToA("targetBasePath", "/csi/other")
	s.a.calls, s.b.calls = nil, nil

	for _, force := range []bool{false, true} {
		err := s.promote(t, s.handleB(t), wrongClass, force)

		requireCode(t, err, codes.InvalidArgument)
		assert.Contains(t, err.Error(), "answered but lists no end")
		assert.Contains(t, err.Error(), paramTargetBasePath)
		assert.False(t, rel.sourceRO, "A must not be frozen through a class that does not describe the relationship (force=%v)", force)
		assert.True(t, rel.targetRO, "B must not be made writable (force=%v)", force)
		assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	}
	assert.Empty(t, s.a.calls)
	assert.Empty(t, s.b.calls)
}

// Reproduced on the fabric before the fix: a planned failover is
// complete (A frozen, B writable) and A's VolumeReplication is set back to
// primary -- operator error, GitOps drift, a failback run against a
// half-applied state. force=false was refused with FailedPrecondition, the
// controller re-called with force, and the peer-blind path unfroze A beside
// the writable B. Now both attempts are Aborted and A stays frozen.
func TestPromoteOnTheOldPrimaryAfterAPlannedFailoverNeverUnfreezes(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false)) // planned failover
	require.True(t, rel.sourceRO)
	require.False(t, rel.targetRO)
	s.a.calls = nil

	for _, force := range []bool{false, true} {
		err := s.promote(t, s.handleA, classToB(), force)

		requireCode(t, err, codes.Aborted)
		assert.Contains(t, err.Error(), "holds the writable copy")
		assert.True(t, rel.sourceRO, "A must stay frozen while B is writable (force=%v)", force)
		assert.False(t, rel.targetRO)
	}
	assert.Empty(t, s.a.calls, "no PATCH reaches the frozen source")
}

// The unplanned variant of the same mistake: A was never frozen (B was
// force-promoted while A was down) and comes back writable with
// its own end still ESTABLISHED. Set to primary, both attempts read
// B's writable end and refuse; neither reports Primary over two writable
// copies.
func TestPromoteOnAnUnfrozenOldPrimaryAfterAnUnplannedFailoverIsRefused(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.a.down = true
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	s.a.down = false
	require.False(t, rel.sourceRO)
	require.False(t, rel.targetRO)

	for _, force := range []bool{false, true} {
		err := s.promote(t, s.handleA, classToB(), force)
		requireCode(t, err, codes.Aborted)
		assert.Contains(t, err.Error(), "holds the writable copy")
	}
}

// A frozen source whose peer cannot be reached is never unfrozen, force or
// not: the peer may have promoted in the meantime. Unavailable, which the
// controller retries without force, and the freeze holds until the peer
// answers and is read as a read-only target.
func TestPromoteOfAFrozenSourceWithThePeerDownHoldsTheFreeze(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.sourceRO = true
	s.b.down = true
	s.a.calls = nil

	for _, force := range []bool{false, true} {
		err := s.promote(t, s.handleA, classToB(), force)
		requireCode(t, err, codes.Unavailable)
		assert.Contains(t, err.Error(), "freeze is held")
		assert.True(t, rel.sourceRO)
	}
	assert.Empty(t, s.a.calls)

	// The peer answers and its end is still a read-only target: the
	// abandoned freeze is cleared, as it must be.
	s.b.down = false
	require.NoError(t, s.promote(t, s.handleA, classToB(), true))
	assert.False(t, rel.sourceRO)
}

// force means "proceed if the peer cannot be reached", not "do not look".
// With the source answering, a forced promote of a read-only target freezes
// and drains it exactly as a planned one does.
func TestPromoteForcedWithAReachableSourceStillDrains(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.a.calls, s.b.calls = nil, nil

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))

	assert.True(t, rel.sourceRO, "the reachable source is frozen before the target is made writable")
	assert.False(t, rel.targetRO)
	assert.Equal(t, []string{"CreateExpiringSnapshot", "PatchSource", "CreateExpiringSnapshot", "DeleteSnapshot", "SnapshotDiffEmpty", "DeleteSnapshot"}, s.a.calls, "the freeze reached A, with its marker")
	assert.Equal(t, []string{"MakeTargetWritable", "SetExportReadOnly"}, s.b.calls)
}

// A writable source whose peer cannot be consulted is not reported primary:
// after an unplanned failover the old primary looks exactly like this, and
// its own end says ESTABLISHED whatever the peer did.
func TestPromoteOnAWritableSourceWithThePeerDownIsUnavailable(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.b.down = true

	for _, force := range []bool{false, true} {
		err := s.promote(t, s.handleA, classToB(), force)
		requireCode(t, err, codes.Unavailable)
		assert.Contains(t, err.Error(), "not reporting primary")
	}
}

// A cycle job may be in flight when the promote lands. The drain waits for
// it to finish (a relationship is made writable only when idle, and
// only once) and then compares what the target holds with the marker.
func TestPromoteWaitsForARunningJob(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.fabric.lag = time.Hour // the job in flight takes an hour
	s.fabric.mu.Lock()
	rel.jobStart = time.Now()
	s.fabric.mu.Unlock()

	err := s.promote(t, s.handleB(t), classToA(), false)

	requireCode(t, err, codes.Aborted)
	assert.Contains(t, err.Error(), "job "+qumulo.ReplicationJobRunning)
	assert.True(t, rel.sourceRO, "frozen and held across the retry")
	assert.True(t, rel.targetRO)

	// The job completes; the retry finds an idle relationship whose
	// replicated snapshot matches the marker, and promotes.
	s.fabric.lag = 0
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	assert.False(t, rel.targetRO)
	after, err := s.a.GetSourceStatus(context.Background(), rel.id)
	require.NoError(t, err)
	assert.NotEmpty(t, after.RecoveryPointSnapshotID, "the completed job carries a snapshot id")
	assert.True(t, after.JobIdle())
}

// The clock-skew case the fabric could not model while its clock was
// the test's: the freeze stamp and the recovery point both come from the
// SOURCE array's clock, so a source running two hours ahead of or behind the
// controller drains the same. A gate that stamped the controller's clock
// would pass a stale point with the array ahead and hang with it behind.
func TestPromoteDrainsAgainstTheSourceArraysClockNotTheControllers(t *testing.T) {
	for _, skew := range []time.Duration{2 * time.Hour, -2 * time.Hour} {
		t.Run(fmt.Sprintf("source clock %v from the controller's", skew), func(t *testing.T) {
			s := newReplScenario(t)
			s.a.clockSkew = skew
			rel := s.protect(t, classToB())

			require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

			assert.True(t, rel.sourceRO)
			assert.False(t, rel.targetRO)
			assert.WithinDuration(t, time.Now().Add(skew), rel.recoveryPoint, 5*time.Second,
				"the recovery point is the array's own timestamp")
		})
	}
}

// Measured on Core 7.9.2.2: a frozen
// continuous source's recovery point keeps moving on the array's 60 s cycle,
// with no job of its own. The fabric models that; a test that wants a pinned
// point uses frozenPoint.
func TestAFrozenSourcesRecoveryPointKeepsMovingOnTheCycle(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())

	first, err := s.a.GetSourceStatus(context.Background(), rel.id)
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	second, err := s.a.GetSourceStatus(context.Background(), rel.id)
	require.NoError(t, err)
	assert.True(t, second.RecoveryPoint.After(first.RecoveryPoint), "an unfrozen continuous source cycles")

	rel.sourceRO = true
	frozen1, err := s.a.GetSourceStatus(context.Background(), rel.id)
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	frozen2, err := s.a.GetSourceStatus(context.Background(), rel.id)
	require.NoError(t, err)
	assert.True(t, frozen2.RecoveryPoint.After(frozen1.RecoveryPoint), "a frozen source's recovery point moves on the cycle too")
	assert.True(t, frozen1.JobIdle(), "and a freeze starts no job")
}

// As the fabric now models it: after a reconnect the SOURCE end says
// DISCONNECTED for a while although the target end is ESTABLISHED and
// read-only. Read from the peer's end, that window is a retry and a
// DEGRADED, not a split-brain refusal and an ERROR.
func TestReconnectLagIsReadFromThePeerEndNotThisOnes(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false)) // planned failover: B writable, A frozen
	s.fabric.reconnectLag = time.Hour
	_, err := s.resync(t, s.handleA, classToB(), false) // reverse on B, reconnect A
	require.NoError(t, err)
	require.Equal(t, s.b, rel.source, "B is the source now")

	sources, err := s.b.ListSourceStatus(context.Background())
	require.NoError(t, err)
	require.Len(t, sources, 1)
	require.Equal(t, qumulo.RelationshipDisconnected, sources[0].State, "the fabric reproduces the lagging source end")

	// Promote on the new primary during the lag: a retry, not a refusal
	// naming a writable peer that does not exist.
	err = s.promote(t, s.handleB(t), classToA(), false)
	requireCode(t, err, codes.Aborted)
	assert.Contains(t, err.Error(), "catch up")

	// Info on it: degraded, with the reason, rather than ERROR.
	h := s.assess(t, s.handleB(t), classToA())
	assert.Equal(t, ReplicationDegraded, h.Status, "message: %s", h.Message)
	assert.Contains(t, h.Message, "catching up")

	// The ends agree; the same promote is the ordinary "already primary".
	s.fabric.reconnectLag = 0
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	assert.Equal(t, ReplicationHealthy, s.assess(t, s.handleB(t), classToA()).Status)
}

func TestPromoteReplumbsAVolumeProtectedBeforePrestaging(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	// Drop what Enable staged, as for a volume protected by hand before
	// this feature existed.
	replicaID := s.b.dirs[replVolumePath]
	s.b.exports = nil
	delete(s.b.quotas, replicaID)
	s.b.calls = nil

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	assert.Equal(t, qumulo.RelationshipDisconnected, rel.state)
	require.Len(t, s.b.exports, 1, "the export is recreated from the peer's definition")
	assert.Equal(t, replExportPath, s.b.exports[0].ExportPath)
	assert.False(t, s.b.exports[0].ReadOnly)
	assert.Equal(t, replQuota, s.b.quotas[replicaID], "the quota is recreated from the peer's limit")
}

// --- DemoteVolume -------------------------------------------------------------

func TestDemoteFreezesTheSourceAndOnlyTheSource(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.a.calls, s.b.calls = nil, nil

	require.NoError(t, s.demote(t, s.handleA, classToB()))
	assert.True(t, rel.sourceRO)
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state, "demote never disconnects or reverses")
	// The freeze, then its held marker.
	assert.Equal(t, []string{"PatchSource", "CreateExpiringSnapshot"}, s.a.calls)
	assert.Empty(t, s.b.calls, "nothing changes on the peer: it is only read, and demote works on an old primary whose peer is down")

	// Idempotent.
	s.a.calls = nil
	require.NoError(t, s.demote(t, s.handleA, classToB()))
	assert.Empty(t, s.a.calls)
}

func TestDemoteOnATargetIsANoOp(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.b.calls = nil

	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	assert.Empty(t, s.b.calls)
}

// Reproduced on the fabric before the fix: Demote of a promoted
// (writable, DISCONNECTED) target returned OK and changed nothing, so with
// autoResync off the copy stayed writable and mountable while the object
// read Secondary. Nothing on the array makes it read-only again but a
// reconnect, so Demote fences at the export: the pre-staged export goes
// read-only, the way promote flipped it the other way.
func TestDemoteOfAWritableTargetFencesItsExport(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.a.down = true
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	require.False(t, rel.targetRO)
	require.False(t, s.b.exports[0].ReadOnly, "promote flipped the export read-write")
	s.b.calls = nil

	require.NoError(t, s.demote(t, s.handleB(t), classToA()))

	assert.True(t, s.b.exports[0].ReadOnly, "the export is fenced read-only")
	assert.Equal(t, []string{"SetExportReadOnly"}, s.b.calls)
	assert.False(t, rel.targetRO, "the directory itself stays writable until the resync reconnects it; the log says so")
	assert.Equal(t, qumulo.RelationshipDisconnected, rel.state, "demote never reconnects or reverses")

	// Idempotent: already fenced, nothing to do.
	s.b.calls = nil
	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	assert.Empty(t, s.b.calls)

	// A later promote of the same copy flips the export back.
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	assert.False(t, s.b.exports[0].ReadOnly)
}

func TestDemoteWorksWithThePeerDown(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.state = qumulo.RelationshipDisconnected // B promoted while A was down; A came back writable
	rel.targetRO = false
	s.b.down = true

	require.NoError(t, s.demote(t, s.handleA, classToB()))
	assert.True(t, rel.sourceRO)
}

func TestDemoteWithoutARelationshipIsFailedPrecondition(t *testing.T) {
	s := newReplScenario(t)
	requireCode(t, s.demote(t, s.handleA, classToB()), codes.FailedPrecondition)
}

// --- ResyncVolume -------------------------------------------------------------

func TestResyncOnAnEstablishedTargetReportsReadiness(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())

	ready, err := s.resync(t, s.handleB(t), classToA(), false)
	require.NoError(t, err)
	assert.True(t, ready)

	// Before the first job completes the replica is empty: not ready.
	rel.recoveryPoint = time.Time{}
	rel.mode = qumulo.ReplicationModeSnapshotPolicy // pins the recovery point in the fake
	ready, err = s.resync(t, s.handleB(t), classToA(), false)
	require.NoError(t, err)
	assert.False(t, ready)
}

// The drill: B made writable, tested, then reconnected -- which discards
// B's drill writes, so it needs force.
func TestResyncOfAWritableTargetNeedsForceThenReconnects(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	// An unplanned failover: A down, B force-promoted at its recovery
	// point, A back writable afterwards. The only forced promote
	// that skips the freeze is one whose source cannot be reached.
	s.a.down = true
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	s.a.down = false
	assert.False(t, rel.sourceRO, "the unreachable source was never frozen")
	s.b.calls = nil

	_, err := s.resync(t, s.handleB(t), classToA(), false)
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "discards")
	assert.Empty(t, s.b.calls)

	ready, err := s.resync(t, s.handleB(t), classToA(), true)
	require.NoError(t, err)
	assert.False(t, ready, "not ready until ESTABLISHED with a recovery point")
	assert.Equal(t, []string{"Reconnect"}, s.b.calls)
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	assert.True(t, rel.targetRO)

	ready, err = s.resync(t, s.handleB(t), classToA(), false)
	require.NoError(t, err)
	assert.True(t, ready)
}

// The reprotect after a planned failover: A was demoted (frozen), B is the
// writable ex-target. Resync on A reverses on B, re-applies the class mode,
// links a policy on the new source, and reconnects A.
// The third live run (Core 7.9.2.2, 2026-09-14) failed here: after the
// planned promote the old primary's own status still read ESTABLISHED, so a
// Resync on it was refused as "the ESTABLISHED source". Only the target end
// reports DISCONNECTED until the reverse. The peer end is the evidence, and
// the fabric now models the disagreement.
func TestResyncOnTheOldPrimaryReadsThePeerEndNotItsOwnState(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	sources, err := s.a.ListSourceStatus(context.Background())
	require.NoError(t, err)
	require.Len(t, sources, 1)
	assert.Equal(t, qumulo.RelationshipEstablished, sources[0].State,
		"the fabric must reproduce the measured lag: the source end still says ESTABLISHED")
	targets, err := s.b.ListTargetStatus(context.Background())
	require.NoError(t, err)
	require.Len(t, targets, 1)
	assert.Equal(t, qumulo.RelationshipDisconnected, targets[0].State)
	assert.False(t, targets[0].TargetRootReadOnly)

	// Promote on the old primary must not report success (two writable
	// copies), and must not be FailedPrecondition either, which the
	// controller would answer with a forced promote.
	err = s.promote(t, s.handleA, classToB(), false)
	requireCode(t, err, codes.Aborted)
	assert.Contains(t, err.Error(), "holds the writable copy")

	ready, err := s.resync(t, s.handleA, classToB(), false)
	require.NoError(t, err)
	assert.False(t, ready)
	assert.Equal(t, s.b, rel.source, "reverse ran: the promoted peer is now the source")
	assert.Equal(t, s.a, rel.target)
}

func TestResyncReversesAndReconnectsAfterFailover(t *testing.T) {
	s := newReplScenario(t)
	s.a.policies = append(s.a.policies, fakePolicy{
		PolicyInfo: qumulo.PolicyInfo{ID: "7", Name: policyNameFor(replVolumeName), SourceFileID: s.fileIDA, Enabled: true},
		schedule:   qumulo.Schedule{ExpirationTimeToLive: "7days"},
	})
	class := classToB("replicationMode", "policy+continuous", "linkSnapshotPolicy", "true")
	rel := s.protect(t, class)
	require.NoError(t, s.promote(t, s.handleB(t), classToA("replicationMode", "policy+continuous", "linkSnapshotPolicy", "true"), false))
	require.True(t, rel.sourceRO, "planned promote froze A")
	s.a.calls, s.b.calls = nil, nil

	ready, err := s.resync(t, s.handleA, class, false)
	require.NoError(t, err)
	assert.False(t, ready)

	// Same ID, roles swapped.
	assert.Equal(t, 1, s.fabric.relCount())
	assert.Equal(t, s.b, rel.source)
	assert.Equal(t, s.a, rel.target)
	assert.Equal(t, arrayA, rel.targetAddress)
	// The reverse reset mode and policies; the class was re-applied and
	// a policy created on the new source from the old source's schedule.
	assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, rel.mode)
	require.Len(t, rel.policies, 1)
	require.Len(t, s.b.policies, 1)
	assert.Equal(t, policyNameFor(replVolumeName), s.b.policies[0].Name)
	assert.Equal(t, s.b.dirs[replVolumePath], s.b.policies[0].SourceFileID)
	assert.Equal(t, "7days", s.b.policies[0].schedule.ExpirationTimeToLive)
	assert.Equal(t, s.b.policies[0].ID, rel.policies[0].ID)
	// Then A reconnected as the target.
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	assert.True(t, rel.targetRO)
	assert.Equal(t, []string{"Reverse", "CreatePolicy", "PatchSource"}, s.b.calls)
	// A's export, read-write since CreateVolume, is set read-only as A
	// becomes the replica, before the reverse.
	assert.Equal(t, []string{"SetExportReadOnly", "Reconnect"}, s.a.calls)

	// The next call sees an established target with a recovery point.
	ready, err = s.resync(t, s.handleA, class, false)
	require.NoError(t, err)
	assert.True(t, ready)
}

func TestResyncOfAWritableDisconnectedSourceNeedsForce(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	// Unplanned: A died, B was force-promoted, A came back writable
	// and nobody demoted it.
	rel.state = qumulo.RelationshipDisconnected
	rel.targetRO = false
	require.False(t, rel.sourceRO)

	_, err := s.resync(t, s.handleA, classToB(), false)
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "Demote it first, or set force")
	assert.Equal(t, s.a, rel.source, "nothing changed")

	ready, err := s.resync(t, s.handleA, classToB(), true)
	require.NoError(t, err)
	assert.False(t, ready)
	assert.Equal(t, s.b, rel.source)
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
}

func TestResyncFallsBackWhenReverseIsRefused(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	rel.refuseReverse = true
	s.a.calls, s.b.calls = nil, nil

	_, err := s.resync(t, s.handleA, classToB(), false)
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "refused to reverse")
	assert.Equal(t, 1, s.fabric.relCount())

	// With force: tear down, create B -> A, authorize on A adopting its
	// non-empty directory.
	ready, err := s.resync(t, s.handleA, classToB(), true)
	require.NoError(t, err)
	assert.False(t, ready)
	fresh := s.fabric.onlyRel()
	assert.NotEqual(t, rel.id, fresh.id, "the fallback makes a new relationship")
	assert.Equal(t, s.b, fresh.source)
	assert.Equal(t, s.a, fresh.target)
	assert.Equal(t, qumulo.RelationshipEstablished, fresh.state)
	// Both attempts tried the reverse first; only the forced one fell back.
	assert.Equal(t, []string{"Reverse", "Reverse", "DeleteTarget", "CreateSource"}, s.b.calls)
	// The export is fenced once, on the first attempt; the retry finds it
	// read-only already.
	assert.Equal(t, []string{"SetExportReadOnly", "DeleteSource", "Authorize"}, s.a.calls)
}

func TestResyncOnTheHealthySourceIsFailedPrecondition(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	_, err := s.resync(t, s.handleA, classToB(), false)
	requireCode(t, err, codes.FailedPrecondition)
}

func TestResyncWithPeerUnreachableIsUnavailable(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.state = qumulo.RelationshipDisconnected
	rel.sourceRO = true
	s.b.down = true

	_, err := s.resync(t, s.handleA, classToB(), false)
	requireCode(t, err, codes.Unavailable)
}

// --- GetVolumeReplicationInfo -------------------------------------------------

func TestAssessVolumeReplication(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		prepare func(s *replScenario, rel *fakeRel)
		want    string
		wantMsg string
	}{
		{name: "healthy", params: classToB(), want: ReplicationHealthy},
		{
			name: "lag over the interval", params: classToB("schedulingInterval", "1s"),
			prepare: func(s *replScenario, rel *fakeRel) { s.fabric.lag = 2 * time.Second },
			want:    ReplicationDegraded, wantMsg: "over the interval",
		},
		{
			name: "lag over three intervals is error, not degraded", params: classToB("schedulingInterval", "1s"),
			prepare: func(s *replScenario, rel *fakeRel) { s.fabric.lag = time.Minute },
			want:    ReplicationError,
		},
		{
			name: "no recovery point yet", params: classToB(),
			prepare: func(s *replScenario, rel *fakeRel) {
				rel.recoveryPoint = time.Time{}
				rel.mode = qumulo.ReplicationModeSnapshotPolicy
				s.a.calls = nil
			},
			want: ReplicationDegraded, wantMsg: "initial sync",
		},
		{
			name: "disconnected", params: classToB(),
			prepare: func(s *replScenario, rel *fakeRel) { rel.state = qumulo.RelationshipDisconnected },
			want:    ReplicationError, wantMsg: "state DISCONNECTED",
		},
		{
			name: "both copies writable", params: classToB(),
			prepare: func(s *replScenario, rel *fakeRel) {
				rel.state = qumulo.RelationshipDisconnected
				rel.targetRO = false
			},
			want: ReplicationError, wantMsg: "split brain",
		},
		{
			name: "peer unreachable", params: classToB(),
			prepare: func(s *replScenario, rel *fakeRel) { s.b.down = true },
			want:    ReplicationDegraded, wantMsg: "unreachable",
		},
		{
			name: "mode drift after a reverse", params: classToB("replicationMode", "policy+continuous"),
			prepare: func(s *replScenario, rel *fakeRel) { rel.mode = qumulo.ReplicationModeContinuous },
			want:    ReplicationDegraded, wantMsg: "class wants",
		},
		{
			name: "job error while established", params: classToB(),
			prepare: func(s *replScenario, rel *fakeRel) { rel.lastJobError = "connection to target lost" },
			want:    ReplicationDegraded, wantMsg: "last job",
		},
		{
			name: "pre-staged quota drifted", params: classToB(),
			prepare: func(s *replScenario, rel *fakeRel) { s.a.quotas[s.fileIDA] = 4 * replQuota },
			want:    ReplicationDegraded, wantMsg: "replica quota",
		},
		{
			// Pre-stage never lowers a replica's limit, and a larger
			// one is harmless, so it is not drift.
			name: "a replica quota above the source's", params: classToB(),
			prepare: func(s *replScenario, rel *fakeRel) { s.b.quotas[s.b.dirs[replVolumePath]] = 4 * replQuota },
			want:    ReplicationHealthy,
		},
		{
			name: "pre-staged export missing", params: classToB(),
			prepare: func(s *replScenario, rel *fakeRel) { s.b.exports = nil },
			want:    ReplicationDegraded, wantMsg: "replica export missing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newReplScenario(t)
			rel := s.protect(t, tt.params)
			if tt.prepare != nil {
				tt.prepare(s, rel)
			}

			h := s.assess(t, s.handleA, tt.params)

			assert.Equal(t, tt.want, h.Status, "message: %s", h.Message)
			if tt.wantMsg != "" {
				assert.Contains(t, h.Message, tt.wantMsg)
			}
			assert.Equal(t, rel.id, h.RelationshipID)
			assert.Equal(t, "source", h.Role)
		})
	}
}

func TestAssessFromTheTargetSide(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())

	h := s.assess(t, s.handleB(t), classToA())
	assert.Equal(t, ReplicationHealthy, h.Status)
	assert.Equal(t, "target", h.Role)
	assert.WithinDuration(t, rel.recoveryPoint, h.LastSyncTime, time.Second)
}

func TestAssessLocalUnreachableIsUnknown(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.a.down = true

	h := s.assess(t, s.handleA, classToB())
	assert.Equal(t, ReplicationUnknown, h.Status)
}

// The gRPC Info carries no class parameters in csi-addons spec v0.2.0. The
// peer is found from the source-end status and its token from the Secret.
func TestGetVolumeReplicationInfoWithoutParameters(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())

	resp, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: s.handleA, Secrets: replSecrets(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp.GetLastSyncTime())
	assert.WithinDuration(t, rel.recoveryPoint, resp.GetLastSyncTime().AsTime(), time.Second)
	assert.NotEmpty(t, s.fabric.tokens[arrayB], "the peer was identified from the status and dialed with its own token")
}

// Info carries no class parameters, so it does not know the
// interval, and a policy-mode class on a 15 m or 1 h schedule was graded
// DEGRADED then ERROR against a five-minute default on every poll. With no
// parameters the lag is reported and not graded, and the message says why;
// the same relationship assessed WITH its class is graded as before.
func TestInfoWithoutParametersDoesNotGradeLagAgainstAnUnknownInterval(t *testing.T) {
	s := newReplScenario(t)
	cls := classToB("replicationMode", "policy", "schedulingInterval", "1h")
	rel := s.protect(t, cls)
	rel.recoveryPoint = time.Now().Add(-20 * time.Minute) // the last policy fire, well inside a 1 h schedule
	s.fabric.frozenPoint = true

	paramless, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())
	require.NoError(t, err)
	assert.Equal(t, ReplicationHealthy, paramless.Status, "message: %s", paramless.Message)
	assert.Contains(t, paramless.Message, "lag not graded")
	assert.Contains(t, paramless.Message, "interval not in the request")
	assert.InDelta(t, 20*time.Minute, paramless.Lag, float64(5*time.Second), "the lag is still reported")

	// With the class: inside its hour, healthy and graded.
	withClass := s.assess(t, s.handleA, cls)
	assert.Equal(t, ReplicationHealthy, withClass.Status, "message: %s", withClass.Message)
	assert.NotContains(t, withClass.Message, "not graded")

	// And a class with a short interval grades the same recovery point ERROR,
	// so the paramless verdict is the absence of an interval, not leniency.
	short := s.assess(t, s.handleA, classToB("replicationMode", "policy", "schedulingInterval", "1m"))
	assert.Equal(t, ReplicationError, short.Status)

	// The other inputs are still graded without parameters.
	rel.lastJobError = "connection to target lost"
	paramless, err = s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())
	require.NoError(t, err)
	assert.Equal(t, ReplicationDegraded, paramless.Status)
	assert.Contains(t, paramless.Message, "last job")
}

func TestGetVolumeReplicationInfoWithoutARelationship(t *testing.T) {
	s := newReplScenario(t)
	_, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: s.handleA, Secrets: replSecrets(),
	})
	requireCode(t, err, codes.FailedPrecondition)
}

// --- destination and identity -------------------------------------------------

func TestResolveReplicationDestination(t *testing.T) {
	s := newReplScenario(t)

	_, err := s.server.ResolveReplicationDestination(context.Background(), s.handleA, "", classToB(), replSecrets())
	requireCode(t, err, codes.Unavailable)

	s.protect(t, classToB())
	dest, err := s.server.ResolveReplicationDestination(context.Background(), s.handleA, "", classToB(), replSecrets())
	require.NoError(t, err)
	assert.Equal(t, s.handleB(t), dest.VolumeID)
	assert.Equal(t, arrayB, dest.Server)
	assert.Equal(t, replVolumePath, dest.RealPath)
	// Measured: the replica has its own file ID, so the DR PV's handle
	// differs from the original in server and file ID and nothing else.
	assert.NotEqual(t, s.fileIDA, dest.FileID)
}

func TestReplicationIdentity(t *testing.T) {
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "1.2.3", Mode: ModeController, Replication: true,
		NewClient: newFakeFabric().newClient})
	require.NoError(t, err)
	id := d.ReplicationIdentityServer()
	require.NotNil(t, id)

	info, err := id.GetIdentity(context.Background(), &identity.GetIdentityRequest{})
	require.NoError(t, err)
	assert.Equal(t, "csi.qumulo.com", info.GetName())
	assert.Equal(t, "1.2.3", info.GetVendorVersion())

	caps, err := id.GetCapabilities(context.Background(), &identity.GetCapabilitiesRequest{})
	require.NoError(t, err)
	var service, volumeReplication bool
	for _, c := range caps.GetCapabilities() {
		if c.GetService().GetType() == identity.Capability_Service_CONTROLLER_SERVICE {
			service = true
		}
		if c.GetVolumeReplication().GetType() == identity.Capability_VolumeReplication_VOLUME_REPLICATION {
			volumeReplication = true
		}
	}
	assert.True(t, service && volumeReplication)

	probe, err := id.Probe(context.Background(), &identity.ProbeRequest{})
	require.NoError(t, err)
	assert.True(t, probe.GetReady().GetValue())
}

func TestReplicationIsOffUnlessAskedFor(t *testing.T) {
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeController,
		NewClient: newFakeFabric().newClient})
	require.NoError(t, err)
	assert.Nil(t, d.ReplicationServer())
	assert.Nil(t, d.ReplicationIdentityServer())
}

// A lone ENDED end -- the other array deleted its half, which the
// driver's own Disable leaves on purpose when the peer is unreachable --
// protects nothing. findRelationship still returns it, so the verdict has to
// look at the state, or QumuloCSIVolumeUnprotected stays silent.
func TestAssessStatesALoneEndedRelationshipAsUnprotected(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())

	rel := s.fabric.onlyRel()
	s.fabric.mu.Lock()
	rel.state = qumulo.RelationshipEnded
	rel.hasTarget = false
	rel.endReason = "the relationship was deleted by the target cluster"
	s.fabric.mu.Unlock()

	_, _ = s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())

	require.NotEmpty(t, s.a.protectionVerdicts)
	assert.False(t, s.a.protectionVerdicts[len(s.a.protectionVerdicts)-1], "an ENDED relationship protects nothing")
}

// freezeMarkers counts the planned promote's marker snapshots on an array.
func freezeMarkers(a *fakeArray) int {
	n := 0
	for _, snap := range a.snapshots {
		if strings.Contains(snap.Name, freezeMarkerSuffix) {
			n++
		}
	}
	return n
}

// The DR side is set primary, then back to
// secondary before the old primary is reversed. The promote froze the source
// and nothing lifted it: no copy anywhere was writable, and the source's own
// VolumeReplication, already Primary, never gets another Promote.
func TestResyncOfTheDRSideLiftsAnAbandonedPromotesFreeze(t *testing.T) {
	t.Run("the drain never finished; the DR copy never became writable", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB())
		s.fabric.frozenPoint = true // the target never catches up within the budget
		rel.write()
		requireCode(t, s.promote(t, s.handleB(t), classToA(), false), codes.Aborted)
		require.True(t, rel.sourceRO, "the drain froze A")
		require.Equal(t, 1, freezeMarkers(s.a), "and left its marker")

		// The operator abandons: the DR side back to secondary.
		require.NoError(t, s.demote(t, s.handleB(t), classToA()))
		_, err := s.resync(t, s.handleB(t), classToA(), false)
		require.NoError(t, err)

		assert.False(t, rel.sourceRO, "A is writable again")
		assert.True(t, rel.targetRO, "B is still a read-only target: one writable copy")
		assert.Zero(t, freezeMarkers(s.a), "the marker went with the freeze")
	})

	t.Run("the promote completed and is undone by the reconnect", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB())
		require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
		require.True(t, rel.sourceRO, "a planned promote leaves A frozen")
		require.False(t, rel.targetRO, "and B writable")

		// Back to secondary on the DR side: demote fences B, and the forced
		// resync reconnects it under A, discarding B's writes.
		require.NoError(t, s.demote(t, s.handleB(t), classToA()))
		_, err := s.resync(t, s.handleB(t), classToA(), true)
		require.NoError(t, err)

		assert.True(t, rel.targetRO, "B is a read-only target again")
		assert.False(t, rel.sourceRO, "A is writable again")
		assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	})

	// The reconnect's reply still shows the in-between state. The
	// freeze is lifted once the target's status reads read-only, rather than
	// held for an operator to lift by hand.
	t.Run("the reconnect reply is not read-only yet", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB())
		require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
		require.NoError(t, s.demote(t, s.handleB(t), classToA()))
		rel.reconnectReplyWritable = true

		_, err := s.resync(t, s.handleB(t), classToA(), true)
		require.NoError(t, err)

		assert.True(t, rel.targetRO, "B is a read-only target again")
		assert.False(t, rel.sourceRO, "A is writable again, once B was read as read-only")
	})
}

// The control: Demote freezes the same flag, as step 1 of the documented
// failover, and that freeze must survive the DR side's ordinary
// resyncs until the promote runs. Nothing but a promote's marker, or the
// driver's own reconnect of a promoted copy, lifts it.
func TestResyncOfTheDRSideLeavesADemotedSourceFrozen(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.demote(t, s.handleA, classToB()))
	require.True(t, rel.sourceRO, "Demote froze A")
	require.Equal(t, 1, freezeMarkers(s.a), "Demote marks its freeze")
	require.True(t, isHeldFreezeMarker(s.a.snapshots[0], rel.id), "as held, the operator's")

	for i := 0; i < 3; i++ {
		_, err := s.resync(t, s.handleB(t), classToA(), false)
		require.NoError(t, err)
	}

	assert.True(t, rel.sourceRO, "a demote's freeze is held for the promote")
}

// blipArray is a fakeArray whose next n ListSourceStatus calls fail as a
// transport error would -- the array up, its API briefly not answering.
type blipArray struct {
	*fakeArray
	mu *sync.Mutex
	n  *int
}

func (b *blipArray) ListSourceStatus(ctx context.Context) ([]qumulo.RelationshipStatus, error) {
	b.mu.Lock()
	if *b.n > 0 {
		*b.n--
		b.mu.Unlock()
		return nil, errFakeUnreachable
	}
	b.mu.Unlock()
	return b.fakeArray.ListSourceStatus(ctx)
}

// withSourceBlip makes array A's source listing miss its next n answers.
func withSourceBlip(s *replScenario, n *int) {
	var mu sync.Mutex
	orig := s.driver.NewClient
	s.driver.NewClient = func(baseURL, token string) qumulo.ClientInterface {
		c := orig(baseURL, token)
		if strings.Contains(baseURL, "//"+arrayA+":") {
			return &blipArray{fakeArray: c.(*fakeArray), mu: &mu, n: n}
		}
		return c
	}
}

// A planned promote during which the
// source's API missed two listings -- one on the unforced call, one on the
// forced re-call FailedPrecondition makes csi-addons issue -- became a forced
// promote: no freeze, no final sync, the source's last writes discarded, two
// writable copies. The peer is now asked again before it is written off.
func TestPromoteAsksAnUnansweringPeerAgainBeforeForcing(t *testing.T) {
	t.Run("a two-call blip gets the planned promote", func(t *testing.T) {
		s := newReplScenario(t)
		s.server.peerRetryWait = 5 * time.Millisecond
		rel := s.protect(t, classToB())
		rel.write() // acknowledged on A, not yet replicated
		blips := 2
		withSourceBlip(s, &blips)

		require.NoError(t, s.promote(t, s.handleB(t), classToA(), false),
			"the retries outlast the blip, so no FailedPrecondition and no forced re-call")

		assert.Zero(t, blips)
		assert.True(t, rel.sourceRO, "A was frozen")
		assert.False(t, rel.targetRO, "B promoted")
		assert.Equal(t, rel.gen, rel.replicatedGen, "A's last writes reached B before the promote")
	})

	t.Run("a blip on the forced re-call still freezes and drains", func(t *testing.T) {
		s := newReplScenario(t)
		s.server.peerRetryWait = 5 * time.Millisecond
		rel := s.protect(t, classToB())
		rel.write()
		blips := 2
		withSourceBlip(s, &blips)

		require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))

		assert.True(t, rel.sourceRO, "force does not skip the freeze of a source that answers")
		assert.Equal(t, rel.gen, rel.replicatedGen)
	})

	t.Run("a source that stays down is still promoted by force", func(t *testing.T) {
		s := newReplScenario(t)
		s.server.peerRetryWait = 5 * time.Millisecond
		rel := s.protect(t, classToB())
		s.a.down = true

		requireCode(t, s.promote(t, s.handleB(t), classToA(), false), codes.FailedPrecondition)
		require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))

		assert.False(t, rel.targetRO, "B promoted at the retained recovery point")
	})
}

// The promote's budget ignored the caller's deadline: with a caller
// timeout shorter than the drain budget, the retries against a silent peer
// ran past it and the promote ended as the caller's DeadlineExceeded,
// instead of the FailedPrecondition that makes csi-addons re-call with force.
// The budget is now capped at the caller's deadline less a reply margin, and
// a retry is started only if its wait and its call both fit.
func TestPromoteRetryWindowHonoursTheCallersDeadline(t *testing.T) {
	promote := func(t *testing.T, s *replScenario, within time.Duration) (time.Time, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), within)
		defer cancel()
		deadline, _ := ctx.Deadline()
		_, err := s.server.PromoteVolume(ctx, &replication.PromoteVolumeRequest{
			VolumeId: s.handleB(t), Parameters: classToA(), Secrets: replSecrets(),
		})
		return deadline, err
	}

	t.Run("a peer that fails at once", func(t *testing.T) {
		s := newReplScenario(t)
		s.server.drainBudget = 10 * time.Second // far longer than the caller waits
		// Uncapped, both retries run: 150 ms + 300 ms of waiting, past the
		// caller's 400 ms. Capped at 350 ms, only the first fits.
		s.server.peerRetryWait = 150 * time.Millisecond
		s.server.peerCallTimeout = 100 * time.Millisecond
		s.server.replyMargin = 50 * time.Millisecond
		s.protect(t, classToB())
		s.a.down = true

		deadline, err := promote(t, s, 400*time.Millisecond)

		requireCode(t, err, codes.FailedPrecondition)
		assert.True(t, time.Now().Before(deadline), "answered before the caller's deadline")
	})

	t.Run("a peer that hangs for the whole call timeout", func(t *testing.T) {
		s := newReplScenario(t)
		s.server.drainBudget = 10 * time.Second
		s.server.peerRetryWait = 40 * time.Millisecond
		s.server.peerCallTimeout = 100 * time.Millisecond
		s.server.replyMargin = 50 * time.Millisecond
		s.protect(t, classToB())
		withHungSource(s, s.server.peerCallTimeout)

		// Each ask takes 100 ms. After the first retry (t = 240 ms) the
		// second's 80 ms wait still fits the 350 ms deadline, but its call
		// does not: counting only the wait would start it and run the
		// promote into the caller's 400 ms.
		deadline, err := promote(t, s, 400*time.Millisecond)

		requireCode(t, err, codes.FailedPrecondition)
		assert.True(t, time.Now().Before(deadline), "answered before the caller's deadline")
	})
}

// hungArray is an array whose source listing takes the whole client timeout
// to fail, as an unresponsive API does.
type hungArray struct {
	*fakeArray
	hang time.Duration
}

func (h *hungArray) ListSourceStatus(ctx context.Context) ([]qumulo.RelationshipStatus, error) {
	timer := time.NewTimer(h.hang)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, &url.Error{Op: "Get", URL: "https://" + h.addr, Err: ctx.Err()}
	case <-timer.C:
		return nil, &url.Error{Op: "Get", URL: "https://" + h.addr, Err: context.DeadlineExceeded}
	}
}

// withHungSource makes array A's source listing hang for d and then fail.
func withHungSource(s *replScenario, d time.Duration) {
	orig := s.driver.NewClient
	s.driver.NewClient = func(baseURL, token string) qumulo.ClientInterface {
		c := orig(baseURL, token)
		if strings.Contains(baseURL, "//"+arrayA+":") {
			return &hungArray{fakeArray: c.(*fakeArray), hang: d}
		}
		return c
	}
}

// enableFrom runs EnableVolumeReplication for the given handle and class.
func (s *replScenario) enableFrom(t *testing.T, handle string, params map[string]string) error {
	t.Helper()
	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId: handle, Parameters: params, Secrets: replSecrets(),
	})
	return err
}

// exportAt is the export whose FSPath is p on a, or nil.
func exportAt(a *fakeArray, p string) *qumulo.ExportInfo {
	for i := range a.exports {
		if a.exports[i].FSPath == p {
			return &a.exports[i]
		}
	}
	return nil
}

// Review of the fix: the original primary's export is created read-write
// by CreateVolume, and a resync that reverses the relationship makes that
// array a target. Its export must become read-only then -- a replica's export
// is read-only -- or the guard mistakes the stale ex-primary for a
// promoted copy, and a pod mounting the replica early writes into a directory
// that drops its appends.
func TestReprotectAfterFailoverFencesTheOldPrimarysExport(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleA, classToB()))
	_, err := s.resync(t, s.handleA, classToB(), false)
	require.NoError(t, err)
	require.Equal(t, s.b, rel.source, "reprotected: B is the source")
	require.Equal(t, s.a, rel.target, "and A its target")

	exp := exportAt(s.a, replVolumePath)
	require.NotNil(t, exp)
	assert.True(t, exp.ReadOnly, "A is a replica now, so its export is read-only")

	t.Run("Disable, then re-protect from B with consent, adopts A's stale copy", func(t *testing.T) {
		s.a.mkdir(replVolumePath + "/replicated-from-b") // a replica holds data
		require.NoError(t, s.disable(t, s.handleB(t), classToA()))
		require.Zero(t, s.fabric.relCount())

		require.NoError(t, s.enableFrom(t, s.handleB(t), classToA("allowNonEmptyTarget", "true")),
			"A is a stale replica, not a promoted copy")
		assert.Equal(t, 1, s.fabric.relCount())
	})
}

// The same after a whole round trip: failover to B, reprotect, failback to A.
// The first promote flipped B's export read-write; B's own resync, reversing
// the relationship back, fences it again, so a later Disable and re-protect
// with consent adopts B as the stale replica it is.
func TestFailbackFencesTheDRCopysExport(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	// Failover and reprotect.
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleA, classToB()))
	_, err := s.resync(t, s.handleA, classToB(), false)
	require.NoError(t, err)
	require.False(t, exportAt(s.b, replVolumePath).ReadOnly, "B serves the live copy")
	// Failback.
	require.NoError(t, s.promote(t, s.handleA, classToB(), false))
	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	_, err = s.resync(t, s.handleB(t), classToA(), false)
	require.NoError(t, err)
	require.Equal(t, s.a, rel.source, "A is the source again")

	assert.True(t, exportAt(s.b, replVolumePath).ReadOnly, "B is a replica again, so its export is read-only")
	assert.False(t, exportAt(s.a, replVolumePath).ReadOnly, "A serves the live copy")

	s.b.mkdir(replVolumePath + "/replicated-from-a")
	require.NoError(t, s.disable(t, s.handleA, classToB()))
	require.NoError(t, s.enable(t, classToB("allowNonEmptyTarget", "true")),
		"B is the stale replica, and re-protecting over it with consent is allowed")
}

// A replica whose target end is still listed, read-only, is a replica
// whatever its export says -- a relationship made before resync fenced
// exports, say -- and the export check must not call it promoted.
func TestALiveReadOnlyTargetEndOutranksAReadWriteExport(t *testing.T) {
	s := newReplScenario(t)
	s.b.mkdir(replVolumePath + "/data")
	s.b.exports = append(s.b.exports, qumulo.ExportInfo{ID: "7", ExportPath: replExportPath, FSPath: replVolumePath,
		Description: exportDescriptionFor(path.Base(replVolumePath))})
	elsewhere := s.fabric.addArray("array-c.example")
	elsewhere.mkdir(replVolumePath)
	s.fabric.seedRel(elsewhere, s.b, replVolumePath, replVolumePath, qumulo.RelationshipEstablished, nil)

	e, err := s.server.resolve(context.Background(), "test", s.handleA, "", classToB(), replSecrets(), false)
	require.NoError(t, err)
	why, err := s.server.peerHoldsWritableCopyAt(context.Background(), "test", e)
	require.NoError(t, err)
	assert.Empty(t, why)
}

// Review of the fix: Demote first (step 1 of the documented failover),
// then a promote whose drain stalls, then the DR side back to secondary. The
// drain found the source already frozen and must not have turned the
// demote's freeze into one the resync lifts: the source's own object says
// Secondary.
func TestResyncHoldsADemotesFreezeAfterAnAbandonedDrain(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.write() // written before the freeze, not yet on the target
	s.fabric.frozenPoint = true
	require.NoError(t, s.demote(t, s.handleA, classToB()))
	requireCode(t, s.promote(t, s.handleB(t), classToA(), false), codes.Aborted) // the drain stalls

	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	_, err := s.resync(t, s.handleB(t), classToA(), false)
	require.NoError(t, err)

	assert.True(t, rel.sourceRO, "the demote's freeze is held for the operator's promote")
}

// The same after an undone promote that completed: Demote, planned promote,
// then the DR side back to secondary and resynced by force. The reconnect
// branch holds the demote's freeze too, and promoting the source object lifts
// it (promoteFreezeRemedy).
func TestReconnectHoldsADemotesFreezeAndThePromoteLiftsIt(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.demote(t, s.handleA, classToB()))
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	_, err := s.resync(t, s.handleB(t), classToA(), true)
	require.NoError(t, err)
	require.True(t, rel.targetRO, "B reconnected as the read-only target")
	assert.True(t, rel.sourceRO, "A, demoted by the operator, stays frozen")

	require.NoError(t, s.promote(t, s.handleA, classToB(), false), "the remedy: A's own promote")
	assert.False(t, rel.sourceRO, "A is writable again")
	assert.True(t, rel.targetRO)
}

// flakyAuthArray fails the first *fails Authorize calls with err.
type flakyAuthArray struct {
	*fakeArray
	mu    *sync.Mutex
	fails *int
	err   error
}

func (f *flakyAuthArray) Authorize(ctx context.Context, id string, allowCreate, allowNonEmpty bool) (qumulo.RelationshipStatus, error) {
	f.mu.Lock()
	if *f.fails > 0 {
		*f.fails--
		f.mu.Unlock()
		return qumulo.RelationshipStatus{}, f.err
	}
	f.mu.Unlock()
	return f.fakeArray.Authorize(ctx, id, allowCreate, allowNonEmpty)
}

// A forced resync whose reverse is refused rebuilds the relationship
// from the peer: B creates B->A, then A authorizes it. When that Authorize
// failed once, the retry found the relationship already there -- A a target
// AWAITING_AUTHORIZATION -- and answered Ready=false for ever without
// authorizing it, so nothing replicated. The retry now authorizes it.
func TestResyncAuthorizesARebuiltRelationshipWhoseAuthorizeFailed(t *testing.T) {
	for _, tt := range []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{name: "not yet visible", err: fakeAPIError(404, "http_not_found_error", "not yet visible"), wantCode: codes.Aborted},
		{name: "a timeout", err: &url.Error{Op: "Post", URL: "https://" + arrayA, Err: context.DeadlineExceeded}, wantCode: codes.Unavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newReplScenario(t)
			rel := s.protect(t, classToB())
			require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
			rel.refuseReverse = true

			var mu sync.Mutex
			fails := 1
			orig := s.driver.NewClient
			s.driver.NewClient = func(baseURL, token string) qumulo.ClientInterface {
				c := orig(baseURL, token)
				if u, err := url.Parse(baseURL); err == nil && u.Hostname() == arrayA {
					return &flakyAuthArray{fakeArray: c.(*fakeArray), mu: &mu, fails: &fails, err: tt.err}
				}
				return c
			}

			_, err := s.resync(t, s.handleA, classToB(), true)
			requireCode(t, err, tt.wantCode)
			fresh := s.fabric.onlyRel()
			require.Equal(t, s.b, fresh.source, "the rebuild created B->A")
			require.Equal(t, qumulo.RelationshipAwaitingAuthorization, fresh.state)

			_, err = s.resync(t, s.handleA, classToB(), true)

			require.NoError(t, err)
			assert.Equal(t, qumulo.RelationshipEstablished, fresh.state, "the retry authorized it")
		})
	}
}

// The row above authorizes a local target whose relationship the
// peer sources. It also accepted a relationship the peer answered about and
// holds no end of -- one a third array created onto this volume -- so a
// forced resync let that array's data overwrite the local copy. The resync
// now refuses that shape, as PromoteVolume does, before any row acts on it.
func TestResyncRefusesARelationshipTheConfiguredPeerDoesNotHold(t *testing.T) {
	for _, state := range []string{qumulo.RelationshipAwaitingAuthorization, qumulo.RelationshipDisconnected} {
		t.Run(state, func(t *testing.T) {
			s := newReplScenario(t)
			c := s.fabric.addArray("c.example.com")
			c.mkdir(replVolumePath)
			foreign := s.fabric.seedRel(c, s.a, replVolumePath, replVolumePath, state, nil)

			_, err := s.resync(t, s.handleA, classToB(), true)

			requireCode(t, err, codes.InvalidArgument)
			assert.Contains(t, err.Error(), foreign.id)
			assert.Equal(t, state, foreign.state, "the third array's relationship is left as it was")
			assert.NotContains(t, s.a.calls, "Authorize")
			assert.NotContains(t, s.a.calls, "Reconnect")
		})
	}
}

// Under multitenancy a handle is <tenant NFS IP>#<management
// apiUrl>#<path>, and the NFS address serves no REST. The controller RPCs
// dial the handle's endpoint; the replication service ignored it, so every
// Info on such a volume looked for a token keyed by the NFS address, or
// dialled it: UNKNOWN, no sync time, no protection verdict. It now dials the
// endpoint the handle records.
func TestInfoDialsTheEndpointTheHandleRecords(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	nfs := s.fabric.addArray("10.9.9.9") // the tenant NFS address: answers no REST
	nfs.down = true
	handle := newVolumeID("10.9.9.9", "https://"+arrayA+":8000", replVolumePath).String()

	resp, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: handle, Secrets: replSecrets(),
	})

	require.NoError(t, err, "the Secret's token for the array is found by the endpoint's host")
	assert.NotNil(t, resp.GetLastSyncTime(), "A's ESTABLISHED relationship is read")
	assert.Empty(t, s.fabric.tokens["10.9.9.9"], "the NFS address is never dialled")
}

// The same handle through a class-driven RPC: a server that is a DNS name no
// longer needs the class's apiUrl when the handle records an endpoint, since
// that endpoint is what gets dialled, not the name.
func TestAClassDrivenRPCAcceptsANamedServerWhoseHandleRecordsAnEndpoint(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	handle := newVolumeID("nfs.tenant.example", "https://"+arrayA+":8000", replVolumePath).String()

	class := classToB()
	delete(class, "apiUrl") // the handle's endpoint is the only one there is

	_, err := s.server.EnableVolumeReplication(context.Background(), &replication.EnableVolumeReplicationRequest{
		VolumeId: handle, Parameters: class, Secrets: replSecrets(),
	})

	require.NoError(t, err, "Enable on the already-protected volume is a no-op, not a refusal")
}

// A read-write replica export this driver made -- an earlier build
// adopted one as it found it, or someone flipped it by hand -- is fenced on
// the next Enable, as before: it is this driver's own object.
func TestEnableFencesAReadWriteReplicaExportItMade(t *testing.T) {
	s := newReplScenario(t)
	require.NoError(t, s.enable(t, classToB()))
	require.Len(t, s.b.exports, 1)
	require.Equal(t, exportDescriptionFor(path.Base(replVolumePath)), s.b.exports[0].Description)
	s.b.exports[0].ReadOnly = false

	require.NoError(t, s.enable(t, classToB()))
	assert.True(t, s.b.exports[0].ReadOnly, "the driver's own replica export is fenced read-only")
}

// A planned promote whose RPC is cancelled right after the source is
// frozen -- a controller shutdown, the sidecar gone -- used to stop there,
// with the source frozen and the target still a read-only replica, until a
// retry on a new leader. The section from the freeze to the re-plumb now
// finishes.
func TestAPlannedPromoteCancelledAfterTheFreezeStillFinishes(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.a.cancelOn, s.a.cancel = "PatchSource", cancel // the freeze, on the source

	_, err := s.server.PromoteVolume(ctx, &replication.PromoteVolumeRequest{
		VolumeId: s.handleB(t), Parameters: classToA(), Secrets: replSecrets(),
	})

	require.NoError(t, err)
	require.Error(t, ctx.Err(), "the RPC's context was cancelled mid-promote")
	rel := s.fabric.onlyRel()
	assert.False(t, rel.targetRO, "the promoted copy is writable")
	require.Len(t, s.b.exports, 1)
	assert.False(t, s.b.exports[0].ReadOnly, "the promoted copy's export was re-plumbed read-write")
}

// A planned promote is counted from the freeze to the re-plumb, so a
// controller shutdown can wait for it rather than exit with both copies
// read-only; it is uncounted once it returns.
func TestAPlannedPromoteIsCountedWhileItRuns(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	seen := -1
	s.a.cancelOn, s.a.cancel = "PatchSource", func() { seen = s.server.PromotesInFlight() } // the freeze

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))

	assert.Equal(t, 1, seen, "counted while it runs")
	assert.Equal(t, 0, s.server.PromotesInFlight(), "and not after")
	assert.True(t, s.server.WaitForPromotes(time.Millisecond), "nothing left to wait for")
}

// While the source still reports snapshots queued to cross, the drain
// cannot end on that poll, so it lists neither array's snapshot table; it
// lists them once, when the queue is empty.
func TestTheDrainListsSnapshotsOnlyOnceTheQueueIsEmpty(t *testing.T) {
	listingsFor := func(queued int) int {
		s := newReplScenario(t)
		s.protect(t, classToB())
		s.server.drainBudget = 2 * time.Second
		s.fabric.onlyRel().queuedPolls = queued
		before := s.a.snapshotListings + s.b.snapshotListings
		require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
		return s.a.snapshotListings + s.b.snapshotListings - before
	}
	assert.Equal(t, listingsFor(0), listingsFor(4),
		"four polls with snapshots queued must add no snapshot listings")
}

// A planned promote listed the source's whole snapshot table four
// times: to find its freeze marker, for the drain's crossing check, and twice
// more to find the marker again to drop it. The marker it took or found is
// now read back by ID; the crossing check lists each array once.
func TestAPlannedPromoteListsEachSnapshotTableOnce(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.server.drainBudget = 2 * time.Second
	a0, b0 := s.a.snapshotListings, s.b.snapshotListings
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	assert.Equal(t, 2, s.a.snapshotListings-a0, "source: one to find no marker, one for the crossing check")
	assert.Equal(t, 1, s.b.snapshotListings-b0, "target: the crossing check")
	for _, snap := range s.a.snapshots {
		assert.NotContains(t, snap.Name, freezeMarkerSuffix, "the promote's marker was dropped")
	}
}

// A snapshot through a handle naming the read-only replica -- the old
// array, after a DNS failover with the PV not re-pointed -- is refused; the
// same snapshot on the source is taken as before.
func TestCreateSnapshotRefusesTheReplica(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	controller := s.controller(t)

	_, err := controller.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: s.handleB(t), Name: "snapshot-on-the-replica", Secrets: map[string]string{"token": "token-b"},
	})
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "read-only replica")

	// The source's Secret carries the peer's token too (the one symmetric
	// Secret), which the snapshot uses to confirm the peer's end is still
	// a read-only replica and not a promoted copy.
	_, err = controller.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: s.handleA, Name: "snapshot-on-the-source", Secrets: deleteSecrets(),
	})
	require.NoError(t, err)
}

// Each status poll logged "relationship assessed" at Info -- one line
// per VolumeReplication per requeue, saying the same thing. A relationship's
// assessment is logged at Info when its status changes, at Debug otherwise.
func TestAnAssessmentIsLoggedAtInfoOnlyWhenItsStatusChanges(t *testing.T) {
	var logged bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(restore) })

	r := NewReplicationServer(nil)
	for _, status := range []string{ReplicationHealthy, ReplicationHealthy, ReplicationHealthy,
		ReplicationDegraded, ReplicationDegraded, ReplicationHealthy} {
		r.logAssessment(context.Background(), "getvolumereplicationinfo", "pvc-a", "rel-1",
			ReplicationHealth{Status: status})
	}
	r.logAssessment(context.Background(), "getvolumereplicationinfo", "pvc-b", "rel-2",
		ReplicationHealth{Status: ReplicationHealthy})

	// rel-1: first HEALTHY, then DEGRADED, then HEALTHY again; rel-2: its first.
	assert.Equal(t, 4, strings.Count(logged.String(), "relationship assessed"), logged.String())
}

// Info answered last_sync_time alone -- csi-addons spec v0.2.0 had
// nothing else -- so the HEALTHY, DEGRADED or ERROR this driver works out on
// every poll reached only its own log, and csi-addons left the
// VolumeReplication's Replicating condition Unknown for ever. On the spec
// csi-addons v0.15.0 builds against, Info carries the status, its message,
// and the last job's duration and bytes.
func TestGetVolumeReplicationInfoReportsTheAssessment(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.lastJobDuration = 42 * time.Second
	rel.bytesTransferred = 1 << 20

	resp, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: s.handleA, Secrets: replSecrets(),
	})
	require.NoError(t, err)
	assert.Equal(t, replication.GetVolumeReplicationInfoResponse_HEALTHY, resp.GetStatus(), resp.GetStatusMessage())
	assert.NotEmpty(t, resp.GetStatusMessage())
	assert.Equal(t, 42*time.Second, resp.GetLastSyncDuration().AsDuration())
	assert.Equal(t, int64(1<<20), resp.GetLastSyncBytes())

	rel.lastJobError = "connection to target lost"
	resp, err = s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: s.handleA, Secrets: replSecrets(),
	})
	require.NoError(t, err)
	assert.Equal(t, replication.GetVolumeReplicationInfoResponse_DEGRADED, resp.GetStatus())
	assert.Contains(t, resp.GetStatusMessage(), "last job")
}

// Info on a policy-mode relationship reports its snapshot policy's
// schedule interval, read from the source array, so the stale alert can be
// scaled to the schedule; a relationship in another mode reports none.
func TestInfoReportsThePolicyScheduleInterval(t *testing.T) {
	daily := qumulo.Schedule{CreationSchedule: qumulo.CreationSchedule{
		Frequency: qumulo.ScheduleDailyOrWeekly, OnDays: []string{qumulo.DayEveryDay}, Hour: intPtr(2), Minute: intPtr(0),
	}}
	t.Run("policy mode", func(t *testing.T) {
		s := newReplScenario(t)
		s.a.policies = append(s.a.policies, fakePolicy{
			PolicyInfo: qumulo.PolicyInfo{ID: "7", Name: policyNameFor(replVolumeName), SourceFileID: s.fileIDA, Enabled: true},
			schedule:   daily,
		})
		rel := s.protect(t, classToB("replicationMode", "policy", "linkSnapshotPolicy", "true"))

		s.assess(t, s.handleA, nil)
		assert.Equal(t, 24*time.Hour, s.a.policyIntervals[rel.id])
	})
	t.Run("continuous mode", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB())

		s.assess(t, s.handleA, nil)
		interval, reported := s.a.policyIntervals[rel.id]
		assert.True(t, reported, "a mode with no policy interval says so, so a stale series goes")
		assert.Zero(t, interval)
	})
}

// Every Info poll listed both status tables on both arrays to find one
// relationship. A report now reads the relationship it found last by ID, on
// both arrays, and lists again only when that fails.
func TestRepeatedInfoReadsTheRelationshipByID(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())

	first := s.assess(t, s.handleA, nil)
	a0, b0 := s.a.statusListings, s.b.statusListings
	second := s.assess(t, s.handleA, nil)

	assert.Equal(t, a0, s.a.statusListings, "no listing on the local array")
	assert.Equal(t, b0, s.b.statusListings, "no listing on the peer")
	assert.Equal(t, rel.id, second.RelationshipID)
	assert.Equal(t, first.Status, second.Status)
	assert.Equal(t, first.Message, second.Message)
}

// The relationship it remembered is gone: the next report lists, and says so.
func TestInfoSeesARelationshipRemovedSinceItsLastPoll(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.assess(t, s.handleA, nil)

	require.NoError(t, s.disable(t, s.handleA, classToB()))

	_, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())
	requireCode(t, err, codes.FailedPrecondition)
}

// A report that found no relationship does not hide one Enable makes after it.
func TestInfoSeesARelationshipEnabledSinceItsLastPoll(t *testing.T) {
	s := newReplScenario(t)
	_, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())
	requireCode(t, err, codes.FailedPrecondition)

	rel := s.protect(t, classToB())

	assert.Equal(t, rel.id, s.assess(t, s.handleA, nil).RelationshipID)
}
