//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Defect tests for the replication / DR service (pkg/driver replication_*.go).
//
// Each test asserts the CORRECT behaviour for a reported defect, so it fails
// while the defect is present and passes once it is fixed. Every helper, type
// and variable here carries the dfx prefix so this file compiles next to the
// fix branch's own in-tree tests.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

	"qumulo-csi-driver/pkg/qumulo"
)

// ---------------------------------------------------------------------------
// dfxArray: one fake array with the fault and counting knobs these tests need.
// It embeds the fabric's *fakeArray and overrides only what it intercepts, so
// every other behaviour is the fabric's own.
// ---------------------------------------------------------------------------

type dfxArray struct {
	*fakeArray

	mu     sync.Mutex
	counts map[string]int

	// failSourceListings fails that many ListSourceStatus calls with a
	// transport error, then answers again.
	failSourceListings int
	// authorize404s answers that many Authorize calls with HTTP 404, as an
	// array does before a just-created relationship is visible.
	authorize404s int
	// reconnectRepliesWritable answers that many Reconnect calls with the
	// in-between state Core may report: the target not yet read-only,
	// although it settles read-only at once.
	reconnectRepliesWritable int
	// ctxAware fails every intercepted call whose context has ended, as the
	// real REST client does.
	ctxAware bool
	// cancelAfter ends the test RPC's context right after the named call
	// returns (a controller shutdown landing mid-RPC).
	cancelAfter string
	cancel      context.CancelFunc
	// realExportClient, when set, serves SetExportReadOnly through the real
	// REST client against an HTTP stand-in.
	realExportClient *qumulo.Client
	// exportByID makes the array answer GET /v3/nfs/exports/<id> (the
	// fix-side Client.GetExport) and report an endpoint, as the real client
	// does, so an implementation that reads a known export by ID can.
	exportByID bool
}

func dfxWrap(a *fakeArray) *dfxArray {
	return &dfxArray{fakeArray: a, counts: map[string]int{}}
}

// dfxInstall makes the scenario's driver hand out the wrappers for the given
// arrays (by address); any other array is handed out unwrapped.
func dfxInstall(s *replScenario, wrapped ...*dfxArray) {
	byAddr := map[string]*dfxArray{}
	for _, w := range wrapped {
		byAddr[w.addr] = w
	}
	orig := s.driver.NewClient
	s.driver.NewClient = func(baseURL, token string) qumulo.ClientInterface {
		c := orig(baseURL, token)
		if w, ok := byAddr[hostOfURL(baseURL)]; ok {
			return w
		}
		return c
	}
}

func (w *dfxArray) pre(ctx context.Context, name string) error {
	w.mu.Lock()
	w.counts[name]++
	w.mu.Unlock()
	if w.ctxAware {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("calling %s: %w", w.addr, err)
		}
	}
	return nil
}

func (w *dfxArray) post(name string) {
	if w.cancelAfter == name && w.cancel != nil {
		w.cancel()
	}
}

func (w *dfxArray) count(names ...string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, name := range names {
		n += w.counts[name]
	}
	return n
}

func (w *dfxArray) total() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, c := range w.counts {
		n += c
	}
	return n
}

func (w *dfxArray) reset() {
	w.mu.Lock()
	w.counts = map[string]int{}
	w.mu.Unlock()
}

func (w *dfxArray) snapshot() map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[string]int{}
	for k, v := range w.counts {
		out[k] = v
	}
	return out
}

func (w *dfxArray) Endpoint() string { return "https://" + w.addr + ":8000" }

func (w *dfxArray) GetExport(ctx context.Context, exportID string) (qumulo.ExportInfo, error) {
	if err := w.pre(ctx, "GetExport"); err != nil {
		return qumulo.ExportInfo{}, err
	}
	if err := w.unreachable(); err != nil {
		return qumulo.ExportInfo{}, err
	}
	if w.exportByID {
		for _, e := range w.exports {
			if e.ID == exportID {
				return e, nil
			}
		}
	}
	return qumulo.ExportInfo{}, fakeAPIError(http.StatusNotFound, "nfs_export_not_found_error", "no export "+exportID)
}

func (w *dfxArray) ListExports(ctx context.Context) ([]qumulo.ExportInfo, error) {
	if err := w.pre(ctx, "ListExports"); err != nil {
		return nil, err
	}
	defer w.post("ListExports")
	return w.fakeArray.ListExports(ctx)
}

func (w *dfxArray) ListSourceStatus(ctx context.Context) ([]qumulo.RelationshipStatus, error) {
	if err := w.pre(ctx, "ListSourceStatus"); err != nil {
		return nil, err
	}
	w.mu.Lock()
	if w.failSourceListings > 0 {
		w.failSourceListings--
		w.mu.Unlock()
		return nil, fmt.Errorf("calling %s: %w", w.addr, errFakeUnreachable)
	}
	w.mu.Unlock()
	defer w.post("ListSourceStatus")
	return w.fakeArray.ListSourceStatus(ctx)
}

func (w *dfxArray) ListTargetStatus(ctx context.Context) ([]qumulo.RelationshipStatus, error) {
	if err := w.pre(ctx, "ListTargetStatus"); err != nil {
		return nil, err
	}
	defer w.post("ListTargetStatus")
	return w.fakeArray.ListTargetStatus(ctx)
}

func (w *dfxArray) GetSourceStatus(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	if err := w.pre(ctx, "GetSourceStatus"); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	defer w.post("GetSourceStatus")
	return w.fakeArray.GetSourceStatus(ctx, id)
}

func (w *dfxArray) GetTargetStatus(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	if err := w.pre(ctx, "GetTargetStatus"); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	defer w.post("GetTargetStatus")
	return w.fakeArray.GetTargetStatus(ctx, id)
}

func (w *dfxArray) LookupFileByPath(ctx context.Context, p string) (qumulo.FileInfo, error) {
	if err := w.pre(ctx, "LookupFileByPath"); err != nil {
		return qumulo.FileInfo{}, err
	}
	return w.fakeArray.LookupFileByPath(ctx, p)
}

func (w *dfxArray) ResolveFileIDs(ctx context.Context, ids []string) (map[string]string, error) {
	if err := w.pre(ctx, "ResolveFileIDs"); err != nil {
		return nil, err
	}
	return w.fakeArray.ResolveFileIDs(ctx, ids)
}

func (w *dfxArray) ListDirectory(ctx context.Context, p, after string) ([]qumulo.DirEntry, error) {
	if err := w.pre(ctx, "ListDirectory"); err != nil {
		return nil, err
	}
	return w.fakeArray.ListDirectory(ctx, p, after)
}

func (w *dfxArray) GetQuota(ctx context.Context, fileID string) (uint64, error) {
	if err := w.pre(ctx, "GetQuota"); err != nil {
		return 0, err
	}
	return w.fakeArray.GetQuota(ctx, fileID)
}

func (w *dfxArray) CreateQuota(ctx context.Context, fileID string, limit uint64) error {
	if err := w.pre(ctx, "CreateQuota"); err != nil {
		return err
	}
	return w.fakeArray.CreateQuota(ctx, fileID, limit)
}

func (w *dfxArray) UpdateQuota(ctx context.Context, fileID string, limit uint64) error {
	if err := w.pre(ctx, "UpdateQuota"); err != nil {
		return err
	}
	return w.fakeArray.UpdateQuota(ctx, fileID, limit)
}

func (w *dfxArray) ListSnapshots(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
	if err := w.pre(ctx, "ListSnapshots"); err != nil {
		return nil, err
	}
	return w.fakeArray.ListSnapshots(ctx)
}

func (w *dfxArray) ListPolicies(ctx context.Context) ([]qumulo.PolicyInfo, error) {
	if err := w.pre(ctx, "ListPolicies"); err != nil {
		return nil, err
	}
	return w.fakeArray.ListPolicies(ctx)
}

func (w *dfxArray) ClusterTime(ctx context.Context) (t2 time.Time, err error) {
	if err := w.pre(ctx, "ClusterTime"); err != nil {
		return time.Time{}, err
	}
	return w.fakeArray.ClusterTime(ctx)
}

func (w *dfxArray) CreateExpiringSnapshot(ctx context.Context, fileID, suffix string, exp time.Time) (qumulo.SnapshotInfo, error) {
	if err := w.pre(ctx, "CreateExpiringSnapshot"); err != nil {
		return qumulo.SnapshotInfo{}, err
	}
	defer w.post("CreateExpiringSnapshot")
	return w.fakeArray.CreateExpiringSnapshot(ctx, fileID, suffix, exp)
}

func (w *dfxArray) SnapshotDiffEmpty(ctx context.Context, newer, older string) (bool, error) {
	if err := w.pre(ctx, "SnapshotDiffEmpty"); err != nil {
		return false, err
	}
	return w.fakeArray.SnapshotDiffEmpty(ctx, newer, older)
}

func (w *dfxArray) DeleteSnapshot(ctx context.Context, id string) error {
	if err := w.pre(ctx, "DeleteSnapshot"); err != nil {
		return err
	}
	return w.fakeArray.DeleteSnapshot(ctx, id)
}

func (w *dfxArray) PatchSource(ctx context.Context, id string, patch qumulo.SourcePatch) (qumulo.Relationship, error) {
	if err := w.pre(ctx, "PatchSource"); err != nil {
		return qumulo.Relationship{}, err
	}
	defer w.post("PatchSource")
	return w.fakeArray.PatchSource(ctx, id, patch)
}

func (w *dfxArray) MakeTargetWritable(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	if err := w.pre(ctx, "MakeTargetWritable"); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	defer w.post("MakeTargetWritable")
	return w.fakeArray.MakeTargetWritable(ctx, id)
}

func (w *dfxArray) Reconnect(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	if err := w.pre(ctx, "Reconnect"); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	reply, err := w.fakeArray.Reconnect(ctx, id)
	w.mu.Lock()
	if err == nil && w.reconnectRepliesWritable > 0 {
		w.reconnectRepliesWritable--
		reply.TargetRootReadOnly = false
		reply.State = qumulo.RelationshipReconnecting
	}
	w.mu.Unlock()
	return reply, err
}

func (w *dfxArray) Authorize(ctx context.Context, id string, allowCreate, allowNonEmpty bool) (qumulo.RelationshipStatus, error) {
	if err := w.pre(ctx, "Authorize"); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	w.mu.Lock()
	if w.authorize404s > 0 {
		w.authorize404s--
		w.mu.Unlock()
		w.record("Authorize(404)")
		return qumulo.RelationshipStatus{}, fakeAPIError(http.StatusNotFound, "http_not_found_error", "not yet visible")
	}
	w.mu.Unlock()
	return w.fakeArray.Authorize(ctx, id, allowCreate, allowNonEmpty)
}

func (w *dfxArray) SetExportReadOnly(ctx context.Context, exportID string, readOnly bool) error {
	if err := w.pre(ctx, "SetExportReadOnly"); err != nil {
		return err
	}
	defer w.post("SetExportReadOnly")
	if w.realExportClient != nil {
		w.record("SetExportReadOnly")
		return w.realExportClient.SetExportReadOnly(ctx, exportID, readOnly)
	}
	return w.fakeArray.SetExportReadOnly(ctx, exportID, readOnly)
}

func (w *dfxArray) CreateExport(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
	if err := w.pre(ctx, "CreateExport"); err != nil {
		return qumulo.ExportInfo{}, err
	}
	return w.fakeArray.CreateExport(ctx, spec)
}

func (w *dfxArray) DeleteExport(ctx context.Context, id string) error {
	if err := w.pre(ctx, "DeleteExport"); err != nil {
		return err
	}
	return w.fakeArray.DeleteExport(ctx, id)
}

// dfxSetDescription sets an export's description where this build's
// ExportInfo has the field (it does not on every tree), so the test compiles
// on both and says what the array would report.
func dfxSetDescription(e *qumulo.ExportInfo, d string) {
	if f := reflect.ValueOf(e).Elem().FieldByName("Description"); f.IsValid() && f.CanSet() && f.Kind() == reflect.String {
		f.SetString(d)
	}
}

// dfxCalled reports whether verb is among calls.
func dfxCalled(calls []string, verb string) bool {
	for _, c := range calls {
		if c == verb {
			return true
		}
	}
	return false
}

// dfxGroupMember is a member handle on B (the DR array) for the shared class.
func dfxGroupMemberOnB(name string) string {
	return newVolumeID(arrayB, "", path.Join(sharedBasePath, name)).String()
}

// ===========================================================================
// Enable on the old primary must refuse while the peer holds the
// promoted, writable copy -- even once every relationship end is gone.
// ===========================================================================

// TestDefect_EnableOnOldPrimaryRefusesAfterDRSideDisable.
// Correct: after a planned failover and a Disable on the DR side (which
// removes both ends), Enable on the old primary is refused and B's live copy
// replication_server.go peerHoldsWritableCopyAt.
func TestDefect_EnableOnOldPrimaryRefusesAfterDRSideDisable(t *testing.T) {
	s := newReplScenario(t)
	class := classToB("allowNonEmptyTarget", "true")
	s.protect(t, class)

	require.NoError(t, s.promote(t, s.handleB(t), classToA("allowNonEmptyTarget", "true"), false))
	s.b.mkdir(replVolumePath + "/written-on-dr-after-failover")
	require.NoError(t, s.disable(t, s.handleB(t), classToA("allowNonEmptyTarget", "true")))
	require.Zero(t, s.fabric.relCount(), "Disable on the promoted target removed both ends")
	before := len(s.b.calls)

	err := s.enable(t, class)

	requireCode(t, err, codes.FailedPrecondition)
	assert.Zero(t, s.fabric.relCount(), "nothing was created toward B's live copy")
	assert.False(t, dfxCalled(s.b.calls[before:], "Authorize"), "B's live copy was authorized as a replica target")
}

// TestDefect_EnableOnReturningPrimaryRefusesAfterOutageDisable.
// Correct: after an unplanned failover whose DR-side VolumeReplication was
// deleted during the outage, Enable on the returning primary does not rebuild
func TestDefect_EnableOnReturningPrimaryRefusesAfterOutageDisable(t *testing.T) {
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
	corpse.sourceReportsState = ""

	err := s.enable(t, class)

	require.Error(t, err, "Enable rebuilt A->B over the promoted copy that holds every outage write")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "got %v", err)
	for _, r := range s.fabric.rels {
		if r.id != corpse.id && r.state != qumulo.RelationshipEnded {
			assert.False(t, r.source == s.a && r.target == s.b && r.targetRO,
				"B's outage writes became a read-only replica slot for A's tree")
		}
	}
}

// ===========================================================================
// An abandoned DR-side promote must not leave the source frozen.
// ===========================================================================

// TestDefect_AbandonedPromoteDoesNotLeaveTheSourceFrozen.
// Correct: once the DR side is back as a read-only ESTABLISHED target, the
func TestDefect_AbandonedPromoteDoesNotLeaveTheSourceFrozen(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	s.fabric.frozenPoint = true // the target never catches up within the budget
	rel.write()
	err := s.promote(t, s.handleB(t), classToA(), false)
	requireCode(t, err, codes.Aborted)
	require.True(t, rel.sourceRO, "A frozen by the drain")

	// The operator abandons: the DR side back to secondary (Demote, Resync).
	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	_, err = s.resync(t, s.handleB(t), classToA(), false)
	require.NoError(t, err)

	assert.True(t, rel.targetRO, "B is a read-only target")
	assert.False(t, rel.sourceRO, "A is still frozen: no copy anywhere is writable")
}

// ===========================================================================
// After a DNS failover, an expand of the unchanged DNS-named claim
// must reach the live copy, or be refused naming the promoted side.
// ===========================================================================

// TestDefect_ExpandOfADNSNamedClaimAfterFailoverReachesTheLiveCopy.
// Correct: the live copy on B ends with the new size, or the expand is refused
func TestDefect_ExpandOfADNSNamedClaimAfterFailoverReachesTheLiveCopy(t *testing.T) {
	s := newReplScenario(t)
	dns := s.fabric.addArray("dr-name.example") // what the DNS name answers for REST: nothing
	dns.down = true
	s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	_, err := s.resync(t, s.handleA, classToB(), true)
	require.NoError(t, err)
	rel := s.fabric.onlyRel()
	require.Equal(t, s.b, rel.source, "failed over and reversed: B is the live source")
	liveID := s.b.dirs[replVolumePath]

	// The unchanged claim: DNS name as server, the class's fixed apiUrl (A).
	handle := newVolumeID("dr-name.example", "https://"+arrayA+":8000", replVolumePath).String()
	err = s.expand(t, handle, expandTo,
		deleteSecrets(paramTargetServer, arrayB, paramTargetAPIURL, "https://"+arrayB+":8000"))

	if err != nil {
		t.Logf("expand refused: %v", err)
		assert.Equal(t, replQuota, s.b.quotas[liveID], "a refused expand changed the live copy")
		assert.Contains(t, err.Error(), arrayB, "a refusal must name the promoted side")
		return
	}
	assert.Equal(t, expandTo, s.b.quotas[liveID],
		"the claim reads the new size but the live copy on B was left at the old one")
}

// TestDefect_UnplannedShape_TheStaleSourceIsRefused: the
// unplanned-failover shape. B was promoted while A was down; A is back as the
// DISCONNECTED, writable source of the relationship, behind the live copy, and
// nobody has demoted it yet. Correct: an expand, a snapshot and a delete of
// the unchanged DNS-named claim, whose handle still names A, are refused
// naming B, and nothing on either array changes. Before: the expand raised
// A's quota alone and answered success, the snapshot captured A's stale tree
// as ReadyToUse, and the delete took A's directory and ended the
// relationship from the stale side.
func TestDefect_UnplannedShape_TheStaleSourceIsRefused(t *testing.T) {
	s := newReplScenario(t)
	dns := s.fabric.addArray("dr-name.example")
	dns.down = true
	rel := s.protect(t, classToB())
	// Promoted on B with A unreachable (the array stays as it was, so the
	// relationship is not reversed), then A comes back.
	s.a.down = true
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), true))
	s.a.down = false
	require.Equal(t, s.a, rel.source, "not reversed: A is still the source end")
	require.False(t, rel.targetRO, "B holds the promoted, writable copy")
	liveID := s.b.dirs[replVolumePath]
	s.b.quotas[liveID] = 3 << 30 // grew during the DR period
	taken := 0
	s.a.CreateSnapshotFunc = func(context.Context, string, string) (qumulo.SnapshotInfo, error) {
		taken++
		return qumulo.SnapshotInfo{}, nil
	}

	handle := newVolumeID("dr-name.example", "https://"+arrayA+":8000", replVolumePath).String()
	controller := s.controller(t)

	err := s.expand(t, handle, expandTo, deleteSecrets())
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), arrayB, "the refusal names the promoted side")
	assert.Equal(t, replQuota, s.a.quotas[s.fileIDA], "the stale source's quota is untouched")
	assert.Equal(t, uint64(3<<30), s.b.quotas[liveID], "the live copy's quota is untouched")

	_, err = controller.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: handle, Name: "snapshot-after-unplanned-failover", Secrets: deleteSecrets(),
	})
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), arrayB)
	assert.Zero(t, taken, "no snapshot was taken of the stale source")

	_, err = controller.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: handle, Secrets: deleteSecrets(paramReplicationDeletePolicy, replicationDeletePolicyDisableOrphan),
	})
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), arrayB)
	_, stillThere := s.a.dirs[replVolumePath]
	assert.True(t, stillThere, "the stale source's directory is still there")
	assert.Equal(t, 1, s.fabric.relCount(), "the relationship was not ended")

	// Demoting the stale side makes it the replica, and the replica rules
	// take over: expand grows both copies, snapshot and delete stay refused.
	require.NoError(t, s.demote(t, s.handleA, classToB()))
	_, err = s.resync(t, s.handleA, classToB(), true)
	require.NoError(t, err)
	require.True(t, s.fabric.onlyRel().targetRO, "A is B's read-only replica now")
	require.NoError(t, s.expand(t, handle, expandTo,
		deleteSecrets(paramTargetServer, arrayB, paramTargetAPIURL, "https://"+arrayB+":8000")))
	assert.Equal(t, expandTo, s.a.quotas[s.fileIDA], "the replica on A has the claim's size")
	assert.Equal(t, uint64(3<<30), s.b.quotas[liveID], "the live copy, already larger, is never lowered")
}

// ===========================================================================
// Replication reads go to the handle's API endpoint.
// ===========================================================================

// TestDefect_InfoDialsTheHandlesAPIEndpoint.
// Correct: GetVolumeReplicationInfo for a handle <nfs address>#<apiUrl>#<path>
// reads from the apiUrl's array, never the NFS address. See
// replication_server.go resolve().
func TestDefect_InfoDialsTheHandlesAPIEndpoint(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	nfs := s.fabric.addArray("10.9.9.9") // a tenant NFS address: answers no REST
	nfs.down = true
	handle := newVolumeID("10.9.9.9", "https://"+arrayA+":8000", replVolumePath).String()

	resp, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: handle, Secrets: replSecrets(),
	})

	require.NoError(t, err)
	assert.NotNil(t, resp.GetLastSyncTime(), "A's relationship is ESTABLISHED, so Info has a sync time")
	assert.Empty(t, s.fabric.tokens["10.9.9.9"], "the NFS address was dialled instead of the handle's endpoint")
}

// ===========================================================================
// A group member resolves to its group's relationship.
// ===========================================================================

// TestDefect_InfoOnTheDataSourceCoversEveryGroupMember.
// Correct: one Info call, for the group's dataSource, leaves every member of
//
// This test used to call Info for each member in turn, and so passed
// while the defect was live: csi-addons never makes that call. A
// group has one VolumeReplication, the sidecar reports on its dataSource
// alone, and the other members had no series at all -- which the unprotected
// alert, `csi_qumulo_volume_replication_protected == 0`, cannot see. The
// sequence below is the one that actually happens in a cluster.
func TestDefect_InfoOnTheDataSourceCoversEveryGroupMember(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	members := s.allMemberHandles()
	s.withPVs(members...)
	dataSource := s.memberHandle("pvc-tenant-a")

	resp, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: dataSource, Secrets: replSecrets(),
	})

	require.NoError(t, err, "the group's dataSource: status unknown")
	assert.NotNil(t, resp.GetLastSyncTime(), "the group's dataSource has no lastSyncTime")
	verdicts := s.verdicts()
	for _, member := range members {
		require.Contains(t, verdicts, member, "member %s has no protected series at all", member)
		assert.True(t, verdicts[member], "member %s gauge reads 0", member)
	}
}

// ===========================================================================
// A member created after protection gets its DR quota.
// ===========================================================================

// TestDefect_LateGroupMemberGetsItsReplicaQuota.
// Correct: once a late member's replica directory exists on B, the driver's
func TestDefect_LateGroupMemberGetsItsReplicaQuota(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))

	late := path.Join(sharedBasePath, "pvc-tenant-late")
	s.a.quotas[s.a.mkdir(late)] = 3 << 30
	replicaID := s.b.mkdir(late) // replicated: contents only, no quota
	require.NotContains(t, s.b.quotas, replicaID)

	_, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
		VolumeId: s.memberHandle("pvc-tenant-a"), Secrets: replSecrets(),
	})
	require.NoError(t, err)

	assert.EqualValues(t, 3<<30, s.b.quotas[replicaID], "the late member's replica has no quota on the DR array")
}

// ===========================================================================
// A group member's DR copy is refused like a pair's replica.
// ===========================================================================

// TestDefect_DeleteOfAGroupMembersDRCopyIsRefused.
// Correct: DeleteVolume on a member's mirror PV on B answers FailedPrecondition
func TestDefect_DeleteOfAGroupMembersDRCopyIsRefused(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	memberOnB := path.Join(sharedBasePath, "pvc-tenant-a")
	require.Contains(t, s.b.dirs, memberOnB)
	before := len(s.b.calls)

	_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: dfxGroupMemberOnB("pvc-tenant-a"),
		Secrets:  deleteSecrets(paramReplicationDeletePolicy, replicationDeletePolicyDisable),
	})

	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, s.b.dirs, memberOnB, "the DR copy was tree-deleted")
	for _, c := range s.b.calls[before:] {
		assert.False(t, strings.HasPrefix(c, "tree-delete"), "a tree-delete was started: %s", c)
	}
	assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
}

// ===========================================================================
// A two-call API blip must not turn a planned promote into a
// forced one.
// ===========================================================================

// TestDefect_TwoCallBlipStillGivesAPlannedPromoteWithAFreeze.
// Correct: a peer that misses two listings and then answers produces a planned
// promote: the source is frozen and the target holds its last writes. The test
// plays csi-addons: on FailedPrecondition it re-calls with force at once.
func TestDefect_TwoCallBlipStillGivesAPlannedPromoteWithAFreeze(t *testing.T) {
	s := newReplScenario(t)
	// The scenario's 200 ms budget scales the controller's waits down; the
	// re-probe's wait is scaled the same way, or no re-probe fits at all.
	s.server.peerRetryWait = 5 * time.Millisecond
	rel := s.protect(t, classToB())
	rel.write() // acknowledged on A, not yet replicated
	wa := dfxWrap(s.a)
	wa.failSourceListings = 2
	dfxInstall(s, wa)

	var err error
	for attempt := 0; attempt < 6; attempt++ {
		err = s.promote(t, s.handleB(t), classToA(), false)
		if status.Code(err) == codes.FailedPrecondition {
			t.Logf("unforced promote answered %v; csi-addons re-calls with force", err)
			err = s.promote(t, s.handleB(t), classToA(), true)
		}
		if err == nil {
			break
		}
		t.Logf("promote attempt %d: %v", attempt, err)
	}
	require.NoError(t, err)

	assert.False(t, rel.targetRO, "B promoted")
	assert.True(t, rel.sourceRO, "A was never frozen: two writable copies after a two-call blip")
	assert.Equal(t, rel.gen, rel.replicatedGen, "A's last acknowledged writes never reached B")
}

// TestDefect_BlipWithTheRealBreakerStillGivesAPlannedPromote.
// Correct: the live sequence, with the process's real circuit
// breaker and the real REST client in front of the source. Other
// volumes' calls open A's breaker when its API goes silent, calls in flight
// from before the cut fail after it ends, and the cut ends between the
// promote's second and third ask. The planned promote freezes and drains A;
// the FailedPrecondition that makes csi-addons force is never answered, and
// the forced path that skips the final sync never runs. Before the fix every
// ask was answered by the breaker inside the process.
func TestDefect_BlipWithTheRealBreakerStillGivesAPlannedPromote(t *testing.T) {
	s, rel, w := breakerScenario(t)
	w.startLate(2) // other volumes' calls, in flight when the cut begins
	w.cut.Store(true)
	w.openBreaker()
	w.cutAsks.Store(2)
	// Never let the cut outlast the promote's deadline, fixed or not.
	time.AfterFunc(time.Second, func() { w.cut.Store(false) })

	err, forced := s.promoteAsCSIAddons(t)

	require.NoError(t, err)
	assert.False(t, forced, "the planned promote answered FailedPrecondition and csi-addons forced it")
	assert.False(t, rel.targetRO, "B promoted")
	assert.True(t, rel.sourceRO, "A was never frozen: the forced path skipped the freeze")
	assert.Equal(t, rel.gen, rel.replicatedGen, "A's last acknowledged writes never reached B")
	assert.False(t, w.failsFast(), "A answered, yet the breaker is open again")
}

// TestDefect_ProvisioningStillFailsFastWhileAPromoteAsksADeadSource:
// Correct: a promote's asks reach a dead source, and
// ordinary calls to it are still failed fast by the breaker throughout, so a
// failover does not put provisioning back behind a dead array.
func TestDefect_ProvisioningStillFailsFastWhileAPromoteAsksADeadSource(t *testing.T) {
	s, rel, w := breakerScenario(t)
	w.cut.Store(true)
	w.openBreaker()

	err, forced := s.promoteAsCSIAddons(t)

	require.NoError(t, err)
	assert.True(t, forced, "a source that stays silent is promoted by force")
	assert.Positive(t, w.asks.Load(), "no ask reached the source: the breaker decided the promote")
	assert.False(t, rel.targetRO, "B promoted")
	assert.True(t, w.failsFast(), "ordinary calls to the dead source are no longer failed fast")
}

// ===========================================================================
// A malformed reply is reported as an
// unreachable array.
// ===========================================================================

// TestDefect_AMalformedSourceListingIsNotAForcedPromote.
// Correct: a source whose status listing comes back 200 with a body the
// client cannot decode answered; a planned promote is refused Internal, not
// FailedPrecondition, so csi-addons never forces it, and B stays read-only.
// Before the fix the reply read as "unreachable", was retried as silence,
// and the forced re-call promoted with no freeze and no final sync.
func TestDefect_AMalformedSourceListingIsNotAForcedPromote(t *testing.T) {
	s, rel, w := breakerScenario(t)
	w.malformed.Store(true)

	err, forced := s.promoteAsCSIAddons(t)

	require.Error(t, err)
	assert.False(t, forced, "the malformed reply was taken for a silent source and the promote forced")
	assert.NotEqual(t, codes.Unavailable, status.Code(err), "got %v", err)
	assert.NotContains(t, err.Error(), "unreachable")
	assert.True(t, rel.targetRO, "B was made writable on a malformed reply")
	assert.False(t, rel.sourceRO, "A was frozen with nothing promoted")
}

// TestDefect_ANullExportRestrictionIsNotReportedUnreachable.
// Correct: the live path -- a resync fencing A's export, which the array
// answers 200 with a null restriction -- is refused as a reply the driver
// cannot use, not as "array unreachable" with code Unavailable.
func TestDefect_ANullExportRestrictionIsNotReportedUnreachable(t *testing.T) {
	arrayHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","export_path":"` + replExportPath + `","fs_path":"` + replVolumePath + `",` +
			`"restrictions":[null]}`))
	}))
	defer arrayHTTP.Close()

	s := newReplScenario(t)
	s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleA, classToB()))
	wa := dfxWrap(s.a)
	wa.realExportClient = qumulo.New(arrayHTTP.URL, "t")
	dfxInstall(s, wa)

	_, err := s.resync(t, s.handleA, classToB(), false)

	require.Error(t, err)
	require.Positive(t, wa.count("SetExportReadOnly"), "the resync never fenced A's export")
	assert.NotEqual(t, codes.Unavailable, status.Code(err), "got %v", err)
	assert.NotContains(t, err.Error(), "unreachable")
	assert.Contains(t, err.Error(), "restriction 0 is null")
}

// ===========================================================================
// A failed Authorize in the rebuild-from-peer path is retried.
// ===========================================================================

// TestDefect_RebuildFromPeerAuthorizesOnRetryAfterA404.
// Correct: the first Authorize answers 404; a later resync authorizes the
func TestDefect_RebuildFromPeerAuthorizesOnRetryAfterA404(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	rel.refuseReverse = true
	wa := dfxWrap(s.a)
	wa.authorize404s = 1
	dfxInstall(s, wa)

	_, err := s.resync(t, s.handleA, classToB(), true)
	t.Logf("first forced resync: %v", err)

	ready := false
	for i := 0; i < 5 && !ready; i++ {
		ready, err = s.resync(t, s.handleA, classToB(), true)
		t.Logf("resync %d: ready=%v err=%v", i, ready, err)
	}
	fresh := s.fabric.onlyRel()
	assert.Equal(t, s.b, fresh.source, "rebuilt from the peer")
	assert.Equal(t, qumulo.RelationshipEstablished, fresh.state, "still unauthorized after 5 retries")
	assert.True(t, ready, "Ready never became true")
}

// ===========================================================================
// The old primary never changes the promoted copy's quota.
// ===========================================================================

func dfxPromotedPeer(t *testing.T, live uint64) (*replScenario, string) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.targetRO = false
	rel.state = qumulo.RelationshipDisconnected // B promoted and live
	liveID := s.b.dirs[replVolumePath]
	s.b.quotas[liveID] = live
	return s, liveID
}

// TestDefect_ExpandOfOldPrimaryLeavesThePromotedCopysQuota.
// Correct: expanding the old primary's claim to 2 GiB leaves the live copy's
func TestDefect_ExpandOfOldPrimaryLeavesThePromotedCopysQuota(t *testing.T) {
	s, liveID := dfxPromotedPeer(t, 5<<30)
	_ = s.expand(t, s.handleA, 2<<30, deleteSecrets())
	assert.Equal(t, uint64(5<<30), s.b.quotas[liveID], "the live copy's quota was lowered")
}

// TestDefect_EnableOnOldPrimaryLeavesThePromotedCopysQuota.
// Correct: Enable on the old primary while the peer is promoted at 5 GiB
func TestDefect_EnableOnOldPrimaryLeavesThePromotedCopysQuota(t *testing.T) {
	s, liveID := dfxPromotedPeer(t, 5<<30)
	_ = s.enable(t, classToB())
	assert.Equal(t, uint64(5<<30), s.b.quotas[liveID], "the live copy's 5 GiB quota was reset to A's 1 GiB")
	_ = s.promote(t, s.handleA, classToB(), false)
	assert.Equal(t, uint64(5<<30), s.b.quotas[liveID])
}

// ===========================================================================
// A plain disable and re-enable of a never-failed-over group works.
// ===========================================================================

// TestDefect_GroupReEnableWithOperatorCreatedCoveringExport.
// Correct: a group that was never failed over and that the driver agreed to
// protect is re-protected by a plain Disable and re-Enable. When the
// operator-made covering export is read-write at protect time the driver may
// either fence it or refuse the protect up front naming the export (the
// settled decision); in the second case the test follows that
// regression-tests/open_group_reenable_test.go.
func TestDefect_GroupReEnableWithOperatorCreatedCoveringExport(t *testing.T) {
	for _, rw := range []bool{false, true} {
		name := "DR covering export read-only"
		if rw {
			name = "DR covering export read-write (operator-created)"
		}
		t.Run(name, func(t *testing.T) {
			s := newSharedScenario(t)
			s.withCoveringExportOnB(!rw)
			class := groupClass("allowNonEmptyTarget", "true")

			err := s.enableGroup(t, class, "")
			if rw && status.Code(err) == codes.FailedPrecondition && strings.Contains(err.Error(), "read-only") {
				t.Logf("protect refused up front with the remedy: %v", err)
				s.b.exports[0].ReadOnly = true // the operator follows the remedy
				err = s.enableGroup(t, class, "")
			}
			require.NoError(t, err)

			_, err = s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
				VolumeId: s.memberHandle("pvc-tenant-a"), Parameters: class, Secrets: replSecrets(),
			})
			require.NoError(t, err)

			err = s.enableGroup(t, class, "")
			require.NoError(t, err, "re-enable after a plain disable left the group unprotected")
			assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
		})
	}
}

// ===========================================================================
// Info on a stale old primary writes nothing onto the live copy.
// ===========================================================================

// TestDefect_InfoOnStalePrimaryLeavesPromotedCopysQuotaAlone.
// Correct: Info on the old primary after a group failover writes no quota on
// regression-tests/open_stale_primary_quota_test.go.
func TestDefect_InfoOnStalePrimaryLeavesPromotedCopysQuotaAlone(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	_, err := s.server.PromoteVolume(context.Background(), &replication.PromoteVolumeRequest{
		VolumeId: dfxGroupMemberOnB("pvc-tenant-a"), Parameters: classToA(), Secrets: replSecrets(),
		ReplicationSource: groupSource(),
	})
	require.NoError(t, err)
	rel := s.fabric.onlyRel()
	t.Logf("after promote: state=%s targetRO=%v", rel.state, rel.targetRO)

	lp := path.Join(sharedBasePath, "pvc-tenant-late")
	s.a.quotas[s.a.mkdir(lp)] = 7 << 30
	bid := s.b.mkdir(lp) // exists on the live copy, with 1 GiB there
	s.b.quotas[bid] = 1 << 30
	before := len(s.b.calls)

	_, err = s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())
	t.Logf("info err=%v; B calls during Info on stale A: %v", err, s.b.calls[before:])

	assert.False(t, dfxCalled(s.b.calls[before:], "UpdateQuota"), "Info on the stale old primary wrote a quota on the promoted live copy")
	assert.False(t, dfxCalled(s.b.calls[before:], "CreateQuota"), "Info on the stale old primary created a quota on the promoted live copy")
	assert.Equal(t, uint64(1<<30), s.b.quotas[bid], "the promoted copy's quota must be left alone")
}

// ===========================================================================
// A resync never authorizes a relationship from an array the class
// never named.
// ===========================================================================

// TestDefect_ForcedResyncDoesNotAuthorizeAThirdArraysRelationship.
// Correct: a forced resync on a local target AWAITING_AUTHORIZATION whose
// source is not the class's peer (the peer was asked and holds no end)
// authorizes nothing; the local copy is not handed to that array.
func TestDefect_ForcedResyncDoesNotAuthorizeAThirdArraysRelationship(t *testing.T) {
	s := newReplScenario(t)
	c := s.fabric.addArray("c.example.com")
	c.mkdir(replVolumePath)
	foreign := s.fabric.seedRel(c, s.a, replVolumePath, replVolumePath, qumulo.RelationshipAwaitingAuthorization,
		func(r *fakeRel) { r.targetRO = false })
	before := len(s.a.calls)

	ready, err := s.resync(t, s.handleA, classToB(), true)
	t.Logf("resync: ready=%v err=%v", ready, err)

	assert.False(t, dfxCalled(s.a.calls[before:], "Authorize"), "A authorized a relationship from array C, which the class never named")
	assert.Equal(t, qumulo.RelationshipAwaitingAuthorization, foreign.state, "C's relationship onto A was established")
	assert.False(t, foreign.targetRO, "A's volume became C's read-only replica")
}

// ===========================================================================
// The delete guard protects a replica below the deleted path.
// ===========================================================================

// TestDefect_DeleteVolumeRefusesADirectoryHoldingAReplica.
// Correct: DeleteVolume refuses (FailedPrecondition) a directory that has a
// replication target below it, as it refuses the target itself, and starts no
func TestDefect_DeleteVolumeRefusesADirectoryHoldingAReplica(t *testing.T) {
	var treeDeleted []string
	client := &MockClient{
		LookupFileByPathFunc: func(ctx context.Context, p string) (qumulo.FileInfo, error) {
			ids := map[string]string{"/csi-volumes/group-a": "20", "/csi-volumes/group-a/pvc-replica": "21"}
			if id, ok := ids[p]; ok {
				return qumulo.FileInfo{ID: id, Path: p, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
			}
			return missingVolume()(ctx, p)
		},
		ListTargetStatusFunc: func(context.Context) ([]qumulo.RelationshipStatus, error) {
			return []qumulo.RelationshipStatus{{
				ID: "rel-1", State: "ESTABLISHED",
				TargetRootPath: "/csi-volumes/group-a/pvc-replica", TargetRootReadOnly: true,
			}}, nil
		},
		StartTreeDeleteFunc: func(_ context.Context, id string) error {
			treeDeleted = append(treeDeleted, id)
			return nil
		},
	}
	srv := newTestControllerServer(t, client)

	// The replica itself is refused.
	_, err := srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: testServer + "##/csi-volumes/group-a/pvc-replica", Secrets: testSecrets()})
	assertCode(t, err, codes.FailedPrecondition)
	require.Empty(t, treeDeleted)

	// The directory that holds it must be refused the same way.
	_, err = srv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: testServer + "##/csi-volumes/group-a", Secrets: testSecrets()})
	t.Logf("DeleteVolume of the replica's parent answered %v; tree deletes %v", err, treeDeleted)
	assert.Empty(t, treeDeleted, "the directory holding a live replica was tree-deleted")
	assertCode(t, err, codes.FailedPrecondition)
}

// TestDefect_DeleteVolumeRefusesADirectoryHoldingAReplicationSource.
// Correct: the same for a relationship's SOURCE root below the deleted path
func TestDefect_DeleteVolumeRefusesADirectoryHoldingAReplicationSource(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	parent := path.Dir(replVolumePath)

	_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: newVolumeID(arrayA, "", parent).String(), Secrets: deleteSecrets(),
	})
	t.Logf("DeleteVolume of the source's parent answered %v", err)

	assert.Contains(t, s.a.dirs, replVolumePath, "the replication source below the deleted directory was tree-deleted")
	assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state, "the relationship was ended")
	if assert.Error(t, err) {
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "got %v", err)
	}
}

// ===========================================================================
// Read-write replica exports from an earlier build are migrated,
// not graded DEGRADED for ever.
// ===========================================================================

// TestDefect_InfoMigratesADriverMadeReadWriteReplicaExport.
// Correct: a replica export this driver made but left read-write (as earlier
// builds did) does not keep the volume DEGRADED on every poll: a status poll
// on a source whose peer is not promoted fences it, and later polls are not
func TestDefect_InfoMigratesADriverMadeReadWriteReplicaExport(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	require.Len(t, s.b.exports, 1, "Enable pre-staged the replica's export")
	s.b.exports[0].ReadOnly = false                                          // as an earlier build left it
	dfxSetDescription(&s.b.exports[0], exportDescriptionFor(replVolumeName)) // the driver's own

	var last ReplicationHealth
	for poll := 0; poll < 3; poll++ {
		h, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())
		require.NoError(t, err)
		t.Logf("poll %d: %s %s", poll, h.Status, h.Message)
		last = h
	}

	assert.True(t, s.b.exports[0].ReadOnly, "the driver's own read-write replica export was never fenced")
	assert.NotContains(t, last.Message, "read-write", "still graded on the read-write export after 3 polls")
	assert.Equal(t, ReplicationHealthy, last.Status, "message: %s", last.Message)
}

// ===========================================================================
// A plain disable and re-enable of a group whose covering export
// was left read-write must re-protect it.
// ===========================================================================

// TestDefect_PlainReEnableReprotectsAGroupWhoseCoveringExportWasLeftReadWrite.
// Correct: a group protected and never failed over, whose covering export on
// the DR array is read-write when replication is disabled (protected by an
// earlier build, or changed by hand), is re-protected by a plain re-enable
// (fence and continue when no promote is in evidence), not left with no
func TestDefect_PlainReEnableReprotectsAGroupWhoseCoveringExportWasLeftReadWrite(t *testing.T) {
	s := newSharedScenario(t)
	s.withCoveringExportOnB(true)
	class := groupClass("allowNonEmptyTarget", "true")
	require.NoError(t, s.enableGroup(t, class, ""))
	s.b.exports[0].ReadOnly = false // as an earlier build left it, or a hand edit

	_, err := s.server.DisableVolumeReplication(context.Background(), &replication.DisableVolumeReplicationRequest{
		VolumeId: s.memberHandle("pvc-tenant-a"), Parameters: class, Secrets: replSecrets(),
	})
	require.NoError(t, err)
	require.Zero(t, s.fabric.relCount(), "Disable removed the relationship on both arrays")

	err = s.enableGroup(t, class, "")
	t.Logf("re-enable: %v", err)
	require.NoError(t, err, "the group was never failed over, yet it is left unprotected")
	assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
}

// TestDefect_PlainReEnableReprotectsAVolumeWhoseDriverMadeExportWasLeftReadWrite.
// Correct: the same for a dedicated-export volume whose driver-made replica
// export is read-write (earlier builds): the export is fenced if it is driver-made.
func TestDefect_PlainReEnableReprotectsAVolumeWhoseDriverMadeExportWasLeftReadWrite(t *testing.T) {
	s := newReplScenario(t)
	class := classToB("allowNonEmptyTarget", "true")
	s.protect(t, class)
	require.Len(t, s.b.exports, 1)
	s.b.mkdir(replVolumePath + "/data") // the replica holds data
	s.b.exports[0].ReadOnly = false
	dfxSetDescription(&s.b.exports[0], exportDescriptionFor(replVolumeName))

	require.NoError(t, s.disable(t, s.handleA, class))
	require.Zero(t, s.fabric.relCount(), "Disable removed both ends")

	err := s.enable(t, class)
	t.Logf("re-enable: %v", err)
	require.NoError(t, err, "never failed over, yet re-enable is refused and the volume left unprotected")
	assert.Equal(t, qumulo.RelationshipEstablished, s.fabric.onlyRel().state)
}

// ===========================================================================
// Enable must not take write access away from an operator export.
// ===========================================================================

// TestDefect_EnableDoesNotFenceAnOperatorCreatedExport.
// Correct: Enable leaves a covering export the operator created (not the
// driver's description) as it is; it may refuse with the remedy instead.
func TestDefect_EnableDoesNotFenceAnOperatorCreatedExport(t *testing.T) {
	s := newSharedScenario(t)
	s.withCoveringExportOnB(false) // operator-made, read-write
	dfxSetDescription(&s.b.exports[0], "made by the storage team")
	before := len(s.b.calls)

	err := s.enableGroup(t, groupClass(), "")
	t.Logf("enable: %v", err)

	if err != nil {
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "got %v", err)
	}
	assert.False(t, s.b.exports[0].ReadOnly, "Enable made the operator's export read-only")
	assert.False(t, dfxCalled(s.b.calls[before:], "SetExportReadOnly"), "Enable wrote the operator's export")
}

// ===========================================================================
// The null-restriction panic, reached through Enable's fencing.
// ===========================================================================

// TestDefect_EnableFencingAnExportWithANullRestrictionDoesNotPanic.
// Correct: when Enable fences a replica export whose restrictions list holds a
// JSON null, the RPC answers an error (or succeeds) and nothing is written
// back; it does not panic. See
// pkg/qumulo/replication.go SetExportReadOnly.
func TestDefect_EnableFencingAnExportWithANullRestrictionDoesNotPanic(t *testing.T) {
	var written atomic.Bool
	arrayHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			written.Store(true)
		}
		_, _ = w.Write([]byte(`{"id":"1","export_path":"` + replExportPath + `","fs_path":"` + replVolumePath + `",` +
			`"description":"` + exportDescriptionFor(replVolumeName) + `","restrictions":[null]}`))
	}))
	defer arrayHTTP.Close()

	s := newReplScenario(t)
	s.protect(t, classToB())
	require.Len(t, s.b.exports, 1)
	s.b.exports[0].ReadOnly = false
	dfxSetDescription(&s.b.exports[0], exportDescriptionFor(replVolumeName))
	wb := dfxWrap(s.b)
	wb.realExportClient = qumulo.New(arrayHTTP.URL, "t")
	dfxInstall(s, wb)

	var err error
	panicked := func() (p any) {
		defer func() { p = recover() }()
		err = s.enable(t, classToB())
		return nil
	}()

	require.Nil(t, panicked, "Enable panicked fencing an export with a null restriction (kills the controller): %v", panicked)
	t.Logf("enable: %v (SetExportReadOnly calls %d)", err, wb.count("SetExportReadOnly"))
	assert.False(t, written.Load(), "the export was written back although a restriction was null")
}

// ===========================================================================
// An interrupted planned promote does not leave both copies
// read-only.
// ===========================================================================

// TestDefect_PlannedPromoteCancelledAfterAStepDoesNotLeaveNothingWritable.
// Correct: when the promote's RPC context is cancelled (a controller shutdown)
// right after the freeze, or right after make-writable, the promote's
// freeze-to-replumb section still completes: the call does not leave the
// source frozen beside a read-only target, nor a writable target behind a
func TestDefect_PlannedPromoteCancelledAfterAStepDoesNotLeaveNothingWritable(t *testing.T) {
	for _, step := range []struct{ array, call string }{
		{"A", "PatchSource"},
		{"B", "MakeTargetWritable"},
	} {
		t.Run("cancelled after "+step.call, func(t *testing.T) {
			s := newReplScenario(t)
			rel := s.protect(t, classToB())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wa, wb := dfxWrap(s.a), dfxWrap(s.b)
			wa.ctxAware, wb.ctxAware = true, true
			if step.array == "A" {
				wa.cancelAfter, wa.cancel = step.call, cancel
			} else {
				wb.cancelAfter, wb.cancel = step.call, cancel
			}
			dfxInstall(s, wa, wb)

			_, err := s.server.PromoteVolume(ctx, &replication.PromoteVolumeRequest{
				VolumeId: s.handleB(t), Parameters: classToA(), Secrets: replSecrets(),
			})
			t.Logf("promote: %v; sourceRO=%v targetRO=%v state=%s", err, rel.sourceRO, rel.targetRO, rel.state)

			assert.False(t, rel.sourceRO && rel.targetRO,
				"the source is frozen and the target still a read-only replica: nothing is writable until a retry")
			if !rel.targetRO {
				for _, e := range s.b.exports {
					if e.FSPath == replVolumePath {
						assert.False(t, e.ReadOnly, "the target is writable but its export is still read-only")
					}
				}
			}
		})
	}
}

// ===========================================================================
// A status poll does not read whole tables on both arrays.
// ===========================================================================

func dfxPolls(t *testing.T, s *replScenario, handle string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
			VolumeId: handle, Secrets: replSecrets(),
		})
		require.NoError(t, err)
	}
}

// TestDefect_InfoPollsDoNotListTheWholeExportTable.
// Correct: once a poll has found the volume's exports, later polls do not list
// the whole export table on either array (read by ID, or skip the read).
// Both arrays here also answer a GET of one export by ID.
func TestDefect_InfoPollsDoNotListTheWholeExportTable(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	wa, wb := dfxWrap(s.a), dfxWrap(s.b)
	wa.exportByID, wb.exportByID = true, true
	dfxInstall(s, wa, wb)

	dfxPolls(t, s, s.handleA, 1) // the first poll may list
	wa.reset()
	wb.reset()
	dfxPolls(t, s, s.handleA, 5)
	t.Logf("5 polls: A %v; B %v", wa.snapshot(), wb.snapshot())

	assert.Zero(t, wa.count("ListExports")+wb.count("ListExports"),
		"each poll re-read the whole export table")
}

// TestDefect_InfoPollsDoNotListTheRelationshipTables.
// Correct: once the relationship ID is known, later polls read it by ID rather
// than listing both relationship status tables on both arrays.
func TestDefect_InfoPollsDoNotListTheRelationshipTables(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	wa, wb := dfxWrap(s.a), dfxWrap(s.b)
	wa.exportByID, wb.exportByID = true, true
	dfxInstall(s, wa, wb)

	dfxPolls(t, s, s.handleA, 1)
	wa.reset()
	wb.reset()
	dfxPolls(t, s, s.handleA, 5)
	t.Logf("5 polls: A %v; B %v", wa.snapshot(), wb.snapshot())

	listings := wa.count("ListSourceStatus", "ListTargetStatus") + wb.count("ListSourceStatus", "ListTargetStatus")
	assert.Zero(t, listings, "each poll listed the relationship status tables (%d listings in 5 polls)", listings)
}

// TestDefect_UnprotectedVolumeInfoPollsDoNotListTheRelationshipTables.
// Correct: a volume with no relationship (Info answers FailedPrecondition)
// does not list both status tables, and both again for a covering group, on
func TestDefect_UnprotectedVolumeInfoPollsDoNotListTheRelationshipTables(t *testing.T) {
	s := newReplScenario(t)
	wa, wb := dfxWrap(s.a), dfxWrap(s.b)
	wa.exportByID, wb.exportByID = true, true
	dfxInstall(s, wa, wb)
	poll := func() {
		_, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
			VolumeId: s.handleA, Secrets: replSecrets(),
		})
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	}

	poll() // the first poll may list
	wa.reset()
	wb.reset()
	for i := 0; i < 5; i++ {
		poll()
	}
	t.Logf("5 polls: A %v; B %v", wa.snapshot(), wb.snapshot())

	listings := wa.count("ListSourceStatus", "ListTargetStatus") + wb.count("ListSourceStatus", "ListTargetStatus")
	assert.Zero(t, listings, "each poll of an unprotected volume listed the status tables (%d listings in 5 polls)", listings)
}

// ===========================================================================
// A group VolumeReplication does not re-stage every member's quota
// on every poll.
// ===========================================================================

// TestDefect_GroupInfoDoesNotRestageEveryMemberOnEveryPoll.
// Correct: with 50 members and no membership change, a repeat poll costs far
// fewer calls than one per member (stage on change, or rate-limit). REST
// (measured 267 REST calls per Info for 50 members).
func TestDefect_GroupInfoDoesNotRestageEveryMemberOnEveryPoll(t *testing.T) {
	const members = 50
	s := newSharedScenario(t)
	for i := 0; i < members-3; i++ {
		s.a.quotas[s.a.mkdir(path.Join(sharedBasePath, fmt.Sprintf("pvc-extra-%02d", i)))] = 1 << 30
	}
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	wa, wb := dfxWrap(s.a), dfxWrap(s.b)
	wa.exportByID, wb.exportByID = true, true
	dfxInstall(s.replScenario, wa, wb)

	dfxPolls(t, s.replScenario, s.memberHandle("pvc-tenant-a"), 1)
	wa.reset()
	wb.reset()
	dfxPolls(t, s.replScenario, s.memberHandle("pvc-tenant-a"), 1)
	perMember := wa.count("GetQuota", "LookupFileByPath", "ResolveFileIDs", "UpdateQuota", "CreateQuota") +
		wb.count("GetQuota", "LookupFileByPath", "ResolveFileIDs", "UpdateQuota", "CreateQuota")
	t.Logf("repeat poll, %d members: %d calls total (A %v; B %v)", members, wa.total()+wb.total(), wa.snapshot(), wb.snapshot())

	assert.Less(t, perMember, members,
		"a repeat poll with no membership change re-read every member's quota (%d per-member calls for %d members)",
		perMember, members)
}

// TestDefect_WritesInAMemberDoNotRestageTheGroup.
// Correct: files created in member volumes change those directories' child
// counts but not the group's membership, so the next poll does not re-stage
func TestDefect_WritesInAMemberDoNotRestageTheGroup(t *testing.T) {
	const members = 50
	s := newSharedScenario(t)
	for i := 0; i < members-3; i++ {
		s.a.quotas[s.a.mkdir(path.Join(sharedBasePath, fmt.Sprintf("pvc-extra-%02d", i)))] = 1 << 30
	}
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	wa, wb := dfxWrap(s.a), dfxWrap(s.b)
	wa.exportByID, wb.exportByID = true, true
	dfxInstall(s.replScenario, wa, wb)

	dfxPolls(t, s.replScenario, s.memberHandle("pvc-tenant-a"), 1)
	s.a.files = map[string]int{}
	for i := 0; i < members-3; i++ {
		s.a.files[path.Join(sharedBasePath, fmt.Sprintf("pvc-extra-%02d", i))] = 10 + i
	}
	wa.reset()
	wb.reset()
	dfxPolls(t, s.replScenario, s.memberHandle("pvc-tenant-a"), 1)
	perMember := wa.count("GetQuota", "LookupFileByPath", "ResolveFileIDs", "UpdateQuota", "CreateQuota") +
		wb.count("GetQuota", "LookupFileByPath", "ResolveFileIDs", "UpdateQuota", "CreateQuota")
	t.Logf("poll after writes in %d members: %d calls total (A %v; B %v)", members-3, wa.total()+wb.total(), wa.snapshot(), wb.snapshot())

	assert.Less(t, perMember, members,
		"writes inside members re-staged every member's quota (%d per-member calls for %d members)", perMember, members)
}

// ===========================================================================
// After a reversed drill, the source's freeze is lifted once the
// target settles read-only, even if the reconnect reply said otherwise.
// ===========================================================================

// TestDefect_FreezeIsLiftedOnceTheTargetSettlesAfterAnInBetweenReconnectReply.
// Correct: a completed DR-side promote is reversed (Demote and Resync on B);
// the reconnect reply reports the target not yet read-only, though it settles
// read-only at once. Within a few further resyncs the source's freeze is
func TestDefect_FreezeIsLiftedOnceTheTargetSettlesAfterAnInBetweenReconnectReply(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.True(t, rel.sourceRO, "the planned promote froze A")
	require.False(t, rel.targetRO, "and made B writable")
	wb := dfxWrap(s.b)
	wb.reconnectRepliesWritable = 1
	dfxInstall(s, wb)

	// The DR side back to secondary: csi-addons calls Demote, then Resync
	// (force = autoResync) until Ready.
	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	for i := 0; i < 4; i++ {
		ready, err := s.resync(t, s.handleB(t), classToA(), true)
		t.Logf("resync %d: ready=%v err=%v state=%s sourceRO=%v targetRO=%v", i, ready, err, rel.state, rel.sourceRO, rel.targetRO)
	}

	require.True(t, rel.targetRO, "B is a read-only target again")
	assert.Equal(t, qumulo.RelationshipEstablished, rel.state)
	assert.False(t, rel.sourceRO, "A stays frozen: no copy anywhere is writable")
}

// TestDefect_ControlFreezeIsLiftedWhenTheReconnectReplyIsReadOnly: control.
// The same drill with the reply Core 7.9.2.2 sends: the freeze is lifted. If
// this fails the drill itself regressed (earlier), not.
func TestDefect_ControlFreezeIsLiftedWhenTheReconnectReplyIsReadOnly(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.NoError(t, s.demote(t, s.handleB(t), classToA()))
	for i := 0; i < 4; i++ {
		_, _ = s.resync(t, s.handleB(t), classToA(), true)
	}
	require.True(t, rel.targetRO)
	assert.False(t, rel.sourceRO, "the reversed drill left A frozen")
}

// ===========================================================================
// The VolumeReplication Replicating condition is set from the
// assessment, not always Unknown.
// ===========================================================================

// dfxInfoStatus reads the status enum of an Info response by its proto field
// name, so this compiles against both csi-addons spec versions; "" means the
// response type has no status field at all.
func dfxInfoStatus(resp *replication.GetVolumeReplicationInfoResponse) string {
	msg := resp.ProtoReflect()
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name("status"))
	if fd == nil || fd.Enum() == nil {
		return ""
	}
	v := fd.Enum().Values().ByNumber(msg.Get(fd).Enum())
	if v == nil {
		return fmt.Sprintf("enum(%d)", msg.Get(fd).Enum())
	}
	return string(v.Name())
}

// TestDefect_InfoReportsTheAssessedStatus.
// Correct: GetVolumeReplicationInfo carries the status csi-addons sets the
// Replicating condition from: HEALTHY for a healthy relationship, and DEGRADED
// or ERROR (never UNKNOWN) for one the driver assessed as not healthy.
func TestDefect_InfoReportsTheAssessedStatus(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		s := newReplScenario(t)
		s.protect(t, classToB())
		h := s.assess(t, s.handleA, nil)
		require.Equal(t, ReplicationHealthy, h.Status, h.Message)

		resp, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
			VolumeId: s.handleA, Secrets: replSecrets(),
		})
		require.NoError(t, err)
		got := dfxInfoStatus(resp)
		require.NotEmpty(t, got, "the Info response has no status field: csi-addons reads Replicating=Unknown for every volume")
		assert.Equal(t, "HEALTHY", got)
	})

	t.Run("not healthy", func(t *testing.T) {
		s := newReplScenario(t)
		rel := s.protect(t, classToB())
		rel.lastJobError = "replication job failed: target out of space"

		h := s.assess(t, s.handleA, nil)
		require.NotEqual(t, ReplicationHealthy, h.Status, "the assessment must see the job error")

		resp, err := s.server.GetVolumeReplicationInfo(context.Background(), &replication.GetVolumeReplicationInfoRequest{
			VolumeId: s.handleA, Secrets: replSecrets(),
		})
		require.NoError(t, err)
		got := dfxInfoStatus(resp)
		require.NotEmpty(t, got, "the Info response has no status field: a broken relationship reads like a healthy one")
		assert.Contains(t, []string{"DEGRADED", "ERROR"}, got, "assessment was %s", h.Status)
	})
}

// ===========================================================================
// After a DNS failover a snapshot of the unchanged claim does not
// silently capture the lagging replica.
// ===========================================================================

// TestDefect_SnapshotAfterDNSFailoverDoesNotCaptureTheReplica.
// Correct: after a failover and reverse (A is now B's read-only replica), a
// CreateSnapshot through the unchanged DNS-named claim's handle (whose API
// endpoint is still A) is refused, or is taken on the live copy (B); it is not
func TestDefect_SnapshotAfterDNSFailoverDoesNotCaptureTheReplica(t *testing.T) {
	s := newReplScenario(t)
	dns := s.fabric.addArray("dr-name.example")
	dns.down = true
	s.protect(t, classToB())
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	_, err := s.resync(t, s.handleA, classToB(), true)
	require.NoError(t, err)
	rel := s.fabric.onlyRel()
	require.Equal(t, s.b, rel.source, "failed over and reversed: B is the live source")
	require.True(t, rel.targetRO, "A is B's read-only replica")

	handle := newVolumeID("dr-name.example", "https://"+arrayA+":8000", replVolumePath).String()
	resp, err := s.controller(t).CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: handle, Name: "snapshot-after-failover", Secrets: deleteSecrets(),
	})
	t.Logf("CreateSnapshot: resp=%v err=%v", resp, err)

	if err != nil {
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "got %v", err)
		return
	}
	assert.Contains(t, resp.GetSnapshot().GetSnapshotId(), arrayB,
		"the snapshot was taken on A, the read-only replica behind the live copy, and reported ReadyToUse=%v",
		resp.GetSnapshot().GetReadyToUse())
}
