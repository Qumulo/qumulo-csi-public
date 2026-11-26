// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// fakeFabric is two (or more) fake arrays sharing one relationship table, so
// a relationship created on one is authorized on the other and every verb
// changes what both report -- which is what the replication service's state
// tables are about. It is the driver-package equivalent of the mock server's
// state machine, with the same measured transitions, kept here rather than
// behind HTTP so a test can put a relationship straight into any row of a
// table and assert on the calls each array received.
type fakeFabric struct {
	mu     sync.Mutex
	arrays map[string]*fakeArray
	rels   map[string]*fakeRel
	nextID int
	// nextSnapID is the cluster-wide snapshot id sequence: replication
	// snapshots and marker snapshots draw from the same one, as on Core.
	nextSnapID int

	// lag is how far behind the clock an established relationship's recovery
	// point trails (Core's cycle is a 60 s step; the fabric smooths it into
	// a trailing point, so lag is the worst-case wait), and how long a job a
	// test starts by hand (jobStart) stays RUNNING; zero drains instantly.
	lag time.Duration
	// frozenPoint stops the recovery point from moving on its own at all.
	// For tests that place the point exactly (the freeze-stamp race, the
	// already-frozen retry), or that model a source nothing drains.
	frozenPoint bool
	// makeWritableDisconnectingPolls seeds fakeRel.disconnectingPolls at
	// make-writable: how many status reads answer DISCONNECTING before the
	// array settles. Zero, the default, is the measured behaviour.
	makeWritableDisconnectingPolls int
	// reconnectLag is how long the SOURCE end keeps reporting DISCONNECTED
	// after a reconnect while the target end already says ESTABLISHED
	// (measured: a few seconds on Core 7.9.2.2).
	reconnectLag time.Duration
	// onPatchSource runs inside PatchSource after the patch is applied, with
	// the fabric lock held: the array's reaction to a freeze.
	onPatchSource func(*fakeRel)

	// tokens records which bearer token each array was dialed with.
	tokens map[string][]string
}

// fakeRel is one relationship, seen from both ends.
type fakeRel struct {
	id             string
	source, target *fakeArray
	sourcePath     string
	targetPath     string
	targetAddress  string

	state    string
	enabled  bool
	mode     string
	policies []qumulo.SnapshotPolicyLink

	sourceRO, targetRO bool
	recoveryPoint      time.Time
	hasSource          bool
	hasTarget          bool
	endReason          string
	lastJobError       string
	// lastJobDuration and bytesTransferred are what Core reports for the
	// last job (duration_of_last_job, replication_job_status).
	lastJobDuration  time.Duration
	bytesTransferred uint64

	// refuseReverse makes Reverse answer HTTP 400, for the fallback rows.
	refuseReverse bool

	// queuedPolls is how many source-status reads report a snapshot still
	// queued to cross.
	queuedPolls int

	// reconnectReplyWritable makes the Reconnect reply show the target not
	// yet read-only, the in-between state, though it settles read-only at
	// once.
	reconnectReplyWritable bool

	// disconnectingPolls is how many target-status READS answer DISCONNECTING
	// with the tree still read-only before the array admits the promote
	// finished. make-writable is asynchronous on Core: it is accepted and
	// answers DISCONNECTING, and the tree stays read-only until the state
	// reaches DISCONNECTED.
	//
	// Zero is the measured shape, and the default. All 62 make-writable
	// calls captured against the 7.9.2.2 lab arrays answered DISCONNECTING,
	// and no status read ever caught the array still in it -- the five pairs
	// with a follow-up read had settled by the first one, 152-163 ms later.
	// So the response is DISCONNECTING and the next read is not.
	//
	// A test models a slower array by setting it, which is the only way to
	// reach awaitWritableTarget's backoff and its deadline. Until this
	// existed the fabric answered DISCONNECTED and writable in the
	// make-writable response itself, a reply the array has never sent, and
	// the whole loop went unexecuted in CI.
	disconnectingPolls int

	// jobStart is when a running replication job began, by the source
	// array's clock; zero when none runs. A freeze starts none
	// (measured); a test sets it to model a cycle job in flight. It
	// stays RUNNING for the fabric's lag and completes with a recovery point
	// equal to its start time and a new snapshot id.
	jobStart time.Time
	// repSnapID is the id of the last replicated snapshot, reported as
	// RecoveryPointSnapshotID; replicatedGen is the content generation it
	// holds. gen is the source tree's current generation: a test bumps it
	// with write() to model writes since the last replication. The marker
	// gate compares generations: SnapshotDiffEmpty is "same generation".
	repSnapID     int
	replicatedGen int
	gen           int
	// reconnectAt is when the target reconnected, by the source's clock; the
	// source end reports DISCONNECTED until reconnectLag has passed.
	reconnectAt time.Time

	// sourceReportsState, when set, is the state the SOURCE array lists for
	// this relationship while the shared state says otherwise. Core's two
	// ends do not update atomically: measured live, a source still listed
	// AWAITING_AUTHORIZATION for a few seconds after the target had
	// authorized, and a repeat authorize then answered
	// relationship_manager_already_authorized_error.
	sourceReportsState string
}

// fakeArray is one array: a directory tree, quotas, exports, policies,
// snapshots, and the replication verbs, all over the shared fabric.
//
// *MockClient is embedded so the ClientInterface methods this fake does not
// model keep the same zero-value behaviour every other test relies on; the
// ones replication touches are overridden below.
type fakeArray struct {
	*MockClient
	fabric *fakeFabric
	addr   string

	// snapshotListings counts ListSnapshots calls.
	snapshotListings int
	// revertSnapshots makes make-writable and reconnect take a snapshot of
	// the target root first, with no expiration, named as the array names
	// it. Opt-in, so the snapshot tables other tests pin are unchanged.
	revertSnapshots bool
	// revertSnapshotLate, when above zero, makes a reconnect's revert
	// snapshot appear only at that many ListSnapshots calls after the
	// reconnect, as a real array takes it after the call has returned
	// lateRevert is the relationship it is pending for.
	revertSnapshotLate int
	lateRevert         *fakeRel
	lateRevertIn       int
	// expirationFault, when set, fails SetSnapshotExpiration.
	expirationFault error
	// files is how many files a directory holds besides its subdirectories,
	// by path: counted in the child_count ListDirectory reports.
	files map[string]int
	// statusListings counts ListSourceStatus and ListTargetStatus calls.
	statusListings int
	// policyIntervals is the last interval ObservePolicyInterval reported
	// against this array, per relationship.
	policyIntervals map[string]time.Duration

	// cancelOn and cancel end a test's RPC context right after the named
	// call is recorded: a controller shutdown landing mid-RPC.
	cancelOn string
	cancel   context.CancelFunc

	// protectionVerdicts is every value the driver passed to
	// ObserveVolumeProtection against this array, oldest first;
	// protectionSubjects is the subject on the context of each.
	protectionVerdicts []bool
	protectionSubjects []string

	// down makes every call fail with a transport error, which the service
	// must classify as Unavailable rather than Internal.
	down bool
	// refuseExportWrites makes SetExportReadOnly fail with HTTP 403, as an
	// array whose token lacks the export write privilege answers.
	refuseExportWrites bool
	// onTargetStatus, when set, runs on the relationship (fabric lock held)
	// before GetTargetStatus reads it: a state change landing between two
	// reads of one RPC.
	onTargetStatus func(*fakeRel)
	// onExportWrite, when set, runs in SetExportReadOnly after the call is
	// recorded and before the export is changed, with no lock held: a
	// promote landing between a fence's read of the end and its write.
	onExportWrite func(exportID string, readOnly bool)
	// wire, when set, is called by every call this array serves, before it
	// is served, with the call's context; an error it returns is the call's.
	// It puts a real REST client and circuit breaker in the path.
	wire func(ctx context.Context) error
	// clockSkew offsets this array's clock from the test's: ClusterTime and
	// every recovery point this array stamps carry it, so a drain gate that
	// mixed the controller's clock with the array's would be caught.
	clockSkew time.Duration

	nextID    int
	dirs      map[string]string // path -> file ID
	ids       map[string]string // file ID -> path
	quotas    map[string]uint64
	exports   []qumulo.ExportInfo
	policies  []fakePolicy
	snapshots []qumulo.SnapshotInfo
	// snapshotGens is the content generation each manual snapshot captured,
	// by id, for SnapshotDiffEmpty.
	snapshotGens map[string]int
	// snapshotTrees is what a manual snapshot of a directory that is no
	// relationship's source root captured -- every directory below it and
	// its file count -- by id. Two such snapshots differ in nothing when
	// they captured the same tree: Disable's unpromoted marker of a replica
	// root and Enable's fresh snapshot of it.
	snapshotTrees map[string]string
	// snapshotFault, when set, is asked before CreateExpiringSnapshot,
	// SnapshotDiffEmpty, DeleteSnapshot and ListSnapshots are served, with the verb and (for
	// a create) the name suffix; an error it returns is the call's.
	snapshotFault func(verb, nameSuffix string) error

	// calls is every replication verb this array received, in order, for
	// assertions such as "make-writable was not called twice".
	calls []string

	// owners and groups are a directory's owner_details and group_details,
	// by file ID. A directory a test did not set reports none, as the
	// fabric always has.
	owners, groups map[string]qumulo.Identity
}

// own sets a directory's owner and group.
func (a *fakeArray) own(p string, owner, group qumulo.Identity) {
	id := a.mkdir(p)
	if a.owners == nil {
		a.owners, a.groups = map[string]qumulo.Identity{}, map[string]qumulo.Identity{}
	}
	a.owners[id], a.groups[id] = owner, group
}

// SetFileAttributesByID applies the owner and group of an update; it is
// recorded as "SetAttributes <path>".
func (a *fakeArray) SetFileAttributesByID(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	a.record("SetAttributes " + a.ids[fileID])
	if a.owners == nil {
		a.owners, a.groups = map[string]qumulo.Identity{}, map[string]qumulo.Identity{}
	}
	if attrs.OwnerDetails != nil {
		a.owners[fileID] = *attrs.OwnerDetails
	}
	if attrs.GroupDetails != nil {
		a.groups[fileID] = *attrs.GroupDetails
	}
	return nil
}

type fakePolicy struct {
	qumulo.PolicyInfo
	schedule qumulo.Schedule
}

var errFakeUnreachable = errors.New("dial tcp: connect: connection refused")

func newFakeFabric() *fakeFabric {
	return &fakeFabric{arrays: map[string]*fakeArray{}, rels: map[string]*fakeRel{}, tokens: map[string][]string{}}
}

func (f *fakeFabric) addArray(addr string) *fakeArray {
	// Each array allocates file IDs from its own range, as real arrays do
	// (measured: the replica's ID differs from the source's), so a test can
	// see a handle carried to the wrong array fail to resolve.
	a := &fakeArray{
		MockClient: &MockClient{},
		fabric:     f,
		addr:       addr,
		nextID:     1000 * (len(f.arrays) + 1),
		dirs:       map[string]string{"/": "2"},
		ids:        map[string]string{"2": "/"},
		quotas:     map[string]uint64{},
	}
	f.arrays[addr] = a
	return a
}

// newClient is the Driver.NewClient hook: the host of the URL picks the array.
func (f *fakeFabric) newClient(baseURL, token string) qumulo.ClientInterface {
	host := hostOfURL(baseURL)
	f.mu.Lock()
	f.tokens[host] = append(f.tokens[host], token)
	a := f.arrays[host]
	f.mu.Unlock()
	if a == nil {
		panic("fake fabric: no array at " + host)
	}
	return a
}

func fakeAPIError(code int, class, msg string) error {
	return &qumulo.APIError{StatusCode: code, Path: "/fake", Class: class, Msg: msg}
}

func (a *fakeArray) unreachable() error {
	if a.down {
		return fmt.Errorf("calling %s: %w", a.addr, errFakeUnreachable)
	}
	return nil
}

// unreachableFor is unreachable, and also fails a call whose context has
// ended, as the real client does; cancelOn, when set, ends the context of a
// test's RPC after the named call (a controller shutdown mid-RPC).
func (a *fakeArray) unreachableFor(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("calling %s: %w", a.addr, err)
	}
	if a.wire != nil {
		if err := a.wire(ctx); err != nil {
			return err
		}
	}
	return a.unreachable()
}

func (a *fakeArray) record(verb string) {
	a.calls = append(a.calls, verb)
	if a.cancelOn != "" && verb == a.cancelOn && a.cancel != nil {
		a.cancel()
	}
}

// now is this array's clock.
func (a *fakeArray) now() time.Time { return time.Now().Add(a.clockSkew) }

// mkdir creates a directory (and its ancestors) and returns its file ID.
func (a *fakeArray) mkdir(p string) string {
	p = path.Clean(p)
	if id, ok := a.dirs[p]; ok {
		return id
	}
	if parent := path.Dir(p); parent != p {
		a.mkdir(parent)
	}
	id := strconv.Itoa(a.nextID)
	a.nextID++
	a.dirs[p] = id
	a.ids[id] = p
	return id
}

func (a *fakeArray) hasChildren(p string) bool {
	for other := range a.dirs {
		if strings.HasPrefix(other, p+"/") {
			return true
		}
	}
	return false
}

// --- ClientInterface overrides -------------------------------------------

func (a *fakeArray) ResolveFileIDs(ctx context.Context, fileIDs []string) (map[string]string, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, id := range fileIDs {
		if p, ok := a.ids[id]; ok {
			out[id] = p
		}
	}
	return out, nil
}

func (a *fakeArray) LookupFileByPath(ctx context.Context, p string) (qumulo.FileInfo, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.FileInfo{}, err
	}
	id, ok := a.dirs[path.Clean(p)]
	if !ok {
		return qumulo.FileInfo{}, fakeAPIError(http.StatusNotFound, qumulo.ErrorClassNoSuchEntry, "no such path "+p)
	}
	return qumulo.FileInfo{ID: id, Path: p, Type: "FS_FILE_TYPE_DIRECTORY", Owner: a.owners[id], Group: a.groups[id]}, nil
}

// ListDirectory returns the immediate subdirectories of p, which is what the
// group form enumerates to find a class's member volumes.
func (a *fakeArray) ListDirectory(ctx context.Context, p, _ string) ([]qumulo.DirEntry, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return nil, err
	}
	if _, ok := a.dirs[path.Clean(p)]; !ok {
		return nil, fakeAPIError(http.StatusNotFound, qumulo.ErrorClassNoSuchEntry, "no such directory "+p)
	}
	prefix := strings.TrimSuffix(path.Clean(p), "/") + "/"
	names := make([]string, 0, len(a.dirs))
	for child := range a.dirs {
		rest, ok := strings.CutPrefix(child, prefix)
		if !ok || rest == "" || strings.Contains(rest, "/") {
			continue
		}
		names = append(names, rest)
	}
	sort.Strings(names)
	out := make([]qumulo.DirEntry, 0, len(names))
	for _, name := range names {
		childPath := path.Join(p, name)
		count := a.files[childPath]
		for other := range a.dirs {
			if rest, ok := strings.CutPrefix(other, childPath+"/"); ok && rest != "" && !strings.Contains(rest, "/") {
				count++
			}
		}
		id := a.dirs[childPath]
		out = append(out, qumulo.DirEntry{
			Name: name, ID: id, Type: "FS_FILE_TYPE_DIRECTORY", ChildCount: &count,
			OwnerDetails: a.owners[id], GroupDetails: a.groups[id],
		})
	}
	return out, nil
}

func (a *fakeArray) ListExports(ctx context.Context) ([]qumulo.ExportInfo, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return nil, err
	}
	return append([]qumulo.ExportInfo(nil), a.exports...), nil
}

func (a *fakeArray) CreateExport(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.ExportInfo{}, err
	}
	a.record("CreateExport")
	for _, e := range a.exports {
		if e.ExportPath == spec.ExportPath {
			return qumulo.ExportInfo{}, fakeAPIError(http.StatusConflict, qumulo.ErrorClassNFSExportAlreadyExists, "exists")
		}
	}
	if _, ok := a.dirs[spec.FSPath]; !ok {
		return qumulo.ExportInfo{}, fakeAPIError(http.StatusBadRequest, qumulo.ErrorClassNFSExportFSPathNotFound, "missing dir")
	}
	info := qumulo.ExportInfo{
		ID: strconv.Itoa(len(a.exports) + 1), ExportPath: spec.ExportPath, FSPath: spec.FSPath,
		TenantID: spec.TenantID, ReadOnly: spec.ReadOnly, AuthenticationMode: spec.AuthenticationMode,
		AllowedClients: spec.AllowedClients, Description: spec.Description,
	}
	a.exports = append(a.exports, info)
	return info, nil
}

func (a *fakeArray) GetQuota(ctx context.Context, fileID string) (uint64, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return 0, err
	}
	limit, ok := a.quotas[fileID]
	if !ok {
		return 0, fakeAPIError(http.StatusNotFound, qumulo.ErrorClassQuotaLimitNotFound, "no quota")
	}
	return limit, nil
}

func (a *fakeArray) CreateQuota(ctx context.Context, fileID string, limit uint64) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	a.record("CreateQuota")
	if _, ok := a.quotas[fileID]; ok {
		return fakeAPIError(http.StatusConflict, "fs_quota_exists_error", "exists")
	}
	a.quotas[fileID] = limit
	return nil
}

func (a *fakeArray) UpdateQuota(ctx context.Context, fileID string, limit uint64) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	a.record("UpdateQuota")
	a.quotas[fileID] = limit
	return nil
}

func (a *fakeArray) ListPolicies(ctx context.Context) ([]qumulo.PolicyInfo, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return nil, err
	}
	out := make([]qumulo.PolicyInfo, 0, len(a.policies))
	for _, p := range a.policies {
		out = append(out, p.PolicyInfo)
	}
	return out, nil
}

func (a *fakeArray) CreatePolicy(ctx context.Context, name, sourceFileID string, schedule qumulo.Schedule) (qumulo.PolicyInfo, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.PolicyInfo{}, err
	}
	a.record("CreatePolicy")
	info := qumulo.PolicyInfo{ID: strconv.Itoa(len(a.policies) + 1), Name: name, SourceFileID: sourceFileID, Enabled: true}
	a.policies = append(a.policies, fakePolicy{PolicyInfo: info, schedule: schedule})
	return info, nil
}

func (a *fakeArray) GetSnapshot(ctx context.Context, id string) (qumulo.SnapshotInfo, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.SnapshotInfo{}, err
	}
	for _, s := range a.snapshots {
		if s.ID == id {
			return s, nil
		}
	}
	return qumulo.SnapshotInfo{}, fakeAPIError(http.StatusNotFound, "fs_no_such_snapshot_error", "no snapshot "+id)
}

func (a *fakeArray) ListSnapshots(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return nil, err
	}
	if a.snapshotFault != nil {
		if err := a.snapshotFault("ListSnapshots", ""); err != nil {
			return nil, err
		}
	}
	// Counted apart from calls, whose exact sequences other tests pin.
	a.snapshotListings++
	if a.lateRevert != nil {
		if a.lateRevertIn--; a.lateRevertIn <= 0 {
			a.fabric.mu.Lock()
			a.takeRevertSnapshotLocked(a.lateRevert)
			a.fabric.mu.Unlock()
			a.lateRevert = nil
		}
	}
	return append([]qumulo.SnapshotInfo(nil), a.snapshots...), nil
}

// replicateLocked models a replication cycle completing: the target now holds
// the source's current generation, under a new snapshot id. A cycle on a
// tree that has not changed still produces a new id, as Core does every 60 s.
func (a *fakeArray) replicateLocked(r *fakeRel) {
	if r.replicatedGen == r.gen && r.repSnapID != 0 {
		return
	}
	a.fabric.nextSnapID++
	r.repSnapID = a.fabric.nextSnapID
	r.replicatedGen = r.gen
}

// write models writes to the source tree since the last replication.
func (r *fakeRel) write() { r.gen++ }

// replicate models the next cycle landing: the target catches up.
func (f *fakeFabric) replicate(r *fakeRel) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r.source.replicateLocked(r)
}

func (a *fakeArray) CreateExpiringSnapshot(ctx context.Context, fileID, nameSuffix string, expiration time.Time) (qumulo.SnapshotInfo, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.SnapshotInfo{}, err
	}
	a.record("CreateExpiringSnapshot")
	if a.snapshotFault != nil {
		if err := a.snapshotFault("CreateExpiringSnapshot", nameSuffix); err != nil {
			return qumulo.SnapshotInfo{}, err
		}
	}
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	a.fabric.nextSnapID++
	id := strconv.Itoa(a.fabric.nextSnapID)
	gen, sourceRoot := 0, false
	for _, r := range a.fabric.rels {
		if r.source == a && a.dirs[r.sourcePath] == fileID {
			gen, sourceRoot = r.gen, true
		}
	}
	if a.snapshotGens == nil {
		a.snapshotGens = map[string]int{}
	}
	a.snapshotGens[id] = gen
	if p, ok := a.ids[fileID]; ok && !sourceRoot {
		if a.snapshotTrees == nil {
			a.snapshotTrees = map[string]string{}
		}
		a.snapshotTrees[id] = a.treeLocked(p)
	}
	info := qumulo.SnapshotInfo{ID: id, Name: id + "_" + nameSuffix, SourceFileID: fileID, CreatedAt: a.now()}
	a.snapshots = append(a.snapshots, info)
	return info, nil
}

func (a *fakeArray) DeleteSnapshot(ctx context.Context, id string) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	a.record("DeleteSnapshot")
	if a.snapshotFault != nil {
		if err := a.snapshotFault("DeleteSnapshot", ""); err != nil {
			return err
		}
	}
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	kept := a.snapshots[:0]
	for _, s := range a.snapshots {
		if s.ID != id {
			kept = append(kept, s)
		}
	}
	a.snapshots = kept
	delete(a.snapshotGens, id)
	delete(a.snapshotTrees, id)
	return nil
}

// treeLocked is the content of the tree at p as the fabric models it: every
// directory below it, and the file count of each.
func (a *fakeArray) treeLocked(p string) string {
	p = path.Clean(p)
	var entries []string
	for other := range a.dirs {
		if other == p || strings.HasPrefix(other, p+"/") {
			entries = append(entries, fmt.Sprintf("%s:%d", other, a.files[other]))
		}
	}
	sort.Strings(entries)
	return strings.Join(entries, "\n")
}

// SnapshotDiffEmpty: two snapshots of the same tree differ in nothing when
// they captured the same content generation.
func (a *fakeArray) SnapshotDiffEmpty(ctx context.Context, newerID, olderID string) (bool, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return false, err
	}
	a.record("SnapshotDiffEmpty")
	if a.snapshotFault != nil {
		if err := a.snapshotFault("SnapshotDiffEmpty", ""); err != nil {
			return false, err
		}
	}
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	if tn, ok := a.snapshotTrees[newerID]; ok {
		if to, ok := a.snapshotTrees[olderID]; ok {
			return tn == to, nil
		}
	}
	gen := func(id string) (int, bool) {
		if g, ok := a.snapshotGens[id]; ok {
			return g, true
		}
		n, err := strconv.Atoi(id)
		if err != nil {
			return 0, false
		}
		for _, r := range a.fabric.rels {
			if r.source == a && r.repSnapID == n {
				return r.replicatedGen, true
			}
		}
		return 0, false
	}
	gn, okn := gen(newerID)
	go_, oko := gen(olderID)
	if !okn || !oko {
		return false, &qumulo.APIError{StatusCode: 404, Msg: "api_snapshot_not_found_error: no such snapshot"}
	}
	return gn == go_, nil
}

// --- ReplicationClient -----------------------------------------------------

func (a *fakeArray) status(r *fakeRel, source bool) qumulo.RelationshipStatus {
	// Timestamps are the SOURCE array's: it is the array that stamps
	// recovery points.
	now := r.source.now()

	// The recovery point tracks the clock, trailing it by the lag, while
	// established and replicating in a mode with a continuous component --
	// frozen or not. Measured on Core 7.9.2.2: the cycle runs every 60 s regardless of the freeze and of
	// writes, and a freeze starts no job. A policy-only relationship's
	// point is pinned where the last fire left it.
	tracking := r.state == qumulo.RelationshipEstablished && r.enabled &&
		r.mode != qumulo.ReplicationModeSnapshotPolicy && r.jobStart.IsZero()
	if tracking && !a.fabric.frozenPoint {
		r.recoveryPoint = now.Add(-a.fabric.lag)
		a.replicateLocked(r)
	}

	// A job a test started by hand (jobStart): RUNNING for the lag, then
	// complete, with the recovery point at the job's start and a new
	// snapshot id. Core does not start one at a freeze (measured); the
	// knob models a cycle job that happens to be in flight.
	jobState := qumulo.ReplicationJobNotRunning
	if !r.jobStart.IsZero() {
		if now.Before(r.jobStart.Add(a.fabric.lag)) {
			jobState = qumulo.ReplicationJobRunning
		} else {
			r.recoveryPoint = r.jobStart
			a.replicateLocked(r)
			r.jobStart = time.Time{}
		}
	}

	// After a reconnect the source end lags the target end.
	if r.sourceReportsState == qumulo.RelationshipDisconnected && !r.reconnectAt.IsZero() &&
		!now.Before(r.reconnectAt.Add(a.fabric.reconnectLag)) {
		r.sourceReportsState = ""
		r.reconnectAt = time.Time{}
	}

	s := qumulo.RelationshipStatus{
		ID: r.id, State: r.state, EndReason: r.endReason,
		SourceRootPath: r.sourcePath, TargetRootPath: r.targetPath,
		SourceRootReadOnly: r.sourceRO, TargetRootReadOnly: r.targetRO,
		ReplicationEnabled: r.enabled, JobState: jobState,
		RecoveryPoint: r.recoveryPoint, ErrorFromLastJob: r.lastJobError,
		DurationOfLastJob: r.lastJobDuration, BytesTransferred: r.bytesTransferred,
		SourceRootID: r.source.dirs[r.sourcePath], TargetRootID: r.target.dirs[r.targetPath],
		SourceClusterUUID: "uuid-" + r.source.addr, TargetClusterUUID: "uuid-" + r.target.addr,
	}
	if r.repSnapID > 0 {
		s.RecoveryPointSnapshotID = strconv.Itoa(r.repSnapID)
	}
	if source && r.queuedPolls > 0 {
		r.queuedPolls--
		s.QueuedSnapshotCount = 1
	}
	if jobState == qumulo.ReplicationJobRunning {
		s.PercentComplete = 50
	}
	if !source && r.disconnectingPolls > 0 {
		// Still settling: the array reports the in-between state and keeps
		// the tree read-only until it lands.
		r.disconnectingPolls--
		s.State = qumulo.RelationshipDisconnecting
		s.TargetRootReadOnly = true
	}
	if source {
		if r.sourceReportsState != "" {
			s.State = r.sourceReportsState
		}
		s.TargetAddress = r.targetAddress
		s.ReplicationMode = r.mode
		s.SnapshotPolicies = append([]qumulo.SnapshotPolicyLink(nil), r.policies...)
	}
	return s
}

func (a *fakeArray) sortedRels() []*fakeRel {
	out := make([]*fakeRel, 0, len(a.fabric.rels))
	for _, r := range a.fabric.rels {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func (a *fakeArray) ListSourceStatus(ctx context.Context) ([]qumulo.RelationshipStatus, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return nil, err
	}
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	a.statusListings++
	var out []qumulo.RelationshipStatus
	for _, r := range a.sortedRels() {
		if r.source == a && r.hasSource {
			out = append(out, a.status(r, true))
		}
	}
	return out, nil
}

func (a *fakeArray) ListTargetStatus(ctx context.Context) ([]qumulo.RelationshipStatus, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return nil, err
	}
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	a.statusListings++
	var out []qumulo.RelationshipStatus
	for _, r := range a.sortedRels() {
		if r.target == a && r.hasTarget {
			out = append(out, a.status(r, false))
		}
	}
	return out, nil
}

func (a *fakeArray) sourceRel(id string) (*fakeRel, error) {
	r, ok := a.fabric.rels[id]
	if !ok || r.source != a || !r.hasSource {
		return nil, fakeAPIError(http.StatusNotFound, "http_not_found_error", "no source relationship "+id)
	}
	return r, nil
}

func (a *fakeArray) targetRel(id string) (*fakeRel, error) {
	r, ok := a.fabric.rels[id]
	if !ok || r.target != a || !r.hasTarget {
		return nil, fakeAPIError(http.StatusNotFound, "http_not_found_error", "no target relationship "+id)
	}
	return r, nil
}

func (a *fakeArray) GetSourceStatus(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	r, err := a.sourceRel(id)
	if err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	return a.status(r, true), nil
}

func (a *fakeArray) GetTargetStatus(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	r, err := a.targetRel(id)
	if err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	if a.onTargetStatus != nil {
		a.onTargetStatus(r)
	}
	return a.status(r, false), nil
}

func (a *fakeArray) CreateSource(ctx context.Context, spec qumulo.SourceSpec) (qumulo.Relationship, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.Relationship{}, err
	}
	a.record("CreateSource")
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	if _, ok := a.dirs[spec.SourceRootPath]; !ok {
		return qumulo.Relationship{}, fakeAPIError(http.StatusNotFound, qumulo.ErrorClassNoSuchEntry, "no such dir")
	}
	target := a.fabric.arrays[spec.TargetAddress]
	if target == nil {
		return qumulo.Relationship{}, fakeAPIError(http.StatusBadRequest, "replication_target_unreachable_error", "no array at "+spec.TargetAddress)
	}
	for _, r := range a.fabric.rels {
		if r.source == a && r.hasSource && r.sourcePath == spec.SourceRootPath && r.targetPath == spec.TargetRootPath {
			return qumulo.Relationship{}, fakeAPIError(http.StatusConflict, "replication_relationship_exists_error", "duplicate")
		}
	}
	mode := spec.ReplicationMode
	if mode == "" {
		mode = qumulo.ReplicationModeContinuous
	}
	a.fabric.nextID++
	r := &fakeRel{
		id: fmt.Sprintf("rel-%d", a.fabric.nextID), source: a, target: target,
		sourcePath: spec.SourceRootPath, targetPath: spec.TargetRootPath, targetAddress: spec.TargetAddress,
		state: qumulo.RelationshipAwaitingAuthorization, enabled: spec.ReplicationEnabled, mode: mode,
		policies: spec.SnapshotPolicies, hasSource: true, hasTarget: true,
	}
	a.fabric.rels[r.id] = r
	return r.config(), nil
}

func (r *fakeRel) config() qumulo.Relationship {
	return qumulo.Relationship{
		ID: r.id, SourceRootID: r.source.dirs[r.sourcePath], SourceRootReadOnly: r.sourceRO,
		TargetAddress: r.targetAddress, TargetPort: qumulo.ReplicationPort,
		ReplicationEnabled: r.enabled, ReplicationMode: r.mode,
		SnapshotPolicies: append([]qumulo.SnapshotPolicyLink(nil), r.policies...),
	}
}

func (a *fakeArray) PatchSource(ctx context.Context, id string, patch qumulo.SourcePatch) (qumulo.Relationship, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.Relationship{}, err
	}
	a.record("PatchSource")
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	r, err := a.sourceRel(id)
	if err != nil {
		return qumulo.Relationship{}, err
	}
	if patch.SourceRootReadOnly != nil {
		r.sourceRO = *patch.SourceRootReadOnly
	}
	if patch.ReplicationEnabled != nil {
		r.enabled = *patch.ReplicationEnabled
	}
	if patch.ReplicationMode != "" {
		r.mode = patch.ReplicationMode
	}
	if patch.SnapshotPolicies != nil {
		r.policies = append([]qumulo.SnapshotPolicyLink(nil), (*patch.SnapshotPolicies)...)
	}
	// A freeze starts no job (measured on Core 7.9.2.2, 2026-09-15): the
	// recovery point keeps moving on the cycle, which the tracking rule in
	// status() models. Nothing to do here.
	if a.fabric.onPatchSource != nil {
		a.fabric.onPatchSource(r)
	}
	return r.config(), nil
}

func (a *fakeArray) endOrDrop(r *fakeRel, reason string) {
	if !r.hasSource && !r.hasTarget {
		delete(a.fabric.rels, r.id)
		return
	}
	r.state = qumulo.RelationshipEnded
	r.endReason = reason
}

func (a *fakeArray) DeleteSource(ctx context.Context, id string) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	a.record("DeleteSource")
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	r, err := a.sourceRel(id)
	if err != nil {
		return err
	}
	r.hasSource = false
	a.endOrDrop(r, "source deleted")
	return nil
}

func (a *fakeArray) DeleteTarget(ctx context.Context, id string) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	a.record("DeleteTarget")
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	r, err := a.targetRel(id)
	if err != nil {
		return err
	}
	r.hasTarget = false
	r.targetRO = false
	a.endOrDrop(r, "target deleted")
	return nil
}

func (a *fakeArray) Replicate(ctx context.Context, id string) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	a.record("Replicate")
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	r, err := a.sourceRel(id)
	if err != nil {
		return err
	}
	if r.enabled {
		return fakeAPIError(http.StatusBadRequest, qumulo.ErrorClassManualReplicationDisallowed, "enabled")
	}
	return nil
}

func (a *fakeArray) Authorize(ctx context.Context, id string, allowCreate, allowNonEmpty bool) (qumulo.RelationshipStatus, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	a.record("Authorize")
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	r, err := a.targetRel(id)
	if err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	if r.state != qumulo.RelationshipAwaitingAuthorization {
		return qumulo.RelationshipStatus{}, fakeAPIError(http.StatusBadRequest, "relationship_manager_already_authorized_error", r.state)
	}
	if _, exists := a.dirs[r.targetPath]; !exists {
		if !allowCreate {
			return qumulo.RelationshipStatus{}, fakeAPIError(http.StatusNotFound, qumulo.ErrorClassNoSuchEntry, "no target dir")
		}
		a.mkdir(r.targetPath)
	} else if a.hasChildren(r.targetPath) && !allowNonEmpty {
		return qumulo.RelationshipStatus{}, fakeAPIError(http.StatusBadRequest, "relationship_manager_target_path_not_empty_error", "not empty")
	}
	r.state = qumulo.RelationshipEstablished
	r.targetRO = true
	r.recoveryPoint = r.source.now().Add(-a.fabric.lag)
	// The initial sync, instantly. Directories only -- files are not modelled
	// -- which is what a group's per-member quota needs: the member
	// directories under a replicated basePath exist on the target only
	// because replication created them.
	a.mirrorFrom(r.source, r.sourcePath, r.targetPath)
	return a.status(r, false), nil
}

// mirrorFrom copies the directory structure under source's sourcePath into
// this array under targetPath.
func (a *fakeArray) mirrorFrom(source *fakeArray, sourcePath, targetPath string) {
	prefix := strings.TrimSuffix(sourcePath, "/") + "/"
	paths := make([]string, 0, len(source.dirs))
	for p := range source.dirs {
		if strings.HasPrefix(p, prefix) {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	for _, p := range paths {
		a.mkdir(path.Join(targetPath, strings.TrimPrefix(p, prefix)))
	}
}

// ObservePolicyInterval records each interval the driver reported, by
// relationship.
func (a *fakeArray) ObservePolicyInterval(_ context.Context, relationshipID string, interval time.Duration) {
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	if a.policyIntervals == nil {
		a.policyIntervals = map[string]time.Duration{}
	}
	a.policyIntervals[relationshipID] = interval
}

// ObserveVolumeProtection records what the driver concluded about the volume
// this RPC was about. The exporter turns this into the one replication series
// keyed by volume rather than by relationship ID, so a test that the driver
// states it at all is the driver half of that signal.
func (a *fakeArray) ObserveVolumeProtection(ctx context.Context, protected bool) {
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	a.protectionVerdicts = append(a.protectionVerdicts, protected)
	// The subject is what the exporter keys the series on. Recorded because
	// the verdict alone passes whether or not the driver labelled the work,
	// and an unlabelled event is dropped -- which is exactly how this shipped
	// broken the first time.
	a.protectionSubjects = append(a.protectionSubjects, qumulo.SubjectFrom(ctx))
}

func (a *fakeArray) MakeTargetWritable(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	a.record("MakeTargetWritable")
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	r, err := a.targetRel(id)
	if err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	if r.state != qumulo.RelationshipEstablished {
		return qumulo.RelationshipStatus{}, fakeAPIError(http.StatusBadRequest, qumulo.ErrorClassIrreplicable,
			"Disconnect and revert is not allowed on a disconnected relationship")
	}
	a.takeRevertSnapshotLocked(r)
	r.state = qumulo.RelationshipDisconnected
	r.targetRO = false
	// Measured live: the source end keeps reporting ESTABLISHED after the
	// target's make-writable. Only a reverse makes both ends DISCONNECTED.
	r.sourceReportsState = qumulo.RelationshipEstablished

	// The call is accepted, but the array is not done: it answers
	// DISCONNECTING with the tree still read-only. The response is rendered
	// before the poll counter is armed, so the counter means what it says --
	// reads that still answer DISCONNECTING, not counting this reply.
	out := a.status(r, false)
	out.State = qumulo.RelationshipDisconnecting
	out.TargetRootReadOnly = true
	r.disconnectingPolls = a.fabric.makeWritableDisconnectingPolls
	return out, nil
}

func (a *fakeArray) Reconnect(ctx context.Context, id string) (qumulo.RelationshipStatus, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	a.record("Reconnect")
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	r, err := a.targetRel(id)
	if err != nil {
		return qumulo.RelationshipStatus{}, err
	}
	if r.state != qumulo.RelationshipDisconnected {
		return qumulo.RelationshipStatus{}, fakeAPIError(http.StatusBadRequest, qumulo.ErrorClassIrreplicable, "not disconnected")
	}
	if a.revertSnapshotLate > 0 {
		a.lateRevert, a.lateRevertIn = r, a.revertSnapshotLate
	} else {
		a.takeRevertSnapshotLocked(r)
	}
	r.state = qumulo.RelationshipEstablished
	r.targetRO = true
	r.recoveryPoint = r.source.now().Add(-a.fabric.lag)
	// The source end keeps saying DISCONNECTED for a while after the
	// target end reads ESTABLISHED. Cleared by status() once reconnectLag
	// has passed -- at the first read, with the default of zero.
	r.sourceReportsState = qumulo.RelationshipDisconnected
	r.reconnectAt = r.source.now()
	reply := a.status(r, false)
	if r.reconnectReplyWritable {
		reply.TargetRootReadOnly = false
	}
	return reply, nil
}

func (a *fakeArray) Reverse(ctx context.Context, id, sourceAddress string, sourcePort int) (qumulo.Relationship, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.Relationship{}, err
	}
	a.record("Reverse")
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	r, err := a.targetRel(id)
	if err != nil {
		return qumulo.Relationship{}, err
	}
	if r.state != qumulo.RelationshipDisconnected {
		return qumulo.Relationship{}, fakeAPIError(http.StatusBadRequest, qumulo.ErrorClassIrreplicable, "not disconnected")
	}
	if r.refuseReverse {
		return qumulo.Relationship{}, fakeAPIError(http.StatusBadRequest, "replication_reverse_refused_error", "both sides diverged")
	}
	r.source, r.target = r.target, r.source
	r.sourcePath, r.targetPath = r.targetPath, r.sourcePath
	r.targetAddress = sourceAddress
	// The measured reset: continuous, no policies, whatever it was before.
	r.mode = qumulo.ReplicationModeContinuous
	r.policies = nil
	r.targetRO = r.sourceRO
	r.sourceRO = false
	r.state = qumulo.RelationshipDisconnected
	r.sourceReportsState = ""
	return r.config(), nil
}

func (a *fakeArray) ClusterID(ctx context.Context) (string, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return "", err
	}
	return "uuid-" + a.addr, nil
}

func (a *fakeArray) ClusterTime(ctx context.Context) (time.Time, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return time.Time{}, err
	}
	return a.now(), nil
}

func (a *fakeArray) GetPolicySchedule(ctx context.Context, policyID string) (qumulo.Schedule, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.Schedule{}, err
	}
	for _, p := range a.policies {
		if p.ID == policyID {
			return p.schedule, nil
		}
	}
	return qumulo.Schedule{}, fakeAPIError(http.StatusNotFound, "api_snapshots_policy_not_found_error", "no policy")
}

func (a *fakeArray) SetExportReadOnly(ctx context.Context, exportID string, readOnly bool) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	a.record("SetExportReadOnly")
	if a.onExportWrite != nil {
		a.onExportWrite(exportID, readOnly)
	}
	if a.refuseExportWrites {
		return fakeAPIError(http.StatusForbidden, "auth_privilege_error", "missing NFS_EXPORT_WRITE")
	}
	for i := range a.exports {
		if a.exports[i].ID == exportID {
			a.exports[i].ReadOnly = readOnly
			return nil
		}
	}
	return fakeAPIError(http.StatusNotFound, "nfs_export_not_found_error", "no export")
}

var _ replicationClient = (*fakeArray)(nil)

// --- Tree delete, the CSI side -------------------------------------------
//
// Modelled here, rather than left to the embedded MockClient's no-op, because
// the whole point of DeleteVolume's guard is that it runs *before* this: Core
// allows a tree-delete of a replication root and ends the relationship on
// both arrays with the copy orphaned. A fake that
// quietly survived the delete would let a broken guard pass.

func (a *fakeArray) StartTreeDelete(ctx context.Context, fileID string) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	root, ok := a.ids[fileID]
	if !ok {
		return fakeAPIError(404, "fs_no_such_entry_error", "no such file "+fileID)
	}
	a.record("tree-delete " + root)
	for id, p := range a.ids {
		if p != root && !strings.HasPrefix(p, strings.TrimSuffix(root, "/")+"/") {
			continue
		}
		delete(a.ids, id)
		delete(a.dirs, p)
		delete(a.quotas, id)
	}
	a.fabric.endRelsUnder(root)
	return nil
}

func (a *fakeArray) TreeDeleteFinished(ctx context.Context, fileID string) (bool, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return false, err
	}
	_, stillThere := a.ids[fileID]
	return !stillThere, nil
}

func (a *fakeArray) DeleteQuota(ctx context.Context, fileID string) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	if _, ok := a.quotas[fileID]; !ok {
		return fakeAPIError(404, "fs_no_such_entry_error", "no quota on "+fileID)
	}
	delete(a.quotas, fileID)
	return nil
}

func (a *fakeArray) DeleteExport(ctx context.Context, exportID string) error {
	if err := a.unreachableFor(ctx); err != nil {
		return err
	}
	for i, existing := range a.exports {
		if existing.ID == exportID {
			a.exports = append(a.exports[:i], a.exports[i+1:]...)
			return nil
		}
	}
	return fakeAPIError(404, "http_not_found_error", "no export "+exportID)
}

// endRelsUnder is the measured consequence of deleting a replication root:
// ENDED on both ends, source_root_path emptied, the copy left behind.
func (f *fakeFabric) endRelsUnder(root string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	under := func(p string) bool {
		return p != "" && (p == root || strings.HasPrefix(p, strings.TrimSuffix(root, "/")+"/"))
	}
	for _, r := range f.rels {
		if r.state == qumulo.RelationshipEnded {
			continue
		}
		switch {
		case under(r.sourcePath):
			r.sourcePath = ""
			r.state = qumulo.RelationshipEnded
			r.endReason = "REPLICATION_END_REASON_SOURCE_DIRECTORY_DELETED"
		case under(r.targetPath):
			r.targetPath = ""
			r.state = qumulo.RelationshipEnded
			r.endReason = "REPLICATION_END_REASON_TARGET_DIRECTORY_DELETED"
		}
	}
}

// --- Test scenario helpers -------------------------------------------------

// findRel returns the single relationship in the fabric, for assertions.
func (f *fakeFabric) onlyRel() *fakeRel {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.rels) != 1 {
		panic(fmt.Sprintf("fake fabric holds %d relationships, want 1", len(f.rels)))
	}
	for _, r := range f.rels {
		return r
	}
	return nil
}

// relByPath returns the one relationship rooted at sourcePath, or fails.
func (f *fakeFabric) relByPath(sourcePath string) *fakeRel {
	f.mu.Lock()
	defer f.mu.Unlock()
	var found *fakeRel
	for _, r := range f.rels {
		if r.sourcePath == sourcePath && r.hasSource {
			if found != nil {
				panic("relByPath: more than one relationship rooted at " + sourcePath)
			}
			found = r
		}
	}
	if found == nil {
		panic("relByPath: no relationship rooted at " + sourcePath)
	}
	return found
}

func (f *fakeFabric) relCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rels)
}

// seedRel puts a relationship straight into a given row of a state table,
// without walking the transitions that would normally lead there.
func (f *fakeFabric) seedRel(source, target *fakeArray, sourcePath, targetPath, state string, mutate func(*fakeRel)) *fakeRel {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	r := &fakeRel{
		id: fmt.Sprintf("rel-%d", f.nextID), source: source, target: target,
		sourcePath: sourcePath, targetPath: targetPath, targetAddress: target.addr,
		state: state, enabled: true, mode: qumulo.ReplicationModeContinuous,
		hasSource: true, hasTarget: true, targetRO: true,
	}
	if state == qumulo.RelationshipEstablished || state == qumulo.RelationshipDisconnected {
		r.recoveryPoint = time.Now().Add(-time.Second)
	}
	if mutate != nil {
		mutate(r)
	}
	f.rels[r.id] = r
	return r
}

// takeRevertSnapshotLocked is the snapshot a real array takes of a target
// root before reverting it: "<id>_revert_to_<recovery point>_for_<name>",
// no expiration. Only with revertSnapshots set.
func (a *fakeArray) takeRevertSnapshotLocked(r *fakeRel) {
	if !a.revertSnapshots {
		return
	}
	a.fabric.nextSnapID++
	id := strconv.Itoa(a.fabric.nextSnapID)
	a.snapshots = append(a.snapshots, qumulo.SnapshotInfo{
		ID: id, Name: id + "_revert_to_" + strconv.Itoa(a.fabric.nextSnapID-1) + "_for_" + path.Base(r.targetPath),
		SourceFileID: a.dirs[r.targetPath], CreatedAt: a.now(),
	})
}

// SetSnapshotExpiration makes fakeArray a snapshotExpirer.
func (a *fakeArray) SetSnapshotExpiration(ctx context.Context, id string, expiration time.Time) (qumulo.SnapshotInfo, error) {
	if err := a.unreachableFor(ctx); err != nil {
		return qumulo.SnapshotInfo{}, err
	}
	a.record("SetSnapshotExpiration")
	if a.expirationFault != nil {
		return qumulo.SnapshotInfo{}, a.expirationFault
	}
	a.fabric.mu.Lock()
	defer a.fabric.mu.Unlock()
	for i := range a.snapshots {
		if a.snapshots[i].ID == id {
			a.snapshots[i].Expiration = expiration
			return a.snapshots[i], nil
		}
	}
	return qumulo.SnapshotInfo{}, fakeAPIError(http.StatusNotFound, "fs_no_such_snapshot_error", "no snapshot "+id)
}
