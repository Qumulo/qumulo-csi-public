// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// MockClient is a hand-rolled test double for qumulo.ClientInterface.
//
// Every method is backed by an optional function field so a test can script
// just the calls it cares about; an unset field succeeds with a zero value
// (or the default documented on the method). That keeps each test focused
// on one behavior instead of restating the whole interface -- at the price
// that a call the test never expected, to a method it never scripted,
// passes silently. Strict removes that for a test that wants every call
// accounted for. The mock also records the context it was last called with
// so tests can assert that cancellation/deadlines are plumbed through, and
// counts every call by method (Calls).
type MockClient struct {
	err    error
	gotCtx context.Context

	// strict, set by Strict, reports every call to a method whose Func
	// field is unset as a test error naming the method. calls counts every
	// call by method name, in either mode; read it with Calls.
	strict testing.TB
	mu     sync.Mutex
	calls  map[string]int

	GetCapacityFunc func(ctx context.Context) (qumulo.FileSystemCapacity, error)

	CreateDirectoryFunc  func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error)
	LookupFileByPathFunc func(ctx context.Context, path string) (qumulo.FileInfo, error)
	ResolveFileIDsFunc   func(ctx context.Context, fileIDs []string) (map[string]string, error)
	// SetFileAttributesByIDFunc is optional: when it is nil the update
	// succeeds.
	SetFileAttributesByIDFunc func(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error

	StartTreeDeleteFunc    func(ctx context.Context, fileID string) error
	TreeDeleteFinishedFunc func(ctx context.Context, fileID string) (bool, error)

	CreateQuotaFunc func(ctx context.Context, fileID string, limitBytes uint64) error
	GetQuotaFunc    func(ctx context.Context, fileID string) (uint64, error)
	// GetTreeUsageFunc is optional: when it is nil the tree reads as empty,
	// so a restore's size checks pass.
	GetTreeUsageFunc func(ctx context.Context, fileID, snapshotID string) (qumulo.TreeUsage, error)
	// GetQuotaInfoFunc is optional: when it is nil, GetQuotaInfo answers from
	// GetQuotaFunc with the usage unknown, which is exactly how a cluster that
	// does not report capacity_usage behaves. That keeps every existing test
	// meaningful without restating its quota stub.
	GetQuotaInfoFunc func(ctx context.Context, fileID string) (qumulo.QuotaInfo, error)
	UpdateQuotaFunc  func(ctx context.Context, fileID string, limitBytes uint64) error
	DeleteQuotaFunc  func(ctx context.Context, fileID string) error

	CreateSnapshotFunc         func(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error)
	CreateExpiringSnapshotFunc func(ctx context.Context, fileID, name string, expiration time.Time) (qumulo.SnapshotInfo, error)
	SnapshotDiffEmptyFunc      func(ctx context.Context, newerID, olderID string) (bool, error)
	DeleteSnapshotFunc         func(ctx context.Context, snapshotID string) error
	ListSnapshotsFunc          func(ctx context.Context) ([]qumulo.SnapshotInfo, error)
	GetSnapshotFunc            func(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error)

	GetNFSSettingsFunc func(ctx context.Context) (qumulo.NFSSettings, error)

	// ProtectionVerdicts is every value passed to ObserveVolumeProtection.
	ProtectionVerdicts []bool

	ListDirectoryFunc func(ctx context.Context, path, snapshotID string) ([]qumulo.DirEntry, error)
	CreateFileFunc    func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error)
	CreateLinkFunc    func(ctx context.Context, parentPath, name, existingPath string) (qumulo.FileInfo, error)
	// SetFileAttributesFunc is optional: when it is nil the update succeeds.
	SetFileAttributesFunc func(ctx context.Context, filePath string, attrs qumulo.FileAttributes) error
	CopyFileFunc          func(ctx context.Context, targetPath, sourceID, sourceSnapshot string, offset uint64) error
	RenameFunc            func(ctx context.Context, parentPath, name, oldPath string) (qumulo.FileInfo, error)

	CreatePolicyFunc func(ctx context.Context, name, sourceFileID string, schedule qumulo.Schedule) (qumulo.PolicyInfo, error)
	ListPoliciesFunc func(ctx context.Context) ([]qumulo.PolicyInfo, error)
	DeletePolicyFunc func(ctx context.Context, policyID string) error

	CreateExportFunc func(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error)
	ListTenantsFunc  func(ctx context.Context) ([]qumulo.TenantInfo, error)
	ClusterIDFunc    func(ctx context.Context) (string, error)
	ListNetworksFunc func(ctx context.Context) ([]qumulo.NetworkInfo, error)
	DeleteExportFunc func(ctx context.Context, exportID string) error
	ListExportsFunc  func(ctx context.Context) ([]qumulo.ExportInfo, error)

	// The replication verbs. They are here, rather than only on the richer
	// fakeArray in replication_fabric_test.go, because DeleteVolume and
	// ControllerExpandVolume now read the relationship table on every call
	// and refuse outright when the client they were handed cannot (see
	// replication_guard.go). An unset field answers "no relationships",
	// which is what every non-replication test means.
	ListSourceStatusFunc func(ctx context.Context) ([]qumulo.RelationshipStatus, error)
	ListTargetStatusFunc func(ctx context.Context) ([]qumulo.RelationshipStatus, error)
	DeleteSourceFunc     func(ctx context.Context, id string) error
	DeleteTargetFunc     func(ctx context.Context, id string) error
}

var (
	_ qumulo.ClientInterface   = (*MockClient)(nil)
	_ qumulo.ReplicationClient = (*MockClient)(nil)
)

// Strict puts f in strict mode for t and returns f: a call to a method whose
// Func field is unset (or, for a method with no Func field, any call) is
// reported with t.Errorf naming the method, and then answered with the
// default as before. Errorf rather than Fatalf because the driver calls the
// client from goroutines other than the test's. A test in strict mode
// scripts every call the RPC under test makes, so a new call shows up as a
// failure rather than as a silent success.
func (f *MockClient) Strict(t testing.TB) *MockClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.strict = t
	return f
}

// Calls is how many times method (e.g. "CreateQuota") was called.
func (f *MockClient) Calls(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[method]
}

// errorRecorder is a testing.TB whose Errorf is recorded rather than
// failing the test, for checking what strict mode reports.
type errorRecorder struct {
	testing.TB
	mu   sync.Mutex
	errs []string
}

func (r *errorRecorder) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

// A strict MockClient reports a call to an unscripted method by name and
// answers a scripted one silently; both modes count every call.
func TestStrictMockClientReportsAnUnscriptedCallByName(t *testing.T) {
	ctx := context.Background()
	rec := &errorRecorder{TB: t}
	m := (&MockClient{
		CreateQuotaFunc: func(context.Context, string, uint64) error { return nil },
	}).Strict(rec)

	_ = m.CreateQuota(ctx, "1", 1)
	_ = m.CreateQuota(ctx, "1", 1)
	_, _ = m.ListExports(ctx)
	_ = m.HealthCheck(ctx) // no Func field: any call is unscripted

	if len(rec.errs) != 2 || !strings.Contains(rec.errs[0], "ListExports") ||
		!strings.Contains(rec.errs[1], "HealthCheck") {
		t.Errorf("strict mode reported %q; want exactly ListExports then HealthCheck", rec.errs)
	}
	if m.Calls("CreateQuota") != 2 || m.Calls("ListExports") != 1 || m.Calls("DeleteQuota") != 0 {
		t.Errorf("calls counted: CreateQuota %d, ListExports %d, DeleteQuota %d; want 2, 1, 0",
			m.Calls("CreateQuota"), m.Calls("ListExports"), m.Calls("DeleteQuota"))
	}

	lax := &MockClient{}
	if exports, err := lax.ListExports(ctx); err != nil || len(exports) == 0 {
		t.Errorf("default mode answered (%v, %v); want the default covering export", exports, err)
	}
	if lax.Calls("ListExports") != 1 {
		t.Errorf("default mode counted %d ListExports calls, want 1", lax.Calls("ListExports"))
	}
}

// record counts a call to method and, in strict mode, reports it when the
// test did not script it.
func (f *MockClient) record(method string, scripted bool) {
	f.mu.Lock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[method]++
	t := f.strict
	f.mu.Unlock()
	if t != nil && !scripted {
		t.Errorf("strict MockClient: %s was called but the test did not script it (set %sFunc)", method, method)
	}
}

func (f *MockClient) HealthCheck(ctx context.Context) error {
	f.record("HealthCheck", false)
	f.gotCtx = ctx
	return f.err
}

func (f *MockClient) GetCapacity(ctx context.Context) (qumulo.FileSystemCapacity, error) {
	f.record("GetCapacity", f.GetCapacityFunc != nil)
	f.gotCtx = ctx
	if f.GetCapacityFunc != nil {
		return f.GetCapacityFunc(ctx)
	}
	return qumulo.FileSystemCapacity{}, f.err
}

func (f *MockClient) CreateDirectory(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
	f.record("CreateDirectory", f.CreateDirectoryFunc != nil)
	f.gotCtx = ctx
	if f.CreateDirectoryFunc == nil {
		return qumulo.FileInfo{}, nil
	}
	return f.CreateDirectoryFunc(ctx, parentPath, name)
}

// LookupFileByPath answers, by default, as a cluster on which the path exists
// and is a directory carrying testFileID.
//
// The default matters more than it used to: a volume handle names a path
// (volume_id.go), so this call is how DeleteVolume, ControllerExpandVolume,
// ValidateVolumeCapabilities, CreateSnapshot and the replication service all
// establish that the volume is there and learn this array's file ID for it.
// Returning a zero FileInfo -- which is what this did when the handle carried
// its own file ID and nothing resolved by path -- would read as "the path is
// not a directory" and quietly turn every one of those tests into a refusal.
// A test that means the volume to be absent says so with missingVolume().
func (f *MockClient) LookupFileByPath(ctx context.Context, path string) (qumulo.FileInfo, error) {
	f.record("LookupFileByPath", f.LookupFileByPathFunc != nil)
	f.gotCtx = ctx
	if f.LookupFileByPathFunc == nil {
		return qumulo.FileInfo{ID: testFileID, Path: path, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
	}
	return f.LookupFileByPathFunc(ctx, path)
}

func (f *MockClient) ResolveFileIDs(ctx context.Context, fileIDs []string) (map[string]string, error) {
	f.record("ResolveFileIDs", f.ResolveFileIDsFunc != nil)
	f.gotCtx = ctx
	if f.ResolveFileIDsFunc == nil {
		return map[string]string{}, nil
	}
	return f.ResolveFileIDsFunc(ctx, fileIDs)
}

func (f *MockClient) StartTreeDelete(ctx context.Context, fileID string) error {
	f.record("StartTreeDelete", f.StartTreeDeleteFunc != nil)
	f.gotCtx = ctx
	if f.StartTreeDeleteFunc == nil {
		return nil
	}
	return f.StartTreeDeleteFunc(ctx, fileID)
}

// TreeDeleteFinished defaults to reporting the job already finished, so a
// test that does not care about the polling loop is not made to script it.
func (f *MockClient) TreeDeleteFinished(ctx context.Context, fileID string) (bool, error) {
	f.record("TreeDeleteFinished", f.TreeDeleteFinishedFunc != nil)
	f.gotCtx = ctx
	if f.TreeDeleteFinishedFunc == nil {
		return true, nil
	}
	return f.TreeDeleteFinishedFunc(ctx, fileID)
}

func (f *MockClient) CreateQuota(ctx context.Context, fileID string, limitBytes uint64) error {
	f.record("CreateQuota", f.CreateQuotaFunc != nil)
	f.gotCtx = ctx
	if f.CreateQuotaFunc == nil {
		return nil
	}
	return f.CreateQuotaFunc(ctx, fileID, limitBytes)
}

func (f *MockClient) GetQuota(ctx context.Context, fileID string) (uint64, error) {
	f.record("GetQuota", f.GetQuotaFunc != nil)
	f.gotCtx = ctx
	if f.GetQuotaFunc == nil {
		return 0, nil
	}
	return f.GetQuotaFunc(ctx, fileID)
}

func (f *MockClient) GetTreeUsage(ctx context.Context, fileID, snapshotID string) (qumulo.TreeUsage, error) {
	f.record("GetTreeUsage", f.GetTreeUsageFunc != nil)
	f.gotCtx = ctx
	if f.GetTreeUsageFunc == nil {
		return qumulo.TreeUsage{}, nil
	}
	return f.GetTreeUsageFunc(ctx, fileID, snapshotID)
}

func (f *MockClient) GetQuotaInfo(ctx context.Context, fileID string) (qumulo.QuotaInfo, error) {
	f.record("GetQuotaInfo", f.GetQuotaInfoFunc != nil || f.GetQuotaFunc != nil)
	f.gotCtx = ctx
	if f.GetQuotaInfoFunc != nil {
		return f.GetQuotaInfoFunc(ctx, fileID)
	}

	// Fall back to the limit-only stub. UsageKnown stays false, so a caller
	// that has not been taught about usage sees the pre-existing behaviour.
	// GetQuotaFunc directly rather than GetQuota, so the call counts once.
	if f.GetQuotaFunc == nil {
		return qumulo.QuotaInfo{}, nil
	}
	limit, err := f.GetQuotaFunc(ctx, fileID)
	if err != nil {
		return qumulo.QuotaInfo{}, err
	}
	return qumulo.QuotaInfo{Limit: limit}, nil
}

func (f *MockClient) UpdateQuota(ctx context.Context, fileID string, limitBytes uint64) error {
	f.record("UpdateQuota", f.UpdateQuotaFunc != nil)
	f.gotCtx = ctx
	if f.UpdateQuotaFunc == nil {
		return nil
	}
	return f.UpdateQuotaFunc(ctx, fileID, limitBytes)
}

func (f *MockClient) DeleteQuota(ctx context.Context, fileID string) error {
	f.record("DeleteQuota", f.DeleteQuotaFunc != nil)
	f.gotCtx = ctx
	if f.DeleteQuotaFunc == nil {
		return nil
	}
	return f.DeleteQuotaFunc(ctx, fileID)
}

func (f *MockClient) CreateSnapshot(ctx context.Context, fileID, name string) (qumulo.SnapshotInfo, error) {
	f.record("CreateSnapshot", f.CreateSnapshotFunc != nil)
	f.gotCtx = ctx
	if f.CreateSnapshotFunc == nil {
		return qumulo.SnapshotInfo{}, nil
	}
	return f.CreateSnapshotFunc(ctx, fileID, name)
}

func (f *MockClient) CreateExpiringSnapshot(ctx context.Context, fileID, name string, expiration time.Time) (qumulo.SnapshotInfo, error) {
	f.record("CreateExpiringSnapshot", f.CreateExpiringSnapshotFunc != nil)
	f.gotCtx = ctx
	if f.CreateExpiringSnapshotFunc == nil {
		return qumulo.SnapshotInfo{ID: "1", Name: "1_" + name, SourceFileID: fileID, CreatedAt: time.Now()}, nil
	}
	return f.CreateExpiringSnapshotFunc(ctx, fileID, name, expiration)
}

func (f *MockClient) SnapshotDiffEmpty(ctx context.Context, newerID, olderID string) (bool, error) {
	f.record("SnapshotDiffEmpty", f.SnapshotDiffEmptyFunc != nil)
	f.gotCtx = ctx
	if f.SnapshotDiffEmptyFunc == nil {
		return true, nil
	}
	return f.SnapshotDiffEmptyFunc(ctx, newerID, olderID)
}

func (f *MockClient) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	f.record("DeleteSnapshot", f.DeleteSnapshotFunc != nil)
	f.gotCtx = ctx
	if f.DeleteSnapshotFunc == nil {
		return nil
	}
	return f.DeleteSnapshotFunc(ctx, snapshotID)
}

func (f *MockClient) ListSnapshots(ctx context.Context) ([]qumulo.SnapshotInfo, error) {
	f.record("ListSnapshots", f.ListSnapshotsFunc != nil)
	f.gotCtx = ctx
	if f.ListSnapshotsFunc == nil {
		return nil, nil
	}
	return f.ListSnapshotsFunc(ctx)
}

func (f *MockClient) GetSnapshot(ctx context.Context, snapshotID string) (qumulo.SnapshotInfo, error) {
	f.record("GetSnapshot", f.GetSnapshotFunc != nil)
	f.gotCtx = ctx
	if f.GetSnapshotFunc == nil {
		// Named as csi-snapshotter names this driver's snapshots, so a test
		// that sets nothing reaches a snapshot DeleteSnapshot and a restore
		// take for the driver's own.
		return qumulo.SnapshotInfo{ID: snapshotID, Name: snapshotID + "_snapshot-mock"}, nil
	}
	return f.GetSnapshotFunc(ctx, snapshotID)
}

func (f *MockClient) CreatePolicy(
	ctx context.Context,
	name, sourceFileID string,
	schedule qumulo.Schedule,
) (qumulo.PolicyInfo, error) {
	f.record("CreatePolicy", f.CreatePolicyFunc != nil)
	f.gotCtx = ctx
	if f.CreatePolicyFunc == nil {
		return qumulo.PolicyInfo{}, nil
	}
	return f.CreatePolicyFunc(ctx, name, sourceFileID, schedule)
}

func (f *MockClient) ListPolicies(ctx context.Context) ([]qumulo.PolicyInfo, error) {
	f.record("ListPolicies", f.ListPoliciesFunc != nil)
	f.gotCtx = ctx
	if f.ListPoliciesFunc == nil {
		return nil, nil
	}
	return f.ListPoliciesFunc(ctx)
}

func (f *MockClient) DeletePolicy(ctx context.Context, policyID string) error {
	f.record("DeletePolicy", f.DeletePolicyFunc != nil)
	f.gotCtx = ctx
	if f.DeletePolicyFunc == nil {
		return nil
	}
	return f.DeletePolicyFunc(ctx, policyID)
}

func (f *MockClient) CreateExport(ctx context.Context, spec qumulo.ExportSpec) (qumulo.ExportInfo, error) {
	f.record("CreateExport", f.CreateExportFunc != nil)
	f.gotCtx = ctx
	if f.CreateExportFunc == nil {
		return qumulo.ExportInfo{}, nil
	}
	return f.CreateExportFunc(ctx, spec)
}

func (f *MockClient) ListTenants(ctx context.Context) ([]qumulo.TenantInfo, error) {
	f.record("ListTenants", f.ListTenantsFunc != nil)
	f.gotCtx = ctx
	if f.ListTenantsFunc == nil {
		return nil, nil
	}
	return f.ListTenantsFunc(ctx)
}

func (f *MockClient) ListNetworks(ctx context.Context) ([]qumulo.NetworkInfo, error) {
	f.record("ListNetworks", f.ListNetworksFunc != nil)
	f.gotCtx = ctx
	if f.ListNetworksFunc == nil {
		return nil, nil
	}
	return f.ListNetworksFunc(ctx)
}

func (f *MockClient) DeleteExport(ctx context.Context, exportID string) error {
	f.record("DeleteExport", f.DeleteExportFunc != nil)
	f.gotCtx = ctx
	if f.DeleteExportFunc == nil {
		return nil
	}
	return f.DeleteExportFunc(ctx, exportID)
}

func (f *MockClient) ListExports(ctx context.Context) ([]qumulo.ExportInfo, error) {
	f.record("ListExports", f.ListExportsFunc != nil)
	f.gotCtx = ctx
	if f.ListExportsFunc == nil {
		// The default is a cluster where the shared-subdirectory prerequisite
		// has been met, because that is what a working deployment looks like:
		// an export covering the class's store path, which the administrator
		// creates before the first PVC. Returning nothing here would make
		// every shared-model test exercise the refusal path instead of the
		// behaviour it is about.
		//
		// A test that means to check the refusal sets ListExportsFunc to
		// return no covering export -- see noCoveringExport.
		return defaultCoveringExports(), nil
	}
	return f.ListExportsFunc(ctx)
}

// defaultCoveringExports is the export testParams needs to be mountable:
// /csi on the NFS side mapping to /csi-volumes on the filesystem side, which
// is exactly the storeExportPath and storeRealPath pair testParams sets.
//
// A test whose class uses a different path layout needs its own table --
// exportsCovering builds one. Longest-match resolution across a nested table
// is asserted directly in TestCheckSharedExportCoverage rather than left to
// whatever this default happens to contain.
func defaultCoveringExports() []qumulo.ExportInfo {
	return []qumulo.ExportInfo{{
		ID:         "1",
		ExportPath: "/csi",
		FSPath:     testStoreRealPath,
	}}
}

// exportsCovering builds a one-entry export table mapping exportPath to
// fsPath, for a test whose class does not use testParams' path layout.
func exportsCovering(exportPath, fsPath string) func(context.Context) ([]qumulo.ExportInfo, error) {
	return func(context.Context) ([]qumulo.ExportInfo, error) {
		return []qumulo.ExportInfo{{ID: "1", ExportPath: exportPath, FSPath: fsPath}}, nil
	}
}

// noCoveringExport makes the cluster answer with an export table that reaches
// nothing this driver provisions, which is the state a reported bug
// was about: a class whose basePath has no export and no ancestor export.
func noCoveringExport(ctx context.Context) ([]qumulo.ExportInfo, error) {
	return []qumulo.ExportInfo{{
		ID:         "9",
		ExportPath: "/unrelated",
		FSPath:     "/unrelated",
	}}, nil
}

func (f *MockClient) SetFileAttributesByID(ctx context.Context, fileID string, attrs qumulo.FileAttributes) error {
	f.record("SetFileAttributesByID", f.SetFileAttributesByIDFunc != nil)
	f.gotCtx = ctx
	if f.SetFileAttributesByIDFunc == nil {
		return nil
	}
	return f.SetFileAttributesByIDFunc(ctx, fileID, attrs)
}

func (f *MockClient) ListDirectory(ctx context.Context, path, snapshotID string) ([]qumulo.DirEntry, error) {
	f.record("ListDirectory", f.ListDirectoryFunc != nil)
	f.gotCtx = ctx
	if f.ListDirectoryFunc != nil {
		return f.ListDirectoryFunc(ctx, path, snapshotID)
	}
	return nil, f.err
}

func (f *MockClient) CreateFile(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
	f.record("CreateFile", f.CreateFileFunc != nil)
	f.gotCtx = ctx
	if f.CreateFileFunc != nil {
		return f.CreateFileFunc(ctx, parentPath, name)
	}
	return qumulo.FileInfo{}, f.err
}

func (f *MockClient) SetFileAttributes(ctx context.Context, filePath string, attrs qumulo.FileAttributes) error {
	f.record("SetFileAttributes", f.SetFileAttributesFunc != nil)
	f.gotCtx = ctx
	if f.SetFileAttributesFunc != nil {
		return f.SetFileAttributesFunc(ctx, filePath, attrs)
	}
	return nil
}

func (f *MockClient) CreateLink(ctx context.Context, parentPath, name, existingPath string) (qumulo.FileInfo, error) {
	f.record("CreateLink", f.CreateLinkFunc != nil)
	f.gotCtx = ctx
	if f.CreateLinkFunc != nil {
		return f.CreateLinkFunc(ctx, parentPath, name, existingPath)
	}
	return qumulo.FileInfo{}, f.err
}

func (f *MockClient) CopyFile(ctx context.Context, targetPath, sourceID, sourceSnapshot string, offset uint64) error {
	f.record("CopyFile", f.CopyFileFunc != nil)
	f.gotCtx = ctx
	if f.CopyFileFunc != nil {
		return f.CopyFileFunc(ctx, targetPath, sourceID, sourceSnapshot, offset)
	}
	return f.err
}

func (f *MockClient) Rename(ctx context.Context, parentPath, name, oldPath string) (qumulo.FileInfo, error) {
	f.record("Rename", f.RenameFunc != nil)
	f.gotCtx = ctx
	if f.RenameFunc != nil {
		return f.RenameFunc(ctx, parentPath, name, oldPath)
	}
	return qumulo.FileInfo{}, f.err
}

func (f *MockClient) GetNFSSettings(ctx context.Context) (qumulo.NFSSettings, error) {
	f.record("GetNFSSettings", f.GetNFSSettingsFunc != nil)
	f.gotCtx = ctx
	if f.GetNFSSettingsFunc != nil {
		return f.GetNFSSettingsFunc(ctx)
	}
	// Unset means a cluster with NFSv4 on, so existing tests that provision
	// nfs4.1 volumes keep working without restating the whole interface.
	return qumulo.NFSSettings{V4Enabled: true, AuthSysEnabled: true}, f.err
}

// --- qumulo.ReplicationClient ---------------------------------------------

// ObserveVolumeProtection records the last protection verdict the driver
// stated, so a test can assert the signal was emitted at all.
func (f *MockClient) ObserveVolumeProtection(_ context.Context, protected bool) {
	f.record("ObserveVolumeProtection", true)
	f.ProtectionVerdicts = append(f.ProtectionVerdicts, protected)
}

// ObservePolicyInterval records nothing; the replication service's use of it
// is tested against fakeArray.
func (f *MockClient) ObservePolicyInterval(context.Context, string, time.Duration) {
	f.record("ObservePolicyInterval", true)
}

//
// Only the four verbs the controller RPCs reach are wired to fields. The rest
// exist to satisfy the interface and return zero values: the replication
// service itself is tested against fakeArray, which models the whole state
// machine.

func (f *MockClient) ListSourceStatus(ctx context.Context) ([]qumulo.RelationshipStatus, error) {
	f.record("ListSourceStatus", f.ListSourceStatusFunc != nil)
	f.gotCtx = ctx
	if f.ListSourceStatusFunc != nil {
		return f.ListSourceStatusFunc(ctx)
	}
	return nil, nil
}

func (f *MockClient) ListTargetStatus(ctx context.Context) ([]qumulo.RelationshipStatus, error) {
	f.record("ListTargetStatus", f.ListTargetStatusFunc != nil)
	f.gotCtx = ctx
	if f.ListTargetStatusFunc != nil {
		return f.ListTargetStatusFunc(ctx)
	}
	return nil, nil
}

func (f *MockClient) DeleteSource(ctx context.Context, id string) error {
	f.record("DeleteSource", f.DeleteSourceFunc != nil)
	f.gotCtx = ctx
	if f.DeleteSourceFunc != nil {
		return f.DeleteSourceFunc(ctx, id)
	}
	return f.err
}

func (f *MockClient) DeleteTarget(ctx context.Context, id string) error {
	f.record("DeleteTarget", f.DeleteTargetFunc != nil)
	f.gotCtx = ctx
	if f.DeleteTargetFunc != nil {
		return f.DeleteTargetFunc(ctx, id)
	}
	return f.err
}

func (f *MockClient) GetSourceStatus(ctx context.Context, _ string) (qumulo.RelationshipStatus, error) {
	f.record("GetSourceStatus", false)
	f.gotCtx = ctx
	return qumulo.RelationshipStatus{}, f.err
}

func (f *MockClient) GetTargetStatus(ctx context.Context, _ string) (qumulo.RelationshipStatus, error) {
	f.record("GetTargetStatus", false)
	f.gotCtx = ctx
	return qumulo.RelationshipStatus{}, f.err
}

func (f *MockClient) CreateSource(ctx context.Context, _ qumulo.SourceSpec) (qumulo.Relationship, error) {
	f.record("CreateSource", false)
	f.gotCtx = ctx
	return qumulo.Relationship{}, f.err
}

func (f *MockClient) PatchSource(ctx context.Context, _ string, _ qumulo.SourcePatch) (qumulo.Relationship, error) {
	f.record("PatchSource", false)
	f.gotCtx = ctx
	return qumulo.Relationship{}, f.err
}

func (f *MockClient) Replicate(ctx context.Context, _ string) error {
	f.record("Replicate", false)
	f.gotCtx = ctx
	return f.err
}

func (f *MockClient) Authorize(ctx context.Context, _ string, _, _ bool) (qumulo.RelationshipStatus, error) {
	f.record("Authorize", false)
	f.gotCtx = ctx
	return qumulo.RelationshipStatus{}, f.err
}

func (f *MockClient) MakeTargetWritable(ctx context.Context, _ string) (qumulo.RelationshipStatus, error) {
	f.record("MakeTargetWritable", false)
	f.gotCtx = ctx
	return qumulo.RelationshipStatus{}, f.err
}

func (f *MockClient) Reconnect(ctx context.Context, _ string) (qumulo.RelationshipStatus, error) {
	f.record("Reconnect", false)
	f.gotCtx = ctx
	return qumulo.RelationshipStatus{}, f.err
}

func (f *MockClient) Reverse(ctx context.Context, _, _ string, _ int) (qumulo.Relationship, error) {
	f.record("Reverse", false)
	f.gotCtx = ctx
	return qumulo.Relationship{}, f.err
}

// ClusterID answers ClusterIDFunc, or "" (unknown) when it is unset, so a
// test that does not care is never told two endpoints are different clusters.
func (f *MockClient) ClusterID(ctx context.Context) (string, error) {
	f.record("ClusterID", f.ClusterIDFunc != nil)
	f.gotCtx = ctx
	if f.ClusterIDFunc != nil {
		return f.ClusterIDFunc(ctx)
	}
	return "", f.err
}

func (f *MockClient) ClusterTime(ctx context.Context) (time.Time, error) {
	f.record("ClusterTime", false)
	f.gotCtx = ctx
	return time.Time{}, f.err
}

func (f *MockClient) GetPolicySchedule(ctx context.Context, _ string) (qumulo.Schedule, error) {
	f.record("GetPolicySchedule", false)
	f.gotCtx = ctx
	return qumulo.Schedule{}, f.err
}

func (f *MockClient) SetExportReadOnly(ctx context.Context, _ string, _ bool) error {
	f.record("SetExportReadOnly", false)
	f.gotCtx = ctx
	return f.err
}
