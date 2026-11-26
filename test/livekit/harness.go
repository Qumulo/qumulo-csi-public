// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package livekit is the Harness shared by every suite that drives the real
// driver against a real Qumulo cluster.
//
// It was lifted out of test/live when the OpenShift tier needed the same
// Harness: a _test.go file cannot be imported, so the only way for two
// packages to drive one cluster the same way is for the Harness to live in an
// ordinary package. Nothing here is specific to either suite.
//
// Importing this package does not run anything. Every entry point is gated on
// QUMULO_LIVE_TEST=1 and skips without it, so a stray `go test ./...` reaches
// no one's cluster -- and the suites that use it are behind build tags on top
// of that.
package livekit

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"qumulo-csi-driver/pkg/driver"
	"qumulo-csi-driver/pkg/qumulo"
)

// GiB is one gibibyte, the unit these tests request capacity in.
const GiB = int64(1) << 30

// LiveTimeout is the per-request budget the Harness's own cluster client gets.
//
// The package default is 10s, which is too tight for the calls these tests
// make against a real cluster (a tree-delete or a quota update on a populated
// directory is not a local operation).
const LiveTimeout = 30 * time.Second

// SafetyMarker must appear in QUMULO_STORE_REAL_PATH.
//
// These tests tree-delete what they create. Requiring a distinctive component
// in the path is what stops a mistyped environment variable from pointing the
// cleanup at a real dataset.
const SafetyMarker = "csi-live-test"

// Harness is one live test's controller, cluster client and cleanup list.
type Harness struct {
	T *testing.T

	// Client talks to the cluster directly, for assertions the CSI API cannot
	// make and for cleanup the driver does not do.
	Client *qumulo.Client

	Driver *driver.Driver

	StoreRealPath   string
	StoreExportPath string
	Token           string

	// APIURL and NFSServer are the two addresses a StorageClass carries. The
	// controller holds neither: it resolves both per request from these
	// parameters, so they live on the Harness rather than in a driver config.
	APIURL    string
	NFSServer string
}

// NewHarness builds a controller wired to the real cluster, or skips.
//
// Every precondition is checked here rather than in each test, so a missing
// variable reads as "skipped, and why" instead of a nil dereference deep in a
// test body.
func NewHarness(t *testing.T) *Harness {
	t.Helper()

	if os.Getenv("QUMULO_LIVE_TEST") != "1" {
		t.Skip("live tests need QUMULO_LIVE_TEST=1 and a real cluster; see the Makefile's live-* targets")
	}

	apiURL := os.Getenv("QUMULO_API_URL")
	tokenFile := os.Getenv("QUMULO_TOKEN_FILE")
	realPath := os.Getenv("QUMULO_STORE_REAL_PATH")
	exportPath := os.Getenv("QUMULO_STORE_EXPORT_PATH")

	for name, value := range map[string]string{
		"QUMULO_API_URL":           apiURL,
		"QUMULO_TOKEN_FILE":        tokenFile,
		"QUMULO_STORE_REAL_PATH":   realPath,
		"QUMULO_STORE_EXPORT_PATH": exportPath,
	} {
		if value == "" {
			t.Skipf("%s is required for live tests", name)
		}
	}

	// The guard that makes tree-delete cleanup safe to run unattended.
	if !strings.Contains(realPath, SafetyMarker) {
		t.Fatalf("QUMULO_STORE_REAL_PATH %q must contain %q: these tests tree-delete what they create",
			realPath, SafetyMarker)
	}

	rawToken, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatalf("reading QUMULO_TOKEN_FILE: %v", err)
	}
	token, err := ExtractToken(rawToken)
	if err != nil {
		t.Fatalf("QUMULO_TOKEN_FILE %q: %v", tokenFile, err)
	}

	// One transport for both the Harness's assertions and the driver's
	// per-request clients, so a TLS policy chosen here cannot differ between
	// what the driver does and what the test checks afterwards.
	insecure := os.Getenv("QUMULO_TLS_INSECURE") == "1"
	if insecure {
		t.Log("WARNING: TLS certificate verification disabled via QUMULO_TLS_INSECURE=1 (test-only)")
	}
	httpClient := &http.Client{
		Timeout: LiveTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure, MinVersion: tls.VersionTLS12},
		},
	}

	client := qumulo.New(apiURL, token)
	client.HTTPClient = httpClient

	// The data address is only ever stamped into a volume ID and a mount
	// source here; nothing in these tests mounts, so the API host is a fine
	// stand-in and keeps the Harness to one environment variable.
	nfsServer := strings.TrimPrefix(strings.TrimPrefix(apiURL, "https://"), "http://")
	if i := strings.IndexByte(nfsServer, ':'); i >= 0 {
		nfsServer = nfsServer[:i]
	}

	h := &Harness{
		T:               t,
		Client:          client,
		StoreRealPath:   realPath,
		StoreExportPath: exportPath,
		Token:           token,
		APIURL:          apiURL,
		NFSServer:       nfsServer,
	}

	// No cluster address and no client: the controller builds one per request
	// from the StorageClass parameters params() returns and the token
	// secrets() carries. HTTPClient is the transport those clients inherit,
	// which is the only way a test can hand them a TLS policy.
	d, err := driver.NewDriver(driver.Config{
		Name:       "csi.qumulo.com",
		Version:    "live-test",
		HTTPClient: httpClient,
		// ModeAll, and stated rather than left to the zero value. An unset
		// Mode parses to ModeController, which builds no NodeServer at all --
		// so every TestLiveNode* test called a method on a nil *NodeServer and
		// died with a SIGSEGV inside NodePublishVolume, before it had mounted
		// anything or reported a usable failure. `make live-node` had
		// therefore never run to completion on any architecture.
		//
		// ModeAll rather than ModeNode because this Harness genuinely needs
		// both services: the node tests provision their volume through the
		// ControllerServer and only then publish it through the NodeServer.
		//
		// NodeID is required whenever the mode serves the node service, and
		// the hostname is the honest value here -- these tests mount on the
		// machine they run on, so that is the node this instance speaks for.
		Mode:   driver.ModeAll,
		NodeID: LiveNodeID(),
	})
	if err != nil {
		t.Fatalf("building the driver: %v", err)
	}
	h.Driver = d

	return h
}

// loginResponse is the body Qumulo Core's own POST /v1/session/login
// returns. `qq login` and a hand-run curl against that endpoint both save
// this envelope verbatim, so a token file is not guaranteed to be a bare
// token -- see the chart README and pkg/driver, where the Secret key
// "token" IS expected to hold a bare value, which this envelope is not.
type loginResponse struct {
	BearerToken string `json:"bearer_token"`
}

// ExtractToken accepts either a bare bearer token or a
// POST /v1/session/login response envelope, and returns the token itself
// either way. A bearer_token field wins over treating the file as a bare
// token; a token that happens to start with "{" is not a realistic case,
// and a "{"-prefixed file that fails to parse as JSON falls through to the
// bare-token path instead of being rejected outright.
func ExtractToken(raw []byte) (string, error) {
	trimmed := strings.TrimSpace(string(raw))

	if strings.HasPrefix(trimmed, "{") {
		var resp loginResponse
		if err := json.Unmarshal(raw, &resp); err == nil && resp.BearerToken != "" {
			return resp.BearerToken, nil
		}
	}

	if trimmed == "" {
		return "", fmt.Errorf("file is empty")
	}
	return trimmed, nil
}

// ctrl returns the controller service under test.
func (h *Harness) Ctrl() csi.ControllerServer {
	return h.Driver.ControllerServer()
}

// secrets is the credential every controller RPC carries.
//
// Not optional: the driver holds no fallback credential, so an RPC without
// this is refused before it reaches the cluster.
func (h *Harness) Secrets() map[string]string {
	return map[string]string{"token": h.Token}
}

// volumeName returns a name unique to this run, prefixed so leftovers on the
// cluster are attributable to a test rather than to a real workload.
func (h *Harness) VolumeName(prefix string) string {
	return fmt.Sprintf("csi-live-%s-%d", prefix, time.Now().UnixNano())
}

// snapshotName is a unique CSI snapshot name in csi-snapshotter's form,
// "snapshot-...": DeleteSnapshot refuses one named otherwise as foreign,
// and these tests delete what they take.
func (h *Harness) SnapshotName(prefix string) string {
	return driver.DefaultSnapshotNamePrefix + "-" + h.VolumeName(prefix)
}

// params is the StorageClass this Harness provisions with.
//
// server is the data address a node would mount from and apiUrl is the REST
// endpoint; both are per-request, because one deployed controller serves many
// clusters and holds no address of its own. apiUrl is set explicitly rather
// than left to be derived from server, since the test cluster's API is not on
// the assumed default port.
func (h *Harness) Params() map[string]string {
	return map[string]string{
		"server":          h.NFSServer,
		"apiUrl":          h.APIURL,
		"storeRealPath":   h.StoreRealPath,
		"storeExportPath": h.StoreExportPath,
	}
}

// createRequest builds a CreateVolume request for the named volume.
func (h *Harness) CreateRequest(name string, capacity int64) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:          name,
		CapacityRange: &csi.CapacityRange{RequiredBytes: capacity},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		}},
		Parameters: h.Params(),
		Secrets:    h.Secrets(),
	}
}

// track registers a provisioned directory for removal at the end of the test.
//
// Cleanup goes through the cluster client rather than DeleteVolume so that a
// test asserting a failure in the delete path still leaves nothing behind.
// Failures are logged, not fatal: cleanup must not mask the assertion that
// actually failed, but a leak has to be visible in the output.
func (h *Harness) Track(fileID, realPath string) {
	h.T.Helper()

	if !strings.Contains(realPath, SafetyMarker) {
		h.T.Fatalf("refusing to schedule cleanup of %q: it is outside %q", realPath, SafetyMarker)
	}

	h.T.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		if err := h.Client.DeleteQuota(ctx, fileID); err != nil && !qumulo.IsNotFound(err) {
			h.T.Logf("cleanup: removing the quota on %s (%s): %v", realPath, fileID, err)
		}
		if err := h.Client.StartTreeDelete(ctx, fileID); err != nil && !qumulo.IsNotFound(err) {
			h.T.Logf("cleanup: tree-deleting %s (%s): %v -- it may need removing by hand",
				realPath, fileID, err)
		}
	})
}

// requireCoveringExport skips unless an NFS export reaches this Harness's
// store path.
//
// CreateVolume refuses a shared-subdirectory volume that no export covers,
// because provisioning one produces a Bound PVC and a pod that can never
// mount. That is a cluster-setup prerequisite rather than anything a test can
// arrange, so a cluster without it is a skip with instructions and not a
// failure -- the driver would be behaving correctly.
//
// Called by the provisioning tests rather than from NewHarness, because the
// read-only tests here have no such prerequisite and must not be blocked by
// it.
func (h *Harness) RequireCoveringExport(ctx context.Context, t *testing.T) {
	t.Helper()

	exports, err := h.Client.ListExports(ctx)
	if err != nil {
		t.Fatalf("listing exports to check the store path is mountable: %v", err)
	}

	for _, export := range exports {
		exportPath := strings.TrimSuffix(export.ExportPath, "/")
		if exportPath == "" {
			return // a root export covers everything
		}
		if h.StoreExportPath == exportPath || strings.HasPrefix(h.StoreExportPath, exportPath+"/") {
			return
		}
	}

	t.Skipf("no NFS export on this cluster covers QUMULO_STORE_EXPORT_PATH %q, so a "+
		"shared-subdirectory volume there is refused by design. Create an export whose NFS "+
		"path is %q (or an ancestor) pointing at %q and re-run",
		h.StoreExportPath, h.StoreExportPath, h.StoreRealPath)
}

// VolumePathFromID pulls the cluster path out of a CSI volume handle.
//
// The handle is <server>#<apiUrl>#<realPath>. Parsed here rather than imported so a
// change to the driver's format shows up as a test failure instead of the
// test silently agreeing with the driver about a new shape -- which is
// exactly what it did when the handle stopped carrying a file ID.
func VolumePathFromID(t *testing.T, volumeID string) string {
	t.Helper()

	return SplitVolumeID(t, volumeID)[2]
}

// VolumeNameFromID pulls the volume name out of a CSI volume handle. The name
// is the last element of the path; the handle does not carry it separately.
func VolumeNameFromID(t *testing.T, volumeID string) string {
	t.Helper()

	return path.Base(SplitVolumeID(t, volumeID)[2])
}

// fileIDOf resolves the array's own file ID for the directory a handle names.
//
// The handle no longer carries one: a file ID is array-local and replication
// does not preserve it, which is why the driver resolves it at use. Tests that
// need an ID -- cleanup, and the snapshot-policy assertions -- resolve it the
// same way the driver does, against the array the handle names.
func (h *Harness) FileIDOf(t *testing.T, volumeID string) string {
	t.Helper()

	realPath := VolumePathFromID(t, volumeID)
	info, err := h.Client.LookupFileByPath(context.Background(), realPath)
	if err != nil {
		t.Fatalf("resolving %q on the cluster for handle %q: %v", realPath, volumeID, err)
	}
	return info.ID
}

// SplitVolumeID decomposes a handle, failing the test if it is not the shape
// the driver documents: <server>#<apiUrl>#<realPath>, where the middle field
// is empty when the endpoint is the default the server implies.
func SplitVolumeID(t *testing.T, volumeID string) []string {
	t.Helper()

	parts := strings.Split(volumeID, "#")
	if len(parts) != 3 || parts[0] == "" || !strings.HasPrefix(parts[2], "/") {
		t.Fatalf("volume ID %q is not <server>#<apiUrl>#<realPath>", volumeID)
	}
	return parts
}

// handleEndpoint is the middle field the driver writes into handles minted
// from this Harness's class: the class's apiUrl, or empty when that is only
// the default https://<server>:8000 spelled out. Kept in step with the
// driver's normalizeAPIURL by hand, for the reason SplitVolumeID is.
func (h *Harness) HandleEndpoint() string {
	trimmed := strings.TrimSuffix(strings.TrimSpace(h.APIURL), "/")
	if strings.EqualFold(trimmed, "https://"+h.NFSServer+":8000") {
		return ""
	}
	return trimmed
}

// volumeHandle is the handle the driver would mint for realPath on this
// Harness's cluster.
func (h *Harness) VolumeHandle(realPath string) string {
	return h.NFSServer + "#" + h.HandleEndpoint() + "#" + realPath
}

// snapshotHandle is the handle the driver would mint for a cluster snapshot
// ID on this Harness's cluster -- what a pre-provisioned
// VolumeSnapshotContent has to carry.
func (h *Harness) SnapshotHandle(clusterID string) string {
	return h.NFSServer + "#" + h.HandleEndpoint() + "#" + clusterID
}

// ClusterSnapshotID strips the cluster prefix from a CSI snapshot handle,
// leaving the ID the Qumulo API takes.
//
// The handle is <server>#<apiUrl>#<snapshotID>, the volume handle's shape.
// Split here rather than imported, for the same reason SplitVolumeID is: the
// test asserts the format rather than inheriting it.
func ClusterSnapshotID(t *testing.T, snapshotID string) string {
	t.Helper()

	parts := strings.Split(snapshotID, "#")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		t.Fatalf("snapshot ID %q is not <server>#<apiUrl>#<snapshotID>", snapshotID)
	}
	return parts[2]
}

// LiveNodeID is the node identity a live-test Driver is built with.
//
// NewDriver refuses an empty NodeID whenever the mode serves the node service,
// and these tests mount on the machine they run on, so the hostname is the
// accurate answer rather than a placeholder. The fallback matters only on a
// host where os.Hostname fails, where any non-empty value will do: nothing in
// the live suite reads this back, it exists to satisfy the same validation a
// real node plugin passes.
func LiveNodeID() string {
	if name, err := os.Hostname(); err == nil && name != "" {
		return name
	}
	return "live-test-node"
}

// ProvisionedVolume is a volume created by a test, with the handles its
// assertions need.
type ProvisionedVolume struct {
	VolumeID string
	FileID   string
	Name     string
	RealPath string
}

// Provision creates one volume and registers it for cleanup.
//
// Cleanup is registered against the cluster client rather than DeleteVolume,
// so a test that deliberately breaks the delete path still leaves nothing
// behind on the array.
func Provision(ctx context.Context, t *testing.T, h *Harness, prefix string, capacity int64) ProvisionedVolume {
	t.Helper()

	name := h.VolumeName(prefix)
	resp, err := h.Ctrl().CreateVolume(ctx, h.CreateRequest(name, capacity))
	if err != nil {
		t.Fatalf("CreateVolume(%s): %v", name, err)
	}

	volume := resp.GetVolume()
	if volume == nil {
		t.Fatalf("CreateVolume(%s) returned no volume", name)
	}

	fileID := h.FileIDOf(t, volume.GetVolumeId())
	realPath := strings.TrimSuffix(h.StoreRealPath, "/") + "/" + name
	h.Track(fileID, realPath)

	return ProvisionedVolume{
		VolumeID: volume.GetVolumeId(),
		FileID:   fileID,
		Name:     name,
		RealPath: realPath,
	}
}

// RequireNodeMounting skips unless this host has opted into being mounted on.
func RequireNodeMounting(t *testing.T) {
	t.Helper()

	if os.Getenv("QUMULO_LIVE_NODE") != "1" {
		t.Skip("this test mounts NFS on the host running it; use `make live-node`, which sets " +
			"QUMULO_LIVE_NODE=1 and checks for mount.nfs and the kernel modules first")
	}
	if os.Geteuid() != 0 {
		t.Skip("mounting needs root; `make live-node` runs the compiled test binary under sudo")
	}
}

// PublishRequest builds the NodePublishVolume a kubelet would send for a
// volume this harness provisioned.
//
// VolumeContext is assembled here rather than taken from CreateVolume's
// response on purpose: it is the contract between the two halves of the
// driver, and writing it out makes a change to that contract show up as a
// compile or assertion failure rather than as both sides agreeing on
// something new.
func PublishRequest(h *Harness, volume ProvisionedVolume, targetPath, protocol string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:   volume.VolumeID,
		TargetPath: targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		VolumeContext: map[string]string{
			"server":     h.NFSServer,
			"exportPath": path.Join(h.StoreExportPath, volume.Name),
			"realPath":   volume.RealPath,
			"protocol":   protocol,
		},
	}
}

// MountTable reports whether anything is mounted at target, and on what
// options.
//
// Read from /proc/self/mounts rather than from the mounter the driver used:
// the question is what the kernel did, and asking the driver would just
// return the driver's own belief.
func MountTable(t *testing.T, target string) (string, bool) {
	t.Helper()

	raw, err := os.ReadFile("/proc/self/mounts")
	if err != nil {
		t.Fatalf("reading /proc/self/mounts: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[1] == target {
			return line, true
		}
	}
	return "", false
}

// RequireWritable writes through the mount and reads it back.
//
// The assertion that actually matters: a mount can succeed and still be
// unusable, because mountPermissions left the directory owned by the API user
// at a mode the mounting identity cannot write.
func RequireWritable(t *testing.T, target string) {
	t.Helper()

	probe := filepath.Join(target, "csi-live-test-probe")
	want := []byte("qumulo-csi live node test\n")

	if err := os.WriteFile(probe, want, 0o644); err != nil {
		t.Fatalf("writing through the mount at %s: %v -- the volume mounted but is not usable",
			target, err)
	}
	t.Cleanup(func() { _ = os.Remove(probe) })

	got, err := os.ReadFile(probe)
	if err != nil {
		t.Fatalf("reading back through the mount: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("read back %q, wrote %q", got, want)
	}
}
