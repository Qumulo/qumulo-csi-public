//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package live

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/replication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/driver"
	"qumulo-csi-driver/pkg/qumulo"
)

// The replication service against two real arrays.
//
// This is a two-array DR end-to-end check (same path on both arrays,
// export and quota pre-staged) run through the driver's own RPCs
// instead of curl, asserting what those scripts recorded as evidence: the
// relationship establishes and gets a recovery point, the replica has its own
// file ID, the export and quota are pre-staged read-only, a planned promote
// freezes the source and drains against the array clock, the promoted copy
// accepts writes, resync reverses on the same ID and reconnects, and after
// failback every file compares identical by whole-file md5 on both arrays.
//
// It needs a second array. Beyond the harness's variables:
//
//	QUMULO_PEER_API_URL     REST endpoint of the peer array
//	QUMULO_PEER_TOKEN_FILE  token minted on the peer, same privileges plus
//	                        PRIVILEGE_REPLICATION_{SOURCE,TARGET}_WRITE
//	QUMULO_PEER_SERVER      the address the LOCAL array replicates to over
//	                        port 3712; defaults to the peer API URL's host
//	QUMULO_LOCAL_SERVER     the address the PEER replicates back to at
//	                        resync; defaults to the local API URL's host
//
// Data is written and read through the REST API (PUT/GET /v1/files/{ref}/data)
// so the md5 assertions need no mount and no root. With QUMULO_LIVE_NODE=1
// under root the promoted replica is additionally mounted and the mount's
// addr= asserted against the peer's resolved address, which is the check the
// runbook wants after any flip: the mount, not the name, says which array a
// pod is talking to.
//
// Everything created is removed: the relationship on both arrays, the replica
// directory, export and quota on the peer, and the volume on the local array.
// A frozen source is unfrozen before cleanup, because a tree-delete of a
// read-only root would otherwise be refused.

// replicationHarness is the harness plus the peer array.
type replicationHarness struct {
	*harness
	peer        *qumulo.Client
	peerAPIURL  string
	peerToken   string
	peerServer  string
	localServer string
	repl        *driver.ReplicationServer
}

// newReplicationHarness builds the two-array harness, or skips.
func newReplicationHarness(t *testing.T) *replicationHarness {
	t.Helper()

	h := newHarness(t)

	peerAPIURL := os.Getenv("QUMULO_PEER_API_URL")
	peerTokenFile := os.Getenv("QUMULO_PEER_TOKEN_FILE")
	if peerAPIURL == "" || peerTokenFile == "" {
		t.Skip("replication tests need a second array: set QUMULO_PEER_API_URL and QUMULO_PEER_TOKEN_FILE")
	}
	rawToken, err := os.ReadFile(peerTokenFile)
	if err != nil {
		t.Fatalf("reading QUMULO_PEER_TOKEN_FILE: %v", err)
	}
	peerToken, err := extractToken(rawToken)
	if err != nil {
		t.Fatalf("QUMULO_PEER_TOKEN_FILE %q: %v", peerTokenFile, err)
	}

	peerServer := os.Getenv("QUMULO_PEER_SERVER")
	if peerServer == "" {
		peerServer = hostOf(t, peerAPIURL)
	}
	localServer := os.Getenv("QUMULO_LOCAL_SERVER")
	if localServer == "" {
		localServer = hostOf(t, h.APIURL)
	}

	peer := qumulo.New(peerAPIURL, peerToken)
	peer.HTTPClient = h.Client.HTTPClient

	// The harness's driver serves the controller only; replication is
	// opt-in there as it is in main, so a second driver is built with it on.
	d, err := driver.NewDriver(driver.Config{
		Name:        "csi.qumulo.com",
		Version:     "live-test",
		HTTPClient:  h.Client.HTTPClient,
		Replication: true,
	})
	if err != nil {
		t.Fatalf("building the driver: %v", err)
	}
	h.Driver = d

	return &replicationHarness{
		harness: h, peer: peer, peerAPIURL: peerAPIURL, peerToken: peerToken,
		peerServer: peerServer, localServer: localServer, repl: d.ReplicationServer(),
	}
}

func hostOf(t *testing.T, rawURL string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Hostname() == "" {
		t.Fatalf("%q is not a URL with a host", rawURL)
	}
	return parsed.Hostname()
}

// classTowardPeer is the VolumeReplicationClass on the local cluster.
func (r *replicationHarness) classTowardPeer() map[string]string {
	return map[string]string{
		"apiUrl":       r.APIURL,
		"targetServer": r.peerServer,
		"targetApiUrl": r.peerAPIURL,
	}
}

// classTowardLocal is the class the DR cluster would carry: the peer is
// "local" there and this harness's array is its target.
func (r *replicationHarness) classTowardLocal() map[string]string {
	return map[string]string{
		"apiUrl":       r.peerAPIURL,
		"targetServer": r.localServer,
		"targetApiUrl": r.APIURL,
	}
}

// replSecrets is the one symmetric Secret: a token per array, keyed by the
// host the controller dials.
func (r *replicationHarness) replSecrets() map[string]string {
	return map[string]string{
		hostOf(r.T, r.APIURL):     r.Token,
		hostOf(r.T, r.peerAPIURL): r.peerToken,
		"token":                   r.Token,
	}
}

// provisionDedicated provisions a dedicated-export volume, which is the model
// a per-volume VolumeReplication requires, or skips where the cluster cannot
// serve that model (KI-1: NFSv4 with an ancestor export).
func (r *replicationHarness) provisionDedicated(ctx context.Context, t *testing.T, prefix string) provisionedVolume {
	t.Helper()

	name := r.VolumeName(prefix)
	req := r.CreateRequest(name, gib)
	req.Parameters["provisioningModel"] = "dedicated-export"
	req.Parameters["allowAnyClient"] = "true" // open, as these exports always were
	resp, err := r.Ctrl().CreateVolume(ctx, req)
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			t.Skipf("this cluster cannot provision dedicated-export volumes, which replication needs: %v", err)
		}
		t.Fatalf("CreateVolume(%s): %v", name, err)
	}
	volume := resp.GetVolume()
	fileID := r.FileIDOf(t, volume.GetVolumeId())
	realPath := strings.TrimSuffix(r.StoreRealPath, "/") + "/" + name
	r.Track(fileID, realPath)
	r.trackOwnExport(realPath)

	return provisionedVolume{VolumeID: volume.GetVolumeId(), FileID: fileID, Name: name, RealPath: realPath}
}

// trackOwnExport removes the dedicated-export volume's own NFS export on the
// local array at cleanup. track() removes the quota and the directory, which
// is all a shared-subdirectory volume owns; a dedicated-export volume also
// owns an export, and the first live run of this test left one behind
// (export 2718 on the lab's C1, removed by hand). The export is looked up by
// fs_path at cleanup time rather than by the ID CreateVolume returned, so a
// test that failed before recording anything still leaves nothing.
func (r *replicationHarness) trackOwnExport(realPath string) {
	r.T.Helper()
	if !strings.Contains(realPath, safetyMarker) {
		r.T.Fatalf("refusing to schedule export cleanup of %q: it is outside %q", realPath, safetyMarker)
	}
	r.T.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		exports, err := r.Client.ListExports(ctx)
		if err != nil {
			r.T.Logf("cleanup: listing exports on the local array to remove %s's: %v", realPath, err)
			return
		}
		for _, e := range exports {
			if e.FSPath != realPath {
				continue
			}
			if err := r.Client.DeleteExport(ctx, e.ID); err != nil && !qumulo.IsNotFound(err) {
				r.T.Logf("cleanup: deleting the volume's own export %s (%s): %v -- it may need removing by hand", e.ExportPath, e.ID, err)
			}
		}
	})
}

// trackReplica registers the replica directory, export and quota on the peer
// for removal, and the relationship on both arrays.
func (r *replicationHarness) trackReplica(volume provisionedVolume) {
	r.T.Helper()
	if !strings.Contains(volume.RealPath, safetyMarker) {
		r.T.Fatalf("refusing to schedule cleanup of %q on the peer: it is outside %q", volume.RealPath, safetyMarker)
	}
	r.T.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		// Relationships first, on both arrays, unfreezing a frozen source so
		// the tree-delete below is not refused.
		for _, array := range []*qumulo.Client{r.Client, r.peer} {
			sources, _ := array.ListSourceStatus(ctx)
			for _, s := range sources {
				if s.SourceRootPath != volume.RealPath {
					continue
				}
				if s.SourceRootReadOnly {
					unfrozen := false
					_, _ = array.PatchSource(ctx, s.ID, qumulo.SourcePatch{SourceRootReadOnly: &unfrozen})
				}
				if err := array.DeleteSource(ctx, s.ID); err != nil && !qumulo.IsNotFound(err) {
					r.T.Logf("cleanup: deleting source relationship %s: %v", s.ID, err)
				}
			}
			targets, _ := array.ListTargetStatus(ctx)
			for _, s := range targets {
				if s.TargetRootPath != volume.RealPath {
					continue
				}
				if err := array.DeleteTarget(ctx, s.ID); err != nil && !qumulo.IsNotFound(err) {
					r.T.Logf("cleanup: deleting target relationship %s: %v", s.ID, err)
				}
			}
		}

		exports, _ := r.peer.ListExports(ctx)
		for _, e := range exports {
			if e.FSPath == volume.RealPath {
				if err := r.peer.DeleteExport(ctx, e.ID); err != nil && !qumulo.IsNotFound(err) {
					r.T.Logf("cleanup: deleting replica export %s: %v", e.ExportPath, err)
				}
			}
		}
		replica, err := r.peer.LookupFileByPath(ctx, volume.RealPath)
		if err != nil {
			return
		}
		if err := r.peer.DeleteQuota(ctx, replica.ID); err != nil && !qumulo.IsNotFound(err) {
			r.T.Logf("cleanup: removing the replica quota: %v", err)
		}
		if err := r.peer.StartTreeDelete(ctx, replica.ID); err != nil && !qumulo.IsNotFound(err) {
			r.T.Logf("cleanup: tree-deleting the replica %s (%s): %v -- it may need removing by hand",
				volume.RealPath, replica.ID, err)
		}
	})
}

// rawArray issues the data requests the client has no method for, against
// either array. The harness's writeFileContent and readFileContent do the
// same for the local array only; two arrays need the address to vary.
type rawArray struct {
	apiURL, token string
}

func (a rawArray) do(ctx context.Context, t *testing.T, method, fullURL string, body []byte) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building %s %s: %v", method, fullURL, err)
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	client := &http.Client{
		Timeout: liveTimeout,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			InsecureSkipVerify: os.Getenv("QUMULO_TLS_INSECURE") == "1",
			MinVersion:         tls.VersionTLS12,
		}},
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, fullURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the response of %s %s: %v", method, fullURL, err)
	}
	return resp.StatusCode, raw
}

// writeFileOn creates a file under dir on one array and fills it through
// the API.
func writeFileOn(ctx context.Context, t *testing.T, client *qumulo.Client, array rawArray, dir, name string, contents []byte) {
	t.Helper()

	if _, err := client.CreateFile(ctx, dir, name); err != nil && !qumulo.IsConflict(err) {
		t.Fatalf("creating %s/%s: %v", dir, name, err)
	}
	code, body := array.do(ctx, t, http.MethodPut, fileDataPath(array.apiURL, path.Join(dir, name)), contents)
	if code < 200 || code > 299 {
		t.Fatalf("writing %s/%s: HTTP %d: %s", dir, name, code, body)
	}
}

// md5On reads a file through one array's API and returns its whole-file
// md5, or "" if the file is absent there.
func md5On(ctx context.Context, t *testing.T, array rawArray, filePath string) string {
	t.Helper()

	code, body := array.do(ctx, t, http.MethodGet, fileDataPath(array.apiURL, filePath), nil)
	if code == http.StatusNotFound {
		return ""
	}
	if code < 200 || code > 299 {
		t.Fatalf("reading %s: HTTP %d: %s", filePath, code, body)
	}
	sum := md5.Sum(body)
	return hex.EncodeToString(sum[:])
}

// waitFor polls until fn reports done, failing the test at the deadline.
func waitFor(ctx context.Context, t *testing.T, what string, every time.Duration, fn func() (bool, error)) {
	t.Helper()

	start := time.Now()
	for {
		done, err := fn()
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		if done {
			t.Logf("%s after %s", what, time.Since(start).Round(time.Second))
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(every):
		}
	}
}

// sourceStatus finds the relationship rooted at realPath on array as a source.
func sourceStatus(ctx context.Context, t *testing.T, array *qumulo.Client, realPath string) *qumulo.RelationshipStatus {
	t.Helper()
	statuses, err := array.ListSourceStatus(ctx)
	if err != nil {
		t.Fatalf("ListSourceStatus: %v", err)
	}
	for i := range statuses {
		if statuses[i].SourceRootPath == realPath {
			return &statuses[i]
		}
	}
	return nil
}

// targetStatus is sourceStatus for the target end.
func targetStatus(ctx context.Context, t *testing.T, array *qumulo.Client, realPath string) *qumulo.RelationshipStatus {
	t.Helper()
	statuses, err := array.ListTargetStatus(ctx)
	if err != nil {
		t.Fatalf("ListTargetStatus: %v", err)
	}
	for i := range statuses {
		if statuses[i].TargetRootPath == realPath {
			return &statuses[i]
		}
	}
	return nil
}

// TestLiveReplicationRoundTrip is the whole sequence: protect, failover,
// reprotect, failback, unprotect, with the evidence the lab scripts recorded
// asserted at each step.
func TestLiveReplicationRoundTrip(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	local := rawArray{apiURL: r.APIURL, token: r.Token}
	peer := rawArray{apiURL: r.peerAPIURL, token: r.peerToken}

	volume := r.provisionDedicated(ctx, t, "repl")
	r.trackReplica(volume)
	writeFileOn(ctx, t, r.Client, local, volume.RealPath, "f1", []byte("written on the source before protect\n"))

	// ---- Phase 1: protect -------------------------------------------------
	enable := func() error {
		_, err := r.repl.EnableVolumeReplication(ctx, &replication.EnableVolumeReplicationRequest{
			VolumeId: volume.VolumeID, Parameters: r.classTowardPeer(), Secrets: r.replSecrets(),
		})
		return err
	}
	if err := enable(); err != nil {
		t.Fatalf("EnableVolumeReplication: %v", err)
	}

	// The source end reports AWAITING_AUTHORIZATION for a few seconds after
	// the peer has authorized (measured, runs 1 and 5), so it is polled.
	var src *qumulo.RelationshipStatus
	waitFor(ctx, t, "the source end to report ESTABLISHED after Enable", 2*time.Second, func() (bool, error) {
		src = sourceStatus(ctx, t, r.Client, volume.RealPath)
		if src == nil {
			return false, errors.New("no source relationship rooted at the volume after Enable")
		}
		return src.State == qumulo.RelationshipEstablished, nil
	})
	relID := src.ID
	t.Logf("PROVED: relationship %s created on %s, state %s", relID, r.NFSServer, src.State)

	// The replica directory now exists, with its own file ID.
	replica, err := r.peer.LookupFileByPath(ctx, volume.RealPath)
	if err != nil {
		t.Fatalf("replica directory %s on the peer: %v", volume.RealPath, err)
	}
	if replica.ID == volume.FileID {
		t.Errorf("replica file ID %s equals the source's; the handle's fileID would be portable, which it is not", replica.ID)
	}
	t.Logf("PROVED: replica file ID %s differs from source %s", replica.ID, volume.FileID)

	// Export pre-staged read-only, quota equal.
	sourceExport, err := r.Client.ListExports(ctx)
	if err != nil {
		t.Fatalf("ListExports on the source: %v", err)
	}
	var srcExport *qumulo.ExportInfo
	for i := range sourceExport {
		if sourceExport[i].FSPath == volume.RealPath {
			srcExport = &sourceExport[i]
		}
	}
	if srcExport == nil {
		t.Fatal("the dedicated-export volume has no export on the source")
	}
	peerExports, err := r.peer.ListExports(ctx)
	if err != nil {
		t.Fatalf("ListExports on the peer: %v", err)
	}
	var staged *qumulo.ExportInfo
	for i := range peerExports {
		if peerExports[i].ExportPath == srcExport.ExportPath {
			staged = &peerExports[i]
		}
	}
	if staged == nil {
		t.Fatalf("no export at %q on the peer after Enable", srcExport.ExportPath)
	}
	if staged.FSPath != volume.RealPath || staged.TenantID != srcExport.TenantID {
		t.Errorf("pre-staged export %+v does not match the source's %+v", *staged, *srcExport)
	}
	if !staged.ReadOnly {
		t.Error("the pre-staged export is writable; it must be read-only until promote")
	}
	srcLimit, err := r.Client.GetQuota(ctx, volume.FileID)
	if err != nil {
		t.Fatalf("source quota: %v", err)
	}
	peerLimit, err := r.peer.GetQuota(ctx, replica.ID)
	if err != nil {
		t.Fatalf("replica quota: %v", err)
	}
	if peerLimit != srcLimit {
		t.Errorf("replica quota %d, source %d", peerLimit, srcLimit)
	}
	t.Logf("PROVED: export %q pre-staged read-only and quota %d mirrored while the target is read-only", staged.ExportPath, peerLimit)

	// A second Enable adopts.
	if err := enable(); err != nil {
		t.Fatalf("repeat EnableVolumeReplication: %v", err)
	}
	if again := sourceStatus(ctx, t, r.Client, volume.RealPath); again == nil || again.ID != relID {
		t.Error("a repeat Enable did not adopt the existing relationship")
	}

	// Protected means a recovery point, and the data on the peer.
	waitFor(ctx, t, "first recovery point and f1 on the peer", 5*time.Second, func() (bool, error) {
		s := sourceStatus(ctx, t, r.Client, volume.RealPath)
		if s == nil || !s.HasRecoveryPoint() || !s.JobIdle() {
			return false, nil
		}
		return md5On(ctx, t, peer, volume.RealPath+"/f1") == md5On(ctx, t, local, volume.RealPath+"/f1"), nil
	})

	// The DR-side handle, as a repoint tool would write it into a PV.
	dest, err := r.repl.ResolveReplicationDestination(ctx, volume.VolumeID, "", r.classTowardPeer(), r.replSecrets())
	if err != nil {
		t.Fatalf("ResolveReplicationDestination: %v", err)
	}
	// The handle names the PEER and the replica's PATH -- and the path is the
	// same one the source carries, which is the whole point: only the server
	// differs across a failover. It deliberately carries no file ID, because
	// the replica's ID (replica.ID here) is exactly the array-local fact that
	// used to make a DR-side PV wrong.
	if got, want := volumePathFromID(t, dest.VolumeID), volumePathFromID(t, volume.VolumeID); got != want {
		t.Errorf("destination handle %q names path %q, want the source's own %q", dest.VolumeID, got, want)
	}
	if !strings.HasPrefix(dest.VolumeID, r.peerServer+"#") {
		t.Errorf("destination handle %q does not name the peer array %s", dest.VolumeID, r.peerServer)
	}
	if strings.Contains(dest.VolumeID, replica.ID) {
		t.Errorf("destination handle %q carries the replica's file ID %s; a handle must not", dest.VolumeID, replica.ID)
	}
	replicaHandle := dest.VolumeID
	t.Logf("PROVED: DR handle %s differs from the source handle %s in the server alone",
		replicaHandle, volume.VolumeID)

	// Health from both ends.
	health, err := r.repl.AssessVolumeReplication(ctx, volume.VolumeID, "", r.classTowardPeer(), r.replSecrets())
	if err != nil {
		t.Fatalf("AssessVolumeReplication: %v", err)
	}
	if health.Status != driver.ReplicationHealthy {
		t.Errorf("health after protect is %s: %s", health.Status, health.Message)
	}
	t.Logf("health: %s (%s), lag %s", health.Status, health.Message, health.Lag.Round(time.Second))

	// ---- Phase 3: planned failover to the peer -----------------------------
	promote := func(handle string, params map[string]string, force bool) error {
		_, err := r.repl.PromoteVolume(ctx, &replication.PromoteVolumeRequest{
			VolumeId: handle, Parameters: params, Secrets: r.replSecrets(), Force: force,
		})
		return err
	}
	// Bounded by the RPC's own budget: an Aborted means "still draining,
	// retry", exactly as the controller would.
	waitFor(ctx, t, "planned promote of the replica", 2*time.Second, func() (bool, error) {
		err := promote(replicaHandle, r.classTowardLocal(), false)
		if status.Code(err) == codes.Aborted {
			t.Logf("promote: %v", err)
			return false, nil
		}
		return true, err
	})

	src = sourceStatus(ctx, t, r.Client, volume.RealPath)
	tgt := targetStatus(ctx, t, r.peer, volume.RealPath)
	if src == nil || tgt == nil {
		t.Fatal("relationship vanished during promote")
	}
	if !src.SourceRootReadOnly {
		t.Error("the source was not frozen by the planned promote (3.2)")
	}
	if tgt.State != qumulo.RelationshipDisconnected || tgt.TargetRootReadOnly {
		t.Errorf("target after promote: state %s, read-only %t; want DISCONNECTED and writable", tgt.State, tgt.TargetRootReadOnly)
	}
	if !tgt.HasRecoveryPoint() {
		t.Error("the promoted target lost its recovery point")
	}
	peerExports, _ = r.peer.ListExports(ctx)
	for _, e := range peerExports {
		if e.ExportPath == srcExport.ExportPath && e.ReadOnly {
			t.Error("the replica's export is still read-only after promote; the flip did not happen")
		}
	}
	t.Logf("PROVED: promote froze the source, made the target writable at recovery point %s, flipped the export", tgt.RecoveryPoint.Format(time.RFC3339))

	// A second promote must not call make-writable again.
	if err := promote(replicaHandle, r.classTowardLocal(), false); err != nil {
		t.Errorf("repeat promote on an already promoted target: %v", err)
	}

	// The promoted copy takes writes; the frozen source is refused.
	writeFileOn(ctx, t, r.peer, peer, volume.RealPath, "f2", []byte("written on the promoted replica\n"))
	if _, err := r.Client.CreateFile(ctx, volume.RealPath, "should-fail"); err == nil {
		t.Error("a create on the frozen source succeeded; the freeze is not holding")
	} else {
		t.Logf("PROVED: frozen source refuses a create: %v", err)
	}

	// Optional: mount the promoted replica and read addr= off the kernel.
	mountReplicaIfAllowed(ctx, t, r, volume, replicaHandle)

	// ---- Phase 5: reprotect (demote local, resync local) -------------------
	if _, err := r.repl.DemoteVolume(ctx, &replication.DemoteVolumeRequest{
		VolumeId: volume.VolumeID, Parameters: r.classTowardPeer(), Secrets: r.replSecrets(),
	}); err != nil {
		t.Fatalf("DemoteVolume: %v", err)
	}
	resync := func(handle string, params map[string]string) {
		waitFor(ctx, t, "resync of "+handle+" to ready", 5*time.Second, func() (bool, error) {
			resp, err := r.repl.ResyncVolume(ctx, &replication.ResyncVolumeRequest{
				VolumeId: handle, Parameters: params, Secrets: r.replSecrets(),
			})
			if err != nil {
				return false, err
			}
			return resp.GetReady(), nil
		})
	}
	resync(volume.VolumeID, r.classTowardPeer())

	// Same ID, roles swapped, ESTABLISHED. The two ends of a relationship
	// do not update together (measured, run 4: the new target reported
	// ESTABLISHED with a recovery point while the new source end still read
	// DISCONNECTED for a few seconds after reconnect), so the source end is
	// polled rather than read once.
	var reversed *qumulo.RelationshipStatus
	waitFor(ctx, t, "the new source end to report ESTABLISHED after reconnect", 2*time.Second, func() (bool, error) {
		reversed = sourceStatus(ctx, t, r.peer, volume.RealPath)
		if reversed == nil {
			return false, errors.New("after resync the peer is not the source")
		}
		return reversed.State == qumulo.RelationshipEstablished, nil
	})
	if reversed.ID != relID {
		t.Errorf("reverse changed the relationship ID: %s -> %s (it must be kept)", relID, reversed.ID)
	}
	if reversed.ReplicationMode != qumulo.ReplicationModeContinuous {
		t.Errorf("mode after reprotect is %s, class wants continuous (the class is re-applied)", reversed.ReplicationMode)
	}
	t.Logf("PROVED: reverse kept ID %s; peer is source, local is target, %s", relID, reversed.State)

	// The point of it all: f2 arrives on the old primary.
	waitFor(ctx, t, "f2 replicated back to the original array", 5*time.Second, func() (bool, error) {
		return md5On(ctx, t, local, volume.RealPath+"/f2") == md5On(ctx, t, peer, volume.RealPath+"/f2") &&
			md5On(ctx, t, local, volume.RealPath+"/f2") != "", nil
	})

	// ---- Phase 6: failback -------------------------------------------------
	waitFor(ctx, t, "planned promote of the original", 2*time.Second, func() (bool, error) {
		err := promote(volume.VolumeID, r.classTowardPeer(), false)
		if status.Code(err) == codes.Aborted {
			return false, nil
		}
		return true, err
	})
	if _, err := r.repl.DemoteVolume(ctx, &replication.DemoteVolumeRequest{
		VolumeId: replicaHandle, Parameters: r.classTowardLocal(), Secrets: r.replSecrets(),
	}); err != nil {
		t.Fatalf("DemoteVolume on the peer: %v", err)
	}
	resync(replicaHandle, r.classTowardLocal())

	// Same per-end lag after the failback's reconnect: poll the source end.
	var final *qumulo.RelationshipStatus
	waitFor(ctx, t, "the original to report itself the ESTABLISHED source after failback", 2*time.Second, func() (bool, error) {
		final = sourceStatus(ctx, t, r.Client, volume.RealPath)
		if final == nil {
			return false, errors.New("after failback the original is not the source")
		}
		return final.State == qumulo.RelationshipEstablished, nil
	})
	if final.ID != relID || final.SourceRootReadOnly {
		t.Fatalf("after failback the original is not the writable established source: %+v", final)
	}
	for _, name := range []string{"f1", "f2"} {
		a, b := md5On(ctx, t, local, volume.RealPath+"/"+name), md5On(ctx, t, peer, volume.RealPath+"/"+name)
		if a == "" || a != b {
			t.Errorf("%s: md5 %q on the original, %q on the peer", name, a, b)
		}
	}
	t.Logf("PROVED: failback complete on relationship %s; f1 and f2 identical by whole-file md5 on both arrays", relID)

	// The original CSI handle still resolves: same PV, same handle.
	if _, err := r.Ctrl().ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: volume.VolumeID,
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		}},
		Secrets: r.Secrets(),
	}); err != nil {
		t.Errorf("the original handle no longer validates after failback: %v", err)
	}

	// ---- Phase 8: unprotect --------------------------------------------------
	if _, err := r.repl.DisableVolumeReplication(ctx, &replication.DisableVolumeReplicationRequest{
		VolumeId: volume.VolumeID, Parameters: r.classTowardPeer(), Secrets: r.replSecrets(),
	}); err != nil {
		t.Fatalf("DisableVolumeReplication: %v", err)
	}
	if sourceStatus(ctx, t, r.Client, volume.RealPath) != nil || targetStatus(ctx, t, r.peer, volume.RealPath) != nil {
		t.Error("a relationship rooted at the volume survives Disable")
	}
	if _, err := r.peer.LookupFileByPath(ctx, volume.RealPath); err != nil {
		t.Errorf("Disable removed the replica directory; it must delete no data: %v", err)
	}
	t.Logf("PROVED: Disable removed the relationship on both arrays and left the replica in place")
}

// mountReplicaIfAllowed mounts the promoted replica through the node service
// and asserts the kernel's addr= names the peer, when the host has opted in.
func mountReplicaIfAllowed(ctx context.Context, t *testing.T, r *replicationHarness, volume provisionedVolume, replicaHandle string) {
	t.Helper()

	if os.Getenv("QUMULO_LIVE_NODE") != "1" || os.Geteuid() != 0 {
		t.Log("skipping the mount check: set QUMULO_LIVE_NODE=1 and run as root to assert addr= on a real mount")
		return
	}

	d, err := driver.NewDriver(driver.Config{
		Name: "csi.qumulo.com", Version: "live-test", Mode: driver.ModeAll, NodeID: "live",
		HTTPClient: r.Client.HTTPClient,
	})
	if err != nil {
		t.Fatalf("building a node driver: %v", err)
	}
	target := filepath.Join(t.TempDir(), "replica")
	if err := os.MkdirAll(target, 0o750); err != nil {
		t.Fatalf("creating the mount target: %v", err)
	}
	node := d.NodeServer()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := node.NodeUnpublishVolume(cleanupCtx, &csi.NodeUnpublishVolumeRequest{VolumeId: replicaHandle, TargetPath: target}); err != nil {
			t.Logf("cleanup: unpublishing %s: %v -- it may need `umount` by hand", target, err)
		}
	})

	// The DR PV: same exportPath and realPath, server is the peer.
	if _, err := node.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:   replicaHandle,
		TargetPath: target,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		},
		VolumeContext: map[string]string{
			"server":     r.peerServer,
			"exportPath": path.Join(r.StoreExportPath, volume.Name),
			"realPath":   volume.RealPath,
			"protocol":   "nfs4.1",
		},
	}); err != nil {
		t.Fatalf("NodePublishVolume of the promoted replica: %v", err)
	}

	line, mounted := mountTable(t, target)
	if !mounted {
		t.Fatalf("nothing is mounted at %s", target)
	}
	t.Logf("mount: %s", line)

	// The assertion the runbook wants: the resolved address, not the name.
	addrs, err := net.LookupHost(r.peerServer)
	if err != nil {
		t.Fatalf("resolving %s: %v", r.peerServer, err)
	}
	var matched bool
	for _, addr := range addrs {
		if strings.Contains(line, "addr="+addr) {
			matched = true
		}
	}
	if !matched {
		t.Errorf("mount addr= is not the peer's address (%v): %s", addrs, line)
	}
	requireWritable(t, target)
	t.Logf("PROVED: the promoted replica mounted from %s (addr= one of %v) and accepted a write", r.peerServer, addrs)
}

// TestLiveReplicationRefusesSharedSubdirectoryVolumes is review C-6: a
// per-volume VolumeReplication on a shared-subdirectory volume is refused
// before anything is created on either array.
func TestLiveReplicationRefusesSharedSubdirectoryVolumes(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	r.RequireCoveringExport(ctx, t)

	volume := provision(ctx, t, r.harness, "repl-shared", gib)

	_, err := r.repl.EnableVolumeReplication(ctx, &replication.EnableVolumeReplicationRequest{
		VolumeId: volume.VolumeID, Parameters: r.classTowardPeer(), Secrets: r.replSecrets(),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Enable on a shared-subdirectory volume returned %v, want FailedPrecondition", err)
	}
	if sourceStatus(ctx, t, r.Client, volume.RealPath) != nil {
		t.Error("a relationship was created for a refused volume")
	}
	t.Logf("PROVED: refused with %v", err)
}
