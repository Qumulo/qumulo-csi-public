//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package live

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// This file proves restore after the source is deleted, and it is the only place that can.
//
// The defect was that a restore resolved the snapshot's source file ID to a
// live path and walked that path, so restoring after the source volume had
// been deleted -- the ordinary backup workflow -- failed with a permanent
// NotFound while the Qumulo snapshot itself was intact. The unit test
// TestRestoreSucceedsAfterTheSourceVolumeIsDeleted pins the driver's logic
// against a scripted client, and the mock server now models the cluster state
// faithfully, but neither can prove the restore actually produces the bytes:
// the mock implements no copy-chunk endpoint, so nothing there ever copies a
// byte of file content. Only a real cluster copies data, so only a real
// cluster can show that a restored volume contains what the snapshot held.
//
// Run with: make live-restore

// fileDataPath renders a cluster path as the /data endpoint for that file.
func fileDataPath(apiURL, filePath string) string {
	return fmt.Sprintf("%s/v1/files/%s/data", apiURL, strings.ReplaceAll(filePath, "/", "%2F"))
}

// writeFileContent creates a file's contents on the cluster.
//
// The production client has no method for this and should not gain one: the
// driver only ever copies server-side, so a write path would exist purely for
// tests. Modelled on readFileContent in test/e2e, which uses the same endpoint
// in the other direction.
//
// Verified against Qumulo Core 7.9.3 on 2026-09-10: PUT with
// application/octet-stream is accepted and the bytes read back match. seedFile
// below re-reads what it wrote anyway, so a Core version that wants a
// different shape here fails naming this helper rather than looking like a
// driver bug.
func (h *harness) writeFileContent(ctx context.Context, filePath string, content []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		fileDataPath(h.APIURL, filePath), bytes.NewReader(content))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+h.Token)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := h.Client.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("PUT %s: HTTP %d: %s", filePath, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// readFileContent reads a file's bytes back from the cluster.
func (h *harness) readFileContent(ctx context.Context, filePath string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileDataPath(h.APIURL, filePath), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+h.Token)

	resp, err := h.Client.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", filePath, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// seedFile creates a file under dirPath and writes content into it, then reads
// it back to prove the seeding worked.
//
// The read-back is not paranoia about the cluster: it separates a broken test
// fixture from a broken driver. If the write endpoint above is wrong for this
// Core version, this fails here naming the harness, rather than surfacing
// later as an empty restored file and looking like a driver bug.
func (h *harness) seedFile(ctx context.Context, t *testing.T, dirPath, name string, content []byte) {
	t.Helper()

	if _, err := h.Client.CreateFile(ctx, dirPath, name); err != nil {
		t.Fatalf("seeding: CreateFile(%s/%s): %v", dirPath, name, err)
	}
	filePath := path.Join(dirPath, name)
	if err := h.writeFileContent(ctx, filePath, content); err != nil {
		t.Fatalf("seeding: writing %s: %v -- this is the test harness's own write path, "+
			"not the driver's; check the /data endpoint against this Core version", filePath, err)
	}
	got, err := h.readFileContent(ctx, filePath)
	if err != nil {
		t.Fatalf("seeding: reading back %s: %v", filePath, err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("seeding: %s reads back as %q, want %q -- the harness could not establish "+
			"the precondition, so nothing below would mean anything", filePath, got, content)
	}
}

// waitUntilTreeDeleted blocks until fileID no longer resolves to a path.
//
// DeleteVolume returns once the tree-delete job is accepted and may not wait
// for it to finish, and the restore under test must run after the source is
// genuinely gone -- otherwise a pass could come from the live path still
// being walkable, which is exactly the bug.
func (h *harness) waitUntilTreeDeleted(ctx context.Context, t *testing.T, fileID string) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Minute)
	for {
		resolved, err := h.Client.ResolveFileIDs(ctx, []string{fileID})
		if err != nil {
			t.Fatalf("waiting for the tree-delete of %s: %v", fileID, err)
		}
		if strings.TrimSuffix(resolved[fileID], "/") == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("file ID %s still resolves to %q after 3 minutes; the source volume was "+
				"not tree-deleted, so this test cannot prove anything about restoring without it",
				fileID, resolved[fileID])
		}
		time.Sleep(2 * time.Second)
	}
}

// restoreVolume calls CreateVolume with a snapshot content source, retrying
// while the driver reports Aborted.
//
// Aborted is not a failure here, it is the documented contract: a restore that
// exhausts its per-call budget returns Aborted so the CO retries and the next
// attempt resumes from the staging directory. A test that treated it as a
// failure would be asserting the opposite of the design.
func (h *harness) restoreVolume(
	ctx context.Context,
	t *testing.T,
	name, snapshotID string,
	capacity int64,
) *csi.Volume {
	t.Helper()

	req := h.CreateRequest(name, capacity)
	req.VolumeContentSource = &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshotID},
		},
	}

	for attempt := 1; attempt <= 10; attempt++ {
		resp, err := h.Ctrl().CreateVolume(ctx, req)
		if err == nil {
			return resp.GetVolume()
		}
		if status.Code(err) != codes.Aborted {
			t.Fatalf("restoring %q from snapshot %q: %v", name, snapshotID, err)
		}
		t.Logf("restore attempt %d returned Aborted (its budget expired); retrying", attempt)
	}
	t.Fatalf("restore of %q did not finish within 10 attempts", name)
	return nil
}

// TestLiveRestoreAfterSourceVolumeDeleted is the restore acceptance test, executed.
//
// Snapshot a volume that has content, delete the volume, restore from the
// snapshot, and read the content back off the cluster. Before the fix the
// restore failed with NotFound at step four, because the walk started from a
// live path that the tree-delete had removed.
//
// Mutating: it creates and tree-deletes directories under
// QUMULO_STORE_REAL_PATH, which the harness refuses to run unless that path
// carries the safety marker.
func TestLiveRestoreAfterSourceVolumeDeleted(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	h.RequireCoveringExport(ctx, t)

	const (
		topName    = "restored.txt"
		nestedName = "nested.txt"
		nestedDir  = "sub"
	)
	topContent := []byte("qcsi-001 top-level payload " + time.Now().UTC().Format(time.RFC3339Nano))
	nestedContent := []byte("qcsi-001 nested payload")

	// 1. A volume with content in it, including one level of nesting -- the
	//    recursion is what the fix changed, so a flat directory would leave
	//    the descend-by-ID path unexercised.
	sourceID := h.createVolume(t, "restore-src", gib)
	// The name comes back out of the handle rather than being generated here:
	// createVolume makes its own, and a second one would name a directory
	// that does not exist.
	sourcePath := path.Join(h.StoreRealPath, volumeNameFromID(t, sourceID))
	sourceFileID := h.FileIDOf(t, sourceID)

	h.seedFile(ctx, t, sourcePath, topName, topContent)
	if _, err := h.Client.CreateDirectory(ctx, sourcePath, nestedDir); err != nil {
		t.Fatalf("creating %s/%s: %v", sourcePath, nestedDir, err)
	}
	h.seedFile(ctx, t, path.Join(sourcePath, nestedDir), nestedName, nestedContent)

	// 2. Snapshot it, through the driver's own RPC.
	snapResp, err := h.Ctrl().CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           h.SnapshotName("restore-snap"),
		SourceVolumeId: sourceID,
		Secrets:        h.Secrets(),
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	snapshotID := snapResp.GetSnapshot().GetSnapshotId()
	if !snapResp.GetSnapshot().GetReadyToUse() {
		t.Fatalf("snapshot %q is not ready to use", snapshotID)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := h.Ctrl().DeleteSnapshot(cleanupCtx, &csi.DeleteSnapshotRequest{
			SnapshotId: snapshotID,
			Secrets:    h.Secrets(),
		}); err != nil {
			t.Logf("cleanup: deleting snapshot %s: %v", snapshotID, err)
		}
	})

	// 3. Delete the source, and wait until it is really gone. This is the step
	//    that used to make the restore impossible.
	h.deleteVolume(t, sourceID)
	h.waitUntilTreeDeleted(ctx, t, sourceFileID)

	// The precondition, stated as an assertion: the snapshot survives its
	// source, and resolve answers the dead ID with an empty path. That empty
	// answer is what the driver used to treat as unrecoverable.
	snapInfo, err := h.Client.GetSnapshot(ctx, clusterSnapshotID(t, snapshotID))
	if err != nil {
		t.Fatalf("the snapshot must outlive its source volume, but GetSnapshot failed: %v", err)
	}
	if snapInfo.SourceFileID != sourceFileID {
		t.Fatalf("snapshot names source file ID %q, want %q", snapInfo.SourceFileID, sourceFileID)
	}
	resolved, err := h.Client.ResolveFileIDs(ctx, []string{sourceFileID})
	if err != nil {
		t.Fatalf("resolving the deleted source: %v", err)
	}
	if p := strings.TrimSuffix(resolved[sourceFileID], "/"); p != "" {
		t.Fatalf("the deleted source still resolves to %q; this test is not exercising the "+
			"deleted-source path at all", p)
	}

	// 4. Restore. This is the assertion the whole file exists for.
	restoredName := h.VolumeName("restore-dst")
	restored := h.restoreVolume(ctx, t, restoredName, snapshotID, gib)
	restoredPath := path.Join(h.StoreRealPath, restoredName)
	h.Track(h.FileIDOf(t, restored.GetVolumeId()), restoredPath)

	// 5. The content, read off the cluster rather than inferred from a
	//    successful RPC. A restore that created an empty tree would satisfy
	//    every check above.
	for _, want := range []struct {
		path    string
		content []byte
	}{
		{path: path.Join(restoredPath, topName), content: topContent},
		{path: path.Join(restoredPath, nestedDir, nestedName), content: nestedContent},
	} {
		got, err := h.readFileContent(ctx, want.path)
		if err != nil {
			t.Fatalf("reading restored %s: %v", want.path, err)
		}
		if !bytes.Equal(got, want.content) {
			t.Errorf("restored %s contains %q, want %q", want.path, got, want.content)
		}
	}

	// 6. The restore must not have resurrected the original directory, and
	//    must have left no staging directory behind: the rename is the commit
	//    point, so a leftover .csi-restoring-* means the commit did not happen
	//    even though the RPC succeeded.
	if _, err := h.Client.LookupFileByPath(ctx, sourcePath); err == nil {
		t.Errorf("the source volume %q exists again after the restore", sourcePath)
	} else if !qumulo.IsNotFound(err) {
		t.Errorf("checking that the source volume stayed deleted: %v", err)
	}

	entries, err := h.Client.ListDirectory(ctx, h.StoreRealPath, "")
	if err != nil {
		t.Fatalf("listing the store root: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name, ".csi-restoring-") {
			t.Errorf("restore left a staging directory behind: %s", entry.Name)
		}
	}
}

// TestLiveRestoreCopiesContentWithTheSourceStillPresent is the control.
//
// Same workflow with no delete in the middle. If this fails too, the problem
// is copy-chunk or the harness rather than the deleted-source path, and
// knowing which is what makes the test above diagnosable.
func TestLiveRestoreCopiesContentWithTheSourceStillPresent(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	h.RequireCoveringExport(ctx, t)

	content := []byte("qcsi-001 control payload " + time.Now().UTC().Format(time.RFC3339Nano))

	sourceID := h.createVolume(t, "restore-live-src", gib)
	sourcePath := path.Join(h.StoreRealPath, volumeNameFromID(t, sourceID))
	h.seedFile(ctx, t, sourcePath, "kept.txt", content)

	snapResp, err := h.Ctrl().CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           h.SnapshotName("restore-live-snap"),
		SourceVolumeId: sourceID,
		Secrets:        h.Secrets(),
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	snapshotID := snapResp.GetSnapshot().GetSnapshotId()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := h.Ctrl().DeleteSnapshot(cleanupCtx, &csi.DeleteSnapshotRequest{
			SnapshotId: snapshotID,
			Secrets:    h.Secrets(),
		}); err != nil {
			t.Logf("cleanup: deleting snapshot %s: %v", snapshotID, err)
		}
	})

	restoredName := h.VolumeName("restore-live-dst")
	restored := h.restoreVolume(ctx, t, restoredName, snapshotID, gib)
	restoredPath := path.Join(h.StoreRealPath, restoredName)
	h.Track(h.FileIDOf(t, restored.GetVolumeId()), restoredPath)

	got, err := h.readFileContent(ctx, path.Join(restoredPath, "kept.txt"))
	if err != nil {
		t.Fatalf("reading restored kept.txt: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("restored kept.txt contains %q, want %q", got, content)
	}
}

// TestLiveRestoreFromAnExternallyTakenSnapshot backs the chart README's
// "Backup tools (Velero and similar)" claim.
//
// That section promises that a restore driven from a pre-provisioned
// VolumeSnapshotContent -- Velero's model, where the snapshot handle is
// supplied by the backup tool rather than produced by this driver's
// CreateSnapshot RPC -- works the same way a CSI-driven restore does. The claim
// is code-correct: CreateVolume resolves a snapshot by ID against the Qumulo
// API and never asks what created it. But it was published with nothing
// testing it, so this is that test.
//
// The snapshot here is taken **out of band**, straight through the cluster
// client, and the CSI handle is assembled by hand exactly as a backup tool
// would have to assemble it. Deliberately not via h.Ctrl().CreateSnapshot:
// going through the driver would prove only that the driver can read its own
// snapshots, which is what the other tests in this file already cover.
func TestLiveRestoreFromAnExternallyTakenSnapshot(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	h.RequireCoveringExport(ctx, t)

	content := []byte("qcsi velero-style payload " + time.Now().UTC().Format(time.RFC3339Nano))

	sourceID := h.createVolume(t, "import-src", gib)
	sourcePath := path.Join(h.StoreRealPath, volumeNameFromID(t, sourceID))
	h.seedFile(ctx, t, sourcePath, "imported.txt", content)

	// The out-of-band snapshot: the cluster client, not the CSI RPC. This is
	// what an external backup tool or a cluster administrator would have.
	external, err := h.Client.CreateSnapshot(ctx, h.FileIDOf(t, sourceID), "external-backup")
	if err != nil {
		t.Fatalf("taking a snapshot outside the driver: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := h.Client.DeleteSnapshot(cleanupCtx, external.ID); err != nil {
			t.Logf("cleanup: deleting snapshot %s: %v", external.ID, err)
		}
	})

	// The handle a pre-provisioned VolumeSnapshotContent would carry. Built
	// here rather than read back from a CreateSnapshot response, because the
	// format being reproducible from the cluster's own snapshot ID is the
	// property a backup tool depends on.
	// The documented import form: <server>##<snapshotID>, or
	// <server>#<apiUrl>#<snapshotID> when the class sets apiUrl -- which the
	// harness does, and which the restore endpoint check enforces.
	importedHandle := h.SnapshotHandle(external.ID)
	t.Logf("restoring from an externally taken snapshot, handle %q", importedHandle)

	restoredName := h.VolumeName("import-dst")
	restored := h.restoreVolume(ctx, t, restoredName, importedHandle, gib)
	restoredPath := path.Join(h.StoreRealPath, restoredName)
	h.Track(h.FileIDOf(t, restored.GetVolumeId()), restoredPath)

	got, err := h.readFileContent(ctx, path.Join(restoredPath, "imported.txt"))
	if err != nil {
		t.Fatalf("reading restored imported.txt: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("restored imported.txt contains %q, want %q", got, content)
	}

	// The content source is echoed back, which is what lets the CO -- or the
	// backup tool -- confirm the restore was bound to the snapshot it asked
	// for rather than to a fresh empty volume.
	if src := restored.GetContentSource().GetSnapshot().GetSnapshotId(); src != importedHandle {
		t.Errorf("restored volume reports content source %q, want %q", src, importedHandle)
	}
}

// The caveat the README's Velero section does not mention: the handle has to be
// exactly <server>#<apiUrl>#<snapshotID> with a decimal ID (the middle field
// empty when the endpoint is the default for the server).
//
// A backup tool that stores a bare cluster snapshot ID, or an operator who
// hand-writes a VolumeSnapshotContent with the wrong shape, gets a refusal
// rather than a wrong volume -- which is the right outcome, and is worth
// pinning because it is the most likely way to get this wrong.
func TestLiveRestoreRejectsAMalformedImportedHandle(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for _, tc := range []struct{ name, handle string }{
		{name: "bare snapshot id with no cluster", handle: "12345"},
		{name: "non-decimal snapshot id", handle: h.SnapshotHandle("not-a-number")},
		{name: "too many fields", handle: h.SnapshotHandle("12345") + "#extra"},
		{name: "another cluster", handle: "some-other-cluster.example.com##12345"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := h.CreateRequest(h.VolumeName("import-bad"), gib)
			req.VolumeContentSource = &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: tc.handle},
				},
			}

			resp, err := h.Ctrl().CreateVolume(ctx, req)
			if err == nil {
				// Nothing should have been created, but track it if it was so
				// the cluster is left clean either way.
				if vol := resp.GetVolume(); vol != nil {
					h.Track(h.FileIDOf(t, vol.GetVolumeId()), path.Join(h.StoreRealPath, req.Name))
				}
				t.Fatalf("CreateVolume accepted a malformed snapshot handle %q", tc.handle)
			}
			code := status.Code(err)
			if code != codes.NotFound && code != codes.InvalidArgument {
				t.Errorf("handle %q refused with %s; want NotFound or InvalidArgument: %v",
					tc.handle, code, err)
			}
			t.Logf("%q -> %s: %v", tc.handle, code, err)
		})
	}
}

// The restore endpoint check live, the case that loses data: the class's array holds a snapshot
// with the number the handle names, but the handle says the snapshot was
// taken through another endpoint. The restore must be refused before that
// snapshot is read, and leave nothing on the array; the same snapshot through
// its own endpoint restores (the control). The other endpoint is never
// dialled, so it needs no second array.
func TestLiveRestoreRefusesASameNumberedSnapshotFromAnotherEndpoint(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	h.RequireCoveringExport(ctx, t)

	content := []byte("qcsi restore-endpoint payload " + time.Now().UTC().Format(time.RFC3339Nano))
	sourceID := h.createVolume(t, "d03-src", gib)
	sourcePath := path.Join(h.StoreRealPath, volumeNameFromID(t, sourceID))
	h.seedFile(ctx, t, sourcePath, "d03.txt", content)

	snap, err := h.Client.CreateSnapshot(ctx, h.FileIDOf(t, sourceID), "d03-same-number")
	if err != nil {
		t.Fatalf("taking snapshot: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := h.Client.DeleteSnapshot(cleanupCtx, snap.ID); err != nil {
			t.Logf("cleanup: deleting snapshot %s: %v", snap.ID, err)
		}
	})

	// L1: snapshot N exists on this array; the handle says another array.
	const otherEndpoint = "https://d03-other-array.invalid:8000"
	foreign := h.NFSServer + "#" + otherEndpoint + "#" + snap.ID
	refusedName := h.VolumeName("d03-refused")
	req := h.CreateRequest(refusedName, gib)
	req.VolumeContentSource = &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: foreign},
		},
	}
	resp, err := h.Ctrl().CreateVolume(ctx, req)
	if err == nil {
		if vol := resp.GetVolume(); vol != nil {
			h.Track(h.FileIDOf(t, vol.GetVolumeId()), path.Join(h.StoreRealPath, refusedName))
		}
		t.Fatalf("restored snapshot %s through %s although the handle names %s", snap.ID, h.APIURL, otherEndpoint)
	}
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("refused with %s, want InvalidArgument: %v", code, err)
	}
	if !strings.Contains(err.Error(), otherEndpoint) || !strings.Contains(err.Error(), "its own cluster") {
		t.Errorf("the refusal does not name the snapshot's endpoint and the rule: %v", err)
	}
	t.Logf("L1 refused: %v", err)
	for _, p := range []string{
		path.Join(h.StoreRealPath, refusedName),
		path.Join(h.StoreRealPath, ".csi-restoring-"+refusedName),
	} {
		if _, err := h.Client.LookupFileByPath(ctx, p); err == nil {
			t.Errorf("the refused restore left %s on the array", p)
		}
	}

	// L2: the control -- the same snapshot through its own endpoint.
	restoredName := h.VolumeName("d03-dst")
	restored := h.restoreVolume(ctx, t, restoredName, h.SnapshotHandle(snap.ID), gib)
	restoredPath := path.Join(h.StoreRealPath, restoredName)
	h.Track(h.FileIDOf(t, restored.GetVolumeId()), restoredPath)
	got, err := h.readFileContent(ctx, path.Join(restoredPath, "d03.txt"))
	if err != nil {
		t.Fatalf("reading restored d03.txt: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("restored d03.txt contains %q, want %q", got, content)
	}
}
