//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package live

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/csi-addons/spec/lib/go/replication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// TestLiveReplicationGroupReEnablesAfterAPlainDisable runs on two real
// arrays. A shared-subdirectory class is protected as a group, with the DR
// array's covering export created by hand and read-write, as an operator
// following the disaster-recovery guide made it. Enable must leave that export alone
// and refuse, naming it; once the operator sets it read-only, Enable
// succeeds, and a plain Disable and re-Enable of the group -- never failed
// over -- must succeed. Before the fix the re-Enable read the read-write
// export as a promoted copy and was refused, after the Disable had already
// removed the protection.
//
// Mutating on both arrays, under a directory of its own below
// QUMULO_STORE_REAL_PATH, removed at the end.
func TestLiveReplicationGroupReEnablesAfterAPlainDisable(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	name := fmt.Sprintf("csi-live-group-%d", time.Now().UnixNano())
	parent := strings.TrimSuffix(r.StoreRealPath, "/")
	base := parent + "/" + name
	if !strings.Contains(base, safetyMarker) {
		t.Fatalf("refusing to use %q: it is outside %q", base, safetyMarker)
	}

	// Registered before anything is created: a failure on the peer must not
	// strand what the loop already made on the local array.
	t.Cleanup(func() { r.removeGroupBase(t, base) })

	// The class's base directory and covering export on both arrays. On the
	// peer the export is read-write: nothing but an operator made it.
	for _, array := range []*qumulo.Client{r.Client, r.peer} {
		if _, err := array.CreateDirectory(ctx, parent, name); err != nil {
			t.Fatalf("creating %s: %v", base, err)
		}
		if _, err := array.CreateExport(ctx, qumulo.ExportSpec{
			ExportPath: base, FSPath: base, Description: "csi live test group " + name,
			AllowedClients: []string{"0.0.0.0/0"},
		}); err != nil {
			t.Fatalf("creating the covering export %s: %v", base, err)
		}
	}

	// One member volume, provisioned through the shared class.
	req := r.CreateRequest(r.VolumeName("grp"), gib)
	req.Parameters["storeRealPath"] = base
	req.Parameters["storeExportPath"] = base
	resp, err := r.Ctrl().CreateVolume(ctx, req)
	if err != nil {
		t.Fatalf("CreateVolume in the group class: %v", err)
	}
	member := resp.GetVolume().GetVolumeId()

	class := r.classTowardPeer()
	class["replicationScope"] = "group"
	class["allowNonEmptyTarget"] = "true"
	enable := func(what string) {
		t.Helper()
		waitFor(ctx, t, what, 5*time.Second, func() (bool, error) {
			_, err := r.repl.EnableVolumeReplication(ctx, &replication.EnableVolumeReplicationRequest{
				VolumeId: member, Parameters: class, Secrets: r.replSecrets(),
			})
			switch status.Code(err) {
			case codes.OK:
				return true, nil
			case codes.Aborted: // not replicated yet, or not listed yet
				t.Logf("%s: %v; retrying", what, err)
				return false, nil
			}
			return false, err
		})
	}

	// The covering export on the peer is the operator's and read-write.
	// Enable does not change it; it refuses, naming the export, while the
	// relationship it already created replicates.
	var refused error
	waitFor(ctx, t, "Enable to refuse the operator's read-write export", 5*time.Second, func() (bool, error) {
		_, refused = r.repl.EnableVolumeReplication(ctx, &replication.EnableVolumeReplicationRequest{
			VolumeId: member, Parameters: class, Secrets: r.replSecrets(),
		})
		switch status.Code(refused) {
		case codes.FailedPrecondition:
			return true, nil
		case codes.Aborted:
			t.Logf("protecting the group: %v; retrying", refused)
			return false, nil
		}
		return false, fmt.Errorf("Enable over the operator's read-write export answered %w, want FailedPrecondition", refused)
	})
	if !strings.Contains(refused.Error(), "not made by this driver") {
		t.Fatalf("the refusal does not say why: %v", refused)
	}
	requireExportReadOnly(ctx, t, r.peer, base, false)
	if rel := sourceStatus(ctx, t, r.Client, base); rel == nil {
		t.Fatal("no source relationship on the base path after the refused Enable")
	}
	t.Logf("PROVED: Enable left the operator's export alone and refused: %v", refused)

	// The operator's one change, then the retry.
	exports, err := r.peer.ListExports(ctx)
	if err != nil {
		t.Fatalf("listing the peer's exports: %v", err)
	}
	for _, e := range exports {
		if path.Clean(e.FSPath) == base {
			if err := r.peer.SetExportReadOnly(ctx, e.ID, true); err != nil {
				t.Fatalf("setting the covering export read-only, as the operator would: %v", err)
			}
		}
	}
	enable("protecting the group once its covering export is read-only")
	if rel := sourceStatus(ctx, t, r.Client, base); rel == nil {
		t.Fatal("no source relationship on the base path after Enable")
	}
	requireExportReadOnly(ctx, t, r.peer, base, true)

	if _, err := r.repl.DisableVolumeReplication(ctx, &replication.DisableVolumeReplicationRequest{
		VolumeId: member, Parameters: class, Secrets: r.replSecrets(),
	}); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if rel := sourceStatus(ctx, t, r.Client, base); rel != nil {
		t.Fatalf("a source relationship is still listed after Disable: %s %s", rel.ID, rel.State)
	}

	enable("re-protecting the group after a plain Disable")
	rel := sourceStatus(ctx, t, r.Client, base)
	if rel == nil {
		t.Fatal("no source relationship after the re-Enable")
	}
	t.Logf("PROVED: re-Enable after a plain Disable re-protected the group (relationship %s, %s)", rel.ID, rel.State)

	if _, err := r.repl.DisableVolumeReplication(ctx, &replication.DisableVolumeReplicationRequest{
		VolumeId: member, Parameters: class, Secrets: r.replSecrets(),
	}); err != nil {
		t.Logf("final Disable: %v (cleanup removes what is left)", err)
	}
}

// requireExportReadOnly fails unless the export serving fsPath on array has
// the given read-only state.
func requireExportReadOnly(ctx context.Context, t *testing.T, array *qumulo.Client, fsPath string, want bool) {
	t.Helper()
	exports, err := array.ListExports(ctx)
	if err != nil {
		t.Fatalf("listing exports: %v", err)
	}
	for _, e := range exports {
		if path.Clean(e.FSPath) == path.Clean(fsPath) {
			if e.ReadOnly != want {
				t.Fatalf("export %s is read-only=%t, want %t", e.ExportPath, e.ReadOnly, want)
			}
			return
		}
	}
	t.Fatalf("no export serves %s", fsPath)
}

// removeGroupBase removes, on both arrays, every relationship rooted at base,
// its covering export, and the directory tree with the quotas under it.
func (r *replicationHarness) removeGroupBase(t *testing.T, base string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, array := range []*qumulo.Client{r.Client, r.peer} {
		sources, _ := array.ListSourceStatus(ctx)
		for _, s := range sources {
			if path.Clean(s.SourceRootPath) == base {
				if err := array.DeleteSource(ctx, s.ID); err != nil && !qumulo.IsNotFound(err) {
					t.Logf("cleanup: deleting source relationship %s: %v", s.ID, err)
				}
			}
		}
		targets, _ := array.ListTargetStatus(ctx)
		for _, s := range targets {
			if path.Clean(s.TargetRootPath) == base {
				if err := array.DeleteTarget(ctx, s.ID); err != nil && !qumulo.IsNotFound(err) {
					t.Logf("cleanup: deleting target relationship %s: %v", s.ID, err)
				}
			}
		}
	}
	for _, array := range []*qumulo.Client{r.Client, r.peer} {
		exports, _ := array.ListExports(ctx)
		for _, e := range exports {
			if strings.HasPrefix(path.Clean(e.FSPath), base) {
				if err := array.DeleteExport(ctx, e.ID); err != nil && !qumulo.IsNotFound(err) {
					t.Logf("cleanup: deleting export %s: %v", e.ExportPath, err)
				}
			}
		}
		dir, err := array.LookupFileByPath(ctx, base)
		if err != nil {
			continue
		}
		if entries, err := array.ListDirectory(ctx, base, ""); err == nil {
			for _, e := range entries {
				_ = array.DeleteQuota(ctx, e.ID)
			}
		}
		if err := array.StartTreeDelete(ctx, dir.ID); err != nil && !qumulo.IsNotFound(err) {
			t.Logf("cleanup: tree-deleting %s (%s): %v -- it may need removing by hand", base, dir.ID, err)
		}
	}
}
