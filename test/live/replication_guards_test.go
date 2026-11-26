//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package live

import (
	"context"
	"os"
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

// The three replication-aware guards on the ORDINARY CSI RPCs, against two
// real arrays.
//
// These are the RPCs that run for every user of the driver, not only for
// someone who turned replication on: DeleteVolume, ControllerExpandVolume and
// ListVolumes are unconditional, because a volume can be protected by a
// relationship an administrator created by hand, and all three would otherwise
// destroy, diverge or misreport it.
//
// replication_test.go proves the replication service itself. It never calls
// these three, which is why they are here. What the mocks in
// pkg/driver/replication_{delete,expand}_test.go and
// list_volumes_inventory_test.go assert against a fabric built to reproduce
// the hazard, this file asserts against a relationship the array actually
// holds, a quota the array actually enforces, and a token whose privileges the
// array actually checks.
//
// Environment is replication_test.go's, plus one optional variable:
//
//	QUMULO_UNPRIVILEGED_TOKEN_FILE  a token on the PEER array carrying the
//	                                ordinary CSI privileges and no
//	                                PRIVILEGE_REPLICATION_* at all. The PEER,
//	                                because the ListVolumes refusal is asked of
//	                                the array holding the replica. Without it
//	                                the privilege case skips: a pair that
//	                                cannot produce such a token cannot be
//	                                asked that question.

// protectedVolume is a dedicated-export volume with a live relationship to the
// peer, which is the starting state all three guards are about.
type protectedVolume struct {
	provisionedVolume
	relationshipID string
	replicaFileID  string
}

// protect provisions a dedicated-export volume, enables replication to the
// peer and waits for the first recovery point, or skips where the cluster
// cannot serve the model.
//
// Every guard test needs exactly this, and needs it to be REAL: the guards key
// off what the arrays report about the relationship, so a volume that merely
// looks protected proves nothing.
func (r *replicationHarness) protect(ctx context.Context, t *testing.T, prefix string) protectedVolume {
	t.Helper()

	volume := r.provisionDedicated(ctx, t, prefix)
	r.trackReplica(volume)

	if _, err := r.repl.EnableVolumeReplication(ctx, &replication.EnableVolumeReplicationRequest{
		VolumeId:   volume.VolumeID,
		Parameters: r.classTowardPeer(),
		Secrets:    r.replSecrets(),
	}); err != nil {
		t.Fatalf("EnableVolumeReplication on %s: %v", volume.Name, err)
	}

	var src *qumulo.RelationshipStatus
	waitFor(ctx, t, "the relationship to take its first recovery point", time.Second, func() (bool, error) {
		src = sourceStatus(ctx, t, r.Client, volume.RealPath)
		return src != nil && src.HasRecoveryPoint(), nil
	})

	replica, err := r.peer.LookupFileByPath(ctx, volume.RealPath)
	if err != nil {
		t.Fatalf("looking up the replica on the peer: %v", err)
	}

	t.Logf("protected: volume %s, relationship %s, replica file ID %s on %s",
		volume.Name, src.ID, replica.ID, r.peerServer)

	return protectedVolume{provisionedVolume: volume, relationshipID: src.ID, replicaFileID: replica.ID}
}

// quotaOn reads one array's quota limit for a file ID.
func quotaOn(ctx context.Context, t *testing.T, array *qumulo.Client, where, fileID string) uint64 {
	t.Helper()
	limit, err := array.GetQuota(ctx, fileID)
	if err != nil {
		t.Fatalf("reading the quota on %s for file %s: %v", where, fileID, err)
	}
	return limit
}

// expand calls ControllerExpandVolume with the given secrets.
func (r *replicationHarness) expand(
	ctx context.Context, handle string, bytes int64, secrets map[string]string,
) error {
	_, err := r.Ctrl().ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      handle,
		CapacityRange: &csi.CapacityRange{RequiredBytes: bytes},
		Secrets:       secrets,
	})
	return err
}

// ---------------------------------------------------------------------------
// 1. ControllerExpandVolume
// ---------------------------------------------------------------------------

// The replica's quota is raised BEFORE the source's, so a peer that cannot
// take the new limit leaves the source at the old one.
//
// The ordering is the whole guard. A source that grew past its replica is not
// a visible fault: it is discovered at promote, as ENOSPC on a copy that
// cannot hold what the source already accepted.
func TestLiveReplicationExpandRaisesTheReplicaFirst(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	volume := r.protect(ctx, t, "guard-expand")

	if got := quotaOn(ctx, t, r.peer, r.peerServer, volume.replicaFileID); got != uint64(gib) {
		t.Fatalf("the replica's pre-staged quota is %d, want %d -- Enable did not mirror the limit", got, gib)
	}

	// a. The happy path: both ends end up at the new limit.
	if err := r.expand(ctx, volume.VolumeID, 2*gib, r.replSecrets()); err != nil {
		t.Fatalf("expanding a protected volume to 2GiB: %v", err)
	}

	replicaQuota := quotaOn(ctx, t, r.peer, r.peerServer, volume.replicaFileID)
	sourceQuota := quotaOn(ctx, t, r.Client, r.NFSServer, volume.FileID)
	if replicaQuota != uint64(2*gib) {
		t.Errorf("the replica's quota is %d, want %d -- the replica did not grow", replicaQuota, 2*gib)
	}
	if sourceQuota != uint64(2*gib) {
		t.Errorf("the source's quota is %d, want %d", sourceQuota, 2*gib)
	}
	t.Logf("PROVED: expand raised both ends to %d (replica on %s, source on %s)",
		2*gib, r.peerServer, r.NFSServer)

	// b. The assertion the guard exists for: when the peer cannot be reached
	// for its half, the source must not take the new limit either.
	//
	// The peer is made unreachable by withholding its credential rather than
	// by cutting the network. That is deliberate and it is the honest choice
	// here: it was measured that removing a security-group rule does NOT cut
	// an established session, so a "partition" staged that way would not be
	// one, and a port pointed at a black hole tests the dial timeout rather
	// than the guard. What is being asserted is the ordering, and the
	// ordering is the same whichever way the peer half fails.
	noPeerCredential := map[string]string{
		"token":                   r.Token,
		hostOf(t, r.APIURL):       r.Token,
		"replicationdeletepolicy": "refuse",
	}

	err := r.expand(ctx, volume.VolumeID, 3*gib, noPeerCredential)
	if err == nil {
		t.Fatal("expanding with no credential for the peer succeeded; the source may now be larger than its replica")
	}
	if code := status.Code(err); code != codes.FailedPrecondition && code != codes.Unavailable {
		t.Errorf("expand without a peer credential returned %s, want FailedPrecondition or Unavailable: %v", code, err)
	}

	after := quotaOn(ctx, t, r.Client, r.NFSServer, volume.FileID)
	if after != uint64(2*gib) {
		t.Errorf("the source's quota is %d after a failed expand, want %d unchanged -- "+
			"this is exactly the ENOSPC at promote", after, 2*gib)
	}
	if got := quotaOn(ctx, t, r.peer, r.peerServer, volume.replicaFileID); got != uint64(2*gib) {
		t.Errorf("the replica's quota is %d after a failed expand, want %d unchanged", got, 2*gib)
	}
	t.Logf("PROVED: expand refused with %v, and BOTH quotas stayed at %d", status.Code(err), 2*gib)
}

// Raising a promoted replica's own quota is allowed: it is the documented
// workaround, and the promoted copy needs it before its first client
// write.
func TestLiveReplicationExpandOnTheReplicaRaisesItsOwnQuota(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	volume := r.protect(ctx, t, "guard-expand-replica")

	// The DR-side handle as the driver itself writes it, rather than one
	// assembled here: a handle built by hand would test this test's idea of
	// the format instead of the driver's.
	dest, err := r.repl.ResolveReplicationDestination(ctx, volume.VolumeID, "", r.classTowardPeer(), r.replSecrets())
	if err != nil {
		t.Fatalf("ResolveReplicationDestination: %v", err)
	}
	replicaHandle := dest.VolumeID
	// The DR side's expand Secret: its own token, and -- as its class's
	// Secret does -- its peer, which for this replica is the source.
	peerSecrets := map[string]string{
		"token": r.peerToken, r.peerServer: r.peerToken,
		"targetserver": r.NFSServer, "targetapiurl": r.APIURL, hostOf(t, r.APIURL): r.Token,
	}

	if _, err = r.Ctrl().ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      replicaHandle,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * gib},
		Secrets:       peerSecrets,
	}); err != nil {
		t.Fatalf("expanding the replica's own quota: %v -- this is the documented workaround and must be allowed: %v",
			replicaHandle, err)
	}

	if got := quotaOn(ctx, t, r.peer, r.peerServer, volume.replicaFileID); got != uint64(2*gib) {
		t.Errorf("the replica's own quota is %d, want %d", got, 2*gib)
	}
	// The array cannot tell this from the expand a DNS-named claim
	// reaches after a failover, where raising only the replica left the live
	// copy at its old size. So the source is raised with it.
	if got := quotaOn(ctx, t, r.Client, r.NFSServer, volume.FileID); got != uint64(2*gib) {
		t.Errorf("the source's quota is %d, want %d: the live copy must grow with its replica", got, 2*gib)
	}
	t.Logf("PROVED: a replica's quota can be raised to %d, and the source is raised with it", 2*gib)
}

// ---------------------------------------------------------------------------
// 2. DeleteVolume
// ---------------------------------------------------------------------------

// A protected volume is refused, and nothing is dismantled by the refusal.
//
// The rows are replication_delete_test.go's, run against a relationship the
// arrays actually hold. The assertion that carries the weight in every
// refusing row is the same: the DIRECTORY IS STILL THERE afterwards, and the
// relationship is still established on BOTH ends -- the guarded case is precisely the
// bug where the local end went first and left the peer an orphan.
func TestLiveReplicationDeleteRefusesAProtectedVolume(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	t.Run("an unprotected volume is deleted as before", func(t *testing.T) {
		volume := r.provisionDedicated(ctx, t, "guard-del-plain")

		if _, err := r.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: volume.VolumeID, Secrets: r.replSecrets(),
		}); err != nil {
			t.Fatalf("deleting an unprotected volume: %v", err)
		}
		waitFor(ctx, t, "the unprotected volume's directory to go", time.Second, func() (bool, error) {
			_, err := r.Client.LookupFileByPath(ctx, volume.RealPath)
			return qumulo.IsNotFound(err), nil
		})
		t.Logf("PROVED: the guard does not stand in the way of an ordinary delete")
	})

	t.Run("a replication source is refused, naming the relationship", func(t *testing.T) {
		volume := r.protect(ctx, t, "guard-del-refuse")

		_, err := r.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: volume.VolumeID, Secrets: r.replSecrets(),
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("DeleteVolume on a protected volume returned %v, want FailedPrecondition", err)
		}
		if !strings.Contains(err.Error(), volume.relationshipID) {
			t.Errorf("the refusal does not name relationship %s, so an operator cannot act on it: %v",
				volume.relationshipID, err)
		}

		assertStillProtected(ctx, t, r, volume, "after a refused delete")
		t.Logf("PROVED: refused with %v", err)
	})

	t.Run("disable removes both ends and then deletes", func(t *testing.T) {
		volume := r.protect(ctx, t, "guard-del-disable")

		secrets := r.replSecrets()
		secrets["replicationdeletepolicy"] = "disable"
		secrets["targetserver"] = r.peerServer
		secrets["targetapiurl"] = r.peerAPIURL

		if _, err := r.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: volume.VolumeID, Secrets: secrets,
		}); err != nil {
			t.Fatalf("DeleteVolume with replicationDeletePolicy=disable: %v", err)
		}

		if src := sourceStatus(ctx, t, r.Client, volume.RealPath); src != nil {
			t.Errorf("the source end of %s survived a disable-and-delete", volume.relationshipID)
		}
		if tgt := targetStatus(ctx, t, r.peer, volume.RealPath); tgt != nil {
			t.Errorf("the target end of %s survived a disable-and-delete -- the peer is orphaned (M7)",
				volume.relationshipID)
		}
		waitFor(ctx, t, "the source directory to go", time.Second, func() (bool, error) {
			_, err := r.Client.LookupFileByPath(ctx, volume.RealPath)
			return qumulo.IsNotFound(err), nil
		})
		t.Logf("PROVED: disable removed both ends and then deleted the volume")
	})

	t.Run("disable with no peer credential changes nothing", func(t *testing.T) {
		volume := r.protect(ctx, t, "guard-del-nocred")

		// The peer's half must come off first, so a peer that cannot be
		// reached is found BEFORE anything is dismantled.
		secrets := map[string]string{
			"token":                   r.Token,
			hostOf(t, r.APIURL):       r.Token,
			"replicationdeletepolicy": "disable",
			"targetserver":            r.peerServer,
			"targetapiurl":            r.peerAPIURL,
		}

		_, err := r.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: volume.VolumeID, Secrets: secrets,
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("DeleteVolume with disable and no peer credential returned %v, want FailedPrecondition", err)
		}

		assertStillProtected(ctx, t, r, volume, "after a refused disable-and-delete")
		t.Logf("PROVED: nothing was dismantled -- %v", err)
	})
}

// assertStillProtected is the post-condition of every refusing row: the volume
// is intact and both ends of the relationship are still there.
func assertStillProtected(
	ctx context.Context, t *testing.T, r *replicationHarness, volume protectedVolume, when string,
) {
	t.Helper()

	if _, err := r.Client.LookupFileByPath(ctx, volume.RealPath); err != nil {
		t.Errorf("the volume directory %s is gone %s: %v -- the guard ran too late", volume.RealPath, when, err)
	}
	src := sourceStatus(ctx, t, r.Client, volume.RealPath)
	if src == nil {
		t.Errorf("the source end of relationship %s is gone %s", volume.relationshipID, when)
	} else if src.State != qumulo.RelationshipEstablished {
		t.Errorf("the source end is %s %s, want %s", src.State, when, qumulo.RelationshipEstablished)
	}
	if tgt := targetStatus(ctx, t, r.peer, volume.RealPath); tgt == nil {
		t.Errorf("the target end of relationship %s is gone %s -- the peer was dismantled (M7)",
			volume.relationshipID, when)
	}
	if _, err := r.peer.LookupFileByPath(ctx, volume.RealPath); err != nil {
		t.Errorf("the replica on %s is gone %s: %v", r.peerServer, when, err)
	}
}

// ---------------------------------------------------------------------------
// 3. ListVolumes
// ---------------------------------------------------------------------------

// inventoryOnThePeer builds an inventory controller pointed at the PEER, which
// is where replicas live, with the given token.
//
// ListVolumes takes no per-request secret, so the credential comes from the
// class's Secret reference -- which is the whole point of the privilege case
// below: the token a listing runs under is a deployment-time choice, not a
// per-call one.
func (r *replicationHarness) inventoryOnThePeer(t *testing.T, token string) csi.ControllerServer {
	t.Helper()

	params := map[string]string{
		"server":          r.peerServer,
		"apiUrl":          r.peerAPIURL,
		"storeRealPath":   r.StoreRealPath,
		"storeExportPath": r.StoreExportPath,
		"csi.storage.k8s.io/provisioner-secret-name":      "qumulo-api-token",
		"csi.storage.k8s.io/provisioner-secret-namespace": "qumulo-csi",
	}

	d, err := driver.NewDriver(driver.Config{
		Name:         "csi.qumulo.com",
		Version:      "live-test",
		HTTPClient:   r.Client.HTTPClient,
		SecretReader: &fileSecretReader{token: token},
		StorageClassLister: &fakeClassLister{classes: []driver.StorageClassInfo{
			{Name: "live-dr-class", Parameters: params},
		}},
		Replication: true,
	})
	if err != nil {
		t.Fatalf("building the inventory driver against the peer: %v", err)
	}
	return d.ControllerServer()
}

// listed reports whether a listing holds a volume of this name.
func listed(resp *csi.ListVolumesResponse, name string) bool {
	for _, entry := range resp.GetEntries() {
		if strings.HasSuffix(entry.GetVolume().GetVolumeId(), "/"+name) {
			return true
		}
	}
	return false
}

// A replica is not a volume, and an inventory listing on the DR array must not
// offer one.
//
// The failure this rules out is a DR copy being handed to a scheduler as a
// free PV. The skip keys on the relationship's STATE, not on the directory,
// which is what the last step here proves: once replication is disabled the
// same directory does appear, because an ENDED relationship names no live
// replica.
func TestLiveReplicationListVolumesSkipsTheReplica(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	inventory := r.inventoryOnThePeer(t, r.peerToken)

	before, err := inventory.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("baseline ListVolumes on the peer: %v", err)
	}
	baseline := len(before.GetEntries())
	t.Logf("MEASURED baseline listing on %s under %q: %d volume(s)", r.peerServer, r.StoreRealPath, baseline)

	volume := r.protect(ctx, t, "guard-list")

	during, err := inventory.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes with a replica present: %v", err)
	}
	if listed(during, volume.Name) {
		t.Errorf("the replica %s is listed as a volume on %s -- a scheduler could bind the DR copy",
			volume.Name, r.peerServer)
	}
	if got := len(during.GetEntries()); got != baseline {
		t.Errorf("the listing has %d entries with a replica present, want the baseline %d", got, baseline)
	}
	t.Logf("PROVED: the replica is absent from the listing while the relationship is established")

	// Disable leaves the replica in place by design. With no live relationship
	// naming it, the same directory is an ordinary volume again.
	if _, err := r.repl.DisableVolumeReplication(ctx, &replication.DisableVolumeReplicationRequest{
		VolumeId:   volume.VolumeID,
		Parameters: r.classTowardPeer(),
		Secrets:    r.replSecrets(),
	}); err != nil {
		t.Fatalf("DisableVolumeReplication: %v", err)
	}

	after, err := inventory.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes after disabling replication: %v", err)
	}
	if !listed(after, volume.Name) {
		t.Errorf("%s is still hidden after the relationship was removed, so the skip is keyed on the "+
			"directory rather than on the relationship state", volume.Name)
	}
	t.Logf("PROVED: the same directory is listed once no live relationship names it")
}

// A token that cannot read the relationship tables must make ListVolumes
// REFUSE, not list replicas as volumes.
//
// This is the deployment break to watch for: an inventory-only token minted
// before replication existed still lists, and
// silently offers every DR copy as a volume. Answering FailedPrecondition and
// naming the missing privilege is the only safe behaviour, because the
// listing cannot tell a replica from a volume without that table.
func TestLiveReplicationListVolumesRefusesWithoutThePrivilege(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tokenFile := os.Getenv("QUMULO_UNPRIVILEGED_TOKEN_FILE")
	if tokenFile == "" {
		t.Skip("QUMULO_UNPRIVILEGED_TOKEN_FILE is unset: this case needs a token on the PEER " +
			"carrying the ordinary CSI privileges and no PRIVILEGE_REPLICATION_*")
	}
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatalf("reading QUMULO_UNPRIVILEGED_TOKEN_FILE: %v", err)
	}
	token, err := extractToken(raw)
	if err != nil {
		t.Fatalf("QUMULO_UNPRIVILEGED_TOKEN_FILE %q: %v", tokenFile, err)
	}

	// The token has to be the right kind of wrong: unprivileged for
	// replication, but able to read the tree. A token that cannot do either
	// would pass this test for the wrong reason.
	unprivileged := qumulo.New(r.peerAPIURL, token)
	unprivileged.HTTPClient = r.Client.HTTPClient
	if _, err := unprivileged.ListDirectory(ctx, r.StoreRealPath, ""); err != nil {
		t.Skipf("the unprivileged token cannot list %q on the peer either, so a refusal would not be "+
			"attributable to the replication privilege: %v", r.StoreRealPath, err)
	}
	if _, err := unprivileged.ListTargetStatus(ctx); err == nil {
		t.Skip("the token in QUMULO_UNPRIVILEGED_TOKEN_FILE CAN read the relationship tables, " +
			"so it does not pose the question this test asks")
	}

	_, err = r.inventoryOnThePeer(t, token).ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err == nil {
		t.Fatal("ListVolumes succeeded with a token that cannot read the relationship tables: " +
			"every replica on this array was just offered as a volume")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Errorf("ListVolumes returned %s, want FailedPrecondition: %v", code, err)
	}
	if !strings.Contains(err.Error(), "PRIVILEGE_REPLICATION") {
		t.Errorf("the refusal does not name the missing privilege, so an operator cannot fix it: %v", err)
	}
	t.Logf("PROVED: refused rather than listing replicas -- %v", err)
}

// TestLiveReplicationExpandAndDeleteAcceptAnAddressKeyedSecret: the chart README's
// replication Secret holds one token per array keyed by address and no plain
// "token" key. The replication service always read it; expand and delete
// read only "token" and refused it, so a lab built from the README had every
// expand of a protected volume refused.
//
// Proves: with "token" removed from the harness Secret, a protected volume
// expands replica-first to the new size, and a delete of it is still refused
// naming the relationship (the guard read the Secret before refusing).
func TestLiveReplicationExpandAndDeleteAcceptAnAddressKeyedSecret(t *testing.T) {
	r := newReplicationHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	volume := r.protect(ctx, t, "guard-addr-keyed")

	secrets := r.replSecrets()
	delete(secrets, "token")

	t.Run("expand with the README's Secret raises both ends", func(t *testing.T) {
		if err := r.expand(ctx, volume.VolumeID, 2*gib, secrets); err != nil {
			t.Fatalf("expanding a protected volume with an address-keyed Secret: %v", err)
		}
		replicaQuota := quotaOn(ctx, t, r.peer, r.peerServer, volume.replicaFileID)
		sourceQuota := quotaOn(ctx, t, r.Client, r.NFSServer, volume.FileID)
		if replicaQuota != uint64(2*gib) {
			t.Errorf("the replica's quota is %d, want %d", replicaQuota, 2*gib)
		}
		if sourceQuota != uint64(2*gib) {
			t.Errorf("the source's quota is %d, want %d", sourceQuota, 2*gib)
		}
		t.Logf("PROVED: an address-keyed Secret expanded both ends to %d", 2*gib)
	})

	t.Run("delete with the README's Secret is refused, naming the relationship", func(t *testing.T) {
		_, err := r.Ctrl().DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: volume.VolumeID, Secrets: secrets,
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("DeleteVolume on a protected volume with an address-keyed Secret returned %v, want FailedPrecondition", err)
		}
		if !strings.Contains(err.Error(), volume.relationshipID) {
			t.Errorf("the refusal does not name relationship %s: %v", volume.relationshipID, err)
		}
		assertStillProtected(ctx, t, r, volume, "after a refused delete with an address-keyed Secret")
		t.Logf("PROVED: refused with %v", err)
	})
}
