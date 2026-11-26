// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	mount "k8s.io/mount-utils"

	"qumulo-csi-driver/pkg/qumulo"
)

// Tests that pin lock and ordering behaviour across RPCs. Each uses a
// fake that blocks at a named point until the test releases it, so the
// interleaving is the one under test on every run -- no sleeps.

// gatedPVLister is a PersistentVolume lister whose FIRST call signals entered
// and then blocks until release is closed; later calls answer at once. It
// counts calls, which is how a test sees whether a caller listed for itself
// or waited for the blocked one.
type gatedPVLister struct {
	pvs     []PersistentVolumeInfo
	entered chan struct{}
	release chan struct{}

	mu    sync.Mutex
	calls int
}

func newGatedPVLister(pvs ...PersistentVolumeInfo) *gatedPVLister {
	return &gatedPVLister{pvs: pvs, entered: make(chan struct{}), release: make(chan struct{})}
}

func (l *gatedPVLister) ListPersistentVolumes(ctx context.Context, _ string) ([]PersistentVolumeInfo, error) {
	l.mu.Lock()
	l.calls++
	first := l.calls == 1
	l.mu.Unlock()
	if first {
		close(l.entered)
		<-l.release
	}
	return append([]PersistentVolumeInfo(nil), l.pvs...), nil
}

func (l *gatedPVLister) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

// waitForGoroutineIn returns once some goroutine's stack holds every one of
// frames -- a caller parked on a named lock, say -- so a test can release a
// gate knowing the waiter is really waiting, rather than sleeping and hoping.
// It polls the scheduler, never a clock; the deadline only turns a hang into
// a failure.
func waitForGoroutineIn(t *testing.T, frames ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			all := true
			for _, f := range frames {
				if !strings.Contains(g, f) {
					all = false
					break
				}
			}
			if all {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no goroutine reached %v", frames)
		}
		runtime.Gosched()
	}
}

// Group reports queue behind a slow PV listing. listVolumesForFanout holds
// memberList.mu across ListPersistentVolumes (replication_protection.go), so
// a second group report arriving while the first is inside a slow listing
// waits for it and reuses its result rather than listing again: one List for
// any number of concurrent reports, at the price of every report waiting as
// long as the slowest listing. Pinned both ways -- the second report gets the
// first's members (so it did wait), and the lister ran once (so it did not
// list for itself) -- so a change to either is a decision, not an accident.
func TestGroupReportsQueueBehindASlowPersistentVolumeListing(t *testing.T) {
	s := newSharedScenario(t)
	require.NoError(t, s.enableGroup(t, groupClass(), ""))
	pvs := make([]PersistentVolumeInfo, 0, 3)
	for i, h := range s.allMemberHandles() {
		pvs = append(pvs, PersistentVolumeInfo{Name: []string{"pv-a", "pv-b", "pv-c"}[i], Handle: h, Phase: "Bound"})
	}
	lister := newGatedPVLister(pvs...)
	s.driver.PersistentVolumeLister = lister

	first := make(chan error, 1)
	go func() {
		_, err := s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-a"), "", nil, replSecrets())
		first <- err
	}()
	<-lister.entered // the first report is inside the listing, holding memberList.mu

	second := make(chan error, 1)
	go func() {
		_, err := s.server.AssessVolumeReplication(context.Background(), s.memberHandle("pvc-tenant-b"), "", nil, replSecrets())
		second <- err
	}()
	// The second report is parked on memberList.mu, behind the first.
	waitForGoroutineIn(t, "(*ReplicationServer).listVolumesForFanout", "sync.(*Mutex).Lock")

	close(lister.release)
	require.NoError(t, <-first)
	require.NoError(t, <-second)

	assert.Equal(t, 1, lister.callCount(),
		"the second report listed for itself instead of queueing behind the first's listing")
	verdicts := s.verdicts()
	for _, member := range s.allMemberHandles() {
		assert.True(t, verdicts[member], "member %s has no protected verdict from the reports", member)
	}
}

// The direct form of the same pin, with nothing but the listing in the way:
// N callers, one List, every caller answered from it.
func TestFanoutListingIsSharedByConcurrentCallers(t *testing.T) {
	s := newReplScenario(t)
	lister := newGatedPVLister(PersistentVolumeInfo{Name: "pv-x", Handle: s.handleA, Phase: "Bound"})
	s.driver.PersistentVolumeLister = lister

	const callers = 4
	results := make(chan int, callers)
	go func() {
		pvs, ok := s.server.listVolumesForFanout(context.Background())
		assert.True(t, ok)
		results <- len(pvs)
	}()
	<-lister.entered
	for i := 1; i < callers; i++ {
		go func() {
			pvs, ok := s.server.listVolumesForFanout(context.Background())
			assert.True(t, ok)
			results <- len(pvs)
		}()
	}
	waitForGoroutineIn(t, "(*ReplicationServer).listVolumesForFanout", "sync.(*Mutex).Lock")
	close(lister.release)
	for i := 0; i < callers; i++ {
		assert.Equal(t, 1, <-results)
	}
	assert.Equal(t, 1, lister.callCount())
}

// Enable landing between DeleteVolume's replication guard and its delete.
// The guard (guardReplicatedDelete) reads the relationship table and finds
// nothing; the delete then runs the PersistentVolume ownership check,
// the export, quota and tree delete. The lister below blocks inside that
// ownership check -- after the guard, before anything is removed -- while
// the test enables replication on the volume. A delete that went on to
// tree-delete the directory would end the new relationship on both arrays
// (ENDED, measured on Core and modelled by endRelsUnder) and leave B holding
// a replica of a volume that no longer exists.
func TestDeleteVolumeRacingEnableNeverLeavesAnEndedRelationship(t *testing.T) {
	s := newReplScenario(t)
	lister := newGatedPVLister(PersistentVolumeInfo{Name: replVolumeName, Handle: s.handleA, Phase: "Released"})
	s.driver.PersistentVolumeLister = lister

	deleted := make(chan error, 1)
	go func() {
		_, err := s.controller(t).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: s.handleA, Secrets: deleteSecrets(),
		})
		deleted <- err
	}()
	<-lister.entered // the guard has passed: no relationship yet

	require.NoError(t, s.enable(t, classToB()))
	rel := s.fabric.onlyRel()
	require.Equal(t, qumulo.RelationshipEstablished, rel.state)

	close(lister.release)
	err := <-deleted

	assert.NotEqual(t, qumulo.RelationshipEnded, rel.state,
		"the delete went ahead over a relationship created after its guard, ending it on both arrays")
	if err == nil {
		t.Fatalf("DeleteVolume answered OK over a protected volume (relationship %s, %s)", rel.id, rel.state)
	}
	requireCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), rel.id)
	assert.Contains(t, s.a.dirs, replVolumePath, "the protected directory was not deleted")
	assert.NotEmpty(t, s.a.exports, "nothing was dismantled")
	assert.Equal(t, replQuota, s.a.quotas[s.fileIDA], "nothing was dismantled")
}

// gatedMountMounter records each Mount, for the forced-unmount gate test.
type gatedMountMounter struct {
	*blockingMounter
	mounted chan string
}

func (g *gatedMountMounter) Mount(source, target, _ string, _ []string) error {
	g.mounted <- target
	return nil
}

// A publish waits behind a forced unmount. umount -f aborts every in-flight
// request on an NFS superblock, which the kernel shares across mounts from
// one server with the same options, so the ladder's -f rung holds
// forceGate's write lock and every mount(8) holds its read lock
// (node_server.go mountWithTimeout, node_unmount.go): a mount started while
// -f runs does not reach mount(8) until -f is done, and so cannot join the
// superblock -f is aborting. The other direction, -f skipped while a mount
// is in flight, is TestUnpublishSkipsTheForcedUnmountWhileAMountIsInFlight.
func TestPublishWaitsBehindAForcedUnmount(t *testing.T) {
	target := resolvedTempDir(t)
	bm := &blockingMounter{
		unmountErr: errors.New("exit status 16: umount.nfs4: " + target + ": Stale file handle"),
		table:      []mount.MountPoint{{Path: target, Device: "192.0.2.10:/csi/volumes/pvc-a", Type: "nfs4"}},
	}
	m := &gatedMountMounter{blockingMounter: bm, mounted: make(chan string, 1)}
	ns := newUnmountTestServer(t, bm, umountForce, nil)
	ns.mounter = m
	ns.mountTimeout = time.Minute // the publish waits; it does not time out

	inForce := make(chan struct{})
	releaseForce := make(chan struct{})
	var order []string
	var orderMu sync.Mutex
	note := func(s string) { orderMu.Lock(); order = append(order, s); orderMu.Unlock() }
	ns.forceUnmount = func(_ context.Context, flag, tgt string) error {
		bm.record("umount " + flag)
		if flag == umountForce {
			close(inForce)
			<-releaseForce
			note("umount -f done")
			bm.clear(tgt)
		}
		return nil
	}

	unpublished := make(chan error, 1)
	go func() { unpublished <- unpublish(t, ns, target) }()
	<-inForce // -f is running, holding forceGate

	other := resolvedTempDir(t)
	published := make(chan error, 1)
	go func() {
		published <- ns.mountWithTimeout(context.Background(), "192.0.2.10:/csi/volumes/pvc-b", other, "nfs", nil)
	}()
	waitForGoroutineIn(t, "(*NodeServer).mountWithTimeout", "sync.(*RWMutex).RLock")
	select {
	case got := <-m.mounted:
		t.Fatalf("mount(8) of %s ran while umount -f held the gate", got)
	default:
	}

	close(releaseForce)
	require.NoError(t, <-unpublished)
	assert.Equal(t, other, <-m.mounted)
	note("mount")
	require.NoError(t, <-published)
	assert.Equal(t, []string{"umount -f done", "mount"}, order)
	assert.Contains(t, bm.recorded(), "umount "+umountForce, "the target was alone on its superblock, so -f ran")
}
