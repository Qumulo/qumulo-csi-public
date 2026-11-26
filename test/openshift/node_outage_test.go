//go:build openshift

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Clean shutdown when the Qumulo array is unreachable, against real mounts.
//
// The forced-unmount ladder -- a bounded plain umount, then `umount -f` once
// the server is confirmed gone, then `umount -l` -- is what lets a pod on a
// dead array terminate inside its grace period and leaves no mount behind on
// the node. Everything that tests it today either scripts the mounter (the 40
// cases in pkg/driver) or needs a Kubernetes cluster and a fault injector
// (test/openshift/live). The first cannot see
// what the kernel does; the second cannot run unattended.
//
// These two run the ladder against a real NFS mount on this host, with the
// array made genuinely unreachable by a firewall rule, and need no cluster at
// all. That is what makes them the part of this feature a nightly can cover.
//
// They are gated three deep, because a blackhole rule on a shared lab runner
// is not something to acquire by accident:
//
//	QUMULO_LIVE_TEST=1    the live suite at all
//	QUMULO_LIVE_NODE=1    this host consents to being mounted on (make live-node)
//	QUMULO_LIVE_OUTAGE=1  this host consents to losing its route to the array
//
// Without the third they skip with the reason, which is deliberate: a skipped
// case in an unattended run must be legible, never silent.
//
// It lives in the OpenShift tier, not in test/live, so one nightly command --
// `go test -tags openshift ./test/openshift/` -- covers every storage-outage
// case this repository has. It needs no cluster: the harness it uses is
// test/livekit, which was lifted out of test/live for exactly this.
//
// Blast radius: TCP 2049 to the array under test is dropped from THIS HOST for
// the length of one unpublish, then restored -- by the test, by a detached
// self-removing timer if the test dies, and by t.Cleanup either way. Any other
// live test running on the same machine at the same time will see the array
// disappear. Run this serially; the suite's own concurrency rule says the same
// about the arrays.
package openshift

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"qumulo-csi-driver/test/livekit"
)

// nftTable is the nft table this test creates and deletes. Its own, so that
// removing the fault cannot touch a rule the runner had before.
const nftTable = "qcsi_outage"

// unpublishBudget is the bar: the kubelet gives NodeUnpublishVolume a fixed
// two minutes, and a pod's grace period is typically the same. A ladder that
// needs longer than this does not help a terminating pod, however correct it
// is. Override with QUMULO_OUTAGE_BUDGET for a deliberately slow lab.
func unpublishBudget(t *testing.T) time.Duration {
	t.Helper()

	if raw := os.Getenv("QUMULO_OUTAGE_BUDGET"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("QUMULO_OUTAGE_BUDGET=%q is not a duration: %v", raw, err)
		}
		return d
	}
	return 120 * time.Second
}

// requireOutageFault skips unless this host has opted into losing its route to
// the array, and unless a firewall this test knows how to drive is present.
func requireOutageFault(t *testing.T) string {
	t.Helper()

	livekit.RequireNodeMounting(t)

	if os.Getenv("QUMULO_LIVE_OUTAGE") != "1" {
		t.Skip("this test drops TCP 2049 to the array from this host for the length of one unpublish; " +
			"set QUMULO_LIVE_OUTAGE=1 to allow it, and run it with nothing else using the array on this machine")
	}

	// Settle which firewall is actually here rather than assuming iptables.
	// A rule that silently fails to install would leave the test measuring a
	// healthy array and reporting a pass, which is the worst outcome
	// available -- so this resolves it up front and skips if it cannot.
	if out, err := exec.Command("iptables", "--version").CombinedOutput(); err == nil {
		t.Logf("fault backend: %s", strings.TrimSpace(string(out)))
		return "iptables"
	}
	if _, err := exec.LookPath("nft"); err == nil {
		t.Log("fault backend: nft")
		return "nft"
	}

	t.Skip("neither iptables nor nft is available, so the array cannot be made unreachable from this host")
	return ""
}

// blackhole drops NFS to addr and returns a function that restores it.
//
// Two safety nets, because an unrestored rule on a lab runner breaks every
// later run on that machine: a detached shell removes the rule after
// safetyLift regardless of what happens to this process, and the returned
// lift is also registered with t.Cleanup by the caller.
func blackhole(t *testing.T, backend, addr string, safetyLift time.Duration) func() {
	t.Helper()

	// On nft the rule goes in a table of this test's own, never in the host's
	// `inet filter`. Removing it is then `delete table`, which takes exactly
	// what this test added -- where flushing a shared output chain would take
	// the runner's other rules with it, and a lab runner's firewall is not
	// this test's to empty.
	var add [][]string
	var del []string
	switch backend {
	case "iptables":
		add = [][]string{{"iptables", "-I", "OUTPUT", "-d", addr, "-p", "tcp", "--dport", "2049", "-j", "DROP"}}
		del = []string{"iptables", "-D", "OUTPUT", "-d", addr, "-p", "tcp", "--dport", "2049", "-j", "DROP"}
	default:
		add = [][]string{
			{"nft", "add", "table", "inet", nftTable},
			{"nft", "add", "chain", "inet", nftTable, "output",
				"{ type filter hook output priority 0 ; policy accept ; }"},
			{"nft", "add", "rule", "inet", nftTable, "output",
				"ip", "daddr", addr, "tcp", "dport", "2049", "drop"},
		}
		del = []string{"nft", "delete", "table", "inet", nftTable}
	}

	for _, cmd := range add {
		if out, err := exec.Command(cmd[0], cmd[1:]...).CombinedOutput(); err != nil {
			// Undo whatever landed before giving up, so a half-installed
			// fault never outlives the test.
			_ = exec.Command(del[0], del[1:]...).Run()
			t.Fatalf("installing the blackhole for %s (%s): %v: %s", addr, strings.Join(cmd, " "), err, out)
		}
	}
	t.Logf("blackholed TCP 2049 to %s from this host", addr)

	// Detached safety net. `setsid sh -c 'sleep N; <del>'` outlives this
	// process, so a panic or a killed test run still restores the route.
	safety := exec.Command("setsid", "sh", "-c",
		fmt.Sprintf("sleep %d; %s >/dev/null 2>&1", int(safetyLift.Seconds()), strings.Join(del, " ")))
	if err := safety.Start(); err != nil {
		t.Logf("warning: could not start the safety-net timer (%v); the rule relies on cleanup alone", err)
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			if out, err := exec.Command(del[0], del[1:]...).CombinedOutput(); err != nil {
				t.Logf("lifting the blackhole for %s: %v: %s (the safety net will retry)", addr, err, out)
				return
			}
			t.Logf("restored the route to %s", addr)
		})
	}
}

// publishForOutage provisions a volume, mounts it for real and returns the
// target. The mount is left in place: the test's whole subject is what
// unpublishing it does once the array is gone.
func publishForOutage(ctx context.Context, t *testing.T, h *livekit.Harness, name string) (livekit.ProvisionedVolume, string) {
	t.Helper()

	volume := livekit.Provision(ctx, t, h, name, livekit.GiB)
	target := filepath.Join(t.TempDir(), "mount")
	if err := os.MkdirAll(target, 0o750); err != nil {
		t.Fatalf("creating the target path: %v", err)
	}

	if _, err := h.Driver.NodeServer().NodePublishVolume(ctx,
		livekit.PublishRequest(h, volume, target, "nfs4.1")); err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}
	if _, mounted := livekit.MountTable(t, target); !mounted {
		t.Fatalf("NodePublishVolume reported success but nothing is mounted at %s", target)
	}

	// Every mount made here is unmounted here, including the one the test is
	// about: NodeUnpublishVolume is idempotent, so a second call after the
	// test's own is a no-op, and a test that fails before unmounting does not
	// leave an NFS mount on a shared runner. A leaked mount is not a tidiness
	// problem -- t.TempDir's own cleanup would then try to remove a directory
	// with a live mount in it, and the next `df` on that machine hangs.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		if _, err := h.Driver.NodeServer().NodeUnpublishVolume(cleanupCtx, &csi.NodeUnpublishVolumeRequest{
			VolumeId:   volume.VolumeID,
			TargetPath: target,
		}); err != nil {
			t.Errorf("cleanup: unpublishing %s: %v -- it may need `umount -l` by hand before this runner "+
				"is used again", target, err)
		}
	})

	return volume, target
}

// TestLiveNodeUnpublishOfAnUnreachableArrayFinishesAndLeavesNoMount is the
// measurement the feature is graded on, at host level.
//
// On Kubernetes with the array powered off, pods terminated on their own in
// 25 to 81 seconds and no mount was left behind. This asserts the same two
// things about the RPC underneath that: it returns inside the kubelet's
// deadline, and the target is gone from the kernel's mount table afterwards.
//
// Both halves matter. An unpublish that returns quickly and leaves the mount
// is what force-deleting a pod does, and it is the behaviour the ladder's
// third rung exists to avoid.
func TestLiveNodeUnpublishOfAnUnreachableArrayFinishesAndLeavesNoMount(t *testing.T) {
	backend := requireOutageFault(t)
	h := livekit.NewHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	volume, target := publishForOutage(ctx, t, h, "node-outage")
	livekit.RequireWritable(t, target)

	budget := unpublishBudget(t)
	lift := blackhole(t, backend, h.NFSServer, budget+2*time.Minute)
	t.Cleanup(lift)

	// Let the mount notice. Nothing is asserted about this window; it is here
	// so the unmount meets an already-dead connection rather than racing the
	// rule.
	time.Sleep(5 * time.Second)

	started := time.Now()
	_, err := h.Driver.NodeServer().NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volume.VolumeID,
		TargetPath: target,
	})
	took := time.Since(started)
	t.Logf("NodeUnpublishVolume against an unreachable array took %s (budget %s)", took.Round(time.Second), budget)

	if err != nil {
		t.Fatalf("NodeUnpublishVolume: %v -- a pod on this volume would stay Terminating", err)
	}
	if took > budget {
		t.Errorf("the unpublish took %s, over the %s budget: the kubelet's deadline would end the call first "+
			"and the pod would not finish terminating", took.Round(time.Second), budget)
	}
	if line, mounted := livekit.MountTable(t, target); mounted {
		t.Errorf("NodeUnpublishVolume reported success and %s is still mounted: %s\n"+
			"a stale mount here is what the lazy rung exists to prevent; the node accumulates one per dead volume",
			target, line)
	}
}

// TestLiveNodeUnpublishOfAnUnreachableArraySparesASharedSuperblock checks the
// shared-superblock case at host level and with real I/O.
//
// Two volumes from one array through one set of mount options share a single
// NFS superblock. `umount -f` on either aborts the in-flight requests of
// *both*, which returns an I/O error to a pod that has nothing to do with the
// volume being unmounted -- measured, before the fix, as `Unknown error 512`
// in a bystander pod. The driver answers that by skipping straight to
// `umount -l` whenever another mount shares the target's device number.
//
// The unit tests assert the decision; only this one can see the consequence,
// because the consequence is a syscall returning EIO to a writer that was
// mid-write when the forced rung ran.
func TestLiveNodeUnpublishOfAnUnreachableArraySparesASharedSuperblock(t *testing.T) {
	backend := requireOutageFault(t)
	h := livekit.NewHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	h.RequireCoveringExport(ctx, t)

	doomed, doomedTarget := publishForOutage(ctx, t, h, "node-outage-a")
	_, bystanderTarget := publishForOutage(ctx, t, h, "node-outage-b")
	livekit.RequireWritable(t, doomedTarget)
	livekit.RequireWritable(t, bystanderTarget)

	if !sameSuperblock(t, doomedTarget, bystanderTarget) {
		t.Skip("the two mounts did not land on one superblock, so there is no shared-superblock case to test " +
			"here -- the kernel coalesces only identical server-and-options mounts")
	}

	// A writer on the bystander, running throughout. Under a `hard` mount a
	// write blocks while the array is away and completes once it is back; an
	// aborted superblock turns that into EIO instead, which is the whole
	// signal.
	writer := startWriter(t, bystanderTarget)

	budget := unpublishBudget(t)
	lift := blackhole(t, backend, h.NFSServer, budget+3*time.Minute)
	t.Cleanup(lift)
	time.Sleep(5 * time.Second)

	if _, err := h.Driver.NodeServer().NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   doomed.VolumeID,
		TargetPath: doomedTarget,
	}); err != nil {
		t.Errorf("NodeUnpublishVolume: %v", err)
	}

	lift()
	// The bystander's blocked write has to come back before its result means
	// anything.
	time.Sleep(20 * time.Second)
	errs := writer.stop()

	for _, e := range errs {
		if errors.Is(e, os.ErrClosed) {
			continue
		}
		t.Errorf("the bystander mount at %s returned an error while the other volume was force-unmounted: %v\n"+
			"a forced unmount reached a healthy volume on the same NFS superblock",
			bystanderTarget, e)
	}

	if _, mounted := livekit.MountTable(t, bystanderTarget); !mounted {
		t.Errorf("the bystander mount at %s is gone; unpublishing one volume unmounted another", bystanderTarget)
	}
}

// sameSuperblock reports whether two mount points share a device number, which
// is what makes `umount -f` on one reach the other.
func sameSuperblock(t *testing.T, a, b string) bool {
	t.Helper()

	devs := map[string]string{}
	raw, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatalf("reading /proc/self/mountinfo: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		devs[fields[4]] = fields[2] // mount point -> major:minor
	}

	t.Logf("superblocks: %s=%s %s=%s", a, devs[a], b, devs[b])
	return devs[a] != "" && devs[a] == devs[b]
}

// backgroundWriter writes through a mount until stopped, keeping every error.
type backgroundWriter struct {
	done   chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	errors []error
}

func startWriter(t *testing.T, target string) *backgroundWriter {
	t.Helper()

	w := &backgroundWriter{done: make(chan struct{})}
	probe := filepath.Join(target, "csi-outage-bystander")

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for i := 0; ; i++ {
			select {
			case <-w.done:
				return
			default:
			}

			// O_APPEND, one open per write: the point is to have a write in
			// flight when the forced rung runs, not to hold a descriptor.
			f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err == nil {
				_, err = f.WriteString(strconv.Itoa(i) + "\n")
				if cerr := f.Close(); err == nil {
					err = cerr
				}
			}
			if err != nil {
				w.mu.Lock()
				w.errors = append(w.errors, err)
				w.mu.Unlock()
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	return w
}

func (w *backgroundWriter) stop() []error {
	close(w.done)

	// A writer blocked in a syscall on a hard mount cannot be interrupted;
	// wait, but do not hang the suite if the array never comes back.
	finished := make(chan struct{})
	go func() { w.wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(2 * time.Minute):
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]error(nil), w.errors...)
}
