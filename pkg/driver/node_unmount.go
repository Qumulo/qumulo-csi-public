// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

// Forced unmount: the node half of the forced-unmount rule.
//
// Measured on the DR lab: with the array powered off for
// 25 minutes, pods on `hard`, `softerr`, `softreval` and `fatal_neterrors`
// mounts *all* failed to terminate inside the 120 s grace period, because
// NodeUnpublishVolume did a plain unmount and umount(8) blocks on a dead NFS
// server exactly as I/O does. The mount option made no difference: only the
// unmount did. Force-deleting the pods then left the mounts behind on the
// node, so the next pod scheduled there inherited the wreckage.
//
// The fix is the one the rest of the industry already ships
// (day-two-research/industry-failover-addressing.md, section (b)):
//
//   - csi-driver-nfs: mount.CleanupMountWithForce(..., 30*time.Second) --
//     plain umount, then `umount -f` after 30 s. No lazy unmount.
//   - Trident: ExecuteWithTimeout("umount", umountTimeout), then on timeout
//     "Umount failed, attempting to force umount" and a retry with -f.
//   - AWS EFS CSI: the same, opt-in behind --force-unmount-after-timeout with
//     --unmount-timeout (default 30 s).
//
// This driver goes one rung further and ends with `umount -l`, because the
// measured failure was not only "the pod did not terminate" but "the mount was
// still on the node afterwards". `umount -f` gives up on a server that never
// answers; it does not detach a mount whose references the kernel still holds,
// and that is the state force-deleting a pod leaves behind. A lazy unmount
// detaches the tree immediately and cleans up when the last reference goes,
// which is what makes the node's mount table -- the thing this RPC is
// answering about -- actually correct when it returns.
//
// The ladder is deliberately not taken on every unmount. A `umount` that fails
// because something on this node still has the volume open is a *live* volume,
// and detaching it lazily would hide an in-flight writer from the operator
// while its writes go into a detached mount. So the escalation fires only on
// the signatures that mean the server, not the mount, is the problem: an error
// naming an unresponsive or stale server (unmountEscalationTriggers), or any
// other failure -- a timeout, or "busy" from a thread stuck in the kernel on
// the dead mount -- WITH the server failing a bounded statfs(2) on the
// mount.
// A timeout alone is not enough: a live array flushing gigabytes
// of dirty pages at unmount can outlive 30 s too, and `umount -f` would abort
// exactly those writes. csi-driver-nfs and Trident escalate on the
// timeout alone; this driver asks the server first, because its ladder goes
// one rung further than theirs.

// defaultUnmountTimeout bounds the plain umount(8) before the escalation
// starts.
//
// 30 s, matching csi-driver-nfs' CleanupMountWithForce constant and EFS CSI's
// --unmount-timeout default, and chosen against the number that actually
// matters: the pod's terminationGracePeriodSeconds, 30 s in this chart for the
// node plugin and 30 s by Kubernetes default for a workload pod. Three rungs
// of 30 s would outlive the grace period on their own, so only the first rung
// gets the full budget; the forced rungs are bounded by the same value but do
// not block in the kernel the way the plain one does.
//
// Separate from defaultMountTimeout (90 s) on purpose. A mount that is slow is
// usually a mount that will succeed -- NFS negotiation, DNS, a busy array --
// and giving up on it early turns a slow start into a failed one. An unmount
// that is slow is a server that is not answering, and waiting longer only
// makes the pod's termination later.
const defaultUnmountTimeout = 30 * time.Second

// umountForce and umountLazy are the two escalation rungs, spelled as the
// flags they pass to umount(8) so the logs and the man page agree.
const (
	umountForce = "-f"
	umountLazy  = "-l"
)

// unmountEscalationTriggers are the umount(8) failures that mean the *server*
// is the problem and a forced unmount is therefore the right answer.
//
// Matched on the error text because umount(8) is an exec, not a syscall: the
// exit status is 1 for everything, and the message is all there is. The list
// is short on purpose -- anything not on it (most importantly "target is
// busy", which means a live reference on this node) escalates only if the
// server also fails a bounded statfs, and is otherwise returned to the CO
// unchanged, so a working volume is never force-detached.
var unmountEscalationTriggers = []string{
	"not responding",       // umount.nfs: "Server not responding"
	"connection timed out", // RPC gave up
	"stale file handle",    // the export or the array was replaced under us
	"no route to host",
	"network is unreachable",
	"input/output error",
}

// shouldEscalateUnmount reports whether err, on its own, says the server is
// gone and `umount -f` is therefore the right answer.
//
// A timeout is deliberately NOT one of these. The unmount outliving its bound
// is the measured dead-array case, but it is also what a live array flushing
// a large dirty page cache looks like, and the two are told apart by asking
// the server (mountServerUnresponsive), not by the clock.
func shouldEscalateUnmount(err error) bool {
	if err == nil {
		return false
	}
	// A dead mount answers every syscall with one of the errnos mount-utils
	// enumerates, umount(8) included.
	if isDeadMountError(err) {
		return true
	}

	text := strings.ToLower(err.Error())
	for _, trigger := range unmountEscalationTriggers {
		if strings.Contains(text, trigger) {
			return true
		}
	}
	return false
}

// errEBADHANDLE is Linux's EBADHANDLE, errno 521, which Go's syscall package
// has no name for. NFS returns it for a file handle the server no longer
// recognises -- a Qumulo export that was deleted, even one re-created at the
// same path -- and only a new mount clears it.
const errEBADHANDLE = syscall.Errno(521)

// isCorruptedMount is mount.IsCorruptedMnt plus EBADHANDLE.
//
// mount-utils counts ENOTCONN, ESTALE, EIO, EACCES and EHOSTDOWN as a broken
// mount, but not EBADHANDLE, so a mount broken by a removed export read as a
// generic stat failure: NodeGetVolumeStats answered an anonymous Internal,
// NodeGetVolumeHealth graded it DEGRADED rather than StaleMount, and the
// publish-time stale-mount recovery would not fire. Every check here goes
// through this one, so the three agree.
func isCorruptedMount(err error) bool {
	return mount.IsCorruptedMnt(err) || errors.Is(err, errEBADHANDLE)
}

// isStaleHandle reports whether err says the server no longer honours the
// mount's file handle: ESTALE, or EBADHANDLE (errno 521), which Core answers
// once the export a mount came through is removed. Narrower than
// isCorruptedMount on purpose. That one follows mount-utils, which also
// counts EACCES and EIO, and a republish must not remount or unmount a mount
// a pod is using on a permission error.
func isStaleHandle(err error) bool {
	return errors.Is(err, syscall.ESTALE) || errors.Is(err, errEBADHANDLE)
}

// isRepublishDeadMount is the single test both republish paths apply before
// they unmount whatever is at the target.
//
// They used to disagree. The IsLikelyNotMountPoint path asked
// isCorruptedMount, which follows mount-utils and counts EACCES and EIO as a
// broken mount; the stat path asked isStaleHandle, which does not. So a
// mount a pod was happily using could be force-unmounted under it because
// one stat came back EACCES -- a permission change on the export, or an
// idmap hiccup -- or EIO, which a healthy mount returns for a single bad
// read. Neither says the mount is dead.
//
// What does: the server no longer honours the handle (ESTALE, EBADHANDLE),
// or the transport endpoint is gone (ENOTCONN). Only a new mount clears any
// of those, so unmounting is the only way forward and nothing is lost.
//
// isCorruptedMount stays as it is for the paths that only *report* --
// NodeGetVolumeStats and NodeGetVolumeHealth -- where naming a degraded
// mount costs nothing and matching mount-utils is the right behaviour.
func isRepublishDeadMount(err error) bool {
	return isStaleHandle(err) || errors.Is(err, syscall.ENOTCONN)
}

// isDeadMountError reports whether err is one of the errnos a mount whose
// server has stopped answering hands back to every syscall.
func isDeadMountError(err error) bool {
	if isCorruptedMount(err) {
		return true
	}
	for _, errno := range []syscall.Errno{syscall.ETIMEDOUT, syscall.EIO, syscall.ESTALE, syscall.ENOTCONN} {
		if errors.Is(err, errno) {
			return true
		}
	}
	return false
}

// mountServerUnresponsive answers the question a timed-out unmount leaves
// open: is the server behind target still answering at all?
//
// A statfs(2) on the mount is the cheapest round trip there is. A live server
// answers it in milliseconds whatever its unmount is doing -- the flush that
// makes an unmount slow does not stop the server answering a FSSTAT -- and a
// dead one either leaves it hanging past ns.statTimeout or fails it with one
// of the dead-mount errnos. Anything else the call returns (ENOENT, EACCES) is
// an answer, and an answer means the server is there.
//
// Residual, stated plainly: a live array so loaded that it cannot answer a
// statfs inside ns.statTimeout (10 s by default) is judged unresponsive and
// its unmount is forced; and a dead array is now detected statTimeout later
// than before. Both are the safe direction for data: the first needs an array
// that is already failing its clients, the second costs seconds of pod
// termination time.
func (ns *NodeServer) mountServerUnresponsive(ctx context.Context, target string) bool {
	_, err := ns.statfsWithTimeout(ctx, target)
	switch {
	case err == nil:
		return false
	case errors.Is(err, errStatTimeout):
		return true
	case errors.Is(err, syscall.EACCES):
		// isDeadMountError counts it, after mount-utils, but a server that
		// refuses the call has answered it. Since any busy unmount now asks
		// this, reading it as gone would force a live volume after a
		// permission change on its export.
		return false
	case isDeadMountError(err):
		return true
	default:
		return false
	}
}

// cleanupPublishedMount unmounts targetPath, escalating as far as it has to,
// and then removes the directory the kubelet published into.
//
// This replaces mount.CleanupMountPoint, which the first cut of
// NodeUnpublishVolume used. Two differences, both measured:
//
//  1. It never stats the target to decide whether it is mounted. stat(2) is
//     precisely what blocks on a dead NFS mount, so the check that was meant
//     to make the unmount safe was itself the thing that hung. The mount table
//     answers the same question, does not touch the server, and is also the
//     condition this RPC is required to leave true: gone from /proc/mounts.
//  2. It escalates. See the file comment.
func (ns *NodeServer) cleanupPublishedMount(ctx context.Context, targetPath string) error {
	mounted, err := ns.isInMountTable(targetPath)
	if err != nil {
		return fmt.Errorf("reading the mount table for %s: %w", targetPath, err)
	}

	if mounted {
		if err := ns.unmountWithEscalation(ctx, targetPath); err != nil {
			return err
		}
	}

	// The kubelet expects the publish directory gone, not merely unmounted:
	// it is what NodePublishVolume created, and an empty directory left behind
	// is indistinguishable from a volume that was never unmounted for anything
	// walking the kubelet's pods directory. mount.CleanupMountPoint did this
	// too; it is kept so the RPC's observable effect is unchanged.
	err = os.Remove(targetPath)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	if !isDirNotEmpty(err) {
		return fmt.Errorf("removing %s after unmounting it: %w", targetPath, err)
	}

	// Not a mount, and not empty: writes landed on the node's own disk
	// after the NFS mount went away. Removing it would delete them; leaving
	// it failed every retry with "directory not empty", and the pod stayed
	// Terminating for ever. It is moved out of the kubelet's tree,
	// so the data is kept and the unpublish completes.
	//
	// Out of the tree, not beside the target: after this RPC answers OK the
	// kubelet's TearDownAt removes vol_data.json and then rmdir(2)s the
	// target's parent, and anything left in it fails that rmdir, fails the
	// teardown and keeps the pod Terminating -- the same outcome, reported by
	// the kubelet instead of the driver. The kubelet never deletes a
	// non-empty volume directory either; its orphaned-pod cleanup only
	// rmdir(2)s it and logs the failure every housekeeping pass.
	aside, err := orphanDestination(targetPath, ns.driver.Name, time.Now())
	if err == nil {
		err = os.Rename(targetPath, aside)
	}
	if err != nil {
		return status.Errorf(codes.FailedPrecondition,
			"%s is not mounted but holds files -- writes that reached the node's own disk after the NFS mount "+
				"went away -- and could not be moved to %s (%v); nothing was deleted. Copy anything needed out of "+
				"it, then remove the directory, and the unpublish completes", targetPath, aside, err)
	}
	slog.ErrorContext(ctx, "nodeunpublishvolume: the target held files written to the node's own disk after its "+
		"mount went away; moved to movedTo, nothing deleted. Copy anything needed back to the volume, then "+
		"remove it", "targetPath", targetPath, "movedTo", aside)
	return nil
}

// orphanDestination names where cleanupPublishedMount moves a target that
// holds data, and creates its parent (0700: the files are a pod's, and
// nothing but an operator should read them).
//
// A kubelet target is <kubeletDir>/pods/<podUID>/volumes/kubernetes.io~csi/<pv>/mount,
// and the destination is <kubeletDir>/<driver>-orphaned/<pv>-<podUID>-<unix>:
// a directory the driver owns, on the filesystem the target is on, since the
// node plugin mounts the whole kubeletDir at the same path, so the move is a
// rename(2) and copies nothing. A target under a pods directory in some other
// layout keeps the same parent and is named after its path below kubeletDir.
// A target under no pods directory at all was not published by a kubelet,
// nothing will rmdir its parent, and it is moved beside itself as before.
//
// Deliberately *beside* the plugin directory and not inside it. The data is a
// pod's only copy, and <kubeletDir>/plugins/<driver> is the directory holding
// csi.sock -- the one an operator removes to clean a node after uninstalling
// the driver. Keeping rescued data out of it means no plugin-directory
// cleanup, of any shape, can reach what this function saved.
func orphanDestination(targetPath, driverName string, now time.Time) (string, error) {
	clean := filepath.Clean(targetPath)
	i := strings.LastIndex(clean, "/pods/")
	if i < 0 {
		return fmt.Sprintf("%s.orphaned-%d", clean, now.Unix()), nil
	}
	kubeletDir, rel := clean[:i], clean[i+len("/pods/"):]

	var name string
	if p := strings.Split(rel, "/"); len(p) == 5 && p[1] == "volumes" && p[2] == "kubernetes.io~csi" && p[4] == "mount" {
		name = fmt.Sprintf("%s-%s-%d", p[3], p[0], now.Unix())
	} else {
		name = fmt.Sprintf("%s-%d", strings.ReplaceAll(rel, "/", "_"), now.Unix())
	}
	driverDir := strings.ReplaceAll(driverName, "/", "_")
	if driverDir == "" || driverDir == "." || driverDir == ".." {
		driverDir = "csi.qumulo.com"
	}
	parent := filepath.Join(kubeletDir, driverDir+"-orphaned")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return parent, err
	}

	// Two unpublishes of the same volume in one second would otherwise share
	// a name, and rename(2) onto an existing empty directory replaces it.
	dest := filepath.Join(parent, name)
	for n := 1; ; n++ {
		if _, err := os.Lstat(dest); os.IsNotExist(err) {
			return dest, nil
		} else if err != nil {
			return dest, err
		}
		dest = filepath.Join(parent, fmt.Sprintf("%s.%d", name, n))
	}
}

// isDirNotEmpty reports whether err is ENOTEMPTY or EEXIST, the two errnos
// rmdir(2) answers for a directory that still has entries.
func isDirNotEmpty(err error) bool {
	return errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST)
}

// unmountWithEscalation runs the ladder: umount, umount -f, umount -l, and
// stops at the first rung after which the mount table no longer lists target.
//
// The mount table is re-read after *every* rung, including a rung that
// reported success, because those two are not the same claim. `umount`
// returning 0 says the call was accepted; the mount being absent from
// /proc/mounts says the node is actually clean, and that is what this RPC
// tells the CO by returning OK.
func (ns *NodeServer) unmountWithEscalation(ctx context.Context, target string) error {
	plain := ns.plainUnmountBudget(ctx)
	if plain < ns.unmountTimeout {
		slog.InfoContext(ctx, "nodeunpublishvolume: plain unmount budget cut to fit the caller's deadline, so the "+
			"escalation can still run", "targetPath", target, "unmountTimeout", ns.unmountTimeout, "budget", plain)
	}
	rungs := []struct {
		name string
		run  func() error
	}{
		{name: "umount", run: func() error { return ns.unmountWithTimeoutOf(ctx, target, plain) }},
		{name: "umount " + umountForce, run: func() error {
			// A forced unmount aborts every in-flight request on the NFS
			// superblock, and the kernel shares one superblock across every
			// mount from the same server with the same options -- other
			// pods' volumes, which nobody is unmounting. So -f runs
			// only for a target that is the superblock's last mount, with no
			// mount being made beside it; otherwise the ladder goes straight
			// to -l, which detaches the target and aborts nothing.
			if !ns.forceGate.TryLock() {
				slog.WarnContext(ctx, "nodeunpublishvolume: skipping umount -f: a mount is being made on this "+
					"node, and it may share the target's superblock; using umount -l", "targetPath", target)
				return nil
			}
			defer ns.forceGate.Unlock()
			if sharers, err := ns.superblockSharers(target); err != nil || len(sharers) > 0 {
				slog.WarnContext(ctx, "nodeunpublishvolume: skipping umount -f: other mounts share the target's "+
					"NFS superblock, and -f would abort their in-flight I/O; using umount -l",
					"targetPath", target, "sharedWith", sharers, "mountinfoError", err)
				return nil
			}
			// Half of what is left, so a -f that blocks after all cannot
			// take the time the -l after it needs.
			fctx, cancel := shareOfRemaining(ctx, 2)
			defer cancel()
			return ns.forceUnmount(fctx, umountForce, target)
		}},
		{name: "umount " + umountLazy, run: func() error { return ns.forceUnmount(ctx, umountLazy, target) }},
	}

	var firstErr error
	for i, rung := range rungs {
		err := rung.run()
		if err != nil && firstErr == nil {
			firstErr = err
		}

		gone, listErr := ns.isNotInMountTable(target)
		if listErr != nil {
			return fmt.Errorf("reading the mount table for %s: %w", target, listErr)
		}
		if gone {
			if i > 0 {
				slog.WarnContext(ctx, "nodeunpublishvolume: target unmounted only after escalating",
					"targetPath", target, "escalatedTo", rung.name, "firstError", firstErr)
			}
			return nil
		}

		if i == len(rungs)-1 {
			break
		}
		// Whether to *enter* the ladder is decided once, on the plain
		// unmount's own failure, and only the server-is-gone signatures open
		// it. A plain failure for any other reason -- "target is busy" above
		// all -- is a live reference on this node, and forcing past it would
		// detach a volume something is still writing to. A timeout opens it
		// only when the server also fails a bounded statfs: a slow unmount on
		// a live array is returned to the CO to retry, with its writes
		// intact. Once the ladder is open, a failing rung does not close it
		// again: the server has already been judged gone, and `umount -f`
		// failing is precisely the case `umount -l` exists for.
		//
		// Every other failure asks the server the same way, "busy" included.
		// A statfs that outlived statTimeout -- NodeGetVolumeStats,
		// which the kubelet polls, on the dead mount -- leaves its thread
		// blocked in the kernel holding the mount, so umount answers "device
		// is busy" (exit 16) at once rather than hanging. The ladder never
		// opened and the pod stayed Terminating until the array came back
		// (measured on OpenShift 4.22 RHCOS 9.8 and on kind). Busy on a
		// server that answers is still a live reference and is returned
		// unchanged.
		if i == 0 && err != nil && !shouldEscalateUnmount(err) {
			timedOut := errors.Is(err, errMountTimeout)
			if !ns.mountServerUnresponsive(ctx, target) {
				if timedOut {
					return fmt.Errorf("unmounting %s: %w; the server still answers a statfs on the mount, so the unmount "+
						"is slow rather than hung and is not forced (a forced unmount would abort its in-flight writes); "+
						"the CO retries", target, err)
				}
				return fmt.Errorf("unmounting %s: %w", target, err)
			}
			slog.WarnContext(ctx, "nodeunpublishvolume: unmount failed and the server fails a statfs; treating it as unreachable",
				"targetPath", target, "timedOut", timedOut, "error", err, "statTimeout", ns.statTimeout)
		}

		slog.WarnContext(ctx, "nodeunpublishvolume: unmount did not clear the mount; escalating",
			"targetPath", target, "attempted", rung.name, "escalatingTo", rungs[i+1].name,
			"error", err, "reason", "a blocked or unresponsive unmount is what leaves a pod "+
				"stuck Terminating and a stale mount on the node")
	}

	if firstErr == nil {
		firstErr = errors.New("umount reported success")
	}
	return fmt.Errorf("%s is still in the mount table after umount, umount %s and umount %s: %w",
		target, umountForce, umountLazy, firstErr)
}

// forcedRungReserve is the time kept back for each forced rung (`-f`, `-l`)
// when the caller's deadline bounds the whole ladder. The forced rungs are not
// meant to block, so a few seconds each is room enough.
const forcedRungReserve = 5 * time.Second

// plainUnmountBudget is how long the plain umount may run: ns.unmountTimeout,
// cut down when the caller's deadline would not otherwise leave room for the
// statfs probe and both forced rungs.
//
// The kubelet gives NodeUnpublishVolume a fixed two minutes. With
// unmountTimeout near that -- the chart once offered "2m" as an example --
// the deadline ended the call first, the wait returned the context's error
// rather than errMountTimeout, and the ladder, which opens only on the latter,
// never ran: a pod on a dead array never terminated. The reserve is
// at most half of what is left, so a short deadline still gets a plain rung.
func (ns *NodeServer) plainUnmountBudget(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return ns.unmountTimeout
	}
	left := time.Until(deadline)
	reserve := min(ns.statTimeout+2*forcedRungReserve, left/2)
	if budget := left - reserve; budget < ns.unmountTimeout {
		return max(budget, 0)
	}
	return ns.unmountTimeout
}

// shareOfRemaining bounds ctx to 1/n of the time left before its deadline, or
// leaves it unbounded when it has none.
func shareOfRemaining(ctx context.Context, n int) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, time.Until(deadline)/time.Duration(n))
}

// isInMountTable reports whether the node's mount table lists path.
//
// /proc/mounts on Linux, read by mount-utils; the FakeMounter's in-memory list
// under test. Either way this is a read of local state that never touches the
// NFS server, which is the whole reason it is used in place of a stat.
func (ns *NodeServer) isInMountTable(path string) (bool, error) {
	mounted, err := ns.existingMount(path)
	if err != nil {
		return false, err
	}
	return mounted != nil, nil
}

// isNotInMountTable is isInMountTable phrased the way the callers read.
func (ns *NodeServer) isNotInMountTable(path string) (bool, error) {
	mounted, err := ns.isInMountTable(path)
	return !mounted, err
}

// unmountWithTimeoutOf is unmountWithTimeout with the bound named explicitly,
// so the unmount path can use ns.unmountTimeout while the stale-mount recovery
// path in NodePublishVolume keeps using ns.mountTimeout.
func (ns *NodeServer) unmountWithTimeoutOf(ctx context.Context, target string, timeout time.Duration) error {
	return awaitCall(ctx, ns.joinUnmount(target), "unmounting "+target, timeout, errMountTimeout,
		"nodeserver: unmount timed out; it may still be blocked in the kernel")
}

// execUmount runs one escalated umount(8) invocation.
//
// k8s.io/mount-utils is not used for this. Its MounterForceUnmounter offers
// UnmountWithForce, which is plain-then-`-f` with its own internal timeout and
// no lazy rung at all -- so it would re-implement the first two rungs of the
// ladder above with different logging and still not reach the one that clears
// a mount the kernel is holding. exec.CommandContext is also what gives the
// forced rungs a deadline of their own: `umount -f` is supposed not to block,
// but "supposed not to" is what the plain unmount was too.
//
// Never given anything but a flag this file defines and the CO's target path,
// and never run through a shell.
func (ns *NodeServer) execUmount(ctx context.Context, flag, target string) error {
	ctx, cancel := context.WithTimeout(ctx, ns.unmountTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "umount", flag, target)
	// The whole process group is killed at the deadline, and the output pipe
	// is let go of soon after, so a helper umount forked cannot hold the rung
	// past its bound.
	killGroupOnCancel(cmd)
	cmd.WaitDelay = umountWaitDelay
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("umount %s %s: %w: %s", flag, target, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// umountWaitDelay is how long a forced rung waits for its output pipe to
// close after the process group is killed.
const umountWaitDelay = 2 * time.Second

// readSelfMountInfo is NodeServer.mountInfo's default.
func readSelfMountInfo() ([]mount.MountInfo, error) {
	return mount.ParseMountInfo("/proc/self/mountinfo")
}

// superblockSharers returns the other mount points on target's device, which
// is to say on its NFS superblock. A bind of the target, or a pod's subPath
// under it, counts: a forced unmount reaches it just the same.
//
// It reads only this container's mount namespace. The node plugin sees every
// pod's volume, because the kubelet's pods directory propagates into it, but
// not a mount someone made on the host outside it -- an administrator's own
// mount of the same array. That one is not protected.
//
// A target missing from mountinfo has no sharers to protect. A mountinfo that
// cannot be read is reported, and the caller then skips -f: the safe answer
// to "is anything else on this superblock" is to assume so.
func (ns *NodeServer) superblockSharers(target string) ([]string, error) {
	infos, err := ns.mountInfo()
	if err != nil {
		return nil, err
	}
	var self *mount.MountInfo
	for i := range infos {
		if infos[i].MountPoint == target {
			self = &infos[i]
		}
	}
	if self == nil {
		return nil, nil
	}
	var sharers []string
	for _, info := range infos {
		if info.MountPoint != target && info.Major == self.Major && info.Minor == self.Minor {
			sharers = append(sharers, info.MountPoint)
		}
	}
	return sharers, nil
}
