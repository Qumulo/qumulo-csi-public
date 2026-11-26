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
	"strings"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The read-only-replica probe: the node half of the read-only-replica rule.
//
// A Qumulo replication target is read-only until somebody promotes it, but
// "read-only" is enforced in two different places with two very different
// consequences, and this file exists because of the difference.
//
//   - **Frozen source, or a writable export over a replica.** Measured on
//     the lab arrays: the array rejects a *create* with EROFS, but an
//     append to a file that already exists returns success to the application
//     and is then silently dropped. Buffered NFS writes go nowhere and nothing
//     the workload can see says so. A pod that reaches a replica through a
//     writable export before the promote is in exactly this state.
//   - **Read-only export.** Measured on the lab arrays: with the replica's
//     pre-staged export marked read-only, *both* a create and an append fail
//     with `Read-only file system` and nothing lands.
//
// So the controller pre-stages the replica's export read-only and flips it at
// promote, and this is the node's half: before a pod is allowed to start
// writing, prove the mount is actually writable. One create-and-unlink of a
// dot-file in the mount root answers it, and answers it for the case that
// matters -- because a create is the operation that fails honestly in *both*
// of the states above, while an append fails honestly in only one.
//
// Every read-write publish is probed. The first cut ran the probe only when
// the PV carried csi.qumulo.com/replicated, and in the recommended topology
// -- one DNS name that floats, the SAME PersistentVolume used on both sides
// -- nothing ever set that marker, so the check was inert exactly where the
// runbook sends people. The probe needs no marker: the mount is
// either writable or it is not, and one create-and-unlink against a server
// that has just answered a mount is a stat-shaped cost, not a mount-shaped
// one. A publish that asked to be read-only is the one exemption, because a
// read-only mount proves nothing by being read-only.
//
// Deliberately narrow. Only EROFS fails the publish. The probe exists to catch
// one specific state -- an export the array serves read-only -- and a probe
// that could fail a publish for any other reason (root_squash, a full quota,
// a directory mode the pod's uid cannot write) would be a new way to break
// volumes that work today. Anything else it learns is logged and the publish
// proceeds.

// replicaProbePrefix names the probe file. A dot-file so it is invisible to a
// workload listing the volume root in the fraction of a second it exists, and
// a driver-specific prefix so a leftover from a killed node plugin is
// identifiable rather than mysterious.
const replicaProbePrefix = ".csi-qumulo-rw-probe-"

// replicaReadOnlyMessage is the message the runbook tells support to expect.
// Support greps for it, so it is a constant rather than a sentence assembled
// at the call site, and its leading sentence does not change.
//
// The rest is for a volume already promoted. A promote can lose its export to
// a fence from the source side's status poll, and an export can be
// set read-only by hand; either way the VolumeReplication reads Primary and
// "promote" is the wrong advice. The toggle an operator would reach for --
// secondary and back with autoResync -- runs a resync that reverts the copy
// to the recovery point, discarding every write made since the failover.
const replicaReadOnlyMessage = "replica is read-only; promote before publishing. If its VolumeReplication " +
	"already reads Primary, check the read-only flag of its export on the array and set it read-write there; " +
	"do not toggle the VolumeReplication through secondary with autoResync, which resyncs it and discards " +
	"every write made since the failover"

// verifyReplicaIsWritable probes a freshly mounted read-write volume and
// refuses the publish if the array is serving it read-only.
//
// readOnlyMount is the assembled mount's own read-only state -- the union of
// the request's readonly flag and an "ro" in the class's mountOptions, the
// same predicate confirmExistingMount compares on. A mount that was *asked* to
// be read-only proves nothing by being read-only, and failing it would refuse
// every legitimate read-only publish of a replica, which is the normal way to
// run a reporting workload against one. The request's flag is the pod's or the
// PV's readOnly: true, not the claim's access mode: a ReadOnlyMany claim
// without it is mounted read-write, probed, and refused on a replica.
//
// volumeContext is read for the log only. csi.qumulo.com/replicated, when a
// tool such as qumulo-csi-repoint mirror set it, names the volume as a known
// replica in the message; its absence changes nothing.
func (ns *NodeServer) verifyReplicaIsWritable(
	ctx context.Context,
	volumeID, targetPath string,
	volumeContext map[string]string,
	readOnlyMount bool,
	unmountOnRefusal bool,
) error {
	if readOnlyMount {
		return nil
	}
	marked := strings.EqualFold(strings.TrimSpace(volumeContext[volumeContextReplicated]), "true")

	err := ns.probeWithTimeout(ctx, targetPath)
	switch {
	case err == nil:
		slog.DebugContext(ctx, "nodepublishvolume: write probe found the volume writable",
			"volumeID", volumeID, "targetPath", targetPath, "markedReplicated", marked)
		ns.unconfirmed.remove(targetPath)
		return nil

	case errors.Is(err, syscall.EROFS) && !unmountOnRefusal:
		// A mount this plugin did not just make and did not fail to confirm:
		// one a pod may be running on, that turned read-only under it -- a
		// source frozen mid-failover. Refused, because a read-write publish
		// of it would be a lie, but not unmounted: that took the volume away
		// from the running pod.
		slog.WarnContext(ctx, "nodepublishvolume: refusing a read-write republish of a mount the array now serves "+
			"read-only; leaving it mounted, as a running pod may be using it",
			"volumeID", volumeID, "targetPath", targetPath, "markedReplicated", marked)
		return status.Errorf(codes.FailedPrecondition,
			"%s (%s is mounted read-write but the array refused a create with EROFS; the mount was not "+
				"unmounted, because a running pod may be using it)", replicaReadOnlyMessage, targetPath)

	case errors.Is(err, errStatTimeout):
		// The probe did not finish: the array is hung or very slow. It used
		// to answer OK, which skipped the replica check, and each republish
		// of the hung mount started another probe beside the one still
		// blocked in the kernel. Unavailable: the kubelet retries,
		// the retry joins the probe in flight, and the mount stays.
		return status.Errorf(codes.Unavailable,
			"the write probe of %s did not finish within %s (%v); the array may be hung. The mount is "+
				"left in place and the retry checks it again", targetPath, ns.statTimeout, err)

	case isStaleHandle(err):
		// The server no longer honours the mount's handle. The republish
		// that retries this stats the target first and remounts it,
		// but a pod already running keeps its own bind of the dead mount.
		return status.Errorf(codes.Unavailable,
			"the mount at %s is stale (%v); retrying remounts the target, but a pod "+
				"already running keeps its own bind of the dead mount and will not "+
				"self-heal -- delete the pod so it is rescheduled onto a fresh "+
				"mount. %s",
			targetPath, err, staleMountRemedy)

	case errors.Is(err, syscall.EROFS):
		// The one case that fails the publish. Unmount first: leaving a
		// read-only mount at the target would let a later republish find it
		// already mounted and report success, which is the outcome this whole
		// probe exists to prevent.
		slog.WarnContext(ctx, "nodepublishvolume: refusing a read-write publish of a volume the array serves read-only",
			"volumeID", volumeID, "targetPath", targetPath, "markedReplicated", marked,
			"remedy", "if this is a replica, promote it on the DR array (VolumeReplication -> Primary) before "+
				"starting the workload; if its VolumeReplication already reads Primary, set its export read-write "+
				"on the array; if the export is read-only on purpose, publish the volume read-only")

		// A mount that will not come away stays refused all the same: the
		// retry finds it through confirmExistingMount, which probes it again.
		// The message says so, because the kubelet will not clean it
		// up for a publish that never succeeded.
		leftover := ""
		if uerr := ns.unmountWithTimeout(ctx, targetPath); uerr != nil {
			ns.unconfirmed.add(targetPath)
			slog.ErrorContext(ctx, "nodepublishvolume: could not unmount the read-only replica",
				"volumeID", volumeID, "targetPath", targetPath, "error", uerr)
			leftover = fmt.Sprintf("; the read-only mount could not be removed (%v) and every retry will be refused "+
				"until it is unmounted or the volume is promoted", uerr)
		}

		// FailedPrecondition, not Internal: nothing is broken, a step is
		// missing. The CO retries, and the retry succeeds the moment the
		// promote lands, with no object change in between.
		return status.Errorf(codes.FailedPrecondition,
			"%s (%s is mounted read-write but the array refused a create with EROFS; if the export is "+
				"read-only on purpose, publish the volume read-only%s)",
			replicaReadOnlyMessage, targetPath, leftover)

	default:
		// Everything else. Logged, never fatal -- see the file comment.
		slog.WarnContext(ctx, "nodepublishvolume: write probe was inconclusive; publishing anyway",
			"volumeID", volumeID, "targetPath", targetPath, "error", err,
			"reason", "only EROFS means the array serves this volume read-only; any other failure is more "+
				"likely about this pod's identity or quota than about replication")
		return nil
	}
}

// probeWithTimeout runs the write probe and stops waiting after
// ns.statTimeout.
//
// Bounded for the same reason statWithTimeout is, and on the same budget: this
// is a create and an unlink against an NFS server that has just answered a
// mount, so it is a stat-shaped call rather than a mount-shaped one, and a
// server that goes away between the mount and the probe must not hold the RPC
// open. The same trade-off applies: on a timeout the goroutine outlives this
// call and ends whenever the kernel gives up.
//
// Joined like a stat: a republish of a hung mount waits on the probe
// already blocked in the kernel instead of starting another, so a burst of
// republishes after a kubelet restart costs one thread per mount, not one
// per attempt.
func (ns *NodeServer) probeWithTimeout(ctx context.Context, mountRoot string) error {
	call := ns.statCalls.join("probe\x00"+mountRoot, "probing "+mountRoot, func() (volumeStats, error) {
		return volumeStats{}, ns.writeProbe(mountRoot)
	})
	_, err := ns.awaitStatCall(ctx, call, "probing "+mountRoot+" for write access")
	return err
}

// probeWritableRoot creates and immediately unlinks a file in a mount root.
//
// Create-and-unlink rather than a write to an existing file, because a create
// is the operation that fails honestly in both of the read-only states this
// probe has to tell apart (see the file comment): an append to an existing
// file on a frozen source is acknowledged and dropped.
//
// os.CreateTemp gives the unique name for free, which matters: two pods on
// this node publishing the same volume at the same moment would collide on a
// fixed name, and one of them would read the other's EEXIST as a failed probe.
func probeWritableRoot(mountRoot string) error {
	file, err := os.CreateTemp(mountRoot, replicaProbePrefix+"*")
	if err != nil {
		// The EROFS the caller switches on arrives here, inside a
		// *os.PathError, which errors.Is unwraps.
		return err
	}

	name := file.Name()
	closeErr := file.Close()

	// Unlinked whatever happened above: a probe file left in a customer's
	// volume is litter, and litter with this driver's name on it.
	if err := os.Remove(name); err != nil {
		return err
	}
	return closeErr
}
