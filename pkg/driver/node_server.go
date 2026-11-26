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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// NodeServer implements csi.NodeServer, mounting/unmounting Qumulo-backed
// NFS exports onto the node. RPCs this driver does not support (staging and
// node-side expansion, see the nodeCapabilities comment below) are declared
// explicitly, returning codes.Unimplemented rather than a fabricated
// success; csi.UnimplementedNodeServer remains embedded only as a forward
// compatibility fallback for RPCs added to the interface in the future.
type NodeServer struct {
	csi.UnimplementedNodeServer
	driver  *Driver
	mounter mount.Interface

	// mountTimeout bounds how long a single mount or unmount may block before
	// the RPC gives up on it. A field rather than a constant so tests can
	// exercise the timeout without waiting for it, and so an operator with a
	// distant cluster can raise it.
	mountTimeout time.Duration

	// unmountTimeout bounds the plain umount(8) in NodeUnpublishVolume before
	// the forced escalation starts. A separate field from mountTimeout, and
	// node_unmount.go says why: a slow mount usually succeeds, a slow unmount
	// is a server that is not answering.
	unmountTimeout time.Duration

	// forceUnmount runs one escalated umount(8) (`-f`, then `-l`). A field for
	// the same reason mounter is an interface: the escalation ladder has to be
	// testable without a dead NFS server to produce a blocked unmount, and
	// without execing umount(8) on a developer's workstation.
	forceUnmount func(ctx context.Context, flag, target string) error

	// statTimeout bounds how long a single stat(2) or statfs(2) call against a
	// volume's mount may block. A separate field from mountTimeout, not the
	// same one reused: a stat has no negotiation the way mount(8) does, and
	// kubelet polls NodeGetVolumeStats far more often than it (un)publishes a
	// volume, so a mount-length timeout here would multiply one wedged
	// volume's cost across every poll.
	statTimeout time.Duration

	// writeProbe creates and unlinks a file in a published mount's root, to
	// find out whether the array is serving it read-only. A field for the same
	// reason statFunc is: EROFS needs a read-only NFS export, which no unit
	// test has. See node_replica.go.
	writeProbe func(mountRoot string) error

	// mountInfo reads this mount namespace's mountinfo, which -- unlike the
	// mount table mounter.List reads -- carries each mount's device number,
	// and so says which mounts share an NFS superblock. A field so the
	// unmount ladder's shared-superblock check is testable.
	mountInfo func() ([]mount.MountInfo, error)

	// forceGate keeps a forced unmount from running while a mount is being
	// made on this node. A mount holds it shared for as long as mount(8)
	// runs; `umount -f` needs it exclusively and is skipped, in favour of
	// `umount -l`, when it cannot have it at once. Without it a mount that
	// joins a superblock between the sharing check and the -f would be hit
	// by the -f.
	forceGate sync.RWMutex

	// statFunc and statfsFunc are the syscalls NodeGetVolumeStats bounds.
	// Fields rather than direct calls, for the same reason mounter is an
	// interface rather than a direct mount-utils call: a test needs to
	// substitute a fake that blocks, to exercise the timeout deterministically.
	statFunc   func(string) (os.FileInfo, error)
	statfsFunc func(string) (volumeStats, error)

	// inspectTarget is the check NodePublishVolume makes of what is at the
	// target before it mounts. A field so a test can have it answer
	// ESTALE, which only a real stale NFS mount produces.
	inspectTarget func(string) (publishTargetState, error)

	// statCalls holds the stat and statfs calls still in flight, one per
	// path, so a poll against a path whose last call has not returned joins
	// that call rather than starting another. See statFlights.
	statCalls statFlights

	// mountCalls holds the mount(8) and plain umount(8) calls still in
	// flight, one per target, so a kubelet retry waits on the call already
	// blocked instead of starting another beside it.
	mountCalls statFlights

	// unconfirmed holds the targets a publish mounted without confirming:
	// the mount outlived mountTimeout, or an EROFS refusal's unmount failed.
	// Only those may be unmounted by a later republish that finds them read-
	// only. Any other existing mount may have a running pod on it.
	unconfirmed unconfirmedTargets
}

// unconfirmedTargets is a set of target paths, safe for concurrent RPCs. In
// memory only: after a plugin restart it is empty, so a leftover read-only
// mount is refused but left in place, which is the safe side.
type unconfirmedTargets struct {
	mu      sync.Mutex
	targets map[string]bool
}

func (u *unconfirmedTargets) add(target string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.targets == nil {
		u.targets = make(map[string]bool)
	}
	u.targets[target] = true
}

func (u *unconfirmedTargets) remove(target string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.targets, target)
}

func (u *unconfirmedTargets) has(target string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.targets[target]
}

// defaultMountTimeout bounds one mount(8) or umount(8) call.
//
// This bounds the *RPC*, not the mount's own retry behaviour. A hard NFS mount
// is supposed to block indefinitely -- that is what `hard` means, and the e2e
// manifests set it deliberately. What this changes is that the CO gets an error
// it can retry rather than a gRPC call that never returns, and that a wedged
// mount stops holding a slot in the node plugin forever.
const defaultMountTimeout = 90 * time.Second

// defaultStatTimeout bounds one stat(2) or statfs(2) call made against a
// volume's mount by NodeGetVolumeStats.
//
// A hard-hung NFS mount blocks these syscalls exactly as it blocks mount(8),
// and this RPC is on kubelet's periodic stats-collection path rather than a
// one-shot lifecycle call, so it is bounded on its own schedule rather than
// borrowing defaultMountTimeout.
const defaultStatTimeout = 10 * time.Second

// errMountTimeout is returned when a mount or unmount outlives mountTimeout.
var errMountTimeout = errors.New("timed out")

// errStatTimeout is returned when a stat or statfs call outlives statTimeout.
var errStatTimeout = errors.New("timed out")

// NewNodeServer creates a NodeServer backed by the given Driver.
//
// newRealMounter (node_mounter_linux.go / node_mounter_other.go) is the only
// Linux-specific call in this file: mount.NewWithoutSystemd is defined by
// mount-utils for linux only, and the node image is Linux-only anyway (see
// ARCHITECTURE.md), so the split exists purely so `go test ./...` still builds
// elsewhere -- calling it on a non-Linux host is a build-time impossibility
// for the shipped binary, not a supported path.
func NewNodeServer(driver *Driver) *NodeServer {
	return newNodeServer(driver, newRealMounter())
}

// newNodeServer is the one place a NodeServer's defaults are set, so the real
// and the fake-mounter constructors cannot drift in what they wire up.
func newNodeServer(driver *Driver, mounter mount.Interface) *NodeServer {
	ns := &NodeServer{
		driver:         driver,
		mounter:        mounter,
		mountTimeout:   defaultMountTimeout,
		unmountTimeout: defaultUnmountTimeout,
		statTimeout:    defaultStatTimeout,
		statFunc:       os.Stat,
		statfsFunc:     volumeStatsFor,
		inspectTarget:  inspectPublishTarget,
		writeProbe:     probeWritableRoot,
		mountInfo:      readSelfMountInfo,
	}
	if driver != nil && driver.UnmountTimeout > 0 {
		ns.unmountTimeout = driver.UnmountTimeout
	}
	// A method value rather than a package function: the escalation's own
	// deadline comes from ns.unmountTimeout, which an operator can raise.
	ns.forceUnmount = ns.execUmount

	return ns
}

// NewNodeServerWithMounter creates a NodeServer using the given mounter in
// place of the real one NewNodeServer builds.
//
// This exists solely for the csi-sanity conformance suite (test/sanity): that
// suite runs the real driver binary, but there is no real NFS server for
// NodePublishVolume to mount from, so the binary is started with
// mount.FakeMounter standing in for the real mount(8) call. Every other RPC
// path, and every gRPC/HTTP boundary, is exercised for real; only the final
// syscall is not. -fake-mounter is the only thing in this codebase that
// constructs a NodeServer this way -- a real deployment never sets it.
func NewNodeServerWithMounter(driver *Driver, mounter mount.Interface) *NodeServer {
	return newNodeServer(driver, mounter)
}

// NodeGetInfo returns the ID of the node this driver instance runs on, and the
// topology segment it belongs to.
//
// The segment is the same on every node -- see topology.go for why this driver
// has a topology at all, and why it is degenerate. Kubernetes records the key
// in CSINode.spec.drivers[].topologyKeys at registration, so a change here
// takes effect when the node plugin restarts.
//
// The kubelet calls this only while registering the plugin, so it is also
// where the node pod learns it is registered: it writes the readiness marker
// (registration.go). A marker that cannot be written leaves the pod
// NotReady, which stops a rollout; it never fails the registration itself.
func (ns *NodeServer) NodeGetInfo(
	ctx context.Context,
	req *csi.NodeGetInfoRequest,
) (*csi.NodeGetInfoResponse, error) {
	if err := recordRegistration(ns.driver.RegistrationMarker); err != nil {
		slog.WarnContext(ctx, "nodegetinfo: could not record the registration; the pod stays NotReady",
			"marker", ns.driver.RegistrationMarker, "error", err)
	}
	return &csi.NodeGetInfoResponse{
		NodeId:             ns.driver.NodeID,
		AccessibleTopology: nodeTopology(),
	}, nil
}

// nodeCapabilities is the set of optional node RPCs this driver implements.
//
// STAGE_UNSTAGE_VOLUME is deliberately absent. Staging exists to mount a
// volume once per node and bind-mount it into each pod, which matters for
// block devices that can only be attached once; an NFS export can be
// mounted independently by every pod that wants it, so staging would add a
// mount to manage and buy nothing.
//
// EXPAND_VOLUME is absent for the same reason ControllerExpandVolume reports
// NodeExpansionRequired false: growing the quota is the whole operation and
// there is no node-side filesystem to resize afterwards.
var nodeCapabilities = []csi.NodeServiceCapability_RPC_Type{
	csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
	// GET_VOLUME_HEALTH is what CSI v1.13 calls the capability older versions
	// spelled VOLUME_CONDITION. Advertised unconditionally, like the rest of
	// this set: it needs no configuration, so there is no deployment in which
	// it is advertised and does not work.
	csi.NodeServiceCapability_RPC_GET_VOLUME_HEALTH,
}

// NodeGetCapabilities reports which optional node RPCs the CO may call.
func (ns *NodeServer) NodeGetCapabilities(
	ctx context.Context,
	req *csi.NodeGetCapabilitiesRequest,
) (*csi.NodeGetCapabilitiesResponse, error) {
	caps := make([]*csi.NodeServiceCapability, 0, len(nodeCapabilities))
	for _, rpc := range nodeCapabilities {
		caps = append(caps, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{Type: rpc},
			},
		})
	}

	return &csi.NodeGetCapabilitiesResponse{Capabilities: caps}, nil
}

// volumeStats is one mounted volume's usage, in bytes and inodes. It is
// filled in per platform by volumeStatsFor.
type volumeStats struct {
	TotalBytes     int64
	UsedBytes      int64
	AvailableBytes int64

	TotalInodes int64
	UsedInodes  int64
	FreeInodes  int64
}

// NodeGetVolumeStats reports a mounted volume's usage, which is what lets
// the CO surface how full a PVC is -- kubelet exposes these as the
// kubelet_volume_stats_* metrics, so without them a volume filling up is
// invisible until a write fails.
func (ns *NodeServer) NodeGetVolumeStats(
	ctx context.Context,
	req *csi.NodeGetVolumeStatsRequest,
) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if err := validateNodeVolumeID(req.GetVolumeId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	volumePath := req.GetVolumePath()
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume path missing in request")
	}
	// Only the control characters here, not the absolute-and-clean rule the
	// publish paths apply. This RPC measures a path and creates and removes
	// nothing, and the CSI conformance suite sends a relative path for it
	// and requires NotFound -- which is the honest answer, since no such
	// path exists. A NUL or a newline is refused all the same: it would
	// forge the log line that records what was measured.
	if hasControlCharacters(volumePath) {
		return nil, status.Errorf(codes.InvalidArgument,
			"volume path %q contains a NUL, newline or carriage return", volumePath)
	}

	// Bounded the same way NodePublishVolume's mount(8) call is: a hard-hung
	// NFS mount blocks stat(2) exactly as it blocks mount(8), and kubelet
	// polls this RPC on its own schedule, so a wedged volume must not hold a
	// goroutine per poll forever.
	//
	// The spec requires NotFound specifically when the path is not there,
	// so the CO can tell "not mounted" apart from "could not measure".
	if err := ns.statWithTimeout(ctx, volumePath); err != nil {
		if errors.Is(err, errStatTimeout) {
			return nil, status.Errorf(codes.DeadlineExceeded,
				"checking volume path %s did not complete within %s", volumePath, ns.statTimeout)
		}
		return nil, volumePathStatError(volumePath, err)
	}

	// A target that is no longer a mount is a plain directory on the node's
	// own disk, and statfs on it measures that disk: the PVC was reported as
	// the node's root filesystem, 40 GiB of it and nearly full, while its
	// writes were going nowhere. NotFound is the spec's answer -- the
	// volume is not at this path -- and leaves kubelet with no stats rather
	// than wrong ones. Asked after the bounded stat above, which is what
	// would have caught a hung mount; the NotMounted health check makes the
	// same call in the same order.
	if notMountPoint, err := ns.mounter.IsLikelyNotMountPoint(volumePath); err == nil && notMountPoint {
		return nil, status.Errorf(codes.NotFound,
			"volume path %s exists but nothing is mounted on it; its statistics would be the "+
				"node's own disk, not the volume's", volumePath)
	}

	stats, err := ns.statfsWithTimeout(ctx, volumePath)
	if err != nil {
		if errors.Is(err, errStatTimeout) {
			return nil, status.Errorf(codes.DeadlineExceeded,
				"reading stats for volume path %s did not complete within %s", volumePath, ns.statTimeout)
		}
		return nil, status.Errorf(codes.Internal, "reading stats for volume path %s: %v", volumePath, err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Unit:      csi.VolumeUsage_BYTES,
				Total:     stats.TotalBytes,
				Used:      stats.UsedBytes,
				Available: stats.AvailableBytes,
			},
			{
				Unit:      csi.VolumeUsage_INODES,
				Total:     stats.TotalInodes,
				Used:      stats.UsedInodes,
				Available: stats.FreeInodes,
			},
		},
	}, nil
}

// NodePublishVolume mounts a volume's NFS export at the target path.
//
// Every request is validated before the filesystem is touched: a malformed
// request that has already had a directory created for it is harder to reason
// about than one rejected outright.
func (ns *NodeServer) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
) (*csi.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if err := validateNodeVolumeID(volumeID); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}
	// Before anything is created at it.
	if err := validateNodeTargetPath(targetPath); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	capability := req.GetVolumeCapability()
	if capability == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	// The same rules the controller applies at provision time. Repeating them
	// here is not redundant: a node instance runs in a different process from
	// the controller that vetted the CreateVolume request, so without this a
	// block-access request would be answered with an NFS mount.
	if err := validateVolumeCapabilities([]*csi.VolumeCapability{capability}); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	source, err := mountSourceFor(req.GetVolumeContext())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Cloned before appending: GetMountFlags returns the request's own slice,
	// and appending to it could write into the caller's backing array.
	mountOptions := slices.Clone(capability.GetMount().GetMountFlags())

	versionOption, err := nfsVersionOptionFor(req.GetVolumeContext(), mountOptions)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if versionOption != "" {
		mountOptions = append(mountOptions, versionOption)
	}

	// After the version option, because it is a consequence of the version:
	// NFSv3 needs nolock and NFSv4.1 must not be given it.
	lockOption, err := nfsLockOptionFor(req.GetVolumeContext(), mountOptions)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if lockOption != "" {
		slog.InfoContext(ctx, "nodepublish: adding nolock for an NFSv3 mount",
			"volumeID", req.GetVolumeId(), "target", targetPath,
			"reason", "the node image runs no rpc.statd, so an NFSv3 mount without nolock fails")
		mountOptions = append(mountOptions, lockOption)
	}

	// The node half of securityFlavor. The export already requires this
	// flavour, so the option is what lets the mount succeed -- not what makes
	// it secure.
	securityOption, err := mountSecurityOptionFor(req.GetVolumeContext())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// A sec= in the class's own mountOptions that asks for a different
	// flavour from the volume's is refused rather than resolved. Two sec=
	// options leave the kernel taking whichever came last, and one alone
	// mounts with a flavour the export does not require -- in either case
	// the mount's security is not what either half of the class says.
	// A volume with no securityFlavor has nothing to contradict, so its
	// class's sec= is honoured and logged.
	if err := refuseContradictorySecurityOption(req.GetVolumeContext(), mountOptions); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if strings.TrimSpace(req.GetVolumeContext()[volumeContextSecurityFlavor]) == "" {
		if classOption := securityMountOption(mountOptions); classOption != "" {
			slog.WarnContext(ctx, "nodepublishvolume: the class's mountOptions choose the security flavour; "+
				"the driver neither checks it against the export nor adds one of its own",
				"volumeID", volumeID, "targetPath", targetPath, "option", classOption,
				"remedy", "set securityFlavor on the StorageClass so new volumes' exports require that flavour")
		}
	}
	if securityOption != "" {
		if err := ns.requireKeytab(); err != nil {
			return nil, err
		}
		mountOptions = append(mountOptions, securityOption)
	}
	if req.GetReadonly() {
		mountOptions = append(mountOptions, "ro")
	}

	// What is actually at the target, with the final element not followed.
	// IsLikelyNotMountPoint below stats through a symlink, so a linked
	// target read as an ordinary directory and the mount landed wherever it
	// pointed.
	//
	// A target that cannot be inspected because the mount on it is broken
	// is not refused here: that is the stale mount the checks below exist to
	// recover, and answering Internal for it left the volume unpublishable
	// on this node on every retry. Those checks decide what the error
	// means -- they unmount only for isRepublishDeadMount, and answer
	// Internal for the rest, as before. A mount is a directory, never a
	// symlink, so nothing the refusals below guard against is let through.
	switch state, err := ns.inspectTarget(targetPath); {
	case err != nil && isCorruptedMount(err):
		slog.WarnContext(ctx, "nodepublishvolume: target could not be inspected; checking it for a stale mount",
			"volumeID", volumeID, "targetPath", targetPath, "error", err)
	case err != nil:
		return nil, status.Errorf(codes.Internal, "inspecting target %s: %v", targetPath, err)
	case state == targetIsSymlink:
		return nil, status.Errorf(codes.FailedPrecondition,
			"target %s is a symbolic link; the kubelet creates a directory, so this one was made by "+
				"something else, and mounting through it would put the volume wherever it points",
			targetPath)
	case state == targetNotADirectory:
		return nil, status.Errorf(codes.FailedPrecondition,
			"target %s exists and is not a directory, so nothing can be mounted on it", targetPath)
	}

	notMnt, err := ns.mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		switch {
		case os.IsNotExist(err):
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true

		case isRepublishDeadMount(err):
			// The target holds a mount whose handle the server no longer
			// honours -- ESTALE or ENOTCONN after a Qumulo reboot, a failover,
			// or an export removed and recreated. Every syscall against it
			// fails, including the check just made, so without this the RPC
			// returned Internal on every retry and the volume stayed
			// unpublishable on this node until someone unmounted it by hand.
			//
			// Recovery is to force the dead mount away and fall through to a
			// fresh one. Gated on isRepublishDeadMount -- the same test the
			// stat path below uses -- and never on a timeout: a slow
			// mount and a dead one must not be conflated, since
			// force-unmounting a mount that is merely slow would throw away a
			// working volume's writes.
			slog.WarnContext(ctx, "nodepublishvolume: target holds a stale mount; unmounting it and remounting, but a running pod keeps its own bind of the dead mount "+
				"and will not self-heal -- delete the pod to get it onto the fresh mount",
				"volumeID", volumeID, "targetPath", targetPath, "error", err, "remedy", staleMountRemedy)

			if uerr := ns.unmountWithTimeout(ctx, targetPath); uerr != nil {
				return nil, status.Errorf(codes.Internal,
					"target %s holds a stale mount that could not be unmounted: %v", targetPath, uerr)
			}
			notMnt = true

		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if !notMnt {
		// IsLikelyNotMountPoint reads cached attributes (statx with
		// AT_STATX_DONT_SYNC in mount-utils), which a mount whose handle the
		// server no longer honours still answers from, so the recovery above
		// never ran for it: a republish after a kubelet restart confirmed the
		// dead mount and answered OK, and the pod kept getting errno 521 on
		// every read and write. A real stat reaches the server.
		if serr := ns.statWithTimeout(ctx, targetPath); serr != nil && isRepublishDeadMount(serr) {
			slog.WarnContext(ctx, "nodepublishvolume: target holds a stale mount; unmounting it and remounting, but a running pod keeps its own bind of the dead mount "+
				"and will not self-heal -- delete the pod to get it onto the fresh mount",
				"volumeID", volumeID, "targetPath", targetPath, "error", serr, "remedy", staleMountRemedy)
			if uerr := ns.unmountWithTimeout(ctx, targetPath); uerr != nil {
				return nil, status.Errorf(codes.Internal,
					"target %s holds a stale mount that could not be unmounted: %v", targetPath, uerr)
			}
			notMnt = true
		}
	}
	if !notMnt {
		// isReadOnly(mountOptions), not req.GetReadonly(). By this point
		// mountOptions is fully assembled and already carries "ro" if the
		// request asked for it, so this is the union of both ways a mount
		// becomes read-only -- and it is deliberately the *same* predicate
		// applied to the kernel's options on the other side of the
		// comparison, so the two sides are like for like.
		//
		// req.GetReadonly() is the pod's or the PV's readOnly flag, not the
		// access mode: the kubelet sets it from readOnly: true alone, and a
		// ReadOnlyMany claim without it is published read-write, as with
		// other NFS drivers. A StorageClass with mountOptions: [ro]
		// -- the normal way to express a read-only volume in a chart --
		// therefore mounted "ro" on the first publish and then compared the
		// kernel's "ro" against a requested false on every republish:
		// AlreadyExists, permanently, so no pod restart on that node ever
		// recovered. Comparing the assembled options on both sides fixes it
		// whichever way the mount became read-only.
		//
		// req.GetReadonly() goes in as well, for the read-only check alone:
		// only the request's flag makes the runtime bind the volume
		// read-only into the pod (node_readonly.go).
		return ns.confirmExistingMount(ctx, volumeID, targetPath, source, isReadOnly(mountOptions),
			req.GetReadonly(), req.GetVolumeContext())
	}

	slog.DebugContext(ctx, "nodepublishvolume: mounting",
		"volumeID", volumeID,
		"source", source,
		"targetPath", targetPath,
		"mountFlags", mountOptions,
		// Present only for a Kerberos volume. Never the credential -- the
		// keytab is the host's and the driver never sees it.
		"securityFlavor", req.GetVolumeContext()[volumeContextSecurityFlavor],
		// Present only for volumes provisioned by a class that set adDomain.
		// The domain is already inside source, since it is a path component,
		// but logging it named makes a mount traceable to its domain without
		// parsing the path.
		"adDomain", req.GetVolumeContext()[volumeContextADDomain],
	)
	err = ns.mountWithTimeout(ctx, source, targetPath, "nfs", mountOptions)
	if err != nil {
		if errors.Is(err, errMountTimeout) {
			// DeadlineExceeded rather than Internal: this says "not yet",
			// which is what the CO should hear about a mount that may still
			// be retrying inside the kernel. The mount may finish behind this
			// answer, unprobed, so the retry that finds it may unmount it.
			ns.unconfirmed.add(targetPath)
			return nil, status.Errorf(codes.DeadlineExceeded,
				"mounting %s at %s did not complete within %s", source, targetPath, ns.mountTimeout)
		}
		if os.IsPermission(err) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if strings.Contains(err.Error(), "invalid argument") {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	// The mount is up. For a read-only publish, check that the kernel made it
	// read-only rather than trusting mount(8) with it; see
	// node_readonly.go.
	if isReadOnly(mountOptions) {
		if err := ns.requireReadOnlyMount(ctx, volumeID, targetPath, true, req.GetReadonly()); err != nil {
			return nil, err
		}
	}

	// For a read-write publish that is not yet enough: a
	// replica reached before its promote is a directory that looks writable
	// and is not, and an append to it through a writable export is
	// acknowledged and silently dropped. One
	// create-and-unlink settles it, for every read-write publish -- no PV
	// marker is needed, and in the recommended one-PV topology none is ever
	// set -- and only EROFS refuses the publish. See node_replica.go.
	if err := ns.verifyReplicaIsWritable(ctx, volumeID, targetPath,
		req.GetVolumeContext(), isReadOnly(mountOptions), true); err != nil {
		return nil, err
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

// nfsLockOptionKeys are the mount options that decide NFS locking.
//
// "nolock" and "lock" are the switch itself; "local_lock=" sets which lock
// types are handled locally and is what nolock is shorthand for, so a class
// that has spelled it out longhand must not have nolock appended on top.
var nfsLockOptionKeys = []string{"nolock", "lock", "local_lock="}

// nfsBareVersionOptions are mount(8)'s shorthand version options: nfs(5)
// documents v3, v4, v4.1 and v4.2 as alternatives to nfsvers=, and mount.nfs
// honours them.
//
// They were invisible to this driver, which looked only for "vers=" and
// "nfsvers=". Two things followed, both silent: mountOptions [v3] got
// no nolock, so the mount failed for want of rpc.statd -- or worse, mounted
// with locking quietly broken -- and an nfs3 class whose mountOptions said
// [v4.1] was not reported as the contradiction it is, so the driver appended
// nfsvers=3 beside it and the kernel took whichever came last.
var nfsBareVersionOptions = []string{"v3", "v4", "v4.0", "v4.1", "v4.2"}

// nfsVersionOf reports the NFS version a single mount option pins, as the
// string after the "v" ("3", "4.1"), or "" when the option pins none.
func nfsVersionOf(option string) string {
	trimmed := strings.ToLower(strings.TrimSpace(option))
	if key, value, ok := strings.Cut(trimmed, "="); ok {
		if key == "vers" || key == "nfsvers" {
			return value
		}
		return ""
	}
	if slices.Contains(nfsBareVersionOptions, trimmed) {
		return strings.TrimPrefix(trimmed, "v")
	}
	return ""
}

// pinsNFSv3 reports whether mountFlags pin NFS version 3: vers=3, nfsvers=3,
// a 3.x minor, or the bare v3.
func pinsNFSv3(mountFlags []string) bool {
	for _, option := range mountOptionList(mountFlags) {
		version := nfsVersionOf(option)
		if version == "3" || strings.HasPrefix(version, "3.") {
			return true
		}
	}
	return false
}

// nfsLockOptionFor returns "nolock" when this volume needs it added, or "".
//
// The node image ships mount.nfs but runs no rpc.statd, so an NFSv3 mount
// without nolock fails outright -- and when it does not fail it is worse,
// because locking is silently broken instead. That made an nfs3 StorageClass
// that forgot the option a guaranteed-broken volume, and nothing in the
// driver or the chart added it: the chart's rendered default is
// [hard, timeo=600], which does not include it, and an explicit mountOptions
// list is rendered verbatim.
//
// Injected here rather than refused at CreateVolume, for one reason: a volume
// provisioned before this existed carries its class's mountOptions frozen in
// the PV, so a controller-side rejection could never help it while this fixes
// it on the next publish. It is also why the chart's default being
// protocol-aware is not sufficient on its own -- a class that sets
// mountOptions explicitly, which is the documented way to ask for anything at
// all, bypasses the default entirely.
//
// Deliberately silent for NFSv4.1: v4 locks in-protocol, and passing nolock
// there is accepted and ignored, so adding it would put a misleading option in
// every v4.1 mount table for no effect. Equally silent for an absent
// protocol with no version pinned in mountOptions, matching
// nfsVersionOptionFor: those volumes negotiate their version with the
// kernel, so the driver does not know whether they will land on v3 and must
// not guess. One whose mountOptions pin v3 is treated as nfs3.
func nfsLockOptionFor(volumeContext map[string]string, mountFlags []string) (string, error) {
	protocol := strings.TrimSpace(volumeContext[volumeContextProtocol])
	// A class with no protocol that pins NFSv3 in mountOptions is NFSv3 as
	// surely as protocol: nfs3 is, and mounted without nolock it failed for
	// want of rpc.statd. Only an unpinned version is left to negotiate.
	if protocol == "" && pinsNFSv3(mountFlags) {
		protocol = protocolNFS3
	}
	if protocol != protocolNFS3 {
		return "", nil
	}

	switch existing := nfsLockFlag(mountFlags); {
	case existing == "":
		return "nolock", nil

	case strings.EqualFold(strings.TrimSpace(existing), "lock"):
		// An explicit request for server-side locking, which this node cannot
		// serve. Refused rather than overridden: silently rewriting "lock"
		// into "nolock" would give the operator the opposite of what they
		// asked for, and locking is exactly the thing they were being
		// deliberate about. nfsVersionOptionFor refuses a contradiction the
		// same way.
		return "", fmt.Errorf("volume is %s=%s and its mount options request %q, "+
			"but the node image runs no rpc.statd, so server-side NFSv3 locking cannot work here; "+
			"remove %q to mount with nolock, or use %s=%s where locking is in-protocol",
			paramProtocol, protocolNFS3, existing, existing, paramProtocol, protocolNFS41)

	default:
		// nolock, or a longhand local_lock=. Already answered.
		return "", nil
	}
}

// nfsLockFlag returns the first mount flag that decides locking, or "".
func nfsLockFlag(mountFlags []string) string {
	for _, flag := range mountOptionList(mountFlags) {
		trimmed := strings.ToLower(strings.TrimSpace(flag))
		for _, key := range nfsLockOptionKeys {
			// "lock" is a bare word and a prefix of "local_lock=", so an exact
			// match is required for the bare forms while local_lock= keeps
			// prefix matching for its value.
			if strings.HasSuffix(key, "=") {
				if strings.HasPrefix(trimmed, key) {
					return flag
				}
				continue
			}
			if trimmed == key {
				return flag
			}
		}
	}
	return ""
}

// nfsVersionOptionFor derives the mount option that pins the NFS version for
// this volume, or "" to leave the version to the kernel or to the class's own
// mountOptions.
//
// An absent protocol is not defaulted. Volumes provisioned before the protocol
// parameter existed carry no value, and neither does a class that did not set
// one; adding a version option for them would silently remount every existing
// volume differently on upgrade.
func nfsVersionOptionFor(volumeContext map[string]string, mountFlags []string) (string, error) {
	protocol := strings.TrimSpace(volumeContext[volumeContextProtocol])
	if protocol == "" {
		return "", nil
	}

	var option, want string
	switch protocol {
	case protocolNFS3:
		option, want = "nfsvers=3", "3"
	case protocolNFS41:
		option, want = "nfsvers=4.1", "4.1"
	default:
		// The value came from a PV the CO stored, not from a freshly validated
		// StorageClass, so it can be anything a hand-edited object contains.
		return "", fmt.Errorf("volume context has unsupported %s %q", paramProtocol, protocol)
	}

	// The StorageClass can also pin a version through its own mountOptions,
	// which arrive here as mount flags. One that pins a different version is
	// refused: letting either win silently would mount a version the
	// administrator did not ask for.
	//
	// One that pins the same version is not a contradiction, and is accepted
	// with nothing appended -- the class already said it. Refusing it broke
	// volumes that had mounted fine, an nfs3 class with mountOptions [v3]
	// among them, and a PV's mountOptions cannot be edited, so their pods
	// could never start again on the upgraded driver.
	pinned := false
	for _, flag := range mountOptionList(mountFlags) {
		version := nfsVersionOf(flag)
		if version == "" {
			continue
		}
		// "3.0" and the like name NFSv3 as surely as "3"; pinsNFSv3 reads
		// them the same way.
		if want == "3" && strings.HasPrefix(version, "3.") {
			version = want
		}
		if version != want {
			return "", fmt.Errorf("volume requests %s=%s but its mount options set %q; "+
				"set the version in one place, not both", paramProtocol, protocol, flag)
		}
		pinned = true
	}
	if pinned {
		return "", nil
	}
	return option, nil
}

// mountSourceFor builds the NFS mount source from the VolumeContext the
// controller stamped onto the volume.
//
// VolumeContext is the only channel between the controller and node services,
// so a key the controller never set arrives here as an empty string rather
// than as an error. Left unchecked, that mounts "<server>:" or ":<path>" --
// which fails deep inside mount(8) with a message that points nowhere near
// the cause, and only after a directory has been created for it.
func mountSourceFor(volumeContext map[string]string) (string, error) {
	server := volumeContext[volumeContextServer]
	exportPath := volumeContext[volumeContextExportPath]

	var missing []string
	if server == "" {
		missing = append(missing, volumeContextServer)
	}
	if exportPath == "" {
		missing = append(missing, volumeContextExportPath)
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("volume context is missing required key(s): %s",
			strings.Join(missing, ", "))
	}

	return fmt.Sprintf("%s:%s", server, exportPath), nil
}

// confirmExistingMount decides whether an already-mounted target satisfies
// this request.
//
// Republishing the same volume with the same parameters must succeed -- the CO
// retries until it does. A request that differs from what is mounted is a
// different matter: the spec's answer is ALREADY_EXISTS, and reporting OK
// instead would tell the CO a read-only publish took effect while the
// workload still holds write access.
func (ns *NodeServer) confirmExistingMount(
	ctx context.Context,
	volumeID, targetPath, source string,
	readonly, requestRO bool,
	volumeContext map[string]string,
) (*csi.NodePublishVolumeResponse, error) {
	mounted, err := ns.existingMount(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspecting existing mount at %s: %v", targetPath, err)
	}
	if mounted == nil {
		// The path looks like a mount point but the mount table does not list
		// it. Racing the kubelet over an unmount is not this RPC's business,
		// and the CO will retry, so report the publish as satisfied.
		slog.WarnContext(ctx, "nodepublishvolume: target is a mount point but absent from the mount table",
			"targetPath", targetPath)
		// Still probed: whatever is mounted there is what the pod will write
		// to, and a read-write answer must not skip the check.
		if err := ns.verifyReplicaIsWritable(ctx, volumeID, targetPath, volumeContext, readonly,
			ns.unconfirmed.has(targetPath)); err != nil {
			return nil, err
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if mounted.Device != source {
		return nil, status.Errorf(codes.AlreadyExists,
			"%s already has %q mounted; cannot publish %q there",
			targetPath, mounted.Device, source)
	}
	if mountedReadOnly := isReadOnly(mounted.Opts); mountedReadOnly != readonly {
		return nil, status.Errorf(codes.AlreadyExists,
			"%s is already mounted with readonly=%t, but was requested with readonly=%t",
			targetPath, mountedReadOnly, readonly)
	}
	// The mount table's ro is the mount's or the superblock's; a read-only
	// republish needs both. The mount is left in place: a running pod
	// may hold it.
	if readonly {
		if err := ns.requireReadOnlyMount(ctx, volumeID, targetPath, false, requestRO); err != nil {
			return nil, err
		}
	}

	// The mount matches, but nothing says it was ever probed. A first
	// publish whose mount outlived mountTimeout answered DeadlineExceeded
	// and left the mount to finish in the background, and an EROFS refusal
	// whose unmount failed left its read-only mount behind; either way the
	// retry lands here, and answering OK would publish an unpromoted replica
	// read-write. So a read-write republish is probed exactly as a
	// fresh mount is -- every one, not only a marked replica, for the reason
	// verifyReplicaIsWritable gives.
	if err := ns.verifyReplicaIsWritable(ctx, volumeID, targetPath, volumeContext, readonly,
		ns.unconfirmed.has(targetPath)); err != nil {
		return nil, err
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

// existingMount returns the mount table entry for path, or nil when the table
// has none.
func (ns *NodeServer) existingMount(path string) (*mount.MountPoint, error) {
	mounts, err := ns.mounter.List()
	if err != nil {
		return nil, err
	}

	for i := range mounts {
		if mounts[i].Path == path {
			return &mounts[i], nil
		}
	}
	return nil, nil
}

// volumePathStatError maps a failed stat of a volume path to the status the
// CO should see.
//
// Split out because the interesting case cannot be produced from a unit test
// otherwise: a real ESTALE needs a live mount whose server has gone away.
// Given the error, the classification is testable; given a directory, it is
// not.
func volumePathStatError(volumePath string, err error) error {
	// The spec requires NotFound specifically when the path is not there, so
	// the CO can tell "not mounted" apart from "could not measure".
	if os.IsNotExist(err) {
		return status.Errorf(codes.NotFound, "volume path %s does not exist", volumePath)
	}

	// A stale mount breaks statfs exactly as it breaks everything else, so
	// kubelet_volume_stats_* goes silent at the same moment the volume becomes
	// unusable -- the metric an operator would notice is disabled by the same
	// fault. This cannot fix the mount (only NodePublishVolume can, and it now
	// does), but it can name the fault instead of reporting an anonymous
	// Internal.
	if isCorruptedMount(err) {
		return status.Errorf(codes.Internal,
			"volume path %s is a stale mount and cannot be measured (%v); "+
				"%s", volumePath, err, staleMountRemedy)
	}

	return status.Errorf(codes.Internal, "checking volume path %s: %v", volumePath, err)
}

// staleMountRemedy is what a stale mount's messages tell the operator. The
// next NodePublishVolume for the target does remount it, but a running
// container holds its own bind of the dead mount, which a new mount at the
// target never reaches, and the kubelet republishes only on a restart.
const staleMountRemedy = "a running container keeps the dead mount: delete the pod to get a fresh one " +
	"(its NodePublishVolume remounts the target)"

// mountWithTimeout runs one mount and stops waiting for it after
// ns.mountTimeout.
//
// A goroutine and a select, because k8s.io/mount-utils takes no context:
// Mount() shells out to mount(8) and there is no way to hand it a deadline
// without replacing the mounter with a custom Exec built on CommandContext.
// This is the smaller change and it does not alter how mounting works.
//
// The trade-off, stated rather than hidden: on a timeout the goroutine
// outlives this call and ends whenever the kernel finally gives up -- one per
// target, because a retry joins it, and never a permanent one. The
// alternative -- killing mount(8) mid-syscall -- is worse.
func (ns *NodeServer) mountWithTimeout(
	ctx context.Context,
	source, target, fsType string,
	options []string,
) error {
	// Joined per source and target: a kubelet retry of a publish
	// whose mount(8) outlived the timeout waits on that mount instead of
	// starting another beside it.
	call := ns.mountCalls.join("mount\x00"+source+"\x00"+target, "mounting "+target, func() (volumeStats, error) {
		// Held for as long as mount(8) runs, even past the timeout: a mount
		// still in the kernel can still join a superblock (see forceGate).
		ns.forceGate.RLock()
		defer ns.forceGate.RUnlock()
		return volumeStats{}, ns.mounter.Mount(source, target, fsType, options)
	})
	return awaitCall(ctx, call, "mounting "+source+" at "+target, ns.mountTimeout, errMountTimeout,
		"nodeserver: mount operation timed out; it may still be retrying in the kernel")
}

// joinUnmount runs the plain umount(8) of target in the mount flight map, so
// a retry while one is blocked joins it.
func (ns *NodeServer) joinUnmount(target string) *statCall {
	return ns.mountCalls.join("umount\x00"+target, "unmounting "+target, func() (volumeStats, error) {
		return volumeStats{}, ns.mounter.Unmount(target)
	})
}

// unmountWithTimeout is the counterpart used on the stale-mount recovery
// path: unmounting a dead mount can block for the same reasons mounting can,
// and recovery that hangs is no better than no recovery.
func (ns *NodeServer) unmountWithTimeout(ctx context.Context, target string) error {
	return awaitCall(ctx, ns.joinUnmount(target), "unmounting "+target, ns.mountTimeout, errMountTimeout,
		"nodeserver: mount operation timed out; it may still be retrying in the kernel")
}

// awaitCall waits for a joined call, its timeout, or the caller going away,
// whichever comes first. On a timeout the call keeps running and stays
// joinable, so the retry waits on it rather than starting another.
func awaitCall(ctx context.Context, call *statCall, what string, timeout time.Duration, timeoutErr error, logMsg string) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-call.done:
		return call.err
	case <-timer.C:
		slog.ErrorContext(ctx, logMsg, "operation", what, "timeout", timeout)
		return fmt.Errorf("%s: %w after %s", what, timeoutErr, timeout)
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", what, ctx.Err())
	}
}

// statWithTimeout stats a volume path and stops waiting after ns.statTimeout.
//
// A goroutine and a select, for the same reason mountWithTimeout uses one:
// stat(2) against a mount whose NFS server has hard-hung blocks in the
// kernel with no way to attach a deadline, so the call is bounded from the
// outside instead. The same trade-off applies: on a timeout the goroutine
// outlives this call and exits whenever the kernel finally gives up.
func (ns *NodeServer) statWithTimeout(ctx context.Context, path string) error {
	call := ns.statCalls.join("stat\x00"+path, "statting "+path, func() (volumeStats, error) {
		_, err := ns.statFunc(path)
		return volumeStats{}, err
	})
	_, err := ns.awaitStatCall(ctx, call, "checking "+path)
	return err
}

// statfsWithTimeout reads a volume's usage and stops waiting after
// ns.statTimeout. statfs(2) is the syscall that actually round-trips to the
// NFS server, so it is exactly as bound-worthy as statWithTimeout's stat(2)
// and mountWithTimeout's mount(8). The same trade-off applies: on a timeout
// the goroutine outlives this call and exits whenever the kernel gives up.
func (ns *NodeServer) statfsWithTimeout(ctx context.Context, path string) (volumeStats, error) {
	call := ns.statCalls.join("statfs\x00"+path, "reading stats for "+path, func() (volumeStats, error) {
		return ns.statfsFunc(path)
	})
	return ns.awaitStatCall(ctx, call, "reading stats for "+path)
}

// awaitStatCall waits for a stat-shaped call, the timeout, or the caller
// going away, whichever comes first. On a timeout the call keeps running and
// stays joinable, so the next poll waits on it rather than starting another.
func (ns *NodeServer) awaitStatCall(ctx context.Context, call *statCall, what string) (volumeStats, error) {
	timer := time.NewTimer(ns.statTimeout)
	defer timer.Stop()

	select {
	case <-call.done:
		return call.stats, call.err
	case <-timer.C:
		// ERROR the first time this call times out, Debug for every poll
		// that joins it after: each kubelet poll of a hung volume used to
		// log it again, about 110 lines an hour per volume, which rotated
		// the first failure out of the log.
		level := slog.LevelDebug
		if call.timeoutLogged.CompareAndSwap(false, true) {
			level = slog.LevelError
		}
		slog.Log(ctx, level, "nodeserver: stat operation timed out; it may still be blocked in the kernel",
			"operation", what, "timeout", ns.statTimeout)
		return volumeStats{}, fmt.Errorf("%s: %w after %s", what, errStatTimeout, ns.statTimeout)
	case <-ctx.Done():
		return volumeStats{}, fmt.Errorf("%s: %w", what, ctx.Err())
	}
}

// statFlights runs at most one stat-shaped call per key at a time.
//
// A stat on a hard NFS mount whose server is gone blocks in the kernel, and
// the goroutine running it pins an OS thread until the kernel gives up. The
// calls are bounded from the outside, but kubelet polls NodeGetVolumeStats on
// its own schedule, and every poll used to start another goroutine beside the
// ones still blocked: measured at about six threads a minute for ten wedged
// mounts, towards Go's 10,000-thread abort, which takes the node plugin down
// for every volume on the node. Joining the call already in flight bounds it
// at one blocked goroutine per path per kind of call. The zero value is ready
// to use.
type statFlights struct {
	mu       sync.Mutex
	inflight map[string]*statCall
}

// statCall is one stat-shaped call; done is closed once stats and err are set.
type statCall struct {
	done  chan struct{}
	stats volumeStats
	err   error
	// timeoutLogged is set by the first wait on this call that timed out,
	// so a hung call is logged at ERROR once, not once per poll.
	timeoutLogged atomic.Bool
}

// join returns the call in flight for key, starting fn as one if there is
// none. A call leaves the set when fn returns, so the next poll after that
// measures afresh.
func (f *statFlights) join(key, what string, fn func() (volumeStats, error)) *statCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if call, ok := f.inflight[key]; ok {
		return call
	}
	if f.inflight == nil {
		f.inflight = make(map[string]*statCall)
	}
	call := &statCall{done: make(chan struct{})}
	f.inflight[key] = call
	go func() {
		defer func() {
			if r := recover(); r != nil {
				call.err = panicError(what, r)
			}
			f.mu.Lock()
			// Only this call's entry: forget may have dropped it and a
			// newer call taken the key since.
			if f.inflight[key] == call {
				delete(f.inflight, key)
			}
			f.mu.Unlock()
			close(call.done)
		}()
		call.stats, call.err = fn()
	}()
	return call
}

// forget drops keys from the set without waiting for their calls, which may
// never return. A caller still waiting on one keeps it; the next join for the
// key starts afresh. Freed is the entry, not the goroutine blocked in the
// kernel.
func (f *statFlights) forget(keys ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range keys {
		delete(f.inflight, key)
	}
}

// forgetPath drops target's stat, statfs, write-probe and umount calls once
// unpublish has removed it. A call blocked on a dead mount never
// returns, so its entry outlived the volume. Mount calls are kept: the forced
// unmount is gated on a mount(8) still in the kernel.
func (ns *NodeServer) forgetPath(target string) {
	ns.statCalls.forget("stat\x00"+target, "statfs\x00"+target, "probe\x00"+target)
	ns.mountCalls.forget("umount\x00" + target)
}

// isReadOnly reports whether a mount's option list marks it read-only. The
// kernel always records one of ro/rw in the mount table, so the absence of
// "ro" means read-write.
func isReadOnly(opts []string) bool {
	return slices.Contains(mountOptionList(opts), "ro")
}

// mountOptionList is opts as the individual options mount(8) will see.
//
// mount-utils joins mountOptions entries with commas, so one entry may carry
// several options: "hard,ro" mounts hard and read-only. The guards compared
// whole entries, so that entry mounted read-only while the driver took it for
// read-write -- and its write probe then refused every publish as an
// unpromoted replica -- and "hard,lock" on NFSv3 got past the refusal the
// same "lock" written alone gets. Every guard reads this instead.
func mountOptionList(opts []string) []string {
	var out []string
	for _, entry := range opts {
		for _, opt := range strings.Split(entry, ",") {
			if opt = strings.TrimSpace(opt); opt != "" {
				out = append(out, opt)
			}
		}
	}
	return out
}

// NodeUnpublishVolume unmounts a volume from the target path, escalating to a
// forced and then a lazy unmount when the array is not answering.
//
// The escalation is the node half of the forced-unmount rule, and
// node_unmount.go
// carries the measurement and the industry comparison that decided its shape.
// The short version: this RPC used to do a plain unmount, umount(8) blocks
// indefinitely on a dead NFS server whatever mount options were used, and the
// result measured in the lab was pods that could not terminate inside their
// grace period and stale mounts left on the node after they were force-deleted.
//
// It returns OK only when the mount is gone from the node's mount table --
// not merely when umount(8) exited 0.
func (ns *NodeServer) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest,
) (*csi.NodeUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if err := validateNodeVolumeID(volumeID); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}
	// This path is unmounted and then removed, so a relative or unclean one
	// would remove something else.
	if err := validateNodeTargetPath(targetPath); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	slog.DebugContext(ctx, "nodeunpublishvolume: unmounting",
		"volumeID", volumeID, "targetPath", targetPath)

	if err := ns.cleanupPublishedMount(ctx, targetPath); err != nil {
		// Pass a status error through with the code it chose. The cleanup
		// answers FailedPrecondition when a target holding data could not be
		// moved out of the kubelet's tree, naming both paths and the
		// remedy; re-wrapping every error as Internal threw that code away
		// and left the operator an opaque one in the single case that most
		// needs a precise one. Only a plain error, which carries no code of
		// its own, becomes Internal.
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	ns.unconfirmed.remove(targetPath)
	// The target is out of the mount table and gone, so a new call on it
	// cannot reach the dead mount; calls still blocked on it are dropped.
	ns.forgetPath(targetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeHealth reports whether a published volume is still usable.
//
// The spelling matters: the request was for
// a volume_condition field on NodeGetVolumeStats and a VOLUME_CONDITION
// capability, which is how CSI expressed this before v1.13. In the spec this
// driver builds against it is a dedicated RPC gated by GET_VOLUME_HEALTH, and
// NodeGetVolumeStatsResponse has no health field at all. Same signal, moved.
//
// The failure it exists to catch is the quiet one. When an export is removed
// on the array, or a node loses its mount, the directory is still there and
// still writable -- it is just a directory on the node's own disk now. Nothing
// errors. A pod keeps writing, and the data goes nowhere anybody will find it.
// Every other signal the driver emits stays green through this, because the
// RPCs that would notice are not being called.
//
// Reported, never returned as an error. An error answers "I could not assess
// this volume", which a CO cannot distinguish from a broken driver; an empty
// health_statuses list is the spec's spelling of healthy, and a populated one
// is an assessment. The only errors here are for a malformed request.
func (ns *NodeServer) NodeGetVolumeHealth(
	ctx context.Context,
	req *csi.NodeGetVolumeHealthRequest,
) (*csi.NodeGetVolumeHealthResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if err := validateNodeVolumeID(volumeID); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	health := &csi.VolumeHealth{VolumeId: volumeID}

	// volume_publish_path is OPTIONAL in the spec, and a CO is entitled to
	// omit it -- csi-sanity does exactly that. Without a path there is nothing
	// on this node to inspect, and the honest answer is the empty list, which
	// the spec defines as "no adverse health condition is known by the
	// Plugin". Refusing the request instead would be a driver-shaped error for
	// a request that is not malformed.
	//
	// staging_target_path is never consulted: this driver does not advertise
	// STAGE_UNSTAGE_VOLUME, so the spec says that field is empty, and an NFS
	// export has no per-node staging mount to assess. See NodeStageVolume.
	volumePath := req.GetVolumePublishPath()
	if volumePath == "" {
		slog.DebugContext(ctx, "nodegetvolumehealth: no publish path in the request; "+
			"reporting no known condition",
			"volumeID", volumeID)
		return &csi.NodeGetVolumeHealthResponse{VolumeHealth: health}, nil
	}

	if entry := ns.assessVolumePath(ctx, volumePath); entry != nil {
		slog.WarnContext(ctx, "nodegetvolumehealth: volume is not healthy",
			"volumeID", volumeID, "volumePath", volumePath,
			"status", entry.GetStatus().String(), "reason", entry.GetReason(),
			"message", entry.GetMessage())
		health.HealthStatuses = []*csi.VolumeHealth_VolumeHealthEntry{entry}
	}

	return &csi.NodeGetVolumeHealthResponse{VolumeHealth: health}, nil
}

// assessVolumePath returns the one health entry describing what is wrong with
// a published path, or nil when nothing is.
//
// Ordered by how badly each case misleads an operator, worst first, because
// only the first finding is reported: a path that is not a mount point at all
// is the silent one, and naming a slow mount instead would bury it.
func (ns *NodeServer) assessVolumePath(ctx context.Context, volumePath string) *csi.VolumeHealth_VolumeHealthEntry {
	if err := ns.statWithTimeout(ctx, volumePath); err != nil {
		switch {
		case errors.Is(err, errStatTimeout):
			// A hard NFS mount blocks indefinitely when the server is
			// unreachable, which is the intended behaviour of `hard` and also
			// indistinguishable from a dead array. Either way the volume is
			// not serving I/O right now.
			return healthEntry(csi.VolumeHealthErrorType_INACCESSIBLE, "MountUnresponsive",
				fmt.Sprintf("%s did not respond within %s; the Qumulo cluster may be "+
					"unreachable from this node, or the mount may be hung", volumePath, ns.statTimeout))

		case os.IsNotExist(err):
			return healthEntry(csi.VolumeHealthErrorType_INACCESSIBLE, "PathMissing",
				fmt.Sprintf("%s does not exist on this node", volumePath))

		case isCorruptedMount(err):
			return healthEntry(csi.VolumeHealthErrorType_INACCESSIBLE, "StaleMount",
				fmt.Sprintf("%s is a stale NFS mount (%v); %s", volumePath, err, staleMountRemedy))

		default:
			// Something answered, and it was not a shape worth naming
			// specifically. DEGRADED rather than INACCESSIBLE: the volume may
			// well still be serving I/O, and claiming otherwise on an
			// unrecognized errno would be the kind of false alarm that teaches
			// people to ignore the signal.
			return healthEntry(csi.VolumeHealthErrorType_DEGRADED, "Unreadable",
				fmt.Sprintf("could not check %s: %v", volumePath, err))
		}
	}

	// The path is there and responsive. The remaining question is whether it
	// is still the NFS mount -- and this is the case the whole RPC is for,
	// because it is the one nothing else notices.
	notMountPoint, err := ns.mounter.IsLikelyNotMountPoint(volumePath)
	if err != nil {
		if isCorruptedMount(err) {
			return healthEntry(csi.VolumeHealthErrorType_INACCESSIBLE, "StaleMount",
				fmt.Sprintf("%s is a stale NFS mount (%v); %s", volumePath, err, staleMountRemedy))
		}
		return healthEntry(csi.VolumeHealthErrorType_DEGRADED, "MountStateUnknown",
			fmt.Sprintf("could not determine whether %s is still mounted: %v", volumePath, err))
	}
	if notMountPoint {
		return healthEntry(csi.VolumeHealthErrorType_INACCESSIBLE, "NotMounted",
			fmt.Sprintf("%s exists but nothing is mounted on it, so writes are landing on "+
				"the node's own disk instead of the Qumulo cluster and will be lost", volumePath))
	}

	return nil
}

// healthEntry builds one VolumeHealth entry.
//
// Reason is CamelCase and stable, because a CO surfaces it as an Event reason
// and operators build alerts on it; Message is the sentence a human reads and
// may change freely.
func healthEntry(errorType csi.VolumeHealthErrorType, reason, message string) *csi.VolumeHealth_VolumeHealthEntry {
	return &csi.VolumeHealth_VolumeHealthEntry{
		Status:  errorType,
		Reason:  reason,
		Message: message,
	}
}

// NodeStageVolume is not implemented: an NFS export can be mounted
// independently by every pod that wants it, so there is no per-node staging
// mount to set up first. See the nodeCapabilities comment above -- this
// driver never advertises STAGE_UNSTAGE_VOLUME, so the CO is not expected to
// call this RPC at all.
func (ns *NodeServer) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest,
) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented,
		"NodeStageVolume is not implemented: NFS exports need no per-node staging mount")
}

// NodeUnstageVolume mirrors NodeStageVolume: nothing was staged, so there is
// nothing to unstage.
//
// This is where a driver that advertised STAGE_UNSTAGE_VOLUME would put the
// forced unmount alongside NodeUnpublishVolume's, and it deliberately has none.
// There is no staging mount to force away: this driver never advertises the
// capability, so the CO never calls this RPC and never creates the mount it
// would be tearing down. Answering OK here -- or unmounting a staging path
// that was never staged -- would claim work that did not happen. The forced
// unmount lives in exactly the one place a mount exists to remove; see
// node_unmount.go.
func (ns *NodeServer) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest,
) (*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented,
		"NodeUnstageVolume is not implemented: NFS exports need no per-node staging mount")
}

// NodeExpandVolume is not implemented for the same reason
// ControllerExpandVolume reports NodeExpansionRequired false: growing the
// quota is the whole operation, and there is no node-side filesystem left to
// resize afterwards.
func (ns *NodeServer) NodeExpandVolume(
	ctx context.Context,
	req *csi.NodeExpandVolumeRequest,
) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented,
		"NodeExpandVolume is not implemented: there is no node-side filesystem to resize")
}

// requireKeytab refuses a Kerberos mount on a node with no usable keytab.
//
// The chart mounts the host's keytab with hostPath type FileOrCreate, so a
// node without one -- not domain-joined -- still runs the node plugin for
// every other class, with an empty placeholder in its place. A
// Kerberos mount there could only fail inside mount.nfs with an opaque
// "access denied"; this says what is wrong, before anything is mounted.
func (ns *NodeServer) requireKeytab() error {
	path := ""
	if ns.driver != nil {
		path = ns.driver.KerberosKeytab
	}
	if path == "" {
		return nil
	}
	info, err := os.Stat(path)
	switch {
	case err != nil:
		return status.Errorf(codes.FailedPrecondition,
			"this node has no Kerberos keytab at %s (%v); a sec=krb5 volume can only mount on a node joined to the "+
				"domain -- schedule the pod onto one", path, err)
	case info.Size() == 0:
		return status.Errorf(codes.FailedPrecondition,
			"this node's Kerberos keytab at %s is empty: the host has none, and the chart mounted an empty "+
				"placeholder so that other volumes still mount here. A sec=krb5 volume can only mount on a node "+
				"joined to the domain -- schedule the pod onto one", path)
	}
	return nil
}
