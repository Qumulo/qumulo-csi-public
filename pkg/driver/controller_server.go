// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"qumulo-csi-driver/pkg/qumulo"
)

// ControllerServer implements csi.ControllerServer.
//
// A volume is a directory under the StorageClass's storeRealPath carrying a
// Qumulo directory quota for its capacity.
//
// Under the default shared-subdirectory model the parent is exported over NFS
// once, by an administrator, and the driver touches no export at all -- so
// provisioning adds a fixed number of cluster objects per volume and cannot
// exhaust the export table. Under dedicated-export each volume additionally
// gets its own export, which is one export-table entry per volume: more
// isolation, at the cost of a ceiling the shared model does not have.
type ControllerServer struct {
	csi.UnimplementedControllerServer
	driver *Driver

	// restores serialises snapshot restores per volume name.
	restores *restoreLocks
	// restoreSlots bounds restore attempts running at once, across every
	// volume.
	restoreSlots chan struct{}

	// snapshotLocks serialises CreateSnapshot per snapshot name, and
	// expandLocks serialises ControllerExpandVolume per volume handle, so
	// neither RPC can race its own retry between a check and the action it
	// decided from it. See keyedLocks.
	snapshotLocks *keyedLocks
	expandLocks   *keyedLocks
	// createLocks serialises a fresh CreateVolume per volume name, so a
	// retry that overlaps its first call waits for it rather than adopting
	// a directory the first call is about to roll back.
	createLocks *keyedLocks

	// restoreProgress carries a restore's walk bookkeeping from one attempt
	// to the next. See restoreProgress.
	restoreProgress *restoreProgress
	// snapshotSizes remembers the summed file sizes of snapshots measured
	// whole. See snapshotSizes.
	snapshotSizes *snapshotSizes

	// stagingSweeper limits CreateVolume's sweep for abandoned restore
	// staging directories, and remembers what earlier sweeps found.
	stagingSweeper *restoreStagingSweeper

	// nfsSettings caches each cluster's NFS configuration, so the protocol
	// preflight costs no round trip per provision.
	nfsSettings *nfsSettingsCache

	// treeDeleteBudget bounds how long DeleteVolume waits for the cluster's
	// asynchronous tree deletion, and treeDeletePollWait spaces the status
	// checks. They are fields rather than constants so tests can exercise
	// the timeout path without actually waiting for it.
	treeDeleteBudget   time.Duration
	treeDeletePollWait time.Duration

	// pvWalk is the PV list a ListVolumes walk's first page read, reused by
	// the pages after it.
	pvWalk walkCache[VolumeRef]
	// classWalk is the StorageClass list a ListVolumes walk's first page
	// read, reused by its later pages for up to a minute.
	classWalk walkCache[StorageClassInfo]

	// noPVListerLogged makes DeleteVolume say once, not per call, that it
	// has no PersistentVolume lister and skips the ownership check.
	noPVListerLogged sync.Once
}

// NewControllerServer creates a ControllerServer backed by the given Driver.
func NewControllerServer(driver *Driver) *ControllerServer {
	return &ControllerServer{
		driver:             driver,
		restores:           newRestoreLocks(),
		restoreSlots:       make(chan struct{}, maxConcurrentRestores),
		snapshotLocks:      newKeyedLocks(),
		expandLocks:        newKeyedLocks(),
		createLocks:        newKeyedLocks(),
		restoreProgress:    newRestoreProgress(),
		snapshotSizes:      newSnapshotSizes(),
		stagingSweeper:     newRestoreStagingSweeper(),
		nfsSettings:        newNFSSettingsCache(),
		treeDeleteBudget:   defaultTreeDeleteBudget,
		treeDeletePollWait: defaultTreeDeletePollWait,
	}
}

// controllerCapabilities is everything this controller serves.
//
// All three RPCs the CSI spec gives no secrets map are now here, and they get
// their credentials in two different ways. GetCapacity is handed the
// StorageClass's parameter map verbatim, secret-reference keys included, so it
// resolves the one Secret its own request named. ListVolumes and
// ControllerGetVolume name no Secret, so they discover clusters by listing
// StorageClasses through the Kubernetes API -- which is why those two are
// opt-in behind controller.inventory.enabled. See get_capacity.go,
// list_volumes.go and Rule 0 in ARCHITECTURE.md.
//
// **The set is fixed rather than computed, and that is deliberate even though
// two of these depend on configuration.** A capability that appears and
// disappears with a value leaves an operator unable to tell a misconfiguration
// from a bug, and leaves a CO's behaviour depending on when it happened to ask.
// So the advertised set is a property of the build, and a deployment that has
// not opted in answers FailedPrecondition with a message naming the value to
// set. That is a worse answer than working, and a much better one than a
// capability that silently comes and goes.
var controllerCapabilities = []csi.ControllerServiceCapability_RPC_Type{
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
	csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	csi.ControllerServiceCapability_RPC_GET_CAPACITY,
	csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
	csi.ControllerServiceCapability_RPC_GET_VOLUME,
	// Answered with the request's Secret, or from the classes as for
	// GET_VOLUME (controller.inventory.enabled). See volume_health.go.
	csi.ControllerServiceCapability_RPC_GET_VOLUME_HEALTH,
	// LIST_VOLUMES_PUBLISHED_NODES is deliberately absent: see the Status
	// field in ListVolumes.
}

// ControllerGetCapabilities reports which optional controller RPCs the CO
// may call.
func (s *ControllerServer) ControllerGetCapabilities(
	ctx context.Context,
	req *csi.ControllerGetCapabilitiesRequest,
) (*csi.ControllerGetCapabilitiesResponse, error) {
	advertised := controllerCapabilities
	caps := make([]*csi.ControllerServiceCapability, 0, len(advertised))
	for _, rpc := range advertised {
		caps = append(caps, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{Type: rpc},
			},
		})
	}

	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

// ControllerPublishVolume is not implemented: an NFS export has no
// attach/detach step for the controller to arbitrate (see the
// controllerCapabilities comment above), so the CO is not expected to call
// this RPC at all.
func (s *ControllerServer) ControllerPublishVolume(
	ctx context.Context,
	req *csi.ControllerPublishVolumeRequest,
) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented,
		"ControllerPublishVolume is not implemented: NFS exports need no controller-side attach step")
}

// ControllerUnpublishVolume mirrors ControllerPublishVolume: nothing was
// attached, so there is nothing to detach.
func (s *ControllerServer) ControllerUnpublishVolume(
	ctx context.Context,
	req *csi.ControllerUnpublishVolumeRequest,
) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented,
		"ControllerUnpublishVolume is not implemented: NFS exports need no controller-side attach step")
}

// CreateVolume provisions a directory with a quota, and is idempotent: the
// CO retries it until it succeeds, so a repeat call for an already-created
// volume must return that volume rather than an error.
func (s *ControllerServer) CreateVolume(
	ctx context.Context,
	req *csi.CreateVolumeRequest,
) (*csi.CreateVolumeResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name must not be empty")
	}
	if err := validateVolumeName(name); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	caps := req.GetVolumeCapabilities()
	if len(caps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities must not be empty")
	}
	if err := validateVolumeCapabilities(caps); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// A content source means a restore. Only snapshots are supported --
	// cloning a live volume would need a consistent source, which a directory
	// being written to is not.
	var sourceSnapshot *snapshotID
	if src := req.GetVolumeContentSource(); src != nil {
		snap := src.GetSnapshot()
		if snap == nil {
			return nil, status.Error(codes.Unimplemented,
				"only snapshot content sources are supported; cloning a volume is not")
		}
		parsed, err := parseSnapshotID(snap.GetSnapshotId())
		if err != nil {
			return nil, status.Errorf(codes.NotFound,
				"source snapshot %q not found: %v", snap.GetSnapshotId(), err)
		}
		sourceSnapshot = &parsed
	}

	params, err := parseVolumeParams(req.GetParameters())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Before anything is created on the array.
	if s.driver == nil || !s.driver.AllowOpenDedicatedExports {
		if err := openExportRefusal(params); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	// A snapshot lives on one cluster, and copy-chunk cannot reach across
	// clusters. Refusing explicitly beats provisioning an empty volume that
	// looks restored.
	if sourceSnapshot != nil && !strings.EqualFold(sourceSnapshot.Server, params.Server) {
		return nil, status.Errorf(codes.InvalidArgument,
			"snapshot %q is on cluster %q but this StorageClass provisions on %q; "+
				"a snapshot can only be restored on its own cluster",
			sourceSnapshot.String(), sourceSnapshot.Server, params.Server)
	}
	// The server alone is not the cluster. Two arrays can share one data
	// name (a floating DNS name for failover), and the REST endpoint is what
	// tells them apart; snapshot IDs are per-cluster numbers, so reading this
	// one on the class's array would find a different snapshot, or none.
	// The client is not built from the snapshot's endpoint instead:
	// copy-chunk cannot cross arrays, and the volume would land on an array
	// the class does not name. Two spellings of one cluster's endpoint are
	// refused too -- nothing here can prove they are the same cluster.
	if sourceSnapshot != nil && !sameEndpoint(sourceSnapshot.APIEndpoint(), apiURLFor(params.Server, params.APIURL)) {
		classEndpoint := apiURLFor(params.Server, params.APIURL)
		hint := ""
		if sourceSnapshot.APIURL == "" {
			// An ID with no endpoint field (<server>/<id>, or <server>##<id>)
			// cannot say which array it came from; it means the server's
			// default endpoint, and is not guessed to be the class's.
			hint = fmt.Sprintf(". The ID names no endpoint, so it means %s; if the snapshot is on the "+
				"array this class reaches, import it as %q",
				sourceSnapshot.APIEndpoint(), newSnapshotID(sourceSnapshot.Server, classEndpoint, sourceSnapshot.ID).String())
		}
		return nil, status.Errorf(codes.InvalidArgument,
			"snapshot %q was taken through %s but this StorageClass reaches its cluster through %s; "+
				"a snapshot can only be restored on its own cluster, through a class whose apiUrl names it%s",
			sourceSnapshot.String(), sourceSnapshot.APIEndpoint(), classEndpoint, hint)
	}

	// The handle this volume will carry, measured before anything is created.
	//
	// A handle names a path (volume_id.go) and the CSI spec says one SHOULD
	// NOT exceed 128 bytes. The ordinary case is about 75, but a deep basePath
	// plus an adDomain plus a long name passes it.
	//
	// This warns rather than refuses, and the reason is worth recording
	// because the first cut refused and csi-sanity caught it. The spec sets
	// the same 128-byte SHOULD on the *name* in this very request, and
	// "should not fail when creating volume with maximum-length name" is a
	// conformance case: a 128-byte name plus any base path cannot produce a
	// handle under 128 bytes, so the two SHOULDs cannot both be honoured.
	// Refusing would trade a documented SHOULD for a conformance failure and
	// would break a volume the CO is entitled to ask for. Both limits are
	// SHOULD NOT, and the handle round-trips through this driver's own parser
	// at any length, so the honest answer is to provision and say so.
	if id := newVolumeID(params.Server, params.APIURL, params.realPathFor(name)); id.tooLong() {
		slog.WarnContext(ctx, "createvolume: the volume handle exceeds the size the CSI spec asks for",
			"volume", name, "cluster", params.Server, "handleBytes", len(id.String()),
			"limit", maxVolumeHandleBytes, "handle", id.String(),
			"consequence", "this driver handles it, but a CO that enforces the limit may not; "+
				"shorten "+paramStoreRealPath+" or "+paramADDomain+" if that matters")
	}

	client, err := s.clientFor(params.Server, params.APIURL, req.GetSecrets())
	if err != nil {
		return nil, err
	}

	capacity, err := requiredCapacity(req.GetCapacityRange())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Before creating anything: refuse a protocol this cluster cannot serve,
	// so the failure lands on the PVC with a reason instead of on a pod with a
	// kernel error.
	// Before creating anything: refuse a Kerberos volume the cluster cannot
	// serve, so the failure lands on the PVC with a reason instead of on a pod
	// that mounts and then cannot read its own files.
	if err := s.checkSecurityFlavorSupported(ctx, client, params); err != nil {
		if u := unavailableIfNoAnswer(params, err); u != nil {
			return nil, u
		}
		return nil, status.Errorf(codes.FailedPrecondition,
			"%s %q cannot be served by cluster %q: %v. %s",
			paramSecurityFlavor, params.SecurityFlavor, params.Server, err,
			nfsSettingsStalenessNote)
	}

	// AUTH_SYS is the other half of the same question: the Kerberos check
	// above covers the flavours that need a ticket, this one the flavour that
	// needs the cluster to accept a plain uid. InvalidArgument rather than
	// FailedPrecondition, matching the protocol check below -- what has to
	// change is the class, not the cluster's state at this instant.
	if err := s.checkAuthSysSupported(ctx, client, params); err != nil {
		if u := unavailableIfNoAnswer(params, err); u != nil {
			return nil, u
		}
		return nil, status.Errorf(codes.InvalidArgument,
			"%s %q cannot be served by cluster %q: %v. %s",
			paramSecurityFlavor, params.SecurityFlavor, params.Server, err,
			nfsSettingsStalenessNote)
	}

	if err := s.checkProtocolSupported(ctx, client, params); err != nil {
		if u := unavailableIfNoAnswer(params, err); u != nil {
			return nil, u
		}
		return nil, status.Errorf(codes.InvalidArgument,
			"cluster %q cannot serve protocol %q: %v. Enable NFSv4 on the cluster "+
				"(PATCH /v2/nfs/settings), or set protocol nfs3 on the StorageClass. "+
				"%s",
			params.Server, params.Protocol, err, nfsSettingsStalenessNote)
	}

	warnIfMultiWriterOnNFS3(ctx, params, name, caps)

	// Resolve the tenant here, before anything is created, and before the
	// restore branch so it covers both.
	//
	// This used to happen inside ensureExport, which runs after the directory
	// and the quota exist: a class naming a tenant that does not resolve, or
	// one with NFS switched off, failed the provision with a directory already
	// on the array for the next retry to adopt. Nothing about either refusal
	// needs the volume to exist.
	tenantID, err := s.resolveTenantID(ctx, client, params.TenantID)
	if err != nil {
		return nil, tenantResolutionStatus(err)
	}

	// And the other half of the same question: the tenant has to be reachable
	// from the address this class mounts from. An export in the right tenant,
	// served on an address that tenant does not answer on, Binds and then
	// never mounts (SIM-08) -- see checkServerServesTenant for exactly what
	// this refuses and what it lets through.
	//
	// FailedPrecondition, not InvalidArgument: every value in the class is
	// legal and the class is well-formed. What is wrong is the cluster's
	// network layout relative to it, which is a fact about the environment.
	if err := s.checkServerServesTenant(ctx, client, params.Server, tenantID); err != nil {
		if u := unavailableIfNoAnswer(params, err); u != nil {
			return nil, u
		}
		return nil, status.Errorf(codes.FailedPrecondition, "%s: %v", paramServer, err)
	}

	// A class that names no tenant is only safe on a cluster with one. On a
	// cluster with more, the export create is refused -- and that
	// happens in ensureExport, after the directory and quota exist. Asking
	// here refuses the class before anything is created.
	if err := s.checkTenantNotRequired(ctx, client, params); err != nil {
		if u := unavailableIfNoAnswer(params, err); u != nil {
			return nil, u
		}
		return nil, status.Errorf(codes.FailedPrecondition, "%s: %v", paramTenantID, err)
	}

	// Before creating anything, and before the restore branch below, so it
	// covers both: a restored shared volume is as unmountable as a fresh one
	// if no export reaches it, and the restore path stages and renames under
	// the same store root.
	if err := s.checkSharedExportCoverage(ctx, client, params, name); err != nil {
		return nil, err
	}

	// The domain directory must exist before a volume can be created inside
	// it: the cluster's create-entry API makes one level at a time.
	if err := s.ensureADDomainDirectory(ctx, client, params); err != nil {
		if u := unavailableIfNoAnswer(params, err); u != nil {
			return nil, u
		}
		if qumulo.IsNotFound(err) {
			return nil, storeRootMissingError(params.StoreRealPath, params.Server)
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	if params.ADDomain != "" {
		slog.InfoContext(ctx, "createvolume: provisioning under a domain namespace",
			"volume", name, "adDomain", params.ADDomain, "path", params.realPathFor(name))
	}

	resp, err := s.provisionVolume(ctx, client, params, name, capacity, sourceSnapshot, tenantID)
	if err == nil {
		// Once the volume is made, and never in its way: the sweep for
		// abandoned restore staging directories under this store root, at
		// most every 10 minutes, after this call has answered.
		s.maybeSweepRestoreStaging(ctx, client, params)
	}
	return resp, err
}

// provisionVolume is CreateVolume once the request is validated and every
// preflight has passed: a restore from sourceSnapshot when it is set,
// otherwise a fresh directory, quota, export and policy.
func (s *ControllerServer) provisionVolume(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
	name string,
	capacity uint64,
	sourceSnapshot *snapshotID,
	tenantID int,
) (*csi.CreateVolumeResponse, error) {
	if sourceSnapshot != nil {
		return s.createVolumeFromSnapshot(ctx, client, params, name, capacity, *sourceSnapshot, tenantID)
	}

	// Held until this call answers, rollback included: a second call for
	// the name waits here, then finds the volume finished or gone, never
	// half made.
	release, err := s.createLocks.acquire(ctx, name)
	if err != nil {
		return nil, err
	}
	defer release()

	created, err := client.CreateDirectory(ctx, params.realStoreRoot(), name)
	if err != nil {
		if qumulo.IsNotFound(err) {
			return nil, storeRootMissingError(params.realStoreRoot(), params.Server)
		}
		// No answer: the POST may or may not have landed, and asking the
		// same dead endpoint whether it did would only wait again. The
		// retry Unavailable earns adopts the directory if it is there.
		if u := unavailableIfNoAnswer(params, didNotAnswer("creating the volume directory", err)); u != nil {
			return nil, u
		}

		// A previous attempt may have got at least this far. Adopt the
		// existing directory if it matches what is being asked for now,
		// otherwise the CO is reusing a name for different parameters and
		// must be told.
		//
		// This branches on the directory actually being there rather than
		// on the status code: Qumulo documents only success codes, so
		// treating 409 as the sole signal would strand every volume on a
		// cluster that reports a name collision some other way.
		resp, adoptErr := s.adoptExistingVolume(ctx, client, params, name, capacity, tenantID)
		if u := unavailableIfNoAnswer(params, adoptErr); u != nil {
			return nil, u
		}
		if !errors.Is(adoptErr, errNoExistingVolume) {
			return resp, adoptErr
		}
		return nil, status.Errorf(internalUnless5xx(err), "creating directory %q: %v", params.realPathFor(name), err)
	}

	// Before the quota, because this is what makes the directory usable at all
	// and its rollback is the cheapest of the three.
	if err := s.ensureDirectoryAccess(ctx, client, params, name, created.ID, nil); err != nil {
		cctx, cancel := detachedCleanup(ctx)
		defer cancel()
		if delErr := client.StartTreeDelete(cctx, created.ID); delErr != nil {
			slog.ErrorContext(ctx, "createvolume: rollback of directory failed; it may need manual cleanup",
				"volume", name, "path", params.realPathFor(name), "fileID", created.ID, "error", delErr)
		}
		return nil, err
	}

	// The handle exists as soon as the directory does, so the quota this
	// creates can be labelled with it. CreateVolume is told a name, not a
	// handle, which is why this is set here rather than at entry.
	ctx = qumulo.WithSubject(ctx, newVolumeID(params.Server, params.APIURL, params.realPathFor(name)).String())

	if err := client.CreateQuota(ctx, created.ID, capacity); err != nil {
		// A quota already on the directory this call made was put there by
		// someone else finishing it -- another controller adopting it, or
		// this call's own POST landing with its reply lost and the retry
		// answering 409 -- and that one may already have reported the
		// volume to the CO. Rolling back would delete a directory a bound
		// PV names. Adopt it instead, which compares the size; and
		// if that cannot finish, report the failure and remove nothing.
		// A zero limit is not a quota this driver ever sets, so it is not
		// taken as one.
		if limit, qerr := client.GetQuota(ctx, created.ID); qerr == nil && limit > 0 {
			slog.InfoContext(ctx, "createvolume: the quota create failed but the directory already has a quota; "+
				"adopting it rather than rolling back", "volume", name, "path", params.realPathFor(name), "error", err)
			resp, adoptErr := s.adoptExistingVolume(ctx, client, params, name, capacity, tenantID)
			if !errors.Is(adoptErr, errNoExistingVolume) {
				return resp, adoptErr
			}
			return nil, status.Errorf(internalUnless5xx(err), "setting quota on %q: %v", params.realPathFor(name), err)
		}

		// Roll back the directory, so the retry starts clean. A rollback that
		// never runs -- the process killed here -- leaves an empty directory
		// with no quota, which adoptVolume recognises and finishes.
		cctx, cancel := detachedCleanup(ctx)
		defer cancel()
		if delErr := client.StartTreeDelete(cctx, created.ID); delErr != nil {
			slog.ErrorContext(ctx, "createvolume: rollback of directory failed; it may need manual cleanup",
				"volume", name, "path", params.realPathFor(name), "fileID", created.ID, "error", delErr)
		}
		return nil, status.Errorf(internalUnless5xx(err), "setting quota on %q: %v", params.realPathFor(name), err)
	}

	// The export and the policy come last, and through the shared helper that
	// every path producing a volume calls -- see ensureVolumeSideEffects.
	if err := s.ensureVolumeSideEffects(ctx, client, params, name, created.ID, tenantID); err != nil {
		s.rollbackVolume(ctx, client, params, name, created.ID)
		return nil, err
	}

	return &csi.CreateVolumeResponse{
		Volume: newCSIVolume(params, name, created.ID, capacity),
	}, nil
}

// ensureDirectoryAccess makes a volume's directory writable by the workload
// that will mount it, and replicable.
//
// The cluster creates a directory owned by whoever the API token authenticates
// as, with mode 0755. An NFS client writes as its own uid and gid -- an export
// this driver creates maps no identity (NFS_MAP_NONE) -- so a non-root pod is
// neither the owner nor in the owning group, and every write fails with EACCES. The mount succeeds and
// the pod starts, so nothing in the provisioning path reports a problem; the
// first write is where it surfaces.
//
// The owner matters as well: the token's user is a local one, and replication
// refuses an entry owned by a local user or group with no NFS ID. So the
// directory is re-owned to the NFS owner in the same PATCH as the mode -- no
// extra call -- and in a PATCH of its own when the class leaves the mode alone.
// current is the directory as an earlier lookup found it, or nil for one this
// call just created: a new directory gets the NFS owner and group outright, an
// existing one only in place of a local owner or group, so an NFS owner a pod
// or an operator set is kept.
//
// Failing the provision when the mode cannot be set is deliberate. A volume
// that exists but cannot be written to is worse than one that was never
// created: the CO reports success, and the error lands on the workload instead.
// The owner fails it too, for the same reason: a volume that can never
// replicate is found out only at the failover it was meant to survive.
func (s *ControllerServer) ensureDirectoryAccess(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
	name, fileID string,
	current *qumulo.FileInfo,
) error {
	attrs := nfsOwnership()
	if current != nil {
		attrs = localReownership(current.Owner, current.Group)
	}
	if params.SetsMode() {
		attrs.Mode = params.MountPermissions
	}
	if attrs.IsEmpty() {
		return nil
	}

	if err := client.SetFileAttributesByID(ctx, fileID, attrs); err != nil {
		what := "the NFS owner"
		if attrs.Mode != "" {
			what = "mode " + attrs.Mode
		}
		return status.Errorf(internalUnless5xx(err), "setting %s on %q: %v",
			what, params.realPathFor(name), err)
	}
	return nil
}

// ensureExport gives the volume its own NFS export when the class asks for
// one, and is a no-op otherwise.
//
// It is written to converge rather than to assume a clean slate: a retry whose
// predecessor created the export but lost the response must succeed, so an
// export already serving this directory is adopted instead of being an error.
// That is the same reasoning adoptExistingVolume applies to the directory.
func (s *ControllerServer) ensureExport(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
	name string,
	tenantID int,
) error {
	if !params.DedicatedExport() {
		return nil
	}

	realPath := params.realPathFor(name)
	exportPath := params.exportPathFor(name)

	existing, err := s.findExportByFSPath(ctx, client, realPath)
	if err != nil {
		return status.Errorf(internalUnless5xx(err), "listing exports for volume %q: %v", name, err)
	}
	if existing != nil {
		// A retry asking for different access scoping is AlreadyExists, for
		// the same reason a retry asking for a different size is: the volume
		// on the array is not the volume this request describes, and the
		// restrictions live on the export, which is never rewritten in
		// place. Adopting it regardless returned a bound PV whose
		// allowedClients, securityFlavor and tenantID were the first
		// request's, with nothing anywhere saying so -- a class edited to
		// narrow access looked applied and was not.
		if why := exportScopingDiffers(*existing, params, name, tenantID); why != "" {
			return status.Errorf(codes.AlreadyExists,
				"volume %q already exists with different export restrictions: %s. The export serving it "+
					"is not rewritten in place, so delete the volume and provision it again through the "+
					"class as it now stands, or change the class back",
				name, why)
		}
		slog.InfoContext(ctx, "createvolume: adopting the export already serving this volume",
			"volume", name, "exportID", existing.ID, "exportPath", existing.ExportPath)
		return nil
	}

	// tenantID was resolved and checked in CreateVolume's preflight, before
	// anything was created. It is passed in rather than resolved again both to
	// save the round trip and so that the refusals it can raise cannot happen
	// here, with a directory already on the array.
	logExportScoping(ctx, name, tenantID, params)

	// The controller half of securityFlavor: the export *demands* the flavour,
	// so a client offering something weaker is refused by the cluster rather
	// than silently negotiated down to sec=sys. The node adds the matching
	// sec= option from the volume context below.
	authMode := exportAuthModes[params.SecurityFlavor]
	if isKerberos(params.SecurityFlavor) {
		slog.InfoContext(ctx, "createvolume: creating a Kerberos-required export",
			"volume", name, "server", params.Server,
			"securityFlavor", params.SecurityFlavor, "authenticationMode", authMode)
	}

	created, err := client.CreateExport(ctx, qumulo.ExportSpec{
		ExportPath:         exportPath,
		FSPath:             realPath,
		Description:        exportDescriptionFor(name),
		TenantID:           tenantID,
		AllowedClients:     params.AllowedClients,
		AuthenticationMode: authMode,
	})
	if err == nil {
		rememberExport(client, realPath, created.ID)
		return nil
	}
	// A tenant the cluster does not have is a StorageClass error, not a
	// transient one: returning Internal would have the provisioner retry a
	// request that can never succeed.
	if qumulo.IsTenantNotFound(err) {
		forgetTenants(client) // the held listing disagrees with the array
		return status.Errorf(codes.InvalidArgument,
			"%s %q: the cluster has no such tenant: %v", paramTenantID, params.TenantID, err)
	}

	// The class names no tenant and the cluster has more than one. The
	// preflight normally catches this before anything is created; this is the
	// fallback for when it could not read the tenant list, or a tenant was
	// added between the two calls. Without it the refusal surfaced as
	// Internal, with a message that did not name the fix.
	if qumulo.IsEmptyTenantInMultitenant(err) {
		forgetTenants(client) // a tenant added since the listing was read
		return status.Errorf(codes.FailedPrecondition,
			"%s: this cluster has more than one tenant, so the class must set %s "+
				"(a tenant id or name) to say which one the export belongs to: %v",
			paramTenantID, paramTenantID, err)
	}

	// An ancestor export is in the way. This is an NFSv4 constraint -- v4
	// presents one pseudo-filesystem tree, so the cluster refuses overlapping
	// exports -- and it arrives as a 409, so it has to be recognised before
	// the conflict branch below treats it as "already exists". No retry can
	// succeed, and the fix is on the cluster, so say so rather than leaving
	// the operator to read a message about an export that does not exist.
	if qumulo.IsExportTreeConflict(err) {
		return status.Errorf(codes.FailedPrecondition,
			"cannot create a dedicated export at %q: %v. "+
				"An existing export is an ancestor of this path, which NFSv4 does not permit. "+
				"The safe fix is provisioningModel=shared-subdirectory. "+
				"An administrator can instead remove the ancestor export, but that is NOT a free action: "+
				"every live mount beneath it gets a stale file handle immediately, and on a "+
				"cluster exporting \"/\" that export is almost certainly load-bearing. "+
				"If you must remove it, do it in this order -- create the exports the "+
				"shared-subdirectory classes need, drain the pods holding mounts beneath the "+
				"ancestor, then remove it",
			exportPath, err)
	}

	// A conflict means the export_path is taken. That is either this volume's
	// own export -- created by an attempt whose response was lost, and whose
	// fs_path lookup above raced it -- or a different export squatting on the
	// path, which the administrator has to resolve.
	if qumulo.IsConflict(err) {
		raced, listErr := s.findExportByFSPath(ctx, client, realPath)
		if listErr == nil && raced != nil {
			// The same comparison the adoption above makes: an
			// export found only by the re-list is still adopted only if it
			// scopes access as this class asks. Adopting it unchecked
			// handed a class asking for one client a volume exported to
			// 0.0.0.0/0.
			if why := exportScopingDiffers(*raced, params, name, tenantID); why != "" {
				return status.Errorf(codes.AlreadyExists,
					"volume %q already exists with different export restrictions: %s. The export serving it "+
						"is not rewritten in place, so delete the volume and provision it again through the "+
						"class as it now stands, or change the class back",
					name, why)
			}
			return nil
		}
		return status.Errorf(codes.AlreadyExists,
			"export path %q is already in use by an export that does not serve %q: %v",
			exportPath, realPath, err)
	}
	return status.Errorf(internalUnless5xx(err), "creating export %q for volume %q: %v", exportPath, name, err)
}

// exportScopingDiffers reports, in words, how an export already serving a
// volume differs from what this request's class asks for, or "" when it
// matches.
//
// Only an export this driver made for this very volume is compared. A
// replica's covering export carries another member's name and is staged from
// the source's own definition rather than from a class, and an
// administrator's export is not this driver's to judge -- comparing either
// would refuse creates that have always worked.
//
// Order does not count for allowedClients: the list is a set on the cluster,
// and a class that lists the same CIDRs in another order asks for the same
// access.
func exportScopingDiffers(existing qumulo.ExportInfo, params volumeParams, name string, tenantID int) string {
	if existing.Description != exportDescriptionFor(name) {
		return ""
	}

	var differences []string

	wantClients := slices.Clone(params.AllowedClients)
	gotClients := slices.Clone(existing.AllowedClients)
	slices.Sort(wantClients)
	slices.Sort(gotClients)
	if !slices.Equal(wantClients, gotClients) {
		differences = append(differences, fmt.Sprintf("%s is %s on the export, the class asks for %s",
			paramAllowedClients, describeClients(existing.AllowedClients), describeClients(params.AllowedClients)))
	}

	// An export created before securityFlavor existed carries an empty
	// authentication mode, which is what the cluster records for "require
	// nothing" -- the same thing sys means. Comparing the mapped values
	// rather than the flavour keeps that upgrade quiet.
	if wantMode, gotMode := exportAuthModes[params.SecurityFlavor], existing.AuthenticationMode; !sameAuthMode(wantMode, gotMode) {
		differences = append(differences, fmt.Sprintf("%s requires %s on the export, the class asks for %s",
			paramSecurityFlavor, describeAuthMode(gotMode), describeAuthMode(wantMode)))
	}

	// Zero means "the class named no tenant", which the cluster filled in
	// with its own default when the export was made. Nothing was asked for,
	// so nothing is compared.
	if tenantID != 0 && existing.TenantID != 0 && existing.TenantID != tenantID {
		differences = append(differences, fmt.Sprintf("%s is %d on the export, this request resolves to %d",
			paramTenantID, existing.TenantID, tenantID))
	}

	return strings.Join(differences, "; ")
}

// sameAuthMode compares two export authentication modes, counting an unset
// one as AuthenticationModeNone -- which is what an export created before
// this driver sent the field has, and what sys maps to.
func sameAuthMode(a, b string) bool {
	normalize := func(mode string) string {
		if mode == "" {
			return qumulo.AuthenticationModeNone
		}
		return mode
	}
	return normalize(a) == normalize(b)
}

func describeAuthMode(mode string) string {
	if mode == "" || mode == qumulo.AuthenticationModeNone {
		return "no particular authentication (sys)"
	}
	return mode
}

func describeClients(clients []string) string {
	if len(clients) == 0 {
		return "no host restriction"
	}
	return strings.Join(clients, ",")
}

// madeByThisDriver reports whether this driver created export, by the label
// every export it makes carries. A replica's export -- a group's covering
// export included -- is labelled with the name of the volume it was made
// for, which for a group is whichever member was enabled first, so only the
// prefix is compared.
func madeByThisDriver(export *qumulo.ExportInfo) bool {
	return strings.HasPrefix(export.Description, exportDescriptionFor(""))
}

// exportDescriptionFor labels an export as this driver's, since the field is
// mandatory anyway and an operator looking at the export table needs to know
// what created the entry.
func exportDescriptionFor(name string) string {
	return "csi.qumulo.com volume " + name
}

// findExportByFSPath returns the export serving fsPath, or nil if none does.
//
// The Qumulo API has no server-side filter for this, so the whole table is
// listed and matched locally.
func (s *ControllerServer) findExportByFSPath(ctx context.Context, client qumulo.ClientInterface, fsPath string) (*qumulo.ExportInfo, error) {
	return findExportByFSPath(ctx, client, fsPath)
}

// findExportByFSPath is the package-level form, shared with the replication
// service, which uses the volume's own export both to tell a dedicated-export
// volume from a shared-subdirectory one and as the definition to mirror onto
// the peer.
func findExportByFSPath(ctx context.Context, client qumulo.ClientInterface, fsPath string) (*qumulo.ExportInfo, error) {
	// The export this path had last time, read by its ID and believed only
	// if it still serves the path. The listing it replaces is the whole
	// export table -- every export on the array, read again on every create,
	// expand, Enable and replication status poll.
	getter, hinted := client.(exportGetter)
	key := ""
	if hinted {
		key = exportHintKey(getter, fsPath)
		if hint, ok := exportHints.Load(key); ok {
			if export, err := getter.GetExport(ctx, hint.(idHint).id); err == nil && export.FSPath == fsPath {
				exportHints.CompareAndSwap(key, hint, newIDHint(export.ID))
				return &export, nil
			}
			exportHints.Delete(key)
		}
	}

	// A report -- replication Info -- trusts a recent "no export here",
	// which is every poll of a shared-subdirectory volume.
	if hinted && reportLookups(ctx) {
		if seen, ok := noExportHints.Load(key); ok && time.Since(seen.(time.Time)) < noExportHintTTL {
			return nil, nil
		}
	}

	exports, err := client.ListExports(ctx)
	if err != nil {
		return nil, err
	}

	for i := range exports {
		if exports[i].FSPath == fsPath {
			if hinted {
				exportHints.Store(key, newIDHint(exports[i].ID))
				noExportHints.Delete(key)
				notePackageHintStored()
			}
			return &exports[i], nil
		}
	}
	if hinted {
		noExportHints.Store(key, time.Now())
		notePackageHintStored()
	}
	return nil, nil
}

// noExportHints maps endpoint + fs_path to when a listing last found no
// export serving that path. Only a report trusts it, for noExportHintTTL; any
// other caller lists, so an export an operator creates is seen at once by
// everything that acts.
//
// The TTL is fixed from the listing, not extended by use as the relationship
// hint's is: nothing can be read back by ID to confirm an absence, so each
// use is one more poll on the strength of the same old listing, and sliding
// it would let a report miss an operator's export indefinitely.
var noExportHints sync.Map

// noExportHintTTL is how long a report trusts "no export here".
const noExportHintTTL = 5 * time.Minute

type reportLookupsKey struct{}

// withReportLookups marks ctx as a report's: replication Info, which acts on
// nothing it reads. Such a lookup may answer from a hint a caller that acts
// would not trust -- a recent "no export here", the covering export found
// last -- and be minutes stale, which costs a report, not a volume.
func withReportLookups(ctx context.Context) context.Context {
	return context.WithValue(ctx, reportLookupsKey{}, true)
}

func reportLookups(ctx context.Context) bool {
	marked, _ := ctx.Value(reportLookupsKey{}).(bool)
	return marked
}

// exportGetter is a client that can read one export by ID and name its
// endpoint -- *qumulo.Client.
type exportGetter interface {
	GetExport(ctx context.Context, exportID string) (qumulo.ExportInfo, error)
	Endpoint() string
}

func exportHintKey(getter exportGetter, fsPath string) string {
	return getter.Endpoint() + "\x00" + fsPath
}

// rememberExport records an export this driver just created, so the volume's
// next lookup reads it by ID rather than listing the table.
func rememberExport(client qumulo.ClientInterface, fsPath, exportID string) {
	if getter, ok := client.(exportGetter); ok && exportID != "" {
		exportHints.Store(exportHintKey(getter, fsPath), newIDHint(exportID))
		notePackageHintStored()
	}
	forgetNoExport(client, fsPath)
}

// forgetVolumeHints drops every per-process hint kept for a volume this
// driver has just deleted: its export, its "no export" answer, the export
// covering it, and its snapshot policy's ID. The sweep would drop them
// later; a deleted volume's are known dead now.
func forgetVolumeHints(client qumulo.ClientInterface, realPath, volumeName string) {
	if getter, ok := client.(exportGetter); ok {
		key := exportHintKey(getter, realPath)
		exportHints.Delete(key)
		noExportHints.Delete(key)
		coveringExportHints.Delete(key)
	}
	forgetPolicyHint(client, policyNameFor(volumeName))
}

// forgetNoExport drops a "no export here" answer for fsPath, once this
// driver has created, or found, one there.
func forgetNoExport(client qumulo.ClientInterface, fsPath string) {
	if getter, ok := client.(exportGetter); ok {
		noExportHints.Delete(exportHintKey(getter, fsPath))
	}
}

// exportHints maps endpoint + fs_path to the export last found serving that
// path, or last created for it (an idHint). A hint only, checked against the array on every use: a
// wrong or stale one costs one GET and falls back to the listing. It never
// answers "no export" -- a path with none is listed every time, so an export
// an operator creates is seen at once.
var exportHints sync.Map

// checkSharedExportCoverage refuses a shared-subdirectory volume that no NFS
// export would make mountable, and is a no-op for the dedicated model.
//
// This is the check whose absence produced the worst failure shape this driver
// had: for the shared model nothing created or verified an export, so
// CreateVolume succeeded against a path no export covered, the PVC went Bound,
// and the pod then failed forever with `mount.nfs4: ... No such file or
// directory`. An operator watching Kubernetes saw a healthy volume and a
// broken workload, with nothing connecting the two.
//
// It verifies rather than creates. Creating a covering export at the class's
// store root would be a cluster-wide grant the driver was never asked to make,
// it collides with the NFSv4 ancestor rule that already blocks
// dedicated-export in some configurations (see docs/provision/dedicated-export.md), and
// DeleteVolume would then have to decide whether to remove an export other
// volumes still sit under. Refusing with instructions is the whole fix.
//
// The check runs on the export path rather than the filesystem path, because
// the export path is what the node mounts -- and following it back to a
// filesystem path is what also catches a class whose exportBasePath and
// basePath have drifted out of step, which provisions cleanly and then fails
// to mount for a different reason with the same symptom.
//
// One consequence worth stating: this runs before the adoption path, so a
// CreateVolume retry for a volume that already exists fails too if its export
// has since been removed. That is deliberate. The CSI spec wants an existing
// volume returned rather than an error, but a volume nothing can mount is not
// a volume the CO should be told is ready, and FailedPrecondition keeps the
// provisioner retrying until somebody restores the export.
func (s *ControllerServer) checkSharedExportCoverage(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
	name string,
) error {
	if params.DedicatedExport() {
		// ensureExport creates this volume's own export, so coverage is not
		// a precondition -- an ancestor export is in fact the thing that
		// makes the dedicated model fail.
		return nil
	}

	exportPath := params.exportPathFor(name)
	realPath := params.realPathFor(name)

	// The export that covered this class's last volume, read back by ID and
	// believed while it still maps this volume's export path to its real
	// path. Every shared-subdirectory CreateVolume listed the whole export
	// table for it. Unlike the hints that can only go wrong by
	// failing the read, this one could miss a deeper export added since --
	// the longest match is what the NFS server mounts, and only the listing
	// sees it -- so it is trusted for coverageHintTTL from the listing that
	// found it, and never extended by use.
	getter, hinted := client.(exportGetter)
	key := ""
	if hinted {
		key = getter.Endpoint() + "\x00" + params.exportStoreRoot()
		if v, ok := coverageHints.Load(key); ok {
			h := v.(idHint)
			if time.Since(h.at) < coverageHintTTL {
				if export, err := getter.GetExport(ctx, h.id); err == nil {
					if rel, ok := relativeUnder(export.ExportPath, exportPath); ok && path.Join(export.FSPath, rel) == realPath {
						slog.InfoContext(ctx, "createvolume: volume is covered by an existing export",
							"volume", name, "exportPath", exportPath,
							"coveringExport", export.ExportPath, "coveringExportID", export.ID)
						return nil
					}
				}
			}
			coverageHints.CompareAndDelete(key, v)
		}
	}

	exports, err := client.ListExports(ctx)
	if err != nil {
		if u := unavailableIfNoAnswer(params, didNotAnswer("listing exports", err)); u != nil {
			return u
		}
		return status.Errorf(internalUnless5xx(err),
			"listing exports on cluster %q to check that %q is mountable: %v",
			params.Server, exportPath, err)
	}

	// The longest matching export wins, which is what the NFS server itself
	// resolves to when exports nest.
	var covering *qumulo.ExportInfo
	var mapped string
	for i := range exports {
		rel, ok := relativeUnder(exports[i].ExportPath, exportPath)
		if !ok {
			continue
		}
		if covering != nil && len(exports[i].ExportPath) <= len(covering.ExportPath) {
			continue
		}
		covering = &exports[i]
		mapped = path.Join(exports[i].FSPath, rel)
	}

	if covering == nil {
		return status.Errorf(codes.FailedPrecondition,
			"no NFS export on cluster %q covers %q, so this volume would provision and then fail "+
				"to mount. Create an export whose NFS path is %q (or an ancestor of it) pointing at %q, "+
				"or set provisioningModel: dedicated-export on the StorageClass so the driver "+
				"creates a per-volume export itself",
			params.Server, exportPath, params.exportStoreRoot(), params.realStoreRoot())
	}

	// An export covers the path but resolves it somewhere else. The volume
	// would mount and then be the wrong directory, or not exist at all.
	if mapped != realPath {
		return status.Errorf(codes.FailedPrecondition,
			"export %q on cluster %q serves %q but maps it to %q, while this volume is provisioned "+
				"at %q -- the class's export path and filesystem path are out of step. "+
				"Check exportBasePath against basePath",
			covering.ExportPath, params.Server, exportPath, mapped, realPath)
	}

	if hinted {
		coverageHints.Store(key, newIDHint(covering.ID))
		notePackageHintStored()
	}
	slog.InfoContext(ctx, "createvolume: volume is covered by an existing export",
		"volume", name, "exportPath", exportPath,
		"coveringExport", covering.ExportPath, "coveringExportID", covering.ID)
	return nil
}

// coverageHints maps endpoint + a shared class's export store root to the
// export that last covered one of its volumes (an idHint), for
// checkSharedExportCoverage.
var coverageHints sync.Map

// coverageHintTTL is how long a coverage hint is trusted from the listing
// that found it: a deeper export an operator adds under the class is seen
// by the first CreateVolume after it.
const coverageHintTTL = 5 * time.Minute

// relativeUnder reports whether child is ancestor or lies beneath it, and
// returns the path from ancestor down to child.
//
// Both arguments are absolute cluster paths. Comparison is on whole path
// components rather than on string prefixes, so "/csi/volumes-old" is not
// treated as living under "/csi/volumes".
func relativeUnder(ancestor, child string) (string, bool) {
	ancestor = strings.TrimSuffix(path.Clean(ancestor), "/")
	child = path.Clean(child)

	// The root export covers everything, and trimming above left it empty.
	if ancestor == "" {
		return strings.TrimPrefix(child, "/"), true
	}
	if child == ancestor {
		return "", true
	}
	if rest, ok := strings.CutPrefix(child, ancestor+"/"); ok {
		return rest, true
	}
	return "", false
}

// internalUnless5xx is the code for a create step the array refused: Internal,
// unless the array answered 5xx, which is the array failing rather than the
// request -- Unavailable, which csi-provisioner retries without recording a
// final failure. For the creates whose repeat is harmless the client has
// already retried a 429 or 503; for a POST it does not retry (
// snapshot create among them) this is the only thing standing between a
// transient array answer and a recorded failure.
func internalUnless5xx(err error) codes.Code {
	var apiErr *qumulo.APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode >= 500 {
		return codes.Unavailable
	}
	return codes.Internal
}

// internalUnlessTransient is internalUnless5xx plus the failures that never
// reached the array at all: a timeout, a cancelled context, a refused dial.
//
// The distinction is the CO's to act on, and the two answers lead to
// different places. Unavailable is retried; Internal is recorded as a failure
// on the VolumeSnapshot or the PVC and raises an Event. An array answering
// 503 during an upgrade, or one slow enough to time out, made every snapshot
// delete and every list look like a driver fault -- and a DeleteSnapshot
// answered Internal leaves a VolumeSnapshotContent that the retry loop backs
// off on rather than clearing.
//
// Deliberately not folded into internalUnless5xx: that one is on the create
// paths, where a context ending is the RPC's own deadline and the work may
// well be half done, and reporting that as a transient failure would tell
// the CO less than it knows.
func internalUnlessTransient(err error) codes.Code {
	if code := internalUnless5xx(err); code == codes.Unavailable {
		return codes.Unavailable
	}
	if isUnreachable(err) {
		return codes.Unavailable
	}
	return codes.Internal
}

// cleanupBudget bounds a rollback that outlives its RPC. Under the
// controller's ten-second shutdown grace (main.go), so a rollback started by
// a shutdown can finish before the process exits.
const cleanupBudget = 8 * time.Second

// detachedCleanup is the context a rollback runs on: the RPC's values (its
// log subject) without its cancellation, and a deadline of its own.
//
// A rollback exists for the moment the RPC failed, and the commonest reason
// for that is the RPC's own context ending -- the provisioner disconnecting at
// a controller shutdown, or its deadline. On that context the rollback's
// first call failed at once with "context canceled", and the half-made
// volume it was there to remove stayed.
func detachedCleanup(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), cleanupBudget)
}

// refuseSnapshotOfAStaleCopy answers FailedPrecondition when the array a
// handle names holds realPath as a copy behind the live one: a live,
// read-only replication target -- the path itself or a member under a
// group's target root -- or the stale source of an unplanned
// failover, DISCONNECTED while its peer holds the promoted copy.
//
// Reading this array's tables is best effort: a token that may not read them
// snapshots as before, with a warning, rather than refusing every snapshot
// of an unreplicated volume. The stale-source check is not: a protected
// source whose peer cannot be asked is refused with the remedy
// (refuseAStaleSource), because the source end's own state does not say
// whether the peer has been promoted.
func (s *ControllerServer) refuseSnapshotOfAStaleCopy(
	ctx context.Context,
	client qumulo.ClientInterface,
	source volumeID,
	realPath string,
	secrets map[string]string,
) error {
	const rpc = "createsnapshot"
	view, ok := client.(qumulo.ReplicationClient)
	if !ok {
		return nil
	}
	rels, err := relationshipsAt(ctx, view, rpc, source.Server, realPath)
	if err != nil {
		slog.WarnContext(ctx, rpc+": could not read the replication tables to check the source is not a replica; "+
			"snapshotting anyway", "volume", source.Name, "path", realPath, "error", err)
		return nil
	}
	for _, rel := range rels {
		if rel.role == roleTarget && rel.status.TargetRootReadOnly {
			return status.Errorf(codes.FailedPrecondition,
				"%s: %q on cluster %q is the read-only replica of replication relationship %s (%s), "+
					"behind the live copy on %s; a snapshot here would capture stale data. After a failover, re-point "+
					"the PV to the promoted array (qumulo-csi-repoint) and snapshot there",
				rpc, realPath, source.Server, rel.id, rel.status.State, firstNonEmpty(rel.status.SourceAddress, "the source array"))
		}
	}
	params, err := replicationParamsFromSecrets(secrets)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%s: %v", rpc, err)
	}
	for _, rel := range rels {
		if err := s.refuseAStaleSource(ctx, rpc, source, realPath, rel, params, secrets, true); err != nil {
			return err
		}
	}
	return nil
}

// secretKeyAllowForeignSnapshotDelete in a VolumeSnapshotContent's deletion
// Secret is the consent to delete a snapshot that is not this driver's:
// one not named the way this driver's snapshots are, or one of a
// directory that is not one of its volumes.
const secretKeyAllowForeignSnapshotDelete = "allowForeignSnapshotDelete"

// DefaultSnapshotNamePrefix is csi-snapshotter's own default
// --snapshot-name-prefix, and so the driver's default -snapshot-name-prefix.
const DefaultSnapshotNamePrefix = "snapshot"

// secretFlag reports whether secrets sets key, matched without regard to
// case, to "true".
func secretFlag(secrets map[string]string, key string) bool {
	for k, v := range secrets {
		if strings.EqualFold(k, key) && strings.EqualFold(strings.TrimSpace(v), "true") {
			return true
		}
	}
	return false
}

// isDriverSnapshotName reports whether a snapshot's stored name has the
// shape this driver's snapshots have: "<id>_<prefix>-<rest>".
//
// The array keeps no other record of who took a snapshot. CreateSnapshot
// hands the CSI name to the array verbatim as the name suffix, and
// csi-snapshotter names every snapshot "<prefix>-<VolumeSnapshot UID>", with
// the prefix the chart passes to both the sidecar and this driver. A snapshot
// an administrator or a backup tool took carries a name of their choosing.
// The ID anchors the match, as in MatchesNameSuffix; the rest is not checked
// to be a UID, so an earlier build's "4242_snapshot-1" still counts.
func (s *ControllerServer) isDriverSnapshotName(snapshot qumulo.SnapshotInfo) bool {
	rest, ok := strings.CutPrefix(snapshot.Name, snapshot.ID+"_"+s.snapshotNamePrefix()+"-")
	return ok && rest != ""
}

// snapshotNamePrefix is the driver's -snapshot-name-prefix, defaulted for a
// server whose Driver was not built by NewDriver.
func (s *ControllerServer) snapshotNamePrefix() string {
	if s.driver != nil && s.driver.SnapshotNamePrefix != "" {
		return s.driver.SnapshotNamePrefix
	}
	return DefaultSnapshotNamePrefix
}

// foreignSnapshotName is why a snapshot whose name is not in this driver's
// form is not taken for one of its own, as a clause after "snapshot X".
func (s *ControllerServer) foreignSnapshotName(snapshot qumulo.SnapshotInfo) string {
	return fmt.Sprintf("is named %q, not %q as this driver names its snapshots",
		snapshot.Name, snapshot.ID+"_"+s.snapshotNamePrefix()+"-<name>")
}

// foreignSnapshot says why a snapshot is not this driver's, as a clause
// after "snapshot X", or "" when it is.
//
// Without it DeleteSnapshot deleted any snapshot a hand-written handle named.
// A snapshot is this driver's when its name is in the driver's form
// (isDriverSnapshotName) and, while its source still resolves, that source
// passes the evidence DeleteVolume uses (confirmVolumeIdentity). The source
// alone is not enough: for a directory with no export of its own the
// evidence is a quota, so a snapshot an administrator took of a quota'd
// /home/alice passed. A source that no longer resolves -- a volume deleted
// after its snapshot, the ordinary case -- leaves the name as the only
// evidence there is.
func (s *ControllerServer) foreignSnapshot(ctx context.Context, client qumulo.ClientInterface, snapshot qumulo.SnapshotInfo) (string, error) {
	resolved, err := client.ResolveFileIDs(ctx, []string{snapshot.SourceFileID})
	if err != nil {
		return "", status.Errorf(codes.Unavailable,
			"resolving the source of snapshot %s to check it is one of this driver's volumes: %v", snapshot.ID, err)
	}
	live := livePath(resolved[snapshot.SourceFileID])
	if live != "" {
		if why, err := foreignSource(ctx, client, live, snapshot.SourceFileID); err != nil || why != "" {
			return "is of " + why, err
		}
	}
	if s.isDriverSnapshotName(snapshot) {
		return "", nil
	}
	if live == "" {
		return s.foreignSnapshotName(snapshot) + ", and its source no longer exists", nil
	}
	return s.foreignSnapshotName(snapshot), nil
}

// livePath is a path from ResolveFileIDs without its trailing "/", "" for a
// file that no longer resolves. The root stays "/": trimmed, it read as "gone",
// and a snapshot of the whole filesystem passed every check.
func livePath(resolved string) string {
	if resolved == "/" {
		return "/"
	}
	return strings.TrimSuffix(resolved, "/")
}

// foreignSource says why a snapshot's source, already resolved to its live
// path, is not one of this driver's volumes, or "" when it is -- or when it
// no longer resolves (live is "") and so cannot be asked.
func foreignSource(ctx context.Context, client qumulo.ClientInterface, live, fileID string) (string, error) {
	if live == "" {
		return "", nil
	}
	if live == "/" || path.Base(live) == "" {
		return fmt.Sprintf("%q, the filesystem root", live), nil
	}
	_, err := confirmVolumeIdentity(ctx, client, volumeID{Name: path.Base(live), RealPath: live},
		resolvedVolume{RealPath: live, FileID: fileID})
	if errors.Is(err, errTreeDeleteInFlight) {
		// One of this driver's volumes, part-way through its delete.
		return "", nil
	}
	switch status.Code(err) {
	case codes.OK:
		return "", nil
	case codes.FailedPrecondition:
		return fmt.Sprintf("%q, which is not one of this driver's volumes", live), nil
	}
	return "", err
}

// promoteFinishBudget bounds what a promote does after its drain -- making the
// target writable and re-plumbing its export and quota -- when the caller set
// no deadline of its own.
const promoteFinishBudget = 30 * time.Second

// uncancelledUntilDeadline is a context with ctx's values and deadline but
// not its cancellation; with no deadline on ctx, it ends after fallback.
//
// For a section that must not stop halfway when the RPC's caller goes away
// -- a sidecar exiting at a controller shutdown cancels the context at once
// -- but must not outlive the deadline the caller is waiting to, either.
func uncancelledUntilDeadline(ctx context.Context, fallback time.Duration) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(detached, deadline)
	}
	return context.WithTimeout(detached, fallback)
}

// rollbackVolume undoes a partially created volume, best effort.
//
// Every step is logged rather than returned, because the caller is already
// failing for a more specific reason and a rollback failure must not mask it.
// What a failed rollback leaves behind is a directory the next retry adopts:
// the quota is recreated by that retry only if it is absent, so the leftovers
// converge rather than accumulating.
//
// It is for a volume that was never committed: CreateVolume's fresh-create
// path, where the directory is this call's own. A restore never rolls back
// after its rename, because by then the directory is the complete restored
// copy.
func (s *ControllerServer) rollbackVolume(ctx context.Context, client qumulo.ClientInterface, params volumeParams, name, fileID string) {
	ctx, cancel := detachedCleanup(ctx)
	defer cancel()

	// Before the directory: a policy outlives its source, so tree-deleting
	// first would leave a schedule firing against a file ID that no longer
	// resolves.
	s.deleteVolumePolicy(ctx, client, name, fileID)
	// So does an export: ensureVolumeSideEffects creates it before the
	// policy, so a policy failure left a dedicated export serving a path
	// that was then tree-deleted. Only a dedicated export: a shared
	// class's covering export is not this volume's to remove.
	if params.DedicatedExport() {
		realPath := params.realPathFor(name)
		if export, err := s.findExportByFSPath(ctx, client, realPath); err != nil {
			slog.ErrorContext(ctx, "createvolume: rollback could not list exports; one may need manual cleanup",
				"volume", name, "path", realPath, "error", err)
		} else if export != nil {
			if err := client.DeleteExport(ctx, export.ID); err != nil && !qumulo.IsNotFound(err) {
				slog.ErrorContext(ctx, "createvolume: rollback of export failed; it may need manual cleanup",
					"volume", name, "exportID", export.ID, "exportPath", export.ExportPath, "error", err)
			}
		}
	}
	if err := client.DeleteQuota(ctx, fileID); err != nil && !qumulo.IsNotFound(err) {
		slog.ErrorContext(ctx, "createvolume: rollback of quota failed; it may need manual cleanup",
			"volume", name, "fileID", fileID, "error", err)
	}
	if err := client.StartTreeDelete(ctx, fileID); err != nil && !qumulo.IsNotFound(err) {
		slog.ErrorContext(ctx, "createvolume: rollback of directory failed; it may need manual cleanup",
			"volume", name, "path", params.realPathFor(name), "fileID", fileID, "error", err)
	}
}

// clientFor builds a Qumulo API client for the cluster a single request names.
//
// This is what makes the controller stateless across clusters: the endpoint
// comes from the request's parameters, or from the volume ID, which carries the
// server it was created on. The credential comes from that request's secrets and
// from nowhere else. Nothing is shared between two requests naming different
// clusters, so no cached session can leak one cluster's token to another.
//
// There is no fallback. A process-wide client existed until the multi-cluster
// purge, serving requests whose StorageClass carried no secret reference; it was
// removed because a controller holding a standing credential for one cluster is
// both a larger target and a way for a request naming cluster A to be answered
// against cluster B. Every call now carries its own credential or fails.
//
// Failing fast is the point. A DeleteVolume that cannot authenticate must return
// an error the CO retries and an operator can see, not succeed while leaving the
// directory and its quota behind on the array -- a leak invisible from
// Kubernetes, because the PV would be gone.
//
// A client is built per call rather than cached. A TLS handshake is cheap
// beside directory creation and tree-delete, and a cache keyed on address alone
// would serve a stale token after rotation -- the failure mode being an
// operation authenticating as the wrong identity.
func (s *ControllerServer) clientFor(server, apiURL string, secrets map[string]string) (qumulo.ClientInterface, error) {
	if server == "" && apiURL == "" {
		return nil, status.Error(codes.InvalidArgument,
			"cannot determine which Qumulo cluster to contact: the request named no server. "+
				"Set the server parameter on the StorageClass")
	}

	baseURL := strings.TrimRight(strings.TrimSpace(apiURL), "/")
	if baseURL == "" {
		baseURL = "https://" + net.JoinHostPort(server, defaultAPIPort)
	}

	// The token keyed by the host this call dials, then the plain "token"
	// key: the rule the replication service already applies to its Secret
	// (replication_server.go, localToken), so the one Secret the chart README
	// shows -- a token per array, keyed by address -- serves replication,
	// expand and delete alike. Only the host actually dialled is read, never
	// the StorageClass server's key when the endpoint is another host: that
	// fallback once sent one array's token to another.
	dialled := hostOfURL(baseURL)
	token := firstNonEmpty(secrets[dialled], secrets[secretKeyToken])
	if token == "" {
		// Named explicitly rather than folded into a generic auth error: the
		// fix is a specific pair of StorageClass parameters, and for a volume
		// created before those were required this is the message that explains
		// why an operation that used to work no longer does.
		return nil, status.Errorf(codes.InvalidArgument,
			"no credential for cluster %q: the request carried neither a %q key nor a key "+
				"named %q, the host this call dials. This driver "+
				"keeps no fallback credential, so every operation must supply one. Set "+
				"csi.storage.k8s.io/provisioner-secret-name and "+
				"csi.storage.k8s.io/controller-expand-secret-name on the StorageClass, and "+
				"csi.storage.k8s.io/provisioner-deletion-secret-name for delete; the "+
				"snapshot RPCs (CreateSnapshot, DeleteSnapshot, ListSnapshots) take their "+
				"own csi.storage.k8s.io/snapshotter-secret-name and "+
				"snapshotter-list-secret-name on the VolumeSnapshotClass -- a StorageClass "+
				"secret does not reach them. A volume provisioned before those were set "+
				"needs the secret references patched onto its PersistentVolume",
			server, secretKeyToken, dialled)
	}

	return s.driver.NewClient(baseURL, token), nil
}

// errNoExistingVolume reports that adoption does not apply because there is
// no directory at the volume's path, so the caller should surface whatever
// error led it to try adopting.
var errNoExistingVolume = errors.New("no existing volume at path")

// validateVolumeName refuses a CreateVolume name that cannot safely become
// the last element of the volume's path and handle.
//
// Kubernetes names volumes pvc-<uid> and never trips this; a direct CSI caller
// can. "#" separates the handle's fields, so a name carrying one mints a
// handle this driver cannot decode, and DeleteVolume answers OK for it and
// leaves the directory, quota and export behind. "/", "." and ".." are
// worse: the name is path.Join-ed onto the class's store root, so ".." names
// the store root's parent -- a directory that exists, that the adopt path
// would take as this volume, and that DeleteVolume would then tree-delete.
func validateVolumeName(name string) error {
	switch {
	case strings.Contains(name, volumeIDSeparator):
		return fmt.Errorf("volume name %q must not contain %q, which separates the fields of a volume "+
			"handle: the volume would get a handle this driver cannot decode, and could never be "+
			"deleted or expanded", name, volumeIDSeparator)
	case strings.ContainsAny(name, "/\x00"):
		return fmt.Errorf("volume name %q must be a single path element: it becomes the volume's "+
			"directory under the class's store path", name)
	case name == "." || name == "..":
		return fmt.Errorf("volume name %q would name the class's store path or its parent, not a "+
			"directory of its own", name)
	}
	return nil
}

// arrayDidNotAnswerError marks a CreateVolume step whose array call got no
// answer at all -- a timeout, a refused connection -- as opposed to an answer
// that refused. It is raised only where the raw client error is in hand,
// because the checks' own refusals are plain errors too and isUnreachable
// cannot tell them apart once they are wrapped.
//
// The first one ends the RPC as Unavailable. Carrying on meant waiting
// out qumulo.apiTimeout again at every later step against the same dead
// endpoint, then reporting Internal, which csi-provisioner treats as final
// even though the directory POST may have landed. Unavailable is retried, and
// the retry adopts whatever the timed-out attempt left.
type arrayDidNotAnswerError struct {
	step string
	err  error
}

func (e *arrayDidNotAnswerError) Error() string {
	return fmt.Sprintf("%s: the cluster did not answer: %v", e.step, e.err)
}

func (e *arrayDidNotAnswerError) Unwrap() error { return e.err }

// arrayGaveNoAnswer reports whether a client call failed in transport: the
// HTTP round trip itself did not complete. Narrower than isUnreachable on
// purpose -- that one also counts a token file it could not read, which is
// not an array that is down (a response the client could not decode it no
// longer counts). Treating a malformed answer as an outage would turn a fail-open check
// on an older cluster into a PVC that is retried for ever.
func arrayGaveNoAnswer(err error) bool {
	var transport *url.Error
	return errors.As(err, &transport)
}

// didNotAnswer wraps err as an arrayDidNotAnswerError when the array gave no
// answer, and returns nil otherwise, so a caller can write
// `if u := didNotAnswer(step, err); u != nil { return u }`.
func didNotAnswer(step string, err error) error {
	if !arrayGaveNoAnswer(err) {
		return nil
	}
	return &arrayDidNotAnswerError{step: step, err: err}
}

// unavailableIfNoAnswer is the CreateVolume status for a step that failed
// because the array did not answer, or nil when that is not why it failed.
//
// Unavailable, retried, except where no retry can succeed: a REST endpoint
// whose name does not resolve, or whose certificate does not verify, is a
// configuration error, and csi-provisioner retried it for ever, even after
// the PVC was deleted. That is InvalidArgument, final,
// naming what to fix.
func unavailableIfNoAnswer(params volumeParams, err error) error {
	var noAnswer *arrayDidNotAnswerError
	if !errors.As(err, &noAnswer) {
		return nil
	}
	// A call cancelled by its own caller -- csi-provisioner disconnecting at
	// a controller shutdown or a lost lease -- says nothing about the array.
	// Reported as "cluster is unreachable", it sent an operator looking for
	// an array outage where there was a pod restart.
	if errors.Is(noAnswer.err, context.Canceled) {
		return status.Errorf(codes.Canceled,
			"cluster %q: the call was cancelled by its caller (the provisioner disconnected, or this controller "+
				"is shutting down) while %s; nothing says the array is down, and the retry resumes the create",
			params.Server, noAnswer.step)
	}
	endpoint := apiURLFor(params.Server, params.APIURL)
	if why, ok := misconfiguredEndpoint(noAnswer.err); ok {
		return status.Errorf(codes.InvalidArgument,
			"cluster %q: the REST API at %s cannot be reached as configured -- %s. A retry cannot fix this: "+
				"correct the StorageClass's server or apiUrl, or the controller's CA bundle: %v",
			params.Server, endpoint, why, noAnswer)
	}
	return status.Errorf(codes.Unavailable, "cluster %q (REST API %s) is unreachable: %v",
		params.Server, endpoint, noAnswer)
}

// misconfiguredEndpoint reports whether a transport failure is one that
// retrying cannot fix, and says which: the endpoint's name does not exist in
// DNS, or its TLS certificate does not verify. A DNS timeout, a refused
// connection or an I/O timeout is not: an array restarting answers like that.
//
// Behind an HTTPS proxy the controller never resolves the array's name
// itself, so an unknown or disallowed host shows up instead as the proxy
// refusing the tunnel (403) or asking for credentials (407); that is final
// too. A proxy answering 502, 503 or 504 is not.
func misconfiguredEndpoint(err error) (string, bool) {
	var refused *qumulo.ProxyRefusedError
	if errors.As(err, &refused) && refused.Final() {
		return fmt.Sprintf("the HTTPS proxy %s refused to connect to %s (%s); add the host to the "+
			"proxy's allow list, or to controller.proxy.noProxy if the controller reaches it directly",
			refused.Proxy, refused.Target, refused.Status), true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return fmt.Sprintf("the name %q does not resolve", dnsErr.Name), true
	}
	var verify *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	if errors.As(err, &verify) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostname) || errors.As(err, &invalid) {
		return "its TLS certificate does not verify", true
	}
	return "", false
}

// storeRootMissingError reports that a directory create failed because its
// parent -- the StorageClass's storeRealPath, or the ad-domain directory
// beneath it -- does not exist on the cluster at all.
//
// This is an operator precondition, not a driver fault: nothing here is
// wrong with the request, the cluster, or this code, only with a managed
// path nobody created there. Reported as FailedPrecondition rather than the
// generic Internal every other CreateDirectory failure falls back to, so an
// operator sees "create this path first" instead of chasing a driver bug.
func storeRootMissingError(missingPath string, server string) error {
	return status.Errorf(codes.FailedPrecondition,
		"managed path %q does not exist on cluster %q; create it before provisioning volumes under it",
		missingPath, server)
}

// clientForVolumeID resolves the cluster from a volume ID, for the RPCs that
// receive nothing else. The ID carries the server precisely so that delete,
// expand and validate need no lookup table.
func (s *ControllerServer) clientForVolumeID(
	parsed volumeID,
	override string,
	secrets map[string]string,
) (qumulo.ClientInterface, error) {
	return s.clientFor(parsed.Server, requestEndpoint(parsed, override), secrets)
}

// storageClassAPIURL reads a StorageClass apiUrl override out of a request's
// parameter map, validated as parseVolumeParams would validate it.
//
// Keys are folded, as everywhere a StorageClass is read; an endpoint that
// does not pass validateAPIURL is refused rather than dialled, so an http://
// one never carries the token. Two keys that fold to apiurl ("apiUrl"
// beside "APIURL") are refused, as parseVolumeParams refuses them:
// taking the first one map iteration happened to visit dialled a different
// endpoint on different calls.
func storageClassAPIURL(params map[string]string) (string, error) {
	var keys []string
	for key := range params {
		if strings.ToLower(strings.TrimSpace(key)) == paramAPIURL {
			keys = append(keys, strings.TrimSpace(key))
		}
	}
	switch len(keys) {
	case 0:
		return "", nil
	case 1:
	default:
		slices.Sort(keys)
		return "", status.Errorf(codes.InvalidArgument,
			"StorageClass parameter(s) given twice, differing only by case: %s. Parameter names are "+
				"matched case-insensitively, so only one would take effect and which one is not defined",
			strings.Join(keys, " and "))
	}
	var apiURL string
	for key, value := range params {
		if strings.TrimSpace(key) == keys[0] {
			apiURL = strings.TrimSpace(value)
		}
	}
	if apiURL == "" {
		return "", nil
	}
	if err := validateAPIURL(apiURL); err != nil {
		return "", status.Errorf(codes.InvalidArgument, "%s %q: %v", paramAPIURL, apiURL, err)
	}
	return apiURL, nil
}

// requestEndpoint is the REST endpoint an ID-only RPC dials for a volume.
//
// Parameters are absent on most ID-only RPCs; when present they may carry
// an apiUrl override for this cluster, and a request that names one is
// more current than a handle minted at provision time.
//
// Otherwise the handle's own endpoint is used. That is the whole of the
// SIM-07 fix: deriving https://<server>:8000 here is right only when the
// NFS address also serves REST, which under multitenancy it does not.
//
// Anything that records where a call went -- a snapshot ID -- must record
// this, not the handle's endpoint: with an override the two differ.
// The override is the value validateSnapshotParams read and checked, never a
// raw parameter map: an endpoint this driver has not validated must not be
// dialled, because the call that dials it carries the class's token.
func requestEndpoint(parsed volumeID, override string) string {
	if strings.TrimSpace(override) != "" {
		return override
	}
	return parsed.APIEndpoint()
}

// requireSameCluster refuses a VolumeSnapshotClass apiurl override that
// reaches a different cluster from the volume's own endpoint.
//
// The override is meant for a newer endpoint of the volume's own cluster.
// One that reaches another array snapshotted whatever that array holds at the
// same path -- in the retest, the DR copy of a volume on the other array --
// and the snapshot then could not be restored through the volume's class.
// The two endpoints' cluster UUIDs (GET /v1/node/state, which needs no
// privilege) are compared; only a proven mismatch refuses. An endpoint that
// cannot say -- the volume's own endpoint down, which is when an override is
// most likely to be in use -- is logged and let through.
func (s *ControllerServer) requireSameCluster(
	ctx context.Context,
	source volumeID,
	endpoint string,
	overrideClient qumulo.ClientInterface,
	secrets map[string]string,
) error {
	own := source.APIEndpoint()
	if sameEndpoint(endpoint, own) {
		return nil
	}
	ownClient, err := s.clientFor(source.Server, own, secrets)
	if err != nil {
		return err
	}
	overrideID, overrideErr := overrideClient.ClusterID(ctx)
	ownID, ownErr := ownClient.ClusterID(ctx)
	switch {
	case overrideErr != nil || ownErr != nil || overrideID == "" || ownID == "":
		slog.WarnContext(ctx, "createsnapshot: could not confirm that the apiurl override reaches the volume's own cluster; "+
			"taking the snapshot through it",
			"volume", source.Name, "override", endpoint, "volumeEndpoint", own,
			"overrideError", overrideErr, "volumeEndpointError", ownErr)
		return nil
	case overrideID != ownID:
		return status.Errorf(codes.InvalidArgument,
			"createsnapshot: the VolumeSnapshotClass apiurl %s reaches cluster %s, but volume %q is on cluster %s "+
				"(its endpoint %s). The override is for another endpoint of the volume's own cluster; through "+
				"another cluster the snapshot would be of whatever that cluster holds at %q",
			endpoint, overrideID, source.Name, ownID, own, source.RealPath)
	}
	return nil
}

// sameEndpoint reports whether two REST endpoints are spelled the same, as
// normalizeAPIURL spells them: case and a trailing slash do not count.
func sameEndpoint(a, b string) bool {
	return strings.EqualFold(strings.TrimSuffix(strings.TrimSpace(a), "/"), strings.TrimSuffix(strings.TrimSpace(b), "/"))
}

func (s *ControllerServer) adoptExistingVolume(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
	name string,
	capacity uint64,
	tenantID int,
) (*csi.CreateVolumeResponse, error) {
	existing, err := client.LookupFileByPath(ctx, params.realPathFor(name))
	if err != nil {
		if u := didNotAnswer("looking for a directory an earlier attempt created", err); u != nil {
			return nil, u
		}
		return nil, errNoExistingVolume
	}
	return s.adoptVolume(ctx, client, params, name, capacity, existing, tenantID)
}

// adoptVolume finishes adopting a directory the caller has already found.
//
// Split from adoptExistingVolume so the snapshot-restore path can share it.
// That path has to distinguish a missing directory from an unreadable one --
// a restore that has not committed yet must be resumed, not reported as a
// failure -- so it does its own lookup and calls in here with the result,
// rather than through the lookup above which folds every error into
// errNoExistingVolume.
func (s *ControllerServer) adoptVolume(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
	name string,
	capacity uint64,
	existing qumulo.FileInfo,
	tenantID int,
) (*csi.CreateVolumeResponse, error) {
	realPath := params.realPathFor(name)

	if !existing.IsDirectory() {
		return nil, status.Errorf(codes.AlreadyExists, "%q already exists and is not a directory", realPath)
	}

	existingCapacity, err := client.GetQuota(ctx, existing.ID)
	if err != nil && qumulo.IsNotFound(err) && existing.ChildCount != nil && *existing.ChildCount == 0 {
		// An empty directory with no quota, at exactly this volume's path:
		// this driver's own create, stopped between the directory and the
		// quota -- by a restart, an eviction or a lost lease, none of which
		// lets the rollback run. The name is the claim's own
		// pvc-<uid>, so nothing else made it, and being empty there is
		// nothing in it to protect. Finish it: the mode, the quota, then the
		// side effects below, as a fresh create would have.
		//
		// Refusing it instead -- what this did before -- stranded the claim
		// for good: every retry's create answered 409 and every adoption
		// found no quota, until someone with array access removed the
		// directory by hand.
		slog.InfoContext(ctx, "createvolume: finishing an earlier attempt that stopped before its quota",
			"volume", name, "path", realPath, "fileID", existing.ID)
		if err := s.ensureDirectoryAccess(ctx, client, params, name, existing.ID, &existing); err != nil {
			return nil, err
		}
		existingCapacity, err = capacity, nil
		if qerr := client.CreateQuota(ctx, existing.ID, capacity); qerr != nil {
			// The create this adoption is finishing set its quota first:
			// take that one, which the size check below compares.
			limit, rerr := client.GetQuota(ctx, existing.ID)
			if rerr != nil || limit == 0 {
				return nil, status.Errorf(internalUnless5xx(qerr), "setting quota on %q: %v", realPath, qerr)
			}
			existingCapacity = limit
		}
	}
	if err != nil {
		// The directory exists but its quota is unreadable, or it is missing
		// on a directory that holds something. Report the underlying create
		// failure rather than adopting a volume of unknown size or contents.
		slog.WarnContext(ctx, "createvolume: existing directory has no readable quota",
			"volume", name, "path", realPath, "childCount", existing.ChildCount, "error", err)
		return nil, errNoExistingVolume
	}

	if existingCapacity != capacity {
		return nil, status.Errorf(codes.AlreadyExists,
			"volume %q already exists with capacity %d bytes, requested %d bytes",
			name, existingCapacity, capacity)
	}

	// Re-applied rather than assumed: the attempt being retried may have died
	// between creating the directory and setting its mode, and an adopted
	// volume nobody can write to is no more useful than a fresh one.
	if err := s.ensureDirectoryAccess(ctx, client, params, name, existing.ID, &existing); err != nil {
		return nil, err
	}

	// The same reasoning, applied to the rest of them. This is what the
	// adoption path used to skip: it re-applied the mode and nothing else, so
	// a create that died after the directory but before the export returned a
	// bound PV with no export at all -- and therefore, since allowedClients
	// and tenantID live on the export, with none of the access scoping its
	// StorageClass asked for. Nothing reported a problem, because from the
	// CO's side the retry succeeded.
	if err := s.ensureVolumeSideEffects(ctx, client, params, name, existing.ID, tenantID); err != nil {
		return nil, err
	}

	return &csi.CreateVolumeResponse{
		Volume: newCSIVolume(params, name, existing.ID, existingCapacity),
	}, nil
}

// ensureVolumeSideEffects applies what a volume needs once its directory is at
// its final path with its quota: the export that makes it mountable, and the
// snapshot policy its class asked for.
//
// One function, called from every path that can produce a volume -- fresh
// create, adoption, restore, and restore adoption. That is the point of it.
// The defect it replaces was four paths applying four different subsets of
// these steps, which produced not a failure but a success that was wrong: a
// PV the CO considers bound, missing pieces nothing checks for afterwards.
//
// The directory mode is deliberately *not* in here, even though it belongs to
// the same set, because where it goes differs per path for reasons that are
// not arbitrary: before the quota on a fresh create, and before the rename on
// a restore, where a quota applied early would reject the copy still filling
// the volume. Each path applies it explicitly; all four now do.
//
// The quota is not in here either. The fresh paths create one and the adoption
// paths compare one, and collapsing those into a single call would hide the
// difference that matters -- a retry asking for a different size has to be
// AlreadyExists, not a silent resize.
//
// Both steps are idempotent by adoption already: ensureExport adopts an export
// already serving this directory, and ensurePolicy adopts a policy already
// carrying this volume's name rather than creating a second one. That is what
// makes calling them on a retry safe, and it is why this fix is mostly a
// matter of calling them at all.
func (s *ControllerServer) ensureVolumeSideEffects(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
	name string,
	fileID string,
	tenantID int,
) error {
	// The export first: it is the step that makes the volume reachable, and an
	// export pointing at a directory with no quota would be mountable and
	// unbounded -- which is why the callers apply the quota before getting
	// here.
	if err := s.ensureExport(ctx, client, params, name, tenantID); err != nil {
		return err
	}

	// The policy last of all. It is the only step that schedules future work
	// rather than changing what the volume is, so it has the cheapest
	// rollback, and a policy attached before the quota existed would schedule
	// snapshots of an unbounded directory.
	return s.ensurePolicy(ctx, client, params, name, fileID)
}

// warnIfMultiWriterOnNFS3 logs when a multi-writer volume is provisioned from
// an NFSv3 class.
//
// NFSv3 on this node image mounts with nolock, so locks are node-local
// (local_lock=all) and two pods on different nodes can each believe they hold
// the same file lock. Measured: a SQLite writer on the second node failed on
// its first statement with "disk I/O error" while flock from that node
// wrongly succeeded. See "Choosing a protocol" in the README.
//
// A log line and not a refusal, because the combination is legitimate --
// plenty of RWX workloads never take a file lock, and refusing them would
// break working deployments over a risk the driver cannot evaluate. A log line
// and not a Kubernetes Event, because the driver emits no Events at all: doing
// so needs a client-go writer in the controller, and reaching into the API
// server is the thing architectural rule 4 asks us to do only deliberately and
// opt-in (controller.inventory.enabled is the precedent). Worth raising
// separately; not worth smuggling in here.
func warnIfMultiWriterOnNFS3(
	ctx context.Context,
	params volumeParams,
	name string,
	caps []*csi.VolumeCapability,
) {
	if params.Protocol != protocolNFS3 {
		return
	}
	for _, capability := range caps {
		switch capability.GetAccessMode().GetMode() {
		case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER:
			slog.WarnContext(ctx,
				"createvolume: multi-node volume on an NFSv3 class has node-local locking only",
				"volume", name, "protocol", params.Protocol,
				"accessMode", capability.GetAccessMode().GetMode().String(),
				"consequence", "nolock means local_lock=all, so two nodes can hold the same "+
					"file lock; use protocol nfs4.1 for a workload that shares file locks")
			return
		}
	}
}

// newCSIVolume builds the Volume the CO persists. VolumeContext is the only
// channel by which the node service learns where to mount from, so the
// server and export path travel here.
func newCSIVolume(params volumeParams, name, fileID string, capacity uint64) *csi.Volume {
	// fileID is no longer part of the handle (volume_id.go): it is the
	// array-local half of a volume's identity and does not survive
	// replication. It stays a parameter because CreateVolume has it in hand
	// and the log line below is where a provision is traced from.
	id := newVolumeID(params.Server, params.APIURL, params.realPathFor(name))

	volumeContext := map[string]string{
		volumeContextServer:     params.Server,
		volumeContextExportPath: params.exportPathFor(name),
		volumeContextRealPath:   params.realPathFor(name),
	}
	// Only set when the class asked for a protocol. An empty value would be
	// indistinguishable from an absent one to the node, but writing the key at
	// all invites a future reader to treat "" as a version they must handle.
	if params.Protocol != "" {
		volumeContext[volumeContextProtocol] = params.Protocol
	}
	// Same reasoning for the domain. The node does not need it to mount --
	// exportPath already carries the full nested path -- but a volume that
	// says which domain it belongs to is far easier to reason about from
	// `kubectl get pv -o yaml`, and NodePublishVolume can log it.
	if params.ADDomain != "" {
		volumeContext[volumeContextADDomain] = params.ADDomain
	}
	// Only when it changes behaviour. sys is what NFSv4.1 negotiates unasked,
	// so writing it would add a key to every PV for no effect -- and a later
	// reader could not tell a deliberate "sys" from an unset default.
	if isKerberos(params.SecurityFlavor) {
		volumeContext[volumeContextSecurityFlavor] = params.SecurityFlavor
	}
	// The access scoping this volume was actually created with. Recorded on
	// the PV because the StorageClass that supplied it can be edited or
	// deleted, and then nothing else remembers.
	if params.TenantID != "" {
		volumeContext[volumeContextTenantID] = params.TenantID
	}
	if len(params.AllowedClients) > 0 {
		volumeContext[volumeContextAllowedClients] = strings.Join(params.AllowedClients, ",")
	}

	return &csi.Volume{
		VolumeId:      id.String(),
		CapacityBytes: int64(capacity),
		VolumeContext: volumeContext,
		// Must match what NodeGetInfo reports, or the PV's nodeAffinity names
		// a segment no node has and every pod using it stays Pending. See
		// topology.go.
		AccessibleTopology: volumeTopology(),
	}
}

// Defaults for how long DeleteVolume waits for the cluster's asynchronous
// tree deletion. The budget is short enough to stay well inside a CO's RPC
// timeout; a tree that outlasts it is finished on a later retry.
const (
	defaultTreeDeleteBudget   = 10 * time.Second
	defaultTreeDeletePollWait = 500 * time.Millisecond
)

// DeleteVolume removes a volume's quota and directory tree. Per the CSI
// spec it is idempotent: a volume that is already gone is a success.
func (s *ControllerServer) DeleteVolume(
	ctx context.Context,
	req *csi.DeleteVolumeRequest,
) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID must not be empty")
	}

	ctx = qumulo.WithSubject(ctx, req.GetVolumeId())
	parsed, err := parseVolumeID(req.GetVolumeId())

	if errors.Is(err, errVolumePathNotClean) || errors.Is(err, errCleartextEndpoint) {
		// Not an ID from another build or another driver: it is this
		// driver's form, edited. Answering OK would drop the PV without a
		// trace, and acting on it is how deleted other claims'
		// volumes, so it is refused and the PV stays for an operator.
		return nil, status.Errorf(codes.InvalidArgument, "deletevolume: %v; nothing was deleted", err)
	}
	if err != nil && inDriverHandleShape(req.GetVolumeId()) {
		// This driver's handle, in a form this build does not read: the
		// volume may well be on the array. Answering OK dropped the PV and
		// left the directory, quota and export with nothing pointing at them
		// -- after a rollback to an older controller, every PV the newer one
		// made. The PV stays Released until a build that reads it runs.
		slog.WarnContext(ctx, "deletevolume: refusing a volume ID this build cannot read", "error", err)
		return nil, status.Errorf(codes.FailedPrecondition,
			"deletevolume: volume ID %q is in this driver's form but not one this build reads (%v). It was "+
				"probably made by a newer or a retired build; nothing was deleted, and the volume may still be on "+
				"the array. Run the controller build that made it, or remove the PV and the volume's data by hand",
			req.GetVolumeId(), err)
	}
	if err != nil {
		// An ID this driver could not have issued names nothing it could
		// delete, so the volume is already absent -- which is success.
		//
		// A handle in an earlier encoding lands here too, and is treated the
		// same way. It used to be adopted -- resolved by file ID and deleted
		// as an ordinary volume -- so that an upgrade did not have to re-point
		// every pre-existing PV first. That path is gone with the encodings it
		// served: the driver has never been released, so the only handles in
		// those forms are in pre-release labs, where the volume is
		// re-provisioned rather than migrated. It is logged at Warn, not
		// Debug, because a volume this answers "already gone" for may well
		// still be on the array.
		slog.WarnContext(ctx, "deletevolume: ignoring unparseable volume ID", "error", err)
		return &csi.DeleteVolumeResponse{}, nil
	}

	// The volume ID names its own cluster, which is what lets one controller
	// delete on any cluster without holding a list of them.
	client, err := s.clientForVolumeID(parsed, "", req.GetSecrets())
	if err != nil {
		return nil, err
	}

	// Establish that the handle's path still names a directory before
	// dismantling anything, and learn this array's file ID for it. Everything
	// below acts on that ID, and a tree delete cannot be undone. One
	// resolution serves the whole RPC: the export lookup and the staging
	// sweep both need the path and both used to fetch it again.
	resolved, resolution, err := s.resolveVolumeForDelete(ctx, client, parsed)
	if err != nil {
		return nil, err
	}
	realPath := resolved.RealPath

	// Replication comes before every piece of dismantling below, the snapshot
	// policy included -- a relationship may have that policy linked, and a
	// refusal that had already deleted it would leave a volume still
	// protected but no longer taking the restore points that cross with it.
	//
	// Only when the handle still names this volume: the other resolutions
	// delete nothing here, so there is nothing to guard, and a directory this
	// ID does not name carries no relationship of this volume's.
	if resolution == volumeFound {
		if err := s.guardReplicatedDelete(ctx, client, parsed, realPath, req.GetSecrets()); err != nil {
			return nil, err
		}
	}

	// Then, before anything acts on the directory, establish that it is this
	// volume: a clean, hand-written handle can still name a store root or a
	// directory of someone else's data, and everything below is permanent.
	// After the guard, which only reads, because a protected directory's
	// refusal names the relationship and its remedy, and says more.
	var ownExport *qumulo.ExportInfo
	if resolution == volumeFound {
		ownExport, err = confirmVolumeIdentity(ctx, client, parsed, resolved)
		if errors.Is(err, errTreeDeleteInFlight) {
			// A retry after Aborted: the export, quota and policy went with
			// the first attempt, and the job is already running.
			return s.finishDelete(ctx, client, parsed, resolved)
		}
		if err != nil {
			return nil, err
		}
		// The array cannot tell a copied handle from its owner's, or a
		// volume this driver made from a folder an administrator put a
		// quota on, so Kubernetes is asked: whether another PV in use names
		// this directory, and whether the PV being released owns it.
		// Last of the checks, because they are the only ones that need the
		// Kubernetes API, and only when there is something to remove: a
		// volume already gone, or a retry of a tree delete already running,
		// succeeds without them.
		if err := s.refuseUnlessPersistentVolumesAllowDelete(ctx, parsed, req.GetVolumeId()); err != nil {
			return nil, err
		}
		// The guard above read the relationship table before the identity
		// check and the PersistentVolume list -- a Kubernetes API round trip
		// that can take seconds. An Enable landing in that window created a
		// relationship the tree delete below then ended on both arrays,
		// leaving the peer a replica of nothing (
		// TestDeleteVolumeRacingEnableNeverLeavesAnEndedRelationship). So the
		// table is read again here, after the last slow step and before the
		// first removal. It narrows the window to the few array calls between
		// here and the tree delete rather than closing it: closing it would
		// need a lock shared by the controller and replication services.
		if err := s.guardReplicatedDelete(ctx, client, parsed, realPath, req.GetSecrets()); err != nil {
			return nil, err
		}
	}

	// The snapshot policy goes next, and unconditionally -- before the
	// resolution is branched on at all. It is the one piece of cleanup keyed
	// on the volume's *name* rather than its file ID: policyNameFor matches
	// only a policy this driver created for this volume, so no path needs
	// verifying for it to be safe. Running it whatever the ID resolved to is
	// also the point, because the case where the directory is already gone is
	// exactly the case where an orphaned policy would otherwise be left
	// firing on a schedule against nothing. Volumes provisioned by a class
	// with no schedule have none, and the lookup then matches nothing.
	if resolution != volumeNotOurs {
		s.deleteVolumePolicy(ctx, client, parsed.Name, resolved.FileID)
	}

	switch resolution {
	case volumeNotOurs:
		// The ID names something that is not this volume, so this volume is
		// gone -- report success and touch nothing further. Deliberately
		// without awaiting a tree delete: whatever job that ID might have
		// belongs to whoever holds it now, and is not ours to wait on.
		return &csi.DeleteVolumeResponse{}, nil
	case volumeGone:
		// The directory is not there, so the volume is gone -- which the CSI
		// spec calls success.
		//
		// This used to wait for an earlier attempt's tree-delete job before
		// answering, because a tree delete that outlasts its budget unlinks
		// the directory and keeps going, and the handle still carried the
		// file ID that job is keyed by. A path-identified handle does not:
		// once the directory is unlinked there is nothing left to resolve an
		// ID from, so the wait is not expressible here any more.
		//
		// What that costs is bounded and is not a correctness problem. The
		// first call still waits -- it holds the ID it just resolved (below)
		// -- so this row is only reached by a retry after that call returned
		// Aborted, or by a delete of an already-deleted volume. In both the
		// PV is reclaimed while the cluster may still be freeing space, which
		// is ordinary asynchronous reclaim and what most CSI drivers do.
		// Restoring the old behaviour would need a "list tree-delete jobs"
		// call whose response shape this driver has never exercised against a
		// real cluster; that is a deliberate non-goal here rather than an
		// oversight.
		//
		// A restore whose claim was deleted before it committed has no final
		// directory either, only its staging sibling, and this is the one
		// DeleteVolume that can reclaim it; reclaimAbandonedRestore, below,
		// is reached only when the final directory exists.
		s.reclaimStagingOfGoneVolume(ctx, client, parsed)
		forgetVolumeHints(client, realPath, parsed.Name)
		return &csi.DeleteVolumeResponse{}, nil
	}

	// The export goes next, before anything the volume is made of is
	// dismantled. It is what clients mount, so removing it stops new mounts
	// reaching a directory that is about to be deleted; the reverse order
	// would leave an export briefly serving a half-deleted tree.
	if err := deleteVolumeExport(ctx, client, parsed, ownExport); err != nil {
		return nil, err
	}

	// A staging sibling beside a volume whose final directory exists is a
	// later restore attempt's leftover -- a retry that staged again after an
	// earlier one had committed. This is the only DeleteVolume for this
	// name, so it is reclaimed here. A restore whose claim was deleted before
	// it committed has no final directory, and is the volumeGone case above
	// or, with no PV at all, CreateVolume's sweep.
	s.reclaimAbandonedRestore(ctx, client, parsed, realPath)

	// Then the quota: a quota outliving its directory is an orphan
	// that blocks nothing but is invisible in the CO's view of the world.
	if err := client.DeleteQuota(ctx, resolved.FileID); err != nil && !qumulo.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "deleting quota for volume %q: %v", parsed.Name, err)
	}

	if err := client.StartTreeDelete(ctx, resolved.FileID); err != nil && !qumulo.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "deleting directory for volume %q: %v", parsed.Name, err)
	}

	return s.finishDelete(ctx, client, parsed, resolved)
}

// finishDelete is awaitTreeDelete, and once the volume is gone, the hints
// kept for it dropped.
func (s *ControllerServer) finishDelete(
	ctx context.Context, client qumulo.ClientInterface, parsed volumeID, resolved resolvedVolume,
) (*csi.DeleteVolumeResponse, error) {
	resp, err := s.awaitTreeDelete(ctx, client, parsed, resolved.FileID)
	if err == nil {
		forgetVolumeHints(client, resolved.RealPath, parsed.Name)
		// And out of the group fan-out's PersistentVolume listing, which is
		// reused for ten minutes and would otherwise keep exporting a
		// protected gauge for this volume.
		s.driver.ReplicationServer().forgetMember(parsed.String())
	}
	return resp, err
}

// How a volume ID's path resolved on the cluster.
type volumeResolution int

const (
	// volumeFound: the path exists and is a directory.
	volumeFound volumeResolution = iota
	// volumeGone: the path does not exist.
	volumeGone
	// volumeNotOurs: the path exists but is not a directory, so it is not a
	// volume this driver provisioned.
	volumeNotOurs
)

// resolveVolumeForDelete resolves a volume's file ID and confirms it still
// names that volume.
//
// DeleteVolume acts on the file ID parsed straight out of the volume ID:
// DeleteQuota and StartTreeDelete each take it directly, and a tree delete is
// not reversible. Two things make an unchecked ID dangerous rather than merely
// untidy.
//
// The first is file-ID reuse. A tree delete that outlasts its budget returns
// Aborted so the CO retries (see awaitTreeDelete); by the time it does, the
// original delete may have finished and the cluster may have handed that ID to
// a directory created since -- including another volume. The retry would then
// delete it, and the CO would be told its PV was reclaimed.
//
// The second is that a volume ID is not a capability. A statically provisioned
// PV carries a volumeHandle written by whoever authored the PV, so any file ID
// on the cluster can be named in one, and DeleteVolume would act on it.
//
// What the ID itself makes available is weaker than an identity check, and
// this is the place to be honest about it. Every volume this driver
// provisions lives at realPathFor(name), which ends in the name the ID
// carries -- with adDomain or without, since the domain nests the parent and
// not the leaf. But Name is not carried separately: it is derived as
// path.Base(RealPath) by newVolumeID (volume_id.go), so comparing the
// resolved path's last element against it compares a value with itself and
// can never fail. It proves nothing about whether this driver provisioned
// the directory.
//
// It also offers no defence against a handle that walks out of its tree. The
// path is path.Clean'd when the handle is parsed, so a `..` written into a
// hand-authored volumeHandle is resolved before it is ever seen here: the
// handle names a sibling volume, or the parent directory holding a whole
// class's volumes, and arrives looking like any other well-formed handle.
//
// This is containment the handle cannot provide: DeleteVolume is handed no
// StorageClass parameters, so storeRealPath is not available to compare
// against, and no configured list of managed paths exists any more (see
// list_volumes.go). What actually stops a copied or hand-written handle from
// deleting data its PV does not own is the PersistentVolume ownership check
// in DeleteVolume, not anything derived from the ID -- and a cluster
// with no lister (csi-sanity) has neither. Prefer the `Retain` reclaim policy
// for statically provisioned PVs.
//
// Anything other than volumeFound means there is nothing here for this volume
// ID to delete, which the CSI spec calls success. Returning an error instead
// would strand the PV: the CO retries DeleteVolume indefinitely and the PV
// object never goes away.
func (s *ControllerServer) resolveVolumeForDelete(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeID,
) (resolvedVolume, volumeResolution, error) {
	return resolveVolumePath(ctx, client, parsed, "deletevolume")
}

// resolvedVolume is a handle proved to name a real directory on one array,
// together with that array's own file ID for it.
//
// The file ID is the array-local half of a volume's identity and the handle no
// longer carries it (volume_id.go): quota, tree-delete and snapshot all key
// off it, so it is resolved once per RPC here and passed down rather than
// re-fetched. On the peer array after a failover the same path yields that
// array's own ID, which is the whole point of the path-identified handle.
type resolvedVolume struct {
	RealPath string
	FileID   string
	// Owner and Group are the directory's, as the lookup that resolved it
	// read them; Enable re-owns a local one before replicating.
	Owner, Group qumulo.Identity
}

// resolveVolumePath is resolveVolumeForDelete's body, shared with the
// replication service, which needs the same resolution before it acts on a
// handle. rpc labels the log lines.
func resolveVolumePath(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeID,
	rpc string,
) (resolvedVolume, volumeResolution, error) {
	return resolveVolumePathWith(ctx, client, parsed, rpc, unreachableEndpointError)
}

// resolveVolumePathRetryingOutages is resolveVolumePath for the RPCs that
// are always safe to retry and act on nothing a lookup failure leaves behind
// -- ControllerExpandVolume and ListSnapshots by source. A lookup the array
// answered 5xx, or one that timed out, is Unavailable there rather than
// Internal or FailedPrecondition: during an array upgrade or
// outage the first step is the one that fails, and the CO should retry it,
// not record a driver fault or send an operator to check the address.
//
// The others keep resolveVolumePath's answers on purpose: DeleteVolume's and
// ValidateVolumeCapabilities' FailedPrecondition for an endpoint that never
// answered is the documented SIM-07 / report.
func resolveVolumePathRetryingOutages(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeID,
	rpc string,
) (resolvedVolume, volumeResolution, error) {
	return resolveVolumePathWith(ctx, client, parsed, rpc, outageEndpointError)
}

// resolveVolumePathWith is resolveVolumePath with the mapping of a lookup
// failure that is neither "gone" nor a foreign 404 left to the caller.
func resolveVolumePathWith(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeID,
	rpc string,
	lookupError func(parsed volumeID, err error, format string, args ...any) error,
) (resolvedVolume, volumeResolution, error) {
	info, err := client.LookupFileByPath(ctx, parsed.RealPath)
	switch {
	case qumulo.IsFileNotFound(err):
		slog.DebugContext(ctx, rpc+": the volume's path no longer exists",
			"volume", parsed.Name, "path", parsed.RealPath)
		return resolvedVolume{}, volumeGone, nil
	case qumulo.IsNotFound(err):
		// A 404 that is not the cluster's file not-found: an unknown route,
		// or a proxy in front of the API answering for it. Taken as "gone",
		// DeleteVolume answered OK and Kubernetes dropped a PV whose
		// directory, quota and export were all still there. Nothing
		// here proves the volume is gone, so the CO is asked to retry.
		return resolvedVolume{}, volumeGone, status.Errorf(codes.Unavailable,
			"%s: resolving volume %q at %q: the REST endpoint %s answered 404 without a Qumulo file "+
				"not-found, so it may not be the cluster's API (a proxy or a changed route); nothing was "+
				"changed: %v", rpc, parsed.Name, parsed.RealPath, parsed.APIEndpoint(), err)
	case err != nil:
		// Not a "gone" answer. An unreachable or erroring cluster is not
		// permission to delete, and not proof there is nothing to delete
		// either, so this is the one path that returns an error.
		return resolvedVolume{}, volumeGone, lookupError(parsed, err,
			"resolving volume %q at %q", parsed.Name, parsed.RealPath)
	}

	// A path that names a file rather than a directory is not a volume this
	// driver provisioned: CreateVolume makes a directory, and nothing turns
	// one into a file afterwards. Warn rather than debug -- it means a
	// hand-written volumeHandle, and an operator wants to see it.
	if !info.IsDirectory() {
		slog.WarnContext(ctx, rpc+": the volume's path is not a directory; acting on nothing",
			"volume", parsed.Name, "path", parsed.RealPath, "fileID", info.ID)
		return resolvedVolume{}, volumeNotOurs, nil
	}

	return resolvedVolume{RealPath: parsed.RealPath, FileID: info.ID, Owner: info.Owner, Group: info.Group}, volumeFound, nil
}

// unreachableEndpointError reports that a volume's cluster never answered at
// the REST endpoint its handle names.
//
// FailedPrecondition rather than the Internal every other cluster failure
// falls back to, and the distinction is worth the code: Internal reads as "the
// driver broke", and a CO retries it silently forever. Nothing about the
// request is wrong here and no retry will help -- what is wrong is that the
// address this volume was provisioned with does not serve the API, which is a
// configuration fact an operator has to act on.
//
// The endpoint is named because it is the whole content of the report. It is
// derived from the handle's server when the class set no apiUrl, and a
// tenant-network address that answers NFS and not REST is exactly the case
// that produced SIM-07, so "which address did you dial" is the first question
// and this answers it unasked.
func unreachableEndpointError(parsed volumeID, err error, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if !isUnreachable(err) {
		return status.Errorf(codes.Internal, "%s: %v", msg, err)
	}
	derived := ""
	if parsed.APIURL == "" {
		derived = fmt.Sprintf(", which was derived from the volume's NFS address %q because its "+
			"StorageClass set no %s", parsed.Server, paramAPIURL)
	}
	return status.Errorf(codes.FailedPrecondition,
		"%s: the cluster's REST API at %s did not answer%s: %v",
		msg, parsed.APIEndpoint(), derived, err)
}

// outageEndpointError is unreachableEndpointError for an RPC that is always
// safe to retry: an array that answered 5xx (after the client's own retries),
// or one that did not answer before the transport timeout, is Unavailable, so
// the CO retries it once the array is back. Both messages still
// name the endpoint dialled.
//
// A refused dial or an unresolvable name keeps unreachableEndpointError's
// FailedPrecondition. Those are what an address that does not serve the API
// looks like (SIM-07), and they do not clear by waiting; a timeout is what an
// array mid-upgrade or briefly partitioned looks like, and does.
func outageEndpointError(parsed volumeID, err error, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if internalUnless5xx(err) == codes.Unavailable {
		return status.Errorf(codes.Unavailable,
			"%s: the cluster's REST API at %s is not serving requests: %v",
			msg, parsed.APIEndpoint(), err)
	}
	if isUnreachable(err) && isTransportTimeout(err) {
		return status.Errorf(codes.Unavailable,
			"%s: the cluster's REST API at %s did not answer in time: %v",
			msg, parsed.APIEndpoint(), err)
	}
	return unreachableEndpointError(parsed, err, format, args...)
}

// isTransportTimeout reports a request that ended because a deadline passed
// -- the HTTP client's own timeout or the context's -- rather than because
// the address refused it or did not resolve.
func isTransportTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// confirmVolumeIdentity establishes that a resolved directory is the volume
// its handle names, and returns the volume's own export, or nil for a
// shared-subdirectory volume.
//
// A handle is not a capability. A static or copied PersistentVolume carries
// whatever path its author wrote, and DeleteVolume would remove the export on
// that path, its quota and the whole tree beneath it. Before the only
// test was "is a directory", so a handle naming a class's store root deleted
// every volume in it, together with the operator's covering export.
//
// DeleteVolume is handed no StorageClass, so the class's store root cannot be
// compared against. What the array does record is what this driver wrote when
// it made the volume, and the three checks read exactly that:
//
//   - An export on the directory must carry this volume's description
//     (exportDescriptionFor). Every export this driver creates -- a dedicated
//     one, or a replica's on the DR array -- is labelled so; a covering
//     export, or one an operator made, is not, and is never deleted here.
//   - No other volume's export may lie below the directory. A directory that
//     holds volumes is a store root or a parent of one, not a volume.
//   - A directory with no export of its own (a shared-subdirectory volume)
//     must carry a quota, as every volume this driver creates does.
//
// A failure is FailedPrecondition, not the "already gone" OK: the directory
// is there, so the volume may be too, and answering OK would let Kubernetes
// drop a PV whose handle someone wrote by mistake. The CO retries, the PV
// stays Released, and the message says what to do.
//
// None of this tells two PVs with the same handle apart: the description is
// derived from the handle, so a verbatim copy of a Bound claim's handle
// passes. Nor does it tell a shared-subdirectory volume from any other
// directory with a quota and no export of its own -- a shared-model store
// root with a quota on it, or a folder an administrator put a quota on --
// since that needs the class's parameters, which DeleteVolume is not given,
// or a mark this driver writes at CreateVolume, which no volume an earlier
// build made carries. DeleteVolume asks Kubernetes for both afterwards
// (refuseUnlessPersistentVolumesAllowDelete): no other PV in use may name the
// directory, and the PV being released must own it -- be named after it, be
// a mirror of one that is, or carry the administrator's opt-in annotation.
func confirmVolumeIdentity(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeID,
	resolved resolvedVolume,
) (*qumulo.ExportInfo, error) {
	exports, err := client.ListExports(ctx)
	if err != nil {
		return nil, status.Errorf(internalUnlessTransient(err), "listing exports for volume %q: %v", parsed.Name, err)
	}

	want := exportDescriptionFor(parsed.Name)
	var own *qumulo.ExportInfo
	for i := range exports {
		e := &exports[i]
		if e.FSPath == resolved.RealPath {
			if e.Description != want {
				return nil, notThisVolume(parsed, resolved, fmt.Sprintf(
					"the export %q on it is not this volume's (its description is %q, not %q)",
					e.ExportPath, e.Description, want))
			}
			own = e
			continue
		}
		if _, below := relPathUnder(resolved.RealPath, e.FSPath); below &&
			strings.HasPrefix(e.Description, exportDescriptionFor("")) {
			return nil, notThisVolume(parsed, resolved, fmt.Sprintf(
				"it holds another volume's export %q at %q", e.ExportPath, e.FSPath))
		}
	}
	if own != nil {
		return own, nil
	}

	if _, err := client.GetQuota(ctx, resolved.FileID); err != nil {
		if qumulo.IsNotFound(err) {
			// An earlier attempt of this delete that answered Aborted has
			// already removed the export and the quota, and a large tree stays
			// at its path until its job ends. A job running on this file
			// ID is that attempt's: the cluster is already deleting the
			// directory, so waiting on it deletes nothing new.
			finished, jerr := client.TreeDeleteFinished(ctx, resolved.FileID)
			if jerr != nil {
				return nil, status.Errorf(codes.Internal, "checking delete of volume %q: %v", parsed.Name, jerr)
			}
			if !finished {
				return nil, errTreeDeleteInFlight
			}
			return nil, notThisVolume(parsed, resolved,
				"it has neither this volume's export nor a quota, and this driver creates every volume with a quota")
		}
		return nil, status.Errorf(codes.Internal, "reading the quota of volume %q: %v", parsed.Name, err)
	}
	return nil, nil
}

// errTreeDeleteInFlight is confirmVolumeIdentity's answer for a directory an
// earlier attempt of this DeleteVolume is still tree-deleting.
var errTreeDeleteInFlight = errors.New("the volume's tree delete is still running")

// notThisVolume is DeleteVolume's refusal for a directory that is not the
// volume its handle names.
func notThisVolume(parsed volumeID, resolved resolvedVolume, why string) error {
	return status.Errorf(codes.FailedPrecondition,
		"deletevolume: %q is not volume %q: %s; nothing was deleted. If this PersistentVolume was written or "+
			"copied by hand, set its persistentVolumeReclaimPolicy to Retain and delete it", resolved.RealPath,
		parsed.Name, why)
}

// deleteVolumeExport removes the volume's own NFS export, if it has one.
//
// DeleteVolume is handed nothing but the volume ID -- no VolumeContext, no
// StorageClass parameters -- so the provisioning model cannot be read back and
// the export path cannot be reconstructed. The export is the one
// confirmVolumeIdentity found on the volume's directory and proved to be this
// volume's by its description; nil means a shared-subdirectory volume, which
// has none. An export is never deleted on a path match alone.
func deleteVolumeExport(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeID,
	export *qumulo.ExportInfo,
) error {
	if export == nil {
		return nil
	}

	slog.InfoContext(ctx, "deletevolume: removing the volume's dedicated export",
		"volume", parsed.Name, "exportID", export.ID, "exportPath", export.ExportPath)

	if err := client.DeleteExport(ctx, export.ID); err != nil && !qumulo.IsNotFound(err) {
		return status.Errorf(codes.Internal, "deleting export %q for volume %q: %v",
			export.ExportPath, parsed.Name, err)
	}
	return nil
}

// awaitTreeDelete waits for the cluster to finish reclaiming a volume's
// directory tree.
//
// Returning as soon as the job is accepted would report success for data
// that still exists: the CO would drop the PV object, and a job that later
// failed would leave an orphaned tree nothing records. Instead this polls
// for a bounded window and, if the job is still running, returns Aborted so
// the CO retries -- a retry re-enters cleanly, since the quota is already
// gone and asking again for an in-flight delete is a no-op
// (qumulo.Client.StartTreeDelete absorbs the cluster's
// tree_delete_already_started_error).
func (s *ControllerServer) awaitTreeDelete(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeID,
	fileID string,
) (*csi.DeleteVolumeResponse, error) {
	deadline := time.Now().Add(s.treeDeleteBudget)
	// The wait doubles from treeDeletePollWait to ten times it. A fixed
	// 500 ms poll made twenty status calls per DeleteVolume of a large tree,
	// and twenty more on every retry. The last check is made at the
	// deadline, so the budget is still used whole.
	wait, maxWait := s.treeDeletePollWait, 10*s.treeDeletePollWait

	for {
		finished, err := client.TreeDeleteFinished(ctx, fileID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "checking delete of volume %q: %v", parsed.Name, err)
		}
		if finished {
			return &csi.DeleteVolumeResponse{}, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			slog.InfoContext(ctx, "deletevolume: tree delete still running, asking the CO to retry",
				"volume", parsed.Name, "fileID", fileID)
			return nil, status.Errorf(codes.Aborted,
				"delete of volume %q is still in progress", parsed.Name)
		}

		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(min(wait, remaining)):
		}
		wait = min(2*wait, maxWait)
	}
}

// ValidateVolumeCapabilities reports whether an existing volume can be used
// with the requested capabilities.
func (s *ControllerServer) ValidateVolumeCapabilities(
	ctx context.Context,
	req *csi.ValidateVolumeCapabilitiesRequest,
) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID must not be empty")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities must not be empty")
	}

	ctx = qumulo.WithSubject(ctx, req.GetVolumeId())
	parsed, err := parseVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: %v", req.GetVolumeId(), err)
	}

	override, err := storageClassAPIURL(req.GetParameters())
	if err != nil {
		return nil, err
	}

	client, err := s.clientForVolumeID(parsed, override, req.GetSecrets())
	if err != nil {
		return nil, err
	}

	// The spec requires this RPC to fail with NotFound for a volume that
	// does not exist, so confirm before answering. The handle names a path,
	// so the directory itself is the proof -- the quota lookup this used to
	// make proved the same thing one step further away, and needed a file ID
	// the handle no longer carries.
	_, resolution, err := resolveVolumePath(ctx, client, parsed, "validatevolumecapabilities")
	if err != nil {
		// Already a status, and the right one: an unreachable endpoint is
		// FailedPrecondition naming the address, as for every other RPC that
		// resolves a handle. Re-wrapping it as Internal reported an outage as
		// a driver fault.
		return nil, err
	}
	if resolution != volumeFound {
		return nil, status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
	}

	// An unsupported capability is reported by omitting Confirmed, not by
	// returning an error -- the RPC itself succeeded.
	if err := validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

// ControllerExpandVolume grows a volume's quota.
//
// Two things happen before the quota is touched, both of them measured
// failures of the version that did not (see replication_guard.go):
//
//   - The handle's file ID is resolved and the path it names is checked to
//     still be this volume. Replication does not preserve file IDs,
//     so after a failover a DR-side handle carrying the original array's ID
//     is usually a *valid* ID for some unrelated directory on the promoted
//     array -- and this RPC would have grown that directory's quota.
//   - If the volume is a replication source, the replica's quota is raised
//     first and this array's only after. Replication writes
//     past the replica's limit; clients do not, so a source grown ahead of
//     its replica is an ENOSPC waiting for the next promote.
func (s *ControllerServer) ControllerExpandVolume(
	ctx context.Context,
	req *csi.ControllerExpandVolumeRequest,
) (*csi.ControllerExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID must not be empty")
	}

	ctx = qumulo.WithSubject(ctx, req.GetVolumeId())
	parsed, err := parseVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: %v", req.GetVolumeId(), err)
	}

	// Serialised per volume, so the quota this call reads is still the quota
	// it writes against: two expands of one volume interleaved used to land
	// in whichever order they finished, leaving the array enforcing the
	// smaller size while the CO believed the larger.
	release, err := s.expandLocks.acquire(ctx, req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	defer release()

	client, err := s.clientForVolumeID(parsed, "", req.GetSecrets())
	if err != nil {
		return nil, err
	}

	wanted, err := requiredCapacity(req.GetCapacityRange())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Resolve the handle's path to THIS array's file ID before touching a
	// quota. This is the step that used to be an identity check and a
	// refusal: the handle carried a file ID, replication does not preserve
	// file IDs, and after a failover that ID usually named an unrelated
	// directory -- so the only safe answer was FailedPrecondition and a
	// manual PV repair. A path-identified handle resolves on
	// whichever array it is pointed at, so the same call now succeeds on the
	// promoted replica with no PV edit.
	resolved, resolution, err := resolveVolumePathRetryingOutages(ctx, client, parsed, "controllerexpandvolume")
	if err != nil {
		return nil, err
	}
	switch resolution {
	case volumeGone:
		// Nothing to grow, and the CO should stop asking: NotFound is what
		// tells it the volume is not there rather than transiently failing.
		return nil, status.Errorf(codes.NotFound,
			"controllerexpandvolume: %q does not exist on cluster %q, so volume %q is gone",
			parsed.RealPath, parsed.Server, parsed.Name)
	case volumeNotOurs:
		// Something is there, but it is not a directory. Not NotFound,
		// because a CO retrying forever would keep finding it;
		// FailedPrecondition, because the remedy is the operator's.
		return nil, status.Errorf(codes.FailedPrecondition,
			"controllerexpandvolume: %q on cluster %q is not a directory, so it is not volume %q; "+
				"check the PersistentVolume's volumeHandle",
			parsed.RealPath, parsed.Server, parsed.Name)
	}
	realPath := resolved.RealPath

	current, err := client.GetQuota(ctx, resolved.FileID)
	if err != nil {
		if qumulo.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
		}
		// An array answering 5xx, or not answering in time, is up or
		// coming back: the CO retries Unavailable, and an expand is always
		// safe to retry. A refused dial keeps the
		// FailedPrecondition that names the endpoint, which is the one
		// answer that tells an operator where to look.
		return nil, outageEndpointError(parsed, err, "reading quota for volume %q", parsed.Name)
	}

	// Already at or above the requested size, including the retry case
	// where a previous attempt succeeded but its response was lost.
	// NodeExpansionRequired is false throughout: an NFS mount has no
	// client-side filesystem to grow.
	if wanted <= current {
		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         int64(current),
			NodeExpansionRequired: false,
		}, nil
	}

	// The replica first, always. A failure here leaves the source at its old
	// size, which is a volume that has not grown yet -- recoverable by a
	// retry. The reverse order leaves a source the workload can fill and a
	// replica that cannot take it, which is only discovered at promote.
	if err := s.expandReplicaFirst(ctx, client, parsed, realPath, wanted, req.GetSecrets()); err != nil {
		return nil, err
	}

	if err := client.UpdateQuota(ctx, resolved.FileID, wanted); err != nil {
		return nil, status.Errorf(internalUnlessTransient(err), "updating quota for volume %q: %v", parsed.Name, err)
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         int64(wanted),
		NodeExpansionRequired: false,
	}, nil
}

// CreateSnapshot snapshots a volume's directory. Like CreateVolume it is
// idempotent on the snapshot name.
func (s *ControllerServer) CreateSnapshot(
	ctx context.Context,
	req *csi.CreateSnapshotRequest,
) (*csi.CreateSnapshotResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name must not be empty")
	}
	if req.GetSourceVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "source volume ID must not be empty")
	}

	ctx = qumulo.WithSubject(ctx, req.GetSourceVolumeId())
	source, err := parseVolumeID(req.GetSourceVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "source volume %q not found: %v", req.GetSourceVolumeId(), err)
	}

	// Checked before a client is built, so an http:// override never carries
	// the token to the address it names. An unknown key -- "apiUrl"
	// among them -- is refused here rather than ignored.
	override, err := validateSnapshotParams(req.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "createsnapshot: %v", err)
	}

	// The endpoint the snapshot is taken through is the one its ID records:
	// DeleteSnapshot and a restore read the ID alone, and must reach the same
	// array.
	endpoint := requestEndpoint(source, override)

	// Serialised per snapshot name, so two calls for one name cannot both
	// find no snapshot and both create one. The name is the CO's
	// own, so the key is per VolumeSnapshot.
	release, err := s.snapshotLocks.acquire(ctx, name)
	if err != nil {
		return nil, err
	}
	defer release()

	client, err := s.clientFor(source.Server, endpoint, req.GetSecrets())
	if err != nil {
		return nil, err
	}
	if err := s.requireSameCluster(ctx, source, endpoint, client, req.GetSecrets()); err != nil {
		return nil, err
	}

	// This array's file ID for the source. A snapshot is taken of a file ID
	// and records the one it was taken of, so the resolution has to happen
	// before either the match below or the CreateSnapshot call.
	resolved, resolution, err := resolveVolumePath(ctx, client, source, "createsnapshot")
	if err != nil {
		return nil, err
	}
	if resolution != volumeFound {
		return nil, status.Errorf(codes.NotFound,
			"createsnapshot: source volume %q does not exist at %q on cluster %q",
			source.Name, source.RealPath, source.Server)
	}

	// Not of a stale copy. After a DNS failover the claim's mount follows
	// the name to the promoted copy, but its handle still names the old
	// array: the read-only replica after a planned failover, or the
	// disconnected stale source after an unplanned one -- behind the live
	// copy either way. A snapshot there read ReadyToUse with no warning, and
	// a backup tool snapshotting every claim after a failover stored stale
	// data.
	if err := s.refuseSnapshotOfAStaleCopy(ctx, client, source, resolved.RealPath, req.GetSecrets()); err != nil {
		return nil, err
	}

	// A retry has to be matched against what is already there, or every
	// attempt leaves another snapshot behind.
	existing, err := s.findSnapshotByName(ctx, client, name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.SourceFileID != resolved.FileID {
			return nil, status.Errorf(codes.AlreadyExists,
				"snapshot %q already exists for a different source volume", name)
		}
		return &csi.CreateSnapshotResponse{
			Snapshot: newCSISnapshot(*existing, source.Server, endpoint, source, s.snapshotSizeBytes(ctx, client, source, resolved.FileID)),
		}, nil
	}

	// Measured before the snapshot is taken rather than after, so the figure
	// describes the tree the snapshot captured. Taking it afterwards would be
	// a different moment: the volume is live, and a workload writing between
	// the two calls would make the reported size describe data the snapshot
	// does not contain.
	sizeBytes := s.snapshotSizeBytes(ctx, client, source, resolved.FileID)

	// Taken all the same, but a name not in the driver's form is not one
	// DeleteSnapshot will recognise as this driver's: its deletion
	// needs allowForeignSnapshotDelete. Usually a snapshotter whose
	// --snapshot-name-prefix differs from this driver's.
	if rest, ok := strings.CutPrefix(name, s.snapshotNamePrefix()+"-"); !ok || rest == "" {
		slog.WarnContext(ctx, "createsnapshot: the snapshot name does not start with this driver's "+
			"snapshot name prefix, so DeleteSnapshot will treat the snapshot as foreign",
			"snapshot", name, "prefix", s.snapshotNamePrefix())
	}

	created, err := client.CreateSnapshot(ctx, resolved.FileID, name)
	if err != nil {
		// Unavailable, not Internal, when the array answered 5xx. The client
		// no longer retries the snapshot POST, so a 503 reaches here
		// now; reporting it as Internal would record a hard error on the
		// VolumeSnapshot and raise a failure Event for what is a transient
		// array answer. csi-snapshotter retries the RPC, findSnapshotByName
		// above adopts a snapshot an earlier attempt left, and the user sees
		// a slower snapshot rather than a failed one.
		return nil, status.Errorf(internalUnless5xx(err),
			"creating snapshot %q of volume %q: %v", name, source.Name, err)
	}
	rememberSnapshot(client, name, created.ID)

	return &csi.CreateSnapshotResponse{Snapshot: newCSISnapshot(created, source.Server, endpoint, source, sizeBytes)}, nil
}

// snapshotSizeBytes reports how much data the source volume holds, or 0.
//
// This is what fills VolumeSnapshot.status.restoreSize, and a zero there is
// not harmless: a backup tool sizing a restore PVC from it asks for a volume
// of zero bytes, and an operator reading the VolumeSnapshot cannot tell a
// snapshot of an empty volume from one the driver declined to measure.
//
// The source is the volume's own quota usage. A CSI volume always has a quota
// -- CreateVolume attaches one and rolls the whole volume back if it cannot --
// so the figure exists for every volume this driver provisioned, and it counts
// the whole tree because the cluster computes it from its own aggregates.
//
// Deliberately best-effort. The spec makes size_bytes optional, so a cluster
// that does not report usage, or a token without quota read, leaves it unset
// exactly as before -- while failing CreateSnapshot over a reporting field
// would turn an optional refinement into an outage of the backup path.
func (s *ControllerServer) snapshotSizeBytes(
	ctx context.Context,
	client qumulo.ClientInterface,
	source volumeID,
	fileID string,
) int64 {
	quota, err := client.GetQuotaInfo(ctx, fileID)
	if err != nil {
		slog.WarnContext(ctx, "createsnapshot: could not read the source volume's usage; "+
			"the snapshot will report no size",
			"volume", source.String(), "fileID", fileID, "error", err)
		return 0
	}
	if !quota.UsageKnown {
		slog.DebugContext(ctx, "createsnapshot: the cluster reported no usage for the source "+
			"volume; the snapshot will report no size",
			"volume", source.String(), "fileID", fileID)
		return 0
	}
	return clampToInt64(quota.Usage)
}

// findSnapshotByName looks for a snapshot this driver created under the
// given CSI snapshot name, returning nil when there is none.
//
// Matching cannot be an equality test against the requested name: Qumulo
// stores a snapshot as "<id>_<suffix>", so the stored name never equals what
// was asked for and an equality check would miss every retry, piling up a
// duplicate snapshot per attempt. It anchors on the full "<id>_<name>" form
// rather than testing the suffix alone -- see SnapshotInfo.MatchesNameSuffix.
//
// Snapshots the cluster's policy engine created are skipped outright. A
// scheduled snapshot is not the driver's to hand to the CO: doing so would
// give the CO a handle to a snapshot it never asked for, and deleting that
// VolumeSnapshot would destroy an administrator's scheduled backup.
//
// The snapshot last found or taken under the name is read back by ID first
// and believed while it still passes the same three tests; a retried
// CreateSnapshot listed every snapshot on the array to find it. It
// never answers "no snapshot": a name not seen before -- every first call --
// is listed, since a snapshot an earlier attempt took before its reply was
// lost is exactly what this lookup is for.
func (s *ControllerServer) findSnapshotByName(
	ctx context.Context,
	client qumulo.ClientInterface,
	name string,
) (*qumulo.SnapshotInfo, error) {
	adoptable := func(snapshot qumulo.SnapshotInfo) bool {
		// A snapshot the cluster is already deleting is not one to adopt.
		return !snapshot.InDelete && !snapshot.CreatedByPolicy() && snapshot.MatchesNameSuffix(name)
	}
	named, hinted := client.(endpointNamer)
	key := ""
	if hinted {
		key = named.Endpoint() + "\x00" + name
		if v, ok := snapshotHints.Load(key); ok {
			if snapshot, err := client.GetSnapshot(ctx, v.(idHint).id); err == nil && adoptable(snapshot) {
				snapshotHints.CompareAndSwap(key, v, newIDHint(snapshot.ID))
				return &snapshot, nil
			}
			snapshotHints.CompareAndDelete(key, v)
		}
	}

	snapshots, err := client.ListSnapshots(ctx)
	if err != nil {
		return nil, status.Errorf(internalUnlessTransient(err), "listing snapshots: %v", err)
	}

	var found *qumulo.SnapshotInfo
	var duplicates []string
	for _, snapshot := range snapshots {
		if !adoptable(snapshot) {
			continue
		}
		if found == nil {
			found = &snapshot
			continue
		}
		duplicates = append(duplicates, snapshot.ID)
	}
	if found == nil {
		return nil, nil
	}
	// More than one snapshot under this name means an earlier POST landed and
	// was sent again -- the retried-POST shape, from a build whose client still
	// retried the snapshot POST, or from any other source of a repeat. The
	// extras are orphans: no VolumeSnapshotContent, no expiry, no policy, so
	// nothing on either side will remove them.
	//
	// This reports and does not delete. ListSnapshots guarantees no ordering,
	// so there is no way here to tell which of two identically named
	// snapshots the live VolumeSnapshotContent references -- an automatic
	// cleanup that guessed wrong would turn wasted capacity into data loss.
	// An administrator removes the extras.
	if len(duplicates) > 0 {
		slog.WarnContext(ctx, "snapshot name matches more than one snapshot on the array; adopting one and "+
			"leaving the rest, which no VolumeSnapshotContent references and nothing expires. Remove them on "+
			"the cluster once their data is not needed",
			"name", name, "adopted", found.ID, "duplicates", duplicates)
	}
	rememberSnapshot(client, name, found.ID)
	return found, nil
}

// endpointNamer is a client that names the endpoint it talks to --
// *qumulo.Client -- which is what a hint is keyed by.
type endpointNamer interface {
	Endpoint() string
}

// snapshotHints maps endpoint + CSI snapshot name to the snapshot last found
// or taken under that name (an idHint). A hint only, read back on every use.
var snapshotHints sync.Map

// rememberSnapshot records the snapshot found or taken under name, so a
// retried CreateSnapshot reads it by ID.
func rememberSnapshot(client qumulo.ClientInterface, name, snapshotID string) {
	if named, ok := client.(endpointNamer); ok && snapshotID != "" {
		snapshotHints.Store(named.Endpoint()+"\x00"+name, newIDHint(snapshotID))
		notePackageHintStored()
	}
}

// excludePolicySnapshots drops snapshots created by the cluster's snapshot
// policy engine.
//
// The CSI spec describes ListSnapshots in terms of the snapshots on the
// storage system, and this narrows that to the ones the plugin manages. The
// alternative is worse in both directions: a retention policy makes the count
// unbounded, and every entry is a snapshot the CO could try to delete but
// never created. The RPC already excludes snapshots being deleted, so it is
// not reporting the raw cluster state in any case.
func excludePolicySnapshots(snapshots []qumulo.SnapshotInfo) []qumulo.SnapshotInfo {
	kept := make([]qumulo.SnapshotInfo, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.CreatedByPolicy() {
			continue
		}
		kept = append(kept, snapshot)
	}
	return kept
}

// newCSISnapshot converts a Qumulo snapshot into its CSI representation.
//
// ReadyToUse is always true because Qumulo snapshots are immediately usable
// once created.
//
// sizeBytes is the caller's measurement of the source volume, or 0 when it
// could not be taken -- the Qumulo API reports no size for a snapshot itself,
// so the source's usage at snapshot time is the closest honest answer. Zero is
// left unset rather than written, which the spec permits; the listing paths
// pass 0 deliberately, since sizing every entry of a listing would mean a
// quota read per snapshot on a call that is already a full table scan.
// server is the cluster the snapshot lives on, passed rather than taken from
// source: a snapshot whose source volume could not be identified still has a
// snapshot ID of its own, and taking the server from an unresolved source
// produced a handle with an empty server.
func newCSISnapshot(info qumulo.SnapshotInfo, server, apiURL string, source volumeID, sizeBytes int64) *csi.Snapshot {
	// The endpoint travels with the snapshot handle for the same reason it
	// travels with a volume handle: DeleteSnapshot and ListSnapshots are
	// handed no parameters, so this is their only record of where the
	// cluster's REST API actually answers.
	id := newSnapshotID(server, apiURL, info.ID)

	snapshot := &csi.Snapshot{
		SnapshotId:     id.String(),
		SourceVolumeId: source.String(),
		ReadyToUse:     true,
		SizeBytes:      sizeBytes,
	}
	if !info.CreatedAt.IsZero() {
		snapshot.CreationTime = timestamppb.New(info.CreatedAt)
	}
	return snapshot
}

// DeleteSnapshot removes a snapshot. A snapshot that is already gone is a
// success, per the spec.
func (s *ControllerServer) DeleteSnapshot(
	ctx context.Context,
	req *csi.DeleteSnapshotRequest,
) (*csi.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID must not be empty")
	}

	parsed, err := parseSnapshotID(req.GetSnapshotId())
	if errors.Is(err, errCleartextEndpoint) {
		return nil, status.Errorf(codes.InvalidArgument, "deletesnapshot: %v; nothing was deleted", err)
	}
	if err != nil && inDriverHandleShape(req.GetSnapshotId()) {
		// As in DeleteVolume: this driver's handle in a form this build does
		// not read, so the snapshot may still be on the array.
		slog.WarnContext(ctx, "deletesnapshot: refusing a snapshot ID this build cannot read", "error", err)
		return nil, status.Errorf(codes.FailedPrecondition,
			"deletesnapshot: snapshot ID %q is in this driver's form but not one this build reads (%v). It was "+
				"probably made by a newer or a retired build; nothing was deleted, and the snapshot may still be on "+
				"the array. Run the controller build that made it, or remove the VolumeSnapshotContent and the "+
				"snapshot by hand", req.GetSnapshotId(), err)
	}
	if err != nil {
		// Not an ID this driver issued, so nothing of its to delete.
		slog.WarnContext(ctx, "deletesnapshot: ignoring unparseable snapshot ID", "error", err)
		return &csi.DeleteSnapshotResponse{}, nil
	}

	client, err := s.clientFor(parsed.Server, parsed.APIEndpoint(), req.GetSecrets())
	if err != nil {
		return nil, err
	}

	// A scheduled snapshot is an administrator's backup, not the CO's to
	// delete. ListSnapshots and findSnapshotByName already hide them, which
	// is what keeps `kubectl delete volumesnapshot` from reaching one; a
	// direct CSI caller with a hand-built handle has no such filter in front
	// of it, so the delete checks too.
	snapshot, err := client.GetSnapshot(ctx, parsed.ID)
	switch {
	case qumulo.IsSnapshotNotFound(err):
		// Already gone, which CSI wants answered as success.
		return &csi.DeleteSnapshotResponse{}, nil
	case qumulo.IsNotFound(err):
		// A 404 without the cluster's "no such snapshot": an unknown route or
		// a proxy answering for it. Answering OK would let the CO drop a
		// VolumeSnapshotContent whose snapshot still exists.
		return nil, status.Errorf(codes.Unavailable,
			"looking up snapshot %q before deleting it: the REST endpoint %s answered 404 without a "+
				"Qumulo snapshot not-found, so it may not be the cluster's API; nothing was deleted: %v",
			req.GetSnapshotId(), parsed.APIEndpoint(), err)
	case err != nil:
		return nil, status.Errorf(internalUnlessTransient(err), "looking up snapshot %q before deleting it: %v",
			req.GetSnapshotId(), err)
	}
	if snapshot.CreatedByPolicy() {
		return nil, status.Errorf(codes.FailedPrecondition,
			"snapshot %q was taken by snapshot policy %s, not through this driver, so it is not "+
				"this driver's to delete; remove it on the cluster if it is really unwanted",
			req.GetSnapshotId(), snapshot.PolicyID)
	}
	// Nor is a snapshot this driver did not take, or one of something that
	// is not one of its volumes -- an administrator's, another cluster's,
	// one a backup tool relies on -- unless the deletion Secret says so.
	if why, err := s.foreignSnapshot(ctx, client, snapshot); err != nil {
		return nil, err
	} else if why != "" && !secretFlag(req.GetSecrets(), secretKeyAllowForeignSnapshotDelete) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"deletesnapshot: snapshot %q %s, so it is not this driver's to delete; nothing was deleted. "+
				"To delete it through Kubernetes anyway, set %s: \"true\" in the VolumeSnapshotContent's "+
				"deletion Secret", req.GetSnapshotId(), why, secretKeyAllowForeignSnapshotDelete)
	}

	if err := client.DeleteSnapshot(ctx, parsed.ID); err != nil && !qumulo.IsNotFound(err) {
		return nil, status.Errorf(internalUnlessTransient(err), "deleting snapshot %q: %v", req.GetSnapshotId(), err)
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots returns snapshots, optionally filtered by snapshot ID or
// source volume ID, with CSI's token-based paging applied on top.
func (s *ControllerServer) ListSnapshots(
	ctx context.Context,
	req *csi.ListSnapshotsRequest,
) (*csi.ListSnapshotsResponse, error) {
	// A snapshot-ID filter selects at most one entry, so it short-circuits
	// the list-and-page path entirely. The ID names its own cluster.
	if req.GetSnapshotId() != "" {
		parsed, err := parseSnapshotID(req.GetSnapshotId())
		if err != nil {
			// An ID this driver never issued matches nothing.
			return &csi.ListSnapshotsResponse{}, nil
		}
		client, err := s.clientFor(parsed.Server, parsed.APIEndpoint(), req.GetSecrets())
		if err != nil {
			return nil, err
		}
		return s.listSnapshotByID(ctx, client, req.GetSnapshotId())
	}

	// The source volume ID is not recoverable from a Qumulo snapshot alone
	// -- only its source file ID is -- so filtering compares file IDs.
	//
	// A filter is also the only way this RPC can know which cluster to ask.
	// One controller serves many clusters and is never told the full set, so
	// "every snapshot everywhere" is a question it cannot answer; inventing an
	// empty list instead would let a CO conclude that snapshots had been
	// deleted. Refusing is the honest answer.
	if req.GetSourceVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument,
			"listing snapshots requires a snapshot_id or source_volume_id filter: this "+
				"controller serves multiple clusters and cannot enumerate them")
	}

	// The filter is mandatory above, so this is always reached with a source
	// to filter by -- there is no "no filter" case left to represent.
	ctx = qumulo.WithSubject(ctx, req.GetSourceVolumeId())
	sourceFilter, err := parseVolumeID(req.GetSourceVolumeId())
	if err != nil {
		// An ID this driver never issued matches no snapshot; an empty
		// list is the spec's answer, not an error.
		return &csi.ListSnapshotsResponse{}, nil
	}

	client, err := s.clientFor(sourceFilter.Server, sourceFilter.APIEndpoint(), req.GetSecrets())
	if err != nil {
		return nil, err
	}

	// A snapshot records the file ID it was taken of, so filtering by source
	// volume means resolving that volume's path to this array's own ID first.
	// A source that no longer exists matches no snapshot, and an empty list is
	// the spec's answer rather than an error.
	resolved, resolution, err := resolveVolumePathRetryingOutages(ctx, client, sourceFilter, "listsnapshots")
	if err != nil {
		return nil, err
	}
	if resolution != volumeFound {
		return &csi.ListSnapshotsResponse{}, nil
	}

	snapshots, err := client.ListSnapshots(ctx)
	if err != nil {
		return nil, status.Errorf(internalUnlessTransient(err), "listing snapshots: %v", err)
	}
	// Before filtering and paging, so a page is never partly spent on
	// snapshots that are then dropped.
	snapshots = excludePolicySnapshots(snapshots)

	snapshots = filterBySourceFileID(snapshots, resolved.FileID)

	// Page before resolving source volume IDs, so resolution covers only the
	// page the caller asked for rather than every snapshot on the cluster.
	page, nextToken, err := pageSnapshots(snapshots, req.GetStartingToken(), req.GetMaxEntries())
	if err != nil {
		return nil, err
	}

	sources := s.sourceVolumeIDsFor(ctx, client, sourceFilter.Server, sourceFilter.APIURL, page, &sourceFilter)

	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, len(page))
	for i, snapshot := range page {
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: newCSISnapshot(snapshot, sourceFilter.Server, sourceFilter.APIURL, sources[i], 0),
		})
	}

	return &csi.ListSnapshotsResponse{Entries: entries, NextToken: nextToken}, nil
}

// filterBySourceFileID keeps only the snapshots taken of the given file.
func filterBySourceFileID(snapshots []qumulo.SnapshotInfo, fileID string) []qumulo.SnapshotInfo {
	filtered := make([]qumulo.SnapshotInfo, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.SourceFileID == fileID {
			filtered = append(filtered, snapshot)
		}
	}
	return filtered
}

// sourceVolumeIDsFor reconstructs, for each snapshot, the volume ID it was
// taken from, returning one entry per input in the same order.
//
// When the caller supplied a source filter its ID is authoritative and no
// lookup is needed. Otherwise only the source file ID is known, so the
// volume name is recovered from that file's path -- the CO matches this ID
// against its own volume handles, and a fabricated name would leave
// snapshots looking orphaned. The paths are fetched in a single batched
// call, so a page costs one round trip rather than one per snapshot.
//
// Resolution failure degrades to the file ID as the name rather than
// dropping the entry: an approximate ID beats a missing snapshot.
func (s *ControllerServer) sourceVolumeIDsFor(
	ctx context.Context,
	client qumulo.ClientInterface,
	server, apiURL string,
	snapshots []qumulo.SnapshotInfo,
	filter *volumeID,
) []volumeID {
	sources := make([]volumeID, len(snapshots))

	if filter != nil {
		for i := range sources {
			sources[i] = *filter
		}
		return sources
	}

	fileIDs := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		fileIDs = append(fileIDs, snapshot.SourceFileID)
	}

	paths, err := client.ResolveFileIDs(ctx, fileIDs)
	if err != nil {
		slog.WarnContext(ctx, "listsnapshots: could not resolve source volume names",
			"count", len(fileIDs), "error", err)
		paths = nil
	}

	for i, snapshot := range snapshots {
		// The resolved path is exactly what a handle carries now, so a
		// successful resolution reconstructs the source volume ID entirely
		// rather than approximating its name. An ID that does not resolve
		// leaves the zero volumeID, which renders as an empty
		// source_volume_id: the snapshot is still listed, and saying nothing
		// about its source beats naming a volume that cannot be looked up.
		resolvedPath := paths[snapshot.SourceFileID]
		if resolvedPath == "" || resolvedPath == "/" {
			slog.DebugContext(ctx, "listsnapshots: a snapshot's source file ID does not resolve; "+
				"reporting the snapshot with no source volume ID",
				"snapshotID", snapshot.ID, "sourceFileID", snapshot.SourceFileID)
			continue
		}
		sources[i] = newVolumeID(server, apiURL, resolvedPath)
	}
	return sources
}

// listSnapshotByID answers a ListSnapshots request filtered to one snapshot.
func (s *ControllerServer) listSnapshotByID(ctx context.Context, client qumulo.ClientInterface, id string) (*csi.ListSnapshotsResponse, error) {
	parsed, err := parseSnapshotID(id)
	if err != nil {
		// Unknown ID format: no match, empty list.
		return &csi.ListSnapshotsResponse{}, nil
	}

	snapshot, err := client.GetSnapshot(ctx, parsed.ID)
	if err != nil {
		// Two different absences, one answer. IsNotFound is the ID the
		// cluster parsed and could not find; IsMalformedSnapshotID is the ID
		// it could not parse at all, which it rejects with HTTP 400
		// decode_error rather than a 404. Both name no snapshot, and the
		// spec's answer for a snapshot_id that matches nothing is an empty
		// list -- an error here makes a CO retry a stale or foreign handle
		// forever, because no retry will make an unparseable ID parse.
		//
		// Scoped to the lookup this RPC made. decode_error is also what the
		// cluster answers for a field it could not read out of a request
		// body, so matching the class alone reported a real failure as a
		// deleted snapshot -- the one answer a backup tool must never be
		// given wrongly.
		if qumulo.IsSnapshotNotFound(err) || qumulo.IsMalformedSnapshotID(err, parsed.ID) {
			return &csi.ListSnapshotsResponse{}, nil
		}
		if qumulo.IsNotFound(err) {
			// A 404 without the cluster's "no such snapshot": an unknown
			// route, or a proxy answering for the API. An empty list tells a
			// backup tool the snapshot is gone when nothing says so.
			return nil, status.Errorf(codes.Unavailable,
				"getting snapshot %q: the REST endpoint %s answered 404 without a Qumulo snapshot not-found, "+
					"so it may not be the cluster's API (a proxy or a changed route): %v",
				id, parsed.APIEndpoint(), err)
		}
		return nil, status.Errorf(internalUnlessTransient(err), "getting snapshot %q: %v", id, err)
	}
	// The ID resolves, but to a snapshot the policy engine created rather than
	// one this plugin manages. An empty list is the same answer the CO gets
	// for any snapshot the plugin does not know about.
	if snapshot.CreatedByPolicy() {
		slog.DebugContext(ctx, "listsnapshots: ignoring a policy-created snapshot",
			"snapshotID", parsed.ID, "policyID", snapshot.PolicyID)
		return &csi.ListSnapshotsResponse{}, nil
	}

	source := s.sourceVolumeIDsFor(ctx, client, parsed.Server, parsed.APIURL, []qumulo.SnapshotInfo{snapshot}, nil)[0]

	return &csi.ListSnapshotsResponse{
		Entries: []*csi.ListSnapshotsResponse_Entry{
			{Snapshot: newCSISnapshot(snapshot, parsed.Server, parsed.APIURL, source,
				s.importedSnapshotSizeBytes(ctx, client, parsed.APIEndpoint(), snapshot))},
		},
	}, nil
}

// snapshotTreeSizeBytes is the snapshot's data as the quota engine counts it,
// or 0.
//
// This is the lookup the snapshotter makes for a pre-provisioned
// VolumeSnapshotContent -- a backup tool's import -- to fill its restoreSize,
// which the provisioner then compares with the claim. 0 means "unspecified",
// so an imported snapshot used to skip that check entirely. Only here,
// for one snapshot: measuring every entry of a full listing would cost a call
// per snapshot. Best effort, like snapshotSizeBytes: size_bytes is optional.
func snapshotTreeSizeBytes(ctx context.Context, client qumulo.ClientInterface, snapshot qumulo.SnapshotInfo) int64 {
	if snapshot.SourceFileID == "" {
		return 0
	}
	usage, err := client.GetTreeUsage(ctx, snapshot.SourceFileID, snapshot.ID)
	if err != nil {
		slog.WarnContext(ctx, "listsnapshots: could not measure the snapshot's data; reporting no size",
			"snapshotID", snapshot.ID, "sourceFileID", snapshot.SourceFileID, "error", err)
		return 0
	}
	return clampToInt64(usage.Capacity)
}

// importedSnapshotSizeBytes is the larger of the snapshot's tree totals and
// its files added up.
//
// The totals are aggregates the array settles in the background, and an
// imported snapshot taken just after a large write reported 24 KiB for
// 1.2 GB, so the provisioner let a 1 GiB claim restore it. Added up from the
// listing, the size is right at once. A walk cut short by
// listMeasureBudget, or failed, still raises the figure by what it found:
// a size can only be under-reported this way, never over.
func (s *ControllerServer) importedSnapshotSizeBytes(
	ctx context.Context, client qumulo.ClientInterface, endpoint string, snapshot qumulo.SnapshotInfo,
) int64 {
	size := snapshotTreeSizeBytes(ctx, client, snapshot)
	if snapshot.SourceFileID == "" {
		return size
	}
	data, err := s.snapshotData(ctx, client, endpoint, snapshot.ID, snapshot.SourceFileID, maxRestoreBytes, listMeasureBudget)
	if err != nil {
		slog.WarnContext(ctx, "listsnapshots: could not add up the snapshot's files; its size may be low",
			"snapshotID", snapshot.ID, "bytesCounted", data.bytes, "error", err)
	}
	if summed := clampToInt64(data.bytes); summed > size {
		slog.InfoContext(ctx, "listsnapshots: the snapshot's files add up to more than its tree totals; reporting the larger",
			"snapshotID", snapshot.ID, "treeBytes", size, "fileBytes", summed, "complete", data.complete)
		size = summed
	}
	return size
}

// pageSnapshots applies CSI's starting-token/max-entries paging to a list of
// snapshots, with the token anchored to a snapshot rather than to an offset.
//
// An offset token is only correct while the list does not change between
// pages, and a snapshot list changes constantly: a snapshot deleted before
// the second page -- by an expiring policy, by a backup tool, by the CO
// itself -- shifts every later entry up by one, and the offset then skips
// exactly as many snapshots as were removed. A CO paging a list to decide
// what still exists concluded that the skipped snapshots were gone.
//
// So the token is the ID of the first snapshot of the next page, and the
// list is ordered by ID before it is cut. Resuming looks for the first
// snapshot whose ID is at or after the token, which is the same entry when
// nothing changed, and the one after the gap when the anchor itself was
// deleted. Either way no snapshot between the pages is skipped.
//
// IDs are compared as numbers when they are numbers, which Qumulo's are, and
// as text otherwise: a text comparison would put "10" before "9" and make
// the order unstable against the cluster's own.
func pageSnapshots(
	snapshots []qumulo.SnapshotInfo,
	startingToken string,
	maxEntries int32,
) (page []qumulo.SnapshotInfo, nextToken string, err error) {
	if maxEntries < 0 {
		return nil, "", status.Errorf(codes.InvalidArgument, "max entries must not be negative, got %d", maxEntries)
	}

	ordered := slices.Clone(snapshots)
	slices.SortFunc(ordered, func(a, b qumulo.SnapshotInfo) int { return compareSnapshotIDs(a.ID, b.ID) })

	start := 0
	if startingToken != "" {
		if _, convErr := strconv.ParseUint(startingToken, 10, 64); convErr != nil {
			// Not a token this driver ever issued. Aborted, as the spec
			// requires, so the CO restarts the listing rather than treating
			// the answer as the truth about what exists.
			return nil, "", status.Errorf(codes.Aborted, "invalid starting token %q", startingToken)
		}
		start = len(ordered)
		for i, snapshot := range ordered {
			if compareSnapshotIDs(snapshot.ID, startingToken) >= 0 {
				start = i
				break
			}
		}
	}

	remaining := ordered[start:]
	if maxEntries == 0 || int(maxEntries) >= len(remaining) {
		return remaining, "", nil
	}

	return remaining[:maxEntries], remaining[maxEntries].ID, nil
}

// compareSnapshotIDs orders two snapshot IDs, numerically when both are
// numbers and lexically otherwise.
func compareSnapshotIDs(a, b string) int {
	na, aerr := strconv.ParseUint(a, 10, 64)
	nb, berr := strconv.ParseUint(b, 10, 64)
	if aerr == nil && berr == nil {
		switch {
		case na < nb:
			return -1
		case na > nb:
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(a, b)
}

// pageSlice applies CSI's starting-token/max-entries paging to any slice.
//
// The token is an offset, so the caller must present items in a stable order
// or a page boundary silently duplicates and omits entries. The spec requires
// Aborted for a token that cannot be honored and InvalidArgument for a
// negative max_entries -- two different codes for two different mistakes, and
// a CO distinguishes them: Aborted tells it to restart the listing, whereas
// InvalidArgument tells it the request itself was wrong.
func pageSlice[T any](
	items []T,
	startingToken string,
	maxEntries int32,
) (page []T, nextToken string, err error) {
	if maxEntries < 0 {
		return nil, "", status.Errorf(codes.InvalidArgument, "max entries must not be negative, got %d", maxEntries)
	}

	start := 0
	if startingToken != "" {
		parsed, convErr := strconv.Atoi(startingToken)
		if convErr != nil || parsed < 0 || parsed > len(items) {
			return nil, "", status.Errorf(codes.Aborted, "invalid starting token %q", startingToken)
		}
		start = parsed
	}

	remaining := items[start:]
	if maxEntries == 0 || int(maxEntries) >= len(remaining) {
		return remaining, "", nil
	}

	return remaining[:maxEntries], strconv.Itoa(start + int(maxEntries)), nil
}

// requiredCapacity extracts the volume size to provision.
//
// A Qumulo directory quota needs a concrete limit, so a range naming no size
// at all is rejected rather than defaulted -- inventing a size here would
// silently give the workload something other than what it asked for.
//
// A limit-only range is a size, though, and the spec says so: required_bytes
// and limit_bytes are both optional, and a request naming only a limit asks
// for a volume no larger than it. Refusing that was a refusal of a request
// the CSI spec permits, and csi-sanity sends one. The limit is used,
// which is the largest volume the request allows.
//
// A negative limit is refused rather than ignored. It used to fall through
// the "limit > 0" test, so a request with required 10 GiB and limit -1
// provisioned 10 GiB against a limit that says no size is acceptable.
func requiredCapacity(capacityRange *csi.CapacityRange) (uint64, error) {
	if capacityRange == nil {
		return 0, fmt.Errorf("capacity range is required")
	}

	required := capacityRange.GetRequiredBytes()
	limit := capacityRange.GetLimitBytes()

	if required < 0 {
		return 0, fmt.Errorf("required bytes must not be negative, got %d", required)
	}
	if limit < 0 {
		return 0, fmt.Errorf("limit bytes must not be negative, got %d", limit)
	}
	if required == 0 && limit == 0 {
		return 0, fmt.Errorf("capacity range names neither required bytes nor limit bytes; one of them is needed")
	}
	if required == 0 {
		return uint64(limit), nil
	}
	if limit > 0 && required > limit {
		return 0, fmt.Errorf("required bytes %d exceeds limit bytes %d", required, limit)
	}

	return uint64(required), nil
}

// validateVolumeCapabilities rejects capabilities this driver cannot serve.
//
// Qumulo volumes are NFS mounts, so block access is impossible; every mount
// access mode is supportable, including multi-writer, because the backing
// filesystem is shared.
func validateVolumeCapabilities(caps []*csi.VolumeCapability) error {
	for _, capability := range caps {
		if capability.GetBlock() != nil {
			return fmt.Errorf("block volumes are not supported; this driver provisions NFS mounts")
		}
		if capability.GetMount() == nil {
			return fmt.Errorf("only mount volume capabilities are supported")
		}

		mode := capability.GetAccessMode().GetMode()
		if mode == csi.VolumeCapability_AccessMode_UNKNOWN {
			return fmt.Errorf("volume access mode is required")
		}
	}
	return nil
}
