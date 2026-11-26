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
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// The two controller RPCs that name neither a cluster nor a credential.
//
// Every other controller RPC is told which cluster to act on: by the secrets a
// sidecar populated, by the parameters GetCapacity is handed, or by a volume
// ID. These two are told nothing useful -- ListVolumes carries only
// max_entries and starting_token, and ControllerGetVolume carries a volume ID
// which names the cluster but no credential for it.
//
// So they are answered by discovering clusters from the Kubernetes API, which
// is the line Rule 0 in ARCHITECTURE.md draws. That makes them opt-in behind
// controller.inventory.enabled: without it both return FailedPrecondition and
// nothing else about the driver changes. See inventory.go.
//
// Both capabilities are advertised unconditionally even so, because
// controllerCapabilities is a property of the build and not of a deployment's
// configuration -- see the comment there.
//
// Why a directory listing rather than the cluster's quota table: the quota
// table is a cluster-wide index, so answering from it means enumerating every
// quota and resolving every file ID to a path on every call, and it puts
// directories this driver never created inside the blast radius of whatever
// acts on the result. A directory listing is scoped by construction, needs no
// resolution step, and lets paging precede the per-volume capacity lookup --
// so a page costs at most max_entries quota reads rather than one per volume
// on the cluster.

// ListVolumes reports every volume this release provisioned, across every
// cluster its StorageClasses name.
func (s *ControllerServer) ListVolumes(
	ctx context.Context,
	req *csi.ListVolumesRequest,
) (*csi.ListVolumesResponse, error) {
	ctx = withSecretMemo(ctx)
	classes, unparsed, err := s.resolveManagedClasses(ctx, req.GetStartingToken() != "")
	if err != nil {
		return nil, err
	}

	pvHandles, pvRefs, err := s.persistentVolumeHandles(ctx, req.GetStartingToken() != "")
	if err != nil {
		return nil, err
	}

	candidates, uncredentialed, err := s.listCandidateVolumes(ctx, classes, pvHandles)
	if err != nil {
		return nil, err
	}
	if candidates, err = s.addVolumesOnlyAPVNames(ctx, classes, pvRefs, candidates); err != nil {
		return nil, err
	}

	// An empty listing and "nothing could be read" must not look the same to a
	// reconciler comparing PersistentVolumes against this: one means the
	// volumes are gone, the other means the answer is unusable. So if every
	// class was skipped, refuse rather than report nothing.
	if skipped := unparsed + uncredentialed; skipped > 0 && skipped == len(classes)+unparsed {
		return nil, status.Errorf(codes.FailedPrecondition,
			"ListVolumes: all %d StorageClass(es) naming this driver were skipped as "+
				"misconfigured, so no cluster could be enumerated; an empty list would be "+
				"indistinguishable from having no volumes. Each one's reason is logged by "+
				"this controller; if they name Secrets this release does not, list those in "+
				"rbac.extraSecrets or set rbac.clusterWideSecrets=true", skipped)
	}
	if len(classes) == 0 {
		slog.DebugContext(ctx, "listvolumes: no StorageClass names this driver")
		return &csi.ListVolumesResponse{}, nil
	}

	// Page before reading quotas, so a listing costs at most max_entries quota
	// reads rather than one per volume across every cluster. The sort in
	// listCandidateVolumes is what makes an offset token safe to do this with.
	page, nextToken, err := pageSlice(candidates, req.GetStartingToken(), req.GetMaxEntries())
	if err != nil {
		return nil, err
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0, len(page))
	for _, candidate := range page {
		// One subject per candidate: this RPC reads a quota per volume, and
		// they are told apart by the handle each belongs to.
		capacity, err := candidate.client.GetQuota(
			qumulo.WithSubject(ctx, candidate.volumeID()), candidate.fileID)
		if err != nil {
			if qumulo.IsNotFound(err) {
				// No quota means this driver did not provision the directory --
				// or a provision failed between creating it and setting the
				// quota. Either way the CO knows nothing about it, and
				// reporting it as a volume invites a reconciler to delete it.
				slog.DebugContext(ctx, "listvolumes: skipping a directory with no quota",
					"cluster", candidate.server, "path", candidate.realPath)
				continue
			}
			return nil, status.Errorf(codes.Internal,
				"ListVolumes: reading quota for %q on cluster %q: %v",
				candidate.realPath, candidate.server, err)
		}

		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      candidate.volumeID(),
				CapacityBytes: clampToInt64(capacity),
				VolumeContext: map[string]string{
					volumeContextServer: candidate.server,
				},
			},
			// PublishedNodeIds is deliberately empty, and this is why
			// LIST_VOLUMES_PUBLISHED_NODES is not advertised: there is no
			// ControllerPublishVolume, attachRequired is false, so no
			// VolumeAttachment objects exist, and the only other source would
			// be watching pods, which Rule 4 forbids. Fabricating node IDs
			// would be worse than omitting them.
			Status: &csi.ListVolumesResponse_VolumeStatus{},
		})
	}

	return &csi.ListVolumesResponse{Entries: entries, NextToken: nextToken}, nil
}

// persistentVolumeHandles is the set of this driver's PV handles, in the
// canonical form volumeID.String() renders, or nil when no VolumeHandleLister
// is configured. A handle that does not parse is left out: it names no
// directory a candidate could match.
//
// A listing that fails is Unavailable, not a nil set. Without it the
// deduplication below can only guess which of two handles is the PV's, and a
// wrong guess lists the PV's directory under a handle no PV has -- the volume
// a reconciler then reads as missing.
//
// refs is every PV with the StorageClass it names, when the lister can say
// (VolumeRefLister), and nil otherwise.
//
// continuing is true for a page after the first. Such a page reuses the PV
// list the walk's first page read, for up to walkReuse: every page used
// to list every PV in the cluster again, and one walk now sees one PV
// set. A first page always reads afresh.
func (s *ControllerServer) persistentVolumeHandles(ctx context.Context, continuing bool) (map[string]bool, []VolumeRef, error) {
	if s.driver.VolumeHandleLister == nil {
		return nil, nil, nil
	}
	refs, err := s.pvWalk.get(continuing, func() ([]VolumeRef, error) { return s.listVolumeRefs(ctx) })
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable,
			"ListVolumes: %v. Refusing to answer without it, because a directory two classes reach "+
				"would be listed under a handle that may not be its PV's", err)
	}
	set := make(map[string]bool, len(refs))
	for _, ref := range refs {
		if parsed, err := parseVolumeID(ref.Handle); err == nil {
			set[parsed.String()] = true
		}
	}
	return set, refs, nil
}

// walkReuse is how long the pages after a ListVolumes walk's first reuse
// what it read: its PV list and its StorageClass list.
const walkReuse = time.Minute

// walkCache holds a list a walk's first page read.
type walkCache[T any] struct {
	mu   sync.Mutex
	list []T
	at   time.Time
}

// get returns the cached list for a continuing page read within walkReuse,
// and otherwise reads with read and caches the result.
func (c *walkCache[T]) get(continuing bool, read func() ([]T, error)) ([]T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if continuing && !c.at.IsZero() && time.Since(c.at) < walkReuse {
		return c.list, nil
	}
	list, err := read()
	if err != nil {
		return nil, err
	}
	c.list, c.at = list, time.Now()
	return list, nil
}

// listVolumeRefs reads every PV of this driver with its StorageClass when
// the lister can say, and with its handle alone otherwise.
func (s *ControllerServer) listVolumeRefs(ctx context.Context) ([]VolumeRef, error) {
	var refs []VolumeRef
	var err error
	if withClass, ok := s.driver.VolumeHandleLister.(VolumeRefLister); ok {
		refs, err = withClass.ListVolumeRefs(ctx, s.driver.Name)
	} else {
		var handles []string
		handles, err = s.driver.VolumeHandleLister.ListVolumeHandles(ctx, s.driver.Name)
		for _, h := range handles {
			refs = append(refs, VolumeRef{Handle: h})
		}
	}
	return refs, err
}

// addVolumesOnlyAPVNames adds the PVs the walk did not list -- a DR copy's
// PV, whose handle is on the DR array while its storageClassName names the
// primary's class. ListVolumes walked only each class's own array, so every DR
// copy's PV was missing from the listing.
//
// The decision is per PV, not per array. A DR array that has classes of its
// own is walked too, and the walk drops replica roots, so skipping
// every PV on a walked array left out every DR copy again whenever the DR
// array had a class -- the common case. A PV whose handle the walk did not
// list is confirmed on its own array, whether or not a class walks it.
//
// Each is listed only once its array confirms the directory, reading with
// the token the PV's class's Secret keys by that array's address -- the key
// the replication Secret already uses -- or, when that key is missing and a
// class walks the array, with that class's own credential for it. A PV with
// neither is left out and logged: the class's plain token is the primary's,
// and is never sent to another array. A directory the array says is
// gone is not listed. A DR copy with no PV here stays unlisted, as
// requires: only PV handles reach this step.
func (s *ControllerServer) addVolumesOnlyAPVNames(
	ctx context.Context,
	classes []managedClass,
	refs []VolumeRef,
	found []candidateVolume,
) ([]candidateVolume, error) {
	walkedBy := make(map[string]managedClass, len(classes))
	byName := make(map[string]managedClass, len(classes))
	for _, class := range classes {
		if _, ok := walkedBy[classEndpoint(class)]; !ok {
			walkedBy[classEndpoint(class)] = class
		}
		byName[class.name] = class
	}
	listed := make(map[string]bool, len(found))
	for _, c := range found {
		listed[c.volumeID()] = true
	}

	added := false
	for _, ref := range refs {
		parsed, err := parseVolumeID(ref.Handle)
		if err != nil || listed[parsed.String()] {
			continue
		}
		endpoint := strings.ToLower(parsed.APIEndpoint())
		client, err := s.clientForUnlistedPV(ctx, ref, parsed, byName, walkedBy[endpoint])
		if err != nil {
			return nil, err
		}
		if client == nil {
			continue
		}
		info, err := client.LookupFileByPath(ctx, parsed.RealPath)
		if err != nil {
			if qumulo.IsFileNotFound(err) {
				continue
			}
			return nil, status.Errorf(codes.Unavailable,
				"ListVolumes: cannot confirm the PV %q on %s: %v. Refusing to return a partial listing",
				ref.Handle, parsed.APIEndpoint(), err)
		}
		found = append(found, candidateVolume{
			server:   parsed.Server,
			endpoint: endpoint,
			apiURL:   parsed.APIURL,
			realPath: parsed.RealPath,
			name:     parsed.Name,
			fileID:   info.ID,
			client:   client,
		})
		listed[parsed.String()] = true
		added = true
	}

	if added {
		slices.SortFunc(found, func(a, b candidateVolume) int {
			return strings.Compare(a.sortKey(), b.sortKey())
		})
	}
	return found, nil
}

// clientForUnlistedPV is the client that confirms a PV the walk did not
// list, or nil when there is none and the PV is left out.
//
// First choice is the token the PV's class's Secret keys by the PV's array
// address. When that is missing and walker -- a class that walks the PV's
// array -- is set, its client is used: that credential is already the one
// the walk read that array with. The PV's class's plain token is never used
// for another array.
func (s *ControllerServer) clientForUnlistedPV(
	ctx context.Context,
	ref VolumeRef,
	parsed volumeID,
	byName map[string]managedClass,
	walker managedClass,
) (qumulo.ClientInterface, error) {
	host := hostOfURL(parsed.APIEndpoint())
	class, known := byName[ref.StorageClass]
	if known {
		if secretRef, err := secretRefFromParams(class.raw.Parameters); err == nil {
			data, err := s.readSecretOncePerCall(ctx, secretRef)
			switch {
			// Only a transient failure fails the call. A Secret that does
			// not exist, or that this controller may not read, is permanent
			// and class-level: fall through to the walker's credential, and
			// then to leaving this one PV out, exactly as before.
			case err != nil && !IsSecretNotFound(err) && !IsSecretForbidden(err):
				return nil, status.Errorf(codes.Unavailable,
					"ListVolumes: reading Secret %s for the PV %q: %v", secretRef, ref.Handle, err)
			case err == nil:
				if token := strings.TrimSpace(string(data[host])); token != "" {
					return s.clientFor(parsed.Server, parsed.APIURL, map[string]string{secretKeyToken: token})
				}
			}
		}
	}
	if walker.name != "" {
		client, err := s.clientForClass(ctx, walker)
		if err == nil {
			return client, nil
		}
		var unreadable *secretReadError
		if errors.As(err, &unreadable) {
			return nil, status.Errorf(codes.Unavailable,
				"ListVolumes: StorageClass %q, for the PV %q: %v", walker.name, ref.Handle, err)
		}
	}
	if !known {
		// Not one of this driver's classes, on an array none walks: there is
		// no credential to read it with, as before.
		return nil, nil
	}
	slog.WarnContext(ctx, "listvolumes: a PV the walk did not list cannot be confirmed: its class's "+
		"Secret has no token for its array, and no usable class walks that array, so the PV is not listed",
		"volumeHandle", ref.Handle, "class", ref.StorageClass, "addTokenKey", host)
	return nil, nil
}

// candidateVolume is one directory that may be a volume, and the client that
// can confirm it.
type candidateVolume struct {
	server string
	// endpoint is the REST endpoint the class reaches the array by, which is
	// what identifies the array: two classes can name one array by different
	// servers.
	endpoint string
	// apiURL is the class's REST endpoint, carried so the reconstructed
	// handle is byte-identical to the one CreateVolume issued from the same
	// class -- including for a tenant class, whose endpoint is not the one
	// its server implies.
	apiURL   string
	realPath string
	name     string
	fileID   string
	client   qumulo.ClientInterface
}

// volumeID reconstructs the CSI volume ID for this directory.
//
// It has to match what CreateVolume issued exactly -- the CO compares these
// against its own volume handles, and an ID differing by so much as the
// server's spelling makes a volume look orphaned.
func (c candidateVolume) volumeID() string {
	return newVolumeID(c.server, c.apiURL, c.realPath).String()
}

// sortKey orders candidates stably across clusters.
//
// CSI's paging token is an offset, so an unstable order silently duplicates
// and omits entries across a page boundary. The cluster's directory listing
// promises no ordering, and there is now more than one cluster, so the key has
// to span both.
//
// NUL rather than "/" as the separator: a hostname cannot contain a NUL, so no
// server name can run into a path and collide with another server's.
//
// Keyed by endpoint rather than server, so the key of a directory does not
// depend on which of two classes reaching it supplied the handle that was
// kept.
func (c candidateVolume) sortKey() string {
	// The handle breaks the tie between two PVs naming one directory.
	return c.endpoint + "\x00" + c.realPath + "\x00" + c.volumeID()
}

// listCandidateVolumes enumerates one level under each class's store root.
//
// A cluster that cannot be listed fails the whole call rather than being
// skipped. That is the opposite of what this did when the unit was a
// hand-configured path rather than a cluster, and the change is deliberate:
// CSI has no partial success, so a listing missing a whole cluster's volumes
// is indistinguishable from those volumes having been deleted -- and
// ListVolumes exists to be consumed by things that act on absence. No answer
// is safer than a quietly incomplete one.
func (s *ControllerServer) listCandidateVolumes(
	ctx context.Context,
	classes []managedClass,
	pvHandles map[string]bool,
) (found []candidateVolume, skipped int, err error) {
	// Two classes can legitimately share a server and a basePath -- differing
	// only in protocol, say -- and would then yield the same directory twice
	// under the same volume ID.
	seen := make(map[string]int)

	// Replica roots per cluster, read once per cluster rather than once per
	// class, since two classes on one array share its relationship table.
	replicaRoots := make(map[string][]string)

	// Before the walk, not after: a walk that fails partway has still
	// established which classes exist, and a class deleted since the last
	// call should stop being reported either way.
	s.forgetClassesExcept(ctx, classes)

	for _, class := range classes {
		// One Secret read per class actually listed, at the point of use.
		client, clientErr := s.clientForClass(ctx, class)
		if clientErr != nil {
			// A Secret the Kubernetes API could not be made to answer for
			// -- throttled, timed out, 5xx -- says nothing about the class,
			// and it will answer on the next attempt. Skipping it would
			// shorten the list silently, or shift the page offsets under a
			// caller mid-walk, so it fails the call retryably, like an
			// unlistable cluster does.
			//
			// Note what no longer reaches here: a Secret the controller is
			// not *permitted* to read. That is permanent, so retrying is
			// pointless and Unavailable is a lie; clientForClass classifies
			// it as the misconfiguration it is and it falls to the skip
			// below.
			var unreadable *secretReadError
			if errors.As(clientErr, &unreadable) {
				return nil, 0, status.Errorf(codes.Unavailable,
					"ListVolumes: StorageClass %q: %v. Refusing to return a listing without that "+
						"class's volumes, which could not be told apart from those volumes having "+
						"been deleted", class.name, clientErr)
			}
			// A class with no usable credential -- no Secret reference, a
			// Secret that does not exist, one this controller may not read,
			// no token key -- is a misconfiguration: skipped so one bad class
			// does not hide every other class's volumes, and counted so the
			// caller can tell "no volumes" from "nothing could be read".
			slog.WarnContext(ctx, "listvolumes: skipping a StorageClass with no usable "+
				"credential; its volumes will not be listed",
				"class", class.name, "cluster", class.params.Server, "error", clientErr)
			s.observeClassCredential(ctx, class, false, clientErr)
			skipped++
			continue
		}
		s.observeClassCredential(ctx, class, true, nil)

		// Keyed by endpoint, not by `server`: two classes can share a DNS
		// server name while their endpoints reach different arrays, and one
		// array's (empty) target table then hid nothing on the other --
		// whose DR copies were listed as ordinary volumes.
		endpoint := classEndpoint(class)
		roots, ok := replicaRoots[endpoint]
		if !ok {
			if roots, err = replicaRootsOn(ctx, client, class.params.Server); err != nil {
				return nil, 0, err
			}
			replicaRoots[endpoint] = roots
		}

		entries, err := client.ListDirectory(ctx, class.root, "")
		if err != nil {
			if qumulo.IsFileNotFound(err) {
				// Normal before the first volume: the store root, or the
				// domain directory under it, is created on demand. Only the
				// cluster's own file not-found says so: a bare 404 -- an
				// unknown route, or a proxy answering for the API -- falls
				// through to Unavailable, rather than listing that class's
				// volumes as not there.
				slog.DebugContext(ctx, "listvolumes: store root does not exist yet",
					"class", class.name, "cluster", class.params.Server, "path", class.root)
				continue
			}
			return nil, 0, status.Errorf(codes.Unavailable,
				"ListVolumes: cannot list %q on cluster %q (StorageClass %q): %v. Refusing to "+
					"return a partial listing, because a listing missing one cluster's volumes "+
					"cannot be told apart from those volumes having been deleted",
				class.root, class.params.Server, class.name, err)
		}

		for _, entry := range entries {
			if !entry.IsDirectory() {
				continue
			}
			realPath := path.Join(class.root, entry.Name)
			if root, isReplica := replicaRootOf(roots, realPath); isReplica {
				// Not this cluster's volume: it is the copy of one from the
				// other array. See replicaRootsOn.
				slog.DebugContext(ctx, "listvolumes: skipping a replication target",
					"cluster", class.params.Server, "path", realPath, "targetRootPath", root)
				continue
			}

			candidate := candidateVolume{
				server:   class.params.Server,
				endpoint: endpoint,
				apiURL:   class.params.APIURL,
				realPath: realPath,
				name:     entry.Name,
				fileID:   entry.ID,
				client:   client,
			}
			// One directory on one array is one volume, however many classes
			// see it. Deduplicating by handle let a DNS-named class and an
			// address class on the same endpoint report it twice, the second
			// under a handle no PV has, whose gauges then outlived the volume.
			// The handle kept is the one a PV carries: keeping the
			// no-endpoint form unconditionally dropped the PV's own handle
			// whenever the PV came from the DNS-named class. Two PVs
			// naming one directory by both handles -- static PVs -- are both
			// listed. With no PV to go by (no lister configured, or no PV
			// yet) the no-endpoint form is kept, else the first class by
			// name.
			key := endpoint + "\x00" + realPath
			if at, dup := seen[key]; dup {
				keptIsPV, candidateIsPV := pvHandles[found[at].volumeID()], pvHandles[candidate.volumeID()]
				switch {
				case candidateIsPV && keptIsPV:
					if found[at].volumeID() != candidate.volumeID() {
						found = append(found, candidate)
					}
				case candidateIsPV:
					found[at] = candidate
				case !keptIsPV && found[at].apiURL != "" && candidate.apiURL == "":
					found[at] = candidate
				}
				continue
			}
			seen[key] = len(found)
			found = append(found, candidate)
		}
	}

	slices.SortFunc(found, func(a, b candidateVolume) int {
		return strings.Compare(a.sortKey(), b.sortKey())
	})
	return found, skipped, nil
}

// ControllerGetVolume reports one volume's current state.
//
// The request carries a volume ID and nothing else. This driver's IDs encode
// <server>/<fileID>/<name>, so the cluster and the file are both known -- only
// the credential is missing, and it is found by looking for a StorageClass
// that names that server.
//
// Worth knowing what this RPC is and is not. Its volume_condition field was
// removed in CSI spec v1.13.0, so it no longer reports health; that moved to
// ControllerGetVolumeHealth, which carries a secrets map of its own and needs
// none of this machinery. What is left is inspection and reconciliation by
// tools that call it directly.
func (s *ControllerServer) ControllerGetVolume(
	ctx context.Context,
	req *csi.ControllerGetVolumeRequest,
) (*csi.ControllerGetVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID must not be empty")
	}

	ctx = withSecretMemo(qumulo.WithSubject(ctx, req.GetVolumeId()))
	parsed, err := parseVolumeID(req.GetVolumeId())
	if err != nil {
		// An ID this driver could not have issued names no volume of its.
		return nil, status.Errorf(codes.NotFound, "volume %q not found: %v", req.GetVolumeId(), err)
	}

	client, err := s.clusterForVolume(ctx, parsed)
	if err != nil {
		return nil, err
	}

	// The handle names a path; this array's file ID for it is what the quota
	// hangs off, so resolve before reading.
	resolved, resolution, err := resolveVolumePath(ctx, client, parsed, "controllergetvolume")
	if err != nil {
		return nil, err
	}
	if resolution != volumeFound {
		return nil, status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
	}

	capacity, err := client.GetQuota(ctx, resolved.FileID)
	if err != nil {
		if qumulo.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
		}
		return nil, status.Errorf(codes.Internal,
			"ControllerGetVolume: reading quota for volume %q: %v", req.GetVolumeId(), err)
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      req.GetVolumeId(),
			CapacityBytes: clampToInt64(capacity),
			VolumeContext: map[string]string{volumeContextServer: parsed.Server},
		},
		// Empty for the same reason as in ListVolumes: no attach state exists
		// to report.
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{},
	}, nil
}

// clusterForVolume finds a credential for the cluster a volume ID names.
//
// Two classes can name one server with different Secrets -- different tenants,
// different service accounts. There is no way to pick the right one from the
// ID alone: matching on the volume's path would disambiguate, but the path
// cannot be resolved without a credential first. So the classes are tried in
// name order and the first that authenticates is used, and which one that was
// is logged rather than left to guesswork.
func (s *ControllerServer) clusterForVolume(
	ctx context.Context,
	parsed volumeID,
) (qumulo.ClientInterface, error) {
	classes, _, err := s.resolveManagedClasses(ctx, false)
	if err != nil {
		return nil, err
	}

	// By the endpoint the handle carries, which is the array the volume is
	// on. Matching on `server` alone let a class naming the same DNS server
	// but another array's endpoint answer, from the wrong array. The
	// class's own Secret goes with it: dialling the handle's endpoint with
	// another class's Secret would send one array's token to the other.
	handleEndpoint := strings.ToLower(parsed.APIEndpoint())
	var matching []managedClass
	for _, class := range classes {
		if classEndpoint(class) == handleEndpoint {
			matching = append(matching, class)
		}
	}
	// A handle with no endpoint field was minted by a class that set none,
	// so its endpoint is the default for `server`. A class that names that
	// server but has since set apiUrl still owns the volume; it is the
	// fallback, matched case-insensitively because a hostname is.
	if len(matching) == 0 && parsed.APIURL == "" {
		for _, class := range classes {
			if strings.EqualFold(class.params.Server, parsed.Server) {
				matching = append(matching, class)
			}
		}
	}

	if len(matching) == 0 {
		// Not NotFound: the volume may well exist. What is missing is a route
		// to its cluster, which is a configuration fact about this release --
		// most likely the class it was provisioned from has since been deleted.
		return nil, status.Errorf(codes.FailedPrecondition,
			"volume %q is on cluster %q, and no StorageClass of this driver names that "+
				"cluster, so there is no credential to read it with. The volume is not "+
				"reported missing, because it may still exist",
			parsed.String(), parsed.Server)
	}

	names := make([]string, 0, len(matching))
	for _, class := range matching {
		names = append(names, class.name)
	}

	// Tried in name order, and the first that yields a credential is used --
	// which is what the comment above has always promised. It used to take
	// matching[0] and stop: one class whose Secret could not be read then
	// made every volume on that array unanswerable, even with another class
	// naming the same array and holding a perfectly good token. A hand-made
	// class sorting earlier by name was enough to do it.
	//
	// A transient failure is remembered rather than returned, so a class the
	// API server merely throttled does not mask a later class that works;
	// if nothing works, it decides the code below.
	var (
		transient *secretReadError
		reasons   []string
	)
	for _, class := range matching {
		client, err := s.clientForClass(ctx, class)
		if err == nil {
			if len(matching) > 1 {
				slog.DebugContext(ctx, "controllergetvolume: several StorageClasses name this "+
					"cluster; using the first by name whose credential reads",
					"cluster", parsed.Server, "classes", names, "using", class.name)
			}
			return client, nil
		}
		var unreadable *secretReadError
		if transient == nil && errors.As(err, &unreadable) {
			transient = unreadable
		}
		reasons = append(reasons, fmt.Sprintf("%s: %v", class.name, err))
	}

	// Every class that names this array failed. Which code to answer with
	// turns on whether any of them could succeed later: a throttle or a
	// timeout is worth retrying, a Secret that does not exist or that this
	// controller may not read is not, and telling the two apart is the whole
	// point of the classification.
	if transient != nil {
		return nil, status.Errorf(codes.Unavailable,
			"volume %q is on cluster %q, and no StorageClass naming it (%s) yielded a "+
				"credential; at least one failed for a reason that may clear: %s",
			parsed.String(), parsed.Server, strings.Join(names, ", "), strings.Join(reasons, "; "))
	}
	return nil, status.Errorf(codes.FailedPrecondition,
		"volume %q is on cluster %q, but no StorageClass naming it (%s) has a usable "+
			"credential: %s",
		parsed.String(), parsed.Server, strings.Join(names, ", "), strings.Join(reasons, "; "))
}

// replicaRootsOn lists the directories on one cluster that are the target
// root of a replication relationship.
//
// A replica is not a volume this Kubernetes cluster provisioned, and it must
// not be reported as one. The measured reason is specific: EnableVolumeReplication
// pre-stages the replica's quota while the target is still read-only, and
// a quota on a directory under a class's store root is exactly what this
// listing uses to decide a directory is a driver-owned volume. So without this
// filter every protected volume appears twice -- once on each array -- and a
// reconciler comparing PersistentVolumes against the listing sees the replica
// as an orphan to delete. That deletes the DR copy.
//
// ENDED relationships are skipped: they name no live replica, and Core empties
// the root path of the deleted end anyway.
//
// A cluster whose relationship table cannot be read fails the whole call, the
// same way a cluster whose directory cannot be listed does. "I could not tell
// whether these are replicas" and "these are not replicas" must not produce
// the same answer, for the same reason a missing cluster's volumes must not
// look deleted.
func replicaRootsOn(ctx context.Context, client qumulo.ClientInterface, server string) ([]string, error) {
	view, err := replicationViewOf(client, "listvolumes", server)
	if err != nil {
		return nil, err
	}
	targets, err := view.ListTargetStatus(ctx)
	if err != nil {
		return nil, replicationListError(server,
			err, "ListVolumes: listing target relationships on cluster %q", server)
	}
	roots := make([]string, 0, len(targets))
	for _, t := range targets {
		if t.TargetRootPath != "" && t.State != qumulo.RelationshipEnded {
			roots = append(roots, t.TargetRootPath)
		}
	}
	return roots, nil
}

// replicaRootOf reports whether realPath is a replica, and which target root
// makes it one.
//
// At or beneath, not only at. The spec's ownership rule says "skip a directory
// that is the target_root_path of a relationship on that cluster", which is
// exact and is right for a dedicated-export class, where the relationship is
// rooted at the volume directory itself. It is not sufficient for the
// shared-subdirectory model measured on the lab arrays: there the one relationship is
// rooted at the class basePath and the volumes are the directories *under* it,
// each with its own pre-staged quota. Those are replicas too, and every one of
// them would otherwise be listed.
func replicaRootOf(roots []string, realPath string) (string, bool) {
	for _, root := range roots {
		if realPath == root {
			return root, true
		}
		if _, under := relativeUnder(root, realPath); under {
			return root, true
		}
	}
	return "", false
}
