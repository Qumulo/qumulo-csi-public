// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// GetCapacity reports how much space a StorageClass can provision into.
//
// This is the one management RPC a multi-cluster controller can answer, and it
// is worth being precise about why, because the other two cannot. The CSI spec
// gives none of the three a secrets map. But csi-provisioner builds this
// request with the class's parameter map passed through verbatim -- see
// `Parameters: sc.Parameters` in its pkg/capacity/capacity.go -- where for
// CreateVolume it strips the secret-reference keys and resolves them itself. So
// this request arrives naming both the cluster (server, apiUrl) and the Secret
// holding that cluster's token, and the driver resolves the one reference the
// request named. Nothing is held between calls and no cluster is discovered
// that a request did not name, which is what keeps this inside the
// stateless-multi-cluster rule.
//
// ListVolumes and ControllerGetVolume have no equivalent. ListVolumes is given
// max_entries and starting_token and nothing else; ControllerGetVolume is
// given a volume ID, which does name the cluster in this driver's encoding but
// names no Secret. Answering either means discovering clusters from the
// Kubernetes API rather than from the request, which is a different decision.
// They stay Unimplemented -- see list_volumes.go.
func (s *ControllerServer) GetCapacity(
	ctx context.Context,
	req *csi.GetCapacityRequest,
) (*csi.GetCapacityResponse, error) {
	params := req.GetParameters()

	// A request with no parameters at all is a capability probe, and is
	// answered with zero rather than an error.
	//
	// The spec makes parameters optional and defines the response as the
	// capacity available for volumes provisioned *with those parameters*. With
	// no class named, this driver -- which keeps no fallback cluster -- has
	// nowhere it could provision, so zero is the true answer. csi-sanity
	// relies on exactly this: it calls GetCapacity with a wholly empty
	// request, requires no error, and notes in its own comment that zero is a
	// possible value.
	//
	// The test is emptiness, deliberately, and not "names no cluster". Zero is
	// a load-bearing number to a CO: Kubernetes compares a requested size
	// against the CSIStorageCapacity objects it builds from this RPC, so a
	// published zero reads as "there is no room here" and makes a topology
	// ineligible -- it does not read as "I do not know". csi-provisioner only
	// ever builds an object from a call carrying a real class's parameters, so
	// this branch never becomes one. A class that *has* parameters but names
	// no server is a different thing entirely -- a misconfiguration -- and
	// falls through to parseVolumeParams below, which says which parameter is
	// missing. Answering that with zero would turn a broken class into a
	// silent scheduling block.
	if len(params) == 0 {
		slog.DebugContext(ctx, "getcapacity: request carried no parameters, reporting zero")
		return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
	}

	// Validated with the same parser CreateVolume uses, so a class that cannot
	// provision cannot silently report capacity either. That is deliberate:
	// this RPC is polled every minute, so it surfaces a malformed class long
	// before the first PVC does.
	parsed, err := parseVolumeParams(params)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "GetCapacity: %v", err)
	}

	client, token, err := s.capacityClientFor(ctx, parsed, params)
	if err != nil {
		return nil, err
	}

	fs, err := clusterFreeSpace(ctx, client, token)
	if err != nil {
		// Classified as CreateVolume classifies it: a name that does
		// not resolve or a certificate that does not verify is final, an
		// array that did not answer is Unavailable, and a call cancelled by
		// its caller is Canceled. Everything used to be Internal, so a broken
		// class and a restarting array read the same.
		return nil, capacityReadError(parsed, err)
	}

	// The tighter of the two bounds. Cluster free space is a real ceiling no
	// quota can exceed, and the store-root quota is what the class itself may
	// still take; reporting either alone overstates capacity in one direction
	// or the other.
	available := fs.FreeSizeBytes
	if quotaAvailable, ok := s.storeRootQuotaLimit(ctx, client, parsed); ok && quotaAvailable < available {
		slog.DebugContext(ctx, "getcapacity: store root quota is tighter than cluster free space",
			"cluster", parsed.Server, "storeRoot", parsed.realStoreRoot(),
			"quotaAvailable", quotaAvailable, "clusterFree", fs.FreeSizeBytes)
		available = quotaAvailable
	}

	return &csi.GetCapacityResponse{AvailableCapacity: clampToInt64(roundDownToGiB(available))}, nil
}

// roundDownToGiB drops the bytes below a whole GiB from a capacity of at
// least 1 GiB, and leaves a smaller one exact.
//
// csi-provisioner rewrites a CSIStorageCapacity object whenever the value
// changes, so a figure exact to the byte rewrote every class's object -- a
// watch event to the scheduler -- on every poll of an array taking any
// writes. Down, never up, so the scheduler is not told about space
// that is not there. Below 1 GiB it stays exact: rounding that to zero would
// refuse a small claim the array can still hold.
func roundDownToGiB(bytes uint64) uint64 {
	const gib = 1 << 30
	if bytes < gib {
		return bytes
	}
	return bytes - bytes%gib
}

// capacityReadError is the status for a failed read of a cluster's capacity.
func capacityReadError(parsed volumeParams, err error) error {
	if !arrayGaveNoAnswer(err) {
		return status.Errorf(codes.Internal,
			"GetCapacity: reading capacity of cluster %q: %v", parsed.Server, err)
	}
	if errors.Is(err, context.Canceled) {
		return status.Errorf(codes.Canceled,
			"GetCapacity: cluster %q: the call was cancelled by its caller; nothing says the array is down: %v",
			parsed.Server, err)
	}
	endpoint := apiURLFor(parsed.Server, parsed.APIURL)
	if why, ok := misconfiguredEndpoint(err); ok {
		return status.Errorf(codes.InvalidArgument,
			"GetCapacity: cluster %q: the REST API at %s cannot be reached as configured -- %s. "+
				"Correct the StorageClass's server or apiUrl, or the controller's CA bundle: %v",
			parsed.Server, endpoint, why, err)
	}
	return status.Errorf(codes.Unavailable,
		"GetCapacity: cluster %q (REST API %s) did not answer: %v", parsed.Server, endpoint, err)
}

// clusterFreeSpace reads the cluster's total and free space, or answers from a
// read of the same endpoint with the same token less than freeSpaceHintTTL ago.
//
// Every class polls separately, so an array serving ten classes was asked for
// the same /v1/file-system ten times a minute -- about 31 of the 84 REST calls
// an idle lab controller made per minute. A hint only, like
// noQuotaHints: only a successful read is remembered, so a class that cannot
// reach its array still fails on every poll with its own classified error; the
// key holds a digest of the token, never the token, so a rotated Secret reads
// afresh at once; and a revoked token, or free space that moved, shows within
// the TTL. Losing the hint costs one read.
func clusterFreeSpace(
	ctx context.Context,
	client qumulo.ClientInterface,
	token string,
) (qumulo.FileSystemCapacity, error) {
	hintKey := ""
	if named, ok := client.(interface{ Endpoint() string }); ok {
		digest := sha256.Sum256([]byte(token))
		hintKey = named.Endpoint() + "\x00" + hex.EncodeToString(digest[:])
		if v, ok := freeSpaceHints.Load(hintKey); ok {
			if hint := v.(freeSpaceHint); capacityNow().Sub(hint.at) < freeSpaceHintTTL {
				return hint.fs, nil
			}
		}
	}

	fs, err := client.GetCapacity(ctx)
	if err == nil && hintKey != "" {
		freeSpaceHints.Store(hintKey, freeSpaceHint{fs: fs, at: capacityNow()})
	}
	return fs, err
}

// freeSpaceHints maps endpoint + token digest to the last free-space read.
var freeSpaceHints sync.Map

// freeSpaceHint is one remembered read and when it was made.
type freeSpaceHint struct {
	fs qumulo.FileSystemCapacity
	at time.Time
}

// freeSpaceHintTTL is how long a free-space read is reused. Shorter than
// csi-provisioner's one-minute poll, so one class polling alone always reads
// afresh; classes sharing an array within one poll window share one read.
const freeSpaceHintTTL = 30 * time.Second

// capacityClientFor builds a Qumulo client for the class this request names,
// resolving the credential from the Secret the request's parameters point at.
// It returns the token too, which keys clusterFreeSpace's hint.
func (s *ControllerServer) capacityClientFor(
	ctx context.Context,
	parsed volumeParams,
	params map[string]string,
) (qumulo.ClientInterface, string, error) {
	if s.driver.SecretReader == nil {
		// Reached when the controller was built without Kubernetes access,
		// which is how it runs under csi-sanity and in local development. Said
		// explicitly rather than as a nil dereference, and FailedPrecondition
		// rather than Internal, because nothing about the request is wrong.
		return nil, "", status.Error(codes.FailedPrecondition,
			"GetCapacity: this controller has no Kubernetes access, so it cannot read the "+
				"StorageClass's provisioner Secret. Capacity reporting requires the driver to "+
				"run in-cluster with a ServiceAccount permitted to get secrets")
	}

	ref, err := secretRefFromParams(params)
	if err != nil {
		return nil, "", status.Errorf(codes.FailedPrecondition,
			"GetCapacity: no credential for cluster %q: %v. Capacity reporting reads the same "+
				"Secret provisioning does, so set %s and %s on the StorageClass",
			parsed.Server, err, coParamProvisionerSecretName, coParamProvisionerSecretNamespace)
	}

	data, err := s.driver.SecretReader.GetSecret(ctx, ref.Namespace, ref.Name)
	if err != nil {
		if IsSecretNotFound(err) {
			return nil, "", status.Errorf(codes.FailedPrecondition,
				"GetCapacity: the StorageClass for cluster %q names Secret %s, which does not exist",
				parsed.Server, ref)
		}
		// A refusal is this release's configuration, not a fault of the
		// driver: the controller holds `get` on only the Secrets the release
		// names. Internal said "the driver is broken" and invited a
		// retry that can never succeed, for the same reason ListVolumes'
		// Unavailable did.
		if IsSecretForbidden(err) {
			return nil, "", status.Errorf(codes.FailedPrecondition,
				"GetCapacity: the StorageClass for cluster %q names Secret %s, which this "+
					"controller is not permitted to read: list it in rbac.extraSecrets, or "+
					"set rbac.clusterWideSecrets=true",
				parsed.Server, ref)
		}
		// A read cut short by this call's own cancellation -- the capacity
		// sidecar disconnecting at a controller shutdown -- says nothing
		// about the driver or the cluster. Internal read "the driver is
		// broken" in the log of every restart under load, the one line's
		// drain left behind.
		if errors.Is(err, context.Canceled) {
			return nil, "", status.Errorf(codes.Canceled,
				"GetCapacity: cluster %q: the call was cancelled by its caller (the sidecar disconnected, "+
					"or this controller is shutting down) while reading Secret %s; nothing says the array "+
					"is down, and the next poll reads it afresh: %v", parsed.Server, ref, err)
		}
		return nil, "", status.Errorf(codes.Internal,
			"GetCapacity: reading Secret %s for cluster %q: %v", ref, parsed.Server, err)
	}

	token, err := tokenFromSecret(data, ref)
	if err != nil {
		return nil, "", status.Errorf(codes.FailedPrecondition,
			"GetCapacity: no credential for cluster %q: %v", parsed.Server, err)
	}

	// Routed through clientFor so this RPC inherits the same apiUrl handling,
	// default port and TLS policy as every other one, rather than growing a
	// second way to reach a cluster.
	client, err := s.clientFor(parsed.Server, parsed.APIURL, map[string]string{secretKeyToken: token})
	return client, token, err
}

// storeRootQuotaLimit returns the space still available under the class's
// store-root quota, and whether there is one.
//
// This is what makes two classes on one array report different numbers instead
// of the same one. Without it, every class sharing a cluster reports that
// cluster's free space, which tells an operator nothing about the class.
//
// **It is remaining space, not the ceiling.** It used to be the limit, because
// the client decoded only id and limit from the quota resource. That made the
// answer a correct upper bound and a badly loose one: a store root with a
// 10 TiB quota already 9 TiB full still reported 10 TiB, and once
// CSIStorageCapacity objects started publishing, the scheduler believed it and
// placed pods against space that was not there. GetQuotaInfo now reads
// capacity_usage alongside the limit, so the number is limit minus used.
//
// A cluster that does not report usage leaves QuotaInfo.UsageKnown false and
// Available() falls back to the limit -- the old behaviour, deliberately, since
// an under-report would stop the scheduler placing a pod on space that does
// exist.
//
// A failure here is logged and ignored rather than returned. The cluster's
// free space is already a valid answer to this RPC, and failing the whole call
// over an optional refinement would replace a slightly loose number with no
// number at all.
func (s *ControllerServer) storeRootQuotaLimit(
	ctx context.Context,
	client qumulo.ClientInterface,
	parsed volumeParams,
) (uint64, bool) {
	root := parsed.realStoreRoot()

	// A store root last seen with no quota is not asked again for a while.
	// The common class has none, and finding that out cost two of the poll's
	// three REST calls, one of them a 404, every minute for every class.
	hintKey := ""
	if named, ok := client.(interface{ Endpoint() string }); ok {
		hintKey = named.Endpoint() + "\x00" + root
		if seen, ok := noQuotaHints.Load(hintKey); ok && capacityNow().Sub(seen.(time.Time)) < noQuotaHintTTL {
			return 0, false
		}
	}

	info, err := client.LookupFileByPath(ctx, root)
	if err != nil {
		if qumulo.IsNotFound(err) {
			// Normal before the first volume on a class with adDomain set:
			// the domain directory is created by the first CreateVolume.
			slog.DebugContext(ctx, "getcapacity: store root does not exist yet",
				"cluster", parsed.Server, "storeRoot", root)
			return 0, false
		}
		slog.WarnContext(ctx, "getcapacity: could not look up the store root; "+
			"reporting cluster free space without a quota bound",
			"cluster", parsed.Server, "storeRoot", root, "error", err)
		return 0, false
	}

	quota, err := client.GetQuotaInfo(ctx, info.ID)
	if err != nil {
		if qumulo.IsNotFound(err) {
			// No quota on the store root, which is the common case: the class
			// is bounded by the cluster, not by its parent directory.
			if hintKey != "" {
				noQuotaHints.Store(hintKey, capacityNow())
			}
			return 0, false
		}
		slog.WarnContext(ctx, "getcapacity: could not read the store root's quota; "+
			"reporting cluster free space without a quota bound",
			"cluster", parsed.Server, "storeRoot", root, "fileID", info.ID, "error", err)
		return 0, false
	}
	if quota.Limit == 0 {
		return 0, false
	}

	available := quota.Available()
	if !quota.UsageKnown {
		slog.DebugContext(ctx, "getcapacity: the cluster reported no usage for the store "+
			"root's quota; bounding by its limit instead",
			"cluster", parsed.Server, "storeRoot", root, "quotaLimit", quota.Limit)
	} else {
		slog.DebugContext(ctx, "getcapacity: store root quota",
			"cluster", parsed.Server, "storeRoot", root,
			"quotaLimit", quota.Limit, "quotaUsed", quota.Usage, "quotaAvailable", available)
	}

	// A full quota is reported as zero available, not as "no bound". Treating
	// it as unbounded would fall through to cluster free space and tell the
	// scheduler there is room in a class that cannot take another byte.
	return available, true
}

// noQuotaHints maps endpoint + store root to when that root's quota lookup
// last answered "no quota". Within noQuotaHintTTL GetCapacity skips the two
// lookups and bounds the class by cluster free space alone, which is what the
// lookup would have said. A hint only: losing it costs one lookup, and a
// quota an operator adds to the store root bounds the class within the TTL.
// A quota that exists is never remembered, so its usage is read every poll.
var noQuotaHints sync.Map

// noQuotaHintTTL is how long a "no quota" answer is trusted.
const noQuotaHintTTL = 5 * time.Minute

// capacityNow is the clock the hint is aged by; tests replace it.
var capacityNow = time.Now

// clampToInt64 narrows a byte count for the response field, which is signed.
//
// A Qumulo cluster will not report more than 8 EiB of free space, but the
// conversion is unchecked in Go and would wrap to a negative number, which the
// scheduler would read as capacity. Clamping is one line and removes the
// possibility.
func clampToInt64(bytes uint64) int64 {
	if bytes > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(bytes)
}
