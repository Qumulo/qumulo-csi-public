// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"log/slog"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// policyNamePrefix marks a snapshot policy as this driver's.
//
// It is the ownership marker, and it is what makes deleting a policy safe: an
// administrator's own policy on the same directory must never be adopted on a
// retry or swept on delete. Matching on the source file ID alone would do
// exactly that, because a directory may legitimately carry someone else's
// schedule.
const policyNamePrefix = "csi-"

// policyNameFor is the deterministic policy name for a volume.
//
// Deterministic because idempotency depends on it: a retried CreateVolume has
// to find the policy a previous attempt made, and the volume name is the only
// identifier both attempts share.
func policyNameFor(volumeName string) string {
	return policyNamePrefix + volumeName
}

// ensurePolicy attaches this volume's snapshot policy, if its class asked for
// one, and does nothing otherwise.
//
// Idempotent by adoption, like every other step in CreateVolume: a policy
// already carrying this volume's name is the one a previous attempt created,
// so it is left alone rather than duplicated. The alternative -- creating
// unconditionally -- would leave a policy per retry, each firing on the same
// directory.
func (s *ControllerServer) ensurePolicy(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
	name, fileID string,
) error {
	if params.SnapshotSchedule == nil {
		return nil
	}

	policyName := policyNameFor(name)

	existing, err := s.findPolicyByName(ctx, client, policyName)
	if err != nil {
		return status.Errorf(codes.Internal, "checking for an existing snapshot policy %q: %v", policyName, err)
	}
	if existing != nil {
		// A retry. The schedule itself is not compared: the class that
		// provisioned this volume is the class that owns it, and a class's
		// parameters cannot change after provisioning -- Kubernetes forbids
		// editing them -- so a differing schedule would mean someone edited
		// the policy on the cluster, which is theirs to do.
		slog.DebugContext(ctx, "createvolume: snapshot policy already exists, adopting",
			"volume", name, "policy", policyName, "policyID", existing.ID)
		return nil
	}

	created, err := client.CreatePolicy(ctx, policyName, fileID, *params.SnapshotSchedule)
	if err != nil {
		return status.Errorf(codes.Internal, "attaching snapshot policy %q to %q: %v",
			policyName, params.realPathFor(name), err)
	}

	rememberPolicy(client, policyName, created.ID)
	slog.InfoContext(ctx, "createvolume: attached a snapshot policy",
		"volume", name, "policy", policyName, "policyID", created.ID,
		"frequency", params.SnapshotSchedule.CreationSchedule.Frequency,
		"retention", params.SnapshotSchedule.ExpirationTimeToLive)
	return nil
}

// deleteVolumePolicy removes this volume's snapshot policy, best effort.
//
// Best effort deliberately, matching reclaimAbandonedRestore: DeleteVolume's
// contract is about the volume, and refusing to delete it because a schedule
// could not be swept would strand the PV -- leaving the CO retrying forever
// over an object the operator cannot see from Kubernetes.
//
// A volume whose class set no schedule has no policy to find, so the listing
// simply matches nothing. That is the common case and not an error.
func (s *ControllerServer) deleteVolumePolicy(
	ctx context.Context,
	client qumulo.ClientInterface,
	volumeName, fileID string,
) {
	policyName := policyNameFor(volumeName)

	existing, err := s.findPolicyByName(ctx, client, policyName)
	if err != nil {
		slog.WarnContext(ctx, "deletevolume: could not list snapshot policies; one may be left behind",
			"volume", volumeName, "policy", policyName, "error", err)
		return
	}
	if existing == nil {
		return
	}
	// The name alone is not enough: a handle for a path that does not exist
	// deleted an administrator's policy that happened to be called
	// csi-<leaf>. With the volume found, the policy must be on its
	// directory. With it gone, only a policy whose own source is gone too --
	// an orphan firing at nothing -- is removed.
	if !s.policyIsThisVolumes(ctx, client, existing, fileID) {
		slog.WarnContext(ctx, "deletevolume: a snapshot policy has this volume's name but not its directory; left alone",
			"volume", volumeName, "policy", policyName, "policyID", existing.ID,
			"policySource", existing.SourceFileID, "volumeFileID", fileID)
		return
	}

	slog.InfoContext(ctx, "deletevolume: removing the volume's snapshot policy",
		"volume", volumeName, "policy", policyName, "policyID", existing.ID)

	if err := client.DeletePolicy(ctx, existing.ID); err != nil && !qumulo.IsNotFound(err) {
		slog.WarnContext(ctx, "deletevolume: could not remove the snapshot policy; it may need manual cleanup",
			"volume", volumeName, "policy", policyName, "policyID", existing.ID, "error", err)
		return
	}
	forgetPolicyHint(client, policyName)
}

// policyIsThisVolumes reports whether policy belongs to the volume whose
// directory has fileID, or, with fileID empty (the directory is gone),
// whether the policy's own source is gone too.
func (s *ControllerServer) policyIsThisVolumes(
	ctx context.Context, client qumulo.ClientInterface, policy *qumulo.PolicyInfo, fileID string,
) bool {
	if fileID != "" {
		return policy.SourceFileID == fileID
	}
	// livePath keeps the root as "/": trimmed, a policy on the filesystem root
	// read as an orphan and was deleted.
	resolved, err := client.ResolveFileIDs(ctx, []string{policy.SourceFileID})
	return err == nil && livePath(resolved[policy.SourceFileID]) == ""
}

// findPolicyByName returns the driver-owned policy for a volume, or nil.
func (s *ControllerServer) findPolicyByName(
	ctx context.Context,
	client qumulo.ClientInterface,
	policyName string,
) (*qumulo.PolicyInfo, error) {
	return findPolicyNamed(ctx, client, policyName)
}

// findPolicyNamed returns the policy called policyName, or nil.
//
// The cluster has no lookup-by-name endpoint, so this lists and filters. The
// listing is cluster-wide rather than per-directory, which is why the name
// match has to be exact: a prefix match could claim an administrator's policy
// whose name merely starts the same way.
//
// The policy last found or created under the name is read back by ID first
// and believed only while it still carries the name; the listing it replaces
// is every policy on the cluster, read on every CreateVolume with a schedule,
// every DeleteVolume and every rollback. It never answers "no policy":
// a name with none is listed every time, so a DeleteVolume still sweeps an
// orphan no hint knew of.
func findPolicyNamed(
	ctx context.Context,
	client qumulo.ClientInterface,
	policyName string,
) (*qumulo.PolicyInfo, error) {
	getter, hinted := client.(policyGetter)
	key := ""
	if hinted {
		key = getter.Endpoint() + "\x00" + policyName
		if v, ok := policyHints.Load(key); ok {
			if policy, err := getter.GetPolicy(ctx, v.(idHint).id); err == nil && policy.Name == policyName {
				policyHints.CompareAndSwap(key, v, newIDHint(policy.ID))
				return &policy, nil
			}
			policyHints.CompareAndDelete(key, v)
		}
	}

	policies, err := client.ListPolicies(ctx)
	if err != nil {
		return nil, err
	}

	for i := range policies {
		if policies[i].Name == policyName {
			if hinted {
				policyHints.Store(key, newIDHint(policies[i].ID))
				notePackageHintStored()
			}
			return &policies[i], nil
		}
	}
	return nil, nil
}

// policyGetter is a client that can read one policy by ID and name its
// endpoint -- *qumulo.Client.
type policyGetter interface {
	GetPolicy(ctx context.Context, policyID string) (qumulo.PolicyInfo, error)
	Endpoint() string
}

// policyHints maps endpoint + policy name to the policy last found or
// created under that name (an idHint). A hint only, read back on every use.
var policyHints sync.Map

// rememberPolicy records a policy this driver just created, so the volume's
// next lookup -- a retry, the rollback, DeleteVolume -- reads it by ID.
func rememberPolicy(client qumulo.ClientInterface, policyName, policyID string) {
	if getter, ok := client.(policyGetter); ok && policyID != "" {
		policyHints.Store(getter.Endpoint()+"\x00"+policyName, newIDHint(policyID))
		notePackageHintStored()
	}
}

// forgetPolicyHint drops the hint for a policy this driver has removed.
func forgetPolicyHint(client qumulo.ClientInterface, policyName string) {
	if getter, ok := client.(policyGetter); ok {
		policyHints.Delete(getter.Endpoint() + "\x00" + policyName)
	}
}
