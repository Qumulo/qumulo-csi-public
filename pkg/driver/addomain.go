// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"qumulo-csi-driver/pkg/qumulo"
)

// adDomain namespaces a StorageClass's volumes under a per-domain
// subdirectory of its store path:
//
//	storeRealPath   /k8s/volumes            -> /k8s/volumes/ad.corp.example.com/pvc-...
//	storeExportPath /exports/k8s           -> /exports/k8s/ad.corp.example.com/pvc-...
//
// It is a layout parameter, not an access-control one. It does not change who
// can read a volume: an NFS export's identity mapping cannot name an AD
// principal, and Qumulo documents that user mapping has no effect under
// Kerberos. Anyone reading this expecting per-domain permissions should look
// at securityFlavor (security.go) instead -- restricting access needs
// Kerberos on the export, which is a different parameter.
//
// What it does buy is separation: one StorageClass per domain, each writing
// into its own tree, so a directory listing on the cluster shows which domain
// a volume belongs to and an administrator can apply cluster-side policy per
// domain directory.

// maxADDomainLength bounds the value before it becomes a path component.
// 253 is the maximum length of a DNS name, so anything longer cannot be a
// domain and is more likely a mistake or an injection attempt.
const maxADDomainLength = 253

// adDomainPattern matches a fully-qualified domain name.
//
// Deliberately strict, because the value becomes a directory name on shared
// storage: labels are alphanumeric with interior hyphens, separated by dots,
// and at least two labels are required so a bare hostname is refused. That
// rejects "..", "/", and anything else that would escape the store path -- the
// traversal check below is a second line of defence rather than the only one.
var adDomainPattern = regexp.MustCompile(
	`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)+$`)

// validateADDomain checks that a domain is safe to use as a path component.
//
// Every rejection names what was wrong. The value comes from a StorageClass an
// administrator wrote, and the only feedback they get is the message on a
// Pending PVC.
func validateADDomain(domain string) error {
	if strings.TrimSpace(domain) != domain {
		return fmt.Errorf("must not have leading or trailing whitespace")
	}
	if len(domain) > maxADDomainLength {
		return fmt.Errorf("is %d characters; a domain name cannot exceed %d", len(domain), maxADDomainLength)
	}
	// Checked before the pattern so the message is specific: a caller who
	// passed a path gets told that, rather than a generic "not an FQDN".
	if strings.ContainsAny(domain, `/\`) || strings.Contains(domain, "..") {
		return fmt.Errorf("must be a domain name, not a path: %q contains a separator or %q", domain, "..")
	}
	if !adDomainPattern.MatchString(domain) {
		return fmt.Errorf(
			"must be a fully-qualified domain name such as %q (lower or upper case letters, "+
				"digits and interior hyphens, at least two dot-separated labels)", "ad.corp.example.com")
	}
	return nil
}

// ensureADDomainDirectory creates the per-domain directory if it is missing.
//
// The cluster's create-entry API makes one level at a time, so the domain
// directory has to exist before the first volume under it. Creating it here
// rather than requiring an administrator to pre-create it keeps the parameter
// self-contained: adding a domain to a class is enough to make it work.
//
// An existing directory is adopted, not an error -- every provision after the
// first finds it already there, which is the common case.
//
// The directory deliberately gets no quota and no mode change. It is a
// container, not a volume: a quota on it would cap the sum of every volume
// inside, and ListVolumes skips it precisely because it has none.
//
// It does get the volumes' NFS owner. A group relationship rooted at
// the class's basePath replicates it, and replication refuses a directory
// owned by the token's local user. One that an older build created is
// re-owned the same way when a lookup finds it local-owned: the lookup is
// made anyway, and it costs a PATCH once.
func (s *ControllerServer) ensureADDomainDirectory(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
) error {
	if params.ADDomain == "" {
		return nil
	}

	root := params.realStoreRoot()

	// Look before creating: the lookup is the cheap path and it is the one
	// taken on every provision but the first.
	existing, err := client.LookupFileByPath(ctx, root)
	if err == nil {
		slog.DebugContext(ctx, "createvolume: domain directory already exists",
			"adDomain", params.ADDomain, "path", root)
		return s.ownDomainDirectory(ctx, client, params, existing.ID,
			localReownership(existing.Owner, existing.Group))
	}
	if u := didNotAnswer("looking up the domain directory", err); u != nil {
		return u
	}

	slog.InfoContext(ctx, "createvolume: creating the domain directory",
		"adDomain", params.ADDomain, "parent", params.StoreRealPath, "path", root)

	created, err := client.CreateDirectory(ctx, params.StoreRealPath, params.ADDomain)
	if err != nil {
		// A concurrent provision for the same domain may have won the race.
		// That is success, not a conflict -- both callers wanted the directory
		// to exist and now it does. The winner sets its owner.
		if qumulo.IsConflict(err) {
			slog.DebugContext(ctx, "createvolume: domain directory was created concurrently",
				"adDomain", params.ADDomain, "path", root)
			return nil
		}
		return fmt.Errorf("creating the domain directory %q: %w", root, err)
	}
	return s.ownDomainDirectory(ctx, client, params, created.ID, nfsOwnership())
}

// ownDomainDirectory applies attrs, an ownership update, to the domain
// directory. Only an array that does not answer fails the provision: the
// owner matters to a group relationship, not to the volume being created,
// and a refusal is logged for the operator and tried again on the next
// provision in the domain.
func (s *ControllerServer) ownDomainDirectory(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
	fileID string,
	attrs qumulo.FileAttributes,
) error {
	if attrs.IsEmpty() {
		return nil
	}
	root := params.realStoreRoot()
	err := client.SetFileAttributesByID(ctx, fileID, attrs)
	if err == nil {
		slog.InfoContext(ctx, "createvolume: gave the domain directory an NFS owner so it can replicate",
			"adDomain", params.ADDomain, "path", root)
		return nil
	}
	if u := didNotAnswer("setting the domain directory's owner", err); u != nil {
		return u
	}
	slog.WarnContext(ctx, "createvolume: could not give the domain directory an NFS owner; "+
		"a group relationship over it will refuse to replicate it",
		"adDomain", params.ADDomain, "path", root, "error", err)
	return nil
}
