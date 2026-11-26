// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"qumulo-csi-driver/pkg/qumulo"
)

// StorageClass parameter keys understood by CreateVolume. Matching is
// case-insensitive because Kubernetes passes StorageClass parameters through
// verbatim and the casing users write varies, so every key here is written in
// the lower-case form that comparison happens in.
const (
	paramServer           = "server"
	paramStoreRealPath    = "storerealpath"
	paramStoreExportPath  = "storeexportpath"
	paramProtocol         = "protocol"
	paramMountPermissions = "mountpermissions"

	paramProvisioningModel = "provisioningmodel"

	// paramAPIURL overrides the REST endpoint derived from the server. Needed
	// when a cluster's API is on a non-default port or behind a proxy, which
	// is per-cluster information and therefore per-StorageClass.
	paramAPIURL = "apiurl"

	// paramSnapshotSchedule attaches a Qumulo snapshot policy to each volume.
	// See schedule.go for the grammar, and snapshotScheduleEnabled below.
	paramSnapshotSchedule = "snapshotschedule"

	// Recognized and deliberately unimplemented. Named rather than left to
	// fall through as an unknown key so that a StorageClass written against
	// the intended parameter surface fails with "not supported yet" instead
	// of "unknown parameter", which reads like a typo.
	// paramADDomain namespaces a class's volumes under a per-domain
	// subdirectory. See adDomain.go.
	paramADDomain = "addomain"

	// paramSecurityFlavor selects the NFS authentication flavour: sys, krb5,
	// krb5i or krb5p. See security.go.
	paramSecurityFlavor = "securityflavor"

	// paramTenantID scopes a dedicated export to a Qumulo network tenant, by
	// integer id or by name. See tenant.go.
	paramTenantID = "tenantid"
	// paramAllowedClients restricts which hosts may mount a dedicated export,
	// as a comma-separated list of IPs and CIDR blocks. See tenant.go.
	paramAllowedClients = "allowedclients"
	// paramAllowForeignSnapshotRestore lets a restore copy from a snapshot
	// not named in this driver's form whose source is gone. See
	// createVolumeFromSnapshot.
	paramAllowForeignSnapshotRestore = "allowforeignsnapshotrestore"
	// paramAllowAnyClient says a dedicated-export class with no
	// allowedClients is meant to be open. See openExportRefusal.
	paramAllowAnyClient = "allowanyclient"
)

// snapshotScheduleEnabled gates the snapshotSchedule parameter.
//
// Opened 2026-08-28, once policy creates were confirmed against a real cluster
// for all three frequencies. Kept as a constant so the parameter can be closed
// again in one line if a cluster release changes the policy API.
const snapshotScheduleEnabled = true

// defaultAPIPort is the port a Qumulo REST endpoint is assumed to be on when a
// StorageClass names a server but no apiUrl.
const defaultAPIPort = "8000"

// secretKeyToken is the key the driver reads a cluster's bearer token from in
// the secrets map a CO delivers with a request. It matches the key the chart
// writes into its Secret, so one name covers both the mounted-file and the
// per-request path.
const secretKeyToken = "token"

// coParamPrefix marks parameters the container orchestrator injects rather
// than the administrator writing them.
//
// csi-provisioner adds csi.storage.k8s.io/pvc/name, .../pvc/namespace and.
// /pv/name to CreateVolume when it runs with --extra-create-metadata. They
// are not StorageClass parameters and no driver should reject them: doing so
// would fail every provision in a real cluster while every unit test passed.
const coParamPrefix = "csi.storage.k8s.io/"

// defaultMountPermissions is the mode applied to a volume directory when the
// class does not say otherwise.
//
// 0777 is deliberate, and it is what upstream csi-driver-nfs defaults its
// equivalent parameter to. The driver creates a directory through the REST API
// with the cluster's default 0755 and gives it NFS uid and gid 0; an NFS client
// writes as its own uid and gid (the driver's exports map no identity,
// NFS_MAP_NONE), so it is neither the owner nor in the owning group and cannot
// write. Nothing
// narrower can be chosen here without knowing the workload's uid, which a
// StorageClass does not.
//
// An operator whose export maps identities so that ownership does line up can
// set mountPermissions to mountPermissionsInherit and keep the cluster default.
const defaultMountPermissions = "0777"

// mountPermissionsInherit tells the driver to leave a new directory's mode
// exactly as the cluster created it.
//
// This needs a word rather than an empty string: a StorageClass parameter
// rendered by Helm is empty when unset, and "unset" has to keep meaning "apply
// the default" -- otherwise the fix for an unwritable volume would be opt-in.
const mountPermissionsInherit = "inherit"

// Values for the protocol parameter, which selects the NFS version the node
// mounts with.
const (
	protocolNFS3  = "nfs3"
	protocolNFS41 = "nfs4.1"
)

// Values for the provisioningModel parameter.
//
// shared-subdirectory makes a volume a directory under the store path, inside
// an export an administrator created once; the driver touches no export.
// dedicated-export additionally gives each volume its own NFS export, which
// costs one entry in the cluster's export table per volume -- a table that
// holds a handful of entries on a typical cluster, so this model has a
// practical ceiling the shared model does not.
const (
	provisioningModelSharedSubdirectory = "shared-subdirectory"
	provisioningModelDedicatedExport    = "dedicated-export"
)

// VolumeContext keys. The controller stamps these onto every volume it
// provisions and the node service reads them back to build its mount
// source, so the two have to agree exactly: VolumeContext is the only
// channel between them, and a key only one side knows is an empty string to
// the other -- which surfaces as a mount of "<server>:" rather than as any
// kind of error. Defining them once here is what keeps them in step.
const (
	volumeContextServer     = paramServer
	volumeContextExportPath = "exportPath"
	volumeContextRealPath   = "realPath"
	volumeContextProtocol   = paramProtocol
	// volumeContextADDomain carries the domain a volume was provisioned under,
	// so the node can see it without re-deriving it from a path.
	volumeContextADDomain = "adDomain"
	// volumeContextSecurityFlavor tells the node which sec= option to mount
	// with. It must match the export's authentication mode, which the
	// controller set from the same parameter.
	volumeContextSecurityFlavor = "securityFlavor"
	// volumeContextTenantID and volumeContextAllowedClients record the access
	// scoping a volume was created with, on the PV itself. A StorageClass can
	// be edited or deleted; the PV is the only durable record of what a given
	// volume was actually restricted to.
	volumeContextTenantID       = "tenantID"
	volumeContextAllowedClients = "allowedClients"
	// volumeContextReplicated marks a volume whose directory is one end of a
	// Qumulo replication relationship. Fully qualified, unlike the keys
	// above, because unlike them it is not a StorageClass parameter the
	// controller copies through: it is a PV-level marker, and the prefix says
	// which driver owns it.
	//
	// It is a marker for operators and tools, not a switch. The node's
	// read-only probe (node_replica.go) runs on EVERY read-write publish and
	// only names the volume as a known replica in its log when this key is
	// present; a volume without it is probed exactly the same. The driver's
	// controller never sets it -- VolumeReplication objects never reach
	// CreateVolume, and in the recommended one-PV topology the same PV
	// serves both sides -- so `qumulo-csi-repoint mirror` writing it onto
	// the DR-side PVs it emits, and an operator setting it on a static PV,
	// are the only producers, and nothing depends on either.
	volumeContextReplicated = "csi.qumulo.com/replicated"
)

// volumeParams is the validated StorageClass configuration for one volume.
//
// The driver provisions a directory under StoreRealPath and relies on an
// admin having already exported the parent over NFS at StoreExportPath; it
// never creates or deletes exports itself.
type volumeParams struct {
	// Server is the Qumulo cluster host the node will mount from.
	Server string
	// StoreRealPath is the cluster filesystem path holding volume directories.
	StoreRealPath string
	// StoreExportPath is the NFS export path the parent is reachable under.
	StoreExportPath string
	// Protocol is the NFS version to mount with, or "" to let the kernel
	// negotiate. Optional on purpose -- see parseVolumeParams.
	Protocol string
	// ProvisioningModel selects whether the volume gets its own NFS export.
	// Always one of the provisioningModel* constants; never empty.
	ProvisioningModel string
	// MountPermissions is the octal mode to set on a volume's directory, or
	// mountPermissionsInherit to leave the cluster's default alone. Never
	// empty: an unset parameter resolves to defaultMountPermissions.
	MountPermissions string
	// APIURL overrides the REST endpoint for this volume's cluster. Empty
	// means derive it from Server.
	APIURL string
	// SnapshotSchedule is the parsed snapshot policy schedule to attach to a
	// volume, or nil when the class asks for none. Parsed here rather than at
	// use so a malformed schedule is rejected before anything is created.
	SnapshotSchedule *qumulo.Schedule
	// ADDomain namespaces this class's volumes under a subdirectory of the
	// store paths. Empty means the flat, cluster-wide layout every volume
	// provisioned before this parameter existed already uses.
	ADDomain string

	// SecurityFlavor is the NFS authentication flavour for this class's
	// volumes. Empty behaves as sys, which is what every volume provisioned
	// before this parameter existed already gets.
	SecurityFlavor string

	// TenantID is the network tenant a dedicated export is attached to, as
	// written in the StorageClass: an integer id or a tenant name. Empty
	// leaves the choice to the cluster, which uses tenant 1.
	TenantID string

	// AllowedClients are the IPs and CIDR blocks permitted to mount a
	// dedicated export. Empty means unrestricted, which is what every export
	// created before this parameter existed already is.
	AllowedClients []string

	// AllowForeignSnapshotRestore lets this class restore a snapshot whose
	// name is not in this driver's form and whose source is gone or does not
	// resolve -- one nothing shows was taken of this driver's volumes.
	// False unless the class sets "true".
	AllowForeignSnapshotRestore bool

	// AllowAnyClient says a dedicated export with no AllowedClients is open
	// on purpose: any host, root unsquashed, no authentication.
	// False unless the class sets "true".
	AllowAnyClient bool
}

// DedicatedExport reports whether volumes from this class get their own NFS
// export.
func (p volumeParams) DedicatedExport() bool {
	return p.ProvisioningModel == provisioningModelDedicatedExport
}

// SetsMode reports whether the driver should set the mode on a new volume
// directory rather than leaving the cluster's default.
func (p volumeParams) SetsMode() bool {
	return p.MountPermissions != mountPermissionsInherit
}

// recognizedParams is every StorageClass parameter this driver knows about,
// including the ones it refuses. A key absent from this set is a typo or a
// parameter from a different driver, and either way the administrator asked
// for something that will not happen.
var recognizedParams = []string{
	paramServer,
	paramStoreRealPath,
	paramStoreExportPath,
	paramProtocol,
	paramProvisioningModel,
	paramMountPermissions,
	paramADDomain,
	paramSnapshotSchedule,
	paramAPIURL,
	paramTenantID,
	paramAllowedClients,
	paramSecurityFlavor,
	paramAllowForeignSnapshotRestore,
	paramAllowAnyClient,
}

// parseVolumeParams validates the StorageClass parameters CreateVolume needs.
//
// Unknown keys are rejected rather than ignored. Silently dropping them means a
// class asking for a dedicated export, a specific protocol and a snapshot
// schedule provisions a plain shared subdirectory and reports success -- the
// administrator gets none of what they asked for and no indication of it.
func parseVolumeParams(params map[string]string) (volumeParams, error) {
	normalized := make(map[string]string, len(params))
	// written records the spelling each folded key came from, so two keys
	// that differ only by case can be refused rather than resolved by map
	// iteration order -- which is random, so "server: a" beside "Server: b"
	// provisioned against a different array on different calls, with
	// nothing in the class saying which.
	written := make(map[string]string, len(params))
	var unknown []string
	var collisions []string
	for k, v := range params {
		key := strings.ToLower(strings.TrimSpace(k))
		// Parameters the CO injects are not the administrator's to get right.
		if strings.HasPrefix(key, coParamPrefix) {
			continue
		}
		if earlier, repeated := written[key]; repeated {
			// Quoted as written, untrimmed: two keys can differ only in
			// whitespace, and "server and server" says nothing about
			// which two lines of the class collided.
			first, second := earlier, k
			if second < first {
				first, second = second, first
			}
			collisions = append(collisions, fmt.Sprintf("%q and %q", first, second))
			continue
		}
		written[key] = k
		if !slices.Contains(recognizedParams, key) {
			// Reported as the administrator wrote it, not as it was folded
			// for the comparison. This message's entire job is to point at a
			// typo, and a lowercased "storerealpth" does not match anything
			// grep finds in the StorageClass that caused it.
			unknown = append(unknown, strings.TrimSpace(k))
			continue
		}
		normalized[key] = strings.TrimSpace(v)
	}
	if len(collisions) > 0 {
		slices.Sort(collisions)
		return volumeParams{}, fmt.Errorf("StorageClass parameter(s) given twice, differing only by case: %s. "+
			"Parameter names are matched case-insensitively, so only one of each pair would take effect and "+
			"which one is not defined", strings.Join(collisions, ", "))
	}
	if len(unknown) > 0 {
		// Folded, so the order does not depend on which keys happened to be
		// capitalized -- the names themselves stay as written.
		slices.SortFunc(unknown, func(a, b string) int {
			return strings.Compare(strings.ToLower(a), strings.ToLower(b))
		})
		recognized := slices.Clone(recognizedParams)
		slices.Sort(recognized)
		return volumeParams{}, fmt.Errorf("unknown StorageClass parameter(s): %s (recognized: %s)",
			strings.Join(unknown, ", "), strings.Join(recognized, ", "))
	}

	// Enum-valued parameters are matched case-insensitively for exactly the
	// reason their keys are: the casing users write varies. Paths and the
	// server are deliberately left as written -- a cluster path is
	// case-sensitive, so folding it would provision somewhere else.
	for _, key := range []string{paramProtocol, paramProvisioningModel, paramMountPermissions} {
		normalized[key] = strings.ToLower(normalized[key])
	}

	// An unimplemented parameter is only a problem if it was actually asked
	// for. Helm templates routinely render an unset value as an empty string,
	// and refusing those would make a chart's default values unusable.
	if err := rejectUnimplemented(normalized); err != nil {
		return volumeParams{}, err
	}

	parsed := volumeParams{
		Server:          normalized[paramServer],
		StoreRealPath:   normalized[paramStoreRealPath],
		StoreExportPath: normalized[paramStoreExportPath],
		Protocol:        normalized[paramProtocol],
		// Resolved rather than left empty, so no downstream code has to know
		// which value an unset parameter means.
		ProvisioningModel: normalized[paramProvisioningModel],
		MountPermissions:  normalized[paramMountPermissions],
		APIURL:            normalized[paramAPIURL],
	}

	// Validated here rather than at use, for the same reason as the schedule
	// below: a malformed value must be refused before CreateVolume has created
	// anything on the cluster.
	if flavor := normalized[paramSecurityFlavor]; flavor != "" {
		if err := validateSecurityFlavor(flavor); err != nil {
			return volumeParams{}, fmt.Errorf("%s %q: %w", paramSecurityFlavor, flavor, err)
		}
		parsed.SecurityFlavor = flavor
	}

	if domain := normalized[paramADDomain]; domain != "" {
		if err := validateADDomain(domain); err != nil {
			return volumeParams{}, fmt.Errorf("%s %q: %w", paramADDomain, domain, err)
		}
		parsed.ADDomain = domain
	}

	if tenant := normalized[paramTenantID]; tenant != "" {
		if err := validateTenantID(tenant); err != nil {
			return volumeParams{}, fmt.Errorf("%s %q: %w", paramTenantID, tenant, err)
		}
		parsed.TenantID = tenant
	}

	// "true" or "false" in any case, or unset. Anything else is refused
	// rather than read as false: a class written to allow something should
	// not quietly refuse it.
	switch strings.ToLower(normalized[paramAllowForeignSnapshotRestore]) {
	case "", "false":
	case "true":
		parsed.AllowForeignSnapshotRestore = true
	default:
		return volumeParams{}, fmt.Errorf("%s must be \"true\" or \"false\", got %q",
			paramAllowForeignSnapshotRestore, normalized[paramAllowForeignSnapshotRestore])
	}

	switch strings.ToLower(normalized[paramAllowAnyClient]) {
	case "", "false":
	case "true":
		parsed.AllowAnyClient = true
	default:
		return volumeParams{}, fmt.Errorf("%s must be \"true\" or \"false\", got %q",
			paramAllowAnyClient, normalized[paramAllowAnyClient])
	}

	if raw := normalized[paramAllowedClients]; raw != "" {
		clients, err := parseAllowedClients(raw)
		if err != nil {
			return volumeParams{}, fmt.Errorf("%s: %w", paramAllowedClients, err)
		}
		parsed.AllowedClients = clients
	}

	// Parsed during validation, not at use: a schedule the cluster would
	// reject must fail before CreateVolume has made a directory. The cluster's
	// own rejection is an opaque "Invalid fields for schedule" that names
	// nothing, so this is the only place a mistake can be described.
	//
	if raw := normalized[paramSnapshotSchedule]; raw != "" {
		schedule, err := parseSnapshotSchedule(raw)
		if err != nil {
			return volumeParams{}, fmt.Errorf("%s %q: %w", paramSnapshotSchedule, raw, err)
		}
		parsed.SnapshotSchedule = &schedule
	}
	if parsed.ProvisioningModel == "" {
		parsed.ProvisioningModel = provisioningModelSharedSubdirectory
	}
	if parsed.MountPermissions == "" {
		parsed.MountPermissions = defaultMountPermissions
	}

	var missing []string
	for key, value := range map[string]string{
		paramServer:          parsed.Server,
		paramStoreRealPath:   parsed.StoreRealPath,
		paramStoreExportPath: parsed.StoreExportPath,
	} {
		if value == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return volumeParams{}, fmt.Errorf("missing required StorageClass parameter(s): %s",
			strings.Join(missing, ", "))
	}

	if err := validateServerHost(parsed.Server); err != nil {
		return volumeParams{}, fmt.Errorf("%s %q: %w", paramServer, parsed.Server, err)
	}

	// After the server, because the endpoint is only meaningful once the
	// address it overrides is known to be one.
	if parsed.APIURL != "" {
		if err := validateAPIURL(parsed.APIURL); err != nil {
			return volumeParams{}, fmt.Errorf("%s %q: %w", paramAPIURL, parsed.APIURL, err)
		}
	}

	// Last, because it is a cross-parameter check: it needs the provisioning
	// model defaulted above and both scoping parameters parsed.
	if err := validateExportScoping(parsed); err != nil {
		return volumeParams{}, err
	}
	if err := validateSecurityFlavorScoping(parsed); err != nil {
		return volumeParams{}, err
	}

	// A relative store path would resolve against an undefined working
	// directory on the cluster, so require both to be absolute.
	if !path.IsAbs(parsed.StoreRealPath) {
		return volumeParams{}, fmt.Errorf("%s must be an absolute path, got %q", paramStoreRealPath, parsed.StoreRealPath)
	}
	if !path.IsAbs(parsed.StoreExportPath) {
		return volumeParams{}, fmt.Errorf("%s must be an absolute path, got %q", paramStoreExportPath, parsed.StoreExportPath)
	}
	// And neither may climb. A store root of "/csi/volumes/.." provisions
	// into "/csi", and the handle minted for a volume under it is cleaned,
	// so the PV names a directory that is not under the root the class
	// declares -- which is what every ownership and coverage check in this
	// driver compares against. The same rule parseVolumeID applies to
	// a handle's path.
	for key, value := range map[string]string{
		paramStoreRealPath:   parsed.StoreRealPath,
		paramStoreExportPath: parsed.StoreExportPath,
	} {
		if hasDotElement(value) {
			// Named as the class writes it ("storeRealPath"), not as it
			// was folded for matching, so grep finds the line.
			return volumeParams{}, fmt.Errorf("%s %q has a %q or %q element; write the path it resolves to (%q)",
				strings.TrimSpace(written[key]), value, ".", "..", path.Clean(value))
		}
	}

	// The volume handle is "<server>#<realPath>" (volume_id.go), so a "#"
	// anywhere in the path this class provisions under produces a handle
	// parseVolumeID can never decode. The failure mode is the one
	// validateServerHost describes for "/": CreateVolume succeeds, the PVC
	// binds, and then DeleteVolume, ControllerExpandVolume and
	// ValidateVolumeCapabilities all fail permanently on a handle the driver
	// itself issued, leaving the directory and quota to be removed by hand.
	//
	// storeRealPath is the one that reaches a handle. storeExportPath and
	// adDomain are checked beside it because adDomain becomes a path element
	// of realPath, and an export path carrying "#" is a surprise nobody needs.
	for _, field := range []struct{ name, value string }{
		{paramStoreRealPath, parsed.StoreRealPath},
		{paramStoreExportPath, parsed.StoreExportPath},
		{paramADDomain, parsed.ADDomain},
	} {
		if strings.Contains(field.value, volumeIDSeparator) {
			return volumeParams{}, fmt.Errorf(
				"%s must not contain %q, which separates the fields of a volume handle (<server>%s<path>): "+
					"a volume provisioned under %q would get a handle this driver cannot decode, and could "+
					"then never be deleted or expanded through Kubernetes",
				field.name, volumeIDSeparator, volumeIDSeparator, field.value)
		}
	}

	// Protocol is optional, and left empty deliberately when unset: the node
	// then adds no version option and the kernel negotiates, which is what
	// every volume provisioned before this parameter existed already does.
	// Defaulting it here would remount those volumes differently on upgrade.
	switch parsed.Protocol {
	case "", protocolNFS3, protocolNFS41:
	default:
		return volumeParams{}, fmt.Errorf("%s must be %q or %q, got %q",
			paramProtocol, protocolNFS3, protocolNFS41, parsed.Protocol)
	}

	if err := validateMountPermissions(parsed.MountPermissions); err != nil {
		return volumeParams{}, err
	}

	return parsed, nil
}

// validateServerHost checks that server is a bare NFS host.
//
// server is not only what the node mounts from -- it is the first field of
// every volume handle this class produces (`<server>/<fileID>/<name>`, see
// volume_id.go). That makes a malformed value far more than a mount failure:
// a value containing "/" produces a handle parseVolumeID can never decode, so
// CreateVolume succeeds, the PVC Binds, and then DeleteVolume,
// ControllerExpandVolume and ValidateVolumeCapabilities all fail permanently
// on a handle the driver itself issued. The volume's directory and quota then
// have to be removed by hand on the cluster.
//
// The value most often gotten wrong is a REST URL, because apiUrl next to it
// is one, so that case is named explicitly rather than folded into a generic
// "not a hostname".
// Whitespace needs no case of its own: parseVolumeParams trims every value
// before this runs, and serverHostPattern is anchored, so an interior space is
// refused by the pattern.
func validateServerHost(server string) error {
	if strings.Contains(server, "://") {
		return fmt.Errorf("is the NFS host to mount from, not a URL -- use a bare hostname "+
			"or IPv4 address such as %q, and set %s to the REST endpoint",
			"qumulo.example.com", paramAPIURL)
	}
	// Before the port check, because a URL-shaped value hits both and the
	// separator is the one that corrupts the volume handle.
	if strings.Contains(server, "/") {
		return fmt.Errorf("must not contain %q: it is the first field of this class's volume handles, "+
			"which are %q-separated, so a handle built from it could not be decoded", "/", volumeIDSeparator)
	}
	if strings.Contains(server, ":") {
		// Covers both a port and an IPv6 literal; the message names both
		// because the remedy differs.
		return fmt.Errorf("must not contain %q: NFS is always mounted on port 2049, so no port belongs here, "+
			"and IPv6 cluster addresses are not supported in this release -- use a hostname", ":")
	}
	if len(server) > maxServerHostLength {
		return fmt.Errorf("is %d characters; a host name cannot exceed %d", len(server), maxServerHostLength)
	}
	if !serverHostPattern.MatchString(server) {
		return fmt.Errorf("must be a hostname or IPv4 address such as %q or %q "+
			"(letters, digits, interior hyphens, dot-separated)",
			"qumulo.example.com", "10.0.0.1")
	}
	return nil
}

// maxServerHostLength is the maximum length of a DNS name. Anything longer
// cannot be a host, and the handle it would build wastes the CSI spec's
// 128-byte budget on a value that will never resolve.
const maxServerHostLength = 253

// serverHostPattern matches a hostname or an IPv4 literal.
//
// One or more dot-separated alphanumeric labels with interior hyphens, which
// admits an unqualified in-cluster name ("qumulo") as well as an FQDN and an
// IPv4 address. Looser than adDomainPattern, which requires two labels
// because it becomes a directory name; a mount target legitimately may not
// have a dot in it. An IPv4 address needs no separate case: it is already
// dot-separated digit labels.
var serverHostPattern = regexp.MustCompile(
	`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*$`)

// validateMountPermissions accepts the inherit keyword or an octal mode.
//
// The value is validated rather than passed through because the cluster takes
// it as an opaque string: a typo like "777x" or a decimal "755" would be
// rejected by the API mid-provision, after a directory already exists.
func validateMountPermissions(value string) error {
	if value == mountPermissionsInherit {
		return nil
	}

	digits := strings.TrimPrefix(value, "0")
	if len(value) < 3 || len(value) > 4 || len(digits) == 0 {
		return fmt.Errorf("%s must be a 3- or 4-digit octal mode such as %q, or %q, got %q",
			paramMountPermissions, defaultMountPermissions, mountPermissionsInherit, value)
	}
	if _, err := strconv.ParseUint(value, 8, 32); err != nil {
		return fmt.Errorf("%s must be a 3- or 4-digit octal mode such as %q, or %q, got %q",
			paramMountPermissions, defaultMountPermissions, mountPermissionsInherit, value)
	}
	return nil
}

// rejectUnimplemented refuses parameters the driver recognizes but cannot act
// on, so that asking for one fails at provision time rather than producing a
// volume that quietly lacks the requested behavior.
func rejectUnimplemented(normalized map[string]string) error {
	switch model := normalized[paramProvisioningModel]; model {
	case "", provisioningModelSharedSubdirectory, provisioningModelDedicatedExport:
	default:
		return fmt.Errorf("%s must be %q or %q, got %q", paramProvisioningModel,
			provisioningModelSharedSubdirectory, provisioningModelDedicatedExport, model)
	}

	// adDomain needs more than a domain name to act on: a Qumulo export maps
	// an identity by SID, so a principal has to be named and resolved against
	// a domain-joined cluster. Accepting the parameter now would mean either
	// guessing that convention or ignoring the value.
	return nil
}

// realPathFor returns the cluster filesystem path of the named volume.
//
// When the class sets adDomain the volume is nested one level deeper, under a
// directory named for the domain. Both path helpers insert it, and they must
// stay in step: the real path is what the REST API acts on and the export path
// is what the node mounts, so a domain applied to one and not the other
// provisions cleanly and then fails to mount.
func (p volumeParams) realPathFor(name string) string {
	return path.Join(p.realStoreRoot(), name)
}

// exportPathFor returns the NFS-visible path of the named volume, which is
// what the node service mounts.
func (p volumeParams) exportPathFor(name string) string {
	return path.Join(p.exportStoreRoot(), name)
}

// realStoreRoot is the directory a volume is created in: the class's store
// path, plus the domain subdirectory when one is set.
func (p volumeParams) realStoreRoot() string {
	if p.ADDomain == "" {
		return p.StoreRealPath
	}
	return path.Join(p.StoreRealPath, p.ADDomain)
}

// exportStoreRoot mirrors realStoreRoot on the NFS side.
func (p volumeParams) exportStoreRoot() string {
	if p.ADDomain == "" {
		return p.StoreExportPath
	}
	return path.Join(p.StoreExportPath, p.ADDomain)
}

// validateAPIURL refuses an apiUrl that could not be dialled, or that would
// corrupt a handle.
//
// Checked at parse time rather than at dial time because this value now
// travels in the volume handle. A malformed endpoint used to cost one failed
// request; it now gets written into an identifier that is immutable for the
// life of the PersistentVolume, so it has to be refused before CreateVolume
// creates anything.
//
// The "#" case is the load-bearing one: it is the handle's field separator, so
// an endpoint containing it would split into fields that parse back as a
// different address, or as a malformed handle for a volume that exists.
func validateAPIURL(apiURL string) error {
	if strings.Contains(apiURL, volumeIDSeparator) {
		return fmt.Errorf("must not contain %q: it separates the fields of this class's volume "+
			"and snapshot handles, so a handle built from it could not be decoded",
			volumeIDSeparator)
	}
	parsed, err := url.Parse(apiURL)
	if err != nil {
		return fmt.Errorf("is not a URL: %w", err)
	}
	// A bare host parses without error and with an empty Scheme, which would
	// then be dialled as a relative path. Naming the scheme is the remedy.
	//
	// https only. Every call to this endpoint carries the class's bearer
	// token, and the endpoint also arrives inside volume and snapshot handles,
	// which anyone who can write a PV or VolumeSnapshotContent can edit: an
	// http:// one sent the token in cleartext on the next Delete, Expand or
	// Validate. Qumulo's REST API is served over TLS.
	if parsed.Scheme != "https" {
		return fmt.Errorf("must be an https:// URL such as %q: every call to it carries the API token, "+
			"which http:// would send in cleartext", "https://qumulo.example.com:8000")
	}
	if parsed.Host == "" {
		return fmt.Errorf("names no host; use a form such as %q", "https://qumulo.example.com:8000")
	}
	return nil
}
