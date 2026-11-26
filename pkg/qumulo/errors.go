// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
)

// Error classes reported by Qumulo Core in the error_class field.
//
// These are more precise than a status code: the class names the condition,
// where a status code names only its HTTP category. Both are matched, because
// a proxy in front of the cluster can produce a status with no JSON body at
// all, and a future cluster version could report a known class under a
// different code.
const (
	// ErrorClassNoSuchEntry is returned when a path does not exist.
	ErrorClassNoSuchEntry = "fs_no_such_entry_error"
	// ErrorClassEntryExists is returned when a name is already taken.
	ErrorClassEntryExists = "fs_entry_exists_error"
	// ErrorClassNoSuchInode is returned when a file ID does not resolve, as
	// distinct from a path that does not exist.
	ErrorClassNoSuchInode = "fs_no_such_inode_error"

	// Quota errors distinguish two different absences: the directory the quota
	// would be attached to, and the quota itself.
	ErrorClassQuotaInodeNotFound = "api_quotas_inode_not_found_error"
	ErrorClassQuotaLimitNotFound = "api_quotas_quota_limit_not_found_error"

	// ErrorClassNFSExportAlreadyExists is returned when an export_path is
	// already in use.
	ErrorClassNFSExportAlreadyExists = "nfs_export_already_exists_error"
	// ErrorClassTenantNotFound is returned when an export names a tenant the
	// cluster does not have. Observed verbatim on Core 7.9.3 as
	// "Connection tenant { id=99 } does not exist."
	ErrorClassTenantNotFound = "tenant_does_not_exist_error"
	// ErrorClassEmptyTenantInMultitenant is returned when an export names no
	// tenant on a cluster that has more than one. HTTP 400. Observed verbatim
	// on a multi-tenant cluster as "Tenant must be
	// specified when more than one tenant configured." A single-tenant cluster
	// accepts the same request and uses its only tenant.
	ErrorClassEmptyTenantInMultitenant = "empty_tenant_in_multitenant_environment_error"
	// ErrorClassNFSExportFSPathNotFound is returned when the directory an
	// export would serve does not exist.
	//
	// Note this arrives as HTTP 400, not 404, and is deliberately *not* part
	// of IsNotFound: it means "the thing I would have exported is missing",
	// which is a failed create, whereas IsNotFound callers treat a match as
	// "already gone" and report success.
	ErrorClassNFSExportFSPathNotFound = "nfs_export_fs_path_doesnt_exist_error"
	// ErrorClassNFSExportTreePrefix is returned when a new export's path would
	// nest inside an existing export's path.
	//
	// This is an NFSv4 constraint: v4 presents one pseudo-filesystem tree, so
	// overlapping exports are ambiguous and the cluster refuses them. A
	// cluster with v4 disabled permits the same nesting, which is why a
	// dedicated export per volume worked before v4 was enabled and stops
	// working after.
	//
	// It arrives as HTTP 409, so IsConflict matches it by status, but it does
	// NOT mean "this export already exists" -- it means the export table has
	// an ancestor in the way. Callers must check this class before treating a
	// 409 as an idempotent hit.
	ErrorClassNFSExportTreePrefix = "nfs_export_tree_name_is_prefix_error"
	// ErrorClassTreeDeleteAlreadyStarted is returned when a tree delete is
	// requested for a directory that already has one running. Observed
	// verbatim on Core 7.9.3 as HTTP 500 ("Tree delete job already started for
	// root (<id>)"): the request is a duplicate, not a failure, and the job it
	// names is the one the caller wanted.
	ErrorClassTreeDeleteAlreadyStarted = "tree_delete_already_started_error"

	// Replication error classes, each observed verbatim on Core 7.9.2.2.

	// ErrorClassIrreplicable is returned by make-writable on a relationship
	// that is already disconnected: "Disconnect and revert is not allowed on
	// a disconnected relationship". HTTP 400. It means the target was already
	// promoted, which for a retried promote is success, not failure.
	ErrorClassIrreplicable = "relationship_manager_irreplicable_error"
	// ErrorClassTargetPathNotEmpty is returned by authorize when the target
	// directory already holds data and allow-non-empty-directory was not set
	// (measured on the lab cluster, 2026-09-14, re-protecting a group after a
	// Disable). The remedy is the class's allowNonEmptyTarget.
	ErrorClassTargetPathNotEmpty = "relationship_manager_target_path_not_empty_error"
	// ErrorClassAlreadyAuthorized is returned by a repeat authorize on a
	// relationship the target already accepted (measured live, 2026-09-14).
	ErrorClassAlreadyAuthorized = "relationship_manager_already_authorized_error"
	// ErrorClassManualReplicationDisallowed is returned by replicate while
	// replication_enabled is true, in every replication mode. HTTP 400. The
	// array drains a frozen source on its own; a manual job is never the way
	// to force it.
	ErrorClassManualReplicationDisallowed = "replication_manual_replication_disallowed_error"

	// ErrorClassSnapshotNotFound is returned for a well-formed snapshot ID
	// that names no snapshot. Observed verbatim on Core 7.10.0 as "This
	// snapshot no longer exists." (the and captures).
	ErrorClassSnapshotNotFound = "snapshot_not_found_error"

	// ErrorClassDecode is returned when the cluster cannot parse a value out
	// of the request itself -- the field is the right name but the wrong
	// shape, so the request never reached the object it names.
	//
	// Measured on Core 7.10.0, 2026-09-20: GET /v2/snapshots/999999999999
	// answers HTTP 400 decode_error, where a well-formed but absent ID such
	// as 99999 answers snapshot_not_found_error. The distinction matters
	// because an ID the cluster cannot parse names nothing, which for a
	// lookup is the same answer as an ID that resolves to nothing.
	//
	// Deliberately NOT part of IsNotFound: this arrives as a 400 alongside
	// conditions that are genuine failures, so only the callers that know a
	// malformed identifier is harmless may match it.
	ErrorClassDecode = "decode_error"
)

// APIError is returned when the Qumulo REST API responds with a non-2xx
// status. Callers branch on the status code to implement CSI idempotency
// rules -- "already exists" and "already gone" are normal outcomes for a
// retried RPC, not failures -- so the code is preserved as a typed field
// rather than being flattened into an error string.
type APIError struct {
	StatusCode int
	// Path is the API path that produced the error, for diagnostics.
	Path string
	// Msg is the error description reported by the cluster, if any.
	//
	// Do not rely on this being human-readable. Qumulo Core frequently sets
	// description to a bare numeral and marks it user_visible: false, and the
	// value is not stable for a given condition -- two identical duplicate
	// -name requests were observed returning "20005" and "20011". Prefer
	// Class when deciding what to show or match on.
	Msg string
	// Class is the cluster's error_class, e.g. "fs_entry_exists_error". It
	// names the condition rather than its HTTP category, so it is both the
	// useful thing to log and a sharper signal to branch on than StatusCode.
	// Empty when the response carried no JSON body.
	Class string
}

// Error leads with the error class, because that is the field that actually
// identifies what went wrong. The description follows in parentheses when
// present: it is often just an opaque number, but for some conditions -- a
// missing path, for instance -- it names the component at fault, which is
// worth keeping in a log line.
func (e *APIError) Error() string {
	switch {
	case e.Class != "" && e.Msg != "":
		return fmt.Sprintf("qumulo api %s: HTTP %d: %s (%s)", e.Path, e.StatusCode, e.Class, e.Msg)
	case e.Class != "":
		return fmt.Sprintf("qumulo api %s: HTTP %d: %s", e.Path, e.StatusCode, e.Class)
	case e.Msg != "":
		return fmt.Sprintf("qumulo api %s: HTTP %d: %s", e.Path, e.StatusCode, e.Msg)
	default:
		return fmt.Sprintf("qumulo api %s: HTTP %d", e.Path, e.StatusCode)
	}
}

// MalformedReplyError is a reply the array sent and this client cannot use:
// a body that is not JSON, or not the JSON asked for, or a field it refuses
// to act on. The array answered, so it is neither an APIError (it said 2xx)
// nor a transport failure (*url.Error); the message is the wrapped error's,
// unchanged.
//
// It is typed so a caller can tell it from an array that did not answer. As
// "unreachable" it sent operators to the network for a healthy array, and in
// a promote it stood in for a silent peer, which licenses skipping the
// freeze and the final sync.
type MalformedReplyError struct {
	Err error
}

func (e *MalformedReplyError) Error() string { return e.Err.Error() }

func (e *MalformedReplyError) Unwrap() error { return e.Err }

// malformedReply wraps err as a MalformedReplyError.
func malformedReply(err error) error { return &MalformedReplyError{Err: err} }

// IsMalformedReply reports whether err is, or wraps, a reply the array sent
// that this client could not use.
func IsMalformedReply(err error) bool {
	var malformed *MalformedReplyError
	return errors.As(err, &malformed)
}

// apiErrorBody is the error envelope Qumulo Core returns alongside a non-2xx
// status. Every field is best-effort: a proxy or load balancer in front of
// the cluster may return an error with no JSON body at all.
type apiErrorBody struct {
	Description string `json:"description"`
	ErrorClass  string `json:"error_class"`
}

// notFoundClasses are the error classes that mean "this object is not there".
//
// Every one of these was observed as a 404 on a real cluster, so matching the
// status alone already handles them; they are listed anyway because the class
// is the sharper signal and a future Core release could report a known class
// under a different code.
var notFoundClasses = []string{
	ErrorClassNoSuchEntry,
	ErrorClassNoSuchInode,
	ErrorClassQuotaInodeNotFound,
	ErrorClassQuotaLimitNotFound,
}

// conflictClasses are the error classes that mean "this object already exists".
var conflictClasses = []string{
	ErrorClassEntryExists,
	ErrorClassNFSExportAlreadyExists,
}

// IsNotFound reports whether err says the object does not exist, by HTTP 404
// or by the cluster's own error class.
func IsNotFound(err error) bool {
	return hasStatus(err, http.StatusNotFound) || hasAnyErrorClass(err, notFoundClasses)
}

// IsFileNotFound reports whether err is the cluster saying a file or
// directory does not exist: one of its not-found error classes, whatever the
// status.
//
// Narrower than IsNotFound on purpose, and only for callers that act on
// "gone" by deleting nothing and reporting success. A 404 is not
// always that. Core answers an unknown route 404 http_not_found_error, and a
// proxy or ingress in front of the API can answer 404 with no Qumulo body at
// all, so a bare 404 on a lookup says the endpoint did not serve the file
// API, not that the file is gone. IsNotFound is left as it is: many callers
// depend on its breadth.
func IsFileNotFound(err error) bool {
	return hasAnyErrorClass(err, notFoundClasses)
}

// IsSnapshotNotFound is IsFileNotFound for a snapshot: the cluster's own
// "no such snapshot" class, not any 404.
func IsSnapshotNotFound(err error) bool {
	return HasErrorClass(err, ErrorClassSnapshotNotFound)
}

// IsMalformedIDAt reports whether err says the cluster could not parse an
// identifier out of a request to reqPath.
//
// Matched by class, never by HTTP 400: the cluster answers 400 for several
// conditions that are real failures -- a missing export path, a disallowed
// manual replication -- and treating those as "nothing there" would turn a
// failed operation into a reported success.
//
// The path is checked because the class alone is not enough either.
// decode_error is what Core 7.9.2.2 answers for a field it could not read out
// of a request BODY as well as for an identifier it could not read out of a
// URL -- a PUT missing its id answers decode_error "field 'id' not found"
// (measured twice on the lab arrays). A caller that treats
// every decode_error as "this object does not exist" therefore reports a real
// failure as an absent object, and no retry will ever correct it.
//
// So the error has to have come from the request whose identifier is in
// doubt. One raised anywhere else is somebody else's problem and is left
// alone.
func IsMalformedIDAt(err error, reqPath string) bool {
	if !HasErrorClass(err, ErrorClassDecode) {
		return false
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.Path == reqPath
}

// IsMalformedSnapshotID reports whether err is this cluster refusing to parse
// snapshotID out of the snapshot lookup it was used for.
//
// The path is built here rather than passed in: it is this package that knows
// the URL a snapshot GET uses, and a caller guessing at it would silently
// stop matching the day the route changes -- which, for a predicate whose
// false answer means "report a failure", is the safe direction to fail but
// still a bug worth not having. It is escaped exactly as GetSnapshot escapes
// it: an unescaped path never matched an ID that needed escaping, which is
// the kind of ID most likely to be refused.
func IsMalformedSnapshotID(err error, snapshotID string) bool {
	return IsMalformedIDAt(err, fmt.Sprintf(snapshotPath, url.PathEscape(snapshotID)))
}

// IsExportTreeConflict reports whether err says a new export would nest inside
// an existing one. Distinct from IsConflict on purpose: the object does not
// exist, and no retry will make this succeed.
func IsExportTreeConflict(err error) bool {
	return HasErrorClass(err, ErrorClassNFSExportTreePrefix)
}

// IsTreeDeleteAlreadyStarted reports whether err says a tree delete is
// already running for that directory.
func IsTreeDeleteAlreadyStarted(err error) bool {
	return HasErrorClass(err, ErrorClassTreeDeleteAlreadyStarted)
}

// IsConflict reports whether err says the object already exists, by HTTP 409
// or by the cluster's own error class.
func IsConflict(err error) bool {
	return hasStatus(err, http.StatusConflict) || hasAnyErrorClass(err, conflictClasses)
}

// hasAnyErrorClass reports whether err is an APIError carrying any of classes.
func hasAnyErrorClass(err error, classes []string) bool {
	class := ErrorClassOf(err)
	return class != "" && slices.Contains(classes, class)
}

// ErrorClassOf returns the cluster's error_class for err, or "" if err is not
// an APIError or carried no JSON body. It lets callers branch on a specific
// condition without matching on error strings.
func ErrorClassOf(err error) string {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return ""
	}
	return apiErr.Class
}

// HasErrorClass reports whether err is an APIError with the given error_class.
func HasErrorClass(err error, class string) bool {
	return class != "" && ErrorClassOf(err) == class
}

func hasStatus(err error, code int) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.StatusCode == code
}

// IsTenantNotFound reports whether err says the export named a tenant that does
// not exist on the cluster.
//
// Worth its own predicate because the fix is entirely in the operator's hands:
// a StorageClass names a tenant that is absent or was deleted, and no retry
// will conjure it. The caller turns this into InvalidArgument rather than a
// retriable Internal.
func IsTenantNotFound(err error) bool {
	return hasAnyErrorClass(err, []string{ErrorClassTenantNotFound})
}

// IsEmptyTenantInMultitenant reports whether err says the export named no
// tenant on a cluster with more than one. Like IsTenantNotFound it is a
// StorageClass problem that no retry fixes: the class has to set tenantID.
func IsEmptyTenantInMultitenant(err error) bool {
	return HasErrorClass(err, ErrorClassEmptyTenantInMultitenant)
}

// IsAlreadyAuthorized reports whether err says authorize was called on a
// relationship the target had already accepted. Measured on Core 7.9.2.2 as
// HTTP 400 relationship_manager_already_authorized_error when a repeat
// EnableVolumeReplication re-authorized a relationship whose source end had
// not yet reported ESTABLISHED. An enable that sees this has nothing left to
// do on this step.
func IsAlreadyAuthorized(err error) bool {
	return HasErrorClass(err, ErrorClassAlreadyAuthorized)
}

// IsAlreadyDisconnected reports whether err says make-writable was called on a
// relationship that is already disconnected -- that is, the target was already
// made writable by an earlier call. A promote that sees this has nothing left
// to do.
func IsAlreadyDisconnected(err error) bool {
	return HasErrorClass(err, ErrorClassIrreplicable)
}

// IsManualReplicationDisallowed reports whether err says the array refused a
// manual replication job because automatic replication is enabled.
func IsManualReplicationDisallowed(err error) bool {
	return HasErrorClass(err, ErrorClassManualReplicationDisallowed)
}
