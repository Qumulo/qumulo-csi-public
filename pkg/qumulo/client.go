// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package qumulo is a minimal client for the Qumulo Core REST API, used by
// the CSI driver to check cluster reachability and to provision and manage
// NFS-exported directories as CSI volumes.
package qumulo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

// Qumulo Core REST endpoints, verified against the Qumulo REST API Guide.
//
// Note there is no "/api" prefix -- paths begin directly at the version
// segment -- and that the version segment differs per resource family:
// snapshots are on v3 (v1 is legacy and v4 exposes status only) while files
// and quotas are on v1. What still needs confirming against a given cluster
// is the Core release it runs, not the shape of these paths; they are
// collected here so a version bump is a single edit.
const (
	// version answers with no credential at all -- it proves the API is
	// reachable, nothing about the credential. See HealthCheck.
	versionPath = "/v1/version"
	// nodeStatePath answers with the cluster's own UUID. Core serves it with
	// no credential, whatever token is sent (measured on 7.9.3 and 7.10.0).
	nodeStatePath = "/v1/node/state"
	// cluster-wide filesystem capacity.
	fileSystemPath = "/v1/file-system"
	// entries under a directory; %s is a file reference (see fileRef).
	fileEntriesPath = "/v1/files/%s/entries/"
	// attributes of a single file; %s is a file reference.
	fileAttributesPath = "/v1/files/%s/info/attributes"
	// recursive usage of a directory tree; %s is a file reference.
	fileAggregatesPath = "/v1/files/%s/aggregates/"
	// batch resolution of file IDs to paths.
	filesResolvePath = "/v1/files/resolve"
	// server-side copy of one file's contents, optionally from a snapshot.
	fileCopyChunkPath = "/v1/files/%s/copy-chunk"
	// cluster-wide NFS configuration. Only v2 serves this resource: v1 and v3
	// both answer 404, so there is no fallback to try.
	nfsSettingsPath = "/v2/nfs/settings"
	// recursive directory deletion, run asynchronously as a job.
	treeDeleteJobsPath = "/v1/tree-delete/jobs/"
	treeDeleteJobPath  = "/v1/tree-delete/jobs/%s"
	// directory quota collection and single-quota resource.
	quotasPath = "/v1/files/quotas/"
	quotaPath  = "/v1/files/quotas/%s"
	// quotaStatusPath is the second of the two documents Core serves for a
	// quota. The plain object above is id and limit; only this one carries
	// capacity_usage. Confirmed live on Core 7.9.2.2: a GET of the quota
	// collection returns {id, limit} with no usage, a PUT answers the same,
	// and the status document is where the figure lives.
	quotaStatusPath = "/v1/files/quotas/status/%s"
	// snapshot collection and single-snapshot resource.
	snapshotsPath = "/v3/snapshots/"
	snapshotPath  = "/v3/snapshots/%s"
	// snapshotChangesSincePath lists what differs between two snapshots of
	// one tree; a planned promote's drain reads it (SnapshotDiffEmpty).
	snapshotChangesSincePath = "/v2/snapshots/%s/changes-since/%s"
	// NFS export collection and single-export resource. v3 rather than v2:
	// both are served, but v2 answers a listing with a bare JSON array where
	// v3 uses the {"entries": [...]} envelope this client already decodes, and
	// v2 still carries the deprecated present_64_bit_fields_as_32_bit field.
	// Note the single-export resource is addressed by ID only on v3, unlike v2
	// which accepts a path reference too.
	nfsExportsPath = "/v3/nfs/exports/"
	// Multitenancy is a v1 API even on 7.9.3; there is no v2 or v3 of it.
	tenantsPath   = "/v1/multitenancy/tenants/"
	nfsExportPath = "/v3/nfs/exports/%s"
	// The network *configuration*, which is what the driver reads. Not
	// /v3/network/status, which reports live per-node state and is a
	// different resource -- and which the Core 7.9.3 docker prototype answers
	// with a 500 because a helper script is missing from the image.
	networkConfigPath = "/v3/network"
)

// snapshotFilterExcludeInDelete restricts a snapshot listing to snapshots
// that are not already being deleted. The API marks this filter required,
// and excluding in-delete snapshots is also what the driver wants: adopting
// one as an idempotent hit would hand the CO a snapshot about to vanish.
const snapshotFilterExcludeInDelete = "exclude_in_delete"

// VersionInfo is the cluster's build metadata, used only to prove the REST
// API answered.
type VersionInfo struct {
	RevisionID string `json:"revision_id"`
	BuildID    string `json:"build_id"`
}

// FileSystemCapacity is the cluster's total and free space, in bytes.
//
// This is cluster-wide, not scoped to any directory: a Qumulo directory
// quota is a logical limit with no capacity check behind it, so no
// StorageClass's basePath corresponds to a distinct pool of space. Every
// class on the cluster is reporting against the same number.
type FileSystemCapacity struct {
	TotalSizeBytes uint64
	FreeSizeBytes  uint64
}

// FileInfo describes a file or directory on the cluster.
//
// ID is the load-bearing field: it is stable across renames and is what
// every subsequent operation (quota, tree-delete, snapshot) keys off, which
// is why the CSI volume ID embeds it rather than the path.
type FileInfo struct {
	ID   string
	Path string
	Type string
	// ChildCount is the number of entries in a directory, as the array
	// reports it with the attributes; nil when the answer carried none.
	// CreateVolume reads it to tell its own interrupted create -- an empty
	// directory with no quota -- from a directory holding data.
	ChildCount *int
	// Owner and Group are the entry's owner_details and group_details: the
	// identity kind and value, which say whether replication can carry the
	// entry. Zero when the answer carried none.
	Owner Identity
	Group Identity
	// Size is the entry's size in bytes, nil when the answer carried none or
	// it could not be read as a number. A pointer because "the array did not
	// say" and "zero bytes" are different facts and a restore's verification
	// acts on only one of them.
	Size *uint64
}

// IsDirectory reports whether the entry is a directory.
func (f FileInfo) IsDirectory() bool {
	return f.Type == fileTypeDirectory
}

const fileTypeDirectory = "FS_FILE_TYPE_DIRECTORY"

// fileTypeFile is a regular file, the only entry type whose size a restore
// copies by.
const fileTypeFile = "FS_FILE_TYPE_FILE"

// SnapshotInfo describes a Qumulo snapshot.
//
// Name is assigned by the cluster, not by the caller: a snapshot is created
// with a name *suffix*, and Qumulo prepends the snapshot ID to it. Callers
// matching a snapshot they created must account for that prefix.
type SnapshotInfo struct {
	ID           string
	Name         string
	SourceFileID string
	CreatedAt    time.Time
	// InDelete reports that the cluster is in the process of deleting this
	// snapshot; it will disappear shortly.
	InDelete bool
	// PolicyID is the snapshot policy that created this snapshot, or "" when
	// it was created directly by a caller. The API documents the field as null
	// for a manually created snapshot.
	PolicyID string
	// Expiration is when the cluster deletes this snapshot on its own, or
	// the zero time when it never does.
	Expiration time.Time
}

// revertSnapshotMarker is the name a Qumulo array gives the snapshot it takes
// of a replication target's root before reverting it -- on make-writable and
// on reconnect -- after its own "<id>_" prefix:
// "53580_revert_to_53578_for_csi-qfs-z2", measured on the lab arrays
// The array sets no expiration on it.
const revertSnapshotMarker = "revert_to_"

// IsReplicationRevertSnapshot reports whether the array took this snapshot
// itself, to keep what a replication revert discarded.
func (s SnapshotInfo) IsReplicationRevertSnapshot() bool {
	return !s.CreatedByPolicy() &&
		(strings.HasPrefix(s.Name, s.ID+"_"+revertSnapshotMarker) || strings.HasPrefix(s.Name, revertSnapshotMarker))
}

// CreatedByPolicy reports whether the cluster's snapshot policy engine created
// this snapshot, rather than a caller asking for one directly.
//
// This is the only reliable way to tell a driver-created snapshot from a
// scheduled one: names are not sufficient, since a policy names its snapshots
// from a template an administrator controls.
//
// A "0" policy ID counts as none. The API documents null for a manual
// snapshot, but a numeric field rendered as 0 to mean "unset" is common enough
// to guard against, and getting this backwards is asymmetric: misreading a
// manual snapshot as policy-created would stop the driver ever recognizing its
// own snapshot on a retry, and pile up a duplicate per attempt.
func (s SnapshotInfo) CreatedByPolicy() bool {
	return s.PolicyID != "" && s.PolicyID != "0"
}

// MatchesNameSuffix reports whether this snapshot was created with the given
// name suffix.
//
// Qumulo prepends the snapshot's own decimal ID and an underscore to the
// suffix a caller supplies, so a request for "foo" on snapshot 16 is stored as
// "16_foo" and never equals what was asked for. Anchoring on the whole
// "<id>_<suffix>" form is what keeps an unrelated snapshot whose name merely
// *ends* in the suffix from being mistaken for this one -- with the ID as the
// prefix, an accidental match is impossible.
func (s SnapshotInfo) MatchesNameSuffix(suffix string) bool {
	return s.Name == s.ID+"_"+suffix
}

// ExportInfo describes an NFS export on the cluster.
//
// FSPath is the directory the export serves and ExportPath is the path clients
// mount; the two are independent, and for a driver-created export they happen
// to be derived from the same volume name. ID is a string, like file and quota
// IDs and unlike snapshot IDs.
type ExportInfo struct {
	ID         string
	ExportPath string
	FSPath     string
	// TenantID is the network tenant this export is attached to. The cluster
	// assigns 1 (the Default tenant) when an export is created without one,
	// but only on a single-tenant cluster: with more than one tenant it
	// refuses the create (ErrorClassEmptyTenantInMultitenant).
	TenantID int
	// Description is the export's free-text label. This driver writes
	// "csi.qumulo.com volume <name>" on every export it creates, so it is how
	// DeleteVolume tells a volume's own export from an operator's.
	Description string
	// ReadOnly reports that every restriction on the export refuses writes.
	// A replica's export is pre-staged this way and flipped at promote, so
	// the flag is how a promote tells "already flipped" from "still staged".
	ReadOnly bool
	// AuthenticationMode is read from the first restriction, which is the
	// only one this driver ever writes.
	//
	// AllowedClients is the union of every restriction's host list, because
	// the cluster is free to answer one written restriction as several --
	// see toExportInfo. An empty list means the export admits every host.
	//
	// Both are carried so a replica's export can be pre-staged from the
	// source's definition rather than from a StorageClass the request does
	// not have.
	AuthenticationMode string
	AllowedClients     []string
}

// TenantInfo is one entry from the cluster's multitenancy configuration.
//
// A tenant is a network namespace: an export attached to it is reachable only
// on that tenant's networks. NFSEnabled matters because a tenant with NFS
// switched off accepts an export and then serves nothing, which looks like a
// driver bug from the pod's side.
type TenantInfo struct {
	ID         int
	Name       string
	NFSEnabled bool
}

// NetworkInfo is one frontend network from the cluster's network
// configuration.
//
// FloatingAddresses is what a client actually mounts from: the addresses that
// move between nodes, which is why an export in this network is reachable at
// one of them and nowhere else. It is empty when the network uses an address
// assignment this driver does not decode -- which is not the same as a
// network with no addresses, and callers must not read it as one.
type NetworkInfo struct {
	ID       int
	Name     string
	TenantID int
	// FloatingAddresses holds the floating IP ranges exactly as the cluster
	// spells them: a bare address, or a range in either "a.b.c.d-e" or
	// "a.b.c.d-w.x.y.z" form.
	FloatingAddresses []string
	// AddressType is the cluster's own name for the assignment scheme
	// ("HOST", "STATIC", ...), carried so a caller that decoded no addresses
	// can say which scheme it did not understand.
	AddressType string
}

// ClientInterface is the subset of Qumulo REST API operations the CSI
// driver depends on. It exists so driver code can be tested against a fake
// implementation without making real HTTP calls.
type ClientInterface interface {
	// HealthCheck returns an error unless the Qumulo REST API is reachable.
	// It proves reachability only -- /v1/version answers with no
	// credential, so a failure here says nothing about whether this
	// client's token is valid. See GetCapacity or GetNFSSettings for a
	// call that does.
	HealthCheck(ctx context.Context) error
	// GetCapacity reports the cluster's total and free space, in bytes.
	GetCapacity(ctx context.Context) (FileSystemCapacity, error)

	// CreateDirectory creates a directory named name under parentPath. It
	// returns an error satisfying IsConflict if the name is already taken.
	CreateDirectory(ctx context.Context, parentPath, name string) (FileInfo, error)
	// GetNFSSettings reads the cluster's NFS configuration, so the driver can
	// refuse a protocol the cluster cannot serve.
	GetNFSSettings(ctx context.Context) (NFSSettings, error)
	// ListDirectory lists the entries under a path, optionally as of a
	// snapshot. Used by a restore to walk an immutable source tree.
	ListDirectory(ctx context.Context, path, snapshotID string) ([]DirEntry, error)
	// CreateFile creates an empty file, which copy-chunk requires as its
	// target.
	CreateFile(ctx context.Context, parentPath, name string) (FileInfo, error)
	// CreateLink creates name under parentPath as a hard link to the file at
	// existingPath, so a restore can rebuild a hard-linked file as one inode.
	CreateLink(ctx context.Context, parentPath, name, existingPath string) (FileInfo, error)
	// CopyFile copies a file's whole contents server-side, optionally from a
	// snapshot version of the source. It loops internally: one copy-chunk
	// call copies only a chunk.
	CopyFile(ctx context.Context, targetPath, sourceID, sourceSnapshot string, offset uint64) error
	// CreateExpiringSnapshot creates a snapshot that the cluster removes at
	// expiration on its own. Used for the freeze marker a planned promote
	// takes of the frozen source tree, so a marker nobody deleted
	// cannot outlive the failover it belonged to.
	CreateExpiringSnapshot(ctx context.Context, fileID, nameSuffix string, expiration time.Time) (SnapshotInfo, error)
	// SnapshotDiffEmpty reports whether nothing changed between two
	// snapshots of the same tree (GET /v2/snapshots/{newer}/changes-since/
	// {older}, first page only). It is the data-level answer to "does the
	// target hold everything the frozen source holds".
	SnapshotDiffEmpty(ctx context.Context, newerID, olderID string) (bool, error)
	// Rename moves a path, which is how a restore commits atomically.
	Rename(ctx context.Context, parentPath, name, oldPath string) (FileInfo, error)
	// LookupFileByPath resolves an absolute cluster path to a FileInfo. It
	// returns an error satisfying IsNotFound if the path does not exist.
	LookupFileByPath(ctx context.Context, path string) (FileInfo, error)
	// ResolveFileIDs maps file IDs to their canonical paths in one call.
	// IDs that do not resolve are absent from the returned map rather than
	// being an error, so one stale ID does not fail the whole batch.
	ResolveFileIDs(ctx context.Context, fileIDs []string) (map[string]string, error)

	// ClusterID returns the cluster's UUID (GET /v1/node/state), the one
	// identity two endpoints of the same cluster share. It needs no
	// privilege.
	ClusterID(ctx context.Context) (string, error)
	// SetFileAttributesByID applies the non-empty fields of attrs to the file
	// identified by fileID. A mode is an octal string as the API renders it,
	// e.g. "0777".
	SetFileAttributesByID(ctx context.Context, fileID string, attrs FileAttributes) error
	// SetFileAttributes applies the non-empty fields of attrs to the file at
	// filePath, leaving the others untouched.
	SetFileAttributes(ctx context.Context, filePath string, attrs FileAttributes) error

	// StartTreeDelete asks the cluster to begin recursively deleting the
	// directory identified by fileID. The walk runs asynchronously: a nil
	// return means the job was accepted, not that the tree is gone.
	StartTreeDelete(ctx context.Context, fileID string) error
	// TreeDeleteFinished reports whether the deletion job for fileID has
	// finished. A job that never existed also reports finished, since the
	// cluster does not distinguish the two.
	TreeDeleteFinished(ctx context.Context, fileID string) (bool, error)

	// CreateQuota attaches a directory quota of limitBytes to fileID.
	CreateQuota(ctx context.Context, fileID string, limitBytes uint64) error
	// GetQuota returns the quota limit in bytes attached to fileID.
	GetQuota(ctx context.Context, fileID string) (uint64, error)
	// GetQuotaInfo returns the quota's limit and how much of it is used.
	// Usage is what lets GetCapacity report remaining space rather than a
	// ceiling, and what gives CreateSnapshot a size to report.
	GetQuotaInfo(ctx context.Context, fileID string) (QuotaInfo, error)
	// UpdateQuota changes the quota limit attached to fileID.
	UpdateQuota(ctx context.Context, fileID string, limitBytes uint64) error
	// GetTreeUsage returns what a directory tree holds -- the bytes it
	// occupies as the quota engine counts them (data and metadata), and its
	// file and directory counts -- in the given snapshot or, with snapshotID
	// empty, live.
	GetTreeUsage(ctx context.Context, fileID, snapshotID string) (TreeUsage, error)
	// DeleteQuota removes the quota attached to fileID.
	DeleteQuota(ctx context.Context, fileID string) error

	// CreateSnapshot snapshots the directory fileID. nameSuffix is a
	// suffix, not the final name -- see SnapshotInfo.
	CreateSnapshot(ctx context.Context, fileID, nameSuffix string) (SnapshotInfo, error)
	// DeleteSnapshot removes a snapshot by ID.
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	// ListSnapshots returns the cluster's snapshots, excluding those already
	// being deleted. Paging is applied by the caller, since the Qumulo API
	// does not expose the cursor semantics CSI's ListSnapshots requires.
	ListSnapshots(ctx context.Context) ([]SnapshotInfo, error)
	// GetSnapshot returns a single snapshot by ID.
	GetSnapshot(ctx context.Context, snapshotID string) (SnapshotInfo, error)

	// CreateExport creates an NFS export serving fsPath at exportPath. The
	// directory at fsPath must already exist -- the cluster will not create it
	// -- and an exportPath already in use returns an error satisfying
	// IsConflict.
	// CreatePolicy attaches a snapshot policy to a directory, so a volume
	// gets scheduled snapshots without the CO asking for each one.
	CreatePolicy(ctx context.Context, name, sourceFileID string, schedule Schedule) (PolicyInfo, error)
	// ListPolicies returns every snapshot policy on the cluster, which is how
	// a retried provision finds the policy a previous attempt already made.
	ListPolicies(ctx context.Context) ([]PolicyInfo, error)
	// DeletePolicy removes a policy, so deleting a volume does not leave a
	// schedule pointing at a directory that no longer exists.
	DeletePolicy(ctx context.Context, policyID string) error

	CreateExport(ctx context.Context, spec ExportSpec) (ExportInfo, error)
	// ListTenants returns the cluster's network tenants.
	ListTenants(ctx context.Context) ([]TenantInfo, error)
	// ListNetworks returns the cluster's frontend networks, each naming the
	// tenant it belongs to.
	ListNetworks(ctx context.Context) ([]NetworkInfo, error)
	// DeleteExport removes an export by ID.
	DeleteExport(ctx context.Context, exportID string) error
	// ListExports returns every NFS export on the cluster. There is no
	// server-side filter by path, so callers that want one filter locally.
	ListExports(ctx context.Context) ([]ExportInfo, error)
}

// Client is an HTTP client for the Qumulo Core REST API.
type Client struct {
	// BaseURL is the cluster's REST API base, e.g. "https://qumulo.example.com".
	BaseURL string
	// TokenFunc supplies the bearer token for each request. It is called per
	// request rather than captured once so a token rotated in a mounted
	// Kubernetes Secret takes effect without restarting the pod. A nil
	// TokenFunc sends no Authorization header.
	TokenFunc func() (string, error)
	// HTTPClient is the underlying HTTP client. If nil, a client with a
	// sane default timeout is used.
	HTTPClient *http.Client
	// Observer, when non-nil, is told the outcome of every call this client
	// makes. It is set once at construction and never mutated, so the one
	// implementation is shared by every per-request client; nil means
	// nothing is observed and no behaviour changes. See Observer.
	Observer Observer
	// Breaker, when non-nil, fails calls to an array that has stopped
	// answering at once, for a while, rather than at the request timeout.
	// Shared by every client the driver builds; nil means none.
	Breaker *Breaker
}

var _ ClientInterface = (*Client)(nil)

// New creates a Qumulo Core REST API client using a fixed token.
func New(baseURL, token string) *Client {
	return NewWithTokenFunc(baseURL, func() (string, error) { return token, nil })
}

// NewWithTokenFunc creates a client that re-reads its token per request.
// Use it with a reader over the mounted Secret path so credential rotation
// needs no pod restart.
func NewWithTokenFunc(baseURL string, tokenFunc func() (string, error)) *Client {
	return &Client{
		BaseURL:   baseURL,
		TokenFunc: tokenFunc,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// noRedirectClient is httpClient that never follows a redirect. Go re-sends
// the Authorization header to a redirect target on the same host -- another
// port, or http:// -- so an https-to-http redirect sent the token in
// cleartext. The Qumulo API answers in place; a redirect is answered
// as the 3xx it is, which the caller sees as an APIError. The copy shares the
// Transport, so connections are pooled as before.
func (c *Client) noRedirectClient() *http.Client {
	client := *c.httpClient()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &client
}

// resolve builds the URL for an API path and refuses one that leaves this
// client's endpoint.
//
// Paths are appended to BaseURL as strings, and one of them -- a listing's
// paging.next -- comes from the array's answer. A cursor such as
// "@other:8000/..." turned the base host into userinfo and sent the next page,
// and the bearer token with it, to another host. A path that resolves
// to any other scheme or host, or carries userinfo, is refused before the
// request is built.
func (c *Client) resolve(path string) (string, error) {
	target := c.BaseURL + path
	base, err := url.Parse(c.BaseURL)
	if err != nil {
		return "", fmt.Errorf("the endpoint %q is not a URL: %w", c.BaseURL, err)
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("building request for %s: %w", path, err)
	}
	if u.User != nil || !strings.EqualFold(u.Scheme, base.Scheme) || !strings.EqualFold(u.Host, base.Host) {
		return "", fmt.Errorf("refusing to send a request for %q: it resolves to %s://%s, not this client's "+
			"endpoint %s, and every request carries the API token", path, u.Scheme, u.Host, c.BaseURL)
	}
	return target, nil
}

// token resolves the bearer token for a request.
func (c *Client) token() (string, error) {
	if c.TokenFunc == nil {
		return "", nil
	}
	return c.TokenFunc()
}

// fileRef renders an absolute cluster path as a Qumulo file reference. The
// API accepts either a numeric file ID or a percent-encoded path in the same
// position, and PathEscape encodes the separators, which is what
// distinguishes a path reference from an ID.
func fileRef(path string) string {
	return url.PathEscape(path)
}

// do issues a request against the Qumulo API and decodes a JSON response.
//
// body, if non-nil, is JSON-encoded as the request body. out, if non-nil,
// receives the decoded response. A non-2xx status becomes an *APIError so
// callers can branch on the status code.
//
// A POST sent through do is NOT retried; see doRetryablePost for the call
// sites that opt back in.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	return c.doMaybeRetrying(ctx, method, path, body, out, method != http.MethodPost)
}

// doRetryablePost is do for a POST whose repeat is harmless, so that a 429 or
// 503 is retried here rather than failing the caller.
//
// Only a call site that can say why a second POST is safe belongs here, and
// the reason is recorded at the call site. Two kinds qualify: the array
// refuses the repeat with a collision the caller already adopts (a 409 on a
// directory, file, link, quota or export create), and the operation is
// naturally idempotent (copy-chunk copies the same bytes to the same offsets;
// resolve is a read; tree-delete already maps "already started" to success).
func (c *Client) doRetryablePost(ctx context.Context, path string, body, out any) error {
	return c.doMaybeRetrying(ctx, http.MethodPost, path, body, out, true)
}

func (c *Client) doMaybeRetrying(ctx context.Context, method, path string, body, out any, retryable bool) error {
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			return fmt.Errorf("encoding request body for %s: %w", path, err)
		}
	}

	// A 429 or 503 is the array saying "not now", the most clearly transient
	// answer it gives. It used to end a CreateVolume as a final Internal,
	// which csi-provisioner reports in a ProvisioningFailed Event. It
	// is retried here, a bounded number of times, honouring Retry-After, and
	// never past the caller's deadline.
	//
	// Retrying is only correct where sending the request twice is harmless,
	// so it is opt-in per call site for POST and on by default for the
	// others. This used to read "a create that did land before its 503
	// answers 409 on the retry, which every create path already adopts",
	// which was never checked and is false for snapshots: the name a caller
	// supplies is a label, not a key -- the array prepends the snapshot's own
	// ID (see SnapshotInfo.MatchesNameSuffix) -- so a repeat of a POST that
	// did land creates a SECOND snapshot under the same label. Kubernetes
	// then holds one and the other is orphaned with no expiry and no policy,
	// which nothing deletes and nothing reports (reproduced live).
	//
	// A POST not retried here is not a failure the caller swallows: it
	// surfaces as Unavailable, the CO retries the whole RPC, and the RPC
	// re-reads state before it re-sends -- which is where the idempotency
	// check lives (findSnapshotByName). That is one sidecar round trip on a
	// rare event, in exchange for never leaking a snapshot.
	for attempt := 0; ; attempt++ {
		retryAfter, err := c.doOnce(ctx, method, path, encoded, body != nil, out)
		wait, retry := retryWait(err, retryAfter, attempt)
		if !retry || !retryable {
			return err
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < wait+time.Second {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
	}
}

// maxDrainBytes bounds how much of an unwanted response body is read so that
// its connection can be reused. Past it, closing the connection is cheaper
// than reading on.
const maxDrainBytes = 256 << 10

// retryAttempts is how many times a 429 or 503 is tried in all.
const retryAttempts = 3

// maxRetryWait caps one wait, Retry-After included: the RPC around the call
// has its own deadline, and an array asking for minutes is better answered
// Unavailable, which the sidecar backs off on.
const maxRetryWait = 5 * time.Second

// retryWait says whether err is a 429 or 503 worth another attempt, and how
// long to wait first: the array's Retry-After when it sent one, otherwise
// 250 ms, then 1 s, each with up to 50% jitter.
func retryWait(err error, retryAfter time.Duration, attempt int) (time.Duration, bool) {
	var apiErr *APIError
	if attempt+1 >= retryAttempts || !errors.As(err, &apiErr) ||
		(apiErr.StatusCode != http.StatusTooManyRequests && apiErr.StatusCode != http.StatusServiceUnavailable) {
		return 0, false
	}
	wait := retryAfter
	if wait <= 0 {
		base := []time.Duration{250 * time.Millisecond, time.Second}[min(attempt, 1)]
		wait = base + time.Duration(rand.Int64N(int64(base/2)))
	}
	return min(wait, maxRetryWait), true
}

// parseRetryAfter reads a Retry-After header in its seconds form; the date
// form, and anything unreadable, count as absent.
func parseRetryAfter(h string) time.Duration {
	if secs, err := strconv.Atoi(strings.TrimSpace(h)); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

// doOnce is one round trip of do. It returns the response's Retry-After, if
// any, beside the error.
func (c *Client) doOnce(ctx context.Context, method, path string, encoded []byte, hasBody bool, out any) (time.Duration, error) {
	var reqBody io.Reader
	if hasBody {
		reqBody = bytes.NewReader(encoded)
	}

	target, err := c.resolve(path)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reqBody)
	if err != nil {
		return 0, fmt.Errorf("building request for %s: %w", path, err)
	}

	token, err := c.token()
	if err != nil {
		return 0, fmt.Errorf("reading qumulo api token for %s: %w", path, err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if hasBody {
		req.Header.Set("Content-Type", "application/json")
	}

	call, err := c.Breaker.allow(req.URL.Host, failFastAllowed(ctx))
	if err != nil {
		c.observeRequest(ctx, method, path, StatusTransport, false, 0)
		return 0, &url.Error{Op: method, URL: path, Err: err}
	}
	start := time.Now()
	resp, err := c.noRedirectClient().Do(req)
	if err != nil {
		c.Breaker.record(call, err, ctx.Err() == nil)
		// No status code exists for a transport failure, and "the array did
		// not answer" is the single most useful thing an operator can be
		// told, so it gets a status of its own rather than being lost.
		c.observeRequest(ctx, method, path, StatusTransport, false, time.Since(start))
		return 0, fmt.Errorf("calling %s: %w", path, err)
	}

	// The breaker's verdict waits for the body, not just the headers. An
	// array that answers a status line and then stalls, or drops the
	// connection mid-body, counted as a healthy call: the breaker reset its
	// failure count on every one of them, so a half-dead array never
	// tripped it and every caller paid the full timeout. bodyErr is
	// set below for exactly that case -- a transport failure reading the
	// body, never a reply this client merely could not parse, which says
	// the array is answering fine.
	var bodyErr error
	defer func() { c.Breaker.record(call, bodyErr, ctx.Err() == nil) }()
	// Read to EOF before closing, or the transport discards the connection
	// instead of reusing it: a call whose answer is unread (out == nil, an
	// error body decoded only in part, the newline after a decoded document)
	// used to cost a fresh TCP and TLS session on the next call. The
	// drain is bounded, so a huge unwanted body is cut off rather than read.
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes))
		resp.Body.Close()
	}()

	// A body no answer of this API's ever approaches. Without it a reply
	// that never ends -- a proxy streaming, an array looping -- was decoded
	// until the caller's deadline or the controller's memory ran out.
	// Error bodies are read through it too: they are the array's answer as
	// much as a 200's is.
	body := &cappedBody{r: resp.Body, left: maxResponseBytes}

	// Observed before the status is judged, so a 404 or a 500 is counted
	// exactly like a 200: this records what the array said, and the caller
	// decides what it means.
	c.observeRequest(ctx, method, path, strconv.Itoa(resp.StatusCode), true, time.Since(start))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		apiErr, readErr := newAPIError(resp.StatusCode, body, path)
		// The status is the answer callers act on, and it arrived; a body
		// that then stalled or dropped is still the array failing, and the
		// breaker has to see it.
		bodyErr = readErr
		return parseRetryAfter(resp.Header.Get("Retry-After")), apiErr
	}

	if out == nil {
		// Nothing to decode, but the body still has to end. A reply whose
		// headers arrived and whose body then stalled or dropped was
		// recorded as a healthy call on every route that wants no answer
		// (DeleteQuota, DeleteSnapshot, a PATCH...), so an array answering
		// headers and nothing else never opened the breaker on them.
		// The caller still gets nil: the status said the operation was done.
		// The drain is bounded as below; a long unwanted body is not an
		// error, only one that fails mid-read.
		if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes)); err != nil {
			bodyErr = err
		}
		return 0, nil
	}

	decoder := json.NewDecoder(body)
	if err := decoder.Decode(out); err != nil {
		// A body that is not JSON, or not this JSON, is a malformed answer:
		// the array answered, and it is typed as such (MalformedReplyError)
		// so no caller reads it as an outage. So is one past the cap:
		// it is complete as far as this client cares, and it used to surface
		// as an "unexpected EOF" transport failure that opened the breaker
		// on a cluster that was answering. Anything else -- the body
		// stalled past the timeout, or the connection dropped mid-body -- is
		// the array not finishing its answer, which is the transport failing,
		// and is reported as one: a *url.Error, what every caller already
		// treats as "the array did not answer". It used to end a CreateVolume
		// as a final Internal.
		if isMalformedBody(err) {
			return 0, malformedReply(fmt.Errorf("decoding response from %s: %w", path, err))
		}
		bodyErr = err
		return 0, &url.Error{Op: method, URL: path, Err: fmt.Errorf("reading the response body: %w", err)}
	}

	// One JSON document, and nothing after it. Two concatenated documents,
	// or a document followed by anything at all, is not this API answering:
	// it is a proxy, a captive portal or a truncated stream, and decoding
	// only the first of them took whichever one came first as the answer.
	more, err := decoderHasTrailingData(decoder)
	switch {
	case errors.Is(err, errReplyTooLarge):
		return 0, malformedReply(fmt.Errorf("decoding response from %s: %w", path, err))
	case err != nil:
		bodyErr = err
		return 0, &url.Error{Op: method, URL: path, Err: fmt.Errorf("reading the response body: %w", err)}
	case more:
		return 0, malformedReply(fmt.Errorf("decoding response from %s: the body carries more than one JSON document", path))
	}
	return 0, nil
}

// maxResponseBytes caps a single decoded response. The largest answer this
// client asks for is a 1,000-entry directory page, a few hundred kilobytes;
// 64 MiB is far past any of them and far short of a controller's memory.
const maxResponseBytes = 64 << 20

// errReplyTooLarge is a reply past maxResponseBytes.
var errReplyTooLarge = fmt.Errorf("the reply exceeds %d MiB", maxResponseBytes>>20)

// cappedBody reads at most left bytes of r, and then fails with
// errReplyTooLarge if r has more. Not io.LimitReader: that ends a long body
// with a plain EOF, which the decoder reports as "unexpected EOF" -- the very
// error a connection dropped mid-body gives -- so an answer that was merely
// too big counted as the transport failing and opened the breaker.
type cappedBody struct {
	r    io.Reader
	left int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		// One byte past the cap says whether there is more; reading it is
		// what tells a body of exactly the cap from one over it.
		var probe [1]byte
		n, err := b.r.Read(probe[:])
		if n > 0 {
			return 0, errReplyTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.r.Read(p)
	b.left -= int64(n)
	return n, err
}

// isMalformedBody reports whether a decode error says the body is unusable
// rather than unfinished: not JSON, not this JSON, empty, or past the cap.
func isMalformedBody(err error) bool {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	return errors.As(err, &syntaxErr) || errors.As(err, &typeErr) ||
		errors.Is(err, io.EOF) || errors.Is(err, errReplyTooLarge)
}

// decoderHasTrailingData reports whether anything but whitespace follows the
// document just decoded. An error here is the body failing mid-read, which
// the caller reports as the transport failure it is.
func decoderHasTrailingData(decoder *json.Decoder) (bool, error) {
	_, err := decoder.Token()
	switch {
	case errors.Is(err, io.EOF):
		return false, nil
	case err != nil:
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) {
			// Trailing bytes that are not even JSON.
			return true, nil
		}
		return false, err
	}
	return true, nil
}

// observeRequest reports one round trip, if anyone is listening.
//
// Every observation in this package is guarded like this: a client with no
// Observer does exactly what it did before metrics existed.
func (c *Client) observeRequest(
	ctx context.Context,
	method, path, status string,
	reachable bool,
	elapsed time.Duration,
) {
	if c.Observer == nil {
		return
	}
	c.Observer.ObserveRequest(ctx, RequestEvent{
		Cluster:      ClusterLabel(c.BaseURL),
		Method:       method,
		PathTemplate: PathTemplate(path),
		Status:       status,
		Duration:     elapsed,
		Reachable:    reachable,
	})
}

// observeQuota reports a quota this client read or wrote.
func (c *Client) observeQuota(ctx context.Context, fileID string, info QuotaInfo) {
	if c.Observer == nil {
		return
	}
	c.Observer.ObserveQuota(ctx, QuotaEvent{
		Cluster:    ClusterLabel(c.BaseURL),
		Subject:    SubjectFrom(ctx),
		FileID:     fileID,
		LimitBytes: info.Limit,
		UsedBytes:  info.Usage,
		UsedKnown:  info.UsageKnown,
	})
}

// ObserveVolumeProtection reports whether the volume this request is about is
// covered by a replication relationship.
//
// Exported and called by the driver rather than emitted from inside a REST
// call, because the fact is a conclusion drawn from several reads, not the
// result of one. Guarded like every other observation: no Observer, no work.
func (c *Client) ObserveVolumeProtection(ctx context.Context, protected bool) {
	if c.Observer == nil {
		return
	}
	c.Observer.ObserveVolumeProtection(ctx, VolumeProtectionEvent{
		Cluster:   ClusterLabel(c.BaseURL),
		Subject:   SubjectFrom(ctx),
		Protected: protected,
	})
}

// ObservePolicyInterval reports the schedule interval of the snapshot policy
// that drives a policy-mode relationship, or zero when it has none.
// Called by the driver, which knows which relationship the policy drives.
func (c *Client) ObservePolicyInterval(ctx context.Context, relationshipID string, interval time.Duration) {
	if c.Observer == nil {
		return
	}
	c.Observer.ObserveReplication(ctx, ReplicationEvent{
		Cluster:          ClusterLabel(c.BaseURL),
		PolicyIntervalOf: relationshipID,
		PolicyInterval:   interval,
		Subject:          SubjectFrom(ctx),
	})
}

// observeCapacity reports the cluster-wide free space this client read.
func (c *Client) observeCapacity(ctx context.Context, capacity FileSystemCapacity) {
	if c.Observer == nil {
		return
	}
	c.Observer.ObserveCapacity(ctx, CapacityEvent{
		Cluster:            ClusterLabel(c.BaseURL),
		FileSystemCapacity: capacity,
	})
}

// observeReplication reports replication status, or the array clock alone
// when statuses is nil.
func (c *Client) observeReplication(
	ctx context.Context,
	role string,
	arrayTime time.Time,
	statuses []RelationshipStatus,
	complete bool,
	started time.Time,
) {
	if c.Observer == nil {
		return
	}
	c.Observer.ObserveReplication(ctx, ReplicationEvent{
		Cluster:       ClusterLabel(c.BaseURL),
		Role:          role,
		ArrayTime:     arrayTime,
		Relationships: statuses,
		Complete:      complete,
		Started:       started,
		Subject:       SubjectFrom(ctx),
	})
}

// observeRelationshipRemoved reports that a relationship no longer exists.
func (c *Client) observeRelationshipRemoved(ctx context.Context, id string) {
	if c.Observer == nil {
		return
	}
	c.Observer.ObserveReplication(ctx, ReplicationEvent{
		Cluster: ClusterLabel(c.BaseURL),
		Removed: id,
	})
}

// newAPIError builds an *APIError, best-effort enriching it with the
// cluster's error class and description. A body that is missing, not JSON or
// past the cap is not itself an error -- the status code is the part callers
// depend on. A body that fails mid-read is returned as readErr, for the
// breaker: the status arrived, but the array did not finish its answer.
func newAPIError(status int, body io.Reader, path string) (apiErr *APIError, readErr error) {
	apiErr = &APIError{StatusCode: status, Path: path}

	var errBody apiErrorBody
	switch err := json.NewDecoder(body).Decode(&errBody); {
	case err == nil:
		apiErr.Msg = errBody.Description
		apiErr.Class = errBody.ErrorClass
	case !isMalformedBody(err):
		readErr = err
	}
	return apiErr, readErr
}

// HealthCheck confirms the Qumulo REST API is reachable. It proves
// reachability only: /v1/version answers with no credential at all, so a
// success here says nothing about whether this client's token is valid --
// see GetCapacity or GetNFSSettings for a call that does.
//
// Readiness is deliberately defined as "the API answered", not "the cluster
// is healthy": Qumulo Core exposes no aggregate cluster-health field, and
// the endpoints that do report health (protection status, per-node status)
// describe conditions a CSI driver should not refuse to serve on -- a
// degraded-but-serving cluster can still provision volumes.
func (c *Client) HealthCheck(ctx context.Context) error {
	var version VersionInfo
	if err := c.do(ctx, http.MethodGet, versionPath, nil, &version); err != nil {
		return fmt.Errorf("qumulo api unreachable: %w", err)
	}
	return nil
}

// fileSystemWire is the API representation of cluster capacity. Sizes arrive
// as decimal strings for the same 64-bit reason as fileWire.ID.
type fileSystemWire struct {
	TotalSizeBytes string `json:"total_size_bytes"`
	FreeSizeBytes  string `json:"free_size_bytes"`
}

// GetCapacity reports the cluster's total and free space.
func (c *Client) GetCapacity(ctx context.Context) (FileSystemCapacity, error) {
	var fs fileSystemWire
	if err := c.do(ctx, http.MethodGet, fileSystemPath, nil, &fs); err != nil {
		return FileSystemCapacity{}, err
	}

	total, err := strconv.ParseUint(fs.TotalSizeBytes, 10, 64)
	if err != nil {
		return FileSystemCapacity{}, fmt.Errorf("parsing total_size_bytes %q: %w", fs.TotalSizeBytes, err)
	}
	free, err := strconv.ParseUint(fs.FreeSizeBytes, 10, 64)
	if err != nil {
		return FileSystemCapacity{}, fmt.Errorf("parsing free_size_bytes %q: %w", fs.FreeSizeBytes, err)
	}

	capacity := FileSystemCapacity{TotalSizeBytes: total, FreeSizeBytes: free}
	c.observeCapacity(ctx, capacity)
	return capacity, nil
}

// fileWire is the API representation of a file entry. Qumulo renders file
// IDs as JSON strings to survive round-tripping through consumers with
// 53-bit numbers, so ID is a string here and everywhere downstream.
// Snapshot IDs, confusingly, are numbers -- see snapshotWire.
type fileWire struct {
	ID         string   `json:"id"`
	Path       string   `json:"path"`
	Type       string   `json:"type"`
	ChildCount *int     `json:"child_count"`
	Owner      Identity `json:"owner_details"`
	Group      Identity `json:"group_details"`
	// Size is a decimal string, as every byte count in this API is.
	Size string `json:"size"`
}

func (f fileWire) toFileInfo() FileInfo {
	// Field by field since Size needs converting. It used to be a struct
	// conversion, which made a field added to one and not the other a
	// compile error; with the two no longer identical, every field is at
	// least named here so a dropped one is visible.
	info := FileInfo{
		ID: f.ID, Path: f.Path, Type: f.Type,
		ChildCount: f.ChildCount, Owner: f.Owner, Group: f.Group,
	}
	// An unreadable size is left unset rather than guessed at zero: a
	// restore's size check treats "the array did not say" as nothing to
	// check, and zero as an empty file.
	if size, err := strconv.ParseUint(f.Size, 10, 64); err == nil {
		info.Size = &size
	}
	return info
}

// CreateDirectory creates a directory named name under parentPath.
func (c *Client) CreateDirectory(ctx context.Context, parentPath, name string) (FileInfo, error) {
	req := struct {
		Name   string `json:"name"`
		Action string `json:"action"`
	}{Name: name, Action: "CREATE_DIRECTORY"}

	var created fileWire
	path := fmt.Sprintf(fileEntriesPath, fileRef(parentPath))
	// Retry is safe here. A repeat answers 409 fs_entry_exists_error, which CreateVolume adopts.
	if err := c.doRetryablePost(ctx, path, req, &created); err != nil {
		return FileInfo{}, err
	}
	return created.toFileInfo(), nil
}

// DirEntry is one entry of a directory listing.
//
// Size, Mode, Owner, Group and ModificationTime are carried because a restore
// has to recreate an entry faithfully, and Size is also what lets a
// resumed restore tell an already-copied file from a truncated one.
type DirEntry struct {
	Name string
	Path string
	ID   string
	Type string
	Size uint64
	Mode string
	// Owner and Group are the cluster's auth IDs, as the attribute endpoint
	// takes them back. They are array-local, which is fine for a restore:
	// it runs on the array the snapshot was taken on.
	Owner string
	Group string
	// OwnerDetails and GroupDetails are the same owner and group by kind and
	// value, which is what tells a local identity from an NFS one.
	OwnerDetails Identity
	GroupDetails Identity
	// ModificationTime is RFC 3339 with nanoseconds, as the API renders it.
	ModificationTime string
	// ChildCount is a directory's entry count, nil when the answer carried
	// none.
	ChildCount *int
	// NumLinks is the entry's link count, nil when the answer carried none.
	// A file with one link cannot be a hard link's other name, which is what
	// lets a restore skip remembering it.
	NumLinks *int
}

// IsDirectory reports whether the entry is a directory.
func (e DirEntry) IsDirectory() bool { return e.Type == fileTypeDirectory }

type dirEntryWire struct {
	Name string `json:"name"`
	Path string `json:"path"`
	ID   string `json:"id"`
	Type string `json:"type"`
	// The API renders sizes as JSON strings, not numbers, because they can
	// exceed what a double represents exactly.
	Size             string   `json:"size"`
	Mode             string   `json:"mode"`
	Owner            string   `json:"owner"`
	Group            string   `json:"group"`
	OwnerDetails     Identity `json:"owner_details"`
	GroupDetails     Identity `json:"group_details"`
	ModificationTime string   `json:"modification_time"`
	NumLinks         *int     `json:"num_links"`
	ChildCount       *int     `json:"child_count"`
}

type entriesWire struct {
	Files  []dirEntryWire `json:"files"`
	Paging struct {
		Next string `json:"next"`
	} `json:"paging"`
}

// maxDirectoryPages bounds one directory listing: 10,000 pages of 1,000
// entries, well beyond the 50,000-entry restore cap and any volume root a
// listing serves.
const maxDirectoryPages = 10000

// ListDirectory lists the entries directly under path.
//
// snapshotID, when non-empty, reads the directory as it existed in that
// snapshot rather than at head -- which is what makes a restore resumable: the
// source is immutable, so a retry sees exactly what the previous attempt saw.
//
// Paging is followed to the end. A listing that stopped at the first page would
// silently restore a partial volume, which is the one outcome a restore must
// never produce.
func (c *Client) ListDirectory(ctx context.Context, path, snapshotID string) ([]DirEntry, error) {
	var out []DirEntry
	err := c.ListDirectoryPages(ctx, path, snapshotID, func(page []DirEntry) error {
		out = append(out, page...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListDirectoryPages is ListDirectory a page at a time: fn is called with
// each page of up to 1,000 entries as it arrives, and an error from fn stops
// the listing and is returned. A caller that handles each page before the
// next holds one page, not the whole directory.
func (c *Client) ListDirectoryPages(ctx context.Context, path, snapshotID string, fn func([]DirEntry) error) error {
	const pageSize = 1000

	next := fmt.Sprintf(fileEntriesPath, fileRef(path)) + fmt.Sprintf("?limit=%d", pageSize)
	if snapshotID != "" {
		next += "&snapshot=" + url.QueryEscape(snapshotID)
	}

	seen := map[string]bool{}
	for pages := 0; next != ""; pages++ {
		// The cursor comes from the array. One that repeats, or a listing
		// that never ends, looped for ever and grew the heap without bound;
		// one that is not a path on this endpoint is refused by do.
		if seen[next] {
			return malformedReply(fmt.Errorf("listing %s: the array returned the paging cursor %q a second time", path, next))
		}
		if pages >= maxDirectoryPages {
			return fmt.Errorf("listing %s: stopped after %d pages of %d entries", path, maxDirectoryPages, pageSize)
		}
		if !strings.HasPrefix(next, "/") {
			return malformedReply(fmt.Errorf("listing %s: the paging cursor %q is not a path on this endpoint", path, next))
		}
		seen[next] = true

		var page entriesWire
		if err := c.do(ctx, http.MethodGet, next, nil, &page); err != nil {
			return err
		}
		out := make([]DirEntry, 0, len(page.Files))
		for _, f := range page.Files {
			// A file's size decides what a restore copies, so an unreadable
			// one is refused rather than read as zero. It used to be
			// discarded: a listing whose size field was missing, empty or
			// not a number made a 0-byte entry, and restoreFile skips a
			// copy at size 0 -- so the restore reported success and
			// committed a volume holding empty files. Only files are
			// checked: the array reports no meaningful size for a
			// directory, and nothing reads it.
			size, parseErr := strconv.ParseUint(f.Size, 10, 64)
			if parseErr != nil && f.Type == fileTypeFile {
				return malformedReply(fmt.Errorf("listing %s: entry %q has size %q, which is not a number of bytes: %w",
					path, f.Name, f.Size, parseErr))
			}
			out = append(out, DirEntry{
				Name: f.Name, Path: f.Path, ID: f.ID,
				Type: f.Type, Size: size, Mode: f.Mode,
				Owner: f.Owner, Group: f.Group, OwnerDetails: f.OwnerDetails, GroupDetails: f.GroupDetails,
				ModificationTime: f.ModificationTime, NumLinks: f.NumLinks, ChildCount: f.ChildCount,
			})
		}
		if err := fn(out); err != nil {
			return err
		}
		// The API returns a relative URI for the next page, already carrying
		// the limit and snapshot parameters.
		next = page.Paging.Next
	}
	return nil
}

// CreateFile creates an empty file named name under parentPath.
//
// copy-chunk requires its target to exist already ("The file must already
// exist"), so a restore is always create-then-copy.
func (c *Client) CreateFile(ctx context.Context, parentPath, name string) (FileInfo, error) {
	req := struct {
		Name   string `json:"name"`
		Action string `json:"action"`
	}{Name: name, Action: "CREATE_FILE"}

	var created fileWire
	path := fmt.Sprintf(fileEntriesPath, fileRef(parentPath))
	// Retry is safe here. A repeat answers 409, which the restore adopts.
	if err := c.doRetryablePost(ctx, path, req, &created); err != nil {
		return FileInfo{}, err
	}
	return created.toFileInfo(), nil
}

// CreateLink creates name under parentPath as a hard link to existingPath:
// POST /v1/files/{parent}/entries/ with action CREATE_LINK, which takes the
// existing file in old_path as RENAME does.
func (c *Client) CreateLink(ctx context.Context, parentPath, name, existingPath string) (FileInfo, error) {
	req := struct {
		Name    string `json:"name"`
		Action  string `json:"action"`
		OldPath string `json:"old_path"`
	}{Name: name, Action: "CREATE_LINK", OldPath: existingPath}

	var created fileWire
	path := fmt.Sprintf(fileEntriesPath, fileRef(parentPath))
	// Retry is safe here. A repeat answers 409, which the restore adopts.
	if err := c.doRetryablePost(ctx, path, req, &created); err != nil {
		return FileInfo{}, err
	}
	return created.toFileInfo(), nil
}

// copyChunkWire is both the request and the response of copy-chunk.
//
// The API copies at most one chunk per call (32 MiB observed on Core 7.9.3) and
// answers with the *next request to make*: the offsets and length still
// outstanding. A JSON null means the copy is complete. That protocol is why
// CopyFile loops rather than issuing one call.
type copyChunkWire struct {
	SourceID       string `json:"source_id,omitempty"`
	SourceSnapshot uint64 `json:"source_snapshot,omitempty"`
	SourceOffset   string `json:"source_offset,omitempty"`
	TargetOffset   string `json:"target_offset,omitempty"`
	Length         string `json:"length,omitempty"`
	// SourceETag guards against the source changing between calls. A snapshot
	// source cannot change, but echoing it back costs nothing and makes a
	// head-version copy safe too.
	SourceETag string `json:"source_etag,omitempty"`
}

// maxCopyChunkCalls bounds the copy loop.
//
// At the observed 32 MiB per call this allows a file of about 128 TiB, which is
// far past anything a restore should attempt -- so hitting it means the API
// stopped making progress, and looping forever would be worse than failing.
const maxCopyChunkCalls = 4_194_304

// CopyFile copies a source file's whole contents into an existing target file,
// server-side.
//
// The bytes never pass through this process, which is what keeps a restore
// bounded by the cluster rather than by the controller's network.
// sourceSnapshot selects the snapshot version of the source; empty means head.
//
// One copy-chunk call copies only a chunk, and returns what remains. Calling it
// once -- which an earlier version of this driver did -- silently truncates any
// file larger than that chunk, so this loops until the API reports completion.
//
// offset is where to start, in source and target alike: 0 copies the whole
// file; a resumed restore passes the size an interrupted copy already wrote,
// so a file too large for one attempt's budget still finishes.
func (c *Client) CopyFile(ctx context.Context, targetPath, sourceID, sourceSnapshot string, offset uint64) error {
	req := copyChunkWire{
		SourceID:     sourceID,
		SourceOffset: strconv.FormatUint(offset, 10),
		TargetOffset: strconv.FormatUint(offset, 10),
	}
	if sourceSnapshot != "" {
		// Typed as a number by the API, unlike the byte offsets.
		n, err := strconv.ParseUint(sourceSnapshot, 10, 64)
		if err != nil {
			return fmt.Errorf("source snapshot %q is not a snapshot ID: %w", sourceSnapshot, err)
		}
		req.SourceSnapshot = n
	}

	path := fmt.Sprintf(fileCopyChunkPath, fileRef(targetPath))

	for calls := 0; calls < maxCopyChunkCalls; calls++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		// A pointer-to-pointer so a JSON null decodes to nil rather than a
		// zero-valued struct, which is how completion is signalled.
		var next *copyChunkWire
		// Retry is safe here. Idempotent: the same offsets copy the same bytes to the same place.
		if err := c.doRetryablePost(ctx, path, req, &next); err != nil {
			return err
		}
		if next == nil {
			return nil // complete
		}

		// Guard against a response that would make the loop spin: without
		// this, an API that stopped advancing would repeat the same call until
		// the iteration cap, doing nothing.
		if next.SourceOffset == req.SourceOffset && next.TargetOffset == req.TargetOffset {
			return fmt.Errorf("copying %s: the cluster reported no progress at offset %s",
				targetPath, req.SourceOffset)
		}

		// A copy only ever moves forward. Backwards is the array
		// contradicting what it already said, and re-copying bytes that are
		// already there would at best waste the budget. With the no-progress
		// guard above, this is also what stops a cycle -- A, B, A, B -- which
		// spun to the iteration cap, millions of calls later, with the file
		// never finished and the restore's whole budget spent: every
		// step must be strictly forward, so no pair can come round again.
		nextSource, sourceErr := strconv.ParseUint(next.SourceOffset, 10, 64)
		nextTarget, targetErr := strconv.ParseUint(next.TargetOffset, 10, 64)
		current, _ := strconv.ParseUint(req.SourceOffset, 10, 64)
		currentTarget, _ := strconv.ParseUint(req.TargetOffset, 10, 64)
		if sourceErr != nil || targetErr != nil {
			return malformedReply(fmt.Errorf("copying %s: the cluster asked to resume at source offset %q, target offset %q, which are not byte offsets",
				targetPath, next.SourceOffset, next.TargetOffset))
		}
		if nextSource < current || nextTarget < currentTarget {
			return malformedReply(fmt.Errorf("copying %s: the cluster asked to resume at offset %d after copying from %d, which is backwards",
				targetPath, nextSource, current))
		}

		next.SourceID = req.SourceID
		next.SourceSnapshot = req.SourceSnapshot
		req = *next
	}
	return fmt.Errorf("copying %s: gave up after %d copy-chunk calls", targetPath, maxCopyChunkCalls)
}

// Rename moves oldPath to name under parentPath.
//
// This is what gives a restore atomicity: the volume is assembled under a
// temporary name and renamed into place as the final step, so it either does
// not exist or is complete. clobber is deliberately false -- a restore must
// never overwrite an existing volume.
func (c *Client) Rename(ctx context.Context, parentPath, name, oldPath string) (FileInfo, error) {
	req := struct {
		Name    string `json:"name"`
		Action  string `json:"action"`
		OldPath string `json:"old_path"`
		Clobber bool   `json:"clobber"`
	}{Name: name, Action: "RENAME", OldPath: oldPath, Clobber: false}

	var moved fileWire
	path := fmt.Sprintf(fileEntriesPath, fileRef(parentPath))
	// Not retried: a repeat 404s on the path the first one already
	// moved, turning a rename that succeeded into a reported failure.
	if err := c.do(ctx, http.MethodPost, path, req, &moved); err != nil {
		return FileInfo{}, err
	}
	return moved.toFileInfo(), nil
}

// NFSSettings is the cluster's NFS configuration.
//
// V4Enabled is the field the driver acts on: mounting with nfsvers=4.1 against
// a cluster that has NFSv4 off fails in the kernel with "Protocol not
// supported", by which point a pod is already stuck. IdmapDomain is carried
// because a future adDomain implementation needs it, and the Kerberos flags
// because they decide whether an explicit sec= is required.
//
// Krb5pEnabled and Krb5iEnabled are their own switches on the cluster, beside
// krb5_enabled: an array can accept krb5 and refuse krb5p. They are decoded
// so the gap is expressible in a test. Nothing reads them yet -- that is the
// defect, not an oversight in this struct.
type NFSSettings struct {
	V4Enabled      bool
	Krb5Enabled    bool
	Krb5pEnabled   bool
	Krb5iEnabled   bool
	AuthSysEnabled bool
	IdmapDomain    string
}

type nfsSettingsWire struct {
	V4Enabled      bool    `json:"v4_enabled"`
	Krb5Enabled    bool    `json:"krb5_enabled"`
	Krb5pEnabled   bool    `json:"krb5p_enabled"`
	Krb5iEnabled   bool    `json:"krb5i_enabled"`
	AuthSysEnabled bool    `json:"auth_sys_enabled"`
	IdmapDomain    *string `json:"idmap_domain"`
}

// GetNFSSettings reads the cluster's NFS configuration.
func (c *Client) GetNFSSettings(ctx context.Context) (NFSSettings, error) {
	var wire nfsSettingsWire
	if err := c.do(ctx, http.MethodGet, nfsSettingsPath, nil, &wire); err != nil {
		return NFSSettings{}, err
	}

	out := NFSSettings{
		V4Enabled:      wire.V4Enabled,
		Krb5Enabled:    wire.Krb5Enabled,
		Krb5pEnabled:   wire.Krb5pEnabled,
		Krb5iEnabled:   wire.Krb5iEnabled,
		AuthSysEnabled: wire.AuthSysEnabled,
	}
	// The cluster sends JSON null when no domain is configured, which is the
	// normal case and not an error.
	if wire.IdmapDomain != nil {
		out.IdmapDomain = *wire.IdmapDomain
	}
	return out, nil
}

// LookupFileByPath resolves an absolute cluster path to a FileInfo.
func (c *Client) LookupFileByPath(ctx context.Context, filePath string) (FileInfo, error) {
	var found fileWire
	path := fmt.Sprintf(fileAttributesPath, fileRef(filePath))
	if err := c.do(ctx, http.MethodGet, path, nil, &found); err != nil {
		return FileInfo{}, err
	}
	return found.toFileInfo(), nil
}

// ResolveFileIDs maps file IDs to their canonical paths in one call.
func (c *Client) ResolveFileIDs(ctx context.Context, fileIDs []string) (map[string]string, error) {
	if len(fileIDs) == 0 {
		return map[string]string{}, nil
	}

	var resolved []struct {
		ID   string `json:"id"`
		Path string `json:"path"`
	}
	// Retry is safe here. A read that happens to be a POST. It changes nothing.
	if err := c.doRetryablePost(ctx, filesResolvePath, fileIDs, &resolved); err != nil {
		return nil, err
	}

	paths := make(map[string]string, len(resolved))
	for _, entry := range resolved {
		// An ID the cluster could not resolve comes back with an empty
		// path; omitting it lets callers fall back per ID.
		if entry.Path == "" {
			continue
		}
		// The cluster renders a directory with a trailing slash here
		// ("/a/b/"), which no other endpoint does -- an NFS export records its
		// fs_path without one. Cleaning at the boundary keeps that wire quirk
		// from leaking into every caller that compares a resolved path to a
		// path from somewhere else. path.Clean leaves "/" alone.
		paths[entry.ID] = path.Clean(entry.Path)
	}
	return paths, nil
}

// StartTreeDelete asks the cluster to begin recursively deleting fileID.
//
// A job already running for fileID is success: DeleteVolume returns Aborted
// while a delete outlives its budget, and the CO's retry asks again for the
// same tree. The cluster answers that second request with HTTP 500
// tree_delete_already_started_error, which surfaced as Internal on every retry
// until the job finished.
func (c *Client) StartTreeDelete(ctx context.Context, fileID string) error {
	req := struct {
		ID string `json:"id"`
	}{ID: fileID}

	// Retry is safe here. A repeat answers tree_delete_already_started_error, mapped to success below.
	err := c.doRetryablePost(ctx, treeDeleteJobsPath, req, nil)
	if IsTreeDeleteAlreadyStarted(err) {
		return nil
	}
	return err
}

// TreeDeleteFinished reports whether the deletion job for fileID has
// finished.
//
// The cluster returns 404 both for a job that has completed and for one that
// never existed, so that status is translated to "finished" here rather than
// leaking an IsNotFound check into the caller as a double negative.
//
// The 404 has to be the cluster's own, though. A bare 404 -- no Qumulo error
// body at all -- is what a proxy, an ingress or a wrong base URL answers, and
// reading that as "the tree is gone" let DeleteVolume report success for a
// volume still on the array, which is the failure in another place.
// Any Qumulo error class counts, not just the file ones: a real
// array answers a finished or unknown job with 404 "http_not_found_error"
// (measured on a lab array), and which class a completed job yields is the
// cluster's business.
//
// Do not narrow this to one class. The check only has to tell the cluster's
// 404 from a proxy's; a class list that missed what the array actually sends
// would read every finished job as "still running", and every DeleteVolume
// would poll until its timeout and fail, on a delete that had succeeded.
func (c *Client) TreeDeleteFinished(ctx context.Context, fileID string) (bool, error) {
	err := c.do(ctx, http.MethodGet, fmt.Sprintf(treeDeleteJobPath, url.PathEscape(fileID)), nil, nil)
	switch {
	case err == nil:
		return false, nil
	case hasStatus(err, http.StatusNotFound) && ErrorClassOf(err) != "":
		return true, nil
	case hasStatus(err, http.StatusNotFound):
		return false, fmt.Errorf("checking the tree-delete job for file %s: the endpoint answered 404 with no "+
			"Qumulo error body, so it may not be the cluster's API rather than the job being finished: %w", fileID, err)
	default:
		return false, err
	}
}

// quotaWire is the request body for creating or updating a directory quota.
// Limit is a decimal string for the same 64-bit reason as fileWire.ID.
//
// Deliberately separate from quotaResponseWire below. The two differ by one
// field, and the temptation is to use one struct for both -- but this one is
// marshalled into a PUT and a POST, so an extra field here is an extra field
// on the wire, and the quota endpoint is strict about the field set it
// accepts (see TestLiveDiscoverNFSExportSchema for the same property on
// exports). Reading a field the driver must never send is exactly the case
// two types exist for.
type quotaWire struct {
	ID    string `json:"id"`
	Limit string `json:"limit"`
}

// quotaResponseWire is what GET /v1/files/quotas/{id} answers with.
type quotaResponseWire struct {
	ID    string `json:"id"`
	Limit string `json:"limit"`
	// CapacityUsage is the bytes currently consumed under the quota's
	// directory, as a decimal string. The cluster computes it from the same
	// aggregates the Qumulo UI shows, so it accounts for the whole tree.
	CapacityUsage string `json:"capacity_usage"`
}

// quotaStatusWire is GET /v1/files/quotas/status/{id}: the same quota, with
// the usage the plain object omits and the path it covers.
type quotaStatusWire struct {
	ID            string `json:"id"`
	Path          string `json:"path"`
	Limit         string `json:"limit"`
	CapacityUsage string `json:"capacity_usage"`
}

// QuotaInfo is a directory quota's limit and what has been used against it.
//
// Usage is the field GetCapacity and CreateSnapshot both waited on. Reporting
// a quota's limit as available space is an upper bound rather than an answer:
// a store root with a 10 TiB quota that is already full still reported 10 TiB
// free, and the scheduler believed it.
type QuotaInfo struct {
	// Limit is the quota's ceiling in bytes.
	Limit uint64
	// Usage is bytes consumed under the quota's directory.
	Usage uint64
	// UsageKnown distinguishes "the cluster reported zero bytes used" from
	// "the cluster did not report usage at all", which an older Core release
	// or a narrower privilege can produce. Callers must not treat a missing
	// figure as an empty directory: that would turn a full volume into one
	// reporting its whole limit as free.
	UsageKnown bool
}

// Available reports the bytes left under the quota.
//
// Zero when usage exceeds the limit, which is reachable: a quota can be
// lowered below current usage, and writes then fail rather than the number
// going negative.
func (q QuotaInfo) Available() uint64 {
	if !q.UsageKnown {
		// No usage figure from the cluster: fall back to the limit, which is
		// the upper bound this driver reported before usage was read at all.
		// Loose, but never an under-report -- and an under-report is the
		// costlier mistake here, since it stops the scheduler placing a pod on
		// space that exists.
		return q.Limit
	}
	if q.Usage >= q.Limit {
		return 0
	}
	return q.Limit - q.Usage
}

// CreateQuota attaches a directory quota of limitBytes to fileID.
func (c *Client) CreateQuota(ctx context.Context, fileID string, limitBytes uint64) error {
	req := quotaWire{ID: fileID, Limit: strconv.FormatUint(limitBytes, 10)}
	// Retry is safe here. A repeat answers 409, which the quota callers adopt.
	if err := c.doRetryablePost(ctx, quotasPath, req, nil); err != nil {
		return err
	}
	// The limit is now known for this file ID; usage is not, and a fresh
	// volume's usage is not worth a second round trip to learn.
	c.observeQuota(ctx, fileID, QuotaInfo{Limit: limitBytes})
	return nil
}

// aggregatesWire is the part of GET /v1/files/{ref}/aggregates/ the driver
// reads. total_capacity is data plus metadata, the measure a quota's
// capacity_usage is in, so it compares directly with a requested capacity.
// Every count is a decimal string.
type aggregatesWire struct {
	TotalCapacity    string `json:"total_capacity"`
	TotalFiles       string `json:"total_files"`
	TotalDirectories string `json:"total_directories"`
}

// TreeUsage is what a directory tree holds, below its root.
type TreeUsage struct {
	// Capacity is data plus metadata, in bytes.
	Capacity uint64
	// Files and Directories count every regular file and directory beneath
	// the root, not the root itself.
	Files       uint64
	Directories uint64
}

// GetTreeUsage returns what the tree rooted at fileID holds, in the snapshot
// snapshotID or, when that is empty, live.
//
// A Qumulo snapshot carries no size of its own, so this is how a restore
// learns, before copying anything, whether the data fits the claim and
// whether the tree is within the restore's caps.
func (c *Client) GetTreeUsage(ctx context.Context, fileID, snapshotID string) (TreeUsage, error) {
	p := fmt.Sprintf(fileAggregatesPath, fileRef(fileID))
	if snapshotID != "" {
		p += "?snapshot=" + url.QueryEscape(snapshotID)
	}
	var agg aggregatesWire
	if err := c.do(ctx, http.MethodGet, p, nil, &agg); err != nil {
		return TreeUsage{}, err
	}
	var usage TreeUsage
	for _, f := range []struct {
		name, raw string
		into      *uint64
	}{
		{"total_capacity", agg.TotalCapacity, &usage.Capacity},
		{"total_files", agg.TotalFiles, &usage.Files},
		{"total_directories", agg.TotalDirectories, &usage.Directories},
	} {
		n, err := strconv.ParseUint(f.raw, 10, 64)
		if err != nil {
			return TreeUsage{}, fmt.Errorf("parsing %s %q for file %s: %w", f.name, f.raw, fileID, err)
		}
		*f.into = n
	}
	return usage, nil
}

// GetQuota returns the quota limit in bytes attached to fileID.
func (c *Client) GetQuota(ctx context.Context, fileID string) (uint64, error) {
	info, err := c.GetQuotaInfo(ctx, fileID)
	if err != nil {
		return 0, err
	}
	return info.Limit, nil
}

// GetQuotaInfo returns the quota's limit and how much of it is used.
//
// A missing or unparseable capacity_usage is not an error: it leaves
// UsageKnown false and the limit intact. The distinction matters because the
// callers differ in what they do without it -- GetCapacity falls back to the
// limit as an upper bound and keeps working, while CreateSnapshot omits
// size_bytes, which the CSI spec permits. Failing the whole call instead would
// turn a narrower API token into a driver that cannot provision.
func (c *Client) GetQuotaInfo(ctx context.Context, fileID string) (QuotaInfo, error) {
	// One GET, not two: the status document carries the limit as well as
	// capacity_usage. Every quota read was two round trips, and a
	// replication status poll makes several.
	var status quotaStatusWire
	err := c.do(ctx, http.MethodGet, fmt.Sprintf(quotaStatusPath, url.PathEscape(fileID)), nil, &status)
	if err == nil && status.Limit != "" {
		limit, perr := strconv.ParseUint(status.Limit, 10, 64)
		if perr != nil {
			return QuotaInfo{}, fmt.Errorf("parsing quota limit %q for file %s: %w", status.Limit, fileID, perr)
		}
		info := QuotaInfo{Limit: limit}
		if usage, uerr := strconv.ParseUint(status.CapacityUsage, 10, 64); uerr == nil && status.CapacityUsage != "" {
			info.Usage, info.UsageKnown = usage, true
		}
		c.observeQuota(ctx, fileID, info)
		return info, nil
	}
	if IsNotFound(err) {
		return QuotaInfo{}, err
	}

	// The status document could not be read, or carried no limit: the quota
	// object has the limit, and the usage stays unknown. Not a failure of
	// this call -- GetCapacity falls back to the limit as an upper bound, and
	// CreateSnapshot omits size_bytes, which the CSI spec permits.
	if err != nil {
		slog.DebugContext(ctx, "quota status unavailable; reading the limit alone", "fileID", fileID, "error", err)
	}
	var quota quotaResponseWire
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf(quotaPath, url.PathEscape(fileID)), nil, &quota); err != nil {
		return QuotaInfo{}, err
	}
	limit, err := strconv.ParseUint(quota.Limit, 10, 64)
	if err != nil {
		return QuotaInfo{}, fmt.Errorf("parsing quota limit %q for file %s: %w", quota.Limit, fileID, err)
	}
	info := QuotaInfo{Limit: limit}
	c.observeQuota(ctx, fileID, info)
	return info, nil
}

// UpdateQuota changes the quota limit attached to fileID.
func (c *Client) UpdateQuota(ctx context.Context, fileID string, limitBytes uint64) error {
	req := quotaWire{ID: fileID, Limit: strconv.FormatUint(limitBytes, 10)}
	if err := c.do(ctx, http.MethodPut, fmt.Sprintf(quotaPath, url.PathEscape(fileID)), req, nil); err != nil {
		return err
	}
	// An expansion changes the limit under the operator's feet; reporting it
	// here is what stops the gauge sitting at the pre-expansion figure until
	// something else happens to read the quota.
	c.observeQuota(ctx, fileID, QuotaInfo{Limit: limitBytes})
	return nil
}

// DeleteQuota removes the quota attached to fileID.
func (c *Client) DeleteQuota(ctx context.Context, fileID string) error {
	return c.do(ctx, http.MethodDelete, fmt.Sprintf(quotaPath, url.PathEscape(fileID)), nil, nil)
}

// snapshotWire is the API representation of a snapshot.
//
// ID is a json.Number because the API renders a snapshot ID as a bare JSON
// number -- unlike file and quota IDs, which are strings. Decoding it as
// json.Number rather than int64 keeps the driver's string-typed IDs intact
// and also accepts a quoted value, should a Core release change its mind.
type snapshotWire struct {
	ID           json.Number `json:"id"`
	Name         string      `json:"name"`
	SourceFileID string      `json:"source_file_id"`
	// Timestamp is RFC 3339 with nanosecond precision.
	Timestamp string `json:"timestamp"`
	InDelete  bool   `json:"in_delete"`
	// PolicyID is null for a manually created snapshot. Decoding null into a
	// json.Number leaves it empty, which is exactly the wanted result.
	PolicyID json.Number `json:"policy_id"`
	// Expiration is RFC 3339, or "" for a snapshot that never expires.
	Expiration string `json:"expiration"`
}

func (s snapshotWire) toSnapshotInfo() SnapshotInfo {
	info := SnapshotInfo{
		ID:           s.ID.String(),
		Name:         s.Name,
		SourceFileID: s.SourceFileID,
		InDelete:     s.InDelete,
		PolicyID:     s.PolicyID.String(),
	}
	// A snapshot with an unparseable timestamp is still usable; CSI treats
	// creation_time as informational, so a zero time is preferable to
	// failing the whole RPC.
	if ts, err := time.Parse(time.RFC3339Nano, s.Timestamp); err == nil {
		info.CreatedAt = ts
	}
	if s.Expiration != "" {
		if ts, err := time.Parse(time.RFC3339Nano, s.Expiration); err == nil {
			info.Expiration = ts
		}
	}
	return info
}

// CreateSnapshot snapshots the directory fileID.
//
// The API field is name_suffix, not name: Qumulo prepends the new snapshot's
// ID and an underscore, so a suffix of "foo" is stored as "12_foo" and the
// returned SnapshotInfo.Name never equals nameSuffix.
//
// expiration must be present even when the snapshot should never expire, in
// which case it is the empty string. Omitting the field is not equivalent:
// Qumulo Core 7.9.3 rejects the request outright with
// "field 'expiration' not found", and JSON null is rejected as well. Hence
// the explicit empty value and the absence of omitempty -- a snapshot the CO
// has not deleted must not disappear on its own.
func (c *Client) CreateSnapshot(ctx context.Context, fileID, nameSuffix string) (SnapshotInfo, error) {
	req := struct {
		SourceFileID string `json:"source_file_id"`
		NameSuffix   string `json:"name_suffix"`
		Expiration   string `json:"expiration"`
	}{SourceFileID: fileID, NameSuffix: nameSuffix, Expiration: ""}

	var created snapshotWire
	// Not retried: the name is a label, not a key, so a repeat that
	// follows a POST that did land creates a second snapshot. The RPC's own
	// findSnapshotByName adopts it on the CO's retry instead.
	if err := c.do(ctx, http.MethodPost, snapshotsPath, req, &created); err != nil {
		return SnapshotInfo{}, err
	}
	return created.toSnapshotInfo(), nil
}

// CreateExpiringSnapshot is CreateSnapshot with an expiration the cluster
// enforces (RFC 3339).
func (c *Client) CreateExpiringSnapshot(ctx context.Context, fileID, nameSuffix string, expiration time.Time) (SnapshotInfo, error) {
	req := struct {
		SourceFileID string `json:"source_file_id"`
		NameSuffix   string `json:"name_suffix"`
		Expiration   string `json:"expiration"`
	}{SourceFileID: fileID, NameSuffix: nameSuffix, Expiration: expiration.UTC().Format(time.RFC3339)}

	var created snapshotWire
	// Not retried: same endpoint as CreateSnapshot, same duplication.
	if err := c.do(ctx, http.MethodPost, snapshotsPath, req, &created); err != nil {
		return SnapshotInfo{}, err
	}
	return created.toSnapshotInfo(), nil
}

// SnapshotDiffEmpty asks for the first change between two snapshots and
// reports whether there is none.
func (c *Client) SnapshotDiffEmpty(ctx context.Context, newerID, olderID string) (bool, error) {
	var listed struct {
		Entries []struct {
			Op   string `json:"op"`
			Path string `json:"path"`
		} `json:"entries"`
	}
	p := fmt.Sprintf(snapshotChangesSincePath+"?limit=1", url.PathEscape(newerID), url.PathEscape(olderID))
	if err := c.do(ctx, http.MethodGet, p, nil, &listed); err != nil {
		return false, err
	}
	return len(listed.Entries) == 0, nil
}

// SetSnapshotExpiration makes the cluster delete a snapshot on its own at
// expiration. PATCH /v3/snapshots/{id} with only the expiration field leaves
// the snapshot's name and contents alone; measured on the lab arrays, it
// answers 200 with the snapshot as changed.
func (c *Client) SetSnapshotExpiration(ctx context.Context, snapshotID string, expiration time.Time) (SnapshotInfo, error) {
	req := struct {
		Expiration string `json:"expiration"`
	}{Expiration: expiration.UTC().Format(time.RFC3339)}
	var patched snapshotWire
	if err := c.do(ctx, http.MethodPatch, fmt.Sprintf(snapshotPath, url.PathEscape(snapshotID)), req, &patched); err != nil {
		return SnapshotInfo{}, err
	}
	return patched.toSnapshotInfo(), nil
}

// DeleteSnapshot removes a snapshot by ID.
func (c *Client) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	return c.do(ctx, http.MethodDelete, fmt.Sprintf(snapshotPath, url.PathEscape(snapshotID)), nil, nil)
}

// ListSnapshots returns the cluster's snapshots, excluding those already
// being deleted.
func (c *Client) ListSnapshots(ctx context.Context) ([]SnapshotInfo, error) {
	var listed struct {
		Entries []snapshotWire `json:"entries"`
	}

	path := snapshotsPath + "?filter=" + snapshotFilterExcludeInDelete
	if err := c.do(ctx, http.MethodGet, path, nil, &listed); err != nil {
		return nil, err
	}

	snapshots := make([]SnapshotInfo, 0, len(listed.Entries))
	for _, entry := range listed.Entries {
		snapshots = append(snapshots, entry.toSnapshotInfo())
	}
	return snapshots, nil
}

// GetSnapshot returns a single snapshot by ID.
func (c *Client) GetSnapshot(ctx context.Context, snapshotID string) (SnapshotInfo, error) {
	var found snapshotWire
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf(snapshotPath, url.PathEscape(snapshotID)), nil, &found); err != nil {
		return SnapshotInfo{}, err
	}
	return found.toSnapshotInfo(), nil
}

// exportWire is the API representation of an NFS export.
type exportWire struct {
	ID          string `json:"id"`
	ExportPath  string `json:"export_path"`
	FSPath      string `json:"fs_path"`
	TenantID    int    `json:"tenant_id"`
	Description string `json:"description"`
	// Restrictions is read, never written, from this struct; the write side
	// is exportRestrictionWire. See ExportInfo for which fields matter.
	Restrictions []exportRestrictionReadWire `json:"restrictions"`
}

// exportRestrictionReadWire is one entry of an export's access policy as the
// cluster reports it. It is named rather than inline so the fields every
// restriction contributes to can be folded over; only the three the driver
// reads are decoded.
type exportRestrictionReadWire struct {
	ReadOnly                   bool     `json:"read_only"`
	RequiredAuthenticationMode string   `json:"required_authentication_mode"`
	HostRestrictions           []string `json:"host_restrictions"`
}

func (e exportWire) toExportInfo() ExportInfo {
	info := ExportInfo{
		ID:          e.ID,
		ExportPath:  e.ExportPath,
		FSPath:      e.FSPath,
		TenantID:    e.TenantID,
		Description: e.Description,
	}
	// Read-only only if every restriction says so: an export with one
	// read-write restriction admits writes from the hosts it names, and a
	// promote must not mistake that for a pre-staged replica export.
	info.ReadOnly = len(e.Restrictions) > 0
	for _, r := range e.Restrictions {
		if !r.ReadOnly {
			info.ReadOnly = false
		}
	}
	if len(e.Restrictions) > 0 {
		info.AuthenticationMode = e.Restrictions[0].RequiredAuthenticationMode
		info.AllowedClients = unionHostRestrictions(e.Restrictions)
	}
	return info
}

// unionHostRestrictions collects the hosts every restriction on an export
// admits, de-duplicated and in the order the cluster listed them.
//
// It is a union and not Restrictions[0] because the driver's one written
// restriction is not necessarily the one restriction read back: the cluster
// may answer it normalised into a block per host. A class naming two
// networks -- an OpenShift cluster's control-plane network and its worker
// network -- comes back that way, and reading only the first block truncated
// the list to its first entry. A replica pre-staged from such a read named
// the control-plane network alone, so no node that runs pods was permitted
// to mount it and every mount of a failed-over replica was refused by the
// array.
//
// A restriction with no host list admits every host, so it widens rather
// than narrows: one such block leaves the whole export unrestricted, and the
// union is then empty -- which is what CreateExport writes for "no host
// restriction" and what an unrestricted export has always read back as.
func unionHostRestrictions(restrictions []exportRestrictionReadWire) []string {
	var clients []string
	seen := make(map[string]struct{}, len(restrictions))
	for _, r := range restrictions {
		if len(r.HostRestrictions) == 0 {
			return nil
		}
		for _, host := range r.HostRestrictions {
			if _, duplicate := seen[host]; duplicate {
				continue
			}
			seen[host] = struct{}{}
			clients = append(clients, host)
		}
	}
	return clients
}

// exportRestrictionWire is one entry in an export's access policy.
//
// The driver sends a single permissive restriction: no host list, read-write,
// no authentication requirement, and no identity mapping. NFS_MAP_NONE is the
// deliberate choice -- it passes the client's uid/gid through untouched, which
// is what makes file ownership inside a pod predictable. Mapping identities is
// what a future adDomain parameter would change, and it needs a domain-joined
// cluster to mean anything.
type exportRestrictionWire struct {
	HostRestrictions           []string `json:"host_restrictions"`
	ReadOnly                   bool     `json:"read_only"`
	RequirePrivilegedPort      bool     `json:"require_privileged_port"`
	RequiredAuthenticationMode string   `json:"required_authentication_mode"`
	UserMapping                string   `json:"user_mapping"`
}

const (
	// Probed against Core 7.9.3: these four are accepted and
	// "AUTHENTICATION_MODE_SYS" is rejected outright, so there is no positive
	// value meaning AUTH_SYS. None is how an export admits an AUTH_SYS client:
	// it requires nothing, and the two ends negotiate down to sec=sys.
	AuthenticationModeNone  = "AUTHENTICATION_MODE_NONE"
	AuthenticationModeKrb5  = "AUTHENTICATION_MODE_KRB5"
	AuthenticationModeKrb5i = "AUTHENTICATION_MODE_KRB5I"
	AuthenticationModeKrb5p = "AUTHENTICATION_MODE_KRB5P"

	authenticationModeNone = AuthenticationModeNone
	nfsMapNone             = "NFS_MAP_NONE"
)

func defaultExportRestriction() exportRestrictionWire {
	return exportRestrictionWire{
		// An explicit empty slice, not nil: nil marshals to JSON null, and the
		// field is required to be present.
		HostRestrictions:           []string{},
		ReadOnly:                   false,
		RequirePrivilegedPort:      false,
		RequiredAuthenticationMode: authenticationModeNone,
		UserMapping:                nfsMapNone,
	}
}

// ExportSpec describes an NFS export to create.
//
// A struct rather than positional arguments: the call already needed three
// strings, and adding a tenant and a host list to that is exactly the shape
// where a transposed pair compiles and then misroutes an export.
type ExportSpec struct {
	// ExportPath is the NFS-visible path; FSPath is the directory on the
	// cluster filesystem it serves.
	ExportPath  string
	FSPath      string
	Description string

	// TenantID scopes the export to a network tenant. Zero means "do not send
	// the field", which leaves the cluster to apply its own default of tenant
	// 1 -- the behaviour every export created before this field existed got,
	// and therefore what an upgrade must keep doing.
	TenantID int

	// AuthenticationMode is the flavour clients must authenticate with. Empty
	// means AuthenticationModeNone -- require nothing -- which is what every
	// export created before this field existed already got.
	//
	// This is the half of securityFlavor that makes it mean anything: without
	// it a client asking for krb5p against a permissive export negotiates
	// quietly back down to sec=sys.
	AuthenticationMode string

	// AllowedClients restricts which hosts may mount, as IP addresses or CIDR
	// blocks. Empty means unrestricted, which is the cluster's own default and
	// what this driver sent before the field existed.
	AllowedClients []string

	// ReadOnly makes the export refuse writes. False -- the default, and what
	// every export created before this field existed got -- is what a volume's
	// own export needs. True is for a replica's export, pre-staged on the
	// target array while the target root is still read-only, so a client
	// reaching it early is refused at mount rather than having its writes
	// silently dropped by the filesystem beneath.
	ReadOnly bool
}

// CreateExport creates an NFS export serving spec.FSPath at spec.ExportPath.
//
// Every field bar tenant_id is sent unconditionally, with no omitempty. Qumulo
// Core 7.9.3 requires each to be *present* and answers a missing one with a
// decode error naming it -- description included, even though an empty
// description is perfectly acceptable. This is the same trap that made
// CreateSnapshot unusable before its expiration field was sent explicitly.
//
// tenant_id is the one exception, and is omitted when zero rather than sent as
// 0: the cluster rejects tenant 0 outright, so sending it would break every
// caller that does not care which tenant they land in.
func (c *Client) CreateExport(ctx context.Context, spec ExportSpec) (ExportInfo, error) {
	restriction := defaultExportRestriction()
	if spec.AuthenticationMode != "" {
		restriction.RequiredAuthenticationMode = spec.AuthenticationMode
	}
	if len(spec.AllowedClients) > 0 {
		restriction.HostRestrictions = spec.AllowedClients
	}
	restriction.ReadOnly = spec.ReadOnly

	req := map[string]any{
		"export_path":  spec.ExportPath,
		"fs_path":      spec.FSPath,
		"description":  spec.Description,
		"restrictions": []exportRestrictionWire{restriction},
	}
	if spec.TenantID != 0 {
		req["tenant_id"] = spec.TenantID
	}

	var created exportWire
	// Retry is safe here. A repeat answers 409 nfs_export_already_exists_error, which CreateExport's caller adopts.
	if err := c.doRetryablePost(ctx, nfsExportsPath, req, &created); err != nil {
		return ExportInfo{}, err
	}
	return created.toExportInfo(), nil
}

// ListTenants returns the cluster's network tenants.
//
// Used to turn a tenant *name* from a StorageClass into the integer id the
// export API wants. Administrators write names; the API speaks ids.
func (c *Client) ListTenants(ctx context.Context) ([]TenantInfo, error) {
	var listed struct {
		Entries []struct {
			ID         int    `json:"id"`
			Name       string `json:"name"`
			NFSEnabled bool   `json:"nfs_enabled"`
		} `json:"entries"`
	}
	if err := c.do(ctx, http.MethodGet, tenantsPath, nil, &listed); err != nil {
		return nil, err
	}

	tenants := make([]TenantInfo, 0, len(listed.Entries))
	for _, e := range listed.Entries {
		tenants = append(tenants, TenantInfo{ID: e.ID, Name: e.Name, NFSEnabled: e.NFSEnabled})
	}
	return tenants, nil
}

// ListNetworks returns the cluster's frontend networks.
//
// Used to check, before anything is created, that a tenant-scoped class is
// mounting from an address that tenant actually answers on. A network names
// its tenant directly, so this is the only call that check needs.
//
// Address assignment is a tagged union on the wire. The three known schemes
// are decoded; anything else comes back with its AddressType set and no
// addresses, and so does a known scheme that lists no floating ranges -- a
// DHCP network with none is reachable at its nodes' own addresses, which this
// call cannot see. Both cases mean "unknown", and the caller must never read
// them as "none": refusing a volume because this code could not enumerate a
// tenant's addresses would break a cluster that is configured perfectly well.
func (c *Client) ListNetworks(ctx context.Context) ([]NetworkInfo, error) {
	// addresses is a tagged union: "type" names the assignment scheme and the
	// matching sub-object carries it. All three known schemes spell the
	// floating ranges the same way, so all three are read -- measured on a
	// HOST cluster (the two-tenant simnode) and a DHCP one (xx.xxx.xx.xx,
	// whose frontend network reports DHCP with no floating ranges at all).
	type addressBlock struct {
		FloatingIPRanges []string `json:"floating_ip_ranges"`
	}
	var config struct {
		FrontendNetworks []struct {
			ID        int    `json:"id"`
			Name      string `json:"name"`
			TenantID  int    `json:"tenant_id"`
			Addresses struct {
				Type            string        `json:"type"`
				HostAddresses   *addressBlock `json:"host_addresses"`
				StaticAddresses *addressBlock `json:"static_addresses"`
				DHCPAddresses   *addressBlock `json:"dhcp_addresses"`
			} `json:"addresses"`
		} `json:"frontend_networks"`
	}
	if err := c.do(ctx, http.MethodGet, networkConfigPath, nil, &config); err != nil {
		return nil, err
	}

	networks := make([]NetworkInfo, 0, len(config.FrontendNetworks))
	for _, n := range config.FrontendNetworks {
		info := NetworkInfo{
			ID:          n.ID,
			Name:        n.Name,
			TenantID:    n.TenantID,
			AddressType: n.Addresses.Type,
		}
		for _, block := range []*addressBlock{
			n.Addresses.HostAddresses, n.Addresses.StaticAddresses, n.Addresses.DHCPAddresses,
		} {
			if block != nil {
				info.FloatingAddresses = append(info.FloatingAddresses, block.FloatingIPRanges...)
			}
		}
		networks = append(networks, info)
	}
	return networks, nil
}

// DeleteExport removes an export by ID.
func (c *Client) DeleteExport(ctx context.Context, exportID string) error {
	return c.do(ctx, http.MethodDelete, fmt.Sprintf(nfsExportPath, url.PathEscape(exportID)), nil, nil)
}

// ListExports returns every NFS export on the cluster.
func (c *Client) ListExports(ctx context.Context) ([]ExportInfo, error) {
	var listed struct {
		Entries []exportWire `json:"entries"`
	}
	if err := c.do(ctx, http.MethodGet, nfsExportsPath, nil, &listed); err != nil {
		return nil, err
	}

	exports := make([]ExportInfo, 0, len(listed.Entries))
	for _, entry := range listed.Entries {
		exports = append(exports, entry.toExportInfo())
	}
	return exports, nil
}

// GetExport returns one NFS export by its ID.
func (c *Client) GetExport(ctx context.Context, exportID string) (ExportInfo, error) {
	var found exportWire
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf(nfsExportPath, url.PathEscape(exportID)), nil, &found); err != nil {
		return ExportInfo{}, err
	}
	return found.toExportInfo(), nil
}

// Endpoint is the REST endpoint this client calls.
func (c *Client) Endpoint() string { return c.BaseURL }

// Identity is an owner or group as the attribute endpoint's owner_details
// and group_details render it: a kind and a value in that kind's namespace.
type Identity struct {
	IDType  string `json:"id_type"`
	IDValue string `json:"id_value"`
}

// The identity kinds the driver reads or writes. A local user or group is
// one defined on the cluster itself, which is what an API token
// authenticates as.
const (
	IDTypeLocalUser  = "LOCAL_USER"
	IDTypeLocalGroup = "LOCAL_GROUP"
	IDTypeNFSUID     = "NFS_UID"
	IDTypeNFSGID     = "NFS_GID"
)

// IsLocal reports whether the identity is a cluster-local user or group.
// Replication refuses an entry owned by one that has no NFS ID.
func (i Identity) IsLocal() bool {
	return i.IDType == IDTypeLocalUser || i.IDType == IDTypeLocalGroup
}

// FileAttributes is a partial attribute update. An empty field is not sent,
// so it is left as it is.
//
// Owner and Group are auth IDs, which is what a restore copies back from a
// listing. OwnerDetails and GroupDetails name an identity by kind and
// value instead, which is how the driver gives a directory an NFS owner;
// a caller sets one form or the other, not both.
type FileAttributes struct {
	Mode             string    `json:"mode,omitempty"`
	Owner            string    `json:"owner,omitempty"`
	Group            string    `json:"group,omitempty"`
	OwnerDetails     *Identity `json:"owner_details,omitempty"`
	GroupDetails     *Identity `json:"group_details,omitempty"`
	ModificationTime string    `json:"modification_time,omitempty"`
	// Size sets the file's length in bytes, a decimal string like every
	// 64-bit number in this API. Shortening a file is the one thing
	// copy-chunk cannot do: it writes bytes in place and leaves whatever lies
	// past them, so a restore cuts a staged file that is too long with this
	// before copying over it.
	Size string `json:"size,omitempty"`
}

// IsEmpty reports whether the update would change nothing.
func (a FileAttributes) IsEmpty() bool { return a == FileAttributes{} }

// SetFileAttributes PATCHes the non-empty fields of attrs onto the file at
// filePath, a path reference. It is what a restore uses to give each entry
// back the mode, owner, group and mtime it had in the snapshot.
func (c *Client) SetFileAttributes(ctx context.Context, filePath string, attrs FileAttributes) error {
	if attrs.IsEmpty() {
		return nil
	}
	return c.do(ctx, http.MethodPatch, fmt.Sprintf(fileAttributesPath, fileRef(filePath)), attrs, nil)
}

// SetFileAttributesByID is SetFileAttributes for a file named by its ID,
// which is what the driver holds after creating a directory.
//
// The driver uses it to set a new volume directory's mode and owner in one
// call. A directory created over REST is owned by whoever the API token
// authenticates as -- a local user -- with the cluster's default mode of
// 0755, while an NFS client writes as its own uid and gid: the driver's
// exports map no identity (NFS_MAP_NONE). A non-root pod is therefore
// neither the owner nor in the owning group, and 0755 leaves it no way to
// write; the mode is what makes the volume usable. And replication refuses
// an entry owned by a local user or group with no NFS ID, so a volume a
// dedicated token user created never replicated; the NFS owner is what
// makes it replicable.
//
// The body is a partial update: the endpoint requires no fields, so what is
// not set is left as it is.
func (c *Client) SetFileAttributesByID(ctx context.Context, fileID string, attrs FileAttributes) error {
	if attrs.IsEmpty() {
		return nil
	}
	// fileID is escaped like every other identifier in a route. An ID
	// is decimal digits, so escaping leaves a real one unchanged; what it
	// stops is a malformed one changing which file the PATCH lands on.
	return c.do(ctx, http.MethodPatch, fmt.Sprintf(fileAttributesPath, url.PathEscape(fileID)), attrs, nil)
}
