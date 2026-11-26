// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Command mock-qumulo-server serves an in-memory stand-in for the Qumulo
// Core REST API, covering exactly the endpoints pkg/qumulo's client calls.
//
// It exists so the CSI driver can be exercised end to end -- real gRPC, real
// HTTP, real csi-sanity runs -- without a Qumulo cluster. State lives in
// memory only and is discarded when the process exits.
//
// It is a test double, not a simulator: it enforces the parts of the API the
// driver's correctness depends on (409 on a duplicate directory or quota,
// 404 for a missing file or a finished tree-delete job, snapshot names
// prefixed with the snapshot ID) and nothing else. There is no permission
// model, no capacity accounting and no persistence.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// API paths, mirroring the constants in pkg/qumulo/client.go.
const (
	versionPath      = "/v1/version"
	fileSystemPath   = "/v1/file-system"
	filesPrefix      = "/v1/files/"
	filesResolvePath = "/v1/files/resolve"
	quotasPath       = "/v1/files/quotas/"
	// nfsSettingsPath is the cluster's NFS configuration. Until now this
	// server had no such route at all, so every preflight that reads it --
	// protocol, Kerberos flavour, AUTH_SYS -- went unexercised by the
	// conformance suite.
	nfsSettingsPath = "/v2/nfs/settings"
	// quotaStatusPath is the second of the two quota documents Core serves.
	// The plain object under quotasPath is id and limit; only this one
	// carries capacity_usage (confirmed live on Core 7.9.2.2).
	quotaStatusPath    = "/v1/files/quotas/status/"
	treeDeleteJobsPath = "/v1/tree-delete/jobs/"
	snapshotsPath      = "/v3/snapshots/"
	nfsExportsPath     = "/v3/nfs/exports/"
)

// Suffixes of the two per-file sub-resources the driver uses. They are
// matched after the file reference, which is percent-encoded and so may
// itself contain slashes.
const (
	entriesSuffix    = "/entries/"
	attributesSuffix = "/info/attributes"
)

const fileTypeDirectory = "FS_FILE_TYPE_DIRECTORY"

// firstFileID starts file IDs high enough to catch a consumer that treats
// them as small integers, matching the magnitude a real cluster reports.
const firstFileID = 12884901888

// Capacity GET /v1/file-system reports: a modest 1 TiB cluster, half free.
// GetCapacity has no way to ask for a different number, and none of this
// driver's tests care about the exact value, only that the call succeeds.
const (
	mockTotalSizeBytes = 1 << 40
	mockFreeSizeBytes  = 1 << 39
)

func main() {
	var (
		addr            = flag.String("addr", ":8000", "address to listen on")
		token           = flag.String("token", "", "bearer token to require; empty accepts any request")
		treeDeleteDelay = flag.Duration("tree-delete-delay", 0, "how long a tree-delete job stays in progress; exercises DeleteVolume's polling path")
		entriesPageSize = flag.Int("entries-page-size", 0, "maximum directory entries per listing response; 0 serves everything in one page. "+
			"A small value is the only way the client's paging loop in ListDirectory is ever executed")
		autoCreateParent = flag.Bool("auto-create-parents", true, "create missing ancestors on directory creation, so a store path need not be seeded")
		revision         = flag.String("revision-id", "Qumulo Core 7.2.0", "revision_id reported by /v1/version")
		tlsEnabled       = flag.Bool("tls", false, "serve HTTPS with a self-signed certificate generated at startup. "+
			"The driver derives https://<server>:8000 for any request that carries no apiUrl, so a "+
			"plaintext mock cannot serve those paths. "+
			"Pair with the driver's -insecure-skip-tls-verify, which is what a self-signed cluster certificate needs anyway")
		logLevel       = flag.String("log-level", "info", "log verbosity: debug, info, warn or error")
		nfsV4          = flag.Bool("nfs-v4", true, "report v4_enabled on /v2/nfs/settings")
		krb5           = flag.Bool("krb5", true, "report krb5_enabled on /v2/nfs/settings")
		krb5p          = flag.Bool("krb5p", true, "report krb5p_enabled; set false to model a cluster that takes krb5 but refuses krb5p")
		krb5i          = flag.Bool("krb5i", true, "report krb5i_enabled; set false to model a cluster that takes krb5 but refuses krb5i")
		authSys        = flag.Bool("auth-sys", true, "report auth_sys_enabled; set false to model a cluster that refuses sec=sys")
		idmap          = flag.String("idmap-domain", "", "idmap_domain reported on /v2/nfs/settings; empty reports null")
		replicationLag = flag.Duration("replication-lag", 0, "how far behind the array clock an ESTABLISHED "+
			"relationship's recovery_point trails. Zero means a frozen source drains instantly; a small value "+
			"is the only way the driver's drain-gate polling loop in PromoteVolume is ever executed")
		clusterName = flag.String("cluster-name", "mock-qumulo", "cluster name reported in replication statuses")
		peerAddr    = flag.String("peer-addr", "", "a second address to serve the SAME cluster state on. The "+
			"replication service refuses a class whose target is the volume's own array, so exercising it against "+
			"one mock needs two endpoints; both ends of every relationship live in this one process either way")
		seedExports = flag.String("exports", "/:/", "NFS exports the cluster starts with, as comma-separated "+
			"<exportPath>:<fsPath> pairs. CreateVolume refuses a shared-subdirectory volume that no export "+
			"covers, so a cluster with none can serve only the dedicated-export model -- the default root "+
			"export is what makes the shared model, and therefore csi-sanity, work. Pass an empty value to "+
			"model a cluster where the administrator has not created one")
	)
	flag.Parse()

	if err := setupLogging(*logLevel); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	cluster := newCluster(*revision, *treeDeleteDelay, *autoCreateParent, *entriesPageSize)
	cluster.nfsV4Enabled = *nfsV4
	cluster.krb5Enabled = *krb5
	cluster.krb5pEnabled = *krb5p
	cluster.krb5iEnabled = *krb5i
	cluster.authSysEnabled = *authSys
	cluster.idmapDomain = *idmap
	cluster.replicationLag = *replicationLag
	cluster.clusterName = *clusterName
	if err := cluster.seedExports(*seedExports); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	server := &http.Server{
		Addr: *addr,
		// The handler is used directly rather than through a ServeMux: file
		// references arrive percent-encoded, and a mux would normalise the
		// path and lose the distinction between %2F and a real separator.
		Handler:           withAuth(*token, cluster),
		ReadHeaderTimeout: 10 * time.Second,
	}

	slog.Info("mock qumulo server listening",
		"addr", *addr, "tls", *tlsEnabled, "authRequired", *token != "", "treeDeleteDelay", *treeDeleteDelay)

	if *peerAddr != "" {
		peer := &http.Server{Addr: *peerAddr, Handler: server.Handler, ReadHeaderTimeout: server.ReadHeaderTimeout}
		go func() {
			if err := peer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("peer listener exited", "error", err)
				os.Exit(1)
			}
		}()
		slog.Info("mock qumulo server also listening as the peer array", "peerAddr", *peerAddr)
	}

	var err error
	if *tlsEnabled {
		cert, certErr := selfSignedCert()
		if certErr != nil {
			slog.Error("generating a self-signed certificate", "error", certErr)
			os.Exit(1)
		}
		server.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
		// Empty paths: the certificate is already in TLSConfig.
		err = server.ListenAndServeTLS("", "")
	} else {
		err = server.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server exited", "error", err)
		os.Exit(1)
	}
}

// fileNode is a file or directory in the fake filesystem.
type fileNode struct {
	ID   string `json:"id"`
	Path string `json:"path"`
	Type string `json:"type"`
	// Mode is the POSIX mode as the cluster renders it. A new directory starts
	// at 0755, which is why the driver sets it: the directory is owned by the
	// API user and an NFS client is mapped to another identity.
	Mode string `json:"mode"`
	// Owner and Group are owner_details and group_details. A new directory
	// is owned by the API user, a local user and group, as on a real
	// cluster: that is what the driver replaces with an NFS owner so
	// replication accepts the directory.
	Owner identity `json:"owner_details"`
	Group identity `json:"group_details"`
}

// identity is an owner or group by kind and value, as the attribute
// endpoint renders it.
type identity struct {
	IDType  string `json:"id_type"`
	IDValue string `json:"id_value"`
}

// The API user's identities, as Core renders its built-in admin user (500)
// and Users group (513).
var (
	apiUserOwner = identity{IDType: "LOCAL_USER", IDValue: "500"}
	apiUserGroup = identity{IDType: "LOCAL_GROUP", IDValue: "513"}
)

// treeDeleteJob tracks one asynchronous recursive deletion.
type treeDeleteJob struct {
	fileID string
	doneAt time.Time
}

// cluster is the in-memory state behind the fake API.
type cluster struct {
	revisionID       string
	treeDeleteDelay  time.Duration
	autoCreateParent bool
	// entriesPageSize caps how many directory entries one listing returns.
	// Zero means "everything in one page", which is what this served
	// unconditionally before -- and which meant the driver's paging loop
	// never iterated, in this suite or anywhere else.
	entriesPageSize int

	mu         sync.Mutex
	nextFileID int64
	nextSnapID int64
	// The NFS settings this cluster reports, set from flags so a run can
	// model an array with NFSv4 off, a Kerberos flavour off, or AUTH_SYS off.
	nfsV4Enabled   bool
	krb5Enabled    bool
	krb5pEnabled   bool
	krb5iEnabled   bool
	authSysEnabled bool
	idmapDomain    string

	filesByID map[string]*fileNode
	// idsByPath indexes the same nodes by absolute path, which is how the
	// driver looks a volume up when adopting one after a failed create.
	idsByPath map[string]string
	quotas    map[string]uint64
	snapshots map[string]*snapshot
	delJobs   map[string]*treeDeleteJob
	// exports indexes NFS exports by ID, for the dedicated-export
	// provisioning model.
	exports      map[string]*nfsExport
	nextExportID int64

	// Directory replication, one record per relationship; see replication.go
	// for why one mock holds both ends of each.
	relationships map[string]*relationship
	nextRelSeq    int64
	// replicationLag is how far behind the array clock a healthy
	// relationship's recovery_point sits, so a drain gate has something to
	// wait for.
	replicationLag time.Duration
	// clusterName and clusterUUID are what relationship statuses report for
	// both ends -- the same value, since both ends are this process.
	clusterName string
	clusterUUID string

	// Snapshot policies, for the replication service's policy links.
	policies     map[int64]*snapshotPolicy
	nextPolicyID int64
}

// nfsExport is one NFS export. ExportPath is what clients mount; FSPath is the
// directory it serves, and is what the driver matches on when it has only a
// volume ID to work from.
type nfsExport struct {
	ID          string
	ExportPath  string
	FSPath      string
	Description string
	// Restrictions is stored as the caller sent it and echoed back, so the
	// read_only bit the replication service pre-stages and later flips
	// survives the round trip without the mock modelling the whole shape.
	Restrictions json.RawMessage
}

type snapshot struct {
	ID           string
	Name         string
	SourceFileID string
	CreatedAt    time.Time
	InDelete     bool
	// frozen is the source subtree as it looked when the snapshot was taken,
	// as a parent file ID -> children map.
	//
	// A real Qumulo snapshot outlives the directory it was taken of: that is
	// what makes "snapshot, delete the volume, restore" work, and restoring
	// after a delete is the case the driver had wrong. Without a
	// frozen copy here the mock could not represent that state at all -- the
	// source nodes went away with the live tree, so the bug reproduced only
	// against a real cluster.
	frozen map[string][]*fileNode
}

func newCluster(revisionID string, treeDeleteDelay time.Duration, autoCreateParent bool, entriesPageSize int) *cluster {
	c := &cluster{
		revisionID:       revisionID,
		treeDeleteDelay:  treeDeleteDelay,
		autoCreateParent: autoCreateParent,
		entriesPageSize:  entriesPageSize,
		nfsV4Enabled:     true,
		krb5Enabled:      true,
		krb5pEnabled:     true,
		krb5iEnabled:     true,
		authSysEnabled:   true,
		nextFileID:       firstFileID,
		nextSnapID:       1,
		filesByID:        map[string]*fileNode{},
		idsByPath:        map[string]string{},
		quotas:           map[string]uint64{},
		snapshots:        map[string]*snapshot{},
		delJobs:          map[string]*treeDeleteJob{},
		exports:          map[string]*nfsExport{},
		nextExportID:     1,
		relationships:    map[string]*relationship{},
		nextRelSeq:       1,
		clusterName:      "mock-qumulo",
		clusterUUID:      "00000000-0000-4000-8000-00000000mock",
		policies:         map[int64]*snapshotPolicy{},
		nextPolicyID:     1,
	}
	// Every cluster has a root; without it nothing could ever be created.
	c.addNodeLocked("/")
	return c
}

// ServeHTTP routes a request to its handler.
func (c *cluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// EscapedPath keeps a percent-encoded file reference intact, so a path
	// reference like %2Fcsi%2Fvolumes stays one path segment.
	reqPath := r.URL.EscapedPath()
	slog.Debug("request", "method", r.Method, "path", reqPath)

	switch {
	case reqPath == versionPath:
		c.handleVersion(w, r)
	case reqPath == fileSystemPath:
		c.handleFileSystem(w, r)
	case reqPath == filesResolvePath:
		c.handleResolve(w, r)
	case reqPath == nfsSettingsPath:
		c.handleNFSSettings(w, r)
	case reqPath == quotasPath:
		c.handleQuotaCollection(w, r)
	case strings.HasPrefix(reqPath, quotaStatusPath):
		c.handleQuotaStatus(w, r, strings.TrimPrefix(reqPath, quotaStatusPath))
	case strings.HasPrefix(reqPath, quotasPath):
		c.handleQuota(w, r, strings.TrimPrefix(reqPath, quotasPath))
	case reqPath == treeDeleteJobsPath:
		c.handleTreeDeleteCollection(w, r)
	case strings.HasPrefix(reqPath, treeDeleteJobsPath):
		c.handleTreeDeleteJob(w, r, strings.TrimPrefix(reqPath, treeDeleteJobsPath))
	case reqPath == nfsExportsPath:
		c.handleExportCollection(w, r)
	case strings.HasPrefix(reqPath, nfsExportsPath):
		c.handleExport(w, r, strings.TrimPrefix(reqPath, nfsExportsPath))
	case reqPath == timeStatusPath:
		c.handleTimeStatus(w, r)
	case strings.HasPrefix(reqPath, snapshotPoliciesPath):
		c.handlePolicies(w, r, strings.TrimPrefix(reqPath, snapshotPoliciesPath))
	case c.handleReplication(w, r, reqPath):
		// Routed inside handleReplication.
	case strings.HasPrefix(reqPath, "/v2/snapshots/") && strings.Contains(reqPath, "/changes-since/"):
		parts := strings.Split(strings.Trim(strings.TrimPrefix(reqPath, "/v2/snapshots/"), "/"), "/changes-since/")
		if len(parts) != 2 {
			writeError(w, http.StatusNotFound, "http_not_found_error", reqPath)
			return
		}
		c.handleChangesSince(w, r, parts[0], strings.TrimSuffix(parts[1], "/"))
	case reqPath == snapshotsPath:
		c.handleSnapshotCollection(w, r)
	case strings.HasPrefix(reqPath, snapshotsPath):
		c.handleSnapshot(w, r, strings.TrimPrefix(reqPath, snapshotsPath))
	// Quota paths also live under /v1/files/, so the file cases must come
	// after them.
	case strings.HasPrefix(reqPath, filesPrefix) && strings.HasSuffix(reqPath, entriesSuffix):
		c.handleEntries(w, r, strings.TrimSuffix(strings.TrimPrefix(reqPath, filesPrefix), entriesSuffix))
	case strings.HasPrefix(reqPath, filesPrefix) && strings.HasSuffix(reqPath, attributesSuffix):
		c.handleAttributes(w, r, strings.TrimSuffix(strings.TrimPrefix(reqPath, filesPrefix), attributesSuffix))
	default:
		writeError(w, http.StatusNotFound, "no_such_path_error", "unknown path "+reqPath)
	}
}

func (c *cluster) handleVersion(w http.ResponseWriter, r *http.Request) {
	if !methodIs(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"revision_id": c.revisionID,
		"build_id":    "mock",
	})
}

// handleFileSystem implements GET /v1/file-system, which GetCapacity reads.
func (c *cluster) handleFileSystem(w http.ResponseWriter, r *http.Request) {
	if !methodIs(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"total_size_bytes": strconv.FormatUint(mockTotalSizeBytes, 10),
		"free_size_bytes":  strconv.FormatUint(mockFreeSizeBytes, 10),
	})
}

// handleEntries dispatches POST (create a child) and GET (list children) on
// /v1/files/<ref>/entries/ -- the same collection resource serves both.
// seedExports populates the export table from the -exports flag.
//
// The mock needs this because CreateVolume now requires a shared-subdirectory
// volume to be covered by an export, the way a real cluster does: without one
// the volume would provision, Bind, and then fail to mount. A real cluster's
// exports are created by an administrator long before any PVC, so the mock
// has to be able to start with them rather than only ever growing them
// through the API.
//
// Seeding goes straight into the map rather than through handleCreateExport,
// because that path requires the fs_path directory to exist already and the
// point of a seeded export is to be there before anything else is.
func (c *cluster) seedExports(spec string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pair := range strings.Split(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		// SplitN from the right would be wrong: both halves are absolute
		// paths and neither contains a colon, so the first one separates.
		exportPath, fsPath, ok := strings.Cut(pair, ":")
		if !ok || !strings.HasPrefix(exportPath, "/") || !strings.HasPrefix(fsPath, "/") {
			return fmt.Errorf("-exports: %q is not an <exportPath>:<fsPath> pair of absolute paths", pair)
		}

		id := strconv.FormatInt(c.nextExportID, 10)
		c.nextExportID++
		c.exports[id] = &nfsExport{
			ID:          id,
			ExportPath:  exportPath,
			FSPath:      fsPath,
			Description: "seeded by -exports",
		}
		slog.Info("seeded export", "id", id, "exportPath", exportPath, "fsPath", fsPath)
	}
	return nil
}

func (c *cluster) handleEntries(w http.ResponseWriter, r *http.Request, ref string) {
	switch r.Method {
	case http.MethodPost:
		c.handleCreateEntry(w, r, ref)
	case http.MethodGet:
		c.handleListEntries(w, r, ref)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed_error", "unsupported method "+r.Method)
	}
}

// handleListEntries implements GET /v1/files/<ref>/entries/, listing the
// direct children of a directory. Used by ListDirectory, which a snapshot
// restore walks to copy a source tree.
//
// A snapshot= query parameter is answered from that snapshot's frozen copy of
// the tree rather than from the live one. The frozen copy is what lets the
// mock represent the state a restore-after-delete bug was about: a snapshot whose source
// directory has since been tree-deleted is still listable by file ID, exactly
// as a real cluster leaves it, so a restore after a delete can be exercised
// here rather than only against a cluster.
//
// The time-travel is shallow: the copy is taken at snapshot time and never
// diverges, so this proves a restore can read a *deleted* source but not that
// it reads a source unaffected by later *writes* -- nothing here writes to a
// snapshotted tree. That second property still needs test/live.
func (c *cluster) handleListEntries(w http.ResponseWriter, r *http.Request, ref string) {
	parentRef, err := decodeRef(ref)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_reference_error", err.Error())
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.reapFinishedJobsLocked()

	children, ok := c.childrenForListingLocked(parentRef, r.URL.Query().Get("snapshot"))
	if !ok {
		writeError(w, http.StatusNotFound, "fs_no_such_entry_error", "no such file "+parentRef)
		return
	}
	sort.Slice(children, func(i, j int) bool { return children[i].Path < children[j].Path })

	// Resume where the previous page ended. Cursor is the last path served,
	// which works because the listing is sorted and stable.
	query := r.URL.Query()
	if after := query.Get("after"); after != "" {
		i := 0
		for i < len(children) && children[i].Path <= after {
			i++
		}
		children = children[i:]
	}

	// Cut the page and build the token for the next one.
	//
	// Off unless -entries-page-size says otherwise, so the default stays what
	// it was. When it is on, the listing is genuinely split, and that is the
	// point: a single always-empty next token is a valid implementation of
	// the wire format but it means the driver's loop body runs exactly once,
	// so a restore that needed a second page would have silently produced a
	// partial volume with nothing to catch it.
	nextURI := ""
	if c.entriesPageSize > 0 && len(children) > c.entriesPageSize {
		children = children[:c.entriesPageSize]

		nextQuery := url.Values{}
		// Carried forward because the driver feeds this URI back verbatim and
		// reads no other state: a next page that dropped ?snapshot would read
		// the directory at head, mid-restore.
		if limit := query.Get("limit"); limit != "" {
			nextQuery.Set("limit", limit)
		}
		if snapshotID := query.Get("snapshot"); snapshotID != "" {
			nextQuery.Set("snapshot", snapshotID)
		}
		nextQuery.Set("after", children[len(children)-1].Path)

		// EscapedPath rather than Path: a file reference arrives
		// percent-encoded, and this URI goes straight back to the driver, so
		// decoding here would lose the %2F-versus-separator distinction the
		// router depends on.
		nextURI = r.URL.EscapedPath() + "?" + nextQuery.Encode()
	}

	files := make([]map[string]string, 0, len(children))
	for _, node := range children {
		files = append(files, map[string]string{
			"name": path.Base(node.Path),
			"path": node.Path,
			"id":   node.ID,
			"type": node.Type,
			"size": "0",
			"mode": node.Mode,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"files":  files,
		"paging": map[string]string{"next": nextURI},
	})
}

// childrenForListingLocked returns the direct children of parentRef, from the
// named snapshot's frozen tree when one is given and from the live tree
// otherwise. The bool reports whether the parent was found at all.
func (c *cluster) childrenForListingLocked(parentRef, snapshotID string) ([]*fileNode, bool) {
	if snapshotID != "" {
		snap, ok := c.snapshots[snapshotID]
		if !ok {
			return nil, false
		}
		// Only by ID: the frozen tree is keyed by file ID, and a path-based
		// lookup would go through idsByPath, which a tree-delete has already
		// emptied for exactly the directories this exists to serve.
		children, ok := snap.frozen[parentRef]
		return children, ok
	}

	parent := c.lookupLocked(parentRef)
	if parent == nil {
		return nil, false
	}

	prefix := strings.TrimSuffix(parent.Path, "/") + "/"
	var children []*fileNode
	for _, node := range c.filesByID {
		if node.Path == parent.Path || !strings.HasPrefix(node.Path, prefix) {
			continue
		}
		if strings.Contains(strings.TrimPrefix(node.Path, prefix), "/") {
			continue // a grandchild, not a direct child
		}
		children = append(children, node)
	}
	return children, true
}

// handleCreateEntry implements POST /v1/files/<ref>/entries/, handling both
// actions the driver sends here: CREATE_DIRECTORY (CreateVolume) and RENAME
// (committing a snapshot restore atomically).
func (c *cluster) handleCreateEntry(w http.ResponseWriter, r *http.Request, ref string) {
	parentRef, err := decodeRef(ref)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_reference_error", err.Error())
		return
	}

	var body struct {
		Name    string `json:"name"`
		Action  string `json:"action"`
		OldPath string `json:"old_path"`
		Clobber bool   `json:"clobber"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_body_error", err.Error())
		return
	}
	if body.Name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request_body_error", "name is required")
		return
	}
	if body.Action != "CREATE_DIRECTORY" && body.Action != "RENAME" {
		writeError(w, http.StatusBadRequest, "unsupported_action_error",
			"only CREATE_DIRECTORY and RENAME are supported, got "+body.Action)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.reapFinishedJobsLocked()

	// The reference is either an encoded absolute path or a file ID, and the
	// two have to behave the same here as they do on a cluster. Resolving an
	// ID is what stops it being read as a path, which would create a
	// directory literally named after the ID.
	var parentPath string
	if strings.HasPrefix(parentRef, "/") {
		parentPath = normalizePath(parentRef)
		if _, ok := c.idsByPath[parentPath]; !ok {
			// Auto-creation is path-only: an unseeded store path can be
			// conjured up, an unknown file ID cannot.
			if !c.autoCreateParent {
				writeError(w, http.StatusNotFound, "fs_no_such_entry_error", "no such directory "+parentPath)
				return
			}
			c.addAncestorsLocked(parentPath)
		}
	} else {
		parent := c.lookupLocked(parentRef)
		if parent == nil {
			writeError(w, http.StatusNotFound, "fs_no_such_entry_error", "no such file "+parentRef)
			return
		}
		parentPath = parent.Path
	}

	if body.Action == "RENAME" {
		c.renameLocked(w, parentPath, body.Name, body.OldPath, body.Clobber)
		return
	}

	childPath := path.Join(parentPath, body.Name)
	if id, exists := c.idsByPath[childPath]; exists {
		// A real cluster reports the name collision this way, and the
		// driver's CreateVolume idempotency depends on seeing it.
		writeError(w, http.StatusConflict, "fs_entry_exists_error",
			fmt.Sprintf("%s already exists as file %s", childPath, id))
		return
	}

	node := c.addNodeLocked(childPath)
	slog.Info("created directory", "path", node.Path, "fileID", node.ID)
	writeJSON(w, http.StatusOK, node)
}

// renameLocked implements the RENAME action: moving oldPath (and everything
// beneath it) to newParentPath/name. Called with c.mu already held.
func (c *cluster) renameLocked(w http.ResponseWriter, newParentPath, name, oldPath string, clobber bool) {
	source := c.lookupLocked(oldPath)
	if source == nil {
		writeError(w, http.StatusNotFound, "fs_no_such_entry_error", "no such file "+oldPath)
		return
	}

	newPath := path.Join(newParentPath, name)
	if existingID, exists := c.idsByPath[newPath]; exists {
		if !clobber {
			writeError(w, http.StatusConflict, "fs_entry_exists_error",
				fmt.Sprintf("%s already exists as file %s", newPath, existingID))
			return
		}
		c.removeSubtreeLocked(newPath)
	}

	oldRoot := source.Path
	for _, node := range c.filesByID {
		if node.Path != oldRoot && !strings.HasPrefix(node.Path, strings.TrimSuffix(oldRoot, "/")+"/") {
			continue
		}
		delete(c.idsByPath, node.Path)
		node.Path = newPath + strings.TrimPrefix(node.Path, oldRoot)
		c.idsByPath[node.Path] = node.ID
	}

	slog.Info("renamed", "from", oldRoot, "to", newPath, "fileID", source.ID)
	writeJSON(w, http.StatusOK, source)
}

// handleAttributes implements GET /v1/files/<ref>/info/attributes, where the
// reference is either an encoded path or a file ID.
func (c *cluster) handleAttributes(w http.ResponseWriter, r *http.Request, ref string) {
	if r.Method == http.MethodPatch {
		c.patchAttributes(w, r, ref)
		return
	}
	if !methodIs(w, r, http.MethodGet) {
		return
	}

	decoded, err := decodeRef(ref)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_reference_error", err.Error())
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.reapFinishedJobsLocked()

	node := c.lookupLocked(decoded)
	if node == nil {
		writeError(w, http.StatusNotFound, "fs_no_such_entry_error", "no such file "+decoded)
		return
	}
	writeJSON(w, http.StatusOK, node)
}

// handleResolve implements POST /v1/files/resolve. IDs that do not resolve
// come back with an empty path rather than failing the batch, which is what
// the client documents.
func (c *cluster) handleResolve(w http.ResponseWriter, r *http.Request) {
	if !methodIs(w, r, http.MethodPost) {
		return
	}

	var fileIDs []string
	if err := json.NewDecoder(r.Body).Decode(&fileIDs); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_body_error", err.Error())
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.reapFinishedJobsLocked()

	resolved := make([]map[string]string, 0, len(fileIDs))
	for _, id := range fileIDs {
		entry := map[string]string{"id": id, "path": ""}
		if node, ok := c.filesByID[id]; ok {
			entry["path"] = node.Path
		}
		resolved = append(resolved, entry)
	}
	writeJSON(w, http.StatusOK, resolved)
}

// handleTreeDeleteCollection implements POST /v1/tree-delete/jobs/.
func (c *cluster) handleTreeDeleteCollection(w http.ResponseWriter, r *http.Request) {
	if !methodIs(w, r, http.MethodPost) {
		return
	}

	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_body_error", err.Error())
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.reapFinishedJobsLocked()

	node, ok := c.filesByID[body.ID]
	if !ok {
		writeError(w, http.StatusNotFound, "fs_no_such_entry_error", "no such file "+body.ID)
		return
	}

	// Restarting an in-flight job is harmless on a real cluster and the
	// driver relies on that when a DeleteVolume is retried, so an existing
	// job is simply left to run.
	if _, running := c.delJobs[body.ID]; !running {
		c.delJobs[body.ID] = &treeDeleteJob{fileID: body.ID, doneAt: time.Now().Add(c.treeDeleteDelay)}
		slog.Info("started tree-delete", "path", node.Path, "fileID", body.ID, "delay", c.treeDeleteDelay)
	}
	c.reapFinishedJobsLocked()

	writeJSON(w, http.StatusOK, map[string]string{"id": body.ID})
}

// handleTreeDeleteJob implements GET /v1/tree-delete/jobs/<id>. A finished
// or unknown job is a 404, which the client maps to "finished", with the
// class a real array sends for it: the generic http_not_found_error, not a
// job-specific class. A mock answering a class no array sends is how
// a client narrowed to that class would pass here and fail on every array.
func (c *cluster) handleTreeDeleteJob(w http.ResponseWriter, r *http.Request, fileID string) {
	if !methodIs(w, r, http.MethodGet) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.reapFinishedJobsLocked()

	job, ok := c.delJobs[fileID]
	if !ok {
		writeError(w, http.StatusNotFound, "http_not_found_error", "no tree-delete job for file "+fileID)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": job.fileID, "status": "IN_PROGRESS"})
}

// handleQuotaCollection implements POST /v1/files/quotas/.
func (c *cluster) handleQuotaCollection(w http.ResponseWriter, r *http.Request) {
	if !methodIs(w, r, http.MethodPost) {
		return
	}

	fileID, limit, ok := decodeQuotaBody(w, r)
	if !ok {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.reapFinishedJobsLocked()

	if _, exists := c.filesByID[fileID]; !exists {
		writeError(w, http.StatusNotFound, "fs_no_such_entry_error", "no such file "+fileID)
		return
	}
	if _, exists := c.quotas[fileID]; exists {
		writeError(w, http.StatusConflict, "fs_quota_exists_error", "quota already exists on file "+fileID)
		return
	}

	c.quotas[fileID] = limit
	slog.Info("created quota", "fileID", fileID, "limitBytes", limit)
	writeJSON(w, http.StatusOK, quotaResponse(fileID, limit))
}

// handleQuota implements GET, PUT and DELETE on /v1/files/quotas/<fileID>.
func (c *cluster) handleQuota(w http.ResponseWriter, r *http.Request, fileID string) {
	fileID, err := decodeRef(fileID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_reference_error", err.Error())
		return
	}

	switch r.Method {
	case http.MethodGet:
		c.mu.Lock()
		defer c.mu.Unlock()
		limit, ok := c.quotas[fileID]
		if !ok {
			writeError(w, http.StatusNotFound, "api_quotas_quota_limit_not_found_error", "no quota on file "+fileID)
			return
		}
		writeJSON(w, http.StatusOK, quotaResponse(fileID, limit))

	case http.MethodPut:
		_, limit, ok := decodeQuotaBody(w, r)
		if !ok {
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, exists := c.quotas[fileID]; !exists {
			writeError(w, http.StatusNotFound, "api_quotas_quota_limit_not_found_error", "no quota on file "+fileID)
			return
		}
		c.quotas[fileID] = limit
		slog.Info("updated quota", "fileID", fileID, "limitBytes", limit)
		writeJSON(w, http.StatusOK, quotaResponse(fileID, limit))

	case http.MethodDelete:
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, exists := c.quotas[fileID]; !exists {
			writeError(w, http.StatusNotFound, "api_quotas_quota_limit_not_found_error", "no quota on file "+fileID)
			return
		}
		delete(c.quotas, fileID)
		slog.Info("deleted quota", "fileID", fileID)
		w.WriteHeader(http.StatusOK)

	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed_error", "unsupported method "+r.Method)
	}
}

// handleSnapshotCollection implements POST and GET on /v3/snapshots/.
func (c *cluster) handleSnapshotCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var body struct {
			SourceFileID string `json:"source_file_id"`
			NameSuffix   string `json:"name_suffix"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request_body_error", err.Error())
			return
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		c.reapFinishedJobsLocked()

		if _, exists := c.filesByID[body.SourceFileID]; !exists {
			writeError(w, http.StatusNotFound, "fs_no_such_entry_error", "no such file "+body.SourceFileID)
			return
		}

		id := strconv.FormatInt(c.nextSnapID, 10)
		c.nextSnapID++
		// The cluster prepends the snapshot ID to the requested suffix; the
		// driver matches on the suffix precisely because of this.
		snap := &snapshot{
			ID:           id,
			Name:         id + "_" + body.NameSuffix,
			SourceFileID: body.SourceFileID,
			CreatedAt:    time.Now().UTC(),
			frozen:       c.freezeSubtreeLocked(body.SourceFileID),
		}
		c.snapshots[id] = snap
		slog.Info("created snapshot", "id", id, "name", snap.Name, "sourceFileID", snap.SourceFileID)
		writeJSON(w, http.StatusOK, snapshotResponse(snap))

	case http.MethodGet:
		excludeInDelete := r.URL.Query().Get("filter") == "exclude_in_delete"

		c.mu.Lock()
		defer c.mu.Unlock()

		ids := make([]string, 0, len(c.snapshots))
		for id := range c.snapshots {
			ids = append(ids, id)
		}
		// Sort numerically so listings are stable and in creation order,
		// which is what the driver's paging tokens assume.
		sort.Slice(ids, func(i, j int) bool { return snapIDLess(ids[i], ids[j]) })

		entries := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			snap := c.snapshots[id]
			if excludeInDelete && snap.InDelete {
				continue
			}
			entries = append(entries, snapshotResponse(snap))
		}
		writeJSON(w, http.StatusOK, map[string]any{"entries": entries})

	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed_error", "unsupported method "+r.Method)
	}
}

// handleSnapshot implements GET and DELETE on /v3/snapshots/<id>.
func (c *cluster) handleSnapshot(w http.ResponseWriter, r *http.Request, id string) {
	id = strings.TrimSuffix(id, "/")

	c.mu.Lock()
	defer c.mu.Unlock()

	snap, ok := c.snapshots[id]
	if !ok {
		// The class a real cluster sends (Core 7.10.0: "This snapshot no
		// longer exists."), which the driver's DeleteSnapshot keys on.
		writeError(w, http.StatusNotFound, "snapshot_not_found_error", "no such snapshot "+id)
		return
	}

	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, snapshotResponse(snap))
	case http.MethodDelete:
		delete(c.snapshots, id)
		slog.Info("deleted snapshot", "id", id)
		w.WriteHeader(http.StatusOK)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed_error", "unsupported method "+r.Method)
	}
}

// addNodeLocked creates a directory node at the given absolute path.
func (c *cluster) addNodeLocked(dirPath string) *fileNode {
	id := strconv.FormatInt(c.nextFileID, 10)
	c.nextFileID++

	// 0755 is what a real cluster gives a new directory, and reproducing it is
	// the point: a permissive default here would hide the reason the driver
	// sets the mode at all.
	node := &fileNode{
		ID: id, Path: normalizePath(dirPath), Type: fileTypeDirectory, Mode: "0755",
		Owner: apiUserOwner, Group: apiUserGroup,
	}
	c.filesByID[id] = node
	c.idsByPath[node.Path] = id
	return node
}

// addAncestorsLocked creates every missing directory along dirPath, the
// equivalent of mkdir -p, so a store path need not be seeded before use.
func (c *cluster) addAncestorsLocked(dirPath string) {
	dirPath = normalizePath(dirPath)

	current := "/"
	for _, segment := range strings.Split(strings.Trim(dirPath, "/"), "/") {
		if segment == "" {
			continue
		}
		current = path.Join(current, segment)
		if _, ok := c.idsByPath[current]; !ok {
			node := c.addNodeLocked(current)
			slog.Info("auto-created parent directory", "path", node.Path, "fileID", node.ID)
		}
	}
}

// lookupLocked resolves a reference that is either an absolute path or a
// file ID, which is how the API treats that position.
func (c *cluster) lookupLocked(ref string) *fileNode {
	if strings.HasPrefix(ref, "/") {
		id, ok := c.idsByPath[normalizePath(ref)]
		if !ok {
			return nil
		}
		return c.filesByID[id]
	}
	return c.filesByID[ref]
}

// reapFinishedJobsLocked applies any tree-delete jobs whose delay has
// elapsed, removing the directory and everything beneath it along with the
// quotas and snapshots that referenced it.
func (c *cluster) reapFinishedJobsLocked() {
	now := time.Now()
	for fileID, job := range c.delJobs {
		if now.Before(job.doneAt) {
			continue
		}
		delete(c.delJobs, fileID)

		node, ok := c.filesByID[fileID]
		if !ok {
			continue
		}
		c.removeSubtreeLocked(node.Path)
		slog.Info("tree-delete finished", "path", node.Path, "fileID", fileID)
	}
}

func (c *cluster) removeSubtreeLocked(root string) {
	root = normalizePath(root)

	for id, node := range c.filesByID {
		if node.Path != root && !strings.HasPrefix(node.Path, strings.TrimSuffix(root, "/")+"/") {
			continue
		}
		delete(c.filesByID, id)
		delete(c.idsByPath, node.Path)
		delete(c.quotas, id)
	}
	c.endRelationshipsUnderLocked(root)

	// Snapshots of the deleted subtree deliberately survive. A real cluster
	// keeps them -- deleting a directory does not delete its snapshots, which
	// is the entire premise of restoring from one after the volume is gone --
	// and each one carries its own frozen copy of the tree, so it stays
	// listable by file ID even though the live path has just stopped
	// resolving. This used to delete them, which made the mock quietly
	// stricter than the cluster and hid that bug.
}

// freezeSubtreeLocked copies the subtree rooted at fileID into a parent ID ->
// children map, so a snapshot can be listed after the live tree is gone.
func (c *cluster) freezeSubtreeLocked(fileID string) map[string][]*fileNode {
	root, ok := c.filesByID[fileID]
	if !ok {
		return nil
	}

	frozen := map[string][]*fileNode{}
	prefix := strings.TrimSuffix(root.Path, "/") + "/"
	for _, node := range c.filesByID {
		if node.Path == root.Path || !strings.HasPrefix(node.Path, prefix) {
			continue
		}
		parentID, ok := c.idsByPath[normalizePath(path.Dir(node.Path))]
		if !ok {
			continue
		}
		copied := *node
		frozen[parentID] = append(frozen[parentID], &copied)
	}
	// The root itself must appear even when empty, so a listing of an empty
	// snapshot is an empty result rather than a 404.
	if _, ok := frozen[fileID]; !ok {
		frozen[fileID] = nil
	}
	for _, children := range frozen {
		sort.Slice(children, func(i, j int) bool { return children[i].Path < children[j].Path })
	}
	return frozen
}

// withAuth rejects requests without the expected bearer token. An empty
// token disables the check, which is the default so a quick curl works.
func withAuth(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			writeError(w, http.StatusUnauthorized, "unauthorized_error", "invalid or missing bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// decodeRef percent-decodes a file reference. The client encodes a path with
// url.PathEscape, so the separators arrive as %2F.
func decodeRef(ref string) (string, error) {
	decoded, err := url.PathUnescape(ref)
	if err != nil {
		return "", fmt.Errorf("reference %q is not valid percent-encoding: %w", ref, err)
	}
	if decoded == "" {
		return "", errors.New("reference is empty")
	}
	return decoded, nil
}

// decodeQuotaBody reads a quota request body. The limit is a decimal string
// on the wire, matching the client's 64-bit-safe encoding.
func decodeQuotaBody(w http.ResponseWriter, r *http.Request) (fileID string, limit uint64, ok bool) {
	var body struct {
		ID    string `json:"id"`
		Limit string `json:"limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_body_error", err.Error())
		return "", 0, false
	}

	parsed, err := strconv.ParseUint(body.Limit, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_body_error",
			fmt.Sprintf("limit %q is not a decimal byte count", body.Limit))
		return "", 0, false
	}
	return body.ID, parsed, true
}

// quotaResponse renders a quota the way the API does.
//
// capacity_usage is deliberately absent: Core 7.9.2.2 serves it on the quota
// *status* document, not here (confirmed live). Attaching it to this response
// -- as this server did until now -- let csi-sanity and the integration tests
// exercise a body the array has never sent, which is why the driver reading
// the wrong URL went unnoticed. See quotaStatusResponse.
func quotaResponse(fileID string, limit uint64) map[string]string {
	return map[string]string{
		"id":    fileID,
		"limit": strconv.FormatUint(limit, 10),
	}
}

// quotaStatusResponse renders the quota *status* document, which is where
// capacity_usage actually lives.
//
// The figure is a fixed fraction of the limit rather than anything tracked:
// this server stores no file contents, so there is no real usage to report,
// and a constant would make the arithmetic under test indistinguishable from
// a hardcoded answer.
func quotaStatusResponse(fileID, path string, limit uint64) map[string]string {
	return map[string]string{
		"id":             fileID,
		"path":           path,
		"limit":          strconv.FormatUint(limit, 10),
		"capacity_usage": strconv.FormatUint(limit/4, 10),
	}
}

// handleNFSSettings implements GET /v2/nfs/settings.
//
// The Kerberos flavours are three switches, not one: a cluster can accept
// krb5 and refuse krb5p. They are served separately so a driver that gates
// only on krb5_enabled can be caught doing it.
func (c *cluster) handleNFSSettings(w http.ResponseWriter, r *http.Request) {
	if !methodIs(w, r, http.MethodGet) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	body := map[string]any{
		"v4_enabled":       c.nfsV4Enabled,
		"krb5_enabled":     c.krb5Enabled,
		"krb5p_enabled":    c.krb5pEnabled,
		"krb5i_enabled":    c.krb5iEnabled,
		"auth_sys_enabled": c.authSysEnabled,
		"idmap_domain":     nil,
	}
	if c.idmapDomain != "" {
		body["idmap_domain"] = c.idmapDomain
	}
	writeJSON(w, http.StatusOK, body)
}

// handleQuotaStatus implements GET /v1/files/quotas/status/<fileID>.
func (c *cluster) handleQuotaStatus(w http.ResponseWriter, r *http.Request, fileID string) {
	fileID, err := decodeRef(fileID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_reference_error", err.Error())
		return
	}
	if !methodIs(w, r, http.MethodGet) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	limit, ok := c.quotas[fileID]
	if !ok {
		writeError(w, http.StatusNotFound, "api_quotas_quota_limit_not_found_error", "no quota on file "+fileID)
		return
	}
	var path string
	if node, ok := c.filesByID[fileID]; ok {
		path = node.Path
	}
	writeJSON(w, http.StatusOK, quotaStatusResponse(fileID, path, limit))
}

// snapshotResponse renders a snapshot the way the API does, notably with a
// bare numeric ID -- unlike file and quota IDs, which are strings.
func snapshotResponse(snap *snapshot) map[string]any {
	id, err := strconv.ParseInt(snap.ID, 10, 64)
	if err != nil {
		return map[string]any{
			"id":             snap.ID,
			"name":           snap.Name,
			"source_file_id": snap.SourceFileID,
			"timestamp":      snap.CreatedAt.Format(time.RFC3339Nano),
			"in_delete":      snap.InDelete,
		}
	}
	return map[string]any{
		"id":             id,
		"name":           snap.Name,
		"source_file_id": snap.SourceFileID,
		"timestamp":      snap.CreatedAt.Format(time.RFC3339Nano),
		"in_delete":      snap.InDelete,
	}
}

func snapIDLess(a, b string) bool {
	na, errA := strconv.ParseInt(a, 10, 64)
	nb, errB := strconv.ParseInt(b, 10, 64)
	if errA != nil || errB != nil {
		return a < b
	}
	return na < nb
}

func normalizePath(p string) string {
	if p == "" {
		return "/"
	}
	cleaned := path.Clean(p)
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	return cleaned
}

func methodIs(w http.ResponseWriter, r *http.Request, want string) bool {
	if r.Method != want {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed_error",
			fmt.Sprintf("%s not allowed here, want %s", r.Method, want))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, statusCode int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("encoding response", "error", err)
	}
}

// writeError renders the error envelope Qumulo Core returns, so the client's
// APIError carries a description the same way it would against a cluster.
func writeError(w http.ResponseWriter, statusCode int, errorClass, description string) {
	writeJSON(w, statusCode, map[string]string{
		"description": description,
		"error_class": errorClass,
	})
}

func setupLogging(level string) error {
	var parsed slog.Level
	if err := parsed.UnmarshalText([]byte(level)); err != nil {
		return fmt.Errorf("invalid -log-level %q: want debug, info, warn or error", level)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parsed})))
	return nil
}

// handleExportCollection serves GET and POST on /v3/nfs/exports/.
func (c *cluster) handleExportCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		c.listExports(w)
	case http.MethodPost:
		c.createExport(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", r.Method+" not allowed")
	}
}

// listExports answers with the entries envelope v3 uses -- v2 returns a bare
// array, and a client written for one silently decodes nothing from the other.
func (c *cluster) listExports(w http.ResponseWriter) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entries := make([]map[string]any, 0, len(c.exports))
	for _, export := range c.exports {
		entries = append(entries, exportResponse(export))
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

// createExport mirrors the real cluster closely enough to be useful, which
// means being strict about the four mandatory fields.
//
// Qumulo Core 7.9.3 requires export_path, fs_path, description and
// restrictions to be *present*, and names the first one missing. description
// counts even when empty, so an omitempty on the request struct breaks every
// create -- the same failure that made CreateSnapshot unusable. A permissive
// fake would hide it, so this one does not.
func (c *cluster) createExport(w http.ResponseWriter, r *http.Request) {
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "decode_error", err.Error())
		return
	}
	for _, field := range []string{"export_path", "fs_path", "description", "restrictions"} {
		if _, present := raw[field]; !present {
			writeError(w, http.StatusBadRequest, "decode_error", "field '"+field+"' not found")
			return
		}
	}

	var req struct {
		ExportPath   string          `json:"export_path"`
		FSPath       string          `json:"fs_path"`
		Description  string          `json:"description"`
		Restrictions json.RawMessage `json:"restrictions"`
	}
	if err := json.Unmarshal(mustMarshalRaw(raw), &req); err != nil {
		writeError(w, http.StatusBadRequest, "decode_error", err.Error())
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// The cluster will not create the directory an export serves, and reports
	// it as a 400 rather than a 404.
	if _, exists := c.idsByPath[req.FSPath]; !exists {
		writeError(w, http.StatusBadRequest, "nfs_export_fs_path_doesnt_exist_error",
			"The directory does not exist. Check for typos or choose to create a new directory.")
		return
	}
	for _, existing := range c.exports {
		if existing.ExportPath == req.ExportPath {
			writeError(w, http.StatusConflict, "nfs_export_already_exists_error",
				"Export path already exists. Choose a unique name and try again.")
			return
		}
	}

	export := &nfsExport{
		ID:           strconv.FormatInt(c.nextExportID, 10),
		ExportPath:   req.ExportPath,
		FSPath:       req.FSPath,
		Description:  req.Description,
		Restrictions: req.Restrictions,
	}
	c.nextExportID++
	c.exports[export.ID] = export

	slog.Info("created export", "id", export.ID, "exportPath", export.ExportPath, "fsPath", export.FSPath)

	// 200, not 201: the real cluster does the same.
	writeJSON(w, http.StatusOK, exportResponse(export))
}

// exportResponse renders one export as the v3 API does, restrictions included.
func exportResponse(export *nfsExport) map[string]any {
	restrictions := export.Restrictions
	if len(restrictions) == 0 {
		// A seeded export has none recorded; render the permissive default a
		// real administrator-created export carries.
		restrictions = json.RawMessage(`[{"host_restrictions":[],"read_only":false,` +
			`"require_privileged_port":false,"required_authentication_mode":"AUTHENTICATION_MODE_NONE",` +
			`"user_mapping":"NFS_MAP_NONE"}]`)
	}
	return map[string]any{
		"id":           export.ID,
		"export_path":  export.ExportPath,
		"fs_path":      export.FSPath,
		"description":  export.Description,
		"tenant_id":    1,
		"restrictions": restrictions,
	}
}

// handleExport serves GET and DELETE on a single export, addressed by ID --
// v3 takes an ID here, unlike v2 which also accepts a path reference.
func (c *cluster) handleExport(w http.ResponseWriter, r *http.Request, exportID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	export, ok := c.exports[exportID]
	if !ok {
		writeError(w, http.StatusNotFound, "nfs_export_not_found_error", "no such export "+exportID)
		return
	}

	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, exportResponse(export))
	case http.MethodPut:
		// A full replace, as v3 defines it. Only the fields the mock models
		// are applied; the rest of the body is accepted and dropped.
		var req struct {
			ID           *string         `json:"id"`
			Description  string          `json:"description"`
			Restrictions json.RawMessage `json:"restrictions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "decode_error", err.Error())
			return
		}
		// Core 7.9.2.2 requires the export's id in a PUT body even though it
		// is also in the path, and answers exactly this when it is missing
		// (measured on a lab array). The mock used
		// to accept a body without it, which let a client bug through.
		if req.ID == nil {
			writeError(w, http.StatusBadRequest, "decode_error", "field 'id' not found")
			return
		}
		if *req.ID != exportID {
			writeError(w, http.StatusBadRequest, "decode_error", "field 'id' does not match the path")
			return
		}
		export.Description = req.Description
		export.Restrictions = req.Restrictions
		slog.Info("replaced export", "id", exportID, "exportPath", export.ExportPath)
		writeJSON(w, http.StatusOK, exportResponse(export))
	case http.MethodDelete:
		delete(c.exports, exportID)
		slog.Info("deleted export", "id", exportID, "exportPath", export.ExportPath)
		writeJSON(w, http.StatusOK, map[string]any{})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", r.Method+" not allowed")
	}
}

func mustMarshalRaw(raw map[string]json.RawMessage) []byte {
	encoded, err := json.Marshal(raw)
	if err != nil {
		panic(err)
	}
	return encoded
}

// patchAttributes implements PATCH /v1/files/{ref}/info/attributes, which the
// driver uses to make a new volume directory writable.
//
// The body is a partial update -- the real API requires no fields -- so only
// what is present is applied: the mode, and the owner and group by kind and
// value, which the driver sets on a directory it creates.
func (c *cluster) patchAttributes(w http.ResponseWriter, r *http.Request, ref string) {
	decoded, err := decodeRef(ref)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_reference_error", err.Error())
		return
	}

	var req struct {
		Mode  string    `json:"mode"`
		Owner *identity `json:"owner_details"`
		Group *identity `json:"group_details"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_body_error", err.Error())
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.reapFinishedJobsLocked()

	node := c.lookupLocked(decoded)
	if node == nil {
		writeError(w, http.StatusNotFound, "fs_no_such_inode_error", "no such file "+decoded)
		return
	}
	if req.Mode != "" {
		node.Mode = req.Mode
		slog.Info("set mode", "fileID", node.ID, "path", node.Path, "mode", node.Mode)
	}
	if req.Owner != nil {
		node.Owner = *req.Owner
	}
	if req.Group != nil {
		node.Group = *req.Group
	}
	if req.Owner != nil || req.Group != nil {
		slog.Info("set owner", "fileID", node.ID, "path", node.Path,
			"owner", node.Owner.IDType+":"+node.Owner.IDValue, "group", node.Group.IDType+":"+node.Group.IDValue)
	}
	writeJSON(w, http.StatusOK, node)
}

// selfSignedCert makes a throwaway certificate for 127.0.0.1 and localhost.
//
// Generated per run rather than committed: a checked-in key is a checked-in
// key, even a useless one, and it would expire. The SANs matter -- Go has
// ignored the certificate CN since 1.15, so a certificate without them cannot
// verify under any configuration, which is the same trap real Qumulo clusters
// ship with.
func selfSignedCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mock-qumulo-server"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
