// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"errors"
	"fmt"
	"net/netip"
	"path"
	"strings"
)

// CSI hands DeleteVolume, ControllerExpandVolume and ValidateVolumeCapabilities
// nothing but the volume ID, so the ID has to carry everything those RPCs
// need, and it is immutable for the life of the PersistentVolume.
//
// The encoding is:
//
//	<server>#<apiURL>#<realPath>
//
//	qumulo.example.com##/csi/volumes/pvc-abc123
//	203.0.113.60#https://198.51.100.2:8000#/tenants/pod1/volumes/pvc-abc123
//
// # Why the API endpoint is a field
//
// server is the address the *node* mounts NFS from. The REST endpoint used to
// be derived from it -- `https://<server>:8000` -- which is right only when
// one address serves both. Under multitenancy it does not: a tenant's network
// carries NFS, and REST is enabled on the management tenant alone, which is
// the secure configuration and the normal one. DeleteVolume,
// ControllerExpandVolume and the snapshot RPCs are handed no StorageClass
// parameters, so a class's apiUrl was visible at CreateVolume and nowhere
// else: every tenant-scoped volume provisioned and mounted, and then could
// not be deleted or expanded, leaving a Released PV and a leaked export
// behind (SIM-07, measured on Core 7.9.3 and 7.10.2).
//
// The endpoint therefore travels in the handle, which is the one thing those
// RPCs always receive. CSI defines volume_id as an opaque plugin-owned string
// the CO passes back verbatim, and carrying backend location in it is what
// csi-driver-nfs, csi-driver-smb and Ceph CSI all do.
//
// The field is **empty when the endpoint is the default** for that server, so
// the common single-network class pays one byte rather than a repeated
// address -- see newVolumeID, which normalises it, and APIEndpoint, which is
// the only way to read it back.
//
// # What this does not solve
//
// A handle is minted once, on the array that created the volume, so it pins
// the endpoint to that array. Where `server` is a floating name that moves at
// failover (the shape the replication runbook recommends, and why
// volumeReplicationClass requires a per-array apiUrl), a handle minted on the
// source still names the source's REST endpoint after a flip. The same holds
// for `server` itself, and for any scheme that resolves the endpoint from the
// class.
//
// That is safe rather than solved: every controller RPC that acts on such a
// handle (ControllerExpandVolume, CreateSnapshot, DeleteVolume) first finds
// the volume's relationship, sees the peer holds the promoted copy, and
// refuses FailedPrecondition with nothing changed, naming the two ways out --
// demote this side, or re-point the PV with qumulo-csi-repoint. Measured on a
// live array pair by TestLiveReplicationOldHandleAfterFailoverIsRefused
// and documented as a known limit in
// docs/reference/supported-features.md, section 5.
//
// # Why the path and not the file ID
//
// The handle used to carry the directory's Qumulo file ID
// (`<server>/<fileID>/<name>`), which made quota and tree-delete free of a
// lookup. It also made the handle **array-specific**, because a file ID is
// allocated by the array that created the directory and *replication does not
// preserve it*: directory 104876396806445471986890824623 on one array became
// 133376728980805811083003327059 on its replica (measured).
//
// That is worse than it sounds. A file ID from the old array is usually a
// *valid* ID on the new one -- for some other directory -- so after a failover
// ControllerExpandVolume would re-quota an unrelated tree and DeleteVolume
// would tree-delete it. The driver defended that with an identity check that
// could only refuse, and refusing meant an operator rewriting every PV by hand.
//
// A path is the same on both arrays (the replica lives at the same path),
// so a path-identified handle is the fix rather than the refusal. The file ID
// is resolved at use, once, by resolveVolumePath -- one extra round trip on
// DeleteVolume and ControllerExpandVolume, nothing on the data path.
//
// # Why "#"
//
// A path contains "/", and so does a URL, so "/" can no longer separate the
// fields. "#" cannot appear in a hostname, has no meaning in a Qumulo path,
// and in a URL introduces a fragment, which a REST base address never has. It
// also makes a handle in the retired format fail cleanly as malformed rather
// than being misread -- which is now all that happens to one.
// validateServerHost, validateBasePath and validateAPIURL keep it out of all
// three fields.
//
// # Size
//
// CSI says a volume handle SHOULD NOT exceed 128 bytes. The common case is
// about 76 ("qumulo.example.com##/csi/volumes/pvc-" plus a 36-character
// UUID), but a deep basePath plus an adDomain plus a long name passes it. A
// class that needs an explicit endpoint spends that address on top -- about
// 98 bytes for the tenant shape above -- which is why the field is omitted
// when it carries nothing the server does not already say.
//
// CreateVolume measures and WARNS; it does not refuse. The spec puts the same
// 128-byte SHOULD on the volume *name* in the same request, and csi-sanity
// requires a 128-byte name to provision successfully -- so for such a name the
// two limits cannot both be met, and refusing would fail conformance over a
// volume the CO is entitled to ask for. See CreateVolume for the full
// reasoning; tooLong is the predicate both sides share.
const (
	volumeIDSeparator = "#"
	volumeIDParts     = 3
	snapshotIDParts   = 3
	// legacyVolumeIDParts is <server>#<path>, the handle before the endpoint
	// field; parseVolumeID decodes it with the endpoint empty.
	legacyVolumeIDParts = 2
	// snapshotIDSeparator matches the volume separator. It was "/" while a
	// snapshot handle carried nothing but a cluster-local ID; it carries an
	// endpoint now, for the same reason a volume handle does -- DeleteSnapshot
	// and ListSnapshots are handed no parameters either, so a snapshot taken
	// of a tenant-network volume could not be deleted -- and a URL contains
	// "/".
	snapshotIDSeparator = volumeIDSeparator
	// maxVolumeHandleBytes is what the CSI spec asks a volume handle to stay
	// within. Measured at provision time and warned about, not refused -- see
	// the Size section above.
	maxVolumeHandleBytes = 128
)

// volumeID identifies a provisioned volume by the array it lives on and its
// absolute path on that array.
type volumeID struct {
	Server   string
	RealPath string
	// APIURL is the REST endpoint for this volume's cluster, and is empty
	// when that endpoint is the default derived from Server. Read it through
	// APIEndpoint rather than directly: empty means "the default", not "none".
	APIURL string
	// Name is path.Base(RealPath), derived rather than encoded. It is carried
	// as a field because it is read at over a hundred call sites for logs and
	// error messages; it is set only by newVolumeID and parseVolumeID, and
	// String never reads it, so it cannot corrupt a handle.
	Name string
}

// newVolumeID builds a volume ID for a directory on one array. The only
// constructor, so Name can never disagree with RealPath and apiURL is
// normalised in exactly one place.
//
// An apiURL that is merely the default spelled out is stored as empty. Two
// classes that name one cluster, one leaving apiUrl unset and one setting it
// to the address the driver would have derived anyway, then mint the same
// handle for the same directory rather than two that differ in a field that
// means nothing.
func newVolumeID(server, apiURL, realPath string) volumeID {
	return volumeID{
		Server:   server,
		APIURL:   normalizeAPIURL(server, apiURL),
		RealPath: realPath,
		Name:     path.Base(realPath),
	}
}

// normalizeAPIURL trims an endpoint and drops it when it says no more than
// the server already does.
//
// Every trailing slash goes, not one: an apiUrl ending "//" minted a handle
// ending ":8000/", which re-rendered as ":8000", so a PV's handle and the
// driver's rendering of the same volume differed as strings.
func normalizeAPIURL(server, apiURL string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(apiURL), "/")
	if trimmed == "" || strings.EqualFold(trimmed, apiURLFor(server, "")) {
		return ""
	}
	return trimmed
}

// APIEndpoint is the REST address to dial for this volume's cluster.
//
// This is the whole point of the APIURL field: every caller that needs an
// endpoint asks here and gets the class's override or the derived default,
// without having to know which it was.
func (v volumeID) APIEndpoint() string {
	return apiURLFor(v.Server, v.APIURL)
}

// String renders the volume ID in its wire form.
//
// The zero value renders as "", not as a separator with nothing around it: a
// volume ID with no path names no volume, and callers that could not identify
// one (ListSnapshots, when a snapshot's source file ID does not resolve) must
// emit an empty field rather than a handle that would fail to parse.
func (v volumeID) String() string {
	if v.Server == "" || v.RealPath == "" {
		return ""
	}
	return strings.Join([]string{v.Server, v.APIURL, v.RealPath}, volumeIDSeparator)
}

// tooLong reports whether the handle exceeds what the CSI spec asks for.
func (v volumeID) tooLong() bool {
	return len(v.String()) > maxVolumeHandleBytes
}

// errVolumePathNotClean marks a handle whose path climbs or re-enters with a
// "." or ".." element. DeleteVolume refuses it out loud rather than taking it
// for an ID it could never have issued, because the PV that carries it is a
// hand-edited copy of a real one.
var errVolumePathNotClean = errors.New(`its path has a "." or ".." element, which no handle this driver mints ` +
	`has; a copied or edited PersistentVolume must name the volume's own directory`)

// inDriverHandleShape reports whether an ID that did not parse still has the
// shape of this driver's handles: a host name or address, then "#" or "/".
//
// Every volume and snapshot handle this driver has issued, in any form,
// starts that way. An ID that does and still does not parse is this driver's
// from a build that writes a form this one does not read -- a newer build
// after a rollback, or a retired form -- and answering it "already deleted"
// let Kubernetes drop the PV or VolumeSnapshotContent while the data stayed
// on the array. Anything else -- csi-sanity's "fake-vol-id", another
// driver's ID -- is not this driver's and still answers OK, as CSI asks.
//
// The host may also be an IPv6 literal, bare or bracketed. No class mints one
// (validateServerHost refuses ":"), but parseVolumeID reads the server field
// as given, so a hand-imported PV can carry one, and an unreadable handle of
// that kind answered OK and leaked exactly as above. Only an address that
// parses as IPv6 counts: a colon alone would sweep in a URL or another
// driver's "<pool>:<id>/..." and refuse IDs that are not this driver's.
func inDriverHandleShape(id string) bool {
	i := strings.IndexAny(id, "#/")
	if i <= 0 {
		return false
	}
	host := id[:i]
	return strings.Trim(host, hostChars) == "" || isIPv6Literal(host)
}

// hostChars is every character a host name or an IPv4 address is made of.
const hostChars = ".-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// isIPv6Literal reports whether host is an IPv6 address, optionally in
// brackets and optionally with a zone.
func isIPv6Literal(host string) bool {
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.Is6()
}

// errCleartextEndpoint marks a handle whose endpoint is http://. Every call to
// the endpoint carries the class's API token, so no RPC dials it; like
// errVolumePathNotClean it means a hand-written or edited handle, and the
// deletes refuse it out loud.
var errCleartextEndpoint = errors.New("it is http://, and every call to it would carry the API token in " +
	"cleartext; a handle's endpoint must be https://")

// isCleartextURL reports whether u names the http scheme.
func isCleartextURL(u string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(u)), "http://")
}

// hasDotElement reports whether p has a "." or ".." path element.
func hasDotElement(p string) bool {
	for _, elem := range strings.Split(p, "/") {
		if elem == "." || elem == ".." {
			return true
		}
	}
	return false
}

// parseVolumeID decodes a volume ID produced by volumeID.String.
func parseVolumeID(id string) (volumeID, error) {
	if id == "" {
		return volumeID{}, fmt.Errorf("volume ID is empty")
	}

	parts := strings.Split(id, volumeIDSeparator)
	if len(parts) == legacyVolumeIDParts {
		// <server>#<path>, the form before the endpoint field
		// existed. It maps one-to-one onto the current form with the
		// endpoint empty -- the address the server implies, which is what
		// that build dialled -- so it is decoded rather than refused. A
		// refusal stranded every PV an older build provisioned: DeleteVolume
		// answered OK for an ID it could not parse, so the PV went and the
		// directory, quota and export stayed. The checks below apply
		// to it exactly as to the current form.
		parts = []string{parts[0], "", parts[1]}
	}
	if len(parts) != volumeIDParts {
		// Every other earlier encoding lands here, notably the retired
		// <server>/<fileID>/<name> form, which named a file ID replication
		// does not preserve and so has no safe mapping. DeleteVolume refuses
		// one in this driver's shape (inDriverHandleShape) rather than
		// answering it OK.
		return volumeID{}, fmt.Errorf("volume ID %q is malformed: want %d %q-separated fields, got %d",
			id, volumeIDParts, volumeIDSeparator, len(parts))
	}

	// The middle field is the one field that is allowed to be empty: empty
	// means "the endpoint the server implies", which is what every class that
	// sets no apiUrl mints.
	server, apiURL, realPath := parts[0], parts[1], parts[2]
	if server == "" || realPath == "" {
		return volumeID{}, fmt.Errorf("volume ID %q is malformed: the server and path fields must not be empty", id)
	}
	// A NUL truncates the path at the syscall and REST boundaries, so one
	// handle names two different directories depending on who reads it; a
	// newline or carriage return splits a line in /etc/mtab, in a mount(8)
	// argument and in every log line that records what was acted on. No
	// handle this driver mints carries any of them.
	if strings.ContainsAny(id, controlCharacters) {
		return volumeID{}, fmt.Errorf("volume ID %q is malformed: it contains a NUL, newline or carriage return", id)
	}
	// An absolute path is what every cluster call takes, and it is what makes
	// path.Base meaningful. A relative one is not something this driver ever
	// issued.
	if !path.IsAbs(realPath) {
		return volumeID{}, fmt.Errorf("volume ID %q is malformed: %q is not an absolute cluster path", id, realPath)
	}
	// A "." or ".." element is refused before the path is cleaned. Every
	// handle this driver mints is already clean, so one that is not was
	// written by hand -- and cleaning it silently re-aimed DeleteVolume at a
	// sibling volume ("<vol>/../<other>"), the parent directory ("<vol>/..")
	// or the class's store root, which it then dismantled whole.
	if hasDotElement(realPath) {
		return volumeID{}, fmt.Errorf("volume ID %q is malformed: %w", id, errVolumePathNotClean)
	}
	// The root is refused explicitly. path.Base("/") is "/", which no volume
	// name can equal, so nothing downstream would act on it -- but a handle
	// naming the root is a hand-written one aimed at the whole filesystem, and
	// it should be refused where it is read rather than where it is used.
	if path.Clean(realPath) == "/" {
		return volumeID{}, fmt.Errorf("volume ID %q is malformed: the cluster root is not a volume", id)
	}

	// Checked on the way in, not at dial time: an endpoint that is not a URL
	// would otherwise surface as a connection error naming a nonsense address,
	// which reads like a cluster fault rather than a malformed handle.
	if apiURL != "" {
		if isCleartextURL(apiURL) {
			return volumeID{}, fmt.Errorf("volume ID %q is malformed: endpoint %q: %w", id, apiURL, errCleartextEndpoint)
		}
		if err := validateAPIURL(apiURL); err != nil {
			return volumeID{}, fmt.Errorf("volume ID %q is malformed: endpoint %q %w", id, apiURL, err)
		}
	}

	return newVolumeID(server, apiURL, path.Clean(realPath)), nil
}

// snapshotID identifies a snapshot on a specific cluster.
type snapshotID struct {
	Server string
	ID     string
	// APIURL is the cluster's REST endpoint, empty when it is the default for
	// Server, exactly as in volumeID. DeleteSnapshot and ListSnapshots are
	// handed no parameters, and CreateSnapshot receives the
	// VolumeSnapshotClass's rather than the StorageClass's -- which carries no
	// endpoint at all -- so without this a snapshot of a tenant-network volume
	// is as undeletable as the volume was.
	APIURL string
}

// newSnapshotID builds a snapshot ID, normalising the endpoint the way
// newVolumeID does. The only constructor, for the same reason.
func newSnapshotID(server, apiURL, id string) snapshotID {
	return snapshotID{Server: server, ID: id, APIURL: normalizeAPIURL(server, apiURL)}
}

// APIEndpoint is the REST address to dial for this snapshot's cluster.
func (s snapshotID) APIEndpoint() string {
	return apiURLFor(s.Server, s.APIURL)
}

// String renders the snapshot ID in its wire form.
func (s snapshotID) String() string {
	return strings.Join([]string{s.Server, s.APIURL, s.ID}, snapshotIDSeparator)
}

// parseSnapshotID decodes a snapshot ID produced by snapshotID.String.
func parseSnapshotID(id string) (snapshotID, error) {
	if id == "" {
		return snapshotID{}, fmt.Errorf("snapshot ID is empty")
	}

	parts := strings.Split(id, snapshotIDSeparator)
	if len(parts) != snapshotIDParts {
		// <server>/<snapshotID>, the form before the endpoint field
		// existed, and still what an old runbook or a hand-built import
		// produces. It maps one-to-one onto <server>##<snapshotID>,
		// the endpoint the server implies, so it is decoded: a refusal
		// left every VolumeSnapshotContent an older build made undeletable
		// and unrestorable.
		if server, legacy, ok := strings.Cut(id, "/"); ok && len(parts) == 1 && server != "" && isDecimal(legacy) {
			return newSnapshotID(server, "", legacy), nil
		}
		return snapshotID{}, fmt.Errorf("snapshot ID %q is malformed: want %d %q-separated fields, got %d",
			id, snapshotIDParts, snapshotIDSeparator, len(parts))
	}

	parsed := newSnapshotID(parts[0], parts[1], parts[2])
	if parsed.Server == "" || parsed.ID == "" {
		return snapshotID{}, fmt.Errorf("snapshot ID %q is malformed: the server and ID fields must not be empty", id)
	}
	if parts[1] != "" {
		if isCleartextURL(parts[1]) {
			return snapshotID{}, fmt.Errorf("snapshot ID %q is malformed: endpoint %q: %w", id, parts[1], errCleartextEndpoint)
		}
		if err := validateAPIURL(parts[1]); err != nil {
			return snapshotID{}, fmt.Errorf("snapshot ID %q is malformed: endpoint %q %w", id, parts[1], err)
		}
	}
	// Qumulo renders a snapshot ID as a bare JSON number, so anything
	// non-decimal is not an ID this driver ever issued. Rejecting it here
	// rather than at the cluster matters: the API answers a malformed ID with
	// 400, which both callers would report as Internal, where the spec wants
	// "already gone" for a delete and an empty list for a lookup.
	if !isDecimal(parsed.ID) {
		return snapshotID{}, fmt.Errorf("snapshot ID %q is malformed: %q is not a decimal snapshot ID", id, parsed.ID)
	}
	return parsed, nil
}

// isDecimal reports whether s consists only of decimal digits. strconv is not
// used because cluster IDs routinely exceed 64 bits.
func isDecimal(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}
