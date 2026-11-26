// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"fmt"
	"net"
	"path"
	"strings"
)

// The volume handle, and why this tool re-parses it rather than importing the
// driver's parser.
//
// A handle is `<server>#<realPath>` (pkg/driver/volume_id.go). Only the first
// field is array-specific, which is what this command is now for:
//
//   - `server` is the NFS host the node mounts from. On the DR array it is a
//     different host -- unless it is a DNS name the customer flips, which is
//     why the DR guidance asks for one, and which is the case where
//     this command has nothing to do at all.
//   - `realPath` is the same on both arrays, so it is carried across
//     unchanged.
//
// This used to have a third field, the Qumulo file ID, and rewriting it was
// the whole job: replication does not preserve file IDs, so a mirrored PV had
// to carry the replica's *own* ID, looked up by path on the peer (measured in DR
// lab testing). Keeping the source's ID was the one thing this tool could
// never do -- a stale ID is usually a *valid* ID on the target, just for a
// different file, and ControllerExpandVolume would then re-quota an unrelated
// directory silently. The handle no longer carries one, so that hazard is
// gone and so is the rewrite; what is left is a server substitution.
//
// The parser is duplicated here on purpose. pkg/driver's is unexported, and
// exporting it to share with an operator CLI would widen the driver's API for
// a tool the driver does not depend on. The encoding is three fields and a
// separator; the test below pins it against the same cases the driver's own
// test uses.

// handleSeparator matches pkg/driver/volume_id.go. A path contains "/", so
// "/" cannot separate the fields.
const handleSeparator = "#"

// volumeHandle is a parsed CSI volume handle.
type volumeHandle struct {
	Server   string
	RealPath string
	// APIURL is the cluster's REST endpoint, empty when it is the default
	// https://<server>:8000. It is array-specific, exactly as Server is, so
	// a re-point has to substitute it too -- a mirrored PV keeping the
	// source's endpoint would send its DeleteVolume and
	// ControllerExpandVolume back to the array it failed over from.
	APIURL string
}

// String renders the handle in its wire form.
func (h volumeHandle) String() string {
	return h.Server + handleSeparator + h.APIURL + handleSeparator + h.RealPath
}

// Name is the volume's name, which is the last element of its path.
func (h volumeHandle) Name() string {
	return path.Base(h.RealPath)
}

// parseVolumeHandle decodes `<server>#<apiUrl>#<realPath>`.
func parseVolumeHandle(handle string) (volumeHandle, error) {
	if handle == "" {
		return volumeHandle{}, fmt.Errorf("volume handle is empty")
	}

	parts := strings.Split(handle, handleSeparator)
	if len(parts) != 3 {
		return volumeHandle{}, fmt.Errorf(
			"volume handle %q is malformed: want <server>#<apiUrl>#<path>, got %d fields "+
				"(a handle of the form <server>/<fileID>/<name>, or of the form "+
				"<server>#<path>, is an earlier encoding this tool no longer reads)", handle, len(parts))
	}

	// Only the endpoint may be empty: empty means the default this array's
	// server implies, which is what every class without an apiUrl mints.
	parsed := volumeHandle{Server: parts[0], APIURL: parts[1], RealPath: parts[2]}
	if parsed.Server == "" || parsed.RealPath == "" {
		return volumeHandle{}, fmt.Errorf(
			"volume handle %q is malformed: the server and path fields must not be empty", handle)
	}
	if !path.IsAbs(parsed.RealPath) {
		return volumeHandle{}, fmt.Errorf(
			"volume handle %q is malformed: %q is not an absolute cluster path", handle, parsed.RealPath)
	}
	if path.Clean(parsed.RealPath) == "/" {
		return volumeHandle{}, fmt.Errorf("volume handle %q is malformed: the cluster root is not a volume", handle)
	}
	parsed.RealPath = path.Clean(parsed.RealPath)
	return parsed, nil
}

// onPeer returns the handle this volume's replica has on another array.
//
// The server and the endpoint are replaced and the path is kept, because the
// replica lives at the same path. It takes no file ID any more: there
// is none in a handle, and that is what makes a mirrored PV correct by
// construction rather than by a lookup that could return the wrong array's
// answer.
//
// peerAPIURL is the peer's REST endpoint, and empty means "the default the
// peer's server implies" -- which is right for a single-network peer and
// wrong for a tenant one, so `mirror` passes the -peer-api-url it was given
// rather than letting it default silently.
func (h volumeHandle) onPeer(peerServer, peerAPIURL, replicaPath string) (volumeHandle, error) {
	if peerServer == "" {
		return volumeHandle{}, fmt.Errorf("peer server must not be empty")
	}
	if replicaPath == "" {
		replicaPath = h.RealPath
	}
	if !path.IsAbs(replicaPath) {
		return volumeHandle{}, fmt.Errorf("replica path %q is not an absolute cluster path", replicaPath)
	}
	if defaultAPIURLFor(peerServer) == strings.TrimRight(strings.TrimSpace(peerAPIURL), "/") {
		peerAPIURL = ""
	}
	return volumeHandle{
		Server:   peerServer,
		APIURL:   strings.TrimRight(strings.TrimSpace(peerAPIURL), "/"),
		RealPath: path.Clean(replicaPath),
	}, nil
}

// defaultAPIURLFor is the endpoint the driver derives from a server when a
// handle carries none. It matches apiURLFor in pkg/driver.
func defaultAPIURLFor(server string) string {
	return "https://" + net.JoinHostPort(server, "8000")
}

// isIPLiteral reports whether a `server` value is an address rather than a
// name.
//
// This is what decides which of the two failover topologies a volume is in,
// and the distinction is not cosmetic. A DNS name can be re-pointed at the DR
// array, so the same PV keeps working and the failover needs no object change
// at all -- measured on the lab cluster: with a 60 s TTL, the pod came back on the
// same PVC after the record was flipped and the pod recreated. An IP
// literal cannot be re-pointed, and `spec.csi` is immutable, so that volume
// needs a replacement PV. Hence `switch` behaves differently for each, and
// hence the DR guidance asks for a name.
func isIPLiteral(server string) bool {
	return net.ParseIP(server) != nil
}
