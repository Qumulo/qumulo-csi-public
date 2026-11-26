// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// What a node RPC refuses before it touches the filesystem.
//
// Every value here arrives from an object the CO stored, not from a request
// this driver has already vetted: a PersistentVolume's handle and volume
// context can be edited by anyone who can write a PV, and nothing between
// there and mount(8) looks at them again. The kubelet itself sends none of
// these shapes, which is exactly why they went unchecked -- and why refusing
// them breaks nothing real.

// controlCharacters are the bytes that change the meaning of the places
// these strings end up: a NUL truncates a path at the syscall boundary, so
// "/var/lib/kubelet/...\x00/evil" is one path to Go and another to the
// kernel; a newline or a carriage return splits a line in /etc/mtab, in a
// mount(8) argument and in every log line that records what was mounted.
const controlCharacters = "\x00\n\r"

// hasControlCharacters reports whether s carries one of them.
func hasControlCharacters(s string) bool {
	return strings.ContainsAny(s, controlCharacters)
}

// validateNodeTargetPath refuses a target path a node RPC must not act on.
//
// Absolute, because a relative path is resolved against this process's
// working directory -- which is "/" in the container today and is not a
// promise anyone made, so the same request would act on a different
// directory after a change nobody connected to it.
//
// No "." or ".." element, which is the check that matters: this path is
// mounted on, and on unpublish unmounted and removed, so an element that
// climbs out of the kubelet's tree aims those at somewhere else entirely.
// It is the reasoning parseVolumeID applies to a handle's path.
//
// Redundant separators are not refused. "/a//b" names exactly "/a/b" to
// every syscall and to filepath, csi-sanity builds paths that way, and
// refusing them would reject a correct caller over spelling.
func validateNodeTargetPath(targetPath string) error {
	if hasControlCharacters(targetPath) {
		return fmt.Errorf("target path %q contains a NUL, newline or carriage return", targetPath)
	}
	if !filepath.IsAbs(targetPath) {
		return fmt.Errorf("target path %q is not absolute", targetPath)
	}
	if hasDotElement(targetPath) {
		return fmt.Errorf("target path %q has a %q or %q element, so it does not name the directory it "+
			"reads as; the kubelet sends neither", targetPath, ".", "..")
	}
	return nil
}

// validateNodeVolumeID refuses a handle no node RPC should carry further.
//
// The node does not decode the handle -- the controller does, and
// parseVolumeID refuses the shapes that matter there -- but it does log it
// and pass it to the replica check, so a handle carrying a newline would
// forge a log line.
func validateNodeVolumeID(volumeID string) error {
	if hasControlCharacters(volumeID) {
		return fmt.Errorf("volume ID %q contains a NUL, newline or carriage return", volumeID)
	}
	return nil
}

// publishTargetState is what the target path is before a mount is attempted.
type publishTargetState int

const (
	// targetUsable is a directory, or nothing yet: either can be mounted on.
	targetUsable publishTargetState = iota
	// targetIsSymlink is a symbolic link, dangling or not.
	targetIsSymlink
	// targetNotADirectory is a regular file, a device, a socket.
	targetNotADirectory
)

// inspectPublishTarget reports what is at targetPath without following a
// final symlink.
//
// Not following it is the whole point: a symlink at the target is a
// redirection, and stat(2) answers for whatever it points at -- so a target
// linked to the node's own root looked like an ordinary directory, and the
// mount, and later the unpublish's removal, acted there instead.
// A dangling link answers IsNotExist under stat and is therefore treated as
// "nothing there yet", which would have had MkdirAll follow it.
//
// The type comes from targetTypeNoSync, which reads it from the node's cache
// without a round trip to the NFS server; see there for why.
func inspectPublishTarget(targetPath string) (publishTargetState, error) {
	mode, err := targetTypeNoSync(targetPath)
	switch {
	case os.IsNotExist(err):
		return targetUsable, nil
	case err != nil:
		return targetUsable, err
	case mode&os.ModeSymlink != 0:
		return targetIsSymlink, nil
	case !mode.IsDir():
		return targetNotADirectory, nil
	}
	return targetUsable, nil
}
