// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"
	"slices"
	"strings"
)

// Snapshot-class parameters: what CreateSnapshot is allowed to be handed, and
// what it does with the one key it recognises.
//
// A VolumeSnapshotClass has its own parameters map, and nothing used to
// check it. Two things followed from that:
//
//   - An apiurl override was dialled as written. The StorageClass's own
//     apiUrl goes through validateAPIURL, which refuses http:// because every
//     call carries the bearer token -- but the snapshot class's did
//     not, so a class naming an http:// endpoint sent the token in cleartext
//     on the first request and only then failed on something else.
//   - Any other key was ignored, "apiUrl" included. A class that spells the
//     key the way the StorageClass spells it got no override at all and no
//     complaint, so the snapshot was quietly taken through the handle's
//     endpoint instead of the one the administrator named.
//
// Both are refused before a client is built, so nothing is dialled and no
// token leaves the controller.

// recognizedSnapshotParams is every VolumeSnapshotClass parameter this driver
// reads. Keys the CO injects (coParamPrefix) are not the administrator's to
// get right and are always allowed.
var recognizedSnapshotParams = []string{paramAPIURL}

// validateSnapshotParams refuses a VolumeSnapshotClass parameter map this
// driver cannot honour, and returns the apiurl override it carries.
//
// Keys are compared exactly, not folded. That is the opposite of
// parseVolumeParams, and deliberately so: a StorageClass key is folded
// because the value is used either way, whereas here folding would mean
// accepting "apiUrl" as an override after builds that ignored it, silently
// moving where existing classes take their snapshots. Refusing names the
// spelling to use instead.
func validateSnapshotParams(params map[string]string) (apiURL string, err error) {
	var unknown []string
	for key, value := range params {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(key)), coParamPrefix) {
			continue
		}
		if !slices.Contains(recognizedSnapshotParams, key) {
			// Quoted as written, untrimmed: keys are compared exactly here,
			// so " apiurl" is refused, and printing it trimmed told the
			// administrator to use the very spelling they appeared to have
			// used.
			unknown = append(unknown, fmt.Sprintf("%q", key))
			continue
		}
		apiURL = strings.TrimSpace(value)
	}
	if len(unknown) > 0 {
		slices.SortFunc(unknown, func(a, b string) int {
			return strings.Compare(strings.ToLower(a), strings.ToLower(b))
		})
		return "", fmt.Errorf("unknown VolumeSnapshotClass parameter(s): %s (recognized: %s). "+
			"The key is spelled exactly %q here, whatever its spelling on the StorageClass",
			strings.Join(unknown, ", "), strings.Join(recognizedSnapshotParams, ", "), paramAPIURL)
	}

	if apiURL != "" {
		if err := validateAPIURL(apiURL); err != nil {
			return "", fmt.Errorf("%s %q: %w", paramAPIURL, apiURL, err)
		}
	}
	return apiURL, nil
}
