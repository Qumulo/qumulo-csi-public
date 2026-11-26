//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package live holds tests that run against a real Qumulo cluster.
//
// Guarded by the "live" build tag AND QUMULO_LIVE_TEST=1: these create and
// delete real directories, quotas and snapshots, so two independent gates
// stand between a stray `go test ./...` and someone's cluster.
//
// The harness itself now lives in test/livekit, so the OpenShift tier can
// drive the same cluster the same way -- a _test.go file cannot be imported,
// which is the whole reason it moved. This file is the seam: `harness` embeds
// it, so the helpers this package defines on its own type (seedFile,
// restoreVolume and the rest) stay exactly where they were and keep working.
package live

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"qumulo-csi-driver/test/livekit"
)

// harness is livekit's, with room for this package's own helpers hung off it.
//
// Embedding rather than aliasing is deliberate: Go forbids declaring a method
// on a type from another package, and this package declares seven of them.
type harness struct {
	*livekit.Harness
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return &harness{Harness: livekit.NewHarness(t)}
}

// provisionedVolume and provision are livekit's, under the names this
// package's tests already use.
type provisionedVolume = livekit.ProvisionedVolume

func provision(ctx context.Context, t *testing.T, h *harness, prefix string, capacity int64) provisionedVolume {
	t.Helper()
	return livekit.Provision(ctx, t, h.Harness, prefix, capacity)
}

// The package-level constants and helpers, re-exported under their old names
// so no call site had to change for the move.
const (
	gib          = livekit.GiB
	liveTimeout  = livekit.LiveTimeout
	safetyMarker = livekit.SafetyMarker
)

var (
	extractToken      = livekit.ExtractToken
	volumePathFromID  = livekit.VolumePathFromID
	volumeNameFromID  = livekit.VolumeNameFromID
	clusterSnapshotID = livekit.ClusterSnapshotID
)

// The node helpers moved with the harness; these keep this package's own
// node tests reading as they did.
var (
	requireNodeMounting = livekit.RequireNodeMounting
)

func publishRequest(h *harness, volume provisionedVolume, targetPath, protocol string) *csi.NodePublishVolumeRequest {
	return livekit.PublishRequest(h.Harness, volume, targetPath, protocol)
}

var mountTable = livekit.MountTable

var requireWritable = livekit.RequireWritable
