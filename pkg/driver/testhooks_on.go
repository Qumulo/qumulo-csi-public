//go:build testhooks

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Test hooks, compiled only with -tags testhooks: never in a release image
// (make docker-buildx builds without tags). Each lets a lab force an
// interleaving a fault outside the driver cannot.

import (
	"context"
	"log/slog"
	"os"
	"time"
)

// infoFencePauseEnv holds a source-side status poll (GetVolumeReplicationInfo)
// after its read of the peer's relationship status and before its read of the
// peer's export, for the duration it names (a Go duration, "40s"). A promote
// of the DR side run inside the pause is the fence race: the poll then finds
// the promoted copy's export read-write. Status polls dial the relationship's
// target address, and the poll and the promote run in one controller, so no
// network fault holds one without the other.
const infoFencePauseEnv = "QUMULO_CSI_TEST_PAUSE_INFO_FENCE"

// pauseBeforeInfoExportRead sleeps for infoFencePauseEnv, read on every call
// so a running controller's environment is what counts; unset, unparsable or
// not positive, it returns at once. The RPC's context ends it early.
func pauseBeforeInfoExportRead(ctx context.Context, volume string) {
	pause, err := time.ParseDuration(os.Getenv(infoFencePauseEnv))
	if err != nil || pause <= 0 {
		return
	}
	slog.WarnContext(ctx, "test hook: holding the status poll before its read of the replica export",
		"volume", volume, "pause", pause, "env", infoFencePauseEnv)
	timer := time.NewTimer(pause)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
