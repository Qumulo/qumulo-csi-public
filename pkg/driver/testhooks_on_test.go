//go:build testhooks

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The fence-race lab hook holds a status poll for the duration it is set to, and
// no longer than the poll's context.
func TestInfoFencePauseHookHoldsThePollAndHonoursItsContext(t *testing.T) {
	t.Setenv(infoFencePauseEnv, "")
	start := time.Now()
	pauseBeforeInfoExportRead(context.Background(), "pvc")
	assert.Less(t, time.Since(start), 50*time.Millisecond, "an unset hook paused")

	t.Setenv(infoFencePauseEnv, "100ms")
	start = time.Now()
	pauseBeforeInfoExportRead(context.Background(), "pvc")
	assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)

	t.Setenv(infoFencePauseEnv, "1h")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start = time.Now()
	pauseBeforeInfoExportRead(ctx, "pvc")
	assert.Less(t, time.Since(start), time.Second, "the hook outlived the poll's context")
}
