// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

// panicError turns a recovered panic into an error, logging the stack once.
//
// A panic in a goroutine the node plugin starts for a mount, an unmount, a
// probe or a stat is outside the gRPC handler, so no interceptor can catch
// it, and it exited the process: every mount and unmount on the node stopped
// until the kubelet restarted the plugin. Every such goroutine now
// runs in a flight map, whose join recovers into the call's error.
func panicError(what string, r any) error {
	slog.Error("nodeserver: recovered a panic; the operation fails, the plugin keeps serving",
		"operation", what, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
	return fmt.Errorf("%s: internal error (recovered panic: %v)", what, r)
}
