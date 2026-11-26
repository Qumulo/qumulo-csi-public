//go:build !testhooks

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import "context"

// pauseBeforeInfoExportRead is a no-op in every build but a lab one; see
// testhooks_on.go.
func pauseBeforeInfoExportRead(context.Context, string) {}
