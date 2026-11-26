// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import "google.golang.org/grpc/codes"

// Aliases keep the step functions readable, where the status code is the
// assertion rather than incidental detail.
const (
	codesAlreadyExists = codes.AlreadyExists
	codesAborted       = codes.Aborted
)
