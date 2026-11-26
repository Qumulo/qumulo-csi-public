// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import "fmt"

// Mode selects which CSI services a driver instance serves.
//
// One binary is deployed as two different workloads: the controller service
// runs once per cluster and holds the Qumulo API credentials, while the node
// service runs on every node and only mounts NFS. Separating the roles is
// what lets a node DaemonSet run without a cluster-wide API token on every
// worker -- the node service never calls the REST API at all.
type Mode string

const (
	// ModeController serves the Identity and Controller services.
	//
	// It is the zero value deliberately: it is the one role that needs no
	// node identity, so a Config that omits a mode cannot accidentally
	// produce a node service advertising an empty node ID.
	ModeController Mode = "controller"

	// ModeNode serves the Identity and Node services. It needs a node ID
	// but no Qumulo credentials.
	ModeNode Mode = "node"

	// ModeAll serves every service from one process. This is for local
	// development and for csi-sanity, which drives all three services over
	// a single endpoint; a real deployment should split the roles so the
	// node pods hold no credentials.
	ModeAll Mode = "all"
)

// ParseMode converts a mode name to a Mode, resolving the empty name to the
// zero value's meaning so callers do not each have to.
func ParseMode(name string) (Mode, error) {
	switch mode := Mode(name); mode {
	case "", ModeController:
		return ModeController, nil
	case ModeNode, ModeAll:
		return mode, nil
	default:
		return "", fmt.Errorf("unknown mode %q: want %q, %q or %q",
			name, ModeController, ModeNode, ModeAll)
	}
}

// ServesController reports whether this mode serves the Controller service.
func (m Mode) ServesController() bool {
	return m == ModeController || m == ModeAll
}

// ServesNode reports whether this mode serves the Node service.
func (m Mode) ServesNode() bool {
	return m == ModeNode || m == ModeAll
}

func (m Mode) String() string {
	if m == "" {
		return string(ModeController)
	}
	return string(m)
}
