// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
)

// Supported CSI endpoint schemes. A CO hands the plugin one of these two:
// unix for the usual socket in a shared volume, tcp for local development
// and for csi-sanity.
const (
	endpointSchemeUnix = "unix"
	endpointSchemeTCP  = "tcp"
)

// ParseEndpoint splits a CSI endpoint into a network and address suitable
// for net.Listen.
//
// Both unix:///csi/csi.sock and unix:/csi/csi.sock are accepted because COs
// and sidecars disagree on how many slashes to write, and a driver that
// rejected one of the two would fail to start for a reason nobody would
// think to look for.
func ParseEndpoint(endpoint string) (network, addr string, err error) {
	switch {
	case strings.HasPrefix(endpoint, endpointSchemeUnix+"://"):
		addr = strings.TrimPrefix(endpoint, endpointSchemeUnix+"://")
		network = endpointSchemeUnix
	case strings.HasPrefix(endpoint, endpointSchemeUnix+":"):
		addr = strings.TrimPrefix(endpoint, endpointSchemeUnix+":")
		network = endpointSchemeUnix
	case strings.HasPrefix(endpoint, endpointSchemeTCP+"://"):
		addr = strings.TrimPrefix(endpoint, endpointSchemeTCP+"://")
		network = endpointSchemeTCP
	default:
		return "", "", fmt.Errorf("unsupported endpoint %q: want a unix:// or tcp:// URL", endpoint)
	}

	if addr == "" {
		return "", "", fmt.Errorf("endpoint %q has no address", endpoint)
	}
	return network, addr, nil
}

// Listen opens the listener for a CSI endpoint, preparing the filesystem
// first when the endpoint is a unix socket.
func Listen(endpoint string) (net.Listener, error) {
	network, addr, err := ParseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	if network == endpointSchemeUnix {
		if err := prepareSocketPath(addr); err != nil {
			return nil, err
		}
	}

	listener, err := net.Listen(network, addr)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", endpoint, err)
	}
	return listener, nil
}

// prepareSocketPath clears a stale socket and makes sure its directory
// exists.
//
// A socket file left behind by a previous container instance makes Listen
// fail with "address already in use" even though nothing is serving on it,
// which on a restarting pod looks like a permanent failure. Removing it is
// safe because only one driver instance owns a given plugin directory.
func prepareSocketPath(addr string) error {
	if err := os.Remove(addr); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing stale socket %s: %w", addr, err)
	} else if err == nil {
		slog.Info("removed stale csi socket", "path", addr)
	}

	if dir := filepath.Dir(addr); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("creating socket directory %s: %w", dir, err)
		}
	}
	return nil
}
