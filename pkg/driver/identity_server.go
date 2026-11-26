// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// IdentityServer implements csi.IdentityServer, the one CSI service every
// plugin (controller, node, or both) must implement. The CO uses it to
// discover the plugin's name/version, its optional capabilities, and its
// readiness.
type IdentityServer struct {
	csi.UnimplementedIdentityServer
	driver *Driver
}

// NewIdentityServer creates an IdentityServer backed by the given Driver.
func NewIdentityServer(driver *Driver) *IdentityServer {
	return &IdentityServer{driver: driver}
}

// GetPluginInfo returns the plugin's name and version so the CO can
// identify which driver is handling a given StorageClass/CSIDriver object.
func (s *IdentityServer) GetPluginInfo(
	ctx context.Context,
	req *csi.GetPluginInfoRequest,
) (*csi.GetPluginInfoResponse, error) {
	if s.driver.Name == "" {
		return nil, status.Error(codes.Unavailable, "Driver name not configured")
	}

	if s.driver.Version == "" {
		return nil, status.Error(codes.Unavailable, "Driver is missing version")
	}
	return &csi.GetPluginInfoResponse{
		Name:          s.driver.Name,
		VendorVersion: s.driver.Version,
	}, nil
}

// GetPluginCapabilities tells the CO which optional CSI capabilities this
// plugin exposes. The Qumulo driver runs a controller service that manages
// the lifecycle of NFS-exported directories as CSI volumes, and those
// volumes can be grown in place.
//
// Note there is no node-service capability to advertise: the CO discovers
// the node service through the kubelet's plugin registration, not through
// this RPC, so these values are the same whatever mode the instance runs in.
func (s *IdentityServer) GetPluginCapabilities(
	ctx context.Context,
	req *csi.GetPluginCapabilitiesRequest,
) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
			{
				// Required for the topology in NodeGetInfo and CreateVolume to
				// be used at all: without this capability the CO ignores both.
				// This driver's topology is one segment covering every node
				// running its node plugin, which constrains nothing that was
				// not already impossible -- see topology.go for why it exists.
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
					},
				},
			},
			{
				// ONLINE, not OFFLINE: growing a volume is a quota change on
				// the cluster, so a workload keeps running through it and
				// there is no node-side filesystem to grow afterwards --
				// which is also why ControllerExpandVolume always reports
				// NodeExpansionRequired false.
				//
				// The controller service already advertises EXPAND_VOLUME.
				// Both are required: the CO checks this one to decide
				// whether expansion is possible at all.
				Type: &csi.PluginCapability_VolumeExpansion_{
					VolumeExpansion: &csi.PluginCapability_VolumeExpansion{
						Type: csi.PluginCapability_VolumeExpansion_ONLINE,
					},
				},
			},
		},
	}, nil
}

// Probe reports plugin readiness. The CO calls this before sending any
// other RPC, and periodically afterwards; an error takes the plugin out of
// rotation. Readiness here is the liveness of this process, for the controller
// as much as for the node: neither contacts a cluster to answer it. See the
// body for why, and note that this paragraph used to claim the opposite for
// the controller while the code below already did not do it.
//
// A node instance is deliberately given no Qumulo client -- the node service
// never calls the REST API, which is what keeps a cluster-wide token off
// every worker (see NewDriver) -- so its readiness cannot be defined in terms
// of the cluster, and reaching for the client here would dereference nil.
// That matters in production, not just in theory: the node DaemonSet runs a
// livenessprobe sidecar whose only job is to call this RPC, and a panic in a
// gRPC handler is not recovered, so it would take down a node pod that is
// working perfectly.
func (s *IdentityServer) Probe(
	ctx context.Context,
	req *csi.ProbeRequest,
) (*csi.ProbeResponse, error) {
	// Liveness of this process, not reachability of any cluster.
	//
	// One controller serves many Qumulo clusters, chosen per StorageClass, so
	// there is no single cluster to health-check -- and the driver is never
	// told the full set, so it cannot check them all.
	//
	// This is not a loss of signal. The old behaviour called HealthCheck
	// against an endpoint that requires no authentication, so it reported
	// healthy while every provision failed with 401: a controller stayed
	// Ready for 36 minutes with a dead token. Per-cluster
	// reachability and credential failures surface where an operator can act
	// on them, as ProvisioningFailed events on the PVC that named the cluster.
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}
