// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import "github.com/container-storage-interface/spec/lib/go/csi"

// This driver's topology is one segment covering every node where its node
// service runs.
//
// Read this before changing it, because the reasoning is not the usual one and
// the shape here is deliberately degenerate.
//
// A topology key normally names a real placement boundary: aws-ebs returns
// topology.ebs.csi.aws.com/zone, and a volume in us-east-1a genuinely cannot
// be mounted from us-east-1b, so the key stops the scheduler making an
// impossible choice. **This driver has no such boundary.** An NFS export
// answers identically from every node that can route to the cluster, and the
// driver held no topology at all until capacity reporting needed one.
//
// What is real, and is all this declares: a node cannot mount without this
// driver's node plugin on it. Every node running the DaemonSet gets the same
// key and the same value, so there is exactly one segment, and the nodeAffinity
// stamped onto a PV is satisfied by every node in that set. It restricts
// nothing that was not already impossible. If an administrator runs the
// DaemonSet on a subset of nodes -- a nodeSelector on the node workload -- then
// PVs are confined to that subset, which is correct rather than surprising.
//
// Why one uniform value and not, say, the node name: the capacity sidecar
// creates one CSIStorageCapacity object per (topology segment, StorageClass)
// pair. A per-node value would make one segment per node, so a five-class,
// fifty-node cluster would carry 250 objects that all report the same number.
// One segment keeps it at one object per class.
//
// Why it exists at all: csi-provisioner's Capacity Controller could not keep a
// stable object for a driver reporting no topology. It created an object, then
// immediately marked the object it had just created obsolete, deleted it, and
// repeated -- so `kubectl get csistoragecapacity` stayed empty forever while
// the RPC itself answered correctly every time. Measured on Kubernetes 1.36.0
// with csi-provisioner v5.3.0; see docs/reference/supported-features.md.
//
// The honest summary: this is a degenerate topology that exists to satisfy a
// sidecar's bookkeeping, not to describe a constraint. It is cheap and it is
// not a lie -- but it is not what a topology key is usually for, and a reader
// who assumes otherwise will be confused by it.
const (
	// topologyKeyPlugin marks a node where this driver's node service runs.
	//
	// The key is namespaced to the driver rather than reusing a well-known
	// label such as topology.kubernetes.io/zone: this is not a zone, and
	// borrowing a standard key would make the scheduler and any topology-aware
	// tooling read a meaning into it that is not there.
	topologyKeyPlugin = "topology.csi.qumulo.com/plugin"

	// topologyValuePresent is the single value every node reports.
	topologyValuePresent = "present"
)

// nodeTopology is the segment this instance's node belongs to.
//
// Identical on every node on purpose -- see the block comment above.
func nodeTopology() *csi.Topology {
	return &csi.Topology{
		Segments: map[string]string{topologyKeyPlugin: topologyValuePresent},
	}
}

// volumeTopology is the accessibility a provisioned volume reports.
//
// The same single segment, which is what puts a matching nodeAffinity on the
// PV. It must agree with nodeTopology: a volume advertising a segment no node
// reports would be unschedulable everywhere, and the failure would show up as
// a pod pending with no obvious cause.
func volumeTopology() []*csi.Topology {
	return []*csi.Topology{nodeTopology()}
}
