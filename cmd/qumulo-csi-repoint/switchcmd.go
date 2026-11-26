// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// `switch`: move workloads onto the promoted copy.
//
// Spec section 6 again, and the two topologies it names. What is *not*
// negotiable in either of them, and is the reason this command prints a plan
// rather than performing a re-point: a live NFS mount cannot be moved.
// `mount.nfs` resolves the server once and hands the kernel an `addr=`; PV
// `spec.csi` is immutable (`ValidatePersistentVolumeUpdate`: "spec.
// persistentvolumesource is immutable after creation"); and CSI has no RPC
// that re-points a bound volume. So every path here ends in "recreate the
// pods", exactly as every other vendor's failover procedure does.
//
// What differs between the topologies is how much else has to change:
//
//   - **DNS-named `server`.** Nothing in Kubernetes changes at all. Flip the
//     record, wait a TTL, recreate the pods; the same PVC comes back on the
//     same PV, now mounting the DR array. Measured on the lab cluster with a 60 s TTL.
//     This is why the DR guidance asks for a name, and it is the whole of
//     the difference between a five-minute failover and a per-volume rewrite.
//   - **IP-literal `server`.** The PV cannot be edited, so the PVC has to be
//     re-bound to the mirrored PV that `mirror` built. That means deleting the
//     claim -- safe only because the mirrored PV is `Retain` and the original
//     PV should be too, which this command checks and refuses over.

// switchTopology is which of the two cases a volume is in.
type switchTopology string

const (
	// topologyDNS: `server` is a name the customer controls.
	topologyDNS switchTopology = "dns"
	// topologyIP: `server` is an address, so the objects must change.
	topologyIP switchTopology = "ip"
)

// dnsFlip is the record change that moves every volume in one go.
type dnsFlip struct {
	Record string
	From   string
	To     string
	// Volumes is how many PVs ride on this one record.
	Volumes int
	// derived is the address the objects give for the record's target --
	// the promoted side's own server -- before any -dns-target, or "" when
	// they give none. Every pair on one record must agree on it.
	derived string
}

// volumeSwitch is one volume's object change, for the IP case.
type volumeSwitch struct {
	SourcePV      string
	SourcePVC     string
	Namespace     string
	MirroredPV    string
	MirroredPVC   string
	ReclaimPolicy corev1.PersistentVolumeReclaimPolicy
}

// switchPlan is what `switch` prints, and what `--dry-run=false` then carries
// out.
type switchPlan struct {
	Topology switchTopology
	Flips    []dnsFlip
	Switches []volumeSwitch
	// Claims whose pods must be recreated. Filled from the cluster when this
	// command can reach one; the PVC list is printed either way, because
	// "which pods" is answerable from it by hand and "which PVCs" is not
	// answerable from a pod list at all.
	Claims []string
	// Warnings are conditions that do not stop the plan but change what it
	// means -- above all a source PV that is not Retain.
	Warnings []string
}

// planSwitch decides what has to happen for a set of mirrored pairs.
//
// Pure, and takes the already-mirrored objects rather than re-deriving them,
// so what `switch` acts on is exactly what `mirror` wrote and a reviewer
// approved.
func planSwitch(pairs []mirrorPair, peerServer string) (switchPlan, error) {
	if len(pairs) == 0 {
		return switchPlan{}, fmt.Errorf("nothing to switch")
	}

	plan := switchPlan{}
	flips := map[string]*dnsFlip{}
	claims := map[string]bool{}

	for _, p := range pairs {
		sourceServer := p.Source.Spec.CSI.VolumeAttributes[volumeAttributeServer]
		if p.Source.Spec.ClaimRef != nil {
			claims[p.Source.Spec.ClaimRef.Namespace+"/"+p.Source.Spec.ClaimRef.Name] = true
		}

		if !isIPLiteral(sourceServer) {
			// One record usually serves every volume in a class, so the flips
			// are folded by name: an operator needs the list of records, not
			// the list of volumes that happen to share one.
			flip, ok := flips[sourceServer]
			if !ok {
				flip = &dnsFlip{Record: sourceServer, From: sourceServer, To: peerServer}
				flips[sourceServer] = flip
			}
			flip.Volumes++
			continue
		}

		plan.Topology = topologyIP
		sw := volumeSwitch{
			SourcePV:      p.Source.Name,
			MirroredPV:    p.Volume.Name,
			MirroredPVC:   p.Claim.Name,
			Namespace:     p.Claim.Namespace,
			ReclaimPolicy: p.Source.Spec.PersistentVolumeReclaimPolicy,
		}
		if p.Source.Spec.ClaimRef != nil {
			sw.SourcePVC = p.Source.Spec.ClaimRef.Name
		}
		plan.Switches = append(plan.Switches, sw)

		// The one condition that turns a failover into data loss: deleting a
		// PVC whose PV still says Delete tree-deletes the source volume, on an
		// array that may be the only place the pre-incident data exists.
		if p.Source.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf(
				"PV %s is %s, not Retain: deleting its PVC would delete the source volume on the "+
					"old array. Patch it to Retain before switching",
				p.Source.Name, p.Source.Spec.PersistentVolumeReclaimPolicy))
		}
	}

	for _, flip := range flips {
		plan.Flips = append(plan.Flips, *flip)
	}
	sort.Slice(plan.Flips, func(i, j int) bool { return plan.Flips[i].Record < plan.Flips[j].Record })

	if plan.Topology == "" {
		plan.Topology = topologyDNS
	} else if len(plan.Flips) > 0 {
		// Both kinds in one run. Not refused -- a cluster part way through
		// adopting DNS names is a real state -- but the operator must not read
		// "flip the record" and stop there.
		plan.Warnings = append(plan.Warnings, "this set mixes DNS-named and IP-named volumes: "+
			"the DNS ones need only the record flip, the IP ones need the PVC rebind below. Do both")
	}

	for claim := range claims {
		plan.Claims = append(plan.Claims, claim)
	}
	sort.Strings(plan.Claims)

	return plan, nil
}

// String renders the plan as the operator reads it.
//
// Comment-prefixed, so the whole output can be piped into a file alongside the
// manifests `switch` emits for the IP case and stay valid YAML.
func (p switchPlan) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "# topology: %s\n", p.Topology)
	for _, w := range p.Warnings {
		fmt.Fprintf(&b, "# WARNING: %s\n", w)
	}

	if len(p.Flips) > 0 {
		b.WriteString("#\n# DNS records to flip (do this first, then wait one TTL):\n")
		for _, f := range p.Flips {
			fmt.Fprintf(&b, "#   %s  ->  %s   (%d volume(s))\n", f.Record, f.To, f.Volumes)
		}
		b.WriteString("#   No Kubernetes object changes are needed for these volumes. A live mount\n" +
			"#   does not follow a DNS change, so the pods below must still be recreated.\n")
	}

	if len(p.Switches) > 0 {
		b.WriteString("#\n# PVCs to re-bind (server is an IP literal, and spec.csi is immutable):\n")
		for _, s := range p.Switches {
			fmt.Fprintf(&b, "#   %s/%s: %s -> %s  (claim %s)\n",
				s.Namespace, s.SourcePVC, s.SourcePV, s.MirroredPV, s.MirroredPVC)
		}
		b.WriteString("#   Order: quiesce the workload, delete the old PVC, apply the PVC below.\n")
	}

	if len(p.Claims) > 0 {
		b.WriteString("#\n# Pods to restart (every pod mounting one of these claims):\n")
		for _, c := range p.Claims {
			fmt.Fprintf(&b, "#   %s\n", c)
		}
	}

	return b.String()
}

// podsUsingClaims returns the pods that mount any of the given claims, as
// "namespace/pod (claim)".
//
// Pure, over a pod list the caller read from the cluster. This is the part
// that makes "the pods to restart" a list rather than an instruction: after a
// failover the operator has to recreate exactly these, and working them out by
// hand across namespaces at 3 a.m. is how one gets missed.
func podsUsingClaims(pods []corev1.Pod, claims []string) []string {
	wanted := map[string]bool{}
	for _, c := range claims {
		wanted[c] = true
	}

	var out []string
	for i := range pods {
		pod := &pods[i]
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			key := pod.Namespace + "/" + vol.PersistentVolumeClaim.ClaimName
			if wanted[key] {
				out = append(out, fmt.Sprintf("%s/%s (%s)", pod.Namespace, pod.Name, key))
			}
		}
	}
	sort.Strings(out)
	return out
}
