// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"context"
	"fmt"
	"maps"
	"path"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// `mirror`: the DR-side PV/PVC pair for a protected volume.
//
// Spec section 6, topology A. csi-addons assumes each Kubernetes cluster has a
// PV whose handle names *its* copy of the volume. Ceph gets that for free;
// here it has to be made true, because a Qumulo replica is a different array
// with a different file ID. Ramen solves the same problem by saving PV objects
// to S3 and restoring them on the peer; this is the same thing for customers
// who do not run Ramen, done on demand with no controller and no informer.
//
// What the mirrored PV differs from the original in, and nothing else:
//
//   - `spec.csi.volumeHandle`: the peer's server and the **replica's own file
//     ID**, read from the peer by path. Never transformed from the source's.
//   - `volumeAttributes.server`: the peer array.
//   - `volumeAttributes[csi.qumulo.com/replicated]`: set, so the node refuses
//     to publish this volume read-write until the replica is promoted
//     (pkg/driver/node_replica.go). This is the one attribute the original does
//     not have at all, and it is the point of mirroring through a tool rather
//     than by hand.
//   - `persistentVolumeReclaimPolicy: Retain`: a DR copy that a stray PVC
//     deletion can tree-delete is not a DR copy.
//   - `claimRef`: pre-set, so the PV is reserved for its claim and no other
//     PVC in the cluster can bind it first. Dell's CSM replication does the
//     same thing with a reserved claimRef, and for the same reason.
//   - `csi.qumulo.com/delete-directory`: set to the replica's path only when
//     the source owned its directory other than by its name, so the mirror
//     owns the replica exactly as far as the source owned the original (
//     see mirroredDeleteDirectory).
//
// Everything else -- capacity, access modes, mount options, fsType, the rest
// of the volume attributes -- is copied verbatim, because the replica sits
// at the same path on both arrays and the mount is therefore identical apart
// from the host.

// Annotation keys recording where a mirrored object came from. `switch` reads
// them, and so does a human trying to work out what a `-dr` PV is.
const (
	annotationMirroredFrom = "csi.qumulo.com/mirrored-from"
	annotationSourceHandle = "csi.qumulo.com/source-volume-handle"
	annotationSourceServer = "csi.qumulo.com/source-server"
	// annotationDeleteDirectory is the driver's opt-in that lets Kubernetes
	// delete a directory for a PV not named after it (spelled here for
	// the same reason as the volume attribute keys below). A mirror carries it
	// when its source owned its directory and mirrored-from alone would not
	// make the mirror own the replica's.
	annotationDeleteDirectory = "csi.qumulo.com/delete-directory"
)

// volumeAttributeServer and volumeAttributeRealPath are the two volume
// attributes this tool reads. They are the driver's own keys
// (pkg/driver/params.go); spelled here rather than imported for the same
// reason the handle parser is duplicated -- see handle.go.
const (
	volumeAttributeServer     = "server"
	volumeAttributeRealPath   = "realPath"
	volumeAttributeReplicated = "csi.qumulo.com/replicated"
)

// mirrorOptions is everything about the peer that cannot be read off the
// source objects.
type mirrorOptions struct {
	// driverName is the provisioner whose PVs are mirrored. Anything else is
	// refused rather than rewritten.
	driverName string
	// peerServer is the DR array's NFS host, as it will appear in the mirrored
	// PV's handle and volumeAttributes.
	peerServer string
	// peerAPIURL is the DR array's REST endpoint, which goes into the
	// mirrored handle's second field. It is already required for the replica
	// lookup, and carrying it into the handle is what keeps a mirrored
	// tenant-network volume deletable: the endpoint is array-specific, so
	// leaving the source's in place would point the DR PV's DeleteVolume and
	// ControllerExpandVolume back at the array it failed over from.
	peerAPIURL string
	// peerBasePath re-roots the replica's cluster path when the peer does not
	// use the same one. Empty means the same path on both arrays, which is
	// what the runbook assumes and what makes the mirrored PV differ in
	// `server` alone.
	peerBasePath string
	// pvSuffix names the mirrored PV. PV names are cluster-scoped, so on a
	// single cluster (topology B) the mirror cannot reuse the original's.
	pvSuffix string
	// claimSuffix names the mirrored PVC. Empty on a second Kubernetes cluster
	// (topology A), where the same namespace/name is the point; required on
	// one cluster, where the original claim still exists.
	claimSuffix string
	// storageClass overrides the mirrored objects' class. Empty keeps the
	// source's name, which is right when the DR cluster has a class of the
	// same name pointing at the DR array.
	storageClass string
	// secretName and secretNamespace override the CSI secret references. The
	// source's point at the source array's token; an expand against the
	// mirrored PV dials the *peer*, so on a second cluster these usually have
	// to change.
	secretName      string
	secretNamespace string
}

// fileIDResolver reads a directory's file ID from an array.
//
// An interface so the rewrite can be tested without an array: the lookup is
// one GET, and everything interesting about this command is what it does with
// the answer.
type fileIDResolver interface {
	FileIDAt(ctx context.Context, clusterPath string) (string, error)
}

// mirrorPair is one volume's DR-side objects.
type mirrorPair struct {
	Source *corev1.PersistentVolume
	Volume *corev1.PersistentVolume
	Claim  *corev1.PersistentVolumeClaim
}

// replicaPathFor returns the cluster path the replica lives at on the peer.
//
// The same path on both arrays, which is what makes the DR PV differ from
// the original in `server` only. peerBasePath is the escape hatch for a peer
// that was laid out differently, and it re-roots only the parent -- the
// volume's own directory name is the third field of its handle and never
// changes.
func replicaPathFor(src *corev1.PersistentVolume, opt mirrorOptions) (string, error) {
	sourcePath := src.Spec.CSI.VolumeAttributes[volumeAttributeRealPath]
	if sourcePath == "" {
		return "", fmt.Errorf("PV %s has no %q volume attribute, so the replica's path on the peer "+
			"cannot be derived; pass -peer-base-path, or mirror a PV provisioned by a driver "+
			"version that records it", src.Name, volumeAttributeRealPath)
	}
	if opt.peerBasePath == "" {
		return sourcePath, nil
	}
	return path.Join(opt.peerBasePath, path.Base(sourcePath)), nil
}

// mirrorVolume builds the DR-side PV and PVC for one source PV.
//
// Pure, and now pure in a stronger sense than before: a handle names a path
// (pkg/driver/volume_id.go), the replica lives at the same path, so
// nothing array-specific has to be resolved for the rewrite at all. The
// caller still checks that the replica directory exists on the peer before
// writing a PV that points at it -- but that is a precondition, not an input.
func mirrorVolume(
	src *corev1.PersistentVolume,
	replicaPath string,
	opt mirrorOptions,
) (*corev1.PersistentVolume, *corev1.PersistentVolumeClaim, error) {
	if src.Spec.CSI == nil {
		return nil, nil, fmt.Errorf("PV %s is not a CSI volume", src.Name)
	}
	if opt.driverName != "" && src.Spec.CSI.Driver != opt.driverName {
		return nil, nil, fmt.Errorf("PV %s belongs to driver %q, not %q",
			src.Name, src.Spec.CSI.Driver, opt.driverName)
	}

	handle, err := parseVolumeHandle(src.Spec.CSI.VolumeHandle)
	if err != nil {
		return nil, nil, fmt.Errorf("PV %s: %w", src.Name, err)
	}
	peerHandle, err := handle.onPeer(opt.peerServer, opt.peerAPIURL, replicaPath)
	if err != nil {
		return nil, nil, fmt.Errorf("PV %s: %w", src.Name, err)
	}

	claimRef, err := mirroredClaimRef(src, opt)
	if err != nil {
		return nil, nil, err
	}

	// Copied, then edited: any attribute this tool does not know about has to
	// survive, because the node reads them all and the controller stamped them
	// for a reason.
	attributes := maps.Clone(src.Spec.CSI.VolumeAttributes)
	if attributes == nil {
		attributes = map[string]string{}
	}
	attributes[volumeAttributeServer] = opt.peerServer
	attributes[volumeAttributeReplicated] = "true"

	annotations := map[string]string{
		annotationMirroredFrom: src.Name,
		annotationSourceHandle: src.Spec.CSI.VolumeHandle,
		annotationSourceServer: src.Spec.CSI.VolumeAttributes[volumeAttributeServer],
	}
	if dir := mirroredDeleteDirectory(src, handle, peerHandle); dir != "" {
		annotations[annotationDeleteDirectory] = dir
	}

	storageClass := src.Spec.StorageClassName
	if opt.storageClass != "" {
		storageClass = opt.storageClass
	}

	mirrored := &corev1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        src.Name + opt.pvSuffix,
			Labels:      maps.Clone(src.Labels),
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:     src.Spec.Capacity,
			AccessModes:  src.Spec.AccessModes,
			MountOptions: src.Spec.MountOptions,
			VolumeMode:   src.Spec.VolumeMode,
			// Retain, always, whatever the source was. This object exists to
			// survive the incident that destroyed the source; a Delete policy
			// would let one `kubectl delete pvc` tree-delete the replica.
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              storageClass,
			ClaimRef:                      claimRef,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:           src.Spec.CSI.Driver,
					VolumeHandle:     peerHandle.String(),
					VolumeAttributes: attributes,
					FSType:           src.Spec.CSI.FSType,
					ReadOnly:         src.Spec.CSI.ReadOnly,
					// The secret references follow the handle: every RPC that
					// carries one dials the array the handle names, which is
					// now the peer.
					NodeStageSecretRef:         mirroredSecretRef(src.Spec.CSI.NodeStageSecretRef, opt),
					NodePublishSecretRef:       mirroredSecretRef(src.Spec.CSI.NodePublishSecretRef, opt),
					ControllerPublishSecretRef: mirroredSecretRef(src.Spec.CSI.ControllerPublishSecretRef, opt),
					ControllerExpandSecretRef:  mirroredSecretRef(src.Spec.CSI.ControllerExpandSecretRef, opt),
					NodeExpandSecretRef:        mirroredSecretRef(src.Spec.CSI.NodeExpandSecretRef, opt),
				},
			},
		},
	}

	claim := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimRef.Name,
			Namespace: claimRef.Namespace,
			Annotations: map[string]string{
				annotationMirroredFrom: src.Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: src.Spec.AccessModes,
			VolumeMode:  src.Spec.VolumeMode,
			// volumeName plus the PV's claimRef is the two-sided binding the
			// Kubernetes PV controller requires for a pre-bound pair. One side
			// alone leaves the claim Pending.
			VolumeName: mirrored.Name,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: src.Spec.Capacity[corev1.ResourceStorage],
				},
			},
		},
	}
	if storageClass != "" {
		class := storageClass
		claim.Spec.StorageClassName = &class
	}

	return mirrored, claim, nil
}

// mirroredDeleteDirectory returns the delete-directory opt-in the mirrored PV
// carries, or "" for none.
//
// The driver lets Kubernetes delete a directory only for a PV that owns it:
// one named after the directory, one whose mirrored-from names it, or one
// whose delete-directory annotation is the directory's real path. A mirror's
// mirrored-from is its source's name, so the mirror of a dynamically
// provisioned PV owns its replica already and gets nothing more. A source
// that owned its directory any other way -- through the opt-in, or as a
// mirror itself -- would leave its mirror owning nothing, so the mirror gets
// the opt-in, naming the replica's path from its own handle, which is where
// -peer-base-path put it. A source that did not own its directory gives its
// mirror nothing: a mirror never owns more than its source did. The mirror is
// Retain regardless; this only decides what happens if an operator later sets
// it to Delete.
func mirroredDeleteDirectory(src *corev1.PersistentVolume, sourceHandle, peerHandle volumeHandle) string {
	if src.Name == peerHandle.Name() {
		return ""
	}
	sourceName := sourceHandle.Name()
	owned := src.Name == sourceName ||
		src.Annotations[annotationMirroredFrom] == sourceName ||
		namesDirectory(src.Annotations[annotationDeleteDirectory], sourceHandle.RealPath)
	if !owned {
		return ""
	}
	return peerHandle.RealPath
}

// namesDirectory reports whether a delete-directory value names dir the way
// the driver reads it: absolute, no "." or ".." element, equal once cleaned.
func namesDirectory(value, dir string) bool {
	if !path.IsAbs(value) {
		return false
	}
	for _, elem := range strings.Split(value, "/") {
		if elem == "." || elem == ".." {
			return false
		}
	}
	return path.Clean(value) == dir
}

// mirroredClaimRef decides which PVC the mirrored PV is reserved for.
//
// Pre-setting it is what stops any other PVC in the cluster binding the DR
// copy first, which on a one-cluster deployment is a real race: the mirrored
// PV is Available and matches the class.
func mirroredClaimRef(src *corev1.PersistentVolume, opt mirrorOptions) (*corev1.ObjectReference, error) {
	if src.Spec.ClaimRef == nil {
		return nil, fmt.Errorf("PV %s has no claimRef, so there is no claim to reserve the mirror for; "+
			"mirror a bound PV, or bind this one first", src.Name)
	}

	return &corev1.ObjectReference{
		APIVersion: "v1",
		Kind:       "PersistentVolumeClaim",
		Namespace:  src.Spec.ClaimRef.Namespace,
		Name:       src.Spec.ClaimRef.Name + opt.claimSuffix,
		// UID and ResourceVersion are deliberately cleared. A claimRef naming
		// the source PVC's UID would never match the PVC created on the DR
		// cluster -- a new object gets a new UID -- and the PV would sit
		// Released forever. The namespace/name-only form is what Dell's
		// `repctl` and Ramen's restored PVs use, and it is what binds.
	}, nil
}

// mirroredSecretRef points a secret reference at the peer's credential.
func mirroredSecretRef(src *corev1.SecretReference, opt mirrorOptions) *corev1.SecretReference {
	if src == nil {
		return nil
	}

	out := *src
	if opt.secretName != "" {
		out.Name = opt.secretName
	}
	if opt.secretNamespace != "" {
		out.Namespace = opt.secretNamespace
	}
	return &out
}

// mirrorAll checks each source PV's replica exists on the peer and builds its
// DR-side pair.
//
// One lookup per volume, which is what the DR doc's "a hundred PVs is a
// hundred lookups" limit was about: the lookups are still one per volume, they
// are just no longer done by a human -- and they no longer decide anything,
// since the handle carries the path rather than the peer's file ID.
func mirrorAll(
	ctx context.Context,
	sources []*corev1.PersistentVolume,
	resolver fileIDResolver,
	opt mirrorOptions,
) ([]mirrorPair, error) {
	pairs := make([]mirrorPair, 0, len(sources))

	for _, src := range sources {
		if src.Spec.CSI == nil || (opt.driverName != "" && src.Spec.CSI.Driver != opt.driverName) {
			// Not ours. Skipped rather than refused: `kubectl get pv -o yaml`
			// hands over every PV in the cluster, and making the operator
			// filter first is friction with nothing behind it.
			continue
		}

		replicaPath, err := replicaPathFor(src, opt)
		if err != nil {
			return nil, err
		}

		// The replica's file ID is no longer part of the handle, so this
		// lookup does not feed the rewrite any more. It stays as a
		// PRECONDITION: writing a PersistentVolume that points at a directory
		// which is not on the peer yet produces a claim that binds and then
		// fails to mount, and the operator finds out during a failover. One
		// call per volume, and the answer is discarded.
		if _, err := resolver.FileIDAt(ctx, replicaPath); err != nil {
			return nil, fmt.Errorf("the replica directory %s is not present on the peer for PV %s: %w",
				replicaPath, src.Name, err)
		}

		mirrored, claim, err := mirrorVolume(src, replicaPath, opt)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, mirrorPair{Source: src, Volume: mirrored, Claim: claim})
	}

	if len(pairs) == 0 {
		return nil, fmt.Errorf("no PersistentVolumes provisioned by %q found in the input", opt.driverName)
	}
	return pairs, nil
}

// mirrorSummary is the one-line-per-volume report printed alongside the
// manifests, so a reviewer can see the two fields that changed without
// reading YAML.
func mirrorSummary(pairs []mirrorPair) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %d volume(s) mirrored\n", len(pairs))
	for _, p := range pairs {
		fmt.Fprintf(&b, "#   %s -> %s\n#       handle %s -> %s\n#       claim  %s/%s\n",
			p.Source.Name, p.Volume.Name,
			p.Source.Spec.CSI.VolumeHandle, p.Volume.Spec.CSI.VolumeHandle,
			p.Claim.Namespace, p.Claim.Name)
	}
	return b.String()
}
