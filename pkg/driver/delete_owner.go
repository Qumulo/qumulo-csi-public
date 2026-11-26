// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// refuseUnlessPersistentVolumesAllowDelete asks Kubernetes, with one paged
// List of this driver's PVs, the two questions about a delete that nothing
// on the array can answer: does another PV still in use name the
// directory (refuseWhileAnotherPVUsesIt), and does the PV being released own
// it (refuseUnlessReleasedPVOwnsIt). handle is the request's volume ID,
// verbatim.
//
// A DeleteVolumeRequest carries the volume ID and secrets and nothing else,
// so nothing the array holds can tell two PVs with one handle apart, or a
// volume this driver made from a directory an administrator made: a static PV
// carrying a verbatim copy of a Bound claim's handle, once released, passed
// confirmVolumeIdentity -- the export's description is derived from the same
// handle -- and its delete removed the owner's export, quota and tree while
// the owner stayed Bound; a hand-written PV aimed at a folder with a
// quota and no export had the folder tree-deleted. Kubernetes can tell
// them apart, so the controller asks it: one List per delete that has
// something to remove, shared by both checks, never a watch and nothing kept
// between requests (Rules 0 and 4 in ARCHITECTURE.md).
//
// A failed list refuses with Unavailable and removes nothing. A tree delete
// cannot be undone and a delayed one costs nothing but time: csi-provisioner
// retries with backoff, and a controller that cannot reach the Kubernetes API
// is one whose provisioner cannot reach it either. With no lister at all --
// csi-sanity, a local run, any controller outside a cluster -- both checks
// are skipped, logged once, and DeleteVolume behaves as it did before them.
func (s *ControllerServer) refuseUnlessPersistentVolumesAllowDelete(
	ctx context.Context, parsed volumeID, handle string,
) error {
	lister := s.driver.PersistentVolumeLister
	if lister == nil {
		s.noPVListerLogged.Do(func() {
			slog.WarnContext(ctx, "deletevolume: no Kubernetes API access, so DeleteVolume cannot check that the "+
				"PersistentVolume being released owns a volume's directory and that no other PersistentVolume still "+
				"uses it before removing it. Expected outside a cluster, such as under csi-sanity; in a "+
				"cluster the controller lists PersistentVolumes through its in-cluster ServiceAccount")
		})
		return nil
	}

	pvs, err := lister.ListPersistentVolumes(ctx, s.driver.Name)
	if err != nil {
		return status.Errorf(codes.Unavailable,
			"deletevolume: cannot list PersistentVolumes to confirm that the released PersistentVolume owns %q and "+
				"that no other PersistentVolume still uses it (%v); nothing was deleted, and the delete is retried. "+
				"The controller's ServiceAccount needs list on persistentvolumes", parsed.RealPath, err)
	}
	if err := refuseWhileAnotherPVUsesIt(ctx, parsed, pvs); err != nil {
		return err
	}
	return refuseUnlessReleasedPVOwnsIt(ctx, parsed, handle, pvs)
}

// refuseWhileAnotherPVUsesIt refuses a DeleteVolume while another
// PersistentVolume that is still in use names the directory the delete would
// remove, or a directory below it.
//
// The rule, applied to the PVs whose handle decodes to the same array and the
// same directory (any handle form: two-field, trailing slash, the default
// endpoint spelled out):
//
//   - If only one PV names the directory, it is the one being deleted --
//     csi-provisioner calls DeleteVolume for a PV that still exists, held by
//     its finalizer -- and nothing here stops the delete, whatever its phase.
//   - Otherwise any of them that is still in use refuses it: a PV not
//     Released and not Failed (Bound, Available, Pending, or no phase yet),
//     and not being deleted. The PV being deleted is Released, so it never
//     blocks itself.
//
// A PV in use whose directory lies *below* this one refuses it too, since a
// tree delete takes that directory with it. One whose directory lies above
// does not: deleting a volume inside a share someone has mounted is what a
// shared-subdirectory class does every day.
//
// The owner is never blocked for good. Once its own claim is deleted it is
// Released too, nothing in use names the directory, and its delete proceeds;
// the refused copy's next retry finds the directory gone, which is the
// idempotent success CSI asks for. Two Released PVs naming one directory are
// both on their way out, so whichever delete comes first removes it -- if
// refuseUnlessReleasedPVOwnsIt lets it.
//
// Which PV the request is for is not known, so the refusal names the PVs in
// use rather than "the copy". Both ways round it is the right answer: if the
// copy is released the owner keeps its data, and if the owner is released
// while a copy is still Bound the copy -- which mounts the same directory --
// keeps it until it is released or removed.
//
// It matches on the decoded directory, so it cannot see a copy whose handle
// pins another REST endpoint of the same array (sameArray keeps two explicit
// endpoints apart, for). refuseUnlessReleasedPVOwnsIt, which needs no
// endpoint reasoning, refuses that copy instead.
func refuseWhileAnotherPVUsesIt(ctx context.Context, parsed volumeID, pvs []PersistentVolumeInfo) error {
	var same, inUse []PersistentVolumeInfo
	for _, pv := range pvs {
		other, err := parseVolumeID(pv.Handle)
		if err != nil || !sameArray(parsed, other) {
			// A handle that does not decode names nothing this driver
			// could delete, and its own delete is refused or ignored.
			continue
		}
		switch {
		case other.RealPath == parsed.RealPath:
			same = append(same, pv)
			if persistentVolumeInUse(pv) {
				inUse = append(inUse, pv)
			}
		case persistentVolumeInUse(pv):
			if _, below := relPathUnder(parsed.RealPath, other.RealPath); below {
				inUse = append(inUse, pv)
			}
		}
	}
	if len(same) == 1 {
		// The only PV naming the directory is this delete's own; drop it
		// from inUse so only PVs below can still refuse.
		inUse = without(inUse, same[0].Name)
	}
	if len(inUse) == 0 {
		return nil
	}

	slog.WarnContext(ctx, "deletevolume: refusing, another PersistentVolume still uses the directory",
		"volume", parsed.Name, "path", parsed.RealPath, "persistentVolumes", describePVs(inUse))
	return status.Errorf(codes.FailedPrecondition,
		"deletevolume: %q is still used by %s; nothing was deleted. A PersistentVolume whose claim was deleted "+
			"cannot take a directory another PersistentVolume still uses. If the released PersistentVolume is a "+
			"copy or a hand-written import, set its persistentVolumeReclaimPolicy to Retain and delete it; if it "+
			"is the owner, the delete proceeds once no other PersistentVolume in use names %q",
		parsed.RealPath, describePVs(inUse), parsed.RealPath)
}

// refuseUnlessReleasedPVOwnsIt refuses a DeleteVolume unless the
// PersistentVolume being released owns the directory.
//
// csi-provisioner calls DeleteVolume for any released PV with reclaim Delete
// that names this driver -- a hand-written one too, with or without the
// provisioned-by annotation -- and the array cannot tell a volume this driver
// made from an administrator's folder that carries a quota, or from a volume
// whose owner was released with Retain. So the PV decides. The PVs being
// released are those whose handle equals the request's volume ID exactly --
// csi-provisioner passes the PV's handle verbatim, so no endpoint reasoning
// is needed -- that are not in use (persistentVolumeInUse) and not reclaim
// Retain: csi-provisioner never deletes a Retain PV's volume, so a retained
// owner lends a copy nothing. One of them must own the directory:
//
//   - its name is the directory's last path element. Every dynamically
//     provisioned PV does: csi-provisioner names the PV and the volume alike
//     (with any --volume-name-prefix), and CreateVolume makes the directory
//     realPathFor(name);
//   - or its AnnotationMirroredFrom is that name: a DR-side PV the repoint
//     tool wrote for a dynamically provisioned source;
//   - or its AnnotationDeleteDirectory, cleaned, is the directory's real path
//     -- an administrator's deliberate opt-in on a hand-written or imported
//     PV. A value with a "." or ".." element is not one, whatever it cleans
//     to, and neither is the NFS export path.
//
// This holds for every directory, a dedicated-export volume's as much as a
// quota-only one: a dedicated export's description is derived from the
// handle, so it says nothing about which PV carries it.
//
// No PV carrying the handle at all is allowed: csi-provisioner deletes a
// volume that has no PV when saving the PV after CreateVolume failed, and a
// refusal would leak it. PVs carrying the handle none of which is released
// yet are the PV being deleted seen a moment early -- refuseWhileAnotherPVUsesIt
// has already refused any second one in use -- so those not reclaim Retain
// are judged instead, and an owner is never held up by its own phase.
//
// The owner is never stranded: a PV named after its directory is deleted as
// before, and a released copy, once refused, retries until it is set to
// Retain and removed, deleting nothing meanwhile.
func refuseUnlessReleasedPVOwnsIt(ctx context.Context, parsed volumeID, handle string, pvs []PersistentVolumeInfo) error {
	var carriers, released []PersistentVolumeInfo
	for _, pv := range pvs {
		if pv.Handle != handle {
			continue
		}
		carriers = append(carriers, pv)
		if !persistentVolumeInUse(pv) && !reclaimRetained(pv) {
			released = append(released, pv)
		}
	}
	if len(carriers) == 0 {
		return nil
	}
	judged := released
	if len(judged) == 0 {
		for _, pv := range carriers {
			if !reclaimRetained(pv) {
				judged = append(judged, pv)
			}
		}
	}
	for _, pv := range judged {
		if ownsDirectory(pv, parsed) {
			return nil
		}
	}

	named := judged
	if len(named) == 0 {
		named = carriers
	}
	slog.WarnContext(ctx, "deletevolume: refusing, the released PersistentVolume does not own the directory",
		"volume", parsed.Name, "path", parsed.RealPath, "persistentVolumes", describePVs(named))
	return status.Errorf(codes.FailedPrecondition,
		"deletevolume: %s does not own %q; nothing was deleted. Kubernetes deletes a directory only for a "+
			"PersistentVolume named after it (%q, as every dynamically provisioned one is), one mirrored from such "+
			"a PersistentVolume (%s: %q), or one an administrator marked. If the PersistentVolume is a copy or a "+
			"hand-written import, set its persistentVolumeReclaimPolicy to Retain and delete it; if Kubernetes "+
			"really should delete the directory and everything in it, add the annotation %s: %q to the "+
			"PersistentVolume",
		describePVs(named), parsed.RealPath, parsed.Name, AnnotationMirroredFrom, parsed.Name,
		AnnotationDeleteDirectory, parsed.RealPath)
}

// ownsDirectory reports whether a PersistentVolume owns the directory its
// handle names; see refuseUnlessReleasedPVOwnsIt.
func ownsDirectory(pv PersistentVolumeInfo, parsed volumeID) bool {
	if pv.Name == parsed.Name || pv.MirroredFrom == parsed.Name {
		return true
	}
	dir := pv.DeleteDirectory
	return path.IsAbs(dir) && !hasDotElement(dir) && path.Clean(dir) == parsed.RealPath
}

// reclaimRetained reports whether a PV's reclaim policy keeps its volume.
// Only "Delete" lets csi-provisioner call DeleteVolume; "" is a lister that
// did not say, and counts as Delete.
func reclaimRetained(pv PersistentVolumeInfo) bool {
	return pv.ReclaimPolicy != "" && pv.ReclaimPolicy != "Delete"
}

// persistentVolumeInUse reports whether a PV still uses its directory: it is
// neither Released nor Failed, and is not being deleted. A PV with no phase
// yet has just been created and counts as in use.
func persistentVolumeInUse(pv PersistentVolumeInfo) bool {
	if pv.Deleting {
		return false
	}
	return pv.Phase != "Released" && pv.Phase != "Failed"
}

// sameArray reports whether two volume IDs may name the same array.
//
// The REST endpoint is what DeleteVolume acts on, so equal endpoints are the
// same array. A shared NFS address is too, unless both handles name an
// endpoint of their own and they differ: that is a class pinned to each array
// of a replicated pair behind one floating name, and a DR copy's PV
// must not hold up the delete of the source's volume, or the reverse. One
// handle with no endpoint dials whatever its server resolves to, which may be
// the other array, so it matches. Matching too much only refuses a delete;
// matching too little lets one through.
func sameArray(a, b volumeID) bool {
	if strings.EqualFold(a.APIEndpoint(), b.APIEndpoint()) {
		return true
	}
	return strings.EqualFold(a.Server, b.Server) && (a.APIURL == "" || b.APIURL == "")
}

// without returns pvs less the one named name.
func without(pvs []PersistentVolumeInfo, name string) []PersistentVolumeInfo {
	out := pvs[:0:0]
	for _, pv := range pvs {
		if pv.Name != name {
			out = append(out, pv)
		}
	}
	return out
}

// describePVs renders PVs for a refusal: name, phase and claim.
func describePVs(pvs []PersistentVolumeInfo) string {
	parts := make([]string, 0, len(pvs))
	for _, pv := range pvs {
		phase := pv.Phase
		if phase == "" {
			phase = "no phase yet"
		}
		desc := fmt.Sprintf("PersistentVolume %q (%s", pv.Name, phase)
		if pv.ClaimName != "" {
			desc += ", claim " + pv.ClaimNamespace + "/" + pv.ClaimName
		}
		parts = append(parts, desc+")")
	}
	return strings.Join(parts, ", ")
}
