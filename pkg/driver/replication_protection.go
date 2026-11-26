// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// The group protection fan-out.
//
// One relationship on a class's basePath protects every volume under it, but
// the protected gauge was written for one volume only: the one the Info
// request named. csi-addons sends GetVolumeReplicationInfo for a
// VolumeReplication's dataSource and for nothing else, and a group has one
// VolumeReplication, so in a group of N volumes N-1 had no series at all.
//
// That is not the same as reading 0. QumuloCSIVolumeUnprotected is
// `csi_qumulo_volume_replication_protected == 0`, and an absent series is not
// zero -- it is invisible. Losing the group's relationship raised an alert
// for at most one of the class's volumes, and the VolumeReplication object
// could not be used instead: on csi-addons v0.15.0 a Completed primary keeps
// reading Healthy after the relationship is gone (docs/reference/metrics.md, Alerts),
// which is why this gauge exists at all.
//
// So the verdict a group report reaches is exported for every member of that
// group, not only for the volume that was asked about.
//
// # Where the member list comes from
//
// From this driver's PersistentVolumes, not from the array.
//
// Info already walks the group on the array, in stageLateMemberQuotas, and
// the member list is right there. It is deliberately not used: that walk is
// fingerprint-gated by and returns without listing the members when the
// membership is unchanged and groupRestageEvery has not elapsed. Hanging the
// gauge off it would either re-introduce the per-member reads removed --
// about five REST calls per member per poll -- or write the series only on
// the polls where staging happened to run, so they would flap in and out of
// existence every ten minutes. Both are worse than the defect.
//
// The PersistentVolume list costs nothing extra: a controller in a cluster
// already holds a lister for DeleteVolume's ownership check, and this
// reuses it behind a cache, because Info is polled continuously and Rule 4
// forbids a watch.
//
// # Why this does not cross ARCHITECTURE.md Rule 0
//
// Rule 0 puts *discovering clusters no request named* behind
// controller.inventory.enabled. This discovers no cluster and resolves no
// credential: the array, the endpoint and the Secret are all still the Info
// request's own, and the PersistentVolume list is the same one DeleteVolume
// reads unconditionally. It only answers "which of this driver's volumes sit
// under the directory this relationship already covers".

// memberListReuse is how long one listing of this driver's PersistentVolumes
// is reused by the fan-out.
//
// Info is polled continuously by csi-addons, for every VolumeReplication in
// the cluster, so a List per RPC would put a Kubernetes API call on the hot
// path of every poll of every relationship. The listing is shared by every
// group on this controller instead, on the same cadence the metrics reconcile
// already uses. Consequence, accepted: a member created since the last
// listing gets its first series up to this late.
const memberListReuse = 10 * time.Minute

// groupFanoutReuse is how long the root of a group that covered a volume is
// remembered after a successful report, so that a later report finding no
// relationship can set the same members to 0.
//
// Without it the loss of a group relationship -- the event this gauge exists
// to catch -- would retire the members' series instead of zeroing them, and
// QumuloCSIVolumeUnprotected cannot fire on an absent series. Refreshed by
// every successful group report, so it expires only once polling has stopped,
// by which time the series are stale anyway and the reconcile retires
// them when their PersistentVolumes go.
const groupFanoutReuse = time.Hour

// memberListCache is one listing of this driver's PersistentVolumes, reused
// for memberListReuse. A hint in the sense of Rule 0: losing it costs one
// list, and nothing is believed from it that is not re-checked -- every
// handle is parsed and matched against the request's own array and the
// group's root on each use.
type memberListCache struct {
	mu   sync.Mutex
	at   time.Time
	pvs  []PersistentVolumeInfo
	warn sync.Once
}

// groupFanoutHint is the group root a report last found covering a volume.
type groupFanoutHint struct {
	root string
	at   time.Time
}

func (h groupFanoutHint) expired(now time.Time) bool {
	return now.Sub(h.at) >= groupFanoutReuse
}

// observeGroupProtection exports protected for every volume of this driver
// that sits under root on the same array as vol, as well as for vol itself,
// which the caller has already done.
//
// root is the group relationship's own root. Nothing is written when the
// controller has no PersistentVolume lister -- csi-sanity, any run outside a
// cluster -- and a listing that fails writes nothing either: this reports a
// verdict, it must never fail or delay the report it rides on.
func (r *ReplicationServer) observeGroupProtection(
	ctx context.Context, local replicationClient, localAddr string, vol volumeID, root string, protected bool,
) {
	members := r.groupMemberVolumes(ctx, localAddr, vol, root)
	for _, member := range members {
		local.ObserveVolumeProtection(qumulo.WithSubject(ctx, member.String()), protected)
	}
	if len(members) > 0 {
		slog.DebugContext(ctx, "replication: exported the group's verdict for its other members",
			"root", root, "members", len(members), "protected", protected)
	}
}

// groupMemberVolumes is every volume of this driver, other than vol itself,
// whose handle places it under root on vol's array.
//
// Matching is on the array *and* the endpoint *and* whole path components,
// never on a string prefix of the path alone. All three matter, and each one
// guards a real false positive:
//
//   - the endpoint and server, because the DR copies on the peer array live
//     at the same paths, and reporting a replica as protected by its own
//     source's relationship would be wrong in the most misleading direction;
//   - whole components, because a sibling class rooted at /csi/shared-r60 is
//     not under a group at /csi/shared-r6, and relativeUnder is what the
//     delete guard already uses for the same reason;
//   - strictly under, because root itself is the class basePath, not a
//     volume.
//
// A false positive here would report an unprotected volume as protected,
// which is strictly worse than the silence this fixes. When in doubt, say
// nothing.
func (r *ReplicationServer) groupMemberVolumes(
	ctx context.Context, localAddr string, vol volumeID, root string,
) []volumeID {
	pvs, ok := r.listVolumesForFanout(ctx)
	if !ok {
		return nil
	}

	self := vol.String()
	endpoint := vol.APIEndpoint()
	members := make([]volumeID, 0, len(pvs))
	for _, pv := range pvs {
		if pv.Handle == "" {
			continue
		}
		member, err := parseVolumeID(pv.Handle)
		if err != nil {
			// A handle this driver did not mint, or one from a version that
			// spelled them differently. Not this function's business to
			// report: it is read by the handle-compat path.
			continue
		}
		// Compared parsed, not as written: a PV carrying the legacy
		// "<server>#<path>" spelling of vol's own handle is vol.
		if member.String() == self {
			continue
		}
		if !strings.EqualFold(member.Server, vol.Server) || member.APIEndpoint() != endpoint {
			continue
		}
		if rest, under := relativeUnder(root, member.RealPath); !under || rest == "" {
			continue
		}
		// A member with a relationship of its own reports its own verdict,
		// and the group's must not overwrite it. EnableVolumeReplication
		// refuses a per-volume VolumeReplication on a shared-subdirectory
		// volume, so a group member cannot acquire one through
		// this driver; this covers one an operator created on the array by
		// hand, as far as a report on this controller has seen it.
		if r.hasOwnRelationshipHint(localAddr + "\x00" + member.RealPath) {
			continue
		}
		members = append(members, member)
	}
	return members
}

// listVolumesForFanout returns this driver's PersistentVolumes, from the
// cache when it is fresh enough. The bool is false when there is no lister or
// the list could not be read; neither is an error here, because the fan-out
// is an addition to a report that must still be made.
func (r *ReplicationServer) listVolumesForFanout(ctx context.Context) ([]PersistentVolumeInfo, bool) {
	lister := r.driver.PersistentVolumeLister
	if lister == nil {
		r.memberList.warn.Do(func() {
			slog.WarnContext(ctx, "replication: no Kubernetes API access, so a group's protected gauge is exported "+
				"for the volume each report names and not for the group's other members. Expected outside a "+
				"cluster, such as under csi-sanity")
		})
		return nil, false
	}

	r.memberList.mu.Lock()
	defer r.memberList.mu.Unlock()
	// The slice is handed out and read after the lock is released, so it is
	// never written again once stored: forgetMember replaces it rather than
	// compacting it in place.
	if r.memberList.pvs != nil && time.Since(r.memberList.at) < memberListReuse {
		return r.memberList.pvs, true
	}

	pvs, err := lister.ListPersistentVolumes(ctx, r.driver.Name)
	if err != nil {
		// Keep whatever the last successful listing held rather than
		// dropping the members' series on one failed call: an unreadable PV
		// list is not an empty one, the same judgement RunVolumeReconcile
		// makes. It is retried on the next poll.
		slog.WarnContext(ctx, "replication: cannot list PersistentVolumes to export a group's verdict for its "+
			"members, so this report covers the volume it names only; retrying on the next poll",
			"error", err)
		return r.memberList.pvs, r.memberList.pvs != nil
	}
	r.memberList.pvs, r.memberList.at = pvs, time.Now()
	return pvs, true
}

// forgetMember drops one volume from the cached PersistentVolume listing the
// group fan-out reads (the shape).
//
// The listing is up to memberListReuse old, so for ten minutes after a
// DeleteVolume the next group report still found the deleted volume among
// the group's members and exported a protected gauge for it -- a series for
// a volume that is gone, which the reconcile then has to retire and which
// reads protected=0 in the meantime, firing QumuloCSIVolumeUnprotected for
// nothing. Evicting the one entry is cheaper and more exact than dropping
// the whole listing, which would put a Kubernetes List back on the next
// poll of every relationship.
//
// A no-op when there is no listing, no replication server, or no lister:
// nothing is lost either way, and the next listing will not carry the
// volume at all.
//
// Handles are compared in their canonical form: DeleteVolume passes the
// parsed handle, while the listing holds each PV's handle as written, which
// for a volume an older build provisioned is the legacy "<server>#<path>"
// spelling -- compared as strings, that volume was never evicted.
//
// The listing is replaced, never edited in place: listVolumesForFanout hands
// the stored slice to readers that iterate it without the lock, and
// compacting it under them shifted a live member out of a report in progress.
func (r *ReplicationServer) forgetMember(handle string) {
	if r == nil {
		return
	}
	target := canonicalMemberHandle(handle)
	r.memberList.mu.Lock()
	defer r.memberList.mu.Unlock()
	var kept []PersistentVolumeInfo
	for i, pv := range r.memberList.pvs {
		if canonicalMemberHandle(pv.Handle) != target {
			if kept != nil {
				kept = append(kept, pv)
			}
			continue
		}
		if kept == nil {
			kept = make([]PersistentVolumeInfo, i, len(r.memberList.pvs))
			copy(kept, r.memberList.pvs[:i])
		}
	}
	if kept != nil {
		r.memberList.pvs = kept
	}
}

// canonicalMemberHandle is the one spelling of a volume handle, so the
// legacy and current forms of one volume compare equal. A handle that does
// not parse is returned as it stands.
func canonicalMemberHandle(handle string) string {
	parsed, err := parseVolumeID(handle)
	if err != nil {
		return handle
	}
	return parsed.String()
}

// hasOwnRelationshipHint reports whether a report on this controller last
// found a relationship rooted at key's own path. Unexpired hints only, and
// read without touching an array: this decides whether to leave a volume's
// own verdict alone, never what that verdict is.
func (r *ReplicationServer) hasOwnRelationshipHint(key string) bool {
	v, ok := r.hints.relationships.Load(key)
	if !ok {
		return false
	}
	hint := v.(relationshipHint)
	return !hint.expired(time.Now()) && hint.id != ""
}

// rememberGroupFanout records that root's relationship covered vol, so a
// later report that finds none can zero the same members.
func (r *ReplicationServer) rememberGroupFanout(key, root string) {
	r.hints.groupFanouts.Store(key, groupFanoutHint{root: root, at: time.Now()})
	r.noteHintStored()
}

// lastGroupFanoutRoot is the root rememberGroupFanout stored for key, while
// it is still within groupFanoutReuse.
func (r *ReplicationServer) lastGroupFanoutRoot(key string) (string, bool) {
	v, ok := r.hints.groupFanouts.Load(key)
	if !ok {
		return "", false
	}
	hint := v.(groupFanoutHint)
	if hint.expired(time.Now()) {
		r.hints.groupFanouts.Delete(key)
		return "", false
	}
	return hint.root, true
}
