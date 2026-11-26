// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// `failover` and `failback`: one command per direction.
//
// Measured on the lab pair (see also docs/protect/recover.md), a failover with the prototype driver
// and the csi-addons controller v0.15.0 is four operator actions, and failback
// is the same four the other way:
//
//  1. the side being promoted: `replicationState: primary`. The controller
//     runs Enable then Promote, and force-promotes by itself when the source
//     array is unreachable.
//  2. the side being demoted: `replicationState: secondary` with
//     `autoResync: true` -- Demote, then Resync as the reverse and reconnect.
//     While the old array is down each Demote fails `Unavailable` and the
//     controller backs off exponentially, so the freeze can land minutes after
//     the array answers. **Any spec change on the object forces an immediate
//     reconcile**, which is the one remedy, and this command applies it
//     itself once rather than printing it.
//  3. for a DNS-named class, the record flip, then one TTL.
//  4. the pods, because a live NFS mount never follows a failover.
//
// None of the four is hard; all four in order, across every volume in a
// namespace, at 3 a.m., is. This command derives the pairs from the objects
// `mirror` already wrote, prints the plan, and -- with `-dry-run=false` -- does
// the four in that order, waiting on each `VolumeReplication` before the next.
//
// What it deliberately does **not** do: move a claim. For an IP-named class
// the pod has to move to the mirrored PVC because `spec.csi` is immutable, and
// that rebind is `switch`'s plan (switchcmd.go), reviewed on its own. This
// command names the pairs that need it and stops there.

// The csi-addons VolumeReplication CRD, addressed dynamically.
//
// The Go types live in `github.com/csi-addons/kubernetes-csi-addons`, which
// this module does not depend on and should not: the driver speaks the
// csi-addons *gRPC* spec (`github.com/csi-addons/spec`), and pulling the
// controller's API module in for one operator CLI would tie the driver's build
// to the controller's release cadence. Six fields are read and two written;
// `unstructured` covers that without a dependency, and an unknown field on a
// newer CRD is carried through a merge patch untouched rather than dropped by
// a stale struct.
var volumeReplicationGVR = schema.GroupVersionResource{
	Group:    "replication.storage.openshift.io",
	Version:  "v1alpha1",
	Resource: "volumereplications",
}

// volumeReplicationClassGVR is the cluster-scoped class, read for one
// parameter: `replicationScope`. A `shared-subdirectory` class is protected as
// a **group** -- one relationship rooted at the class `basePath`, one
// `make-writable` at promote (the group form in
// docs/reference/supported-features.md) -- so the driver refuses a per-PVC VolumeReplication
// on one, and exactly one object drives every member. Without reading the
// class there is no way to tell that from a member with a *missing* object,
// and the two need opposite answers: one is a working configuration, the other
// is a failover that would leave volumes behind.
var volumeReplicationClassGVR = schema.GroupVersionResource{
	Group:    "replication.storage.openshift.io",
	Version:  "v1alpha1",
	Resource: "volumereplicationclasses",
}

// volumeReplicationAPIVersion is the same coordinates in the form a manifest
// carries, for the `-f` path.
const volumeReplicationAPIVersion = "replication.storage.openshift.io/v1alpha1"

// volumeReplicationKind and volumeReplicationClassKind separate the two kinds
// that share those coordinates.
const (
	volumeReplicationKind      = "VolumeReplication"
	volumeReplicationClassKind = "VolumeReplicationClass"
)

// replicationScopeParameter is the class parameter that says a class is
// protected as one unit. `group` is the only value this command acts on.
const (
	replicationScopeParameter = "replicationScope"
	replicationScopeGroup     = "group"
)

// volumeReplicationClass is a class reduced to its name and parameters.
type volumeReplicationClass struct {
	Name       string
	Parameters map[string]string
}

// isGroup reports whether the class is protected as one unit.
func (c volumeReplicationClass) isGroup() bool {
	return c.Parameters[replicationScopeParameter] == replicationScopeGroup
}

// volumeReplicationClassFrom reads a class off an unstructured object.
func volumeReplicationClassFrom(u *unstructured.Unstructured) (volumeReplicationClass, error) {
	class := volumeReplicationClass{Name: u.GetName()}
	if class.Name == "" {
		return class, fmt.Errorf("a VolumeReplicationClass with no metadata.name")
	}

	parameters, _, err := unstructured.NestedStringMap(u.Object, "spec", "parameters")
	if err != nil {
		return class, fmt.Errorf("reading spec.parameters of VolumeReplicationClass %s: %w",
			class.Name, err)
	}
	class.Parameters = parameters
	return class, nil
}

// The two `spec.replicationState` values this command writes, and the two
// `status.state` values it waits for. The spec field is lower case and the
// status field is capitalised; that is the CRD's own asymmetry, not a typo.
const (
	replicationStatePrimary   = "primary"
	replicationStateSecondary = "secondary"

	replicationStatusPrimary   = "Primary"
	replicationStatusSecondary = "Secondary"
)

// Condition types on a VolumeReplication. `Completed` is the one that says the
// requested transition actually finished -- `status.state` alone flips to the
// requested role before the array work is done.
const (
	conditionCompleted = "Completed"
	conditionDegraded  = "Degraded"
	conditionResyncing = "Resyncing"
)

// reasonVolumeDegraded is the `Degraded` reason the csi-addons controller sets
// while a resync is rewinding the copy. It is transient by design, and
// it is the one reason a spec toggle must not land on: the controller is
// already reconciling, and the toggle only re-enqueues work it is doing.
const reasonVolumeDegraded = "VolumeDegraded"

// direction is which way the pair is being moved.
type direction string

const (
	// directionFailover promotes the mirrored side and demotes the original.
	directionFailover direction = "failover"
	// directionFailback is the same four steps the other way, and brings the
	// original PVC back into service unchanged.
	directionFailback direction = "failback"
)

// volumeReplication is the subset of the CR this tool reads.
type volumeReplication struct {
	Namespace string
	Name      string
	Class     string
	// ClaimName is `spec.dataSource.name`, which is how a VolumeReplication
	// names the PVC it protects and therefore the only join between the CR and
	// the mirrored objects.
	ClaimName    string
	DesiredState string
	AutoResync   bool
	State        string
	Message      string
	Conditions   map[string]string
	// Reasons is each condition's `reason`, which is what separates a transient
	// `Degraded` during a resync (`VolumeDegraded`) from one that is not
	// going to clear on its own.
	Reasons map[string]string
}

// volumeReplicationFrom reads the fields this tool uses off an unstructured CR.
func volumeReplicationFrom(u *unstructured.Unstructured) (volumeReplication, error) {
	vr := volumeReplication{
		Namespace:  u.GetNamespace(),
		Name:       u.GetName(),
		Conditions: map[string]string{},
		Reasons:    map[string]string{},
	}
	if vr.Name == "" {
		return vr, fmt.Errorf("a VolumeReplication with no metadata.name")
	}

	kind, _, _ := unstructured.NestedString(u.Object, "spec", "dataSource", "kind")
	if kind != "" && kind != "PersistentVolumeClaim" {
		return vr, fmt.Errorf("VolumeReplication %s/%s has a %s dataSource; this command pairs "+
			"PersistentVolumeClaim-backed objects only", vr.Namespace, vr.Name, kind)
	}

	claim, _, _ := unstructured.NestedString(u.Object, "spec", "dataSource", "name")
	if claim == "" {
		return vr, fmt.Errorf("VolumeReplication %s/%s names no spec.dataSource.name, so the claim "+
			"it protects cannot be derived", vr.Namespace, vr.Name)
	}
	vr.ClaimName = claim

	vr.Class, _, _ = unstructured.NestedString(u.Object, "spec", "volumeReplicationClass")
	vr.DesiredState, _, _ = unstructured.NestedString(u.Object, "spec", "replicationState")
	vr.AutoResync, _, _ = unstructured.NestedBool(u.Object, "spec", "autoResync")
	vr.State, _, _ = unstructured.NestedString(u.Object, "status", "state")
	vr.Message, _, _ = unstructured.NestedString(u.Object, "status", "message")

	conditions, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		conditionType, _ := condition["type"].(string)
		conditionStatus, _ := condition["status"].(string)
		conditionReason, _ := condition["reason"].(string)
		if conditionType != "" {
			vr.Conditions[conditionType] = conditionStatus
			if conditionReason != "" {
				vr.Reasons[conditionType] = conditionReason
			}
		}
	}
	return vr, nil
}

func (v volumeReplication) completed() bool { return v.Conditions[conditionCompleted] == "True" }
func (v volumeReplication) degraded() bool  { return v.Conditions[conditionDegraded] == "True" }

// resyncing reports the `Resyncing` condition, which the controller holds True
// for as long as the reverse job is rewinding the copy.
func (v volumeReplication) resyncing() bool { return v.Conditions[conditionResyncing] == "True" }

// reconciling reports the two states a nudge must never land on.
//
// Measured on the lab cluster: after the demote the tool waited for `Secondary`
// and not for `Degraded`, so the resync's transient `VolumeDegraded` made it
// toggle a spec field once on **every** object before they settled. Harmless
// there, but a spec toggle is the backoff remedy for a controller that is *not*
// working, and firing it at one that is re-enqueues work already in flight and
// hides the signal that the remedy was needed at all. So: wait these out first,
// and toggle only an object that has sat still.
func (v volumeReplication) reconciling() bool {
	return v.resyncing() || (v.degraded() && v.Reasons[conditionDegraded] == reasonVolumeDegraded)
}

// fingerprint is "has this object moved at all", for the no-progress gate on
// the nudge. Status, conditions and message: everything the controller writes
// back that a reconcile would change.
func (v volumeReplication) fingerprint() string {
	parts := make([]string, 0, len(v.Conditions)+2)
	parts = append(parts, v.State, v.Message)
	for _, conditionType := range []string{conditionCompleted, conditionDegraded, conditionResyncing} {
		parts = append(parts, conditionType+"="+v.Conditions[conditionType]+"/"+v.Reasons[conditionType])
	}
	return strings.Join(parts, "|")
}

// reached reports whether the CR is in the requested role **and finished
// getting there**.
//
// Both halves are needed. `status.state` goes to the requested role early:
// during a resync the old side already reads `Secondary` while `Completed` is
// still False and `Resyncing` True, and acting on the role alone would restart
// the pods onto a copy the reconnect is still rewinding.
func (v volumeReplication) reached(wantState string) bool {
	return strings.EqualFold(v.State, wantState) && v.completed()
}

// healthySecondary is the peer check the plan makes after the promote lands:
// demoted, finished, and not degraded.
func (v volumeReplication) healthySecondary() bool {
	return v.reached(replicationStatusSecondary) && !v.degraded()
}

// describe is the one-line "why not" for an object that has not got there.
func (v volumeReplication) describe() string {
	state := v.State
	if state == "" {
		state = "<no status yet>"
	}
	parts := []string{"state " + state}
	for _, conditionType := range []string{conditionCompleted, conditionDegraded, conditionResyncing} {
		if status, ok := v.Conditions[conditionType]; ok {
			if reason := v.Reasons[conditionType]; reason != "" {
				parts = append(parts, conditionType+"="+status+"("+reason+")")
				continue
			}
			parts = append(parts, conditionType+"="+status)
		}
	}
	if v.Message != "" {
		parts = append(parts, strings.TrimSpace(v.Message))
	}
	return strings.Join(parts, ", ")
}

// failoverPair is one protected volume's two sides.
type failoverPair struct {
	Namespace string

	SourcePV    *corev1.PersistentVolume
	MirrorPV    *corev1.PersistentVolume
	SourceClaim string
	MirrorClaim string
	SourceVR    volumeReplication
	MirrorVR    volumeReplication

	SourceServer string
	MirrorServer string

	// GroupClass is the StorageClass whose *group* VolumeReplication covers
	// this pair, and is empty for a pair driven by its own per-volume objects.
	// A group member has no object of its own by design, so this is what says
	// "covered" rather than "missing".
	GroupClass string
}

// derivePairs joins the mirrored objects to their originals and to the
// VolumeReplication on each side.
//
// The whole join runs off what `mirror` already wrote, because anything else
// would be a second source of truth for which copy is which:
//
//	mirrored PV  --annotation csi.qumulo.com/mirrored-from-->  original PV
//	mirrored PV  --spec.claimRef-->  mirrored PVC  <--spec.dataSource.name--  mirrored VR
//	original PV  --spec.claimRef-->  original PVC  <--spec.dataSource.name--  original VR
//
// Every ambiguity in that join is refused rather than resolved by a rule: two
// mirrors of one PV, two VolumeReplications on one claim, a claim in another
// namespace. A failover that picked one of two candidates would be picking
// which copy of the customer's data survives.
//
// The last hop has two shapes. A `shared-subdirectory` class is protected as a
// **group**: one relationship at the class `basePath`, one VolumeReplication
// per direction, and every member volume of that StorageClass riding on it --
// the driver refuses a per-PVC object on such a class, so a member with none
// of its own is correct rather than unprotected. So a claim with no object
// falls back to the group object for its StorageClass, and only a member with
// neither is refused. See replicationIndex.
func derivePairs(in objectStream, namespace, driverName string) ([]failoverPair, error) {
	if namespace == "" {
		return nil, fmt.Errorf("-namespace is required: pairs are derived within one namespace, " +
			"because a VolumeReplication names its claim by name alone")
	}

	volumesByName := map[string]*corev1.PersistentVolume{}
	for _, pv := range in.Volumes {
		volumesByName[pv.Name] = pv
	}

	claimsPresent := map[string]bool{}
	for _, pvc := range in.Claims {
		claimsPresent[pvc.Namespace+"/"+pvc.Name] = true
	}

	index := newReplicationIndex(in, namespace)

	var pairs []failoverPair
	mirroredOf := map[string]string{}
	claimedBy := map[string]string{}

	for _, mirror := range in.Volumes {
		sourceName := mirror.Annotations[annotationMirroredFrom]
		if sourceName == "" {
			continue
		}
		if mirror.Spec.CSI == nil || (driverName != "" && mirror.Spec.CSI.Driver != driverName) {
			continue
		}
		if mirror.Spec.ClaimRef == nil {
			return nil, fmt.Errorf("mirrored PV %s has no claimRef, so the claim its "+
				"VolumeReplication protects cannot be derived; re-run `mirror`", mirror.Name)
		}
		if mirror.Spec.ClaimRef.Namespace != namespace {
			continue
		}

		if previous, ok := mirroredOf[sourceName]; ok {
			return nil, fmt.Errorf("PVs %s and %s are both mirrors of %s: which copy is the DR copy "+
				"is ambiguous, and this command will not guess. Delete the stale mirror",
				previous, mirror.Name, sourceName)
		}
		mirroredOf[sourceName] = mirror.Name

		mirrorClaim := mirror.Spec.ClaimRef.Name
		if previous, ok := claimedBy[mirrorClaim]; ok {
			return nil, fmt.Errorf("mirrored PVs %s and %s both reserve claim %s/%s",
				previous, mirror.Name, namespace, mirrorClaim)
		}
		claimedBy[mirrorClaim] = mirror.Name

		source, ok := volumesByName[sourceName]
		if !ok {
			return nil, fmt.Errorf("mirrored PV %s names source PV %s, which is not in the input; "+
				"a failover needs both sides", mirror.Name, sourceName)
		}
		if source.Spec.ClaimRef == nil {
			return nil, fmt.Errorf("source PV %s has no claimRef, so its VolumeReplication cannot "+
				"be found", source.Name)
		}
		if source.Spec.ClaimRef.Namespace != namespace {
			return nil, fmt.Errorf("source PV %s is claimed in namespace %s but its mirror %s is "+
				"claimed in %s; this command works within one namespace",
				source.Name, source.Spec.ClaimRef.Namespace, mirror.Name, namespace)
		}
		sourceClaim := source.Spec.ClaimRef.Name

		if len(claimsPresent) > 0 {
			for _, claim := range []string{sourceClaim, mirrorClaim} {
				if !claimsPresent[namespace+"/"+claim] {
					return nil, fmt.Errorf("PVC %s/%s does not exist; the pair for PV %s is "+
						"incomplete. Apply what `mirror` emitted before failing over",
						namespace, claim, source.Name)
				}
			}
		}

		sourceVR, sourceGroup, err := index.forClaim(namespace, sourceClaim)
		if err != nil {
			return nil, err
		}
		mirrorVR, mirrorGroup, err := index.forClaim(namespace, mirrorClaim)
		if err != nil {
			return nil, err
		}
		groupClass := sourceGroup
		if groupClass == "" {
			groupClass = mirrorGroup
		}
		if sourceVR.Name == mirrorVR.Name {
			return nil, fmt.Errorf("VolumeReplication %s/%s is the only one found for both sides of "+
				"PV %s; each side needs its own", namespace, sourceVR.Name, source.Name)
		}

		pairs = append(pairs, failoverPair{
			Namespace:    namespace,
			SourcePV:     source,
			MirrorPV:     mirror,
			SourceClaim:  sourceClaim,
			MirrorClaim:  mirrorClaim,
			SourceVR:     sourceVR,
			MirrorVR:     mirrorVR,
			SourceServer: serverOf(source),
			MirrorServer: serverOf(mirror),
			GroupClass:   groupClass,
		})
	}

	if len(pairs) == 0 {
		return nil, fmt.Errorf("no mirrored volume pairs found in namespace %s: no PersistentVolume "+
			"carries the %s annotation. Run `qumulo-csi-repoint mirror` first",
			namespace, annotationMirroredFrom)
	}

	sort.Slice(pairs, func(i, j int) bool { return pairs[i].SourcePV.Name < pairs[j].SourcePV.Name })
	return pairs, nil
}

// replicationIndex answers "which VolumeReplication drives this claim", for
// both shapes the driver supports.
//
// Two shapes, because a `shared-subdirectory` class is protected as a group:
// one relationship rooted at the class `basePath`, one object, and every
// member volume of that StorageClass riding on it. The driver *refuses* a
// per-PVC VolumeReplication on such a class, so a member with no object of its
// own is a correct configuration, not a gap -- and telling that apart from a
// genuinely unprotected volume needs the class's `replicationScope` parameter,
// which is why this command reads VolumeReplicationClasses at all.
type replicationIndex struct {
	// byClaim is the per-volume shape: claim name -> the objects naming it.
	byClaim map[string][]volumeReplication
	// byGroupStorageClass is the group shape: the StorageClass of a group
	// object's own claim -> that object. Every member of that StorageClass is
	// driven by it.
	byGroupStorageClass map[string][]volumeReplication
	// storageClassOf resolves a claim in the namespace to its StorageClass.
	storageClassOf map[string]string
	// groupClassNames is the VolumeReplicationClasses whose scope is `group`.
	groupClassNames map[string]bool
	// mirrorClaims is the claims reserved by mirrored PVs. It is what keeps
	// the two sides of a group apart: on one cluster (topology B) the mirrored
	// PVCs can carry the *same* StorageClass as the originals, so both
	// directions' group objects would key to one entry. A group object drives
	// only the side its own claim is on.
	mirrorClaims map[string]bool
}

// newReplicationIndex builds both shapes from one pass over the input.
func newReplicationIndex(in objectStream, namespace string) replicationIndex {
	index := replicationIndex{
		byClaim:             map[string][]volumeReplication{},
		byGroupStorageClass: map[string][]volumeReplication{},
		storageClassOf:      map[string]string{},
		groupClassNames:     map[string]bool{},
		mirrorClaims:        map[string]bool{},
	}

	for _, pv := range in.Volumes {
		if pv.Annotations[annotationMirroredFrom] == "" {
			continue
		}
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != namespace {
			continue
		}
		index.mirrorClaims[pv.Spec.ClaimRef.Name] = true
	}

	for _, pvc := range in.Claims {
		if pvc.Namespace != namespace {
			continue
		}
		if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
			index.storageClassOf[pvc.Name] = *pvc.Spec.StorageClassName
		}
	}
	// A bound PVC may leave `storageClassName` unset while its PV carries it,
	// and a run from files may hold the PVs and not the claims.
	for _, pv := range in.Volumes {
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != namespace {
			continue
		}
		if _, ok := index.storageClassOf[pv.Spec.ClaimRef.Name]; !ok && pv.Spec.StorageClassName != "" {
			index.storageClassOf[pv.Spec.ClaimRef.Name] = pv.Spec.StorageClassName
		}
	}

	for _, class := range in.ReplicationClasses {
		if class.isGroup() {
			index.groupClassNames[class.Name] = true
		}
	}

	for _, vr := range in.Replications {
		if vr.Namespace != "" && vr.Namespace != namespace {
			continue
		}
		index.byClaim[vr.ClaimName] = append(index.byClaim[vr.ClaimName], vr)

		if !index.groupClassNames[vr.Class] {
			continue
		}
		// A group object still names one member claim as its dataSource; that
		// member's StorageClass is what it covers.
		storageClass := index.storageClassOf[vr.ClaimName]
		if storageClass == "" {
			continue
		}
		index.byGroupStorageClass[storageClass] = append(index.byGroupStorageClass[storageClass], vr)
	}

	return index
}

// forClaim returns the object driving one side of a pair, and the StorageClass
// whose group object it is when the claim is a group member.
//
// Its own object wins when there is one. A member with none falls back to the
// group object for its StorageClass, and only a member with neither is
// refused.
func (index replicationIndex) forClaim(namespace, claim string) (volumeReplication, string, error) {
	found := index.byClaim[claim]
	switch len(found) {
	case 1:
		// A group's object still names one member as its dataSource, so that
		// member reaches it here rather than through the class. It is the same
		// group all the same, and the plan has to say so.
		if index.groupClassNames[found[0].Class] {
			return found[0], index.storageClassOf[claim], nil
		}
		return found[0], "", nil
	case 0:
		// Fall through to the group shape.
	default:
		return volumeReplication{}, "", fmt.Errorf("%d VolumeReplications name PVC %s/%s as their "+
			"dataSource (%s): which one drives this side is ambiguous",
			len(found), namespace, claim, strings.Join(namesOf(found), ", "))
	}

	storageClass := index.storageClassOf[claim]
	if storageClass == "" {
		return volumeReplication{}, "", fmt.Errorf("no VolumeReplication protects PVC %s/%s, and "+
			"its StorageClass is not resolvable from the input, so whether a group object covers "+
			"it cannot be decided", namespace, claim)
	}

	// Only the group object on the same side: a mirrored claim is driven by
	// the DR direction's object, an original by the other.
	var group []volumeReplication
	for _, candidate := range index.byGroupStorageClass[storageClass] {
		if index.mirrorClaims[candidate.ClaimName] == index.mirrorClaims[claim] {
			group = append(group, candidate)
		}
	}

	switch len(group) {
	case 1:
		return group[0], storageClass, nil
	case 0:
		return volumeReplication{}, "", fmt.Errorf("no VolumeReplication protects PVC %s/%s, and no "+
			"group object covers StorageClass %s. A group object is one whose "+
			"VolumeReplicationClass sets %s: %s and whose dataSource is another PVC of the same "+
			"StorageClass. Either create a VolumeReplication for this claim, or protect the class "+
			"as a group", namespace, claim, storageClass,
			replicationScopeParameter, replicationScopeGroup)
	default:
		return volumeReplication{}, "", fmt.Errorf("%d group VolumeReplications cover StorageClass "+
			"%s (%s): which one drives PVC %s/%s is ambiguous",
			len(group), storageClass, strings.Join(namesOf(group), ", "), namespace, claim)
	}
}

// namesOf renders a set of objects for an error message, in a stable order.
func namesOf(replications []volumeReplication) []string {
	names := make([]string, 0, len(replications))
	for _, vr := range replications {
		names = append(names, vr.Name)
	}
	sort.Strings(names)
	return names
}

// serverOf reads the NFS host out of a PV's volume attributes.
func serverOf(pv *corev1.PersistentVolume) string {
	if pv.Spec.CSI == nil {
		return ""
	}
	return pv.Spec.CSI.VolumeAttributes[volumeAttributeServer]
}

// failoverOptions is what the plan needs beyond the pairs themselves.
type failoverOptions struct {
	namespace   string
	dnsRecord   string
	dnsTarget   string
	dnsTTL      int
	route53Zone string
	restartPods bool

	// The step 0 and step 4 inputs: the pods mounting the affected claims and
	// the workloads that own them. Passed in rather than read here, so the plan
	// stays pure and the quiesce refusals are testable without an API server.
	pods      []corev1.Pod
	workloads []workload
	// podsObserved says a pod list was actually read. An empty list from a `-f`
	// run that held no pods is "not looked at", not "nothing is running".
	podsObserved  bool
	allowBarePods bool
}

// failoverGroup is one StorageClass protected as a unit: one VolumeReplication
// pair, several member volumes.
type failoverGroup struct {
	StorageClass string
	PromoteVR    string
	DemoteVR     string
	// Members is one "<claim> -> <claim>" line per member volume, in the
	// direction the plan runs.
	Members []string
}

// replicationChange is one edit to one VolumeReplication, with the status it
// is then waited for.
type replicationChange struct {
	Namespace  string
	Name       string
	Claim      string
	From       string
	To         string
	AutoResync bool
	WantState  string

	// FromState and FromCompleted are `status.state` and the `Completed`
	// condition **as read when the plan was built**, which is what the summary
	// compares against to say whether this run changed anything. The spec field
	// above is not enough: an object whose spec already says `primary` may still
	// be `Secondary` with the promote unfinished.
	FromState     string
	FromCompleted bool
}

// failoverPlan is what the command prints, and -- with -dry-run=false -- what
// it then carries out, in this order.
type failoverPlan struct {
	Direction direction
	Namespace string
	// Volumes is how many volume pairs the plan moves, which for a group is
	// more than the number of objects edited.
	Volumes int

	// Quiesce is step 0 and step 4: the owners scaled to zero before the
	// promote freezes the source, and scaled back after the TTL.
	Quiesce quiescePlan
	// Promote is step 1: the side being made writable.
	Promote []replicationChange
	// Demote is step 2: the side being frozen and resynced.
	Demote []replicationChange
	// Flips is step 3, for DNS-named classes only.
	Flips []dnsFlip
	// Claims is every claim on either side of an affected pair. A pod mounting
	// any of them is holding a mount that will not follow the failover, which
	// makes it both a writer to stop in step 0 and a pod to bring back in step
	// 4.
	Claims []string

	// Groups names the StorageClasses driven as one unit, with their members,
	// so the operator can see that one edit moves several PVCs.
	Groups []failoverGroup

	// Rebinds names the pairs whose class is IP-named, which `switch` has to
	// move. This command does not move them.
	Rebinds  []string
	Warnings []string

	TTL         int
	ApplyDNS    bool
	RestartPods bool
}

// planFailover turns the derived pairs into the ordered plan.
//
// Pure. Everything that talks to a cluster is behind failoverExecutor, so what
// is printed under the dry-run default and what is applied are the same object
// walked by the same code.
func planFailover(pairs []failoverPair, dir direction, opt failoverOptions) (failoverPlan, error) {
	if len(pairs) == 0 {
		return failoverPlan{}, fmt.Errorf("nothing to %s", dir)
	}
	if dir != directionFailover && dir != directionFailback {
		return failoverPlan{}, fmt.Errorf("unknown direction %q", dir)
	}

	plan := failoverPlan{
		Direction:   dir,
		Namespace:   opt.namespace,
		Volumes:     len(pairs),
		TTL:         opt.dnsTTL,
		ApplyDNS:    opt.route53Zone != "",
		RestartPods: opt.restartPods,
	}

	targets, err := parseDNSTargets(opt.dnsTarget)
	if err != nil {
		return failoverPlan{}, err
	}

	flips := map[string]*dnsFlip{}
	claims := map[string]bool{}
	groups := map[string]*failoverGroup{}
	// A group's members share one VolumeReplication pair, so the *edits* are
	// deduplicated while the claims, the pods and the rebinds stay per volume.
	// Writing `primary` twice to one object is harmless; waiting for it twice,
	// once per member, is minutes of an outage spent on nothing.
	edited := map[string]bool{}
	named, numbered := 0, 0

	for _, pair := range pairs {
		// Failover promotes the mirror; failback promotes the original. That
		// one swap is the whole of the difference between the two directions,
		// which is why they are one code path and two names.
		promoteVR, demoteVR := pair.MirrorVR, pair.SourceVR
		promoteClaim, demoteClaim := pair.MirrorClaim, pair.SourceClaim
		promoteServer, demoteServer := pair.MirrorServer, pair.SourceServer
		promotePV, demotePV := pair.MirrorPV.Name, pair.SourcePV.Name
		if dir == directionFailback {
			promoteVR, demoteVR = pair.SourceVR, pair.MirrorVR
			promoteClaim, demoteClaim = pair.SourceClaim, pair.MirrorClaim
			promoteServer, demoteServer = pair.SourceServer, pair.MirrorServer
			promotePV, demotePV = pair.SourcePV.Name, pair.MirrorPV.Name
		}

		if pair.GroupClass != "" {
			group, ok := groups[pair.GroupClass]
			if !ok {
				group = &failoverGroup{
					StorageClass: pair.GroupClass,
					PromoteVR:    pair.Namespace + "/" + promoteVR.Name,
					DemoteVR:     pair.Namespace + "/" + demoteVR.Name,
				}
				groups[pair.GroupClass] = group
			}
			group.Members = append(group.Members, demoteClaim+" -> "+promoteClaim)
		}

		if edited[pair.Namespace+"/"+promoteVR.Name] {
			// Another member of the same group already carries these two
			// edits. Its claims, pods and rebind are still collected below.
			claims[promoteClaim] = true
			claims[demoteClaim] = true
			if isIPLiteral(pair.SourceServer) {
				numbered++
				plan.Rebinds = append(plan.Rebinds, fmt.Sprintf(
					"%s/%s -> %s/%s (PV %s -> %s)",
					pair.Namespace, demoteClaim, pair.Namespace, promoteClaim,
					demotePV, promotePV))
				continue
			}
			named++
			if err := recordFlip(flips, pair, opt, targets, promoteServer, demoteServer); err != nil {
				return failoverPlan{}, err
			}
			continue
		}
		edited[pair.Namespace+"/"+promoteVR.Name] = true

		plan.Promote = append(plan.Promote, replicationChange{
			Namespace: pair.Namespace, Name: promoteVR.Name, Claim: promoteClaim,
			From: promoteVR.DesiredState, To: replicationStatePrimary,
			AutoResync: promoteVR.AutoResync,
			WantState:  replicationStatusPrimary,
			// What the object reads *now*, so the summary can say whether this
			// run promoted anything or found it already promoted.
			FromState: promoteVR.State, FromCompleted: promoteVR.completed(),
		})
		// autoResync is defaulted true on the side being made secondary, and
		// not offered as a choice. `secondary` without it demotes and stops:
		// the pair is then frozen on one side, promoted on the other, and
		// nothing reprotects it. Reprotect is what the operator meant (runbook
		// phase 5), and it is also what makes the *next* failback possible.
		plan.Demote = append(plan.Demote, replicationChange{
			Namespace: pair.Namespace, Name: demoteVR.Name, Claim: demoteClaim,
			From: demoteVR.DesiredState, To: replicationStateSecondary,
			AutoResync: true,
			WantState:  replicationStatusSecondary,
			FromState:  demoteVR.State, FromCompleted: demoteVR.completed(),
		})

		claims[promoteClaim] = true
		claims[demoteClaim] = true

		// Which topology this pair is in is decided by the *original* class's
		// `server`, because that is the name or address the pods mount
		// through, in both directions.
		if isIPLiteral(pair.SourceServer) {
			numbered++
			plan.Rebinds = append(plan.Rebinds, fmt.Sprintf(
				"%s/%s -> %s/%s (PV %s -> %s)",
				pair.Namespace, demoteClaim, pair.Namespace, promoteClaim,
				demotePV, promotePV))
			continue
		}

		named++
		if err := recordFlip(flips, pair, opt, targets, promoteServer, demoteServer); err != nil {
			return failoverPlan{}, err
		}
	}

	for _, group := range groups {
		sort.Strings(group.Members)
		plan.Groups = append(plan.Groups, *group)
	}
	sort.Slice(plan.Groups, func(i, j int) bool {
		return plan.Groups[i].StorageClass < plan.Groups[j].StorageClass
	})

	for _, flip := range flips {
		plan.Flips = append(plan.Flips, *flip)
	}
	sort.Slice(plan.Flips, func(i, j int) bool { return plan.Flips[i].Record < plan.Flips[j].Record })

	// One bare -dns-target for several records points all of them at one
	// array. On a failback nothing in the objects says which array each
	// record's original class lived on, so this cannot be checked -- only
	// refused, with the per-record spelling as the way through.
	if targets.all != "" && len(plan.Flips) > 1 {
		records := make([]string, 0, len(plan.Flips))
		for _, flip := range plan.Flips {
			records = append(records, flip.Record)
		}
		return failoverPlan{}, fmt.Errorf("-dns-target %s would point %d records at one address (%s), "+
			"and they may belong to different arrays. Name the address for each: "+
			"-dns-target %s=<address>,<record>=<address>", targets.all, len(records),
			strings.Join(records, ", "), records[0])
	}
	for record := range targets.byRecord {
		if _, ok := flips[record]; !ok {
			return failoverPlan{}, fmt.Errorf("-dns-target names %s, which no volume in this %s uses",
				record, dir)
		}
	}

	for claim := range claims {
		plan.Claims = append(plan.Claims, claim)
	}
	sort.Strings(plan.Claims)

	// Step 0 and step 4. Built from the pods and workloads the caller read, and
	// marked "not observed" when there were none to read: an empty quiesce from
	// a `-f` run that carried no pods means nothing was looked at.
	if opt.podsObserved {
		plan.Quiesce = planQuiesce(opt.pods, opt.workloads, plan.Claims, opt.allowBarePods)
	} else {
		plan.Quiesce = quiescePlan{AllowBarePods: opt.allowBarePods}
	}

	for _, flip := range plan.Flips {
		if flip.To == "" {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf(
				"the address to point %s at is not derivable from the objects -- on a %s the "+
					"promoted side is the record itself. Pass -dns-target with the target array's "+
					"address, or flip the record by hand", flip.Record, dir))
		}
	}
	// With a zone id the DNS change is going to be *applied*, so everything about
	// it that can be checked without a network call is checked now and refused
	// now -- before the promote, not after it. A typo in the zone id or the
	// target is how every pod ends up pointed at an arbitrary address.
	if plan.ApplyDNS {
		if err := validateHostedZoneID(opt.route53Zone); err != nil {
			return failoverPlan{}, err
		}
		for _, flip := range plan.Flips {
			if _, err := route53ChangeBatch(flip, plan.TTL); err != nil {
				return failoverPlan{}, err
			}
		}
	}
	if numbered > 0 {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf(
			"%d pair(s) have an IP literal for `server`: spec.csi is immutable, so their pods must "+
				"move to the other side's claim. This command does not move a claim -- run "+
				"`qumulo-csi-repoint switch` for those, and see the list below", numbered))
	}
	if numbered > 0 && named > 0 {
		plan.Warnings = append(plan.Warnings, "this namespace mixes DNS-named and IP-named "+
			"classes: the DNS ones need only the record flip, the IP ones need the rebind. Do both")
	}

	return plan, nil
}

// recordFlip folds one pair's DNS change into the set, because one record
// usually serves every volume in a class and an operator needs the list of
// records, not the list of volumes that share one.
func recordFlip(
	flips map[string]*dnsFlip,
	pair failoverPair,
	opt failoverOptions,
	targets dnsTargets,
	promoteServer, demoteServer string,
) error {
	record := pair.SourceServer
	if opt.dnsRecord != "" {
		record = opt.dnsRecord
	}
	// Where the objects say the record should point: the promoted side's
	// own `server`, when it is not the record itself. On failback the
	// promoted side *is* the record -- the original class's name -- so the
	// objects give nothing, and only the operator knows the original array's
	// address.
	derived := ""
	if promoteServer != record {
		derived = promoteServer
	}
	from := ""
	if demoteServer != record {
		from = demoteServer
	}

	// The conflict check runs on what the objects say, before -dns-target is
	// applied. Applied first, the explicit address replaced every pair's own
	// and the check compared it with itself, so two pairs whose writable
	// copies are on different arrays were planned onto one.
	flip, ok := flips[record]
	if !ok {
		flip = &dnsFlip{Record: record, From: from, derived: derived}
		flips[record] = flip
	} else if flip.derived != derived {
		// Two pairs share the record but their live copies are on different
		// arrays. One A record cannot serve both, so no flip is right, and no
		// -dns-target makes one right.
		return fmt.Errorf("%s serves volumes whose writable copies are on different arrays "+
			"(%s and %s); one DNS record cannot point at both. Give each array its own record, "+
			"or fail these volumes over separately",
			record, orUnknown(flip.derived), orUnknown(derived))
	}
	// -dns-target sets the address -- the objects may give a name where the
	// record needs an IPv4 address, or nothing at all on failback. It never
	// reconciles pairs that disagree; that was refused above.
	flip.To = targets.forRecord(record)
	if flip.To == "" {
		flip.To = derived
	}
	flip.Volumes++
	return nil
}

func orUnknown(address string) string {
	if address == "" {
		return "an address the objects do not give"
	}
	return address
}

// dnsTargets is -dns-target parsed: one address for every record, or an
// address per record.
type dnsTargets struct {
	all      string
	byRecord map[string]string
}

func (t dnsTargets) forRecord(record string) string {
	if address, ok := t.byRecord[record]; ok {
		return address
	}
	return t.all
}

// parseDNSTargets reads -dns-target: either one address, or comma-separated
// record=address pairs. The two forms do not mix, because an address for
// "every other record" is exactly the fold this check prevents.
func parseDNSTargets(value string) (dnsTargets, error) {
	value = strings.TrimSpace(value)
	if value == "" || !strings.Contains(value, "=") {
		return dnsTargets{all: value}, nil
	}
	targets := dnsTargets{byRecord: map[string]string{}}
	for _, entry := range strings.Split(value, ",") {
		record, address, ok := strings.Cut(strings.TrimSpace(entry), "=")
		record, address = strings.TrimSpace(record), strings.TrimSpace(address)
		if !ok || record == "" || address == "" {
			return dnsTargets{}, fmt.Errorf("-dns-target %q: every entry must be record=address "+
				"when any is", value)
		}
		if previous, dup := targets.byRecord[record]; dup && previous != address {
			return dnsTargets{}, fmt.Errorf("-dns-target names %s twice, as %s and %s", record, previous, address)
		}
		targets.byRecord[record] = address
	}
	return targets, nil
}

// String renders the plan the way an operator reads it, and the way a change
// review reads it: comment-prefixed, so it can sit at the head of a file of
// manifests and stay valid YAML.
func (p failoverPlan) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "# %s in namespace %s: %d volume pair(s) through %d VolumeReplication pair(s)\n",
		p.Direction, p.Namespace, p.Volumes, len(p.Promote))
	b.WriteString(singleClusterNote)
	for _, warning := range p.Warnings {
		fmt.Fprintf(&b, "# WARNING: %s\n", warning)
	}

	if len(p.Groups) > 0 {
		b.WriteString("#\n# groups -- one VolumeReplication pair drives every PVC of the class:\n")
		for _, group := range p.Groups {
			fmt.Fprintf(&b, "#   StorageClass %s (%s: %s), %d member(s):\n",
				group.StorageClass, replicationScopeParameter, replicationScopeGroup,
				len(group.Members))
			fmt.Fprintf(&b, "#     promote %s, demote %s\n", group.PromoteVR, group.DemoteVR)
			for _, member := range group.Members {
				fmt.Fprintf(&b, "#     member %s\n", member)
			}
		}
		b.WriteString("#   One edit per side moves all of them. Every member's pods are stopped in\n" +
			"#   step 0 and brought back in step 4, because one promote makes every member\n" +
			"#   writable at once.\n")
	}

	b.WriteString(p.Quiesce.String(p.Namespace))
	if !p.RestartPods {
		b.WriteString("#   -restart-pods=false: nothing is scaled. The command only checks that no pod\n" +
			"#   mounts an affected claim, and refuses to promote if one does.\n")
	}

	b.WriteString("#\n# 1. promote -- set replicationState: primary and wait for Primary/Completed:\n")
	for _, change := range p.Promote {
		fmt.Fprintf(&b, "#   %s/%s (claim %s): %s -> %s\n",
			change.Namespace, change.Name, change.Claim, orNone(change.From), change.To)
	}

	b.WriteString("#\n# 2. demote and reprotect -- replicationState: secondary, autoResync: true:\n")
	for _, change := range p.Demote {
		fmt.Fprintf(&b, "#   %s/%s (claim %s): %s -> %s, autoResync=true\n",
			change.Namespace, change.Name, change.Claim, orNone(change.From), change.To)
	}
	b.WriteString("#   While the old array is unreachable each Demote fails Unavailable and the\n" +
		"#   controller backs off exponentially. If the object has not reached\n" +
		"#   Secondary/Completed within the wait, a spec field is toggled once to force an\n" +
		"#   immediate reconcile -- but never while the object reports Resyncing or a Degraded\n" +
		"#   reason of VolumeDegraded, and never while it is still moving: those are\n" +
		"#   waited out first. Keep the old side's pods away from that array until it reads\n" +
		"#   frozen.\n")

	if len(p.Flips) > 0 {
		b.WriteString("#\n# 3. DNS records to flip, then wait one TTL:\n")
		for _, flip := range p.Flips {
			target := flip.To
			if target == "" {
				target = "<the array now holding the writable copy -- pass -dns-target>"
			}
			if flip.From != "" {
				fmt.Fprintf(&b, "#   %s: %s -> %s   TTL %ds   (%d volume(s))\n",
					flip.Record, flip.From, target, p.TTL, flip.Volumes)
			} else {
				fmt.Fprintf(&b, "#   %s -> %s   TTL %ds   (%d volume(s))\n",
					flip.Record, target, p.TTL, flip.Volumes)
			}
		}
		if p.ApplyDNS {
			b.WriteString("#   A hosted zone id was given, so this is applied as a Route53 UPSERT.\n")
		} else {
			b.WriteString("#   No -route53-zone-id was given, so nothing touches DNS: flip the record\n" +
				"#   above with whatever runs your zone, then wait one TTL.\n")
		}
	}

	if len(p.Rebinds) > 0 {
		b.WriteString("#\n# 3b. claims `switch` must rebind (IP-named classes, not done here):\n")
		for _, rebind := range p.Rebinds {
			fmt.Fprintf(&b, "#   %s\n", rebind)
		}
	}

	if p.RestartPods {
		b.WriteString(p.Quiesce.restoreString())
	} else {
		b.WriteString("#\n# 4. restore -- nothing to restore: -restart-pods=false scaled nothing down.\n" +
			"#   Bring the workloads back yourself, after the TTL. A live NFS mount never\n" +
			"#   follows a failover.\n")
	}
	b.WriteString("#   claims on either side of an affected pair:\n")
	for _, claim := range p.Claims {
		fmt.Fprintf(&b, "#     %s/%s\n", p.Namespace, claim)
	}

	return b.String()
}

// singleClusterNote is printed at the head of every plan and repeated in
// `-h`: this command works within **one namespace of one cluster**.
//
// The join it makes needs both PVs, both PVCs and both VolumeReplications
// visible at once, because a VolumeReplication names its claim by name alone
// (topology B, which is the lab's shape and the one every measurement was made
// in). The spec recommends two clusters, and on two clusters this command
// answers "no mirrored volume pairs found" -- so say so here rather than let it
// be discovered at 3 a.m.
const singleClusterNote = `#
# SINGLE-CLUSTER ONLY: both sides of every pair must be in this one namespace of
# this one cluster (topology B). The join needs both PVs, both PVCs and both
# VolumeReplications at once, and a VolumeReplication names its claim by name
# alone. On two clusters this command finds no pairs at all.
#   Two clusters would differ in four ways, none of them implemented here:
#     - two kubeconfigs, and a -promote-context/-demote-context to say which is
#       which, since neither cluster can see the other's objects;
#     - the promote and the demote would run against different API servers, so
#       the ordering between them would be this command's to hold rather than
#       one cluster's;
#     - the mirrored PVC would keep the *same* name in the same namespace on the
#       DR cluster (no -claim-suffix), so the pairing would be by name across
#       clusters rather than by the mirrored-from annotation within one;
#     - step 0 and step 4 would quiesce on one cluster and start on the other,
#       which is two pod lists and two sets of owners, not one.
`

func orNone(s string) string {
	if s == "" {
		return "<unset>"
	}
	return s
}

// failoverExecutor is everything the plan needs a cluster (or a DNS zone) for.
//
// An interface for one reason: the *order* is the product here, and the order
// is testable against a recorder without a cluster, a CRD or an API server.
type failoverExecutor interface {
	// SetReplicationState writes spec.replicationState (and autoResync).
	SetReplicationState(ctx context.Context, change replicationChange) error
	// AwaitReplication polls until the object reaches wantState with
	// Completed=True, or the bound expires. The last observation is returned
	// either way, so a timeout can say *why* rather than only that.
	AwaitReplication(ctx context.Context, namespace, name, wantState string) (volumeReplication, error)
	// AwaitSettled waits out a transient Resyncing or VolumeDegraded before a
	// spec toggle is even considered, and reports whether the object changed at
	// all while it waited.
	AwaitSettled(ctx context.Context, namespace, name string) (volumeReplication, bool, error)
	// NudgeReplication forces an immediate reconcile by changing a spec field
	// and changing it back.
	NudgeReplication(ctx context.Context, namespace, name string) error
	// PreflightDNS proves the zone exists, that the record is under it and that
	// the change batch is well formed -- before anything at all is changed.
	PreflightDNS(ctx context.Context, flips []dnsFlip, ttl int) error
	// FlipRecord applies the DNS change.
	FlipRecord(ctx context.Context, flip dnsFlip, ttl int) error
	// Settle waits out one TTL after a record change.
	Settle(ctx context.Context, ttl int) error
	// QuiesceOwner records the owner's replica count on the owner and scales it
	// to zero (step 0).
	QuiesceOwner(ctx context.Context, owner quiesceOwner) error
	// RestoreOwner scales the owner back to what step 0 recorded and clears the
	// annotation (step 4).
	RestoreOwner(ctx context.Context, owner quiesceOwner) error
	// DeletePod deletes one pod. Only ever a bare pod, only ever with
	// -allow-bare-pods.
	DeletePod(ctx context.Context, namespace, name string) error
	// AwaitNoPodsMounting waits until no pod in the namespace mounts any of the
	// claims, and returns the ones still there when it gave up.
	AwaitNoPodsMounting(ctx context.Context, namespace string, claims []string) ([]string, error)
}

// executeFailover carries the plan out in the one order that is safe.
//
// **Quiesce first.** A promote freezes the source (the driver's drain), and a
// frozen Qumulo source silently drops every append the NFS client
// has already acknowledged. Until this step existed the pods were
// stopped in step 4, minutes after the freeze, so a planned failover of a
// healthy pair threw away whatever production wrote in between with nothing in
// any log to say so. Nothing may be promoted while a pod mounts an affected
// claim.
//
// Then: promote, because the DR copy has to be writable before anything is
// pointed at it; demote, because the old array must be frozen before its pods
// can come back (an array that returns from an outage comes back
// *writable*); DNS, because a record pointing at a read-only replica is worse
// than one pointing at nothing; and the workloads last, one TTL after the
// record moved, because a live NFS mount follows none of the above.
//
// Only the quiesce and the promote are fatal. A demote that has not landed is
// the measured, expected state while the old array is still down, and
// stopping there would leave the promoted side unused and the pods down.
func executeFailover(ctx context.Context, plan failoverPlan, ex failoverExecutor, out io.Writer) error {
	t := newTimer(plan.Direction, plan.Namespace)
	err := executePhases(ctx, plan, ex, out, t)
	// The summary is printed whatever happened: a run that stopped half way is
	// exactly the one whose phases and transitions somebody has to read.
	t.finish(out)
	return err
}

func executePhases(
	ctx context.Context,
	plan failoverPlan,
	ex failoverExecutor,
	out io.Writer,
	t *timer,
) error {
	// Before anything is changed at all: prove the DNS zone and record are what
	// the operator thinks they are. A wrong zone id or a record that is not in
	// the zone would otherwise be found in step 3, with production already
	// frozen and promoted.
	if plan.ApplyDNS {
		if err := ex.PreflightDNS(ctx, plan.Flips, plan.TTL); err != nil {
			return fmt.Errorf("DNS preflight (nothing was changed): %w", err)
		}
	}

	if err := executeQuiesce(ctx, plan, ex, out, t); err != nil {
		return err
	}
	if err := executePromote(ctx, plan, ex, out, t); err != nil {
		return err
	}
	executeDemote(ctx, plan, ex, out, t)
	if err := executeDNS(ctx, plan, ex, out, t); err != nil {
		return err
	}
	return executeRestore(ctx, plan, ex, out, t)
}

// executeQuiesce is step 0: stop every writer, and prove it stopped.
func executeQuiesce(
	ctx context.Context,
	plan failoverPlan,
	ex failoverExecutor,
	out io.Writer,
	t *timer,
) error {
	t.startPhase()
	defer t.endPhase(0, "quiesce")

	if !plan.Quiesce.Observed {
		return fmt.Errorf("refusing to promote: the pods in namespace %s were never read, so whether "+
			"anything is still writing to the source is unknown. A promote freezes the source and "+
			"every append a writer has acknowledged since is dropped silently. Re-run "+
			"without -f, so the pods and their owners are read from the cluster", plan.Namespace)
	}

	if !plan.RestartPods {
		if !plan.Quiesce.empty() {
			return fmt.Errorf("refusing to promote: -restart-pods=false, and pod(s) %s still mount an "+
				"affected claim. A promote freezes the source and every append they have "+
				"acknowledged since is dropped silently. Stop them yourself, or drop "+
				"-restart-pods=false and let this command scale their owners to zero",
				strings.Join(plan.Quiesce.Pods, ", "))
		}
		writef(out, "# 0/4 -restart-pods=false, and no pod mounts an affected claim: nothing to quiesce\n")
		return nil
	}

	if err := plan.Quiesce.refusal(); err != nil {
		return err
	}
	if plan.Quiesce.empty() {
		writef(out, "# 0/4 no pod mounts an affected claim: nothing to quiesce\n")
		return nil
	}

	for _, owner := range plan.Quiesce.Owners {
		started := time.Now()
		writef(out, "# 0/4 %s: %d -> 0 replicas (restored to %d in step 4)\n",
			owner.object(), owner.Replicas, owner.Restore)
		if err := ex.QuiesceOwner(ctx, owner); err != nil {
			return fmt.Errorf("scaling %s to 0 replicas: %w", owner.object(), err)
		}
		t.record(objectTransition{
			Step:       "quiesce",
			Object:     owner.object(),
			Transition: fmt.Sprintf("%d -> 0 replicas", owner.Replicas),
			Changed:    owner.Replicas != 0,
			Seconds:    round(time.Since(started)),
		})
	}

	for _, pod := range plan.Quiesce.BarePods {
		started := time.Now()
		namespace, name := splitNamespaced(pod, plan.Namespace)
		if err := ex.DeletePod(ctx, namespace, name); err != nil {
			return fmt.Errorf("deleting bare pod %s: %w", pod, err)
		}
		writef(out, "# 0/4 WARNING: deleted bare pod %s (-allow-bare-pods). **Nothing recreates it.** "+
			"Re-apply its manifest yourself once the failover is done\n", pod)
		t.record(objectTransition{
			Step: "quiesce", Object: pod, Transition: "bare pod deleted, not recreated",
			Changed: true, Seconds: round(time.Since(started)),
		})
	}

	writef(out, "# 0/4 waiting until no pod in %s mounts an affected claim "+
		"(`kubectl delete pod` is not a quiesce)\n", plan.Namespace)
	started := time.Now()
	remaining, err := ex.AwaitNoPodsMounting(ctx, plan.Namespace, plan.Claims)
	switch {
	case err != nil:
		return fmt.Errorf("refusing to promote: waiting for the pods in %s to go away failed, so "+
			"whether anything is still writing to the source is unknown: %w",
			plan.Namespace, err)
	case len(remaining) > 0:
		return fmt.Errorf("refusing to promote: pod(s) %s still mount an affected claim after the "+
			"wait. A promote freezes the source and every append they have acknowledged since is "+
			"dropped silently", strings.Join(remaining, ", "))
	}
	writef(out, "# 0/4 quiesced: nothing mounts an affected claim (%.1fs)\n", round(time.Since(started)))
	return nil
}

// executePromote is step 1, and the one step whose failure stops everything:
// nothing downstream may point at a copy that is not writable.
func executePromote(
	ctx context.Context,
	plan failoverPlan,
	ex failoverExecutor,
	out io.Writer,
	t *timer,
) error {
	t.startPhase()
	defer t.endPhase(1, "promote")

	// promoted is what this run has already promoted, for the error when a
	// later promote fails. Those are deliberately not undone: a demote issued
	// by a tool that has just watched the controller fail is a second change
	// nobody reviewed, on the one step that decides which copy is writable. The
	// operator is told instead, and a re-run finds them already primary.
	var promoted []string
	stopped := func(err error) error {
		if len(promoted) == 0 {
			return err
		}
		writef(out, "# 1/4 STOPPED: %d already promoted by this run and left promoted (not undone): %s\n",
			len(promoted), strings.Join(promoted, ", "))
		return fmt.Errorf("%w; already promoted by this run and left promoted, not undone: %s. "+
			"Those copies are now writable; fix the failed one and re-run, which finds them "+
			"already primary", err, strings.Join(promoted, ", "))
	}

	for _, change := range plan.Promote {
		started := time.Now()
		object := change.Namespace + "/" + change.Name
		writef(out, "# 1/4 %s: replicationState=%s\n", object, change.To)
		if err := ex.SetReplicationState(ctx, change); err != nil {
			return stopped(fmt.Errorf("promoting VolumeReplication %s: %w", object, err))
		}
		observed, err := ex.AwaitReplication(ctx, change.Namespace, change.Name, change.WantState)
		transition, changed := transitionOf(change.FromState, change.FromCompleted, change.WantState,
			err == nil)
		t.record(objectTransition{
			Step: "promote", Object: object, Transition: transition, Changed: changed,
			Seconds: round(time.Since(started)), Detail: observed.describe(),
		})
		if err != nil {
			return stopped(fmt.Errorf("VolumeReplication %s did not reach %s/Completed (%s): %w",
				object, change.WantState, observed.describe(), err))
		}
		promoted = append(promoted, object)
		writef(out, "#      %s (%s)\n", transition, observed.describe())
	}
	return nil
}

// executeDemote is step 2. It never stops the run: the old array being
// unreachable is the measured, expected case.
func executeDemote(
	ctx context.Context,
	plan failoverPlan,
	ex failoverExecutor,
	out io.Writer,
	t *timer,
) {
	t.startPhase()
	defer t.endPhase(2, "demote")

	for _, change := range plan.Demote {
		started := time.Now()
		object := change.Namespace + "/" + change.Name
		writef(out, "# 2/4 %s: replicationState=%s, autoResync=true\n", object, change.To)
		if err := ex.SetReplicationState(ctx, change); err != nil {
			writef(out, "# WARNING: demoting %s failed: %v. The old copy is writable until it is "+
				"frozen; keep pods away from it\n", object, err)
			t.record(objectTransition{
				Step: "demote", Object: object, Changed: true,
				Transition: lowerOrUnknown(change.FromState) + " -> " +
					strings.ToLower(change.WantState) + " DID NOT COMPLETE",
				Seconds: round(time.Since(started)), Detail: err.Error(),
			})
			continue
		}

		observed, err := ex.AwaitReplication(ctx, change.Namespace, change.Name, change.WantState)
		if err != nil || !observed.healthySecondary() {
			observed, err = settleThenNudge(ctx, ex, change, observed, out)
		}

		reached := err == nil && observed.healthySecondary()
		transition, changed := transitionOf(change.FromState, change.FromCompleted, change.WantState,
			reached)
		t.record(objectTransition{
			Step: "demote", Object: object, Transition: transition, Changed: changed,
			Seconds: round(time.Since(started)), Detail: observed.describe(),
		})

		switch {
		case reached:
			writef(out, "#      %s (%s)\n", transition, observed.describe())
		case observed.reached(replicationStatusSecondary):
			writef(out, "# WARNING: %s is Secondary but degraded (%s). Reprotection is not "+
				"complete; do not fail back until it is\n", object, observed.describe())
		default:
			writef(out, "# WARNING: %s has not reached %s (%s). The controller's Demote is "+
				"still failing or backing off; the old array is writable until it lands, so keep "+
				"pods away from it and re-run this command when the array answers\n",
				object, change.WantState, observed.describe())
		}
	}
}

// settleThenNudge is the backoff remedy, gated on the controller having
// settled.
//
// While the old array is down each Demote fails `Unavailable` and the
// csi-addons controller backs off exponentially -- 98 s of a writable old copy
// in the measured case -- and **any** spec change forces an immediate
// reconcile. So the tool toggles a spec field once rather than telling a human
// to.
//
// The gate is the other half: a controller that is *already* reconciling must not
// be toggled. After the demote the resync holds `Resyncing: True` and a
// `Degraded` reason of `VolumeDegraded` for a while, and the tool used to fire
// its toggle at every object before they settled. The toggle is a remedy for a
// controller that has stopped working; firing it at one that is working
// re-enqueues work already in flight and hides the signal. So: wait those out,
// and toggle only an object that has sat still for the whole wait.
func settleThenNudge(
	ctx context.Context,
	ex failoverExecutor,
	change replicationChange,
	observed volumeReplication,
	out io.Writer,
) (volumeReplication, error) {
	object := change.Namespace + "/" + change.Name
	if observed.reconciling() {
		writef(out, "#      %s is still reconciling (%s); waiting for that to clear before any "+
			"spec toggle\n", object, observed.describe())
	}

	settled, progressed, err := ex.AwaitSettled(ctx, change.Namespace, change.Name)
	if err != nil {
		return settled, err
	}

	switch {
	case settled.reached(change.WantState) && !settled.degraded():
		writef(out, "#      reached %s while waiting; no spec toggle was needed\n", change.WantState)
		return settled, nil
	case settled.reconciling():
		writef(out, "# WARNING: %s is still %s after the wait (%s). NOT toggling a spec field: the "+
			"controller is reconciling, and a toggle would only re-enqueue work already in flight"+
			". Re-run when the resync has finished\n",
			object, conditionResyncing+"/"+reasonVolumeDegraded, settled.describe())
		return settled, fmt.Errorf("still reconciling after the settle wait")
	case progressed:
		writef(out, "#      %s moved while waiting (%s); re-reading rather than toggling a spec "+
			"field, because the controller is making progress\n", object, settled.describe())
		reread, rerr := ex.AwaitReplication(ctx, change.Namespace, change.Name, change.WantState)
		if rerr == nil && reread.healthySecondary() {
			return reread, nil
		}
		observed = reread
	}

	writef(out, "#      not %s yet (%s) and it has not moved for the whole wait; toggling a spec "+
		"field once to force an immediate reconcile\n", change.WantState, observed.describe())
	if err := ex.NudgeReplication(ctx, change.Namespace, change.Name); err != nil {
		return observed, fmt.Errorf("forcing a reconcile on VolumeReplication %s: %w", object, err)
	}
	return ex.AwaitReplication(ctx, change.Namespace, change.Name, change.WantState)
}

// executeDNS is step 3: the record, then one TTL.
func executeDNS(
	ctx context.Context,
	plan failoverPlan,
	ex failoverExecutor,
	out io.Writer,
	t *timer,
) error {
	t.startPhase()
	defer t.endPhase(3, "dns")

	for _, flip := range plan.Flips {
		if !plan.ApplyDNS {
			writef(out, "# 3/4 DNS: point %s at %s yourself, then wait one TTL (%ds). "+
				"No -route53-zone-id was given, so nothing was changed\n",
				flip.Record, orUnknownTarget(flip.To), plan.TTL)
			continue
		}
		started := time.Now()
		writef(out, "# 3/4 DNS: %s -> %s (TTL %ds)\n", flip.Record, flip.To, plan.TTL)
		if err := ex.FlipRecord(ctx, flip, plan.TTL); err != nil {
			return fmt.Errorf("flipping %s: %w", flip.Record, err)
		}
		if err := ex.Settle(ctx, plan.TTL); err != nil {
			return fmt.Errorf("waiting one TTL after flipping %s: %w", flip.Record, err)
		}
		t.record(objectTransition{
			Step: "dns", Object: flip.Record, Transition: orNone(flip.From) + " -> " + flip.To,
			Changed: true, Seconds: round(time.Since(started)),
		})
	}
	return nil
}

// executeRestore is step 4: the workloads come back, on the far side of the TTL.
func executeRestore(
	ctx context.Context,
	plan failoverPlan,
	ex failoverExecutor,
	out io.Writer,
	t *timer,
) error {
	t.startPhase()
	defer t.endPhase(4, "restore")

	if !plan.RestartPods {
		writef(out, "# 4/4 -restart-pods=false: nothing was scaled down, so nothing is brought back. "+
			"A live NFS mount does not follow a failover; restart the workloads yourself\n")
		return nil
	}

	var failed []string
	for _, owner := range plan.Quiesce.Owners {
		started := time.Now()
		writef(out, "# 4/4 %s: 0 -> %d replicas\n", owner.object(), owner.Restore)
		if err := ex.RestoreOwner(ctx, owner); err != nil {
			// Not a return: leaving the other workloads down because one owner
			// failed to scale is worse than reporting all of them at the end.
			writef(out, "# WARNING: scaling %s back to %d failed: %v\n", owner.object(), owner.Restore, err)
			failed = append(failed, fmt.Sprintf("%s (%v)", owner.object(), err))
			continue
		}
		t.record(objectTransition{
			Step:       "restore",
			Object:     owner.object(),
			Transition: fmt.Sprintf("0 -> %d replicas", owner.Restore),
			Changed:    owner.Restore != 0,
			Seconds:    round(time.Since(started)),
		})
	}
	for _, pod := range plan.Quiesce.BarePods {
		writef(out, "# 4/4 WARNING: bare pod %s was deleted in step 0 and is NOT recreated. "+
			"Re-apply its manifest yourself\n", pod)
	}
	if len(plan.Quiesce.Owners) == 0 && len(plan.Quiesce.BarePods) == 0 {
		writef(out, "# 4/4 nothing was scaled down, so nothing is brought back\n")
	}

	if len(failed) > 0 {
		return fmt.Errorf("the failover completed but %d workload(s) are still scaled to 0 and must "+
			"be scaled back by hand: %s. Their original replica counts are in the %s annotation",
			len(failed), strings.Join(failed, ", "), annotationQuiescedReplicas)
	}
	return nil
}

func orUnknownTarget(target string) string {
	if target == "" {
		return "the array now holding the writable copy"
	}
	return target
}

// splitNamespaced splits "namespace/name", falling back to the plan's own
// namespace for a bare name.
func splitNamespaced(qualified, fallback string) (string, string) {
	if namespace, name, ok := strings.Cut(qualified, "/"); ok {
		return namespace, name
	}
	return fallback, qualified
}

// podsMountingClaims returns the pods that mount any of the named claims.
//
// Pure, over a pod list the caller read. Same idea as podsUsingClaims in
// switchcmd.go, but within one namespace and returning the pods themselves,
// because this command deletes them rather than printing them.
func podsMountingClaims(pods []corev1.Pod, claims []string) []corev1.Pod {
	wanted := map[string]bool{}
	for _, claim := range claims {
		wanted[claim] = true
	}

	var out []corev1.Pod
	for i := range pods {
		pod := pods[i]
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim == nil {
				continue
			}
			if wanted[volume.PersistentVolumeClaim.ClaimName] {
				out = append(out, pod)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// awaitReplication is the bounded poll both directions share.
//
// Bounded, always. A failover that blocks forever on a CR that will never move
// is worse than one that reports what it saw: the operator's next decision
// depends on knowing which of the four steps did not land.
func awaitReplication(
	ctx context.Context,
	get func(context.Context) (volumeReplication, error),
	wantState string,
	poll, bound time.Duration,
) (volumeReplication, error) {
	deadline := time.Now().Add(bound)
	var last volumeReplication

	for {
		observed, err := get(ctx)
		if err != nil {
			return last, err
		}
		last = observed
		if observed.reached(wantState) {
			return observed, nil
		}
		if !time.Now().Add(poll).Before(deadline) {
			return last, fmt.Errorf("gave up after %s", bound)
		}

		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(poll):
		}
	}
}
