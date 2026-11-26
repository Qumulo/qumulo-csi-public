// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Step 0: quiesce. The first step of a planned failover, which this command used to skip.
//
// **A planned failover must never freeze a source that pods are still writing
// to.** A frozen Qumulo source rejects *creates* with
// `EROFS`, but an append to a file that already exists returns success to the
// application and is then silently dropped. The promote in step 1 is what
// freezes the source (the driver's drain), so every acknowledged append between
// that freeze and the pod restart is lost, with nothing in any log to say so.
// Before this file existed the pods were restarted in step 4, minutes later.
//
// "Delete the pod" is not a quiesce either: a busybox `sh` loop as
// PID 1 ignores SIGTERM and keeps writing until the grace-period kill. What
// makes a workload stop is scaling its owner to zero and **waiting until the
// pods are gone**, which is what this plans and what step 0 then does.
//
// A bare pod has no owner to scale, and deleting one destroys the only copy of
// its definition -- a DR run that loses a workload while recovering its data has
// recovered nothing. So a bare pod stops the command by default and is named;
// `-allow-bare-pods` deletes it and says loudly that nothing will bring it back.

// annotationQuiescedReplicas remembers what an owner was scaled to before step
// 0 scaled it to zero.
//
// On the owner rather than in the command's memory because the restore has to
// survive three things: a run that is interrupted between the quiesce and the
// restore, a *re-run* of the same direction (which must not record the zero it
// just wrote as the count to come back to), and the failback, which is a
// separate invocation and often a separate day.
const annotationQuiescedReplicas = "csi.qumulo.com/repoint-quiesced-replicas"

// The workload kinds this command can scale. A Deployment or a StatefulSet is
// scaled directly; a ReplicaSet is scaled only when nothing owns it, because a
// ReplicaSet under a Deployment is scaled straight back by the Deployment
// controller within a reconcile.
const (
	kindDeployment  = "Deployment"
	kindStatefulSet = "StatefulSet"
	kindReplicaSet  = "ReplicaSet"
)

// workload is a scalable owner reduced to the five things the quiesce reads.
//
// A reduced struct rather than the apps/v1 objects, for the same reason the
// VolumeReplication is reduced: the plan is pure over it, so the ordering and
// the refusals are testable without an API server.
type workload struct {
	Kind      string
	Namespace string
	Name      string
	UID       string
	// Replicas is `spec.replicas` as read now.
	Replicas int32
	// Remembered is the annotation this command wrote on a previous run, and
	// the empty string when there is none.
	Remembered string
	// ControllerKind and ControllerName are this object's own controller, which
	// for a ReplicaSet is its Deployment.
	ControllerKind string
	ControllerName string
}

// id is the key a pod's ownerReference resolves against.
func (w workload) id() string { return w.Kind + "/" + w.Namespace + "/" + w.Name }

// workloadsFromAPI reduces the three apps/v1 kinds to the struct above.
func workloadsFromAPI(
	deployments []appsv1.Deployment,
	statefulSets []appsv1.StatefulSet,
	replicaSets []appsv1.ReplicaSet,
) []workload {
	out := make([]workload, 0, len(deployments)+len(statefulSets)+len(replicaSets))
	for i := range deployments {
		out = append(out, workloadFrom(kindDeployment, &deployments[i].ObjectMeta, deployments[i].Spec.Replicas))
	}
	for i := range statefulSets {
		out = append(out, workloadFrom(kindStatefulSet, &statefulSets[i].ObjectMeta, statefulSets[i].Spec.Replicas))
	}
	for i := range replicaSets {
		out = append(out, workloadFrom(kindReplicaSet, &replicaSets[i].ObjectMeta, replicaSets[i].Spec.Replicas))
	}
	return out
}

// workloadFrom reduces one object's metadata and replica count.
func workloadFrom(kind string, meta *metav1.ObjectMeta, replicas *int32) workload {
	w := workload{
		Kind:       kind,
		Namespace:  meta.Namespace,
		Name:       meta.Name,
		UID:        string(meta.UID),
		Remembered: meta.Annotations[annotationQuiescedReplicas],
	}
	// An unset `spec.replicas` means one, which is the API's own default and
	// the count the restore has to put back.
	w.Replicas = 1
	if replicas != nil {
		w.Replicas = *replicas
	}
	if owner := metav1.GetControllerOfNoCopy(meta); owner != nil {
		w.ControllerKind = owner.Kind
		w.ControllerName = owner.Name
	}
	return w
}

// quiesceOwner is one workload the plan scales to zero and back.
type quiesceOwner struct {
	Kind      string
	Namespace string
	Name      string
	// Replicas is what the owner reads now: zero on a re-run.
	Replicas int32
	// Restore is what step 4 scales it back to. The remembered count when a
	// previous run already recorded one, otherwise what it reads now.
	Restore int32
	// Remembered says the annotation was already present, which is what makes a
	// re-run of step 0 leave it alone instead of recording the zero it wrote.
	Remembered bool
	// Pods are the pods of this owner that mount an affected claim, named so the
	// plan says why the owner is in it.
	Pods []string
	// LeftAtZero says no pod of this owner mounts an affected claim now, and the
	// owner is in the plan only because an earlier run's annotation says it
	// scaled it down and never brought it back (the run was interrupted
	// after step 0). Step 0 has nothing to do for it; step 4 restores it.
	LeftAtZero bool
}

// object is the owner as the plan and the summary name it.
func (o quiesceOwner) object() string { return o.Namespace + "/" + o.Kind + "/" + o.Name }

// quiescePlan is step 0 and step 4: who is scaled down, who is deleted, and who
// stops the command.
type quiescePlan struct {
	// Observed says a pod list was actually read. Without one -- a plan built
	// from `-f` manifests that hold no pods -- an empty quiesce means "not
	// looked at", not "nothing is running", and applying refuses on it.
	Observed bool
	// Owners are the Deployments, StatefulSets and unowned ReplicaSets scaled to
	// zero before the promote and back after the TTL wait.
	Owners []quiesceOwner
	// BarePods are pods with no controller. They stop the command unless
	// -allow-bare-pods was passed, in which case they are deleted and will not
	// come back on their own.
	BarePods []string
	// AllowBarePods carries the flag into the plan so what is printed and what
	// is applied cannot disagree.
	AllowBarePods bool
	// Unscalable are pods whose controller is a kind this command will not
	// scale (a DaemonSet, a Job, a CronJob, an operator's own CR). They always
	// stop the command: guessing at how to stop one is how a DR run takes a
	// cluster service down with it.
	Unscalable []string
	// Pods is every pod found mounting an affected claim, in name order.
	Pods []string
}

// empty reports whether nothing mounts an affected claim.
func (q quiescePlan) empty() bool {
	return len(q.Owners) == 0 && len(q.BarePods) == 0 && len(q.Unscalable) == 0
}

// refusal is the reason step 0 must stop, or nil.
//
// Returned rather than raised at plan time so the dry run can print the whole
// plan -- including the steps after the one that would stop -- and the operator
// can see what the flag would buy them before passing it.
func (q quiescePlan) refusal() error {
	if len(q.Unscalable) > 0 {
		return fmt.Errorf("refusing to promote: %s mount an affected claim and their controller is a "+
			"kind this command will not scale. Stop them yourself, then re-run: a promote freezes the "+
			"source and every append those pods have acknowledged since is dropped silently", strings.Join(q.Unscalable, ", "))
	}
	if len(q.BarePods) > 0 && !q.AllowBarePods {
		return fmt.Errorf("refusing to promote: bare pod(s) %s mount an affected claim and have no "+
			"controller to scale. A promote freezes the source and every append they have "+
			"acknowledged since is dropped silently. Delete them yourself, or "+
			"pass -allow-bare-pods to have this command delete them -- nothing will recreate them",
			strings.Join(q.BarePods, ", "))
	}
	return nil
}

// planQuiesce works out what has to stop before the promote, and what step 4
// has to bring back.
//
// Pure over a pod list and a workload list the caller read, like every other
// plan in this command: the refusals are the product here, and a refusal that
// needs an API server to test is a refusal nobody tests.
func planQuiesce(
	pods []corev1.Pod,
	workloads []workload,
	claims []string,
	allowBarePods bool,
) quiescePlan {
	plan := quiescePlan{Observed: true, AllowBarePods: allowBarePods}

	byID := map[string]workload{}
	for _, w := range workloads {
		byID[w.id()] = w
	}

	owners := map[string]*quiesceOwner{}
	for _, pod := range podsMountingClaims(pods, claims) {
		name := pod.Namespace + "/" + pod.Name
		plan.Pods = append(plan.Pods, name)

		controller := metav1.GetControllerOf(&pod)
		if controller == nil {
			plan.BarePods = append(plan.BarePods, name)
			continue
		}

		owner, err := resolveScalableOwner(pod.Namespace, controller, byID)
		if err != nil {
			plan.Unscalable = append(plan.Unscalable, fmt.Sprintf("%s (%s)", name, err))
			continue
		}

		entry, ok := owners[owner.id()]
		if !ok {
			restore, remembered := restoreCount(owner)
			entry = &quiesceOwner{
				Kind:       owner.Kind,
				Namespace:  owner.Namespace,
				Name:       owner.Name,
				Replicas:   owner.Replicas,
				Restore:    restore,
				Remembered: remembered,
			}
			owners[owner.id()] = entry
		}
		entry.Pods = append(entry.Pods, name)
	}

	// Measured on the lab cluster: a run interrupted after
	// step 0 leaves every owner at zero with the annotation on it and no pod
	// to find it by. The next run found nothing to quiesce and step 4 had
	// nothing to restore; the workloads stayed down. So every workload that
	// still carries the annotation is an owner of this plan too, restored in
	// step 4 whether or not this run scaled it.
	for _, w := range workloads {
		if w.Remembered == "" {
			continue
		}
		if w.Kind == kindReplicaSet && w.ControllerKind != "" {
			continue // the Deployment carries the annotation, not its ReplicaSet
		}
		if _, ok := owners[w.id()]; ok {
			continue
		}
		restore, remembered := restoreCount(w)
		if !remembered {
			continue // unparseable: not this command's annotation
		}
		owners[w.id()] = &quiesceOwner{
			Kind: w.Kind, Namespace: w.Namespace, Name: w.Name,
			Replicas: w.Replicas, Restore: restore, Remembered: true, LeftAtZero: true,
		}
	}

	for _, owner := range owners {
		sort.Strings(owner.Pods)
		plan.Owners = append(plan.Owners, *owner)
	}
	sort.Slice(plan.Owners, func(i, j int) bool {
		return plan.Owners[i].object() < plan.Owners[j].object()
	})
	sort.Strings(plan.BarePods)
	sort.Strings(plan.Unscalable)
	sort.Strings(plan.Pods)
	return plan
}

// resolveScalableOwner walks a pod's controller up to the object that decides
// how many pods there are.
//
// One hop, and only one: a pod's controller is a ReplicaSet, and a ReplicaSet's
// controller is the Deployment. Scaling the ReplicaSet under a live Deployment
// would be undone by the Deployment controller on its next reconcile, so the
// pods would come straight back -- while this command believed it had quiesced
// them and went on to freeze the source.
func resolveScalableOwner(
	namespace string,
	controller *metav1.OwnerReference,
	byID map[string]workload,
) (workload, error) {
	switch controller.Kind {
	case kindDeployment, kindStatefulSet, kindReplicaSet:
	default:
		return workload{}, fmt.Errorf("owned by %s %s", controller.Kind, controller.Name)
	}

	owner, ok := byID[controller.Kind+"/"+namespace+"/"+controller.Name]
	if !ok {
		return workload{}, fmt.Errorf("owned by %s %s, which is not readable here", controller.Kind,
			controller.Name)
	}

	if owner.Kind == kindReplicaSet && owner.ControllerKind != "" {
		if owner.ControllerKind != kindDeployment {
			return workload{}, fmt.Errorf("owned by ReplicaSet %s, which is owned by %s %s",
				owner.Name, owner.ControllerKind, owner.ControllerName)
		}
		deployment, ok := byID[kindDeployment+"/"+namespace+"/"+owner.ControllerName]
		if !ok {
			return workload{}, fmt.Errorf("owned by ReplicaSet %s, whose Deployment %s is not "+
				"readable here", owner.Name, owner.ControllerName)
		}
		return deployment, nil
	}
	return owner, nil
}

// restoreCount decides what step 4 scales an owner back to.
//
// The annotation wins whenever it is there and parses, because the count read
// now is zero on a re-run and on a run that was interrupted between step 0 and
// step 4. An annotation that does not parse is ignored rather than trusted:
// somebody edited it, and the live count is the better guess.
func restoreCount(owner workload) (int32, bool) {
	if owner.Remembered != "" {
		if parsed, err := strconv.ParseInt(owner.Remembered, 10, 32); err == nil && parsed >= 0 {
			return int32(parsed), true
		}
	}
	return owner.Replicas, false
}

// String renders step 0 and step 4 the way the rest of the plan reads.
func (q quiescePlan) String(namespace string) string {
	var b strings.Builder

	b.WriteString("#\n# 0. quiesce -- stop every writer before the promote freezes the source:\n")
	if !q.Observed {
		b.WriteString("#   the pod list is not in this input, so step 0 cannot be planned here. It is\n" +
			"#   planned and applied from the live cluster when this runs with -dry-run=false.\n")
		return b.String()
	}
	if q.empty() {
		b.WriteString("#   no pod mounts an affected claim; nothing to quiesce and nothing to restore.\n")
		return b.String()
	}

	for _, owner := range q.Owners {
		if owner.LeftAtZero {
			fmt.Fprintf(&b, "#   %s is at %d replicas with %s recording %d: left there by an earlier run "+
				"that did not reach step 4; nothing to stop now, restored to %d in step 4\n",
				owner.object(), owner.Replicas, annotationQuiescedReplicas, owner.Restore, owner.Restore)
			continue
		}
		remembered := ""
		if owner.Remembered {
			remembered = fmt.Sprintf(", %s already records %d", annotationQuiescedReplicas, owner.Restore)
		}
		fmt.Fprintf(&b, "#   scale %s to 0 (now %d, restored to %d in step 4%s)\n",
			owner.object(), owner.Replicas, owner.Restore, remembered)
		for _, pod := range owner.Pods {
			fmt.Fprintf(&b, "#     pod %s\n", pod)
		}
	}
	for _, pod := range q.BarePods {
		if q.AllowBarePods {
			fmt.Fprintf(&b, "#   DELETE bare pod %s (-allow-bare-pods). Nothing recreates it\n", pod)
			continue
		}
		fmt.Fprintf(&b, "#   REFUSAL: bare pod %s has no controller to scale\n", pod)
	}
	for _, pod := range q.Unscalable {
		fmt.Fprintf(&b, "#   REFUSAL: %s\n", pod)
	}
	if err := q.refusal(); err != nil {
		fmt.Fprintf(&b, "#   This plan will not be applied as it stands: %s\n", err)
	}
	b.WriteString("#   Then wait until no pod in " + namespace + " mounts any affected claim.\n" +
		"#   `kubectl delete pod` is not a quiesce: a process that ignores SIGTERM writes until\n" +
		"#   the grace-period kill, and the promote in step 1 freezes the source.\n")
	return b.String()
}

// restoreString renders step 4, which is the mirror image of step 0.
func (q quiescePlan) restoreString() string {
	var b strings.Builder

	b.WriteString("#\n# 4. restore -- scale every owner back to what step 0 recorded:\n")
	if !q.Observed {
		b.WriteString("#   planned from the live cluster at apply time, like step 0.\n")
		return b.String()
	}
	if len(q.Owners) == 0 {
		b.WriteString("#   nothing was scaled down, so nothing is scaled back up.\n")
	}
	for _, owner := range q.Owners {
		fmt.Fprintf(&b, "#   scale %s back to %d\n", owner.object(), owner.Restore)
	}
	for _, pod := range q.BarePods {
		if q.AllowBarePods {
			fmt.Fprintf(&b, "#   WARNING: bare pod %s was deleted in step 0 and is NOT recreated "+
				"here. Re-apply it yourself\n", pod)
		}
	}
	b.WriteString("#   The pods come back only after the DNS change has had one TTL to propagate:\n" +
		"#   a live NFS mount never follows a failover, so a pod that came back earlier\n" +
		"#   would mount the array the record still points at.\n")
	return b.String()
}
