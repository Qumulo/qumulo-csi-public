// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// The fixtures: one protected volume, mirrored by `mirror` itself.
//
// The pair is built by calling mirrorVolume rather than by hand-writing the
// mirrored PV, because the join `failover` makes runs entirely off what
// `mirror` wrote. A test that hand-wrote the annotation would still pass if
// `mirror` stopped setting it.

func failoverMirrorOptions() mirrorOptions {
	opt := testMirrorOptions()
	// Topology B: one cluster, so the mirrored claim needs its own name.
	opt.claimSuffix = "-dr"
	return opt
}

// aVolumeReplication is the CR as the csi-addons controller writes one back.
func aVolumeReplication(name, claim, desired, state string, conditions map[string]string) volumeReplication {
	if conditions == nil {
		conditions = map[string]string{conditionCompleted: "True", conditionDegraded: "False"}
	}
	return volumeReplication{
		Namespace:    "apps",
		Name:         name,
		Class:        "qumulo-replication",
		ClaimName:    claim,
		DesiredState: desired,
		State:        state,
		Conditions:   conditions,
	}
}

// failoverFixture is one mirrored pair plus the four objects around it.
func failoverFixture(t *testing.T, server string) objectStream {
	t.Helper()

	source := sourcePV()
	source.Spec.CSI.VolumeAttributes[volumeAttributeServer] = server
	source.Spec.CSI.VolumeHandle = server + "##/csi/volumes/pvc-abc123"

	mirror, mirrorClaim, err := mirrorVolume(source, "/csi/volumes/pvc-abc123", failoverMirrorOptions())
	require.NoError(t, err)

	sourceClaim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "data"},
	}

	return objectStream{
		Volumes: []*corev1.PersistentVolume{source, mirror},
		Claims:  []*corev1.PersistentVolumeClaim{sourceClaim, mirrorClaim},
		Replications: []volumeReplication{
			aVolumeReplication("data-vr", "data", replicationStatePrimary, replicationStatusPrimary, nil),
			aVolumeReplication("data-dr-vr", "data-dr", replicationStateSecondary,
				replicationStatusSecondary, nil),
		},
	}
}

// The join, which is the part the rest of the command rests on.
func TestDerivePairsJoinsEachSideThroughWhatMirrorWrote(t *testing.T) {
	pairs, err := derivePairs(failoverFixture(t, "qumulo.example.com"), "apps", "csi.qumulo.com")
	require.NoError(t, err)
	require.Len(t, pairs, 1)

	pair := pairs[0]
	assert.Equal(t, "pvc-abc123", pair.SourcePV.Name)
	assert.Equal(t, "pvc-abc123-dr", pair.MirrorPV.Name)
	assert.Equal(t, "data", pair.SourceClaim)
	assert.Equal(t, "data-dr", pair.MirrorClaim)
	assert.Equal(t, "data-vr", pair.SourceVR.Name)
	assert.Equal(t, "data-dr-vr", pair.MirrorVR.Name)
	assert.Equal(t, "qumulo.example.com", pair.SourceServer)
	assert.Equal(t, "c2.example.com", pair.MirrorServer)
}

// Every ambiguity is refused rather than resolved by a rule. Picking one of
// two candidates here is picking which copy of the customer's data survives.
func TestDerivePairsRefusals(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(in *objectStream)
		ns      string
		wantErr string
	}{
		{
			name:    "no namespace",
			ns:      "",
			wantErr: "-namespace is required",
		},
		{
			name: "two mirrors of one source PV",
			mutate: func(in *objectStream) {
				second := in.Volumes[1].DeepCopy()
				second.Name = "pvc-abc123-dr2"
				second.Spec.ClaimRef.Name = "data-dr2"
				in.Volumes = append(in.Volumes, second)
			},
			wantErr: "are both mirrors of pvc-abc123",
		},
		{
			name: "two VolumeReplications naming one claim",
			mutate: func(in *objectStream) {
				in.Replications = append(in.Replications,
					aVolumeReplication("data-vr-copy", "data", replicationStatePrimary,
						replicationStatusPrimary, nil))
			},
			wantErr: "2 VolumeReplications name PVC apps/data",
		},
		{
			name: "no VolumeReplication on the DR side",
			mutate: func(in *objectStream) {
				in.Replications = in.Replications[:1]
			},
			wantErr: "no VolumeReplication protects PVC apps/data-dr",
		},
		{
			name: "the source PV is not in the input",
			mutate: func(in *objectStream) {
				in.Volumes = in.Volumes[1:]
			},
			wantErr: "names source PV pvc-abc123, which is not in the input",
		},
		{
			name: "the mirrored PVC was never applied",
			mutate: func(in *objectStream) {
				in.Claims = in.Claims[:1]
			},
			wantErr: "PVC apps/data-dr does not exist",
		},
		{
			name: "the mirror has no claimRef",
			mutate: func(in *objectStream) {
				in.Volumes[1].Spec.ClaimRef = nil
			},
			wantErr: "has no claimRef",
		},
		{
			name: "nothing in the namespace is mirrored",
			mutate: func(in *objectStream) {
				in.Volumes[1].Annotations = nil
			},
			wantErr: "no mirrored volume pairs found in namespace apps",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := failoverFixture(t, "qumulo.example.com")
			if tc.mutate != nil {
				tc.mutate(&in)
			}
			namespace := "apps"
			if tc.ns == "" && tc.name == "no namespace" {
				namespace = ""
			}

			_, err := derivePairs(in, namespace, "csi.qumulo.com")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// A pair whose source PV is claimed in another namespace is refused, not
// silently half-planned.
func TestDerivePairsRefusesASplitNamespace(t *testing.T) {
	in := failoverFixture(t, "qumulo.example.com")
	in.Volumes[0].Spec.ClaimRef.Namespace = "other"

	_, err := derivePairs(in, "apps", "csi.qumulo.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "works within one namespace")
}

func planFor(t *testing.T, server string, dir direction, opt failoverOptions) failoverPlan {
	t.Helper()

	pairs, err := derivePairs(failoverFixture(t, server), "apps", "csi.qumulo.com")
	require.NoError(t, err)
	opt.namespace = "apps"
	if opt.dnsTTL == 0 {
		opt.dnsTTL = 60
	}
	// A pod list was read and held nothing: step 0 has nothing to quiesce.
	// Distinct from "nobody looked", which refuses (see the quiesce tests).
	opt.podsObserved = true
	plan, err := planFailover(pairs, dir, opt)
	require.NoError(t, err)
	return plan
}

// The DNS class, which is the one the DR guidance asks customers to be in:
// the record moves and no Kubernetes object does.
func TestPlanFailoverForADNSClass(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})

	require.Len(t, plan.Promote, 1)
	assert.Equal(t, "data-dr-vr", plan.Promote[0].Name, "failover promotes the mirrored side")
	assert.Equal(t, replicationStatePrimary, plan.Promote[0].To)
	assert.Equal(t, replicationStatusPrimary, plan.Promote[0].WantState)

	require.Len(t, plan.Demote, 1)
	assert.Equal(t, "data-vr", plan.Demote[0].Name, "and demotes the original")
	assert.Equal(t, replicationStateSecondary, plan.Demote[0].To)
	// Defaulted, never asked about: `secondary` without it demotes and stops,
	// leaving the pair frozen on one side and promoted on the other with
	// nothing reprotecting it.
	assert.True(t, plan.Demote[0].AutoResync)

	require.Len(t, plan.Flips, 1)
	assert.Equal(t, "qumulo.example.com", plan.Flips[0].Record)
	assert.Equal(t, "c2.example.com", plan.Flips[0].To)
	assert.Empty(t, plan.Rebinds, "a DNS-named class needs no rebind")
	assert.Equal(t, []string{"data", "data-dr"}, plan.Claims)
	assert.False(t, plan.ApplyDNS, "no zone id was given, so nothing touches DNS")
}

// Failback is the same four steps the other way, and the original PVC comes
// back into service unchanged.
func TestPlanFailbackPromotesTheOriginalSide(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailback, failoverOptions{restartPods: true})

	assert.Equal(t, "data-vr", plan.Promote[0].Name)
	assert.Equal(t, "data-dr-vr", plan.Demote[0].Name)
	assert.True(t, plan.Demote[0].AutoResync)

	// The record is the original class's own name, so the promoted side *is*
	// the record and its address is in no object. The plan says so rather than
	// inventing one.
	require.Len(t, plan.Flips, 1)
	assert.Equal(t, "qumulo.example.com", plan.Flips[0].Record)
	assert.Equal(t, "c2.example.com", plan.Flips[0].From)
	assert.Empty(t, plan.Flips[0].To)
	require.NotEmpty(t, plan.Warnings)
	assert.Contains(t, strings.Join(plan.Warnings, " "), "Pass -dns-target")
	assert.Contains(t, plan.String(), "pass -dns-target")
}

// And with -dns-target it is a complete plan.
func TestPlanFailbackWithADNSTarget(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailback,
		failoverOptions{restartPods: true, dnsTarget: "192.0.2.20", route53Zone: "Z123"})

	require.Len(t, plan.Flips, 1)
	assert.Equal(t, "192.0.2.20", plan.Flips[0].To)
	assert.True(t, plan.ApplyDNS)
	assert.Empty(t, plan.Warnings)
}

// The IP case. `spec.csi` is immutable, so the pods have to move to the other
// side's claim -- and that rebind is `switch`'s plan, reviewed on its own. This
// command names the pairs and stops.
func TestPlanFailoverForAnIPNamedClassDefersTheRebindToSwitch(t *testing.T) {
	plan := planFor(t, "192.0.2.30", directionFailover, failoverOptions{restartPods: true})

	assert.Empty(t, plan.Flips, "an IP literal cannot be re-pointed")
	require.Len(t, plan.Rebinds, 1)
	assert.Contains(t, plan.Rebinds[0], "apps/data -> apps/data-dr")
	assert.Contains(t, plan.Rebinds[0], "pvc-abc123 -> pvc-abc123-dr")
	require.NotEmpty(t, plan.Warnings)
	assert.Contains(t, strings.Join(plan.Warnings, " "), "qumulo-csi-repoint switch")

	// The VolumeReplication half is identical: the topology decides how the
	// pods get to the promoted copy, never whether it is promoted.
	assert.Equal(t, "data-dr-vr", plan.Promote[0].Name)
	assert.Equal(t, "data-vr", plan.Demote[0].Name)
}

// The plan shares stdout with whatever else is piped alongside it, so every
// line has to be a YAML comment.
func TestFailoverPlanIsValidAsAYAMLPreamble(t *testing.T) {
	for _, server := range []string{"qumulo.example.com", "192.0.2.30"} {
		plan := planFor(t, server, directionFailover, failoverOptions{restartPods: true})
		for _, line := range strings.Split(strings.TrimSpace(plan.String()), "\n") {
			assert.True(t, strings.HasPrefix(line, "#"), "not a comment: %q", line)
		}
	}
}

func TestPlanFailoverRefusesAnEmptySetAndAnUnknownDirection(t *testing.T) {
	_, err := planFailover(nil, directionFailover, failoverOptions{})
	require.Error(t, err)

	pairs, err := derivePairs(failoverFixture(t, "qumulo.example.com"), "apps", "csi.qumulo.com")
	require.NoError(t, err)
	_, err = planFailover(pairs, direction("sideways"), failoverOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown direction")
}

// recordingExecutor records the order, which is the product here: the four
// steps are each easy and the sequence is the part that is not.
type recordingExecutor struct {
	calls        []string
	observations map[string][]volumeReplication
	// settled is what AwaitSettled hands back per object, with progressed
	// saying whether the object moved while the settle wait ran.
	settled    map[string]volumeReplication
	progressed map[string]bool
	// remaining is what AwaitNoPodsMounting still sees; empty means quiesced.
	remaining    []string
	preflightErr error
	scaleErr     error
}

func (r *recordingExecutor) SetReplicationState(_ context.Context, change replicationChange) error {
	r.calls = append(r.calls, fmt.Sprintf("set %s/%s %s autoResync=%t",
		change.Namespace, change.Name, change.To, change.AutoResync))
	return nil
}

func (r *recordingExecutor) AwaitReplication(
	_ context.Context,
	namespace, name, wantState string,
) (volumeReplication, error) {
	key := namespace + "/" + name
	r.calls = append(r.calls, fmt.Sprintf("await %s %s", key, wantState))

	queue := r.observations[key]
	var observed volumeReplication
	if len(queue) > 0 {
		observed = queue[0]
		if len(queue) > 1 {
			r.observations[key] = queue[1:]
		}
	}
	if observed.reached(wantState) {
		return observed, nil
	}
	return observed, fmt.Errorf("gave up after 1s")
}

func (r *recordingExecutor) NudgeReplication(_ context.Context, namespace, name string) error {
	r.calls = append(r.calls, fmt.Sprintf("nudge %s/%s", namespace, name))
	return nil
}

func (r *recordingExecutor) FlipRecord(_ context.Context, flip dnsFlip, ttl int) error {
	r.calls = append(r.calls, fmt.Sprintf("flip %s -> %s ttl=%d", flip.Record, flip.To, ttl))
	return nil
}

func (r *recordingExecutor) Settle(_ context.Context, ttl int) error {
	r.calls = append(r.calls, fmt.Sprintf("settle %d", ttl))
	return nil
}

func (r *recordingExecutor) AwaitSettled(
	_ context.Context,
	namespace, name string,
) (volumeReplication, bool, error) {
	key := namespace + "/" + name
	r.calls = append(r.calls, "settle-wait "+key)
	return r.settled[key], r.progressed[key], nil
}

func (r *recordingExecutor) PreflightDNS(_ context.Context, flips []dnsFlip, ttl int) error {
	records := make([]string, 0, len(flips))
	for _, flip := range flips {
		records = append(records, flip.Record)
	}
	r.calls = append(r.calls, fmt.Sprintf("preflight-dns %s ttl=%d", strings.Join(records, ","), ttl))
	return r.preflightErr
}

func (r *recordingExecutor) QuiesceOwner(_ context.Context, owner quiesceOwner) error {
	r.calls = append(r.calls, fmt.Sprintf("quiesce %s %d->0", owner.object(), owner.Replicas))
	return r.scaleErr
}

func (r *recordingExecutor) RestoreOwner(_ context.Context, owner quiesceOwner) error {
	r.calls = append(r.calls, fmt.Sprintf("restore %s 0->%d", owner.object(), owner.Restore))
	return r.scaleErr
}

func (r *recordingExecutor) DeletePod(_ context.Context, namespace, name string) error {
	r.calls = append(r.calls, fmt.Sprintf("delete-pod %s/%s", namespace, name))
	return nil
}

func (r *recordingExecutor) AwaitNoPodsMounting(
	_ context.Context,
	namespace string,
	claims []string,
) ([]string, error) {
	r.calls = append(r.calls, fmt.Sprintf("await-no-pods %s %s", namespace, strings.Join(claims, ",")))
	return r.remaining, nil
}

func healthyAt(state string) volumeReplication {
	return volumeReplication{
		State:      state,
		Conditions: map[string]string{conditionCompleted: "True", conditionDegraded: "False"},
	}
}

// The order, which is the one thing this command adds over five `kubectl`
// edits: the DNS preflight, then quiesce, promote, demote, DNS, restore, and
// nothing overlapping.
func TestExecuteFailoverAppliesTheFiveStepsInOrder(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover,
		failoverOptions{restartPods: true, route53Zone: "Z123", dnsTarget: "192.0.2.20", dnsTTL: 0})
	plan.TTL = 0 // no real sleep in a unit test; Settle is still called.

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {healthyAt(replicationStatusSecondary)},
		},
	}

	var out bytes.Buffer
	require.NoError(t, executeFailover(context.Background(), plan, ex, &out))

	assert.Equal(t, []string{
		"preflight-dns qumulo.example.com ttl=0",
		"set apps/data-dr-vr primary autoResync=false",
		"await apps/data-dr-vr Primary",
		"set apps/data-vr secondary autoResync=true",
		"await apps/data-vr Secondary",
		"flip qumulo.example.com -> 192.0.2.20 ttl=0",
		"settle 0",
	}, ex.calls)

	assert.Contains(t, out.String(), "no pod mounts an affected claim")
	assert.Contains(t, out.String(), "===== failover summary =====")
	assert.Contains(t, out.String(), summaryPrefix)
}

// Nothing is promoted until the quiesce has landed, and the DNS zone is proved
// before either. The whole point of step 0 is where it sits.
func TestExecuteFailoverQuiescesBeforeItPromotes(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover,
		failoverOptions{restartPods: true, route53Zone: "Z123", dnsTarget: "192.0.2.20"})
	plan.TTL = 0
	plan.Quiesce = quiescePlan{
		Observed: true,
		Owners: []quiesceOwner{
			{Kind: kindDeployment, Namespace: "apps", Name: "web", Replicas: 3, Restore: 3,
				Pods: []string{"apps/web-0"}},
		},
	}

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {healthyAt(replicationStatusSecondary)},
		},
	}

	var out bytes.Buffer
	require.NoError(t, executeFailover(context.Background(), plan, ex, &out))

	assert.Equal(t, []string{
		"preflight-dns qumulo.example.com ttl=0",
		"quiesce apps/Deployment/web 3->0",
		"await-no-pods apps data,data-dr",
		"set apps/data-dr-vr primary autoResync=false",
		"await apps/data-dr-vr Primary",
		"set apps/data-vr secondary autoResync=true",
		"await apps/data-vr Secondary",
		"flip qumulo.example.com -> 192.0.2.20 ttl=0",
		"settle 0",
		"restore apps/Deployment/web 0->3",
	}, ex.calls)
}

// A pod that does not go away is a writer that did not stop: a process
// that ignores SIGTERM keeps writing until the grace-period kill, so the scale
// to zero is a promise and the pod list is the proof.
func TestExecuteFailoverRefusesToPromoteWhileAPodStillMounts(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})
	plan.Quiesce = quiescePlan{
		Observed: true,
		Owners: []quiesceOwner{
			{Kind: kindStatefulSet, Namespace: "apps", Name: "db", Replicas: 1, Restore: 1},
		},
	}

	ex := &recordingExecutor{remaining: []string{"apps/db-0"}}

	var out bytes.Buffer
	err := executeFailover(context.Background(), plan, ex, &out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to promote")
	assert.Contains(t, err.Error(), "apps/db-0")
	assert.Contains(t, err.Error(), "dropped silently")
	assert.NotContains(t, strings.Join(ex.calls, "|"), "set apps/data-dr-vr")
}

// And with no pod list at all -- a plan from `-f` manifests that carried none --
// "nothing is running" and "nobody looked" are the same value, so it refuses.
func TestExecuteFailoverRefusesWhenThePodsWereNeverRead(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})
	plan.Quiesce = quiescePlan{Observed: false}

	ex := &recordingExecutor{}
	var out bytes.Buffer
	err := executeFailover(context.Background(), plan, ex, &out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "were never read")
	assert.Empty(t, ex.calls)
}

// -restart-pods=false does not mean "promote anyway": it means the operator is
// handling the workloads, and the command still refuses while one is writing.
func TestExecuteFailoverWithRestartPodsFalseStillRefusesAWriter(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: false})
	plan.Quiesce = quiescePlan{
		Observed: true,
		Pods:     []string{"apps/web-0"},
		Owners: []quiesceOwner{
			{Kind: kindDeployment, Namespace: "apps", Name: "web", Replicas: 1, Restore: 1},
		},
	}

	ex := &recordingExecutor{}
	var out bytes.Buffer
	err := executeFailover(context.Background(), plan, ex, &out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-restart-pods=false")
	assert.Contains(t, err.Error(), "apps/web-0")
	assert.Empty(t, ex.calls, "nothing is scaled and nothing is promoted")
}

// The restore puts back what step 0 recorded, not what the object read at the
// time: on a re-run that number is the zero this command wrote last time.
func TestExecuteFailoverRestoresTheRememberedReplicas(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})
	plan.TTL = 0
	plan.Quiesce = quiescePlan{
		Observed: true,
		Owners: []quiesceOwner{
			// A re-run: the owner reads 0 now, and the annotation remembers 4.
			{Kind: kindDeployment, Namespace: "apps", Name: "web", Replicas: 0, Restore: 4,
				Remembered: true},
		},
	}

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {healthyAt(replicationStatusSecondary)},
		},
	}

	var out bytes.Buffer
	require.NoError(t, executeFailover(context.Background(), plan, ex, &out))
	assert.Contains(t, ex.calls, "restore apps/Deployment/web 0->4")
	assert.Contains(t, out.String(), "apps/Deployment/web: 0 -> 4 replicas")
}

// The measured failure, and the measured remedy, from the lab cluster.
//
// With the old array down, Demote fails `Unavailable` and the csi-addons
// controller backs off exponentially -- 98 seconds of a writable old copy in
// the measured case. Any spec change forces an immediate reconcile, and the
// tool applies it once rather than printing it.
func TestExecuteFailoverForcesAReconcileOnAStuckSecondary(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr": {
				{State: replicationStatusPrimary, Message: "rpc error: code = Unavailable"},
				healthyAt(replicationStatusSecondary),
			},
		},
	}

	var out bytes.Buffer
	require.NoError(t, executeFailover(context.Background(), plan, ex, &out))

	assert.Equal(t, []string{
		"set apps/data-dr-vr primary autoResync=false",
		"await apps/data-dr-vr Primary",
		"set apps/data-vr secondary autoResync=true",
		"await apps/data-vr Secondary",
		"settle-wait apps/data-vr",
		"nudge apps/data-vr",
		"await apps/data-vr Secondary",
	}, ex.calls)
	assert.Contains(t, out.String(), "toggling a spec field once")
}

// And when even the forced reconcile does not land -- the array is still down --
// the failover carries on and says exactly what is unfinished. Stopping here
// would leave the promoted copy unused and the pods on a dead mount.
func TestExecuteFailoverCarriesOnWhenTheOldSideIsStillUnreachable(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {{State: replicationStatusPrimary, Message: "code = Unavailable"}},
		},
	}

	var out bytes.Buffer
	require.NoError(t, executeFailover(context.Background(), plan, ex, &out))

	assert.Contains(t, ex.calls, "nudge apps/data-vr")
	assert.Contains(t, out.String(), "has not reached Secondary")
	assert.Contains(t, out.String(), "keep pods away from it")
}

// A Secondary that is Completed but Degraded is not reprotected, and a
// failback from there would resync onto a copy that is not caught up.
func TestExecuteFailoverFlagsADegradedSecondary(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})

	degraded := volumeReplication{
		State: replicationStatusSecondary,
		Conditions: map[string]string{
			conditionCompleted: "True", conditionDegraded: "True", conditionResyncing: "True",
		},
		Message: "resync in progress",
	}
	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {degraded},
		},
	}

	var out bytes.Buffer
	require.NoError(t, executeFailover(context.Background(), plan, ex, &out))
	assert.Contains(t, out.String(), "Secondary but degraded")
	assert.Contains(t, out.String(), "do not fail back until it is")
}

// The nudge gate. After the demote the resync holds `Resyncing: True`
// and a `Degraded` reason of `VolumeDegraded` for a while, and the tool used to
// toggle a spec field on every object before they settled. The toggle is the
// remedy for a controller that has *stopped*; firing it at one that is working
// re-enqueues work in flight and hides the signal.
func TestExecuteFailoverDoesNotNudgeAnObjectThatIsStillResyncing(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})

	resyncing := volumeReplication{
		State: replicationStatusSecondary,
		Conditions: map[string]string{
			conditionCompleted: "False", conditionDegraded: "True", conditionResyncing: "True",
		},
		Reasons: map[string]string{conditionDegraded: reasonVolumeDegraded},
		Message: "resync in progress",
	}
	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {resyncing},
		},
		// Still reconciling when the settle wait gave up.
		settled: map[string]volumeReplication{"apps/data-vr": resyncing},
	}

	var out bytes.Buffer
	require.NoError(t, executeFailover(context.Background(), plan, ex, &out))

	assert.Contains(t, ex.calls, "settle-wait apps/data-vr")
	assert.NotContains(t, strings.Join(ex.calls, "|"), "nudge",
		"a reconciling controller is never toggled")
	assert.Contains(t, out.String(), "NOT toggling a spec field")
	assert.Contains(t, out.String(), "Re-run when the resync has finished")
}

// And an object that moved while the wait ran is re-read rather than
// toggled: progress is the other half of the gate.
func TestExecuteFailoverRereadsAnObjectThatMovedInsteadOfNudgingIt(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr": {
				{State: replicationStatusPrimary, Message: "demote pending"},
				healthyAt(replicationStatusSecondary),
			},
		},
		settled: map[string]volumeReplication{
			"apps/data-vr": {State: replicationStatusSecondary,
				Conditions: map[string]string{conditionCompleted: "False"}},
		},
		progressed: map[string]bool{"apps/data-vr": true},
	}

	var out bytes.Buffer
	require.NoError(t, executeFailover(context.Background(), plan, ex, &out))

	assert.Equal(t, []string{
		"set apps/data-dr-vr primary autoResync=false",
		"await apps/data-dr-vr Primary",
		"set apps/data-vr secondary autoResync=true",
		"await apps/data-vr Secondary",
		"settle-wait apps/data-vr",
		"await apps/data-vr Secondary",
	}, ex.calls)
	assert.NotContains(t, strings.Join(ex.calls, "|"), "nudge")
	assert.Contains(t, out.String(), "re-reading rather than toggling")
}

// A promote that never completes is the one fatal step. Nothing downstream --
// the record, the pods -- may point at a copy that is not writable.
func TestExecuteFailoverStopsWhenThePromoteNeverCompletes(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {{State: replicationStatusPrimary,
				Conditions: map[string]string{conditionCompleted: "False"},
				Message:    "promoting"}},
		},
	}

	var out bytes.Buffer
	err := executeFailover(context.Background(), plan, ex, &out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not reach Primary/Completed")
	assert.Contains(t, err.Error(), "Completed=False")
	assert.NotContains(t, strings.Join(ex.calls, "|"), "restore")
	assert.NotContains(t, strings.Join(ex.calls, "|"), "set apps/data-vr")
	// The summary is printed even when the run stopped: that run is exactly the
	// one somebody has to read.
	assert.Contains(t, out.String(), "DID NOT COMPLETE")
}

// A bare pod stops the command. Deleting one destroys the only copy of its
// definition, and a DR run that loses a workload while recovering its data has
// not recovered anything -- but leaving it writing through the promote loses
// data silently, so it cannot be ignored either. The operator decides.
func TestExecuteFailoverRefusesABarePodAndDeletesItOnlyWhenAllowed(t *testing.T) {
	refusing := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})
	refusing.Quiesce = quiescePlan{Observed: true, BarePods: []string{"apps/hand-made"}}

	ex := &recordingExecutor{}
	var out bytes.Buffer
	err := executeFailover(context.Background(), refusing, ex, &out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apps/hand-made")
	assert.Contains(t, err.Error(), "-allow-bare-pods")
	assert.Empty(t, ex.calls)

	allowed := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})
	allowed.TTL = 0
	allowed.Quiesce = quiescePlan{
		Observed: true, AllowBarePods: true, BarePods: []string{"apps/hand-made"},
	}
	ex = &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {healthyAt(replicationStatusSecondary)},
		},
	}
	out.Reset()
	require.NoError(t, executeFailover(context.Background(), allowed, ex, &out))
	assert.Contains(t, ex.calls, "delete-pod apps/hand-made")
	assert.Contains(t, out.String(), "**Nothing recreates it.**")
	assert.Contains(t, out.String(), "is NOT recreated")
}

// With no zone id nothing touches DNS, whatever else the plan does.
func TestExecuteFailoverTouchesNoDNSWithoutAZoneID(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {healthyAt(replicationStatusSecondary)},
		},
	}

	var out bytes.Buffer
	require.NoError(t, executeFailover(context.Background(), plan, ex, &out))
	assert.NotContains(t, strings.Join(ex.calls, "|"), "flip")
	assert.Contains(t, out.String(), "No -route53-zone-id was given")
}

// `status.state` alone is not enough. During a resync the old side already
// reads Secondary while Completed is still False, and acting on the role alone
// would restart pods onto a copy the reconnect is still rewinding.
func TestReachedRequiresTheCompletedCondition(t *testing.T) {
	resyncing := volumeReplication{
		State: replicationStatusSecondary,
		Conditions: map[string]string{
			conditionCompleted: "False", conditionResyncing: "True",
		},
	}
	assert.False(t, resyncing.reached(replicationStatusSecondary))
	assert.False(t, resyncing.healthySecondary())
	assert.True(t, healthyAt(replicationStatusSecondary).healthySecondary())
	assert.Contains(t, resyncing.describe(), "Resyncing=True")
	assert.Contains(t, volumeReplication{}.describe(), "<no status yet>")
}

// The wait is bounded, always: a failover that blocks forever on a CR that
// will never move is worse than one that reports what it saw.
func TestAwaitReplicationIsBoundedAndReturnsTheLastObservation(t *testing.T) {
	calls := 0
	observed, err := awaitReplication(context.Background(),
		func(context.Context) (volumeReplication, error) {
			calls++
			return volumeReplication{State: replicationStatusPrimary}, nil
		},
		replicationStatusSecondary, time.Millisecond, 5*time.Millisecond)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "gave up after")
	assert.Equal(t, replicationStatusPrimary, observed.State)
	assert.Positive(t, calls)
}

func TestAwaitReplicationReturnsAsSoonAsItArrives(t *testing.T) {
	observed, err := awaitReplication(context.Background(),
		func(context.Context) (volumeReplication, error) {
			return healthyAt(replicationStatusPrimary), nil
		},
		replicationStatusPrimary, time.Hour, time.Hour)

	require.NoError(t, err)
	assert.Equal(t, replicationStatusPrimary, observed.State)
}

// The CR, as the controller writes it back.
func TestVolumeReplicationFrom(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": volumeReplicationAPIVersion,
		"kind":       "VolumeReplication",
		"metadata":   map[string]any{"namespace": "apps", "name": "data-vr"},
		"spec": map[string]any{
			"volumeReplicationClass": "qumulo-replication",
			"replicationState":       "primary",
			"autoResync":             true,
			"dataSource": map[string]any{
				"apiGroup": "", "kind": "PersistentVolumeClaim", "name": "data",
			},
		},
		"status": map[string]any{
			"state":   "Primary",
			"message": "volume is marked primary",
			"conditions": []any{
				map[string]any{"type": "Completed", "status": "True"},
				map[string]any{"type": "Degraded", "status": "False"},
			},
		},
	}}

	vr, err := volumeReplicationFrom(object)
	require.NoError(t, err)
	assert.Equal(t, "data", vr.ClaimName)
	assert.Equal(t, "primary", vr.DesiredState)
	assert.True(t, vr.AutoResync)
	assert.True(t, vr.reached(replicationStatusPrimary))
	assert.False(t, vr.degraded())
}

func TestVolumeReplicationFromRefusals(t *testing.T) {
	noClaim := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "apps", "name": "data-vr"},
		"spec":     map[string]any{"replicationState": "primary"},
	}}
	_, err := volumeReplicationFrom(noClaim)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names no spec.dataSource.name")

	wrongKind := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "apps", "name": "group-vr"},
		"spec": map[string]any{
			"dataSource": map[string]any{"kind": "VolumeGroupReplication", "name": "g"},
		},
	}}
	_, err = volumeReplicationFrom(wrongKind)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PersistentVolumeClaim-backed objects only")
}

// The Route53 change, rendered whether or not it will be applied: it is the
// exact change, reviewable, and applicable by hand on a machine this command
// never runs on.
func TestRoute53ChangeBatch(t *testing.T) {
	batch, err := route53ChangeBatch(dnsFlip{Record: "qumulo.example.com", To: "192.0.2.20"}, 60)
	require.NoError(t, err)

	assert.Contains(t, batch, `"Action": "UPSERT"`)
	assert.Contains(t, batch, `"Name": "qumulo.example.com"`)
	assert.Contains(t, batch, `"Type": "A"`)
	assert.Contains(t, batch, `"TTL": 60`)
	assert.Contains(t, batch, `"Value": "192.0.2.20"`)
}

func TestRoute53ChangeBatchRefusals(t *testing.T) {
	_, err := route53ChangeBatch(dnsFlip{Record: "qumulo.example.com"}, 60)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no target address")

	// An A record cannot name a host, and silently emitting a CNAME instead
	// would change what the operator reviewed.
	_, err = route53ChangeBatch(dnsFlip{Record: "qumulo.example.com", To: "c2.example.com"}, 60)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is a name, not an address")

	_, err = route53ChangeBatch(dnsFlip{Record: "qumulo.example.com", To: "192.0.2.20"}, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not usable")
}

func TestPodsMountingClaims(t *testing.T) {
	pods := []corev1.Pod{
		podWithClaims("apps", "web-1", "data"),
		podWithClaims("apps", "web-0", "data-dr"),
		podWithClaims("apps", "unrelated", "logs"),
		{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "no-volumes"}},
	}

	got := podsMountingClaims(pods, []string{"data", "data-dr"})
	require.Len(t, got, 2)
	assert.Equal(t, "web-0", got[0].Name)
	assert.Equal(t, "web-1", got[1].Name)
}

// The group form: a `shared-subdirectory` class is protected as one unit.
//
// Measured on the lab cluster (in the namespace the first live dry-run of
// this command refused): one relationship rooted at the class
// `basePath`, one VolumeReplication per direction, and every member volume of
// that StorageClass riding on it. The driver *refuses* a per-PVC object on
// such a class, so a member with none of its own is a working configuration,
// not a gap -- and the two need opposite answers.

const groupStorageClass = "qumulo-shared"

// groupMember builds one member volume of a shared class and its mirror.
func groupMember(t *testing.T, name string) (*corev1.PersistentVolume, *corev1.PersistentVolume,
	*corev1.PersistentVolumeClaim, *corev1.PersistentVolumeClaim) {
	t.Helper()

	source := sourcePV()
	source.Name = "pvc-" + name
	source.Spec.StorageClassName = groupStorageClass
	source.Spec.ClaimRef.Name = name
	source.Spec.CSI.VolumeAttributes[volumeAttributeServer] = "qumulo.example.com"
	source.Spec.CSI.VolumeAttributes[volumeAttributeRealPath] = "/csi/shared/" + name
	source.Spec.CSI.VolumeHandle = "qumulo.example.com##/csi/volumes/pvc-" + name

	mirror, mirrorClaim, err := mirrorVolume(source, "/csi/volumes/pvc-abc123", failoverMirrorOptions())
	require.NoError(t, err)

	class := groupStorageClass
	sourceClaim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: name},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &class},
	}
	return source, mirror, sourceClaim, mirrorClaim
}

// groupFixture is the lab's shape: two members, one VolumeReplication pair on
// the first member's claim, and no object at all on the second.
func groupFixture(t *testing.T) objectStream {
	t.Helper()

	sourceA, mirrorA, claimA, mirrorClaimA := groupMember(t, "r12-shared-a")
	sourceB, mirrorB, claimB, mirrorClaimB := groupMember(t, "r12-shared-b")

	return objectStream{
		Volumes: []*corev1.PersistentVolume{sourceA, mirrorA, sourceB, mirrorB},
		Claims: []*corev1.PersistentVolumeClaim{
			claimA, mirrorClaimA, claimB, mirrorClaimB,
		},
		Replications: []volumeReplication{
			withClass(aVolumeReplication("r12-shared-group", "r12-shared-a",
				replicationStatePrimary, replicationStatusPrimary, nil), "qumulo-c1-to-c2-group"),
			withClass(aVolumeReplication("r12-shared-group-dr", "r12-shared-a-dr",
				replicationStateSecondary, replicationStatusSecondary, nil), "qumulo-c2-to-c1-group"),
		},
		ReplicationClasses: []volumeReplicationClass{
			groupReplicationClass("qumulo-c1-to-c2-group"),
			groupReplicationClass("qumulo-c2-to-c1-group"),
		},
	}
}

func withClass(vr volumeReplication, class string) volumeReplication {
	vr.Class = class
	return vr
}

func groupReplicationClass(name string) volumeReplicationClass {
	return volumeReplicationClass{
		Name: name,
		Parameters: map[string]string{
			"targetServer":            "c2.example.com",
			replicationScopeParameter: replicationScopeGroup,
		},
	}
}

// The refusal the coordinator hit, and why it was wrong: the second member has
// no object of its own *by design*.
func TestDerivePairsCoversAGroupMemberFromTheClassObject(t *testing.T) {
	pairs, err := derivePairs(groupFixture(t), "apps", "csi.qumulo.com")
	require.NoError(t, err)
	require.Len(t, pairs, 2, "both members are pairs, even though only one carries the objects")

	for _, pair := range pairs {
		assert.Equal(t, "r12-shared-group", pair.SourceVR.Name)
		assert.Equal(t, "r12-shared-group-dr", pair.MirrorVR.Name,
			"the DR-side object must be the one on a *mirrored* claim, not the other direction's")
		assert.Equal(t, groupStorageClass, pair.GroupClass)
	}
	assert.Equal(t, "r12-shared-a", pairs[0].SourceClaim)
	assert.Equal(t, "r12-shared-b", pairs[1].SourceClaim)
}

// One edit per side moves every member, and the plan says so.
func TestPlanFailoverForAGroupEditsOncePerSideAndRestartsEveryMember(t *testing.T) {
	pairs, err := derivePairs(groupFixture(t), "apps", "csi.qumulo.com")
	require.NoError(t, err)

	plan, err := planFailover(pairs, directionFailover,
		failoverOptions{namespace: "apps", dnsTTL: 60, restartPods: true})
	require.NoError(t, err)

	// Two members, one pair of objects. Writing `primary` twice is harmless;
	// waiting for it twice is minutes of an outage spent on nothing.
	require.Len(t, plan.Promote, 1)
	assert.Equal(t, "r12-shared-group-dr", plan.Promote[0].Name)
	require.Len(t, plan.Demote, 1)
	assert.Equal(t, "r12-shared-group", plan.Demote[0].Name)
	assert.True(t, plan.Demote[0].AutoResync)

	// But every member's pods come back, because one promote makes all of
	// them writable at once.
	assert.Equal(t, []string{
		"r12-shared-a", "r12-shared-a-dr", "r12-shared-b", "r12-shared-b-dr",
	}, plan.Claims)

	require.Len(t, plan.Groups, 1)
	group := plan.Groups[0]
	assert.Equal(t, groupStorageClass, group.StorageClass)
	assert.Equal(t, "apps/r12-shared-group-dr", group.PromoteVR)
	assert.Equal(t, "apps/r12-shared-group", group.DemoteVR)
	assert.Equal(t, []string{
		"r12-shared-a -> r12-shared-a-dr",
		"r12-shared-b -> r12-shared-b-dr",
	}, group.Members)

	rendered := plan.String()
	assert.Contains(t, rendered, "StorageClass qumulo-shared (replicationScope: group), 2 member(s):")
	assert.Contains(t, rendered, "promote apps/r12-shared-group-dr, demote apps/r12-shared-group")
	assert.Contains(t, rendered, "member r12-shared-b -> r12-shared-b-dr")
	assert.Contains(t, rendered, "One edit per side moves all of them")

	// And the whole plan is still a valid YAML preamble.
	for _, line := range strings.Split(strings.TrimSpace(rendered), "\n") {
		assert.True(t, strings.HasPrefix(line, "#"), "not a comment: %q", line)
	}
}

// Failback runs the group the other way, with the same one edit per side.
func TestPlanFailbackForAGroup(t *testing.T) {
	pairs, err := derivePairs(groupFixture(t), "apps", "csi.qumulo.com")
	require.NoError(t, err)

	plan, err := planFailover(pairs, directionFailback,
		failoverOptions{namespace: "apps", dnsTTL: 60, restartPods: true, dnsTarget: "192.0.2.30"})
	require.NoError(t, err)

	require.Len(t, plan.Promote, 1)
	assert.Equal(t, "r12-shared-group", plan.Promote[0].Name)
	require.Len(t, plan.Demote, 1)
	assert.Equal(t, "r12-shared-group-dr", plan.Demote[0].Name)
	require.Len(t, plan.Groups, 1)
	assert.Equal(t, []string{
		"r12-shared-a-dr -> r12-shared-a",
		"r12-shared-b-dr -> r12-shared-b",
	}, plan.Groups[0].Members)
}

// A member whose class has no group object at all is still refused, and the
// refusal now says what it looked for.
func TestDerivePairsRefusesAMemberWithNoGroupObject(t *testing.T) {
	in := groupFixture(t)
	// The class is no longer a group one, so the object on r12-shared-a covers
	// only its own claim and r12-shared-b is genuinely unprotected.
	in.ReplicationClasses = []volumeReplicationClass{
		{Name: "qumulo-c1-to-c2-group", Parameters: map[string]string{"targetServer": "c2.example.com"}},
		{Name: "qumulo-c2-to-c1-group", Parameters: map[string]string{"targetServer": "c1.example.com"}},
	}

	_, err := derivePairs(in, "apps", "csi.qumulo.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no VolumeReplication protects PVC apps/r12-shared-b")
	assert.Contains(t, err.Error(), "no group object covers StorageClass qumulo-shared")
	assert.Contains(t, err.Error(), "replicationScope: group")
}

// And so is a member whose class has two group objects on one side.
func TestDerivePairsRefusesTwoGroupObjectsOnOneSide(t *testing.T) {
	in := groupFixture(t)
	in.Replications = append(in.Replications,
		withClass(aVolumeReplication("r12-shared-group-2", "r12-shared-a",
			replicationStatePrimary, replicationStatusPrimary, nil), "qumulo-c1-to-c2-group"))

	_, err := derivePairs(in, "apps", "csi.qumulo.com")
	require.Error(t, err)
	// r12-shared-a itself now has two objects of its own; r12-shared-b sees
	// two candidates for its class. Either way it is ambiguous and refused.
	assert.Contains(t, err.Error(), "ambiguous")
}

// A namespace holding both shapes: per-volume pairs and a group.
func TestDerivePairsHandlesAMixedNamespace(t *testing.T) {
	in := groupFixture(t)
	perVolume := failoverFixture(t, "qumulo.example.com")
	in.Volumes = append(in.Volumes, perVolume.Volumes...)
	in.Claims = append(in.Claims, perVolume.Claims...)
	in.Replications = append(in.Replications, perVolume.Replications...)
	// The per-volume pair's own class is not a group one.
	in.ReplicationClasses = append(in.ReplicationClasses, volumeReplicationClass{
		Name:       "qumulo-replication",
		Parameters: map[string]string{"targetServer": "c2.example.com"},
	})

	pairs, err := derivePairs(in, "apps", "csi.qumulo.com")
	require.NoError(t, err)
	require.Len(t, pairs, 3)

	byClaim := map[string]failoverPair{}
	for _, pair := range pairs {
		byClaim[pair.SourceClaim] = pair
	}
	assert.Empty(t, byClaim["data"].GroupClass, "a per-volume pair carries its own objects")
	assert.Equal(t, "data-vr", byClaim["data"].SourceVR.Name)
	assert.Equal(t, groupStorageClass, byClaim["r12-shared-b"].GroupClass)
	assert.Equal(t, "r12-shared-group", byClaim["r12-shared-b"].SourceVR.Name)

	plan, err := planFailover(pairs, directionFailover,
		failoverOptions{namespace: "apps", dnsTTL: 60, restartPods: true})
	require.NoError(t, err)

	// Two objects per side: the per-volume one and the group's.
	require.Len(t, plan.Promote, 2)
	require.Len(t, plan.Demote, 2)
	assert.ElementsMatch(t, []string{"data-dr-vr", "r12-shared-group-dr"},
		[]string{plan.Promote[0].Name, plan.Promote[1].Name})
	require.Len(t, plan.Groups, 1, "only the shared class is a group")
	assert.Len(t, plan.Claims, 6, "every member of both shapes gets its pods restarted")
}

// The class, off the wire.
func TestVolumeReplicationClassFrom(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": volumeReplicationAPIVersion,
		"kind":       volumeReplicationClassKind,
		"metadata":   map[string]any{"name": "qumulo-c1-to-c2-group"},
		"spec": map[string]any{
			"provisioner": "csi.qumulo.com",
			"parameters": map[string]any{
				"targetServer":            "c2.example.com",
				replicationScopeParameter: replicationScopeGroup,
			},
		},
	}}

	class, err := volumeReplicationClassFrom(object)
	require.NoError(t, err)
	assert.Equal(t, "qumulo-c1-to-c2-group", class.Name)
	assert.True(t, class.isGroup())

	// A class with no parameters at all is a per-volume class, not an error.
	bare, err := volumeReplicationClassFrom(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "qumulo-c1-to-c2"},
	}})
	require.NoError(t, err)
	assert.False(t, bare.isGroup())

	_, err = volumeReplicationClassFrom(&unstructured.Unstructured{Object: map[string]any{}})
	require.Error(t, err)
}

// One DNS record, two pairs whose writable copies are on different
// arrays. Without -dns-target that is refused; an explicit target was applied
// before the conflict check, which then compared it with itself and planned
// both volumes onto one array. The check now runs on what the objects say.
func TestAnExplicitDNSTargetDoesNotHideSplitLiveCopies(t *testing.T) {
	pairs := []failoverPair{dnsPair("1", "csi-a.lab", "192.0.2.2"), dnsPair("2", "csi-a.lab", "192.0.2.3")}
	for _, target := range []string{"", "192.0.2.2", "csi-a.lab=192.0.2.2"} {
		plan, err := planFailover(pairs, directionFailover, failoverOptions{namespace: "ns", dnsTTL: 60, podsObserved: true, dnsTarget: target})
		require.Error(t, err, "-dns-target %q: plan %+v points one record at one array for live copies on two", target, plan.Flips)
		assert.Contains(t, err.Error(), "different arrays")
	}
}

// The controls. On failback the objects give no address for the record --
// the promoted side is the record itself -- so -dns-target is the address,
// and one source failing back from two DR arrays is legitimate. Pairs that
// agree take the explicit address over the derived one.
func TestAnExplicitDNSTargetSetsTheAddressWhenThePairsAgree(t *testing.T) {
	t.Run("failback", func(t *testing.T) {
		pairs := []failoverPair{dnsPair("1", "csi-a.lab", "192.0.2.2"), dnsPair("2", "csi-a.lab", "192.0.2.3")}
		plan, err := planFailover(pairs, directionFailback, failoverOptions{namespace: "ns", dnsTTL: 60, podsObserved: true, dnsTarget: "192.0.2.1"})
		require.NoError(t, err)
		require.Len(t, plan.Flips, 1)
		assert.Equal(t, "192.0.2.1", plan.Flips[0].To)
		assert.Equal(t, 2, plan.Flips[0].Volumes)
	})
	t.Run("failover, both live copies on one array", func(t *testing.T) {
		pairs := []failoverPair{dnsPair("1", "csi-a.lab", "b.lab"), dnsPair("2", "csi-a.lab", "b.lab")}
		plan, err := planFailover(pairs, directionFailover, failoverOptions{namespace: "ns", dnsTTL: 60, podsObserved: true, dnsTarget: "192.0.2.9"})
		require.NoError(t, err)
		require.Len(t, plan.Flips, 1)
		assert.Equal(t, "192.0.2.9", plan.Flips[0].To, "the explicit address, not the derived name")
	})
}
