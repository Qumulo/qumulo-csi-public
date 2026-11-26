// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The summary exists because of one measurement that was quoted four times.
//
// The lab's "63 s one-command failover" was an idempotent re-run: every object
// was already in its target state, so the run promoted nothing, demoted
// nothing, and spent 3 s on checks plus a 60 s TTL wait. The run that did the
// work took 39 s and moved one object of four. Nothing in the tool's output
// made either of those readable -- it printed an "achieved state" per object and
// never said which of them had changed.

func TestClassifyRun(t *testing.T) {
	for _, tc := range []struct {
		name             string
		promotes, moved  int
		wantRun, wantSay string
	}{
		{"cold: every promote moved", 4, 4, runCold, "COLD RUN"},
		{"rerun: nothing moved", 4, 0, runRerun, "RE-RUN"},
		{"partial: some moved", 4, 1, runPartial, "PARTIAL RUN"},
		{"nothing to promote", 0, 0, runNone, "no promote at all"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run, reason := classifyRun(tc.promotes, tc.moved)
			assert.Equal(t, tc.wantRun, run)
			assert.Contains(t, reason, tc.wantSay)
		})
	}

	// The sentence a reader has to see on the run that was misquoted.
	_, reason := classifyRun(4, 0)
	assert.Contains(t, reason, "NOT a failover")
}

func TestTransitionOf(t *testing.T) {
	transition, changed := transitionOf("Secondary", true, replicationStatusPrimary, true)
	assert.Equal(t, "secondary -> primary", transition)
	assert.True(t, changed)

	transition, changed = transitionOf("Primary", true, replicationStatusPrimary, true)
	assert.Equal(t, "already primary, no change", transition)
	assert.False(t, changed)

	// `status.state` without `Completed` is not "already there": the promote is
	// still running, and a run that called that no change would report the work
	// it did as nothing.
	transition, changed = transitionOf("Primary", false, replicationStatusPrimary, true)
	assert.Equal(t, "primary -> primary", transition)
	assert.True(t, changed)

	transition, _ = transitionOf("", false, replicationStatusPrimary, false)
	assert.Equal(t, "unknown -> primary DID NOT COMPLETE", transition)
}

// summaryOf runs one plan against the recorder and decodes the machine-readable
// line, which is what a lab harness records.
func summaryOf(t *testing.T, plan failoverPlan, ex *recordingExecutor) (runSummary, string) {
	t.Helper()

	var out bytes.Buffer
	require.NoError(t, executeFailover(context.Background(), plan, ex, &out))

	var encoded string
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(line, summaryPrefix) {
			encoded = strings.TrimPrefix(line, summaryPrefix)
		}
	}
	require.NotEmpty(t, encoded, "no machine-readable summary line in:\n%s", out.String())

	var summary runSummary
	require.NoError(t, json.Unmarshal([]byte(encoded), &summary))
	return summary, out.String()
}

// A cold run: the DR side was Secondary and is now Primary. This is the only
// shape whose wall-clock number may be quoted as a failover.
func TestSummaryClassifiesAColdRun(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})
	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {healthyAt(replicationStatusSecondary)},
		},
	}

	summary, printed := summaryOf(t, plan, ex)

	assert.Equal(t, runCold, summary.Run)
	assert.Equal(t, 1, summary.Promotes)
	assert.Equal(t, 1, summary.PromotesChanged)
	assert.Contains(t, printed, "run: COLD")

	promote := transitionFor(t, summary, "promote", "apps/data-dr-vr")
	assert.Equal(t, "secondary -> primary", promote.Transition)
	assert.True(t, promote.Changed)
}

// The run that was recorded as "63 s": every object already in its target
// state, nothing promoted, nothing demoted. The output now says so in its first
// line and in every transition.
func TestSummaryClassifiesAnIdempotentRerun(t *testing.T) {
	fixture := failoverFixture(t, "qumulo.example.com")
	// The DR side is already Primary and the original already Secondary, which
	// is what the third lab attempt found.
	fixture.Replications = []volumeReplication{
		aVolumeReplication("data-vr", "data", replicationStateSecondary, replicationStatusSecondary, nil),
		aVolumeReplication("data-dr-vr", "data-dr", replicationStatePrimary, replicationStatusPrimary, nil),
	}
	pairs, err := derivePairs(fixture, "apps", "csi.qumulo.com")
	require.NoError(t, err)
	plan, err := planFailover(pairs, directionFailover, failoverOptions{
		namespace: "apps", dnsTTL: 60, restartPods: true, podsObserved: true,
	})
	require.NoError(t, err)

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {healthyAt(replicationStatusSecondary)},
		},
	}

	summary, printed := summaryOf(t, plan, ex)

	assert.Equal(t, runRerun, summary.Run)
	assert.Equal(t, 1, summary.Promotes)
	assert.Zero(t, summary.PromotesChanged)
	assert.Contains(t, printed, "run: RERUN")
	assert.Contains(t, printed, "NOT a failover")

	promote := transitionFor(t, summary, "promote", "apps/data-dr-vr")
	assert.Equal(t, "already primary, no change", promote.Transition)
	assert.False(t, promote.Changed)

	demote := transitionFor(t, summary, "demote", "apps/data-vr")
	assert.Equal(t, "already secondary, no change", demote.Transition)
	assert.False(t, demote.Changed)
}

// The 39 s run: one object moved, the others had already been moved by an
// earlier attempt and by hand. Neither "cold" nor "re-run" describes it, and
// calling it either is how the number was misread.
func TestSummaryClassifiesAPartialRun(t *testing.T) {
	pairs, err := derivePairs(groupFixture(t), "apps", "csi.qumulo.com")
	require.NoError(t, err)
	plan, err := planFailover(pairs, directionFailover, failoverOptions{
		namespace: "apps", dnsTTL: 60, restartPods: true, podsObserved: true,
	})
	require.NoError(t, err)

	// One more promote in the plan, already in its target state.
	plan.Promote = append(plan.Promote, replicationChange{
		Namespace: "apps", Name: "already-vr", Claim: "other-dr",
		To: replicationStatePrimary, WantState: replicationStatusPrimary,
		FromState: replicationStatusPrimary, FromCompleted: true,
	})

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/r12-shared-group-dr": {healthyAt(replicationStatusPrimary)},
			"apps/already-vr":          {healthyAt(replicationStatusPrimary)},
			"apps/r12-shared-group":    {healthyAt(replicationStatusSecondary)},
		},
	}

	summary, printed := summaryOf(t, plan, ex)

	assert.Equal(t, runPartial, summary.Run)
	assert.Equal(t, 2, summary.Promotes)
	assert.Equal(t, 1, summary.PromotesChanged)
	assert.Contains(t, printed, "PARTIAL RUN: 1 of 2")
}

// Every numbered step is timed, including the two new ones, and each object
// carries its own elapsed time.
func TestSummaryTimesEveryPhaseAndEveryObject(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})
	plan.Quiesce = quiescePlan{
		Observed: true,
		Owners: []quiesceOwner{
			{Kind: kindDeployment, Namespace: "apps", Name: "web", Replicas: 2, Restore: 2},
		},
	}
	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			"apps/data-vr":    {healthyAt(replicationStatusSecondary)},
		},
	}

	summary, printed := summaryOf(t, plan, ex)

	names := map[string]bool{}
	for _, phase := range summary.Phases {
		names[phase.Name] = true
	}
	assert.Equal(t, map[string]bool{
		"quiesce": true, "promote": true, "demote": true, "dns": true, "restore": true,
	}, names)

	steps := map[string]bool{}
	for _, tr := range summary.Transitions {
		steps[tr.Step] = true
	}
	assert.True(t, steps["quiesce"], "the quiesce is its own numbered step with its own timing")
	assert.True(t, steps["restore"], "and so is the restore")

	assert.Contains(t, printed, "step 0 quiesce")
	assert.Contains(t, printed, "step 4 restore")
	assert.Contains(t, printed, "quiesce apps/Deployment/web: 2 -> 0 replicas")
	assert.Contains(t, printed, "restore apps/Deployment/web: 0 -> 2 replicas")
}

func transitionFor(t *testing.T, summary runSummary, step, object string) objectTransition {
	t.Helper()
	for _, tr := range summary.Transitions {
		if tr.Step == step && tr.Object == object {
			return tr
		}
	}
	t.Fatalf("no %s transition for %s in %+v", step, object, summary.Transitions)
	return objectTransition{}
}
