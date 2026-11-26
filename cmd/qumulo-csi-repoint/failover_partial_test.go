// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A promote that fails part way through a namespace stops the run, and
// the promotes before it are not undone: that is by design (executePromote --
// a promote is the one step that must not be half-reversed by a tool that has
// just seen the controller fail). What the run owes the operator instead is
// the list of what it already promoted, so the next move is taken knowing
// which copies are now writable.
func TestExecuteFailoverNamesEveryPairAlreadyPromotedWhenALaterPromoteFails(t *testing.T) {
	plan := planFor(t, "qumulo.example.com", directionFailover, failoverOptions{restartPods: true})
	require.Len(t, plan.Promote, 1)
	second := plan.Promote[0]
	second.Name, second.Claim = "logs-dr-vr", "logs-dr"
	plan.Promote = append(plan.Promote, second)

	ex := &recordingExecutor{
		observations: map[string][]volumeReplication{
			"apps/data-dr-vr": {healthyAt(replicationStatusPrimary)},
			// apps/logs-dr-vr never reaches Primary.
		},
	}

	var out bytes.Buffer
	err := executeFailover(context.Background(), plan, ex, &out)
	t.Logf("err: %v\noutput:\n%s", err, out.String())

	require.Error(t, err, "a failed promote is a failed run")
	assert.Contains(t, err.Error(), "apps/logs-dr-vr", "the error names the promote that failed")
	assert.Contains(t, err.Error(), "already promoted",
		"the error says which pairs this run promoted before it stopped")
	assert.Contains(t, err.Error(), "apps/data-dr-vr")
	assert.Contains(t, err.Error(), "not undone")

	assert.Equal(t, []string{
		"set apps/data-dr-vr primary autoResync=false",
		"await apps/data-dr-vr Primary",
		"set apps/logs-dr-vr primary autoResync=false",
		"await apps/logs-dr-vr Primary",
	}, ex.calls, "nothing is demoted, flipped or rolled back after the failure")

	summary := out.String()
	assert.Contains(t, summary, "apps/data-dr-vr")
	assert.Contains(t, summary, "secondary -> primary", "the summary records the promote that landed")
	assert.Contains(t, summary, "DID NOT COMPLETE")
	assert.NotContains(t, strings.ToLower(summary), "rolled back")
}
