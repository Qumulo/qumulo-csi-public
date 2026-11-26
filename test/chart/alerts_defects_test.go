//go:build chart && defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

// Alert-rule defects, decided by evaluating the rendered PrometheusRule's
// expressions over sample series with promtool (the same pinned binary
// `make alert-rules-test` uses). Each check evaluates count(<alert expr>) at
// several times: empty means the alert's condition does not hold, 1 means it
// does. The rule's `for:` is deliberately not part of this -- a condition that
// holds at all is the defect the reports describe.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// dfxPromtool finds promtool: $DFX_PROMTOOL, then PATH, then the pinned copy
// `make alert-rules-test` downloads into bin/ of this tree or of the main
// checkout this worktree belongs to. Never skipped: a missing binary fails.
func dfxPromtool(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("DFX_PROMTOOL"); p != "" {
		return p
	}
	if p, err := exec.LookPath("promtool"); err == nil {
		return p
	}
	roots := []string{dfxRepoRoot(t)}
	if out, err := exec.Command("git", "-C", dfxRepoRoot(t), "rev-parse", "--path-format=absolute", "--git-common-dir").Output(); err == nil {
		roots = append(roots, filepath.Dir(strings.TrimSpace(string(out))))
	}
	for _, root := range roots {
		if found, _ := filepath.Glob(filepath.Join(root, "bin", "promtool-*")); len(found) > 0 {
			return found[len(found)-1]
		}
	}
	t.Fatalf("promtool not found: set DFX_PROMTOOL, put it on PATH, or run `make alert-rules-test` once to fetch the pinned copy")
	return ""
}

// dfxAlertExprs renders the chart with the PrometheusRule on and returns each
// alert's expression by name.
func dfxAlertExprs(t *testing.T, sets ...string) map[string]string {
	t.Helper()
	objs := dfxRender(t, "", dfxSets(dfxBaseClass, []string{
		"metrics.enabled=true", "metrics.forceEnable=true", "metrics.prometheusRule.enabled=true",
	}, sets)...)
	rules := dfxFind(objs, "PrometheusRule")
	require.Len(t, rules, 1, "want exactly one PrometheusRule")
	exprs := map[string]string{}
	for _, g := range dfxList(dfxGet(rules[0], "spec", "groups")) {
		for _, r := range dfxList(dfxMap(g)["rules"]) {
			rule := dfxMap(r)
			if name := dfxStr(rule["alert"]); name != "" {
				exprs[name] = dfxStr(rule["expr"])
			}
		}
	}
	return exprs
}

// dfxSeries is one input series in promtool notation.
type dfxSeries struct{ series, values string }

// dfxCondition evaluates count(expr) at each minute in at, over the series,
// and checks it is 1 when want is true and empty when false. It returns
// promtool's output and whether every evaluation matched.
func dfxCondition(t *testing.T, expr string, series []dfxSeries, at []int, want bool) (string, bool) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "rules.yaml"), []byte("groups: []\n"), 0o644))

	var b strings.Builder
	b.WriteString("rule_files: [rules.yaml]\nevaluation_interval: 1m\ntests:\n  - interval: 1m\n    input_series:\n")
	for _, s := range series {
		fmt.Fprintf(&b, "      - series: %q\n        values: %q\n", s.series, s.values)
	}
	b.WriteString("    promql_expr_test:\n")
	for _, m := range at {
		fmt.Fprintf(&b, "      - expr: %q\n        eval_time: %dm\n", "count("+expr+")", m)
		if want {
			b.WriteString("        exp_samples:\n          - labels: '{}'\n            value: 1\n")
		} else {
			b.WriteString("        exp_samples: []\n")
		}
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "test.yaml"), []byte(b.String()), 0o644))

	cmd := exec.Command(dfxPromtool(t), "test", "rules", "test.yaml")
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil && !strings.Contains(string(out), "FAILED") {
		t.Fatalf("promtool did not run the test (%v):\n%s\n--- test file ---\n%s", err, out, b.String())
	}
	return string(out), err == nil
}

const (
	dfxAgeMetric  = "csi_qumulo_replication_recovery_point_age_seconds"
	dfxModeMetric = "csi_qumulo_replication_mode"
	dfxPolicyMode = "REPLICATION_SNAPSHOT_POLICY"
)

// dfxHourlyClimb is a healthy hourly policy-mode recovery point: 5 minutes
// old after a replicated snapshot, one minute older each minute, to an hour.
const dfxHourlyClimb = "300+60x55"

// TestDefect_HealthyPolicyModePairDoesNotMeetStaleCondition: On a healthy
// policy-mode relationship, seen with its mode by the
// source's controller, the QumuloCSIReplicationRecoveryPointStale condition
// must not hold while the age climbs to the schedule interval; a really stale
// one must still meet it. (templates/metrics.yaml:177-178.)
func TestDefect_HealthyPolicyModePairDoesNotMeetStaleCondition(t *testing.T) {
	expr := dfxAlertExprs(t)["QumuloCSIReplicationRecoveryPointStale"]
	require.NotEmpty(t, expr)
	mode := dfxSeries{dfxModeMetric + `{relationship_id="r1",mode="` + dfxPolicyMode + `"}`, "1x60"}

	out, ok := dfxCondition(t, expr, []dfxSeries{
		{dfxAgeMetric + `{relationship_id="r1",role="source"}`, dfxHourlyClimb}, mode,
	}, []int{10, 20, 30, 40, 50, 55}, false)
	require.True(t, ok, "the stale condition holds on a healthy hourly policy-mode pair:\n%s", out)

	out, ok = dfxCondition(t, expr, []dfxSeries{
		{dfxAgeMetric + `{relationship_id="r1",role="source"}`, "20000x60"}, mode,
	}, []int{10, 30}, true)
	require.True(t, ok, "a policy-mode pair 5.5 h behind does not meet the stale condition:\n%s", out)
}

// TestDefect_DRSideAndLongSchedulesDoNotAlertWhileHealthy: The policy-mode
// threshold must fit the relationship: (1) a DR-side
// controller, which sees only target ends and so no mode series, must not
// meet the stale condition on a healthy hourly policy pair -- and must still
// on one hours behind; (2) a healthy daily schedule must not meet it, either
// because the threshold is derived from the interval or because it is
// configurable and documented for long schedules.
// (templates/metrics.yaml:220-227,
// pkg/metrics/replication.go:82-84.)
func TestDefect_DRSideAndLongSchedulesDoNotAlertWhileHealthy(t *testing.T) {
	t.Run("DR-side controller, healthy hourly policy pair", func(t *testing.T) {
		expr := dfxAlertExprs(t)["QumuloCSIReplicationRecoveryPointStale"]
		require.NotEmpty(t, expr)
		out, ok := dfxCondition(t, expr, []dfxSeries{
			{dfxAgeMetric + `{relationship_id="r1",role="target"}`, dfxHourlyClimb},
		}, []int{10, 20, 30, 40, 50, 55}, false)
		require.True(t, ok, "a DR-side controller (target end, no mode series) meets the stale condition on a "+
			"healthy hourly policy pair for most of every hour:\n%s", out)

		out, ok = dfxCondition(t, expr, []dfxSeries{
			{dfxAgeMetric + `{relationship_id="r1",role="target"}`, "30000x60"},
		}, []int{10, 30}, true)
		require.True(t, ok, "a DR-side relationship 8 h behind no longer meets the stale condition:\n%s", out)
	})

	t.Run("daily policy schedule", func(t *testing.T) {
		const age = "80000x60" // late in a healthy daily cycle (86400 s)
		series := []dfxSeries{
			{dfxAgeMetric + `{relationship_id="d1",role="source"}`, age},
			{dfxModeMetric + `{relationship_id="d1",mode="` + dfxPolicyMode + `"}`, "1x60"},
		}
		expr := dfxAlertExprs(t)["QumuloCSIReplicationRecoveryPointStale"]
		if _, ok := dfxCondition(t, expr, series, []int{10, 30}, false); ok {
			return // derived from the schedule: nothing to configure
		}
		// Not derived: then the threshold must be configurable for a long
		// schedule, and the chart must say how to.
		expr = dfxAlertExprs(t, "metrics.prometheusRule.policyModeRecoveryPointAgeSeconds=129600")["QumuloCSIReplicationRecoveryPointStale"]
		out, ok := dfxCondition(t, expr, series, []int{10, 30}, false)
		require.True(t, ok, "a healthy daily policy pair meets the stale condition even with the policy-mode "+
			"threshold set to 1.5 days:\n%s", out)
		values, err := os.ReadFile(filepath.Join(chartPath, "values.yaml"))
		require.NoError(t, err)
		require.True(t, dfxNear(string(values), "policyModeRecoveryPointAgeSeconds", "schedule", 600),
			"values.yaml does not tell the operator to fit policyModeRecoveryPointAgeSeconds to the schedule")
	})
}

// TestDefect_PrivilegeAlertIgnoresDroppablePrivileges:
// QumuloCSIPrivilegeMissing must not hold for a 403 on a call the chart
// README says may be refused (resolving a file ID, PRIVILEGE_ANALYTICS_READ,
// which may be dropped), and must still hold for a privilege that is
// required. (templates/metrics.yaml:180-182; chart README privilege section.)
func TestDefect_PrivilegeAlertIgnoresDroppablePrivileges(t *testing.T) {
	readme := dfxReadme(t)
	require.True(t, strings.Contains(readme, "PRIVILEGE_ANALYTICS_READ` can be dropped"),
		"the chart README no longer calls PRIVILEGE_ANALYTICS_READ droppable; revisit this test")
	expr := dfxAlertExprs(t)["QumuloCSIPrivilegeMissing"]
	require.NotEmpty(t, expr)
	const counter = "csi_qumulo_api_requests_total"

	out, ok := dfxCondition(t, expr, []dfxSeries{
		{counter + `{cluster="a.example:8000",method="POST",path_template="/v1/files/resolve",status="403"}`, "0+1x30"},
	}, []int{5, 12, 20}, false)
	require.True(t, ok, "the privilege alert's condition holds for PRIVILEGE_ANALYTICS_READ, which the README "+
		"says may be dropped:\n%s", out)

	out, ok = dfxCondition(t, expr, []dfxSeries{
		{counter + `{cluster="a.example:8000",method="GET",path_template="/v2/replication/source-relationships/status/",status="403"}`, "0+1x30"},
	}, []int{5, 12}, true)
	require.True(t, ok, "the privilege alert no longer holds for a required privilege:\n%s", out)
}
