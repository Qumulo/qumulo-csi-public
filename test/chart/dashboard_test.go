//go:build chart

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type configMap struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name        string            `yaml:"name"`
		Namespace   string            `yaml:"namespace"`
		Labels      map[string]string `yaml:"labels"`
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	Data map[string]string `yaml:"data"`
}

func renderDashboards(t *testing.T, sets ...string) []configMap {
	t.Helper()
	args := []string{"template", "qumulo-csi", chartPath, "--namespace", "qumulo-csi"}
	for _, s := range sets {
		// "string:" sends the value as a string (a label "true" stays a string).
		if v, ok := strings.CutPrefix(s, "string:"); ok {
			args = append(args, "--set-string", v)
		} else {
			args = append(args, "--set", s)
		}
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	require.NoError(t, err, "helm template failed: %s", out)
	var found []configMap
	for _, doc := range strings.Split(string(out), "\n---\n") {
		var cm configMap
		if yaml.Unmarshal([]byte(doc), &cm) == nil && cm.Kind == "ConfigMap" && strings.HasSuffix(cm.Metadata.Name, "-dashboard") {
			found = append(found, cm)
		}
	}
	return found
}

// The dashboard ships in the chart, off by default, and only alongside the
// metrics it charts: a dashboard with metrics off would be all empty panels.
func TestDashboardRendersOnlyWhenAskedForWithMetrics(t *testing.T) {
	assert.Empty(t, renderDashboards(t), "the dashboard must be off by default")
	assert.Empty(t, renderDashboards(t, "metrics.dashboard.enabled=true"), "no dashboard without metrics.enabled")
	assert.Empty(t, renderDashboards(t, "metrics.enabled=true"), "metrics alone must not add the dashboard")
}

// What an importer receives: the chart's JSON byte for byte, under the label
// the kube-prometheus-stack Grafana sidecar watches by default, in the
// release namespace unless told otherwise.
func TestDashboardConfigMapCarriesTheShippedJSON(t *testing.T) {
	want, err := os.ReadFile(filepath.Join(chartPath, "dashboards", "qumulo-csi.json"))
	require.NoError(t, err)

	cms := renderDashboards(t, "metrics.enabled=true", "metrics.dashboard.enabled=true")
	require.Len(t, cms, 1)
	cm := cms[0]
	assert.Equal(t, "qumulo-csi", cm.Metadata.Namespace)
	assert.Equal(t, "1", cm.Metadata.Labels["grafana_dashboard"])
	got := cm.Data["qumulo-csi.json"]
	assert.Equal(t, strings.TrimRight(string(want), "\n"), got, "the ConfigMap must carry dashboards/qumulo-csi.json unchanged")
	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(got), &parsed))
	assert.Equal(t, "Qumulo CSI driver", parsed["title"])
}

// The OpenShift console imports ConfigMaps from openshift-config-managed that
// carry its own label; both are settable without editing the template.
func TestDashboardFollowsTheOpenShiftConsoleSettings(t *testing.T) {
	cms := renderDashboards(t, "metrics.enabled=true", "metrics.dashboard.enabled=true",
		"metrics.dashboard.namespace=openshift-config-managed",
		`string:metrics.dashboard.labels.console\.openshift\.io/dashboard=true`,
		`metrics.dashboard.annotations.grafana_folder=Storage`)
	require.Len(t, cms, 1)
	cm := cms[0]
	assert.Equal(t, "openshift-config-managed", cm.Metadata.Namespace)
	assert.Equal(t, "true", cm.Metadata.Labels["console.openshift.io/dashboard"])
	assert.Equal(t, "Storage", cm.Metadata.Annotations["grafana_folder"])
}

// The OpenShift console draws a dashboard ConfigMap with its own legacy
// renderer (monitoring-plugin, LegacyDashboardsPage), which knows only these
// panel types and returns nothing for any other: a Grafana "timeseries" or
// "stat" panel is skipped without an error, so the page looks half empty and
// nothing anywhere says why. Grafana migrates "graph" to "timeseries" and
// "singlestat" to "stat" on load, so these types serve both importers.
var consolePanelTypes = map[string]bool{
	"row": true, "graph": true, "singlestat": true, "gauge": true, "table": true, "grafana-piechart-panel": true,
}

// The console formats a value with these units only (anything else is shown
// as a plain number, which is harmless for reqps but wrong for sizes and
// times named some other way).
var consoleUnits = map[string]bool{
	"short": true, "none": true, "percentunit": true, "bytes": true, "Bytes": true,
	"bps": true, "Bps": true, "pps": true, "ms": true, "s": true, "reqps": true,
}

type dashPanel struct {
	ID      int    `json:"id"`
	Type    string `json:"type"`
	Title   string `json:"title"`
	Format  string `json:"format"`
	Targets []struct {
		Expr string `json:"expr"`
	} `json:"targets"`
	Yaxes []struct {
		Format string `json:"format"`
	} `json:"yaxes"`
	Panels []dashPanel `json:"panels"`
}

func shippedPanels(t *testing.T) []dashPanel {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(chartPath, "dashboards", "qumulo-csi.json"))
	require.NoError(t, err)
	var d struct {
		Panels []dashPanel `json:"panels"`
	}
	require.NoError(t, json.Unmarshal(raw, &d))
	var all []dashPanel
	for _, p := range d.Panels {
		all = append(all, p)
		all = append(all, p.Panels...) // a collapsed row nests its panels
	}
	require.NotEmpty(t, all)
	return all
}

func TestDashboardPanelsAreTypesTheOpenShiftConsoleDraws(t *testing.T) {
	for _, p := range shippedPanels(t) {
		assert.True(t, consolePanelTypes[p.Type],
			"panel %d %q is type %q, which the OpenShift console skips without an error; use graph, singlestat or table", p.ID, p.Title, p.Type)
		if p.Type == "row" {
			continue
		}
		assert.NotEmpty(t, p.Targets, "panel %d %q has no query: the console skips it", p.ID, p.Title)
		for _, tg := range p.Targets {
			assert.NotEmpty(t, strings.TrimSpace(tg.Expr), "panel %d %q has an empty query", p.ID, p.Title)
		}
		if p.Type == "graph" {
			require.NotEmpty(t, p.Yaxes, "graph %d %q has no yaxes: the console reads its unit from yaxes[0].format", p.ID, p.Title)
			assert.True(t, consoleUnits[p.Yaxes[0].Format], "graph %d %q unit %q is not one the console formats", p.ID, p.Title, p.Yaxes[0].Format)
		}
		if p.Type == "singlestat" {
			// The console shows only the first series of a singlestat query.
			assert.Len(t, p.Targets, 1, "singlestat %d %q: the console reads one query", p.ID, p.Title)
			assert.Regexp(t, `^\s*(min|max|sum|avg|count)\s*\(`, p.Targets[0].Expr,
				"singlestat %d %q must aggregate to one series: the console shows only the first", p.ID, p.Title)
		}
	}
}

// process_* and go_* are exported by every Go program the cluster monitoring
// scrapes; unfiltered, a panel charts all of them, not the driver.
func TestDashboardProcessPanelsAreFilteredToTheDriver(t *testing.T) {
	generic := regexp.MustCompile(`\b(process|go)_[a-z_]+`)
	for _, p := range shippedPanels(t) {
		for _, tg := range p.Targets {
			if generic.MatchString(tg.Expr) {
				assert.Contains(t, tg.Expr, "csi_qumulo_build_info",
					"panel %d %q queries a process-wide series without restricting it to the driver's containers", p.ID, p.Title)
			}
		}
	}
}
