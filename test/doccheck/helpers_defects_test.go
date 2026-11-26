//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package doccheck holds the documentation-review defect tests: each one
// reads a doc (and, where the claim is about behaviour, the code or the
// rendered chart) as files relative to the repo root, and fails while the
// doc says something the code contradicts.
package doccheck

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Paths used by many tests, relative to the repo root.
const (
	dfxChartREADME  = "charts/qumulo-csi-driver/README.md"
	dfxValuesYAML   = "charts/qumulo-csi-driver/values.yaml"
	dfxValuesSchema = "charts/qumulo-csi-driver/values.schema.json"
	dfxMetricsTpl   = "charts/qumulo-csi-driver/templates/metrics.yaml"
	dfxChartDir     = "charts/qumulo-csi-driver"
	dfxRootREADME   = "README.md"
)

// dfxRoot is the repo root: the nearest directory above the test's working
// directory that holds go.mod.
func dfxRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod above the test directory")
		}
		dir = parent
	}
}

// dfxRead returns a repo file's text. A missing file fails the test.
func dfxRead(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dfxRoot(t), rel))
	if err != nil {
		t.Fatalf("reading %s: %v", rel, err)
	}
	return string(b)
}

// dfxExists reports whether a repo path exists.
func dfxExists(t *testing.T, rel string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dfxRoot(t), rel))
	return err == nil
}

var dfxSpace = regexp.MustCompile(`\s+`)

// dfxFlat collapses every run of whitespace (line wraps included) to one
// space, and drops a Go comment's leading "//" and a YAML comment's "#", so
// a phrase can be matched however the doc happens to wrap it.
func dfxFlat(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		l := strings.TrimSpace(line)
		l = strings.TrimPrefix(l, "//")
		if strings.HasPrefix(l, "#") && !strings.HasPrefix(l, "##") {
			l = strings.TrimPrefix(l, "#")
		}
		b.WriteString(l)
		b.WriteString(" ")
	}
	return strings.TrimSpace(dfxSpace.ReplaceAllString(b.String(), " "))
}

// dfxReadFlat is dfxRead then dfxFlat.
func dfxReadFlat(t *testing.T, rel string) string {
	t.Helper()
	return dfxFlat(dfxRead(t, rel))
}

// dfxRow returns every line of a doc that contains anchor (a table row is
// one line), joined; "" when none does.
func dfxRow(t *testing.T, rel, anchor string) string {
	t.Helper()
	var rows []string
	for _, line := range strings.Split(dfxRead(t, rel), "\n") {
		if strings.Contains(line, anchor) {
			rows = append(rows, line)
		}
	}
	return strings.Join(rows, "\n")
}

// dfxParagraph returns the blank-line-delimited block of a doc that
// contains anchor, flattened; "" when no block does. Every matching block
// is returned, joined, so a phrase repeated in two places is checked in
// both.
func dfxParagraph(t *testing.T, rel, anchor string) string {
	t.Helper()
	text := dfxRead(t, rel)
	var out []string
	for _, block := range regexp.MustCompile(`\n[ \t]*\n`).Split(text, -1) {
		if strings.Contains(dfxFlat(block), anchor) || strings.Contains(block, anchor) {
			out = append(out, dfxFlat(block))
		}
	}
	return strings.Join(out, "\n")
}

// dfxBetween returns the flattened text of a doc from the first line
// matching start up to (not including) the first later line matching end;
// "" when start is not found.
func dfxBetween(t *testing.T, rel, start, end string) string {
	t.Helper()
	lines := strings.Split(dfxRead(t, rel), "\n")
	startRe, endRe := regexp.MustCompile(start), regexp.MustCompile(end)
	for i, line := range lines {
		if !startRe.MatchString(line) {
			continue
		}
		j := i + 1
		for ; j < len(lines); j++ {
			if endRe.MatchString(lines[j]) {
				break
			}
		}
		return dfxFlat(strings.Join(lines[i:j], "\n"))
	}
	return ""
}

// dfxMatch reports whether the case-insensitive regexp re matches s.
func dfxMatch(re, s string) bool {
	return regexp.MustCompile(`(?i)` + re).MatchString(s)
}

// dfxClip shortens s for a failure message.
func dfxClip(s string) string {
	if len(s) > 600 {
		return s[:600] + " ..."
	}
	return s
}

// dfxGoSource returns the concatenated non-test Go files of a package dir.
func dfxGoSource(t *testing.T, dir string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dfxRoot(t), dir, "*.go"))
	if err != nil {
		t.Fatalf("glob %s: %v", dir, err)
	}
	var b strings.Builder
	for _, m := range matches {
		if strings.HasSuffix(m, "_test.go") {
			continue
		}
		data, err := os.ReadFile(m)
		if err != nil {
			t.Fatalf("reading %s: %v", m, err)
		}
		b.Write(data)
		b.WriteString("\n")
	}
	return b.String()
}

// dfxRender runs `helm template` on the chart with an extra values file
// (YAML text, "" for none) and returns the rendered objects, or the error
// output when the render fails.
func dfxRender(t *testing.T, release, values string) ([]map[string]any, string, error) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Fatalf("helm is not on PATH; this check renders the chart: %v", err)
	}
	args := []string{"template", release, filepath.Join(dfxRoot(t), dfxChartDir),
		"--namespace", "qumulo-csi", "--kube-version", "1.36.0"}
	if values != "" {
		f := filepath.Join(t.TempDir(), "values.yaml")
		if err := os.WriteFile(f, []byte(values), 0o600); err != nil {
			t.Fatalf("writing values: %v", err)
		}
		args = append(args, "-f", f)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		return nil, string(out), err
	}
	var objs []map[string]any
	dec := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			break
		}
		if obj != nil {
			objs = append(objs, obj)
		}
	}
	return objs, string(out), nil
}

// dfxContainers returns the containers of the first rendered object of the
// given kind, keyed by container name.
func dfxContainers(t *testing.T, objs []map[string]any, kind string) map[string]map[string]any {
	t.Helper()
	for _, o := range objs {
		if o["kind"] != kind {
			continue
		}
		spec, _ := o["spec"].(map[string]any)
		tmpl, _ := spec["template"].(map[string]any)
		podSpec, _ := tmpl["spec"].(map[string]any)
		list, _ := podSpec["containers"].([]any)
		out := map[string]map[string]any{}
		for _, c := range list {
			cm, _ := c.(map[string]any)
			name, _ := cm["name"].(string)
			out[name] = cm
		}
		return out
	}
	t.Fatalf("no rendered %s", kind)
	return nil
}
