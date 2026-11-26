//go:build openshift

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package openshift

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// openShiftKubeVersion is the Kubernetes version OpenShift Container Platform
// 4.22 reports. Every render passes it explicitly: the pinned Helm's default
// capabilities version is its own build's, not the target cluster's, so a
// render without this answers a question nobody asked.
const openShiftKubeVersion = "1.35.6"

// SCCAPI is the API the chart's `openshift.enabled: auto` detects the platform
// by. Every render here declares it: an offline render sees no cluster, so a
// render without it silently produces the plain-Kubernetes install and every
// assertion below would be about the wrong manifest.
const SCCAPI = "security.openshift.io/v1/SecurityContextConstraints"

// defaultChartPath is the chart, relative to this directory. Override it with
// QCSI_CHART to render a chart from somewhere else -- a packaged one, say.
const defaultChartPath = "../../charts/qumulo-csi-driver"

// ChartPath resolves the chart directory and fails the test if it is not there,
// rather than letting helm produce a less obvious error.
func ChartPath(t *testing.T) string {
	t.Helper()

	path := os.Getenv("QCSI_CHART")
	if path == "" {
		path = defaultChartPath
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("resolving chart path %q: %v", path, err)
	}
	if _, err := os.Stat(filepath.Join(abs, "Chart.yaml")); err != nil {
		t.Fatalf("no Chart.yaml under %s -- set QCSI_CHART to the chart directory: %v", abs, err)
	}

	return abs
}

// ValuesFile resolves one of this suite's committed values files.
func ValuesFile(t *testing.T, name string) string {
	t.Helper()

	abs, err := filepath.Abs(filepath.Join("values", name))
	if err != nil {
		t.Fatalf("resolving values file %q: %v", name, err)
	}
	if _, err := os.Stat(abs); err != nil {
		t.Fatalf("values file %s is missing: %v", abs, err)
	}

	return abs
}

// Options are the inputs to one render.
type Options struct {
	// Release is the Helm release name; it prefixes every object name.
	Release string
	// Namespace is the install namespace.
	Namespace string
	// KubeVersion is what the chart sees as .Capabilities.KubeVersion.
	// Empty means openShiftKubeVersion.
	KubeVersion string
	// ValuesFiles are passed as -f, in order.
	ValuesFiles []string
	// Sets are passed as --set, in order, after the values files.
	Sets []string
	// APIVersions are passed as --api-versions, for the chart's presence
	// checks. OpenShift ships CRDs the chart only reacts to, so a test that
	// cares about those branches has to say so. The SCC API is added for
	// every render unless PlainKubernetes is set.
	APIVersions []string
	// PlainKubernetes renders without the SCC API, which is how the chart
	// decides it is not on OpenShift. Only the cases that compare the two
	// platforms set it.
	PlainKubernetes bool
}

func (o Options) kubeVersion() string {
	if o.KubeVersion == "" {
		return openShiftKubeVersion
	}
	return o.KubeVersion
}

func (o Options) release() string {
	if o.Release == "" {
		return "qcsi"
	}
	return o.Release
}

func (o Options) namespace() string {
	if o.Namespace == "" {
		return "qumulo-csi"
	}
	return o.Namespace
}

// RenderRaw runs `helm template` and returns the manifest, or the error helm
// printed. Tests that expect a render to fail use this one.
func RenderRaw(t *testing.T, opts Options) (string, error) {
	t.Helper()

	args := []string{
		"template", opts.release(), ChartPath(t),
		"--namespace", opts.namespace(),
		"--kube-version", opts.kubeVersion(),
	}
	for _, f := range opts.ValuesFiles {
		args = append(args, "-f", f)
	}
	for _, s := range opts.Sets {
		args = append(args, "--set", s)
	}
	// The SCC API always, plus whatever the case adds. A case that needs the
	// plain-Kubernetes install asks for it explicitly with PlainKubernetes.
	if !opts.PlainKubernetes {
		args = append(args, "--api-versions", SCCAPI)
	}
	for _, a := range opts.APIVersions {
		args = append(args, "--api-versions", a)
	}

	cmd := exec.Command("helm", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("helm template failed: %w\n%s", err, stderr.String())
	}

	return stdout.String(), nil
}

// Render renders the chart and parses it, failing the test on either step.
func Render(t *testing.T, opts Options) Manifest {
	t.Helper()

	out, err := RenderRaw(t, opts)
	if err != nil {
		t.Fatalf("rendering the chart for OpenShift %s: %v", opts.kubeVersion(), err)
	}

	return parse(t, out)
}

// Object is one rendered Kubernetes object, kept as the generic tree helm
// produced. The suite reads a handful of fields out of it rather than binding
// to the Kubernetes Go types, which would pull the whole API machinery in for
// no gain.
type Object map[string]any

// Manifest is everything one render produced.
type Manifest []Object

func parse(t *testing.T, raw string) Manifest {
	t.Helper()

	var out Manifest
	dec := yaml.NewDecoder(strings.NewReader(raw))
	for {
		var doc Object
		err := dec.Decode(&doc)
		if err != nil {
			break
		}
		if len(doc) == 0 {
			continue
		}
		out = append(out, doc)
	}

	if len(out) == 0 {
		t.Fatal("the render produced no objects")
	}

	return out
}

// Kind returns the object's kind, or "" if it has none.
func (o Object) Kind() string { return o.str("kind") }

// Name returns metadata.name.
func (o Object) Name() string {
	meta, ok := asMap(o["metadata"])
	if !ok {
		return ""
	}
	return meta.str("name")
}

// asMap normalises a decoded mapping. yaml.v3 propagates the named map type it
// was handed, so a nested mapping comes back as Object rather than
// map[string]any; both have to be accepted or every lookup silently returns
// nothing -- a type assertion on map[string]any alone reads as correct and
// finds nothing, which is how this went unnoticed for a whole first run.
func asMap(v any) (Object, bool) {
	switch m := v.(type) {
	case Object:
		return m, true
	case map[string]any:
		return Object(m), true
	}
	return nil, false
}

func (o Object) str(key string) string {
	s, _ := o[key].(string)
	return s
}

// Dig walks nested maps and returns the value at the path, or nil.
func (o Object) Dig(path ...string) any {
	var cur any = o
	for _, step := range path {
		m, ok := asMap(cur)
		if !ok {
			return nil
		}
		cur, ok = m[step]
		if !ok {
			return nil
		}
	}
	return cur
}

// Kinds returns every object of the given kind.
func (m Manifest) Kinds(kind string) []Object {
	var out []Object
	for _, o := range m {
		if o.Kind() == kind {
			out = append(out, o)
		}
	}
	return out
}

// Named returns the single object of that kind whose name contains suffix, and
// fails if there is not exactly one -- an ambiguous match is a test bug, not a
// finding.
func (m Manifest) Named(t *testing.T, kind, suffix string) Object {
	t.Helper()

	var hits []Object
	for _, o := range m.Kinds(kind) {
		if strings.HasSuffix(o.Name(), suffix) {
			hits = append(hits, o)
		}
	}

	switch len(hits) {
	case 1:
		return hits[0]
	case 0:
		t.Fatalf("no %s whose name ends in %q; the render has %d of that kind", kind, suffix, len(m.Kinds(kind)))
	default:
		t.Fatalf("%d objects of kind %s end in %q, expected one", len(hits), kind, suffix)
	}

	return nil
}

// APIGroups returns the distinct apiVersion values in the manifest. It is how
// the installation tests check that nothing needs an API group OpenShift would
// have to be extended to provide.
func (m Manifest) APIGroups() []string {
	seen := map[string]bool{}
	var out []string
	for _, o := range m {
		v := o.str("apiVersion")
		if v != "" && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
