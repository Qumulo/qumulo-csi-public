//go:build chart && defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

// Shared helpers for the chart defect tests (TestDefect_*). Every name here
// carries the dfx prefix so it cannot collide with helpers the fix branch adds
// to this package.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// dfxRelease and dfxNamespace are what every render in these tests uses, the
// same as the golden tests.
const (
	dfxRelease   = "qumulo-csi"
	dfxNamespace = "qumulo-csi"
)

// dfxObj is one rendered Kubernetes object.
type dfxObj map[string]any

func (o dfxObj) kind() string { s, _ := o["kind"].(string); return s }

func (o dfxObj) name() string { return dfxStr(dfxGet(o, "metadata", "name")) }

func (o dfxObj) namespace() string { return dfxStr(dfxGet(o, "metadata", "namespace")) }

// dfxGet walks nested maps (string keys) and lists (int keys).
func dfxGet(v any, path ...any) any {
	cur := v
	for _, p := range path {
		switch key := p.(type) {
		case string:
			var m map[string]any
			switch c := cur.(type) {
			case dfxObj:
				m = c
			case map[string]any:
				m = c
			default:
				return nil
			}
			cur = m[key]
		case int:
			l, ok := cur.([]any)
			if !ok || key < 0 || key >= len(l) {
				return nil
			}
			cur = l[key]
		default:
			return nil
		}
	}
	return cur
}

func dfxStr(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func dfxList(v any) []any {
	l, _ := v.([]any)
	return l
}

func dfxMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// dfxHelmEnv is the environment every helm call gets. HOME and the XDG dirs
// point at a temp dir, so a run neither reads nor writes the user's helm or
// kube configuration and caches.
func dfxHelmEnv(t *testing.T) []string {
	t.Helper()
	home := t.TempDir()
	env := []string{}
	for _, kv := range os.Environ() {
		switch {
		case strings.HasPrefix(kv, "HOME="), strings.HasPrefix(kv, "KUBECONFIG="),
			strings.HasPrefix(kv, "XDG_CACHE_HOME="), strings.HasPrefix(kv, "XDG_CONFIG_HOME="),
			strings.HasPrefix(kv, "XDG_DATA_HOME="), strings.HasPrefix(kv, "HELM_"):
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"HOME="+home,
		"XDG_CACHE_HOME="+filepath.Join(home, "cache"),
		"XDG_CONFIG_HOME="+filepath.Join(home, "config"),
		"XDG_DATA_HOME="+filepath.Join(home, "data"),
		"KUBECONFIG="+filepath.Join(home, "no-kubeconfig"),
	)
}

// dfxHelm runs helm with args and returns combined output and the error.
func dfxHelm(t *testing.T, env []string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("helm", args...)
	if env == nil {
		env = dfxHelmEnv(t)
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// dfxTemplateArgs builds `helm template` arguments: --set entries and an
// optional values file body.
func dfxTemplateArgs(t *testing.T, chart string, values string, sets ...string) []string {
	t.Helper()
	args := []string{"template", dfxRelease, chart, "--namespace", dfxNamespace}
	if values != "" {
		path := filepath.Join(t.TempDir(), "values.yaml")
		require.NoError(t, os.WriteFile(path, []byte(values), 0o644))
		args = append(args, "--values", path)
	}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	return args
}

// dfxTryRender renders the chart in this tree and returns the output and the
// error, without failing the test.
func dfxTryRender(t *testing.T, values string, sets ...string) (string, error) {
	t.Helper()
	return dfxHelm(t, nil, dfxTemplateArgs(t, chartPath, values, sets...)...)
}

// dfxRender renders and requires success.
func dfxRender(t *testing.T, values string, sets ...string) []dfxObj {
	t.Helper()
	out, err := dfxTryRender(t, values, sets...)
	require.NoError(t, err, "helm template failed:\n%s", out)
	return dfxParse(t, out)
}

// dfxParse decodes a YAML stream into objects, dropping empty documents.
func dfxParse(t *testing.T, stream string) []dfxObj {
	t.Helper()
	dec := yaml.NewDecoder(bytes.NewBufferString(stream))
	var objs []dfxObj
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "rendered output is not YAML")
		if len(doc) == 0 {
			continue
		}
		objs = append(objs, dfxObj(doc))
	}
	return objs
}

// dfxFind returns the objects of one kind.
func dfxFind(objs []dfxObj, kind string) []dfxObj {
	var out []dfxObj
	for _, o := range objs {
		if o.kind() == kind {
			out = append(out, o)
		}
	}
	return out
}

// dfxOne returns the single object of a kind whose name has the suffix.
func dfxOne(t *testing.T, objs []dfxObj, kind, nameSuffix string) dfxObj {
	t.Helper()
	var hit []dfxObj
	for _, o := range dfxFind(objs, kind) {
		if strings.HasSuffix(o.name(), nameSuffix) {
			hit = append(hit, o)
		}
	}
	require.Len(t, hit, 1, "want exactly one %s named *%s", kind, nameSuffix)
	return hit[0]
}

// dfxContainers returns a workload's pod-template containers by name.
func dfxContainers(o dfxObj) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, c := range dfxList(dfxGet(o, "spec", "template", "spec", "containers")) {
		m := dfxMap(c)
		out[dfxStr(m["name"])] = m
	}
	return out
}

// dfxHasRequests reports whether a container sets any resource request (or a
// limit, which Kubernetes copies to the request).
func dfxHasRequests(c map[string]any) bool {
	return len(dfxMap(dfxGet(c, "resources", "requests"))) > 0 ||
		len(dfxMap(dfxGet(c, "resources", "limits"))) > 0
}

// dfxBaseClass is one valid shared-subdirectory class, the minimum most tests
// need on top of their own settings.
var dfxBaseClass = []string{
	"storageClasses[0].name=qumulo-nfs",
	"storageClasses[0].server=qumulo.example.com",
	"storageClasses[0].secretName=qumulo-token",
	"storageClasses[0].basePath=/csi/volumes",
}

// dfxSets joins set lists.
func dfxSets(lists ...[]string) []string {
	var out []string
	for _, l := range lists {
		out = append(out, l...)
	}
	return out
}

// dfxRepoRoot is the repository root of this tree.
func dfxRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	return root
}
