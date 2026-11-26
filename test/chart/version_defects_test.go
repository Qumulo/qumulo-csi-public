//go:build chart && defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dfxChartPackageTargets are the make targets accepted as "package the chart
// for one build".
var dfxChartPackageTargets = []string{"chart-package", "helm-package", "package-chart"}

// dfxPackageChart runs the build's chart-packaging step for a given commit
// into a temp dir, and returns the packaged chart's path. The recipe is taken
// from `make -n` and run with the destination redirected, so nothing is
// written into the tree.
func dfxPackageChart(t *testing.T, commit string) string {
	t.Helper()
	root := dfxRepoRoot(t)
	var recipe string
	for _, target := range dfxChartPackageTargets {
		out, err := exec.Command("make", "-s", "-n", "-C", root, target, "GIT_COMMIT="+commit).CombinedOutput()
		if err == nil && strings.Contains(string(out), "helm package") {
			recipe = string(out)
			break
		}
	}
	require.NotEmpty(t, recipe, "no make target (%v) packages the chart for one build: every build "+
		"is chart and app version %s", dfxChartPackageTargets, "0.1.0")

	// Join continuation lines, then rebuild the helm call with the
	// destination replaced, and run it directly (no shell).
	recipe = strings.ReplaceAll(recipe, "\\\n", " ")
	var fields []string
	for _, l := range strings.Split(recipe, "\n") {
		if strings.Contains(l, "helm package") {
			fields = strings.Fields(l)
		}
	}
	require.NotEmpty(t, fields)
	require.Equal(t, "helm", fields[0], "unexpected packaging recipe: %v", fields)
	dest := t.TempDir()
	args := []string{}
	for i := 1; i < len(fields); i++ {
		if fields[i] == "--destination" || fields[i] == "-d" {
			i++
			continue
		}
		if strings.HasPrefix(fields[i], "--destination=") {
			continue
		}
		args = append(args, fields[i])
	}
	args = append(args, "--destination", dest)
	cmd := exec.Command("helm", args...)
	cmd.Dir = root
	cmd.Env = dfxHelmEnv(t)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "the packaging recipe failed: helm %v\n%s", args, out)
	pkgs, err := filepath.Glob(filepath.Join(dest, "*.tgz"))
	require.NoError(t, err)
	require.Len(t, pkgs, 1, "the packaging recipe wrote no chart: helm %v", args)
	return pkgs[0]
}

// TestDefect_EachReleaseStampsItsVersionAndNoCommit: the original defect was
// "every build reports chart and app version 0.1.0", and the fix was to name the build: chart version $(VERSION)+$(GIT_COMMIT), appVersion
// $(VERSION)-$(GIT_COMMIT), and an image tagged the same, so helm history told
// builds apart and a node could not serve a cached image from another commit.
//
// That is no longer how a release is named. Published images carry the release
// version alone -- `0.1.1`, never `0.1.1-<commit>` -- so a packaged chart whose
// appVersion named a commit would default to an image tag that does not exist,
// which is a worse failure than the original one. The commit is still in
// the binary (pkg/version.GitCommit) and in `make version`; it is not in any
// published name.
//
// What has to hold instead, and is what this test checks:
//
//  1. the packaging step stamps $(VERSION) and nothing else, so no commit can
//     leak into a chart version or an appVersion and name a missing image;
//  2. the default image tag IS that appVersion, for both images, so the chart
//     and the images it deploys cannot drift -- the property the original
//     defect was really about;
//  3. that tag is pulled Always. This is the compensating control. One VERSION
//     now means one published release, so a node must never keep serving an
//     earlier image it cached under the same name.
//
// The remaining assumption is outside the chart: VERSION must be bumped for
// every release published from main. Nothing in a packaged chart can check
// that. (Chart.yaml:9-10; Makefile chart-package.)
func TestDefect_EachReleaseStampsItsVersionAndNoCommit(t *testing.T) {
	root := dfxRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "VERSION"))
	require.NoError(t, err)
	version := strings.TrimSpace(string(raw))
	require.NotEmpty(t, version)

	type stamped struct {
		version, appVersion string
		images, policies    map[string]string
	}
	read := func(commit string) stamped {
		pkg := dfxPackageChart(t, commit)
		meta, err := dfxHelm(t, nil, "show", "chart", pkg)
		require.NoError(t, err, meta)
		s := stamped{images: map[string]string{}, policies: map[string]string{}}
		for _, l := range strings.Split(meta, "\n") {
			if v, ok := strings.CutPrefix(l, "version: "); ok {
				s.version = strings.Trim(v, `"'`)
			}
			if v, ok := strings.CutPrefix(l, "appVersion: "); ok {
				s.appVersion = strings.Trim(v, `"'`)
			}
		}
		out, err := dfxHelm(t, nil, dfxTemplateArgs(t, pkg, "", dfxBaseClass...)...)
		require.NoError(t, err, out)
		objs := dfxParse(t, out)
		for name, w := range map[string]dfxObj{
			"controller": dfxController(t, objs),
			"node":       dfxOne(t, objs, "DaemonSet", "-node"),
		} {
			c := dfxContainers(w)["qumulo-csi-driver"]
			s.images[name] = dfxStr(c["image"])
			s.policies[name] = dfxStr(c["imagePullPolicy"])
		}
		return s
	}
	a, b := read("dfx0aaa1"), read("dfx0bbb2")
	t.Logf("VERSION %s; build dfx0aaa1: %+v; build dfx0bbb2: %+v", version, a, b)

	// 1. The commit does not reach the chart. Two commits, one release.
	assert.Equal(t, version, a.version, "the packaged chart version is not VERSION")
	assert.Equal(t, version, a.appVersion, "the packaged appVersion is not VERSION")
	assert.Equal(t, a.version, b.version, "the chart version differs between two commits of one release")
	assert.Equal(t, a.appVersion, b.appVersion, "the appVersion differs between two commits of one release")

	for _, role := range []string{"controller", "node"} {
		// 2. Chart and image cannot drift: the tag is the appVersion.
		assert.Equal(t, a.appVersion, dfxImageTag(a.images[role]),
			"the %s image tag is not the chart's appVersion: %s", role, a.images[role])
		assert.Equal(t, a.images[role], b.images[role],
			"two commits of one release default to different %s images", role)
		// 3. The compensating control.
		assert.Equal(t, "Always", a.policies[role],
			"a release tag moves between releases, so the %s image must be pulled Always: %s %s",
			role, a.images[role], a.policies[role])
	}
}

// dfxImageTag is the tag of an image reference, i.e. what follows the last
// colon that is not part of a registry's host:port.
func dfxImageTag(ref string) string {
	i := strings.LastIndex(ref, ":")
	if i < 0 || strings.Contains(ref[i+1:], "/") {
		return ""
	}
	return ref[i+1:]
}

// TestDefect_ChartFolderPullsTheMovingTagAlways follows up
// TestDefect_EachReleaseStampsItsVersionAndNoCommit.
//
// The chart in the repo defaults to the bare VERSION, which every publish
// moves. Pulled IfNotPresent, a node kept an earlier cached 0.1.0 under a
// newer chart: an older build, which exits on the -registration-marker flag the branch's chart passes.
//
// This now covers every install, not just a chart-folder one: a packaged
// chart defaults to the same bare tag, because nothing published names a
// commit any more. Inverting this to IfNotPresent is safe only once one
// VERSION is guaranteed to mean one published build.
func TestDefect_ChartFolderPullsTheMovingTagAlways(t *testing.T) {
	out, err := dfxTryRender(t, "", dfxBaseClass...)
	require.NoError(t, err, out)
	objs := dfxParse(t, out)
	for _, w := range []dfxObj{dfxController(t, objs), dfxOne(t, objs, "DaemonSet", "-node")} {
		c := dfxContainers(w)["qumulo-csi-driver"]
		assert.Equal(t, "Always", dfxStr(c["imagePullPolicy"]), "%s pulls %s %s", w.kind(), dfxStr(c["image"]), dfxStr(c["imagePullPolicy"]))
	}
}
