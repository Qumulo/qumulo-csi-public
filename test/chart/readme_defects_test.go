//go:build chart && defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

// Defects in what the chart README tells an operator, checked against the
// driver code or the chart itself.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dfxReadmeRole returns the privileges in the chart README's least-privilege
// role: the first fenced block of the "Required Qumulo API privileges"
// section (the base role, before the replication additions).
func dfxReadmeRole(t *testing.T) map[string]bool {
	t.Helper()
	readme := dfxReadme(t)
	at := strings.Index(readme, "## Required Qumulo API privileges")
	require.GreaterOrEqual(t, at, 0, "the chart README has no privileges section")
	section := readme[at:]
	if next := strings.Index(section[3:], "\n## "); next >= 0 {
		section = section[:next+3]
	}
	block := regexp.MustCompile("(?s)```[a-z]*\n(.*?)```").FindStringSubmatch(section)
	require.NotNil(t, block, "the privileges section has no role block")
	role := map[string]bool{}
	for _, p := range regexp.MustCompile(`PRIVILEGE_[A-Z_]+`).FindAllString(block[1], -1) {
		role[p] = true
	}
	require.NotEmpty(t, role)
	return role
}

// dfxDriverCalls reports whether the driver's REST client (non-test code in
// pkg/qumulo) names the route.
func dfxDriverCalls(t *testing.T, route string) bool {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dfxRepoRoot(t), "pkg", "qumulo", "*.go"))
	require.NoError(t, err)
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		if strings.Contains(string(b), `"`+route) {
			return true
		}
	}
	return false
}

// TestDefect_ReadmeRoleCanReadTheNetwork: The driver calls GET /v3/network for
// the tenant-network check, which
// needs PRIVILEGE_NETWORK_READ; the chart README's least-privilege role must
// include it, or the check is silently skipped for every install that follows
// the README. (pkg/qumulo/client.go:88.)
func TestDefect_ReadmeRoleCanReadTheNetwork(t *testing.T) {
	require.True(t, dfxDriverCalls(t, "/v3/network"), "the driver no longer calls /v3/network; revisit this test")
	assert.True(t, dfxReadmeRole(t)["PRIVILEGE_NETWORK_READ"],
		"the README role lacks PRIVILEGE_NETWORK_READ, which the driver's /v3/network call needs")
}

// TestDefect_ReadmeRoleCanDeleteAndExpand: DeleteVolume and
// ControllerExpandVolume read both replication
// relationship status tables for every volume; the README's least-privilege
// role must hold PRIVILEGE_REPLICATION_SOURCE_READ and _TARGET_READ, or no
// volume can be deleted or expanded.
func TestDefect_ReadmeRoleCanDeleteAndExpand(t *testing.T) {
	role := dfxReadmeRole(t)
	for route, priv := range map[string]string{
		"/v2/replication/source-relationships/status/": "PRIVILEGE_REPLICATION_SOURCE_READ",
		"/v2/replication/target-relationships/status/": "PRIVILEGE_REPLICATION_TARGET_READ",
	} {
		require.True(t, dfxDriverCalls(t, route), "the driver no longer calls %s; revisit this test", route)
		assert.True(t, role[priv], "the README role lacks %s, which %s needs", priv, route)
	}
}

// TestDefect_ReadmeStatesTheFsGroupFileCost: Opt-in. The README offers
// fsGroupPolicy: File; it must state what it
// costs (the kubelet re-owns every entry on every mount) and recommend
// fsGroupChangePolicy: OnRootMismatch. (values.yaml:224.)
func TestDefect_ReadmeStatesTheFsGroupFileCost(t *testing.T) {
	readme := dfxReadme(t)
	require.Contains(t, readme, "fsGroupPolicy", "the README no longer mentions fsGroupPolicy; revisit this test")
	assert.True(t, dfxNear(readme, "OnRootMismatch", "fsGroup", 2000),
		"the README does not recommend fsGroupChangePolicy: OnRootMismatch with fsGroupPolicy: File")
	assert.True(t, dfxNear(readme, "OnRootMismatch", "SETATTR", 2000) ||
		dfxNear(readme, "OnRootMismatch", "every entry", 2000) || dfxNear(readme, "OnRootMismatch", "every file", 2000),
		"the README does not state, with that advice, that the kubelet re-owns every entry on every mount")
}
