//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Node-area defects whose correct behaviour is documentation,
// plus the repository-file helpers the node defect tests share.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dfxReadRepoFile reads a file relative to the repository root (two levels up
// from pkg/driver, where `go test` runs).
func dfxReadRepoFile(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(rel)))
	require.NoError(t, err, "reading %s", rel)
	return string(b)
}

var dfxCommentMarker = regexp.MustCompile(`(?m)^\s*(//|#)\s?`)
var dfxSpaces = regexp.MustCompile(`\s+`)

// dfxNormalizeComment lowercases text, drops line-comment markers and folds
// whitespace, so a phrase wrapped across comment lines still matches.
func dfxNormalizeComment(text string) string {
	text = dfxCommentMarker.ReplaceAllString(text, "")
	return strings.ToLower(dfxSpaces.ReplaceAllString(text, " "))
}

// The Linux NFS client shares one connection per array address,
// so a later class's nconnect is silently ignored. The fix is
// documentation: the chart README must say that nconnect takes effect only
// on the first mount to an address (and how to spread load). The kernel
// behaviour itself is not the driver's to change.
func TestDefect_ChartReadmeExplainsNconnectIsPerAddress(t *testing.T) {
	readme := dfxNormalizeComment(dfxReadRepoFile(t, "charts/qumulo-csi-driver/README.md"))
	if !strings.Contains(readme, "nconnect") {
		t.Fatalf("the chart README says nothing about nconnect")
	}
	perAddress := regexp.MustCompile(`nconnect[^.]*(first mount|only the first|first class)|(first mount|only the first|first class)[^.]*nconnect`)
	assert.True(t, perAddress.MatchString(readme),
		"the chart README does not say that nconnect takes effect only on the first mount to an address")
}
