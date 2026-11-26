// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every base image the Dockerfile builds on must be pinned by digest.
//
// A tag such as golang:1.27.4 or debian-base:trixie-v1.0.1 is re-pushed when
// its publisher rebuilds it, so one commit could build on different bytes
// from one day to the next -- a release that cannot be rebuilt, and a base
// that changes under review without a diff. The digest (the multi-arch
// index, so every platform in PLATFORMS still resolves) is what makes a build
// reproducible from the checkout alone. Named stages, not a count, so a new
// stage cannot pass by replacing an old one. A stage built FROM an earlier
// stage (controller and node FROM ubi-micro) inherits that stage's pin.
func TestDockerfilePinsEveryBaseImageByDigest(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "Dockerfile"))
	require.NoError(t, err)
	dockerfile := string(b)

	from := regexp.MustCompile(`(?i)^FROM\s+(?:--platform=\S+\s+)?(\S+)\s+AS\s+(\S+)\s*$`)
	pinned := regexp.MustCompile(`^[^\s@]+@sha256:[a-f0-9]{64}$`)

	want := map[string]bool{
		"build": false, "ubi-micro": false, "controller-rootfs": false, "controller": false,
		"node-rootfs": false, "tini": false, "node": false,
	}
	declared := map[string]bool{}
	for _, line := range strings.Split(dockerfile, "\n") {
		l := strings.TrimSpace(line)
		if !strings.HasPrefix(strings.ToUpper(l), "FROM ") {
			continue
		}
		m := from.FindStringSubmatch(l)
		require.NotNil(t, m, "FROM line not in the form `FROM [--platform=...] <image> AS <stage>`: %q", l)
		image, stage := m[1], strings.ToLower(m[2])

		_, expected := want[stage]
		assert.True(t, expected, "stage %q is in the Dockerfile but not in this test's expected set", stage)
		want[stage] = true
		if !declared[strings.ToLower(image)] {
			assert.Regexp(t, pinned, image, "stage %s: base image %q is not pinned by digest", stage, image)
		}
		declared[stage] = true
	}
	for stage, seen := range want {
		assert.True(t, seen, "stage %q was expected but the Dockerfile has no such FROM", stage)
	}
}
