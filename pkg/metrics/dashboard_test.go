// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The dashboard the chart ships (metrics.dashboard.enabled) names series by
// string. A series renamed or removed here leaves a panel that renders empty
// and raises no error anywhere, so every csi_qumulo_* name the dashboard
// queries must still be declared by this package.
func TestShippedDashboardNamesOnlySeriesThisPackageDeclares(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "qumulo-csi-driver", "dashboards", "qumulo-csi.json"))
	require.NoError(t, err)
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(raw, &parsed), "the shipped dashboard is not valid JSON")

	src, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var code strings.Builder
	for _, f := range src {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		code.Write(b)
	}
	declared := code.String()

	names := map[string]bool{}
	for _, n := range regexp.MustCompile(namespace+`_[a-z_]+`).FindAllString(string(raw), -1) {
		for _, s := range []string{"_bucket", "_sum", "_count"} {
			n = strings.TrimSuffix(n, s)
		}
		names[n] = true
	}
	require.NotEmpty(t, names, "the dashboard queries no %s series", namespace)

	var missing []string
	for n := range names {
		short := strings.TrimPrefix(n, namespace+"_")
		// Declared as Name: "<short>" (Opts) or namespace+"_<short>" (NewDesc).
		if !strings.Contains(declared, `"`+short+`"`) && !strings.Contains(declared, `"_`+short+`"`) {
			missing = append(missing, n)
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing, "the dashboard queries series this package no longer declares")
}
