// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// TestEveryNFSSettingIsConsulted fails when a field is added to
// qumulo.NFSSettings and nothing in this package ever looks at it.
//
// Three defects in one release were the same bug: a field was fetched off the
// cluster, stored on the settings struct, and then never consulted.
// auth_sys_enabled had no reader at all; krb5p_enabled and krb5i_enabled were
// not even decoded. Each one provisioned a volume the cluster had already said
// it could not serve, and each surfaced as a pod stuck in ContainerCreating
// behind an error naming nothing an operator could act on.
//
// Reading the cluster's answer and discarding it is worse than not asking: the
// round trip is spent, the refusal is available, and the driver throws it away.
// So the check is "somebody in pkg/driver reads this field", not "the field is
// decoded" -- decoding is what the broken versions already did.
//
// Deliberately a source scan rather than a coverage or behaviour assertion. It
// cannot tell a real gate from an idle read, and a determined author can
// satisfy it with `_ = settings.NewField`. It is a tripwire for the field
// nobody remembered to wire up, not a proof of correctness.
func TestEveryNFSSettingIsConsulted(t *testing.T) {
	read := fieldsReadIn(t, ".")

	settings := reflect.TypeOf(qumulo.NFSSettings{})
	for i := range settings.NumField() {
		name := settings.Field(i).Name
		t.Run(name, func(t *testing.T) {
			// Deliberately not require.Contains: its failure message renders
			// the whole selector set, which is every identifier in the
			// package and buries the one line that matters.
			if _, ok := read[name]; !ok {
				require.FailNowf(t, "NFS setting is never consulted",
					"qumulo.NFSSettings.%s is fetched from the cluster and then read by "+
						"nothing in pkg/driver.\n\n"+
						"Either gate a provision on it or drop it from the struct. A field "+
						"read off the array and discarded spends the round trip, has the "+
						"cluster's refusal in hand, and throws it away -- which is how "+
						"auth_sys_enabled, krb5p_enabled and krb5i_enabled each shipped a "+
						"volume the cluster had already said it could not serve.", name)
			}
		})
	}
}

// fieldsReadIn returns the name of every field selected on anything in the
// non-test Go files of dir, e.g. both x and y from `settings.x` and `p.q.y`.
//
// Field names are matched across the whole package rather than per-type: the
// selector's receiver type is not resolved, so a field named like one of
// NFSSettings' elsewhere would satisfy this. Names like Krb5pEnabled are
// distinctive enough that the looser match costs nothing and keeps the check
// free of a type-checking pass.
func fieldsReadIn(t *testing.T, dir string) map[string]struct{} {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	fset := token.NewFileSet()
	read := map[string]struct{}{}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		require.NoError(t, err, "parsing %s", name)

		ast.Inspect(file, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				read[sel.Sel.Name] = struct{}{}
			}
			return true
		})
	}

	require.NotEmpty(t, read, "no selectors found in %s; the scan is not looking at the package", dir)
	return read
}
