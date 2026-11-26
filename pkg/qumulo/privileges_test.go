// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// routeLiteral is a REST route as a string literal in this package: a
// versioned path, possibly a format string, possibly with a query.
var routeLiteral = regexp.MustCompile(`^/v[0-9]+/`)

// clientRoutes returns every route literal in this package's non-test source,
// with any query string cut off.
func clientRoutes(t *testing.T) map[string]bool {
	t.Helper()
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	routes := map[string]bool{}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil || !routeLiteral.MatchString(value) {
				return true
			}
			routes[strings.SplitN(value, "?", 2)[0]] = true
			return true
		})
	}
	require.NotEmpty(t, routes)
	return routes
}

// privilegesSection is the chart README's "Required Qumulo API privileges"
// section, up to the next second-level heading.
func privilegesSection(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("../../charts/qumulo-csi-driver/README.md")
	require.NoError(t, err)
	readme := string(raw)
	start := strings.Index(readme, "## Required Qumulo API privileges")
	require.NotEqual(t, -1, start, "the chart README has no privileges section")
	section := readme[start:]
	if end := strings.Index(section[3:], "\n## "); end != -1 {
		section = section[:end+3]
	}
	return section
}

var privilegeName = regexp.MustCompile(`PRIVILEGE_[A-Z_]+`)

// TestEveryRouteHasItsPrivileges ties the client's routes to the privileges
// they need and to the chart README that tells an operator which to grant.
// A route added without an entry fails, and so does an entry naming a
// privilege the README does not: the README role once lacked the replication
// READ pair, so no volume could be deleted, and the privileges Info,
// the planned-promote drain and the resync reverse need.
func TestEveryRouteHasItsPrivileges(t *testing.T) {
	routes := clientRoutes(t)

	for route := range routes {
		_, ok := routePrivileges[route]
		assert.True(t, ok, "route %s has no entry in routePrivileges: find the privilege Core's 403 names and add it", route)
	}
	for route := range routePrivileges {
		assert.True(t, routes[route], "routePrivileges lists %s, which the client no longer calls", route)
	}

	section := privilegesSection(t)
	documented := map[string]bool{}
	for _, name := range privilegeName.FindAllString(section, -1) {
		documented[name] = true
	}
	var missing []string
	needed := map[string]bool{}
	for route, privileges := range routePrivileges {
		for _, privilege := range privileges {
			needed[privilege] = true
			if !documented[privilege] {
				missing = append(missing, privilege+" ("+route+")")
			}
		}
	}
	sort.Strings(missing)
	assert.Empty(t, missing, "privileges the client needs that the chart README's privileges section never names")

	// The README's full-set block names nothing no route needs.
	block := section[strings.Index(section, "```")+3:]
	block = block[:strings.Index(block, "```")]
	for _, name := range privilegeName.FindAllString(block, -1) {
		assert.True(t, needed[name], "the README's full set lists %s, which no route in routePrivileges needs", name)
	}
}
