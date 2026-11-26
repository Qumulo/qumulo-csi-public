// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package doccheck

// This file has no build tag on purpose. Every other test in this package
// is built only with -tags defects, so a guard that the tag is passed must
// not itself depend on it: it would vanish together with what it guards.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// makeVarRef matches a $(NAME) reference in a Makefile recipe.
var makeVarRef = regexp.MustCompile(`\$\(([A-Za-z_][A-Za-z0-9_]*)\)`)

// makeTagsFlag matches go test's -tags flag and its value, quoted or not.
var makeTagsFlag = regexp.MustCompile(`-tags[ =]+(?:'([^']*)'|"([^"]*)"|(\S+))`)

// repoRoot is the nearest directory above the working directory holding go.mod.
func repoRoot(t *testing.T) string {
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

// makefileVars returns the Makefile's simple top-level assignments
// (NAME = v, NAME := v, NAME ?= v). Good enough for the one-line tag
// variables a recipe expands; anything conditional is out of scope.
func makefileVars(text string) map[string]string {
	vars := map[string]string{}
	assign := regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*(?:\?=|:=|=)\s*(.*)$`)
	for _, line := range strings.Split(text, "\n") {
		if m := assign.FindStringSubmatch(line); m != nil {
			vars[m[1]] = strings.TrimSpace(m[2])
		}
	}
	return vars
}

// makeRecipe returns the recipe lines of target, with variable references
// expanded from vars (an unknown variable expands to nothing, as in make).
func makeRecipe(t *testing.T, text, target string, vars map[string]string) []string {
	t.Helper()
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, target+":") {
			continue
		}
		var recipe []string
		for _, r := range lines[i+1:] {
			if !strings.HasPrefix(r, "\t") {
				break
			}
			recipe = append(recipe, makeVarRef.ReplaceAllStringFunc(r, func(ref string) string {
				return vars[makeVarRef.FindStringSubmatch(ref)[1]]
			}))
		}
		return recipe
	}
	t.Fatalf("Makefile has no %q target", target)
	return nil
}

// goTestTags returns the build tags of every `go test` line in recipe; a
// line with no -tags flag contributes an empty set.
func goTestTags(recipe []string) [][]string {
	var all [][]string
	for _, line := range recipe {
		if !strings.Contains(line, "go test") {
			continue
		}
		var tags []string
		if m := makeTagsFlag.FindStringSubmatch(line); m != nil {
			v := m[1] + m[2] + m[3]
			tags = strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ' ' })
		}
		all = append(all, tags)
	}
	return all
}

// TestDefectSuiteIsPartOfMakeTest: every TestDefect_* outside the chart and
// known-limitation sets is built only with -tags defects, and CI runs them
// by running `make test`. If the test target loses the tag, CI goes on
// passing while compiling none of them -- which is how they went unrun
// before. The coverage target must build the same tests, or its numbers
// describe a different suite. Neither may pass knownlimits, whose tests
// fail by design.
func TestDefectSuiteIsPartOfMakeTest(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	text := string(raw)
	vars := makefileVars(text)

	for _, target := range []string{"test", "coverage"} {
		runs := goTestTags(makeRecipe(t, text, target, vars))
		if len(runs) == 0 {
			t.Errorf("make %s runs no go test", target)
		}
		for _, tags := range runs {
			if !contains(tags, "defects") {
				t.Errorf("make %s runs go test with tags %q: without defects no TestDefect_* is built", target, tags)
			}
			if contains(tags, "knownlimits") {
				t.Errorf("make %s builds the knownlimits tests, which fail by design", target)
			}
		}
	}

	ci, err := os.ReadFile(filepath.Join(root, ".github/workflows/ci.yml"))
	if err != nil {
		t.Fatalf("read ci.yml: %v", err)
	}
	if !regexp.MustCompile(`(?m)^\s*run:\s*make test\b`).Match(ci) {
		t.Errorf("ci.yml no longer runs `make test`, so the defect suite does not run in CI")
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
