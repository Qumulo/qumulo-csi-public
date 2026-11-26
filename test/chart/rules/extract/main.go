// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Command extract reads a rendered chart (a YAML stream) on stdin and writes
// the rule groups of its PrometheusRule to stdout as a Prometheus rules file,
// which is what promtool checks and unit-tests. It exits non-zero unless the
// stream holds exactly one PrometheusRule, so a chart that stops rendering
// its rules fails the gate instead of testing nothing.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

type prometheusRule struct {
	Kind string `yaml:"kind"`
	Spec struct {
		Groups []yaml.Node `yaml:"groups"`
	} `yaml:"spec"`
}

func main() {
	if err := run(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "extract:", err)
		os.Exit(1)
	}
}

func run(in io.Reader, out io.Writer) error {
	decoder := yaml.NewDecoder(in)
	var found []prometheusRule
	for {
		var doc prometheusRule
		err := decoder.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if doc.Kind == "PrometheusRule" {
			found = append(found, doc)
		}
	}
	if len(found) != 1 {
		return fmt.Errorf("want exactly one PrometheusRule in the rendered chart, found %d", len(found))
	}
	if len(found[0].Spec.Groups) == 0 {
		return errors.New("the PrometheusRule has no rule groups")
	}
	encoder := yaml.NewEncoder(out)
	if err := encoder.Encode(map[string][]yaml.Node{"groups": found[0].Spec.Groups}); err != nil {
		return err
	}
	return encoder.Close()
}
