// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"k8s.io/client-go/kubernetes"

	"qumulo-csi-driver/pkg/k8s"
)

// readyCheckClient builds the check's Kubernetes client; a test swaps in a
// fake clientset.
var readyCheckClient = func() (kubernetes.Interface, error) { return k8s.NewInClusterClientset() }

// readyCheckMain is `qumulo-csi-driver ready-check`: the chart's helm test
// readiness check (test-driver-ready.yaml), described on k8s.ReadyCheck. Exit
// codes as realMain: 2 for a flag error, 1 for a failed check, 0 for a pass.
func readyCheckMain(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("qumulo-csi-driver ready-check", flag.ContinueOnError)
	fs.SetOutput(stderr)
	namespace := fs.String("namespace", "", "namespace of the controller Deployment and node DaemonSet")
	driverName := fs.String("driver-name", "", "CSI driver name to find in CSINode objects")
	deployment := fs.String("deployment", "", "controller Deployment name")
	daemonSet := fs.String("daemonset", "", "node DaemonSet name")
	timeout := fs.Duration("timeout", 240*time.Second, "budget for all three checks together")
	interval := fs.Duration("interval", 5*time.Second, "time between reads of a check that is not yet true")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	var missing []string
	for name, v := range map[string]string{"namespace": *namespace, "driver-name": *driverName, "deployment": *deployment, "daemonset": *daemonSet} {
		if v == "" {
			missing = append(missing, "-"+name)
		}
	}
	if len(missing) > 0 || fs.NArg() > 0 || *timeout <= 0 || *interval <= 0 {
		_, _ = fmt.Fprintln(stderr, "ready-check: -namespace, -driver-name, -deployment and -daemonset are required, -timeout and -interval must be positive, and no arguments are taken")
		return 2
	}

	client, err := readyCheckClient()
	if err != nil {
		_, _ = fmt.Fprintf(stdout, "FAIL: %v\n", err)
		return 1
	}
	err = k8s.ReadyCheck{
		Client: client, Namespace: *namespace, DriverName: *driverName,
		Deployment: *deployment, DaemonSet: *daemonSet,
		Timeout: *timeout, Interval: *interval, Out: stdout,
	}.Run(context.Background())
	if err != nil {
		_, _ = fmt.Fprintf(stdout, "FAIL: %v\n", err)
		return 1
	}
	return 0
}
