//go:build chart && defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"os/exec"
	"strings"
	"testing"
)

// A chart install whose dedicated-export
// class sets no allowed clients must not give an export open to any client as
// root by default: the render must refuse the class, or the class must carry
// an allowedClients restriction.
func TestDefect_ChartRefusesOrRestrictsAnOpenDedicatedClassByDefault(t *testing.T) {
	// --kube-version: the chart's floor is 1.36, above a local Helm's default
	// capabilities version; the render must not fail for that reason.
	out, err := exec.Command("helm", "template", "test-release", chartPath, "--kube-version", "1.36.0",
		"--set", "storageClasses[0].name=dfx-open-class", "--set", "storageClasses[0].server=192.0.2.10",
		"--set", "storageClasses[0].secretName=tok", "--set", "storageClasses[0].basePath=/csi",
		"--set", "storageClasses[0].provisioningModel=dedicated-export").CombinedOutput()
	if err != nil {
		t.Logf("the render refused the open class: %s", strings.TrimSpace(string(out)))
		if strings.Contains(string(out), "kubeVersion") {
			t.Fatalf("the render failed on the Kubernetes version, not on the class: %s", out)
		}
		if !strings.Contains(string(out), "dfx-open-class") {
			t.Errorf("the refusal does not name the class: %s", out)
		}
		return
	}
	if !strings.Contains(string(out), "allowedClients") {
		t.Errorf("an install with a dedicated-export class and no allowed clients rendered without a " +
			"refusal and without any allowedClients restriction: every export it makes admits any NFS client as root")
	}
}
