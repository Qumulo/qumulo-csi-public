// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package k8s

import (
	"testing"

	"k8s.io/client-go/rest"
)

// The in-cluster client set no rate limits, so client-go's 5 QPS and
// burst of 10 throttled a single ListVolumes page for seconds.
func TestTheKubernetesClientIsNotLeftAtClientGosDefaultRate(t *testing.T) {
	config := &rest.Config{}
	withClientLimits(config)
	if config.QPS < 20 || config.Burst < 50 {
		t.Fatalf("QPS %v, burst %d; want at least 20 and 50", config.QPS, config.Burst)
	}
}
