// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"qumulo-csi-driver/pkg/qumulo"
)

const abandoned = "192.0.2.1:8000"

// pinEndpointClock sets the tracker's clock, returning a function that moves it.
func pinEndpointClock(r *Registry, at time.Time) func(time.Duration) {
	r.endpoints.now = func() time.Time { return at }
	return func(d time.Duration) {
		at = at.Add(d)
		r.endpoints.now = func() time.Time { return at }
	}
}

func observeUnreachable(r *Registry, cluster string) {
	r.ObserveRequest(context.Background(), qumulo.RequestEvent{
		Cluster: cluster, Method: "GET", PathTemplate: "/v1/file-system",
		Status: qumulo.StatusTransport, Duration: 10 * time.Second, Reachable: false,
	})
	r.ObserveCapacity(context.Background(), qumulo.CapacityEvent{Cluster: cluster})
	r.ObserveRestoreStaging(context.Background(), qumulo.RestoreStagingEvent{Cluster: cluster, State: qumulo.RestoreStagingRemoved})
}

// An endpoint nothing has called for the idle window loses its
// reachability and capacity series, so the critical ArrayUnreachable alert
// clears, and its request and latency series too, so an endpoint the
// controller no longer uses stops costing series for ever.
func TestAnAbandonedEndpointIsRetired(t *testing.T) {
	r := testRegistry()
	advance := pinEndpointClock(r, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	observeUnreachable(r, abandoned)

	advance(endpointIdleWindow)
	r.RetireIdleEndpoints(context.Background())

	assert.Equal(t, 0, testutil.CollectAndCount(r.apiReachable))
	assert.Equal(t, 0, testutil.CollectAndCount(r.clusterCapacityBytes))
	assert.Equal(t, 0, testutil.CollectAndCount(r.clusterFreeBytes))
	assert.Equal(t, 0, testutil.CollectAndCount(r.apiRequests), "request series retired")
	assert.Equal(t, 0, testutil.CollectAndCount(r.apiDuration), "latency series retired")
	assert.Equal(t, 0, testutil.CollectAndCount(r.restoreStaging), "restore staging series retired")
}

// Retiring one endpoint leaves every other endpoint's request and
// latency series alone.
func TestRetiringAnEndpointKeepsOtherEndpointsRequestSeries(t *testing.T) {
	r := testRegistry()
	advance := pinEndpointClock(r, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	observeUnreachable(r, abandoned)
	advance(endpointIdleWindow)
	observeUnreachable(r, "192.0.2.2:8000")
	r.RetireIdleEndpoints(context.Background())

	assert.Equal(t, 1, testutil.CollectAndCount(r.apiRequests))
	assert.Equal(t, 1, testutil.CollectAndCount(r.apiDuration))
	assert.Equal(t, 1, testutil.CollectAndCount(r.apiReachable))
}

// An endpoint something still calls -- a failing provision retried with
// backoff, a replication peer read on every status poll -- is never idle, so
// a down array that anything depends on keeps alerting.
func TestAnEndpointStillBeingCalledIsKept(t *testing.T) {
	r := testRegistry()
	advance := pinEndpointClock(r, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	for i := 0; i < 90; i++ {
		observeUnreachable(r, abandoned)
		advance(time.Minute)
		r.RetireIdleEndpoints(context.Background())
	}

	assert.Equal(t, 1, testutil.CollectAndCount(r.apiReachable))
}

// A retired endpoint that is called again is simply exported again.
func TestARetiredEndpointReturnsWhenCalledAgain(t *testing.T) {
	r := testRegistry()
	advance := pinEndpointClock(r, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	observeUnreachable(r, abandoned)
	advance(endpointIdleWindow)
	r.RetireIdleEndpoints(context.Background())
	assert.Equal(t, 0, testutil.CollectAndCount(r.apiReachable))

	observeUnreachable(r, abandoned)

	assert.Equal(t, 1, testutil.CollectAndCount(r.apiReachable))
}

// The retirement ran only from ListVolumes and ControllerGetVolume, which
// the chart deploys nothing to call, so on the chart as shipped nothing ever
// retired. RunRetirement does it on its own timer, with no RPC and no
// StorageClass.
func TestRunRetirementRetiresWithoutAnyRPC(t *testing.T) {
	r := testRegistry()
	advance := pinEndpointClock(r, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	r.endpoints.every = time.Millisecond
	observeUnreachable(r, abandoned)
	advance(endpointIdleWindow)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.RunRetirement(ctx); close(done) }()
	assert.Eventually(t, func() bool { return testutil.CollectAndCount(r.apiReachable) == 0 },
		time.Second, time.Millisecond)
	cancel()
	<-done
}
