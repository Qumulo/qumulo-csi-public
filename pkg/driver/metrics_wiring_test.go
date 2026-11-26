// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// nopObserver is a distinguishable observer; nothing in these tests calls it.
type nopObserver struct{}

func (nopObserver) ObserveRequest(context.Context, qumulo.RequestEvent)         {}
func (nopObserver) ObserveQuota(context.Context, qumulo.QuotaEvent)             {}
func (nopObserver) ObserveCapacity(context.Context, qumulo.CapacityEvent)       {}
func (nopObserver) ObserveReplication(context.Context, qumulo.ReplicationEvent) {}

func (nopObserver) ObserveVolumeProtection(context.Context, qumulo.VolumeProtectionEvent) {}
func (nopObserver) ObserveRestoreStaging(context.Context, qumulo.RestoreStagingEvent)     {}

// The observer has to reach the clients the driver builds per request, or
// every array-side series stays empty while /metrics happily serves zeroes.
func TestPerRequestClientsCarryTheObserver(t *testing.T) {
	observer := nopObserver{}

	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "test", Mode: ModeController,
		Observer: observer,
	})
	require.NoError(t, err)

	client, ok := d.NewClient("https://qumulo.example.com:8000", "token").(*qumulo.Client)
	require.True(t, ok, "the default constructor must still hand back a *qumulo.Client")
	assert.Equal(t, observer, client.Observer)
}

// The replication service asserts the client it is handed to its own
// interface. Wrapping the client to observe it -- the obvious alternative to
// a field -- would fail that assertion and turn every replication RPC into
// Unimplemented, silently. This pins that the observed client still
// satisfies both interfaces.
func TestAnObservedClientStillSatisfiesTheReplicationInterface(t *testing.T) {
	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "test", Mode: ModeController,
		Observer: nopObserver{},
	})
	require.NoError(t, err)

	client := d.NewClient("https://qumulo.example.com:8000", "token")
	_, ok := client.(replicationClient)
	assert.True(t, ok, "an observed client must still be a replicationClient")
}

// A driver built with no observer must produce clients that observe nothing,
// which is what every deployment without -metrics-address gets.
func TestWithoutAnObserverClientsAreUnchanged(t *testing.T) {
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeController})
	require.NoError(t, err)

	client, ok := d.NewClient("https://qumulo.example.com:8000", "token").(*qumulo.Client)
	require.True(t, ok)
	assert.Nil(t, client.Observer)
}
