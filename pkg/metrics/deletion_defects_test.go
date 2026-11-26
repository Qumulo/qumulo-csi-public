//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// Review follow-ups to the deletion metrics.

const (
	f06LegacyHandle    = "qumulo.example.com#/csi/volumes/pvc-legacy"
	f06CanonicalHandle = "qumulo.example.com##/csi/volumes/pvc-legacy"
)

// TestDefect_LegacyHandleDeleteSilencesGroupReport: a volume an older
// build provisioned carries the legacy "<server>#<path>" handle, and
// DeleteVolume is called with it. The group fan-out reports the same volume
// under the canonical spelling (member.String()). The deletion guard keyed
// by the raw request ID did not match, so the next group report brought the
// series back reading protected=0 -- QumuloCSIVolumeUnprotected for a
// volume that is gone.
func TestDefect_LegacyHandleDeleteSilencesGroupReport(t *testing.T) {
	r := testRegistry()
	r.ObserveVolumeProtection(context.Background(),
		qumulo.VolumeProtectionEvent{Subject: f06CanonicalHandle, Protected: true})
	require.Equal(t, 1, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"))

	_, err := call(t, r, "/csi.v1.Controller/DeleteVolume",
		&csi.DeleteVolumeRequest{VolumeId: f06LegacyHandle},
		func(context.Context, any) (any, error) { return &csi.DeleteVolumeResponse{}, nil })
	require.NoError(t, err)
	require.Equal(t, 0, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"))

	// The group report, from its still-cached listing, in canonical form.
	r.ObserveVolumeProtection(context.Background(),
		qumulo.VolumeProtectionEvent{Subject: f06CanonicalHandle, Protected: false})

	assert.Equal(t, 0, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"),
		"a volume deleted under its legacy handle got its series back from a group report")
}

// TestDefect_ReportBetweenForgetAndRecordDoesNotReviveSeries pins the
// order of DeleteVolume's two metric steps with a controlled interleaving.
//
// The test holds the deletion log's lock, so the delete parks at the point
// where it records the deletion. A protection report that passed its
// "deleted?" check just before then writes the series. Recorded after the
// series were dropped, the delete had already dropped them, so
// the report's write survived for good. Recorded first, the drop comes
// after the write and removes it.
func TestDefect_ReportBetweenForgetAndRecordDoesNotReviveSeries(t *testing.T) {
	r := testRegistry()
	r.ObserveVolumeProtection(context.Background(),
		qumulo.VolumeProtectionEvent{Subject: testHandle, Protected: true})
	require.Equal(t, 1, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"))

	r.deletions.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := call(t, r, "/csi.v1.Controller/DeleteVolume",
			&csi.DeleteVolumeRequest{VolumeId: testHandle},
			func(context.Context, any) (any, error) { return &csi.DeleteVolumeResponse{}, nil })
		done <- err
	}()
	f14WaitForGoroutineIn(t, "(*deletionLog).record", "sync.(*Mutex).Lock")

	// The write of a report whose check ran before the delete was recorded.
	name, _ := volumeNameFromHandle(testHandle)
	r.volumeProtected.WithLabelValues(volumeID(testHandle), name).Set(0)

	r.deletions.mu.Unlock()
	require.NoError(t, <-done)

	assert.Equal(t, 0, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"),
		"a report landing between the series drop and the deletion record revived a deleted volume's series")
}

// f14WaitForGoroutineIn returns once some goroutine's stack holds every one
// of frames.
func f14WaitForGoroutineIn(t *testing.T, frames ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			all := true
			for _, f := range frames {
				if !strings.Contains(g, f) {
					all = false
					break
				}
			}
			if all {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no goroutine reached %v", frames)
		}
		runtime.Gosched()
	}
}
