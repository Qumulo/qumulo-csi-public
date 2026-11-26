//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Review follow-ups in the group fan-out's
// cached PersistentVolume listing.

const (
	f02Array = "red.example.com"
	f02Root  = "/csi/shared"
)

// staticPVLister answers every call with the same PersistentVolumes.
type staticPVLister struct{ pvs []PersistentVolumeInfo }

func (l staticPVLister) ListPersistentVolumes(context.Context, string) ([]PersistentVolumeInfo, error) {
	return append([]PersistentVolumeInfo(nil), l.pvs...), nil
}

func f02Server(t *testing.T, pvs ...PersistentVolumeInfo) *ReplicationServer {
	t.Helper()
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeController, Replication: true})
	require.NoError(t, err)
	d.PersistentVolumeLister = staticPVLister{pvs: pvs}
	server := d.ReplicationServer()
	require.NotNil(t, server)
	return server
}

func f02Handle(name string) string {
	return newVolumeID(f02Array, "", f02Root+"/"+name).String()
}

// TestDefect_ForgetMemberLeavesReaderSnapshotIntact: a listing handed
// to a fan-out reader must not change under it when a DeleteVolume evicts a
// member. forgetMember compacted the cached slice in place, so the reader's
// [a, b, c] became [b, c, c]: b and c shifted and a live member's slot was
// overwritten.
func TestDefect_ForgetMemberLeavesReaderSnapshotIntact(t *testing.T) {
	pvs := []PersistentVolumeInfo{
		{Name: "pv-a", Handle: f02Handle("pvc-a")},
		{Name: "pv-b", Handle: f02Handle("pvc-b")},
		{Name: "pv-c", Handle: f02Handle("pvc-c")},
	}
	s := f02Server(t, pvs...)

	snapshot, ok := s.listVolumesForFanout(context.Background())
	require.True(t, ok)
	before := append([]PersistentVolumeInfo(nil), snapshot...)

	s.forgetMember(f02Handle("pvc-a"))

	assert.Equal(t, before, snapshot, "a reader's listing changed under it when a member was forgotten")
	after, ok := s.listVolumesForFanout(context.Background())
	require.True(t, ok)
	assert.Equal(t, pvs[1:], after, "the forgotten member is still in the cached listing")
}

// TestDefect_ConcurrentForgetReportsEachLiveMemberOnce runs DeleteVolume's
// eviction concurrently with group reports. Every report must name each
// live member exactly once; under -race the in-place compaction is also a
// reported data race.
func TestDefect_ConcurrentForgetReportsEachLiveMemberOnce(t *testing.T) {
	const doomed, live = 16, 16
	var pvs []PersistentVolumeInfo
	var liveHandles []string
	// Doomed and live members interleaved, so every eviction shifts a live
	// member in an in-place compaction.
	for i := 0; i < doomed+live; i++ {
		h := f02Handle(fmt.Sprintf("pvc-%02d", i))
		pvs = append(pvs, PersistentVolumeInfo{Name: fmt.Sprintf("pv-%02d", i), Handle: h})
		if i%2 == 1 {
			liveHandles = append(liveHandles, h)
		}
	}
	s := f02Server(t, pvs...)
	self, err := parseVolumeID(f02Handle("pvc-self"))
	require.NoError(t, err)
	_, ok := s.listVolumesForFanout(context.Background()) // fill the cache
	require.True(t, ok)

	const readers, rounds = 4, 200
	var wg sync.WaitGroup
	errs := make(chan string, readers)
	start, stop := make(chan struct{}), make(chan struct{})
	for g := 0; g < readers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}
				seen := map[string]int{}
				for _, m := range s.groupMemberVolumes(context.Background(), f02Array, self, f02Root) {
					seen[m.String()]++
				}
				for _, h := range liveHandles {
					if seen[h] != 1 {
						errs <- fmt.Sprintf("live member %s reported %d times", h, seen[h])
						return
					}
				}
			}
		}()
	}
	// Each round evicts every doomed member, then expires the listing so
	// the next report lists afresh and the evictions start over.
	close(start)
	for round := 0; round < rounds; round++ {
		for i := 0; i < doomed+live; i += 2 {
			s.forgetMember(pvs[i].Handle)
		}
		s.memberList.mu.Lock()
		s.memberList.at = time.Time{}
		s.memberList.mu.Unlock()
	}
	close(stop)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

// TestDefect_LegacyHandleIsEvictedFromFanoutListing: DeleteVolume
// passes the parsed handle ("s##/p") while the listing holds each PV's
// handle as written. A volume an older build provisioned carries the legacy
// "s#/p", which never compared equal, so it stayed in the listing and the
// next group report exported a series for it.
func TestDefect_LegacyHandleIsEvictedFromFanoutListing(t *testing.T) {
	legacy := f02Array + "#" + f02Root + "/pvc-legacy"
	s := f02Server(t,
		PersistentVolumeInfo{Name: "pv-legacy", Handle: legacy},
		PersistentVolumeInfo{Name: "pv-b", Handle: f02Handle("pvc-b")},
	)
	self, err := parseVolumeID(f02Handle("pvc-self"))
	require.NoError(t, err)
	parsed, err := parseVolumeID(legacy)
	require.NoError(t, err)
	_, ok := s.listVolumesForFanout(context.Background())
	require.True(t, ok)

	s.forgetMember(parsed.String()) // what finishDelete passes

	var reported []string
	for _, m := range s.groupMemberVolumes(context.Background(), f02Array, self, f02Root) {
		reported = append(reported, m.String())
	}
	assert.Equal(t, []string{f02Handle("pvc-b")}, reported,
		"a volume deleted under its legacy handle is still reported by the group fan-out")
}
