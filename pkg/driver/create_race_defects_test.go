//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Two CreateVolume calls for one name at once. A fresh call whose
// quota create lost to the other call's rolled its directory back after the
// other had already answered OK, leaving a bound PV naming no directory.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// raceArray is the fake cluster with what the race needs from a real one: a
// second quota POST for a file answers 409, the first quota POST can be held
// until the test lets it through, and tree deletes are counted.
type raceArray struct {
	mu      sync.Mutex
	cluster *fakeCluster
	inner   http.HandlerFunc

	holdFirstQuota bool
	quotaPosts     int
	firstQuotaSeen chan struct{}
	releaseFirst   chan struct{}

	// lostReply applies the first quota POST and then answers 409, which is
	// what the client's retry of a POST whose reply was lost sees.
	lostReply bool

	treeDeletes int
	releaseOnce sync.Once
}

// release lets the held quota POST through; safe to call twice, and
// deferred after srv.Close so a failed assertion does not hang the test.
func (a *raceArray) release() { a.releaseOnce.Do(func() { close(a.releaseFirst) }) }

func newRaceArray() *raceArray {
	c := newFakeCluster()
	return &raceArray{
		cluster:        c,
		inner:          c.handler(),
		firstQuotaSeen: make(chan struct{}),
		releaseFirst:   make(chan struct{}),
	}
}

type raceBody struct{ *strings.Reader }

func (raceBody) Close() error { return nil }

func (a *raceArray) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath()
	if path == "/v1/files/quotas/" && r.Method == http.MethodPost {
		a.mu.Lock()
		a.quotaPosts++
		n := a.quotaPosts
		a.mu.Unlock()
		if n == 1 && a.holdFirstQuota {
			close(a.firstQuotaSeen)
			<-a.releaseFirst
		}
		body, _ := io.ReadAll(r.Body)
		var req struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(body, &req)

		a.mu.Lock()
		_, exists := a.cluster.quotas[req.ID]
		a.mu.Unlock()
		if exists {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error_class": "api_quotas_quota_limit_already_set_error",
				"description": "Quota limit already set for id " + req.ID,
			})
			return
		}
		r.Body = raceBody{strings.NewReader(string(body))}
		if n == 1 && a.lostReply {
			a.mu.Lock()
			a.inner(httptest.NewRecorder(), r)
			a.mu.Unlock()
			writeJSON(w, http.StatusConflict, map[string]string{
				"error_class": "api_quotas_quota_limit_already_set_error",
				"description": "Quota limit already set for id " + req.ID,
			})
			return
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if path == "/v1/tree-delete/jobs/" && r.Method == http.MethodPost {
		a.treeDeletes++
	}
	a.inner(w, r)
}

func (a *raceArray) state() (dirs, quotas, exports, treeDeletes int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.cluster.directories), len(a.cluster.quotas), len(a.cluster.exports), a.treeDeletes
}

type createResult struct {
	resp *csi.CreateVolumeResponse
	err  error
}

func createAsync(cs *ControllerServer, req *csi.CreateVolumeRequest) chan createResult {
	done := make(chan createResult, 1)
	go func() {
		resp, err := cs.CreateVolume(context.Background(), req)
		done <- createResult{resp, err}
	}()
	return done
}

// One controller: the second call waits for the first, and both get the
// one volume the first made.
func TestDefect_ConcurrentCreateVolumesOfOneNameMakeOneDirectoryOneQuotaOneExport(t *testing.T) {
	arr := newRaceArray()
	arr.holdFirstQuota = true
	srv := httptest.NewServer(arr)
	defer srv.Close()
	defer arr.release()
	cs := newIntegrationControllerServer(t, srv.URL)
	const name = "pvc-race"

	a := createAsync(cs, dfxOptedInCreate(name))
	<-arr.firstQuotaSeen // A has made the directory and is at its quota

	b := createAsync(cs, dfxOptedInCreate(name))
	select {
	case r := <-b:
		t.Fatalf("the second call answered while the first was still creating: %v", r.err)
	case <-time.After(100 * time.Millisecond):
	}
	require.True(t, cs.createLocks.held(name))

	arr.release()
	ra, rb := <-a, <-b
	require.NoError(t, ra.err)
	require.NoError(t, rb.err)
	assert.Equal(t, ra.resp.GetVolume().GetVolumeId(), rb.resp.GetVolume().GetVolumeId())

	dirs, quotas, exports, deletes := arr.state()
	assert.Equal(t, [4]int{1, 1, 1, 0}, [4]int{dirs, quotas, exports, deletes},
		"one directory, one quota, one export, nothing deleted")
	assert.False(t, cs.createLocks.held(name), "the lock is let go once both have answered")
}

// Two controllers (a leader handover), so no shared lock: the adopting call
// finishes the volume first, and the fresh call must not delete it.
func TestDefect_AFreshCreateThatLosesTheQuotaToAnotherControllerKeepsTheDirectory(t *testing.T) {
	arr := newRaceArray()
	arr.holdFirstQuota = true
	srv := httptest.NewServer(arr)
	defer srv.Close()
	defer arr.release()
	csA := newIntegrationControllerServer(t, srv.URL)
	csB := newIntegrationControllerServer(t, srv.URL)
	const name = "pvc-race"

	a := createAsync(csA, dfxOptedInCreate(name))
	<-arr.firstQuotaSeen
	rb := <-createAsync(csB, dfxOptedInCreate(name)) // adopts and finishes it
	require.NoError(t, rb.err)

	arr.release()
	ra := <-a
	require.NoError(t, ra.err, "the fresh call adopts the volume the other controller finished")
	assert.Equal(t, rb.resp.GetVolume().GetVolumeId(), ra.resp.GetVolume().GetVolumeId())

	dirs, quotas, exports, deletes := arr.state()
	assert.Equal(t, [4]int{1, 1, 1, 0}, [4]int{dirs, quotas, exports, deletes})
}

// The same race with different sizes: the volume the other controller
// reported stays as it was, and the fresh call is told AlreadyExists.
func TestDefect_ARacingCreateOfADifferentSizeIsAlreadyExistsAndDeletesNothing(t *testing.T) {
	arr := newRaceArray()
	arr.holdFirstQuota = true
	srv := httptest.NewServer(arr)
	defer srv.Close()
	defer arr.release()
	csA := newIntegrationControllerServer(t, srv.URL)
	csB := newIntegrationControllerServer(t, srv.URL)
	const name = "pvc-race"

	reqA := dfxOptedInCreate(name)
	reqB := dfxOptedInCreate(name)
	reqB.CapacityRange = &csi.CapacityRange{RequiredBytes: reqA.GetCapacityRange().GetRequiredBytes() * 2}

	a := createAsync(csA, reqA)
	<-arr.firstQuotaSeen
	rb := <-createAsync(csB, reqB)
	require.NoError(t, rb.err)

	arr.release()
	ra := <-a
	assert.Equal(t, codes.AlreadyExists, status.Code(ra.err), "%v", ra.err)

	dirs, quotas, exports, deletes := arr.state()
	assert.Equal(t, [4]int{1, 1, 1, 0}, [4]int{dirs, quotas, exports, deletes})
	for _, limit := range arr.cluster.quotas {
		assert.Equal(t, strconv.FormatInt(reqB.GetCapacityRange().GetRequiredBytes(), 10), limit,
			"the size the other controller reported is the size on the array")
	}
}

// One call, no race: its quota POST landed but the reply was lost, and the
// retry answered 409. The volume is whole; it used to be deleted and made
// again on the next CreateVolume.
func TestDefect_AQuotaWhoseReplyWasLostIsAdoptedNotRolledBack(t *testing.T) {
	arr := newRaceArray()
	arr.lostReply = true
	srv := httptest.NewServer(arr)
	defer srv.Close()
	defer arr.release()

	resp, err := newIntegrationControllerServer(t, srv.URL).CreateVolume(context.Background(), dfxOptedInCreate("pvc-lost"))
	require.NoError(t, err)
	assert.NotEmpty(t, resp.GetVolume().GetVolumeId())

	dirs, quotas, exports, deletes := arr.state()
	assert.Equal(t, [4]int{1, 1, 1, 0}, [4]int{dirs, quotas, exports, deletes})
}

// A waiter whose context ends gets Aborted, which the provisioner retries,
// and the first call is not disturbed.
func TestDefect_AWaitingCreateWhoseContextEndsIsAborted(t *testing.T) {
	arr := newRaceArray()
	arr.holdFirstQuota = true
	srv := httptest.NewServer(arr)
	defer srv.Close()
	defer arr.release()
	cs := newIntegrationControllerServer(t, srv.URL)
	const name = "pvc-race"

	a := createAsync(cs, dfxOptedInCreate(name))
	<-arr.firstQuotaSeen

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := cs.CreateVolume(ctx, dfxOptedInCreate(name))
	assert.Equal(t, codes.Aborted, status.Code(err), "%v", err)

	arr.release()
	require.NoError(t, (<-a).err)
	dirs, quotas, exports, deletes := arr.state()
	assert.Equal(t, [4]int{1, 1, 1, 0}, [4]int{dirs, quotas, exports, deletes})
}

// Different names never wait for each other.
func TestDefect_CreatesOfDifferentNamesDoNotWaitForEachOther(t *testing.T) {
	arr := newRaceArray()
	arr.holdFirstQuota = true
	srv := httptest.NewServer(arr)
	defer srv.Close()
	defer arr.release()
	cs := newIntegrationControllerServer(t, srv.URL)

	a := createAsync(cs, dfxOptedInCreate("pvc-one"))
	<-arr.firstQuotaSeen

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cs.CreateVolume(ctx, dfxOptedInCreate("pvc-two"))
	require.NoError(t, err, "a second claim is provisioned while the first is still at its quota")

	arr.release()
	require.NoError(t, (<-a).err)
}
