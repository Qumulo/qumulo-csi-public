// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// UnaryServerInterceptor times every RPC and exports what it learned.
//
// It is an interceptor, and not a line inside each handler, on purpose: the
// RPC bodies are the part of this driver with the most invariants per line,
// and a metric is not worth editing one of them. Chained after logRPC in
// main.go, so the log line and the series describe the same call.
//
// It also carries the join between the array's vocabulary and CSI's. A quota
// reading made by pkg/qumulo names a Qumulo file ID; the volume handle that
// file ID belongs to is only knowable here, from the request or the response.
// Both halves meet in the per-RPC sink this installs.
func (r *Registry) UnaryServerInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	ctx, sink := withQuotaSink(ctx)
	// The replication RPCs are labelled with their volume, as the controller
	// RPCs label themselves, so the replication listings they make keep only
	// that volume's relationships.
	if subject := replicationSubject(req); subject != "" && qumulo.SubjectFrom(ctx) == "" {
		ctx = qumulo.WithSubject(ctx, subject)
	}

	start := time.Now()
	resp, err := handler(ctx, req)
	elapsed := time.Since(start)

	r.rpcTotal.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
	r.rpcDuration.WithLabelValues(info.FullMethod).Observe(elapsed.Seconds())

	r.recordVolumes(req, resp, err, sink, start)

	return resp, err
}

// recordVolumes joins the quota readings this RPC made to the volume handles
// it was about, and retires the series for a volume it deleted. started is
// when this RPC began.
func (r *Registry) recordVolumes(req, resp any, err error, sink *quotaSink, started time.Time) {
	// A successful DeleteVolume is the one event that should remove series
	// rather than add them. Left alone, a deleted volume's gauges would sit
	// at their last value for the life of the process and every
	// used/quota ratio alert would keep firing on a volume that is gone.
	//
	// The deletion is recorded before the series are dropped, and both under
	// the deletion log's lock, which a protection or quota write also holds
	// from its check to its write. Dropped first, a report landing between
	// the two found no deletion and wrote the series back, for good.
	if deleted, ok := req.(*csi.DeleteVolumeRequest); ok && err == nil {
		handle := deleted.GetVolumeId()
		r.deletions.record(handle, func() { r.forgetVolume(handle) })
		return
	}

	// A volume provisioned again at a handle this controller deleted is a
	// live volume, so the deletion is forgotten rather than left to silence
	// its series for an hour.
	if created, ok := resp.(*csi.CreateVolumeResponse); ok && err == nil {
		r.deletions.forget(created.GetVolume().GetVolumeId())
	}

	if sink.empty() {
		return
	}

	for _, handle := range volumeHandlesIn(req, resp) {
		name, ok := volumeNameFromHandle(handle)
		if !ok {
			continue
		}
		quota, ok := sink.get(handle)
		if !ok {
			continue
		}
		// A reading taken before this volume's DeleteVolume finished
		// describes a volume that is gone. A ListVolumes that overlapped the
		// delete used to export it again, for good.
		r.deletions.unlessDeletedSince(handle, started, func() {
			id := volumeID(handle)
			r.volumeQuotaBytes.WithLabelValues(id, name).Set(float64(quota.LimitBytes))
			if quota.UsedKnown {
				r.volumeUsedBytes.WithLabelValues(id, name).Set(float64(quota.UsedBytes))
			}
			// So the reconcile can tell this reading from one frozen in a
			// replica that no longer serves the volume.
			r.noteQuotaWritten(id)
		})
	}
}

// forgetVolume drops every per-volume gauge for one handle.
//
// The protected gauge included: left behind, a deleted volume that was
// protected went on reading 1, and one that was not went on reading 0 -- which
// is QumuloCSIVolumeUnprotected firing, for ever, for a volume that no longer
// exists.
func (r *Registry) forgetVolume(handle string) {
	id := volumeID(handle)
	// Separate from forgetVolumeID, which ReconcileVolumes calls with
	// volumes.mu already held.
	r.forgetVolumeWrites(id)
	r.forgetVolumeID(id)
}

// forgetVolumeWrites drops when this process last wrote a volume's readings.
// Must not be called with volumes.mu held.
func (r *Registry) forgetVolumeWrites(id string) {
	r.volumes.mu.Lock()
	defer r.volumes.mu.Unlock()
	delete(r.volumes.quotaAt, id)
	delete(r.volumes.protectedAt, id)
}

// forgetVolumeID drops every per-volume gauge whose volume_id is id.
func (r *Registry) forgetVolumeID(id string) {
	labels := prometheus.Labels{"volume_id": id}
	r.volumeQuotaBytes.DeletePartialMatch(labels)
	r.volumeUsedBytes.DeletePartialMatch(labels)
	r.volumeProtected.DeletePartialMatch(labels)
}

// deletionLog remembers when each volume was deleted, long enough for any
// RPC that was already running to finish.
//
// Keyed by the canonical handle (canonicalHandle), the spelling volume_id is
// derived from: a volume an older build provisioned is deleted under its
// legacy "<server>#<path>" handle, while a group report names the same volume
// in the current form, and keyed as written the deletion did not stop that
// report bringing the series back.
//
// Its lock is also held across the write a check guards, and across the
// series removal a delete makes, so a report cannot check before a delete
// is recorded and write after its series are dropped. Lock order is
// mu, then the registry's volumes.mu; nothing takes them the other way.
type deletionLog struct {
	mu sync.Mutex
	at map[string]time.Time
}

// deletionMemory is how long a deletion is remembered: longer than any RPC
// runs, a restore's included.
const deletionMemory = time.Hour

// record notes handle's deletion and then runs drop, the removal of its
// series, before any guarded write can run.
func (d *deletionLog) record(handle string, drop func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if d.at == nil {
		d.at = map[string]time.Time{}
	}
	for h, when := range d.at {
		if now.Sub(when) > deletionMemory {
			delete(d.at, h)
		}
	}
	d.at[canonicalHandle(handle)] = now
	if drop != nil {
		drop()
	}
}

// unlessDeleted runs write unless handle was deleted recently enough to
// still be remembered, whenever that was. Used where there is no RPC start
// to compare with: a reading about a volume this controller has deleted
// describes nothing.
func (d *deletionLog) unlessDeleted(handle string, write func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	when, ok := d.at[canonicalHandle(handle)]
	if ok && time.Since(when) <= deletionMemory {
		return
	}
	write()
}

// forget drops a handle's deletion, so a volume provisioned again at the
// same handle gets its series back at once rather than staying silent for
// deletionMemory.
func (d *deletionLog) forget(handle string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.at, canonicalHandle(handle))
}

// unlessDeletedSince runs write unless handle was deleted at or after
// started.
func (d *deletionLog) unlessDeletedSince(handle string, started time.Time, write func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if when, ok := d.at[canonicalHandle(handle)]; ok && !when.Before(started) {
		return
	}
	write()
}

// handlePath is the absolute cluster path a volume handle names, the last
// of its fields, or false for anything that is not a handle.
func handlePath(handle string) (string, bool) {
	const handleParts, legacyHandleParts = 3, 2
	fields := strings.Split(handle, "#")
	if len(fields) == legacyHandleParts {
		fields = []string{fields[0], "", fields[1]}
	}
	if len(fields) != handleParts {
		return "", false
	}
	server, realPath := fields[0], fields[2]
	if server == "" || !path.IsAbs(realPath) {
		return "", false
	}
	return realPath, true
}

// replicationSubject is the volume handle a csi-addons replication request
// is about, read as the driver reads it: replication_source first, then the
// legacy volume_id. "" for every other request.
func replicationSubject(req any) string {
	type sourced interface {
		GetVolumeId() string
		GetReplicationSource() *replication.ReplicationSource
	}
	switch req.(type) {
	case *replication.EnableVolumeReplicationRequest, *replication.DisableVolumeReplicationRequest,
		*replication.PromoteVolumeRequest, *replication.DemoteVolumeRequest,
		*replication.ResyncVolumeRequest, *replication.GetVolumeReplicationInfoRequest:
		r := req.(sourced)
		if id := r.GetReplicationSource().GetVolume().GetVolumeId(); id != "" {
			return id
		}
		return r.GetVolumeId()
	}
	return ""
}

// volumeHandlesIn returns every CSI volume handle named by one RPC.
//
// Only the controller RPCs that actually touch a quota are listed, because a
// handle from an RPC that read no quota joins to nothing. The request is
// consulted for the RPCs that are handed a handle, and the response for the
// two that mint or enumerate them.
func volumeHandlesIn(req, resp any) []string {
	switch typed := resp.(type) {
	case *csi.CreateVolumeResponse:
		// CreateVolume is told a name, not a handle: the handle only exists
		// once the directory does, so it comes from the response.
		return []string{typed.GetVolume().GetVolumeId()}
	case *csi.ListVolumesResponse:
		handles := make([]string, 0, len(typed.GetEntries()))
		for _, entry := range typed.GetEntries() {
			handles = append(handles, entry.GetVolume().GetVolumeId())
		}
		return handles
	}

	switch typed := req.(type) {
	case *csi.ControllerExpandVolumeRequest:
		return []string{typed.GetVolumeId()}
	case *csi.ValidateVolumeCapabilitiesRequest:
		return []string{typed.GetVolumeId()}
	case *csi.ControllerGetVolumeRequest:
		return []string{typed.GetVolumeId()}
	case *csi.CreateSnapshotRequest:
		return []string{typed.GetSourceVolumeId()}
	}

	return nil
}

// volumeID is the volume_id label for a CSI volume handle: the first 16 hex
// digits of the handle's SHA-256.
//
// The label used to be the handle itself, which names the array address, the
// management endpoint and the path of every volume -- the list an
// unauthenticated reader of /metrics needs to mount exports directly.
// volume_name alone cannot replace it: a DR copy has the same path, and so
// the same name, on both arrays, and the two series would collide. A digest
// keeps each volume's series apart and names nothing; to find the series for
// a known handle, hash it the same way (docs/reference/metrics.md). Joins to
// kube-state-metrics go through volume_name.
func volumeID(handle string) string {
	sum := sha256.Sum256([]byte(canonicalHandle(handle)))
	return hex.EncodeToString(sum[:8])
}

// canonicalHandle is the one spelling of a handle that every label is
// derived from.
//
// One volume has two handles in a cluster that has been upgraded: the
// current "<server>#<apiUrl>#<path>" and the legacy "<server>#<path>" the
// driver still decodes. They name the same directory on the same
// array, but they are different strings, so hashing them produced two
// volume_id values for one volume -- and whichever RPC happened to carry the
// other spelling wrote a second series that the reconcile then kept alive,
// against a PersistentVolume that has only one of them. A rate or ratio
// alert computed across them is computed across a volume counted twice.
//
// The path is cleaned for the same reason: "/vol" and "/vol/" are one
// directory, and the driver cleans the path when it mints the handle, so an
// uncleaned one came from somewhere else and must not get its own series.
//
// Anything that does not parse as a handle is hashed as it stands: it is not
// this driver's, and inventing a spelling for it would be guessing.
func canonicalHandle(handle string) string {
	realPath, ok := handlePath(handle)
	if !ok {
		return handle
	}
	server := strings.SplitN(handle, "#", 2)[0]
	apiURL := ""
	if fields := strings.Split(handle, "#"); len(fields) == 3 {
		apiURL = fields[1]
	}
	return server + "#" + apiURL + "#" + path.Clean(realPath)
}

// volumeNameFromHandle pulls the volume's name out of a CSI volume handle,
// for the gauge's volume_name label.
//
// Only the name. The join to a quota reading is the subject the driver put on
// the request (see ObserveQuota), not anything dug out of this string -- a
// handle that changes shape must not be able to silently unhook the gauges
// again, and this parser is now cosmetic.
//
// The handle is "<server>#<apiUrl>#<absolute path>", with the endpoint field
// empty when it is the default, or the older "<server>#<absolute path>" the
// driver still decodes. Only the last field is read, so the endpoint
// never reaches a label: it is an address, it varies between otherwise
// identical clusters, and a metrics label is the wrong place for either. This
// package parses rather than imports because pkg/driver imports this one.
func volumeNameFromHandle(handle string) (name string, ok bool) {
	realPath, ok := handlePath(handle)
	if !ok {
		return "", false
	}
	base := path.Base(path.Clean(realPath))
	if base == "/" || base == "." {
		return "", false
	}
	return base, true
}
