// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Command csi-probe drives a running qumulo-csi-driver through a full
// volume and snapshot lifecycle and reports what happened at each step.
//
// It exists to answer the questions unit tests cannot: what a real Qumulo
// cluster returns for a duplicate directory, whether a repeated
// CreateSnapshot is recognised as the same snapshot, and whether a quota is
// what statfs reports through an NFS mount. Every step prints its gRPC
// status, so the output stands alone as a test record.
//
// Controller steps need only network reachability to the driver. Node steps
// perform a real NFS mount and so must run as root on a host that can reach
// the cluster's data endpoint.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type options struct {
	endpoint          string
	server            string
	storeRealPath     string
	storeExportPath   string
	protocol          string
	provisioningModel string
	volumeName        string
	capacityGiB       int64
	expandToGiB       int64
	targetPath        string
	mountFlags        string
	skipController    bool
	skipNode          bool
	keep              bool
	timeout           time.Duration
}

func main() {
	var opt options

	flag.StringVar(&opt.endpoint, "endpoint", "tcp://127.0.0.1:10000", "driver endpoint (tcp://host:port or unix:///path)")
	flag.StringVar(&opt.server, "server", "", "Qumulo server the node should mount from (StorageClass 'server')")
	flag.StringVar(&opt.storeRealPath, "store-real-path", "", "cluster path holding volume directories (StorageClass 'storeRealPath')")
	flag.StringVar(&opt.storeExportPath, "store-export-path", "", "NFS export path of that parent (StorageClass 'storeExportPath')")
	flag.StringVar(&opt.protocol, "protocol", "", "NFS version to request via the StorageClass 'protocol' parameter: nfs3, nfs4.1, or empty to let the kernel negotiate")
	flag.StringVar(&opt.provisioningModel, "provisioning-model", "", "StorageClass 'provisioningModel': shared-subdirectory, dedicated-export, or empty for the driver default")
	flag.StringVar(&opt.volumeName, "name", "csi-probe-1", "volume name to create")
	flag.Int64Var(&opt.capacityGiB, "capacity-gib", 1, "capacity to request")
	flag.Int64Var(&opt.expandToGiB, "expand-to-gib", 2, "capacity to expand to")
	flag.StringVar(&opt.targetPath, "target-path", "", "mount point for node steps; empty skips them. Requires root")
	flag.StringVar(&opt.mountFlags, "mount-flags", "", "comma-separated NFS mount options, e.g. nfsvers=4.1 or nfsvers=3,nolock")
	flag.BoolVar(&opt.skipController, "skip-controller", false, "skip controller steps")
	flag.BoolVar(&opt.skipNode, "skip-node", false, "skip node steps")
	flag.BoolVar(&opt.keep, "keep", false, "leave the volume behind instead of deleting it")
	flag.DurationVar(&opt.timeout, "timeout", 5*time.Minute, "overall timeout")
	flag.Parse()

	if err := run(opt); err != nil {
		fmt.Println("\nPROBE FAILED:", err)
		os.Exit(1)
	}
}

// probe carries the state that later steps need from earlier ones.
type probe struct {
	opt      options
	identity csi.IdentityClient
	ctrl     csi.ControllerClient
	node     csi.NodeClient

	// ctx bounds the whole run; each step derives a shorter one from it.
	ctx      context.Context
	volumeID string
	// volumeContext is passed to NodePublishVolume exactly as CreateVolume
	// returned it -- that handoff is the contract between the two services,
	// and faking it here would test nothing.
	volumeContext map[string]string
	snapshotID    string
	failures      int
}

func run(opt options) error {
	target := strings.TrimPrefix(strings.TrimPrefix(opt.endpoint, "tcp://"), "unix://")
	if strings.HasPrefix(opt.endpoint, "unix://") {
		target = "unix://" + target
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dialing %s: %w", opt.endpoint, err)
	}
	// Explicitly discarded: the process is exiting either way, and a failed
	// close on a gRPC connection is not something a probe run can act on.
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), opt.timeout)
	defer cancel()

	p := &probe{
		ctx:      ctx,
		opt:      opt,
		identity: csi.NewIdentityClient(conn),
		ctrl:     csi.NewControllerClient(conn),
		node:     csi.NewNodeClient(conn),
	}

	fmt.Printf("csi-probe against %s at %s\n", opt.endpoint, time.Now().Format(time.RFC3339))
	fmt.Println(strings.Repeat("=", 72))

	p.section("identity")
	p.step("GetPluginInfo", p.getPluginInfo)
	p.step("GetPluginCapabilities", p.getPluginCapabilities)
	p.step("Probe", p.probeReady)

	if !opt.skipController {
		if opt.server == "" || opt.storeRealPath == "" || opt.storeExportPath == "" {
			return fmt.Errorf("-server, -store-real-path and -store-export-path are required for controller steps")
		}

		p.section("controller")
		p.step("ControllerGetCapabilities", p.controllerCapabilities)
		p.step("CreateVolume", p.createVolume)
		p.step("CreateVolume again (idempotent: same volume expected)", p.createVolumeAgain)
		p.step("CreateVolume, different capacity (expect AlreadyExists)", p.createVolumeMismatch)
		p.step("ValidateVolumeCapabilities", p.validateCapabilities)
		p.step("ControllerExpandVolume (grow)", p.expandVolume)
		p.step("ControllerExpandVolume (shrink: expect no-op)", p.expandVolumeSmaller)
		p.step("CreateSnapshot", p.createSnapshot)
		p.step("CreateSnapshot again (idempotent: SAME id expected)", p.createSnapshotAgain)
		p.step("ListSnapshots unfiltered (expect refusal)", p.listSnapshots)
		p.step("ListSnapshots by source volume", p.listSnapshotsBySource)
		p.step("ListSnapshots by snapshot id", p.listSnapshotsByID)
	}

	if !opt.skipNode && opt.targetPath != "" {
		p.section("node (real mount)")
		p.step("NodeGetInfo", p.nodeGetInfo)
		p.step("NodeGetCapabilities", p.nodeCapabilities)
		p.step("NodePublishVolume", p.nodePublish)
		p.step("NodeGetVolumeStats", p.nodeStats)
		p.step("NodePublishVolume again (idempotent)", p.nodePublishAgain)
		p.step("NodeUnpublishVolume", p.nodeUnpublish)
		p.step("NodeUnpublishVolume again (idempotent)", p.nodeUnpublishAgain)
	}

	if !opt.skipController {
		p.section("cleanup")
		if opt.keep {
			fmt.Println("SKIP  DeleteSnapshot/DeleteVolume (-keep set)")
			fmt.Println("      volume id:", p.volumeID)
		} else {
			p.step("DeleteSnapshot", p.deleteSnapshot)
			p.step("DeleteVolume", p.deleteVolume)
			p.step("DeleteVolume again (idempotent)", p.deleteVolumeAgain)
		}
	}

	fmt.Println(strings.Repeat("=", 72))
	if p.failures > 0 {
		return fmt.Errorf("%d step(s) failed", p.failures)
	}
	fmt.Println("all steps passed")
	return nil
}

func (p *probe) section(name string) {
	fmt.Printf("\n--- %s ---\n", name)
}

// step runs one operation and records the outcome. A failure is reported and
// the run continues: later steps often still yield useful information, and a
// single record of everything beats stopping at the first surprise.
func (p *probe) step(name string, fn func(context.Context) (string, error)) {
	ctx, cancel := context.WithTimeout(p.ctx, 90*time.Second)
	defer cancel()

	detail, err := fn(ctx)
	if err != nil {
		p.failures++
		fmt.Printf("FAIL  %s\n        code=%s err=%v\n", name, status.Code(err), err)
		return
	}
	fmt.Printf("PASS  %s\n        %s\n", name, detail)
}

func (p *probe) getPluginInfo(ctx context.Context) (string, error) {
	resp, err := p.identity.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("name=%s version=%s", resp.GetName(), resp.GetVendorVersion()), nil
}

func (p *probe) getPluginCapabilities(ctx context.Context) (string, error) {
	resp, err := p.identity.GetPluginCapabilities(ctx, &csi.GetPluginCapabilitiesRequest{})
	if err != nil {
		return "", err
	}

	var caps []string
	for _, c := range resp.GetCapabilities() {
		if s := c.GetService(); s != nil {
			caps = append(caps, s.GetType().String())
		}
		if e := c.GetVolumeExpansion(); e != nil {
			caps = append(caps, "EXPANSION_"+e.GetType().String())
		}
	}
	return strings.Join(caps, " "), nil
}

func (p *probe) probeReady(ctx context.Context) (string, error) {
	resp, err := p.identity.Probe(ctx, &csi.ProbeRequest{})
	if err != nil {
		return "", err
	}
	// Liveness of the driver process, not reachability of any cluster: Probe
	// contacts no cluster (see IdentityServer.Probe). Saying otherwise here
	// was worse than in the install notes -- this is the tool an operator
	// reaches for when provisioning is already failing.
	return fmt.Sprintf("ready=%v (driver process is live; contacts no cluster)", resp.GetReady().GetValue()), nil
}

func (p *probe) controllerCapabilities(ctx context.Context) (string, error) {
	resp, err := p.ctrl.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		return "", err
	}

	var caps []string
	for _, c := range resp.GetCapabilities() {
		caps = append(caps, c.GetRpc().GetType().String())
	}
	return strings.Join(caps, " "), nil
}

func (p *probe) params() map[string]string {
	params := map[string]string{
		"server":          p.opt.server,
		"storeRealPath":   p.opt.storeRealPath,
		"storeExportPath": p.opt.storeExportPath,
	}
	// Only sent when asked for. The driver treats an absent protocol as "let
	// the kernel negotiate", and sending an empty value would instead be a
	// parameter it has to reject.
	if p.opt.protocol != "" {
		params["protocol"] = p.opt.protocol
	}
	if p.opt.provisioningModel != "" {
		params["provisioningModel"] = p.opt.provisioningModel
		if p.opt.provisioningModel == "dedicated-export" {
			// The probe's exports were always open; the driver now asks that
			// an open class say so.
			params["allowAnyClient"] = "true"
		}
	}
	return params
}

func mountCapability() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
	}}
}

func (p *probe) createRequest(capacityGiB int64) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:               p.opt.volumeName,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: capacityGiB << 30},
		VolumeCapabilities: mountCapability(),
		Parameters:         p.params(),
	}
}

func (p *probe) createVolume(ctx context.Context) (string, error) {
	resp, err := p.ctrl.CreateVolume(ctx, p.createRequest(p.opt.capacityGiB))
	if err != nil {
		return "", err
	}

	p.volumeID = resp.GetVolume().GetVolumeId()
	p.volumeContext = resp.GetVolume().GetVolumeContext()
	return fmt.Sprintf("id=%s capacity=%d context=%v",
		p.volumeID, resp.GetVolume().GetCapacityBytes(), p.volumeContext), nil
}

// createVolumeAgain repeats the request the CO would retry. The cluster has
// to report the name collision and the driver has to adopt the existing
// directory, so an id that differs here means retries would leak volumes.
func (p *probe) createVolumeAgain(ctx context.Context) (string, error) {
	resp, err := p.ctrl.CreateVolume(ctx, p.createRequest(p.opt.capacityGiB))
	if err != nil {
		return "", err
	}

	got := resp.GetVolume().GetVolumeId()
	if got != p.volumeID {
		return "", fmt.Errorf("NOT IDEMPOTENT: first call gave %s, second gave %s", p.volumeID, got)
	}
	return "same id returned: " + got, nil
}

func (p *probe) createVolumeMismatch(ctx context.Context) (string, error) {
	_, err := p.ctrl.CreateVolume(ctx, p.createRequest(p.opt.capacityGiB+7))
	if status.Code(err) == codesAlreadyExists {
		return "AlreadyExists, as the spec requires: " + status.Convert(err).Message(), nil
	}
	if err != nil {
		return "", fmt.Errorf("wanted AlreadyExists, got %s: %w", status.Code(err), err)
	}
	return "", fmt.Errorf("wanted AlreadyExists, got success -- a name reused with a different size was accepted")
}

func (p *probe) validateCapabilities(ctx context.Context) (string, error) {
	resp, err := p.ctrl.ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId:           p.volumeID,
		VolumeCapabilities: mountCapability(),
		VolumeContext:      p.volumeContext,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("confirmed=%v message=%q", resp.GetConfirmed() != nil, resp.GetMessage()), nil
}

func (p *probe) expandVolume(ctx context.Context) (string, error) {
	resp, err := p.ctrl.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      p.volumeID,
		CapacityRange: &csi.CapacityRange{RequiredBytes: p.opt.expandToGiB << 30},
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("capacity=%d nodeExpansionRequired=%v",
		resp.GetCapacityBytes(), resp.GetNodeExpansionRequired()), nil
}

func (p *probe) expandVolumeSmaller(ctx context.Context) (string, error) {
	resp, err := p.ctrl.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      p.volumeID,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
	})
	if err != nil {
		return "", err
	}
	if resp.GetCapacityBytes() < p.opt.expandToGiB<<30 {
		return "", fmt.Errorf("quota shrank to %d; a smaller request must be a no-op", resp.GetCapacityBytes())
	}
	return fmt.Sprintf("unchanged at %d, as expected", resp.GetCapacityBytes()), nil
}

func (p *probe) createSnapshot(ctx context.Context) (string, error) {
	resp, err := p.ctrl.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           p.opt.volumeName + "-snap",
		SourceVolumeId: p.volumeID,
	})
	if err != nil {
		return "", err
	}

	p.snapshotID = resp.GetSnapshot().GetSnapshotId()
	return fmt.Sprintf("id=%s source=%s created=%s ready=%v",
		p.snapshotID,
		resp.GetSnapshot().GetSourceVolumeId(),
		resp.GetSnapshot().GetCreationTime().AsTime().Format(time.RFC3339),
		resp.GetSnapshot().GetReadyToUse()), nil
}

// createSnapshotAgain is the highest-value check in the probe. The driver
// matches an existing snapshot by name suffix, because Qumulo prepends the
// snapshot ID to the requested name with a separator the API guide does not
// document. If that match fails, this returns a second ID and every CO retry
// would leave another snapshot on the cluster.
func (p *probe) createSnapshotAgain(ctx context.Context) (string, error) {
	resp, err := p.ctrl.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           p.opt.volumeName + "-snap",
		SourceVolumeId: p.volumeID,
	})
	if err != nil {
		return "", err
	}

	got := resp.GetSnapshot().GetSnapshotId()
	if got != p.snapshotID {
		return "", fmt.Errorf("NOT IDEMPOTENT: first snapshot %s, second %s -- name-suffix matching failed on this cluster",
			p.snapshotID, got)
	}
	return "same id returned: " + got, nil
}

// listSnapshots checks that an unfiltered listing is REFUSED.
//
// One controller serves many clusters and is never told the full set, so
// "every snapshot everywhere" is a question it cannot answer. Returning an
// empty list would let a CO conclude snapshots had been deleted, so the driver
// refuses instead -- and this step passes on that refusal, not on a listing.
func (p *probe) listSnapshots(ctx context.Context) (string, error) {
	_, err := p.ctrl.ListSnapshots(ctx, &csi.ListSnapshotsRequest{})
	if status.Code(err) != codes.InvalidArgument {
		return "", fmt.Errorf("unfiltered ListSnapshots returned %s, want InvalidArgument: %w",
			status.Code(err), err)
	}
	return "refused with InvalidArgument, as a multi-cluster controller must", nil
}

func (p *probe) listSnapshotsBySource(ctx context.Context) (string, error) {
	resp, err := p.ctrl.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SourceVolumeId: p.volumeID})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d entries for this volume", len(resp.GetEntries())), nil
}

func (p *probe) listSnapshotsByID(ctx context.Context) (string, error) {
	resp, err := p.ctrl.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SnapshotId: p.snapshotID})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d entries for snapshot %s", len(resp.GetEntries()), p.snapshotID), nil
}

func (p *probe) nodeGetInfo(ctx context.Context) (string, error) {
	resp, err := p.node.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if err != nil {
		return "", err
	}
	return "nodeID=" + resp.GetNodeId(), nil
}

func (p *probe) nodeCapabilities(ctx context.Context) (string, error) {
	resp, err := p.node.NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		return "", err
	}

	var caps []string
	for _, c := range resp.GetCapabilities() {
		caps = append(caps, c.GetRpc().GetType().String())
	}
	return strings.Join(caps, " "), nil
}

func (p *probe) publishRequest() *csi.NodePublishVolumeRequest {
	capability := mountCapability()[0]
	if p.opt.mountFlags != "" {
		capability.GetMount().MountFlags = strings.Split(p.opt.mountFlags, ",")
	}

	return &csi.NodePublishVolumeRequest{
		VolumeId:         p.volumeID,
		TargetPath:       p.opt.targetPath,
		VolumeCapability: capability,
		VolumeContext:    p.volumeContext,
	}
}

func (p *probe) nodePublish(ctx context.Context) (string, error) {
	if p.volumeID == "" {
		return "", fmt.Errorf("no volume to publish; controller steps must run first")
	}
	if _, err := p.node.NodePublishVolume(ctx, p.publishRequest()); err != nil {
		return "", err
	}
	return fmt.Sprintf("mounted at %s from context %v", p.opt.targetPath, p.volumeContext), nil
}

func (p *probe) nodePublishAgain(ctx context.Context) (string, error) {
	if _, err := p.node.NodePublishVolume(ctx, p.publishRequest()); err != nil {
		return "", err
	}
	return "second publish accepted (already-mounted path)", nil
}

// nodeStats reports what statfs sees through the mount. Compared with the
// quota just set, it answers whether a quota'd directory presents itself as
// a filesystem of that size -- which is what makes a PVC's usage metrics
// mean anything.
func (p *probe) nodeStats(ctx context.Context) (string, error) {
	resp, err := p.node.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
		VolumeId:   p.volumeID,
		VolumePath: p.opt.targetPath,
	})
	if err != nil {
		return "", err
	}

	var parts []string
	for _, u := range resp.GetUsage() {
		parts = append(parts, fmt.Sprintf("%s total=%d used=%d available=%d",
			u.GetUnit(), u.GetTotal(), u.GetUsed(), u.GetAvailable()))
	}
	parts = append(parts, fmt.Sprintf("(quota set to %d)", p.opt.expandToGiB<<30))
	return strings.Join(parts, " | "), nil
}

func (p *probe) nodeUnpublish(ctx context.Context) (string, error) {
	if _, err := p.node.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   p.volumeID,
		TargetPath: p.opt.targetPath,
	}); err != nil {
		return "", err
	}
	return "unmounted " + p.opt.targetPath, nil
}

func (p *probe) nodeUnpublishAgain(ctx context.Context) (string, error) {
	if _, err := p.node.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   p.volumeID,
		TargetPath: p.opt.targetPath,
	}); err != nil {
		return "", err
	}
	return "second unpublish accepted (already gone)", nil
}

func (p *probe) deleteSnapshot(ctx context.Context) (string, error) {
	if p.snapshotID == "" {
		return "nothing to delete", nil
	}
	if _, err := p.ctrl.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: p.snapshotID}); err != nil {
		return "", err
	}
	return "deleted " + p.snapshotID, nil
}

// deleteVolume tolerates Aborted: DeleteVolume waits a bounded time for the
// cluster's asynchronous tree-delete and asks the CO to retry if the walk
// outlives it, which is a correct outcome rather than a failure.
func (p *probe) deleteVolume(ctx context.Context) (string, error) {
	if p.volumeID == "" {
		return "nothing to delete", nil
	}

	for attempt := 1; attempt <= 10; attempt++ {
		_, err := p.ctrl.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: p.volumeID})
		if err == nil {
			return fmt.Sprintf("deleted after %d attempt(s)", attempt), nil
		}
		if status.Code(err) != codesAborted {
			return "", err
		}
		fmt.Printf("        attempt %d: tree delete still running (Aborted), retrying as a CO would\n", attempt)
		time.Sleep(5 * time.Second)
	}
	return "", fmt.Errorf("tree delete still running after 10 attempts")
}

func (p *probe) deleteVolumeAgain(ctx context.Context) (string, error) {
	if _, err := p.ctrl.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: p.volumeID}); err != nil {
		return "", err
	}
	return "second delete accepted (already gone)", nil
}
