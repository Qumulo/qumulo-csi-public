// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// ControllerGetVolumeHealth reports what the array can say about one volume:
// that its directory is gone or is not a directory, that no NFS
// export covers it -- so no pod can mount it, which is how an export removed
// under running pods looks from here -- or that its quota is gone, so
// its size is no longer enforced. The node's NodeGetVolumeHealth says what a
// mount looks like; this says what the array holds.
//
// external-health-monitor v0.19.0 is the caller: it polls each bound volume
// and records the answer on the claim (pvc.status.healthStatus, Kubernetes
// 1.37 with the CSIVolumeHealth gate). Its RBAC reads no Secret, so it sends
// none, and the credential then comes from the classes as for
// ControllerGetVolume -- controller.inventory.enabled. A request that carries
// its own Secret uses that.
//
// A condition is reported, never returned as an error. An array that does not
// answer is an error: nothing about the volume is known.
func (s *ControllerServer) ControllerGetVolumeHealth(
	ctx context.Context,
	req *csi.ControllerGetVolumeHealthRequest,
) (*csi.ControllerGetVolumeHealthResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID must not be empty")
	}

	// A report: acts on nothing it reads, so the export lookup may use the
	// hints a report may (see withReportLookups). The monitor asks about
	// every bound volume every minute.
	ctx = withReportLookups(withSecretMemo(qumulo.WithSubject(ctx, req.GetVolumeId())))
	parsed, err := parseVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: %v", req.GetVolumeId(), err)
	}

	var client qumulo.ClientInterface
	if len(req.GetSecrets()) > 0 {
		client, err = s.clientForVolumeID(parsed, "", req.GetSecrets())
	} else {
		client, err = s.clusterForVolume(ctx, parsed)
	}
	if err != nil {
		return nil, err
	}

	health := &csi.VolumeHealth{VolumeId: req.GetVolumeId()}
	report := func(kind csi.VolumeHealthErrorType, reason, message string) {
		health.HealthStatuses = append(health.HealthStatuses, &csi.VolumeHealth_VolumeHealthEntry{
			Status: kind, Reason: reason, Message: message,
		})
	}
	respond := func() (*csi.ControllerGetVolumeHealthResponse, error) {
		return &csi.ControllerGetVolumeHealthResponse{VolumeHealth: health}, nil
	}

	resolved, resolution, err := resolveVolumePath(ctx, client, parsed, "controllergetvolumehealth")
	if err != nil {
		return nil, err
	}
	switch resolution {
	case volumeGone:
		report(csi.VolumeHealthErrorType_INACCESSIBLE, "VolumeMissing",
			fmt.Sprintf("%s no longer exists on %s; the volume's data is gone from the array", parsed.RealPath, parsed.Server))
		return respond()
	case volumeNotOurs:
		report(csi.VolumeHealthErrorType_INACCESSIBLE, "NotADirectory",
			fmt.Sprintf("%s on %s is not a directory, so it is not this volume", parsed.RealPath, parsed.Server))
		return respond()
	}

	covering, err := findCoveringExport(ctx, client, resolved.RealPath)
	if err != nil {
		return nil, unreachableEndpointError(parsed, err, "listing the exports covering volume %q", parsed.Name)
	}
	if covering == nil {
		report(csi.VolumeHealthErrorType_INACCESSIBLE, "NotExported",
			fmt.Sprintf("no NFS export on %s covers %s, so the volume cannot be mounted; a pod still running "+
				"on it holds a stale mount: %s", parsed.Server, resolved.RealPath, staleMountRemedy))
	}

	if _, err := client.GetQuota(ctx, resolved.FileID); err != nil {
		if !qumulo.IsNotFound(err) {
			return nil, unreachableEndpointError(parsed, err, "reading the quota of volume %q", parsed.Name)
		}
		report(csi.VolumeHealthErrorType_DEGRADED, "QuotaMissing",
			fmt.Sprintf("%s on %s has no quota, so its size is not enforced and the claim's capacity is "+
				"not what bounds it", resolved.RealPath, parsed.Server))
	}

	return respond()
}
