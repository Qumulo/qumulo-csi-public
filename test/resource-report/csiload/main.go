// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Command csiload drives a running qumulo-csi-driver over its CSI gRPC sockets through a volume lifecycle and
// records every RPC as one JSON line, so a sampler running beside it can attribute the driver's resource use to
// phases. It is the load generator of the resource-report suite (test/resource-report/README.md).
//
//	csiload -endpoint unix:///run/ctrl.sock -node-endpoint unix:///run/node.sock \
//	        -server <nfs address> -apiurl https://<rest address>:8000 -token-file <token file> \
//	        -realpath <store real path> -exportpath <store export path> \
//	        -prefix csi-rcr-b -n 50 -concurrency 8 [-snapshots 10] [-expand 10] [-publish 10 -target-root /mnt/x]
//
// The token file holds the raw bearer token or the JSON login response. The token is read into memory and sent in
// each request's Secrets map, the way the CSI sidecars do. Every volume and snapshot it creates carries -prefix,
// and it deletes everything it created before exiting.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type record struct {
	TS     string  `json:"ts"`
	Phase  string  `json:"phase"`
	Op     string  `json:"op,omitempty"`
	Volume string  `json:"volume,omitempty"`
	MS     float64 `json:"ms,omitempty"`
	Code   string  `json:"code,omitempty"`
	Err    string  `json:"err,omitempty"`
	Event  string  `json:"event,omitempty"`
	// Retries counts Aborted answers retried before this result, as a CO's sidecar would.
	Retries int `json:"retries,omitempty"`
}

var (
	outMu sync.Mutex
	enc   = json.NewEncoder(os.Stdout)
)

func emit(r record) {
	r.TS = time.Now().UTC().Format(time.RFC3339Nano)
	outMu.Lock()
	defer outMu.Unlock()
	_ = enc.Encode(r)
}

func main() {
	endpoint := flag.String("endpoint", "", "controller socket, unix:///path")
	nodeEndpoint := flag.String("node-endpoint", "", "node socket (default: -endpoint)")
	server := flag.String("server", "", "NFS server address (StorageClass server)")
	apiURL := flag.String("apiurl", "", "REST URL (StorageClass apiUrl)")
	tokenFile := flag.String("token-file", "", "file holding the array token (one line)")
	realPath := flag.String("realpath", "", "storeRealPath")
	exportPath := flag.String("exportpath", "", "storeExportPath")
	model := flag.String("model", "shared-subdirectory", "provisioningModel")
	protocol := flag.String("protocol", "nfs4.1", "protocol")
	prefix := flag.String("prefix", "csi-rcr", "name prefix for every object created")
	n := flag.Int("n", 20, "volumes to create")
	conc := flag.Int("concurrency", 4, "parallel RPCs per phase")
	nSnap := flag.Int("snapshots", 0, "volumes to snapshot (create+delete)")
	nExpand := flag.Int("expand", 0, "volumes to expand 1Gi -> 2Gi")
	nPublish := flag.Int("publish", 0, "volumes to NodePublish/stat/unpublish (needs node mode, root)")
	targetRoot := flag.String("target-root", "", "directory for publish targets")
	hold := flag.Duration("hold", 0, "keep published mounts this long before unpublishing")
	idle := flag.Duration("idle", 30*time.Second, "idle gap between phases (sampler sees settle)")
	flag.Parse()

	raw, err := os.ReadFile(*tokenFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "token:", err)
		os.Exit(2)
	}
	token := strings.TrimSpace(string(raw))
	if strings.HasPrefix(token, "{") { // a JSON login response
		var login struct {
			BearerToken string `json:"bearer_token"`
		}
		if err := json.Unmarshal([]byte(token), &login); err != nil || login.BearerToken == "" {
			fmt.Fprintln(os.Stderr, "token: file looks like JSON but has no bearer_token")
			os.Exit(2)
		}
		token = login.BearerToken
	}
	secrets := map[string]string{"token": token}
	params := map[string]string{
		"server": *server, "apiUrl": *apiURL, "storeRealPath": *realPath, "storeExportPath": *exportPath,
		"protocol": *protocol, "provisioningModel": *model,
	}
	if *model == "dedicated-export" {
		// The load's exports were always open; the driver now asks that an
		// open class say so.
		params["allowAnyClient"] = "true"
	}

	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(2)
	}
	defer func() { _ = conn.Close() }()
	nodeConn := conn
	if *nodeEndpoint != "" && *nodeEndpoint != *endpoint {
		if nodeConn, err = grpc.NewClient(*nodeEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
			fmt.Fprintln(os.Stderr, "dial node:", err)
			os.Exit(2)
		}
		defer func() { _ = nodeConn.Close() }()
	}
	ctrl, node := csi.NewControllerClient(conn), csi.NewNodeClient(nodeConn)
	mountCap := []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}}

	call := func(phase, op, vol string, f func(ctx context.Context) error) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		t0 := time.Now()
		// Aborted is the CSI "still in progress, call again" answer (a tree-delete or restore that outlived the
		// per-call budget). external-provisioner retries it with backoff, so the load does too; anything else is final.
		var err error
		retries := 0
		for {
			err = f(ctx)
			if status.Code(err) != codes.Aborted || retries >= 20 || ctx.Err() != nil {
				break
			}
			retries++
			time.Sleep(time.Duration(min(retries, 5)) * time.Second)
		}
		r := record{Phase: phase, Op: op, Volume: vol, MS: float64(time.Since(t0).Microseconds()) / 1000, Code: "OK",
			Retries: retries}
		if err != nil {
			s, _ := status.FromError(err)
			r.Code, r.Err = s.Code().String(), s.Message()
			if len(r.Err) > 300 {
				r.Err = r.Err[:300]
			}
		}
		emit(r)
		return err
	}
	parallel := func(phase string, items []string, fn func(string)) {
		emit(record{Phase: phase, Event: "start"})
		sem := make(chan struct{}, *conc)
		var wg sync.WaitGroup
		for _, it := range items {
			wg.Add(1)
			sem <- struct{}{}
			go func(it string) { defer wg.Done(); defer func() { <-sem }(); fn(it) }(it)
		}
		wg.Wait()
		emit(record{Phase: phase, Event: "end"})
		emit(record{Phase: phase + "-idle", Event: "start"})
		time.Sleep(*idle)
		emit(record{Phase: phase + "-idle", Event: "end"})
	}

	names := make([]string, *n)
	for i := range names {
		names[i] = fmt.Sprintf("%s-%03d", *prefix, i)
	}
	var mu sync.Mutex
	vols := map[string]*csi.Volume{}

	parallel("create", names, func(name string) {
		_ = call("create", "CreateVolume", name, func(ctx context.Context) error {
			resp, err := ctrl.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: name, Parameters: params,
				VolumeCapabilities: mountCap, Secrets: secrets,
				CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}})
			if err == nil {
				mu.Lock()
				vols[name] = resp.GetVolume()
				mu.Unlock()
			}
			return err
		})
	})
	created := make([]string, 0, len(vols))
	for _, nm := range names {
		if vols[nm] != nil {
			created = append(created, nm)
		}
	}
	first := func(k int) []string {
		if k > len(created) {
			k = len(created)
		}
		return created[:k]
	}

	if *nExpand > 0 {
		parallel("expand", first(*nExpand), func(name string) {
			_ = call("expand", "ControllerExpandVolume", name, func(ctx context.Context) error {
				_, err := ctrl.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
					VolumeId: vols[name].GetVolumeId(), Secrets: secrets, VolumeCapability: mountCap[0],
					CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30}})
				return err
			})
		})
	}

	if *nSnap > 0 {
		snaps := map[string]string{}
		parallel("snapshot-create", first(*nSnap), func(name string) {
			_ = call("snapshot-create", "CreateSnapshot", name, func(ctx context.Context) error {
				resp, err := ctrl.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: name + "-snap",
					SourceVolumeId: vols[name].GetVolumeId(), Secrets: secrets})
				if err == nil {
					mu.Lock()
					snaps[name] = resp.GetSnapshot().GetSnapshotId()
					mu.Unlock()
				}
				return err
			})
		})
		withSnap := []string{}
		for _, nm := range first(*nSnap) {
			if snaps[nm] != "" {
				withSnap = append(withSnap, nm)
			}
		}
		parallel("snapshot-delete", withSnap, func(name string) {
			_ = call("snapshot-delete", "DeleteSnapshot", name, func(ctx context.Context) error {
				_, err := ctrl.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snaps[name], Secrets: secrets})
				return err
			})
		})
	}

	if *nPublish > 0 {
		targets := map[string]string{}
		for _, nm := range first(*nPublish) {
			targets[nm] = filepath.Join(*targetRoot, nm)
		}
		published := map[string]bool{}
		parallel("publish", first(*nPublish), func(name string) {
			err := call("publish", "NodePublishVolume", name, func(ctx context.Context) error {
				_, err := node.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
					VolumeId: vols[name].GetVolumeId(), TargetPath: targets[name], VolumeCapability: mountCap[0],
					VolumeContext: vols[name].GetVolumeContext(), Secrets: secrets})
				return err
			})
			if err == nil {
				mu.Lock()
				published[name] = true
				mu.Unlock()
			}
		})
		pub := []string{}
		for _, nm := range first(*nPublish) {
			if published[nm] {
				pub = append(pub, nm)
			}
		}
		parallel("stats", pub, func(name string) {
			_ = call("stats", "NodeGetVolumeStats", name, func(ctx context.Context) error {
				_, err := node.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
					VolumeId: vols[name].GetVolumeId(), VolumePath: targets[name]})
				return err
			})
		})
		if *hold > 0 {
			emit(record{Phase: "hold-mounted", Event: "start"})
			time.Sleep(*hold)
			emit(record{Phase: "hold-mounted", Event: "end"})
		}
		parallel("unpublish", pub, func(name string) {
			_ = call("unpublish", "NodeUnpublishVolume", name, func(ctx context.Context) error {
				_, err := node.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
					VolumeId: vols[name].GetVolumeId(), TargetPath: targets[name]})
				return err
			})
		})
	}

	parallel("delete", created, func(name string) {
		_ = call("delete", "DeleteVolume", name, func(ctx context.Context) error {
			_, err := ctrl.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: vols[name].GetVolumeId(), Secrets: secrets})
			return err
		})
	})
	emit(record{Phase: "done", Event: "end"})
}
