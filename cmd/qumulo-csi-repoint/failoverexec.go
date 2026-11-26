// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The cluster-backed failoverExecutor, and the one thing in this tool that
// reaches outside Kubernetes: the DNS record.

// clusterExecutor carries the plan out against a real cluster.
type clusterExecutor struct {
	client *kubeClient
	// poll and bound are the wait: how often the VolumeReplication is read,
	// and how long before the command reports what it saw instead of waiting
	// longer. Bounded on purpose -- see awaitReplication.
	poll  time.Duration
	bound time.Duration

	// route53Zone is empty unless the operator passed one, and an empty zone
	// means no DNS call is ever made.
	route53Zone string
	// awsProfile is the `--profile` passed to every `aws` call, from
	// -aws-profile or from AWS_PROFILE. Empty means the CLI's own default.
	//
	// The lab's first apply failed "session has expired" because the
	// profile with the hosted zone was not the default one. The tool ran with
	// ambient credentials and had no way to say which.
	awsProfile string
	// runCommand is the process runner, injected so the Route53 path is
	// testable without an AWS account. stdout and stderr are kept apart: the
	// AWS CLI puts its diagnostics on stderr, and an error that swallows them
	// into one buffer is an error an operator cannot act on.
	runCommand func(ctx context.Context, name string, args ...string) (stdout, stderr []byte, err error)

	out io.Writer
}

func newClusterExecutor(client *kubeClient, opt options, out io.Writer) *clusterExecutor {
	return &clusterExecutor{
		client:      client,
		poll:        opt.pollInterval,
		bound:       opt.waitFor,
		route53Zone: opt.route53Zone,
		awsProfile:  opt.awsProfile,
		runCommand:  runCommand,
		out:         out,
	}
}

// SetReplicationState patches the two spec fields that drive a failover.
//
// A merge patch, not an update: the object may carry fields this tool knows
// nothing about (a newer CRD, a GitOps annotation, a finalizer), and a
// read-modify-write through a struct with six fields in it would drop them.
func (e *clusterExecutor) SetReplicationState(ctx context.Context, change replicationChange) error {
	return e.client.patchVolumeReplicationSpec(ctx, change.Namespace, change.Name, map[string]any{
		"replicationState": change.To,
		"autoResync":       change.AutoResync,
	})
}

// AwaitReplication polls one object until it reaches the wanted role with
// Completed=True, or the bound expires.
func (e *clusterExecutor) AwaitReplication(
	ctx context.Context,
	namespace, name, wantState string,
) (volumeReplication, error) {
	return awaitReplication(ctx, func(ctx context.Context) (volumeReplication, error) {
		return e.client.getVolumeReplication(ctx, namespace, name)
	}, wantState, e.poll, e.bound)
}

// AwaitSettled waits out a transient Resyncing or VolumeDegraded, and reports
// whether the object moved at all while it waited.
func (e *clusterExecutor) AwaitSettled(
	ctx context.Context,
	namespace, name string,
) (volumeReplication, bool, error) {
	return awaitSettled(ctx, func(ctx context.Context) (volumeReplication, error) {
		return e.client.getVolumeReplication(ctx, namespace, name)
	}, e.poll, e.bound)
}

// NudgeReplication forces an immediate reconcile.
//
// Measured on the lab cluster: with the old array down, each Demote fails
// `Unavailable` and the csi-addons controller backs off exponentially, so
// after a long outage the freeze lands minutes after the array answers -- 98
// seconds in the measured case, and the old copies are writable for every one
// of them. **Any spec change on the object forces an immediate reconcile.** So
// `autoResync` is written false and then true again: two spec changes, and the
// field ends where the plan wanted it. The second patch is not optional, which
// is why its error is returned rather than logged -- a run that stopped between
// them would leave reprotection switched off.
//
// When this is called is gated by settleThenNudge: never while the controller
// is reconciling, and never on an object that is still moving.
func (e *clusterExecutor) NudgeReplication(ctx context.Context, namespace, name string) error {
	if err := e.client.patchVolumeReplicationSpec(ctx, namespace, name,
		map[string]any{"autoResync": false}); err != nil {
		return err
	}
	return e.client.patchVolumeReplicationSpec(ctx, namespace, name,
		map[string]any{"autoResync": true})
}

// QuiesceOwner records the owner's replica count on the owner itself and scales
// it to zero.
//
// One patch, so the two can never be separated: an owner at zero replicas with
// nothing recording what it was is the one state step 4 cannot recover from.
// The annotation is written only when there is not one already -- on a re-run
// the count read now is the zero this command wrote last time, and recording it
// would turn "scale back to 3" into "scale back to 0".
func (e *clusterExecutor) QuiesceOwner(ctx context.Context, owner quiesceOwner) error {
	annotations := map[string]any{}
	if !owner.Remembered {
		annotations[annotationQuiescedReplicas] = strconv.Itoa(int(owner.Restore))
	}
	return e.client.patchWorkload(ctx, owner.Kind, owner.Namespace, owner.Name, map[string]any{
		"metadata": map[string]any{"annotations": annotations},
		"spec":     map[string]any{"replicas": 0},
	})
}

// RestoreOwner scales the owner back to what step 0 recorded and clears the
// annotation, so the *next* failover records the count afresh.
func (e *clusterExecutor) RestoreOwner(ctx context.Context, owner quiesceOwner) error {
	return e.client.patchWorkload(ctx, owner.Kind, owner.Namespace, owner.Name, map[string]any{
		// A null value in a merge patch removes the key.
		"metadata": map[string]any{"annotations": map[string]any{annotationQuiescedReplicas: nil}},
		"spec":     map[string]any{"replicas": owner.Restore},
	})
}

// DeletePod deletes one pod. Only ever a bare pod, only ever with
// -allow-bare-pods, and only ever after the plan said so in writing.
func (e *clusterExecutor) DeletePod(ctx context.Context, namespace, name string) error {
	return e.client.deletePod(ctx, namespace, name)
}

// AwaitNoPodsMounting waits until nothing in the namespace mounts any of the
// claims, and returns what was still there when it gave up.
//
// This, not the scale-to-zero, is what makes the quiesce a quiesce: a
// process that ignores SIGTERM keeps writing until the grace-period kill, so an
// owner scaled to zero is a promise, and the pod list is the proof. The promote
// in step 1 runs only when this returns empty.
func (e *clusterExecutor) AwaitNoPodsMounting(
	ctx context.Context,
	namespace string,
	claims []string,
) ([]string, error) {
	deadline := time.Now().Add(e.bound)
	for {
		pods, err := e.client.listPodsIn(ctx, namespace)
		if err != nil {
			return nil, err
		}

		var remaining []string
		for _, pod := range podsMountingClaims(pods, claims) {
			// A pod already terminating still holds its mount, so it counts.
			remaining = append(remaining, pod.Namespace+"/"+pod.Name)
		}
		if len(remaining) == 0 {
			return nil, nil
		}
		if !time.Now().Add(e.poll).Before(deadline) {
			return remaining, nil
		}

		select {
		case <-ctx.Done():
			return remaining, ctx.Err()
		case <-time.After(e.poll):
		}
	}
}

// hostedZoneIDPattern is the shape of a Route53 hosted zone id.
//
// Checked because the failure it prevents is silent: `change-resource-record-
// sets` against a zone id that is not yours is a permission error, but a typo
// that happens to be *someone else's* zone in the same account is an UPSERT
// into it. The id is uppercase alphanumeric, conventionally starting with Z.
var hostedZoneIDPattern = regexp.MustCompile(`^Z[A-Z0-9]{1,31}$`)

// validateHostedZoneID checks the shape and strips the `/hostedzone/` prefix
// the AWS console shows.
func validateHostedZoneID(zone string) error {
	if normalizeHostedZoneID(zone) == "" {
		return fmt.Errorf("-route53-zone-id %q is not a hosted zone id: expected something like "+
			"Z1D633PJN98FT9 (uppercase, starting with Z), optionally carrying the /hostedzone/ "+
			"prefix the AWS console shows", zone)
	}
	return nil
}

// normalizeHostedZoneID returns the bare id, or "" when it is not one.
func normalizeHostedZoneID(zone string) string {
	id := strings.TrimSpace(zone)
	id = strings.TrimPrefix(id, "/hostedzone/")
	id = strings.TrimPrefix(id, "hostedzone/")
	if !hostedZoneIDPattern.MatchString(id) {
		return ""
	}
	return id
}

// recordUnderZone refuses a record that is not inside the zone being changed.
//
// The other half of the DNS preflight. `change-resource-record-sets` with a name outside the
// zone is an error, but a name that is *inside* a zone the operator did not
// mean -- the record typed for one environment against the zone id of
// another -- creates a new A record and points every pod at it after the TTL
// wait. The zone's own name is read from the API before anything is changed,
// and the record has to be at or under it.
func recordUnderZone(record, zoneName string) error {
	name := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(record), "."))
	zone := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(zoneName), "."))
	if zone == "" {
		return fmt.Errorf("the hosted zone reported no name, so %q cannot be checked against it", record)
	}
	if name == zone || strings.HasSuffix(name, "."+zone) {
		return nil
	}
	return fmt.Errorf("refusing to change %s: it is not in hosted zone %s. Either the zone id or the "+
		"record name is wrong, and an UPSERT with the wrong pair points every pod at an arbitrary "+
		"address one TTL later", record, zoneName)
}

// hostedZone is the part of `aws route53 get-hosted-zone` this command reads.
type hostedZone struct {
	HostedZone struct {
		ID   string `json:"Id"`
		Name string `json:"Name"`
	} `json:"HostedZone"`
}

// PreflightDNS proves the zone and the records before anything is changed.
//
// Ordered first in the run for that reason: a zone id typo found in step 3 is
// found with production already quiesced, frozen and promoted, and the only way
// out is forward. Three checks, one API call: the zone exists and is readable
// with these credentials; every record in the plan is at or under the zone's
// own name; and every change batch renders (which is where the IPv4 check on
// -dns-target lives).
func (e *clusterExecutor) PreflightDNS(ctx context.Context, flips []dnsFlip, ttl int) error {
	if len(flips) == 0 {
		return nil
	}
	if err := validateHostedZoneID(e.route53Zone); err != nil {
		return err
	}

	zone := normalizeHostedZoneID(e.route53Zone)
	profile := "the aws CLI's default profile"
	if e.awsProfile != "" {
		profile = "profile " + e.awsProfile
	}
	writef(e.out, "# preflight: reading hosted zone %s with %s\n", zone, profile)

	stdout, err := e.aws(ctx, "route53", "get-hosted-zone", "--id", zone)
	if err != nil {
		return err
	}

	var read hostedZone
	if err := json.Unmarshal(stdout, &read); err != nil {
		return fmt.Errorf("reading the reply from `aws route53 get-hosted-zone --id %s`: %w", zone, err)
	}
	if read.HostedZone.Name == "" {
		return fmt.Errorf("`aws route53 get-hosted-zone --id %s` returned no zone name, so no record "+
			"can be checked against it", zone)
	}
	writef(e.out, "# preflight: hosted zone %s is %s\n", zone, read.HostedZone.Name)

	for _, flip := range flips {
		if err := recordUnderZone(flip.Record, read.HostedZone.Name); err != nil {
			return err
		}
		batch, err := route53ChangeBatch(flip, ttl)
		if err != nil {
			return err
		}
		writef(e.out, "# preflight: %s is in %s; the change batch that would be applied is:\n%s\n",
			flip.Record, read.HostedZone.Name, batch)
	}
	return nil
}

// FlipRecord applies the DNS change, and only ever for Route53.
//
// The tool does not assume Route53 -- with no `-route53-zone-id` it prints the
// record change and touches nothing, which is the default and is what a
// customer running BIND, Infoblox or a cloud that is not AWS gets. When a zone
// id *is* given, the change goes through the `aws` CLI rather than an SDK: the
// alternative is a new module dependency in the driver's go.mod for a
// Preview operator command, and the CLI is already on the machine of anyone
// who has a hosted zone id to hand.
//
// The exact change batch is printed before it is applied, every time, so what
// was changed is in the run's own output and not only in CloudTrail.
func (e *clusterExecutor) FlipRecord(ctx context.Context, flip dnsFlip, ttl int) error {
	batch, err := route53ChangeBatch(flip, ttl)
	if err != nil {
		return err
	}
	writef(e.out, "# applying this Route53 change batch to zone %s:\n%s\n",
		normalizeHostedZoneID(e.route53Zone), batch)

	dir, err := os.MkdirTemp("", "qumulo-csi-repoint")
	if err != nil {
		return fmt.Errorf("staging the Route53 change batch: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	path := filepath.Join(dir, "change-batch.json")
	if err := os.WriteFile(path, []byte(batch), 0o600); err != nil {
		return fmt.Errorf("staging the Route53 change batch: %w", err)
	}

	_, err = e.aws(ctx, "route53", "change-resource-record-sets",
		"--hosted-zone-id", normalizeHostedZoneID(e.route53Zone), "--change-batch", "file://"+path)
	return err
}

// aws runs one `aws` command and fails fast with the CLI's own stderr.
//
// `--profile` is passed when the operator gave one, because the lab's first
// apply failed "Your session has expired" with no clue that the credentials in
// use were the default profile's and not the one holding the zone.
// AWS_PROFILE is honoured too and resolved in main, so the value that was used
// is the value that gets printed.
func (e *clusterExecutor) aws(ctx context.Context, args ...string) ([]byte, error) {
	full := args
	if e.awsProfile != "" {
		full = append([]string{"--profile", e.awsProfile}, args...)
	}

	stdout, stderr, err := e.runCommand(ctx, "aws", full...)
	if err != nil {
		message := strings.TrimSpace(string(stderr))
		if message == "" {
			message = strings.TrimSpace(string(stdout))
		}
		return nil, fmt.Errorf("`aws %s` failed: %w: %s", strings.Join(full, " "), err, message)
	}
	return stdout, nil
}

// Settle waits one TTL, so the record change has propagated before the pods
// come back. Bounded by the command's own context like everything else.
func (e *clusterExecutor) Settle(ctx context.Context, ttl int) error {
	if ttl <= 0 {
		return nil
	}
	writef(e.out, "# waiting one TTL (%ds) before the workloads come back\n", ttl)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(ttl) * time.Second):
		return nil
	}
}

// route53ChangeBatch renders the record change as Route53's own change batch.
//
// Rendered even when nothing will be applied: it is the exact change, in a
// form that can be reviewed, pasted into a ticket, or fed to `aws route53
// change-resource-record-sets` by hand on a machine this command never runs
// on.
func route53ChangeBatch(flip dnsFlip, ttl int) (string, error) {
	if flip.To == "" {
		return "", fmt.Errorf("no target address for %s: pass -dns-target with the address of the "+
			"array now holding the writable copy", flip.Record)
	}
	// An A record holds an IPv4 address and nothing else. A name would silently
	// become a record nothing resolves; an IPv6 address would be an AAAA record,
	// which this command does not write.
	address := net.ParseIP(flip.To)
	if address == nil {
		return "", fmt.Errorf("refusing to build a Route53 A record for %s: %q is a name, not an "+
			"address. Pass -dns-target with the target array's address", flip.Record, flip.To)
	}
	if address.To4() == nil {
		return "", fmt.Errorf("refusing to build a Route53 A record for %s: %q is an IPv6 address, "+
			"and an A record holds IPv4. An AAAA record is not something this command writes",
			flip.Record, flip.To)
	}
	if ttl <= 0 {
		return "", fmt.Errorf("a TTL of %d is not usable for %s: pass -dns-ttl", ttl, flip.Record)
	}

	batch := map[string]any{
		"Comment": fmt.Sprintf("qumulo-csi-repoint: %s -> %s", flip.Record, flip.To),
		"Changes": []any{
			map[string]any{
				"Action": "UPSERT",
				"ResourceRecordSet": map[string]any{
					"Name":            flip.Record,
					"Type":            "A",
					"TTL":             ttl,
					"ResourceRecords": []any{map[string]any{"Value": flip.To}},
				},
			},
		},
	}

	encoded, err := json.MarshalIndent(batch, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encoding the Route53 change batch: %w", err)
	}
	return string(encoded), nil
}

// runCommand is the default process runner, with stdout and stderr kept apart
// so a failure can be reported in the CLI's own words.
func runCommand(ctx context.Context, name string, args ...string) ([]byte, []byte, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // a fixed argv
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

// awaitSettled waits out a transient Resyncing or VolumeDegraded, and
// reports whether the object changed at all while it waited.
//
// "Changed at all" is the no-progress gate on the nudge: a controller that is
// writing status back is working, and the spec toggle is the remedy for one
// that has stopped. The fingerprint is status, conditions, reasons and message
// -- everything a reconcile would touch.
func awaitSettled(
	ctx context.Context,
	get func(context.Context) (volumeReplication, error),
	poll, bound time.Duration,
) (volumeReplication, bool, error) {
	deadline := time.Now().Add(bound)

	observed, err := get(ctx)
	if err != nil {
		return observed, false, err
	}
	baseline := observed.fingerprint()
	progressed := false

	for {
		if observed.fingerprint() != baseline {
			progressed = true
		}
		if !observed.reconciling() {
			return observed, progressed, nil
		}
		if !time.Now().Add(poll).Before(deadline) {
			return observed, progressed, nil
		}

		select {
		case <-ctx.Done():
			return observed, progressed, ctx.Err()
		case <-time.After(poll):
		}

		observed, err = get(ctx)
		if err != nil {
			return observed, progressed, err
		}
	}
}
