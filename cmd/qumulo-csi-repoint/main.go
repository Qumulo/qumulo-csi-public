// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Command qumulo-csi-repoint builds and switches to the DR-side objects for
// volumes protected by Qumulo replication.
//
// PREVIEW: not shipped in a release image; run from a source checkout.
// Written against the measurements in the DR validation lab and exercised
// against manifests and unit tests, then run against the lab's EKS cluster
// and array pair in both directions. See
// docs/reference/supported-features.md, section 5.
//
//	qumulo-csi-repoint mirror -peer-server dr.example.com \
//	    -peer-api-url https://dr.example.com:8000 \
//	    -peer-token-file /run/secrets/dr-token
//
//	qumulo-csi-repoint switch -peer-server dr.example.com \
//	    -peer-api-url https://dr.example.com:8000 \
//	    -peer-token-file /run/secrets/dr-token
//
//	qumulo-csi-repoint failover -namespace apps -dry-run=false
//	qumulo-csi-repoint failback -namespace apps -dns-target 192.0.2.30 -dry-run=false
//
// Four subcommands. The first two are from spec section 6 and build or plan
// objects; the last two drive the csi-addons VolumeReplication objects through
// a failover and back:
//
//   - **mirror** writes the DR-side PV and PVC for each protected volume: the
//     same volume attributes except `server`, the handle rewritten with the
//     *replica's own* file ID read from the peer array, `Retain`, and a
//     pre-set `claimRef`. Run it before the incident. It is the slow path
//     otherwise, and the runbook (3.9) says so.
//   - **switch** moves the workloads onto the promoted copy. With a DNS-named
//     `server` that is a record flip and a pod restart and no object change at
//     all; with an IP literal it is a PVC rebind per volume, because
//     `spec.csi` is immutable. The rebind deletes the source claim, so with
//     -dry-run=false it first reads the pods using each claim from the cluster
//     and refuses if one does, or if they could not be read; with -f it never
//     deletes, and prints the kubectl commands to run by hand instead.
//   - **failover** and **failback** are the measured operator actions of a
//     failover, in order, for every mirrored pair in **one namespace of one
//     cluster**: quiesce every pod mounting an affected claim and wait until
//     they are gone (step 0 -- a promote freezes the source, and a frozen source
//     drops acknowledged appends silently), promote one side's
//     `VolumeReplication` and wait for it, demote the other with `autoResync`
//     and wait for it, flip the DNS record and wait one TTL, then scale the
//     workloads back to the replicas step 0 recorded. They dial no array and
//     hold no credential -- everything they join is already a Kubernetes
//     object. See failover.go and quiesce.go.
//
// Why this is a separate binary and not part of the driver: ARCHITECTURE.md rule 4.
// The driver reads no Kubernetes objects; an operator CLI, run on demand under
// a human's own kubeconfig, may. See cluster.go.
//
// **--dry-run defaults to true.** Nothing is written to any cluster unless it
// is passed `-dry-run=false`. The default run prints manifests and a plan,
// which is what a DR change is reviewed as.
//
// **Tokens come from a file, never a flag value, and are never printed.**
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"qumulo-csi-driver/pkg/version"
)

// options is the resolved configuration for one run.
type options struct {
	// Input. Files (or "-" for stdin) beat the cluster: a run against
	// manifests needs no credentials and no reachable API server, which is
	// what makes this usable from a laptop during an incident.
	files      []string
	kubeconfig string
	driverName string

	// The peer array.
	peerServer   string
	peerAPIURL   string
	peerToken    string
	peerBasePath string
	insecureTLS  bool

	// How the mirrored objects are named.
	pvSuffix        string
	claimSuffix     string
	storageClass    string
	secretName      string
	secretNamespace string

	dryRun  bool
	timeout time.Duration

	// failover/failback only.
	namespace    string
	waitFor      time.Duration
	pollInterval time.Duration
	route53Zone  string
	awsProfile   string
	dnsRecord    string
	dnsTarget    string
	dnsTTL       int
	restartPods  bool
	// allowBarePods lets step 0 delete a pod with no controller. Off by
	// default: deleting one destroys the only copy of its definition.
	allowBarePods bool
}

// isFailoverSubcommand reports whether a subcommand drives the
// VolumeReplication objects rather than building or planning PV/PVC pairs.
// Those two take a different flag set, and above all need no peer credentials:
// everything they act on is already a Kubernetes object.
func isFailoverSubcommand(subcommand string) bool {
	return subcommand == "failover" || subcommand == "failback"
}

func main() {
	// A one-shot command that holds every object it read until it exits:
	// collecting at 25% growth rather than Go's default 100% costs about a
	// fifth more CPU on a 10,000-pair failover input and takes its peak
	// memory from about 5 to under 4 times the input. GOGC still
	// wins when it is set.
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(25)
	}
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		writeln(os.Stderr, "qumulo-csi-repoint:", err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		usage(stderr)
		return fmt.Errorf("a subcommand is required")
	}

	subcommand := args[0]
	switch subcommand {
	case "-h", "--help", "help":
		usage(stderr)
		return nil
	case "-version", "--version", "version":
		writef(stdout, "qumulo-csi-repoint %s (%s)\n", version.DriverVersion, version.GitCommit)
		return nil
	case "mirror", "switch", "failover", "failback":
	default:
		usage(stderr)
		return fmt.Errorf("unknown subcommand %q", subcommand)
	}

	opt, err := parseFlags(subcommand, args[1:], stderr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opt.timeout)
	defer cancel()

	switch subcommand {
	case "mirror":
		return runMirror(ctx, opt, stdin, stdout, stderr)
	case "switch":
		return runSwitch(ctx, opt, stdin, stdout, stderr)
	case "failover":
		return runFailoverCommand(ctx, opt, directionFailover, stdin, stdout, stderr)
	default:
		return runFailoverCommand(ctx, opt, directionFailback, stdin, stdout, stderr)
	}
}

// writef, writeln and writeText write the commentary this command prints
// alongside its manifests.
//
// The write error is dropped on purpose, and only here: a failed write to
// stdout or stderr has no recovery inside a CLI whose entire output is that
// write, and threading it back would put an `if err != nil` around every line
// of an explanation. Everything that changes a cluster still checks its
// errors.
func writef(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func writeln(w io.Writer, args ...any) {
	_, _ = fmt.Fprintln(w, args...)
}

func writeText(w io.Writer, args ...any) {
	_, _ = fmt.Fprint(w, args...)
}

// stringList collects a repeatable flag.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func parseFlags(subcommand string, args []string, stderr io.Writer) (options, error) {
	var opt options
	var files stringList

	fs := flag.NewFlagSet("qumulo-csi-repoint "+subcommand, flag.ContinueOnError)
	fs.SetOutput(stderr)

	fs.Var(&files, "f", "read PersistentVolumes, PersistentVolumeClaims and (for failover/failback) "+
		"VolumeReplications from this YAML/JSON file instead of the cluster; \"-\" means stdin. "+
		"Repeatable. With no -f, the cluster named by the current kubeconfig context is read")
	fs.StringVar(&opt.kubeconfig, "kubeconfig", "",
		"kubeconfig to read the cluster with; empty uses KUBECONFIG, then ~/.kube/config, then in-cluster credentials")
	fs.StringVar(&opt.driverName, "drivername", version.DriverName,
		"only act on PersistentVolumes provisioned by this driver")

	// The peer array and the naming of the mirrored objects belong to `mirror`
	// and `switch`. `failover` and `failback` need none of it: they act on
	// objects `mirror` already wrote, so they dial no array and hold no
	// credential -- which is also why they are the two subcommands an operator
	// can run under an incident's own RBAC.
	if !isFailoverSubcommand(subcommand) {
		fs.StringVar(&opt.peerServer, "peer-server", "",
			"the DR array's NFS host, as it will appear in the mirrored PV's handle and volumeAttributes.server. Required")
		fs.StringVar(&opt.peerAPIURL, "peer-api-url", "",
			"the DR array's REST endpoint, e.g. https://dr.example.com:8000. Required for mirror: the "+
				"replica directory's presence on that array is checked before a PV is written pointing at it")
		fs.StringVar(&opt.peerToken, "peer-token-file", "",
			"path to a file holding the DR array's API token. A path only -- never the token itself, which "+
				"on a command line would be in the process table and the shell history. Never printed")
		fs.StringVar(&opt.peerBasePath, "peer-base-path", "",
			"the parent directory the replicas live under on the peer, if it differs from the source. "+
				"Empty means the same path on both arrays, which is what replication gives you by default")
		fs.BoolVar(&opt.insecureTLS, "insecure-skip-tls-verify", false,
			"skip verification of the peer array's TLS certificate; for development against self-signed certs only")

		fs.StringVar(&opt.pvSuffix, "pv-suffix", "-dr",
			"appended to each source PV's name to name its mirror. PV names are cluster-scoped, so this "+
				"must be non-empty when both PVs live in one cluster")
		fs.StringVar(&opt.claimSuffix, "claim-suffix", "",
			"appended to each PVC's name in the mirrored objects. Empty is right on a second Kubernetes "+
				"cluster, where the same namespace/name is the point; on one cluster the original claim "+
				"still exists, so a suffix is required")
		fs.StringVar(&opt.storageClass, "storage-class", "",
			"storageClassName for the mirrored objects; empty keeps the source's")
		fs.StringVar(&opt.secretName, "secret-name", "",
			"replaces the name in every CSI secret reference on the mirrored PV. The references follow the "+
				"handle, and the handle now names the peer, so on a second cluster this usually has to change")
		fs.StringVar(&opt.secretNamespace, "secret-namespace", "",
			"replaces the namespace in every CSI secret reference on the mirrored PV")
	}

	fs.BoolVar(&opt.dryRun, "dry-run", true,
		"print what would be done and change nothing. Pass -dry-run=false to write to the cluster")

	// A failover waits on the csi-addons controller, which after an outage is
	// somewhere in an exponential backoff minutes wide. Five minutes is
	// the right overall bound for a rewrite and the wrong one for that.
	timeoutDefault := 5 * time.Minute
	if isFailoverSubcommand(subcommand) {
		timeoutDefault = 30 * time.Minute
		fs.StringVar(&opt.namespace, "namespace", "",
			"the namespace whose mirrored pairs are failed over. Required: a VolumeReplication "+
				"names its claim by name alone, so pairs are derived within one namespace")
		fs.DurationVar(&opt.waitFor, "wait", 5*time.Minute,
			"how long to wait for each VolumeReplication to reach the expected state with "+
				"Completed=True before reporting what it saw instead")
		fs.DurationVar(&opt.pollInterval, "poll", 5*time.Second,
			"how often each VolumeReplication is re-read while waiting")
		fs.StringVar(&opt.route53Zone, "route53-zone-id", "",
			"apply the DNS record change as a Route53 UPSERT in this hosted zone, through the "+
				"`aws` CLI. Empty -- the default -- prints the record change and touches no DNS, "+
				"which is what a zone that is not Route53 gets. The id's shape is checked, the "+
				"zone is read before anything is changed, and the record must be under it")
		fs.StringVar(&opt.awsProfile, "aws-profile", "",
			"the AWS CLI profile every `aws` call is made with (--profile). Empty falls back to "+
				"AWS_PROFILE, then to the CLI's own default. The lab's first apply failed \"session "+
				"has expired\" because the profile holding the zone was not the default")
		fs.StringVar(&opt.dnsRecord, "dns-record", "",
			"the record to flip, if it is not the `server` value in the original PVs")
		fs.StringVar(&opt.dnsTarget, "dns-target", "",
			"the address to point the record at, or record=address pairs (comma-separated) "+
				"when the namespace has more than one record. Derived from the promoted side's PV "+
				"when it differs from the record itself, which on a failback it does not. One bare "+
				"address for several records is refused: they may belong to different arrays")
		fs.IntVar(&opt.dnsTTL, "dns-ttl", 60,
			"TTL for the flipped record, and how long the command waits after flipping it")
		fs.BoolVar(&opt.restartPods, "restart-pods", true,
			"scale the owners of every pod mounting an affected claim to zero before the promote "+
				"(step 0) and back to their remembered replicas after the DNS wait (step 4). With "+
				"=false nothing is scaled and the command refuses to promote while any pod mounts "+
				"an affected claim -- a promote freezes the source and a frozen source drops "+
				"acknowledged appends silently")
		fs.BoolVar(&opt.allowBarePods, "allow-bare-pods", false,
			"delete pods that have no controller during the quiesce. Off by default: a bare pod "+
				"stops the command instead, because deleting one destroys the only copy of its "+
				"definition and nothing will bring it back")
	}
	fs.DurationVar(&opt.timeout, "timeout", timeoutDefault, "overall timeout")

	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	opt.files = files

	if isFailoverSubcommand(subcommand) {
		if opt.namespace == "" {
			return options{}, fmt.Errorf("-namespace is required")
		}
		// AWS_PROFILE is honoured, and resolved here rather than left to the
		// CLI, so the profile that was used is the profile that gets printed.
		if opt.awsProfile == "" {
			opt.awsProfile = os.Getenv("AWS_PROFILE")
		}
		if opt.route53Zone != "" {
			if err := validateHostedZoneID(opt.route53Zone); err != nil {
				return options{}, err
			}
		}
		return opt, nil
	}

	if opt.peerServer == "" {
		return options{}, fmt.Errorf("-peer-server is required")
	}
	if opt.pvSuffix == "" && opt.claimSuffix == "" {
		// Not fatal in every deployment -- on a second cluster the mirrored PV
		// can legitimately carry the same name -- but silently overwriting a
		// PV name in one cluster is not a mistake worth allowing by default.
		writeln(stderr, "# note: -pv-suffix and -claim-suffix are both empty. That is right only "+
			"on a *second* Kubernetes cluster; in one cluster the mirrored objects would collide with "+
			"the originals.")
	}

	return opt, nil
}

func usage(w io.Writer) {
	writeText(w, `qumulo-csi-repoint -- DR-side objects for Qumulo-replicated volumes (Preview)

  qumulo-csi-repoint mirror   [flags]   build the DR PV/PVC pair for each protected volume
  qumulo-csi-repoint switch   [flags]   move workloads onto the promoted copy
  qumulo-csi-repoint failover [flags]   promote the DR side of every mirrored pair in a namespace
  qumulo-csi-repoint failback [flags]   the same four steps in the other direction

switch, for an IP-named server, deletes each source PVC and applies its
replacement bound to the mirrored PV. With -dry-run=false it first reads the
pods using those claims from the cluster, and deletes nothing if one still
mounts a claim or if the pods could not be read (a PVC with a pod on it stays
Terminating). With -f it reads no pods and so deletes nothing at all: it prints
the kubectl commands to run by hand once the workloads are quiesced, and exits
non-zero. A claim still present after its delete is reported NOT DONE, its
replacement is not applied, and the run exits non-zero.

failover/failback do, in order:
  0. quiesce  -- scale the owner of every pod mounting an affected claim to 0
                 and wait until the pods are gone. A promote freezes the source,
                 and a frozen source drops acknowledged appends silently,
                 so nothing is promoted until this is done.
                 A bare pod stops the command unless -allow-bare-pods is given.
  1. promote  -- the DR side's VolumeReplication, and wait for Primary/Completed.
  2. demote   -- the old side with autoResync, and wait. A spec toggle forces an
                 immediate reconcile if the controller is backing off, but
                 never while the object is still Resyncing or VolumeDegraded and
                 never while it is making progress.
  3. DNS      -- the A record, then one TTL. With -route53-zone-id the zone is
                 read and the record checked against it before anything changes.
  4. restore  -- scale every owner back to the replicas step 0 recorded.

Every run ends with a summary saying, per object, the transition actually made
("secondary -> primary", or "already primary, no change"), what each step cost,
and whether the run was COLD (every promote changed state) or a RE-RUN.

SINGLE-CLUSTER ONLY. failover/failback pair both sides within one namespace of
one cluster (topology B): the join needs both PVs, both PVCs and both
VolumeReplications at once, and a VolumeReplication names its claim by name
alone. On two clusters -- the topology the spec recommends -- this command finds
no pairs at all. Two clusters would need two kubeconfigs and a flag saying which
is the promote side, the ordering held across two API servers rather than one,
pairing by name across clusters instead of by the mirrored-from annotation, and
a step 0/step 4 that quiesces on one cluster and starts workloads on the other.
None of that is implemented.

All four default to --dry-run=true and print a plan; pass -dry-run=false to act.
Run "qumulo-csi-repoint failover -h" for the flags.
`)
}

// sourceVolumes reads the PVs to act on, from files or from the cluster.
func sourceVolumes(ctx context.Context, opt options, stdin io.Reader) ([]*corev1.PersistentVolume, error) {
	if len(opt.files) > 0 {
		stream, err := readObjectFiles(opt.files, stdin)
		if err != nil {
			return nil, err
		}
		return stream.Volumes, nil
	}

	client, err := kubeClientFor(opt.kubeconfig)
	if err != nil {
		return nil, err
	}
	return client.listVolumes(ctx, opt.driverName)
}

// mirrorOptionsFrom projects the CLI options onto the rewrite's own options,
// which know nothing about flags.
func mirrorOptionsFrom(opt options) mirrorOptions {
	return mirrorOptions{
		driverName:      opt.driverName,
		peerServer:      opt.peerServer,
		peerAPIURL:      opt.peerAPIURL,
		peerBasePath:    opt.peerBasePath,
		pvSuffix:        opt.pvSuffix,
		claimSuffix:     opt.claimSuffix,
		storageClass:    opt.storageClass,
		secretName:      opt.secretName,
		secretNamespace: opt.secretNamespace,
	}
}

// runMirror builds and prints (or applies) the DR-side pairs.
func runMirror(ctx context.Context, opt options, stdin io.Reader, stdout, stderr io.Writer) error {
	sources, err := sourceVolumes(ctx, opt, stdin)
	if err != nil {
		return err
	}

	resolver, err := newQumuloResolver(opt.peerAPIURL, opt.peerToken, opt.insecureTLS)
	if err != nil {
		return err
	}

	pairs, err := mirrorAll(ctx, sources, resolver, mirrorOptionsFrom(opt))
	if err != nil {
		return err
	}

	writeText(stdout, mirrorSummary(pairs))
	for _, p := range pairs {
		if err := writeManifest(stdout, p.Volume, p.Claim); err != nil {
			return err
		}
	}

	if opt.dryRun {
		writeln(stderr, "# dry run: nothing was written. Review the manifests above, then either "+
			"`kubectl apply -f` them on the DR cluster or re-run with -dry-run=false.")
		return nil
	}

	client, err := kubeClientFor(opt.kubeconfig)
	if err != nil {
		return err
	}
	for _, p := range pairs {
		pvResult, err := client.applyVolume(ctx, p.Volume)
		if err != nil {
			return err
		}
		pvcResult, err := client.applyClaim(ctx, p.Claim)
		if err != nil {
			return err
		}
		writef(stderr, "# %s: PV %s, PVC %s/%s %s\n",
			pvResult, p.Volume.Name, p.Claim.Namespace, p.Claim.Name, pvcResult)
	}
	return nil
}

// runSwitch prints the failover plan, and carries out the PVC rebind when
// asked to.
func runSwitch(ctx context.Context, opt options, stdin io.Reader, stdout, stderr io.Writer) error {
	sources, err := sourceVolumes(ctx, opt, stdin)
	if err != nil {
		return err
	}

	// `switch` re-derives the mirrored objects rather than reading them back,
	// for one reason: the plan has to name the PV a PVC will be re-bound to,
	// and deriving it from the same code that wrote it is the only way the two
	// cannot disagree. The peer lookup is repeated for the same reason -- the
	// file ID is read from the array that holds it, now, not trusted from an
	// object written before the incident.
	resolver, err := newQumuloResolver(opt.peerAPIURL, opt.peerToken, opt.insecureTLS)
	if err != nil {
		return err
	}
	pairs, err := mirrorAll(ctx, sources, resolver, mirrorOptionsFrom(opt))
	if err != nil {
		return err
	}

	plan, err := planSwitch(pairs, opt.peerServer)
	if err != nil {
		return err
	}

	// The pods using the claims, read from the cluster when the objects came
	// from it. For the plan this is best effort -- a plan without them is still
	// a plan, and an incident is exactly when the API server might not answer
	// -- so a failure here is remembered, not returned. The apply below is
	// where it stops being optional.
	var (
		podsInUse []string
		podCheck  error
	)
	if len(opt.files) == 0 {
		podsInUse, podCheck = podsUsingClaimsLive(ctx, opt, plan.Claims)
		if len(podsInUse) > 0 {
			plan.Claims = podsInUse
		}
	}

	writeText(stdout, plan.String())

	if len(plan.Switches) == 0 {
		return nil
	}

	// The IP case needs objects, so it emits them: the replacement PVCs,
	// already bound to the mirrored PVs.
	for _, p := range pairs {
		if isIPLiteral(p.Source.Spec.CSI.VolumeAttributes[volumeAttributeServer]) {
			if err := writeManifest(stdout, p.Claim); err != nil {
				return err
			}
		}
	}

	if opt.dryRun {
		switch {
		case len(opt.files) > 0:
			writeln(stderr, "# NOTE: with -f, -dry-run=false deletes nothing: the pods using these "+
				"claims are not read from a file, so it prints the kubectl commands to run by hand "+
				"once the workloads are quiesced.")
		case podCheck != nil:
			writef(stderr, "# NOTE: the pods using these claims could not be read (%v). "+
				"-dry-run=false would refuse until they can.\n", podCheck)
		}
		writeln(stderr, "# dry run: nothing was changed. Quiesce the workloads first, then re-run "+
			"with -dry-run=false, or carry out the steps above by hand.")
		return nil
	}

	for _, w := range plan.Warnings {
		return fmt.Errorf("refusing to switch: %s", w)
	}

	// Deleting a claim is safe only once nothing mounts it: a pod still on it
	// means kubernetes.io/pvc-protection leaves the claim Terminating, and the
	// replacement would be applied over a half-deleted one -- a switch that
	// looks complete and is not. Quiesce first, as the runbook's phase 3.1 says
	// and as `failover`'s own step 0 does. And "nothing mounts it" has to be
	// something this run *read*: a pod list from a file is whatever was running
	// when somebody saved it, and a list that failed proves nothing. `failover`
	// refuses on the same grounds (executeQuiesce).
	if len(opt.files) > 0 {
		writeHandSwitch(stderr, plan, pairs)
		return fmt.Errorf("refusing to delete claims with -f: the pods using them were not read "+
			"from the cluster, so whether one still mounts a claim is unknown, and nothing was "+
			"deleted. Quiesce the workloads and run the %d command(s) printed above by hand, or "+
			"re-run without -f", len(plan.Switches))
	}
	if podCheck != nil {
		return fmt.Errorf("refusing to switch: could not read the pods using the claims this would "+
			"delete, so whether one still mounts a claim is unknown, and nothing was deleted: %w",
			podCheck)
	}
	if len(podsInUse) > 0 {
		return fmt.Errorf("refusing to switch: pod(s) %s still mount a claim this would delete. "+
			"A PVC with a pod on it stays Terminating (kubernetes.io/pvc-protection) and the "+
			"replacement claim would be applied over it. Scale their owners to zero first",
			strings.Join(podsInUse, ", "))
	}

	client, err := kubeClientFor(opt.kubeconfig)
	if err != nil {
		return err
	}
	return applySwitch(ctx, client, plan, pairs, opt, stderr)
}

// podsUsingClaimsLive reads the pods in the claims' namespaces and returns the
// ones that mount a claim, as podsUsingClaims names them.
func podsUsingClaimsLive(ctx context.Context, opt options, claims []string) ([]string, error) {
	client, err := kubeClientFor(opt.kubeconfig)
	if err != nil {
		return nil, err
	}
	pods, err := client.listPodsForClaims(ctx, claims)
	if err != nil {
		return nil, err
	}
	return podsUsingClaims(pods, claims), nil
}

// The wait for a deleted claim to go, when the options carry none: `switch`
// has no -wait flag, and pvc-protection releases an unused claim in seconds.
const (
	switchClaimPoll  = time.Second
	switchClaimBound = 30 * time.Second
)

// applySwitch deletes each source claim, confirms it is gone, and applies its
// replacement. A claim that does not go is reported as not done and its
// replacement is not applied; the others carry on, and the run fails naming
// every one that did not switch.
func applySwitch(
	ctx context.Context,
	client *kubeClient,
	plan switchPlan,
	pairs []mirrorPair,
	opt options,
	stderr io.Writer,
) error {
	poll, bound := opt.pollInterval, opt.waitFor
	if poll <= 0 {
		poll = switchClaimPoll
	}
	if bound <= 0 {
		bound = switchClaimBound
	}

	var notDone []string
	gone := map[string]bool{}
	for _, sw := range plan.Switches {
		// Delete then create, in that order and never the reverse: two PVCs
		// cannot hold one PV, and the mirrored PV's claimRef already names the
		// replacement.
		if err := client.deleteClaim(ctx, sw.Namespace, sw.SourcePVC); err != nil {
			return err
		}
		if err := client.awaitClaimGone(ctx, sw.Namespace, sw.SourcePVC, poll, bound); err != nil {
			notDone = append(notDone, err.Error())
			writef(stderr, "# NOT DONE: %v; its replacement was not applied\n", err)
			continue
		}
		gone[sw.Namespace+"/"+sw.SourcePVC] = true
		writef(stderr, "# deleted PVC %s/%s (its PV is Retain; the data is untouched)\n",
			sw.Namespace, sw.SourcePVC)
	}
	for _, p := range pairs {
		if !isIPLiteral(p.Source.Spec.CSI.VolumeAttributes[volumeAttributeServer]) {
			continue
		}
		if ref := p.Source.Spec.ClaimRef; ref == nil || !gone[ref.Namespace+"/"+ref.Name] {
			continue
		}
		if _, err := client.applyVolume(ctx, p.Volume); err != nil {
			return err
		}
		result, err := client.applyClaim(ctx, p.Claim)
		if err != nil {
			notDone = append(notDone, err.Error())
			writef(stderr, "# NOT DONE: %v\n", err)
			continue
		}
		writef(stderr, "# %s PVC %s/%s bound to %s\n", result, p.Claim.Namespace, p.Claim.Name, p.Volume.Name)
	}

	if len(notDone) > 0 {
		return fmt.Errorf("switch not done for %d of %d claim(s): %s", len(notDone), len(plan.Switches),
			strings.Join(notDone, "; "))
	}
	writeln(stderr, "# now recreate the pods listed above: a live NFS mount never follows a "+
		"failover, whatever changed underneath it.")
	return nil
}

// writeHandSwitch prints the rebind as kubectl commands, for an operator who
// planned from files and has to carry it out by hand after quiescing.
func writeHandSwitch(w io.Writer, plan switchPlan, pairs []mirrorPair) {
	writeln(w, "# -f: the claims are not deleted by this command. Once every workload using "+
		"them is scaled to zero, run, for each claim:")
	for _, sw := range plan.Switches {
		writef(w, "#   kubectl --namespace %s describe pvc %s    # \"Used By:\" must say <none>\n",
			sw.Namespace, sw.SourcePVC)
		writef(w, "#   kubectl --namespace %s delete pvc %s --wait=true --timeout=60s\n", sw.Namespace, sw.SourcePVC)
		writef(w, "#   kubectl --namespace %s get pvc %s    # must answer NotFound; a claim still "+
			"Terminating has a pod on it\n", sw.Namespace, sw.SourcePVC)
		writef(w, "#   kubectl get pv %s    # the mirrored PV `mirror` wrote; apply mirror's output "+
			"if it is missing\n", sw.MirroredPV)
	}
	writef(w, "#   kubectl apply -f <the PVC manifest(s) this command printed on stdout>    "+
		"# %d claim(s), each bound to its mirrored PV\n", countIP(pairs))
}

// countIP is how many pairs are IP-named, which is how many replacement claims
// this command prints.
func countIP(pairs []mirrorPair) int {
	n := 0
	for _, p := range pairs {
		if isIPLiteral(p.Source.Spec.CSI.VolumeAttributes[volumeAttributeServer]) {
			n++
		}
	}
	return n
}

// failoverOptionsFrom projects the CLI options onto the plan's own options.
func failoverOptionsFrom(opt options, input objectStream) failoverOptions {
	return failoverOptions{
		namespace:     opt.namespace,
		dnsRecord:     opt.dnsRecord,
		dnsTarget:     opt.dnsTarget,
		dnsTTL:        opt.dnsTTL,
		route53Zone:   opt.route53Zone,
		restartPods:   opt.restartPods,
		pods:          input.Pods,
		workloads:     input.Workloads,
		podsObserved:  input.PodsObserved,
		allowBarePods: opt.allowBarePods,
	}
}

// failoverObjects reads the three kinds the pairing needs, from files or from
// the cluster.
//
// Files are the incident path, as everywhere else in this command: a plan can
// be built and reviewed from a saved `kubectl get pv,pvc,volumereplication -o
// yaml` with no cluster reachable at all. Applying one still needs the
// cluster, and says so.
func failoverObjects(ctx context.Context, opt options, stdin io.Reader) (objectStream, error) {
	if len(opt.files) > 0 {
		return readObjectFiles(opt.files, stdin)
	}

	client, err := kubeClientFor(opt.kubeconfig)
	if err != nil {
		return objectStream{}, err
	}

	volumes, err := client.listVolumes(ctx, opt.driverName)
	if err != nil {
		return objectStream{}, err
	}
	claims, err := client.listClaimsIn(ctx, opt.namespace)
	if err != nil {
		return objectStream{}, err
	}
	replications, err := client.listVolumeReplications(ctx, opt.namespace)
	if err != nil {
		return objectStream{}, err
	}
	// The classes are cluster-scoped, and are read for one parameter:
	// `replicationScope: group`, which says a member volume's missing
	// VolumeReplication is the design rather than a gap.
	classes, err := client.listVolumeReplicationClasses(ctx)
	if err != nil {
		return objectStream{}, err
	}
	// The pods and their owners: step 0's input. Read here rather than at apply
	// time so that what the dry run prints and what the apply does are the same
	// plan built by the same code.
	pods, err := client.listPodsIn(ctx, opt.namespace)
	if err != nil {
		return objectStream{}, err
	}
	workloads, err := client.listWorkloadsIn(ctx, opt.namespace)
	if err != nil {
		return objectStream{}, err
	}
	return objectStream{
		Volumes:            volumes,
		Claims:             claims,
		Replications:       replications,
		ReplicationClasses: classes,
		Pods:               pods,
		Workloads:          workloads,
		PodsObserved:       true,
	}, nil
}

// withLivePods re-reads the pods and their owners from the cluster.
//
// Called on every apply, whatever the plan was built from. A quiesce planned
// from a file is a quiesce planned from whatever was running when somebody ran
// `kubectl get`; the promote in step 1 freezes the source, so step 0 gets a
// fresh list or the run refuses (see executeQuiesce).
func withLivePods(ctx context.Context, client *kubeClient, namespace string, in objectStream) (objectStream, error) {
	pods, err := client.listPodsIn(ctx, namespace)
	if err != nil {
		return in, err
	}
	workloads, err := client.listWorkloadsIn(ctx, namespace)
	if err != nil {
		return in, err
	}
	in.Pods, in.Workloads, in.PodsObserved = pods, workloads, true
	return in, nil
}

// runFailoverCommand is `failover` and `failback`: one code path, one flag set,
// and the direction chosen by which name was typed.
func runFailoverCommand(
	ctx context.Context,
	opt options,
	dir direction,
	stdin io.Reader,
	stdout, stderr io.Writer,
) error {
	input, err := failoverObjects(ctx, opt, stdin)
	if err != nil {
		return err
	}

	pairs, err := derivePairs(input, opt.namespace, opt.driverName)
	if err != nil {
		return err
	}

	// Applying always needs a cluster, even when the plan came from files: the
	// plan is a set of edits to live objects, and step 0 needs the pod list that
	// is running now.
	var client *kubeClient
	if !opt.dryRun {
		if client, err = kubeClientFor(opt.kubeconfig); err != nil {
			return err
		}
		// Always from the cluster, never from the file: a quiesce planned from a
		// manifest is a quiesce planned from whatever was running when somebody
		// ran `kubectl get`, and step 1 freezes the source.
		if input, err = withLivePods(ctx, client, opt.namespace, input); err != nil {
			return err
		}
	}

	plan, err := planFailover(pairs, dir, failoverOptionsFrom(opt, input))
	if err != nil {
		return err
	}
	writeText(stdout, plan.String())

	if opt.dryRun {
		writef(stderr, "# dry run: nothing was changed. Review the five steps above -- step 0 is the "+
			"quiesce, and nothing is promoted until it is done -- then re-run with -dry-run=false, "+
			"or carry them out by hand.\n")
		if err := plan.Quiesce.refusal(); err != nil {
			writef(stderr, "# NOTE: applied as it stands, this plan would stop at step 0: %v\n", err)
		}
		return nil
	}

	return executeFailover(ctx, plan, newClusterExecutor(client, opt, stderr), stderr)
}
