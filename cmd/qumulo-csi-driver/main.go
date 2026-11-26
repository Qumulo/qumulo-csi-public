// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Command qumulo-csi-driver runs the Qumulo CSI plugin's gRPC server.
//
// It wires a Qumulo REST API client to the CSI Identity and Controller
// services and serves them on the CSI endpoint the container orchestrator
// supplies. All configuration is via flags, each of which also reads a
// corresponding environment variable so the same binary works from a shell
// and from a Kubernetes Deployment without a wrapper script.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/identity"
	"github.com/csi-addons/spec/lib/go/replication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"qumulo-csi-driver/pkg/driver"
	"qumulo-csi-driver/pkg/k8s"
	"qumulo-csi-driver/pkg/metrics"
	"qumulo-csi-driver/pkg/qumulo"
	"qumulo-csi-driver/pkg/version"
)

// config is the driver's resolved runtime configuration.
type config struct {
	endpoint       string
	driverName     string
	mode           string
	nodeID         string
	insecureTLS    bool
	requestTimeout time.Duration
	unmountTimeout time.Duration
	kerberosKeytab string
	regMarker      string
	checkReg       string
	logLevel       string
	showVersion    bool
	fakeMounter    bool
	capacity       bool
	inventory      bool
	replication    bool
	// revertRetention is the expiration given to a replication revert
	// snapshot; zero leaves them never expiring.
	revertRetention time.Duration
	metricsAddress  string
	snapshotPrefix  string
	requireAllowed  bool
}

func main() {
	os.Exit(realMain(os.Args[1:], os.Stdout, os.Stderr))
}

// realMain is main without the exit, so the exit codes are testable: 2 for a
// flag error, 1 for a failed probe or a driver that stopped on an error, 0
// otherwise.
func realMain(args []string, stdout, stderr io.Writer) int {
	// The chart's `helm test` readiness check, run from the controller image
	// so the test pulls no image of its own.
	if len(args) > 0 && args[0] == "ready-check" {
		return readyCheckMain(args[1:], stdout, stderr)
	}

	cfg, err := parseFlags(args)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}

	if cfg.showVersion {
		printVersion(stdout)
		return 0
	}

	// The node pod's readiness exec probe: one stat, no driver.
	if cfg.checkReg != "" {
		if err := driver.RegistrationRecorded(cfg.checkReg); err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}

	deps := productionDeps()
	deps.logOutput = stderr
	if err := runUntilSignalled(cfg, deps); err != nil {
		slog.Error("driver exited", "error", err)
		return 1
	}
	return 0
}

// startupDeps is what run reaches outside the process for: the hostname, the
// Kubernetes API and somewhere to log. The Kubernetes clients come from the
// pod's in-cluster config, which lives at fixed paths, so a test passes
// clients built on a fake clientset instead.
type startupDeps struct {
	hostname     func() (string, error)
	secretClient func() (*k8s.SecretClient, error)
	classClient  func() (*k8s.StorageClassClient, error)
	volumeClient func() (*k8s.PersistentVolumeClient, error)
	claimClient  func() (*k8s.PersistentVolumeClaimClient, error)
	logOutput    io.Writer
}

// productionDeps is what a real process starts with.
func productionDeps() startupDeps {
	return startupDeps{
		hostname:     os.Hostname,
		secretClient: k8s.NewSecretClient,
		classClient:  k8s.NewStorageClassClient,
		volumeClient: k8s.NewPersistentVolumeClient,
		claimClient:  k8s.NewPersistentVolumeClaimClient,
		logOutput:    os.Stderr,
	}
}

// runUntilSignalled runs the driver until SIGINT or SIGTERM.
//
// GracefulStop waits for in-flight handlers, but it does not keep them
// running: the sidecars in this pod get the same signal and exit, the gRPC
// connection closes, and every in-flight handler's context is cancelled at
// once (measured: a CreateVolume failed 50 ms after the drain line). So
// a handler is never relied on to finish here. Rollbacks run on a detached
// context (detachedCleanup), a create stopped at any step is finished by the
// retry, and a cancelled call answers Canceled, not "the array did not
// answer".
//
// But graceful with no deadline is not graceful, it is indefinite. One wedged
// mount held the process for the whole of terminationGracePeriodSeconds and
// the pod was SIGKILLed at the end of it anyway -- so the drain achieved
// nothing and the shutdown looked like a hang. Escalating (see shutdown)
// means termination is a decision this process makes rather than one the
// kubelet makes for it, and the log line says which path was taken, which is
// exactly what an operator wants when a node pod is slow to restart.
func runUntilSignalled(cfg config, deps startupDeps) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return run(ctx, cfg, deps)
}

// parseFlags builds a config from command line arguments, defaulting each
// value from the environment. Environment defaults exist because a CSI
// sidecar deployment sets CSI_ENDPOINT itself, and because the API token
// must not appear in a pod's command line where anyone with read access to
// the object would see it.
func parseFlags(args []string) (config, error) {
	var cfg config

	fs := flag.NewFlagSet("qumulo-csi-driver", flag.ContinueOnError)
	fs.StringVar(&cfg.endpoint, "endpoint", envOr("CSI_ENDPOINT", "unix:///csi/csi.sock"),
		"CSI gRPC endpoint to listen on, e.g. unix:///csi/csi.sock or tcp://127.0.0.1:10000 (env CSI_ENDPOINT)")
	fs.StringVar(&cfg.driverName, "drivername", envOr("CSI_DRIVER_NAME", version.DriverName),
		"plugin name reported to the CO; must match the CSIDriver object (env CSI_DRIVER_NAME)")
	fs.StringVar(&cfg.mode, "mode", envOr("CSI_MODE", string(driver.ModeAll)),
		"which CSI services to serve: controller, node or all (env CSI_MODE)")
	fs.StringVar(&cfg.nodeID, "nodeid", envOr("NODE_ID", ""),
		"ID of the node this instance runs on; required in node mode. In Kubernetes, set NODE_ID from spec.nodeName via fieldRef (env NODE_ID)")
	fs.BoolVar(&cfg.insecureTLS, "insecure-skip-tls-verify", envOr("QUMULO_INSECURE_SKIP_TLS_VERIFY", "") == "true",
		"skip verification of the cluster's TLS certificate; for development against self-signed certs only (env QUMULO_INSECURE_SKIP_TLS_VERIFY=true)")
	fs.DurationVar(&cfg.requestTimeout, "qumulo-api-timeout", 10*time.Second,
		"per-request timeout for calls to the Qumulo REST API")
	fs.StringVar(&cfg.kerberosKeytab, "kerberos-keytab", envOr("KERBEROS_KEYTAB", ""),
		"the host keytab's path inside the node container, when Kerberos is on; a sec=krb5* mount on a "+
			"node whose keytab there is missing or empty is refused before mount(8) runs. Node mode only")
	fs.StringVar(&cfg.regMarker, "registration-marker", envOr("REGISTRATION_MARKER", ""),
		"file the node service writes when the kubelet registers the plugin (its first NodeGetInfo); "+
			"the node pod's readiness checks it with -check-registration. Node mode only (env REGISTRATION_MARKER)")
	fs.StringVar(&cfg.checkReg, "check-registration", "",
		"exit 0 if the -registration-marker file at this path exists, 1 otherwise, and do nothing "+
			"else; the node pod's readiness exec probe")
	fs.DurationVar(&cfg.unmountTimeout, "unmount-timeout", envDurationOr("UNMOUNT_TIMEOUT", 30*time.Second),
		"how long NodeUnpublishVolume waits for a plain umount(8) before escalating to a forced "+
			"and then a lazy unmount. Node mode only. A dead Qumulo array blocks umount(8) indefinitely on "+
			"every NFS mount option, which is what leaves pods stuck Terminating past their grace period "+
			"and stale mounts on the node after a force delete. Raise it on a slow network, where "+
			"a plain unmount that would have succeeded is worth waiting for (env UNMOUNT_TIMEOUT)")
	fs.StringVar(&cfg.logLevel, "log-level", envOr("LOG_LEVEL", "info"),
		"log verbosity: debug, info, warn or error (env LOG_LEVEL)")
	fs.BoolVar(&cfg.capacity, "enable-capacity", envOr("ENABLE_CAPACITY", "") == "true",
		"answer GetCapacity by reading each StorageClass's provisioner Secret from the "+
			"Kubernetes API. Requires running in-cluster with a ServiceAccount permitted to "+
			"get secrets. Without it, GetCapacity reports FailedPrecondition for any request "+
			"naming a cluster (env ENABLE_CAPACITY=true)")
	fs.BoolVar(&cfg.inventory, "enable-inventory", envOr("ENABLE_INVENTORY", "") == "true",
		"answer ListVolumes and ControllerGetVolume by listing this driver's StorageClasses "+
			"from the Kubernetes API to discover the clusters they name, and reading each "+
			"class's provisioner Secret. Requires running in-cluster with a ServiceAccount "+
			"permitted to list storageclasses and get secrets. Without it both RPCs report "+
			"FailedPrecondition and nothing else changes (env ENABLE_INVENTORY=true)")
	fs.BoolVar(&cfg.replication, "enable-replication", envOr("ENABLE_REPLICATION", "") == "true",
		"Serve the csi-addons replication.Controller and identity.Identity services on the CSI "+
			"socket, so a csi-addons sidecar can drive Qumulo directory replication from VolumeReplication "+
			"objects. Every RPC authenticates from its own request, like every other controller RPC; no "+
			"Kubernetes access is needed. Without it the services are not registered and nothing else "+
			"changes (env ENABLE_REPLICATION=true)")
	fs.DurationVar(&cfg.revertRetention, "revert-snapshot-retention",
		envDurationOr("REVERT_SNAPSHOT_RETENTION", driver.DefaultRevertSnapshotRetention),
		"with -enable-replication: how long the array keeps the snapshot it takes of a replication "+
			"target before each revert (promote, DR drill, resync, failback), counted from when the "+
			"driver sees it. The array gives those snapshots no expiration, so without one each "+
			"failover left a snapshot holding the discarded data for ever. 0 leaves them never "+
			"expiring (env REVERT_SNAPSHOT_RETENTION)")
	fs.StringVar(&cfg.metricsAddress, "metrics-address", envOr("METRICS_ADDRESS", ""),
		"Serve Prometheus metrics and a process-liveness /healthz on this "+
			"address, e.g. :29654. Empty (the default) starts no HTTP listener at all and "+
			"registers no collectors, so the driver behaves exactly as it did without this "+
			"flag. What is exported is what the driver already does -- RPC latency and "+
			"outcomes, Qumulo API calls and their status, per-array reachability, per-volume "+
			"quota and usage, and replication recovery-point age. Nothing is polled: the "+
			"controller holds no cluster address and no standing credential (ARCHITECTURE.md "+
			"Rule 0), so every figure is the last one an RPC produced. See docs/reference/metrics.md "+
			"(env METRICS_ADDRESS)")
	fs.StringVar(&cfg.snapshotPrefix, "snapshot-name-prefix", envOr("SNAPSHOT_NAME_PREFIX", driver.DefaultSnapshotNamePrefix),
		"the csi-snapshotter's --snapshot-name-prefix. A snapshot whose name is not \"<prefix>-...\" is not "+
			"taken for this driver's: DeleteSnapshot refuses it without allowForeignSnapshotDelete in the "+
			"deletion Secret, and a restore of one whose source is gone needs allowForeignSnapshotRestore on "+
			"the StorageClass. Controller mode only (env SNAPSHOT_NAME_PREFIX)")
	fs.BoolVar(&cfg.requireAllowed, "require-allowed-clients", envOr("REQUIRE_ALLOWED_CLIENTS", "true") != "false",
		"refuse CreateVolume on a dedicated-export StorageClass with no allowedClients, no krb5 "+
			"securityFlavor and no allowAnyClient: \"true\". Such a class's exports admit any NFS client, as "+
			"root, with no authentication. The chart sets it from requireAllowedClients. Existing "+
			"volumes are not affected. Controller mode only (env REQUIRE_ALLOWED_CLIENTS=false turns it off)")
	fs.BoolVar(&cfg.showVersion, "version", false, "print version information and exit")
	fs.BoolVar(&cfg.fakeMounter, "fake-mounter", false,
		"DANGEROUS, TEST ONLY: never call mount(8); record mounts in memory instead. "+
			"Exists so the csi-sanity conformance suite (test/sanity) can run this binary with no real NFS "+
			"server to mount from. No environment variable on purpose. Never set this in a real deployment: "+
			"NodePublishVolume will report success while nothing is actually mounted")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	// An empty prefix would make every "<id>_-..." name the driver's, and a
	// space or a slash is no name the snapshotter would give.
	if p := cfg.snapshotPrefix; p == "" || strings.ContainsAny(p, " \t\n/") {
		return config{}, fmt.Errorf("-snapshot-name-prefix %q: must be non-empty, with no whitespace or \"/\"", p)
	}
	// A negative retention would stamp an expiration in the past, and the
	// array deletes such a snapshot at once -- the opposite of keeping it.
	if cfg.revertRetention < 0 {
		return config{}, fmt.Errorf("-revert-snapshot-retention %s: must be zero (never expire) or positive", cfg.revertRetention)
	}
	return cfg, nil
}

func printVersion(w io.Writer) {
	info := version.Get()
	_, _ = fmt.Fprintf(w, "driver:  %s\nversion: %s\ncommit:  %s\nbuilt:   %s\ngo:      %s\n",
		info.DriverName, info.DriverVersion, info.GitCommit, info.BuildDate, info.GoVersion)
}

// shutdownGracePeriod bounds the drain of in-flight RPCs on SIGTERM before
// the server is stopped outright.
//
// Comfortably inside the chart's terminationGracePeriodSeconds, so a slow
// shutdown ends in a deliberate stop rather than a SIGKILL. The numbers are
// related and the chart sets its side explicitly rather than inheriting
// Kubernetes' default, so that the relationship is visible.
const shutdownGracePeriod = 10 * time.Second

// promoteShutdownWait is how much longer shutdown waits, past the drain, while
// a planned promote is in flight: its whole budget, so one that began just
// before the signal finishes. Stopped after the freeze, a promote leaves both
// copies read-only until csi-addons retries it on another controller.
// The drain and this together stay inside the chart's controller
// terminationGracePeriodSeconds (90).
const promoteShutdownWait = driver.PlannedPromoteBudget

// promoteWaiter is what shutdown needs of the replication server.
type promoteWaiter interface {
	PromotesInFlight() int
	WaitForPromotes(timeout time.Duration) bool
}

// stoppableServer is what shutdown needs of a gRPC server, so the escalation
// can be tested without standing one up.
type stoppableServer interface {
	GracefulStop()
	Stop()
}

// shutdown drains in-flight RPCs, then stops the server outright if the drain
// outlasts grace -- unless a promote is still in flight, in which case it
// waits up to promoteWait more for the promotes first. The server is not
// stopped, and Serve does not return, until then.
//
// Returns true when the drain completed, false when it had to be forced, so a
// test can assert which path was taken rather than reading logs.
func shutdown(server stoppableServer, grace time.Duration, promotes promoteWaiter, promoteWait time.Duration) bool {
	slog.Info("shutdown signal received; draining in-flight RPCs", "gracePeriod", grace)

	drained := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(drained)
	}()

	timer := time.NewTimer(grace)
	defer timer.Stop()

	select {
	case <-drained:
		slog.Info("in-flight RPCs drained; stopping")
		return true
	case <-timer.C:
	}

	if promotes != nil {
		if n := promotes.PromotesInFlight(); n > 0 {
			slog.Warn("graceful stop timed out with planned promotes in flight; waiting for them before stopping",
				"promotes", n, "wait", promoteWait)
			finished := make(chan bool, 1)
			go func() { finished <- promotes.WaitForPromotes(promoteWait) }()
			select {
			case <-drained:
				slog.Info("in-flight RPCs drained; stopping")
				return true
			case ok := <-finished:
				if !ok {
					slog.Warn("planned promotes did not finish in time; the controller that takes over retries them",
						"wait", promoteWait)
				}
			}
			// The promote may have been the drain's last RPC.
			select {
			case <-drained:
				slog.Info("in-flight RPCs drained; stopping")
				return true
			case <-time.After(100 * time.Millisecond):
			}
		}
	}

	// Stop unblocks GracefulStop, so the goroutine above ends rather than
	// outliving the process's last moments.
	slog.Warn("graceful stop timed out, forcing; an in-flight RPC was abandoned",
		"gracePeriod", grace)
	server.Stop()
	return false
}

// run serves the driver until ctx is cancelled, then drains and returns.
func run(ctx context.Context, cfg config, deps startupDeps) error {
	if err := setupLogging(cfg.logLevel, deps.logOutput); err != nil {
		return err
	}

	mode, err := driver.ParseMode(cfg.mode)
	if err != nil {
		return fmt.Errorf("invalid -mode: %w", err)
	}

	// ModeAll exists for local development and csi-sanity, where demanding
	// a node ID for a single-process run is friction with nothing to catch.
	// A real node deployment must still pass one explicitly: in Kubernetes
	// the node's name is what the CO matches against, and the container's
	// hostname is not reliably that.
	if mode == driver.ModeAll && cfg.nodeID == "" {
		hostname, err := deps.hostname()
		if err != nil {
			return fmt.Errorf("no -nodeid given and hostname unavailable: %w", err)
		}
		cfg.nodeID = hostname
		slog.Warn("no -nodeid set, defaulting to hostname; acceptable in all mode, never in a node deployment",
			"nodeID", cfg.nodeID)
	}

	// No process-wide client is built any more. Every controller call
	// authenticates from the secrets on its own request, so there is nothing
	// to construct here and no standing credential for any cluster.
	warnOnLegacyFallbackConfig()

	// Built before the driver, because the driver's per-request clients are
	// handed the registry as their observer. Nil when the flag is unset, and
	// a nil observer is what every deployment that does not ask for metrics
	// gets: no collectors, no listener, no behaviour change.
	var registry *metrics.Registry
	if cfg.metricsAddress != "" {
		registry = metrics.New(version.Get())
	}

	cfgDriver := driver.Config{
		Name:     cfg.driverName,
		Version:  version.DriverVersion,
		Mode:     mode,
		NodeID:   cfg.nodeID,
		Endpoint: cfg.endpoint,
		// Shared with every per-request client the controller builds, so the
		// process's TLS policy and timeout apply to all clusters.
		HTTPClient:  newHTTPClient(cfg),
		FakeMounter: cfg.fakeMounter,
		Replication: cfg.replication && mode.ServesController(),
		// Only read by the replication service.
		RevertSnapshotRetention: cfg.revertRetention,
		// Node instances get one too, and hold it unused: the node service
		// never calls the REST API. It costs nothing and means the field
		// does not have to be conditional on the mode in two places.
		Observer: observerOrNil(registry),
		// Set beside Observer and from the same registry. It is only ever
		// called from the inventory paths, so a controller without
		// -enable-inventory never reaches it.
		InventoryObserver: inventoryObserverOrNil(registry),
		// Node-side only; a controller has no mounts. Passed unconditionally
		// because Config is shared, and ignored when the node service is not
		// served.
		UnmountTimeout: cfg.unmountTimeout,
		KerberosKeytab: cfg.kerberosKeytab,
		// Node-side only.
		RegistrationMarker: cfg.regMarker,
		// Controller-side only, like the timeout above is node-side only.
		SnapshotNamePrefix: cfg.snapshotPrefix,
		// Controller-side only.
		AllowOpenDedicatedExports: !cfg.requireAllowed,
		// One per process, shared by every per-request client.
		Breaker: qumulo.NewBreaker(),
	}

	// Capacity reporting is the one feature that needs Kubernetes access, so
	// it is the one feature that is opt-in. A controller without it serves
	// every other RPC unchanged.
	if cfg.capacity && mode.ServesController() {
		reader, err := deps.secretClient()
		if err != nil {
			// Deliberately not fatal. Every other RPC works without Kubernetes
			// access, so refusing to start would turn an optional feature into
			// an outage -- and the most likely cause is running outside a pod,
			// where that is expected rather than wrong.
			slog.Warn("-enable-capacity is set but no in-cluster Kubernetes config is available; "+
				"GetCapacity will report FailedPrecondition and every other RPC is unaffected",
				"error", err)
		} else {
			cfgDriver.SecretReader = reader
			slog.Info("capacity reporting enabled; GetCapacity will resolve each StorageClass's provisioner Secret")
		}
	}

	// Volume inventory needs the same Kubernetes access plus the ability to
	// list StorageClasses, because ListVolumes is told no cluster at all. This
	// is the one feature that lets the controller act on a cluster no request
	// named, so it is opt-in and Rule 0 in ARCHITECTURE.md says why.
	if cfg.inventory && mode.ServesController() {
		reader, readerErr := deps.secretClient()
		lister, listerErr := deps.classClient()
		pvs, pvsErr := deps.volumeClient()
		if listerErr == nil {
			listerErr = pvsErr
		}
		switch {
		case readerErr != nil || listerErr != nil:
			// Not fatal, for the same reason as capacity: every other RPC
			// works without Kubernetes access, and the likeliest cause is
			// running outside a pod.
			slog.Warn("-enable-inventory is set but no in-cluster Kubernetes config is available; "+
				"ListVolumes and ControllerGetVolume will report FailedPrecondition and every "+
				"other RPC is unaffected",
				"secretReaderError", readerErr, "storageClassListerError", listerErr)
		default:
			// Assigned rather than only the lister: ControllerGetVolume needs
			// both, and -enable-inventory without -enable-capacity must still
			// give it a way to read a Secret.
			cfgDriver.SecretReader = reader
			cfgDriver.StorageClassLister = lister
			// ListVolumes reads this driver's PV handles too, to list a
			// directory two classes reach under the handle its PV carries.
			// The same kind of read as the StorageClass list.
			cfgDriver.VolumeHandleLister = pvs
			slog.Info("volume inventory enabled; ListVolumes and ControllerGetVolume will " +
				"discover clusters from this driver's StorageClasses")
		}
	}
	// DeleteVolume reads this driver's PersistentVolumes before it removes
	// anything, and refuses while another PV in use names the same directory.
	// Unlike inventory this is not opt-in: it protects a delete rather
	// than discovering a cluster, it is a plain paged List per delete (Rule 4:
	// never a watch), and the chart grants list on persistentvolumes to the
	// controller unconditionally, because csi-provisioner needs it.
	if mode.ServesController() {
		if pvs, err := deps.volumeClient(); err != nil {
			// Not fatal, for the same reason as capacity: outside a pod --
			// csi-sanity, a local run -- there is no API to ask, and
			// DeleteVolume then behaves as it did before the check.
			slog.Warn("no in-cluster Kubernetes config is available, so DeleteVolume cannot check that no other "+
				"PersistentVolume still uses a volume's directory; expected outside a cluster", "error", err)
		} else {
			cfgDriver.PersistentVolumeLister = pvs
		}
		// And the claims, for the same kind of reason: a restore's staging
		// directory is named after its claim's UID, and it is removed only
		// once that claim is gone -- by DeleteVolume, and by the
		// sweep a successful CreateVolume runs at most every 10 minutes per
		// store root. A paged List, never a watch; the chart grants list on
		// persistentvolumeclaims to the controller because csi-provisioner
		// needs it.
		if pvcs, err := deps.claimClient(); err != nil {
			slog.Warn("no in-cluster Kubernetes config is available, so CreateVolume will not sweep the staging "+
				"directories of restores whose claims were deleted; expected outside a cluster", "error", err)
		} else {
			cfgDriver.PersistentVolumeClaimLister = pvcs
		}
	}
	if cfg.fakeMounter {
		slog.Warn("-fake-mounter is set: NodePublishVolume will not actually mount anything. " +
			"This is a csi-sanity test mode and must never be used in a real deployment")
	}
	d, err := driver.NewDriver(cfgDriver)
	if err != nil {
		return fmt.Errorf("configuring driver: %w", err)
	}

	listener, err := driver.Listen(cfg.endpoint)
	if err != nil {
		return err
	}

	grpcServer := newGRPCServer(registry)

	served := registerServices(grpcServer, d)

	// Stop when ctx is done (runUntilSignalled: SIGINT/SIGTERM). The child
	// context also ends the shutdown goroutine below if Serve fails first.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var metricsServer *http.Server
	if registry != nil {
		metricsServer = newMetricsServer(cfg.metricsAddress, registry)
		go serveMetrics(metricsServer)
		// Retires the reachability and capacity series of an endpoint nothing
		// has called for an hour, so its alert clears.
		go registry.RunRetirement(ctx)
		// Retires the per-volume series of volumes no PersistentVolume carries
		// any more -- every way a volume leaves other than a DeleteVolume this
		// process served -- and the readings another controller
		// replica is the one keeping current. A periodic list, never
		// a watch (rule 4). The PVs' capacities come with the same List, so
		// the stale-quota check costs no extra call.
		if mode.ServesController() {
			if pvs, err := deps.volumeClient(); err != nil {
				slog.Warn("cannot reach the Kubernetes API, so per-volume series of volumes removed without a "+
					"DeleteVolume here, and readings this replica no longer refreshes, are kept until a restart",
					"error", err)
			} else {
				go registry.RunVolumeReconcile(ctx, volumeRecordLister(pvs, cfg.driverName))
			}
		}
		slog.Info("serving prometheus metrics", "address", cfg.metricsAddress,
			"paths", "/metrics,/healthz")
	}

	go func() {
		<-ctx.Done()
		// The scrape endpoint goes first. It serves a reading nobody will
		// act on once the pod is terminating, and a scrape in flight must
		// not eat into the RPC drain the grace period is there for.
		stopMetricsServer(metricsServer)
		var promotes promoteWaiter
		if repl := d.ReplicationServer(); repl != nil {
			promotes = repl
		}
		shutdown(grpcServer, shutdownGracePeriod, promotes, promoteShutdownWait)
	}()

	info := version.Get()
	attrs := []any{
		"name", cfg.driverName,
		"version", info.DriverVersion,
		"commit", info.GitCommit,
		"mode", mode.String(),
		"services", strings.Join(served, ","),
		"endpoint", cfg.endpoint,
	}
	if mode.ServesNode() {
		attrs = append(attrs, "nodeID", cfg.nodeID)
	}
	if mode.ServesController() {
		// Said explicitly so the log answers "which cluster is this talking
		// to?" the same way every time: none, until a request names one.
		attrs = append(attrs, "routing", "per-request (StorageClass parameters + secrets)")
	}
	slog.Info("starting qumulo csi driver", attrs...)

	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("serving csi endpoint %s: %w", cfg.endpoint, err)
	}

	slog.Info("driver stopped")
	return nil
}

// registerServices registers what d serves on grpcServer and names them, for
// the startup log line.
func registerServices(grpcServer *grpc.Server, d *driver.Driver) []string {
	// Identity is mandatory for every plugin; the other two follow the mode.
	// Registering a service this instance does not serve would answer RPCs
	// it has no configuration for, so an unregistered service correctly
	// reports Unimplemented instead.
	served := []string{"identity"}
	csi.RegisterIdentityServer(grpcServer, d.IdentityServer())
	if controller := d.ControllerServer(); controller != nil {
		csi.RegisterControllerServer(grpcServer, controller)
		served = append(served, "controller")
	}
	if node := d.NodeServer(); node != nil {
		csi.RegisterNodeServer(grpcServer, node)
		served = append(served, "node")
	}
	// The csi-addons services share the socket: the sidecar dials the same
	// endpoint and asks identity.Identity what is served. Registered only
	// when asked for, so a deployment without the sidecar advertises nothing
	// it cannot be driven through.
	if repl := d.ReplicationServer(); repl != nil {
		replication.RegisterControllerServer(grpcServer, repl)
		identity.RegisterIdentityServer(grpcServer, d.ReplicationIdentityServer())
		served = append(served, "csi-addons replication")
		slog.Info("csi-addons replication service enabled; supported limits are in docs/reference/supported-features.md")
	}
	return served
}

// volumeRecordLister lists this driver's PersistentVolumes as the records the
// metrics volume reconcile keeps series for.
func volumeRecordLister(pvs *k8s.PersistentVolumeClient, driverName string) func(context.Context) ([]metrics.VolumeRecord, error) {
	return func(ctx context.Context) ([]metrics.VolumeRecord, error) {
		infos, err := pvs.ListPersistentVolumes(ctx, driverName)
		if err != nil {
			return nil, err
		}
		records := make([]metrics.VolumeRecord, 0, len(infos))
		for _, info := range infos {
			records = append(records, metrics.VolumeRecord{
				Handle: info.Handle, CapacityBytes: info.CapacityBytes,
			})
		}
		return records, nil
	}
}

// newHTTPClient builds the transport every Qumulo client shares.
//
// It is separate from newQumuloClient because the controller also builds a
// client per request, for the cluster that request names. Those clients must
// inherit the same timeout and TLS policy -- otherwise a flag such as
// -insecure-skip-tls-verify would apply only to the process-wide fallback
// client and silently not to any cluster reached through a StorageClass.
func newHTTPClient(cfg config) *http.Client {
	// One transport for both paths, cloned from Go's default so it keeps the
	// proxy settings, an idle-connection expiry and a TLS handshake bound.
	// The insecure path used to build a bare Transport holding only the TLS
	// setting: no dial or handshake timeout (the 10 s request timeout was the
	// only bound), idle connections kept for every endpoint ever dialled, and
	// HTTPS_PROXY silently ignored.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// A dead array is found in a few seconds, not at the request timeout, so
	// a call to it holds a sidecar worker that much less.
	tr.DialContext = (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext
	tr.TLSHandshakeTimeout = tlsHandshakeTimeout
	// Go keeps 2 idle connections per host by default, so every burst of
	// calls to one array beyond two -- the sidecars run 10 to 100 workers --
	// paid a new TCP and TLS session per call.
	tr.MaxIdleConnsPerHost = maxIdleConnsPerHost
	// A proxy refusing the CONNECT to an array comes back typed, so a
	// refusal no retry can pass is reported as one rather than as an array
	// that is down.
	tr.OnProxyConnectResponse = qumulo.OnProxyConnectResponse
	if cfg.insecureTLS {
		slog.Warn("TLS certificate verification is disabled for the qumulo api; do not use this in production")
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &http.Client{Timeout: cfg.requestTimeout, Transport: tr}
}

// dialTimeout and tlsHandshakeTimeout bound connecting to an array.
const (
	dialTimeout         = 3 * time.Second
	tlsHandshakeTimeout = 5 * time.Second
)

// maxIdleConnsPerHost is how many idle connections to one array are kept for
// reuse. Idle connections still expire after the default transport's 90 s.
const maxIdleConnsPerHost = 32

// logRPC logs every RPC with its outcome and duration. CSI drivers are
// driven entirely by sidecars, so this log is usually the only record of
// what the CO asked for.
func logRPC(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	elapsed := time.Since(start)

	if err != nil {
		slog.ErrorContext(ctx, "rpc failed", "method", info.FullMethod, "duration", elapsed, "error", err)
		return resp, err
	}
	slog.DebugContext(ctx, "rpc served", "method", info.FullMethod, "duration", elapsed)
	return resp, nil
}

func setupLogging(level string, w io.Writer) error {
	var parsed slog.Level
	if err := parsed.UnmarshalText([]byte(level)); err != nil {
		return fmt.Errorf("invalid -log-level %q: want debug, info, warn or error", level)
	}

	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: parsed}))
	slog.SetDefault(logger)
	// k8s.io/mount-utils logs through klog. Left alone, klog writes its own
	// unstructured, multi-line format ("E1119 ... Mount failed: exit status 32"
	// followed by the mount command and output on lines of their own) beside the
	// driver's key=value lines, so a node log was only partly parseable and a
	// failed mount's output was split from its timestamp (OBS-18). Routed here,
	// the same failure is one structured line at level ERROR.
	klog.SetSlogLogger(logger)
	return nil
}

// envDurationOr is envOr for a duration-valued flag. An unparseable value is
// reported and the default kept rather than failing to start: an environment
// variable typo must not turn a node plugin into a crash loop on every worker,
// and the flag it backs is a tuning knob, not a correctness one.
func envDurationOr(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ignoring %s=%q: %v; using %s\n", key, raw, err, fallback)
		return fallback
	}
	return parsed
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// legacyFallbackEnv are the environment variables that configured the
// process-wide fallback client, removed in the multi-cluster purge.
//
// A deployment upgrading from a chart that set them will still have them in the
// pod spec, and the driver now ignores them silently -- which is the worst
// combination: an operator who set QUMULO_SERVER reasonably assumes it is being
// used. Naming them at startup turns that into one line in the log.
var legacyFallbackEnv = []string{
	"QUMULO_SERVER",
	"QUMULO_API_URL",
	"QUMULO_API_TOKEN",
	"QUMULO_API_TOKEN_FILE",
}

// warnOnLegacyFallbackConfig reports any leftover fallback configuration.
//
// Deliberately a warning rather than a fatal error: the variables are inert, so
// refusing to start would turn a cosmetic upgrade artefact into an outage. The
// message says what to do instead.
func warnOnLegacyFallbackConfig() {
	var found []string
	for _, name := range legacyFallbackEnv {
		if _, ok := os.LookupEnv(name); ok {
			found = append(found, name)
		}
	}
	if len(found) == 0 {
		slog.Debug("no legacy fallback configuration present")
		return
	}

	slog.Warn("ignoring legacy fallback configuration left over from an older chart; "+
		"this driver keeps no process-wide credential, so every operation authenticates "+
		"from its own request. Remove qumulo.clusterAddress and qumulo.token from your "+
		"values, and set server plus csi.storage.k8s.io/*-secret-name on each StorageClass",
		"variables", found)
}

// metricsShutdownTimeout bounds the drain of the metrics listener.
//
// Deliberately short and separate from shutdownGracePeriod: a scrape is a
// read of numbers that are about to stop mattering, and the whole point of
// stopping it first is to leave the RPC drain its full budget.
const metricsShutdownTimeout = 2 * time.Second

// metricsReadHeaderTimeout bounds how long a client may take to send its
// request headers. Set explicitly because an HTTP server with no header
// timeout is a slowloris away from holding a connection forever, and because
// gosec fails the build without it.
const metricsReadHeaderTimeout = 5 * time.Second

// observerOrNil returns the registry as a qumulo.Observer, or a nil
// interface when there is no registry.
//
// It exists to avoid the typed-nil trap: assigning a nil *metrics.Registry
// straight into an interface field produces an interface that is not nil, so
// every guarded call site in pkg/qumulo would dereference it.
func observerOrNil(registry *metrics.Registry) qumulo.Observer {
	if registry == nil {
		return nil
	}
	return registry
}

// inventoryObserverOrNil is observerOrNil for the inventory signal: a typed
// nil in an interface is not nil, so the registry must be left out rather
// than handed over as one.
func inventoryObserverOrNil(registry *metrics.Registry) driver.InventoryObserver {
	if registry == nil {
		return nil
	}
	return registry
}

// newMetricsServer builds the HTTP server that serves /metrics and /healthz.
//
// /healthz answers 200 unconditionally and means exactly what Probe means:
// this process is alive. It says nothing about any array, for the same
// reason Probe does not -- one controller serves many clusters and is never
// told the full set, and a health endpoint that failed on an unreachable
// array would restart a controller still serving every other one. Array
// reachability is csi_qumulo_api_reachable, a gauge to alert on, not a
// liveness check to restart on. See ARCHITECTURE.md Rule 0.
func newMetricsServer(address string, registry *metrics.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", registry.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	return &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: metricsReadHeaderTimeout,
	}
}

// serveMetrics runs the metrics listener until it is shut down.
//
// A failure here is logged and never fatal. Metrics are an observability
// feature; a port already in use must not take provisioning down with it,
// which is the same judgement the capacity and inventory features make about
// missing Kubernetes access.
func serveMetrics(server *http.Server) {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("metrics listener stopped; every other service is unaffected",
			"address", server.Addr, "error", err)
	}
}

// stopMetricsServer drains the metrics listener, if there is one.
func stopMetricsServer(server *http.Server) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Warn("metrics listener did not drain", "error", err)
	}
}

// newGRPCServer is the gRPC server run() serves every service on, with its
// interceptor chain. registry may be nil when metrics are off.
func newGRPCServer(registry *metrics.Registry) *grpc.Server {
	// Chained rather than a single interceptor, so the log line and the
	// series describe the same call and neither has to be folded into the
	// other. Metrics run inside logRPC: the duration logRPC reports is the
	// handler's, and the one the histogram records is the same.
	interceptors := []grpc.UnaryServerInterceptor{logRPC}
	if registry != nil {
		interceptors = append(interceptors, registry.UnaryServerInterceptor)
	}
	// Innermost, so a panic is an Internal answer by the time the metrics
	// and the log line see it. Without it one panicking handler exited the
	// process and stopped every storage operation the plugin serves.
	interceptors = append(interceptors, recoverRPC(registry))
	return grpc.NewServer(grpc.ChainUnaryInterceptor(interceptors...))
}

// recoverRPC is the innermost interceptor: a handler that panics answers
// Internal, its stack is logged, and csi_qumulo_rpc_panics_total counts it.
// registry may be nil when metrics are off.
func recoverRPC(registry *metrics.Registry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "rpc panicked; answered Internal, the plugin keeps serving",
					"method", info.FullMethod, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
				if registry != nil {
					registry.RecordPanic(info.FullMethod)
				}
				resp, err = nil, status.Errorf(codes.Internal, "%s: internal error (recovered panic: %v)", info.FullMethod, r)
			}
		}()
		return handler(ctx, req)
	}
}
