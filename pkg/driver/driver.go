// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package driver implements the CSI Identity, Controller and Node gRPC
// services for the Qumulo CSI driver.
package driver

import (
	"errors"
	"net/http"
	"time"

	mount "k8s.io/mount-utils"

	"qumulo-csi-driver/pkg/qumulo"
)

// Driver holds the state shared by the Identity, Controller and Node
// service implementations.
type Driver struct {
	// Name is the plugin name reported to the CO, e.g. "csi.qumulo.com".
	Name string
	// Version is the plugin version reported to the CO.
	Version string
	// Mode selects which CSI services this instance serves. See Mode.
	Mode Mode
	// NodeID identifies the node this driver instance is running on. It is
	// required only when the node service is served.
	NodeID string
	// Endpoint is the CSI gRPC endpoint this driver instance listens on,
	// e.g. "unix:///csi/csi.sock".
	Endpoint string
	// NewClient builds a client for one request's target cluster. It is a
	// field so tests can hand out fakes per cluster and assert that a call
	// naming one cluster never reaches another's client.
	NewClient func(baseURL, token string) qumulo.ClientInterface
	// SecretReader resolves the Kubernetes Secret a StorageClass names, for
	// the one controller RPC whose request carries a secret reference but not
	// the secret itself. Nil on a node instance, and nil in any controller
	// built without Kubernetes access -- GetCapacity reports that rather than
	// dereferencing it. See SecretReader and get_capacity.go.
	SecretReader SecretReader
	// StorageClassLister discovers the clusters this release provisions into,
	// for the two RPCs that name none. Nil unless the deployment opted in --
	// see StorageClassLister and inventory.go, and Rule 0 in ARCHITECTURE.md for why
	// this one is opt-in when SecretReader is not.
	StorageClassLister StorageClassLister
	// VolumeHandleLister reads this driver's PersistentVolume handles, so
	// ListVolumes can list a directory under the handle its PV carries.
	// Nil unless inventory is enabled; see VolumeHandleLister.
	VolumeHandleLister VolumeHandleLister
	// PersistentVolumeLister reads this driver's PersistentVolumes, so
	// DeleteVolume can refuse to remove a directory another live PV names.
	// Nil outside a cluster; see PersistentVolumeLister.
	PersistentVolumeLister PersistentVolumeLister
	// PersistentVolumeClaimLister reads the UIDs of the cluster's claims, so
	// a restore staging directory is removed only once its claim is gone.
	// Nil outside a cluster; see PersistentVolumeClaimLister.
	PersistentVolumeClaimLister PersistentVolumeClaimLister
	// InventoryObserver is told whether each StorageClass the inventory
	// walked had a usable credential, so a class skipped for a permanent
	// reason is alertable and not only logged. Nil unless metrics are
	// enabled, and a nil observer changes no behaviour.
	InventoryObserver InventoryObserver

	// RevertSnapshotRetention is the expiration given to the snapshot a
	// replication target array takes before each revert. Zero leaves
	// those snapshots as the array made them, never expiring.
	RevertSnapshotRetention time.Duration
	// Observer is handed to every per-request client this driver builds, so
	// what those clients learn reaches the metrics registry. Nil unless
	// -metrics-address is set, and a nil Observer changes no behaviour.
	Observer qumulo.Observer
	// UnmountTimeout bounds the plain umount(8) in NodeUnpublishVolume before
	// it escalates to `umount -f` and then `umount -l`. Zero means the
	// package default; see defaultUnmountTimeout and node_unmount.go.
	UnmountTimeout time.Duration
	// KerberosKeytab is where the node plugin finds the host's keytab, or ""
	// when Kerberos is off. A Kerberos mount on a node whose keytab there is
	// missing or empty is refused before mount(8) runs.
	KerberosKeytab string
	// RegistrationMarker is the file NodeGetInfo writes when the kubelet
	// registers the plugin, which the node pod's readiness checks; "" writes
	// none (registration.go).
	RegistrationMarker string
	// SnapshotNamePrefix is the prefix csi-snapshotter puts on the snapshot
	// names it asks for, "<prefix>-<VolumeSnapshot UID>". A snapshot whose
	// stored name has that shape is one this driver made; see
	// isDriverSnapshotName. Never empty once NewDriver has run.
	SnapshotNamePrefix string
	// AllowOpenDedicatedExports turns off the refusal of an open
	// dedicated-export class at CreateVolume; see openExportRefusal.
	// False, the safe value, unless -require-allowed-clients=false.
	AllowOpenDedicatedExports bool

	identityServer   *IdentityServer
	controllerServer *ControllerServer
	nodeServer       *NodeServer

	// The csi-addons replication service and its identity twin. Built only
	// when Config.Replication is set on a controller instance; see
	// ReplicationServer for why it is opt-in.
	replicationServer         *ReplicationServer
	replicationIdentityServer *ReplicationIdentityServer
}

// Config carries the values needed to construct a Driver.
type Config struct {
	Name    string
	Version string
	// Mode selects which services to serve. The zero value serves the
	// controller; see Mode.
	Mode     Mode
	NodeID   string
	Endpoint string
	Server   string
	Client   qumulo.ClientInterface
	// NewClient builds a client for one request's target cluster. Optional:
	// left nil, the driver uses the real Qumulo constructor. Tests set it to
	// hand out a fake per cluster.
	NewClient func(baseURL, token string) qumulo.ClientInterface

	// SecretReader resolves the Secret a StorageClass names, so GetCapacity
	// can authenticate to the cluster that class points at. Optional: left
	// nil, GetCapacity answers FailedPrecondition for any request naming a
	// cluster, which is what happens under csi-sanity and in local runs.
	SecretReader SecretReader

	// StorageClassLister enumerates the StorageClasses naming this driver, so
	// ListVolumes and ControllerGetVolume can find the clusters their requests
	// do not name. Optional: left nil, both answer FailedPrecondition and
	// neither capability is affected -- the set is fixed, see
	// controllerCapabilities.
	StorageClassLister StorageClassLister

	// VolumeHandleLister reads the handles of this driver's
	// PersistentVolumes. Optional, and only consulted by ListVolumes: left
	// nil, a directory two classes reach is listed under the handle with no
	// endpoint field, a guess.
	VolumeHandleLister VolumeHandleLister

	// PersistentVolumeLister reads this driver's PersistentVolumes for
	// DeleteVolume's check that no other live PV names the directory it is
	// about to remove. Optional: left nil -- csi-sanity, local runs,
	// any controller outside a cluster -- DeleteVolume skips that check and
	// says so once in the log. Not tied to inventory: a controller in a
	// cluster always has it.
	PersistentVolumeLister PersistentVolumeLister

	// PersistentVolumeClaimLister reads the UIDs of the cluster's claims, so
	// DeleteVolume and CreateVolume's sweep leave a restore's staging
	// directory alone while its claim exists. Optional: left nil --
	// csi-sanity, local runs -- CreateVolume sweeps nothing and DeleteVolume
	// reclaims the staging sibling of a volume already gone without asking.
	// Not tied to inventory: a controller in a cluster always has it.
	PersistentVolumeClaimLister PersistentVolumeClaimLister

	// InventoryObserver is told whether each StorageClass the inventory
	// walked had a usable credential. Optional, and set from the same place
	// Observer is: a deployment without metrics gets nil and behaves exactly
	// as it did. See InventoryObserver.
	InventoryObserver InventoryObserver

	// Observer is told what each per-request Qumulo client learned, so
	// pkg/metrics can export array-side figures without any RPC handler
	// being edited to record one. Optional: left nil, every client is built
	// exactly as it was before metrics existed.
	//
	// It is attached only to clients this package constructs. A Config that
	// supplies its own NewClient -- which is what the tests do -- owns that
	// decision, because a fake has nothing to observe.
	Observer qumulo.Observer

	// Replication serves the csi-addons replication.Controller and
	// identity.Identity services alongside the CSI controller. Supported,
	// opt-in: off by default, and nothing else about the driver changes when it is
	// off -- the services are simply not registered, so a sidecar asking for
	// them gets Unimplemented.
	Replication bool

	// RevertSnapshotRetention is how long the array keeps the snapshot it
	// takes of a replication target before reverting it, on make-writable
	// and reconnect. Zero leaves them never expiring, as the array
	// makes them; the controller binary defaults it to
	// DefaultRevertSnapshotRetention.
	RevertSnapshotRetention time.Duration

	// HTTPClient is the transport per-request clients are given. It carries
	// the process's timeout and TLS policy, so a flag like
	// -insecure-skip-tls-verify applies to every cluster this controller
	// talks to and not merely to the one configured at startup.
	HTTPClient *http.Client

	// UnmountTimeout bounds the plain umount(8) in NodeUnpublishVolume before
	// the forced escalation starts. Optional: zero selects
	// defaultUnmountTimeout. Wired from the node's -unmount-timeout flag.
	UnmountTimeout time.Duration

	// Breaker is shared by every client the default NewClient builds; nil
	// means none. The binary sets one.
	Breaker *qumulo.Breaker

	// KerberosKeytab is the keytab's path inside the node container, from the
	// node's -kerberos-keytab flag; "" when the chart has Kerberos off.
	KerberosKeytab string

	// RegistrationMarker is the node's -registration-marker flag; "" when
	// unset. Node-side only.
	RegistrationMarker string

	// SnapshotNamePrefix is the csi-snapshotter's --snapshot-name-prefix,
	// from the controller's -snapshot-name-prefix flag. Optional: empty
	// selects DefaultSnapshotNamePrefix, the sidecar's own default.
	SnapshotNamePrefix string

	// AllowOpenDedicatedExports is the controller's -require-allowed-clients
	// flag, inverted so the zero value refuses an open dedicated-export class.
	// Set by the chart's requireAllowedClients: false.
	AllowOpenDedicatedExports bool

	// FakeMounter swaps the node service's mounter for mount.FakeMounter,
	// which records mount/unmount calls in memory and never execs mount(8).
	//
	// It exists solely for the csi-sanity conformance suite (test/sanity),
	// which runs this binary with no real NFS server for it to mount from.
	// A real deployment must never set this: NodePublishVolume would report
	// success while nothing was actually mounted.
	FakeMounter bool
}

// NewDriver validates the supplied configuration and wires up the CSI
// service implementations. Validating once here, rather than in every RPC
// handler, lets the Identity/Controller/Node servers assume a Driver is
// always well-formed.
func NewDriver(cfg Config) (*Driver, error) {
	if cfg.Name == "" {
		return nil, errors.New("driver name must not be empty")
	}
	if cfg.Version == "" {
		return nil, errors.New("driver version must not be empty")
	}

	mode, err := ParseMode(string(cfg.Mode))
	if err != nil {
		return nil, err
	}

	// What a Driver needs depends on the role it plays. Requiring the
	// controller's credentials of a node instance would mean mounting a
	// Qumulo API token on every worker for code that never makes an API
	// call; requiring a node ID of a controller would demand a value that
	// has no meaning there.
	// A controller no longer requires a cluster address or a client: it
	// resolves both per request from StorageClass parameters and secrets. When
	// they are supplied they act as the single-cluster fallback described on
	// Driver.Client, which is what makes an in-place upgrade safe.
	if mode.ServesNode() && cfg.NodeID == "" {
		return nil, errors.New("node ID must not be empty when serving the node service")
	}

	d := &Driver{
		Name:                   cfg.Name,
		Version:                cfg.Version,
		Mode:                   mode,
		NodeID:                 cfg.NodeID,
		Endpoint:               cfg.Endpoint,
		NewClient:              cfg.NewClient,
		SecretReader:           cfg.SecretReader,
		StorageClassLister:     cfg.StorageClassLister,
		VolumeHandleLister:     cfg.VolumeHandleLister,
		PersistentVolumeLister: cfg.PersistentVolumeLister,
		InventoryObserver:      cfg.InventoryObserver,
		Observer:               cfg.Observer,
		UnmountTimeout:         cfg.UnmountTimeout,
		KerberosKeytab:         cfg.KerberosKeytab,
		RegistrationMarker:     cfg.RegistrationMarker,
		SnapshotNamePrefix:     cfg.SnapshotNamePrefix,

		PersistentVolumeClaimLister: cfg.PersistentVolumeClaimLister,
		AllowOpenDedicatedExports:   cfg.AllowOpenDedicatedExports,
		RevertSnapshotRetention:     cfg.RevertSnapshotRetention,
	}
	if d.SnapshotNamePrefix == "" {
		d.SnapshotNamePrefix = DefaultSnapshotNamePrefix
	}

	// Only the services this instance serves are built, so an accessor
	// returning nil is the signal not to register that service rather than
	// something for the caller to guess at.
	if d.NewClient == nil {
		httpClient := cfg.HTTPClient
		observer := cfg.Observer
		breaker := cfg.Breaker
		d.NewClient = func(baseURL, token string) qumulo.ClientInterface {
			c := qumulo.New(baseURL, token)
			if httpClient != nil {
				c.HTTPClient = httpClient
			}
			// Set here and never mutated afterwards, so one registry is
			// shared by every per-request client without a lock of its own.
			// Still a *qumulo.Client, so the replication service's assertion
			// to its own interface keeps working -- a wrapper type here would
			// silently turn every replication RPC into Unimplemented.
			c.Observer = observer
			c.Breaker = breaker
			return c
		}
	}

	d.identityServer = NewIdentityServer(d)
	if mode.ServesController() {
		d.controllerServer = NewControllerServer(d)
		if cfg.Replication {
			d.replicationServer = NewReplicationServer(d)
			d.replicationIdentityServer = NewReplicationIdentityServer(d)
		}
	}
	if mode.ServesNode() {
		if cfg.FakeMounter {
			d.nodeServer = NewNodeServerWithMounter(d, mount.NewFakeMounter(nil))
		} else {
			d.nodeServer = NewNodeServer(d)
		}
	}

	return d, nil
}

// IdentityServer returns the driver's csi.IdentityServer implementation.
func (d *Driver) IdentityServer() *IdentityServer {
	return d.identityServer
}

// ControllerServer returns the driver's csi.ControllerServer
// implementation, or nil if this instance does not serve it.
func (d *Driver) ControllerServer() *ControllerServer {
	return d.controllerServer
}

// NodeServer returns the driver's csi.NodeServer implementation, or nil if
// this instance does not serve it.
func (d *Driver) NodeServer() *NodeServer {
	return d.nodeServer
}

// ReplicationServer returns the csi-addons replication.Controller
// implementation, or nil if this instance does not serve it.
func (d *Driver) ReplicationServer() *ReplicationServer {
	return d.replicationServer
}

// ReplicationIdentityServer returns the csi-addons identity.Identity
// implementation, or nil if this instance does not serve replication.
func (d *Driver) ReplicationIdentityServer() *ReplicationIdentityServer {
	return d.replicationIdentityServer
}
