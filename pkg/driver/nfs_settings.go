// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// errNFSv4Disabled reports that the target cluster has NFSv4 turned off.
var errNFSv4Disabled = errors.New("the cluster reports v4_enabled false")

// nfsSettingsTTL is how long a cluster's NFS configuration is trusted.
//
// v4_enabled changes approximately never -- it is a deliberate administrative
// act -- so a per-CreateVolume round trip would spend an API call on every
// provision to re-read a constant. A few minutes is short enough that an
// operator who enables NFSv4 does not have to restart the controller to use it.
const nfsSettingsTTL = 5 * time.Minute

// nfsSettingsStalenessNote is appended to every refusal that was decided from
// cached NFS settings.
//
// Without it the message names an action -- "enable NFSv4 on the cluster" --
// and then goes on failing for up to nfsSettingsTTL after the operator has
// done exactly that, because this controller is still answering from the
// reading it took before the change. An operator watching a PVC repeat a
// refusal that names the fix they just applied concludes the driver is broken,
// and reaches for a restart.
//
// Validated 2026-09-15 against a live cluster: NFSv4 was enabled while a
// refusal was in flight, the same PVC kept failing for the rest of the window,
// and then bound on the provisioner's own retry 4m47s after the cache was
// populated -- with no restart and no intervention. So the remedy really is to
// wait, and the only thing missing was saying so.
//
// Built from the constant rather than written out, so the sentence cannot drift
// from the behaviour if the TTL is ever retuned.
var nfsSettingsStalenessNote = fmt.Sprintf(
	"This controller re-reads the cluster's NFS settings every %s, so a change "+
		"made on the cluster just now can take up to that long to take effect here; "+
		"no restart is needed.", nfsSettingsTTL)

// nfsSettingsCache remembers each cluster's NFS configuration.
//
// Keyed per cluster because one controller serves many: a single cached value
// would let cluster A's configuration decide what cluster B may provision.
type nfsSettingsCache struct {
	mu     sync.Mutex
	byHost map[string]nfsSettingsEntry
	now    func() time.Time // injectable so tests need no sleeping
}

type nfsSettingsEntry struct {
	settings qumulo.NFSSettings
	fetched  time.Time
}

func newNFSSettingsCache() *nfsSettingsCache {
	return &nfsSettingsCache{
		byHost: make(map[string]nfsSettingsEntry),
		now:    time.Now,
	}
}

// get returns a cluster's NFS settings, reading through on a miss.
//
// An error is returned to the caller rather than cached: a failed read must not
// be remembered as fact for five minutes.
func (c *nfsSettingsCache) get(
	ctx context.Context,
	client qumulo.ClientInterface,
	host string,
) (qumulo.NFSSettings, error) {
	c.mu.Lock()
	entry, ok := c.byHost[host]
	fresh := ok && c.now().Sub(entry.fetched) < nfsSettingsTTL
	c.mu.Unlock()

	if fresh {
		return entry.settings, nil
	}

	settings, err := client.GetNFSSettings(ctx)
	if err != nil {
		return qumulo.NFSSettings{}, err
	}

	c.mu.Lock()
	c.byHost[host] = nfsSettingsEntry{settings: settings, fetched: c.now()}
	c.mu.Unlock()

	return settings, nil
}

// checkProtocolSupported refuses a protocol the target cluster cannot serve.
//
// Only nfs4.1 needs checking: NFSv3 is always available, and an empty protocol
// lets the kernel negotiate. Without this a v4.1 volume provisions happily and
// then fails in the kernel with "Protocol not supported" -- a pod stuck in
// ContainerCreating behind an error that names nothing an operator can act on.
//
// Failure to read the settings is **fail-open**: the volume is allowed. An API
// blip must not stop provisioning, and the cost of being wrong here is the
// mount error that was the status quo, not data loss.
func (s *ControllerServer) checkProtocolSupported(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
) error {
	if params.Protocol != protocolNFS41 {
		return nil
	}

	settings, err := s.nfsSettings.get(ctx, client, params.Server)
	if err != nil {
		if u := didNotAnswer("reading the cluster's NFS settings", err); u != nil {
			return u
		}
		slog.WarnContext(ctx, "createvolume: could not read the cluster's NFS settings; "+
			"allowing the volume rather than blocking provisioning",
			"server", params.Server, "protocol", params.Protocol, "error", err)
		return nil
	}

	if !settings.V4Enabled {
		return errNFSv4Disabled
	}
	return nil
}

// krb5FlavorEnabled reports the cluster's switch for one Kerberos flavour,
// and whether there is a switch to report at all.
//
// Plain krb5 has no separate flag: krb5_enabled is its flag, and the caller
// has already checked it. Returning known=false for it keeps that one check
// in one place rather than asserting the same field twice.
func krb5FlavorEnabled(settings qumulo.NFSSettings, flavor string) (enabled, known bool) {
	switch flavor {
	case securityFlavorKrb5i:
		return settings.Krb5iEnabled, true
	case securityFlavorKrb5p:
		return settings.Krb5pEnabled, true
	default:
		return false, false
	}
}

// checkAuthSysSupported refuses a sec=sys volume on a cluster with AUTH_SYS off.
//
// auth_sys_enabled was read off the cluster and stored from the day the
// settings struct existed, and nothing ever consulted it. A class asking for
// securityFlavor sys against such a cluster provisions, binds, and then no
// pod can mount it: the export demands nothing, but the cluster refuses the
// AUTH_SYS credential the client presents.
//
// Only an explicit sys is refused. An unset securityFlavor lets the client
// and the cluster negotiate, and on a cluster with AUTH_SYS off that
// negotiation can still land on Kerberos -- refusing there would block a
// provision that works.
//
// Failure to read the settings is fail-open, like checkProtocolSupported and
// unlike the Kerberos check above: this is an availability preflight, not a
// security control. Being wrong here costs the mount error that was the
// status quo; being wrong the other way stops provisioning over an API blip.
func (s *ControllerServer) checkAuthSysSupported(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
) error {
	if params.SecurityFlavor != securityFlavorSys {
		return nil
	}

	settings, err := s.nfsSettings.get(ctx, client, params.Server)
	if err != nil {
		// Fail-open is for a cluster that answers without these settings,
		// not for one that does not answer at all.
		if u := didNotAnswer("reading the cluster's NFS settings", err); u != nil {
			return u
		}
		slog.WarnContext(ctx, "createvolume: could not read the cluster's NFS settings; "+
			"allowing the volume rather than blocking provisioning",
			"server", params.Server, "securityFlavor", params.SecurityFlavor, "error", err)
		return nil
	}

	if !settings.AuthSysEnabled {
		return fmt.Errorf(
			"the cluster reports auth_sys_enabled false, so a pod could not mount this " +
				"volume with sec=sys. Enable AUTH_SYS on the cluster, or set a " +
				"securityFlavor it accepts")
	}
	return nil
}

// checkSecurityFlavorSupported refuses a Kerberos volume the cluster cannot serve.
//
// Two conditions, and the second is the one that bites quietly:
//
//   - krb5_enabled false means the cluster will not accept a Kerberos mount at
//     all, so the export would be created and then be unmountable.
//
//   - idmap_domain unset means NFSv4 has no domain to map principals in, and
//     every authenticated user resolves to nobody. The mount succeeds, the
//     Kerberos handshake succeeds, and then nothing can read its own files --
//     far harder to diagnose from a pod than a refusal here.
//
// Both fail the provision rather than warn. Unlike the protocol preflight
// above, which allows the volume when the settings cannot be read because an
// API blip must not stop provisioning, a security control that might not be in
// force is not something to be optimistic about.
func (s *ControllerServer) checkSecurityFlavorSupported(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
) error {
	if !isKerberos(params.SecurityFlavor) {
		return nil
	}

	settings, err := s.nfsSettings.get(ctx, client, params.Server)
	if err != nil {
		if u := didNotAnswer("reading the cluster's NFS settings", err); u != nil {
			return u
		}
		return fmt.Errorf(
			"could not read the cluster's NFS settings to confirm it accepts %s: %w",
			params.SecurityFlavor, err)
	}

	if !settings.Krb5Enabled {
		return fmt.Errorf(
			"the cluster reports krb5_enabled false, so a %s export would never be "+
				"mountable. Enable Kerberos on the cluster, or use securityFlavor sys",
			params.SecurityFlavor)
	}

	// krb5, krb5i and krb5p are three switches on the cluster, not one.
	// krb5_enabled says a Kerberos mount is possible at all; it says nothing
	// about whether this particular flavour is among the ones the cluster
	// accepts. An export demanding a flavour the cluster has off is created
	// happily and is then unmountable by anyone -- the same outcome
	// krb5_enabled false produces, reached a step later.
	if enabled, known := krb5FlavorEnabled(settings, params.SecurityFlavor); known && !enabled {
		return fmt.Errorf(
			"the cluster reports %s_enabled false, so a %s export would never be "+
				"mountable even though Kerberos itself is on. Enable %s on the cluster, "+
				"or choose a securityFlavor it accepts",
			params.SecurityFlavor, params.SecurityFlavor, params.SecurityFlavor)
	}

	if strings.TrimSpace(settings.IdmapDomain) == "" {
		return fmt.Errorf(
			"the cluster has no idmap_domain set, so every Kerberos principal would map "+
				"to nobody and a pod could not read its own files. Set the cluster's NFSv4 "+
				"idmap domain before using %s", params.SecurityFlavor)
	}

	return nil
}
