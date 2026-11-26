// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"
	"net"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

// VolumeReplicationClass parameter keys understood by the replication
// service. Lower-cased for the same reason the StorageClass keys are: the
// sidecar passes the class's parameters through verbatim.
//
// Nothing here changes the StorageClass. Replication is opted into per volume
// through a VolumeReplication naming one of these classes, and the class
// names the *peer* array: on each Kubernetes cluster the class points at the
// other side.
const (
	// paramReplAPIURL is the REST endpoint of the array the volume lives on.
	// The handle's server is the NFS mount target and, when it is the DNS
	// name the runbook recommends, it floats between arrays at failover -- so
	// it cannot be what the controller dials: after the flip it is the other
	// array, and every RPC on the original side fails to resolve its own
	// volume. Required whenever the handle's server is not an IP literal
	// (checked in resolve, which is where the handle is); optional for an IP
	// literal, where the endpoint is derived as every other RPC derives it.
	paramReplAPIURL = "apiurl"
	// paramTargetServer is the peer array's replication address: a bare host
	// or IPv4 literal, validated like the StorageClass server, because it is
	// what the source array is told to replicate to on port 3712.
	paramTargetServer = "targetserver"
	// paramTargetAPIURL is the peer's REST endpoint. Required whenever
	// targetServer is not an IP literal, for the same floating-name reason
	// as paramReplAPIURL; for an IP literal it defaults to
	// https://<targetServer>:8000.
	paramTargetAPIURL = "targetapiurl"
	// paramTargetBasePath is the directory on the peer the replica lives
	// under. Defaults to the source volume's own parent, so the replica sits
	// at the same path on both arrays and a promoted PV differs from the
	// original in server only.
	paramTargetBasePath = "targetbasepath"
	// paramReplicationMode selects continuous, policy or policy+continuous.
	paramReplicationMode = "replicationmode"
	// paramLinkSnapshotPolicy replicates the volume's csi-<name> policy
	// snapshots along with its data.
	paramLinkSnapshotPolicy = "linksnapshotpolicy"
	// paramSchedulingInterval is read by the csi-addons controller to pace
	// its Info polls; a class without it is polled only at Enable. Lag is
	// graded against it only when a request carries the class's parameters,
	// which csi-addons' Info never does.
	paramSchedulingInterval = "schedulinginterval"
	// paramAllowNonEmptyTarget lets Authorize adopt a peer directory that
	// already holds data, which the first job then overwrites. Off by
	// default: silently overwriting a directory somebody pre-seeded is the
	// wrong way to be helpful.
	paramAllowNonEmptyTarget = "allownonemptytarget"
	// paramReplicationDeletePolicy decides what DeleteVolume does when the
	// volume it is told to delete is the source root of a live relationship.
	//
	// It is named here, with the rest of the class vocabulary, but
	// DeleteVolume cannot read a class: the CSI DeleteVolumeRequest carries
	// a volume ID and a secrets map and nothing else. So this one key is
	// read out of the delete Secret instead -- see replication_guard.go,
	// which says why and what that costs.
	paramReplicationDeletePolicy = "replicationdeletepolicy"
	// paramReplicationScope selects the per-volume or the class-wide form of
	// every replication RPC. A shared-subdirectory class has one covering
	// export and can only be protected as a whole (measured), so its class
	// sets group; a dedicated-export class leaves this alone. See
	// replication_group.go for why this parameter exists at all rather than
	// a VolumeGroupReplication service.
	paramReplicationScope = "replicationscope"
)

// replicationParamPrefix marks parameters the csi-addons controller injects
// rather than the administrator writing them: the secret reference keys.
const replicationParamPrefix = "replication.storage.openshift.io/"

// Values for replicationMode, in the class's vocabulary. Mapped to the
// array's REPLICATION_* constants by replicationParams.arrayMode.
const (
	replicationModeContinuous       = "continuous"
	replicationModePolicy           = "policy"
	replicationModePolicyContinuous = "policy+continuous"
)

// Values for replicationDeletePolicy.
//
// The runbook (section 9) calls these retain and delete, and its delete also
// removes the replica's export and quota and tree-deletes the replica
// directory. The driver implements the first half only and names the
// values for what it actually does: refuse, or disable replication and then
// delete the *source*. Destroying the DR copy as a side effect of a PVC
// delete is the one step here that cannot be undone from Kubernetes, so the
// driver never does it; this is a documented limit. See
// docs/reference/supported-features.md, section 5.
const (
	// replicationDeletePolicyRefuse is the default: a protected volume is
	// not deleted, and the error names the relationship and the remedy.
	replicationDeletePolicyRefuse = "refuse"
	// replicationDeletePolicyDisable removes the relationship on BOTH arrays
	// and then deletes the volume, leaving the replica directory, its export
	// and its quota on the peer. When the peer's end cannot be removed --
	// peer unreachable, no peer token in the Secret, peer refused -- the
	// delete is refused with nothing dismantled, because proceeding would
	// leave the peer an orphaned ENDED end and a replica its inventory then
	// lists as a volume.
	replicationDeletePolicyDisable = "disable"
	// replicationDeletePolicyDisableOrphan is disable plus consent to that
	// orphan: the local end is removed and the volume deleted even when the
	// peer's end cannot be, and the log names what was left where. For a
	// peer that is gone for good, or a delete that must not wait for it.
	replicationDeletePolicyDisableOrphan = "disable-orphan"
)

// Values for replicationScope.
const (
	replicationScopeVolume = "volume"
	replicationScopeGroup  = "group"
)

// defaultSchedulingInterval applies when the class sets none. Five minutes
// matches the csi-addons examples and is generous for a continuous
// relationship, whose recovery point moves every few seconds when idle.
const defaultSchedulingInterval = 5 * time.Minute

// recognizedReplicationParams is every class parameter this service knows.
var recognizedReplicationParams = []string{
	paramReplAPIURL,
	paramTargetServer,
	paramTargetAPIURL,
	paramTargetBasePath,
	paramReplicationMode,
	paramLinkSnapshotPolicy,
	paramSchedulingInterval,
	paramAllowNonEmptyTarget,
	paramReplicationDeletePolicy,
	paramReplicationScope,
}

// replicationParams is the validated VolumeReplicationClass configuration.
type replicationParams struct {
	APIURL             string
	TargetServer       string
	TargetAPIURL       string
	TargetBasePath     string
	ReplicationMode    string
	LinkSnapshotPolicy bool
	SchedulingInterval time.Duration
	AllowNonEmpty      bool
	DeletePolicy       string
	Scope              string
	// ModeExplicit is set when the request named replicationMode at all.
	// Info requests carry no parameters, and a default must not read as drift.
	ModeExplicit bool
}

// arrayMode is the class's mode in the array's vocabulary.
func (p replicationParams) arrayMode() string {
	switch p.ReplicationMode {
	case replicationModePolicy:
		return qumulo.ReplicationModeSnapshotPolicy
	case replicationModePolicyContinuous:
		return qumulo.ReplicationModeSnapshotPolicyContinuous
	default:
		return qumulo.ReplicationModeContinuous
	}
}

// drainsContinuously reports whether a frozen source drains on its own. A
// policy-only relationship moves its recovery point only when a linked policy
// fires, so a planned promote switches it to the combined mode for the drain.
func (p replicationParams) drainsContinuously() bool {
	return p.ReplicationMode != replicationModePolicy
}

// parseReplicationParams validates a VolumeReplicationClass's parameters.
//
// Unknown keys are rejected, as parseVolumeParams rejects them and for the
// same reason: a class asking for policy replication with a misspelled key
// would otherwise replicate continuously and report success.
func parseReplicationParams(params map[string]string) (replicationParams, error) {
	normalized := make(map[string]string, len(params))
	var unknown []string
	for k, v := range params {
		key := strings.ToLower(strings.TrimSpace(k))
		if strings.HasPrefix(key, replicationParamPrefix) || strings.HasPrefix(key, coParamPrefix) {
			continue
		}
		if !slices.Contains(recognizedReplicationParams, key) {
			unknown = append(unknown, key)
			continue
		}
		normalized[key] = strings.TrimSpace(v)
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		recognized := slices.Clone(recognizedReplicationParams)
		slices.Sort(recognized)
		return replicationParams{}, fmt.Errorf("unknown VolumeReplicationClass parameter(s): %s (recognized: %s)",
			strings.Join(unknown, ", "), strings.Join(recognized, ", "))
	}

	parsed := replicationParams{
		APIURL:             normalized[paramReplAPIURL],
		TargetServer:       normalized[paramTargetServer],
		TargetAPIURL:       normalized[paramTargetAPIURL],
		TargetBasePath:     normalized[paramTargetBasePath],
		ReplicationMode:    strings.ToLower(normalized[paramReplicationMode]),
		ModeExplicit:       normalized[paramReplicationMode] != "",
		SchedulingInterval: defaultSchedulingInterval,
		DeletePolicy:       strings.ToLower(normalized[paramReplicationDeletePolicy]),
		Scope:              strings.ToLower(normalized[paramReplicationScope]),
	}

	switch parsed.Scope {
	case "":
		parsed.Scope = replicationScopeVolume
	case replicationScopeVolume, replicationScopeGroup:
	default:
		return replicationParams{}, fmt.Errorf("%s must be %q or %q, got %q", paramReplicationScope,
			replicationScopeVolume, replicationScopeGroup, parsed.Scope)
	}

	switch parsed.DeletePolicy {
	case "":
		parsed.DeletePolicy = replicationDeletePolicyRefuse
	case replicationDeletePolicyRefuse, replicationDeletePolicyDisable, replicationDeletePolicyDisableOrphan:
	default:
		return replicationParams{}, fmt.Errorf("%s must be %q, %q or %q, got %q", paramReplicationDeletePolicy,
			replicationDeletePolicyRefuse, replicationDeletePolicyDisable, replicationDeletePolicyDisableOrphan, parsed.DeletePolicy)
	}

	// targetServer is required by every RPC but GetVolumeReplicationInfo,
	// whose request carries no parameters at all in csi-addons spec v0.2.0;
	// resolve enforces it per RPC rather than this parser for all.
	if err := validateServerHost(parsed.TargetServer); parsed.TargetServer != "" && err != nil {
		return replicationParams{}, fmt.Errorf("%s %q: %w", paramTargetServer, parsed.TargetServer, err)
	}
	if parsed.TargetBasePath != "" && !path.IsAbs(parsed.TargetBasePath) {
		return replicationParams{}, fmt.Errorf("%s must be an absolute path, got %q", paramTargetBasePath, parsed.TargetBasePath)
	}
	for _, key := range []string{paramReplAPIURL, paramTargetAPIURL} {
		if raw := normalized[key]; raw != "" {
			if _, err := url.ParseRequestURI(raw); err != nil || hostOfURL(raw) == "" {
				return replicationParams{}, fmt.Errorf("%s %q is not a URL such as %q", key, raw, "https://qumulo.example.com:8000")
			}
			// The peer's token goes to it; http:// would send it in cleartext.
			if !strings.HasPrefix(strings.ToLower(raw), "https://") {
				return replicationParams{}, fmt.Errorf("%s %q must be an https:// URL: every call to it carries the API token", key, raw)
			}
		}
	}
	if parsed.TargetServer != "" && parsed.TargetAPIURL == "" && !isIPLiteral(parsed.TargetServer) {
		return replicationParams{}, fmt.Errorf("%s %q is a DNS name, so %s is required: %s", paramTargetServer,
			parsed.TargetServer, paramTargetAPIURL, floatingNameReason(parsed.TargetServer))
	}

	switch parsed.ReplicationMode {
	case "":
		parsed.ReplicationMode = replicationModeContinuous
	case replicationModeContinuous, replicationModePolicy, replicationModePolicyContinuous:
	default:
		return replicationParams{}, fmt.Errorf("%s must be %q, %q or %q, got %q", paramReplicationMode,
			replicationModeContinuous, replicationModePolicy, replicationModePolicyContinuous, parsed.ReplicationMode)
	}

	for key, target := range map[string]*bool{
		paramLinkSnapshotPolicy:  &parsed.LinkSnapshotPolicy,
		paramAllowNonEmptyTarget: &parsed.AllowNonEmpty,
	} {
		if raw := normalized[key]; raw != "" {
			value, err := strconv.ParseBool(raw)
			if err != nil {
				return replicationParams{}, fmt.Errorf("%s must be true or false, got %q", key, raw)
			}
			*target = value
		}
	}

	if raw := normalized[paramSchedulingInterval]; raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval <= 0 {
			return replicationParams{}, fmt.Errorf("%s must be a positive duration such as %q, got %q",
				paramSchedulingInterval, "5m", raw)
		}
		parsed.SchedulingInterval = interval
	}

	// Policy snapshots can only cross in a mode that carries them.
	if parsed.LinkSnapshotPolicy && parsed.ReplicationMode == replicationModeContinuous {
		return replicationParams{}, fmt.Errorf("%s requires %s %q or %q: a continuous relationship carries no policy snapshots",
			paramLinkSnapshotPolicy, paramReplicationMode, replicationModePolicy, replicationModePolicyContinuous)
	}

	return parsed, nil
}

// isIPLiteral reports whether addr is an IP address rather than a name. Only
// a name can float between arrays at failover, so only a name needs a fixed
// REST endpoint declared beside it.
func isIPLiteral(addr string) bool {
	return net.ParseIP(strings.TrimSpace(addr)) != nil
}

// floatingNameReason is the one explanation both fixed-endpoint requirements
// give, so an operator reading either error learns the same thing.
func floatingNameReason(name string) string {
	return fmt.Sprintf("a DNS name can float between arrays at failover (the runbook recommends exactly that for the "+
		"StorageClass server, so pods follow the record), and the controller must never dial an array's REST API at a "+
		"name that can flip -- after the flip it is the other array, and every RPC on the original side fails to "+
		"resolve its own volume. Point it at an endpoint that never moves: an IP literal, or a per-array name, e.g. "+
		"https://%s:8000 if %q never moves. Class parameters are immutable, so set it before the class is created",
		name, name)
}

// apiURLFor is the REST endpoint for an array, from an explicit URL or from
// the address alone. The same derivation clientFor uses, so a class that
// names no endpoint dials exactly what every other RPC dials.
func apiURLFor(server, override string) string {
	if base := strings.TrimSuffix(strings.TrimSpace(override), "/"); base != "" {
		return base
	}
	return "https://" + net.JoinHostPort(server, defaultAPIPort)
}

// hostOfURL is the host part of a URL, or "" if it has none. It is the key a
// token is looked up under in the replication Secret: tokens are named by the
// array they were minted on, and the array is named by the stable endpoint
// the controller dials rather than by a floating mount name.
func hostOfURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}
