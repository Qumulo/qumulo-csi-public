// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"context"
	"net/url"
	"strings"
	"time"
)

// Observer is told what a client learned from the calls it made.
//
// It exists so pkg/metrics can export Qumulo-side figures without this
// package importing Prometheus, and without any RPC handler being edited to
// record a metric. A Client's Observer is set once at construction and never
// mutated afterwards, so the one shared implementation is reached
// concurrently by every per-request client -- implementations must be safe
// for that. A nil Observer means nothing is observed and no behaviour
// changes anywhere: every call site is guarded.
//
// Nothing here ever carries a token. The only cluster identifier is the host
// of the client's BaseURL, which is what an operator already sees in a
// StorageClass.
type Observer interface {
	// ObserveRequest is called once per REST round trip, after the response
	// arrives or the transport fails.
	ObserveRequest(ctx context.Context, event RequestEvent)
	// ObserveQuota is called when a call read or wrote a directory quota.
	ObserveQuota(ctx context.Context, event QuotaEvent)
	// ObserveCapacity is called when a call read the cluster's free space.
	ObserveCapacity(ctx context.Context, event CapacityEvent)
	// ObserveReplication is called when a call read replication status, and
	// with Relationships empty when a call read only the array's clock.
	ObserveReplication(ctx context.Context, event ReplicationEvent)
	// ObserveVolumeProtection is called when the driver has established
	// whether one volume is covered by a replication relationship.
	ObserveVolumeProtection(ctx context.Context, event VolumeProtectionEvent)
	// ObserveRestoreStaging is called when the driver found a restore's
	// staging directory that no claim owns, or removed one.
	ObserveRestoreStaging(ctx context.Context, event RestoreStagingEvent)
}

// Restore staging states a RestoreStagingEvent reports.
const (
	// RestoreStagingAbandoned is a staging directory a sweep found with no
	// PersistentVolumeClaim and no PersistentVolume behind it, counted once
	// when it is first seen so.
	RestoreStagingAbandoned = "abandoned"
	// RestoreStagingRemoved is a staging directory the driver removed.
	RestoreStagingRemoved = "removed"
)

// RestoreStagingEvent is one restore staging directory, `.csi-restoring-<name>`,
// that the driver found abandoned or removed. A restore abandoned
// mid-copy leaves one, holding up to the snapshot's whole size under no
// quota, and nothing in Kubernetes shows it.
type RestoreStagingEvent struct {
	Cluster string
	// State is RestoreStagingAbandoned or RestoreStagingRemoved.
	State string
}

// VolumeProtectionEvent says whether one volume was found to be protected by a
// replication relationship at the moment the driver looked.
//
// It exists because the absence of a relationship has no relationship ID to
// hang a series on, and a series that simply stops being reported is not
// something an operator can alert on without knowing in advance which ID to
// expect. csi-addons v0.15.0 leaves a Completed VolumeReplication reading
// Healthy after the relationship is removed on the arrays, so the object's own
// conditions cannot be trusted either -- this is the signal that can be.
type VolumeProtectionEvent struct {
	Cluster string
	// Subject is the caller's name for the volume, from WithSubject. An event
	// with no subject joins to nothing and is dropped.
	Subject string
	// Protected is false when the driver looked for a relationship covering
	// this volume and found none.
	Protected bool
}

// StatusTransport is the Status of a RequestEvent whose request never got an
// HTTP response -- DNS, TCP, TLS or timeout. There is no status code to
// report and the distinction matters: a 401 proves the array answered.
const StatusTransport = "transport"

// Roles a relationship status was read from. Which listing a status came out
// of is the only reliable statement of the role, exactly as the replication
// service determines it.
const (
	RoleSource = "source"
	RoleTarget = "target"
)

// RequestEvent is one REST round trip.
type RequestEvent struct {
	// Cluster is the host (and port) of the client's BaseURL. Never a token.
	Cluster string
	// Method is the HTTP method.
	Method string
	// PathTemplate is the request path with its variable segments replaced,
	// so the label set stays bounded. See PathTemplate.
	PathTemplate string
	// Status is the HTTP status code as a string, or StatusTransport.
	Status string
	// Duration is how long the round trip took.
	Duration time.Duration
	// Reachable is false only when the array did not answer at all. Any
	// HTTP status proves reachability, 401 and 403 included. Counting a
	// rejected credential as unreachable made the gauge -- one per array --
	// flip between 0 and 1 whenever two classes on one array carried
	// different tokens, or one token lacked a privilege, so its alert both
	// misfired and never held for its five minutes. A dead token
	// and a missing privilege have alerts of their own, on the 401
	// and 403 request rates.
	Reachable bool
}

// subjectKey carries the caller's label for whatever this client is acting on
// during one request. See WithSubject.
type subjectKey struct{}

// WithSubject labels every quota this client reads or writes under ctx with a
// caller-supplied string.
//
// The string is opaque here on purpose. This package knows Qumulo file IDs;
// it does not know CSI volume handles, and teaching it would put one
// consumer's vocabulary into the API client every other consumer shares. The
// caller knows both, so the caller says which of its own names this work
// belongs to, and whoever collects the events joins on it.
//
// It used to be unnecessary: a volume handle was <server>/<fileID>/<name>, so
// the file ID on the event WAS the handle's middle field and an exporter
// could split the handle to find it. Handles became path-identified
// and carry no file ID at all, which silently broke that join and
// every per-volume gauge with it.
func WithSubject(ctx context.Context, subject string) context.Context {
	return context.WithValue(ctx, subjectKey{}, subject)
}

// SubjectFrom returns the label WithSubject put on ctx, or "".
func SubjectFrom(ctx context.Context) string {
	subject, _ := ctx.Value(subjectKey{}).(string)
	return subject
}

// QuotaEvent is a directory quota the client read or wrote.
type QuotaEvent struct {
	Cluster string
	// Subject is the caller's name for what this quota belongs to, from
	// WithSubject, or "" outside any labelled request. It is what an
	// exporter joins on; nothing in this package interprets it.
	Subject string
	// FileID is the Qumulo file ID the quota is attached to.
	FileID     string
	LimitBytes uint64
	// UsedBytes is meaningful only when UsedKnown is set: the API omits
	// capacity_usage for a token without the right privilege, and reporting
	// zero used bytes would look like an empty volume rather than no answer.
	UsedBytes uint64
	UsedKnown bool
}

// CapacityEvent is the cluster-wide free space one call reported.
type CapacityEvent struct {
	Cluster string
	FileSystemCapacity
}

// ReplicationEvent is what one replication-status call saw.
//
// An event with no Relationships carries only ArrayTime: replication
// timestamps are stamped by the array, so recovery-point age has to be
// measured against the array's clock and not the controller's.
type ReplicationEvent struct {
	Cluster string
	// Role is RoleSource or RoleTarget, or empty for a clock-only event.
	Role string
	// ArrayTime is the array's own clock, zero when this call did not read it.
	ArrayTime     time.Time
	Relationships []RelationshipStatus
	// Complete is set when Relationships is the array's whole list for this
	// role, so a relationship absent from it no longer exists there. A
	// single-relationship read leaves it false. Measured on the lab cluster
	// (2026-09-14): without this, series for a deleted relationship lived on
	// with an ever-growing recovery-point age.
	Complete bool
	// Removed is the ID of a relationship this call deleted, or that the
	// array said was already gone. Its series are retired on every array
	// and role at once: a relationship shares one ID across both ends, and
	// after the last one on an array pair is deleted no complete listing
	// ever comes back to retire it. An end that in fact survives is
	// re-exported by the next listing of its array.
	Removed string
	// Started is when the request that produced Relationships was sent, or
	// zero when the caller did not record it. A listing that started before
	// a Removed was recorded may predate the delete, so it does not bring
	// that relationship's series back.
	Started time.Time
	// PolicyIntervalOf names a relationship whose snapshot policy's schedule
	// interval this event reports, in PolicyInterval; zero says it has none
	// (not policy-mode any more). Such an event carries nothing else.
	PolicyIntervalOf string
	PolicyInterval   time.Duration
	// Subject is the volume handle the request was made for (WithSubject),
	// or "" outside a labelled request. A listing is every relationship on
	// the array; the subject is what lets the exporter keep only the
	// driver's own.
	Subject string
}

// ClusterLabel renders a base URL as the cluster label used on every metric.
//
// The host, with its port when one is given, and nothing else: a base URL
// carries no credential today, and url.Parse drops any userinfo from Host in
// any case, so this cannot leak one even if a future caller embeds it.
func ClusterLabel(baseURL string) string {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" {
		// Not a URL we can parse. Report something stable rather than
		// nothing, minus anything that could be a credential.
		trimmed := strings.TrimPrefix(strings.TrimPrefix(baseURL, "https://"), "http://")
		if at := strings.LastIndex(trimmed, "@"); at >= 0 {
			trimmed = trimmed[at+1:]
		}
		return strings.TrimSuffix(trimmed, "/")
	}
	return parsed.Host
}

// PathTemplate turns a formatted request path back into the template it was
// built from, so a metric labelled by path has bounded cardinality.
//
// do receives paths that are already formatted -- a file ID, a snapshot ID,
// a percent-encoded path reference or a relationship UUID is substituted in
// before this package ever sees them -- and labelling on those verbatim
// would mint one time series per volume. Every variable the path constants
// in client.go, policy.go and replication.go substitute is one of four
// shapes, and each is recognised here.
func PathTemplate(path string) string {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}

	segments := strings.Split(path, "/")
	for i, segment := range segments {
		if isVariableSegment(segment) {
			segments[i] = "{id}"
		}
	}
	return strings.Join(segments, "/")
}

// isVariableSegment reports whether one path segment is a substituted value
// rather than a fixed part of the API surface.
func isVariableSegment(segment string) bool {
	switch {
	case segment == "":
		// A trailing slash, which several collection paths carry and which
		// is part of the path rather than a value.
		return false
	case strings.ContainsRune(segment, '%'):
		// A percent-encoded path reference, e.g. %2Fcsi%2Fvolumes. No fixed
		// segment in this API contains a percent sign.
		return true
	case isAllDigits(segment):
		// File, quota, snapshot and policy IDs.
		return true
	case isUUID(segment):
		// Replication relationship IDs.
		return true
	default:
		return false
	}
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// isUUID reports whether s has the 8-4-4-4-12 hexadecimal shape.
func isUUID(s string) bool {
	const uuidLength = 36
	if len(s) != uuidLength {
		return false
	}
	for i, r := range s {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			isHex := (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}
