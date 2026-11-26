// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// Multi-tenant exports: tenantID and allowedClients.
//
// A Qumulo tenant is a network namespace. An export attached to tenant N is
// reachable only on N's networks, and allowedClients narrows that further to a
// list of addresses. Together they are the per-volume access control the
// shared-subdirectory model cannot offer.
//
// Both parameters require provisioningModel: dedicated-export, and the driver
// refuses them otherwise rather than ignoring them. See requireDedicatedExport
// below for why that refusal is the whole point.

// maxAllowedClients bounds the list before it becomes an API payload.
//
// The cluster imposes no documented limit. This one exists so a malformed
// parameter -- a whole subnet inventory pasted in by accident -- fails with a
// clear message instead of a timed-out REST call.
const maxAllowedClients = 256

// parseAllowedClients splits and validates a comma-separated IP/CIDR list.
//
// Every entry must be an IP address or a CIDR block. Hostnames are refused even
// though Qumulo accepts them: a name resolves at mount time on the cluster, so
// a typo becomes an intermittent access failure long after provisioning, and an
// access control that depends on DNS is not one you can reason about.
//
// The returned slice is in the order written, with duplicates preserved: it is
// echoed back in the PV's volume context, and reordering it would make the PV
// disagree with the StorageClass for no benefit.
func parseAllowedClients(raw string) ([]string, error) {
	fields := strings.Split(raw, ",")
	clients := make([]string, 0, len(fields))

	for _, field := range fields {
		entry := strings.TrimSpace(field)
		if entry == "" {
			// A trailing or doubled comma. Silently dropping it would also
			// silently accept ",,," as an empty allowlist, which reads as
			// "allow nothing" and behaves as "allow everything".
			return nil, fmt.Errorf("contains an empty entry: check for a stray or doubled comma in %q", raw)
		}

		if strings.Contains(entry, "/") {
			if _, _, err := net.ParseCIDR(entry); err != nil {
				return nil, fmt.Errorf("%q is not a valid CIDR block: %w", entry, err)
			}
		} else if net.ParseIP(entry) == nil {
			return nil, fmt.Errorf(
				"%q is neither an IP address nor a CIDR block. Host names are not accepted, "+
					"because an allowlist that resolves at mount time fails intermittently "+
					"rather than at provision time", entry)
		}
		clients = append(clients, entry)
	}

	if len(clients) > maxAllowedClients {
		return nil, fmt.Errorf("lists %d entries; the limit is %d", len(clients), maxAllowedClients)
	}
	return clients, nil
}

// validateTenantID checks the shape of a tenant reference.
//
// Two forms are accepted, because administrators have both to hand: the integer
// id the REST API uses, and the human name shown in the Web UI. A numeric value
// must parse as a positive integer -- tenant 0 does not exist, and the cluster
// rejects it with a less obvious message than this one.
//
// Existence is NOT checked here. That needs a cluster round trip, so it happens
// in resolveTenantID at provision time; this is the cheap check that runs
// during parameter parsing, before any credential is available.
func validateTenantID(tenant string) error {
	if strings.TrimSpace(tenant) != tenant {
		return fmt.Errorf("must not have leading or trailing whitespace")
	}
	if tenant == "" {
		return fmt.Errorf("must not be empty")
	}

	// Numeric-looking values are held to being real ids. Anything else is
	// taken as a name and resolved against the cluster later.
	if id, err := strconv.Atoi(tenant); err == nil {
		if id <= 0 {
			return fmt.Errorf("must be a positive tenant id, got %d", id)
		}
		return nil
	}

	// A name. Reject the characters that would make the eventual lookup
	// ambiguous rather than failing with "no such tenant".
	if strings.ContainsAny(tenant, `,/\"`) {
		return fmt.Errorf("must be a tenant id or a tenant name, and %q contains a separator", tenant)
	}
	return nil
}

// resolveTenantID turns the StorageClass's tenant reference into the integer id
// the export API takes.
//
// Existence is the cluster's business either way: a numeric value it does not
// know is passed straight through, because the cluster refuses it clearly
// (tenant_does_not_exist_error) and duplicating that check here would buy
// nothing. What the cluster does NOT refuse is an export placed in a tenant
// that exists with NFS switched off -- that one is accepted, and then serves
// nothing, so the PVC binds and the mount hangs.
//
// So the listing is consulted for both spellings, and for one property only.
// It used to be read for a name and skipped for an id, which left the same
// tenant accepted as "7" and refused as "smb-only" -- the driver disagreeing
// with itself over which characters were typed. A listing that cannot be made
// refuses nothing: for an id the value is used as written, since existence was
// never this function's question.
//
// The listing is shared with checkTenantNotRequired and reused for
// tenantListingTTL per cluster (listTenants). A name or id the held
// listing does not have is read again before it is refused, so a tenant added
// since is found at once; a rename, or NFS turned off on a tenant, is seen
// within the TTL.
func (s *ControllerServer) resolveTenantID(
	ctx context.Context,
	client qumulo.ClientInterface,
	tenant string,
) (int, error) {
	if tenant == "" {
		// No tenant asked for. Send no tenant_id and let the cluster apply its
		// own default, which is what every export created before this
		// parameter existed already got.
		return 0, nil
	}

	numeric, isNumeric := 0, false
	if id, err := strconv.Atoi(tenant); err == nil {
		numeric, isNumeric = id, true
	}

	tenants, cached, err := listTenants(ctx, client)
	if err == nil && cached && !tenantListed(tenants, tenant, numeric, isNumeric) {
		// Possibly added since the listing was read.
		forgetTenants(client)
		tenants, _, err = listTenants(ctx, client)
	}
	if err != nil {
		// An id needs no lookup to be usable: the cluster is the authority on
		// whether it exists, and it says so clearly. The listing is consulted
		// only to catch a tenant that exists with NFS off, so a listing this
		// call could not make is not a reason to refuse the volume.
		// An id is used as written when the array answered without a
		// listing, but not when it did not answer: the next call would only
		// wait out the same timeout again.
		if isNumeric && !arrayGaveNoAnswer(err) {
			slog.WarnContext(ctx, "createvolume: could not list tenants to check tenantID; "+
				"using it as written", "tenantID", numeric, "error", err)
			return numeric, nil
		}
		return 0, &tenantListingError{tenant: tenant, err: err}
	}

	for _, t := range tenants {
		if isNumeric {
			if t.ID != numeric {
				continue
			}
		} else if !strings.EqualFold(t.Name, tenant) {
			continue
		}
		if !t.NFSEnabled {
			// The export would be created and then serve nothing. Failing here
			// turns a silent mount hang into a Pending PVC with the reason.
			return 0, fmt.Errorf(
				"tenant %q (id %d) has NFS disabled, so an export placed in it would "+
					"never be mountable; enable NFS on that tenant or choose another",
				t.Name, t.ID)
		}
		return t.ID, nil
	}

	// An id that matched nothing is still passed through: the cluster is the
	// authority on existence and refuses an unknown tenant itself, with a
	// message that names it. Only a NAME has to resolve here, because a name
	// is not something the export API can be handed.
	if isNumeric {
		return numeric, nil
	}

	names := make([]string, 0, len(tenants))
	for _, t := range tenants {
		names = append(names, fmt.Sprintf("%s (id %d)", t.Name, t.ID))
	}
	return 0, fmt.Errorf("no tenant named %q on this cluster; it has: %s",
		tenant, strings.Join(names, ", "))
}

// tenantListed reports whether tenants holds the tenant a class names.
func tenantListed(tenants []qumulo.TenantInfo, tenant string, numeric int, isNumeric bool) bool {
	return slices.ContainsFunc(tenants, func(t qumulo.TenantInfo) bool {
		if isNumeric {
			return t.ID == numeric
		}
		return strings.EqualFold(t.Name, tenant)
	})
}

// tenantListings maps an endpoint to the last tenant listing read from it,
// for listTenants.
var tenantListings sync.Map

// tenantListingTTL is how long a tenant listing is reused. Tenants change
// rarely, and a dedicated-export CreateVolume read the whole table once or
// twice per call.
const tenantListingTTL = 5 * time.Minute

// tenantListing is one held listing and when it was read. Stored by pointer:
// the slice makes the struct uncomparable, and CompareAndDelete panics on an
// uncomparable value.
type tenantListing struct {
	tenants []qumulo.TenantInfo
	at      time.Time
}

// listTenants returns the cluster's tenants, from a listing at most
// tenantListingTTL old when one is held; cached says it was.
//
// A hint like the others (ARCHITECTURE.md rule 0): only a successful listing is
// kept, it is never extended by use, and losing it costs one read. It is
// keyed by endpoint and not by token, because tenants are the cluster's
// configuration and the listing only feeds the pre-checks: the export itself
// is still created with the request's own credential, which the array
// authorizes. A client that names no endpoint lists every time.
func listTenants(ctx context.Context, client qumulo.ClientInterface) ([]qumulo.TenantInfo, bool, error) {
	named, ok := client.(interface{ Endpoint() string })
	if !ok {
		tenants, err := client.ListTenants(ctx)
		return tenants, false, err
	}
	key := named.Endpoint()
	if v, ok := tenantListings.Load(key); ok {
		if held := v.(*tenantListing); time.Since(held.at) < tenantListingTTL {
			return held.tenants, true, nil
		}
		tenantListings.CompareAndDelete(key, v)
	}
	tenants, err := client.ListTenants(ctx)
	if err == nil {
		tenantListings.Store(key, &tenantListing{tenants: tenants, at: time.Now()})
		notePackageHintStored()
	}
	return tenants, false, err
}

// forgetTenants drops the listing held for client's cluster, so the next
// listTenants reads afresh.
func forgetTenants(client qumulo.ClientInterface) {
	if named, ok := client.(interface{ Endpoint() string }); ok {
		tenantListings.Delete(named.Endpoint())
	}
}

// tenantListingError is a tenant name that could not be resolved because the
// tenant listing itself failed. It is kept apart from the refusals above
// because it says nothing about the class: a timeout or an array-side error
// here is an outage, and reporting it as InvalidArgument sent operators off to
// fix a class that was correct.
type tenantListingError struct {
	tenant string
	err    error
}

func (e *tenantListingError) Error() string {
	return fmt.Sprintf("listing tenants to resolve %q: %v", e.tenant, e.err)
}

func (e *tenantListingError) Unwrap() error { return e.err }

// tenantResolutionStatus maps a resolveTenantID failure to a gRPC status.
//
// A listing that did not answer, answered with a server error, or was
// throttled (429) or timed out server-side (408), is Unavailable, so the
// provisioner retries rather than recording a final failure; a 429 reported
// as InvalidArgument told the operator to fix a correct class.
// Anything else -- no tenant by that name, NFS off, or the array refusing the
// listing outright -- is a statement about the class and stays
// InvalidArgument. Not ResourceExhausted for a 429: the alerts read that code
// as a restore over its cap.
func tenantResolutionStatus(err error) error {
	var listing *tenantListingError
	if errors.As(err, &listing) {
		var apiErr *qumulo.APIError
		_, misconfigured := misconfiguredEndpoint(listing.err)
		if (arrayGaveNoAnswer(listing.err) && !misconfigured) ||
			(errors.As(listing.err, &apiErr) && transientStatus(apiErr.StatusCode)) {
			return status.Errorf(codes.Unavailable, "%s: %v", paramTenantID, err)
		}
	}
	return status.Errorf(codes.InvalidArgument, "%s: %v", paramTenantID, err)
}

// transientStatus reports whether an HTTP status says "try again later"
// rather than anything about the request: a server error, a throttle, or a
// server-side timeout.
func transientStatus(code int) bool {
	return code >= 500 || code == http.StatusTooManyRequests || code == http.StatusRequestTimeout
}

// checkTenantNotRequired refuses a dedicated-export class that names no tenant
// on a cluster that has more than one.
//
// Such a cluster refuses an export with no tenant_id
// (empty_tenant_in_multitenant_environment_error), and it refuses it in
// ensureExport, after the directory and quota already exist. A PVC deleted
// while still Pending then leaves both behind, because the provisioner never
// calls DeleteVolume for a volume it never got back. Asking here fails the
// class before anything is created.
//
// It costs one GET per provision for these classes only. A class that names a
// tenant is resolved by resolveTenantID instead, and a shared-subdirectory
// class creates no export, so neither is checked.
//
// Like resolveTenantID, a listing that cannot be made refuses nothing: an
// older cluster may not serve the tenants API at all, and ensureExport still
// maps the cluster's own refusal to the same clear message.
func (s *ControllerServer) checkTenantNotRequired(
	ctx context.Context,
	client qumulo.ClientInterface,
	params volumeParams,
) error {
	if params.TenantID != "" || !params.DedicatedExport() {
		return nil
	}

	tenants, _, err := listTenants(ctx, client)
	if err != nil {
		if u := didNotAnswer("listing tenants", err); u != nil {
			return u
		}
		slog.WarnContext(ctx, "createvolume: could not list tenants to check that the class "+
			"needs no tenantID; continuing", "error", err)
		return nil
	}
	if len(tenants) <= 1 {
		return nil
	}

	names := make([]string, 0, len(tenants))
	for _, t := range tenants {
		names = append(names, fmt.Sprintf("%s (id %d)", t.Name, t.ID))
	}
	return fmt.Errorf(
		"this cluster has more than one tenant, so a %s=%s class must set %s "+
			"(a tenant id or name) to say which one the export belongs to; it has: %s",
		paramProvisioningModel, provisioningModelDedicatedExport, paramTenantID,
		strings.Join(names, ", "))
}

// requireDedicatedExport refuses tenantID and allowedClients on a class that
// creates no export of its own.
//
// This is the load-bearing check of the whole feature. Under
// shared-subdirectory a volume is a directory beneath one export an
// administrator made: there is no per-volume export, so there is nothing to
// attach a tenant or a host list to. Accepting the parameters and dropping them
// would hand an operator a PVC they believe is restricted to three addresses
// while it is in fact reachable by everyone the parent export admits.
//
// A silently ignored access control is worse than an unimplemented one, so this
// fails the provision instead.
func validateExportScoping(params volumeParams) error {
	if params.DedicatedExport() {
		return nil
	}

	var set []string
	if params.TenantID != "" {
		set = append(set, paramTenantID)
	}
	if len(params.AllowedClients) > 0 {
		set = append(set, paramAllowedClients)
	}
	if len(set) == 0 {
		return nil
	}

	return fmt.Errorf(
		"%s requires %s=%s: a %s volume is a directory under an export somebody else "+
			"owns, so there is no per-volume export to scope or restrict. Set "+
			"%s=%s on this class, or remove %s",
		strings.Join(set, " and "), paramProvisioningModel, provisioningModelDedicatedExport,
		provisioningModelSharedSubdirectory,
		paramProvisioningModel, provisioningModelDedicatedExport, strings.Join(set, " and "))
}

// openExportRefusal refuses a dedicated-export class whose exports would be
// open: no allowedClients, an AUTH_SYS flavour, and no allowAnyClient.
//
// Such an export is created with no host restriction, NFS_MAP_NONE, no
// privileged port and no authentication, so any client that reaches the array
// can mount every volume of the class read-write as root. The chart refuses
// the class at install (requireAllowedClients); this is the same check for a
// class made outside the chart. A Kerberos class is not open: its
// export requires the flavour.
//
// CreateVolume only. Existing volumes and their exports are left alone, and
// GetCapacity and the inventory still read an open class.
func openExportRefusal(params volumeParams) error {
	if !params.DedicatedExport() || len(params.AllowedClients) > 0 || params.AllowAnyClient ||
		strings.HasPrefix(strings.ToLower(params.SecurityFlavor), "krb5") {
		return nil
	}
	// Spelled as a StorageClass is written, not as the keys are folded.
	return errors.New("a provisioningModel: dedicated-export class with no allowedClients gives every " +
		"volume an export any NFS client that reaches the array can mount, as root, from any port, " +
		"with no authentication; set allowedClients to your nodes' addresses or CIDRs, a krb5 " +
		"securityFlavor, or allowAnyClient: \"true\" if an open export is intended. A StorageClass's " +
		"parameters cannot be changed, so re-create the class")
}

// logExportScoping records the tenant and allowlist actually committed.
//
// Deliberately at Info: this is the audit trail for an access control decision,
// and the question "what was this volume restricted to when it was created?"
// has no other answer once the StorageClass has been edited.
func logExportScoping(ctx context.Context, name string, tenantID int, params volumeParams) {
	if params.TenantID == "" && len(params.AllowedClients) == 0 && !params.AllowAnyClient {
		return
	}

	attrs := []any{"volume", name}
	if params.TenantID != "" {
		// Both: the id is what was sent, the reference is what was written.
		attrs = append(attrs, "tenantID", tenantID, "tenantRef", params.TenantID)
	}
	if len(params.AllowedClients) > 0 {
		attrs = append(attrs, "allowedClients", params.AllowedClients)
	} else {
		attrs = append(attrs, "allowedClients", "unrestricted")
	}
	if params.AllowAnyClient && len(params.AllowedClients) == 0 {
		// The audit trail for an export left open on purpose.
		attrs = append(attrs, "allowAnyClient", true)
	}

	slog.InfoContext(ctx, "createvolume: committing export scoping", attrs...)
}

// checkServerServesTenant refuses a class whose server is not an address the
// chosen tenant answers on.
//
// SIM-08. A class naming tenant 3 with a server on tenant 1's network
// provisions cleanly: the export really is created in tenant 3, the PVC
// really does Bind, and the cluster is right to do both. The pod then fails
// with `mount.nfs: ... No such file or directory`, because tenant 3's export
// is invisible on tenant 1's network. That is the cluster enforcing isolation
// correctly -- and it is a bind-then-FailedMount, the pattern a customer
// reads as "the storage lied to me": the PVC is green and the workload never
// starts.
//
// So the misconfiguration is caught before anything is created, where the
// message can name the addresses that would have worked.
//
// # What this deliberately does not do
//
// It refuses only on positive evidence. Three things make the tenant's
// addresses unknown rather than mismatched, and each one proceeds with a
// warning instead:
//
//   - The network configuration cannot be read. That is the same judgement
//     resolveTenantID already makes about the tenant listing: a lookup this
//     call could not perform is not a reason to refuse a volume.
//   - The tenant has networks whose address assignment this driver does not
//     decode (ListNetworks documents the one form it does). Refusing a
//     correctly configured cluster because of a scheme this code has not met
//     would be far worse than the bug being fixed.
//   - server is a DNS name. Resolving it here would put the controller's own
//     resolver between a StorageClass and a provision, and a name that
//     resolves differently inside the cluster than in this pod would refuse
//     volumes that mount perfectly well.
func (s *ControllerServer) checkServerServesTenant(
	ctx context.Context,
	client qumulo.ClientInterface,
	server string,
	tenantID int,
) error {
	if tenantID == 0 || server == "" {
		return nil
	}
	if !isIPLiteral(server) {
		slog.DebugContext(ctx, "createvolume: not checking the tenant's networks; "+
			"the class names the cluster by name, which this check does not resolve",
			"server", server, "tenantID", tenantID)
		return nil
	}

	networks, err := client.ListNetworks(ctx)
	if err != nil {
		if u := didNotAnswer("reading the cluster's network configuration", err); u != nil {
			return u
		}
		slog.WarnContext(ctx, "createvolume: could not read the cluster's network configuration; "+
			"not checking that the class's server is on the tenant's network",
			"server", server, "tenantID", tenantID, "error", err)
		return nil
	}

	var known []string
	undecoded := map[string]bool{}
	// otherTenant is the tenant whose floating addresses include server, when
	// that tenant is not this one: the one piece of positive evidence this
	// check can have that the export would be unmountable from server.
	otherTenant := 0
	for _, n := range networks {
		if n.TenantID != tenantID {
			for _, spec := range n.FloatingAddresses {
				if addressSpecContains(spec, server) {
					otherTenant = n.TenantID
				}
			}
			continue
		}
		if len(n.FloatingAddresses) == 0 {
			undecoded[n.AddressType] = true
			continue
		}
		known = append(known, n.FloatingAddresses...)
	}

	for _, spec := range known {
		if addressSpecContains(spec, server) {
			return nil
		}
	}

	// Nothing matched, but nothing was knowable either.
	if len(known) == 0 {
		slog.WarnContext(ctx, "createvolume: the tenant's addresses could not be determined, so the "+
			"class's server was not checked against them",
			"server", server, "tenantID", tenantID,
			"addressTypesNotDecoded", strings.Join(sortedKeys(undecoded), ", "))
		return nil
	}

	// Not among this tenant's floating addresses, but not among another
	// tenant's either. This check reads only floating ranges: a node's own
	// address -- which a tenant owning the network answers on, DHCP or
	// otherwise -- is invisible to it. So an address it cannot place is not
	// evidence of anything, and refusing it refused classes that mount fine.
	// The documented rule is to refuse only on positive evidence.
	if otherTenant == 0 {
		slog.WarnContext(ctx, "createvolume: the class's server is not one of the tenant's floating "+
			"addresses, nor any other tenant's; it may be a node's own address, which this check "+
			"cannot see, so it was not refused",
			"server", server, "tenantID", tenantID, "tenantFloatingAddresses", strings.Join(known, ", "))
		return nil
	}

	// Some of the tenant's networks were decoded and some were not, so the
	// server may be on one this code cannot see. Saying so is the difference
	// between a message an operator can act on and one that is simply wrong.
	caveat := ""
	if len(undecoded) > 0 {
		caveat = fmt.Sprintf(" (this tenant also has networks using %s addressing, "+
			"whose addresses this driver does not read, so they are not listed here)",
			strings.Join(sortedKeys(undecoded), " and "))
	}
	return fmt.Errorf(
		"server %q is a floating address of tenant %d, not of tenant %d, so an export in tenant %d "+
			"would be created and then be unmountable from that address; use one of: %s%s",
		server, otherTenant, tenantID, tenantID, strings.Join(known, ", "), caveat)
}

// addressSpecContains reports whether a floating-address spec covers addr.
//
// A spec is one address ("10.0.0.5") or a range, and Qumulo writes a range
// both ways: "10.0.0.5-10.0.0.9" and the abbreviated "10.0.0.5-9". Both are
// accepted, and anything else is treated as covering nothing -- this decides
// whether to refuse a volume, so an unparsed spec must not be able to make
// the answer "yes" by accident.
func addressSpecContains(spec, addr string) bool {
	target := net.ParseIP(strings.TrimSpace(addr))
	if target == nil {
		return false
	}

	spec = strings.TrimSpace(spec)
	low, high, isRange := strings.Cut(spec, "-")

	start := net.ParseIP(strings.TrimSpace(low))
	if start == nil {
		return false
	}
	if !isRange {
		return start.Equal(target)
	}

	end := net.ParseIP(strings.TrimSpace(high))
	if end == nil {
		// The abbreviated form: the last octet only. Rebuilt against the
		// start address rather than parsed alone, because "9" is not an IP.
		octets := strings.Split(strings.TrimSpace(low), ".")
		if len(octets) != 4 {
			return false
		}
		octets[3] = strings.TrimSpace(high)
		if end = net.ParseIP(strings.Join(octets, ".")); end == nil {
			return false
		}
	}

	// Compared as 16-byte forms so a v4 address written either way sorts
	// against a v4 range correctly.
	t, s, e := target.To16(), start.To16(), end.To16()
	if t == nil || s == nil || e == nil {
		return false
	}
	return bytes.Compare(t, s) >= 0 && bytes.Compare(t, e) <= 0
}

// sortedKeys returns a map's keys in order, so a message built from a set
// reads the same way twice.
func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		if k == "" {
			k = "an unnamed"
		}
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
