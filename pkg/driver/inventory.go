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
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// StorageClassLister enumerates the StorageClasses that name this driver.
//
// This is the one component ListVolumes and ControllerGetVolume need that no
// other RPC does, and it is the reason those two are opt-in. Every other
// controller RPC is *told* which cluster to act on -- by its secrets map, or
// by its parameters, or by a volume ID. These two are told nothing, so the
// only way to answer them is to discover clusters from the Kubernetes API.
// Rule 0 in ARCHITECTURE.md draws the line there deliberately.
//
// Read-only and un-cached on purpose. Rule 4 forbids watching the API, so
// there is no informer and no lister behind this: it is a plain List per call
// of a handful of objects. That also means a new class or a rotated Secret
// takes effect on the next call rather than after a restart.
type StorageClassLister interface {
	// ListStorageClasses returns every StorageClass whose provisioner equals
	// provisioner, in no particular order. Callers sort.
	ListStorageClasses(ctx context.Context, provisioner string) ([]StorageClassInfo, error)
}

// VolumeHandleLister reads the volume handles of the PersistentVolumes that
// name this driver.
//
// ListVolumes needs it for one decision: which handle to list a directory
// under when two classes reach it -- a DNS-named class with an apiUrl and an
// address class for the same array. Both handles decode to the same
// directory, only one of them is the PV's, and nothing on the array says
// which. Behind controller.inventory.enabled like StorageClassLister,
// and the same shape: a plain paged List per call, never a watch, nothing
// cached (Rules 0 and 4 in ARCHITECTURE.md).
type VolumeHandleLister interface {
	// ListVolumeHandles returns the spec.csi.volumeHandle of every
	// PersistentVolume whose spec.csi.driver equals driverName.
	ListVolumeHandles(ctx context.Context, driverName string) ([]string, error)
}

// VolumeRef is one of this driver's PersistentVolumes: its handle, and the
// StorageClass it names.
type VolumeRef struct {
	Handle       string
	StorageClass string
}

// VolumeRefLister is a VolumeHandleLister that also says which StorageClass
// each PV names. ListVolumes uses it, when the lister has it, to list a DR
// copy's PV: its handle names an array no class walks, and its class's
// Secret is where that array's token is found.
type VolumeRefLister interface {
	ListVolumeRefs(ctx context.Context, driverName string) ([]VolumeRef, error)
}

// PersistentVolumeInfo is one of this driver's PersistentVolumes, as much of
// it as DeleteVolume needs to tell whether another PV still uses the
// directory a delete would remove. Plain strings, for the same reason
// as StorageClassInfo: pkg/driver does not import Kubernetes API types.
type PersistentVolumeInfo struct {
	// Name is the PV's object name.
	Name string
	// Handle is its spec.csi.volumeHandle, verbatim.
	Handle string
	// Phase is its status.phase: "Bound", "Released", "Available",
	// "Pending", "Failed", or "" before the PV controller has set one.
	Phase string
	// Deleting is set when the PV has a deletion timestamp.
	Deleting bool
	// ClaimNamespace and ClaimName are its spec.claimRef, "" when it has none.
	ClaimNamespace string
	ClaimName      string
	// ReclaimPolicy is its spec.persistentVolumeReclaimPolicy: "Delete",
	// "Retain", "Recycle", or "" when the lister did not read it.
	ReclaimPolicy string
	// MirroredFrom is its AnnotationMirroredFrom annotation, "" when unset.
	MirroredFrom string
	// DeleteDirectory is its AnnotationDeleteDirectory annotation, verbatim,
	// "" when unset.
	DeleteDirectory string
	// CapacityBytes is its spec.capacity.storage in bytes, 0 when the lister
	// did not read it or the PV carries none. It is what Kubernetes believes
	// the volume's size to be once an expand has completed, which is how a
	// controller replica tells its own pre-expand quota reading from a
	// current one.
	CapacityBytes int64
}

// Annotations DeleteVolume reads on the PersistentVolume being deleted to
// tell whether it owns its directory.
const (
	// AnnotationDeleteDirectory is the opt-in an administrator adds to a
	// hand-written or imported PV to let Kubernetes delete its directory.
	// Its value must be the directory's real path on the array -- the path
	// in the volume handle, not the NFS export path.
	AnnotationDeleteDirectory = "csi.qumulo.com/delete-directory"
	// AnnotationMirroredFrom names the PV a DR-side PV was mirrored from.
	// cmd/qumulo-csi-repoint writes it, under the same key.
	AnnotationMirroredFrom = "csi.qumulo.com/mirrored-from"
)

// PersistentVolumeLister reads this driver's PersistentVolumes for
// DeleteVolume's ownership check. Unlike VolumeHandleLister it is not
// behind inventory: a controller wires it whenever it runs in a cluster,
// because it protects a delete rather than discovering a cluster. A paged
// List per call, never a watch, nothing cached (Rules 0 and 4 in ARCHITECTURE.md).
type PersistentVolumeLister interface {
	// ListPersistentVolumes returns every PersistentVolume whose
	// spec.csi.driver equals driverName.
	ListPersistentVolumes(ctx context.Context, driverName string) ([]PersistentVolumeInfo, error)
}

// PersistentVolumeClaimLister reads the UIDs of the cluster's
// PersistentVolumeClaims, so a restore's staging directory, named
// `.csi-restoring-pvc-<claim UID>`, is removed only once its claim is gone:
// by DeleteVolume when the volume's directory is already gone, and
// by CreateVolume's sweep of a store root. Like PersistentVolumeLister it is
// not behind inventory -- it discovers no cluster and names no Secret -- and
// a controller wires it whenever it runs in a cluster. A paged List per call,
// never a watch, nothing cached (Rules 0 and 4 in ARCHITECTURE.md).
type PersistentVolumeClaimLister interface {
	// ListPersistentVolumeClaimUIDs returns the UID of every
	// PersistentVolumeClaim in every namespace. A failed page fails the
	// whole list.
	ListPersistentVolumeClaimUIDs(ctx context.Context) ([]string, error)
}

// StorageClassInfo is the part of a StorageClass this driver cares about.
//
// Deliberately not a client-go type: pkg/driver must not import Kubernetes API
// packages, which is what lets every test here run against a literal.
type StorageClassInfo struct {
	// Name is the class's object name, and the tie-breaker when two classes
	// name one cluster. See clusterForVolume.
	Name string
	// Parameters is the class's parameter map exactly as written, including
	// the csi.storage.k8s.io/ secret-reference keys.
	Parameters map[string]string
}

// managedClass is one StorageClass parsed into what a listing needs: where its
// volumes live, and enough to build a client when one is actually wanted.
//
// It deliberately holds no client. Building one costs a Secret read, and
// ControllerGetVolume needs exactly one class out of however many the cluster
// has -- so resolving eagerly would read every Secret in the release to answer
// a question about a single volume. Clients are built by clientForClass, at
// the point of use.
type managedClass struct {
	name   string
	params volumeParams
	// raw is the class as it came back, kept for its secret-reference keys.
	raw StorageClassInfo
	// root is the directory this class's volumes are created in, domain
	// included. Held separately because it is the sort key's second half and
	// is read on every candidate.
	root string
}

// errInventoryDisabled reports that this controller cannot discover clusters.
var errInventoryDisabled = errors.New(
	"this controller is not configured to enumerate volumes: the RPC names no cluster and no " +
		"credential, so answering it requires reading StorageClasses from the Kubernetes API. " +
		"Set controller.inventory.enabled and give the driver -enable-inventory")

// resolveManagedClasses parses the cluster's StorageClasses for this driver.
//
// Failures are split three ways, and the split is the whole point:
//
//   - No lister at all: the deployment did not opt in. FailedPrecondition,
//     because no retry can change it.
//   - A class this driver cannot parse: that class is misconfigured. Skipped
//     with a warning, so one bad class does not hide every other class's
//     volumes. The count is returned so a caller can tell "no volumes" from
//     "every class is broken".
//   - The Kubernetes API itself failing: nothing can be enumerated, so this is
//     returned.
//
// Credentials are not resolved here -- see managedClass.
//
// A continuing page of a ListVolumes walk reuses the class list the walk's
// first page read; every other caller reads afresh. Only the classes
// are reused, never a Secret or a client.
func (s *ControllerServer) resolveManagedClasses(
	ctx context.Context,
	continuing bool,
) (classes []managedClass, skipped int, err error) {
	if s.driver.StorageClassLister == nil {
		return nil, 0, status.Error(codes.FailedPrecondition, errInventoryDisabled.Error())
	}
	if s.driver.SecretReader == nil {
		return nil, 0, status.Error(codes.FailedPrecondition, errInventoryDisabled.Error())
	}

	listed, err := s.classWalk.get(continuing, func() ([]StorageClassInfo, error) {
		return s.driver.StorageClassLister.ListStorageClasses(ctx, s.driver.Name)
	})
	if err != nil {
		return nil, 0, status.Errorf(codes.Internal,
			"listing StorageClasses for provisioner %q: %v", s.driver.Name, err)
	}
	// Sorted below, so a copy: the cached list is shared with later pages.
	found := slices.Clone(listed)

	// Sorted by name so the whole operation is deterministic: it fixes the
	// order clusters are contacted in, and it is the tie-breaker when two
	// classes name one server (see clusterForVolume).
	slices.SortFunc(found, func(a, b StorageClassInfo) int {
		return strings.Compare(a.Name, b.Name)
	})

	for _, class := range found {
		parsed, parseErr := parseVolumeParams(class.Parameters)
		if parseErr != nil {
			slog.WarnContext(ctx, "inventory: skipping a StorageClass this driver cannot parse; "+
				"its volumes will not be listed",
				"class", class.Name, "error", parseErr)
			skipped++
			continue
		}

		classes = append(classes, managedClass{
			name:   class.Name,
			params: parsed,
			raw:    class,
			root:   parsed.realStoreRoot(),
		})
	}

	return classes, skipped, nil
}

// secretReadError is a class's Secret that exists, or may, but could not be
// read because the Kubernetes API is in trouble: it throttled, timed out, or
// answered 5xx. It says nothing about the class, so unlike a missing Secret or
// key it must not make a listing skip the class.
//
// A *refusal* is deliberately not one of these, and used to be. The original
// wording listed "refused" beside "throttled" and "timed out", which was true
// when the controller could read any Secret and so a refusal could only mean
// the API server was misbehaving. narrowed that permission, and from then
// on a refusal was ordinary configuration -- permanent, cleared by no retry,
// and a statement about exactly one class. Classifying it here failed every
// ListVolumes in the cluster whenever a single class named a Secret the
// release does not list. The axis that matters to a caller is
// permanent versus transient, and this type is the transient half.
type secretReadError struct {
	ref secretRef
	err error
}

func (e *secretReadError) Error() string { return fmt.Sprintf("reading Secret %s: %v", e.ref, e.err) }

func (e *secretReadError) Unwrap() error { return e.err }

// Reasons a class's credential is unusable, as reported to an
// InventoryObserver. Each is permanent -- a class is only reported unusable
// once retrying has been ruled out.
const (
	// ClassCredentialForbidden is a Secret this controller may not read:
	// the release does not name it, so's Role does not grant it.
	ClassCredentialForbidden = "forbidden"
	// ClassCredentialMissing is a Secret that does not exist.
	ClassCredentialMissing = "missing"
	// ClassCredentialUnusable is a class that names no Secret, or one with
	// no token key in it.
	ClassCredentialUnusable = "unusable"
)

// InventoryObserver is told, per StorageClass, whether the inventory could
// build a credential for it.
//
// It exists because of what skipping a class costs. ListVolumes skips a class
// whose credential is permanently unusable rather than failing the whole call,
// which keeps one bad class from hiding every good one -- but it also
// means a class's volumes can stop being listed with nothing in the response
// to say so. CSI's ListVolumesResponse has no field for it, so the count the
// driver keeps internally reaches no caller.
//
// A log line alone is not enough to carry that: it is the difference between
// an operator finding this in minutes and finding it never. So the skip is
// also reported here, where it can be made alertable. Nil unless the
// deployment asked for metrics, and a nil observer changes no behaviour --
// the same contract qumulo.Observer has.
//
// Deliberately primitive arguments, so pkg/metrics implements this without
// pkg/driver's types and no import is added in either direction.
type InventoryObserver interface {
	// ObserveClassCredential reports one class's credential as usable or
	// not. reason is one of the ClassCredential* constants when usable is
	// false, and empty when it is true.
	ObserveClassCredential(ctx context.Context, class, cluster, reason string, usable bool)
	// ForgetClassesExcept retires what is held for every class outside the
	// set a walk just saw, so a class that has been deleted stops being
	// reported. Without it the forbidden-Secret case itself -- create a class, see the
	// alert, delete the class -- would leave the alert firing for ever.
	ForgetClassesExcept(ctx context.Context, classes []string)
}

// observeClassCredential reports one class's credential to the observer, if
// there is one. cause is nil when the credential was built.
func (s *ControllerServer) observeClassCredential(
	ctx context.Context,
	class managedClass,
	usable bool,
	cause error,
) {
	observer := s.driver.InventoryObserver
	if observer == nil {
		return
	}
	reason := ""
	switch {
	case usable:
	case IsSecretForbidden(cause):
		reason = ClassCredentialForbidden
	case IsSecretNotFound(cause):
		reason = ClassCredentialMissing
	default:
		reason = ClassCredentialUnusable
	}
	observer.ObserveClassCredential(ctx, class.name, class.params.Server, reason, usable)
}

// forgetClassesExcept retires the observer's state for classes this walk did
// not see. names is every class the walk considered, whether or not its
// credential worked.
//
// A class the driver could not parse never reaches the walk, so it is retired
// here rather than reported: its skip is counted by resolveManagedClasses and
// logged there, and a series for a class whose parameters cannot be read would
// have nothing useful in its labels.
func (s *ControllerServer) forgetClassesExcept(ctx context.Context, classes []managedClass) {
	observer := s.driver.InventoryObserver
	if observer == nil {
		return
	}
	names := make([]string, 0, len(classes))
	for _, class := range classes {
		names = append(names, class.name)
	}
	observer.ForgetClassesExcept(ctx, names)
}

// classEndpoint is the REST endpoint a class's clients dial, normalised for
// comparison. Two classes that name one array by different `server` values
// but the same endpoint reach the same array; two that share a `server` name
// but not an endpoint may not.
func classEndpoint(class managedClass) string {
	return strings.ToLower(apiURLFor(class.params.Server, class.params.APIURL))
}

// clientForClass builds a Qumulo client for one class from the Secret its
// parameters name.
//
// Identical in shape to what GetCapacity does, and for the same reason: the
// credential is resolved at the point of use and never held, so a rotated
// Secret takes effect immediately. The difference is only where the reference
// came from -- there, the request's own parameters; here, a class the driver
// looked up.
func (s *ControllerServer) clientForClass(
	ctx context.Context,
	class managedClass,
) (qumulo.ClientInterface, error) {
	ref, err := secretRefFromParams(class.raw.Parameters)
	if err != nil {
		return nil, err
	}

	data, err := s.readSecretOncePerCall(ctx, ref)
	if err != nil {
		// Wrapped, not flattened, so a caller can still tell which permanent
		// reason it was -- the skipped-class signal reports them apart.
		if IsSecretNotFound(err) {
			return nil, fmt.Errorf("the class's credential cannot be read: %w", err)
		}
		// Permanent, like a missing Secret, and reported the same way: this
		// is one class the release does not grant a credential for, not a
		// fault of the Kubernetes API. The wrapped error carries the remedy.
		if IsSecretForbidden(err) {
			return nil, fmt.Errorf("the class's credential cannot be read: %w", err)
		}
		return nil, &secretReadError{ref: ref, err: err}
	}

	token, err := tokenFromSecret(data, ref)
	if err != nil {
		return nil, err
	}

	return s.clientFor(class.params.Server, class.params.APIURL,
		map[string]string{secretKeyToken: token})
}

// secretMemo holds the Secrets one ListVolumes or ControllerGetVolume call
// has read, so a Secret every class names is read once per call rather than
// once per class: a paged walk of 2,010 volumes made 630 Secret GETs on a
// client the API server throttles. It lives only as long as the call,
// so a rotated Secret takes effect on the next one.
type secretMemo struct {
	mu   sync.Mutex
	read map[secretRef]secretRead
}

type secretRead struct {
	data map[string][]byte
	err  error
}

type secretMemoKey struct{}

// withSecretMemo returns a context whose Secret reads are shared for the
// call's lifetime.
func withSecretMemo(ctx context.Context) context.Context {
	return context.WithValue(ctx, secretMemoKey{}, &secretMemo{read: map[secretRef]secretRead{}})
}

// readSecretOncePerCall reads a Secret, or answers from the call's memo when
// this call already read it. Outside a memoised call it reads every time.
func (s *ControllerServer) readSecretOncePerCall(ctx context.Context, ref secretRef) (map[string][]byte, error) {
	memo, ok := ctx.Value(secretMemoKey{}).(*secretMemo)
	if !ok {
		return s.driver.SecretReader.GetSecret(ctx, ref.Namespace, ref.Name)
	}
	memo.mu.Lock()
	defer memo.mu.Unlock()
	if got, ok := memo.read[ref]; ok {
		return got.data, got.err
	}
	data, err := s.driver.SecretReader.GetSecret(ctx, ref.Namespace, ref.Name)
	memo.read[ref] = secretRead{data: data, err: err}
	return data, err
}
