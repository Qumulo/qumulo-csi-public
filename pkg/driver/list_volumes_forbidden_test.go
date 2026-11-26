// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// The controller may read only the Secrets this release names,
// so a StorageClass made outside the chart -- by hand, or by a GitOps
// pipeline -- is answered Forbidden for its Secret. That is permanent and it
// is about one class, but the inventory classified it with throttles and
// timeouts and failed every ListVolumes in the cluster with Unavailable: a
// retryable code that no retry can clear.
//
// Reproduced live: 37 entries, then one extra class, then none,
// then the class was removed and 37 again. The class had no volumes of its
// own. Creating it was enough.

// forbiddenFor answers Forbidden for one Secret, as a SecretReader must when
// the API server refuses the read under the Role. pkg/k8s is where a
// real apierrors.IsForbidden becomes this; that translation has its own test.
type forbiddenFor struct {
	fakeSecretReader
	name string
}

func (f *forbiddenFor) GetSecret(ctx context.Context, ns, name string) (map[string][]byte, error) {
	if name == f.name {
		return nil, NewSecretForbiddenError(ns, name)
	}
	return f.fakeSecretReader.GetSecret(ctx, ns, name)
}

// transientFor answers an untyped error for one Secret: the API server
// throttled, timed out or returned 5xx. This is the case that must still fail
// the whole call.
type transientFor struct {
	fakeSecretReader
	name string
}

func (f *transientFor) GetSecret(ctx context.Context, ns, name string) (map[string][]byte, error) {
	if name == f.name {
		return nil, errors.New("etcdserver: request timed out")
	}
	return f.fakeSecretReader.GetSecret(ctx, ns, name)
}

// twoClassesOneArray is the live shape: a chart-rendered class, and one made
// outside the release naming a Secret the release does not list. Both name the
// same array, and the hand-made one sorts first.
func twoClassesOneArray(t *testing.T) *ControllerServer {
	t.Helper()
	client := &MockClient{
		ListDirectoryFunc: func(context.Context, string, string) ([]qumulo.DirEntry, error) {
			return dirEntries(map[string]string{"pvc-1": "101"}), nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 30, nil },
	}
	return inventoryServer(t,
		&fakeClassLister{classes: []StorageClassInfo{
			classFor("class-by-hand", "qumulo-a.example.com", "/csi/a", "tok-unnamed"),
			classFor("class-helm", "qumulo-a.example.com", "/csi/a", "tok"),
		}},
		map[string]map[string][]byte{"qumulo-csi/tok": {secretKeyToken: []byte("t")}},
		map[string]*MockClient{baseURLFor("qumulo-a.example.com"): client},
	)
}

func withSecrets(srv *ControllerServer, reader SecretReader) *ControllerServer {
	srv.driver.SecretReader = reader
	return srv
}

func namedSecrets() fakeSecretReader {
	return fakeSecretReader{secrets: map[string]map[string][]byte{
		"qumulo-csi/tok": {secretKeyToken: []byte("t")},
	}}
}

// TestListVolumesSkipsAClassWhoseSecretIsForbidden is the case itself: one class
// the release does not grant a credential for must cost that class's volumes,
// not every class's.
func TestListVolumesSkipsAClassWhoseSecretIsForbidden(t *testing.T) {
	srv := withSecrets(twoClassesOneArray(t),
		&forbiddenFor{fakeSecretReader: namedSecrets(), name: "tok-unnamed"})

	resp, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})

	require.NoError(t, err, "a forbidden Secret on one class must not fail the whole call")
	require.NotEmpty(t, resp.GetEntries(),
		"the class whose Secret the release does name must still be listed")
}

// TestListVolumesStillFailsForATransientSecretError guards the other half of
// the classification. Its reasoning is right for a Kubernetes API that is
// actually in trouble, and that case must keep failing retryably -- otherwise
// the fix would have traded one silent wrong answer for another.
func TestListVolumesStillFailsForATransientSecretError(t *testing.T) {
	srv := withSecrets(twoClassesOneArray(t),
		&transientFor{fakeSecretReader: namedSecrets(), name: "tok-unnamed"})

	_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err),
		"a throttled or timed-out Secret read is still an outage of the Kubernetes API")
}

// TestListVolumesRefusesWhenEveryClassIsForbidden covers the guard that makes
// skipping safe: if nothing could be enumerated, an empty listing would be
// indistinguishable from having no volumes, so the call is refused -- and the
// refusal must name the remedy rather than leaving an operator to find it.
func TestListVolumesRefusesWhenEveryClassIsForbidden(t *testing.T) {
	srv := withSecrets(twoClassesOneArray(t),
		&forbiddenFor{fakeSecretReader: fakeSecretReader{}, name: "tok"})
	srv.driver.SecretReader = &everythingForbidden{}

	_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err),
		"nothing could be listed, and no retry will change that")
	assert.Contains(t, status.Convert(err).Message(), "rbac.extraSecrets",
		"the refusal must say what to change")
}

type everythingForbidden struct{}

func (everythingForbidden) GetSecret(_ context.Context, ns, name string) (map[string][]byte, error) {
	return nil, NewSecretForbiddenError(ns, name)
}

// TestControllerGetVolumeTriesTheNextClassWhenTheFirstIsForbidden is the half
// of the forbidden-Secret case found by reading rather than in the lab, and the half a sidecar
// actually reaches: ControllerGetVolume and ControllerGetVolumeHealth resolve
// a credential through clusterForVolume, whose own comment says the classes
// are tried in name order and the first that authenticates is used. It took
// matching[0] and gave up, so a hand-made class sorting earlier made every
// volume on that array unanswerable.
func TestControllerGetVolumeTriesTheNextClassWhenTheFirstIsForbidden(t *testing.T) {
	srv := withSecrets(twoClassesOneArray(t),
		&forbiddenFor{fakeSecretReader: namedSecrets(), name: "tok-unnamed"})

	resp, err := srv.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{
		VolumeId: "qumulo-a.example.com##/csi/a/pvc-1",
	})

	require.NoError(t, err, "the second class names the same array and has a readable Secret")
	require.Equal(t, "qumulo-a.example.com##/csi/a/pvc-1", resp.GetVolume().GetVolumeId())
}

// TestControllerGetVolumeIsFailedPreconditionWhenEveryClassIsForbidden: when
// no class yields a credential and none of the failures can clear, the answer
// must be permanent too. Unavailable would invite a retry that cannot work.
func TestControllerGetVolumeIsFailedPreconditionWhenEveryClassIsForbidden(t *testing.T) {
	srv := twoClassesOneArray(t)
	srv.driver.SecretReader = &everythingForbidden{}

	_, err := srv.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{
		VolumeId: "qumulo-a.example.com##/csi/a/pvc-1",
	})

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestControllerGetVolumeIsUnavailableWhenAClassReadIsTransient: a class the
// API server merely throttled may work on the next call, so the whole answer
// is retryable even though a second class failed permanently.
func TestControllerGetVolumeIsUnavailableWhenAClassReadIsTransient(t *testing.T) {
	srv := withSecrets(twoClassesOneArray(t),
		&transientFor{fakeSecretReader: fakeSecretReader{}, name: "tok-unnamed"})

	_, err := srv.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{
		VolumeId: "qumulo-a.example.com##/csi/a/pvc-1",
	})

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err),
		"one class was throttled, so the answer may differ on a retry")
}

// recordingObserver captures what the inventory reported about each class.
type recordingObserver struct {
	usable  map[string]bool
	reasons map[string]string
	kept    []string
}

func (o *recordingObserver) ObserveClassCredential(
	_ context.Context,
	class, _, reason string,
	usable bool,
) {
	if o.usable == nil {
		o.usable = map[string]bool{}
		o.reasons = map[string]string{}
	}
	o.usable[class] = usable
	o.reasons[class] = reason
}

func (o *recordingObserver) ForgetClassesExcept(_ context.Context, classes []string) {
	o.kept = classes
}

// TestListVolumesReportsASkippedClass is the containment for the skip. CSI's
// ListVolumesResponse has no field for a class the driver left out, so the
// count it keeps internally reaches no caller: without this signal the fix
// would trade a loud wrong answer for a quiet incomplete one.
func TestListVolumesReportsASkippedClass(t *testing.T) {
	srv := withSecrets(twoClassesOneArray(t),
		&forbiddenFor{fakeSecretReader: namedSecrets(), name: "tok-unnamed"})
	observer := &recordingObserver{}
	srv.driver.InventoryObserver = observer

	_, err := srv.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)

	assert.False(t, observer.usable["class-by-hand"], "the skipped class must be reported")
	assert.Equal(t, ClassCredentialForbidden, observer.reasons["class-by-hand"],
		"the reason decides the remedy: forbidden is fixed in rbac.extraSecrets")
	assert.True(t, observer.usable["class-helm"], "the class that worked is reported too")
	assert.ElementsMatch(t, []string{"class-by-hand", "class-helm"}, observer.kept,
		"the walk reports its full class set, so a deleted class can be retired")
}

// TestGetCapacityForbiddenSecretIsFailedPrecondition applies the same axis to
// the other RPC that resolves a Secret itself. It reported a refusal as
// Internal -- "the driver is broken" -- and invited the same futile retry.
func TestGetCapacityForbiddenSecretIsFailedPrecondition(t *testing.T) {
	srv := twoClassesOneArray(t)
	srv.driver.SecretReader = &everythingForbidden{}

	_, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: classFor("class-helm", "qumulo-a.example.com", "/csi/a", "tok").Parameters,
	})

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "rbac.extraSecrets")
}
