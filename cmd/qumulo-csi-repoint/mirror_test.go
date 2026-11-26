// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// sourcePV is a PV as this driver's provisioner writes one: the handle and the
// volume attributes are copied from a real `kubectl get pv -o yaml` of a
// lab-provisioned volume, with the names changed.
func sourcePV() *corev1.PersistentVolume {
	fsType := "nfs"
	volumeMode := corev1.PersistentVolumeFilesystem

	return &corev1.PersistentVolume{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"},
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-abc123", Labels: map[string]string{"tier": "gold"}},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			MountOptions:                  []string{"hard", "timeo=600"},
			VolumeMode:                    &volumeMode,
			StorageClassName:              "qumulo-nfs",
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			ClaimRef: &corev1.ObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Namespace:  "apps",
				Name:       "data",
				UID:        "8b4a2c1e-0000-0000-0000-000000000000",
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "csi.qumulo.com",
					VolumeHandle: "c1.example.com##/csi/volumes/pvc-abc123",
					FSType:       fsType,
					VolumeAttributes: map[string]string{
						"server":     "c1.example.com",
						"exportPath": "/csi/volumes/pvc-abc123",
						"realPath":   "/csi/volumes/pvc-abc123",
						"protocol":   "nfs4.1",
						"tenantID":   "2",
					},
					ControllerExpandSecretRef: &corev1.SecretReference{Name: "qumulo-token", Namespace: "qumulo-csi"},
				},
			},
		},
	}
}

func testMirrorOptions() mirrorOptions {
	return mirrorOptions{
		driverName: "csi.qumulo.com",
		peerServer: "c2.example.com",
		pvSuffix:   "-dr",
	}
}

// The rewrite, field by field. This is the test the whole command exists for:
// every other part is plumbing, and this is the part that must not be wrong.
func TestMirrorVolumeRewritesOnlyWhatMustChange(t *testing.T) {
	src := sourcePV()

	pv, pvc, err := mirrorVolume(src, "/csi/volumes/pvc-abc123", testMirrorOptions())
	require.NoError(t, err)

	t.Run("the handle carries the peer's server and the replica's own file ID", func(t *testing.T) {
		// Never the source's ID. Replication does not preserve file IDs, and a
		// stale one is usually a *valid* ID for a different file on the target
		// -- which ControllerExpandVolume would then re-quota silently.
		assert.Equal(t, "c2.example.com##/csi/volumes/pvc-abc123", pv.Spec.CSI.VolumeHandle)
		assert.NotContains(t, pv.Spec.CSI.VolumeHandle, "12884901888")
	})

	t.Run("server is the only volume attribute that changes, plus the replica marker", func(t *testing.T) {
		assert.Equal(t, "c2.example.com", pv.Spec.CSI.VolumeAttributes["server"])
		// The same path on both arrays, so everything path-shaped is
		// carried through untouched.
		assert.Equal(t, src.Spec.CSI.VolumeAttributes["exportPath"], pv.Spec.CSI.VolumeAttributes["exportPath"])
		assert.Equal(t, src.Spec.CSI.VolumeAttributes["realPath"], pv.Spec.CSI.VolumeAttributes["realPath"])
		// An attribute this tool knows nothing about must survive: the node
		// reads them all, and the controller stamped them for a reason.
		assert.Equal(t, "2", pv.Spec.CSI.VolumeAttributes["tenantID"])
		assert.Equal(t, "nfs4.1", pv.Spec.CSI.VolumeAttributes["protocol"])
		// And the one attribute the original does not have at all.
		assert.Equal(t, "true", pv.Spec.CSI.VolumeAttributes["csi.qumulo.com/replicated"],
			"the node's replica probe is what stops a pod writing into an unpromoted replica")
	})

	t.Run("the source object is not modified", func(t *testing.T) {
		// maps.Clone, not a shared reference: mirroring must not edit the PV
		// the operator piped in, which a later step may still need.
		assert.Equal(t, "c1.example.com", src.Spec.CSI.VolumeAttributes["server"])
		assert.NotContains(t, src.Spec.CSI.VolumeAttributes, "csi.qumulo.com/replicated")
	})

	t.Run("reclaim policy is Retain whatever the source was", func(t *testing.T) {
		require.Equal(t, corev1.PersistentVolumeReclaimDelete, src.Spec.PersistentVolumeReclaimPolicy)
		assert.Equal(t, corev1.PersistentVolumeReclaimRetain, pv.Spec.PersistentVolumeReclaimPolicy,
			"a DR copy one kubectl delete pvc can tree-delete is not a DR copy")
	})

	t.Run("claimRef is pre-set and carries no UID", func(t *testing.T) {
		require.NotNil(t, pv.Spec.ClaimRef)
		assert.Equal(t, "apps", pv.Spec.ClaimRef.Namespace)
		assert.Equal(t, "data", pv.Spec.ClaimRef.Name)
		// A UID copied from the source PVC would never match the PVC created
		// on the DR cluster, and the PV would sit Released forever.
		assert.Empty(t, pv.Spec.ClaimRef.UID)
	})

	t.Run("everything else is copied", func(t *testing.T) {
		assert.Equal(t, src.Spec.Capacity, pv.Spec.Capacity)
		assert.Equal(t, src.Spec.AccessModes, pv.Spec.AccessModes)
		assert.Equal(t, src.Spec.MountOptions, pv.Spec.MountOptions)
		assert.Equal(t, src.Spec.StorageClassName, pv.Spec.StorageClassName)
		assert.Equal(t, src.Spec.CSI.FSType, pv.Spec.CSI.FSType)
		assert.Equal(t, map[string]string{"tier": "gold"}, pv.Labels)
	})

	t.Run("provenance is recorded", func(t *testing.T) {
		assert.Equal(t, "pvc-abc123", pv.Annotations[annotationMirroredFrom])
		assert.Equal(t, "c1.example.com##/csi/volumes/pvc-abc123", pv.Annotations[annotationSourceHandle])
		assert.Equal(t, "c1.example.com", pv.Annotations[annotationSourceServer])
	})

	t.Run("the claim is pre-bound to the mirrored PV", func(t *testing.T) {
		assert.Equal(t, "data", pvc.Name)
		assert.Equal(t, "apps", pvc.Namespace)
		// Both halves of the binding. One alone leaves the claim Pending.
		assert.Equal(t, "pvc-abc123-dr", pvc.Spec.VolumeName)
		assert.Equal(t, pv.Name, pvc.Spec.VolumeName)
		require.NotNil(t, pvc.Spec.StorageClassName)
		assert.Equal(t, "qumulo-nfs", *pvc.Spec.StorageClassName)
		assert.Equal(t, resource.MustParse("10Gi"), pvc.Spec.Resources.Requests[corev1.ResourceStorage])
	})
}

// Topology B: one Kubernetes cluster, two arrays. The mirrored objects live
// alongside the originals, so both names have to move.
func TestMirrorVolumeNamesForASingleCluster(t *testing.T) {
	opt := testMirrorOptions()
	opt.claimSuffix = "-dr"

	pv, pvc, err := mirrorVolume(sourcePV(), "/csi/volumes/pvc-abc123", opt)
	require.NoError(t, err)

	assert.Equal(t, "pvc-abc123-dr", pv.Name)
	assert.Equal(t, "data-dr", pvc.Name)
	assert.Equal(t, "data-dr", pv.Spec.ClaimRef.Name,
		"the PV must be reserved for the claim that will actually exist")
}

// The secret references follow the handle, because every RPC that carries one
// dials the array the handle names.
func TestMirrorVolumeRepointsSecretReferences(t *testing.T) {
	opt := testMirrorOptions()
	opt.secretName = "qumulo-dr-token"
	opt.secretNamespace = "dr"

	pv, _, err := mirrorVolume(sourcePV(), "/csi/volumes/pvc-abc123", opt)
	require.NoError(t, err)

	require.NotNil(t, pv.Spec.CSI.ControllerExpandSecretRef)
	assert.Equal(t, "qumulo-dr-token", pv.Spec.CSI.ControllerExpandSecretRef.Name)
	assert.Equal(t, "dr", pv.Spec.CSI.ControllerExpandSecretRef.Namespace)
	assert.Nil(t, pv.Spec.CSI.NodeStageSecretRef, "a reference the source did not have is not invented")
}

// Every way the rewrite must refuse rather than guess.
func TestMirrorVolumeRefusals(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mutate      func(*corev1.PersistentVolume)
		replicaPath string
		opt         func(*mirrorOptions)
		wantErr     string
	}{
		{
			// The classic mistake -- changing
			// only `server` and keeping the source's file ID -- can no longer
			// be expressed at all: there is no file ID in a handle. What is
			// left to refuse is a replica path that is not a cluster path.
			name:        "a relative replica path",
			replicaPath: "csi/volumes/pvc-abc123",
			wantErr:     "not an absolute cluster path",
		},
		{
			name:    "an unbound PV",
			mutate:  func(pv *corev1.PersistentVolume) { pv.Spec.ClaimRef = nil },
			wantErr: "has no claimRef",
		},
		{
			name:    "another driver's PV",
			mutate:  func(pv *corev1.PersistentVolume) { pv.Spec.CSI.Driver = "nfs.csi.k8s.io" },
			wantErr: "belongs to driver",
		},
		{
			name:    "a non-CSI PV",
			mutate:  func(pv *corev1.PersistentVolume) { pv.Spec.CSI = nil },
			wantErr: "not a CSI volume",
		},
		{
			name:    "a malformed handle",
			mutate:  func(pv *corev1.PersistentVolume) { pv.Spec.CSI.VolumeHandle = "c1.example.com/csi/volumes/pvc-abc123" },
			wantErr: "malformed",
		},
		{
			name:    "no peer server",
			opt:     func(o *mirrorOptions) { o.peerServer = "" },
			wantErr: "peer server must not be empty",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := sourcePV()
			if tc.mutate != nil {
				tc.mutate(src)
			}
			opt := testMirrorOptions()
			if tc.opt != nil {
				tc.opt(&opt)
			}
			replicaPath := tc.replicaPath
			if replicaPath == "" {
				replicaPath = "/csi/volumes/pvc-abc123"
			}

			_, _, err := mirrorVolume(src, replicaPath, opt)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// fakeResolver stands in for the peer array's
// GET /v1/files/<path>/info/attributes.
type fakeResolver struct {
	byPath map[string]string
	err    error
	asked  []string
}

func (f *fakeResolver) FileIDAt(_ context.Context, clusterPath string) (string, error) {
	f.asked = append(f.asked, clusterPath)
	if f.err != nil {
		return "", f.err
	}
	id, ok := f.byPath[clusterPath]
	if !ok {
		return "", errors.New("404 fs_no_such_entry_error")
	}
	return id, nil
}

// The path looked up on the peer is the volume's own path, because the replica
// sits at the same path on both arrays.
func TestMirrorAllLooksUpEachReplicaOnThePeer(t *testing.T) {
	resolver := &fakeResolver{byPath: map[string]string{"/csi/volumes/pvc-abc123": "60129542144"}}

	pairs, err := mirrorAll(context.Background(),
		[]*corev1.PersistentVolume{sourcePV()}, resolver, testMirrorOptions())

	require.NoError(t, err)
	require.Len(t, pairs, 1)
	assert.Equal(t, []string{"/csi/volumes/pvc-abc123"}, resolver.asked)
	assert.Equal(t, "c2.example.com##/csi/volumes/pvc-abc123", pairs[0].Volume.Spec.CSI.VolumeHandle)
}

// A peer laid out differently re-roots the parent and keeps the volume's own
// directory name.
func TestMirrorAllHonoursAPeerBasePath(t *testing.T) {
	resolver := &fakeResolver{byPath: map[string]string{"/dr/volumes/pvc-abc123": "60129542144"}}
	opt := testMirrorOptions()
	opt.peerBasePath = "/dr/volumes"

	_, err := mirrorAll(context.Background(),
		[]*corev1.PersistentVolume{sourcePV()}, resolver, opt)

	require.NoError(t, err)
	assert.Equal(t, []string{"/dr/volumes/pvc-abc123"}, resolver.asked)
}

// Other drivers' PVs are skipped, not refused: `kubectl get pv -o yaml` hands
// over the whole cluster's worth.
func TestMirrorAllSkipsOtherDriversVolumes(t *testing.T) {
	other := sourcePV()
	other.Name = "pvc-other"
	other.Spec.CSI.Driver = "ebs.csi.aws.com"

	resolver := &fakeResolver{byPath: map[string]string{"/csi/volumes/pvc-abc123": "60129542144"}}

	pairs, err := mirrorAll(context.Background(),
		[]*corev1.PersistentVolume{other, sourcePV()}, resolver, testMirrorOptions())

	require.NoError(t, err)
	require.Len(t, pairs, 1)
	assert.Equal(t, "pvc-abc123-dr", pairs[0].Volume.Name)
}

// A replica the peer has never heard of stops the run. Emitting a PV whose
// handle names a directory that does not exist would produce a volume that
// mounts (the node never reads the file ID) and then expands the wrong
// directory.
func TestMirrorAllStopsOnALookupFailure(t *testing.T) {
	resolver := &fakeResolver{byPath: map[string]string{}}

	_, err := mirrorAll(context.Background(),
		[]*corev1.PersistentVolume{sourcePV()}, resolver, testMirrorOptions())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "the replica directory /csi/volumes/pvc-abc123 is not present on the peer")
}

// A PV with no realPath cannot have its replica located, and says which flag
// fixes it rather than guessing a path.
func TestMirrorAllRefusesAPVWithNoRealPath(t *testing.T) {
	src := sourcePV()
	delete(src.Spec.CSI.VolumeAttributes, "realPath")

	_, err := mirrorAll(context.Background(),
		[]*corev1.PersistentVolume{src}, &fakeResolver{}, testMirrorOptions())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "-peer-base-path")
}

// The summary is what a reviewer reads before applying anything, so it must
// name both handles.
func TestMirrorSummaryNamesBothHandles(t *testing.T) {
	resolver := &fakeResolver{byPath: map[string]string{"/csi/volumes/pvc-abc123": "60129542144"}}
	pairs, err := mirrorAll(context.Background(),
		[]*corev1.PersistentVolume{sourcePV()}, resolver, testMirrorOptions())
	require.NoError(t, err)

	summary := mirrorSummary(pairs)

	assert.Contains(t, summary, "c1.example.com##/csi/volumes/pvc-abc123 -> c2.example.com##/csi/volumes/pvc-abc123")
	assert.Contains(t, summary, "apps/data")
	for _, line := range strings.Split(strings.TrimSpace(summary), "\n") {
		assert.True(t, strings.HasPrefix(line, "#"),
			"the summary shares stdout with the manifests, so every line must be a YAML comment: %q", line)
	}
}

// importedSourcePV is a hand-written PV for an administrator's folder that
// carries the driver's delete-directory opt-in: not named after its
// directory, so without the opt-in the driver would refuse to delete it.
func importedSourcePV(optIn string) *corev1.PersistentVolume {
	src := sourcePV()
	src.Name = "finance-import"
	src.Spec.CSI.VolumeHandle = "c1.example.com##/data/finance"
	src.Spec.CSI.VolumeAttributes["realPath"] = "/data/finance"
	src.Spec.CSI.VolumeAttributes["exportPath"] = "/finance"
	if optIn != "" {
		src.Annotations = map[string]string{annotationDeleteDirectory: optIn}
	}
	return src
}

// The driver lets Kubernetes delete a directory only for a PV that owns
// it: named after it, mirrored from one that is, or carrying the
// administrator's delete-directory opt-in. A mirror of a dynamically
// provisioned PV owns its replica through mirrored-from and needs nothing
// more; a mirror of a source that owned its directory only through the opt-in
// carries the opt-in too, naming the replica's own path.
func TestMirrorVolumeCarriesTheDeleteDirectoryOptIn(t *testing.T) {
	t.Run("a dynamically provisioned source needs none", func(t *testing.T) {
		pv, _, err := mirrorVolume(sourcePV(), "/csi/volumes/pvc-abc123", testMirrorOptions())
		require.NoError(t, err)
		assert.Equal(t, "pvc-abc123", pv.Annotations[annotationMirroredFrom])
		assert.NotContains(t, pv.Annotations, annotationDeleteDirectory)
	})

	t.Run("an opted-in source's opt-in names the replica, cleaned", func(t *testing.T) {
		pv, _, err := mirrorVolume(importedSourcePV("/data/finance/"), "/data/finance", testMirrorOptions())
		require.NoError(t, err)
		assert.Equal(t, "/data/finance", pv.Annotations[annotationDeleteDirectory])
	})

	t.Run("under -peer-base-path the opt-in follows the replica", func(t *testing.T) {
		opt := testMirrorOptions()
		opt.peerBasePath = "/dr/data"
		src := importedSourcePV("/data/finance")
		replicaPath, err := replicaPathFor(src, opt)
		require.NoError(t, err)

		pv, _, err := mirrorVolume(src, replicaPath, opt)

		require.NoError(t, err)
		assert.Equal(t, "c2.example.com##/dr/data/finance", pv.Spec.CSI.VolumeHandle)
		assert.Equal(t, "/dr/data/finance", pv.Annotations[annotationDeleteDirectory])
	})

	t.Run("a source mirrored from a dynamic PV keeps its replica deletable", func(t *testing.T) {
		src := sourcePV()
		src.Name = "pvc-abc123-dr"
		src.Annotations = map[string]string{annotationMirroredFrom: "pvc-abc123"}

		pv, _, err := mirrorVolume(src, "/csi/volumes/pvc-abc123", testMirrorOptions())

		require.NoError(t, err)
		assert.Equal(t, "pvc-abc123-dr", pv.Annotations[annotationMirroredFrom])
		assert.Equal(t, "/csi/volumes/pvc-abc123", pv.Annotations[annotationDeleteDirectory])
	})

	for name, optIn := range map[string]string{
		"no opt-in":                  "",
		"an opt-in for another path": "/data/payroll",
		"the export path":            "/finance",
		"a path with a dot element":  "/data/x/../finance",
	} {
		t.Run("a source that does not own its directory: "+name, func(t *testing.T) {
			pv, _, err := mirrorVolume(importedSourcePV(optIn), "/data/finance", testMirrorOptions())
			require.NoError(t, err)
			assert.NotContains(t, pv.Annotations, annotationDeleteDirectory,
				"the mirror would own a directory its source did not")
		})
	}

	t.Run("the source object is not modified", func(t *testing.T) {
		src := importedSourcePV("/data/finance/")
		_, _, err := mirrorVolume(src, "/dr/data/finance", testMirrorOptions())
		require.NoError(t, err)
		assert.Equal(t, "/data/finance/", src.Annotations[annotationDeleteDirectory])
	})
}
