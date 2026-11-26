// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The output has to be applicable, not merely YAML-shaped: a DR procedure that
// ends in `kubectl apply -f` is only as good as what it applies. So the
// manifest is round-tripped back through the decoder and compared on the
// fields that carry the failover.
func TestManifestRoundTrips(t *testing.T) {
	pv, pvc, err := mirrorVolume(sourcePV(), "/csi/volumes/pvc-abc123", testMirrorOptions())
	require.NoError(t, err)

	var out bytes.Buffer
	require.NoError(t, writeManifest(&out, pv, pvc))

	t.Run("it is a multi-document stream with apiVersion and kind", func(t *testing.T) {
		text := out.String()
		separators := 0
		for _, line := range strings.Split(text, "\n") {
			if line == "---" {
				separators++
			}
		}
		assert.Equal(t, 2, separators, "each object needs its own document separator for kubectl apply")
		assert.Contains(t, text, "kind: PersistentVolume\n")
		assert.Contains(t, text, "kind: PersistentVolumeClaim\n")
		assert.Contains(t, text, "apiVersion: v1")
	})

	t.Run("and decodes back to the same objects", func(t *testing.T) {
		stream, err := decodeObjects(bytes.NewReader(out.Bytes()))
		require.NoError(t, err)
		require.Len(t, stream.Volumes, 1)
		require.Len(t, stream.Claims, 1)

		got := stream.Volumes[0]
		assert.Equal(t, "pvc-abc123-dr", got.Name)
		assert.Equal(t, "c2.example.com##/csi/volumes/pvc-abc123", got.Spec.CSI.VolumeHandle)
		assert.Equal(t, "c2.example.com", got.Spec.CSI.VolumeAttributes["server"])
		assert.Equal(t, "true", got.Spec.CSI.VolumeAttributes["csi.qumulo.com/replicated"])
		assert.Equal(t, "Retain", string(got.Spec.PersistentVolumeReclaimPolicy))
		require.NotNil(t, got.Spec.ClaimRef)
		assert.Equal(t, "apps", got.Spec.ClaimRef.Namespace)
		assert.Equal(t, "data", got.Spec.ClaimRef.Name)

		assert.Equal(t, "pvc-abc123-dr", stream.Claims[0].Spec.VolumeName)
	})

	t.Run("and carries no status or creation timestamp a human must strip", func(t *testing.T) {
		// A mirrored object is written, not copied: pasting `kubectl get -o
		// yaml` output back into a cluster is exactly the workflow this
		// replaces, and its resourceVersion/uid/status are what make that
		// painful.
		assert.NotContains(t, out.String(), "resourceVersion:")
		assert.NotContains(t, out.String(), "uid:")
	})
}

// The input side: the three shapes an operator actually has to hand.
func TestDecodeObjectsAcceptsWhatKubectlProduces(t *testing.T) {
	const pvYAML = `
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pvc-one
spec:
  csi:
    driver: csi.qumulo.com
    volumeHandle: c1.example.com##/csi/volumes/pvc-one
`
	const pvcYAML = `
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  namespace: apps
spec:
  volumeName: pvc-one
`

	t.Run("a single document", func(t *testing.T) {
		stream, err := decodeObjects(strings.NewReader(pvYAML))
		require.NoError(t, err)
		assert.Len(t, stream.Volumes, 1)
	})

	t.Run("several documents", func(t *testing.T) {
		stream, err := decodeObjects(strings.NewReader(pvYAML + "---" + pvcYAML + "\n---\n"))
		require.NoError(t, err)
		assert.Len(t, stream.Volumes, 1)
		assert.Len(t, stream.Claims, 1)
	})

	t.Run("a kubectl get -o yaml List", func(t *testing.T) {
		const list = `
apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: PersistentVolume
  metadata:
    name: pvc-one
  spec:
    csi:
      driver: csi.qumulo.com
      volumeHandle: c1.example.com##/csi/volumes/pvc-one
- apiVersion: v1
  kind: PersistentVolume
  metadata:
    name: pvc-two
  spec:
    csi:
      driver: csi.qumulo.com
      volumeHandle: c1.example.com##/csi/volumes/pvc-two
`
		stream, err := decodeObjects(strings.NewReader(list))
		require.NoError(t, err)
		require.Len(t, stream.Volumes, 2)
		assert.Equal(t, "pvc-two", stream.Volumes[1].Name)
	})

	t.Run("JSON", func(t *testing.T) {
		stream, err := decodeObjects(strings.NewReader(
			`{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"pvc-json"}}`))
		require.NoError(t, err)
		require.Len(t, stream.Volumes, 1)
		assert.Equal(t, "pvc-json", stream.Volumes[0].Name)
	})

	// Named rather than ignored: silently dropping an object the operator
	// piped in is how a DR run ends up half done.
	t.Run("something else is refused by name", func(t *testing.T) {
		_, err := decodeObjects(strings.NewReader(
			"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nope\n"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported object")
	})

	// A List's items are decoded one by one, without a stream decoder each;
	// every check a top-level document gets, an item gets too.
	t.Run("something else inside a List is refused by name", func(t *testing.T) {
		_, err := decodeObjects(strings.NewReader(`{"apiVersion":"v1","kind":"List","items":[` +
			`{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"pvc-ok"}},` +
			`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"nope"}}]}`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported object")
	})

	// A single JSON List is unmarshalled whole; every other JSON
	// input still streams, and must read the same.
	t.Run("JSON input on the whole-List path and off it", func(t *testing.T) {
		const pv = `{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"%s"}}`
		list := `{"apiVersion":"v1","kind":"List","items":[` + fmt.Sprintf(pv, "pvc-a") + `,` + fmt.Sprintf(pv, "pvc-b") + `]}`
		for name, tc := range map[string]struct {
			input string
			want  []string
		}{
			"one List, indented":            {"\n\t  " + list + "\n", []string{"pvc-a", "pvc-b"}},
			"a List, then another document": {list + "\n" + fmt.Sprintf(pv, "pvc-c"), []string{"pvc-a", "pvc-b", "pvc-c"}},
			"two bare objects":              {fmt.Sprintf(pv, "pvc-a") + fmt.Sprintf(pv, "pvc-b"), []string{"pvc-a", "pvc-b"}},
			"empty input":                   {"  \n", nil},
		} {
			t.Run(name, func(t *testing.T) {
				stream, err := decodeObjects(strings.NewReader(tc.input))
				require.NoError(t, err)
				var got []string
				for _, v := range stream.Volumes {
					got = append(got, v.Name)
				}
				assert.Equal(t, tc.want, got)
			})
		}
	})

	t.Run("a List of every kind the tool reads, nested once", func(t *testing.T) {
		const list = `
apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: PersistentVolumeClaim
  metadata: {name: data, namespace: apps}
  spec: {volumeName: pvc-one}
- apiVersion: replication.storage.openshift.io/v1alpha1
  kind: VolumeReplication
  metadata: {name: data-vr, namespace: apps}
  spec:
    volumeReplicationClass: qumulo-replication
    replicationState: primary
    dataSource: {apiGroup: "", kind: PersistentVolumeClaim, name: data}
- apiVersion: v1
  kind: Pod
  metadata: {name: writer, namespace: apps}
- apiVersion: v1
  kind: List
  items:
  - apiVersion: v1
    kind: PersistentVolume
    metadata: {name: pvc-one}
`
		stream, err := decodeObjects(strings.NewReader(list))
		require.NoError(t, err)
		assert.Len(t, stream.Claims, 1)
		require.Len(t, stream.Replications, 1)
		assert.Equal(t, "data", stream.Replications[0].ClaimName)
		assert.Len(t, stream.Pods, 1)
		assert.True(t, stream.PodsObserved, "a pod inside a List still marks the input as having a pod list")
		require.Len(t, stream.Volumes, 1)
		assert.Equal(t, "pvc-one", stream.Volumes[0].Name)
	})
}
