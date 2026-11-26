// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The input is read a List item at a time. These pin that the
// streamed reading reads every shape the whole-document reading did, and
// refuses what it refused.

// streamPV and streamPVC are one PV and its claim as JSON items.
const (
	streamPV = `{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"pv-a",` +
		`"annotations":{"pv.kubernetes.io/provisioned-by":"csi.qumulo.com","csi.qumulo.com/mirrored-from":"pv-src"},` +
		`"labels":{"app":"reg"},"managedFields":[{"manager":"kube-controller-manager","operation":"Update"}]},` +
		`"spec":{"accessModes":["ReadWriteMany"],"capacity":{"storage":"1Gi"},` +
		`"claimRef":{"kind":"PersistentVolumeClaim","namespace":"apps","name":"data-a"},` +
		`"csi":{"driver":"csi.qumulo.com","volumeHandle":"qumulo.example.com##/csi/volumes/pv-a",` +
		`"volumeAttributes":{"server":"qumulo.example.com","realPath":"/csi/volumes/pv-a"}}},` +
		`"status":{"phase":"Bound"}}`
	streamPVC = `{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"namespace":"apps","name":"data-a",` +
		`"annotations":{"kubectl.kubernetes.io/last-applied-configuration":"{\"a\":1}"}},` +
		`"spec":{"accessModes":["ReadWriteMany"],"volumeName":"pv-a"},"status":{"phase":"Bound"}}`
)

func TestDecodeObjectsReadsAJSONListWhateverItsKeyOrder(t *testing.T) {
	for name, input := range map[string]string{
		// kubectl prints the keys sorted, so items comes before kind.
		"items before kind": `{"apiVersion":"v1","items":[` + streamPV + `,` + streamPVC + `],"kind":"List","metadata":{"resourceVersion":""}}`,
		"kind before items": `{"kind":"List","apiVersion":"v1","metadata":{},"items":[` + streamPV + `,` + streamPVC + `]}`,
		"indented": "{\n    \"apiVersion\": \"v1\",\n    \"items\": [\n        " + streamPV + ",\n        " + streamPVC +
			"\n    ],\n    \"kind\": \"List\"\n}\n",
	} {
		t.Run(name, func(t *testing.T) {
			stream, err := decodeObjects(strings.NewReader(input))
			require.NoError(t, err)
			require.Len(t, stream.Volumes, 1)
			require.Len(t, stream.Claims, 1)
			assert.Equal(t, "pv-a", stream.Volumes[0].Name)
			assert.Equal(t, "qumulo.example.com##/csi/volumes/pv-a", stream.Volumes[0].Spec.CSI.VolumeHandle)
			assert.Equal(t, "pv-a", stream.Claims[0].Spec.VolumeName)
		})
	}
}

// encoding/json, which read the List before, matches a field whatever its
// case; so does the streamed reading.
func TestDecodeObjectsReadsAMiscasedJSONItemsKey(t *testing.T) {
	stream, err := decodeObjects(strings.NewReader(`{"apiVersion":"v1","kind":"List","Items":[` + streamPV + `]}`))
	require.NoError(t, err)
	assert.Len(t, stream.Volumes, 1)
}

func TestDecodeObjectsReadsSeveralJSONObjectsInOneInput(t *testing.T) {
	input := `{"apiVersion":"v1","kind":"List","items":[` + streamPV + `]}` + "\n" + streamPVC + "\n"
	stream, err := decodeObjects(strings.NewReader(input))
	require.NoError(t, err)
	assert.Len(t, stream.Volumes, 1)
	assert.Len(t, stream.Claims, 1)
}

func TestDecodeObjectsReadsEmptyAndNullLists(t *testing.T) {
	for name, input := range map[string]string{
		"empty input":     "",
		"white space":     " \n\t\n",
		"empty JSON list": `{"apiVersion":"v1","kind":"List","items":[]}`,
		"null JSON items": `{"apiVersion":"v1","kind":"List","items":null}`,
		"null JSON item":  `{"apiVersion":"v1","kind":"List","items":[null]}`,
		"empty YAML list": "apiVersion: v1\nkind: List\nitems: []\n",
		"bare YAML items": "apiVersion: v1\nitems:\nkind: List\n",
		"only separators": "---\n---\n",
		"only a comment":  "# nothing here\n",
	} {
		t.Run(name, func(t *testing.T) {
			stream, err := decodeObjects(strings.NewReader(input))
			require.NoError(t, err)
			assert.Empty(t, stream.Volumes)
			assert.Empty(t, stream.Claims)
		})
	}
}

// A typed list is not the List wrapper kubectl prints, and was refused for
// what it is when the input was read whole; it still is, in both formats,
// even though its items are read before its kind.
func TestDecodeObjectsStillRefusesATypedList(t *testing.T) {
	for name, input := range map[string]string{
		"JSON": `{"apiVersion":"v1","items":[` + streamPV + `],"kind":"PersistentVolumeList"}`,
		"YAML": "apiVersion: v1\nitems:\n- apiVersion: v1\n  kind: PersistentVolume\n  metadata:\n    name: pv-a\n" +
			"kind: PersistentVolumeList\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeObjects(strings.NewReader(input))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unsupported object *v1.PersistentVolumeList")
		})
	}
}

func TestDecodeObjectsNamesAnItemItCannotRead(t *testing.T) {
	service := `{"apiVersion":"v1","kind":"Service","metadata":{"name":"s"}}`
	for name, input := range map[string]string{
		"JSON": `{"apiVersion":"v1","items":[` + streamPV + `,` + service + `],"kind":"List"}`,
		"YAML": "apiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: Service\n  metadata:\n    name: s\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeObjects(strings.NewReader(input))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unsupported object *v1.Service")
		})
	}
}

// Every layout of a YAML List an operator is likely to pipe in reads to the
// same two objects.
func TestDecodeObjectsReadsEveryYAMLListLayout(t *testing.T) {
	const pv = `apiVersion: v1
  kind: PersistentVolume
  metadata:
    name: pv-a
    annotations:
      csi.qumulo.com/mirrored-from: pv-src
  spec:
    accessModes: [ReadWriteMany]
    claimRef: {kind: PersistentVolumeClaim, namespace: apps, name: data-a}
    csi:
      driver: csi.qumulo.com
      volumeHandle: qumulo.example.com##/csi/volumes/pv-a
      volumeAttributes: {server: qumulo.example.com, realPath: /csi/volumes/pv-a}
`
	const pvc = `apiVersion: v1
  kind: PersistentVolumeClaim
  metadata: {namespace: apps, name: data-a}
  spec: {accessModes: [ReadWriteMany], volumeName: pv-a}
`
	indent := func(s string) string { return strings.ReplaceAll(s, "\n  ", "\n    ") }

	for name, input := range map[string]string{
		"kubectl": "apiVersion: v1\nitems:\n- " + pv + "- " + pvc + "kind: List\nmetadata:\n  resourceVersion: \"\"\n",
		"kind first, items last, no newline at the end": strings.TrimSuffix(
			"apiVersion: v1\nkind: List\nitems:\n- "+pv+"- "+pvc, "\n"),
		"indented sequence": "apiVersion: v1\nkind: List\nitems:\n  - " + indent(pv) + "  - " + indent(pvc),
		"comments and blank lines between items": "apiVersion: v1\nkind: List\nitems:\n# the volume\n- " + pv +
			"\n   # the claim\n\n- " + pvc + "# end\n",
		"a dash on a line of its own": "apiVersion: v1\nkind: List\nitems:\n-\n  " + pv + "-\n  " + pvc,
		"a block text that looks like keys": "apiVersion: v1\nkind: List\nitems:\n- " +
			strings.Replace(pv, "    annotations:\n", "    annotations:\n      note: |\n        kind: List\n        - not an item\n", 1) +
			"- " + pvc,
		"Windows line endings":                  strings.ReplaceAll("apiVersion: v1\nkind: List\nitems:\n- "+pv+"- "+pvc, "\n", "\r\n"),
		"a document separator before and after": "---\napiVersion: v1\nkind: List\nitems:\n- " + pv + "- " + pvc + "---\n",
		"items inline as a flow sequence": "apiVersion: v1\nkind: List\nitems: [{" +
			`apiVersion: v1, kind: PersistentVolume, metadata: {name: pv-a, annotations: {csi.qumulo.com/mirrored-from: pv-src}},` +
			` spec: {accessModes: [ReadWriteMany], claimRef: {kind: PersistentVolumeClaim, namespace: apps, name: data-a},` +
			` csi: {driver: csi.qumulo.com, volumeHandle: "qumulo.example.com##/csi/volumes/pv-a",` +
			` volumeAttributes: {server: qumulo.example.com, realPath: /csi/volumes/pv-a}}}}, {` +
			`apiVersion: v1, kind: PersistentVolumeClaim, metadata: {namespace: apps, name: data-a},` +
			` spec: {accessModes: [ReadWriteMany], volumeName: pv-a}}]` + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			stream, err := decodeObjects(strings.NewReader(input))
			require.NoError(t, err)
			require.Len(t, stream.Volumes, 1)
			require.Len(t, stream.Claims, 1)
			assert.Equal(t, "pv-a", stream.Volumes[0].Name)
			assert.Equal(t, "qumulo.example.com##/csi/volumes/pv-a", stream.Volumes[0].Spec.CSI.VolumeHandle)
			assert.Equal(t, "pv-src", stream.Volumes[0].Annotations[annotationMirroredFrom])
			assert.Equal(t, "pv-a", stream.Claims[0].Spec.VolumeName)
		})
	}
}

func TestDecodeObjectsReadsAYAMLListAmongOtherDocuments(t *testing.T) {
	input := "apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata: {namespace: apps, name: data-b}\n" +
		"spec: {volumeName: pv-b}\n---\n" +
		"apiVersion: v1\nitems:\n- apiVersion: v1\n  kind: PersistentVolume\n  metadata:\n    name: pv-a\n" +
		"kind: List\n---\n" +
		"apiVersion: v1\nkind: PersistentVolume\nmetadata:\n  name: pv-b\n"
	stream, err := decodeObjects(strings.NewReader(input))
	require.NoError(t, err)
	require.Len(t, stream.Volumes, 2)
	require.Len(t, stream.Claims, 1)
	assert.Equal(t, "pv-a", stream.Volumes[0].Name)
	assert.Equal(t, "pv-b", stream.Volumes[1].Name)
}

// Items are read one at a time, so an anchor in one item cannot be used by
// another. Refused, and the refusal says how to get past it.
func TestDecodeObjectsNamesAYAMLAnchorUsedAcrossItems(t *testing.T) {
	input := "apiVersion: v1\nkind: List\nitems:\n" +
		"- apiVersion: v1\n  kind: PersistentVolume\n  metadata:\n    name: pv-a\n    labels: &shared {app: reg}\n" +
		"- apiVersion: v1\n  kind: PersistentVolume\n  metadata:\n    name: pv-b\n    labels: *shared\n"
	_, err := decodeObjects(strings.NewReader(input))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-o json")
}

// What no subcommand reads is not kept: every object is held until the
// command exits. What is read -- this driver's annotations, the
// labels a mirror copies -- is.
func TestDecodeObjectsKeepsOnlyWhatTheSubcommandsRead(t *testing.T) {
	stream, err := decodeObjects(strings.NewReader(
		`{"apiVersion":"v1","kind":"List","items":[` + streamPV + `,` + streamPVC + `]}`))
	require.NoError(t, err)
	require.Len(t, stream.Volumes, 1)
	require.Len(t, stream.Claims, 1)

	pv := stream.Volumes[0]
	assert.Equal(t, map[string]string{annotationMirroredFrom: "pv-src"}, pv.Annotations)
	assert.Equal(t, map[string]string{"app": "reg"}, pv.Labels)
	assert.Nil(t, pv.ManagedFields)
	assert.Empty(t, pv.Status.Phase)
	assert.Equal(t, "qumulo.example.com", pv.Spec.CSI.VolumeAttributes[volumeAttributeServer])

	claim := stream.Claims[0]
	assert.Nil(t, claim.Annotations, "last-applied-configuration is a second copy of the object")
	assert.Empty(t, claim.Status.Phase)
}

// Shapes a fuzz run comparing the streamed reading with the whole-document
// one turned up, pinned so that they keep reading the same.
func TestDecodeObjectsReadsYAMLEdgeShapesAsTheWholeDocumentReaderDid(t *testing.T) {
	t.Run("a flow mapping in an item that closes at the margin", func(t *testing.T) {
		input := "apiVersion: v1\nkind: List\nitems:\n  - apiVersion: v1\n    kind: PersistentVolume\n" +
			"    metadata: {name: pv-a,\n}\n"
		stream, err := decodeObjects(strings.NewReader(input))
		require.NoError(t, err)
		require.Len(t, stream.Volumes, 1)
		assert.Equal(t, "pv-a", stream.Volumes[0].Name)
	})
	t.Run("a flow mapping document is YAML, not broken JSON", func(t *testing.T) {
		stream, err := decodeObjects(strings.NewReader("{apiVersion: v1, kind: PersistentVolume, metadata: {name: pv-a}}\n"))
		require.NoError(t, err)
		require.Len(t, stream.Volumes, 1)
	})
	t.Run("old Mac line endings", func(t *testing.T) {
		stream, err := decodeObjects(strings.NewReader(
			"apiVersion: v1\rkind: List\ritems:\r- apiVersion: v1\r  kind: PersistentVolume\r  metadata:\r    name: pv-a\r"))
		require.NoError(t, err)
		require.Len(t, stream.Volumes, 1)
	})
	t.Run("items:# is a key of its own, not items with a comment", func(t *testing.T) {
		_, err := decodeObjects(strings.NewReader("items:#\napiVersion: v1\nkind: List\n"))
		require.Error(t, err)
	})
	t.Run("a bare items: alone is still a document", func(t *testing.T) {
		_, err := decodeObjects(strings.NewReader("items:\n"))
		require.Error(t, err, "an object with no kind, as before")
	})
	t.Run("a separator with text after it", func(t *testing.T) {
		_, err := decodeObjects(strings.NewReader("--- kind: List\n"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid Yaml document separator")
	})
	t.Run("a line inside items: indented less than its items", func(t *testing.T) {
		_, err := decodeObjects(strings.NewReader(
			"apiVersion: v1\nkind: List\nitems:\n  - apiVersion: v1\n    kind: PersistentVolume\n x: 1\n"))
		require.Error(t, err)
	})
	t.Run("a List whose own metadata is not an object", func(t *testing.T) {
		_, err := decodeObjects(strings.NewReader(
			"apiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: PersistentVolume\n  metadata: {name: a}\nmetadata: 0\n"))
		require.Error(t, err)
	})
}

// Two `items` are refused: which list a List is made of would be a guess.
func TestDecodeObjectsRefusesTwoItemsKeys(t *testing.T) {
	for name, input := range map[string]string{
		"JSON":                `{"apiVersion":"v1","kind":"List","items":[` + streamPV + `],"items":[]}`,
		"YAML":                "apiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: PersistentVolume\n  metadata: {name: a}\nitems: []\n",
		"YAML, spelled apart": "apiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: PersistentVolume\n  metadata: {name: a}\n\"items\": []\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeObjects(strings.NewReader(input))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "more than one items")
		})
	}
}

// JSON is read as JSON throughout. A file that turns into YAML part way --
// which the whole-document reader guessed at -- is refused with what to do.
func TestDecodeObjectsRefusesJSONThatTurnsIntoYAML(t *testing.T) {
	_, err := decodeObjects(strings.NewReader(streamPV + "\n---\napiVersion: v1\nkind: PersistentVolume\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-f is repeatable")
}
