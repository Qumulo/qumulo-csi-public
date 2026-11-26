//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDefect_FailoverPeakMemoryStaysWithinTheLine.
// Correct: `failover -f` holds the objects it decoded and nothing in
// proportion to the input besides -- neither the input whole nor a second
// copy of every item. The input is mirrored pairs as the 15th-folder sweep
// built them (source and DR claim, PV and VolumeReplication each), in the
// three shapes an operator pipes in, and the whole command runs at the
// collector setting main() starts it with.
//
// Two lines. Against the input: 3 times for `kubectl get -o json`, the shape
// the runbook asks for, and 5 times for compact JSON and YAML. Against the
// decoded objects, for every shape: 2.2 times. The second is the one that does not depend on how wordy the input
// is -- compact JSON and YAML are about half as long as kubectl's indented
// JSON for the same objects, so the same memory is a larger multiple of
// them -- and it is the one a whole-input copy breaks: on 10,000 pairs the
// reading this replaced peaked at 4.4 (indented JSON), 6.75 (compact) and
// 31.7 (YAML) times the input, where the streamed one peaks at about 1.8
// times the objects in all three (process RSS 2.1, 3.7 and 3.9 times the
// input).
func TestDefect_FailoverPeakMemoryStaysWithinTheLine(t *testing.T) {
	const pairs = 3000
	defer debug.SetGCPercent(debug.SetGCPercent(25))

	list := d101List(pairs)
	compact, err := json.Marshal(list)
	require.NoError(t, err)
	indented, err := json.MarshalIndent(list, "", "    ")
	require.NoError(t, err)
	var yaml strings.Builder
	d101YAML(&yaml, list, "", false)

	for _, input := range []struct {
		name  string
		data  []byte
		limit float64
	}{
		{"kubectl get -o json", indented, 3},
		{"compact JSON", compact, 5},
		{"kubectl get -o yaml", []byte(yaml.String()), 5},
	} {
		t.Run(input.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "input")
			require.NoError(t, os.WriteFile(path, input.data, 0o600))

			retained := d101Retained(t, input.data)

			var runErr error
			peak := dfxPeakHeapGrowth(func() {
				runErr = run([]string{"failover", "-f", path, "-namespace", "dr", "-dry-run=true"},
					strings.NewReader(""), io.Discard, io.Discard)
			})
			require.NoError(t, runErr)

			ratio := float64(peak) / float64(len(input.data))
			t.Logf("%d pairs, %.1f MiB of input: decoded objects %.1f MiB (%.2fx); peak heap growth %.1f MiB = %.2fx the input, %.2fx the objects",
				pairs, float64(len(input.data))/(1<<20), float64(retained)/(1<<20),
				float64(retained)/float64(len(input.data)), float64(peak)/(1<<20), ratio,
				float64(peak)/float64(retained))
			assert.LessOrEqual(t, ratio, input.limit,
				"failover held %.2fx its input at peak (line %.0fx)", ratio, input.limit)
			assert.LessOrEqual(t, float64(peak), 2.2*float64(retained),
				"failover held %.2fx the objects it decoded at peak: the input, or a copy of it, is held too",
				float64(peak)/float64(retained))
		})
	}
}

// d101Retained reports the heap the decoded objects of input hold.
func d101Retained(t *testing.T, input []byte) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	stream, err := decodeObjects(bytes.NewReader(input))
	require.NoError(t, err)
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(stream)
	return after.HeapAlloc - before.HeapAlloc
}

// d101List builds a v1 List of mirrored pairs: per pair, the source and DR
// claims, PVs and VolumeReplications, plus the class they name.
func d101List(pairs int) map[string]any {
	items := []any{map[string]any{
		"apiVersion": "replication.storage.openshift.io/v1alpha1", "kind": "VolumeReplicationClass",
		"metadata": map[string]any{"name": "qumulo-vrc"},
		"spec":     map[string]any{"provisioner": "csi.qumulo.com"},
	}}
	claim := func(name, volume string) map[string]any {
		return map[string]any{
			"apiVersion": "v1", "kind": "PersistentVolumeClaim",
			"metadata": map[string]any{"name": name, "namespace": "dr", "annotations": map[string]any{
				"pv.kubernetes.io/bind-completed":               "yes",
				"pv.kubernetes.io/bound-by-controller":          "yes",
				"volume.kubernetes.io/storage-provisioner":      "csi.qumulo.com",
				"volume.beta.kubernetes.io/storage-provisioner": "csi.qumulo.com",
			}},
			"spec": map[string]any{
				"accessModes": []any{"ReadWriteMany"}, "storageClassName": "qumulo-nfs3",
				"resources":  map[string]any{"requests": map[string]any{"storage": "1Gi"}},
				"volumeMode": "Filesystem", "volumeName": volume,
			},
		}
	}
	volume := func(name, server, claimName string, annotations map[string]any) map[string]any {
		annotations["pv.kubernetes.io/provisioned-by"] = "csi.qumulo.com"
		return map[string]any{
			"apiVersion": "v1", "kind": "PersistentVolume",
			"metadata": map[string]any{"name": name, "annotations": annotations},
			"spec": map[string]any{
				"accessModes": []any{"ReadWriteMany"}, "capacity": map[string]any{"storage": "1Gi"},
				"claimRef": map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim",
					"name": claimName, "namespace": "dr"},
				"csi": map[string]any{
					"driver": "csi.qumulo.com", "volumeHandle": server + "##/csi/volumes/" + name,
					"volumeAttributes": map[string]any{
						"server": server, "exportPath": "/csi/volumes/" + name,
						"realPath": "/csi/volumes/" + name, "protocol": "nfs3",
						"storage.kubernetes.io/csiProvisionerIdentity": "1760000000000-1234-csi.qumulo.com",
					},
				},
				"mountOptions":                  []any{"nfsvers=3", "hard"},
				"persistentVolumeReclaimPolicy": "Delete", "storageClassName": "qumulo-nfs3",
				"volumeMode": "Filesystem",
			},
			"status": map[string]any{"phase": "Bound"},
		}
	}
	replication := func(name, state string) map[string]any {
		return map[string]any{
			"apiVersion": "replication.storage.openshift.io/v1alpha1", "kind": "VolumeReplication",
			"metadata": map[string]any{"name": name, "namespace": "dr"},
			"spec": map[string]any{
				"autoResync": false, "replicationState": state, "volumeReplicationClass": "qumulo-vrc",
				"dataSource": map[string]any{"apiGroup": "", "kind": "PersistentVolumeClaim", "name": name},
			},
			"status": map[string]any{"state": "Primary", "message": "ok", "conditions": []any{
				map[string]any{"type": "Completed", "status": "True", "reason": "Promoted"},
				map[string]any{"type": "Degraded", "status": "False", "reason": "Healthy"},
			}},
		}
	}
	for i := 0; i < pairs; i++ {
		source, mirror := fmt.Sprintf("reg-%05d", i), fmt.Sprintf("reg-%05d-dr", i)
		sourcePV, mirrorPV := fmt.Sprintf("pvc-%08x-source", i), fmt.Sprintf("pvc-%08x-mirror", i)
		items = append(items,
			claim(source, sourcePV), claim(mirror, mirrorPV),
			volume(sourcePV, "array-a.example.com", source, map[string]any{}),
			volume(mirrorPV, "array-b.example.com", mirror, map[string]any{
				annotationMirroredFrom: sourcePV,
				annotationSourceHandle: "array-a.example.com##/csi/volumes/" + sourcePV,
				annotationSourceServer: "array-a.example.com",
			}),
			replication(source, "primary"), replication(mirror, "secondary"))
	}
	return map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{}, "items": items}
}

// d101YAML writes v as block YAML laid out the way kubectl prints it: keys
// sorted, a list's dashes at its key's indentation, every string quoted.
// afterDash says the line already holds a list item's "- ".
func d101YAML(w *strings.Builder, v any, indent string, afterDash bool) {
	switch v := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			if i > 0 || !afterDash {
				w.WriteString(indent)
			}
			fmt.Fprintf(w, "%s:", k)
			switch child := v[k].(type) {
			case map[string]any:
				if len(child) == 0 {
					w.WriteString(" {}\n")
					continue
				}
				w.WriteString("\n")
				d101YAML(w, child, indent+"  ", false)
			case []any:
				w.WriteString("\n")
				d101YAML(w, child, indent, false)
			default:
				w.WriteString(" ")
				d101YAML(w, child, indent, false)
			}
		}
	case []any:
		for _, item := range v {
			w.WriteString(indent + "- ")
			d101YAML(w, item, indent+"  ", true)
		}
	case string:
		fmt.Fprintf(w, "%q\n", v)
	default:
		fmt.Fprintf(w, "%v\n", v)
	}
}
