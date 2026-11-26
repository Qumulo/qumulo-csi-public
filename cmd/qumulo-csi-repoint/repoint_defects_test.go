//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

// Defect tests for the qumulo-csi-repoint failover tool. Each asserts the
// CORRECT behaviour, so it fails while the defect is present. Every helper
// carries the dfx prefix so this file compiles next to the fix branch's own
// tests.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	k8stesting "k8s.io/client-go/testing"
)

func dfxPair(n, sourceServer, mirrorServer string) failoverPair {
	pv := func(name string) *corev1.PersistentVolume {
		return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	vr := func(name string) volumeReplication {
		return volumeReplication{Namespace: "ns", Name: name, DesiredState: "primary"}
	}
	return failoverPair{
		Namespace: "ns", SourcePV: pv("pv" + n), MirrorPV: pv("pv" + n + "-dr"),
		SourceClaim: "c" + n, MirrorClaim: "c" + n + "-dr",
		SourceVR: vr("vr" + n), MirrorVR: vr("vr" + n + "-dr"),
		SourceServer: sourceServer, MirrorServer: mirrorServer,
	}
}

// ===========================================================================
// One DNS target must not be applied to records that need
// different ones.
// ===========================================================================

// TestDefect_DNSFlipDoesNotFoldTwoDifferentTargetsIntoOne.
// Correct: two pairs on one record whose promoted copies are on different
// arrays are refused (or warned about naming the second array), never planned
// unit-tests/defect_repoint_test.go TestDefectDNSFlipFoldsTwoDifferentTargetsIntoOne.
func TestDefect_DNSFlipDoesNotFoldTwoDifferentTargetsIntoOne(t *testing.T) {
	pairs := []failoverPair{dfxPair("1", "csi-a.lab", "10.0.0.2"), dfxPair("2", "csi-a.lab", "10.0.0.3")}
	plan, err := planFailover(pairs, directionFailover, failoverOptions{namespace: "ns", dnsTTL: 60, podsObserved: true})
	if err != nil {
		t.Logf("refused: %v", err)
		return
	}
	warned := false
	for _, w := range plan.Warnings {
		warned = warned || strings.Contains(w, "10.0.0.3")
	}
	assert.True(t, warned, "plan %+v points one record at one array for live copies on two, with no warning", plan.Flips)
}

// TestDefect_FailbackDoesNotPointEveryRecordAtTheOneDNSTarget.
// Correct: on failback of two records (csi-a.lab, csi-c.lab) one bare
// -dns-target is not applied to both; the tool refuses or takes a target per
// TestDefectFailbackPointsEveryRecordAtTheOneDNSTarget.
func TestDefect_FailbackDoesNotPointEveryRecordAtTheOneDNSTarget(t *testing.T) {
	pairs := []failoverPair{dfxPair("1", "csi-a.lab", "10.0.0.2"), dfxPair("2", "csi-c.lab", "10.0.0.2")}
	plan, err := planFailover(pairs, directionFailback,
		failoverOptions{namespace: "ns", dnsTTL: 60, podsObserved: true, dnsTarget: "10.0.0.1"})
	if err != nil {
		t.Logf("refused: %v", err)
		return
	}
	targets := map[string]string{}
	for _, f := range plan.Flips {
		targets[f.Record] = f.To
	}
	assert.False(t, targets["csi-a.lab"] == "10.0.0.1" && targets["csi-c.lab"] == "10.0.0.1",
		"both records pointed at the one -dns-target: %+v", plan.Flips)
}

// TestDefect_ExplicitDNSTargetDoesNotCollapseSplitLiveCopies.
// Correct: an explicit -dns-target does not bypass the split-record refusal.
func TestDefect_ExplicitDNSTargetDoesNotCollapseSplitLiveCopies(t *testing.T) {
	pairs := []failoverPair{dfxPair("1", "csi-a.lab", "192.0.2.2"), dfxPair("2", "csi-a.lab", "192.0.2.3")}
	for _, target := range []string{"192.0.2.2", "csi-a.lab=192.0.2.2"} {
		plan, err := planFailover(pairs, directionFailover, failoverOptions{namespace: "ns", dnsTTL: 60, podsObserved: true, dnsTarget: target})
		assert.Error(t, err, "-dns-target %q: plan %+v points one record at one array for live copies on two", target, plan.Flips)
	}
	// The original test's failback half (one target for DR copies on two
	// arrays) is not ported: on failback both live copies return to the one
	// array the record named before, so one target is consistent there, and
	// neither report states that case as wrong.
}

// ===========================================================================
// Paged LISTs, and a bounded decode of the input.
// ===========================================================================

func dfxPV(i int, driver string) corev1.PersistentVolume {
	name := fmt.Sprintf("pvc-%05d", i)
	return corev1.PersistentVolume{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			StorageClassName:              "qumulo-nfs",
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			ClaimRef:                      &corev1.ObjectReference{Kind: "PersistentVolumeClaim", Namespace: "apps", Name: "data-" + name},
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver:       driver,
				VolumeHandle: "qumulo.example.com##/csi/volumes/" + name,
				VolumeAttributes: map[string]string{
					"server": "qumulo.example.com", "exportPath": "/csi/volumes/" + name,
					"realPath": "/csi/volumes/" + name, "protocol": "nfs4.1",
				},
			}},
		},
	}
}

// TestDefect_PersistentVolumeListIsPaged.
// Correct: the tool lists PersistentVolumes in bounded pages (Limit set,
// Continue followed), not one unpaged whole-cluster LIST, and still returns
// cmd/qumulo-csi-repoint/cluster.go listVolumes.
func TestDefect_PersistentVolumeListIsPaged(t *testing.T) {
	const total = 1200
	all := make([]corev1.PersistentVolume, 0, total)
	for i := 0; i < total; i++ {
		driver := "csi.qumulo.com"
		if i%2 == 1 {
			driver = "other.example.com"
		}
		all = append(all, dfxPV(i, driver))
	}

	var (
		mu     sync.Mutex
		limits []int64
	)
	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor("list", "persistentvolumes", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		opts := action.(k8stesting.ListActionImpl).ListOptions
		mu.Lock()
		limits = append(limits, opts.Limit)
		mu.Unlock()
		start := 0
		if opts.Continue != "" {
			start, _ = strconv.Atoi(opts.Continue)
		}
		end := total
		if opts.Limit > 0 && start+int(opts.Limit) < total {
			end = start + int(opts.Limit)
		}
		list := &corev1.PersistentVolumeList{Items: append([]corev1.PersistentVolume(nil), all[start:end]...)}
		if end < total {
			list.Continue = strconv.Itoa(end)
		}
		return true, list, nil
	})

	k := &kubeClient{clientset: clientset}
	got, err := k.listVolumes(context.Background(), "csi.qumulo.com")
	require.NoError(t, err)
	t.Logf("%d LIST calls with limits %v", len(limits), limits)

	assert.Len(t, got, total/2, "every PV of this driver, across pages")
	require.NotEmpty(t, limits)
	for i, l := range limits {
		assert.Positive(t, l, "LIST %d of PersistentVolumes is unpaged (no limit)", i)
	}
}

// dfxKubeAPI is a minimal HTTP stand-in for the Kubernetes API: it serves PVs
// and pods, honours limit/continue, and records every request.
type dfxKubeAPI struct {
	mu       sync.Mutex
	requests []string
	pvs      []corev1.PersistentVolume
	pods     []corev1.Pod
}

func (k *dfxKubeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	k.mu.Lock()
	k.requests = append(k.requests, r.Method+" "+r.URL.RequestURI())
	k.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	start, _ := strconv.Atoi(r.URL.Query().Get("continue"))
	page := func(n int) (int, int, string) {
		end := n
		if limit > 0 && start+limit < n {
			end = start + limit
		}
		cont := ""
		if end < n {
			cont = strconv.Itoa(end)
		}
		return start, end, cont
	}
	p := r.URL.Path
	switch {
	case r.Method == http.MethodGet && p == "/api/v1/persistentvolumes":
		s, e, c := page(len(k.pvs))
		_ = json.NewEncoder(w).Encode(&corev1.PersistentVolumeList{
			TypeMeta: metav1.TypeMeta{Kind: "PersistentVolumeList", APIVersion: "v1"},
			ListMeta: metav1.ListMeta{Continue: c}, Items: k.pvs[s:e]})
	case r.Method == http.MethodGet && (p == "/api/v1/pods" || strings.HasPrefix(p, "/api/v1/namespaces/") && strings.HasSuffix(p, "/pods")):
		var in []corev1.Pod
		for _, pod := range k.pods {
			if p == "/api/v1/pods" || p == "/api/v1/namespaces/"+pod.Namespace+"/pods" {
				in = append(in, pod)
			}
		}
		s, e, c := page(len(in))
		_ = json.NewEncoder(w).Encode(&corev1.PodList{
			TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"},
			ListMeta: metav1.ListMeta{Continue: c}, Items: in[s:e]})
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`))
	}
}

func dfxKubeconfig(t *testing.T, server string) string {
	t.Helper()
	cfg := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: dfx
  cluster:
    server: %s
contexts:
- name: dfx
  context:
    cluster: dfx
    user: dfx
current-context: dfx
users:
- name: dfx
  user:
    token: dfx-token
`, server)
	p := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(p, []byte(cfg), 0o600))
	return p
}

// TestDefect_SwitchListsNoUnpagedOrClusterWideLists.
// Correct: `switch` against a cluster pages every LIST it makes (limit set)
// and does not list the pods of every namespace; it lists only where the
func TestDefect_SwitchListsNoUnpagedOrClusterWideLists(t *testing.T) {
	api := &dfxKubeAPI{}
	pv := dfxPV(1, "csi.qumulo.com")
	api.pvs = []corev1.PersistentVolume{pv, dfxPV(2, "other.example.com")}
	api.pods = []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "writer"},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "d", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-pvc-00001"}}}}}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "unrelated", Name: "bystander"}},
	}
	kube := httptest.NewServer(api)
	defer kube.Close()

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"777","path":"/csi/volumes/pvc-00001","type":"FS_FILE_TYPE_DIRECTORY"}`))
	}))
	defer peer.Close()
	token := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(token, []byte("peer-token\n"), 0o600))

	var stdout, stderr bytes.Buffer
	err := run([]string{
		"switch",
		"-kubeconfig", dfxKubeconfig(t, kube.URL),
		"-peer-server", "c2.example.com",
		"-peer-api-url", peer.URL,
		"-peer-token-file", token,
	}, strings.NewReader(""), &stdout, &stderr)
	t.Logf("switch: err=%v\nrequests: %v\nstderr: %s", err, api.requests, stderr.String())
	require.NoError(t, err)

	lists := 0
	for _, req := range api.requests {
		if !strings.HasPrefix(req, "GET ") {
			continue
		}
		lists++
		path, query, _ := strings.Cut(strings.TrimPrefix(req, "GET "), "?")
		assert.NotEqual(t, "/api/v1/pods", path, "pods were listed across every namespace")
		assert.Contains(t, "&"+query, "&limit=", "unpaged LIST: %s", req)
	}
	assert.Positive(t, lists, "switch made no LIST at all")
}

// TestDefect_DecodingInputDecodesEachObjectOnce.
// Correct: a manifest is decoded once per object (the report's fix: probe the
// kind with a TypeMeta unmarshal, decode List.Items once), not twice plus
// string re-copies. Measured as bytes allocated by decodeObjects against the
// irreducible cost on the same input -- one json unmarshal of the List and one
// typed decode of each item with client-go's own deserializer -- allowing 3x
// that for YAML splitting and the kind probe. Peak heap growth is logged for:
// 103-127, :172).
func TestDefect_DecodingInputDecodesEachObjectOnce(t *testing.T) {
	const n = 2000
	items := make([]k8sruntime.RawExtension, 0, 2*n)
	for i := 0; i < n; i++ {
		pv := dfxPV(i, "csi.qumulo.com")
		raw, err := json.Marshal(&pv)
		require.NoError(t, err)
		items = append(items, k8sruntime.RawExtension{Raw: raw})
		claim := corev1.PersistentVolumeClaim{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "data-" + pv.Name},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
				VolumeName:  pv.Name,
			},
		}
		raw, err = json.Marshal(&claim)
		require.NoError(t, err)
		items = append(items, k8sruntime.RawExtension{Raw: raw})
	}
	input, err := json.Marshal(&corev1.List{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "List"}, Items: items})
	require.NoError(t, err)
	items = nil //nolint:ineffassign // released so the measurements below do not count it as live

	once := dfxAllocated(func() {
		var list corev1.List
		require.NoError(t, json.Unmarshal(input, &list))
		for i := range list.Items {
			_, _, err := scheme.Codecs.UniversalDeserializer().Decode(list.Items[i].Raw, nil, nil)
			require.NoError(t, err)
		}
	})
	var stream objectStream
	tool := dfxAllocated(func() {
		stream, err = decodeObjects(bytes.NewReader(input))
	})
	require.NoError(t, err)
	require.Len(t, stream.Volumes, n)
	require.Len(t, stream.Claims, n)
	peak := dfxPeakHeapGrowth(func() {
		s, err := decodeObjects(bytes.NewReader(input))
		require.NoError(t, err)
		runtime.KeepAlive(s)
	})
	in := float64(len(input))
	t.Logf("input %.1f MiB: decodeObjects allocated %.1fx the input, one typed decode %.1fx (ratio %.1f); peak heap growth %.1fx",
		in/(1<<20), float64(tool)/in, float64(once)/in, float64(tool)/float64(once), float64(peak)/in)

	assert.Less(t, float64(tool), 3*float64(once),
		"decodeObjects allocated %.1fx what decoding each object once costs", float64(tool)/float64(once))
}

// TestDefect_YAMLListDecodesTheSameObjects: the shape
// operators pipe in most is `kubectl get pv,pvc -o yaml`. The same 2,000 PVs
// and claims as YAML decode to the same objects as the JSON form; the
// allocation ratio is logged, not limited, because YAML has to be converted to
func TestDefect_YAMLListDecodesTheSameObjects(t *testing.T) {
	const n = 2000
	var y strings.Builder
	y.WriteString("apiVersion: v1\nkind: List\nitems:\n")
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("pvc-%05d", i)
		fmt.Fprintf(&y, `- apiVersion: v1
  kind: PersistentVolume
  metadata:
    name: %[1]s
  spec:
    accessModes: [ReadWriteMany]
    storageClassName: qumulo-nfs
    persistentVolumeReclaimPolicy: Retain
    claimRef: {kind: PersistentVolumeClaim, namespace: apps, name: data-%[1]s}
    csi:
      driver: csi.qumulo.com
      volumeHandle: qumulo.example.com##/csi/volumes/%[1]s
      volumeAttributes: {server: qumulo.example.com, exportPath: /csi/volumes/%[1]s, realPath: /csi/volumes/%[1]s, protocol: nfs4.1}
- apiVersion: v1
  kind: PersistentVolumeClaim
  metadata: {namespace: apps, name: data-%[1]s}
  spec: {accessModes: [ReadWriteMany], volumeName: %[1]s}
`, name)
	}
	input := []byte(y.String())

	var stream objectStream
	var err error
	allocated := dfxAllocated(func() { stream, err = decodeObjects(bytes.NewReader(input)) })
	require.NoError(t, err)
	require.Len(t, stream.Volumes, n)
	require.Len(t, stream.Claims, n)
	for i, pv := range stream.Volumes {
		name := fmt.Sprintf("pvc-%05d", i)
		require.Equal(t, name, pv.Name)
		require.Equal(t, "qumulo.example.com##/csi/volumes/"+name, pv.Spec.CSI.VolumeHandle)
		require.Equal(t, name, stream.Claims[i].Spec.VolumeName)
	}
	t.Logf("YAML input %.1f MiB: decodeObjects allocated %.1fx the input",
		float64(len(input))/(1<<20), float64(allocated)/float64(len(input)))
}

// dfxAllocated reports the bytes f allocates.
func dfxAllocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// dfxPeakHeapGrowth runs f and reports the highest heap-in-use above the
// starting point, sampled every millisecond.
func dfxPeakHeapGrowth(f func()) uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapInuse
	var peak uint64
	done := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var m runtime.MemStats
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			runtime.ReadMemStats(&m)
			if m.HeapInuse > base && m.HeapInuse-base > peak {
				peak = m.HeapInuse - base
			}
			select {
			case <-done:
				return
			case <-tick.C:
			}
		}
	}()
	f()
	close(done)
	<-sampled
	return peak
}
