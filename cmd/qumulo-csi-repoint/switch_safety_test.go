// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// `switch -dry-run=false` deletes the source claims of IP-named volumes
// and applies replacements bound to the mirrored PVs. A claim a pod still
// mounts is not deleted -- kubernetes.io/pvc-protection leaves it Terminating
// -- and the replacement is then applied over it. The command checked for such
// pods only when it had read them, and carried on when it had not: with -f it
// never read them, and a client it could not build or a pod list that failed
// was skipped as "best effort". Then it reported the AlreadyExists of the
// replacement, which was the half-deleted original, as "exists".

// ipSourcePV is the source PV of an IP-named class, Retain so the plan has no
// warning of its own to refuse over.
func ipSourcePV() *corev1.PersistentVolume {
	pv := sourcePV()
	pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	pv.Spec.CSI.VolumeAttributes[volumeAttributeServer] = "10.0.0.1"
	pv.Spec.CSI.VolumeHandle = "10.0.0.1##/csi/volumes/pvc-abc123"
	return pv
}

// sourceClaim is the claim the source PV is bound to.
func sourceClaim() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "data"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pvc-abc123"},
	}
}

// podMounting is a pod in apps mounting one claim.
func podMounting(name, claim string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: name},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "d",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
			},
		}}},
	}
}

// stubKubeClients makes kubeClientFor hand out client, except on the calls
// (1-based) listed in failOn, which fail the way an expired exec credential
// does. Restored when the test ends.
func stubKubeClients(t *testing.T, client *fake.Clientset, failOn ...int) {
	t.Helper()
	saved := kubeClientFor
	t.Cleanup(func() { kubeClientFor = saved })

	calls := 0
	kubeClientFor = func(string) (*kubeClient, error) {
		calls++
		for _, n := range failOn {
			if n == calls {
				return nil, errors.New("building a Kubernetes client: getting credentials: exec: " +
					"executable aws failed with exit code 255")
			}
		}
		return &kubeClient{clientset: client}, nil
	}
}

// switchOptions is an apply (dry run off) against the stand-in peer array,
// with a claim wait short enough for a unit test.
func switchOptions(t *testing.T) options {
	t.Helper()
	peer, _ := fakePeerArray(t, map[string]string{"%2Fcsi%2Fvolumes%2Fpvc-abc123": "60129542144"})
	return options{
		driverName:   "csi.qumulo.com",
		peerServer:   "192.0.2.20",
		peerAPIURL:   peer.URL,
		peerToken:    writeTempFile(t, "token", testToken),
		pvSuffix:     "-dr",
		dryRun:       false,
		waitFor:      50 * time.Millisecond,
		pollInterval: 5 * time.Millisecond,
	}
}

// sourceManifestFile is the source PV saved with `kubectl get pv -o yaml`.
func sourceManifestFile(t *testing.T) string {
	t.Helper()
	var b bytes.Buffer
	require.NoError(t, writeManifest(&b, ipSourcePV()))
	return writeTempFile(t, "pvs.yaml", b.String())
}

// claimDeletes is every DELETE of a PersistentVolumeClaim the fake saw.
func claimDeletes(client *fake.Clientset) []string {
	var out []string
	for _, a := range client.Actions() {
		if a.GetVerb() == "delete" && a.GetResource().Resource == "persistentvolumeclaims" {
			out = append(out, a.GetNamespace()+"/"+a.(k8stesting.DeleteAction).GetName())
		}
	}
	return out
}

// claimCreates is every CREATE of a PersistentVolumeClaim the fake saw.
func claimCreates(client *fake.Clientset) int {
	n := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == "create" && a.GetResource().Resource == "persistentvolumeclaims" {
			n++
		}
	}
	return n
}

// TestSwitchRefusesToDeleteClaimsWhenThePodListWasNotRead. Before the fix, a pod LIST answering 500 still sent the DELETE and printed
// "deleted PVC", and with -f the pods were never listed at all. A claim is
// deleted only after the pods using it were read from the cluster and none
// was found; anything else refuses before the first DELETE.
func TestSwitchRefusesToDeleteClaimsWhenThePodListWasNotRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup arranges the run; it returns the fake the deletes would land on.
		setup func(t *testing.T, opt *options) *fake.Clientset
		// wantErr is what the refusal must say.
		wantErr []string
		// wantStderr is what the operator is told to do instead.
		wantStderr []string
	}{
		{
			name: "-f given, so the pods were never read",
			setup: func(t *testing.T, opt *options) *fake.Clientset {
				opt.files = []string{sourceManifestFile(t)}
				client := fake.NewClientset(sourceClaim(), podMounting("writer", "data"))
				stubKubeClients(t, client)
				return client
			},
			wantErr: []string{"-f", "nothing was deleted"},
			wantStderr: []string{
				"kubectl --namespace apps describe pvc data",
				"kubectl --namespace apps delete pvc data --wait=true",
				"kubectl --namespace apps get pvc data",
				"kubectl get pv pvc-abc123-dr",
				"kubectl apply -f",
			},
		},
		{
			name: "the client for the pod check could not be built",
			setup: func(t *testing.T, opt *options) *fake.Clientset {
				client := fake.NewClientset(ipSourcePV(), sourceClaim(), podMounting("writer", "data"))
				// 1: listing the PVs. 2: the pod check. 3: the apply.
				stubKubeClients(t, client, 2)
				return client
			},
			wantErr: []string{"could not read the pods", "nothing was deleted", "exit code 255"},
		},
		{
			name: "the pod list failed",
			setup: func(t *testing.T, opt *options) *fake.Clientset {
				client := fake.NewClientset(ipSourcePV(), sourceClaim(), podMounting("writer", "data"))
				client.PrependReactor("list", "pods",
					func(k8stesting.Action) (bool, runtime.Object, error) {
						return true, nil, apierrors.NewInternalError(errors.New("etcdserver: request timed out"))
					})
				stubKubeClients(t, client)
				return client
			},
			wantErr: []string{"could not read the pods", "nothing was deleted", "request timed out"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := switchOptions(t)
			client := tc.setup(t, &opt)

			var stdout, stderr bytes.Buffer
			err := runSwitch(context.Background(), opt, strings.NewReader(""), &stdout, &stderr)
			t.Logf("err: %v\nstderr:\n%s", err, stderr.String())

			require.Error(t, err, "an apply whose pod check did not happen must not succeed")
			assert.Empty(t, claimDeletes(client), "no claim may be deleted")
			assert.Zero(t, claimCreates(client), "no replacement may be applied")
			assert.NotContains(t, stderr.String(), "deleted PVC")
			for _, want := range tc.wantErr {
				assert.Contains(t, err.Error(), want)
			}
			for _, want := range tc.wantStderr {
				assert.Contains(t, stderr.String(), want)
			}
		})
	}
}

// A pod that mounts the claim refuses the switch, on the path where the pods
// were read.
func TestSwitchRefusesWhileAPodStillMountsTheClaim(t *testing.T) {
	opt := switchOptions(t)
	client := fake.NewClientset(ipSourcePV(), sourceClaim(), podMounting("writer", "data"))
	stubKubeClients(t, client)

	var stdout, stderr bytes.Buffer
	err := runSwitch(context.Background(), opt, strings.NewReader(""), &stdout, &stderr)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "apps/writer (apps/data)")
	assert.Contains(t, err.Error(), "Scale their owners to zero first")
	assert.Empty(t, claimDeletes(client))
	assert.Zero(t, claimCreates(client))
}

// TestSwitchReportsAClaimStillTerminatingAsNotDone. The DELETE is
// accepted and the claim stays, Terminating, because something still holds it
// (a pod started after the check, or one on a node the list did not show).
// The replacement has the same namespace/name, so its create answers
// AlreadyExists -- which was reported as "exists" and the run as a success,
// with the workload still on the old array.
func TestSwitchReportsAClaimStillTerminatingAsNotDone(t *testing.T) {
	opt := switchOptions(t)
	client := fake.NewClientset(ipSourcePV(), sourceClaim())
	// pvc-protection: the DELETE sets deletionTimestamp and the object stays.
	client.PrependReactor("delete", "persistentvolumeclaims",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			del := action.(k8stesting.DeleteAction)
			obj, err := client.Tracker().Get(action.GetResource(), del.GetNamespace(), del.GetName())
			if err != nil {
				return true, nil, err
			}
			pvc := obj.(*corev1.PersistentVolumeClaim).DeepCopy()
			now := metav1.Now()
			pvc.DeletionTimestamp = &now
			pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
			return true, nil, client.Tracker().Update(action.GetResource(), pvc, del.GetNamespace())
		})
	stubKubeClients(t, client)

	var stdout, stderr bytes.Buffer
	err := runSwitch(context.Background(), opt, strings.NewReader(""), &stdout, &stderr)
	t.Logf("err: %v\nstderr:\n%s", err, stderr.String())

	require.Error(t, err, "a claim that is still there was not switched")
	assert.Contains(t, err.Error(), "apps/data")
	assert.Contains(t, err.Error(), "Terminating")
	assert.NotContains(t, stderr.String(), "exists PVC")
	assert.NotContains(t, stderr.String(), "now recreate the pods",
		"the run must not read as finished")

	claim, getErr := client.CoreV1().PersistentVolumeClaims("apps").Get(context.Background(), "data",
		metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Equal(t, "pvc-abc123", claim.Spec.VolumeName, "the original is untouched, not replaced")
}

// The path that works: the pods were read, none uses the claim, the claim goes,
// and the replacement is created on the mirrored PV.
func TestSwitchRebindsTheClaimWhenNothingUsesIt(t *testing.T) {
	opt := switchOptions(t)
	client := fake.NewClientset(ipSourcePV(), sourceClaim(), podMounting("bystander", "other"))
	stubKubeClients(t, client)

	var stdout, stderr bytes.Buffer
	err := runSwitch(context.Background(), opt, strings.NewReader(""), &stdout, &stderr)
	t.Logf("stderr:\n%s", stderr.String())
	require.NoError(t, err)

	assert.Equal(t, []string{"apps/data"}, claimDeletes(client))
	assert.Contains(t, stderr.String(), "deleted PVC apps/data")
	assert.Contains(t, stderr.String(), "created PVC apps/data bound to pvc-abc123-dr")
	assert.Contains(t, stderr.String(), "now recreate the pods")

	claim, err := client.CoreV1().PersistentVolumeClaims("apps").Get(context.Background(), "data",
		metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "pvc-abc123-dr", claim.Spec.VolumeName)
	_, err = client.CoreV1().PersistentVolumes().Get(context.Background(), "pvc-abc123-dr", metav1.GetOptions{})
	assert.NoError(t, err, "the mirrored PV is applied with its claim")
}

// The dry run is unchanged: it still plans when the pods cannot be read, and
// says what the apply would do about it.
func TestSwitchDryRunStillPlansWhenThePodsCannotBeRead(t *testing.T) {
	opt := switchOptions(t)
	opt.dryRun = true
	client := fake.NewClientset(ipSourcePV(), sourceClaim())
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("etcdserver: request timed out"))
	})
	stubKubeClients(t, client)

	var stdout, stderr bytes.Buffer
	require.NoError(t, runSwitch(context.Background(), opt, strings.NewReader(""), &stdout, &stderr))

	assert.Contains(t, stdout.String(), "PVCs to re-bind")
	assert.Contains(t, stderr.String(), "dry run: nothing was changed")
	assert.Contains(t, stderr.String(), "-dry-run=false would refuse")
	assert.Empty(t, claimDeletes(client))
}

// The dry run with -f says what the apply will do instead of deleting.
func TestSwitchDryRunFromFilesSaysTheApplyPrintsCommands(t *testing.T) {
	opt := switchOptions(t)
	opt.dryRun = true
	opt.files = []string{sourceManifestFile(t)}

	var stdout, stderr bytes.Buffer
	require.NoError(t, runSwitch(context.Background(), opt, strings.NewReader(""), &stdout, &stderr))

	assert.Contains(t, stderr.String(), "with -f, -dry-run=false deletes nothing")
}

// applyClaim is create-only, and AlreadyExists is "exists" only when what
// exists is the replacement itself.
func TestApplyClaimReportsOnlyTheReplacementItselfAsExisting(t *testing.T) {
	replacement := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "data"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pvc-abc123-dr"},
	}
	terminating := sourceClaim()
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	terminating.Finalizers = []string{"kubernetes.io/pvc-protection"}

	for _, tc := range []struct {
		name     string
		existing *corev1.PersistentVolumeClaim
		want     string
		wantErr  string
	}{
		{name: "nothing there", want: "created"},
		{name: "the replacement, from an earlier run", existing: replacement.DeepCopy(), want: "exists"},
		{name: "the original, still Terminating", existing: terminating, wantErr: "Terminating"},
		{name: "another claim on another volume", existing: sourceClaim(), wantErr: "bound to pvc-abc123"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset()
			if tc.existing != nil {
				client = fake.NewClientset(tc.existing)
			}
			k := &kubeClient{clientset: client}

			got, err := k.applyClaim(context.Background(), replacement.DeepCopy())

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				assert.Contains(t, err.Error(), "apps/data")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
