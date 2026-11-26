// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"qumulo-csi-driver/pkg/qumulo"
)

// Kubernetes and Qumulo access for the repoint tool.
//
// **This is why `qumulo-csi-repoint` is a separate binary.** ARCHITECTURE.md rule 4
// says the driver never reads the Kubernetes API for volume lifecycle: that is
// the sidecars' job, and a driver that grew an informer would be
// reimplementing them. An operator CLI is not the driver. It runs on demand,
// under a human's own kubeconfig and RBAC, watches nothing, caches nothing,
// and is not deployed. Putting `mirror` and `switch` inside the driver -- as
// an RPC, or worse as a controller -- is what rule 4 forbids; putting them
// here is what rule 4 leaves room for.

// kubeClient is the subset of the Kubernetes API this tool uses. Narrow on
// purpose: it is also the list of RBAC verbs an operator has to hold.
type kubeClient struct {
	clientset kubernetes.Interface
	// dynamic serves the csi-addons VolumeReplication CRD. A dynamic client
	// rather than a generated one because this module does not depend on the
	// csi-addons *Kubernetes* API module and should not -- see failover.go.
	dynamic dynamic.Interface
}

// kubeClientFor is how every subcommand gets its client. A variable only so a
// test can hand `switch` a fake clientset, or a client that fails on its
// second construction; nothing else assigns it.
var kubeClientFor = newKubeClient

// newKubeClient builds a client from a kubeconfig, or from the pod's
// credentials when the path is empty and the process is in-cluster.
func newKubeClient(kubeconfig string) (*kubeClient, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("building a Kubernetes client: %w "+
			"(pass -f to work from manifests instead of a cluster)", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("building a Kubernetes client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("building a Kubernetes dynamic client: %w", err)
	}
	return &kubeClient{clientset: clientset, dynamic: dynamicClient}, nil
}

// listPageSize bounds one List response. The tool runs during an incident,
// against an API server that may be struggling: one unpaged LIST of every PV
// was 15.5 MiB for 20,000 of them.
const listPageSize = 500

// listVolumes returns every PV provisioned by the named driver, a page at a
// time.
func (k *kubeClient) listVolumes(ctx context.Context, driverName string) ([]*corev1.PersistentVolume, error) {
	var (
		out       []*corev1.PersistentVolume
		continued string
	)
	for {
		list, err := k.clientset.CoreV1().PersistentVolumes().List(ctx,
			metav1.ListOptions{Limit: listPageSize, Continue: continued})
		if err != nil {
			return nil, fmt.Errorf("listing PersistentVolumes: %w", err)
		}
		for i := range list.Items {
			pv := &list.Items[i]
			if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == driverName {
				out = append(out, pv)
			}
		}
		if continued = list.Continue; continued == "" {
			return out, nil
		}
	}
}

// listPodsForClaims returns the pods in the namespaces of claims, which are
// "namespace/name", for podsUsingClaims to filter. It used to list every pod
// in every namespace.
func (k *kubeClient) listPodsForClaims(ctx context.Context, claims []string) ([]corev1.Pod, error) {
	seen := map[string]bool{}
	var out []corev1.Pod
	for _, claim := range claims {
		namespace, _, ok := strings.Cut(claim, "/")
		if !ok || seen[namespace] {
			continue
		}
		seen[namespace] = true
		pods, err := k.listPodsIn(ctx, namespace)
		if err != nil {
			return nil, err
		}
		out = append(out, pods...)
	}
	return out, nil
}

// applyVolume creates the mirrored PV, or reports it already present.
//
// Create-only, never update. A PV's source is immutable, so an "update" that
// looked like it worked would have silently failed at the only field that
// matters; and a mirrored PV that already exists is the normal state, because
// `mirror` is meant to be run before the incident and re-run afterwards.
func (k *kubeClient) applyVolume(ctx context.Context, pv *corev1.PersistentVolume) (string, error) {
	_, err := k.clientset.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
	switch {
	case err == nil:
		return "created", nil
	case apierrors.IsAlreadyExists(err):
		return "exists", nil
	default:
		return "", fmt.Errorf("creating PV %s: %w", pv.Name, err)
	}
}

// applyClaim creates the mirrored PVC, or reports it already present.
//
// "Already present" means the claim there *is* the mirrored one: same name,
// not being deleted, and on the mirrored PV (or on none yet, as `mirror`
// writes it). AlreadyExists alone proves only that the name is taken, and on
// the `switch` path the name is usually the original claim's -- which, when a
// pod still holds it, is Terminating under kubernetes.io/pvc-protection.
// Reporting that as "exists" made a switch that never happened read
// as done.
func (k *kubeClient) applyClaim(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (string, error) {
	claims := k.clientset.CoreV1().PersistentVolumeClaims(pvc.Namespace)
	_, err := claims.Create(ctx, pvc, metav1.CreateOptions{})
	switch {
	case err == nil:
		return "created", nil
	case !apierrors.IsAlreadyExists(err):
		return "", fmt.Errorf("creating PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
	}

	existing, err := claims.Get(ctx, pvc.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("PVC %s/%s already exists and could not be read back to tell whether "+
			"it is the mirrored claim: %w", pvc.Namespace, pvc.Name, err)
	}
	if existing.DeletionTimestamp != nil {
		return "", fmt.Errorf("PVC %s/%s was not created: a claim of that name is still Terminating "+
			"(finalizers %v) -- usually a pod still mounts it. The replacement was not applied",
			pvc.Namespace, pvc.Name, existing.Finalizers)
	}
	if existing.Spec.VolumeName != pvc.Spec.VolumeName {
		return "", fmt.Errorf("PVC %s/%s was not created: a different claim of that name exists, "+
			"bound to %s rather than %s", pvc.Namespace, pvc.Name,
			volumeOrNone(existing.Spec.VolumeName), pvc.Spec.VolumeName)
	}
	return "exists", nil
}

// volumeOrNone renders an empty volume name as "no volume".
func volumeOrNone(name string) string {
	if name == "" {
		return "no volume"
	}
	return name
}

// awaitClaimGone polls one claim until it is gone, or the bound expires.
//
// A DELETE answering 200 says the claim is being deleted, not that it is gone:
// kubernetes.io/pvc-protection keeps it, Terminating, for as long as a pod
// mounts it, and its controller removes the finalizer asynchronously even when
// none does. `switch` creates the replacement under the same name, so it has
// to know which of the two it is looking at.
func (k *kubeClient) awaitClaimGone(ctx context.Context, namespace, name string, poll, bound time.Duration) error {
	deadline := time.Now().Add(bound)
	for {
		claim, err := k.clientset.CoreV1().PersistentVolumeClaims(namespace).
			Get(ctx, name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			return nil
		case err != nil:
			return fmt.Errorf("reading PVC %s/%s back after deleting it: %w", namespace, name, err)
		}
		if !time.Now().Add(poll).Before(deadline) {
			state := "still present"
			if claim.DeletionTimestamp != nil {
				state = fmt.Sprintf("still Terminating (finalizers %v; a pod still mounts it, or "+
					"kubernetes.io/pvc-protection has not caught up)", claim.Finalizers)
			}
			return fmt.Errorf("PVC %s/%s is %s %s after its delete", namespace, name, state, bound)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for PVC %s/%s to go: %w", namespace, name, ctx.Err())
		case <-time.After(poll):
		}
	}
}

// deleteClaim removes a PVC. Only ever called on the IP path, only after the
// plan has confirmed the PV behind it is Retain, and only with
// --dry-run=false.
func (k *kubeClient) deleteClaim(ctx context.Context, namespace, name string) error {
	err := k.clientset.CoreV1().PersistentVolumeClaims(namespace).
		Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting PVC %s/%s: %w", namespace, name, err)
	}
	return nil
}

// listClaimsIn returns every PVC in one namespace.
func (k *kubeClient) listClaimsIn(ctx context.Context, namespace string) ([]*corev1.PersistentVolumeClaim, error) {
	list, err := k.clientset.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing PersistentVolumeClaims in %s: %w", namespace, err)
	}

	out := make([]*corev1.PersistentVolumeClaim, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, &list.Items[i])
	}
	return out, nil
}

// listPodsIn returns every pod in one namespace, for podsMountingClaims to
// filter.
func (k *kubeClient) listPodsIn(ctx context.Context, namespace string) ([]corev1.Pod, error) {
	var (
		out       []corev1.Pod
		continued string
	)
	for {
		list, err := k.clientset.CoreV1().Pods(namespace).List(ctx,
			metav1.ListOptions{Limit: listPageSize, Continue: continued})
		if err != nil {
			return nil, fmt.Errorf("listing Pods in %s: %w", namespace, err)
		}
		out = append(out, list.Items...)
		if continued = list.Continue; continued == "" {
			return out, nil
		}
	}
}

// deletePod deletes one pod and lets its controller recreate it.
//
// A pod already gone is success: between listing and deleting, a failover is
// exactly the window in which a kubelet finishes evicting one.
func (k *kubeClient) deletePod(ctx context.Context, namespace, name string) error {
	err := k.clientset.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting Pod %s/%s: %w", namespace, name, err)
	}
	return nil
}

// listWorkloadsIn returns the three scalable kinds in one namespace, reduced to
// what the quiesce reads.
//
// Three list calls rather than one `scale` lookup per pod: an incident is not
// the time to make N API calls where three will do, and the plan has to name
// every owner before it changes any of them. ReplicaSets are listed because a
// pod's controller *is* a ReplicaSet -- resolving it to the Deployment that owns
// it is what keeps step 0 from scaling something the Deployment controller
// scales straight back (see resolveScalableOwner).
func (k *kubeClient) listWorkloadsIn(ctx context.Context, namespace string) ([]workload, error) {
	deployments, err := k.clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing Deployments in %s: %w", namespace, err)
	}
	statefulSets, err := k.clientset.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing StatefulSets in %s: %w", namespace, err)
	}
	replicaSets, err := k.clientset.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing ReplicaSets in %s: %w", namespace, err)
	}
	return workloadsFromAPI(deployments.Items, statefulSets.Items, replicaSets.Items), nil
}

// patchWorkload merge-patches one scalable owner.
//
// A merge patch on the object rather than a write to the `scale` subresource,
// for one reason: the replica count and the annotation that remembers it have
// to move together. Two calls could leave an owner at zero with nothing
// recording what it was, which is the one state a restore cannot recover from.
func (k *kubeClient) patchWorkload(
	ctx context.Context,
	kind, namespace, name string,
	patch map[string]any,
) error {
	encoded, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("encoding the patch for %s %s/%s: %w", kind, namespace, name, err)
	}

	apps := k.clientset.AppsV1()
	switch kind {
	case kindDeployment:
		_, err = apps.Deployments(namespace).Patch(ctx, name, types.MergePatchType, encoded,
			metav1.PatchOptions{})
	case kindStatefulSet:
		_, err = apps.StatefulSets(namespace).Patch(ctx, name, types.MergePatchType, encoded,
			metav1.PatchOptions{})
	case kindReplicaSet:
		_, err = apps.ReplicaSets(namespace).Patch(ctx, name, types.MergePatchType, encoded,
			metav1.PatchOptions{})
	default:
		return fmt.Errorf("%s is not a kind this command scales", kind)
	}
	if err != nil {
		return fmt.Errorf("patching %s %s/%s: %w", kind, namespace, name, err)
	}
	return nil
}

// listVolumeReplications returns the csi-addons VolumeReplication objects in a
// namespace, reduced to the fields this tool reads.
//
// A CRD that is not installed is reported as such rather than as an empty
// list: "no VolumeReplications" and "csi-addons is not on this cluster" are
// different problems with different remedies, and a failover that printed the
// first when it meant the second would send an operator looking in the wrong
// place.
func (k *kubeClient) listVolumeReplications(ctx context.Context, namespace string) ([]volumeReplication, error) {
	list, err := k.dynamic.Resource(volumeReplicationGVR).Namespace(namespace).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("the %s CRD is not installed on this cluster: a failover is "+
				"driven through those objects: %w", volumeReplicationGVR.Resource, err)
		}
		return nil, fmt.Errorf("listing VolumeReplications in %s: %w", namespace, err)
	}

	out := make([]volumeReplication, 0, len(list.Items))
	for i := range list.Items {
		vr, err := volumeReplicationFrom(&list.Items[i])
		if err != nil {
			return nil, err
		}
		out = append(out, vr)
	}
	return out, nil
}

// listVolumeReplicationClasses returns the cluster-scoped classes, reduced to
// their parameters.
//
// One parameter is read: `replicationScope`. It is what separates a group
// member -- correct, and driven by one object shared with its class -- from a
// volume nobody protected. An absent CRD is reported rather than answered with
// an empty list, for the same reason as the objects themselves.
func (k *kubeClient) listVolumeReplicationClasses(ctx context.Context) ([]volumeReplicationClass, error) {
	list, err := k.dynamic.Resource(volumeReplicationClassGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("the %s CRD is not installed on this cluster: %w",
				volumeReplicationClassGVR.Resource, err)
		}
		return nil, fmt.Errorf("listing VolumeReplicationClasses: %w", err)
	}

	out := make([]volumeReplicationClass, 0, len(list.Items))
	for i := range list.Items {
		class, err := volumeReplicationClassFrom(&list.Items[i])
		if err != nil {
			return nil, err
		}
		out = append(out, class)
	}
	return out, nil
}

// getVolumeReplication reads one object, for the bounded wait.
func (k *kubeClient) getVolumeReplication(ctx context.Context, namespace, name string) (volumeReplication, error) {
	object, err := k.dynamic.Resource(volumeReplicationGVR).Namespace(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return volumeReplication{}, fmt.Errorf("reading VolumeReplication %s/%s: %w", namespace, name, err)
	}
	return volumeReplicationFrom(object)
}

// patchVolumeReplicationSpec merge-patches fields under `spec`.
//
// Merge patch rather than update: the controller owns `status`, GitOps may own
// annotations, and a newer CRD may carry spec fields this tool has never heard
// of. A patch touching two keys leaves every one of those alone; a
// read-modify-write through a six-field struct would not.
func (k *kubeClient) patchVolumeReplicationSpec(
	ctx context.Context,
	namespace, name string,
	spec map[string]any,
) error {
	patch, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		return fmt.Errorf("encoding the patch for VolumeReplication %s/%s: %w", namespace, name, err)
	}

	_, err = k.dynamic.Resource(volumeReplicationGVR).Namespace(namespace).
		Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("patching VolumeReplication %s/%s: %w", namespace, name, err)
	}
	return nil
}

// Classification of API errors uses apimachinery's own helpers, as pkg/k8s
// does: "already exists" is the normal outcome of re-running `mirror`, not a
// failure.

// qumuloResolver reads a replica directory's file ID from the peer array.
//
// It implements fileIDResolver over the driver's own REST client, so the path
// this tool looks up and the path the driver later acts on are resolved by the
// same code: `GET /v1/files/<path>/info/attributes`.
type qumuloResolver struct {
	client *qumulo.Client
}

// newQumuloResolver builds a resolver for the peer array.
//
// **The token comes from a file and only from a file.** Never a flag: a flag
// value is in the process table, in the shell history and in any `ps` a
// colleague runs. Never an environment variable either, for the middle one of
// those. And the token is read per call through TokenFunc rather than held,
// which is the same rule the driver follows (ARCHITECTURE.md rule 0) and means a
// rotated file takes effect without restarting anything. Nothing in this
// binary ever prints it: the only thing done with the string is putting it in
// an Authorization header.
func newQumuloResolver(apiURL, tokenFile string, insecure bool) (*qumuloResolver, error) {
	if apiURL == "" {
		return nil, fmt.Errorf("-peer-api-url is required: the replica's file ID has to be read from " +
			"the peer array, and it is not derivable from anything in the PV")
	}
	if tokenFile == "" {
		return nil, fmt.Errorf("-peer-token-file is required")
	}
	if _, err := os.Stat(tokenFile); err != nil {
		return nil, fmt.Errorf("reading the peer token file: %w", err)
	}

	client := qumulo.NewWithTokenFunc(apiURL, func() (string, error) {
		raw, err := os.ReadFile(tokenFile) //nolint:gosec // the operator's own path
		if err != nil {
			// Deliberately does not echo any part of what was read.
			return "", fmt.Errorf("reading the peer token file %s: %w", tokenFile, err)
		}
		token := strings.TrimSpace(string(raw))
		if token == "" {
			return "", fmt.Errorf("the peer token file %s is empty", tokenFile)
		}
		return token, nil
	})
	if insecure {
		client.HTTPClient = insecureHTTPClient()
	}

	return &qumuloResolver{client: client}, nil
}

// FileIDAt returns the file ID the peer array allocated for a path.
func (q *qumuloResolver) FileIDAt(ctx context.Context, clusterPath string) (string, error) {
	info, err := q.client.LookupFileByPath(ctx, clusterPath)
	if err != nil {
		return "", err
	}
	if info.ID == "" {
		return "", fmt.Errorf("the peer returned no file ID for %s", clusterPath)
	}
	return info.ID, nil
}

// insecureHTTPClient mirrors the driver's -insecure-skip-tls-verify, for the
// same lab clusters with self-signed certificates. Named the same way, and as
// loudly, so nobody reaches for it by accident.
func insecureHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in, for self-signed lab clusters
	return &http.Client{Transport: transport}
}
