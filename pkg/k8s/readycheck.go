// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package k8s

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ReadyCheck is the chart's `helm test` readiness check (test-driver-ready),
// run as `qumulo-csi-driver ready-check` from the controller image.
//
// It used to be a bash script run from an OpenShift CLI image -- ose-cli on
// OpenShift, OKD's origin-cli elsewhere -- which put a third image, and that
// image's CVEs, into every install for the sake of three reads (the 0.1.1
// image scan: origin-cli a CentOS Stream build months old, ose-cli graded F by
// Red Hat). The same three checks, in the binary the release already ships:
//
//  1. the controller Deployment is Available with every replica Ready, and at
//     least one (a Deployment scaled to 0 is Available, 0 of 0);
//  2. the node DaemonSet's rollout is complete: every node pod updated, Ready
//     and available, at least one -- `helm upgrade --wait` hands over with
//     maxUnavailable node pods still starting;
//  3. the kubelet has registered DriverName in at least as many CSINodes as
//     the DaemonSet runs node pods. Counted over CSINodes rather than
//     matched per pod, so the check needs no pods access.
//
// All three share one Timeout, each is re-read every Interval while it is not
// yet true, and a failure names the last state seen. Every read is a GET or a
// LIST: nothing watches, so Rule 4 in ARCHITECTURE.md is untouched, and the test's
// ServiceAccount needs get on the Deployment and DaemonSet and list on
// CSINodes, nothing more.
type ReadyCheck struct {
	Client     kubernetes.Interface
	Namespace  string
	DriverName string
	Deployment string
	DaemonSet  string
	// Timeout bounds all three checks together. Helm's own test timeout is
	// 300 s, so the chart passes 240 s and a failure reports its own cause
	// instead of Helm's "timed out".
	Timeout  time.Duration
	Interval time.Duration
	Out      io.Writer

	// sleep waits between reads; nil means a real, context-aware wait.
	sleep func(context.Context, time.Duration) error
}

// errNotYet marks a state that may still become true: the check is re-read
// until it does or the timeout ends. Any other error fails at once.
var errNotYet = errors.New("not yet")

type notYet struct{ state string }

func (n notYet) Error() string { return n.state }
func (n notYet) Unwrap() error { return errNotYet }

// Run performs the three checks in order and returns nil only when all pass.
func (r ReadyCheck) Run(ctx context.Context) error {
	if r.Interval <= 0 {
		r.Interval = 5 * time.Second
	}
	if r.Out == nil {
		r.Out = io.Discard
	}
	if r.sleep == nil {
		r.sleep = sleepCtx
	}
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()

	ctrl, err := r.wait(ctx, func(ctx context.Context) (string, error) { return r.controller(ctx) })
	if err != nil {
		return fmt.Errorf("controller deployment/%s: %w", r.Deployment, err)
	}
	r.printf("ok: controller deployment/%s is Available, %s", r.Deployment, ctrl)

	var want int32
	node, err := r.wait(ctx, func(ctx context.Context) (string, error) {
		state, n, err := r.node(ctx)
		want = n
		return state, err
	})
	if err != nil {
		return fmt.Errorf("daemonset/%s: %w", r.DaemonSet, err)
	}
	r.printf("ok: daemonset/%s: %s", r.DaemonSet, node)

	reg, err := r.wait(ctx, func(ctx context.Context) (string, error) { return r.registered(ctx, want) })
	if err != nil {
		return fmt.Errorf("%s: %w", r.DriverName, err)
	}
	r.printf("ok: %s registered in %s", r.DriverName, reg)
	r.printf("PASS: controller Available (%s), node %s, %s registered in %s", ctrl, node, r.DriverName, reg)
	return nil
}

// wait re-runs check until it passes, fails outright, or ctx ends. A state
// that has not changed since the last read is not printed again.
func (r ReadyCheck) wait(ctx context.Context, check func(context.Context) (string, error)) (string, error) {
	seen := ""
	for {
		state, err := check(ctx)
		if err == nil {
			return state, nil
		}
		if !errors.Is(err, errNotYet) {
			return "", err
		}
		if err.Error() != seen {
			r.printf("waiting: %s", err)
			seen = err.Error()
		}
		if err := r.sleep(ctx, r.Interval); err != nil {
			return "", fmt.Errorf("%s after %s", seen, r.Timeout)
		}
	}
}

func (r ReadyCheck) controller(ctx context.Context) (string, error) {
	dep, err := r.Client.AppsV1().Deployments(r.Namespace).Get(ctx, r.Deployment, metav1.GetOptions{})
	if err != nil {
		return "", readErr(ctx, err)
	}
	spec := int32(1)
	if dep.Spec.Replicas != nil {
		spec = *dep.Spec.Replicas
	}
	if spec == 0 {
		return "", errors.New("scaled to 0: nothing would serve a CreateVolume")
	}
	state := fmt.Sprintf("%d of %d replicas Ready", dep.Status.ReadyReplicas, spec)
	if dep.Status.ObservedGeneration < dep.Generation || !deploymentAvailable(dep) {
		return "", notYet{"not Available, " + state}
	}
	if dep.Status.ReadyReplicas != spec {
		return "", notYet{state}
	}
	return state, nil
}

func deploymentAvailable(dep *appsv1.Deployment) bool {
	for _, c := range dep.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// node reports the DaemonSet's rollout and, once the rollout is observed, the
// number of node pods it schedules.
func (r ReadyCheck) node(ctx context.Context) (string, int32, error) {
	ds, err := r.Client.AppsV1().DaemonSets(r.Namespace).Get(ctx, r.DaemonSet, metav1.GetOptions{})
	if err != nil {
		return "", 0, readErr(ctx, err)
	}
	st := ds.Status
	want := st.DesiredNumberScheduled
	state := fmt.Sprintf("%d of %d node pods Ready (%d updated, %d available)",
		st.NumberReady, want, st.UpdatedNumberScheduled, st.NumberAvailable)
	if st.ObservedGeneration < ds.Generation {
		return "", 0, notYet{"rollout not yet observed, " + state}
	}
	if want == 0 {
		return "", 0, errors.New("schedules no node pods")
	}
	if st.UpdatedNumberScheduled != want || st.NumberReady != want || st.NumberAvailable != want {
		return "", 0, notYet{state}
	}
	return fmt.Sprintf("%d of %d node pods Ready", st.NumberReady, want), want, nil
}

// registered counts the CSINodes listing DriverName. A new node's entry
// appears only once its node pod has registered with the kubelet.
func (r ReadyCheck) registered(ctx context.Context, want int32) (string, error) {
	list, err := r.Client.StorageV1().CSINodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", readErr(ctx, err)
	}
	var nodes []string
	for _, n := range list.Items {
		for _, d := range n.Spec.Drivers {
			if d.Name == r.DriverName {
				nodes = append(nodes, n.Name)
				break
			}
		}
	}
	sort.Strings(nodes)
	state := fmt.Sprintf("%d CSINode(s): %s", len(nodes), strings.Join(nodes, " "))
	if int32(len(nodes)) < want {
		return "", notYet{fmt.Sprintf("registered in %s, but %d node pods run", state, want)}
	}
	return state, nil
}

// readErr turns a failed read into the error the check reports. A read cut
// short by the deadline is "not yet", so wait reports the last state it saw;
// anything else (forbidden, not found) fails at once -- waiting would not fix
// it.
func readErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return notYet{"read did not complete: " + err.Error()}
	}
	return fmt.Errorf("cannot read: %w", err)
}

func (r ReadyCheck) printf(format string, args ...any) {
	_, _ = fmt.Fprintf(r.Out, format+"\n", args...)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// NewInClusterClientset returns the clientset this package's readers use,
// built from the pod's in-cluster credentials.
func NewInClusterClientset() (kubernetes.Interface, error) {
	return inClusterClientset()
}
