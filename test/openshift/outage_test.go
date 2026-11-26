//go:build openshift

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package openshift

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Clean application shutdown when the Qumulo array is unreachable.
//
// The driver answers an unreachable array with a three-rung ladder in
// NodeUnpublishVolume -- a bounded plain umount, then `umount -f` when the
// server is confirmed gone, then `umount -l` -- so a pod whose volume is on a
// dead array terminates inside its grace period instead of hanging, and the
// node is left with no stale mount. That logic is distribution-blind and is
// tested in pkg/driver.
//
// What is *not* distribution-blind is the install the ladder runs inside. On
// OpenShift the node plugin runs under an SCC, and three things the chart
// renders are what let `umount(8)` reach the host at all:
//
//  1. the kubelet directory mounted with Bidirectional propagation,
//  2. a driver container privileged enough to call umount in the host
//     namespace,
//  3. the -unmount-timeout plumbing that bounds the first rung.
//
// Each is currently unasserted anywhere. A chart change that quietly drops one
// produces an install that rolls out, reports healthy, provisions and mounts
// normally -- and then hangs pods forever the first time an array goes away,
// which is the one failure nobody is watching for. These cases close that gap.
//
// They assert the install as it ships today. None of them required a change to
// the chart, and none should ever be "fixed" by changing one: a failure here is
// a finding about the install, not about the test.

// unmountLadderValues renders the recommended OpenShift install with an
// explicit unmountTimeout, which the shipped values leave empty so the
// driver's own 30 s default stays the single source of truth.
func unmountLadderValues(t *testing.T, timeout string) Options {
	t.Helper()

	opts := openShiftValues(t)
	if timeout != "" {
		opts.Sets = append(opts.Sets, "node.unmountTimeout="+timeout)
	}
	return opts
}

// volumeMountOf returns one container's named volumeMount, read straight off
// the rendered object. PodSpec deliberately models only what admission reads,
// and mount propagation is not that -- it is what makes the mount work.
func volumeMountOf(t *testing.T, o Object, container, volume string) Object {
	t.Helper()

	spec, ok := asMap(o.Dig("spec", "template", "spec"))
	require.True(t, ok, "%s/%s has no pod spec", o.Kind(), o.Name())

	for _, c := range sliceOf(spec["containers"]) {
		co, ok := asMap(c)
		if !ok || co.str("name") != container {
			continue
		}
		for _, vm := range sliceOf(co["volumeMounts"]) {
			m, ok := asMap(vm)
			if ok && m.str("name") == volume {
				return m
			}
		}
		t.Fatalf("container %s has no volumeMount named %q", container, volume)
	}

	t.Fatalf("%s/%s has no container named %q", o.Kind(), o.Name(), container)
	return nil
}

// argsOf returns one container's args.
func argsOf(t *testing.T, o Object, container string) []string {
	t.Helper()

	spec, ok := asMap(o.Dig("spec", "template", "spec"))
	require.True(t, ok, "%s/%s has no pod spec", o.Kind(), o.Name())

	for _, c := range sliceOf(spec["containers"]) {
		co, ok := asMap(c)
		if ok && co.str("name") == container {
			return stringsOf(co["args"])
		}
	}

	t.Fatalf("%s/%s has no container named %q", o.Kind(), o.Name(), container)
	return nil
}

// driverContainer is the node DaemonSet's own container -- the one that runs
// the ladder. The sidecars beside it run no umount.
const driverContainer = "qumulo-csi-driver"

// kubeletVolume is the hostPath volume carrying /var/lib/kubelet.
const kubeletVolume = "pods-mount-dir"

// TestForcedUnmountReachesTheHostMountNamespace.
//
// `umount` inside the driver container only affects the host -- and so only
// actually frees the pod's volume -- if the kubelet directory is mounted with
// Bidirectional propagation. With HostToContainer the ladder runs, reports
// success, and the mount is still on the node afterwards: the pod stays
// Terminating and the worker accumulates dead mounts.
//
// Nothing else in the suite asserts propagation, and the SCC model cannot --
// admission does not read it. So it is asserted here, on the shipped render.
func TestForcedUnmountReachesTheHostMountNamespace(t *testing.T) {
	manifest := Render(t, openShiftValues(t))
	daemonSet := manifest.Named(t, "DaemonSet", "-node")

	mount := volumeMountOf(t, daemonSet, driverContainer, kubeletVolume)

	assert.Equal(t, "Bidirectional", mount.str("mountPropagation"),
		"the kubelet directory is not mounted Bidirectional, so a forced unmount in the driver container "+
			"does not propagate to the host and a pod on a dead array never finishes terminating")

	assert.Equal(t, "/var/lib/kubelet", mount.str("mountPath"),
		"the driver container no longer mounts the RHCOS kubelet directory, so it cannot see the targets it unmounts")

	// And the volume behind it is the host's directory, not an emptyDir --
	// propagation on a pod-local volume propagates nowhere.
	pod := PodTemplate(t, daemonSet)
	assert.Contains(t, pod.VolumeTypes, "hostPath",
		"the node pod has no hostPath volume, so there is no host mount namespace to unmount in")
}

// TestDriverContainerCanUnmountOnTheHost.
//
// umount(2) needs CAP_SYS_ADMIN. The chart gets it by running the driver
// container privileged; the alternative -- unprivileged with SYS_ADMIN added
// -- would work too. What must never happen is neither, which renders an
// install that mounts fine (the kubelet does that part) and cannot unmount a
// dead one.
//
// The SCC is checked alongside: a privileged container the shipped SCC does
// not permit is refused at admission, and then there is no node plugin at all.
func TestDriverContainerCanUnmountOnTheHost(t *testing.T) {
	manifest := Render(t, openShiftValues(t))
	daemonSet := manifest.Named(t, "DaemonSet", "-node")
	pod := PodTemplate(t, daemonSet)

	security := pod.Effective(t, driverContainer)
	privileged := security.Privileged != nil && *security.Privileged
	hasSysAdmin := hasCapability(security.AddCapabilities, "SYS_ADMIN")

	assert.True(t, privileged || hasSysAdmin,
		"the driver container is neither privileged nor granted SYS_ADMIN, so umount(2) is refused on the host "+
			"and the forced-unmount ladder cannot run: pods on an unreachable array will hang")

	// The required drop list must not take back what the ladder needs.
	assert.False(t, hasCapability(security.DropCapabilities, "SYS_ADMIN"),
		"SYS_ADMIN is dropped from the driver container, which removes the capability umount needs")

	scc := SCCFromManifest(t, manifest)
	if privileged {
		assert.True(t, scc.AllowPrivilegedContainer,
			"the driver container is privileged but the SCC the chart ships (%s) forbids privileged containers, "+
				"so the node pod is refused at admission", scc.Name)
	}
	assert.True(t, scc.AllowHostDirVolumePlugin,
		"the SCC the chart ships (%s) forbids hostPath, so the driver container cannot reach the kubelet directory",
		scc.Name)
}

// TestUnmountTimeoutIsWiredFromValuesToTheDriver.
//
// node.unmountTimeout bounds the plain umount before the escalation. If it
// never reaches the binary, an operator who tunes it for a slow array gets the
// 30 s default and no warning. The matching trap is at the other end: a
// value near the kubelet's fixed two-minute NodeUnpublish deadline would let
// that deadline end the call before the forced rungs ever ran. The driver
// clamps it (TestDefect_UnmountTimeoutAboveTheKubeletDeadlineStillEscalates
// in pkg/driver), so the chart's job is only to pass the value through
// faithfully -- including a large one, which must render rather than be
// silently dropped or rejected.
func TestUnmountTimeoutIsWiredFromValuesToTheDriver(t *testing.T) {
	for _, timeout := range []string{"15s", "30s", "110s", "2m"} {
		t.Run(timeout, func(t *testing.T) {
			daemonSet := Render(t, unmountLadderValues(t, timeout)).Named(t, "DaemonSet", "-node")

			assert.Contains(t, argsOf(t, daemonSet, driverContainer), "-unmount-timeout="+timeout,
				"node.unmountTimeout=%s did not reach the driver container, so the forced-unmount ladder "+
					"runs on the built-in default instead", timeout)
		})
	}
}

// And with the value unset -- which is how the chart ships -- the flag must be
// absent rather than rendered empty. `-unmount-timeout=` would be a parse
// error at startup, and the driver's own 30 s default is meant to be the
// single source of truth for anyone who has not tuned it.
func TestUnsetUnmountTimeoutLeavesTheDriverDefaultAlone(t *testing.T) {
	daemonSet := Render(t, openShiftValues(t)).Named(t, "DaemonSet", "-node")

	for _, arg := range argsOf(t, daemonSet, driverContainer) {
		assert.False(t, strings.HasPrefix(arg, "-unmount-timeout"),
			"the shipped install renders %q; with no value set the flag must be omitted so the driver's "+
				"own default applies", arg)
	}
}

// TestNodePluginSurvivesAnArrayOutageAtTheKubeletLevel.
//
// Two install-level properties decide whether the ladder ever gets to run
// during an outage, and both are easy to lose in a values change:
//
//   - the DaemonSet must tolerate a node under pressure, or the node plugin is
//     evicted mid-outage and the remaining pods have nothing to unmount them;
//   - the driver container's liveness probe must not kill the plugin while it
//     is blocked on a dead array, which would restart it into the same block.
//
// This case records what the install does today so a change to either shows up
// here rather than during an outage.
func TestNodePluginSurvivesAnArrayOutageAtTheKubeletLevel(t *testing.T) {
	daemonSet := Render(t, openShiftValues(t)).Named(t, "DaemonSet", "-node")
	spec, ok := asMap(daemonSet.Dig("spec", "template", "spec"))
	require.True(t, ok)

	var tolerated []string
	for _, tol := range sliceOf(spec["tolerations"]) {
		m, ok := asMap(tol)
		if !ok {
			continue
		}
		key := m.str("key")
		if key == "" && m.str("operator") == "Exists" {
			key = "*"
		}
		tolerated = append(tolerated, key)
	}

	assert.Contains(t, strings.Join(tolerated, ","), "*",
		"the node DaemonSet does not tolerate every taint (tolerations: %v); a worker tainted during a storage "+
			"outage can lose its node plugin, and nothing is then left to unmount the dead volumes", tolerated)

	// The driver container's liveness probe, bounded.
	//
	// The probe is an HTTP GET against the livenessprobe sidecar, which calls
	// the driver's Probe RPC -- liveness only; it contacts no array, so a hung
	// mount does not fail it by itself. What would hurt is the probe budget
	// being cut below the time the ladder legitimately takes: the plugin is
	// then restarted mid-escalation, the kubelet retries NodeUnpublish against
	// a fresh process, and the terminating pod's shutdown starts over.
	//
	// Budget = initialDelaySeconds + failureThreshold x periodSeconds. It has
	// to clear the kubelet's own two-minute NodeUnpublish deadline, which is
	// the longest the ladder can legitimately occupy.
	const kubeletUnpublishDeadline = int64(120)

	for _, c := range sliceOf(spec["containers"]) {
		co, ok := asMap(c)
		if !ok || co.str("name") != driverContainer {
			continue
		}

		probe, ok := asMap(co["livenessProbe"])
		require.True(t, ok, "the driver container has no liveness probe; this case no longer describes the install")

		initial, _ := intOf(probe["initialDelaySeconds"])
		period, _ := intOf(probe["periodSeconds"])
		threshold, _ := intOf(probe["failureThreshold"])
		budget := initial + threshold*period

		assert.GreaterOrEqual(t, budget, kubeletUnpublishDeadline,
			"the driver container's liveness budget is %ds (initialDelay %d + failureThreshold %d x period %d), "+
				"under the kubelet's %ds NodeUnpublish deadline: the plugin can be restarted part-way through the "+
				"forced-unmount ladder and a terminating pod's shutdown starts again",
			budget, initial, threshold, period, kubeletUnpublishDeadline)

		assert.NotNil(t, co["readinessProbe"],
			"the driver container lost its registration readiness probe")
	}
}
