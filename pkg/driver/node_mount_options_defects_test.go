//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

// Defect tests for the review of the node mount-option checks.
// Each asserts the CORRECT behaviour, so it fails while its defect is
// present. Fakes only: no real mount, no sudo.

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mount "k8s.io/mount-utils"
)

// f0xMountedTarget is an existing directory fm lists as mounted.
func f0xMountedTarget(t *testing.T, fm *mount.FakeMounter) string {
	t.Helper()
	target := filepath.Join(resolvedTempDir(t), "target")
	require.NoError(t, os.MkdirAll(target, 0o750))
	fm.MountPoints = []mount.MountPoint{{Path: target, Device: "qumulo.example.com:/export", Type: "nfs"}}
	return target
}

// f0xActions lists what fm was asked to do, in order.
func f0xActions(fm *mount.FakeMounter) []string {
	var actions []string
	for _, a := range fm.GetLog() {
		actions = append(actions, a.Action)
	}
	return actions
}

// ---------------------------------------------------------------- stale mount

// On a stale NFS mount every syscall that reaches the server
// fails ESTALE, including the inspection of the target the checks added before the
// stale-mount recovery. The publish answered Internal on every retry and the
// recovery never ran. The inspection failing ESTALE, with the mount-point
// check failing the same way, must still unmount the dead mount, mount afresh
// and answer OK.
func TestDefect_StaleMountTheTargetInspectionCannotReadIsRecovered(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	ns.writeProbe = (&probeRecorder{}).probe
	target := f0xMountedTarget(t, fm)
	stale := &os.PathError{Op: "lstat", Path: target, Err: syscall.ESTALE}
	fm.MountCheckErrors = map[string]error{target: stale}
	unmounted := false
	fm.UnmountFunc = func(string) error {
		unmounted = true
		delete(fm.MountCheckErrors, target)
		return nil
	}
	ns.inspectTarget = func(string) (publishTargetState, error) {
		if unmounted {
			return targetUsable, nil
		}
		return targetUsable, stale
	}

	_, err := ns.NodePublishVolume(context.Background(), publishRequestFor(t, target, nil, nil))

	t.Logf("publish: %v; mounter log: %v", err, f0xActions(fm))
	require.NoError(t, err, "a stale mount must be recovered, not answered Internal")
	assert.Equal(t, []string{mount.FakeActionUnmount, mount.FakeActionMount}, f0xActions(fm))
}

// The shape -- the mount-point check reads cached
// attributes and says "mounted", the real stat says ESTALE -- behind an
// inspection that also says ESTALE. The stat path's recovery must run.
func TestDefect_StaleMountTheCachedCheckCallsMountedIsRecovered(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	ns.writeProbe = (&probeRecorder{}).probe
	target := f0xMountedTarget(t, fm)
	unmounted := false
	fm.UnmountFunc = func(string) error { unmounted = true; return nil }
	ns.inspectTarget = func(p string) (publishTargetState, error) {
		if unmounted {
			return targetUsable, nil
		}
		return targetUsable, &os.PathError{Op: "lstat", Path: p, Err: syscall.ESTALE}
	}
	ns.statFunc = func(p string) (os.FileInfo, error) {
		if unmounted {
			return os.Stat(p)
		}
		return nil, &os.PathError{Op: "stat", Path: p, Err: syscall.ESTALE}
	}

	_, err := ns.NodePublishVolume(context.Background(), publishRequestFor(t, target, nil, nil))

	t.Logf("publish: %v; mounter log: %v", err, f0xActions(fm))
	require.NoError(t, err, "a stale mount must be recovered, not answered Internal")
	assert.Equal(t, []string{mount.FakeActionUnmount, mount.FakeActionMount}, f0xActions(fm))
}

// ---------------------------------------------------------------- agreeing version

// A version option in the class's mountOptions that AGREES
// with its protocol is no contradiction. The checks refused [v3] on an nfs3 class
// (and [v4.1] on nfs4.1), which mounted before them; a PV's mountOptions
// cannot be edited, so those pods could never start after the upgrade. The
// version is pinned once, and NFSv3 still gets nolock.
func TestDefect_AVersionShorthandThatAgreesWithTheProtocolMounts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol string
		flags    []string
		want     []string
	}{
		{name: "nfs3 and v3", protocol: protocolNFS3, flags: []string{"v3"}, want: []string{"v3", "nolock"}},
		{name: "nfs3 and nfsvers=3", protocol: protocolNFS3, flags: []string{"nfsvers=3"}, want: []string{"nfsvers=3", "nolock"}},
		{name: "nfs4.1 and v4.1", protocol: protocolNFS41, flags: []string{"v4.1"}, want: []string{"v4.1"}},
		{name: "nfs4.1 and vers=4.1", protocol: protocolNFS41, flags: []string{"vers=4.1"}, want: []string{"vers=4.1"}},
		{name: "no protocol and v3", flags: []string{"v3"}, want: []string{"v3", "nolock"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)
			volumeContext := map[string]string{}
			if tc.protocol != "" {
				volumeContext[volumeContextProtocol] = tc.protocol
			}

			_, err := ns.NodePublishVolume(context.Background(),
				publishRequestFor(t, filepath.Join(t.TempDir(), "target"), volumeContext, tc.flags))

			require.NoError(t, err)
			require.Len(t, fm.MountPoints, 1)
			assert.Equal(t, tc.want, fm.MountPoints[0].Opts)
		})
	}
}

// ---------------------------------------------------------------- sec=

// With no securityFlavor the driver chooses no flavour, so
// the class's sec= is the only one there is. The checks read an unset flavour as
// sys and refused sec=krb5p, which mounted before -- the way a class asked
// for Kerberos before securityFlavor existed. It must mount with the
// class's option, and nothing the driver added.
func TestDefect_AnUnsetFlavourMountsWithTheClassSecOption(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)

	_, err := ns.NodePublishVolume(context.Background(),
		publishRequestFor(t, filepath.Join(t.TempDir(), "target"),
			map[string]string{volumeContextProtocol: protocolNFS41}, []string{"sec=krb5p"}))

	require.NoError(t, err)
	require.Len(t, fm.MountPoints, 1)
	assert.Equal(t, []string{"sec=krb5p", "nfsvers=4.1"}, fm.MountPoints[0].Opts)
}
