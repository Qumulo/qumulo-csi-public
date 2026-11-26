// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	mount "k8s.io/mount-utils"
)

// The node's mount options and the requests it accepts.

// --- The bare version options mount(8) accepts ---

func TestABareVersionOptionPinsTheVersion(t *testing.T) {
	for _, tc := range []struct {
		option string
		v3     bool
	}{
		{option: "v3", v3: true},
		{option: "V3", v3: true},
		{option: "vers=3", v3: true},
		{option: "nfsvers=3", v3: true},
		{option: "vers=3.1", v3: true},
		{option: "v4", v3: false},
		{option: "v4.1", v3: false},
		{option: "v4.2", v3: false},
		{option: "nfsvers=4.1", v3: false},
		{option: "hard", v3: false},
		{option: "noverslike", v3: false},
	} {
		t.Run(tc.option, func(t *testing.T) {
			assert.Equal(t, tc.v3, pinsNFSv3([]string{tc.option}))
		})
	}
}

// The failure this cost: the node image runs no rpc.statd, so an NFSv3
// mount without nolock fails outright, or mounts with locking silently
// broken. A class that pinned v3 the short way got neither the option nor a
// complaint.
func TestAClassPinningV3TheShortWayGetsNolock(t *testing.T) {
	option, err := nfsLockOptionFor(map[string]string{}, []string{"hard", "v3"})

	require.NoError(t, err)
	assert.Equal(t, "nolock", option)
}

// The other half: an nfs3 volume whose mountOptions say v4.1 is a
// contradiction, and the driver must say so rather than appending nfsvers=3
// beside it and leaving the kernel to take whichever came last.
func TestAnNFS3VolumeRefusesABareV41MountOption(t *testing.T) {
	_, err := nfsVersionOptionFor(
		map[string]string{volumeContextProtocol: protocolNFS3},
		[]string{"v4.1"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "v4.1")
	assert.Contains(t, err.Error(), "one place")
}

func TestABareVersionOptionNamesItsVersion(t *testing.T) {
	assert.Equal(t, "3", nfsVersionOf("v3"))
	assert.Equal(t, "4.2", nfsVersionOf("v4.2"))
	assert.Empty(t, nfsVersionOf("hard"))
	assert.Empty(t, nfsVersionOf("timeo=600"))
}

// End to end: the publish of a v3-pinned volume carries nolock into the
// mount table.
func TestNodePublishAddsNolockForABareV3Option(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	targetPath := filepath.Join(resolvedTempDir(t), "target")

	_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{MountFlags: []string{"hard", "v3"}},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		VolumeContext: map[string]string{
			volumeContextServer:     "qumulo.example.com",
			volumeContextExportPath: "/export",
		},
	})

	require.NoError(t, err)
	points := fm.MountPoints
	require.Len(t, points, 1)
	assert.Contains(t, points[0].Opts, "nolock")
}

// --- A sec= that contradicts securityFlavor ---

func TestASecMountOptionThatContradictsTheFlavourIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flavor  string
		flags   []string
		wantErr bool
	}{
		{name: "sys volume asked to mount krb5", flavor: "sys", flags: []string{"sec=krb5"}, wantErr: true},
		// No securityFlavor: the class's mountOptions are the only place the
		// flavour is chosen, so there is nothing to contradict.
		{name: "unset flavour, class asks for krb5p", flavor: "", flags: []string{"sec=krb5p"}, wantErr: false},
		{name: "unset flavour, class asks for sys", flavor: "", flags: []string{"sec=sys"}, wantErr: false},
		{name: "krb5p volume asked to mount sys", flavor: "krb5p", flags: []string{"sec=sys"}, wantErr: true},
		{name: "krb5p volume asked to mount krb5", flavor: "krb5p", flags: []string{"sec=krb5"}, wantErr: true},
		{name: "the same flavour spelled out", flavor: "krb5p", flags: []string{"sec=krb5p"}, wantErr: false},
		{name: "the same flavour in another case", flavor: "krb5p", flags: []string{"SEC=KRB5P"}, wantErr: false},
		{name: "sys volume, sys option", flavor: "", flags: []string{"sec=sys"}, wantErr: false},
		{name: "no sec option at all", flavor: "krb5", flags: []string{"hard"}, wantErr: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := refuseContradictorySecurityOption(
				map[string]string{volumeContextSecurityFlavor: tc.flavor}, tc.flags)

			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "sec=")
		})
	}
}

// --- One error test for both republish paths ---

// A republish must not unmount a mount a pod is using because one stat came
// back EACCES or EIO. The two paths used to disagree about that: the
// mount-point check followed mount-utils, which counts both, while the stat
// check did not.
func TestTheRepublishPathsAgreeOnWhatADeadMountIs(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		dead bool
	}{
		{name: "ESTALE", err: syscall.ESTALE, dead: true},
		{name: "EBADHANDLE", err: errEBADHANDLE, dead: true},
		{name: "ENOTCONN", err: syscall.ENOTCONN, dead: true},
		{name: "EACCES", err: syscall.EACCES, dead: false},
		{name: "EIO", err: syscall.EIO, dead: false},
		{name: "ETIMEDOUT", err: syscall.ETIMEDOUT, dead: false},
		{name: "ENOENT", err: syscall.ENOENT, dead: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.dead, isRepublishDeadMount(tc.err),
				"a live mount must not be unmounted under a running pod")
		})
	}

	// The narrow test is what both paths now use, and it is narrower than
	// the mount-utils one the reporting paths keep.
	assert.True(t, isCorruptedMount(syscall.EACCES), "mount-utils counts EACCES; the reporting paths still do")
	assert.False(t, isRepublishDeadMount(syscall.EACCES), "the republish paths must not")
}

// --- What the target path may be ---

func TestNodePublishRefusesASymlinkTarget(t *testing.T) {
	dir := resolvedTempDir(t)
	elsewhere := filepath.Join(dir, "elsewhere")
	require.NoError(t, os.MkdirAll(elsewhere, 0o750))
	target := filepath.Join(dir, "target")
	require.NoError(t, os.Symlink(elsewhere, target))

	ns, fm := newTestNodeServerWithFakeMounter(t)

	_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))

	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "symbolic link")
	assert.Empty(t, fm.MountPoints, "nothing may be mounted through the link")
}

// A dangling link is the same refusal, and the one a Stat-based check misses
// entirely: Stat answers IsNotExist for it, so the publish would have taken
// it for an empty target and called MkdirAll through it.
func TestNodePublishRefusesADanglingSymlinkTarget(t *testing.T) {
	dir := resolvedTempDir(t)
	target := filepath.Join(dir, "target")
	require.NoError(t, os.Symlink(filepath.Join(dir, "nothing-here"), target))

	ns, fm := newTestNodeServerWithFakeMounter(t)

	_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))

	assertCode(t, err, codes.FailedPrecondition)
	assert.Empty(t, fm.MountPoints)
}

func TestNodePublishRefusesARegularFileTarget(t *testing.T) {
	dir := resolvedTempDir(t)
	target := filepath.Join(dir, "target")
	require.NoError(t, os.WriteFile(target, []byte("not a directory"), 0o600))

	ns, fm := newTestNodeServerWithFakeMounter(t)

	_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))

	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), "not a directory")
	assert.Empty(t, fm.MountPoints)
}

// An ordinary directory, and a target that does not exist yet, are both
// publishable: this check must not refuse what the kubelet actually sends.
func TestNodePublishAcceptsADirectoryOrAnAbsentTarget(t *testing.T) {
	dir := resolvedTempDir(t)
	existing := filepath.Join(dir, "existing")
	require.NoError(t, os.MkdirAll(existing, 0o750))

	for name, target := range map[string]string{
		"an existing directory": existing,
		"nothing there yet":     filepath.Join(dir, "fresh"),
	} {
		t.Run(name, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)

			_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))

			require.NoError(t, err)
			assert.Len(t, fm.MountPoints, 1)
		})
	}
}

// --- A target that cannot be inspected because its mount is broken ---

// mountedTarget is an existing directory the fake mounter lists as mounted.
func mountedTarget(t *testing.T, fm *mount.FakeMounter) string {
	t.Helper()
	target := filepath.Join(resolvedTempDir(t), "target")
	require.NoError(t, os.MkdirAll(target, 0o750))
	fm.MountPoints = []mount.MountPoint{{Path: target, Device: "qumulo.example.com:/export", Type: "nfs"}}
	return target
}

// fakeMounterActions lists what the fake mounter was asked to do, in order.
func fakeMounterActions(fm *mount.FakeMounter) []string {
	var actions []string
	for _, a := range fm.GetLog() {
		actions = append(actions, a.Action)
	}
	return actions
}

// Each errno a broken mount answers is handed to the mount-point checks,
// which recover the stale ones; none is answered Internal by the
// inspection itself.
func TestNodePublishRecoversAStaleMountTheInspectionCouldNotRead(t *testing.T) {
	for name, errno := range map[string]error{
		"ESTALE":     syscall.ESTALE,
		"EBADHANDLE": errEBADHANDLE,
		"ENOTCONN":   syscall.ENOTCONN,
	} {
		t.Run(name, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)
			target := mountedTarget(t, fm)
			deadErr := &os.PathError{Op: "statx", Path: target, Err: errno}
			fm.MountCheckErrors = map[string]error{target: deadErr}
			fm.UnmountFunc = func(string) error { delete(fm.MountCheckErrors, target); return nil }
			ns.inspectTarget = func(string) (publishTargetState, error) { return targetUsable, deadErr }

			_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))

			require.NoError(t, err)
			assert.Equal(t, []string{mount.FakeActionUnmount, mount.FakeActionMount}, fakeMounterActions(fm))
		})
	}
}

// An inspection error that says nothing about a mount is still refused
// before anything is mounted or unmounted.
func TestNodePublishRefusesATargetItCannotInspectForAnotherReason(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	target := mountedTarget(t, fm)
	ns.inspectTarget = func(p string) (publishTargetState, error) {
		return targetUsable, &os.PathError{Op: "statx", Path: p, Err: syscall.ELOOP}
	}

	_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))

	assertCode(t, err, codes.Internal)
	assert.Empty(t, fakeMounterActions(fm))
}

// --- At the publish: the mount-point check's EACCES and EIO ---

// The mount-point check at the publish answering EACCES or EIO must leave
// the mount alone: neither says it is dead, and a pod may be using it. The
// predicate is tested on its own above; this pins the call site, which no
// test covered.
func TestNodePublishDoesNotUnmountOnEACCESOrEIOFromTheMountPointCheck(t *testing.T) {
	for name, errno := range map[string]error{"EACCES": syscall.EACCES, "EIO": syscall.EIO} {
		t.Run(name, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)
			target := mountedTarget(t, fm)
			fm.MountCheckErrors = map[string]error{target: &os.PathError{Op: "statx", Path: target, Err: errno}}

			_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))

			assertCode(t, err, codes.Internal)
			assert.NotContains(t, fakeMounterActions(fm), mount.FakeActionUnmount,
				"a live mount must not be unmounted under a running pod")
			assert.Len(t, fm.MountPoints, 1)
		})
	}
}

// --- A version option that agrees with the protocol ---

func TestAVersionOptionIsRefusedOnlyWhenItDisagreesWithTheProtocol(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol string
		flags    []string
		want     string
		wantErr  bool
	}{
		{name: "nfs3 and v3", protocol: protocolNFS3, flags: []string{"v3"}},
		{name: "nfs3 and nfsvers=3", protocol: protocolNFS3, flags: []string{"nfsvers=3"}},
		{name: "nfs3 and vers=3 in a joined entry", protocol: protocolNFS3, flags: []string{"hard,vers=3"}},
		{name: "nfs3 and V3", protocol: protocolNFS3, flags: []string{"V3"}},
		{name: "nfs4.1 and v4.1", protocol: protocolNFS41, flags: []string{"v4.1"}},
		{name: "nfs4.1 and nfsvers=4.1", protocol: protocolNFS41, flags: []string{"nfsvers=4.1"}},
		{name: "nfs4.1 and vers=4.1", protocol: protocolNFS41, flags: []string{"vers=4.1"}},
		{name: "nfs3 and no version", protocol: protocolNFS3, flags: []string{"hard"}, want: "nfsvers=3"},
		{name: "nfs4.1 and no version", protocol: protocolNFS41, want: "nfsvers=4.1"},
		{name: "nfs3 and v4.1", protocol: protocolNFS3, flags: []string{"v4.1"}, wantErr: true},
		{name: "nfs4.1 and v3", protocol: protocolNFS41, flags: []string{"v3"}, wantErr: true},
		{name: "nfs4.1 and v4", protocol: protocolNFS41, flags: []string{"v4"}, wantErr: true},
		{name: "nfs4.1 and v4.2", protocol: protocolNFS41, flags: []string{"v4.2"}, wantErr: true},
		{name: "agreeing then disagreeing", protocol: protocolNFS3, flags: []string{"v3", "nfsvers=4.1"}, wantErr: true},
		{name: "no protocol and v3", flags: []string{"v3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := map[string]string{}
			if tc.protocol != "" {
				ctx[volumeContextProtocol] = tc.protocol
			}
			option, err := nfsVersionOptionFor(ctx, tc.flags)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "one place")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, option, "an agreeing version must not be pinned a second time")
		})
	}
}

// End to end, the mount carries the class's version once, and an NFSv3
// mount still gets nolock however its version was named.
func TestNodePublishMountsAVersionOptionThatAgreesWithTheProtocol(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol string
		flags    []string
		want     []string
	}{
		{name: "nfs3 and v3", protocol: protocolNFS3, flags: []string{"v3"}, want: []string{"v3", "nolock"}},
		{name: "nfs4.1 and v4.1", protocol: protocolNFS41, flags: []string{"v4.1"}, want: []string{"v4.1"}},
		{name: "no protocol and v3", flags: []string{"v3"}, want: []string{"v3", "nolock"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)
			ctx := map[string]string{}
			if tc.protocol != "" {
				ctx[volumeContextProtocol] = tc.protocol
			}

			_, err := ns.NodePublishVolume(context.Background(),
				publishRequestFor(t, filepath.Join(t.TempDir(), "target"), ctx, tc.flags))

			require.NoError(t, err)
			require.Len(t, fm.MountPoints, 1)
			assert.Equal(t, tc.want, fm.MountPoints[0].Opts)
		})
	}
}

func TestNodeRPCsRefuseAPathOrHandleThatIsNotOne(t *testing.T) {
	good := filepath.Join(resolvedTempDir(t), "target")

	for _, tc := range []struct {
		name     string
		path     string
		volumeID string
	}{
		{name: "relative target path", path: "relative/target", volumeID: "vol-1"},
		{name: "climbing target path", path: "/var/lib/kubelet/../../etc/target", volumeID: "vol-1"},
		{name: "trailing dot element", path: "/var/lib/kubelet/pods/./target", volumeID: "vol-1"},
		{name: "NUL in the target path", path: "/var/lib/kubelet/target\x00/evil", volumeID: "vol-1"},
		{name: "newline in the target path", path: "/var/lib/kubelet/target\nmore", volumeID: "vol-1"},
		{name: "NUL in the handle", path: good, volumeID: "qumulo.example.com##/vol\x00"},
		{name: "newline in the handle", path: good, volumeID: "qumulo.example.com##/vol\nmount"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)
			ctx := context.Background()

			req := publishRequest(tc.path)
			req.VolumeId = tc.volumeID
			_, err := ns.NodePublishVolume(ctx, req)
			assertCode(t, err, codes.InvalidArgument)
			assert.Empty(t, fm.MountPoints)

			_, err = ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
				VolumeId: tc.volumeID, TargetPath: tc.path,
			})
			assertCode(t, err, codes.InvalidArgument)

			// NodeGetVolumeStats measures a path and changes nothing, and
			// the conformance suite sends it a relative path and requires
			// NotFound, so only the control characters are refused there.
			_, err = ns.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
				VolumeId: tc.volumeID, VolumePath: tc.path,
			})
			require.Error(t, err)
			if hasControlCharacters(tc.path) || hasControlCharacters(tc.volumeID) {
				assertCode(t, err, codes.InvalidArgument)
			}
		})
	}
}

// A handle carrying a NUL or a newline is refused where it is decoded too,
// so the controller never acts on one either.
func TestAHandleWithControlCharactersIsRefused(t *testing.T) {
	for name, id := range map[string]string{
		"NUL":             "qumulo.example.com##/csi-volumes/pvc-1\x00",
		"newline":         "qumulo.example.com##/csi-volumes/pvc-1\nrm -rf",
		"carriage return": "qumulo.example.com##/csi-volumes/pvc-1\r",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseVolumeID(id)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "NUL")
		})
	}
}

// --- Every missing field is InvalidArgument, with the code asserted ---

func TestEveryNodeRPCAnswersInvalidArgumentForAMissingField(t *testing.T) {
	good := filepath.Join(resolvedTempDir(t), "target")

	for _, tc := range []struct {
		name string
		call func(*NodeServer) error
	}{
		{
			name: "NodePublishVolume without a volume ID",
			call: func(ns *NodeServer) error {
				req := publishRequest(good)
				req.VolumeId = ""
				_, err := ns.NodePublishVolume(context.Background(), req)
				return err
			},
		},
		{
			name: "NodePublishVolume without a target path",
			call: func(ns *NodeServer) error {
				req := publishRequest("")
				_, err := ns.NodePublishVolume(context.Background(), req)
				return err
			},
		},
		{
			name: "NodePublishVolume without a capability",
			call: func(ns *NodeServer) error {
				req := publishRequest(good)
				req.VolumeCapability = nil
				_, err := ns.NodePublishVolume(context.Background(), req)
				return err
			},
		},
		{
			name: "NodePublishVolume without a server in the volume context",
			call: func(ns *NodeServer) error {
				req := publishRequest(good)
				req.VolumeContext = map[string]string{volumeContextExportPath: "/export"}
				_, err := ns.NodePublishVolume(context.Background(), req)
				return err
			},
		},
		{
			name: "NodePublishVolume without an export path in the volume context",
			call: func(ns *NodeServer) error {
				req := publishRequest(good)
				req.VolumeContext = map[string]string{volumeContextServer: "qumulo.example.com"}
				_, err := ns.NodePublishVolume(context.Background(), req)
				return err
			},
		},
		{
			name: "NodeUnpublishVolume without a volume ID",
			call: func(ns *NodeServer) error {
				_, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
					TargetPath: good,
				})
				return err
			},
		},
		{
			name: "NodeUnpublishVolume without a target path",
			call: func(ns *NodeServer) error {
				_, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
					VolumeId: "vol-1",
				})
				return err
			},
		},
		{
			name: "NodeGetVolumeStats without a volume ID",
			call: func(ns *NodeServer) error {
				_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
					VolumePath: good,
				})
				return err
			},
		},
		{
			name: "NodeGetVolumeStats without a volume path",
			call: func(ns *NodeServer) error {
				_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
					VolumeId: "vol-1",
				})
				return err
			},
		},
		{
			name: "NodeGetVolumeHealth without a volume ID",
			call: func(ns *NodeServer) error {
				_, err := ns.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns, _ := newTestNodeServerWithFakeMounter(t)

			err := tc.call(ns)

			assertCode(t, err, codes.InvalidArgument)
			assert.NotEmpty(t, strings.TrimSpace(err.Error()), "the refusal has to say what is missing")
		})
	}
}

// publishRequest is a publish that would succeed, for a test to spoil one
// field of.
func publishRequest(targetPath string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-1",
		TargetPath:       targetPath,
		VolumeCapability: mountVolumeCapability(),
		VolumeContext: map[string]string{
			volumeContextServer:     "qumulo.example.com",
			volumeContextExportPath: "/export",
		},
	}
}
