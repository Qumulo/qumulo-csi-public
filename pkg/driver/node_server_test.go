// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

func newTestNodeServer(t *testing.T) *NodeServer {
	t.Helper()

	d, err := NewDriver(Config{
		Name:    "csi.qumulo.com",
		Version: "0.1.0",
		NodeID:  "node-1",
		Server:  "qumulo.example.com",
		Client:  &MockClient{},
	})
	require.NoError(t, err)

	return NewNodeServer(d)
}

// newTestNodeServerWithFakeMounter returns a NodeServer backed by a
// mount.FakeMounter so tests can drive NodePublishVolume/NodeUnpublishVolume
// without touching the real filesystem's mount table.
func newTestNodeServerWithFakeMounter(t *testing.T) (*NodeServer, *mount.FakeMounter) {
	t.Helper()

	ns := newTestNodeServer(t)
	fm := mount.NewFakeMounter(nil)
	ns.mounter = fm

	return ns, fm
}

// resolvedTempDir returns a fresh temp directory with any symlinks (or, on
// Windows, short-path aliasing) resolved, matching the normalization that
// mount.FakeMounter applies internally so path comparisons in tests line up.
func resolvedTempDir(t *testing.T) string {
	t.Helper()

	resolved, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	return resolved
}

func mountVolumeCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
	}
}

func TestNodeGetInfo(t *testing.T) {
	ns := newTestNodeServer(t)

	resp, err := ns.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})

	require.NoError(t, err)
	assert.Equal(t, "node-1", resp.GetNodeId())
}

func TestNodeGetCapabilities(t *testing.T) {
	ns := newTestNodeServer(t)

	resp, err := ns.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})

	require.NoError(t, err)

	// Every advertised capability commits the driver to answering that RPC,
	// so the set is asserted exactly rather than loosely. Notably absent:
	// STAGE_UNSTAGE_VOLUME, which an NFS mount has no use for.
	got := make([]csi.NodeServiceCapability_RPC_Type, 0, len(resp.GetCapabilities()))
	for _, capability := range resp.GetCapabilities() {
		got = append(got, capability.GetRpc().GetType())
	}
	assert.ElementsMatch(t, []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
		// CSI v1.13's name for what earlier spec versions called
		// VOLUME_CONDITION: health moved from a field on
		// NodeGetVolumeStatsResponse to its own RPC. See NodeGetVolumeHealth.
		csi.NodeServiceCapability_RPC_GET_VOLUME_HEALTH,
	}, got)
}

// NodeStageVolume/NodeUnstageVolume/NodeExpandVolume are deliberately
// unimplemented (see node_server.go), and this pins that behavior so a
// future change can't silently start answering OK for an RPC the driver
// never advertises via NodeGetCapabilities.
func TestNodeStageVolume_Unimplemented(t *testing.T) {
	ns := newTestNodeServer(t)

	resp, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: filepath.Join(t.TempDir(), "staging"),
		VolumeCapability:  mountVolumeCapability(),
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestNodeUnstageVolume_Unimplemented(t *testing.T) {
	ns := newTestNodeServer(t)

	resp, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: filepath.Join(t.TempDir(), "staging"),
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestNodeExpandVolume_Unimplemented(t *testing.T) {
	ns := newTestNodeServer(t)

	resp, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:   "vol-1",
		VolumePath: filepath.Join(t.TempDir(), "target"),
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestNodePublishVolume_InvalidArgument(t *testing.T) {
	targetPath := filepath.Join(t.TempDir(), "target")

	tests := []struct {
		name string
		req  *csi.NodePublishVolumeRequest
	}{
		{
			name: "missing volume capability",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:   "vol-1",
				TargetPath: targetPath,
			},
		},
		{
			name: "missing volume id",
			req: &csi.NodePublishVolumeRequest{
				VolumeCapability: mountVolumeCapability(),
				TargetPath:       targetPath,
			},
		},
		{
			name: "missing target path",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:         "vol-1",
				VolumeCapability: mountVolumeCapability(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, _ := newTestNodeServerWithFakeMounter(t)

			resp, err := ns.NodePublishVolume(context.Background(), tt.req)

			require.Error(t, err)
			assert.Nil(t, resp)
		})
	}
}

func TestNodePublishVolume_Success(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	targetPath := filepath.Join(t.TempDir(), "target")

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-1",
		TargetPath:       targetPath,
		VolumeCapability: mountVolumeCapability(),
		VolumeContext: map[string]string{
			volumeContextServer:     "qumulo.example.com",
			volumeContextExportPath: "/export",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Len(t, fm.MountPoints, 1)
	assert.Equal(t, "qumulo.example.com:/export", fm.MountPoints[0].Device)
}

func TestNodePublishVolume_AlreadyMounted(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	targetPath := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{Path: targetPath, Device: "qumulo.example.com:/export"}}

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-1",
		TargetPath:       targetPath,
		VolumeCapability: mountVolumeCapability(),
		VolumeContext: map[string]string{
			volumeContextServer:     "qumulo.example.com",
			volumeContextExportPath: "/export",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, fm.MountPoints, 1, "no new mount should be recorded for an already-mounted target")
}

func TestNodeUnpublishVolume_InvalidArgument(t *testing.T) {
	tests := []struct {
		name string
		req  *csi.NodeUnpublishVolumeRequest
	}{
		{
			name: "missing volume id",
			req:  &csi.NodeUnpublishVolumeRequest{TargetPath: "/some/path"},
		},
		{
			name: "missing target path",
			req:  &csi.NodeUnpublishVolumeRequest{VolumeId: "vol-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, _ := newTestNodeServerWithFakeMounter(t)

			resp, err := ns.NodeUnpublishVolume(context.Background(), tt.req)

			require.Error(t, err)
			assert.Nil(t, resp)
		})
	}
}

func TestNodeUnpublishVolume_Success(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	targetPath := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{Path: targetPath}}

	resp, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: targetPath,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Empty(t, fm.MountPoints)
	_, err = os.Stat(targetPath)
	assert.True(t, os.IsNotExist(err), "target path should be removed after unpublish")
}

func TestNodeUnpublishVolume_NotMounted(t *testing.T) {
	ns, _ := newTestNodeServerWithFakeMounter(t)
	targetPath := filepath.Join(t.TempDir(), "missing")

	resp, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: targetPath,
	})

	require.NoError(t, err)
	assert.NotNil(t, resp)
}

// A volume context missing either key is a malformed request, not something to
// paper over: without this the driver mounts "<server>:" or ":<path>" and the
// failure surfaces from mount(8) with nothing pointing at the real cause.
func TestNodePublishVolume_MissingVolumeContext(t *testing.T) {
	tests := []struct {
		name          string
		volumeContext map[string]string
		wantMessage   string
	}{
		{
			name:          "both keys absent",
			volumeContext: map[string]string{},
			wantMessage:   "server, exportPath",
		},
		{
			name:          "nil context",
			volumeContext: nil,
			wantMessage:   "server, exportPath",
		},
		{
			name:          "server only",
			volumeContext: map[string]string{volumeContextServer: "qumulo.example.com"},
			wantMessage:   "exportPath",
		},
		{
			name:          "export path only",
			volumeContext: map[string]string{volumeContextExportPath: "/export"},
			wantMessage:   "server",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)

			resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
				VolumeId:         "vol-1",
				TargetPath:       filepath.Join(t.TempDir(), "target"),
				VolumeCapability: mountVolumeCapability(),
				VolumeContext:    tt.volumeContext,
			})

			require.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), tt.wantMessage)
			assert.Empty(t, fm.MountPoints, "nothing should be mounted for a malformed request")
		})
	}
}

// The controller rejects block volumes at provision time, but it runs in a
// different process; a node-only instance has to make the same check or it
// answers a block request with an NFS mount.
func TestNodePublishVolume_BlockCapabilityRejected(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: filepath.Join(t.TempDir(), "target"),
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		VolumeContext: map[string]string{
			volumeContextServer:     "qumulo.example.com",
			volumeContextExportPath: "/export",
		},
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "block volumes are not supported")
	assert.Empty(t, fm.MountPoints)
}

// Republishing read-only over a read-write mount must not report success: the
// CO would believe the workload had been downgraded to read-only when it can
// still write.
func TestNodePublishVolume_ReadonlyMismatchRejected(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	targetPath := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{
		Path:   targetPath,
		Device: "qumulo.example.com:/export",
		Opts:   []string{"rw"},
	}}

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-1",
		TargetPath:       targetPath,
		VolumeCapability: mountVolumeCapability(),
		Readonly:         true,
		VolumeContext: map[string]string{
			volumeContextServer:     "qumulo.example.com",
			volumeContextExportPath: "/export",
		},
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Contains(t, err.Error(), "readonly")
}

// A read-only republish over a read-only mount is the same request twice, so
// it stays idempotent.
// A StorageClass with mountOptions: [ro] must survive a pod restart.
//
// This is the shape the existing republish test missed. It sets Readonly on
// the request, so both sides of the comparison agreed; a class that expresses
// read-only through mountOptions instead -- the normal way to do it in a chart
// -- mounted "ro" and then failed every subsequent publish with AlreadyExists,
// because the mounted options were compared against the access mode rather
// than against what was actually asked for.
func TestNodePublishVolume_StorageClassReadOnlyRepublishIsIdempotent(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	targetPath := resolvedTempDir(t)

	// First publish: nothing mounted, so the volume is mounted with the
	// class's own options.
	req := publishRequestFor(t, targetPath, nil, []string{"ro"})
	_, err := ns.NodePublishVolume(context.Background(), req)
	require.NoError(t, err, "the first publish has always worked; it is the second that broke")

	require.Len(t, fm.MountPoints, 1)
	assert.Contains(t, fm.MountPoints[0].Opts, "ro", "the class's mountOptions must reach the mount")
	// Readonly is false throughout: the access mode is MULTI_NODE_MULTI_WRITER,
	// which is exactly the combination that used to wedge.
	assert.False(t, req.GetReadonly())

	// The republish, as the kubelet issues it after a pod restart.
	_, err = ns.NodePublishVolume(context.Background(), publishRequestFor(t, targetPath, nil, []string{"ro"}))

	require.NoError(t, err, "republishing a mountOptions:[ro] volume must not fail")
	assert.Len(t, fm.MountPoints, 1, "no second mount for a repeated request")
}

// The genuine incompatibility the check exists for must still be reported: a
// target mounted read-only where nothing in this request asks for read-only.
// The fix compares like with like; it does not stop comparing.
func TestNodePublishVolume_ReadonlyMismatchIsStillRejected(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	targetPath := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{
		Path:   targetPath,
		Device: "qumulo.example.com:/export",
		Opts:   []string{"ro"},
	}}

	// No mountOptions, Readonly false: read-only is neither requested nor
	// implied, so the existing read-only mount really is incompatible.
	_, err := ns.NodePublishVolume(context.Background(), publishRequestFor(t, targetPath, nil, nil))

	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestNodePublishVolume_ReadonlyRepublishIsIdempotent(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	targetPath := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{
		Path:   targetPath,
		Device: "qumulo.example.com:/export",
		Opts:   []string{"ro"},
	}}

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-1",
		TargetPath:       targetPath,
		VolumeCapability: mountVolumeCapability(),
		Readonly:         true,
		VolumeContext: map[string]string{
			volumeContextServer:     "qumulo.example.com",
			volumeContextExportPath: "/export",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, fm.MountPoints, 1, "no second mount for a repeated request")
}

// A different export already mounted at the target is not this volume, and
// mounting over it would hide whatever is there.
func TestNodePublishVolume_DifferentSourceRejected(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)
	targetPath := resolvedTempDir(t)
	fm.MountPoints = []mount.MountPoint{{
		Path:   targetPath,
		Device: "other.example.com:/somewhere-else",
	}}

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-1",
		TargetPath:       targetPath,
		VolumeCapability: mountVolumeCapability(),
		VolumeContext: map[string]string{
			volumeContextServer:     "qumulo.example.com",
			volumeContextExportPath: "/export",
		},
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Contains(t, err.Error(), "other.example.com:/somewhere-else")
}

// publishRequestFor builds a valid publish request so protocol tests can vary
// one thing at a time.
func publishRequestFor(t *testing.T, targetPath string, volumeContext map[string]string, mountFlags []string) *csi.NodePublishVolumeRequest {
	t.Helper()

	base := map[string]string{
		volumeContextServer:     "qumulo.example.com",
		volumeContextExportPath: "/export",
	}
	for k, v := range volumeContext {
		base[k] = v
	}

	return &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{MountFlags: mountFlags},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		VolumeContext: base,
	}
}

func TestNodePublishVolume_ProtocolMountOption(t *testing.T) {
	tests := []struct {
		name       string
		protocol   string
		mountFlags []string
		wantOpts   []string
	}{
		{
			// Volumes provisioned before the protocol parameter existed carry
			// no value. Adding a version for them would remount every existing
			// PV differently on upgrade.
			name:     "absent protocol adds no version option",
			protocol: "",
			wantOpts: []string{},
		},
		{
			// nolock comes with nfs3 unasked: the node image runs no
			// rpc.statd, so a v3 mount without it fails. See
			// TestNodePublishVolumeAddsNolockForNFS3.
			name:     "nfs3",
			protocol: protocolNFS3,
			wantOpts: []string{"nfsvers=3", "nolock"},
		},
		{
			name:     "nfs4.1",
			protocol: protocolNFS41,
			wantOpts: []string{"nfsvers=4.1"},
		},
		{
			name:       "protocol is appended after the class's own mount options",
			protocol:   protocolNFS41,
			mountFlags: []string{"hard", "timeo=600"},
			wantOpts:   []string{"hard", "timeo=600", "nfsvers=4.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)
			volumeContext := map[string]string{}
			if tt.protocol != "" {
				volumeContext[volumeContextProtocol] = tt.protocol
			}

			req := publishRequestFor(t, filepath.Join(t.TempDir(), "target"), volumeContext, tt.mountFlags)
			_, err := ns.NodePublishVolume(context.Background(), req)

			require.NoError(t, err)
			require.Len(t, fm.MountPoints, 1)
			assert.Equal(t, tt.wantOpts, fm.MountPoints[0].Opts)
		})
	}
}

// Pinning the version in two places means one of them loses silently, and the
// volume mounts with a version nobody asked for.
func TestNodePublishVolume_ProtocolConflictsWithMountOptions(t *testing.T) {
	tests := []struct {
		name string
		flag string
	}{
		{name: "nfsvers", flag: "nfsvers=3"},
		{name: "vers synonym", flag: "vers=3"},
		{name: "bare shorthand", flag: "v3"},
		{name: "another minor", flag: "v4.2"},
		{name: "mixed case", flag: "NFSVERS=3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)

			req := publishRequestFor(t,
				filepath.Join(t.TempDir(), "target"),
				map[string]string{volumeContextProtocol: protocolNFS41},
				[]string{tt.flag},
			)
			resp, err := ns.NodePublishVolume(context.Background(), req)

			require.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), "set the version in one place")
			assert.Empty(t, fm.MountPoints, "nothing should be mounted for a contradictory request")
		})
	}
}

// A mount option that merely contains "vers" is not a version selector, and
// rejecting it would refuse a legitimate mount.
func TestNodePublishVolume_ProtocolIgnoresUnrelatedOptions(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)

	req := publishRequestFor(t,
		filepath.Join(t.TempDir(), "target"),
		map[string]string{volumeContextProtocol: protocolNFS3},
		[]string{"noversdummy", "retrans=2"},
	)
	_, err := ns.NodePublishVolume(context.Background(), req)

	require.NoError(t, err)
	require.Len(t, fm.MountPoints, 1)
	assert.Equal(t, []string{"noversdummy", "retrans=2", "nfsvers=3", "nolock"}, fm.MountPoints[0].Opts)
}

// VolumeContext comes from a PV the CO stored, not from a freshly validated
// StorageClass, so it can carry anything a hand-edited object contains.
func TestNodePublishVolume_UnsupportedProtocolInVolumeContext(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)

	req := publishRequestFor(t,
		filepath.Join(t.TempDir(), "target"),
		map[string]string{volumeContextProtocol: "nfs4.2"},
		nil,
	)
	resp, err := ns.NodePublishVolume(context.Background(), req)

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), `unsupported protocol "nfs4.2"`)
	assert.Empty(t, fm.MountPoints)
}

// The node service is stateless across clusters by construction: it composes
// its mount source from the VolumeContext alone. This asserts that two volumes
// naming different clusters mount from different sources in the same process --
// the node-side half of the multi-cluster model.
//
// It also guards the property that keeps a cluster-wide credential off every
// worker: the node needs no client and no token to do this.
func TestNodePublishVolumeRoutesPerVolumeContext(t *testing.T) {
	ns, fm := newTestNodeServerWithFakeMounter(t)

	for _, tc := range []struct {
		volumeID   string
		server     string
		exportPath string
	}{
		{volumeID: "cluster-a/1/pvc-a", server: "cluster-a.example.com", exportPath: "/csi/a/pvc-a"},
		{volumeID: "cluster-b/2/pvc-b", server: "cluster-b.example.com", exportPath: "/csi/b/pvc-b"},
	} {
		target := filepath.Join(t.TempDir(), "mnt")
		_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
			VolumeId:         tc.volumeID,
			TargetPath:       target,
			VolumeCapability: mountVolumeCapability(),
			VolumeContext: map[string]string{
				volumeContextServer:     tc.server,
				volumeContextExportPath: tc.exportPath,
			},
		})
		require.NoError(t, err)
	}

	require.Len(t, fm.MountPoints, 2)
	assert.Equal(t, "cluster-a.example.com:/csi/a/pvc-a", fm.MountPoints[0].Device)
	assert.Equal(t, "cluster-b.example.com:/csi/b/pvc-b", fm.MountPoints[1].Device,
		"the second volume must mount from its own cluster, not the first's")
}

// flakyMounter is a FakeMounter that can be told to fail specific calls, so
// the stale-mount and timeout paths can be exercised without a real mount.
//
// A real ESTALE needs a live NFS mount whose server has gone away, which
// cannot be produced honestly in a unit test. So
// the *error handling* is tested here with the error the kernel would give,
// and the fact that the kernel gives it is taken from mount-utils'
// IsCorruptedMnt, which enumerates exactly those errnos.
type flakyMounter struct {
	*mount.FakeMounter

	// checkErr is returned by the first IsLikelyNotMountPoint call, then
	// cleared, so a retry sees a working filesystem.
	checkErr error
	// unmountErr, when set, fails Unmount.
	unmountErr error
	// blockMount, when set, makes Mount hang until it is closed.
	blockMount chan struct{}

	unmounted []string
	mounted   int
}

func (f *flakyMounter) IsLikelyNotMountPoint(file string) (bool, error) {
	if f.checkErr != nil {
		err := f.checkErr
		f.checkErr = nil
		return false, err
	}
	return f.FakeMounter.IsLikelyNotMountPoint(file)
}

func (f *flakyMounter) Unmount(target string) error {
	f.unmounted = append(f.unmounted, target)
	if f.unmountErr != nil {
		return f.unmountErr
	}
	return f.FakeMounter.Unmount(target)
}

func (f *flakyMounter) Mount(source, target, fstype string, options []string) error {
	if f.blockMount != nil {
		<-f.blockMount
	}
	f.mounted++
	return f.FakeMounter.Mount(source, target, fstype, options)
}

// staleError builds the error a syscall against a dead NFS mount produces.
func staleError() error {
	return &os.PathError{Op: "stat", Path: "/target", Err: syscall.ESTALE}
}

func newFlakyNodeServer(t *testing.T, m *flakyMounter) *NodeServer {
	t.Helper()

	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeNode, NodeID: "n"})
	require.NoError(t, err)
	return NewNodeServerWithMounter(d, m)
}

// A stale mount must be recovered from, not reported forever.
//
// mount.IsCorruptedMnt appeared nowhere in this tree, so an ESTALE handle --
// a Qumulo reboot, a failover, an export removed and recreated -- fell into
// the "everything else" branch and returned Internal on every retry. The
// volume stayed unpublishable on that node until someone unmounted it by hand.
func TestNodePublishVolume_RecoversFromAStaleMount(t *testing.T) {
	m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil), checkErr: staleError()}
	ns := newFlakyNodeServer(t, m)
	targetPath := resolvedTempDir(t)

	_, err := ns.NodePublishVolume(context.Background(), publishRequestFor(t, targetPath, nil, nil))

	require.NoError(t, err, "a stale mount must be recovered from, not surfaced on every retry")
	assert.Equal(t, []string{targetPath}, m.unmounted, "the dead mount must be forced away first")
	assert.Equal(t, 1, m.mounted, "and then a fresh mount made")
}

// If the dead mount cannot be removed, say so rather than mounting over it.
func TestNodePublishVolume_StaleMountThatCannotBeUnmounted(t *testing.T) {
	m := &flakyMounter{
		FakeMounter: mount.NewFakeMounter(nil),
		checkErr:    staleError(),
		unmountErr:  errors.New("device is busy"),
	}
	ns := newFlakyNodeServer(t, m)
	targetPath := resolvedTempDir(t)

	_, err := ns.NodePublishVolume(context.Background(), publishRequestFor(t, targetPath, nil, nil))

	assertCode(t, err, codes.Internal)
	assert.Zero(t, m.mounted, "must not mount over a stale mount that is still there")
}

// Only a corrupted mount triggers a force-unmount. Any other error from the
// mount-point check is still an error.
//
// This guards the risk directly: conflating a slow mount with a dead one
// would force-unmount a working volume and throw away its writes.
func TestNodePublishVolume_OtherCheckErrorsDoNotForceUnmount(t *testing.T) {
	m := &flakyMounter{
		FakeMounter: mount.NewFakeMounter(nil),
		checkErr:    &os.PathError{Op: "stat", Path: "/target", Err: syscall.ENOMEM},
	}
	ns := newFlakyNodeServer(t, m)
	targetPath := resolvedTempDir(t)

	_, err := ns.NodePublishVolume(context.Background(), publishRequestFor(t, targetPath, nil, nil))

	assertCode(t, err, codes.Internal)
	assert.Empty(t, m.unmounted, "a non-corrupted error must never force an unmount")
	assert.Zero(t, m.mounted)
}

// A mount that never returns must bound the RPC, not the call.
//
// Nothing in node_server.go was bounded by a context or a deadline, so one
// wedged mount held a gRPC call open indefinitely.
func TestNodePublishVolume_TimesOutOnAWedgedMount(t *testing.T) {
	block := make(chan struct{})
	m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil), blockMount: block}
	ns := newFlakyNodeServer(t, m)
	ns.mountTimeout = 20 * time.Millisecond
	targetPath := resolvedTempDir(t)

	done := make(chan error, 1)
	go func() {
		_, err := ns.NodePublishVolume(context.Background(), publishRequestFor(t, targetPath, nil, nil))
		done <- err
	}()

	select {
	case err := <-done:
		// DeadlineExceeded says "not yet", which is what the CO should hear
		// about a mount that may still be retrying in the kernel.
		assertCode(t, err, codes.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("NodePublishVolume did not return: the mount timeout is not bounding the RPC")
	}

	// Let the blocked mount finish so the goroutine ends rather than being
	// left behind by the test.
	close(block)
}

// A caller that gives up first gets its own cancellation back, not a timeout.
func TestNodePublishVolume_HonoursCallerCancellation(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil), blockMount: block}
	ns := newFlakyNodeServer(t, m)
	ns.mountTimeout = time.Hour // never fires; the context must be what ends this
	targetPath := resolvedTempDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ns.NodePublishVolume(ctx, publishRequestFor(t, targetPath, nil, nil))
		done <- err
	}()
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.NotEqual(t, codes.DeadlineExceeded, status.Code(err),
			"a cancelled caller is not a mount timeout")
	case <-time.After(5 * time.Second):
		t.Fatal("NodePublishVolume ignored its context")
	}
}

// NodeGetVolumeStats must name a stale mount rather than reporting an
// anonymous Internal.
//
// The metric that would tell an operator something is wrong --
// kubelet_volume_stats_* -- is disabled by the same fault that breaks the
// volume, so the error text is the only signal left.
func TestVolumePathStatErrorClassification(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantText string
	}{
		{
			name:     "missing path is NotFound, per the spec",
			err:      &os.PathError{Op: "stat", Path: "/v", Err: syscall.ENOENT},
			wantCode: codes.NotFound,
			wantText: "does not exist",
		},
		{
			name:     "a stale handle is named",
			err:      &os.PathError{Op: "stat", Path: "/v", Err: syscall.ESTALE},
			wantCode: codes.Internal,
			wantText: "stale mount",
		},
		{
			// What a mount answers after its export was removed on the
			// array, even once it is re-created. mount.IsCorruptedMnt misses it.
			name:     "so is a handle the server no longer recognises (EBADHANDLE)",
			err:      &os.PathError{Op: "statfs", Path: "/v", Err: errEBADHANDLE},
			wantCode: codes.Internal,
			wantText: "stale mount",
		},
		{
			name:     "so is a disconnected one",
			err:      &os.PathError{Op: "stat", Path: "/v", Err: syscall.ENOTCONN},
			wantCode: codes.Internal,
			wantText: "stale mount",
		},
		{
			name:     "anything else stays a plain Internal",
			err:      &os.PathError{Op: "stat", Path: "/v", Err: syscall.ENOMEM},
			wantCode: codes.Internal,
			wantText: "checking volume path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := volumePathStatError("/v", tt.err)
			assertCode(t, err, tt.wantCode)
			assert.Contains(t, err.Error(), tt.wantText)
		})
	}
}

// The node image ships mount.nfs but runs no rpc.statd, so an NFSv3 mount
// without nolock fails outright -- and when it does not fail it is worse,
// because locking is silently broken instead. Nothing used to add it: the
// chart's rendered default is [hard, timeo=600], and an explicit mountOptions
// list is rendered verbatim, so an nfs3 class that omitted the option was a
// guaranteed-broken volume with no warning anywhere.
//
// Injection is at the node rather than a refusal at CreateVolume because a PV
// provisioned before this existed carries its class's mountOptions frozen in
// it: only the node can fix those, and it fixes them on the next publish.
func TestNodePublishVolumeAddsNolockForNFS3(t *testing.T) {
	tests := []struct {
		name       string
		protocol   string
		mountFlags []string
		wantOpts   []string
	}{
		{
			name:     "nfs3 with no options at all",
			protocol: protocolNFS3,
			wantOpts: []string{"nfsvers=3", "nolock"},
		},
		{
			// The case the chart now produces by default, and the one that
			// was broken in the field.
			name:       "nfs3 with the chart's default options",
			protocol:   protocolNFS3,
			mountFlags: []string{"hard", "timeo=600"},
			wantOpts:   []string{"hard", "timeo=600", "nfsvers=3", "nolock"},
		},
		{
			name:       "nfs3 already carrying nolock is not given a second one",
			protocol:   protocolNFS3,
			mountFlags: []string{"nolock", "hard"},
			wantOpts:   []string{"nolock", "hard", "nfsvers=3"},
		},
		{
			name:       "nolock in mixed case still counts",
			protocol:   protocolNFS3,
			mountFlags: []string{"NoLock"},
			wantOpts:   []string{"NoLock", "nfsvers=3"},
		},
		{
			// nolock is shorthand for exactly this, so a class that spelled it
			// out longhand has already answered the question and must not have
			// a contradicting option appended after it.
			name:       "an explicit local_lock is left alone",
			protocol:   protocolNFS3,
			mountFlags: []string{"local_lock=all"},
			wantOpts:   []string{"local_lock=all", "nfsvers=3"},
		},
		{
			name:       "local_lock=none is also the operator's decision",
			protocol:   protocolNFS3,
			mountFlags: []string{"local_lock=none"},
			wantOpts:   []string{"local_lock=none", "nfsvers=3"},
		},
		{
			// v4 locks in-protocol and ignores nolock, so adding it would put
			// a misleading option in every v4.1 mount table for no effect.
			name:     "nfs4.1 is given no lock option",
			protocol: protocolNFS41,
			wantOpts: []string{"nfsvers=4.1"},
		},
		{
			// Same reasoning as nfsVersionOptionFor: the version is
			// negotiated, so the driver does not know this will land on v3 and
			// must not guess.
			name:     "an absent protocol is left to the kernel",
			protocol: "",
			wantOpts: []string{},
		},
		{
			// "lock" is a prefix of "local_lock=" and a bare word; neither
			// this nor "nolockdummy" is a lock selector.
			name:       "an option that merely contains lock is not a selector",
			protocol:   protocolNFS3,
			mountFlags: []string{"nolockdummy"},
			wantOpts:   []string{"nolockdummy", "nfsvers=3", "nolock"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)
			volumeContext := map[string]string{}
			if tt.protocol != "" {
				volumeContext[volumeContextProtocol] = tt.protocol
			}

			req := publishRequestFor(t, filepath.Join(t.TempDir(), "target"), volumeContext, tt.mountFlags)
			_, err := ns.NodePublishVolume(context.Background(), req)

			require.NoError(t, err)
			require.Len(t, fm.MountPoints, 1)
			assert.Equal(t, tt.wantOpts, fm.MountPoints[0].Opts)
		})
	}
}

// An nfs3 class asking for server-side locking is refused, not rewritten.
//
// Overriding it would hand the operator the exact opposite of what they asked
// for, on the one setting they were being deliberate about. The version
// conflict in TestNodePublishVolume_ProtocolConflictsWithMountOptions is
// refused for the same reason.
func TestNodePublishVolumeRefusesNFS3AskingForServerLocks(t *testing.T) {
	for _, flag := range []string{"lock", "LOCK", " lock "} {
		t.Run(flag, func(t *testing.T) {
			ns, fm := newTestNodeServerWithFakeMounter(t)

			req := publishRequestFor(t,
				filepath.Join(t.TempDir(), "target"),
				map[string]string{volumeContextProtocol: protocolNFS3},
				[]string{flag},
			)
			resp, err := ns.NodePublishVolume(context.Background(), req)

			require.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), "rpc.statd")
			assert.Contains(t, err.Error(), protocolNFS41,
				"the message must name the protocol where locking does work")
			assert.Empty(t, fm.MountPoints, "nothing should be mounted for a contradictory request")
		})
	}
}

// mount-utils joins mountOptions entries with commas, so "hard,ro" is
// one entry that mounts hard and read-only. The guards compared whole entries:
// "hard,ro" read as read-write, so its write probe refused every publish as
// an unpromoted replica; "hard,lock" on NFSv3 was not refused, and silently
// got nolock; "hard,vers=4.1" escaped the version-conflict refusal. Each
// option is now checked on its own.
func TestMountOptionGuardsSeeEveryOptionInACommaJoinedEntry(t *testing.T) {
	assert.True(t, isReadOnly([]string{"hard,ro"}))
	assert.True(t, isReadOnly([]string{"hard, ro"}), "spaces around the comma")
	assert.False(t, isReadOnly([]string{"hard,rw"}))
	assert.False(t, isReadOnly([]string{"hard,rootcontext=x"}), "ro is a whole option, not a prefix")

	_, err := nfsLockOptionFor(map[string]string{volumeContextProtocol: protocolNFS3}, []string{"hard,lock"})
	require.Error(t, err, "an explicit lock is refused however it is written")

	_, err = nfsVersionOptionFor(map[string]string{volumeContextProtocol: protocolNFS3}, []string{"hard,vers=4.1"})
	require.Error(t, err, "a version contradiction is refused however it is written")

	lock, err := nfsLockOptionFor(map[string]string{volumeContextProtocol: protocolNFS3}, []string{"hard,nolock"})
	require.NoError(t, err)
	assert.Empty(t, lock, "an explicit nolock is the operator's answer, and nothing is added")
}

// A class with no protocol that pins NFSv3 in mountOptions mounted
// without nolock -- nolock was added for protocol: nfs3 only -- and the node
// image runs no rpc.statd, so the mount failed. A pinned v3 is NFSv3 too.
// An absent protocol with no pin still negotiates, and gets nothing added.
func TestAVersionPinnedToNFSv3GetsNolock(t *testing.T) {
	for _, tc := range []struct {
		options []string
		want    string
	}{
		{[]string{"vers=3"}, "nolock"},
		{[]string{"hard,nfsvers=3"}, "nolock"},
		{[]string{"vers=3.0"}, "nolock"},
		{[]string{"vers=3", "nolock"}, ""},
		{[]string{"vers=4.1"}, ""},
		{[]string{"hard"}, ""},
		{nil, ""},
	} {
		got, err := nfsLockOptionFor(map[string]string{}, tc.options)
		require.NoError(t, err, "%v", tc.options)
		assert.Equal(t, tc.want, got, "%v", tc.options)
	}
	_, err := nfsLockOptionFor(map[string]string{}, []string{"vers=3,lock"})
	require.Error(t, err, "an explicit lock on a pinned v3 is refused as it is for protocol: nfs3")
}
