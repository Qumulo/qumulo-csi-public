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
	"strings"
	"sync/atomic"
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

// As in node_unmount_test.go, these names avoid "Node" on purpose so the
// darwin skip pattern does not swallow them: every server here is built over
// a FakeMounter and never reaches newRealMounter, so they run on a Mac as
// well as on CI.

// probeRecorder stands in for the create-and-unlink against the mount root.
// A real EROFS needs a read-only NFS export, which no unit test has.
type probeRecorder struct {
	err   error
	calls atomic.Int32
	roots []string
}

func (p *probeRecorder) probe(mountRoot string) error {
	p.calls.Add(1)
	p.roots = append(p.roots, mountRoot)
	return p.err
}

// erofs builds the error a read-only NFS export gives a create, in the shape
// the syscall layer actually produces it: a *os.PathError wrapping the errno.
func erofs() error {
	return &os.PathError{Op: "open", Path: "/target/.csi-qumulo-rw-probe-1", Err: syscall.EROFS}
}

func newProbeTestServer(t *testing.T, p *probeRecorder) (*NodeServer, *mount.FakeMounter) {
	t.Helper()

	d, err := NewDriver(Config{
		Name:        "csi.qumulo.com",
		Version:     "test",
		Mode:        ModeNode,
		NodeID:      "node-1",
		FakeMounter: true,
	})
	require.NoError(t, err)

	fm := mount.NewFakeMounter(nil)
	ns := NewNodeServerWithMounter(d, fm)
	ns.writeProbe = p.probe

	return ns, fm
}

// replicaPublishRequest is a publish of a volume, with or without the PV
// marker a tool such as qumulo-csi-repoint mirror sets. The marker changes
// nothing about the probe; it is kept in the request shape so both cases are
// exercised.
func replicaPublishRequest(targetPath string, replicated bool, readonly bool, mountFlags []string) *csi.NodePublishVolumeRequest {
	volumeContext := map[string]string{
		volumeContextServer:     "dr.qumulo.example.com",
		volumeContextExportPath: "/csi/volumes/pvc-a",
	}
	if replicated {
		volumeContext[volumeContextReplicated] = "true"
	}

	return &csi.NodePublishVolumeRequest{
		VolumeId:   "dr.qumulo.example.com/54321/pvc-a",
		TargetPath: targetPath,
		Readonly:   readonly,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{MountFlags: mountFlags},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		VolumeContext: volumeContext,
	}
}

// The ordinary case: a protected volume on the array that actually holds the
// writable copy. The probe runs, it succeeds, and the publish is unaffected.
func TestPublishProbesAProtectedVolumeAndSucceeds(t *testing.T) {
	p := &probeRecorder{}
	ns, fm := newProbeTestServer(t, p)
	targetPath := filepath.Join(t.TempDir(), "target")

	_, err := ns.NodePublishVolume(context.Background(), replicaPublishRequest(targetPath, true, false, nil))

	require.NoError(t, err)
	assert.Equal(t, int32(1), p.calls.Load(), "a protected volume must be probed")
	assert.Equal(t, []string{targetPath}, p.roots, "the probe belongs in the mount root")
	assert.Len(t, fm.MountPoints, 1, "and the mount stays")
}

// The case this rule exists for: a pod reaching the replica before somebody
// promoted it.
//
// Measured on the lab arrays: with the replica's export read-only, a create and an
// append both fail with `Read-only file system`. Without the read-only export
// the same pod would mount, write, and lose its appends silently.
// So EROFS is refused here rather than handed to the workload.
func TestPublishRefusesAReplicaThatIsStillReadOnly(t *testing.T) {
	p := &probeRecorder{err: erofs()}
	ns, fm := newProbeTestServer(t, p)
	targetPath := filepath.Join(t.TempDir(), "target")

	_, err := ns.NodePublishVolume(context.Background(), replicaPublishRequest(targetPath, true, false, nil))

	assertCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, err.Error(), replicaReadOnlyMessage,
		"support greps the runbook's wording out of the kubelet's events")
	assert.Empty(t, fm.MountPoints,
		"the read-only mount must be removed, or a republish finds it mounted and reports success")
}

// A read-only publish proves nothing by being read-only, so it is not probed.
//
// This is the normal way to run a reporting workload against a replica --
// ReadOnlyMany against the DR array before any failover -- and failing it
// would make the protection useless precisely where it is least needed.
func TestPublishSkipsTheProbeOnAReadOnlyPublish(t *testing.T) {
	for _, tc := range []struct {
		name       string
		readonly   bool
		mountFlags []string
	}{
		{name: "readonly flag on the request", readonly: true},
		// The class's own mountOptions are the other way a volume becomes
		// read-only, and confirmExistingMount already treats the two as one
		// question. The probe uses the same predicate, so both are covered.
		{name: "ro in the class mountOptions", mountFlags: []string{"ro"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &probeRecorder{err: erofs()}
			ns, fm := newProbeTestServer(t, p)
			targetPath := filepath.Join(t.TempDir(), "target")

			_, err := ns.NodePublishVolume(context.Background(),
				replicaPublishRequest(targetPath, true, tc.readonly, tc.mountFlags))

			require.NoError(t, err, "a read-only publish of a read-only replica is exactly what was asked for")
			assert.Zero(t, p.calls.Load(), "and must not be probed at all")
			assert.Len(t, fm.MountPoints, 1)
		})
	}
}

// A volume with no replication marker is probed exactly like a marked one.
//
// In the recommended topology -- one DNS name that floats, the same PV used
// on both sides -- nothing ever sets the marker, and a probe gated on it was
// inert precisely where the runbook sends people. So a read-write
// publish of an unmarked volume the array serves read-only is refused too.
func TestPublishProbesEveryReadWritePublish(t *testing.T) {
	t.Run("an unmarked writable volume is probed and publishes", func(t *testing.T) {
		p := &probeRecorder{}
		ns, fm := newProbeTestServer(t, p)
		targetPath := filepath.Join(t.TempDir(), "target")

		_, err := ns.NodePublishVolume(context.Background(), replicaPublishRequest(targetPath, false, false, nil))

		require.NoError(t, err)
		assert.Equal(t, int32(1), p.calls.Load(), "every read-write publish is probed, marker or not")
		assert.Len(t, fm.MountPoints, 1)
	})

	t.Run("an unmarked read-only replica is refused", func(t *testing.T) {
		p := &probeRecorder{err: erofs()}
		ns, fm := newProbeTestServer(t, p)
		targetPath := filepath.Join(t.TempDir(), "target")

		_, err := ns.NodePublishVolume(context.Background(), replicaPublishRequest(targetPath, false, false, nil))

		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), replicaReadOnlyMessage)
		assert.Contains(t, err.Error(), "publish the volume read-only", "the message names the remedy for an export that is read-only on purpose")
		assert.Empty(t, fm.MountPoints, "the read-only mount is not left behind")
	})
}

// Only EROFS refuses a publish.
//
// The risk this guards is the probe becoming a new way to break volumes that
// work today: a squashed root, a directory mode the pod's uid cannot write, a
// full quota. None of those is "this replica was not promoted", and none of
// them should stop a workload the array is perfectly willing to serve.
func TestPublishToleratesAnInconclusiveProbe(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "permission denied", err: &os.PathError{Op: "open", Path: "/t", Err: syscall.EACCES}},
		{name: "quota exhausted", err: &os.PathError{Op: "open", Path: "/t", Err: syscall.EDQUOT}},
		{name: "anything else", err: errors.New("no idea")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &probeRecorder{err: tc.err}
			ns, fm := newProbeTestServer(t, p)
			targetPath := filepath.Join(t.TempDir(), "target")

			_, err := ns.NodePublishVolume(context.Background(), replicaPublishRequest(targetPath, true, false, nil))

			require.NoError(t, err, "only EROFS means an unpromoted replica")
			assert.Len(t, fm.MountPoints, 1, "and the mount must survive an inconclusive probe")
		})
	}
}

// The real probe, against a real writable directory: it must leave nothing
// behind. A probe file left in a customer's volume is litter with this
// driver's name on it.
func TestWriteProbeLeavesNothingBehind(t *testing.T) {
	root := t.TempDir()

	require.NoError(t, probeWritableRoot(root))

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	for _, e := range entries {
		assert.False(t, strings.HasPrefix(e.Name(), replicaProbePrefix),
			"the probe file must be unlinked, found %q", e.Name())
	}
	assert.Empty(t, entries)
}

// And against a directory that does not exist, which is the closest a unit
// test gets to a refused create: the error is returned, not swallowed.
func TestWriteProbeReportsACreateItCouldNotMake(t *testing.T) {
	err := probeWritableRoot(filepath.Join(t.TempDir(), "nope"))

	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

// newFlakyProbeServer is newProbeTestServer over a flakyMounter, for the
// republish paths: a mount that finishes after the RPC gave up, and an
// unmount that fails.
func newFlakyProbeServer(t *testing.T, m *flakyMounter, p *probeRecorder) *NodeServer {
	t.Helper()

	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeNode, NodeID: "n", FakeMounter: true})
	require.NoError(t, err)
	ns := NewNodeServerWithMounter(d, m)
	ns.writeProbe = p.probe
	return ns
}

// A read-write publish whose mount outlived
// mountTimeout answered DeadlineExceeded, and the mount completed in the
// background. The kubelet's retry found the target mounted and was answered
// OK without the probe ever running -- an unpromoted replica published
// read-write.
func TestRepublishAfterATimedOutMountProbesTheReplica(t *testing.T) {
	block := make(chan struct{})
	m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil), blockMount: block}
	p := &probeRecorder{err: erofs()} // the array serves this volume read-only
	ns := newFlakyProbeServer(t, m, p)
	ns.mountTimeout = 20 * time.Millisecond
	target := resolvedTempDir(t)
	req := replicaPublishRequest(target, false, false, nil)

	_, err := ns.NodePublishVolume(context.Background(), req)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))

	close(block) // the kernel finishes the mount after the RPC gave up
	require.Eventually(t, func() bool {
		mps, _ := m.List()
		return len(mps) == 1
	}, 2*time.Second, 5*time.Millisecond)

	_, err = ns.NodePublishVolume(context.Background(), req)

	require.Equal(t, codes.FailedPrecondition, status.Code(err), "the retry must not publish a read-only replica")
	assert.Contains(t, status.Convert(err).Message(), replicaReadOnlyMessage)
	assert.EqualValues(t, 1, p.calls.Load(), "the retry probed the mount it found")
	// A mount this plugin never confirmed has no pod on it, so it is taken
	// away rather than left for a later publish to find.
	assert.Equal(t, []string{target}, m.unmounted)
}

// Second trigger: the EROFS refusal's unmount fails and leaves the
// read-only mount behind. Every retry is still refused, and the first
// refusal says a mount was left.
func TestRepublishAfterAFailedUnmountIsStillRefused(t *testing.T) {
	m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil), unmountErr: errors.New("umount: target is busy")}
	p := &probeRecorder{err: erofs()}
	ns := newFlakyProbeServer(t, m, p)
	target := resolvedTempDir(t)
	req := replicaPublishRequest(target, false, false, nil)

	_, err := ns.NodePublishVolume(context.Background(), req)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "could not be removed")

	for attempt := 2; attempt <= 3; attempt++ {
		_, err = ns.NodePublishVolume(context.Background(), req)
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "attempt %d", attempt)
		assert.Contains(t, status.Convert(err).Message(), replicaReadOnlyMessage)
	}
	assert.EqualValues(t, 3, p.calls.Load(), "every attempt probed")
}

// The controls. A writable volume's republish is probed and succeeds; a
// read-only republish proves nothing by being read-only and is not probed.
func TestRepublishOfAnExistingMount(t *testing.T) {
	t.Run("read-write, writable: probed and published", func(t *testing.T) {
		m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil)}
		p := &probeRecorder{}
		ns := newFlakyProbeServer(t, m, p)
		req := replicaPublishRequest(resolvedTempDir(t), false, false, nil)

		_, err := ns.NodePublishVolume(context.Background(), req)
		require.NoError(t, err)
		_, err = ns.NodePublishVolume(context.Background(), req)
		require.NoError(t, err)

		assert.EqualValues(t, 2, p.calls.Load())
		assert.Equal(t, 1, m.mounted, "the republish reused the mount")
	})

	t.Run("read-only publish of a read-only replica: not probed", func(t *testing.T) {
		m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil)}
		p := &probeRecorder{err: erofs()}
		ns := newFlakyProbeServer(t, m, p)
		req := replicaPublishRequest(resolvedTempDir(t), true, true, nil)

		_, err := ns.NodePublishVolume(context.Background(), req)
		require.NoError(t, err)
		_, err = ns.NodePublishVolume(context.Background(), req)
		require.NoError(t, err)

		assert.Zero(t, p.calls.Load())
	})
}

// ghostMountMounter reports every path as a mount point while listing no
// mounts: the racing-unmount shape confirmExistingMount answers without a
// mount table entry.
type ghostMountMounter struct{ *mount.FakeMounter }

func (ghostMountMounter) IsLikelyNotMountPoint(string) (bool, error) { return false, nil }
func (ghostMountMounter) List() ([]mount.MountPoint, error)          { return nil, nil }

// Review follow-up: a target that looks mounted but is absent from the
// mount table is still probed before a read-write publish is answered.
func TestRepublishOfAMountAbsentFromTheTableIsProbed(t *testing.T) {
	p := &probeRecorder{err: erofs()}
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeNode, NodeID: "n", FakeMounter: true})
	require.NoError(t, err)
	ns := NewNodeServerWithMounter(d, ghostMountMounter{mount.NewFakeMounter(nil)})
	ns.writeProbe = p.probe

	_, err = ns.NodePublishVolume(context.Background(), replicaPublishRequest(resolvedTempDir(t), false, false, nil))

	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.EqualValues(t, 1, p.calls.Load())
}

// The replica probe runs on every read-write republish, and
// on EROFS it unmounted the target. A mount a pod is running on that turned
// read-only under it -- a source frozen mid-failover -- was then taken away
// from the pod. Only a mount this plugin made without confirming it may be
// unmounted; any other is refused and left in place.
func TestRepublishOfAMountThatTurnedReadOnlyIsRefusedWithoutUnmount(t *testing.T) {
	m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil)}
	p := &probeRecorder{}
	ns := newFlakyProbeServer(t, m, p)
	req := replicaPublishRequest(resolvedTempDir(t), false, false, nil)
	_, err := ns.NodePublishVolume(context.Background(), req)
	require.NoError(t, err, "published read-write while the volume was writable")

	p.err = erofs() // the source is frozen under the running pod
	_, err = ns.NodePublishVolume(context.Background(), req)

	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "was not unmounted")
	assert.Empty(t, m.unmounted, "the running pod's mount is left in place")
	mps, _ := m.List()
	assert.Len(t, mps, 1)
}

// A mount whose export was removed answers EBADHANDLE (errno 521) to a
// real stat, but IsLikelyNotMountPoint reads cached attributes and saw a
// healthy mount point, so a republish confirmed the dead mount and answered
// OK. The republish now stats the target and remounts a stale one.
func TestRepublishRemountsAStaleTarget(t *testing.T) {
	for _, tt := range []struct {
		name      string
		statErr   error
		remounted bool
	}{
		{name: "EBADHANDLE", statErr: errEBADHANDLE, remounted: true},
		{name: "ESTALE", statErr: syscall.ESTALE, remounted: true},
		// mount-utils counts EACCES as a corrupted mount; a republish must
		// not unmount a pod's volume on a permission error.
		{name: "EACCES is not stale", statErr: syscall.EACCES, remounted: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := &flakyMounter{FakeMounter: mount.NewFakeMounter(nil)}
			ns := newFlakyProbeServer(t, m, &probeRecorder{})
			target := resolvedTempDir(t)
			req := replicaPublishRequest(target, false, false, nil)
			_, err := ns.NodePublishVolume(context.Background(), req)
			require.NoError(t, err)

			ns.statFunc = func(p string) (os.FileInfo, error) {
				if p == target {
					return nil, &os.PathError{Op: "stat", Path: p, Err: tt.statErr}
				}
				return os.Stat(p)
			}
			_, err = ns.NodePublishVolume(context.Background(), req)

			require.NoError(t, err)
			if tt.remounted {
				assert.Equal(t, []string{target}, m.unmounted, "the stale mount was removed")
				assert.Equal(t, 2, m.mounted, "and mounted again")
			} else {
				assert.Empty(t, m.unmounted)
				assert.Equal(t, 1, m.mounted)
			}
		})
	}
}

// A probe that does not finish -- a hung array -- used to answer the
// publish OK ("inconclusive; publishing anyway"), which skipped the replica
// check, and each republish of the hung mount started another probe beside
// the one still blocked, pinning another OS thread. A probe in flight is now
// joined, not repeated, and a timeout answers Unavailable, so the kubelet
// retries and the check is not skipped. The mount is left in place.
func TestAHungWriteProbeIsJoinedAndAnswersUnavailable(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var probes atomic.Int32
	ns, fm := newProbeTestServer(t, &probeRecorder{})
	ns.writeProbe = func(string) error {
		probes.Add(1)
		<-release
		return nil
	}
	ns.statTimeout = 50 * time.Millisecond
	targetPath := filepath.Join(t.TempDir(), "target")

	for range 5 {
		_, err := ns.NodePublishVolume(context.Background(), replicaPublishRequest(targetPath, false, false, nil))
		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	}
	assert.Equal(t, int32(1), probes.Load(), "five publishes of one hung mount ran one probe")
	assert.Len(t, fm.MountPoints, 1, "the mount is left for the retry")
}

// A write probe that finds the mount stale answers
// Unavailable, and the retry does remount the target, but a running
// container keeps its own bind of the dead mount: live, the pod kept getting
// errno 521 after the remount. The message used to say only "retrying
// remounts it", which reads as self-healing; it now gives the remedy the
// stats path already gives.
func TestAStaleWriteProbeTellsTheOperatorToDeleteThePod(t *testing.T) {
	ns, _ := newProbeTestServer(t, &probeRecorder{})
	ns.writeProbe = func(string) error { return errEBADHANDLE }
	targetPath := filepath.Join(t.TempDir(), "target")

	_, err := ns.NodePublishVolume(context.Background(), replicaPublishRequest(targetPath, false, false, nil))
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	assert.Contains(t, status.Convert(err).Message(), staleMountRemedy)
}
