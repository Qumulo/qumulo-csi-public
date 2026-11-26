// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// A replica's export is fenced read-only from the source side by
// Enable, Disable and the status poll, while the DR side may be promoting --
// which makes the target writable and then that export read-write. A fence
// after the promote's replumb took the live copy's writes away, and nothing
// undid it. Every fence now reads the target end just before its write and
// again after it, and undoes a write a promote overtook.

// gatedArray is a fakeArray whose next ListExports, once armed, parks until
// released: it holds one RPC between two of its reads of the peer.
type gatedArray struct {
	*fakeArray
	mu      sync.Mutex
	armed   bool
	blocked chan struct{} // closed when the gated call has parked
	release chan struct{} // closed by the test to let it continue
}

func (g *gatedArray) ListExports(ctx context.Context) ([]qumulo.ExportInfo, error) {
	g.mu.Lock()
	armed := g.armed
	g.armed = false
	blocked, release := g.blocked, g.release
	g.mu.Unlock()
	if armed {
		close(blocked)
		<-release
	}
	return g.fakeArray.ListExports(ctx)
}

func (g *gatedArray) arm() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed = true
	g.blocked = make(chan struct{})
	g.release = make(chan struct{})
}

// newGatedScenario is newReplScenario with B reached through a gatedArray.
func newGatedScenario(t *testing.T) (*replScenario, *gatedArray) {
	t.Helper()
	s := newReplScenario(t)
	gated := &gatedArray{fakeArray: s.b}
	s.driver.NewClient = func(baseURL, token string) qumulo.ClientInterface {
		if hostOfURL(baseURL) == arrayB {
			return gated
		}
		return s.fabric.newClient(baseURL, token)
	}
	return s, gated
}

// awaitParked waits for the gated call, or fails the test.
func awaitParked(t *testing.T, g *gatedArray, what string) {
	t.Helper()
	select {
	case <-g.blocked:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never reached its export read of the peer", what)
	}
}

// checkInfoPollLeavesAJustPromotedExportWritable is the fence race
// as it was found: a source-side status poll, as csi-addons sends
// it (no parameters), reads B's end as a read-only replica; a planned promote
// of B runs to completion; then the poll reads B's export, read-write now.
// The promoted copy's export must stay read-write.
func checkInfoPollLeavesAJustPromotedExportWritable(t *testing.T) {
	s, gated := newGatedScenario(t)
	rel := s.protect(t, classToB())
	require.Equal(t, qumulo.RelationshipEstablished, rel.state)
	replicaExport := exportAt(s.b, replVolumePath)
	require.NotNil(t, replicaExport)
	require.True(t, replicaExport.ReadOnly, "prestage leaves the replica export read-only")
	require.True(t, madeByThisDriver(replicaExport))

	gated.arm()
	infoDone := make(chan error, 1)
	go func() {
		_, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())
		infoDone <- err
	}()
	awaitParked(t, gated, "the status poll")

	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	promoted := s.fabric.onlyRel()
	require.Equal(t, qumulo.RelationshipDisconnected, promoted.state)
	require.False(t, promoted.targetRO, "B is the writable copy now")
	require.False(t, exportAt(s.b, replVolumePath).ReadOnly, "replumb made B's export read-write")

	close(gated.release)
	select {
	case err := <-infoDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the status poll never finished")
	}

	assert.False(t, exportAt(s.b, replVolumePath).ReadOnly,
		"the status poll set the promoted copy's export read-only: pods on the live copy get EROFS, "+
			"and csi-addons does not run Promote again on a Primary object")
	t.Logf("B calls: %v", s.b.calls)
}

func TestInfoPollDoesNotReFenceAJustPromotedReplicaExport(t *testing.T) {
	checkInfoPollLeavesAJustPromotedExportWritable(t)
}

// The probe's control: Enable's fence under the same interleaving. It read
// the end before its write already, and still does.
func TestEnableFenceLeavesAJustPromotedReplicaExportWritable(t *testing.T) {
	s, gated := newGatedScenario(t)
	s.protect(t, classToB())

	gated.arm()
	enableDone := make(chan error, 1)
	go func() { enableDone <- s.enable(t, classToB()) }()
	awaitParked(t, gated, "Enable")
	require.NoError(t, s.promote(t, s.handleB(t), classToA(), false))
	require.False(t, exportAt(s.b, replVolumePath).ReadOnly)
	close(gated.release)
	select {
	case err := <-enableDone:
		// OK, or a refusal over the promoted peer; never Internal.
		if err != nil {
			assert.NotEqual(t, codes.Internal, status.Code(err), "got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Enable never finished")
	}
	assert.False(t, exportAt(s.b, replVolumePath).ReadOnly, "Enable's fence set a promoted copy's export read-only")
}

// promoteDuringFence makes B's next SetExportReadOnly(true) land just after
// a promote: the target made writable and the export replumbed read-write,
// between the fence's read of the end and its write -- the window a read
// before the write cannot close.
func promoteDuringFence(s *replScenario) {
	done := false
	s.b.onExportWrite = func(exportID string, readOnly bool) {
		if !readOnly || done {
			return
		}
		done = true
		s.fabric.mu.Lock()
		for _, r := range s.fabric.rels {
			r.state = qumulo.RelationshipDisconnected
			r.targetRO = false
			r.sourceReportsState = qumulo.RelationshipEstablished
		}
		s.fabric.mu.Unlock()
		for i := range s.b.exports {
			if s.b.exports[i].ID == exportID {
				s.b.exports[i].ReadOnly = false // replumb
			}
		}
	}
}

func dl02Warnings(logged *bytes.Buffer) int {
	n := 0
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, "a promote landed while the replica export") {
			n++
		}
	}
	return n
}

// Each fence, with and without a promote landing between its read of the end
// and its write: overtaken, the export ends read-write with a WARN naming
// the promote; not, the fence is kept and nothing warns of it.
func TestEveryReplicaExportFenceUndoesAWriteAPromoteOvertook(t *testing.T) {
	fences := []struct {
		name string
		run  func(t *testing.T, s *replScenario)
	}{
		{"Enable", func(t *testing.T, s *replScenario) {
			// The promote leaves this Enable answering OK or refused; what
			// matters here is the export.
			_ = s.enable(t, classToB())
		}},
		{"Disable", func(t *testing.T, s *replScenario) {
			_ = s.disable(t, s.handleA, classToB("allowNonEmptyTarget", "true"))
		}},
		{"Info", func(t *testing.T, s *replScenario) {
			h, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())
			require.NoError(t, err)
			assert.NotContains(t, h.Message, "read-write", "a promoted copy's export is not drift")
		}},
	}
	for _, f := range fences {
		for _, promoted := range []bool{true, false} {
			name := f.name + "/kept"
			if promoted {
				name = f.name + "/promoteInTheWindow"
			}
			t.Run(name, func(t *testing.T) {
				s := newReplScenario(t)
				s.protect(t, classToB())
				require.Len(t, s.b.exports, 1)
				s.b.exports[0].ReadOnly = false // as an earlier build left it
				if promoted {
					promoteDuringFence(s)
				}
				logged := captureWarnings(t)
				before := len(s.b.calls)

				f.run(t, s)

				assert.Contains(t, s.b.calls[before:], "SetExportReadOnly", "the fence never wrote")
				if promoted {
					assert.False(t, s.b.exports[0].ReadOnly, "the fence left the promoted copy's export read-only")
					assert.Equal(t, 1, dl02Warnings(logged), "logged:\n%s", logged)
				} else {
					assert.True(t, s.b.exports[0].ReadOnly, "the fence was not kept over a read-only replica")
					assert.Zero(t, dl02Warnings(logged), "logged:\n%s", logged)
				}
			})
		}
	}
}

// A target end that cannot be read again after the write leaves the fence
// in place, with a WARN: the driver does not guess that a promote landed.
func TestAFenceWhoseEndCannotBeReadAgainIsKeptWithAWarning(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.b.exports[0].ReadOnly = false
	s.b.onExportWrite = func(string, bool) { s.b.down = true }
	t.Cleanup(func() { s.b.down = false })
	logged := captureWarnings(t)

	_, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())

	require.NoError(t, err)
	s.b.down = false
	assert.True(t, s.b.exports[0].ReadOnly)
	assert.Contains(t, logged.String(), "could not be read again")
}

// A promote overtaking the fence whose undo the array refuses leaves the
// export read-only over the live copy. Info reports it with the remedy,
// rather than reading it as fixed.
func TestInfoReportsAnOvertakenFenceItCouldNotUndo(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	s.b.exports[0].ReadOnly = false
	promoteDuringFence(s)
	fence := s.b.onExportWrite
	s.b.onExportWrite = func(exportID string, readOnly bool) {
		fence(exportID, readOnly)
		s.b.refuseExportWrites = !readOnly // the undo
	}

	h, err := s.server.AssessVolumeReplication(context.Background(), s.handleA, "", nil, replSecrets())

	require.NoError(t, err)
	assert.True(t, s.b.exports[0].ReadOnly)
	assert.Equal(t, ReplicationDegraded, h.Status, "message: %s", h.Message)
	assert.Contains(t, h.Message, "set it read-write")
	assert.Contains(t, h.Message, "as its target end was promoted")
}
