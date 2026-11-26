// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// A copy can end up
// the writable source without a completed promote -- a promote answered
// "retry" and Enable re-protected the copy first -- and keep the read-only
// export it had as a replica. Every row that then answers OK must leave the
// volume's own export read-write, or the VolumeReplication reads Primary
// while no pod can mount the volume read-write.

// exportOnA is the volume's own export on A, as the fake holds it now.
func exportOnA(t *testing.T, s *replScenario) qumulo.ExportInfo {
	t.Helper()
	for _, e := range s.a.exports {
		if e.FSPath == replVolumePath {
			return e
		}
	}
	t.Fatalf("no export at %s on A", replVolumePath)
	return qumulo.ExportInfo{}
}

func setExportOnA(s *replScenario, readOnly bool) {
	for i := range s.a.exports {
		if s.a.exports[i].FSPath == replVolumePath {
			s.a.exports[i].ReadOnly = readOnly
		}
	}
}

func TestB5PromoteOnAWritableSourceMakesALeftoverReadOnlyExportWritable(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	setExportOnA(s, true) // the replica export the copy kept
	s.a.calls = nil

	require.NoError(t, s.promote(t, s.handleA, classToB(), false))

	assert.False(t, exportOnA(t, s).ReadOnly, "a Primary volume must be mountable read-write")
	assert.Contains(t, s.a.calls, "SetExportReadOnly")

	// Again: export already writable, nothing to do.
	s.a.calls = nil
	require.NoError(t, s.promote(t, s.handleA, classToB(), false))
	assert.Empty(t, s.a.calls)
}

func TestB5AbandonedPromoteUnfreezeAlsoMakesTheExportWritable(t *testing.T) {
	s := newReplScenario(t)
	rel := s.protect(t, classToB())
	rel.sourceRO = true
	setExportOnA(s, true)
	s.a.calls = nil

	require.NoError(t, s.promote(t, s.handleA, classToB(), false))

	assert.False(t, rel.sourceRO, "the abandoned freeze is cleared")
	assert.False(t, exportOnA(t, s).ReadOnly, "and the export is writable too")
	assert.Equal(t, []string{"PatchSource", "SetExportReadOnly"}, s.a.calls)
}

func TestB5EnableReprotectingAPromotedExTargetMakesItsExportWritable(t *testing.T) {
	s := newReplScenario(t)
	// A became writable from B (its make-writable landed) but no promote
	// completed, so its export is still the read-only replica export.
	s.b.mkdir(replVolumePath)
	old := s.fabric.seedRel(s.b, s.a, replVolumePath, replVolumePath, qumulo.RelationshipDisconnected, func(r *fakeRel) {
		r.targetRO = false
	})
	old.sourceRO = true // the old primary is frozen: consent to overwrite it
	setExportOnA(s, true)

	require.NoError(t, s.enable(t, classToB()))

	fresh := s.fabric.onlyRel()
	assert.Equal(t, s.a, fresh.source, "A is now the source")
	assert.False(t, exportOnA(t, s).ReadOnly, "the new source's own export is read-write")
}

func TestB5WritableExportIsNotTouched(t *testing.T) {
	s := newReplScenario(t)
	s.protect(t, classToB())
	require.False(t, exportOnA(t, s).ReadOnly)
	s.a.calls = nil

	require.NoError(t, s.promote(t, s.handleA, classToB(), false))

	assert.NotContains(t, s.a.calls, "SetExportReadOnly")
}
