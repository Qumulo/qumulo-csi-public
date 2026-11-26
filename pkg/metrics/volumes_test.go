// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"qumulo-csi-driver/pkg/version"
)

// pvs is the reconcile's view of a set of PersistentVolumes whose capacity
// is not being tested: unknown, which contradicts no quota reading.
func pvs(handles ...string) []VolumeRecord {
	out := make([]VolumeRecord, 0, len(handles))
	for _, h := range handles {
		out = append(out, VolumeRecord{Handle: h})
	}
	return out
}

// A volume's series outlive it only until two reconcile passes in a
// row find no PersistentVolume carrying its handle; a handle a PV carries is
// kept, and one whose PV appears before the second pass is kept too.
func TestReconcileVolumesRetiresOnlyHandlesNoPVCarries(t *testing.T) {
	r := New(version.Info{})
	const kept, gone, late = "qumulo.example.com##/csi/pvc-kept", "qumulo.example.com##/csi/pvc-gone", "qumulo.example.com##/csi/pvc-late"
	for _, h := range []string{kept, gone, late} {
		name, _ := volumeNameFromHandle(h)
		r.volumeQuotaBytes.WithLabelValues(volumeID(h), name).Set(1)
		r.volumeProtected.WithLabelValues(volumeID(h), name).Set(1)
	}
	ids := func() []string { s := r.volumeIDsWithSeries(); sort.Strings(s); return s }

	assert.Zero(t, r.ReconcileVolumes(pvs(kept)), "one pass retires nothing")
	assert.Len(t, ids(), 3)

	// late's PV now exists (csi-provisioner wrote it after CreateVolume).
	assert.Equal(t, 1, r.ReconcileVolumes(pvs(kept, late)))
	want := []string{volumeID(kept), volumeID(late)}
	sort.Strings(want)
	assert.Equal(t, want, ids(), "only the handle missing twice is retired")
}
