// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/version"
)

// The loop around ReconcileVolumes. The pass itself is tested above;
// this is the timer, the failed-list rule and the stop. synctest's fake
// clock runs the 10-minute ticker without waiting for it.
func TestRunVolumeReconcileRetiresOnItsTickerAndStopsWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New(version.Info{})
		const kept, gone = "qumulo.example.com##/csi/pvc-kept", "qumulo.example.com##/csi/pvc-gone"
		for _, h := range []string{kept, gone} {
			name, _ := volumeNameFromHandle(h)
			r.volumeProtected.WithLabelValues(volumeID(h), name).Set(1)
		}
		ids := func() []string { s := r.volumeIDsWithSeries(); sort.Strings(s); return s }

		var lists atomic.Int64
		list := func(context.Context) ([]VolumeRecord, error) {
			if lists.Add(1) == 1 {
				// An unreadable PV list is not an empty one: were it
				// taken as one, this pass and the next would retire
				// kept as well.
				return nil, errors.New("the API server is restarting")
			}
			return pvs(kept), nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		stopped := make(chan struct{})
		go func() { r.RunVolumeReconcile(ctx, list); close(stopped) }()

		synctest.Wait()
		assert.Zero(t, lists.Load(), "nothing runs before the first tick")

		time.Sleep(VolumeReconcileEvery)
		synctest.Wait()
		assert.Equal(t, int64(1), lists.Load())
		assert.Len(t, ids(), 2, "a failed list retires nothing")

		time.Sleep(VolumeReconcileEvery)
		synctest.Wait()
		assert.Equal(t, int64(2), lists.Load())
		assert.Len(t, ids(), 2, "one pass without a PV only marks a volume")

		time.Sleep(VolumeReconcileEvery)
		synctest.Wait()
		assert.Equal(t, int64(3), lists.Load())
		assert.Equal(t, []string{volumeID(kept)}, ids(), "the second pass without a PV retires it")

		cancel()
		synctest.Wait()
		select {
		case <-stopped:
		default:
			require.Fail(t, "the loop is still running after its context ended")
		}
		time.Sleep(3 * VolumeReconcileEvery)
		synctest.Wait()
		assert.Equal(t, int64(3), lists.Load(), "and lists nothing more")
	})
}
