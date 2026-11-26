//go:build live

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package live

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/driver"
	"qumulo-csi-driver/pkg/qumulo"
)

// fileSecretReader serves the harness's real token for any reference.
//
// The unit tests already cover which Secret is chosen; what cannot be faked is
// the cluster's answers, so this stands in for the Kubernetes half and lets the
// Qumulo half run for real. The in-cluster client-go path is exercised by
// deploying the chart, not from here -- these tests run outside the cluster.
type fileSecretReader struct {
	token string
	calls []string
}

func (f *fileSecretReader) GetSecret(
	_ context.Context,
	namespace, name string,
) (map[string][]byte, error) {
	f.calls = append(f.calls, namespace+"/"+name)
	return map[string][]byte{"token": []byte(f.token)}, nil
}

// capacityHarness returns a controller whose GetCapacity can reach the array,
// plus the reader it was given.
func capacityHarness(t *testing.T) (*harness, csi.ControllerServer, *fileSecretReader) {
	t.Helper()

	h := newHarness(t)
	reader := &fileSecretReader{token: h.Token}

	d, err := driver.NewDriver(driver.Config{
		Name:         "csi.qumulo.com",
		Version:      "live-test",
		HTTPClient:   h.Client.HTTPClient,
		SecretReader: reader,
	})
	if err != nil {
		t.Fatalf("building the driver: %v", err)
	}

	return h, d.ControllerServer(), reader
}

// capacityParams is params() plus the secret reference csi-provisioner would
// leave in the map. The reference values are arbitrary here: fileSecretReader
// answers for any of them, and the point is that the RPC reads *a* reference
// out of its own parameters rather than holding a credential.
func capacityParams(h *harness) map[string]string {
	params := h.Params()
	params["csi.storage.k8s.io/provisioner-secret-name"] = "qumulo-api-token"
	params["csi.storage.k8s.io/provisioner-secret-namespace"] = "qumulo-csi"
	return params
}

// TestLiveGetCapacityReportsRealFreeSpace is the whole RPC against the array.
//
// Read-only: it calls GET /v1/file-system and, at most, reads a quota. It
// creates nothing.
func TestLiveGetCapacityReportsRealFreeSpace(t *testing.T) {
	h, ctrl, reader := capacityHarness(t)

	resp, err := ctrl.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: capacityParams(h),
	})
	if err != nil {
		t.Fatalf("GetCapacity against the live cluster: %v", err)
	}

	available := resp.GetAvailableCapacity()
	if available <= 0 {
		t.Fatalf("GetCapacity reported %d bytes; a live cluster must report a positive number, "+
			"and zero would make a topology ineligible for scheduling", available)
	}

	// Cross-check against the array directly, so this asserts the number is
	// the cluster's and not merely non-zero.
	fs, err := h.Client.GetCapacity(context.Background())
	if err != nil {
		t.Fatalf("reading capacity directly: %v", err)
	}
	if uint64(available) > fs.FreeSizeBytes {
		t.Errorf("GetCapacity reported %d bytes, more than the cluster's free space of %d; "+
			"the answer must never exceed what the array has", available, fs.FreeSizeBytes)
	}

	if len(reader.calls) != 1 {
		t.Errorf("expected exactly one Secret read per call, got %d: %v", len(reader.calls), reader.calls)
	}

	t.Logf("MEASURED cluster total=%d bytes (%.2f TiB), free=%d bytes (%.2f TiB), GetCapacity reported=%d bytes (%.2f TiB)",
		fs.TotalSizeBytes, float64(fs.TotalSizeBytes)/(1<<40),
		fs.FreeSizeBytes, float64(fs.FreeSizeBytes)/(1<<40),
		available, float64(available)/(1<<40))
}

// TestLiveStoreRootQuotaBehaviour measures the branch the implementation
// guesses at: what the array answers when a directory has no quota.
//
// This is the one assumption in storeRootQuotaLimit that cannot be verified
// from a unit test. The code treats a not-found as "no quota here, report the
// cluster's free space". If the array answers some other way -- a 200 with an
// empty body, or an error class qumulo.IsNotFound does not recognise -- the
// fallback would misfire and the reported number would be wrong.
func TestLiveStoreRootQuotaBehaviour(t *testing.T) {
	h, _, _ := capacityHarness(t)
	ctx := context.Background()

	info, err := h.Client.LookupFileByPath(ctx, h.StoreRealPath)
	if err != nil {
		t.Fatalf("looking up the store root %q: %v", h.StoreRealPath, err)
	}
	t.Logf("MEASURED store root %q resolves to file ID %s", h.StoreRealPath, info.ID)

	limit, err := h.Client.GetQuota(ctx, info.ID)
	switch {
	case err == nil:
		t.Logf("MEASURED the store root HAS a quota: limit=%d bytes (%.2f TiB). "+
			"GetCapacity will report min(cluster free, this limit)",
			limit, float64(limit)/(1<<40))
	case qumulo.IsNotFound(err):
		t.Logf("MEASURED the store root has NO quota, and the array says so with an error "+
			"that qumulo.IsNotFound recognises: %v. This is the branch "+
			"storeRootQuotaLimit relies on, and it holds", err)
	default:
		t.Fatalf("the store root has no quota, but the array reported it in a way "+
			"qumulo.IsNotFound does NOT recognise: %v (error class %q). "+
			"storeRootQuotaLimit would log a warning and fall back, which is safe, "+
			"but the classification in pkg/qumulo/errors.go needs this status added",
			err, qumulo.ErrorClassOf(err))
	}
}

// TestLiveGetCapacityEmptyRequestReportsZero confirms against a real cluster
// that the csi-sanity path is unchanged: a parameterless probe contacts nothing
// and answers zero.
func TestLiveGetCapacityEmptyRequestReportsZero(t *testing.T) {
	_, ctrl, reader := capacityHarness(t)

	resp, err := ctrl.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	if err != nil {
		t.Fatalf("GetCapacity with an empty request must not error: %v", err)
	}
	if resp.GetAvailableCapacity() != 0 {
		t.Errorf("expected zero for a parameterless probe, got %d", resp.GetAvailableCapacity())
	}
	if len(reader.calls) != 0 {
		t.Errorf("a parameterless probe must read no Secret, but read %v", reader.calls)
	}
}

// TestLiveGetCapacityRejectsAMisconfiguredClass is the counterpart, against a
// real cluster: parameters present but no server must error rather than report
// zero, because a published zero blocks scheduling.
func TestLiveGetCapacityRejectsAMisconfiguredClass(t *testing.T) {
	h, ctrl, _ := capacityHarness(t)

	params := capacityParams(h)
	delete(params, "server")
	delete(params, "apiUrl")

	resp, err := ctrl.GetCapacity(context.Background(), &csi.GetCapacityRequest{Parameters: params})
	if err == nil {
		t.Fatalf("expected an error for a class naming no server, got capacity %d",
			resp.GetAvailableCapacity())
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %s: %v", status.Code(err), err)
	}
	t.Logf("MEASURED a class naming no server is refused: %v", err)
}

// TestLiveGetCapacityWithABadTokenIsNotSilent checks that a credential the
// array rejects surfaces as an error rather than as a zero or a stale number.
func TestLiveGetCapacityWithABadTokenIsNotSilent(t *testing.T) {
	h := newHarness(t)

	d, err := driver.NewDriver(driver.Config{
		Name:         "csi.qumulo.com",
		Version:      "live-test",
		HTTPClient:   h.Client.HTTPClient,
		SecretReader: &fileSecretReader{token: "not-a-valid-token"},
	})
	if err != nil {
		t.Fatalf("building the driver: %v", err)
	}

	resp, err := d.ControllerServer().GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: capacityParams(h),
	})
	if err == nil {
		t.Fatalf("expected an error with an invalid token, got capacity %d",
			resp.GetAvailableCapacity())
	}
	if status.Code(err) != codes.Internal {
		t.Errorf("a rejected credential is a cluster-side failure, so Internal; got %s: %v",
			status.Code(err), err)
	}
	t.Logf("MEASURED an invalid token surfaces as: %v", err)
}

// TestLiveGetCapacityIsBoundedByARealQuota exercises the min() branch against
// the array: with a quota on the store root, the RPC must report the quota
// rather than the cluster's free space.
//
// This is the whole value of reporting capacity per class -- without it, every
// class on one array reports the same number. It is the only mutating test in
// this file: it sets a quota on the store root and removes it again. The store
// root must contain the safety marker (newHarness enforces that), and the quota
// is deliberately larger than anything these tests write, so a concurrent
// provision is not affected during the seconds it exists.
func TestLiveGetCapacityIsBoundedByARealQuota(t *testing.T) {
	h, ctrl, _ := capacityHarness(t)
	ctx := context.Background()

	info, err := h.Client.LookupFileByPath(ctx, h.StoreRealPath)
	if err != nil {
		t.Fatalf("looking up the store root: %v", err)
	}

	// Refuse to run if a quota is already there: removing someone else's quota
	// afterwards would be worse than skipping.
	if existing, err := h.Client.GetQuota(ctx, info.ID); err == nil {
		t.Skipf("the store root already has a quota (limit=%d); not disturbing it", existing)
	} else if !qumulo.IsNotFound(err) {
		t.Fatalf("checking for an existing quota: %v", err)
	}

	fs, err := h.Client.GetCapacity(ctx)
	if err != nil {
		t.Fatalf("reading cluster capacity: %v", err)
	}

	// Half the cluster's free space: comfortably above anything these tests
	// write, and unambiguously below free space, so min() has a visible effect.
	quotaLimit := fs.FreeSizeBytes / 2
	if err := h.Client.CreateQuota(ctx, info.ID, quotaLimit); err != nil {
		t.Fatalf("creating the test quota: %v", err)
	}
	t.Cleanup(func() {
		if err := h.Client.DeleteQuota(context.Background(), info.ID); err != nil {
			t.Errorf("CLEANUP FAILED: the test quota of %d bytes is still on %q (file ID %s) "+
				"and must be removed by hand: %v", quotaLimit, h.StoreRealPath, info.ID, err)
		}
	})

	resp, err := ctrl.GetCapacity(ctx, &csi.GetCapacityRequest{Parameters: capacityParams(h)})
	if err != nil {
		t.Fatalf("GetCapacity with a quota on the store root: %v", err)
	}

	// What the quota still has room for -- its limit less what the store root
	// already holds -- rounded down to whole GiB. Comparing with the
	// bare limit failed by exactly the store root's usage once usage was read.
	quota, err := h.Client.GetQuotaInfo(ctx, info.ID)
	if err != nil {
		t.Fatalf("reading the test quota back: %v", err)
	}
	want := quota.Available()
	if want >= 1<<30 {
		want -= want % (1 << 30)
	}
	if got := uint64(resp.GetAvailableCapacity()); got != want {
		t.Errorf("GetCapacity reported %d bytes; with a quota of %d (%d used) and cluster free space of %d "+
			"it must report the quota's remaining space, %d, which is the tighter bound",
			got, quotaLimit, quota.Usage, fs.FreeSizeBytes, want)
	}

	t.Logf("MEASURED cluster free=%d bytes (%.2f GiB), store-root quota limit=%d bytes (%.2f GiB), "+
		"GetCapacity reported=%d bytes (%.2f GiB) -- the quota bound applies",
		fs.FreeSizeBytes, float64(fs.FreeSizeBytes)/(1<<30),
		quotaLimit, float64(quotaLimit)/(1<<30),
		resp.GetAvailableCapacity(), float64(resp.GetAvailableCapacity())/(1<<30))
}
