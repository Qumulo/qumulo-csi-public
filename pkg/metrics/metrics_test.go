// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
	"qumulo-csi-driver/pkg/version"
)

func testRegistry() *Registry {
	return New(version.Info{
		DriverName:    "csi.qumulo.com",
		DriverVersion: "0.1.0",
		GitCommit:     "2f15ba4",
		BuildDate:     "2026-09-11",
		GoVersion:     "go1.26.6",
	})
}

// call runs one RPC through the interceptor, as the gRPC server would.
func call(t *testing.T, r *Registry, method string, req any, handler grpc.UnaryHandler) (any, error) {
	t.Helper()
	return r.UnaryServerInterceptor(
		context.Background(), req, &grpc.UnaryServerInfo{FullMethod: method}, handler)
}

func TestBuildInfoIsAlwaysOneAndCarriesTheBuild(t *testing.T) {
	r := testRegistry()

	expected := `
# HELP csi_qumulo_build_info Always 1. Carries the driver's version, commit and Go version as labels, so a dashboard can answer "which build is running where" without reading a log line.
# TYPE csi_qumulo_build_info gauge
csi_qumulo_build_info{build_date="2026-09-11",commit="2f15ba4",driver_name="csi.qumulo.com",go_version="go1.26.6",version="0.1.0"} 1
`
	require.NoError(t, testutil.GatherAndCompare(
		r.Gatherer(), strings.NewReader(expected), "csi_qumulo_build_info"))
}

// The interceptor must count both halves of the outcome: rpc_total carries
// the code, rpc_duration_seconds deliberately does not.
func TestInterceptorCountsEveryRPCByMethodAndCode(t *testing.T) {
	r := testRegistry()

	_, err := call(t, r, "/csi.v1.Controller/CreateVolume", &csi.CreateVolumeRequest{},
		func(context.Context, any) (any, error) { return &csi.CreateVolumeResponse{}, nil })
	require.NoError(t, err)

	_, err = call(t, r, "/csi.v1.Controller/CreateVolume", &csi.CreateVolumeRequest{},
		func(context.Context, any) (any, error) {
			return nil, grpcstatus.Error(codes.ResourceExhausted, "too big")
		})
	require.Error(t, err)

	assert.Equal(t, 1.0, testutil.ToFloat64(r.rpcTotal.WithLabelValues(
		"/csi.v1.Controller/CreateVolume", "OK")))
	assert.Equal(t, 1.0, testutil.ToFloat64(r.rpcTotal.WithLabelValues(
		"/csi.v1.Controller/CreateVolume", "ResourceExhausted")))
	assert.Equal(t, 1, testutil.CollectAndCount(r.rpcDuration, "csi_qumulo_rpc_duration_seconds"),
		"both calls land in ONE histogram series: duration is not labelled by code, "+
			"so the ok and the failure share it")
}

// A panic in a handler must not be swallowed by the interceptor: the gRPC
// server has its own recovery, and hiding it here would turn a crash into a
// silent Unknown.
func TestInterceptorDoesNotSwallowAPanic(t *testing.T) {
	r := testRegistry()

	assert.Panics(t, func() {
		_, _ = call(t, r, "/csi.v1.Controller/CreateVolume", &csi.CreateVolumeRequest{},
			func(context.Context, any) (any, error) { panic("boom") })
	})
}

func TestAPIRequestSeriesAndReachability(t *testing.T) {
	r := testRegistry()
	ctx := context.Background()

	r.ObserveRequest(ctx, qumulo.RequestEvent{
		Cluster: "a.example.com:8000", Method: http.MethodGet,
		PathTemplate: "/v1/files/{id}/info/attributes", Status: "200",
		Duration: 12 * time.Millisecond, Reachable: true,
	})
	r.ObserveRequest(ctx, qumulo.RequestEvent{
		Cluster: "b.example.com:8000", Method: http.MethodGet,
		PathTemplate: "/v1/file-system", Status: qumulo.StatusTransport,
		Duration: 10 * time.Second, Reachable: false,
	})

	assert.Equal(t, 1.0, testutil.ToFloat64(r.apiRequests.WithLabelValues(
		"a.example.com:8000", http.MethodGet, "/v1/files/{id}/info/attributes", "200")))
	assert.Equal(t, 1.0, testutil.ToFloat64(r.apiRequests.WithLabelValues(
		"b.example.com:8000", http.MethodGet, "/v1/file-system", "transport")))

	assert.Equal(t, 1.0, testutil.ToFloat64(r.apiReachable.WithLabelValues("a.example.com:8000")))
	assert.Equal(t, 0.0, testutil.ToFloat64(r.apiReachable.WithLabelValues("b.example.com:8000")),
		"an array that never answered is unreachable")

	// And it recovers: reachability is last-observed, not sticky.
	r.ObserveRequest(ctx, qumulo.RequestEvent{
		Cluster: "b.example.com:8000", Method: http.MethodGet,
		PathTemplate: "/v1/file-system", Status: "200", Duration: time.Millisecond, Reachable: true,
	})
	assert.Equal(t, 1.0, testutil.ToFloat64(r.apiReachable.WithLabelValues("b.example.com:8000")))
}

func TestClusterCapacityGauges(t *testing.T) {
	r := testRegistry()

	r.ObserveCapacity(context.Background(), qumulo.CapacityEvent{
		Cluster:            "a.example.com:8000",
		FileSystemCapacity: qumulo.FileSystemCapacity{TotalSizeBytes: 100, FreeSizeBytes: 40},
	})

	assert.Equal(t, 100.0, testutil.ToFloat64(r.clusterCapacityBytes.WithLabelValues("a.example.com:8000")))
	assert.Equal(t, 40.0, testutil.ToFloat64(r.clusterFreeBytes.WithLabelValues("a.example.com:8000")))
}

// Each restore staging directory found abandoned or removed is
// counted, per array and state.
func TestRestoreStagingDirectoriesAreCountedByState(t *testing.T) {
	r := testRegistry()
	ctx := context.Background()

	r.ObserveRestoreStaging(ctx, qumulo.RestoreStagingEvent{Cluster: "a.example.com:8000", State: qumulo.RestoreStagingAbandoned})
	r.ObserveRestoreStaging(ctx, qumulo.RestoreStagingEvent{Cluster: "a.example.com:8000", State: qumulo.RestoreStagingAbandoned})
	r.ObserveRestoreStaging(ctx, qumulo.RestoreStagingEvent{Cluster: "a.example.com:8000", State: qumulo.RestoreStagingRemoved})

	assert.Equal(t, 2.0, testutil.ToFloat64(r.restoreStaging.WithLabelValues("a.example.com:8000", "abandoned")))
	assert.Equal(t, 1.0, testutil.ToFloat64(r.restoreStaging.WithLabelValues("a.example.com:8000", "removed")))
	assert.Equal(t, 2, testutil.CollectAndCount(r.restoreStaging, "csi_qumulo_restore_staging_directories_total"))
}

// testHandle is the handle this driver actually issues:
// <server>#<absolute cluster path>. The retired <server>/<fileID>/<name>
// form every test in this file used until now is a shape CreateVolume has
// not minted since that commit, and it is the only shape splitVolumeHandle
// accepts -- so the volume gauges were exercised solely against handles the
// driver no longer produces (defect: per-volume quota metrics never attach).
const testHandle = "qumulo.example.com##/csi/volumes/pvc-abc123"

// testFileID is the file ID the array reports for testHandle's directory.
// Nothing in the handle carries it any more, which is the defect: the sink
// is keyed by file ID and the handle cannot supply one.
const testFileID = "12884901888"

// The join this package exists for: the client knows a file ID, the RPC
// knows a handle, and only the two together make a series an operator can
// find a PVC from.
func TestVolumeGaugesJoinAQuotaReadingToTheHandleOfTheRPCThatMadeIt(t *testing.T) {
	r := testRegistry()

	_, err := call(t, r, "/csi.v1.Controller/ControllerGetVolume",
		&csi.ControllerGetVolumeRequest{VolumeId: testHandle},
		func(ctx context.Context, _ any) (any, error) {
			r.ObserveQuota(qumulo.WithSubject(ctx, testHandle), qumulo.QuotaEvent{
				Cluster: "qumulo.example.com:8000", FileID: testFileID,
				Subject:    testHandle,
				LimitBytes: 5368709120, UsedBytes: 69632, UsedKnown: true,
			})
			return &csi.ControllerGetVolumeResponse{}, nil
		})
	require.NoError(t, err)

	assert.Equal(t, 5368709120.0,
		testutil.ToFloat64(r.volumeQuotaBytes.WithLabelValues(volumeID(testHandle), "pvc-abc123")))
	assert.Equal(t, 69632.0,
		testutil.ToFloat64(r.volumeUsedBytes.WithLabelValues(volumeID(testHandle), "pvc-abc123")))
}

// The per-volume series used to carry the handle as a label, which named
// every volume's array, endpoint and path to anyone who could read /metrics.
// They now carry volumeID, a digest that names none of them. A DR copy has the
// same path, and so the same volume_name, on both arrays; its series must
// still be two series, not one.
func TestVolumeSeriesNameNoArrayOrPath(t *testing.T) {
	r := testRegistry()
	const primary = "red.example.com#https://red.example.com:8000#/csi/volumes/pvc-dr1"
	const copyOnB = "blue.example.com#https://blue.example.com:8000#/csi/volumes/pvc-dr1"

	for i, handle := range []string{primary, copyOnB} {
		_, err := call(t, r, "/csi.v1.Controller/ControllerGetVolume",
			&csi.ControllerGetVolumeRequest{VolumeId: handle},
			func(ctx context.Context, _ any) (any, error) {
				r.ObserveQuota(ctx, qumulo.QuotaEvent{
					Subject: handle, FileID: testFileID,
					LimitBytes: uint64(i+1) << 30, UsedKnown: false,
				})
				return &csi.ControllerGetVolumeResponse{}, nil
			})
		require.NoError(t, err)
	}
	r.ObserveVolumeProtection(context.Background(), qumulo.VolumeProtectionEvent{Subject: primary, Protected: true})

	assert.Equal(t, 2, testutil.CollectAndCount(r.volumeQuotaBytes, "csi_qumulo_volume_quota_bytes"),
		"the same path on two arrays is two series")
	assert.Equal(t, float64(2<<30), testutil.ToFloat64(r.volumeQuotaBytes.WithLabelValues(volumeID(copyOnB), "pvc-dr1")))
	assert.Regexp(t, `^[0-9a-f]{16}$`, volumeID(primary))

	var out strings.Builder
	for _, vec := range []prometheus.Collector{r.volumeQuotaBytes, r.volumeUsedBytes, r.volumeProtected} {
		ch := make(chan prometheus.Metric, 8)
		go func() { vec.Collect(ch); close(ch) }()
		for m := range ch {
			var pb dto.Metric
			require.NoError(t, m.Write(&pb))
			out.WriteString(pb.String())
		}
	}
	for _, leak := range []string{"red.example.com", "blue.example.com", "/csi/volumes", "volume_handle"} {
		assert.NotContains(t, out.String(), leak)
	}
	assert.Contains(t, out.String(), volumeID(primary))
}

// A quota the array reported without capacity_usage must leave used_bytes
// absent rather than zero: zero reads as an empty volume, and the difference
// decides whether a near-quota alert fires.
func TestUnknownUsageLeavesTheUsedSeriesAbsent(t *testing.T) {
	r := testRegistry()

	_, err := call(t, r, "/csi.v1.Controller/ControllerGetVolume",
		&csi.ControllerGetVolumeRequest{VolumeId: testHandle},
		func(ctx context.Context, _ any) (any, error) {
			r.ObserveQuota(ctx, qumulo.QuotaEvent{
				Subject: testHandle, FileID: testFileID,
				LimitBytes: 5368709120, UsedKnown: false,
			})
			return &csi.ControllerGetVolumeResponse{}, nil
		})
	require.NoError(t, err)

	assert.Equal(t, 1, testutil.CollectAndCount(r.volumeQuotaBytes, "csi_qumulo_volume_quota_bytes"))
	assert.Equal(t, 0, testutil.CollectAndCount(r.volumeUsedBytes, "csi_qumulo_volume_used_bytes"))
}

func TestListVolumesSetsAGaugePerEntry(t *testing.T) {
	r := testRegistry()
	second := "qumulo.example.com##/csi/volumes/pvc-def456"

	_, err := call(t, r, "/csi.v1.Controller/ListVolumes", &csi.ListVolumesRequest{},
		func(ctx context.Context, _ any) (any, error) {
			// The driver labels each read with the handle it belongs to,
			// which is what tells two volumes' quotas apart inside one RPC.
			r.ObserveQuota(ctx, qumulo.QuotaEvent{Subject: testHandle, FileID: testFileID, LimitBytes: 10})
			r.ObserveQuota(ctx, qumulo.QuotaEvent{Subject: second, FileID: "999", LimitBytes: 20})
			return &csi.ListVolumesResponse{Entries: []*csi.ListVolumesResponse_Entry{
				{Volume: &csi.Volume{VolumeId: testHandle}},
				{Volume: &csi.Volume{VolumeId: second}},
			}}, nil
		})
	require.NoError(t, err)

	assert.Equal(t, 10.0, testutil.ToFloat64(r.volumeQuotaBytes.WithLabelValues(volumeID(testHandle), "pvc-abc123")))
	assert.Equal(t, 20.0, testutil.ToFloat64(r.volumeQuotaBytes.WithLabelValues(volumeID(second), "pvc-def456")))
}

// GetCapacity reads the store root's quota, which belongs to no volume. It
// must not invent one.
func TestAQuotaReadingWithNoHandleToJoinIsDropped(t *testing.T) {
	r := testRegistry()

	_, err := call(t, r, "/csi.v1.Controller/GetCapacity", &csi.GetCapacityRequest{},
		func(ctx context.Context, _ any) (any, error) {
			r.ObserveQuota(ctx, qumulo.QuotaEvent{Subject: "", FileID: "5", LimitBytes: 1})
			return &csi.GetCapacityResponse{AvailableCapacity: 1}, nil
		})
	require.NoError(t, err)

	assert.Equal(t, 0, testutil.CollectAndCount(r.volumeQuotaBytes, "csi_qumulo_volume_quota_bytes"))
}

func TestDeleteVolumeRetiresTheVolumeSeries(t *testing.T) {
	r := testRegistry()

	_, err := call(t, r, "/csi.v1.Controller/ControllerGetVolume",
		&csi.ControllerGetVolumeRequest{VolumeId: testHandle},
		func(ctx context.Context, _ any) (any, error) {
			r.ObserveQuota(ctx, qumulo.QuotaEvent{Subject: testHandle, FileID: testFileID, LimitBytes: 1, UsedBytes: 1, UsedKnown: true})
			r.ObserveVolumeProtection(ctx, qumulo.VolumeProtectionEvent{Subject: testHandle, Protected: false})
			return &csi.ControllerGetVolumeResponse{}, nil
		})
	require.NoError(t, err)
	require.Equal(t, 1, testutil.CollectAndCount(r.volumeQuotaBytes, "csi_qumulo_volume_quota_bytes"))
	require.Equal(t, 1, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"))

	_, err = call(t, r, "/csi.v1.Controller/DeleteVolume",
		&csi.DeleteVolumeRequest{VolumeId: testHandle},
		func(context.Context, any) (any, error) { return &csi.DeleteVolumeResponse{}, nil })
	require.NoError(t, err)

	assert.Equal(t, 0, testutil.CollectAndCount(r.volumeQuotaBytes, "csi_qumulo_volume_quota_bytes"))
	assert.Equal(t, 0, testutil.CollectAndCount(r.volumeUsedBytes, "csi_qumulo_volume_used_bytes"))
	// An unprotected volume's 0 would otherwise fire
	// QumuloCSIVolumeUnprotected for a volume that no longer exists.
	assert.Equal(t, 0, testutil.CollectAndCount(r.volumeProtected, "csi_qumulo_volume_replication_protected"))
}

// A failed DeleteVolume deleted nothing, so it must retire nothing.
func TestAFailedDeleteVolumeKeepsTheVolumeSeries(t *testing.T) {
	r := testRegistry()

	_, err := call(t, r, "/csi.v1.Controller/ControllerGetVolume",
		&csi.ControllerGetVolumeRequest{VolumeId: testHandle},
		func(ctx context.Context, _ any) (any, error) {
			r.ObserveQuota(ctx, qumulo.QuotaEvent{Subject: testHandle, FileID: testFileID, LimitBytes: 1})
			return &csi.ControllerGetVolumeResponse{}, nil
		})
	require.NoError(t, err)

	_, err = call(t, r, "/csi.v1.Controller/DeleteVolume",
		&csi.DeleteVolumeRequest{VolumeId: testHandle},
		func(context.Context, any) (any, error) {
			return nil, grpcstatus.Error(codes.Unavailable, "array down")
		})
	require.Error(t, err)

	assert.Equal(t, 1, testutil.CollectAndCount(r.volumeQuotaBytes, "csi_qumulo_volume_quota_bytes"))
}

// The absence of a relationship has no relationship ID to key on, so this is
// the one replication series an operator can alert on by volume. A volume the
// driver looked at and found unprotected must read 0, not vanish.
func TestVolumeProtectionGaugeRecordsBothVerdicts(t *testing.T) {
	r := testRegistry()

	r.ObserveVolumeProtection(context.Background(), qumulo.VolumeProtectionEvent{
		Cluster: "qumulo.example.com:8000", Subject: testHandle, Protected: true,
	})
	assert.Equal(t, 1.0,
		testutil.ToFloat64(r.volumeProtected.WithLabelValues(volumeID(testHandle), "pvc-abc123")))

	// The relationship is removed on the arrays. The series must stay and go
	// to zero: a series that simply stops being reported is not alertable
	// without knowing in advance which volume to expect.
	r.ObserveVolumeProtection(context.Background(), qumulo.VolumeProtectionEvent{
		Cluster: "qumulo.example.com:8000", Subject: testHandle, Protected: false,
	})
	assert.Equal(t, 0.0,
		testutil.ToFloat64(r.volumeProtected.WithLabelValues(volumeID(testHandle), "pvc-abc123")))
	assert.Equal(t, 1, testutil.CollectAndCount(r.volumeProtected,
		"csi_qumulo_volume_replication_protected"))
}

// An event the driver did not label belongs to no volume, so it is dropped
// rather than exported against an empty handle.
func TestVolumeProtectionIgnoresAnUnlabelledEvent(t *testing.T) {
	r := testRegistry()

	r.ObserveVolumeProtection(context.Background(), qumulo.VolumeProtectionEvent{
		Cluster: "qumulo.example.com:8000", Protected: false,
	})

	assert.Equal(t, 0, testutil.CollectAndCount(r.volumeProtected,
		"csi_qumulo_volume_replication_protected"))
}

func TestVolumeNameFromHandle(t *testing.T) {
	for _, tc := range []struct {
		handle string
		name   string
		ok     bool
	}{
		// The handle the driver issues today.
		{handle: testHandle, name: "pvc-abc123", ok: true},
		// Depth is not a constraint any more: the name is the path's base,
		// however deep the class's basePath goes.
		{handle: "qumulo.example.com##/csi/pvc-abc123", name: "pvc-abc123", ok: true},
		{handle: "qumulo.example.com##/a/b/c/d/pvc-deep", name: "pvc-deep", ok: true},
		// A tenant class's handle carries a REST endpoint in the middle
		// field. The name still comes from the path, and the endpoint must
		// not leak into a label.
		{
			handle: "tenant.example.com#https://mgmt.example.com:8000#/tenants/pod1/pvc-abc123",
			name:   "pvc-abc123",
			ok:     true,
		},
		// A handle naming the cluster root names no volume.
		{handle: "qumulo.example.com##/", ok: false},
		{handle: "qumulo.example.com##relative/path", ok: false},
		{handle: "##/csi/pvc-abc123", ok: false},
		// The two-field form predates the endpoint field; the driver still
		// decodes it, so it still names its volume. The retired
		// <server>/<fileID>/<name> form predates both and is not accepted.
		{handle: "qumulo.example.com#/csi/pvc-abc123", name: "pvc-abc123", ok: true},
		{handle: "qumulo.example.com/12884901888/pvc-old", ok: false},
		{handle: "qumulo.example.com/123", ok: false},
		{handle: "a//c", ok: false},
		{handle: "", ok: false},
	} {
		t.Run(tc.handle, func(t *testing.T) {
			name, ok := volumeNameFromHandle(tc.handle)
			assert.Equal(t, tc.ok, ok)
			if tc.ok {
				assert.Equal(t, tc.name, name)
			}
		})
	}
}

func TestHandlerServesTheExpositionFormat(t *testing.T) {
	r := testRegistry()
	_, err := call(t, r, "/csi.v1.Identity/Probe", &csi.ProbeRequest{},
		func(context.Context, any) (any, error) { return &csi.ProbeResponse{}, nil })
	require.NoError(t, err)

	server := httptest.NewServer(r.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), `csi_qumulo_rpc_total{code="OK",method="/csi.v1.Identity/Probe"} 1`)
	assert.Contains(t, string(body), "csi_qumulo_build_info")
	assert.Contains(t, string(body), "go_goroutines",
		"the Go collector is registered deliberately, so a restart is diagnosable")
}

// A ListVolumes that read a volume's quota before a DeleteVolume of it
// finished, and returned after, used to put the deleted volume's series back
// for good. A reading taken by an RPC that started before the delete is now
// dropped; one from an RPC that started after it is exported as usual.
func TestAListVolumesOverlappingADeleteDoesNotReviveItsSeries(t *testing.T) {
	r := testRegistry()
	listWithQuota := func(ctx context.Context, during func()) {
		_, err := call(t, r, "/csi.v1.Controller/ListVolumes", &csi.ListVolumesRequest{},
			func(ctx context.Context, _ any) (any, error) {
				r.ObserveQuota(ctx, qumulo.QuotaEvent{Subject: testHandle, FileID: testFileID, LimitBytes: 10})
				during()
				return &csi.ListVolumesResponse{Entries: []*csi.ListVolumesResponse_Entry{
					{Volume: &csi.Volume{VolumeId: testHandle}},
				}}, nil
			})
		require.NoError(t, err)
	}

	listWithQuota(context.Background(), func() {
		_, err := call(t, r, "/csi.v1.Controller/DeleteVolume", &csi.DeleteVolumeRequest{VolumeId: testHandle},
			func(context.Context, any) (any, error) { return &csi.DeleteVolumeResponse{}, nil })
		require.NoError(t, err)
	})
	assert.Equal(t, 0, testutil.CollectAndCount(r.volumeQuotaBytes, "csi_qumulo_volume_quota_bytes"),
		"a listing that began before the delete must not bring the series back")

	listWithQuota(context.Background(), func() {})
	assert.Equal(t, 1, testutil.CollectAndCount(r.volumeQuotaBytes, "csi_qumulo_volume_quota_bytes"),
		"a listing that began after the delete is exported as usual")
}

// ControllerExpandVolume reads the quota with its usage, then writes
// the new limit, and the write carries no usage. The sink kept only the last
// event, so an expand set the new limit and dropped the usage: the near-quota
// alert never saw an expanded volume until something else read it. The usage
// read in the same RPC is now kept beside the new limit.
func TestAnExpandKeepsTheUsageItReadBeforeWritingTheLimit(t *testing.T) {
	r := testRegistry()

	_, err := call(t, r, "/csi.v1.Controller/ControllerExpandVolume",
		&csi.ControllerExpandVolumeRequest{VolumeId: testHandle},
		func(ctx context.Context, _ any) (any, error) {
			r.ObserveQuota(ctx, qumulo.QuotaEvent{Subject: testHandle, FileID: testFileID,
				LimitBytes: 1 << 30, UsedBytes: 900 << 20, UsedKnown: true})
			r.ObserveQuota(ctx, qumulo.QuotaEvent{Subject: testHandle, FileID: testFileID,
				LimitBytes: 2 << 30})
			return &csi.ControllerExpandVolumeResponse{}, nil
		})
	require.NoError(t, err)

	assert.Equal(t, float64(2<<30), testutil.ToFloat64(r.volumeQuotaBytes.WithLabelValues(volumeID(testHandle), "pvc-abc123")),
		"the new limit")
	assert.Equal(t, float64(900<<20), testutil.ToFloat64(r.volumeUsedBytes.WithLabelValues(volumeID(testHandle), "pvc-abc123")),
		"the usage read before the write")
}

// The replication RPCs are labelled with the volume they are about,
// as the controller RPCs are, so the replication listings they make can be
// told apart from everyone else's relationships.
func TestReplicationRPCsAreLabelledWithTheirVolume(t *testing.T) {
	r := testRegistry()
	var seen string
	_, err := r.UnaryServerInterceptor(context.Background(),
		&replication.EnableVolumeReplicationRequest{VolumeId: testHandle},
		&grpc.UnaryServerInfo{FullMethod: "/replication.Controller/EnableVolumeReplication"},
		func(ctx context.Context, _ any) (any, error) {
			seen = qumulo.SubjectFrom(ctx)
			return &replication.EnableVolumeReplicationResponse{}, nil
		})
	require.NoError(t, err)
	assert.Equal(t, testHandle, seen)

	_, err = r.UnaryServerInterceptor(context.Background(),
		&replication.PromoteVolumeRequest{ReplicationSource: &replication.ReplicationSource{
			Type: &replication.ReplicationSource_Volume{Volume: &replication.ReplicationSource_VolumeSource{VolumeId: testHandle}},
		}},
		&grpc.UnaryServerInfo{FullMethod: "/replication.Controller/PromoteVolume"},
		func(ctx context.Context, _ any) (any, error) {
			seen = qumulo.SubjectFrom(ctx)
			return &replication.PromoteVolumeResponse{}, nil
		})
	require.NoError(t, err)
	assert.Equal(t, testHandle, seen, "the replication_source form is read first")
}

// TestInventoryClassCredentialGauge covers the only machine-readable signal
// that a ListVolumes listing is short. CSI's response has no field for a
// skipped class, so without this an operator learns about the skip from a
// log line or not at all.
func TestInventoryClassCredentialGauge(t *testing.T) {
	r := testRegistry()
	ctx := context.Background()

	r.ObserveClassCredential(ctx, "class-helm", "array-a", "", true)
	r.ObserveClassCredential(ctx, "class-by-hand", "array-a", "forbidden", false)

	assert.Equal(t, 2, testutil.CollectAndCount(r.inventoryClassCredential))

	// One series per class: a class that is fixed must not leave its old
	// reason behind reading 0 for ever.
	r.ObserveClassCredential(ctx, "class-by-hand", "array-a", "", true)
	assert.Equal(t, 2, testutil.CollectAndCount(r.inventoryClassCredential),
		"the forbidden series must be replaced, not added to")

	// And a class that is deleted -- which is how the forbidden-Secret case is resolved --
	// must stop being reported, or the alert never clears.
	r.ForgetClassesExcept(ctx, []string{"class-helm"})
	assert.Equal(t, 1, testutil.CollectAndCount(r.inventoryClassCredential))
}
