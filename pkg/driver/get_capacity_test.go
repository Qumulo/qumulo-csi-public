// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"qumulo-csi-driver/pkg/qumulo"
)

// fakeSecretReader serves Secrets from a map, keyed "namespace/name".
type fakeSecretReader struct {
	secrets map[string]map[string][]byte
	err     error
	// calls records every lookup, so a test can assert that the *class's own*
	// Secret was read and not some other one -- the property that matters most
	// in a multi-cluster deployment.
	calls []string
}

func (f *fakeSecretReader) GetSecret(
	_ context.Context,
	namespace, name string,
) (map[string][]byte, error) {
	key := namespace + "/" + name
	f.calls = append(f.calls, key)
	if f.err != nil {
		return nil, f.err
	}
	data, ok := f.secrets[key]
	if !ok {
		return nil, NewSecretNotFoundError(namespace, name)
	}
	return data, nil
}

// capacityParams builds the parameter map csi-provisioner would send: the
// StorageClass's own parameters, secret-reference keys included.
func capacityParams(server, basePath, secretNS, secretName string) map[string]string {
	return map[string]string{
		paramServer:                       server,
		paramStoreRealPath:                basePath,
		paramStoreExportPath:              basePath,
		coParamProvisionerSecretName:      secretName,
		coParamProvisionerSecretNamespace: secretNS,
	}
}

func newCapacityServer(
	t *testing.T,
	client *MockClient,
	reader SecretReader,
) *ControllerServer {
	t.Helper()

	d, err := NewDriver(Config{
		Name:         "csi.qumulo.com",
		Version:      "0.1.0",
		NewClient:    func(baseURL, token string) qumulo.ClientInterface { return client },
		SecretReader: reader,
	})
	require.NoError(t, err)

	return NewControllerServer(d)
}

// TestGetCapacityEmptyRequestReportsZero pins the csi-sanity contract.
//
// The suite calls GetCapacity with a wholly empty request and requires no
// error. Returning InvalidArgument here -- defensible in isolation, since the
// request names no cluster -- turns `make sanity-test` and therefore `make ci`
// red. Zero is both what the suite allows and what is true: with no class
// named, this driver has nowhere to provision.
func TestGetCapacityEmptyRequestReportsZero(t *testing.T) {
	srv := newCapacityServer(t, &MockClient{}, &fakeSecretReader{})

	resp, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetAvailableCapacity())
}

// TestGetCapacityReportsACancelledSecretReadAsCancelled: the capacity sidecar
// disconnecting at a controller shutdown cancels this call, and the Secret
// read is the first thing it cuts short. That is Canceled, as CreateVolume's
// cancelled array call is, not an Internal error in every restart's log.
func TestGetCapacityReportsACancelledSecretReadAsCancelled(t *testing.T) {
	reader := &fakeSecretReader{err: fmt.Errorf("reading: %w", context.Canceled)}
	srv := newCapacityServer(t, &MockClient{}, reader)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := srv.GetCapacity(ctx, &csi.GetCapacityRequest{Parameters: capacityParams("array-a", "/csi", "ns", "s")})
	require.Error(t, err)
	require.Equal(t, codes.Canceled, status.Code(err), "got %v", err)

	reader.err = errors.New("etcdserver: request timed out")
	_, err = srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{Parameters: capacityParams("array-a", "/csi", "ns", "s")})
	require.Equal(t, codes.Internal, status.Code(err), "a transient API-server error is still Internal: %v", err)
}

// TestGetCapacityDoesNotReportZeroForAMisconfiguredClass is the other half of
// the rule above, and it matters more than it looks.
//
// Zero is load-bearing to a CO: Kubernetes compares a requested size against
// the CSIStorageCapacity objects built from this RPC, so a published zero reads
// as "there is no room" and makes a topology ineligible -- not as "I do not
// know". So the zero path must be reachable only by a parameterless capability
// probe, never by a class that named parameters but got them wrong. Those must
// error, loudly, naming the missing parameter.
func TestGetCapacityDoesNotReportZeroForAMisconfiguredClass(t *testing.T) {
	srv := newCapacityServer(t, &MockClient{}, &fakeSecretReader{})

	tests := []struct {
		name   string
		params map[string]string
	}{
		{
			name:   "names a path but no server",
			params: map[string]string{paramStoreRealPath: "/csi/fast"},
		},
		{
			name:   "carries only a secret reference",
			params: map[string]string{coParamProvisionerSecretName: "qumulo-token"},
		},
		{
			name:   "server present but empty",
			params: map[string]string{paramServer: "", paramStoreRealPath: "/csi/fast"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
				Parameters: tc.params,
			})
			require.Error(t, err,
				"a class with parameters but no usable server must error, not report zero capacity")
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Contains(t, err.Error(), paramServer,
				"the error must name the parameter an operator has to fix")
			require.Nil(t, resp)
		})
	}
}

// TestGetCapacityReportsClusterFreeSpace is the ordinary path: a class with no
// quota on its store root reports the cluster's free space.
func TestGetCapacityReportsClusterFreeSpace(t *testing.T) {
	const free = uint64(80 << 40) // 80 TiB

	client := &MockClient{
		GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
			return qumulo.FileSystemCapacity{TotalSizeBytes: 100 << 40, FreeSizeBytes: free}, nil
		},
		LookupFileByPathFunc: func(_ context.Context, path string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Path: path, Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) {
			return 0, newTestNotFoundError()
		},
	}
	reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
		"qumulo-csi/qumulo-a-token": {secretKeyToken: []byte("token-a")},
	}}
	srv := newCapacityServer(t, client, reader)

	resp, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "qumulo-csi", "qumulo-a-token"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(free), resp.GetAvailableCapacity())

	// The class's own Secret, in the class's own namespace.
	require.Equal(t, []string{"qumulo-csi/qumulo-a-token"}, reader.calls)
}

// TestGetCapacityIsBoundedByTheStoreRootQuota is the reason this RPC is worth
// having per class rather than per cluster: two classes on one array otherwise
// report the same number.
func TestGetCapacityIsBoundedByTheStoreRootQuota(t *testing.T) {
	tests := []struct {
		name        string
		clusterFree uint64
		quotaLimit  uint64
		want        uint64
	}{
		{name: "quota is tighter", clusterFree: 80 << 40, quotaLimit: 10 << 40, want: 10 << 40},
		{name: "cluster is tighter", clusterFree: 5 << 40, quotaLimit: 10 << 40, want: 5 << 40},
		{name: "equal", clusterFree: 10 << 40, quotaLimit: 10 << 40, want: 10 << 40},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &MockClient{
				GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
					return qumulo.FileSystemCapacity{FreeSizeBytes: tc.clusterFree}, nil
				},
				LookupFileByPathFunc: func(_ context.Context, path string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{ID: "42", Path: path}, nil
				},
				GetQuotaFunc: func(context.Context, string) (uint64, error) {
					return tc.quotaLimit, nil
				},
			}
			reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
				"ns/s": {secretKeyToken: []byte("t")},
			}}
			srv := newCapacityServer(t, client, reader)

			resp, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
				Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s"),
			})
			require.NoError(t, err)
			require.Equal(t, int64(tc.want), resp.GetAvailableCapacity())
		})
	}
}

// TestGetCapacityQuotaLookupUsesTheDomainDirectory pins the adDomain half.
//
// A class with adDomain nests its volumes one level deeper, so the quota that
// bounds it is on that subdirectory and not on the bare store path. Looking up
// the wrong one would report the parent's limit for every domain.
func TestGetCapacityQuotaLookupUsesTheDomainDirectory(t *testing.T) {
	var lookedUp string
	client := &MockClient{
		GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
			return qumulo.FileSystemCapacity{FreeSizeBytes: 80 << 40}, nil
		},
		LookupFileByPathFunc: func(_ context.Context, path string) (qumulo.FileInfo, error) {
			lookedUp = path
			return qumulo.FileInfo{ID: "42", Path: path}, nil
		},
		GetQuotaFunc: func(context.Context, string) (uint64, error) { return 1 << 40, nil },
	}
	reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
		"ns/s": {secretKeyToken: []byte("t")},
	}}
	srv := newCapacityServer(t, client, reader)

	params := capacityParams("qumulo-b.example.com", "/csi/dept", "ns", "s")
	params[paramADDomain] = "corp.example.com"

	_, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{Parameters: params})
	require.NoError(t, err)
	require.Equal(t, "/csi/dept/corp.example.com", lookedUp)
}

// TestGetCapacityFallsBackWhenTheQuotaCannotBeRead asserts the refinement is
// optional. The cluster's free space is already a valid answer, so an error
// reading the store root's quota must not fail the whole call.
func TestGetCapacityFallsBackWhenTheQuotaCannotBeRead(t *testing.T) {
	const free = uint64(80 << 40)

	tests := []struct {
		name   string
		client *MockClient
	}{
		{
			name: "store root does not exist yet",
			client: &MockClient{
				LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{}, newTestNotFoundError()
				},
			},
		},
		{
			name: "lookup fails outright",
			client: &MockClient{
				LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{}, errors.New("boom")
				},
			},
		},
		{
			name: "quota read fails outright",
			client: &MockClient{
				LookupFileByPathFunc: func(_ context.Context, path string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{ID: "42", Path: path}, nil
				},
				GetQuotaFunc: func(context.Context, string) (uint64, error) {
					return 0, errors.New("boom")
				},
			},
		},
		{
			name: "quota limit is zero",
			client: &MockClient{
				LookupFileByPathFunc: func(_ context.Context, path string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{ID: "42", Path: path}, nil
				},
				GetQuotaFunc: func(context.Context, string) (uint64, error) { return 0, nil },
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.client.GetCapacityFunc = func(context.Context) (qumulo.FileSystemCapacity, error) {
				return qumulo.FileSystemCapacity{FreeSizeBytes: free}, nil
			}
			reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
				"ns/s": {secretKeyToken: []byte("t")},
			}}
			srv := newCapacityServer(t, tc.client, reader)

			resp, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
				Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s"),
			})
			require.NoError(t, err)
			require.Equal(t, int64(free), resp.GetAvailableCapacity())
		})
	}
}

// TestGetCapacityCredentialFailuresAreFailedPrecondition covers every way the
// credential can be missing or unusable. All of them are configuration, so all
// of them are FailedPrecondition and none is Internal: an operator can fix
// each one by editing a StorageClass or a Secret.
func TestGetCapacityCredentialFailuresAreFailedPrecondition(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		reader  SecretReader
		wantMsg string
	}{
		{
			name: "class names no secret",
			params: map[string]string{
				paramServer:          "qumulo-a.example.com",
				paramStoreRealPath:   "/csi/fast",
				paramStoreExportPath: "/csi/fast",
			},
			reader:  &fakeSecretReader{},
			wantMsg: "provisioner-secret-name",
		},
		{
			name:    "secret does not exist",
			params:  capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "gone"),
			reader:  &fakeSecretReader{secrets: map[string]map[string][]byte{}},
			wantMsg: "which does not exist",
		},
		{
			name:   "secret has no token key",
			params: capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s"),
			reader: &fakeSecretReader{secrets: map[string]map[string][]byte{
				"ns/s": {"password": []byte("nope")},
			}},
			wantMsg: `has no "token" key`,
		},
		{
			name: "secret reference is templated",
			params: capacityParams(
				"qumulo-a.example.com", "/csi/fast", "${pvc.namespace}", "qumulo-token"),
			reader:  &fakeSecretReader{},
			wantMsg: "is templated",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newCapacityServer(t, &MockClient{}, tc.reader)

			_, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
				Parameters: tc.params,
			})
			require.Error(t, err)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

// TestGetCapacityWithoutKubernetesAccessIsFailedPrecondition covers the
// controller built with no SecretReader: csi-sanity, local runs, and a
// deployment that did not set -enable-capacity. It must not panic, and it must
// not claim an internal fault.
func TestGetCapacityWithoutKubernetesAccessIsFailedPrecondition(t *testing.T) {
	srv := newCapacityServer(t, &MockClient{}, nil)

	_, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s"),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "no Kubernetes access")

	// The empty request still answers, because it needs no credential.
	resp, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetAvailableCapacity())
}

// TestGetCapacityRejectsAnInvalidStorageClass asserts that the class is
// validated with the same parser CreateVolume uses. This RPC is polled every
// minute, so it reports a malformed class long before the first PVC does.
func TestGetCapacityRejectsAnInvalidStorageClass(t *testing.T) {
	reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
		"ns/s": {secretKeyToken: []byte("t")},
	}}
	srv := newCapacityServer(t, &MockClient{}, reader)

	tests := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{name: "unknown parameter", mutate: func(p map[string]string) { p["nonsense"] = "1" }},
		{name: "relative store path", mutate: func(p map[string]string) {
			p[paramStoreRealPath] = "relative/path"
		}},
		{name: "bad protocol", mutate: func(p map[string]string) { p[paramProtocol] = "nfs9" }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s")
			tc.mutate(params)

			_, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{Parameters: params})
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

// TestGetCapacityClusterErrorIsInternal separates a cluster fault from
// configuration: this one an operator cannot fix by editing a StorageClass.
func TestGetCapacityClusterErrorIsInternal(t *testing.T) {
	client := &MockClient{
		GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
			return qumulo.FileSystemCapacity{}, errors.New("connection refused")
		},
	}
	reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
		"ns/s": {secretKeyToken: []byte("t")},
	}}
	srv := newCapacityServer(t, client, reader)

	_, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s"),
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, err.Error(), "qumulo-a.example.com")
}

// TestGetCapacitySecretReadErrorIsInternal keeps an API failure distinct from
// an absent Secret. A NotFound is the administrator's to fix; a timeout
// talking to the API server is not.
func TestGetCapacitySecretReadErrorIsInternal(t *testing.T) {
	reader := &fakeSecretReader{err: errors.New("etcdserver: request timed out")}
	srv := newCapacityServer(t, &MockClient{}, reader)

	_, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s"),
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

// TestGetCapacityRoutesEachClassToItsOwnCluster is the multi-cluster property
// this RPC exists to preserve: two classes on two arrays must each be answered
// from their own Secret and their own endpoint, never from the other's.
func TestGetCapacityRoutesEachClassToItsOwnCluster(t *testing.T) {
	free := map[string]uint64{
		"https://qumulo-a.example.com:8000": 80 << 40,
		"https://qumulo-b.example.com:8000": 4 << 40,
	}
	seen := map[string]string{} // baseURL -> token

	reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
		"qumulo-csi/token-a": {secretKeyToken: []byte("secret-a")},
		"qumulo-csi/token-b": {secretKeyToken: []byte("secret-b")},
	}}

	d, err := NewDriver(Config{
		Name:         "csi.qumulo.com",
		Version:      "0.1.0",
		SecretReader: reader,
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			seen[baseURL] = token
			return &MockClient{
				GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
					return qumulo.FileSystemCapacity{FreeSizeBytes: free[baseURL]}, nil
				},
				LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{}, newTestNotFoundError()
				},
			}
		},
	})
	require.NoError(t, err)
	srv := NewControllerServer(d)

	respA, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "qumulo-csi", "token-a"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(80<<40), respA.GetAvailableCapacity())

	respB, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: capacityParams("qumulo-b.example.com", "/csi/dept", "qumulo-csi", "token-b"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(4<<40), respB.GetAvailableCapacity())

	require.Equal(t, map[string]string{
		"https://qumulo-a.example.com:8000": "secret-a",
		"https://qumulo-b.example.com:8000": "secret-b",
	}, seen)
}

// TestGetCapacityAdvertisesTheCapability guards the pairing: serving the RPC
// without advertising it means no CO ever calls it, and advertising without
// serving means every poll errors.
func TestGetCapacityAdvertisesTheCapability(t *testing.T) {
	srv := newCapacityServer(t, &MockClient{}, &fakeSecretReader{})

	resp, err := srv.ControllerGetCapabilities(
		context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	require.NoError(t, err)

	var found bool
	for _, capability := range resp.GetCapabilities() {
		if capability.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_GET_CAPACITY {
			found = true
		}
	}
	require.True(t, found, "GET_CAPACITY must be advertised now that GetCapacity is served")
}

// TestClampToInt64 covers the narrowing on the response field, which is signed
// while the cluster reports unsigned. An unchecked conversion would wrap to a
// negative number and the scheduler would read that as capacity.
func TestClampToInt64(t *testing.T) {
	require.Equal(t, int64(0), clampToInt64(0))
	require.Equal(t, int64(1<<40), clampToInt64(1<<40))
	require.Equal(t, int64(math.MaxInt64), clampToInt64(math.MaxInt64))
	require.Equal(t, int64(math.MaxInt64), clampToInt64(math.MaxUint64))
}

// TestSecretRefFromParams covers the extraction in isolation, including the
// case-insensitivity the rest of the parameter handling already has.
func TestSecretRefFromParams(t *testing.T) {
	ref, err := secretRefFromParams(map[string]string{
		"CSI.Storage.K8s.io/Provisioner-Secret-Name":      "  my-token  ",
		"csi.storage.k8s.io/provisioner-secret-namespace": "my-ns",
	})
	require.NoError(t, err)
	require.Equal(t, secretRef{Namespace: "my-ns", Name: "my-token"}, ref)
	require.Equal(t, "my-ns/my-token", ref.String())

	_, err = secretRefFromParams(map[string]string{
		coParamProvisionerSecretName: "only-a-name",
	})
	require.ErrorContains(t, err, coParamProvisionerSecretNamespace)
}

// newTestNotFoundError builds an error the qumulo package classifies as a
// not-found, so a test can drive the "no quota here" branch.
func newTestNotFoundError() error {
	return &qumulo.APIError{StatusCode: 404}
}

// The store-root quota used to be read for its limit alone, because the client
// decoded only id and limit from the quota resource. That made the answer a
// correct upper bound and a badly loose one: a class whose 10 TiB store root
// was already 9 TiB full still reported 10 TiB available. Harmless while
// nothing consumed the number -- and not harmless at all once the driver started
// publishing CSIStorageCapacity objects, because the scheduler then places
// pods against space that is not there.
func TestGetCapacityReportsRemainingQuotaNotTheLimit(t *testing.T) {
	tests := []struct {
		name        string
		clusterFree uint64
		quota       qumulo.QuotaInfo
		want        uint64
	}{
		{
			name:        "a partly used quota reports what is left",
			clusterFree: 80 << 40,
			quota:       qumulo.QuotaInfo{Limit: 10 << 40, Usage: 9 << 40, UsageKnown: true},
			want:        1 << 40,
		},
		{
			// The case that most misleads the scheduler: full, but the limit
			// alone would still advertise the whole 10 TiB.
			name:        "a full quota reports zero, not its limit",
			clusterFree: 80 << 40,
			quota:       qumulo.QuotaInfo{Limit: 10 << 40, Usage: 10 << 40, UsageKnown: true},
			want:        0,
		},
		{
			// Reachable: a quota can be lowered below what is already stored,
			// and the subtraction must not wrap.
			name:        "usage beyond the limit reports zero rather than wrapping",
			clusterFree: 80 << 40,
			quota:       qumulo.QuotaInfo{Limit: 1 << 40, Usage: 4 << 40, UsageKnown: true},
			want:        0,
		},
		{
			// An older Core release, or a token without the privilege. The
			// old upper-bound behaviour is the deliberate fallback: an
			// under-report would stop the scheduler using space that exists.
			name:        "no usage reported falls back to the limit",
			clusterFree: 80 << 40,
			quota:       qumulo.QuotaInfo{Limit: 10 << 40},
			want:        10 << 40,
		},
		{
			// Cluster free space is a real ceiling no quota can exceed, so it
			// still wins when it is the tighter of the two.
			name:        "cluster free space still wins when it is tighter",
			clusterFree: 2 << 40,
			quota:       qumulo.QuotaInfo{Limit: 10 << 40, Usage: 1 << 40, UsageKnown: true},
			want:        2 << 40,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &MockClient{
				GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
					return qumulo.FileSystemCapacity{FreeSizeBytes: tc.clusterFree}, nil
				},
				LookupFileByPathFunc: func(_ context.Context, path string) (qumulo.FileInfo, error) {
					return qumulo.FileInfo{ID: "42", Path: path}, nil
				},
				GetQuotaInfoFunc: func(context.Context, string) (qumulo.QuotaInfo, error) {
					return tc.quota, nil
				},
			}
			reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
				"ns/s": {secretKeyToken: []byte("t")},
			}}
			srv := newCapacityServer(t, client, reader)

			resp, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
				Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s"),
			})
			require.NoError(t, err)
			require.Equal(t, int64(tc.want), resp.GetAvailableCapacity())
		})
	}
}

// TestGetCapacityClassifiesAnArrayThatDidNotAnswer is. A capacity read
// that got no answer is classified the way CreateVolume classifies it:
// a name that does not resolve is a misconfiguration, InvalidArgument; a
// timeout or refused connection is the array being down, Unavailable; a call
// its caller cancelled is Canceled. Every one of them used to be Internal, so
// a broken class and a restarting array read the same in the provisioner's
// log. An answer that refused stays Internal
// (TestGetCapacityClusterErrorIsInternal).
func TestGetCapacityClassifiesAnArrayThatDidNotAnswer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		want  codes.Code
	}{
		{"NXDOMAIN", &net.DNSError{Name: "qumulo-a.example.com", Err: "no such host", IsNotFound: true}, codes.InvalidArgument},
		{"timeout", context.DeadlineExceeded, codes.Unavailable},
		{"refused", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, codes.Unavailable},
		{"cancelled by the caller", context.Canceled, codes.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &MockClient{
				GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
					return qumulo.FileSystemCapacity{}, fmt.Errorf("calling /v1/file-system: %w",
						&url.Error{Op: "Get", URL: "https://qumulo-a.example.com:8000/v1/file-system", Err: tc.cause})
				},
			}
			reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
				"ns/s": {secretKeyToken: []byte("t")},
			}}
			srv := newCapacityServer(t, client, reader)

			_, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
				Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s"),
			})
			require.Error(t, err)
			require.Equal(t, tc.want, status.Code(err), "%v", err)
			require.Contains(t, err.Error(), "qumulo-a.example.com")
		})
	}
}

// With the breaker in the path. Two NXDOMAIN answers in a row
// open the endpoint's breaker, and its fail-fast answered Unavailable ("did
// not answer ... not calling it again yet"), so GetCapacity alternated
// between InvalidArgument and Unavailable for a class no retry can fix.
// Every poll now answers InvalidArgument, and the calls the
// breaker stops still do not go out.
func TestGetCapacityKeepsInvalidArgumentWhileTheBreakerIsOpen(t *testing.T) {
	dials := 0
	breaker := qumulo.NewBreaker()
	d, err := NewDriver(Config{
		Name: "csi.qumulo.com", Version: "0.1.0",
		SecretReader: &fakeSecretReader{secrets: map[string]map[string][]byte{"ns/s": {secretKeyToken: []byte("t")}}},
		NewClient: func(baseURL, token string) qumulo.ClientInterface {
			c := qumulo.New(baseURL, token)
			c.Breaker = breaker
			c.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				dials++
				return nil, &net.DNSError{Name: "qumulo-a.example.com", Err: "no such host", IsNotFound: true}
			})}
			return c
		},
	})
	require.NoError(t, err)
	srv := NewControllerServer(d)

	for i := 0; i < 5; i++ {
		_, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
			Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s"),
		})
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "poll %d: %v", i+1, err)
	}
	require.Less(t, dials, 5, "the breaker still stops calls to the endpoint")
}

// roundTripFunc is an http.RoundTripper made of a function.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// namedMock is a MockClient that names its endpoint, as *qumulo.Client does,
// which is what lets GetCapacity keep its no-quota hint.
type namedMock struct {
	*MockClient
	endpoint string
}

func (n namedMock) Endpoint() string { return n.endpoint }

// TestGetCapacitySkipsTheQuotaLookupForAStoreRootWithNone is. A store
// root with no quota cost two REST calls per poll, one of them a 404, every
// minute for every class. The "no quota" answer is now trusted for
// noQuotaHintTTL; a quota that exists is read on every poll, so its usage
// stays current; and an expired hint is asked again.
func TestGetCapacitySkipsTheQuotaLookupForAStoreRootWithNone(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	capacityNow = func() time.Time { return now }
	t.Cleanup(func() { capacityNow = time.Now; noQuotaHints.Clear(); freeSpaceHints.Clear() })

	reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
		"ns/s": {secretKeyToken: []byte("t")},
	}}
	poll := func(t *testing.T, srv *ControllerServer, root string) {
		t.Helper()
		_, err := srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
			Parameters: capacityParams("qumulo-a.example.com", root, "ns", "s"),
		})
		require.NoError(t, err)
	}

	t.Run("no quota", func(t *testing.T) {
		var lookups, quotaReads int
		mock := &MockClient{
			GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
				return qumulo.FileSystemCapacity{FreeSizeBytes: 100 << 30}, nil
			},
			LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
				lookups++
				return qumulo.FileInfo{ID: "7"}, nil
			},
			GetQuotaInfoFunc: func(context.Context, string) (qumulo.QuotaInfo, error) {
				quotaReads++
				return qumulo.QuotaInfo{}, &qumulo.APIError{StatusCode: 404, Class: "api_quotas_quota_limit_not_found_error"}
			},
		}
		srv := newCapacityServer(t, nil, reader)
		srv.driver.NewClient = func(string, string) qumulo.ClientInterface {
			return namedMock{MockClient: mock, endpoint: "https://qumulo-a.example.com:8000"}
		}

		for range 3 {
			poll(t, srv, "/csi/none")
		}
		require.Equal(t, 1, lookups, "a store root with no quota is looked up once, not every poll")
		require.Equal(t, 1, quotaReads)

		now = now.Add(noQuotaHintTTL)
		poll(t, srv, "/csi/none")
		require.Equal(t, 2, quotaReads, "an expired hint is asked again")
	})

	t.Run("a quota that exists is read every poll", func(t *testing.T) {
		var quotaReads int
		mock := &MockClient{
			GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
				return qumulo.FileSystemCapacity{FreeSizeBytes: 100 << 30}, nil
			},
			LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
				return qumulo.FileInfo{ID: "8"}, nil
			},
			GetQuotaInfoFunc: func(context.Context, string) (qumulo.QuotaInfo, error) {
				quotaReads++
				return qumulo.QuotaInfo{Limit: 10 << 30}, nil
			},
		}
		srv := newCapacityServer(t, nil, reader)
		srv.driver.NewClient = func(string, string) qumulo.ClientInterface {
			return namedMock{MockClient: mock, endpoint: "https://qumulo-a.example.com:8000"}
		}
		for range 3 {
			poll(t, srv, "/csi/bounded")
		}
		require.Equal(t, 3, quotaReads)
	})
}

// TestGetCapacitySharesOneFreeSpaceReadPerArray is the remainder: every
// class asked its array for /v1/file-system on every poll, so classes sharing
// an array repeated the same read. A successful read is now reused for
// freeSpaceHintTTL by any class on the same endpoint with the same token; a
// different token, a failed read, and an expired hint all read afresh.
func TestGetCapacitySharesOneFreeSpaceReadPerArray(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	capacityNow = func() time.Time { return now }
	t.Cleanup(func() { capacityNow = time.Now; noQuotaHints.Clear(); freeSpaceHints.Clear() })

	reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
		"ns/s":       {secretKeyToken: []byte("t")},
		"ns/rotated": {secretKeyToken: []byte("t2")},
	}}
	var reads int
	var readErr error
	mock := &MockClient{
		GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
			reads++
			if readErr != nil {
				return qumulo.FileSystemCapacity{}, readErr
			}
			return qumulo.FileSystemCapacity{FreeSizeBytes: uint64(100+reads) << 30}, nil
		},
		LookupFileByPathFunc: func(context.Context, string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{}, &qumulo.APIError{StatusCode: 404}
		},
	}
	srv := newCapacityServer(t, nil, reader)
	srv.driver.NewClient = func(string, string) qumulo.ClientInterface {
		return namedMock{MockClient: mock, endpoint: "https://qumulo-a.example.com:8000"}
	}
	poll := func(root, secret string) (*csi.GetCapacityResponse, error) {
		return srv.GetCapacity(context.Background(), &csi.GetCapacityRequest{
			Parameters: capacityParams("qumulo-a.example.com", root, "ns", secret),
		})
	}

	first, err := poll("/csi/a", "s")
	require.NoError(t, err)
	for _, root := range []string{"/csi/b", "/csi/c"} {
		resp, err := poll(root, "s")
		require.NoError(t, err)
		require.Equal(t, first.GetAvailableCapacity(), resp.GetAvailableCapacity())
	}
	require.Equal(t, 1, reads, "three classes on one array and token share one read")

	_, err = poll("/csi/a", "rotated")
	require.NoError(t, err)
	require.Equal(t, 2, reads, "a different token reads afresh")

	now = now.Add(freeSpaceHintTTL)
	readErr = &qumulo.APIError{StatusCode: 401}
	_, err = poll("/csi/a", "s")
	require.Error(t, err, "an expired hint reads afresh and reports the failure")
	_, err = poll("/csi/b", "s")
	require.Error(t, err, "a failed read is not remembered")
	require.Equal(t, 4, reads)

	readErr = nil
	resp, err := poll("/csi/a", "s")
	require.NoError(t, err)
	require.Equal(t, 5, reads)
	require.NotEqual(t, first.GetAvailableCapacity(), resp.GetAvailableCapacity(), "the new read is what is reported")
}

// TestGetCapacityReportsWholeGiB is. csi-provisioner rewrites a
// CSIStorageCapacity object whenever the value changes, so free space
// reported to the byte rewrote every class's object on every poll of an array
// taking any writes. Above 1 GiB the answer is rounded down to whole GiB --
// down, so the scheduler is never told about space that is not there. Below
// 1 GiB it stays exact: rounding that to zero would refuse a small claim the
// array can hold.
func TestGetCapacityReportsWholeGiB(t *testing.T) {
	const gib = uint64(1) << 30
	for _, tc := range []struct {
		free uint64
		want int64
	}{
		{free: 100*gib + 12345, want: int64(100 * gib)},
		{free: 100*gib + gib - 1, want: int64(100 * gib)},
		{free: 3 * gib, want: int64(3 * gib)},
		{free: gib, want: int64(gib)},
		{free: gib - 1, want: int64(gib - 1)},
		{free: 512 << 20, want: 512 << 20},
		{free: 0, want: 0},
	} {
		client := &MockClient{
			GetCapacityFunc: func(context.Context) (qumulo.FileSystemCapacity, error) {
				return qumulo.FileSystemCapacity{FreeSizeBytes: tc.free}, nil
			},
		}
		reader := &fakeSecretReader{secrets: map[string]map[string][]byte{
			"ns/s": {secretKeyToken: []byte("t")},
		}}
		resp, err := newCapacityServer(t, client, reader).GetCapacity(context.Background(), &csi.GetCapacityRequest{
			Parameters: capacityParams("qumulo-a.example.com", "/csi/fast", "ns", "s"),
		})
		require.NoError(t, err)
		require.Equal(t, tc.want, resp.GetAvailableCapacity(), "free %d", tc.free)
	}
}
