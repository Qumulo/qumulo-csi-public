//go:build chart && defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

// dfxFakeAPI is a minimal, read-only Kubernetes API server on 127.0.0.1.
//
// It exists so the chart's render-time guards against the live cluster --
// Helm's `lookup` and .Capabilities -- can be exercised without a cluster:
// `helm template --dry-run=server` answers `lookup` from whatever API server
// the kubeconfig names. It serves discovery for the kinds the chart renders
// or looks up, and GET/LIST of objects a test seeds. Every other request is a
// 404, and nothing is ever written: a dry run does not write.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type dfxResource struct {
	group, version, resource, kind string
	namespaced                     bool
}

func (r dfxResource) gv() string {
	if r.group == "" {
		return r.version
	}
	return r.group + "/" + r.version
}

// dfxBaseResources are the kinds every fake cluster serves: everything the
// chart renders in any configuration, plus what its guards look up. The
// snapshot and replication CRD kinds are opt-in per test (dfxWithSnapshotCRD,
// dfxWithReplicationCRD), because their absence is what TestDefect_MissingClassCRDIsRefusedClearly
// is about.
var dfxBaseResources = []dfxResource{
	{"", "v1", "namespaces", "Namespace", false},
	{"", "v1", "serviceaccounts", "ServiceAccount", true},
	{"", "v1", "services", "Service", true},
	{"", "v1", "secrets", "Secret", true},
	{"", "v1", "configmaps", "ConfigMap", true},
	{"", "v1", "pods", "Pod", true},
	{"", "v1", "persistentvolumes", "PersistentVolume", false},
	{"apps", "v1", "deployments", "Deployment", true},
	{"apps", "v1", "daemonsets", "DaemonSet", true},
	{"rbac.authorization.k8s.io", "v1", "clusterroles", "ClusterRole", false},
	{"rbac.authorization.k8s.io", "v1", "clusterrolebindings", "ClusterRoleBinding", false},
	{"rbac.authorization.k8s.io", "v1", "roles", "Role", true},
	{"rbac.authorization.k8s.io", "v1", "rolebindings", "RoleBinding", true},
	{"storage.k8s.io", "v1", "storageclasses", "StorageClass", false},
	{"storage.k8s.io", "v1", "csidrivers", "CSIDriver", false},
	{"scheduling.k8s.io", "v1", "priorityclasses", "PriorityClass", false},
	{"policy", "v1", "poddisruptionbudgets", "PodDisruptionBudget", true},
	{"networking.k8s.io", "v1", "networkpolicies", "NetworkPolicy", true},
	{"apiextensions.k8s.io", "v1", "customresourcedefinitions", "CustomResourceDefinition", false},
}

var (
	dfxSnapshotClassResource    = dfxResource{"snapshot.storage.k8s.io", "v1", "volumesnapshotclasses", "VolumeSnapshotClass", false}
	dfxReplicationClassResource = dfxResource{"replication.storage.openshift.io", "v1alpha1", "volumereplicationclasses", "VolumeReplicationClass", false}
)

type dfxFakeAPI struct {
	t         *testing.T
	srv       *httptest.Server
	mu        sync.Mutex
	resources []dfxResource
	// objects by "<gv>/<resource>/<namespace>/<name>"
	objects map[string]map[string]any
	// requests seen, for diagnosing a test
	seen []string
}

func dfxNewFakeAPI(t *testing.T, extra ...dfxResource) *dfxFakeAPI {
	t.Helper()
	f := &dfxFakeAPI{t: t, objects: map[string]map[string]any{}}
	f.resources = append(append([]dfxResource{}, dfxBaseResources...), extra...)
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	// The guards in the fixed chart check for kube-system before trusting a
	// lookup at all; a real cluster always has it.
	f.seed("v1", "Namespace", "", "kube-system", nil)
	f.seed("v1", "Namespace", "", dfxNamespace, nil)
	return f
}

func (f *dfxFakeAPI) resourceFor(apiVersion, kind string) dfxResource {
	for _, r := range f.resources {
		if r.gv() == apiVersion && r.kind == kind {
			return r
		}
	}
	f.t.Fatalf("fake API serves no %s %s", apiVersion, kind)
	return dfxResource{}
}

// seed stores one object. body is merged over apiVersion/kind/metadata.
func (f *dfxFakeAPI) seed(apiVersion, kind, namespace, name string, body map[string]any) {
	r := f.resourceFor(apiVersion, kind)
	obj := map[string]any{}
	for k, v := range body {
		obj[k] = v
	}
	meta := dfxMap(obj["metadata"])
	if meta == nil {
		meta = map[string]any{}
	}
	meta["name"] = name
	if namespace != "" {
		meta["namespace"] = namespace
	}
	if _, ok := meta["uid"]; !ok {
		meta["uid"] = fmt.Sprintf("uid-%s-%s", r.resource, name)
	}
	if _, ok := meta["resourceVersion"]; !ok {
		meta["resourceVersion"] = "1"
	}
	obj["metadata"] = meta
	obj["apiVersion"] = apiVersion
	obj["kind"] = kind
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[r.gv()+"/"+r.resource+"/"+namespace+"/"+name] = obj
}

// dfxHelmOwned is the metadata Helm puts on an object it manages for this
// test's release.
func dfxHelmOwned(extraAnnotations map[string]any) map[string]any {
	ann := map[string]any{
		"meta.helm.sh/release-name":      dfxRelease,
		"meta.helm.sh/release-namespace": dfxNamespace,
	}
	for k, v := range extraAnnotations {
		ann[k] = v
	}
	return map[string]any{
		"labels":      map[string]any{"app.kubernetes.io/managed-by": "Helm"},
		"annotations": ann,
	}
}

func (f *dfxFakeAPI) writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *dfxFakeAPI) notFound(w http.ResponseWriter, what string) {
	f.writeJSON(w, http.StatusNotFound, map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Failure",
		"message": what + " not found", "reason": "NotFound", "code": 404,
	})
}

func (f *dfxFakeAPI) serve(w http.ResponseWriter, req *http.Request) {
	f.mu.Lock()
	f.seen = append(f.seen, req.Method+" "+req.URL.Path)
	f.mu.Unlock()

	if req.Method != http.MethodGet {
		f.writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
			"kind": "Status", "apiVersion": "v1", "status": "Failure",
			"message": "fake API is read-only", "reason": "MethodNotAllowed", "code": 405,
		})
		return
	}

	path := strings.TrimSuffix(req.URL.Path, "/")
	switch path {
	case "/version":
		f.writeJSON(w, 200, map[string]any{
			"major": "1", "minor": "36", "gitVersion": "v1.36.0",
			"platform": "linux/amd64", "goVersion": "go1.26.0", "compiler": "gc",
		})
		return
	case "/api":
		f.writeJSON(w, 200, map[string]any{
			"kind": "APIVersions", "versions": []string{"v1"},
			"serverAddressByClientCIDRs": []any{map[string]any{"clientCIDR": "0.0.0.0/0", "serverAddress": "127.0.0.1"}},
		})
		return
	case "/apis":
		f.writeJSON(w, 200, f.groupList())
		return
	}

	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")
	var gv string
	var rest []string
	switch {
	case len(segs) >= 2 && segs[0] == "api":
		gv, rest = segs[1], segs[2:]
	case len(segs) >= 3 && segs[0] == "apis":
		gv, rest = segs[1]+"/"+segs[2], segs[3:]
	default:
		f.notFound(w, path)
		return
	}
	if len(rest) == 0 {
		// A group/version the cluster does not serve is a 404, as on a real
		// API server -- not an empty list.
		list := f.resourceList(gv)
		if len(dfxList(list["resources"])) == 0 {
			f.notFound(w, path)
			return
		}
		f.writeJSON(w, 200, list)
		return
	}
	ns := ""
	if len(rest) >= 2 && rest[0] == "namespaces" && len(rest) != 2 {
		ns, rest = rest[1], rest[2:]
	}
	var res *dfxResource
	for i := range f.resources {
		if f.resources[i].gv() == gv && f.resources[i].resource == rest[0] {
			res = &f.resources[i]
		}
	}
	if res == nil {
		f.notFound(w, path)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(rest) == 1 {
		items := []any{}
		prefix := gv + "/" + res.resource + "/"
		for key, obj := range f.objects {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if ns != "" && !strings.HasPrefix(key, prefix+ns+"/") {
				continue
			}
			items = append(items, obj)
		}
		f.writeJSON(w, 200, map[string]any{
			"kind": res.kind + "List", "apiVersion": gv,
			"metadata": map[string]any{"resourceVersion": "1"}, "items": items,
		})
		return
	}
	obj, ok := f.objects[gv+"/"+res.resource+"/"+ns+"/"+rest[1]]
	if !ok {
		f.notFound(w, res.resource+" "+rest[1])
		return
	}
	f.writeJSON(w, 200, obj)
}

func (f *dfxFakeAPI) groupList() map[string]any {
	order := []string{}
	versions := map[string][]string{}
	for _, r := range f.resources {
		if r.group == "" {
			continue
		}
		if _, ok := versions[r.group]; !ok {
			order = append(order, r.group)
		}
		found := false
		for _, v := range versions[r.group] {
			found = found || v == r.version
		}
		if !found {
			versions[r.group] = append(versions[r.group], r.version)
		}
	}
	groups := []any{}
	for _, g := range order {
		vs := []any{}
		for _, v := range versions[g] {
			vs = append(vs, map[string]any{"groupVersion": g + "/" + v, "version": v})
		}
		groups = append(groups, map[string]any{
			"name": g, "versions": vs, "preferredVersion": vs[0],
		})
	}
	return map[string]any{"kind": "APIGroupList", "apiVersion": "v1", "groups": groups}
}

func (f *dfxFakeAPI) resourceList(gv string) map[string]any {
	list := []any{}
	for _, r := range f.resources {
		if r.gv() != gv {
			continue
		}
		list = append(list, map[string]any{
			"name": r.resource, "singularName": strings.ToLower(r.kind), "namespaced": r.namespaced,
			"kind": r.kind, "verbs": []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		})
	}
	return map[string]any{"kind": "APIResourceList", "apiVersion": "v1", "groupVersion": gv, "resources": list}
}

// kubeconfig writes a kubeconfig naming this server and returns the helm
// environment that uses it.
func (f *dfxFakeAPI) env(t *testing.T) []string {
	t.Helper()
	env := dfxHelmEnv(t)
	path := filepath.Join(t.TempDir(), "kubeconfig")
	cfg := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: fake
  cluster:
    server: %s
contexts:
- name: fake
  context:
    cluster: fake
    user: fake
    namespace: %s
users:
- name: fake
  user: {}
current-context: fake
`, f.srv.URL, dfxNamespace)
	require.NoError(t, os.WriteFile(path, []byte(cfg), 0o600))
	for i, kv := range env {
		if strings.HasPrefix(kv, "KUBECONFIG=") {
			env[i] = "KUBECONFIG=" + path
		}
	}
	return env
}

// render runs `helm template --dry-run=server` against this server, as an
// upgrade when upgrade is set. That is the render `helm upgrade` performs
// before it applies anything, with `lookup` answered by the cluster.
func (f *dfxFakeAPI) render(t *testing.T, upgrade bool, values string, sets ...string) (string, error) {
	t.Helper()
	args := dfxTemplateArgs(t, chartPath, values, sets...)
	// --validate makes the render use the server's discovery (Capabilities
	// and kind mappings), as `helm upgrade` does; the fake serves no OpenAPI.
	args = append(args, "--dry-run=server", "--validate", "--disable-openapi-validation")
	if upgrade {
		args = append(args, "--is-upgrade")
	}
	out, err := dfxHelm(t, f.env(t), args...)
	return out, err
}

func (f *dfxFakeAPI) requests() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.seen, "\n")
}
