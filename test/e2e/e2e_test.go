//go:build e2e

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package e2e drives the CSI driver as it is actually deployed: the Helm
// chart's controller Deployment and node DaemonSet, the official CSI sidecars,
// and the kubelet performing the mount.
//
// Nothing here is mocked or simulated. Volumes are requested with real PVCs,
// written by real pods, and every effect is verified against the live Qumulo
// cluster's REST API rather than by asking the driver whether the driver
// worked.
//
// Gates: the "e2e" build tag and QUMULO_E2E=1, plus the same store-path
// sentinel the live suite uses.
package e2e

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"qumulo-csi-driver/pkg/qumulo"
)

const (
	envEnable      = "QUMULO_E2E"
	envAPIURL      = "QUMULO_API_URL"
	envTokenFile   = "QUMULO_TOKEN_FILE"
	envStoreReal   = "QUMULO_STORE_REAL_PATH"
	envStoreExport = "QUMULO_STORE_EXPORT_PATH"
	envTLSInsecure = "QUMULO_TLS_INSECURE"
	envNamespace   = "QUMULO_E2E_NAMESPACE"

	storePathSentinel    = "csi-live-test"
	driverNamespace      = "qumulo-csi"
	nodeDaemonSet        = "ds/qumulo-csi-qumulo-csi-driver-node"
	controllerDeployment = "deploy/qumulo-csi-qumulo-csi-driver-controller"
	gib                  = 1 << 30

	// helmReleaseName and chartPath are the release
	// TestE2EUninstallReinstallPreservesRunningIO uninstalls and reinstalls.
	// They match what make e2e-minikube deploys, since that test operates on
	// the actual deployed release rather than a scratch one.
	helmReleaseName = "qumulo-csi"
	chartPath       = "../../charts/qumulo-csi-driver"
)

// nfsV4Enabled reports whether the cluster can serve NFSv4 at all.
//
// The driver refuses a protocol: nfs4.1 StorageClass on a cluster with NFSv4
// off, so every v4.1 case here would fail for a reason that is correct rather
// than interesting. Reading the cluster's own answer keeps the suite honest on
// either configuration instead of encoding one of them.
func (c config) nfsV4Enabled(t *testing.T) bool {
	t.Helper()

	settings, err := c.Client.GetNFSSettings(context.Background())
	if err != nil {
		t.Fatalf("reading the cluster's NFS settings: %v", err)
	}
	return settings.V4Enabled
}

// requireNFSv4 skips a test that cannot mean anything with NFSv4 disabled.
func requireNFSv4(t *testing.T, cfg config) {
	t.Helper()

	if !cfg.nfsV4Enabled(t) {
		t.Skipf("the cluster reports v4_enabled false, so protocol nfs4.1 cannot be provisioned; " +
			"enable NFSv4 (PATCH /v2/nfs/settings) to run this")
	}
}

type config struct {
	APIURL        string
	TokenFile     string
	StoreRealPath string
	// StoreExportPath is the same directory as seen in the NFS namespace. The
	// two differ whenever the export is not mounted at the filesystem root,
	// and the nested-export rule is expressed in *this* namespace, not the
	// filesystem one.
	StoreExportPath string
	TLSInsecure     bool
	Namespace       string
	Client          *qumulo.Client
	RunID           string
}

func load(t *testing.T) config {
	t.Helper()

	if os.Getenv(envEnable) != "1" {
		t.Fatalf("%s must be set to 1: these tests deploy PVCs and pods against a live cluster", envEnable)
	}
	cfg := config{
		APIURL:          strings.TrimRight(strings.TrimSpace(os.Getenv(envAPIURL)), "/"),
		TokenFile:       strings.TrimSpace(os.Getenv(envTokenFile)),
		StoreRealPath:   strings.TrimSpace(os.Getenv(envStoreReal)),
		StoreExportPath: strings.TrimSpace(os.Getenv(envStoreExport)),
		TLSInsecure:     os.Getenv(envTLSInsecure) == "1",
		Namespace:       strings.TrimSpace(os.Getenv(envNamespace)),
	}
	if cfg.APIURL == "" || cfg.TokenFile == "" || cfg.StoreRealPath == "" {
		t.Fatalf("%s, %s and %s are all required", envAPIURL, envTokenFile, envStoreReal)
	}
	if cfg.StoreExportPath == "" {
		// They coincide only when the export sits at the filesystem root.
		cfg.StoreExportPath = cfg.StoreRealPath
	}
	// The same guard the live suite uses: these tests provision into a path
	// the driver later tree-deletes, so it must be a dedicated test directory
	// whose name says so.
	if !strings.Contains(cfg.StoreRealPath, storePathSentinel) {
		t.Fatalf("refusing to run: %s=%q does not contain %q", envStoreReal, cfg.StoreRealPath, storePathSentinel)
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "qumulo-e2e"
	}

	client := qumulo.NewWithTokenFunc(cfg.APIURL, qumulo.TokenFromFile(cfg.TokenFile))
	client.HTTPClient = &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.TLSInsecure, MinVersion: tls.VersionTLS12}},
	}
	cfg.Client = client

	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("run ID: %v", err)
	}
	cfg.RunID = hex.EncodeToString(b[:])

	t.Logf("e2e config: api=%s store=%s ns=%s runID=%s", cfg.APIURL, cfg.StoreRealPath, cfg.Namespace, cfg.RunID)
	return cfg
}

// hostFromURL strips the scheme and port from an API URL, leaving the host a
// node actually mounts from.
func hostFromURL(apiURL string) string {
	h := strings.TrimPrefix(strings.TrimPrefix(apiURL, "https://"), "http://")
	if i := strings.Index(h, "/"); i >= 0 {
		h = h[:i]
	}
	if i := strings.LastIndex(h, ":"); i >= 0 {
		h = h[:i]
	}
	return h
}

// ---------------------------------------------------------------------------
// kubectl plumbing. Shelling out on purpose: this is the interface an operator
// uses, and it keeps the test honest about what is observable from outside.

func kubectl(t *testing.T, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func mustKubectl(t *testing.T, args ...string) string {
	t.Helper()
	out, err := kubectl(t, args...)
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func kubectlApply(t *testing.T, manifest string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("kubectl apply: %v\n%s\nmanifest:\n%s", err, out, manifest)
	}
}

// podExec runs a command inside a workload pod.
//
// The container is named explicitly, as nodeExec does. kubectl picks a default
// container on its own, but when a pod has more than one -- an init container
// counts -- it announces the choice on stderr:
//
//	Defaulted container "app" out of: app, fix-ownership (init)
//
// kubectl here captures combined output, so that notice lands in front of the
// command's own output and every assertion comparing it to an exact string
// fails. It cost a round to find (F6, 2026-09-21), because the pods that grew
// an init container were exactly the ones whose output is compared byte for
// byte. Both workload fixtures name their container "app".
func podExec(t *testing.T, cfg config, pod, script string) (string, error) {
	t.Helper()
	return kubectl(t, "-n", cfg.Namespace, "exec", pod, "-c", "app", "--", "sh", "-c", script)
}

func mustPodExec(t *testing.T, cfg config, pod, script string) string {
	t.Helper()
	out, err := podExec(t, cfg, pod, script)
	if err != nil {
		t.Fatalf("exec in %s (%s): %v\n%s", pod, script, err, out)
	}
	return out
}

// nodeExec runs a command in the driver's node container -- the shipped image.
func nodeExec(t *testing.T, script string) (string, error) {
	t.Helper()
	return kubectl(t, "-n", driverNamespace, "exec", nodeDaemonSet,
		"-c", "qumulo-csi-driver", "--", "sh", "-c", script)
}

// ---------------------------------------------------------------------------
// Cluster-side verification, through the REST API.

// readFileContent reads a file's bytes from the Qumulo cluster. The client has
// no method for this because the driver never needs one.
func readFileContent(t *testing.T, cfg config, filePath string) ([]byte, error) {
	t.Helper()

	endpoint := fmt.Sprintf("%s/v1/files/%s/data", cfg.APIURL, strings.ReplaceAll(filePath, "/", "%2F"))
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	token, err := qumulo.TokenFromFile(cfg.TokenFile)()
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := cfg.Client.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", filePath, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// fileAttributes reads a path's mode and owner from the cluster. FileInfo
// carries neither, and the mode is what decides whether a pod can write.
func fileAttributes(t *testing.T, cfg config, filePath string) (mode, owner string, err error) {
	t.Helper()

	endpoint := fmt.Sprintf("%s/v1/files/%s/info/attributes", cfg.APIURL, strings.ReplaceAll(filePath, "/", "%2F"))
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return "", "", err
	}
	token, err := qumulo.TokenFromFile(cfg.TokenFile)()
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := cfg.Client.HTTPClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode/100 != 2 {
		return "", "", fmt.Errorf("GET attributes %s: HTTP %d: %s", filePath, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var decoded struct {
		Mode         string `json:"mode"`
		OwnerDetails struct {
			IDValue string `json:"id_value"`
		} `json:"owner_details"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", "", err
	}
	return decoded.Mode, decoded.OwnerDetails.IDValue, nil
}

// ---------------------------------------------------------------------------
// Waiting helpers. Every wait reports what it last saw, so a timeout says why.

func waitFor(t *testing.T, what string, budget time.Duration, probe func() (bool, string)) {
	t.Helper()

	deadline := time.Now().Add(budget)
	var last string
	for time.Now().Before(deadline) {
		ok, state := probe()
		last = state
		if ok {
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("timed out after %s waiting for %s; last state: %s", budget, what, last)
}

func pvcPhase(t *testing.T, cfg config, name string) string {
	out, err := kubectl(t, "-n", cfg.Namespace, "get", "pvc", name, "-o", "jsonpath={.status.phase}")
	if err != nil {
		return "error: " + strings.TrimSpace(out)
	}
	return strings.TrimSpace(out)
}

func podPhase(t *testing.T, cfg config, name string) string {
	out, err := kubectl(t, "-n", cfg.Namespace, "get", "pod", name, "-o", "jsonpath={.status.phase}")
	if err != nil {
		return "error: " + strings.TrimSpace(out)
	}
	return strings.TrimSpace(out)
}

func waitPVCBound(t *testing.T, cfg config, name string) {
	t.Helper()
	waitFor(t, fmt.Sprintf("PVC %s to bind", name), 3*time.Minute, func() (bool, string) {
		p := pvcPhase(t, cfg, name)
		if p == "Bound" {
			return true, p
		}
		// Surface the provisioning error rather than just "Pending".
		ev, _ := kubectl(t, "-n", cfg.Namespace, "describe", "pvc", name)
		lines := strings.Split(ev, "\n")
		tail := lines
		if len(lines) > 6 {
			tail = lines[len(lines)-6:]
		}
		return false, p + " | " + strings.Join(tail, " ")
	})
}

func waitPodReady(t *testing.T, cfg config, name string) {
	t.Helper()
	waitFor(t, fmt.Sprintf("pod %s to be Running", name), 3*time.Minute, func() (bool, string) {
		p := podPhase(t, cfg, name)
		if p == "Running" {
			return true, p
		}
		ev, _ := kubectl(t, "-n", cfg.Namespace, "describe", "pod", name)
		lines := strings.Split(ev, "\n")
		tail := lines
		if len(lines) > 8 {
			tail = lines[len(lines)-8:]
		}
		return false, p + " | " + strings.Join(tail, " ")
	})
}

// ---------------------------------------------------------------------------
// Manifests.

func pvcManifest(ns, name, class, size string, rwx bool) string {
	mode := "ReadWriteOnce"
	if rwx {
		mode = "ReadWriteMany"
	}
	return fmt.Sprintf(`apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: %s
spec:
  accessModes: [%s]
  storageClassName: %s
  resources:
    requests:
      storage: %s
`, name, ns, mode, class, size)
}

func podManifest(ns, name, claim string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  terminationGracePeriodSeconds: 1
  containers:
    - name: app
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: %s
`, name, ns, claim)
}

// ---------------------------------------------------------------------------

type protocolCase struct {
	class    string
	protocol string
	wantVers string
}

func TestE2EProtocolMatrix(t *testing.T) {
	cfg := load(t)
	ensureNamespace(t, cfg)

	cases := []protocolCase{
		{class: "qumulo-nfs3", protocol: "nfs3", wantVers: "vers=3"},
	}
	// Only offer the v4.1 case where the cluster can actually serve it.
	if cfg.nfsV4Enabled(t) {
		cases = append(cases, protocolCase{class: "qumulo-nfs41", protocol: "nfs4.1", wantVers: "vers=4.1"})
	} else {
		t.Log("cluster reports v4_enabled false: the nfs4.1 case is skipped, nfs3 still runs")
	}

	for _, tc := range cases {
		t.Run(tc.protocol, func(t *testing.T) {
			runProtocolCase(t, cfg, tc)
		})
	}
}

func ensureNamespace(t *testing.T, cfg config) {
	t.Helper()
	out, err := kubectl(t, "create", "namespace", cfg.Namespace)
	if err != nil && !strings.Contains(out, "AlreadyExists") {
		t.Fatalf("creating namespace %s: %v\n%s", cfg.Namespace, err, out)
	}
}

func runProtocolCase(t *testing.T, cfg config, tc protocolCase) {
	short := strings.ReplaceAll(tc.protocol, ".", "")
	pvcName := fmt.Sprintf("e2e-%s-%s", short, cfg.RunID)
	podName := pvcName + "-pod"

	// Cleanup first, so a failing assertion still tidies the cluster.
	t.Cleanup(func() {
		kubectl(t, "-n", cfg.Namespace, "delete", "pod", podName, "--ignore-not-found", "--force", "--grace-period=0")
		kubectl(t, "-n", cfg.Namespace, "delete", "pvc", pvcName, "--ignore-not-found")
	})

	kubectlApply(t, pvcManifest(cfg.Namespace, pvcName, tc.class, "1Gi", true))
	kubectlApply(t, podManifest(cfg.Namespace, podName, pvcName))

	// --- E3.1: the PVC binds and the PV carries what the class asked for ---
	waitPVCBound(t, cfg, pvcName)
	pvName := strings.TrimSpace(mustKubectl(t, "-n", cfg.Namespace, "get", "pvc", pvcName, "-o", "jsonpath={.spec.volumeName}"))
	attrsRaw := mustKubectl(t, "get", "pv", pvName, "-o", "jsonpath={.spec.csi.volumeAttributes}")

	var attrs map[string]string
	if err := json.Unmarshal([]byte(attrsRaw), &attrs); err != nil {
		t.Fatalf("decoding volumeAttributes %q: %v", attrsRaw, err)
	}
	t.Logf("E3.1 PV %s volumeAttributes=%v", pvName, attrs)
	if attrs["protocol"] != tc.protocol {
		t.Errorf("PV protocol = %q, want %q", attrs["protocol"], tc.protocol)
	}
	realPath := attrs["realPath"]
	if realPath == "" {
		t.Fatal("PV has no realPath attribute")
	}

	waitPodReady(t, cfg, podName)

	// --- E3.2: the version reached the kernel ---
	mounts := mustPodExec(t, cfg, podName, "grep /data /proc/mounts")
	t.Logf("E3.2 pod mount: %s", strings.TrimSpace(mounts))
	if !strings.Contains(mounts, tc.wantVers) {
		t.Errorf("pod mount lacks %s: %s", tc.wantVers, strings.TrimSpace(mounts))
	}

	// --- E3.3: the data path, proved from both ends ---
	payload := fmt.Sprintf("e2e-%s-%s", tc.protocol, cfg.RunID)
	mustPodExec(t, cfg, podName, fmt.Sprintf("printf '%s' > /data/proof.txt", payload))

	got, err := readFileContent(t, cfg, realPath+"/proof.txt")
	if err != nil {
		t.Fatalf("E3.3 reading proof.txt from the cluster: %v", err)
	}
	if string(got) != payload {
		t.Errorf("E3.3 cluster has %q, pod wrote %q", string(got), payload)
	} else {
		t.Logf("E3.3 PROVED: pod wrote %d bytes over %s; the REST API read back the same bytes from %s",
			len(payload), tc.protocol, realPath)
	}

	// --- E3.4: permissions and the identity files land with ---
	mustPodExec(t, cfg, podName, "mkdir -p /data/sub && echo nested > /data/sub/f.txt")
	lsOut := mustPodExec(t, cfg, podName, "ls -ln /data")
	t.Logf("E3.4 ls -ln in the pod:\n%s", strings.TrimSpace(lsOut))

	info, err := cfg.Client.LookupFileByPath(context.Background(), realPath)
	if err != nil {
		t.Fatalf("E3.4 looking up %s: %v", realPath, err)
	}
	mode, owner, err := fileAttributes(t, cfg, realPath)
	if err != nil {
		t.Fatalf("E3.4 reading attributes of %s: %v", realPath, err)
	}
	t.Logf("E3.4 cluster reports volume dir mode=%s owner=%s", mode, owner)
	if mode != "0777" {
		t.Errorf("volume directory mode = %s, want 0777", mode)
	}

	// --- E3.5: expansion, with the pod running ---
	mustKubectl(t, "-n", cfg.Namespace, "patch", "pvc", pvcName, "-p",
		`{"spec":{"resources":{"requests":{"storage":"3Gi"}}}}`)

	waitFor(t, "the quota to reach 3GiB on the cluster", 2*time.Minute, func() (bool, string) {
		q, err := cfg.Client.GetQuota(context.Background(), info.ID)
		if err != nil {
			return false, "quota read error: " + err.Error()
		}
		return q == 3*gib, fmt.Sprintf("quota=%d", q)
	})
	t.Logf("E3.5 cluster quota is exactly %d bytes after expansion", 3*gib)

	// The pod's own view should follow, since statfs reports the quota --
	// re-proved here through a real kubelet mount.
	waitFor(t, "the pod's df to show 3GiB", 90*time.Second, func() (bool, string) {
		out, err := podExec(t, cfg, podName, "df -k /data | tail -1")
		if err != nil {
			return false, "df error"
		}
		// busybox df wraps a long device name onto its own line, so the
		// device column may be absent. The last five fields are always
		// size, used, available, use%, mount point -- index from the end.
		fields := strings.Fields(out)
		if len(fields) < 5 {
			return false, strings.TrimSpace(out)
		}
		size := fields[len(fields)-5]
		return size == "3145728", strings.TrimSpace(out) + " (parsed size=" + size + "KiB)"
	})
	t.Logf("E3.5 the pod's df tracks the quota through the expansion")

	// --- E3.6: ReadWriteMany, two pods, one volume ---
	pod2 := podName + "-b"
	t.Cleanup(func() {
		kubectl(t, "-n", cfg.Namespace, "delete", "pod", pod2, "--ignore-not-found", "--force", "--grace-period=0")
	})
	kubectlApply(t, podManifest(cfg.Namespace, pod2, pvcName))
	waitPodReady(t, cfg, pod2)

	mustPodExec(t, cfg, pod2, fmt.Sprintf("printf 'from-pod-b-%s' > /data/from-b.txt", cfg.RunID))
	seenByA := mustPodExec(t, cfg, podName, "cat /data/from-b.txt")
	if !strings.Contains(seenByA, "from-pod-b-"+cfg.RunID) {
		t.Errorf("E3.6 pod A cannot see pod B's file: %q", seenByA)
	}
	seenByB := mustPodExec(t, cfg, pod2, "cat /data/proof.txt")
	if !strings.Contains(seenByB, payload) {
		t.Errorf("E3.6 pod B cannot see pod A's file: %q", seenByB)
	}
	t.Logf("E3.6 PROVED: ReadWriteMany -- two pods on one PV, each reads the other's writes")

	// --- E3.8: deleting the pods drops the node mounts ---
	before := nodeMountCount(t, cfg)
	mustKubectl(t, "-n", cfg.Namespace, "delete", "pod", pod2, "--force", "--grace-period=0")
	mustKubectl(t, "-n", cfg.Namespace, "delete", "pod", podName, "--force", "--grace-period=0")
	waitFor(t, "node mounts to drop after pod deletion", 2*time.Minute, func() (bool, string) {
		now := nodeMountCount(t, cfg)
		return now < before, fmt.Sprintf("mounts before=%d now=%d", before, now)
	})
	t.Logf("E3.8 node mount count dropped from %d after the pods were deleted", before)

	// --- E3.9: deleting the PVC removes the directory from the cluster ---
	mustKubectl(t, "-n", cfg.Namespace, "delete", "pvc", pvcName)
	waitFor(t, "the volume directory to disappear from the cluster", 2*time.Minute, func() (bool, string) {
		_, err := cfg.Client.LookupFileByPath(context.Background(), realPath)
		if err != nil && qumulo.IsNotFound(err) {
			return true, "gone"
		}
		if err != nil {
			return false, "lookup error: " + err.Error()
		}
		return false, "still present"
	})
	t.Logf("E3.9 the cluster directory %s is gone after the PVC was deleted", realPath)
}

// nodeMountCount counts NFS mounts of the Qumulo cluster on the node.
//
// The host is derived from the configured API URL rather than written in: a
// hardcoded address makes the test pass or fail for the wrong reason the moment
// it is pointed at another cluster.
func nodeMountCount(t *testing.T, cfg config) int {
	t.Helper()
	out, err := nodeExec(t, fmt.Sprintf("grep -c %s /proc/mounts || true", hostFromURL(cfg.APIURL)))
	if err != nil {
		t.Logf("counting node mounts: %v (%s)", err, out)
		return -1
	}
	var n int
	fmt.Sscanf(strings.TrimSpace(out), "%d", &n)
	return n
}

// ---------------------------------------------------------------------------
// E3.7 -- dedicated-export provisioning, on nfs4.1.
//
// The driver creates one NFS export per volume. Under v4 that matters more than
// it did under v3: driver-created exports use NFS_MAP_NONE while the
// pre-existing "/" uses NFS_MAP_ROOT -> guest, and v4 carries string identities
// rather than numeric ones.

func TestE2EDedicatedExport(t *testing.T) {
	cfg := load(t)
	requireNFSv4(t, cfg)
	ensureNamespace(t, cfg)

	pvcName := fmt.Sprintf("e2e-dedic-%s", cfg.RunID)
	podName := pvcName + "-pod"

	t.Cleanup(func() {
		kubectl(t, "-n", cfg.Namespace, "delete", "pod", podName, "--ignore-not-found", "--force", "--grace-period=0")
		kubectl(t, "-n", cfg.Namespace, "delete", "pvc", pvcName, "--ignore-not-found")
	})

	before := exportCount(t, cfg)
	ancestor := ancestorExportExists(t, cfg)
	t.Logf("E3.7 exports before: %d; an ancestor export of the store path exists: %t", before, ancestor)

	kubectlApply(t, pvcManifest(cfg.Namespace, pvcName, "qumulo-nfs41-dedicated", "1Gi", true))

	// With NFSv4 enabled the cluster refuses an export nested inside another,
	// so on a cluster whose "/" is exported the dedicated model cannot work at
	// all. Which outcome is correct therefore depends on the export table, and
	// the test asserts whichever one applies rather than assuming.
	if ancestor {
		msg := waitProvisioningFailed(t, cfg, pvcName)
		t.Logf("E3.7 provisioning was refused, as it must be:\n  %s", msg)

		for _, want := range []string{"FailedPrecondition", "NFSv4", "shared-subdirectory"} {
			if !strings.Contains(msg, want) {
				t.Errorf("E3.7 the refusal does not mention %q, so it is not actionable: %s", want, msg)
			}
		}
		if strings.Contains(msg, "already in use by an export that does not serve") {
			t.Errorf("E3.7 the refusal still blames a nonexistent conflicting export: %s", msg)
		}
		if got := exportCount(t, cfg); got != before {
			t.Errorf("E3.7 a refused provision changed the export table: %d -> %d", before, got)
		}
		t.Logf("E3.7 PROVED: dedicated-export under NFSv4 fails at provision time with an actionable reason, " +
			"and leaves the export table untouched")
		return
	}

	// No ancestor export: the dedicated model should work end to end.
	kubectlApply(t, podManifest(cfg.Namespace, podName, pvcName))
	waitPVCBound(t, cfg, pvcName)

	pvName := strings.TrimSpace(mustKubectl(t, "-n", cfg.Namespace, "get", "pvc", pvcName, "-o", "jsonpath={.spec.volumeName}"))
	attrsRaw := mustKubectl(t, "get", "pv", pvName, "-o", "jsonpath={.spec.csi.volumeAttributes}")
	var attrs map[string]string
	if err := json.Unmarshal([]byte(attrsRaw), &attrs); err != nil {
		t.Fatalf("decoding volumeAttributes: %v", err)
	}
	realPath := attrs["realPath"]

	if got := exportCount(t, cfg); got != before+1 {
		t.Errorf("E3.7 exports = %d, want %d", got, before+1)
	}
	waitPodReady(t, cfg, podName)

	mounts := mustPodExec(t, cfg, podName, "grep /data /proc/mounts")
	t.Logf("E3.7 pod mount: %s", strings.TrimSpace(mounts))
	if !strings.Contains(mounts, "vers=4.1") {
		t.Errorf("E3.7 dedicated-export mount is not v4.1: %s", strings.TrimSpace(mounts))
	}

	payload := "dedicated-" + cfg.RunID
	mustPodExec(t, cfg, podName, fmt.Sprintf("printf '%s' > /data/proof.txt", payload))
	got, err := readFileContent(t, cfg, realPath+"/proof.txt")
	if err != nil {
		t.Fatalf("E3.7 reading back: %v", err)
	}
	if string(got) != payload {
		t.Errorf("E3.7 cluster has %q, want %q", string(got), payload)
	}

	mustKubectl(t, "-n", cfg.Namespace, "delete", "pod", podName, "--force", "--grace-period=0")
	mustKubectl(t, "-n", cfg.Namespace, "delete", "pvc", pvcName)
	waitFor(t, "the dedicated export to be removed", 2*time.Minute, func() (bool, string) {
		now := exportCount(t, cfg)
		return now == before, fmt.Sprintf("exports=%d want=%d", now, before)
	})
}

// ancestorExportExists reports whether any export is an ancestor of the export
// path a per-volume export would be created at -- which is what NFSv4 refuses
// to nest under.
//
// The comparison is in **export-path space**, not filesystem space. An earlier
// version compared against StoreRealPath and so answered "no ancestor" on a
// cluster whose only export was `/qumulo-nfs41-csi` -> `/root`: the paths look
// unrelated as strings while the export is very much an ancestor of
// `/qumulo-nfs41-csi/...`. That sent the test down the wrong branch.
func ancestorExportExists(t *testing.T, cfg config) bool {
	t.Helper()

	exports, err := cfg.Client.ListExports(context.Background())
	if err != nil {
		t.Fatalf("listing exports: %v", err)
	}

	target := strings.TrimSuffix(cfg.StoreExportPath, "/") + "/"
	for _, e := range exports {
		ancestor := strings.TrimSuffix(e.ExportPath, "/") + "/"
		if strings.HasPrefix(target, ancestor) {
			return true
		}
	}
	return false
}

// waitProvisioningFailed returns the first ProvisioningFailed message for a PVC.
func waitProvisioningFailed(t *testing.T, cfg config, pvc string) string {
	t.Helper()

	var msg string
	waitFor(t, "a ProvisioningFailed event on "+pvc, 2*time.Minute, func() (bool, string) {
		out, err := kubectl(t, "-n", cfg.Namespace, "get", "events",
			"--field-selector", "involvedObject.name="+pvc,
			"-o", "jsonpath={range .items[?(@.reason=='ProvisioningFailed')]}{.message}{\"\\n\"}{end}")
		if err != nil {
			return false, "event read error"
		}
		for _, line := range strings.Split(out, "\n") {
			if strings.TrimSpace(line) != "" {
				msg = strings.TrimSpace(line)
				return true, msg
			}
		}
		return false, "no ProvisioningFailed event yet"
	})
	return msg
}

func exportCount(t *testing.T, cfg config) int {
	t.Helper()
	exports, err := cfg.Client.ListExports(context.Background())
	if err != nil {
		t.Fatalf("listing exports: %v", err)
	}
	return len(exports)
}

// ---------------------------------------------------------------------------
// E4.1 -- locking.
//
// What is observable here is bounded by the cluster having ONE node. `nolock`
// on nfs3 gives local_lock=all, which still serialises two pods sharing that
// node's kernel, so a single-node minikube cannot distinguish node-local locks
// from server-side ones. What it can prove is that mutual exclusion holds at
// all on a v4.1 mount, which is not free: v4.1 does locking in-protocol, and a
// server that refused NLM-less locking would fail here.

func TestE2ELockingNFS41(t *testing.T) {
	cfg := load(t)
	requireNFSv4(t, cfg)
	ensureNamespace(t, cfg)

	pvcName := fmt.Sprintf("e2e-lock-%s", cfg.RunID)
	podA := pvcName + "-a"
	podB := pvcName + "-b"

	t.Cleanup(func() {
		for _, p := range []string{podA, podB} {
			kubectl(t, "-n", cfg.Namespace, "delete", "pod", p, "--ignore-not-found", "--force", "--grace-period=0")
		}
		kubectl(t, "-n", cfg.Namespace, "delete", "pvc", pvcName, "--ignore-not-found")
	})

	kubectlApply(t, pvcManifest(cfg.Namespace, pvcName, "qumulo-nfs41", "1Gi", true))
	kubectlApply(t, podManifest(cfg.Namespace, podA, pvcName))
	kubectlApply(t, podManifest(cfg.Namespace, podB, pvcName))
	waitPVCBound(t, cfg, pvcName)
	waitPodReady(t, cfg, podA)
	waitPodReady(t, cfg, podB)

	mustPodExec(t, cfg, podA, "touch /data/lockfile")

	// busybox's `flock FILE -c PROG` form answers "Bad file descriptor" on
	// this image -- it treats the path as an fd -- so the lock is taken by
	// redirecting a descriptor, which is the form that works. Establish that
	// the mechanism functions at all before asserting anything with it:
	// otherwise a broken command reads as successful mutual exclusion.
	if out, err := podExec(t, cfg, podA, "exec 9>/data/lockfile; flock -n 9 && echo LOCKABLE"); err != nil ||
		!strings.Contains(out, "LOCKABLE") {
		t.Skipf("cannot take an flock on this mount at all, so contention cannot be tested: %q (%v)", strings.TrimSpace(out), err)
	}

	// Pod A takes an exclusive lock and holds it for 25s.
	mustPodExec(t, cfg, podA,
		`setsid sh -c "exec 9>/data/lockfile; flock -x 9; sleep 25" >/dev/null 2>&1 & sleep 3; echo A-holding`)

	// Pod B must not get it.
	out, err := podExec(t, cfg, podB, "exec 9>/data/lockfile; if flock -n 9; then echo ACQUIRED; else echo BLOCKED; fi")
	t.Logf("E4.1 pod B while A holds the lock: %s (err=%v)", strings.TrimSpace(out), err)
	switch {
	case strings.Contains(out, "ACQUIRED"):
		t.Errorf("E4.1 pod B acquired a lock pod A holds -- no mutual exclusion on an nfs4.1 mount")
	case strings.Contains(out, "BLOCKED"):
		t.Logf("E4.1 PROVED: mutual exclusion holds on an nfs4.1 mount (local_lock=none, so locking is in-protocol)")
	default:
		t.Errorf("E4.1 flock produced neither outcome, so nothing was tested: %q", strings.TrimSpace(out))
	}

	// And it becomes available once A is done -- a lock that is never released
	// is as broken as one that never holds.
	waitFor(t, "the lock to be released", 60*time.Second, func() (bool, string) {
		out, _ := podExec(t, cfg, podB, "exec 9>/data/lockfile; if flock -n 9; then echo ACQUIRED; else echo BLOCKED; fi")
		return strings.Contains(out, "ACQUIRED"), strings.TrimSpace(out)
	})
	t.Logf("E4.1 the lock was released and pod B acquired it")
	t.Logf("E4.1 LIMITATION: one node means local_lock=all (nfs3) also serialises; " +
		"distinguishing node-local from server-side locks needs a second node")
}

// ---------------------------------------------------------------------------
// E4.2 -- the node plugin restarts while a volume is mounted.
//
// v4.1 keeps server-side state and has a grace period; nfs3 has neither. A
// workload whose I/O never resumes after a node-plugin restart is the failure
// most likely to surprise an operator after an upgrade.

func TestE2ENodeRestartWithMountedVolume(t *testing.T) {
	cfg := load(t)
	requireNFSv4(t, cfg)
	ensureNamespace(t, cfg)

	pvcName := fmt.Sprintf("e2e-restart-%s", cfg.RunID)
	podName := pvcName + "-pod"

	t.Cleanup(func() {
		kubectl(t, "-n", cfg.Namespace, "delete", "pod", podName, "--ignore-not-found", "--force", "--grace-period=0")
		kubectl(t, "-n", cfg.Namespace, "delete", "pvc", pvcName, "--ignore-not-found")
	})

	kubectlApply(t, pvcManifest(cfg.Namespace, pvcName, "qumulo-nfs41", "1Gi", true))
	kubectlApply(t, podManifest(cfg.Namespace, podName, pvcName))
	waitPVCBound(t, cfg, pvcName)
	waitPodReady(t, cfg, podName)

	mustPodExec(t, cfg, podName, "printf 'before-restart' > /data/before.txt")
	t.Logf("E4.2 wrote before.txt; restarting the node DaemonSet pod")

	oldPod := strings.TrimSpace(mustKubectl(t, "-n", driverNamespace, "get", "pods",
		"-l", "app.kubernetes.io/component=node", "-o", "jsonpath={.items[0].metadata.name}"))
	mustKubectl(t, "-n", driverNamespace, "delete", "pod", oldPod, "--force", "--grace-period=0")

	waitFor(t, "a new node plugin pod to be Running", 3*time.Minute, func() (bool, string) {
		out, err := kubectl(t, "-n", driverNamespace, "get", "pods",
			"-l", "app.kubernetes.io/component=node", "-o", "jsonpath={.items[0].metadata.name} {.items[0].status.phase}")
		if err != nil {
			return false, "error"
		}
		fields := strings.Fields(out)
		if len(fields) < 2 {
			return false, strings.TrimSpace(out)
		}
		return fields[0] != oldPod && fields[1] == "Running", strings.TrimSpace(out)
	})
	t.Logf("E4.2 node plugin replaced; the workload pod was never restarted")

	// The mount is the kubelet's, not the plugin's, so it should have survived.
	// What matters is whether I/O still works.
	out, err := podExec(t, cfg, podName, "cat /data/before.txt && printf 'after-restart' > /data/after.txt && cat /data/after.txt")
	if err != nil {
		t.Fatalf("E4.2 I/O failed after the node plugin restarted: %v\n%s", err, out)
	}
	t.Logf("E4.2 pod I/O after the restart: %s", strings.TrimSpace(out))
	if !strings.Contains(out, "before-restart") || !strings.Contains(out, "after-restart") {
		t.Errorf("E4.2 unexpected I/O result: %q", strings.TrimSpace(out))
	}

	// And the cluster agrees.
	pvName := strings.TrimSpace(mustKubectl(t, "-n", cfg.Namespace, "get", "pvc", pvcName, "-o", "jsonpath={.spec.volumeName}"))
	attrsRaw := mustKubectl(t, "get", "pv", pvName, "-o", "jsonpath={.spec.csi.volumeAttributes}")
	var attrs map[string]string
	if err := json.Unmarshal([]byte(attrsRaw), &attrs); err != nil {
		t.Fatalf("decoding volumeAttributes: %v", err)
	}
	got, err := readFileContent(t, cfg, attrs["realPath"]+"/after.txt")
	if err != nil {
		t.Fatalf("E4.2 reading after.txt from the cluster: %v", err)
	}
	if string(got) != "after-restart" {
		t.Errorf("E4.2 cluster has %q, want %q", string(got), "after-restart")
	} else {
		t.Logf("E4.2 PROVED: a v4.1 volume survived a node-plugin restart -- writes after it reached the cluster")
	}
}

// currentHelmValues captures the release's own user-supplied values, so a
// test that uninstalls it can reinstall with exactly what was there before
// -- not a guess at what an operator's values file contained. The
// whole point is that reinstalling resumes an operator's real deployment, so
// this test must not replace it with a different one.
func currentHelmValues(t *testing.T) string {
	t.Helper()

	out, err := exec.Command("helm", "get", "values", helmReleaseName, "-n", driverNamespace, "-o", "yaml").CombinedOutput()
	if err != nil {
		t.Fatalf("helm get values %s: %v\n%s", helmReleaseName, err, out)
	}
	return string(out)
}

// helmUninstall removes the release under test.
func helmUninstall(t *testing.T) {
	t.Helper()

	out, err := exec.Command("helm", "uninstall", helmReleaseName, "-n", driverNamespace).CombinedOutput()
	if err != nil {
		t.Fatalf("helm uninstall %s: %v\n%s", helmReleaseName, err, out)
	}
}

// helmReinstall reinstalls the release from valuesYAML, the exact capture
// currentHelmValues took before uninstalling it.
func helmReinstall(t *testing.T, valuesYAML string) {
	t.Helper()

	f, err := os.CreateTemp("", "e2e-uninstall-reinstall-values-*.yaml")
	if err != nil {
		t.Fatalf("creating a temp values file: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(valuesYAML); err != nil {
		t.Fatalf("writing the temp values file: %v", err)
	}
	f.Close()

	out, err := exec.Command("helm", "upgrade", "--install", helmReleaseName, chartPath,
		"-n", driverNamespace, "-f", f.Name()).CombinedOutput()
	if err != nil {
		t.Fatalf("helm upgrade --install %s (reinstall): %v\n%s", helmReleaseName, err, out)
	}
}

// TestE2EUninstallReinstallPreservesRunningIO is the reinstall acceptance
// test, executed: uninstall the release, confirm an already-mounted pod keeps
// doing I/O and a new PVC stays Pending, reinstall with the release's own
// prior values, and confirm the pending PVC provisions and the
// already-mounted pod is still unaffected.
//
// This is the most disruptive test in this package: unlike every other test
// here, it removes and restores the actual deployed release rather than a
// scratch object it created. It captures the release's own values first and
// restores exactly those, both at the normal end of the test and from a
// t.Cleanup if an earlier step fails first -- an operator's real deployment
// must come back the way it was, not as a guess at what it might have been.
func TestE2EUninstallReinstallPreservesRunningIO(t *testing.T) {
	cfg := load(t)
	ensureNamespace(t, cfg)

	originalValues := currentHelmValues(t)
	t.Cleanup(func() {
		// Best-effort final restoration in case a step below failed before
		// its own reinstall ran -- this must never leave the cluster
		// without the driver it had before the test started.
		helmReinstall(t, originalValues)
	})

	// 1. A pod with a volume already mounted, before anything is touched.
	mountedPVC := fmt.Sprintf("e2e-uninstall-mounted-%s", cfg.RunID)
	mountedPod := mountedPVC + "-pod"
	t.Cleanup(func() {
		kubectl(t, "-n", cfg.Namespace, "delete", "pod", mountedPod, "--ignore-not-found", "--force", "--grace-period=0")
		kubectl(t, "-n", cfg.Namespace, "delete", "pvc", mountedPVC, "--ignore-not-found")
	})
	kubectlApply(t, pvcManifest(cfg.Namespace, mountedPVC, "qumulo-nfs", "1Gi", true))
	kubectlApply(t, podManifest(cfg.Namespace, mountedPod, mountedPVC))
	waitPVCBound(t, cfg, mountedPVC)
	waitPodReady(t, cfg, mountedPod)
	mustPodExec(t, cfg, mountedPod, "printf 'before-uninstall' > /data/before.txt")

	// 2. Uninstall.
	helmUninstall(t)
	waitFor(t, "the controller Deployment to be gone", time.Minute, func() (bool, string) {
		_, err := kubectl(t, "-n", driverNamespace, "get", controllerDeployment)
		return err != nil, "still present"
	})

	// 3. The already-mounted pod keeps working -- the mount is the kernel's,
	//    not the (now-deleted) driver's.
	out, err := podExec(t, cfg, mountedPod,
		"cat /data/before.txt && printf 'during-uninstall' > /data/after.txt && cat /data/after.txt")
	if err != nil {
		t.Fatalf("I/O failed on an already-mounted volume while the driver was uninstalled: %v\n%s", err, out)
	}
	if !strings.Contains(out, "before-uninstall") || !strings.Contains(out, "during-uninstall") {
		t.Errorf("unexpected I/O result during the uninstall gap: %q", strings.TrimSpace(out))
	}

	// 4. A new PVC stays Pending: no controller to provision it, and the
	//    StorageClass it names is gone along with everything else the chart
	//    rendered.
	newPVC := fmt.Sprintf("e2e-uninstall-new-%s", cfg.RunID)
	t.Cleanup(func() { kubectl(t, "-n", cfg.Namespace, "delete", "pvc", newPVC, "--ignore-not-found") })
	kubectlApply(t, pvcManifest(cfg.Namespace, newPVC, "qumulo-nfs", "1Gi", true))
	time.Sleep(15 * time.Second)
	if phase := strings.TrimSpace(mustKubectl(t, "-n", cfg.Namespace, "get", "pvc", newPVC,
		"-o", "jsonpath={.status.phase}")); phase != "Pending" {
		t.Errorf("a new PVC's phase is %q while the driver is uninstalled; want Pending", phase)
	}

	// 5. Reinstall with the exact values that were there before -- resuming
	//    service, not starting a different deployment.
	helmReinstall(t, originalValues)
	mustKubectl(t, "-n", driverNamespace, "rollout", "status", controllerDeployment, "--timeout=180s")
	mustKubectl(t, "-n", driverNamespace, "rollout", "status", nodeDaemonSet, "--timeout=180s")

	// 6. The PVC that was waiting provisions now that the controller is back.
	waitPVCBound(t, cfg, newPVC)

	// 7. The already-mounted pod was never restarted through any of this,
	//    and can still write.
	out, err = podExec(t, cfg, mountedPod,
		"cat /data/after.txt && printf 'after-reinstall' > /data/final.txt && cat /data/final.txt")
	if err != nil {
		t.Fatalf("I/O failed on the already-mounted volume after reinstall: %v\n%s", err, out)
	}
	if !strings.Contains(out, "during-uninstall") || !strings.Contains(out, "after-reinstall") {
		t.Errorf("unexpected I/O result after reinstall: %q", strings.TrimSpace(out))
	}
}

// ---------------------------------------------------------------------------

// TestE2ENonRootPodOnRestrictiveMountPermissions is the non-root acceptance test.
//
// The reported gap: the chart sets `fsGroupPolicy: None`, so the kubelet never
// applies a pod's `fsGroup`, and it creates `subPath` directories as root. A
// non-root pod therefore has nothing making its own directory writable except
// the volume's mode -- and validation's own OP-02 run passed only because
// `mountPermissions` defaults to `0777`. Tighten the class and a uid 1001 pod
// gets EACCES on a directory it supposedly owns, after the volume mounted
// cleanly and the pod started.
//
// The test runs the same pod twice, and the first run is the important one:
// without the remedy it must FAIL to write. A test that only checks the fixed
// case would pass just as happily on a `0777` volume where nothing was ever
// broken, which is precisely how this went unnoticed.
func TestE2ENonRootPodOnRestrictiveMountPermissions(t *testing.T) {
	cfg := load(t)
	ensureNamespace(t, cfg)

	className := fmt.Sprintf("e2e-restrictive-%s", cfg.RunID)
	t.Cleanup(func() {
		kubectl(t, "delete", "storageclass", className, "--ignore-not-found")
	})
	kubectlApply(t, restrictiveClassManifest(t, cfg, className))

	pvcName := fmt.Sprintf("e2e-fsgroup-%s", cfg.RunID)
	t.Cleanup(func() {
		kubectl(t, "-n", cfg.Namespace, "delete", "pvc", pvcName, "--ignore-not-found")
	})
	kubectlApply(t, pvcManifest(cfg.Namespace, pvcName, className, "1Gi", true))
	waitPVCBound(t, cfg, pvcName)

	// The volume really is 0755, or the rest of this proves nothing.
	pvName := strings.TrimSpace(mustKubectl(t, "-n", cfg.Namespace,
		"get", "pvc", pvcName, "-o", "jsonpath={.spec.volumeName}"))
	attrsRaw := mustKubectl(t, "get", "pv", pvName, "-o", "jsonpath={.spec.csi.volumeAttributes}")
	var attrs map[string]string
	if err := json.Unmarshal([]byte(attrsRaw), &attrs); err != nil {
		t.Fatalf("decoding volumeAttributes %q: %v", attrsRaw, err)
	}
	if mode, _, err := fileAttributes(t, cfg, attrs["realPath"]); err != nil {
		t.Logf("could not read the mode of %s: %v", attrs["realPath"], err)
	} else if !strings.HasSuffix(mode, "755") {
		t.Fatalf("volume directory mode is %q, want 0755 -- the class did not take effect, "+
			"so neither half of this test means anything", mode)
	}

	// --- Without the remedy: the write must fail. ---
	t.Run("a non-root subPath pod cannot write unaided", func(t *testing.T) {
		podName := pvcName + "-unaided"
		t.Cleanup(func() {
			kubectl(t, "-n", cfg.Namespace, "delete", "pod", podName,
				"--ignore-not-found", "--force", "--grace-period=0")
		})
		kubectlApply(t, nonRootSubPathPod(cfg.Namespace, podName, pvcName, "user-1001", false))
		waitPodReady(t, cfg, podName)

		out, err := podExec(t, cfg, podName, "printf 'x' > /data/probe.txt")
		if err == nil {
			t.Fatalf("a uid 1001 pod wrote to a subPath on a 0755 volume with no remedy "+
				"applied (%q). Either the kubelet stopped creating subPath directories as "+
				"root, or mountPermissions no longer takes effect -- in both cases the "+
				"guidance in the chart README is now wrong and should be revisited", out)
		}
		t.Logf("unaided write refused as expected: %v (%s)", err, strings.TrimSpace(out))
	})

	// --- With the initContainer chown: the write must succeed. ---
	t.Run("an initContainer chown makes the subPath writable", func(t *testing.T) {
		podName := pvcName + "-chowned"
		t.Cleanup(func() {
			kubectl(t, "-n", cfg.Namespace, "delete", "pod", podName,
				"--ignore-not-found", "--force", "--grace-period=0")
		})
		// A different subPath from the unaided case, so this is testing the
		// remedy rather than inheriting a directory the other pod created.
		kubectlApply(t, nonRootSubPathPod(cfg.Namespace, podName, pvcName, "user-1002", true))
		waitPodReady(t, cfg, podName)

		payload := "qcsi-018-" + cfg.RunID
		mustPodExec(t, cfg, podName, fmt.Sprintf("printf '%s' > /data/probe.txt", payload))
		got := mustPodExec(t, cfg, podName, "cat /data/probe.txt")
		if strings.TrimSpace(got) != payload {
			t.Errorf("read back %q, wrote %q", strings.TrimSpace(got), payload)
		}

		// And it is the pod's own uid that owns it, not root -- which is what
		// distinguishes the remedy working from the volume being loose.
		owner := mustPodExec(t, cfg, podName, "stat -c '%u:%g' /data/probe.txt")
		if strings.TrimSpace(owner) != "1001:1001" {
			t.Errorf("the file is owned by %q, want 1001:1001", strings.TrimSpace(owner))
		}
	})
}

// restrictiveClassManifest clones a working class with mountPermissions: 0755.
//
// Derived from a class the release already installed rather than written from
// scratch: the parameters name a cluster, a Secret and a store path this test
// has no other way to know, and hardcoding them would make the test pass or
// fail on how the lab happens to be configured.
func restrictiveClassManifest(t *testing.T, cfg config, name string) string {
	t.Helper()

	source := "qumulo-nfs41"
	if !cfg.nfsV4Enabled(t) {
		source = "qumulo-nfs3"
	}

	raw := mustKubectl(t, "get", "storageclass", source, "-o", "jsonpath={.parameters}")
	var params map[string]string
	if err := json.Unmarshal([]byte(raw), &params); err != nil {
		t.Fatalf("decoding parameters of StorageClass %s (%q): %v", source, raw, err)
	}
	params["mountPermissions"] = "0755"

	var lines []string
	for key, value := range params {
		lines = append(lines, fmt.Sprintf("  %s: %q", key, value))
	}
	sort.Strings(lines)

	return fmt.Sprintf(`apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: %s
provisioner: csi.qumulo.com
reclaimPolicy: Delete
allowVolumeExpansion: true
parameters:
%s
`, name, strings.Join(lines, "\n"))
}

// nonRootSubPathPod is the home-directory pattern: a pod running as uid 1001
// with its own subPath, optionally preceded by the initContainer chown the
// chart README documents.
//
// Both containers mount the same subPath on purpose. Without that the init
// container chowns the volume root instead of the pod's directory, which is
// the shared-tree problem the fsGroupPolicy: None default exists to avoid.
//
// The security context is per-container, exactly as the README's example
// writes it, and there is deliberately no pod-level one. A pod-level
// runAsNonRoot: true is inherited by the init container, which must run as
// root to chown anything -- the kubelet then refuses it with "container's
// runAsUser breaks non-root policy" and the pod never starts. That is what
// this fixture did until the 2026-09-20 round (F6): it timed out proving the
// remedy, while the remedy as published was fine. Scoping the contexts keeps
// the test a test of what the README actually tells people to write.
func nonRootSubPathPod(ns, name, claim, subPath string, withChown bool) string {
	initSection := ""
	if withChown {
		initSection = fmt.Sprintf(`  initContainers:
    - name: fix-ownership
      image: busybox:1.36
      # Root on purpose: it exists to hand the directory to the uid the app
      # container runs as, and nothing else can.
      securityContext:
        runAsUser: 0
      command: ["sh", "-c", "chown 1001:1001 /data && chmod 0700 /data"]
      volumeMounts:
        - name: data
          mountPath: /data
          subPath: %s
`, subPath)
	}

	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  terminationGracePeriodSeconds: 1
%s  containers:
    - name: app
      image: busybox:1.36
      # runAsNonRoot belongs here rather than on the pod: it must constrain the
      # workload without also constraining the init container above.
      securityContext:
        runAsUser: 1001
        runAsGroup: 1001
        runAsNonRoot: true
      command: ["sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
          subPath: %s
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: %s
`, name, ns, initSection, subPath, claim)
}
