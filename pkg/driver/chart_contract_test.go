//go:build chart

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"qumulo-csi-driver/pkg/qumulo"
)

// The Helm chart writes StorageClass parameters and the driver validates them,
// and nothing else checks that the two agree. Strict validation makes that gap
// expensive: a chart emitting one key the driver does not recognize fails every
// provision, and neither `helm lint` nor `go test ./...` would notice -- the
// failure appears only as a PVC stuck Pending in a real cluster.
//
// So this renders the chart for real and feeds what it produces to the real
// parameter validation. Behind the "chart" build tag because it needs helm on
// PATH; `make helm-verify` runs it and fails loudly if helm is missing, rather
// than skipping quietly.

const chartPath = "../../charts/qumulo-csi-driver"

// renderedStorageClass is the part of a StorageClass this test cares about.
type renderedStorageClass struct {
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Kind         string            `yaml:"kind"`
	Provisioner  string            `yaml:"provisioner"`
	Parameters   map[string]string `yaml:"parameters"`
	MountOptions []string          `yaml:"mountOptions"`
}

// renderDocs runs `helm template` with the given --set arguments and returns
// the raw YAML documents it produced, so each test can unmarshal only the
// shape it cares about.
func renderDocs(t *testing.T, sets ...string) []string {
	t.Helper()

	args := []string{"template", "test-release", chartPath}
	for _, set := range sets {
		args = append(args, "--set", set)
	}

	out, err := exec.Command("helm", args...).CombinedOutput()
	require.NoError(t, err, "helm template failed: %s", out)

	return strings.Split(string(out), "\n---")
}

// renderChart returns every StorageClass the chart produced.
func renderChart(t *testing.T, sets ...string) []renderedStorageClass {
	t.Helper()

	var classes []renderedStorageClass
	for _, doc := range renderDocs(t, sets...) {
		var parsed renderedStorageClass
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
			continue
		}
		if parsed.Kind == "StorageClass" {
			classes = append(classes, parsed)
		}
	}
	return classes
}

// renderedContainer is the part of a container these tests assert on.
type renderedContainer struct {
	Name            string   `yaml:"name"`
	Image           string   `yaml:"image"`
	ImagePullPolicy string   `yaml:"imagePullPolicy"`
	Args            []string `yaml:"args"`
	Ports           []struct {
		Name          string `yaml:"name"`
		ContainerPort int    `yaml:"containerPort"`
	} `yaml:"ports"`
	// A probe's port is an int or a port name, as in the Kubernetes API: the
	// drivers' probes are numeric, the sidecars' name their own ports.
	LivenessProbe *struct {
		HTTPGet *struct {
			Path string `yaml:"path"`
			Port any    `yaml:"port"`
		} `yaml:"httpGet"`
	} `yaml:"livenessProbe"`
	SecurityContext *renderedSecurityContext `yaml:"securityContext"`
}

// renderedSecurityContext is a container's securityContext. Pointers
// throughout, so an absent field is distinguishable from an explicit false --
// which is the whole question for runAsNonRoot.
type renderedSecurityContext struct {
	RunAsNonRoot             *bool `yaml:"runAsNonRoot"`
	RunAsUser                *int  `yaml:"runAsUser"`
	Privileged               *bool `yaml:"privileged"`
	AllowPrivilegeEscalation *bool `yaml:"allowPrivilegeEscalation"`
	ReadOnlyRootFilesystem   *bool `yaml:"readOnlyRootFilesystem"`
	Capabilities             *struct {
		Drop []string `yaml:"drop"`
	} `yaml:"capabilities"`
}

// renderedWorkload covers both the controller Deployment and the node
// DaemonSet, whose pod specs sit at the same path.
type renderedWorkload struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		// Selector is the workload's own pod selector. A metrics Service has
		// to select the same pods, and a Service selecting nothing is the
		// failure that looks exactly like a driver exporting nothing.
		Selector struct {
			MatchLabels map[string]string `yaml:"matchLabels"`
		} `yaml:"selector"`
		Template struct {
			Metadata struct {
				Labels map[string]string `yaml:"labels"`
			} `yaml:"metadata"`
			Spec struct {
				Containers      []renderedContainer `yaml:"containers"`
				SecurityContext *struct {
					SeccompProfile *struct {
						Type string `yaml:"type"`
					} `yaml:"seccompProfile"`
				} `yaml:"securityContext"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

// renderedService is a Service: the selector it points at, and the ports it
// exposes. A Service whose selector matches no pod is the failure that looks
// exactly like a driver exporting nothing, so the selector is asserted.
type renderedService struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name        string            `yaml:"name"`
		Labels      map[string]string `yaml:"labels"`
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	Spec struct {
		ClusterIP string            `yaml:"clusterIP"`
		Selector  map[string]string `yaml:"selector"`
		Ports     []struct {
			Name       string `yaml:"name"`
			Port       int    `yaml:"port"`
			TargetPort int    `yaml:"targetPort"`
		} `yaml:"ports"`
	} `yaml:"spec"`
}

// renderedMonitoring is a ServiceMonitor or a PrometheusRule. They share a
// struct because both are gated on the same CRD and both are asserted for
// the same thing: that they name ports and series that actually exist.
type renderedMonitoring struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name   string            `yaml:"name"`
		Labels map[string]string `yaml:"labels"`
	} `yaml:"metadata"`
	Spec struct {
		Selector struct {
			MatchLabels map[string]string `yaml:"matchLabels"`
		} `yaml:"selector"`
		Endpoints []struct {
			Port string `yaml:"port"`
			Path string `yaml:"path"`
		} `yaml:"endpoints"`
		Groups []struct {
			Name  string `yaml:"name"`
			Rules []struct {
				Alert string `yaml:"alert"`
				Expr  string `yaml:"expr"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	} `yaml:"spec"`
}

// renderedRBAC is a ClusterRole or Role with its rules.
type renderedRBAC struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Rules []struct {
		APIGroups []string `yaml:"apiGroups"`
		Resources []string `yaml:"resources"`
		Verbs     []string `yaml:"verbs"`
	} `yaml:"rules"`
}

// renderWorkloads returns the chart's Deployment and DaemonSet.
func renderWorkloads(t *testing.T, sets ...string) []renderedWorkload {
	t.Helper()

	var out []renderedWorkload
	for _, doc := range renderDocs(t, sets...) {
		var parsed renderedWorkload
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
			continue
		}
		if parsed.Kind == "Deployment" || parsed.Kind == "DaemonSet" {
			out = append(out, parsed)
		}
	}
	return out
}

// driverContainerIn returns the container running this driver's own binary.
func driverContainerIn(t *testing.T, w renderedWorkload) renderedContainer {
	t.Helper()

	for _, c := range w.Spec.Template.Spec.Containers {
		if c.Name == "qumulo-csi-driver" {
			return c
		}
	}
	t.Fatalf("%s %q has no qumulo-csi-driver container", w.Kind, w.Metadata.Name)
	return renderedContainer{}
}

// baseSets is the minimum needed to render, plus one StorageClass exercising
// every parameter the chart knows how to emit.
func baseSets(extra ...string) []string {
	// Every class must carry its own server and Secret: there is no
	// cluster-wide address or token left to fall back to.
	sets := []string{
		"storageClasses[0].name=qumulo-nfs",
		"storageClasses[0].server=" + testServer,
		"storageClasses[0].secretName=qumulo-token",
		"storageClasses[0].basePath=/csi/volumes",
	}
	return append(sets, extra...)
}

// TestChartParametersAreAcceptedByTheDriver is the contract: every parameter
// the chart writes must be one the driver recognizes and accepts.
func TestChartParametersAreAcceptedByTheDriver(t *testing.T) {
	variants := []struct {
		name string
		sets []string
	}{
		{name: "defaults", sets: baseSets()},
		{name: "protocol nfs3", sets: baseSets("storageClasses[0].protocol=nfs3")},
		{name: "protocol nfs4.1", sets: baseSets("storageClasses[0].protocol=nfs4\\.1")},
		{
			// What the chart renders, the driver must accept -- including the
			// comma-separated modifiers, which a Helm --set has to escape and
			// a template could mangle.
			name: "snapshotSchedule",
			sets: baseSets("storageClasses[0].snapshotSchedule=daily@02:00\\,keep=7days\\,tz=UTC"),
		},
		{
			name: "snapshotSchedule, weekly form with its own comma",
			sets: baseSets("storageClasses[0].snapshotSchedule=weekly@SUN\\,02:00\\,keep=30days"),
		},
		{
			// A boolean the driver reads as the string "true".
			name: "allowForeignSnapshotRestore",
			sets: baseSets("storageClasses[0].allowForeignSnapshotRestore=true"),
		},

		{
			// The path-namespacing parameter. A dot-separated domain is
			// exactly the shape a Helm --set is most likely to mangle.
			//
			// The chart deliberately does not validate the format -- the
			// driver does, so a bad domain fails identically whether it came
			// from the chart or a hand-written StorageClass. That is what
			// TestChartRendersAnInvalidADDomainForTheDriverToRefuse checks.
			name: "adDomain",
			sets: baseSets("storageClasses[0].adDomain=corp\\.example\\.com"),
		},

		{
			name: "dedicated export",
			sets: baseSets("storageClasses[0].provisioningModel=dedicated-export", "storageClasses[0].allowAnyClient=true"),
		},
		{
			name: "separate export base path",
			sets: baseSets("storageClasses[0].exportBasePath=/exports/csi"),
		},
		{
			name: "explicit server distinct from the management endpoint",
			sets: baseSets("storageClasses[0].server=nfs-floating.example.com"),
		},
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			classes := renderChart(t, variant.sets...)
			require.Len(t, classes, 1, "expected exactly one StorageClass")
			sc := classes[0]

			assert.Equal(t, "csi.qumulo.com", sc.Provisioner,
				"the chart's provisioner must be the driver's name")

			// The parameters the chart wrote, through the driver's own
			// validation -- the same code path CreateVolume uses.
			parsed, err := parseVolumeParams(sc.Parameters)
			require.NoError(t, err,
				"the driver rejected parameters the chart produced: %v", sc.Parameters)

			assert.NotEmpty(t, parsed.Server)
			assert.NotEmpty(t, parsed.StoreRealPath)
			assert.NotEmpty(t, parsed.StoreExportPath)
		})
	}
}

// A class the chart renders must provision through a real controller. The chart
// used to also emit qumuloClusterAddress so a class aimed at another cluster was
// refused; multi-cluster removed both the parameter and the refusal, so what is
// left to assert is that the rendered parameter set is still one the driver
// accepts end to end.
func TestChartRenderedClassProvisions(t *testing.T) {
	classes := renderChart(t, baseSets()...)
	require.Len(t, classes, 1)

	// The cluster is given the export the rendered class needs, derived from
	// that class's own parameters rather than hardcoded: a shared-model volume
	// no export covers is refused, so provisioning here has to start from a
	// cluster where the administrator met the chart's documented
	// prerequisite. Deriving it keeps this test honest if the chart's default
	// paths change.
	parsed, err := parseVolumeParams(classes[0].Parameters)
	require.NoError(t, err)

	srv := newTestControllerServer(t, &MockClient{
		CreateDirectoryFunc: func(ctx context.Context, parentPath, name string) (qumulo.FileInfo, error) {
			return qumulo.FileInfo{ID: "12884901888", Type: "FS_FILE_TYPE_DIRECTORY"}, nil
		},
		ListExportsFunc: exportsCovering(parsed.StoreExportPath, parsed.StoreRealPath),
	})

	_, err = srv.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Secrets:            testSecrets(),
		Name:               "pvc-chart",
		VolumeCapabilities: []*csi.VolumeCapability{mountVolumeCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         classes[0].Parameters,
	})
	require.NoError(t, err, "a class the chart rendered must provision against the controller it deploys with")
}

// The chart must refuse, at install time, whatever the driver would refuse at
// provision time: a PVC stuck Pending is a much worse message than a failed
// install.
//
// Two layers can do the refusing -- values.schema.json for anything expressible
// as a type or enum, and the fail calls in _validate.tpl for the cross-field
// rules a schema cannot state. Which one fires is an implementation detail and
// the schema is the earlier of the two, so these assertions check what actually
// matters: the render is refused, and the message names the field at fault.
func TestChartRefusesWhatTheDriverRefuses(t *testing.T) {
	tests := []struct {
		name string
		sets []string
		// wantErr is matched against the output; it names the field rather
		// than a whole sentence, so a check moving between the schema and the
		// templates does not break the test.
		wantErr string
	}{
		{
			// Supplying a CA and then telling the driver to ignore it is
			// always a mistake, so the chart refuses rather than rendering a
			// deployment whose CA mount does nothing.
			name: "CA bundle with verification disabled",
			sets: append(baseSets(),
				"qumulo.caBundle.existingConfigMap=qumulo-ca",
				"qumulo.insecureSkipTlsVerify=true"),
			wantErr: "caBundle",
		},
		{
			name: "CA bundle with an empty key",
			sets: append(baseSets(),
				"qumulo.caBundle.existingConfigMap=qumulo-ca",
				"qumulo.caBundle.key="),
			wantErr: "caBundle.key",
		},

		{
			name:    "unsupported protocol",
			sets:    baseSets("storageClasses[0].protocol=nfs4\\.2"),
			wantErr: "protocol",
		},
		{
			name:    "unsupported provisioning model",
			sets:    baseSets("storageClasses[0].provisioningModel=magic"),
			wantErr: "provisioningModel",
		},
		{
			name:    "relative basePath",
			sets:    baseSets("storageClasses[0].basePath=relative/path"),
			wantErr: "basePath",
		},
		{
			// The driver rejects this at NodePublishVolume, by which point a
			// pod is already stuck mounting.
			name: "version pinned twice",
			sets: baseSets(
				"storageClasses[0].protocol=nfs3",
				"storageClasses[0].mountOptions[0]=nfsvers=4\\.1",
			),
			wantErr: "must not also pin the version",
		},
		{
			// The purge's central rule: with no cluster-wide address, a class
			// that names no server can never be provisioned, so it must fail
			// while somebody is looking at the values file.
			name: "a class with no server",
			sets: []string{
				"storageClasses[0].name=qumulo-nfs",
				"storageClasses[0].secretName=qumulo-token",
				"storageClasses[0].basePath=/csi/volumes",
			},
			wantErr: "server",
		},
		{
			// An nfs3 class asking for server-side locking cannot work: the
			// node image runs no rpc.statd. The driver refuses it at
			// NodePublish, by which point the PVC is bound and a pod is stuck,
			// so the chart refuses it at install.
			name:    "nfs3 asking for server-side locking",
			sets:    baseSets("storageClasses[0].protocol=nfs3", "storageClasses[0].mountOptions[0]=lock"),
			wantErr: "rpc.statd",
		},
		{
			// mount-utils joins entries with commas, so one entry can
			// hold several options, and each is checked on its own.
			name:    "nfs3 asking for locking inside a comma-joined entry",
			sets:    baseSets("storageClasses[0].protocol=nfs3", `storageClasses[0].mountOptions[0]=hard\,lock`),
			wantErr: "rpc.statd",
		},
		{
			name: "version pinned twice inside a comma-joined entry",
			sets: baseSets(
				"storageClasses[0].protocol=nfs3",
				`storageClasses[0].mountOptions[0]=hard\,vers=4\.1`,
			),
			wantErr: "must not also pin the version",
		},
		{
			// server is also the first field of the volume handle, so these
			// three are handle-integrity failures, not just bad mounts: each
			// one would Bind a PVC whose PV the driver could never delete or
			// expand. Kept next to the driver's own rejections in
			// TestChartAndDriverAgreeOnServerShape.
			name:    "server given as a REST URL",
			sets:    baseSets(`storageClasses[0].server=https://10\.0\.0\.1:8000`),
			wantErr: "not a URL",
		},
		{
			name:    "server carrying a port",
			sets:    baseSets(`storageClasses[0].server=qumulo\.example\.com:8000`),
			wantErr: "server must not contain",
		},
		{
			name:    "server carrying a path",
			sets:    baseSets(`storageClasses[0].server=qumulo\.example\.com/csi`),
			wantErr: "server must not contain",
		},
		{
			name:    "server that is not a hostname",
			sets:    baseSets("storageClasses[0].server=-nope"),
			wantErr: "server must be a hostname",
		},
		{
			// Likewise the credential: every request must carry one.
			name: "a class with no secretName",
			sets: []string{
				"storageClasses[0].name=qumulo-nfs",
				"storageClasses[0].server=" + testServer,
				"storageClasses[0].basePath=/csi/volumes",
			},
			wantErr: "secretName",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []string{"template", "test-release", chartPath}
			for _, set := range tt.sets {
				args = append(args, "--set", set)
			}

			out, err := exec.Command("helm", args...).CombinedOutput()

			require.Error(t, err, "helm should have refused to render: %s", out)
			assert.Contains(t, string(out), tt.wantErr)
		})
	}
}

// Every document the chart renders must be a well-formed Kubernetes object.
//
// This is the check that a live cluster would do, and cannot be done here:
// kubectl needs an API server to fetch the OpenAPI schema, so it cannot
// validate offline. What is verifiable without one is that each document parses
// and carries the fields the API server keys on -- which catches the failure
// mode templating actually produces, namely an indentation slip that nests a
// field under the wrong parent.
func TestChartRendersWellFormedObjects(t *testing.T) {
	args := append([]string{"template", "test-release", chartPath}, setArgs(baseSets(
		"storageClasses[0].protocol=nfs4\\.1",
		"storageClasses[0].provisioningModel=dedicated-export",
		"storageClasses[0].allowAnyClient=true",
	))...)

	out, err := exec.Command("helm", args...).CombinedOutput()
	require.NoError(t, err, "helm template failed: %s", out)

	type object struct {
		APIVersion string `yaml:"apiVersion"`
		Kind       string `yaml:"kind"`
		Metadata   struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
	}

	seen := map[string]int{}
	for _, doc := range strings.Split(string(out), "\n---") {
		if strings.TrimSpace(stripComments(doc)) == "" {
			continue
		}

		var parsed object
		require.NoError(t, yaml.Unmarshal([]byte(doc), &parsed),
			"a rendered document is not valid YAML:\n%s", doc)

		// A document that renders to nothing but comments is fine; one that
		// renders to content without these is a templating mistake.
		require.NotEmpty(t, parsed.Kind, "document has no kind:\n%s", doc)
		require.NotEmpty(t, parsed.APIVersion, "%s has no apiVersion", parsed.Kind)
		require.NotEmpty(t, parsed.Metadata.Name, "%s has no metadata.name", parsed.Kind)

		seen[parsed.Kind]++
	}

	// The workload set is the contract with the cluster: two images, two
	// workloads, and the objects that let them run.
	for kind, want := range map[string]int{
		"CSIDriver":  1,
		"Deployment": 1,
		"DaemonSet":  1,
		// Controller and node, separately, plus the read-only identity of the
		// `helm test` readiness check (tests.ready.enabled, on by default).
		"ServiceAccount":     3,
		"ClusterRole":        2, // the controller's; the readiness check's csinodes list
		"ClusterRoleBinding": 2,
		// No Secret: the chart no longer manages a credential. Each cluster's
		// token is a Secret the operator creates and a StorageClass names.
		"Secret":       0,
		"StorageClass": 1,
	} {
		assert.Equal(t, want, seen[kind], "unexpected number of %s objects", kind)
	}
}

// setArgs turns --set values into the flag pairs exec.Command needs.
func setArgs(sets []string) []string {
	args := make([]string, 0, len(sets)*2)
	for _, set := range sets {
		args = append(args, "--set", set)
	}
	return args
}

// stripComments removes whole-line comments so a document containing nothing
// else is recognized as empty rather than parsed as an object.
func stripComments(doc string) string {
	var kept []string
	for _, line := range strings.Split(doc, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// The chart passes adDomain through unvalidated, on purpose: one validator, in
// the driver, so the same bad value fails the same way however it arrived.
// This is the half of that contract the chart is responsible for -- it must
// render the value rather than silently drop it, so the driver gets a chance
// to refuse it.
func TestChartRendersAnInvalidADDomainForTheDriverToRefuse(t *testing.T) {
	// renderChart fails the test if helm refuses, which is the first half of
	// the assertion: the chart must not reject this value itself.
	classes := renderChart(t, baseSets("storageClasses[0].adDomain=not-a-domain")...)
	require.Len(t, classes, 1)
	require.Equal(t, "not-a-domain", classes[0].Parameters["adDomain"],
		"the value must reach the driver verbatim")

	_, err := parseVolumeParams(classes[0].Parameters)
	require.Error(t, err, "the driver must be the one to refuse it")
	require.Contains(t, err.Error(), paramADDomain)
}

// The chart's driver arguments must be ones the driver's own flag parser
// accepts.
//
// This is the args analogue of the parameter contract above, and it closes the
// same class of gap: the chart passes flags, the binary defines them, and
// nothing checked that the two agree. A flag the chart emits but the binary
// does not define makes the container exit 2 at startup -- a crash-loop in a
// real cluster, and invisible to `helm lint`, `go test ./...` and the golden
// files alike, since all three are happy with any string.
//
// The real parser is used rather than a list of known flags kept here, because
// a list would be one more thing to drift. parseFlags lives in package main,
// so the binary itself is the parser: `-version` returns immediately after
// parsing and touches nothing else, so this exercises exactly the flag
// handling and nothing more.
func TestChartDriverArgsAreAcceptedByTheDriver(t *testing.T) {
	for _, w := range renderWorkloads(t, baseSets()...) {
		t.Run(w.Kind, func(t *testing.T) {
			container := driverContainerIn(t, w)
			require.NotEmpty(t, container.Args, "the driver container must be given explicit args")

			args := append([]string{"run", "../../cmd/qumulo-csi-driver"}, container.Args...)
			args = append(args, "-version")

			out, err := exec.Command("go", args...).CombinedOutput()
			require.NoError(t, err,
				"the driver rejected the args this chart passes it; it would crash-loop with these:\n  %s\n%s",
				strings.Join(container.Args, "\n  "), out)
		})
	}
}

// Each workload must pin the driver to its own mode.
//
// The binary's default is `all`, which serves the controller and the node in
// one process. That exists for local development and csi-sanity; a node
// DaemonSet inheriting it would run a controller on every worker, and put
// cluster-wide API credentials there with it. The images set their own CMD for
// this reason, and the chart overrides CMD with args, so the chart has to
// carry the mode itself.
func TestChartPinsEachWorkloadToItsMode(t *testing.T) {
	wantMode := map[string]string{
		"Deployment": "-mode=controller",
		"DaemonSet":  "-mode=node",
	}

	seen := map[string]bool{}
	for _, w := range renderWorkloads(t, baseSets()...) {
		want, ok := wantMode[w.Kind]
		require.True(t, ok, "unexpected workload kind %q", w.Kind)
		seen[w.Kind] = true

		args := driverContainerIn(t, w).Args
		assert.Contains(t, args, want,
			"%s must pin the driver's mode explicitly, never inherit the `all` default", w.Kind)
	}
	assert.Len(t, seen, 2, "both the controller Deployment and the node DaemonSet must be rendered")
}

// The node needs its node ID from the API's view of the node, not the
// container's hostname, so the DaemonSet must pass -nodeid.
//
// Without it the driver in node mode refuses to start, because the CO matches
// on the node's own name and a container hostname is not reliably that.
func TestChartGivesTheNodeItsNodeID(t *testing.T) {
	for _, w := range renderWorkloads(t, baseSets()...) {
		if w.Kind != "DaemonSet" {
			continue
		}
		args := driverContainerIn(t, w).Args
		var found bool
		for _, a := range args {
			if strings.HasPrefix(a, "-nodeid=") {
				found = true
				assert.NotEqual(t, "-nodeid=", a, "-nodeid must not be empty")
			}
		}
		assert.True(t, found, "the node DaemonSet must pass -nodeid; the driver refuses node mode without one")
	}
}

// Every image must be pinned to an explicit version, and every sidecar by
// digest as well.
//
// Guarded here rather than only in the golden files, which `make
// helm-golden-update` rewrites wholesale -- so an image that silently became
// floating would be regenerated into the goldens and reviewed as noise.
//
// A sidecar tag alone is a label its registry can move; the digest names the
// bytes the chart was tested with. So the chart's sidecar defaults are
// "<name>:<tag>@sha256:<64 hex>" -- the tag for the reader, the digest for
// the kubelet. The driver's own images are exempt: their tag is the chart's
// appVersion, chosen per build, and a digest is the user's to add.
func TestChartPinsEveryImage(t *testing.T) {
	// The opt-in sidecars are rendered too: an unpinned image on a sidecar
	// nobody enabled by default is still an unpinned image.
	sets := baseSets(
		"sidecars.snapshotter.enabled=true",
		"sidecars.csiAddons.enabled=true",
		"sidecars.healthMonitor.enabled=true",
		"controller.inventory.enabled=true",
	)

	// Named rather than counted. A count passes when one container is
	// replaced by another, and asserting a number is the same weakness this
	// test exists to remove from the RBAC checks.
	want := map[string]bool{
		"Deployment/csi-provisioner":                        false,
		"Deployment/csi-resizer":                            false,
		"Deployment/csi-snapshotter":                        false,
		"Deployment/csi-addons":                             false,
		"Deployment/csi-external-health-monitor-controller": false,
		"Deployment/liveness-probe":                         false,
		"Deployment/qumulo-csi-driver":                      false,
		"DaemonSet/node-driver-registrar":                   false,
		"DaemonSet/liveness-probe":                          false,
		"DaemonSet/qumulo-csi-driver":                       false,
	}
	// An upstream release tag, or -- only for an image we rebuild and publish
	// ourselves on ghcr.io/qumulo (build/csi-addons-sidecar/) -- that tag
	// plus a -qumulo.N suffix. The digest is required either way.
	sidecarPin := regexp.MustCompile(`^([^\s@]+:v[0-9]+\.[0-9]+\.[0-9]+|ghcr\.io/qumulo/[^\s@:]+:v[0-9]+\.[0-9]+\.[0-9]+-qumulo\.[0-9]+)@sha256:[a-f0-9]{64}$`)

	for _, w := range renderWorkloads(t, sets...) {
		for _, c := range w.Spec.Template.Spec.Containers {
			name := w.Kind + "/" + c.Name
			_, expected := want[name]
			assert.True(t, expected, "%s is rendered but not in this test's expected set", name)
			want[name] = true

			if c.Name != "qumulo-csi-driver" {
				assert.Regexp(t, sidecarPin, c.Image,
					"%s: a sidecar's default image must be <name>:vX.Y.Z@sha256:<digest> -- a tag alone can be moved", name)
				continue
			}

			// A digest pin is the strongest form and needs no tag check.
			if strings.Contains(c.Image, "@sha256:") {
				continue
			}

			tag := c.Image[strings.LastIndex(c.Image, ":")+1:]
			require.Contains(t, c.Image, ":", "%s: image %q carries no tag, so it resolves to :latest", name, c.Image)
			assert.NotEqual(t, "latest", tag, "%s: image %q is floating", name, c.Image)
			assert.NotEmpty(t, tag, "%s: image %q has an empty tag", name, c.Image)
		}
	}

	// Absence would otherwise pass this test trivially: a container that
	// stopped rendering has no image to find unpinned.
	for name, rendered := range want {
		assert.True(t, rendered, "%s was expected but is not rendered at all", name)
	}
}

// A sidecar image a user sets must still be a pin: the schema refuses one
// with neither a tag nor a digest, and _validate.tpl the tag "latest". A
// tag without a digest stays allowed -- a mirror that copies one platform
// has a digest of its own, and a tag there is still a version.
func TestChartRefusesAFloatingSidecarImage(t *testing.T) {
	render := func(extra ...string) (string, error) {
		args := []string{"template", "test-release", chartPath}
		for _, set := range baseSets(extra...) {
			args = append(args, "--set", set)
		}
		out, err := exec.Command("helm", args...).CombinedOutput()
		return string(out), err
	}
	digest := "@sha256:" + strings.Repeat("a", 64)

	for _, tc := range []struct {
		image string
		want  string
	}{
		{"example.com/csi-provisioner", "does not match pattern"},
		{"registry.example.com:5000/csi-provisioner", "does not match pattern"},
		{"example.com/csi-provisioner:", "does not match pattern"},
		{"example.com/csi-provisioner:v5.3.0@sha256:abc", "does not match pattern"},
		{"example.com/csi-provisioner:latest", "the tag latest is not a pin"},
		{"example.com/csi-provisioner:latest" + digest, "the tag latest is not a pin"},
	} {
		t.Run("refuses "+tc.image, func(t *testing.T) {
			out, err := render("sidecars.provisioner.image=" + tc.image)
			require.Error(t, err, "must be refused at render: %s", out)
			assert.Contains(t, out, tc.want)
		})
	}

	// Every sidecar is checked, enabled or not: switching one on later must
	// not be what first runs an unpinned image.
	for _, sidecar := range []string{"provisioner", "resizer", "livenessProbe", "nodeDriverRegistrar", "snapshotter", "csiAddons", "healthMonitor"} {
		out, err := render("sidecars." + sidecar + ".image=example.com/x:latest")
		require.Error(t, err, "sidecars.%s.image latest must be refused: %s", sidecar, out)
		assert.Contains(t, out, "sidecars."+sidecar+".image")
	}

	for _, image := range []string{
		"example.com/csi-provisioner:v5.3.0",
		"registry.example.com:5000/mirror/csi-provisioner:v5.3.0",
		"registry.example.com:5000/csi-provisioner:v5.3.0" + digest,
		"example.com/csi-provisioner" + digest,
		"example.com/csi-provisioner:latest-ubi9",
	} {
		out, err := render("sidecars.provisioner.image=" + image)
		assert.NoError(t, err, "%s must be accepted: %s", image, out)
	}
}

// The driver images' pull policy must follow what the tag promises.
//
// A bare release tag ("0.1.0") is moved by every publish. Pulled
// IfNotPresent, a node that cached an earlier 0.1.0 kept running it under a
// newer chart: at one point the 0.1.0 on ghcr.io was an older build that exits
// on the -registration-marker flag the chart now passes, so the documented
// install crash-looped on any node. A build tag ("0.1.0-<commit>", what a
// packaged chart defaults to) or a digest names one image for good, and
// pulling it every pod start would only add a registry dependency.
func TestChartDriverPullPolicyFollowsTheTag(t *testing.T) {
	cases := []struct {
		name string
		sets []string
		want string
	}{
		{"default tag is the moving release tag", nil, "Always"},
		{"explicit release tag", []string{"image.controller.tag=0.1.0", "image.node.tag=0.1.0"}, "Always"},
		{"build tag", []string{"image.controller.tag=0.1.0-9994a48", "image.node.tag=0.1.0-9994a48"}, "IfNotPresent"},
		{"digest", []string{"image.controller.tag=0.1.0@sha256:" + strings.Repeat("a", 64), "image.node.tag=0.1.0@sha256:" + strings.Repeat("a", 64)}, "IfNotPresent"},
		{"explicit policy wins over a release tag", []string{"image.controller.pullPolicy=Never", "image.node.pullPolicy=Never"}, "Never"},
		{"explicit policy wins over a build tag", []string{"image.controller.tag=0.1.0-9994a48", "image.node.tag=0.1.0-9994a48", "image.controller.pullPolicy=Always", "image.node.pullPolicy=Always"}, "Always"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workloads := renderWorkloads(t, baseSets(tc.sets...)...)
			require.Len(t, workloads, 2, "expected the controller Deployment and the node DaemonSet")
			for _, w := range workloads {
				c := driverContainerIn(t, w)
				assert.Equal(t, tc.want, c.ImagePullPolicy, "%s: image %s", w.Kind, c.Image)
			}
		})
	}
}

// A wedged driver must be restarted by the kubelet, which means something has
// to actually probe the liveness endpoint.
//
// The livenessprobe sidecar calls the CSI Identity.Probe RPC over the shared
// socket and serves the result over HTTP -- but serving it achieves nothing
// unless a container declares a livenessProbe against it. The chart declared
// none at all: both sidecars ran, both served /healthz, and nothing ever read
// it, so the sidecars were pure overhead and a hung driver was never
// restarted. This asserts the probe exists and that its port is the one the
// sidecar was told to listen on, since a probe pointed at the wrong port is
// the same defect wearing a disguise.
func TestChartProbesTheLivenessEndpoint(t *testing.T) {
	for _, w := range renderWorkloads(t, baseSets()...) {
		t.Run(w.Kind, func(t *testing.T) {
			var sidecarPort int
			for _, c := range w.Spec.Template.Spec.Containers {
				if c.Name != "liveness-probe" {
					continue
				}
				for _, a := range c.Args {
					// ":<port>" in the controller; "$(POD_IP):<port>" in the
					// node, which is hostNetwork and binds the node's own
					// address only.
					if endpoint, ok := strings.CutPrefix(a, "--http-endpoint="); ok {
						port, err := strconv.Atoi(endpoint[strings.LastIndex(endpoint, ":")+1:])
						require.NoError(t, err, "unparseable --http-endpoint %q", a)
						sidecarPort = port
						if w.Kind == "DaemonSet" {
							assert.True(t, strings.HasPrefix(endpoint, "$(POD_IP):"),
								"the node's /healthz is a host port and must bind the node's address, not every interface: %q", a)
						}
					}
				}
			}
			require.NotZero(t, sidecarPort, "the liveness-probe sidecar must be told a port to serve on")

			probe := driverContainerIn(t, w).LivenessProbe
			require.NotNil(t, probe, "the driver container declares no livenessProbe, so a hung driver is never restarted")
			require.NotNil(t, probe.HTTPGet, "the livenessProbe must be an httpGet against the sidecar")
			assert.Equal(t, "/healthz", probe.HTTPGet.Path)
			assert.Equal(t, sidecarPort, probe.HTTPGet.Port,
				"the probe must target the port the liveness-probe sidecar actually serves on")
		})
	}
}

// Every sidecar that talks to the driver's socket must be able to open it.
//
// The test above asserts the probe exists and points at the port the sidecar
// was told to serve on. Both were true, and the node plugin still restarted
// every ~2.5 minutes for as long as it was deployed: the sidecar never opened
// that port at all, because it could not connect to the socket in the first
// place. Nothing checked the one step in between, so this does.
//
// The rule is a filesystem one. The driver creates /csi/csi.sock and it is
// owned by whatever uid the driver runs as, mode 0755. Connecting to a unix
// socket requires the *write* bit, so a client is only able to open it as uid 0
// (which bypasses the check) or as the socket's own owner. That makes the two
// workloads' arrangements correct for opposite reasons -- the controller's
// sidecars match its non-root driver, and the node's must be root because its
// driver is -- and it keeps holding if the node driver is ever made non-root,
// which is why this asserts the relationship instead of a pair of uids.
//
// A container is a client of the socket if it was passed --csi-address; that is
// how every sig-storage sidecar is told where to connect.
func TestChartCSISocketClientsCanConnectToTheSocket(t *testing.T) {
	// effectiveUID is the uid a container actually runs as: runAsUser when the
	// chart sets one, and otherwise root, since every image here either
	// declares USER 0 or none at all.
	effectiveUID := func(c renderedContainer) int {
		if c.SecurityContext == nil || c.SecurityContext.RunAsUser == nil {
			return 0
		}
		return *c.SecurityContext.RunAsUser
	}

	for _, w := range renderWorkloads(t, baseSets("sidecars.snapshotter.enabled=true")...) {
		t.Run(w.Kind, func(t *testing.T) {
			driver := driverContainerIn(t, w)
			ownerUID := effectiveUID(driver)

			clients := 0
			for _, c := range w.Spec.Template.Spec.Containers {
				if c.Name == driver.Name {
					continue
				}
				isClient := false
				for _, a := range c.Args {
					if strings.HasPrefix(a, "--csi-address=") {
						isClient = true
					}
				}
				if !isClient {
					continue
				}
				clients++

				uid := effectiveUID(c)
				assert.True(t, uid == 0 || uid == ownerUID,
					"%s/%s connects to the driver's socket as uid %d, but the socket is owned by "+
						"uid %d at mode 0755 -- it can only be opened as uid 0 or as %d, so this "+
						"container will sit in \"Still connecting\" forever",
					w.Kind, c.Name, uid, ownerUID, ownerUID)
			}

			// Guards the test itself: if --csi-address were renamed, every
			// assertion above would vacuously pass.
			assert.NotZero(t, clients, "%s renders no sidecar with --csi-address, so this test checked nothing", w.Kind)
		})
	}
}

// The sidecars' RBAC must grant the verbs they actually use.
//
// Checked as (apiGroup, resource, verb) triples rather than by counting rules.
// A count passes when a rule is present but its verbs have been narrowed, and
// the failure that produces is remote from its cause: the provisioner cannot
// create PVs, so PVCs sit Pending with an error in a sidecar's log rather than
// anywhere an operator looks first.
//
// The list is what the deployed sidecar versions require, so it is a statement
// of the contract rather than a copy of the template: a rule deleted from
// rbac.yaml fails here, and a rule added does not.
func TestChartRBACGrantsTheVerbsTheSidecarsNeed(t *testing.T) {
	required := []struct {
		apiGroup string
		resource string
		verbs    []string
	}{
		// csi-provisioner
		{"", "persistentvolumes", []string{"get", "list", "watch", "create", "delete"}},
		{"", "persistentvolumeclaims", []string{"get", "list", "watch", "update"}},
		{"storage.k8s.io", "storageclasses", []string{"get", "list", "watch"}},
		{"", "events", []string{"list", "watch", "create", "update", "patch"}},
		{"storage.k8s.io", "csinodes", []string{"get", "list", "watch"}},
		{"", "nodes", []string{"get", "list", "watch"}},
		// The per-request credential: read-only, and only because the Secret
		// a StorageClass names may live in any namespace.
		{"", "secrets", []string{"get"}},
		// csi-resizer
		{"", "persistentvolumeclaims/status", []string{"patch"}},
		// csi-snapshotter
		{"snapshot.storage.k8s.io", "volumesnapshots", []string{"get", "list"}},
		{"snapshot.storage.k8s.io", "volumesnapshotclasses", []string{"get", "list", "watch"}},
		{"snapshot.storage.k8s.io", "volumesnapshotcontents", []string{"get", "list", "watch", "update", "patch"}},
		{"snapshot.storage.k8s.io", "volumesnapshotcontents/status", []string{"update", "patch"}},
		// Leader election, which every controller sidecar here enables.
		{"coordination.k8s.io", "leases", []string{"get", "watch", "list", "create", "update"}},
	}

	// Flattened across every ClusterRole and Role the chart renders: which
	// role carries a grant is an implementation detail, that it is granted at
	// all is the contract.
	granted := map[string]map[string]bool{}
	var roles int
	for _, doc := range renderDocs(t, baseSets("sidecars.snapshotter.enabled=true", "volumeSnapshotClasses[0].name=snaps", "volumeSnapshotClasses[0].secretName=qumulo-token")...) {
		var parsed renderedRBAC
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
			continue
		}
		if parsed.Kind != "ClusterRole" && parsed.Kind != "Role" {
			continue
		}
		roles++
		for _, rule := range parsed.Rules {
			for _, group := range rule.APIGroups {
				for _, resource := range rule.Resources {
					key := group + "/" + resource
					if granted[key] == nil {
						granted[key] = map[string]bool{}
					}
					for _, verb := range rule.Verbs {
						granted[key][verb] = true
					}
				}
			}
		}
	}
	require.NotZero(t, roles, "the chart rendered no ClusterRole or Role at all")

	for _, want := range required {
		key := want.apiGroup + "/" + want.resource
		t.Run(key, func(t *testing.T) {
			have, ok := granted[key]
			require.True(t, ok, "no rule grants anything on %q", key)
			for _, verb := range want.verbs {
				assert.True(t, have[verb] || have["*"],
					"%q is granted %v but a sidecar needs %q", key, sortedVerbs(have), verb)
			}
		})
	}
}

// sortedVerbs renders a verb set for a failure message.
func sortedVerbs(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for verb := range set {
		out = append(out, verb)
	}
	sort.Strings(out)
	return out
}

// Every container's privilege must be exactly what its job needs.
//
// Asserted rather than left to comments, because the interesting entries are
// counter-intuitive and every one of them would look like tidying to remove:
//
//   - node-driver-registrar stays *root* while dropping privilege and every
//     capability. It creates a socket in the kubelet's plugins_registry, a
//     root-owned hostPath, and a non-root process cannot write there. The
//     failure that produces is the worst kind -- the plugin silently never
//     registers while the DaemonSet reports healthy -- so "privilege" and
//     "root" are separable here and must not be conflated.
//
//   - the *node's* liveness-probe stays root too, for the same reason one step
//     removed: it connects to the driver's /csi/csi.sock, the node driver is
//     root and so owns that socket at 0755, and connecting to a unix socket
//     needs the write bit. As uid 65532 it never opened its HTTP port, so the
//     driver container's own livenessProbe was refused and the kubelet killed
//     the node plugin every ~2.5 minutes indefinitely. See
//     TestChartCSISocketClientsCanConnectToTheSocket, which asserts the
//     underlying rule rather than this one uid.
//
//   - the node's driver container keeps privileged: true. It mounts
//     filesystems in the host's namespace; that is a real requirement, not an
//     oversight, and it is the only container in the chart that has it.
//
// The controller's five containers are all non-root with no escalation, no
// capabilities and a read-only root filesystem -- including its own copy of
// liveness-probe, which is safe precisely because that driver is 65532 too.
func TestChartSecurityContextsAreWhatEachContainerNeeds(t *testing.T) {
	type want struct {
		nonRoot    bool // runAsNonRoot: true, with an explicit uid
		privileged bool
		hardened   bool // no escalation, drop ALL, read-only root fs
	}
	expected := map[string]want{
		"Deployment/csi-provisioner":      {nonRoot: true, hardened: true},
		"Deployment/csi-resizer":          {nonRoot: true, hardened: true},
		"Deployment/csi-snapshotter":      {nonRoot: true, hardened: true},
		"Deployment/liveness-probe":       {nonRoot: true, hardened: true},
		"Deployment/qumulo-csi-driver":    {nonRoot: true, hardened: true},
		"DaemonSet/liveness-probe":        {nonRoot: false, hardened: true},
		"DaemonSet/node-driver-registrar": {nonRoot: false, hardened: true},
		"DaemonSet/qumulo-csi-driver":     {privileged: true},
	}

	seen := map[string]bool{}
	for _, w := range renderWorkloads(t, baseSets("sidecars.snapshotter.enabled=true")...) {
		// Pod-level, on both workloads.
		require.NotNil(t, w.Spec.Template.Spec.SecurityContext, "%s has no pod-level securityContext", w.Kind)
		require.NotNil(t, w.Spec.Template.Spec.SecurityContext.SeccompProfile, "%s sets no seccompProfile", w.Kind)
		assert.Equal(t, "RuntimeDefault", w.Spec.Template.Spec.SecurityContext.SeccompProfile.Type,
			"%s: seccomp must be RuntimeDefault", w.Kind)

		for _, c := range w.Spec.Template.Spec.Containers {
			name := w.Kind + "/" + c.Name
			exp, ok := expected[name]
			require.True(t, ok, "%s is rendered but this test has no expectation for it", name)
			seen[name] = true

			sc := c.SecurityContext
			require.NotNil(t, sc, "%s has no securityContext at all", name)

			if exp.nonRoot {
				require.NotNil(t, sc.RunAsNonRoot, "%s: runAsNonRoot must be set", name)
				assert.True(t, *sc.RunAsNonRoot, "%s must run as non-root", name)
				// Explicit uid, or the kubelet cannot prove non-root for an
				// image that declares no numeric USER and refuses to start it.
				require.NotNil(t, sc.RunAsUser, "%s: runAsNonRoot needs an explicit runAsUser to be admissible", name)
				assert.NotZero(t, *sc.RunAsUser, "%s: runAsUser must not be 0", name)
			} else if sc.RunAsNonRoot != nil {
				assert.False(t, *sc.RunAsNonRoot,
					"%s must stay root: see this test's doc comment before changing it", name)
			}

			if exp.privileged {
				require.NotNil(t, sc.Privileged, "%s must be privileged to mount in the host namespace", name)
				assert.True(t, *sc.Privileged, "%s must be privileged", name)
			} else {
				if sc.Privileged != nil {
					assert.False(t, *sc.Privileged, "%s must not be privileged", name)
				}
			}

			if exp.hardened {
				require.NotNil(t, sc.AllowPrivilegeEscalation, "%s: allowPrivilegeEscalation must be set", name)
				assert.False(t, *sc.AllowPrivilegeEscalation, "%s must not allow privilege escalation", name)
				require.NotNil(t, sc.ReadOnlyRootFilesystem, "%s: readOnlyRootFilesystem must be set", name)
				assert.True(t, *sc.ReadOnlyRootFilesystem, "%s must have a read-only root filesystem", name)
				require.NotNil(t, sc.Capabilities, "%s must drop capabilities", name)
				assert.Equal(t, []string{"ALL"}, sc.Capabilities.Drop, "%s must drop ALL capabilities", name)
			}
		}
	}

	for name := range expected {
		assert.True(t, seen[name], "%s was expected but is not rendered", name)
	}
}

// Exactly one container in the whole chart may be privileged.
//
// A separate assertion because it is a property of the chart rather than of
// any one container: a second privileged container added anywhere should fail
// here even if its own expectations were written to allow it.
func TestChartHasExactlyOnePrivilegedContainer(t *testing.T) {
	var privileged []string
	for _, w := range renderWorkloads(t, baseSets("sidecars.snapshotter.enabled=true")...) {
		for _, c := range w.Spec.Template.Spec.Containers {
			if sc := c.SecurityContext; sc != nil && sc.Privileged != nil && *sc.Privileged {
				privileged = append(privileged, w.Kind+"/"+c.Name)
			}
		}
	}
	assert.Equal(t, []string{"DaemonSet/qumulo-csi-driver"}, privileged,
		"only the node's driver container may be privileged; it is the only one that mounts")
}

// Capacity reporting must be wired all-or-nothing.
//
// It takes four independent pieces of chart output to work, and any three of
// them without the fourth is a silent failure rather than a loud one:
//
//   - --enable-capacity on csi-provisioner, or nothing ever calls the RPC;
//   - -enable-capacity on the driver, or the RPC has no credential and every
//     poll answers FailedPrecondition;
//   - the csistoragecapacities RBAC rule, or the sidecar is forbidden from
//     writing what it just fetched;
//   - CSIDriver.storageCapacity, which is how the scheduler learns the objects
//     exist at all.
//
// None of those four failures stops provisioning, so none of them is visible
// without looking. Hence a test rather than a comment.
func TestChartCapacityReportingIsWiredConsistently(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled", enabled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sets := baseSets(fmt.Sprintf("sidecars.capacity.enabled=%t", tc.enabled))

			var (
				provisionerArgs []string
				driverArgs      []string
			)
			for _, w := range renderWorkloads(t, sets...) {
				if w.Kind != "Deployment" {
					continue
				}
				driverArgs = driverContainerIn(t, w).Args
				for _, c := range w.Spec.Template.Spec.Containers {
					if c.Name == "csi-provisioner" {
						provisionerArgs = c.Args
					}
				}
			}
			require.NotEmpty(t, provisionerArgs, "csi-provisioner container not found")
			require.NotEmpty(t, driverArgs, "driver container not found")

			var storageCapacity, hasCapacityRBAC bool
			for _, doc := range renderDocs(t, sets...) {
				var head struct {
					Kind string `yaml:"kind"`
					Spec struct {
						StorageCapacity bool `yaml:"storageCapacity"`
					} `yaml:"spec"`
					Rules []struct {
						Resources []string `yaml:"resources"`
					} `yaml:"rules"`
				}
				if err := yaml.Unmarshal([]byte(doc), &head); err != nil {
					continue
				}
				switch head.Kind {
				case "CSIDriver":
					storageCapacity = head.Spec.StorageCapacity
				case "ClusterRole":
					for _, rule := range head.Rules {
						if slices.Contains(rule.Resources, "csistoragecapacities") {
							hasCapacityRBAC = true
						}
					}
				}
			}

			assert.Equal(t, tc.enabled, slices.Contains(provisionerArgs, "--enable-capacity"),
				"csi-provisioner --enable-capacity must follow sidecars.capacity.enabled")
			assert.Equal(t, tc.enabled, slices.Contains(driverArgs, "-enable-capacity"),
				"the driver's -enable-capacity must follow sidecars.capacity.enabled")
			assert.Equal(t, tc.enabled, storageCapacity,
				"CSIDriver.storageCapacity must follow sidecars.capacity.enabled")
			assert.Equal(t, tc.enabled, hasCapacityRBAC,
				"the csistoragecapacities RBAC rule must follow sidecars.capacity.enabled")

			if tc.enabled {
				// The ownerref level is meaningless without POD_NAME, and the
				// sidecar exits rather than degrading if it is absent.
				assert.Contains(t, provisionerArgs, "--capacity-ownerref-level=2")

				// Without this the whole feature is inert. The sidecar skips
				// every StorageClass with volumeBindingMode: Immediate unless
				// it is set, and this chart renders nothing else -- so the
				// Capacity Controller starts, says so, and publishes nothing.
				// Found by deploying it; a unit test cannot see it, because
				// the omission produces no error anywhere.
				assert.Contains(t, provisionerArgs, "--capacity-for-immediate-binding",
					"every StorageClass this chart renders is Immediate, so the sidecar "+
						"publishes no capacity at all without this flag")
			}
		})
	}
}

// The driver must accept the args the chart passes with capacity on, not just
// the default set. This is the gate that fails the moment the chart passes a
// flag the real parser does not know -- which for a flag added to both sides
// at once is exactly the mistake that is easy to make.
func TestChartDriverArgsWithCapacityAreAcceptedByTheDriver(t *testing.T) {
	sets := baseSets("sidecars.capacity.enabled=true")

	for _, w := range renderWorkloads(t, sets...) {
		t.Run(w.Kind, func(t *testing.T) {
			container := driverContainerIn(t, w)
			require.NotEmpty(t, container.Args)

			args := append([]string{"run", "../../cmd/qumulo-csi-driver"}, container.Args...)
			args = append(args, "-version")

			out, err := exec.Command("go", args...).CombinedOutput()
			require.NoError(t, err,
				"the driver rejected the args this chart passes it:\n  %s\n%s",
				strings.Join(container.Args, "\n  "), out)
		})
	}
}

// Volume inventory must render the driver flag and nothing else.
//
// Unlike capacity there is no paired sidecar flag to keep in step: the callers
// are the external-health-monitor sidecar and tools calling the RPC directly,
// neither of which this chart deploys. So the whole contract is that the value
// controls exactly one argument, and that the RBAC it depends on is present
// either way -- because it was already granted to the sidecars, which is what
// makes this feature add no privilege.
func TestChartInventoryRendersTheDriverFlag(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled", enabled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sets := baseSets(fmt.Sprintf("controller.inventory.enabled=%t", tc.enabled))

			var driverArgs []string
			for _, w := range renderWorkloads(t, sets...) {
				if w.Kind == "Deployment" {
					driverArgs = driverContainerIn(t, w).Args
				}
			}
			require.NotEmpty(t, driverArgs, "driver container not found")

			assert.Equal(t, tc.enabled, slices.Contains(driverArgs, "-enable-inventory"),
				"-enable-inventory must follow controller.inventory.enabled")

			// The permissions this needs are unconditional, because the
			// sidecars already require them. If that ever changes, inventory
			// silently starts failing with a forbidden error from the API
			// server rather than anything about Qumulo.
			var hasSecrets, hasClasses bool
			for _, doc := range renderDocs(t, sets...) {
				var head struct {
					Kind  string `yaml:"kind"`
					Rules []struct {
						APIGroups []string `yaml:"apiGroups"`
						Resources []string `yaml:"resources"`
						Verbs     []string `yaml:"verbs"`
					} `yaml:"rules"`
				}
				if err := yaml.Unmarshal([]byte(doc), &head); err != nil || (head.Kind != "ClusterRole" && head.Kind != "Role") {
					continue
				}
				for _, rule := range head.Rules {
					// From the per-namespace Role of the named Secrets.
					if slices.Contains(rule.Resources, "secrets") && slices.Contains(rule.Verbs, "get") {
						hasSecrets = true
					}
					if slices.Contains(rule.Resources, "storageclasses") && slices.Contains(rule.Verbs, "list") {
						hasClasses = true
					}
				}
			}
			assert.True(t, hasSecrets, "inventory needs secrets:get, which the sidecars already require")
			assert.True(t, hasClasses, "inventory needs storageclasses:list, which the sidecars already require")
		})
	}
}

// TestChartAndDriverAgreeOnServerShape holds the chart's server check and the
// driver's against each other.
//
// Both exist on purpose -- the chart so a typo fails at `helm install`, the
// driver so a hand-written StorageClass that never passed through Helm is
// still refused -- and two copies of a rule drift. What makes drift expensive
// here is the asymmetry: a value the chart accepts and the driver rejects is
// merely a Pending PVC, but a value the chart rejects and the driver accepts
// means the looser check is the one guarding the volume handle, and the
// symptom is a PV nothing can delete.
func TestChartAndDriverAgreeOnServerShape(t *testing.T) {
	// escaped is the same value written for `helm --set`, which treats an
	// unescaped dot as a path separator.
	cases := []struct {
		server  string
		escaped string
		accept  bool
	}{
		{server: "qumulo.example.com", escaped: `qumulo\.example\.com`, accept: true},
		{server: "192.0.2.10", escaped: `10\.12\.17\.253`, accept: true},
		{server: "qumulo", escaped: "qumulo", accept: true},
		{server: "nfs-floating-1.storage.example.com", escaped: `nfs-floating-1\.storage\.example\.com`, accept: true},

		{server: "https://192.0.2.10:8000", escaped: `https://10\.12\.17\.253:8000`},
		{server: "qumulo.example.com:8000", escaped: `qumulo\.example\.com:8000`},
		{server: "qumulo.example.com/csi", escaped: `qumulo\.example\.com/csi`},
		{server: "fd00::1", escaped: "fd00::1"},
		{server: "-nope", escaped: "-nope"},
		{server: "under_score.example.com", escaped: `under_score\.example\.com`},
	}

	for _, tc := range cases {
		t.Run(tc.server, func(t *testing.T) {
			args := []string{"template", "test-release", chartPath}
			for _, set := range baseSets("storageClasses[0].server=" + tc.escaped) {
				args = append(args, "--set", set)
			}
			out, chartErr := exec.Command("helm", args...).CombinedOutput()

			driverErr := validateServerHost(tc.server)

			if tc.accept {
				require.NoError(t, chartErr, "chart refused a valid server: %s", out)
				require.NoError(t, driverErr, "driver refused a valid server")
				return
			}
			assert.Error(t, chartErr,
				"chart accepted %q; the driver refuses it, so this only fails at the first PVC: %s",
				tc.server, out)
			assert.Error(t, driverErr,
				"driver accepted %q while the chart refuses it -- the looser of the two checks "+
					"is the one guarding the volume handle", tc.server)
		})
	}
}

// TestChartNFS3DefaultsCarryNolock holds the chart's rendered default and the
// node's injection against each other.
//
// Both supply nolock for nfs3, and both are needed: the default cannot reach a
// class that sets mountOptions explicitly, and the node cannot put an option
// into a PV provisioned before the injection existed. What must not happen is
// the two disagreeing about when it applies -- a default that adds nolock for
// nfs4.1, or a node that adds a second one on top of the default.
func TestChartNFS3DefaultsCarryNolock(t *testing.T) {
	cases := []struct {
		name       string
		sets       []string
		wantOption bool
	}{
		{
			name:       "nfs3 with no mountOptions",
			sets:       baseSets("storageClasses[0].protocol=nfs3"),
			wantOption: true,
		},
		{
			name:       "nfs4.1 with no mountOptions",
			sets:       baseSets(`storageClasses[0].protocol=nfs4\.1`),
			wantOption: false,
		},
		{
			// Verbatim: the operator's list is what lands on the PV, and the
			// node fills the gap at mount time instead.
			name: "nfs3 with explicit mountOptions is left alone",
			sets: baseSets("storageClasses[0].protocol=nfs3",
				"storageClasses[0].mountOptions[0]=hard"),
			wantOption: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			classes := renderChart(t, tc.sets...)
			require.Len(t, classes, 1)
			options := classes[0].MountOptions

			assert.Equal(t, tc.wantOption, slices.Contains(options, "nolock"),
				"rendered mountOptions were %v", options)

			// Whatever the chart rendered, feeding it back through the node's
			// own decision must never produce a duplicate.
			context := map[string]string{}
			if protocol := classes[0].Parameters[paramProtocol]; protocol != "" {
				context[volumeContextProtocol] = protocol
			}
			injected, err := nfsLockOptionFor(context, options)
			require.NoError(t, err)
			if slices.Contains(options, "nolock") {
				assert.Empty(t, injected,
					"the node must not add a second nolock to a class that already renders one")
			}
			assert.False(t, slices.Contains(options, "nolock") && injected != "",
				"exactly one of the chart and the node supplies nolock, never both")
		})
	}
}

// The csi-addons replication service must render as one piece or not at
// all: the sidecar container, -enable-replication on the driver, the
// CSIAddonsNode RBAC, and the VolumeReplicationClass. Any one without the
// others fails silently -- a sidecar with no capability behind it does
// nothing, a driver flag with no sidecar is never called -- so the value
// controls all four, exactly as sidecars.capacity.enabled does.
func TestChartCSIAddonsIsWiredConsistently(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled", enabled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sets := baseSets(fmt.Sprintf("sidecars.csiAddons.enabled=%t", tc.enabled))

			var driverArgs []string
			var sidecar *renderedContainer
			for _, w := range renderWorkloads(t, sets...) {
				if w.Kind != "Deployment" {
					continue
				}
				driverArgs = driverContainerIn(t, w).Args
				for i := range w.Spec.Template.Spec.Containers {
					if w.Spec.Template.Spec.Containers[i].Name == "csi-addons" {
						sidecar = &w.Spec.Template.Spec.Containers[i]
					}
				}
			}
			require.NotEmpty(t, driverArgs, "driver container not found")

			var hasRBAC, hasTokenReview bool
			for _, doc := range renderDocs(t, sets...) {
				var parsed renderedRBAC
				if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil ||
					(parsed.Kind != "ClusterRole" && parsed.Kind != "Role") {
					continue
				}
				for _, rule := range parsed.Rules {
					// Namespaced since: the CSIAddonsNode lives in the
					// pod's own namespace.
					if slices.Contains(rule.Resources, "csiaddonsnodes") {
						hasRBAC = true
					}
					if slices.Contains(rule.Resources, "tokenreviews") && slices.Contains(rule.Verbs, "create") {
						hasTokenReview = true
					}
				}
			}

			assert.Equal(t, tc.enabled, sidecar != nil, "the csi-addons container must follow sidecars.csiAddons.enabled")
			assert.Equal(t, tc.enabled, slices.Contains(driverArgs, "-enable-replication"),
				"the driver's -enable-replication must follow sidecars.csiAddons.enabled")
			assert.Equal(t, tc.enabled, hasRBAC, "the csiaddonsnodes RBAC rule must follow sidecars.csiAddons.enabled")
			// The sidecar verifies the controller's token with a TokenReview;
			// without the verb the CSIAddonsNode is created and deleted in a
			// loop and nothing is ever served (measured, 2026-09-14).
			assert.Equal(t, tc.enabled, hasTokenReview, "the tokenreviews create verb must follow sidecars.csiAddons.enabled")

			if tc.enabled {
				// Pinned on purpose. The lab runs (the TokenReview RBAC,
				// the replication_source handle field, the enable-once
				// cadence, force on FailedPrecondition) were
				// measured on v0.15.0; v0.15.1 is its dependency-only patch,
				// taken for CVE fixes, and v0.15.1-qumulo.2 is our rebuild of
				// it with the rest of them (build/csi-addons-sidecar/), from
				// the same source and toolchain as -qumulo.1, which was
				// re-measured on kind on 2026-11-25. Any other tag is an
				// unreviewed sidecar.
				assert.True(t, strings.HasPrefix(sidecar.Image, "ghcr.io/qumulo/qumulo-csi-public/csi-addons-k8s-sidecar:v0.15.1-qumulo.2@sha256:"),
					"the csi-addons sidecar must be the measured version, pinned by digest: %s", sidecar.Image)
				// The sidecar dials the SAME socket the CSI sidecars use: the
				// driver serves the csi-addons services alongside the CSI
				// ones, and a second socket would need a second listener the
				// driver does not have.
				assert.Contains(t, sidecar.Args, "--csi-addons-address=$(CSIADDONS_ENDPOINT)")
				assert.Contains(t, sidecar.Args, "--controller-port=9070")
				require.Len(t, sidecar.Ports, 1)
				assert.Equal(t, 9070, sidecar.Ports[0].ContainerPort)
				// The sidecar runs under the controller's non-root context
				// like every other container in the pod.
				require.NotNil(t, sidecar.SecurityContext)
				require.NotNil(t, sidecar.SecurityContext.RunAsNonRoot)
				assert.True(t, *sidecar.SecurityContext.RunAsNonRoot)
			}
		})
	}
}

// The driver must accept the args the chart passes with replication on. Same
// gate as capacity: a flag added to both sides at once is exactly the mistake
// that compiles and then crash-loops.
func TestChartDriverArgsWithReplicationAreAcceptedByTheDriver(t *testing.T) {
	sets := baseSets("sidecars.csiAddons.enabled=true")

	for _, w := range renderWorkloads(t, sets...) {
		t.Run(w.Kind, func(t *testing.T) {
			container := driverContainerIn(t, w)
			require.NotEmpty(t, container.Args)

			args := append([]string{"run", "../../cmd/qumulo-csi-driver"}, container.Args...)
			args = append(args, "-version")

			out, err := exec.Command("go", args...).CombinedOutput()
			require.NoError(t, err,
				"the driver rejected the args this chart passes it:\n  %s\n%s",
				strings.Join(container.Args, "\n  "), out)
		})
	}
}

// The rendered VolumeReplicationClass must carry parameters the driver's
// own parser accepts -- the same contract TestChartParametersAreAcceptedByTheDriver
// holds for StorageClasses -- and must be refused without the sidecar that
// would act on it.
func TestChartReplicationClassParametersAreAcceptedByTheDriver(t *testing.T) {
	sets := baseSets(
		"sidecars.csiAddons.enabled=true",
		"volumeReplicationClasses[0].name=qumulo-to-b",
		"volumeReplicationClasses[0].targetServer=cluster-b.example.com",
		`volumeReplicationClasses[0].targetApiUrl=https://cluster-b\.example\.com:8000`,
		`volumeReplicationClasses[0].apiUrl=https://`+testServer+`:8000`,
		"volumeReplicationClasses[0].replicationMode=policy+continuous",
		"volumeReplicationClasses[0].linkSnapshotPolicy=true",
		"volumeReplicationClasses[0].schedulingInterval=10m",
		"volumeReplicationClasses[0].replicationScope=group",
		"volumeReplicationClasses[0].secretName=tokens",
	)

	var classes []renderedStorageClass
	for _, doc := range renderDocs(t, sets...) {
		var parsed renderedStorageClass
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil || parsed.Kind != "VolumeReplicationClass" {
			continue
		}
		classes = append(classes, parsed)
	}
	require.Len(t, classes, 1, "expected one VolumeReplicationClass")

	// The class's parameters sit under spec, which renderedStorageClass does
	// not model; unmarshal the one document again for them.
	var vrc struct {
		Spec struct {
			Provisioner string            `yaml:"provisioner"`
			Parameters  map[string]string `yaml:"parameters"`
		} `yaml:"spec"`
	}
	for _, doc := range renderDocs(t, sets...) {
		if strings.Contains(doc, "kind: VolumeReplicationClass") {
			require.NoError(t, yaml.Unmarshal([]byte(doc), &vrc))
		}
	}
	assert.Equal(t, "csi.qumulo.com", vrc.Spec.Provisioner)
	assert.Equal(t, "tokens", vrc.Spec.Parameters["replication.storage.openshift.io/replication-secret-name"])

	parsed, err := parseReplicationParams(vrc.Spec.Parameters)
	require.NoError(t, err, "the chart rendered a parameter the driver refuses: %v", vrc.Spec.Parameters)
	assert.Equal(t, "cluster-b.example.com", parsed.TargetServer)
	assert.Equal(t, qumulo.ReplicationModeSnapshotPolicyContinuous, parsed.arrayMode())
	assert.True(t, parsed.LinkSnapshotPolicy)
	assert.Equal(t, 10*time.Minute, parsed.SchedulingInterval)
	// A shared-subdirectory class is protected as one unit or not at all.
	assert.Equal(t, replicationScopeGroup, parsed.Scope)
	// The delete policy is deliberately NOT a VolumeReplicationClass parameter
	// the chart renders: DeleteVolume never sees a class, and reads it from
	// the StorageClass's delete Secret instead (replication_guard.go).
	assert.Equal(t, replicationDeletePolicyRefuse, parsed.DeletePolicy)

	// Without the sidecar the class would be an object nothing acts on.
	orphan := append([]string{"template", "test-release", chartPath}, setArgs(baseSets(
		"volumeReplicationClasses[0].name=orphan",
		"volumeReplicationClasses[0].targetServer=10.0.0.2",
		"volumeReplicationClasses[0].secretName=tokens",
	))...)
	out, err := exec.Command("helm", orphan...).CombinedOutput()
	require.Error(t, err, "a VolumeReplicationClass without sidecars.csiAddons.enabled must be refused")
	assert.Contains(t, string(out), "sidecars.csiAddons.enabled")
}

// A DNS name may float between arrays at failover -- the runbook recommends
// exactly that for the StorageClass server -- so it must never be what the
// controller dials for the API: after a flip it is the other array.
// The chart and the driver both require the fixed endpoint whenever the
// corresponding address is not an IP literal, and both accept IP literals
// without one.
func TestChartRequiresFixedEndpointsForDNSNamedArrays(t *testing.T) {
	render := func(sets ...string) (string, error) {
		args := append([]string{"template", "test-release", chartPath}, setArgs(sets)...)
		out, err := exec.Command("helm", args...).CombinedOutput()
		return string(out), err
	}
	class := func(extra ...string) []string {
		return append([]string{
			"sidecars.csiAddons.enabled=true",
			"volumeReplicationClasses[0].name=qumulo-to-b",
			"volumeReplicationClasses[0].secretName=tokens",
		}, extra...)
	}

	t.Run("DNS-named targetServer without targetApiUrl is refused", func(t *testing.T) {
		out, err := render(baseSets(class(
			"volumeReplicationClasses[0].targetServer=cluster-b.example.com",
			`volumeReplicationClasses[0].apiUrl=https://`+testServer+`:8000`,
		)...)...)
		require.Error(t, err)
		assert.Contains(t, out, "targetApiUrl")
		assert.Contains(t, out, "float")
	})

	t.Run("DNS-named StorageClass server without apiUrl on the class is refused", func(t *testing.T) {
		out, err := render(baseSets(class(
			"volumeReplicationClasses[0].targetServer=10.0.0.2",
		)...)...)
		require.Error(t, err)
		assert.Contains(t, out, "apiUrl")
		assert.Contains(t, out, testServer)
	})

	t.Run("IP literals need no fixed endpoint", func(t *testing.T) {
		_, err := render(
			"storageClasses[0].name=qumulo-nfs",
			"storageClasses[0].server=10.0.0.1",
			"storageClasses[0].secretName=qumulo-token",
			"storageClasses[0].basePath=/csi/volumes",
			"sidecars.csiAddons.enabled=true",
			"volumeReplicationClasses[0].name=qumulo-to-b",
			"volumeReplicationClasses[0].secretName=tokens",
			"volumeReplicationClasses[0].targetServer=10.0.0.2",
		)
		require.NoError(t, err)
	})

	t.Run("both fixed endpoints satisfy the chart and the driver alike", func(t *testing.T) {
		out, err := render(baseSets(class(
			"volumeReplicationClasses[0].targetServer=cluster-b.example.com",
			`volumeReplicationClasses[0].targetApiUrl=https://cluster-b\.example\.com:8000`,
			`volumeReplicationClasses[0].apiUrl=https://`+testServer+`:8000`,
		)...)...)
		require.NoError(t, err, out)
		_, err = parseReplicationParams(map[string]string{
			"targetServer": "cluster-b.example.com", "targetApiUrl": "https://cluster-b.example.com:8000",
			"apiUrl": "https://" + testServer + ":8000",
		})
		require.NoError(t, err)
	})
}

// renderServices returns every Service the chart produced.
func renderServices(t *testing.T, sets ...string) []renderedService {
	t.Helper()

	var out []renderedService
	for _, doc := range renderDocs(t, sets...) {
		var parsed renderedService
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
			continue
		}
		if parsed.Kind == "Service" {
			out = append(out, parsed)
		}
	}
	return out
}

// renderMonitoring returns every ServiceMonitor and PrometheusRule.
func renderMonitoring(t *testing.T, sets ...string) []renderedMonitoring {
	t.Helper()

	var out []renderedMonitoring
	for _, doc := range renderDocs(t, sets...) {
		var parsed renderedMonitoring
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
			continue
		}
		if parsed.Kind == "ServiceMonitor" || parsed.Kind == "PrometheusRule" {
			out = append(out, parsed)
		}
	}
	return out
}

// containerIn returns one named container from a workload.
func containerIn(t *testing.T, w renderedWorkload, name string) (renderedContainer, bool) {
	t.Helper()

	for _, c := range w.Spec.Template.Spec.Containers {
		if c.Name == name {
			return c, true
		}
	}
	return renderedContainer{}, false
}

// portNamed returns a container's port by name.
func portNamed(c renderedContainer, name string) (int, bool) {
	for _, p := range c.Ports {
		if p.Name == name {
			return p.ContainerPort, true
		}
	}
	return 0, false
}

// TestChartPodPortNamesAreUnique renders every container that opens a port
// and checks no two in one pod share a port name. Kubernetes accepts the
// duplicate but warns on every install and upgrade, and a lookup by name
// (kubectl port-forward pod/... metrics) silently gets the first one: the
// provisioner, resizer and snapshotter all once named theirs "metrics", like
// the driver.
func TestChartPodPortNamesAreUnique(t *testing.T) {
	workloads := renderWorkloads(t, baseSets(
		"metrics.enabled=true",
		"sidecars.snapshotter.enabled=true",
		"sidecars.csiAddons.enabled=true",
	)...)
	require.NotEmpty(t, workloads)
	for _, w := range workloads {
		owner := map[string]string{}
		for _, c := range w.Spec.Template.Spec.Containers {
			for _, p := range c.Ports {
				if first, dup := owner[p.Name]; dup {
					t.Errorf("%s %q: containers %q and %q both name a port %q",
						w.Kind, w.Metadata.Name, first, c.Name, p.Name)
					continue
				}
				owner[p.Name] = c.Name
			}
		}
	}
}

// TestChartMetricsAreWiredConsistently holds every moving part of the
// feature together, because each one alone fails silently.
//
// The driver's -metrics-address without a containerPort is a listener no
// Service can reach; a Service without the flag is a Service with no
// endpoints; a sidecar without --http-endpoint serves nothing while the
// Service claims a port for it. All of those render cleanly and then produce
// an empty dashboard, which is the failure mode this whole file exists for.
func TestChartMetricsAreWiredConsistently(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled", enabled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sets := baseSets(
				fmt.Sprintf("metrics.enabled=%t", tc.enabled),
				// The snapshotter's port is the one conditional port in the
				// Service, so it is on for both halves of this table.
				"sidecars.snapshotter.enabled=true",
			)

			services := renderServices(t, sets...)
			assert.Equal(t, tc.enabled, len(services) > 0,
				"the metrics Services must follow metrics.enabled")

			for _, w := range renderWorkloads(t, sets...) {
				driver := driverContainerIn(t, w)
				_, hasPort := portNamed(driver, "metrics")

				hasFlag := false
				for _, arg := range driver.Args {
					if strings.HasPrefix(arg, "-metrics-address=") {
						hasFlag = true
					}
				}

				assert.Equal(t, tc.enabled, hasFlag,
					"%s: -metrics-address must follow metrics.enabled", w.Kind)
				assert.Equal(t, tc.enabled, hasPort,
					"%s: the driver's metrics containerPort must follow the flag", w.Kind)
			}

			// The three CSI sidecars publish csi_sidecar_operations_seconds,
			// which the driver cannot produce: it is how long the sidecar
			// waited, retries included.
			for _, w := range renderWorkloads(t, sets...) {
				if w.Kind != "Deployment" {
					continue
				}
				for _, name := range []string{"csi-provisioner", "csi-resizer", "csi-snapshotter"} {
					container, ok := containerIn(t, w, name)
					require.True(t, ok, "%s not rendered", name)

					hasEndpoint := false
					for _, arg := range container.Args {
						if strings.HasPrefix(arg, "--http-endpoint=") {
							hasEndpoint = true
						}
						// --metrics-address is the deprecated spelling of the
						// same flag and csi-lib-utils refuses to start when
						// both are given, so it must never appear.
						assert.NotContains(t, arg, "--metrics-address=",
							"%s: --http-endpoint and --metrics-address are the same flag; "+
								"passing both makes the sidecar exit at startup", name)
					}
					// Always on: the sidecar's liveness and readiness probes read
					// /healthz/leader-election there. Only the Service that
					// publishes its metrics follows metrics.enabled.
					assert.True(t, hasEndpoint,
						"%s: --http-endpoint serves the probes and must be on whether or not metrics are published", name)
				}
			}

			if !tc.enabled {
				return
			}

			// Every port a Service claims must be a port some container in
			// the workload it selects actually binds.
			workloads := map[string]renderedWorkload{}
			for _, w := range renderWorkloads(t, sets...) {
				workloads[w.Kind] = w
			}

			byComponent := map[string]renderedService{}
			for _, svc := range services {
				byComponent[svc.Metadata.Labels["app.kubernetes.io/component"]] = svc
			}
			require.Contains(t, byComponent, "controller")
			require.Contains(t, byComponent, "node")

			for component, kind := range map[string]string{"controller": "Deployment", "node": "DaemonSet"} {
				svc := byComponent[component]
				workload := workloads[kind]

				// A selector that does not match the pods is the failure
				// that looks identical to a driver exporting nothing.
				for key, want := range svc.Spec.Selector {
					assert.Equal(t, want, workload.Spec.Template.Metadata.Labels[key],
						"%s Service selector key %q does not match the pod labels", component, key)
				}

				bound := map[int]bool{}
				for _, container := range workload.Spec.Template.Spec.Containers {
					for _, port := range container.Ports {
						bound[port.ContainerPort] = true
					}
				}
				for _, port := range svc.Spec.Ports {
					assert.True(t, bound[port.TargetPort],
						"%s Service claims port %d that no container binds", component, port.TargetPort)
				}
			}

			// The node pod is hostNetwork, so its Service must be headless:
			// a ClusterIP would load-balance across nodes and scrape a
			// different one on every interval.
			assert.Equal(t, "None", byComponent["node"].Spec.ClusterIP,
				"the node metrics Service must be headless, one target per node")
		})
	}
}

// The ServiceMonitor and PrometheusRule are guarded on the CRD, not merely
// on their own value: a manifest for a kind the API server does not know
// fails the whole release, which would turn "I enabled metrics" into "my
// install broke" on any cluster without the Prometheus Operator.
func TestChartMonitoringObjectsAreGuardedOnTheCRD(t *testing.T) {
	enabled := baseSets(
		"metrics.enabled=true",
		"metrics.serviceMonitor.enabled=true",
		"metrics.prometheusRule.enabled=true",
	)

	// helm template has no cluster to ask, so the capability check is false
	// and neither object may render.
	assert.Empty(t, renderMonitoring(t, enabled...),
		"neither object may render when the monitoring CRD cannot be proven present")

	forced := append(append([]string{}, enabled...), "metrics.forceEnable=true")
	objects := renderMonitoring(t, forced...)
	require.Len(t, objects, 2, "forceEnable must render both objects")

	byKind := map[string]renderedMonitoring{}
	for _, object := range objects {
		byKind[object.Kind] = object
	}
	require.Contains(t, byKind, "ServiceMonitor")
	require.Contains(t, byKind, "PrometheusRule")

	// The ServiceMonitor must select the Services this chart renders, or it
	// scrapes nothing and says so nowhere.
	services := renderServices(t, forced...)
	require.NotEmpty(t, services)
	for key, want := range byKind["ServiceMonitor"].Spec.Selector.MatchLabels {
		for _, svc := range services {
			assert.Equal(t, want, svc.Metadata.Labels[key],
				"ServiceMonitor selector key %q does not match Service %s", key, svc.Metadata.Name)
		}
	}

	// And it must name ports the Services actually expose.
	exposed := map[string]bool{}
	for _, svc := range services {
		for _, port := range svc.Spec.Ports {
			exposed[port.Name] = true
		}
	}
	for _, endpoint := range byKind["ServiceMonitor"].Spec.Endpoints {
		assert.True(t, exposed[endpoint.Port],
			"the ServiceMonitor scrapes port %q that no Service exposes", endpoint.Port)
		assert.Equal(t, "/metrics", endpoint.Path)
	}
}

// Every alert must be written against a series this driver actually exports.
// An alert on a metric name that does not exist never fires, and an alert
// that never fires is indistinguishable from a system that is healthy.
func TestChartAlertsNameSeriesTheDriverExports(t *testing.T) {
	sets := baseSets(
		"metrics.enabled=true",
		"metrics.prometheusRule.enabled=true",
		"metrics.forceEnable=true",
	)

	var rule renderedMonitoring
	for _, object := range renderMonitoring(t, sets...) {
		if object.Kind == "PrometheusRule" {
			rule = object
		}
	}
	require.NotEmpty(t, rule.Spec.Groups, "no PrometheusRule rendered")

	// The names pkg/metrics registers. Kept here rather than imported so a
	// rename in either place breaks this test rather than an operator's
	// alerting.
	exported := []string{
		"csi_qumulo_rpc_total",
		"csi_qumulo_rpc_duration_seconds",
		"csi_qumulo_api_requests_total",
		"csi_qumulo_api_duration_seconds",
		"csi_qumulo_api_reachable",
		"csi_qumulo_volume_quota_bytes",
		"csi_qumulo_volume_used_bytes",
		"csi_qumulo_volume_replication_protected",
		"csi_qumulo_cluster_capacity_bytes",
		"csi_qumulo_cluster_free_bytes",
		"csi_qumulo_restore_staging_directories_total",
		"csi_qumulo_replication_recovery_point_age_seconds",
		"csi_qumulo_replication_state",
		"csi_qumulo_replication_queued_snapshots",
		"csi_qumulo_replication_mode",
		"csi_qumulo_inventory_class_credential_usable",
		"csi_qumulo_build_info",
	}

	var alerts []string
	for _, group := range rule.Spec.Groups {
		for _, entry := range group.Rules {
			alerts = append(alerts, entry.Alert)

			named := false
			for _, series := range exported {
				if strings.Contains(entry.Expr, series) {
					named = true
				}
			}
			assert.True(t, named,
				"alert %s is written against a series this driver does not export: %s",
				entry.Alert, entry.Expr)
		}
	}

	// The three alerts the day-two vision names, plus the quota one. The
	// replication alert is on recovery-point age and not on state, because a
	// target array reports ESTABLISHED with a dead source.
	assert.Contains(t, alerts, "QumuloCSIArrayUnreachable")
	// A refused credential is an HTTP answer, not unreachability, so
	// a dead token and a missing privilege have alerts of their own.
	assert.Contains(t, alerts, "QumuloCSICredentialRejected")
	assert.Contains(t, alerts, "QumuloCSIPrivilegeMissing")
	assert.Contains(t, alerts, "QumuloCSIProvisioningFailing")
	assert.Contains(t, alerts, "QumuloCSIReplicationRecoveryPointStale")
	assert.Contains(t, alerts, "QumuloCSIVolumeNearQuota")
	// CSI gives ListVolumes no way to report a class it left out, so this
	// alert is the only signal that a listing is short.
	assert.Contains(t, alerts, "QumuloCSIInventoryClassSkipped")

	for _, group := range rule.Spec.Groups {
		for _, entry := range group.Rules {
			if entry.Alert == "QumuloCSIReplicationRecoveryPointStale" {
				assert.Contains(t, entry.Expr, "recovery_point_age_seconds")
				assert.NotContains(t, entry.Expr, "replication_state",
					"state is the description, age is the alarm")
			}
		}
	}
}

// No two containers in one pod may claim the same port, and the metrics
// ports must not collide with the livenessprobe sidecars already on :29652
// and :29653. A collision leaves one container crash-looping, which takes
// the whole pod -- provisioning included -- with it.
func TestChartMetricsPortsDoNotCollide(t *testing.T) {
	sets := baseSets(
		"metrics.enabled=true",
		"sidecars.snapshotter.enabled=true",
		"sidecars.csiAddons.enabled=true",
	)

	for _, w := range renderWorkloads(t, sets...) {
		t.Run(w.Kind, func(t *testing.T) {
			owner := map[int]string{}
			for _, container := range w.Spec.Template.Spec.Containers {
				for _, port := range container.Ports {
					previous, taken := owner[port.ContainerPort]
					assert.False(t, taken,
						"port %d is claimed by both %s and %s",
						port.ContainerPort, previous, container.Name)
					owner[port.ContainerPort] = container.Name
				}
			}
		})
	}
}

// The driver must accept the args the chart passes with metrics on. Same
// gate as capacity and replication: a flag added to the chart and to the
// binary in one change is exactly the mistake that compiles and then
// crash-loops.
func TestChartDriverArgsWithMetricsAreAcceptedByTheDriver(t *testing.T) {
	sets := baseSets("metrics.enabled=true")

	for _, w := range renderWorkloads(t, sets...) {
		t.Run(w.Kind, func(t *testing.T) {
			container := driverContainerIn(t, w)
			require.NotEmpty(t, container.Args)

			args := append([]string{"run", "../../cmd/qumulo-csi-driver"}, container.Args...)
			args = append(args, "-version")

			out, err := exec.Command("go", args...).CombinedOutput()
			require.NoError(t, err,
				"the driver rejected the args this chart passes it:\n  %s\n%s",
				strings.Join(container.Args, "\n  "), out)
		})
	}
}

// The forced-unmount timeout is a node-only knob, and rendering it must not
// change anything else.
//
// Three things are held together here, which is why it is one test: the value
// renders exactly one argument, it renders it on the *node* and nowhere else
// (a controller has no mounts and the flag would be inert noise there), and
// the argument it renders is one the driver's own flag parser accepts. The
// last part is the same contract TestChartDriverArgsAreAcceptedByTheDriver
// holds for the base args: a chart that renders a flag the binary rejects
// crash-loops the DaemonSet on every node at once.
func TestChartUnmountTimeoutIsANodeOnlyFlag(t *testing.T) {
	// Unset: nothing rendered, so the driver's own default is the single
	// source of truth for a deployment that has not tuned it.
	for _, w := range renderWorkloads(t, baseSets()...) {
		for _, arg := range driverContainerIn(t, w).Args {
			assert.NotContains(t, arg, "-unmount-timeout",
				"%s must render no unmount timeout when node.unmountTimeout is unset", w.Kind)
		}
	}

	sets := baseSets("node.unmountTimeout=45s")
	seen := map[string]bool{}
	for _, w := range renderWorkloads(t, sets...) {
		args := driverContainerIn(t, w).Args
		seen[w.Kind] = true

		if w.Kind == "DaemonSet" {
			assert.Contains(t, args, "-unmount-timeout=45s",
				"the node DaemonSet must carry the configured timeout")
		} else {
			for _, arg := range args {
				assert.NotContains(t, arg, "-unmount-timeout",
					"only the node mounts anything; %s must not carry this flag", w.Kind)
			}
		}

		t.Run(w.Kind, func(t *testing.T) {
			runArgs := append([]string{"run", "../../cmd/qumulo-csi-driver"}, args...)
			runArgs = append(runArgs, "-version")

			out, err := exec.Command("go", runArgs...).CombinedOutput()
			require.NoError(t, err,
				"the driver rejected the args this chart passes it:\n  %s\n%s",
				strings.Join(args, "\n  "), out)
		})
	}
	assert.Len(t, seen, 2, "both workloads must render")
}

// The node plugin must not be the first thing preempted or evicted: it
// is what unmounts every volume on its node. By default the chart creates its
// own PriorityClass and gives it to the node pods -- a system-*-critical
// default is refused outside kube-system on some clusters (GKE), which would
// stop the DaemonSet scheduling at all. An explicit priorityClassName wins
// and no class is created; priorityClass.create=false gives none.
func TestChartNodeHasAPriorityClassByDefault(t *testing.T) {
	render := func(sets ...string) (priorities map[string]string, classes []string) {
		priorities = map[string]string{}
		for _, doc := range renderDocs(t, sets...) {
			var w struct {
				Kind     string `yaml:"kind"`
				Metadata struct {
					Name string `yaml:"name"`
				} `yaml:"metadata"`
				PreemptionPolicy string `yaml:"preemptionPolicy"`
				Spec             struct {
					Template struct {
						Spec struct {
							PriorityClassName string `yaml:"priorityClassName"`
						} `yaml:"spec"`
					} `yaml:"template"`
				} `yaml:"spec"`
			}
			if err := yaml.Unmarshal([]byte(doc), &w); err != nil {
				continue
			}
			switch w.Kind {
			case "Deployment", "DaemonSet":
				priorities[w.Kind] = w.Spec.Template.Spec.PriorityClassName
			case "PriorityClass":
				assert.Equal(t, "Never", w.PreemptionPolicy, "the node plugin must not evict workloads to fit")
				classes = append(classes, w.Metadata.Name)
			}
		}
		return priorities, classes
	}

	priorities, classes := render(baseSets()...)
	assert.Equal(t, []string{"test-release-qumulo-csi-driver-node-critical"}, classes)
	// The controller has its own, system-cluster-critical.
	assert.Equal(t, map[string]string{"Deployment": "system-cluster-critical", "DaemonSet": "test-release-qumulo-csi-driver-node-critical"}, priorities,
		"only the node plugin takes it")

	// A --reuse-values upgrade from a chart without node.priorityClass keeps
	// the old chart's defaults: the class is still created and used.
	priorities, classes = render(append(baseSets(), "node.priorityClass=null")...)
	assert.Equal(t, []string{"test-release-qumulo-csi-driver-node-critical"}, classes)
	assert.Equal(t, "test-release-qumulo-csi-driver-node-critical", priorities["DaemonSet"])

	priorities, classes = render(append(baseSets(), "node.priorityClassName=system-node-critical")...)
	assert.Empty(t, classes, "an explicit class means none is created")
	assert.Equal(t, "system-node-critical", priorities["DaemonSet"])

	priorities, classes = render(append(baseSets(), "node.priorityClass.create=false")...)
	assert.Empty(t, classes)
	assert.Empty(t, priorities["DaemonSet"])
}

// With no requests every node container was BestEffort, so the node
// pods were the first the kubelet evicted under pressure. Each container now
// has requests by default, and the older flat resources form still applies
// to all three.
func TestChartNodePodsAreNotBestEffort(t *testing.T) {
	requestsOf := func(sets ...string) map[string]map[string]string {
		got := map[string]map[string]string{}
		for _, doc := range renderDocs(t, sets...) {
			var w struct {
				Kind string `yaml:"kind"`
				Spec struct {
					Template struct {
						Spec struct {
							Containers []struct {
								Name      string `yaml:"name"`
								Resources struct {
									Requests map[string]string `yaml:"requests"`
								} `yaml:"resources"`
							} `yaml:"containers"`
						} `yaml:"spec"`
					} `yaml:"template"`
				} `yaml:"spec"`
			}
			if err := yaml.Unmarshal([]byte(doc), &w); err != nil || w.Kind != "DaemonSet" {
				continue
			}
			for _, c := range w.Spec.Template.Spec.Containers {
				got[c.Name] = c.Resources.Requests
			}
		}
		return got
	}

	byDefault := requestsOf(baseSets()...)
	require.Len(t, byDefault, 3)
	for name, requests := range byDefault {
		assert.NotEmpty(t, requests["cpu"], "%s has no CPU request", name)
		assert.NotEmpty(t, requests["memory"], "%s has no memory request", name)
	}

	// node.resources {} -- the old chart's default, which a --reuse-values
	// upgrade keeps -- renders the same requests as the new default.
	assert.Equal(t, byDefault, requestsOf(append(baseSets(), "node.resources=null")...))

	flat := requestsOf(append(baseSets(), `node.resources.requests.cpu=50m`, `node.resources.requests.memory=32Mi`)...)
	for name, requests := range flat {
		assert.Equal(t, map[string]string{"cpu": "50m", "memory": "32Mi"}, requests, "%s: the flat form applies to every container", name)
	}
}

// The stale-recovery-point alert gives policy-mode relationships their
// own threshold, joined on the mode series, and both thresholds are the
// values' own -- a value that renders nowhere is a setting that does nothing.
func TestChartRecoveryPointAlertScopesTheThresholdByMode(t *testing.T) {
	sets := baseSets(
		"metrics.enabled=true",
		"metrics.prometheusRule.enabled=true",
		"metrics.forceEnable=true",
		"metrics.prometheusRule.recoveryPointAgeSeconds=901",
		"metrics.prometheusRule.policyModeRecoveryPointAgeSeconds=5401",
	)

	var expr string
	for _, object := range renderMonitoring(t, sets...) {
		for _, group := range object.Spec.Groups {
			for _, rule := range group.Rules {
				if rule.Alert == "QumuloCSIReplicationRecoveryPointStale" {
					expr = rule.Expr
				}
			}
		}
	}
	require.NotEmpty(t, expr, "QumuloCSIReplicationRecoveryPointStale not rendered")

	assert.Contains(t, expr, "> 901")
	assert.Contains(t, expr, "> 5401")
	assert.Contains(t, expr, `and on (relationship_id) csi_qumulo_replication_mode{mode!="REPLICATION_SNAPSHOT_POLICY"}`)
	assert.Contains(t, expr, `and on (relationship_id) csi_qumulo_replication_mode{mode="REPLICATION_SNAPSHOT_POLICY"}`)
	// A relationship with no mode series gets its own threshold, by
	// default the policy one.
	assert.Contains(t, expr, "> 5401\n  unless on (relationship_id) csi_qumulo_replication_mode)")

	sets = append(sets, "metrics.prometheusRule.unknownModeRecoveryPointAgeSeconds=1200")
	expr = ""
	for _, object := range renderMonitoring(t, sets...) {
		for _, group := range object.Spec.Groups {
			for _, rule := range group.Rules {
				if rule.Alert == "QumuloCSIReplicationRecoveryPointStale" {
					expr = rule.Expr
				}
			}
		}
	}
	assert.Contains(t, expr, "> 1200\n  unless on (relationship_id) csi_qumulo_replication_mode)")

	// A known schedule interval scales the threshold, never below the
	// strict one, and the fixed branches skip that relationship.
	sets = append(sets, "metrics.prometheusRule.policyIntervalFactor=2")
	expr = ""
	for _, object := range renderMonitoring(t, sets...) {
		for _, group := range object.Spec.Groups {
			for _, rule := range group.Rules {
				if rule.Alert == "QumuloCSIReplicationRecoveryPointStale" {
					expr = rule.Expr
				}
			}
		}
	}
	assert.Contains(t, expr,
		"clamp_min(max by (relationship_id) (csi_qumulo_replication_policy_interval_seconds) * 2, 901)")
	assert.True(t, strings.HasSuffix(strings.TrimSpace(expr), "unless on (relationship_id) csi_qumulo_replication_policy_interval_seconds"))

	// The aggregation around the interval is load-bearing. group_left
	// needs its right-hand side unique per relationship_id, and two
	// controller replicas can both export one relationship's interval after
	// a csi-addons leadership change -- two matches make the whole
	// expression error, taking every branch of this alert with it. Asserted
	// here as well as in the promtool tests, because a rewrite that drops
	// the aggregation would still pass promtool's single-series cases.
	assert.NotContains(t, expr, "clamp_min(csi_qumulo_replication_policy_interval_seconds",
		"the interval lookup must stay aggregated by relationship_id")
}

// The policy-mode threshold was added in 0.1.0 after releases were already
// installed, and `helm upgrade --reuse-values` does not bring in a value a
// newer chart adds: it rendered "age_seconds > " with nothing after it, the
// prometheus-operator webhook rejected the rule, and the whole upgrade failed
// (found upgrading the minikube release, 2026-09-29). An absent value renders
// the chart's default.
func TestChartRecoveryPointAlertSurvivesAnUpgradeThatReusesOldValues(t *testing.T) {
	sets := baseSets(
		"metrics.enabled=true",
		"metrics.prometheusRule.enabled=true",
		"metrics.forceEnable=true",
		"metrics.prometheusRule.policyModeRecoveryPointAgeSeconds=null",
	)

	var expr string
	for _, object := range renderMonitoring(t, sets...) {
		for _, group := range object.Spec.Groups {
			for _, rule := range group.Rules {
				if rule.Alert == "QumuloCSIReplicationRecoveryPointStale" {
					expr = rule.Expr
				}
			}
		}
	}
	require.NotEmpty(t, expr, "QumuloCSIReplicationRecoveryPointStale not rendered")
	assert.Contains(t, expr, "> 5400")
	assert.NotRegexp(t, `>\s*\n`, expr, "no comparison may be left without its threshold")
}

// No sidecar in the controller pod lists or watches pods, and the two
// that read a pod read their own. So no configuration may grant pods access
// cluster-wide: a ClusterRole with pods list/watch let the controller read
// every pod spec in the cluster, environment variables included.
func TestChartControllerHasNoClusterWidePodsAccess(t *testing.T) {
	for _, extra := range [][]string{
		nil,
		{"sidecars.capacity.enabled=true"},
		{"sidecars.csiAddons.enabled=true"},
		{"sidecars.capacity.enabled=true", "sidecars.csiAddons.enabled=true", "sidecars.snapshotter.enabled=true",
			"volumeSnapshotClasses[0].name=snaps", "volumeSnapshotClasses[0].secretName=qumulo-token"},
	} {
		t.Run(strings.Join(extra, ","), func(t *testing.T) {
			var podsInRole bool
			for _, doc := range renderDocs(t, baseSets(extra...)...) {
				var parsed renderedRBAC
				if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
					continue
				}
				for _, rule := range parsed.Rules {
					for _, resource := range rule.Resources {
						if resource != "pods" {
							continue
						}
						switch parsed.Kind {
						case "ClusterRole":
							t.Errorf("ClusterRole %s grants pods %v", parsed.Metadata.Name, rule.Verbs)
						case "Role":
							podsInRole = true
							assert.Equal(t, []string{"get"}, rule.Verbs, "a sidecar reads its own pod, nothing more")
						}
					}
				}
			}
			// The two sidecars that read their own pod still can.
			wantPods := strings.Contains(strings.Join(extra, ","), "capacity") ||
				strings.Contains(strings.Join(extra, ","), "csiAddons")
			assert.Equal(t, wantPods, podsInRole)
		})
	}
}

// Every class the chart renders carries helm.sh/resource-policy: keep,
// so an upgrade that renders none of them (`--set` without the values file)
// cannot delete them, and neither can an uninstall.
func TestChartClassesAreNeverDeletedByHelm(t *testing.T) {
	docs := renderDocs(t,
		"storageClasses[0].name=sc", "storageClasses[0].server=192.0.2.10",
		"storageClasses[0].secretName=tok", "storageClasses[0].basePath=/csi",
		"sidecars.snapshotter.enabled=true",
		"volumeSnapshotClasses[0].name=snaps", "volumeSnapshotClasses[0].secretName=tok",
		"sidecars.csiAddons.enabled=true",
		"volumeReplicationClasses[0].name=repl", "volumeReplicationClasses[0].targetServer=192.0.2.20",
		"volumeReplicationClasses[0].secretName=tok",
	)
	seen := map[string]bool{}
	for _, doc := range docs {
		var parsed struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name        string            `json:"name"`
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
			continue
		}
		switch parsed.Kind {
		case "StorageClass", "VolumeSnapshotClass", "VolumeReplicationClass":
			seen[parsed.Kind] = true
			assert.Equal(t, "keep", parsed.Metadata.Annotations["helm.sh/resource-policy"],
				"%s %s must carry helm.sh/resource-policy: keep", parsed.Kind, parsed.Metadata.Name)
		}
	}
	assert.Equal(t, map[string]bool{"StorageClass": true, "VolumeSnapshotClass": true, "VolumeReplicationClass": true}, seen)
}

// A dedicated-export class that names no allowedClients -- whose
// exports admit any client, as root, with no authentication -- is refused,
// by default, unless it says allowAnyClient: true or is a
// Kerberos class, whose export requires the flavour. requireAllowedClients:
// false renders it as before.
func TestChartRequireAllowedClientsRefusesAnOpenDedicatedClass(t *testing.T) {
	base := []string{
		"storageClasses[0].name=open", "storageClasses[0].server=192.0.2.10",
		"storageClasses[0].secretName=tok", "storageClasses[0].basePath=/csi",
		"storageClasses[0].provisioningModel=dedicated-export",
	}
	render := func(extra ...string) (string, error) {
		args := []string{"template", "test-release", chartPath}
		for _, set := range append(append([]string{}, base...), extra...) {
			args = append(args, "--set", set)
		}
		out, err := exec.Command("helm", args...).CombinedOutput()
		return string(out), err
	}

	for _, extra := range [][]string{nil, {"requireAllowedClients=true"}, {"storageClasses[0].securityFlavor=sys"}} {
		out, err := render(extra...)
		require.Error(t, err, "%v: an open dedicated class must be refused: %s", extra, out)
		assert.Contains(t, out, "storageClasses[open]")
		assert.Contains(t, out, "allowAnyClient")
	}

	// Every open class is named at once, not one per attempt.
	out, err := render("storageClasses[1].name=open2", "storageClasses[1].server=192.0.2.10",
		"storageClasses[1].secretName=tok", "storageClasses[1].basePath=/csi2",
		"storageClasses[1].provisioningModel=dedicated-export")
	require.Error(t, err)
	assert.Contains(t, out, "storageClasses[open, open2]")

	for _, extra := range [][]string{
		{"storageClasses[0].allowAnyClient=true"},
		{"storageClasses[0].allowedClients[0]=192.0.2.0/24"},
		{"storageClasses[0].protocol=nfs4\\.1", "storageClasses[0].securityFlavor=krb5p", "node.kerberos.enabled=true"},
		{"requireAllowedClients=false"},
	} {
		out, err := render(extra...)
		assert.NoError(t, err, "%v: %s", extra, out)
	}

	// The driver's half: the class carries the opt-in, the parsed
	// class reads it, and the controller's -require-allowed-clients follows
	// requireAllowedClients.
	out, err = render("storageClasses[0].allowAnyClient=true")
	require.NoError(t, err, out)
	assert.Contains(t, out, `allowAnyClient: "true"`)
	assert.Contains(t, out, "- -require-allowed-clients=true")
	classes := renderChart(t, "storageClasses[0].name=open", "storageClasses[0].server=192.0.2.10",
		"storageClasses[0].secretName=tok", "storageClasses[0].basePath=/csi",
		"storageClasses[0].provisioningModel=dedicated-export", "storageClasses[0].allowAnyClient=true")
	require.Len(t, classes, 1)
	parsed, err := parseVolumeParams(classes[0].Parameters)
	require.NoError(t, err)
	assert.True(t, parsed.AllowAnyClient)
	assert.NoError(t, openExportRefusal(parsed))

	out, err = render("requireAllowedClients=false")
	require.NoError(t, err, out)
	assert.Contains(t, out, "- -require-allowed-clients=false")
	assert.NotContains(t, out, "allowAnyClient:", "no opt-in is rendered for a class that did not ask")
}

// The chart refuses an http:// apiUrl, which would send the class's
// token in cleartext.
func TestChartRefusesAnHTTPApiURL(t *testing.T) {
	out, err := exec.Command("helm", "template", "test-release", chartPath,
		"--set", "storageClasses[0].name=sc", "--set", "storageClasses[0].server=192.0.2.10",
		"--set", "storageClasses[0].secretName=tok", "--set", "storageClasses[0].basePath=/csi",
		"--set", "storageClasses[0].apiUrl=http://192.0.2.10:8000").CombinedOutput()
	require.Error(t, err, "an http:// apiUrl must be refused: %s", out)
	assert.Contains(t, string(out), "must be an https:// URL")
}

// The controller pod is not BestEffort, controller.resources reaches
// the driver container, and the pod carries a PriorityClass.
func TestChartControllerPodIsNotBestEffort(t *testing.T) {
	controller := func(sets ...string) (map[string]map[string]string, string) {
		got, priority := map[string]map[string]string{}, ""
		for _, doc := range renderDocs(t, sets...) {
			var w struct {
				Kind string `yaml:"kind"`
				Spec struct {
					Template struct {
						Spec struct {
							PriorityClassName string `yaml:"priorityClassName"`
							Containers        []struct {
								Name      string `yaml:"name"`
								Resources struct {
									Requests map[string]string `yaml:"requests"`
								} `yaml:"resources"`
							} `yaml:"containers"`
						} `yaml:"spec"`
					} `yaml:"template"`
				} `yaml:"spec"`
			}
			if err := yaml.Unmarshal([]byte(doc), &w); err != nil || w.Kind != "Deployment" {
				continue
			}
			priority = w.Spec.Template.Spec.PriorityClassName
			for _, c := range w.Spec.Template.Spec.Containers {
				got[c.Name] = c.Resources.Requests
			}
		}
		return got, priority
	}

	byDefault, priority := controller(baseSets()...)
	assert.Equal(t, "system-cluster-critical", priority)
	for name, requests := range byDefault {
		assert.NotEmpty(t, requests["cpu"], "%s has no CPU request", name)
		assert.NotEmpty(t, requests["memory"], "%s has no memory request", name)
	}

	set, _ := controller(append(baseSets(), "controller.resources.requests.memory=256Mi")...)
	assert.Equal(t, "256Mi", set["qumulo-csi-driver"]["memory"], "controller.resources must reach the driver container")
	assert.Equal(t, "128Mi", set["liveness-probe"]["memory"], "the sidecars keep sidecarResources")
	assert.Equal(t, "512Mi", set["csi-provisioner"]["memory"], "csi-provisioner keeps provisionerResources")
	assert.Equal(t, "256Mi", set["csi-resizer"]["memory"], "csi-resizer keeps resizerResources")

	// csi-provisioner and csi-resizer cache every claim and PV: at the
	// shared 128Mi the provisioner was OOM-killed with 5,000 volumes and the
	// resizer reached the limit. Each has its own setting, which the other
	// sidecars' does not change.
	assert.Equal(t, "512Mi", byDefault["csi-provisioner"]["memory"], "csi-provisioner defaults above the other sidecars")
	assert.Equal(t, "256Mi", byDefault["csi-resizer"]["memory"], "csi-resizer defaults above the other sidecars")
	sidecars, _ := controller(append(baseSets(), "controller.sidecarResources.requests.memory=64Mi")...)
	assert.Equal(t, "64Mi", sidecars["liveness-probe"]["memory"])
	assert.Equal(t, "512Mi", sidecars["csi-provisioner"]["memory"], "sidecarResources does not reach csi-provisioner")
	assert.Equal(t, "256Mi", sidecars["csi-resizer"]["memory"], "sidecarResources does not reach csi-resizer")
	prov, _ := controller(append(baseSets(), "controller.provisionerResources.requests.memory=1Gi",
		"controller.resizerResources.requests.memory=384Mi")...)
	assert.Equal(t, "1Gi", prov["csi-provisioner"]["memory"], "controller.provisionerResources must reach csi-provisioner")
	assert.Equal(t, "384Mi", prov["csi-resizer"]["memory"], "controller.resizerResources must reach csi-resizer")
	assert.Equal(t, "128Mi", prov["liveness-probe"]["memory"], "and only those two")

	// A --reuse-values upgrade from a chart that predates these defaults:
	// controller.resources is {}, and sidecarResources, provisionerResources
	// and resizerResources are absent. The defaults still apply.
	reused, _ := controller(append(baseSets(), "controller.resources=null", "controller.sidecarResources=null",
		"controller.provisionerResources=null", "controller.resizerResources=null")...)
	assert.Equal(t, byDefault, reused)
}

// By default the controller runs two replicas, spread softly over
// nodes, under a PodDisruptionBudget; one replica renders no PDB, and the
// lease timings reach the sidecars.
func TestChartControllerIsHighlyAvailableByDefault(t *testing.T) {
	type rendered struct {
		replicas int
		pdb      bool
		anti     bool
		args     []string
	}
	render := func(sets ...string) rendered {
		var r rendered
		for _, doc := range renderDocs(t, sets...) {
			var w struct {
				Kind string `yaml:"kind"`
				Spec struct {
					Replicas int `yaml:"replicas"`
					Template struct {
						Spec struct {
							Affinity struct {
								PodAntiAffinity map[string]any `yaml:"podAntiAffinity"`
							} `yaml:"affinity"`
							Containers []struct {
								Args []string `yaml:"args"`
							} `yaml:"containers"`
						} `yaml:"spec"`
					} `yaml:"template"`
				} `yaml:"spec"`
			}
			if err := yaml.Unmarshal([]byte(doc), &w); err != nil {
				continue
			}
			switch w.Kind {
			case "PodDisruptionBudget":
				r.pdb = true
			case "Deployment":
				r.replicas, r.anti = w.Spec.Replicas, w.Spec.Template.Spec.Affinity.PodAntiAffinity != nil
				for _, c := range w.Spec.Template.Spec.Containers {
					r.args = append(r.args, c.Args...)
				}
			}
		}
		return r
	}

	def := render(baseSets()...)
	assert.Equal(t, 2, def.replicas)
	assert.True(t, def.pdb, "a PodDisruptionBudget by default")
	assert.True(t, def.anti, "a soft anti-affinity by default")

	one := render(append(baseSets(), "controller.replicaCount=1")...)
	assert.False(t, one.pdb, "with one replica a PDB would block every drain")

	timed := render(append(baseSets(), "controller.leaderElectionTimings.leaseDuration=30s")...)
	assert.Contains(t, timed.args, "--leader-election-lease-duration=30s")
}

// leaderElection: false is refused with more than one replica, renders
// Recreate rollouts with one, and csi-addons keeps its Lease rights either way.
func TestChartLeaderElectionOffIsSafe(t *testing.T) {
	out, err := exec.Command("helm", append([]string{"template", "test-release", chartPath},
		setArgs(append(baseSets(), "controller.leaderElection=false", "controller.replicaCount=2"))...)...).CombinedOutput()
	require.Error(t, err, "two un-elected controllers must be refused: %s", out)
	assert.Contains(t, string(out), "controller.leaderElection is false")

	docs := strings.Join(renderDocs(t, append(baseSets(), "controller.leaderElection=false", "controller.replicaCount=1",
		"sidecars.csiAddons.enabled=true")...), "\n---")
	assert.Contains(t, docs, "type: Recreate", "a rollout must not run two un-elected controllers")
	assert.Contains(t, docs, `apiGroups: ["coordination.k8s.io"]`, "csi-addons runs its own election and needs Lease rights")
}

// The controller may read exactly the Secrets the release names, one
// Role per namespace, unless rbac.clusterWideSecrets asks for get everywhere.
func TestChartControllerReadsOnlyTheNamedSecrets(t *testing.T) {
	type secretRule struct {
		kind, namespace string
		names           []string
	}
	rulesOf := func(sets ...string) []secretRule {
		var out []secretRule
		for _, doc := range renderDocs(t, sets...) {
			var r struct {
				Kind     string `yaml:"kind"`
				Metadata struct {
					Namespace string `yaml:"namespace"`
				} `yaml:"metadata"`
				Rules []struct {
					Resources     []string `yaml:"resources"`
					ResourceNames []string `yaml:"resourceNames"`
				} `yaml:"rules"`
			}
			if err := yaml.Unmarshal([]byte(doc), &r); err != nil || (r.Kind != "Role" && r.Kind != "ClusterRole") {
				continue
			}
			for _, rule := range r.Rules {
				if slices.Contains(rule.Resources, "secrets") {
					out = append(out, secretRule{r.Kind, r.Metadata.Namespace, rule.ResourceNames})
				}
			}
		}
		return out
	}

	sets := append(baseSets(), "storageClasses[1].name=other", "storageClasses[1].server=192.0.2.11",
		"storageClasses[1].basePath=/csi", "storageClasses[1].secretName=other-token",
		"storageClasses[1].secretNamespace=storage", "rbac.extraSecrets[0].secretName=legacy",
		"rbac.extraSecrets[0].secretNamespace=storage")
	rules := rulesOf(sets...)
	for _, r := range rules {
		assert.Equal(t, "Role", r.kind, "no cluster-wide Secret access by default")
		assert.NotEmpty(t, r.names, "a Secret rule must name its Secrets")
	}
	byNS := map[string][]string{}
	for _, r := range rules {
		byNS[r.namespace] = r.names
	}
	assert.Equal(t, []string{"legacy", "other-token"}, byNS["storage"])

	wide := rulesOf(append(sets, "rbac.clusterWideSecrets=true")...)
	require.Len(t, wide, 1)
	assert.Equal(t, "ClusterRole", wide[0].kind)
}

// The node rollout is gated on the kubelet registering the plugin: the
// driver writes a marker on the kubelet's first NodeGetInfo, the pod's
// readiness checks it, and a pod must stay Ready minReadySeconds before the
// rollout moves on -- also after a --reuse-values upgrade from a chart that
// still carries node.registrarHealthPort. The registrar's /healthz checks only
// its own socket, so it is never the registration signal: it backs only the
// registrar container's own liveness and readiness (Gatekeeper requiredprobes),
// on the node's address, and the pod is still not Ready until the
// driver's marker says the kubelet registered the plugin.
func TestChartNodeRolloutWaitsForRegistration(t *testing.T) {
	const marker = "/run/qumulo-csi/registered"
	for _, sets := range [][]string{baseSets(), append(baseSets(), "node.registrarHealthPort=29659", "node.minReadySeconds=null")} {
		var found bool
		for _, doc := range renderDocs(t, sets...) {
			var ds struct {
				Kind string `yaml:"kind"`
				Spec struct {
					MinReadySeconds int `yaml:"minReadySeconds"`
					Template        struct {
						Spec struct {
							Containers []struct {
								Name           string   `yaml:"name"`
								Args           []string `yaml:"args"`
								Ports          []any    `yaml:"ports"`
								ReadinessProbe *struct {
									Exec *struct {
										Command []string `yaml:"command"`
									} `yaml:"exec"`
									HTTPGet *struct {
										Path string `yaml:"path"`
										Port any    `yaml:"port"`
									} `yaml:"httpGet"`
								} `yaml:"readinessProbe"`
								VolumeMounts []struct {
									Name      string `yaml:"name"`
									MountPath string `yaml:"mountPath"`
								} `yaml:"volumeMounts"`
							} `yaml:"containers"`
							Volumes []struct {
								Name     string    `yaml:"name"`
								EmptyDir *struct{} `yaml:"emptyDir"`
							} `yaml:"volumes"`
						} `yaml:"spec"`
					} `yaml:"template"`
				} `yaml:"spec"`
			}
			if err := yaml.Unmarshal([]byte(doc), &ds); err != nil || ds.Kind != "DaemonSet" {
				continue
			}
			assert.Equal(t, 10, ds.Spec.MinReadySeconds)
			for _, c := range ds.Spec.Template.Spec.Containers {
				switch c.Name {
				case "node-driver-registrar":
					assert.Contains(t, c.Args, "--http-endpoint=$(POD_IP):29659",
						"the registrar's /healthz is a host port and must bind the node's own address")
					require.NotNil(t, c.ReadinessProbe)
					assert.Nil(t, c.ReadinessProbe.Exec, "the registrar's readiness is its own /healthz, not the registration marker")
					require.NotNil(t, c.ReadinessProbe.HTTPGet)
					assert.Equal(t, "/healthz", c.ReadinessProbe.HTTPGet.Path)
					assert.Equal(t, "registrar", c.ReadinessProbe.HTTPGet.Port, "the probe reads the port the container declares")
				case "qumulo-csi-driver":
					found = true
					assert.Contains(t, c.Args, "-registration-marker="+marker)
					require.NotNil(t, c.ReadinessProbe, "the driver must back the pod's readiness")
					require.NotNil(t, c.ReadinessProbe.Exec)
					assert.Equal(t, []string{"/qumulo-csi-driver", "-check-registration=" + marker}, c.ReadinessProbe.Exec.Command)
					var mounted bool
					for _, m := range c.VolumeMounts {
						mounted = mounted || (m.Name == "registration-state" && m.MountPath == "/run/qumulo-csi")
					}
					assert.True(t, mounted, "the marker must live in the pod-scoped volume")
				}
			}
			var emptyDir bool
			for _, v := range ds.Spec.Template.Spec.Volumes {
				emptyDir = emptyDir || (v.Name == "registration-state" && v.EmptyDir != nil)
			}
			assert.True(t, emptyDir, "registration-state must be an emptyDir: kept across a container restart, gone with the pod")
		}
		assert.True(t, found, "no node driver container rendered for %v", sets)
	}
}

// With Kerberos on, the host files are mounted FileOrCreate, so a node
// without them still runs the node plugin; File is still available.
func TestChartKerberosHostFilesDoNotBlockTheNodePlugin(t *testing.T) {
	typesOf := func(sets ...string) []string {
		var types []string
		for _, doc := range renderDocs(t, sets...) {
			var ds struct {
				Kind string `yaml:"kind"`
				Spec struct {
					Template struct {
						Spec struct {
							Volumes []struct {
								Name     string `yaml:"name"`
								HostPath *struct {
									Type string `yaml:"type"`
								} `yaml:"hostPath"`
							} `yaml:"volumes"`
						} `yaml:"spec"`
					} `yaml:"template"`
				} `yaml:"spec"`
			}
			if err := yaml.Unmarshal([]byte(doc), &ds); err != nil || ds.Kind != "DaemonSet" {
				continue
			}
			for _, v := range ds.Spec.Template.Spec.Volumes {
				if strings.HasPrefix(v.Name, "krb5-") && v.HostPath != nil {
					types = append(types, v.HostPath.Type)
				}
			}
		}
		return types
	}
	assert.Equal(t, []string{"FileOrCreate", "FileOrCreate"}, typesOf(append(baseSets(), "node.kerberos.enabled=true")...))
	assert.Equal(t, []string{"File", "File"}, typesOf(append(baseSets(), "node.kerberos.enabled=true", "node.kerberos.hostPathType=File")...))
}

// Two classes of one kind with one name are refused; Helm used to apply
// one of them and leave it labelled as its own for ever.
func TestChartRefusesDuplicateClassNames(t *testing.T) {
	out, err := exec.Command("helm", append([]string{"template", "test-release", chartPath}, setArgs(append(baseSets(),
		"storageClasses[1].name=qumulo-nfs", "storageClasses[1].server=192.0.2.11",
		"storageClasses[1].basePath=/other", "storageClasses[1].secretName=tok"))...)...).CombinedOutput()
	require.Error(t, err, "a duplicate class name must be refused: %s", out)
	assert.Contains(t, string(out), "is used twice")
}

// Each sidecar's workers, rate limits and retry intervals can be tuned
// through extraArgs, rendered verbatim after the chart's own arguments.
func TestChartSidecarsTakeExtraArgs(t *testing.T) {
	var args []string
	for _, w := range renderWorkloads(t, append(baseSets(),
		"sidecars.provisioner.extraArgs[0]=--kube-api-qps=50",
		"sidecars.resizer.extraArgs[0]=--workers=20",
		"sidecars.nodeDriverRegistrar.extraArgs[0]=--timeout=5s")...) {
		for _, c := range w.Spec.Template.Spec.Containers {
			args = append(args, c.Args...)
		}
	}
	for _, want := range []string{"--kube-api-qps=50", "--workers=20", "--timeout=5s"} {
		assert.Contains(t, args, want)
	}
}

// Each of these rendered and then failed later -- at start-up, at the
// first provision, or by weakening what it configures. They are refused at
// render, naming the value.
func TestChartRefusesSettingsThatRenderButBreak(t *testing.T) {
	render := func(extra ...string) (string, error) {
		args := []string{"template", "test-release", chartPath}
		for _, set := range baseSets(extra...) {
			args = append(args, "--set", set)
		}
		out, err := exec.Command("helm", args...).CombinedOutput()
		return string(out), err
	}

	for _, tc := range []struct {
		name string
		set  []string
		want string
	}{
		{"an apiTimeout that is not a duration crash-loops the controller",
			[]string{"qumulo.apiTimeout=ten"}, "apiTimeout"},
		{"a zero apiTimeout means no HTTP timeout at all",
			[]string{"qumulo.apiTimeout=0s"}, "apiTimeout"},
		{"a zero sidecar timeout fails the provisioner's start-up",
			[]string{"sidecars.provisioner.timeout=0s"}, "timeout"},
		{"a zero node unmount timeout skips the plain umount",
			[]string{"node.unmountTimeout=0s"}, "node.unmountTimeout"},
		{"a # in basePath leaves every claim Pending",
			[]string{"storageClasses[0].basePath=/csi/a#b"}, "basePath"},
		{"a # in exportBasePath too",
			[]string{"storageClasses[0].exportBasePath=/csi/a#b"}, "exportBasePath"},
		{"a metrics port on a liveness port",
			[]string{"metrics.enabled=true", "metrics.port.controller=29652"}, "29652"},
		{"two metrics ports alike",
			[]string{"metrics.enabled=true", "metrics.port.resizer=29656"}, "29656"},
		{"a health monitor without inventory fails every poll",
			[]string{"sidecars.healthMonitor.enabled=true"}, "controller.inventory.enabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := render(tc.set...)
			require.Error(t, err, "must be refused at render: %s", out)
			assert.Contains(t, out, tc.want)
		})
	}

	for _, ok := range [][]string{
		{"qumulo.apiTimeout=30s"},
		{"qumulo.apiTimeout=1m30s"},
		{"sidecars.provisioner.timeout=120s"},
		{"node.unmountTimeout=45s"},
		{"metrics.enabled=true"},
	} {
		out, err := render(ok...)
		assert.NoError(t, err, "%v: %s", ok, out)
	}
}

// The controller's metrics -- whose volume_handle labels named every
// volume's array and path, now a digest (pkg/metrics volumeID) -- were readable by any pod in any namespace, and
// the node plugin's listener bound every interface of the host network. The
// node listener now binds the pod's own IP (the node's, on hostNetwork),
// and metrics.networkPolicy renders a NetworkPolicy that admits the
// controller's metrics ports only from the peers it names. The policy keeps
// the livenessprobe and csi-addons ports open, which a bare deny-all would
// cut: the kubelet probes one, and the csi-addons controller dials the other.
func TestChartMetricsAreNotOpenToEveryPod(t *testing.T) {
	sets := baseSets("metrics.enabled=true", "sidecars.csiAddons.enabled=true")

	var nodeArgs []string
	for _, w := range renderWorkloads(t, sets...) {
		if w.Kind == "DaemonSet" {
			nodeArgs = driverContainerIn(t, w).Args
		}
	}
	assert.Contains(t, nodeArgs, "-metrics-address=$(POD_IP):29655", "the node listener binds the pod IP")

	policies := func(sets ...string) []string {
		var out []string
		for _, doc := range renderDocs(t, sets...) {
			if strings.Contains(doc, "kind: NetworkPolicy") {
				out = append(out, doc)
			}
		}
		return out
	}
	assert.Empty(t, policies(sets...), "off by default")

	got := policies(append(sets, "metrics.networkPolicy.enabled=true")...)
	require.Len(t, got, 1)
	var policy struct {
		Spec struct {
			PolicyTypes []string `yaml:"policyTypes"`
			Ingress     []struct {
				From  []map[string]any `yaml:"from"`
				Ports []struct {
					Port int `yaml:"port"`
				} `yaml:"ports"`
			} `yaml:"ingress"`
		} `yaml:"spec"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(got[0]), &policy))
	require.Equal(t, []string{"Ingress"}, policy.Spec.PolicyTypes)

	restricted, open := map[int]bool{}, map[int]bool{}
	for _, rule := range policy.Spec.Ingress {
		for _, p := range rule.Ports {
			if len(rule.From) == 0 {
				open[p.Port] = true
			} else {
				restricted[p.Port] = true
			}
		}
	}
	assert.Equal(t, map[int]bool{29654: true, 29656: true, 29657: true}, restricted,
		"the metrics ports are admitted only from the named peers")
	assert.Equal(t, map[int]bool{29652: true, 9070: true}, open,
		"the livenessprobe and csi-addons ports stay open")
}

// With preemptionPolicy: Never, a node plugin pod re-rolled onto a
// packed node stays Pending -- the node then has no plugin, and no Qumulo
// volume on it can mount or unmount. Never stays the default (the
// plugin does not evict workloads to fit), and PreemptLowerPriority, what
// system-node-critical has, can now be chosen.
func TestChartNodePriorityClassMayPreempt(t *testing.T) {
	policyOf := func(sets ...string) string {
		for _, doc := range renderDocs(t, sets...) {
			if !strings.Contains(doc, "kind: PriorityClass") {
				continue
			}
			var pc struct {
				PreemptionPolicy string `yaml:"preemptionPolicy"`
			}
			require.NoError(t, yaml.Unmarshal([]byte(doc), &pc))
			return pc.PreemptionPolicy
		}
		t.Fatal("no PriorityClass rendered")
		return ""
	}
	assert.Equal(t, "Never", policyOf(baseSets()...))
	assert.Equal(t, "PreemptLowerPriority", policyOf(baseSets("node.priorityClass.preemptionPolicy=PreemptLowerPriority")...))

	out, err := exec.Command("helm", append([]string{"template", "test-release", chartPath},
		setArgs(baseSets("node.priorityClass.preemptionPolicy=Sometimes"))...)...).CombinedOutput()
	require.Error(t, err, "only the two Kubernetes policies: %s", out)
}

// The health monitor is opt-in, needs inventory (the RBAC it uses is
// the controller's own), and is off by default.
func TestChartHealthMonitorIsOptIn(t *testing.T) {
	find := func(sets ...string) *renderedContainer {
		for _, w := range renderWorkloads(t, baseSets(sets...)...) {
			if w.Kind != "Deployment" {
				continue
			}
			for i := range w.Spec.Template.Spec.Containers {
				if w.Spec.Template.Spec.Containers[i].Name == "csi-external-health-monitor-controller" {
					return &w.Spec.Template.Spec.Containers[i]
				}
			}
		}
		return nil
	}

	assert.Nil(t, find(), "off by default")

	monitor := find("sidecars.healthMonitor.enabled=true", "controller.inventory.enabled=true",
		"sidecars.healthMonitor.monitorInterval=7m")
	require.NotNil(t, monitor)
	assert.Contains(t, monitor.Args, "--csi-address=$(ADDRESS)")
	assert.Contains(t, monitor.Args, "--monitor-interval=7m")
	assert.Contains(t, monitor.Args, "--leader-election")
}

// DeleteVolume lists this driver's PersistentVolumes before it removes
// anything, on every install -- not only with inventory or metrics on -- so a
// default render must let the controller's own ServiceAccount list them. A
// missing grant would not fail an install: every delete would answer
// Unavailable and every reclaimed PV would stay Released.
func TestChartDefaultInstallLetsTheControllerListPersistentVolumes(t *testing.T) {
	requireControllerMayList(t, "persistentvolumes")
}

// DeleteVolume and CreateVolume's sweep list every claim before they
// remove a restore's staging directory, on every install; without the grant
// every list fails, and a failed list removes nothing, so leftovers stay.
func TestChartDefaultInstallLetsTheControllerListPersistentVolumeClaims(t *testing.T) {
	requireControllerMayList(t, "persistentvolumeclaims")
}

// requireControllerMayList fails unless a ClusterRole bound to the
// controller Deployment's ServiceAccount grants list on the core resource in
// a default render. Every container in the pod runs as that account, the
// driver's included.
func requireControllerMayList(t *testing.T, resource string) {
	t.Helper()
	type binding struct {
		Kind     string `yaml:"kind"`
		Subjects []struct {
			Kind string `yaml:"kind"`
			Name string `yaml:"name"`
		} `yaml:"subjects"`
		RoleRef struct {
			Kind string `yaml:"kind"`
			Name string `yaml:"name"`
		} `yaml:"roleRef"`
	}
	type deployment struct {
		Kind string `yaml:"kind"`
		Spec struct {
			Template struct {
				Spec struct {
					ServiceAccountName string `yaml:"serviceAccountName"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}

	docs := renderDocs(t, baseSets()...)
	var controllerSA string
	clusterRoles := map[string]renderedRBAC{}
	var bindings []binding
	for _, doc := range docs {
		var d deployment
		if yaml.Unmarshal([]byte(doc), &d) == nil && d.Kind == "Deployment" {
			controllerSA = d.Spec.Template.Spec.ServiceAccountName
		}
		var r renderedRBAC
		if yaml.Unmarshal([]byte(doc), &r) == nil && r.Kind == "ClusterRole" {
			clusterRoles[r.Metadata.Name] = r
		}
		var b binding
		if yaml.Unmarshal([]byte(doc), &b) == nil && b.Kind == "ClusterRoleBinding" {
			bindings = append(bindings, b)
		}
	}
	require.NotEmpty(t, controllerSA, "the default render has no controller Deployment with a ServiceAccount")

	for _, b := range bindings {
		bound := false
		for _, s := range b.Subjects {
			bound = bound || (s.Kind == "ServiceAccount" && s.Name == controllerSA)
		}
		if !bound || b.RoleRef.Kind != "ClusterRole" {
			continue
		}
		for _, rule := range clusterRoles[b.RoleRef.Name].Rules {
			if slices.Contains(rule.APIGroups, "") && slices.Contains(rule.Resources, resource) &&
				(slices.Contains(rule.Verbs, "list") || slices.Contains(rule.Verbs, "*")) {
				return
			}
		}
	}
	t.Fatalf("no ClusterRole bound to the controller's ServiceAccount %q grants list on %s "+
		"in a default render", controllerSA, resource)
}
