//go:build chart

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

// Package chart holds golden-file tests for the Helm chart.
package chart

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite the golden files instead of comparing against them")

const chartPath = "../../charts/qumulo-csi-driver"

// fixtures are the value sets whose rendered output is committed.
//
// They are chosen to cover the branches in the templates rather than to be
// exhaustive: the minimum that renders, everything switched on at once, and the
// bring-your-own-Secret path, which is the one that changes which objects exist.
var fixtures = []struct {
	name string
	sets []string
}{
	{
		name: "minimal",
		sets: []string{},
	},
	{
		name: "full",
		sets: []string{
			// Per class, not chart-wide: qumulo.apiUrl was set here for a
			// long time and did nothing, which closing the schema is what
			// caught. storageClasses[0] carries it below.
			`storageClasses[0].apiUrl=https://qumulo\.example\.com:8000`,
			"qumulo.insecureSkipTlsVerify=true",
			"fsGroupPolicy=File",
			"controller.replicaCount=2",
			"node.kubeletDir=/var/lib/k0s/kubelet",
			"storageClasses[0].name=qumulo-nfs",
			"storageClasses[0].server=qumulo.example.com",
			"storageClasses[0].secretName=qumulo-token",
			"storageClasses[0].basePath=/csi/volumes",
			"storageClasses[0].protocol=nfs4\\.1",
			"storageClasses[0].isDefault=true",
			"storageClasses[0].mountOptions[0]=hard",
			"storageClasses[1].name=qumulo-dedicated",
			"storageClasses[1].server=qumulo.example.com",
			"storageClasses[1].secretName=qumulo-token",
			"storageClasses[1].basePath=/csi/volumes",
			"storageClasses[1].exportBasePath=/exports/csi",
			"storageClasses[1].protocol=nfs3",
			"storageClasses[1].provisioningModel=dedicated-export",
			"storageClasses[1].allowAnyClient=true",
			"storageClasses[1].reclaimPolicy=Retain",
			"storageClasses[1].allowVolumeExpansion=false",
			"storageClasses[1].mountPermissions=0770",
			// The two parameters with their own template branch. Pinned here
			// so a change to either branch shows up as a reviewable diff.
			"storageClasses[2].name=qumulo-corp",
			"storageClasses[2].server=qumulo.example.com",
			"storageClasses[2].secretName=qumulo-token",
			"storageClasses[2].basePath=/csi/volumes",
			"storageClasses[2].protocol=nfs4\\.1",
			"storageClasses[2].adDomain=corp\\.example\\.com",
			"storageClasses[2].snapshotSchedule=daily@02:00\\,keep=7days",
		},
	},
	{
		// A private CA. The controller must gain the volume, the mount and
		// SSL_CERT_FILE; the node must gain none of them, because it never
		// calls the REST API.
		name: "ca-bundle",
		sets: []string{
			"qumulo.insecureSkipTlsVerify=false",
			"qumulo.caBundle.existingConfigMap=qumulo-ca",
			"storageClasses[0].name=qumulo-nfs",
			"storageClasses[0].server=qumulo.example.com",
			"storageClasses[0].secretName=qumulo-token",
			"storageClasses[0].basePath=/csi/volumes",
		},
	},
	{
		// The only way a credential reaches the driver now: named per class and
		// rendered into the sidecar secret parameters.
		name: "existing-secret",
		sets: []string{
			"storageClasses[0].name=qumulo-nfs",
			"storageClasses[0].server=qumulo.example.com",
			"storageClasses[0].secretName=my-qumulo-token",
			"storageClasses[0].secretNamespace=other-ns",
			"storageClasses[0].basePath=/csi/volumes",
		},
	},
	{
		// Two clusters from one release: each class names its own server,
		// endpoint and Secret, and the chart renders the sidecar secret
		// parameters that carry the per-cluster credential into each RPC.
		name: "multicluster",
		sets: []string{
			"storageClasses[0].name=qumulo-a",
			"storageClasses[0].server=cluster-a.example.com",
			"storageClasses[0].secretName=qumulo-cluster-a",
			"storageClasses[0].basePath=/csi/a",
			"storageClasses[0].protocol=nfs3",
			"storageClasses[0].mountOptions[0]=nolock",
			"storageClasses[1].name=qumulo-b",
			"storageClasses[1].basePath=/csi/b",
			"storageClasses[1].server=cluster-b.example.com",
			"storageClasses[1].apiUrl=https://cluster-b.example.com:9000",
			"storageClasses[1].secretName=qumulo-cluster-b",
			"storageClasses[1].protocol=nfs4\\.1",
		},
	},
	{
		// Capacity reporting on, across two clusters -- the configuration the
		// feature exists for. Four separate pieces of output have to agree:
		// --enable-capacity on the provisioner, -enable-capacity on the driver,
		// the csistoragecapacities RBAC rule, and CSIDriver.storageCapacity.
		// Any three without the fourth fails silently, so the whole rendering
		// is worth a committed diff.
		name: "capacity",
		sets: []string{
			"sidecars.capacity.enabled=true",
			"storageClasses[0].name=qumulo-a",
			"storageClasses[0].server=cluster-a.example.com",
			"storageClasses[0].secretName=qumulo-cluster-a",
			"storageClasses[0].basePath=/csi/a",
			"storageClasses[1].name=qumulo-b",
			"storageClasses[1].server=cluster-b.example.com",
			"storageClasses[1].secretName=qumulo-cluster-b",
			"storageClasses[1].basePath=/csi/b",
		},
	},
	{
		// Volume inventory on, across two clusters. ListVolumes spans both, so
		// this is the rendering that matters: one driver flag, and the RBAC it
		// leans on, which is unconditional because the sidecars already need it.
		name: "inventory",
		sets: []string{
			"controller.inventory.enabled=true",
			"storageClasses[0].name=qumulo-a",
			"storageClasses[0].server=cluster-a.example.com",
			"storageClasses[0].secretName=qumulo-cluster-a",
			"storageClasses[0].basePath=/csi/a",
			"storageClasses[1].name=qumulo-b",
			"storageClasses[1].server=cluster-b.example.com",
			"storageClasses[1].secretName=qumulo-cluster-b",
			"storageClasses[1].basePath=/csi/b",
		},
	},
	{
		// OpenShift: the node plugin's own SCC and its RBAC grant, both pods
		// pinned to their SCC, and the controller without its fixed uid,
		// which restricted-v2 refuses. enabled=true stands in for a cluster
		// serving SecurityContextConstraints, which a render cannot see.
		name: "openshift",
		sets: []string{
			"openshift.enabled=true",
		},
	},
	{
		// Snapshot support on: the sidecar, its RBAC, and a snapshot class
		// carrying its own per-cluster credential.
		name: "snapshots",
		sets: []string{
			"sidecars.snapshotter.enabled=true",
			"volumeSnapshotClasses[0].name=qumulo-snapshots",
			"volumeSnapshotClasses[0].secretName=qumulo-token",
			"volumeSnapshotClasses[0].deletionPolicy=Retain",
		},
	},
	{
		// Metrics on, with everything the feature can render: the driver's
		// own listener in both workloads, --http-endpoint on all three CSI
		// sidecars, both Services, the ServiceMonitor and the PrometheusRule.
		// forceEnable is what lets the last two render here at all -- `helm
		// template` has no cluster to ask whether the Prometheus Operator's
		// CRD exists, and the guard on that CRD is the point of the template.
		// The snapshotter is on because its metrics port is the one
		// conditional port in the Service.
		name: "metrics",
		sets: []string{
			"metrics.enabled=true",
			"metrics.forceEnable=true",
			"metrics.serviceMonitor.enabled=true",
			"metrics.prometheusRule.enabled=true",
			"sidecars.snapshotter.enabled=true",
			"storageClasses[0].name=qumulo-nfs",
			"storageClasses[0].server=qumulo.example.com",
			"storageClasses[0].secretName=qumulo-token",
			"storageClasses[0].basePath=/csi/volumes",
		},
	},
	{
		// The csi-addons replication service on: the sidecar, its RBAC,
		// -enable-replication on the driver, and a replication class naming
		// the peer array. Four pieces that have to agree, like capacity.
		name: "csi-addons",
		sets: []string{
			"sidecars.csiAddons.enabled=true",
			"storageClasses[0].name=qumulo-nfs",
			"storageClasses[0].server=cluster-a.example.com",
			"storageClasses[0].secretName=qumulo-cluster-a",
			"storageClasses[0].basePath=/csi/volumes",
			"storageClasses[0].provisioningModel=dedicated-export",
			"storageClasses[0].allowAnyClient=true",
			"volumeReplicationClasses[0].name=qumulo-to-b",
			"volumeReplicationClasses[0].targetServer=cluster-b.example.com",
			`volumeReplicationClasses[0].targetApiUrl=https://cluster-b\.example\.com:8000`,
			// Both arrays are DNS-named here, so both fixed endpoints are
			// required: a name that floats at failover must not
			// be what the controller dials.
			`volumeReplicationClasses[0].apiUrl=https://cluster-a\.example\.com:8000`,
			"volumeReplicationClasses[0].replicationMode=policy+continuous",
			"volumeReplicationClasses[0].linkSnapshotPolicy=true",
			"volumeReplicationClasses[0].schedulingInterval=5m",
			"volumeReplicationClasses[0].secretName=qumulo-replication-tokens",
		},
	},
}

func TestChartGolden(t *testing.T) {
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			args := []string{"template", "qumulo-csi", chartPath, "--namespace", "qumulo-csi"}
			for _, set := range fixture.sets {
				args = append(args, "--set", set)
			}

			rendered, err := exec.Command("helm", args...).CombinedOutput()
			require.NoError(t, err, "helm template failed: %s", rendered)

			goldenPath := filepath.Join("golden", fixture.name+".yaml")

			if *update {
				require.NoError(t, os.WriteFile(goldenPath, rendered, 0o644))
				t.Logf("wrote %s (%d bytes)", goldenPath, len(rendered))
				return
			}

			want, err := os.ReadFile(goldenPath)
			require.NoError(t, err, "missing golden file; run: make helm-golden-update")

			assert.Equal(t, string(want), string(rendered),
				"rendered output differs from %s. If the change is intended, run "+
					"`make helm-golden-update` and review the diff.", goldenPath)
		})
	}
}

// TestChartKubeVersionAcceptsDistributionSuffixes pins the "-0" in the chart's
// kubeVersion floor.
//
// Managed distributions report a version with a build suffix, and semver orders
// any prerelease below the release it qualifies -- so a bare ">=1.36.0" refused
// every EKS 1.36 cluster while `helm template` with no --kube-version passed,
// which is why it reached a customer. Both halves are asserted here: the
// suffixed versions must render, and a genuinely too-old cluster must still be
// refused, because widening the range far enough to admit the first is only
// correct if it does not also admit the second.
//
// The floor moved from 1.36.0 to 1.35.0 on 2026-10-13 (a customer on 1.35.6),
// so the boundary these cases guard moved down one minor with it: 1.35.x now
// renders and 1.34.x is the first version refused. The suffixed-version half
// is asserted at the new floor too, since that is where an off-by-one in the
// "-0" handling would now show up.
func TestChartKubeVersionAcceptsDistributionSuffixes(t *testing.T) {
	cases := []struct {
		version string
		accept  bool
	}{
		// The reported failure, verbatim.
		{version: "v1.36.3-eks-cb19647", accept: true},
		{version: "v1.38.1-gke.1234000", accept: true},
		{version: "v1.36.0+rke2r1", accept: true},
		{version: "v1.36.0", accept: true},
		{version: "v1.40.2", accept: true},
		// The 1.35 floor: the customer's own version, and the same
		// suffix shapes at the boundary that now matters.
		{version: "v1.35.6", accept: true},
		{version: "v1.35.6-eks-cb19647", accept: true},
		{version: "v1.35.0+rke2r1", accept: true},
		{version: "v1.35.0", accept: true},
		// Still refused: the floor dropped one minor, it did not vanish.
		{version: "v1.34.0", accept: false},
		{version: "v1.34.4-eks-cb19647", accept: false},
		{version: "v1.26.0", accept: false},
	}

	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			out, err := exec.Command("helm", "template", "qumulo-csi", chartPath,
				"--namespace", "qumulo-csi", "--kube-version", tc.version).CombinedOutput()

			if tc.accept {
				require.NoError(t, err, "chart refused a supported version: %s", out)
				return
			}
			require.Error(t, err, "chart accepted a version below its floor")
			assert.Contains(t, string(out), "kubeVersion",
				"refusal should name kubeVersion so the operator knows what to change")
		})
	}
}

// The driver refuses an unrecognised CSI parameter at CreateVolume, but the
// chart used to drop an unrecognised *Helm* key silently: `protocl: nfs4.1`
// rendered a class with no protocol and `helm upgrade` exited 0, so the
// operator got a class that was not what they wrote and no signal at all.
func TestValuesSchemaRejectsUnknownClassKeys(t *testing.T) {
	cases := []struct {
		name   string
		values string
		accept bool
	}{
		{
			name: "a correct class still renders",
			values: `storageClasses:
  - name: ok
    server: qumulo.example.com
    secretName: tok
    basePath: /csi
    protocol: nfs4.1
`,
			accept: true,
		},
		{
			// The two typos named in the report.
			name: "a misspelled protocol is refused",
			values: `storageClasses:
  - name: typo
    server: qumulo.example.com
    secretName: tok
    basePath: /csi
    protocl: nfs4.1
`,
		},
		{
			name: "a misspelled basePath is refused",
			values: `storageClasses:
  - name: typo
    server: qumulo.example.com
    secretName: tok
    basePath: /csi
    baseePath: /elsewhere
`,
		},
		{
			// Removed keys are the same failure with a worse cause: values
			// files copied from an older release carry them.
			name: "a removed key is refused rather than ignored",
			values: `storageClasses:
  - name: stale
    server: qumulo.example.com
    secretName: tok
    basePath: /csi
    qumuloClusterAddress: qumulo.example.com
`,
		},
		{
			name: "an unknown snapshot-class key is refused",
			values: `volumeSnapshotClasses:
  - name: snaps
    secretName: tok
    deletionPolicey: Delete
`,
		},
		{
			// The level above the class lists, which stayed open long after
			// they were closed. A typo here renders a chart that ignores the
			// setting entirely, and `helm upgrade` exits 0.
			name: "an unknown top-level key is refused",
			values: `fsGrupPolicy: File
`,
		},
		{
			// The worst of the top-level cases, and why this is not merely
			// hygiene: inventory is off by default, so a typo leaves the
			// operator believing they enabled ListVolumes when the controller
			// will keep answering FailedPrecondition.
			name: "an unknown controller key is refused",
			values: `controller:
  inventroy:
    enabled: true
`,
		},
		{
			name: "an unknown node key is refused",
			values: `node:
  kubletDir: /var/lib/kubelet
`,
		},
		{
			// Sidecar image pins are the values most often copied between
			// releases, so a stale or misspelled key here is likely.
			name: "an unknown sidecar key is refused",
			values: `sidecars:
  provisioner:
    imagee: registry.k8s.io/sig-storage/csi-provisioner:v5.3.0
`,
		},
		{
			name: "an unknown image key is refused",
			values: `image:
  controller:
    repostory: somewhere/else
`,
		},
		{
			// The counterpart the closed root must not break: every key
			// values.yaml itself ships has to remain settable, including the
			// free-form Kubernetes blocks that are deliberately left open
			// because the API server, not this chart, defines their shape.
			name:   "the pass-through Kubernetes blocks stay settable",
			accept: true,
			values: `nameOverride: qcsi
fullnameOverride: ""
commonLabels:
  app.kubernetes.io/part-of: storage
image:
  controller:
    repository: registry.example.com/qumulo-csi-driver-controller
    tag: "0.1.0"
    pullPolicy: Always
  pullSecrets:
    - name: regcred
sidecars:
  snapshotter:
    enabled: true
    timeout: 90s
controller:
  inventory:
    enabled: true
  resources:
    limits: {cpu: 500m, memory: 256Mi}
  nodeSelector: {kubernetes.io/os: linux}
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - {key: kubernetes.io/arch, operator: In, values: [amd64]}
  podAnnotations: {prometheus.io/scrape: "false"}
node:
  kerberos:
    enabled: false
    keytabPath: /etc/krb5.keytab
  resources: {}
  podAnnotations: {}
storageClasses:
  - name: qumulo-nfs
    server: qumulo.example.com
    secretName: tok
    basePath: /csi/volumes
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "values.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.values), 0o644))

			out, err := exec.Command("helm", "template", "qumulo-csi", chartPath,
				"--namespace", "qumulo-csi", "--values", path).CombinedOutput()

			if tc.accept {
				require.NoError(t, err, "chart refused a valid values file: %s", out)
				return
			}
			require.Error(t, err, "chart accepted an unknown key: %s", out)
			assert.Contains(t, string(out), "additional properties",
				"the message must name the offending key, which is the whole point")
		})
	}
}

// schemaAdditional decodes a JSON Schema "additionalProperties", which the
// spec allows to be either a boolean or a nested schema.
//
// Both spellings are in this chart's schema. The closed objects say `false`;
// commonLabels says `{"type": "string"}`, because it is a free-form map whose
// keys cannot be enumerated but whose values must be strings -- an unquoted
// number there renders a label value Kubernetes rejects. Decoding this as
// *bool worked while `false` was the only form in the file and then failed the
// whole unmarshal the moment the second one appeared, taking every unrelated
// assertion in these tests with it. So the question the guards ask is "is this
// object closed?", which both forms can answer, rather than "is this field
// false?", which only one of them can.
type schemaAdditional struct {
	present bool
	closed  bool
}

func (a *schemaAdditional) UnmarshalJSON(raw []byte) error {
	a.present = true

	var asBool bool
	if err := json.Unmarshal(raw, &asBool); err == nil {
		a.closed = !asBool
		return nil
	}

	// A nested schema constrains the extra keys rather than forbidding them,
	// so the object is open. Not an error: it is a legitimate spelling that
	// these particular guards simply do not apply to.
	a.closed = false
	return nil
}

// Present reports whether the schema stated additionalProperties at all.
func (a schemaAdditional) Present() bool { return a.present }

// Closed reports whether the object refuses keys its properties do not name.
func (a schemaAdditional) Closed() bool { return a.present && a.closed }

// TestValuesSchemaCoversEveryKeyTheTemplatesRead is the guard that makes
// closing the schema safe to keep.
//
// With additionalProperties: false, a key the templates read but the schema
// omits is no longer a missing type check -- it is a hard install failure for
// anyone using that feature. Four keys were in exactly that state when the
// object was closed (adDomain, allowedClients, securityFlavor, tenantID), all
// of them shipped features with their own README sections. This asserts the
// two never diverge again, in both directions: a schema key no template reads
// is either a typo in the schema or a feature that was removed from the
// templates and left advertised.
func TestValuesSchemaCoversEveryKeyTheTemplatesRead(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(chartPath, "values.schema.json"))
	require.NoError(t, err)

	var schema struct {
		Properties map[string]struct {
			// Set on the object itself (qumulo), or on its items (the class
			// lists). Only one of the two is ever populated per key.
			AdditionalProperties schemaAdditional           `json:"additionalProperties"`
			Properties           map[string]json.RawMessage `json:"properties"`
			Items                struct {
				AdditionalProperties schemaAdditional           `json:"additionalProperties"`
				Properties           map[string]json.RawMessage `json:"properties"`
			} `json:"items"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(raw, &schema))

	// $sc is the loop variable both templates use for one storage class.
	reference := regexp.MustCompile(`\$sc\.([A-Za-z][A-Za-z0-9]*)`)
	read := map[string]bool{}
	for _, name := range []string{"templates/storageclass.yaml", "templates/_validate.tpl"} {
		body, err := os.ReadFile(filepath.Join(chartPath, name))
		require.NoError(t, err)
		for _, match := range reference.FindAllStringSubmatch(string(body), -1) {
			read[match[1]] = true
		}
	}
	require.NotEmpty(t, read, "found no $sc references; the regexp or the templates moved")

	classes := schema.Properties["storageClasses"].Items
	require.True(t, classes.AdditionalProperties.Present(),
		"storageClasses[] must state additionalProperties explicitly")
	assert.True(t, classes.AdditionalProperties.Closed(),
		"storageClasses[] must be closed, or a typo renders a class nobody asked for")

	var missing, unread []string
	for key := range read {
		if _, ok := classes.Properties[key]; !ok {
			missing = append(missing, key)
		}
	}
	for key := range classes.Properties {
		if !read[key] {
			unread = append(unread, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(unread)

	assert.Empty(t, missing,
		"these keys are read by the templates but absent from the schema, so a values file "+
			"using them now fails to install: %v", missing)
	assert.Empty(t, unread,
		"these schema keys are read by no template, so the schema advertises what the chart "+
			"ignores: %v", unread)
}

// TestValuesSchemaCoversEveryQumuloKey is the same guard as above, for the
// .qumulo object.
//
// Closing .qumulo had to wait: deploy-values.yaml carried three
// keys the chart had already removed (clusterAddress, token, managedPaths), so
// closing it earlier would have failed the repo's own values file. And
// caBundle was read by two templates while absent from the schema, which is
// the same trap the class lists were in -- with the object closed, that is an
// install failure for anyone supplying a CA rather than a missing type check.
func TestValuesSchemaCoversEveryQumuloKey(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(chartPath, "values.schema.json"))
	require.NoError(t, err)

	var schema struct {
		Properties map[string]struct {
			AdditionalProperties schemaAdditional           `json:"additionalProperties"`
			Properties           map[string]json.RawMessage `json:"properties"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(raw, &schema))

	// Every .Values.qumulo.<key> the templates dereference, from all of them
	// rather than a listed few: a new template reading a new key is exactly
	// the case this has to catch.
	reference := regexp.MustCompile(`\.Values\.qumulo\.([A-Za-z][A-Za-z0-9]*)`)
	entries, err := os.ReadDir(filepath.Join(chartPath, "templates"))
	require.NoError(t, err)

	read := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		body, err := os.ReadFile(filepath.Join(chartPath, "templates", entry.Name()))
		require.NoError(t, err)
		for _, match := range reference.FindAllStringSubmatch(string(body), -1) {
			read[match[1]] = true
		}
	}
	require.NotEmpty(t, read, "found no .Values.qumulo references; the regexp or the templates moved")

	qumulo := schema.Properties["qumulo"]
	require.True(t, qumulo.AdditionalProperties.Present(),
		"qumulo must state additionalProperties explicitly")
	assert.True(t, qumulo.AdditionalProperties.Closed(),
		"qumulo must be closed, or a removed key such as clusterAddress is accepted and ignored")

	var missing []string
	for key := range read {
		if _, ok := qumulo.Properties[key]; !ok {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	assert.Empty(t, missing,
		"these qumulo keys are read by the templates but absent from the schema, so a values "+
			"file using them now fails to install: %v", missing)

	// The other direction, stated as the specific regression it guards: a key
	// removed from the templates but left in the schema is how clusterAddress
	// stayed in three documents and a shipped values file after it stopped
	// doing anything.
	var unread []string
	for key := range qumulo.Properties {
		if !read[key] {
			unread = append(unread, key)
		}
	}
	sort.Strings(unread)
	assert.Empty(t, unread,
		"these qumulo keys are in the schema but read by no template, so the chart advertises "+
			"settings it ignores: %v", unread)
}

// TestDocumentedValuesExamplesInstall renders every chart-values example in
// the published documentation.
//
// This is an acceptance criterion turned into a gate. The reported defect was
// docs naming keys the chart had removed, and the acceptance was "a reader
// following the README alone produces a working install" -- which nobody can
// check by reading, and which was false in eight places at once: two install
// commands and six values examples, several of them omitting `server` and
// `secretName` after those became required. Greping for removed key names
// finds some of it; actually rendering the examples finds all of it, including
// keys that were never named because they were simply missing.
//
// A block is treated as chart values when it starts with a top-level chart
// key. A block that is deliberately invalid -- the README shows one, the
// `storageClasses: []` mistake -- says so in a comment and is skipped.
func TestDocumentedValuesExamplesInstall(t *testing.T) {
	docs := []string{"../../README.md", filepath.Join(chartPath, "README.md")}
	found, err := filepath.Glob("../../deploy/*/*.md")
	require.NoError(t, err)
	docs = append(docs, found...)
	nested, err := filepath.Glob("../../deploy/*/*/*.md")
	require.NoError(t, err)
	docs = append(docs, nested...)

	// Top-level keys that make a fenced yaml block a values file rather than a
	// manifest or a fragment.
	valuesBlock := regexp.MustCompile(
		`^(storageClasses|volumeSnapshotClasses|volumeReplicationClasses|qumulo|controller|node|image|sidecars|fsGroupPolicy):`)
	fenced := regexp.MustCompile("(?s)```yaml\n(.*?)```")

	checked := 0
	for _, doc := range docs {
		body, err := os.ReadFile(doc)
		require.NoError(t, err)

		for i, match := range fenced.FindAllStringSubmatch(string(body), -1) {
			block := match[1]
			if strings.Contains(block, "INVALID ON PURPOSE") {
				continue
			}
			if !valuesBlock.MatchString(strings.TrimSpace(block)) {
				continue
			}

			checked++
			t.Run(fmt.Sprintf("%s#%d", filepath.Base(doc), i), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "values.yaml")
				require.NoError(t, os.WriteFile(path, []byte(block), 0o644))

				out, err := exec.Command("helm", "template", "qumulo-csi", chartPath,
					"--namespace", "qumulo-csi", "--values", path).CombinedOutput()
				require.NoError(t, err,
					"a values example in %s does not install:\n%s\n--- the example ---\n%s",
					doc, out, block)
			})
		}
	}

	// A regexp that matches nothing would make this test pass vacuously, which
	// is the failure mode a docs test is most likely to have.
	assert.GreaterOrEqual(t, checked, 8,
		"expected to find at least 8 values examples across the docs, found %d -- "+
			"the fence or key regexp probably stopped matching", checked)
}
