// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The whole command, end to end, against manifests on disk and an HTTP stand-in
// for the peer array. No cluster: `-f` is the path an operator takes during an
// incident, from a laptop, with `kubectl get pv -o yaml` saved to a file.

const testToken = "super-secret-bearer-token-value"

// fakePeerArray serves the one endpoint mirror needs, and records the
// Authorization header so the token handling can be asserted rather than
// assumed.
func fakePeerArray(t *testing.T, fileIDs map[string]string) (*httptest.Server, *[]string) {
	t.Helper()

	var auths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auths = append(auths, r.Header.Get("Authorization"))

		// `GET /v1/files/<percent-encoded path>/info/attributes`, exactly as
		// the driver's own client spells it.
		// EscapedPath, not Path: the driver's client percent-encodes the
		// cluster path into one path segment, and this test pins that it does.
		path := strings.TrimPrefix(r.URL.EscapedPath(), "/v1/files/")
		path = strings.TrimSuffix(path, "/info/attributes")

		id, ok := fileIDs[path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error_class":"fs_no_such_entry_error"}`)
			return
		}
		fmt.Fprintf(w, `{"id":%q,"path":%q,"type":"FS_FILE_TYPE_DIRECTORY"}`, id, path)
	}))
	t.Cleanup(server.Close)

	return server, &auths
}

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

const sourceManifest = `
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pvc-abc123
spec:
  capacity:
    storage: 10Gi
  accessModes: [ReadWriteMany]
  mountOptions: [hard, "timeo=600"]
  storageClassName: qumulo-nfs
  persistentVolumeReclaimPolicy: Delete
  claimRef:
    apiVersion: v1
    kind: PersistentVolumeClaim
    namespace: apps
    name: data
    uid: 8b4a2c1e-0000-0000-0000-000000000000
  csi:
    driver: csi.qumulo.com
    volumeHandle: c1.example.com##/csi/volumes/pvc-abc123
    volumeAttributes:
      server: c1.example.com
      exportPath: /csi/volumes/pvc-abc123
      realPath: /csi/volumes/pvc-abc123
      protocol: nfs4.1
`

func TestMirrorFromManifestsEndToEnd(t *testing.T) {
	// The path arrives percent-encoded, as fileRef encodes it.
	peer, auths := fakePeerArray(t, map[string]string{"%2Fcsi%2Fvolumes%2Fpvc-abc123": "60129542144"})
	manifest := writeTempFile(t, "pv.yaml", sourceManifest)
	tokenFile := writeTempFile(t, "token", testToken+"\n")

	var stdout, stderr bytes.Buffer
	err := run([]string{
		"mirror",
		"-f", manifest,
		"-peer-server", "c2.example.com",
		"-peer-api-url", peer.URL,
		"-peer-token-file", tokenFile,
	}, strings.NewReader(""), &stdout, &stderr)
	require.NoError(t, err)

	t.Run("it emits an applicable DR pair", func(t *testing.T) {
		stream, err := decodeObjects(bytes.NewReader(stdout.Bytes()))
		require.NoError(t, err)
		require.Len(t, stream.Volumes, 1)
		require.Len(t, stream.Claims, 1)

		// The peer's REST endpoint is recorded in the handle, not derived
		// from its NFS name: on a tenant network the two differ, and a
		// mirrored PV that kept the source's endpoint would send its
		// DeleteVolume back to the array it failed over from.
		assert.Equal(t, "c2.example.com#"+peer.URL+"#/csi/volumes/pvc-abc123",
			stream.Volumes[0].Spec.CSI.VolumeHandle)
		assert.Equal(t, "Retain", string(stream.Volumes[0].Spec.PersistentVolumeReclaimPolicy))
		assert.Equal(t, "apps", stream.Claims[0].Namespace)
	})

	t.Run("the peer is dialed with the token from the file", func(t *testing.T) {
		require.Len(t, *auths, 1)
		assert.Equal(t, "Bearer "+testToken, (*auths)[0],
			"trailing whitespace in the file must be trimmed, not sent")
	})

	t.Run("and the token appears in no output", func(t *testing.T) {
		// The reason the flag takes a path and not a value. Asserted rather
		// than trusted: this output is pasted into tickets.
		assert.NotContains(t, stdout.String(), testToken)
		assert.NotContains(t, stderr.String(), testToken)
	})

	t.Run("dry run is the default and says so", func(t *testing.T) {
		assert.Contains(t, stderr.String(), "dry run: nothing was written")
	})
}

// A failover plan built from the same manifests, for a DNS-named class.
func TestSwitchFromManifestsEndToEnd(t *testing.T) {
	peer, _ := fakePeerArray(t, map[string]string{"%2Fcsi%2Fvolumes%2Fpvc-abc123": "60129542144"})
	manifest := writeTempFile(t, "pv.yaml",
		strings.ReplaceAll(sourceManifest, "c1.example.com", "qumulo.example.com"))
	tokenFile := writeTempFile(t, "token", testToken)

	var stdout, stderr bytes.Buffer
	err := run([]string{
		"switch",
		"-f", manifest,
		"-peer-server", "c2.example.com",
		"-peer-api-url", peer.URL,
		"-peer-token-file", tokenFile,
	}, strings.NewReader(""), &stdout, &stderr)
	require.NoError(t, err)

	assert.Contains(t, stdout.String(), "topology: dns")
	assert.Contains(t, stdout.String(), "qumulo.example.com  ->  c2.example.com")
	assert.Contains(t, stdout.String(), "apps/data")
	assert.NotContains(t, stdout.String(), "kind: PersistentVolume",
		"a DNS-named class needs no object changes, so none are emitted")
	assert.NotContains(t, stdout.String(), testToken)
}

// Refusals that must happen before anything is read or dialed.
func TestRunRefusesIncompleteInvocations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "no subcommand", args: nil, wantErr: "subcommand is required"},
		{name: "a failover with no namespace", args: []string{"failover"}, wantErr: "-namespace is required"},
		{name: "a failback with no namespace", args: []string{"failback"}, wantErr: "-namespace is required"},
		{name: "an unknown subcommand", args: []string{"repoint"}, wantErr: "unknown subcommand"},
		{
			name:    "no peer server",
			args:    []string{"mirror", "-peer-api-url", "https://x:8000", "-peer-token-file", "/dev/null"},
			wantErr: "-peer-server is required",
		},
		{
			name:    "no peer API URL",
			args:    []string{"mirror", "-f", "-", "-peer-server", "c2", "-peer-token-file", "/dev/null"},
			wantErr: "-peer-api-url is required",
		},
		{
			name:    "no token file",
			args:    []string{"mirror", "-f", "-", "-peer-server", "c2", "-peer-api-url", "https://x:8000"},
			wantErr: "-peer-token-file is required",
		},
		{
			name: "a token file that is not there",
			args: []string{"mirror", "-f", "-", "-peer-server", "c2",
				"-peer-api-url", "https://x:8000", "-peer-token-file", "/no/such/token"},
			wantErr: "reading the peer token file",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(tc.args, strings.NewReader(""), &stdout, &stderr)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// There is no flag that takes a token value, and there must never be one: a
// flag value is in the process table, in `ps`, and in the shell history.
func TestNoFlagAcceptsATokenValue(t *testing.T) {
	var stderr bytes.Buffer
	_, err := parseFlags("mirror", []string{"-h"}, &stderr)
	require.Error(t, err) // flag.ErrHelp

	help := stderr.String()
	assert.Contains(t, help, "-peer-token-file")
	assert.NotContains(t, help, "-peer-token ")
	assert.Contains(t, help, "never the token itself")
}

// The failover plan built from manifests alone: the mirrored pair as `mirror`
// itself emitted it, plus the claims and the two VolumeReplication objects.
//
// No cluster, no peer credentials. `failover` needs neither -- everything it
// joins is already a Kubernetes object -- and the manifests path is what makes
// the plan reviewable before the incident and buildable during one.
const failoverExtrasManifest = `
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  namespace: apps
  name: data
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-nfs
  volumeName: pvc-abc123
  resources:
    requests:
      storage: 10Gi
---
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeReplication
metadata:
  namespace: apps
  name: data-vr
spec:
  volumeReplicationClass: qumulo-replication
  replicationState: primary
  autoResync: false
  dataSource:
    apiGroup: ""
    kind: PersistentVolumeClaim
    name: data
status:
  state: Primary
  message: volume is marked primary
  conditions:
  - type: Completed
    status: "True"
  - type: Degraded
    status: "False"
---
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeReplication
metadata:
  namespace: apps
  name: data-dr-vr
spec:
  volumeReplicationClass: qumulo-replication-dr
  replicationState: secondary
  autoResync: true
  dataSource:
    apiGroup: ""
    kind: PersistentVolumeClaim
    name: data-dr
status:
  state: Secondary
  conditions:
  - type: Completed
    status: "True"
  - type: Degraded
    status: "False"
`

func TestFailoverFromManifestsEndToEnd(t *testing.T) {
	peer, _ := fakePeerArray(t, map[string]string{"%2Fcsi%2Fvolumes%2Fpvc-abc123": "60129542144"})
	sourceFile := writeTempFile(t, "pv.yaml",
		strings.ReplaceAll(sourceManifest, "c1.example.com", "qumulo.example.com"))
	tokenFile := writeTempFile(t, "token", testToken)

	// The DR objects come from `mirror` itself, so this test fails if the two
	// commands ever stop agreeing about what a mirrored pair looks like.
	var mirrored, discard bytes.Buffer
	require.NoError(t, run([]string{
		"mirror",
		"-f", sourceFile,
		"-peer-server", "c2.example.com",
		"-peer-api-url", peer.URL,
		"-peer-token-file", tokenFile,
		"-claim-suffix", "-dr",
	}, strings.NewReader(""), &mirrored, &discard))

	drFile := writeTempFile(t, "dr.yaml", mirrored.String())
	extrasFile := writeTempFile(t, "extras.yaml", failoverExtrasManifest)

	var stdout, stderr bytes.Buffer
	err := run([]string{
		"failover",
		"-f", sourceFile, "-f", drFile, "-f", extrasFile,
		"-namespace", "apps",
	}, strings.NewReader(""), &stdout, &stderr)
	require.NoError(t, err)

	plan := stdout.String()
	t.Run("it plans the five steps in order", func(t *testing.T) {
		quiesce := strings.Index(plan, "0. quiesce")
		promote := strings.Index(plan, "1. promote")
		demote := strings.Index(plan, "2. demote")
		dns := strings.Index(plan, "3. DNS")
		restore := strings.Index(plan, "4. restore")
		require.Positive(t, quiesce)
		assert.Less(t, quiesce, promote, "nothing is promoted before the quiesce")
		assert.Less(t, promote, demote)
		assert.Less(t, demote, dns)
		assert.Less(t, dns, restore)
	})

	t.Run("it says it is single-cluster, and what two clusters would need", func(t *testing.T) {
		assert.Contains(t, plan, "SINGLE-CLUSTER ONLY")
		assert.Contains(t, plan, "two kubeconfigs")
	})

	t.Run("and a plan from files says the quiesce is planned at apply time", func(t *testing.T) {
		// A file that happens to hold no pods must never read as "nothing is
		// running": the promote in step 1 freezes the source.
		assert.Contains(t, plan, "the pod list is not in this input")
	})

	t.Run("the DR side is promoted and the original demoted with autoResync", func(t *testing.T) {
		assert.Contains(t, plan, "apps/data-dr-vr (claim data-dr): secondary -> primary")
		assert.Contains(t, plan, "apps/data-vr (claim data): primary -> secondary, autoResync=true")
	})

	t.Run("and the DNS record is printed, not flipped", func(t *testing.T) {
		// On a failover the demoted side *is* the record, so the plan names the
		// record and its new target and does not invent a "from".
		assert.Contains(t, plan, "qumulo.example.com -> c2.example.com   TTL 60s")
		assert.Contains(t, plan, "No -route53-zone-id was given")
	})

	t.Run("dry run is the default here too", func(t *testing.T) {
		assert.Contains(t, stderr.String(), "dry run: nothing was changed")
		assert.NotContains(t, plan, testToken)
	})
}

// Failback from the same manifests: the same four steps, the other way.
func TestFailbackFromManifestsPromotesTheOriginal(t *testing.T) {
	peer, _ := fakePeerArray(t, map[string]string{"%2Fcsi%2Fvolumes%2Fpvc-abc123": "60129542144"})
	sourceFile := writeTempFile(t, "pv.yaml",
		strings.ReplaceAll(sourceManifest, "c1.example.com", "qumulo.example.com"))
	tokenFile := writeTempFile(t, "token", testToken)

	var mirrored, discard bytes.Buffer
	require.NoError(t, run([]string{
		"mirror", "-f", sourceFile,
		"-peer-server", "c2.example.com",
		"-peer-api-url", peer.URL,
		"-peer-token-file", tokenFile,
		"-claim-suffix", "-dr",
	}, strings.NewReader(""), &mirrored, &discard))

	drFile := writeTempFile(t, "dr.yaml", mirrored.String())
	extrasFile := writeTempFile(t, "extras.yaml", failoverExtrasManifest)

	var stdout, stderr bytes.Buffer
	require.NoError(t, run([]string{
		"failback",
		"-f", sourceFile, "-f", drFile, "-f", extrasFile,
		"-namespace", "apps",
		"-dns-target", "192.0.2.30",
	}, strings.NewReader(""), &stdout, &stderr))

	plan := stdout.String()
	assert.Contains(t, plan, "failback in namespace apps")
	assert.Contains(t, plan, "apps/data-vr (claim data): primary -> primary")
	assert.Contains(t, plan, "apps/data-dr-vr (claim data-dr): secondary -> secondary, autoResync=true")
	assert.Contains(t, plan, "qumulo.example.com: c2.example.com -> 192.0.2.30")
}

// `failover` and `failback` dial no array, so they take no peer credentials at
// all. Asserted rather than assumed: a flag set that quietly grew one would be
// a token on the command line of the command most likely to be run in a hurry.
func TestFailoverTakesNoPeerCredentials(t *testing.T) {
	var stderr bytes.Buffer
	_, err := parseFlags("failover", []string{"-h"}, &stderr)
	require.Error(t, err) // flag.ErrHelp

	help := stderr.String()
	assert.NotContains(t, help, "-peer-token-file")
	assert.NotContains(t, help, "-peer-server")
	assert.NotContains(t, help, "-peer-api-url")
	assert.Contains(t, help, "-namespace")
	assert.Contains(t, help, "-route53-zone-id")
	assert.Contains(t, help, "-aws-profile")
	assert.Contains(t, help, "-allow-bare-pods")
}

// The lab's first Route53 apply failed "session has expired" because the
// profile holding the zone was not the default. AWS_PROFILE is honoured, and
// resolved here rather than left to the CLI, so the profile that was used is
// the profile that gets printed.
func TestFailoverTakesTheAWSProfileFromTheFlagThenTheEnvironment(t *testing.T) {
	var stderr bytes.Buffer

	t.Setenv("AWS_PROFILE", "from-env")
	opt, err := parseFlags("failover", []string{"-namespace", "apps"}, &stderr)
	require.NoError(t, err)
	assert.Equal(t, "from-env", opt.awsProfile)

	opt, err = parseFlags("failover",
		[]string{"-namespace", "apps", "-aws-profile", "from-flag"}, &stderr)
	require.NoError(t, err)
	assert.Equal(t, "from-flag", opt.awsProfile, "the flag wins over the environment")

	t.Setenv("AWS_PROFILE", "")
	opt, err = parseFlags("failover", []string{"-namespace", "apps"}, &stderr)
	require.NoError(t, err)
	assert.Empty(t, opt.awsProfile, "and with neither, the CLI's own default")
}

// A zone id that is not one is refused before the command does anything at all,
// not after the promote has frozen production.
func TestFailoverRefusesAMalformedZoneIDAtFlagTime(t *testing.T) {
	var stderr bytes.Buffer
	_, err := parseFlags("failover",
		[]string{"-namespace", "apps", "-route53-zone-id", "example.com"}, &stderr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a hosted zone id")

	_, err = parseFlags("failover",
		[]string{"-namespace", "apps", "-route53-zone-id", "/hostedzone/Z1D633PJN98FT9"}, &stderr)
	require.NoError(t, err)
}

// The `-h` text is where an operator on two clusters finds out this command is
// not for them, before they run it during an incident.
func TestUsageSaysItIsSingleClusterAndWhatTwoClustersWouldNeed(t *testing.T) {
	var out bytes.Buffer
	usage(&out)

	text := out.String()
	assert.Contains(t, text, "SINGLE-CLUSTER ONLY")
	assert.Contains(t, text, "two kubeconfigs")
	assert.Contains(t, text, "0. quiesce")
	assert.Contains(t, text, "4. restore")
	assert.Contains(t, text, "COLD")
}

// The group form, end to end through the CLI and the decoder.
//
// The namespace the first live dry-run of this command refused: a
// `shared-subdirectory` class protected as a group, where only one member
// carries the VolumeReplication objects and the other has none by design. The
// VolumeReplicationClass documents are part of the input, because the
// `replicationScope` parameter is the only thing that tells the two apart.
const groupNamespaceManifest = `
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pvc-r12-shared-a
spec:
  capacity:
    storage: 10Gi
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-shared
  persistentVolumeReclaimPolicy: Retain
  claimRef:
    apiVersion: v1
    kind: PersistentVolumeClaim
    namespace: dr-demo12
    name: r12-shared-a
  csi:
    driver: csi.qumulo.com
    volumeHandle: qumulo.example.com##/csi/volumes/pvc-r12-shared-a
    volumeAttributes:
      server: qumulo.example.com
      exportPath: /csi/shared/r12-shared-a
      realPath: /csi/shared/r12-shared-a
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pvc-r12-shared-a-dr
  annotations:
    csi.qumulo.com/mirrored-from: pvc-r12-shared-a
spec:
  capacity:
    storage: 10Gi
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-shared
  persistentVolumeReclaimPolicy: Retain
  claimRef:
    apiVersion: v1
    kind: PersistentVolumeClaim
    namespace: dr-demo12
    name: r12-shared-a-dr
  csi:
    driver: csi.qumulo.com
    volumeHandle: c2.example.com##/csi/volumes/pvc-r12-shared-a-dr
    volumeAttributes:
      server: c2.example.com
      exportPath: /csi/shared/r12-shared-a
      realPath: /csi/shared/r12-shared-a
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  namespace: dr-demo12
  name: r12-shared-a
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-shared
  volumeName: pvc-r12-shared-a
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  namespace: dr-demo12
  name: r12-shared-a-dr
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-shared
  volumeName: pvc-r12-shared-a-dr
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pvc-r12-shared-b
spec:
  capacity:
    storage: 10Gi
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-shared
  persistentVolumeReclaimPolicy: Retain
  claimRef:
    apiVersion: v1
    kind: PersistentVolumeClaim
    namespace: dr-demo12
    name: r12-shared-b
  csi:
    driver: csi.qumulo.com
    volumeHandle: qumulo.example.com##/csi/volumes/pvc-r12-shared-b
    volumeAttributes:
      server: qumulo.example.com
      exportPath: /csi/shared/r12-shared-b
      realPath: /csi/shared/r12-shared-b
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pvc-r12-shared-b-dr
  annotations:
    csi.qumulo.com/mirrored-from: pvc-r12-shared-b
spec:
  capacity:
    storage: 10Gi
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-shared
  persistentVolumeReclaimPolicy: Retain
  claimRef:
    apiVersion: v1
    kind: PersistentVolumeClaim
    namespace: dr-demo12
    name: r12-shared-b-dr
  csi:
    driver: csi.qumulo.com
    volumeHandle: c2.example.com##/csi/volumes/pvc-r12-shared-b-dr
    volumeAttributes:
      server: c2.example.com
      exportPath: /csi/shared/r12-shared-b
      realPath: /csi/shared/r12-shared-b
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  namespace: dr-demo12
  name: r12-shared-b
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-shared
  volumeName: pvc-r12-shared-b
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  namespace: dr-demo12
  name: r12-shared-b-dr
spec:
  accessModes: [ReadWriteMany]
  storageClassName: qumulo-shared
  volumeName: pvc-r12-shared-b-dr
  resources:
    requests:
      storage: 10Gi
---
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeReplication
metadata:
  namespace: dr-demo12
  name: r12-shared-group
spec:
  volumeReplicationClass: qumulo-c1-to-c2-group
  replicationState: primary
  dataSource:
    apiGroup: ""
    kind: PersistentVolumeClaim
    name: r12-shared-a
status:
  state: Primary
  conditions:
  - type: Completed
    status: "True"
  - type: Degraded
    status: "False"
---
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeReplication
metadata:
  namespace: dr-demo12
  name: r12-shared-group-dr
spec:
  volumeReplicationClass: qumulo-c2-to-c1-group
  replicationState: secondary
  dataSource:
    apiGroup: ""
    kind: PersistentVolumeClaim
    name: r12-shared-a-dr
status:
  state: Secondary
  conditions:
  - type: Completed
    status: "True"
  - type: Degraded
    status: "False"
---
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeReplicationClass
metadata:
  name: qumulo-c1-to-c2-group
spec:
  provisioner: csi.qumulo.com
  parameters:
    targetServer: c2.example.com
    replicationScope: group
---
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeReplicationClass
metadata:
  name: qumulo-c2-to-c1-group
spec:
  provisioner: csi.qumulo.com
  parameters:
    targetServer: qumulo.example.com
    replicationScope: group
`

func TestFailoverPlansAGroupProtectedNamespace(t *testing.T) {
	manifest := writeTempFile(t, "group.yaml", groupNamespaceManifest)

	var stdout, stderr bytes.Buffer
	require.NoError(t, run([]string{
		"failover", "-f", manifest, "-namespace", "dr-demo12",
	}, strings.NewReader(""), &stdout, &stderr))

	plan := stdout.String()
	assert.Contains(t, plan, "2 volume pair(s) through 1 VolumeReplication pair(s)")
	assert.Contains(t, plan, "StorageClass qumulo-shared (replicationScope: group), 2 member(s):")
	assert.Contains(t, plan, "promote dr-demo12/r12-shared-group-dr, demote dr-demo12/r12-shared-group")
	assert.Contains(t, plan, "member r12-shared-b -> r12-shared-b-dr")

	// One edit per side...
	assert.Equal(t, 1, strings.Count(plan, "-> primary"))
	assert.Equal(t, 1, strings.Count(plan, "-> secondary, autoResync=true"))
	// And every member's pods, both sides.
	for _, claim := range []string{
		"dr-demo12/r12-shared-a", "dr-demo12/r12-shared-a-dr",
		"dr-demo12/r12-shared-b", "dr-demo12/r12-shared-b-dr",
	} {
		assert.Contains(t, plan, claim)
	}
	assert.Contains(t, stderr.String(), "dry run: nothing was changed")
}
