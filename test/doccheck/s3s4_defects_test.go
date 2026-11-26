//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package doccheck

import (
	"regexp"
	"strings"
	"testing"
)

// Documentation-review checks for the lower-severity doc defects.

// TestDefect_StaleMountMessagesSayDeleteThePod.
// The publish error and the log line for a stale mount
// read as self-healing; a running container keeps the dead mount until its
// pod is deleted, which both must say. Ref: node_replica.go:127-131,
// node_server.go:432, :453.
func TestDefect_StaleMountMessagesSayDeleteThePod(t *testing.T) {
	src := dfxGoSource(t, "pkg/driver")
	for _, m := range regexp.MustCompile(`"[^"\n]*(is stale \(%v\)|target holds a stale mount)[^"\n]*"`).FindAllString(src, -1) {
		if !dfxMatch(`pod`, m) {
			t.Errorf("stale-mount message %s does not say a running container keeps the dead mount until its pod is deleted", m)
		}
	}
}

// TestDefect_ReadOnlyManyAloneIsNotSaidToMountReadOnly.
// The node never reads the access mode; only the
// publish's readonly flag or an `ro` option makes the mount read-only. The
// node_server.go must not say "ReadOnlyMany appended \"ro\" itself". Ref:
// node_server.go:470-478.
func TestDefect_ReadOnlyManyAloneIsNotSaidToMountReadOnly(t *testing.T) {
	src := dfxGoSource(t, "pkg/driver")
	if strings.Contains(src, `ReadOnlyMany appended "ro" itself`) {
		t.Errorf("node_server.go says \"ReadOnlyMany appended \\\"ro\\\" itself\"; only the readonly flag or an ro mount option does")
	}
}

// TestDefect_SecurityFlavorRowMatchesTheChartCheck.
// The chart README key table says securityFlavor is
// "`dedicated-export` and `protocol: nfs4.1` only"; an unset protocol is
// accepted, and only nfs3 is refused. Ref: chart README:123;
// templates/_validate.tpl:71-80; security.go.
func TestDefect_SecurityFlavorRowMatchesTheChartCheck(t *testing.T) {
	row := dfxRow(t, dfxChartREADME, "| `securityFlavor` |")
	if strings.Contains(row, "`protocol: nfs4.1` only") {
		t.Errorf("chart README securityFlavor row says \"`protocol: nfs4.1` only\"; an unset protocol is accepted and only nfs3 is refused: %q", dfxClip(row))
	}
}

// TestDefect_SnapshotClassCommentHasNoProcessWideToken.
// There is no process-wide token: a snapshot RPC without a
// secret is refused. Ref: templates/volumesnapshotclass.yaml:29-32.
func TestDefect_SnapshotClassCommentHasNoProcessWideToken(t *testing.T) {
	if dfxMatch(`fall back to the process-wide token`, dfxReadFlat(t, "charts/qumulo-csi-driver/templates/volumesnapshotclass.yaml")) {
		t.Errorf("templates/volumesnapshotclass.yaml says snapshot calls \"fall back to the process-wide token\"; the driver keeps no fallback token")
	}
}

// TestDefect_NoCredentialMessageNamesTheSnapshotRemedy.
// The no-credential error answers the snapshot RPCs too; it
// must name the VolumeSnapshotClass snapshotter-secret parameters or the
// deletion-secret annotations, not only the StorageClass parameters. Ref:
// controller_server.go:817-831.
func TestDefect_NoCredentialMessageNamesTheSnapshotRemedy(t *testing.T) {
	src := dfxGoSource(t, "pkg/driver")
	idx := regexp.MustCompile(`no credential for cluster`).FindAllStringIndex(src, -1)
	if len(idx) == 0 {
		t.Fatalf("pkg/driver has no \"no credential for cluster\" message")
	}
	for _, ix := range idx {
		window := src[ix[0]:min(len(src), ix[0]+2000)]
		if dfxMatch(`snapshotter-secret|deletion-secret`, window) {
			return
		}
	}
	t.Errorf("the \"no credential for cluster\" message names only the StorageClass provisioner/controller-expand secrets, though DeleteSnapshot/CreateSnapshot/ListSnapshots get it too")
}

// dfxAlertExcludesResolve reports whether QumuloCSIPrivilegeMissing's
// expression excludes the best-effort /v1/files/resolve route.
func dfxAlertExcludesResolve(t *testing.T) bool {
	tpl := dfxRead(t, dfxMetricsTpl)
	i := strings.Index(tpl, "QumuloCSIPrivilegeMissing")
	if i < 0 {
		return true // no such alert
	}
	expr := tpl[i:min(len(tpl), i+800)]
	return regexp.MustCompile(`path_template\s*!~[^}]*resolve`).MatchString(expr)
}

// TestDefect_DroppingAnalyticsReadSaysTheAlertFires.
// The README says PRIVILEGE_ANALYTICS_READ can be dropped; the
// QumuloCSIPrivilegeMissing alert then fires on every ListSnapshots and
// restore. Either the alert excludes that route or the README says so.
// Ref: chart README:1406-1407; templates/metrics.yaml:180-190.
func TestDefect_DroppingAnalyticsReadSaysTheAlertFires(t *testing.T) {
	if dfxAlertExcludesResolve(t) {
		return
	}
	p := dfxDroppable(t)
	if dfxMatch(`PRIVILEGE_ANALYTICS_READ[^.]*can be dropped`, p) && !strings.Contains(p, "QumuloCSIPrivilegeMissing") {
		t.Errorf("chart README says PRIVILEGE_ANALYTICS_READ can be dropped, without saying QumuloCSIPrivilegeMissing then fires on /v1/files/resolve: %q", dfxClip(p))
	}
}

// TestDefect_OverrideCheckSkipsAreAllNamed.
// The README says the cross-cluster check is skipped only when
// the volume's own endpoint does not answer; it is also skipped when the
// override fails or either array reports no cluster ID. Ref: chart
// README:457-461; controller_server.go:1027-1036.
func TestDefect_OverrideCheckSkipsAreAllNamed(t *testing.T) {
	para := dfxParagraph(t, dfxChartREADME, "A VolumeSnapshotClass may set `apiurl`")
	if para == "" {
		return
	}
	if !dfxMatch(`either endpoint|neither endpoint|override[^.]*(does not answer|fails)|(does not|cannot) report[^.]*(cluster|UUID)|empty cluster`, para) {
		t.Errorf("chart README names only the volume's own endpoint not answering as the case where the cluster check is skipped: %q", dfxClip(para))
	}
}

// TestDefect_DeployedTableStatesTheControllerQoS.
// The "What gets deployed" controller row says nothing
// about the controller's QoS, while the node row contrasts Burstable node
// pods; it must say what requests/priority the controller gets. Ref: chart
// README:30.
func TestDefect_DeployedTableStatesTheControllerQoS(t *testing.T) {
	objs, out, err := dfxRender(t, "qumulo-csi", "")
	if err != nil {
		t.Fatalf("render: %v: %s", err, dfxClip(out))
	}
	for _, o := range objs {
		if o["kind"] != "Deployment" {
			continue
		}
		spec, _ := o["spec"].(map[string]any)
		tmpl, _ := spec["template"].(map[string]any)
		pod, _ := tmpl["spec"].(map[string]any)
		driver := dfxContainers(t, objs, "Deployment")["qumulo-csi-driver"]
		if pod["priorityClassName"] != nil && driver["resources"] != nil {
			return // the controller is no longer BestEffort without a class
		}
	}
	row := dfxRow(t, dfxChartREADME, "| `Deployment` (controller)")
	if row == "" {
		t.Fatalf("chart README has no controller row in \"What gets deployed\"")
	}
	if !dfxMatch(`request|resources|BestEffort|Burstable|priority`, row) {
		t.Errorf("chart README controller row says nothing about its requests or priority class: %q", dfxClip(row))
	}
}

// TestDefect_MigratedPVSecretsGoOnThePV.
// The migration example PV has storageClassName "", so the
// expand/delete secrets cannot come from "the class's" parameters; the
// README must point at controllerExpandSecretRef and the deletion-secret
// annotations (or a storageClassName). Ref: chart README:1290-1295.
func TestDefect_MigratedPVSecretsGoOnThePV(t *testing.T) {
	para := dfxParagraph(t, dfxChartREADME, "**Quotas and exports are reused, not recreated,**")
	if para == "" {
		return
	}
	if !dfxMatch(`controllerExpandSecretRef|provisioner-deletion-secret`, para) {
		t.Errorf("chart README migration text tells the reader to add secrets to \"the class\" of a PV with storageClassName \"\", without naming controllerExpandSecretRef or the deletion-secret annotations: %q", dfxClip(para))
	}
}

// TestDefect_EmptyNodeContainerEntryDropsRequests.
// values.yaml says `driver: {}` runs a container with no
// requests; Helm merges {} with the default, so the requests stay. Ref:
// values.yaml:444-446.
func TestDefect_EmptyNodeContainerEntryDropsRequests(t *testing.T) {
	if !strings.Contains(dfxReadFlat(t, dfxValuesYAML), "driver: {}") {
		return
	}
	objs, out, err := dfxRender(t, "qumulo-csi", "node:\n  resources:\n    driver: {}\n")
	if err != nil {
		t.Fatalf("render: %v: %s", err, dfxClip(out))
	}
	cs := dfxContainers(t, objs, "DaemonSet")
	for name, c := range cs {
		if !strings.Contains(name, "driver") || strings.Contains(name, "registrar") {
			continue
		}
		res, _ := c["resources"].(map[string]any)
		if res["requests"] != nil {
			t.Errorf("values.yaml says `driver: {}` gives the node driver container no requests, but container %s renders requests %v", name, res["requests"])
		}
	}
}

// TestDefect_ValuesSayTheNodeClassNeverPreempts.
// The class has preemptionPolicy Never; a full node cannot fit
// the rolled node pod. values.yaml must say it never preempts. Ref:
// values.yaml:456-463.
func TestDefect_ValuesSayTheNodeClassNeverPreempts(t *testing.T) {
	if !dfxExists(t, "charts/qumulo-csi-driver/templates/priorityclass.yaml") {
		return
	}
	block := dfxBetween(t, dfxValuesYAML, "node plugin is a node-level add-on|^  priorityClass:", "^  priorityClassName:")
	if !dfxMatch(`never preempt|preemptionPolicy|cannot (evict|preempt)|room|Pending`, block) {
		t.Errorf("values.yaml node.priorityClass does not say the class never preempts (a full node leaves the node pod Pending): %q", dfxClip(block))
	}
}

// TestDefect_ProvisioningAlertExplainsEveryCode.
// QumuloCSIProvisioningFailing's description lists five codes;
// FailedPrecondition and Internal are not explained. Ref:
// templates/metrics.yaml:199-207.
func TestDefect_ProvisioningAlertExplainsEveryCode(t *testing.T) {
	tpl := dfxRead(t, dfxMetricsTpl)
	i := strings.Index(tpl, "QumuloCSIProvisioningFailing")
	if i < 0 {
		return
	}
	end := strings.Index(tpl[i+10:], "- alert:")
	if end < 0 {
		end = len(tpl) - i - 10
	}
	alert := dfxFlat(tpl[i : i+10+end])
	for _, code := range []string{"FailedPrecondition", "Internal"} {
		if !strings.Contains(alert, code) {
			t.Errorf("QumuloCSIProvisioningFailing description does not explain %s", code)
		}
	}
}

// TestDefect_PrivilegeAlertDoesNotPromiseAnOperationError.
// Some 403s are best-effort reads the driver continues past, so
// "The operation's error names the privilege" is untrue for them, unless
// the alert excludes those routes. Ref: templates/metrics.yaml:180-190.
func TestDefect_PrivilegeAlertDoesNotPromiseAnOperationError(t *testing.T) {
	if dfxAlertExcludesResolve(t) {
		return
	}
	if strings.Contains(dfxReadFlat(t, dfxMetricsTpl), "The operation's error names the privilege") {
		t.Errorf("QumuloCSIPrivilegeMissing fires on every 403, including best-effort reads with no operation error, yet says \"The operation's error names the privilege\"")
	}
}

// TestDefect_ImageTagCommentSaysAppVersionIsShared.
// Every build is appVersion 0.1.0, so "Defaults to the
// chart's appVersion" with IfNotPresent runs whatever 0.1.0 a node cached;
// the comment must say to pin a build tag (or pull Always). Ref:
// values.yaml:39; Chart.yaml:9-10.
func TestDefect_ImageTagCommentSaysAppVersionIsShared(t *testing.T) {
	block := dfxBetween(t, dfxValuesYAML, "^  controller:$", "^  node:$")
	if !strings.Contains(block, "appVersion") {
		return
	}
	if !dfxMatch(`pin|every build|Always|names the build|commit`, block) {
		t.Errorf("values.yaml image tag comment says it defaults to appVersion without warning that every build shares that version: %q", dfxClip(block))
	}
}

// TestDefect_ArchitectureMdSidecarListMatchesTheChart.
// ARCHITECTURE.md lists csi-attacher among the sidecars; the chart
// deploys none (and does deploy csi-resizer and csi-snapshotter). Ref:
// ARCHITECTURE.md:125; templates/controller.yaml.
func TestDefect_ArchitectureMdSidecarListMatchesTheChart(t *testing.T) {
	line := dfxRow(t, "ARCHITECTURE.md", "rely entirely on official sidecars")
	if line == "" {
		return
	}
	tpl := dfxRead(t, "charts/qumulo-csi-driver/templates/controller.yaml")
	if strings.Contains(line, "`csi-attacher`") && !dfxMatch("no `csi-attacher`", line) && !strings.Contains(tpl, "csi-attacher:") {
		t.Errorf("ARCHITECTURE.md lists csi-attacher as a sidecar the driver relies on; the chart deploys none: %q", strings.TrimSpace(line))
	}
	for _, sc := range []string{"csi-resizer", "csi-snapshotter"} {
		if strings.Contains(tpl, sc) && !strings.Contains(line, sc) {
			t.Errorf("ARCHITECTURE.md sidecar list omits %s, which the chart deploys", sc)
		}
	}
}

// TestDefect_ArchitectureMdListVolumesClaimHolds.
// ARCHITECTURE.md says list_volumes.go "predicts the shift" in the
// csi-sanity figures; it says nothing about csi-sanity. Ref: ARCHITECTURE.md:154.
func TestDefect_ArchitectureMdListVolumesClaimHolds(t *testing.T) {
	if !strings.Contains(dfxReadFlat(t, "ARCHITECTURE.md"), "`pkg/driver/list_volumes.go` predicts") {
		return
	}
	if !dfxMatch(`sanity`, dfxRead(t, "pkg/driver/list_volumes.go")) {
		t.Errorf("ARCHITECTURE.md says pkg/driver/list_volumes.go predicts the csi-sanity shift; the file never mentions csi-sanity")
	}
}

// TestDefect_UninstallListNamesThePriorityClass.
// Uninstall also removes the cluster-scoped PriorityClass. Ref:
// chart README:63-69.
func TestDefect_UninstallListNamesThePriorityClass(t *testing.T) {
	if !dfxExists(t, "charts/qumulo-csi-driver/templates/priorityclass.yaml") {
		return
	}
	para := dfxParagraph(t, dfxChartREADME, "uninstall` actually removes")
	if para == "" {
		t.Fatalf("chart README has no \"what helm uninstall actually removes\" paragraph")
	}
	if !dfxMatch(`PriorityClass`, para) {
		t.Errorf("chart README uninstall list omits the node PriorityClass: %q", dfxClip(para))
	}
}

// TestDefect_NotesCapIsInEntries.
// NOTES.txt says a restore is refused "past 50,000 files"; the
// cap is 50,000 entries (files and directories) or 2 TiB. Ref:
// templates/NOTES.txt; restore.go:44-45.
func TestDefect_NotesCapIsInEntries(t *testing.T) {
	notes := dfxReadFlat(t, "charts/qumulo-csi-driver/templates/NOTES.txt")
	if strings.Contains(notes, "50,000 files") {
		t.Errorf("NOTES.txt says the restore cap is \"50,000 files\"; it is 50,000 entries (files and directories) / 2 TiB")
	}
}

// TestDefect_NoLabLabelsInMetricsDocsAndAlerts.
// The alert template and values.yaml must not cite internal lab labels
// (KB-nn) as reasons. Ref: templates/metrics.yaml:140-208; values.yaml:294.
func TestDefect_NoLabLabelsInMetricsDocsAndAlerts(t *testing.T) {
	for _, doc := range []string{dfxMetricsTpl, dfxValuesYAML} {
		seen := map[string]bool{}
		for _, m := range regexp.MustCompile(`\bKB-\d+\b`).FindAllString(dfxRead(t, doc), -1) {
			if !seen[m] {
				seen[m] = true
				t.Errorf("%s cites %q, an internal lab label a reader cannot look up", doc, m)
			}
		}
	}
}

// TestDefect_MetricsNoLongerSayNeverScraped.
// The series have since been scraped from a live controller.
// Ref: docs/dashboards/README.md:5.
func TestDefect_MetricsNoLongerSayNeverScraped(t *testing.T) {
	for _, doc := range []string{"docs/dashboards/README.md"} {
		if !dfxExists(t, doc) {
			continue
		}
		if dfxMatch(`has not been scraped by a real Prometheus against a real array`, dfxReadFlat(t, doc)) {
			t.Errorf("%s says the metrics \"have not been scraped by a real Prometheus against a real array\"; they have", doc)
		}
	}
}
