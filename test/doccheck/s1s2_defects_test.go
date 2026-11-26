//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package doccheck

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// Documentation-review checks for the higher-severity doc defects.

// TestDefect_DriverExportsAreNotSaidToSquashRoot.
// The chart README must not tell the reader a client is
// mapped "under an export's NFS_MAP_ROOT": every driver-made export is
// NFS_MAP_NONE with no host list, so root is root unless allowedClients is
// set. values.yaml's allowedClients "Omit for unrestricted" must say root is
// not squashed. Ref: chart README:128-131, values.yaml:660-661;
// pkg/qumulo/client.go defaultExportRestriction.
func TestDefect_DriverExportsAreNotSaidToSquashRoot(t *testing.T) {
	readme := dfxReadFlat(t, dfxChartREADME)
	if strings.Contains(readme, "under an export's `NFS_MAP_ROOT`") {
		t.Errorf("chart README still says an NFS client is mapped \"under an export's `NFS_MAP_ROOT`\"; driver-made exports use NFS_MAP_NONE (root not squashed)")
	}
	if !strings.Contains(readme, "NFS_MAP_NONE") && !dfxMatch(`root (is|are) not squashed|no root squash|root is root`, readme) {
		t.Errorf("chart README never says that a driver-made export does not squash root (NFS_MAP_NONE)")
	}
	values := dfxReadFlat(t, dfxValuesYAML)
	if i := strings.Index(values, "Omit for unrestricted"); i >= 0 {
		tail := values[i:min(len(values), i+300)]
		if !dfxMatch(`squash|NFS_MAP_NONE|root`, tail) {
			t.Errorf("values.yaml allowedClients: %q does not say root is not squashed", dfxClip(tail))
		}
	}
}

// TestDefect_HardMountTextAdmitsTheSharedSuperblock.
// The chart README's `hard` bullet must not promise
// "Nothing is lost" without saying that a forced unmount on one volume acts
// on every mount sharing the NFS superblock (same server address, same
// options). Ref: chart README:306-309; pkg/driver/node_unmount.go.
func TestDefect_HardMountTextAdmitsTheSharedSuperblock(t *testing.T) {
	bullet := dfxBetween(t, dfxChartREADME, "^\\* \\*\\*`hard`\\*\\*", "^\\* \\*\\*`soft`\\*\\*")
	if bullet == "" {
		t.Fatalf("chart README has no `hard` mount-option bullet")
	}
	if strings.Contains(bullet, "Nothing is lost") && !dfxMatch(`superblock|same server address|share`, bullet) {
		t.Errorf("chart README `hard` bullet says \"Nothing is lost\" and never says a forced unmount aborts I/O on other mounts of the same server: %q", dfxClip(bullet))
	}
	if !dfxMatch(`superblock|same server address|share[sd]? one`, bullet) {
		t.Errorf("chart README `hard` bullet does not address mounts sharing one NFS connection/superblock: %q", dfxClip(bullet))
	}
}

// TestDefect_ShutdownCommentDoesNotPromiseInFlightRPCsFinish.
// main.go must not say GracefulStop lets in-flight RPCs
// finish: the sidecars exit on the same SIGTERM and every in-flight handler
// is cancelled at once. Ref: cmd/qumulo-csi-driver/main.go:372-374.
func TestDefect_ShutdownCommentDoesNotPromiseInFlightRPCsFinish(t *testing.T) {
	main := dfxFlat(dfxRead(t, "cmd/qumulo-csi-driver/main.go"))
	if strings.Contains(main, "GracefulStop lets in-flight RPCs finish") {
		t.Errorf("main.go still says \"GracefulStop lets in-flight RPCs finish\"; in-flight handlers are cancelled when the sidecars exit")
	}
	tpl := dfxFlat(dfxRead(t, "charts/qumulo-csi-driver/templates/controller.yaml"))
	if strings.Contains(tpl, "The driver drains in-flight RPCs for shutdownGracePeriod") && !dfxMatch(`cancel`, tpl) {
		t.Errorf("templates/controller.yaml says the driver drains in-flight RPCs, without saying they are cancelled when the sidecars exit")
	}
}

// TestDefect_DeleteIdentityCommentAndHandleDotDot.
// The resolveVolumeForDelete comment must not describe a
// last-element identity check that cannot fail, and the chart README's
// static-handle section must say a handle's path is cleaned, so `..` names
// a sibling or the parent. Ref: controller_server.go:1429-1454; chart
// README:1298-1310; volume_id.go path.Clean.
func TestDefect_DeleteIdentityCommentAndHandleDotDot(t *testing.T) {
	src := dfxFlat(dfxGoSource(t, "pkg/driver"))
	if strings.Contains(src, "So a resolved path whose last element is not that name is not this volume") {
		t.Errorf("pkg/driver still describes a last-element identity check (\"a resolved path whose last element is not that name is not this volume\"); Name is path.Base of the same path, so it cannot fail")
	}
	section := dfxBetween(t, dfxChartREADME, "^## Static PV handles are not scoped", "^## ")
	if section == "" {
		t.Fatalf("chart README has no \"Static PV handles are not scoped\" section")
	}
	if !strings.Contains(section, "`..`") && !strings.Contains(section, "/../") && !strings.Contains(section, "/..`") {
		t.Errorf("chart README static-handle section never says a `..` in a handle is resolved (it names a sibling volume or the parent): %q", dfxClip(section))
	}
}

// TestDefect_RollbackGuidanceExists.
// The READMEs must give rollback guidance. Ref: chart README:59-95.
func TestDefect_RollbackGuidanceExists(t *testing.T) {
	both := dfxReadFlat(t, dfxRootREADME) + "\n" + dfxReadFlat(t, dfxChartREADME)
	if !dfxMatch(`roll(ing)? ?back`, both) {
		t.Errorf("neither README.md nor the chart README gives any rollback guidance")
	}
}

// dfxHelmUpgradeSetWithoutValues finds `helm upgrade` commands in a doc that
// pass --set without -f/--values/--reuse-values/--reset-then-reuse-values.
func dfxHelmUpgradeSetWithoutValues(text string) []string {
	var bad []string
	re := regexp.MustCompile("helm upgrade[^`\\n]*(\\\\\\n[^`\\n]*)*")
	for _, loc := range re.FindAllStringIndex(text, -1) {
		cmd := text[loc[0]:loc[1]]
		if !strings.Contains(cmd, "--set") {
			continue
		}
		// A sentence warning against the command is not an example of it.
		after := dfxFlat(text[loc[1]:min(len(text), loc[1]+200)])
		if dfxMatch(`^\S*\s*(alone|without|on its own)|deletes|renders the chart's defaults`, after) {
			continue
		}
		if dfxMatch(`(^|\s)-f\s|--values|reuse-values`, cmd) {
			continue
		}
		bad = append(bad, strings.TrimSpace(cmd))
	}
	return bad
}

// TestDefect_UpgradeWithSetAlwaysPassesValues.
// No doc may show `helm upgrade --set` without -f or
// --reuse-values (such an upgrade deletes every class the release made),
// and the chart README must say to pass the values file on every upgrade.
func TestDefect_UpgradeWithSetAlwaysPassesValues(t *testing.T) {
	for _, doc := range []string{dfxRootREADME, dfxChartREADME} {
		for _, cmd := range dfxHelmUpgradeSetWithoutValues(dfxRead(t, doc)) {
			t.Errorf("%s shows %q: an upgrade with --set and no -f/--reuse-values renders no classes and deletes them", doc, cmd)
		}
	}
	warn := `reuse-values|values file on every|every upgrade|without .?-f`
	if !dfxMatch(warn, dfxReadFlat(t, dfxChartREADME)) {
		t.Errorf("chart README does not warn that an upgrade without -f/--reuse-values deletes every StorageClass, VolumeSnapshotClass and VolumeReplicationClass")
	}
}

// TestDefect_ReplicatingListNamesTheSnapshotPrivileges.
// A planned promote takes, lists and deletes a marker
// snapshot on the source, so the chart README's "Replicating" privilege
// list must name PRIVILEGE_SNAPSHOT_READ and PRIVILEGE_SNAPSHOT_WRITE (or
// say the provisioning set's snapshot pair is needed by replication). Ref:
// chart README:1387-1400; replication_server.go marker snapshot.
func TestDefect_ReplicatingListNamesTheSnapshotPrivileges(t *testing.T) {
	src := dfxGoSource(t, "pkg/driver")
	if !dfxMatch(`marker`, src) {
		t.Fatalf("pkg/driver no longer mentions a promote marker snapshot; re-check this row")
	}
	block := dfxBetween(t, dfxChartREADME, "^Replicating \\(the csi-addons", "^A class that never sets|^`PRIVILEGE_REPLICATION_SOURCE_READ` and")
	if block == "" {
		t.Fatalf("chart README has no \"Replicating (the csi-addons ...)\" privilege list")
	}
	if !strings.Contains(block, "PRIVILEGE_SNAPSHOT_READ") || !strings.Contains(block, "PRIVILEGE_SNAPSHOT_WRITE") {
		t.Errorf("chart README Replicating privilege list does not name PRIVILEGE_SNAPSHOT_READ and PRIVILEGE_SNAPSHOT_WRITE, which a planned promote needs: %q", dfxClip(block))
	}
}

// TestDefect_CoveringExportTextSaysReEnableIsRefused.
// The chart README's group-protection text says a
// read-write DR covering export "is set read-only at protect time"; it must
// say that after a Disable removes the relationship the re-enable is
// refused instead, and the group stays unprotected. Ref: chart
// README:713-716; replication_server.go:938-980, :1194-1217.
func TestDefect_CoveringExportTextSaysReEnableIsRefused(t *testing.T) {
	para := dfxParagraph(t, dfxChartREADME, "**Protecting a `shared-subdirectory` class.**")
	i := strings.Index(para, "covering export at that path")
	if i < 0 {
		t.Fatalf("chart README group-protection paragraph no longer mentions a DR covering export; re-check: %q", dfxClip(para))
	}
	tail := para[i:]
	if !dfxMatch(`refus`, tail) {
		t.Errorf("chart README says a read-write DR covering export is set read-only at protect time, without saying a re-enable after a Disable is refused: %q", dfxClip(tail))
	}
}

// dfxCopyAlwaysFromZero reports whether the client's CopyFile still starts
// every copy at offset 0: then no doc may promise a byte-level
// resume.
func dfxCopyAlwaysFromZero(t *testing.T) bool {
	return strings.Contains(dfxGoSource(t, "pkg/qumulo"), `SourceOffset: "0"`)
}

// TestDefect_ValuesDoNotPromiseRestoreResumesMidFile.
// values.yaml must not say each restore retry
// "resumes where the last stopped" while a partly copied file restarts from
// byte 0. Ref: values.yaml:106-112; restore.go; client.go CopyFile.
func TestDefect_ValuesDoNotPromiseRestoreResumesMidFile(t *testing.T) {
	if !dfxCopyAlwaysFromZero(t) {
		return // a byte-level resume exists; the promise is true
	}
	if strings.Contains(dfxReadFlat(t, dfxValuesYAML), "resumes where the last stopped") {
		t.Errorf("values.yaml says each restore retry \"resumes where the last stopped\", but CopyFile always starts at offset 0")
	}
}

// TestDefect_ReadmeDoesNotPromiseRestoreResumesMidFile.
// Same claim in the chart README restore table. Ref:
// chart README:473.
func TestDefect_ReadmeDoesNotPromiseRestoreResumesMidFile(t *testing.T) {
	if !dfxCopyAlwaysFromZero(t) {
		return
	}
	if strings.Contains(dfxReadFlat(t, dfxChartREADME), "resumes where the last stopped") {
		t.Errorf("chart README says each restore attempt \"resumes where the last stopped\", but a cut-off file is copied again from byte 0")
	}
}

// dfxDroppable returns the chart README's droppable-privileges paragraph.
func dfxDroppable(t *testing.T) string {
	p := dfxParagraph(t, dfxChartREADME, "can drop the corresponding")
	if p == "" {
		p = dfxParagraph(t, dfxChartREADME, "can be dropped")
	}
	return p
}

// TestDefect_ExportReadIsNotDroppable.
// Every shared-subdirectory provision lists exports, and every
// delete looks for a volume export, so PRIVILEGE_NFS_EXPORT_READ is never
// droppable; the README must not offer both PRIVILEGE_NFS_EXPORT_*. Ref:
// chart README:1402-1405; controller_server.go:665-673, :1585-1588.
func TestDefect_ExportReadIsNotDroppable(t *testing.T) {
	p := dfxDroppable(t)
	if p == "" {
		t.Fatalf("chart README has no droppable-privileges paragraph")
	}
	if strings.Contains(p, "PRIVILEGE_NFS_EXPORT_*") {
		t.Errorf("chart README lets a class drop both `PRIVILEGE_NFS_EXPORT_*`; PRIVILEGE_NFS_EXPORT_READ is needed by every provision and delete: %q", dfxClip(p))
	}
	for _, m := range regexp.MustCompile(`[^.]*PRIVILEGE_NFS_EXPORT_READ[^.]*`).FindAllString(p, -1) {
		if !dfxMatch(`not droppable|cannot be dropped|is needed|needs|keep`, m) {
			t.Errorf("chart README offers PRIVILEGE_NFS_EXPORT_READ as droppable: %q", m)
		}
	}
}

// TestDefect_SnapshotDropRuleExcludesReplicatedClasses.
// The droppable rule lets a class that never takes snapshots
// drop PRIVILEGE_SNAPSHOT_*; a replicated class needs them for a planned
// promote, so the rule must be qualified with replication. Ref: chart
// README:1387-1396, :1402-1405.
func TestDefect_SnapshotDropRuleExcludesReplicatedClasses(t *testing.T) {
	p := dfxDroppable(t)
	if p == "" {
		t.Fatalf("chart README has no droppable-privileges paragraph")
	}
	if dfxMatch(`PRIVILEGE_SNAPSHOT_\*|PRIVILEGE_SNAPSHOT_(READ|WRITE)`, p) && !dfxMatch(`replicat|promote`, p) {
		t.Errorf("chart README lets a class that never takes snapshots drop the snapshot privileges, without excepting a replicated class (a planned promote needs them): %q", dfxClip(p))
	}
}

// TestDefect_SchedulingIntervalIsNotALagGrade.
// csi-addons' GetVolumeReplicationInfo carries no class
// parameters, so the driver never grades lag from schedulingInterval; the
// chart README key table and the class template comment must not say it
// does. Ref: chart README:643; templates/volumereplicationclass.yaml:70-71.
func TestDefect_SchedulingIntervalIsNotALagGrade(t *testing.T) {
	re := `lag (past|beyond) which[^.|]*Degraded`
	if row := dfxRow(t, dfxChartREADME, "`schedulingInterval`"); dfxMatch(re, row) {
		t.Errorf("chart README says schedulingInterval is the lag past which the driver reports Degraded: %q", dfxClip(row))
	}
	if tpl := dfxReadFlat(t, "charts/qumulo-csi-driver/templates/volumereplicationclass.yaml"); dfxMatch(re, tpl) {
		t.Errorf("templates/volumereplicationclass.yaml says the driver grades lag beyond schedulingInterval as Degraded")
	}
}

// TestDefect_OverrideSnapshotRestoreNeedsTheOverrideClass.
// A snapshot taken through a VolumeSnapshotClass apiurl records
// the override, and a restore is refused unless the StorageClass endpoint
// equals it; the README must not say "delete and restore reach it too".
// Ref: chart README:453-456; controller_server.go:216-221.
func TestDefect_OverrideSnapshotRestoreNeedsTheOverrideClass(t *testing.T) {
	if strings.Contains(dfxReadFlat(t, dfxChartREADME), "so delete and restore reach it too") {
		t.Errorf("chart README says a VolumeSnapshotClass apiurl override is recorded \"so delete and restore reach it too\"; restore needs a StorageClass whose apiUrl is that override")
	}
}

// TestDefect_PriorityClassRowNamesItsUpgradeHazards.
// The chart README's PriorityClass row must say the class's
// value cannot be changed after install, and that upgrading needs patch on
// priorityclasses. Ref: chart README:32-33; templates/priorityclass.yaml.
func TestDefect_PriorityClassRowNamesItsUpgradeHazards(t *testing.T) {
	if !dfxExists(t, "charts/qumulo-csi-driver/templates/priorityclass.yaml") {
		return
	}
	row := dfxRow(t, dfxChartREADME, "| `PriorityClass`")
	if row == "" {
		t.Fatalf("chart README \"What gets deployed\" has no PriorityClass row")
	}
	if !dfxMatch(`immutable|cannot be changed|can't be changed|may not be changed|fixed once`, row) {
		t.Errorf("chart README PriorityClass row does not say the value cannot be changed after install: %q", dfxClip(row))
	}
	if !dfxMatch(`upgrad[^|]{0,200}(patch|update|rights|need)`, row) {
		t.Errorf("chart README PriorityClass row does not say what an upgrade needs on priorityclasses (only install's create): %q", dfxClip(row))
	}
}

// TestDefect_HTTPApiUrlIsRefusedOrWarned.
// An http:// apiUrl sends the bearer token in cleartext.
// Either the chart refuses it, or the README's apiUrl/TLS text warns about
// it. Ref: chart README:118, :337-354; templates/_validate.tpl:63-64.
func TestDefect_HTTPApiUrlIsRefusedOrWarned(t *testing.T) {
	values := "storageClasses:\n  - name: dfx-http\n    server: 192.0.2.10\n    apiUrl: http://192.0.2.10:8000\n    secretName: tok\n    basePath: /csi\n"
	if _, out, err := dfxRender(t, "qumulo-csi", values); err != nil {
		if dfxMatch(`https://|cleartext`, out) {
			return // refused at render
		}
		t.Fatalf("render failed for another reason: %s", dfxClip(out))
	}
	readme := dfxReadFlat(t, dfxChartREADME)
	if !dfxMatch("http://[^.]{0,200}(unencrypted|cleartext|clear text|in the clear|plain ?text)|(unencrypted|cleartext|clear text|in the clear|plain ?text)[^.]{0,200}http://", readme) {
		t.Errorf("the chart renders an http:// apiUrl and the chart README never warns that the token is then sent unencrypted")
	}
}

// dfxPriorityImmutable matches a statement that the PriorityClass value
// cannot be changed after install.
const dfxPriorityImmutable = `immutable|cannot be changed|can't be changed|may not be changed|fixed once|not be changed after`

// TestDefect_ControllerResourcesDescriptionMatchesTheRender.
// values.schema.json describes controller.resources as
// the driver container's resources; the render must then give them to the
// driver container. Ref: values.schema.json:494; templates/controller.yaml.
func TestDefect_ControllerResourcesDescriptionMatchesTheRender(t *testing.T) {
	objs, out, err := dfxRender(t, "qumulo-csi", "controller:\n  resources:\n    requests: {cpu: 11m, memory: 33Mi}\n")
	if err != nil {
		t.Fatalf("render: %v: %s", err, dfxClip(out))
	}
	cs := dfxContainers(t, objs, "Deployment")
	driver, ok := cs["qumulo-csi-driver"]
	if !ok {
		t.Fatalf("controller Deployment has no qumulo-csi-driver container")
	}
	got := fmt.Sprint(driver["resources"])
	if !strings.Contains(got, "11m") {
		schema := dfxReadFlat(t, dfxValuesSchema)
		if strings.Contains(schema, "Container resources for the driver container") {
			t.Errorf("values.schema.json says controller.resources is for the driver container, but the rendered driver container has resources %s", got)
		}
	}
}

// TestDefect_FlatNodeResourcesRenderAsDocumented.
// values.yaml says the flat {requests, limits} node.resources
// form is applied to all three containers; with -f it must render exactly
// that, not the merged map with registrar/livenessProbe/driver keys. Ref:
// values.yaml:439-446; templates/_helpers.tpl.
func TestDefect_FlatNodeResourcesRenderAsDocumented(t *testing.T) {
	if !dfxMatch(`flat form|flat \{requests`, dfxReadFlat(t, dfxValuesYAML)+dfxReadFlat(t, dfxValuesSchema)) {
		return // the flat form is no longer documented
	}
	objs, out, err := dfxRender(t, "qumulo-csi", "node:\n  resources:\n    requests: {cpu: 50m, memory: 128Mi}\n")
	if err != nil {
		t.Fatalf("render with a flat node.resources: %v: %s", err, dfxClip(out))
	}
	for name, c := range dfxContainers(t, objs, "DaemonSet") {
		res, _ := c["resources"].(map[string]any)
		for k := range res {
			if k != "requests" && k != "limits" && k != "claims" {
				t.Errorf("node container %s: a flat node.resources renders resources key %q (the merged per-container map), not the documented flat form", name, k)
			}
		}
		if !strings.Contains(fmt.Sprint(res["requests"]), "50m") {
			t.Errorf("node container %s: flat node.resources requests not applied: %v", name, res)
		}
	}
}

// TestDefect_ValuesSayPriorityValueIsImmutable.
// values.yaml / values.schema.json node.priorityClass must say
// the value cannot be changed once the class exists. Ref:
// values.yaml:456-467; values.schema.json:750-756.
func TestDefect_ValuesSayPriorityValueIsImmutable(t *testing.T) {
	if !dfxExists(t, "charts/qumulo-csi-driver/templates/priorityclass.yaml") {
		return
	}
	block := dfxBetween(t, dfxValuesYAML, "node plugin is a node-level add-on|^  priorityClass:", "^  priorityClassName:")
	schema := dfxRow(t, dfxValuesSchema, "The PriorityClass value")
	if !dfxMatch(dfxPriorityImmutable, block) && !dfxMatch(dfxPriorityImmutable, schema) {
		t.Errorf("values.yaml / values.schema.json node.priorityClass never say the value cannot be changed after install: %q", dfxClip(block))
	}
}

// TestDefect_ReadmeRoleHasNetworkRead.
// The chart README's least-privilege role must include
// PRIVILEGE_NETWORK_READ, which the tenant-network check's GET /v3/network
// needs. Ref: pkg/qumulo/privileges.go.
func TestDefect_ReadmeRoleHasNetworkRead(t *testing.T) {
	if !strings.Contains(dfxRead(t, "pkg/qumulo/privileges.go"), "PRIVILEGE_NETWORK_READ") {
		t.Fatalf("pkg/qumulo/privileges.go no longer names PRIVILEGE_NETWORK_READ; re-check")
	}
	set := dfxReadmePrivilegeSet(t)
	if !set["PRIVILEGE_NETWORK_READ"] {
		t.Errorf("the chart README's privilege lists do not include PRIVILEGE_NETWORK_READ (GET /v3/network)")
	}
}

// dfxReadmePrivilegeSet is every PRIVILEGE_* named in the chart README's
// fenced privilege lists (the full set and the Replicating list).
func dfxReadmePrivilegeSet(t *testing.T) map[string]bool {
	t.Helper()
	text := dfxRead(t, dfxChartREADME)
	i := strings.Index(text, "Required Qumulo API privileges")
	if i < 0 {
		t.Fatalf("chart README has no \"Required Qumulo API privileges\" section")
	}
	set := map[string]bool{}
	for _, block := range regexp.MustCompile("(?s)```[a-z]*\\n(.*?)```").FindAllStringSubmatch(text[i:], -1) {
		for _, p := range regexp.MustCompile(`PRIVILEGE_[A-Z_]+`).FindAllString(block[1], -1) {
			set[p] = true
		}
	}
	return set
}

// TestDefect_ReadmeRoleHasEveryPrivilegeTheDriverCalls.
// Every privilege the driver's routes need (routePrivileges in
// pkg/qumulo/privileges.go) is in the chart README's privilege lists, and
// the two replication READ privileges are not offered as droppable. Ref:
// chart README "Required Qumulo API
// privileges".
func TestDefect_ReadmeRoleHasEveryPrivilegeTheDriverCalls(t *testing.T) {
	set := dfxReadmePrivilegeSet(t)
	need := map[string]bool{}
	for _, p := range regexp.MustCompile(`"(PRIVILEGE_[A-Z_]+)"`).FindAllStringSubmatch(dfxRead(t, "pkg/qumulo/privileges.go"), -1) {
		need[p[1]] = true
	}
	if len(need) == 0 {
		t.Fatalf("no privileges found in pkg/qumulo/privileges.go")
	}
	for p := range need {
		if !set[p] {
			t.Errorf("the driver calls a route needing %s, which the chart README's privilege lists do not name", p)
		}
	}
	for _, p := range []string{"PRIVILEGE_REPLICATION_SOURCE_READ", "PRIVILEGE_REPLICATION_TARGET_READ"} {
		if !set[p] {
			t.Errorf("chart README privilege set lacks %s; without it no volume can be deleted or expanded", p)
		}
	}
	if p := dfxDroppable(t); strings.Contains(p, "PRIVILEGE_REPLICATION_*_READ") {
		t.Errorf("chart README offers the replication READ privileges as droppable: %q", dfxClip(p))
	}
}

// TestDefect_ImportAnnotationUsesTheKubernetesIoPrefix.
// The pre-provisioned VolumeSnapshotContent annotations
// csi-snapshotter reads are snapshot.storage.kubernetes.io/deletion-secret-*;
// no doc may give the snapshot.storage.k8s.io/ prefix, and the chart README
// must give the right one. Ref:
// chart README:525-526.
func TestDefect_ImportAnnotationUsesTheKubernetesIoPrefix(t *testing.T) {
	docs := []string{dfxChartREADME, dfxRootREADME, dfxValuesYAML, "charts/qumulo-csi-driver/templates/NOTES.txt"}
	for _, doc := range docs {
		if !dfxExists(t, doc) {
			continue
		}
		for i, line := range strings.Split(dfxRead(t, doc), "\n") {
			if strings.Contains(line, "snapshot.storage.k8s.io/deletion-secret") {
				t.Errorf("%s:%d uses snapshot.storage.k8s.io/deletion-secret-*; csi-snapshotter reads snapshot.storage.kubernetes.io/deletion-secret-*: %s", doc, i+1, strings.TrimSpace(line))
			}
		}
	}
	readme := dfxRead(t, dfxChartREADME)
	for _, key := range []string{"snapshot.storage.kubernetes.io/deletion-secret-name", "snapshot.storage.kubernetes.io/deletion-secret-namespace"} {
		if !strings.Contains(readme, key) {
			t.Errorf("chart README never gives the annotation %s", key)
		}
	}
}
