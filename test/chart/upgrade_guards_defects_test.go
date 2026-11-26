//go:build chart && defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package chart

// Upgrade-safety defects: what an upgrade render does against the objects a
// release already has. The render is `helm template --dry-run=server`
// against dfxFakeAPI, which is the render `helm upgrade` performs before it
// applies anything; Helm's apply and rollback themselves are not exercised.

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dfxClassesSets renders one class of each kind: a StorageClass, a
// VolumeSnapshotClass and a VolumeReplicationClass.
var dfxClassesSets = dfxSets(dfxBaseClass, []string{
	"sidecars.snapshotter.enabled=true",
	"volumeSnapshotClasses[0].name=qumulo-snapshots",
	"volumeSnapshotClasses[0].secretName=qumulo-token",
	"sidecars.csiAddons.enabled=true",
	"volumeReplicationClasses[0].name=qumulo-to-b",
	"volumeReplicationClasses[0].targetServer=cluster-b.example.com",
	`volumeReplicationClasses[0].targetApiUrl=https://cluster-b\.example\.com:8000`,
	`volumeReplicationClasses[0].apiUrl=https://qumulo\.example\.com:8000`,
	"volumeReplicationClasses[0].secretName=qumulo-replication-tokens",
})

var dfxClassKinds = []string{"StorageClass", "VolumeSnapshotClass", "VolumeReplicationClass"}

// dfxSeedOwned seeds every rendered object of the given kinds into the fake
// API, as objects this release owns -- the state after an install with the
// same values.
func dfxSeedOwned(t *testing.T, api *dfxFakeAPI, objs []dfxObj, kinds ...string) {
	t.Helper()
	for _, kind := range kinds {
		for _, o := range dfxFind(objs, kind) {
			body := map[string]any{}
			for k, v := range o {
				body[k] = v
			}
			ann := dfxMap(dfxGet(o, "metadata", "annotations"))
			meta := dfxHelmOwned(ann)
			body["metadata"] = meta
			api.seed(dfxStr(o["apiVersion"]), kind, o.namespace(), o.name(), body)
		}
	}
}

// TestDefect_ValuesLessUpgradeKeepsEveryClass: A `helm upgrade --set x=y`
// without the values file must not delete the
// release's StorageClasses, VolumeSnapshotClasses or VolumeReplicationClasses:
// either every class carries `helm.sh/resource-policy: keep` (Helm then never
// deletes it), or the upgrade render is refused, naming the classes.
// (templates/storageclass.yaml,
// volumesnapshotclass.yaml, volumereplicationclass.yaml.)
func TestDefect_ValuesLessUpgradeKeepsEveryClass(t *testing.T) {
	full := dfxRender(t, "", dfxClassesSets...)
	for _, kind := range dfxClassKinds {
		require.NotEmpty(t, dfxFind(full, kind), "the full render must hold a %s for this test to mean anything", kind)
	}

	// What the defect is: the --set-only render holds no classes, so every
	// class the release manages is absent from the new manifest.
	setOnly := dfxRender(t, "", "image.controller.tag=0.1.0-dfx")
	for _, kind := range dfxClassKinds {
		require.Empty(t, dfxFind(setOnly, kind), "a --set-only render unexpectedly holds a %s", kind)
	}

	// Answer 1: every class is annotated keep.
	var unkept []string
	for _, kind := range dfxClassKinds {
		for _, o := range dfxFind(full, kind) {
			if dfxStr(dfxGet(o, "metadata", "annotations", "helm.sh/resource-policy")) != "keep" {
				unkept = append(unkept, kind+" "+o.name())
			}
		}
	}

	// Answer 2: the upgrade render against a cluster holding the release's
	// classes is refused, naming them.
	api := dfxNewFakeAPI(t, dfxSnapshotClassResource, dfxReplicationClassResource)
	dfxSeedOwned(t, api, full, dfxClassKinds...)
	out, err := api.render(t, true, "", "image.controller.tag=0.1.0-dfx")
	refused := err != nil && strings.Contains(out, "qumulo-nfs")
	t.Logf("keep annotation missing on: %v", unkept)
	t.Logf("values-less upgrade render refused naming the classes: %v (err=%v)\n%s", refused, err, dfxTail(out))

	assert.True(t, len(unkept) == 0 || refused,
		"a values-less `helm upgrade --set` renders no classes, and Helm deletes every class the "+
			"release manages: these classes carry no helm.sh/resource-policy: keep (%v), and the "+
			"upgrade render was not refused", unkept)
}

// TestDefect_DuplicateClassNamesAreRefused covers the render part.
//
// Two classes of one kind with one name must be refused at render: they
// render two objects for one name, Helm applies one, and the other is left as
// an orphan Helm labels its own and never deletes.
// (templates/_validate.tpl.)
func TestDefect_DuplicateClassNamesAreRefused(t *testing.T) {
	cases := map[string][]string{
		"StorageClass": dfxSets(dfxBaseClass, []string{
			"storageClasses[1].name=qumulo-nfs",
			"storageClasses[1].server=other.example.com",
			"storageClasses[1].secretName=qumulo-token",
			"storageClasses[1].basePath=/csi/other",
		}),
		"VolumeSnapshotClass": {
			"sidecars.snapshotter.enabled=true",
			"volumeSnapshotClasses[0].name=snaps",
			"volumeSnapshotClasses[0].secretName=tok",
			"volumeSnapshotClasses[1].name=snaps",
			"volumeSnapshotClasses[1].secretName=tok2",
		},
	}
	for kind, sets := range cases {
		t.Run(kind, func(t *testing.T) {
			out, err := dfxTryRender(t, "", sets...)
			assert.Error(t, err, "two %ss named alike rendered (exit 0); Helm applies one and orphans the other", kind)
			if err != nil {
				assert.NotContains(t, out, "additional properties", "refused for an unrelated schema reason:\n%s", out)
			}
		})
	}
}

// TestDefect_ImmutableClassChangeIsRefusedBeforeApply covers the lookup
// part.
//
// An upgrade that would change an immutable field of a class the release
// already has (here a StorageClass's parameters) must fail the render, naming
// the class and the field, before Helm applies anything -- not roll the new
// images and then report UPGRADE FAILED. The same upgrade with unchanged
// classes must still render.
func TestDefect_ImmutableClassChangeIsRefusedBeforeApply(t *testing.T) {
	installed := dfxRender(t, "", dfxBaseClass...)

	t.Run("unchanged class still upgrades", func(t *testing.T) {
		api := dfxNewFakeAPI(t)
		dfxSeedOwned(t, api, installed, "StorageClass", "CSIDriver", "PriorityClass")
		out, err := api.render(t, true, "", dfxSets(dfxBaseClass, []string{"image.controller.tag=0.1.0-dfx"})...)
		require.NoError(t, err, "an upgrade that changes no immutable field was refused:\n%s", out)
	})

	t.Run("changed parameters are refused", func(t *testing.T) {
		api := dfxNewFakeAPI(t)
		dfxSeedOwned(t, api, installed, "StorageClass", "CSIDriver", "PriorityClass")
		sets := dfxSets(dfxBaseClass, []string{
			"storageClasses[0].basePath=/csi/elsewhere",
			"image.controller.tag=0.1.0-dfx",
		})
		out, err := api.render(t, true, "", sets...)
		require.Error(t, err, "the render of an upgrade changing an immutable StorageClass field succeeded, "+
			"so Helm would roll the images and then fail on the class (requests: %s)", api.requests())
		assert.Contains(t, out, "qumulo-nfs", "the refusal must name the class:\n%s", out)
		assert.Contains(t, strings.ToLower(out), "parameters", "the refusal must name the field:\n%s", out)
	})
}

// TestDefect_MissingClassCRDIsRefusedClearly: When the cluster does not serve
// the VolumeSnapshotClass or
// VolumeReplicationClass CRD, a render that sets those classes must fail with
// a message naming the chart setting and the missing CRD -- not Helm's
// generic "no matches for kind". With the CRD present it must render.
// (templates/volumesnapshotclass.yaml:12,
// volumereplicationclass.yaml:47.) The part of this defect that is Helm's own --
// a release record naming a kind the cluster no longer serves wedges every
// later upgrade and rollback -- needs a real Helm release and is not covered.
func TestDefect_MissingClassCRDIsRefusedClearly(t *testing.T) {
	cases := []struct {
		name     string
		res      dfxResource
		crd      string
		valueKey string
		sets     []string
	}{
		{
			name: "VolumeSnapshotClass", res: dfxSnapshotClassResource,
			crd: "volumesnapshotclasses.snapshot.storage.k8s.io", valueKey: "volumeSnapshotClasses",
			sets: dfxSets(dfxBaseClass, []string{
				"sidecars.snapshotter.enabled=true",
				"volumeSnapshotClasses[0].name=qumulo-snapshots",
				"volumeSnapshotClasses[0].secretName=qumulo-token",
			}),
		},
		{
			name: "VolumeReplicationClass", res: dfxReplicationClassResource,
			crd: "volumereplicationclasses.replication.storage.openshift.io", valueKey: "volumeReplicationClasses",
			sets: dfxSets(dfxBaseClass, []string{
				"sidecars.csiAddons.enabled=true",
				"volumeReplicationClasses[0].name=qumulo-to-b",
				"volumeReplicationClasses[0].targetServer=cluster-b.example.com",
				`volumeReplicationClasses[0].targetApiUrl=https://cluster-b\.example\.com:8000`,
				`volumeReplicationClasses[0].apiUrl=https://qumulo\.example\.com:8000`,
				"volumeReplicationClasses[0].secretName=qumulo-replication-tokens",
			}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name+" CRD present renders", func(t *testing.T) {
			api := dfxNewFakeAPI(t, tc.res)
			api.seed("apiextensions.k8s.io/v1", "CustomResourceDefinition", "", tc.crd, nil)
			out, err := api.render(t, false, "", tc.sets...)
			require.NoError(t, err, "render with the CRD served was refused:\n%s", out)
		})
		t.Run(tc.name+" CRD missing is refused by the chart", func(t *testing.T) {
			api := dfxNewFakeAPI(t)
			out, err := api.render(t, false, "", tc.sets...)
			require.Error(t, err, "rendered %s with no CRD to serve it", tc.name)
			assert.Contains(t, out, tc.valueKey,
				"the refusal must name the chart setting (%s) and the missing CRD, not only Helm's "+
					"mapping error:\n%s", tc.valueKey, dfxTail(out))
		})
	}
}

// TestDefect_DriverNameChangeIsRefused: An upgrade that changes driverName must
// fail the render with a message
// naming driverName, before anything is applied -- every existing PV names
// the old driver. The same upgrade without the change must render.
// (templates/csidriver.yaml:5.)
func TestDefect_DriverNameChangeIsRefused(t *testing.T) {
	installed := dfxRender(t, "", dfxBaseClass...)
	t.Run("same driverName upgrades", func(t *testing.T) {
		api := dfxNewFakeAPI(t)
		dfxSeedOwned(t, api, installed, "StorageClass", "CSIDriver", "PriorityClass")
		out, err := api.render(t, true, "", dfxBaseClass...)
		require.NoError(t, err, "an upgrade with the same driverName was refused:\n%s", out)
	})
	t.Run("changed driverName is refused", func(t *testing.T) {
		api := dfxNewFakeAPI(t)
		// Only the CSIDriver: a class carrying the old provisioner would be
		// refused for its parameters by the immutable-class guard, which is not the check
		// under test.
		dfxSeedOwned(t, api, installed, "CSIDriver", "PriorityClass")
		out, err := api.render(t, true, "", dfxSets(dfxBaseClass, []string{"driverName=csi2.qumulo.com"})...)
		require.Error(t, err, "a driverName change rendered; Helm would apply a second CSIDriver and "+
			"re-register every node under the new name before failing on the classes")
		assert.Contains(t, out, "driverName", "the refusal must name driverName:\n%s", dfxTail(out))
	})
}

// TestDefect_PriorityClassValueChangeIsRefused: node.priorityClass.value is
// immutable on the live PriorityClass; an
// upgrade changing it must fail the render naming the PriorityClass, not fail
// the apply and leave the release `failed`. The same upgrade without the
// change must render. (templates/priorityclass.yaml:12.)
func TestDefect_PriorityClassValueChangeIsRefused(t *testing.T) {
	installed := dfxRender(t, "", dfxBaseClass...)
	require.NotEmpty(t, dfxFind(installed, "PriorityClass"), "the default render has no PriorityClass")
	t.Run("same value upgrades", func(t *testing.T) {
		api := dfxNewFakeAPI(t)
		dfxSeedOwned(t, api, installed, "StorageClass", "CSIDriver", "PriorityClass")
		out, err := api.render(t, true, "", dfxBaseClass...)
		require.NoError(t, err, "an upgrade with the same PriorityClass value was refused:\n%s", out)
	})
	t.Run("changed value is refused", func(t *testing.T) {
		api := dfxNewFakeAPI(t)
		dfxSeedOwned(t, api, installed, "StorageClass", "CSIDriver", "PriorityClass")
		out, err := api.render(t, true, "", dfxSets(dfxBaseClass, []string{"node.priorityClass.value=2000000"})...)
		require.Error(t, err, "a PriorityClass value change rendered; the apply then fails on the "+
			"immutable field and leaves the release failed")
		assert.Contains(t, out, "PriorityClass", "the refusal must name the PriorityClass:\n%s", dfxTail(out))
	})
}

// dfxTail keeps the last lines of helm output, which is where the error is.
func dfxTail(out string) string {
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) > 12 {
		lines = lines[len(lines)-12:]
	}
	return strings.Join(lines, "\n")
}
