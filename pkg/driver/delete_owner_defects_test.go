//go:build defects

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// dfxOwnerServer is the integration controller with a PV lister reporting pvs.
func dfxOwnerServer(t *testing.T, baseURL string, pvs ...PersistentVolumeInfo) *ControllerServer {
	t.Helper()
	lister := &stubPVLister{}
	lister.set(pvs...)
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "0.1.0",
		NewClient:              func(_, token string) qumulo.ClientInterface { return qumulo.New(baseURL, token) },
		PersistentVolumeLister: lister,
	})
	require.NoError(t, err)
	return NewControllerServer(d)
}

// (a): a static PV carrying a verbatim copy of a Bound
// claim's handle (reclaim Delete, provisioned-by annotation) is released; its
// DeleteVolume must remove nothing of the owner's volume, which stays Bound.
// Before the fix the owner's export, quota and directory were deleted and
// the owner claim left Bound.
func TestDefect_DeleteVolumeRemovesNothingForACopiedHandleWhileItsOwnerIsBound(t *testing.T) {
	c := newFakeCluster()
	c.directories["/csi/volumes"] = "20"
	c.directories["/csi/volumes/pvc-bf80278c"] = "21"
	c.quotas["21"] = "1073741824"
	c.exports["e21"] = fakeExport{ExportPath: "/pvc-bf80278c", FSPath: "/csi/volumes/pvc-bf80278c",
		Description: exportDescriptionFor("pvc-bf80278c")}
	l := &dfxDestructiveCallLog{fakeCluster: c}
	server := httptest.NewServer(l.handler())
	defer server.Close()
	handle := testServer + "##/csi/volumes/pvc-bf80278c"

	_, err := dfxOwnerServer(t, server.URL,
		PersistentVolumeInfo{Name: "pvc-bf80278c", Handle: handle, Phase: "Bound", ClaimNamespace: "default", ClaimName: "p93-owner"},
		PersistentVolumeInfo{Name: "p93-copy-pv", Handle: handle, Phase: "Released", ClaimNamespace: "default", ClaimName: "p93-copy"},
	).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})

	l.mu.Lock()
	defer l.mu.Unlock()
	t.Logf("DeleteVolume answered %v; tree deletes %v, exports deleted %v, quotas deleted %v",
		err, l.treeDeletes, l.exportDels, l.quotaDels)
	assert.Error(t, err, "the copy's delete was answered OK")
	assert.Empty(t, l.treeDeletes, "the owner's directory was tree-deleted")
	assert.Empty(t, l.exportDels, "the owner's export was deleted")
	assert.Empty(t, l.quotaDels, "the owner's quota was deleted")
	assert.Contains(t, l.directories, "/csi/volumes/pvc-bf80278c")
}

// (b): a static PV naming a directory an administrator
// made -- a quota, no export, contents -- must not be tree-deleted: "has a
// quota" is no proof this driver made it. The PV is not named after the
// directory and carries no csi.qumulo.com/delete-directory opt-in, so it does
// not own it (refuseUnlessReleasedPVOwnsIt).
// Before the fix the directory and its child were tree-deleted. See
// controller_server.go confirmVolumeIdentity.
func TestDefect_DeleteVolumeRemovesNothingForAQuotaOnlyDirectoryItNeverMade(t *testing.T) {
	c := newFakeCluster()
	c.directories["/csi/volumes"] = "20"
	c.directories["/csi/volumes/p93-quota-noexp"] = "30"
	c.directories["/csi/volumes/p93-quota-noexp/inner"] = "31"
	c.quotas["30"] = "1073741824"
	l := &dfxDestructiveCallLog{fakeCluster: c}
	server := httptest.NewServer(l.handler())
	defer server.Close()
	handle := testServer + "##/csi/volumes/p93-quota-noexp"

	_, err := dfxOwnerServer(t, server.URL,
		PersistentVolumeInfo{Name: "p93-quota-pv", Handle: handle, Phase: "Released", ClaimNamespace: "default", ClaimName: "p93-quota"},
	).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})

	l.mu.Lock()
	defer l.mu.Unlock()
	t.Logf("DeleteVolume answered %v; tree deletes %v, quotas deleted %v", err, l.treeDeletes, l.quotaDels)
	assert.Empty(t, l.treeDeletes, "the administrator's directory was tree-deleted")
	assert.Empty(t, l.quotaDels, "the administrator's quota was deleted")
}

// dfxOwnerCluster is an array with one dedicated-export volume,
// pvc-bf80278c, with its quota and its own described export.
func dfxOwnerCluster() *dfxDestructiveCallLog {
	c := newFakeCluster()
	c.directories["/csi/volumes"] = "20"
	c.directories["/csi/volumes/pvc-bf80278c"] = "21"
	c.quotas["21"] = "1073741824"
	c.exports["e21"] = fakeExport{ExportPath: "/pvc-bf80278c", FSPath: "/csi/volumes/pvc-bf80278c",
		Description: exportDescriptionFor("pvc-bf80278c")}
	return &dfxDestructiveCallLog{fakeCluster: c}
}

// A static PV whose handle copies a Bound claim's but pins a
// different REST endpoint of the same array is released; its DeleteVolume
// must remove nothing of the owner's volume. sameArray cannot match two
// explicit endpoints of one array, so the PV-in-use check missed the owner.
// Before the fix the owner's export, quota and directory were deleted
// while the owner stayed Bound.
func TestDefect_DeleteVolumeRemovesNothingForACopyPinningAnotherEndpointWhileItsOwnerIsBound(t *testing.T) {
	l := dfxOwnerCluster()
	server := httptest.NewServer(l.handler())
	defer server.Close()
	owner := testServer + "#https://10.0.0.1:8000#/csi/volumes/pvc-bf80278c"
	copied := testServer + "#https://10.0.0.2:8000#/csi/volumes/pvc-bf80278c"

	_, err := dfxOwnerServer(t, server.URL,
		PersistentVolumeInfo{Name: "pvc-bf80278c", Handle: owner, Phase: "Bound", ClaimNamespace: "default", ClaimName: "p113-owner"},
		PersistentVolumeInfo{Name: "p113-copy-pv", Handle: copied, Phase: "Released", ReclaimPolicy: "Delete",
			ClaimNamespace: "default", ClaimName: "p113-copy"},
	).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: copied, Secrets: testSecrets()})

	l.mu.Lock()
	defer l.mu.Unlock()
	t.Logf("DeleteVolume answered %v; tree deletes %v, exports deleted %v, quotas deleted %v",
		err, l.treeDeletes, l.exportDels, l.quotaDels)
	assert.Error(t, err, "the copy's delete was answered OK")
	assert.Empty(t, l.treeDeletes, "the owner's directory was tree-deleted")
	assert.Empty(t, l.exportDels, "the owner's export was deleted")
	assert.Empty(t, l.quotaDels, "the owner's quota was deleted")
}

// The owner PV was released with reclaim Retain -- its data is to
// be kept -- and a static PV copying its handle, reclaim Delete, is released
// too. The copy's DeleteVolume must remove nothing, whether the owner's PV
// object is still there or was removed: no PV in use names the directory, so
// the PV-in-use check let it through.
// Ref: reproduced live on kind; delete_owner.go.
func TestDefect_DeleteVolumeRemovesNothingForACopyOfAnOwnerReleasedWithRetain(t *testing.T) {
	handle := testServer + "##/csi/volumes/pvc-bf80278c"
	owner := PersistentVolumeInfo{Name: "pvc-bf80278c", Handle: handle, Phase: "Released", ReclaimPolicy: "Retain",
		ClaimNamespace: "default", ClaimName: "p113-owner"}
	copied := PersistentVolumeInfo{Name: "p113-copy-pv", Handle: handle, Phase: "Released", ReclaimPolicy: "Delete",
		ClaimNamespace: "default", ClaimName: "p113-copy"}
	for name, pvs := range map[string][]PersistentVolumeInfo{
		"owner PV removed":  {copied},
		"owner PV retained": {owner, copied},
	} {
		t.Run(name, func(t *testing.T) {
			l := dfxOwnerCluster()
			server := httptest.NewServer(l.handler())
			defer server.Close()

			_, err := dfxOwnerServer(t, server.URL, pvs...).
				DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: handle, Secrets: testSecrets()})

			l.mu.Lock()
			defer l.mu.Unlock()
			t.Logf("DeleteVolume answered %v; tree deletes %v, exports deleted %v, quotas deleted %v",
				err, l.treeDeletes, l.exportDels, l.quotaDels)
			assert.Error(t, err, "the copy's delete was answered OK")
			assert.Empty(t, l.treeDeletes, "the retained directory was tree-deleted")
			assert.Empty(t, l.exportDels, "the retained volume's export was deleted")
			assert.Empty(t, l.quotaDels, "the retained volume's quota was deleted")
		})
	}
}
