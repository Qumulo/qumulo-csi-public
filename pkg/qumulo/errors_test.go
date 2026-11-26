// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestAPIErrorMessageLeadsWithClass pins the message format.
//
// The description alone is not a usable message: Qumulo Core reports it as a
// bare numeral marked user_visible: false, and that string reaches operators
// through PVC events. The class has to come first.
func TestAPIErrorMessageLeadsWithClass(t *testing.T) {
	tests := []struct {
		name string
		err  *APIError
		want string
	}{
		{
			name: "class and description",
			err:  &APIError{StatusCode: 409, Path: "/v1/files/x/entries/", Msg: "20011", Class: ErrorClassEntryExists},
			want: "qumulo api /v1/files/x/entries/: HTTP 409: fs_entry_exists_error (20011)",
		},
		{
			name: "class only",
			err:  &APIError{StatusCode: 404, Path: "/v1/files/x/info/attributes", Class: ErrorClassNoSuchEntry},
			want: "qumulo api /v1/files/x/info/attributes: HTTP 404: fs_no_such_entry_error",
		},
		{
			name: "description only",
			err:  &APIError{StatusCode: 500, Path: "/v1/version", Msg: "boom"},
			want: "qumulo api /v1/version: HTTP 500: boom",
		},
		{
			name: "neither, e.g. a proxy with no JSON body",
			err:  &APIError{StatusCode: 502, Path: "/v1/version"},
			want: "qumulo api /v1/version: HTTP 502",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.err.Error())
		})
	}
}

func TestErrorClassAccessors(t *testing.T) {
	err := &APIError{StatusCode: 409, Class: ErrorClassEntryExists}

	assert.Equal(t, ErrorClassEntryExists, ErrorClassOf(err))
	assert.True(t, HasErrorClass(err, ErrorClassEntryExists))
	assert.False(t, HasErrorClass(err, ErrorClassNoSuchEntry))

	// A wrapped error must still be reachable, since callers wrap with %w.
	assert.Equal(t, ErrorClassEntryExists, ErrorClassOf(fmt.Errorf("creating directory: %w", err)))

	// Non-API errors, and an empty class, must not match.
	assert.Equal(t, "", ErrorClassOf(fmt.Errorf("plain error")))
	assert.False(t, HasErrorClass(fmt.Errorf("plain error"), ErrorClassEntryExists))
	assert.False(t, HasErrorClass(&APIError{StatusCode: 502}, ""))
}

// TestIsNotFoundAndIsConflictMatchClassOrStatus covers the two signals
// independently: a proxy can strip the JSON body, and a cluster version could
// report a known class under an unexpected status.
func TestIsNotFoundAndIsConflictMatchClassOrStatus(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantNotFound bool
		wantConflict bool
	}{
		{
			name:         "404 with matching class",
			err:          &APIError{StatusCode: http.StatusNotFound, Class: ErrorClassNoSuchEntry},
			wantNotFound: true,
		},
		{
			name:         "404 with no body",
			err:          &APIError{StatusCode: http.StatusNotFound},
			wantNotFound: true,
		},
		{
			name:         "no-such-entry class under an unexpected status",
			err:          &APIError{StatusCode: http.StatusInternalServerError, Class: ErrorClassNoSuchEntry},
			wantNotFound: true,
		},
		{
			name:         "409 with matching class",
			err:          &APIError{StatusCode: http.StatusConflict, Class: ErrorClassEntryExists},
			wantConflict: true,
		},
		{
			name:         "409 with no body",
			err:          &APIError{StatusCode: http.StatusConflict},
			wantConflict: true,
		},
		{
			name:         "entry-exists class under an unexpected status",
			err:          &APIError{StatusCode: http.StatusBadRequest, Class: ErrorClassEntryExists},
			wantConflict: true,
		},
		{
			name: "unrelated error is neither",
			err:  &APIError{StatusCode: http.StatusInternalServerError, Class: "fs_something_else"},
		},
		{
			name: "non-API error is neither",
			err:  fmt.Errorf("plain error"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantNotFound, IsNotFound(tc.err), "IsNotFound")
			assert.Equal(t, tc.wantConflict, IsConflict(tc.err), "IsConflict")
		})
	}
}

// Error classes observed on a real cluster during Phase 1 discovery and in
// earlier phases. Every one arrived alongside a status code that already
// classified it correctly, so these assertions guard the class path -- the
// sharper signal, and the one that keeps working behind a proxy that rewrites
// statuses.
func TestClassifiersRecognizeObservedErrorClasses(t *testing.T) {
	tests := []struct {
		class        string
		wantNotFound bool
		wantConflict bool
	}{
		{class: ErrorClassNoSuchEntry, wantNotFound: true},
		{class: ErrorClassNoSuchInode, wantNotFound: true},
		{class: ErrorClassQuotaInodeNotFound, wantNotFound: true},
		{class: ErrorClassQuotaLimitNotFound, wantNotFound: true},
		{class: ErrorClassEntryExists, wantConflict: true},
		{class: ErrorClassNFSExportAlreadyExists, wantConflict: true},
		// A failed create, not an absent object: the directory an export would
		// have served is missing. Treating it as "already gone" would let a
		// caller report failure as success.
		{class: ErrorClassNFSExportFSPathNotFound},
		// An identifier the cluster could not parse. Neither absent nor
		// conflicting: only the callers that know a malformed identifier
		// names nothing may act on it, via IsMalformedIDAt.
		{class: ErrorClassDecode},
	}

	for _, tt := range tests {
		t.Run(tt.class, func(t *testing.T) {
			// StatusCode is deliberately a 4xx that classifies as neither, so
			// only the class can satisfy the assertions.
			err := &APIError{StatusCode: 418, Class: tt.class}

			assert.Equal(t, tt.wantNotFound, IsNotFound(err), "IsNotFound")
			assert.Equal(t, tt.wantConflict, IsConflict(err), "IsConflict")
			assert.Equal(t, tt.class, ErrorClassOf(err))
		})
	}
}

// TestIsMalformedID: the class is necessary and not sufficient. Core answers
// HTTP 400 for several unrelated conditions, so a bare 400 must never read as
// "malformed" -- that would turn a failed export create into a reported
// success -- and decode_error itself is answered for a field missing from a
// request BODY as well as for an identifier missing from a URL, so it only
// means "this ID is unparseable" when it came from the lookup of that ID.
func TestIsMalformedID(t *testing.T) {
	const snapPath = "/v3/snapshots/999999999999"

	// Measured on Core 7.10.0: a GET of a snapshot ID the cluster cannot parse.
	assert.True(t, IsMalformedIDAt(&APIError{StatusCode: 400, Path: snapPath, Class: ErrorClassDecode}, snapPath))
	assert.True(t, IsMalformedIDAt(fmt.Errorf("getting snapshot: %w",
		&APIError{StatusCode: 400, Path: snapPath, Class: ErrorClassDecode}), snapPath))
	assert.True(t, IsMalformedSnapshotID(
		&APIError{StatusCode: 400, Path: snapPath, Class: ErrorClassDecode}, "999999999999"),
		"the snapshot helper builds the same path the client requests")

	// Core 7.9.2.2 answers decode_error "field 'id' not found" for a PUT
	// whose body is missing a field. That is a driver bug, not an absent
	// object, and it must not be mistaken for one.
	assert.False(t, IsMalformedIDAt(
		&APIError{StatusCode: 400, Path: "/v3/nfs/exports/4", Class: ErrorClassDecode, Msg: "field 'id' not found"},
		snapPath),
		"a decode_error from another request is a real failure")
	assert.False(t, IsMalformedIDAt(&APIError{StatusCode: 400, Path: snapPath}, snapPath),
		"a 400 with no class is not a parse failure")
	assert.False(t, IsMalformedIDAt(
		&APIError{StatusCode: 400, Path: snapPath, Class: ErrorClassNFSExportFSPathNotFound}, snapPath),
		"a missing export path is a failed create, not an unparseable ID")
	assert.False(t, IsMalformedIDAt(fmt.Errorf("plain error"), snapPath))

	// And it stays out of the two classifiers that mean "this is fine".
	err := &APIError{StatusCode: 400, Path: snapPath, Class: ErrorClassDecode}
	assert.False(t, IsNotFound(err))
	assert.False(t, IsConflict(err))
}

func TestIsTenantNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			// What Core 7.9.3 actually returns for a missing tenant.
			name: "the cluster's tenant error",
			err:  &APIError{StatusCode: 404, Class: ErrorClassTenantNotFound, Msg: "Connection tenant { id=99 } does not exist."},
			want: true,
		},
		{name: "a different 404 is not a tenant problem", err: &APIError{StatusCode: 404, Class: ErrorClassNoSuchEntry}, want: false},
		{name: "a bare 404 with no class", err: &APIError{StatusCode: 404}, want: false},
		{name: "a non-API error", err: errors.New("boom"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsTenantNotFound(tt.err))
		})
	}
}

func TestIsEmptyTenantInMultitenant(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			// What a multi-tenant cluster returns.
			name: "the cluster's multi-tenant refusal",
			err: &APIError{StatusCode: 400, Class: ErrorClassEmptyTenantInMultitenant,
				Msg: "Tenant must be specified when more than one tenant configured."},
			want: true,
		},
		{name: "matched on the class, not the text", err: &APIError{StatusCode: 400, Msg: "Tenant must be specified"}, want: false},
		{name: "a missing tenant is a different problem", err: &APIError{StatusCode: 404, Class: ErrorClassTenantNotFound}, want: false},
		{name: "a non-API error", err: errors.New("boom"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsEmptyTenantInMultitenant(tt.err))
		})
	}
}

// A nested-export refusal must be distinguishable from "already exists",
// even though both arrive as HTTP 409.
//
// This is the trap KI-1 sets. IsConflict matches on status, so it matches a
// tree-prefix refusal too -- which means any caller checking IsConflict first
// treats "an ancestor export is in the way" as "this export already exists"
// and reports the provision as an idempotent success. The order of the checks
// in ensureExport is load-bearing, and this pins the property that makes it
// so.
func TestExportTreeConflictIsAConflictButNotAnAlreadyExists(t *testing.T) {
	treePrefix := &APIError{
		StatusCode: http.StatusConflict,
		Class:      ErrorClassNFSExportTreePrefix,
		Path:       "/v3/nfs/exports/",
	}

	assert.True(t, IsExportTreeConflict(treePrefix),
		"the tree-prefix class must be recognised for what it is")
	assert.True(t, IsConflict(treePrefix),
		"it is a 409, so a status-based check matches it -- which is exactly why "+
			"IsExportTreeConflict has to be consulted first")

	alreadyExists := &APIError{
		StatusCode: http.StatusConflict,
		Class:      ErrorClassNFSExportAlreadyExists,
		Path:       "/v3/nfs/exports/",
	}
	assert.False(t, IsExportTreeConflict(alreadyExists),
		"a genuine already-exists must not be reported as an ancestor conflict")
	assert.True(t, IsConflict(alreadyExists))
}

// And it is not a not-found, which would make a delete report success.
func TestExportTreeConflictIsNotANotFound(t *testing.T) {
	treePrefix := &APIError{
		StatusCode: http.StatusConflict,
		Class:      ErrorClassNFSExportTreePrefix,
	}
	assert.False(t, IsNotFound(treePrefix))
}

// Only the cluster's own not-found classes say a file or snapshot is
// gone. A bare 404, or Core's unknown-route 404, does not.
func TestIsFileNotFoundAndIsSnapshotNotFoundNeedTheClass(t *testing.T) {
	for _, tt := range []struct {
		name           string
		err            error
		file, snapshot bool
	}{
		{name: "no such entry", err: &APIError{StatusCode: 404, Class: ErrorClassNoSuchEntry}, file: true},
		{name: "no such inode", err: &APIError{StatusCode: 404, Class: ErrorClassNoSuchInode}, file: true},
		{name: "no such snapshot", err: &APIError{StatusCode: 404, Class: ErrorClassSnapshotNotFound}, snapshot: true},
		{name: "a bare 404", err: &APIError{StatusCode: 404}},
		{name: "an unknown route", err: &APIError{StatusCode: 404, Class: "http_not_found_error"}},
		{name: "not an API error", err: errors.New("boom")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.file, IsFileNotFound(tt.err))
			assert.Equal(t, tt.snapshot, IsSnapshotNotFound(tt.err))
		})
	}
}
