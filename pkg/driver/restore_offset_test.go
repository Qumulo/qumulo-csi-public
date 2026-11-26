// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// A resumed restore must continue a partly copied file from the bytes already staged, not from byte 0.
// It used to start at source_offset "0" on every attempt.
func TestRestoreResumesAPartlyCopiedFileWhereItStopped(t *testing.T) {
	const (
		chunk    = uint64(32 << 20)
		fileSize = uint64(100 << 20) // four chunks
		staged   = uint64(64 << 20)  // two of them, from the attempt that ran out of time
	)
	var mu sync.Mutex
	var offsets []string
	var bytesAsked uint64
	array := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.EscapedPath(), "/copy-chunk") || r.Method != http.MethodPost {
			writeJSON(w, http.StatusNotFound, map[string]string{"description": "not modelled: " + r.URL.EscapedPath()})
			return
		}
		var req struct {
			SourceOffset string `json:"source_offset"`
			TargetOffset string `json:"target_offset"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		from, _ := strconv.ParseUint(req.SourceOffset, 10, 64)
		mu.Lock()
		offsets = append(offsets, req.SourceOffset)
		n := min(chunk, fileSize-min(from, fileSize))
		bytesAsked += n
		mu.Unlock()
		if from+n >= fileSize {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("null")) // complete
			return
		}
		next := strconv.FormatUint(from+n, 10)
		writeJSON(w, http.StatusOK, map[string]string{
			"source_id": "900", "source_offset": next, "target_offset": next,
			"length": strconv.FormatUint(fileSize-from-n, 10),
		})
	}))
	defer array.Close()

	cs := newIntegrationControllerServer(t, array.URL)
	entry := qumulo.DirEntry{Name: "big.bin", Path: "/csi-volumes/pvc-src/big.bin", ID: "900",
		Type: qumuloFileTypeRegular, Size: fileSize}
	copied, err := cs.restoreFile(context.Background(), qumulo.New(array.URL, "t"), "7", entry,
		"/csi-volumes/.csi-restoring-pvc-new", "/csi-volumes/.csi-restoring-pvc-new/big.bin", true, staged)
	require.NoError(t, err)
	require.True(t, copied)

	t.Logf("copy-chunk source offsets requested: %v (%d MiB of %d)", offsets, bytesAsked>>20, fileSize>>20)
	require.NotEmpty(t, offsets)
	if offsets[0] != fmt.Sprint(staged) {
		t.Errorf("the resumed copy started at source offset %s; want %d, where the staged bytes end", offsets[0], staged)
	}
	if bytesAsked > fileSize-staged {
		t.Errorf("the resumed copy asked for %d MiB; want at most the %d MiB still missing",
			bytesAsked>>20, (fileSize-staged)>>20)
	}
}
