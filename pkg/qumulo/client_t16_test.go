// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The branches of the restore copy loop and of the tree-delete poll that no
// test reached.

// --- The copy loop's failure paths ---

// cancelAfterFirstReply cancels the copy's context once the first copy-chunk
// reply has been read in full, so the loop is between chunks -- not inside an
// HTTP call -- when it sees the cancel.
type cancelAfterFirstReply struct {
	base   http.RoundTripper
	cancel context.CancelFunc
	calls  atomic.Int64
}

func (c *cancelAfterFirstReply) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.base.RoundTrip(r)
	if err != nil || c.calls.Add(1) != 1 {
		return resp, err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	c.cancel()
	return resp, nil
}

func TestCopyFileStopsBetweenChunksWhenItsContextEnds(t *testing.T) {
	var served atomic.Int64
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		fmt.Fprint(w, `{"source_offset":"33554432","target_offset":"33554432"}`)
	}, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := client.HTTPClient.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.HTTPClient.Transport = &cancelAfterFirstReply{base: base, cancel: cancel}

	err := client.CopyFile(ctx, "/v/big.bin", "5", "42", 0)

	// The loop's own check, not a failed request: the bare context error,
	// before another copy-chunk call is even built.
	assert.Equal(t, context.Canceled, err)
	assert.Equal(t, int64(1), served.Load(), "no further chunk is asked for once the restore is cancelled")
}

func TestCopyFileReportsAnErrorOnALaterChunk(t *testing.T) {
	var calls atomic.Int64
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			fmt.Fprint(w, `{"source_offset":"33554432","target_offset":"33554432"}`)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error_class":"fs_invalid_argument_error","description":"bad offset"}`)
	}, "")

	err := client.CopyFile(context.Background(), "/v/big.bin", "5", "42", 0)

	require.Error(t, err, "a file is never reported copied after a chunk failed")
	assert.Contains(t, err.Error(), "HTTP 400")
	assert.Equal(t, int64(2), calls.Load())
}

func TestCopyFileStopsWhenTheClusterReportsNoProgress(t *testing.T) {
	client, calls := copyChunkServer(t, []*[2]string{
		offsets("33554432", "33554432"),
		offsets("33554432", "33554432"),
	})

	err := client.CopyFile(context.Background(), "/v/big.bin", "5", "42", 0)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no progress at offset 33554432")
	assert.Equal(t, int64(2), calls.Load(), "caught on the first repeat, not at the iteration cap")
}

func TestCopyFileRefusesOffsetsThatAreNotNumbers(t *testing.T) {
	for name, next := range map[string]*[2]string{
		"source": offsets("a lot", "33554432"),
		"target": offsets("33554432", "-1"),
	} {
		t.Run(name, func(t *testing.T) {
			client, calls := copyChunkServer(t, []*[2]string{next})

			err := client.CopyFile(context.Background(), "/v/big.bin", "5", "42", 0)

			require.Error(t, err)
			assert.True(t, IsMalformedReply(err), "the array answered; its answer is the problem: %v", err)
			assert.Contains(t, err.Error(), "not byte offsets")
			assert.Equal(t, int64(1), calls.Load())
		})
	}
}

// --- A bare 404 is not "the tree delete finished" ---

// A 404 with no Qumulo error body is what a proxy, an ingress or a wrong base
// URL answers. Read as "finished", DeleteVolume reported success for a volume
// still on the array (in another place).
func TestTreeDeleteFinishedIgnoresABare404WithoutAQumuloClass(t *testing.T) {
	for name, body := range map[string]string{
		"empty body":         ``,
		"proxy page":         `<html><body><h1>404 Not Found</h1></body></html>`,
		"JSON without class": `{"message":"not found"}`,
		"plain text, go mux": `404 page not found`,
	} {
		t.Run(name, func(t *testing.T) {
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, body)
			}, "")

			finished, err := client.TreeDeleteFinished(context.Background(), "12884901888")

			require.Error(t, err)
			assert.False(t, finished, "only the cluster's own 404 means the tree is gone")
			assert.Contains(t, err.Error(), "no Qumulo error body")
			var apiErr *APIError
			assert.True(t, errors.As(err, &apiErr), "the 404 itself stays inspectable: %v", err)
		})
	}
}
