// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

// panickingMounter panics where a mount or an unmount would run.
type panickingMounter struct{ mount.FakeMounter }

func (*panickingMounter) Mount(string, string, string, []string) error { panic("mount exploded") }
func (*panickingMounter) Unmount(string) error                         { panic("unmount exploded") }

// A panic in a goroutine the node plugin starts is outside every gRPC
// handler, so no interceptor sees it. It exited the plugin; it now fails the
// one operation.
func TestANodeGoroutinePanicFailsTheOperationNotThePlugin(t *testing.T) {
	d, err := NewDriver(Config{Name: "csi.qumulo.com", Version: "test", Mode: ModeNode, NodeID: "n", FakeMounter: true})
	require.NoError(t, err)
	ns := NewNodeServerWithMounter(d, &panickingMounter{})
	ns.mountTimeout = time.Second

	err = ns.unmountWithTimeoutOf(context.Background(), "/target", time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "recovered panic")

	err = ns.mountWithTimeout(context.Background(), "192.0.2.10:/v", "/target", "nfs", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "recovered panic")

	ns.writeProbe = func(string) error { panic("probe exploded") }
	err = ns.probeWithTimeout(context.Background(), "/target")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "recovered panic")
}

// With Kerberos on, a node with no keytab -- the chart mounts an empty
// placeholder there so other volumes still mount -- refuses a Kerberos mount
// with a clear message instead of an opaque mount.nfs failure.
func TestAKerberosMountNeedsAUsableKeytab(t *testing.T) {
	dir := t.TempDir()
	empty, full := dir+"/empty.keytab", dir+"/full.keytab"
	require.NoError(t, os.WriteFile(empty, nil, 0o600))
	require.NoError(t, os.WriteFile(full, []byte("keytab"), 0o600))

	for _, tc := range []struct {
		keytab string
		want   codes.Code
	}{
		{"", codes.OK},                    // Kerberos off
		{full, codes.OK},                  // a domain-joined node
		{empty, codes.FailedPrecondition}, // the placeholder
		{dir + "/missing", codes.FailedPrecondition},
	} {
		ns := &NodeServer{driver: &Driver{KerberosKeytab: tc.keytab}}
		assert.Equal(t, tc.want, status.Code(ns.requireKeytab()), "keytab %q", tc.keytab)
	}
}
