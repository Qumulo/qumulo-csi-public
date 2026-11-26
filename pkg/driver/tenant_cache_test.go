// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qumulo-csi-driver/pkg/qumulo"
)

// namedTenantClient is a mock that names its endpoint, which is what lets
// listTenants hold a listing for it, and counts the listings it serves.
type namedTenantClient struct {
	*MockClient
	endpoint string
	listings atomic.Int32
}

func (c *namedTenantClient) Endpoint() string { return c.endpoint }

func newNamedTenantClient(t *testing.T, tenants *[]qumulo.TenantInfo) *namedTenantClient {
	t.Helper()
	c := &namedTenantClient{endpoint: "https://" + t.Name() + ":8000"}
	c.MockClient = &MockClient{ListTenantsFunc: func(context.Context) ([]qumulo.TenantInfo, error) {
		c.listings.Add(1)
		return *tenants, nil
	}}
	t.Cleanup(func() { tenantListings.Delete(c.endpoint) })
	return c
}

// One listing serves every create on the cluster for the TTL.
func TestTenantListingIsReusedAcrossCreates(t *testing.T) {
	tenants := []qumulo.TenantInfo{{ID: 1, Name: "Default", NFSEnabled: true}, {ID: 4, Name: "finance", NFSEnabled: true}}
	client := newNamedTenantClient(t, &tenants)
	srv := newTestControllerServer(t, client.MockClient)

	for i := 0; i < 3; i++ {
		id, err := srv.resolveTenantID(context.Background(), client, "finance")
		require.NoError(t, err)
		assert.Equal(t, 4, id)
	}
	assert.Equal(t, int32(1), client.listings.Load(), "the tenant table was listed on every call")
}

// A tenant added after the listing was read is found at once, not refused
// for the TTL.
func TestATenantMissingFromTheHeldListingIsReadAgain(t *testing.T) {
	tenants := []qumulo.TenantInfo{{ID: 1, Name: "Default", NFSEnabled: true}}
	client := newNamedTenantClient(t, &tenants)
	srv := newTestControllerServer(t, client.MockClient)

	_, err := srv.resolveTenantID(context.Background(), client, "1")
	require.NoError(t, err)

	tenants = append(tenants, qumulo.TenantInfo{ID: 7, Name: "new-team", NFSEnabled: true})
	id, err := srv.resolveTenantID(context.Background(), client, "new-team")
	require.NoError(t, err)
	assert.Equal(t, 7, id)
	assert.Equal(t, int32(2), client.listings.Load())

	// And a name that is in neither listing is still refused, after one re-read.
	_, err = srv.resolveTenantID(context.Background(), client, "nobody")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `no tenant named "nobody"`)
	assert.Equal(t, int32(3), client.listings.Load())
}

// A listing older than the TTL is read again.
func TestAnOldTenantListingIsReadAgain(t *testing.T) {
	tenants := []qumulo.TenantInfo{{ID: 1, Name: "Default", NFSEnabled: true}}
	client := newNamedTenantClient(t, &tenants)

	_, _, err := listTenants(context.Background(), client)
	require.NoError(t, err)
	v, ok := tenantListings.Load(client.endpoint)
	require.True(t, ok)
	aged := *v.(*tenantListing)
	aged.at = time.Now().Add(-tenantListingTTL - time.Second)
	tenantListings.Store(client.endpoint, &aged)

	_, cached, err := listTenants(context.Background(), client)
	require.NoError(t, err)
	assert.False(t, cached)
	assert.Equal(t, int32(2), client.listings.Load())
}

// A failed listing is not held, so the next call reads again.
func TestAFailedTenantListingIsNotHeld(t *testing.T) {
	client := &namedTenantClient{endpoint: "https://" + t.Name() + ":8000"}
	client.MockClient = &MockClient{ListTenantsFunc: func(context.Context) ([]qumulo.TenantInfo, error) {
		client.listings.Add(1)
		return nil, errors.New("boom")
	}}
	t.Cleanup(func() { tenantListings.Delete(client.endpoint) })

	for i := 0; i < 2; i++ {
		_, _, err := listTenants(context.Background(), client)
		require.Error(t, err)
	}
	assert.Equal(t, int32(2), client.listings.Load())
}

// A client that names no endpoint is never cached for: a listing is keyed by
// cluster, and such a client cannot say which one it is.
func TestATenantListingIsNotHeldForAnUnnamedClient(t *testing.T) {
	var listings int
	client := &MockClient{ListTenantsFunc: func(context.Context) ([]qumulo.TenantInfo, error) {
		listings++
		return []qumulo.TenantInfo{{ID: 1, Name: "Default", NFSEnabled: true}}, nil
	}}
	for i := 0; i < 2; i++ {
		_, cached, err := listTenants(context.Background(), client)
		require.NoError(t, err)
		assert.False(t, cached)
	}
	assert.Equal(t, 2, listings)
}

func TestForgetTenantsDropsTheHeldListing(t *testing.T) {
	tenants := []qumulo.TenantInfo{{ID: 1, Name: "Default", NFSEnabled: true}}
	client := newNamedTenantClient(t, &tenants)

	_, _, err := listTenants(context.Background(), client)
	require.NoError(t, err)
	forgetTenants(client)
	_, cached, err := listTenants(context.Background(), client)
	require.NoError(t, err)
	assert.False(t, cached)
	assert.Equal(t, int32(2), client.listings.Load())
}
