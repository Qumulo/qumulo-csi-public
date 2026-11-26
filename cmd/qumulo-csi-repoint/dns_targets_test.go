// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// dnsPair is one DNS-named volume pair for the plans.
func dnsPair(n, sourceServer, mirrorServer string) failoverPair {
	pv := func(name string) *corev1.PersistentVolume {
		return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	vr := func(name string) volumeReplication {
		return volumeReplication{Namespace: "ns", Name: name, DesiredState: "primary"}
	}
	return failoverPair{
		Namespace: "ns", SourcePV: pv("pv" + n), MirrorPV: pv("pv" + n + "-dr"),
		SourceClaim: "c" + n, MirrorClaim: "c" + n + "-dr",
		SourceVR: vr("vr" + n), MirrorVR: vr("vr" + n + "-dr"),
		SourceServer: sourceServer, MirrorServer: mirrorServer,
	}
}

// Two pairs on one record whose live copies land on different arrays
// cannot share one A record; the first pair's target used to win silently.
func TestFailoverRefusesARecordThatWouldNeedTwoTargets(t *testing.T) {
	pairs := []failoverPair{
		dnsPair("1", "csi-a.lab", "192.0.2.2"),
		dnsPair("2", "csi-a.lab", "192.0.2.3"),
	}

	_, err := planFailover(pairs, directionFailover, failoverOptions{namespace: "ns", dnsTTL: 60, podsObserved: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "csi-a.lab")
	assert.Contains(t, err.Error(), "192.0.2.2")
	assert.Contains(t, err.Error(), "192.0.2.3")
}

// On failback the objects cannot say which array each record's class
// came from, so one bare address for two records is refused rather than
// pointing both at one array.
func TestFailbackRefusesOneBareTargetForSeveralRecords(t *testing.T) {
	pairs := []failoverPair{
		dnsPair("1", "csi-a.lab", "192.0.2.2"),
		dnsPair("2", "csi-c.lab", "192.0.2.2"),
	}

	_, err := planFailover(pairs, directionFailback,
		failoverOptions{namespace: "ns", dnsTTL: 60, podsObserved: true, dnsTarget: "192.0.2.1"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "csi-a.lab, csi-c.lab")
	assert.Contains(t, err.Error(), "record")
}

// The way through: an address per record.
func TestFailbackTakesAnAddressPerRecord(t *testing.T) {
	pairs := []failoverPair{
		dnsPair("1", "csi-a.lab", "192.0.2.2"),
		dnsPair("2", "csi-c.lab", "192.0.2.2"),
	}

	plan, err := planFailover(pairs, directionFailback, failoverOptions{
		namespace: "ns", dnsTTL: 60, podsObserved: true,
		dnsTarget: "csi-a.lab=192.0.2.1, csi-c.lab=192.0.2.4",
	})

	require.NoError(t, err)
	require.Len(t, plan.Flips, 2)
	assert.Equal(t, dnsFlip{Record: "csi-a.lab", From: "192.0.2.2", To: "192.0.2.1", Volumes: 1}, plan.Flips[0])
	assert.Equal(t, dnsFlip{Record: "csi-c.lab", From: "192.0.2.2", To: "192.0.2.4", Volumes: 1}, plan.Flips[1])
}

// One record, one bare address: today's single-class failback is unchanged.
func TestFailbackWithOneRecordStillTakesABareTarget(t *testing.T) {
	pairs := []failoverPair{
		dnsPair("1", "csi-a.lab", "192.0.2.2"),
		dnsPair("2", "csi-a.lab", "192.0.2.2"),
	}

	plan, err := planFailover(pairs, directionFailback,
		failoverOptions{namespace: "ns", dnsTTL: 60, podsObserved: true, dnsTarget: "192.0.2.1"})

	require.NoError(t, err)
	require.Len(t, plan.Flips, 1)
	assert.Equal(t, "192.0.2.1", plan.Flips[0].To)
	assert.Equal(t, 2, plan.Flips[0].Volumes)
}

func TestParseDNSTargetsRefusesAmbiguousInput(t *testing.T) {
	for _, value := range []string{
		"csi-a.lab=192.0.2.1,192.0.2.4", // mixed forms
		"csi-a.lab=",                    // no address
		"=192.0.2.1",                    // no record
		"csi-a.lab=192.0.2.1,csi-a.lab=192.0.2.4",
	} {
		_, err := parseDNSTargets(value)
		assert.Error(t, err, value)
	}
}

// A per-record address for a record no volume uses is a typo, and would leave
// the record that was meant unflipped.
func TestFailoverRefusesATargetForARecordItDoesNotFlip(t *testing.T) {
	_, err := planFailover([]failoverPair{dnsPair("1", "csi-a.lab", "192.0.2.2")}, directionFailback,
		failoverOptions{namespace: "ns", dnsTTL: 60, podsObserved: true, dnsTarget: "csi-typo.lab=192.0.2.1"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "csi-typo.lab")
}
