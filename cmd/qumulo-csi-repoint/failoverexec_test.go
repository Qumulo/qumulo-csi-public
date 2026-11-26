// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Route53 step, which shells out to the `aws` CLI with whatever credentials
// the environment has, and which used to validate nothing at all before its
// UPSERT. Everything here runs against a recorded process runner: no AWS
// account, no network.

// fakeAWS records the argv of every `aws` call and replies from a script.
type fakeAWS struct {
	calls   [][]string
	stdout  []string
	stderr  string
	failAt  int // 1-based call number that fails; 0 means none fail
	callNum int
}

func (f *fakeAWS) run(_ context.Context, name string, args ...string) ([]byte, []byte, error) {
	f.callNum++
	f.calls = append(f.calls, append([]string{name}, args...))

	if f.failAt == f.callNum {
		return nil, []byte(f.stderr), fmt.Errorf("exit status 255")
	}
	if len(f.stdout) >= f.callNum {
		return []byte(f.stdout[f.callNum-1]), nil, nil
	}
	return []byte("{}"), nil, nil
}

func executorWithAWS(zone, profile string, aws *fakeAWS, out *bytes.Buffer) *clusterExecutor {
	return &clusterExecutor{
		poll: time.Millisecond, bound: 10 * time.Millisecond,
		route53Zone: zone, awsProfile: profile, runCommand: aws.run, out: out,
	}
}

const zoneReply = `{"HostedZone":{"Id":"/hostedzone/Z1D633PJN98FT9","Name":"example.com."}}`

// The shape of the id is checked before anything else: a typo that happens to
// be another zone in the same account is an UPSERT into it.
func TestValidateHostedZoneID(t *testing.T) {
	for _, good := range []string{"Z1D633PJN98FT9", "/hostedzone/Z1D633PJN98FT9", "Z123"} {
		require.NoError(t, validateHostedZoneID(good), good)
		assert.NotEmpty(t, normalizeHostedZoneID(good))
	}
	for _, bad := range []string{"", "z1d633pjn98ft9", "example.com", "Z", "Z_123", "  "} {
		err := validateHostedZoneID(bad)
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "not a hosted zone id")
	}
}

// And the record has to be inside the zone that is about to be changed. A
// record typed for one environment against another environment's zone id
// creates a new A record and points every pod at it one TTL later.
func TestRecordUnderZone(t *testing.T) {
	require.NoError(t, recordUnderZone("qumulo.example.com", "example.com."))
	require.NoError(t, recordUnderZone("example.com.", "example.com"))
	require.NoError(t, recordUnderZone("QUMULO.Example.COM", "example.com."))

	err := recordUnderZone("qumulo.other.com", "example.com.")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in hosted zone")

	// A suffix match must be on a label boundary: notexample.com is not in
	// example.com.
	require.Error(t, recordUnderZone("notexample.com", "example.com."))
	require.Error(t, recordUnderZone("qumulo.example.com", ""))
}

// The preflight is the whole of this guard: the zone is read, the record is checked
// against it and the exact change batch is printed -- before anything at all is
// changed, because a zone id typo found in step 3 is found with production
// already frozen and promoted.
func TestPreflightDNSReadsTheZoneBeforeAnythingIsChanged(t *testing.T) {
	aws := &fakeAWS{stdout: []string{zoneReply}}
	var out bytes.Buffer
	ex := executorWithAWS("/hostedzone/Z1D633PJN98FT9", "", aws, &out)

	flips := []dnsFlip{{Record: "qumulo.example.com", To: "192.0.2.20"}}
	require.NoError(t, ex.PreflightDNS(context.Background(), flips, 60))

	require.Len(t, aws.calls, 1)
	assert.Equal(t,
		[]string{"aws", "route53", "get-hosted-zone", "--id", "Z1D633PJN98FT9"}, aws.calls[0],
		"the zone is read, and nothing is changed")
	assert.Contains(t, out.String(), "hosted zone Z1D633PJN98FT9 is example.com.")
	assert.Contains(t, out.String(), `"Action": "UPSERT"`)
	assert.Contains(t, out.String(), `"Value": "192.0.2.20"`)
}

func TestPreflightDNSRefusesARecordOutsideTheZone(t *testing.T) {
	aws := &fakeAWS{stdout: []string{zoneReply}}
	var out bytes.Buffer
	ex := executorWithAWS("Z1D633PJN98FT9", "", aws, &out)

	err := ex.PreflightDNS(context.Background(),
		[]dnsFlip{{Record: "qumulo.elsewhere.com", To: "192.0.2.20"}}, 60)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in hosted zone example.com.")
	assert.Len(t, aws.calls, 1, "the zone read, and no change call")
}

// The lab's first apply failed "Your session has expired" because the
// profile holding the zone was not the default one, and the tool had no way to
// say which profile to use and no way to show what the CLI had said.
func TestAWSCallsCarryTheProfileAndFailFastWithStderr(t *testing.T) {
	aws := &fakeAWS{failAt: 1, stderr: "An error occurred (ExpiredToken): the session has expired"}
	var out bytes.Buffer
	ex := executorWithAWS("Z1D633PJN98FT9", "dr-lab", aws, &out)

	err := ex.PreflightDNS(context.Background(),
		[]dnsFlip{{Record: "qumulo.example.com", To: "192.0.2.20"}}, 60)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the session has expired", "the CLI's own stderr is shown")

	require.Len(t, aws.calls, 1)
	assert.Equal(t, []string{"aws", "--profile", "dr-lab", "route53", "get-hosted-zone",
		"--id", "Z1D633PJN98FT9"}, aws.calls[0])
	assert.Contains(t, out.String(), "profile dr-lab")
}

// The apply itself prints the exact change batch it is about to send, so what
// was changed is in the run's own output and not only in CloudTrail.
func TestFlipRecordPrintsTheChangeBatchAndPassesTheProfile(t *testing.T) {
	aws := &fakeAWS{}
	var out bytes.Buffer
	ex := executorWithAWS("Z1D633PJN98FT9", "dr-lab", aws, &out)

	require.NoError(t, ex.FlipRecord(context.Background(),
		dnsFlip{Record: "qumulo.example.com", To: "192.0.2.20"}, 60))

	require.Len(t, aws.calls, 1)
	assert.Equal(t, []string{"aws", "--profile", "dr-lab", "route53",
		"change-resource-record-sets", "--hosted-zone-id", "Z1D633PJN98FT9"}, aws.calls[0][:7])
	assert.Contains(t, out.String(), "applying this Route53 change batch to zone Z1D633PJN98FT9")
	assert.Contains(t, out.String(), `"TTL": 60`)
}

// An A record holds IPv4 and nothing else. A name would become a record nothing
// resolves; an IPv6 address belongs in an AAAA record this command does not
// write. Both are refused when the plan is built, not after the promote.
func TestPlanFailoverRefusesADNSTargetThatIsNotIPv4(t *testing.T) {
	_, err := route53ChangeBatch(dnsFlip{Record: "qumulo.example.com", To: "2001:db8::1"}, 60)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IPv6")

	pairs, err := derivePairs(failoverFixture(t, "qumulo.example.com"), "apps", "csi.qumulo.com")
	require.NoError(t, err)

	_, err = planFailover(pairs, directionFailback, failoverOptions{
		namespace: "apps", dnsTTL: 60, podsObserved: true,
		route53Zone: "Z1D633PJN98FT9", dnsTarget: "2001:db8::1",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IPv6")

	// And a malformed zone id never reaches the CLI either.
	_, err = planFailover(pairs, directionFailback, failoverOptions{
		namespace: "apps", dnsTTL: 60, podsObserved: true,
		route53Zone: "example.com", dnsTarget: "192.0.2.20",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a hosted zone id")
}

// The nudge gate. awaitSettled waits out a transient Resyncing or a
// Degraded reason of VolumeDegraded, and reports whether the object moved at
// all while it waited -- which is what decides whether a spec toggle is the
// remedy or an interruption.
func TestAwaitSettledWaitsOutAResyncAndReportsProgress(t *testing.T) {
	resyncing := volumeReplication{
		State:      replicationStatusSecondary,
		Conditions: map[string]string{conditionResyncing: "True", conditionDegraded: "True"},
		Reasons:    map[string]string{conditionDegraded: reasonVolumeDegraded},
	}
	settledObservation := volumeReplication{
		State:      replicationStatusSecondary,
		Conditions: map[string]string{conditionCompleted: "True", conditionDegraded: "False"},
	}

	queue := []volumeReplication{resyncing, resyncing, settledObservation}
	var reads int
	get := func(context.Context) (volumeReplication, error) {
		observed := queue[min(reads, len(queue)-1)]
		reads++
		return observed, nil
	}

	observed, progressed, err := awaitSettled(context.Background(), get, time.Millisecond, time.Second)
	require.NoError(t, err)
	assert.False(t, observed.reconciling(), "it waited for Resyncing and VolumeDegraded to clear")
	assert.True(t, progressed, "and it moved while waiting, so a toggle is not the remedy")
}

// An object that never clears is reported as still reconciling, and the caller
// refuses to toggle it.
func TestAwaitSettledGivesUpOnAnObjectThatNeverClears(t *testing.T) {
	stuck := volumeReplication{
		State:      replicationStatusSecondary,
		Conditions: map[string]string{conditionResyncing: "True"},
	}
	get := func(context.Context) (volumeReplication, error) { return stuck, nil }

	observed, progressed, err := awaitSettled(context.Background(), get, time.Millisecond,
		5*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, observed.reconciling())
	assert.False(t, progressed, "it never moved, which is the no-progress half of the gate")
}

// A Degraded with any other reason is not the resync's transient one, so it
// does not hold the toggle back.
func TestReconcilingIsOnlyResyncingOrVolumeDegraded(t *testing.T) {
	other := volumeReplication{
		Conditions: map[string]string{conditionDegraded: "True"},
		Reasons:    map[string]string{conditionDegraded: "FailedPrecondition"},
	}
	assert.False(t, other.reconciling())
	assert.Contains(t, other.describe(), "Degraded=True(FailedPrecondition)")

	volumeDegraded := volumeReplication{
		Conditions: map[string]string{conditionDegraded: "True"},
		Reasons:    map[string]string{conditionDegraded: reasonVolumeDegraded},
	}
	assert.True(t, volumeDegraded.reconciling())

	assert.True(t, volumeReplication{
		Conditions: map[string]string{conditionResyncing: "True"},
	}.reconciling())
}

// The fingerprint is what "made no progress" means: status, conditions, reasons
// and message, which is everything a reconcile writes back.
func TestFingerprintChangesWithAnythingTheControllerWrites(t *testing.T) {
	base := volumeReplication{
		State:      replicationStatusSecondary,
		Conditions: map[string]string{conditionCompleted: "False"},
		Reasons:    map[string]string{},
	}
	moved := base
	moved.Conditions = map[string]string{conditionCompleted: "True"}
	assert.NotEqual(t, base.fingerprint(), moved.fingerprint())

	same := base
	same.Conditions = map[string]string{conditionCompleted: "False"}
	assert.Equal(t, base.fingerprint(), same.fingerprint())

	assert.NotContains(t, strings.Join([]string{base.fingerprint()}, ""), "nil")
}
