// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// The end-of-run summary, and why it is the shape it is.
//
// The lab's recorded "63 s one-command failover" was an **idempotent re-run**:
// every object was already in its target state, so the run promoted nothing,
// demoted nothing, and spent 3 s on checks and 60 s waiting out the TTL. The
// run that did the work took 39 s and performed one promote and two demotes,
// the rest having been moved by an earlier attempt and by hand. Four documents
// then quoted the 63 s as the measured failover. Nothing in the tool's output
// made that wrong -- it printed "achieved state" for every object and said
// nothing about which of them had *changed*.
//
// So every run now ends with two things a reader cannot misread: a top line
// classifying the run (cold, re-run, or partial, with the count), and one line
// per object saying the transition actually made -- `secondary -> primary`, or
// `already primary, no change`. The same summary is emitted as a single line of
// JSON behind a fixed prefix, so a lab harness records the classification
// rather than a wall-clock number somebody else has to interpret.

// summaryPrefix is what a harness greps for. One line, fixed, machine-readable.
const summaryPrefix = "qumulo-csi-repoint-summary: "

// Run classifications. `cold` is the only one that measures a failover.
const (
	runCold    = "cold"
	runRerun   = "rerun"
	runPartial = "partial"
	runNone    = "none"
)

// phaseTiming is one numbered step and what it cost.
type phaseTiming struct {
	Step    int     `json:"step"`
	Name    string  `json:"name"`
	Seconds float64 `json:"seconds"`
}

// objectTransition is what actually happened to one object in one step.
type objectTransition struct {
	Step   string `json:"step"`
	Object string `json:"object"`
	// Transition is the sentence the operator reads: "secondary -> primary", or
	// "already primary, no change".
	Transition string  `json:"transition"`
	Changed    bool    `json:"changed"`
	Seconds    float64 `json:"seconds"`
	// Detail carries the object's own words when it did not get there.
	Detail string `json:"detail,omitempty"`
}

// runSummary is the whole of what one apply did.
type runSummary struct {
	Command   string `json:"command"`
	Namespace string `json:"namespace"`
	// Run is cold, rerun, partial or none. See classifyRun.
	Run       string `json:"run"`
	RunReason string `json:"runReason"`
	// Promotes and PromotesChanged are what the classification rests on.
	Promotes        int                `json:"promotes"`
	PromotesChanged int                `json:"promotesChanged"`
	Demotes         int                `json:"demotes"`
	DemotesChanged  int                `json:"demotesChanged"`
	Phases          []phaseTiming      `json:"phases"`
	Transitions     []objectTransition `json:"transitions"`
	TotalSeconds    float64            `json:"totalSeconds"`
	// Incomplete names the objects that did not reach the state the plan wanted,
	// so a summary is never read as a success it was not.
	Incomplete []string `json:"incomplete,omitempty"`
}

// timer accumulates one run's phases in the order they ran.
type timer struct {
	summary runSummary
	started time.Time
	phase   time.Time
}

func newTimer(dir direction, namespace string) *timer {
	now := time.Now()
	return &timer{
		summary: runSummary{Command: string(dir), Namespace: namespace},
		started: now,
		phase:   now,
	}
}

// startPhase marks the beginning of a numbered step.
func (t *timer) startPhase() { t.phase = time.Now() }

// endPhase records a numbered step's elapsed time.
func (t *timer) endPhase(step int, name string) {
	t.summary.Phases = append(t.summary.Phases, phaseTiming{
		Step: step, Name: name, Seconds: round(time.Since(t.phase)),
	})
}

// record adds one object's transition.
func (t *timer) record(tr objectTransition) {
	t.summary.Transitions = append(t.summary.Transitions, tr)
}

// transitionOf turns a before-and-after into the sentence and the fact.
//
// "Before" is the object's **status**, not its spec: a VolumeReplication whose
// `spec.replicationState` already says `primary` may still be `Secondary` with
// the promote unfinished, and a run that treated the spec as the before state
// would report the work it did as no change.
func transitionOf(fromState string, fromCompleted bool, wantState string, reached bool) (string, bool) {
	already := strings.EqualFold(fromState, wantState) && fromCompleted
	want := strings.ToLower(wantState)

	switch {
	case already && reached:
		return "already " + want + ", no change", false
	case already:
		// It read the wanted role before and does not now: something moved it
		// the other way, which is worth saying out loud.
		return "was already " + want + ", and is not now", true
	case reached:
		return lowerOrUnknown(fromState) + " -> " + want, true
	default:
		return lowerOrUnknown(fromState) + " -> " + want + " DID NOT COMPLETE", true
	}
}

func lowerOrUnknown(state string) string {
	if strings.TrimSpace(state) == "" {
		return "unknown"
	}
	return strings.ToLower(state)
}

// classifyRun is the top line, and the whole point of the summary.
//
// A run that promoted nothing measured nothing. A run that promoted some of the
// objects measured the ones it moved and nothing about the rest. Only a run in
// which **every** promote changed state is an end-to-end failover, and only that
// one may be quoted as a failover time.
func classifyRun(promotes, changed int) (string, string) {
	switch {
	case promotes == 0:
		return runNone, "this plan contained no promote at all"
	case changed == promotes:
		return runCold, fmt.Sprintf("COLD RUN: every promote changed state (%d of %d). "+
			"This run's timings measure a failover", changed, promotes)
	case changed == 0:
		return runRerun, fmt.Sprintf("RE-RUN: no promote changed state (0 of %d) -- every object was "+
			"already in its target state. This run's timings measure checks and waits, NOT a failover",
			promotes)
	default:
		return runPartial, fmt.Sprintf("PARTIAL RUN: %d of %d promotes changed state; the rest were "+
			"already there. This run's timings measure only the %d that moved",
			changed, promotes, changed)
	}
}

// finish classifies the run and writes it, human-readable and then machine-
// readable, as the last thing the command prints.
func (t *timer) finish(out io.Writer) runSummary {
	s := &t.summary
	s.TotalSeconds = round(time.Since(t.started))

	for _, tr := range s.Transitions {
		switch tr.Step {
		case "promote":
			s.Promotes++
			if tr.Changed {
				s.PromotesChanged++
			}
		case "demote":
			s.Demotes++
			if tr.Changed {
				s.DemotesChanged++
			}
		}
		if strings.Contains(tr.Transition, "DID NOT COMPLETE") {
			s.Incomplete = append(s.Incomplete, tr.Step+" "+tr.Object)
		}
	}
	s.Run, s.RunReason = classifyRun(s.Promotes, s.PromotesChanged)

	writef(out, "#\n# ===== %s summary =====\n", s.Command)
	writef(out, "# run: %s -- %s\n", strings.ToUpper(s.Run), s.RunReason)

	writef(out, "# phases:\n")
	for _, phase := range s.Phases {
		writef(out, "#   step %d %-8s %6.1fs\n", phase.Step, phase.Name, phase.Seconds)
	}
	writef(out, "#   %-13s %6.1fs\n", "total", s.TotalSeconds)

	writef(out, "# transitions:\n")
	for _, tr := range s.Transitions {
		detail := ""
		if tr.Detail != "" {
			detail = " -- " + tr.Detail
		}
		writef(out, "#   %-7s %s: %s (%.1fs)%s\n", tr.Step, tr.Object, tr.Transition, tr.Seconds, detail)
	}
	for _, name := range s.Incomplete {
		writef(out, "# WARNING: %s did not complete; this run is not a finished %s\n", name, s.Command)
	}

	encoded, err := json.Marshal(s)
	if err != nil {
		// Nothing to recover: the human-readable summary above is already out,
		// and it carries the same facts.
		writef(out, "# (the machine-readable summary could not be encoded: %v)\n", err)
		return *s
	}
	writef(out, "%s%s\n", summaryPrefix, encoded)
	return *s
}

// round keeps a tenth of a second, which is the resolution any of this is
// measured to and more than a change review needs.
func round(d time.Duration) float64 {
	return float64(d.Round(100*time.Millisecond)) / float64(time.Second)
}
