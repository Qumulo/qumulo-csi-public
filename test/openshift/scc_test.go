//go:build openshift

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package openshift

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// RunAsUserStrategy is the SCC's runAsUser strategy type.
type RunAsUserStrategy string

const (
	// MustRunAsRange confines the UID to the namespace's
	// openshift.io/sa.scc.uid-range annotation. This is restricted-v2.
	MustRunAsRange RunAsUserStrategy = "MustRunAsRange"
	// MustRunAsNonRoot requires a non-zero UID but constrains it no further.
	// This is nonroot-v2.
	MustRunAsNonRoot RunAsUserStrategy = "MustRunAsNonRoot"
	// RunAsAny imposes nothing. This is privileged and hostnetwork-v2.
	RunAsAny RunAsUserStrategy = "RunAsAny"
)

// SCC is the subset of an OpenShift SecurityContextConstraints that decides
// admission for this chart's workloads.
//
// These are models of the OpenShift 4.x built-ins, transcribed from their
// documented definitions. They are not read from a cluster, so a cluster whose
// built-ins have been edited -- which OpenShift permits and some customers do
// -- is outside what this suite can see. That limit is the point of the
// `oc get scc` step in README.md.
type SCC struct {
	Name                     string
	AllowPrivilegedContainer bool
	AllowHostNetwork         bool
	AllowHostPorts           bool
	AllowHostDirVolumePlugin bool
	AllowPrivilegeEscalation bool
	AllowHostPID             bool
	AllowHostIPC             bool
	RunAsUser                RunAsUserStrategy
	// UIDRange applies only to MustRunAsRange. It is the namespace's assigned
	// range; 1000670000-1000679999 is a representative OpenShift allocation.
	UIDRange [2]int64
	// RequiredDropCapabilities must appear in every container's drop list.
	RequiredDropCapabilities []string
	// Volumes are the permitted volume source types; "*" permits all.
	Volumes []string
}

// Representative namespace UID range. The exact numbers differ per namespace;
// what matters to every assertion here is that it is a high range that does
// not contain 65532.
var openShiftNamespaceUIDRange = [2]int64{1000670000, 1000679999}

// RestrictedV2 is the SCC every authenticated user gets by default, and the
// one a customer security review expects a storage driver to fit inside.
func RestrictedV2() SCC {
	return SCC{
		Name:                     "restricted-v2",
		AllowPrivilegedContainer: false,
		AllowHostNetwork:         false,
		AllowHostPorts:           false,
		AllowHostDirVolumePlugin: false,
		AllowPrivilegeEscalation: false,
		RunAsUser:                MustRunAsRange,
		UIDRange:                 openShiftNamespaceUIDRange,
		RequiredDropCapabilities: []string{"ALL"},
		Volumes: []string{
			"configMap", "csi", "downwardAPI", "emptyDir", "ephemeral",
			"persistentVolumeClaim", "projected", "secret",
		},
	}
}

// NonRootV2 keeps every restriction except the UID range: it requires non-root
// but accepts whatever non-zero UID the pod asks for. It is the SCC that admits
// this chart's controller with its shipped hardening untouched.
func NonRootV2() SCC {
	scc := RestrictedV2()
	scc.Name = "nonroot-v2"
	scc.RunAsUser = MustRunAsNonRoot
	scc.UIDRange = [2]int64{}
	return scc
}

// HostNetworkV2 adds host networking and host ports to restricted-v2. Modelled
// because it is the SCC an operator reaches for when they see hostNetwork on
// the DaemonSet -- and it is not enough, which is worth asserting rather than
// discovering during a bring-up.
func HostNetworkV2() SCC {
	scc := RestrictedV2()
	scc.Name = "hostnetwork-v2"
	scc.AllowHostNetwork = true
	scc.AllowHostPorts = true
	return scc
}

// RestrictedV3 is restricted-v2 with the 1.3x pod-security defaults. It
// matters here only for its name: the chart drops the controller's fixed uid
// for any SCC named restricted*, because those are the ones that assign one.
func RestrictedV3() SCC {
	scc := RestrictedV2()
	scc.Name = "restricted-v3"
	return scc
}

// AnyUID permits any uid and nothing else. It is one of the three SCCs the
// chart must keep the fixed uid for, because it assigns none.
func AnyUID() SCC {
	scc := RestrictedV2()
	scc.Name = "anyuid"
	scc.RunAsUser = RunAsAny
	scc.UIDRange = [2]int64{}
	return scc
}

// Privileged imposes nothing. The node DaemonSet needs it.
func Privileged() SCC {
	return SCC{
		Name:                     "privileged",
		AllowPrivilegedContainer: true,
		AllowHostNetwork:         true,
		AllowHostPorts:           true,
		AllowHostDirVolumePlugin: true,
		AllowPrivilegeEscalation: true,
		AllowHostPID:             true,
		AllowHostIPC:             true,
		RunAsUser:                RunAsAny,
		Volumes:                  []string{"*"},
	}
}

// SCCFromManifest reads the SecurityContextConstraints the chart renders.
//
// This is the one SCC in the suite that is not a model of an OpenShift
// built-in: it is the chart's own, read back from the install. Evaluating the
// node pod against it is what "no manual workaround" means -- the install
// carries the permission it needs rather than asking an administrator to run
// `oc adm policy add-scc-to-user privileged` and widening the cluster.
func SCCFromManifest(t *testing.T, m Manifest) SCC {
	t.Helper()

	rendered := m.Kinds("SecurityContextConstraints")
	if len(rendered) != 1 {
		t.Fatalf("expected exactly one SecurityContextConstraints in the install, found %d", len(rendered))
	}
	o := rendered[0]

	scc := SCC{
		Name:                     o.Name(),
		AllowPrivilegedContainer: boolAt(o, "allowPrivilegedContainer"),
		AllowHostNetwork:         boolAt(o, "allowHostNetwork"),
		AllowHostPorts:           boolAt(o, "allowHostPorts"),
		AllowHostDirVolumePlugin: boolAt(o, "allowHostDirVolumePlugin"),
		AllowPrivilegeEscalation: boolAt(o, "allowPrivilegeEscalation"),
		AllowHostPID:             boolAt(o, "allowHostPID"),
		AllowHostIPC:             boolAt(o, "allowHostIPC"),
		RequiredDropCapabilities: stringsOf(o["requiredDropCapabilities"]),
		Volumes:                  stringsOf(o["volumes"]),
	}

	if strategy, ok := asMap(o["runAsUser"]); ok {
		scc.RunAsUser = RunAsUserStrategy(strategy.str("type"))
	}

	return scc
}

// Admit evaluates the pod against the SCC and returns every reason it would be
// refused. An empty result means the SCC admits it.
//
// Only the strategies this chart can trip are implemented -- there is no SELinux
// or fsGroup evaluation here, because the chart sets neither and a model of a
// strategy nothing exercises would be a model nothing checks. SELinux on RHCOS
// is a live question for the *workload* pod reading the mount, not for these
// two, and it cannot be settled offline.
func (s SCC) Admit(pod PodSpec) []string {
	var why []string

	if pod.HostNetwork && !s.AllowHostNetwork {
		why = append(why, fmt.Sprintf("%s forbids hostNetwork", s.Name))
	}
	if pod.HostPID && !s.AllowHostPID {
		why = append(why, fmt.Sprintf("%s forbids hostPID", s.Name))
	}
	if pod.HostIPC && !s.AllowHostIPC {
		why = append(why, fmt.Sprintf("%s forbids hostIPC", s.Name))
	}
	if len(pod.HostPorts) > 0 && !s.AllowHostPorts {
		why = append(why, fmt.Sprintf("%s forbids hostPorts %v", s.Name, pod.HostPorts))
	}

	// Reported once per type, not once per volume: three hostPath mounts are
	// one refusal to act on.
	reported := map[string]bool{}
	for _, v := range pod.VolumeTypes {
		if !s.allowsVolume(v) && !reported[v] {
			reported[v] = true
			why = append(why, fmt.Sprintf("%s forbids the %s volume type", s.Name, v))
		}
	}

	for _, c := range pod.Containers {
		sc := c.Security.merge(pod.Security)
		why = append(why, s.admitContainer(c.Name, sc)...)
	}

	sort.Strings(why)
	return why
}

func (s SCC) allowsVolume(kind string) bool {
	if kind == "hostPath" && s.AllowHostDirVolumePlugin {
		return true
	}
	for _, v := range s.Volumes {
		if v == "*" || v == kind {
			return true
		}
	}
	return false
}

func (s SCC) admitContainer(name string, sc SecurityContext) []string {
	var why []string

	if boolVal(sc.Privileged) && !s.AllowPrivilegedContainer {
		why = append(why, fmt.Sprintf("%s forbids privileged (container %s)", s.Name, name))
	}
	if boolVal(sc.AllowPrivilegeEscalation) && !s.AllowPrivilegeEscalation {
		why = append(why, fmt.Sprintf("%s forbids allowPrivilegeEscalation (container %s)", s.Name, name))
	}

	switch s.RunAsUser {
	case MustRunAsRange:
		// An unset UID is the admitted case: OpenShift assigns one from the
		// range. An explicitly set UID is accepted only if it lands inside.
		if sc.RunAsUser != nil {
			if u := *sc.RunAsUser; u < s.UIDRange[0] || u > s.UIDRange[1] {
				why = append(why, fmt.Sprintf(
					"%s runAsUser %d is outside the namespace range %d-%d (container %s)",
					s.Name, u, s.UIDRange[0], s.UIDRange[1], name))
			}
		}
	case MustRunAsNonRoot:
		if sc.RunAsUser != nil && *sc.RunAsUser == 0 {
			why = append(why, fmt.Sprintf("%s forbids runAsUser 0 (container %s)", s.Name, name))
		}
		if sc.RunAsUser == nil && !boolVal(sc.RunAsNonRoot) {
			why = append(why, fmt.Sprintf(
				"%s needs runAsNonRoot true or a non-zero runAsUser (container %s)", s.Name, name))
		}
	case RunAsAny:
	}

	for _, required := range s.RequiredDropCapabilities {
		if !hasCapability(sc.DropCapabilities, required) {
			why = append(why, fmt.Sprintf(
				"%s requires capabilities.drop to include %s (container %s)", s.Name, required, name))
		}
	}

	return why
}

func hasCapability(have []string, want string) bool {
	for _, c := range have {
		if strings.EqualFold(c, want) || strings.EqualFold(c, "ALL") {
			return true
		}
	}
	return false
}

// PSALevel is a Pod Security Admission level.
type PSALevel string

const (
	PSAPrivileged PSALevel = "privileged"
	PSABaseline   PSALevel = "baseline"
	PSARestricted PSALevel = "restricted"
)

// RequiredPSALevel returns the weakest Pod Security Admission level that admits
// the pod. OpenShift runs PSA alongside SCC, so a namespace labelled below this
// refuses the workload even with the right SCC bound -- which is the failure
// that looks like the SCC binding did not take.
func RequiredPSALevel(pod PodSpec) PSALevel {
	if pod.IsPrivileged() || pod.HostNetwork || pod.HostPID || pod.HostIPC || len(pod.HostPorts) > 0 {
		return PSAPrivileged
	}
	for _, v := range pod.VolumeTypes {
		if v == "hostPath" {
			return PSAPrivileged
		}
	}

	for _, c := range pod.Containers {
		sc := c.Security.merge(pod.Security)
		if len(c.Security.AddCapabilities) > 0 {
			return PSABaseline
		}
		// restricted additionally wants escalation off, ALL dropped,
		// non-root, and a seccomp profile that is not Unconfined.
		if boolVal(sc.AllowPrivilegeEscalation) ||
			!hasCapability(sc.DropCapabilities, "ALL") ||
			!(boolVal(sc.RunAsNonRoot) || (sc.RunAsUser != nil && *sc.RunAsUser != 0)) ||
			sc.SeccompProfileType == "" || sc.SeccompProfileType == "Unconfined" {
			return PSABaseline
		}
	}

	return PSARestricted
}
