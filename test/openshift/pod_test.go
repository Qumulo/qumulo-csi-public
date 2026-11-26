//go:build openshift

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package openshift

import (
	"sort"
	"testing"
)

// PodSpec is the slice of a pod template admission actually looks at.
type PodSpec struct {
	// Owner is the workload the template came from, for error messages.
	Owner string

	ServiceAccount    string
	PriorityClassName string
	HostNetwork       bool
	HostPID           bool
	HostIPC           bool
	HostPorts         []int64
	// VolumeTypes are the volume types admission sees: the template's own,
	// plus `projected` when the pod gets a ServiceAccount token, because the
	// ServiceAccount admission plugin injects that volume before the SCC
	// plugin runs.
	VolumeTypes []string
	// MountsToken is false only when the template sets
	// automountServiceAccountToken: false.
	MountsToken bool

	// Security is the pod-level securityContext.
	Security SecurityContext

	Containers []Container
}

// Container is one container and its effective security context -- the
// pod-level context with the container's own merged over it, which is what
// admission evaluates.
type Container struct {
	Name     string
	Image    string
	Security SecurityContext
}

// SecurityContext holds the fields the SCC and PSA strategies read. Every one
// is a pointer because "unset" and "false"/"0" are different answers: a
// controller that leaves runAsUser unset is admitted by restricted-v2, and one
// that sets it to 65532 is not.
type SecurityContext struct {
	Privileged               *bool
	RunAsNonRoot             *bool
	RunAsUser                *int64
	RunAsGroup               *int64
	AllowPrivilegeEscalation *bool
	ReadOnlyRootFilesystem   *bool
	DropCapabilities         []string
	AddCapabilities          []string
	SeccompProfileType       string
}

// merge returns the container's effective context: pod-level defaults with the
// container's own settings taking precedence, field by field. This mirrors the
// kubelet, which is what makes the verdicts below meaningful.
func (c SecurityContext) merge(pod SecurityContext) SecurityContext {
	out := c

	if out.Privileged == nil {
		out.Privileged = pod.Privileged
	}
	if out.RunAsNonRoot == nil {
		out.RunAsNonRoot = pod.RunAsNonRoot
	}
	if out.RunAsUser == nil {
		out.RunAsUser = pod.RunAsUser
	}
	if out.RunAsGroup == nil {
		out.RunAsGroup = pod.RunAsGroup
	}
	if out.AllowPrivilegeEscalation == nil {
		out.AllowPrivilegeEscalation = pod.AllowPrivilegeEscalation
	}
	if out.ReadOnlyRootFilesystem == nil {
		out.ReadOnlyRootFilesystem = pod.ReadOnlyRootFilesystem
	}
	// Capabilities are not inherited by the kubelet -- a container with no
	// capabilities block gets the runtime default set, never the pod's.
	if out.SeccompProfileType == "" {
		out.SeccompProfileType = pod.SeccompProfileType
	}

	return out
}

// IsPrivileged reports whether any container in the pod is privileged.
func (p PodSpec) IsPrivileged() bool {
	for _, c := range p.Containers {
		if boolVal(c.Security.merge(p.Security).Privileged) {
			return true
		}
	}
	return false
}

// Effective returns the container's merged context, by name.
func (p PodSpec) Effective(t *testing.T, name string) SecurityContext {
	t.Helper()

	for _, c := range p.Containers {
		if c.Name == name {
			return c.Security.merge(p.Security)
		}
	}

	t.Fatalf("%s has no container named %q", p.Owner, name)
	return SecurityContext{}
}

// ContainerNames returns the container names, sorted, for exhaustive checks
// that must notice a container the chart grows later.
func (p PodSpec) ContainerNames() []string {
	out := make([]string, 0, len(p.Containers))
	for _, c := range p.Containers {
		out = append(out, c.Name)
	}
	sort.Strings(out)
	return out
}

// PodTemplate extracts the pod template out of a Deployment or DaemonSet.
func PodTemplate(t *testing.T, o Object) PodSpec {
	t.Helper()

	spec, ok := asMap(o.Dig("spec", "template", "spec"))
	if !ok {
		t.Fatalf("%s/%s has no spec.template.spec", o.Kind(), o.Name())
	}

	out := PodSpec{
		Owner:             o.Kind() + "/" + o.Name(),
		ServiceAccount:    spec.str("serviceAccountName"),
		PriorityClassName: spec.str("priorityClassName"),
		HostNetwork:       boolAt(spec, "hostNetwork"),
		HostPID:           boolAt(spec, "hostPID"),
		HostIPC:           boolAt(spec, "hostIPC"),
		Security:          securityContext(spec["securityContext"]),
	}

	for _, v := range sliceOf(spec["volumes"]) {
		vol, ok := asMap(v)
		if !ok {
			continue
		}
		for key := range vol {
			// Everything but the name is the volume source, and its key is
			// the type name SCC.Volumes lists.
			if key != "name" {
				out.VolumeTypes = append(out.VolumeTypes, key)
			}
		}
	}
	out.MountsToken = true
	if v, ok := spec["automountServiceAccountToken"].(bool); ok && !v {
		out.MountsToken = false
	}
	if out.MountsToken {
		out.VolumeTypes = append(out.VolumeTypes, "projected")
	}
	sort.Strings(out.VolumeTypes)

	for _, c := range sliceOf(spec["containers"]) {
		co, ok := asMap(c)
		if !ok {
			continue
		}
		out.Containers = append(out.Containers, Container{
			Name:     co.str("name"),
			Image:    co.str("image"),
			Security: securityContext(co["securityContext"]),
		})
		for _, p := range sliceOf(co["ports"]) {
			pm, ok := asMap(p)
			if !ok {
				continue
			}
			if hp, ok := intOf(pm["hostPort"]); ok {
				out.HostPorts = append(out.HostPorts, hp)
			}
		}
	}

	if len(out.Containers) == 0 {
		t.Fatalf("%s has no containers", out.Owner)
	}

	return out
}

func securityContext(raw any) SecurityContext {
	m, ok := asMap(raw)
	if !ok {
		return SecurityContext{}
	}

	sc := SecurityContext{
		Privileged:               boolPtr(m["privileged"]),
		RunAsNonRoot:             boolPtr(m["runAsNonRoot"]),
		RunAsUser:                intPtr(m["runAsUser"]),
		RunAsGroup:               intPtr(m["runAsGroup"]),
		AllowPrivilegeEscalation: boolPtr(m["allowPrivilegeEscalation"]),
		ReadOnlyRootFilesystem:   boolPtr(m["readOnlyRootFilesystem"]),
	}

	if caps, ok := asMap(m["capabilities"]); ok {
		sc.DropCapabilities = stringsOf(caps["drop"])
		sc.AddCapabilities = stringsOf(caps["add"])
	}
	if prof, ok := m.Dig("seccompProfile", "type").(string); ok {
		sc.SeccompProfileType = prof
	}

	return sc
}

func sliceOf(v any) []any {
	s, _ := v.([]any)
	return s
}

func stringsOf(v any) []string {
	var out []string
	for _, item := range sliceOf(v) {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func boolPtr(v any) *bool {
	b, ok := v.(bool)
	if !ok {
		return nil
	}
	return &b
}

func intPtr(v any) *int64 {
	i, ok := intOf(v)
	if !ok {
		return nil
	}
	return &i
}

func intOf(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func boolVal(p *bool) bool { return p != nil && *p }

func boolAt(o Object, key string) bool { return boolVal(boolPtr(o[key])) }
