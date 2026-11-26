//go:build openshift

// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package openshift

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// supportedControllerSCCs is the set of SCCs an OpenShift administrator may
// pin the controller to, and the model that evaluates each one.
//
// The chart decides from the *name*: it drops the fixed uid for anything
// called restricted*, because those assign a uid from the namespace range and
// refuse any other, and keeps it otherwise, because an SCC that assigns no uid
// would otherwise leave the sidecar images running as root and runAsNonRoot
// would refuse them. Deciding from a name is a real design risk, so each
// configuration is evaluated rather than assumed.
var supportedControllerSCCs = []struct {
	name       string
	scc        SCC
	assignsUID bool
}{
	{"restricted-v2", RestrictedV2(), true},
	{"restricted-v3", RestrictedV3(), true},
	{"nonroot-v2", NonRootV2(), false},
	{"anyuid", AnyUID(), false},
	{"privileged", Privileged(), false},
}

// TestControllerRunsUnderEverySupportedSCC.
//
// This is the application security compatibility matrix for the controller.
// For each SCC: pin it, render, and check both that the SCC admits the pod and
// that the uid decision came out the right way round. Getting it backwards is
// not a render error -- it is five of six containers in
// CreateContainerConfigError, or a pod stuck Pending on a uid the range does
// not contain, both of which look like an install problem rather than a values
// problem.
func TestControllerRunsUnderEverySupportedSCC(t *testing.T) {
	for _, tc := range supportedControllerSCCs {
		t.Run(tc.name, func(t *testing.T) {
			opts := openShiftValues(t)
			opts.Sets = []string{"openshift.controllerSCC=" + tc.name}

			manifest := Render(t, opts)
			deployment := manifest.Named(t, "Deployment", "-controller")
			pod := PodTemplate(t, deployment)

			require.Equal(t, tc.name, requiredSCC(t, deployment),
				"the controller is not pinned to the SCC it was configured with")

			assert.Empty(t, tc.scc.Admit(pod), "%s refuses the controller", tc.name)

			for _, name := range pod.ContainerNames() {
				sc := pod.Effective(t, name)

				if tc.assignsUID {
					assert.Nil(t, sc.RunAsUser,
						"%s assigns a uid from the namespace range and refuses any other, but container %s pins one", tc.name, name)
				} else {
					require.NotNil(t, sc.RunAsUser,
						"%s assigns no uid, so container %s would run as root and runAsNonRoot would refuse it", tc.name, name)
					assert.EqualValues(t, 65532, *sc.RunAsUser, "container %s", name)
				}

				// True either way: dropping the uid must never drop the
				// guarantee that goes with it.
				assert.True(t, boolValue(sc.RunAsNonRoot), "container %s may run as root under %s", name, tc.name)
				assert.True(t, boolValue(sc.ReadOnlyRootFilesystem), "container %s has a writable root filesystem", name)
				assert.Contains(t, sc.DropCapabilities, "ALL", "container %s does not drop ALL capabilities", name)
			}
		})
	}
}

// TestNodeHelperContainersKeepUIDZeroDeliberately.
//
// node-driver-registrar and livenessprobe run unprivileged with every
// capability dropped, but at uid 0 on purpose: the registrar writes a socket
// into the kubelet's root-owned plugins_registry and the probe connects to a
// root-owned socket. A hardening pass that gives them a non-root uid produces
// a DaemonSet that reports healthy and never registers the plugin.
//
// The chart's own tests assert this. It is repeated here because the pressure
// to "fix" it comes specifically from an OpenShift security review, and this
// is the tier that review will be run against.
func TestNodeHelperContainersKeepUIDZeroDeliberately(t *testing.T) {
	pod := PodTemplate(t, Render(t, openShiftValues(t)).Named(t, "DaemonSet", "-node"))

	for _, name := range []string{"node-driver-registrar", "liveness-probe"} {
		sc := pod.Effective(t, name)

		if sc.RunAsUser != nil {
			assert.EqualValues(t, 0, *sc.RunAsUser,
				"%s runs as uid %d; it must stay uid 0 or it cannot write into the kubelet's root-owned directories",
				name, *sc.RunAsUser)
		}
		assert.False(t, boolValue(sc.RunAsNonRoot), "%s has runAsNonRoot set; it must stay uid 0", name)

		// Root, but nothing beyond root.
		assert.False(t, boolValue(sc.Privileged), "%s is privileged; only the driver container needs that", name)
		assert.False(t, boolValue(sc.AllowPrivilegeEscalation), "%s allows privilege escalation", name)
		assert.Contains(t, sc.DropCapabilities, "ALL", "%s does not drop ALL capabilities", name)
		assert.True(t, boolValue(sc.ReadOnlyRootFilesystem), "%s has a writable root filesystem", name)
	}
}

// TestOnlyTheDriverContainerIsPrivileged.
//
// Privilege is per container and the chart keeps it that way. The whole node
// pod is admitted by the one SCC regardless, so this is not an admission
// question -- it is the blast radius, which is what the review asks about.
func TestOnlyTheDriverContainerIsPrivileged(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	node := PodTemplate(t, manifest.Named(t, "DaemonSet", "-node"))
	for _, name := range node.ContainerNames() {
		privileged := boolValue(node.Effective(t, name).Privileged)
		if name == "qumulo-csi-driver" {
			assert.True(t, privileged, "the node driver container is not privileged; it cannot mount")
			continue
		}
		assert.False(t, privileged, "node container %s is privileged and does not need to be", name)
	}

	controller := PodTemplate(t, manifest.Named(t, "Deployment", "-controller"))
	for _, name := range controller.ContainerNames() {
		assert.False(t, boolValue(controller.Effective(t, name).Privileged),
			"controller container %s is privileged", name)
	}
	assert.False(t, controller.HostNetwork, "the controller uses hostNetwork; it only speaks HTTPS to the array")
	assert.NotContains(t, controller.VolumeTypes, "hostPath",
		"the controller mounts a hostPath; it touches no host filesystem")
}

// TestNamespaceLabelsMatchWhatAdmissionActuallyRequires.
//
// OpenShift enforces Pod Security Admission alongside SCC. A namespace below
// the required level refuses the DaemonSet even with the SCC correctly granted,
// and the error names PSA rather than the grant -- so the two get chased in the
// wrong order. The SCC work in the chart does not remove this step, which is
// exactly why it is asserted here.
func TestNamespaceLabelsMatchWhatAdmissionActuallyRequires(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	node := PodTemplate(t, manifest.Named(t, "DaemonSet", "-node"))
	require.Equal(t, PSAPrivileged, RequiredPSALevel(node),
		"the node plugin no longer needs a privileged namespace; the namespace manifest over-permits")

	// The controller is restricted-level on its own. It shares the namespace
	// with the node plugin, so it is installed into a privileged one -- worth
	// recording, because a customer who wants the controller in a restricted
	// namespace of its own can have that.
	controller := PodTemplate(t, manifest.Named(t, "Deployment", "-controller"))
	assert.Equal(t, PSARestricted, RequiredPSALevel(controller),
		"the controller stopped being restricted-level, which removes the option of splitting it into its own namespace")

	raw, err := os.ReadFile(ValuesFile(t, "openshift-namespace.yaml"))
	require.NoError(t, err)

	var ns Object
	require.NoError(t, yaml.Unmarshal(raw, &ns))

	for _, label := range []string{"enforce", "audit", "warn"} {
		key := "pod-security.kubernetes.io/" + label
		got := ns.Dig("metadata", "labels", key)
		assert.EqualValues(t, string(PSAPrivileged), got,
			"the namespace manifest sets %s=%v but admission requires %s", key, got, PSAPrivileged)
	}

	assert.Equal(t, "false", ns.Dig("metadata", "annotations", "security.openshift.io/scc.podSecurityLabelSync"),
		"without the label syncer disabled, OpenShift rewrites the labels above and the DaemonSet starts being refused after a working install")
}

// TestArbitraryUIDWorkloadsCanWriteTheMountedVolume.
//
// This is volume access under the application security configuration almost
// every OpenShift workload actually runs in. restricted-v2 gives the workload
// pod a random namespace-assigned uid with gid 0, and the CSIDriver ships
// fsGroupPolicy: None, so the kubelet does no recursive chown and the export's
// own mode is the whole story. The 0777 default is what makes an arbitrary uid
// able to write; anything stricter, or "inherit", is permission-denied, and it
// fails in the application rather than at the mount, which makes it slow to
// diagnose.
func TestArbitraryUIDWorkloadsCanWriteTheMountedVolume(t *testing.T) {
	t.Run("fsGroupPolicy stays None", func(t *testing.T) {
		driver := Render(t, openShiftValues(t)).Named(t, "CSIDriver", "csi.qumulo.com")

		assert.Equal(t, "None", driver.Dig("spec", "fsGroupPolicy"),
			"fsGroupPolicy changed; on OpenShift this decides whether the kubelet relabels and chowns the mount, so the permission requirement changes with it")
	})

	t.Run("the shipped values file sets no stricter mode", func(t *testing.T) {
		raw, err := os.ReadFile(ValuesFile(t, "openshift-values.yaml"))
		require.NoError(t, err)

		var values map[string]any
		require.NoError(t, yaml.Unmarshal(raw, &values))

		classes, _ := values["storageClasses"].([]any)
		for _, c := range classes {
			class, _ := asMap(c)
			mode, present := class["mountPermissions"]
			assert.False(t, present,
				"the OpenShift values file sets mountPermissions=%v; anything but the 0777 default breaks arbitrary-uid workloads", mode)
		}
	})

	t.Run("a stricter mode is what it looks like", func(t *testing.T) {
		opts := openShiftValues(t)
		opts.Sets = []string{
			"storageClasses[0].name=qumulo-strict",
			"storageClasses[0].server=qumulo.example.com",
			"storageClasses[0].secretName=qumulo-token",
			"storageClasses[0].basePath=/csi/volumes",
			`storageClasses[0].protocol=nfs4\.1`,
			"storageClasses[0].mountPermissions=0770",
		}

		classes := Render(t, opts).Kinds("StorageClass")
		require.Len(t, classes, 1)

		mode := classes[0].Dig("parameters", "mountPermissions")
		require.NotNil(t, mode, "mountPermissions is no longer a StorageClass parameter; this check needs rewriting")
		assert.NotEqual(t, "0777", mode)

		t.Logf("mountPermissions %v grants nothing to an arbitrary uid outside the owning user and group -- "+
			"an OpenShift workload under a random namespace uid with gid 0 cannot write this volume", mode)
	})
}

// TestSeccompProfileMatchesWhatTheSCCPermits.
//
// RuntimeDefault at pod level is what keeps the controller restricted-level
// under PSA, and the node pod's profile has to be one its SCC lists -- an SCC
// permitting only runtime/default refuses a pod asking for Unconfined, which
// is easy to introduce through a values override and invisible until then.
func TestSeccompProfileMatchesWhatTheSCCPermits(t *testing.T) {
	manifest := Render(t, openShiftValues(t))

	for _, w := range []struct{ kind, suffix string }{
		{"Deployment", "-controller"},
		{"DaemonSet", "-node"},
	} {
		pod := PodTemplate(t, manifest.Named(t, w.kind, w.suffix))
		assert.Equal(t, "RuntimeDefault", pod.Security.SeccompProfileType,
			"%s has no RuntimeDefault seccomp profile", pod.Owner)
	}

	scc := manifest.Kinds("SecurityContextConstraints")[0]
	profiles := stringList(scc["seccompProfiles"])
	assert.Equal(t, []string{"runtime/default"}, profiles,
		"the chart's SCC permits %v; RuntimeDefault on the pod maps to runtime/default and nothing else should be allowed", profiles)
}

// TestKerberosStaysOffUntilRHCOSHostConfigurationIsDecided.
//
// Kerberos mounts need rpc.gssd running on the host and a keytab at
// /etc/krb5.keytab. On RHCOS both are MachineConfig changes, each rebooting
// the worker pool, and shipping a keytab inside a MachineConfig puts it in a
// cluster-readable object -- a security decision, not an engineering one.
//
// What is asserted is the safe default and the fail-soft behaviour: with
// Kerberos on, the host paths stay FileOrCreate, so a node missing the keytab
// still runs the plugin and still mounts non-Kerberos volumes. `File`
// would take the whole node out of service instead.
func TestKerberosStaysOffUntilRHCOSHostConfigurationIsDecided(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		raw, err := RenderRaw(t, openShiftValues(t))
		require.NoError(t, err)

		assert.NotContains(t, raw, "krb5.keytab",
			"the recommended OpenShift install mounts a keytab, which needs a MachineConfig that does not exist yet")
	})

	t.Run("fails soft when enabled", func(t *testing.T) {
		opts := openShiftValues(t)
		opts.Sets = []string{"node.kerberos.enabled=true"}

		manifest := Render(t, opts)
		node := manifest.Named(t, "DaemonSet", "-node")

		volumes, _ := node.Dig("spec", "template", "spec", "volumes").([]any)
		var checked int
		for _, v := range volumes {
			vol, _ := asMap(v)
			hostPath, ok := asMap(vol["hostPath"])
			if !ok {
				continue
			}
			if path := hostPath.str("path"); !strings.Contains(path, "krb5") {
				continue
			}
			checked++
			assert.Equal(t, "FileOrCreate", hostPath["type"],
				"%v uses hostPathType %v; `File` takes a node missing the keytab out of service entirely",
				hostPath["path"], hostPath["type"])
		}
		assert.Equal(t, 2, checked, "expected krb5.conf and krb5.keytab host mounts, found %d", checked)

		// The keytab is a Kerberos requirement that the SCC has to permit too,
		// so enabling it must not silently fall outside the shipped SCC.
		assert.Empty(t, SCCFromManifest(t, manifest).Admit(PodTemplate(t, node)),
			"with Kerberos on, the node pod is no longer admitted by the SCC the chart ships")
	})
}

// TestVolumeAccessRequirementsAreDocumented.
//
// "Document any requirements" is part of the ask, and a requirement that lives
// only in a test is not documented. These are the four that decide whether a
// workload can read and write a volume on OpenShift, and the one that cannot
// be settled offline at all. The check is deliberately shallow -- it asserts
// the subject is addressed, not how -- because its job is to fail when the
// requirements change and the document does not.
func TestVolumeAccessRequirementsAreDocumented(t *testing.T) {
	raw, err := os.ReadFile("README.md")
	require.NoError(t, err, "the tier has no README to carry the requirements")
	readme := string(raw)

	for _, subject := range []string{
		"mountPermissions", // the export mode an arbitrary uid needs
		"fsGroupPolicy",    // why the kubelet does not fix it up
		"arbitrary",        // the uid OpenShift assigns the workload
		"SELinux",          // unprovable offline, and the largest remaining risk
		"restricted-v2",    // the configuration nearly every workload runs in
	} {
		assert.Contains(t, readme, subject,
			"README.md does not document %q, which decides whether a workload can use a volume on OpenShift", subject)
	}
}

func boolValue(p *bool) bool { return p != nil && *p }
