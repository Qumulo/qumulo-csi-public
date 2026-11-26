// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package driver

import (
	"fmt"
	"strings"

	"qumulo-csi-driver/pkg/qumulo"
)

// securityFlavor selects the NFS authentication flavour for a class's volumes.
//
// It is deliberately one setting driving two halves that only work together:
//
//   - the controller sets the export's required_authentication_mode, so the
//     cluster *demands* that flavour;
//   - the node adds the matching sec= mount option, so the client *offers* it.
//
// Either alone is useless. An export requiring krb5p rejects a sec=sys client;
// a sec=krb5p mount against an export requiring nothing quietly negotiates back
// down to sec=sys, because NFSv4.1 settles on the weakest both ends accept. That
// silent downgrade is why this is not simply a mount option an operator can add
// to mountOptions themselves: without the export half they would believe traffic
// was encrypted when it was not.
//
// What the driver does NOT do is encrypt anything. AES-256 at rest is the
// cluster's, and krb5p's wire encryption is the NFS protocol's. The driver's
// whole contribution is naming the flavour on both ends so the two agree.
const (
	securityFlavorSys   = "sys"
	securityFlavorKrb5  = "krb5"
	securityFlavorKrb5i = "krb5i"
	securityFlavorKrb5p = "krb5p"
)

// securityFlavors is every accepted value, in ascending order of protection:
// authentication only, then integrity, then privacy.
var securityFlavors = []string{
	securityFlavorSys,
	securityFlavorKrb5,
	securityFlavorKrb5i,
	securityFlavorKrb5p,
}

// exportAuthModes maps a flavour to the cluster's enum.
//
// sys maps to AUTHENTICATION_MODE_NONE rather than to a "SYS" value: the export
// API has no such member -- probed on Core 7.9.3, which rejects
// AUTHENTICATION_MODE_SYS outright -- and NONE means "require nothing", which is
// what lets an AUTH_SYS client in. It is the value every export this driver has
// ever created already used, so an unset securityFlavor changes nothing.
var exportAuthModes = map[string]string{
	securityFlavorSys:   qumulo.AuthenticationModeNone,
	securityFlavorKrb5:  qumulo.AuthenticationModeKrb5,
	securityFlavorKrb5i: qumulo.AuthenticationModeKrb5i,
	securityFlavorKrb5p: qumulo.AuthenticationModeKrb5p,
}

// isKerberos reports whether a flavour needs a Kerberos ticket to mount.
func isKerberos(flavor string) bool {
	return flavor != "" && flavor != securityFlavorSys
}

// validateSecurityFlavor checks the value in isolation.
//
// Whether it can be *honoured* depends on the provisioning model and the
// cluster, which need a StorageClass and a round trip respectively; those checks
// live in validateSecurityFlavorScoping and the controller's preflight.
func validateSecurityFlavor(flavor string) error {
	if strings.TrimSpace(flavor) != flavor {
		return fmt.Errorf("must not have leading or trailing whitespace")
	}
	for _, known := range securityFlavors {
		if flavor == known {
			return nil
		}
	}
	return fmt.Errorf("must be one of %s", strings.Join(securityFlavors, ", "))
}

// validateSecurityFlavorScoping refuses a Kerberos flavour a class cannot deliver.
//
// Two combinations are impossible rather than merely unwise, and both are
// refused rather than ignored. A security control that is silently dropped is
// worse than one that was never offered: the operator believes traffic is
// authenticated and encrypted when it is neither.
//
//   - shared-subdirectory creates no export of its own. The volume is a
//     directory beneath an export an administrator owns, and the driver must not
//     rewrite someone else's export -- doing so would change the authentication
//     requirement for every other volume under it, including ones belonging to
//     other classes. Kerberos there is an administrator action on the parent.
//
//   - nfs3 has no Kerberos in this driver. RPCSEC_GSS over NFSv3 needs a
//     running rpc.gssd *and* the lock manager the node image cannot run (see the
//     nolock requirement), and Qumulo documents Kerberos as NFSv4.1.
func validateSecurityFlavorScoping(params volumeParams) error {
	if !isKerberos(params.SecurityFlavor) {
		return nil
	}

	if !params.DedicatedExport() {
		return fmt.Errorf(
			"%s=%s requires %s=%s: a %s volume is a directory under an export somebody "+
				"else owns, and changing that export's authentication would affect every "+
				"other volume beneath it. Kerberos on a shared export is an administrator "+
				"action on the parent, not a per-volume one",
			paramSecurityFlavor, params.SecurityFlavor,
			paramProvisioningModel, provisioningModelDedicatedExport,
			provisioningModelSharedSubdirectory)
	}

	if params.Protocol == protocolNFS3 {
		return fmt.Errorf(
			"%s=%s requires %s=%s: Kerberos is an NFSv4.1 feature here, and the node "+
				"image cannot run the lock manager NFSv3 would additionally need",
			paramSecurityFlavor, params.SecurityFlavor, paramProtocol, protocolNFS41)
	}

	return nil
}

// mountSecurityOptionFor returns the sec= option for a volume, or "".
//
// Empty for an unset flavour and for sys: NFSv4.1 negotiates sec=sys unasked, so
// sending it changes nothing, and omitting it keeps every volume provisioned
// before this parameter existed mounting byte-identically to how it did before.
func mountSecurityOptionFor(volumeContext map[string]string) (string, error) {
	flavor := strings.TrimSpace(volumeContext[volumeContextSecurityFlavor])
	if flavor == "" || flavor == securityFlavorSys {
		return "", nil
	}
	if err := validateSecurityFlavor(flavor); err != nil {
		// The controller validated this at provision time, so reaching here
		// means a hand-authored PV. Say so plainly rather than passing an
		// unknown token to mount(8), which would fail far less legibly.
		return "", fmt.Errorf("volume context %s %q: %w", volumeContextSecurityFlavor, flavor, err)
	}
	return "sec=" + flavor, nil
}

// refuseContradictorySecurityOption refuses a sec= in the class's own
// mountOptions that asks for a different flavour from the volume's
// securityFlavor.
//
// Resolving it either way would be wrong. Appending the volume's sec= beside
// the class's leaves the kernel taking whichever came last, which is not a
// decision anyone made; honouring the class's alone mounts with a flavour
// the export does not require, so a class that asks for krb5p can negotiate
// quietly down to sec=sys -- the exact failure securityFlavor exists to
// prevent. Naming the contradiction is the only answer that cannot be wrong,
// and it is how nfsVersionOptionFor already treats the same shape of
// mistake.
//
// Only a volume that set securityFlavor can be contradicted. Without it the
// driver appends no sec= and the class's mountOptions are the only place the
// flavour is chosen -- the documented way to mount Kerberos before
// securityFlavor existed, on an export an administrator configured by hand.
// Reading an unset flavour as sys refused those volumes on upgrade, and a
// PV's mountOptions cannot be edited to recover them. The publish
// logs which flavour the class chose instead; see securityMountOption.
func refuseContradictorySecurityOption(volumeContext map[string]string, mountFlags []string) error {
	flavor := strings.TrimSpace(volumeContext[volumeContextSecurityFlavor])
	if flavor == "" {
		return nil
	}

	for _, option := range mountOptionList(mountFlags) {
		key, value, ok := strings.Cut(strings.ToLower(strings.TrimSpace(option)), "=")
		if !ok || key != "sec" {
			continue
		}
		if value == strings.ToLower(flavor) {
			continue
		}
		return fmt.Errorf("volume is %s=%s but its mount options request %q; the export requires %s, so a "+
			"mount with %q would be refused by the cluster or negotiated to something neither asked for. "+
			"Set the flavour in one place, not both",
			paramSecurityFlavor, flavor, option, flavor, option)
	}
	return nil
}

// securityMountOption returns the first sec= among mountFlags, or "".
func securityMountOption(mountFlags []string) string {
	for _, option := range mountOptionList(mountFlags) {
		key, _, ok := strings.Cut(strings.ToLower(strings.TrimSpace(option)), "=")
		if ok && key == "sec" {
			return strings.TrimSpace(option)
		}
	}
	return ""
}
