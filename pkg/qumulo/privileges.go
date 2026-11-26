// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package qumulo

// routePrivileges is every REST route this client calls, and the Qumulo
// privileges a token needs for the methods the driver uses on it.
//
// It cannot be derived: Core says which privilege a route needs only in the
// 403 it answers without it. The entries are what Core 7.9.2.2 named when
// each route was called with a token lacking it (lab, 2026-09-30), or what
// the chart README's table was measured as. TestEveryRouteHasItsPrivileges
// keeps it complete: a route added to this package without an entry fails,
// and so does an entry whose privilege the chart README's "Required Qumulo
// API privileges" section does not name -- which is how a least-privilege
// install that followed the README could not delete a volume, or
// report, drain or reverse a replication relationship.
var routePrivileges = map[string][]string{
	versionPath:   {}, // answers with no credential at all
	nodeStatePath: {}, // likewise

	fileSystemPath:     {"PRIVILEGE_CAPACITY_READ"},
	fileEntriesPath:    {"PRIVILEGE_FS_ATTRIBUTES_READ", "PRIVILEGE_FILE_FULL_ACCESS"},
	fileAttributesPath: {"PRIVILEGE_FS_ATTRIBUTES_READ", "PRIVILEGE_FILE_FULL_ACCESS"},
	// Answered 200 to the README role; which of its privileges it needs was
	// not isolated.
	fileAggregatesPath: {"PRIVILEGE_FS_ATTRIBUTES_READ"},
	fileCopyChunkPath:  {"PRIVILEGE_FILE_FULL_ACCESS"},
	filesResolvePath:   {"PRIVILEGE_ANALYTICS_READ"},

	quotasPath:      {"PRIVILEGE_QUOTA_READ", "PRIVILEGE_QUOTA_WRITE"},
	quotaPath:       {"PRIVILEGE_QUOTA_READ", "PRIVILEGE_QUOTA_WRITE"},
	quotaStatusPath: {"PRIVILEGE_QUOTA_READ"},

	treeDeleteJobsPath: {"PRIVILEGE_FS_DELETE_TREE_WRITE"},
	treeDeleteJobPath:  {"PRIVILEGE_FS_DELETE_TREE_READ"},

	snapshotsPath:            {"PRIVILEGE_SNAPSHOT_READ", "PRIVILEGE_SNAPSHOT_WRITE"},
	snapshotPath:             {"PRIVILEGE_SNAPSHOT_READ", "PRIVILEGE_SNAPSHOT_WRITE"},
	snapshotChangesSincePath: {"PRIVILEGE_SNAPSHOT_DIFFERENCE_READ"},
	snapshotPoliciesPath:     {"PRIVILEGE_SNAPSHOT_POLICY_READ", "PRIVILEGE_SNAPSHOT_POLICY_WRITE"},
	snapshotPolicyPath:       {"PRIVILEGE_SNAPSHOT_POLICY_READ", "PRIVILEGE_SNAPSHOT_POLICY_WRITE"},
	snapshotPolicyByIDPath:   {"PRIVILEGE_SNAPSHOT_POLICY_READ"},

	nfsSettingsPath:   {"PRIVILEGE_NFS_SETTINGS_READ"},
	nfsExportsPath:    {"PRIVILEGE_NFS_EXPORT_READ", "PRIVILEGE_NFS_EXPORT_WRITE"},
	nfsExportPath:     {"PRIVILEGE_NFS_EXPORT_READ", "PRIVILEGE_NFS_EXPORT_WRITE"},
	networkConfigPath: {"PRIVILEGE_NETWORK_READ"},
	tenantsPath:       {"PRIVILEGE_TENANT_READ"},
	timeStatusPath:    {"PRIVILEGE_TIME_READ"},

	replicationSourceRelationshipsPath: {"PRIVILEGE_REPLICATION_SOURCE_WRITE"},
	replicationSourceRelationshipPath:  {"PRIVILEGE_REPLICATION_SOURCE_READ", "PRIVILEGE_REPLICATION_SOURCE_WRITE"},
	replicationSourceStatusPath:        {"PRIVILEGE_REPLICATION_SOURCE_READ"},
	replicationSourceStatusesPath:      {"PRIVILEGE_REPLICATION_SOURCE_READ"},
	replicationReplicatePath:           {"PRIVILEGE_REPLICATION_SOURCE_WRITE"},
	replicationReversePath:             {"PRIVILEGE_REPLICATION_REVERSE_RELATIONSHIP"},
	replicationTargetStatusPath:        {"PRIVILEGE_REPLICATION_TARGET_READ"},
	replicationTargetStatusesPath:      {"PRIVILEGE_REPLICATION_TARGET_READ"},
	replicationAuthorizePath:           {"PRIVILEGE_REPLICATION_TARGET_WRITE"},
	replicationMakeWritablePath:        {"PRIVILEGE_REPLICATION_TARGET_WRITE"},
	replicationReconnectPath:           {"PRIVILEGE_REPLICATION_TARGET_WRITE"},
	replicationTargetDeletePath:        {"PRIVILEGE_REPLICATION_TARGET_WRITE"},
}
