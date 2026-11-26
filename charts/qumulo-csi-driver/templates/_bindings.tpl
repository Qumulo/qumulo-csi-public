{{/*
Every RoleBinding and ClusterRoleBinding this chart can render, in one place.

rbac-bindings.yaml renders them when rbac.createBindings is true (the
default). With it false, none is rendered -- a cluster whose policy (for
example a Gatekeeper rule on RoleBindings) lets only a platform team create or
change bindings applies them itself -- and the same list feeds the NOTES and
the live check in _validate.tpl, so the bindings the chart would make, the
ones it prints and the ones it checks can never drift apart.

The platform team gets the exact YAML from this file alone:

  helm template <release> <chart> -n <namespace> -f values.yaml \
    --set rbac.createBindings=true --show-only templates/rbac-bindings.yaml

The roles they point at (Roles, ClusterRoles) and the ServiceAccounts stay in
rbac.yaml, openshift-scc.yaml and test-rbac.yaml and are always rendered.
*/}}
{{- define "qumulo-csi-driver.bindingManifests" -}}
{{- $fullname := include "qumulo-csi-driver.fullname" . -}}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ $fullname }}-controller
  labels:
    {{- include "qumulo-csi-driver.labels" . | nindent 4 }}
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}-controller
    namespace: {{ .Release.Namespace }}
roleRef:
  kind: ClusterRole
  name: {{ $fullname }}-controller
  apiGroup: rbac.authorization.k8s.io
{{- if or .Values.sidecars.capacity.enabled .Values.sidecars.csiAddons.enabled }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ $fullname }}-controller-self
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "qumulo-csi-driver.labels" . | nindent 4 }}
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}-controller
    namespace: {{ .Release.Namespace }}
roleRef:
  kind: Role
  name: {{ $fullname }}-controller-self
  apiGroup: rbac.authorization.k8s.io
{{- end }}
{{- if or .Values.controller.leaderElection .Values.sidecars.csiAddons.enabled }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ $fullname }}-controller
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "qumulo-csi-driver.labels" . | nindent 4 }}
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}-controller
    namespace: {{ .Release.Namespace }}
roleRef:
  kind: Role
  name: {{ $fullname }}-controller
  apiGroup: rbac.authorization.k8s.io
{{- end }}
{{- if not .Values.rbac.clusterWideSecrets }}
{{- range $ns, $names := (include "qumulo-csi-driver.secretNamespaces" . | fromJson) }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ $fullname }}-controller-secrets
  namespace: {{ $ns }}
  labels:
    {{- include "qumulo-csi-driver.labels" $ | nindent 4 }}
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}-controller
    namespace: {{ $.Release.Namespace }}
roleRef:
  kind: Role
  name: {{ $fullname }}-controller-secrets
  apiGroup: rbac.authorization.k8s.io
{{- end }}
{{- end }}
{{- if and (include "qumulo-csi-driver.isOpenShift" .) (include "qumulo-csi-driver.controllerHostNetwork" .) }}
---
# controller.hostNetwork: the controller's own SCC grant (hostnetwork-v2 by
# default), to the controller service account only and in this namespace only.
# Separate from the node's grant below: the controller never gets the node's
# privileged SCC, and the node never gets this one. restricted-v2 needs no
# grant (every service account may use it); hostnetwork-v2 does.
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ $fullname }}-controller-scc
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "qumulo-csi-driver.labels" . | nindent 4 }}
    app.kubernetes.io/component: controller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "qumulo-csi-driver.controllerSCCRole" . }}
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}-controller
    namespace: {{ .Release.Namespace }}
{{- end }}
{{- if include "qumulo-csi-driver.createNodeSCC" . }}
{{- $scc := include "qumulo-csi-driver.nodeSCCName" . }}
---
# A RoleBinding, not a ClusterRoleBinding: the node service account may use the
# SCC for pods in this namespace only.
#
# Named after the SCC, like the ClusterRole it binds. roleRef is immutable, so
# a binding with a fixed name could not follow a renamed SCC: the upgrade that
# renamed qumulo-csi-qumulo-csi-qumulo-csi-driver-node failed with "cannot
# change roleRef" after the DaemonSet already required the new SCC, leaving
# the node pod it replaced unschedulable (OCP 4.22, 2026-11-04). A name that
# changes with the SCC makes Helm create the new binding and delete the old
# one instead. The same holds for any change to openshift.nodeSCC.name.
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ $scc }}-use
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "qumulo-csi-driver.labels" . | nindent 4 }}
    app.kubernetes.io/component: node
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ $scc }}-use
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}-node
    namespace: {{ .Release.Namespace }}
{{- end }}
{{- if dig "tests" "ready" "enabled" true .Values.AsMap }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ $fullname }}-test
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "qumulo-csi-driver.labels" . | nindent 4 }}
    app.kubernetes.io/component: test
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ $fullname }}-test
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}-test
    namespace: {{ .Release.Namespace }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ printf "%s-%s-test" .Release.Namespace $fullname | trunc 253 }}
  labels:
    {{- include "qumulo-csi-driver.labels" . | nindent 4 }}
    app.kubernetes.io/component: test
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ printf "%s-%s-test" .Release.Namespace $fullname | trunc 253 }}
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}-test
    namespace: {{ .Release.Namespace }}
{{- end }}
{{- end -}}

{{/*
The bindings above as a JSON list of {kind, name, namespace, roleKind,
roleName, subjects: ["<namespace>/<name>", ...]}, for NOTES.txt and the live
check. Parsed from the same YAML, never written twice.
*/}}
{{- define "qumulo-csi-driver.bindingList" -}}
{{- $out := list -}}
{{- range $doc := regexSplit "(?m)^---[ \t]*$" (include "qumulo-csi-driver.bindingManifests" .) -1 -}}
  {{- $o := fromYaml $doc -}}
  {{- if and $o (hasKey $o "kind") -}}
    {{- $subjects := list -}}
    {{- range $s := ($o.subjects | default list) -}}
      {{- $subjects = append $subjects (printf "%s/%s" ($s.namespace | default "") $s.name) -}}
    {{- end -}}
    {{- $out = append $out (dict "kind" $o.kind "name" $o.metadata.name "namespace" ($o.metadata.namespace | default "") "roleKind" $o.roleRef.kind "roleName" $o.roleRef.name "subjects" $subjects) -}}
  {{- end -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}

{{/*
"true" when the chart renders its RoleBindings and ClusterRoleBindings
(rbac.createBindings, default true). Read with dig: a --reuse-values upgrade
from a release made before the value existed carries no such key.
*/}}
{{- define "qumulo-csi-driver.createBindings" -}}
{{- if dig "rbac" "createBindings" true .Values.AsMap -}}true{{- end -}}
{{- end -}}

{{/*
The Secrets the controller may read, by namespace -- every class's token
Secret, plus rbac.extraSecrets -- as JSON {namespace: [names]}. One Role and
one RoleBinding per namespace (rbac.yaml, rbac-bindings.yaml).
*/}}
{{- define "qumulo-csi-driver.secretNamespaces" -}}
{{- $byNamespace := dict -}}
{{- range $c := concat (.Values.storageClasses | default list) (.Values.volumeSnapshotClasses | default list) (.Values.volumeReplicationClasses | default list) (.Values.rbac.extraSecrets | default list) -}}
  {{- if $c.secretName -}}
    {{- $ns := $c.secretNamespace | default $.Release.Namespace -}}
    {{- $_ := set $byNamespace $ns (append (get $byNamespace $ns | default list) $c.secretName | uniq) -}}
  {{- end -}}
{{- end -}}
{{- toJson $byNamespace -}}
{{- end -}}
