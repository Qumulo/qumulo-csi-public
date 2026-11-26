{{/*
Chart name, and the release-qualified name everything else is derived from.
*/}}
{{- define "qumulo-csi-driver.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "qumulo-csi-driver.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The resources for one node container, named by $.container: the per-container
entry of node.resources, or -- for the older flat form, one {requests, limits}
object -- that object for every container.
*/}}
{{- define "qumulo-csi-driver.nodeResources" -}}
{{- $r := .root.Values.node.resources | default dict -}}
{{- /* Empty -- including a --reuse-values upgrade from a chart whose default
       was {}, which keeps that old default -- means the same requests
       values.yaml ships. */ -}}
{{- if not $r -}}
{{- $small := dict "requests" (dict "cpu" "5m" "memory" "64Mi" "ephemeral-storage" "16Mi") "limits" (dict "memory" "64Mi" "ephemeral-storage" "500Mi") -}}
{{- $r = dict "registrar" $small "livenessProbe" $small
              "driver" (dict "requests" (dict "cpu" "20m" "memory" "256Mi" "ephemeral-storage" "16Mi") "limits" (dict "memory" "256Mi" "ephemeral-storage" "500Mi")) -}}
{{- end -}}
{{- /* The flat form: one {requests, limits} object for every container. Only
       those keys are emitted -- Helm has already coalesced $r with the
       per-container defaults, so $r also holds registrar/livenessProbe/driver,
       and passing it through whole renders an invalid ResourceRequirements
       the API server refuses (DOCNEW42). A flat form wins over a per-container
       key set alongside it. */ -}}
{{- if or (hasKey $r "requests") (hasKey $r "limits") -}}
{{- toYaml (pick $r "requests" "limits" "claims") -}}
{{- else -}}
{{- toYaml (get $r .container | default dict) -}}
{{- end -}}
{{- end -}}

{{/*
The node pods' PriorityClass: node.priorityClassName when set, else the one
the chart creates, else none. A missing node.priorityClass -- a --reuse-values
upgrade keeps the previous chart's defaults, which had none -- means create.
*/}}
{{- define "qumulo-csi-driver.nodePriorityClassName" -}}
{{- if .Values.node.priorityClassName -}}
{{- .Values.node.priorityClassName -}}
{{- else if dig "create" true (.Values.node.priorityClass | default dict) -}}
{{- include "qumulo-csi-driver.fullname" . }}-node-critical
{{- end -}}
{{- end -}}

{{- define "qumulo-csi-driver.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "qumulo-csi-driver.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: {{ include "qumulo-csi-driver.name" . }}
{{- /*
app.kubernetes.io/component is reserved too even though this helper never
sets it -- each caller appends it after this include (e.g. controller.yaml),
so a commonLabels override of that key would render as a second, duplicate
"app.kubernetes.io/component" mapping key in the same block instead of merging.
*/}}
{{- $reserved := list "helm.sh/chart" "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/version" "app.kubernetes.io/managed-by" "app.kubernetes.io/part-of" "app.kubernetes.io/component" }}
{{- range $k, $v := .Values.commonLabels }}
{{- if not (has $k $reserved) }}
{{ $k }}: {{ $v | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "qumulo-csi-driver.controllerSelectorLabels" -}}
app.kubernetes.io/name: {{ include "qumulo-csi-driver.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: controller
{{- end -}}

{{- define "qumulo-csi-driver.nodeSelectorLabels" -}}
app.kubernetes.io/name: {{ include "qumulo-csi-driver.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: node
{{- end -}}

{{/*
Image references. An empty tag falls back to the chart's appVersion, so the
chart and the driver it deploys stay in step by default.
*/}}
{{- define "qumulo-csi-driver.controllerImage" -}}
{{- printf "%s:%s" .Values.image.controller.repository (default .Chart.AppVersion .Values.image.controller.tag) -}}
{{- end -}}

{{- define "qumulo-csi-driver.nodeImage" -}}
{{- printf "%s:%s" .Values.image.node.repository (default .Chart.AppVersion .Values.image.node.tag) -}}
{{- end -}}

{{/*
Pull policy for a driver image, given (list <image values> <root>). An
explicit pullPolicy wins. Otherwise the tag decides: a digest, or a tag
carrying a "-" suffix ("0.1.1-<commit>", what `make docker-build` applies
locally and never publishes), names one image for good and is pulled
IfNotPresent; anything else -- the bare release tag "0.1.1", the only tag a
released image carries and one that every publish of that version moves --
is pulled Always, so a node never keeps running an earlier image under the
same name.
*/}}
{{- define "qumulo-csi-driver.pullPolicy" -}}
{{- $image := index . 0 -}}
{{- $root := index . 1 -}}
{{- if $image.pullPolicy -}}
{{- $image.pullPolicy -}}
{{- else -}}
{{- $tag := default $root.Chart.AppVersion $image.tag -}}
{{- if or (contains "@sha256:" $tag) (contains "-" $tag) -}}
IfNotPresent
{{- else -}}
Always
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The directory the kubelet expects this plugin's socket in. Derived from the
driver name, because that is what node-driver-registrar tells the kubelet.
*/}}
{{- define "qumulo-csi-driver.pluginDir" -}}
{{- printf "%s/plugins/%s" (.Values.node.kubeletDir | trimSuffix "/") .Values.driverName -}}
{{- end -}}

{{/*
controllerResources renders one controller container's resources: the
driver's from controller.resources, csi-provisioner's from
controller.provisionerResources, csi-resizer's from
controller.resizerResources, every other sidecar's from
controller.sidecarResources. An empty or missing value -- what a
--reuse-values upgrade from a chart that predates these defaults (or, for the
provisioner and the resizer, their own settings) carries -- falls back to the
default, so such an upgrade does not leave the pod BestEffort or those two at
the other sidecars' 128Mi.
*/}}
{{- define "qumulo-csi-driver.controllerResources" -}}
{{- $c := .root.Values.controller -}}
{{- if eq .container "driver" -}}
{{- toYaml ($c.resources | default (dict "requests" (dict "cpu" "20m" "memory" "1Gi" "ephemeral-storage" "16Mi") "limits" (dict "memory" "1Gi" "ephemeral-storage" "500Mi"))) -}}
{{- else if eq .container "provisioner" -}}
{{- toYaml ($c.provisionerResources | default (dict "requests" (dict "cpu" "5m" "memory" "512Mi" "ephemeral-storage" "16Mi") "limits" (dict "memory" "512Mi" "ephemeral-storage" "500Mi"))) -}}
{{- else if eq .container "resizer" -}}
{{- toYaml ($c.resizerResources | default (dict "requests" (dict "cpu" "5m" "memory" "256Mi" "ephemeral-storage" "16Mi") "limits" (dict "memory" "256Mi" "ephemeral-storage" "500Mi"))) -}}
{{- else -}}
{{- toYaml ($c.sidecarResources | default (dict "requests" (dict "cpu" "5m" "memory" "128Mi" "ephemeral-storage" "16Mi") "limits" (dict "memory" "128Mi" "ephemeral-storage" "500Mi"))) -}}
{{- end -}}
{{- end -}}

{{/*
leaderElectionTimings renders the csi-lib-utils lease flags the operator set
under controller.leaderElectionTimings; none by default, which keeps the
sidecars' own 15s / 10s / 5s.
*/}}
{{- define "qumulo-csi-driver.leaderElectionTimings" -}}
{{- with .Values.controller.leaderElectionTimings -}}
{{- with .leaseDuration }}
- --leader-election-lease-duration={{ . }}
{{- end }}
{{- with .renewDeadline }}
- --leader-election-renew-deadline={{ . }}
{{- end }}
{{- with .retryPeriod }}
- --leader-election-retry-period={{ . }}
{{- end }}
{{- end -}}
{{- end -}}

{{/*
guardImmutable fails the render when an upgrade would change a field the API
server will not let it change on an existing object -- a StorageClass's
provisioner, reclaimPolicy, volumeBindingMode or parameters, a
VolumeReplicationClass's parameters, a PriorityClass's value. Helm applies
every object and collects the errors, so such an upgrade used to roll the new
images, then report UPGRADE FAILED and leave the release failed.
lookup answers only against a live API server, so helm template and a client
dry run render as before. Values are compared as strings, which is how the API
server holds every parameter.
*/}}
{{- define "qumulo-csi-driver.guardImmutable" -}}
{{- /*
A kind the cluster does not serve has nothing live to compare, and a lookup
of it fails with Helm's raw "could not find the requested resource" before
the chart can say why. The caller passes root to have that checked.
*/ -}}
{{- if or (not .root) (.root.Capabilities.APIVersions.Has (printf "%s/%s" .apiVersion .kind)) -}}
{{- $live := lookup .apiVersion .kind "" .name -}}
{{- if $live -}}
{{- range $field := .fields -}}
  {{- $newVal := get $.new $field -}}
  {{- $liveVal := get $live $field -}}
  {{- if and (eq $.kind "VolumeReplicationClass") (eq $field "parameters") -}}
    {{- $newVal = get ($.new.spec | default dict) "parameters" -}}
    {{- $liveVal = get ($live.spec | default dict) "parameters" -}}
  {{- end -}}
  {{- if kindIs "map" $newVal -}}
    {{- $n := dict -}}{{- range $k, $v := $newVal }}{{- $_ := set $n $k (toString $v) -}}{{- end -}}
    {{- $l := dict -}}{{- range $k, $v := ($liveVal | default dict) }}{{- $_ := set $l $k (toString $v) -}}{{- end -}}
    {{- $newVal = toJson $n -}}{{- $liveVal = toJson $l -}}
  {{- else -}}
    {{- $newVal = toString $newVal -}}{{- $liveVal = toString $liveVal -}}
  {{- end -}}
  {{- if and (ne $newVal $liveVal) (eq $field "provisioner") -}}
    {{- fail (printf "%s %s: its provisioner would change from %s to %s, which means driverName changed. Every existing PersistentVolume names %s, and a provisioner cannot change in place: install a second release for the new name instead" $.kind $.name $liveVal $newVal $liveVal) -}}
  {{- end -}}
  {{- if ne $newVal $liveVal -}}
    {{- fail (printf "%s %s: %s cannot change on an existing object (the API server refuses it), and Helm would roll the new images before failing on it. Live: %s; this render: %s. Keep the old value, give the class a new name, or delete the old %s with kubectl once nothing uses it (PersistentVolumes are kept) and upgrade again" $.kind $.name $field $liveVal $newVal $.kind) -}}
  {{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- /*
requireServed fails with .message when the live cluster does not serve .gvk
("group/version/Kind"). Discovery lists only served versions, so this catches
a missing CRD and a CRD that no longer serves the version this chart renders
alike. Offline -- helm template, a client dry run -- it does nothing.
*/ -}}
{{- define "qumulo-csi-driver.requireServed" -}}
{{- if and (lookup "v1" "Namespace" "" "kube-system") (not (.root.Capabilities.APIVersions.Has .gvk)) -}}
{{- fail .message -}}
{{- end -}}
{{- end -}}

{{- define "qumulo-csi-driver.vrcNotServed" -}}
volumeReplicationClasses are set but the cluster does not serve replication.storage.openshift.io/v1alpha1 VolumeReplicationClass (the CRD is missing, or does not serve v1alpha1): install the csi-addons CRDs first, or remove volumeReplicationClasses (and set sidecars.csiAddons.enabled=false)
{{- end -}}

{{- define "qumulo-csi-driver.vscNotServed" -}}
volumeSnapshotClasses are set but the cluster does not serve snapshot.storage.k8s.io/v1 VolumeSnapshotClass (the CRD is missing, or does not serve v1): install the external-snapshotter CRDs first, or remove volumeSnapshotClasses (and set sidecars.snapshotter.enabled=false)
{{- end -}}

{{/*
One openshift.* value, read with its default when the key is missing.
A `helm upgrade --reuse-values` from a release made before this block existed
carries no `openshift` key at all -- --reuse-values keeps the old release's
values and ignores the new chart's values.yaml -- so `.Values.openshift.enabled`
was a nil pointer and the upgrade failed. Every openshift.* read goes through
here. `dig` returns a value that is set even when it is false or empty, so an
explicit `nodeSCC.create: false` still wins over the default.

Called as (list $ "nodeSCC" "create" true): the root, the path, the default.
*/}}
{{- define "qumulo-csi-driver.openshiftValue" -}}
{{- $root := first . -}}
{{- $args := rest . -}}
{{- $path := initial $args -}}
{{- $default := last $args -}}
{{- $v := $root.Values.AsMap -}}
{{- $found := true -}}
{{- range $key := prepend $path "openshift" -}}
{{- if and $found (kindIs "map" $v) (hasKey $v $key) -}}
{{- $v = index $v $key -}}
{{- else -}}
{{- $found = false -}}
{{- end -}}
{{- end -}}
{{- if and $found (not (kindIs "invalid" $v)) -}}{{ $v }}{{- else -}}{{ $default }}{{- end -}}
{{- end -}}

{{/*
"true" when the chart renders for OpenShift: openshift.enabled true, or auto
and the cluster serves SecurityContextConstraints. Empty otherwise, so it reads
as a boolean in `if`.
*/}}
{{- define "qumulo-csi-driver.isOpenShift" -}}
{{- $e := include "qumulo-csi-driver.openshiftValue" (list . "enabled" "auto") -}}
{{- if or (eq $e "true") (and (eq $e "auto") (.Capabilities.APIVersions.Has "security.openshift.io/v1/SecurityContextConstraints")) -}}true{{- end -}}
{{- end -}}

{{/*
The SCC the controller pods are pinned to (openshift.controllerSCC). Empty:
hostnetwork-v2 with controller.hostNetwork, restricted-v2 otherwise.
*/}}
{{- define "qumulo-csi-driver.controllerSCC" -}}
{{- include "qumulo-csi-driver.openshiftValue" (list . "controllerSCC" "") | default (ternary "hostnetwork-v2" "restricted-v2" (include "qumulo-csi-driver.controllerHostNetwork" . | eq "true")) -}}
{{- end -}}

{{/*
"true" when the controller runs on the node's network (controller.hostNetwork).
Read with dig: a --reuse-values upgrade from a release made before the value
existed carries no such key.
*/}}
{{- define "qumulo-csi-driver.controllerHostNetwork" -}}
{{- if dig "controller" "hostNetwork" false .Values.AsMap -}}true{{- end -}}
{{- end -}}

{{/*
The ClusterRole that grants use of the controller's SCC, for the grant in
_bindings.tpl (openshift.controllerSCCRole; empty:
system:openshift:scc:<controllerSCC>, which OpenShift ships for its own SCCs).
*/}}
{{- define "qumulo-csi-driver.controllerSCCRole" -}}
{{- include "qumulo-csi-driver.openshiftValue" (list . "controllerSCCRole" "") | default (printf "system:openshift:scc:%s" (include "qumulo-csi-driver.controllerSCC" .)) -}}
{{- end -}}

{{/* "true" when the chart renders the node SCC and its grant itself. */}}
{{- define "qumulo-csi-driver.createNodeSCC" -}}
{{- if and (include "qumulo-csi-driver.isOpenShift" .) (eq (include "qumulo-csi-driver.openshiftValue" (list . "nodeSCC" "create" true)) "true") -}}true{{- end -}}
{{- end -}}

{{/*
The node plugin's SCC. Cluster-scoped, so the namespace is in the name:
<namespace>-<fullname>-node. When the release is named after its namespace (the
documented `helm install qumulo-csi ... -n qumulo-csi`), the fullname already
starts with the namespace, and prefixing it again gave
qumulo-csi-qumulo-csi-qumulo-csi-driver-node; that case is <fullname>-node.
The namespace is still in the name, so releases in different namespaces stay
apart. A name another release already owns fails the install (Helm refuses to
adopt an object with another release's ownership annotations); it is never
shared.
*/}}
{{- define "qumulo-csi-driver.nodeSCCName" -}}
{{- $fullname := include "qumulo-csi-driver.fullname" . -}}
{{- $derived := printf "%s-%s-node" .Release.Namespace $fullname -}}
{{- if and (eq .Release.Name .Release.Namespace) (hasPrefix (printf "%s-" .Release.Namespace) $fullname) -}}
{{- $derived = printf "%s-node" $fullname -}}
{{- end -}}
{{- include "qumulo-csi-driver.openshiftValue" (list . "nodeSCC" "name" "") | default $derived | trunc 253 -}}
{{- end -}}

{{/*
"true" when OpenShift assigns the controller's uid, so the chart leaves
runAsUser and runAsGroup out (openshift.assignControllerUID). auto: when the
controller's SCC is named restricted* or hostnetwork* -- restricted-v2,
restricted-v3, the usual custom copies such as restricted-csi, and
hostnetwork-v2 (controller.hostNetwork) assign a uid from the namespace's
range and refuse any other, 65532 included. true: always on OpenShift, for an
SCC that assigns a uid under another name (MustRunAsRange). false: never, for
an SCC that lets the pod pick its uid (nonroot-v2, anyuid). Off OpenShift the
uid is always kept. The helm test pods follow the same rule and the same SCC.
*/}}
{{- define "qumulo-csi-driver.assignControllerUID" -}}
{{- if include "qumulo-csi-driver.isOpenShift" . -}}
{{- $a := include "qumulo-csi-driver.openshiftValue" (list . "assignControllerUID" "auto") -}}
{{- $scc := include "qumulo-csi-driver.controllerSCC" . -}}
{{- if or (eq $a "true") (and (eq $a "auto") (or (hasPrefix "restricted" $scc) (hasPrefix "hostnetwork" $scc))) -}}true{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The SCC the helm test pods are pinned to (openshift.testSCC). Empty: the
controller's SCC -- except with controller.hostNetwork, where the test pods,
which never use the host network, stay on restricted-v2 rather than needing a
grant of the controller's host-network SCC to the test service account.
*/}}
{{- define "qumulo-csi-driver.testSCC" -}}
{{- include "qumulo-csi-driver.openshiftValue" (list . "testSCC" "") | default (ternary "restricted-v2" (include "qumulo-csi-driver.controllerSCC" .) (include "qumulo-csi-driver.controllerHostNetwork" . | eq "true")) -}}
{{- end -}}

{{/*
"true" when OpenShift assigns the helm test pods' uid: assignControllerUID's
rule, read against testSCC.
*/}}
{{- define "qumulo-csi-driver.assignTestUID" -}}
{{- if include "qumulo-csi-driver.isOpenShift" . -}}
{{- $a := include "qumulo-csi-driver.openshiftValue" (list . "assignControllerUID" "auto") -}}
{{- $scc := include "qumulo-csi-driver.testSCC" . -}}
{{- if or (eq $a "true") (and (eq $a "auto") (or (hasPrefix "restricted" $scc) (hasPrefix "hostnetwork" $scc))) -}}true{{- end -}}
{{- end -}}
{{- end -}}

{{/*
controller.securityContext for every controller container, without
runAsUser and runAsGroup when OpenShift assigns the uid (assignControllerUID
above). An SCC that assigns none (nonroot-v2, anyuid, privileged) keeps the
uid: without it the sidecar images run as root and runAsNonRoot refuses them.
*/}}
{{- define "qumulo-csi-driver.controllerSecurityContext" -}}
{{- $sc := .Values.controller.securityContext | default dict -}}
{{- if include "qumulo-csi-driver.assignControllerUID" . -}}
{{- $sc = omit $sc "runAsUser" "runAsGroup" -}}
{{- end -}}
{{- with $sc -}}
{{- toYaml . -}}
{{- end -}}
{{- end -}}

{{/*
node.healthPort, 29653 when unset: a --reuse-values upgrade from a chart
without the value carries none.
*/}}
{{- define "qumulo-csi-driver.nodeHealthPort" -}}
{{- .Values.node.healthPort | default 29653 | int -}}
{{- end -}}

{{/*
node.registrarHealthPort: the registrar's /healthz, a host port on the node's
own address. 0 or unset (a --reuse-values upgrade from a chart without the
value carries none) is node.healthPort + 6: 29659 for the default 29653.
Derived rather than fixed so a second release on the cluster, which already
needs its own node.healthPort, gets its own registrar port too: a fixed 29659
in every release left the second release's node pods unschedulable.
*/}}
{{- define "qumulo-csi-driver.nodeRegistrarHealthPort" -}}
{{- .Values.node.registrarHealthPort | default (add (include "qumulo-csi-driver.nodeHealthPort" . | int) 6) | int -}}
{{- end -}}

{{/*
The ConfigMap whose CA the controller trusts for the Qumulo API, as
"<name>/<key>", or empty: openshift.trustedCABundle's injected bundle, else
qumulo.caBundle.existingConfigMap.
*/}}
{{- define "qumulo-csi-driver.caConfigMap" -}}
{{- if include "qumulo-csi-driver.trustedCABundle" . -}}
{{- printf "%s-trusted-ca/ca-bundle.crt" (include "qumulo-csi-driver.fullname" .) -}}
{{- else if .Values.qumulo.caBundle.existingConfigMap -}}
{{- printf "%s/%s" .Values.qumulo.caBundle.existingConfigMap .Values.qumulo.caBundle.key -}}
{{- end -}}
{{- end -}}

{{/* "true" when the controller trusts OpenShift's injected CA bundle. */}}
{{- define "qumulo-csi-driver.trustedCABundle" -}}
{{- if and (include "qumulo-csi-driver.isOpenShift" .) (eq (include "qumulo-csi-driver.openshiftValue" (list . "trustedCABundle" false)) "true") -}}true{{- end -}}
{{- end -}}

{{/*
metrics.networkPolicy.from. Left at the chart's default -- a namespace
named `monitoring` -- on OpenShift, which has none: there the Prometheus that
scrapes user namespaces runs in openshift-user-workload-monitoring, and the
platform one in openshift-monitoring. A `from` the operator set is used as is.
*/}}
{{- define "qumulo-csi-driver.metricsPolicyFrom" -}}
{{- $from := .Values.metrics.networkPolicy.from -}}
{{- $chartDefault := list (dict "namespaceSelector" (dict "matchLabels" (dict "kubernetes.io/metadata.name" "monitoring"))) -}}
{{- if and (include "qumulo-csi-driver.isOpenShift" .) (eq (toJson $from) (toJson $chartDefault)) -}}
{{- $from = list (dict "namespaceSelector" (dict "matchLabels" (dict "kubernetes.io/metadata.name" "openshift-user-workload-monitoring"))) (dict "namespaceSelector" (dict "matchLabels" (dict "kubernetes.io/metadata.name" "openshift-monitoring"))) -}}
{{- end -}}
{{- toYaml $from -}}
{{- end -}}

{{/*
Liveness and readiness for a CSI sidecar with --http-endpoint on the named
port. With leader election, csi-lib-utils serves /healthz/leader-election
there: 200 on a standby, and on the leader while it keeps renewing its lease,
so a leader that has lost its lease without noticing is restarted. Without
leader election that path is not registered, so the probe is the listener
accepting connections.
*/}}
{{- define "qumulo-csi-driver.sidecarProbes" -}}
{{- $probe := dict "tcpSocket" (dict "port" .port) -}}
{{- if .root.Values.controller.leaderElection -}}
{{- $probe = dict "httpGet" (dict "path" "/healthz/leader-election" "port" .port) -}}
{{- end -}}
livenessProbe:
  {{- toYaml $probe | nindent 2 }}
  initialDelaySeconds: 10
  periodSeconds: 30
  timeoutSeconds: 10
  failureThreshold: 5
readinessProbe:
  {{- toYaml $probe | nindent 2 }}
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 10
  failureThreshold: 3
{{- end -}}
