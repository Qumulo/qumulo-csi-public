{{/*
Install-time validation.

Every check here is something the driver would otherwise reject at provision
time, which surfaces as a PVC stuck Pending with the reason buried in a sidecar
log. Failing the install instead puts the message in front of the person who
can fix it, while they are looking at it.
*/}}
{{- define "qumulo-csi-driver.validate" -}}

{{- /*
An upgrade that renders none of the classes this release already
manages is almost always `helm upgrade --set ...` without the values file:
Helm then renders the chart's defaults, which hold no classes, and used to
delete every class. The classes carry helm.sh/resource-policy: keep, so
nothing is deleted any more; this refuses the upgrade too, because the same
render also drops every other value (images, sidecars). lookup answers only
against a live API server, so `helm template` and a client-side dry run are
unaffected. allowClassRemoval: true is the deliberate removal.
*/ -}}
{{- /*
Settings that rendered and then broke later. A zero duration is no
timeout at all for the driver's REST client, and fails csi-provisioner's
start-up probe as a sidecar timeout; the schema checks only the form. Two
metrics ports alike, or one on a livenessprobe port (29652 controller,
node.healthPort node), let a probe or a scrape reach the wrong process.
*/ -}}
{{- $durations := dict "qumulo.apiTimeout" (toString .Values.qumulo.apiTimeout) -}}
{{- with .Values.node.unmountTimeout -}}
  {{- /* A zero unmount bound skips the plain umount and goes straight to the
  forced and lazy escalation on every unpublish. */ -}}
  {{- $_ := set $durations "node.unmountTimeout" (toString .) -}}
{{- end -}}
{{- range $name := list "provisioner" "resizer" "snapshotter" -}}
  {{- $sidecar := index $.Values.sidecars $name | default dict -}}
  {{- if $sidecar.timeout -}}
    {{- $_ := set $durations (printf "sidecars.%s.timeout" $name) (toString $sidecar.timeout) -}}
  {{- end -}}
{{- end -}}
{{- range $key, $value := $durations -}}
  {{- if regexMatch "^([0.]+(ns|us|ms|s|m|h))+$" $value -}}
    {{- fail (printf "%s is %q, a zero duration: the call would have no timeout at all (or, for a sidecar, fail its start-up). Set a positive duration such as 30s" $key $value) -}}
  {{- end -}}
{{- end -}}
{{- /* The sidecars' --http-endpoint ports are bound whether or not metrics
       are published (their probes read them), so they are checked always; the
       drivers' own metrics ports only with metrics.enabled. The node's two
       health ports are host ports and must differ too. */ -}}
{{- $nodeHealth := include "qumulo-csi-driver.nodeHealthPort" . -}}
{{- $registrar := include "qumulo-csi-driver.nodeRegistrarHealthPort" . -}}
{{- if eq $nodeHealth $registrar -}}
  {{- fail (printf "node.registrarHealthPort and node.healthPort are both %s; each is a host port on every node and needs its own number" $registrar) -}}
{{- end -}}
{{- $owner := dict "29652" "the controller's livenessprobe" $nodeHealth "the node's livenessprobe (node.healthPort)" $registrar "the node's registrar (node.registrarHealthPort)" -}}
{{- if and (include "qumulo-csi-driver.controllerHostNetwork" .) .Values.sidecars.csiAddons.enabled -}}
  {{- /* On the host network the csi-addons port is a node port too, on the
         same nodes as the node pods' ports. */ -}}
  {{- $p := toString .Values.sidecars.csiAddons.controllerPort -}}
  {{- if hasKey $owner $p -}}
    {{- fail (printf "sidecars.csiAddons.controllerPort is %s, which %s already uses; with controller.hostNetwork both are ports on the node, so give it its own number" $p (index $owner $p)) -}}
  {{- end -}}
  {{- $_ := set $owner $p "sidecars.csiAddons.controllerPort" -}}
{{- end -}}
{{- $ports := list "provisioner" "resizer" "snapshotter" -}}
{{- if .Values.metrics.enabled -}}
  {{- $ports = concat (list "controller" "node") $ports -}}
{{- end -}}
{{- range $name := $ports -}}
  {{- $port := toString (index $.Values.metrics.port $name) -}}
  {{- if hasKey $owner $port -}}
    {{- fail (printf "metrics.port.%s is %s, which %s already uses; give each metrics port its own number" $name $port (index $owner $port)) -}}
  {{- end -}}
  {{- $_ := set $owner $port (printf "metrics.port.%s" $name) -}}
{{- end -}}

{{- /*
controller.hostNetwork on OpenShift: restricted-v2 and the other restricted
SCCs refuse host networking, so a controller pinned to one would never be
admitted ("Host network is not allowed to be used"). Usually a
--reuse-values upgrade carrying the old explicit restricted-v2.
*/ -}}
{{- if and (include "qumulo-csi-driver.isOpenShift" .) (include "qumulo-csi-driver.controllerHostNetwork" .) (hasPrefix "restricted" (include "qumulo-csi-driver.controllerSCC" .)) -}}
{{- fail (printf "controller.hostNetwork is true but openshift.controllerSCC is %s, which refuses host networking, so the controller pods would never be admitted. Set openshift.controllerSCC to hostnetwork-v2 (or leave it empty), or an SCC of your own that allows host networking" (include "qumulo-csi-driver.controllerSCC" .)) -}}
{{- end -}}

{{- /*
Without leader election every replica acts on every claim, so two
provisioners and resizers raced on the same volumes.
*/ -}}
{{- if and (not .Values.controller.leaderElection) (gt (int .Values.controller.replicaCount) 1) -}}
{{- fail (printf "controller.leaderElection is false with controller.replicaCount %d: without leader election every replica provisions, resizes and snapshots the same claims at once. Set replicaCount: 1, or turn leaderElection back on" (int .Values.controller.replicaCount)) -}}
{{- end -}}

{{- /*
The driver judges a snapshot its own by the sidecar's name prefix, so
the two must be given the same one; a prefix in extraArgs reaches only the
sidecar, and every snapshot it named would be refused deletion.
*/ -}}
{{- range .Values.sidecars.snapshotter.extraArgs -}}
{{- if hasPrefix "--snapshot-name-prefix" (toString .) -}}
{{- fail (printf "sidecars.snapshotter.extraArgs sets %q: set sidecars.snapshotter.snapshotNamePrefix instead, which the chart passes to both the sidecar and the driver -- the driver needs it to recognise the snapshots it made" (toString .)) -}}
{{- end -}}
{{- end -}}
{{- $prefix := toString .Values.sidecars.snapshotter.snapshotNamePrefix -}}
{{- if or (eq $prefix "") (regexMatch "[\\s/]" $prefix) -}}
{{- fail (printf "sidecars.snapshotter.snapshotNamePrefix must be non-empty, with no whitespace or \"/\", got %q" $prefix) -}}
{{- end -}}

{{- /*
The health monitor sends no Secret, so the driver can answer it only
from the classes -- the inventory path. Without it every poll of every volume
fails FailedPrecondition.
*/ -}}
{{- if and .Values.sidecars.healthMonitor.enabled (not .Values.controller.inventory.enabled) -}}
{{- fail "sidecars.healthMonitor.enabled needs controller.inventory.enabled: the monitor sends no Secret, so the driver finds each volume's credential from its StorageClass, which is the inventory path" -}}
{{- end -}}

{{- /*
Sidecar images must be pinned. The schema refuses an image with neither a tag
nor a digest; this refuses the tag "latest", which names whatever was pushed
last -- a different sidecar on each node that pulls it. Checked here rather
than in the schema because a schema "not" fails with a bare "'not' failed".
Every sidecar is checked, enabled or not, so turning one on later cannot be
what first runs an unpinned image.
*/ -}}
{{- range $name, $sidecar := .Values.sidecars -}}
  {{- if and (kindIs "map" $sidecar) (hasKey $sidecar "image") -}}
    {{- if regexMatch ":latest(@|$)" (toString $sidecar.image) -}}
      {{- fail (printf "sidecars.%s.image is %q: the tag latest is not a pin -- each node would run whatever was pushed last. Use a release tag, ideally with its digest: <name>:<tag>@sha256:<digest>" $name (toString $sidecar.image)) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- /* The helm test images too, read through dig: a --reuse-values upgrade
from an older chart carries no tests block. */ -}}
{{- $testImages := dict "tests.image" (dig "tests" "image" "" .Values.AsMap) "tests.ready.image" (dig "tests" "ready" "image" "" .Values.AsMap) -}}
{{- range $key, $image := $testImages -}}
  {{- if regexMatch ":latest(@|$)" (toString $image) -}}
    {{- fail (printf "%s is %q: the tag latest is not a pin -- each node would run whatever was pushed last. Use a release tag, ideally with its digest: <name>:<tag>@sha256:<digest>" $key (toString $image)) -}}
  {{- end -}}
{{- end -}}

{{- /*
Two classes of one kind with one name render two objects for one name;
Helm applied one, labelled it its own and never deleted it.
*/ -}}
{{- range $kind := list "storageClasses" "volumeSnapshotClasses" "volumeReplicationClasses" -}}
  {{- $seen := dict -}}
  {{- range $c := (get $.Values $kind | default list) -}}
    {{- if hasKey $seen (toString $c.name) -}}
      {{- fail (printf "%s: the name %q is used twice; each class needs a name of its own" $kind (toString $c.name)) -}}
    {{- end -}}
    {{- $_ := set $seen (toString $c.name) true -}}
  {{- end -}}
{{- end -}}

{{- /*
Checks against the live cluster. lookup answers only against a live API
server -- helm template and a client dry run see nothing -- so each of these
is skipped there, and a render offline is unchanged.
*/ -}}
{{- if lookup "v1" "Namespace" "" "kube-system" -}}

  {{- /*
  driverName names the CSIDriver, every class's provisioner and the
  kubelet plugin directory. An upgrade changing it failed on the immutable
  provisioner only after the rest was applied: a second CSIDriver, the
  controller and node plugins serving the new name, and every existing PV
  naming the old one.
  */ -}}
  {{- range $d := ((lookup "storage.k8s.io/v1" "CSIDriver" "" "").items | default list) -}}
    {{- $ann := $d.metadata.annotations | default dict -}}
    {{- if and (eq (index $ann "meta.helm.sh/release-name" | default "") $.Release.Name) (eq (index $ann "meta.helm.sh/release-namespace" | default "") $.Release.Namespace) (ne $d.metadata.name $.Values.driverName) -}}
      {{- fail (printf "driverName cannot change from %q to %q: every existing PersistentVolume names %q, and the StorageClasses' provisioner cannot be changed in place. Install a second release for the new name instead" $d.metadata.name $.Values.driverName $d.metadata.name) -}}
    {{- end -}}
  {{- end -}}
  {{- with lookup "storage.k8s.io/v1" "CSIDriver" "" .Values.driverName -}}
    {{- if or (ne (toString .spec.attachRequired) "false") (ne (toJson .spec.volumeLifecycleModes) (toJson (list "Persistent"))) -}}
      {{- fail (printf "CSIDriver %s has attachRequired %v and volumeLifecycleModes %v, which this chart renders as false and [Persistent]; both are immutable. Delete the CSIDriver with kubectl and upgrade again" .metadata.name .spec.attachRequired .spec.volumeLifecycleModes) -}}
    {{- end -}}
  {{- end -}}

  {{- /*
  The node pod is hostNetwork, so node.healthPort,
  node.registrarHealthPort and (with metrics) metrics.port.node are host ports
  on every node it runs on. A port another DaemonSet already holds -- most
  often a second release of this chart, or csi-driver-nfs on 29653 -- left
  every node pod Pending with "didn't have free ports", and on an upgrade
  stopped the rollout with the first node's plugin gone. Refused here, naming
  the DaemonSet. Our own DaemonSet (an upgrade) is skipped; another
  DaemonSet's node selector is not compared, so two kept on disjoint nodes
  must still differ.
  */ -}}
  {{- $mine := dict (include "qumulo-csi-driver.nodeHealthPort" .) "node.healthPort" (include "qumulo-csi-driver.nodeRegistrarHealthPort" .) "node.registrarHealthPort" -}}
  {{- if .Values.metrics.enabled -}}
    {{- $_ := set $mine (toString .Values.metrics.port.node) "metrics.port.node" -}}
  {{- end -}}
  {{- if include "qumulo-csi-driver.controllerHostNetwork" . -}}
    {{- /* controller.hostNetwork: the controller's ports are node ports on
           the nodes it runs on, which a DaemonSet's ports cover too. */ -}}
    {{- $_ := set $mine "29652" "the controller's livenessprobe port 29652 (controller.hostNetwork)" -}}
    {{- range $name := list "provisioner" "resizer" "snapshotter" -}}
      {{- $_ := set $mine (toString (index $.Values.metrics.port $name)) (printf "metrics.port.%s (controller.hostNetwork)" $name) -}}
    {{- end -}}
    {{- if .Values.metrics.enabled -}}
      {{- $_ := set $mine (toString .Values.metrics.port.controller) "metrics.port.controller (controller.hostNetwork)" -}}
    {{- end -}}
    {{- if .Values.sidecars.csiAddons.enabled -}}
      {{- $_ := set $mine (toString .Values.sidecars.csiAddons.controllerPort) "sidecars.csiAddons.controllerPort (controller.hostNetwork)" -}}
    {{- end -}}
  {{- end -}}
  {{- $ownDS := printf "%s-node" (include "qumulo-csi-driver.fullname" .) -}}
  {{- range $ds := ((lookup "apps/v1" "DaemonSet" "" "").items | default list) -}}
    {{- if not (and (eq $ds.metadata.namespace $.Release.Namespace) (eq $ds.metadata.name $ownDS)) -}}
      {{- $spec := $ds.spec.template.spec -}}
      {{- range $c := concat ($spec.initContainers | default list) ($spec.containers | default list) -}}
        {{- range $p := ($c.ports | default list) -}}
          {{- $host := $p.hostPort | default (ternary $p.containerPort 0 (eq (toString $spec.hostNetwork) "true")) | int -}}
          {{- if and $host (ne ($p.protocol | default "TCP") "UDP") (hasKey $mine (toString $host)) -}}
            {{- fail (printf "%s is %d, a host port on every node, but DaemonSet %s/%s (container %s) already holds it there; this release's pods would never start there. Give this release its own %s (a second release of this chart needs its own node.healthPort, node.registrarHealthPort and metrics.port.node)" (index $mine (toString $host)) $host $ds.metadata.namespace $ds.metadata.name $c.name (index $mine (toString $host))) -}}
          {{- end -}}
        {{- end -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{- /*
  rbac.createBindings=false: the platform team applies every RoleBinding and
  ClusterRoleBinding (_bindings.tpl). Each one this release needs must exist,
  point at the role this chart renders and name the right ServiceAccount;
  otherwise the controller would start without its permissions and fail every
  request, or the node pods would never be admitted. Checked before anything
  is applied, and read-only: nothing here creates, patches or adopts a
  binding. A binding this release made itself (an upgrade switching
  createBindings from true to false) is refused too unless it carries
  helm.sh/resource-policy: keep: Helm deletes what a new revision no longer
  renders, so the controller would lose its permissions mid-upgrade.
  rbac.checkBindings=false skips all of this, for an install identity that may
  not read bindings (lookup then fails with "forbidden").
  */ -}}
  {{- if and (not (include "qumulo-csi-driver.createBindings" .)) (dig "rbac" "checkBindings" true .Values.AsMap) -}}
    {{- $problems := list -}}
    {{- range $b := (include "qumulo-csi-driver.bindingList" . | fromJsonArray) -}}
      {{- $where := ternary (printf "%s %s" $b.kind $b.name) (printf "%s %s/%s" $b.kind $b.namespace $b.name) (eq $b.namespace "") -}}
      {{- $live := lookup "rbac.authorization.k8s.io/v1" $b.kind $b.namespace $b.name -}}
      {{- $ann := dig "metadata" "annotations" dict ($live | default dict) -}}
      {{- if not $live -}}
        {{- $problems = append $problems (printf "%s is missing" $where) -}}
      {{- else if and (eq (index $ann "meta.helm.sh/release-name" | default "") $.Release.Name) (eq (index $ann "meta.helm.sh/release-namespace" | default "") $.Release.Namespace) (ne (index $ann "helm.sh/resource-policy" | default "") "keep") -}}
        {{- /* Made by this release with createBindings=true: Helm would delete it right after this check. */ -}}
        {{- $problems = append $problems (printf "%s was made by this release, so Helm would delete it now; have the platform team mark it first (kubectl annotate %s %s helm.sh/resource-policy=keep%s)" $where (lower $b.kind) $b.name (ternary "" (printf " -n %s" $b.namespace) (eq $b.namespace ""))) -}}
      {{- else if or (ne $live.roleRef.kind $b.roleKind) (ne $live.roleRef.name $b.roleName) -}}
        {{- $problems = append $problems (printf "%s points at %s %s, not %s %s" $where $live.roleRef.kind $live.roleRef.name $b.roleKind $b.roleName) -}}
      {{- else -}}
        {{- $have := list -}}
        {{- range $s := ($live.subjects | default list) -}}
          {{- if eq $s.kind "ServiceAccount" -}}
            {{- $have = append $have (printf "%s/%s" ($s.namespace | default "") $s.name) -}}
          {{- end -}}
        {{- end -}}
        {{- range $want := $b.subjects -}}
          {{- if not (has $want $have) -}}
            {{- $problems = append $problems (printf "%s does not name ServiceAccount %s" $where $want) -}}
          {{- end -}}
        {{- end -}}
      {{- end -}}
    {{- end -}}
    {{- if $problems -}}
      {{- fail (printf "rbac.createBindings is false, so the platform team must apply this release's bindings before the install, and %d are not right: %s. Get the exact YAML with: helm template %s <chart> -n %s -f <your values> --set rbac.createBindings=true --show-only templates/rbac-bindings.yaml" (len $problems) (join "; " $problems) .Release.Name .Release.Namespace) -}}
    {{- end -}}
  {{- end -}}

  {{- /* A PriorityClass's value is immutable. */ -}}
  {{- if and (not .Values.node.priorityClassName) (dig "create" true (.Values.node.priorityClass | default dict)) -}}
    {{- $pc := printf "%s-node-critical" (include "qumulo-csi-driver.fullname" .) -}}
    {{- with lookup "scheduling.k8s.io/v1" "PriorityClass" "" $pc -}}
      {{- $want := dig "value" 1000000000 ($.Values.node.priorityClass | default dict) | int -}}
      {{- if ne (int .value) $want -}}
        {{- fail (printf "PriorityClass %s has value %d and this render %d; the value is immutable. Keep %d, or delete the PriorityClass with kubectl (running node pods keep their priority) and upgrade again" $pc (int .value) $want (int .value)) -}}
      {{- end -}}
      {{- /* So is its preemptionPolicy. Unset, the live one is kept. */ -}}
      {{- $policy := dig "preemptionPolicy" "" ($.Values.node.priorityClass | default dict) -}}
      {{- if and $policy (ne $policy (.preemptionPolicy | default "PreemptLowerPriority")) -}}
        {{- fail (printf "PriorityClass %s has preemptionPolicy %s and node.priorityClass.preemptionPolicy is %s; the field is immutable. Unset it to keep %s, or delete the PriorityClass with kubectl and upgrade again" $pc (.preemptionPolicy | default "PreemptLowerPriority") $policy (.preemptionPolicy | default "PreemptLowerPriority")) -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{- /*
  A class whose CRD the cluster does not serve. Said plainly here, rather
  than as Helm's "no matches for kind", which also wedged every later upgrade
  and rollback. Checked by served group/version/kind, not by the CRD's name: a
  CRD that no longer serves the version rendered here is as missing.
  volumereplicationclass.yaml makes the same check first, because it renders
  before this file and its immutability lookup would otherwise fail first.
  */ -}}
  {{- if .Values.volumeSnapshotClasses -}}
    {{- include "qumulo-csi-driver.requireServed" (dict "root" $ "gvk" "snapshot.storage.k8s.io/v1/VolumeSnapshotClass" "message" (include "qumulo-csi-driver.vscNotServed" $)) -}}
  {{- end -}}
  {{- if .Values.volumeReplicationClasses -}}
    {{- include "qumulo-csi-driver.requireServed" (dict "root" $ "gvk" "replication.storage.openshift.io/v1alpha1/VolumeReplicationClass" "message" (include "qumulo-csi-driver.vrcNotServed" $)) -}}
  {{- end -}}
{{- end -}}

{{- if and .Release.IsUpgrade (not .Values.allowClassRemoval) -}}
  {{- $dropped := list -}}
  {{- $kinds := list (list "storage.k8s.io/v1" "StorageClass" .Values.storageClasses) -}}
  {{- if .Capabilities.APIVersions.Has "snapshot.storage.k8s.io/v1/VolumeSnapshotClass" -}}
    {{- $kinds = append $kinds (list "snapshot.storage.k8s.io/v1" "VolumeSnapshotClass" .Values.volumeSnapshotClasses) -}}
  {{- end -}}
  {{- if .Capabilities.APIVersions.Has "replication.storage.openshift.io/v1alpha1/VolumeReplicationClass" -}}
    {{- $kinds = append $kinds (list "replication.storage.openshift.io/v1alpha1" "VolumeReplicationClass" .Values.volumeReplicationClasses) -}}
  {{- end -}}
  {{- range $k := $kinds -}}
    {{- $rendered := list -}}
    {{- range $c := (index $k 2) -}}
      {{- $rendered = append $rendered (toString $c.name) -}}
    {{- end -}}
    {{- $live := lookup (index $k 0) (index $k 1) "" "" -}}
    {{- range $item := ($live.items | default list) -}}
      {{- $ann := $item.metadata.annotations | default dict -}}
      {{- if and (eq (index $ann "meta.helm.sh/release-name" | default "") $.Release.Name) (eq (index $ann "meta.helm.sh/release-namespace" | default "") $.Release.Namespace) (not (has $item.metadata.name $rendered)) -}}
        {{- $dropped = append $dropped (printf "%s %s" (index $k 1) $item.metadata.name) -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
  {{- if $dropped -}}
    {{- fail (printf "this upgrade renders no %s, which release %q manages -- usually `helm upgrade --set ...` without the values file, which also resets every other value to the chart's default. Re-run with -f <your values file> or --reuse-values. To remove them deliberately, set allowClassRemoval=true; they are kept on the cluster (helm.sh/resource-policy: keep), so delete them with kubectl once nothing uses them" (join ", " $dropped) .Release.Name) -}}
  {{- end -}}
{{- end -}}

{{- if include "qumulo-csi-driver.trustedCABundle" . -}}
{{- if .Values.qumulo.caBundle.existingConfigMap -}}
{{- fail "openshift.trustedCABundle is set together with qumulo.caBundle.existingConfigMap: the controller can trust one CA bundle. Add your CA to the cluster Proxy's trustedCA and keep trustedCABundle, or drop trustedCABundle" -}}
{{- end -}}
{{- if .Values.qumulo.insecureSkipTlsVerify -}}
{{- fail "openshift.trustedCABundle is set together with qumulo.insecureSkipTlsVerify: true -- that supplies a CA and then ignores it" -}}
{{- end -}}
{{- end -}}
{{- if and .Values.qumulo.caBundle.existingConfigMap .Values.qumulo.insecureSkipTlsVerify -}}
{{- fail "qumulo.caBundle.existingConfigMap is set together with qumulo.insecureSkipTlsVerify: true -- that supplies a CA and then ignores it. Set insecureSkipTlsVerify: false to use the CA, or drop the caBundle" -}}
{{- end -}}
{{- if and .Values.qumulo.caBundle.existingConfigMap (not .Values.qumulo.caBundle.key) -}}
{{- fail "qumulo.caBundle.key is required when caBundle.existingConfigMap is set: it names the PEM entry in that ConfigMap and the file it is mounted as" -}}
{{- end -}}

{{- if not (has (toString .Values.fsGroupPolicy) (list "None" "File" "ReadWriteOnceWithFSType")) -}}
{{- fail (printf "fsGroupPolicy must be None, File or ReadWriteOnceWithFSType, got %q" (toString .Values.fsGroupPolicy)) -}}
{{- end -}}

{{- /*
An open dedicated export admits any client, as root, with no
authentication. Refused by default since 0.1.0; a Kerberos class
is not open -- its export requires the flavour -- so it needs neither
allowedClients nor allowAnyClient. Every open class is named in one message,
so an upgrade with several is fixed in one pass.
*/ -}}
{{- if .Values.requireAllowedClients -}}
  {{- $open := list -}}
  {{- range $sc := .Values.storageClasses -}}
    {{- if and (eq (toString $sc.provisioningModel) "dedicated-export") (not $sc.allowedClients) (not $sc.allowAnyClient) (not (hasPrefix "krb5" (toString $sc.securityFlavor))) -}}
      {{- $open = append $open (toString $sc.name) -}}
    {{- end -}}
  {{- end -}}
  {{- if $open -}}
    {{- fail (printf "storageClasses[%s]: a dedicated-export class with no allowedClients gives every volume an export any NFS client that reaches the array can mount, as root, from any port, with no authentication; set allowedClients to your nodes' addresses or CIDRs, or allowAnyClient: true on each class if that is intended (see the chart README's upgrade note)" (join ", " $open)) -}}
  {{- end -}}
{{- end -}}
{{- range $index, $sc := .Values.storageClasses -}}
  {{- $where := printf "storageClasses[%d]" $index -}}
  {{- /*
  A snapshotSchedule's tz= must name a zone the driver's IANA database has.
  The schema checks the schedule's shape but cannot hold the database, so a
  made-up, wrong-case or since-dropped zone used to install and then fail
  every claim on the class with "is not an IANA timezone". The list is the
  driver's own (_tzzones.tpl, from `make chart-timezones`). tz=Local is the
  schema's to refuse, with its own reason.
  */ -}}
  {{- with $sc.snapshotSchedule -}}
    {{- with regexFind "tz=[^,]*" (toString .) -}}
      {{- $tz := trimPrefix "tz=" . | trim -}}
      {{- $zones := splitList "\n" (include "qumulo-csi-driver.timezones" $) -}}
      {{- if and $tz (ne $tz "Local") (not (has $tz $zones)) -}}
        {{- $hint := "." -}}
        {{- range $zones -}}
          {{- if eq (lower .) (lower $tz) -}}
            {{- $hint = printf " -- zone names are case-sensitive; did you mean %q?" . -}}
          {{- end -}}
        {{- end -}}
        {{- fail (printf "storageClasses[%s]: snapshotSchedule tz=%s is not an IANA time zone%s Use a name such as \"UTC\" or \"America/Los_Angeles\"; the driver would refuse every claim on this class" (toString $sc.name) $tz $hint) -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}


  {{- if not $sc.name -}}
    {{- fail (printf "%s: name is required" $where) -}}
  {{- end -}}
  {{- if not $sc.server -}}
    {{- fail (printf "storageClasses[%s]: server is required -- a valid Qumulo endpoint must be set per StorageClass, because this driver keeps no fallback cluster" $sc.name) -}}
  {{- end -}}
  {{- /*
    server is a bare NFS host, and it is also the first field of every volume
    handle this class produces (<server>#<apiUrl>#<realPath>). A value carrying
    "/" or ":" therefore provisions, Binds, and then makes DeleteVolume and
    ControllerExpandVolume fail permanently on a handle the driver issued
    itself. Caught here as well as in the driver so it fails at
    `helm install` rather than at somebody's first PVC; the driver keeps its
    own check because a hand-written StorageClass never passes through Helm.
  */ -}}
  {{- if contains "://" (toString $sc.server) -}}
    {{- fail (printf "storageClasses[%s]: server is the NFS host to mount from, not a URL, got %q -- use a bare hostname or IPv4 address, and set this class's own apiUrl to the REST endpoint" $sc.name (toString $sc.server)) -}}
  {{- end -}}
  {{- if contains "/" (toString $sc.server) -}}
    {{- fail (printf "storageClasses[%s]: server must not contain \"/\", got %q -- it is the first field of this class's volume handles, which are \"#\"-separated, so a handle built from it could not be decoded" $sc.name (toString $sc.server)) -}}
  {{- end -}}
  {{- if contains ":" (toString $sc.server) -}}
    {{- fail (printf "storageClasses[%s]: server must not contain \":\", got %q -- NFS is always mounted on port 2049, so no port belongs here, and IPv6 cluster addresses are not supported in this release" $sc.name (toString $sc.server)) -}}
  {{- end -}}
  {{- if not (regexMatch "^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?([.][a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*$" (toString $sc.server)) -}}
    {{- fail (printf "storageClasses[%s]: server must be a hostname or IPv4 address such as \"qumulo.example.com\" or \"10.0.0.1\", got %q" $sc.name (toString $sc.server)) -}}
  {{- end -}}
  {{- /*
    apiUrl now travels in every handle this class mints, because DeleteVolume,
    ControllerExpandVolume and the snapshot RPCs are handed no parameters and
    a tenant network's NFS address does not serve REST. A value carrying the
    handle separator would split into fields that decode as a different
    address, so it is refused before a single volume exists.
  */ -}}
  {{- if $sc.apiUrl -}}
    {{- if contains "#" (toString $sc.apiUrl) -}}
      {{- fail (printf "storageClasses[%s]: apiUrl must not contain \"#\", got %q -- it separates the fields of this class's volume and snapshot handles, so a handle built from it could not be decoded" $sc.name (toString $sc.apiUrl)) -}}
    {{- end -}}
    {{- if not (hasPrefix "https://" (toString $sc.apiUrl)) -}}
      {{- fail (printf "storageClasses[%s]: apiUrl must be an https:// URL, got %q -- every call to it carries the API token, which http:// would send in cleartext; e.g. apiUrl: https://qumulo.example.com:8000" $sc.name (toString $sc.apiUrl)) -}}
    {{- end -}}
  {{- end -}}
  {{- if $sc.securityFlavor -}}
    {{- if not (has (toString $sc.securityFlavor) (list "sys" "krb5" "krb5i" "krb5p")) -}}
      {{- fail (printf "storageClasses[%s]: securityFlavor must be sys, krb5, krb5i or krb5p, got %q" $sc.name (toString $sc.securityFlavor)) -}}
    {{- end -}}
    {{- if ne (toString $sc.securityFlavor) "sys" -}}
      {{- if ne (toString ($sc.provisioningModel | default "shared-subdirectory")) "dedicated-export" -}}
        {{- fail (printf "storageClasses[%s]: securityFlavor %s requires provisioningModel: dedicated-export -- a shared-subdirectory volume sits under an export somebody else owns, and changing its authentication would affect every other volume beneath it" $sc.name (toString $sc.securityFlavor)) -}}
      {{- end -}}
      {{- if eq (toString ($sc.protocol | default "")) "nfs3" -}}
        {{- fail (printf "storageClasses[%s]: securityFlavor %s requires protocol nfs4.1 -- Kerberos is an NFSv4.1 feature here" $sc.name (toString $sc.securityFlavor)) -}}
      {{- end -}}
      {{- if not $.Values.node.kerberos.enabled -}}
        {{- fail (printf "storageClasses[%s]: securityFlavor %s needs node.kerberos.enabled -- without it the node has no keytab and no rpc.gssd, and the mount fails at the GSS handshake" $sc.name (toString $sc.securityFlavor)) -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
  {{- if not $sc.basePath -}}
    {{- fail (printf "storageClasses[%s]: basePath is required -- the cluster path volume directories are created under" $sc.name) -}}
  {{- end -}}
  {{- if not (hasPrefix "/" $sc.basePath) -}}
    {{- fail (printf "storageClasses[%s]: basePath must be absolute, got %q" $sc.name $sc.basePath) -}}
  {{- end -}}
  {{- if and $sc.exportBasePath (not (hasPrefix "/" $sc.exportBasePath)) -}}
    {{- fail (printf "storageClasses[%s]: exportBasePath must be absolute, got %q" $sc.name $sc.exportBasePath) -}}
  {{- end -}}
  {{- /* Refused only at provisioning before, so every claim stayed Pending. */ -}}
  {{- range $key := list "basePath" "exportBasePath" -}}
    {{- if contains "#" (toString (index $sc $key | default "")) -}}
      {{- fail (printf "storageClasses[%s]: %s must not contain \"#\", got %q -- it separates the fields of this class's volume handles, so a handle built from it could not be decoded" $sc.name $key (toString (index $sc $key))) -}}
    {{- end -}}
  {{- end -}}

  {{- if and (or $sc.tenantID $sc.allowedClients) (ne (toString ($sc.provisioningModel | default "shared-subdirectory")) "dedicated-export") -}}
    {{- fail (printf "storageClasses[%s]: tenantID and allowedClients require provisioningModel: dedicated-export -- a shared-subdirectory volume is a directory under somebody else's export, so there is no per-volume export to scope or restrict. Accepting them here would report an access restriction that is not applied" $sc.name) -}}
  {{- end -}}
  {{- if and $sc.allowedClients (not (kindIs "slice" $sc.allowedClients)) -}}
    {{- fail (printf "storageClasses[%s]: allowedClients must be a list of IPs/CIDRs, not a string" $sc.name) -}}
  {{- end -}}

  {{- if and $sc.protocol (not (has (toString $sc.protocol) (list "nfs3" "nfs4.1"))) -}}
    {{- fail (printf "storageClasses[%s]: protocol must be nfs3 or nfs4.1, got %q" $sc.name (toString $sc.protocol)) -}}
  {{- end -}}

  {{- if and $sc.provisioningModel (not (has (toString $sc.provisioningModel) (list "shared-subdirectory" "dedicated-export"))) -}}
    {{- fail (printf "storageClasses[%s]: provisioningModel must be shared-subdirectory or dedicated-export, got %q" $sc.name (toString $sc.provisioningModel)) -}}
  {{- end -}}

  {{- if and $sc.reclaimPolicy (not (has (toString $sc.reclaimPolicy) (list "Delete" "Retain"))) -}}
    {{- fail (printf "storageClasses[%s]: reclaimPolicy must be Delete or Retain, got %q" $sc.name (toString $sc.reclaimPolicy)) -}}
  {{- end -}}

  {{/*
  The version can be pinned by protocol or by a mount option, and pinning it in
  both places means one of them loses. The driver refuses such a volume at
  publish time, by which point the PVC is bound and a pod is stuck mounting.

  Each entry is split on commas first, as the node does: mount-utils joins the
  entries with commas, so "hard,vers=3" is two options.
  */}}
  {{- if $sc.protocol -}}
    {{- range $entry := ($sc.mountOptions | default list) -}}
      {{- range $option := splitList "," (toString $entry) -}}
        {{- $key := lower (trim $option) -}}
        {{- if or (hasPrefix "nfsvers=" $key) (hasPrefix "vers=" $key) -}}
          {{- fail (printf "storageClasses[%s]: protocol is set to %q, so mountOptions must not also pin the version with %q -- set it in one place" $sc.name (toString $sc.protocol) (trim $option)) -}}
        {{- end -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{/*
  An nfs3 class asking for server-side locking cannot work: the node image ships
  mount.nfs but runs no rpc.statd. The driver refuses such a volume at publish
  time, by which point the PVC is bound and a pod is stuck mounting, so it is
  caught here too.

  Only "lock" is refused. Omitting a lock option is fine and is the common case
  -- the node adds nolock itself -- and an explicit "nolock" or "local_lock="
  is the operator answering the question themselves.
  */}}
  {{- if eq (toString ($sc.protocol | default "")) "nfs3" -}}
    {{- range $entry := ($sc.mountOptions | default list) -}}
      {{- range $option := splitList "," (toString $entry) -}}
        {{- if eq (lower (trim $option)) "lock" -}}
          {{- fail (printf "storageClasses[%s]: protocol is nfs3 and mountOptions request %q, but the node image runs no rpc.statd, so server-side NFSv3 locking cannot work -- drop it to mount with nolock, or use protocol nfs4.1 where locking is in-protocol" $sc.name (trim $option)) -}}
        {{- end -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{- if $sc.mountPermissions -}}
    {{- $mode := lower (toString $sc.mountPermissions) -}}
    {{- if ne $mode "inherit" -}}
      {{- if not (regexMatch "^[0-7]{3,4}$" $mode) -}}
        {{- fail (printf "storageClasses[%s]: mountPermissions must be a 3- or 4-digit octal mode such as \"0777\", or \"inherit\", got %q" $sc.name (toString $sc.mountPermissions)) -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{/*
  Parameters the driver recognizes but refuses. Named individually so the
  message says why, rather than leaving the value to be silently dropped here
  and rejected later.

  This list is currently empty; both former entries are now implemented.

  snapshotSchedule was here until 2026-08-28 and is now implemented against
  the v1 policy API. Its grammar is validated by the driver rather than
  re-implemented in Go templates: a wrong schedule must fail the same way
  whether it arrived through the chart or a hand-written StorageClass.

  adDomain was here until 2026-08-31, refused on the grounds that an export
  maps identities by SID so a domain name cannot name a principal. That is
  still true, and it is why adDomain does NOT control access. It is now
  implemented as what it can be: a path namespace, nesting a class's volumes
  under a directory named for the domain. Its format is validated by the
  driver, for the same reason as the schedule above.
  */}}

{{- end -}}
{{- end -}}
