================================================================================
QUMULO CSI DRIVER 0.1.1 -- INSTALL ON OPENSHIFT
================================================================================

This file is a standalone installation guide for OpenShift Container Platform.
The chart and both container images are pulled from a public registry, so there
is no mirroring step, no pull secret and no disconnected-install procedure.

Everything in this file is also on the documentation site, under
"Install on OpenShift".


--------------------------------------------------------------------------------
1. SUPPORTED VERSIONS
--------------------------------------------------------------------------------

  OpenShift Container Platform ... 4.22
  Kubernetes ..................... 1.35 and later
  Node operating system .......... RHCOS 9.8
  NFS ............................ 4.1 and 3
  Architectures .................. x86_64, aarch64
  Driver ......................... 0.1.1

  OpenShift 4.21 and older are NOT supported. The chart enforces
  kubeVersion ">=1.35.0-0" and OpenShift 4.21 ships Kubernetes 1.34, so
  "helm install" refuses the chart rather than installing something that
  cannot work.

  Validated on 28 October 2026 against OpenShift 4.22 with SELinux enforcing,
  on both NFS 4.1 and NFS 3.


--------------------------------------------------------------------------------
2. BEFORE YOU START
--------------------------------------------------------------------------------

  On the OpenShift cluster:

    - The workers can reach the public registry. A cluster behind a proxy
      reaches it through the cluster-wide Proxy, which the kubelet already
      uses. That is separate from the controller's own proxy settings in
      section 8.

    - cluster-admin, or enough rights to create a project, a
      SecurityContextConstraints object and a ClusterRole. The chart creates
      the SCC itself; if your site requires an admin to own SCCs, see
      section 10.

  On your workstation:

    - oc, logged in to the cluster.
    - helm 3.13 or later. Helm pulls the chart; the kubelet pulls the images,
      so both machines need to reach the registry.

  On the Qumulo cluster:

    - A directory that already exists and is already exported over NFS. Each
      volume is created as a subdirectory beneath it.
    - An API token with rights to manage directories, quotas, exports and
      snapshots.

  Qumulo support gives you the public registry address and the exact chart
  version for your release. Throughout this file they appear as
  <public-registry>/<org> and 0.1.1+<commit>.


--------------------------------------------------------------------------------
3. WHAT THE CHART DOES ON OPENSHIFT
--------------------------------------------------------------------------------

  OpenShift admits every pod through a SecurityContextConstraint. The chart
  detects OpenShift by itself (openshift.enabled: auto) and then:

    Controller pods -> restricted-v2, OpenShift's own default.
        The controller only speaks HTTPS to the Qumulo REST API. The chart
        drops its fixed user ID so OpenShift can assign one from the
        project's range.

    Node plugin pods -> their own SCC, named
        <namespace>-<release>-qumulo-csi-driver-node.
        The node plugin mounts NFS on the host, so it must be privileged, on
        the host network, and able to use host paths. The SCC allows that and
        nothing more, and only the node service account may use it.

  Each pod names its SCC through the openshift.io/required-scc annotation, so
  OpenShift never silently picks another SCC a cluster admin granted for
  something else.

  There is no separate SCC step for you to run.


--------------------------------------------------------------------------------
4. CREATE THE PROJECT AND THE API TOKEN SECRET
--------------------------------------------------------------------------------

  oc new-project qumulo-csi

  oc -n qumulo-csi create secret generic qumulo-api-token \
    --from-literal=token='<api-token>'

  The key must be exactly "token". A Secret without it produces
  "no credential for cluster ..." on every provision.

  The chart renders no Secret, deliberately: a token in a values file would
  show up in "helm get values", and a Secret the chart does not own survives
  an uninstall.

  You need one Secret per Qumulo cluster.


--------------------------------------------------------------------------------
5. WRITE THE VALUES FILE
--------------------------------------------------------------------------------

  Create qumulo-values.yaml. This is the only file you author; everything
  else has a working default.

    storageClasses:
      - name: qumulo-nfs              # the name people will ask for
        server: qumulo.example.com    # host name or IP -- no https://, no port
        secretName: qumulo-api-token  # the Secret from section 4
        basePath: /k8s/volumes        # must exist, and be exported over NFS
        protocol: nfs4.1
        isDefault: false
        allowVolumeExpansion: true
        mountOptions:
          - hard
          - timeo=600

  Notes:

    - "server" is a host name, not a URL. Both "https://host:8000" and
      "host:8000" are rejected. If the Qumulo management interface is not on
      the default port, leave "server" as the bare host and set the class's
      own "apiUrl" instead.

    - Do NOT set image.controller.repository, image.node.repository or
      image.pullSecrets. The chart published to the public registry already
      defaults to the public image repositories.

    - A misspelled setting stops the install and names the key, rather than
      being ignored.


--------------------------------------------------------------------------------
6. INSTALL
--------------------------------------------------------------------------------

  helm install qumulo-csi \
    oci://<public-registry>/<org>/charts/qumulo-csi-driver \
    --version 0.1.1+<commit> \
    --namespace qumulo-csi \
    --values qumulo-values.yaml

  To see what the install will pull before you run it:

  helm show values oci://<public-registry>/<org>/charts/qumulo-csi-driver \
    --version 0.1.1+<commit> | grep -A2 'repository:'

  If you render manifests offline instead of installing directly -- helm
  template, Argo CD, any GitOps pipeline -- the render cannot see the cluster,
  does not detect OpenShift, and silently leaves the SCC out. Set
  openshift.enabled: true in qumulo-values.yaml for those.


--------------------------------------------------------------------------------
7. CHECK THE INSTALL
--------------------------------------------------------------------------------

  oc -n qumulo-csi get pods \
    -o custom-columns='NAME:.metadata.name,SCC:.metadata.annotations.openshift\.io/scc'

  The controller pods show restricted-v2; the node pods show the chart's own
  SCC. "helm status qumulo-csi -n qumulo-csi" prints both commands with your
  names filled in.

  On a fresh project the DaemonSet can be created before the SCC and its grant
  are in effect, so node pods may be refused for a few seconds. The DaemonSet
  retries and the pods are admitted. This is expected.


--------------------------------------------------------------------------------
8. THINGS TO CHECK FOR YOUR ENVIRONMENT
--------------------------------------------------------------------------------

  THE NODE PLUGIN BINDS A HOST PORT

    The node DaemonSet runs with hostNetwork: true, so the livenessprobe's and
    the registrar's /healthz ports are HOST ports on every worker:

      node:
        healthPort: 29653         # the default
        registrarHealthPort: 0    # the default: healthPort + 6, so 29659

    They must not collide with anything else on your workers. A second release
    of this chart on the same cluster needs its own healthPort (and, with
    metrics on, its own metrics.port.node); its registrar port then follows.
    The install or upgrade is refused, naming the DaemonSet that holds the
    port, if another DaemonSet already uses one. 29653 is also the upstream
    csi-driver-nfs node health port, so a cluster running both needs a
    different number here.

  CLUSTERS BEHIND A PROXY

    Pods installed with Helm do not inherit the cluster's proxy settings. Read
    them from the cluster:

      oc get proxy cluster -o jsonpath='{.status}'

    and give them to the controller:

      controller:
        proxy:
          httpsProxy: http://proxy.example.com:3128
          noProxy: 10.0.0.13        # arrays reached directly, if any

    The Kubernetes API, .svc and .cluster.local are always added to the
    no-proxy list.

  TRUST THE CLUSTER'S CA BUNDLE

    If the Qumulo cluster's certificate is signed by a CA your OpenShift
    cluster already trusts (its Proxy trustedCA), set:

      openshift:
        trustedCABundle: true

    OpenShift fills a ConfigMap the chart creates, and the controller reads it.
    This REPLACES qumulo.caBundle -- do not set both, and it is refused
    together with qumulo.insecureSkipTlsVerify.


--------------------------------------------------------------------------------
9. WHAT APPLICATION PODS NEED
--------------------------------------------------------------------------------

  Tested 28 October 2026 with NFS 4.1 and NFS 3, on OpenShift 4.22 with
  SELinux enforcing.

  NO SPECIAL SETTINGS FOR MOST APPS. A pod under OpenShift's default SCC,
  restricted-v2, runs with a random user ID and group 0. It can read and write
  a Qumulo volume with the default class settings. It needs no fsGroup, no
  supplementalGroups and no SELinux option. Two pods with different SELinux
  labels can share one volume.

  THE FOLDER PERMISSION DECIDES WHO CAN WRITE. Set it per StorageClass with
  mountPermissions:

    mountPermissions   restricted-v2     nonroot-v2        anyuid / privileged
                       (random uid)      (fixed non-root)  (root)
    ----------------   ---------------   ---------------   -------------------
    0777 (default)     read and write    read and write    read and write
    0770               read and write    read and write    read and write
                       (group 0)         (group 0)
    0750               read only         read only         read and write

  The driver sets fsGroupPolicy: None, so Kubernetes never changes file
  owners. A pod's fsGroup does not grant it access: with 0750, a pod with
  fsGroup still cannot write. Root pods write as root on the array unless the
  export squashes root.

  USER-NAMESPACE PODS CANNOT USE NFS VOLUMES. A pod with hostUsers: false
  (for example under restricted-v3) fails to start with
  "mount_setattr ... doesn't support idmap mounts on this kernel". The
  RHCOS 9.8 kernel cannot ID-map NFS mounts. This is a platform limit for
  every NFS volume, not only Qumulo's. Run such apps with hostUsers unset.

  READ-ONLY VOLUMES. Set readOnly: true on the pod's volume or volumeMounts.
  A class that only adds "ro" to mountOptions is refused on RHCOS.

  SINGLE-WRITER VOLUMES. ReadWriteOnce does not stop a second pod on another
  node from writing -- this driver has no attach stage, so Kubernetes does not
  enforce it. Use ReadWriteOncePod, which Kubernetes enforces itself.


--------------------------------------------------------------------------------
10. HANDING SCC MANAGEMENT TO A CLUSTER ADMIN
--------------------------------------------------------------------------------

  If your site does not let a chart create SCCs, have the cluster admin create
  one and grant it to the node service account, then install with:

    openshift:
      nodeSCC:
        create: false
        name: <the admin's SCC>

  The admin's SCC must allow privileged containers, privilege escalation, host
  network, host ports and host paths, and must permit the emptyDir and
  hostPath volume types. The exact permission set the chart's own SCC uses is
  in the documentation site under "Operate on OpenShift".


--------------------------------------------------------------------------------
11. UPGRADES
--------------------------------------------------------------------------------

  "helm upgrade --reuse-values" works, including from a release installed
  before the chart had OpenShift support. The old release's node pods never
  started on OpenShift; after the upgrade they start with no SCC step from you.

  Node pods roll one at a time. Running workloads keep their mounts throughout
  -- the mount belongs to the node's kernel, not to the node pod.

  IF AN UPGRADE FAILS PART-WAY, the failed revision's settings can be left on
  the live objects, and Helm compares the next upgrade against the last
  SUCCESSFUL revision, so it will not remove them. "helm rollback" does not
  repair this.

    Helm 3:

      helm upgrade qumulo-csi <chart> -n qumulo-csi -f qumulo-values.yaml --force

    Helm 4: the command above does not work. Helm 4 applies server side by
    default, and a replacement-strategy force does not fit that path. Repair
    by hand instead:

      oc delete deployment qumulo-csi-qumulo-csi-driver-controller -n qumulo-csi
      helm upgrade qumulo-csi <chart> -n qumulo-csi -f qumulo-values.yaml

    Deleting the controller Deployment stops provisioning, resize and snapshot
    calls until the new pods are Ready. Mounted volumes and running workloads
    are not affected. Name the node DaemonSet instead when it is the node
    plugin's settings that are stuck.

  Pods already running keep serving throughout. Volumes are not affected.


--------------------------------------------------------------------------------
12. TROUBLESHOOTING
--------------------------------------------------------------------------------

  NODE PODS NEVER START, NO SCC ON THEM
      OpenShift was not detected. This happens when manifests were rendered
      offline. Set openshift.enabled: true and upgrade.

  A CLAIM STAYS PENDING WITH "secrets <name> is forbidden"
      The controller may read only the Secrets its StorageClasses name, so for
      any other name the API answers "forbidden", not "not found". The Secret
      name in the class is misspelt, or the Secret is in another namespace.

  THE CONTROLLER CANNOT REACH THE ARRAY
      Check the proxy settings in section 8. Pods do not inherit the cluster
      proxy.

  TLS ERRORS TALKING TO THE QUMULO API
      Use openshift.trustedCABundle (section 8) rather than disabling
      verification.


--------------------------------------------------------------------------------
13. UNINSTALL
--------------------------------------------------------------------------------

  helm uninstall qumulo-csi -n qumulo-csi

  Delete the project last: deleting it first can leave the release's
  cluster-scoped objects, the SCC among them, with nothing to remove them.

  The API token Secret is not owned by the chart and is not removed.


================================================================================
Driver 0.1.1. CSI plugin name: csi.qumulo.com. Qumulo CSI Driver License.
================================================================================
