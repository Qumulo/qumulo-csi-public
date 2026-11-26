# Step 1 -- install the driver

The chart pulls its two images from the GitHub Container Registry
(`ghcr.io/qumulo/qumulo-csi-public/qumulo-csi-driver-controller` and `-node`). They are visible
to members of the Qumulo GitHub organization only, so the nodes pull with
your token, held in a pull secret.

Save the token to a file without it reaching the screen or shell history,
then make the secret from it:

```sh
read -rsp 'GitHub token: ' T && printf '%s' "$T" > ~/.ghcr-pat && chmod 600 ~/.ghcr-pat && unset T

kubectl create namespace qumulo-csi

kubectl -n qumulo-csi create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io \
  --docker-username=<your-github-user> \
  --docker-password="$(cat ~/.ghcr-pat)"
```

Then install, naming that secret. This walkthrough installs from the repository
folder, which is fine for a local evaluation; a real deployment installs the
published chart instead -- see the installation guide.

```sh
helm install qumulo-csi charts/qumulo-csi-driver \
  -n qumulo-csi \
  --set 'image.pullSecrets[0].name=ghcr-pull' \
  --set qumulo.insecureSkipTlsVerify=true    # a stock Qumulo cert has no SAN, so this is needed;
                                             # omit it once you install a cert with SANs -- see the
                                             # chart README's TLS section
```

No cluster address is set here, and there is nowhere to set one: the address
and the credential are both per StorageClass (`server` and `secretName`), so
they arrive in step 3.

This installs only the controller Deployment and node DaemonSet -- no
Secret, no StorageClass yet. Those are steps 2 and 3.

Confirm the controller is up before moving on:

```sh
kubectl -n qumulo-csi rollout status deploy/qumulo-csi-qumulo-csi-driver-controller
kubectl -n qumulo-csi get pods
```

You should see one controller pod and one node pod per worker, all
`Running`. A pod in `ErrImagePull` or `ImagePullBackOff` means the token or
the pull secret is wrong -- see
[image pull failures](../../docs/provision/troubleshoot.md#image-pull-failures).
