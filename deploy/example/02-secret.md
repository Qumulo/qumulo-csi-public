# Step 2 -- the Qumulo API token as a Kubernetes Secret

The key must be named `token` -- that is the key the driver reads out of
whichever Secret a StorageClass's `secretName` points at.

**Create it from your token file, not by pasting the value into a YAML
manifest.** A manifest with the token embedded in it is one `git add .` away
from being in your history permanently; `--from-file` never writes the value
anywhere but the Secret itself.

```sh
NAMESPACE=qumulo-csi
SECRET_NAME=qumulo-api-token
TOKEN_FILE=/path/to/your/token.txt   # keep this file outside your git checkout

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME" \
  --from-file=token="$TOKEN_FILE"
```

Confirm it exists -- this prints metadata only, never the value:

```sh
kubectl -n "$NAMESPACE" get secret "$SECRET_NAME"
```

## Rotating the token later

The controller re-reads the token file on every request
(`pkg/qumulo/token.go`'s `TokenFromFile`), so rotating the Secret takes
effect immediately -- nothing needs restarting.

```sh
kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME" \
  --from-file=token="$TOKEN_FILE" \
  --dry-run=client -o yaml | kubectl -n "$NAMESPACE" apply -f -
```
