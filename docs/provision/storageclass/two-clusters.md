# Two Qumulo clusters from one release

You do not need to install the driver twice to use two Qumulo clusters. One
installation can work with as many clusters as you like.

Give each cluster its own class and its own Secret, and that is all:

```yaml
storageClasses:
  - name: qumulo-site-a
    server: array-a.example.com
    apiUrl: https://array-a.example.com:8000
    secretName: qumulo-token-a
    basePath: /k8s/volumes
    isDefault: true

  - name: qumulo-site-b
    server: array-b.example.com
    apiUrl: https://array-b.example.com:8000
    secretName: qumulo-token-b
    basePath: /k8s/volumes
```

Each volume remembers which cluster it came from, so people only have to pick
a storage class name — `qumulo-site-a` or `qumulo-site-b` — and everything
after that goes to the right place.

Setting `isDefault: true` on one of them means anyone who does not name a
class gets that one.
