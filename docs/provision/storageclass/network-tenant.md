# Dedicated export on a network tenant

Qumulo can split one cluster into separate networks, called tenants, so that
different teams only see their own. A class set up for a tenant is reachable
only from that tenant's network.

These classes usually need **two addresses**, because the storage traffic and
the management traffic travel on different networks.

```yaml
storageClasses:
  - name: qumulo-finance
    server: 10.20.4.60                   # the tenant's address, for storage traffic
    apiUrl: https://10.20.0.10:8000      # the management address, for the driver's own calls
    secretName: qumulo-api-token
    basePath: /k8s/finance
    provisioningModel: dedicated-export
    tenantID: tenant-finance             # the tenant name, or its number
    allowedClients:
      - 10.20.4.0/24
```

Both addresses have to be right:

- **`server`** must be one of the tenant's own addresses. If it is not,
  volumes look fine at first and then fail when a pod tries to use them.
- **`apiUrl`** must be the management address. Without it, the driver tries to
  manage the cluster over the tenant's storage address, which does not answer
  those requests — so deleting and resizing volumes stops working.
