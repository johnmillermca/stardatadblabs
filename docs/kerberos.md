# Kerberos KDC

## Overview
MIT Kerberos Key Distribution Center deployed as a Kubernetes workload for centralized Kerberos authentication across the platform. Used for Hadoop-style service authentication and integration with Ranger, HDFS, HBase, and other Kerberized services.

| Property | Value |
|---|---|
| Realm | `STARDATADBLABS.LOCAL` |
| Namespace | `kerberos` |
| Node | `master.local` (pinned) |
| KDC port | 88 TCP/UDP (ClusterIP) |
| kadmin port | 749 TCP (ClusterIP) |
| Image | `rockylinux:9` + `krb5-server` installed at startup |
| Secret | `kerberos-admin` |
| Manifest | `manifests/kerberos/kerberos-deployment.yaml` |

## Prerequisites
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh   # seeds kerberos-admin secret
```

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-kerberos.yaml`  
ArgoCD syncs `manifests/kerberos/` to the `kerberos` namespace automatically.

## Manual Deploy
```bash
kubectl apply -f manifests/kerberos/kerberos-deployment.yaml
kubectl rollout status deployment/kerberos-kdc -n kerberos
```

## Verify
```bash
# Check pod
kubectl get pods -n kerberos

# Exec into KDC and list principals
kubectl exec -n kerberos deploy/kerberos-kdc -- kadmin.local -q "listprincs"
```

## Add a Principal
```bash
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -randkey spark/worker1.local@STARDATADBLABS.LOCAL"

# Export keytab
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "ktadd -k /tmp/spark.keytab spark/worker1.local@STARDATADBLABS.LOCAL"
```

## krb5.conf for Client Nodes
```ini
[libdefaults]
    default_realm = STARDATADBLABS.LOCAL
    dns_lookup_realm = false
    dns_lookup_kdc = false
[realms]
    STARDATADBLABS.LOCAL = {
        kdc = kerberos-kdc.kerberos.svc.cluster.local
        admin_server = kerberos-kadmin.kerberos.svc.cluster.local
    }
[domain_realm]
    .cluster.local = STARDATADBLABS.LOCAL
```

## Secrets
| Key | Description |
|---|---|
| `master-password` | KDC database master key |
| `admin-password` | `admin/admin@STARDATADBLABS.LOCAL` principal password |
| `kadmin-password` | kadmin service password (same as admin in lab) |

OpenBao path: `secret/data/kerberos/credentials`

## Production Hardening
- Enable TLS between KDC and kadmin clients
- Use dedicated keytabs per service, rotate quarterly
- Restrict `kadm5.acl` to specific admin principals
- Mount KDC PVC on dedicated fast storage
