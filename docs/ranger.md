# Apache Ranger

## Overview
Apache Ranger 2.7.0 — centralized security framework for fine-grained RBAC, data masking, row-level filtering, and audit logging across the data platform.

| Property | Value |
|---|---|
| Namespace | `prod` |
| Admin UI | `http://192.168.1.50:30680` |
| Credentials | `admin` / (from `ranger-db-credentials` secret, key `admin-password`) |
| Image | `192.168.1.50:30500/apache-ranger:2.7.0` |
| Depends on | PostgreSQL `prod` namespace, database `ranger` |
| Secret | `ranger-db-credentials` |
| Manifest | [`manifests/ranger/ranger-deployment.yaml`](../manifests/ranger/ranger-deployment.yaml) |

## Prerequisites
1. PostgreSQL deployed with `ranger` database created
2. Build and push the Ranger image (includes PostgreSQL JDBC driver):
```bash
podman build -t 192.168.1.50:30500/apache-ranger:2.7.0 docker/ranger/
podman push 192.168.1.50:30500/apache-ranger:2.7.0
```
3. Seed secrets:
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-ranger.yaml`
Syncs `manifests/ranger/` to the `prod` namespace.

## Manual Deploy
```bash
kubectl apply -f manifests/ranger/ranger-deployment.yaml
kubectl rollout status deployment/ranger-admin -n prod
```

## Verify
```bash
kubectl get pods -n prod -l app=ranger-admin
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
curl -s -u "admin:${RANGER_PASS}" \
  http://192.168.1.50:30680/service/public/v2/api/service | python3 -m json.tool
```

## Secret Keys
| Key | Description |
|---|---|
| `db-user` | PostgreSQL user (`ranger`) |
| `db-password` | PostgreSQL password for ranger user |
| `db-root-password` | PostgreSQL root password (for install script) |
| `admin-password` | Ranger admin UI password |
| `tagsync-password` | Tag-sync service password |
| `usersync-password` | User-sync service password |
| `keyadmin-password` | Key admin password |

OpenBao path: `secret/data/ranger/credentials`

---

## Registered Services

| Service Name | Type | Description | Registered By |
|---|---|---|---|
| `doris` | hive | Apache Doris 4.0.7 RBAC | REST API (see below) |
| `doris_service` | hive | Legacy Doris hive-type service | Manual UI |
| `kafka` | kafka | Strimzi Kafka 4.2.0 broker | Manual UI (see [`docs/kafka.md`](kafka.md)) |
| `opensearch` | elasticsearch | OpenSearch 3.7.0 | Manual UI (see [`docs/opensearch.md`](opensearch.md)) |

### Re-registering the `doris` service

The `doris` service must exist in Ranger for the Doris FE Ranger plugin to
download policies. If it is deleted, re-register it with:

```bash
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

curl -s -u "admin:${RANGER_PASS}" \
  -X POST http://192.168.1.50:30680/service/public/v2/api/service \
  -H "Content-Type: application/json" \
  -d "{
    \"type\": \"hive\",
    \"name\": \"doris\",
    \"displayName\": \"Apache Doris\",
    \"description\": \"Apache Doris 4.0.7 RBAC via Ranger Hive plugin\",
    \"isEnabled\": true,
    \"configs\": {
      \"username\": \"root\",
      \"password\": \"${DORIS_PASS}\",
      \"jdbc.driverClassName\": \"com.mysql.jdbc.Driver\",
      \"jdbc.url\": \"jdbc:mysql://doris-fe.prod.svc.cluster.local:9030\",
      \"policy.download.auth.users\": \"root\",
      \"tag.download.auth.users\": \"root\",
      \"ranger.plugin.policy.pollIntervalMs\": \"30000\"
    }
  }"
```

---

## Configuring Policies

1. Open `http://192.168.1.50:30680` → login as `admin`
2. **Access Manager → Service Manager** → select the service
3. **Add New Policy** → set resources (database / table / column), users/groups, permissions
4. **Save** — policies are pushed to all plugins within 30 seconds

### Policy Types
| Type | Use Case |
|---|---|
| **Access** | Allow/deny SELECT, INSERT, UPDATE, DROP etc. |
| **Masking** | Mask/redact column values for specific users |
| **Row Filter** | Limit rows returned based on user/group |

---

## Ranger + Kerberos Integration
To protect a Kerberized service, configure the service plugin with:
- `policy.download.auth.users` = service principal
- `ranger.plugin.audit.destination.hdfs.config.conf.dir` = `/etc/krb5.conf`
- Enable SPNEGO for Ranger Admin UI (see Production Hardening below)

---

## Production Hardening
- Enable HTTPS on Ranger Admin (configure `ranger-admin-site.xml`)
- Integrate with Kerberos for admin authentication (SPNEGO)
- Enable Solr-based audit log storage
- Set up UserSync with LDAP/AD
