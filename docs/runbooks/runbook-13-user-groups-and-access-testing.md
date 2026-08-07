# Runbook 13 — User Groups, Role Binding, and Access Testing

> **RBAC Plane:** `http://192.168.1.50:30850` · **Realm:** `STARDATADBLABS.LOCAL`
> **KDC:** `kerberos-kdc.prod.svc.cluster.local:88`
> **Related runbooks:** [11 — Kerberos](runbook-11-kerberos-integration.md) · [12 — New User Setup](runbook-12-rbac-new-user-testing.md)

This runbook covers the full lifecycle of user groups on the platform:

| Section | Topic |
|---|---|
| [(a)](#a-adding-a-new-user-to-a-group) | Adding a new user to each group — Doris, Kafka, OpenSearch, Spark |
| [(b)](#b-admin-user-group-platform_admin---full-admin-on-all-services) | `platform_admin` group — full admin on all services |
| [(c)](#c-data_engineer-user-group---select--dml) | `data_engineer` group — SELECT + DML on all services |
| [(d)](#d-account_admin-user-group---top-level-governance) | `account_admin` group — account-level governance admin |
| [(e)](#e-adding-and-removing-privileges-for-a-user-group) | Adding and removing privileges for a user group (all services) |
| [(f)](#f-negative-test---user-in-rbac-group-but-no-kerberos-principal) | Negative test — RBAC binding without a KDC principal |
| [(g)](#g-creating-a-new-user-group-with-custom-privileges-and-adding-users) | Creating a brand-new user group with custom privileges — Doris, Kafka, OpenSearch, Spark |

---

## Prerequisites

```bash
# Set these in your shell for every section below
export RBAC_URL="http://192.168.1.50:30850"
export RBAC_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)
export DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
export OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d 2>/dev/null || echo "admin")
export REALM="STARDATADBLABS.LOCAL"
```

Verify the RBAC plane is healthy:
```bash
curl -s ${RBAC_URL}/health
# Expected: {"status":"ok","version":"1.0.0"}
```

### Permission ID reference

Every `POST/DELETE /api/v1/roles/{role_id}/permissions/{permission_id}` call uses these IDs:

| ID | Service | Permission | What it grants |
|---|---|---|---|
| 1 | doris | SELECT | Read rows |
| 2 | doris | INSERT | Insert rows (maps to LOAD_PRIV) |
| 3 | doris | UPDATE | Update rows (maps to LOAD_PRIV) |
| 4 | doris | DELETE | Delete rows (maps to LOAD_PRIV) |
| 5 | doris | CREATE | Create tables/databases |
| 6 | doris | DROP | Drop tables/databases |
| 7 | doris | ALTER | Alter table schema |
| 8 | doris | LOAD | Stream/routine load (maps to LOAD_PRIV) |
| 9 | doris | GRANT | Re-grant privileges to others |
| 10 | doris | ADMIN | Full Doris admin (ADMIN_PRIV on *.*.*) |
| 11 | kafka | PRODUCE | Write messages to topics |
| 12 | kafka | CONSUME | Read messages from topics |
| 13 | kafka | CREATE_TOPIC | Create Kafka topics |
| 14 | kafka | DELETE_TOPIC | Delete Kafka topics |
| 15 | kafka | DESCRIBE | Describe topics / cluster metadata |
| 16 | kafka | ADMIN | Full Kafka admin |
| 17 | opensearch | INDEX_READ | Search / get documents |
| 18 | opensearch | INDEX_WRITE | Index / write documents |
| 19 | opensearch | INDEX_ADMIN | Create/delete/manage indexes |
| 20 | opensearch | CLUSTER_READ | Read cluster metadata & health |
| 21 | opensearch | CLUSTER_ADMIN | Full cluster administration |
| 22 | spark | SUBMIT_JOB | Submit Spark jobs |
| 23 | spark | KILL_OWN_JOB | Kill own running jobs |
| 24 | spark | KILL_ANY_JOB | Kill any running job (operator) |
| 25 | spark | VIEW_UI | Access Spark Master Web UI |

```bash
# Fetch live permission IDs at any time
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/services/doris/permissions" | \
  python3 -c "import sys,json; [print(f'  {p[\"id\"]:3} {p[\"name\"]}') for p in json.load(sys.stdin)]"
```

---

## (a) Adding a New User to a Group

> This is the **complete onboarding checklist** for any new user on the platform.
> All four services require different setup steps. Complete them in the order shown.
>
> **Identity convention:** the same short username (e.g. `newuser`) is used across
> Kerberos, Doris, Kafka, OpenSearch, and Spark. The KDC principal carries the realm:
> `newuser@STARDATADBLABS.LOCAL`. All services strip the realm and use `newuser`.

```bash
# Set for the whole section
USERNAME="newuser"
PASSWORD="TempPass1!"
ROLE="data_engineer"    # or: platform_admin, account_admin, analyst, etl_writer …
DISPLAY_NAME="New User"
EMAIL="${USERNAME}@example.com"
```

### Step 1 — Kerberos: create the KDC principal

> Kerberos is the authentication gate for every service. Without a KDC principal
> the user cannot connect to anything, regardless of RBAC grants.

```bash
# Create principal with a temporary password
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -pw ${PASSWORD} ${USERNAME}@${REALM}"

# Verify
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc ${USERNAME}@${REALM}" 2>/dev/null | grep "Principal:"
```

#### Export a keytab (for batch/service-account access)

```bash
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "ktadd -k /tmp/${USERNAME}.keytab ${USERNAME}@${REALM}"

KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')
kubectl cp prod/${KDC_POD}:/tmp/${USERNAME}.keytab /tmp/${USERNAME}.keytab
klist -ekt /tmp/${USERNAME}.keytab

# Store as K8s secret (so Spark jobs can mount it)
kubectl create secret generic ${USERNAME}-keytab \
  --from-file=keytab=/tmp/${USERNAME}.keytab \
  -n prod --dry-run=client -o yaml | kubectl apply -f -

kubectl exec -n prod deploy/kerberos-kdc -- rm /tmp/${USERNAME}.keytab
rm /tmp/${USERNAME}.keytab
```

### Step 2 — Doris: create the SQL user

> Doris has no native Kerberos. The `krb-doris-guard` sidecar checks the KDC at
> connect time and proxies to Doris only if the principal exists. Doris itself uses
> SQL password auth — the username must match the KDC short name exactly.

```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "CREATE USER IF NOT EXISTS '${USERNAME}'@'%' IDENTIFIED BY '${PASSWORD}';"

# Verify
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SELECT user, host FROM mysql.user WHERE user='${USERNAME}';" 2>/dev/null
```

### Step 3 — RBAC plane: register user and bind role

> The RBAC plane is the single source of truth for *what* a user can do. Binding a role
> here defines the permission set that is synced to all four downstream services.

```bash
# Register user
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"display_name\":\"${DISPLAY_NAME}\",\"email\":\"${EMAIL}\"}" \
  "${RBAC_URL}/api/v1/users" | python3 -m json.tool

# Bind role
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"role_name\":\"${ROLE}\"}" \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings" | python3 -m json.tool
```

### Step 4 — Sync to all four services

```bash
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool

# Expected: all four services show "synced", errors: 0
```

### Step 5 — Store credentials in OpenBao

```bash
BAO_ADDR="http://192.168.1.50:30820"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('/root/openbao-init-keys.json'))['root_token'])")

curl -sf -X POST -H "X-Vault-Token: ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"data\":{\"principal\":\"${USERNAME}@${REALM}\",\"keytab_secret\":\"${USERNAME}-keytab\",\"created_by\":\"admin\"}}" \
  "${BAO_ADDR}/v1/secret/data/kerberos/users/${USERNAME}" && echo "Kerberos entry stored"

curl -sf -X POST -H "X-Vault-Token: ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"data\":{\"username\":\"${USERNAME}\",\"password\":\"${PASSWORD}\",\"service\":\"doris\",\"created_by\":\"admin\"}}" \
  "${BAO_ADDR}/v1/secret/data/doris/users/${USERNAME}" && echo "Doris credential stored"
```

### Step 6 — Verify access per service

#### Doris

```bash
# Connect as the new user through the krb-doris-guard
mysql -h 192.168.1.50 -P 30090 -u ${USERNAME} --password="${PASSWORD}" \
  -e "SHOW DATABASES;" 2>/dev/null

# Verify grants from admin side
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR '${USERNAME}'@'%';" 2>/dev/null
```

| Role | Expected Doris grants |
|---|---|
| `platform_admin` / `account_admin` | `GlobalPrivs: Admin_priv`, `CatalogPrivs: Grant_priv, Select_priv, Load_priv, Alter_priv, Create_priv, Drop_priv` |
| `data_engineer` | `CatalogPrivs: Select_priv, Load_priv` (no Admin_priv, no Grant_priv) |
| `analyst` | `CatalogPrivs: Select_priv` only |

#### Kafka

```bash
# KafkaUser CR created and Ready
kubectl get kafkauser ${USERNAME} -n prod

# Expected:
# NAME        CLUSTER         AUTHENTICATION   READY
# newuser     strimzi-kafka   scram-sha-512    True
```

> **Note:** Kafka ACL enforcement is disabled on this cluster
> (`allow.everyone.if.no.acl.found=true`). The KafkaUser CR provisions SCRAM-SHA-512
> credentials. The Kerberos GSSAPI listener on port 9093 is the actual authentication
> gate — no KDC principal means the SASL handshake is rejected before any ACL check.

#### OpenSearch

```bash
# Internal user was created by the sync
curl -sk -u admin:${OPENSEARCH_PASS} \
  "https://192.168.1.50:30920/_plugins/_security/api/internalusers/${USERNAME}" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print('exists:', '${USERNAME}' in d)"

# Role mappings applied by RBAC sync
curl -sk -u admin:${OPENSEARCH_PASS} \
  "https://192.168.1.50:30920/_plugins/_security/api/rolesmapping" | \
  python3 -c "
import sys,json
for role,m in json.load(sys.stdin).items():
    if '${USERNAME}' in m.get('users',[]):
        print(f'  mapped: {role}')
"
```

| Role | Expected OpenSearch role mappings |
|---|---|
| `platform_admin` / `account_admin` | `rbac_index_read_all`, `rbac_index_write_all`, `rbac_index_admin_all`, `rbac_cluster_read_all`, `rbac_cluster_admin_all` |
| `data_engineer` | `rbac_index_read_all`, `rbac_index_write_all`, `rbac_cluster_read_all` |
| `analyst` | `rbac_index_read_all`, `rbac_cluster_read_all` |

#### Spark

```bash
# User appears in the allowlist ConfigMap
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "
import sys,json
entry=json.load(sys.stdin).get('${USERNAME}')
print('Spark entry:', entry) if entry else print('WARNING: not in allowlist')
"
```

| Role | Expected Spark allowlist entry |
|---|---|
| `platform_admin` / `account_admin` | `{"can_submit": true, "can_kill_any": true, "view_ui": true}` |
| `data_engineer` | `{"can_submit": true, "can_kill_any": false, "view_ui": true}` |
| `analyst` | `{"can_submit": false, "can_kill_any": false, "view_ui": true}` |

### Quick-check: list all KDC principals

```bash
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "listprincs" 2>/dev/null | grep -v "^Authenticating"
```

---

## (b) Admin User Group (`platform_admin`) — Full Admin on All Services

> **Role name:** `platform_admin`
>
> | Service | Privileges |
> |---|---|
> | Doris | SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, LOAD, GRANT, **ADMIN_PRIV** |
> | Kafka | PRODUCE, CONSUME, CREATE_TOPIC, DELETE_TOPIC, DESCRIBE, ADMIN |
> | OpenSearch | INDEX_READ, INDEX_WRITE, INDEX_ADMIN, CLUSTER_READ, CLUSTER_ADMIN |
> | Spark | SUBMIT_JOB, KILL_OWN_JOB, KILL_ANY_JOB, VIEW_UI |

### Add a new user to the admin group

Follow [section (a)](#a-adding-a-new-user-to-a-group) with `ROLE="platform_admin"`.

```bash
# Quick one-liner for an existing KDC+Doris user
USERNAME="bob"

curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"role_name":"platform_admin"}' \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings" | python3 -m json.tool

curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

### Verify admin grants — all four services

#### Doris
```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR '${USERNAME}'@'%';" 2>/dev/null
# Expected GlobalPrivs: Admin_priv
# Expected CatalogPrivs: Grant_priv, Select_priv, Load_priv, Alter_priv, Create_priv, Drop_priv
```

#### Kafka
```bash
kubectl get kafkauser ${USERNAME} -n prod
# Expected: READY=True, authentication=scram-sha-512
```

#### OpenSearch
```bash
curl -sk -u admin:${OPENSEARCH_PASS} \
  "https://192.168.1.50:30920/_plugins/_security/api/rolesmapping" | \
  python3 -c "
import sys,json
roles=[r for r,m in json.load(sys.stdin).items() if '${USERNAME}' in m.get('users',[])]
print('Mapped roles:', roles)
"
# Expected: 5 rbac_* roles including rbac_cluster_admin_all and rbac_index_admin_all
```

#### Spark
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('${USERNAME}'))"
# Expected: {"can_kill_any": true, "can_submit": true, "view_ui": true}
```

---

## (c) `data_engineer` User Group — SELECT + DML

> **Role name:** `data_engineer`
>
> | Service | Privileges |
> |---|---|
> | Doris | SELECT, INSERT, UPDATE, DELETE, LOAD |
> | Kafka | PRODUCE, CONSUME, DESCRIBE |
> | OpenSearch | INDEX_READ, INDEX_WRITE, CLUSTER_READ |
> | Spark | SUBMIT_JOB, KILL_OWN_JOB, VIEW_UI |

### Add a new user to the data_engineer group

Follow [section (a)](#a-adding-a-new-user-to-a-group) with `ROLE="data_engineer"`.

```bash
USERNAME="carol"

curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"role_name":"data_engineer"}' \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings" | python3 -m json.tool

curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

### Verify DML grants — all four services

#### Doris
```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR '${USERNAME}'@'%';" 2>/dev/null
# Expected CatalogPrivs: Select_priv, Load_priv
# NOT expected: Admin_priv, Grant_priv, Create_priv, Drop_priv, Alter_priv
```

#### Kafka
```bash
kubectl get kafkauser ${USERNAME} -n prod
# Expected: READY=True, authentication=scram-sha-512
```

#### OpenSearch
```bash
curl -sk -u admin:${OPENSEARCH_PASS} \
  "https://192.168.1.50:30920/_plugins/_security/api/rolesmapping" | \
  python3 -c "
import sys,json
roles=[r for r,m in json.load(sys.stdin).items() if '${USERNAME}' in m.get('users',[])]
print('Mapped roles:', roles)
"
# Expected: rbac_index_read_all, rbac_index_write_all, rbac_cluster_read_all
# NOT expected: rbac_index_admin_all, rbac_cluster_admin_all
```

#### Spark
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('${USERNAME}'))"
# Expected: {"can_kill_any": false, "can_submit": true, "view_ui": true}
# NOT expected: can_kill_any=true
```

---

## (d) `account_admin` User Group — Top-Level Governance

> **Role name:** `account_admin`
> **Purpose:** account-level governance and user provisioning. Full admin on all services.
> Same permission set as `platform_admin` — named separately for governance boundary.

### Add a new user to the account_admin group

Follow [section (a)](#a-adding-a-new-user-to-a-group) with `ROLE="account_admin"`.

```bash
USERNAME="dave"

curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"role_name":"account_admin"}' \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings" | python3 -m json.tool

curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

### Verify admin grants — all four services

#### Doris
```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR '${USERNAME}'@'%';" 2>/dev/null
# Expected GlobalPrivs: Admin_priv
```

#### Kafka
```bash
kubectl get kafkauser ${USERNAME} -n prod
# Expected: READY=True
```

#### OpenSearch
```bash
curl -sk -u admin:${OPENSEARCH_PASS} \
  "https://192.168.1.50:30920/_plugins/_security/api/rolesmapping" | \
  python3 -c "
import sys,json
roles=[r for r,m in json.load(sys.stdin).items() if '${USERNAME}' in m.get('users',[])]
print('Mapped roles:', roles)
"
# Expected: all 5 rbac_* roles
```

#### Spark
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('${USERNAME}'))"
# Expected: {"can_kill_any": true, "can_submit": true, "view_ui": true}
```

### Role comparison table

| Role | Permission count | Intended for |
|---|---|---|
| `data_admin` | 25 (all) | Generic full admin (legacy seed) |
| `platform_admin` | 25 (all) | Platform infrastructure admin group |
| `account_admin` | 25 (all) | Account governance team |
| `data_engineer` | 14 (SELECT + DML only) | Day-to-day engineering team |
| `analyst` | 6 (SELECT + read only) | Read-only analysts |

---

## (e) Adding and Removing Privileges for a User Group

> Privileges are managed at the **role** level — changes apply to every user bound
> to that role. After modifying a role, re-sync all affected users to push the new
> state to the downstream services.
>
> **API endpoints:**
> - Add permission: `POST /api/v1/roles/{role_id}/permissions/{permission_id}`
> - Remove permission: `DELETE /api/v1/roles/{role_id}/permissions/{permission_id}`
> - See the [Permission ID reference](#permission-id-reference) table above for IDs.

### Get role IDs

```bash
# List all roles with their IDs
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles" | \
  python3 -c "
import sys,json
for r in json.load(sys.stdin):
    print(f'  id={r[\"id\"]:2}  name={r[\"name\"]}  ({len(r[\"permissions\"])} perms)')
"
```

Output (current roles):
```
  id= 1  name=analyst          ( 6 perms)
  id= 2  name=etl_writer        ( 7 perms)
  id= 3  name=spark_user        ( 3 perms)
  id= 4  name=data_admin        (25 perms)
  id= 5  name=kafka_consumer    ( 2 perms)
  id= 6  name=platform_admin    (25 perms)
  id= 7  name=data_engineer     (14 perms)
  id= 8  name=account_admin     (25 perms)
```

### Adding a privilege to a user group

**Pattern:** `POST /api/v1/roles/{role_id}/permissions/{permission_id}`

After adding, sync all users bound to the role so the new privilege is pushed to the
downstream services.

#### Example A — Give `data_engineer` the ability to CREATE tables in Doris

> Adding permission `CREATE` (id=5) to role `data_engineer` (id=7).

```bash
ROLE_ID=7          # data_engineer
PERMISSION_ID=5    # doris.CREATE

# 1. Add the permission to the role
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{}' \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/${PERMISSION_ID}" | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
print(f'Role: {r[\"name\"]} ({len(r[\"permissions\"])} perms)')
doris=[p['permission_name'] for p in r['permissions'] if p['service_name']=='doris']
print('Doris perms:', sorted(doris))
"

# 2. Sync all data_engineer users to push the change to Doris
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"service":"doris","dry_run":false}' \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

**Verify on Doris:**
```bash
# For each data_engineer user (e.g. carol)
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR 'carol'@'%';" 2>/dev/null
# Now expected: CatalogPrivs includes Create_priv (was absent before)
```

---

#### Example B — Give `data_engineer` the ability to CREATE_TOPIC in Kafka

> Adding permission `CREATE_TOPIC` (id=13) to role `data_engineer` (id=7).

```bash
ROLE_ID=7
PERMISSION_ID=13   # kafka.CREATE_TOPIC

# 1. Add permission
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{}' \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/${PERMISSION_ID}" | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
kafka=[p['permission_name'] for p in r['permissions'] if p['service_name']=='kafka']
print('Kafka perms:', sorted(kafka))
"

# 2. Sync data_engineer users to Kafka only
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"service":"kafka","dry_run":false}' \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

**Verify on Kafka:**
```bash
# Kafka ACL enforcement is disabled on this cluster (allow.everyone.if.no.acl.found=true)
# The KafkaUser CR is updated — verify the sync recorded the state change
kubectl get kafkauser carol -n prod -o jsonpath='{.status.conditions[0].type}'
# Expected: Ready
```

---

#### Example C — Give `data_engineer` INDEX_ADMIN on OpenSearch

> Adding permission `INDEX_ADMIN` (id=19) to role `data_engineer` (id=7).

```bash
ROLE_ID=7
PERMISSION_ID=19   # opensearch.INDEX_ADMIN

# 1. Add permission
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{}' \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/${PERMISSION_ID}" | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
os=[p['permission_name'] for p in r['permissions'] if p['service_name']=='opensearch']
print('OpenSearch perms:', sorted(os))
"

# 2. Sync to OpenSearch
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"service":"opensearch","dry_run":false}' \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

**Verify on OpenSearch:**
```bash
curl -sk -u admin:${OPENSEARCH_PASS} \
  "https://192.168.1.50:30920/_plugins/_security/api/rolesmapping" | \
  python3 -c "
import sys,json
for role,m in json.load(sys.stdin).items():
    if 'carol' in m.get('users',[]):
        print(f'  mapped: {role}')
"
# Now expected: rbac_index_admin_all is in the list (was absent before)
```

---

#### Example D — Give `data_engineer` KILL_ANY_JOB on Spark

> Adding permission `KILL_ANY_JOB` (id=24) to role `data_engineer` (id=7).

```bash
ROLE_ID=7
PERMISSION_ID=24   # spark.KILL_ANY_JOB

# 1. Add permission
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{}' \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/${PERMISSION_ID}" | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
sp=[p['permission_name'] for p in r['permissions'] if p['service_name']=='spark']
print('Spark perms:', sorted(sp))
"

# 2. Sync to Spark
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"service":"spark","dry_run":false}' \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

**Verify on Spark:**
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('carol'))"
# Now expected: {"can_kill_any": true, "can_submit": true, "view_ui": true}
# (can_kill_any was false before)
```

---

### Removing a privilege from a user group

**Pattern:** `DELETE /api/v1/roles/{role_id}/permissions/{permission_id}`

After removing, sync all affected users to revoke the privilege in the downstream services.

#### Example E — Remove CREATE from `data_engineer` in Doris

> Removing permission `CREATE` (id=5) from role `data_engineer` (id=7).

```bash
ROLE_ID=7
PERMISSION_ID=5    # doris.CREATE

# 1. Remove the permission from the role
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/${PERMISSION_ID}" | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
doris=[p['permission_name'] for p in r['permissions'] if p['service_name']=='doris']
print(f'Doris perms after removal: {sorted(doris)}')
"

# 2. Sync — revokes in Doris by doing REVOKE ALL then re-granting remaining privs
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"service":"doris","dry_run":false}' \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

**Verify on Doris — CREATE_PRIV is gone:**
```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR 'carol'@'%';" 2>/dev/null
# CatalogPrivs should no longer contain Create_priv
```

---

#### Example F — Remove INDEX_ADMIN from `data_engineer` in OpenSearch

> Removing permission `INDEX_ADMIN` (id=19) from role `data_engineer` (id=7).

```bash
ROLE_ID=7
PERMISSION_ID=19   # opensearch.INDEX_ADMIN

# 1. Remove
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/${PERMISSION_ID}" | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
os=[p['permission_name'] for p in r['permissions'] if p['service_name']=='opensearch']
print(f'OpenSearch perms after removal: {sorted(os)}')
"

# 2. Sync to OpenSearch
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"service":"opensearch","dry_run":false}' \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

**Verify on OpenSearch — INDEX_ADMIN mapping removed:**
```bash
curl -sk -u admin:${OPENSEARCH_PASS} \
  "https://192.168.1.50:30920/_plugins/_security/api/rolesmapping" | \
  python3 -c "
import sys,json
for role,m in json.load(sys.stdin).items():
    if 'carol' in m.get('users',[]):
        print(f'  mapped: {role}')
"
# rbac_index_admin_all should no longer appear
```

---

#### Example G — Remove KILL_ANY_JOB from `data_engineer` in Spark

> Removing permission `KILL_ANY_JOB` (id=24) from role `data_engineer` (id=7).

```bash
ROLE_ID=7
PERMISSION_ID=24   # spark.KILL_ANY_JOB

# 1. Remove
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/${PERMISSION_ID}"

# 2. Sync to Spark
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"service":"spark","dry_run":false}' \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

**Verify on Spark:**
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('carol'))"
# Expected: {"can_kill_any": false, ...}  (reverted)
```

---

#### Example H — Remove CREATE_TOPIC from `data_engineer` in Kafka

> Removing permission `CREATE_TOPIC` (id=13) from role `data_engineer` (id=7).

```bash
ROLE_ID=7
PERMISSION_ID=13   # kafka.CREATE_TOPIC

# 1. Remove
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/${PERMISSION_ID}"

# 2. Sync to Kafka
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"service":"kafka","dry_run":false}' \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

**Verify on Kafka:**
```bash
kubectl get kafkauser carol -n prod
# KafkaUser should be Ready — the sync cleared and rewrote the SCRAM credential
```

---

### Verifying the final role state after privilege changes

After any add or remove operation, inspect the role to confirm the full permission set:

```bash
ROLE_NAME="data_engineer"

curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles" | \
  python3 -c "
import sys,json
for r in json.load(sys.stdin):
    if r['name']=='${ROLE_NAME}':
        print(f'Role: {r[\"display_name\"]} (id={r[\"id\"]})')
        svc={}
        for p in r['permissions']:
            svc.setdefault(p['service_name'],[]).append(p['permission_name'])
        for s,perms in sorted(svc.items()):
            print(f'  {s}: {sorted(perms)}')
"
```

### Dry-run before applying privilege changes

Use `dry_run=true` to preview which users would be affected and what would change
before committing:

```bash
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"dry_run":true}' \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

### Force re-sync all users to all services

If you need to ensure every user's state is in sync (e.g. after bulk role edits):

```bash
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"dry_run":false}' \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

---

## (f) Negative Test — User in RBAC Group But No Kerberos Principal

> **Expected result:** The RBAC sync will succeed — the RBAC plane has no KDC awareness
> and happily provisions Doris grants, KafkaUser CRs, OpenSearch users, and Spark allowlist
> entries. However, the user will be **blocked at the Kerberos authentication gate** on
> every single service.
>
> This is the fundamental security property of the platform: **RBAC grants are necessary
> but not sufficient. A valid KDC principal is unconditionally required.**

### Setup: create RBAC-only user (no KDC principal)

```bash
USERNAME="ghost"

# 1. Doris SQL user (guard will block, but SQL user must exist for RBAC sync to succeed)
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "CREATE USER IF NOT EXISTS '${USERNAME}'@'%' IDENTIFIED BY 'Ghost1Pass!';"

# 2. Register in RBAC plane and bind a role
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"display_name\":\"Ghost User (negative test)\",\"email\":\"ghost@test.invalid\"}" \
  "${RBAC_URL}/api/v1/users"

curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"role_name":"analyst"}' \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings"

# 3. Sync — this SUCCEEDS (plane doesn't consult the KDC)
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
# Expected: all four services show "synced", errors: 0
```

### Confirm: `ghost` is provisioned in all downstream services

```bash
# Doris: SQL user exists and has grants
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR 'ghost'@'%';" 2>/dev/null

# Kafka: KafkaUser CR exists and is Ready
kubectl get kafkauser ghost -n prod

# OpenSearch: internal user created
curl -sk -u admin:${OPENSEARCH_PASS} \
  "https://192.168.1.50:30920/_plugins/_security/api/internalusers/ghost" | \
  python3 -c "import sys,json; print('OS user exists:', 'ghost' in json.load(sys.stdin))"

# Spark: appears in allowlist
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print('in allowlist:', 'ghost' in json.load(sys.stdin))"
```

All four checks should return positive results. Provisioning succeeded.

### Test: attempt connection — all services must BLOCK

#### Doris

```bash
# The guard (port 19030) intercepts the MySQL handshake, extracts the username,
# and runs: kinit -V -n ghost@STARDATADBLABS.LOCAL </dev/null
# Since ghost has no KDC principal, kinit returns exit code 1 → guard sends MySQL ERR

mysql -h 192.168.1.50 -P 30090 -u ghost --password="Ghost1Pass!" \
  -e "SHOW DATABASES;" 2>&1

# Expected error:
# ERROR 1045 (28000): Access denied: principal ghost@STARDATADBLABS.LOCAL not found in KDC
```

#### Kafka (GSSAPI listener, port 9093)

```bash
# Confirm the KDC principal genuinely does not exist
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc ghost@${REALM}" 2>&1 | grep -i "does not exist"
# Expected: "Principal does not exist"

# Without a TGT (kinit fails) → SASL/GSSAPI handshake on port 9093 is rejected
# The SCRAM listener (port 9092) is reserved for operators — not accessible externally
```

#### OpenSearch (SPNEGO)

```bash
# SPNEGO requires a valid Kerberos TGT for the client.
# Without a KDC principal the client cannot obtain a TGT → negotiate fails → HTTP 401
curl -sk --negotiate -u : \
  "https://192.168.1.50:30920/_cluster/health" -o /dev/null -w "%{http_code}\n"
# Expected: 401
```

#### Spark (krb-spark-guard auth endpoint)

```bash
# The guard's POST /auth API calls kinit against the provided keytab.
# ghost has no keytab and no KDC principal → kinit fails → HTTP 401
curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"ghost","keytab_b64":""}' \
  http://192.168.1.50:30778/auth
# Expected: 401
```

### Summary table

| Service | RBAC provisioned | KDC principal | Connection |
|---|---|---|---|
| Doris | ✅ SQL user + grants exist | ❌ no principal | ❌ **BLOCKED** by `krb-doris-guard` — `kinit` returns exit 1 |
| Kafka | ✅ KafkaUser CR Ready | ❌ no principal | ❌ **BLOCKED** — cannot obtain TGT, GSSAPI rejected |
| OpenSearch | ✅ internal user + role mappings | ❌ no principal | ❌ **BLOCKED** — SPNEGO returns HTTP 401 |
| Spark | ✅ in allowlist | ❌ no principal | ❌ **BLOCKED** — guard `kinit` call returns 401 |

> **Security conclusion:**
> Kerberos is the **hard authentication gate** before every service. The RBAC plane
> controls *authorisation scope* (what a verified user can do) but cannot bypass the
> KDC check. Deleting a user's KDC principal is therefore an **immediate hard revocation**
> across all services — even if their RBAC role bindings remain intact.

### Cleanup: remove ghost from all services

```bash
# RBAC plane (also triggers downstream cleanup on next sync or explicit delete)
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/ghost"

# Doris SQL user
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "DROP USER IF EXISTS 'ghost'@'%';" 2>/dev/null

# Kafka KafkaUser CR
kubectl delete kafkauser ghost -n prod --ignore-not-found

# Verify removal
kubectl get kafkauser ghost -n prod 2>&1 | grep -i "not found"
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SELECT user FROM mysql.user WHERE user='ghost';" 2>/dev/null
```

---

## (g) Creating a New User Group with Custom Privileges and Adding Users

> This section shows the **complete workflow** for creating a brand-new user group
> (role) from scratch: defining it, assigning custom privileges for each service, then
> onboarding a new user into it and verifying access on every service.
>
> Use this whenever the built-in roles (`analyst`, `data_engineer`, `platform_admin`, etc.)
> don't exactly match a team's access requirements.

### Overview of steps

| Step | Action |
|---|---|
| 1 | Define the new role (name + description) |
| 2 | Add the desired permissions per service |
| 3 | Verify the role definition |
| 4 | Create the KDC principal for the new user |
| 5 | Create the Doris SQL user |
| 6 | Register the user in the RBAC plane and bind the new role |
| 7 | Sync to all four services |
| 8 | Verify access on Doris, Kafka, OpenSearch, Spark |
| 9 | Optional: add more users to the same group |

---

### Worked example — `reporting_analyst` group

> **Scenario:** The reporting team needs read-only access to Doris and OpenSearch,
> consume-only access to Kafka, and UI-only access to Spark. No write or admin rights.
>
> **Privileges chosen:**
> | Service | Permissions |
> |---|---|
> | Doris | SELECT (id=1) |
> | Kafka | CONSUME (id=12), DESCRIBE (id=15) |
> | OpenSearch | INDEX_READ (id=17), CLUSTER_READ (id=20) |
> | Spark | VIEW_UI (id=25) |

```bash
# Set once for the whole workflow
export RBAC_URL="http://192.168.1.50:30850"
export RBAC_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)
export DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
export REALM="STARDATADBLABS.LOCAL"

# Role and user variables
NEW_ROLE="reporting_analyst"
NEW_ROLE_DISPLAY="Reporting Analyst"
NEW_ROLE_DESC="Read-only access to Doris and OpenSearch; consume-only on Kafka; Spark UI only"
```

---

### Step 1 — Create the new role

> Role names must be lowercase alphanumeric, underscores and hyphens only (`^[a-z0-9_\-]+$`).

```bash
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\":         \"${NEW_ROLE}\",
    \"display_name\": \"${NEW_ROLE_DISPLAY}\",
    \"description\":  \"${NEW_ROLE_DESC}\"
  }" \
  "${RBAC_URL}/api/v1/roles" | python3 -m json.tool

# Save the role ID from the response
ROLE_ID=$(curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles" | \
  python3 -c "
import sys,json
for r in json.load(sys.stdin):
    if r['name']=='${NEW_ROLE}': print(r['id'])
")
echo "New role id: ${ROLE_ID}"
```

Expected response:
```json
{
  "id": 9,
  "name": "reporting_analyst",
  "display_name": "Reporting Analyst",
  "description": "Read-only access ...",
  "created_at": "2026-08-07T00:12:44.994273Z",
  "permissions": []
}
```

---

### Step 2 — Add permissions for each service

> Each permission is added with `POST /api/v1/roles/{role_id}/permissions/{permission_id}`.
> The body must be `{}` (empty JSON object, not empty body).
> See the [Permission ID reference](#permission-id-reference) table for all 25 IDs.

#### Doris — SELECT only

```bash
# doris.SELECT = permission id 1
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" -d '{}' \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/1" | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(f'  doris perms: {[p[\"permission_name\"] for p in r[\"permissions\"] if p[\"service_name\"]==\"doris\"]}')"
```

#### Kafka — CONSUME + DESCRIBE

```bash
# kafka.CONSUME = 12, kafka.DESCRIBE = 15
for pid in 12 15; do
  curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
    -H "Content-Type: application/json" -d '{}' \
    "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/${pid}" > /dev/null \
    && echo "  added kafka perm ${pid}"
done
```

#### OpenSearch — INDEX_READ + CLUSTER_READ

```bash
# opensearch.INDEX_READ = 17, opensearch.CLUSTER_READ = 20
for pid in 17 20; do
  curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
    -H "Content-Type: application/json" -d '{}' \
    "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/${pid}" > /dev/null \
    && echo "  added opensearch perm ${pid}"
done
```

#### Spark — VIEW_UI only

```bash
# spark.VIEW_UI = 25
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" -d '{}' \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/25" > /dev/null \
  && echo "  added spark perm 25"
```

---

### Step 3 — Verify the complete role definition

```bash
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}" | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
print(f'Role: {r[\"name\"]}  id={r[\"id\"]}  ({len(r[\"permissions\"])} permissions)')
svc={}
for p in r['permissions']:
    svc.setdefault(p['service_name'],[]).append(p['permission_name'])
for s,perms in sorted(svc.items()):
    print(f'  {s:12}: {sorted(perms)}')
"
```

Expected output:
```
Role: reporting_analyst  id=9  (6 permissions)
  doris       : ['SELECT']
  kafka       : ['CONSUME', 'DESCRIBE']
  opensearch  : ['CLUSTER_READ', 'INDEX_READ']
  spark       : ['VIEW_UI']
```

---

### Step 4 — Create the KDC principal for the new user

> Each user must have a Kerberos principal **before** connecting to any service.
> See [section (a)](#a-adding-a-new-user-to-a-group) for the full onboarding checklist.

```bash
USERNAME="eve"
PASSWORD="TempPass1!"

# Create principal
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -pw ${PASSWORD} ${USERNAME}@${REALM}"

# Verify
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc ${USERNAME}@${REALM}" 2>/dev/null | grep "Principal:"
```

Export a keytab for batch / Spark job use:
```bash
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "ktadd -k /tmp/${USERNAME}.keytab ${USERNAME}@${REALM}"

KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')
kubectl cp prod/${KDC_POD}:/tmp/${USERNAME}.keytab /tmp/${USERNAME}.keytab
kubectl create secret generic ${USERNAME}-keytab \
  --from-file=keytab=/tmp/${USERNAME}.keytab -n prod \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl exec -n prod deploy/kerberos-kdc -- rm /tmp/${USERNAME}.keytab
rm /tmp/${USERNAME}.keytab
```

---

### Step 5 — Create the Doris SQL user

```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "CREATE USER IF NOT EXISTS '${USERNAME}'@'%' IDENTIFIED BY '${PASSWORD}';"

# Verify
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SELECT user, host FROM mysql.user WHERE user='${USERNAME}';" 2>/dev/null
```

---

### Step 6 — Register the user in RBAC and bind the new role

```bash
# Register the user
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\":     \"${USERNAME}\",
    \"display_name\": \"Eve Martinez\",
    \"email\":        \"${USERNAME}@example.com\"
  }" \
  "${RBAC_URL}/api/v1/users" | python3 -m json.tool

# Bind the new role
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"role_name\":\"${NEW_ROLE}\"}" \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings" | python3 -m json.tool
```

Expected binding response:
```json
{
  "id": 5,
  "username": "eve",
  "role_name": "reporting_analyst",
  "service_name": null,
  "granted_by": "master",
  "granted_at": "2026-08-07T00:...",
  "expires_at": null
}
```

---

### Step 7 — Sync to all four services

```bash
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

Expected output — all four services synced, zero errors:
```json
{
  "results": [
    {"username": "eve", "service": "doris",      "status": "synced", "detail": "1 permissions applied"},
    {"username": "eve", "service": "kafka",      "status": "synced", "detail": "2 permissions applied"},
    {"username": "eve", "service": "opensearch", "status": "synced", "detail": "2 permissions applied"},
    {"username": "eve", "service": "spark",      "status": "synced", "detail": "1 permissions applied"}
  ],
  "errors": 0
}
```

---

### Step 8 — Verify access on all four services

#### Doris — SELECT_PRIV only, no write/admin

```bash
# Admin-side verification
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR '${USERNAME}'@'%';" 2>/dev/null

# Expected: CatalogPrivs contains Select_priv only
# NOT expected: Load_priv, Create_priv, Drop_priv, Admin_priv, Grant_priv

# User-side test — connect and read (krb-doris-guard checks KDC first)
mysql -h 192.168.1.50 -P 30090 -u ${USERNAME} --password="${PASSWORD}" \
  -e "SHOW DATABASES;" 2>/dev/null

# Confirm no write access — this MUST fail
mysql -h 192.168.1.50 -P 30090 -u ${USERNAME} --password="${PASSWORD}" \
  -e "CREATE DATABASE rbac_test_forbidden;" 2>&1 | grep -i "denied\|error"
# Expected: Access denied
```

#### Kafka — KafkaUser CR with SCRAM credentials

```bash
kubectl get kafkauser ${USERNAME} -n prod

# Expected:
# NAME   CLUSTER         AUTHENTICATION   AUTHORIZATION   READY
# eve    strimzi-kafka   scram-sha-512                    True
```

> **Note:** This cluster has `allow.everyone.if.no.acl.found=true` — Kafka authorization is
> not enforced at the broker level. The KafkaUser CR provisions SCRAM-SHA-512 credentials.
> Kerberos GSSAPI on port 9093 is the authentication gate; all topic access is open once
> authenticated.

#### OpenSearch — internal user + rbac role mappings

```bash
# Verify the internal OpenSearch user was created by the sync
# (requires admin access — typically run from master node or a pod with a valid TGT)
#
# From a node with kinit access to admin@STARDATADBLABS.LOCAL:
#   kinit admin/admin@STARDATADBLABS.LOCAL
#   curl --negotiate -u : \
#     "https://192.168.1.50:30920/_plugins/_security/api/internalusers/${USERNAME}"
#
# From inside the rbac-plane pod (uses Basic auth with OPENSEARCH_ADMIN_PASSWORD):
kubectl exec -n prod deploy/rbac-plane -- python3 -c "
import urllib.request, json, ssl, base64, os
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
host = os.environ['OPENSEARCH_HOST']
port = os.environ['OPENSEARCH_PORT']
user = os.environ['OPENSEARCH_ADMIN_USER']
pw   = os.environ['OPENSEARCH_ADMIN_PASSWORD']
cred = base64.b64encode(f'{user}:{pw}'.encode()).decode()
req  = urllib.request.Request(
  f'https://{host}:{port}/_plugins/_security/api/rolesmapping',
  headers={'Authorization': f'Basic {cred}'}
)
data = json.loads(urllib.request.urlopen(req, context=ctx).read())
mapped = [r for r,m in data.items() if '${USERNAME}' in m.get('users',[])]
print('OpenSearch roles for ${USERNAME}:', mapped)
" 2>&1

# Expected output:
# OpenSearch roles for eve: ['rbac_index_read_all', 'rbac_cluster_read_all']
```

> **Note on OpenSearch auth:** This cluster has `kerberos_auth_domain.http_enabled: true`,
> which means HTTP clients require a Kerberos TGT (SPNEGO). The RBAC plane adapter uses
> Basic auth with the admin password from `rbac-plane-credentials`; if Kerberos is the
> only active auth domain this call will return 401. When that happens, verify using the
> Kerberos-authenticated path shown above (kinit + `--negotiate`).

#### Spark — allowlist ConfigMap

```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('${USERNAME}'))"

# Expected for reporting_analyst (VIEW_UI only, no SUBMIT_JOB):
# {'can_kill_any': False, 'can_submit': False, 'view_ui': True}
```

---

### Step 9 — Adding more users to the same group

Once the role exists, adding further users is a 4-command operation:

```bash
# Replace NEWUSER_* with the new user's details
NEWUSER="frank"
NEWUSER_PASS="TempPass1!"

# 1. KDC principal
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -pw ${NEWUSER_PASS} ${NEWUSER}@${REALM}"

# 2. Doris SQL user
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "CREATE USER IF NOT EXISTS '${NEWUSER}'@'%' IDENTIFIED BY '${NEWUSER_PASS}';"

# 3. RBAC register + bind same role
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${NEWUSER}\",\"display_name\":\"Frank Lin\",\"email\":\"${NEWUSER}@example.com\"}" \
  "${RBAC_URL}/api/v1/users"

curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"role_name\":\"${NEW_ROLE}\"}" \
  "${RBAC_URL}/api/v1/users/${NEWUSER}/bindings"

# 4. Sync
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${NEWUSER}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

All users bound to `reporting_analyst` automatically inherit any future permission
changes made to the role — just re-sync after modifying the role (see [section (e)](#e-adding-and-removing-privileges-for-a-user-group)).

---

### Removing the user group (role)

> Deleting a role **does not** automatically revoke downstream grants. You must:
> 1. Remove all user bindings to the role, then re-sync each user.
> 2. Delete the role.

```bash
# Step 1 — find all users bound to this role
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" "${RBAC_URL}/api/v1/users" | \
  python3 -c "import sys,json; [print(u['username']) for u in json.load(sys.stdin)]" | \
  while read u; do
    curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
      "${RBAC_URL}/api/v1/users/${u}/bindings" | \
      python3 -c "
import sys,json
for b in json.load(sys.stdin):
    if b['role_name']=='${NEW_ROLE}':
        print(f'${u} binding_id={b[\"id\"]}')
"
  done

# Step 2 — for each user: delete binding, then re-sync to revoke downstream grants
AFFECTED_USER="eve"
BINDING_ID=5   # from step 1 output above

curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/${AFFECTED_USER}/bindings/${BINDING_ID}"

curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${AFFECTED_USER}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
# All services should now show "synced" with 0 permissions applied (revoked)

# Step 3 — delete the role
ROLE_ID=9  # from Step 1 above
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}" | python3 -m json.tool
# Expected: {"ok": true, "message": "Role 'reporting_analyst' deleted"}
```

---

## Role Reference

| Role | Doris | Kafka | OpenSearch | Spark | Use case |
|---|---|---|---|---|---|
| `analyst` | SELECT | CONSUME, DESCRIBE | INDEX_READ, CLUSTER_READ | VIEW_UI | Read-only analyst |
| `etl_writer` | SELECT, INSERT, UPDATE, LOAD | PRODUCE, CONSUME, DESCRIBE | — | — | ETL pipeline writer |
| `spark_user` | — | — | — | SUBMIT_JOB, KILL_OWN_JOB, VIEW_UI | Spark job submitter |
| `kafka_consumer` | — | CONSUME, DESCRIBE | — | — | Kafka consumer only |
| **`data_engineer`** | SELECT, INSERT, UPDATE, DELETE, LOAD | PRODUCE, CONSUME, DESCRIBE | INDEX_READ, INDEX_WRITE, CLUSTER_READ | SUBMIT_JOB, KILL_OWN_JOB, VIEW_UI | Data engineering team |
| **`platform_admin`** | All (incl. ADMIN_PRIV) | All | All | All | Platform infra admin |
| **`account_admin`** | All (incl. ADMIN_PRIV) | All | All | All | Account governance admin |
| `data_admin` | All | All | All | All | Generic full admin (legacy seed) |

---

## Quick-Reference Commands

```bash
# List all roles with permission counts
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles" | python3 -c \
  "import sys,json; [print(f'id={r[\"id\"]:2} {r[\"name\"]:20} ({len(r[\"permissions\"])} perms)') for r in json.load(sys.stdin)]"

# Inspect a specific role's permissions
ROLE_NAME="data_engineer"
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" "${RBAC_URL}/api/v1/roles" | \
  python3 -c "
import sys,json
for r in json.load(sys.stdin):
    if r['name']=='${ROLE_NAME}':
        svc={}
        for p in r['permissions']:
            svc.setdefault(p['service_name'],[]).append(p['permission_name'])
        for s,perms in sorted(svc.items()):
            print(f'  {s}: {sorted(perms)}')
"

# List all users and their bound roles
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" "${RBAC_URL}/api/v1/users" | \
  python3 -c "import sys,json; [print(u['username']) for u in json.load(sys.stdin)]" | \
  while read u; do
    roles=$(curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
      "${RBAC_URL}/api/v1/users/${u}/roles" | \
      python3 -c "import sys,json; print(', '.join(r['name'] for r in json.load(sys.stdin)))")
    echo "${u}: ${roles}"
  done

# Full platform sync — all users to all services
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"dry_run":false}' "${RBAC_URL}/api/v1/sync" | python3 -m json.tool

# Sync a single user to all services
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"username":"carol","dry_run":false}' "${RBAC_URL}/api/v1/sync" | python3 -m json.tool

# Sync all users to a single service
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"service":"doris","dry_run":false}' "${RBAC_URL}/api/v1/sync" | python3 -m json.tool

# List KDC principals
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "listprincs" 2>/dev/null | grep -v "^Authenticating"

# Remove a user from a group (delete binding by ID)
BINDING_ID=3  # get from GET /api/v1/users/{username}/bindings
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/carol/bindings/${BINDING_ID}"
# Then re-sync to revoke downstream
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"username":"carol","dry_run":false}' "${RBAC_URL}/api/v1/sync"
```
