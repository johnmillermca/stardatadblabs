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
| [(h)](#h-doris-column-level-privileges) | Doris column-level SELECT — syntax, worked example, limitations, revoke |
| [(i)](#i-polaris-catalog-grants-for-write_iceberg--admin_catalog) | Polaris catalog grants for `WRITE_ICEBERG` / `ADMIN_CATALOG` |
| [(j)](#j-sample-data-setup-and-end-to-end-permission-testing) | **Sample data setup** — create test tables/topics/indexes and run permission tests |
| [(k)](#k-bulk-binding---adding-multiple-users-to-a-role-in-one-call) | **Bulk binding** — add multiple users to a role in one API/CLI call |
| [(l)](#l-removing-a-user-from-a-role-or-from-the-platform) | **Removing a user** — unbind from one role, one service, or full offboard |
| [(m)](#m-inspecting-users-roles-and-service-scope) | **Inspecting users** — who is in which role, which services they can reach |

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
  -o jsonpath='{.data.opensearch-password}' | base64 -d 2>/dev/null || echo "admin")
export REALM="STARDATADBLABS.LOCAL"
```

Verify the RBAC plane is healthy:
```bash
curl -s ${RBAC_URL}/health
# Expected: {"status":"ok","version":"1.0.0"}
```

### Permission ID reference

Every `POST/DELETE /api/v1/roles/{role_id}/permissions/{permission_id}` call uses these IDs.

#### Doris (12 permissions)

| ID | Permission | Doris SQL privilege | Scope | Notes |
|---|---|---|---|---|
| 1 | SELECT | `SELECT_PRIV` | table / view | Read rows |
| 2 | INSERT | `LOAD_PRIV` | table | Doris merges INSERT/UPDATE/DELETE/LOAD into LOAD_PRIV |
| 3 | UPDATE | `LOAD_PRIV` | table | See INSERT |
| 4 | DELETE | `LOAD_PRIV` | table | See INSERT |
| 5 | CREATE | `CREATE_PRIV` | catalog / database | Create tables, databases, catalogs |
| 6 | DROP | `DROP_PRIV` | catalog / database | Drop tables, databases |
| 7 | ALTER | `ALTER_PRIV` | table | Alter column types, add columns |
| 8 | LOAD | `LOAD_PRIV` | table | STREAM LOAD / ROUTINE LOAD / INSERT INTO |
| 9 | GRANT | `GRANT_PRIV` | global | Re-grant privileges to others |
| 10 | ADMIN | `ADMIN_PRIV ON *.*.*` | global | Full admin — manage users, roles, system vars |
| 26 | NODE | `NODE_PRIV ON *.*.*` | global | **Critical** — add/decommission cluster nodes |
| 27 | SHOW_VIEW | `Show_view_priv` | table / view | `SHOW CREATE VIEW`; required by BI tools |

> **Column-level SELECT** — see [section (h)](#h-doris-column-level-privileges) for `GRANT SELECT_PRIV(col1, col2)`.

#### Kafka (11 permissions)

| ID | Permission | Kafka / Strimzi ACL | Notes |
|---|---|---|---|
| 11 | PRODUCE | topic:\*:Write + Describe | Write messages to topics |
| 12 | CONSUME | topic:\*:Read + Describe, group:\*:Read | Read messages; join any consumer group |
| 13 | CREATE_TOPIC | cluster:Create, topic:\*:Create | Create new topics |
| 14 | DELETE_TOPIC | topic:\*:Delete | Delete topics |
| 15 | DESCRIBE | topic:\*:Describe | Describe topic metadata |
| 16 | ADMIN | cluster:All | Full broker-level admin |
| 28 | SCHEMA_REGISTRY_READ | Application-layer (Schema Registry REST API) | GET `/subjects`, `/schemas` — read registered schemas |
| 29 | SCHEMA_REGISTRY_WRITE | Application-layer (Schema Registry REST API) | POST `/subjects` — register or evolve schemas |
| 30 | CDC_CONNECT | Application-layer (Kafka Connect REST API :8083) | Create/update/delete Debezium connector configs |
| 31 | CONSUMER_GROUP_MANAGE | group:\*:Describe + Delete | Describe lag, reset offsets, delete consumer groups |
| 32 | TRANSACTIONAL_WRITE | transactionalId:\*:Write + Describe | Use Kafka transactional producers (exactly-once) |

> **Schema Registry & Debezium notes:**
> Permissions 28–30 are enforced at the **application layer**, not natively by Kafka ACLs.
> Schema Registry and the Connect REST API have their own auth layers (currently open in
> this cluster — no username/password required on the internal endpoints). These permissions
> are recorded in the RBAC plane for governance and future enforcement.

#### OpenSearch (5 permissions)

| ID | Permission | OpenSearch action patterns | Notes |
|---|---|---|---|
| 17 | INDEX_READ | `indices:data/read/*`, `indices:admin/mappings/get` | Search, get, scroll |
| 18 | INDEX_WRITE | `indices:data/write/*` | Index, bulk, update, delete by query |
| 19 | INDEX_ADMIN | `indices:admin/*` + read + write | Create/delete indexes, aliases, mappings |
| 20 | CLUSTER_READ | `cluster:monitor/*` | Cluster health, stats, node info |
| 21 | CLUSTER_ADMIN | `cluster:admin/*` + monitor | Snapshot, reroute, settings, security API |

#### Spark (7 permissions)

| ID | Permission | Allowlist field | Notes |
|---|---|---|---|
| 22 | SUBMIT_JOB | `can_submit: true` | Submit jobs to Spark master via krb-spark-guard |
| 23 | KILL_OWN_JOB | implicit with SUBMIT_JOB | Kill own running applications |
| 24 | KILL_ANY_JOB | `can_kill_any: true` | Kill any user's running application (operator) |
| 25 | VIEW_UI | `view_ui: true` | Access Spark Master Web UI (port 30707) |
| 33 | USE_CATALOG | `can_use_catalog: true` | Read Polaris REST catalog metadata; list namespaces and Iceberg tables |
| 34 | WRITE_ICEBERG | `can_write_iceberg: true` | Create, INSERT INTO, and modify Iceberg tables via Polaris |
| 35 | ADMIN_CATALOG | `can_admin_catalog: true` | Full Polaris catalog admin — create/drop namespaces, manage grants |

> **Polaris Iceberg notes:**
> Permissions 33–35 are recorded in the `spark-rbac-allowlist` ConfigMap.
> Actual enforcement happens at **Apache Polaris** (`http://polaris-rest.prod.svc.cluster.local:8181`).
> You must also grant the user the corresponding Polaris catalog role via the Polaris management API —
> see [section (h)](#h-doris-column-level-privileges) — the allowlist field signals intent; Polaris enforces it.
>
> Spark jobs use the Iceberg REST catalog at:
> ```
> spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog
> spark.sql.catalog.polaris.type=rest
> spark.sql.catalog.polaris.uri=http://polaris-rest.prod.svc.cluster.local:8181/api/catalog
> ```

```bash
# Fetch live permission IDs for any service
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/services/spark/permissions" | \
  python3 -c "import sys,json; [print(f'  [{p[\"id\"]:2}] {p[\"name\"]:25} — {p[\"description\"][:65]}') for p in sorted(json.load(sys.stdin),key=lambda x:x['id'])]"
```

---

## (a) Adding a New User to a Group

> This is the **complete onboarding checklist** for any new user on the platform.
> All four services require different setup steps. Complete them in the order shown.
>
> **Identity convention:** the same short username (e.g. `john`) is used across
> Kerberos, Doris, Kafka, OpenSearch, and Spark. The KDC principal carries the realm:
> `john@STARDATADBLABS.LOCAL`. All services strip the realm and use `john`.

```bash
# Set for the whole section
USERNAME="john"
PASSWORD="TempPass1!"
ROLE="data_engineer"    # or: platform_admin, account_admin, analyst, etl_writer …
DISPLAY_NAME="John Miller"
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
KEYS_FILE="${HOME}/openbao-init-keys.json"
[ -f "${KEYS_FILE}" ] || KEYS_FILE="/root/openbao-init-keys.json"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")

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

> **Reminder:** `USERNAME` must be set in your shell before running these commands.
> If you started a new session: `USERNAME="john"` (or whatever username you onboarded).

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

> **Note:** OpenSearch's Security REST API requires the admin TLS client certificate —
> Basic auth (`-u admin:password`) is rejected when Kerberos is the active HTTP auth domain.
> All Security API checks below use `kubectl exec` with the admin cert from inside the cluster.

```bash
# Set USERNAME before running (should already be set from the top of this section)
# USERNAME="carol"   # ← uncomment and set if running this step standalone

# Internal user was created by the sync
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  curl -sk \
    --cert /usr/share/opensearch/config/tls/admin.pem \
    --key  /usr/share/opensearch/config/tls/admin-key.pem \
    --cacert /usr/share/opensearch/config/tls/root-ca.pem \
    'https://localhost:9200/_plugins/_security/api/internalusers/${USERNAME}' | \
  python3 -c \"import sys,json; d=json.load(sys.stdin); print('exists:', '${USERNAME}' in d)\"
"

# Role mappings applied by RBAC sync
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  curl -sk \
    --cert /usr/share/opensearch/config/tls/admin.pem \
    --key  /usr/share/opensearch/config/tls/admin-key.pem \
    --cacert /usr/share/opensearch/config/tls/root-ca.pem \
    'https://localhost:9200/_plugins/_security/api/rolesmapping' | \
  python3 -c \"
import sys,json
for role,m in json.load(sys.stdin).items():
    if '${USERNAME}' in m.get('users',[]):
        print(f'  mapped: {role}')
\"
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
| `platform_admin` / `account_admin` | `{"can_admin_catalog": true, "can_kill_any": true, "can_submit": true, "can_use_catalog": true, "can_write_iceberg": true, "view_ui": true}` |
| `data_engineer` | `{"can_admin_catalog": false, "can_kill_any": false, "can_submit": true, "can_use_catalog": true, "can_write_iceberg": true, "view_ui": true}` |
| `analyst` | `{"can_admin_catalog": false, "can_kill_any": false, "can_submit": false, "can_use_catalog": true, "can_write_iceberg": false, "view_ui": true}` |

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

### Add users to the admin group

Follow [section (a)](#a-adding-a-new-user-to-a-group) with `ROLE="platform_admin"` to fully onboard a new user (KDC principal, Doris SQL user, RBAC register + bind, sync).

#### Single user (quick one-liner for an existing KDC+Doris user)

```bash
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

#### Multiple users at once (bulk bind)

> All users must already be registered in the RBAC plane. See [section (k)](#k-bulk-binding---adding-multiple-users-to-a-role-in-one-call) for the full bulk-bind workflow.

```bash
# API — bind bob, alice, and charlie to platform_admin in one call
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"usernames":["bob","alice","charlie"]}' \
  "${RBAC_URL}/api/v1/users/roles/platform_admin/members" | python3 -m json.tool

# CLI equivalent
rbacctl role add-members platform_admin bob alice charlie

# Then sync all three to all services
rbacctl sync run
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
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  curl -sk \
    --cert /usr/share/opensearch/config/tls/admin.pem \
    --key  /usr/share/opensearch/config/tls/admin-key.pem \
    --cacert /usr/share/opensearch/config/tls/root-ca.pem \
    'https://localhost:9200/_plugins/_security/api/rolesmapping' | \
  python3 -c \"
import sys,json
roles=[r for r,m in json.load(sys.stdin).items() if '${USERNAME}' in m.get('users',[])]
print('Mapped roles:', roles)
\"
"
# Expected: 5 rbac_* roles including rbac_cluster_admin_all and rbac_index_admin_all
```

#### Spark
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('${USERNAME}'))"
# Expected: {"can_admin_catalog": true, "can_kill_any": true, "can_submit": true, "can_use_catalog": true, "can_write_iceberg": true, "view_ui": true}
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

### Add users to the data_engineer group

Follow [section (a)](#a-adding-a-new-user-to-a-group) with `ROLE="data_engineer"` to fully onboard a new user.

#### Single user

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

#### Multiple users at once (bulk bind)

> See [section (k)](#k-bulk-binding---adding-multiple-users-to-a-role-in-one-call) for the full bulk-bind workflow.

```bash
# API
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"usernames":["carol","dave","frank"]}' \
  "${RBAC_URL}/api/v1/users/roles/data_engineer/members" | python3 -m json.tool

# CLI
rbacctl role add-members data_engineer carol dave frank

# Sync
rbacctl sync run
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
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  curl -sk \
    --cert /usr/share/opensearch/config/tls/admin.pem \
    --key  /usr/share/opensearch/config/tls/admin-key.pem \
    --cacert /usr/share/opensearch/config/tls/root-ca.pem \
    'https://localhost:9200/_plugins/_security/api/rolesmapping' | \
  python3 -c \"
import sys,json
roles=[r for r,m in json.load(sys.stdin).items() if '${USERNAME}' in m.get('users',[])]
print('Mapped roles:', roles)
\"
"
# Expected: rbac_index_read_all, rbac_index_write_all, rbac_cluster_read_all
# NOT expected: rbac_index_admin_all, rbac_cluster_admin_all
```

#### Spark
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('${USERNAME}'))"
# Expected: {"can_admin_catalog": false, "can_kill_any": false, "can_submit": true, "can_use_catalog": true, "can_write_iceberg": true, "view_ui": true}
# NOT expected: can_kill_any=true or can_admin_catalog=true
```

---

## (d) `account_admin` User Group — Top-Level Governance

> **Role name:** `account_admin`
> **Purpose:** account-level governance and user provisioning. Full admin on all services.
> Same permission set as `platform_admin` — named separately for governance boundary.

### Add users to the account_admin group

Follow [section (a)](#a-adding-a-new-user-to-a-group) with `ROLE="account_admin"` to fully onboard a new user.

#### Single user

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

#### Multiple users at once (bulk bind)

> See [section (k)](#k-bulk-binding---adding-multiple-users-to-a-role-in-one-call) for the full bulk-bind workflow.

```bash
# API
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"usernames":["dave","grace"]}' \
  "${RBAC_URL}/api/v1/users/roles/account_admin/members" | python3 -m json.tool

# CLI
rbacctl role add-members account_admin dave grace

# Sync
rbacctl sync run
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
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  curl -sk \
    --cert /usr/share/opensearch/config/tls/admin.pem \
    --key  /usr/share/opensearch/config/tls/admin-key.pem \
    --cacert /usr/share/opensearch/config/tls/root-ca.pem \
    'https://localhost:9200/_plugins/_security/api/rolesmapping' | \
  python3 -c \"
import sys,json
roles=[r for r,m in json.load(sys.stdin).items() if '${USERNAME}' in m.get('users',[])]
print('Mapped roles:', roles)
\"
"
# Expected: all 5 rbac_* roles
```

#### Spark
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('${USERNAME}'))"
# Expected: {"can_admin_catalog": true, "can_kill_any": true, "can_submit": true, "can_use_catalog": true, "can_write_iceberg": true, "view_ui": true}
```

### Role comparison table

| Role | Permission count | Intended for |
|---|---|---|
| `data_admin` | 35 (all) | Generic full admin (legacy seed) |
| `platform_admin` | 35 (all) | Platform infrastructure admin group |
| `account_admin` | 35 (all) | Account governance team |
| `data_engineer` | 20 (SELECT + DML + schema/catalog) | Day-to-day engineering team |
| `analyst` | 9 (SELECT + read + schema/catalog read) | Read-only analysts |

---

## (e) Adding and Removing Privileges for a User Group

> Privileges are managed at the **role** level — changes apply to every user bound
> to that role. After modifying a role, re-sync all affected users to push the new
> state to the downstream services.
>
> **API endpoints:**
> - Add permission: `POST /api/v1/roles/{role_id}/permissions/{service_name}/{permission_name}`
> - Remove permission: `DELETE /api/v1/roles/{role_id}/permissions/{service_name}/{permission_name}`
> - `service_name` is one of `doris`, `kafka`, `opensearch`, `spark`.
> - `permission_name` is the token exactly as listed in the [Permission ID reference](#permission-id-reference), e.g. `SELECT`, `CONSUME`, `INDEX_READ`.

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
  id= 1  name=analyst          ( 9 perms)
  id= 2  name=etl_writer        (12 perms)
  id= 3  name=spark_user        ( 3 perms)
  id= 4  name=data_admin        (35 perms)
  id= 5  name=kafka_consumer    ( 3 perms)
  id= 6  name=platform_admin    (35 perms)
  id= 7  name=data_engineer     (20 perms)
  id= 8  name=account_admin     (35 perms)
```

### Adding a privilege to a user group

**Pattern:** `POST /api/v1/roles/{role_id}/permissions/{permission_id}`

After adding, sync all users bound to the role so the new privilege is pushed to the
downstream services.

#### Example A — Give `data_engineer` the ability to CREATE tables in Doris

> Adding permission `doris:CREATE` to role `data_engineer` (id=7).

```bash
ROLE_ID=7   # data_engineer

# 1. Add the permission to the role
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/doris/CREATE" | \
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

> Adding permission `kafka:CREATE_TOPIC` to role `data_engineer` (id=7).

```bash
ROLE_ID=7

# 1. Add permission
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/kafka/CREATE_TOPIC" | \
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

> Adding permission `opensearch:INDEX_ADMIN` to role `data_engineer` (id=7).

```bash
ROLE_ID=7

# 1. Add permission
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/opensearch/INDEX_ADMIN" | \
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
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  curl -sk \
    --cert /usr/share/opensearch/config/tls/admin.pem \
    --key  /usr/share/opensearch/config/tls/admin-key.pem \
    --cacert /usr/share/opensearch/config/tls/root-ca.pem \
    'https://localhost:9200/_plugins/_security/api/rolesmapping' | \
  python3 -c \"
import sys,json
for role,m in json.load(sys.stdin).items():
    if 'carol' in m.get('users',[]):
        print(f'  mapped: {role}')
\"
"
# Now expected: rbac_index_admin_all is in the list (was absent before)
```

---

#### Example D — Give `data_engineer` KILL_ANY_JOB on Spark

> Adding permission `spark:KILL_ANY_JOB` to role `data_engineer` (id=7).

```bash
ROLE_ID=7

# 1. Add permission
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/spark/KILL_ANY_JOB" | \
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
# Now expected: {"can_admin_catalog": false, "can_kill_any": true, "can_submit": true, "can_use_catalog": true, "can_write_iceberg": true, "view_ui": true}
# (can_kill_any was false before)
```

---

### Removing a privilege from a user group

**Pattern:** `DELETE /api/v1/roles/{role_id}/permissions/{service_name}/{permission_name}`

After removing, sync all affected users to revoke the privilege in the downstream services.

#### Example E — Remove CREATE from `data_engineer` in Doris

> Removing permission `doris:CREATE` from role `data_engineer` (id=7).

```bash
ROLE_ID=7

# 1. Remove the permission from the role
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/doris/CREATE" | \
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

> Removing permission `opensearch:INDEX_ADMIN` from role `data_engineer` (id=7).

```bash
ROLE_ID=7

# 1. Remove
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/opensearch/INDEX_ADMIN" | \
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
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  curl -sk \
    --cert /usr/share/opensearch/config/tls/admin.pem \
    --key  /usr/share/opensearch/config/tls/admin-key.pem \
    --cacert /usr/share/opensearch/config/tls/root-ca.pem \
    'https://localhost:9200/_plugins/_security/api/rolesmapping' | \
  python3 -c \"
import sys,json
for role,m in json.load(sys.stdin).items():
    if 'carol' in m.get('users',[]):
        print(f'  mapped: {role}')
\"
"
# rbac_index_admin_all should no longer appear
```

---

#### Example G — Remove KILL_ANY_JOB from `data_engineer` in Spark

> Removing permission `spark:KILL_ANY_JOB` from role `data_engineer` (id=7).

```bash
ROLE_ID=7

# 1. Remove
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/spark/KILL_ANY_JOB"

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
# Expected: {"can_admin_catalog": false, "can_kill_any": false, "can_submit": true, "can_use_catalog": true, "can_write_iceberg": true, "view_ui": true}  (reverted)
```

---

#### Example H — Remove CREATE_TOPIC from `data_engineer` in Kafka

> Removing permission `kafka:CREATE_TOPIC` from role `data_engineer` (id=7).

```bash
ROLE_ID=7

# 1. Remove
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/kafka/CREATE_TOPIC"

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
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  curl -sk \
    --cert /usr/share/opensearch/config/tls/admin.pem \
    --key  /usr/share/opensearch/config/tls/admin-key.pem \
    --cacert /usr/share/opensearch/config/tls/root-ca.pem \
    'https://localhost:9200/_plugins/_security/api/internalusers/ghost' | \
  python3 -c \"import sys,json; print('OS user exists:', 'ghost' in json.load(sys.stdin))\"
"

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

> Each permission is added with `POST /api/v1/roles/{role_id}/permissions/{service_name}/{permission_name}`.
> No request body is needed.
> See the [Permission ID reference](#permission-id-reference) table for the full list of permission names.

#### Doris — SELECT only

```bash
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/doris/SELECT" | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(f'  doris perms: {[p[\"permission_name\"] for p in r[\"permissions\"] if p[\"service_name\"]==\"doris\"]}')"
```

#### Kafka — CONSUME + DESCRIBE

```bash
for PERM in CONSUME DESCRIBE; do
  curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
    "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/kafka/${PERM}" > /dev/null \
    && echo "  added kafka:${PERM}"
done
```

#### OpenSearch — INDEX_READ + CLUSTER_READ

```bash
for PERM in INDEX_READ CLUSTER_READ; do
  curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
    "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/opensearch/${PERM}" > /dev/null \
    && echo "  added opensearch:${PERM}"
done
```

#### Spark — VIEW_UI only

```bash
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/spark/VIEW_UI" > /dev/null \
  && echo "  added spark:VIEW_UI"
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

#### Single additional user (4-command operation)

```bash
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

#### Multiple users at once (bulk bind)

> Use this when you have several already-registered users to add to the same role.
> See [section (k)](#k-bulk-binding---adding-multiple-users-to-a-role-in-one-call) for the full workflow.

```bash
# Bind frank, grace, and henry to reporting_analyst in one call
rbacctl role add-members ${NEW_ROLE} frank grace henry

# Sync all three to all services
rbacctl sync run
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
| `analyst` | SELECT, SHOW_VIEW | CONSUME, DESCRIBE, SCHEMA_REGISTRY_READ | INDEX_READ, CLUSTER_READ | VIEW_UI, USE_CATALOG | Read-only analyst |
| `etl_writer` | SELECT, INSERT, UPDATE, LOAD | PRODUCE, CONSUME, DESCRIBE, SCHEMA_REGISTRY_READ, SCHEMA_REGISTRY_WRITE, TRANSACTIONAL_WRITE | — | SUBMIT_JOB, KILL_OWN_JOB, VIEW_UI, USE_CATALOG, WRITE_ICEBERG | ETL pipeline writer |
| `spark_user` | — | — | — | SUBMIT_JOB, KILL_OWN_JOB, VIEW_UI | Spark job submitter |
| `kafka_consumer` | — | CONSUME, DESCRIBE, SCHEMA_REGISTRY_READ | — | — | Kafka consumer only |
| **`data_engineer`** | SELECT, INSERT, UPDATE, DELETE, LOAD, SHOW_VIEW | PRODUCE, CONSUME, DESCRIBE, SCHEMA_REGISTRY_READ, CONSUMER_GROUP_MANAGE, TRANSACTIONAL_WRITE | INDEX_READ, INDEX_WRITE, CLUSTER_READ | SUBMIT_JOB, KILL_OWN_JOB, VIEW_UI, USE_CATALOG, WRITE_ICEBERG | Data engineering team |
| **`platform_admin`** | All 12 (incl. ADMIN_PRIV, NODE) | All 11 | All 5 | All 7 | Platform infra admin |
| **`account_admin`** | All 12 (incl. ADMIN_PRIV, NODE) | All 11 | All 5 | All 7 | Account governance admin |
| `data_admin` | All 12 | All 11 | All 5 | All 7 | Generic full admin (legacy seed) |

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

---

## (h) Doris Column-Level Privileges

> Column-level privileges in Doris restrict which columns a user can SELECT from a specific
> table. They are **not** managed by the RBAC plane sync — they are applied manually with
> SQL and survive a full `REVOKE ALL` sync cycle.
>
> Use column-level grants when a table contains sensitive columns (PII, salary, health data)
> that should not be visible to all users in a role.

### Syntax

```sql
-- Grant SELECT on specific columns only
GRANT SELECT_PRIV(col1, col2, col3) ON internal.<database>.<table> TO '<user>'@'%';

-- The catalog must be specified as the 3-part name: internal.<db>.<table>
-- Using just <db>.<table> will be rejected by the Doris parser.
-- SELECT_PRIV is the correct keyword — SELECT without _PRIV is invalid here.
```

### Worked example — restrict the `salary` and `ssn` columns

> **Scenario:** The `analyst` role has `SELECT_PRIV` on all tables in `internal.*.*`.
> The `hr.employees` table contains `salary` and `ssn` columns. The analyst `alice`
> should only be able to read `employee_id`, `name`, and `department`.

#### Step 1 — Verify current table structure

```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "DESCRIBE hr.employees;" 2>/dev/null
# Expected columns: employee_id, name, department, salary, ssn, hire_date
```

#### Step 2 — Revoke the broad table-level SELECT, grant column-level SELECT

```bash
# First revoke the broad SELECT on the entire table
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "REVOKE SELECT_PRIV ON internal.hr.employees FROM 'alice'@'%';" 2>/dev/null

# Then grant SELECT only on the safe columns
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "GRANT SELECT_PRIV(employee_id, name, department, hire_date) \
      ON internal.hr.employees TO 'alice'@'%';" 2>/dev/null
```

#### Step 3 — Verify the column grants from the admin side

```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR 'alice'@'%';" 2>/dev/null

# Expected output will include a line like:
# GRANT SELECT_PRIV(employee_id,name,department,hire_date)
#   ON internal.hr.employees TO 'alice'@'%'
```

#### Step 4 — Test as the user

```bash
# This MUST succeed — allowed columns only
mysql -h 192.168.1.50 -P 30090 -u alice --password="${ALICE_PASS}" \
  -e "SELECT employee_id, name, department FROM hr.employees LIMIT 5;" 2>/dev/null

# This MUST fail — salary is not in the grant
mysql -h 192.168.1.50 -P 30090 -u alice --password="${ALICE_PASS}" \
  -e "SELECT salary FROM hr.employees LIMIT 1;" 2>&1 | grep -i "denied\|error"
# Expected: Access denied; you need (at least one of) the SELECT privilege(s) for ...

# Selecting all columns (*) also fails when any column is restricted
mysql -h 192.168.1.50 -P 30090 -u alice --password="${ALICE_PASS}" \
  -e "SELECT * FROM hr.employees LIMIT 1;" 2>&1 | grep -i "denied\|error"
# Expected: Access denied
```

### Adding a column to an existing column grant

Doris column grants are **additive** — you can grant additional columns without revoking
the existing grant:

```bash
# Add hire_date to an existing 3-column grant (if not already included)
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "GRANT SELECT_PRIV(hire_date) \
      ON internal.hr.employees TO 'alice'@'%';" 2>/dev/null
```

### Revoking column-level privileges

> **Important:** `REVOKE ALL ON *.*` (the broad sync revoke) does **not** clear
> column-level grants. You must explicitly revoke them per table.

```bash
# Revoke all column-level SELECT on a specific table
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "REVOKE SELECT_PRIV(employee_id, name, department, hire_date) \
      ON internal.hr.employees FROM 'alice'@'%';" 2>/dev/null

# Verify — the column grant line should be gone
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR 'alice'@'%';" 2>/dev/null
```

### Applying column grants to multiple users

```bash
# Grant to every user currently bound to the analyst role
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
RBAC_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)

ANALYST_USERS=$(curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "http://192.168.1.50:30850/api/v1/users" | \
  python3 -c "
import sys, json
users = json.load(sys.stdin)
print('\n'.join(u['username'] for u in users))
" | while read u; do
    roles=$(curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
      "http://192.168.1.50:30850/api/v1/users/${u}/bindings" | \
      python3 -c "import sys,json; print(' '.join(b['role_name'] for b in json.load(sys.stdin)))")
    echo "${roles}" | grep -q "analyst" && echo "${u}"
  done)

for USER in ${ANALYST_USERS}; do
  echo "Granting column-level SELECT to ${USER}..."
  mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
    -e "REVOKE SELECT_PRIV ON internal.hr.employees FROM '${USER}'@'%';" 2>/dev/null
  mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
    -e "GRANT SELECT_PRIV(employee_id, name, department, hire_date) \
        ON internal.hr.employees TO '${USER}'@'%';" 2>/dev/null
done
```

### Known limitations

| Limitation | Detail |
|---|---|
| RBAC plane does not manage column grants | Column-level `SELECT_PRIV` is applied manually. Running a full platform sync will **not** revoke or reapply column grants. |
| `REVOKE ALL ON *.*` does not clear column grants | Column grants on specific tables survive the sync revoke cycle. They must be explicitly removed. |
| No `SELECT` shorthand | `GRANT SELECT(col)` is rejected — only `GRANT SELECT_PRIV(col)` is valid. |
| 3-part name required | `ON hr.employees` is rejected — must be `ON internal.hr.employees` (catalog.db.table). |
| `SELECT *` blocked | If any column is denied, `SELECT *` returns an access-denied error for that column. |
| Column grants are per-table | There is no wildcard column grant — you cannot do `GRANT SELECT_PRIV(salary) ON internal.hr.*`. |

---

## (i) Polaris Catalog Grants for `WRITE_ICEBERG` / `ADMIN_CATALOG`

> The RBAC plane records `WRITE_ICEBERG` and `ADMIN_CATALOG` in the `spark-rbac-allowlist`
> ConfigMap — this signals *intent* and enforces access at the **krb-spark-guard** layer.
> However, **Apache Polaris enforces catalog privileges independently** via its own
> principal-role system. You must grant the user the corresponding Polaris catalog role
> separately, or Iceberg write/admin operations will be rejected at the catalog REST API
> even if the allowlist entry is present.
>
> Polaris REST catalog: `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog`
> Polaris management API: `http://polaris-rest.prod.svc.cluster.local:8181/api/management/v1`

### Polaris role mapping

| RBAC permission | RBAC allowlist field | Polaris catalog role to grant |
|---|---|---|
| `USE_CATALOG` (id=33) | `can_use_catalog: true` | `catalog_viewer` (or any role with `CATALOG_READ`) |
| `WRITE_ICEBERG` (id=34) | `can_write_iceberg: true` | `catalog_writer` (TABLE_WRITE_DATA + TABLE_CREATE) |
| `ADMIN_CATALOG` (id=35) | `can_admin_catalog: true` | `catalog_admin` (CATALOG_MANAGE_CONTENT + MANAGE_GRANTS) |

### Prerequisites

```bash
# Polaris management API is internal — run from inside the cluster or via port-forward
kubectl port-forward svc/polaris-rest -n prod 8181:8181 &
POLARIS_URL="http://localhost:8181"

# Get the Polaris root token (set during Polaris bootstrap)
POLARIS_TOKEN=$(kubectl get secret polaris-credentials -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d 2>/dev/null || \
  kubectl get secret polaris-credentials -n prod \
  -o jsonpath='{.data.token}' | base64 -d)
```

### Step 1 — Create or verify the Polaris principal for the user

Polaris maintains its own principal registry. Each user who needs catalog access must
have a Polaris principal whose name matches their Kerberos short name.

```bash
USERNAME="carol"

# List existing principals
curl -s -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/principals" | \
  python3 -c "import sys,json; [print(p['name']) for p in json.load(sys.stdin).get('principals',[])]"

# Create the Polaris principal if it does not exist
curl -s -X POST \
  -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"${USERNAME}\", \"type\": \"USER\"}" \
  "${POLARIS_URL}/api/management/v1/principals" | python3 -m json.tool
```

> **Note:** The Polaris principal name must exactly match the Doris/Kerberos short name
> (e.g. `carol`, not `carol@STARDATADBLABS.LOCAL`).

### Step 2 — List available catalog roles

```bash
# List all catalog roles in the default catalog
CATALOG="polaris"   # the catalog name configured in spark-defaults.conf

curl -s -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/catalogs/${CATALOG}/catalogRoles" | \
  python3 -c "
import sys,json
for r in json.load(sys.stdin).get('roles',[]):
    print(f'  {r[\"name\"]:30}  {r.get(\"description\",\"\")}')
"
```

### Step 3 — Grant the appropriate catalog role to the principal

#### For `WRITE_ICEBERG` — grant `catalog_writer`

```bash
USERNAME="carol"
CATALOG="polaris"
CATALOG_ROLE="catalog_writer"   # TABLE_WRITE_DATA + TABLE_CREATE

curl -s -X PUT \
  -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  -H "Content-Type: application/json" \
  "${POLARIS_URL}/api/management/v1/principals/${USERNAME}/principal-roles/${CATALOG_ROLE}" | \
  python3 -m json.tool
# Expected: 200 OK or {"message":"Principal role assigned"}
```

#### For `ADMIN_CATALOG` — grant `catalog_admin`

```bash
USERNAME="bob"
CATALOG_ROLE="catalog_admin"    # CATALOG_MANAGE_CONTENT + MANAGE_GRANTS

curl -s -X PUT \
  -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  -H "Content-Type: application/json" \
  "${POLARIS_URL}/api/management/v1/principals/${USERNAME}/principal-roles/${CATALOG_ROLE}" | \
  python3 -m json.tool
```

#### For `USE_CATALOG` only — grant `catalog_viewer`

```bash
USERNAME="alice"
CATALOG_ROLE="catalog_viewer"   # CATALOG_READ only

curl -s -X PUT \
  -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  -H "Content-Type: application/json" \
  "${POLARIS_URL}/api/management/v1/principals/${USERNAME}/principal-roles/${CATALOG_ROLE}" | \
  python3 -m json.tool
```

### Step 4 — Verify the assignment

```bash
# List all catalog roles assigned to the principal
curl -s -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/principals/${USERNAME}/principal-roles" | \
  python3 -c "
import sys,json
roles = json.load(sys.stdin).get('principalRoles', [])
print(f'{USERNAME} catalog roles: {[r[\"name\"] for r in roles]}')
"
```

### Step 5 — Test Iceberg access from Spark

```bash
# Port-forward the Spark master UI (optional — for job submission)
# Submit a test Spark job as the user (requires a valid keytab)

kubectl exec -n prod deploy/spark-master -- spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.kerberos.principal=${USERNAME}@STARDATADBLABS.LOCAL" \
  --conf "spark.kerberos.keytab=/etc/security/keytabs/${USERNAME}.keytab" \
  --conf "spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.polaris.type=rest" \
  --conf "spark.sql.catalog.polaris.uri=http://polaris-rest.prod.svc.cluster.local:8181/api/catalog" \
  --class org.apache.spark.examples.SparkPi \
  local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar 10 2>&1 | tail -5

# Quick Iceberg read test via spark-shell (data_engineer / catalog_writer user)
kubectl exec -it -n prod deploy/spark-master -- spark-shell \
  --conf "spark.sql.catalog.polaris.uri=http://polaris-rest.prod.svc.cluster.local:8181/api/catalog" \
  << 'EOF'
spark.sql("SHOW NAMESPACES IN polaris").show()
spark.sql("SHOW TABLES IN polaris.default").show()
EOF
```

#### For `WRITE_ICEBERG` — write test

```bash
kubectl exec -it -n prod deploy/spark-master -- spark-shell \
  --conf "spark.sql.catalog.polaris.uri=http://polaris-rest.prod.svc.cluster.local:8181/api/catalog" \
  << 'EOF'
spark.sql("""
  CREATE TABLE IF NOT EXISTS polaris.default.rbac_write_test (
    id   BIGINT,
    name STRING
  ) USING iceberg
""")
spark.sql("INSERT INTO polaris.default.rbac_write_test VALUES (1, 'rbac_test')")
spark.sql("SELECT * FROM polaris.default.rbac_write_test").show()
spark.sql("DROP TABLE polaris.default.rbac_write_test")
EOF
```

### Revoking a Polaris catalog role

```bash
USERNAME="carol"
CATALOG_ROLE="catalog_writer"

curl -s -X DELETE \
  -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/principals/${USERNAME}/principal-roles/${CATALOG_ROLE}"
# Expected: 200 OK / empty body

# Verify removal
curl -s -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/principals/${USERNAME}/principal-roles" | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('principalRoles',[]))"
```

### Combined workflow — RBAC plane + Polaris (complete sequence)

When granting `WRITE_ICEBERG` or `ADMIN_CATALOG` to a user, complete **both** steps:

```bash
USERNAME="carol"
RBAC_URL="http://192.168.1.50:30850"
RBAC_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)

# ── Step A: RBAC plane — ensure the permission is on the role ──────────────
# data_engineer already has WRITE_ICEBERG after migration 004.
# If adding to a custom role:
#   curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
#     "${RBAC_URL}/api/v1/roles/${ROLE_ID}/permissions/spark/WRITE_ICEBERG"

# ── Step B: RBAC plane — sync to push allowlist entry ─────────────────────
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
# Verify: allowlist shows can_write_iceberg: true

# ── Step C: Polaris — grant the catalog writer role ────────────────────────
kubectl port-forward svc/polaris-rest -n prod 8181:8181 &
POLARIS_TOKEN=$(kubectl get secret polaris-credentials -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

curl -s -X PUT \
  -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  -H "Content-Type: application/json" \
  "http://localhost:8181/api/management/v1/principals/${USERNAME}/principal-roles/catalog_writer" | \
  python3 -m json.tool

kill %1   # stop port-forward
```

### Summary — Two-layer enforcement

| Layer | What enforces it | How to grant | How to revoke |
|---|---|---|---|
| **krb-spark-guard allowlist** | RBAC plane sync → `spark-rbac-allowlist` ConfigMap | Add permission to role + sync | Remove permission from role + sync |
| **Polaris REST catalog** | Apache Polaris principal-role system | `PUT /principals/{user}/principal-roles/{role}` | `DELETE /principals/{user}/principal-roles/{role}` |

> Both layers must grant access. A user with `can_write_iceberg: true` in the allowlist
> but **no Polaris catalog role** will pass the guard but receive a 403 from the Polaris
> catalog API when attempting to write. Conversely, a user with a Polaris catalog role
> but `can_write_iceberg: false` will be blocked by the guard before reaching Polaris.

---

## (j) Sample Data Setup and End-to-End Permission Testing

> **Purpose:** Create concrete test fixtures — a Doris database with tables, Kafka topics,
> OpenSearch indexes, and a Spark Iceberg table — then run permission-gated queries as each
> role to confirm the RBAC plane is enforcing access correctly.
>
> All samples use the test user **`testuser`** (bound to `data_engineer`) and **`readonlyuser`**
> (bound to `analyst`). Run section [(a)](#a-adding-a-new-user-to-a-group) to create them
> before starting here.

```bash
# Prerequisite variables — already set if you followed the Prerequisites section above
export RBAC_URL="http://192.168.1.50:30850"
export RBAC_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)
export DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
export OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.opensearch-password}' | base64 -d 2>/dev/null || echo "admin")
export REALM="STARDATADBLABS.LOCAL"

TESTUSER="testuser"
TESTPASS="TestPass1!"
READONLY="readonlyuser"
READONLY_PASS="ReadOnly1!"
```

### Step 0 — Create the two test users (skip if already exists)

```bash
# KDC principals
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -pw ${TESTPASS} ${TESTUSER}@${REALM}"
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -pw ${READONLY_PASS} ${READONLY}@${REALM}"

# Doris SQL users
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" -e "
  CREATE USER IF NOT EXISTS '${TESTUSER}'@'%'  IDENTIFIED BY '${TESTPASS}';
  CREATE USER IF NOT EXISTS '${READONLY}'@'%'  IDENTIFIED BY '${READONLY_PASS}';
" 2>/dev/null

# RBAC: register + bind roles
for U in ${TESTUSER} ${READONLY}; do
  curl -sf -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${U}\",\"display_name\":\"${U} (test)\",\"email\":\"${U}@test.invalid\"}" \
    "${RBAC_URL}/api/v1/users" > /dev/null && echo "Registered ${U}"
done

curl -sf -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"role_name":"data_engineer"}' \
  "${RBAC_URL}/api/v1/users/${TESTUSER}/bindings" > /dev/null && echo "Bound data_engineer → ${TESTUSER}"

curl -sf -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"role_name":"analyst"}' \
  "${RBAC_URL}/api/v1/users/${READONLY}/bindings" > /dev/null && echo "Bound analyst → ${READONLY}"

# Sync both users to all services
for U in ${TESTUSER} ${READONLY}; do
  curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${U}\",\"dry_run\":false}" \
    "${RBAC_URL}/api/v1/sync" | \
    python3 -c "import sys,json; r=json.load(sys.stdin); print(f'sync {\"${U}\"}: errors={r[\"errors\"]}')"
done
```

---

### Doris — Create sample database and tables

All sample objects live in the `rbac_test` database. Run these as the Doris **root** user.

#### Create database and tables (root only)

```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" 2>/dev/null << 'SQL'

-- ── Database ─────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS rbac_test;

-- ── orders table ─────────────────────────────────────────
-- Simulates a transactional orders table.
-- data_engineer can SELECT + INSERT/UPDATE/DELETE.
-- analyst can SELECT only.
CREATE TABLE IF NOT EXISTS rbac_test.orders (
  order_id    BIGINT        NOT NULL,
  customer    VARCHAR(100)  NOT NULL,
  product     VARCHAR(200)  NOT NULL,
  amount      DECIMAL(12,2) NOT NULL,
  status      VARCHAR(30)   NOT NULL DEFAULT 'pending',
  created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- ── products table ────────────────────────────────────────
-- Lookup / reference table.
CREATE TABLE IF NOT EXISTS rbac_test.products (
  product_id  INT           NOT NULL,
  name        VARCHAR(200)  NOT NULL,
  category    VARCHAR(100)  NOT NULL,
  price       DECIMAL(10,2) NOT NULL
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- ── events table ─────────────────────────────────────────
-- Append-only event log — tests INSERT (LOAD_PRIV) restriction.
CREATE TABLE IF NOT EXISTS rbac_test.events (
  event_id    BIGINT        NOT NULL,
  event_type  VARCHAR(50)   NOT NULL,
  user_id     VARCHAR(80)   NOT NULL,
  payload     VARCHAR(4000) NOT NULL DEFAULT '{}',
  ts          DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(event_id, ts)
DISTRIBUTED BY HASH(event_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- ── Seed data ─────────────────────────────────────────────
INSERT INTO rbac_test.products VALUES
  (1, 'Widget Alpha',  'Widgets',  9.99),
  (2, 'Widget Beta',   'Widgets', 14.99),
  (3, 'Gizmo Pro',     'Gizmos',  49.99),
  (4, 'Gizmo Lite',    'Gizmos',  24.99),
  (5, 'Thingamajig',   'Other',    5.00);

INSERT INTO rbac_test.orders VALUES
  (1001, 'alice',   'Widget Alpha',  19.98, 'completed',  '2025-01-10 09:00:00'),
  (1002, 'bob',     'Gizmo Pro',     49.99, 'shipped',    '2025-01-11 10:30:00'),
  (1003, 'carol',   'Widget Beta',   14.99, 'pending',    '2025-01-12 14:15:00'),
  (1004, 'alice',   'Gizmo Lite',    24.99, 'completed',  '2025-01-13 08:45:00'),
  (1005, 'dave',    'Thingamajig',    5.00, 'cancelled',  '2025-01-14 16:00:00');

INSERT INTO rbac_test.events VALUES
  (1, 'login',     'alice',  '{"ip":"10.0.0.1"}',          '2025-01-10 08:59:00'),
  (2, 'purchase',  'alice',  '{"order_id":1001}',           '2025-01-10 09:00:01'),
  (3, 'login',     'bob',    '{"ip":"10.0.0.2"}',           '2025-01-11 10:29:00'),
  (4, 'purchase',  'bob',    '{"order_id":1002}',           '2025-01-11 10:30:01'),
  (5, 'logout',    'carol',  '{"session_duration":"120s"}', '2025-01-12 14:20:00');

SQL
echo "Doris sample data created"
```

#### Verify tables and row counts (root)

```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" 2>/dev/null -e "
  SELECT 'orders'   AS tbl, COUNT(*) AS rows FROM rbac_test.orders
  UNION ALL
  SELECT 'products', COUNT(*) FROM rbac_test.products
  UNION ALL
  SELECT 'events',   COUNT(*) FROM rbac_test.events;
"
```

Expected:
```
+-----------+------+
| tbl       | rows |
+-----------+------+
| orders    |    5 |
| products  |    5 |
| events    |    5 |
+-----------+------+
```

---

#### Test: `analyst` — SELECT allowed, writes blocked

```bash
# ✅ SELECT — must succeed
mysql -h 192.168.1.50 -P 30090 -u ${READONLY} --password="${READONLY_PASS}" 2>/dev/null \
  -e "SELECT order_id, customer, amount FROM rbac_test.orders LIMIT 3;"

# ✅ Aggregate query — must succeed
mysql -h 192.168.1.50 -P 30090 -u ${READONLY} --password="${READONLY_PASS}" 2>/dev/null \
  -e "SELECT status, COUNT(*) AS cnt, SUM(amount) AS total
      FROM rbac_test.orders GROUP BY status ORDER BY cnt DESC;"

# ❌ INSERT — must be denied (analyst has no LOAD_PRIV)
mysql -h 192.168.1.50 -P 30090 -u ${READONLY} --password="${READONLY_PASS}" 2>&1 \
  -e "INSERT INTO rbac_test.orders VALUES (9999,'x','y',1.00,'pending','2025-01-01 00:00:00');" | \
  grep -i "denied\|error"
# Expected: Access denied

# ❌ CREATE TABLE — must be denied
mysql -h 192.168.1.50 -P 30090 -u ${READONLY} --password="${READONLY_PASS}" 2>&1 \
  -e "CREATE TABLE rbac_test.forbidden (id INT);" | grep -i "denied\|error"
# Expected: Access denied
```

#### Test: `data_engineer` — SELECT + DML allowed, admin ops blocked

```bash
# ✅ SELECT — must succeed
mysql -h 192.168.1.50 -P 30090 -u ${TESTUSER} --password="${TESTPASS}" 2>/dev/null \
  -e "SELECT p.name, p.category, COUNT(o.order_id) AS order_count
      FROM rbac_test.products p
      LEFT JOIN rbac_test.orders o ON o.product = p.name
      GROUP BY p.name, p.category
      ORDER BY order_count DESC;"

# ✅ INSERT — must succeed
mysql -h 192.168.1.50 -P 30090 -u ${TESTUSER} --password="${TESTPASS}" 2>/dev/null \
  -e "INSERT INTO rbac_test.orders
      VALUES (1006,'eve','Widget Alpha',9.99,'pending','2025-01-15 11:00:00');"
echo "Insert result: $?"

# ✅ UPDATE — must succeed
mysql -h 192.168.1.50 -P 30090 -u ${TESTUSER} --password="${TESTPASS}" 2>/dev/null \
  -e "UPDATE rbac_test.orders SET status='shipped' WHERE order_id=1006;"

# ✅ DELETE — must succeed
mysql -h 192.168.1.50 -P 30090 -u ${TESTUSER} --password="${TESTPASS}" 2>/dev/null \
  -e "DELETE FROM rbac_test.orders WHERE order_id=1006;"

# ❌ DROP TABLE — must be denied (no DROP_PRIV)
mysql -h 192.168.1.50 -P 30090 -u ${TESTUSER} --password="${TESTPASS}" 2>&1 \
  -e "DROP TABLE rbac_test.orders;" | grep -i "denied\|error"
# Expected: Access denied

# ❌ GRANT — must be denied (no GRANT_PRIV)
mysql -h 192.168.1.50 -P 30090 -u ${TESTUSER} --password="${TESTPASS}" 2>&1 \
  -e "GRANT SELECT_PRIV ON rbac_test.orders TO 'someuser'@'%';" | grep -i "denied\|error"
# Expected: Access denied
```

---

### Kafka — Create sample topics

Topics are created by an admin user. The three topics test produce, consume, and schema
registry permissions.

#### Create topics (admin)

```bash
# Get the Kafka bootstrap address
KAFKA_BOOTSTRAP="192.168.1.50:30092"   # NodePort for the PLAINTEXT / SCRAM listener

# Use the strimzi/kafka container to run kafka-topics.sh
kubectl exec -n prod deploy/kafka-admin -- \
  kafka-topics.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --command-config /opt/kafka/config/admin.properties \
    --create --if-not-exists \
    --topic rbac-test-orders \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000

kubectl exec -n prod deploy/kafka-admin -- \
  kafka-topics.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --command-config /opt/kafka/config/admin.properties \
    --create --if-not-exists \
    --topic rbac-test-events \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000

kubectl exec -n prod deploy/kafka-admin -- \
  kafka-topics.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --command-config /opt/kafka/config/admin.properties \
    --create --if-not-exists \
    --topic rbac-test-products \
    --partitions 1 \
    --replication-factor 1

# Verify topics were created
kubectl exec -n prod deploy/kafka-admin -- \
  kafka-topics.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --command-config /opt/kafka/config/admin.properties \
    --list | grep rbac-test
```

Expected output:
```
rbac-test-events
rbac-test-orders
rbac-test-products
```

#### Seed `rbac-test-orders` with sample messages (admin)

```bash
# Produce 5 sample JSON messages to rbac-test-orders
kubectl exec -n prod deploy/kafka-admin -- bash -c "
cat <<'MSGS' | kafka-console-producer.sh \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --producer.config /opt/kafka/config/admin.properties \
  --topic rbac-test-orders
{\"order_id\":1001,\"customer\":\"alice\",\"product\":\"Widget Alpha\",\"amount\":19.98,\"status\":\"completed\"}
{\"order_id\":1002,\"customer\":\"bob\",\"product\":\"Gizmo Pro\",\"amount\":49.99,\"status\":\"shipped\"}
{\"order_id\":1003,\"customer\":\"carol\",\"product\":\"Widget Beta\",\"amount\":14.99,\"status\":\"pending\"}
{\"order_id\":1004,\"customer\":\"alice\",\"product\":\"Gizmo Lite\",\"amount\":24.99,\"status\":\"completed\"}
{\"order_id\":1005,\"customer\":\"dave\",\"product\":\"Thingamajig\",\"amount\":5.00,\"status\":\"cancelled\"}
MSGS
"
echo "Messages produced to rbac-test-orders"
```

#### Test: `data_engineer` — PRODUCE + CONSUME

```bash
# Get the SCRAM credentials that the RBAC sync created for testuser
SCRAM_SECRET=$(kubectl get secret ${TESTUSER} -n prod \
  -o jsonpath='{.data.password}' | base64 -d 2>/dev/null)

# Write a consumer config with testuser credentials
cat > /tmp/testuser-consumer.properties << EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="${TESTUSER}" password="${SCRAM_SECRET}";
group.id=rbac-test-consumer-group
auto.offset.reset=earliest
EOF

# ✅ CONSUME — read messages from rbac-test-orders (must succeed)
kubectl exec -n prod deploy/kafka-admin -- \
  kafka-console-consumer.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --consumer.config /tmp/testuser-consumer.properties \
    --topic rbac-test-orders \
    --from-beginning \
    --max-messages 5 \
    --timeout-ms 10000
# Expected: 5 JSON messages printed

# ✅ PRODUCE — write a new event message (must succeed)
echo '{"order_id":1006,"customer":"eve","product":"Widget Alpha","amount":9.99,"status":"pending"}' | \
kubectl exec -i -n prod deploy/kafka-admin -- \
  kafka-console-producer.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --producer.config /tmp/testuser-consumer.properties \
    --topic rbac-test-events
echo "Produce result: $?"
```

#### Test: `analyst` — CONSUME only, PRODUCE blocked

```bash
READONLY_SCRAM=$(kubectl get secret ${READONLY} -n prod \
  -o jsonpath='{.data.password}' | base64 -d 2>/dev/null)

cat > /tmp/readonly-consumer.properties << EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="${READONLY}" password="${READONLY_SCRAM}";
group.id=rbac-test-analyst-group
auto.offset.reset=earliest
EOF

# ✅ CONSUME — must succeed (analyst has CONSUME permission)
kubectl exec -n prod deploy/kafka-admin -- \
  kafka-console-consumer.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --consumer.config /tmp/readonly-consumer.properties \
    --topic rbac-test-orders \
    --from-beginning \
    --max-messages 3 \
    --timeout-ms 10000
# Expected: 3 JSON messages

# Note: this cluster has allow.everyone.if.no.acl.found=true, so broker-level ACL
# enforcement is not active. The KafkaUser CR and SCRAM credentials are the
# enforced boundary — the user must be authenticated. Permission differences between
# roles are recorded in the RBAC plane for governance and future ACL enforcement.
```

#### Describe topic metadata (DESCRIBE permission test)

```bash
# Both analyst and data_engineer have DESCRIBE — both should succeed
kubectl exec -n prod deploy/kafka-admin -- \
  kafka-topics.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --command-config /tmp/testuser-consumer.properties \
    --describe \
    --topic rbac-test-orders
```

---

### OpenSearch — Create sample indexes

#### Create indexes and mappings (admin)

> All admin OpenSearch calls use `kubectl exec` with the TLS admin cert — Basic auth
> is rejected when Kerberos is the active HTTP auth domain on this cluster.

```bash
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
CERT='--cert /usr/share/opensearch/config/tls/admin.pem'
KEY='--key  /usr/share/opensearch/config/tls/admin-key.pem'
CA='--cacert /usr/share/opensearch/config/tls/root-ca.pem'
OS='https://localhost:9200'

# ── rbac-test-orders index ─────────────────────────────────
curl -sk \$CERT \$KEY \$CA -X PUT \"\${OS}/rbac-test-orders\" \
  -H 'Content-Type: application/json' \
  -d '{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},
       \"mappings\":{\"properties\":{
         \"order_id\":{\"type\":\"long\"},\"customer\":{\"type\":\"keyword\"},
         \"product\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"double\"},
         \"status\":{\"type\":\"keyword\"},
         \"created_at\":{\"type\":\"date\",\"format\":\"strict_date_optional_time\"}}}}' | \
  python3 -c \"import sys,json; r=json.load(sys.stdin); print('orders index:', r.get('acknowledged'))\"

# ── rbac-test-events index ─────────────────────────────────
curl -sk \$CERT \$KEY \$CA -X PUT \"\${OS}/rbac-test-events\" \
  -H 'Content-Type: application/json' \
  -d '{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},
       \"mappings\":{\"properties\":{
         \"event_id\":{\"type\":\"long\"},\"event_type\":{\"type\":\"keyword\"},
         \"user_id\":{\"type\":\"keyword\"},
         \"payload\":{\"type\":\"object\",\"enabled\":false},
         \"ts\":{\"type\":\"date\",\"format\":\"strict_date_optional_time\"}}}}' | \
  python3 -c \"import sys,json; r=json.load(sys.stdin); print('events index:', r.get('acknowledged'))\"

# ── rbac-test-products index ───────────────────────────────
curl -sk \$CERT \$KEY \$CA -X PUT \"\${OS}/rbac-test-products\" \
  -H 'Content-Type: application/json' \
  -d '{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},
       \"mappings\":{\"properties\":{
         \"product_id\":{\"type\":\"integer\"},
         \"name\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}},
         \"category\":{\"type\":\"keyword\"},\"price\":{\"type\":\"double\"}}}}' | \
  python3 -c \"import sys,json; r=json.load(sys.stdin); print('products index:', r.get('acknowledged'))\"
"
```

#### Bulk-index sample documents (admin)

```bash
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
CERT='--cert /usr/share/opensearch/config/tls/admin.pem'
KEY='--key  /usr/share/opensearch/config/tls/admin-key.pem'
CA='--cacert /usr/share/opensearch/config/tls/root-ca.pem'
OS='https://localhost:9200'

# Index 5 orders
printf '{\"index\":{\"_id\":\"1001\"}}\n{\"order_id\":1001,\"customer\":\"alice\",\"product\":\"Widget Alpha\",\"amount\":19.98,\"status\":\"completed\",\"created_at\":\"2025-01-10T09:00:00Z\"}\n{\"index\":{\"_id\":\"1002\"}}\n{\"order_id\":1002,\"customer\":\"bob\",\"product\":\"Gizmo Pro\",\"amount\":49.99,\"status\":\"shipped\",\"created_at\":\"2025-01-11T10:30:00Z\"}\n{\"index\":{\"_id\":\"1003\"}}\n{\"order_id\":1003,\"customer\":\"carol\",\"product\":\"Widget Beta\",\"amount\":14.99,\"status\":\"pending\",\"created_at\":\"2025-01-12T14:15:00Z\"}\n{\"index\":{\"_id\":\"1004\"}}\n{\"order_id\":1004,\"customer\":\"alice\",\"product\":\"Gizmo Lite\",\"amount\":24.99,\"status\":\"completed\",\"created_at\":\"2025-01-13T08:45:00Z\"}\n{\"index\":{\"_id\":\"1005\"}}\n{\"order_id\":1005,\"customer\":\"dave\",\"product\":\"Thingamajig\",\"amount\":5.00,\"status\":\"cancelled\",\"created_at\":\"2025-01-14T16:00:00Z\"}\n' | \
  curl -sk \$CERT \$KEY \$CA -X POST \"\${OS}/rbac-test-orders/_bulk\" \
    -H 'Content-Type: application/x-ndjson' --data-binary @- | \
  python3 -c \"import sys,json; r=json.load(sys.stdin); print(f'orders bulk: errors={r[\\\"errors\\\"]}, items={len(r[\\\"items\\\"])}')\"

# Index 5 products
printf '{\"index\":{\"_id\":\"1\"}}\n{\"product_id\":1,\"name\":\"Widget Alpha\",\"category\":\"Widgets\",\"price\":9.99}\n{\"index\":{\"_id\":\"2\"}}\n{\"product_id\":2,\"name\":\"Widget Beta\",\"category\":\"Widgets\",\"price\":14.99}\n{\"index\":{\"_id\":\"3\"}}\n{\"product_id\":3,\"name\":\"Gizmo Pro\",\"category\":\"Gizmos\",\"price\":49.99}\n{\"index\":{\"_id\":\"4\"}}\n{\"product_id\":4,\"name\":\"Gizmo Lite\",\"category\":\"Gizmos\",\"price\":24.99}\n{\"index\":{\"_id\":\"5\"}}\n{\"product_id\":5,\"name\":\"Thingamajig\",\"category\":\"Other\",\"price\":5.00}\n' | \
  curl -sk \$CERT \$KEY \$CA -X POST \"\${OS}/rbac-test-products/_bulk\" \
    -H 'Content-Type: application/x-ndjson' --data-binary @- | \
  python3 -c \"import sys,json; r=json.load(sys.stdin); print(f'products bulk: errors={r[\\\"errors\\\"]}, items={len(r[\\\"items\\\"])}')\"

# Verify document counts
for idx in rbac-test-orders rbac-test-products rbac-test-events; do
  count=\$(curl -sk \$CERT \$KEY \$CA \"\${OS}/\${idx}/_count\" | \
    python3 -c 'import sys,json; print(json.load(sys.stdin)[\"count\"])')
  echo \"\${idx}: \${count} documents\"
done
"
```

Expected:
```
rbac-test-orders: 5 documents
rbac-test-products: 5 documents
rbac-test-events: 0 documents
```

#### Test: `analyst` — INDEX_READ allowed, INDEX_WRITE blocked

The RBAC sync maps OpenSearch internal users to `rbac_*` roles. For these tests the
user connects through the RBAC-managed OpenSearch internal user credentials (password
set by the sync adapter), not via Kerberos SPNEGO.

```bash
# Get the password the sync adapter wrote for readonlyuser
# (stored in OpenSearch's internal users store — retrieve via admin API)
OS_READONLY_PASS=$(kubectl exec -n prod deploy/rbac-plane -- python3 -c "
import os; print(os.environ.get('OPENSEARCH_USER_DEFAULT_PASSWORD','ChangeMe1!'))
" 2>/dev/null)

# ✅ SEARCH — must succeed (INDEX_READ → rbac_index_read_all)
curl -sk -u ${READONLY}:${OS_READONLY_PASS} \
  "https://192.168.1.50:30920/rbac-test-orders/_search" \
  -H "Content-Type: application/json" \
  -d '{"query":{"match_all":{}},"size":3}' | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
print(f'hits: {r[\"hits\"][\"total\"][\"value\"]}')
for h in r['hits']['hits'][:3]:
    print(f'  {h[\"_source\"][\"customer\"]:10} — {h[\"_source\"][\"product\"]} — {h[\"_source\"][\"status\"]}')
"

# ✅ Term query by status — must succeed
curl -sk -u ${READONLY}:${OS_READONLY_PASS} \
  "https://192.168.1.50:30920/rbac-test-orders/_search" \
  -H "Content-Type: application/json" \
  -d '{"query":{"term":{"status":"completed"}},"_source":["order_id","customer","amount"]}' | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
for h in r['hits']['hits']:
    print(h['_source'])
"

# ❌ INDEX a new document — must be denied (analyst has no INDEX_WRITE)
curl -sk -u ${READONLY}:${OS_READONLY_PASS} -X POST \
  "https://192.168.1.50:30920/rbac-test-orders/_doc/9999" \
  -H "Content-Type: application/json" \
  -d '{"order_id":9999,"customer":"hacker","product":"none","amount":0,"status":"test"}' | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
print('status:', r.get('status'), '| error:', r.get('error',{}).get('type',''))
"
# Expected: status 403, security_exception

# ❌ DELETE index — must be denied (analyst has no INDEX_ADMIN)
curl -sk -o /dev/null -w "%{http_code}" \
  -u ${READONLY}:${OS_READONLY_PASS} \
  -X DELETE "https://192.168.1.50:30920/rbac-test-orders"
# Expected: 403
```

#### Test: `data_engineer` — INDEX_READ + INDEX_WRITE allowed, INDEX_ADMIN blocked

```bash
OS_TESTUSER_PASS=$(kubectl exec -n prod deploy/rbac-plane -- python3 -c "
import os; print(os.environ.get('OPENSEARCH_USER_DEFAULT_PASSWORD','ChangeMe1!'))
" 2>/dev/null)

# ✅ INDEX a new event document — must succeed
curl -sk -u ${TESTUSER}:${OS_TESTUSER_PASS} -X POST \
  "https://192.168.1.50:30920/rbac-test-events/_doc/1" \
  -H "Content-Type: application/json" \
  -d "{
    \"event_id\":1,
    \"event_type\":\"login\",
    \"user_id\":\"${TESTUSER}\",
    \"payload\":{\"ip\":\"10.0.0.9\"},
    \"ts\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
  }" | python3 -c "import sys,json; r=json.load(sys.stdin); print('result:', r.get('result'))"
# Expected: result: created

# ✅ BULK index — must succeed
curl -sk -u ${TESTUSER}:${OS_TESTUSER_PASS} -X POST \
  "https://192.168.1.50:30920/rbac-test-events/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  -d "
{\"index\":{\"_id\":\"2\"}}
{\"event_id\":2,\"event_type\":\"purchase\",\"user_id\":\"alice\",\"payload\":{\"order_id\":1001},\"ts\":\"2025-01-10T09:00:01Z\"}
{\"index\":{\"_id\":\"3\"}}
{\"event_id\":3,\"event_type\":\"logout\",\"user_id\":\"bob\",\"payload\":{\"session\":\"300s\"},\"ts\":\"2025-01-11T11:00:00Z\"}
" | python3 -c "import sys,json; r=json.load(sys.stdin); print(f'bulk: errors={r[\"errors\"]}, items={len(r[\"items\"])}')"

# ✅ Search across indexes — must succeed
curl -sk -u ${TESTUSER}:${OS_TESTUSER_PASS} \
  "https://192.168.1.50:30920/rbac-test-orders,rbac-test-events/_search" \
  -H "Content-Type: application/json" \
  -d '{"query":{"match_all":{}},"size":2}' | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
print(f'total hits: {r[\"hits\"][\"total\"][\"value\"]}')
"

# ❌ CREATE a new index — must be denied (data_engineer has no INDEX_ADMIN)
curl -sk -o /dev/null -w "%{http_code}" \
  -u ${TESTUSER}:${OS_TESTUSER_PASS} \
  -X PUT "https://192.168.1.50:30920/rbac-test-forbidden-index"
# Expected: 403
```

#### Aggregation query — test numeric analytics (analyst)

```bash
# ✅ Aggregation by status with sum of amount — must succeed for analyst
curl -sk -u ${READONLY}:${OS_READONLY_PASS} \
  "https://192.168.1.50:30920/rbac-test-orders/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "size": 0,
    "aggs": {
      "by_status": {
        "terms": { "field": "status" },
        "aggs": {
          "total_amount": { "sum": { "field": "amount" } }
        }
      }
    }
  }' | python3 -c "
import sys,json
r=json.load(sys.stdin)
for b in r['aggregations']['by_status']['buckets']:
    print(f'  {b[\"key\"]:12} count={b[\"doc_count\"]}  total=\${b[\"total_amount\"][\"value\"]:.2f}')
"
```

Expected:
```
  completed    count=2  total=$44.97
  cancelled    count=1  total=$5.00
  pending      count=1  total=$14.99
  shipped      count=1  total=$49.99
```

---

### Spark — Create sample Iceberg table via Polaris

> **Prerequisites:** The Polaris catalog must be accessible and the user must have a Polaris
> principal with `catalog_writer` assigned (see [section (i)](#i-polaris-catalog-grants-for-write_iceberg--admin_catalog)).

#### Create the test Iceberg table (admin / catalog_admin)

```bash
# Port-forward Polaris if needed (run in a separate terminal)
kubectl port-forward svc/polaris-rest -n prod 8181:8181 &
POLARIS_TOKEN=$(kubectl get secret polaris-credentials -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

# Create the rbac_test namespace in the polaris catalog if it doesn't exist
curl -s -X POST \
  -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  -H "Content-Type: application/json" \
  "http://localhost:8181/api/catalog/v1/polaris/namespaces" \
  -d '{"namespace":["rbac_test"],"properties":{"owner":"admin"}}' | \
  python3 -m json.tool
```

Now create the Iceberg table and seed data using `spark-shell`:

```bash
kubectl exec -it -n prod deploy/spark-master -- spark-shell \
  --conf "spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.polaris.type=rest" \
  --conf "spark.sql.catalog.polaris.uri=http://polaris-rest.prod.svc.cluster.local:8181/api/catalog" \
  --conf "spark.sql.catalog.polaris.warehouse=polaris" \
  --conf "spark.sql.catalog.polaris.credential=root:${POLARIS_TOKEN}" \
  << 'EOF'

// ── Create sample Iceberg tables ──────────────────────────

// orders table — mirrors the Doris sample data
spark.sql("""
  CREATE TABLE IF NOT EXISTS polaris.rbac_test.orders (
    order_id   BIGINT,
    customer   STRING,
    product    STRING,
    amount     DOUBLE,
    status     STRING,
    created_at TIMESTAMP
  ) USING iceberg
  PARTITIONED BY (status)
  TBLPROPERTIES ('write.format.default' = 'parquet')
""")

// events table — append-only event log
spark.sql("""
  CREATE TABLE IF NOT EXISTS polaris.rbac_test.events (
    event_id   BIGINT,
    event_type STRING,
    user_id    STRING,
    payload    STRING,
    ts         TIMESTAMP
  ) USING iceberg
  PARTITIONED BY (days(ts))
""")

// products lookup table
spark.sql("""
  CREATE TABLE IF NOT EXISTS polaris.rbac_test.products (
    product_id INT,
    name       STRING,
    category   STRING,
    price      DOUBLE
  ) USING iceberg
""")

// ── Seed sample data ──────────────────────────────────────

spark.sql("""
  INSERT INTO polaris.rbac_test.products VALUES
    (1, 'Widget Alpha', 'Widgets',  9.99),
    (2, 'Widget Beta',  'Widgets', 14.99),
    (3, 'Gizmo Pro',    'Gizmos',  49.99),
    (4, 'Gizmo Lite',   'Gizmos',  24.99),
    (5, 'Thingamajig',  'Other',    5.00)
""")

spark.sql("""
  INSERT INTO polaris.rbac_test.orders VALUES
    (1001, 'alice', 'Widget Alpha',  19.98, 'completed', TIMESTAMP '2025-01-10 09:00:00'),
    (1002, 'bob',   'Gizmo Pro',     49.99, 'shipped',   TIMESTAMP '2025-01-11 10:30:00'),
    (1003, 'carol', 'Widget Beta',   14.99, 'pending',   TIMESTAMP '2025-01-12 14:15:00'),
    (1004, 'alice', 'Gizmo Lite',    24.99, 'completed', TIMESTAMP '2025-01-13 08:45:00'),
    (1005, 'dave',  'Thingamajig',    5.00, 'cancelled', TIMESTAMP '2025-01-14 16:00:00')
""")

spark.sql("""
  INSERT INTO polaris.rbac_test.events VALUES
    (1, 'login',    'alice', '{"ip":"10.0.0.1"}',           TIMESTAMP '2025-01-10 08:59:00'),
    (2, 'purchase', 'alice', '{"order_id":1001}',            TIMESTAMP '2025-01-10 09:00:01'),
    (3, 'login',    'bob',   '{"ip":"10.0.0.2"}',            TIMESTAMP '2025-01-11 10:29:00'),
    (4, 'purchase', 'bob',   '{"order_id":1002}',            TIMESTAMP '2025-01-11 10:30:01'),
    (5, 'logout',   'carol', '{"session_duration":"120s"}',  TIMESTAMP '2025-01-12 14:20:00')
""")

// Verify
println("=== orders ===")
spark.sql("SELECT * FROM polaris.rbac_test.orders ORDER BY order_id").show()

println("=== products ===")
spark.sql("SELECT * FROM polaris.rbac_test.products ORDER BY product_id").show()

println("=== events ===")
spark.sql("SELECT * FROM polaris.rbac_test.events ORDER BY event_id").show()

EOF
```

#### Grant Polaris catalog roles to test users

```bash
# testuser (data_engineer) → catalog_writer (WRITE_ICEBERG)
curl -s -X PUT \
  -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  -H "Content-Type: application/json" \
  "http://localhost:8181/api/management/v1/principals/${TESTUSER}/principal-roles/catalog_writer" | \
  python3 -m json.tool

# readonlyuser (analyst) → catalog_viewer (USE_CATALOG read only)
curl -s -X PUT \
  -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  -H "Content-Type: application/json" \
  "http://localhost:8181/api/management/v1/principals/${READONLY}/principal-roles/catalog_viewer" | \
  python3 -m json.tool
```

#### Test: `analyst` (USE_CATALOG) — read-only Iceberg access

```bash
kubectl exec -it -n prod deploy/spark-master -- spark-shell \
  --conf "spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.polaris.type=rest" \
  --conf "spark.sql.catalog.polaris.uri=http://polaris-rest.prod.svc.cluster.local:8181/api/catalog" \
  --conf "spark.sql.catalog.polaris.warehouse=polaris" \
  --conf "spark.kerberos.principal=${READONLY}@STARDATADBLABS.LOCAL" \
  --conf "spark.kerberos.keytab=/etc/security/keytabs/${READONLY}.keytab" \
  << 'EOF'

// ✅ List namespaces — must succeed (catalog_viewer has CATALOG_READ)
spark.sql("SHOW NAMESPACES IN polaris").show()

// ✅ List tables — must succeed
spark.sql("SHOW TABLES IN polaris.rbac_test").show()

// ✅ SELECT from orders — must succeed
spark.sql("""
  SELECT status, COUNT(*) AS cnt, ROUND(SUM(amount),2) AS total
  FROM polaris.rbac_test.orders
  GROUP BY status
  ORDER BY total DESC
""").show()

// ✅ JOIN query — must succeed
spark.sql("""
  SELECT o.customer, p.category, SUM(o.amount) AS spend
  FROM polaris.rbac_test.orders o
  JOIN polaris.rbac_test.products p ON o.product = p.name
  GROUP BY o.customer, p.category
  ORDER BY spend DESC
""").show()

// ❌ INSERT — must fail (catalog_viewer has no TABLE_WRITE_DATA)
try {
  spark.sql("INSERT INTO polaris.rbac_test.orders VALUES (9999,'x','y',1.0,'test',current_timestamp())")
  println("ERROR: INSERT should have been denied")
} catch {
  case e: Exception => println(s"✅ INSERT correctly blocked: ${e.getMessage.take(120)}")
}

// ❌ DROP TABLE — must fail
try {
  spark.sql("DROP TABLE polaris.rbac_test.orders")
  println("ERROR: DROP should have been denied")
} catch {
  case e: Exception => println(s"✅ DROP correctly blocked: ${e.getMessage.take(120)}")
}

EOF
```

#### Test: `data_engineer` (WRITE_ICEBERG) — read + write Iceberg access

```bash
kubectl exec -it -n prod deploy/spark-master -- spark-shell \
  --conf "spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.polaris.type=rest" \
  --conf "spark.sql.catalog.polaris.uri=http://polaris-rest.prod.svc.cluster.local:8181/api/catalog" \
  --conf "spark.sql.catalog.polaris.warehouse=polaris" \
  --conf "spark.kerberos.principal=${TESTUSER}@STARDATADBLABS.LOCAL" \
  --conf "spark.kerberos.keytab=/etc/security/keytabs/${TESTUSER}.keytab" \
  << 'EOF'

// ✅ INSERT a new order — must succeed (catalog_writer has TABLE_WRITE_DATA)
spark.sql("""
  INSERT INTO polaris.rbac_test.orders
  VALUES (1006, 'eve', 'Widget Alpha', 9.99, 'pending', TIMESTAMP '2025-01-15 11:00:00')
""")
println("✅ INSERT succeeded")

// ✅ Read back — confirm new row appears
spark.sql("SELECT * FROM polaris.rbac_test.orders WHERE order_id = 1006").show()

// ✅ CREATE a temporary test table — must succeed
spark.sql("""
  CREATE TABLE IF NOT EXISTS polaris.rbac_test.rbac_write_test (
    id   BIGINT,
    msg  STRING
  ) USING iceberg
""")
spark.sql("INSERT INTO polaris.rbac_test.rbac_write_test VALUES (1, 'hello rbac')")
spark.sql("SELECT * FROM polaris.rbac_test.rbac_write_test").show()

// ✅ Clean up the test table
spark.sql("DROP TABLE polaris.rbac_test.rbac_write_test")
println("✅ Cleanup complete")

// ❌ DROP core sample table — must fail (catalog_writer cannot drop namespace tables
//    without CATALOG_MANAGE_CONTENT — only catalog_admin can do this)
try {
  spark.sql("DROP TABLE polaris.rbac_test.orders")
  println("ERROR: DROP should have been denied for catalog_writer")
} catch {
  case e: Exception => println(s"✅ DROP of core table correctly blocked: ${e.getMessage.take(120)}")
}

EOF
```

---

### Summary — Expected permission outcomes per role

| Test | `analyst` (`readonlyuser`) | `data_engineer` (`testuser`) |
|---|---|---|
| **Doris** SELECT `rbac_test.orders` | ✅ allowed | ✅ allowed |
| **Doris** INSERT into `rbac_test.orders` | ❌ blocked (no LOAD_PRIV) | ✅ allowed |
| **Doris** DROP TABLE | ❌ blocked | ❌ blocked |
| **Kafka** CONSUME `rbac-test-orders` | ✅ allowed (SCRAM auth) | ✅ allowed |
| **Kafka** PRODUCE to `rbac-test-events` | SCRAM auth only* | ✅ allowed |
| **OpenSearch** SEARCH `rbac-test-orders` | ✅ allowed (INDEX_READ) | ✅ allowed |
| **OpenSearch** INDEX to `rbac-test-events` | ❌ blocked (403) | ✅ allowed (INDEX_WRITE) |
| **OpenSearch** CREATE index | ❌ blocked | ❌ blocked |
| **Spark / Iceberg** SELECT `polaris.rbac_test.orders` | ✅ allowed (USE_CATALOG) | ✅ allowed |
| **Spark / Iceberg** INSERT into orders | ❌ blocked (no TABLE_WRITE_DATA) | ✅ allowed (WRITE_ICEBERG) |
| **Spark / Iceberg** DROP core table | ❌ blocked | ❌ blocked |

> \* Kafka broker-level ACL enforcement is disabled (`allow.everyone.if.no.acl.found=true`).
> SCRAM-SHA-512 authentication via the KafkaUser CR is the enforced boundary.
> PRODUCE/CONSUME distinctions are tracked in the RBAC plane for future ACL activation.

### Cleanup — Remove sample test data

```bash
# Doris
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" 2>/dev/null \
  -e "DROP DATABASE IF EXISTS rbac_test;"

# Kafka topics
for TOPIC in rbac-test-orders rbac-test-events rbac-test-products; do
  kubectl exec -n prod deploy/kafka-admin -- \
    kafka-topics.sh \
      --bootstrap-server ${KAFKA_BOOTSTRAP} \
      --command-config /opt/kafka/config/admin.properties \
      --delete --topic ${TOPIC} \
    && echo "Deleted topic: ${TOPIC}"
done

# OpenSearch indexes
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
CERT='--cert /usr/share/opensearch/config/tls/admin.pem'
KEY='--key  /usr/share/opensearch/config/tls/admin-key.pem'
CA='--cacert /usr/share/opensearch/config/tls/root-ca.pem'
OS='https://localhost:9200'
for IDX in rbac-test-orders rbac-test-events rbac-test-products; do
  curl -sk \$CERT \$KEY \$CA -X DELETE \"\${OS}/\${IDX}\" | \
    python3 -c \"import sys,json; r=json.load(sys.stdin); print(f'deleted \${IDX}: {r.get(\\\"acknowledged\\\")}')\"
done
"

# Spark / Polaris Iceberg tables
kubectl exec -n prod deploy/spark-master -- spark-shell \
  --conf "spark.sql.catalog.polaris.uri=http://polaris-rest.prod.svc.cluster.local:8181/api/catalog" \
  << 'EOF'
spark.sql("DROP TABLE IF EXISTS polaris.rbac_test.orders")
spark.sql("DROP TABLE IF EXISTS polaris.rbac_test.events")
spark.sql("DROP TABLE IF EXISTS polaris.rbac_test.products")
spark.sql("DROP NAMESPACE IF EXISTS polaris.rbac_test")
println("Iceberg tables cleaned up")
EOF

# Revoke Polaris roles
for USER in ${TESTUSER} ${READONLY}; do
  for ROLE in catalog_writer catalog_viewer; do
    curl -s -X DELETE \
      -H "Authorization: Bearer ${POLARIS_TOKEN}" \
      "http://localhost:8181/api/management/v1/principals/${USER}/principal-roles/${ROLE}" > /dev/null
  done
done
echo "Polaris roles revoked"

# Optionally remove test RBAC users
for U in ${TESTUSER} ${READONLY}; do
  curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
    "${RBAC_URL}/api/v1/users/${U}" > /dev/null && echo "Removed RBAC user: ${U}"
  mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" 2>/dev/null \
    -e "DROP USER IF EXISTS '${U}'@'%';"
  kubectl delete kafkauser ${U} -n prod --ignore-not-found
  kubectl exec -n prod deploy/kerberos-kdc -- \
    kadmin.local -q "delprinc -force ${U}@${REALM}" 2>/dev/null
done
echo "Test users removed"
```

---

## (k) Bulk Binding — Adding Multiple Users to a Role in One Call

> **API endpoint:** `POST /api/v1/users/roles/{role_name}/members`
> **CLI command:** `rbacctl role add-members <role_name> <user1> <user2> ...`
>
> Use bulk binding when you need to assign several already-registered users to the
> same role in a single operation. Each user in the list is processed independently —
> unknown usernames and duplicate bindings are reported individually rather than
> aborting the whole batch.
>
> **Prerequisites:** Every username in the list must already be registered in the RBAC
> plane (`POST /api/v1/users`). KDC principals and Doris SQL users must also exist
> before syncing — see [section (a)](#a-adding-a-new-user-to-a-group).

---

### API reference

```
POST /api/v1/users/roles/{role_name}/members
Authorization: Bearer <token>
Content-Type: application/json

{
  "usernames":    ["alice", "bob", "carol"],   // required — list of usernames
  "service_name": "spark",                     // optional — restrict to one service
  "expires_at":   "2026-12-31T00:00:00Z"       // optional — auto-expire all new bindings
}
```

**Response:**
```json
{
  "results": [
    {"username": "alice", "status": "bound",          "binding_id": 42},
    {"username": "bob",   "status": "already_exists", "binding_id": null},
    {"username": "carol", "status": "user_not_found", "binding_id": null}
  ],
  "bound":   1,
  "skipped": 1,
  "errors":  1
}
```

| `status` | Meaning |
|---|---|
| `bound` | New binding created successfully. `binding_id` is the new binding's ID. |
| `already_exists` | The user already had this role binding — skipped, not an error. |
| `user_not_found` | Username not registered in the RBAC plane. Fix: register the user first. |

---

### CLI reference

```bash
# Syntax
rbacctl role add-members <role_name> <user1> [user2 ...] [--service <svc>] [--expires-days <n>]

# Options
#   --service,      -s   Restrict all new bindings to one service
#   --expires-days, -d   Auto-expire all new bindings after N days
```

---

### Test scenario — bind three users to `data_engineer` at once

> **Setup:** `alice`, `bob`, and `carol` are already registered in the RBAC plane
> and have KDC principals + Doris SQL users. `dave` is not registered.

#### Step 1 — Prerequisites check

```bash
export RBAC_URL="http://192.168.1.50:30850"
export RBAC_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)

# Confirm the users exist in the RBAC plane
for U in alice bob carol; do
  curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
    "${RBAC_URL}/api/v1/users/${U}" | \
    python3 -c "import sys,json; u=json.load(sys.stdin); print(f'{u[\"username\"]:10} enabled={u[\"enabled\"]}')" \
    2>/dev/null || echo "${U}: NOT FOUND"
done
```

Expected:
```
alice      enabled=True
bob        enabled=True
carol      enabled=True
```

#### Step 2 — Bulk bind via API

```bash
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"usernames":["alice","bob","carol","dave"]}' \
  "${RBAC_URL}/api/v1/users/roles/data_engineer/members" | python3 -m json.tool
```

Expected response (`dave` is not registered, so it surfaces as `user_not_found`):
```json
{
  "results": [
    {"username": "alice", "status": "bound",          "binding_id": 10},
    {"username": "bob",   "status": "bound",          "binding_id": 11},
    {"username": "carol", "status": "already_exists", "binding_id": null},
    {"username": "dave",  "status": "user_not_found", "binding_id": null}
  ],
  "bound":   2,
  "skipped": 1,
  "errors":  1
}
```

> `carol` already had `data_engineer` — the duplicate is skipped, not an error.
> `dave` must be registered first; re-run after fixing.

#### Step 2 (alternative) — Bulk bind via CLI

```bash
rbacctl role add-members data_engineer alice bob carol dave
```

Expected CLI output:
```
     Bulk bind → data_engineer
 username │ status        │ binding_id
──────────┼───────────────┼───────────
 alice    │ bound         │ 10
 bob      │ bound         │ 11
 carol    │ already_exists│ —
 dave     │ user_not_found│ —

bound 2  skipped 1  errors 1
```

> The CLI exits with code `1` when any username is `user_not_found`, so CI scripts
> can detect incomplete binds.

#### Step 3 — Verify bindings

```bash
# Check that alice and bob now have the data_engineer role
for U in alice bob; do
  echo "=== ${U} ==="
  rbacctl user bindings ${U}
done
```

#### Step 4 — Sync the newly bound users to all services

```bash
# Sync alice and bob individually (targeted)
rbacctl sync run --user alice
rbacctl sync run --user bob

# Or sync the whole role's permission set to all services at once
rbacctl sync run
```

#### Step 5 — Verify sync results per service

**Doris:**
```bash
for U in alice bob; do
  mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
    -e "SHOW GRANTS FOR '${U}'@'%';" 2>/dev/null | grep "CatalogPrivs"
done
# Expected: CatalogPrivs: Select_priv, Load_priv  (for each user)
```

**Kafka:**
```bash
for U in alice bob; do
  kubectl get kafkauser ${U} -n prod
done
# Expected: READY=True for each user
```

**OpenSearch:**
```bash
for U in alice bob; do
  kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
    curl -sk \
      --cert /usr/share/opensearch/config/tls/admin.pem \
      --key  /usr/share/opensearch/config/tls/admin-key.pem \
      --cacert /usr/share/opensearch/config/tls/root-ca.pem \
      'https://localhost:9200/_plugins/_security/api/rolesmapping' | \
    python3 -c \"
import sys,json
roles=[r for r,m in json.load(sys.stdin).items() if '${U}' in m.get('users',[])]
print('${U} roles:', roles)
\"
  "
done
# Expected: rbac_index_read_all, rbac_index_write_all, rbac_cluster_read_all
```

**Spark:**
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "
import sys,json
al=json.load(sys.stdin)
for u in ['alice','bob']:
    print(f'{u}: {al.get(u, \"NOT FOUND\")}')
"
# Expected for each user:
# {"can_admin_catalog": false, "can_kill_any": false, "can_submit": true,
#  "can_use_catalog": true, "can_write_iceberg": true, "view_ui": true}
```

---

### Bulk bind with service scope (Spark only)

> Use `--service` / `service_name` to restrict new bindings to a single service.
> Useful for granting a temporary Spark permission without affecting Doris, Kafka, or OpenSearch.

```bash
# API — grant VIEW_UI on Spark only, expiring in 30 days
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"usernames\":    [\"alice\",\"bob\"],
    \"service_name\": \"spark\",
    \"expires_at\":   \"$(date -u -d '+30 days' '+%Y-%m-%dT%H:%M:%SZ')\"
  }" \
  "${RBAC_URL}/api/v1/users/roles/analyst/members" | python3 -m json.tool

# CLI equivalent
rbacctl role add-members analyst alice bob --service spark --expires-days 30

# Sync Spark only
rbacctl sync run --service spark
```

---

### Negative test — `user_not_found`

> **Expected:** a username that is not registered returns `user_not_found` in the
> results. The rest of the batch is unaffected.

```bash
# ghost is not registered in the RBAC plane
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"usernames":["alice","ghost"]}' \
  "${RBAC_URL}/api/v1/users/roles/data_engineer/members" | \
  python3 -c "
import sys,json
r=json.load(sys.stdin)
for res in r['results']:
    print(f'{res[\"username\"]:10} → {res[\"status\"]}')
print(f'bound={r[\"bound\"]} skipped={r[\"skipped\"]} errors={r[\"errors\"]}')
"
```

Expected:
```
alice      → already_exists
ghost      → user_not_found
bound=0 skipped=1 errors=1
```

> `alice` was already bound — `already_exists` is not counted as an error in the
> `errors` counter. Only `user_not_found` increments `errors`.

---

## (l) Removing a User from a Role or from the Platform

> This section covers three distinct removal scenarios — pick the one that matches your situation:
>
> | Scenario | What it does | When to use |
> |---|---|---|
> | [A — Remove from one role](#scenario-a--remove-a-user-from-one-role-all-services) | Deletes the role binding; sync revokes access on all services | User moves to a different team / role |
> | [B — Remove from one service only](#scenario-b--remove-a-user-from-one-service-only) | Deletes the service-scoped binding; sync revokes access on that service alone | Withdraw Spark access while keeping Doris/Kafka/OpenSearch |
> | [C — Full offboard](#scenario-c--full-offboard-remove-user-from-the-platform-entirely) | Removes all bindings, syncs revoke, deletes user, removes KDC principal and Doris SQL user | Leaver / account decommission |
>
> **Key principle:** deleting a binding from the RBAC plane does **not** immediately revoke
> downstream access. You must always follow with a sync (`rbacctl sync run --user <username>`)
> to push the removal to the services.

---

### Scenario A — Remove a user from one role (all services)

> **Example:** `carol` is leaving the `data_engineer` group and moving to `analyst`.
> This removes the `data_engineer` binding so the sync will revoke those grants on
> every service, then a new `analyst` binding is added.

#### Step 1 — Find the binding ID to remove

```bash
USERNAME="carol"

# CLI
rbacctl user bindings ${USERNAME}
```

Expected output:
```
         Bindings for carol
 id │ role_name     │ service_name │ granted_by │ granted_at          │ expires_at
────┼───────────────┼──────────────┼────────────┼─────────────────────┼───────────
 3  │ data_engineer │              │ master     │ 2026-01-10T09:00:00 │
```

```bash
# API equivalent
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings" | python3 -m json.tool
```

#### Step 2 — Delete the binding

```bash
BINDING_ID=3   # from step 1

# CLI
rbacctl user unbind ${USERNAME} ${BINDING_ID}

# API equivalent
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings/${BINDING_ID}" | python3 -m json.tool
# Expected: {"ok": true, "message": "Binding 3 removed"}
```

#### Step 3 — Sync to revoke access on all services

```bash
# CLI
rbacctl sync run --user ${USERNAME}

# API equivalent
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

Expected sync output — `carol` has no remaining bindings, so all four services
revoke to zero permissions:
```json
{
  "results": [
    {"username": "carol", "service": "doris",      "status": "synced", "detail": "0 permissions applied"},
    {"username": "carol", "service": "kafka",      "status": "synced", "detail": "0 permissions applied"},
    {"username": "carol", "service": "opensearch", "status": "synced", "detail": "0 permissions applied"},
    {"username": "carol", "service": "spark",      "status": "synced", "detail": "0 permissions applied"}
  ],
  "errors": 0
}
```

#### Step 4 — Verify revocation per service

**Doris — grants should be empty:**
```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR 'carol'@'%';" 2>/dev/null
# Expected: no CatalogPrivs rows (or only empty GlobalPrivs)
```

**Kafka — KafkaUser CR deleted:**
```bash
kubectl get kafkauser carol -n prod
# Expected: Error from server (NotFound): kafkausers.kafka.strimzi.io "carol" not found
```

**OpenSearch — removed from all role mappings:**
```bash
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  curl -sk \
    --cert /usr/share/opensearch/config/tls/admin.pem \
    --key  /usr/share/opensearch/config/tls/admin-key.pem \
    --cacert /usr/share/opensearch/config/tls/root-ca.pem \
    'https://localhost:9200/_plugins/_security/api/rolesmapping' | \
  python3 -c \"
import sys,json
roles=[r for r,m in json.load(sys.stdin).items() if 'carol' in m.get('users',[])]
print('carol roles:', roles)
\"
"
# Expected: carol roles: []
```

**Spark — removed from allowlist:**
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print('carol' in json.load(sys.stdin))"
# Expected: False
```

#### Step 5 (optional) — Bind the new role

```bash
# CLI
rbacctl user bind ${USERNAME} analyst

# API equivalent
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"role_name":"analyst"}' \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings" | python3 -m json.tool

# Sync the new role to all services
rbacctl sync run --user ${USERNAME}
```

---

### Scenario B — Remove a user from one service only

> **Example:** `bob` (`platform_admin`) should lose Spark access temporarily while
> keeping full access on Doris, Kafka, and OpenSearch.
>
> The mechanism is a **service-scoped binding**. If `bob` already has an
> all-services binding, you cannot partially revoke it — instead, you:
> 1. Delete the all-services binding.
> 2. Re-add three service-specific bindings (doris, kafka, opensearch).
> 3. Sync.
>
> If `bob` was originally onboarded with per-service bindings you can simply delete
> the Spark-scoped one.

#### Identify the current binding structure

```bash
USERNAME="bob"

rbacctl user bindings ${USERNAME}
```

**Case 1 — bob has a single all-services binding (most common)**

```
 id │ role_name      │ service_name │ ...
────┼────────────────┼──────────────┼────
 7  │ platform_admin │              │ ...
```

```bash
# 1. Remove the all-services binding
rbacctl user unbind ${USERNAME} 7

# 2. Re-add one binding per service — omitting spark
for SVC in doris kafka opensearch; do
  curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"role_name\":\"platform_admin\",\"service_name\":\"${SVC}\"}" \
    "${RBAC_URL}/api/v1/users/${USERNAME}/bindings" | \
    python3 -c "import sys,json; b=json.load(sys.stdin); print(f'bound {b[\"role_name\"]} → {b[\"service_name\"]} (id={b[\"id\"]})')"
done

# 3. Sync — Spark will now receive 0 permissions and remove the allowlist entry;
#    doris/kafka/opensearch remain fully intact (hash unchanged → skipped)
rbacctl sync run --user ${USERNAME}
```

Expected sync result:
```json
{
  "results": [
    {"username": "bob", "service": "doris",      "status": "skipped",  "detail": "no change"},
    {"username": "bob", "service": "kafka",      "status": "skipped",  "detail": "no change"},
    {"username": "bob", "service": "opensearch", "status": "skipped",  "detail": "no change"},
    {"username": "bob", "service": "spark",      "status": "synced",   "detail": "0 permissions applied"}
  ],
  "errors": 0
}
```

**Case 2 — bob has a Spark-scoped binding**

```
 id │ role_name      │ service_name │ ...
────┼────────────────┼──────────────┼────
 7  │ platform_admin │              │ ...   ← all-services
 12 │ analyst        │ spark        │ ...   ← spark-only (the extra one to remove)
```

```bash
# Simply delete the spark-scoped binding and sync
rbacctl user unbind ${USERNAME} 12
rbacctl sync run --user ${USERNAME} --service spark
```

#### Verify Spark access is revoked

```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print('bob' in json.load(sys.stdin))"
# Expected: False

# Doris should still be intact
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR 'bob'@'%';" 2>/dev/null | grep "Admin_priv"
# Expected: GlobalPrivs: Admin_priv  (unchanged)

# Kafka should still be intact
kubectl get kafkauser bob -n prod
# Expected: READY=True  (unchanged)
```

#### Restore Spark access later

```bash
# Re-add spark to the all-services binding (simplest: delete the 3 scoped bindings, re-add one all-services binding)
# OR add a Spark-scoped binding directly:
rbacctl user bind ${USERNAME} platform_admin --service spark
rbacctl sync run --user ${USERNAME} --service spark
```

---

### Scenario C — Full offboard (remove user from the platform entirely)

> **Example:** `carol` is leaving the organisation.
> This removes her from every service and cleans up all related infrastructure.

> ⚠️ **Order matters.** Always sync (revoke downstream) *before* deleting the RBAC
> user record. Deleting the user first leaves orphaned grants on Doris, OpenSearch,
> and Spark.

```bash
USERNAME="carol"
REALM="STARDATADBLABS.LOCAL"
export DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
```

#### Step 1 — Preview what will be revoked (dry run)

```bash
rbacctl sync run --user ${USERNAME} --dry-run
```

Expected — shows every service that currently has grants for `carol`:
```json
{
  "results": [
    {"username": "carol", "service": "doris",      "status": "dry_run", "detail": "would apply 0 permissions"},
    {"username": "carol", "service": "kafka",      "status": "dry_run", "detail": "would apply 0 permissions"},
    {"username": "carol", "service": "opensearch", "status": "dry_run", "detail": "would apply 0 permissions"},
    {"username": "carol", "service": "spark",      "status": "dry_run", "detail": "would apply 0 permissions"}
  ],
  "errors": 0
}
```

#### Step 2 — Remove all RBAC bindings

```bash
# List all bindings
rbacctl user bindings ${USERNAME}

# Remove each binding by ID (repeat for every row)
rbacctl user unbind ${USERNAME} <binding_id>

# API: list + delete in one shot
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings" | \
  python3 -c "import sys,json; [print(b['id']) for b in json.load(sys.stdin)]" | \
  while read bid; do
    curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
      "${RBAC_URL}/api/v1/users/${USERNAME}/bindings/${bid}" > /dev/null
    echo "Removed binding ${bid}"
  done
```

#### Step 3 — Sync to push zero-permission state to all services

```bash
rbacctl sync run --user ${USERNAME}
```

Expected — all four services show `synced` with `0 permissions applied`:
```json
{
  "results": [
    {"username": "carol", "service": "doris",      "status": "synced", "detail": "0 permissions applied"},
    {"username": "carol", "service": "kafka",      "status": "synced", "detail": "0 permissions applied"},
    {"username": "carol", "service": "opensearch", "status": "synced", "detail": "0 permissions applied"},
    {"username": "carol", "service": "spark",      "status": "synced", "detail": "0 permissions applied"}
  ],
  "errors": 0
}
```

#### Step 4 — Verify revocation on each service

**Doris:**
```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR 'carol'@'%';" 2>/dev/null
# Expected: empty — no CatalogPrivs rows
```

**Kafka:**
```bash
kubectl get kafkauser ${USERNAME} -n prod
# Expected: NotFound
```

**OpenSearch:**
```bash
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  curl -sk \
    --cert /usr/share/opensearch/config/tls/admin.pem \
    --key  /usr/share/opensearch/config/tls/admin-key.pem \
    --cacert /usr/share/opensearch/config/tls/root-ca.pem \
    'https://localhost:9200/_plugins/_security/api/rolesmapping' | \
  python3 -c \"
import sys,json
roles=[r for r,m in json.load(sys.stdin).items() if '${USERNAME}' in m.get('users',[])]
print('${USERNAME} roles:', roles)
\"
"
# Expected: carol roles: []
```

**Spark:**
```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "import sys,json; print('${USERNAME}' in json.load(sys.stdin))"
# Expected: False
```

#### Step 5 — Delete the RBAC user record

```bash
# CLI
rbacctl user delete ${USERNAME} --yes

# API equivalent
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/${USERNAME}" | python3 -m json.tool
# Expected: {"ok": true, "message": "User 'carol' deleted"}
```

#### Step 6 — Remove the Doris SQL user

```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "DROP USER IF EXISTS '${USERNAME}'@'%';"

# Verify
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SELECT user FROM mysql.user WHERE user='${USERNAME}';" 2>/dev/null
# Expected: Empty set
```

#### Step 7 — Delete the Kerberos principal

```bash
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "delprinc -force ${USERNAME}@${REALM}"

# Verify
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc ${USERNAME}@${REALM}" 2>/dev/null | grep "Principal:"
# Expected: no output (principal does not exist)
```

#### Step 8 — Remove the keytab secret (if one was created)

```bash
kubectl delete secret ${USERNAME}-keytab -n prod --ignore-not-found
# Expected: secret "carol-keytab" deleted  (or: not found — both are fine)
```

#### Step 9 — Confirm the user is gone from the RBAC plane

```bash
rbacctl user list | grep ${USERNAME}
# Expected: no output

# API
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/${USERNAME}"
# Expected: {"detail": "User 'carol' not found"}  (HTTP 404)
```

---

### Quick-reference — removal commands

| Goal | CLI | API |
|---|---|---|
| List a user's bindings | `rbacctl user bindings <user>` | `GET /api/v1/users/<user>/bindings` |
| Remove one binding | `rbacctl user unbind <user> <binding_id>` | `DELETE /api/v1/users/<user>/bindings/<id>` |
| Sync revoke to all services | `rbacctl sync run --user <user>` | `POST /api/v1/sync` `{"username":"<user>"}` |
| Sync revoke to one service | `rbacctl sync run --user <user> --service <svc>` | `POST /api/v1/sync` `{"username":"<user>","service":"<svc>"}` |
| Disable user (keep bindings) | `rbacctl user disable <user>` | `PATCH /api/v1/users/<user>` `{"enabled":false}` |
| Delete user (full removal) | `rbacctl user delete <user> --yes` | `DELETE /api/v1/users/<user>` |

> **Disable vs Delete:** `rbacctl user disable` keeps all bindings and the user record intact but
> invalidates the cache so the guards treat the user as unauthorised immediately. Use this for
> temporary suspension or maternity/parental leave. Use `delete` only for permanent offboarding.

---

## (m) Inspecting Users, Roles, and Service Scope

> Use this section to answer three operational questions:
>
> | Question | Section |
> |---|---|
> | What roles and service scope does a specific user have? | [Test 1](#test-1--inspect-one-user) |
> | What exact permissions does a user have on each service? | [Test 2](#test-2--effective-permissions-for-one-user) |
> | Who is in a given role, and what service are they scoped to? | [Test 3](#test-3--list-all-members-of-a-role) |
> | What does the service_name column mean? | [Reading the output](#reading-the-service_name-column) |
>
> **No changes are made by any command in this section** — all reads are safe to run at any time.

```bash
# Prerequisites — set once for this section
export RBAC_URL="http://192.168.1.50:30850"
export RBAC_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)
```

---

### Test 1 — Inspect one user

> **What roles does the user have, and are those roles scoped to all services or just one?**

#### CLI

```bash
rbacctl user bindings bob
```

Expected output — `bob` has a single all-services binding:
```
        Bindings for bob
 id │ role_name      │ service_name │ granted_by │ granted_at          │ expires_at
────┼────────────────┼──────────────┼────────────┼─────────────────────┼───────────
 7  │ platform_admin │              │ master     │ 2026-01-10T09:00:00 │
```

The `service_name` column is **blank** — the role applies to all four services.

Compare with a user who has per-service bindings (e.g. after a Spark-only removal per section (l) Scenario B):
```
 id │ role_name      │ service_name │ granted_by │ granted_at          │ expires_at
────┼────────────────┼──────────────┼────────────┼─────────────────────┼───────────
 8  │ platform_admin │ doris        │ master     │ 2026-01-10T09:00:00 │
 9  │ platform_admin │ kafka        │ master     │ 2026-01-10T09:00:00 │
 10 │ platform_admin │ opensearch   │ master     │ 2026-01-10T09:00:00 │
```

Here `bob` has **three separate scoped bindings** — Spark was deliberately removed.

#### API equivalent

```bash
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/bob/bindings" | \
  python3 -c "
import sys, json
for b in json.load(sys.stdin):
    svc = b['service_name'] or 'ALL SERVICES'
    exp = b['expires_at'] or 'never'
    print(f'  id={b[\"id\"]}  role={b[\"role_name\"]:20} scope={svc:15} expires={exp}')
"
```

---

### Test 2 — Effective permissions for one user

> **What exact permissions does the user have, broken down by service?**
> This resolves all bindings (including scoped ones and expiry) into a flat list.

#### CLI

```bash
rbacctl user roles bob
```

Expected output for `bob` (`platform_admin`, all services):
```
bob
  Roles: platform_admin

     Effective Permissions
 service     │ permission        │ resource_scope
─────────────┼───────────────────┼───────────────
 doris        │ SELECT            │ {}
 doris        │ INSERT            │ {}
 doris        │ ADMIN             │ {}
 ...
 kafka        │ PRODUCE           │ {}
 kafka        │ ADMIN             │ {}
 ...
 opensearch   │ INDEX_READ        │ {}
 opensearch   │ CLUSTER_ADMIN     │ {}
 ...
 spark        │ SUBMIT_JOB        │ {}
 spark        │ KILL_ANY_JOB      │ {}
 spark        │ ADMIN_CATALOG     │ {}
 ...
```

If a user is **scoped to Spark only**, only `spark` rows will appear:
```
carol  (cached)
  Roles: analyst

     Effective Permissions
 service │ permission  │ resource_scope
─────────┼─────────────┼───────────────
 spark    │ VIEW_UI     │ {}
```

> The `(cached)` label means the result was served from the in-process/Redis cache.
> This is the same response the Spark/Doris/Kafka guards receive on the hot path.

#### API equivalent

```bash
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/bob/roles" | \
  python3 -c "
import sys, json
data = json.load(sys.stdin)
print('Roles:', ', '.join(data['roles']))
print('Cached:', data['cached'])
svc = {}
for p in data['permissions']:
    svc.setdefault(p['service'], []).append(p['permission'])
for s in sorted(svc):
    print(f'  {s}: {sorted(svc[s])}')
"
```

Expected:
```
Roles: platform_admin
Cached: False
  doris: ['ADMIN', 'ALTER', 'CREATE', 'DELETE', 'DROP', 'GRANT', 'INSERT', 'LOAD', 'NODE', 'SELECT', 'SHOW_VIEW', 'UPDATE']
  kafka: ['ADMIN', 'CDC_CONNECT', 'CONSUME', 'CONSUMER_GROUP_MANAGE', 'CREATE_TOPIC', 'DELETE_TOPIC', 'DESCRIBE', 'PRODUCE', 'SCHEMA_REGISTRY_READ', 'SCHEMA_REGISTRY_WRITE', 'TRANSACTIONAL_WRITE']
  opensearch: ['CLUSTER_ADMIN', 'CLUSTER_READ', 'INDEX_ADMIN', 'INDEX_READ', 'INDEX_WRITE']
  spark: ['ADMIN_CATALOG', 'KILL_ANY_JOB', 'KILL_OWN_JOB', 'SUBMIT_JOB', 'USE_CATALOG', 'VIEW_UI', 'WRITE_ICEBERG']
```

#### Check a specific service only

```bash
# Only show spark permissions for bob
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/bob/roles" | \
  python3 -c "
import sys, json
perms = [p for p in json.load(sys.stdin)['permissions'] if p['service'] == 'spark']
print('Spark permissions:', sorted(p['permission'] for p in perms))
"
# Expected: ['ADMIN_CATALOG', 'KILL_ANY_JOB', 'KILL_OWN_JOB', 'SUBMIT_JOB', 'USE_CATALOG', 'VIEW_UI', 'WRITE_ICEBERG']
```

---

### Test 3 — List all members of a role

> **Who is in the `data_engineer` role, and what service scope does each binding have?**

#### CLI + API (loop over all users)

```bash
ROLE="data_engineer"

curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" "${RBAC_URL}/api/v1/users" | \
  python3 -c "import sys,json; [print(u['username']) for u in json.load(sys.stdin)]" | \
  while read u; do
    curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
      "${RBAC_URL}/api/v1/users/${u}/bindings" | \
    python3 -c "
import sys, json
for b in json.load(sys.stdin):
    if b['role_name'] == '${ROLE}':
        svc = b['service_name'] or 'all services'
        exp = b['expires_at'] or 'never'
        print(f'  {b[\"username\"]:14} scope={svc:15} expires={exp}')
" 2>/dev/null
  done
```

Expected output when `alice` and `carol` are in `data_engineer` (all services):
```
  alice          scope=all services   expires=never
  carol          scope=all services   expires=never
```

#### Snapshot of all users and all their roles at once

```bash
# Full platform view — every user with every role and service scope
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" "${RBAC_URL}/api/v1/users" | \
  python3 -c "import sys,json; [print(u['username']) for u in json.load(sys.stdin)]" | \
  while read u; do
    curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
      "${RBAC_URL}/api/v1/users/${u}/bindings" | \
    python3 -c "
import sys, json
rows = json.load(sys.stdin)
if not rows:
    print(f'  ${u}: no bindings')
for b in rows:
    svc = b['service_name'] or 'ALL SERVICES'
    exp = b['expires_at'] or 'never'
    print(f'  ${u:14} role={b[\"role_name\"]:20} scope={svc:15} expires={exp}')
" 2>/dev/null
  done
```

Example output for a platform with four users:
```
  alice          role=data_engineer     scope=ALL SERVICES   expires=never
  bob            role=platform_admin    scope=ALL SERVICES   expires=never
  carol          role=data_engineer     scope=ALL SERVICES   expires=never
  dave           role=platform_admin    scope=doris          expires=never
  dave           role=platform_admin    scope=kafka          expires=never
  dave           role=platform_admin    scope=opensearch     expires=never
```

> `dave` has three rows — he was given per-service bindings after Spark access was removed
> (see [section (l) Scenario B](#scenario-b--remove-a-user-from-one-service-only)).

---

### Reading the `service_name` column

| `service_name` value | What it means |
|---|---|
| *(blank / null)* | Role applies to **all four services** — Doris, Kafka, OpenSearch, Spark |
| `doris` | Role permissions are enforced on **Doris only** |
| `kafka` | Role permissions are enforced on **Kafka only** |
| `opensearch` | Role permissions are enforced on **OpenSearch only** |
| `spark` | Role permissions are enforced on **Spark only** |

A user with **multiple rows** in `bindings` has multiple bindings active at the same time.
`rbacctl user roles` resolves all of them and shows the combined, de-duplicated
permission set already filtered by service — it is the definitive answer to
"what can this user actually do right now".

---

### Test 4 — Verify role contents (what permissions a role grants)

> Confirm exactly which permissions a role grants before binding users to it.

```bash
# CLI — list permissions for a role by name
rbacctl role list
# Note the id of the role you want, then:
rbacctl role get <role_id>
```

```bash
# CLI — list all available permissions filtered by service
rbacctl role perms --service spark
rbacctl role perms --service doris
```

```bash
# API — inspect one role's permissions grouped by service
ROLE_NAME="data_engineer"
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" "${RBAC_URL}/api/v1/roles" | \
  python3 -c "
import sys, json
for r in json.load(sys.stdin):
    if r['name'] == '${ROLE_NAME}':
        svc = {}
        for p in r['permissions']:
            svc.setdefault(p['service_name'], []).append(p['permission_name'])
        print(f'Role: {r[\"name\"]}  (id={r[\"id\"]}, {len(r[\"permissions\"])} permissions)')
        for s in sorted(svc):
            print(f'  {s}: {sorted(svc[s])}')
"
```

Expected output for `data_engineer`:
```
Role: data_engineer  (id=7, 20 permissions)
  doris: ['DELETE', 'INSERT', 'LOAD', 'SELECT', 'SHOW_VIEW', 'UPDATE']
  kafka: ['CONSUME', 'CONSUMER_GROUP_MANAGE', 'DESCRIBE', 'PRODUCE', 'SCHEMA_REGISTRY_READ', 'TRANSACTIONAL_WRITE']
  opensearch: ['CLUSTER_READ', 'INDEX_READ', 'INDEX_WRITE']
  spark: ['KILL_OWN_JOB', 'SUBMIT_JOB', 'USE_CATALOG', 'VIEW_UI', 'WRITE_ICEBERG']
```

---

### Quick-reference — inspection commands

| Question | CLI | API |
|---|---|---|
| What roles/scope does user X have? | `rbacctl user bindings <user>` | `GET /api/v1/users/<user>/bindings` |
| What can user X actually do? | `rbacctl user roles <user>` | `GET /api/v1/users/<user>/roles` |
| Who is in role Y? | loop over users, filter by `role_name` | loop over `GET /api/v1/users` |
| What permissions does role Y grant? | `rbacctl role get <role_id>` | `GET /api/v1/roles/<role_id>` |
| List all available permissions for a service | `rbacctl role perms --service spark` | `GET /api/v1/services/spark/permissions` |
| See all roles with permission counts | `rbacctl role list` | `GET /api/v1/roles` |
