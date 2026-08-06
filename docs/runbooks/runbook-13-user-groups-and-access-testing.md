# Runbook 13 — User Groups, Role Binding, and Access Testing

> **RBAC Plane:** `http://192.168.1.50:30850` · **Realm:** `STARDATADBLABS.LOCAL`
> **KDC:** `kerberos-kdc.prod.svc.cluster.local:88`
> **Related runbooks:** [11 — Kerberos](runbook-11-kerberos-integration.md) · [12 — New User Setup](runbook-12-rbac-new-user-testing.md)

This runbook covers the full lifecycle of user groups on the platform:

| Section | Topic |
|---|---|
| [(a)](#a-adding-a-user-to-kerberos) | Adding a user to Kerberos (KDC) |
| [(b)](#b-admin-user-group---full-admin-on-all-services) | Admin user group — full admin on Doris, Kafka, OpenSearch, Spark |
| [(c)](#c-data_engineer-user-group---select--dml) | `data_engineer` group — SELECT + DML on all services |
| [(d)](#d-account_admin-user-group---top-level-governance) | `account_admin` group — account-level governance admin |
| [(e)](#e-adding-a-user-to-a-group-and-testing-access) | Adding a user to a group and verifying access end-to-end |
| [(f)](#f-negative-test---user-in-rbac-group-but-no-kerberos-principal) | Negative test — RBAC binding without a KDC principal |

---

## Prerequisites

```bash
# Set these in your shell for every section below
export RBAC_URL="http://192.168.1.50:30850"
export RBAC_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)
export DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
export REALM="STARDATADBLABS.LOCAL"
```

Verify the RBAC plane is healthy:
```bash
curl -s ${RBAC_URL}/health
# Expected: {"status":"ok","version":"1.0.0"}
```

---

## (a) Adding a User to Kerberos

> Every user on this platform needs a KDC principal **before** they can authenticate
> to any Kerberos-protected service (Kafka port 9093, OpenSearch SPNEGO, Spark guard).
> Kerberos provides **authentication** only — the RBAC plane controls **authorisation**.

### Step 1 — Create the KDC principal

```bash
USERNAME="newuser"
PASSWORD="TempPass1!"

# Non-interactive — set a temporary password
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -pw ${PASSWORD} ${USERNAME}@${REALM}"

# Verify principal exists
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc ${USERNAME}@${REALM}" 2>/dev/null | grep "Principal:"
```

### Step 2 — Export a keytab (for service accounts and batch jobs)

```bash
# Export inside the KDC pod
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "ktadd -k /tmp/${USERNAME}.keytab ${USERNAME}@${REALM}"

# Copy to master node
KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')
kubectl cp prod/${KDC_POD}:/tmp/${USERNAME}.keytab /tmp/${USERNAME}.keytab

# Verify keytab content
klist -ekt /tmp/${USERNAME}.keytab

# Store as K8s secret
kubectl create secret generic ${USERNAME}-keytab \
  --from-file=keytab=/tmp/${USERNAME}.keytab \
  -n prod --dry-run=client -o yaml | kubectl apply -f -

# Clean temp files
kubectl exec -n prod deploy/kerberos-kdc -- rm /tmp/${USERNAME}.keytab
rm /tmp/${USERNAME}.keytab
```

### Step 3 — Create a matching Doris SQL user

> Doris has no native Kerberos/GSSAPI. The `krb-doris-guard` sidecar enforces KDC
> principal existence at connection time. Doris uses its own SQL password for the
> actual query session — the username must match the KDC short name.

```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "CREATE USER IF NOT EXISTS '${USERNAME}'@'%' IDENTIFIED BY '${PASSWORD}';"
```

### Step 4 — Store credentials in OpenBao

```bash
BAO_ADDR="http://192.168.1.50:30820"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('/root/openbao-init-keys.json'))['root_token'])")

# Kerberos principal metadata
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"data\":{\"principal\":\"${USERNAME}@${REALM}\",\"keytab_secret\":\"${USERNAME}-keytab\",\"created_by\":\"admin\"}}" \
  "${BAO_ADDR}/v1/secret/data/kerberos/users/${USERNAME}" && echo "Kerberos entry stored"

# Doris credential
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"data\":{\"username\":\"${USERNAME}\",\"password\":\"${PASSWORD}\",\"service\":\"doris\",\"created_by\":\"admin\"}}" \
  "${BAO_ADDR}/v1/secret/data/doris/users/${USERNAME}" && echo "Doris credential stored"
```

### Quick-check: list all KDC principals

```bash
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "listprincs" 2>/dev/null | grep -v "^Authenticating"
```

---

## (b) Admin User Group — Full Admin on All Services

> **Role name:** `platform_admin`
> **Permissions:** all privileges on Doris, Kafka, OpenSearch, and Spark.
>
> | Service | Privileges |
> |---|---|
> | Doris | SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, LOAD, GRANT, **ADMIN_PRIV** |
> | Kafka | PRODUCE, CONSUME, CREATE_TOPIC, DELETE_TOPIC, DESCRIBE, ADMIN |
> | OpenSearch | INDEX_READ, INDEX_WRITE, INDEX_ADMIN, CLUSTER_READ, CLUSTER_ADMIN |
> | Spark | SUBMIT_JOB, KILL_OWN_JOB, KILL_ANY_JOB, VIEW_UI |

This role already exists in the RBAC plane (seeded by migration `003_seed_groups.sql`).

### View the role definition

```bash
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles" | \
  python3 -c "
import sys,json
for r in json.load(sys.stdin):
    if r['name']=='platform_admin':
        print('Role:', r['display_name'])
        for p in r['permissions']:
            print(f'  {p[\"service_name\"]}.{p[\"permission_name\"]}')
"
```

### Add a user to the admin group

```bash
USERNAME="bob"   # must already have a KDC principal + Doris SQL user (see section a)

# 1. Register user in RBAC plane (idempotent)
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"display_name\":\"Bob Jones\",\"email\":\"${USERNAME}@example.com\"}" \
  "${RBAC_URL}/api/v1/users" | python3 -m json.tool

# 2. Bind the platform_admin role
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"role_name":"platform_admin"}' \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings" | python3 -m json.tool

# 3. Sync to all services
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

### Verify admin grants — Doris

```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR '${USERNAME}'@'%';" 2>/dev/null
# Expected: GlobalPrivs includes Admin_priv, CatalogPrivs includes Grant_priv, Select_priv, etc.
```

---

## (c) `data_engineer` User Group — SELECT + DML

> **Role name:** `data_engineer`
> **Purpose:** day-to-day data engineering — read and write data, no schema/admin ops.
>
> | Service | Privileges |
> |---|---|
> | Doris | SELECT, INSERT, UPDATE, DELETE, LOAD |
> | Kafka | PRODUCE, CONSUME, DESCRIBE |
> | OpenSearch | INDEX_READ, INDEX_WRITE, CLUSTER_READ |
> | Spark | SUBMIT_JOB, KILL_OWN_JOB, VIEW_UI |

### View the role definition

```bash
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles" | \
  python3 -c "
import sys,json
for r in json.load(sys.stdin):
    if r['name']=='data_engineer':
        print('Role:', r['display_name'])
        for p in r['permissions']:
            print(f'  {p[\"service_name\"]}.{p[\"permission_name\"]}')
"
```

### Add a user to the data_engineer group

```bash
USERNAME="carol"   # must have KDC principal + Doris SQL user

# 1. Register
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"display_name\":\"Carol Reyes\",\"email\":\"${USERNAME}@example.com\"}" \
  "${RBAC_URL}/api/v1/users"

# 2. Bind
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"role_name":"data_engineer"}' \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings"

# 3. Sync
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

### Verify DML grants — Doris

```bash
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR '${USERNAME}'@'%';" 2>/dev/null
# Expected: CatalogPrivs includes Select_priv, Load_priv (INSERT/UPDATE/DELETE/LOAD all map to LOAD_PRIV)
# NOT expected: Admin_priv, Grant_priv, Create_priv, Drop_priv, Alter_priv
```

---

## (d) `account_admin` User Group — Top-Level Governance

> **Role name:** `account_admin`
> **Purpose:** account-level administration — user provisioning, policy management, full
> platform access. Typically reserved for platform/security team leads.
>
> This role has **identical permissions to `platform_admin`** (all 25 permissions across all
> 4 services), but is named separately to represent a distinct governance boundary.

### Add a user to the account_admin group

```bash
USERNAME="dave"

# 1. Register
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"display_name\":\"Dave Kim\",\"email\":\"${USERNAME}@example.com\"}" \
  "${RBAC_URL}/api/v1/users"

# 2. Bind
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"role_name":"account_admin"}' \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings"

# 3. Sync
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

### Difference between `platform_admin`, `data_admin`, and `account_admin`

| Role | Permission set | Intended for |
|---|---|---|
| `data_admin` | All 25 permissions | General platform admin (legacy seed) |
| `platform_admin` | All 25 permissions | Infrastructure admin group |
| `account_admin` | All 25 permissions | Account governance team |
| `data_engineer` | 14 permissions (SELECT+DML only) | Day-to-day engineering team |

All three admin roles carry identical privileges. The naming distinction exists to support
future per-group policy customisation (e.g., `account_admin` may later receive additional
user management API scopes while `platform_admin` is restricted to service-level ops).

---

## (e) Adding a User to a Group and Testing Access

> This section shows the complete end-to-end flow: Kerberos → RBAC → services → access test.
> Replace `${USERNAME}` and `${ROLE}` with the target user and role.

### Full provisioning script

```bash
USERNAME="newengineer"
ROLE="data_engineer"
PASSWORD="TempPass1!"

# ── Step 1: Kerberos ────────────────────────────────────────
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -pw ${PASSWORD} ${USERNAME}@${REALM}"

# ── Step 2: Doris SQL user ──────────────────────────────────
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "CREATE USER IF NOT EXISTS '${USERNAME}'@'%' IDENTIFIED BY '${PASSWORD}';"

# ── Step 3: RBAC register + bind ───────────────────────────
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"display_name\":\"New Engineer\",\"email\":\"${USERNAME}@example.com\"}" \
  "${RBAC_URL}/api/v1/users"

curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"role_name\":\"${ROLE}\"}" \
  "${RBAC_URL}/api/v1/users/${USERNAME}/bindings"

# ── Step 4: Sync to all four services ──────────────────────
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

### Verify access per service

#### Doris

```bash
# Connect as the new user (guard checks KDC first, then proxies to Doris)
mysql -h 192.168.1.50 -P 30090 -u ${USERNAME} --password="${PASSWORD}" \
  -e "SHOW DATABASES;" 2>/dev/null

# Verify grants from admin side
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR '${USERNAME}'@'%';" 2>/dev/null
```

Expected for `data_engineer`: `Select_priv, Load_priv` in CatalogPrivs. No `Admin_priv`.

#### Kafka — SCRAM credentials (KafkaUser CR)

```bash
# KafkaUser CR should exist and be Ready
kubectl get kafkauser ${USERNAME} -n prod

# Expected:
# NAME          CLUSTER         AUTHENTICATION   READY
# newengineer   strimzi-kafka   scram-sha-512    True
```

> **Note:** Kafka ACL enforcement is disabled on this cluster (`allow.everyone.if.no.acl.found=true`).
> The KafkaUser CR creates SCRAM credentials only. Kerberos authentication on port 9093 is
> the actual enforcement gate. The RBAC sync ensures the user has a valid SCRAM credential
> for the internal SCRAM listener used by operators.

#### OpenSearch — internal user + role mapping

```bash
# Check internal user exists
curl -sk -u admin:${OPENSEARCH_ADMIN_PASS} \
  "https://192.168.1.50:30920/_plugins/_security/api/internalusers/${USERNAME}" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print('User exists:', '${USERNAME}' in d)"

# Check role mappings
curl -sk -u admin:${OPENSEARCH_ADMIN_PASS} \
  "https://192.168.1.50:30920/_plugins/_security/api/rolesmapping" | \
  python3 -c "
import sys,json
d=json.load(sys.stdin)
for role,mapping in d.items():
    if '${USERNAME}' in mapping.get('users',[]):
        print(f'  mapped to role: {role}')
"
```

#### Spark — allowlist ConfigMap

```bash
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | \
  python3 -c "
import sys,json
d=json.load(sys.stdin)
entry=d.get('${USERNAME}')
if entry:
    print('Spark allowlist entry:', entry)
else:
    print('WARNING: ${USERNAME} not in allowlist')
"
# Expected for data_engineer:
# {"can_kill_any": false, "can_submit": true, "view_ui": true}
```

### Check the audit log

```bash
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/audit?limit=20" | \
  python3 -c "
import sys,json
for e in json.load(sys.stdin)['entries']:
    if '${USERNAME}' in str(e.get('detail','')):
        print(e['ts'], e['action'], e['target_type'], e.get('detail'))
"
```

---

## (f) Negative Test — User in RBAC Group But No Kerberos Principal

> **Expected result:** The RBAC sync will succeed (RBAC plane has no KDC awareness),
> but the user will be **blocked at the Kerberos authentication gate** on every service.
>
> This is an important security property: RBAC grants are necessary but not sufficient.
> A valid KDC principal is always required first.

### Setup: create RBAC-only user (no KDC principal)

```bash
USERNAME="ghost"

# 1. Create Doris SQL user (grants exist, but guard will block)
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

# 3. Sync — this SUCCEEDS (plane doesn't check KDC)
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"dry_run\":false}" \
  "${RBAC_URL}/api/v1/sync" | python3 -m json.tool
```

**Expected sync output:** all four services show `synced` or `skipped`. Zero errors.

### Confirm: `ghost` exists in downstream services

```bash
# Doris: SQL user exists, has grants
mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "SHOW GRANTS FOR 'ghost'@'%';" 2>/dev/null

# Kafka: KafkaUser CR created
kubectl get kafkauser ghost -n prod

# Spark: appears in allowlist
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print('ghost in allowlist:', 'ghost' in d)"
```

### Test: attempt connection — all services should BLOCK

#### Doris (via krb-doris-guard)

```bash
# The guard intercepts MySQL connections on port 30090 (→ internal 19030)
# It runs: kinit -V -n ghost@STARDATADBLABS.LOCAL < /dev/null
# Since ghost has no KDC principal, kinit returns exit code 1 → guard rejects

mysql -h 192.168.1.50 -P 30090 -u ghost --password="Ghost1Pass!" \
  -e "SHOW DATABASES;" 2>&1

# Expected error:
# ERROR 1045 (28000): Access denied for user 'ghost'@...: principal ghost@STARDATADBLABS.LOCAL not found in Kerberos KDC
```

#### Kafka (GSSAPI listener, port 9093)

```bash
# Without a TGT (kinit) the SASL handshake fails immediately
# kinit itself fails since the principal doesn't exist:
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc ghost@STARDATADBLABS.LOCAL" 2>&1 | grep -i "does not exist"
# Expected: "Principal does not exist"
```

#### OpenSearch (SPNEGO)

```bash
# Cannot obtain a Kerberos ticket for ghost → SPNEGO negotiation fails → HTTP 401
# (The internal OpenSearch user created by the RBAC plane is irrelevant —
#  SPNEGO auth fails before that user is ever consulted)
curl -sk --negotiate -u : \
  "https://192.168.1.50:30920/_cluster/health" 2>&1 | head -3
# Expected: HTTP 401 Unauthorized
```

#### Spark (krb-spark-guard, auth endpoint)

```bash
# The guard's POST /auth endpoint calls kinit with the user's keytab
# Without a KDC principal there is no keytab — the call returns HTTP 401
curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"ghost","keytab_b64":""}' \
  http://192.168.1.50:30778/auth
# Expected: HTTP 401 or {"error":"kinit failed: ..."}
```

### Summary table

| Service | RBAC grant state | KDC principal | Connection result |
|---|---|---|---|
| Doris | ✅ SELECT_PRIV granted | ❌ no principal | ❌ **BLOCKED** by krb-doris-guard (`kinit` fails) |
| Kafka | ✅ KafkaUser CR exists | ❌ no principal | ❌ **BLOCKED** — no TGT possible, GSSAPI rejected |
| OpenSearch | ✅ internal user + role mapping | ❌ no principal | ❌ **BLOCKED** — SPNEGO returns HTTP 401 |
| Spark | ✅ in allowlist | ❌ no principal | ❌ **BLOCKED** — krb-spark-guard auth call fails |

> **Security conclusion:**
> Kerberos acts as the **hard authentication gate** in front of every service.
> The RBAC plane manages *authorisation scope* (what a user can do), but a valid KDC
> principal is unconditionally required before any service will accept a connection.
> Removing a user's KDC principal is therefore an immediate, hard revocation — even if
> their RBAC role bindings remain intact.

### Cleanup

```bash
# Remove ghost from all downstream services
curl -s -X DELETE -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/users/ghost"

mysql -h 192.168.1.50 -P 30090 -u root --password="${DORIS_PASS}" \
  -e "DROP USER IF EXISTS 'ghost'@'%';" 2>/dev/null
kubectl delete kafkauser ghost -n prod --ignore-not-found
```

---

## Role Reference

| Role | Doris | Kafka | OpenSearch | Spark | Use case |
|---|---|---|---|---|---|
| `analyst` | SELECT | CONSUME, DESCRIBE | INDEX_READ, CLUSTER_READ | VIEW_UI | Read-only analyst |
| `etl_writer` | SELECT, INSERT, UPDATE, LOAD | PRODUCE, CONSUME, DESCRIBE | — | — | ETL pipeline writer |
| `spark_user` | — | — | — | SUBMIT_JOB, KILL_OWN_JOB, VIEW_UI | Spark job submitter |
| `kafka_consumer` | — | CONSUME, DESCRIBE | — | — | Kafka consumer only |
| **`data_engineer`** | SELECT, INSERT, UPDATE, DELETE, LOAD | PRODUCE, CONSUME, DESCRIBE | INDEX_READ, INDEX_WRITE, CLUSTER_READ | SUBMIT_JOB, KILL_OWN_JOB, VIEW_UI | **Data engineering team** |
| **`platform_admin`** | All (incl. ADMIN_PRIV) | All | All | All | **Platform infra admin** |
| **`account_admin`** | All (incl. ADMIN_PRIV) | All | All | All | **Account governance admin** |
| `data_admin` | All | All | All | All | Generic full admin (legacy seed) |

---

## Quick-Reference Commands

```bash
# List all roles
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  "${RBAC_URL}/api/v1/roles" | python3 -c \
  "import sys,json; [print(f'{r[\"name\"]:20} ({len(r[\"permissions\"])} perms)') for r in json.load(sys.stdin)]"

# List all users and their roles
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" "${RBAC_URL}/api/v1/users" | \
  python3 -c "import sys,json; [print(u['username']) for u in json.load(sys.stdin)]" | \
  while read u; do
    roles=$(curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" "${RBAC_URL}/api/v1/users/${u}/roles" | \
      python3 -c "import sys,json; print(', '.join(r['name'] for r in json.load(sys.stdin)))")
    echo "${u}: ${roles}"
  done

# Full sync of all users to all services
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"dry_run":false}' "${RBAC_URL}/api/v1/sync" | python3 -m json.tool

# Dry-run to preview changes
curl -s -X POST -H "Authorization: Bearer ${RBAC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"dry_run":true}' "${RBAC_URL}/api/v1/sync" | python3 -m json.tool

# List KDC principals
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "listprincs" 2>/dev/null | grep -v "^Authenticating"
```
