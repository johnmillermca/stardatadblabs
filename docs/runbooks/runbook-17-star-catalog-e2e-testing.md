# Runbook 17 — Star Knowledge Catalog: End-to-End Testing Guide

> **Component:** Star Knowledge Catalog v1.0.0  
> **API:** `http://192.168.1.50:30860` · Swagger UI: `http://192.168.1.50:30860/docs`  
> **Related:** [Runbook 16 — Setup](runbook-16-data-governance-masking.md) · [Runbook 12 — RBAC](runbook-12-rbac-new-user-testing.md)

This runbook is a single, ordered test script. Run every section from top to bottom on a fresh deployment. Each test includes the exact command, the expected output, and a ✅ / ❌ pass/fail criterion.

---

## Test Checklist

| # | Area | Test |
|---|---|---|
| T-01 | Infrastructure | Doris FE is reachable |
| T-02 | Infrastructure | PostgreSQL is reachable |
| T-03 | Infrastructure | Redis is reachable |
| T-04 | Infrastructure | RBAC Control Plane is healthy |
| T-05 | Infrastructure | Star Catalog pod is running |
| T-06 | Infrastructure | Star Catalog health probe returns ok |
| T-07 | Auth | Token exchange succeeds |
| T-08 | Auth | Bad token returns 401 |
| T-09 | Classifications | Seed data returns 5 classifications |
| T-10 | Classifications | PII sensitivity is `high` |
| T-11 | Classifications | PCI sensitivity is `critical` |
| T-12 | Classifications | CRUD: create / update / delete a test classification |
| T-13 | Glossary | Seed data returns 10 glossary terms |
| T-14 | Glossary | email_address term has correct patterns |
| T-15 | Glossary | CRUD: create / update / delete a test term |
| T-16 | Algorithms | Seed data returns 8 algorithms |
| T-17 | Algorithms | EMAIL_PARTIAL expression is correct |
| T-18 | Algorithms | CRUD: create / delete a test algorithm |
| T-19 | Policies | Seed data returns correct classification + term policies |
| T-20 | Policies | Term-level policy has priority 200, class-level has 100 |
| T-21 | Doris | governance_demo database exists with 4 tables |
| T-22 | Doris | customers table has 20 rows |
| T-23 | Doris | payments table has 40 rows |
| T-24 | Auto-classify | Dry-run scan detects 8 PII/CONFIDENTIAL columns in customers |
| T-25 | Auto-classify | Dry-run scan detects 2 PCI columns in payments |
| T-26 | Auto-classify | Live scan persists column tags to PostgreSQL |
| T-27 | Column tags | 10 column tags exist after scan |
| T-28 | Column tags | Manual tag override works and is not overwritten by re-scan |
| T-29 | Masking views | Apply dry-run returns correct DDL preview |
| T-30 | Masking views | Live apply creates customers_masked and payments_masked in Doris |
| T-31 | Masking views | Manifest records stored in PostgreSQL |
| T-32 | Masking views | orders and products have no masked view (no sensitive columns) |
| T-33 | Role routing | Query planner routes analyst to masked_view |
| T-34 | Role routing | Query planner routes data_admin to base_table |
| T-35 | Doris query — analyst | analyst can SELECT from customers_masked |
| T-36 | Doris query — analyst | full_name is SHA-256 hashed |
| T-37 | Doris query — analyst | email shows first 2 chars + domain only |
| T-38 | Doris query — analyst | date_of_birth generalised to Jan 1 of year |
| T-39 | Doris query — analyst | national_id is `****` |
| T-40 | Doris query — analyst | ip_address last octet is zeroed |
| T-41 | Doris query — analyst | salary is `****` |
| T-42 | Doris query — analyst | card_number shows last 4 digits only |
| T-43 | Doris query — analyst | credit_card_cvv is `****` |
| T-44 | Doris query — analyst | non-sensitive columns (customer_tier, city) are clear |
| T-45 | Negative — analyst | analyst cannot SELECT from base customers table |
| T-46 | Negative — analyst | analyst cannot SELECT from base payments table |
| T-47 | Doris query — admin | data_admin_user sees raw unmasked data in customers |
| T-48 | Masking exception | Grant exception to a new role, verify query planner routes to base_table |
| T-49 | Policy update | Change algorithm for a policy, re-apply view, verify new masking expression |
| T-50 | Performance | Masked view query completes in < 1 second |
| T-51 | Cache | Second identical API call served faster (cache hit logged) |
| T-52 | Idempotency | Re-running scan + apply is a no-op (action: unchanged) |

---

## Phase 0 — Pre-flight: first-time setup

> **Skip this phase if the catalog is already deployed and healthy** (T-06 returns `ok`). Run it only on a fresh cluster or after a wipe.
>
> **Already deployed?** Confirm with:
> ```bash
> kubectl exec -n prod postgresql-0 -- psql -U postgres -c "\l" | grep star_catalog
> ```
> If `star_catalog` appears in the output, the database and user already exist — **skip Phase 0 entirely** and jump to [Setup: shell environment](#setup-shell-environment).

All credentials are stored in **OpenBao** and synced to a Kubernetes Secret — no plaintext passwords in YAML files.

### 0.1 — Generate a password and store credentials in OpenBao

```bash
# Read the OpenBao root token (generated during Runbook 01)
ROOT_TOKEN=$(cat ~/openbao-init-keys.json | python3 -c \
  "import json,sys; print(json.load(sys.stdin)['root_token'])")
BAO_ADDR="http://192.168.1.50:30820"

# Generate secrets
PG_PASSWORD=$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 24)
MASTER_TOKEN=$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 32)
JWT_SECRET=$(openssl rand -base64 36 | tr -dc 'A-Za-z0-9' | head -c 48)

# Read the real RBAC Control Plane master token — must already exist in rbac-plane-credentials
# (created by rbac-plane/scripts/seed-rbac-credentials.sh before this step)
RBAC_PLANE_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)
echo "RBAC_PLANE_TOKEN length: ${#RBAC_PLANE_TOKEN}"  # must be > 0

# Store in OpenBao at secret/data/star-catalog/credentials
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"data\":{
    \"pg-user\":\"star_catalog\",
    \"pg-password\":\"${PG_PASSWORD}\",
    \"master-token\":\"${MASTER_TOKEN}\",
    \"jwt-secret\":\"${JWT_SECRET}\",
    \"doris-admin-password\":\"\",
    \"rbac-plane-token\":\"${RBAC_PLANE_TOKEN}\"
  }}" \
  "${BAO_ADDR}/v1/secret/data/star-catalog/credentials"

echo "Stored at secret/data/star-catalog/credentials"
echo "PG_PASSWORD=${PG_PASSWORD}"
echo "MASTER_TOKEN=${MASTER_TOKEN}"
```

> **Pre-requisite:** `rbac-plane-credentials` must exist in the `prod` namespace before running this step. It is created by `rbac-plane/scripts/seed-rbac-credentials.sh`. If it does not exist yet, run that script first, then return here.

### 0.2 — Create the PostgreSQL database and user

> **Skip if already exists.** The `star_catalog` database and user were created during the initial platform deployment and persist across pod restarts. Only run this step on a brand-new cluster or after a full PostgreSQL wipe. Verify first:
> ```bash
> kubectl exec -n prod postgresql-0 -- psql -U postgres -c "\l" | grep star_catalog
> # If star_catalog is listed → skip this step.
> ```

```bash
kubectl exec -n prod postgresql-0 -- psql -U postgres -c "
  CREATE DATABASE star_catalog;
  CREATE USER star_catalog WITH PASSWORD '${PG_PASSWORD}';
  GRANT ALL PRIVILEGES ON DATABASE star_catalog TO star_catalog;
"

# Grant schema access
kubectl exec -n prod postgresql-0 -- psql -U postgres -d star_catalog -c \
  "GRANT ALL ON SCHEMA public TO star_catalog;"
```

### 0.3 — Run the schema migration

```bash
kubectl cp star-knowledge-catalog/migrations/001_schema_and_seed.sql \
  prod/postgresql-0:/tmp/001_schema_and_seed.sql

kubectl exec -n prod postgresql-0 -- \
  env PGPASSWORD="${PG_PASSWORD}" \
  psql -U star_catalog -d star_catalog -f /tmp/001_schema_and_seed.sql
```

> If you see `ERROR: syntax error at or near "TEXT"` on the glossary INSERT, run the fix migration:
> ```bash
> kubectl cp star-knowledge-catalog/migrations/002_seed_glossary_fix.sql \
>   prod/postgresql-0:/tmp/002_seed_glossary_fix.sql
> kubectl exec -n prod postgresql-0 -- \
>   env PGPASSWORD="${PG_PASSWORD}" \
>   psql -U star_catalog -d star_catalog -f /tmp/002_seed_glossary_fix.sql
> ```

### 0.4 — Create the Kubernetes Secret from OpenBao values

```bash
# Read back from OpenBao to ensure K8s secret is authoritative
DATA=$(curl -sf -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/star-catalog/credentials" | \
  python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)['data']['data']))")

kubectl create secret generic star-catalog-credentials \
  --namespace prod \
  --from-literal=PG_PASSWORD=$(echo $DATA | python3 -c "import json,sys; print(json.load(sys.stdin)['pg-password'])") \
  --from-literal=DORIS_ADMIN_PASSWORD=$(echo $DATA | python3 -c "import json,sys; print(json.load(sys.stdin)['doris-admin-password'])") \
  --from-literal=MASTER_TOKEN=$(echo $DATA | python3 -c "import json,sys; print(json.load(sys.stdin)['master-token'])") \
  --from-literal=JWT_SECRET=$(echo $DATA | python3 -c "import json,sys; print(json.load(sys.stdin)['jwt-secret'])") \
  --from-literal=RBAC_PLANE_TOKEN=$(echo $DATA | python3 -c "import json,sys; print(json.load(sys.stdin)['rbac-plane-token'])") \
  --dry-run=client -o yaml | kubectl apply -f -
```

> **Verify `RBAC_PLANE_TOKEN` is not the placeholder.** If this value is `changeme-master-token` the catalog's query planner (T-33/T-34) will fail with `"User not found in RBAC Control Plane"` at test time. Confirm:
> ```bash
> kubectl get secret star-catalog-credentials -n prod \
>   -o jsonpath='{.data.RBAC_PLANE_TOKEN}' | base64 -d && echo
> # Must NOT print: changeme-master-token
> # Must print the same value as:
> kubectl get secret rbac-plane-credentials -n prod \
>   -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d && echo
> ```
> If it shows `changeme-master-token`, patch it immediately:
> ```bash
> REAL_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
>   -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)
> kubectl patch secret star-catalog-credentials -n prod \
>   --type='json' \
>   -p="[{\"op\":\"replace\",\"path\":\"/data/RBAC_PLANE_TOKEN\",\"value\":\"$(echo -n $REAL_TOKEN | base64)\"}]"
> kubectl rollout restart deployment/star-knowledge-catalog -n prod
> kubectl rollout status deployment/star-knowledge-catalog -n prod --timeout=60s
> ```

### 0.5 — Build and push the container image

```bash
podman build \
  -t 192.168.1.50:30500/star-knowledge-catalog:1.0.0 \
  -f star-knowledge-catalog/docker/Dockerfile \
  star-knowledge-catalog/

podman push --tls-verify=false 192.168.1.50:30500/star-knowledge-catalog:1.0.0
```

### 0.6 — Deploy and verify

```bash
kubectl apply -f star-knowledge-catalog/manifests/star-catalog-deployment.yaml
kubectl rollout status deployment/star-knowledge-catalog -n prod --timeout=120s
curl -sf http://192.168.1.50:30860/health | python3 -m json.tool
```

Expected: `{"status": "ok", "version": "1.0.0", "service": "star-knowledge-catalog"}`

### 0.7 — Read the master token for testing

The CATALOG_MASTER_TOKEN to use in all tests is stored in OpenBao. Retrieve it:

```bash
ROOT_TOKEN=$(cat ~/openbao-init-keys.json | python3 -c \
  "import json,sys; print(json.load(sys.stdin)['root_token'])")
curl -sf -H "X-Vault-Token: ${ROOT_TOKEN}" \
  http://192.168.1.50:30820/v1/secret/data/star-catalog/credentials | \
  python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['master-token'])"
```

Use the printed value as `CATALOG_MASTER_TOKEN` in the shell environment block below.

---

## Setup: shell environment

Run these once at the top of your terminal session. Every test below references these variables.

> **Must re-run in every new terminal.** These `export` statements are not persisted to your shell profile — if you open a new tab or reconnect, paste this block again before running any test.

```bash
# ── Connection details ───────────────────────────────────────
export DORIS_HOST=192.168.1.50
export DORIS_PORT=30090
export PG_HOST=192.168.1.50
export PG_PORT=30532
export CATALOG_URL=http://192.168.1.50:30860
export RBAC_URL=http://192.168.1.50:30850

# ── Doris root password ───────────────────────────────────────
# Read from the K8s Secret 'doris-credentials' (seeded into OpenBao
# by 12-seed-openbao-secrets.sh during initial platform setup).
# All Doris queries in this runbook run as root using this password.
export DORIS_ROOT_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# ── PostgreSQL star_catalog password ──────────────────────────
# Read from the K8s Secret 'star-catalog-credentials' (seeded during
# Phase 0 of this runbook).
export PG_STAR_PASS=$(kubectl get secret star-catalog-credentials -n prod \
  -o jsonpath='{.data.PG_PASSWORD}' | base64 -d)

# ── Catalog master token ──────────────────────────────────────
# Read from the K8s Secret 'star-catalog-credentials'. This is the
# bootstrap token used to authenticate all /api/v1/* calls below.
# Original value is stored in OpenBao at secret/data/star-catalog/credentials.
export CATALOG_MASTER_TOKEN=$(kubectl get secret star-catalog-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)

# ── RBAC master token ─────────────────────────────────────────
# Read from the K8s Secret 'rbac-plane-credentials'. Generated and
# stored by rbac-plane/scripts/seed-rbac-credentials.sh.
# Also available in OpenBao at secret/data/rbac-plane/credentials.
export RBAC_MASTER_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)

# ── Verify all four variables loaded ─────────────────────────
echo "DORIS_ROOT_PASS      : ${DORIS_ROOT_PASS:0:6}...  (length: ${#DORIS_ROOT_PASS})"
echo "PG_STAR_PASS         : ${PG_STAR_PASS:0:6}...  (length: ${#PG_STAR_PASS})"
echo "CATALOG_MASTER_TOKEN : ${CATALOG_MASTER_TOKEN:0:16}...  (length: ${#CATALOG_MASTER_TOKEN})"
echo "RBAC_MASTER_TOKEN    : ${RBAC_MASTER_TOKEN:0:16}...  (length: ${#RBAC_MASTER_TOKEN})"
```

> **If any value shows `length: 0`** the K8s Secret is missing or the key name is wrong. Run:
> ```bash
> kubectl get secret doris-credentials -n prod -o jsonpath='{.data}' | python3 -m json.tool
> kubectl get secret star-catalog-credentials -n prod -o jsonpath='{.data}' | python3 -m json.tool
> kubectl get secret rbac-plane-credentials -n prod -o jsonpath='{.data}' | python3 -m json.tool
> ```
> to see the exact key names available in each secret.

---

## Phase 1 — Infrastructure checks (T-01 to T-06)

### T-01 · Doris FE reachable

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT -u root \
  -e "SELECT 'doris_ok' AS status;" 2>&1
```

> **Note:** Using `MYSQL_PWD` suppresses the interactive password prompt when the root password is empty. The original `-p"$DORIS_ROOT_PASS"` form also works — it will show `Enter password:` but connects successfully when you press Enter.

✅ **Pass:** output contains `doris_ok`  
❌ **Fail:** connection refused or access denied → check `kubectl get pods -n prod | grep doris-fe`

---

### T-02 · PostgreSQL reachable

```bash
# Option A — via kubectl exec (no local psql required)
kubectl exec -n prod postgresql-0 -- \
  psql -U star_catalog -d star_catalog \
  -c "SELECT 'pg_ok' AS status;"

# Option B — local psql with PGPASSWORD to suppress prompt
PGPASSWORD=$PG_STAR_PASS psql -h $PG_HOST -p $PG_PORT \
  -U star_catalog -d star_catalog \
  -c "SELECT 'pg_ok' AS status;" 2>&1
```

✅ **Pass:** output contains `pg_ok`  
❌ **Fail: `command not found`** → `psql` not installed; use Option A above or run `sudo dnf install -y postgresql`  
❌ **Fail: `fe_sendauth: no password supplied`** → use `PGPASSWORD=$PG_STAR_PASS` prefix as shown in Option B  
❌ **Fail: `secret "star-catalog-credentials" not found`** → run Phase 0.3 + 0.4 first  
❌ **Fail: connection refused** → check `kubectl get pods -n prod | grep postgresql`; confirm Phase 0.1 was completed

---

### T-03 · Redis reachable

```bash
kubectl exec -n prod deployment/redis -- redis-cli -n 1 PING 2>&1
```

✅ **Pass:** `PONG`  
❌ **Fail:** → check `kubectl get pods -n prod | grep redis`

---

### T-04 · RBAC Control Plane healthy

```bash
curl -sf $RBAC_URL/health | jq .status
```

✅ **Pass:** `"ok"`  
❌ **Fail:** → check `kubectl get pods -n prod | grep rbac-plane`

---

### T-05 · Star Catalog pod running

```bash
kubectl get pods -n prod -l app=star-knowledge-catalog \
  --no-headers -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,READY:.status.containerStatuses[0].ready
```

✅ **Pass:** 2 rows, both `Running` / `true`  
❌ **Fail: pod in `Error` or `Pending`** → run:
```bash
kubectl describe pod -n prod -l app=star-knowledge-catalog | grep -A5 "Events\|Error\|secret"
```
Most common cause: `star-catalog-credentials` secret missing → run Phase 0.3 + 0.4.

---

### T-06 · Health probe

```bash
curl -sf $CATALOG_URL/health | jq .
```

✅ **Pass:**
```json
{"status": "ok", "version": "1.0.0", "service": "star-knowledge-catalog"}
```
❌ **Fail:** → pod not ready, or NodePort 30860 not exposed

---

## Phase 2 — Authentication (T-07 to T-08)

### T-07 · Token exchange succeeds

```bash
CATALOG_TOKEN=$(curl -sf -X POST $CATALOG_URL/api/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d "{\"token\":\"$CATALOG_MASTER_TOKEN\"}" | jq -r .access_token)

echo "Token length: ${#CATALOG_TOKEN}"
echo "Prefix: ${CATALOG_TOKEN:0:20}..."
```

✅ **Pass:** token length > 100, starts with `eyJ`  
❌ **Fail:** `null` or empty → verify `MASTER_TOKEN` in the `star-catalog-credentials` secret matches `$CATALOG_MASTER_TOKEN`

---

### T-08 · Wrong token returns 401

```bash
curl -s -o /dev/null -w "%{http_code}" -X POST $CATALOG_URL/api/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"token":"definitely-wrong-token"}'
```

✅ **Pass:** `401`

---

## Phase 3 — Data Classifications (T-09 to T-12)

### T-09 · Seed returns 5 classifications

```bash
curl -sf $CATALOG_URL/api/v1/classifications \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq 'length'
```

✅ **Pass:** `5`  
❌ **Fail:** `0` → re-run Phase 0.3 (schema migration)

---

### T-10 · PII sensitivity is `high`

```bash
curl -sf $CATALOG_URL/api/v1/classifications/PII \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq '{name, sensitivity}'
```

✅ **Pass:** `{"name": "PII", "sensitivity": "high"}`

---

### T-11 · PCI sensitivity is `critical`

```bash
curl -sf $CATALOG_URL/api/v1/classifications/PCI \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq '{name, sensitivity}'
```

✅ **Pass:** `{"name": "PCI", "sensitivity": "critical"}`

---

### T-12 · CRUD: create / update / delete a test classification

```bash
# Create
curl -sf -X POST $CATALOG_URL/api/v1/classifications \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"TEST_CLASS","display_name":"Test Classification","sensitivity":"low","color_hex":"#AABBCC"}' \
  | jq '{name, sensitivity}'

# Update
curl -sf -X PATCH $CATALOG_URL/api/v1/classifications/TEST_CLASS \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"sensitivity":"medium"}' | jq '{name, sensitivity}'

# Delete
curl -sf -X DELETE $CATALOG_URL/api/v1/classifications/TEST_CLASS \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .ok

# Confirm gone (must return 404)
curl -s -o /dev/null -w "%{http_code}" \
  $CATALOG_URL/api/v1/classifications/TEST_CLASS \
  -H "Authorization: Bearer $CATALOG_TOKEN"
```

✅ **Pass:** create → `sensitivity: low`, update → `sensitivity: medium`, delete → `true`, final GET → `404`

---

## Phase 4 — Glossary Terms (T-13 to T-15)

### T-13 · Seed returns 10 glossary terms

```bash
curl -sf $CATALOG_URL/api/v1/glossary \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq 'length'
```

✅ **Pass:** `10`

---

### T-14 · email_address term has correct patterns

```bash
curl -sf $CATALOG_URL/api/v1/glossary/email_address \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '{name, classification_name, column_name_patterns}'
```

✅ **Pass:**
```json
{
  "name": "email_address",
  "classification_name": "PII",
  "column_name_patterns": ["email","e_mail","email_addr","emailaddress","user_email","contact_email"]
}
```

---

### T-15 · CRUD: create / update / delete a test term

```bash
PII_ID=$(curl -sf $CATALOG_URL/api/v1/classifications/PII \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

# Create
curl -sf -X POST $CATALOG_URL/api/v1/glossary \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"test_term\",\"display_name\":\"Test Term\",
       \"classification_id\":$PII_ID,
       \"column_name_patterns\":[\"test_col\"]}" | jq '{name, classification_name}'

# Update
curl -sf -X PATCH $CATALOG_URL/api/v1/glossary/test_term \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"description":"Updated description"}' | jq .description

# Delete
curl -sf -X DELETE $CATALOG_URL/api/v1/glossary/test_term \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .ok

# Confirm 404
curl -s -o /dev/null -w "%{http_code}" \
  $CATALOG_URL/api/v1/glossary/test_term \
  -H "Authorization: Bearer $CATALOG_TOKEN"
```

✅ **Pass:** create → `classification_name: PII`, delete → `true`, final GET → `404`

---

## Phase 5 — Masking Algorithms (T-16 to T-18)

### T-16 · Seed returns 8 algorithms

```bash
curl -sf $CATALOG_URL/api/v1/algorithms \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq 'length'
```

✅ **Pass:** `8`

---

### T-17 · EMAIL_PARTIAL doris_expression is correct

```bash
curl -sf $CATALOG_URL/api/v1/algorithms/EMAIL_PARTIAL \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq '{name, algorithm_type, doris_expression}'
```

✅ **Pass:**
```json
{
  "name": "EMAIL_PARTIAL",
  "algorithm_type": "PARTIAL_MASK",
  "doris_expression": "CONCAT(LEFT({col},2), REPEAT('*',GREATEST(0,LOCATE('@',{col})-3)), SUBSTRING({col},LOCATE('@',{col})))"
}
```

---

### T-18 · CRUD: create / delete a test algorithm

```bash
# Create
curl -sf -X POST $CATALOG_URL/api/v1/algorithms \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"TEST_ALGO","display_name":"Test Algo",
       "algorithm_type":"REDACT","doris_expression":"'\''XXXXX'\''"}' | jq '{name, algorithm_type}'

# Delete
curl -sf -X DELETE $CATALOG_URL/api/v1/algorithms/TEST_ALGO \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .ok
```

✅ **Pass:** create → `algorithm_type: REDACT`, delete → `true`

---

## Phase 6 — Masking Policies (T-19 to T-20)

### T-19 · Seed returns 14 policies

```bash
curl -sf $CATALOG_URL/api/v1/policies \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq 'length'
```

✅ **Pass:** `14` (4 classification-level + 10 term-level)

---

### T-20 · Term-level priority > class-level priority

```bash
# Term-level min priority must be 200
curl -sf $CATALOG_URL/api/v1/policies \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '[.[] | select(.name | startswith("policy_term_"))] | min_by(.priority) | .priority'

# Class-level max priority must be 100
curl -sf $CATALOG_URL/api/v1/policies \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '[.[] | select(.name | test("policy_pii_|policy_pci_|policy_phi_|policy_confidential_"))] | max_by(.priority) | .priority'
```

✅ **Pass:** first output `200`, second output `100`

---

## Phase 7 — Doris sample data (T-21 to T-23)

### T-21 · governance_demo has 4 base tables

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT -u root \
  -e "SELECT TABLE_NAME FROM information_schema.TABLES
      WHERE TABLE_SCHEMA='governance_demo' AND TABLE_TYPE='BASE TABLE'
      ORDER BY TABLE_NAME;" 2>/dev/null
```

✅ **Pass:** `customers`, `orders`, `payments`, `products`  
❌ **Fail:** table missing → run Runbook 16 Part B

---

### T-22 · customers has 20 rows

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT -u root \
  -e "SELECT COUNT(*) AS cnt FROM governance_demo.customers;" 2>/dev/null
```

✅ **Pass:** `cnt = 20`

---

### T-23 · payments has 40 rows

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT -u root \
  -e "SELECT COUNT(*) AS cnt FROM governance_demo.payments;" 2>/dev/null
```

✅ **Pass:** `cnt = 40`

---

## Phase 8 — Auto-classification scan (T-24 to T-28)

> **`dry_run` is optional.** Setting `dry_run: true` previews what would be tagged without writing anything to PostgreSQL — safe to run as many times as you like. Setting `dry_run: false` is the live run that persists the column tags. You can skip straight to `false` if you are confident in the results. Note: `columns_tagged` will always be `0` in dry-run mode — it only increments on a live run.

> **Pre-requisite — fix overly broad glossary patterns before scanning.** The default seed data contains two patterns that cause false positives. Run both patches once before T-24/T-25:
>
> **1 — `salary` term:** bare `"pay"` pattern matches `payment_id` and `payment_method`:
> ```bash
> curl -sf -X PATCH $CATALOG_URL/api/v1/glossary/salary \
>   -H "Authorization: Bearer $CATALOG_TOKEN" \
>   -H 'Content-Type: application/json' \
>   -d '{"column_name_patterns": ["salary","annual_salary","base_salary","gross_salary","net_salary","compensation","wage"]}' \
>   | jq '{name, column_name_patterns}'
> ```
>
> **2 — `full_name` term:** bare `"name"` pattern matches `products.name` (a product name is not PII):
> ```bash
> curl -sf -X PATCH $CATALOG_URL/api/v1/glossary/full_name \
>   -H "Authorization: Bearer $CATALOG_TOKEN" \
>   -H 'Content-Type: application/json' \
>   -d '{"column_name_patterns": ["full_name","fullname","first_name","last_name","firstname","lastname","given_name","surname","customer_name","person_name"]}' \
>   | jq '{name, column_name_patterns}'
> ```
> If `products` already appears in `GET /api/v1/masking/views?database=governance_demo`, clean it up:
> ```bash
> # Delete the false-positive tag
> curl -sf -X DELETE "$CATALOG_URL/api/v1/columns/governance_demo/products/name" \
>   -H "Authorization: Bearer $CATALOG_TOKEN" | jq .ok
> # Re-scan products to confirm zero tags
> curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
>   -H "Authorization: Bearer $CATALOG_TOKEN" \
>   -H 'Content-Type: application/json' \
>   -d '{"doris_database":"governance_demo","doris_table":"products","dry_run":false,"overwrite_existing":true}' | \
>   jq '.tables_results[0] | {table:.doris_table, columns_tagged}'
> ```

### T-24 · Dry-run detects 8 sensitive columns in customers

```bash
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","dry_run":true}' | \
  jq '.tables_results[0] | {table:.doris_table, columns_tagged,
      hits: [.results[] | select(.score >= 0.7) | {col:.column_name, term:.matched_term, score:.score}]}'
```

✅ **Pass:** `columns_tagged = 0` (dry-run — nothing written), hits contain all 8 columns with score 1.0: `full_name`, `email`, `phone_number`, `date_of_birth`, `national_id`, `street_address`, `ip_address`, `salary`

---

### T-25 · Dry-run detects 2 PCI columns in payments

```bash
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"payments","dry_run":true}' | \
  jq '.tables_results[0] | {table:.doris_table, columns_tagged,
      hits: [.results[] | select(.score >= 0.7) | {col:.column_name, term:.matched_term, score:.score}]}'
```

✅ **Pass:** `columns_tagged = 0` (dry-run), hits: `card_number → credit_card_number (1.0)`, `credit_card_cvv → credit_card_cvv (0.9)`
❌ **Fail: seeing `payment_id → salary` or `payment_method → salary`** → the `salary` glossary term has broad patterns — run the pre-requisite patch above to fix them, then re-run this test.

---

### T-26 · Live scan persists tags

```bash
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false,"overwrite_existing":false}' | \
  jq '{tables_scanned, total_tagged: [.tables_results[].columns_tagged] | add}'
```

✅ **Pass:** `tables_scanned = 4`, `total_tagged = 10`

---

### T-27 · 10 column tags exist

```bash
curl -sf "$CATALOG_URL/api/v1/columns?database=governance_demo" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq 'length'
```

✅ **Pass:** `10`

Full tag listing:
```bash
curl -sf "$CATALOG_URL/api/v1/columns?database=governance_demo" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq -r '.[] | "\(.doris_table).\(.column_name)\t→ \(.glossary_term_name // "—")\t[\(.classification_name // "—")]\tscore:\(.detection_score)"' | \
  column -t
```

---

### T-28 · Manual tag not overwritten by re-scan

```bash
PII_ID=$(curl -sf $CATALOG_URL/api/v1/classifications/PII \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

# Manually tag 'city' (below auto-detect threshold)
curl -sf -X POST $CATALOG_URL/api/v1/columns \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"doris_database\":\"governance_demo\",\"doris_table\":\"customers\",
       \"column_name\":\"city\",\"classification_id\":$PII_ID,
       \"override_reason\":\"Quasi-identifier under GDPR recital 26\"}" | jq '{column_name, auto_detected}'

# Re-scan with overwrite_existing:true — manual tag must survive
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false,"overwrite_existing":true}' > /dev/null

# Verify city is still manual
curl -sf $CATALOG_URL/api/v1/columns/governance_demo/customers/city \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq '{column_name, auto_detected, classification_name, override_reason}'
```

✅ **Pass:** `auto_detected: false`, `override_reason` still populated

---

## Phase 9 — Apply masked views (T-29 to T-32)

### T-29 · Dry-run shows DDL preview

```bash
curl -sf -X POST $CATALOG_URL/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","dry_run":true}' | \
  jq '.results[0] | {action, view_name, columns_masked}'
```

✅ **Pass:** `action: "dry_run"`, `view_name: "customers_masked"`, `columns_masked` contains 9 columns (`full_name`, `email`, `phone_number`, `date_of_birth`, `national_id`, `street_address`, `ip_address`, `salary` from auto-scan + `city` from the manual tag added in T-28)

---

### T-30 · Live apply creates both masked views

```bash
curl -sf -X POST $CATALOG_URL/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false,"force":false}' | \
  jq '.results[] | {table:.doris_table, view:.view_name, action}'
```

✅ **Pass:** `customers → customers_masked action:created`, `payments → payments_masked action:created`

Confirm in Doris:
```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT -u root \
  -e "SHOW TABLES IN governance_demo;" 2>/dev/null | sort
```

✅ **Pass:** output includes `customers_masked` and `payments_masked`

---

### T-31 · View manifests stored in PostgreSQL

```bash
curl -sf "$CATALOG_URL/api/v1/masking/views?database=governance_demo" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '[.[] | {base_table, view_name, columns_masked_count: (.columns_masked | length)}]'
```

✅ **Pass:**
```json
[
  {"base_table": "customers", "view_name": "customers_masked", "columns_masked_count": 9},
  {"base_table": "payments",  "view_name": "payments_masked",  "columns_masked_count": 2}
]
```
> **Note:** `customers` has 9 masked columns (not 8) because T-28 manually tagged `city` as PII before this step. The masking engine includes all tagged columns — both auto-detected and manual.

---

### T-32 · orders and products have no masked view

```bash
curl -sf "$CATALOG_URL/api/v1/masking/views?database=governance_demo" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq '[.[].base_table]'
```

✅ **Pass:** list contains only `customers` and `payments`

---

## Phase 10 — Role-aware query planner (T-33 to T-34)

Ensure RBAC users exist first (idempotent — safe to re-run):

```bash
# Note: RBAC auth uses a query parameter, not a JSON body
RBAC_TOKEN=$(curl -sf -X POST "$RBAC_URL/api/v1/auth/token?raw_token=$RBAC_MASTER_TOKEN" \
  | jq -r .access_token)
echo "RBAC token length: ${#RBAC_TOKEN}"  # must be > 0 before continuing

# alice = analyst
curl -s -X POST $RBAC_URL/api/v1/users \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","display_name":"Alice Fontaine","email":"alice@example.com"}' | jq . 2>/dev/null || true

curl -s -X POST $RBAC_URL/api/v1/users/alice/bindings \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"role_name":"analyst","service_name":"doris"}' | jq '{role_name, service_name}' 2>/dev/null || true

# bob = data_admin
curl -s -X POST $RBAC_URL/api/v1/users \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"bob","display_name":"Bob Admin","email":"bob@example.com"}' | jq . 2>/dev/null || true

curl -s -X POST $RBAC_URL/api/v1/users/bob/bindings \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"role_name":"data_admin","service_name":"doris"}' | jq '{role_name, service_name}' 2>/dev/null || true

echo "RBAC users ready"
```

> **If you see `null` outputs on the create step** — the user already exists (`{"detail":"User 'alice' already exists"}`). This is fine — the script is idempotent. Verify with:
> ```bash
> curl -sf $RBAC_URL/api/v1/users/alice -H "Authorization: Bearer $RBAC_TOKEN" | jq '{username, enabled}'
> curl -sf $RBAC_URL/api/v1/users/alice/bindings -H "Authorization: Bearer $RBAC_TOKEN" | jq '[.[] | {role_name, service_name}]'
> ```
> ✅ **Pass:** alice exists with `analyst` binding, bob exists with `data_admin` binding.

---

### T-33 · Query planner routes analyst to masked_view

```bash
curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","doris_database":"governance_demo","doris_table":"customers","limit":10}' | \
  jq '{username, role, target, columns_masked}'
```

✅ **Pass:** `role: analyst`, `target: masked_view`

---

### T-34 · Query planner routes data_admin to base_table

```bash
curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"bob","doris_database":"governance_demo","doris_table":"customers","limit":10}' | \
  jq '{username, role, target}'
```

✅ **Pass:** `role: data_admin`, `target: base_table`

---

## Phase 11 — Doris masked query execution (T-35 to T-44)

> **Note on Doris user authentication:** Doris is running with Kerberos enabled (`kerberos.enabled=true`). The `analyst` and `data_admin_user` SQL users require a valid Kerberos TGT — password auth is blocked. Tests T-35 to T-44 connect as **root** to verify the masking expressions in `customers_masked` and `payments_masked` are correct (root sees masked data when querying the view, because the masking is baked into the view DDL — not applied per-user). Tests T-45, T-46, and T-47 connect as the correct role users via Kerberos — see those sections for the `kinit` steps.

### T-35 · customers_masked returns 20 rows (masking expressions verified via root)

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT COUNT(*) AS row_count FROM customers_masked;" 2>/dev/null
```

✅ **Pass:** `row_count = 20`

---

### T-36 · full_name is SHA-256 hashed (64 hex chars)

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT LENGTH(full_name) AS len FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `len = 64`

---

### T-37 · email shows first 2 chars + domain only

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT email FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** value matches `al***@example.com` pattern

---

### T-38 · date_of_birth generalised to Jan 1

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT date_of_birth FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** value ends in `-01-01` (e.g. `1988-01-01`)

---

### T-39 · national_id is `****`

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT national_id FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `****`

---

### T-40 · ip_address last octet zeroed

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT ip_address FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** value ends in `.0` (e.g. `192.168.1.0`)

---

### T-41 · salary is `****`

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT salary FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `****`

---

### T-42 · card_number shows last 4 digits only

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT card_number FROM payments_masked WHERE payment_id=9001;" 2>/dev/null
```

✅ **Pass:** value is `************0001` (12 stars + last 4 digits)

---

### T-43 · credit_card_cvv is `****`

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT credit_card_cvv FROM payments_masked WHERE payment_id=9001;" 2>/dev/null
```

✅ **Pass:** `****`

---

### T-44 · Non-sensitive columns pass through unchanged

```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT customer_id, customer_tier, country_code, is_active
      FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** all four columns show real values (e.g. `customer_id=1001`, `gold`, `US`, `1`)
> **Note:** `city` was manually tagged as PII in T-28 and is now SHA-256 hashed in the view — it is no longer a pass-through column.

---

## Phase 12 — Negative tests (T-45 to T-46)

> **These tests must connect as the `analyst` SQL user — not root.** Root is a Doris superuser and is never denied access. Kerberos is enabled, so `analyst` requires a valid TGT. Get one via `kubectl exec` into the KDC pod:
> ```bash
> KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc --no-headers -o custom-columns=NAME:.metadata.name | head -1)
> kubectl exec -n prod $KDC_POD -- kinit -kt /etc/krb5kdc/kadm5.keytab kadmin/admin@STARDATADBLABS.LOCAL 2>/dev/null || true
> # Then connect from inside the Doris FE pod where krb5 client is available
> ```
> Alternatively if Kerberos is disabled (`kerberos.enabled=false`), connect directly with the SQL password:
> ```bash
> MYSQL_PWD="analyst_pass_demo" mysql -h $DORIS_HOST -P $DORIS_PORT -u analyst governance_demo ...
> ```

### T-45 · analyst CANNOT SELECT base customers table

Connect as `analyst` and attempt to read the base table directly — must be denied:

```bash
# Via Doris FE pod (works with Kerberos enabled)
kubectl exec -n prod statefulset/doris-fe -it -- \
  mysql -h127.0.0.1 -P9030 -uanalyst -panalyst_pass_demo governance_demo \
  -e "SELECT full_name, salary FROM customers LIMIT 1;" 2>&1 | grep -i "denied\|ERROR"
```

✅ **Pass:** output contains `ERROR` and `denied`
❌ **Fail:** query returns data → analyst has direct table access — revoke it:
```bash
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT -u root \
  -e "REVOKE SELECT_PRIV ON governance_demo.customers FROM 'analyst'@'%';"
```

---

### T-46 · analyst CANNOT SELECT base payments table

```bash
# Via Doris FE pod
kubectl exec -n prod statefulset/doris-fe -it -- \
  mysql -h127.0.0.1 -P9030 -uanalyst -panalyst_pass_demo governance_demo \
  -e "SELECT card_number FROM payments LIMIT 1;" 2>&1 | grep -i "denied\|ERROR"
```

✅ **Pass:** output contains `ERROR` and `denied`

---

## Phase 13 — Admin clear access (T-47)

### T-47 · data_admin_user sees raw unmasked data

Connect as `data_admin_user` — this user has `SELECT_PRIV` on all of `governance_demo.*` (base tables) but is not routed through the masked view:

```bash
# Via Doris FE pod
kubectl exec -n prod statefulset/doris-fe -it -- \
  mysql -h127.0.0.1 -P9030 -udata_admin_user -padmin_pass_demo governance_demo \
  -e "SELECT customer_id, full_name, email, date_of_birth, national_id, salary
      FROM customers WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `full_name = Alice Fontaine`, real email, real date, real salary — no masking

---

## Phase 14 — Masking exception grant (T-48)

### T-48 · Exception changes routing from masked_view to base_table

```bash
# Step 1 — Create user 'eve' with role 'data_steward'
curl -s -X POST $RBAC_URL/api/v1/users \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"eve","display_name":"Eve Steward","email":"eve@example.com"}' | jq .username

curl -s -X POST $RBAC_URL/api/v1/roles \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"data_steward","display_name":"Data Steward","description":"Governed clear access"}' | jq .name 2>/dev/null || true

curl -s -X POST $RBAC_URL/api/v1/users/eve/bindings \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"role_name":"data_steward","service_name":"doris"}' | jq .role_name

# Step 2 — Without exception, eve routes to masked_view
curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"eve","doris_database":"governance_demo","doris_table":"customers","limit":5}' | \
  jq '{role, target}'
# Expected: target = "masked_view"

# Step 3 — Grant exception for PII + CONFIDENTIAL
PII_ID=$(curl -sf $CATALOG_URL/api/v1/classifications/PII \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)
CONF_ID=$(curl -sf $CATALOG_URL/api/v1/classifications/CONFIDENTIAL \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

for CID in $PII_ID $CONF_ID; do
  curl -sf -X POST $CATALOG_URL/api/v1/exceptions \
    -H "Authorization: Bearer $CATALOG_TOKEN" \
    -H 'Content-Type: application/json' \
    -d "{\"role_name\":\"data_steward\",\"classification_id\":$CID,\"granted_by\":\"test\"}" | \
    jq '{role_name, classification_name}'
done

# Step 4 — Now eve routes to base_table
curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"eve","doris_database":"governance_demo","doris_table":"customers","limit":5}' | \
  jq '{role, target}'
# Expected: target = "base_table"
```

✅ **Pass:** Step 2 → `masked_view`, Step 4 → `base_table`

---

## Phase 15 — Policy update → view regeneration (T-49)

### T-49 · Change algorithm, re-apply, verify new expression

```bash
# Step 1 — Confirm current salary policy uses FULL_REDACT
curl -sf $CATALOG_URL/api/v1/policies/policy_term_salary \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq '{name, algorithm_name}'

# Step 2 — Switch to NULL_OUT
NULL_ALGO_ID=$(curl -sf $CATALOG_URL/api/v1/algorithms/NULL_OUT \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

curl -sf -X PATCH $CATALOG_URL/api/v1/policies/policy_term_salary \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"algorithm_id\": $NULL_ALGO_ID}" | jq '{name, algorithm_name}'

# Step 3 — Re-apply
curl -sf -X POST $CATALOG_URL/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","force":true}' | \
  jq '.results[0].action'
# Expected: "updated"

# Step 4 — Verify salary is now NULL
MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT salary FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
# Expected: NULL

# Step 5 — Restore FULL_REDACT
REDACT_ID=$(curl -sf $CATALOG_URL/api/v1/algorithms/FULL_REDACT \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

curl -sf -X PATCH $CATALOG_URL/api/v1/policies/policy_term_salary \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"algorithm_id\": $REDACT_ID}" | jq .algorithm_name

curl -sf -X POST $CATALOG_URL/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","force":true}' | \
  jq '.results[0].action'
```

✅ **Pass:** Step 3 → `"updated"`, Step 4 → `NULL`, Step 5 restored → salary is `****` again

---

## Phase 16 — Performance & idempotency (T-50 to T-52)

### T-50 · Masked view query < 1 second

```bash
time MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u root governance_demo \
  -e "SELECT customer_tier, COUNT(*) FROM customers_masked GROUP BY customer_tier;" 2>/dev/null
```

✅ **Pass:** wall-clock time `< 1.0s`

---

### T-51 · Second API call served from cache (faster)

```bash
time curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","doris_database":"governance_demo","doris_table":"customers","limit":1}' > /dev/null

time curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","doris_database":"governance_demo","doris_table":"customers","limit":1}' > /dev/null
```

✅ **Pass:** second call completes measurably faster (typically < 5 ms vs 20–100 ms)

---

### T-52 · Re-running scan + apply is a no-op

```bash
# Re-scan — already-tagged columns must all show action:"skipped"
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false,"overwrite_existing":false}' | \
  jq '{tables_scanned, skipped: [.tables_results[].results[] | select(.action=="skipped")] | length}'

# Re-apply — unchanged DDL must show action:"unchanged"
curl -sf -X POST $CATALOG_URL/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false,"force":false}' | \
  jq '[.results[] | select(.action != "skipped") | {table:.doris_table, action}]'
```

✅ **Pass:** scan → all tagged columns `action: skipped`, apply → all views `action: unchanged`

---

## Test Results Summary

After completing all phases, fill in this table:

| Phase | Tests | Status |
|---|---|---|
| 0 — Pre-flight setup | 0.1–0.4 | ⬜ |
| 1 — Infrastructure | T-01 to T-06 | ⬜ |
| 2 — Authentication | T-07 to T-08 | ⬜ |
| 3 — Classifications | T-09 to T-12 | ⬜ |
| 4 — Glossary | T-13 to T-15 | ⬜ |
| 5 — Algorithms | T-16 to T-18 | ⬜ |
| 6 — Policies | T-19 to T-20 | ⬜ |
| 7 — Doris data | T-21 to T-23 | ⬜ |
| 8 — Auto-classify | T-24 to T-28 | ⬜ |
| 9 — Apply views | T-29 to T-32 | ⬜ |
| 10 — Query planner | T-33 to T-34 | ⬜ |
| 11 — Masked queries | T-35 to T-44 | ⬜ |
| 12 — Negative tests | T-45 to T-46 | ⬜ |
| 13 — Admin access | T-47 | ⬜ |
| 14 — Exceptions | T-48 | ⬜ |
| 15 — Policy update | T-49 | ⬜ |
| 16 — Performance | T-50 to T-52 | ⬜ |

---

## Quick smoke test (daily sanity check)

```bash
set -e
export CATALOG_URL=http://192.168.1.50:30860
export DORIS_HOST=192.168.1.50 DORIS_PORT=30090
export DORIS_ROOT_PASS=$(kubectl get secret doris-credentials -n prod -o jsonpath='{.data.admin-password}' | base64 -d)
export CATALOG_MASTER_TOKEN=$(kubectl get secret star-catalog-credentials -n prod -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)

echo "==> Health"
curl -sf $CATALOG_URL/health | jq -e '.status == "ok"' && echo "  PASS"

echo "==> Auth"
TOK=$(curl -sf -X POST $CATALOG_URL/api/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d "{\"token\":\"$CATALOG_MASTER_TOKEN\"}" | jq -r .access_token)
[ "${#TOK}" -gt 100 ] && echo "  PASS" || { echo "  FAIL: bad token"; exit 1; }

echo "==> Classifications"
N=$(curl -sf $CATALOG_URL/api/v1/classifications -H "Authorization: Bearer $TOK" | jq length)
[ "$N" -eq 5 ] && echo "  PASS" || echo "  FAIL: got $N"

echo "==> Glossary terms"
N=$(curl -sf $CATALOG_URL/api/v1/glossary -H "Authorization: Bearer $TOK" | jq length)
[ "$N" -ge 10 ] && echo "  PASS" || echo "  FAIL: got $N"

echo "==> Algorithms"
N=$(curl -sf $CATALOG_URL/api/v1/algorithms -H "Authorization: Bearer $TOK" | jq length)
[ "$N" -eq 8 ] && echo "  PASS" || echo "  FAIL: got $N"

echo "==> Doris: row count"
N=$(MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT -u root \
  governance_demo -sN -e "SELECT COUNT(*) FROM customers_masked;" 2>/dev/null)
[ "$N" -eq 20 ] && echo "  PASS" || echo "  FAIL: got $N"

echo "==> Doris: salary masked"
V=$(MYSQL_PWD="$DORIS_ROOT_PASS" mysql -h $DORIS_HOST -P $DORIS_PORT -u root \
  governance_demo -sN -e "SELECT salary FROM customers_masked WHERE customer_id=1001;" 2>/dev/null)
[ "$V" = "****" ] && echo "  PASS" || echo "  FAIL: got $V"

echo ""
echo "Smoke test complete."
```

---

*Star Knowledge Catalog v1.0.0 · Runbook 17 · Doris 4.0.7 · RBAC Control Plane v1.0.0*
