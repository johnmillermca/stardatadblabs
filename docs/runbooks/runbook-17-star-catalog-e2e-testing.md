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
| T-53 | Classifier v2 | HIGH-confidence columns carry `confidence:"HIGH"` in scan response |
| T-54 | Classifier v2 | Exact-match score=1.0 → HIGH, no arbitration signals |
| T-55 | Classifier v2 | Substring hit that is a prefix → MEDIUM (arb_score ≥ 0.85) |
| T-56 | Classifier v2 | Sibling context boosts 0.7 hit from LOW to MEDIUM or HIGH |
| T-57 | Classifier v2 | Negative guard rejects `company_name` as `full_name` term |
| T-58 | Classifier v2 | MEDIUM tag uses classification_id only — glossary_term_id is null |
| T-59 | Classifier v2 | `use_conservative_policy: true` on MEDIUM scan result |
| T-60 | Classifier v2 | LOW column never creates a tag in PostgreSQL |
| T-61 | Classifier v2 | REJECT column carries `action: "rejected"` in scan response |
| T-62 | Governance circuit breaker | Disable governance for governance_demo — masking/query returns 503 |
| T-63 | Governance circuit breaker | Query planner blocks scans while disabled |
| T-64 | Governance circuit breaker | Re-enable governance — masking/query succeeds again |
| T-65 | Governance circuit breaker | Enable/disable state is visible via GET governance/status |

---

## Setup: shell environment

Run these once at the top of your terminal session. Every test below references these variables.

```bash
# ── Connection details ───────────────────────────────────────
export DORIS_HOST=192.168.1.50
export DORIS_PORT=30090
export PG_HOST=192.168.1.50
export PG_PORT=30532
export CATALOG_URL=http://192.168.1.50:30860
export RBAC_URL=http://192.168.1.50:30850

# ── Default passwords (change if you seeded different values) ─
export DORIS_ROOT_PASS=""          # empty on first boot
export ANALYST_PASS=analyst_pass_demo
export ADMIN_PASS=admin_pass_demo
export PG_STAR_PASS=changeme       # the password you set in A.1

# ── Catalog master token (matches MASTER_TOKEN in the secret) ─
export CATALOG_MASTER_TOKEN=changeme-catalog-master-token
export RBAC_MASTER_TOKEN=changeme-master-token
```

---

## Phase 1 — Infrastructure checks (T-01 to T-06)

### T-01 · Doris FE reachable

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT -u root -p"$DORIS_ROOT_PASS" \
  -e "SELECT 'doris_ok' AS status;" 2>&1
```

✅ **Pass:** output contains `doris_ok`  
❌ **Fail:** connection refused or access denied → check `kubectl get pods -n prod | grep doris-fe`

---

### T-02 · PostgreSQL reachable

```bash
psql -h $PG_HOST -p $PG_PORT -U star_catalog -d star_catalog \
  -c "SELECT 'pg_ok' AS status;" 2>&1
```

✅ **Pass:** output contains `pg_ok`  
❌ **Fail:** → check `kubectl get pods -n prod | grep postgresql`; confirm the `star_catalog` DB and user were created per Runbook 16 Part A.

---

### T-03 · Redis reachable

```bash
# Run from inside the cluster or via kubectl exec
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
❌ **Fail:** → check `kubectl describe pod -n prod -l app=star-knowledge-catalog` for image pull or secret errors

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
❌ **Fail:** `null` or empty → verify `MASTER_TOKEN` in the `star-catalog-credentials` Kubernetes Secret matches `$CATALOG_MASTER_TOKEN`

---

### T-08 · Wrong token returns 401

```bash
curl -s -o /dev/null -w "%{http_code}" -X POST $CATALOG_URL/api/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"token":"definitely-wrong-token"}'
```

✅ **Pass:** `401`  
❌ **Fail:** any other code

---

## Phase 3 — Data Classifications (T-09 to T-12)

### T-09 · Seed returns 5 classifications

```bash
curl -sf $CATALOG_URL/api/v1/classifications \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq 'length'
```

✅ **Pass:** `5`  
❌ **Fail:** → re-run `migrations/001_schema_and_seed.sql`

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

✅ **Pass:** create returns `{"name":"TEST_CLASS","sensitivity":"low"}`, update returns `"medium"`, delete returns `true`, final GET returns `404`

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

✅ **Pass:** create returns `{"name":"test_term","classification_name":"PII"}`, delete returns `true`, final GET returns `404`

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

✅ **Pass:** create returns `{"name":"TEST_ALGO","algorithm_type":"REDACT"}`, delete returns `true`

---

## Phase 6 — Masking Policies (T-19 to T-20)

### T-19 · Seed returns classification + term policies

```bash
curl -sf $CATALOG_URL/api/v1/policies \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq '[.[] | {name, priority}] | sort_by(.name)'
```

✅ **Pass:** list includes entries like `policy_pii_default` (priority 100) and `policy_term_email_address` (priority 200).

Check totals:
```bash
# Should be 4 classification-level + 10 term-level = 14 policies
curl -sf $CATALOG_URL/api/v1/policies \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq 'length'
```

✅ **Pass:** `14`

---

### T-20 · Term-level policy has higher priority than class-level

```bash
curl -sf $CATALOG_URL/api/v1/policies \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '[.[] | select(.name | startswith("policy_term_"))] | min_by(.priority) | .priority'
# Must be 200

curl -sf $CATALOG_URL/api/v1/policies \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '[.[] | select(.name | startswith("policy_pii_") or startswith("policy_pci_") or startswith("policy_phi_") or startswith("policy_confidential_"))] | max_by(.priority) | .priority'
# Must be 100
```

✅ **Pass:** first output `200`, second output `100`

---

## Phase 7 — Doris sample data (T-21 to T-23)

### T-21 · governance_demo has 4 base tables

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT -u root -p"$DORIS_ROOT_PASS" \
  -e "SELECT TABLE_NAME FROM information_schema.TABLES
      WHERE TABLE_SCHEMA='governance_demo' AND TABLE_TYPE='BASE TABLE'
      ORDER BY TABLE_NAME;" 2>/dev/null
```

✅ **Pass:** `customers`, `orders`, `payments`, `products`

---

### T-22 · customers has 20 rows

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT -u root -p"$DORIS_ROOT_PASS" \
  -e "SELECT COUNT(*) AS cnt FROM governance_demo.customers;" 2>/dev/null
```

✅ **Pass:** `cnt = 20`

---

### T-23 · payments has 40 rows

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT -u root -p"$DORIS_ROOT_PASS" \
  -e "SELECT COUNT(*) AS cnt FROM governance_demo.payments;" 2>/dev/null
```

✅ **Pass:** `cnt = 40`

---

## Phase 8 — Auto-classification scan (T-24 to T-28)

### T-24 · Dry-run detects 8 sensitive columns in customers

```bash
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","dry_run":true}' | \
  jq '.tables_results[0] | {table:.doris_table, columns_tagged,
      hits: [.results[] | select(.score >= 0.7) | {col:.column_name, term:.matched_term, score}]}'
```

✅ **Pass:** `columns_tagged = 8`, hits contains all of:
`full_name`, `email`, `phone_number`, `date_of_birth`, `national_id`, `street_address`, `ip_address`, `salary`

---

### T-25 · Dry-run detects 2 PCI columns in payments

```bash
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"payments","dry_run":true}' | \
  jq '.tables_results[0] | {table:.doris_table, columns_tagged,
      hits: [.results[] | select(.score >= 0.7) | {col:.column_name, term:.matched_term, class:.matched_classification}]}'
```

✅ **Pass:** `columns_tagged = 2`, hits contain `card_number → credit_card_number [PCI]` and `credit_card_cvv → credit_card_cvv [PCI]`

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

### T-27 · 10 column tags exist in PostgreSQL

```bash
curl -sf "$CATALOG_URL/api/v1/columns?database=governance_demo" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq 'length'
```

✅ **Pass:** `10`

Inspect the full tag list:
```bash
curl -sf "$CATALOG_URL/api/v1/columns?database=governance_demo" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq -r '.[] | "\(.doris_table).\(.column_name)\t→ \(.glossary_term_name // "—")\t[\(.classification_name // "—")]\tscore:\(.detection_score)"' | \
  column -t
```

✅ **Expected output:**
```
customers.date_of_birth   → date_of_birth        [PII]          score:1.0
customers.email           → email_address         [PII]          score:0.9
customers.full_name       → full_name             [PII]          score:1.0
customers.ip_address      → ip_address            [PII]          score:1.0
customers.national_id     → national_id           [PII]          score:0.9
customers.phone_number    → phone_number          [PII]          score:1.0
customers.salary          → salary                [CONFIDENTIAL] score:1.0
customers.street_address  → street_address        [PII]          score:1.0
payments.card_number      → credit_card_number    [PCI]          score:0.9
payments.credit_card_cvv  → credit_card_cvv       [PCI]          score:1.0
```

---

### T-28 · Manual tag not overwritten by re-scan

```bash
# Manually tag the 'city' column (not auto-detected — below threshold)
PII_ID=$(curl -sf $CATALOG_URL/api/v1/classifications/PII \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

curl -sf -X POST $CATALOG_URL/api/v1/columns \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"doris_database\":\"governance_demo\",\"doris_table\":\"customers\",
       \"column_name\":\"city\",\"classification_id\":$PII_ID,
       \"override_reason\":\"Quasi-identifier under GDPR recital 26\"}" | jq '{column_name, auto_detected, override_reason}'

# Re-run scan with overwrite_existing: true — manual tags must survive
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false,"overwrite_existing":true}' | \
  jq '.tables_results[] | select(.doris_table=="customers") | .results[] | select(.column_name=="city")'

# Verify city tag still exists and is still manual (auto_detected=false)
curl -sf $CATALOG_URL/api/v1/columns/governance_demo/customers/city \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq '{column_name, auto_detected, classification_name, override_reason}'
```

✅ **Pass:** `auto_detected: false`, `override_reason` is still populated, scan result for `city` shows `action: "skipped"`

---

## Phase 9 — Apply masked views (T-29 to T-32)

### T-29 · Dry-run shows DDL preview

```bash
curl -sf -X POST $CATALOG_URL/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","dry_run":true}' | \
  jq '.results[0] | {action, view_name, columns_masked, ddl_preview: .detail}' | head -40
```

✅ **Pass:** `action: "dry_run"`, `view_name: "customers_masked"`, DDL in `detail` contains `CREATE OR REPLACE VIEW` and `SHA2(`full_name`, 256)`

---

### T-30 · Live apply creates both masked views in Doris

```bash
curl -sf -X POST $CATALOG_URL/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false,"force":false}' | \
  jq '.results[] | {table:.doris_table, view:.view_name, action}'
```

✅ **Pass:**
```json
{"table": "customers", "view": "customers_masked", "action": "created"}
{"table": "payments",  "view": "payments_masked",  "action": "created"}
```
(orders and products will show `action: "skipped"` — no sensitive columns)

Confirm views exist directly in Doris:
```bash
mysql -h $DORIS_HOST -P $DORIS_PORT -u root -p"$DORIS_ROOT_PASS" \
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
  {"base_table": "customers", "view_name": "customers_masked", "columns_masked_count": 8},
  {"base_table": "payments",  "view_name": "payments_masked",  "columns_masked_count": 2}
]
```
(count includes the manually-tagged `city` column if T-28 was run)

---

### T-32 · orders and products have no masked view

```bash
curl -sf "$CATALOG_URL/api/v1/masking/views?database=governance_demo" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '[.[].base_table]'
```

✅ **Pass:** list contains `customers` and `payments` only — not `orders` or `products`

---

## Phase 10 — Role-aware query planner (T-33 to T-34)

First, make sure the RBAC users exist. If you haven't done Runbook 16 Part C yet, run this block:

```bash
# Get RBAC token
RBAC_TOKEN=$(curl -sf -X POST $RBAC_URL/api/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d "{\"token\":\"$RBAC_MASTER_TOKEN\"}" | jq -r .access_token)

# Create analyst user 'alice' (idempotent — ignore 409)
curl -s -X POST $RBAC_URL/api/v1/users \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","display_name":"Alice Fontaine","email":"alice@example.com"}' \
  | jq '{username, id}' 2>/dev/null || true

# Bind analyst role to alice
curl -s -X POST $RBAC_URL/api/v1/users/alice/bindings \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"role_name":"analyst","service_name":"doris"}' | jq .role_name 2>/dev/null || true

# Create data_admin user 'bob' (idempotent)
curl -s -X POST $RBAC_URL/api/v1/users \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"bob","display_name":"Bob Admin","email":"bob@example.com"}' \
  | jq '{username, id}' 2>/dev/null || true

# Bind data_admin role to bob
curl -s -X POST $RBAC_URL/api/v1/users/bob/bindings \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"role_name":"data_admin","service_name":"doris"}' | jq .role_name 2>/dev/null || true

echo "RBAC users ready"
```

---

### T-33 · Query planner routes analyst to masked_view

```bash
curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","doris_database":"governance_demo","doris_table":"customers","limit":10}' | \
  jq '{username, role, target, sql, columns_masked}'
```

✅ **Pass:**
```json
{
  "username": "alice",
  "role": "analyst",
  "target": "masked_view",
  "sql": "SELECT *\nFROM `governance_demo`.`customers_masked`\nLIMIT 10;",
  "columns_masked": ["full_name","email","phone_number","date_of_birth","national_id","street_address","ip_address","salary"]
}
```

---

### T-34 · Query planner routes data_admin to base_table

```bash
curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"bob","doris_database":"governance_demo","doris_table":"customers","limit":10}' | \
  jq '{username, role, target, columns_masked}'
```

✅ **Pass:**
```json
{
  "username": "bob",
  "role": "data_admin",
  "target": "base_table",
  "columns_masked": []
}
```

---

## Phase 11 — Doris masked query execution (T-35 to T-44)

Run all of these as the `analyst` Doris user.

### T-35 · analyst can SELECT from customers_masked

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT COUNT(*) AS row_count FROM customers_masked;" 2>/dev/null
```

✅ **Pass:** `row_count = 20`

---

### T-36 · full_name is SHA-256 hashed

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT customer_id, full_name FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `full_name` is a 64-character hex string (SHA-256), not `Alice Fontaine`

Quick verification of length:
```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT LENGTH(full_name) AS len FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `len = 64`

---

### T-37 · email shows first 2 chars + domain only

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT customer_id, email FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** value matches pattern `al***@example.com` (starts with `al`, has `***`, ends with `@example.com`)

---

### T-38 · date_of_birth generalised to Jan 1

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT customer_id, date_of_birth FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `date_of_birth = 1988-01-01` (original is `1988-03-14`)

---

### T-39 · national_id is `****`

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT customer_id, national_id FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `national_id = ****`

---

### T-40 · ip_address last octet zeroed

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT customer_id, ip_address FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `ip_address = 192.168.1.0` (original `192.168.1.101` → last octet zeroed)

---

### T-41 · salary is `****`

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT customer_id, salary FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `salary = ****`

---

### T-42 · card_number shows last 4 digits only

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT payment_id, card_number FROM payments_masked WHERE payment_id=9001;" 2>/dev/null
```

✅ **Pass:** `card_number` ends with `0001` (original `4532015112830001`), preceded by `*` characters

---

### T-43 · credit_card_cvv is `****`

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT payment_id, credit_card_cvv FROM payments_masked WHERE payment_id=9001;" 2>/dev/null
```

✅ **Pass:** `credit_card_cvv = ****`

---

### T-44 · Non-sensitive columns pass through unchanged

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT customer_id, customer_tier, city, country_code, is_active
      FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:**
```
customer_id  customer_tier  city         country_code  is_active
1001         gold           Springfield  US            1
```
All columns show their original values — only sensitive columns are masked.

---

## Phase 12 — Negative tests (T-45 to T-46)

### T-45 · analyst CANNOT SELECT base customers table

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT full_name, email, salary FROM customers LIMIT 1;" 2>&1 | grep -i "denied\|ERROR"
```

✅ **Pass:** output contains `ERROR` and `denied`  
❌ **Fail:** if query returns actual data — analyst has been granted direct table access, which must be revoked:
```bash
mysql -h $DORIS_HOST -P $DORIS_PORT -u root -p"$DORIS_ROOT_PASS" \
  -e "REVOKE SELECT_PRIV ON governance_demo.customers FROM 'analyst'@'%';" 2>/dev/null
```

---

### T-46 · analyst CANNOT SELECT base payments table

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT card_number, credit_card_cvv FROM payments LIMIT 1;" 2>&1 | grep -i "denied\|ERROR"
```

✅ **Pass:** output contains `ERROR` and `denied`

---

## Phase 13 — Admin clear access (T-47)

### T-47 · data_admin_user sees raw unmasked data

```bash
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u data_admin_user -p"$ADMIN_PASS" \
  governance_demo \
  -e "SELECT customer_id, full_name, email, date_of_birth, national_id,
             ip_address, salary
      FROM customers WHERE customer_id=1001;" 2>/dev/null
```

✅ **Pass:** `full_name = Alice Fontaine`, `email = alice.fontaine@example.com`, `date_of_birth = 1988-03-14`, `national_id = SSN-001-00-0001`, `salary = 78000.00` — all real values, no masking

---

## Phase 14 — Masking exception grant (T-48)

### T-48 · Grant exception to a new role, verify routing changes

```bash
# Step 1 — Create a new user 'eve' with role 'data_steward' (no exception yet)
curl -s -X POST $RBAC_URL/api/v1/users \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"eve","display_name":"Eve Steward","email":"eve@example.com"}' | jq .username

curl -s -X POST $RBAC_URL/api/v1/roles \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"data_steward","display_name":"Data Steward",
       "description":"Data steward with governed clear access"}' | jq .name 2>/dev/null || true

curl -s -X POST $RBAC_URL/api/v1/users/eve/bindings \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"role_name":"data_steward","service_name":"doris"}' | jq .role_name

# Step 2 — Without exception, eve should be routed to masked_view
curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"eve","doris_database":"governance_demo","doris_table":"customers","limit":5}' | \
  jq '{role, target}'
# Expected: target = "masked_view"

# Step 3 — Grant exception to data_steward for PII and CONFIDENTIAL
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

# Step 4 — Now eve must be routed to base_table
curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"eve","doris_database":"governance_demo","doris_table":"customers","limit":5}' | \
  jq '{role, target}'
# Expected: target = "base_table"
```

✅ **Pass:** Step 2 returns `"masked_view"`, Step 4 returns `"base_table"`

---

## Phase 15 — Policy update → view regeneration (T-49)

### T-49 · Change algorithm, re-apply, verify new expression in Doris

```bash
# Step 1 — Get the policy for the salary term
SALARY_POLICY=$(curl -sf $CATALOG_URL/api/v1/policies/policy_term_salary \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq '{id, name, algorithm_name}')
echo "Current salary policy: $SALARY_POLICY"
# Should show algorithm_name: FULL_REDACT

# Step 2 — Get the NULL_OUT algorithm ID
NULL_ALGO_ID=$(curl -sf $CATALOG_URL/api/v1/algorithms/NULL_OUT \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

# Step 3 — Update the policy to use NULL_OUT instead of FULL_REDACT
curl -sf -X PATCH $CATALOG_URL/api/v1/policies/policy_term_salary \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"algorithm_id\": $NULL_ALGO_ID}" | jq '{name, algorithm_name}'

# Step 4 — Re-apply with force=true to regenerate the view
curl -sf -X POST $CATALOG_URL/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","force":true}' | \
  jq '.results[0] | {action}'
# Expected: "updated"

# Step 5 — Verify salary is now NULL (not '****') in Doris
mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT customer_id, salary FROM customers_masked WHERE customer_id=1001;" 2>/dev/null
# Expected: salary = NULL

# Step 6 — Restore original policy
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

✅ **Pass:** Step 3 shows `algorithm_name: "NULL_OUT"`, Step 4 shows `action: "updated"`, Step 5 shows `salary = NULL`, Step 6 restores `FULL_REDACT`

---

## Phase 16 — Performance validation (T-50 to T-52)

### T-50 · Masked view query completes in < 1 second

```bash
time mysql -h $DORIS_HOST -P $DORIS_PORT \
  -u analyst -p"$ANALYST_PASS" \
  governance_demo \
  -e "SELECT customer_tier, COUNT(*) AS cnt, COUNT(DISTINCT country_code) AS countries
      FROM customers_masked
      GROUP BY customer_tier
      ORDER BY cnt DESC;" 2>/dev/null
```

✅ **Pass:** wall-clock time `< 1.0s`; result shows correct grouping by tier

Compare against base table (should be similar):
```bash
time mysql -h $DORIS_HOST -P $DORIS_PORT -u root -p"$DORIS_ROOT_PASS" \
  governance_demo \
  -e "SELECT customer_tier, COUNT(*) FROM customers GROUP BY customer_tier;" 2>/dev/null
```

✅ **Pass:** both queries are sub-second; masked view overhead is negligible

---

### T-51 · Cache hit on second identical API call

```bash
# First call — cache miss — triggers PostgreSQL lookup
time curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","doris_database":"governance_demo","doris_table":"customers","limit":1}' \
  > /dev/null

# Second identical call — served from in-process LRU cache
time curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","doris_database":"governance_demo","doris_table":"customers","limit":1}' \
  > /dev/null
```

✅ **Pass:** second call completes measurably faster (typically < 5 ms vs 20–100 ms for first)

---

### T-52 · Re-running scan + apply is a no-op

```bash
# Re-run scan (all columns already tagged)
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false,"overwrite_existing":false}' | \
  jq '{tables_scanned, skipped_total: [.tables_results[].results[] | select(.action=="skipped")] | length}'

# Re-apply views (DDL unchanged — should be "unchanged" for both)
curl -sf -X POST $CATALOG_URL/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false,"force":false}' | \
  jq '.results[] | select(.action != "skipped") | {table:.doris_table, action}'
```

✅ **Pass:** scan shows all tagged columns with `action: "skipped"`, apply shows all tables with `action: "unchanged"` (no views re-created unnecessarily)

---

## Phase 17 — Classifier v2 confidence signals (T-53 to T-61)

> These tests verify the two-stage scoring engine introduced in Classifier v2: exact/word-boundary matches are promoted directly to HIGH; 0.7 substring matches go through three-signal arbitration (position, sibling context, negative guard). The `confidence`, `arb_score`, `arb_signals`, and `use_conservative_policy` fields must be present in every scan result.

### T-53 · HIGH-confidence columns carry `confidence:"HIGH"` in scan response

```bash
# Dry-run scan of customers — inspect confidence field on each result
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","dry_run":true}' | \
  jq '.tables_results[0].results[] | {col:.column_name, score, confidence, action}' | head -60
```

✅ **Pass:** every column with `score: 1` or `score: 0.9` shows `confidence: "HIGH"`.
Columns with `score: 0.7` show `confidence: "MEDIUM"`, `"LOW"`, or `"REJECT"`.

---

### T-54 · Exact-match score=1.0 → HIGH, no arbitration signals in arb_signals

```bash
# full_name = exact match → score 1.0 → HIGH, arb_signals contains only the base note
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","dry_run":true}' | \
  jq '.tables_results[0].results[] | select(.column_name == "full_name") |
      {col:.column_name, score, confidence, arb_score, arb_signals}'
```

✅ **Pass:**
```json
{
  "col": "full_name",
  "score": 1,
  "confidence": "HIGH",
  "arb_score": 1,
  "arb_signals": ["base_score=1.0 → direct HIGH"]
}
```

---

### T-55 · Substring hit that is a prefix → MEDIUM (arb_score ≥ 0.85)

> The `customers` table does **not** have a pure-prefix 0.7 test column in the seed data. Use a temporary dry-run against an ad-hoc table you create, or verify the principle by adding a column to the scan payload using the manual column endpoint and checking arb signals.

Use a one-shot SQL column check via the classifier logic inspection:

```bash
# Create a temp table in Doris with a 0.7-prefix column 'addr_line1'
# (matches street_address term via substring 'addr', prefix position)
mysql -h $DORIS_HOST -P $DORIS_PORT -u root -p"$DORIS_ROOT_PASS" \
  governance_demo -e "
  CREATE TABLE IF NOT EXISTS classifier_test (
    id         INT NOT NULL,
    addr_line1 VARCHAR(200),
    addr_notes VARCHAR(200),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  ) ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
  PROPERTIES ('replication_num'='1');" 2>/dev/null

# Dry-run scan — addr_line1 is a prefix match (addr → address)
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"classifier_test","dry_run":true}' | \
  jq '.tables_results[0].results[] | select(.score == 0.7) |
      {col:.column_name, score, confidence, arb_score, arb_signals, use_conservative_policy}'
```

✅ **Pass:** `addr_line1` shows `score: 0.7`, `confidence: "MEDIUM"` or `"HIGH"` (depending on sibling context), `arb_signals` contains an entry starting with `"A:prefix"`, `use_conservative_policy` is `true` for MEDIUM results.

---

### T-56 · Sibling context boosts 0.7 hit from LOW to MEDIUM or HIGH

```bash
# Add a column that would be LOW alone but benefits from confirmed siblings
# 'addr_notes' (substring match 'addr', middle position → A:middle +0.00)
# Without sibling → arb_score = 0.70 (A=0.00, B=0.00) → LOW
# With addr_line1 already HIGH/MEDIUM in the same scan pass → B context fires

curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"classifier_test","dry_run":true}' | \
  jq '.tables_results[0].results[] | select(.column_name == "addr_notes") |
      {col:.column_name, score, confidence, arb_score, arb_signals}'
```

✅ **Pass (if addr_line1 was tagged MEDIUM/HIGH earlier in the column scan order):**
`arb_signals` contains `"B:weak_context(+0.12)"` or `"B:strong_context(+0.20)"` and `arb_score` is ≥ 0.82.

> **Note:** Doris returns columns in ordinal order. `addr_line1` (ordinal 2) appears before `addr_notes` (ordinal 3), so Signal B will have one confirmed sibling — `+0.12`. Combined: `0.7 + 0.00 + 0.12 = 0.82` → still LOW unless Signal A also fires. This is expected and correct — the test verifies Signal B fires and is recorded in `arb_signals`.

---

### T-57 · Negative guard rejects `company_name` as `full_name` term

```bash
# Add a 'company_name' column to classifier_test
mysql -h $DORIS_HOST -P $DORIS_PORT -u root -p"$DORIS_ROOT_PASS" \
  governance_demo -e "
  ALTER TABLE classifier_test
    ADD COLUMN company_name VARCHAR(200);" 2>/dev/null || true

# Dry-run — company_name should be REJECTED for the full_name term
# because 'company' appears in full_name.negative_patterns
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"classifier_test","dry_run":true}' | \
  jq '.tables_results[0].results[] | select(.column_name == "company_name") |
      {col:.column_name, score, confidence, action, arb_signals}'
```

✅ **Pass:**
```json
{
  "col": "company_name",
  "score": 0.7,
  "confidence": "REJECT",
  "action": "dry_run",
  "arb_signals": ["C:negative_guard — 'company' found in 'company_name'"]
}
```

> Signal C fires immediately before A and B are evaluated. The negative guard uses the `negative_patterns` column on the `glossary_terms` table seeded by `migrations/003_negative_patterns.sql`.

---

### T-58 · MEDIUM tag uses classification_id only — glossary_term_id is null

> After a live scan (not dry-run), MEDIUM-confidence tags must have `glossary_term_id = null` (conservative — linked to classification only, not the specific term).

```bash
# Run live scan on classifier_test (overwrite allowed since it's a test table)
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"classifier_test",
       "dry_run":false,"overwrite_existing":true}' | \
  jq '.tables_results[0].results[] | select(.confidence == "MEDIUM") |
      {col:.column_name, confidence, action}'

# Inspect the persisted tag for any MEDIUM column
MEDIUM_COL=$(curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"classifier_test","dry_run":true}' | \
  jq -r '.tables_results[0].results[] | select(.confidence == "MEDIUM") | .column_name' | head -1)

if [ -n "$MEDIUM_COL" ]; then
  curl -sf "$CATALOG_URL/api/v1/columns/governance_demo/classifier_test/$MEDIUM_COL" \
    -H "Authorization: Bearer $CATALOG_TOKEN" | \
    jq '{column_name, glossary_term_id, glossary_term_name, classification_id, classification_name, auto_detected}'
fi
```

✅ **Pass:** the persisted tag for any MEDIUM column shows `glossary_term_id: null`, `glossary_term_name: null`, `classification_id` is set, `classification_name` is the correct PII/PCI/CONFIDENTIAL class.

---

### T-59 · `use_conservative_policy: true` on MEDIUM scan result

```bash
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"classifier_test","dry_run":true}' | \
  jq '.tables_results[0].results[] | select(.confidence == "MEDIUM") |
      {col:.column_name, use_conservative_policy}'
```

✅ **Pass:** every result with `confidence: "MEDIUM"` has `use_conservative_policy: true`.
Every result with `confidence: "HIGH"` has `use_conservative_policy: false`.

---

### T-60 · LOW column never creates a tag in PostgreSQL

```bash
# Verify no tag exists for 'addr_notes' (expected to stay LOW in most conditions)
curl -sf "$CATALOG_URL/api/v1/columns/governance_demo/classifier_test/addr_notes" \
  -H "Authorization: Bearer $CATALOG_TOKEN" 2>&1 | grep -q '"detail"' && \
  echo "PASS: addr_notes has no tag (404 as expected)" || \
  curl -sf "$CATALOG_URL/api/v1/columns/governance_demo/classifier_test/addr_notes" \
    -H "Authorization: Bearer $CATALOG_TOKEN" | jq '{column_name, confidence: .detection_score}'
```

✅ **Pass:** API returns HTTP 404 — no tag was persisted for the LOW-confidence column.

---

### T-61 · REJECT column carries `action: "rejected"` in scan response

```bash
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"classifier_test","dry_run":false,
       "overwrite_existing":true}' | \
  jq '.tables_results[0].results[] | select(.action == "rejected") |
      {col:.column_name, score, confidence, arb_signals}'
```

✅ **Pass:** `company_name` (and any other column whose name contains a negative-pattern token) appears with `action: "rejected"`, `confidence: "REJECT"`, and `arb_signals` shows the `C:negative_guard` reason.

Verify no tag was persisted for `company_name`:
```bash
curl -sf "$CATALOG_URL/api/v1/columns/governance_demo/classifier_test/company_name" \
  -H "Authorization: Bearer $CATALOG_TOKEN" 2>&1 | grep -q '"detail"' && \
  echo "PASS: no tag for company_name" || echo "FAIL: tag exists — REJECT should not tag"
```

✅ **Pass:** HTTP 404 — REJECT columns produce no tag regardless of `dry_run` and `overwrite_existing`.

**Cleanup** (optional, keeps Doris tidy):
```bash
mysql -h $DORIS_HOST -P $DORIS_PORT -u root -p"$DORIS_ROOT_PASS" \
  governance_demo -e "DROP TABLE IF EXISTS classifier_test;" 2>/dev/null
```

---

## Phase 18 — Governance circuit breaker (T-62 to T-65)

> The governance circuit breaker lets an operator disable masking enforcement for an entire Doris database in one API call — for example during a break-glass incident, a migration, or a Doris upgrade. While disabled, `masking/query` and `masking/apply` return HTTP 503. Re-enabling restores normal routing immediately.

### T-62 · Disable governance for governance_demo — masking/query returns 503

```bash
# Disable governance for the governance_demo database
curl -sf -X POST "$CATALOG_URL/api/v1/governance/governance_demo/disable" \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"reason":"T-62: circuit breaker test — planned disable","disabled_by":"test"}' | \
  jq '{database, enabled, reason}'
```

✅ **Pass:**
```json
{"database": "governance_demo", "enabled": false, "reason": "T-62: circuit breaker test — planned disable"}
```

Now verify that masking/query is blocked:
```bash
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","doris_database":"governance_demo","doris_table":"customers","limit":1}')
echo "HTTP status: $HTTP_CODE"
[ "$HTTP_CODE" -eq 503 ] && echo "PASS" || echo "FAIL: expected 503"
```

✅ **Pass:** HTTP 503 with body:
```json
{"detail": "Governance is disabled for database 'governance_demo'. Contact your data admin."}
```

---

### T-63 · Scan endpoint also blocked while governance is disabled

```bash
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST $CATALOG_URL/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false}')
echo "HTTP status: $HTTP_CODE"
[ "$HTTP_CODE" -eq 503 ] && echo "PASS: apply blocked" || echo "FAIL: expected 503"
```

✅ **Pass:** `masking/apply` also returns HTTP 503 when governance is disabled — view regeneration is prevented during the break-glass window.

> **Note:** `GET /columns` (listing existing tags) and `POST /columns/scan` are **not** blocked — an operator can still inspect and update tags while masking is paused. Only the view application and query routing calls are gated.

---

### T-64 · Re-enable governance — masking/query succeeds again

```bash
# Re-enable
curl -sf -X POST "$CATALOG_URL/api/v1/governance/governance_demo/enable" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '{database, enabled}'
```

✅ **Pass:**
```json
{"database": "governance_demo", "enabled": true}
```

Immediately verify masking/query is unblocked:
```bash
curl -sf -X POST $CATALOG_URL/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","doris_database":"governance_demo","doris_table":"customers","limit":1}' | \
  jq '{role, target, note}'
```

✅ **Pass:** response returns `"target": "masked_view"` (alice is analyst-role) — governance is active again.

---

### T-65 · Enable/disable state is visible via GET governance status

```bash
# Check current status of all governed databases
curl -sf "$CATALOG_URL/api/v1/governance" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '[.[] | {database: .doris_database, enabled, disabled_reason}]'
```

✅ **Pass:** `governance_demo` appears in the list with `"enabled": true` after T-64. If you run T-62 without T-64, it would show `"enabled": false` with the disable reason.

Test toggle once more and observe state change:
```bash
# Disable
curl -sf -X POST "$CATALOG_URL/api/v1/governance/governance_demo/disable" \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"reason":"T-65 state verification test","disabled_by":"test"}' | jq '{enabled}'

# Check list — must show false
curl -sf "$CATALOG_URL/api/v1/governance" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '.[] | select(.doris_database == "governance_demo") | {database:.doris_database, enabled}'

# Re-enable immediately
curl -sf -X POST "$CATALOG_URL/api/v1/governance/governance_demo/enable" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq '{enabled}'

# Confirm enabled
curl -sf "$CATALOG_URL/api/v1/governance" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '.[] | select(.doris_database == "governance_demo") | {database:.doris_database, enabled}'
```

✅ **Pass:** state transitions from `true → false → true` and is reflected immediately in the list endpoint.

---

## Test Results Summary

After completing all phases, fill in this table:

| Phase | Tests | Status |
|---|---|---|
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
| 17 — Classifier v2 confidence | T-53 to T-61 | ⬜ |
| 18 — Governance circuit breaker | T-62 to T-65 | ⬜ |

---

## Quick re-test one-liner (smoke test only)

For a fast daily smoke test after a deployment:

```bash
set -e
export CATALOG_URL=http://192.168.1.50:30860
export CATALOG_MASTER_TOKEN=changeme-catalog-master-token
export DORIS_HOST=192.168.1.50 DORIS_PORT=30090 ANALYST_PASS=analyst_pass_demo

echo "==> Health"
curl -sf $CATALOG_URL/health | jq -e '.status == "ok"' && echo "  PASS"

echo "==> Auth"
TOK=$(curl -sf -X POST $CATALOG_URL/api/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d "{\"token\":\"$CATALOG_MASTER_TOKEN\"}" | jq -r .access_token)
[ "${#TOK}" -gt 100 ] && echo "  PASS"

echo "==> Classifications (5 expected)"
N=$(curl -sf $CATALOG_URL/api/v1/classifications -H "Authorization: Bearer $TOK" | jq length)
[ "$N" -eq 5 ] && echo "  PASS" || echo "  FAIL: got $N"

echo "==> Glossary terms (10 expected)"
N=$(curl -sf $CATALOG_URL/api/v1/glossary -H "Authorization: Bearer $TOK" | jq length)
[ "$N" -ge 10 ] && echo "  PASS" || echo "  FAIL: got $N"

echo "==> Algorithms (8 expected)"
N=$(curl -sf $CATALOG_URL/api/v1/algorithms -H "Authorization: Bearer $TOK" | jq length)
[ "$N" -eq 8 ] && echo "  PASS" || echo "  FAIL: got $N"

echo "==> Doris: customers row count (20 expected)"
N=$(mysql -h $DORIS_HOST -P $DORIS_PORT -u analyst -p"$ANALYST_PASS" \
  governance_demo -sN -e "SELECT COUNT(*) FROM customers_masked;" 2>/dev/null)
[ "$N" -eq 20 ] && echo "  PASS" || echo "  FAIL: got $N"

echo "==> Doris: salary masked ('****' expected)"
V=$(mysql -h $DORIS_HOST -P $DORIS_PORT -u analyst -p"$ANALYST_PASS" \
  governance_demo -sN -e "SELECT salary FROM customers_masked WHERE customer_id=1001;" 2>/dev/null)
[ "$V" = "****" ] && echo "  PASS" || echo "  FAIL: got $V"

echo "==> Governance: governance_demo enabled"
ENABLED=$(curl -sf "$CATALOG_URL/api/v1/governance" -H "Authorization: Bearer $TOK" | \
  jq -r '.[] | select(.doris_database == "governance_demo") | .enabled')
[ "$ENABLED" = "true" ] && echo "  PASS" || echo "  FAIL: governance disabled (run T-64)"

echo "==> Scan response has confidence field"
CONF=$(curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $TOK" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","dry_run":true}' | \
  jq -r '.tables_results[0].results[0].confidence')
[ -n "$CONF" ] && [ "$CONF" != "null" ] && echo "  PASS: confidence=$CONF" || echo "  FAIL: confidence field missing"

echo ""
echo "Smoke test complete."
```

---

*Star Knowledge Catalog v1.0.0 · Runbook 17 · Doris 4.0.7 · RBAC Control Plane v1.0.0*
