# Runbook 16 — Data Governance & Automatic Column Masking

> **Component:** Star Knowledge Catalog  
> **Namespace:** `prod`  
> **API:** `http://192.168.1.50:30860` · Docs: `http://192.168.1.50:30860/docs`  
> **Related runbooks:** [12 — RBAC New User Setup](runbook-12-rbac-new-user-testing.md) · [13 — User Groups](runbook-13-user-groups-and-access-testing.md) · [04 — Databases](runbook-04-databases.md)

---

## Overview

**Star Knowledge Catalog** is an IBM Knowledge Catalog-inspired governance layer for Apache Doris. It provides:

- **Data Classifications** — sensitivity tiers (PII, PCI, PHI, CONFIDENTIAL, PUBLIC)
- **Business Glossary** — curated terms with keyword patterns used for automatic column detection
- **Masking Algorithms** — named Doris-native SQL expressions (SHA-256, email partial, credit card last-4, etc.)
- **Masking Policies** — bind a classification or glossary term to an algorithm
- **Auto Column Tagger** — scans Doris table schemas and tags sensitive columns without manual intervention
- **Masked Views** — generates `CREATE OR REPLACE VIEW` DDL in Doris; masking is applied natively by the Doris vectorised engine at query time (sub-second performance preserved)
- **Role-Aware Query Router** — integrates with the RBAC Control Plane to route analyst users to masked views and privileged roles directly to base tables

### Data flow

```
1. Data lands in Doris base tables (governance_demo.customers, .payments, ...)
                  ↓
2. Star Catalog scans column names → matches against Glossary Term patterns
                  ↓
3. Column Tags created automatically (e.g. customers.email → email_address / PII)
                  ↓
4. Masking Policies resolved (term-level priority > classification-level)
                  ↓
5. Engine generates Doris VIEW DDL with masking expressions inline
                  ↓
6. Views applied to Doris (CREATE OR REPLACE VIEW customers_masked)
                  ↓
7. analyst user → directed to customers_masked (masking at Doris vectorised layer)
   data_admin  → directed to customers           (full CLEAR access)
```

### Performance architecture

| Layer | Technology | Latency |
|---|---|---|
| Masking expression evaluation | Doris native vectorised engine | Same as projection push-down — microseconds per row |
| Column tag / policy resolution | Redis → in-process LRU cache | < 1 ms (cached) |
| Role routing decision | RBAC Control Plane (Redis-cached) | < 1 ms |
| Full query execution on masked view | Doris MPP with partition pruning | Sub-second for typical analyst queries |

---

## Prerequisites

| Item | Requirement |
|---|---|
| Apache Doris | Running at `192.168.1.50:30090` (see [Doris doc](../doris.md)) |
| RBAC Control Plane | Running at `192.168.1.50:30850` (see [Runbook 12](runbook-12-rbac-new-user-testing.md)) |
| PostgreSQL 17 | Running at `192.168.1.50:30532` |
| Redis | Running at `192.168.1.50` port 6379 |
| `mysql` client | Available on the operator workstation |
| `curl` + `jq` | Available on the operator workstation |

---

## Part A — First-Time Setup

### A.1 — Create the PostgreSQL catalog database

```bash
psql -h 192.168.1.50 -p 30532 -U postgres
```

```sql
CREATE DATABASE star_catalog;
CREATE USER star_catalog WITH PASSWORD 'your-secure-password';
GRANT ALL PRIVILEGES ON DATABASE star_catalog TO star_catalog;
\q
```

### A.2 — Run the schema migration

```bash
psql -h 192.168.1.50 -p 30532 \
  -U star_catalog \
  -d star_catalog \
  -f star-knowledge-catalog/migrations/001_schema_and_seed.sql
```

This migration creates all tables, indexes, and seeds:
- 5 data classifications (PII, PCI, PHI, CONFIDENTIAL, PUBLIC)
- 10 glossary terms (email_address, full_name, phone_number, date_of_birth, national_id, credit_card_number, credit_card_cvv, ip_address, street_address, salary)
- 8 masking algorithms (FULL_REDACT, SHA256_HASH, EMAIL_PARTIAL, CREDIT_CARD_LAST4, DATE_YEAR_ONLY, PHONE_LAST4, NULL_OUT, IP_LAST_OCTET)
- Classification-level and term-level masking policies
- Role masking exceptions for data_admin, platform_admin, account_admin

**Verify:**
```bash
psql -h 192.168.1.50 -p 30532 -U star_catalog -d star_catalog \
  -c "SELECT name, sensitivity FROM data_classifications ORDER BY name;"
```

Expected output:
```
     name      | sensitivity
---------------+-------------
 CONFIDENTIAL  | medium
 PCI           | critical
 PHI           | critical
 PII           | high
 PUBLIC        | low
```

---

## Part B — Create Sample Doris Database

### B.1 — Create the governance_demo database and tables

```bash
mysql -h 192.168.1.50 -P 30090 -u root \
  < star-knowledge-catalog/doris/001_create_schema.sql
```

This creates `governance_demo` with four tables:

| Table | PII Columns | PCI Columns | CONFIDENTIAL Columns |
|---|---|---|---|
| `customers` | full_name, email, phone_number, date_of_birth, national_id, street_address, ip_address | — | salary |
| `orders` | — | — | — |
| `payments` | — | card_number, credit_card_cvv | — |
| `products` | — | — | — |

### B.2 — Load synthetic seed data

```bash
mysql -h 192.168.1.50 -P 30090 -u root \
  < star-knowledge-catalog/doris/002_seed_data.sql
```

This loads 20 customers, 40 orders, 40 payments, 10 products.

**Verify:**
```bash
mysql -h 192.168.1.50 -P 30090 -u root \
  -e "SELECT COUNT(*) FROM governance_demo.customers;"
# Expected: 20
```

### B.3 — Confirm Doris users were created

```bash
mysql -h 192.168.1.50 -P 30090 -u root \
  -e "SELECT user, host FROM mysql.user WHERE user IN ('analyst','data_admin_user');"
```

---

## Part C — Wire the analyst role in the RBAC Control Plane

This section provisions the `alice` user with the `analyst` role and syncs her Doris account.

### C.1 — Get an RBAC API token

```bash
RBAC_TOKEN=$(curl -s -X POST http://192.168.1.50:30850/api/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"token":"changeme-master-token"}' | jq -r .access_token)
echo "RBAC token acquired: ${RBAC_TOKEN:0:20}..."
```

### C.2 — Create the analyst user

```bash
curl -s -X POST http://192.168.1.50:30850/api/v1/users \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "username": "alice",
    "display_name": "Alice Fontaine",
    "email": "alice@example.com"
  }' | jq .
```

### C.3 — Bind the analyst role to alice

```bash
curl -s -X POST http://192.168.1.50:30850/api/v1/users/alice/bindings \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "role_name": "analyst",
    "service_name": "doris"
  }' | jq .
```

### C.4 — Sync alice's role to Doris

```bash
curl -s -X POST http://192.168.1.50:30850/api/v1/sync \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","service":"doris"}' | jq .
```

Expected: `{"results":[{"username":"alice","service":"doris","status":"synced"}],"errors":0}`

### C.5 — Verify alice exists in Doris

```bash
mysql -h 192.168.1.50 -P 30090 -u root \
  -e "SHOW GRANTS FOR 'alice'@'%';"
```

---

## Part D — Auto-classify columns and apply masking

### D.1 — Get a Catalog API token

```bash
CATALOG_TOKEN=$(curl -s -X POST http://192.168.1.50:30860/api/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"token":"changeme-catalog-master-token"}' | jq -r .access_token)
echo "Catalog token acquired: ${CATALOG_TOKEN:0:20}..."
```

### D.2 — Run auto-classification scan (dry run first)

```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "doris_database": "governance_demo",
    "dry_run": true
  }' | jq '.tables_results[] | {table: .doris_table, tagged: .columns_tagged, results: [.results[] | select(.score >= 0.7)]}'
```

**Expected output (excerpt):**
```json
{
  "table": "customers",
  "tagged": 8,
  "results": [
    {"column_name": "full_name",      "matched_term": "full_name",           "score": 1.0, "action": "dry_run"},
    {"column_name": "email",          "matched_term": "email_address",       "score": 0.9, "action": "dry_run"},
    {"column_name": "phone_number",   "matched_term": "phone_number",        "score": 1.0, "action": "dry_run"},
    {"column_name": "date_of_birth",  "matched_term": "date_of_birth",       "score": 1.0, "action": "dry_run"},
    {"column_name": "national_id",    "matched_term": "national_id",         "score": 0.9, "action": "dry_run"},
    {"column_name": "street_address", "matched_term": "street_address",      "score": 1.0, "action": "dry_run"},
    {"column_name": "ip_address",     "matched_term": "ip_address",          "score": 1.0, "action": "dry_run"},
    {"column_name": "salary",         "matched_term": "salary",              "score": 1.0, "action": "dry_run"}
  ]
}
```

### D.3 — Apply auto-classification (persist tags)

```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "doris_database": "governance_demo",
    "dry_run": false,
    "overwrite_existing": false
  }' | jq '{tables_scanned, results: [.tables_results[] | {table:.doris_table, tagged:.columns_tagged}]}'
```

### D.4 — Review all column tags

```bash
curl -s "http://192.168.1.50:30860/api/v1/columns?database=governance_demo" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '.[] | "\(.doris_table).\(.column_name)  →  \(.glossary_term_name // "—")  [\(.classification_name // "—")]  score:\(.detection_score)"'
```

Expected output (customers table columns, formatted):
```
"customers.full_name  →  full_name  [PII]  score:1.0"
"customers.email  →  email_address  [PII]  score:0.9"
"customers.phone_number  →  phone_number  [PII]  score:1.0"
"customers.date_of_birth  →  date_of_birth  [PII]  score:1.0"
"customers.national_id  →  national_id  [PII]  score:0.9"
"customers.street_address  →  street_address  [PII]  score:1.0"
"customers.ip_address  →  ip_address  [PII]  score:1.0"
"customers.salary  →  salary  [CONFIDENTIAL]  score:1.0"
"payments.card_number  →  credit_card_number  [PCI]  score:0.9"
"payments.credit_card_cvv  →  credit_card_cvv  [PCI]  score:1.0"
```

### D.5 — Apply masked views to Doris

```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "doris_database": "governance_demo",
    "dry_run": false,
    "force": false
  }' | jq '.results[] | {table:.doris_table, view:.view_name, action:.action, masked:.columns_masked}'
```

Expected:
```json
[
  {"table":"customers","view":"customers_masked","action":"created","masked":["full_name","email","phone_number","date_of_birth","national_id","street_address","ip_address","salary"]},
  {"table":"payments","view":"payments_masked","action":"created","masked":["card_number","credit_card_cvv"]},
  {"table":"orders","view":null,"action":"skipped","masked":null},
  {"table":"products","view":null,"action":"skipped","masked":null}
]
```

### D.6 — Verify masked views exist in Doris

```bash
mysql -h 192.168.1.50 -P 30090 -u root \
  -e "SHOW TABLES IN governance_demo;" | grep masked
```

Expected:
```
customers_masked
payments_masked
```

---

## Part E — Query testing: analyst vs admin

### E.1 — Get the role-aware SQL for alice (analyst)

```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "username": "alice",
    "doris_database": "governance_demo",
    "doris_table": "customers",
    "limit": 5
  }' | jq .
```

Expected response:
```json
{
  "username": "alice",
  "role": "analyst",
  "target": "masked_view",
  "sql": "SELECT *\nFROM `governance_demo`.`customers_masked`\nLIMIT 5;",
  "columns_masked": ["full_name","email","phone_number","date_of_birth","national_id","street_address","ip_address","salary"],
  "columns_clear": [],
  "note": "Masked access: role 'analyst' is routed to view 'customers_masked'..."
}
```

### E.2 — Execute the query as alice in Doris

```bash
mysql -h 192.168.1.50 -P 30090 -u alice -panalyst_pass_demo \
  governance_demo \
  -e "SELECT customer_id, full_name, email, phone_number, date_of_birth, national_id, ip_address, salary FROM customers_masked LIMIT 5\G"
```

**Expected — masked output:**
```
customer_id: 1001
  full_name: 3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1a  ← SHA-256
      email: al***@example.com                                                      ← EMAIL_PARTIAL
phone_number: *******0101                                                           ← PHONE_LAST4
date_of_birth: 1988-01-01                                                           ← DATE_YEAR_ONLY
 national_id: ****                                                                  ← FULL_REDACT
  ip_address: 192.168.1.0                                                           ← IP_LAST_OCTET
      salary: ****                                                                  ← FULL_REDACT
```

### E.3 — Verify analyst CANNOT query the base table

```bash
mysql -h 192.168.1.50 -P 30090 -u alice -panalyst_pass_demo \
  governance_demo \
  -e "SELECT full_name, email, salary FROM customers LIMIT 1;" 2>&1
```

Expected: `ERROR 1142 (42000): SELECT command denied to user 'alice'@'%' for table 'customers'`

### E.4 — Verify data_admin sees unmasked data

```bash
# Get the role-aware SQL for a data_admin user
curl -s -X POST http://192.168.1.50:30860/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "username": "bob",
    "doris_database": "governance_demo",
    "doris_table": "customers",
    "limit": 5
  }' | jq '{role, target, note}'
```

Expected (assuming bob has data_admin role):
```json
{
  "role": "data_admin",
  "target": "base_table",
  "note": "Full access: role 'data_admin' has masking exception for all sensitive classifications in this table."
}
```

```bash
mysql -h 192.168.1.50 -P 30090 -u data_admin_user -padmin_pass_demo \
  governance_demo \
  -e "SELECT customer_id, full_name, email, salary FROM customers LIMIT 3\G"
```

Expected — **unmasked** clear text values.

---

## Part F — Manual column tag override

Use this when the auto-classifier missed a column or you need to override its assignment.

### F.1 — List current tag for a column

```bash
curl -s "http://192.168.1.50:30860/api/v1/columns/governance_demo/customers/city" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .
```

If `city` was not auto-tagged (score below threshold), the response is 404.

### F.2 — Manually tag the column

```bash
# Get the classification ID for PII
PII_ID=$(curl -s "http://192.168.1.50:30860/api/v1/classifications/PII" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

curl -s -X POST http://192.168.1.50:30860/api/v1/columns \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{
    \"doris_database\": \"governance_demo\",
    \"doris_table\": \"customers\",
    \"column_name\": \"city\",
    \"classification_id\": $PII_ID,
    \"override_reason\": \"City is quasi-identifier under GDPR recital 26\"
  }" | jq .
```

### F.3 — Re-apply masked view to pick up the new tag

```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "doris_database": "governance_demo",
    "doris_table": "customers",
    "force": true
  }' | jq .results[0]
```

The `city` column will now be masked by the PII default policy (SHA256_HASH) in the regenerated view.

---

## Part G — Add a new masking algorithm

### G.1 — Create a custom algorithm

```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/algorithms \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "CITY_GENERALIZE",
    "display_name": "City Generalisation",
    "description": "Replaces city name with its ISO country code. Reduces geo-precision for GDPR compliance.",
    "algorithm_type": "CUSTOM",
    "doris_expression": "country_code",
    "applicable_types": ["VARCHAR","TEXT"]
  }' | jq .
```

### G.2 — Create a term-level policy pointing to it

```bash
# Get IDs
CITY_TERM_ID=$(curl -s "http://192.168.1.50:30860/api/v1/glossary/city_name" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id // empty)

ALGO_ID=$(curl -s "http://192.168.1.50:30860/api/v1/algorithms/CITY_GENERALIZE" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

# Create term first if it doesn't exist
PII_ID=$(curl -s "http://192.168.1.50:30860/api/v1/classifications/PII" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

curl -s -X POST http://192.168.1.50:30860/api/v1/glossary \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{
    \"name\": \"city_name\",
    \"display_name\": \"City Name\",
    \"description\": \"Physical city location of a person.\",
    \"classification_id\": $PII_ID,
    \"column_name_patterns\": [\"city\",\"town\",\"municipality\",\"city_name\"],
    \"description_patterns\": [\"city\",\"town\",\"municipality\"]
  }" | jq .id

# Re-fetch term ID after creation
CITY_TERM_ID=$(curl -s "http://192.168.1.50:30860/api/v1/glossary/city_name" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

curl -s -X POST http://192.168.1.50:30860/api/v1/policies \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{
    \"name\": \"policy_term_city_name\",
    \"description\": \"Generalise city to country code for GDPR geo-reduction.\",
    \"glossary_term_id\": $CITY_TERM_ID,
    \"algorithm_id\": $ALGO_ID,
    \"priority\": 200
  }" | jq .
```

---

## Part H — Grant a masking exception to a new privileged role

### H.1 — Create the exception

```bash
# Get classification IDs
PII_ID=$(curl -s "http://192.168.1.50:30860/api/v1/classifications/PII" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)
PCI_ID=$(curl -s "http://192.168.1.50:30860/api/v1/classifications/PCI" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .id)

# Grant the 'data_steward' role CLEAR access to PII and PCI
for CLASS_ID in $PII_ID $PCI_ID; do
  curl -s -X POST http://192.168.1.50:30860/api/v1/exceptions \
    -H "Authorization: Bearer $CATALOG_TOKEN" \
    -H 'Content-Type: application/json' \
    -d "{
      \"role_name\": \"data_steward\",
      \"classification_id\": $CLASS_ID,
      \"granted_by\": \"platform_admin\"
    }" | jq .
done
```

### H.2 — Verify via the query planner

```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/masking/query \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "username": "eve",
    "doris_database": "governance_demo",
    "doris_table": "customers",
    "limit": 10
  }' | jq '{role, target}'
```

If `eve` has the `data_steward` role and the exception covers all PII+CONFIDENTIAL classifications in `customers`, `target` will be `"base_table"`.

---

## Part I — Performance validation

### I.1 — Measure masked view query latency

```bash
# Time a full-table scan on the masked view (analyst user)
time mysql -h 192.168.1.50 -P 30090 -u analyst -panalyst_pass_demo \
  governance_demo \
  -e "SELECT COUNT(*), customer_tier FROM customers_masked GROUP BY customer_tier;"
```

Typical result on 20-row demo dataset: **< 10 ms**. For production-scale datasets (millions of rows) the masking expressions (SHA2, CONCAT, DATE_FORMAT) are vectorised by Doris — expect similar or better latency vs. an equivalent projection on the base table.

### I.2 — Compare base vs masked view on the same query

```bash
# Base table (data_admin)
time mysql -h 192.168.1.50 -P 30090 -u root \
  -e "USE governance_demo; SELECT customer_tier, COUNT(*) FROM customers GROUP BY customer_tier;"

# Masked view (analyst)
time mysql -h 192.168.1.50 -P 30090 -u analyst -panalyst_pass_demo \
  -e "USE governance_demo; SELECT customer_tier, COUNT(*) FROM customers_masked GROUP BY customer_tier;"
```

Both queries should return in sub-second time. The masked view adds only:
- One extra projection step (scalar function per masked column)
- No joins, no sub-queries, no Python in the query path

### I.3 — Explain plan for masked view

```bash
mysql -h 192.168.1.50 -P 30090 -u root \
  -e "EXPLAIN SELECT * FROM governance_demo.customers_masked WHERE country_code='US' LIMIT 100;"
```

Verify that the plan shows:
- `TABLE SCAN` on the underlying `customers` base table
- `PREDICATE: (country_code = 'US')` pushed down to the scan layer
- Column projections evaluated as scalar functions in the `PROJECTION` node

---

## Part J — Inspect and update view manifests

### J.1 — List all view manifests

```bash
curl -s "http://192.168.1.50:30860/api/v1/masking/views?database=governance_demo" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | \
  jq '.[] | {database:.doris_database, base_table, view_name, masked:.columns_masked, last_applied:.last_applied_at}'
```

### J.2 — Force re-apply all views (after policy changes)

```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "doris_database": "governance_demo",
    "dry_run": false,
    "force": true
  }' | jq '.results[] | {table:.doris_table, action}'
```

---

## Troubleshooting

### T1 — Auto-scan finds 0 columns tagged

**Symptom:** `POST /api/v1/columns/scan` returns `columns_tagged: 0` for all tables.

**Cause:** Either the glossary terms have no patterns, or the `AUTO_CLASSIFY_THRESHOLD` is too high.

**Fix:**
```bash
# Check glossary term patterns
curl -s "http://192.168.1.50:30860/api/v1/glossary/email_address" \
  -H "Authorization: Bearer $CATALOG_TOKEN" | jq .column_name_patterns

# Lower threshold (temporarily) to debug
curl -s "http://192.168.1.50:30860/api/v1/columns/scan" \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":true}' | \
  jq '.tables_results[].results[] | select(.score > 0) | {column:.column_name, score, term:.matched_term}'
```

### T2 — Masked view returns `Table 'xxx_masked' doesn't exist`

**Symptom:** Analyst user gets an error accessing the masked view.

**Cause:** The masking engine has not yet applied the view to Doris.

**Fix:**
```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","force":true}' | jq .
```

### T3 — `POST /api/v1/masking/apply` returns `action: error`

**Symptom:** View apply fails with a Doris error in `detail`.

**Common causes:**

| Error message | Fix |
|---|---|
| `Access denied for user 'root'@...` | Check `DORIS_ADMIN_PASSWORD` in the secret |
| `Unknown column 'xxx' in 'field list'` | Column was renamed or dropped; re-run scan with `overwrite_existing: true` |
| `connect_timeout expired` | Doris FE is not reachable — check `kubectl get pods -n prod \| grep doris` |

### T4 — alice cannot connect to Doris

**Symptom:** `mysql -u alice ...` returns `ERROR 1045 (28000): Access denied`.

**Cause:** The RBAC Control Plane sync step was skipped or alice's Doris user was dropped.

**Fix:**
```bash
# Force re-sync alice to Doris
curl -s -X POST http://192.168.1.50:30850/api/v1/sync \
  -H "Authorization: Bearer $RBAC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","service":"doris"}' | jq .

# Manually grant masked views if sync doesn't include view grants
mysql -h 192.168.1.50 -P 30090 -u root \
  -e "GRANT SELECT_PRIV ON governance_demo.customers_masked TO 'alice'@'%';
      GRANT SELECT_PRIV ON governance_demo.payments_masked  TO 'alice'@'%';
      GRANT SELECT_PRIV ON governance_demo.orders           TO 'alice'@'%';
      GRANT SELECT_PRIV ON governance_demo.products         TO 'alice'@'%';"
```

### T5 — `POST /api/v1/masking/query` returns `User not found in RBAC Control Plane`

**Symptom:** 404 with "not found in RBAC Control Plane or has no roles".

**Cause:** The user does not exist in the RBAC Control Plane or has no active role bindings.

**Fix:**
```bash
# Verify user exists
curl -s "http://192.168.1.50:30850/api/v1/users/alice" \
  -H "Authorization: Bearer $RBAC_TOKEN" | jq .

# Verify role binding
curl -s "http://192.168.1.50:30850/api/v1/users/alice/roles" \
  -H "Authorization: Bearer $RBAC_TOKEN" | jq .
```

### T6 — Policy change not reflected after re-apply

**Symptom:** After updating a masking algorithm, the view still uses the old expression.

**Cause:** View checksum unchanged (policy update didn't change DDL if algorithm expression was unchanged), or Redis cache serving stale policy.

**Fix:**
```bash
# Force re-apply with cache bypass
curl -s -X POST http://192.168.1.50:30860/api/v1/masking/apply \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","force":true}' | jq .
```

---

## Service Endpoints Summary

| Service | URL | Notes |
|---|---|---|
| Star Knowledge Catalog API | `http://192.168.1.50:30860` | NodePort 30860 |
| Star Knowledge Catalog Docs | `http://192.168.1.50:30860/docs` | Swagger UI |
| RBAC Control Plane API | `http://192.168.1.50:30850` | NodePort 30850 |
| Apache Doris Web UI | `http://192.168.1.50:30030` | FE web console |
| Apache Doris MySQL | `192.168.1.50:30090` | MySQL protocol |
| PostgreSQL | `192.168.1.50:30532` | Metadata store |

## OpenBao Secret Paths

| Secret | Path |
|---|---|
| Star Catalog credentials | `secret/data/star-catalog/credentials` |
| Doris credentials | `secret/data/doris/credentials` |
| PostgreSQL credentials | `secret/data/postgresql/credentials` |

---

## Quick Reference: Masking Algorithms

| Algorithm | Type | Doris Expression | Example: `john.doe@example.com` |
|---|---|---|---|
| `FULL_REDACT` | REDACT | `'****'` | `****` |
| `SHA256_HASH` | HASH | `SHA2({col}, 256)` | `a94a8fe5ccb19ba6...` |
| `EMAIL_PARTIAL` | PARTIAL_MASK | `CONCAT(LEFT({col},2), REPEAT('*',...), SUBSTRING(...))` | `jo***@example.com` |
| `CREDIT_CARD_LAST4` | PARTIAL_MASK | `CONCAT(REPEAT('*',...), RIGHT({col},4))` | `************0366` |
| `DATE_YEAR_ONLY` | DATE_GENERALIZE | `DATE_FORMAT({col},'%Y-01-01')` | `1985-01-01` |
| `PHONE_LAST4` | PARTIAL_MASK | `CONCAT(REPEAT('*',...), RIGHT({col},4))` | `*******1234` |
| `NULL_OUT` | NULL_OUT | `NULL` | `NULL` |
| `IP_LAST_OCTET` | PARTIAL_MASK | `CONCAT(SUBSTRING_INDEX({col},'.',3),'.0')` | `192.168.1.0` |

---

*Last updated: Star Knowledge Catalog v1.0.0 · Doris 4.0.7 · RBAC Control Plane v1.0.0*
