# Star Knowledge Catalog — Column Classifier: Signal Reference

> **Component:** Auto Column Classifier (`catalog/engine/classifier.py`)  
> **Version:** v2 (Signal D added)  
> **Related:** [Runbook 17 — E2E Testing](../../docs/runbooks/runbook-17-star-catalog-e2e-testing.md)

---

## Overview

The classifier detects sensitive columns in Doris tables by comparing column names against glossary term patterns. It never reads table data — only `information_schema.COLUMNS` metadata is queried.

Every column passes through **two stages**:

```
Stage 1 — Base Score          (runs for every column)
       │
       ├─ score 1.0 / 0.9  →  HIGH confidence → tagged immediately*
       │
       └─ score 0.7        →  Stage 2 arbitration
                                    │
                           Signal C → REJECT?  (column name guard)
                           Signal D → REJECT?  (table name guard)
                           Signal D → +boost   (table name positive)
                           Signal A → +boost   (token position)
                           Signal B → +boost   (sibling context)
                                    │
                           arb_score = 0.7 + A + B + D_boost
                                    │
                           ≥ 0.95 → HIGH
                           ≥ 0.85 → MEDIUM
                           < 0.85 → LOW (not tagged)

* Signal D table-reject also runs on 1.0/0.9 hits
```

Only **HIGH** and **MEDIUM** produce column tags.  
**LOW** and **REJECT** are stored in the audit trail only (visible in scan response).

---

## Stage 1 — Base Score

Compares the column name (lowercased) against every pattern in a glossary term's `column_name_patterns` list.

| Score | Match type | Rule | Example |
|---|---|---|---|
| **1.0** | Exact | `column == pattern` exactly | `email` == `email` |
| **0.9** | Word-boundary | Pattern is a whole word/token inside the column name | `user_email` contains `email` as a boundary token |
| **0.7** | Substring | Pattern appears anywhere inside the column name | `emailaddress` contains `email` as a substring |
| **0.0** | No match | Pattern not found | `order_date` vs `email` |

**Word-boundary** means the pattern is surrounded by non-alphanumeric characters (`_`, start, or end of string). So `email` matches `user_email` and `email_addr` but not `emailaddress` (which scores 0.7 substring instead).

Score 1.0 / 0.9 → **HIGH confidence immediately**. Stage 2 is skipped except for Signal D table-reject check.  
Score 0.7 → **ambiguous** → proceed to Stage 2.

---

## Stage 2 — Arbitration Signals

Only reached when base score == 0.7. Signals run in this order:

### Signal C first (fast exit on column-name reject)
### Signal D second (fast exit on table-name reject, or boost)
### Signal A + B last (score boosters)

Final arbitration score:
```
arb_score = 0.7 + Signal_A + Signal_B + Signal_D_boost
```

---

## Signal A — Token Position Weight

**Question:** *Where does the matched pattern sit inside the column name?*

| Position | Boost | Example |
|---|---|---|
| Prefix | **+0.15** | `name_first` — `name` starts the column |
| Suffix | **+0.10** | `customer_name` — `name` ends the column |
| Middle | **+0.00** | `renamed_col` — `name` is buried |

**Why it matters:** Column naming conventions reveal intent. A column called `name_prefix` or `customer_name` is far more likely to be a person name than `renamed_flag` or `codename_internal`. Prefix position is the strongest signal — the primary concept comes first.

**Configured by:** Built into the classifier engine. No user configuration.

---

## Signal B — Sibling Table Context

**Question:** *Are there already confirmed sensitive columns in this same table scan?*

| Confirmed siblings with same classification | Boost |
|---|---|
| 2 or more | **+0.20** (strong context) |
| exactly 1 | **+0.12** (weak context) |
| 0 | **+0.00** |

**Why it matters:** A table that already contains confirmed `email` (HIGH) and `date_of_birth` (HIGH) PII columns is almost certainly a person-record table. An ambiguous `name` or `city` column in that table should lean toward being tagged. A table with no other sensitive columns should not.

**How siblings are counted:** Columns are scanned in **Doris ordinal position order** (the order they were defined in `CREATE TABLE`). Confirmed HIGH/MEDIUM matches build up the sibling accumulator as the scan progresses left-to-right. This means columns defined later in the table (e.g. `description`, `notes`) benefit from earlier high-confidence hits (e.g. `email`, `phone_number`).

**Configured by:** Built into the classifier engine. No user configuration.

---

## Signal C — Negative Column Token Guard

**Question:** *Does the column name itself contain a word that definitively means it is NOT sensitive?*

If any token from the glossary term's `negative_patterns` list appears as a word-boundary match in the column name → **immediate REJECT**.

**Examples for `full_name` term:**

| Column name | Matched negative token | Result |
|---|---|---|
| `company_name` | `company` | **REJECT** — company name is not a person name |
| `display_name` | `display` | **REJECT** — UI display label, not PII |
| `brand_name` | `brand` | **REJECT** — product brand |
| `product_name` | `product` | **REJECT** — product name |
| `customer_name` | *(none)* | **pass** → proceed to A/B/D |

**Configured by:** `negative_patterns` array on each glossary term. Update via:
```bash
curl -sf -X PATCH $CATALOG_URL/api/v1/glossary/<term_name> \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"negative_patterns": ["company","display","brand","product","vendor"]}' \
  | jq '{name, negative_patterns}'
```

---

## Signal D — Table Name Context

**Question:** *Does the TABLE the column lives in tell us whether the column is sensitive?*

Signal D checks the table name against two lists on each glossary term:

### Table Name Negative Patterns → REJECT

If the table name contains any token from `table_name_negative_patterns` → **immediate REJECT**, even for base score 1.0/0.9.

| Term | Example negative table tokens | What it prevents |
|---|---|---|
| `full_name` | `product`, `item`, `inventory`, `catalog` | `products.name` tagged as PII |
| `salary` | `payment`, `transaction`, `order`, `invoice` | `payments.pay_amount` tagged as CONFIDENTIAL |
| `ip_address` | `server`, `device`, `router`, `network` | `network_devices.ip_address` tagged as PII |
| `street_address` | `warehouse`, `depot`, `store`, `location` | `warehouses.address` tagged as PII |

### Table Name Positive Patterns → +0.20 boost

If the table name contains any token from `table_name_positive_patterns` → **+0.20 boost** added to the arbitration score.

| Term | Example positive table tokens | What it enables |
|---|---|---|
| `full_name` | `customer`, `employee`, `user`, `person` | `customers.name` boosted → HIGH |
| `salary` | `employee`, `payroll`, `hr`, `workforce` | `payroll_summary.wage` boosted → HIGH |
| `ip_address` | `session`, `login`, `audit`, `user` | `user_sessions.ip` boosted → HIGH |

### Unknown table → neutral

If the table name is not in either list → `+0.00` — the column proceeds with only Signal A and B deciding.

```
products table   + 'name' column  →  REJECT  ('product' in negatives)
customers table  + 'name' column  →  +0.20   ('customer' in positives)
orders table     + 'name' column  →  +0.00   (not in either list → A+B decide)
```

**Configured by:** `table_name_negative_patterns` and `table_name_positive_patterns` arrays on each glossary term. Update via:
```bash
curl -sf -X PATCH $CATALOG_URL/api/v1/glossary/<term_name> \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "table_name_negative_patterns": ["product","item","inventory","catalog"],
    "table_name_positive_patterns": ["customer","employee","user","person"]
  }' | jq '{name, table_name_negative_patterns, table_name_positive_patterns}'
```

---

## Worked Examples

### Example 1 — `products.name` (false positive prevented)
```
Column: name    Table: products
Base score: 1.0 (exact match on 'name' — if pattern were present)
Signal D:   REJECT — 'product' found in full_name.table_name_negative_patterns
Result:     REJECT ✅  products.name is a product name, not a person name
```

### Example 2 — `customers.full_name` (standard case)
```
Column: full_name    Table: customers
Base score: 1.0 (exact match)
Signal D:   neutral → no negatives match, 'customer' in positives but
            base 1.0 → already HIGH, D only checked for rejection
Result:     HIGH ✅
```

### Example 3 — `audit_log.name` (unknown table, ambiguous)
```
Column: name    Table: audit_log
Base score: 0.7 (substring match — if 'name' pattern present)
Signal C:   pass (no negative column tokens)
Signal D:   neutral — 'audit_log' not in any list → +0.00
Signal A:   +0.15 (prefix — 'name' starts the column)
Signal B:   +0.00 (no confirmed PII siblings in audit_log yet)
arb_score:  0.7 + 0.15 + 0.00 + 0.00 = 0.85 → MEDIUM ✅
Result:     MEDIUM — tagged with conservative class-level masking
```

### Example 4 — `network_devices.ip_address` (infra table)
```
Column: ip_address    Table: network_devices
Base score: 1.0 (exact match)
Signal D:   REJECT — 'network' found in ip_address.table_name_negative_patterns
Result:     REJECT ✅  infrastructure IP, not a person's IP
```

### Example 5 — `employee_records.wage` (payroll table)
```
Column: wage    Table: employee_records
Base score: 1.0 (exact match on salary.column_name_patterns)
Signal D:   'employee' in salary.table_name_positive_patterns
            → base 1.0 + table is positive context → HIGH confirmed
Result:     HIGH ✅
```

---

## Confidence → Masking Policy Mapping

| Confidence | Masking applied |
|---|---|
| **HIGH** | Full **term-level** policy — e.g. `email_address` → `EMAIL_PARTIAL` expression |
| **MEDIUM** | Conservative **classification-level** policy — e.g. any PII column → `SHA256_HASH` |
| **LOW** | Not tagged — stored in audit log, visible in scan response |
| **REJECT** | Not tagged — stored in audit log with rejection reason |

MEDIUM uses the classification default intentionally. If the classifier isn't certain it's an `email_address` specifically, it applies the general PII protection (SHA-256 hash) rather than the more revealing partial-email expression.

---

## Viewing Signal Details in Scan Output

The scan response includes the full signal breakdown per column. Use `arb_signals` to debug why a column was or wasn't tagged:

```bash
curl -sf -X POST $CATALOG_URL/api/v1/columns/scan \
  -H "Authorization: Bearer $CATALOG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","doris_table":"customers","dry_run":true}' | \
  jq '.tables_results[0].results[] | {col:.column_name, term:.matched_term,
      confidence, score:.score, arb_score:.arb_score, signals:.arb_signals}'
```

---

## Adding New Patterns

All four signal configurations are stored in PostgreSQL and can be updated via the Catalog API at runtime — no deployment required.

| What to change | API field | When to use |
|---|---|---|
| Add a new column name to detect | `column_name_patterns` | New naming convention found |
| Block a specific column name | `negative_patterns` | False positive on a specific column name token |
| Block an entire table type | `table_name_negative_patterns` | False positives across a table domain |
| Boost confidence for a table type | `table_name_positive_patterns` | Ambiguous columns in a known-sensitive table domain |

---

*Star Knowledge Catalog v1.0.0 · Classifier Signal Reference*
