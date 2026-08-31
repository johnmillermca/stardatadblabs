# CDC Sync-Point Design
## Snowflake Bulk Copy ↔ Oracle LogMiner — Zero-Gap Guarantee

> **Audience**: Platform engineers and data pipeline operators.  
> **Scope**: Generic — applies to ANY Snowflake database/schema pair, not just TPC-DS.

---

## 1. Problem Statement

When we bulk-copy a Snowflake table to Iceberg and then start streaming changes
from the mirrored Oracle table via Debezium CDC, there is a critical ordering
constraint:

```
Snowflake bulk copy (historical data up to time T)
      +
Oracle CDC stream (changes from time T onwards)
      ↓
Zero gap, zero duplication in Iceberg
```

If the Debezium connector starts from an Oracle SCN that is **earlier than T**,
rows already in Iceberg are replayed → **duplicates**.  
If it starts from an SCN **later than T**, rows inserted between T and the later
time are lost → **data gap**.

The solution: **capture T as a Snowflake server-side timestamp exactly before the
first batch SELECT**, convert T to an Oracle SCN, and configure Debezium to start
from that SCN.

---

## 2. Why Snowflake `CURRENT_TIMESTAMP()`, Not the Spark Driver Clock

| Property | Snowflake `CURRENT_TIMESTAMP()` | Spark driver `datetime.now()` |
|----------|--------------------------------|-------------------------------|
| Clock source | Snowflake transaction coordinator | Spark driver JVM |
| Timezone | Resolved in Snowflake session | OS tz + JVM tz |
| Reflects data visibility | ✅ Yes — Snowflake MVCC boundary | ❌ No — arbitrary wall clock |
| Accurate across restart/retry | ✅ Same semantics on re-run | ❌ Drifts between retries |
| Maps to Oracle SCN? | ✅ `TIMESTAMP_TO_SCN(T)` | ❌ Oracle SCN is independent |

`capture_sf_extraction_ts()` in [`starpump.py`](../snowflake-to-iceberg/starpump.py) runs:

```sql
SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::VARCHAR AS ts
```

inside the Snowflake Spark connector session, **immediately before** the first
`LIMIT … OFFSET 0` batch SELECT.  This ensures:

- The timestamp is within the same Snowflake transaction timeline as the data read.
- It is stored in UTC (no tz ambiguity).
- It is available as an Oracle `TIMESTAMP WITH TIME ZONE` input to `TIMESTAMP_TO_SCN`.

---

## 3. The `_pipeline_watermarks` Control Table (Postgres)

### 3.1 Why PostgreSQL (dedicated `pipeline` database)

The Debezium connector registration script is a plain shell script — it cannot
spin up a Spark session to query Iceberg.  A dedicated PostgreSQL database
(`pipeline`) provides a lightweight, always-available store that any process
can query with a single `psql` command.

The `pipeline` database is **separate from the `rbac` database** — no schema
coupling, independent backup cadence, and no risk of RBAC migrations affecting
pipeline state.

OpenBao path: `secret/data/platform/pipeline_db`

### 3.2 Schema

```sql
-- pipeline database (postgresql.prod.svc.cluster.local:5432)
pipeline_watermarks (
    source_db        TEXT    -- Snowflake database, e.g. SNOWFLAKE_SAMPLE_DATA
    source_schema    TEXT    -- Snowflake schema,   e.g. TPCDS_SF10TCL
    table_name       TEXT    -- lower-cased,        e.g. income_band
    sf_extraction_ts TEXT    -- ISO-8601 UTC: "2026-08-18T04:01:39.123456Z"
    oracle_start_scn BIGINT  -- NULL until Debezium bootstrap resolves it
    rows_copied      BIGINT  -- rows written by the Spark bulk-copy run
    pipeline_run_ts  TIMESTAMPTZ
    iceberg_namespace TEXT   -- == lower(source_schema)
    updated_at       TIMESTAMPTZ  -- auto-updated on every write
    UNIQUE (source_db, source_schema, table_name)
)
```

### 3.3 Dual-Write Strategy

Every successful Spark table copy writes the watermark to **two** places:

| Store | Writer | Reader | Purpose |
|-------|--------|--------|---------|
| `pipeline.pipeline_watermarks` (Postgres) | Spark (`psycopg2`) | Debezium bootstrap script (`psql`) | Shell-queryable, no Spark session needed |
| `polaris.tpcds_sf10tcl._pipeline_watermarks` (Iceberg) | Spark (SQL MERGE) | Any Spark job | Spark-native, S3-backed, version-tracked |
| Iceberg table property `pipeline.sf_extraction_ts` | Spark (`ALTER TABLE`) | `DESCRIBE EXTENDED` | Visible in catalog metadata |

---

## 4. Oracle SCN Resolution

### 4.1 Theory

Oracle's **System Change Number (SCN)** is a monotonically increasing counter
that advances with every committed transaction.  Oracle provides two functions:

```sql
-- SCN → timestamp (approximate, within ~3 seconds)
SELECT SCN_TO_TIMESTAMP(1234567) FROM DUAL;

-- timestamp → SCN (rounds up to next SCN after that timestamp)
SELECT TIMESTAMP_TO_SCN(
    TO_TIMESTAMP_TZ('2026-08-18T04:01:39.123456Z', 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM')
) FROM DUAL;
```

`TIMESTAMP_TO_SCN` returns the **lowest SCN whose commit timestamp ≥ the input**.
Starting Debezium at this SCN guarantees:

- All rows committed **before** `sf_extraction_ts` are already in Iceberg (from bulk copy).
- Debezium streams rows committed **at or after** `sf_extraction_ts` → no gap.
- A row committed at exactly `sf_extraction_ts` may appear in both (Iceberg from bulk
  copy AND CDC stream) — the Iceberg Sink connector is configured with
  `upsert-mode-enabled=true` so the duplicate is idempotently overwritten.

### 4.2 Implementation in Debezium Bootstrap Script

```bash
# Query pipeline_watermarks for a specific table
SF_TS=$(psql -h postgresql.prod.svc.cluster.local -U pipeline -d pipeline -At \
    -c "SELECT sf_extraction_ts FROM pipeline_watermarks
        WHERE source_db='SNOWFLAKE_SAMPLE_DATA'
          AND source_schema='TPCDS_SF10TCL'
          AND table_name='income_band'")

# Resolve to Oracle SCN
ORACLE_SCN=$(sqlplus -s tpcds/TpcdsPwd123!@oracle-xe.prod.svc.cluster.local/XEPDB1 <<EOF
SET HEADING OFF FEEDBACK OFF
SELECT TIMESTAMP_TO_SCN(
    TO_TIMESTAMP_TZ('${SF_TS}', 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"')
    AT TIME ZONE 'UTC'
) FROM DUAL;
EXIT;
EOF
)

# Store resolved SCN back in pipeline_watermarks
psql -h ... -c "UPDATE pipeline_watermarks SET oracle_start_scn=$ORACLE_SCN
                WHERE table_name='income_band' ..."

# Configure Debezium connector with snapshot.offset.scn
curl -X POST .../connectors -d '{ "snapshot.mode": "schema_only",
                                   "snapshot.offset.scn": "'$ORACLE_SCN'" }'
```

See [`02_register_debezium_oracle_connector.sh`](../oracle-cdc-iceberg/02_register_debezium_oracle_connector.sh)
for the full production implementation.

---

## 5. Debezium Connector Configuration

### 5.1 `snapshot.mode`

| Mode | Behaviour | When to use |
|------|-----------|-------------|
| `initial` | Full snapshot of all rows + stream going forward | First run, no bulk copy yet |
| `schema_only` | Schema only, no row snapshot; stream from given SCN | **After bulk copy** — use this |
| `never` | Skip snapshot; stream from current SCN | Existing Kafka offsets |

After the Snowflake bulk copy has completed, **always use `schema_only`** so
Debezium does not re-read rows already in Iceberg.

### 5.2 `snapshot.offset.scn`

When `snapshot.mode=schema_only`, set `snapshot.offset.scn` to the value from
`pipeline_watermarks.oracle_start_scn`.  This tells LogMiner the exact position
in the redo log from which to begin streaming.

```json
{
  "snapshot.mode":        "schema_only",
  "snapshot.offset.scn":  "12345678"
}
```

---

## 6. Generic Pattern (Any Source Database/Schema)

This design is **not** TPC-DS specific.  For any `DATABASE` / `SCHEMAS`:

1. Run `starpump` with the appropriate env vars:
   ```bash
   kubectl exec -n prod $MASTER -c spark-master -- \
     env USER=dave TOKEN="$TOKEN" \
         ADDR="http://openbao.prod.svc.cluster.local:8200" \
         DATABASE=MY_DB SCHEMAS=MY_SCHEMA \
     starpump snowflake
   ```
2. After completion, query watermarks:
   ```sql
   -- In pipeline DB:
   SELECT table_name, sf_extraction_ts, oracle_start_scn
   FROM pipeline_watermarks
   WHERE source_db='MY_DB' AND source_schema='MY_SCHEMA'
   ORDER BY table_name;
   ```
3. Run the Debezium bootstrap script with the appropriate table list and the
   resolved SCN values from `oracle_start_scn`.
4. The Iceberg Sink connector routes each topic to the right namespace
   automatically via `iceberg.tables.routeField`.

---

## 7. Failure Modes and Recovery

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Spark bulk copy fails mid-table | `pipeline_run_log.status = 'partial'` | Re-run with `INCLUDE_TABLES=<failed_table>` — watermark is only written on success, so a re-run re-captures the timestamp |
| Postgres write fails after Iceberg write | `oracle_start_scn IS NULL` in Postgres | Re-run the Postgres upsert from Iceberg watermark or re-run the table copy |
| `TIMESTAMP_TO_SCN` returns ORA-08181 (timestamp too old) | Script exit code ≠ 0 | Oracle only retains SCN→timestamp mapping for `undo_retention` seconds (default 900s). Run the Debezium bootstrap within 15 minutes of bulk copy. For large schemas, extend `undo_retention` before running |
| Debezium connector registered before bulk copy finishes | `oracle_start_scn IS NULL` | The script checks for NULL and aborts with a clear error |
| Duplicate rows at sync boundary | `upsert-mode-enabled=true` on Iceberg Sink | Idempotent overwrite — no action needed |

---

## 8. Observability

```sql
-- Current watermark state for all tables (pipeline DB):
SELECT source_db, source_schema, table_name,
       sf_extraction_ts,
       oracle_start_scn,
       rows_copied,
       updated_at
FROM pipeline_watermarks
ORDER BY source_db, source_schema, table_name;

-- Run history (pipeline DB):
SELECT run_id, source_db, source_schema,
       started_at, finished_at,
       tables_ok, tables_failed, total_rows, status
FROM pipeline_run_log
ORDER BY started_at DESC
LIMIT 20;

-- Iceberg watermarks (Spark SQL):
SELECT * FROM polaris.tpcds_sf10tcl._pipeline_watermarks
ORDER BY source_db, table_name;
```

---

## 9. Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│  Spark bulk-copy job  (starpump.py)                     │
│                                                                     │
│  for each table:                                                    │
│    1. ts = Snowflake CURRENT_TIMESTAMP()  ← CDC sync point         │
│    2. bulk copy rows → Iceberg                                      │
│    3. ALTER TABLE SET TBLPROPERTIES pipeline.sf_extraction_ts=ts    │
│    4a. MERGE INTO polaris.ns._pipeline_watermarks  (Iceberg)        │
│    4b. UPSERT pipeline.pipeline_watermarks  (PostgreSQL)            │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │  ts stored in Postgres
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Debezium bootstrap  (02_register_debezium_oracle_connector.sh)     │
│                                                                     │
│  1. psql → SELECT sf_extraction_ts FROM pipeline_watermarks        │
│  2. sqlplus → TIMESTAMP_TO_SCN(ts) → oracle_start_scn              │
│  3. psql → UPDATE pipeline_watermarks SET oracle_start_scn=…       │
│  4. curl → POST /connectors  snapshot.mode=schema_only             │
│                               snapshot.offset.scn=oracle_start_scn │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │  Debezium streams from SCN
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Oracle XE LogMiner → Kafka → Iceberg Sink Connector               │
│  (changes from oracle_start_scn onwards → same Iceberg tables)     │
└─────────────────────────────────────────────────────────────────────┘
```
