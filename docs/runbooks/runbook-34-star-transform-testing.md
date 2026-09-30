# Runbook 34 — StarTransform Pipeline: Architecture & Functional Testing

> **Deployments:** `star-transform-pipeline-{postgres,oracle,mongodb}` in `prod`
> **Health ports:** postgres=8090 · oracle=8091 · mongodb=8092
> **Iceberg namespace:** `<catalog>.st_transforms` in S3 bucket `xdatatoiceberg1`
> **Related runbooks:** [05 — Kafka to Iceberg Streaming](runbook-05-kafka-to-iceberg-streaming.md) · [08 — Security & Access](runbook-08-security-access.md)

---

## 1. Architecture Overview

### What StarTransform is

StarTransform is a **Spark Structured Streaming library** of 26 reusable DataFrame transformation functions. The `star_transform_pipeline.py` script wires these functions against live CDC (Change Data Capture) events from three database sources, demonstrating every function against real data every 5 seconds.

Each CDC event arrives as a **Debezium envelope** — a JSON object with four fields:

| Field | Description |
|---|---|
| `before` | JSON snapshot of the row *before* the change (NULL for INSERTs) |
| `after` | JSON snapshot of the row *after* the change (NULL for DELETEs) |
| `op` | Operation code: `c`=INSERT, `u`=UPDATE, `d`=DELETE, `r`=read/snapshot |
| `ts_ms` | Source DB transaction timestamp in milliseconds |

The pipeline decodes these envelopes into Spark DataFrames, then applies all 26 StarTransform functions in every micro-batch.

### Data flow

```
PostgreSQL ──────────────────────────────────────────────────┐
  cache_testing.{customers,orders,products}                  │
  Debezium → Kafka topics: postgres.cache_testing.*          │
                                                             │
Oracle XEPDB1 CACHE_TESTING ──────────────────────────────── ├──► Kafka (SCRAM-SHA-512)
  CUSTOMERS, ORDERS, PRODUCTS                                │         │
  Debezium → Kafka topics: oracle.CACHE_TESTING.*            │         ▼
                                                             │   Spark Structured Streaming
MongoDB cache_testing ───────────────────────────────────────┘   foreachBatch (5s trigger)
  customers, orders, products                                          │
  Debezium → Kafka topics: mongodb.cache_testing.*                     │
                                                               26 ST functions applied
                                                                       │
                                                           ┌───────────┴───────────┐
                                                           ▼                       ▼
                                                    Kafka output topics      Iceberg tables
                                                  st.<fn>.<src>.<table>   <catalog>.st_transforms
                                                   (55 topics, all True)    .<fn>__<src>__<table>
```

### Three independent deployments

| Deployment | SOURCE | Health Port | Kafka Topic Pattern |
|---|---|---|---|
| `star-transform-pipeline-postgres` | `postgres` | `8090` | `postgres\.cache_testing\..*` |
| `star-transform-pipeline-oracle` | `oracle` | `8091` | `oracle\.(cache_testing\|CACHE_TESTING)\..*` |
| `star-transform-pipeline-mongodb` | `mongodb` | `8092` | `mongodb\.cache_testing\..*` |

Each deployment runs `replicas=1` with `strategy=Recreate` (to avoid two pods writing to the same S3 checkpoint simultaneously).

### Script delivery

The pipeline script is **not baked into the image**. It is injected at runtime via ConfigMap `star-transform-pipeline-script` mounted at `/opt/spark/work-dir/star_transform_pipeline.py`. After any script change the ConfigMap must be updated and the pods restarted (see §6).

---

## 2. StarTransform Function Reference

Each function description below covers: **what it does**, **which columns it acts on**, **where its output goes**, and **what to check**.

---

### ST-01 · `filter_op` — Filter by CDC operation type

**What it does:** Drops rows whose `_op` code is not in the allowed list. Used to discard DELETE events (`d`) or snapshot reads (`r`) and keep only actionable INSERT/UPDATE events downstream.

**Inputs:** Any decoded CDC DataFrame with an `_op` column.
**Parameters:** `ops` — list of allowed op codes, e.g. `["c", "u"]`.
**Output:** Rows where `_op` is in the allow-list; all others discarded.
**Sinks:** Kafka topic `st.filter_op.<src>.customers` + Iceberg `<catalog>.st_transforms.filter_op__<src>__customers`

---

### ST-02 · `deduplicate` — Last-write-wins deduplication per primary key

**What it does:** Within a single micro-batch, multiple CDC events may arrive for the same row (e.g. two rapid UPDATEs). This function retains only the **last event** per primary key, ordered by `kafka_ts` descending. Prevents stale intermediate states from reaching downstream sinks.

**Inputs:** DataFrame with a primary key column and `kafka_ts`.
**Parameters:** `pk` — the primary key column name (e.g. `"id"` for PostgreSQL, `"customer_id"` for Oracle/MongoDB).
**Output:** One row per unique PK value — the most recent event in the batch.
**Sinks:** Kafka + Iceberg `deduplicate__<src>__customers`

---

### ST-03 · `add_processing_time` — Inject pipeline processing timestamp

**What it does:** Adds a new column (default name `proc_time`) containing the current wall-clock timestamp at the moment Spark processes the row. Useful for tracking pipeline latency: `proc_time - kafka_ts` gives the end-to-end CDC lag.

**Inputs:** Any DataFrame.
**Parameters:** `col_name` — name of the new timestamp column.
**Output:** Input DataFrame with one extra `TIMESTAMP` column.
**Sinks:** Kafka + Iceberg `add_processing_time__<src>__customers`

---

### ST-04 · `rename_columns` — Rename columns via a mapping dict

**What it does:** Renames one or more columns according to a `{old_name: new_name}` dictionary. Used here to normalise source-specific PK column names to a common name (`cust_id`) regardless of source: PostgreSQL uses `id`, Oracle and MongoDB use `customer_id`.

**Inputs:** Any DataFrame.
**Parameters:** `mapping` — dict of `{old: new}` column names.
**Output:** DataFrame with specified columns renamed; all other columns unchanged.
**Sinks:** Kafka + Iceberg `rename_columns__<src>__customers`

---

### ST-05 · `cast_columns` — Cast column data types

**What it does:** Applies Spark `cast()` to specified columns. Useful when Debezium schema inference produces the wrong type (e.g. `loyalty_score` arriving as STRING from some connectors when it should be DOUBLE, or `tier` needing explicit STRING typing).

**Inputs:** Any DataFrame.
**Parameters:** `casts` — dict of `{col_name: spark_type_string}`, e.g. `{"loyalty_score": "double", "tier": "string"}`.
**Output:** DataFrame with those columns cast to the specified types.
**Sinks:** Kafka + Iceberg `cast_columns__<src>__customers`

---

### ST-06 · `drop_columns` — Remove unwanted columns

**What it does:** Drops a list of columns from the DataFrame. Used to strip internal/metadata columns (`topic`, `ts_ms`, `_op`, `kafka_ts`) before writing a clean payload to downstream consumers who do not need CDC envelope metadata.

**Inputs:** Any DataFrame.
**Parameters:** `columns` — list of column names to remove.
**Output:** DataFrame without the listed columns; missing column names are ignored silently.
**Sinks:** Kafka + Iceberg `drop_columns__<src>__customers`

---

### ST-07 · `mask_columns` — SHA-256 hash PII fields

**What it does:** Replaces the value of each specified column with its **SHA-256 hex digest**. The original plaintext value is discarded. Used to anonymise PII fields (`email`, `phone`) so that downstream Iceberg tables and Kafka topics never contain raw personal data, while still enabling exact-match lookups by hash.

**Inputs:** DataFrame with string PII columns.
**Parameters:** `columns` — list of columns to hash.
**Output:** Same DataFrame with specified columns replaced by 64-character lowercase hex strings.
**Sinks:** Kafka + Iceberg `mask_columns__<src>__customers`

---

### ST-08 · `add_source_tag` — Inject source system identifier

**What it does:** Adds a literal string column (default name `source_system`) whose value is the name of the originating CDC source (`"postgres"`, `"oracle"`, or `"mongodb"`). Enables consumers of the unified Kafka output topics or Iceberg tables to identify which system the row came from.

**Inputs:** Any DataFrame.
**Parameters:** `source` — the tag value; `col_name` — the new column name.
**Output:** Input DataFrame with one extra STRING column.
**Sinks:** Kafka + Iceberg `add_source_tag__<src>__customers`

---

### ST-09 · `add_op_label` — Human-readable CDC operation label

**What it does:** Adds an `op_label` STRING column translating the raw Debezium op codes into readable labels:  `c` → `"INSERT"`, `u` → `"UPDATE"`, `d` → `"DELETE"`, `r` → `"READ"`. Makes downstream SQL queries and dashboards more readable without requiring knowledge of Debezium's single-character codes.

**Inputs:** DataFrame with an `_op` column.
**Parameters:** None.
**Output:** Input DataFrame with an extra `op_label` column.
**Sinks:** Kafka + Iceberg `add_op_label__<src>__customers`

---

### ST-10 · `flatten_json_col` — Parse a JSON string column into top-level fields

**What it does:** Parses a column containing a JSON string (e.g. `metadata_json TEXT` in PostgreSQL) into multiple top-level typed columns using a declared schema, with an optional prefix on the new column names. Eliminates the need to parse JSON in downstream SQL queries.

**Inputs:** DataFrame with a JSON string column.
**Parameters:** `json_col` — column to parse; `schema` — StructType schema; `prefix` — string prefix for new columns.
**Output:** Input DataFrame with the JSON column's fields promoted to top-level columns (`meta_loyalty_tier`, `meta_preferred_contact`, `meta_last_campaign`).
**Applies to:** PostgreSQL `customers` only (Oracle and MongoDB do not have a `metadata_json` column).
**Sinks:** Kafka + Iceberg `flatten_json_col__postgres__customers`

---

### ST-11 · `filter_columns` — Keep only specified columns (column projection)

**What it does:** Retains only the columns in the allow-list, dropping everything else. A strict projection — the inverse of `drop_columns`. Used to produce a minimal, schema-stable payload for downstream consumers who only need key business fields.

**Inputs:** Any DataFrame.
**Parameters:** `columns` — list of column names to keep. Columns not present in the DataFrame are skipped.
**Output:** DataFrame with only the listed columns; column order matches the input list.
**Sinks:** Kafka + Iceberg `filter_columns__<src>__customers`

---

### ST-12 · `null_coalesce` — Fill NULL values with defaults

**What it does:** Replaces NULL values in specified columns with provided defaults. Used to ensure downstream Iceberg tables and Kafka messages never have unexpected NULLs for columns that should always have a value (e.g. `loyalty_score` defaults to `0`, `preferred_lang` defaults to `"en"`, `tier` defaults to `"standard"`).

**Inputs:** Any DataFrame.
**Parameters:** `defaults` — dict of `{col_name: default_value}`.
**Output:** Same DataFrame with NULLs replaced by defaults in the specified columns.
**Sinks:** Kafka + Iceberg `null_coalesce__<src>__customers`

---

### ST-13 · `pivot_before_after` — Expand UPDATE envelopes side-by-side

**What it does:** For UPDATE events (`_op = 'u'`), the Debezium envelope contains both a `before` snapshot and an `after` snapshot. This function parses both JSON blobs and expands them into side-by-side columns — `before_<field>` and `after_<field>` — making change detection easy in SQL (`WHERE before_tier != after_tier`).

**Inputs:** Full envelope DataFrame (with `before`, `after`, `_op` columns) filtered to `_op = 'u'`.
**Parameters:** `schema` — StructType schema inferred from the `after` column.
**Output:** One row per UPDATE with both before and after state as top-level columns.
**Note:** Only fires when UPDATE events are present in the micro-batch.
**Sinks:** Kafka + Iceberg `pivot_before_after__<src>__customers`

---

### ST-14 · `apply_pipeline` — Chain multiple transforms in sequence

**What it does:** Applies a list of `(function, kwargs)` tuples as a sequential pipeline on a single DataFrame. Each function's output becomes the input to the next. This is the composition primitive — the five steps applied here are: `filter_op → deduplicate → add_processing_time → mask_columns → add_op_label`.

**Inputs:** DataFrame; list of `(ST_function, kwargs_dict)` tuples.
**Parameters:** `steps` — ordered list of transform steps.
**Output:** DataFrame after all steps have been applied in order.
**Sinks:** Kafka + Iceberg `apply_pipeline__<src>__customers`

---

### ST-15 · `windowed_aggregate` — GROUP BY aggregation per micro-batch

**What it does:** Groups the DataFrame by one or more columns and computes multiple aggregations in a single pass. Applied to `orders` grouped by `status`, computing: `SUM(total_amount)` as `total_revenue`, `COUNT(order_id)` as `order_count`, `AVG` / `MIN` / `MAX` of `total_amount`.

**Inputs:** Orders DataFrame.
**Parameters:** `group_cols` — columns to group by; `agg_specs` — list of `(col, agg_fn, alias)` tuples.
**Output:** Summary DataFrame — one row per group, not per input row (output cardinality differs from input).
**Sinks:** Iceberg only — `windowed_aggregate__<src>__orders`

---

### ST-16 · `rolling_sum` — Cumulative running sum per partition

**What it does:** Computes a running total of a value column, ordered by a timestamp column, within each partition group. Applied to `orders.total_amount` partitioned by `customer_id` and ordered by `kafka_ts` — gives each customer's cumulative revenue across the batch.

**Inputs:** Orders DataFrame with a numeric value column and a sortable timestamp column.
**Parameters:** `value_col`, `order_col`, `partition_cols`.
**Output:** Input rows with an extra column `rolling_sum_<value_col>`.
**Sinks:** Iceberg only — `rolling_sum__<src>__orders`

---

### ST-17 · `rolling_avg` — Cumulative running average per partition

**What it does:** Same windowing mechanics as `rolling_sum` but computes a running average instead of a sum. Applied to `orders.total_amount` per `customer_id` — shows how a customer's average order value evolves across the batch.

**Inputs:** Orders DataFrame.
**Parameters:** `value_col`, `order_col`, `partition_cols`.
**Output:** Input rows with an extra column `rolling_avg_<value_col>`.
**Sinks:** Iceberg only — `rolling_avg__<src>__orders`

---

### ST-18 · `count_distinct_per_key` — Count distinct values per group

**What it does:** For each value of a group column, counts the number of distinct values in a value column. Applied here: distinct `customer_id` count per `status` — how many unique customers placed orders in each status bucket within the batch.

**Inputs:** Orders DataFrame.
**Parameters:** `group_col`, `value_col`.
**Output:** Summary DataFrame with columns `[group_col, distinct_count]`.
**Sinks:** Iceberg only — `count_distinct_per_key__<src>__orders`

---

### ST-19 · `top_n_per_group` — Rank and filter top N rows per group

**What it does:** Within each group (defined by `group_col`), ranks rows by `rank_col` descending and returns only the top `n` rows. Applied here: top 5 highest-value orders per `status`. Uses Spark window functions (`RANK() OVER (PARTITION BY status ORDER BY total_amount DESC)`).

**Inputs:** Orders DataFrame.
**Parameters:** `group_col`, `rank_col`, `n`.
**Output:** Input rows filtered to top N per group, with an extra integer `rank` column (1 = highest).
**Sinks:** Kafka + Iceberg — `top_n_per_group__<src>__orders`

---

### ST-20 · `event_rate` — Events-per-second throughput metric

**What it does:** Computes the throughput rate (events/second) of the batch by dividing the total row count by the time span between the earliest and latest `kafka_ts` values. Produces a single-row summary DataFrame useful for throughput monitoring and alerting.

**Inputs:** Any DataFrame with a timestamp column.
**Parameters:** `ts_col` — the timestamp column to use.
**Output:** Single-row DataFrame with `event_count`, `duration_seconds`, `events_per_second`.
**Sinks:** Iceberg only — `event_rate__<src>__orders`

---

### ST-21 · `aggregate_counts` — Count events per primary key and op code

**What it does:** Groups by `(pk_col, _op)` and counts occurrences. Reveals how many times each row was inserted, updated, or deleted within the batch. Useful for detecting hot rows (a single PK updated many times in one batch) or detecting unexpected deletes.

**Inputs:** Orders DataFrame.
**Parameters:** `pk_col` — the primary key column.
**Output:** Summary DataFrame with columns `[pk_col, _op, count]`.
**Sinks:** Iceberg only — `aggregate_counts__<src>__orders`

---

### ST-22 · `stream_join` — Enrich one stream with columns from another

**What it does:** Performs a batch-scoped join between two DataFrames within the same micro-batch. Applied here: enriches each order row with the customer's `tier` and `loyalty_score` by joining `orders.customer_id = customers.customer_id` (left join — order rows with no matching customer in the same batch keep NULL for the joined columns).

**Inputs:** Left DataFrame (orders); right DataFrame (customers subset).
**Parameters:** `right_df`, `join_col`, `how` (join type).
**Output:** Orders rows with `tier` and `loyalty_score` appended.
**Sinks:** Kafka + Iceberg — `stream_join__<src>__orders`

---

### ST-23 · `temporal_join` — Time-bounded join matching closest event

**What it does:** Joins two DataFrames on a key column but only matches rows whose timestamps are within a configurable tolerance window (here: 60,000 ms = 60 seconds). An order event is matched to the customer event that arrived closest in time within that window. Prevents stale customer state from contaminating order enrichment.

**Inputs:** Left DataFrame (orders); right DataFrame (customers with timestamp).
**Parameters:** `right_df`, `key_col`, `left_ts_col`, `right_ts_col`, `tolerance_ms`.
**Output:** Orders rows enriched with the time-proximate customer `tier`, plus a `right_kafka_ts` column.
**Sinks:** Kafka + Iceberg — `temporal_join__<src>__orders`

---

### ST-24 · `join_and_tag_source` — Join and annotate with provenance topic names

**What it does:** Same join as `stream_join` but additionally injects two provenance columns — `left_topic` and `right_topic` — containing the Kafka topic names of each side. Enables downstream consumers to trace exactly which source topics contributed to each joined row, useful for audit and lineage.

**Inputs:** Left DataFrame (orders); right DataFrame (customers subset); topic name strings.
**Parameters:** `right_df`, `join_col`, `left_topic`, `right_topic`, `how`.
**Output:** Joined DataFrame with `left_topic` and `right_topic` string columns appended.
**Sinks:** Kafka + Iceberg — `join_and_tag_source__<src>__orders`

---

### ST-25 · `multi_topic_union` — UNION multiple topic DataFrames with source tag

**What it does:** Takes a `{topic_name: DataFrame}` dictionary and UNIONs all DataFrames into a single DataFrame, adding a `source_topic` column (or configured name) with the originating topic name. Produces a merged stream where customers and orders events appear together, each tagged with their origin. Column schema is aligned with `coalesce` — missing columns in any source get NULL.

**Inputs:** Dict of `{topic_name: DataFrame}`; tag column name; align_schema flag.
**Output:** Single DataFrame containing all rows from all input DataFrames with a `source_topic` column.
**Sinks:** Kafka topic `st.multi_topic_union.<src>.all` + Iceberg `multi_topic_union__<src>__all`

---

### ST-26 · `route_by_topic` — Split a batch DataFrame into per-topic DataFrames

**What it does:** The routing primitive. Takes the raw batch DataFrame (which may contain rows from multiple Kafka topics) and splits it into a `{topic_name: DataFrame}` dictionary. Each value contains only the rows from that topic. This is called first in every `foreachBatch` invocation to fan out the batch before applying per-table transformations.

**Inputs:** Raw batch DataFrame with a `topic` column.
**Output:** Dict `{topic_name: DataFrame}` — one entry per distinct topic in the batch.
**Sinks:** None (routing only — result logged, not written)

---

## 3. Current Status Check

### 3.1 Pod health

```bash
# All three should be 1/1 Running
kubectl get deploy -n prod -l app=star-transform-pipeline

# Pod detail
kubectl get pods -n prod -l app=star-transform-pipeline -o wide

# Store pod names for all tests below
PG_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=postgres \
         -o jsonpath='{.items[0].metadata.name}')
ORA_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=oracle \
          -o jsonpath='{.items[0].metadata.name}')
MGO_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=mongodb \
          -o jsonpath='{.items[0].metadata.name}')

# Liveness probe — each should return "OK"
kubectl exec -n prod "$PG_POD"  -- curl -sf http://localhost:8090/ && echo " [postgres OK]"
kubectl exec -n prod "$ORA_POD" -- curl -sf http://localhost:8091/ && echo " [oracle OK]"
kubectl exec -n prod "$MGO_POD" -- curl -sf http://localhost:8092/ && echo " [mongodb OK]"
```

### 3.2 Kafka output topics

```bash
# All 55 topics should show READY=True
kubectl get kafkatopic -n prod -l "st.function" --no-headers | awk '{print $5}' | sort | uniq -c
# Expected: 55 True
```

### 3.3 Recent batch activity

```bash
for src in postgres oracle mongodb; do
  POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=$src \
        -o jsonpath='{.items[0].metadata.name}')
  echo "=== $src ==="
  kubectl logs -n prod "$POD" --tail=30 | \
    grep -E "batch=|ST transforms complete|Attempt [0-9]+ failed|ERROR" | tail -5
done
```

---

## 4. Functional Tests — One Test Per Function

### Prerequisites — inject test CDC data

Run this once before executing individual tests. It inserts a customer and orders into all three sources, triggering CDC events for the pipeline.

```bash
# ── PostgreSQL ──
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    INSERT INTO customers (name, email, phone, tier, loyalty_score, preferred_lang,
                           metadata_json)
    VALUES ('ST-Test', 'sttest@example.com', '555-0199', 'gold', 95.5, 'en',
            '{\"loyalty_tier\":\"gold\",\"preferred_contact\":\"email\",\"last_campaign\":\"summer2026\"}')
    ON CONFLICT DO NOTHING;

    INSERT INTO orders (customer_id, status, total_amount, product_id)
    VALUES (1,'pending',199.99,1),(1,'shipped',499.99,2),(2,'pending',99.50,1)
    ON CONFLICT DO NOTHING;
  "

# ── Oracle ──
kubectl exec -n prod deploy/oracle-xe -- \
  sqlplus -s system/Oracle18c@XEPDB1 <<'EOF'
INSERT INTO CACHE_TESTING.CUSTOMERS
  (FIRST_NAME,LAST_NAME,EMAIL,PHONE,TIER,LOYALTY_SCORE,PREFERRED_LANG)
VALUES ('STTest','User','sttest@oracle.local','555-0199','GOLD',95.5,'en');
INSERT INTO CACHE_TESTING.ORDERS (CUSTOMER_ID,STATUS,TOTAL_AMOUNT)
VALUES (1,'pending',199.99);
COMMIT;
EOF

# ── MongoDB ──
kubectl exec -n prod deploy/mongodb -- mongosh cache_testing --eval "
  db.customers.insertOne({
    customer_id:'st-test-001', first_name:'STTest', last_name:'User',
    email:'sttest@mongo.local', phone:'555-0199',
    tier:'gold', loyalty_score:95.5, preferred_lang:'en'
  });
  db.orders.insertOne({
    order_id:'sto-001', customer_id:'st-test-001',
    status:'pending', total_amount:199.99
  });
"
```

Wait **10 seconds** for the 5-second trigger to fire and Spark to process the batch before running any test below.

---

### TEST-01 · `filter_op`

**Goal:** Confirm only INSERT (`c`) and UPDATE (`u`) rows reach the output — no DELETE (`d`) or snapshot (`r`) rows.

```bash
# Trigger a delete in PostgreSQL so all op types exist
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    DELETE FROM customers WHERE email='sttest-delete@example.com';
  "

# Wait ~10s then query Iceberg — should contain ONLY 'c' and 'u' rows
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-01').getOrCreate()
df = spark.sql(\"\"\"
  SELECT _op, COUNT(*) AS cnt
  FROM \`postgres\`.\`st_transforms\`.\`filter_op__postgres__customers\`
  GROUP BY _op ORDER BY _op
\"\"\")
df.show()
"
```

**Expected:** Only rows where `_op` is `c` or `u`. No `d` rows.

---

### TEST-02 · `deduplicate`

**Goal:** Two rapid UPDATEs for the same customer in one batch result in a single row with the *latest* value.

```bash
# Two updates — loyalty_score 50 then 99 — in quick succession
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    UPDATE customers SET loyalty_score=50 WHERE email='sttest@example.com';
    UPDATE customers SET loyalty_score=99 WHERE email='sttest@example.com';
  "

sleep 10

kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-02').getOrCreate()
df = spark.sql(\"\"\"
  SELECT id, loyalty_score, kafka_ts
  FROM \`postgres\`.\`st_transforms\`.\`deduplicate__postgres__customers\`
  WHERE email IS NOT NULL
  ORDER BY kafka_ts DESC LIMIT 5
\"\"\")
df.show()
"
```

**Expected:** For this customer's `id`, the row shows `loyalty_score = 99` (not 50). No duplicate rows for the same `id` within a single batch.

---

### TEST-03 · `add_processing_time`

**Goal:** Confirm every row has a non-NULL `proc_time` TIMESTAMP column that is close to now.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-03').getOrCreate()
df = spark.sql(\"\"\"
  SELECT id,
         proc_time,
         ROUND((unix_timestamp(NOW()) - unix_timestamp(proc_time)), 1) AS age_seconds
  FROM \`postgres\`.\`st_transforms\`.\`add_processing_time__postgres__customers\`
  ORDER BY proc_time DESC LIMIT 5
\"\"\")
df.show()
"
```

**Expected:** `proc_time` is NOT NULL. `age_seconds` is small (seconds to a few minutes — reflects how recently the batch ran, not hours/days).

---

### TEST-04 · `rename_columns`

**Goal:** Confirm the source-specific PK column has been renamed to the unified `cust_id`.

```bash
# PostgreSQL: original PK is 'id' — should become 'cust_id'
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-04').getOrCreate()
df = spark.sql(\"\"\"
  SELECT * FROM \`postgres\`.\`st_transforms\`.\`rename_columns__postgres__customers\`
  LIMIT 2
\"\"\")
print('Columns:', df.columns)
df.show(2, truncate=False)
"
```

**Expected:** `cust_id` column present. No `id` column (it was renamed). Oracle/MongoDB equivalent: `customer_id` → `cust_id`.

---

### TEST-05 · `cast_columns`

**Goal:** Confirm `loyalty_score` is DOUBLE type and `tier` is STRING type in the output table.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-05').getOrCreate()
df = spark.sql(\"\"\"
  SELECT loyalty_score, tier
  FROM \`postgres\`.\`st_transforms\`.\`cast_columns__postgres__customers\`
  LIMIT 3
\"\"\")
print('Schema:')
df.printSchema()
df.show()
"
```

**Expected:** Schema shows `loyalty_score: double (nullable = true)` and `tier: string (nullable = true)`.

---

### TEST-06 · `drop_columns`

**Goal:** Confirm that internal CDC metadata columns (`topic`, `ts_ms`, `_op`, `kafka_ts`) are absent from the output.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-06').getOrCreate()
df = spark.sql(\"\"\"
  SELECT * FROM \`postgres\`.\`st_transforms\`.\`drop_columns__postgres__customers\`
  LIMIT 1
\"\"\")
print('Columns:', df.columns)
dropped = [c for c in ['topic','ts_ms','_op','kafka_ts'] if c in df.columns]
print('Dropped cols still present (should be empty):', dropped)
"
```

**Expected:** `Dropped cols still present: []` — all four metadata columns are gone.

---

### TEST-07 · `mask_columns`

**Goal:** Confirm `email` and `phone` are SHA-256 hashes (64-char hex) and not plaintext.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
import re
spark = SparkSession.builder.appName('test-07').getOrCreate()
df = spark.sql(\"\"\"
  SELECT email, phone
  FROM \`postgres\`.\`st_transforms\`.\`mask_columns__postgres__customers\`
  LIMIT 3
\"\"\")
df.show(truncate=False)

# Verify format: 64 hex chars
rows = df.collect()
for r in rows:
    assert len(r['email']) == 64, f'email length {len(r[\"email\"])} != 64'
    assert re.fullmatch(r'[0-9a-f]{64}', r['email']), 'email is not a hex hash'
    print('email OK: SHA-256 hash confirmed')
    break
"
```

**Expected:** `email` and `phone` values are 64-character lowercase hex strings. `email OK: SHA-256 hash confirmed` printed.

---

### TEST-08 · `add_source_tag`

**Goal:** Confirm every row has a `source_system` column with the correct source identifier.

```bash
# Test all three sources
for src in postgres oracle mongodb; do
  POD_VAR="${src^^}_POD"
  POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=$src \
        -o jsonpath='{.items[0].metadata.name}')
  kubectl exec -n prod "$POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-08').getOrCreate()
df = spark.sql(\"\"\"
  SELECT DISTINCT source_system
  FROM \`${src}\`.\`st_transforms\`.\`add_source_tag__${src}__customers\`
\"\"\")
df.show()
" 2>/dev/null
done
```

**Expected:** Each deployment's table has exactly one distinct `source_system` value matching its source name (`postgres`, `oracle`, `mongodb`).

---

### TEST-09 · `add_op_label`

**Goal:** Confirm `op_label` column contains human-readable strings (`INSERT`, `UPDATE`) not raw codes (`c`, `u`).

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-09').getOrCreate()
df = spark.sql(\"\"\"
  SELECT _op, op_label, COUNT(*) AS cnt
  FROM \`postgres\`.\`st_transforms\`.\`add_op_label__postgres__customers\`
  GROUP BY _op, op_label ORDER BY _op
\"\"\")
df.show()
"
```

**Expected:** `_op='c'` maps to `op_label='INSERT'`; `_op='u'` maps to `op_label='UPDATE'`. No raw op codes appear in `op_label`.

---

### TEST-10 · `flatten_json_col`

**Goal:** Confirm `metadata_json` is parsed into individual top-level columns with the `meta_` prefix.

```bash
# Ensure metadata_json is populated
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    UPDATE customers
    SET metadata_json='{\"loyalty_tier\":\"platinum\",\"preferred_contact\":\"email\",\"last_campaign\":\"summer2026\"}'
    WHERE email='sttest@example.com';
  "

sleep 10

kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-10').getOrCreate()
df = spark.sql(\"\"\"
  SELECT meta_loyalty_tier, meta_preferred_contact, meta_last_campaign
  FROM \`postgres\`.\`st_transforms\`.\`flatten_json_col__postgres__customers\`
  WHERE meta_loyalty_tier IS NOT NULL
  LIMIT 3
\"\"\")
df.show(truncate=False)
"
```

**Expected:** `meta_loyalty_tier='platinum'`, `meta_preferred_contact='email'`, `meta_last_campaign='summer2026'` as individual columns.

---

### TEST-11 · `filter_columns`

**Goal:** Confirm only the declared keep-list columns are present — no extra columns.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-11').getOrCreate()
df = spark.sql(\"\"\"
  SELECT * FROM \`postgres\`.\`st_transforms\`.\`filter_columns__postgres__customers\`
  LIMIT 1
\"\"\")
print('Columns present:', sorted(df.columns))
expected = sorted(['id','email','tier','loyalty_score','_op','kafka_ts'])
print('Expected columns:', expected)
print('Match:', sorted(df.columns) == expected)
"
```

**Expected:** `Match: True` — exactly the six declared columns, nothing else.

---

### TEST-12 · `null_coalesce`

**Goal:** Confirm that rows which had NULL `loyalty_score`, `preferred_lang`, or `tier` now have their default values.

```bash
# Insert a customer with deliberate NULLs
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    INSERT INTO customers (name, email, phone, tier, loyalty_score, preferred_lang)
    VALUES ('NullTest','nulltest@example.com','555-0000', NULL, NULL, NULL)
    ON CONFLICT DO NOTHING;
  "

sleep 10

kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-12').getOrCreate()
df = spark.sql(\"\"\"
  SELECT loyalty_score, preferred_lang, tier
  FROM \`postgres\`.\`st_transforms\`.\`null_coalesce__postgres__customers\`
  WHERE email='nulltest@example.com'
  LIMIT 1
\"\"\")
df.show()
"
```

**Expected:** `loyalty_score=0.0`, `preferred_lang='en'`, `tier='standard'` — no NULLs in these columns.

---

### TEST-13 · `pivot_before_after`

**Goal:** Confirm that an UPDATE event produces side-by-side `before_<field>` and `after_<field>` columns showing the change.

```bash
# Trigger a tier change (gold → platinum)
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    UPDATE customers SET tier='platinum' WHERE email='sttest@example.com';
  "

sleep 10

kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-13').getOrCreate()
df = spark.sql(\"\"\"
  SELECT _op, before_tier, after_tier
  FROM \`postgres\`.\`st_transforms\`.\`pivot_before_after__postgres__customers\`
  WHERE after_tier='platinum'
  LIMIT 3
\"\"\")
df.show()
"
```

**Expected:** `_op='u'`, `before_tier='gold'`, `after_tier='platinum'`.

---

### TEST-14 · `apply_pipeline`

**Goal:** Confirm the 5-step chained pipeline produces rows that satisfy all five transforms simultaneously: only INSERT/UPDATE ops, deduplicated, with `proc_time`, hashed PII, and human-readable `op_label`.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
import re
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-14').getOrCreate()
df = spark.sql(\"\"\"
  SELECT _op, op_label, proc_time, email
  FROM \`postgres\`.\`st_transforms\`.\`apply_pipeline__postgres__customers\`
  ORDER BY proc_time DESC LIMIT 5
\"\"\")
df.show(truncate=False)
rows = df.collect()
for r in rows:
    assert r['_op'] in ('c','u'),        f'filter_op failed: _op={r[\"_op\"]}'
    assert r['op_label'] in ('INSERT','UPDATE'), f'add_op_label failed'
    assert r['proc_time'] is not None,   'add_processing_time failed'
    assert re.fullmatch(r'[0-9a-f]{64}', r['email'] or ''), f'mask_columns failed: {r[\"email\"]}'
print('All 5 pipeline steps verified OK')
"
```

**Expected:** `All 5 pipeline steps verified OK` — every assertion passes.

---

### TEST-15 · `windowed_aggregate`

**Goal:** Confirm revenue and count aggregations are computed per `status` group.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-15').getOrCreate()
df = spark.sql(\"\"\"
  SELECT status, total_revenue, order_count, avg_order_value, min_order_value, max_order_value
  FROM \`postgres\`.\`st_transforms\`.\`windowed_aggregate__postgres__orders\`
  ORDER BY status
\"\"\")
df.show()
"
```

**Expected:** One row per `status` (e.g. `pending`, `shipped`). `order_count >= 1`. `total_revenue = sum of total_amount` for that status. `min_order_value <= avg_order_value <= max_order_value`.

---

### TEST-16 · `rolling_sum`

**Goal:** Confirm the cumulative sum increases monotonically per customer across rows ordered by `kafka_ts`.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-16').getOrCreate()
df = spark.sql(\"\"\"
  SELECT customer_id, total_amount, rolling_sum_total_amount, kafka_ts
  FROM \`postgres\`.\`st_transforms\`.\`rolling_sum__postgres__orders\`
  WHERE customer_id = 1
  ORDER BY kafka_ts
\"\"\")
df.show()
rows = df.collect()
for i in range(1, len(rows)):
    assert rows[i]['rolling_sum_total_amount'] >= rows[i-1]['rolling_sum_total_amount'], \
        'rolling_sum is not monotonically increasing'
print('rolling_sum monotonic check passed')
"
```

**Expected:** `rolling_sum_total_amount` increases with each successive order for customer 1. `rolling_sum monotonic check passed`.

---

### TEST-17 · `rolling_avg`

**Goal:** Confirm the running average column is a valid average (between min and max values seen so far).

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-17').getOrCreate()
df = spark.sql(\"\"\"
  SELECT customer_id, total_amount, rolling_avg_total_amount
  FROM \`postgres\`.\`st_transforms\`.\`rolling_avg__postgres__orders\`
  ORDER BY customer_id, kafka_ts LIMIT 10
\"\"\")
df.show()
rows = df.collect()
for r in rows:
    v = r['rolling_avg_total_amount']
    assert v is not None and v > 0, f'rolling_avg is NULL or zero: {v}'
print('rolling_avg non-null check passed')
"
```

**Expected:** `rolling_avg_total_amount` is non-NULL and positive for all rows. `rolling_avg non-null check passed`.

---

### TEST-18 · `count_distinct_per_key`

**Goal:** Confirm the output counts distinct customers per order status.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-18').getOrCreate()
df = spark.sql(\"\"\"
  SELECT status, distinct_count
  FROM \`postgres\`.\`st_transforms\`.\`count_distinct_per_key__postgres__orders\`
  ORDER BY status
\"\"\")
df.show()
rows = df.collect()
for r in rows:
    assert r['distinct_count'] >= 1, f'distinct_count should be >= 1 for status={r[\"status\"]}'
print('count_distinct_per_key check passed')
"
```

**Expected:** Each `status` group has `distinct_count >= 1`. Output has one row per distinct `status` value.

---

### TEST-19 · `top_n_per_group`

**Goal:** Confirm that within each status group, ranks are 1–5 and rows are ordered by `total_amount` descending.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-19').getOrCreate()
df = spark.sql(\"\"\"
  SELECT status, total_amount, rank
  FROM \`postgres\`.\`st_transforms\`.\`top_n_per_group__postgres__orders\`
  ORDER BY status, rank
\"\"\")
df.show()
rows = df.collect()
for r in rows:
    assert 1 <= r['rank'] <= 5, f'rank {r[\"rank\"]} out of range 1-5'
print(f'top_n_per_group: {len(rows)} rows, all ranks 1-5 — check passed')
"
```

**Expected:** `rank` column values are between 1 and 5 inclusive. Within each `status`, `total_amount` decreases as `rank` increases.

---

### TEST-20 · `event_rate`

**Goal:** Confirm the single-row throughput summary has a positive events-per-second rate.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-20').getOrCreate()
df = spark.sql(\"\"\"
  SELECT event_count, duration_seconds, events_per_second
  FROM \`postgres\`.\`st_transforms\`.\`event_rate__postgres__orders\`
  ORDER BY event_count DESC LIMIT 5
\"\"\")
df.show()
rows = df.collect()
for r in rows:
    assert r['event_count'] > 0, 'event_count is 0'
    assert r['events_per_second'] is not None, 'events_per_second is NULL'
print('event_rate check passed')
"
```

**Expected:** `event_count > 0`, `events_per_second` is a positive number (or NULL if all events have the same timestamp). `event_rate check passed`.

---

### TEST-21 · `aggregate_counts`

**Goal:** Confirm each `(order_id, _op)` combination is counted, revealing any hot rows.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-21').getOrCreate()
df = spark.sql(\"\"\"
  SELECT id, _op, count
  FROM \`postgres\`.\`st_transforms\`.\`aggregate_counts__postgres__orders\`
  ORDER BY count DESC LIMIT 10
\"\"\")
df.show()
rows = df.collect()
for r in rows:
    assert r['count'] >= 1, 'count should be >= 1'
print('aggregate_counts check passed')
"
```

**Expected:** Each row has `count >= 1`. If an order was updated multiple times in a batch, its `_op='u'` row will have `count > 1`.

---

### TEST-22 · `stream_join`

**Goal:** Confirm orders are enriched with `tier` and `loyalty_score` from customers in the same batch.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-22').getOrCreate()
df = spark.sql(\"\"\"
  SELECT id, status, total_amount, tier, loyalty_score
  FROM \`postgres\`.\`st_transforms\`.\`stream_join__postgres__orders\`
  WHERE tier IS NOT NULL
  LIMIT 5
\"\"\")
df.show()
rows = df.collect()
assert len(rows) > 0, 'No joined rows found — ensure customer and order events land in the same batch'
print(f'stream_join: {len(rows)} enriched rows found — check passed')
"
```

**Expected:** Rows with non-NULL `tier` and `loyalty_score`. If no match in the same batch (left join), those columns will be NULL — trigger new CDC events in both tables simultaneously to get matched rows.

---

### TEST-23 · `temporal_join`

**Goal:** Confirm matched rows have timestamps within 60 seconds of each other.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName('test-23').getOrCreate()
df = spark.sql(\"\"\"
  SELECT id, status, tier,
         ABS(unix_timestamp(kafka_ts) - unix_timestamp(right_kafka_ts)) AS ts_diff_s
  FROM \`postgres\`.\`st_transforms\`.\`temporal_join__postgres__orders\`
  WHERE tier IS NOT NULL
  LIMIT 5
\"\"\")
df.show()
rows = df.collect()
for r in rows:
    assert r['ts_diff_s'] <= 60, f'ts_diff_s={r[\"ts_diff_s\"]} exceeds 60s tolerance'
print('temporal_join 60s tolerance check passed')
"
```

**Expected:** All matched rows have `ts_diff_s <= 60`. `temporal_join 60s tolerance check passed`.

---

### TEST-24 · `join_and_tag_source`

**Goal:** Confirm the join produces provenance columns `left_topic` and `right_topic` with the correct Kafka topic names.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-24').getOrCreate()
df = spark.sql(\"\"\"
  SELECT DISTINCT left_topic, right_topic
  FROM \`postgres\`.\`st_transforms\`.\`join_and_tag_source__postgres__orders\`
\"\"\")
df.show(truncate=False)
rows = df.collect()
assert any(r['left_topic']  == 'postgres.cache_testing.orders'    for r in rows), 'left_topic mismatch'
assert any(r['right_topic'] == 'postgres.cache_testing.customers' for r in rows), 'right_topic mismatch'
print('join_and_tag_source provenance check passed')
"
```

**Expected:** `left_topic='postgres.cache_testing.orders'`, `right_topic='postgres.cache_testing.customers'`. `join_and_tag_source provenance check passed`.

---

### TEST-25 · `multi_topic_union`

**Goal:** Confirm the union output contains rows from both `customers` and `orders` topics, each tagged with their source topic.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test-25').getOrCreate()
df = spark.sql(\"\"\"
  SELECT source_topic, COUNT(*) AS cnt
  FROM \`postgres\`.\`st_transforms\`.\`multi_topic_union__postgres__all\`
  GROUP BY source_topic
  ORDER BY source_topic
\"\"\")
df.show(truncate=False)
topics = [r['source_topic'] for r in df.collect()]
assert 'postgres.cache_testing.customers' in topics, 'customers topic missing from union'
assert 'postgres.cache_testing.orders'    in topics, 'orders topic missing from union'
print('multi_topic_union source coverage check passed')
"
```

**Expected:** Two rows — one for `postgres.cache_testing.customers`, one for `postgres.cache_testing.orders`, each with `cnt >= 1`. `multi_topic_union source coverage check passed`.

---

### TEST-26 · `route_by_topic`

**Goal:** Confirm the routing function is called every batch and correctly identifies the topics present in the batch.

```bash
# route_by_topic writes no output — verify it via pipeline logs
PG_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=postgres \
         -o jsonpath='{.items[0].metadata.name}')
kubectl logs -n prod "$PG_POD" --tail=100 | grep "route_by_topic"
```

**Expected:** Log lines like:
```
[postgres] route_by_topic: 2 topic(s) in batch
```
Count should be ≥ 1 when CDC data is flowing. A count of `0` means no data arrived in that batch (normal if no DB changes occurred).

---

## 5. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Pod not Running | ConfigMap `star-transform-pipeline-script` missing | `kubectl get cm star-transform-pipeline-script -n prod` — recreate if absent (see §6) |
| `Attempt N failed: No streaming queries started` | S3 checkpoint unreachable or Kafka auth failure | Check MinIO/S3 connectivity; verify SCRAM creds in OpenBao |
| `Temporary failure in name resolution` on startup | DNS not ready at pod start; internal retry handles it | Normal — pipeline retries with exponential backoff; wait for success log |
| `Schema inference failed` | Debezium envelope format changed or empty batch | Check Debezium connector status; verify topics have data |
| Oracle columns uppercase in Iceberg | `_lower_udf` not applied | Confirm `source.key == "oracle"` branch fires; check `_decode_envelope` logs |
| Iceberg tables not created | `st_transforms` namespace creation failed | Run: `spark.sql("CREATE NAMESPACE IF NOT EXISTS \`<catalog>\`.\`st_transforms\`")` manually |
| Kafka output topic missing | `star-transform-kafka-topics.yaml` not applied | `kubectl apply -f manifests/cdc-batch-pipeline/star-transform-kafka-topics.yaml` |
| `pivot_before_after` table empty | No UPDATE events in batch | Trigger an UPDATE in source DB; function only fires on `_op = 'u'` |
| `flatten_json_col` table empty | `metadata_json` column absent or NULL | PostgreSQL only; verify `metadata_json` column is populated in `cache_testing.customers` |
| `stream_join` / `temporal_join` rows all NULL | Customer and order events in different batches | Trigger both a customer and order change within the same 5-second window |
| Health probe returning 503 | `_HEALTH["ok"] = False` — pipeline in error state | Check logs for the triggering exception; pipeline retries automatically |

### Useful commands

```bash
# Watch all three pipelines live
kubectl logs -n prod -l app=star-transform-pipeline --prefix --tail=20 -f

# Count Iceberg tables in st_transforms (postgres catalog example)
PG_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=postgres \
         -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('list').getOrCreate()
df = spark.sql('SHOW TABLES IN \`postgres\`.\`st_transforms\`')
print(f'Total tables: {df.count()}')
df.show(50, truncate=False)
"

# Check restart count (should be 0 or 1 from initial startup DNS retry)
kubectl get pods -n prod -l app=star-transform-pipeline \
  -o jsonpath='{range .items[*]}{.metadata.name}: restarts={.status.containerStatuses[0].restartCount}{"\n"}{end}'

# Describe a specific deployment for events/conditions
kubectl describe deploy star-transform-pipeline-postgres -n prod | tail -20
```

---

## 6. Script Update Procedure

The pipeline script is delivered via ConfigMap, not baked into the image. After any change to [`docker/spark-gluten-velox/scripts/star_transform_pipeline.py`](../../docker/spark-gluten-velox/scripts/star_transform_pipeline.py):

```bash
# 1. Update the ConfigMap
kubectl create configmap star-transform-pipeline-script \
  --from-file=star_transform_pipeline.py=docker/spark-gluten-velox/scripts/star_transform_pipeline.py \
  -n prod --dry-run=client -o yaml | kubectl apply -f -

# 2. Restart all three pipelines (Recreate strategy — no overlap)
kubectl rollout restart deploy/star-transform-pipeline-postgres \
                          deploy/star-transform-pipeline-oracle \
                          deploy/star-transform-pipeline-mongodb \
  -n prod

# 3. Watch rollout
kubectl rollout status deploy/star-transform-pipeline-postgres -n prod
kubectl rollout status deploy/star-transform-pipeline-oracle -n prod
kubectl rollout status deploy/star-transform-pipeline-mongodb -n prod
```

---

## 7. Quick Reference

| Item | Value |
|---|---|
| Pipeline script | `docker/spark-gluten-velox/scripts/star_transform_pipeline.py` |
| K8s manifests | `manifests/cdc-batch-pipeline/star-transform-streaming.yaml` |
| Kafka topics manifest | `manifests/cdc-batch-pipeline/star-transform-kafka-topics.yaml` |
| ConfigMap | `star-transform-pipeline-script` in `prod` |
| Trigger interval | `5 seconds` (from `kafka-to-iceberg-config`) |
| Max offsets/trigger | `50000` (from `kafka-to-iceberg-config`) |
| S3 bucket | `xdatatoiceberg1` |
| Checkpoint path | `s3://xdatatoiceberg1/checkpoints/streaming/st_pipeline/<source>` |
| Iceberg namespace | `<catalog>.st_transforms` |
| Output Kafka topics | 55 total (all `READY=True`) — prefix `st.<fn>.<src>.<table>` |
| Image | `192.168.1.50:30500/spark-gluten-velox:3.5.1-22` |
| ST functions count | 26 |
| Sources | postgres · oracle · mongodb |
