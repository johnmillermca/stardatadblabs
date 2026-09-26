# Runbook 29 — CDC Debezium → Kafka → Iceberg Architecture

**Status:** Reference  
**Namespace:** `prod`  
**Last updated:** 2025

---

## Table of Contents

1. [Architecture Diagram](#1-architecture-diagram)
2. [Component Table](#2-component-table)
3. [Data Flow](#3-data-flow)
4. [Topic Mapping](#4-topic-mapping)
5. [Write Modes Comparison](#5-write-modes-comparison)
6. [Iceberg Table Design](#6-iceberg-table-design)
7. [StarTransform](#7-startransform)
8. [Performance Tuning](#8-performance-tuning)
9. [Security](#9-security)
10. [Operational Commands](#10-operational-commands)

---

## 1. Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE SYSTEMS                                          │
│                                                                                      │
│  ┌─────────────────────┐  ┌──────────────────────────┐  ┌────────────────────────┐  │
│  │   PostgreSQL 14      │  │      Oracle XE 21c        │  │       MongoDB          │  │
│  │  postgresql.prod     │  │  oracle-xe.prod:1521      │  │  mongodb.prod:27017    │  │
│  │  :5432               │  │  PDB: XEPDB1              │  │                        │  │
│  │  db: cache_testing   │  │  CACHE_TESTING (6 tables) │  │  cache_testing.*       │  │
│  │  4 tables            │  │  TPCDS (10 tables)        │  │  customers, products   │  │
│  │  WAL replication     │  │  LogMiner (redo logs)     │  │  change streams        │  │
│  └──────────┬──────────┘  └─────────────┬────────────┘  └───────────┬────────────┘  │
└─────────────┼───────────────────────────┼───────────────────────────┼───────────────┘
              │ WAL slot (rbac user)       │ LogMiner (c##dbzcdc)      │ Change stream (root)
              ▼                           ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           DEBEZIUM KAFKA CONNECT                                    │
│                    http://192.168.1.54:30083  (NodePort, worker2.local)             │
│                                                                                     │
│  ┌───────────────────────────┐  ┌───────────────────────────┐  ┌─────────────────┐ │
│  │ postgres-cache-testing-cdc│  │  oracle-cache-testing-cdc  │  │mongodb-cache-   │ │
│  │ PostgresConnector          │  │  OracleConnector            │  │testing-cdc      │ │
│  │ snapshot.mode=never        │  │  snapshot.mode=schema_only  │  │MongoDbConnector │ │
│  └───────────────────────────┘  ├───────────────────────────┤  │snapshot.mode=   │ │
│                                  │  oracle-tpcds-cdc           │  │never            │ │
│                                  │  OracleConnector            │  └─────────────────┘ │
│                                  │  snapshot.mode=schema_only  │                      │
│                                  └───────────────────────────┘                      │
│  Schema Registry: http://schema-registry.prod.svc.cluster.local:8081                │
└──────────────────────────────────────┬──────────────────────────────────────────────┘
                                       │ Avro messages (key + envelope)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           KAFKA (Strimzi KRaft)                                     │
│          strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092                  │
│                   SASL/SCRAM-SHA-512 · 3 partitions · lz4 · 7-day retention        │
│                                                                                     │
│  postgres.*   oracle.tpcds.*   oracle.cache_testing.*   mongodb.cache_testing.*     │
│  schema-changes.postgres / .oracle / .mongodb                                       │
└──────────────────────────────────────┬──────────────────────────────────────────────┘
                                       │ Spark Kafka source (startingOffsets=latest)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        SPARK STRUCTURED STREAMING                                   │
│            192.168.1.50:30500/spark-gluten-velox:3.5.1-12  (Kubernetes)            │
│                    Namespace: prod · Trigger: 2-second micro-batch                  │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐        │
│  │  05_kafka_to_iceberg_streaming.py                                        │        │
│  │                                                                          │        │
│  │  1. Avro deserialise (Schema Registry UDF)                               │        │
│  │  2. Parse Debezium envelope  (before / after / op / source / ts_ms)      │        │
│  │  3. StarTransform pipeline   (deduplicate, mask_pii, add_op_label, ...)  │        │
│  │  4. foreachBatch → Iceberg MERGE / append                                │        │
│  └─────────────────────────────────────────────────────────────────────────┘        │
│                                                                                     │
│  Active deployment (replicas=1):  kafka-to-iceberg-standard                        │
│  Standby deployments (replicas=0): soft-delete · history-tracking                  │
│                                                                                     │
│  Secrets via OpenBao: http://openbao.prod.svc.cluster.local:8200                   │
└──────────────────────────────────────┬──────────────────────────────────────────────┘
                                       │ Iceberg REST catalog (OAuth2)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                       POLARIS REST CATALOG + S3 STORAGE                             │
│                                                                                     │
│  Catalogs: postgres  ·  oracle  ·  mongodb                                          │
│  Storage:  s3://xdatatoiceberg1/iceberg/                                            │
│  Format:   Parquet · snappy · format-version 2 · 256 MB target files               │
│  Partitioning: hours(snap_timestamp) + bucket(16, pk_col)                           │
└──────────────────────────────────────┬──────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              QUERY TOOLS                                            │
│                                                                                     │
│   Spark SQL · Trino · Flink · DuckDB (all via Polaris REST catalog)                 │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Table

| Component | Role | Endpoint / Location | Version / Notes |
|---|---|---|---|
| PostgreSQL | CDC source — WAL replication | `postgresql.prod.svc.cluster.local:5432` | db: `cache_testing`; replication user: `rbac` |
| Oracle XE | CDC source — LogMiner | `oracle-xe.prod.svc.cluster.local:1521` | PDB: `XEPDB1`; CDC user: `c##dbzcdc` |
| MongoDB | CDC source — change streams | `mongodb.prod.svc.cluster.local:27017` | user: `root`; db: `cache_testing` |
| Debezium Kafka Connect | Capture & publish CDC events | `http://192.168.1.54:30083` (NodePort) | Pod on `worker2.local`; 4 connectors; image `192.168.1.50:30500/debezium/connect:2.7` |
| Schema Registry (Confluent-compat) | Avro schema storage & evolution | `http://schema-registry.prod.svc.cluster.local:8081` | Keyed by subject (topic + key/value) |
| Kafka (Strimzi KRaft) | Message bus — durable ordered log | `strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092` | SASL/SCRAM-SHA-512; 3 partitions; lz4; 7-day retention |
| Spark Structured Streaming | Stream processing & Iceberg writes | Kubernetes `prod` namespace | Spark 3.5.1 + Gluten/Velox; image `192.168.1.50:30500/spark-gluten-velox:3.5.1-12` |
| Polaris REST Catalog | Iceberg metastore & REST API | Internal cluster service | OAuth2; catalogs: `postgres`, `oracle`, `mongodb` |
| S3 Object Storage | Parquet data files & metadata | `s3://xdatatoiceberg1/iceberg/` | Iceberg format-version 2 |
| OpenBao | Secrets management (credentials) | `http://openbao.prod.svc.cluster.local:8200` | Kafka SCRAM, DB passwords, S3 keys |
| StarTransform (`star_transform.py`) | Reusable DataFrame transform functions | Bundled in Spark image | See Section 7 |

---

## 3. Data Flow

### Step-by-step: source transaction → Iceberg snapshot

```
Step 1  SOURCE TRANSACTION
        A DML statement (INSERT / UPDATE / DELETE) executes on the source database.
        PostgreSQL writes to its WAL; Oracle appends to redo logs; MongoDB emits a
        change stream event.

Step 2  DEBEZIUM CAPTURE
        The connector polls the source log and produces an Avro-encoded Kafka message.
        Key   = primary key field(s) of the changed row (Avro schema registered in
                Schema Registry under <topic>-key).
        Value = Debezium change event envelope (schema registered under <topic>-value).

Step 3  DEBEZIUM ENVELOPE (Avro payload, simplified JSON representation)
        {
          "before": "<JSON string of old row — populated for UPDATE and DELETE>",
          "after":  "<JSON string of new row — populated for INSERT and UPDATE>",
          "op":     "c"  // c=INSERT  u=UPDATE  d=DELETE  r=snapshot read
          "source": {
            "connector": "postgresql",
            "db": "cache_testing",
            "schema": "public",
            "table": "customers",
            "lsn": 12345678,
            "ts_ms": 1718000000000
          },
          "ts_ms": 1718000000123   // connector processing timestamp
        }

Step 4  KAFKA TOPIC
        Message lands in the appropriate topic (e.g. postgres.cache_testing.customers).
        Stored with lz4 compression; retained 7 days; replicated across Kafka brokers.

Step 5  SPARK READS MICRO-BATCH
        The Streaming job triggers every 2 seconds, reading up to MAX_OFFSETS_PER_TRIGGER
        (50 000) messages from all subscribed topics.

Step 6  AVRO DESERIALISATION
        A UDF calls Schema Registry to fetch the schema for each message's schema-id
        prefix and deserialises the Avro bytes into a Spark Row.

Step 7  ENVELOPE PARSING
        The streaming job extracts before, after, op, and ts_ms from each envelope.
        For before/after (JSON strings): ST.flatten_json_col() or inline fromJson().

Step 8  STARTRANSFORM PIPELINE
        Configurable steps (TRANSFORM_PIPELINE env var) are applied in sequence:
          deduplicate      → last-write-wins per PK within the micro-batch
          mask_pii         → SHA-256 hash PII_COLUMNS (email, phone, ssn, …)
          add_processing_time → inject proc_time TIMESTAMP
          add_op_label     → human-readable INSERT/UPDATE/DELETE label
          add_source_tag   → inject source_system STRING

Step 9  foreachBatch — WRITE MODE DISPATCH
        The active WRITE_MODE determines the Iceberg operation:
          standard         → MERGE upsert + hard DELETE
          soft_delete      → MERGE upsert + logical DELETE (is_deleted flag)
          history_tracking → always INSERT into <table>_hist

Step 10 ICEBERG MERGE / APPEND
        Spark issues an Iceberg MERGE INTO or DataFrame.write.format("iceberg")
        against the Polaris REST catalog.  snap_id (monotonically_increasing_id)
        and snap_timestamp (current_timestamp) are appended to every row.
        AQE coalesces small files; Iceberg's hidden partitioning routes files to
        hours(snap_timestamp) + bucket(16, pk) partitions.

Step 11 S3 DATA FILES
        Parquet files land under s3://xdatatoiceberg1/iceberg/<catalog>/<ns>/<table>/.
        Iceberg metadata JSON files (snapshot, manifest list, manifests) are updated
        atomically; the new snapshot becomes visible to all readers immediately.

Step 12 QUERY TOOLS
        Spark SQL / Trino / Flink connect to the Polaris REST catalog and query the
        latest snapshot.  Partition pruning on snap_timestamp makes time-ranged
        queries efficient; bucket pruning on pk_col accelerates point lookups.
```

---

## 4. Topic Mapping

### PostgreSQL

| Source Table | Kafka Topic | Iceberg Catalog | Iceberg Table |
|---|---|---|---|
| `public.customers` | `postgres.cache_testing.customers` | `postgres` | `cache_testing.customers` |
| `public.products` | `postgres.cache_testing.products` | `postgres` | `cache_testing.products` |
| `public.product_reviews` | `postgres.cache_testing.product_reviews` | `postgres` | `cache_testing.product_reviews` |
| `public.orders` | `postgres.cache_testing.orders` | `postgres` | `cache_testing.orders` |
| _(DDL)_ | `schema-changes.postgres` | — | — |

### Oracle — CACHE_TESTING schema

| Source Table | Kafka Topic | Iceberg Catalog | Iceberg Table |
|---|---|---|---|
| `CACHE_TESTING.CUSTOMERS` | `oracle.cache_testing.customers` | `oracle` | `cache_testing.customers` |
| `CACHE_TESTING.PRODUCTS` | `oracle.cache_testing.products` | `oracle` | `cache_testing.products` |
| `CACHE_TESTING.ORDERS` | `oracle.cache_testing.orders` | `oracle` | `cache_testing.orders` |
| `CACHE_TESTING.ORDER_ITEMS` | `oracle.cache_testing.order_items` | `oracle` | `cache_testing.order_items` |
| `CACHE_TESTING.PRODUCT_REVIEWS` | `oracle.cache_testing.product_reviews` | `oracle` | `cache_testing.product_reviews` |
| `CACHE_TESTING.INVENTORY_EVENTS` | `oracle.cache_testing.inventory_events` | `oracle` | `cache_testing.inventory_events` |
| _(DDL)_ | `schema-changes.oracle` | — | — |

### Oracle — TPCDS schema

| Source Table | Kafka Topic | Iceberg Catalog | Iceberg Table |
|---|---|---|---|
| `TPCDS.CALL_CENTER` | `oracle.tpcds.call_center` | `oracle` | `tpcds.call_center` |
| `TPCDS.CATALOG_PAGE` | `oracle.tpcds.catalog_page` | `oracle` | `tpcds.catalog_page` |
| `TPCDS.HOUSEHOLD_DEMOGRAPHICS` | `oracle.tpcds.household_demographics` | `oracle` | `tpcds.household_demographics` |
| `TPCDS.INCOME_BAND` | `oracle.tpcds.income_band` | `oracle` | `tpcds.income_band` |
| `TPCDS.PROMOTION` | `oracle.tpcds.promotion` | `oracle` | `tpcds.promotion` |
| `TPCDS.REASON` | `oracle.tpcds.reason` | `oracle` | `tpcds.reason` |
| `TPCDS.SHIP_MODE` | `oracle.tpcds.ship_mode` | `oracle` | `tpcds.ship_mode` |
| `TPCDS.WAREHOUSE` | `oracle.tpcds.warehouse` | `oracle` | `tpcds.warehouse` |
| `TPCDS.WEB_PAGE` | `oracle.tpcds.web_page` | `oracle` | `tpcds.web_page` |
| `TPCDS.WEB_SITE` | `oracle.tpcds.web_site` | `oracle` | `tpcds.web_site` |

### MongoDB

| Source Collection | Kafka Topic | Iceberg Catalog | Iceberg Table |
|---|---|---|---|
| `cache_testing.customers` | `mongodb.cache_testing.customers` | `mongodb` | `cache_testing.customers` |
| `cache_testing.products` | `mongodb.cache_testing.products` | `mongodb` | `cache_testing.products` |
| _(DDL)_ | `schema-changes.mongodb` | — | — |

---

## 5. Write Modes Comparison

| Attribute | `standard` (SCD Type 0) | `soft_delete` | `history_tracking` |
|---|---|---|---|
| **Deployment** | `kafka-to-iceberg-standard` | `kafka-to-iceberg-soft-delete` | `kafka-to-iceberg-history-tracking` |
| **Target table** | `<catalog>.<ns>.<table>` | `<catalog>.<ns>.<table>` (same) | `<catalog>.<ns>.<table>_hist` |
| **INSERT/UPDATE op** | MERGE MATCHED UPDATE + NOT MATCHED INSERT | MERGE upsert; `is_deleted=false`, `deleted_at=NULL` | Always append-INSERT; `_change_type='INSERT'` or `'UPDATE'` |
| **DELETE op** | MERGE MATCHED DELETE (hard delete — row gone) | MERGE UPDATE SET `is_deleted=true`, `deleted_at=now()` | Append-INSERT; `_change_type='DELETE'`; after_* columns NULL |
| **before/after columns** | Only `after` columns stored | Only `after` columns stored | `before_<col>` + `after_<col>` side-by-side |
| **Extra columns** | `snap_id BIGINT`, `snap_timestamp TIMESTAMP` | `snap_id`, `snap_timestamp`, `is_deleted BOOLEAN`, `deleted_at TIMESTAMP` | `_change_type STRING`, `_change_ts TIMESTAMP`, `snap_id`, `snap_timestamp` |
| **Schema evolution** | Standard Iceberg ALTER TABLE | Standard Iceberg ALTER TABLE | `mergeSchema=true` on each write |
| **Use case** | Current state; storage-efficient; no history | Current state + logical delete audit trail | Full audit log; time-travel queries; regulatory compliance |
| **Storage cost** | Lowest | Low (soft-deleted rows retained) | Highest (every change is a new row) |
| **Query complexity** | Simplest — no filter needed | Add `WHERE NOT is_deleted` for live rows | JOIN on pk + ORDER BY `_change_ts` for history |

---

## 6. Iceberg Table Design

### Partitioning

All tables use a two-level hidden partition spec:

```
PartitionSpec {
  hours(snap_timestamp),     -- hourly bucket on the write-wall-clock timestamp
  bucket(16, <pk_col>)       -- 16-way hash bucket on the primary key column
}
```

- **`snap_timestamp`** is `current_timestamp()` at the time Spark writes the batch (wall-clock, not source event time). This makes partition pruning predictable for recent-data queries.
- **`bucket(16, pk_col)`** distributes rows evenly and enables predicate pushdown for point lookups (`WHERE customer_id = ?`).

### File Format

| Setting | Value |
|---|---|
| File format | Parquet |
| Compression | Snappy |
| Iceberg format version | 2 (row-level deletes, position/equality delete files) |
| Target file size | 256 MB |
| AQE coalesce target | 64 MB (`ADAPTIVE_COALESCE_TARGET=67108864`) |
| Files coalesced before merge | `COALESCE_BEFORE_MERGE=4` (peak: 8) |

### snap_id and snap_timestamp Columns

Every row in every Iceberg table written by this pipeline carries two system columns:

| Column | Type | Source | Purpose |
|---|---|---|---|
| `snap_id` | `BIGINT` | `monotonically_increasing_id()` cast to BIGINT | Unique-per-row within a batch; not globally sequential across batches |
| `snap_timestamp` | `TIMESTAMP` | `current_timestamp()` — same value for all rows in a micro-batch | Records when the row was physically written; drives hourly partitioning |

> **Note:** `snap_timestamp` is the _write_ timestamp, not the _source event_ timestamp. Use `ts_ms` from the Debezium envelope if you need the source-side event time.

### Format Version 2 Implications

- Row-level DELETEs in `standard` mode use Iceberg equality-delete files, avoiding full file rewrites during MERGE.
- Position-delete files may be produced during MERGE; compaction (not covered here) should be run periodically to merge delete files into data files.

---

## 7. StarTransform

### What It Is

`star_transform.py` is a library of reusable PySpark DataFrame transformation functions bundled inside the Spark image. Import it as:

```python
import star_transform as ST
```

All functions accept a Spark DataFrame as their first argument and return a transformed DataFrame, making them composable.

### Available Functions

| Function | Signature | Description |
|---|---|---|
| `filter_op` | `(df, ops=["c","u"])` | Keep rows matching Debezium op codes (`c`=INSERT, `u`=UPDATE, `d`=DELETE, `r`=snapshot) |
| `deduplicate` | `(pk, order_col="kafka_ts")` | Last-write-wins deduplication per PK within a micro-batch |
| `add_processing_time` | `(col_name="proc_time")` | Inject current TIMESTAMP as a new column |
| `rename_columns` | `(mapping)` | Rename columns; `mapping` = `{old_name: new_name, …}` |
| `cast_columns` | `(casts)` | Cast columns; `casts` = `{col_name: "target_type", …}` |
| `drop_columns` | `(columns)` | Drop a list of column names |
| `mask_columns` | `(columns, algorithm="sha256")` | Replace PII columns with SHA-256 hex digest |
| `add_source_tag` | `(source_system, col_name="source_system")` | Inject a literal STRING column with the source system name |
| `add_op_label` | `(op_col="_op", label_col="op_label")` | Map op codes to `"INSERT"` / `"UPDATE"` / `"DELETE"` |
| `flatten_json_col` | `(json_col, schema, prefix="")` | Parse a JSON string column into typed columns using the provided schema |
| `enrich_from_broadcast` | `(dim_df, join_col, select_cols, how="left")` | Broadcast-join a small dimension DataFrame onto the stream |
| `aggregate_counts` | `(pk_col, op_col, out_col)` | Count operations per (pk, op) — useful for metrics |
| `pivot_before_after` | `(before_col, after_col, schema)` | Expand Debezium `before` / `after` JSON strings to `before_<col>` and `after_<col>` pairs |
| `filter_columns` | `(keep)` | Project — keep only listed columns |
| `null_coalesce` | `(defaults)` | `coalesce(col, default)` for each entry in `defaults` dict |
| `route_by_topic` | `(df)` | Split a multi-topic DataFrame into a `{topic_name: DataFrame}` dict |
| `apply_pipeline` | `(df, [(fn, kwargs), …])` | Chain a list of `(function_reference, kwargs_dict)` steps |

### Example Pipeline

```python
import star_transform as ST

def transform(df):
    return ST.apply_pipeline(df, [
        (ST.filter_op,           {"ops": ["c", "u", "d"]}),
        (ST.deduplicate,         {"pk": "customer_id", "order_col": "kafka_ts"}),
        (ST.mask_columns,        {"columns": ["email", "phone"], "algorithm": "sha256"}),
        (ST.add_processing_time, {"col_name": "proc_time"}),
        (ST.add_op_label,        {"op_col": "op", "label_col": "op_label"}),
        (ST.add_source_tag,      {"source_system": "postgres", "col_name": "source_system"}),
    ])
```

The `TRANSFORM_PIPELINE` environment variable selects named steps; custom pipelines can be coded directly as shown above.

---

## 8. Performance Tuning

### Normal-Operation Settings

| Environment Variable | Default | Description |
|---|---|---|
| `MAX_OFFSETS_PER_TRIGGER` | `50000` | Maximum Kafka messages per micro-batch |
| `MERGE_PARALLELISM` | `8` | Spark shuffle partitions during MERGE |
| `COALESCE_BEFORE_MERGE` | `4` | Coalesce partitions before writing to limit small files |
| `ADAPTIVE_COALESCE_TARGET` | `67108864` (64 MB) | AQE target partition size |

### Peak-Hour Settings

| Environment Variable | Peak Value | Effect |
|---|---|---|
| `MERGE_PARALLELISM` | `16` or `32` | More parallelism for large MERGE operations |
| `COALESCE_BEFORE_MERGE` | `8` | Larger output files; fewer S3 PUT requests |

### Kafka Consumer Tuning

```properties
# Increase fetch size for high-throughput topics
fetch.min.bytes=65536
fetch.max.wait.ms=500
max.partition.fetch.bytes=10485760   # 10 MB per partition per fetch

# Back-pressure: lower MAX_OFFSETS_PER_TRIGGER to protect Spark on lag spikes
MAX_OFFSETS_PER_TRIGGER=20000
```

### Spark AQE Settings

```sql
-- Enabled by default in Spark 3.x
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 67108864;  -- 64 MB
SET spark.sql.adaptive.skewJoin.enabled = true;
```

### Iceberg Write Optimisations

```python
# In SparkSession builder
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.<catalog>.cache-enabled", "true")
.config("spark.sql.catalog.<catalog>.cache.expiration-interval-ms", "30000")
# Target file size (also configurable per table via ALTER TABLE SET TBLPROPERTIES)
.config("spark.sql.iceberg.write.target-file-size-bytes", "268435456")  # 256 MB
```

---

## 9. Security

### Kafka — SASL/SCRAM-SHA-512

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="<kafka-user>" \
  password="<kafka-password>";
```

Credentials are stored in OpenBao and injected into the Spark pod via a Kubernetes Secret. The Debezium Kafka Connect worker uses the same mechanism for its producer configuration.

### Oracle CDC User Permissions

The `c##dbzcdc` user requires the following grants:

```sql
-- Minimum required Oracle grants for LogMiner CDC
GRANT CREATE SESSION            TO c##dbzcdc CONTAINER=ALL;
GRANT SET CONTAINER             TO c##dbzcdc CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION    TO c##dbzcdc CONTAINER=ALL;
GRANT LOGMINING                 TO c##dbzcdc CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR    TO c##dbzcdc CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D  TO c##dbzcdc CONTAINER=ALL;
GRANT SELECT ON V_$LOG          TO c##dbzcdc CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE      TO c##dbzcdc CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzcdc CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE     TO c##dbzcdc CONTAINER=ALL;
GRANT SELECT ON V_$THREAD       TO c##dbzcdc CONTAINER=ALL;
```

### PostgreSQL CDC User Permissions

```sql
-- replication slot ownership + table read access
ALTER USER rbac REPLICATION;
GRANT CONNECT ON DATABASE cache_testing TO rbac;
GRANT USAGE   ON SCHEMA public TO rbac;
GRANT SELECT  ON ALL TABLES IN SCHEMA public TO rbac;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO rbac;
```

### OpenBao Secret Paths

| Secret | Path (example) |
|---|---|
| Kafka SCRAM credentials | `secret/prod/kafka/scram` |
| PostgreSQL password | `secret/prod/postgres/dbzcdc` |
| Oracle password | `secret/prod/oracle/c##dbzcdc` |
| MongoDB password | `secret/prod/mongodb/root` |
| S3 access key / secret | `secret/prod/s3/iceberg` |
| Polaris OAuth2 client secret | `secret/prod/polaris/client` |

Access OpenBao:

```bash
# List secrets at a path
curl -H "X-Vault-Token: <token>" \
  http://openbao.prod.svc.cluster.local:8200/v1/secret/prod/kafka/scram

# Read a specific secret
curl -H "X-Vault-Token: <token>" \
  http://openbao.prod.svc.cluster.local:8200/v1/secret/data/prod/kafka/scram \
  | jq '.data.data'
```

---

## 10. Operational Commands

### Start / Stop / Switch Write Modes

```bash
# Activate standard mode (SCD Type 0)
kubectl scale deployment kafka-to-iceberg-standard         -n prod --replicas=1
kubectl scale deployment kafka-to-iceberg-soft-delete      -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-history-tracking -n prod --replicas=0

# Activate soft_delete mode
kubectl scale deployment kafka-to-iceberg-standard         -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-soft-delete      -n prod --replicas=1
kubectl scale deployment kafka-to-iceberg-history-tracking -n prod --replicas=0

# Activate history_tracking mode
kubectl scale deployment kafka-to-iceberg-standard         -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-soft-delete      -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-history-tracking -n prod --replicas=1

# Stop all modes (pause pipeline)
kubectl scale deployment kafka-to-iceberg-standard         -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-soft-delete      -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-history-tracking -n prod --replicas=0
```

### Check Connector Status

```bash
# List all connectors
curl -s http://192.168.1.54:30083/connectors | jq .

# Check a specific connector status
curl -s http://192.168.1.54:30083/connectors/postgres-cache-testing-cdc/status | jq .
curl -s http://192.168.1.54:30083/connectors/oracle-cache-testing-cdc/status   | jq .
curl -s http://192.168.1.54:30083/connectors/oracle-tpcds-cdc/status           | jq .
curl -s http://192.168.1.54:30083/connectors/mongodb-cache-testing-cdc/status  | jq .

# Restart a connector
curl -X POST http://192.168.1.54:30083/connectors/postgres-cache-testing-cdc/restart

# Pause / Resume a connector
curl -X PUT http://192.168.1.54:30083/connectors/postgres-cache-testing-cdc/pause
curl -X PUT http://192.168.1.54:30083/connectors/postgres-cache-testing-cdc/resume
```

### View Iceberg Snapshots

```sql
-- List recent snapshots for a table
SELECT snapshot_id, committed_at, operation, summary
FROM postgres.cache_testing.customers.snapshots
ORDER BY committed_at DESC
LIMIT 10;

-- Check current snapshot metadata
SELECT *
FROM postgres.cache_testing.customers.metadata_log_entries
ORDER BY timestamp DESC
LIMIT 5;

-- Show table partitions
SELECT partition, file_count, total_size
FROM postgres.cache_testing.customers.partitions;

-- Time-travel: query data as of a specific snapshot
SELECT * FROM postgres.cache_testing.customers
VERSION AS OF <snapshot_id>;

-- Time-travel: query data as of a timestamp
SELECT * FROM postgres.cache_testing.customers
TIMESTAMP AS OF '2025-01-15 10:00:00';
```

### Rolling Restart (e.g. after ConfigMap change)

```bash
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

### View Streaming Job Logs

```bash
# Tail the active streaming pod logs
kubectl logs -n prod -l app=kafka-to-iceberg-standard -f

# Get last 100 lines
kubectl logs -n prod -l app=kafka-to-iceberg-standard --tail=100
```

### DRY_RUN Mode (stream without Iceberg writes)

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod DRY_RUN=1
# (restore)
kubectl set env deployment/kafka-to-iceberg-standard -n prod DRY_RUN=0
```
