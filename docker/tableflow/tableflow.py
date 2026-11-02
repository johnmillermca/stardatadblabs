#!/usr/bin/env python3
"""
tableflow.py
============
Kafka-native TableFlow — continuously mirrors ksqlDB-derived _standard, _sd
and _hist topics into Apache Iceberg tables via Polaris REST catalog.

Behaviour mirrors Confluent TableFlow:
  - Auto-creates Iceberg tables on first message (schema inferred from JSON)
  - Auto-evolves schema when new fields appear in the Kafka message
  - Commits Iceberg snapshots atomically — Kafka offset only advanced after
    the snapshot is committed to Polaris (exactly-once delivery guarantee)
  - _standard topics → UPSERT  (latest state per PK, raw Debezium after/before)
  - _sd  topics      → UPSERT  (merge-on-read, keyed by PK_ID)
  - _hist topics     → APPEND  (every event is a new Iceberg row)
  - Dynamic topic discovery — polls Kafka metadata every TOPIC_REFRESH_INTERVAL
    seconds and automatically subscribes to any new *_standard / *_sd / *_hist
    topics.  Also exposes POST /refresh-topics (port 8090) so the Lakehouse UI
    provision endpoint can signal an IMMEDIATE re-subscribe the moment a new
    ksqlDB stream is created — no pod restart required.

Sources handled (single pod per SOURCE env var):
  postgres  → reads postgres.cache_testing.*_standard / *_sd / *_hist
  oracle    → reads oracle.cache_testing.*_standard  / *_sd  / *_hist
  mongodb   → reads mongodb.cache_testing.*_standard / *_sd  / *_hist

Each source writes to its own Polaris warehouse:
  postgres → pg_lakehouse  / cache_testing namespace
  oracle   → ora_lakehouse / cache_testing namespace
  mongodb  → mgo_lakehouse / cache_testing namespace

Credentials — all from OpenBao via K8s SA JWT (role: platform-secrets-read):
  secret/data/platform/s3      → S3 access/secret key, endpoint, region, bucket
  secret/data/platform/polaris → Polaris client_id, client_secret, url
  secret/data/platform/kafka   → bootstrap, username, password

Environment variables:
  SOURCE                 postgres | oracle | mongodb  (required)
  BATCH_SIZE             messages per Iceberg write batch  (default 500)
  FLUSH_INTERVAL         max seconds before forced flush   (default 10)
  TOPIC_REFRESH_INTERVAL seconds between Kafka topic-list polls (default 60)
  SIGNAL_PORT            port for the HTTP signal server    (default 8090)
  BAO_ADDR               OpenBao address override
  DRY_RUN                1 = consume but do not write to Iceberg
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, TopicPartition
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo, In
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("tableflow")

# ── Config ─────────────────────────────────────────────────────────────────────
SOURCE         = os.environ.get("SOURCE", "").lower()
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE",     "500"))
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL", "10"))
DRY_RUN                = os.environ.get("DRY_RUN", "0") == "1"
BAO_ADDR               = os.environ.get("BAO_ADDR", "")
TOPIC_REFRESH_INTERVAL = int(os.environ.get("TOPIC_REFRESH_INTERVAL", "60"))
SIGNAL_PORT            = int(os.environ.get("SIGNAL_PORT", "8090"))
# When the Polaris catalog's STS credential vending is unavailable (e.g. no
# AWS STS endpoint or self-hosted S3), set to "none" to pass S3 keys directly.
# Set to "vended-credentials" if the catalog should vend temporary STS creds.
ICEBERG_ACCESS_DELEGATION = os.environ.get("ICEBERG_ACCESS_DELEGATION", "none")
# Polaris OAuth tokens expire after 3600 s.  Proactively rebuild the catalog
# every CATALOG_REFRESH_INTERVAL seconds so the token never expires mid-run.
# Default 3000 s (50 min) — safely inside the 3600 s Polaris token TTL.
CATALOG_REFRESH_INTERVAL = int(os.environ.get("CATALOG_REFRESH_INTERVAL", "3000"))

_VALID_SOURCES = ("postgres", "oracle", "mongodb")
if SOURCE not in _VALID_SOURCES:
    sys.exit(f"ERROR: SOURCE={SOURCE!r} must be one of {_VALID_SOURCES}")

# Source → Polaris warehouse + topic prefix
_SOURCE_CFG = {
    "postgres": {"warehouse": "pg_lakehouse",  "topic_prefix": "postgres.cache_testing."},
    "oracle":   {"warehouse": "ora_lakehouse", "topic_prefix": "oracle.cache_testing."},
    "mongodb":  {"warehouse": "mgo_lakehouse", "topic_prefix": "mongodb.cache_testing."},
}
WAREHOUSE    = _SOURCE_CFG[SOURCE]["warehouse"]
TOPIC_PREFIX = _SOURCE_CFG[SOURCE]["topic_prefix"]
NAMESPACE    = "cache_testing"

# Consumer group — one per source so each pod tracks its own offsets
CONSUMER_GROUP = f"tableflow-{SOURCE}"


# ── OpenBao credential loader ──────────────────────────────────────────────────
_K8S_JWT  = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_K8S_NS   = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


def _bao_address() -> str:
    if BAO_ADDR:
        return BAO_ADDR
    ns = open(_K8S_NS).read().strip() if os.path.exists(_K8S_NS) else "prod"
    return f"http://openbao.{ns}.svc.cluster.local:8200"


def _bao_token(address: str) -> str:
    if env_tok := os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN"):
        return env_tok
    jwt = open(_K8S_JWT).read().strip()
    body = json.dumps({"role": "platform-secrets-read", "jwt": jwt}).encode()
    req = urllib.request.Request(
        f"{address}/v1/auth/kubernetes/login",
        data=body, headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["auth"]["client_token"]


def _bao_secret(address: str, token: str, path: str) -> dict:
    req = urllib.request.Request(
        f"{address}/v1/{path}",
        headers={"X-Vault-Token": token},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["data"]["data"]


def load_credentials() -> dict:
    addr  = _bao_address()
    token = _bao_token(addr)
    s3    = _bao_secret(addr, token, "secret/data/platform/s3")
    pol   = _bao_secret(addr, token, "secret/data/platform/polaris")
    kfk   = _bao_secret(addr, token, "secret/data/platform/kafka")
    return {"s3": s3, "polaris": pol, "kafka": kfk}


# ── Polaris REST catalog ───────────────────────────────────────────────────────
def make_catalog(creds: dict) -> RestCatalog:
    pol = creds["polaris"]
    s3  = creds["s3"]
    polaris_uri = pol.get("url") or f"http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"
    return RestCatalog(
        name=WAREHOUSE,
        **{
            "uri":            polaris_uri,
            "credential":     f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}",
            "warehouse":      WAREHOUSE,
            "scope":          "PRINCIPAL_ROLE:ALL",
            "s3.access-key-id":     s3["access_key"],
            "s3.secret-access-key": s3["secret_key"],
            "s3.endpoint":          s3["endpoint"],
            "s3.region":            s3.get("region", "us-east-2"),
            "s3.path-style-access": "true",
            "header.X-Iceberg-Access-Delegation": ICEBERG_ACCESS_DELEGATION,
        },
    )


# ── Kafka consumer ─────────────────────────────────────────────────────────────
def make_consumer(creds: dict) -> Consumer:
    kfk = creds["kafka"]
    return Consumer({
        "bootstrap.servers":  kfk["bootstrap"],
        "security.protocol":  "SASL_PLAINTEXT",
        "sasl.mechanism":     "SCRAM-SHA-512",
        "sasl.username":      kfk.get("username", kfk.get("debezium_user", "debezium-user")),
        "sasl.password":      kfk.get("password", kfk.get("debezium_password", "")),
        "group.id":           CONSUMER_GROUP,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,   # manual commit AFTER Iceberg snapshot
        "max.poll.interval.ms": "300000",
        "session.timeout.ms":   "30000",
    })


# ── Schema inference + evolution ───────────────────────────────────────────────
# JSON value type → PyIceberg type
def _infer_type(value: Any) -> Any:
    if isinstance(value, bool):
        return BooleanType()
    if isinstance(value, int):
        return LongType()
    if isinstance(value, float):
        return DoubleType()
    return StringType()


def _json_to_schema(record: dict, field_id_start: int = 1) -> Schema:
    """Infer a PyIceberg Schema from a flat dict (one level deep)."""
    fields = []
    fid = field_id_start
    for key, val in record.items():
        iceberg_type = _infer_type(val)
        fields.append(NestedField(fid, key, iceberg_type, required=False))
        fid += 1
    return Schema(*fields)


def _evolve_schema(
    catalog: RestCatalog,
    table_id: tuple,
    record: dict,
    forbidden: frozenset[str] | None = None,
) -> None:
    """Add any columns present in record but missing from the Iceberg table.

    forbidden — optional set of column names that must never be added (used to
                block CDC metadata columns from being introduced into _standard
                tables even if they somehow appear in a record dict).
    """
    tbl = catalog.load_table(table_id)
    existing = {f.name for f in tbl.schema().fields}
    new_cols  = {
        k: v for k, v in record.items()
        if k not in existing and (forbidden is None or k not in forbidden)
    }
    if not new_cols:
        return
    with tbl.update_schema() as upd:
        for col_name, val in new_cols.items():
            upd.add_column(col_name, _infer_type(val))
    log.info("[%s] Schema evolved: added columns %s", table_id, list(new_cols))


# ── Table name from topic ──────────────────────────────────────────────────────
def topic_to_table(topic: str) -> tuple[str, str]:
    """
    Returns (table_name, mode).
    e.g. 'postgres.cache_testing.customers_standard' → ('customers',      'standard')
         'postgres.cache_testing.customers_sd'        → ('customers',      'sd')
         'postgres.cache_testing.customers_hist'      → ('customers_hist', 'hist')

    _hist keeps its suffix so the Iceberg table is named customers_hist,
    making it obvious which table is the history-tracking table.
    """
    suffix = topic.replace(TOPIC_PREFIX, "")
    if suffix.endswith("_standard"):
        return suffix[:-9], "standard"
    if suffix.endswith("_sd"):
        return suffix[:-3], "sd"
    if suffix.endswith("_hist"):
        return suffix, "hist"          # keep full name e.g. customers_hist
    return suffix, "unknown"


# CDC metadata column names that must NEVER appear in a _standard table.
# These are the ksqlDB-derived wrapper fields — only AFTER_JSON contents
# (the actual source-table columns) belong in the standard replica.
_STANDARD_FORBIDDEN_COLS: frozenset[str] = frozenset({
    "pk_id", "cdc_op", "cdc_ts", "snap_ts",
    "after_json", "before_json", "is_deleted", "deleted_at",
})


# ── Row flattening ─────────────────────────────────────────────────────────────
def flatten_standard(msg: dict, kafka_key: bytes | None = None) -> dict | None:
    """
    Flatten a _standard message into a plain source-table replica row.

    INSERT / UPDATE / snapshot(r):
        Returns the full AFTER_JSON contents (source columns only, no CDC metadata).
    DELETE:
        Returns {"__deleted__": True, "id": <pk>} so write_standard_batch can
        issue a tbl.delete() for that PK.  The PK comes from the Kafka message
        key (ksqlDB PARTITION BY) — same as how _sd handles it — and falls back
        to BEFORE_JSON if the key is missing or unparseable.
    """
    cdc_op = str(msg.get("CDC_OP", "")).lower()

    if cdc_op == "d":
        # Extract PK from Kafka message key first (most reliable), then BEFORE_JSON
        pk = None
        if kafka_key is not None:
            try:
                pk = kafka_key.decode("utf-8")
            except Exception:
                pass
        if pk is None:
            before_json = msg.get("BEFORE_JSON")
            if before_json and before_json != "null":
                try:
                    before = json.loads(before_json) if isinstance(before_json, str) else before_json
                    if isinstance(before, dict):
                        pk = before.get("id") or before.get("ID")
                except Exception:
                    pass
        if pk is None:
            return None
        return {"__deleted__": True, "id": pk}

    after_json = msg.get("AFTER_JSON")
    if not after_json or after_json == "null":
        return None
    try:
        row = json.loads(after_json) if isinstance(after_json, str) else after_json
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(row, dict):
        return None

    # Source columns only — lowercase keys, no CDC metadata.
    return {
        k.lower(): v
        for k, v in row.items()
        if k.lower() not in _STANDARD_FORBIDDEN_COLS
    }


def flatten_sd(msg: dict, kafka_key: bytes | None = None) -> dict | None:
    """
    Flatten a _sd message into an Iceberg row.
    PK_ID is the Kafka message KEY (ksqlDB PARTITION BY promotes it out of
    the JSON value body).
    - INSERT/UPDATE: expand AFTER_JSON fields + is_deleted=false
    - DELETE:        expand BEFORE_JSON fields + is_deleted=true, deleted_at=<ts>
    """
    is_del  = str(msg.get("IS_DELETED", "false")).lower() == "true"
    cdc_ts  = msg.get("CDC_TS", int(time.time() * 1000))

    src_json = msg.get("BEFORE_JSON") if is_del else msg.get("AFTER_JSON")
    if not src_json:
        return None
    try:
        row = json.loads(src_json) if isinstance(src_json, str) else src_json
    except (json.JSONDecodeError, TypeError):
        return None

    # lowercase all keys (Oracle sends uppercase)
    row = {k.lower(): v for k, v in row.items()}
    # pk_id comes from the Kafka message key (ksqlDB PARTITION BY field)
    if kafka_key is not None:
        try:
            row["pk_id"] = kafka_key.decode("utf-8")
        except Exception:
            pass

    row["is_deleted"] = is_del
    row["deleted_at"] = datetime.fromtimestamp(cdc_ts / 1000, tz=timezone.utc).isoformat() if is_del else None
    row["cdc_op"]     = str(msg.get("CDC_OP", "")).lower()
    row["snap_ts"]    = datetime.fromtimestamp(cdc_ts / 1000, tz=timezone.utc).isoformat()
    return row


def flatten_hist(msg: dict, kafka_key: bytes | None = None) -> dict | None:
    """
    Flatten a _hist message into an append-only Iceberg row.
    Expands AFTER_JSON → after_* columns, BEFORE_JSON → before_* columns.

    For DELETE events Debezium sets after=null, so only before_* columns are
    populated.  pk_id comes from the Kafka message key.  The _hist CSAS
    ROWKEY is the full Debezium key JSON (e.g. '{"id":27}'), so we extract
    the first value from it.
    """
    cdc_ts   = msg.get("CDC_TS", int(time.time() * 1000))
    cdc_op   = str(msg.get("CDC_OP", "")).upper()

    after_json  = msg.get("AFTER_JSON")
    before_json = msg.get("BEFORE_JSON")

    row: dict = {
        "_change_type": cdc_op,
        "_change_ts":   datetime.fromtimestamp(cdc_ts / 1000, tz=timezone.utc).isoformat(),
        "snap_ts":      datetime.fromtimestamp(cdc_ts / 1000, tz=timezone.utc).isoformat(),
    }

    # pk_id: Kafka key for _hist is the Debezium row key JSON e.g. b'{"id":27}'
    if kafka_key is not None:
        try:
            key_obj = json.loads(kafka_key.decode("utf-8"))
            if isinstance(key_obj, dict):
                # Take the first (and usually only) value as the PK string
                row["pk_id"] = str(next(iter(key_obj.values())))
            else:
                row["pk_id"] = str(key_obj)
        except Exception:
            try:
                row["pk_id"] = kafka_key.decode("utf-8")
            except Exception:
                pass

    # after_json may be the string "null" (not Python None) for DELETE events
    after_parsed = None
    if after_json and after_json != "null":
        try:
            after_parsed = json.loads(after_json) if isinstance(after_json, str) else after_json
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(after_parsed, dict):
        for k, v in after_parsed.items():
            row[f"after_{k.lower()}"] = v

    before_parsed = None
    if before_json and before_json != "null":
        try:
            before_parsed = json.loads(before_json) if isinstance(before_json, str) else before_json
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(before_parsed, dict):
        for k, v in before_parsed.items():
            row[f"before_{k.lower()}"] = v

    # Must have at least one data column beyond the three metadata fields
    return row if len(row) > 3 else None


# ── Iceberg write ──────────────────────────────────────────────────────────────
def _rows_to_arrow(rows: list[dict]) -> pa.Table:
    """Convert a list of flat dicts to a PyArrow table (union of all keys)."""
    all_keys: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)
    columns = {k: [r.get(k) for r in rows] for k in all_keys}
    return pa.table(columns)


def _iceberg_to_arrow_type(ice_type: Any) -> pa.DataType:
    """Map a PyIceberg field type to the closest PyArrow type for casting."""
    type_name = type(ice_type).__name__
    if type_name in ("LongType", "IntegerType"):
        return pa.int64()
    if type_name == "FloatType":
        return pa.float32()
    if type_name == "DoubleType":
        return pa.float64()
    if type_name == "BooleanType":
        return pa.bool_()
    if type_name in ("TimestampType", "TimestamptzType"):
        return pa.string()   # keep as ISO string — safest cross-version approach
    return pa.string()


def _cast_arrow_to_schema(arrow: pa.Table, tbl: Any) -> pa.Table:
    """
    Cast each column in `arrow` to match the existing Iceberg table schema.
    Columns not in the schema are left as-is (schema evolution will add them).

    Null-safe: rebuilds each column via pa.array(raw_values, type=target)
    which handles Python None correctly without failing on null values.
    Falls back to stringify-then-cast for columns arriving as strings
    (e.g. Kafka JSON delivers id as "42" but Iceberg schema expects LongType).
    """
    ice_types = {f.name: f.field_type for f in tbl.schema().fields}
    for col_name in arrow.schema.names:
        if col_name not in ice_types:
            continue
        target_pa  = _iceberg_to_arrow_type(ice_types[col_name])
        current_pa = arrow.schema.field(col_name).type
        if current_pa == target_pa:
            continue
        col_idx    = arrow.schema.get_field_index(col_name)
        raw_values = arrow.column(col_name).to_pylist()
        try:
            # Direct build — pa.array handles None natively for numeric types
            new_col = pa.array(raw_values, type=target_pa)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            try:
                # Fallback: stringify non-null values then cast ("42" → 42)
                str_col = pa.array(
                    [str(v) if v is not None else None for v in raw_values],
                    type=pa.string(),
                )
                new_col = str_col.cast(target_pa, safe=False)
            except Exception:
                continue  # leave as-is; schema evolution will handle on next pass
        arrow = arrow.set_column(col_idx, col_name, new_col)
    return arrow


# ── Pre-defined base schemas ───────────────────────────────────────────────────
# Iceberg tables are created eagerly at subscription time (before any message
# arrives) using these baseline schemas.  Schema evolution adds any extra
# columns automatically when the first real message is processed.
#
# _standard — plain replica of the source table (id + source columns only)
# _sd        — soft-delete view (source cols + is_deleted / deleted_at)
# _hist      — append-only audit trail (_change_type / after_* / before_*)

def _base_schema_standard() -> Schema:
    # Minimal seed schema — just the PK.  All source columns are added by
    # schema evolution on the first INSERT/UPDATE message.
    return Schema(
        NestedField(1, "id", LongType(), required=False),
    )


def _base_schema_sd() -> Schema:
    return Schema(
        NestedField(1,  "pk_id",      StringType(),    required=False),
        NestedField(2,  "is_deleted", BooleanType(),   required=False),
        NestedField(3,  "deleted_at", StringType(),    required=False),
        NestedField(4,  "cdc_op",     StringType(),    required=False),
        NestedField(5,  "snap_ts",    StringType(),    required=False),
    )


def _base_schema_hist() -> Schema:
    return Schema(
        NestedField(1,  "_change_type", StringType(),  required=False),
        NestedField(2,  "_change_ts",   StringType(),  required=False),
        NestedField(3,  "snap_ts",      StringType(),  required=False),
    )


def ensure_table(catalog: RestCatalog, table_id: tuple, sample_row: dict, mode: str) -> None:
    """
    Create Iceberg table if it doesn't exist.
    Called lazily during flush (sample_row available) — evolves schema from data.
    Pre-creation at subscription time uses ensure_table_eager() below.
    """
    try:
        catalog.load_table(table_id)
        return   # already exists
    except NoSuchTableError:
        pass

    schema = _json_to_schema(sample_row)
    location = f"s3://{os.environ.get('S3_BUCKET','xdatatoiceberg1')}/iceberg/{WAREHOUSE}/{NAMESPACE}/{table_id[1]}"

    props = {
        "pipeline.source":     SOURCE,
        "pipeline.write-mode": mode,
        "pipeline.component":  "tableflow",
    }
    catalog.create_table(table_id, schema=schema, location=location, properties=props)
    log.info("[%s] Created Iceberg table (mode=%s)", table_id, mode)


def ensure_table_eager(catalog: RestCatalog, topic: str) -> None:
    """
    Eagerly create the Iceberg table for a topic immediately at subscription
    time — before any message arrives.  Uses the predefined base schema for
    the mode (_sd or _hist).  Schema evolution will add data columns when the
    first real message is flushed.  Idempotent — no-op if the table exists.
    """
    tbl_name, mode = topic_to_table(topic)
    if mode == "unknown":
        return
    table_id = (NAMESPACE, tbl_name if mode in ("hist", "unknown") else f"{tbl_name}_{mode}")
    try:
        catalog.load_table(table_id)
        log.info("[eager] Iceberg table already exists: %s", table_id)
        return
    except NoSuchTableError:
        pass

    if mode == "standard":
        schema = _base_schema_standard()
    elif mode == "sd":
        schema = _base_schema_sd()
    else:
        schema = _base_schema_hist()
    location = f"s3://{os.environ.get('S3_BUCKET','xdatatoiceberg1')}/iceberg/{WAREHOUSE}/{NAMESPACE}/{table_id[1]}"
    props    = {
        "pipeline.source":     SOURCE,
        "pipeline.write-mode": mode,
        "pipeline.component":  "tableflow",
    }
    try:
        catalog.create_table(table_id, schema=schema, location=location, properties=props)
        log.info("[eager] Created Iceberg table immediately at subscription: %s (mode=%s)", table_id, mode)
    except Exception as exc:
        # Another pod or a race condition — log and continue; table will be
        # created lazily on first flush if this fails.
        log.warning("[eager] Could not pre-create table %s: %s", table_id, exc)


def write_standard_batch(catalog: RestCatalog, table_id: tuple, rows: list[dict]) -> int:
    """Incremental upsert+delete for the _standard Iceberg table.

    Processes the entire batch in memory first — O(batch), never O(table):

      Step 1 — batch-resolve: replay events in arrival order keyed by PK.
               Last event per PK wins within the batch (correct CDC ordering).
               Result: final_upserts (INSERT/UPDATE) + delete_pks (DELETE).

      Step 2 — schema evolution from upsert rows only.

      Step 3 — single tbl.delete() covering ALL affected PKs (upserts + deletes).
               This removes any stale version of every upserted row AND every
               deleted row in one Iceberg operation — position deletes, O(batch).

      Step 4 — single tbl.append() for the resolved upsert rows only.

    Result: exactly one row per PK in the table at all times, matching the
    source table — same as Postgres. Deletes remove the row entirely.
    """
    if not rows:
        return 0

    # ── Step 1: batch-resolve in memory ───────────────────────────────────────
    # Normalise all PKs to int so delete sentinels (string key from Kafka) and
    # upsert rows (int from JSON) key the same dict entry correctly.
    def _to_int(v: Any) -> Any:
        try:
            return int(v)
        except (TypeError, ValueError):
            return v

    state: dict[Any, dict] = {}   # pk (int) → latest row or delete sentinel
    for row in rows:
        raw_pk = row.get("id")
        pk = _to_int(raw_pk) if raw_pk is not None else None
        if pk is not None:
            # Normalise id in the row itself so upsert Arrow cast works cleanly
            row = {**row, "id": pk}
            state[pk] = row
        else:
            state[id(row)] = row   # no PK — pass through as-is

    final_upserts = [r for r in state.values() if not r.get("__deleted__")]
    delete_pks    = [r["id"] for r in state.values()
                     if r.get("__deleted__") and r.get("id") is not None]

    # PKs needing an Iceberg delete: every upserted PK (remove stale version)
    # + every deleted PK (remove entirely). One combined delete call.
    all_affected_pks = list({r["id"] for r in final_upserts if r.get("id") is not None}
                            | set(delete_pks))

    # ── Step 2: schema evolution (upserts only) ───────────────────────────────
    if final_upserts:
        merged_sample = {k: v for row in final_upserts for k, v in row.items() if v is not None}
        _evolve_schema(catalog, table_id, merged_sample, forbidden=_STANDARD_FORBIDDEN_COLS)

    tbl = catalog.load_table(table_id)

    # ── Step 3: delete all old/stale versions of affected PKs ─────────────────
    # PKs are ints; EqualTo/In must receive the correct Python type so PyIceberg
    # builds a valid predicate against the LongType id column.
    if all_affected_pks:
        try:
            expr = (EqualTo("id", all_affected_pks[0])
                    if len(all_affected_pks) == 1
                    else In("id", all_affected_pks))
            tbl.delete(expr)
        except Exception as exc:
            log.error("[%s] delete failed for pks %s: %s", table_id, all_affected_pks, exc)

    # ── Step 4: append the resolved latest state for upserted rows ────────────
    written = 0
    if final_upserts:
        arrow           = _rows_to_arrow(final_upserts)
        arrow           = _cast_arrow_to_schema(arrow, tbl)
        ice_field_names = [f.name for f in tbl.schema().fields]
        ice_cols        = set(ice_field_names)
        for missing in ice_cols - set(arrow.schema.names):
            arrow = arrow.append_column(missing, pa.array([None] * len(arrow), type=pa.string()))
        arrow = arrow.select([n for n in ice_field_names if n in set(arrow.schema.names)])
        tbl.append(arrow)
        written = len(final_upserts)

    log.info("[%s] standard: upserted %d, deleted %d pk(s)",
             table_id, written, len(delete_pks))
    return written


def write_sd_batch(catalog: RestCatalog, table_id: tuple, rows: list[dict]) -> int:
    """Upsert rows into _sd Iceberg table. Returns rows written."""
    if not rows:
        return 0
    _evolve_schema(catalog, table_id, rows[0])
    tbl    = catalog.load_table(table_id)
    arrow  = _rows_to_arrow(rows)
    arrow  = _cast_arrow_to_schema(arrow, tbl)
    # align arrow schema to iceberg schema (add missing cols as null, drop unknown)
    ice_field_names = [f.name for f in tbl.schema().fields]
    ice_cols        = set(ice_field_names)
    arrow_cols      = set(arrow.schema.names)
    for missing in ice_cols - arrow_cols:
        arrow = arrow.append_column(missing, pa.array([None] * len(arrow), type=pa.string()))
    keep  = [n for n in ice_field_names if n in set(arrow.schema.names)]
    arrow = arrow.select(keep)
    tbl.append(arrow)   # append; compaction handles deduplication via PK_ID key
    return len(rows)


def write_hist_batch(catalog: RestCatalog, table_id: tuple, rows: list[dict]) -> int:
    """Append rows into _hist Iceberg table. Returns rows written."""
    if not rows:
        return 0
    # Evolve schema from the union of ALL rows — DELETE rows carry before_*
    # columns that INSERT rows don't have.  Using only rows[0] would silently
    # drop before_* columns from DELETE events.
    merged = {k: v for row in rows for k, v in row.items() if v is not None}
    _evolve_schema(catalog, table_id, merged)
    # Reload table AFTER evolution so ice_field_names includes the new columns
    tbl             = catalog.load_table(table_id)
    arrow           = _rows_to_arrow(rows)
    arrow           = _cast_arrow_to_schema(arrow, tbl)
    ice_field_names = [f.name for f in tbl.schema().fields]
    ice_cols        = set(ice_field_names)
    arrow_cols      = set(arrow.schema.names)
    for missing in ice_cols - arrow_cols:
        arrow = arrow.append_column(missing, pa.array([None] * len(arrow), type=pa.string()))
    keep  = [n for n in ice_field_names if n in set(arrow.schema.names)]
    arrow = arrow.select(keep)
    tbl.append(arrow)
    return len(rows)


# ── HTTP signal server ─────────────────────────────────────────────────────────
# Listens on SIGNAL_PORT (default 8090).
# POST /refresh-topics  → sets _refresh_now event so the main loop immediately
#                         re-discovers Kafka topics and re-subscribes.
# GET  /healthz         → returns 200 OK (used by liveness probe).
# The server runs in a daemon thread — it dies automatically when the main
# process exits.  Thread-safety: only a threading.Event is shared; no locks
# needed because Consumer.subscribe() is called only from the main thread
# (the event is consumed there, not in the handler thread).

class _SignalHandler(BaseHTTPRequestHandler):
    refresh_event: threading.Event   # set by TableFlow.__init__

    def log_message(self, fmt: str, *args: Any) -> None:  # silence default access log
        pass

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self) -> None:
        if self.path == "/refresh-topics":
            _SignalHandler.refresh_event.set()
            log.info("[signal] /refresh-topics received — triggering immediate topic re-discovery")
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
        else:
            self.send_response(404)
            self.end_headers()


def _start_signal_server(refresh_event: threading.Event) -> None:
    """Start the HTTP signal server in a background daemon thread."""
    _SignalHandler.refresh_event = refresh_event
    srv = HTTPServer(("0.0.0.0", SIGNAL_PORT), _SignalHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True, name="signal-server")
    t.start()
    log.info("Signal server listening on port %d", SIGNAL_PORT)


# ── Main loop ──────────────────────────────────────────────────────────────────
class TableFlow:
    def __init__(self) -> None:
        log.info("TableFlow starting — source=%s warehouse=%s", SOURCE, WAREHOUSE)
        self._running = True
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT,  self._stop)

        # Event set by the HTTP signal server when /refresh-topics is called.
        # Checked in the run loop for immediate re-subscribe without waiting
        # for the TOPIC_REFRESH_INTERVAL timer to expire.
        self._refresh_now = threading.Event()
        _start_signal_server(self._refresh_now)

        creds         = load_credentials()
        self.catalog  = make_catalog(creds)
        self.consumer = make_consumer(creds)
        self._creds   = creds   # kept for admin client re-use

        # Initial dynamic topic discovery from Kafka metadata
        topics = self._discover_topics_from_kafka()
        log.info("Subscribing to %d topics on startup: %s", len(topics), topics)
        self._subscribed: set[str] = set(topics)
        self.consumer.subscribe(list(self._subscribed))

        # Eagerly create Iceberg tables for all subscribed topics now —
        # no need to wait for the first message to arrive.
        for topic in topics:
            ensure_table_eager(self.catalog, topic)

        # Buffer: topic → list[dict]
        self._buffers:  dict[str, list[dict]] = {}
        self._offsets:  dict[tuple, int] = {}   # (topic, partition) → offset
        self._last_flush = time.monotonic()
        self._last_topic_refresh   = time.monotonic()
        self._last_catalog_refresh = time.monotonic()

    def _stop(self, *_: Any) -> None:
        log.info("Shutdown signal received.")
        self._running = False

    def _rebuild_catalog(self, reason: str = "scheduled") -> None:
        """
        Re-authenticate with Polaris by constructing a brand-new RestCatalog.

        PyIceberg's RestCatalog fetches an OAuth2 token during __init__ and
        caches it internally for the lifetime of the object.  Polaris tokens
        expire after 3600 s.  Rather than patching PyIceberg internals, we
        simply replace self.catalog with a fresh instance — this forces a new
        OAuth handshake and a fresh token with zero downtime.

        Called in two situations:
          1. Proactively every CATALOG_REFRESH_INTERVAL seconds (default 3000 s)
             so the token never silently expires between flushes.
          2. Reactively when _flush() catches a RESTError 401 — the bad catalog
             is swapped out immediately so the next retry succeeds.
        """
        log.info("[catalog] Rebuilding RestCatalog (reason=%s) …", reason)
        try:
            creds = load_credentials()
            self.catalog  = make_catalog(creds)
            self._creds   = creds
            self._last_catalog_refresh = time.monotonic()
            log.info("[catalog] RestCatalog rebuilt successfully — fresh token obtained.")
        except Exception as exc:
            log.error("[catalog] Rebuild failed: %s — will retry on next cycle.", exc)

    def _refresh_catalog_if_needed(self) -> None:
        """Proactively rebuild the catalog before the Polaris token expires."""
        if time.monotonic() - self._last_catalog_refresh >= CATALOG_REFRESH_INTERVAL:
            self._rebuild_catalog(reason="scheduled")

    def _discover_topics_from_kafka(self) -> list[str]:
        """
        Query Kafka broker metadata and return all *_standard / *_sd / *_hist
        topics whose name starts with TOPIC_PREFIX.  This means any new table
        added to the source database — and captured by Debezium + ksqlDB — is
        picked up automatically without restarting the pod.
        """
        from confluent_kafka import Consumer as _AdminConsumer
        kfk = self._creds["kafka"] if hasattr(self, "_creds") else load_credentials()["kafka"]
        tmp = _AdminConsumer({
            "bootstrap.servers":  kfk["bootstrap"],
            "security.protocol":  "SASL_PLAINTEXT",
            "sasl.mechanism":     "SCRAM-SHA-512",
            "sasl.username":      kfk.get("username", kfk.get("debezium_user", "debezium-user")),
            "sasl.password":      kfk.get("password", kfk.get("debezium_password", "")),
            "group.id":           f"{CONSUMER_GROUP}-discovery",
        })
        try:
            metadata = tmp.list_topics(timeout=15)
            all_topics = list(metadata.topics.keys())
        finally:
            tmp.close()

        matched = sorted([
            t for t in all_topics
            if t.startswith(TOPIC_PREFIX) and (
                t.endswith("_standard") or t.endswith("_sd") or t.endswith("_hist")
            )
        ])
        log.info("Kafka discovery found %d matching topics for prefix %r: %s",
                 len(matched), TOPIC_PREFIX, matched)
        return matched

    def _refresh_topics_if_needed(self) -> None:
        """
        Re-query Kafka for new _standard/_sd/_hist topics and re-subscribe if any are found.
        Fires in two cases:
          1. TOPIC_REFRESH_INTERVAL seconds have elapsed (periodic safety net).
          2. _refresh_now event is set by POST /refresh-topics from the Lakehouse
             UI provision endpoint — triggers immediately when a new ksqlDB stream
             is created so there is zero wait regardless of the timer interval.
        Called from the main run loop — no separate thread required.
        """
        forced = self._refresh_now.is_set()
        if not forced and time.monotonic() - self._last_topic_refresh < TOPIC_REFRESH_INTERVAL:
            return
        self._refresh_now.clear()   # consume the event before doing the work
        self._last_topic_refresh = time.monotonic()

        try:
            current = set(self._discover_topics_from_kafka())
        except Exception as exc:
            log.warning("Topic refresh failed (will retry): %s", exc)
            return

        new_topics = current - self._subscribed
        if not new_topics:
            return

        log.info("New topics detected — re-subscribing to include: %s", sorted(new_topics))
        self._subscribed = current
        self.consumer.subscribe(list(self._subscribed))

        # Eagerly create Iceberg tables for newly discovered topics immediately —
        # tables are ready in Polaris/Doris before the first CDC message arrives.
        for topic in new_topics:
            ensure_table_eager(self.catalog, topic)

    def _flush(self, topic: str, force: bool = False) -> None:
        rows = self._buffers.get(topic, [])
        if not rows:
            return
        if not force and len(rows) < BATCH_SIZE:
            return

        tbl_name, mode = topic_to_table(topic)
        table_id = (NAMESPACE, tbl_name if mode in ("hist", "unknown") else f"{tbl_name}_{mode}")
        # _hist → customers_hist,  _sd → customers_sd,  _standard → customers_standard

        if DRY_RUN:
            log.info("[DRY_RUN] %s mode=%s rows=%d — not writing", table_id, mode, len(rows))
            self._buffers[topic] = []
            return

        try:
            # For standard mode, delete sentinels carry no source columns —
            # use the first upsert row for schema inference; fall back to rows[0]
            # for other modes (they never contain delete sentinels).
            sample = next((r for r in rows if not r.get("__deleted__")), rows[0])
            ensure_table(self.catalog, table_id, sample, mode)
            if mode == "standard":
                written = write_standard_batch(self.catalog, table_id, rows)
            elif mode == "sd":
                written = write_sd_batch(self.catalog, table_id, rows)
            else:
                written = write_hist_batch(self.catalog, table_id, rows)
            log.info("[%s/%s] flushed %d rows → Iceberg snapshot committed", SOURCE, table_id, written)
        except Exception as exc:
            err_str = str(exc)
            # ── 401 Unauthorized: Polaris token has expired ───────────────────
            # Rebuild the catalog immediately so the next flush cycle succeeds
            # without waiting for the proactive CATALOG_REFRESH_INTERVAL timer.
            if "401" in err_str:
                log.warning("[%s/%s] 401 detected — rebuilding catalog for fresh token …",
                            SOURCE, table_id)
                self._rebuild_catalog(reason="401-reactive")
            else:
                log.error("[%s/%s] write failed: %s — will retry on next batch", SOURCE, table_id, exc)
            return   # do NOT clear buffer or commit offsets on failure

        self._buffers[topic] = []

    def _flush_all(self, force: bool = False) -> None:
        for topic in list(self._buffers):
            self._flush(topic, force=force)

    def _commit(self, msg: Any) -> None:
        """Commit Kafka offset for this message."""
        self.consumer.commit(message=msg, asynchronous=False)

    def run(self) -> None:
        try:
            self._run_loop()
        finally:
            log.info("Flushing remaining buffers before shutdown…")
            self._flush_all(force=True)
            self.consumer.close()
            log.info("TableFlow stopped.")

    def _run_loop(self) -> None:
        last_msg: Any = None

        while self._running:
            msg = self.consumer.poll(timeout=1.0)

            # Time-based flush even when no messages
            if time.monotonic() - self._last_flush >= FLUSH_INTERVAL:
                self._flush_all(force=True)
                if last_msg is not None:
                    self._commit(last_msg)
                    last_msg = None
                self._last_flush = time.monotonic()
                # Heartbeat — readiness/liveness probes check this file
                try:
                    open("/tmp/tableflow.alive", "w").close()
                except Exception:
                    pass

            # Proactive catalog token refresh — before Polaris token expires
            self._refresh_catalog_if_needed()

            # Periodic new-table discovery — re-subscribe if new topics appeared
            self._refresh_topics_if_needed()

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            topic  = msg.topic()
            value  = msg.value()
            if value is None:
                continue   # tombstone — skip

            try:
                raw = json.loads(value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                log.warning("Could not parse message on %s: %s", topic, exc)
                continue

            kafka_key = msg.key()   # bytes or None — holds PK_ID from PARTITION BY
            _, mode = topic_to_table(topic)
            if mode == "standard":
                row = flatten_standard(raw, kafka_key)
            elif mode == "sd":
                row = flatten_sd(raw, kafka_key)
            elif mode == "hist":
                row = flatten_hist(raw, kafka_key)
            else:
                continue

            if row is None:
                continue

            self._buffers.setdefault(topic, []).append(row)
            last_msg = msg

            # Size-based flush
            if len(self._buffers[topic]) >= BATCH_SIZE:
                self._flush(topic, force=True)
                self._commit(msg)
                last_msg = None
                self._last_flush = time.monotonic()


if __name__ == "__main__":
    TableFlow().run()
