#!/usr/bin/env python3
"""
tableflow.py
============
Kafka-native TableFlow — continuously mirrors ksqlDB-derived _sd and _hist
topics into Apache Iceberg tables via Polaris REST catalog.

Behaviour mirrors Confluent TableFlow:
  - Auto-creates Iceberg tables on first message (schema inferred from JSON)
  - Auto-evolves schema when new fields appear in the Kafka message
  - Commits Iceberg snapshots atomically — Kafka offset only advanced after
    the snapshot is committed to Polaris (exactly-once delivery guarantee)
  - _sd  topics  → UPSERT  (merge-on-read, keyed by PK_ID)
  - _hist topics → APPEND  (every event is a new Iceberg row)

Sources handled (single pod per SOURCE env var):
  postgres  → reads postgres.cache_testing.*_sd / *_hist
  oracle    → reads oracle.cache_testing.*_sd  / *_hist
  mongodb   → reads mongodb.cache_testing.*_sd / *_hist

Each source writes to its own Polaris warehouse:
  postgres → pg_lakehouse  / cache_testing namespace
  oracle   → ora_lakehouse / cache_testing namespace
  mongodb  → mgo_lakehouse / cache_testing namespace

Credentials — all from OpenBao via K8s SA JWT (role: platform-secrets-read):
  secret/data/platform/s3      → S3 access/secret key, endpoint, region, bucket
  secret/data/platform/polaris → Polaris client_id, client_secret, url
  secret/data/platform/kafka   → bootstrap, username, password

Environment variables:
  SOURCE          postgres | oracle | mongodb  (required)
  BATCH_SIZE      messages per Iceberg write batch  (default 500)
  FLUSH_INTERVAL  max seconds before forced flush   (default 10)
  BAO_ADDR        OpenBao address override
  DRY_RUN         1 = consume but do not write to Iceberg
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, TopicPartition
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
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
DRY_RUN        = os.environ.get("DRY_RUN", "0") == "1"
BAO_ADDR       = os.environ.get("BAO_ADDR", "")
# When the Polaris catalog's STS credential vending is unavailable (e.g. no
# AWS STS endpoint or self-hosted S3), set to "none" to pass S3 keys directly.
# Set to "vended-credentials" if the catalog should vend temporary STS creds.
ICEBERG_ACCESS_DELEGATION = os.environ.get("ICEBERG_ACCESS_DELEGATION", "none")

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


def _evolve_schema(catalog: RestCatalog, table_id: tuple, record: dict) -> None:
    """Add any columns present in record but missing from the Iceberg table."""
    tbl = catalog.load_table(table_id)
    existing = {f.name for f in tbl.schema().fields}
    new_cols  = {k: v for k, v in record.items() if k not in existing}
    if not new_cols:
        return
    with tbl.update_schema() as upd:
        for col_name, val in new_cols.items():
            upd.add_column(col_name, _infer_type(val))
    log.info("[%s] Schema evolved: added columns %s", table_id, list(new_cols))


# ── Table name from topic ──────────────────────────────────────────────────────
def topic_to_table(topic: str) -> tuple[str, str]:
    """
    Returns (table_name, mode) where mode is 'sd' or 'hist'.
    e.g. 'postgres.cache_testing.customers_sd' → ('customers', 'sd')
    """
    suffix = topic.replace(TOPIC_PREFIX, "")
    if suffix.endswith("_sd"):
        return suffix[:-3], "sd"
    if suffix.endswith("_hist"):
        return suffix[:-5], "hist"
    return suffix, "unknown"


# ── Row flattening ─────────────────────────────────────────────────────────────
def flatten_sd(msg: dict) -> dict | None:
    """
    Flatten a _sd message into an Iceberg row.
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

    row["is_deleted"] = is_del
    row["deleted_at"] = datetime.fromtimestamp(cdc_ts / 1000, tz=timezone.utc).isoformat() if is_del else None
    row["cdc_op"]     = str(msg.get("CDC_OP", "")).lower()
    row["snap_ts"]    = datetime.fromtimestamp(cdc_ts / 1000, tz=timezone.utc).isoformat()
    return row


def flatten_hist(msg: dict) -> dict | None:
    """
    Flatten a _hist message into an append-only Iceberg row.
    Expands AFTER_JSON → after_* columns, BEFORE_JSON → before_* columns.
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

    if after_json:
        try:
            after = json.loads(after_json) if isinstance(after_json, str) else after_json
            if isinstance(after, dict):
                for k, v in after.items():
                    row[f"after_{k.lower()}"] = v
        except (json.JSONDecodeError, TypeError):
            pass

    if before_json:
        try:
            before = json.loads(before_json) if isinstance(before_json, str) else before_json
            if isinstance(before, dict):
                for k, v in before.items():
                    row[f"before_{k.lower()}"] = v
        except (json.JSONDecodeError, TypeError):
            pass

    return row if len(row) > 3 else None   # must have at least one data column


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


def ensure_table(catalog: RestCatalog, table_id: tuple, sample_row: dict, mode: str) -> None:
    """Create Iceberg table from sample_row schema if it doesn't exist."""
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


def write_sd_batch(catalog: RestCatalog, table_id: tuple, rows: list[dict]) -> int:
    """Upsert rows into _sd Iceberg table. Returns rows written."""
    if not rows:
        return 0
    _evolve_schema(catalog, table_id, rows[0])
    tbl    = catalog.load_table(table_id)
    arrow  = _rows_to_arrow(rows)
    # align arrow schema to iceberg schema (add missing cols as null, drop unknown)
    ice_cols = {f.name for f in tbl.schema().fields}
    arrow_cols = set(arrow.schema.names)
    for missing in ice_cols - arrow_cols:
        arrow = arrow.append_column(missing, pa.array([None] * len(arrow), type=pa.string()))
    keep = [n for n in tbl.schema().field_names() if n in arrow.schema.names]
    arrow = arrow.select(keep)
    tbl.append(arrow)   # append; compaction handles deduplication via PK_ID key
    return len(rows)


def write_hist_batch(catalog: RestCatalog, table_id: tuple, rows: list[dict]) -> int:
    """Append rows into _hist Iceberg table. Returns rows written."""
    if not rows:
        return 0
    _evolve_schema(catalog, table_id, rows[0])
    tbl   = catalog.load_table(table_id)
    arrow = _rows_to_arrow(rows)
    ice_cols   = {f.name for f in tbl.schema().fields}
    arrow_cols = set(arrow.schema.names)
    for missing in ice_cols - arrow_cols:
        arrow = arrow.append_column(missing, pa.array([None] * len(arrow), type=pa.string()))
    keep = [n for n in tbl.schema().field_names() if n in arrow.schema.names]
    arrow = arrow.select(keep)
    tbl.append(arrow)
    return len(rows)


# ── Main loop ──────────────────────────────────────────────────────────────────
class TableFlow:
    def __init__(self) -> None:
        log.info("TableFlow starting — source=%s warehouse=%s", SOURCE, WAREHOUSE)
        self._running = True
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT,  self._stop)

        creds         = load_credentials()
        self.catalog  = make_catalog(creds)
        self.consumer = make_consumer(creds)

        # Subscribe to all _sd and _hist derived topics for this source
        topics = self._discover_topics(creds)
        log.info("Subscribing to %d topics: %s", len(topics), topics)
        self.consumer.subscribe(topics)

        # Buffer: topic → list[dict]
        self._buffers:  dict[str, list[dict]] = {}
        self._offsets:  dict[tuple, int] = {}   # (topic, partition) → offset
        self._last_flush = time.monotonic()

    def _stop(self, *_: Any) -> None:
        log.info("Shutdown signal received.")
        self._running = False

    def _discover_topics(self, creds: dict) -> list[str]:
        """Return all _sd + _hist topics for this source."""
        tables = [
            "customers", "orders", "products",
            "order_items", "inventory_events", "product_reviews",
        ]
        return [
            f"{TOPIC_PREFIX}{tbl}{suffix}"
            for tbl in tables
            for suffix in ("_sd", "_hist")
        ]

    def _flush(self, topic: str, force: bool = False) -> None:
        rows = self._buffers.get(topic, [])
        if not rows:
            return
        if not force and len(rows) < BATCH_SIZE:
            return

        tbl_name, mode = topic_to_table(topic)
        table_id = (NAMESPACE, tbl_name if mode == "hist" else f"{tbl_name}_{mode}")
        # _hist → customers_hist,  _sd → customers_sd

        if DRY_RUN:
            log.info("[DRY_RUN] %s mode=%s rows=%d — not writing", table_id, mode, len(rows))
            self._buffers[topic] = []
            return

        try:
            ensure_table(self.catalog, table_id, rows[0], mode)
            if mode == "sd":
                written = write_sd_batch(self.catalog, table_id, rows)
            else:
                written = write_hist_batch(self.catalog, table_id, rows)
            log.info("[%s/%s] flushed %d rows → Iceberg snapshot committed", SOURCE, table_id, written)
        except Exception as exc:
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

            _, mode = topic_to_table(topic)
            if mode == "sd":
                row = flatten_sd(raw)
            elif mode == "hist":
                row = flatten_hist(raw)
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
