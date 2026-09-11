"""
doris_write_proxy.py
====================
Transparent MySQL protocol proxy for Doris write-pushdown to Apache Spark.

Architecture
------------
Clients connect to this proxy on port 9040 using the standard MySQL protocol.
The proxy handles the MySQL handshake and then forwards every statement to
the real Doris FE.  For most statements (SELECT, DDL, local DML) the response
from Doris is forwarded directly back to the client unchanged.

The one exception: DML statements (INSERT / UPDATE / DELETE / MERGE / INSERT
OVERWRITE) that target a managed external Iceberg catalog (polaris, databricks,
postgres, oracle, mongodb).  For those:

  1. The proxy detects the catalog name in the SQL *before* sending to Doris.
  2. The statement is executed directly via a persistent in-process SparkSession.
  3. On SUCCESS  → returns a MySQL OK packet to the client (rows affected = 0).
  4. On FAILURE  → returns a MySQL ERR packet with the Spark error message.

Performance model
-----------------
The SparkSession (JVM, Gluten/Velox, executors, Polaris OAuth) is initialised
ONCE at proxy startup and reused for every subsequent INSERT.  This eliminates:
  - JVM cold start          (~2 s per call with spark-submit)
  - Gluten native lib init  (~2 s per call)
  - Executor launch         (~3–8 s per call)
  - Polaris OAuth roundtrip (~2 s per call, token cached)
  - py4j gateway start      (~1 s per call)
After the first call (warm-up ~10–15 s), each INSERT takes <3 s.

Environment variables
---------------------
  DORIS_HOST          Doris FE host         (default: 127.0.0.1)
  DORIS_PORT          Doris FE MySQL port   (default: 9030)
  LISTEN_HOST         Bind address          (default: 0.0.0.0)
  LISTEN_PORT         Proxy listen port     (default: 9040)
  SPARK_MASTER_URL    spark:// master URL   (default: spark://spark-master-internal.prod.svc.cluster.local:17077)
  SPARK_SQL_TIMEOUT_S Per-statement timeout (default: 300)
  ADDR / BAO_ADDR     OpenBao address       (default: http://openbao.prod.svc.cluster.local:8200)
  SPARK_INIT_TIMEOUT_S Seconds to wait for initial SparkSession startup (default: 120)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import struct
import sys
import threading
import time
import urllib.request
from typing import Optional, Tuple

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s doris-write-proxy — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("doris-write-proxy")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
DORIS_HOST          = os.environ.get("DORIS_HOST",   "127.0.0.1")
DORIS_PORT          = int(os.environ.get("DORIS_PORT",  "9030"))
LISTEN_HOST         = os.environ.get("LISTEN_HOST",  "0.0.0.0")
LISTEN_PORT         = int(os.environ.get("LISTEN_PORT", "9040"))

SPARK_MASTER_URL    = os.environ.get(
    "SPARK_MASTER_URL",
    "spark://spark-master-internal.prod.svc.cluster.local:17077",
)
SPARK_SQL_TIMEOUT_S   = int(os.environ.get("SPARK_SQL_TIMEOUT_S",   "300"))
SPARK_INIT_TIMEOUT_S  = int(os.environ.get("SPARK_INIT_TIMEOUT_S",  "120"))
# How long (seconds) to cache a table's Iceberg schema before re-fetching.
# Increase for stable schemas; lower if DDL changes must propagate faster.
SCHEMA_CACHE_TTL_S    = int(os.environ.get("SCHEMA_CACHE_TTL_S",    "3600"))

BAO_ADDR            = os.environ.get("ADDR") or os.environ.get("BAO_ADDR",
    "http://openbao.prod.svc.cluster.local:8200")
_BAO_K8S_SA_JWT     = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_BAO_ROLE           = os.environ.get("BAO_ROLE", "platform-secrets-read")
_PATH_POLARIS       = "secret/data/platform/polaris"
_PATH_S3            = "secret/data/platform/s3"
_POLARIS_URI        = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"

# JAR names baked into the image at /opt/spark-jars/
_LOCAL_JARS_DIR = "/opt/spark-jars"
_JAR_NAMES = [
    "gluten-velox-bundle-spark3.5_2.12-centos_7_x86_64-1.2.0.jar",
    "iceberg-spark-runtime-3.5_2.12-1.9.2.jar",
    "iceberg-aws-bundle-1.9.2.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
]

# ─────────────────────────────────────────────────────────────────────────────
# Managed catalogs + warehouse mapping
# ─────────────────────────────────────────────────────────────────────────────
MANAGED_CATALOGS: dict[str, str] = {
    "polaris":    "IcebergCatalog",
    "databricks": "star_lakehouse",
    "postgres":   "pg_lakehouse",
    "oracle":     "ora_lakehouse",
    "mongodb":    "mgo_lakehouse",
}

# ─────────────────────────────────────────────────────────────────────────────
# OpenBao helpers (stdlib only)
# ─────────────────────────────────────────────────────────────────────────────
def _bao_token() -> str:
    if tok := (os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN")):
        return tok
    with open(_BAO_K8S_SA_JWT) as fh:
        jwt = fh.read().strip()
    payload = json.dumps({"role": _BAO_ROLE, "jwt": jwt}).encode()
    req = urllib.request.Request(
        f"{BAO_ADDR}/v1/auth/kubernetes/login",
        data=payload, headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())["auth"]["client_token"]


def _read_secret(path: str, token: str) -> dict:
    req = urllib.request.Request(
        f"{BAO_ADDR}/v1/{path}",
        headers={"X-Vault-Token": token}, method="GET",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    outer = data.get("data", {})
    return outer.get("data", outer)


# ─────────────────────────────────────────────────────────────────────────────
# Persistent SparkSession — initialised once, shared across all INSERT calls
# ─────────────────────────────────────────────────────────────────────────────
# PYSPARK_SUBMIT_ARGS must be set before pyspark is imported so the JVM is
# launched with the correct --driver-class-path containing all required JARs.
# This must happen at module load time, before any `from pyspark import ...`.
_DRIVER_CP   = ":".join(f"{_LOCAL_JARS_DIR}/{j}" for j in _JAR_NAMES)
_EXECUTOR_CP = ":".join(f"/opt/spark/jars/{j}" for j in _JAR_NAMES)

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    f"--driver-class-path {_DRIVER_CP} "
    f"--conf spark.executor.extraClassPath={_EXECUTOR_CP} "
    "pyspark-shell"
)
os.environ.setdefault("PYSPARK_PYTHON", "python3")

from pyspark.sql import SparkSession  # noqa: E402 — must follow PYSPARK_SUBMIT_ARGS
from pyspark import SparkConf         # noqa: E402

# IcebergTableBuilder lives in /app/spark_iceberg_utils.py (copied from Spark image)
sys.path.insert(0, "/app")
from spark_iceberg_utils import IcebergTableBuilder  # noqa: E402


def _build_spark_conf(pol: dict, s3: dict) -> SparkConf:
    """
    Build a SparkConf that is wired for ALL managed catalogs simultaneously.
    Called once at startup; the resulting SparkSession is reused forever.
    """
    conf = SparkConf()
    conf.setAppName("doris-write-proxy-persistent")
    conf.setMaster(SPARK_MASTER_URL)

    # ── Resource sizing ──────────────────────────────────────────────────────
    conf.set("spark.driver.memory",   "4g")
    conf.set("spark.executor.memory", "3g")
    conf.set("spark.executor.cores",  "2")
    conf.set("spark.cores.max",       "8")   # 4 executors × 2 cores maximum

    # ── Dynamic allocation ────────────────────────────────────────────────────
    # Keep 1 executor warm at all times (minExecutors=1, initialExecutors=1)
    # so that every INSERT after the first runs immediately without waiting for
    # executor re-acquisition (~5-15s cold launch from the Spark workers).
    # The warm executor holds only 3g RAM on one worker — acceptable cost for
    # sub-second subsequent INSERTs.  Scale up to 4 executors under load, then
    # scale back to 1 after 60s idle (not 0 — avoids the cold-start penalty).
    conf.set("spark.dynamicAllocation.enabled",                    "true")
    conf.set("spark.dynamicAllocation.shuffleTracking.enabled",    "true")  # no ext shuffle svc needed
    conf.set("spark.dynamicAllocation.minExecutors",               "1")     # always keep 1 warm
    conf.set("spark.dynamicAllocation.maxExecutors",               "4")     # cap at 4
    conf.set("spark.dynamicAllocation.initialExecutors",           "1")     # start with 1 immediately
    conf.set("spark.dynamicAllocation.executorIdleTimeout",        "120s")  # scale back to min after 2min idle
    conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout",  "300s")  # hold cached data 5min

    # ── Gluten + Velox native execution ─────────────────────────────────────
    conf.set("spark.plugins",                         "org.apache.gluten.GlutenPlugin")
    conf.set("spark.gluten.sql.columnar.backend.lib", "velox")
    conf.set("spark.memory.offHeap.enabled",          "true")
    conf.set("spark.memory.offHeap.size",             "2g")

    # ── Executor heartbeat / network ─────────────────────────────────────────
    conf.set("spark.executor.heartbeatInterval",        "10s")
    conf.set("spark.network.timeout",                   "120s")
    conf.set("spark.storage.blockManagerSlaveTimeoutMs","120000")

    # ── S3A fast upload ───────────────────────────────────────────────────────
    conf.set("spark.hadoop.fs.s3a.fast.upload",        "true")
    conf.set("spark.hadoop.fs.s3a.multipart.size",     "67108864")
    conf.set("spark.hadoop.fs.s3a.threads.max",        "20")
    conf.set("spark.hadoop.fs.s3a.connection.maximum", "50")

    # ── SQL extensions (Iceberg) ──────────────────────────────────────────────
    conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    # ── Adaptive query execution ──────────────────────────────────────────────
    # AQE lets Spark replan mid-execution and coalesce small output files.
    conf.set("spark.sql.adaptive.enabled",                    "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # ── Wire every managed catalog ────────────────────────────────────────────
    polaris_uri = pol.get("url") or _POLARIS_URI
    credential  = f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}"

    for cat, warehouse in MANAGED_CATALOGS.items():
        conf.set(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        conf.set(f"spark.sql.catalog.{cat}.type",              "rest")
        conf.set(f"spark.sql.catalog.{cat}.uri",               polaris_uri)
        conf.set(f"spark.sql.catalog.{cat}.oauth2-server-uri", f"{polaris_uri}/v1/oauth/tokens")
        conf.set(f"spark.sql.catalog.{cat}.credential",        credential)
        conf.set(f"spark.sql.catalog.{cat}.scope",             "PRINCIPAL_ROLE:ALL")
        conf.set(f"spark.sql.catalog.{cat}.warehouse",         warehouse)
        conf.set(f"spark.sql.catalog.{cat}.rest.auth.type",    "oauth2")
        # S3FileIO credentials per-catalog
        conf.set(f"spark.sql.catalog.{cat}.s3.access-key-id",     s3["access_key"])
        conf.set(f"spark.sql.catalog.{cat}.s3.secret-access-key", s3["secret_key"])
        conf.set(f"spark.sql.catalog.{cat}.s3.endpoint",          s3["endpoint"])
        conf.set(f"spark.sql.catalog.{cat}.s3.path-style-access", "true")
        conf.set(f"spark.sql.catalog.{cat}.client.region",        s3["region"])

    # ── Shared S3A credentials (hadoop fs layer) ──────────────────────────────
    conf.set("spark.hadoop.fs.s3a.access.key",             s3["access_key"])
    conf.set("spark.hadoop.fs.s3a.secret.key",             s3["secret_key"])
    conf.set("spark.hadoop.fs.s3a.endpoint",               s3["endpoint"])
    conf.set("spark.hadoop.fs.s3a.endpoint.region",        s3["region"])
    conf.set("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.path.style.access",      "true")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")

    # ── Iceberg write defaults ────────────────────────────────────────────────
    conf.set("spark.sql.iceberg.write.format.default",       "parquet")
    conf.set("spark.sql.iceberg.target-file-size-bytes",     "134217728")  # 128 MB
    conf.set("spark.sql.iceberg.aggregate-pushdown.enabled", "true")

    # ── Misc ──────────────────────────────────────────────────────────────────
    conf.set("spark.serializer",                "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "false")
    # Suppress verbose Spark logs from the driver — keep proxy logs readable
    conf.set("spark.driver.extraJavaOptions",
             "-Dlog4j2.configurationFile="
             f"{_LOCAL_JARS_DIR}/../spark-log4j2.properties "
             "-Dsun.reflect.inflationThreshold=2147483647")  # suppress sun.reflect warning

    return conf


class _SparkManager:
    """
    Manages a single persistent SparkSession for the lifetime of the proxy process.

    Initialisation runs in a background thread at startup so the proxy starts
    accepting MySQL connections immediately; the first INSERT that arrives while
    Spark is still warming up will block (at most SPARK_INIT_TIMEOUT_S) then
    proceed.  All subsequent INSERTs are <3 s on a warm session.

    Thread safety: a reentrant lock serialises concurrent INSERTs.  asyncio
    callers must run _execute() via loop.run_in_executor() to avoid blocking
    the event loop.
    """

    def __init__(self) -> None:
        self._spark: Optional[SparkSession] = None
        self._error: Optional[str] = None
        self._lock  = threading.Lock()
        self._ready = threading.Event()
        # Schema cache: fqn → (StructType, fetched_at_epoch)
        # Protected by _lock (same lock that serialises writes).
        self._schema_cache: dict[str, tuple] = {}
        # Builder cache: user → IcebergTableBuilder
        # Avoids re-instantiating on every write for the same Doris user.
        self._builder_cache: dict[str, IcebergTableBuilder] = {}

    # ── Public ─────────────────────────────────────────────────────────────

    def start_background_init(self) -> None:
        """Kick off SparkSession init in a daemon thread. Returns immediately."""
        t = threading.Thread(target=self._init_spark, daemon=True, name="spark-init")
        t.start()

    def execute(self, catalog: str, db: str, table: str, stmt: str, user: str = "") -> Tuple[bool, str]:
        """
        Execute a single DML statement in the persistent SparkSession.
        Blocks until Spark is ready (at most SPARK_INIT_TIMEOUT_S seconds).
        Returns (success, message).

        INSERT INTO … VALUES rows are extracted into a Spark DataFrame and
        written via IcebergTableBuilder.write_append(), which injects
        snap_id (monotonically_increasing_id) and snap_timestamp
        (current_timestamp) automatically.  Callers never supply those columns.

        All other DML (UPDATE, DELETE, MERGE, INSERT … SELECT) falls back to
        spark.sql(stmt) for execution inside the correct catalog.

        If the SparkContext was killed by a Spark master restart, the session
        is transparently reinitialised before the statement is retried once.
        """
        if not self._ready.wait(timeout=SPARK_INIT_TIMEOUT_S):
            return False, "SparkSession failed to initialise within timeout"
        if self._error:
            return False, f"SparkSession unavailable: {self._error}"

        with self._lock:
            t0 = time.time()
            try:
                return self._run_stmt(catalog, db, table, stmt, user, t0)
            except Exception as exc:
                cause = getattr(exc, "java_exception", None)
                msg   = str(cause if cause is not None else exc)
                # Detect Spark master restart killing our app, then recover once.
                if "Master removed our application" in msg or "SparkContext" in msg:
                    elapsed = time.time() - t0
                    logger.warning(
                        "SparkContext lost (%.1fs) — reinitialising and retrying: %s",
                        elapsed, msg.split("\n")[0][:200],
                    )
                    self._reinit_spark()
                    if not self._ready.wait(timeout=SPARK_INIT_TIMEOUT_S):
                        return False, "SparkSession recovery timed out"
                    if self._error:
                        return False, f"SparkSession recovery failed: {self._error}"
                    try:
                        return self._run_stmt(catalog, db, table, stmt, user, time.time())
                    except Exception as exc2:
                        cause2 = getattr(exc2, "java_exception", None)
                        msg2   = str(cause2 if cause2 is not None else exc2).split("\n")[0][:400]
                        logger.error("Spark DML FAILED after recovery: %s", msg2)
                        return False, msg2
                elapsed = time.time() - t0
                logger.error(
                    "Spark DML FAILED: %s.%s.%s elapsed=%.2fs error=%s",
                    catalog, db, table, elapsed, msg,
                )
                return False, msg.split("\n")[0][:400]

    # ── Private helpers ─────────────────────────────────────────────────────

    def _run_stmt(
        self, catalog: str, db: str, table: str, stmt: str, user: str, t0: float
    ) -> Tuple[bool, str]:
        """Inner execution — called by execute() and the recovery retry."""
        if _is_insert_values(stmt):
            rows_written = self._write_via_append(catalog, db, table, stmt, user)
            elapsed = time.time() - t0
            logger.info(
                "write_append SUCCESS: %s.%s.%s elapsed=%.2fs rows=%d",
                catalog, db, table, elapsed, rows_written,
            )
            return True, f"Write succeeded (rows={rows_written}, elapsed={elapsed:.2f}s)"

        # Fallback: UPDATE / DELETE / MERGE / INSERT … SELECT
        self._spark.sql(f"USE {catalog}.{db}")
        result = self._spark.sql(stmt)
        rows = result.count() if result is not None else 0
        elapsed = time.time() - t0
        logger.info(
            "Spark SQL SUCCESS: %s.%s elapsed=%.2fs rows=%d",
            catalog, db, elapsed, rows,
        )
        return True, f"Write succeeded (rows={rows}, elapsed={elapsed:.2f}s)"

    def _reinit_spark(self) -> None:
        """Stop the dead SparkSession and kick off a fresh background init."""
        logger.info("SparkSession: stopping dead context for reinit…")
        self._ready.clear()
        self._error = None
        # Flush caches — schema/builder state tied to the old SparkSession.
        self._schema_cache.clear()
        self._builder_cache.clear()
        try:
            if self._spark:
                self._spark.stop()
        except Exception:
            pass
        self._spark = None
        t = threading.Thread(target=self._init_spark, daemon=True, name="spark-reinit")
        t.start()


    def _write_via_append(
        self, catalog: str, db: str, table: str, stmt: str, user: str = ""
    ) -> int:
        """
        Parse INSERT INTO … VALUES (…), (…) into a Spark DataFrame and
        write it via IcebergTableBuilder.write_append().

        Schema resolution is cached per table (TTL = SCHEMA_CACHE_TTL_S) so
        repeated writes to the same table pay zero Iceberg metadata overhead
        after the first call.  The DataFrame is built dynamically from the
        parsed VALUES rows on every call — schema lookup is the only cached
        part; actual data is never reused across calls.

        user — the authenticated Doris MySQL username forwarded as running_user
               to IcebergTableBuilder so the RBAC gate sees the real identity.
        """
        from pyspark.sql.functions import col as _col

        spark = self._spark
        fqn   = f"{catalog}.{db}.{table}"          # cache key (plain, no backticks)
        fqn_q = f"`{catalog}`.`{db}`.`{table}`"    # quoted form for Spark APIs

        # ── 1. Schema lookup (cached) ─────────────────────────────────────────
        # Use spark.sql("DESCRIBE TABLE …") NOT spark.table().schema.
        # spark.table().schema triggers a Spark job that reads Iceberg metadata
        # files from S3 — that requires a live executor and is the source of the
        # multi-minute stall.  DESCRIBE TABLE is a pure Iceberg REST catalog RPC
        # (no S3, no tasks, no executor required) and returns in <1s.
        from pyspark.sql.types import (
            BooleanType, DateType, DecimalType, DoubleType, FloatType,
            IntegerType, LongType, ShortType, StringType, StructField,
            StructType, TimestampType,
        )

        cached = self._schema_cache.get(fqn)
        if cached is None or (time.time() - cached[1]) > SCHEMA_CACHE_TTL_S:
            raw_schema = _describe_to_schema(spark, fqn_q)
            self._schema_cache[fqn] = (raw_schema, time.time())
            logger.info("Schema cache MISS for %s — fetched %d fields", fqn, len(raw_schema.fields))
        else:
            raw_schema = cached[0]
            logger.debug("Schema cache HIT  for %s", fqn)

        # Build a case-insensitive name → StructField map for column lookup.
        schema_map = {f.name.lower(): f for f in raw_schema.fields}

        # ── 2. Resolve which columns this INSERT supplies ─────────────────────
        # Honour the explicit column list when present; fall back to all
        # business columns in Iceberg schema order when the INSERT has no list.
        _SNAP = {"snap_id", "snap_timestamp"}
        all_business = [f for f in raw_schema.fields if f.name.lower() not in _SNAP]
        explicit_cols = _extract_column_list(stmt)
        if explicit_cols:
            supplied_names = {c.lower() for c in explicit_cols if c.lower() not in _SNAP}
            col_fields = [schema_map[c.lower()] for c in explicit_cols if c.lower() not in _SNAP]
        else:
            supplied_names = {f.name.lower() for f in all_business}
            col_fields = all_business

        # ── 3. Parse the VALUES rows from the SQL text ────────────────────────
        values_text = _extract_values_text(stmt)
        rows        = _parse_values_rows(values_text)

        # ── 4. Build a typed DataFrame dynamically ────────────────────────────
        # Step 4a: create the supplied columns as string→cast.
        # Step 4b: add NULL literals for every business column NOT in the INSERT
        #          so Iceberg's append() sees a complete schema and does not
        #          raise CANNOT_FIND_DATA for omitted nullable columns.
        from pyspark.sql.functions import lit  # noqa: F811 (re-import is harmless)
        str_schema = StructType([
            StructField(f.name, StringType(), True) for f in col_fields
        ])
        str_rows = [
            tuple(None if v is None else str(v) for v in row)
            for row in rows
        ]
        supplied_exprs = [_col(f.name).cast(f.dataType).alias(f.name) for f in col_fields]
        missing_exprs  = [
            lit(None).cast(f.dataType).alias(f.name)
            for f in all_business
            if f.name.lower() not in supplied_names
        ]
        df = spark.createDataFrame(str_rows, schema=str_schema).select(
            supplied_exprs + missing_exprs
        )
        logger.info(
            "write_append: %s — %d col(s), %d row(s) [schema from %s]",
            fqn,
            len(col_fields),
            len(rows),
            "cache" if cached else "Iceberg",
        )

        # ── 5. Write via the platform's authorised path ───────────────────────
        # snap_id and snap_timestamp are injected here by write_append().
        # IcebergTableBuilder instances are cached per user to avoid repeated
        # object allocation on the hot path.
        if user not in self._builder_cache:
            self._builder_cache[user] = IcebergTableBuilder(
                spark, running_user=user or None
            )
        builder = self._builder_cache[user]
        return builder.write_append(df, catalog, db, table)

    @property
    def is_ready(self) -> bool:
        return self._ready.is_set() and self._error is None

    # ── Private ─────────────────────────────────────────────────────────────

    def _init_spark(self) -> None:
        t0 = time.time()
        logger.info("SparkSession: initialising (JVM + Gluten + executor launch)…")
        try:
            token = _bao_token()
            pol   = _read_secret(_PATH_POLARIS, token)
            s3    = _read_secret(_PATH_S3, token)
            conf  = _build_spark_conf(pol, s3)
            spark = SparkSession.builder.config(conf=conf).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            # Warm up the catalog connections with a lightweight no-op query
            # so the first real INSERT does not pay the OAuth roundtrip cost.
            for cat in MANAGED_CATALOGS:
                try:
                    spark.sql(f"USE {cat}.tpcds_sf10tcl")
                    break  # one successful USE is enough to warm the session
                except Exception:
                    pass
            self._spark = spark
            elapsed = time.time() - t0
            logger.info(
                "SparkSession: READY — Gluten/Velox active, executors up, "
                "catalogs connected. Cold-start took %.1fs. "
                "Subsequent INSERTs will complete in <3 s.",
                elapsed,
            )
        except Exception as exc:
            self._error = str(exc)
            logger.error("SparkSession: INIT FAILED — %s", exc)
        finally:
            self._ready.set()


# Module-level singleton — initialised at startup
_spark_manager = _SparkManager()


# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# Schema helpers
# ─────────────────────────────────────────────────────────────────────────────

def _describe_to_schema(spark, fqn_q: str):
    """
    Return a StructType for *fqn_q* using DESCRIBE TABLE — a pure Iceberg REST
    catalog RPC that requires no executor and no S3 access.

    spark.table(fqn).schema would also work but it reads Iceberg metadata files
    from S3, which needs a live executor and causes multi-second stalls when
    the executor hasn't been allocated yet (the first INSERT of a session).

    DESCRIBE TABLE output columns: col_name, data_type, comment.
    Rows after the first blank col_name are partition/metadata — stop there.
    """
    from pyspark.sql.types import (
        BooleanType, DateType, DecimalType, DoubleType, FloatType,
        IntegerType, LongType, ShortType, StringType, StructField,
        StructType, TimestampType,
    )

    rows = spark.sql(f"DESCRIBE TABLE {fqn_q}").collect()
    fields = []
    for r in rows:
        name = r["col_name"].strip()
        if not name or name.startswith("#"):
            break                          # stop at partition info section
        dtype_str = r["data_type"].strip().lower()
        fields.append(StructField(name, _sql_type_to_spark(dtype_str), nullable=True))
    return StructType(fields)


def _sql_type_to_spark(dtype: str):
    """
    Map a Spark/Iceberg SQL type string from DESCRIBE TABLE to a StructField
    DataType.  Covers all types used in TPC-DS and standard Iceberg tables.
    """
    from pyspark.sql.types import (
        BooleanType, DateType, DecimalType, DoubleType, FloatType,
        IntegerType, LongType, ShortType, StringType, TimestampType,
    )
    dtype = dtype.strip().lower()
    if dtype in ("string", "varchar", "char", "text"):
        return StringType()
    if dtype in ("bigint", "int8", "long"):
        return LongType()
    if dtype in ("int", "integer", "int4"):
        return IntegerType()
    if dtype in ("smallint", "int2", "short"):
        return ShortType()
    if dtype in ("boolean", "bool"):
        return BooleanType()
    if dtype in ("float", "real", "float4"):
        return FloatType()
    if dtype in ("double", "float8", "double precision"):
        return DoubleType()
    if dtype in ("date",):
        return DateType()
    if dtype in ("timestamp", "timestamp_ntz", "timestamp with time zone",
                 "timestamp without time zone"):
        return TimestampType()
    if dtype.startswith("decimal("):
        # decimal(precision,scale)
        inner = dtype[8:-1]
        parts = inner.split(",")
        p, s = int(parts[0].strip()), int(parts[1].strip()) if len(parts) > 1 else 0
        return DecimalType(p, s)
    if dtype.startswith("decimal"):
        return DecimalType(38, 18)
    # Unknown types → string (safe fallback; cast will handle conversion)
    logger.warning("_sql_type_to_spark: unknown type %r → StringType fallback", dtype)
    return StringType()


# INSERT VALUES parser helpers
# ─────────────────────────────────────────────────────────────────────────────

# Matches the VALUES keyword and everything after it
_VALUES_RE = re.compile(r"\bVALUES\s*(.+)$", re.IGNORECASE | re.DOTALL)

# Matches optional column list between table ref and VALUES:
#   INSERT INTO tbl (col1, col2, …) VALUES …
_COL_LIST_RE = re.compile(
    r"^\s*INSERT\s+(?:INTO|OVERWRITE)\s+"
    r"[\w`.]+(?:\.[\w`.]+)*"   # table ref (possibly 3-part)
    r"\s*\(([^)]+)\)"          # (col1, col2, …)
    r"\s*VALUES\b",
    re.IGNORECASE,
)


def _extract_column_list(stmt: str) -> list[str] | None:
    """
    Return the explicit column name list from INSERT INTO tbl (col, …) VALUES …
    Returns None if no column list is present (meaning positional / all columns).
    """
    m = _COL_LIST_RE.match(stmt)
    if not m:
        return None
    return [c.strip().strip("`") for c in m.group(1).split(",")]


def _is_insert_values(stmt: str) -> bool:
    """Return True if the statement is INSERT … VALUES (not INSERT … SELECT)."""
    upper = stmt.upper()
    return (
        re.match(r"\s*INSERT\b", upper) is not None
        and "VALUES" in upper
        and not re.search(r"\bSELECT\b", upper)
    )


def _extract_values_text(stmt: str) -> str:
    """Return the raw text after the VALUES keyword."""
    m = _VALUES_RE.search(stmt)
    if not m:
        raise ValueError(f"No VALUES clause found in statement: {stmt[:120]}")
    return m.group(1).strip().rstrip(";")


def _parse_values_rows(values_text: str) -> list[tuple]:
    """
    Parse a VALUES clause into a list of Python tuples.

    Handles:
      (1, 'hello', NULL, 3.14), (2, 'world', NULL, 2.71)

    Limitations (sufficient for business INSERT usage):
      - String literals may contain escaped single quotes (\\') but not
        unescaped parentheses.
      - NULL becomes Python None.
      - Numeric literals are parsed as int or float.
    """
    rows: list[tuple] = []
    # Split on top-level commas between row groups: (…), (…)
    # We iterate character-by-character to handle nested parens if ever needed.
    depth = 0
    current: list[str] = []
    buf = ""
    in_str = False
    escape = False

    for ch in values_text:
        if escape:
            buf += ch
            escape = False
            continue
        if ch == "\\" and in_str:
            buf += ch
            escape = True
            continue
        if ch == "'" and not in_str:
            in_str = True
            buf += ch
            continue
        if ch == "'" and in_str:
            in_str = False
            buf += ch
            continue
        if in_str:
            buf += ch
            continue
        if ch == "(" and depth == 0:
            depth = 1
            buf = ""
            continue
        if ch == "(":
            depth += 1
            buf += ch
            continue
        if ch == ")" and depth == 1:
            depth = 0
            current.append(buf.strip())
            buf = ""
            rows.append(tuple(_parse_value(v.strip()) for v in current))
            current = []
            continue
        if ch == ")" and depth > 1:
            depth -= 1
            buf += ch
            continue
        if ch == "," and depth == 0:
            # separator between row groups — nothing to do
            continue
        if ch == "," and depth == 1:
            current.append(buf.strip())
            buf = ""
            continue
        buf += ch

    return rows


def _parse_value(token: str):
    """Convert a SQL token string to a Python scalar."""
    if token.upper() == "NULL":
        return None
    # Unquote string literals
    if token.startswith("'") and token.endswith("'"):
        return token[1:-1].replace("\\'", "'").replace("''", "'")
    # Try int then float
    try:
        return int(token)
    except ValueError:
        pass
    try:
        return float(token)
    except ValueError:
        pass
    return token


# ─────────────────────────────────────────────────────────────────────────────
# MySQL packet helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pack_packet(payload: bytes, seq: int) -> bytes:
    return struct.pack("<I", len(payload))[:3] + bytes([seq]) + payload


def _mysql_ok_packet(seq: int, affected_rows: int = 0) -> bytes:
    def _lenc(n: int) -> bytes:
        if n < 251:
            return bytes([n])
        if n < 65536:
            return b"\xfc" + struct.pack("<H", n)
        return b"\xfd" + struct.pack("<I", n)[:3]
    payload = b"\x00" + _lenc(affected_rows) + _lenc(0) + b"\x00\x00" + b"\x00\x00"
    return _pack_packet(payload, seq)


def _mysql_err_packet(seq: int, message: str, error_code: int = 2000) -> bytes:
    msg_bytes = message.encode("utf-8", errors="replace")[:512]
    payload = (
        b"\xff"
        + struct.pack("<H", error_code)
        + b"#"
        + b"HY000"
        + msg_bytes
    )
    return _pack_packet(payload, seq)


async def _read_packet(reader: asyncio.StreamReader) -> Tuple[int, bytes]:
    header = await reader.readexactly(4)
    length = struct.unpack("<I", header[:3] + b"\x00")[0]
    seq    = header[3]
    payload = await reader.readexactly(length) if length else b""
    return seq, payload


async def _read_all_response(reader: asyncio.StreamReader) -> list[Tuple[int, bytes]]:
    packets: list[Tuple[int, bytes]] = []
    seq, payload = await _read_packet(reader)
    packets.append((seq, payload))
    if not payload:
        return packets
    first_byte = payload[0]
    if first_byte == 0x00 or first_byte == 0xFF:
        return packets
    if first_byte == 0xFE and len(payload) < 9:
        return packets
    col_count = payload[0]
    for _ in range(col_count):
        seq, pkt = await _read_packet(reader)
        packets.append((seq, pkt))
    seq, pkt = await _read_packet(reader)
    packets.append((seq, pkt))
    while True:
        seq, pkt = await _read_packet(reader)
        packets.append((seq, pkt))
        if pkt and (pkt[0] == 0xFE and len(pkt) < 9):
            break
        if pkt and pkt[0] == 0xFF:
            break
    return packets


def _parse_handshake_username(auth_resp: bytes) -> str:
    """
    Extract the username from a MySQL HandshakeResponse41 packet payload
    (i.e. the raw bytes after the 4-byte packet header is stripped).

    HandshakeResponse41 layout:
      4 bytes  capability flags
      4 bytes  max packet size
      1 byte   character set
      23 bytes reserved (zeros)
      n bytes  username (null-terminated)
      ...      auth response, db name, etc.

    Returns an empty string on any parse failure.
    """
    try:
        offset = 4 + 4 + 1 + 23   # skip capability(4) + max_pkt(4) + charset(1) + reserved(23)
        if len(auth_resp) <= offset:
            return ""
        end = auth_resp.index(b"\x00", offset)
        return auth_resp[offset:end].decode("utf-8", errors="replace")
    except (ValueError, UnicodeDecodeError):
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# SQL inspection
# ─────────────────────────────────────────────────────────────────────────────
_DML_RE = re.compile(
    r"^\s*(insert\s+(?:into|overwrite)|update|delete(?:\s+from)?|merge\s+into)\b",
    re.IGNORECASE,
)


def _catalog_and_parts(stmt: str) -> Tuple[str | None, str | None, str | None]:
    if not _DML_RE.match(stmt):
        return None, None, None
    tokens = stmt.split()
    verb   = tokens[0].upper()
    second = tokens[1].upper() if len(tokens) > 1 else ""
    if verb == "INSERT" and second in ("INTO", "OVERWRITE"):
        ref = tokens[2] if len(tokens) > 2 else ""
    elif verb == "UPDATE":
        ref = tokens[1] if len(tokens) > 1 else ""
    elif verb == "DELETE" and second == "FROM":
        ref = tokens[2] if len(tokens) > 2 else ""
    elif verb == "DELETE":
        ref = tokens[1] if len(tokens) > 1 else ""
    elif verb == "MERGE" and second == "INTO":
        ref = tokens[2] if len(tokens) > 2 else ""
    else:
        return None, None, None
    ref   = ref.strip("`").rstrip(";,(")
    parts = [p.strip("`") for p in ref.split(".")]
    if len(parts) >= 3:
        catalog, db, table = parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        catalog, db, table = None, parts[0], parts[1]
    else:
        return None, None, None
    if catalog and catalog.lower() in MANAGED_CATALOGS:
        return catalog.lower(), db, table
    return None, None, None


# ─────────────────────────────────────────────────────────────────────────────
# Per-connection handler
# ─────────────────────────────────────────────────────────────────────────────

class ProxyConnection:
    _COM_QUERY = 0x03

    def __init__(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._cr   = client_reader
        self._cw   = client_writer
        self._loop = loop
        peer = client_writer.get_extra_info("peername", ("?", 0))
        self._peer = f"{peer[0]}:{peer[1]}"
        self._user = ""   # filled from HandshakeResponse41 during auth

    async def run(self) -> None:
        logger.info("WriteProxy: client connected from %s", self._peer)
        doris_reader: asyncio.StreamReader | None = None
        doris_writer: asyncio.StreamWriter | None = None
        try:
            doris_reader, doris_writer = await asyncio.open_connection(DORIS_HOST, DORIS_PORT)

            # ── Handshake ─────────────────────────────────────────────────────
            seq, handshake = await _read_packet(doris_reader)
            self._cw.write(_pack_packet(handshake, seq))
            await self._cw.drain()

            seq, auth_resp = await _read_packet(self._cr)
            self._user = _parse_handshake_username(auth_resp)
            doris_writer.write(_pack_packet(auth_resp, seq))
            await doris_writer.drain()

            auth_packets = await _read_all_response(doris_reader)
            for s, p in auth_packets:
                self._cw.write(_pack_packet(p, s))
            await self._cw.drain()

            if auth_packets and auth_packets[0][1] and auth_packets[0][1][0] == 0xFF:
                logger.warning("WriteProxy: auth failed for %s", self._peer)
                return

            logger.info("WriteProxy: %s authenticated as '%s'.", self._peer, self._user)

            # ── Command loop ──────────────────────────────────────────────────
            while True:
                try:
                    seq, payload = await _read_packet(self._cr)
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    break

                if not payload:
                    break

                cmd = payload[0]

                if cmd == 0x01:  # COM_QUIT
                    doris_writer.write(_pack_packet(payload, seq))
                    await doris_writer.drain()
                    break

                if cmd == self._COM_QUERY:
                    stmt = payload[1:].decode("utf-8", errors="replace").strip()
                    catalog, db, table = _catalog_and_parts(stmt)

                    if catalog:
                        logger.info(
                            "WriteProxy: intercepted %s.%s.%s DML from %s — routing to Spark.",
                            catalog, db, table, self._peer,
                        )
                        ok, message = await self._loop.run_in_executor(
                                None,
                                _spark_manager.execute,
                                catalog, db, table, stmt, self._user,
                            )
                        if ok:
                            self._cw.write(_mysql_ok_packet(seq + 1))
                        else:
                            self._cw.write(_mysql_err_packet(seq + 1, message))
                        await self._cw.drain()
                        continue

                doris_writer.write(_pack_packet(payload, seq))
                await doris_writer.drain()
                response_packets = await _read_all_response(doris_reader)
                for s, p in response_packets:
                    self._cw.write(_pack_packet(p, s))
                await self._cw.drain()

        except Exception as exc:
            logger.error("WriteProxy: connection error (%s): %s", self._peer, exc)
        finally:
            if doris_writer:
                try:
                    doris_writer.close()
                    await doris_writer.wait_closed()
                except Exception:
                    pass
            try:
                self._cw.close()
                await self._cw.wait_closed()
            except Exception:
                pass
            logger.info("WriteProxy: %s disconnected.", self._peer)


# ─────────────────────────────────────────────────────────────────────────────
# Server entry point
# ─────────────────────────────────────────────────────────────────────────────

async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    loop = asyncio.get_event_loop()
    await ProxyConnection(reader, writer, loop).run()


async def main() -> None:
    # Start SparkSession initialisation in background immediately —
    # proxy accepts connections while Spark warms up.
    _spark_manager.start_background_init()

    logger.info(
        "Doris Write Proxy starting. "
        "listen=%s:%d  doris=%s:%d  spark=%s",
        LISTEN_HOST, LISTEN_PORT, DORIS_HOST, DORIS_PORT, SPARK_MASTER_URL,
    )
    logger.info("Managed catalogs: %s", ", ".join(MANAGED_CATALOGS.keys()))
    logger.info(
        "SparkSession: warming up in background — first INSERT will wait up to %ds, "
        "subsequent INSERTs will be <3 s.",
        SPARK_INIT_TIMEOUT_S,
    )
    server = await asyncio.start_server(_handle, LISTEN_HOST, LISTEN_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
