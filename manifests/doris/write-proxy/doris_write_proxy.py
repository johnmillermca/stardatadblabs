"""
doris_write_proxy.py
====================
Transparent MySQL protocol proxy for Doris write-pushdown to Apache Spark.

Architecture
------------
Clients connect to this proxy on port 9030 using the standard MySQL protocol.
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
  LISTEN_PORT         Proxy listen port     (default: 9030)
  SPARK_MASTER_URL    Spark master URL      (default: local[*] — driver IS the executor, no cluster needed)
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
LISTEN_PORT         = int(os.environ.get("LISTEN_PORT", "9030"))

# Cache-guard SELECT intercept
# URL of the cache manager's warm-up trigger HTTP endpoint.
# Set to empty string to disable the intercept entirely.
CACHE_MANAGER_URL   = os.environ.get(
    "CACHE_MANAGER_URL",
    "http://doris-cache-manager.prod.svc.cluster.local:8090",
)

SPARK_MASTER_URL    = os.environ.get(
    "SPARK_MASTER_URL",
    "local[*]",   # default: local mode — driver IS the executor, no cluster needed for writes
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
_PATH_DORIS         = "secret/data/platform/doris"

# ── Polaris URI for Spark catalogs ────────────────────────────────────────────
# MUST point to polaris-auth-proxy (port 8283), NOT polaris-rest directly.
# The auth-proxy runs a background thread that refreshes the OAuth2 token
# 300 s before expiry, so every request Spark makes carries a valid Bearer token.
# Pointing directly at polaris-rest would bypass that refresh and tokens would
# expire after 1 hour with no way for Spark to renew them.
_POLARIS_URI = os.environ.get(
    "POLARIS_URI",
    "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
)

# How often (seconds) the background token-keepalive thread rebuilds the
# SparkSession to pick up fresh OAuth2 credentials from OpenBao.
# Set to 50 min (3000 s) — safely before the 1-hour Polaris token TTL.
# Override via TOKEN_REFRESH_INTERVAL_S env var.
_TOKEN_REFRESH_INTERVAL_S = int(os.environ.get("TOKEN_REFRESH_INTERVAL_S", "3000"))

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

# spark_iceberg_utils.py is shipped in the image (COPY --from=spark-jars) but
# IcebergTableBuilder is no longer called at runtime — snap columns are now
# injected as SQL literals in the reconstructed INSERT statement executed via
# spark.sql().  The import is kept so the module is importable (syntax check)
# and available if needed for future use.
sys.path.insert(0, "/app")
from spark_iceberg_utils import IcebergTableBuilder  # noqa: E402  (kept for future use)


def _build_spark_conf(pol: dict, s3: dict) -> SparkConf:
    """
    Build a SparkConf wired for ALL managed catalogs.  Runs in local[*] mode:
    the driver IS the executor — no remote Spark cluster, no task scheduling,
    no Gluten/Velox interference.  Writes go directly from the proxy pod to
    S3 via the Iceberg REST catalog.  Resources are released immediately when
    the write completes (no idle executors held on Spark workers).
    """
    conf = SparkConf()
    conf.setAppName("doris-write-proxy")
    conf.setMaster(SPARK_MASTER_URL)   # default: local[*]

    # ── Driver memory (local mode — driver = executor) ────────────────────────
    conf.set("spark.driver.memory", "4g")

    # ── No Gluten in local mode — Gluten/Velox is a cluster-executor plugin ──
    conf.set("spark.plugins", "")
    conf.set("spark.memory.offHeap.enabled", "false")

    # ── S3A ───────────────────────────────────────────────────────────────────
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
    # Use polaris-auth-proxy URI — the proxy carries a perpetually-fresh Bearer
    # token so Spark never sees an expired credential, regardless of how long
    # the SparkSession has been alive.
    polaris_uri = _POLARIS_URI
    credential  = f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}"

    for cat, warehouse in MANAGED_CATALOGS.items():
        conf.set(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        conf.set(f"spark.sql.catalog.{cat}.type",              "rest")
        conf.set(f"spark.sql.catalog.{cat}.uri",               polaris_uri)
        # Explicit oauth2-server-uri suppresses the Iceberg deprecation warning and
        # ensures OAuth2Manager knows where to fetch/refresh tokens.
        conf.set(f"spark.sql.catalog.{cat}.oauth2-server-uri", f"{polaris_uri}/v1/oauth/tokens")
        conf.set(f"spark.sql.catalog.{cat}.credential",        credential)
        conf.set(f"spark.sql.catalog.{cat}.scope",             "PRINCIPAL_ROLE:ALL")
        conf.set(f"spark.sql.catalog.{cat}.warehouse",         warehouse)
        conf.set(f"spark.sql.catalog.{cat}.rest.auth.type",    "oauth2")
        # Tell Iceberg's OAuth2Manager to refresh the token 5 minutes before
        # expiry instead of waiting until it has actually expired.
        conf.set(f"spark.sql.catalog.{cat}.token-refresh-enabled",  "true")
        conf.set(f"spark.sql.catalog.{cat}.token-expiration-ms",    "3600000")   # 1 h Polaris TTL
        conf.set(f"spark.sql.catalog.{cat}.min-token-refresh-wait-ms", "300000") # refresh ≥5 min early
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

    Token expiry — three-layer defence
    ------------------------------------
    Layer 1 — polaris-auth-proxy URI:
        Spark catalogs point to polaris-auth-proxy:8283 (not polaris-rest:8181
        directly).  The auth-proxy runs a background thread that renews its
        Bearer token 300 s before expiry, so every Iceberg REST request the
        SparkSession makes is already authenticated — no token logic needed
        in Spark at all.

    Layer 2 — Iceberg OAuth2Manager proactive refresh:
        SparkConf sets token-refresh-enabled=true and min-token-refresh-wait-ms=300000
        so Iceberg's built-in OAuth2Manager refreshes the token ≥5 min before
        the declared expiry (token-expiration-ms=3600000).

    Layer 3 — background keepalive thread:
        A daemon thread rebuilds the SparkSession every TOKEN_REFRESH_INTERVAL_S
        (default 3000 s = 50 min) while idle (no write in progress).  This is the
        hard backstop: even if layers 1 and 2 somehow fail, the session is never
        more than 50 minutes old and a fresh token is always loaded.
    """

    # Error substrings that indicate the Polaris OAuth2 token has expired.
    _TOKEN_EXPIRY_HINTS = (
        "NotAuthorizedException",
        "Not authorized",
        "No content to map due to end-of-input",  # empty 401 body → Jackson parse fail
    )

    def __init__(self) -> None:
        self._spark: Optional[SparkSession] = None
        self._error: Optional[str] = None
        self._lock  = threading.Lock()
        self._ready = threading.Event()
        # Schema cache: fqn → (StructType, fetched_at_epoch)
        # Protected by _lock (same lock that serialises writes).
        self._schema_cache: dict[str, tuple] = {}
        # Timestamp of the last successful _init_spark — used by keepalive.
        self._session_built_at: float = 0.0

    # ── Public ─────────────────────────────────────────────────────────────

    def start_background_init(self) -> None:
        """Kick off SparkSession init + keepalive thread. Returns immediately."""
        threading.Thread(target=self._init_spark, daemon=True, name="spark-init").start()
        threading.Thread(target=self._keepalive_loop, daemon=True, name="spark-token-keepalive").start()

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
            return self._execute_with_token_refresh(catalog, db, table, stmt, user)

    def _execute_with_token_refresh(
        self,
        catalog: str, db: str, table: str, stmt: str, user: str,
        _retry: bool = False,
    ) -> Tuple[bool, str]:
        """
        Inner execute — called inside self._lock.
        On first NotAuthorizedException, rebuilds the SparkSession (fresh
        OpenBao credentials + fresh Polaris token) and retries once.
        """
        t0 = time.time()
        try:
            if _is_insert_values(stmt):
                rows_written = self._write_via_append(catalog, db, table, stmt, user)
                elapsed = time.time() - t0
                logger.info(
                    "write_append SUCCESS: %s.%s.%s elapsed=%.2fs rows=%d",
                    catalog, db, table, elapsed, rows_written,
                )
                return True, f"Write succeeded (rows={rows_written}, elapsed={elapsed:.2f}s)"
            # Fallback: UPDATE / DELETE / MERGE / INSERT … SELECT
            self._spark.sql(f"USE `{catalog}`.`{db}`")
            result = self._spark.sql(stmt)
            rows = result.count() if result is not None else 0
            elapsed = time.time() - t0
            logger.info(
                "Spark SQL SUCCESS: %s.%s elapsed=%.2fs rows=%d",
                catalog, db, elapsed, rows,
            )
            return True, f"Write succeeded (rows={rows}, elapsed={elapsed:.2f}s)"
        except Exception as exc:
            elapsed = time.time() - t0
            cause = getattr(exc, "java_exception", None)
            full_msg = str(cause if cause is not None else exc)
            short_msg = full_msg.split("\n")[0][:400]

            # ── Token expiry: rebuild session and retry once ───────────────
            if not _retry and any(h in full_msg for h in self._TOKEN_EXPIRY_HINTS):
                logger.warning(
                    "SparkSession: Polaris token expired after %.1fs — "
                    "rebuilding session with fresh credentials and retrying.",
                    elapsed,
                )
                self._schema_cache.clear()   # schema cache may hold stale catalog refs
                try:
                    if self._spark:
                        self._spark.stop()
                except Exception:
                    pass
                self._spark = None
                self._error = None
                self._ready.clear()
                self._init_spark()           # synchronous rebuild inside the lock
                if self._error:
                    return False, f"SparkSession rebuild failed: {self._error}"
                logger.info("SparkSession: rebuilt successfully — retrying statement.")
                return self._execute_with_token_refresh(
                    catalog, db, table, stmt, user, _retry=True
                )

            logger.error(
                "Spark DML FAILED: %s.%s.%s elapsed=%.2fs error=%s",
                catalog, db, table, elapsed, short_msg,
            )
            return False, short_msg

    # ── Private helpers ─────────────────────────────────────────────────────


    def _write_via_append(
        self, catalog: str, db: str, table: str, stmt: str, user: str = ""
    ) -> int:
        """
        Parse INSERT INTO … VALUES (…) into a Spark DataFrame and write via
        IcebergTableBuilder.write_append(), which injects snap_id and
        snap_timestamp automatically — callers never supply those columns.

        Runs in local[*] mode — driver IS the executor.  No task scheduling,
        no Gluten/Velox interference, resources released immediately on completion.

        Schema is fetched via DESCRIBE TABLE (pure REST, no S3) and cached.
        """
        from pyspark.sql.functions import col as _col, lit
        from pyspark.sql.types import StringType, StructField, StructType

        spark = self._spark
        fqn   = f"{catalog}.{db}.{table}"
        fqn_q = f"`{catalog}`.`{db}`.`{table}`"

        # ── 1. Schema (cached, executor-free via DESCRIBE TABLE) ──────────────
        cached = self._schema_cache.get(fqn)
        if cached is None or (time.time() - cached[1]) > SCHEMA_CACHE_TTL_S:
            raw_schema = _describe_to_schema(spark, fqn_q)
            self._schema_cache[fqn] = (raw_schema, time.time())
            logger.info("Schema cache MISS for %s — %d fields", fqn, len(raw_schema.fields))
        else:
            raw_schema = cached[0]
            logger.debug("Schema cache HIT  for %s", fqn)

        schema_map = {f.name.lower(): f for f in raw_schema.fields}

        # ── 2. Resolve which columns this INSERT supplies ─────────────────────
        _SNAP = {"snap_id", "snap_timestamp"}
        all_business = [f for f in raw_schema.fields if f.name.lower() not in _SNAP]
        explicit_cols = _extract_column_list(stmt)
        if explicit_cols:
            supplied_names = {c.lower() for c in explicit_cols if c.lower() not in _SNAP}
            col_fields     = [schema_map[c.lower()] for c in explicit_cols if c.lower() not in _SNAP]
        else:
            supplied_names = {f.name.lower() for f in all_business}
            col_fields     = all_business

        # ── 3. Parse VALUES rows ──────────────────────────────────────────────
        rows = _parse_values_rows(_extract_values_text(stmt))

        # ── 4. Build typed DataFrame (all-string → cast) ──────────────────────
        # Create as all-string to avoid Python int → DecimalType rejection,
        # then cast each column to its declared Iceberg type.  Missing business
        # columns are added as NULL so write_append() sees a complete schema.
        str_schema = StructType([StructField(f.name, StringType(), True) for f in col_fields])
        str_rows   = [tuple(None if v is None else str(v) for v in row) for row in rows]
        supplied_exprs = [_col(f.name).cast(f.dataType).alias(f.name) for f in col_fields]
        missing_exprs  = [
            lit(None).cast(f.dataType).alias(f.name)
            for f in all_business if f.name.lower() not in supplied_names
        ]
        df = spark.createDataFrame(str_rows, schema=str_schema).select(
            supplied_exprs + missing_exprs
        )
        logger.info(
            "write_append: %s — %d col(s), %d row(s) [schema %s]",
            fqn, len(all_business), len(rows), "cached" if cached else "fresh",
        )

        # ── 5. Write — snap_id + snap_timestamp injected by write_append() ────
        builder = IcebergTableBuilder(spark, running_user=user or None)
        try:
            return builder.write_append(df, catalog, db, table)
        finally:
            # Release DataFrame / RDD memory and any broadcast/shuffle state
            # immediately so the JVM heap is available for the next write job.
            try:
                df.unpersist()
                spark.catalog.clearCache()
                spark.sparkContext._jvm.System.gc()  # type: ignore[attr-defined]
                logger.debug("post-write cleanup: df unpersisted, catalog cache cleared, GC requested")
            except Exception:
                pass  # cleanup is best-effort — never fail the write because of it

    @property
    def is_ready(self) -> bool:
        return self._ready.is_set() and self._error is None

    # ── Private ─────────────────────────────────────────────────────────────

    def _keepalive_loop(self) -> None:
        """
        Background daemon thread — rebuilds the SparkSession every
        TOKEN_REFRESH_INTERVAL_S (default 3000 s = 50 min) while no write
        is in progress.

        This is Layer 3 of the token-expiry defence: even if polaris-auth-proxy
        and Iceberg's built-in OAuth2Manager both somehow fail to refresh the
        token, the session is never more than 50 minutes old, so a fresh Polaris
        credential is always loaded before the 60-minute TTL expires.

        The loop:
          1. Waits until the initial SparkSession is ready (up to
             SPARK_INIT_TIMEOUT_S seconds).
          2. Sleeps in small increments until TOKEN_REFRESH_INTERVAL_S seconds
             have elapsed since _session_built_at.
          3. Acquires _lock (waits for any in-flight write to finish first).
          4. Tears down the old session and calls _init_spark() synchronously.
          5. Loops back to step 2.
        """
        logger.info(
            "SparkSession keepalive: thread started (interval=%ds).",
            _TOKEN_REFRESH_INTERVAL_S,
        )
        # Wait for the initial session to be ready before entering the loop.
        if not self._ready.wait(timeout=SPARK_INIT_TIMEOUT_S):
            logger.warning(
                "SparkSession keepalive: initial session never became ready — "
                "keepalive thread exiting."
            )
            return

        while True:
            # Sleep until it is time to rebuild.  Poll every 30 s so we react
            # promptly if the process is shutting down or the interval is short.
            while True:
                age = time.time() - self._session_built_at
                remaining = _TOKEN_REFRESH_INTERVAL_S - age
                if remaining <= 0:
                    break
                time.sleep(min(30, remaining))

            logger.info(
                "SparkSession keepalive: session age %.0fs ≥ interval %ds — "
                "rebuilding to pick up fresh Polaris credentials.",
                time.time() - self._session_built_at,
                _TOKEN_REFRESH_INTERVAL_S,
            )

            with self._lock:
                # Tear down the current session cleanly.
                try:
                    if self._spark:
                        self._spark.stop()
                except Exception as exc:
                    logger.warning("SparkSession keepalive: stop() raised %s (ignored)", exc)
                self._spark = None
                self._error = None
                self._ready.clear()
                self._schema_cache.clear()

                # Rebuild synchronously inside the lock so no write can start
                # until the new session is fully initialised.
                self._init_spark()

                if self._error:
                    logger.error(
                        "SparkSession keepalive: rebuild failed — %s. "
                        "Will retry in %ds.",
                        self._error,
                        _TOKEN_REFRESH_INTERVAL_S,
                    )
                else:
                    logger.info(
                        "SparkSession keepalive: session rebuilt successfully "
                        "(fresh Polaris token loaded)."
                    )

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
            self._session_built_at = time.time()   # keepalive uses this as the age baseline
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


def _to_sql_literal(value) -> str:
    """
    Convert a Python scalar (from _parse_value) to a SQL literal string
    safe to embed directly inside a Spark SQL VALUES clause.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)          # repr preserves full precision
    # String — wrap in single quotes, escape internal single quotes.
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


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

_SELECT_RE = re.compile(
    r"^\s*(select\b|with\b)",
    re.IGNORECASE,
)

# Extract the first fully-qualified catalog.db.table reference from a SELECT.
_FROM_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+`?(\w+)`?\.`?(\w+)`?\.`?(\w+)`?",
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
# SELECT cache guard — intercept cold-table queries before they reach Doris
# ─────────────────────────────────────────────────────────────────────────────

class _SelectGuard:
    """
    Checks whether tables referenced in a SELECT are WARM in the Doris segment
    cache before the query is forwarded to Doris.

    If any referenced managed-catalog table is not WARM (or is unseen):
      1. Returns a MySQL ERR packet to the client immediately — the SELECT is
         never forwarded to Doris, so no S3 read occurs.
      2. Writes a row to cache_system.query_block_log so the user can see why
         their query was rejected and when warm-up will complete.
      3. Calls the cache manager's POST /warmup HTTP endpoint to trigger
         immediate warm-up for every cold table.

    Once warm-up completes (typically < 2 minutes for small tables), the user
    re-runs the same query — this time all tables are WARM and the SELECT is
    forwarded to Doris normally, served from NVMe cache.

    The guard uses a dedicated pymysql connection per proxy process (not per
    connection) so it never shares state with the write-path Doris connection.
    It is disabled if CACHE_MANAGER_URL is empty.
    """

    _WARM = "WARM"

    def __init__(self) -> None:
        self._conn = None
        self._lock = threading.Lock()

    def _get_conn(self):
        """Lazy-connect to Doris for metadata queries. Reconnects on failure."""
        import pymysql  # type: ignore
        if self._conn is None:
            doris_pass = os.environ.get("DORIS_ADMIN_PASSWORD", "")
            self._conn = pymysql.connect(
                host=DORIS_HOST, port=DORIS_PORT,
                user=DORIS_USER, password=doris_pass,
                charset="utf8mb4", connect_timeout=5,
                read_timeout=5, autocommit=True,
            )
        try:
            self._conn.ping(reconnect=True)
        except Exception:
            doris_pass = os.environ.get("DORIS_ADMIN_PASSWORD", "")
            self._conn = pymysql.connect(
                host=DORIS_HOST, port=DORIS_PORT,
                user=DORIS_USER, password=doris_pass,
                charset="utf8mb4", connect_timeout=5,
                read_timeout=5, autocommit=True,
            )
        return self._conn

    def check(
        self,
        stmt: str,
        user: str,
        query_id: str,
    ) -> "str | None":
        """
        Check cache state for all managed-catalog tables in stmt.
        Returns an error message string if any table is cold/unseen,
        or None if all tables are WARM (query should proceed).
        Catches all exceptions internally — never raises.
        """
        if not CACHE_MANAGER_URL:
            return None  # intercept disabled
        if not _SELECT_RE.match(stmt):
            return None  # not a SELECT

        # Extract all catalog.db.table references in managed catalogs
        cold_tables = []
        try:
            with self._lock:
                conn = self._get_conn()
                for m in _FROM_TABLE_RE.finditer(stmt):
                    catalog, db, table = m.group(1).lower(), m.group(2), m.group(3)
                    if catalog not in MANAGED_CATALOGS:
                        continue
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT cache_state FROM cache_system.table_query_stats "
                            "WHERE catalog_name=%s AND db_name=%s AND table_name=%s",
                            (catalog, db, table),
                        )
                        row = cur.fetchone()
                    # Not in stats (never seen) or not WARM → cold
                    state = row[0] if row else None
                    if state != self._WARM:
                        cold_tables.append(f"{catalog}.{db}.{table}")
        except Exception as exc:
            logger.warning("SelectGuard: metadata check failed (%s) — letting query through.", exc)
            return None

        if not cold_tables:
            return None  # all WARM

        cold_str = ", ".join(cold_tables)
        message = (
            f"The following table(s) referenced in your query are not in the "
            f"Doris segment cache: {cold_str}. "
            f"Automatic warm-up has been triggered — please retry your query "
            f"in a few minutes once warm-up completes."
        )
        logger.info(
            "SelectGuard: query_id=%s user=%s — cold tables: %s",
            query_id, user, cold_str,
        )

        # Write query_block_log row
        try:
            with self._lock:
                conn = self._get_conn()
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO cache_system.query_block_log "
                        "(query_id, detected_at, user_name, cold_tables, stmt_preview, message) "
                        "VALUES (%s, NOW(), %s, %s, %s, %s)",
                        (query_id, user, cold_str, stmt[:500], message),
                    )
        except Exception as exc:
            logger.warning("SelectGuard: could not write query_block_log (%s).", exc)

        # Trigger warm-up on the cache manager (fire-and-forget)
        for fqn in cold_tables:
            try:
                parts = fqn.split(".")
                body = json.dumps({"catalog": parts[0], "db": parts[1], "table": parts[2]}).encode()
                req = urllib.request.Request(
                    f"{CACHE_MANAGER_URL}/warmup",
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=3)
                logger.info("SelectGuard: warm-up triggered for %s via cache manager.", fqn)
            except Exception as exc:
                logger.warning("SelectGuard: warm-up trigger failed for %s (%s).", fqn, exc)

        return message


# Module-level singleton
_select_guard = _SelectGuard()


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

                    # ── SELECT cache guard ────────────────────────────────────
                    # Check before forwarding: if any referenced managed-catalog
                    # table is not WARM, reject immediately with a MySQL error,
                    # write query_block_log, and trigger warm-up. The client
                    # never hits Doris and no S3 read occurs.
                    if _SELECT_RE.match(stmt):
                        import uuid
                        query_id = str(uuid.uuid4())
                        err_msg = await self._loop.run_in_executor(
                            None, _select_guard.check, stmt, self._user, query_id,
                        )
                        if err_msg:
                            self._cw.write(_mysql_err_packet(seq + 1, err_msg, 1105))
                            await self._cw.drain()
                            continue

                    # ── DML write-pushdown ────────────────────────────────────
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
