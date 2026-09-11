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

    # ── Dynamic allocation — release executors when idle ─────────────────────
    # This is the KEY setting for a persistent session: executors are acquired
    # when a SQL statement arrives and released ~idle_timeout seconds after it
    # completes, so workers are free between INSERTs.
    # Without this, static executors hold worker memory permanently, causing
    # "Initial job has not accepted any resources" on the next INSERT because
    # minRegisteredResourcesRatio cannot be met.
    conf.set("spark.dynamicAllocation.enabled",                    "true")
    conf.set("spark.dynamicAllocation.shuffleTracking.enabled",    "true")  # no ext shuffle svc needed
    conf.set("spark.dynamicAllocation.minExecutors",               "0")     # release all when idle
    conf.set("spark.dynamicAllocation.maxExecutors",               "4")     # cap at 4
    conf.set("spark.dynamicAllocation.initialExecutors",           "0")     # don't pre-allocate
    conf.set("spark.dynamicAllocation.executorIdleTimeout",        "30s")   # release after 30s idle
    conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout",  "60s")   # cached data held 60s

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

    # ── Public ─────────────────────────────────────────────────────────────

    def start_background_init(self) -> None:
        """Kick off SparkSession init in a daemon thread. Returns immediately."""
        t = threading.Thread(target=self._init_spark, daemon=True, name="spark-init")
        t.start()

    def execute(self, catalog: str, db: str, stmt: str) -> Tuple[bool, str]:
        """
        Execute a single DML statement in the persistent SparkSession.
        Blocks until Spark is ready (at most SPARK_INIT_TIMEOUT_S seconds).
        Returns (success, message).
        """
        if not self._ready.wait(timeout=SPARK_INIT_TIMEOUT_S):
            return False, "SparkSession failed to initialise within timeout"
        if self._error:
            return False, f"SparkSession unavailable: {self._error}"

        with self._lock:
            t0 = time.time()
            try:
                self._spark.sql(f"USE {catalog}.{db}")
                result = self._spark.sql(stmt)
                # Materialise the result to trigger execution and get row count
                rows = result.count() if result is not None else 0
                elapsed = time.time() - t0
                logger.info(
                    "Spark SQL SUCCESS: %s.%s elapsed=%.2fs rows=%d",
                    catalog, db, elapsed, rows,
                )
                return True, f"Write succeeded (rows={rows}, elapsed={elapsed:.2f}s)"
            except Exception as exc:
                elapsed = time.time() - t0
                # py4j wraps Java exceptions — unwrap to get the real Spark error.
                cause = getattr(exc, "java_exception", None)
                if cause is not None:
                    msg = str(cause).split("\n")[0][:400]
                else:
                    msg = str(exc).split("\n")[0][:400]
                logger.error(
                    "Spark SQL FAILED: %s.%s elapsed=%.2fs error=%s",
                    catalog, db, elapsed, msg,
                )
                return False, msg

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
            doris_writer.write(_pack_packet(auth_resp, seq))
            await doris_writer.drain()

            auth_packets = await _read_all_response(doris_reader)
            for s, p in auth_packets:
                self._cw.write(_pack_packet(p, s))
            await self._cw.drain()

            if auth_packets and auth_packets[0][1] and auth_packets[0][1][0] == 0xFF:
                logger.warning("WriteProxy: auth failed for %s", self._peer)
                return

            logger.info("WriteProxy: %s authenticated OK.", self._peer)

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
                            catalog, db, stmt,
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
