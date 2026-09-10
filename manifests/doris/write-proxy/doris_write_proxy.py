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
  2. The statement is submitted directly to the Spark standalone REST API.
  3. The proxy polls Spark until the job reaches a terminal state.
  4. On SUCCESS  → returns a MySQL OK packet to the client (rows affected = 0).
  5. On FAILURE  → returns a MySQL ERR packet with the Spark error message.

The client never sees a "not writable" error from Doris — the write either
succeeds (Spark executed it) or fails with a meaningful Spark error.

Local Doris DML (internal tables, no managed catalog prefix) flows through
to Doris unchanged and the client receives Doris's native response.

Environment variables
---------------------
  DORIS_HOST          Doris FE host         (default: 127.0.0.1)
  DORIS_PORT          Doris FE MySQL port   (default: 9030)
  LISTEN_HOST         Bind address          (default: 0.0.0.0)
  LISTEN_PORT         Proxy listen port     (default: 9040)
  SPARK_REST_URL      Spark REST endpoint   (default: http://spark-master-svc.prod.svc.cluster.local:6066)
  SPARK_MASTER_URL    spark:// master URL   (default: spark://spark-master-internal.prod.svc.cluster.local:17077)
  SPARK_JOB_TIMEOUT_S Seconds to wait       (default: 300)
  SPARK_POLL_INTERVAL_S Poll cadence        (default: 5)
  ADDR / BAO_ADDR     OpenBao address       (default: http://openbao.prod.svc.cluster.local:8200)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import struct
import time
import urllib.request
from typing import Tuple

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

SPARK_REST_URL      = os.environ.get(
    "SPARK_REST_URL",
    "http://spark-master-svc.prod.svc.cluster.local:6066",
)
SPARK_MASTER_URL    = os.environ.get(
    "SPARK_MASTER_URL",
    "spark://spark-master-internal.prod.svc.cluster.local:17077",
)
SPARK_JOB_TIMEOUT_S   = int(os.environ.get("SPARK_JOB_TIMEOUT_S",   "300"))
SPARK_POLL_INTERVAL_S = int(os.environ.get("SPARK_POLL_INTERVAL_S", "5"))

BAO_ADDR            = os.environ.get("ADDR") or os.environ.get("BAO_ADDR",
    "http://openbao.prod.svc.cluster.local:8200")
_BAO_K8S_SA_JWT     = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_BAO_ROLE           = os.environ.get("BAO_ROLE", "platform-secrets-read")
_PATH_S3            = "secret/data/platform/s3"

def _load_s3_creds() -> tuple[str, str]:
    """Load S3 access/secret keys from OpenBao at startup (stdlib only)."""
    # Try K8s SA JWT first
    if os.path.exists(_BAO_K8S_SA_JWT):
        with open(_BAO_K8S_SA_JWT) as fh:
            jwt = fh.read().strip()
        payload = json.dumps({"role": _BAO_ROLE, "jwt": jwt}).encode()
        req = urllib.request.Request(
            f"{BAO_ADDR}/v1/auth/kubernetes/login",
            data=payload, headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            token = json.loads(resp.read())["auth"]["client_token"]
    else:
        token = os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN", "")

    req = urllib.request.Request(
        f"{BAO_ADDR}/v1/{_PATH_S3}",
        headers={"X-Vault-Token": token}, method="GET",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    s3 = data.get("data", {}).get("data", data.get("data", {}))
    return s3["access_key"], s3["secret_key"]

# Load S3 credentials at module import time (once per process).
try:
    _S3_KEY, _S3_SECRET = _load_s3_creds()
    logger.info("S3 credentials loaded from OpenBao for s3a:// script fetch.")
except Exception as _e:
    logger.warning("Could not load S3 creds from OpenBao (%s) — s3a:// fetch may fail.", _e)
    _S3_KEY  = os.environ.get("S3_ACCESS_KEY", "")
    _S3_SECRET = os.environ.get("S3_SECRET_KEY", "")

# Path to the PySpark write script.
# In client mode the driver runs inside THIS pod, so the path must exist
# locally here — /app/spark_iceberg_write.py is baked into the image.
# Override via SPARK_WRITE_SCRIPT env var if the path changes.
_SPARK_WRITE_SCRIPT = os.environ.get(
    "SPARK_WRITE_SCRIPT",
    "/app/spark_iceberg_write.py",
)

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

# Regex: DML verb at start of statement
_DML_RE = re.compile(
    r"^\s*(insert\s+(?:into|overwrite)|update|delete(?:\s+from)?|merge\s+into)\b",
    re.IGNORECASE,
)

# ─────────────────────────────────────────────────────────────────────────────
# MySQL packet helpers
# ─────────────────────────────────────────────────────────────────────────────
# MySQL packets: 3-byte length (LE) + 1-byte sequence number + payload.

def _pack_packet(payload: bytes, seq: int) -> bytes:
    return struct.pack("<I", len(payload))[:3] + bytes([seq]) + payload


def _mysql_ok_packet(seq: int, affected_rows: int = 0) -> bytes:
    """Minimal MySQL OK packet (no session state, no warnings)."""
    def _lenc(n: int) -> bytes:
        if n < 251:
            return bytes([n])
        if n < 65536:
            return b"\xfc" + struct.pack("<H", n)
        return b"\xfd" + struct.pack("<I", n)[:3]

    payload = b"\x00" + _lenc(affected_rows) + _lenc(0) + b"\x00\x00" + b"\x00\x00"
    return _pack_packet(payload, seq)


def _mysql_err_packet(seq: int, message: str, error_code: int = 2000) -> bytes:
    """Minimal MySQL ERR packet."""
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
    """Read one MySQL packet; return (sequence_number, payload)."""
    header = await reader.readexactly(4)
    length = struct.unpack("<I", header[:3] + b"\x00")[0]
    seq    = header[3]
    payload = await reader.readexactly(length) if length else b""
    return seq, payload


async def _read_all_response(reader: asyncio.StreamReader) -> list[Tuple[int, bytes]]:
    """
    Read a complete MySQL response (one or more packets) from Doris.
    Stops after an OK (0x00), ERR (0xFF), or EOF (0xFE with len<9) packet,
    or after draining a result-set (column-defs + EOF + rows + EOF).
    """
    packets: list[Tuple[int, bytes]] = []
    # First packet determines the response type.
    seq, payload = await _read_packet(reader)
    packets.append((seq, payload))
    if not payload:
        return packets
    first_byte = payload[0]

    # OK or ERR — single packet response.
    if first_byte == 0x00 or first_byte == 0xFF:
        return packets

    # EOF (0xFE, len < 9) — single packet.
    if first_byte == 0xFE and len(payload) < 9:
        return packets

    # Otherwise this is a result-set:
    # column count (length-encoded int) → N column-def packets → EOF → row packets → EOF
    col_count = payload[0]  # simplified: works for count < 251
    for _ in range(col_count):
        seq, pkt = await _read_packet(reader)
        packets.append((seq, pkt))
    # EOF after column defs
    seq, pkt = await _read_packet(reader)
    packets.append((seq, pkt))
    # Row data packets until EOF or OK
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

def _catalog_and_parts(stmt: str) -> Tuple[str | None, str | None, str | None]:
    """
    If stmt is a DML against a managed catalog, return (catalog, db, table).
    Otherwise return (None, None, None).

    Handles fully-qualified references:
      catalog.db.table
      `catalog`.`db`.`table`
    """
    if not _DML_RE.match(stmt):
        return None, None, None

    # Extract the target table reference — the token after the DML verb keyword.
    # INSERT INTO / INSERT OVERWRITE → token[2], UPDATE → token[1],
    # DELETE FROM → token[2], DELETE → token[1], MERGE INTO → token[2].
    tokens = stmt.split()
    verb = tokens[0].upper()
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

    # Strip backticks
    ref = ref.strip("`").rstrip(";,(")
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
# Spark submission via spark-submit (synchronous, runs in executor thread)
# ─────────────────────────────────────────────────────────────────────────────
import subprocess

# spark-submit binary — must be present in the proxy image or on PATH.
# We invoke it in client mode from inside the pod so the driver runs here
# and logs are captured directly.  The Spark REST API does not support
# PySpark cluster-mode submission (DriverWrapper requires a non-empty Java
# mainClass; Python scripts need PythonRunner which is only wired by
# spark-submit, not the REST endpoint).
_SPARK_SUBMIT  = os.environ.get("SPARK_SUBMIT_BIN", "spark-submit")

def _spark_submit_and_wait(catalog: str, db: str, table: str, stmt: str) -> Tuple[bool, str]:
    """
    Execute spark_iceberg_write.py via spark-submit (client mode, blocking).
    Returns (success: bool, message: str).
    Runs in a thread-pool executor so it does not block the asyncio event loop.
    """
    warehouse = MANAGED_CATALOGS[catalog]
    job_args  = json.dumps({
        "catalog":   catalog,
        "warehouse": warehouse,
        "db":        db,
        "table":     table,
        "stmt":      stmt,
    })

    # Iceberg + AWS JARs are not bundled in the pyspark pip package —
    # they live on the Spark cluster nodes at /opt/spark/jars/.
    # Pass them via --jars so the local SparkSession can load IcebergCatalog.
    _SPARK_MASTER_HTTP = os.environ.get(
        "SPARK_MASTER_HTTP", "http://spark-master-svc.prod.svc.cluster.local:8080"
    )
    _ICEBERG_JARS = ",".join([
        f"{_SPARK_MASTER_HTTP}/static/../jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar",
        f"{_SPARK_MASTER_HTTP}/static/../jars/iceberg-aws-bundle-1.9.2.jar",
        f"{_SPARK_MASTER_HTTP}/static/../jars/hadoop-aws-3.3.4.jar",
        f"{_SPARK_MASTER_HTTP}/static/../jars/aws-java-sdk-bundle-1.12.262.jar",
        # Gluten+Velox native execution — same bundle used by all cluster jobs.
        # GlutenPlugin is registered via spark.plugins in spark_iceberg_write.py.
        f"{_SPARK_MASTER_HTTP}/static/../jars/gluten-velox-bundle-spark3.5_2.12-centos_7_x86_64-1.2.0.jar",
    ])

    cmd = [
        _SPARK_SUBMIT,
        "--master",      SPARK_MASTER_URL,
        "--deploy-mode", "client",
        "--name",        f"doris-write-proxy-{catalog}-{table}",
        "--jars",        _ICEBERG_JARS,
        _SPARK_WRITE_SCRIPT,
        job_args,
    ]

    env = os.environ.copy()
    env["ADDR"] = BAO_ADDR

    logger.info("WriteProxy: spark-submit %s.%s.%s", catalog, db, table)
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=SPARK_JOB_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        return False, f"spark-submit timed out after {SPARK_JOB_TIMEOUT_S}s"
    except FileNotFoundError:
        return False, f"spark-submit not found at '{_SPARK_SUBMIT}' — set SPARK_SUBMIT_BIN env var"

    # Log the driver output for observability
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            logger.info("WriteProxy [spark stdout]: %s", line)
    if result.stderr:
        for line in result.stderr.strip().splitlines()[-20:]:
            logger.info("WriteProxy [spark stderr]: %s", line)

    if result.returncode == 0:
        logger.info("WriteProxy: %s.%s.%s spark-submit FINISHED.", catalog, db, table)
        return True, "Write succeeded via spark-submit"
    else:
        # Extract last meaningful error line from stderr
        err_lines = [l for l in result.stderr.strip().splitlines() if l.strip()]
        last_err  = err_lines[-1] if err_lines else "unknown error"
        logger.error("WriteProxy: %s.%s.%s spark-submit FAILED (rc=%d): %s",
                     catalog, db, table, result.returncode, last_err)
        return False, f"spark-submit failed (rc={result.returncode}): {last_err}"


# ─────────────────────────────────────────────────────────────────────────────
# Per-connection handler
# ─────────────────────────────────────────────────────────────────────────────

class ProxyConnection:
    """
    Manages one client ↔ proxy ↔ Doris connection triple.

    Handshake phase:
      - Doris sends ServerHandshake → proxy forwards to client.
      - Client sends HandshakeResponse → proxy forwards to Doris.
      - Doris sends OK/ERR → proxy forwards to client.

    Command phase (loop):
      - Client sends COM_QUERY or other command.
      - If COM_QUERY with DML against a managed catalog:
          → submit to Spark, wait, return OK or ERR to client (Doris not contacted).
      - Otherwise:
          → forward to Doris, stream full response back to client.
    """

    # COM_QUERY type byte
    _COM_QUERY = 0x03

    def __init__(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._cr = client_reader
        self._cw = client_writer
        self._loop = loop
        peer = client_writer.get_extra_info("peername", ("?", 0))
        self._peer = f"{peer[0]}:{peer[1]}"

    async def run(self) -> None:
        logger.info("WriteProxy: client connected from %s", self._peer)
        doris_reader: asyncio.StreamReader | None = None
        doris_writer: asyncio.StreamWriter | None = None
        try:
            doris_reader, doris_writer = await asyncio.open_connection(DORIS_HOST, DORIS_PORT)

            # ── Handshake ───────────────────────────────────────────────────
            # 1. Doris → client: ServerHandshake
            seq, handshake = await _read_packet(doris_reader)
            self._cw.write(_pack_packet(handshake, seq))
            await self._cw.drain()

            # 2. Client → Doris: HandshakeResponse
            seq, auth_resp = await _read_packet(self._cr)
            doris_writer.write(_pack_packet(auth_resp, seq))
            await doris_writer.drain()

            # 3. Doris → client: OK or ERR (auth result)
            auth_packets = await _read_all_response(doris_reader)
            for s, p in auth_packets:
                self._cw.write(_pack_packet(p, s))
            await self._cw.drain()

            # If auth failed (first byte 0xFF), close.
            if auth_packets and auth_packets[0][1] and auth_packets[0][1][0] == 0xFF:
                logger.warning("WriteProxy: auth failed for %s", self._peer)
                return

            logger.info("WriteProxy: %s authenticated OK.", self._peer)

            # ── Command loop ─────────────────────────────────────────────────
            while True:
                try:
                    seq, payload = await _read_packet(self._cr)
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    break  # client disconnected

                if not payload:
                    break

                cmd = payload[0]

                # COM_QUIT (0x01)
                if cmd == 0x01:
                    doris_writer.write(_pack_packet(payload, seq))
                    await doris_writer.drain()
                    break

                # COM_QUERY (0x03) — inspect the SQL
                if cmd == self._COM_QUERY:
                    stmt = payload[1:].decode("utf-8", errors="replace").strip()
                    catalog, db, table = _catalog_and_parts(stmt)

                    if catalog:
                        # ── Managed catalog DML → Spark ──────────────────────
                        logger.info(
                            "WriteProxy: intercepted %s.%s.%s DML from %s — routing to Spark.",
                            catalog, db, table, self._peer,
                        )
                        # Run the blocking Spark call in a thread executor
                        ok, message = await self._loop.run_in_executor(
                            None,
                            _spark_submit_and_wait,
                            catalog, db, table, stmt,
                        )
                        if ok:
                            self._cw.write(_mysql_ok_packet(seq + 1))
                        else:
                            self._cw.write(_mysql_err_packet(seq + 1, message))
                        await self._cw.drain()
                        continue  # do NOT send to Doris

                # ── All other statements → forward to Doris ──────────────────
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
    logger.info(
        "Doris Write Proxy starting. "
        "listen=%s:%d  doris=%s:%d  spark=%s",
        LISTEN_HOST, LISTEN_PORT, DORIS_HOST, DORIS_PORT, SPARK_REST_URL,
    )
    logger.info(
        "Managed catalogs: %s",
        ", ".join(MANAGED_CATALOGS.keys()),
    )
    server = await asyncio.start_server(_handle, LISTEN_HOST, LISTEN_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
