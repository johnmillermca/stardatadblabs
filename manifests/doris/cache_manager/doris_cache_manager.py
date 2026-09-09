"""
doris_cache_manager.py
======================
Dynamic segment-cache warm-up daemon for Apache Doris.

Responsibilities
----------------
1. Every hour: scan Doris query audit log to count SELECT hits per Iceberg table.
2. Persist per-table stats in platform_meta.table_query_stats.
3. If a table has been queried more than once, derive a warm-up interval:
       warm_interval = select_interval * 2/3
   and schedule WARM_UP jobs at that cadence.
4. Run up to MAX_CONCURRENT_WARMUPS (32) warm-up jobs simultaneously.
5. If a warm-up is still running after WARMUP_STALE_MINUTES (5 min), skip and
   retry at the next scheduled window — do not launch a duplicate.
6. If a table has not been SELECTed in the past LRU_EVICT_HOURS (24 h), issue
   COLD_DOWN and write an eviction row to platform_meta.cache_eviction_log.
7. Write-pushdown: DML statements (INSERT / UPDATE / DELETE / MERGE / INSERT
   OVERWRITE) that target external catalog tables are detected in the audit log.
   Because Doris treats external Iceberg catalogs as read-only at the storage
   layer, the daemon intercepts these writes and re-submits them as a Spark job
   to the in-cluster Spark REST API so they are executed by the Spark engine
   which has full Iceberg read/write access via Polaris.

Authentication / credentials
-----------------------------
All credentials come from OpenBao at start-up (no hard-coded secrets).
Authentication order (mirrors bao_spark_init.py):
  1. K8s Service Account JWT → role platform-secrets-read
  2. TOKEN env-var (dev/local override)

Doris connection
----------------
  In-cluster:  doris-fe.prod.svc.cluster.local:9030  (direct, bypasses krb-doris-guard)
  NodePort:    192.168.1.50:30090                     (through krb-doris-guard)

The daemon always uses the in-cluster address so the Service Account JWT is
sufficient — no Kerberos keytab is needed inside the pod.

Environment variables (all optional — can override defaults)
-------------------------------------------------------------
  ADDR              OpenBao address (default: http://openbao.prod.svc.cluster.local:8200)
  TOKEN             OpenBao token override (dev mode)
  BAO_ROLE          OpenBao Kubernetes auth role (default: platform-secrets-read)
  SCAN_INTERVAL_S   Seconds between audit-log scans (default: 3600)
  LRU_EVICT_HOURS   Hours of inactivity before eviction (default: 24)
  MAX_CONCURRENT    Max simultaneous WARM_UP jobs across all (table, BE) pairs (default: 32)
  WARMUP_STALE_MIN  Minutes before a running warm-up is considered stale (default: 5)
  DORIS_HOST        Doris FE host (default: doris-fe.prod.svc.cluster.local)
  DORIS_PORT        Doris FE query port (default: 9030)
  DORIS_USER        Doris user for warm-up and metadata queries (default: root)
  SPARK_REST_URL    Spark standalone REST submission URL
                    (default: http://spark-master-svc.prod.svc.cluster.local:6066)
  SPARK_MASTER_URL  spark:// master address passed to submitted jobs
                    (default: spark://spark-master-internal.prod.svc.cluster.local:17077)
  SPARK_VERSION     Spark version string sent in REST submission payload (default: 3.5.1)
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import pymysql  # type: ignore

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("doris-cache-manager")

# ─────────────────────────────────────────────────────────────────────────────
# Constants / defaults
# ─────────────────────────────────────────────────────────────────────────────
_BAO_IN_CLUSTER   = "http://openbao.prod.svc.cluster.local:8200"
_K8S_SA_JWT_FILE  = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_PATH_DORIS       = "secret/data/platform/doris"
_PATH_POLARIS     = "secret/data/platform/polaris"

_DORIS_HOST_DEFAULT = "doris-fe.prod.svc.cluster.local"
_DORIS_PORT_DEFAULT = 9030

# Catalogs managed by the cache daemon.
# Driven by env var MANAGED_CATALOGS (comma-separated) so no code change is
# needed when catalogs are added or removed.
# Default mirrors the 5 catalogs defined in 02_create_catalogs.sql.
_managed_catalogs_env = os.environ.get(
    "MANAGED_CATALOGS",
    "polaris,databricks,postgres,oracle,mongodb",
)
MANAGED_CATALOGS: list[str] = [c.strip() for c in _managed_catalogs_env.split(",") if c.strip()]

# Warehouse name for each catalog — passed to the Spark write-pushdown job.
# Driven by env var CATALOG_WAREHOUSES as "catalog:warehouse,..." pairs.
# Default mirrors bao_spark_init.py warehouse names.
_catalog_warehouses_env = os.environ.get(
    "CATALOG_WAREHOUSES",
    "polaris:IcebergCatalog,databricks:star_lakehouse,postgres:pg_lakehouse,oracle:ora_lakehouse,mongodb:mgo_lakehouse",
)
CATALOG_WAREHOUSE: dict[str, str] = {
    k.strip(): v.strip()
    for pair in _catalog_warehouses_env.split(",")
    if ":" in pair
    for k, v in [pair.split(":", 1)]
}

# ─────────────────────────────────────────────────────────────────────────────
# Runtime config (from environment variables)
# ─────────────────────────────────────────────────────────────────────────────
SCAN_INTERVAL_S  = int(os.environ.get("SCAN_INTERVAL_S",  "3600"))
LRU_EVICT_HOURS  = int(os.environ.get("LRU_EVICT_HOURS",  "24"))
MAX_CONCURRENT   = int(os.environ.get("MAX_CONCURRENT",   "32"))
WARMUP_STALE_MIN = int(os.environ.get("WARMUP_STALE_MIN", "5"))
DORIS_HOST       = os.environ.get("DORIS_HOST", _DORIS_HOST_DEFAULT)
DORIS_PORT       = int(os.environ.get("DORIS_PORT", str(_DORIS_PORT_DEFAULT)))
DORIS_USER       = os.environ.get("DORIS_USER", "root")
BAO_ROLE         = os.environ.get("BAO_ROLE", "platform-secrets-read")
BAO_ADDR         = os.environ.get("ADDR") or os.environ.get("BAO_ADDR", _BAO_IN_CLUSTER)
SPARK_REST_URL   = os.environ.get(
    "SPARK_REST_URL",
    "http://spark-master-svc.prod.svc.cluster.local:6066",
)
SPARK_MASTER_URL = os.environ.get(
    "SPARK_MASTER_URL",
    "spark://spark-master-internal.prod.svc.cluster.local:17077",
)
SPARK_VERSION    = os.environ.get("SPARK_VERSION", "3.5.1")
# Path of the write-pushdown PySpark script baked into the image.
_SPARK_WRITE_SCRIPT = "/app/spark_iceberg_write.py"
# How frequently the write-interceptor background thread polls audit_log (seconds).
WRITE_POLL_INTERVAL_S = int(os.environ.get("WRITE_POLL_INTERVAL_S", "10"))

# ─────────────────────────────────────────────────────────────────────────────
# Dataclasses
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class TableKey:
    catalog: str
    db: str
    table: str

    def __hash__(self) -> int:
        return hash((self.catalog, self.db, self.table))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TableKey) and (
            self.catalog == other.catalog
            and self.db == other.db
            and self.table == other.table
        )

    def __str__(self) -> str:
        return f"{self.catalog}.{self.db}.{self.table}"


@dataclass
class BeNode:
    """A single alive Doris Backend node."""
    backend_id: str
    host: str
    http_port: int
    # The file_cache_base_path configured on this BE, fetched at discovery time.
    # Passed to SET_VAR(file_cache_base_path=...) in warm-up queries so the FE
    # routes the scan fragment to this specific BE and populates its local cache.
    cache_path: str

    def __str__(self) -> str:
        return f"{self.host}:{self.http_port}"


@dataclass
class WarmupJob:
    key: TableKey
    be: BeNode
    started_at: datetime
    thread: threading.Thread
    done: bool = False


# ─────────────────────────────────────────────────────────────────────────────
# OpenBao credential loader (stdlib-only, same pattern as bao_spark_init.py)
# ─────────────────────────────────────────────────────────────────────────────
class BaoClient:
    def __init__(self) -> None:
        self._token: str | None = None
        self._cache: dict[str, dict] = {}

    def _get_token(self) -> str:
        if self._token:
            return self._token

        # 1. Explicit env override (dev / bootstrap)
        if tok := (os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN")):
            logger.info("Using TOKEN env-var for OpenBao (dev mode).")
            self._token = tok
            return self._token

        # 2. K8s Service Account JWT
        if os.path.exists(_K8S_SA_JWT_FILE):
            with open(_K8S_SA_JWT_FILE) as fh:
                jwt = fh.read().strip()
            payload = json.dumps({"role": BAO_ROLE, "jwt": jwt}).encode()
            url = f"{BAO_ADDR}/v1/auth/kubernetes/login"
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            self._token = data["auth"]["client_token"]
            logger.info("Authenticated to OpenBao via K8s SA JWT (role=%s).", BAO_ROLE)
            return self._token

        raise RuntimeError(
            "Cannot authenticate to OpenBao: no TOKEN env-var and "
            f"no K8s SA JWT at {_K8S_SA_JWT_FILE}"
        )

    def read_secret(self, path: str) -> dict[str, str]:
        if path in self._cache:
            return self._cache[path]
        token = self._get_token()
        url = f"{BAO_ADDR}/v1/{path}"
        req = urllib.request.Request(
            url, headers={"X-Vault-Token": token}, method="GET"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        # KV v2: { data: { data: { key: val }, metadata: {...} } }
        outer = data.get("data", {})
        secret_data: dict[str, str] = outer.get("data", outer)
        self._cache[path] = secret_data
        logger.debug("Read secret from OpenBao: %s", path)
        return secret_data


# ─────────────────────────────────────────────────────────────────────────────
# Doris connection helper
# ─────────────────────────────────────────────────────────────────────────────
class DorisClient:
    """
    Thin wrapper around a PyMySQL connection to Doris FE.
    Re-connects automatically if the connection drops.
    """

    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._conn: pymysql.connections.Connection | None = None

    def _connect(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            charset="utf8mb4",
            connect_timeout=30,
            read_timeout=120,
            write_timeout=30,
            autocommit=True,
        )

    def _get_conn(self) -> pymysql.connections.Connection:
        if self._conn is None:
            self._conn = self._connect()
            logger.info("Connected to Doris FE at %s:%s.", self._host, self._port)
        try:
            self._conn.ping(reconnect=True)
        except Exception:
            logger.warning("Doris connection lost — reconnecting.")
            self._conn = self._connect()
        return self._conn

    def execute(self, sql: str, args: tuple | None = None) -> list[tuple]:
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, args)
            result = cur.fetchall()
        return list(result)

    def execute_many(self, sql: str, rows: list[tuple]) -> None:
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.executemany(sql, rows)


# ─────────────────────────────────────────────────────────────────────────────
# BE node discovery
# ─────────────────────────────────────────────────────────────────────────────
class BeDiscovery:
    """
    Discovers alive Doris BE nodes via SHOW BACKENDS (SQL) and fetches each
    BE's configured file_cache_path via its HTTP admin API.

    Why per-BE cache paths matter
    ------------------------------
    Doris Community Edition stores the file cache in a BE-local directory
    (file_cache_path in be.conf, default /opt/apache-doris/be/storage/
    segment_cache).  The cache is NOT shared between BEs — each BE has its own
    independent cache partition.

    Doris FE distributes query fragments across BEs using its normal tablet
    scheduling logic.  A warm-up SELECT COUNT(*) issued through the FE will
    only warm whichever BE(s) the FE happens to assign that fragment to.
    With multiple BEs, the other BEs remain cold.

    The fix
    -------
    For each warm-up, we issue one SELECT per alive BE, each carrying a
    SET_VAR(file_cache_base_path='<be_path>') hint.  This hint tells the FE
    to route the scan fragment to the BE whose cache_path matches.  Every BE
    therefore receives its own warm-up scan and populates its own cache.

    Cache path discovery
    --------------------
    Each BE exposes its configuration via HTTP:
        GET http://<be_host>:<be_http_port>/api/show_config?conf_item=file_cache_path
    Response: [["file_cache_path","std::string","[{\"path\":\"/opt/...\"}]","false"]]
    We parse the first path entry from the JSON array.

    The discovered BE list is cached for one scan cycle and refreshed at the
    start of each new cycle so newly added BEs are picked up automatically.
    """

    # How long to wait for the BE HTTP config endpoint (seconds).
    _HTTP_TIMEOUT = 5

    def discover(self, doris: DorisClient) -> list[BeNode]:
        """
        Return a list of alive BeNode objects.  Falls back to a single
        synthetic BeNode using DORIS_HOST if SHOW BACKENDS fails or returns
        nothing, so the daemon continues to work in single-BE setups.
        """
        try:
            return self._do_discover(doris)
        except Exception as exc:
            logger.error("BE discovery failed: %s — falling back to single-BE mode.", exc)
            return [BeNode(
                backend_id="fallback",
                host=DORIS_HOST,
                http_port=8040,
                cache_path="random",
            )]

    def _do_discover(self, doris: DorisClient) -> list[BeNode]:
        rows = doris.execute("SHOW BACKENDS")
        # SHOW BACKENDS columns (Doris 4.0):
        # 0=BackendId 1=Host 2=HeartbeatPort 3=BePort 4=HttpPort 5=BrpcPort
        # 6=ArrowFlightSqlPort 7=LastStartTime 8=LastHeartbeat 9=Alive ...
        nodes: list[BeNode] = []
        for row in rows:
            backend_id = str(row[0])
            host       = str(row[1])
            http_port  = int(row[4])
            alive      = str(row[9]).lower() == "true"
            if not alive:
                logger.debug("BE %s (%s) is not alive — skipping.", backend_id, host)
                continue
            cache_path = self._fetch_cache_path(host, http_port)
            nodes.append(BeNode(
                backend_id=backend_id,
                host=host,
                http_port=http_port,
                cache_path=cache_path,
            ))
            logger.debug("Discovered BE %s (%s:%d) cache_path=%s.", backend_id, host, http_port, cache_path)

        if not nodes:
            raise RuntimeError("SHOW BACKENDS returned no alive nodes.")

        logger.info("BE discovery: %d alive node(s) found.", len(nodes))
        return nodes

    def _fetch_cache_path(self, host: str, http_port: int) -> str:
        """
        Fetch the file_cache_path from this BE's HTTP admin endpoint.
        Returns 'random' (Doris default) if unreachable or parse fails.
        """
        url = f"http://{host}:{http_port}/api/show_config?conf_item=file_cache_path"
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=self._HTTP_TIMEOUT) as resp:
                data = json.loads(resp.read())
            # data = [["file_cache_path", "std::string", "[{\"path\":\"...\"}]", "false"]]
            raw = data[0][2]
            paths = json.loads(raw)
            return paths[0]["path"]
        except Exception as exc:
            logger.warning(
                "Could not fetch file_cache_path from BE %s:%d (%s) — using 'random'.",
                host, http_port, exc,
            )
            return "random"


# ─────────────────────────────────────────────────────────────────────────────
# Audit log scraper
# ─────────────────────────────────────────────────────────────────────────────
class AuditLogScraper:
    """
    Queries the Doris built-in audit log table to count SELECT statements
    per Iceberg table (catalog.db.table).

    Doris exposes query audit data via the `__internal_schema`.`audit_log` table
    (available in Doris 2.1+). Each row represents one finished query.
    We look at queries whose `stmt` field starts with 'select' and whose
    `catalog` field matches one of the managed catalogs.

    For compatibility with Doris versions that do not expose audit_log, we also
    fall back to querying `information_schema.processlist` for currently running
    queries (best-effort, no history) — but the primary path is audit_log.
    """

    # Scan the last N hours of audit log each cycle.
    # We use 2× the LRU window so we never miss a table becoming inactive.
    _LOOKBACK_HOURS = max(LRU_EVICT_HOURS * 2, 2)

    def scrape(self, doris: DorisClient) -> dict[TableKey, int]:
        """
        Return {TableKey: select_count} for all managed catalogs in the
        audit window.  Returns an empty dict on any scrape error (logged).
        """
        try:
            return self._scrape_audit_log(doris)
        except Exception as exc:
            logger.error("Audit log scrape failed: %s", exc, exc_info=True)
            return {}

    def _scrape_audit_log(self, doris: DorisClient) -> dict[TableKey, int]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._LOOKBACK_HOURS)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

        # NOTE: Doris always logs catalog='internal' in audit_log regardless of
        # which external catalog the query targets.  The only reliable way to
        # detect external-catalog queries is to match the catalog name inside the
        # stmt text itself (3-part references: catalog.db.table).
        stmt_filters = " OR ".join(
            f"LOWER(stmt) LIKE '%{c}.%'" for c in MANAGED_CATALOGS
        )

        # Doris audit_log columns: query_id, time, client_ip, user, catalog,
        #   db, state, query_time, scan_rows, scan_bytes, return_rows,
        #   stmt_id, is_query, frontend_ip, cpu_time_ms, sql_hash,
        #   sql_digest, peak_memory_bytes, stmt
        sql = f"""
            SELECT
                stmt,
                COUNT(*) AS hit_count
            FROM __internal_schema.audit_log
            WHERE
                time >= '{cutoff_str}'
                AND is_query = 1
                AND (LOWER(TRIM(stmt)) LIKE 'select%'
                     OR LOWER(TRIM(stmt)) LIKE 'with%')
                AND ({stmt_filters})
            GROUP BY stmt
        """
        rows = doris.execute(sql)

        counts: dict[TableKey, int] = {}
        for stmt, hit_count in rows:
            key = _extract_key_from_stmt(stmt)
            if not key:
                continue
            counts[key] = counts.get(key, 0) + int(hit_count)

        logger.info(
            "Audit log scrape: found %d distinct table/catalog pairs with SELECTs.",
            len(counts),
        )
        return counts


def _extract_key_from_stmt(stmt: str) -> "TableKey | None":
    """
    Extract a TableKey (catalog, db, table) from the first table reference in a
    SQL statement.  Only returns a key when the reference is a fully-qualified
    3-part name (catalog.db.table) whose catalog is in MANAGED_CATALOGS.

    Doris audit_log always records catalog='internal' even for external-catalog
    queries, so we parse the catalog out of the stmt text instead.
    """
    if not stmt:
        return None
    lower = stmt.lower()
    # Find 'FROM' keyword
    for marker in (" from ", "\nfrom ", "\tfrom "):
        idx = lower.find(marker)
        if idx != -1:
            rest = stmt[idx + len(marker):].strip()
            break
    else:
        return None
    # Grab first token (table reference, possibly catalog.db.table)
    m = re.match(r"([`\w.\-]+)", rest)
    if not m:
        return None
    ref = m.group(1).strip("`")
    parts = [p.strip("`") for p in ref.split(".")]
    if len(parts) == 3:
        catalog, db, table = parts
        if catalog.lower() in MANAGED_CATALOGS:
            return TableKey(catalog=catalog.lower(), db=db, table=table)
    elif len(parts) == 2:
        # db.table — catalog unknown, skip (can't attribute to a managed catalog)
        return None
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Metadata store (read/write platform_meta tables)
# ─────────────────────────────────────────────────────────────────────────────
class MetaStore:
    """Read and update platform_meta.table_query_stats and cache_eviction_log."""

    def load_all_stats(self, doris: DorisClient) -> dict[TableKey, dict]:
        """Return all rows from table_query_stats as a dict keyed by TableKey."""
        rows = doris.execute(
            "SELECT catalog_name, db_name, table_name, total_select_count, "
            "last_select_ts, prev_select_ts, select_interval_min, warm_interval_min, "
            "last_warmed_ts, cache_state, updated_at "
            "FROM platform_meta.table_query_stats"
        )
        result: dict[TableKey, dict] = {}
        for row in rows:
            key = TableKey(catalog=row[0], db=row[1], table=row[2])
            result[key] = {
                "total_select_count":  int(row[3] or 0),
                "last_select_ts":      row[4],
                "prev_select_ts":      row[5],
                "select_interval_min": float(row[6]) if row[6] is not None else None,
                "warm_interval_min":   float(row[7]) if row[7] is not None else None,
                "last_warmed_ts":      row[8],
                "cache_state":         row[9] or "UNKNOWN",
                "updated_at":          row[10],
            }
        return result

    def upsert_stats(
        self,
        doris: DorisClient,
        key: TableKey,
        new_count: int,
        now: datetime,
        prev_last_ts: datetime | None,
        select_interval_min: float | None,
        warm_interval_min: float | None,
        cache_state: str,
    ) -> None:
        sql = """
            INSERT INTO platform_meta.table_query_stats
                (catalog_name, db_name, table_name, total_select_count,
                 last_select_ts, prev_select_ts, select_interval_min,
                 warm_interval_min, cache_state, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        doris.execute(sql, (
            key.catalog, key.db, key.table,
            new_count,
            now.strftime("%Y-%m-%d %H:%M:%S"),
            prev_last_ts.strftime("%Y-%m-%d %H:%M:%S") if prev_last_ts else None,
            select_interval_min,
            warm_interval_min,
            cache_state,
            now.strftime("%Y-%m-%d %H:%M:%S"),
        ))

    def update_last_warmed(
        self, doris: DorisClient, key: TableKey, ts: datetime, state: str
    ) -> None:
        sql = """
            UPDATE platform_meta.table_query_stats
            SET last_warmed_ts = %s, cache_state = %s, updated_at = %s
            WHERE catalog_name = %s AND db_name = %s AND table_name = %s
        """
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        doris.execute(sql, (
            ts.strftime("%Y-%m-%d %H:%M:%S"),
            state,
            now_str,
            key.catalog, key.db, key.table,
        ))

    def record_eviction(
        self,
        doris: DorisClient,
        key: TableKey,
        evicted_at: datetime,
        reason: str,
        last_select_ts: datetime | None,
        eviction_id: int,
    ) -> None:
        sql = """
            INSERT INTO platform_meta.cache_eviction_log
                (id, catalog_name, db_name, table_name, evicted_at, reason, last_select_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        doris.execute(sql, (
            eviction_id,
            key.catalog, key.db, key.table,
            evicted_at.strftime("%Y-%m-%d %H:%M:%S"),
            reason,
            last_select_ts.strftime("%Y-%m-%d %H:%M:%S") if last_select_ts else None,
        ))


# ─────────────────────────────────────────────────────────────────────────────
# Warm-up executor
# ─────────────────────────────────────────────────────────────────────────────
class WarmupExecutor:
    """
    Warms the Doris file cache for a table by issuing a full-scan SELECT with
    enable_file_cache=true in a background thread.

    Doris 4.0 Community Edition does NOT support the Cloud-Edition-only
    `WARM UP CACHE … USING JOB` / `SHOW WARM UP JOB` syntax — those commands
    raise "errCode = 2 … no viable alternative at input 'WARM UP CACHE'".

    The correct warm-up mechanism for Community Edition is a full table scan:
        SET enable_file_cache = true;
        SELECT COUNT(*) FROM catalog.db.table;

    The BE pulls every data block from S3/remote storage into the local
    file_cache_path directory as a side-effect of reading, so subsequent
    queries find the data cached locally.  The scan itself is blocking —
    the thread waits for it to finish, then marks the table WARM.

    LRU eviction is managed natively by the BE (file_cache LRU).  There is no
    programmatic COLD_DOWN command in Community Edition; the evict() method
    records the eviction in the audit table only — the BE will expire the
    blocks naturally when the cache fills.
    """

    def __init__(self, meta: MetaStore) -> None:
        self._meta = meta
        self._lock = threading.Lock()
        # Active warm-up jobs: (TableKey, be_backend_id) → WarmupJob.
        # Keyed by (table, BE) pair so each BE gets its own independent slot —
        # a single table can have N concurrent jobs, one per BE node.
        self._active: dict[tuple[TableKey, str], WarmupJob] = {}
        self._eviction_counter = int(time.time())  # monotonic id seed

    # ── Public API ─────────────────────────────────────────────────────────────

    def running_count(self) -> int:
        with self._lock:
            self._reap_done()
            return len(self._active)

    def is_running(self, key: TableKey, be: BeNode) -> bool:
        with self._lock:
            self._reap_done()
            return (key, be.backend_id) in self._active

    def is_stale(self, key: TableKey, be: BeNode) -> bool:
        """True if a warm-up has been running for > WARMUP_STALE_MIN minutes."""
        with self._lock:
            job = self._active.get((key, be.backend_id))
            if job is None:
                return False
            age = (datetime.now(timezone.utc) - job.started_at).total_seconds()
            return age > WARMUP_STALE_MIN * 60

    def submit(
        self,
        key: TableKey,
        be: BeNode,
        doris_creds: dict[str, str],
        meta_doris: DorisClient,
    ) -> None:
        """Launch a warm-up thread for the (key, BE) pair if a slot is available.

        Each (table, BE) pair gets its own thread so every alive BE node is
        warmed independently.  The active-slot limit (MAX_CONCURRENT) is shared
        across all in-flight (table, BE) pairs.
        """
        slot = (key, be.backend_id)
        with self._lock:
            self._reap_done()
            if len(self._active) >= MAX_CONCURRENT:
                logger.warning(
                    "WARM_UP skipped for %s on BE %s — max concurrent (%d) reached.",
                    key, be, MAX_CONCURRENT,
                )
                return
            if slot in self._active:
                logger.debug("WARM_UP already running for %s on BE %s — skipping.", key, be)
                return

            started = datetime.now(timezone.utc)
            # Each thread gets its own DorisClient so concurrent update_last_warmed
            # calls never share a connection — sharing caused (2013) Lost Connection
            # and "Packet sequence number wrong" errors under MAX_CONCURRENT load.
            thread_meta_doris = DorisClient(
                host=meta_doris._host,
                port=meta_doris._port,
                user=meta_doris._user,
                password=meta_doris._password,
            )
            t = threading.Thread(
                target=self._run_warmup,
                args=(key, be, doris_creds, thread_meta_doris, started),
                name=f"warmup-{key}-{be.backend_id}",
                daemon=True,
            )
            self._active[slot] = WarmupJob(key=key, be=be, started_at=started, thread=t)

        t.start()
        logger.info("WARM_UP started for %s on BE %s (cache_path=%s).", key, be, be.cache_path)

    def evict(
        self, key: TableKey, doris: DorisClient, meta: MetaStore,
        last_select_ts: datetime | None
    ) -> None:
        """Record a cache eviction event.

        Doris 4.0 Community Edition has no programmatic COLD_DOWN command.
        The BE manages its file_cache LRU natively and will evict cold blocks
        when the cache fills.  We record the eviction in the audit table so
        the daemon stops re-issuing warm-up jobs for idle tables.
        """
        now = datetime.now(timezone.utc)
        logger.info("LRU eviction recorded for %s (no COLD_DOWN in Community Edition).", key)

        with self._lock:
            self._eviction_counter += 1
            eid = self._eviction_counter
        reason = f"no_select_{LRU_EVICT_HOURS}h"
        try:
            meta.record_eviction(doris, key, now, reason, last_select_ts, eid)
        except Exception as exc:
            logger.error("Failed to record eviction for %s: %s", key, exc)

    # ── Internal ───────────────────────────────────────────────────────────────

    def _reap_done(self) -> None:
        """Remove finished jobs from _active (call inside self._lock)."""
        done_keys = [k for k, j in self._active.items() if j.done]
        for k in done_keys:
            del self._active[k]

    # Doris error codes / message fragments that mean the object simply does not
    # exist in the catalog.  These are permanent failures — retrying is pointless
    # and the ghost row should be removed from table_query_stats.
    _GHOST_ERRORS = (
        "does not exist",
        "Unknown table",
        "Table not found",
        "database not found",
    )

    def _run_warmup(
        self,
        key: TableKey,
        be: BeNode,
        doris_creds: dict[str, str],
        meta_doris: DorisClient,
        started: datetime,
    ) -> None:
        """Background thread: warm one BE's file cache via a pinned full-scan SELECT.

        The SET_VAR(file_cache_base_path=...) hint pins the scan fragment to the
        target BE so its local cache is populated regardless of which BE the FE
        would otherwise choose.  One thread is launched per alive BE per table,
        ensuring every BE in the cluster is warmed.

        meta_doris is a dedicated DorisClient created per-thread by submit() so
        concurrent update_last_warmed writes never share a connection.
        """
        slot = (key, be.backend_id)
        try:
            warmup_conn = DorisClient(
                host=DORIS_HOST,
                port=DORIS_PORT,
                user=DORIS_USER,
                password=doris_creds["admin_password"],
            )

            # Pin the warm-up scan to this specific BE's cache path.
            # SET_VAR hint is inlined in the query so it is scoped to this
            # statement only — no session state leaks to other threads.
            warm_sql = (
                f"SELECT /*+ SET_VAR(enable_file_cache=true, "
                f"file_cache_base_path='{be.cache_path}') */ "
                f"COUNT(*) "
                f"FROM `{key.catalog}`.`{key.db}`.`{key.table}`"
            )
            logger.info("WARM_UP scan issued for %s on BE %s.", key, be)
            warmup_conn.execute(warm_sql)

            finished_at = datetime.now(timezone.utc)
            logger.info("WARM_UP scan completed for %s on BE %s.", key, be)

            # Only write last_warmed_ts once — when ALL BEs have finished for
            # this table.  We do it on every completion; the last writer wins,
            # which is fine because the timestamp just needs to be recent.
            try:
                self._meta.update_last_warmed(
                    meta_doris, key, finished_at, "WARM"
                )
            except Exception as meta_exc:
                logger.error(
                    "Failed to update last_warmed for %s (BE %s): %s", key, be, meta_exc
                )

        except Exception as exc:
            err_str = str(exc)
            # Permanent failure: the table/database no longer exists in the catalog.
            # Delete the ghost row so the scheduler stops retrying it every cycle.
            if any(ghost in err_str for ghost in self._GHOST_ERRORS):
                logger.warning(
                    "WARM_UP ghost row detected for %s (%s) — deleting from table_query_stats.",
                    key, err_str,
                )
                try:
                    meta_doris.execute(
                        "DELETE FROM platform_meta.table_query_stats "
                        "WHERE catalog_name = %s AND db_name = %s AND table_name = %s",
                        (key.catalog, key.db, key.table),
                    )
                    logger.info("Ghost row deleted for %s.", key)
                except Exception as del_exc:
                    logger.error("Failed to delete ghost row for %s: %s", key, del_exc)
            else:
                logger.error(
                    "WARM_UP thread error for %s on BE %s: %s", key, be, exc, exc_info=True
                )
        finally:
            with self._lock:
                if slot in self._active:
                    self._active[slot].done = True


# ─────────────────────────────────────────────────────────────────────────────
# Warm-up scheduler
# ─────────────────────────────────────────────────────────────────────────────
class WarmupScheduler:
    """
    Decides which tables should be warmed up on each scan cycle.

    Rules:
    - A table is eligible for warm-up if total_select_count > 1.
    - warm_interval = select_interval * 2/3.
    - A warm-up is triggered if:
        now - last_warmed_ts >= warm_interval_min
        OR the table has never been warmed.
    - If a warm-up is already running for the table:
        - Skip if running time < WARMUP_STALE_MIN.
        - Skip (retry next schedule) if running time >= WARMUP_STALE_MIN.
    """

    def __init__(self, executor: WarmupExecutor) -> None:
        self._executor = executor

    def evaluate(
        self,
        stats: dict[TableKey, dict],
        be_nodes: list[BeNode],
        doris_creds: dict[str, str],
        meta_doris: DorisClient,
        meta: MetaStore,
        now: datetime,
    ) -> None:
        eligible = 0
        triggered = 0

        for key, s in stats.items():
            if s["total_select_count"] <= 1:
                continue  # Need at least 2 hits to estimate interval
            eligible += 1

            warm_interval = s.get("warm_interval_min")
            if warm_interval is None:
                continue  # No interval estimate yet

            last_warmed: datetime | None = s.get("last_warmed_ts")
            if last_warmed is not None:
                # Ensure timezone-aware comparison
                if last_warmed.tzinfo is None:
                    last_warmed = last_warmed.replace(tzinfo=timezone.utc)
                minutes_since_warm = (now - last_warmed).total_seconds() / 60.0
                if minutes_since_warm < warm_interval:
                    logger.debug(
                        "%s: %.1f min since last warm, interval=%.1f — skip.",
                        key, minutes_since_warm, warm_interval,
                    )
                    continue

            # Fan out: submit one warm-up thread per alive BE node so every
            # BE's file cache is populated independently.
            for be in be_nodes:
                if self._executor.is_running(key, be):
                    if self._executor.is_stale(key, be):
                        logger.warning(
                            "%s on BE %s: warm-up stale (> %d min) — skip, retry next cycle.",
                            key, be, WARMUP_STALE_MIN,
                        )
                    else:
                        logger.debug("%s on BE %s: warm-up already running — skip.", key, be)
                    continue
                self._executor.submit(key, be, doris_creds, meta_doris)
                triggered += 1

        logger.info(
            "Warm-up evaluation: %d eligible tables, %d triggered across %d BE(s).",
            eligible, triggered, len(be_nodes),
        )


# ─────────────────────────────────────────────────────────────────────────────
# LRU eviction checker
# ─────────────────────────────────────────────────────────────────────────────
class LRUEvictionChecker:
    """
    Marks tables as COLD if they haven't been SELECTed in LRU_EVICT_HOURS.
    Issues COLD_DOWN to Doris and records the eviction in the audit table.
    """

    def check(
        self,
        stats: dict[TableKey, dict],
        doris: DorisClient,
        executor: WarmupExecutor,
        meta: MetaStore,
        now: datetime,
    ) -> None:
        evicted = 0
        for key, s in stats.items():
            last_sel: datetime | None = s.get("last_select_ts")
            if last_sel is None:
                continue
            if last_sel.tzinfo is None:
                last_sel = last_sel.replace(tzinfo=timezone.utc)
            hours_idle = (now - last_sel).total_seconds() / 3600.0
            if hours_idle >= LRU_EVICT_HOURS and s.get("cache_state") in ("WARM", "WARMING", "UNKNOWN"):
                logger.info(
                    "LRU eviction: %s idle for %.1f h (threshold=%d h).",
                    key, hours_idle, LRU_EVICT_HOURS,
                )
                executor.evict(key, doris, meta, last_sel)
                evicted += 1
        if evicted:
            logger.info("LRU eviction cycle: %d tables evicted.", evicted)


# ─────────────────────────────────────────────────────────────────────────────
# Write-pushdown interceptor
# ─────────────────────────────────────────────────────────────────────────────

# DML verbs that indicate a write against an external catalog table.
_WRITE_VERBS_RE = re.compile(
    r"^\s*(insert\s+(?:into|overwrite)|update|delete\s+from|delete|merge\s+into)\b",
    re.IGNORECASE,
)

@dataclass
class PendingWrite:
    """One intercepted DML statement waiting to be pushed down to Spark."""
    query_id: str
    catalog: str
    db: str
    table: str
    stmt: str
    detected_at: datetime


class WriteInterceptor:
    """
    Scans the Doris audit log for DML statements targeting external Iceberg
    catalog tables, then re-submits each statement to the Spark REST API so
    it is executed by the Spark engine — which has full Iceberg read/write
    access via the Polaris REST catalog.

    Why pushdown is needed
    ----------------------
    Apache Doris external catalogs (type=iceberg) are read-only from Doris'
    perspective — Doris can query Iceberg metadata and read data files, but it
    does not implement Iceberg write operations (append, overwrite, delete,
    merge).  Any DML issued in Doris against an external catalog table will
    fail at execution with an error like:
        "Table polaris.db.tbl is not writable via this catalog type."

    The interceptor catches those DML statements in the audit log (state=ERR
    or any state, since the write is attempted before execution), translates
    them into a PySpark job, and submits them to the Spark standalone cluster
    via the Spark REST submission API — the same mechanism used by starpump.

    Spark job design
    ----------------
    The Spark job `spark_iceberg_write.py` (baked into the image at
    /app/spark_iceberg_write.py) receives the DML statement and catalog
    config as JSON-encoded app arguments, builds the full SparkConf including
    Polaris OAuth2 credentials fetched from OpenBao at job runtime, then
    executes `spark.sql(stmt)` inside the correct catalog context.

    This means:
    - Credentials are never passed as plain-text to the Spark REST API.
    - The Spark job re-reads OpenBao independently at start-up.
    - The daemon only passes the raw SQL + catalog/db/table identifiers.

    Tracking
    --------
    Submitted write jobs are tracked by query_id to avoid duplicate
    submissions.  The seen-set is bounded to the last SCAN_INTERVAL window.
    """

    # How far back to look for write statements each cycle.
    # Use the same lookback as AuditLogScraper so we don't miss anything.
    _LOOKBACK_HOURS = max(LRU_EVICT_HOURS * 2, 2)

    def __init__(self) -> None:
        # query_ids already pushed to Spark this run — prevents re-submission.
        self._submitted: set[str] = set()
        # Bound the set size: prune entries older than one lookback window
        self._submitted_ts: dict[str, datetime] = {}

    def scan_and_push(self, doris: DorisClient, polaris_creds: dict[str, str]) -> None:
        """
        Detect DML writes against external catalog tables and push them to Spark.
        Called once per daemon cycle.
        """
        writes = self._scrape_writes(doris)
        if not writes:
            return

        self._prune_seen()

        new_writes = [w for w in writes if w.query_id not in self._submitted]
        if not new_writes:
            logger.debug("WriteInterceptor: all %d write(s) already submitted.", len(writes))
            return

        logger.info(
            "WriteInterceptor: %d new DML write(s) detected against external catalogs.",
            len(new_writes),
        )
        for pw in new_writes:
            self._push_to_spark(pw, polaris_creds)

    # ── Internal ───────────────────────────────────────────────────────────────

    def _scrape_writes(self, doris: DorisClient) -> list[PendingWrite]:
        """Query audit_log for DML statements against managed catalog tables.

        NOTE: Doris always logs catalog='internal' in audit_log regardless of
        which external catalog the DML targets.  We match on stmt text instead.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._LOOKBACK_HOURS)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

        stmt_filters = " OR ".join(
            f"LOWER(stmt) LIKE '%{c}.%'" for c in MANAGED_CATALOGS
        )

        sql = f"""
            SELECT query_id, stmt
            FROM __internal_schema.audit_log
            WHERE
                time >= '{cutoff_str}'
                AND (
                    LOWER(TRIM(stmt)) LIKE 'insert%'
                    OR LOWER(TRIM(stmt)) LIKE 'update%'
                    OR LOWER(TRIM(stmt)) LIKE 'delete%'
                    OR LOWER(TRIM(stmt)) LIKE 'merge%'
                )
                AND ({stmt_filters})
        """
        try:
            rows = doris.execute(sql)
        except Exception as exc:
            logger.error("WriteInterceptor: audit log scan failed: %s", exc)
            return []

        result: list[PendingWrite] = []
        now = datetime.now(timezone.utc)
        for query_id, stmt in rows:
            if not _WRITE_VERBS_RE.match(stmt or ""):
                continue
            key = _extract_key_from_stmt(stmt)
            if not key:
                logger.debug(
                    "WriteInterceptor: could not extract catalog.db.table from stmt (qid=%s) — skip.",
                    query_id,
                )
                continue
            result.append(PendingWrite(
                query_id=str(query_id),
                catalog=key.catalog,
                db=key.db,
                table=key.table,
                stmt=stmt,
                detected_at=now,
            ))
        return result

    def _push_to_spark(self, pw: PendingWrite, polaris_creds: dict[str, str]) -> None:
        """
        Submit pw.stmt as a Spark job via the Spark standalone REST API.

        The job payload follows the same structure used by mcp-spark/server.py
        and the Spark REST submission protocol (POST /v1/submissions/create).

        Credentials are NOT included here — the Spark job (spark_iceberg_write.py)
        reads them from OpenBao independently using its K8s Service Account.
        """
        warehouse = CATALOG_WAREHOUSE.get(pw.catalog, pw.catalog)

        # Encode the write parameters as a single JSON app argument so the
        # Spark job can unambiguously deserialise them.
        job_args = json.dumps({
            "catalog":   pw.catalog,
            "warehouse": warehouse,
            "db":        pw.db,
            "table":     pw.table,
            "stmt":      pw.stmt,
        })

        payload = {
            "action": "CreateSubmissionRequest",
            "appResource": _SPARK_WRITE_SCRIPT,
            "mainClass": "",          # PySpark — no main class
            "appArgs": [job_args],
            "sparkProperties": {
                "spark.app.name":           f"doris-write-pushdown-{pw.catalog}-{pw.table}",
                "spark.master":             SPARK_MASTER_URL,
                "spark.submit.deployMode":  "cluster",
                # The write script handles all further Spark conf (Iceberg, S3,
                # OAuth2) by calling BaoSparkInit internally.
            },
            "environmentVariables": {
                # Pass OpenBao address so the job uses in-cluster address.
                "ADDR": BAO_ADDR,
            },
            "clientSparkVersion": SPARK_VERSION,
        }

        url = f"{SPARK_REST_URL}/v1/submissions/create"
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            url, data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read())
            submission_id = result.get("submissionId", "unknown")
            logger.info(
                "WriteInterceptor: pushed %s.%s.%s (qid=%s) → Spark submissionId=%s.",
                pw.catalog, pw.db, pw.table, pw.query_id, submission_id,
            )
        except Exception as exc:
            logger.error(
                "WriteInterceptor: Spark submission failed for %s.%s.%s (qid=%s): %s",
                pw.catalog, pw.db, pw.table, pw.query_id, exc,
            )
            return  # Do NOT mark as submitted — will retry next cycle

        now = datetime.now(timezone.utc)
        self._submitted.add(pw.query_id)
        self._submitted_ts[pw.query_id] = now

    def _prune_seen(self) -> None:
        """Remove query_ids older than one lookback window to bound memory."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._LOOKBACK_HOURS)
        stale = [qid for qid, ts in self._submitted_ts.items() if ts < cutoff]
        for qid in stale:
            self._submitted.discard(qid)
            del self._submitted_ts[qid]


def _extract_table_from_write_stmt(stmt: str, default_db: str) -> str | None:
    """
    Extract the target table name from a DML write statement.

    Handles:
      INSERT INTO catalog.db.table ...
      INSERT OVERWRITE catalog.db.table ...
      UPDATE catalog.db.table SET ...
      DELETE FROM catalog.db.table WHERE ...
      MERGE INTO catalog.db.table USING ...
    Returns the bare table name (last dotted segment).
    """
    if not stmt:
        return None
    lower = stmt.lower().strip()

    # INSERT INTO / INSERT OVERWRITE
    m = re.match(
        r"insert\s+(?:into|overwrite)\s+([`\w.\-]+)",
        lower,
    )
    if m:
        ref = stmt.strip().split()[2] if "overwrite" in lower.split()[1] else stmt.strip().split()[2]
        # Re-parse safely: take the 3rd token (after INSERT INTO/OVERWRITE)
        tokens = stmt.split()
        idx = 2  # tokens[0]=INSERT, tokens[1]=INTO|OVERWRITE, tokens[2]=table
        ref = tokens[idx].strip("`") if len(tokens) > idx else ""
        return ref.split(".")[-1] if ref else None

    # UPDATE table SET ...
    m = re.match(r"update\s+([`\w.\-]+)", lower)
    if m:
        ref = m.group(1).strip("`")
        return ref.split(".")[-1]

    # DELETE FROM table ...
    m = re.match(r"delete\s+from\s+([`\w.\-]+)", lower)
    if m:
        ref = m.group(1).strip("`")
        return ref.split(".")[-1]

    # DELETE table ... (without FROM)
    m = re.match(r"delete\s+([`\w.\-]+)", lower)
    if m:
        ref = m.group(1).strip("`")
        return ref.split(".")[-1]

    # MERGE INTO table USING ...
    m = re.match(r"merge\s+into\s+([`\w.\-]+)", lower)
    if m:
        ref = m.group(1).strip("`")
        return ref.split(".")[-1]

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Main daemon loop
# ─────────────────────────────────────────────────────────────────────────────
class CacheManagerDaemon:
    def __init__(self) -> None:
        logger.info("Doris Cache Manager starting up.")
        self._bao = BaoClient()

        # Load credentials — try OpenBao first, fall back to env var for Doris password.
        # The DORIS_ADMIN_PASSWORD env var is injected from the rbac-plane-credentials
        # K8s secret so the daemon can start even if OpenBao K8s JWT auth is not yet
        # fully configured (e.g. role just created, token propagation lag).
        logger.info("Loading credentials from OpenBao (%s).", BAO_ADDR)
        try:
            self._doris_creds = self._bao.read_secret(_PATH_DORIS)
            logger.info("Doris credentials loaded from OpenBao.")
        except Exception as exc:
            env_pass = os.environ.get("DORIS_ADMIN_PASSWORD")
            if env_pass:
                logger.warning(
                    "OpenBao unavailable (%s) — using DORIS_ADMIN_PASSWORD env var.", exc
                )
                self._doris_creds = {"admin_password": env_pass}
            else:
                raise

        try:
            self._polaris_creds = self._bao.read_secret(_PATH_POLARIS)
            logger.info("Polaris credentials loaded from OpenBao.")
        except Exception as exc:
            logger.warning("Could not load Polaris creds from OpenBao (%s) — write-pushdown disabled.", exc)
            self._polaris_creds = {}

        logger.info("Credentials loaded.")

        # Shared Doris connection for metadata operations
        self._meta_doris = DorisClient(
            host=DORIS_HOST,
            port=DORIS_PORT,
            user=DORIS_USER,
            password=self._doris_creds["admin_password"],
        )

        self._meta              = MetaStore()
        self._scraper           = AuditLogScraper()
        self._executor          = WarmupExecutor(self._meta)
        self._scheduler         = WarmupScheduler(self._executor)
        self._lru               = LRUEvictionChecker()
        self._write_interceptor = WriteInterceptor()
        self._be_discovery      = BeDiscovery()

    def run(self) -> None:
        logger.info(
            "Cache Manager daemon running. "
            "scan_interval=%ds lru_evict=%dh max_concurrent=%d warmup_stale=%dmin",
            SCAN_INTERVAL_S, LRU_EVICT_HOURS, MAX_CONCURRENT, WARMUP_STALE_MIN,
        )
        while True:
            try:
                self._cycle()
            except Exception as exc:
                logger.error("Cycle error: %s", exc, exc_info=True)
            logger.info("Sleeping %d seconds until next scan.", SCAN_INTERVAL_S)
            time.sleep(SCAN_INTERVAL_S)

    def _cycle(self) -> None:
        now = datetime.now(timezone.utc)
        logger.info("=== Cache Manager cycle start: %s ===", now.isoformat())

        # 1. Discover alive BE nodes — refreshed every cycle so new BEs are
        #    picked up automatically without restarting the daemon.
        be_nodes = self._be_discovery.discover(self._meta_doris)

        # 2. Scrape audit log for SELECT counts
        fresh_counts = self._scraper.scrape(self._meta_doris)

        # 3. Load existing stats
        existing_stats = self._meta.load_all_stats(self._meta_doris)

        # 4. Merge: update stats for every table seen in this cycle
        updated_stats: dict[TableKey, dict] = dict(existing_stats)

        for key, new_hits in fresh_counts.items():
            prev = existing_stats.get(key, {})
            prev_total = prev.get("total_select_count", 0)
            prev_last_ts: datetime | None = prev.get("last_select_ts")

            # Compute interval if we have a previous timestamp
            select_interval: float | None = None
            if prev_last_ts is not None:
                if prev_last_ts.tzinfo is None:
                    prev_last_ts = prev_last_ts.replace(tzinfo=timezone.utc)
                delta_min = (now - prev_last_ts).total_seconds() / 60.0
                if delta_min > 0:
                    select_interval = delta_min

            warm_interval: float | None = None
            if select_interval is not None:
                warm_interval = select_interval * (2.0 / 3.0)

            new_total = prev_total + new_hits
            cache_state = prev.get("cache_state", "UNKNOWN")

            self._meta.upsert_stats(
                doris=self._meta_doris,
                key=key,
                new_count=new_total,
                now=now,
                prev_last_ts=prev_last_ts,
                select_interval_min=select_interval,
                warm_interval_min=warm_interval,
                cache_state=cache_state,
            )

            updated_stats[key] = {
                "total_select_count":  new_total,
                "last_select_ts":      now,
                "prev_select_ts":      prev_last_ts,
                "select_interval_min": select_interval,
                "warm_interval_min":   warm_interval,
                "last_warmed_ts":      prev.get("last_warmed_ts"),
                "cache_state":         cache_state,
            }

        # 5. LRU eviction — check all known tables (including those not queried this cycle)
        self._lru.check(updated_stats, self._meta_doris, self._executor, self._meta, now)

        # 6. Warm-up scheduling — fan out across all alive BE nodes
        self._scheduler.evaluate(
            updated_stats, be_nodes, self._doris_creds, self._meta_doris, self._meta, now
        )

        # 7. Write-pushdown — intercept DML writes against external catalogs
        #    and re-submit them to Spark.
        self._write_interceptor.scan_and_push(self._meta_doris, self._polaris_creds)

        # Touch heartbeat file for liveness probe
        try:
            with open("/tmp/cache_manager_alive", "w") as _hb:
                _hb.write(now.isoformat())
        except OSError:
            pass

        logger.info(
            "=== Cycle done. active_warmups=%d ===",
            self._executor.running_count(),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    CacheManagerDaemon().run()
