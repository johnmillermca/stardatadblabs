#!/usr/bin/env python3
"""
sync_worker.py — Stardata Dynamic LHS Sidecar Sync Worker
==========================================================
Runs as a permanent sidecar Deployment in the `prod` namespace.

Every POLL_INTERVAL_SECONDS (default 60) it:
  A. Discovers all candidate control tables in cache_system
  B. Resolves each table's source app database
  C. Discovers the Polaris/Iceberg catalog from SHOW CATALOGS (never hard-coded)
  D. Provisions the Iceberg DB and table on-demand if missing
  E. Checks whether the _log table is blocking this table
  F. Skips tables that have a batch already in-flight
  G. Computes the updated_at sync window (ts_from → ts_to, with safety lag)
  H. Executes the UPSERT batch (INSERT ... SELECT with DELETE_SIGN filter)
  I. Executes the DELETE sweep (tombstone snap_ids from _deletes table)
  J. Issues REFRESH TABLE so the catalog sees new data immediately

This worker is fully dynamic: it handles any table that appears in cache_system,
now or in the future, without any restart or configuration change.

Environment variables:
  STARDATA_HOST      (default: stardata-fe.prod.svc.cluster.local)
  STARDATA_PORT      (default: 9030)
  STARDATA_USER      (default: root)
  STARDATA_PASSWORD  (required)
  POLL_INTERVAL_SECONDS  (default: 60)
"""

import os
import sys
import time
import traceback
import logging

import pymysql
import pymysql.cursors

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("lhs-sync")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STARDATA_HOST     = os.environ.get("STARDATA_HOST", "stardata-fe.prod.svc.cluster.local")
STARDATA_PORT     = int(os.environ.get("STARDATA_PORT", "9030"))
STARDATA_USER     = os.environ.get("STARDATA_USER", "root")
# Use sentinel None to distinguish "not set" from intentionally empty string
STARDATA_PASSWORD = os.environ.get("STARDATA_PASSWORD")
POLL_INTERVAL     = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))

# Tables in cache_system that are NOT control tables — same list as the UI
SYSTEM_TABLES = {
    "table_query_stats", "cache_eviction_log", "table_cache_metrics",
    "catalog_sync_log", "query_block_log", "datetime_load_tracker",
    "lhs_schedule", "backup_schedule",
}

# Doris external type mapping: Doris base type → type used in CREATE TABLE on
# external Iceberg catalog.  Matches the DORIS_EXT_TYPE map in stardata.html.
DORIS_EXT_TYPE = {
    "BOOLEAN": "BOOLEAN",
    "TINYINT": "TINYINT",
    "SMALLINT": "SMALLINT",
    "INT": "INT",
    "BIGINT": "BIGINT",
    "LARGEINT": "BIGINT",   # Iceberg has no LARGEINT → downcast to BIGINT
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DATE": "DATE",
    "DATEV2": "DATE",
    "DATETIME": "DATETIMEV2",
    "DATETIMEV2": "DATETIMEV2",
    "CHAR": "STRING",
    "VARCHAR": "STRING",
    "STRING": "STRING",
    "TEXT": "STRING",
    "JSON": "STRING",
    "VARIANT": "STRING",
    "IPV4": "STRING",
    "IPV6": "STRING",
    "DECIMAL": "DECIMAL",
    "DECIMALV3": "DECIMAL",
}

# Doris types not supported on Iceberg external tables — block creation
DORIS_UNSUPPORTED = {"HLL", "BITMAP", "QUANTILE_STATE", "AGG_STATE"}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def connect() -> pymysql.Connection:
    """Open a fresh connection to Stardata FE (normal session, DML allowed)."""
    return pymysql.connect(
        host=STARDATA_HOST,
        port=STARDATA_PORT,
        user=STARDATA_USER,
        password=STARDATA_PASSWORD or "",
        database="information_schema",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=15,
        read_timeout=600,   # Iceberg writes on large batches can take several minutes
        write_timeout=600,
        autocommit=True,
    )


def connect_hidden() -> pymysql.Connection:
    """
    Open a dedicated connection with show_hidden_columns=true.

    Setting show_hidden_columns at session level puts Stardata into a
    read-only debug mode that blocks ALL DML (INSERT/UPDATE/DELETE) on that
    connection.  This connection is therefore used ONLY for the
    INSERT ... SELECT that reads __DORIS_DELETE_SIGN__ from the source table.
    It is opened fresh per batch and closed immediately after — never reused
    for control-table writes or any other DML.
    """
    conn = pymysql.connect(
        host=STARDATA_HOST,
        port=STARDATA_PORT,
        user=STARDATA_USER,
        password=STARDATA_PASSWORD or "",
        database="information_schema",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=15,
        read_timeout=600,
        write_timeout=600,
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute("SET show_hidden_columns = true")
    return conn


def query(conn: pymysql.Connection, sql: str, args=None) -> list[dict]:
    """Execute a SELECT / SHOW and return all rows as list-of-dicts."""
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def execute(conn: pymysql.Connection, sql: str, args=None) -> int:
    """Execute a DML / DDL statement. Returns affected-row count."""
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.rowcount


# ---------------------------------------------------------------------------
# Step A — discover candidate control tables
# ---------------------------------------------------------------------------

def discover_control_tables(conn: pymysql.Connection) -> list[str]:
    """
    Return all table names in cache_system that are control tables:
    - not ending with _deletes
    - not containing _log
    - not in the known system-table exclusion list
    """
    rows = query(conn, """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = 'cache_system'
          AND TABLE_NAME NOT LIKE '%\\_log'
          AND TABLE_NAME NOT LIKE '%\\_deletes'
        ORDER BY TABLE_NAME
    """)
    return [
        r["TABLE_NAME"] for r in rows
        if r["TABLE_NAME"] not in SYSTEM_TABLES
    ]


# ---------------------------------------------------------------------------
# Step B — resolve the source app database
# ---------------------------------------------------------------------------

def resolve_app_db(conn: pymysql.Connection, tbl: str) -> str | None:
    """
    Find which user database owns a table with this name.
    Excludes Doris system databases and cache_system itself.
    Returns None if not found.
    """
    rows = query(conn, """
        SELECT TABLE_SCHEMA
        FROM information_schema.TABLES
        WHERE TABLE_NAME = %s
          AND TABLE_SCHEMA NOT IN (
            'information_schema', '__internal_schema',
            'cache_system', 'mysql', 'performance_schema'
          )
          AND TABLE_CATALOG = 'internal'
        LIMIT 1
    """, (tbl,))
    return rows[0]["TABLE_SCHEMA"] if rows else None


# ---------------------------------------------------------------------------
# Step C — discover the Polaris/Iceberg catalog
# ---------------------------------------------------------------------------

def discover_iceberg_catalog(conn: pymysql.Connection) -> str | None:
    """
    Run SHOW CATALOGS and return the name of the best available Iceberg catalog:
    - Type must be 'iceberg'
    - Prefer catalogs with no ErrorMsg (healthy) over ones with errors
    - Among healthy catalogs, return the first one found
    Never hard-codes the catalog name.
    """
    rows = query(conn, "SHOW CATALOGS")
    healthy = []
    fallback = []
    for row in rows:
        name      = row.get("CatalogName") or list(row.values())[0]
        type_val  = (row.get("Type") or list(row.values())[1] or "").lower()
        error_msg = (row.get("ErrorMsg") or "").strip()
        if type_val != "iceberg":
            continue
        if error_msg:
            log.debug("Iceberg catalog '%s' has error: %s", name, error_msg)
            fallback.append(name)
        else:
            healthy.append(name)

    if healthy:
        log.debug("Available healthy Iceberg catalogs: %s", healthy)
        return healthy[0]
    if fallback:
        log.warning("No healthy Iceberg catalog found. Using '%s' (has errors: check Polaris).",
                    fallback[0])
        return fallback[0]
    return None


# ---------------------------------------------------------------------------
# Step D — provision Iceberg database and table
# ---------------------------------------------------------------------------

def describe_source_table(conn: pymysql.Connection, app_db: str, tbl: str) -> list[dict]:
    """
    Run DESCRIBE <app_db>.<tbl> and return column info filtered to user columns
    (excludes hidden __DORIS_* columns and system-injected internals we map).
    Returns list of dicts: {name, doris_type, ext_type}
    """
    rows = query(conn, f"DESCRIBE `{app_db}`.`{tbl}`")
    result = []
    for r in rows:
        col_name = r.get("Field") or r.get("field") or list(r.values())[0]
        col_type = r.get("Type") or r.get("type") or list(r.values())[1]
        if col_name.startswith("__DORIS"):
            continue
        base = col_type.upper().split("(")[0].split(" ")[0]
        if base in DORIS_UNSUPPORTED:
            log.warning("Table %s.%s has unsupported column %s (%s) — skipping table",
                        app_db, tbl, col_name, base)
            return []
        # Preserve DECIMAL precision
        if base in ("DECIMAL", "DECIMALV3"):
            import re
            prec = re.search(r"\([^)]+\)", col_type)
            ext = "DECIMAL" + (prec.group(0) if prec else "(18,2)")
        else:
            ext = DORIS_EXT_TYPE.get(base, "STRING")
        result.append({"name": col_name, "doris_type": col_type, "ext_type": ext})
    return result


def provision_iceberg_table(conn: pymysql.Connection,
                            catalog: str, app_db: str, tbl: str,
                            columns: list[dict]) -> bool:
    """
    Idempotently create:
      1. The Iceberg database <catalog>.<app_db>_iceberg
      2. The Iceberg table   <catalog>.<app_db>_iceberg.<tbl>
    Returns True on success, False on error.
    """
    ice_db = f"{app_db}_iceberg"
    try:
        execute(conn, f"CREATE DATABASE IF NOT EXISTS `{catalog}`.`{ice_db}`")
        log.info("Iceberg DB `%s`.`%s` ensured.", catalog, ice_db)
    except Exception as exc:
        log.error("Failed to create Iceberg DB %s.%s: %s", catalog, ice_db, exc)
        return False

    col_defs = ",\n  ".join(f"`{c['name']}` {c['ext_type']}" for c in columns)
    create_sql = (
        f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{ice_db}`.`{tbl}` (\n"
        f"  {col_defs}\n"
        f") PROPERTIES ('format-version'='2')"
    )
    try:
        execute(conn, create_sql)
        log.info("Iceberg table `%s`.`%s`.`%s` ensured.", catalog, ice_db, tbl)
        return True
    except Exception as exc:
        log.error("Failed to create Iceberg table %s.%s.%s: %s",
                  catalog, ice_db, tbl, exc)
        return False


# ---------------------------------------------------------------------------
# Step D (part 2) — ensure cache_system control / _deletes / _log tables
# ---------------------------------------------------------------------------

def ensure_control_tables(conn: pymysql.Connection, tbl: str) -> bool:
    """
    Idempotently create cache_system.<tbl>, <tbl>_deletes, <tbl>_log.
    Uses IF NOT EXISTS so it is safe to call every tick.
    """
    try:
        execute(conn, f"""
            CREATE TABLE IF NOT EXISTS cache_system.`{tbl}` (
                batch_id        BIGINT        NOT NULL,
                database_name   VARCHAR(256)  NOT NULL,
                table_name      VARCHAR(256)  NOT NULL,
                batch_type      VARCHAR(32)   NOT NULL,
                snap_id_from    BIGINT        NOT NULL DEFAULT 0,
                snap_id_to      BIGINT        NOT NULL DEFAULT 0,
                row_count       BIGINT        NOT NULL DEFAULT 0,
                status          VARCHAR(32)   NOT NULL DEFAULT 'running',
                started_at      DATETIME      NOT NULL,
                finished_at     DATETIME      NULL,
                max_created_at  DATETIME      NULL,
                max_updated_at  DATETIME      NULL
            )
            DUPLICATE KEY(batch_id)
            DISTRIBUTED BY HASH(batch_id) BUCKETS 4
            PROPERTIES ("replication_num" = "1")
        """)
        execute(conn, f"""
            CREATE TABLE IF NOT EXISTS cache_system.`{tbl}_deletes` (
                snap_id     BIGINT    NOT NULL,
                deleted_at  DATETIME  NOT NULL
            )
            DUPLICATE KEY(snap_id)
            DISTRIBUTED BY HASH(snap_id) BUCKETS 4
            PROPERTIES ("replication_num" = "1")
        """)
        execute(conn, f"""
            CREATE TABLE IF NOT EXISTS cache_system.`{tbl}_log` (
                batch_id   BIGINT    NOT NULL,
                error_msg  TEXT      NOT NULL,
                logged_at  DATETIME  NOT NULL
            )
            DUPLICATE KEY(batch_id)
            DISTRIBUTED BY HASH(batch_id) BUCKETS 4
            PROPERTIES ("replication_num" = "1")
        """)
        return True
    except Exception as exc:
        log.error("Failed to ensure control tables for %s: %s", tbl, exc)
        return False


# ---------------------------------------------------------------------------
# Step E — gate: is the _log table blocking?
# ---------------------------------------------------------------------------

def is_blocked(conn: pymysql.Connection, tbl: str) -> bool:
    """Return True if cache_system.<tbl>_log has any error rows."""
    rows = query(conn, """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = 'cache_system'
          AND TABLE_NAME = %s
          AND TABLE_ROWS > 0
    """, (f"{tbl}_log",))
    return bool(rows)


# ---------------------------------------------------------------------------
# Step F — skip if a batch is already in-flight; auto-recover stale batches
# ---------------------------------------------------------------------------

STALE_BATCH_MINUTES = 10   # batch stuck longer than this = worker crashed mid-flight

def has_running_batch(conn: pymysql.Connection, tbl: str) -> bool:
    """
    Returns True if a live in-flight batch exists.
    Auto-recovers stale batches (started > STALE_BATCH_MINUTES ago with no
    active FE session) by marking them 'failed' so the next tick retries.
    """
    rows = query(conn, f"""
        SELECT batch_id, status, started_at
        FROM cache_system.`{tbl}`
        WHERE status IN ('running', 'delete_pending')
    """)
    if not rows:
        return False

    for row in rows:
        started = row["started_at"]
        if started is None:
            continue
        import datetime
        age_minutes = (datetime.datetime.now() - started).total_seconds() / 60
        if age_minutes > STALE_BATCH_MINUTES:
            log.warning("[%s] Stale batch %s detected (started %.1f min ago) — "
                        "marking failed and recovering for next tick.",
                        tbl, row["batch_id"], age_minutes)
            try:
                execute(conn, f"""
                    UPDATE cache_system.`{tbl}`
                    SET status = 'failed', finished_at = NOW()
                    WHERE batch_id = %s
                """, (row["batch_id"],))
                execute(conn, f"""
                    INSERT INTO cache_system.`{tbl}_log` (batch_id, error_msg, logged_at)
                    VALUES (%s, 'Stale batch auto-recovered: worker connection lost mid-flight', NOW())
                """, (row["batch_id"],))
            except Exception as exc:
                log.error("[%s] Failed to recover stale batch %s: %s",
                          tbl, row["batch_id"], exc)
            return False   # allow next tick to retry

    return True  # live in-flight batch, skip this tick


# ---------------------------------------------------------------------------
# Step G — compute the sync window (updated_at watermark)
#
# WHY updated_at and NOT snap_id:
#   Doris AUTO_INCREMENT (snap_id) is allocated per-BE in independent blocks.
#   In a 3-BE cluster, row A on BE-0 may get snap_id=400000 while row B on
#   BE-1 written in the same INSERT batch gets snap_id=244000.  Using
#   MAX(snap_id) as a watermark permanently skips any rows whose snap_id was
#   allocated below that MAX — even if those rows were written concurrently.
#
#   updated_at is a wall-clock DATETIME set on every INSERT and UPDATE.  It
#   is monotonically increasing for any given row and not affected by BE
#   partitioning.  We apply a small safety lag (SYNC_LAG_SECONDS) so that
#   rows written in the last N seconds are excluded from the current batch —
#   this avoids a race where a row is mid-write when we snapshot the window.
# ---------------------------------------------------------------------------

SYNC_LAG_SECONDS = 5   # exclude rows updated in the last 5 seconds (write safety margin)


def get_updated_from(conn: pymysql.Connection, tbl: str):
    """
    Return the updated_at watermark from the last completed upsert batch.
    Returns None only when NO completed upsert batch exists (first-ever run).

    Watermark priority (highest to lowest):
      1. MAX(max_updated_at) — actual MAX(updated_at) of live rows written
      2. MAX(max_created_at) — ts_to ceiling stored at batch start, used when
         the batch processed only deleted rows (0 live rows written) so that
         the window advances past those deleted rows and they are not
         re-processed on every future tick.
    Both are taken only from 'completed upsert' batches.
    """
    rows = query(conn, f"""
        SELECT
            MAX(max_updated_at) AS ts_actual,
            MAX(max_created_at) AS ts_ceiling,
            COUNT(*)            AS completed_count
        FROM cache_system.`{tbl}`
        WHERE status = 'completed upsert'
    """)
    r = rows[0]
    if not r["completed_count"]:
        return None   # no completed batch → INITIAL load
    # Advance to whichever is later: actual data watermark or ts_to ceiling
    ts_actual  = r["ts_actual"]
    ts_ceiling = r["ts_ceiling"]
    if ts_actual and ts_ceiling:
        return ts_actual if ts_actual > ts_ceiling else ts_ceiling
    return ts_actual or ts_ceiling


def get_next_batch_id(conn: pymysql.Connection, tbl: str) -> int:
    """MAX(batch_id) + 1 from control table (1 if no rows exist yet)."""
    rows = query(conn, f"SELECT COALESCE(MAX(batch_id), 0) AS max_id FROM cache_system.`{tbl}`")
    return int(rows[0]["max_id"]) + 1


# ---------------------------------------------------------------------------
# Control-row helpers
# ---------------------------------------------------------------------------

def insert_control_row(conn: pymysql.Connection, tbl: str, batch_id: int,
                       app_db: str, batch_type: str,
                       ts_from: str | None, ts_to: str) -> None:
    """
    Write a running control row.
    - max_created_at stores ts_to  (the batch window ceiling — fallback watermark)
    - max_updated_at stores ts_from (the batch window floor — for display)
    mark_success will overwrite both with actual MAX(created_at)/MAX(updated_at)
    from the rows written.  If mark_success stats return NULL, the ceiling
    stored in max_created_at is used by get_updated_from() as the fallback
    watermark so INITIAL does not re-run.
    """
    execute(conn, f"""
        INSERT INTO cache_system.`{tbl}`
            (batch_id, database_name, table_name, batch_type,
             snap_id_from, snap_id_to, row_count, status,
             started_at, finished_at, max_created_at, max_updated_at)
        VALUES
            (%s, %s, %s, %s, 0, 0, 0, 'running', NOW(), NULL, %s, %s)
    """, (batch_id, app_db, tbl, batch_type, ts_to, ts_from))


def mark_success(conn: pymysql.Connection, tbl: str, batch_id: int,
                 status: str, row_count: int,
                 max_created: str | None, max_updated: str | None) -> None:
    """
    Update control row to completed status.
    max_created_at is only overwritten when the stats value is non-NULL —
    preserving the ts_to ceiling written by insert_control_row() when
    0 rows were processed (so the watermark keeps advancing even on idle ticks).
    """
    if max_created is not None:
        execute(conn, f"""
            UPDATE cache_system.`{tbl}`
            SET status = %s, row_count = %s,
                max_created_at = %s, max_updated_at = %s,
                finished_at = NOW()
            WHERE batch_id = %s
        """, (status, row_count, max_created, max_updated, batch_id))
    else:
        # Preserve existing max_created_at (the ts_to ceiling) — only update max_updated_at
        execute(conn, f"""
            UPDATE cache_system.`{tbl}`
            SET status = %s, row_count = %s,
                max_updated_at = %s,
                finished_at = NOW()
            WHERE batch_id = %s
        """, (status, row_count, max_updated, batch_id))


def mark_failed(conn: pymysql.Connection, tbl: str, batch_id: int,
                error_msg: str) -> None:
    try:
        execute(conn, f"""
            UPDATE cache_system.`{tbl}`
            SET status = 'failed', finished_at = NOW()
            WHERE batch_id = %s
        """, (batch_id,))
        execute(conn, f"""
            INSERT INTO cache_system.`{tbl}_log` (batch_id, error_msg, logged_at)
            VALUES (%s, %s, NOW())
        """, (batch_id, error_msg[:4096]))
    except Exception as exc:
        log.error("Failed to write failure record for %s batch %d: %s", tbl, batch_id, exc)


# ---------------------------------------------------------------------------
# Step H — UPSERT batch (updated_at watermark)
# ---------------------------------------------------------------------------

def run_upsert(conn: pymysql.Connection,
               catalog: str, app_db: str, tbl: str,
               columns: list[dict],
               ts_from,     # datetime | None — None means INITIAL (full load)
               ts_to: str   # upper bound updated_at (exclusive of last N seconds)
               ) -> None:
    """
    Sync rows from app_db.tbl into the Iceberg table for the window
    updated_at in (ts_from, ts_to].

    For every row in the window (regardless of DELETE_SIGN):
      1. DELETE the existing Iceberg row by snap_id  — removes stale version
         (prevents duplicate rows on UPDATE; also removes rows on DELETE)
      2. INSERT the live version if DELETE_SIGN = 0  — skips hard-deleted rows

    This two-step approach makes Iceberg behave like an UPSERT+DELETE store
    without requiring MERGE INTO (not supported) or tombstone tables.

    Uses updated_at as the watermark (not snap_id) because Doris
    AUTO_INCREMENT snap_id is allocated per-BE and is NOT globally
    monotonic — using snap_id as a watermark permanently skips rows
    whose snap_id was allocated on a different BE below MAX(snap_id).
    """
    ice_db     = f"{app_db}_iceberg"
    batch_type = "INITIAL" if ts_from is None else "UPSERT"
    batch_id   = get_next_batch_id(conn, tbl)

    log.info("[%s] Starting %s batch %d  updated_at: %s → %s",
             tbl, batch_type, batch_id, ts_from or "beginning", ts_to)

    insert_control_row(conn, tbl, batch_id, app_db, batch_type,
                       str(ts_from) if ts_from else None, str(ts_to))

    col_list = ", ".join(f"`{c['name']}`" for c in columns)

    # WHERE clause covering ALL rows in window (both live and deleted)
    # used to collect snap_ids for pre-delete step
    if ts_from is None:
        where_all  = f"updated_at <= '{ts_to}'"
        where_live = f"updated_at <= '{ts_to}' AND __DORIS_DELETE_SIGN__ = 0"
    else:
        where_all  = f"updated_at > '{ts_from}' AND updated_at <= '{ts_to}'"
        where_live = f"updated_at > '{ts_from}' AND updated_at <= '{ts_to}' AND __DORIS_DELETE_SIGN__ = 0"

    hidden_conn = None
    try:
        hidden_conn = connect_hidden()

        # ── Step 1: collect snap_ids of ALL rows in window (live + deleted) ──
        # These are the rows whose current state changed — delete any existing
        # Iceberg version of them first (prevents duplicates on UPDATE,
        # propagates hard DELETEs to Iceberg without tombstone tables).
        snap_rows = query(hidden_conn, f"""
            SELECT snap_id
            FROM `{app_db}`.`{tbl}`
            WHERE {where_all}
        """)
        snap_ids = [r["snap_id"] for r in snap_rows]

        if snap_ids:
            # Chunk deletions to avoid oversized IN lists (max 1000 per chunk)
            chunk_size = 1000
            del_total  = 0
            for i in range(0, len(snap_ids), chunk_size):
                chunk = snap_ids[i:i + chunk_size]
                id_list = ", ".join(str(s) for s in chunk)
                affected = execute(conn, f"""
                    DELETE FROM `{catalog}`.`{ice_db}`.`{tbl}`
                    WHERE snap_id IN ({id_list})
                """)
                del_total += max(affected, 0)
            log.info("[%s] %s batch %d — pre-deleted %d stale/deleted Iceberg rows",
                     tbl, batch_type, batch_id, del_total)

        # ── Step 2: INSERT only live rows (DELETE_SIGN = 0) ─────────────────
        execute(hidden_conn, f"""
            INSERT /*+ SET_VAR(enable_insert_group_commit=false) */
            INTO `{catalog}`.`{ice_db}`.`{tbl}` ({col_list})
            SELECT {col_list}
            FROM `{app_db}`.`{tbl}`
            WHERE {where_live}
        """)

        # Stats: count live rows written and advance watermark
        stats = query(hidden_conn, f"""
            SELECT COUNT(*) AS cnt,
                   MAX(created_at) AS max_c,
                   MAX(updated_at) AS max_u
            FROM `{app_db}`.`{tbl}`
            WHERE {where_live}
        """)
        row_count   = int(stats[0]["cnt"])
        max_created = stats[0]["max_c"]
        max_updated = stats[0]["max_u"]

        # Count how many rows in window were hard-deleted (DELETE_SIGN=1)
        del_stats = query(hidden_conn, f"""
            SELECT COUNT(*) AS del_cnt
            FROM `{app_db}`.`{tbl}`
            WHERE {where_all} AND __DORIS_DELETE_SIGN__ = 1
        """)
        del_count = int(del_stats[0]["del_cnt"])
        if del_count:
            log.info("[%s] %s batch %d — %d hard-deleted rows removed from Iceberg",
                     tbl, batch_type, batch_id, del_count)

        mark_success(conn, tbl, batch_id, "completed upsert",
                     row_count, max_created, max_updated)
        log.info("[%s] %s batch %d complete — %d upserted, %d deleted  max_updated_at=%s",
                 tbl, batch_type, batch_id, row_count, del_count, max_updated)

    except Exception as exc:
        err = traceback.format_exc()
        log.error("[%s] %s batch %d FAILED: %s", tbl, batch_type, batch_id, exc)
        mark_failed(conn, tbl, batch_id, str(err))
    finally:
        if hidden_conn:
            try:
                hidden_conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Step I — DELETE sweep (tombstone fallback — no-op when run_upsert handles deletes)
# ---------------------------------------------------------------------------

def run_delete_sweep(conn: pymysql.Connection,
                     catalog: str, app_db: str, tbl: str) -> None:
    """
    Tombstone-based delete sweep for explicit out-of-band deletes written to
    cache_system.<tbl>_deletes.  run_upsert already handles deletes detected
    via __DORIS_DELETE_SIGN__=1 in the updated_at window, so this sweep is
    only needed for rows deleted outside the normal updated_at watermark flow
    (e.g. bulk DELETEs that bypass updated_at).  It is a no-op when the
    _deletes table is empty, which is the common case.
    """
    ice_db = f"{app_db}_iceberg"

    # Last delete watermark (snap_id_to of last 'deleted' batch)
    rows = query(conn, f"""
        SELECT COALESCE(MAX(snap_id_to), 0) AS del_from
        FROM cache_system.`{tbl}`
        WHERE status = 'deleted'
    """)
    del_from = int(rows[0]["del_from"])

    pending = query(conn, f"""
        SELECT COUNT(DISTINCT snap_id) AS cnt,
               COALESCE(MAX(snap_id), 0) AS max_snap
        FROM cache_system.`{tbl}_deletes`
        WHERE snap_id > {del_from}
    """)
    del_count = int(pending[0]["cnt"])
    del_to    = int(pending[0]["max_snap"])

    if del_count == 0:
        return   # nothing to do — common case

    batch_id = get_next_batch_id(conn, tbl)
    log.info("[%s] Starting tombstone DELETE batch %d — %d snap_ids", tbl, batch_id, del_count)

    execute(conn, f"""
        INSERT INTO cache_system.`{tbl}`
            (batch_id, database_name, table_name, batch_type,
             snap_id_from, snap_id_to, row_count, status,
             started_at, finished_at, max_created_at, max_updated_at)
        VALUES (%s, %s, %s, 'DELETE', %s, %s, 0, 'delete_pending',
                NOW(), NULL, NULL, NULL)
    """, (batch_id, app_db, tbl, del_from, del_to))

    try:
        execute(conn, f"""
            DELETE FROM `{catalog}`.`{ice_db}`.`{tbl}`
            WHERE snap_id IN (
                SELECT DISTINCT snap_id
                FROM cache_system.`{tbl}_deletes`
                WHERE snap_id > {del_from}
                  AND snap_id <= {del_to}
            )
        """)
        mark_success(conn, tbl, batch_id, "deleted", del_count, None, None)
        log.info("[%s] Tombstone DELETE batch %d complete — %d snap_ids removed",
                 tbl, batch_id, del_count)

    except Exception as exc:
        err = traceback.format_exc()
        log.error("[%s] Tombstone DELETE batch %d FAILED: %s", tbl, batch_id, exc)
        mark_failed(conn, tbl, batch_id, str(err))


# ---------------------------------------------------------------------------
# Step J — REFRESH TABLE
# ---------------------------------------------------------------------------

def refresh_table(conn: pymysql.Connection,
                  catalog: str, app_db: str, tbl: str) -> None:
    ice_db = f"{app_db}_iceberg"
    try:
        execute(conn, f"REFRESH TABLE `{catalog}`.`{ice_db}`.`{tbl}`")
        log.info("[%s] REFRESH TABLE complete.", tbl)
    except Exception as exc:
        # Non-fatal — data is already written, just catalog metadata may lag
        log.warning("[%s] REFRESH TABLE failed (non-fatal): %s", tbl, exc)


# ---------------------------------------------------------------------------
# Main sync loop — one full tick
# ---------------------------------------------------------------------------

HEARTBEAT_FILE = "/tmp/lhs_heartbeat"


def touch_heartbeat() -> None:
    """Write/update the heartbeat file so the liveness probe sees a live worker."""
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(str(time.time()))
    except Exception:
        pass


def run_tick(conn: pymysql.Connection) -> None:
    """Execute one full discovery + sync cycle across all candidate tables."""

    touch_heartbeat()

    # ── Step C: discover Iceberg catalog once per tick ───────────────────────
    catalog = discover_iceberg_catalog(conn)
    if not catalog:
        log.error("No Iceberg catalog found in SHOW CATALOGS — skipping tick. "
                  "Ensure a Polaris/REST Iceberg catalog is registered in Stardata.")
        return

    log.info("Using Iceberg catalog: %s", catalog)

    # ── Step A: discover candidate control tables ─────────────────────────────
    tables = discover_control_tables(conn)
    if not tables:
        log.info("No candidate control tables found in cache_system.")
        return

    log.info("Discovered %d candidate table(s): %s", len(tables), tables)

    for tbl in tables:
        try:
            _sync_one_table(conn, catalog, tbl)
        except Exception as exc:
            # Catch-all: a crash on one table must not stop others
            log.error("[%s] Unexpected error — skipping: %s", tbl, traceback.format_exc())


def _sync_one_table(conn: pymysql.Connection, catalog: str, tbl: str) -> None:
    """Full sync pipeline for a single table."""

    # ── Step B: resolve source app DB ────────────────────────────────────────
    app_db = resolve_app_db(conn, tbl)
    if not app_db:
        log.warning("[%s] Source table not found in any user database — skipping.", tbl)
        return

    iceberg_db = f"{app_db}_iceberg"
    log.debug("[%s] Source: %s.%s  →  Target: %s.%s.%s",
              tbl, app_db, tbl, catalog, iceberg_db, tbl)

    # ── Step D (part 1): introspect source columns ────────────────────────────
    columns = describe_source_table(conn, app_db, tbl)
    if not columns:
        log.warning("[%s] No usable columns (all unsupported or empty) — skipping.", tbl)
        return

    # Verify source table has snap_id, created_at, updated_at
    col_names = {c["name"].lower() for c in columns}
    for required in ("snap_id", "created_at", "updated_at"):
        if required not in col_names:
            log.warning("[%s] Missing required column '%s' — not an iceberg-type table, skipping.",
                        tbl, required)
            return

    # ── Step D (part 2): ensure cache_system control tables ──────────────────
    if not ensure_control_tables(conn, tbl):
        return

    # ── Step D (part 3): provision Iceberg DB + table ────────────────────────
    if not provision_iceberg_table(conn, catalog, app_db, tbl, columns):
        return

    # ── Step E: blocked by _log? ──────────────────────────────────────────────
    if is_blocked(conn, tbl):
        log.warning("[%s] BLOCKED — cache_system.%s_log has error rows. "
                    "Clear via UI 'Clear error & resume' to resume.", tbl, tbl)
        return

    # ── Step F: in-flight batch? ──────────────────────────────────────────────
    if has_running_batch(conn, tbl):
        log.info("[%s] Batch already in-flight — skipping this tick.", tbl)
        return

    # ── Step G: compute sync window (updated_at watermark) ───────────────────
    import datetime as _dt
    ts_from = get_updated_from(conn, tbl)   # None on first run → INITIAL
    # Upper bound: now minus safety lag (exclude rows mid-write)
    ts_to   = (_dt.datetime.now() - _dt.timedelta(seconds=SYNC_LAG_SECONDS)).strftime("%Y-%m-%d %H:%M:%S")

    log.info("[%s] Sync window: updated_at > %s AND <= %s",
             tbl, ts_from or "beginning", ts_to)

    # ── Step H: upsert ────────────────────────────────────────────────────────
    # INITIAL: ts_from is None → sync all rows up to ts_to
    # UPSERT:  sync only rows updated after last watermark
    if ts_from is None or ts_from < _dt.datetime.strptime(ts_to, "%Y-%m-%d %H:%M:%S"):
        run_upsert(conn, catalog, app_db, tbl, columns, ts_from, ts_to)
    else:
        log.info("[%s] No new rows (watermark %s is current) — skipping upsert.", tbl, ts_from)

    # ── Step I: delete sweep ──────────────────────────────────────────────────
    run_delete_sweep(conn, catalog, app_db, tbl)

    # ── Step J: refresh catalog ───────────────────────────────────────────────
    refresh_table(conn, catalog, app_db, tbl)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    if STARDATA_PASSWORD is None:
        log.error("STARDATA_PASSWORD environment variable is not set — exiting.")
        sys.exit(1)

    log.info("LHS Sync Sidecar starting.")
    log.info("  Host     : %s:%d", STARDATA_HOST, STARDATA_PORT)
    log.info("  User     : %s", STARDATA_USER)
    log.info("  Interval : %d seconds", POLL_INTERVAL)

    while True:
        conn = None
        try:
            conn = connect()
            log.info("=== Tick start ===")
            run_tick(conn)
            log.info("=== Tick complete — sleeping %d s ===", POLL_INTERVAL)
        except pymysql.OperationalError as exc:
            log.error("Database connection error: %s — will retry next tick.", exc)
        except Exception as exc:
            log.error("Unexpected tick-level error: %s", traceback.format_exc())
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
