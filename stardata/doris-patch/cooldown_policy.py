#!/usr/bin/env python3
import os
os.environ.setdefault("PYTHONUNBUFFERED", "1")

"""
cooldown_policy.py — Stardata partition cooldown sidecar
=========================================================
Runs as a sidecar container inside the stardata-fe pod (leader only).
Every POLL_INTERVAL_SECONDS it:

  1. Discovers all OLAP tables in every user database (excludes Doris
     system databases and cache_system).
  2. For each table that is UNIQUE KEY + MOW (enable_unique_key_merge_on_write=true),
     lists its partitions.
  3. For every partition that has no storage_cooldown_time set (value = "9999-12-31"),
     sets COOLDOWN TIME = partition_creation_time + COOLDOWN_DAYS.

This gives MOW tables the same effective 1-day cold-tier behaviour as
the storage_policy = sd_cold_1d applied to DUP/AGG tables — segments
migrate to S3 after 1 day.

Environment variables
─────────────────────
  DORIS_HOST               FE host           (default: 127.0.0.1)
  DORIS_PORT               FE query port     (default: 9030)
  DORIS_USER               FE user           (default: root)
  DORIS_PASS               FE password       (required)
  COOLDOWN_DAYS            days until cold   (default: 1)
  POLL_INTERVAL_SECONDS    loop interval     (default: 3600)
  LEADER_CHECK             only run on FE leader (default: true)
"""

import os
import time
import logging
import pymysql
from datetime import datetime, timedelta

# ── configuration ─────────────────────────────────────────────────────────────
DORIS_HOST             = os.environ.get("DORIS_HOST", "127.0.0.1")
DORIS_PORT             = int(os.environ.get("DORIS_PORT", "9030"))
DORIS_USER             = os.environ.get("DORIS_USER", "root")
DORIS_PASS             = os.environ.get("DORIS_PASS", "")
COOLDOWN_DAYS          = int(os.environ.get("COOLDOWN_DAYS", "1"))
POLL_INTERVAL_SECONDS  = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))
LEADER_CHECK           = os.environ.get("LEADER_CHECK", "true").lower() == "true"

# Doris system databases — never touched
SYSTEM_DBS = {"__internal_schema", "information_schema", "mysql", "performance_schema"}
# cache_system tables are internal bookkeeping — skip regular tables there
# (iceberg-type tables in cache_system are handled by storage_policy via patch)
SKIP_DBS   = SYSTEM_DBS | {"cache_system"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [cooldown] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("cooldown")

# No-cooldown sentinel Doris uses when cooldown time is not set
NO_COOLDOWN_SENTINEL = "9999-12-31 00:00:00"


def connect():
    return pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASS,
        database="information_schema",
        connect_timeout=10,
        autocommit=True,
    )


def is_leader(cur) -> bool:
    """Return True if this FE node is the current master."""
    cur.execute("SHOW FRONTENDS")
    for row in cur.fetchall():
        # row is a dict when cursor uses DictCursor
        if str(row.get("CurrentConnected", "")).lower() == "yes":
            return str(row.get("IsMaster", "")).lower() == "true"
    return False


def get_user_databases(cur) -> list[str]:
    cur.execute("SHOW DATABASES")
    dbs = []
    for row in cur.fetchall():
        db = list(row.values())[0] if isinstance(row, dict) else row[0]
        if db.lower() not in SKIP_DBS:
            dbs.append(db)
    return dbs


def get_mow_tables(cur, db: str) -> list[str]:
    """Return table names in db that are UNIQUE KEY + MOW."""
    cur.execute(
        """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_TYPE   = 'BASE TABLE'
        """,
        (db,),
    )
    tables = [row["TABLE_NAME"] if isinstance(row, dict) else row[0]
              for row in cur.fetchall()]

    mow_tables = []
    for tbl in tables:
        try:
            cur.execute(f"SHOW CREATE TABLE `{db}`.`{tbl}`")
            row = cur.fetchone()
            ddl = list(row.values())[1] if isinstance(row, dict) else row[1]
            if ('UNIQUE KEY' in ddl
                    and '"enable_unique_key_merge_on_write" = "true"' in ddl):
                mow_tables.append(tbl)
        except Exception as e:
            log.warning("SHOW CREATE TABLE %s.%s failed: %s", db, tbl, e)
    return mow_tables


def get_partitions(cur, db: str, tbl: str) -> list[dict]:
    """
    Returns list of dicts from SHOW PARTITIONS.
    Skips rows where PartitionName equals the table name (auto-partition
    placeholder row that Doris returns for tables with no data yet).
    Returns empty list if no real partitions exist.

    SHOW PARTITIONS columns include:
      PartitionId, PartitionName, VisibleVersion, VisibleVersionTime,
      State, PartitionKey, Range, DistributionKey, Buckets,
      ReplicationNum, StorageMedium, CooldownTime, ...
    """
    try:
        cur.execute(f"SHOW PARTITIONS FROM `{db}`.`{tbl}`")
        rows = cur.fetchall()
        if not rows:
            return []
        # Normalise to list-of-dicts
        if not isinstance(rows[0], dict):
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in rows]
        # Only process rows that belong to a range-partitioned table.
        # Non-partitioned tables have empty PartitionKey and Range — skip them.
        # Auto-partition tables have PartitionKey set (e.g. `created_at`).
        real = [r for r in rows
                if r.get("PartitionKey", "").strip() != ""
                or r.get("Range", "").strip() not in ("", "NULL")]
        return real
    except Exception as e:
        log.warning("SHOW PARTITIONS %s.%s failed: %s", db, tbl, e)
        return []


def apply_cooldown(cur, db: str, tbl: str, partition: str, cooldown_dt: datetime):
    cooldown_str = cooldown_dt.strftime("%Y-%m-%d %H:%M:%S")
    sql = (
        f'ALTER TABLE `{db}`.`{tbl}` '
        f'MODIFY PARTITION `{partition}` '
        f'SET ("storage_cooldown_time" = "{cooldown_str}")'
    )
    try:
        cur.execute(sql)
        log.info("SET cooldown %s → %s.%s PARTITION %s",
                 cooldown_str, db, tbl, partition)
    except Exception as e:
        log.warning("ALTER PARTITION failed %s.%s.%s: %s", db, tbl, partition, e)


def process_once(conn):
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        if LEADER_CHECK and not is_leader(cur):
            log.info("Not the FE leader — skipping this cycle.")
            return

        dbs = get_user_databases(cur)
        log.info("Scanning %d user databases for MOW tables...", len(dbs))

        total_set = 0
        for db in dbs:
            mow_tables = get_mow_tables(cur, db)
            for tbl in mow_tables:
                partitions = get_partitions(cur, db, tbl)
                for p in partitions:
                    cooldown_val = str(p.get("CooldownTime", p.get("StorageCooldownTime", NO_COOLDOWN_SENTINEL)))
                    if NO_COOLDOWN_SENTINEL not in cooldown_val:
                        continue  # already has a cooldown set
                    # Parse partition creation time
                    # Doris SHOW PARTITIONS uses "VisibleVersionTime" for the partition creation time
                    create_time_str = str(p.get("VisibleVersionTime", p.get("CreateTime", "")))
                    try:
                        create_dt = datetime.strptime(create_time_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        log.warning("Cannot parse CreateTime '%s' for %s.%s.%s",
                                    create_time_str, db, tbl, p.get("PartitionName"))
                        continue
                    cooldown_dt = create_dt + timedelta(days=COOLDOWN_DAYS)
                    apply_cooldown(cur, db, tbl, str(p["PartitionName"]), cooldown_dt)
                    total_set += 1

        log.info("Cycle complete — %d partition(s) had cooldown set.", total_set)


def main():
    log.info("Stardata cooldown sidecar starting — host=%s port=%d cooldown=%dd interval=%ds",
             DORIS_HOST, DORIS_PORT, COOLDOWN_DAYS, POLL_INTERVAL_SECONDS)

    # On startup, retry every 30s until we get a successful connection,
    # then switch to the normal POLL_INTERVAL_SECONDS cadence.
    startup_done = False
    while True:
        try:
            conn = connect()
            process_once(conn)
            conn.close()
            startup_done = True
        except Exception as e:
            log.error("Cycle error: %s", e)
            if not startup_done:
                log.info("Retrying startup connection in 30s...")
                time.sleep(30)
                continue
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
