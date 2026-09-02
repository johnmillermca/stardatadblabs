#!/usr/bin/env python3
"""
databricks_product_insert_loop.py
==================================
Continuously insert new timestamped product rows into Databricks and
immediately trigger ``starpump databricks`` to copy them to Iceberg.

Each cycle:
  1. Queries the current MAX(product_id) from Databricks.
  2. Inserts BATCH_SIZE new rows with ``created_at = NOW()`` and
     product_id values starting from max + 1.
  3. Runs ``kubectl exec … starpump databricks`` against the live cluster.
  4. Prints a timestamped summary and waits INTERVAL_SECONDS before repeating.

Running
-------
  pip install databricks-sql-connector   # once

  TOKEN=<openbao-root-token> python3 databricks_product_insert_loop.py

  # Optional overrides:
  INTERVAL_SECONDS=30     seconds between cycles          (default: 30)
  BATCH_SIZE=5            new rows inserted per cycle     (default: 5)
  STARPUMP_NAMESPACE=prod kubernetes namespace            (default: prod)
  STARPUMP_CONTAINER=spark-master  container name         (default: spark-master)
  DRY_RUN=1               print SQL only, do not execute  (default: off)
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import random
import subprocess
import sys
import time
import urllib.request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("insert-loop")

# ── Configuration (env overrides) ─────────────────────────────────────────────
CATALOG             = "lakehouse"
SCHEMA              = "lakehouse_db"
TABLE               = "product"
INTERVAL_SECONDS    = int(os.environ.get("INTERVAL_SECONDS", "30"))
BATCH_SIZE          = int(os.environ.get("BATCH_SIZE", "5"))
K8S_NAMESPACE       = os.environ.get("STARPUMP_NAMESPACE", "prod")
K8S_CONTAINER       = os.environ.get("STARPUMP_CONTAINER", "spark-master")
DRY_RUN             = os.environ.get("DRY_RUN", "0") == "1"

_BAO_IN_CLUSTER = "http://openbao.prod.svc.cluster.local:8200"
_BAO_NODEPORT   = "http://192.168.1.50:30820"

# ── Fixture pools (same as seed script for consistent naming) ─────────────────
_CATEGORIES = {
    "Electronics":  ["Audio", "Cameras", "Laptops", "Phones", "Tablets", "Wearables"],
    "Clothing":     ["Men", "Women", "Kids", "Sportswear", "Footwear"],
    "Home":         ["Kitchen", "Furniture", "Bedding", "Decor", "Garden"],
    "Sports":       ["Outdoor", "Fitness", "Cycling", "Swimming", "Team Sports"],
    "Books":        ["Fiction", "Science", "History", "Children", "Cooking"],
    "Toys":         ["Board Games", "Action Figures", "Puzzles", "Outdoor Play", "Educational"],
    "Beauty":       ["Skincare", "Haircare", "Fragrance", "Makeup", "Wellness"],
    "Automotive":   ["Parts", "Tools", "Accessories", "Care", "Safety"],
}
_BRANDS = [
    "NovaTech", "SoundWave", "BrightLife", "AeroFit", "PineCrest",
    "UrbanEdge", "SwiftLine", "PureForm", "EcoWave", "StarCore",
    "BluePeak", "IronCraft", "SilverLeaf", "TerraGear", "ZenFlow",
]
_ADJECTIVES = [
    "Pro", "Ultra", "Max", "Lite", "Plus", "Elite", "Prime", "Sport",
    "Smart", "Flex", "Boost", "Core", "Edge", "Swift", "Nano",
]
_NOUNS = [
    "Headphones", "Backpack", "Watch", "Sneakers", "Jacket", "Desk Lamp",
    "Water Bottle", "Keyboard", "Yoga Mat", "Coffee Maker", "Camera",
    "Notebook", "Tent", "Blender", "Monitor", "Helmet", "Sunglasses",
    "Gloves", "Speaker", "Charger",
]


# ── OpenBao credential helper ─────────────────────────────────────────────────
def _fetch_bao(path: str) -> dict[str, str]:
    token = os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN")
    if not token:
        raise RuntimeError(
            "Set TOKEN env-var to your OpenBao root/bootstrap token before running."
        )
    # Try in-cluster first, fall back to NodePort for local dev.
    for base in (_BAO_IN_CLUSTER, _BAO_NODEPORT):
        try:
            req = urllib.request.Request(
                f"{base}/v1/{path}",
                headers={"X-Vault-Token": token},
            )
            data = json.loads(urllib.request.urlopen(req, timeout=5).read())
            return data["data"]["data"]
        except Exception:
            continue
    raise RuntimeError(f"Cannot reach OpenBao at either address to read {path}")


# ── Row generator ─────────────────────────────────────────────────────────────
def _gen_rows(start_id: int, n: int, ts: datetime.datetime) -> list[dict]:
    """
    Generate *n* synthetic product rows starting at product_id = *start_id*.
    ``created_at`` and ``updated_at`` are both set to *ts* so every batch
    carries a precise timestamp that can be correlated with the starpump run.
    """
    rng  = random.Random(start_id)          # deterministic per batch for reproducibility
    cats = list(_CATEGORIES.keys())
    rows = []
    for i in range(n):
        pid     = start_id + i
        cat     = rng.choice(cats)
        sub_cat = rng.choice(_CATEGORIES[cat])
        brand   = rng.choice(_BRANDS)
        adj     = rng.choice(_ADJECTIVES)
        noun    = rng.choice(_NOUNS)
        name    = f"{brand} {noun} {adj}"
        tag     = hashlib.md5(f"{name}{pid}".encode()).hexdigest()[:4].upper()
        sku     = f"SKU-{pid:05d}-{tag}"
        unit_price = round(rng.uniform(5.0, 999.99), 2)
        cost_price = round(unit_price * rng.uniform(0.3, 0.65), 2)
        ts_str  = ts.strftime("%Y-%m-%d %H:%M:%S")
        rows.append({
            "product_id":     pid,
            "product_name":   name,
            "category":       cat,
            "sub_category":   sub_cat,
            "brand":          brand,
            "sku":            sku,
            "unit_price":     unit_price,
            "cost_price":     cost_price,
            "stock_quantity": rng.randint(0, 2000),
            "reorder_level":  rng.randint(10, 200),
            "weight_kg":      round(rng.uniform(0.05, 25.0), 3),
            "is_active":      1,
            "created_at":     ts_str,
            "updated_at":     ts_str,
        })
    return rows


def _esc(s: str) -> str:
    return s.replace("'", "''")


def _build_values(rows: list[dict]) -> str:
    parts = []
    for r in rows:
        parts.append(
            f"({r['product_id']}, "
            f"'{_esc(r['product_name'])}', "
            f"'{r['category']}', "
            f"'{r['sub_category']}', "
            f"'{r['brand']}', "
            f"'{r['sku']}', "
            f"{r['unit_price']}, "
            f"{r['cost_price']}, "
            f"{r['stock_quantity']}, "
            f"{r['reorder_level']}, "
            f"{r['weight_kg']}, "
            f"{r['is_active']}, "
            f"TIMESTAMP'{r['created_at']}', "
            f"TIMESTAMP'{r['updated_at']}', "
            f"NULL, NULL)"
        )
    return ",\n".join(parts)


# ── Kubernetes helpers ─────────────────────────────────────────────────────────
def _get_master_pod() -> str:
    result = subprocess.run(
        [
            "kubectl", "get", "pod",
            "-n", K8S_NAMESPACE,
            "-l", "component=master",
            "-o", "jsonpath={.items[0].metadata.name}",
        ],
        capture_output=True, text=True, check=True,
    )
    pod = result.stdout.strip()
    if not pod:
        raise RuntimeError(
            f"No spark-master pod found in namespace '{K8S_NAMESPACE}' "
            "(label component=master). Is the cluster running?"
        )
    return pod


def _run_starpump(master_pod: str, token: str) -> int:
    """
    Run ``starpump databricks`` inside the spark-master container.
    Streams stdout/stderr to the console in real time.
    Returns the process exit code.
    """
    cmd = [
        "kubectl", "exec", "-n", K8S_NAMESPACE, master_pod,
        "-c", K8S_CONTAINER, "--",
        "env",
        f"USER=dave",
        f"TOKEN={token}",
        "DATABASE=lakehouse",
        "SCHEMAS=lakehouse_db",
        "starpump", "databricks",
    ]
    logger.info("Running starpump databricks …")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f"    {line}", end="", flush=True)
    proc.wait()
    return proc.returncode


# ── Main loop ─────────────────────────────────────────────────────────────────
def main() -> None:
    # Validate TOKEN present before anything else.
    token = os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN")
    if not token:
        raise SystemExit("ERROR: Set TOKEN env-var to your OpenBao root/bootstrap token.")

    logger.info(
        "=== insert-loop | catalog=%s schema=%s table=%s "
        "batch=%d interval=%ds namespace=%s ===",
        CATALOG, SCHEMA, TABLE, BATCH_SIZE, INTERVAL_SECONDS, K8S_NAMESPACE,
    )
    if DRY_RUN:
        logger.info("DRY_RUN=1 — SQL will be printed but not executed; starpump will NOT run.")

    # Fetch Databricks creds once (cached for the whole run).
    db_creds = _fetch_bao("secret/data/platform/databricks")
    host      = db_creds["host"]
    http_path = db_creds["http_path"]
    db_token  = db_creds["token"]
    logger.info("Databricks creds loaded — host=%s", host)

    try:
        from databricks import sql as dbsql
    except ImportError:
        raise SystemExit(
            "databricks-sql-connector not installed.\n"
            "Run: pip install databricks-sql-connector"
        )

    cycle = 0
    with dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=db_token,
    ) as conn:
        while True:
            cycle += 1
            cycle_ts = datetime.datetime.utcnow()
            ts_label = cycle_ts.strftime("%Y-%m-%dT%H:%M:%S")

            with conn.cursor() as cur:
                # ── 1. Discover current max product_id ────────────────────────
                cur.execute(
                    f"SELECT COALESCE(MAX(product_id), 0) "
                    f"FROM `{CATALOG}`.`{SCHEMA}`.`{TABLE}`"
                )
                max_id = cur.fetchone()[0]
                start_id = max_id + 1
                end_id   = max_id + BATCH_SIZE

                rows = _gen_rows(start_id, BATCH_SIZE, cycle_ts)
                insert_sql = (
                    f"INSERT INTO `{CATALOG}`.`{SCHEMA}`.`{TABLE}`\n"
                    f"  (product_id, product_name, category, sub_category, brand, sku,\n"
                    f"   unit_price, cost_price, stock_quantity, reorder_level, weight_kg,\n"
                    f"   is_active, created_at, updated_at, snap_id, snap_timestamp)\n"
                    f"VALUES\n{_build_values(rows)}"
                )

                if DRY_RUN:
                    print(f"\n[{ts_label}] DRY_RUN Cycle {cycle} — would insert "
                          f"{BATCH_SIZE} rows (product_id {start_id}–{end_id}):")
                    print(insert_sql)
                else:
                    # ── 2. Insert the batch ───────────────────────────────────
                    cur.execute(insert_sql)
                    logger.info(
                        "[%s] Cycle %d — inserted %d rows "
                        "(product_id %d–%d, created_at=%s)",
                        ts_label, cycle, BATCH_SIZE, start_id, end_id, ts_label,
                    )

            # ── 3. Run starpump (skip in DRY_RUN) ────────────────────────────
            if not DRY_RUN:
                try:
                    master_pod = _get_master_pod()
                except Exception as exc:
                    logger.error("Could not resolve master pod: %s", exc)
                    exit_code = -1
                else:
                    exit_code = _run_starpump(master_pod, token)

                status = "✅" if exit_code == 0 else f"❌ (exit={exit_code})"
                logger.info(
                    "[%s] %s Cycle %d complete — starpump exit=%d. "
                    "Next cycle in %ds.",
                    ts_label, status, cycle, exit_code, INTERVAL_SECONDS,
                )
                if exit_code != 0:
                    logger.warning(
                        "starpump exited non-zero (%d) on cycle %d. "
                        "The loop will continue — check output above for details.",
                        exit_code, cycle,
                    )
            else:
                logger.info(
                    "[%s] DRY_RUN Cycle %d — skipping starpump. "
                    "Next cycle in %ds.",
                    ts_label, cycle, INTERVAL_SECONDS,
                )

            # ── 4. Wait before the next cycle ────────────────────────────────
            try:
                time.sleep(INTERVAL_SECONDS)
            except KeyboardInterrupt:
                logger.info("Interrupted by user — stopping after %d cycle(s).", cycle)
                sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.")
        sys.exit(0)
