#!/usr/bin/env python3
"""
00_catalog_bootstrap.py
=======================
Spark Iceberg catalog pre-flight bootstrap.

Creates and validates the three Iceberg REST catalog namespaces:
  • postgres  → namespace: cache_testing  (source: PostgreSQL cache_testing DB)
  • oracle    → namespace: tpcds           (source: Oracle XEPDB1.TPCDS schema)
  • mongodb   → namespace: cache_testing  (source: MongoDB cache_testing DB)

Design
------
• Idempotent — safe to run multiple times; CREATE NAMESPACE IF NOT EXISTS is used.
• Called as a pre-flight step by starpump and the Debezium pipeline startup before
  any data copy or capture begins.
• Validates live connectivity to the Polaris REST catalog for each namespace by
  running SHOW NAMESPACES after creation.
• All credentials from OpenBao — never hardcoded.
• Can be imported as a module (call bootstrap_all_catalogs) or run standalone.

Usage
-----
  # Standalone pre-flight check:
  SPARK_USER=dave python3 00_catalog_bootstrap.py

  # Programmatic (from starpump or streaming job):
  from 00_catalog_bootstrap import bootstrap_all_catalogs
  bootstrap_all_catalogs(spark, bao)

  # Dry-run (validate only — do not create):
  DRY_RUN=1 SPARK_USER=dave python3 00_catalog_bootstrap.py
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from bao_spark_init import BaoSparkInit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("catalog-bootstrap")

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_USER = os.environ.get("SPARK_USER", "dave")
DRY_RUN    = os.environ.get("DRY_RUN", "0") == "1"

# Catalog → namespace mapping (catalog name = technology, namespace = source DB name)
# Each entry: (catalog_name, namespace, description)
@dataclass
class _CatalogSpec:
    catalog:     str
    namespace:   str
    description: str


_CATALOG_SPECS: list[_CatalogSpec] = [
    _CatalogSpec(
        catalog     = "postgres",
        namespace   = "cache_testing",
        description = "PostgreSQL cache_testing DB → Iceberg (pg_lakehouse)",
    ),
    _CatalogSpec(
        catalog     = "oracle",
        namespace   = "tpcds",
        description = "Oracle XEPDB1.TPCDS schema → Iceberg (ora_lakehouse)",
    ),
    _CatalogSpec(
        catalog     = "mongodb",
        namespace   = "cache_testing",
        description = "MongoDB cache_testing DB → Iceberg (mgo_lakehouse)",
    ),
]


def _ensure_namespace(spark: SparkSession, spec: _CatalogSpec) -> None:
    """
    Create a Polaris REST catalog namespace if it does not already exist.

    Uses CREATE NAMESPACE IF NOT EXISTS so multiple callers (starpump threads,
    streaming job startup) can call this concurrently without error.

    Raises on any Spark/Iceberg error — caller should handle and log.
    """
    fqn = f"`{spec.catalog}`.`{spec.namespace}`"
    logger.info(
        "[%s] Ensuring namespace %s … (%s)",
        spec.catalog, fqn, spec.description,
    )

    if DRY_RUN:
        logger.info("[%s] DRY_RUN — skipping CREATE NAMESPACE.", spec.catalog)
        return

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {fqn}"
    )
    logger.info("[%s] Namespace %s ready.", spec.catalog, fqn)


def _validate_namespace(spark: SparkSession, spec: _CatalogSpec) -> bool:
    """
    Validate live connectivity to the catalog by running SHOW NAMESPACES.

    Returns True if the namespace is visible, False otherwise.
    Logs a detailed error on failure (connectivity issue, OAuth2 failure, etc.).
    """
    try:
        result = spark.sql(
            f"SHOW NAMESPACES IN `{spec.catalog}`"
        ).collect()
        namespaces = [r[0].lower() for r in result]
        if spec.namespace.lower() in namespaces:
            logger.info(
                "[%s] ✓ Validation passed — namespace '%s' confirmed in catalog.",
                spec.catalog, spec.namespace,
            )
            return True
        else:
            logger.warning(
                "[%s] ✗ Namespace '%s' not found after creation. "
                "Found: %s. Possible Polaris sync lag — retrying is safe.",
                spec.catalog, spec.namespace, namespaces,
            )
            return False
    except Exception as exc:
        logger.error(
            "[%s] ✗ Connectivity validation failed: %s\n"
            "  Check Polaris REST at polaris-rest.prod.svc.cluster.local:8181\n"
            "  and OAuth2 credentials in secret/data/platform/polaris.",
            spec.catalog, exc,
        )
        return False


def bootstrap_all_catalogs(
    spark: "SparkSession",
    bao:   "BaoSparkInit | None" = None,
    *,
    fail_fast: bool = True,
) -> dict[str, bool]:
    """
    Idempotent pre-flight bootstrap for all three CDC source catalogs.

    Creates namespaces and validates connectivity.  Call this before any
    data copy or Kafka streaming job begins.

    Args:
        spark:     Active SparkSession (must have catalog credentials wired via
                   BaoSparkInit.spark_conf()).
        bao:       Optional BaoSparkInit instance (unused directly here — the
                   credentials are already embedded in the SparkConf by the
                   caller).  Accepted for API uniformity with other pipeline modules.
        fail_fast: If True (default), raises RuntimeError when any catalog fails
                   validation.  Set to False to continue despite partial failures
                   (useful for debugging individual catalog issues).

    Returns:
        Dict mapping catalog name to bool (True = validation passed).

    Raises:
        RuntimeError: If fail_fast=True and any catalog fails to validate.
    """
    logger.info(
        "=== Catalog Bootstrap | user=%s | dry_run=%s | catalogs=%s ===",
        SPARK_USER,
        DRY_RUN,
        [f"{s.catalog}.{s.namespace}" for s in _CATALOG_SPECS],
    )

    results: dict[str, bool] = {}
    failures: list[str] = []

    for spec in _CATALOG_SPECS:
        try:
            _ensure_namespace(spark, spec)
        except Exception as exc:
            logger.error(
                "[%s] Failed to create namespace '%s': %s",
                spec.catalog, spec.namespace, exc,
            )
            results[spec.catalog] = False
            failures.append(spec.catalog)
            if fail_fast:
                break
            continue

        ok = _validate_namespace(spark, spec)
        results[spec.catalog] = ok
        if not ok:
            failures.append(spec.catalog)
            if fail_fast:
                break

    # Summary
    logger.info("─" * 60)
    for spec in _CATALOG_SPECS:
        status = "✓ OK" if results.get(spec.catalog) else "✗ FAILED"
        logger.info(
            "  %s  %s.%s  —  %s",
            status, spec.catalog, spec.namespace, spec.description,
        )
    logger.info("─" * 60)

    if failures and fail_fast:
        raise RuntimeError(
            f"Catalog bootstrap failed for: {failures}. "
            "Check Polaris REST connectivity and OpenBao credentials. "
            "See logged errors above for details."
        )

    return results


def bootstrap_single_catalog(
    spark:     "SparkSession",
    catalog:   str,
    namespace: str,
) -> bool:
    """
    Ensure a single catalog namespace exists and return its validation status.

    Convenience wrapper used by starpump incremental mode and streaming consumer
    when they only need to bootstrap their own target catalog.

    Args:
        spark:     Active SparkSession.
        catalog:   Catalog name (e.g. "postgres", "oracle", "mongodb").
        namespace: Namespace/database name (e.g. "cache_testing", "tpcds").

    Returns:
        True if namespace exists and is reachable.
    """
    spec = _CatalogSpec(
        catalog     = catalog,
        namespace   = namespace,
        description = f"{catalog}.{namespace} (single bootstrap)",
    )
    try:
        _ensure_namespace(spark, spec)
    except Exception as exc:
        logger.error("[%s] Failed to ensure namespace '%s': %s", catalog, namespace, exc)
        return False
    return _validate_namespace(spark, spec)


# ── Standalone entry-point ────────────────────────────────────────────────────

def main() -> None:
    os.environ["SPARK_USER"] = SPARK_USER

    logger.info(
        "=== Catalog Bootstrap (standalone) | user=%s | dry_run=%s ===",
        SPARK_USER, DRY_RUN,
    )

    from bao_spark_init import BaoSparkInit
    bao  = BaoSparkInit()
    conf = bao.spark_conf(app_name="catalog-bootstrap")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        results = bootstrap_all_catalogs(spark, bao, fail_fast=False)
        failed  = [k for k, v in results.items() if not v]
        if failed:
            logger.error("Bootstrap completed with failures: %s", failed)
            sys.exit(1)
        else:
            logger.info("All catalogs bootstrapped and validated successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
