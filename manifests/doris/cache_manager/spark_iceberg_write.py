#!/usr/bin/env python3
"""
spark_iceberg_write.py
======================
PySpark write-pushdown job submitted by the Doris Cache Manager.

Receives a single JSON argument containing:
  catalog   — Doris/Spark catalog name (polaris, databricks, postgres, oracle, mongodb)
  warehouse — Polaris warehouse name   (IcebergCatalog, star_lakehouse, …)
  db        — Iceberg database / namespace
  table     — table name
  stmt      — original DML SQL statement (INSERT / UPDATE / DELETE / MERGE)

The job:
1. Loads credentials from OpenBao using K8s SA JWT authentication
   (same mechanism as bao_spark_init.py).
2. Builds a SparkConf wiring the target Iceberg catalog via Polaris REST
   with full OAuth2 credentials.
3. Executes the DML statement via spark.sql() inside the correct catalog
   context — Spark has full Iceberg read/write access.

Usage (called by WriteInterceptor via Spark REST API — not invoked directly):
  spark-submit spark_iceberg_write.py '<json_args>'

Credentials:
  All credentials come from OpenBao at runtime — never hardcoded.
  Secret paths:
    secret/data/platform/polaris  → spark_svc_id, spark_svc_secret
    secret/data/platform/s3       → access_key, secret_key, endpoint, region
"""
from __future__ import annotations

import json
import logging
import os
import sys
import urllib.request

from pyspark.sql import SparkSession
from pyspark import SparkConf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s spark-iceberg-write — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("spark-iceberg-write")

# ─────────────────────────────────────────────────────────────────────────────
# OpenBao constants (mirrors bao_spark_init.py)
# ─────────────────────────────────────────────────────────────────────────────
_BAO_IN_CLUSTER  = "http://openbao.prod.svc.cluster.local:8200"
_K8S_SA_JWT_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_PATH_POLARIS    = "secret/data/platform/polaris"
_PATH_S3         = "secret/data/platform/s3"
_POLARIS_URI     = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"
_SPARK_MASTER    = "spark://spark-master-internal.prod.svc.cluster.local:17077"

BAO_ADDR = os.environ.get("ADDR") or os.environ.get("BAO_ADDR", _BAO_IN_CLUSTER)
BAO_ROLE = os.environ.get("BAO_ROLE", "platform-secrets-read")


# ─────────────────────────────────────────────────────────────────────────────
# Minimal OpenBao client (stdlib only — no extra deps on Spark executors)
# ─────────────────────────────────────────────────────────────────────────────
def _bao_token() -> str:
    if tok := (os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN")):
        return tok
    if os.path.exists(_K8S_SA_JWT_FILE):
        with open(_K8S_SA_JWT_FILE) as fh:
            jwt = fh.read().strip()
        payload = json.dumps({"role": BAO_ROLE, "jwt": jwt}).encode()
        req = urllib.request.Request(
            f"{BAO_ADDR}/v1/auth/kubernetes/login",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())["auth"]["client_token"]
    raise RuntimeError(
        f"Cannot authenticate to OpenBao: no TOKEN env-var and no SA JWT at {_K8S_SA_JWT_FILE}"
    )


def _read_secret(path: str, token: str) -> dict[str, str]:
    req = urllib.request.Request(
        f"{BAO_ADDR}/v1/{path}",
        headers={"X-Vault-Token": token},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    outer = data.get("data", {})
    return outer.get("data", outer)


# ─────────────────────────────────────────────────────────────────────────────
# SparkConf builder for a single catalog
# ─────────────────────────────────────────────────────────────────────────────
def _build_conf(catalog: str, warehouse: str, pol: dict, s3: dict) -> SparkConf:
    polaris_uri = pol.get("url") or _POLARIS_URI
    conf = SparkConf()
    conf.setAppName(f"doris-write-pushdown-{catalog}")
    conf.setMaster(_SPARK_MASTER)

    conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    # ── Iceberg catalog wiring (Polaris REST + OAuth2) ─────────────────────
    conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{catalog}.type",             "rest")
    conf.set(f"spark.sql.catalog.{catalog}.uri",              polaris_uri)
    conf.set(f"spark.sql.catalog.{catalog}.oauth2-server-uri", f"{polaris_uri}/v1/oauth/tokens")
    conf.set(f"spark.sql.catalog.{catalog}.credential",
             f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
    conf.set(f"spark.sql.catalog.{catalog}.scope",            "PRINCIPAL_ROLE:ALL")
    conf.set(f"spark.sql.catalog.{catalog}.warehouse",        warehouse)
    conf.set(f"spark.sql.catalog.{catalog}.rest.auth.type",   "oauth2")

    # ── S3 / Iceberg S3FileIO ──────────────────────────────────────────────
    conf.set(f"spark.sql.catalog.{catalog}.s3.access-key-id",     s3["access_key"])
    conf.set(f"spark.sql.catalog.{catalog}.s3.secret-access-key", s3["secret_key"])
    conf.set(f"spark.sql.catalog.{catalog}.s3.endpoint",          s3["endpoint"])
    conf.set(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
    conf.set(f"spark.sql.catalog.{catalog}.client.region",        s3["region"])

    conf.set("spark.hadoop.fs.s3a.access.key",          s3["access_key"])
    conf.set("spark.hadoop.fs.s3a.secret.key",          s3["secret_key"])
    conf.set("spark.hadoop.fs.s3a.endpoint",            s3["endpoint"])
    conf.set("spark.hadoop.fs.s3a.endpoint.region",     s3["region"])
    conf.set("spark.hadoop.fs.s3a.impl",
             "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.path.style.access",   "true")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "false")

    return conf


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    if len(sys.argv) < 2:
        logger.error("Usage: spark_iceberg_write.py '<json_args>'")
        sys.exit(1)

    args = json.loads(sys.argv[1])
    catalog   = args["catalog"]
    warehouse = args["warehouse"]
    db        = args["db"]
    table     = args["table"]
    stmt      = args["stmt"]

    logger.info(
        "Write-pushdown job: catalog=%s warehouse=%s db=%s table=%s",
        catalog, warehouse, db, table,
    )
    logger.info("Statement: %s", stmt[:200] + ("…" if len(stmt) > 200 else ""))

    # ── Load credentials from OpenBao ──────────────────────────────────────
    token = _bao_token()
    pol   = _read_secret(_PATH_POLARIS, token)
    s3    = _read_secret(_PATH_S3, token)
    logger.info("Credentials loaded from OpenBao.")

    # ── Build SparkSession ─────────────────────────────────────────────────
    conf  = _build_conf(catalog, warehouse, pol, s3)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Switch into catalog context and execute ─────────────────────────────
    # Fully qualify the statement in the current catalog so the SQL context
    # resolves table references correctly even if the original statement
    # used unqualified names.
    spark.sql(f"USE {catalog}.{db}")
    logger.info("Executing DML via Spark SQL…")

    try:
        result_df = spark.sql(stmt)
        # Trigger execution for statements that return a DataFrame
        # (e.g. INSERT … SELECT returns row-count in some Iceberg versions).
        count = result_df.count() if result_df is not None else 0
        logger.info(
            "Write-pushdown SUCCESS: catalog=%s db=%s table=%s affected_rows=%d",
            catalog, db, table, count,
        )
    except Exception as exc:
        logger.error(
            "Write-pushdown FAILED: catalog=%s db=%s table=%s error=%s",
            catalog, db, table, exc,
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
