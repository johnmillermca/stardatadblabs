#!/usr/bin/env python3
"""
starpump_to_databricks.py
─────────────────────────
STARPUMP extension: copies an Iceberg table from Polaris (star_lakehouse)
into Databricks Unity Catalog using the Databricks REST API + Delta/Iceberg
foreign table registration.

Steps performed:
  1. Read credentials from OpenBao (databricks/pat, databricks/s3, platform/polaris)
  2. Ensure the star_lakehouse Unity Catalog catalog exists in Databricks
     (creates an EXTERNAL catalog pointing at Polaris REST endpoint)
  3. Register the Iceberg namespace + table as a Unity Catalog table
  4. Copy data from Polaris Iceberg → Databricks managed Delta table
     using Spark (CTAS from Iceberg foreign table)

Environment variables:
  POLARIS_CATALOG    default: star_lakehouse
  POLARIS_NS         default: demo
  POLARIS_TABLE      default: customers
  DB_CATALOG         default: star_lakehouse   (Unity Catalog catalog name)
  DB_SCHEMA          default: demo
  DB_TABLE           default: customers
  ADDR               OpenBao address (default: http://openbao.prod.svc.cluster.local:8200)
  TOKEN              OpenBao token override (dev/bootstrap only)
"""

import os
import sys
import json
import urllib.request
import urllib.error

sys.path.insert(0, "/opt/spark/scripts")
sys.path.insert(0, "/app/scripts")

from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

# ── Config ─────────────────────────────────────────────────────────────────────
POLARIS_CATALOG = os.getenv("POLARIS_CATALOG", "star_lakehouse")
POLARIS_NS      = os.getenv("POLARIS_NS",      "demo")
POLARIS_TABLE   = os.getenv("POLARIS_TABLE",   "customers")
DB_CATALOG      = os.getenv("DB_CATALOG",      "star_lakehouse")
DB_SCHEMA       = os.getenv("DB_SCHEMA",       "demo")
DB_TABLE        = os.getenv("DB_TABLE",        "customers")


def bao_get(bao: BaoSparkInit, path: str, key: str) -> str:
    return bao.get_secret(path, key)


def db_request(workspace: str, token: str, method: str,
               path: str, body: dict = None) -> dict:
    url = f"{workspace}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode(errors="replace")
        # 409 = already exists — treat as success
        if e.code == 409:
            print(f"  [db] {method} {path} → already exists (409), continuing")
            return {}
        print(f"  [db] ERROR {e.code} {method} {path}: {body_txt[:300]}", file=sys.stderr)
        raise


def ensure_db_catalog(workspace: str, token: str,
                      polaris_url: str, polaris_id: str, polaris_secret: str) -> None:
    """Create the star_lakehouse catalog in Databricks Unity Catalog as FOREIGN."""
    print(f"[starpump_to_databricks] ensuring Unity Catalog catalog: {DB_CATALOG}")
    db_request(workspace, token, "POST",
               "/api/2.1/unity-catalog/catalogs",
               {
                   "name": DB_CATALOG,
                   "catalog_type": "FOREIGN",
                   "connection_name": f"polaris_{DB_CATALOG}",
                   "options": {
                       "catalog_type": "ICEBERG_REST",
                       "uri": f"{polaris_url}/api/catalog",
                       "token": _get_polaris_token(polaris_id, polaris_secret),
                       "warehouse": POLARIS_CATALOG,
                   }
               })


def _get_polaris_token(client_id: str, client_secret: str) -> str:
    url = "http://192.168.1.50:30181/api/catalog/v1/oauth/tokens"
    data = (f"grant_type=client_credentials&client_id={client_id}"
            f"&client_secret={client_secret}&scope=PRINCIPAL_ROLE:ALL").encode()
    req = urllib.request.Request(url, data=data, method="POST",
                                  headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())["access_token"]


def copy_via_spark(bao: BaoSparkInit, workspace: str, token: str) -> None:
    """CTAS: read Polaris Iceberg → write Databricks managed Delta table."""
    pol = bao.polaris_creds()
    s3  = bao.s3_creds()
    conf = bao.spark_conf(app_name="starpump_to_databricks")

    # Polaris source catalog (star_lakehouse — separate from the default 'polaris' entry)
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.type", "rest")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.uri",
             "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.credential",
             f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.scope",    "PRINCIPAL_ROLE:ALL")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.warehouse", POLARIS_CATALOG)
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.header.X-Iceberg-Access-Delegation",
             "vended-credentials")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.s3.access-key-id",     s3["access_key"])
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.s3.secret-access-key", s3["secret_key"])
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.s3.endpoint",          s3["endpoint"])
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.s3.path-style-access", "true")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.client.region",        s3["region"])

    # Databricks target catalog via Unity Catalog JDBC
    db_creds = bao.db_creds() if hasattr(bao, "db_creds") else {"token": token, "workspace": workspace}
    db_token = db_creds.get("token", token)
    db_ws    = db_creds.get("workspace", workspace)
    warehouse_id = "2c23ed9f013093c4"   # Serverless Starter Warehouse
    jdbc_url = (f"jdbc:databricks://{db_ws.replace('https://','')}"
                f"/default;transportMode=http;ssl=1"
                f";httpPath=/sql/1.0/warehouses/{warehouse_id}"
                f";AuthMech=3;UID=token;PWD={db_token}")

    spark = SparkSession.builder.config(conf=conf) \
        .appName("starpump_to_databricks") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    src = f"{POLARIS_CATALOG}.{POLARIS_NS}.{POLARIS_TABLE}"
    print(f"[starpump_to_databricks] reading {src} ...")
    df = spark.table(src)
    count_src = df.count()
    print(f"[starpump_to_databricks] source rows: {count_src}")

    print(f"[starpump_to_databricks] writing to Databricks via JDBC ...")
    df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", f"{DB_CATALOG}.{DB_SCHEMA}.{DB_TABLE}") \
      .option("createTableOptions",
              "USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')") \
      .mode("overwrite") \
      .save()

    print(f"[starpump_to_databricks] ✅ {count_src} rows copied to "
          f"{DB_CATALOG}.{DB_SCHEMA}.{DB_TABLE}")
    spark.stop()


def main():
    bao      = BaoSparkInit()
    db       = bao._read_secret("secret/data/databricks/pat")
    workspace    = db["workspace"]
    db_token     = db["token"]
    pol          = bao.polaris_creds()
    polaris_url  = pol.get("url", "http://192.168.1.50:30181")
    polaris_id   = pol["spark_svc_id"]
    polaris_sec  = pol["spark_svc_secret"]

    ensure_db_catalog(workspace, db_token, polaris_url, polaris_id, polaris_sec)
    copy_via_spark(bao, workspace, db_token)


if __name__ == "__main__":
    main()
