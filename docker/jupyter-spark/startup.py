"""
00-spark-auto-init.py
=====================
IPython kernel startup script — runs automatically every time a Jupyter kernel
starts inside a JupyterHub singleuser pod.

What this does
--------------
1. Reads all platform credentials from OpenBao (in-cluster SA JWT auth).
2. Builds a fully-configured SparkConf via BaoSparkInit — all Iceberg catalogs
   (postgres, oracle, mongodb, polaris, databricks), S3 credentials, and the
   Gluten/Velox plugin are wired automatically.
3. Creates a SparkSession connected to the cluster Spark master and exposes it
   as the global `spark` variable in every notebook kernel.
4. Registers a `sql()` shortcut so you can run Spark SQL without any boilerplate.
5. Prints a short summary of available catalogs when the kernel starts.

After this script runs you can start coding immediately:
    # Spark SQL — no setup needed
    sql("SHOW NAMESPACES IN polaris").show()
    sql("SELECT * FROM postgres.st_transforms.filter_op__postgres__customers LIMIT 5").show()

    # Or use the spark global directly
    spark.sql("SHOW TABLES IN `polaris`.`pg_lakehouse`").show()

    # IcebergTableBuilder is also imported and ready
    from spark_iceberg_utils import IcebergTableBuilder
    builder = IcebergTableBuilder(spark)

Environment variables (set automatically by KubeSpawner via values.yaml)
--------------------------
  SPARK_LOCAL_IP   — pod IP for Spark driver binding (injected via Downward API)
  SPARK_USER       — defaults to the JupyterHub logged-in username
  DISABLE_GLUTEN   — set to "1" to disable Gluten/Velox (for debugging)

If initialisation fails (OpenBao unreachable, credentials missing, etc.) the
error is printed to the notebook and `spark` is set to None so other cells
can detect the failure and show a helpful message.
"""

from __future__ import annotations

import logging
import os
import socket
import sys

# ── Path setup ────────────────────────────────────────────────────────────────
# bao_spark_init.py and spark_iceberg_utils.py are installed at site-packages
# so they are already importable — no sys.path manipulation needed.

logging.basicConfig(
    level=logging.WARNING,           # suppress Spark/Py4J noise in notebooks
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
_log = logging.getLogger("jupyter-spark-init")

# ── Determine running user ─────────────────────────────────────────────────────
# JupyterHub sets JUPYTERHUB_USER; fall back to USER / "jovyan"
_jh_user = (
    os.environ.get("JUPYTERHUB_USER")
    or os.environ.get("USER")
    or "jovyan"
)
os.environ.setdefault("SPARK_USER", _jh_user)

# Ensure SPARK_LOCAL_IP is set so the driver binds to a routable pod IP.
# KubeSpawner injects this via Downward API; fall back to hostname resolution.
if not os.environ.get("SPARK_LOCAL_IP"):
    try:
        os.environ["SPARK_LOCAL_IP"] = socket.gethostbyname(socket.gethostname())
    except Exception:
        pass

# ── Initialise Spark ──────────────────────────────────────────────────────────
spark = None
_init_error = None

try:
    print("🔄  Connecting to OpenBao and starting Spark session…", flush=True)

    from bao_spark_init import BaoSparkInit
    from pyspark.sql import SparkSession

    _bao  = BaoSparkInit()
    _conf = _bao.spark_conf(app_name=f"jupyter-{_jh_user}")

    # Notebook-friendly tuning — smaller shuffle, AQE on
    _conf.set("spark.sql.shuffle.partitions",                    "8")
    _conf.set("spark.sql.adaptive.enabled",                      "true")
    _conf.set("spark.sql.adaptive.coalescePartitions.enabled",   "true")
    # Reduce executor overhead for interactive use
    _conf.set("spark.executor.instances",                        "2")
    _conf.set("spark.executor.cores",                            "2")
    _conf.set("spark.executor.memory",                           "2g")
    _conf.set("spark.memory.offHeap.enabled",                    "true")
    _conf.set("spark.memory.offHeap.size",                       "1g")

    spark = SparkSession.builder.config(conf=_conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"✅  Spark {spark.version} ready  |  user={_jh_user}  |  master={spark.sparkContext.master}", flush=True)

except Exception as _exc:
    _init_error = _exc
    print(f"❌  Spark initialisation failed: {_exc}", flush=True)
    print("    Set DISABLE_GLUTEN=1 and restart the kernel to try without Velox.", flush=True)
    spark = None

# ── Register globals in the IPython namespace + builtins ──────────────────────
# Two-layer injection so spark/sql() are always reachable:
#   1. builtins — available in every Python scope immediately, no timing dependency
#   2. IPython user_ns via push() — shows up in tab-completion and %whos
import builtins as _builtins

if spark is not None:
    def sql(query: str, **kwargs):
        """Shortcut for spark.sql(). Returns a Spark DataFrame."""
        return spark.sql(query, **kwargs)
else:
    sql = None  # type: ignore

# Inject into builtins first — works even if IPython isn't ready yet
_builtins.spark = spark  # type: ignore
_builtins.sql   = sql    # type: ignore

# Also push into IPython user namespace for tab-completion / %whos
try:
    _ip = get_ipython()   # noqa: F821 — available in IPython kernel context
    if _ip is not None:
        _ip.push({"spark": spark, "sql": sql})
except Exception:
    pass   # Not in an IPython context (e.g. unit test) — skip push

# ── Print available catalogs if Spark is up ───────────────────────────────────
if spark is not None:
    try:
        _cats = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
        print(f"📦  Available catalogs: {_cats}", flush=True)
        print("    Usage examples:", flush=True)
        print("      sql(\"SHOW NAMESPACES IN polaris\").show()", flush=True)
        print("      sql(\"SHOW TABLES IN `postgres`.`st_transforms`\").show()", flush=True)
        print("      spark.sql(\"SELECT * FROM `polaris`.`pg_lakehouse`.`<table>` LIMIT 5\").show()", flush=True)
    except Exception as _ce:
        print(f"    (Could not list catalogs: {_ce})", flush=True)
