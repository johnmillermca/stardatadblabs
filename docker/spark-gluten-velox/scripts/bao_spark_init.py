"""
bao_spark_init.py
=================
OpenBao credential loader for Spark jobs.

Reads all platform secrets from OpenBao (in-cluster address) and returns a
fully-configured SparkConf + credential dict so that no passwords are
hard-coded anywhere.

Authentication order
--------------------
1. K8s Service Account JWT  (used inside pods – role: platform-secrets-read)
2. Root / bootstrap token   (env-var BAO_TOKEN – dev/local override only)

Usage
-----
from bao_spark_init import BaoSparkInit

init = BaoSparkInit()
conf = init.spark_conf(app_name="snowflake-to-iceberg")
snowflake_creds = init.snowflake_creds()
s3_creds        = init.s3_creds()

spark = SparkSession.builder.config(conf=conf).getOrCreate()
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any

from pyspark import SparkConf

logger = logging.getLogger(__name__)

# ── OpenBao addresses ──────────────────────────────────────────────────────────
_BAO_IN_CLUSTER  = "http://openbao.prod.svc.cluster.local:8200"
_BAO_NODEPORT    = "http://192.168.1.50:30820"
_K8S_SA_JWT_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"

# ── Secret paths (OpenBao KV v2 — read via secret/data/<path>) ───────────────
_PATH_S3        = "secret/data/platform/s3"
_PATH_SNOWFLAKE = "secret/data/platform/snowflake"
_PATH_POLARIS   = "secret/data/platform/polaris"
_PATH_DORIS     = "secret/data/platform/doris"
_PATH_ORACLE       = "secret/data/platform/oracle"
_PATH_KAFKA        = "secret/data/platform/kafka"
_PATH_PIPELINE_DB  = "secret/data/platform/pipeline_db"

# JARs baked into the spark-gluten-velox:3.5.1 image
_ICEBERG_JAR_NAME    = "iceberg-spark-runtime-3.5_2.12-1.9.2.jar"
_ICEBERG_JAR_PATH    = f"/opt/spark/jars/{_ICEBERG_JAR_NAME}"
_SNOWFLAKE_JAR_NAME  = "spark-snowflake_2.12-3.2.1-spark_3.5.jar"
_SNOWFLAKE_JAR_PATH  = f"/opt/spark/jars/{_SNOWFLAKE_JAR_NAME}"
# spark-snowflake 3.2.1 requires JDBC 4.x (internal API package restructure)
_SNOWFLAKE_JDBC_JAR  = "/opt/spark/jars/snowflake-jdbc-4.0.2.jar"

_POLARIS_URI = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"
# In-cluster drivers connect directly on port 17077 (bypasses krb-spark-guard sidecar).
# External spark-submit should use spark-master-svc:7077 (guard-proxied NodePort).
_SPARK_MASTER = "spark://spark-master-internal.prod.svc.cluster.local:17077"


# ─────────────────────────────────────────────────────────────────────────────
class BaoSparkInit:
    """
    Fetch credentials from OpenBao and expose them as SparkConf / dicts.
    All network calls happen lazily and results are cached per instance.
    """

    def __init__(
        self,
        bao_address: str | None = None,
        bao_role: str = "platform-secrets-read",
        k8s_auth_path: str = "auth/kubernetes/login",
    ) -> None:
        self._address = bao_address or os.environ.get("BAO_ADDR", _BAO_IN_CLUSTER)
        self._role = bao_role
        self._k8s_auth_path = k8s_auth_path
        self._token: str | None = None
        self._cache: dict[str, dict] = {}

    # ── Authentication ─────────────────────────────────────────────────────────
    def _get_token(self) -> str:
        if self._token:
            return self._token

        # 1. Explicit env override (dev/bootstrap only)
        if env_tok := os.environ.get("BAO_TOKEN"):
            logger.info("Using BAO_TOKEN from environment (dev mode).")
            self._token = env_tok
            return self._token

        # 2. K8s Service Account JWT
        if os.path.exists(_K8S_SA_JWT_FILE):
            with open(_K8S_SA_JWT_FILE) as fh:
                jwt = fh.read().strip()
            payload = json.dumps({"role": self._role, "jwt": jwt}).encode()
            url = f"{self._address}/v1/{self._k8s_auth_path}"
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            self._token = data["auth"]["client_token"]
            logger.info("Authenticated to OpenBao via K8s SA JWT (role=%s).", self._role)
            return self._token

        raise RuntimeError(
            "Cannot authenticate to OpenBao: no BAO_TOKEN env-var and "
            f"no K8s SA JWT at {_K8S_SA_JWT_FILE}"
        )

    # ── Secret reading ─────────────────────────────────────────────────────────
    def _read_secret(self, path: str) -> dict[str, str]:
        if path in self._cache:
            return self._cache[path]
        token = self._get_token()
        url = f"{self._address}/v1/{path}"
        req = urllib.request.Request(
            url, headers={"X-Vault-Token": token}, method="GET"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        # KV v2: response is { data: { data: { key: val }, metadata: {...} } }
        outer = data.get("data", {})
        secret_data: dict[str, str] = outer.get("data", outer)
        self._cache[path] = secret_data
        return secret_data

    # ── Public credential accessors ────────────────────────────────────────────
    def s3_creds(self) -> dict[str, str]:
        """Return {'access_key', 'secret_key', 'region', 'endpoint', 'bucket'}."""
        return self._read_secret(_PATH_S3)

    def snowflake_creds(self) -> dict[str, str]:
        """Return {'account', 'user', 'password', 'warehouse'}."""
        return self._read_secret(_PATH_SNOWFLAKE)

    def polaris_creds(self) -> dict[str, str]:
        """Return {'spark_svc_id', 'spark_svc_secret', ...}."""
        return self._read_secret(_PATH_POLARIS)

    def doris_creds(self) -> dict[str, str]:
        """Return {'admin_password', ...}."""
        return self._read_secret(_PATH_DORIS)

    def oracle_creds(self) -> dict[str, str]:
        """Return {'user', 'password', 'host', 'port', 'sid', 'jdbc_url'}."""
        return self._read_secret(_PATH_ORACLE)

    def kafka_creds(self) -> dict[str, str]:
        """Return {'debezium_user', 'debezium_password', 'bootstrap', 'schema_registry'}."""
        return self._read_secret(_PATH_KAFKA)

    def pipeline_db_creds(self) -> dict[str, str]:
        """
        Return credentials for the dedicated `pipeline` PostgreSQL database.
        Keys: host, port, database, user, password, jdbc_url.
        This DB stores pipeline_watermarks and pipeline_run_log — the
        authoritative CDC sync-point tables that Debezium reads without Spark.
        """
        return self._read_secret(_PATH_PIPELINE_DB)

    # ── SparkConf builder ──────────────────────────────────────────────────────
    def spark_conf(
        self,
        app_name: str = "iceberg-job",
        extra_conf: dict[str, str] | None = None,
    ) -> SparkConf:
        """
        Build a SparkConf pre-wired with:
          - Polaris REST catalog  (catalog name: polaris)
          - Snowflake catalog     (catalog name: snowflake_sample)
          - S3 credentials (AWS SDK v2 style)
          - Parquet + Iceberg defaults
        """
        s3  = self.s3_creds()
        sf  = self.snowflake_creds()
        pol = self.polaris_creds()

        conf = SparkConf()
        conf.setAppName(app_name)
        conf.setMaster(_SPARK_MASTER)

        # ── Driver host: use pod IP so executors on other nodes can reach it ──
        # Default Spark behaviour advertises the pod hostname
        # (e.g. spark-master-6ddc5d8578-8qxbd) which is not resolvable by
        # other pods via DNS.  Binding to the pod IP fixes
        # "UnknownHostException: spark-master-XXXXXXXX" in executor logs.
        import socket as _socket
        _pod_ip = os.environ.get(
            "SPARK_LOCAL_IP",
            _socket.gethostbyname(_socket.gethostname()),
        )
        conf.set("spark.driver.host",      _pod_ip)
        conf.set("spark.driver.bindAddress", _pod_ip)

        # ── Java 17 module opens for Snowflake JDBC 4.x ───────────────────────
        # JDBC 4.x uses reflection-based serialization internally (Unsafe, ObjectInputStream
        # on internal JDK classes) which Java 17 blocks by default. Required for
        # SnowflakeResultSetSerializableV1 static initializer and query execution.
        #
        # The -Dlog4j.configurationFile flag is included here so that this single
        # string is the authoritative value of extraJavaOptions for both driver and
        # executor.  spark-defaults.conf sets the same property as a baseline, but
        # SparkConf.set() called here takes precedence — omitting the log4j flag
        # here would silently drop it and revert to the default log4j2 config on
        # the classpath, losing the console-filter and rolling-file appender.
        _log4j_flag = "-Dlog4j.configurationFile=/opt/spark/conf/log4j2.properties"
        _opens = " ".join([
            _log4j_flag,
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        ])
        conf.set("spark.driver.extraJavaOptions",   _opens)
        conf.set("spark.executor.extraJavaOptions", _opens)

        # ── Gluten + Velox opt-in (default: disabled) ─────────────────────────
        # Set ENABLE_GLUTEN=1 to activate the Gluten columnar engine.
        # When enabled:
        #   - spark.plugins loads GlutenPlugin (Velox backend)
        #   - off-heap is enabled (2g per executor — requires 8 GB worker pods)
        # When disabled (default):
        #   - spark.plugins is cleared so no ClassNotFoundException at startup
        #   - off-heap is not reserved (frees ~2 GB per executor for heap use)
        _gluten_enabled = os.environ.get("ENABLE_GLUTEN", "0") == "1"
        if _gluten_enabled:
            logger.info("Gluten/Velox enabled (ENABLE_GLUTEN=1).")
            conf.set("spark.plugins", "io.glutenproject.GlutenPlugin")
            conf.set("spark.gluten.sql.columnar.backend.lib", "velox")
            conf.set("spark.memory.offHeap.enabled", "true")
            conf.set("spark.memory.offHeap.size",    "2g")
        else:
            conf.set("spark.plugins", "")
            conf.set("spark.memory.offHeap.enabled", "false")

        # ── Iceberg extension ──────────────────────────────────────────────────
        conf.set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )

        # ── Polaris REST catalog ───────────────────────────────────────────────
        conf.set("spark.sql.catalog.polaris",
                 "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.polaris.type", "rest")
        conf.set("spark.sql.catalog.polaris.uri", _POLARIS_URI)
        conf.set("spark.sql.catalog.polaris.credential",
                 f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
        conf.set("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        conf.set("spark.sql.catalog.polaris.warehouse", "IcebergCatalog")
        # Explicit OAuth2 config — suppresses "inferring rest.auth.type" warning
        # and "missing OAuth2 server URI" fallback warning from Iceberg REST client.
        conf.set("spark.sql.catalog.polaris.rest.auth.type", "oauth2")
        conf.set("spark.sql.catalog.polaris.oauth2-server-uri",
                 f"{_POLARIS_URI}/v1/oauth/tokens")

        # ── Snowflake internal catalog (for reading source tables) ─────────────
        conf.set("spark.sql.catalog.snowflake_sample",
                 "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.snowflake_sample.type", "hadoop")
        # The Snowflake Spark connector is used for actual reads; the catalog
        # entry here declares the namespace so the copy app can validate it.
        conf.set("spark.sql.catalog.snowflake_sample.warehouse",
                 "SNOWFLAKE_SAMPLE_DATA")

        # ── S3 / AWS credentials ───────────────────────────────────────────────
        conf.set("spark.hadoop.fs.s3a.access.key",  s3["access_key"])
        conf.set("spark.hadoop.fs.s3a.secret.key",  s3["secret_key"])
        conf.set("spark.hadoop.fs.s3a.endpoint",    s3["endpoint"])
        conf.set("spark.hadoop.fs.s3a.endpoint.region", s3["region"])
        conf.set("spark.hadoop.fs.s3a.impl",
                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        # Alias bare s3:// to the s3a implementation.
        # Polaris validates table LOCATION as s3:// (not s3a://), but Hadoop
        # needs an explicit FileSystem binding for that scheme.
        conf.set("spark.hadoop.fs.s3.impl",
                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set("spark.hadoop.fs.s3.access.key",  s3["access_key"])
        conf.set("spark.hadoop.fs.s3.secret.key",  s3["secret_key"])
        conf.set("spark.hadoop.fs.s3.endpoint",    s3["endpoint"])
        conf.set("spark.hadoop.fs.s3.endpoint.region", s3["region"])
        conf.set("spark.hadoop.fs.s3.path.style.access", "true")
        conf.set("spark.hadoop.fs.s3.connection.ssl.enabled", "true")

        # ── Iceberg AWS SDK v2 credentials (S3FileIO used by iceberg-aws-bundle)
        # spark.hadoop.fs.s3a.* feeds Hadoop S3A only — the Iceberg writer uses
        # its own AWS SDK v2 DefaultCredentialsProvider chain on each executor.
        # These catalog-scoped properties inject the credentials directly into
        # the Iceberg S3FileIO so no cloud IMDS / profile lookup is attempted.
        conf.set("spark.sql.catalog.polaris.s3.access-key-id",     s3["access_key"])
        conf.set("spark.sql.catalog.polaris.s3.secret-access-key", s3["secret_key"])
        conf.set("spark.sql.catalog.polaris.s3.endpoint",          s3["endpoint"])
        conf.set("spark.sql.catalog.polaris.s3.path-style-access", "true")
        conf.set("spark.sql.catalog.polaris.client.region",        s3["region"])

        # ── Parquet defaults ───────────────────────────────────────────────────
        conf.set("spark.sql.parquet.compression.codec", "snappy")

        # ── Serializer (Kryo avoids NotSerializableException: StorageStatus bug)
        # Spark 3.5 JavaSerializer incorrectly tries to serialize StorageStatus
        # in RPC responses, causing ERROR Inbox warnings. Kryo suppresses this.
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.registrationRequired", "false")

        # ── JARs (baked into image — list for explicitness) ───────────────────
        conf.set("spark.jars", ",".join([
            _ICEBERG_JAR_PATH,
            "/opt/spark/jars/iceberg-aws-bundle-1.9.2.jar",
            _SNOWFLAKE_JAR_PATH,
            _SNOWFLAKE_JDBC_JAR,
            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
        ]))

        if extra_conf:
            for k, v in extra_conf.items():
                conf.set(k, v)

        return conf

    # ── Snowflake JDBC / connector options ─────────────────────────────────────
    def snowflake_options(
        self,
        schema:   str = "TPCDS_SF10TCL",
        database: str = "SNOWFLAKE_SAMPLE_DATA",
    ) -> dict[str, str]:
        """
        Return a dict of options for spark.read.format("snowflake").
        Requires net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5 on classpath.

        Args:
            schema:   Snowflake schema name (e.g. TPCDS_SF10TCL)
            database: Snowflake database name (default: SNOWFLAKE_SAMPLE_DATA)
        """
        sf = self.snowflake_creds()
        return {
            "sfURL":       f"{sf['account']}.snowflakecomputing.com",
            "sfUser":      sf["user"],
            "sfPassword":  sf["password"],
            "sfDatabase":  database,
            "sfSchema":    schema,
            "sfWarehouse": sf.get("warehouse", "COMPUTE_WH"),
        }
