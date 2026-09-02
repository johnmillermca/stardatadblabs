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
2. Root / bootstrap token   (env-var TOKEN – dev/local override only)

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
_PATH_S3             = "secret/data/platform/s3"
_PATH_SNOWFLAKE      = "secret/data/platform/snowflake"
_PATH_POLARIS        = "secret/data/platform/polaris"
_PATH_DORIS          = "secret/data/platform/doris"
_PATH_ORACLE         = "secret/data/platform/oracle"
_PATH_KAFKA          = "secret/data/platform/kafka"
_PATH_PIPELINE_DB    = "secret/data/platform/pipeline_db"
_PATH_DATABRICKS     = "secret/data/platform/databricks"
_PATH_POSTGRES       = "secret/data/platform/postgres"
_PATH_MONGODB        = "secret/data/platform/mongodb"
# JARs baked into the spark-gluten-velox image
_ICEBERG_JAR_NAME    = "iceberg-spark-runtime-3.5_2.12-1.9.2.jar"
_ICEBERG_JAR_PATH    = f"/opt/spark/jars/{_ICEBERG_JAR_NAME}"
_SNOWFLAKE_JAR_NAME  = "spark-snowflake_2.12-3.2.1-spark_3.5.jar"
_SNOWFLAKE_JAR_PATH  = f"/opt/spark/jars/{_SNOWFLAKE_JAR_NAME}"
# spark-snowflake 3.2.1 requires JDBC 4.x (internal API package restructure)
_SNOWFLAKE_JDBC_JAR  = "/opt/spark/jars/snowflake-jdbc-4.0.2.jar"
# Databricks JDBC driver (Simba) — baked into image
_DATABRICKS_JDBC_JAR = "/opt/spark/jars/databricks-jdbc-2.6.36.1070.jar"
# Oracle JDBC thin driver (ojdbc11) — baked into image
_ORACLE_JDBC_JAR     = "/opt/spark/jars/ojdbc11-23.4.0.24.05.jar"
# MongoDB Spark connector uber-jar — baked into image
_MONGODB_CONNECTOR_JAR = "/opt/spark/jars/mongo-spark-connector_2.12-10.4.0-all.jar"

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
        # Accept TOKEN/ADDR (starpump convention) or legacy BAO_TOKEN/BAO_ADDR.
        self._address = (
            bao_address
            or os.environ.get("ADDR")
            or os.environ.get("BAO_ADDR", _BAO_IN_CLUSTER)
        )
        self._role = bao_role
        self._k8s_auth_path = k8s_auth_path
        self._token: str | None = None
        self._cache: dict[str, dict] = {}

    # ── Authentication ─────────────────────────────────────────────────────────
    def _get_token(self) -> str:
        if self._token:
            return self._token

        # 1. Explicit env override (dev/bootstrap only)
        # Accept TOKEN (starpump convention) or legacy BAO_TOKEN.
        if env_tok := (os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN")):
            logger.info("Using TOKEN from environment (dev mode).")
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
            "Cannot authenticate to OpenBao: no TOKEN env-var and "
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

    def databricks_creds(self) -> dict[str, str]:
        """
        Return Databricks JDBC credentials from OpenBao.
        Expected keys in secret/data/platform/databricks:
          host          e.g. dbc-11a1dbc5-061a.cloud.databricks.com
          http_path     e.g. /sql/1.0/warehouses/942026cf5e55f3c3
          token         Personal Access Token or OAuth M2M token
          catalog       e.g. lakehouse   (Unity Catalog catalog name)
          schema        e.g. lakehouse_db
        """
        return self._read_secret(_PATH_DATABRICKS)

    def postgres_creds(self) -> dict[str, str]:
        """
        Return PostgreSQL credentials from OpenBao.
        Expected keys in secret/data/platform/postgres:
          host        e.g. postgresql.prod.svc.cluster.local
          port        e.g. 5432
          database    e.g. cach_testing
          user        e.g. spark_user
          password
          schema      e.g. public  (default schema to read from)
        """
        return self._read_secret(_PATH_POSTGRES)

    def postgres_jdbc_options(
        self,
        database: str | None = None,
        schema:   str | None = None,
    ) -> dict[str, str]:
        """
        Return JDBC options for spark.read.format("jdbc") against PostgreSQL.
        Uses the postgresql-42.7.4.jar already baked into the image.
        """
        pg = self.postgres_creds()
        _db     = database or pg["database"]
        _schema = schema   or pg.get("schema", "public")
        jdbc_url = (
            f"jdbc:postgresql://{pg['host']}:{pg.get('port','5432')}/{_db}"
            f"?currentSchema={_schema}&ApplicationName=starpump"
        )
        return {
            "url":      jdbc_url,
            "driver":   "org.postgresql.Driver",
            "user":     pg["user"],
            "password": pg["password"],
        }

    def mongodb_creds(self) -> dict[str, str]:
        """
        Return MongoDB credentials from OpenBao.
        Expected keys in secret/data/platform/mongodb:
          host        e.g. mongodb.prod.svc.cluster.local
          port        e.g. 27017
          database    e.g. cach_testing
          user        e.g. spark_user
          password
          auth_source e.g. admin  (authentication database, default: admin)
        """
        return self._read_secret(_PATH_MONGODB)

    def mongodb_options(
        self,
        database: str | None = None,
    ) -> dict[str, str]:
        """
        Return options for spark.read.format("mongodb") using the
        MongoDB Spark connector 10.x (mongo-spark-connector_2.12-10.4.0).
        """
        mg = self.mongodb_creds()
        _db        = database or mg["database"]
        auth_src   = mg.get("auth_source", "admin")
        uri = (
            f"mongodb://{mg['user']}:{mg['password']}@"
            f"{mg['host']}:{mg.get('port','27017')}/{_db}"
            f"?authSource={auth_src}"
        )
        return {
            "spark.mongodb.read.connection.uri": uri,
            "database": _db,
        }

    def oracle_jdbc_options(
        self,
        schema: str | None = None,
    ) -> dict[str, str]:
        """
        Return JDBC options for spark.read.format("jdbc") against Oracle.
        Uses the ojdbc11-23.4.0.24.05.jar baked into the image.
        oracle_creds() already exists and returns:
          user, password, host, port, sid, jdbc_url
        """
        ora = self.oracle_creds()
        # Use the pre-built jdbc_url from OpenBao if present; otherwise construct.
        jdbc_url = ora.get("jdbc_url") or (
            f"jdbc:oracle:thin:@{ora['host']}:{ora.get('port','1521')}:{ora['sid']}"
        )
        _schema = schema or ora.get("schema", ora["user"].upper())
        return {
            "url":             jdbc_url,
            "driver":          "oracle.jdbc.OracleDriver",
            "user":            ora["user"],
            "password":        ora["password"],
            "oracle.jdbc.mapDateToTimestamp": "false",
            # currentSchema sets the default schema for unqualified object refs
            "sessionInitStatement": f"ALTER SESSION SET CURRENT_SCHEMA = {_schema}",
        }

    def databricks_jdbc_options(
        self,
        catalog: str | None = None,
        schema:  str | None = None,
    ) -> dict[str, str]:
        """
        Return a dict of JDBC options for
        spark.read.format("jdbc").options(**opts).

        Uses the Databricks Simba JDBC driver baked into the image at
        /opt/spark/jars/databricks-jdbc-2.6.36.1070.jar.

        Args:
            catalog: Unity Catalog catalog name (overrides secret default).
            schema:  Schema / database name    (overrides secret default).
        """
        db = self.databricks_creds()
        host      = db["host"]
        http_path = db["http_path"]
        token     = db["token"]
        _catalog  = catalog or db.get("catalog", "lakehouse")
        _schema   = schema  or db.get("schema",  "lakehouse_db")

        jdbc_url = (
            f"jdbc:databricks://{host}:443"
            f";httpPath={http_path}"
            f";AuthMech=3"
            f";UID=token"
            f";PWD={token}"
            f";ConnCatalog={_catalog}"
            f";ConnSchema={_schema}"
            f";SSL=1"
        )
        return {
            "url":    jdbc_url,
            "driver": "com.databricks.client.jdbc.Driver",
        }

    def catalog_credential(self, catalog: str, conf: "SparkConf") -> str:
        """
        Return the Polaris OAuth2 service-account ID wired for *catalog* in *conf*.

        The credential is the ``spark_svc_id`` portion of the
        ``spark.sql.catalog.<catalog>.credential`` property set by
        :meth:`spark_conf`.  If the property is absent the catalog was never
        registered as a Spark external catalog, and starpump must not proceed.

        Raises:
            ValueError: if ``spark.sql.catalog.<catalog>`` has no credential
                        configured in *conf* (catalog not registered).
        """
        key = f"spark.sql.catalog.{catalog}.credential"
        credential = conf.get(key)
        if not credential:
            registered = sorted(
                name.split(".")[3]
                for name, _ in conf.getAll()
                if name.startswith("spark.sql.catalog.")
                and len(name.split(".")) == 4
            )
            raise ValueError(
                f"No Spark external catalog registered for '{catalog}'. "
                f"starpump cannot write to a catalog that has no wired credential. "
                f"Registered catalogs in spark_conf: {registered}. "
                f"Add a 'spark.sql.catalog.{catalog}.*' block to BaoSparkInit.spark_conf() "
                f"before running starpump against this target."
            )
        # Return only the client-id half (everything before the first colon).
        return credential.split(":")[0]

    # ── SparkConf builder ──────────────────────────────────────────────────────
    def spark_conf(
        self,
        app_name: str = "iceberg-job",
        extra_conf: dict[str, str] | None = None,
    ) -> SparkConf:
        """
        Build a SparkConf pre-wired with:
          - Polaris REST catalog  (catalog name: polaris)
          - Databricks catalog    (catalog name: databricks)
          - Snowflake catalog     (catalog name: snowflake, hadoop-type placeholder)
          - S3 credentials (AWS SDK v2 style)
          - Parquet + Iceberg defaults

        Only catalogs wired here may be used as starpump copy targets.
        starpump validates this via :meth:`catalog_credential` before opening
        a Spark session.
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

        # ── Gluten + Velox (DEFAULT: always enabled) ──────────────────────────
        # Gluten/Velox columnar native execution is ON by default for every
        # Spark job on this cluster.  Set DISABLE_GLUTEN=1 to turn it off for
        # a specific run (e.g. debugging, compatibility testing).
        #
        # When enabled (default):
        #   - spark.plugins loads GlutenPlugin (Velox backend)
        #   - off-heap memory: 2g per executor (workers are 8 GB; safe headroom)
        # When disabled (DISABLE_GLUTEN=1):
        #   - spark.plugins is cleared — no ClassNotFoundException at startup
        #   - off-heap is not reserved (frees ~2 GB per executor for heap use)
        _gluten_disabled = os.environ.get("DISABLE_GLUTEN", "0") == "1"
        if not _gluten_disabled:
            logger.info("Gluten/Velox enabled (default — set DISABLE_GLUTEN=1 to turn off).")
            conf.set("spark.plugins",                         "org.apache.gluten.GlutenPlugin")
            conf.set("spark.gluten.sql.columnar.backend.lib", "velox")
            conf.set("spark.memory.offHeap.enabled",          "true")
            conf.set("spark.memory.offHeap.size",             "2g")
        else:
            logger.info("Gluten/Velox DISABLED (DISABLE_GLUTEN=1).")
            conf.set("spark.plugins",                "")
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
        conf.set("spark.sql.catalog.snowflake",
                 "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.snowflake.type", "hadoop")
        # The Snowflake Spark connector is used for actual reads; the catalog
        # entry here declares the namespace so the copy app can validate it.
        conf.set("spark.sql.catalog.snowflake.warehouse",
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

        # ── Databricks catalog → Polaris REST (warehouse: star_lakehouse) ────
        # Registered here so every starpump run with SOURCE=databricks has the
        # catalog available without per-script setup.
        conf.set("spark.sql.catalog.databricks",
                 "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.databricks.type",             "rest")
        conf.set("spark.sql.catalog.databricks.uri",              _POLARIS_URI)
        conf.set("spark.sql.catalog.databricks.oauth2-server-uri",
                 f"{_POLARIS_URI}/v1/oauth/tokens")
        conf.set("spark.sql.catalog.databricks.credential",
                 f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
        conf.set("spark.sql.catalog.databricks.scope",            "PRINCIPAL_ROLE:ALL")
        conf.set("spark.sql.catalog.databricks.warehouse",        "star_lakehouse")
        conf.set("spark.sql.catalog.databricks.rest.auth.type",   "oauth2")
        # Iceberg S3FileIO for databricks catalog
        s3 = self.s3_creds()
        conf.set("spark.sql.catalog.databricks.s3.access-key-id",     s3["access_key"])
        conf.set("spark.sql.catalog.databricks.s3.secret-access-key", s3["secret_key"])
        conf.set("spark.sql.catalog.databricks.s3.endpoint",          s3["endpoint"])
        conf.set("spark.sql.catalog.databricks.s3.path-style-access", "true")
        conf.set("spark.sql.catalog.databricks.client.region",        s3["region"])

        # ── PostgreSQL catalog → Polaris REST (warehouse: pg_lakehouse) ──────
        pg = self.postgres_creds()
        conf.set("spark.sql.catalog.postgres",
                 "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.postgres.type",             "rest")
        conf.set("spark.sql.catalog.postgres.uri",              _POLARIS_URI)
        conf.set("spark.sql.catalog.postgres.oauth2-server-uri",
                 f"{_POLARIS_URI}/v1/oauth/tokens")
        conf.set("spark.sql.catalog.postgres.credential",
                 f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
        conf.set("spark.sql.catalog.postgres.scope",            "PRINCIPAL_ROLE:ALL")
        conf.set("spark.sql.catalog.postgres.warehouse",        "pg_lakehouse")
        conf.set("spark.sql.catalog.postgres.rest.auth.type",   "oauth2")
        conf.set("spark.sql.catalog.postgres.s3.access-key-id",     s3["access_key"])
        conf.set("spark.sql.catalog.postgres.s3.secret-access-key", s3["secret_key"])
        conf.set("spark.sql.catalog.postgres.s3.endpoint",          s3["endpoint"])
        conf.set("spark.sql.catalog.postgres.s3.path-style-access", "true")
        conf.set("spark.sql.catalog.postgres.client.region",        s3["region"])

        # ── Oracle catalog → Polaris REST (warehouse: ora_lakehouse) ─────────
        conf.set("spark.sql.catalog.oracle",
                 "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.oracle.type",             "rest")
        conf.set("spark.sql.catalog.oracle.uri",              _POLARIS_URI)
        conf.set("spark.sql.catalog.oracle.oauth2-server-uri",
                 f"{_POLARIS_URI}/v1/oauth/tokens")
        conf.set("spark.sql.catalog.oracle.credential",
                 f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
        conf.set("spark.sql.catalog.oracle.scope",            "PRINCIPAL_ROLE:ALL")
        conf.set("spark.sql.catalog.oracle.warehouse",        "ora_lakehouse")
        conf.set("spark.sql.catalog.oracle.rest.auth.type",   "oauth2")
        conf.set("spark.sql.catalog.oracle.s3.access-key-id",     s3["access_key"])
        conf.set("spark.sql.catalog.oracle.s3.secret-access-key", s3["secret_key"])
        conf.set("spark.sql.catalog.oracle.s3.endpoint",          s3["endpoint"])
        conf.set("spark.sql.catalog.oracle.s3.path-style-access", "true")
        conf.set("spark.sql.catalog.oracle.client.region",        s3["region"])

        # ── MongoDB catalog → Polaris REST (warehouse: mgo_lakehouse) ────────
        conf.set("spark.sql.catalog.mongodb",
                 "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.mongodb.type",             "rest")
        conf.set("spark.sql.catalog.mongodb.uri",              _POLARIS_URI)
        conf.set("spark.sql.catalog.mongodb.oauth2-server-uri",
                 f"{_POLARIS_URI}/v1/oauth/tokens")
        conf.set("spark.sql.catalog.mongodb.credential",
                 f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
        conf.set("spark.sql.catalog.mongodb.scope",            "PRINCIPAL_ROLE:ALL")
        conf.set("spark.sql.catalog.mongodb.warehouse",        "mgo_lakehouse")
        conf.set("spark.sql.catalog.mongodb.rest.auth.type",   "oauth2")
        conf.set("spark.sql.catalog.mongodb.s3.access-key-id",     s3["access_key"])
        conf.set("spark.sql.catalog.mongodb.s3.secret-access-key", s3["secret_key"])
        conf.set("spark.sql.catalog.mongodb.s3.endpoint",          s3["endpoint"])
        conf.set("spark.sql.catalog.mongodb.s3.path-style-access", "true")
        conf.set("spark.sql.catalog.mongodb.client.region",        s3["region"])

        # ── JARs (baked into image — list for explicitness) ───────────────────
        conf.set("spark.jars", ",".join([
            _ICEBERG_JAR_PATH,
            "/opt/spark/jars/iceberg-aws-bundle-1.9.2.jar",
            _SNOWFLAKE_JAR_PATH,
            _SNOWFLAKE_JDBC_JAR,
            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
            "/opt/spark/jars/postgresql-42.7.4.jar",
            _DATABRICKS_JDBC_JAR,
            _ORACLE_JDBC_JAR,
            _MONGODB_CONNECTOR_JAR,
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
