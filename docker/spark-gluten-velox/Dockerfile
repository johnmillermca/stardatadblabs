# =============================================================================
# Spark 3.5.1 + Gluten 1.2.0 (Velox backend, CentOS/RHEL build)
#
# Bundles:
#   gluten-velox-bundle-spark3.5_2.12-centos_7_x86_64-1.2.0.jar  (Gluten+Velox native)
#   iceberg-spark-runtime-3.5_2.12-1.9.2.jar                     (Iceberg)
#   iceberg-aws-bundle-1.9.2.jar                                  (Iceberg S3FileIO — AWS SDK v2)
#   spark-snowflake_2.12-3.2.1-spark_3.5.jar                     (Snowflake connector)
#   snowflake-jdbc-4.0.2.jar                                      (Snowflake JDBC)
#   databricks-jdbc-2.6.36.1070.jar                               (Databricks JDBC — binary 3.4.2)
#   hadoop-aws-3.3.4.jar                                          (S3A filesystem)
#   aws-java-sdk-bundle-1.12.262.jar                              (AWS SDK v1 — S3A)
#   postgresql-42.7.4.jar                                         (Hive Metastore backend — PostgreSQL JDBC)
#
# Python packages:
#   pyspark==3.5.1, snowflake-connector-python, confluent-kafka,
#   pyarrow, pandas, boto3, pyiceberg, psycopg2-binary
#
# Place all JARs in jars/ next to this Dockerfile before building.
# Build:
#   bash docker/spark-gluten-velox/build-and-push.sh
# =============================================================================
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

# ── Python 3.11 — match the JupyterHub driver (Python 3.10 is the base image default)
# PYSPARK_PYTHON must be identical on driver and every executor or Spark aborts
# with: [PYTHON_VERSION_MISMATCH] Python in worker has different version (3,10)
# than that in driver 3.11
RUN apt-get update && apt-get install -y --no-install-recommends \
        software-properties-common \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y --no-install-recommends \
        python3.11 python3.11-distutils python3.11-venv \
    && rm -rf /var/lib/apt/lists/*

# Make python3.11 the default python3 for Spark workers
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 2 \
    && update-alternatives --set python3 /usr/bin/python3.11 \
    && curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11

# Tell Spark executors to use Python 3.11 — must match PYSPARK_DRIVER_PYTHON on driver
ENV PYSPARK_PYTHON=python3.11

# ── System libs required by Velox native engine ───────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgflags-dev \
        libgoogle-glog-dev \
        libunwind-dev \
        libre2-dev \
        libfmt-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY jars/gluten-velox-bundle-spark3.5_2.12-centos_7_x86_64-1.2.0.jar \
     /opt/spark/jars/gluten-velox-bundle-spark3.5_2.12-centos_7_x86_64-1.2.0.jar

COPY jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
     /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar

COPY jars/spark-snowflake_2.12-3.2.1-spark_3.5.jar \
     /opt/spark/jars/spark-snowflake_2.12-3.2.1-spark_3.5.jar

# spark-snowflake 3.2.1 requires JDBC 4.x (internal API package restructure in 4.0+)
COPY jars/snowflake-jdbc-4.0.2.jar \
     /opt/spark/jars/snowflake-jdbc-4.0.2.jar

COPY jars/hadoop-aws-3.3.4.jar \
     /opt/spark/jars/hadoop-aws-3.3.4.jar

COPY jars/aws-java-sdk-bundle-1.12.262.jar \
     /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# iceberg-aws-bundle provides AWS SDK v2 classes (S3FileIO, S3Exception, etc.)
# required by iceberg-spark-runtime 1.9.2 — absent from aws-java-sdk-bundle (v1).
COPY jars/iceberg-aws-bundle-1.9.2.jar \
     /opt/spark/jars/iceberg-aws-bundle-1.9.2.jar

# PostgreSQL JDBC driver — required by Hive Metastore to connect to the
# hive_metastore database on the cluster's PostgreSQL 17 instance.
COPY jars/postgresql-42.7.4.jar \
     /opt/spark/jars/postgresql-42.7.4.jar

# Databricks JDBC driver — required by starpump databricks connector.
# Staged from: ~/Downloads/DatabricksJDBC-3.4.2/databricks-jdbc-3.4.2.jar
# Stored in jars/ as databricks-jdbc-2.6.36.1070.jar (binary is 3.4.2;
# driver class com.databricks.client.jdbc.Driver is stable across versions).
COPY jars/databricks-jdbc-2.6.36.1070.jar \
     /opt/spark/jars/databricks-jdbc-2.6.36.1070.jar
RUN chmod 644 /opt/spark/jars/databricks-jdbc-2.6.36.1070.jar

# ── Python packages — split into layers to avoid pip botocore conflict ────────
# Layer 1: boto3 + s3fs together so pip resolves a compatible botocore/aiobotocore
RUN pip3 install --no-cache-dir \
        "boto3" \
        "s3fs" \
        "fsspec"

# Layer 2: remaining packages — all pinned, no botocore conflict
RUN pip3 install --no-cache-dir \
        "pyspark==3.5.1" \
        "snowflake-connector-python==3.12.3" \
        "confluent-kafka==2.6.1" \
        "pyarrow==17.0.0" \
        "pandas==2.2.3" \
        "pyiceberg==0.7.1" \
        "psycopg2-binary==2.9.10" \
        "thrift==0.24.0"

# ── Set HOME for Snowflake JDBC credential cache + log directory ──────────────
# Done early (before COPY conf/) so /home/spark exists and is owned by spark
# when the RollingFile appender creates spark-warnings.log at first write.
# Snowflake JDBC 4.x also needs a writable $HOME for its credential cache
# (SF_TEMPORARY_CREDENTIAL_CACHE_DIR); without it every query prints
# "WARN NoOpFileCacheManager: Cache is not available".
ENV HOME=/home/spark
RUN mkdir -p /home/spark/logs \
    && chown -R spark:spark /home/spark

# ── Logging — suppress harmless transport / OAuth2 / NativeLoader noise ──────
# log4j2.properties:
#   Console  : WARN+ only (StacklessClosedChannelException etc. never printed)
#   Log file : /home/spark/logs/spark-warnings.log — all levels, 500 MB cap,
#              single-file overwrite on rollover (disk never exceeds 500 MB).
#
# Why spark-env.sh is required:
#   spark.driver.extraJavaOptions in spark-defaults.conf is processed by the
#   Spark launcher *after* the launcher mini-JVM has already initialised log4j
#   with its default config — so early messages (NativeCodeLoader, OAuth2Manager,
#   TransportRequestHandler) print before the filter ever takes effect.
#   SPARK_JAVA_OPTS in spark-env.sh is sourced by load-spark-env.sh inside
#   spark-class *before* any JVM is launched, so the -Dlog4j flag applies to
#   the launcher JVM, the driver JVM, and all executor JVMs from the first line.
RUN mkdir -p "${SPARK_HOME}/conf"
COPY conf/log4j2.properties ${SPARK_HOME}/conf/log4j2.properties
RUN echo 'export SPARK_JAVA_OPTS="-Dlog4j.configurationFile=/opt/spark/conf/log4j2.properties"' \
    > "${SPARK_HOME}/conf/spark-env.sh"

# ── Pipeline scripts — baked in so work-dir is always populated ──────────────
# Scripts are maintained in docs/runbooks/snowflake-to-iceberg/ and mirrored
# into docker/spark-gluten-velox/scripts/ so they are part of the build context.
# Update scripts/ whenever the source files change, then rebuild the image.
RUN mkdir -p /opt/spark/work-dir
COPY scripts/bao_spark_init.py           /opt/spark/work-dir/bao_spark_init.py
COPY scripts/spark_iceberg_utils.py      /opt/spark/work-dir/spark_iceberg_utils.py
COPY scripts/starpump.py                 /opt/spark/work-dir/starpump.py
COPY scripts/databricks_customer_seed.py /opt/spark/work-dir/databricks_customer_seed.py
COPY scripts/databricks_product_seed.py  /opt/spark/work-dir/databricks_product_seed.py
RUN chmod 644 /opt/spark/work-dir/*.py

# ── CLI entry-points ──────────────────────────────────────────────────────────
# starpump: thin wrapper so the pipeline runs without an explicit path.
#   kubectl exec ... -- env KEY=VAL ... starpump snowflake
# NOTE: the centos_7 Velox build prints a harmless JVM execstack warning once
# per pod lifetime (on first Velox load). The JVM self-corrects immediately.
# It cannot be suppressed without risking broken job execution — accept it.
RUN printf '#!/bin/sh\nexec python3 /opt/spark/work-dir/starpump.py "$@"\n' \
    > /usr/local/bin/starpump \
    && chmod 755 /usr/local/bin/starpump \
    && ln -sf /usr/local/bin/starpump /usr/local/bin/snowflake-to-iceberg

# spark-app-cleanup: kills stale/orphaned apps via the Spark master REST API.
#   kubectl exec -n prod $MASTER -c spark-master -- spark-app-cleanup
COPY scripts/spark-app-cleanup.sh /usr/local/bin/spark-app-cleanup
RUN chmod 755 /usr/local/bin/spark-app-cleanup

# ── spark-defaults.conf ───────────────────────────────────────────────────────
# Written to ${SPARK_HOME}/conf/spark-defaults.conf (empty in base image).
# Credentials (S3 keys, Polaris secret, Snowflake password) are NOT here —
# they are injected at runtime by bao_spark_init.py from OpenBao.
RUN mkdir -p "${SPARK_HOME}/conf" && cat > "${SPARK_HOME}/conf/spark-defaults.conf" << 'EOF'
# ── Gluten + Velox native execution (DEFAULT ON) ─────────────────────────────
# Gluten is ALWAYS ACTIVE on this cluster — bao_spark_init.py loads
# GlutenPlugin and enables off-heap (2g) for every job by default.
# To disable for one run: DISABLE_GLUTEN=1 starpump snowflake
# NOTE: JAR is org.apache.gluten (donated to Apache; old io.glutenproject is gone).
spark.plugins                                        org.apache.gluten.GlutenPlugin
spark.gluten.sql.columnar.backend.lib                velox
spark.memory.offHeap.enabled                         true
spark.memory.offHeap.size                            2g

# ── SQL extensions (Iceberg only — Delta added per-session if needed) ─────────
spark.sql.extensions                                 org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# ── Polaris REST catalog (credentials injected at runtime by bao_spark_init) ──
spark.sql.catalog.polaris                            org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.polaris.type                       rest
spark.sql.catalog.polaris.uri                        http://polaris-rest.prod.svc.cluster.local:8181/api/catalog
spark.sql.catalog.polaris.warehouse                  IcebergCatalog
spark.sql.catalog.polaris.scope                      PRINCIPAL_ROLE:ALL

# ── Snowflake source catalog (namespace = tpcds_sf10tcl) ─────────────────────
spark.sql.catalog.snowflake                   org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.snowflake.type              hadoop
spark.sql.catalog.snowflake.warehouse         SNOWFLAKE_SAMPLE_DATA
spark.sql.catalog.snowflake.default-namespace tpcds_sf10tcl

# ── Databricks catalog → Polaris REST (warehouse: star_lakehouse) ─────────────
# Credentials (credential, oauth2-server-uri, s3 keys) injected at runtime
# by bao_spark_init.spark_conf().  This stanza declares the catalog so pods
# have it available without a per-job SparkSession.builder call.
spark.sql.catalog.databricks                            org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.databricks.type                       rest
spark.sql.catalog.databricks.uri                        http://polaris-rest.prod.svc.cluster.local:8181/api/catalog
spark.sql.catalog.databricks.warehouse                  star_lakehouse
spark.sql.catalog.databricks.scope                      PRINCIPAL_ROLE:ALL
spark.sql.catalog.databricks.rest.auth.type             oauth2

# ── Global Iceberg write defaults (256 MB file size) ─────────────────────────
spark.sql.iceberg.write.format.default               parquet
spark.sql.iceberg.target-file-size-bytes             268435456

# ── Iceberg query optimisations ───────────────────────────────────────────────
# COUNT(*) / COUNT(1) answered from snapshot manifest metadata — no S3 data scan.
# Enabled on workers too so the optimisation applies whether the query runs from
# a notebook driver or from a spark-submit job on the cluster.
spark.sql.iceberg.aggregate-pushdown.enabled         true

# ── S3A defaults (credentials injected at runtime) ───────────────────────────
spark.hadoop.fs.s3a.impl                             org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint                         https://s3.us-east-2.amazonaws.com
spark.hadoop.fs.s3a.endpoint.region                  us-east-2
spark.hadoop.fs.s3a.path.style.access                true
spark.hadoop.fs.s3a.connection.ssl.enabled           true

# ── Parquet / compression ─────────────────────────────────────────────────────
spark.sql.parquet.compression.codec                  snappy

# ── Serializer ───────────────────────────────────────────────────────────────
spark.serializer                                     org.apache.spark.serializer.KryoSerializer

# ── Driver / executor sizing ──────────────────────────────────────────────────
# Worker pods: 8 GB RAM limit, SPARK_WORKER_MEMORY=7g.
# 3g executor + 300 MB overhead = 3.3 GB → 2 executors per worker (safe).
# 2 executors × 2 cores = 4 tasks/worker; 3 workers = 12 total parallel tasks.
spark.driver.memory                                  4g
spark.executor.memory                                3g
spark.executor.cores                                 2

# ── Logging config — point Spark at the baked-in log4j2.properties ───────────
# Uses the resolved path /opt/spark (not ${SPARK_HOME}) because this heredoc
# uses a quoted delimiter ('EOF') which suppresses shell variable expansion.
# bao_spark_init.py APPENDS this flag to its --add-opens string; it must not
# set extraJavaOptions independently or it will overwrite this value.
spark.driver.extraJavaOptions   -Dlog4j.configurationFile=/opt/spark/conf/log4j2.properties
spark.executor.extraJavaOptions -Dlog4j.configurationFile=/opt/spark/conf/log4j2.properties

# ── Orphan / stalled-app protection ──────────────────────────────────────────
# worker.timeout: seconds before the master marks a silent worker DEAD and
#   removes its apps.  OOM-killed drivers disconnect immediately; this catches
#   hung or network-partitioned workers within 60 s.
spark.worker.timeout                                 60
# dead.worker.persistence: UI history entries for dead workers (0 = no history).
spark.dead.worker.persistence                        0
# network.timeout: global RPC + heartbeat deadline — 60 s (1 minute).
#   A driver/executor that stops responding is evicted after this window.
spark.network.timeout                                60s
# Worker-side app working-directory cleanup (removes shuffle/tmp files).
spark.worker.cleanup.enabled                         true
spark.worker.cleanup.interval                        60
spark.worker.cleanup.appDataTtl                      60
EOF

USER spark
