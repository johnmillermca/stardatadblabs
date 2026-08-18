# =============================================================================
# Spark 3.5.1 + Gluten 1.2.0 (Velox backend, CentOS/RHEL build)
#
# Bundles:
#   gluten-velox-bundle-spark3.5_2.12-centos_7_x86_64-1.2.0.jar  (Gluten+Velox native)
#   iceberg-spark-runtime-3.5_2.12-1.9.2.jar                     (Iceberg)
#   spark-snowflake_2.12-3.2.1-spark_3.5.jar                     (Snowflake connector)
#   snowflake-jdbc-3.16.1.jar                                     (Snowflake JDBC)
#   hadoop-aws-3.3.4.jar                                          (S3A filesystem)
#   aws-java-sdk-bundle-1.12.262.jar                              (AWS SDK)
#
# Python packages:
#   pyspark==3.5.1, snowflake-connector-python, confluent-kafka,
#   pyarrow, pandas, boto3, pyiceberg
#
# Place all JARs in jars/ next to this Dockerfile before building.
# Build:
#   bash docker/spark-gluten-velox/build-and-push.sh
# =============================================================================
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

# ── System libs required by Velox native engine ───────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgflags-dev \
        libgoogle-glog-dev \
        libunwind-dev \
        libre2-dev \
        libfmt-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ── Copy all JARs from build context ─────────────────────────────────────────
COPY jars/gluten-velox-bundle-spark3.5_2.12-centos_7_x86_64-1.2.0.jar \
     /opt/spark/jars/gluten-velox-bundle-spark3.5_2.12-centos_7_x86_64-1.2.0.jar

COPY jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
     /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar

COPY jars/spark-snowflake_2.12-3.2.1-spark_3.5.jar \
     /opt/spark/jars/spark-snowflake_2.12-3.2.1-spark_3.5.jar

COPY jars/snowflake-jdbc-3.16.1.jar \
     /opt/spark/jars/snowflake-jdbc-3.16.1.jar

COPY jars/hadoop-aws-3.3.4.jar \
     /opt/spark/jars/hadoop-aws-3.3.4.jar

COPY jars/aws-java-sdk-bundle-1.12.262.jar \
     /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

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
        "pyiceberg==0.7.1"

# ── spark-defaults.conf ───────────────────────────────────────────────────────
# Written to ${SPARK_HOME}/conf/spark-defaults.conf (empty in base image).
# Credentials (S3 keys, Polaris secret, Snowflake password) are NOT here —
# they are injected at runtime by bao_spark_init.py from OpenBao.
RUN mkdir -p "${SPARK_HOME}/conf" && cat > "${SPARK_HOME}/conf/spark-defaults.conf" << 'EOF'
# ── Gluten + Velox native execution (OPT-IN — not enabled globally) ──────────
# To enable Gluten in a specific session:
#   spark.conf.set("spark.plugins", "io.glutenproject.GlutenPlugin")
#   spark.conf.set("spark.gluten.sql.columnar.backend.lib", "velox")
# Do NOT set spark.plugins here — it causes ClassNotFoundException at SparkContext
# init when the Velox native lib fails to load on non-matching kernel/glibc.
spark.gluten.sql.columnar.backend.lib                velox
spark.gluten.loadLibFromJar                          true
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
spark.sql.catalog.snowflake_sample                   org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.snowflake_sample.type              hadoop
spark.sql.catalog.snowflake_sample.warehouse         SNOWFLAKE_SAMPLE_DATA
spark.sql.catalog.snowflake_sample.default-namespace tpcds_sf10tcl

# ── Global Iceberg write defaults (256 MB file size) ─────────────────────────
spark.sql.iceberg.write.format.default               parquet
spark.sql.iceberg.target-file-size-bytes             268435456

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
spark.driver.memory                                  2g
spark.executor.memory                                4g
spark.executor.cores                                 2
EOF

USER spark
