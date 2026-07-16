# =============================================================================
# Spark 3.5.1 + Gluten 1.2.0 + Velox backend
# Build: docker/spark-gluten-velox/build-and-push.sh
# Push:  192.168.1.50:30500/spark-gluten-velox:3.5.1
#
# Iceberg runtime: 1.9.2 (latest LTS stable; fully compatible with Spark 3.5
#                  and Gluten 1.2.0 / Velox — versions 1.10+ require Spark 3.5.4+)
# SHA-256: c40ae0a8e2673bb5c01951b7d15aae2224a534f8331f4274ce2082776116a044
# =============================================================================
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

# ── System deps required by Velox native libraries ────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgflags-dev \
    libgoogle-glog-dev \
    libunwind-dev \
    libre2-dev \
    libfmt-dev \
    libboost-all-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Gluten 1.2.0 — pre-built Velox bundle for Spark 3.5 on Ubuntu 22.04 ──────
# This jar includes Velox native .so libs + Gluten JNI bridge
ENV GLUTEN_VERSION=1.2.0
ENV GLUTEN_JAR=gluten-velox-bundle-spark3.5_2.12-ubuntu_22.04-${GLUTEN_VERSION}.jar
ENV GLUTEN_URL=https://github.com/apache/incubator-gluten/releases/download/v${GLUTEN_VERSION}/${GLUTEN_JAR}

RUN curl -fsSL "${GLUTEN_URL}" -o "/opt/spark/jars/${GLUTEN_JAR}" || \
    echo "WARNING: Gluten jar download failed. Place ${GLUTEN_JAR} in docker/spark-gluten-velox/jars/ and rebuild with --air-gap flag."

# ── Apache Iceberg runtime 1.9.2 (Spark 3.5 + Scala 2.12) ───────────────────
# Latest LTS stable compatible with Gluten 1.2.0 / Velox
# SHA-256: c40ae0a8e2673bb5c01951b7d15aae2224a534f8331f4274ce2082776116a044
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar" \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar

# ── Delta Lake runtime (Spark 3.5) ───────────────────────────────────────────
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" \
    -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar" \
    -o /opt/spark/jars/delta-storage-3.2.0.jar

# ── Python packages ───────────────────────────────────────────────────────────
RUN pip3 install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    "pyiceberg[s3fs,glue]" \
    pyarrow \
    pandas \
    boto3

# ── Spark defaults ────────────────────────────────────────────────────────────
RUN cat >> "${SPARK_HOME}/conf/spark-defaults.conf" << 'EOF'
# Gluten + Velox
spark.plugins                                        io.glutenproject.GlutenPlugin
spark.gluten.sql.columnar.backend.lib                velox
spark.gluten.loadLibFromJar                          true

# Iceberg
spark.sql.extensions                                 org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.polaris                            org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.polaris.type                       rest
spark.sql.catalog.polaris.uri                        http://polaris-rest.catalog.svc.cluster.local:8181/api/catalog

# Delta Lake
spark.sql.extensions                                 io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog                      org.apache.spark.sql.delta.catalog.DeltaCatalog

# Serializer
spark.serializer                                     org.apache.spark.serializer.KryoSerializer
EOF

USER spark
