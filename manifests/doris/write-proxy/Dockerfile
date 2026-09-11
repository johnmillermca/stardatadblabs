# =============================================================================
# Doris Write Proxy
# Transparent MySQL protocol proxy — intercepts DML against managed Iceberg
# catalogs and pushes them to Spark via spark-submit, returning OK/ERR to
# the MySQL client.
#
# pyspark is installed to provide the spark-submit binary (client mode).
# The driver runs inside this pod and submits work to the remote Spark master.
#
# The Gluten JAR must be physically present on the driver (this pod) at
# SparkContext init time — spark.plugins loads GlutenPlugin before --jars
# are placed on the classpath, so an HTTP URL in --driver-class-path won't
# work.  We copy the JAR from the cluster Spark image via a multi-stage build.
# =============================================================================

# ── Stage 1: extract the Gluten JAR from the cluster Spark image ─────────────
FROM 192.168.1.50:30500/spark-gluten-velox:3.5.1-5 AS spark-jars

# ── Stage 2: the actual proxy image ──────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# pyspark provides spark-submit; version must match the cluster (3.5.1)
# Install Java (required by spark-submit) and pyspark
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    pip install --no-cache-dir pyspark==3.5.1 && \
    rm -rf /var/lib/apt/lists/*

# Copy the Gluten JAR from the Spark stage so the driver classloader can find
# it locally — required for --driver-class-path at SparkContext init time.
RUN mkdir -p /opt/gluten
COPY --from=spark-jars \
    /opt/spark/jars/gluten-velox-bundle-spark3.5_2.12-centos_7_x86_64-1.2.0.jar \
    /opt/gluten/gluten-velox-bundle-spark3.5_2.12-centos_7_x86_64-1.2.0.jar

# Copy proxy script and the Spark write-pushdown job
COPY doris_write_proxy.py   /app/doris_write_proxy.py
COPY spark_iceberg_write.py /app/spark_iceberg_write.py

EXPOSE 9040

CMD ["python", "/app/doris_write_proxy.py"]
