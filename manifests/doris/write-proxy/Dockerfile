# =============================================================================
# Doris Write Proxy
# Transparent MySQL protocol proxy — intercepts DML against managed Iceberg
# catalogs and pushes them to Spark via spark-submit, returning OK/ERR to
# the MySQL client.
#
# pyspark is installed to provide the spark-submit binary (client mode).
# The driver runs inside this pod and submits work to the remote Spark master.
# =============================================================================
FROM python:3.11-slim

WORKDIR /app

# pyspark provides spark-submit; version must match the cluster (3.5.1)
# Install Java (required by spark-submit) and pyspark
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    pip install --no-cache-dir pyspark==3.5.1 && \
    rm -rf /var/lib/apt/lists/*

# Copy proxy script and the Spark write-pushdown job
COPY doris_write_proxy.py   /app/doris_write_proxy.py
COPY spark_iceberg_write.py /app/spark_iceberg_write.py

EXPOSE 9040

CMD ["python", "/app/doris_write_proxy.py"]
