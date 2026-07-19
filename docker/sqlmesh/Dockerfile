# =============================================================================
# SQLMesh 0.99.0 with Spark + Iceberg connectors — amd64
# Base  : docker.io/library/python:3.11-slim
# Target: 192.168.1.50:30500/sqlmesh:0.99.0
# =============================================================================
FROM --platform=linux/amd64 docker.io/library/python:3.11-slim

ARG SQLMESH_VERSION=0.99.0
ARG SPARK_VERSION=3.5.1
ARG ICEBERG_VERSION=1.9.2
ARG SCALA_VERSION=2.12
ARG POSTGRES_JDBC_VERSION=42.7.3

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jdk-headless curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "sqlmesh[spark,github,web]==${SQLMESH_VERSION}" \
    "pyspark==${SPARK_VERSION}"

RUN mkdir -p /opt/spark/jars && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
    -o /opt/spark/jars/iceberg-spark-runtime.jar && \
    curl -fsSL \
    "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    -o /opt/spark/jars/postgresql.jar

WORKDIR /app
RUN mkdir -p /app/config /app/models

EXPOSE 8001

CMD ["sqlmesh", "ui", "--host", "0.0.0.0", "--port", "8001"]
