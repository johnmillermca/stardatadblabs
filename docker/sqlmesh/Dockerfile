# =============================================================================
# SQLMesh 0.99.0 — production image
# Base: python:3.11-slim
# Includes: sqlmesh, pyspark 3.5, iceberg connector, postgres driver
# Build: bash docker/sqlmesh/build-and-push.sh
# =============================================================================
FROM python:3.11-slim

ARG SQLMESH_VERSION=0.99.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget git openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --no-cache-dir \
    "sqlmesh[spark]==${SQLMESH_VERSION}" \
    psycopg2-binary \
    "pyspark==3.5.1" \
    pyiceberg \
    pandas \
    pyarrow

WORKDIR /app

RUN mkdir -p /app/models /app/config /app/logs

EXPOSE 8001

CMD ["/bin/bash", "/app/config/entrypoint.sh"]
