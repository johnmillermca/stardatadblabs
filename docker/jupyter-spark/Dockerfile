# =============================================================================
# JupyterHub singleuser — Python 3.11 + PySpark 3.5.1 + Iceberg 0.11.1 — amd64
# Base  : quay.io/jupyterhub/singleuser:4.1.6
# Target: 192.168.1.50:30500/jupyter-spark:latest
# =============================================================================
FROM --platform=linux/amd64 quay.io/jupyterhub/singleuser:4.1.6

ARG SPARK_VERSION=3.5.1
ARG ICEBERG_VERSION=1.9.1
ARG PYICEBERG_VERSION=0.11.1
ARG SCALA_VERSION=2.12
ARG POSTGRES_JDBC_VERSION=42.7.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jdk-headless curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN pip install --no-cache-dir \
    "pyspark==${SPARK_VERSION}" \
    pandas pyarrow \
    "pyiceberg==${PYICEBERG_VERSION}" \
    "s3fs>=2023.1.0" \
    sqlmesh boto3 matplotlib seaborn scikit-learn

RUN PYSPARK_JARS=$(python3 -c "import pyspark; print(pyspark.__file__.replace('__init__.py','jars'))") && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
    -o "${PYSPARK_JARS}/iceberg-spark-runtime.jar" && \
    curl -fsSL \
    "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    -o "${PYSPARK_JARS}/postgresql.jar"

RUN mkdir -p /usr/local/spark/conf
COPY spark-defaults.conf /usr/local/spark/conf/spark-defaults.conf

USER ${NB_USER}
WORKDIR /home/${NB_USER}
