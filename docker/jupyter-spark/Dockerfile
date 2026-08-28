# =============================================================================
# JupyterHub singleuser — Python 3.11 + PySpark 3.5.1 + Iceberg 1.9.1 — amd64
# Base  : quay.io/jupyterhub/singleuser:4.1.6
# Target: 192.168.1.50:30500/jupyter-spark:latest
#
# S3A JARs are baked in so every user pod can read s3:// / s3a:// URIs without
# any manual JAR copy step or spark.jars upload overhead.
# =============================================================================
FROM --platform=linux/amd64 quay.io/jupyterhub/singleuser:4.1.6

ARG SPARK_VERSION=3.5.1
ARG ICEBERG_VERSION=1.9.1
ARG PYICEBERG_VERSION=0.11.1
ARG SCALA_VERSION=2.12
ARG POSTGRES_JDBC_VERSION=42.7.3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

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

# Download all extra JARs into the pyspark jars directory so they are on the
# driver classpath automatically — no spark.jars upload, no manual copy needed.
RUN PYSPARK_JARS=$(python3 -c "import pyspark; print(pyspark.__file__.replace('__init__.py','jars'))") && \
    # Iceberg Spark runtime
    curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
    -o "${PYSPARK_JARS}/iceberg-spark-runtime.jar" && \
    # PostgreSQL JDBC
    curl -fsSL \
    "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    -o "${PYSPARK_JARS}/postgresql.jar" && \
    # hadoop-aws — S3A filesystem implementation
    curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -o "${PYSPARK_JARS}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    # aws-java-sdk-bundle — AWS SDK v1 required by hadoop-aws
    curl -fsSL \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    -o "${PYSPARK_JARS}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

RUN mkdir -p /usr/local/spark/conf
COPY spark-defaults.conf /usr/local/spark/conf/spark-defaults.conf

USER ${NB_USER}
WORKDIR /home/${NB_USER}
