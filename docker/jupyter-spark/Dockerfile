# =============================================================================
# JupyterHub singleuser — Python 3.11 + PySpark 3.5.1 + Iceberg 1.9.2 — amd64
# Base  : quay.io/jupyterhub/singleuser:4.1.6
# Target: 192.168.1.50:30500/jupyter-spark:latest
#
# S3A JARs are baked in so every user pod can read s3:// / s3a:// URIs without
# any manual JAR copy step or spark.jars upload overhead.
#
# iceberg-aws-bundle is required alongside iceberg-spark-runtime so that the
# driver JVM can resolve S3FileIO (software.amazon.awssdk.*) — the same bundle
# that ships on every Spark worker via spark-gluten-velox.
# =============================================================================
FROM --platform=linux/amd64 quay.io/jupyterhub/singleuser:4.1.6

ARG SPARK_VERSION=3.5.1
ARG ICEBERG_VERSION=1.9.2
ARG ICEBERG_AWS_BUNDLE_VERSION=1.9.2
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

# Download Iceberg + PostgreSQL JARs from Maven (small files, fast).
# hadoop-aws, aws-java-sdk-bundle, and iceberg-aws-bundle are COPY'd from the
# build context to avoid re-downloading large files on every rebuild.
RUN PYSPARK_JARS=$(python3 -c "import pyspark; print(pyspark.__file__.replace('__init__.py','jars'))") && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
    -o "${PYSPARK_JARS}/iceberg-spark-runtime.jar" && \
    curl -fsSL \
    "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    -o "${PYSPARK_JARS}/postgresql.jar"

# Copy S3A JARs from build context (pre-fetched from the Spark master pod).
# To update versions: copy new JARs into docker/jupyter-spark/ and update ARG above.
COPY hadoop-aws-${HADOOP_AWS_VERSION}.jar \
     /opt/conda/lib/python3.11/site-packages/pyspark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar
COPY aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
     /opt/conda/lib/python3.11/site-packages/pyspark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# iceberg-aws-bundle provides AWS SDK v2 classes (S3FileIO, S3Exception, etc.)
# required by iceberg-spark-runtime 1.9.2 on the driver JVM.
# Workers already have this at /opt/spark/jars/ via spark-gluten-velox.
# Without it the driver throws: NoClassDefFoundError: software/amazon/awssdk/services/s3/model/S3Exception
COPY iceberg-aws-bundle-${ICEBERG_AWS_BUNDLE_VERSION}.jar \
     /opt/conda/lib/python3.11/site-packages/pyspark/jars/iceberg-aws-bundle.jar

RUN mkdir -p /usr/local/spark/conf
COPY spark-defaults.conf /usr/local/spark/conf/spark-defaults.conf

# Point PySpark at the conf dir where spark-defaults.conf lives.
# Without this, the file is never loaded and catalog registrations
# (spark.sql.catalog.polaris) are absent from the JVM, causing:
#   CatalogNotFoundException: spark.sql.catalog.polaris is not defined
ENV SPARK_CONF_DIR=/usr/local/spark/conf

USER ${NB_USER}
WORKDIR /home/${NB_USER}
