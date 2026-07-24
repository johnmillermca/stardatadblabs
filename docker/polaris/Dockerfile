# =============================================================================
# Apache Polaris 1.6.0 (Iceberg REST Catalog) — amd64
# Base  : docker.io/eclipse-temurin:21-jre-jammy
# Source: polaris-bin-1.6.0.tgz pre-built binary from GitHub releases
# Target: 192.168.1.50:30500/apache-polaris:1.6.0
# =============================================================================
FROM --platform=linux/amd64 docker.io/eclipse-temurin:21-jre-jammy

ARG POLARIS_VERSION=1.6.0

ENV POLARIS_HOME=/opt/polaris

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl bash tar gzip \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r polaris && useradd -r -g polaris -d ${POLARIS_HOME} polaris \
    && mkdir -p ${POLARIS_HOME}/config ${POLARIS_HOME}/logs

RUN curl -fsSL \
    "https://github.com/apache/polaris/releases/download/apache-polaris-${POLARIS_VERSION}/polaris-bin-${POLARIS_VERSION}.tgz" \
    | tar -xzf - -C /opt \
    && mv /opt/polaris-bin-${POLARIS_VERSION} ${POLARIS_HOME}/dist \
    && chown -R polaris:polaris ${POLARIS_HOME}

WORKDIR ${POLARIS_HOME}

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER polaris
EXPOSE 8181

ENTRYPOINT ["/entrypoint.sh"]
