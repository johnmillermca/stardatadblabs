# =============================================================================
# Apache Doris FE with Ranger Hive plugin on classpath
# Base  : 192.168.1.50:30500/apache/doris:fe-4.0.7
# Adds  : ranger-hive-plugin JARs (matching Ranger Admin version: 2.7.0)
# Target: 192.168.1.50:30500/apache/doris:fe-4.0.7-ranger
# =============================================================================
FROM 192.168.1.50:30500/apache/doris:fe-4.0.7

USER root

ARG RANGER_VERSION=2.7.0
ENV RANGER_VERSION=${RANGER_VERSION}

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && BASE="https://repo1.maven.org/maven2/org/apache/ranger" \
    && LIB="/opt/apache-doris/fe/lib" \
    && curl -fsSL "${BASE}/ranger-hive-plugin/${RANGER_VERSION}/ranger-hive-plugin-${RANGER_VERSION}.jar"       -o "${LIB}/ranger-hive-plugin-${RANGER_VERSION}.jar" \
    && curl -fsSL "${BASE}/ranger-plugins-common/${RANGER_VERSION}/ranger-plugins-common-${RANGER_VERSION}.jar" -o "${LIB}/ranger-plugins-common-${RANGER_VERSION}.jar" \
    && curl -fsSL "${BASE}/ranger-audit-core/${RANGER_VERSION}/ranger-audit-core-${RANGER_VERSION}.jar"         -o "${LIB}/ranger-audit-core-${RANGER_VERSION}.jar"
