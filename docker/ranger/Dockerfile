# =============================================================================
# Apache Ranger 2.7.0 Admin Server (amd64)
# Base  : docker.io/apache/ranger:2.7.0  (multi-arch, includes amd64)
# Adds  : PostgreSQL JDBC driver + env-var entrypoint wrapper
# Target: 192.168.1.50:30500/apache-ranger:2.7.0
# =============================================================================
FROM --platform=linux/amd64 docker.io/apache/ranger:2.7.0

USER root

ARG POSTGRES_JDBC_VERSION=42.7.3

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL \
       "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
       -o /usr/share/java/postgresql.jar

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 6080

ENTRYPOINT ["/entrypoint.sh"]
