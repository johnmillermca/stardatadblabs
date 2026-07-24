#!/bin/bash
# Polaris entrypoint — substitutes env vars then starts the Polaris server
set -euo pipefail

POLARIS_HOME=/opt/polaris
PROPS=${POLARIS_HOME}/application.properties

# Substitute env vars injected from Kubernetes secret
if [ -f "${PROPS}" ]; then
    sed -i \
        -e "s|\${POLARIS_DB_USER}|${POLARIS_DB_USER:-polaris}|g" \
        -e "s|\${POLARIS_DB_PASS}|${POLARIS_DB_PASS:-polaris}|g" \
        "${PROPS}"
fi

# Start the Quarkus server from the dist/server directory
JAR="${POLARIS_HOME}/dist/server/quarkus-run.jar"
if [[ -f "${JAR}" ]]; then
    echo "[entrypoint] Starting Apache Polaris (jar: ${JAR})..."
    exec java \
        -Dquarkus.config.locations="${PROPS}" \
        -jar "${JAR}"
else
    echo "[ERROR] Polaris server jar not found at ${JAR}" >&2
    find ${POLARIS_HOME}/dist -name "*.jar" | head -10
    exit 1
fi
