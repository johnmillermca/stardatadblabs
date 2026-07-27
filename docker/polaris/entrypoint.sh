#!/bin/bash
# Polaris entrypoint — substitutes env vars then starts the Polaris server
set -euo pipefail

POLARIS_HOME=/home/polaris
# The ConfigMap is mounted read-only; copy to /tmp so sed -i can write to it.
PROPS_SRC=${POLARIS_HOME}/application.properties
PROPS=/tmp/polaris-application.properties

if [ -f "${PROPS_SRC}" ]; then
    cp "${PROPS_SRC}" "${PROPS}"
    sed -i \
        -e "s|\${POLARIS_DB_USER}|${POLARIS_DB_USER:-polaris}|g" \
        -e "s|\${POLARIS_DB_PASS}|${POLARIS_DB_PASS:-changeme}|g" \
        -e "s|\${POLARIS_BOOTSTRAP_CLIENT_ID}|${POLARIS_BOOTSTRAP_CLIENT_ID}|g" \
        -e "s|\${POLARIS_BOOTSTRAP_CLIENT_SECRET}|${POLARIS_BOOTSTRAP_CLIENT_SECRET}|g" \
        "${PROPS}"
    echo "[entrypoint] Configuration written to ${PROPS}"
else
    echo "[ERROR] Source properties not found at ${PROPS_SRC}" >&2
    exit 1
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
