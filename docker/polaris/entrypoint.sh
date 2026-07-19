#!/bin/bash
# Polaris entrypoint — substitutes env vars then starts the Polaris server
set -euo pipefail

POLARIS_HOME=/opt/polaris
PROPS=${POLARIS_HOME}/config/application.properties

# Substitute env vars injected from Kubernetes secret
if [ -f "${PROPS}" ]; then
    sed -i \
        -e "s|\${POLARIS_DB_USER}|${POLARIS_DB_USER:-polaris}|g" \
        -e "s|\${POLARIS_DB_PASS}|${POLARIS_DB_PASS:-polaris}|g" \
        "${PROPS}"
fi

# Find the server start script in the distribution
START_SCRIPT=$(find ${POLARIS_HOME}/dist -name "polaris-server" -o -name "run.sh" 2>/dev/null | head -1)
if [[ -n "${START_SCRIPT}" ]]; then
    echo "[entrypoint] Starting Apache Polaris via ${START_SCRIPT}..."
    exec "${START_SCRIPT}"
else
    # Fallback: find and run the quarkus runner jar
    JAR=$(find ${POLARIS_HOME}/dist -name "*runner*.jar" -o -name "*polaris*.jar" 2>/dev/null | head -1)
    echo "[entrypoint] Starting Apache Polaris (jar: ${JAR})..."
    exec java \
        -Dquarkus.config.locations="${PROPS}" \
        -jar "${JAR}"
fi
