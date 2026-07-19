#!/bin/bash
# =============================================================================
# Ranger Admin entrypoint
# Merges environment variables into install.properties then runs setup.
# On first boot: runs db_setup.py. Always starts the admin server.
# =============================================================================
set -euo pipefail

RANGER_HOME=/opt/ranger
ADMIN_HOME=${RANGER_HOME}/admin
PROPS=/opt/ranger/install.properties
MARKER=${RANGER_HOME}/data/.setup_done

# If install.properties is mounted via ConfigMap, copy it in
if [ -f "/opt/ranger/install.properties" ]; then
    cp /opt/ranger/install.properties ${ADMIN_HOME}/install.properties
fi

# Substitute environment variables into install.properties
sed -i \
    -e "s|\${RANGER_DB_USER}|${RANGER_DB_USER:-ranger}|g" \
    -e "s|\${RANGER_DB_PASS}|${RANGER_DB_PASS:-ranger}|g" \
    -e "s|\${RANGER_DB_ROOT_PASS}|${RANGER_DB_ROOT_PASS:-postgres}|g" \
    -e "s|\${RANGER_ADMIN_PASSWORD}|${RANGER_ADMIN_PASSWORD:-Rangeradmin1}|g" \
    -e "s|\${RANGER_TAGSYNC_PASSWORD}|${RANGER_TAGSYNC_PASSWORD:-Rangeradmin1}|g" \
    -e "s|\${RANGER_USERSYNC_PASSWORD}|${RANGER_USERSYNC_PASSWORD:-Rangeradmin1}|g" \
    -e "s|\${RANGER_KEYADMIN_PASSWORD}|${RANGER_KEYADMIN_PASSWORD:-Rangeradmin1}|g" \
    ${ADMIN_HOME}/install.properties

# Run DB setup on first boot
if [ ! -f "${MARKER}" ]; then
    echo "[entrypoint] First boot — running Ranger DB setup..."
    cd ${ADMIN_HOME}
    python3 db_setup.py || true   # non-fatal if DB already initialised
    touch ${MARKER}
fi

echo "[entrypoint] Starting Ranger Admin..."
exec ${ADMIN_HOME}/ews/ranger-admin start
# Keep container alive — tail the log
tail -f ${ADMIN_HOME}/ews/logs/ranger-admin-*.log 2>/dev/null || sleep infinity
