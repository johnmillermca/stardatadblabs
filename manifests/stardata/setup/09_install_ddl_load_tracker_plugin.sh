#!/usr/bin/env bash
# =============================================================================
# 09_install_ddl_load_tracker_plugin.sh
# Build the DDL Load Tracker audit plugin and install it into StarData.
#
# What this script does
# ─────────────────────
#   1.  Reads the StarData admin password from the K8s secret
#       "stardata-credentials" (namespace prod).
#   2.  Applies 08_datetime_load_tracker.sql
#       • Creates cache_system (if not exists)
#       • Creates cache_system.datetime_load_tracker (idempotent)
#   3.  Builds the Java plugin via Maven (produces a fat-jar with shaded
#       MySQL Connector/J bundled inside).
#   4.  Packages the jar + plugin.properties into a zip archive that the
#       Doris INSTALL PLUGIN command expects.
#   5.  Copies the zip into the stardata-fe-0 pod (any FE will replicate it
#       to the other FE nodes automatically via BdbJE metadata sync).
#   6.  Runs INSTALL PLUGIN FROM "/path/ddl_load_tracker.zip" via mysql.
#       If the plugin is already installed, it is reinstalled (safe — Doris
#       handles idempotent reinstalls in 2.1+).
#
# Prerequisites
# ─────────────
#   • kubectl in PATH, with access to the prod namespace
#   • mysql client in PATH
#   • Java 11+ (javac, jar) in PATH
#   • Maven 3.8+ (mvn) in PATH — OR set MVN_SKIP=1 to skip build and use
#     a pre-built jar at stardata-access/ddl-load-tracker-plugin/target/
#
# Environment variables
# ─────────────────────
#   STARDATA_PASS   Override password (auto-fetched from K8s secret if unset)
#   SD_HOST         Doris FE MySQL host     (default: 192.168.1.50)
#   SD_PORT         Doris FE MySQL port     (default: 9030)
#   SD_USER         Doris user              (default: root)
#   SD_HTTP_PORT    Doris FE HTTP port      (default: 8030)
#   MVN_SKIP        Set to 1 to skip Maven build (use existing target jar)
#   DRY_RUN         Set to 1 to print commands without executing
#
# Usage
# ─────
#   bash manifests/stardata/setup/09_install_ddl_load_tracker_plugin.sh
#
#   # Skip Maven build (jar already built):
#   MVN_SKIP=1 bash manifests/stardata/setup/09_install_ddl_load_tracker_plugin.sh
#
#   # Dry-run (print all steps without executing):
#   DRY_RUN=1 bash manifests/stardata/setup/09_install_ddl_load_tracker_plugin.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
PLUGIN_DIR="${REPO_ROOT}/stardata-access/ddl-load-tracker-plugin"
PLUGIN_ZIP="${PLUGIN_DIR}/target/ddl_load_tracker.zip"
PLUGIN_JAR="${PLUGIN_DIR}/target/ddl-load-tracker-plugin-1.0.0.jar"
PROPS_FILE="${PLUGIN_DIR}/src/main/resources/plugin.properties"

SD_HOST="${SD_HOST:-192.168.1.50}"
SD_PORT="${SD_PORT:-9030}"
SD_USER="${SD_USER:-root}"
SD_HTTP_PORT="${SD_HTTP_PORT:-8030}"
MVN_SKIP="${MVN_SKIP:-0}"
DRY_RUN="${DRY_RUN:-0}"

# Destination path inside the FE pod — must be writable by the doris process
PLUGIN_POD_PATH="/opt/apache-doris/fe/plugins/ddl_load_tracker.zip"

# ── Colour helpers ────────────────────────────────────────────────────────────
green()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[0;33m%s\033[0m\n' "$*"; }
red()    { printf '\033[0;31m%s\033[0m\n' "$*"; }
bold()   { printf '\033[1m%s\033[0m\n'    "$*"; }

run() {
    # Wrapper: in DRY_RUN mode, print the command; otherwise execute it.
    if [[ "${DRY_RUN}" == "1" ]]; then
        yellow "  [DRY RUN] $*"
    else
        "$@"
    fi
}

bold "=== StarData — DDL Load Tracker Plugin Setup ==="
echo ""

# ── Step 0: Password ──────────────────────────────────────────────────────────
if [[ -z "${STARDATA_PASS:-}" ]]; then
    yellow "→ Reading STARDATA_PASS from K8s secret stardata-credentials …"
    STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
        -o jsonpath='{.data.admin-password}' | base64 -d)"
fi
[[ -z "${STARDATA_PASS}" ]] && { red "ERROR: STARDATA_PASS is empty."; exit 1; }
green "✓ Credentials ready"

# MySQL helper
mysql_exec() {
    local label="$1"; shift
    bold "── ${label} ──"
    if [[ "${DRY_RUN}" == "1" ]]; then
        yellow "  [DRY RUN] mysql -h ${SD_HOST} -P ${SD_PORT} $*"
        return 0
    fi
    mysql -h "${SD_HOST}" -P "${SD_PORT}" \
          -u "${SD_USER}" -p"${STARDATA_PASS}" \
          --default-character-set=utf8mb4 \
          "$@" \
    && green "  ✓ ${label} OK" \
    || { red "  ✗ ${label} FAILED"; exit 1; }
}

# ── Step 1: Create tracker table ──────────────────────────────────────────────
echo ""
mysql_exec "08 — Create cache_system.datetime_load_tracker" \
    < "${SCRIPT_DIR}/08_datetime_load_tracker.sql"

# ── Step 2: Build the plugin jar via Maven ────────────────────────────────────
echo ""
bold "── Build — Maven fat-jar ──"
if [[ "${MVN_SKIP}" == "1" ]]; then
    yellow "  MVN_SKIP=1 — skipping Maven build, using existing jar"
    [[ -f "${PLUGIN_JAR}" ]] || { red "ERROR: pre-built jar not found at ${PLUGIN_JAR}"; exit 1; }
else
    if [[ "${DRY_RUN}" == "1" ]]; then
        yellow "  [DRY RUN] mvn -f ${PLUGIN_DIR}/pom.xml clean package -q"
    else
        mvn -f "${PLUGIN_DIR}/pom.xml" clean package -q \
        && green "  ✓ Maven build succeeded → ${PLUGIN_JAR}" \
        || { red "  ✗ Maven build FAILED — see output above"; exit 1; }
    fi
fi

# ── Step 3: Package into zip ──────────────────────────────────────────────────
echo ""
bold "── Package — zip archive ──"
if [[ "${DRY_RUN}" == "1" ]]; then
    yellow "  [DRY RUN] zip ${PLUGIN_ZIP} ${PLUGIN_JAR} ${PROPS_FILE}"
else
    # Doris INSTALL PLUGIN expects: plugin.properties + the jar at zip root level
    ZIP_STAGING="$(mktemp -d)"
    cp "${PLUGIN_JAR}"   "${ZIP_STAGING}/ddl-load-tracker-plugin-1.0.0.jar"
    cp "${PROPS_FILE}"   "${ZIP_STAGING}/plugin.properties"
    (cd "${ZIP_STAGING}" && zip -j "${PLUGIN_ZIP}" ./*.jar plugin.properties)
    rm -rf "${ZIP_STAGING}"
    green "  ✓ Plugin zip ready → ${PLUGIN_ZIP}"
fi

# ── Step 4: Copy zip into the FE pod ─────────────────────────────────────────
echo ""
bold "── Deploy — copy zip into stardata-fe-0 ──"

# Find the first ready FE pod
FE_POD="$(kubectl get pod -n prod -l app=stardata-fe \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"

if [[ -z "${FE_POD}" ]]; then
    red "ERROR: No running stardata-fe pod found in namespace prod."
    exit 1
fi
green "  → Using FE pod: ${FE_POD}"

# Ensure the plugins directory exists inside the pod
run kubectl exec -n prod "${FE_POD}" -- \
    mkdir -p /opt/apache-doris/fe/plugins

run kubectl cp "${PLUGIN_ZIP}" \
    "prod/${FE_POD}:${PLUGIN_POD_PATH}"
green "  ✓ Zip copied to ${FE_POD}:${PLUGIN_POD_PATH}"

# ── Step 5: INSTALL PLUGIN ────────────────────────────────────────────────────
echo ""
bold "── Install — INSTALL PLUGIN FROM pod path ──"

# If already installed, uninstall first so we get a clean reinstall
# (Doris 2.1+ supports INSTALL PLUGIN idempotently, but older versions don't)
UNINSTALL_SQL="UNINSTALL PLUGIN ddl_load_tracker;"
INSTALL_SQL="INSTALL PLUGIN FROM \"${PLUGIN_POD_PATH}\";"

if [[ "${DRY_RUN}" == "1" ]]; then
    yellow "  [DRY RUN] UNINSTALL PLUGIN ddl_load_tracker  (best-effort, ignore error)"
    yellow "  [DRY RUN] INSTALL PLUGIN FROM \"${PLUGIN_POD_PATH}\""
else
    # Best-effort uninstall — ignore error if not previously installed
    mysql -h "${SD_HOST}" -P "${SD_PORT}" \
          -u "${SD_USER}" -p"${STARDATA_PASS}" \
          --default-character-set=utf8mb4 \
          -e "${UNINSTALL_SQL}" 2>/dev/null || true

    mysql -h "${SD_HOST}" -P "${SD_PORT}" \
          -u "${SD_USER}" -p"${STARDATA_PASS}" \
          --default-character-set=utf8mb4 \
          -e "${INSTALL_SQL}" \
    && green "  ✓ Plugin installed successfully" \
    || { red "  ✗ INSTALL PLUGIN FAILED — see output above"; exit 1; }
fi

# ── Step 6: Verify ────────────────────────────────────────────────────────────
echo ""
bold "── Verify ──"
mysql_exec "SHOW PLUGINS" \
    -e "SHOW PLUGINS;" | grep -i "ddl_load_tracker" \
    || yellow "  ⚠ Plugin not visible in SHOW PLUGINS yet — may need FE restart"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
bold "════════════════════════════════════════════════════════════════"
green "  DDL Load Tracker plugin installed."
bold "════════════════════════════════════════════════════════════════"
echo ""
echo "  Tracker table : cache_system.datetime_load_tracker"
echo "  Plugin        : ddl_load_tracker  (Doris Audit Plugin, async)"
echo ""
echo "  Behaviour:"
echo "    Every CREATE TABLE or ALTER TABLE that completes on any user"
echo "    database triggers an async background upsert into the tracker"
echo "    table — only for tables that have BOTH:"
echo "      (a) a DATETIME DEFAULT CURRENT_TIMESTAMP column"
echo "      (b) a DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE … column"
echo ""
echo "  Zero performance impact: the plugin callback returns immediately;"
echo "  all JDBC work happens on a dedicated background thread."
echo ""
echo "  To verify a table was tracked after CREATE TABLE:"
echo "    SELECT * FROM cache_system.datetime_load_tracker"
echo "    WHERE table_schema = '<db>' AND table_name = '<tbl>';"
echo ""
