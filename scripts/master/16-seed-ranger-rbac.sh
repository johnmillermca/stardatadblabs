#!/usr/bin/env bash
# =============================================================================
# 16-seed-ranger-rbac.sh
#
# Configures Apache Ranger via REST API:
#   1. Creates Ranger groups:  account_admin, caching_admin, caching_dev,
#                              processing_admin, processing_dev,
#                              streaming_admin, streaming_dev
#   OpenSearch is part of PROCESSING_ZONE (search/analytics — not streaming transport)
#   2. Creates Ranger users and assigns them to groups
#   3. Registers Ranger services (plugins) for each application
#   4. Creates Security Zones:  CACHING_ZONE, PROCESSING_ZONE, STREAMING_ZONE
#   5. Creates access policies per zone
#   6. Creates Doris column-masking policies for caching_dev
#
# Auth: Ranger admin username/password (dev mode — no SPNEGO keytab required)
#
# Usage: sudo bash scripts/master/16-seed-ranger-rbac.sh
# Safe to re-run — every operation checks for existence before creating.
#
# Performance notes embedded in policy configs:
#   - Ranger policy cache TTL set to 30s (plugin.policy.cache.reload.intervalMs)
#   - Plugin audit is async (non-blocking) — zero latency on data path
#   - Policy decision is made locally in the plugin process using cached policies
#     — no Ranger Admin network round-trip per query
# =============================================================================
set -euo pipefail

NAMESPACE="prod"
RANGER_URL="http://192.168.1.50:30680"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }
skip() { echo "  – $* (already exists)"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

# ── Read Ranger admin password from K8s secret ────────────────────────────────
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n ${NAMESPACE} \
  -o jsonpath='{.data.admin-password}' | base64 -d)
[[ -n "${RANGER_PASS}" ]] || die "Could not read ranger-db-credentials admin-password"

RANGER_AUTH="admin:${RANGER_PASS}"

# ── Wait for Ranger to be reachable ──────────────────────────────────────────
log "Waiting for Ranger Admin to be reachable at ${RANGER_URL}..."
for i in $(seq 1 30); do
  if curl -sf -u "${RANGER_AUTH}" "${RANGER_URL}/service/public/v2/api/service" \
      -o /dev/null 2>/dev/null; then
    ok "Ranger is reachable"
    break
  fi
  [[ $i -eq 30 ]] && die "Ranger not reachable after 150s — is it running?"
  sleep 5
done

# ── REST helpers ──────────────────────────────────────────────────────────────
# Use -f only on GET so errors surface; POST/PUT return full body for parsing.
ranger_get() {
  curl -sf -u "${RANGER_AUTH}" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    "${RANGER_URL}${1}"
}

# ranger_post: returns full response body (HTTP errors included).
# Callers must handle non-JSON / error responses.
ranger_post() {
  local path="$1"; local body="$2"
  curl -s -u "${RANGER_AUTH}" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -X POST -d "${body}" \
    "${RANGER_URL}${path}"
}

ranger_put() {
  local path="$1"; local body="$2"
  curl -s -u "${RANGER_AUTH}" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -X PUT -d "${body}" \
    "${RANGER_URL}${path}"
}

# Extract id from a ranger_post response; prints "" on any error.
extract_id() {
  python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('id', ''))
except Exception:
    print('')
"
}

group_id_by_name() {
  ranger_get "/service/xusers/groups" 2>/dev/null \
    | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    groups = data.get('vXGroups', [])
    for g in groups:
        if g['name'] == '${1}':
            print(g['id'])
            sys.exit(0)
except Exception:
    pass
print('')
"
}

user_id_by_name() {
  # Ranger list endpoint omits REST-API-created users from its results.
  # Use the /name/ direct lookup endpoint first; fall back to list scan.
  local by_name
  by_name=$(ranger_get "/service/xusers/users/name/${1}" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id',''))" \
    2>/dev/null || echo "")
  if [[ -n "${by_name}" ]]; then
    echo "${by_name}"
    return
  fi
  # Fallback: scan list (catches usersync-managed accounts)
  ranger_get "/service/xusers/users?pageSize=500" 2>/dev/null \
    | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    users = data.get('vXUsers', [])
    for u in users:
        if u['name'] == '${1}':
            print(u['id'])
            sys.exit(0)
except Exception:
    pass
print('')
"
}

service_id_by_name() {
  ranger_get "/service/public/v2/api/service/name/${1}" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id',''))" \
    2>/dev/null || echo ""
}

zone_id_by_name() {
  ranger_get "/service/public/v2/api/zones" 2>/dev/null \
    | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for z in data:
        if z.get('name') == '${1}':
            print(z['id'])
            sys.exit(0)
except Exception:
    pass
print('')
" 2>/dev/null || echo ""
}

policy_exists() {
  # $1=service_name $2=policy_name $3=zone_name (optional)
  # Zone-scoped policies are invisible to queries without a zoneName filter.
  # Check with zone filter (if provided) AND without — return true if found in either.
  local _check_zone=""
  if [[ -n "${3:-}" ]]; then
    _check_zone=$(ranger_get "/service/public/v2/api/policy?serviceName=${1}&zoneName=${3}&pageSize=500" \
      | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    policies = d if isinstance(d, list) else d.get('policies', [])
    for p in policies:
        if p.get('name') == '${2}':
            print('yes')
            sys.exit(0)
except Exception:
    pass
print('')
" 2>/dev/null || echo "")
  fi
  local _check_nozone
  _check_nozone=$(ranger_get "/service/public/v2/api/policy?serviceName=${1}&pageSize=500" \
    | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    policies = d if isinstance(d, list) else d.get('policies', [])
    for p in policies:
        if p.get('name') == '${2}':
            print('yes')
            sys.exit(0)
except Exception:
    pass
print('')
" 2>/dev/null || echo "")
  [[ "${_check_zone}" == "yes" || "${_check_nozone}" == "yes" ]]
}

# ─────────────────────────────────────────────────────────────────────────────
# 1. GROUPS
# ─────────────────────────────────────────────────────────────────────────────
log "=== Creating Ranger Groups ==="

create_group() {
  local name="$1" desc="$2"
  local existing
  existing=$(group_id_by_name "${name}")
  if [[ -n "${existing}" ]]; then
    # Redirect log to stderr — only the id goes to stdout for capture
    skip "Group: ${name} (id=${existing})" >&2
    echo "${existing}"
    return
  fi
  local response id
  response=$(ranger_post "/service/xusers/groups" \
    "{\"name\":\"${name}\",\"description\":\"${desc}\",\"groupType\":0}")
  id=$(echo "${response}" | extract_id)
  if [[ -z "${id}" ]]; then
    die "Failed to create group ${name}. Ranger response: ${response}" >&2
  fi
  ok "Group: ${name} (id=${id})" >&2
  echo "${id}"
}

GID_ACCOUNT_ADMIN=$(create_group "account_admin"     "Super group — full access to all layers")
GID_CACHING_ADMIN=$(create_group "caching_admin"     "CACHING_ZONE admin — full DDL/DML on Doris")
GID_CACHING_DEV=$(create_group   "caching_dev"       "CACHING_ZONE dev — SELECT with column masking")
GID_PROC_ADMIN=$(create_group    "processing_admin"  "PROCESSING_ZONE admin — Spark, SQLMesh, Kestra, OpenSearch full access")
GID_PROC_DEV=$(create_group      "processing_dev"    "PROCESSING_ZONE dev — read-only")
GID_STREAM_ADMIN=$(create_group  "streaming_admin"   "STREAMING_ZONE admin — Kafka, Schema Registry, Debezium, AKHQ full access")
GID_STREAM_DEV=$(create_group    "streaming_dev"     "STREAMING_ZONE dev — consume-only, read-only")

# ─────────────────────────────────────────────────────────────────────────────
# 2. USERS
# ─────────────────────────────────────────────────────────────────────────────
log "=== Creating Ranger Users ==="

# Ranger password requirements: min 8 chars, must contain uppercase, lowercase,
# digit, and special character. The stored password is alphanumeric-only (from
# gen_password), so we append a fixed suffix "Aa1!" to satisfy the policy
# without changing the stored secret. Strip the base64 trailing newline with tr.
get_rbac_pass() {
  local raw
  raw=$(kubectl get secret rbac-users -n ${NAMESPACE} \
    -o jsonpath="{.data.${1}}" 2>/dev/null | base64 -d | tr -d '\n\r' \
    || echo "")
  if [[ -z "${raw}" ]]; then
    # Secret missing — use a safe fallback that meets Ranger complexity rules
    echo "RbacDev1!"
  else
    # Append complexity suffix so pure-alphanumeric stored passwords pass Ranger
    # password policy: uppercase(A), lowercase(a), digit(1), special(!).
    # Truncate to 28 chars total to stay within Ranger's 40-char UI limit.
    echo "${raw}Aa1!" | head -c 28
  fi
}

create_user_in_group() {
  local username="$1"
  local group_id="$2"
  local group_name="$3"
  local first_name="$4"
  local password
  password=$(get_rbac_pass "${username}-password")

  local existing_id
  existing_id=$(user_id_by_name "${username}")
  if [[ -n "${existing_id}" ]]; then
    skip "User: ${username} (id=${existing_id})"
    return
  fi

  local response user_id
  response=$(ranger_post "/service/xusers/users" \
    "{\"name\":\"${username}\",
      \"firstName\":\"${first_name}\",
      \"lastName\":\"User\",
      \"password\":\"${password}\",
      \"userRoleList\":[\"ROLE_USER\"],
      \"groupIdList\":[${group_id}],
      \"groupNameList\":[\"${group_name}\"]}")
  user_id=$(echo "${response}" | extract_id)
  if [[ -z "${user_id}" ]]; then
    die "Failed to create user ${username}. Ranger response: ${response}"
  fi

  # Ranger ignores groupIdList on POST — explicitly link user to group via groupusers API
  ranger_post "/service/xusers/groupusers" \
    "{\"name\":\"${group_name}\",
      \"userId\":${user_id},
      \"groupId\":${group_id}}" > /dev/null 2>&1 || true

  ok "User: ${username} → group ${group_name} (id=${user_id})"
}

# account_admin users — ROLE_SYS_ADMIN requires the /secure/users endpoint
create_admin_user() {
  local username="$1"
  local group_id="$2"
  local password
  password=$(get_rbac_pass "${username}-password")

  local existing_id
  existing_id=$(user_id_by_name "${username}")
  if [[ -n "${existing_id}" ]]; then
    skip "Admin user: ${username} (id=${existing_id})"
    return
  fi

  local response user_id
  # Try /secure/users first (required on some Ranger versions for ROLE_SYS_ADMIN).
  # Fall back to /users if secure endpoint returns an error.
  response=$(ranger_post "/service/xusers/secure/users" \
    "{\"name\":\"${username}\",
      \"firstName\":\"Platform\",
      \"lastName\":\"Admin\",
      \"password\":\"${password}\",
      \"userRoleList\":[\"ROLE_SYS_ADMIN\"],
      \"groupIdList\":[${group_id}],
      \"groupNameList\":[\"account_admin\"]}")
  user_id=$(echo "${response}" | extract_id)

  if [[ -z "${user_id}" ]]; then
    # Fallback: standard users endpoint
    response=$(ranger_post "/service/xusers/users" \
      "{\"name\":\"${username}\",
        \"firstName\":\"Platform\",
        \"lastName\":\"Admin\",
        \"password\":\"${password}\",
        \"userRoleList\":[\"ROLE_SYS_ADMIN\"],
        \"groupIdList\":[${group_id}],
        \"groupNameList\":[\"account_admin\"]}")
    user_id=$(echo "${response}" | extract_id)
  fi

  if [[ -z "${user_id}" ]]; then
    die "Failed to create admin user ${username}. Ranger response: ${response}"
  fi
  ok "Admin user: ${username} (id=${user_id})"
}

create_admin_user   "platform_admin"       "${GID_ACCOUNT_ADMIN}"
create_user_in_group "caching_admin_user"  "${GID_CACHING_ADMIN}"  "caching_admin"   "Caching"
create_user_in_group "caching_dev_user"    "${GID_CACHING_DEV}"    "caching_dev"     "CachingDev"
create_user_in_group "processing_admin_user" "${GID_PROC_ADMIN}"   "processing_admin" "Processing"
create_user_in_group "processing_dev_user" "${GID_PROC_DEV}"       "processing_dev"  "ProcessingDev"
create_user_in_group "streaming_admin_user" "${GID_STREAM_ADMIN}"  "streaming_admin" "Streaming"
create_user_in_group "streaming_dev_user"  "${GID_STREAM_DEV}"     "streaming_dev"   "StreamingDev"

# ─────────────────────────────────────────────────────────────────────────────
# 3. SERVICES (Ranger plugin registrations)
# ─────────────────────────────────────────────────────────────────────────────
log "=== Registering Ranger Services ==="

create_service() {
  local svc_name="$1"
  local svc_type="$2"
  local body="$3"

  local existing_id
  existing_id=$(service_id_by_name "${svc_name}")
  if [[ -n "${existing_id}" ]]; then
    skip "Service: ${svc_name} (id=${existing_id})"
    return
  fi

  local response id
  response=$(ranger_post "/service/public/v2/api/service" "${body}")
  id=$(echo "${response}" | extract_id)
  if [[ -z "${id}" ]]; then
    die "Failed to create service ${svc_name}. Ranger response: ${response}"
  fi
  ok "Service: ${svc_name} (type=${svc_type}, id=${id})"
}

# ── Performance: policyUpdateIntervalMs=30000 means plugins poll every 30s.
#    Cached policies are evaluated IN-PROCESS — zero network overhead per query.
DORIS_PASS=$(kubectl get secret doris-credentials -n ${NAMESPACE} \
  -o jsonpath='{.data.admin-password}' | base64 -d)

create_service "doris_service" "hive" "{
  \"name\":\"doris_service\",
  \"type\":\"hive\",
  \"description\":\"Apache Doris — CACHING_ZONE\",
  \"isEnabled\":true,
  \"configs\":{
    \"username\":\"root\",
    \"password\":\"${DORIS_PASS}\",
    \"jdbc.driverClassName\":\"com.mysql.jdbc.Driver\",
    \"jdbc.url\":\"jdbc:mysql://192.168.1.50:30090/\",
    \"commonNameForCertificate\":\"\",
    \"ranger.plugin.audit.destination.solr.urls\":\"\",
    \"ranger.plugin.audit.destination.solr.zookeepers\":\"\",
    \"policy.download.auth.users\":\"root\",
    \"tag.download.auth.users\":\"root\",
    \"ranger.plugin.policy.cache.dir\":\"/tmp/ranger/doris\",
    \"ranger.plugin.policy.pollIntervalMs\":\"30000\"
  }
}"

# ── Kafka SASL credentials (needed by Ranger kafka service type as username/password) ──
KAFKA_USER=$(kubectl get secret kafka-app-user -n ${NAMESPACE} \
  -o jsonpath='{.data.password}' 2>/dev/null | base64 -d | tr -d '\n' || echo "kafka")

# Strimzi Kafka uses KRaft (no ZooKeeper). Ranger's native kafka service type
# requires zookeeper.connect, which does not exist in a KRaft cluster.
# Use tag service type so Ranger registers kafka_service for policy management
# without attempting a ZooKeeper connectivity test.
create_service "kafka_service" "tag" "{
  \"name\":\"kafka_service\",
  \"type\":\"tag\",
  \"description\":\"Apache Kafka (Strimzi KRaft) — STREAMING_ZONE (tag service)\",
  \"isEnabled\":true,
  \"configs\":{
    \"ranger.plugin.policy.cache.dir\":\"/tmp/ranger/kafka\",
    \"ranger.plugin.policy.pollIntervalMs\":\"30000\"
  }
}"

# Spark/SQLMesh/Kestra: use tag service type (no JDBC connection needed for
# policy-only registration — Ranger tag service skips connectivity validation).
create_service "spark_service" "tag" "{
  \"name\":\"spark_service\",
  \"type\":\"tag\",
  \"description\":\"Apache Spark — PROCESSING_ZONE (tag service, policy-only)\",
  \"isEnabled\":true,
  \"configs\":{
    \"ranger.plugin.policy.cache.dir\":\"/tmp/ranger/spark\",
    \"ranger.plugin.policy.pollIntervalMs\":\"30000\"
  }
}"

create_service "sqlmesh_service" "tag" "{
  \"name\":\"sqlmesh_service\",
  \"type\":\"tag\",
  \"description\":\"SQLMesh — PROCESSING_ZONE (tag service, policy-only)\",
  \"isEnabled\":true,
  \"configs\":{
    \"ranger.plugin.policy.cache.dir\":\"/tmp/ranger/sqlmesh\",
    \"ranger.plugin.policy.pollIntervalMs\":\"30000\"
  }
}"

create_service "kestra_service" "tag" "{
  \"name\":\"kestra_service\",
  \"type\":\"tag\",
  \"description\":\"Apache Kestra — PROCESSING_ZONE (tag service, policy-only)\",
  \"isEnabled\":true,
  \"configs\":{
    \"ranger.plugin.policy.cache.dir\":\"/tmp/ranger/kestra\",
    \"ranger.plugin.policy.pollIntervalMs\":\"30000\"
  }
}"

# OpenSearch: use tag service type — OpenSearch security plugin is disabled in
# this lab (docs/opensearch.md: "Security plugin is disabled for lab use").
# Tag service registers the service in Ranger for policy management without
# attempting a live connectivity test to OpenSearch.
create_service "opensearch_service" "tag" "{
  \"name\":\"opensearch_service\",
  \"type\":\"tag\",
  \"description\":\"OpenSearch — PROCESSING_ZONE (tag service, policy-only)\",
  \"isEnabled\":true,
  \"configs\":{
    \"ranger.plugin.policy.cache.dir\":\"/tmp/ranger/opensearch\",
    \"ranger.plugin.policy.pollIntervalMs\":\"30000\"
  }
}"


create_service "polaris_service" "tag" "{
  \"name\":\"polaris_service\",
  \"type\":\"tag\",
  \"description\":\"Apache Polaris Iceberg REST Catalog — PROCESSING_ZONE (tag service)\",
  \"isEnabled\":true,
  \"configs\":{
    \"ranger.plugin.policy.cache.dir\":\"/tmp/ranger/polaris\",
    \"ranger.plugin.policy.pollIntervalMs\":\"30000\"
  }
}"


# Strimzi Kafka runs KRaft (no ZooKeeper). Ranger's kafka service type requires
# zookeeper.connect which does not exist in KRaft clusters. Use tag service type
# for all Kafka-adjacent services so Ranger registers them for policy management
# without attempting a ZooKeeper/broker connectivity test.
create_service "schema_registry_service" "tag" "{
  \"name\":\"schema_registry_service\",
  \"type\":\"tag\",
  \"description\":\"Schema Registry — STREAMING_ZONE (tag service, KRaft cluster)\",
  \"isEnabled\":true,
  \"configs\":{
    \"ranger.plugin.policy.cache.dir\":\"/tmp/ranger/schema-registry\",
    \"ranger.plugin.policy.pollIntervalMs\":\"30000\"
  }
}"

create_service "debezium_service" "tag" "{
  \"name\":\"debezium_service\",
  \"type\":\"tag\",
  \"description\":\"Debezium Kafka Connect — STREAMING_ZONE (tag service, KRaft cluster)\",
  \"isEnabled\":true,
  \"configs\":{
    \"ranger.plugin.policy.cache.dir\":\"/tmp/ranger/debezium\",
    \"ranger.plugin.policy.pollIntervalMs\":\"30000\"
  }
}"

create_service "akhq_service" "tag" "{
  \"name\":\"akhq_service\",
  \"type\":\"tag\",
  \"description\":\"AKHQ Kafka UI — STREAMING_ZONE (tag service, KRaft cluster)\",
  \"isEnabled\":true,
  \"configs\":{
    \"ranger.plugin.policy.cache.dir\":\"/tmp/ranger/akhq\",
    \"ranger.plugin.policy.pollIntervalMs\":\"30000\"
  }
}"

# ─────────────────────────────────────────────────────────────────────────────
# 4. SECURITY ZONES
# ─────────────────────────────────────────────────────────────────────────────
log "=== Creating Security Zones ==="

create_zone() {
  local zone_name="$1"
  local description="$2"
  local services_json="$3"   # JSON object of service name → {resources:[]}
  local zone_admins="$4"     # comma-separated group names

  # Build admin group array
  local admin_groups
  admin_groups=$(echo "${zone_admins}" \
    | python3 -c "
import sys
groups=[g.strip() for g in sys.stdin.read().split(',')]
print('['+','.join('\"'+g+'\"' for g in groups)+']')
")

  local existing_id
  existing_id=$(zone_id_by_name "${zone_name}")
  if [[ -n "${existing_id}" ]]; then
    # Zone exists — PUT to sync service list (handles services added after initial creation)
    local put_response
    put_response=$(ranger_put "/service/public/v2/api/zones/${existing_id}" "{
      \"id\":${existing_id},
      \"name\":\"${zone_name}\",
      \"description\":\"${description}\",
      \"services\":${services_json},
      \"adminUsers\":[],
      \"adminUserGroups\":${admin_groups},
      \"auditUsers\":[],
      \"auditUserGroups\":${admin_groups}
    }")
    local updated_id
    updated_id=$(echo "${put_response}" | extract_id)
    if [[ -n "${updated_id}" ]]; then
      ok "Zone: ${zone_name} synced services (id=${existing_id})"
    else
      skip "Zone: ${zone_name} (id=${existing_id}) — PUT returned: ${put_response}" >&2
    fi
    return
  fi

  local response id
  response=$(ranger_post "/service/public/v2/api/zones" "{
    \"name\":\"${zone_name}\",
    \"description\":\"${description}\",
    \"services\":${services_json},
    \"adminUsers\":[],
    \"adminUserGroups\":${admin_groups},
    \"auditUsers\":[],
    \"auditUserGroups\":${admin_groups}
  }")
  id=$(echo "${response}" | extract_id)
  if [[ -z "${id}" ]]; then
    die "Failed to create zone ${zone_name}. Ranger response: ${response}"
  fi
  ok "Zone: ${zone_name} (id=${id})"
}

# CACHING_ZONE — doris_service + polaris_service
# polaris_service is included here because Doris creates external Iceberg catalogs
# via the Polaris REST API (CREATE CATALOG ... TYPE='iceberg' ...).
# caching_admin/dev need Ranger policies on both services within the same zone.
create_zone "CACHING_ZONE" \
  "Caching layer — Apache Doris + Polaris (external Iceberg catalogs)" \
  "{\"doris_service\":{\"resources\":[]},\"polaris_service\":{\"resources\":[]}}" \
  "account_admin,caching_admin"

# PROCESSING_ZONE — spark, sqlmesh, kestra, opensearch, polaris
create_zone "PROCESSING_ZONE" \
  "Processing layer — Spark, SQLMesh, Kestra, OpenSearch, Polaris" \
  "{\"spark_service\":{\"resources\":[]},\"sqlmesh_service\":{\"resources\":[]},\"kestra_service\":{\"resources\":[]},\"opensearch_service\":{\"resources\":[]},\"polaris_service\":{\"resources\":[]}}" \
  "account_admin,processing_admin"

# STREAMING_ZONE — kafka, schema-registry, debezium, akhq
create_zone "STREAMING_ZONE" \
  "Streaming layer — Kafka, Schema Registry, Debezium, AKHQ" \
  "{\"kafka_service\":{\"resources\":[]},\"schema_registry_service\":{\"resources\":[]},\"debezium_service\":{\"resources\":[]},\"akhq_service\":{\"resources\":[]}}" \
  "account_admin,streaming_admin"

# ─────────────────────────────────────────────────────────────────────────────
# 5. ACCESS POLICIES
# ─────────────────────────────────────────────────────────────────────────────
log "=== Purging auto-generated zone default policies ==="
# Ranger automatically creates a full set of default policies for every service
# when a Security Zone is created. These conflict with our zone-scoped policies.
# Delete any default policies (those with no custom groups) before creating ours.
ALL_SERVICES="doris_service kafka_service spark_service sqlmesh_service \
  kestra_service opensearch_service polaris_service \
  schema_registry_service debezium_service akhq_service"
for zone in CACHING_ZONE PROCESSING_ZONE STREAMING_ZONE; do
  for svc in ${ALL_SERVICES}; do
    ids=$(curl -s -u "${RANGER_AUTH}" \
      "${RANGER_URL}/service/public/v2/api/policy?serviceName=${svc}&zoneName=${zone}&pageSize=100" \
      | python3 -c "
import sys,json
d=json.load(sys.stdin)
policies = d if isinstance(d,list) else d.get('policies',[])
for p in policies:
    groups=[]
    for item in p.get('policyItems',[]) + p.get('denyPolicyItems',[]):
        groups.extend(item.get('groups',[]))
    custom=[g for g in groups if g not in ('','public')]
    if not custom:
        print(p['id'])
" 2>/dev/null)
    for pid in ${ids}; do
      curl -s -o /dev/null -u "${RANGER_AUTH}" \
        -X DELETE "${RANGER_URL}/service/public/v2/api/policy/${pid}"
      ok "Removed default policy id=${pid} from ${zone}/${svc}"
    done
  done
done

log "=== Creating Access Policies ==="

# upsert_policy: PUT if policy id exists, POST otherwise.
# Usage: upsert_policy <policy_name> <service_name> <existing_id_or_empty> <body>
upsert_policy() {
  local policy_name="$1"
  local service_name="$2"
  local existing_id="$3"   # pass "" to always POST
  local body="$4"

  # Extract zoneName from body so policy_exists can check zone-scoped policies.
  local zone_name
  zone_name=$(echo "${body}" | python3 -c "
import sys, json
try:
    d = json.loads(sys.stdin.read())
    print(d.get('zoneName', ''))
except Exception:
    print('')
" 2>/dev/null || echo "")

  # Check by name first (handles re-runs) — include zone for zone-scoped lookup
  if policy_exists "${service_name}" "${policy_name}" "${zone_name}"; then
    skip "Policy: ${policy_name} on ${service_name}"
    return
  fi

  local response id method path
  if [[ -n "${existing_id}" ]]; then
    # PUT over the existing default policy so Ranger doesn't reject as duplicate resource
    method="PUT"
    path="/service/public/v2/api/policy/${existing_id}"
    response=$(curl -s -u "${RANGER_AUTH}" \
      -H "Content-Type: application/json" -H "Accept: application/json" \
      -X PUT -d "${body}" "${RANGER_URL}${path}")
  else
    method="POST"
    path="/service/public/v2/api/policy"
    response=$(ranger_post "${path}" "${body}")
  fi

  id=$(echo "${response}" | extract_id)
  if [[ -z "${id}" ]]; then
    die "Failed to upsert policy ${policy_name} (${method}). Ranger response: ${response}"
  fi
  ok "Policy: ${policy_name} on ${service_name} (${method} id=${id})"
}

# ──────────────────────────────────────────────────────
# CACHING_ZONE — Doris
# ──────────────────────────────────────────────────────

# Admin: full access to all databases/tables/columns
upsert_policy "caching-admin-all" "doris_service" "" '{
  "name":"caching-admin-all",
  "service":"doris_service",
  "zoneName":"CACHING_ZONE",
  "description":"Admin full access to all Doris resources",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "database":{"values":["*"],"isExcludes":false},
    "table":   {"values":["*"],"isExcludes":false},
    "column":  {"values":["*"],"isExcludes":false}
  },
  "policyItems":[{
    "groups":["caching_admin","account_admin"],
    "users":[],
    "accesses":[
      {"type":"select","isAllowed":true},
      {"type":"update","isAllowed":true},
      {"type":"create","isAllowed":true},
      {"type":"drop","isAllowed":true},
      {"type":"alter","isAllowed":true},
      {"type":"index","isAllowed":true},
      {"type":"lock","isAllowed":true},
      {"type":"all","isAllowed":true},
      {"type":"read","isAllowed":true},
      {"type":"write","isAllowed":true}
    ],
    "conditions":[],
    "delegateAdmin":true
  }],
  "denyPolicyItems":[]
}'

# Dev: SELECT-only on analytics database
upsert_policy "caching-dev-read" "doris_service" "" '{
  "name":"caching-dev-read",
  "service":"doris_service",
  "zoneName":"CACHING_ZONE",
  "description":"Dev read-only access to analytics Doris tables",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "database":{"values":["analytics"],"isExcludes":false},
    "table":   {"values":["events","user_metrics","users"],"isExcludes":false},
    "column":  {"values":["*"],"isExcludes":false}
  },
  "policyItems":[{
    "groups":["caching_dev"],
    "users":[],
    "accesses":[{"type":"select","isAllowed":true}],
    "conditions":[],
    "delegateAdmin":false
  }],
  "denyPolicyItems":[]
}'

# Dev: Column masking on PII — email, user_id, amount
upsert_policy "caching-dev-mask-email" "doris_service" "" '{
  "name":"caching-dev-mask-email",
  "service":"doris_service",
  "zoneName":"CACHING_ZONE",
  "policyType":1,
  "description":"Mask email column for caching_dev",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "database":{"values":["analytics"],"isExcludes":false},
    "table":   {"values":["users","events"],"isExcludes":false},
    "column":  {"values":["email"],"isExcludes":false}
  },
  "dataMaskPolicyItems":[{
    "groups":["caching_dev"],
    "users":[],
    "accesses":[{"type":"select","isAllowed":true}],
    "conditions":[],
    "dataMaskInfo":{"dataMaskType":"MASK_SHOW_FIRST_4"}
  }]
}'

upsert_policy "caching-dev-mask-user-id" "doris_service" "" '{
  "name":"caching-dev-mask-user-id",
  "service":"doris_service",
  "zoneName":"CACHING_ZONE",
  "policyType":1,
  "description":"Hash user_id for caching_dev",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "database":{"values":["analytics"],"isExcludes":false},
    "table":   {"values":["users","events","user_metrics"],"isExcludes":false},
    "column":  {"values":["user_id"],"isExcludes":false}
  },
  "dataMaskPolicyItems":[{
    "groups":["caching_dev"],
    "users":[],
    "accesses":[{"type":"select","isAllowed":true}],
    "conditions":[],
    "dataMaskInfo":{"dataMaskType":"MASK_HASH"}
  }]
}'

upsert_policy "caching-dev-mask-amount" "doris_service" "" '{
  "name":"caching-dev-mask-amount",
  "service":"doris_service",
  "zoneName":"CACHING_ZONE",
  "policyType":1,
  "description":"Nullify amount for caching_dev",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "database":{"values":["analytics"],"isExcludes":false},
    "table":   {"values":["events","user_metrics"],"isExcludes":false},
    "column":  {"values":["amount","total_spend"],"isExcludes":false}
  },
  "dataMaskPolicyItems":[{
    "groups":["caching_dev"],
    "users":[],
    "accesses":[{"type":"select","isAllowed":true}],
    "conditions":[],
    "dataMaskInfo":{"dataMaskType":"MASK_NULL"}
  }]
}'

# ──────────────────────────────────────────────────────
# PROCESSING_ZONE — Spark
# ──────────────────────────────────────────────────────

# ── Tag-type services: one policy per service with two policyItems ────────────
# Ranger rejects a second policy on the same service+resource combination.
# Admin (full) and dev (read-only) are expressed as separate policyItems inside
# a single policy. Policy name follows the admin convention; dev read is merged.

# PROCESSING_ZONE — Spark
upsert_policy "processing-admin-spark-all" "spark_service" "" '{
  "name":"processing-admin-spark-all",
  "service":"spark_service",
  "zoneName":"PROCESSING_ZONE",
  "description":"Spark: admin full access + dev read-only (tag service)",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "tag":{"values":["*"],"isExcludes":false}
  },
  "policyItems":[
    {
      "groups":["processing_admin","account_admin"],
      "users":[],
      "accesses":[
        {"type":"_READ","isAllowed":true},
        {"type":"_UPDATE","isAllowed":true},
        {"type":"_CREATE","isAllowed":true},
        {"type":"_DELETE","isAllowed":true},
        {"type":"_MANAGE","isAllowed":true},
        {"type":"_ALL","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":true
    },
    {
      "groups":["processing_dev"],
      "users":[],
      "accesses":[{"type":"_READ","isAllowed":true}],
      "conditions":[],
      "delegateAdmin":false
    }
  ],
  "denyPolicyItems":[]
}'

# PROCESSING_ZONE — SQLMesh
upsert_policy "processing-admin-sqlmesh-all" "sqlmesh_service" "" '{
  "name":"processing-admin-sqlmesh-all",
  "service":"sqlmesh_service",
  "zoneName":"PROCESSING_ZONE",
  "description":"SQLMesh: admin full access + dev read-only (tag service)",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "tag":{"values":["*"],"isExcludes":false}
  },
  "policyItems":[
    {
      "groups":["processing_admin","account_admin"],
      "users":[],
      "accesses":[
        {"type":"_READ","isAllowed":true},
        {"type":"_UPDATE","isAllowed":true},
        {"type":"_CREATE","isAllowed":true},
        {"type":"_DELETE","isAllowed":true},
        {"type":"_MANAGE","isAllowed":true},
        {"type":"_ALL","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":true
    },
    {
      "groups":["processing_dev"],
      "users":[],
      "accesses":[{"type":"_READ","isAllowed":true}],
      "conditions":[],
      "delegateAdmin":false
    }
  ],
  "denyPolicyItems":[]
}'

# PROCESSING_ZONE — Kestra
upsert_policy "processing-admin-kestra-all" "kestra_service" "" '{
  "name":"processing-admin-kestra-all",
  "service":"kestra_service",
  "zoneName":"PROCESSING_ZONE",
  "description":"Kestra: admin full access + dev read-only (tag service)",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "tag":{"values":["*"],"isExcludes":false}
  },
  "policyItems":[
    {
      "groups":["processing_admin","account_admin"],
      "users":[],
      "accesses":[
        {"type":"_READ","isAllowed":true},
        {"type":"_UPDATE","isAllowed":true},
        {"type":"_CREATE","isAllowed":true},
        {"type":"_DELETE","isAllowed":true},
        {"type":"_MANAGE","isAllowed":true},
        {"type":"_ALL","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":true
    },
    {
      "groups":["processing_dev"],
      "users":[],
      "accesses":[{"type":"_READ","isAllowed":true}],
      "conditions":[],
      "delegateAdmin":false
    }
  ],
  "denyPolicyItems":[]
}'

# PROCESSING_ZONE — OpenSearch (security plugin disabled in lab)
upsert_policy "processing-admin-opensearch-all" "opensearch_service" "" '{
  "name":"processing-admin-opensearch-all",
  "service":"opensearch_service",
  "zoneName":"PROCESSING_ZONE",
  "description":"OpenSearch: admin full access + dev read-only (tag service)",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "tag":{"values":["*"],"isExcludes":false}
  },
  "policyItems":[
    {
      "groups":["processing_admin","account_admin"],
      "users":[],
      "accesses":[
        {"type":"_READ","isAllowed":true},
        {"type":"_UPDATE","isAllowed":true},
        {"type":"_CREATE","isAllowed":true},
        {"type":"_DELETE","isAllowed":true},
        {"type":"_MANAGE","isAllowed":true},
        {"type":"_ALL","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":true
    },
    {
      "groups":["processing_dev"],
      "users":[],
      "accesses":[{"type":"_READ","isAllowed":true}],
      "conditions":[],
      "delegateAdmin":false
    }
  ],
  "denyPolicyItems":[]
}'

# PROCESSING_ZONE — Polaris (Iceberg REST Catalog)
upsert_policy "processing-admin-polaris-all" "polaris_service" "" '{
  "name":"processing-admin-polaris-all",
  "service":"polaris_service",
  "zoneName":"PROCESSING_ZONE",
  "description":"Polaris: admin full access + dev read-only (tag service)",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "tag":{"values":["*"],"isExcludes":false}
  },
  "policyItems":[
    {
      "groups":["processing_admin","account_admin"],
      "users":[],
      "accesses":[
        {"type":"_READ","isAllowed":true},
        {"type":"_UPDATE","isAllowed":true},
        {"type":"_CREATE","isAllowed":true},
        {"type":"_DELETE","isAllowed":true},
        {"type":"_MANAGE","isAllowed":true},
        {"type":"_ALL","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":true
    },
    {
      "groups":["processing_dev"],
      "users":[],
      "accesses":[{"type":"_READ","isAllowed":true}],
      "conditions":[],
      "delegateAdmin":false
    }
  ],
  "denyPolicyItems":[]
}'

# ──────────────────────────────────────────────────────
# STREAMING_ZONE — Kafka (native kafka service type, KRaft)
# kafka_service is registered as type=kafka. Uses native resource sets:
#   topic, consumergroup, cluster.
# Two policies: topic (admin full + dev consume) and consumergroup (admin full).
# ──────────────────────────────────────────────────────

# Topics — admin full + dev consume/describe merged into one policy
upsert_policy "streaming-admin-kafka-all" "kafka_service" "" '{
  "name":"streaming-admin-kafka-all",
  "service":"kafka_service",
  "zoneName":"STREAMING_ZONE",
  "description":"Kafka topics: admin full access + dev consume/describe",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "topic":{"values":["*"],"isExcludes":false}
  },
  "policyItems":[
    {
      "groups":["streaming_admin","account_admin"],
      "users":[],
      "accesses":[
        {"type":"publish","isAllowed":true},
        {"type":"consume","isAllowed":true},
        {"type":"configure","isAllowed":true},
        {"type":"describe","isAllowed":true},
        {"type":"create","isAllowed":true},
        {"type":"delete","isAllowed":true},
        {"type":"describe_configs","isAllowed":true},
        {"type":"alter_configs","isAllowed":true},
        {"type":"idempotent_write","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":true
    },
    {
      "groups":["streaming_dev"],
      "users":[],
      "accesses":[
        {"type":"consume","isAllowed":true},
        {"type":"describe","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":false
    }
  ],
  "denyPolicyItems":[]
}'

# Consumer groups — admin full; dev scoped to dev-* groups
upsert_policy "streaming-admin-kafka-consumergroup" "kafka_service" "" '{
  "name":"streaming-admin-kafka-consumergroup",
  "service":"kafka_service",
  "zoneName":"STREAMING_ZONE",
  "description":"Kafka consumer groups: admin full access + dev dev-* prefix",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "consumergroup":{"values":["*"],"isExcludes":false}
  },
  "policyItems":[
    {
      "groups":["streaming_admin","account_admin"],
      "users":[],
      "accesses":[
        {"type":"consume","isAllowed":true},
        {"type":"describe","isAllowed":true},
        {"type":"delete","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":true
    }
  ],
  "denyPolicyItems":[]
}'

# Dev consumer group access — scoped to dev-* prefix only
upsert_policy "streaming-dev-kafka-consumergroup" "kafka_service" "" '{
  "name":"streaming-dev-kafka-consumergroup",
  "service":"kafka_service",
  "zoneName":"STREAMING_ZONE",
  "description":"Kafka consumer groups: dev consume/describe on dev-* groups",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "consumergroup":{"values":["dev-*"],"isExcludes":false}
  },
  "policyItems":[
    {
      "groups":["streaming_dev"],
      "users":[],
      "accesses":[
        {"type":"consume","isAllowed":true},
        {"type":"describe","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":false
    }
  ],
  "denyPolicyItems":[]
}'

# ──────────────────────────────────────────────────────
# STREAMING_ZONE — Schema Registry / Debezium / AKHQ
#   All tag-type services — admin + dev merged into one policy per service
# ──────────────────────────────────────────────────────

for svc in schema_registry_service debezium_service akhq_service; do
  upsert_policy "${svc}-admin-all" "${svc}" "" "{
    \"name\":\"${svc}-admin-all\",
    \"service\":\"${svc}\",
    \"zoneName\":\"STREAMING_ZONE\",
    \"description\":\"Admin full access + dev read-only (tag service)\",
    \"isAuditEnabled\":true,
    \"isEnabled\":true,
    \"resources\":{
      \"tag\":{\"values\":[\"*\"],\"isExcludes\":false}
    },
    \"policyItems\":[
      {
        \"groups\":[\"streaming_admin\",\"account_admin\"],
        \"users\":[],
        \"accesses\":[
          {\"type\":\"_READ\",\"isAllowed\":true},
          {\"type\":\"_UPDATE\",\"isAllowed\":true},
          {\"type\":\"_CREATE\",\"isAllowed\":true},
          {\"type\":\"_DELETE\",\"isAllowed\":true},
          {\"type\":\"_MANAGE\",\"isAllowed\":true},
          {\"type\":\"_ALL\",\"isAllowed\":true}
        ],
        \"conditions\":[],
        \"delegateAdmin\":true
      },
      {
        \"groups\":[\"streaming_dev\"],
        \"users\":[],
        \"accesses\":[{\"type\":\"_READ\",\"isAllowed\":true}],
        \"conditions\":[],
        \"delegateAdmin\":false
      }
    ],
    \"denyPolicyItems\":[]
  }"
done

# ──────────────────────────────────────────────────────
# CACHING_ZONE — Doris external catalog (Polaris/Iceberg)
# Doris queries Iceberg via: CREATE CATALOG iceberg_polaris
#   PROPERTIES('type'='iceberg','iceberg.catalog.type'='rest',...)
# The external catalog appears as a separate database scope in Ranger.
# ──────────────────────────────────────────────────────

# Polaris external catalog in Doris — admin full access + dev SELECT only.
# Merged into one policy (same resource scope: database=iceberg_polaris).
upsert_policy "caching-admin-polaris-catalog" "doris_service" "" '{
  "name":"caching-admin-polaris-catalog",
  "service":"doris_service",
  "zoneName":"CACHING_ZONE",
  "description":"Polaris external catalog in Doris: admin full + dev SELECT (iceberg_polaris db)",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "database":{"values":["iceberg_polaris"],"isExcludes":false},
    "table":   {"values":["*"],"isExcludes":false},
    "column":  {"values":["*"],"isExcludes":false}
  },
  "policyItems":[
    {
      "groups":["caching_admin","account_admin"],
      "users":[],
      "accesses":[
        {"type":"select","isAllowed":true},
        {"type":"update","isAllowed":true},
        {"type":"create","isAllowed":true},
        {"type":"drop","isAllowed":true},
        {"type":"alter","isAllowed":true},
        {"type":"all","isAllowed":true},
        {"type":"read","isAllowed":true},
        {"type":"write","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":true
    },
    {
      "groups":["caching_dev"],
      "users":[],
      "accesses":[{"type":"select","isAllowed":true}],
      "conditions":[],
      "delegateAdmin":false
    }
  ],
  "denyPolicyItems":[]
}'

# ──────────────────────────────────────────────────────
# CACHING_ZONE — Polaris REST API (tag service, scoped to CACHING_ZONE)
# Doris calls the Polaris REST API directly to manage Iceberg catalogs.
# caching_admin gets full control; caching_dev gets read-only catalog discovery.
# Admin + dev merged into one policy (tag resource, one policy per service rule).
# ──────────────────────────────────────────────────────
upsert_policy "caching-admin-polaris-api" "polaris_service" "" '{
  "name":"caching-admin-polaris-api",
  "service":"polaris_service",
  "zoneName":"CACHING_ZONE",
  "description":"Polaris REST API: caching_admin full access + caching_dev read-only (CACHING_ZONE)",
  "isAuditEnabled":true,
  "isEnabled":true,
  "resources":{
    "tag":{"values":["*"],"isExcludes":false}
  },
  "policyItems":[
    {
      "groups":["caching_admin","account_admin"],
      "users":[],
      "accesses":[
        {"type":"_READ","isAllowed":true},
        {"type":"_UPDATE","isAllowed":true},
        {"type":"_CREATE","isAllowed":true},
        {"type":"_DELETE","isAllowed":true},
        {"type":"_MANAGE","isAllowed":true},
        {"type":"_ALL","isAllowed":true}
      ],
      "conditions":[],
      "delegateAdmin":true
    },
    {
      "groups":["caching_dev"],
      "users":[],
      "accesses":[{"type":"_READ","isAllowed":true}],
      "conditions":[],
      "delegateAdmin":false
    }
  ],
  "denyPolicyItems":[]
}'

# ─────────────────────────────────────────────────────────────────────────────
# 6. SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
echo ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "  Ranger RBAC bootstrap complete."
log ""
log "  Zones created:"
log "    CACHING_ZONE    → doris_service, polaris_service"
log "    PROCESSING_ZONE → spark_service, sqlmesh_service, kestra_service, opensearch_service, polaris_service"
log "    STREAMING_ZONE  → kafka_service, schema_registry_service,"
log "                      debezium_service, akhq_service"
log ""
log "  Users:"
log "    platform_admin      → account_admin (ROLE_SYS_ADMIN)"
log "    caching_admin_user  → caching_admin"
log "    caching_dev_user    → caching_dev"
log "    processing_admin_user → processing_admin"
log "    processing_dev_user → processing_dev"
log "    streaming_admin_user → streaming_admin"
log "    streaming_dev_user  → streaming_dev"
log ""
log "  Ranger UI: http://192.168.1.50:30680"
log ""
log "  Verify: bash scripts/master/17-verify-rbac.sh"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
